"""代理防护联动服务 — 跨策略冲突处理与防护限流回执。

数据工程核心：跨策略冲突裁决 + 可导出数据文件产出 — 跨模块数据产物

数据流:
    处理项承载的代理切换计划 → channel("rpa.proxy_switch_plan")
    → 反封锁模块接收并展开（consume，consumer=anti_block）
    → 重算窗口内代理防护联动的防护策略数据（请求间隔按最终节点绑定更新）
    → 跨策略冲突裁决：取两者更严格值 / 按节点绑定的策略版本重新计算
    → 节点切换与有效频控共同发布 channel("rpa.guard_binding")
    → 生成防护限流回执（每个当前绑定节点唯一上限）→ 可导出 CSV 本地交付

既有问题：代理切换会更新防护策略，站点频控却保留旧上限，同一站点切到
新节点后可能同时存在多个互不相同的每秒限制。本服务保证：导出回执只
包含当前绑定节点的唯一上限；冲突会把代理防护联动的处理终态推进到
30 号（冲突已裁决）。
"""
from __future__ import annotations

import re
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

# 处理终态编号：冲突会把联动终态推进到 30 号（冲突已裁决）。
TERMINAL_STATES: dict[str, int] = {
    "待处理": 10,
    "已对齐": 20,
    "冲突已裁决": 30,
    "部分驳回": 40,
}
TERMINAL_BY_CODE: dict[int, str] = {code: name for name, code in TERMINAL_STATES.items()}

# 冲突裁决策略：strict=取两者更严格值；version=按节点绑定的策略版本重新计算。
STRATEGY_MODES = ("strict", "version")

# 策略版本重算系数：按节点绑定的策略版本重新计算每秒上限。
# 未知版本回退 1.0，兼容既有版本边界。
VERSION_FACTORS: dict[str, float] = {"v1": 1.0, "v2": 0.5, "v3": 0.25}

# 兼容既有版本边界：处理项负载版本 1（旧版）与 2（当前）都可受理。
SUPPORTED_PAYLOAD_VERSIONS = (1, 2)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_per_second(value: Any) -> float | None:
    """从「每秒上限」文本解析数值，如 "8"、"8次/秒"、"5/s"；无法解析返回 None。

    必须全串匹配（数字 + 可选单位），避免把占位文本里的序号误当上限。
    """
    text = str(value or "").strip()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(次/秒|次/s|/s|req/s|rps)?", text,
                         flags=re.IGNORECASE)
    if not match:
        return None
    limit = float(match.group(1))
    return limit if limit > 0 else None


def parse_interval_ms(value: Any) -> float | None:
    """从「请求间隔」文本解析毫秒数：支持 250ms / 0.5s / 2秒 / 500毫秒 / 裸数字(秒)。"""
    text = str(value or "").strip()
    if not text:
        return None
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(ms|毫秒)", text, flags=re.IGNORECASE)
    if match:
        return float(match.group(1))
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*(s|秒)", text, flags=re.IGNORECASE)
    if match:
        return float(match.group(1)) * 1000.0
    match = re.fullmatch(r"(\d+(?:\.\d+)?)", text)
    if match:
        return float(match.group(1)) * 1000.0
    return None


def format_interval(ms: float) -> str:
    """把毫秒格式化为请求间隔文本，保留足够精度保证换算可逆。"""
    if ms >= 1000:
        seconds = ms / 1000.0
        return f"{seconds:g}s"
    if abs(ms - round(ms)) < 1e-6:
        return f"{int(round(ms))}ms"
    return f"{ms:.3f}".rstrip("0").rstrip(".") + "ms"


class ProxyGuardService:
    """代理防护联动：切换计划受理、冲突裁决、限流回执产出。"""

    SERVICE_NAME = "proxy_guard"
    ENTITY = "防护限流回执"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._process_lock = threading.Lock()
        self._plan_seq = 0
        self._item_seq = 0
        self._receipt_seq = 0
        self._plans: list[dict[str, Any]] = []
        self._items: list[dict[str, Any]] = []
        self._receipts: list[dict[str, Any]] = []
        self._terminal_state = "待处理"

    # ------------------------------------------------------------------
    # 处理项承载的代理切换计划：提交与归一化
    # ------------------------------------------------------------------

    def submit_plan(self, items: list[dict[str, Any]],
                    payload_version: int = 2) -> dict[str, Any]:
        """受理代理切换计划：逐条归一化处理项并发布到通道。

        兼容既有版本边界：负载版本 1（旧版，字段为 站点/节点）与
        版本 2（当前，字段为 站点/目标节点/策略版本/窗口）都可受理；
        不支持的版本逐条驳回，不影响同计划其他处理项。
        """
        if payload_version not in SUPPORTED_PAYLOAD_VERSIONS:
            return {
                "ok": False,
                "message": f"不支持的负载版本：{payload_version}，"
                           f"兼容边界为 {SUPPORTED_PAYLOAD_VERSIONS}",
                "accepted": 0, "rejected": 0, "items": [],
            }
        with self._lock:
            self._plan_seq += 1
            plan_id = f"PLAN-{self._plan_seq:04d}"

        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for raw in items:
            item, error = self._normalize_item(raw, plan_id, payload_version)
            if error:
                rejected.append({"raw": raw, "原因": error})
                continue
            with self._lock:
                self._items.append(item)
            accepted.append(item)
            # 处理项承载的切换计划进入通道，等待反封锁模块接收展开。
            store.channel.publish(
                topic="rpa.proxy_switch_plan",
                source=self.SERVICE_NAME,
                payload=dict(item),
            )

        return {
            "ok": True,
            "plan_id": plan_id,
            "accepted": len(accepted),
            "rejected": len(rejected),
            "items": accepted,
            "rejections": rejected,
            "message": f"切换计划 {plan_id} 已受理：{len(accepted)} 条处理项待展开",
        }

    def submit_demo_plan(self) -> dict[str, Any]:
        """按当前代理节点与频控规则生成一份示例切换计划（本地演示用）。"""
        sites: list[str] = []
        for row in store.rows("rate_limiter"):
            site = str(row.get("目标站点", "")).strip()
            if (row.get("status") == "已生效" and site
                    and parse_per_second(row.get("每秒上限")) and site not in sites):
                sites.append(site)
        nodes = [r for r in store.rows("proxy_router") if r.get("status") == "在线"]
        if not sites or len(nodes) < 2:
            return {
                "ok": False,
                "message": "示例计划生成失败：需要至少 1 条已生效频控规则和 2 个在线代理节点",
                "accepted": 0, "rejected": 0, "items": [],
            }
        now = datetime.now(timezone.utc)
        window = f"{now:%Y-%m-%d} {now:%H}:00~{now.hour + 1:02d}:00"
        items: list[dict[str, Any]] = []
        for index, site in enumerate(sites[:2]):
            source = nodes[index % len(nodes)]
            target = nodes[(index + 1) % len(nodes)]
            items.append({
                "站点": site,
                "源节点": source.get("节点名称"),
                "目标节点": target.get("节点名称"),
                "切换原因": "触发封锁-联动演练",
                "策略版本": "v2",
                "窗口": window,
            })
        return self.submit_plan(items, payload_version=2)

    def _normalize_item(self, raw: dict[str, Any], plan_id: str,
                        payload_version: int) -> tuple[dict[str, Any] | None, str]:
        """把一条处理项归一化为当前结构；失败返回错误原因。"""
        if not isinstance(raw, dict):
            return None, "处理项必须是对象"
        site = str(raw.get("站点") or "").strip()
        if not site:
            return None, "缺少站点"
        if payload_version == 1:
            # 旧版负载：节点字段为「节点」，无策略版本与窗口。
            target = str(raw.get("节点") or raw.get("目标节点") or "").strip()
            version = "v1"
            window = "默认窗口"
            reason = str(raw.get("切换原因") or "旧版计划-版本兼容")
        else:
            target = str(raw.get("目标节点") or "").strip()
            version = str(raw.get("策略版本") or "v2").strip() or "v2"
            window = str(raw.get("窗口") or "默认窗口").strip()
            reason = str(raw.get("切换原因") or "代理切换")
        if not target:
            return None, "缺少目标节点"
        with self._lock:
            self._item_seq += 1
            item_id = f"ITEM-{self._item_seq:04d}"
        item = {
            "item_id": item_id,
            "plan_id": plan_id,
            "站点": site,
            "源节点": str(raw.get("源节点") or ""),
            "目标节点": target,
            "切换原因": reason,
            "策略版本": version,
            "窗口": window,
            "负载版本": payload_version,
            "状态": "待处理",
            "提交时间": _now_iso(),
        }
        return item, ""

    # ------------------------------------------------------------------
    # 反封锁模块接收并展开：联动重算与回执产出
    # ------------------------------------------------------------------

    def process_plans(self, strategy: str = "strict") -> dict[str, Any]:
        """接收并展开处理项承载的代理切换计划，生成防护限流回执。

        并发执行只接受首个成功结果：同一时刻只允许一个联动处理在跑，
        后来的并发调用直接拒绝。
        """
        if strategy not in STRATEGY_MODES:
            return {
                "ok": False,
                "message": f"未知冲突裁决策略：{strategy}，可选 {STRATEGY_MODES}",
                "processed": 0,
            }
        if not self._process_lock.acquire(blocking=False):
            return {
                "ok": False,
                "message": "已有联动处理在执行中，并发执行只接受首个成功结果",
                "processed": 0,
            }
        try:
            return self._process_locked(strategy)
        finally:
            self._process_lock.release()

    def _process_locked(self, strategy: str) -> dict[str, Any]:
        # 反封锁模块接收通道中的切换计划处理项。
        messages = store.channel.consume(
            consumer="anti_block", topic="rpa.proxy_switch_plan")
        results: list[dict[str, Any]] = []
        conflicts = 0
        rejected = 0
        issued = 0

        for msg in messages:
            payload = dict(msg.payload)
            item = self._find_item(payload.get("item_id"))
            if item is None:
                # 通道里出现未登记的处理项：直接展开登记，保持链路完整。
                item = {**payload, "状态": "待处理"}
                with self._lock:
                    self._items.append(item)
            item["状态"] = "已展开"
            outcome = self._expand_item(item, strategy)
            results.append(outcome)
            if outcome["结果"] == "已生成回执":
                issued += 1
                if outcome.get("冲突"):
                    conflicts += 1
            else:
                rejected += 1
            store.channel.confirm(msg.msg_id, "anti_block")

        processed = issued + rejected
        if conflicts:
            terminal = "冲突已裁决"  # 30 号处理终态
        elif rejected:
            terminal = "部分驳回"
        elif processed:
            terminal = "已对齐"
        else:
            terminal = self._terminal_state
        self._terminal_state = terminal

        return {
            "ok": True,
            "strategy": strategy,
            "processed": processed,
            "receipts": issued,
            "conflicts": conflicts,
            "rejected": rejected,
            "terminal_state": terminal,
            "terminal_code": TERMINAL_STATES[terminal],
            "items": results,
            "message": (f"联动处理完成：展开 {processed} 条处理项，"
                        f"回执 {issued} 条，冲突 {conflicts} 条，驳回 {rejected} 条"),
        }

    def _expand_item(self, item: dict[str, Any], strategy: str) -> dict[str, Any]:
        """展开一条处理项：解析有效请求边界，共同发布并生成回执。"""
        site = item["站点"]
        node = self._resolve_node(item["目标节点"])
        if node is None:
            item["状态"] = "已驳回"
            return {"item_id": item["item_id"], "站点": site,
                    "结果": "已驳回", "原因": f"目标节点不存在：{item['目标节点']}"}
        if node.get("status") == "已禁用":
            item["状态"] = "已驳回"
            return {"item_id": item["item_id"], "站点": site,
                    "结果": "已驳回", "原因": f"目标节点已禁用：{node.get('节点名称')}"}

        site_limits = self._site_limits(site)
        protection_limits = self._protection_limits(site)
        if not site_limits and not protection_limits:
            item["状态"] = "已驳回"
            return {"item_id": item["item_id"], "站点": site,
                    "结果": "已驳回", "原因": "站点缺少可用频控规则与防护策略数据"}

        candidates = sorted({value for _, value in site_limits}
                            | {value for _, value in protection_limits})
        conflict = len(candidates) > 1

        if strategy == "version":
            # 按节点绑定的策略版本重新计算上限。
            base = (protection_limits[0][1] if protection_limits
                    else min(value for _, value in site_limits))
            factor = VERSION_FACTORS.get(item["策略版本"], 1.0)
            final_limit = round(base * factor, 4)
            resolution = "按策略版本重算" if conflict else "无冲突"
        else:
            # 取两者更严格值。
            final_limit = min(candidates)
            resolution = "取更严格值" if conflict else "无冲突"

        interval_ms = round(1000.0 / final_limit, 3)
        interval_text = format_interval(interval_ms)
        rule_source = self._rule_source(site_limits, protection_limits)

        # 重算窗口内代理防护联动的防护策略数据：按最终节点绑定更新请求间隔。
        strategy_ids = self._rebind_strategies(
            site=site, node=node, interval_text=interval_text,
            version=item["策略版本"], window=item["窗口"])

        receipt = self._issue_receipt(
            item=item, node=node, final_limit=final_limit,
            interval_text=interval_text, rule_source=rule_source,
            resolution=resolution, conflict=conflict,
            strategy_ids=strategy_ids)

        # 节点切换与有效频控必须共同发布：一条消息同时携带绑定与上限。
        store.channel.publish(
            topic="rpa.guard_binding",
            source="anti_block",
            payload={
                "receipt_id": receipt["receipt_id"],
                "站点": site,
                "节点ID": node.get("id"),
                "节点名称": node.get("节点名称"),
                "最终每秒上限": final_limit,
                "请求间隔": interval_text,
                "处理终态": receipt["处理终态"],
                "终态码": receipt["终态码"],
                "切换时间": receipt["切换时间"],
            },
        )
        item["状态"] = "已生成回执"
        return {
            "item_id": item["item_id"], "站点": site,
            "结果": "已生成回执", "回执": receipt["receipt_id"],
            "最终每秒上限": final_limit, "冲突": conflict,
            "冲突处理": resolution,
        }

    # ------------------------------------------------------------------
    # 限流回执：当前绑定唯一上限
    # ------------------------------------------------------------------

    def _issue_receipt(self, *, item: dict[str, Any], node: dict[str, Any],
                       final_limit: float, interval_text: str,
                       rule_source: str, resolution: str, conflict: bool,
                       strategy_ids: list[int]) -> dict[str, Any]:
        with self._lock:
            # 同一站点只保留当前绑定节点的唯一上限：旧回执全部取代。
            for old in self._receipts:
                if old["站点"] == item["站点"] and old["状态"] == "当前":
                    old["状态"] = "已取代"
            self._receipt_seq += 1
            receipt = {
                "receipt_id": f"RCPT-{self._receipt_seq:04d}",
                "站点": item["站点"],
                "节点ID": node.get("id"),
                "节点名称": node.get("节点名称"),
                "最终每秒上限": final_limit,
                "请求间隔": interval_text,
                "规则来源": rule_source,
                "冲突处理": resolution,
                "处理终态": "冲突已裁决" if conflict else "已对齐",
                "终态码": TERMINAL_STATES["冲突已裁决" if conflict else "已对齐"],
                "策略版本": item["策略版本"],
                "切换时间": _now_iso(),
                "重算窗口": item["窗口"],
                "防护策略IDs": strategy_ids,
                "状态": "当前",
            }
            self._receipts.append(receipt)
        return receipt

    def list_receipts(self, site: str | None = None,
                      node: str | None = None,
                      current_only: bool = False) -> list[dict[str, Any]]:
        """结果列表：按站点和节点查看最终上限、规则来源与切换时间。"""
        with self._lock:
            rows = list(self._receipts)
        if site:
            rows = [r for r in rows if site.casefold() in str(r["站点"]).casefold()]
        if node:
            token = node.casefold()
            rows = [r for r in rows
                    if token in str(r["节点名称"]).casefold()
                    or token == str(r["节点ID"]).casefold()]
        if current_only:
            rows = [r for r in rows if r["状态"] == "当前"]
        return rows

    def export_rows(self) -> list[dict[str, Any]]:
        """导出回执数据：只包含当前绑定节点的唯一上限。"""
        return self.list_receipts(current_only=True)

    # ------------------------------------------------------------------
    # 处理清单与快照
    # ------------------------------------------------------------------

    def list_plans(self) -> list[dict[str, Any]]:
        """处理清单：全部切换计划处理项。"""
        with self._lock:
            return list(self._items)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            items = list(self._items)
            receipts = list(self._receipts)
        by_status: dict[str, int] = {}
        for item in items:
            by_status[item["状态"]] = by_status.get(item["状态"], 0) + 1
        current = [r for r in receipts if r["状态"] == "当前"]
        return {
            "module": self.SERVICE_NAME,
            "entity": self.ENTITY,
            "artifact": "防护限流回执数据文件",
            "plans": len({item["plan_id"] for item in items}),
            "items": len(items),
            "items_by_status": by_status,
            "receipts": len(receipts),
            "current_receipts": len(current),
            "superseded": len(receipts) - len(current),
            "conflicts": sum(1 for r in receipts if r["处理终态"] == "冲突已裁决"),
            "terminal_state": self._terminal_state,
            "terminal_code": TERMINAL_STATES[self._terminal_state],
            "strategy_modes": list(STRATEGY_MODES),
            "supported_payload_versions": list(SUPPORTED_PAYLOAD_VERSIONS),
        }

    # ------------------------------------------------------------------
    # 内部：站点/节点数据解析
    # ------------------------------------------------------------------

    def _find_item(self, item_id: Any) -> dict[str, Any] | None:
        with self._lock:
            for item in self._items:
                if item.get("item_id") == item_id:
                    return item
        return None

    @staticmethod
    def _resolve_node(token: Any) -> dict[str, Any] | None:
        """按 id、节点名称或 IP 解析代理节点。"""
        text = str(token or "").strip()
        if not text:
            return None
        rows = store.rows("proxy_router")
        for row in rows:
            if str(row.get("id")) == text:
                return row
        for row in rows:
            if text in (str(row.get("节点名称", "")), str(row.get("IP地址", ""))):
                return row
        return None

    @staticmethod
    def _site_limits(site: str) -> list[tuple[int, float]]:
        """站点频控给出的每秒上限候选（已生效规则）。"""
        limits: list[tuple[int, float]] = []
        for row in store.rows("rate_limiter"):
            if row.get("status") != "已生效":
                continue
            if site not in str(row.get("目标站点", "")):
                continue
            value = parse_per_second(row.get("每秒上限"))
            if value:
                limits.append((int(row.get("id", 0)), value))
        return limits

    @staticmethod
    def _protection_limits(site: str) -> list[tuple[int, float]]:
        """节点防护给出的每秒上限候选（已启用防护策略的请求间隔换算）。"""
        limits: list[tuple[int, float]] = []
        for row in store.rows("anti_block"):
            if row.get("status") != "已启用":
                continue
            if site not in str(row.get("目标站点", "")):
                continue
            ms = parse_interval_ms(row.get("请求间隔"))
            if ms and ms > 0:
                limits.append((int(row.get("id", 0)), round(1000.0 / ms, 4)))
        return limits

    @staticmethod
    def _rule_source(site_limits: list[tuple[int, float]],
                     protection_limits: list[tuple[int, float]]) -> str:
        parts = [f"频控规则#{rid}" for rid, _ in site_limits]
        parts += [f"防护策略#{sid}" for sid, _ in protection_limits]
        return " + ".join(parts) if parts else "无"

    @staticmethod
    def _rebind_strategies(*, site: str, node: dict[str, Any],
                           interval_text: str, version: str,
                           window: str) -> list[int]:
        """按最终节点绑定更新防护策略的请求间隔。

        同一站点所有已启用策略统一重算为最终间隔，消除切换后并存
        多个互不相同每秒限制的旧问题；站点没有已启用策略时补建一条。
        """
        bound: list[int] = []
        rows = store.rows("anti_block")
        for row in rows:
            if row.get("status") == "已启用" and site in str(row.get("目标站点", "")):
                row["请求间隔"] = interval_text
                row["绑定节点"] = node.get("节点名称")
                row["绑定节点ID"] = node.get("id")
                row["策略版本"] = version
                row["重算窗口"] = window
                bound.append(int(row.get("id", 0)))
        if not bound:
            new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            rows.append({
                "id": new_id,
                "status": "已启用",
                "pending": True,
                "abnormal": False,
                "策略名称": f"{site}-联动防护",
                "目标站点": site,
                "请求间隔": interval_text,
                "IP代理池": str(node.get("节点名称", "")),
                "UA轮换": "按批次",
                "验证码处理": "自动打码",
                "Cookie策略": "隔离会话",
                "绑定节点": node.get("节点名称"),
                "绑定节点ID": node.get("id"),
                "策略版本": version,
                "重算窗口": window,
            })
            bound.append(new_id)
        return bound


# 全局联动实例
proxy_guard = ProxyGuardService()
