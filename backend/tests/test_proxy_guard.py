"""代理防护联动测试 — 跨策略冲突处理与防护限流回执。

覆盖：
1. 处理项承载的切换计划受理（含旧版负载兼容、版本边界驳回）
2. 反封锁模块接收展开、按最终节点绑定更新请求间隔
3. 冲突裁决两种策略（取更严格值 / 按策略版本重算）与 30 号处理终态
4. 节点切换与有效频控共同发布、当前绑定唯一上限、回执导出
5. 并发执行只接受首个成功结果
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.store import store
from app.services.anti_block import AntiBlockService
from app.services.pipeline import pipeline_proxy_guard_linkage
from app.services.proxy_guard import (
    ProxyGuardService,
    format_interval,
    parse_interval_ms,
    parse_per_second,
    proxy_guard,
)


# ── 测试工具 ──

def _add_rate_rule(site: str, per_second: str) -> dict:
    rows = store.rows("rate_limiter")
    new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    row = {
        "id": new_id, "status": "已生效", "pending": True, "abnormal": False,
        "规则名称": f"{site}-限速{new_id}", "目标站点": site,
        "每秒上限": per_second, "每日上限": "100000", "并发上限": "4",
        "冷却时间": "30s", "优先级": "高",
    }
    rows.append(row)
    return row


def _add_strategy(site: str, interval: str) -> dict:
    rows = store.rows("anti_block")
    new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    row = {
        "id": new_id, "status": "已启用", "pending": True, "abnormal": False,
        "策略名称": f"{site}-防护{new_id}", "目标站点": site,
        "请求间隔": interval, "IP代理池": "华东-edge-01",
        "UA轮换": "按批次", "验证码处理": "自动打码", "Cookie策略": "隔离会话",
    }
    rows.append(row)
    return row


def _submit_and_process(site: str, target_node: str, strategy: str = "strict",
                        version: str = "v2") -> dict:
    result = proxy_guard.submit_plan([{
        "站点": site, "目标节点": target_node,
        "切换原因": "测试切换", "策略版本": version, "窗口": "测试窗口",
    }])
    assert result["ok"] and result["accepted"] == 1
    return proxy_guard.process_plans(strategy=strategy)


def _current_receipt(site: str) -> dict | None:
    for receipt in proxy_guard.list_receipts(site=site, current_only=True):
        return receipt
    return None


# ── 解析函数 ──

def test_parse_helpers():
    assert parse_per_second("8") == 8.0
    assert parse_per_second("8次/秒") == 8.0
    assert parse_per_second("频控策略样例2") is None
    assert parse_interval_ms("250ms") == 250.0
    assert parse_interval_ms("0.5s") == 500.0
    assert parse_interval_ms("2秒") == 2000.0
    assert parse_interval_ms("2") == 2000.0
    assert parse_interval_ms("反封锁样例1") is None
    assert format_interval(1000) == "1s"
    assert format_interval(200) == "200ms"
    # 间隔 → 上限换算按毫秒精度可逆（最终上限保持一致）
    assert round(1000 / parse_interval_ms(format_interval(1000 / 6)), 4) == 6.0


# ── 处理项承载的切换计划 ──

def test_submit_plan_registers_items_and_channel():
    before = proxy_guard.snapshot()["items"]
    result = proxy_guard.submit_plan([{
        "站点": "guard-submit.example", "目标节点": "华东-edge-01",
        "策略版本": "v2", "窗口": "2026-11-18 10:00~11:00",
    }])
    assert result["ok"] is True
    assert result["accepted"] == 1
    assert proxy_guard.snapshot()["items"] == before + 1
    item = result["items"][0]
    assert item["状态"] == "待处理"
    msgs = store.channel.snapshot(topic="rpa.proxy_switch_plan")
    assert any(m["payload"].get("item_id") == item["item_id"] for m in msgs)
    # 清理：展开并驳回（站点无规则），避免影响后续用例
    proxy_guard.process_plans()


def test_legacy_payload_version_compatible():
    """兼容既有版本边界：旧版负载（版本1，字段为 站点/节点）可受理。"""
    result = proxy_guard.submit_plan(
        [{"站点": "guard-legacy.example", "节点": "华东-edge-01"}],
        payload_version=1,
    )
    assert result["ok"] is True
    item = result["items"][0]
    assert item["目标节点"] == "华东-edge-01"
    assert item["策略版本"] == "v1"
    assert item["窗口"] == "默认窗口"
    assert item["负载版本"] == 1
    proxy_guard.process_plans()  # 清理通道


def test_unsupported_payload_version_rejected():
    result = proxy_guard.submit_plan(
        [{"站点": "guard-v99.example", "目标节点": "华东-edge-01"}],
        payload_version=99,
    )
    assert result["ok"] is False
    assert "兼容边界" in result["message"]


# ── 冲突裁决与处理终态 ──

def test_strict_conflict_resolution_and_terminal_state():
    """站点频控与节点防护给出不同上限：取更严格值，终态推进到 30 号。"""
    site = "guard-strict.example"
    _add_rate_rule(site, "8")
    _add_rate_rule(site, "5")
    _add_strategy(site, "1000ms")  # 节点防护 1/s

    run = _submit_and_process(site, "华东-edge-01", strategy="strict")
    assert run["ok"] is True
    assert run["conflicts"] == 1
    assert run["terminal_state"] == "冲突已裁决"
    assert run["terminal_code"] == 30

    receipt = _current_receipt(site)
    assert receipt is not None
    assert receipt["最终每秒上限"] == 1.0  # min(8, 5, 1)
    assert receipt["冲突处理"] == "取更严格值"
    assert receipt["处理终态"] == "冲突已裁决"
    assert receipt["终态码"] == 30
    assert receipt["规则来源"]
    assert receipt["切换时间"]


def test_version_recompute_resolution():
    """按节点绑定的策略版本重新计算上限，同样只导出一个最终上限。"""
    site = "guard-version.example"
    _add_rate_rule(site, "8")
    _add_strategy(site, "500ms")  # 节点防护 2/s

    run = _submit_and_process(site, "华南-edge-02", strategy="version", version="v2")
    assert run["ok"] is True
    assert run["conflicts"] == 1

    receipt = _current_receipt(site)
    assert receipt is not None
    assert receipt["最终每秒上限"] == 1.0  # 2/s × v2系数0.5
    assert receipt["冲突处理"] == "按策略版本重算"
    assert receipt["终态码"] == 30


def test_aligned_when_no_conflict():
    site = "guard-align.example"
    _add_rate_rule(site, "4")

    run = _submit_and_process(site, "华东-edge-01")
    assert run["terminal_state"] == "已对齐"
    assert run["terminal_code"] == 20

    receipt = _current_receipt(site)
    assert receipt is not None
    assert receipt["最终每秒上限"] == 4.0
    assert receipt["冲突处理"] == "无冲突"
    assert receipt["请求间隔"] == "250ms"


# ── 节点绑定、共同发布与唯一上限 ──

def test_interval_rebound_on_strategy():
    """按最终节点绑定更新防护策略的请求间隔。"""
    site = "guard-rebind.example"
    _add_rate_rule(site, "6")
    strategy_row = _add_strategy(site, "100ms")  # 节点防护 10/s，与频控 6/s 冲突

    run = _submit_and_process(site, "华东-edge-01")
    assert run["receipts"] == 1
    assert strategy_row["请求间隔"] == format_interval(1000 / 6)
    assert strategy_row["请求间隔"] != "100ms"
    assert strategy_row["绑定节点"] == "华东-edge-01"
    assert strategy_row["策略版本"] == "v2"


def test_joint_publish_and_unique_current_receipt():
    """节点切换与有效频控共同发布；导出只含当前绑定节点的唯一上限。"""
    site = "guard-switch.example"
    _add_rate_rule(site, "6")

    _submit_and_process(site, "华东-edge-01")
    first = _current_receipt(site)
    assert first is not None and first["节点名称"] == "华东-edge-01"

    _submit_and_process(site, "华南-edge-02")
    receipts = proxy_guard.list_receipts(site=site)
    current = [r for r in receipts if r["状态"] == "当前"]
    superseded = [r for r in receipts if r["状态"] == "已取代"]
    assert len(current) == 1, "同一站点只能有一个当前绑定回执"
    assert current[0]["节点名称"] == "华南-edge-02"
    assert len(superseded) == 1

    # 共同发布：一条通道消息同时携带节点绑定与有效上限
    latest = store.channel.latest("rpa.guard_binding")
    assert latest is not None
    payload = latest["payload"]
    assert payload["站点"] == site
    assert payload["节点名称"] == "华南-edge-02"
    assert payload["最终每秒上限"] == current[0]["最终每秒上限"]

    # 导出回执只包含当前绑定节点的唯一上限
    exported = [r for r in proxy_guard.export_rows() if r["站点"] == site]
    assert len(exported) == 1
    assert exported[0]["节点名称"] == "华南-edge-02"


def test_unknown_node_rejected():
    site = "guard-nonode.example"
    _add_rate_rule(site, "4")
    run = _submit_and_process(site, "不存在的节点")
    assert run["rejected"] == 1
    assert run["terminal_state"] == "部分驳回"
    assert _current_receipt(site) is None


# ── 并发执行只接受首个成功结果 ──

def test_concurrent_process_accepts_only_first():
    acquired = proxy_guard._process_lock.acquire(blocking=False)
    assert acquired is True
    try:
        result = proxy_guard.process_plans()
        assert result["ok"] is False
        assert "首个成功结果" in result["message"]
    finally:
        proxy_guard._process_lock.release()
    # 锁释放后可以正常执行
    assert proxy_guard.process_plans()["ok"] is True


# ── 导出与快照 ──

def test_export_rows_only_current_bindings():
    rows = proxy_guard.export_rows()
    assert rows, "导出回执不能为空"
    assert all(r["状态"] == "当前" for r in rows)
    sites = [r["站点"] for r in rows]
    assert len(sites) == len(set(sites)), "每个站点只能导出一条当前绑定回执"
    for row in rows:
        assert row["最终每秒上限"] > 0
        assert row["节点名称"]


def test_snapshot_terminal_state():
    snapshot = proxy_guard.snapshot()
    assert snapshot["terminal_code"] in (10, 20, 30, 40)
    assert snapshot["current_receipts"] >= 1
    assert snapshot["conflicts"] >= 1
    assert snapshot["supported_payload_versions"] == [1, 2]


# ── 反封锁模块入口与联动链路 ──

def test_anti_block_service_consumes_switch_plans():
    """反封锁模块接收并展开处理项承载的代理切换计划。"""
    site = "guard-antiblock.example"
    _add_rate_rule(site, "3")
    proxy_guard.submit_plan([{"站点": site, "目标节点": "华东-edge-01"}])
    svc = AntiBlockService()
    result = svc.consume_switch_plans()
    assert result["ok"] is True
    assert result["receipts"] == 1
    receipt = _current_receipt(site)
    assert receipt is not None and receipt["最终每秒上限"] == 3.0
    snapshot = svc.domain_snapshot()
    assert snapshot["guard_receipts"] >= 1
    assert snapshot["guard_terminal_code"] in (10, 20, 30, 40)


def test_pipeline_guard_linkage_terminal_state():
    result = pipeline_proxy_guard_linkage()
    assert result.ok is True
    assert "terminal_state" in result.data
    assert "terminal_code" in result.data
    assert result.data["proxy_online"] >= 1
    modules_in_trace = {t.split(":")[0] for t in result.trace}
    assert {"proxy_router", "rate_limiter", "anti_block"}.issubset(modules_in_trace)


def test_demo_plan_from_seed_data():
    """示例计划：基于种子数据生成并完整走通联动。"""
    result = proxy_guard.submit_demo_plan()
    assert result["ok"] is True
    assert result["accepted"] >= 1
    run = proxy_guard.process_plans(strategy="strict")
    assert run["ok"] is True
    receipt = _current_receipt("shop.example")
    assert receipt is not None
    # shop.example：频控 8/s 与 5/s、节点防护 1/s → 取更严格值 1/s
    assert receipt["最终每秒上限"] == 1.0
    assert receipt["终态码"] == 30
