"""反封锁领域服务 — 防护策略管理，输出 domain_snapshot 供跨模块消费。

数据工程核心：数据清洗与转换链路处理 — 跨模块数据产物
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class AntiBlockService(BaseModuleService):
    MODULE = "anti_block"
    ENTITY = "防护策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略名称", "目标站点", "请求间隔", "IP代理池", "UA轮换", "验证码处理", "Cookie策略"]
    REQUIRED_FIELDS = ["策略名称", "目标站点", "请求间隔"]
    STATUS_ORDER = ["草稿", "已启用", "触发封锁", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已启用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def get_site_protection(self, site: str) -> dict[str, Any]:
        """查询指定站点的反封锁策略覆盖情况。"""
        rows = store.rows(self.MODULE)
        active = [
            {
                "id": r["id"], "name": r.get("策略名称"),
                "interval": r.get("请求间隔"), "ip_pool": r.get("IP代理池"),
                "ua_rotation": r.get("UA轮换"), "captcha": r.get("验证码处理"),
            }
            for r in rows
            if r.get("status") == "已启用" and (site in r.get("目标站点", "") or site == r.get("目标站点"))
        ]
        return {
            "site": site,
            "active_protections": len(active),
            "strategies": active,
            "blocked": sum(1 for r in rows if r.get("status") == "触发封锁"),
        }

    def consume_switch_plans(self, strategy: str = "strict") -> dict[str, Any]:
        """接收并展开处理项承载的代理切换计划（代理防护联动入口）。

        按处理项约定和可导出的防护限流回执数据文件，按最终节点绑定
        更新请求间隔并生成导出记录；冲突裁决策略见 proxy_guard。
        """
        from app.services.proxy_guard import proxy_guard

        return proxy_guard.process_plans(strategy=strategy)

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        from app.services.proxy_guard import proxy_guard

        guard = proxy_guard.snapshot()
        return {
            **base,
            "active_strategies": sum(1 for r in rows if r.get("status") == "已启用"),
            "blocked_count": sum(1 for r in rows if r.get("status") == "触发封锁"),
            "guard_receipts": guard["current_receipts"],
            "guard_terminal_state": guard["terminal_state"],
            "guard_terminal_code": guard["terminal_code"],
        }