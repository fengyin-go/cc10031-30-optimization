"""代理防护联动接口：切换计划受理、联动处理、限流回执查询与导出。

与通用资源路由并存，兼容既有版本边界：只做新增端点，不改写既有
/api/{module} 行为。注册顺序需在资源路由之前。
"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.proxy_guard import STRATEGY_MODES, proxy_guard

router = APIRouter(prefix="/api/guard", tags=["代理防护联动"])

RECEIPT_COLUMNS = [
    "回执编号", "站点", "节点ID", "节点名称", "最终每秒上限", "请求间隔",
    "规则来源", "冲突处理", "处理终态", "终态码", "策略版本", "切换时间", "重算窗口",
]

_RECEIPT_KEY_MAP = {
    "回执编号": "receipt_id",
    "节点ID": "节点ID",
    "节点名称": "节点名称",
    "最终每秒上限": "最终每秒上限",
    "请求间隔": "请求间隔",
    "规则来源": "规则来源",
    "冲突处理": "冲突处理",
    "处理终态": "处理终态",
    "终态码": "终态码",
    "策略版本": "策略版本",
    "切换时间": "切换时间",
    "重算窗口": "重算窗口",
}


class SwitchPlanPayload(BaseModel):
    """代理切换计划：处理项列表 + 负载版本（兼容既有版本边界）。"""
    items: list[dict[str, Any]] = Field(default_factory=list)
    版本: int = 2
    备注: str | None = None


@router.get("/plans")
def list_plans() -> dict[str, Any]:
    """处理清单：全部切换计划处理项及其展开状态。"""
    items = proxy_guard.list_plans()
    return {"items": items, "total": len(items)}


@router.post("/plans")
def submit_plan(payload: SwitchPlanPayload) -> dict[str, Any]:
    """提交代理切换计划：处理项承载，发布到通道等待反封锁模块接收。"""
    if not payload.items:
        return {"ok": False, "message": "切换计划为空，未受理任何处理项",
                "accepted": 0, "rejected": 0, "items": []}
    return proxy_guard.submit_plan(payload.items, payload_version=payload.版本)


@router.post("/plans/demo")
def submit_demo_plan() -> dict[str, Any]:
    """按当前节点与频控规则生成示例切换计划。"""
    return proxy_guard.submit_demo_plan()


@router.post("/process")
def process_plans(strategy: str = Query(default="strict")) -> dict[str, Any]:
    """反封锁模块接收并展开处理项，重算防护策略并生成限流回执。

    并发执行只接受首个成功结果；strategy=strict 取更严格值，
    strategy=version 按节点绑定的策略版本重新计算上限。
    """
    if strategy not in STRATEGY_MODES:
        return {"ok": False,
                "message": f"未知冲突裁决策略：{strategy}，可选 {list(STRATEGY_MODES)}",
                "processed": 0}
    return proxy_guard.process_plans(strategy=strategy)


@router.get("/receipts")
def list_receipts(
    站点: str | None = Query(default=None),
    节点: str | None = Query(default=None),
    仅当前: bool = Query(default=False),
) -> dict[str, Any]:
    """结果列表：按站点和节点查看最终上限、规则来源与切换时间。"""
    items = proxy_guard.list_receipts(site=站点, node=节点, current_only=仅当前)
    return {"items": items, "total": len(items)}


@router.get("/receipts/export")
def export_receipts() -> StreamingResponse:
    """导出防护限流回执数据文件（CSV，本地交付）。

    导出回执只包含当前绑定节点的唯一上限，可逐条核对代理记录的
    最终请求边界。
    """
    rows = proxy_guard.export_rows()
    output = io.StringIO()
    output.write("﻿")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(RECEIPT_COLUMNS)
    for row in rows:
        writer.writerow([
            _csv_safe(row.get(_RECEIPT_KEY_MAP.get(column, column), ""))
            for column in RECEIPT_COLUMNS
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="guard_receipts.csv"'},
    )


@router.get("/snapshot")
def guard_snapshot() -> dict[str, Any]:
    """联动快照：处理项、回执、冲突与当前处理终态。"""
    return proxy_guard.snapshot()


def _csv_safe(value: Any) -> Any:
    """与通用导出一致的公式注入防护。"""
    text = str(value or "")
    if text.startswith(("=", "+", "-", "@")):
        return "'" + text
    return value if text else value
