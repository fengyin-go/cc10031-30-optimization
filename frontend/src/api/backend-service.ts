import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  EntryRow,
  GuardPlanItem,
  GuardProcessResult,
  GuardReceipt,
  GuardSnapshot,
  OverviewResult,
  PageResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

/* ---------- 代理防护联动：切换计划、限流回执 ---------- */

/** 处理清单：全部切换计划处理项 */
export async function listGuardPlans(): Promise<{ items: GuardPlanItem[]; total: number }> {
  const response = await request('/api/guard/plans')
  if (!response.ok) throw new Error('处理清单读取失败')
  return await response.json()
}

/** 提交示例切换计划（处理项承载，等待反封锁模块接收展开） */
export async function submitGuardDemoPlan(): Promise<GuardProcessResult> {
  const response = await request('/api/guard/plans/demo', { method: 'POST' })
  return await response.json()
}

/** 执行联动处理：并发执行只接受首个成功结果 */
export async function processGuardPlans(strategy: string): Promise<GuardProcessResult> {
  const response = await request(`/api/guard/process?strategy=${encodeURIComponent(strategy)}`, {
    method: 'POST',
  })
  return await response.json()
}

/** 结果列表：按站点和节点查看最终上限、规则来源与切换时间 */
export async function listGuardReceipts(
  filters: { 站点?: string; 节点?: string } = {},
): Promise<{ items: GuardReceipt[]; total: number }> {
  const query = new URLSearchParams()
  if (filters.站点) query.set('站点', filters.站点)
  if (filters.节点) query.set('节点', filters.节点)
  const response = await request(`/api/guard/receipts?${query.toString()}`)
  if (!response.ok) throw new Error('限流回执读取失败')
  return await response.json()
}

/** 联动快照：处理终态与统计 */
export async function loadGuardSnapshot(): Promise<GuardSnapshot> {
  const response = await request('/api/guard/snapshot')
  if (!response.ok) throw new Error('联动快照读取失败')
  return await response.json()
}

/** 下载防护限流回执数据文件（CSV，本地交付，只含当前绑定节点的唯一上限） */
export async function downloadGuardReceipts(): Promise<void> {
  const response = await request('/api/guard/receipts/export')
  if (!response.ok) throw new Error('防护限流回执导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = 'guard_receipts.csv'
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}
