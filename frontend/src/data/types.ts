/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 代理防护联动：处理项承载的切换计划条目 */
export type GuardPlanItem = {
  item_id: string
  plan_id: string
  站点: string
  源节点: string
  目标节点: string
  切换原因: string
  策略版本: string
  窗口: string
  负载版本: number
  状态: string
  提交时间: string
}

/** 代理防护联动：防护限流回执 */
export type GuardReceipt = {
  receipt_id: string
  站点: string
  节点ID: number
  节点名称: string
  最终每秒上限: number
  请求间隔: string
  规则来源: string
  冲突处理: string
  处理终态: string
  终态码: number
  策略版本: string
  切换时间: string
  重算窗口: string
  状态: string
}

/** 代理防护联动：快照 */
export type GuardSnapshot = {
  plans: number
  items: number
  items_by_status: Record<string, number>
  receipts: number
  current_receipts: number
  superseded: number
  conflicts: number
  terminal_state: string
  terminal_code: number
  strategy_modes: string[]
  supported_payload_versions: number[]
}

/** 代理防护联动：处理结果 */
export type GuardProcessResult = {
  ok: boolean
  message: string
  processed?: number
  receipts?: number
  conflicts?: number
  rejected?: number
  terminal_state?: string
  terminal_code?: number
}
