<template>
  <section class="page" data-module="proxy_guard">
    <header class="page-head">
      <div>
        <h2>代理防护联动</h2>
        <p class="page-desc">
          反封锁模块接收并展开处理项承载的代理切换计划，重算窗口内防护策略数据，
          节点切换与有效频控共同发布；导出回执只包含当前绑定节点的唯一上限。
        </p>
      </div>
      <div class="page-actions">
        <select v-model="strategy" class="strategy-select" data-testid="strategy-select">
          <option value="strict">冲突取更严格值</option>
          <option value="version">按策略版本重算</option>
        </select>
        <button class="btn" data-testid="submit-demo" type="button" @click="submitDemo">
          提交示例切换计划
        </button>
        <button
          class="btn primary"
          data-testid="process"
          type="button"
          :disabled="processing"
          @click="runProcess"
        >
          {{ processing ? '联动处理中…' : '执行联动处理' }}
        </button>
        <button class="btn" data-testid="export-receipts" type="button" @click="exportReceipts">
          下载防护限流回执
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">处理项</span>
        <strong class="stat-value">{{ snapshot?.items ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">当前回执</span>
        <strong class="stat-value">{{ snapshot?.current_receipts ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">冲突裁决</span>
        <strong class="stat-value">{{ snapshot?.conflicts ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">处理终态</span>
        <strong class="stat-value">
          {{ snapshot ? `${snapshot.terminal_state}（${snapshot.terminal_code}号）` : '—' }}
        </strong>
      </article>
    </div>

    <p v-if="noticeMessage" class="notice-text" data-testid="notice">{{ noticeMessage }}</p>
    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>

    <h3 class="section-title">处理清单</h3>
    <table class="data-table" data-testid="plans-table">
      <thead>
        <tr>
          <th>处理项</th>
          <th>站点</th>
          <th>源节点</th>
          <th>目标节点</th>
          <th>切换原因</th>
          <th>策略版本</th>
          <th>重算窗口</th>
          <th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in plans" :key="item.item_id" :data-testid="`plan-${item.item_id}`">
          <td>{{ item.item_id }}</td>
          <td>{{ item.站点 }}</td>
          <td>{{ item.源节点 || '—' }}</td>
          <td>{{ item.目标节点 }}</td>
          <td>{{ item.切换原因 }}</td>
          <td>{{ item.策略版本 }}</td>
          <td>{{ item.窗口 }}</td>
          <td>{{ item.状态 }}</td>
        </tr>
        <tr v-if="!plans.length">
          <td colspan="8" class="empty-state">暂无切换计划处理项，可先提交示例切换计划</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">结果列表（防护限流回执）</h3>
    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>站点</span>
        <input v-model="receiptFilters.站点" data-testid="filter-site" placeholder="按站点检索" />
      </label>
      <label class="filter-item">
        <span>节点</span>
        <input v-model="receiptFilters.节点" data-testid="filter-node" placeholder="按节点检索" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>
    <table class="data-table" data-testid="receipts-table">
      <thead>
        <tr>
          <th>回执编号</th>
          <th>站点</th>
          <th>绑定节点</th>
          <th>最终每秒上限</th>
          <th>请求间隔</th>
          <th>规则来源</th>
          <th>冲突处理</th>
          <th>处理终态</th>
          <th>切换时间</th>
          <th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in receipts" :key="row.receipt_id" :data-testid="`receipt-${row.receipt_id}`">
          <td>{{ row.receipt_id }}</td>
          <td>{{ row.站点 }}</td>
          <td>{{ row.节点名称 }}</td>
          <td>{{ row.最终每秒上限 }}</td>
          <td>{{ row.请求间隔 }}</td>
          <td>{{ row.规则来源 }}</td>
          <td>{{ row.冲突处理 }}</td>
          <td>{{ row.处理终态 }}（{{ row.终态码 }}号）</td>
          <td>{{ formatTime(row.切换时间) }}</td>
          <td>{{ row.状态 }}</td>
        </tr>
        <tr v-if="!receipts.length">
          <td colspan="10" class="empty-state">暂无限流回执，执行联动处理后生成</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ receipts.length }} 条回执记录</span>
      <span class="foot-hint">其余业务页面（反封锁、频控策略、代理路由）随处理结果联动刷新</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  downloadGuardReceipts,
  listGuardPlans,
  listGuardReceipts,
  loadGuardSnapshot,
  processGuardPlans,
  submitGuardDemoPlan,
} from '@/api/backend-service'
import type { GuardPlanItem, GuardReceipt, GuardSnapshot } from '@/data/types'

const plans = ref<GuardPlanItem[]>([])
const receipts = ref<GuardReceipt[]>([])
const snapshot = ref<GuardSnapshot | null>(null)
const strategy = ref('strict')
const processing = ref(false)
const noticeMessage = ref('')
const errorMessage = ref('')
const receiptFilters = reactive<{ 站点: string; 节点: string }>({ 站点: '', 节点: '' })

function formatTime(value: string): string {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

/** 处理清单和结果列表一起变化：一次刷新同时拉取清单、回执与快照。 */
async function reload() {
  errorMessage.value = ''
  try {
    const [planPage, receiptPage, snap] = await Promise.all([
      listGuardPlans(),
      listGuardReceipts(receiptFilters),
      loadGuardSnapshot(),
    ])
    plans.value = planPage.items
    receipts.value = receiptPage.items
    snapshot.value = snap
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '联动数据读取失败'
  }
}

function resetFilters() {
  receiptFilters.站点 = ''
  receiptFilters.节点 = ''
  void reload()
}

async function submitDemo() {
  noticeMessage.value = ''
  errorMessage.value = ''
  try {
    const result = await submitGuardDemoPlan()
    if (!result.ok) throw new Error(result.message)
    noticeMessage.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '切换计划提交失败'
  }
}

async function runProcess() {
  // 并发执行只接受首个成功结果：处理中禁止重复触发，后端同样拒绝并发。
  if (processing.value) return
  processing.value = true
  noticeMessage.value = ''
  errorMessage.value = ''
  try {
    const result = await processGuardPlans(strategy.value)
    if (!result.ok) throw new Error(result.message)
    noticeMessage.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '联动处理失败'
  } finally {
    processing.value = false
  }
}

async function exportReceipts() {
  errorMessage.value = ''
  try {
    await downloadGuardReceipts()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回执导出失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.strategy-select {
  padding: 6px 10px;
  border: 1px solid var(--border, #d0d7de);
  border-radius: 6px;
  background: #fff;
  font-size: 13px;
}

.section-title {
  margin: 18px 0 8px;
  font-size: 15px;
}

.notice-text {
  color: #1a7f37;
  font-size: 13px;
  margin: 8px 0;
}

.foot-hint {
  color: #888;
  font-size: 12px;
}
</style>
