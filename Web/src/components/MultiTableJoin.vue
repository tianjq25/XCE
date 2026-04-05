<template>
  <div class="query-plan-container">
    <div class="plan-panels">
      <section class="plan-panel" :class="{ 'plan-panel--worse': worseSide === 'left' }" aria-label="Left query plan">
        <header class="plan-panel__header">
          <span class="plan-panel__title">Query plan</span>
          <el-tag size="small" type="primary">{{ methodLeft }}</el-tag>
          <el-tag v-if="worseSide === 'left'" size="small" type="danger">
            Slower (+{{ ((latencyLeft ?? 0) - (latencyRight ?? 0)).toFixed(2) }} ms)
          </el-tag>
          <el-tag v-else-if="worseSide === 'right' && latencyLeft !== null" size="small" type="success">Faster</el-tag>
          <span v-if="latencyLeft !== null" class="plan-panel__latency">
            {{ latencyLeft.toFixed(2) }} ms
          </span>
        </header>
        <div class="plan-panel__body">
          <ExecutionPlanTree
            v-if="executionTreeLeft"
            :tree="executionTreeLeft"
            :cardinality-map="cardMapLeft"
            :other-cardinality-map="cardMapRight"
            :method-name="methodLeft"
            :other-method-name="methodRight"
            :is-worse-plan="worseSide === 'left'"
          />
          <div v-else class="plan-panel__empty">Submit a query to view the left method's execution plan</div>
        </div>
      </section>
      <section class="plan-panel" :class="{ 'plan-panel--worse': worseSide === 'right' }" aria-label="Right query plan">
        <header class="plan-panel__header">
          <span class="plan-panel__title">Query plan</span>
          <el-tag size="small" type="success">{{ methodRight }}</el-tag>
          <el-tag v-if="worseSide === 'right'" size="small" type="danger">
            Slower (+{{ ((latencyRight ?? 0) - (latencyLeft ?? 0)).toFixed(2) }} ms)
          </el-tag>
          <el-tag v-else-if="worseSide === 'left' && latencyRight !== null" size="small" type="success">Faster</el-tag>
          <span v-if="latencyRight !== null" class="plan-panel__latency">
            {{ latencyRight.toFixed(2) }} ms
          </span>
        </header>
        <div class="plan-panel__body">
          <ExecutionPlanTree
            v-if="executionTreeRight"
            :tree="executionTreeRight"
            :cardinality-map="cardMapRight"
            :other-cardinality-map="cardMapLeft"
            :method-name="methodRight"
            :other-method-name="methodLeft"
            :is-worse-plan="worseSide === 'right'"
          />
          <div v-else class="plan-panel__empty">Submit a query to view the right method's execution plan</div>
        </div>
      </section>
    </div>

    <div class="bottom-section">
      <div class="control-bar">
        <div class="control-bar__group control-bar__group--job">
          <span class="control-bar__label">JOB query</span>
          <el-select
            v-model="selectedJobId"
            placeholder="Select JOB query"
            class="control-bar__select control-bar__select--job"
            filterable
            :loading="jobListLoading"
            :disabled="!!jobListError"
            @change="handleJobChange"
          >
            <el-option
              v-for="q in jobQueries"
              :key="q.id"
              :label="`${q.name}`"
              :value="q.id"
            />
          </el-select>
          <span v-if="jobListError" class="control-bar__hint control-bar__hint--error">{{ jobListError }}</span>
        </div>
        <div class="control-bar__group">
          <span class="control-bar__label">Method (left)</span>
          <el-select v-model="methodLeft" placeholder="Cardinality method" class="control-bar__select">
            <el-option
              v-for="type in CARDINALITY_ESTIMATION_METHODS"
              :key="`L-${type}`"
              :label="type"
              :value="type"
            />
          </el-select>
        </div>
        <div class="control-bar__group">
          <span class="control-bar__label">Method (right)</span>
          <el-select v-model="methodRight" placeholder="Cardinality method" class="control-bar__select">
            <el-option
              v-for="type in CARDINALITY_ESTIMATION_METHODS"
              :key="`R-${type}`"
              :label="type"
              :value="type"
            />
          </el-select>
        </div>
        <div class="control-bar__actions">
          <el-button type="primary" :loading="submitting" :disabled="!!jobListError"
            @click="handleSubmit">
            Submit query
          </el-button>
        </div>
      </div>

      <div class="sql-preview sql-preview--formatted">
        <pre><code class="hljs sql" v-html="highlightedQuery"></code></pre>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import hljs from 'highlight.js/lib/core'
import sql from 'highlight.js/lib/languages/sql'
import 'highlight.js/styles/github.css'
import { ElMessage } from 'element-plus'
import api from '../api'
import type { JoinQueryResponse, CardinalityEntry } from '../api'
import ExecutionPlanTree from './ExecutionPlanTree.vue'
import { loadJobQueries, type JobQueryEntry } from '../data/jobQueries'
import { formatJobSql } from '../utils/formatSql'

hljs.registerLanguage('sql', sql)

const CARDINALITY_ESTIMATION_METHODS = [
  'Postgres',
  'TrueCard',
  'ASM',
  'ASM+XCE(offline)',
  'ASM+XCE(online)'
] as const
type CardinalityEstimationMethod = (typeof CARDINALITY_ESTIMATION_METHODS)[number]

const methodLeft = ref<CardinalityEstimationMethod>('Postgres')
const methodRight = ref<CardinalityEstimationMethod>('TrueCard')

const latencyLeft = ref<number | null>(null)
const latencyRight = ref<number | null>(null)
const submitting = ref(false)

const jobQueries = ref<JobQueryEntry[]>([])
const jobListLoading = ref(true)
const jobListError = ref<string | null>(null)
const selectedJobId = ref<number>(1)

const currentSql = computed(() => {
  const q = jobQueries.value.find(j => j.id === selectedJobId.value)
  return q?.sql ?? ''
})

/** Format with sql-formatter then highlight with highlight.js */
const highlightedQuery = computed(() => {
  const raw = currentSql.value?.trim() ?? ''
  const text = raw ? formatJobSql(raw) : '-- (no SQL)'
  try {
    return hljs.highlight(text, { language: 'sql' }).value
  } catch {
    return hljs.highlightAuto(text).value
  }
})

const handleJobChange = () => {
  executionTreeLeft.value = null
  executionTreeRight.value = null
  latencyLeft.value = null
  latencyRight.value = null
  cardMapLeft.value = {}
  cardMapRight.value = {}
}

const getMethodNumber = (m: CardinalityEstimationMethod): number => {
  switch (m) {
    case 'Postgres':
      return 0
    case 'TrueCard':
      return 1
    case 'ASM':
      return 2
    case 'ASM+XCE(offline)':
      return 3
    case 'ASM+XCE(online)':
      return 4
  }
}

const executionTreeLeft = ref<Record<string, any> | null>(null)
const executionTreeRight = ref<Record<string, any> | null>(null)
const cardMapLeft = ref<Record<string, CardinalityEntry>>({})
const cardMapRight = ref<Record<string, CardinalityEntry>>({})

const worseSide = computed<'left' | 'right' | null>(() => {
  if (latencyLeft.value == null || latencyRight.value == null) return null
  if (latencyLeft.value > latencyRight.value) return 'left'
  if (latencyRight.value > latencyLeft.value) return 'right'
  return null
})

const handleSubmit = async () => {
  const entry = jobQueries.value.find(j => j.id === selectedJobId.value)
  const sqlText = entry?.sql?.trim() ?? ''
  if (!sqlText) {
    ElMessage.error('No valid SQL for this JOB query. Check public/job-queries.json')
    return
  }

  const qName = entry?.name ?? ''
  if (!qName) {
    ElMessage.error('Query name is missing in job-queries.json')
    return
  }

  const idxL = getMethodNumber(methodLeft.value)
  const idxR = getMethodNumber(methodRight.value)

  submitting.value = true
  executionTreeLeft.value = null
  executionTreeRight.value = null
  latencyLeft.value = null
  latencyRight.value = null
  cardMapLeft.value = {}
  cardMapRight.value = {}

  try {
    const [leftSettled, rightSettled] = await Promise.allSettled([
      api.submitJobJoinQuery({ method: idxL, qName, sql: sqlText }),
      api.submitJobJoinQuery({ method: idxR, qName, sql: sqlText })
    ])

    const applyResponse = (
      settled: PromiseSettledResult<JoinQueryResponse>,
      side: 'left' | 'right'
    ) => {
      if (settled.status === 'fulfilled') {
        const res = settled.value
        if (side === 'left') {
          latencyLeft.value = res.latency
          executionTreeLeft.value = res.tree ?? null
          cardMapLeft.value = res.cardinality_map ?? {}
        } else {
          latencyRight.value = res.latency
          executionTreeRight.value = res.tree ?? null
          cardMapRight.value = res.cardinality_map ?? {}
        }
        return res
      }
      const msg =
        settled.reason?.response?.data?.message ??
        settled.reason?.message ??
        'Request failed'
      ElMessage.error(`${side === 'left' ? 'Left' : 'Right'} plan: ${msg}`)
      return null
    }

    const resL = applyResponse(leftSettled, 'left')
    const resR = applyResponse(rightSettled, 'right')

    if (resL || resR) {
      ElMessage.success('Query finished')
    }
  } catch (error: unknown) {
    const err = error as { message?: string }
    ElMessage.error(err.message ?? 'Query submission failed')
    console.error(error)
  } finally {
    submitting.value = false
  }
}

onMounted(async () => {
  jobListLoading.value = true
  jobListError.value = null
  try {
    const list = await loadJobQueries()
    jobQueries.value = list
    if (list.length !== 113) {
      console.warn(`job-queries.json has ${list.length} entries, expected 113`)
    }
    if (!list.some(q => q.id === selectedJobId.value)) {
      selectedJobId.value = list[0]?.id ?? 1
    }
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : String(e)
    jobListError.value = msg
    ElMessage.error(msg)
  } finally {
    jobListLoading.value = false
  }
})
</script>

<style scoped>
.query-plan-container {
  display: flex;
  flex-direction: column;
  height: 100%;
  min-height: 0;
  padding: 16px;
  gap: 16px;
  box-sizing: border-box;
  overflow-y: auto;
}

.plan-panels {
  flex: 1;
  min-height: 600px;
  display: flex;
  gap: 16px;
  min-width: 0;
}

.plan-panel {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  border: 1px solid var(--el-border-color);
  border-radius: 8px;
  background: var(--el-bg-color);
  overflow: hidden;
}

.plan-panel__header {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding: 10px 12px;
  border-bottom: 1px solid var(--el-border-color-lighter);
  background: var(--el-fill-color-light);
}

.plan-panel__title {
  font-weight: 600;
  font-size: 14px;
  color: var(--el-text-color-primary);
}

.plan-panel__latency {
  margin-left: auto;
  font-size: 13px;
  font-variant-numeric: tabular-nums;
  color: var(--el-text-color-secondary);
}

.plan-panel--worse {
  border-color: #fca5a5;
}

.plan-panel__body {
  flex: 1;
  min-height: 420px;
  position: relative;
  overflow: hidden;
  padding: 0;
}

.plan-panel__empty {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  min-height: 200px;
  padding: 16px;
  text-align: center;
  font-size: 13px;
  color: var(--el-text-color-secondary);
}

.bottom-section {
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.control-bar {
  display: flex;
  flex-wrap: wrap;
  align-items: flex-end;
  gap: 12px 20px;
  padding: 12px 14px;
  border: 1px solid var(--el-border-color);
  border-radius: 8px;
  background: var(--el-fill-color-light);
}

.control-bar__group {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.control-bar__group--job {
  min-width: 200px;
}

.control-bar__label {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.control-bar__hint {
  font-size: 11px;
  line-height: 1.3;
}

.control-bar__hint--error {
  color: var(--el-color-danger);
}

.control-bar__select {
  width: 180px;
}

.control-bar__select--job {
  width: 220px;
}

.control-bar__actions {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 12px;
}

.sql-preview--formatted {
  border: 1px solid var(--el-border-color);
  border-radius: 8px;
  padding: 12px 14px;
  max-height: min(420px, 45vh);
  overflow: auto;
  background: #ffffff;
}

.sql-preview--formatted pre {
  margin: 0;
  font-family: ui-monospace, 'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace;
  font-size: 12px;
  line-height: 1.5;
  white-space: pre;
  word-break: normal;
  overflow-x: auto;
}

/* Light bg, keywords purple, functions orange, strings green, body dark grey */
.sql-preview--formatted :deep(.hljs) {
  background: transparent;
  color: #24292f;
}

.sql-preview--formatted :deep(.hljs-keyword) {
  color: #6f42c1;
  font-weight: 500;
}

.sql-preview--formatted :deep(.hljs-built_in),
.sql-preview--formatted :deep(.hljs-type) {
  color: #d97706;
}

.sql-preview--formatted :deep(.hljs-string) {
  color: #22863a;
}

.sql-preview--formatted :deep(.hljs-number),
.sql-preview--formatted :deep(.hljs-literal) {
  color: #0550ae;
}

.sql-preview--formatted :deep(.hljs-comment) {
  color: #6a737d;
  font-style: italic;
}
</style>
