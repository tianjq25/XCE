<template>
  <div class="dt-page">
    <!-- ─── Control Bar ─── -->
    <div class="dt-controls">
      <div class="dt-controls__row">
        <div class="dt-field">
          <span class="dt-field__label">Model Type</span>
          <el-radio-group v-model="modelType" size="small" @change="handleTypeChange">
            <el-radio-button value="offline">Offline</el-radio-button>
            <el-radio-button value="online">Online</el-radio-button>
          </el-radio-group>
        </div>

        <div v-if="modelType === 'online'" class="dt-field">
          <span class="dt-field__label">Query</span>
          <el-select
            v-model="selectedModelId"
            placeholder="Select query"
            class="dt-field__select"
            :loading="loadingModels"
            @change="handleModelChange"
          >
            <el-option
              v-for="m in onlineModels"
              :key="m.id"
              :value="m.id"
              :label="m.query_name"
            >
              <span style="font-weight:600">{{ m.query_name }}</span>
              <span style="color:#909399;margin-left:8px;font-size:12px">{{ m.training_samples }} samples</span>
            </el-option>
          </el-select>
        </div>

        <div class="dt-field">
          <span class="dt-field__label">Static Key</span>
          <el-select
            v-model="selectedStaticKey"
            placeholder="Select table group"
            class="dt-field__select dt-field__select--wide"
            filterable
            :disabled="staticKeys.length === 0"
            @change="handleKeyChange"
          >
            <el-option v-for="k in staticKeys" :key="k" :label="k" :value="k">
              <span style="font-weight:600">{{ k }}</span>
              <span v-if="modelData?.trees[k]" style="color:#909399;margin-left:8px;font-size:11px">
                {{ calcStats(modelData.trees[k]).totalNodes }} nodes, depth {{ calcStats(modelData.trees[k]).depth }}
              </span>
            </el-option>
          </el-select>
        </div>

        <div class="dt-controls__tags" v-if="modelData">
          <el-tag size="small" effect="plain" round>
            Samples {{ modelData.training_samples }}
          </el-tag>
          <el-tag v-if="treeStats" size="small" effect="plain" round>
            Depth {{ treeStats.depth }}
          </el-tag>
          <el-tag v-if="treeStats" size="small" effect="plain" round>
            {{ treeStats.totalNodes }} nodes / {{ treeStats.leafNodes }} leaves
          </el-tag>
        </div>
      </div>

      <div v-if="modelData?.description" class="dt-controls__desc">
        {{ modelData.description }}
      </div>
    </div>

    <!-- ─── Legend ─── -->
    <div class="dt-legend" v-if="currentTree">
      <span class="dt-legend__item">
        <span class="dt-legend__dot dt-legend__dot--split"></span>Split Node
      </span>
      <!-- <span class="dt-legend__item">
        <span class="dt-legend__dot dt-legend__dot--leaf-lo"></span>Leaf (σ²&lt;0.5)
      </span>
      <span class="dt-legend__item">
        <span class="dt-legend__dot dt-legend__dot--leaf-mid"></span>Leaf (0.5≤σ²&lt;3)
      </span>
      <span class="dt-legend__item">
        <span class="dt-legend__dot dt-legend__dot--leaf-hi"></span>Leaf (σ²≥3)
      </span> -->
      <span class="dt-legend__item">
        <span class="dt-legend__dot dt-legend__dot--leaf-lo"></span>Leaf (σ² &lt; 3 and N ≥ 5)
      </span>
      <span class="dt-legend__item">
        <span class="dt-legend__dot dt-legend__dot--leaf-hi"></span>Leaf (σ² ≥ 3 or N &lt; 5)
      </span>
      <span class="dt-legend__sep"></span>
      <span class="dt-legend__hint">L = condition met &nbsp;|&nbsp; R = not met</span>
    </div>

    <!-- ─── Main ─── -->
    <div class="dt-body" v-loading="loadingData">
      <div v-if="currentTree" class="dt-canvas-wrap">
        <div class="dt-canvas" ref="chartEl"></div>
        <div class="dt-toolbar">
          <button class="dt-toolbar__btn" title="Zoom In" @click="zoomIn">+</button>
          <button class="dt-toolbar__btn" title="Zoom Out" @click="zoomOut">&minus;</button>
          <button class="dt-toolbar__btn" title="Reset" @click="zoomReset">&#8634;</button>
        </div>
      </div>
      <div v-else class="dt-empty">
        <span v-if="loadingData">Loading...</span>
        <span v-else>Select a model and static key to view the decision tree</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'
import { ElMessage } from 'element-plus'
import api from '../api'
import type { DTModelMeta, DTDataResponse, DTNode } from '../api'

// ── state ──
const models = ref<DTModelMeta[]>([])
const modelType = ref<'offline' | 'online'>('offline')
const selectedModelId = ref('')
const selectedStaticKey = ref('')
const modelData = ref<DTDataResponse | null>(null)
const loadingModels = ref(false)
const loadingData = ref(false)
const chartEl = ref<HTMLElement | null>(null)
let chart: echarts.ECharts | null = null

const offlineModel = computed(() => models.value.find(m => m.type === 'offline'))
const onlineModels = computed(() => models.value.filter(m => m.type === 'online'))

const staticKeys = computed(() => {
  if (!modelData.value?.trees) return []
  return sortKeysByComplexity(modelData.value.trees)
})

const currentTree = computed<DTNode | null>(() => {
  if (!modelData.value?.trees || !selectedStaticKey.value) return null
  return modelData.value.trees[selectedStaticKey.value] ?? null
})

interface TreeStats { depth: number; totalNodes: number; leafNodes: number }

function calcStats(node: DTNode | null | undefined, d = 0): TreeStats {
  if (!node) return { depth: 0, totalNodes: 0, leafNodes: 0 }
  if (node.is_leaf) return { depth: d + 1, totalNodes: 1, leafNodes: 1 }
  const l = calcStats(node.left, d + 1)
  const r = calcStats(node.right, d + 1)
  return {
    depth: Math.max(l.depth, r.depth),
    totalNodes: 1 + l.totalNodes + r.totalNodes,
    leafNodes: l.leafNodes + r.leafNodes,
  }
}

function sortKeysByComplexity(trees: Record<string, DTNode>): string[] {
  return Object.keys(trees).sort((a, b) => {
    const sa = calcStats(trees[a])
    const sb = calcStats(trees[b])
    if (sb.totalNodes !== sa.totalNodes) return sb.totalNodes - sa.totalNodes
    if (sb.depth !== sa.depth) return sb.depth - sa.depth
    return a.localeCompare(b)
  })
}

const treeStats = computed<TreeStats | null>(() =>
  currentTree.value ? calcStats(currentTree.value) : null
)

// ── colour helpers ──
const SPLIT_BG   = '#e8f0fe'
const SPLIT_BD   = '#5b9bd5'
const LEAF_LO_BG = '#e6f7ee'
const LEAF_LO_BD = '#52c41a'
// const LEAF_MI_BG = '#fff7e6'
// const LEAF_MI_BD = '#faad14'
const LEAF_HI_BG = '#fff1f0'
const LEAF_HI_BD = '#f5222d'

function leafColors(size: number, v: number) {
  // if (v < 0.5) return { bg: LEAF_LO_BG, bd: LEAF_LO_BD }
  // if (size < 5 || v < 3.0) return { bg: LEAF_HI_BG, bd: LEAF_HI_BD }
  if (size < 5 || v >= 3.0) return { bg: LEAF_HI_BG, bd: LEAF_HI_BD }
  return { bg: LEAF_LO_BG, bd: LEAF_LO_BD }
}

function fmtNum(x: number): string {
  return Number.isInteger(x) ? String(x) : x.toFixed(4)
}

// ── ECharts tree data ──
let nodeCounter = 0

function convertNode(node: DTNode, branch?: string): any {
  nodeCounter++
  if (node.is_leaf) {
    const { bg, bd } = leafColors(node.size, node.variance)
    return {
      name: `leaf_${nodeCounter}`,
      _raw: node,
      _branch: branch,
      _isLeaf: true,
      symbol: 'roundRect',
      symbolSize: [14, 14],
      itemStyle: { color: bg, borderColor: bd, borderWidth: 2, shadowBlur: 3, shadowColor: 'rgba(0,0,0,0.08)' },
      label: { color: '#374151' },
      children: [],
    }
  }

  const result: any = {
    name: `split_${nodeCounter}`,
    _raw: node,
    _branch: branch,
    _isLeaf: false,
    symbol: 'circle',
    symbolSize: 12,
    itemStyle: { color: SPLIT_BG, borderColor: SPLIT_BD, borderWidth: 2, shadowBlur: 4, shadowColor: 'rgba(0,0,0,0.10)' },
    label: { color: '#1f2937' },
    children: [],
  }
  if (node.left) result.children.push(convertNode(node.left, 'L'))
  if (node.right) result.children.push(convertNode(node.right, 'R'))
  return result
}

function initChart() {
  if (!chartEl.value || !currentTree.value) return
  if (chart) chart.dispose()

  nodeCounter = 0
  chart = echarts.init(chartEl.value)
  const data = convertNode(currentTree.value)

  const option: echarts.EChartsOption = {
    tooltip: {
      trigger: 'item',
      triggerOn: 'mousemove',
      backgroundColor: '#fff',
      borderColor: '#e5e7eb',
      borderWidth: 1,
      padding: [12, 16],
      textStyle: { color: '#1f2937', fontSize: 12, lineHeight: 18 },
      extraCssText: 'box-shadow:0 4px 16px rgba(0,0,0,0.10);max-width:440px;white-space:pre-wrap;border-radius:8px;',
      formatter: (params: any) => {
        const d = params.data
        if (!d?._raw) return ''
        const n: DTNode = d._raw
        const s: string[] = []

        if (d._branch) {
          const tag = d._branch === 'L'
            ? '<span style="background:#e8f0fe;color:#2563eb;padding:1px 6px;border-radius:3px;font-size:11px;font-weight:600">L Satisfied</span>'
            : '<span style="background:#fef3cd;color:#b45309;padding:1px 6px;border-radius:3px;font-size:11px;font-weight:600">R Not Satisfied</span>'
          s.push(tag)
          s.push('')
        }

        if (n.is_leaf) {
          s.push('<b style="font-size:13px">Leaf Node</b>')
          s.push(`<span style="color:#6b7280">━━━━━━━━━━━━━━━━━━</span>`)
          s.push(`Samples: <b>${n.size}</b>`)
          // s.push(`Mean \u03bc: <b>${fmtNum(Math.exp(n.mean))}</b>`)
          s.push(`Estimation Bias: <b>${fmtNum(Math.min(Math.exp(n.mean), 2))}</b>`)
          s.push(`Variance \u03c3\u00b2: <b>${fmtNum(n.variance)}</b>`)
          if (n.min != null && n.max != null)
            s.push(`Range: [${fmtNum(n.min)}, ${fmtNum(n.max)}]`)

          if (n.candidate_splits?.length) {
            const top = n.candidate_splits.slice().sort((a, b) => b.score - a.score).slice(0, 5)
            s.push('')
            s.push(`<b>Top ${top.length} Candidate Splits</b>`)
            top.forEach((sp, i) => {
              s.push(`<span style="color:#6b7280">${i + 1}.</span> ${sp.condition}`)
              s.push(`&nbsp;&nbsp;&nbsp;<span style="color:#9ca3af">L:${sp.left_size} R:${sp.right_size} score:${sp.score}</span>`)
            })
            if (n.candidate_splits.length > 5)
              s.push(`<span style="color:#bbb">${n.candidate_splits.length - 5} more...</span>`)
          }
        } else {
          s.push('<b style="font-size:13px">Split Node</b>')
          s.push(`<span style="color:#6b7280">━━━━━━━━━━━━━━━━━━</span>`)
          s.push(`Condition: ${n.split_condition}`)
          if (n.split_type) s.push(`Type: ${n.split_type}`)
          s.push(`Samples: <b>${n.size}</b>`)
          // s.push(`Mean \u03bc: <b>${fmtNum(Math.exp(n.mean))}</b>`)
          s.push(`Estimation Bias: <b>${fmtNum(Math.min(Math.exp(n.mean), 2))}</b>`)
          s.push(`Variance \u03c3\u00b2: <b>${fmtNum(n.variance)}</b>`)
        }
        return s.join('<br/>')
      },
    },
    series: [
      {
        type: 'tree',
        orient: 'LR',
        initialTreeDepth: -1,
        data: [data],
        top: '4%',
        left: '10%',
        bottom: '4%',
        right: '18%',
        symbolSize: 12,
        nodeGap: 28,
        layerPadding: 120,
        layout: 'orthogonal',
        roam: true,
        scaleLimit: { min: 0.15, max: 8 },
        label: {
          position: 'right',
          verticalAlign: 'middle',
          align: 'left',
          fontSize: 11,
          distance: 10,
          formatter: (params: any) => {
            const d = params.data
            if (!d?._raw) return ''
            const n: DTNode = d._raw
            const branchTag = d._branch
              ? (d._branch === 'L' ? '{brL|L} ' : '{brR|R} ')
              : ''

            if (n.is_leaf) {
              return `${branchTag}{leafHead|N=${n.size}  Estimation Bias=${fmtNum(Math.min(Math.exp(n.mean), 2))}\n{leafVar|σ²=${fmtNum(n.variance)}}`
            }
            const cond = n.split_condition ?? ''
            const short = cond.length > 36 ? cond.slice(0, 34) + '…' : cond
            return `${branchTag}{splitCond|${short}}\n{splitStat|N=${n.size}  σ²=${fmtNum(n.variance)}}`
          },
          rich: {
            brL: {
              backgroundColor: '#e8f0fe',
              borderColor: '#93b8ef',
              borderWidth: 1,
              borderRadius: 3,
              padding: [1, 4, 1, 4],
              fontSize: 9,
              fontWeight: 'bold' as const,
              color: '#2563eb',
              lineHeight: 16,
            },
            brR: {
              backgroundColor: '#fef3cd',
              borderColor: '#e6c35c',
              borderWidth: 1,
              borderRadius: 3,
              padding: [1, 4, 1, 4],
              fontSize: 9,
              fontWeight: 'bold' as const,
              color: '#b45309',
              lineHeight: 16,
            },
            splitCond: { fontWeight: 'bold' as const, fontSize: 11, lineHeight: 18, color: '#1e3a5f' },
            splitStat: { fontSize: 10, lineHeight: 15, color: '#7c8db5' },
            leafHead:  { fontSize: 11, lineHeight: 17, color: '#374151', fontWeight: 'bold' as const },
            leafVar:   { fontSize: 10, lineHeight: 14, color: '#9ca3af' },
          },
        },
        leaves: {
          label: {
            position: 'right',
            verticalAlign: 'middle',
            align: 'left',
          },
        },
        edgeShape: 'polyline',
        edgeForkPosition: '40%',
        emphasis: {
          focus: 'descendant',
          itemStyle: { borderColor: '#f59e0b', borderWidth: 3 },
        },
        expandAndCollapse: true,
        animationDuration: 350,
        animationDurationUpdate: 500,
        lineStyle: { width: 1.8, color: '#c6d2de', curveness: 0.2 },
      },
    ],
  }

  chart.setOption(option)
}

// ── zoom helpers ──
function zoomIn() {
  if (!chart) return
  const z = (chart.getOption() as any).series?.[0]?.zoom ?? 1
  chart.setOption({ series: [{ zoom: Math.min(z * 1.3, 8) }] })
}
function zoomOut() {
  if (!chart) return
  const z = (chart.getOption() as any).series?.[0]?.zoom ?? 1
  chart.setOption({ series: [{ zoom: Math.max(z / 1.3, 0.15) }] })
}
function zoomReset() {
  if (!chart) return
  chart.setOption({ series: [{ zoom: 1, center: undefined }] })
}

const handleResize = () => chart?.resize()

// ── data loading ──
async function loadModels() {
  loadingModels.value = true
  try {
    const res = await api.getDTModels()
    models.value = res.models
    if (offlineModel.value) {
      selectedModelId.value = offlineModel.value.id
      await loadModelData(offlineModel.value.id)
    }
  } catch {
    ElMessage.error('Failed to load model list')
  } finally {
    loadingModels.value = false
  }
}

async function loadModelData(modelId: string) {
  loadingData.value = true
  try {
    const res = await api.getDTData(modelId)
    modelData.value = res
    const keys = sortKeysByComplexity(res.trees)
    selectedStaticKey.value = keys.length > 0 ? keys[0] : ''
  } catch {
    ElMessage.error('Failed to load decision tree data')
    modelData.value = null
  } finally {
    loadingData.value = false
  }
}

function handleTypeChange(type: 'offline' | 'online') {
  selectedStaticKey.value = ''
  if (type === 'offline' && offlineModel.value) {
    selectedModelId.value = offlineModel.value.id
    loadModelData(offlineModel.value.id)
  } else if (type === 'online' && onlineModels.value.length > 0) {
    selectedModelId.value = onlineModels.value[0].id
    loadModelData(onlineModels.value[0].id)
  }
}

async function handleModelChange(modelId: string) {
  selectedStaticKey.value = ''
  await loadModelData(modelId)
}

function handleKeyChange() {
  nextTick(() => initChart())
}

watch(currentTree, () => {
  if (currentTree.value) nextTick(() => initChart())
})

onMounted(() => {
  loadModels()
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chart?.dispose()
})
</script>

<style scoped>
.dt-page {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  gap: 0;
  overflow: hidden;
}

/* ─── Control Bar ─── */
.dt-controls {
  flex-shrink: 0;
  padding: 14px 18px 10px;
  background: linear-gradient(135deg, #f7f9fc 0%, #eef2f7 100%);
  border: 1px solid #e4e7ed;
  border-radius: 10px;
  box-shadow: 0 1px 6px rgba(0,0,0,0.04);
}

.dt-controls__row {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.dt-controls__tags {
  display: flex;
  gap: 6px;
  margin-left: auto;
}

.dt-controls__desc {
  margin-top: 6px;
  font-size: 12px;
  color: #909399;
  line-height: 1.5;
}

.dt-field {
  display: flex;
  align-items: center;
  gap: 6px;
}

.dt-field__label {
  font-size: 13px;
  font-weight: 600;
  color: #4b5563;
  white-space: nowrap;
}

.dt-field__select {
  width: 160px;
}

.dt-field__select--wide {
  width: 280px;
}

/* ─── Legend ─── */
.dt-legend {
  flex-shrink: 0;
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 6px 18px;
  font-size: 12px;
  color: #6b7280;
}

.dt-legend__item {
  display: flex;
  align-items: center;
  gap: 4px;
}

.dt-legend__dot {
  width: 10px;
  height: 10px;
  border-radius: 3px;
  border: 1.5px solid;
}

.dt-legend__dot--split {
  border-radius: 50%;
  background: #e8f0fe;
  border-color: #5b9bd5;
}
.dt-legend__dot--leaf-lo { background: #e6f7ee; border-color: #52c41a; }
.dt-legend__dot--leaf-mid{ background: #fff7e6; border-color: #faad14; }
.dt-legend__dot--leaf-hi { background: #fff1f0; border-color: #f5222d; }

.dt-legend__sep {
  width: 1px;
  height: 14px;
  background: #d1d5db;
}

.dt-legend__hint {
  color: #9ca3af;
  font-style: italic;
}

/* ─── Main ─── */
.dt-body {
  flex: 1;
  margin-top: 6px;
  border: 1px solid #e4e7ed;
  border-radius: 10px;
  overflow: hidden;
  position: relative;
  background: #fdfdfe;
}

.dt-canvas-wrap {
  width: 100%;
  height: 100%;
  position: relative;
}

.dt-canvas {
  width: 100%;
  height: 100%;
  min-height: 480px;
}

.dt-toolbar {
  position: absolute;
  top: 8px;
  right: 8px;
  display: flex;
  gap: 4px;
  z-index: 10;
}

.dt-toolbar__btn {
  width: 28px;
  height: 28px;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  background: rgba(255,255,255,0.92);
  color: #374151;
  font-size: 15px;
  line-height: 1;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.15s;
  backdrop-filter: blur(4px);
}

.dt-toolbar__btn:hover {
  background: #f0f4ff;
  border-color: #93b8ef;
  color: #2563eb;
}

.dt-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  min-height: 400px;
  color: #bbb;
  font-size: 14px;
}
</style>
