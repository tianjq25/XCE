<template>
  <div class="tree-wrapper">
    <div class="tree-container" ref="treeContainer"></div>
    <div class="tree-toolbar">
      <button class="tree-toolbar__btn" title="Zoom In" @click="zoomIn">+</button>
      <button class="tree-toolbar__btn" title="Zoom Out" @click="zoomOut">&minus;</button>
      <button class="tree-toolbar__btn" title="Reset" @click="zoomReset">&#8634;</button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import * as echarts from 'echarts'
import type { TreeNode, CardinalityEntry } from '../api'

interface Props {
  tree: Record<string, TreeNode>
  cardinalityMap?: Record<string, CardinalityEntry>
  otherCardinalityMap?: Record<string, CardinalityEntry>
  methodName?: string
  otherMethodName?: string
  isWorsePlan?: boolean
}

const props = withDefaults(defineProps<Props>(), {
  cardinalityMap: () => ({}),
  otherCardinalityMap: () => ({}),
  methodName: '',
  otherMethodName: '',
  isWorsePlan: false,
})

const treeContainer = ref<HTMLElement | null>(null)
let chart: echarts.ECharts | null = null

/** 与 series.scaleLimit 一致，用于标签随 zoom 缩放时的上下限 */
const TREE_ZOOM_MIN = 0.2
const TREE_ZOOM_MAX = 6

/**
 * 将基准像素按当前 zoom 缩放，并限制在可读范围内。
 * @param basePx zoom=1 时的像素
 * @param zoom 树图 roam 缩放系数
 * @return 缩放后的像素（整数）
 */
function scaledPx(basePx: number, zoom: number): number {
  const z = Math.max(TREE_ZOOM_MIN, Math.min(TREE_ZOOM_MAX, zoom))
  return Math.max(8, Math.min(30, Math.round(basePx * z)))
}

/**
 * 树节点标签 formatter（与 zoom 无关，供初始 option 与缩放更新共用）
 * @param params ECharts label formatter 参数
 * @return 富文本字符串
 */
function treeLabelFormatter(params: any): string {
  const d = params.data
  if (!d?._shortName) return params.name
  const lines = [`{bold|${d._shortName}}`]
  if (d._timeMs) lines.push(`{time|${d._timeMs}ms}`)
  if (d._estCard != null) lines.push(`{est|${formatCard(d._estCard)}}`)
  return lines.join('\n')
}

/**
 * 根据当前 zoom 生成 series.label / rich，使放大时文字同步变大。
 * @param zoom 当前树图缩放系数
 * @return 可合并进 series[0] 的 label 配置
 */
function buildLabelOptionForZoom(zoom: number): any {
  return {
    position: 'right',
    verticalAlign: 'middle',
    align: 'left',
    fontSize: scaledPx(11, zoom),
    distance: scaledPx(10, zoom),
    formatter: treeLabelFormatter,
    rich: {
      bold: {
        fontWeight: 'bold',
        fontSize: scaledPx(11, zoom),
        lineHeight: scaledPx(16, zoom),
      },
      time: {
        fontSize: scaledPx(10, zoom),
        lineHeight: scaledPx(14, zoom),
        color: '#6b7280',
      },
      est: {
        fontSize: scaledPx(10, zoom),
        lineHeight: scaledPx(14, zoom),
        color: '#2563eb',
        fontWeight: 'bold',
      },
    },
  }
}

/**
 * 写入 zoom 并同步更新标签字号（工具栏与 roam 共用）
 * @param nextZoom 目标缩放系数
 */
function applyTreeZoom(nextZoom: number) {
  if (!chart) return
  const z = Math.max(TREE_ZOOM_MIN, Math.min(TREE_ZOOM_MAX, nextZoom))
  chart.setOption({
    series: [{ zoom: z, label: buildLabelOptionForZoom(z) }],
  })
}

/**
 * 从当前 option 读取树 series 的 zoom
 * @return 缩放系数，缺省时为 1
 */
function getCurrentTreeZoom(): number {
  if (!chart) return 1
  const opt = chart.getOption() as any
  return opt.series?.[0]?.zoom ?? 1
}

function stripBrackets(name: string): { short: string; tables: string } {
  const m = name.match(/^(.+?)\s*\[(.+)\]$/)
  if (m) return { short: m[1].trim(), tables: m[2].trim() }
  return { short: name, tables: '' }
}

function formatCard(x: number): string {
  if (x >= 1e9) return `${(x / 1e9).toFixed(1)}B`
  if (x >= 1e6) return `${(x / 1e6).toFixed(1)}M`
  if (x >= 1e3) return `${(x / 1e3).toFixed(1)}K`
  return x.toFixed(0)
}

function qError(trueCard: number, estCard: number): number {
  const t = Math.max(trueCard, 1)
  const e = Math.max(estCard, 1)
  return Math.max(e / t, t / e)
}

const convertTreeData = (node: TreeNode, name: string): any => {
  const aliasKey = node.alias_key ?? null
  const entry = aliasKey ? props.cardinalityMap[aliasKey] : undefined
  const otherEntry = aliasKey ? props.otherCardinalityMap[aliasKey] : undefined

  const trueCard = entry?.true_card
  const estCard = entry?.est_card
  const otherEstCard = otherEntry?.est_card

  const isUnderestimated =
    props.isWorsePlan &&
    trueCard != null &&
    estCard != null &&
    estCard < trueCard

  const timeMs = node.time ? (node.time / 1_000_000).toFixed(2) : null

  const { short: shortName, tables: bracketTables } = stripBrackets(name)

  let labelLines = [shortName]
  if (timeMs) labelLines.push(`${timeMs}ms`)
  if (estCard != null && props.methodName)
    labelLines.push(`${props.methodName}: ${formatCard(estCard)}`)

  const normalColor = '#4b5563'
  const highlightColor = '#ef4444'

  const result: any = {
    name: labelLines.join('\n'),
    value: '',
    children: [],
    _aliasKey: aliasKey,
    _trueCard: trueCard,
    _estCard: estCard,
    _otherEstCard: otherEstCard,
    _timeMs: timeMs,
    _nodeName: name,
    _shortName: shortName,
    _bracketTables: bracketTables,
    itemStyle: isUnderestimated
      ? { color: highlightColor, borderColor: highlightColor, borderWidth: 2.5 }
      : { color: '#fff', borderColor: '#409EFF', borderWidth: 1.5 },
    label: {
      color: isUnderestimated ? highlightColor : normalColor,
    },
  }

  if (node.children) {
    for (const [childName, childNode] of Object.entries(node.children)) {
      result.children.push(convertTreeData(childNode, childName))
    }
  }

  return result
}

const initChart = () => {
  if (!treeContainer.value) return

  if (chart) {
    chart.dispose()
  }

  const rootName = Object.keys(props.tree)[0]
  const rootNode = props.tree[rootName]
  if (!rootName || !rootNode) return

  chart = echarts.init(treeContainer.value)
  const option: echarts.EChartsOption = {
    tooltip: {
      trigger: 'item',
      triggerOn: 'mousemove',
      backgroundColor: 'rgba(255,255,255,0.96)',
      borderColor: '#e5e7eb',
      textStyle: { color: '#1f2937', fontSize: 12 },
      extraCssText: 'box-shadow: 0 2px 8px rgba(0,0,0,0.12); max-width: 360px; white-space: pre-wrap;',
      formatter: (params: any) => {
        const data = params.data
        if (!data) return ''
        const lines: string[] = [`<b>${data._nodeName ?? params.name}</b>`]
        if (data._bracketTables)
          lines.push(`<span style="color:#6366f1">Tables: [${data._bracketTables}]</span>`)
        if (data._timeMs) lines.push(`Time: ${data._timeMs} ms`)
        if (data._trueCard != null) {
          lines.push(`True Card: ${Number(data._trueCard).toLocaleString()}`)
        }
        if (data._estCard != null && props.methodName) {
          const qe = data._trueCard != null ? ` (Q-Error: ${qError(data._trueCard, data._estCard).toFixed(2)})` : ''
          lines.push(`${props.methodName} Est: ${Number(data._estCard).toLocaleString()}${qe}`)
        }
        if (data._otherEstCard != null && props.otherMethodName) {
          const qe = data._trueCard != null ? ` (Q-Error: ${qError(data._trueCard, data._otherEstCard).toFixed(2)})` : ''
          lines.push(`${props.otherMethodName} Est: ${Number(data._otherEstCard).toLocaleString()}${qe}`)
        }
        if (data._aliasKey) lines.push(`<span style="color:#9ca3af">Alias: ${data._aliasKey}</span>`)
        return lines.join('<br/>')
      },
    },
    series: [
      {
        type: 'tree',
        orient: 'TB',
        initialTreeDepth: -1,
        data: [convertTreeData(rootNode, rootName)],
        top: '5%',
        left: '8%',
        bottom: '5%',
        right: '8%',
        symbolSize: 8,
        nodeGap: 16,
        layerPadding: 40,
        layout: 'orthogonal',
        roam: true,
        scaleLimit: { min: TREE_ZOOM_MIN, max: TREE_ZOOM_MAX },
        label: buildLabelOptionForZoom(1),
        leaves: {
          label: {
            position: 'right',
            verticalAlign: 'middle',
            align: 'left',
          },
        },
        emphasis: {
          focus: 'descendant',
          itemStyle: { borderColor: '#67C23A', borderWidth: 2 },
        },
        expandAndCollapse: true,
        animationDuration: 550,
        animationDurationUpdate: 750,
        lineStyle: { width: 1.5, color: '#93c5fd' },
      },
    ],
  }

  chart.setOption(option)
  chart.off('treeRoam')
  chart.on('treeRoam', () => {
    applyTreeZoom(getCurrentTreeZoom())
  })
}

function zoomIn() {
  if (!chart) return
  const currentZoom = getCurrentTreeZoom()
  applyTreeZoom(Math.min(currentZoom * 1.3, TREE_ZOOM_MAX))
}

function zoomOut() {
  if (!chart) return
  const currentZoom = getCurrentTreeZoom()
  applyTreeZoom(Math.max(currentZoom / 1.3, TREE_ZOOM_MIN))
}

function zoomReset() {
  if (!chart) return
  chart.setOption({
    series: [{ zoom: 1, center: undefined, label: buildLabelOptionForZoom(1) }],
  })
}

const handleResize = () => {
  chart?.resize()
}

watch(
  () => props.tree,
  () => {
    if (props.tree) {
      nextTick(() => initChart())
    }
  },
  { deep: true },
)

watch(
  [() => props.cardinalityMap, () => props.otherCardinalityMap, () => props.isWorsePlan],
  () => {
    if (props.tree) {
      nextTick(() => initChart())
    }
  },
)

onMounted(() => {
  nextTick(() => initChart())
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  chart?.dispose()
})
</script>

<style scoped>
.tree-wrapper {
  position: relative;
  width: 100%;
  height: 100%;
}

.tree-container {
  width: 100%;
  height: 100%;
  min-height: 400px;
}

.tree-toolbar {
  position: absolute;
  top: 6px;
  right: 6px;
  display: flex;
  gap: 4px;
  z-index: 10;
}

.tree-toolbar__btn {
  width: 26px;
  height: 26px;
  border: 1px solid #d1d5db;
  border-radius: 4px;
  background: rgba(255, 255, 255, 0.9);
  color: #374151;
  font-size: 14px;
  line-height: 1;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: background 0.15s;
}

.tree-toolbar__btn:hover {
  background: #f3f4f6;
}
</style>
