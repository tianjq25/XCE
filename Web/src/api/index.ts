import axios from 'axios'

// 创建axios实例
const request = axios.create({
  baseURL: '/', // 使用相对路径，通过代理访问后端
  timeout: 60000 // 设置超时时间为1分钟
})

// 请求拦截器
request.interceptors.request.use(
  config => {
    // 在这里可以添加token等认证信息
    return config
  },
  error => {
    return Promise.reject(error)
  }
)

// 响应拦截器
request.interceptors.response.use(
  response => {
    return response.data
  },
  error => {
    // 统一处理错误
    console.error('请求错误：', error)
    return Promise.reject(error)
  }
)

// API接口
export interface TreeNode {
  children?: Record<string, TreeNode>
  time?: number
  alias_key?: string | null
}

export interface CardinalityEntry {
  true_card: number
  est_card: number
}

export interface JoinQueryResponse {
  latency: number
  pgm_detail_rows: (number | string)[][]
  res_table: {
    col_type: string[]
    rows: (number[] | string)[][]
  }
  tree?: Record<string, TreeNode>
  cardinality_map?: Record<string, CardinalityEntry>
}

// ---------- 决策树可视化 ----------

export interface CandidateSplit {
  condition: string
  left_size: number
  right_size: number
  score: number
}

export interface DTNode {
  is_leaf: boolean
  size: number
  mean: number
  variance: number
  // leaf
  min?: number
  max?: number
  candidate_splits?: CandidateSplit[]
  // internal
  split_condition?: string
  split_type?: string
  left?: DTNode
  right?: DTNode
}

export interface DTModelMeta {
  id: string
  type: 'offline' | 'online'
  name: string
  description: string
  training_samples: number
  query_name?: string
}

export interface DTModelsResponse {
  models: DTModelMeta[]
}

export interface DTDataResponse {
  model_id: string
  model_name: string
  description: string
  training_samples: number
  trees: Record<string, DTNode>
}

export const api = {
  /**
   * JOB / multi-table join: fetch pre-computed response by method + query name.
   */
  submitJobJoinQuery: (payload: { method: number; qName: string; sql: string }) => {
    const formData = new FormData()
    formData.append('method', String(payload.method))
    formData.append('q_name', payload.qName)
    formData.append('sql', payload.sql)
    return request.post<JoinQueryResponse, JoinQueryResponse>('/api/job-join/', formData)
  },

  getDTModels: () =>
    request.get<DTModelsResponse, DTModelsResponse>('/api/decision-tree/models/'),

  getDTData: (modelId: string) => {
    const formData = new FormData()
    formData.append('model_id', modelId)
    return request.post<DTDataResponse, DTDataResponse>('/api/decision-tree/data/', formData)
  },
}

export default api 