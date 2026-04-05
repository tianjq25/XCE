export interface JobQueryEntry {
  id: number
  sql: string
  /** 与 all_queries.pkl 中键 q_name 一致；由 SQL 行文件生成时为 line_N */
  name?: string
}

/** 从 public/job-queries.json 加载 113 条 JOB SQL（构建后位于站点根路径） */
export async function loadJobQueries(): Promise<JobQueryEntry[]> {
  const res = await fetch('/job-queries.json')
  if (!res.ok) {
    throw new Error(`Failed to load /job-queries.json: HTTP ${res.status}`)
  }
  const data = (await res.json()) as JobQueryEntry[]
  if (!Array.isArray(data) || data.length === 0) {
    throw new Error('job-queries.json is invalid or empty')
  }
  return data.sort((a, b) => a.id - b.id)
}
