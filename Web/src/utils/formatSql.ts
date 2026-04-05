import { format } from 'sql-formatter'

/**
 * 将 JOB SQL 格式化为多行缩进（关键字大写、SELECT/WHERE 等换行），便于阅读。
 * 与 PostgreSQL 风格兼容；解析失败时返回原文。
 */
export function formatJobSql (sql: string): string {
  const s = sql.trim()
  if (!s) {
    return ''
  }
  const options = {
    language: 'postgresql' as const,
    tabWidth: 4,
    useTabs: false,
    keywordCase: 'upper' as const,
    dataTypeCase: 'upper' as const,
    functionCase: 'upper' as const,
    indentStyle: 'standard' as const,
    linesBetweenQueries: 2
  }
  try {
    return format(s, options)
  } catch {
    try {
      return format(s, { ...options, language: 'sql' as const })
    } catch {
      return sql
    }
  }
}
