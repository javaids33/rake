import { clsx, type ClassValue } from 'clsx'

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs)
}

export function formatDuration(ms: number): string {
  if (ms < 1) return '<1ms'
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60000).toFixed(1)}m`
}

export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return String(n)
}

export function formatRelativeTime(iso: string): string {
  const time = new Date(iso).getTime()
  if (isNaN(time)) return '-'
  const diff = Date.now() - time
  const seconds = Math.floor(diff / 1000)
  if (seconds < 60) return 'just now'
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

export function truncate(str: string, len: number): string {
  return str.length > len ? str.slice(0, len) + '...' : str
}

export function inferFormat(tableName: string): { format: string; variant: string } {
  if (!tableName) return { format: 'Unknown', variant: 'unknown' }
  if (tableName.startsWith('pg_')) return { format: 'PostgreSQL', variant: 'external' }
  if (tableName.startsWith('mysql_')) return { format: 'MySQL', variant: 'external' }
  if (tableName.startsWith('mongo_')) return { format: 'MongoDB', variant: 'external' }
  if (tableName.startsWith('uploads_')) return { format: 'Upload', variant: 'file' }
  if (tableName.includes('parquet')) return { format: 'Parquet', variant: 'file' }
  if (tableName.includes('csv')) return { format: 'CSV', variant: 'file' }
  if (tableName.includes('json')) return { format: 'JSON', variant: 'file' }
  if (tableName.includes('lance')) return { format: 'Lance', variant: 'vector' }
  if (tableName.includes('delta')) return { format: 'Delta', variant: 'lakehouse' }
  return { format: 'Iceberg', variant: 'lakehouse' }
}

export const QUERY_TYPE_COLORS: Record<string, string> = {
  OLAP: 'bg-blue-500/15 text-blue-400 border-blue-500/20',
  Interactive: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/20',
  DDL: 'bg-amber-500/15 text-amber-400 border-amber-500/20',
  DML: 'bg-purple-500/15 text-purple-400 border-purple-500/20',
  Streaming: 'bg-cyan-500/15 text-cyan-400 border-cyan-500/20',
  Vector: 'bg-pink-500/15 text-pink-400 border-pink-500/20',
}

export const FORMAT_COLORS: Record<string, string> = {
  Iceberg: 'bg-sky-500/15 text-sky-400 border-sky-500/20',
  Delta: 'bg-teal-500/15 text-teal-400 border-teal-500/20',
  Parquet: 'bg-violet-500/15 text-violet-400 border-violet-500/20',
  CSV: 'bg-amber-500/15 text-amber-400 border-amber-500/20',
  JSON: 'bg-lime-500/15 text-lime-400 border-lime-500/20',
  Lance: 'bg-pink-500/15 text-pink-400 border-pink-500/20',
  PostgreSQL: 'bg-blue-500/15 text-blue-400 border-blue-500/20',
  MySQL: 'bg-cyan-500/15 text-cyan-400 border-cyan-500/20',
  MongoDB: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/20',
  Upload: 'bg-orange-500/15 text-orange-400 border-orange-500/20',
}
