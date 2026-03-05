import { useState, useEffect } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { EmptyState } from '../components/ui/EmptyState'
import { cn, formatDuration, formatRelativeTime, QUERY_TYPE_COLORS, formatNumber } from '../lib/utils'
import { getQueryHistory } from '../api/client'
import type { QueryHistoryEntry } from '../types'
import { useEditorStore } from '../stores/editor'
import { useNavigate } from 'react-router-dom'
import {
  Layers, Clock, Zap, CheckCircle2, XCircle, Rows3,
  RotateCcw, Search, BarChart3, Copy,
} from 'lucide-react'
import toast from 'react-hot-toast'

export function QueryHistory() {
  const [entries, setEntries] = useState<QueryHistoryEntry[]>([])
  const [filter, setFilter] = useState('all')
  const [engineFilter, setEngineFilter] = useState<string>('all')
  const [search, setSearch] = useState('')
  const navigate = useNavigate()
  const { updateTabSql, activeTabId } = useEditorStore()

  useEffect(() => {
    getQueryHistory(500).then(r => setEntries(r.history || [])).catch(() => {})
  }, [])

  const filtered = entries.filter(e => {
    if (filter === 'success' && e.status !== 'success') return false
    if (filter === 'error' && e.status !== 'error') return false
    if (filter !== 'all' && filter !== 'success' && filter !== 'error' && e.query_type !== filter) return false
    if (engineFilter !== 'all') {
      const entryEngine = e.engine || 'DataFusion'
      if (engineFilter === 'datafusion' && entryEngine !== 'DataFusion') return false
      if (engineFilter === 'duckdb' && entryEngine !== 'DuckDB') return false
      if (engineFilter === 'polars' && entryEngine !== 'Polars') return false
    }
    if (search && !e.sql.toLowerCase().includes(search.toLowerCase())) return false
    return true
  })

  const successCount = entries.filter(e => e.status === 'success').length
  const errorCount = entries.filter(e => e.status === 'error').length
  const avgDuration = entries.length ? entries.reduce((s, e) => s + e.duration_ms, 0) / entries.length : 0
  const totalRows = entries.reduce((s, e) => s + e.row_count, 0)
  const types = [...new Set(entries.map(e => e.query_type))]

  const replay = (sql: string) => {
    updateTabSql(activeTabId, sql)
    navigate('/sql')
  }

  return (
    <div className="flex flex-col h-full animate-fade-in">
      {/* Header */}
      <div className="px-6 py-4 border-b border-white/[0.04]">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-blue-400/10 border border-blue-400/20 flex items-center justify-center">
            <Layers className="w-4.5 h-4.5 text-blue-400" />
          </div>
          <div>
            <h1 className="text-base font-display font-bold text-zinc-100">Query History</h1>
            <p className="text-2xs text-zinc-500">Browse and replay past query executions</p>
          </div>
        </div>
      </div>

      {/* Stats strip */}
      <div className="grid grid-cols-4 gap-px bg-white/[0.02] border-b border-white/[0.04]">
        {[
          { label: 'Total Queries', value: String(entries.length), icon: BarChart3, color: 'text-blue-400' },
          { label: 'Successful', value: String(successCount), icon: CheckCircle2, color: 'text-emerald-400' },
          { label: 'Avg Duration', value: formatDuration(avgDuration), icon: Clock, color: 'text-amber-400' },
          { label: 'Total Rows', value: formatNumber(totalRows), icon: Rows3, color: 'text-violet-400' },
        ].map(m => (
          <div key={m.label} className="flex items-center gap-3 px-4 py-3 bg-navy-950/60">
            <m.icon className={cn('w-4 h-4', m.color)} />
            <div>
              <p className="text-sm font-bold font-mono text-zinc-100">{m.value}</p>
              <p className="text-2xs text-zinc-600">{m.label}</p>
            </div>
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="flex items-center gap-3 px-6 pt-4">
        <Tabs
          tabs={[
            { id: 'all', label: 'All', count: entries.length },
            { id: 'success', label: 'Success', count: successCount },
            { id: 'error', label: 'Errors', count: errorCount },
            ...types.slice(0, 3).map(t => ({ id: t, label: t })),
          ]}
          active={filter}
          onChange={setFilter}
        />
        <div className="flex gap-1 p-1 bg-navy-950/60 rounded-lg border border-white/[0.04]">
          {[
            { id: 'all', label: 'All Engines' },
            { id: 'datafusion', label: 'DataFusion' },
            { id: 'duckdb', label: 'DuckDB' },
            { id: 'polars', label: 'Polars' },
          ].map(eng => (
            <button
              key={eng.id}
              onClick={() => setEngineFilter(eng.id)}
              className={cn(
                'px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200',
                engineFilter === eng.id
                  ? 'bg-white/[0.06] text-zinc-100 shadow-sm border border-white/[0.05]'
                  : 'text-zinc-500 hover:text-zinc-300'
              )}
            >
              {eng.label}
            </button>
          ))}
        </div>
        <div className="relative flex-1 max-w-xs">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-600" />
          <input
            className="w-full pl-8 pr-3 py-1.5 text-xs rounded-lg bg-white/[0.04] border border-white/[0.06] text-zinc-300 placeholder-zinc-600 focus:outline-none focus:ring-1 focus:ring-amber-400/25 transition-all"
            placeholder="Search queries..."
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
        </div>
      </div>

      {/* List */}
      <div className="flex-1 overflow-auto px-6 py-4">
        <div className="rounded-xl border border-white/[0.04] overflow-hidden">
          {filtered.length === 0 ? (
            <EmptyState icon={<Layers className="w-5 h-5" />} title="No matching queries" description="Adjust filters or run some queries in the SQL Editor" />
          ) : (
            <div className="divide-y divide-white/[0.03] max-h-[calc(100vh-320px)] overflow-y-auto">
              {filtered.map(entry => (
                <div key={entry.query_id} className="flex items-start gap-3 px-4 py-3 hover:bg-white/[0.02] transition-colors group">
                  {entry.status === 'success'
                    ? <CheckCircle2 className="w-4 h-4 text-emerald-400 mt-0.5 flex-shrink-0" />
                    : <XCircle className="w-4 h-4 text-rose-400 mt-0.5 flex-shrink-0" />
                  }
                  <div className="flex-1 min-w-0">
                    <code className="text-xs font-mono text-zinc-300 block truncate">{entry.sql}</code>
                    <div className="flex items-center gap-2 mt-1.5">
                      <Badge className={QUERY_TYPE_COLORS[entry.query_type] || 'bg-white/[0.04] text-zinc-400 border-white/[0.06]'}>{entry.query_type}</Badge>
                      <Badge className={(entry.engine || 'DataFusion') === 'DuckDB' ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' : (entry.engine || 'DataFusion') === 'Polars' ? 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20' : 'bg-amber-400/10 text-amber-400 border-amber-400/20'}>
                        {(entry.engine || 'DataFusion') === 'DuckDB' ? 'DK' : (entry.engine || 'DataFusion') === 'Polars' ? 'PL' : 'DF'}
                      </Badge>
                      <span className="text-2xs font-mono text-zinc-500 flex items-center gap-1"><Zap className="w-3 h-3" />{formatDuration(entry.duration_ms)}</span>
                      <span className="text-2xs font-mono text-zinc-500 flex items-center gap-1"><Rows3 className="w-3 h-3" />{formatNumber(entry.row_count)}</span>
                      <span className="text-2xs text-zinc-600">{formatRelativeTime(entry.timestamp)}</span>
                    </div>
                    {entry.error && <p className="text-2xs text-rose-400 mt-1 truncate">{entry.error}</p>}
                  </div>
                  <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0">
                    <button onClick={() => { navigator.clipboard.writeText(entry.sql); toast.success('Copied') }}
                      className="p-1.5 rounded-md text-zinc-600 hover:text-zinc-300 hover:bg-white/[0.05] transition-colors">
                      <Copy className="w-3.5 h-3.5" />
                    </button>
                    <button onClick={() => replay(entry.sql)}
                      className="p-1.5 rounded-md text-zinc-600 hover:text-zinc-300 hover:bg-white/[0.05] transition-colors">
                      <RotateCcw className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
