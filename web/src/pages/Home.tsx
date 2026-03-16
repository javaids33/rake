import { useEffect, useState, useMemo } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { cn, formatRelativeTime } from '../lib/utils'
import {
  getTables, getQueryHistory, getTransforms, getSchedules,
  getPipelines, getConnections,
} from '../api/client'
import type { QueryHistoryEntry, ConnectionEntry } from '../types'
import {
  Database, Terminal, Radio, Search, Clock, Activity,
  ArrowRight, GitBranch, FileText, BarChart3,
  Plus, Command, Upload, Server, Zap,
} from 'lucide-react'

const TYPE_CONFIG: Record<string, { icon: typeof Terminal; color: string }> = {
  Query: { icon: Terminal, color: 'text-blue-400' },
  Pipeline: { icon: GitBranch, color: 'text-emerald-400' },
  Table: { icon: Database, color: 'text-cyan-400' },
  Transform: { icon: Activity, color: 'text-violet-400' },
  Job: { icon: Clock, color: 'text-amber-400' },
  Connection: { icon: Server, color: 'text-emerald-400' },
}

interface RecentItem {
  name: string
  path?: string
  type: string
  viewedAt: string
  to: string
}

export function Home() {
  const navigate = useNavigate()
  const [recentQueries, setRecentQueries] = useState<QueryHistoryEntry[]>([])
  const [tableNames, setTableNames] = useState<string[]>([])
  const [connections, setConnections] = useState<ConnectionEntry[]>([])
  const [transformCount, setTransformCount] = useState(0)
  const [jobCount, setJobCount] = useState(0)
  const [pipelineCount, setPipelineCount] = useState(0)
  const [filter, setFilter] = useState<'suggested' | 'queries' | 'tables' | 'connections'>('suggested')

  useEffect(() => {
    getTables().then(r => {
      const raw = r.tables || []
      setTableNames(raw.map((t: string | { name: string }) => typeof t === 'string' ? t : t.name))
    }).catch(() => {})
    getQueryHistory(20).then(r => setRecentQueries(r.history || [])).catch(() => {})
    getTransforms().then(r => setTransformCount(r.transforms?.length || 0)).catch(() => {})
    getSchedules().then(r => setJobCount(r.schedules?.length || 0)).catch(() => {})
    getPipelines().then(r => setPipelineCount(r.pipelines?.length || 0)).catch(() => {})
    getConnections().then(r => setConnections(r.connections || [])).catch(() => {})
  }, [])

  // Build unified recent items list
  const recentItems: RecentItem[] = useMemo(() => {
    const items: RecentItem[] = []
    for (const q of recentQueries) {
      items.push({
        name: q.sql.slice(0, 80).replace(/\s+/g, ' '),
        type: 'Query',
        viewedAt: q.timestamp,
        to: '/sql',
      })
    }
    // Show connections as browseable items (like Databricks shows pipelines/queries)
    for (const c of connections) {
      items.push({
        name: `${c.name}`,
        path: `${c.host}:${c.port}/${c.database} — ${c.tables?.length || 0} tables`,
        type: 'Connection',
        viewedAt: c.created_at,
        to: '/sources',
      })
    }
    // Sort by most recent
    items.sort((a, b) => {
      if (!a.viewedAt) return 1
      if (!b.viewedAt) return -1
      return new Date(b.viewedAt).getTime() - new Date(a.viewedAt).getTime()
    })
    return items
  }, [recentQueries, connections])

  const filteredItems = useMemo(() => {
    if (filter === 'queries') return recentItems.filter(i => i.type === 'Query')
    if (filter === 'tables') return tableNames.map(name => ({
      name, type: 'Table' as const, viewedAt: '', to: '/catalog', path: undefined,
    }))
    if (filter === 'connections') return recentItems.filter(i => i.type === 'Connection')
    return recentItems.slice(0, 15)
  }, [filter, recentItems, tableNames])

  return (
    <div className="flex flex-col items-center pt-8 pb-8 px-6 min-h-full">
      {/* Welcome header */}
      <h1 className="text-2xl font-display font-bold text-zinc-100 mb-5">
        Welcome to RustLake
      </h1>

      {/* Global search bar */}
      <button
        onClick={() => window.dispatchEvent(new KeyboardEvent('keydown', { key: 'k', metaKey: true }))}
        className="w-full max-w-2xl flex items-center gap-3 px-4 py-3 rounded-xl border border-white/[0.08] bg-white/[0.03] hover:bg-white/[0.05] hover:border-white/[0.12] transition-all mb-5 group cursor-pointer"
      >
        <Search className="w-4 h-4 text-zinc-500" />
        <span className="text-sm text-zinc-500 flex-1 text-left">Search tables, queries, pipelines, and more...</span>
        <kbd className="text-2xs text-zinc-600 bg-white/[0.04] border border-white/[0.06] rounded px-1.5 py-0.5 font-mono">
          <Command className="w-3 h-3 inline -mt-0.5" /> K
        </kbd>
      </button>

      {/* Quick actions — like Databricks "Create new" */}
      <div className="w-full max-w-3xl grid grid-cols-4 gap-3 mb-6">
        {[
          { label: 'New Query', desc: 'Write SQL', icon: Terminal, to: '/sql', color: 'text-amber-400 bg-amber-400/10 border-amber-400/20' },
          { label: 'Add Source', desc: 'Connect database', icon: Database, to: '/sources', color: 'text-cyan-400 bg-cyan-400/10 border-cyan-400/20' },
          { label: 'New Pipeline', desc: 'CDC / streaming', icon: Radio, to: '/streaming', color: 'text-emerald-400 bg-emerald-400/10 border-emerald-400/20' },
          { label: 'New Job', desc: 'ETL / schedule', icon: Clock, to: '/scheduler', color: 'text-violet-400 bg-violet-400/10 border-violet-400/20' },
        ].map(a => (
          <Link
            key={a.label}
            to={a.to}
            className="flex items-center gap-3 p-3 rounded-lg border border-white/[0.06] bg-white/[0.02] hover:bg-white/[0.04] hover:border-white/[0.10] transition-all group"
          >
            <div className={cn('w-8 h-8 rounded-lg border flex items-center justify-center flex-shrink-0', a.color)}>
              <a.icon className="w-4 h-4" />
            </div>
            <div className="min-w-0">
              <p className="text-xs font-medium text-zinc-200 group-hover:text-zinc-100">{a.label}</p>
              <p className="text-2xs text-zinc-600">{a.desc}</p>
            </div>
          </Link>
        ))}
      </div>

      {/* Filter tabs */}
      <div className="flex items-center gap-1 mb-4">
        {([
          { id: 'suggested' as const, label: 'Suggested' },
          { id: 'queries' as const, label: `Queries${recentQueries.length ? ` (${recentQueries.length})` : ''}` },
          { id: 'tables' as const, label: `Tables${tableNames.length ? ` (${tableNames.length})` : ''}` },
          { id: 'connections' as const, label: `Connections${connections.length ? ` (${connections.length})` : ''}` },
        ]).map(tab => (
          <button
            key={tab.id}
            onClick={() => setFilter(tab.id)}
            className={cn(
              'px-3 py-1.5 rounded-lg text-xs font-medium transition-colors',
              filter === tab.id
                ? 'bg-amber-400/10 text-amber-400 border border-amber-400/20'
                : 'text-zinc-500 hover:text-zinc-300 hover:bg-white/[0.03] border border-transparent'
            )}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Recent items list */}
      <div className="w-full max-w-3xl flex-1">
        {filteredItems.length === 0 ? (
          <div className="text-center py-12">
            <Database className="w-8 h-8 text-zinc-700 mx-auto mb-3" />
            <p className="text-sm text-zinc-500 mb-4">
              {filter === 'tables' ? 'No tables registered yet' :
               filter === 'queries' ? 'No queries run yet' :
               filter === 'connections' ? 'No connections added yet' :
               'Get started by connecting a data source or running a query'}
            </p>
            <div className="flex items-center justify-center gap-3">
              <Link
                to="/sql"
                className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-amber-400/10 border border-amber-400/20 text-amber-400 text-sm font-medium hover:bg-amber-400/15 transition-colors"
              >
                <Plus className="w-4 h-4" /> New Query
              </Link>
              <Link
                to="/sources"
                className="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-white/[0.04] border border-white/[0.06] text-zinc-300 text-sm font-medium hover:bg-white/[0.06] transition-colors"
              >
                <Database className="w-4 h-4" /> Add Data Source
              </Link>
            </div>
          </div>
        ) : (
          <div className="divide-y divide-white/[0.04]">
            {filteredItems.map((item, i) => {
              const cfg = TYPE_CONFIG[item.type] || TYPE_CONFIG.Query
              const Icon = cfg.icon
              return (
                <Link
                  key={`${item.type}-${i}`}
                  to={item.to}
                  className="flex items-center gap-4 px-4 py-3 hover:bg-white/[0.02] transition-colors rounded-lg group"
                >
                  <Icon className={cn('w-4 h-4 flex-shrink-0', cfg.color)} />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-zinc-200 truncate group-hover:text-zinc-100 transition-colors font-mono">
                      {item.name}
                    </p>
                    {item.path && (
                      <p className="text-2xs text-zinc-600 truncate">{item.path}</p>
                    )}
                  </div>
                  {item.viewedAt && (
                    <span className="text-2xs text-zinc-600 flex-shrink-0">
                      {formatRelativeTime(item.viewedAt)}
                    </span>
                  )}
                  <span className="text-xs text-zinc-600 flex-shrink-0 w-20 text-right">
                    {item.type}
                  </span>
                </Link>
              )
            })}
          </div>
        )}
      </div>

      {/* Quick stats footer */}
      <div className="flex items-center gap-6 mt-auto pt-6 text-2xs text-zinc-600">
        <Link to="/catalog" className="hover:text-zinc-400 transition-colors">{tableNames.length} tables</Link>
        <Link to="/sources" className="hover:text-zinc-400 transition-colors">{connections.length} connections</Link>
        <Link to="/scheduler" className="hover:text-zinc-400 transition-colors">{jobCount} jobs</Link>
        <Link to="/streaming" className="hover:text-zinc-400 transition-colors">{pipelineCount} pipelines</Link>
        <Link to="/transforms" className="hover:text-zinc-400 transition-colors">{transformCount} transforms</Link>
      </div>
    </div>
  )
}
