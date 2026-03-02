import { useState, useEffect, useCallback, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useEditorStore } from '../stores/editor'
import { SqlEditorComponent } from '../components/editor/SqlEditor'
import { DataTable } from '../components/ui/DataTable'
import { QueryChart } from '../components/charts/QueryChart'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Card } from '../components/ui/Card'
import { Tabs } from '../components/ui/Tabs'
import { Modal } from '../components/ui/Modal'
import { Input } from '../components/ui/Input'
import { cn, formatDuration, QUERY_TYPE_COLORS, FORMAT_COLORS, inferFormat } from '../lib/utils'
import { executeSql, explainSql, estimateQuery, getTables, getTableSchema, getConnections } from '../api/client'
import type { ChartType, ColumnSchema, ConnectionEntry, ExplainResponse, QueryEstimateResponse } from '../types'
import { Tooltip } from '../components/ui/Tooltip'
import {
  Play, Plus, X, Table2, BarChart3, LineChart, ScatterChart, PieChart,
  AreaChart, Save, BookOpen, Zap, Clock, Rows3, Terminal, FileSearch, Gauge,
  ChevronDown, ChevronRight, GitBranch, Radio, Workflow, Database, Plug, Search, Columns3, MousePointerClick,
} from 'lucide-react'

const chartOptions: Array<{ type: ChartType; icon: React.ReactNode; label: string }> = [
  { type: 'bar', icon: <BarChart3 className="w-3.5 h-3.5" />, label: 'Bar' },
  { type: 'line', icon: <LineChart className="w-3.5 h-3.5" />, label: 'Line' },
  { type: 'area', icon: <AreaChart className="w-3.5 h-3.5" />, label: 'Area' },
  { type: 'scatter', icon: <ScatterChart className="w-3.5 h-3.5" />, label: 'Scatter' },
  { type: 'pie', icon: <PieChart className="w-3.5 h-3.5" />, label: 'Pie' },
]

export function SqlEditorPage() {
  const store = useEditorStore()
  const navigate = useNavigate()
  const activeTab = store.tabs.find(t => t.id === store.activeTabId) || store.tabs[0]
  const result = store.results[store.activeTabId]
  const error = store.errors[store.activeTabId]
  const loading = store.loading[store.activeTabId]

  const [view, setView] = useState<'table' | 'chart' | 'explain'>('table')
  const [saveOpen, setSaveOpen] = useState(false)
  const [explainResult, setExplainResult] = useState<ExplainResponse | null>(null)
  const [explaining, setExplaining] = useState(false)
  const [saveName, setSaveName] = useState('')
  const [tables, setTables] = useState<string[]>([])
  const [colMap, setColMap] = useState<Record<string, Array<{ name: string; type: string }>>>({})
  const [estimate, setEstimate] = useState<QueryEstimateResponse | null>(null)
  const [estimating, setEstimating] = useState(false)
  const [workflowOpen, setWorkflowOpen] = useState(false)
  const [demoOpen, setDemoOpen] = useState(false)
  const [sidebarTab, setSidebarTab] = useState<'catalog' | 'saved'>('catalog')
  const [connections, setConnections] = useState<ConnectionEntry[]>([])
  const [expandedConn, setExpandedConn] = useState<Set<string>>(new Set())
  const [expandedTable, setExpandedTable] = useState<string | null>(null)
  const [tableSchemas, setTableSchemas] = useState<Record<string, ColumnSchema[]>>({})
  const [catalogFilter, setCatalogFilter] = useState('')
  const workflowRef = useRef<HTMLDivElement>(null)
  const demoRef = useRef<HTMLDivElement>(null)

  // Close dropdowns on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (workflowRef.current && !workflowRef.current.contains(e.target as Node)) setWorkflowOpen(false)
      if (demoRef.current && !demoRef.current.contains(e.target as Node)) setDemoOpen(false)
    }
    if (workflowOpen || demoOpen) document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [workflowOpen, demoOpen])

  const pushToWorkflow = (target: 'scheduler' | 'transforms' | 'streaming') => {
    const sql = activeTab.sql.trim()
    if (!sql) { setWorkflowOpen(false); return }
    const name = activeTab.name !== `Query ${activeTab.id}` ? activeTab.name : ''
    setWorkflowOpen(false)
    navigate(`/${target}`, { state: { sql, name } })
  }

  // Clear estimate when switching tabs
  useEffect(() => { setEstimate(null) }, [store.activeTabId])

  useEffect(() => {
    getTables().then(async (r) => {
      const raw = r.tables || []
      const names = raw.map((t: string | { name: string }) => typeof t === 'string' ? t : t.name)
      setTables(names)
      const map: Record<string, Array<{ name: string; type: string }>> = {}
      for (const name of names.slice(0, 30)) {
        try {
          const schema = await getTableSchema(name)
          map[name] = schema.columns.map((c: ColumnSchema) => ({ name: c.name, type: c.data_type }))
        } catch { /* skip */ }
      }
      setColMap(map)
    }).catch(() => {})
    getConnections().then(r => {
      const conns = r.connections || []
      setConnections(conns)
      if (conns.length === 1) setExpandedConn(new Set([conns[0].id]))
    }).catch(() => {})
  }, [])

  const runQuery = useCallback(async () => {
    const sql = activeTab.sql.trim()
    if (!sql) return
    store.setLoading(store.activeTabId, true)
    store.setError(store.activeTabId, null)
    try {
      const res = await executeSql(sql)
      store.setResult(store.activeTabId, res)
    } catch (e) {
      store.setError(store.activeTabId, (e as Error).message)
      store.setResult(store.activeTabId, null)
    } finally {
      store.setLoading(store.activeTabId, false)
    }
  }, [activeTab, store])

  const runExplain = useCallback(async () => {
    const sql = activeTab.sql.trim()
    if (!sql) return
    setExplaining(true)
    try {
      const res = await explainSql(sql)
      setExplainResult(res)
      setView('explain')
    } catch (e) {
      store.setError(store.activeTabId, (e as Error).message)
    } finally {
      setExplaining(false)
    }
  }, [activeTab, store])

  const handleSave = () => {
    if (saveName.trim() && activeTab.sql.trim()) {
      store.saveQuery(saveName.trim(), activeTab.sql)
      setSaveName('')
      setSaveOpen(false)
    }
  }

  const handleEstimate = async () => {
    const sql = activeTab.sql
    if (!sql?.trim()) return
    setEstimating(true)
    try {
      const est = await estimateQuery(sql)
      setEstimate(est)
    } catch { setEstimate(null) }
    setEstimating(false)
  }

  const toggleConn = (id: string) => {
    setExpandedConn(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }

  const handleExpandTable = async (tableName: string) => {
    if (expandedTable === tableName) { setExpandedTable(null); return }
    setExpandedTable(tableName)
    if (!tableSchemas[tableName]) {
      try {
        const schema = await getTableSchema(tableName)
        setTableSchemas(prev => ({ ...prev, [tableName]: schema.columns }))
      } catch { /* skip */ }
    }
  }

  const insertAtCursor = (text: string) => {
    const current = activeTab.sql
    store.updateTabSql(store.activeTabId, current ? `${current}\n${text}` : text)
  }

  // Compute tables belonging to connections vs standalone
  const connTableSet = new Set(connections.flatMap(c => c.tables || []))
  const otherTables = tables.filter(t => !connTableSet.has(t))
  const filteredOther = catalogFilter
    ? otherTables.filter(t => t.toLowerCase().includes(catalogFilter.toLowerCase()))
    : otherTables

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-white/[0.03] bg-navy-950/50 backdrop-blur-md">
        {/* Tabs */}
        <div className="flex items-center gap-1 flex-1 min-w-0 overflow-x-auto">
          {store.tabs.map(tab => (
            <button
              key={tab.id}
              onClick={() => store.setActiveTab(tab.id)}
              className={cn(
                'group flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-md transition-all whitespace-nowrap',
                tab.id === store.activeTabId
                  ? 'bg-white/[0.06] text-zinc-100 border border-white/[0.06]'
                  : 'text-zinc-500 hover:text-zinc-300 hover:bg-white/[0.03] border border-transparent'
              )}
            >
              <span onDoubleClick={() => {
                const name = prompt('Tab name:', tab.name)
                if (name) store.renameTab(tab.id, name)
              }}>{tab.name}</span>
              {store.tabs.length > 1 && (
                <X
                  className="w-3 h-3 opacity-0 group-hover:opacity-100 hover:text-red-400 transition-all"
                  onClick={(e) => { e.stopPropagation(); store.removeTab(tab.id) }}
                />
              )}
            </button>
          ))}
          <button onClick={store.addTab} className="p-1.5 rounded-md text-zinc-600 hover:text-zinc-400 hover:bg-surface-3 transition-colors">
            <Plus className="w-3.5 h-3.5" />
          </button>
        </div>

        <div className="flex items-center gap-2 flex-shrink-0">
          <Button variant="ghost" size="sm" icon={<Save className="w-3.5 h-3.5" />} onClick={() => setSaveOpen(true)}>Save</Button>

          {/* Demo queries dropdown — visible when pg_ tables detected */}
          {tables.some(t => t.startsWith('pg_')) && (
            <div ref={demoRef} className="relative">
              <Button
                variant="ghost"
                size="sm"
                icon={<BookOpen className="w-3.5 h-3.5 text-emerald-400" />}
                onClick={() => setDemoOpen(!demoOpen)}
              >
                <span className="text-emerald-400">Demos</span>
                <ChevronDown className={cn('w-3 h-3 ml-0.5 text-emerald-400 transition-transform', demoOpen && 'rotate-180')} />
              </Button>
              {demoOpen && (
                <div className="absolute right-0 top-full mt-1 w-72 rounded-lg border border-white/[0.06] bg-navy-950/95 backdrop-blur-xl shadow-2xl z-50 py-1 animate-fade-in">
                  {[
                    { label: 'Customer Orders', sql: 'SELECT c.name, COUNT(*) as orders\nFROM pg_customers c\nJOIN pg_orders o ON c.id = o.customer_id\nGROUP BY c.name\nORDER BY orders DESC' },
                    { label: 'Sales Summary', sql: 'SELECT p.name, SUM(s.amount) as revenue\nFROM pg_products p\nJOIN pg_sales s ON p.id = s.product_id\nGROUP BY p.name\nORDER BY revenue DESC' },
                    { label: 'All Tables', sql: 'SHOW TABLES' },
                  ].map(q => (
                    <button
                      key={q.label}
                      onClick={() => { store.updateTabSql(store.activeTabId, q.sql); setDemoOpen(false) }}
                      className="w-full flex flex-col px-3 py-2.5 text-left hover:bg-white/[0.04] transition-colors"
                    >
                      <p className="text-xs font-medium text-zinc-200">{q.label}</p>
                      <p className="text-2xs text-zinc-500 font-mono mt-0.5 truncate">{q.sql.split('\n')[0]}</p>
                    </button>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Workflow actions dropdown */}
          <div ref={workflowRef} className="relative">
            <Button
              variant="ghost"
              size="sm"
              icon={<Workflow className="w-3.5 h-3.5 text-cyan-400" />}
              onClick={() => setWorkflowOpen(!workflowOpen)}
            >
              <span className="text-cyan-400">Use in</span>
              <ChevronDown className={cn('w-3 h-3 ml-0.5 text-cyan-400 transition-transform', workflowOpen && 'rotate-180')} />
            </Button>
            {workflowOpen && (
              <div className="absolute right-0 top-full mt-1 w-56 rounded-lg border border-white/[0.06] bg-navy-950/95 backdrop-blur-xl shadow-2xl z-50 py-1 animate-fade-in">
                <button
                  onClick={() => pushToWorkflow('scheduler')}
                  className="w-full flex items-center gap-3 px-3 py-2.5 text-left hover:bg-white/[0.04] transition-colors"
                >
                  <Clock className="w-4 h-4 text-amber-400" />
                  <div>
                    <p className="text-xs font-medium text-zinc-200">Schedule as Job</p>
                    <p className="text-2xs text-zinc-500">Run this query on a cron schedule</p>
                  </div>
                </button>
                <button
                  onClick={() => pushToWorkflow('transforms')}
                  className="w-full flex items-center gap-3 px-3 py-2.5 text-left hover:bg-white/[0.04] transition-colors"
                >
                  <GitBranch className="w-4 h-4 text-violet-400" />
                  <div>
                    <p className="text-xs font-medium text-zinc-200">Create Transform</p>
                    <p className="text-2xs text-zinc-500">Turn into a dbt-compatible model</p>
                  </div>
                </button>
                <button
                  onClick={() => pushToWorkflow('streaming')}
                  className="w-full flex items-center gap-3 px-3 py-2.5 text-left hover:bg-white/[0.04] transition-colors"
                >
                  <Radio className="w-4 h-4 text-cyan-400" />
                  <div>
                    <p className="text-xs font-medium text-zinc-200">Create Pipeline</p>
                    <p className="text-2xs text-zinc-500">Use as streaming transform SQL</p>
                  </div>
                </button>
              </div>
            )}
          </div>

          <Button variant="ghost" size="sm" icon={<FileSearch className="w-3.5 h-3.5" />} onClick={runExplain} loading={explaining}>Explain</Button>
          <Button variant="ghost" size="sm" icon={<Gauge className="w-3.5 h-3.5 text-amber-400" />} onClick={handleEstimate} loading={estimating}>
            <span className="text-amber-400">Estimate</span>
          </Button>
          <Button variant="primary" size="sm" icon={<Play className="w-3.5 h-3.5" />} onClick={runQuery} loading={loading}>
            Run <kbd className="ml-1 text-2xs opacity-60">⌘↵</kbd>
          </Button>
        </div>
      </div>

      {/* Editor + Saved Queries sidebar */}
      <div className="flex flex-1 min-h-0">
        <div className="flex flex-col flex-1 min-w-0">
          {/* Monaco Editor */}
          <div className="h-[45%] min-h-[200px] border-b border-white/[0.03]">
            <SqlEditorComponent
              value={activeTab.sql}
              onChange={(v) => { store.updateTabSql(store.activeTabId, v); setEstimate(null) }}
              onRun={runQuery}
              tables={tables}
              columns={colMap}
            />
          </div>

          {/* Cost Estimation Bar */}
          {estimate && (
            <div style={{
              display: 'flex', alignItems: 'center', gap: 16,
              padding: '8px 16px',
              background: estimate.cost_rating === 'high' ? 'rgba(239,68,68,0.1)' : estimate.cost_rating === 'medium' ? 'rgba(245,158,11,0.1)' : 'rgba(34,197,94,0.1)',
              border: `1px solid ${estimate.cost_rating === 'high' ? 'rgba(239,68,68,0.3)' : estimate.cost_rating === 'medium' ? 'rgba(245,158,11,0.3)' : 'rgba(34,197,94,0.3)'}`,
              borderRadius: 8, fontSize: 12, color: '#e2e8f0',
            }}>
              <span style={{ fontWeight: 700, textTransform: 'uppercase', fontSize: 11,
                color: estimate.cost_rating === 'high' ? '#ef4444' : estimate.cost_rating === 'medium' ? '#f59e0b' : '#22c55e'
              }}>
                {estimate.cost_rating} cost
              </span>
              <span>~{estimate.estimated_rows.toLocaleString()} rows</span>
              <span>{estimate.estimated_scan_size} scan</span>
              <span>{estimate.partitions} partitions</span>
              {estimate.tables_referenced.length > 0 && (
                <span style={{ color: '#94a3b8' }}>Tables: {estimate.tables_referenced.join(', ')}</span>
              )}
              <button onClick={() => setEstimate(null)} style={{ marginLeft: 'auto', background: 'none', border: 'none', color: '#64748b', cursor: 'pointer', fontSize: 14 }}>&#x2715;</button>
            </div>
          )}

          {/* Results */}
          <div className="flex-1 min-h-0 flex flex-col">
            {error && (
              <div className="px-4 py-3 bg-rose-500/[0.06] border-b border-rose-500/15">
                <p className="text-xs text-rose-400 font-mono">{error}</p>
              </div>
            )}

            {result && (
              <>
                {/* Result toolbar */}
                <div className="flex items-center justify-between px-4 py-2 border-b border-white/[0.03] bg-navy-950/40">
                  <div className="flex items-center gap-3">
                    <Tabs
                      tabs={[
                        { id: 'table', label: 'Table', icon: <Table2 className="w-3 h-3" /> },
                        { id: 'chart', label: 'Chart', icon: <BarChart3 className="w-3 h-3" /> },
                        ...(explainResult ? [{ id: 'explain', label: 'Plan', icon: <FileSearch className="w-3 h-3" /> }] : []),
                      ]}
                      active={view}
                      onChange={(id) => setView(id as 'table' | 'chart' | 'explain')}
                    />
                    {view === 'chart' && (
                      <div className="flex items-center gap-1 ml-2">
                        {chartOptions.map(opt => (
                          <button
                            key={opt.type}
                            onClick={() => store.setChartType(opt.type)}
                            className={cn(
                              'p-1.5 rounded-md transition-colors',
                              store.chartType === opt.type ? 'bg-rust-500/20 text-rust-400' : 'text-zinc-600 hover:text-zinc-400'
                            )}
                            title={opt.label}
                          >
                            {opt.icon}
                          </button>
                        ))}
                      </div>
                    )}
                  </div>
                  <div className="flex items-center gap-3">
                    <Badge className={QUERY_TYPE_COLORS[result.query_type] || 'bg-surface-4 text-zinc-400 border-zinc-700/50'}>
                      {result.query_type}
                    </Badge>
                    <Tooltip content={`Rows returned: ${result.row_count.toLocaleString()} | Query type: ${result.query_type}`} position="bottom">
                      <span className="flex items-center gap-1 text-2xs font-mono text-zinc-500">
                        <Rows3 className="w-3 h-3" /> {result.row_count}
                      </span>
                    </Tooltip>
                    <Tooltip content={`Planning: ${result.parse_ms ?? '?'}ms | Execution: ${result.exec_ms ?? '?'}ms | Total: ${result.duration_ms}ms`} position="bottom">
                      <span className="flex items-center gap-1 text-2xs font-mono text-zinc-500">
                        <Zap className="w-3 h-3" /> {formatDuration(result.duration_ms)}
                      </span>
                    </Tooltip>
                    {result.exec_ms !== undefined && (
                      <span className="flex items-center gap-1 text-2xs font-mono text-zinc-600">
                        <Clock className="w-3 h-3" /> exec {formatDuration(result.exec_ms)}
                      </span>
                    )}
                  </div>
                </div>

                {/* Content */}
                <div className="flex-1 overflow-auto">
                  {view === 'table' ? (
                    <DataTable columns={result.columns} rows={result.rows} maxHeight="100%" />
                  ) : view === 'explain' && explainResult ? (
                    <div className="p-4 space-y-4">
                      <div>
                        <h3 className="text-xs font-semibold text-zinc-300 mb-2">Query Plan Tree</h3>
                        <div className="space-y-0.5">
                          {explainResult.nodes.map(node => (
                            <div key={node.id} className="flex items-center gap-2" style={{ paddingLeft: `${node.depth * 24}px` }}>
                              <div className={cn('w-2 h-2 rounded-full flex-shrink-0', node.depth === 0 ? 'bg-amber-400' : 'bg-cyan-400/60')} />
                              <span className="text-xs font-mono text-amber-400/80">{node.operator}</span>
                              <span className="text-2xs font-mono text-zinc-500 truncate">{node.detail !== node.operator ? node.detail : ''}</span>
                            </div>
                          ))}
                          {explainResult.nodes.length === 0 && (
                            <p className="text-xs text-zinc-600">No plan nodes parsed</p>
                          )}
                        </div>
                      </div>
                      <div>
                        <h3 className="text-xs font-semibold text-zinc-300 mb-2">Logical Plan</h3>
                        <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/60 rounded-lg p-3 overflow-x-auto border border-white/[0.04] whitespace-pre-wrap">{explainResult.logical_plan}</pre>
                      </div>
                      <div>
                        <h3 className="text-xs font-semibold text-zinc-300 mb-2">Physical Plan</h3>
                        <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/60 rounded-lg p-3 overflow-x-auto border border-white/[0.04] whitespace-pre-wrap">{explainResult.physical_plan}</pre>
                      </div>
                    </div>
                  ) : (
                    <div className="p-4">
                      <QueryChart type={store.chartType} columns={result.columns} rows={result.rows} />
                    </div>
                  )}
                </div>
              </>
            )}

            {!result && !error && explainResult && (
              <div className="flex-1 overflow-auto p-4 space-y-4">
                <div>
                  <h3 className="text-xs font-semibold text-zinc-300 mb-2">Query Plan Tree</h3>
                  <div className="space-y-0.5">
                    {explainResult.nodes.map(node => (
                      <div key={node.id} className="flex items-center gap-2" style={{ paddingLeft: `${node.depth * 24}px` }}>
                        <div className={cn('w-2 h-2 rounded-full flex-shrink-0', node.depth === 0 ? 'bg-amber-400' : 'bg-cyan-400/60')} />
                        <span className="text-xs font-mono text-amber-400/80">{node.operator}</span>
                        <span className="text-2xs font-mono text-zinc-500 truncate">{node.detail !== node.operator ? node.detail : ''}</span>
                      </div>
                    ))}
                  </div>
                </div>
                <div>
                  <h3 className="text-xs font-semibold text-zinc-300 mb-2">Logical Plan</h3>
                  <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/60 rounded-lg p-3 overflow-x-auto border border-white/[0.04] whitespace-pre-wrap">{explainResult.logical_plan}</pre>
                </div>
                <div>
                  <h3 className="text-xs font-semibold text-zinc-300 mb-2">Physical Plan</h3>
                  <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/60 rounded-lg p-3 overflow-x-auto border border-white/[0.04] whitespace-pre-wrap">{explainResult.physical_plan}</pre>
                </div>
              </div>
            )}

            {!result && !error && !explainResult && (
              <div className="flex-1 flex items-center justify-center">
                <div className="text-center">
                  <Terminal className="w-8 h-8 text-zinc-700 mx-auto mb-3" />
                  <p className="text-xs text-zinc-600">Run a query to see results</p>
                  <p className="text-2xs text-zinc-700 mt-1">⌘+Enter to execute</p>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Catalog + Saved sidebar */}
        <div className="w-64 border-l border-white/[0.03] bg-navy-950/50 backdrop-blur-sm flex flex-col flex-shrink-0">
          {/* Tab toggle */}
          <div className="flex border-b border-white/[0.03]">
            <button
              onClick={() => setSidebarTab('catalog')}
              className={cn(
                'flex-1 flex items-center justify-center gap-1.5 px-3 py-2 text-2xs font-semibold uppercase tracking-wider transition-colors',
                sidebarTab === 'catalog'
                  ? 'text-amber-400 border-b-2 border-amber-400/60 bg-white/[0.02]'
                  : 'text-zinc-500 hover:text-zinc-400'
              )}
            >
              <Database className="w-3 h-3" />
              Catalog
              <span className="text-2xs font-normal opacity-60">({tables.length})</span>
            </button>
            <button
              onClick={() => setSidebarTab('saved')}
              className={cn(
                'flex-1 flex items-center justify-center gap-1.5 px-3 py-2 text-2xs font-semibold uppercase tracking-wider transition-colors',
                sidebarTab === 'saved'
                  ? 'text-amber-400 border-b-2 border-amber-400/60 bg-white/[0.02]'
                  : 'text-zinc-500 hover:text-zinc-400'
              )}
            >
              <BookOpen className="w-3 h-3" />
              Saved
              {store.savedQueries.length > 0 && (
                <span className="text-2xs font-normal opacity-60">({store.savedQueries.length})</span>
              )}
            </button>
          </div>

          {/* Catalog tab */}
          {sidebarTab === 'catalog' && (
            <div className="flex-1 overflow-y-auto">
              {/* Search filter */}
              <div className="px-2 py-2 border-b border-white/[0.03]">
                <div className="relative">
                  <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-zinc-600" />
                  <input
                    type="text"
                    value={catalogFilter}
                    onChange={e => setCatalogFilter(e.target.value)}
                    placeholder="Filter tables..."
                    className="w-full bg-white/[0.03] border border-white/[0.04] rounded-md pl-7 pr-2 py-1.5 text-2xs text-zinc-300 placeholder:text-zinc-600 focus:outline-none focus:border-amber-400/30"
                  />
                </div>
              </div>

              {/* Connections */}
              {connections.map(conn => {
                const connTables = (conn.tables || []).filter(t =>
                  !catalogFilter || t.toLowerCase().includes(catalogFilter.toLowerCase())
                )
                if (catalogFilter && connTables.length === 0) return null
                const isExpanded = expandedConn.has(conn.id)
                return (
                  <div key={conn.id}>
                    <button
                      onClick={() => toggleConn(conn.id)}
                      className="w-full flex items-center gap-2 px-3 py-2 hover:bg-white/[0.02] transition-colors group"
                    >
                      {isExpanded
                        ? <ChevronDown className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                        : <ChevronRight className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                      }
                      <Plug className="w-3 h-3 text-blue-400 flex-shrink-0" />
                      <span className="text-xs font-semibold text-zinc-300 truncate">{conn.name}</span>
                      <Badge className="ml-auto text-2xs bg-blue-500/15 text-blue-400 border-blue-500/20">
                        {conn.conn_type}
                      </Badge>
                    </button>
                    {isExpanded && (
                      <div className="border-l-2 border-l-amber-400/20 ml-4">
                        {connTables.length === 0 && (
                          <p className="px-4 py-1.5 text-2xs text-zinc-600 italic">No tables</p>
                        )}
                        {connTables.map(tbl => {
                          const isTableExpanded = expandedTable === tbl
                          const cols = tableSchemas[tbl]
                          return (
                            <div key={tbl}>
                              <div className="flex items-center group">
                                <button
                                  onClick={() => handleExpandTable(tbl)}
                                  className="flex-1 flex items-center gap-1.5 px-4 py-1.5 hover:bg-white/[0.02] transition-colors min-w-0"
                                >
                                  {isTableExpanded
                                    ? <ChevronDown className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                    : <ChevronRight className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                  }
                                  <Table2 className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                                  <span className="text-2xs font-mono text-zinc-400 truncate">{tbl}</span>
                                </button>
                                <Tooltip content="Insert SELECT query" position="left">
                                  <button
                                    onClick={() => insertAtCursor(`SELECT * FROM ${tbl} LIMIT 100;`)}
                                    className="p-1 mr-2 rounded opacity-0 group-hover:opacity-100 hover:bg-white/[0.04] transition-all"
                                  >
                                    <MousePointerClick className="w-3 h-3 text-amber-400/70" />
                                  </button>
                                </Tooltip>
                              </div>
                              {isTableExpanded && cols && (
                                <div className="ml-8 border-l border-white/[0.04]">
                                  {cols.map(col => (
                                    <button
                                      key={col.name}
                                      onClick={() => insertAtCursor(`${tbl}.${col.name}`)}
                                      className="w-full flex items-center gap-1.5 px-3 py-1 hover:bg-white/[0.02] transition-colors cursor-pointer"
                                      title={`Insert ${tbl}.${col.name}`}
                                    >
                                      <Columns3 className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                      <span className="text-2xs font-mono text-zinc-500 truncate">{col.name}</span>
                                      <span className="ml-auto text-2xs font-mono text-zinc-700 flex-shrink-0">{col.data_type}</span>
                                    </button>
                                  ))}
                                </div>
                              )}
                              {isTableExpanded && !cols && (
                                <p className="ml-8 px-3 py-1 text-2xs text-zinc-700 italic">Loading...</p>
                              )}
                            </div>
                          )
                        })}
                      </div>
                    )}
                  </div>
                )
              })}

              {/* Other tables (not in any connection) */}
              {filteredOther.length > 0 && (
                <div>
                  {connections.length > 0 && (
                    <div className="px-3 py-2 border-t border-white/[0.03]">
                      <p className="text-2xs font-semibold text-zinc-500 uppercase tracking-wider">
                        Other Tables ({filteredOther.length})
                      </p>
                    </div>
                  )}
                  {filteredOther.map(tbl => {
                    const isTableExpanded = expandedTable === tbl
                    const cols = tableSchemas[tbl]
                    const { format } = inferFormat(tbl)
                    return (
                      <div key={tbl}>
                        <div className="flex items-center group">
                          <button
                            onClick={() => handleExpandTable(tbl)}
                            className="flex-1 flex items-center gap-1.5 px-3 py-1.5 hover:bg-white/[0.02] transition-colors min-w-0"
                          >
                            {isTableExpanded
                              ? <ChevronDown className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                              : <ChevronRight className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                            }
                            <Table2 className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                            <span className="text-2xs font-mono text-zinc-400 truncate">{tbl}</span>
                            <Badge className={cn('ml-1 text-2xs', FORMAT_COLORS[format] || 'bg-surface-4 text-zinc-400 border-zinc-700/50')}>
                              {format}
                            </Badge>
                          </button>
                          <Tooltip content="Insert SELECT query" position="left">
                            <button
                              onClick={() => insertAtCursor(`SELECT * FROM ${tbl} LIMIT 100;`)}
                              className="p-1 mr-2 rounded opacity-0 group-hover:opacity-100 hover:bg-white/[0.04] transition-all"
                            >
                              <MousePointerClick className="w-3 h-3 text-amber-400/70" />
                            </button>
                          </Tooltip>
                        </div>
                        {isTableExpanded && cols && (
                          <div className="ml-6 border-l border-white/[0.04]">
                            {cols.map(col => (
                              <button
                                key={col.name}
                                onClick={() => insertAtCursor(`${tbl}.${col.name}`)}
                                className="w-full flex items-center gap-1.5 px-3 py-1 hover:bg-white/[0.02] transition-colors cursor-pointer"
                                title={`Insert ${tbl}.${col.name}`}
                              >
                                <Columns3 className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                <span className="text-2xs font-mono text-zinc-500 truncate">{col.name}</span>
                                <span className="ml-auto text-2xs font-mono text-zinc-700 flex-shrink-0">{col.data_type}</span>
                              </button>
                            ))}
                          </div>
                        )}
                        {isTableExpanded && !cols && (
                          <p className="ml-6 px-3 py-1 text-2xs text-zinc-700 italic">Loading...</p>
                        )}
                      </div>
                    )
                  })}
                </div>
              )}

              {/* Empty state */}
              {tables.length === 0 && connections.length === 0 && (
                <div className="flex flex-col items-center justify-center py-8 px-4">
                  <Database className="w-6 h-6 text-zinc-700 mb-2" />
                  <p className="text-2xs text-zinc-600 text-center">No tables registered yet</p>
                  <p className="text-2xs text-zinc-700 text-center mt-1">Add a data source to browse tables</p>
                </div>
              )}
            </div>
          )}

          {/* Saved tab */}
          {sidebarTab === 'saved' && (
            <div className="flex-1 overflow-y-auto">
              {store.savedQueries.length === 0 ? (
                <div className="flex flex-col items-center justify-center py-8 px-4">
                  <BookOpen className="w-6 h-6 text-zinc-700 mb-2" />
                  <p className="text-2xs text-zinc-600 text-center">No saved queries yet</p>
                  <p className="text-2xs text-zinc-700 text-center mt-1">Save a query to access it here</p>
                </div>
              ) : (
                store.savedQueries.map(q => (
                  <button
                    key={q.id}
                    onClick={() => store.updateTabSql(store.activeTabId, q.sql)}
                    className="w-full text-left px-3 py-2 border-b border-zinc-800/20 hover:bg-surface-3/40 transition-colors group"
                  >
                    <p className="text-xs text-zinc-300 truncate">{q.name}</p>
                    <p className="text-2xs text-zinc-600 font-mono truncate mt-0.5">{q.sql}</p>
                  </button>
                ))
              )}
            </div>
          )}
        </div>
      </div>

      {/* Save modal */}
      <Modal open={saveOpen} onClose={() => setSaveOpen(false)} title="Save Query">
        <div className="space-y-4">
          <Input label="Query Name" value={saveName} onChange={e => setSaveName(e.target.value)} placeholder="My query" />
          <div className="flex justify-end gap-2">
            <Button variant="secondary" size="sm" onClick={() => setSaveOpen(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleSave}>Save</Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
