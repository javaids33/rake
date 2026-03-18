import { useState, useEffect, useCallback, useRef } from 'react'
import { useNavigate } from 'react-router-dom'
import { useEditorStore } from '../stores/editor'
import { SqlEditorComponent } from '../components/editor/SqlEditor'
import { DataTable } from '../components/ui/DataTable'
import { QueryChart } from '../components/charts/QueryChart'
import { DataProfile } from '../components/editor/DataProfile'
import { CommandPalette } from '../components/ui/CommandPalette'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Card } from '../components/ui/Card'
import { Tabs } from '../components/ui/Tabs'
import { Modal } from '../components/ui/Modal'
import { Input } from '../components/ui/Input'
import { cn, formatDuration, QUERY_TYPE_COLORS } from '../lib/utils'
import { executeSql, explainSql, estimateQuery, compareSql, getTables, getTableSchema, getConnections, getS3Configs, trinoBrowse, trinoColumns, trinoRefresh } from '../api/client'
import { useWebSocket } from '../hooks/useWebSocket'
import { useServerEvents } from '../components/layout/Shell'
import type { SqlResponse } from '../types'
import type { SqlCompareResponse } from '../api/client'
import type { ChartType, ColumnSchema, ConnectionEntry, S3Config, ExplainResponse, QueryEstimateResponse } from '../types'
import { Tooltip } from '../components/ui/Tooltip'
import {
  Play, Plus, X, Table2, BarChart3, LineChart, ScatterChart, PieChart,
  AreaChart, Save, BookOpen, Zap, Clock, Rows3, Terminal, FileSearch, Gauge, ArrowLeftRight, Trophy,
  ChevronDown, ChevronRight, GitBranch, Radio, Workflow, Database, Plug, Search, Columns3, MousePointerClick, HardDrive, Trash2, RefreshCw, Layers, Square, Wifi, PanelRightClose, PanelRightOpen,
  Clipboard, Download, Braces, Eye, Activity, Command,
} from 'lucide-react'
import toast from 'react-hot-toast'

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

  const [view, setView] = useState<'table' | 'chart' | 'explain' | 'profile'>('table')
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
  const [sidebarTab, setSidebarTab] = useState<'catalog' | 'saved' | 'toolkit'>('catalog')
  const [catalogWidth, setCatalogWidth] = useState(() => {
    const saved = localStorage.getItem('sql_catalog_width')
    return saved ? parseInt(saved, 10) : 256
  })
  const [catalogCollapsed, setCatalogCollapsed] = useState(() => localStorage.getItem('sql_catalog_collapsed') === 'true')
  const catalogResizing = useRef(false)
  const [connections, setConnections] = useState<ConnectionEntry[]>([])
  const [expandedConn, setExpandedConn] = useState<Set<string>>(new Set())
  const [expandedTable, setExpandedTable] = useState<string | null>(null)
  const [tableSchemas, setTableSchemas] = useState<Record<string, ColumnSchema[]>>({})
  const [s3Configs, setS3Configs] = useState<S3Config[]>([])
  const [catalogFilter, setCatalogFilter] = useState('')
  const [renamingTabId, setRenamingTabId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState('')
  const [editorHeight, setEditorHeight] = useState(45) // percentage
  const [engineChoice, setEngineChoice] = useState<string>('auto')
  const [compareResult, setCompareResult] = useState<SqlCompareResponse | null>(null)
  const [comparing, setComparing] = useState(false)
  // Trino catalog browser state
  const [trinoCatalogs, setTrinoCatalogs] = useState<Record<string, { catalogs: Array<{ name: string; schemas: Array<{ name: string; tables: string[] }> }>; cached_at: string | null; total_tables: number }>>({})
  const [expandedTrinoCatalog, setExpandedTrinoCatalog] = useState<Set<string>>(new Set()) // "connId:catalog"
  const [expandedTrinoSchema, setExpandedTrinoSchema] = useState<Set<string>>(new Set()) // "connId:catalog:schema"
  const [expandedTrinoTable, setExpandedTrinoTable] = useState<string | null>(null) // "connId:catalog:schema:table"
  const [trinoColumnCache, setTrinoColumnCache] = useState<Record<string, Array<{ name: string; data_type: string; nullable: boolean; ordinal: number }>>>({})
  const [trinoRefreshing, setTrinoRefreshing] = useState<Set<string>>(new Set())
  const [cmdPaletteOpen, setCmdPaletteOpen] = useState(false)
  const resizing = useRef(false)
  const containerRef = useRef<HTMLDivElement>(null)
  const workflowRef = useRef<HTMLDivElement>(null)
  const demoRef = useRef<HTMLDivElement>(null)

  // Global Cmd+K handler for command palette
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        setCmdPaletteOpen(prev => !prev)
      }
    }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [])

  // WebSocket streaming
  const { connected: wsConnected, sendQuery, cancelQuery } = useWebSocket()
  const activeQueryIdRef = useRef<string | null>(null)
  const streamedRowsRef = useRef<Record<string, unknown>[]>([])
  const streamedColumnsRef = useRef<string[]>([])
  const rafRef = useRef<number>(0)

  // Panel resizer
  const startResize = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    resizing.current = true
    const onMove = (ev: MouseEvent) => {
      if (!resizing.current || !containerRef.current) return
      const rect = containerRef.current.getBoundingClientRect()
      const pct = ((ev.clientY - rect.top) / rect.height) * 100
      setEditorHeight(Math.max(15, Math.min(85, pct)))
    }
    const onUp = () => { resizing.current = false; document.removeEventListener('mousemove', onMove); document.removeEventListener('mouseup', onUp) }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }, [])

  // Catalog sidebar resizer (horizontal)
  const startCatalogResize = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    catalogResizing.current = true
    document.body.style.cursor = 'col-resize'
    document.body.style.userSelect = 'none'
    const onMove = (ev: MouseEvent) => {
      if (!catalogResizing.current) return
      const newWidth = window.innerWidth - ev.clientX
      const clamped = Math.max(180, Math.min(500, newWidth))
      setCatalogWidth(clamped)
    }
    const onUp = () => {
      catalogResizing.current = false
      document.body.style.cursor = ''
      document.body.style.userSelect = ''
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
      setCatalogWidth(w => { localStorage.setItem('sql_catalog_width', String(w)); return w })
    }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }, [])

  const toggleCatalogCollapse = useCallback(() => {
    setCatalogCollapsed(prev => {
      const next = !prev
      localStorage.setItem('sql_catalog_collapsed', String(next))
      return next
    })
  }, [])

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

  // Clear transient state when switching tabs
  useEffect(() => {
    setEstimate(null)
    setExplainResult(null)
    setCompareResult(null)
    setView('table')
  }, [store.activeTabId])

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
      if (conns.length === 1) {
        setExpandedConn(new Set([conns[0].id]))
        // Auto-load Trino catalog tree if the single connection is Trino
        if (conns[0].conn_type === 'trino') {
          trinoBrowse(conns[0].id).then(data => {
            setTrinoCatalogs(prev => ({ ...prev, [conns[0].id]: data }))
          }).catch(() => {})
        }
      }
    }).catch(() => {})
    getS3Configs().then(r => setS3Configs(r.configs || [])).catch(() => {})
  }, [])

  // Refresh connections when background sync completes (SSE)
  const { onConnectionSync } = useServerEvents()
  useEffect(() => {
    const unsub = onConnectionSync((event) => {
      if (event.sync_status === 'ready' || event.sync_status === 'error') {
        // Refresh the full connections list so sidebar shows updated tables
        getConnections().then(r => setConnections(r.connections || [])).catch(() => {})
        // Also refresh registered tables for the catalog tab
        getTables().then(async r => {
          const raw = r.tables || []
          const names: string[] = raw.map((t: string | { name: string }) => typeof t === 'string' ? t : t.name)
          setTables(names)
        }).catch(() => {})
      }
    })
    return unsub
  }, [onConnectionSync])

  const handleCancelQuery = useCallback(() => {
    if (activeQueryIdRef.current) {
      cancelQuery(activeQueryIdRef.current)
      activeQueryIdRef.current = null
    }
  }, [cancelQuery])

  const runQuery = useCallback(async () => {
    const sql = activeTab.sql.trim()
    if (!sql) return
    const tabId = store.activeTabId
    store.setLoading(tabId, true)
    store.setError(tabId, null)

    // ── WebSocket streaming path ──
    if (wsConnected) {
      const queryId = crypto.randomUUID()
      activeQueryIdRef.current = queryId
      streamedRowsRef.current = []
      streamedColumnsRef.current = []

      sendQuery(queryId, sql, engineChoice, {
        onStart: () => {
          store.setResult(tabId, null)
        },
        onRows: (msg) => {
          streamedRowsRef.current = streamedRowsRef.current.concat(msg.rows)
          if (streamedColumnsRef.current.length === 0) {
            streamedColumnsRef.current = msg.columns
          }
          // Batch UI updates via rAF to avoid per-chunk re-renders
          cancelAnimationFrame(rafRef.current)
          rafRef.current = requestAnimationFrame(() => {
            const partial: SqlResponse = {
              query_id: queryId,
              columns: streamedColumnsRef.current,
              rows: streamedRowsRef.current,
              row_count: streamedRowsRef.current.length,
              query_type: 'STREAMING',
              duration_ms: 0,
              engine: engineChoice === 'auto' ? 'datafusion' : engineChoice,
            }
            store.setResult(tabId, partial)
          })
        },
        onComplete: (msg) => {
          cancelAnimationFrame(rafRef.current)
          activeQueryIdRef.current = null
          const final: SqlResponse = {
            query_id: msg.query_id,
            columns: streamedColumnsRef.current,
            rows: streamedRowsRef.current,
            row_count: msg.row_count,
            query_type: msg.query_type,
            duration_ms: msg.duration_ms,
            parse_ms: msg.parse_ms,
            exec_ms: msg.exec_ms,
            engine: msg.engine,
          }
          store.setResult(tabId, final)
          store.setLoading(tabId, false)
        },
        onError: (msg) => {
          cancelAnimationFrame(rafRef.current)
          activeQueryIdRef.current = null
          store.setError(tabId, msg.error)
          store.setResult(tabId, null)
          store.setLoading(tabId, false)
        },
        onCancelled: () => {
          cancelAnimationFrame(rafRef.current)
          activeQueryIdRef.current = null
          store.setError(tabId, 'Query cancelled')
          store.setLoading(tabId, false)
        },
      })
      return
    }

    // ── HTTP fallback path ──
    try {
      const res = await executeSql(sql, engineChoice)
      store.setResult(tabId, res)
    } catch (e) {
      store.setError(tabId, (e as Error).message)
      store.setResult(tabId, null)
    } finally {
      store.setLoading(tabId, false)
    }
  }, [activeTab, store, engineChoice, wsConnected, sendQuery])

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

  const handleCompare = async () => {
    const sql = activeTab.sql.trim()
    if (!sql) return
    setComparing(true)
    setCompareResult(null)
    try {
      const res = await compareSql(sql)
      setCompareResult(res)
    } catch (e) {
      store.setError(store.activeTabId, (e as Error).message)
    }
    setComparing(false)
  }

  const toggleConn = (id: string) => {
    setExpandedConn(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id); else next.add(id)
      return next
    })
  }

  // Trino: load catalog tree when connection expanded
  const expandTrinoConn = async (connId: string) => {
    toggleConn(connId)
    if (!trinoCatalogs[connId]) {
      try {
        const data = await trinoBrowse(connId)
        setTrinoCatalogs(prev => ({ ...prev, [connId]: data }))
      } catch { /* skip */ }
    }
  }

  const toggleTrinoCatalog = (key: string) => {
    setExpandedTrinoCatalog(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key); else next.add(key)
      return next
    })
  }

  const toggleTrinoSchema = (key: string) => {
    setExpandedTrinoSchema(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key); else next.add(key)
      return next
    })
  }

  const expandTrinoTableCols = async (connId: string, catalog: string, schema: string, table: string) => {
    const key = `${connId}:${catalog}:${schema}:${table}`
    if (expandedTrinoTable === key) { setExpandedTrinoTable(null); return }
    setExpandedTrinoTable(key)
    if (!trinoColumnCache[key]) {
      try {
        const data = await trinoColumns(connId, catalog, schema, table)
        setTrinoColumnCache(prev => ({ ...prev, [key]: data.columns }))
      } catch { /* skip */ }
    }
  }

  const handleTrinoRefresh = async (connId: string) => {
    setTrinoRefreshing(prev => new Set(prev).add(connId))
    try {
      await trinoRefresh(connId)
      const data = await trinoBrowse(connId)
      setTrinoCatalogs(prev => ({ ...prev, [connId]: data }))
      setTrinoColumnCache({}) // invalidate column cache
    } catch { /* skip */ }
    setTrinoRefreshing(prev => { const next = new Set(prev); next.delete(connId); return next })
  }

  // Build mapping: connection raw table name → registered table name (e.g. "customers" → "pg.customers")
  const resolveRegistered = useCallback((connTableName: string, connType?: string) => {
    // Direct match first
    if (tables.includes(connTableName)) return connTableName
    // Try schema-qualified names (pg.table, mysql.table, mongo.table) and legacy prefixes
    const schemas = connType === 'mysql' ? ['mysql.', 'mysql_'] : connType === 'mongodb' ? ['mongo.', 'mongo_'] : ['pg.', 'mysql.', 'mongo.', 'pg_', 'mysql_', 'mongo_']
    for (const prefix of schemas) {
      const qualified = `${prefix}${connTableName}`
      if (tables.includes(qualified)) return qualified
    }
    return null
  }, [tables])

  const handleExpandTable = async (tableName: string) => {
    if (expandedTable === tableName) { setExpandedTable(null); return }
    setExpandedTable(tableName)
    // Try registered name for schema lookup
    const registered = resolveRegistered(tableName) || tableName
    if (!tableSchemas[tableName]) {
      try {
        const schema = await getTableSchema(registered)
        setTableSchemas(prev => ({ ...prev, [tableName]: schema.columns }))
      } catch { /* skip */ }
    }
  }

  const insertAtCursor = (text: string) => {
    const current = activeTab.sql
    store.updateTabSql(store.activeTabId, current ? `${current}\n${text}` : text)
  }

  // Group S3 tables by schema for tree display
  const s3SchemaMap = new Map<string, Map<string, string[]>>() // s3Name -> schema -> tables
  for (const s3 of s3Configs) {
    const schemas = new Map<string, string[]>()
    for (const tbl of (s3.tables || [])) {
      const dotIdx = tbl.indexOf('.')
      if (dotIdx > 0) {
        const schema = tbl.substring(0, dotIdx)
        const table = tbl.substring(dotIdx + 1)
        if (!schemas.has(schema)) schemas.set(schema, [])
        schemas.get(schema)!.push(table)
      } else {
        if (!schemas.has('default')) schemas.set('default', [])
        schemas.get('default')!.push(tbl)
      }
    }
    // Sort schemas and tables
    for (const [, tbls] of schemas) tbls.sort()
    s3SchemaMap.set(s3.name, schemas)
  }

  return (
    <div className="flex flex-col h-full">
      {/* Toolbar */}
      <div className="flex items-center gap-2 px-4 py-2 border-b border-white/[0.03] bg-navy-950/50 backdrop-blur-md relative z-20">
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
              {renamingTabId === tab.id ? (
                <input
                  autoFocus
                  value={renameValue}
                  onChange={e => setRenameValue(e.target.value)}
                  onBlur={() => { if (renameValue.trim()) store.renameTab(tab.id, renameValue.trim()); setRenamingTabId(null) }}
                  onKeyDown={e => { if (e.key === 'Enter') { if (renameValue.trim()) store.renameTab(tab.id, renameValue.trim()); setRenamingTabId(null) } else if (e.key === 'Escape') setRenamingTabId(null) }}
                  className="bg-transparent border-b border-amber-400/50 text-xs text-zinc-100 outline-none w-20"
                  onClick={e => e.stopPropagation()}
                />
              ) : (
                <span onDoubleClick={(e) => { e.stopPropagation(); setRenamingTabId(tab.id); setRenameValue(tab.name) }}>{tab.name}</span>
              )}
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

          {/* Demo queries dropdown — visible when pg. tables detected */}
          {tables.some(t => t.startsWith('pg.') || t.startsWith('pg_')) && (
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
                    { label: 'Customer Orders', sql: 'SELECT c.name, COUNT(*) as order_count\nFROM pg.customers c\nJOIN pg.orders o ON c.customer_id = o.customer_id\nGROUP BY c.name\nORDER BY order_count DESC' },
                    { label: 'Sales by Product', sql: 'SELECT p.name, p.category, SUM(s.amount) as revenue\nFROM pg.products p\nJOIN pg.sales s ON p.product_id = s.product_id\nGROUP BY p.name, p.category\nORDER BY revenue DESC' },
                    { label: 'Cross-Source Join', sql: 'SELECT pg.name as pg_name, my.c_name as mysql_name\nFROM pg.tpch_customer pg\nJOIN mysql.tpch_customer my ON pg.c_custkey = my.c_custkey\nLIMIT 20' },
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
          <select
            value={engineChoice}
            onChange={(e) => setEngineChoice(e.target.value)}
            className="h-7 px-2 text-2xs font-medium rounded-md border border-zinc-700/50 bg-surface-3 text-zinc-300 outline-none focus:border-amber-500/50 cursor-pointer"
          >
            <option value="auto">Auto (recommended)</option>
            <option value="datafusion">DataFusion</option>
            <option value="duckdb">DuckDB</option>
            <option value="polars">Polars</option>
          </select>
          {wsConnected && (
            <Tooltip content="WebSocket connected — streaming results enabled">
              <span className="flex items-center gap-1 px-1.5 py-0.5 rounded text-2xs font-medium text-emerald-400/80 border border-emerald-500/20 bg-emerald-500/5">
                <Wifi className="w-3 h-3" />
                <span>Live</span>
              </span>
            </Tooltip>
          )}
          <Button variant="primary" size="sm" icon={<Play className="w-3.5 h-3.5" />} onClick={runQuery} loading={loading}>
            Run
          </Button>
          {loading && wsConnected && (
            <Button variant="ghost" size="sm" icon={<Square className="w-3.5 h-3.5 text-red-400" />} onClick={handleCancelQuery}>
              <span className="text-red-400">Cancel</span>
            </Button>
          )}
          <Button variant="ghost" size="sm" icon={<ArrowLeftRight className="w-3.5 h-3.5 text-cyan-400" />} onClick={handleCompare} loading={comparing}>
            <span className="text-cyan-400">Compare All</span>
          </Button>
          <Tooltip content="Command Palette — search tables, pages, actions" position="bottom">
            <button
              onClick={() => setCmdPaletteOpen(true)}
              className="flex items-center gap-1.5 px-2 py-1 rounded-md text-2xs text-zinc-500 border border-zinc-700/40 bg-zinc-800/20 hover:bg-white/[0.04] hover:text-zinc-300 transition-colors"
            >
              <Command className="w-3 h-3" />
              <span className="font-mono">K</span>
            </button>
          </Tooltip>
        </div>
      </div>

      {/* Editor + Saved Queries sidebar */}
      <div className="flex flex-1 min-h-0">
        <div ref={containerRef} className="flex flex-col flex-1 min-w-0">
          {/* Monaco Editor */}
          <div style={{ height: `${editorHeight}%` }} className="min-h-[120px]">
            <SqlEditorComponent
              value={activeTab.sql}
              onChange={(v) => { store.updateTabSql(store.activeTabId, v); setEstimate(null) }}
              onRun={runQuery}
              tables={tables}
              columns={colMap}
            />
          </div>

          {/* Resize handle */}
          <div
            onMouseDown={startResize}
            className="h-1.5 cursor-row-resize border-y border-white/[0.03] bg-navy-950/60 hover:bg-amber-400/10 transition-colors flex items-center justify-center group"
          >
            <div className="w-8 h-0.5 rounded-full bg-zinc-700 group-hover:bg-amber-400/40 transition-colors" />
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

          {/* Engine Comparison Panel */}
          {compareResult && (
            <div className="px-4 py-3 border-b border-white/[0.04] bg-navy-950/60">
              <div className="flex items-center justify-between mb-2.5">
                <div className="flex items-center gap-2">
                  <ArrowLeftRight className="w-3.5 h-3.5 text-cyan-400" />
                  <span className="text-xs font-semibold text-zinc-200">Engine Comparison</span>
                </div>
                <div className="flex items-center gap-2">
                  {compareResult.winner !== 'N/A' && (
                    <span className="flex items-center gap-1 text-2xs font-medium text-amber-400">
                      <Trophy className="w-3 h-3" /> {compareResult.winner} wins
                      {compareResult.speedup > 1 && ` (${compareResult.speedup}x faster)`}
                    </span>
                  )}
                  <button onClick={() => setCompareResult(null)} className="text-zinc-600 hover:text-zinc-400 transition-colors">
                    <X className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>
              <div className="grid grid-cols-3 gap-2">
                {([
                  { key: 'datafusion' as const, label: 'DataFusion', short: 'DF', color: 'amber' },
                  { key: 'duckdb' as const, label: 'DuckDB', short: 'DK', color: 'emerald' },
                  { key: 'polars' as const, label: 'Polars', short: 'PL', color: 'cyan' },
                ] as const).map(eng => {
                  const r = compareResult[eng.key]
                  const isWinner = compareResult.winner === eng.label
                  return (
                    <div key={eng.key} className={cn(
                      'rounded-lg border p-2.5 transition-all',
                      isWinner
                        ? `border-${eng.color}-500/40 bg-${eng.color}-500/[0.06]`
                        : 'border-white/[0.04] bg-navy-950/40'
                    )} style={isWinner ? {
                      borderColor: eng.color === 'amber' ? 'rgba(245,158,11,0.4)' : eng.color === 'emerald' ? 'rgba(16,185,129,0.4)' : 'rgba(6,182,212,0.4)',
                      background: eng.color === 'amber' ? 'rgba(245,158,11,0.06)' : eng.color === 'emerald' ? 'rgba(16,185,129,0.06)' : 'rgba(6,182,212,0.06)',
                    } : {}}>
                      <div className="flex items-center justify-between mb-1.5">
                        <span className={cn(
                          'inline-flex items-center px-1.5 py-0.5 rounded text-2xs font-bold',
                          eng.color === 'amber' ? 'bg-amber-500/15 text-amber-400' :
                          eng.color === 'emerald' ? 'bg-emerald-500/15 text-emerald-400' :
                          'bg-cyan-500/15 text-cyan-400'
                        )}>
                          {eng.short}
                        </span>
                        {isWinner && <Trophy className="w-3 h-3 text-amber-400" />}
                      </div>
                      {r.status === 'success' ? (
                        <div className="space-y-0.5">
                          <div className="text-sm font-mono font-bold text-zinc-200">{r.duration_ms}ms</div>
                          <div className="text-2xs text-zinc-500">{r.row_count.toLocaleString()} rows</div>
                        </div>
                      ) : r.status === 'unavailable' ? (
                        <div className="text-2xs text-zinc-600 italic">unavailable</div>
                      ) : (
                        <div className="space-y-0.5">
                          <div className="text-2xs text-rose-400">error</div>
                          <div className="text-2xs text-zinc-600 truncate" title={r.error}>{r.error}</div>
                        </div>
                      )}
                    </div>
                  )
                })}
              </div>
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
                        { id: 'profile', label: 'Profile', icon: <Activity className="w-3 h-3" /> },
                        ...(explainResult ? [{ id: 'explain', label: 'Plan', icon: <FileSearch className="w-3 h-3" /> }] : []),
                      ]}
                      active={view}
                      onChange={(id) => setView(id as 'table' | 'chart' | 'explain' | 'profile')}
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
                    {/* Result action buttons */}
                    <div className="flex items-center gap-1 border-r border-white/[0.06] pr-3 mr-1">
                      <Tooltip content="Copy SQL to clipboard" position="bottom">
                        <button
                          onClick={() => { navigator.clipboard.writeText(activeTab.sql); import('react-hot-toast').then(m => m.default.success('SQL copied')) }}
                          className="p-1.5 rounded-md text-zinc-600 hover:text-zinc-300 hover:bg-white/[0.04] transition-colors"
                        >
                          <Clipboard className="w-3.5 h-3.5" />
                        </button>
                      </Tooltip>
                      <Tooltip content="Export results as CSV" position="bottom">
                        <button
                          onClick={() => {
                            if (!result) return
                            const header = result.columns.join(',')
                            const rows_csv = result.rows.map((r: Record<string, unknown>) => result.columns.map((c: string) => {
                              const v = r[c]
                              const s = v === null || v === undefined ? '' : String(v)
                              return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s
                            }).join(',')).join('\n')
                            const blob = new Blob([header + '\n' + rows_csv], { type: 'text/csv' })
                            const url = URL.createObjectURL(blob)
                            const a = document.createElement('a')
                            a.href = url; a.download = `query_result_${Date.now()}.csv`; a.click()
                            URL.revokeObjectURL(url)
                          }}
                          className="p-1.5 rounded-md text-zinc-600 hover:text-zinc-300 hover:bg-white/[0.04] transition-colors"
                        >
                          <Download className="w-3.5 h-3.5" />
                        </button>
                      </Tooltip>
                      <Tooltip content="Copy results as JSON" position="bottom">
                        <button
                          onClick={() => { if (result) { navigator.clipboard.writeText(JSON.stringify(result.rows, null, 2)); import('react-hot-toast').then(m => m.default.success('JSON copied')) } }}
                          className="p-1.5 rounded-md text-zinc-600 hover:text-zinc-300 hover:bg-white/[0.04] transition-colors"
                        >
                          <Braces className="w-3.5 h-3.5" />
                        </button>
                      </Tooltip>
                      <Tooltip content="Create VIEW from this query" position="bottom">
                        <button
                          onClick={() => {
                            const sql = activeTab.sql.trim().replace(/;$/, '')
                            const viewName = `v_${activeTab.name.toLowerCase().replace(/[^a-z0-9]/g, '_')}`
                            store.updateTabSql(store.activeTabId, `CREATE VIEW ${viewName} AS\n${sql};`)
                            import('react-hot-toast').then(m => m.default.success('CREATE VIEW SQL generated'))
                          }}
                          className="p-1.5 rounded-md text-zinc-600 hover:text-zinc-300 hover:bg-white/[0.04] transition-colors"
                        >
                          <Eye className="w-3.5 h-3.5" />
                        </button>
                      </Tooltip>
                    </div>
                    <Tooltip content="Detected query type (auto-classified)" position="bottom">
                      <span className={cn('inline-flex items-center px-2 py-0.5 rounded-md text-2xs font-medium border', QUERY_TYPE_COLORS[result.query_type] || 'bg-surface-4 text-zinc-400 border-zinc-700/50')}>
                        {result.query_type}
                      </span>
                    </Tooltip>
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
                    {result.engine && (
                      <Tooltip content={`Query executed by ${result.engine} engine`} position="bottom">
                        <span className={cn(
                          'inline-flex items-center px-2 py-0.5 rounded-md text-2xs font-medium border',
                          result.engine.toLowerCase() === 'duckdb'
                            ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30'
                            : result.engine.toLowerCase() === 'polars'
                              ? 'bg-cyan-500/10 text-cyan-400 border-cyan-500/30'
                              : 'bg-amber-500/10 text-amber-400 border-amber-500/30'
                        )}>
                          {result.engine}
                        </span>
                      </Tooltip>
                    )}
                  </div>
                </div>

                {/* Content */}
                <div className="flex-1 overflow-auto">
                  {view === 'table' ? (
                    <DataTable columns={result.columns} rows={result.rows} maxHeight="100%" />
                  ) : view === 'profile' ? (
                    <DataProfile columns={result.columns} rows={result.rows} />
                  ) : view === 'explain' && explainResult ? (
                    <div className="p-4 space-y-5">
                      <div>
                        <h3 className="text-xs font-semibold text-zinc-300 mb-3 flex items-center gap-2">
                          <FileSearch className="w-3.5 h-3.5 text-amber-400" />
                          Query Plan Tree
                        </h3>
                        <div className="bg-navy-950/60 rounded-lg border border-white/[0.04] p-3 space-y-0.5">
                          {explainResult.nodes.map(node => (
                            <div key={node.id} className="flex items-center gap-2" style={{ paddingLeft: `${node.depth * 20}px` }}>
                              <div className={cn(
                                'w-1.5 h-1.5 rounded-full flex-shrink-0',
                                node.depth === 0 ? 'bg-amber-400' : node.depth === 1 ? 'bg-cyan-400' : 'bg-zinc-500'
                              )} />
                              <span className="text-xs font-mono text-amber-400/90 font-semibold">{node.operator}</span>
                              {node.detail !== node.operator && (
                                <span className="text-2xs font-mono text-zinc-500 truncate">{node.detail}</span>
                              )}
                            </div>
                          ))}
                          {explainResult.nodes.length === 0 && (
                            <p className="text-xs text-zinc-600 italic">No plan nodes parsed</p>
                          )}
                        </div>
                      </div>
                      <div>
                        <h3 className="text-xs font-semibold text-zinc-300 mb-2">Logical Plan</h3>
                        <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/60 rounded-lg p-3 overflow-x-auto border border-white/[0.04] whitespace-pre-wrap leading-relaxed">{explainResult.logical_plan}</pre>
                      </div>
                      <div>
                        <h3 className="text-xs font-semibold text-zinc-300 mb-2">Physical Plan</h3>
                        <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/60 rounded-lg p-3 overflow-x-auto border border-white/[0.04] whitespace-pre-wrap leading-relaxed">{explainResult.physical_plan}</pre>
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

            {!result && !error && explainResult && !loading && (
              <div className="flex-1 overflow-auto p-4 space-y-5">
                <div>
                  <h3 className="text-xs font-semibold text-zinc-300 mb-3 flex items-center gap-2">
                    <FileSearch className="w-3.5 h-3.5 text-amber-400" />
                    Query Plan Tree
                  </h3>
                  <div className="bg-navy-950/60 rounded-lg border border-white/[0.04] p-3 space-y-0.5">
                    {explainResult.nodes.map(node => (
                      <div key={node.id} className="flex items-center gap-2" style={{ paddingLeft: `${node.depth * 20}px` }}>
                        <div className={cn(
                          'w-1.5 h-1.5 rounded-full flex-shrink-0',
                          node.depth === 0 ? 'bg-amber-400' : node.depth === 1 ? 'bg-cyan-400' : 'bg-zinc-500'
                        )} />
                        <span className="text-xs font-mono text-amber-400/90 font-semibold">{node.operator}</span>
                        {node.detail !== node.operator && (
                          <span className="text-2xs font-mono text-zinc-500 truncate">{node.detail}</span>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
                <div>
                  <h3 className="text-xs font-semibold text-zinc-300 mb-2">Logical Plan</h3>
                  <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/60 rounded-lg p-3 overflow-x-auto border border-white/[0.04] whitespace-pre-wrap leading-relaxed">{explainResult.logical_plan}</pre>
                </div>
                <div>
                  <h3 className="text-xs font-semibold text-zinc-300 mb-2">Physical Plan</h3>
                  <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/60 rounded-lg p-3 overflow-x-auto border border-white/[0.04] whitespace-pre-wrap leading-relaxed">{explainResult.physical_plan}</pre>
                </div>
              </div>
            )}

            {loading && (
              <div className="flex-1 flex items-center justify-center">
                <div className="text-center">
                  <div className="w-6 h-6 border-2 border-amber-400/30 border-t-amber-400 rounded-full animate-spin mx-auto mb-3" />
                  <p className="text-xs text-zinc-500">Executing query...</p>
                </div>
              </div>
            )}

            {!result && !error && !explainResult && !loading && (
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
        <div
          style={{ width: catalogCollapsed ? 36 : catalogWidth }}
          className="border-l border-white/[0.03] bg-navy-950/50 backdrop-blur-sm flex flex-col flex-shrink-0 relative transition-[width] duration-150"
        >
          {/* Resize drag handle */}
          {!catalogCollapsed && (
            <div
              onMouseDown={startCatalogResize}
              className="absolute left-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-amber-400/30 active:bg-amber-400/50 z-10 transition-colors"
            />
          )}
          {/* Collapsed state */}
          {catalogCollapsed ? (
            <div className="flex flex-col items-center pt-2 gap-2">
              <button
                onClick={toggleCatalogCollapse}
                className="p-1.5 rounded-md text-zinc-500 hover:text-amber-400 hover:bg-white/[0.04] transition-colors"
                title="Expand catalog"
              >
                <PanelRightOpen className="w-4 h-4" />
              </button>
              <button
                onClick={() => { setCatalogCollapsed(false); localStorage.setItem('sql_catalog_collapsed', 'false'); setSidebarTab('catalog') }}
                className={cn('p-1.5 rounded-md transition-colors', sidebarTab === 'catalog' ? 'text-amber-400 bg-white/[0.04]' : 'text-zinc-600 hover:text-zinc-400')}
                title="Catalog"
              >
                <Database className="w-3.5 h-3.5" />
              </button>
              <button
                onClick={() => { setCatalogCollapsed(false); localStorage.setItem('sql_catalog_collapsed', 'false'); setSidebarTab('saved') }}
                className={cn('p-1.5 rounded-md transition-colors', sidebarTab === 'saved' ? 'text-amber-400 bg-white/[0.04]' : 'text-zinc-600 hover:text-zinc-400')}
                title="Saved queries"
              >
                <BookOpen className="w-3.5 h-3.5" />
              </button>
              <button
                onClick={() => { setCatalogCollapsed(false); localStorage.setItem('sql_catalog_collapsed', 'false'); setSidebarTab('toolkit') }}
                className={cn('p-1.5 rounded-md transition-colors', sidebarTab === 'toolkit' ? 'text-amber-400 bg-white/[0.04]' : 'text-zinc-600 hover:text-zinc-400')}
                title="SQL Toolkit"
              >
                <Zap className="w-3.5 h-3.5" />
              </button>
            </div>
          ) : (
          <>
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
            <button
              onClick={() => setSidebarTab('toolkit')}
              className={cn(
                'flex-1 flex items-center justify-center gap-1.5 px-3 py-2 text-2xs font-semibold uppercase tracking-wider transition-colors',
                sidebarTab === 'toolkit'
                  ? 'text-amber-400 border-b-2 border-amber-400/60 bg-white/[0.02]'
                  : 'text-zinc-500 hover:text-zinc-400'
              )}
            >
              <Zap className="w-3 h-3" />
              Toolkit
            </button>
            <button
              onClick={toggleCatalogCollapse}
              className="px-2 py-2 text-zinc-600 hover:text-zinc-400 transition-colors"
              title="Collapse sidebar"
            >
              <PanelRightClose className="w-3.5 h-3.5" />
            </button>
          </div>

          {/* Toolkit tab — quick SQL templates */}
          {sidebarTab === 'toolkit' && (
            <div className="flex-1 overflow-y-auto p-2 space-y-3">
              {[
                {
                  category: 'Table Operations',
                  color: 'text-amber-400',
                  templates: [
                    { label: 'Create Table (CTAS)', sql: 'CREATE TABLE my_table AS\nSELECT * FROM source_table\nWHERE condition = true;' },
                    { label: 'Create View', sql: 'CREATE VIEW my_view AS\nSELECT col1, col2, SUM(amount) as total\nFROM my_table\nGROUP BY col1, col2;' },
                    { label: 'Insert Into', sql: 'INSERT INTO target_table\nSELECT * FROM source_table\nWHERE updated_at > NOW() - INTERVAL \'1 hour\';' },
                    { label: 'Drop Table', sql: 'DROP TABLE IF EXISTS my_table;' },
                  ]
                },
                {
                  category: 'Data Exploration',
                  color: 'text-cyan-400',
                  templates: [
                    { label: 'Preview (100 rows)', sql: 'SELECT *\nFROM my_table\nLIMIT 100;' },
                    { label: 'Row Count', sql: 'SELECT COUNT(*) as total_rows\nFROM my_table;' },
                    { label: 'Column Stats', sql: 'SELECT\n  COUNT(*) as rows,\n  COUNT(DISTINCT col) as unique_vals,\n  MIN(col) as min_val,\n  MAX(col) as max_val,\n  AVG(col) as avg_val\nFROM my_table;' },
                    { label: 'Null Analysis', sql: 'SELECT\n  COUNT(*) as total,\n  COUNT(col) as non_null,\n  COUNT(*) - COUNT(col) as nulls,\n  ROUND(100.0 * (COUNT(*) - COUNT(col)) / COUNT(*), 2) as null_pct\nFROM my_table;' },
                    { label: 'Top N Values', sql: 'SELECT col, COUNT(*) as cnt\nFROM my_table\nGROUP BY col\nORDER BY cnt DESC\nLIMIT 20;' },
                    { label: 'Schema / Describe', sql: 'DESCRIBE my_table;' },
                  ]
                },
                {
                  category: 'Cross-Source Queries',
                  color: 'text-emerald-400',
                  templates: [
                    { label: 'Postgres → Query', sql: 'SELECT * FROM pg.my_table\nLIMIT 100;' },
                    { label: 'MySQL → Query', sql: 'SELECT * FROM mysql.my_table\nLIMIT 100;' },
                    { label: 'MongoDB → Query', sql: 'SELECT * FROM mongo.my_collection\nLIMIT 100;' },
                    { label: 'Cross-Source JOIN', sql: '-- Join data across Postgres and MongoDB\nSELECT\n  p.id, p.name, m.metadata\nFROM pg.users p\nJOIN mongo.user_profiles m\n  ON p.id = CAST(m.user_id AS INT)\nLIMIT 100;' },
                  ]
                },
                {
                  category: 'Iceberg / Lakehouse',
                  color: 'text-violet-400',
                  templates: [
                    { label: 'Create Iceberg Table', sql: 'CREATE TABLE iceberg.warehouse.events AS\nSELECT * FROM pg.events;' },
                    { label: 'Materialized View', sql: 'CREATE TABLE iceberg.analytics.daily_revenue AS\nSELECT\n  DATE_TRUNC(\'day\', order_date) as day,\n  SUM(total) as revenue,\n  COUNT(*) as orders\nFROM pg.orders\nGROUP BY 1\nORDER BY 1;' },
                    { label: 'Incremental Load', sql: 'INSERT INTO iceberg.warehouse.orders\nSELECT * FROM pg.orders\nWHERE updated_at > (\n  SELECT COALESCE(MAX(updated_at), \'1970-01-01\')\n  FROM iceberg.warehouse.orders\n);' },
                    { label: 'Snapshot Export', sql: '-- Export current state to S3\nCREATE TABLE iceberg.backups.orders_snapshot AS\nSELECT *, NOW() as snapshot_at\nFROM iceberg.warehouse.orders;' },
                  ]
                },
                {
                  category: 'ETL Patterns',
                  color: 'text-rose-400',
                  templates: [
                    { label: 'Full Sync (Replace)', sql: '-- Drop and recreate\nDROP TABLE IF EXISTS target_table;\nCREATE TABLE target_table AS\nSELECT * FROM source_table;' },
                    { label: 'SCD Type 1 (Upsert)', sql: '-- Merge new data (DataFusion supports MERGE coming soon)\nCREATE TABLE target_table_new AS\nSELECT * FROM source_table\nUNION ALL\nSELECT t.* FROM target_table t\nWHERE t.id NOT IN (SELECT id FROM source_table);' },
                    { label: 'Deduplication', sql: 'SELECT DISTINCT ON (id) *\nFROM my_table\nORDER BY id, updated_at DESC;' },
                    { label: 'Pivot / Aggregate', sql: 'SELECT\n  category,\n  SUM(CASE WHEN status = \'active\' THEN 1 ELSE 0 END) as active,\n  SUM(CASE WHEN status = \'inactive\' THEN 1 ELSE 0 END) as inactive\nFROM my_table\nGROUP BY category;' },
                  ]
                },
                {
                  category: 'System / Debug',
                  color: 'text-zinc-400',
                  templates: [
                    { label: 'List All Tables', sql: 'SHOW TABLES;' },
                    { label: 'Explain Query Plan', sql: 'EXPLAIN\nSELECT * FROM my_table\nWHERE id > 100;' },
                    { label: 'Explain Verbose', sql: 'EXPLAIN VERBOSE\nSELECT * FROM my_table\nWHERE id > 100;' },
                    { label: 'Show Table Schema', sql: 'SHOW COLUMNS FROM my_table;' },
                    { label: 'System Info', sql: 'SELECT 1 + 1 as health_check;' },
                  ]
                },
              ].map(group => (
                <div key={group.category}>
                  <h4 className={cn('text-2xs font-semibold uppercase tracking-wider px-2 py-1', group.color)}>
                    {group.category}
                  </h4>
                  <div className="space-y-0.5">
                    {group.templates.map(t => (
                      <button
                        key={t.label}
                        onClick={() => insertAtCursor(t.sql)}
                        className="w-full text-left px-3 py-1.5 text-2xs text-zinc-400 hover:text-zinc-200 hover:bg-white/[0.02] rounded transition-colors font-mono truncate"
                        title={t.sql}
                      >
                        {t.label}
                      </button>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Catalog tab */}
          {sidebarTab === 'catalog' && (
            <div className="flex-1 overflow-y-auto">
              {/* Search filter + global refresh */}
              <div className="px-2 py-2 border-b border-white/[0.03]">
                <div className="flex items-center gap-1.5">
                  <div className="relative flex-1">
                    <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-zinc-600" />
                    <input
                      type="text"
                      value={catalogFilter}
                      onChange={e => setCatalogFilter(e.target.value)}
                      placeholder="Filter tables..."
                      className="w-full bg-white/[0.03] border border-white/[0.04] rounded-md pl-7 pr-2 py-1.5 text-2xs text-zinc-300 placeholder:text-zinc-600 focus:outline-none focus:border-amber-400/30"
                    />
                  </div>
                  <Tooltip content="Refresh all connections" position="left">
                    <button
                      onClick={() => {
                        getConnections().then(r => setConnections(r.connections || [])).catch(() => {})
                        getS3Configs().then(r => setS3Configs(r.configs || [])).catch(() => {})
                        getTables().then(async r => {
                          const raw = r.tables || []
                          const names: string[] = raw.map((t: string | { name: string }) => typeof t === 'string' ? t : t.name)
                          setTables(names)
                        }).catch(() => {})
                        toast.success('Refreshing catalog...')
                      }}
                      className="p-1.5 rounded-md hover:bg-white/[0.04] text-zinc-600 hover:text-zinc-300 transition-colors flex-shrink-0"
                    >
                      <RefreshCw className="w-3.5 h-3.5" />
                    </button>
                  </Tooltip>
                </div>
              </div>

              {/* Connections */}
              {connections.map(conn => {
                const isTrino = conn.conn_type === 'trino'
                const connTables = (conn.tables || []).filter(t =>
                  !catalogFilter || t.toLowerCase().includes(catalogFilter.toLowerCase())
                )
                if (catalogFilter && connTables.length === 0 && !isTrino) return null
                const isExpanded = expandedConn.has(conn.id)

                // Trino connection — render catalog → schema → table → column tree
                if (isTrino) {
                  const tree = trinoCatalogs[conn.id]
                  return (
                    <div key={conn.id}>
                      <button
                        onClick={() => expandTrinoConn(conn.id)}
                        className="w-full flex items-center gap-2 px-3 py-2 hover:bg-white/[0.02] transition-colors group"
                      >
                        {isExpanded
                          ? <ChevronDown className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                          : <ChevronRight className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                        }
                        <Layers className="w-3 h-3 text-red-400 flex-shrink-0" />
                        <span className="text-xs font-semibold text-zinc-300 truncate">{conn.name}</span>
                        <Badge className="ml-auto text-2xs bg-red-500/15 text-red-400 border-red-500/20">
                          trino
                        </Badge>
                      </button>
                      {isExpanded && (
                        <div className="border-l-2 border-l-red-400/20 ml-4">
                          {/* Refresh + stats bar */}
                          <div className="flex items-center gap-2 px-4 py-1.5 border-b border-white/[0.03]">
                            <button
                              onClick={() => handleTrinoRefresh(conn.id)}
                              className="text-2xs text-zinc-500 hover:text-zinc-300 flex items-center gap-1 transition-colors"
                              disabled={trinoRefreshing.has(conn.id)}
                            >
                              <RefreshCw className={cn('w-3 h-3', trinoRefreshing.has(conn.id) && 'animate-spin')} />
                              Refresh
                            </button>
                            {tree && (
                              <span className="text-2xs text-zinc-600 ml-auto">
                                {tree.total_tables} tables{tree.cached_at ? ' (cached)' : ''}
                              </span>
                            )}
                          </div>
                          {!tree && (
                            <p className="px-4 py-2 text-2xs text-zinc-600 italic">Loading catalogs...</p>
                          )}
                          {tree && tree.catalogs.map(cat => {
                            const catKey = `${conn.id}:${cat.name}`
                            const isCatExpanded = expandedTrinoCatalog.has(catKey)
                            const filteredSchemas = catalogFilter
                              ? cat.schemas.filter(s => s.name.toLowerCase().includes(catalogFilter.toLowerCase()) || s.tables.some(t => t.toLowerCase().includes(catalogFilter.toLowerCase())))
                              : cat.schemas
                            if (catalogFilter && filteredSchemas.length === 0) return null
                            return (
                              <div key={cat.name}>
                                <button
                                  onClick={() => toggleTrinoCatalog(catKey)}
                                  className="w-full flex items-center gap-1.5 px-4 py-1.5 hover:bg-white/[0.02] transition-colors"
                                >
                                  {isCatExpanded
                                    ? <ChevronDown className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                    : <ChevronRight className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                  }
                                  <Database className="w-3 h-3 text-red-400/70 flex-shrink-0" />
                                  <span className="text-2xs font-semibold text-zinc-400">{cat.name}</span>
                                  <span className="text-2xs text-zinc-600 ml-auto">{cat.schemas.reduce((n, s) => n + s.tables.length, 0)}</span>
                                </button>
                                {isCatExpanded && filteredSchemas.map(sch => {
                                  const schKey = `${conn.id}:${cat.name}:${sch.name}`
                                  const isSchExpanded = expandedTrinoSchema.has(schKey)
                                  const filteredTables = catalogFilter
                                    ? sch.tables.filter(t => t.toLowerCase().includes(catalogFilter.toLowerCase()))
                                    : sch.tables
                                  if (catalogFilter && filteredTables.length === 0) return null
                                  return (
                                    <div key={sch.name} className="ml-3">
                                      <button
                                        onClick={() => toggleTrinoSchema(schKey)}
                                        className="w-full flex items-center gap-1.5 px-4 py-1 hover:bg-white/[0.02] transition-colors"
                                      >
                                        {isSchExpanded
                                          ? <ChevronDown className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                          : <ChevronRight className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                        }
                                        <span className="text-2xs font-mono text-zinc-500">{sch.name}</span>
                                        <span className="text-2xs text-zinc-700 ml-auto">{sch.tables.length}</span>
                                      </button>
                                      {isSchExpanded && filteredTables.map(tbl => {
                                        const tblKey = `${conn.id}:${cat.name}:${sch.name}:${tbl}`
                                        const isTblExpanded = expandedTrinoTable === tblKey
                                        const cols = trinoColumnCache[tblKey]
                                        const fqn = `trino_${cat.name}.${sch.name}_${tbl}`
                                        return (
                                          <div key={tbl} className="ml-3">
                                            <div className="flex items-center group">
                                              <button
                                                onClick={() => expandTrinoTableCols(conn.id, cat.name, sch.name, tbl)}
                                                className="flex-1 flex items-center gap-1.5 px-4 py-1 hover:bg-white/[0.02] transition-colors min-w-0"
                                              >
                                                {isTblExpanded
                                                  ? <ChevronDown className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                                  : <ChevronRight className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                                }
                                                <Table2 className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                                                <span className="text-2xs font-mono text-zinc-400 truncate">{tbl}</span>
                                              </button>
                                              <Tooltip content={`SELECT * FROM ${fqn} LIMIT 100`} position="left">
                                                <button
                                                  onClick={() => insertAtCursor(`SELECT * FROM ${fqn} LIMIT 100;`)}
                                                  className="p-1 mr-2 rounded opacity-0 group-hover:opacity-100 hover:bg-white/[0.04] transition-all"
                                                >
                                                  <MousePointerClick className="w-3 h-3 text-amber-400/70" />
                                                </button>
                                              </Tooltip>
                                            </div>
                                            {isTblExpanded && cols && (
                                              <div className="ml-8 border-l border-white/[0.04]">
                                                {cols.map(col => (
                                                  <button
                                                    key={col.name}
                                                    onClick={() => insertAtCursor(`${fqn}.${col.name}`)}
                                                    className="w-full flex items-center gap-1.5 px-3 py-1 hover:bg-white/[0.02] transition-colors cursor-pointer"
                                                    title={`Insert ${fqn}.${col.name}`}
                                                  >
                                                    <Columns3 className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                                    <span className="text-2xs font-mono text-zinc-500 truncate">{col.name}</span>
                                                    <span className="ml-auto text-2xs font-mono text-zinc-700 flex-shrink-0">{col.data_type}</span>
                                                  </button>
                                                ))}
                                              </div>
                                            )}
                                            {isTblExpanded && !cols && (
                                              <p className="ml-8 px-3 py-1 text-2xs text-zinc-700 italic">Loading columns...</p>
                                            )}
                                          </div>
                                        )
                                      })}
                                    </div>
                                  )
                                })}
                              </div>
                            )
                          })}
                        </div>
                      )}
                    </div>
                  )
                }

                // Non-Trino connection — existing behavior
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
                        {/* Refresh bar */}
                        <div className="flex items-center gap-2 px-4 py-1 border-b border-white/[0.03]">
                          <button
                            onClick={(e) => {
                              e.stopPropagation()
                              // Re-fetch connections to get latest tables
                              getConnections().then(r => setConnections(r.connections || [])).catch(() => {})
                              getTables().then(async r => {
                                const raw = r.tables || []
                                const names: string[] = raw.map((t: string | { name: string }) => typeof t === 'string' ? t : t.name)
                                setTables(names)
                              }).catch(() => {})
                              toast.success(`Refreshing ${conn.name}...`)
                            }}
                            className="text-2xs text-zinc-500 hover:text-zinc-300 flex items-center gap-1 transition-colors"
                          >
                            <RefreshCw className="w-3 h-3" />
                            Refresh
                          </button>
                          <span className="text-2xs text-zinc-600 ml-auto">
                            {connTables.length} table{connTables.length !== 1 ? 's' : ''}
                          </span>
                        </div>
                        {connTables.length === 0 && (
                          <p className="px-4 py-1.5 text-2xs text-zinc-600 italic">No tables</p>
                        )}
                        {connTables.map(tbl => {
                          const registered = resolveRegistered(tbl, conn.conn_type) || tbl
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
                                  <span className="text-2xs font-mono text-zinc-400 truncate">{registered}</span>
                                </button>
                                <Tooltip content={`SELECT * FROM ${registered} LIMIT 100`} position="left">
                                  <button
                                    onClick={() => insertAtCursor(`SELECT * FROM ${registered} LIMIT 100;`)}
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
                                      onClick={() => insertAtCursor(`${registered}.${col.name}`)}
                                      className="w-full flex items-center gap-1.5 px-3 py-1 hover:bg-white/[0.02] transition-colors cursor-pointer"
                                      title={`Insert ${registered}.${col.name}`}
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

              {/* S3/MinIO connections */}
              {s3Configs.map(s3 => {
                const s3Id = `s3-${s3.name}`
                const isExpanded = expandedConn.has(s3Id)
                const schemas = s3SchemaMap.get(s3.name) || new Map()
                const tableCount = (s3.tables || []).length
                if (catalogFilter && !s3.name.toLowerCase().includes(catalogFilter.toLowerCase()) && !s3.bucket.toLowerCase().includes(catalogFilter.toLowerCase()) && !(s3.tables || []).some(t => t.toLowerCase().includes(catalogFilter.toLowerCase()))) return null
                return (
                  <div key={s3Id}>
                    <button
                      onClick={() => toggleConn(s3Id)}
                      className="w-full flex items-center gap-2 px-3 py-2 hover:bg-white/[0.02] transition-colors group"
                    >
                      {isExpanded
                        ? <ChevronDown className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                        : <ChevronRight className="w-3 h-3 text-zinc-500 flex-shrink-0" />
                      }
                      <HardDrive className="w-3 h-3 text-emerald-400 flex-shrink-0" />
                      <span className="text-xs font-semibold text-zinc-300 truncate">{s3.name}</span>
                      <Badge className="ml-auto text-2xs bg-emerald-500/15 text-emerald-400 border-emerald-500/20">
                        {tableCount > 0 ? (() => {
                          const fmts = s3.format_counts || {}
                          const fmtStr = Object.entries(fmts).map(([k, v]) => `${v} ${k}`).join(', ')
                          return fmtStr || `${tableCount} tables`
                        })() : s3.sync_status === 'syncing' ? 'Scanning...' : s3.status}
                      </Badge>
                    </button>
                    {isExpanded && (
                      <div className="ml-4">
                        {/* Bucket info */}
                        <div className="border-l-2 border-l-emerald-400/20 px-4 py-1.5 space-y-0.5 mb-1">
                          <p className="text-2xs text-zinc-500"><span className="text-zinc-600">Bucket:</span> {s3.bucket}</p>
                          <p className="text-2xs text-zinc-500"><span className="text-zinc-600">Endpoint:</span> {s3.endpoint}</p>
                          <p className="text-2xs text-zinc-500"><span className="text-zinc-600">Region:</span> {s3.region}</p>
                        </div>
                        {/* Schema → Table tree */}
                        {[...schemas.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([schema, schemaTables]) => {
                          const schemaKey = `s3-schema-${s3.name}-${schema}`
                          const isSchemaExpanded = expandedConn.has(schemaKey)
                          const filteredTables = catalogFilter
                            ? schemaTables.filter((t: string) => t.toLowerCase().includes(catalogFilter.toLowerCase()) || schema.toLowerCase().includes(catalogFilter.toLowerCase()))
                            : schemaTables
                          if (filteredTables.length === 0) return null
                          return (
                            <div key={schemaKey}>
                              <button
                                onClick={() => toggleConn(schemaKey)}
                                className="w-full flex items-center gap-1.5 px-3 py-1.5 hover:bg-white/[0.02] transition-colors"
                              >
                                {isSchemaExpanded
                                  ? <ChevronDown className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                  : <ChevronRight className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                }
                                <Layers className="w-3 h-3 text-emerald-400/60 flex-shrink-0" />
                                <span className="text-2xs font-medium text-zinc-400 truncate">{schema}</span>
                                <span className="ml-auto text-2xs text-zinc-700">{filteredTables.length}</span>
                              </button>
                              {isSchemaExpanded && (
                                <div className="ml-4 border-l border-white/[0.04]">
                                  {filteredTables.map((tblName: string) => {
                                    const fullName = `${schema}.${tblName}`
                                    const isTableExpanded = expandedTable === fullName
                                    const cols = tableSchemas[fullName]
                                    return (
                                      <div key={fullName}>
                                        <div className="flex items-center group">
                                          <button
                                            onClick={() => handleExpandTable(fullName)}
                                            className="flex-1 flex items-center gap-1.5 px-3 py-1 hover:bg-white/[0.02] transition-colors min-w-0"
                                          >
                                            {isTableExpanded
                                              ? <ChevronDown className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                              : <ChevronRight className="w-2.5 h-2.5 text-zinc-600 flex-shrink-0" />
                                            }
                                            {(() => {
                                              const tt = (s3.table_types || {})[fullName] || ''
                                              const fmt = (s3.table_formats || {})[fullName] || ''
                                              const isMV = tt.toUpperCase().includes('MATERIALIZED')
                                              const isView = !isMV && tt.toUpperCase().includes('VIEW')
                                              const isDelta = fmt === 'delta'
                                              const isHudi = fmt === 'hudi'
                                              const isPq = fmt === 'parquet'
                                              const iconColor = isMV ? 'text-violet-400' : isView ? 'text-sky-400' : isDelta ? 'text-yellow-400' : isHudi ? 'text-orange-400' : isPq ? 'text-zinc-400' : 'text-sky-400/60'
                                              return isMV || isView
                                                ? <Layers className={`w-3 h-3 ${iconColor} flex-shrink-0`} />
                                                : <Table2 className={`w-3 h-3 ${iconColor} flex-shrink-0`} />
                                            })()}
                                            <span className="text-2xs font-mono text-zinc-400 truncate">{tblName}</span>
                                            {(() => {
                                              const tt = (s3.table_types || {})[fullName] || ''
                                              const fmt = (s3.table_formats || {})[fullName] || ''
                                              const isMV = tt.toUpperCase().includes('MATERIALIZED')
                                              const isView = !isMV && tt.toUpperCase().includes('VIEW')
                                              if (isMV) return <span className="text-2xs text-violet-400/70 ml-1 flex-shrink-0">MV</span>
                                              if (isView) return <span className="text-2xs text-sky-400/70 ml-1 flex-shrink-0">VIEW</span>
                                              if (fmt && fmt !== 'iceberg') return <span className={`text-2xs ml-1 flex-shrink-0 ${fmt === 'delta' ? 'text-yellow-400/60' : fmt === 'hudi' ? 'text-orange-400/60' : 'text-zinc-500'}`}>{fmt}</span>
                                              return null
                                            })()}
                                          </button>
                                          <Tooltip content="Insert SELECT query" position="left">
                                            <button
                                              onClick={() => insertAtCursor(`SELECT * FROM ${fullName} LIMIT 100;`)}
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
                                                onClick={() => insertAtCursor(`${fullName}.${col.name}`)}
                                                className="w-full flex items-center gap-1.5 px-3 py-1 hover:bg-white/[0.02] transition-colors cursor-pointer"
                                                title={`Insert ${fullName}.${col.name}`}
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
                        {tableCount === 0 && (
                          <p className="px-4 py-2 text-2xs text-zinc-600 italic">No tables discovered yet</p>
                        )}
                      </div>
                    )}
                  </div>
                )
              })}


              {/* Empty state */}
              {tables.length === 0 && connections.length === 0 && s3Configs.length === 0 && (
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
                  <div
                    key={q.id}
                    className="flex items-center border-b border-zinc-800/20 hover:bg-surface-3/40 transition-colors group"
                  >
                    <button
                      onClick={() => store.updateTabSql(store.activeTabId, q.sql)}
                      className="flex-1 text-left px-3 py-2 min-w-0"
                    >
                      <p className="text-xs text-zinc-300 truncate">{q.name}</p>
                      <p className="text-2xs text-zinc-600 font-mono truncate mt-0.5">{q.sql}</p>
                    </button>
                    <button
                      onClick={(e) => { e.stopPropagation(); store.deleteSavedQuery(q.id) }}
                      className="px-2 py-2 opacity-0 group-hover:opacity-100 transition-opacity text-zinc-600 hover:text-rose-400"
                      title="Delete saved query"
                    >
                      <Trash2 className="w-3 h-3" />
                    </button>
                  </div>
                ))
              )}
            </div>
          )}
          </>
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

      {/* Command Palette (Cmd+K) */}
      <CommandPalette
        open={cmdPaletteOpen}
        onClose={() => setCmdPaletteOpen(false)}
        tables={tables}
        savedQueries={store.savedQueries}
        onRunQuery={runQuery}
        onNewTab={() => store.addTab()}
        onExportCsv={() => {
          if (!result) return
          const header = result.columns.join(',')
          const csvRows = result.rows.map((r: Record<string, unknown>) => result.columns.map((c: string) => {
            const v = r[c]; const s = v === null || v === undefined ? '' : String(v)
            return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s.replace(/"/g, '""')}"` : s
          }).join(',')).join('\n')
          const blob = new Blob([header + '\n' + csvRows], { type: 'text/csv' })
          const url = URL.createObjectURL(blob)
          const a = document.createElement('a'); a.href = url; a.download = `query_result_${Date.now()}.csv`; a.click()
          URL.revokeObjectURL(url)
        }}
        onInsertSql={(sql) => store.updateTabSql(store.activeTabId, sql)}
      />
    </div>
  )
}
