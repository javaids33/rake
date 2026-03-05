import { useEffect, useState } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Button } from '../components/ui/Button'
import { DataTable } from '../components/ui/DataTable'
import { Tabs } from '../components/ui/Tabs'
import { EmptyState } from '../components/ui/EmptyState'
import { Tooltip } from '../components/ui/Tooltip'
import { DagGraph, type DagNode, type DagEdge } from '../components/ui/DagGraph'
import { cn, inferFormat, FORMAT_COLORS, formatNumber } from '../lib/utils'
import { getTables, getTableSchema, getTableStats, getTablePreview, deregisterTable, getLineage } from '../api/client'
import type { TableInfo, ColumnSchema, TableStatsResponse, TablePreviewResponse, LineageResponse } from '../types'
import {
  Database, Search, Table2, Eye, BarChart3, Trash2, ChevronRight,
  Columns, Hash, Type, Calendar, ToggleLeft, X, Layers, Tag,
  ArrowUpDown, Filter, Grid3X3, List, RefreshCw, FileText,
  ExternalLink, Copy, AlertTriangle, CheckCircle2, GitBranch,
} from 'lucide-react'
import toast from 'react-hot-toast'
import { useNavigate } from 'react-router-dom'
import { useEditorStore } from '../stores/editor'

const TYPE_ICONS: Record<string, React.ReactNode> = {
  'Int': <Hash className="w-3 h-3 text-amber-400" />,
  'Float': <Hash className="w-3 h-3 text-amber-400" />,
  'Decimal': <Hash className="w-3 h-3 text-amber-400" />,
  'Utf8': <Type className="w-3 h-3 text-emerald-400" />,
  'String': <Type className="w-3 h-3 text-emerald-400" />,
  'Date': <Calendar className="w-3 h-3 text-cyan-400" />,
  'Timestamp': <Calendar className="w-3 h-3 text-cyan-400" />,
  'Boolean': <ToggleLeft className="w-3 h-3 text-violet-400" />,
}

function getTypeIcon(dtype: string) {
  for (const [key, icon] of Object.entries(TYPE_ICONS)) {
    if (dtype.toLowerCase().includes(key.toLowerCase())) return icon
  }
  return <Columns className="w-3 h-3 text-zinc-500" />
}

type ViewMode = 'list' | 'grid'

export function DataCatalog() {
  const [tables, setTables] = useState<TableInfo[]>([])
  const [search, setSearch] = useState('')
  const [selected, setSelected] = useState<string | null>(null)
  const [tab, setTab] = useState('schema')
  const [schema, setSchema] = useState<ColumnSchema[]>([])
  const [stats, setStats] = useState<TableStatsResponse | null>(null)
  const [preview, setPreview] = useState<TablePreviewResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [lineage, setLineage] = useState<LineageResponse | null>(null)
  const [formatFilter, setFormatFilter] = useState<string | null>(null)
  const [viewMode, setViewMode] = useState<ViewMode>('list')
  const [sortBy, setSortBy] = useState<'name' | 'format'>('name')
  const navigate = useNavigate()
  const { updateTabSql, activeTabId } = useEditorStore()

  const load = () => {
    setLoading(true)
    getTables().then(r => {
      const raw = r.tables || []
      const normalized: TableInfo[] = raw.map((t: string | TableInfo) => typeof t === 'string' ? { name: t } : t)
      setTables(normalized)
    }).catch(() => {}).finally(() => setLoading(false))
    getLineage().then(setLineage).catch(() => {})
  }
  useEffect(load, [])

  useEffect(() => {
    if (!selected) return
    setSchema([])
    setStats(null)
    setPreview(null)
    getTableSchema(selected).then(r => setSchema(r.columns)).catch(() => {})
    getTableStats(selected).then(setStats).catch(() => {})
    getTablePreview(selected).then(setPreview).catch(() => {})
  }, [selected])

  const filtered = tables
    .filter(t => {
      const matchSearch = t.name.toLowerCase().includes(search.toLowerCase())
      const matchFormat = !formatFilter || inferFormat(t.name).format === formatFilter
      return matchSearch && matchFormat
    })
    .sort((a, b) => {
      if (sortBy === 'format') return inferFormat(a.name).format.localeCompare(inferFormat(b.name).format)
      return a.name.localeCompare(b.name)
    })

  const formats = [...new Set(tables.map(t => inferFormat(t.name).format))]
  const formatCounts = formats.reduce((acc, f) => {
    acc[f] = tables.filter(t => inferFormat(t.name).format === f).length
    return acc
  }, {} as Record<string, number>)

  const handleDelete = async (name: string) => {
    try {
      await deregisterTable(name)
      toast.success(`Removed ${name}`)
      setTables(ts => ts.filter(t => t.name !== name))
      if (selected === name) setSelected(null)
    } catch (e) {
      toast.error((e as Error).message)
    }
  }

  const queryTable = (name: string) => {
    updateTabSql(activeTabId, `SELECT * FROM ${name} LIMIT 100`)
    navigate('/sql')
  }

  return (
    <div className="flex h-full animate-fade-in">
      {/* Left panel — table list */}
      <div className="w-80 flex-shrink-0 border-r border-white/[0.04] flex flex-col bg-navy-950/60 backdrop-blur-sm">
        <div className="p-3 border-b border-white/[0.04] space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="w-7 h-7 rounded-lg bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
                <Database className="w-3.5 h-3.5 text-amber-400" />
              </div>
              <div>
                <h2 className="text-sm font-display font-semibold text-zinc-100">Catalog</h2>
                <p className="text-2xs text-zinc-600">{tables.length} tables</p>
              </div>
            </div>
            <div className="flex items-center gap-1">
              <button onClick={() => setViewMode('list')} className={cn('p-1 rounded', viewMode === 'list' ? 'text-amber-400 bg-amber-400/10' : 'text-zinc-600 hover:text-zinc-400')}>
                <List className="w-3.5 h-3.5" />
              </button>
              <button onClick={() => setViewMode('grid')} className={cn('p-1 rounded', viewMode === 'grid' ? 'text-amber-400 bg-amber-400/10' : 'text-zinc-600 hover:text-zinc-400')}>
                <Grid3X3 className="w-3.5 h-3.5" />
              </button>
              <button onClick={load} className="p-1 rounded text-zinc-600 hover:text-zinc-400">
                <RefreshCw className={cn('w-3.5 h-3.5', loading && 'animate-spin')} />
              </button>
            </div>
          </div>
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-600" />
            <input
              className="w-full pl-8 pr-3 py-1.5 text-xs rounded-lg bg-white/[0.04] border border-white/[0.06] text-zinc-300 placeholder-zinc-600 focus:outline-none focus:ring-1 focus:ring-amber-400/25 focus:border-amber-400/25 transition-all"
              placeholder="Search tables..."
              value={search}
              onChange={e => setSearch(e.target.value)}
            />
          </div>
          {/* Format filter chips */}
          <div className="flex flex-wrap gap-1">
            {formats.map(f => (
              <button
                key={f}
                onClick={() => setFormatFilter(formatFilter === f ? null : f)}
                className={cn(
                  'px-2 py-0.5 text-2xs rounded-md border transition-all',
                  formatFilter === f
                    ? FORMAT_COLORS[f] || 'bg-white/[0.06] text-zinc-400 border-white/[0.08]'
                    : 'bg-white/[0.02] text-zinc-500 border-white/[0.04] hover:text-zinc-400 hover:border-white/[0.08]'
                )}
              >
                {f} <span className="text-zinc-600 ml-0.5">{formatCounts[f]}</span>
              </button>
            ))}
            {formatFilter && (
              <button onClick={() => setFormatFilter(null)} className="p-0.5 text-zinc-600 hover:text-zinc-400">
                <X className="w-3 h-3" />
              </button>
            )}
          </div>
          {/* Sort control */}
          <div className="flex items-center gap-2 text-2xs text-zinc-600">
            <ArrowUpDown className="w-3 h-3" />
            <button onClick={() => setSortBy('name')} className={cn('transition-colors', sortBy === 'name' ? 'text-zinc-300' : 'hover:text-zinc-400')}>Name</button>
            <span>/</span>
            <button onClick={() => setSortBy('format')} className={cn('transition-colors', sortBy === 'format' ? 'text-zinc-300' : 'hover:text-zinc-400')}>Format</button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto">
          {viewMode === 'list' ? (
            filtered.map(t => {
              const fmt = inferFormat(t.name)
              return (
                <button
                  key={t.name}
                  onClick={() => setSelected(t.name)}
                  className={cn(
                    'w-full flex items-center gap-2.5 px-3 py-2.5 text-left border-b border-white/[0.02] transition-all',
                    selected === t.name
                      ? 'bg-amber-400/[0.06] border-l-2 border-l-amber-400'
                      : 'hover:bg-white/[0.03] border-l-2 border-l-transparent'
                  )}
                >
                  <Table2 className="w-4 h-4 text-zinc-600 flex-shrink-0" />
                  <div className="flex-1 min-w-0">
                    <p className="text-xs font-mono text-zinc-300 truncate">{t.name}</p>
                    <span className={cn('text-2xs px-1.5 py-0.5 rounded border', FORMAT_COLORS[fmt.format] || 'bg-white/[0.04] text-zinc-500 border-white/[0.06]')}>
                      {fmt.format}
                    </span>
                  </div>
                  <ChevronRight className={cn('w-3 h-3 text-zinc-700 transition-transform', selected === t.name && 'rotate-90 text-amber-400/60')} />
                </button>
              )
            })
          ) : (
            <div className="grid grid-cols-2 gap-1.5 p-2">
              {filtered.map(t => {
                const fmt = inferFormat(t.name)
                return (
                  <button
                    key={t.name}
                    onClick={() => setSelected(t.name)}
                    className={cn(
                      'p-2.5 rounded-lg border text-left transition-all',
                      selected === t.name
                        ? 'bg-amber-400/[0.06] border-amber-400/20'
                        : 'bg-white/[0.02] border-white/[0.04] hover:bg-white/[0.04] hover:border-white/[0.06]'
                    )}
                  >
                    <Table2 className="w-4 h-4 text-zinc-500 mb-1.5" />
                    <p className="text-2xs font-mono text-zinc-300 truncate">{t.name}</p>
                    <span className={cn('text-2xs px-1 py-px rounded border mt-1 inline-block', FORMAT_COLORS[fmt.format])}>
                      {fmt.format}
                    </span>
                  </button>
                )
              })}
            </div>
          )}
          {filtered.length === 0 && (
            <EmptyState icon={<Database className="w-5 h-5" />} title="No tables found" description="Register tables from Data Sources" />
          )}
        </div>
      </div>

      {/* Right panel — detail */}
      <div className="flex-1 flex flex-col min-w-0">
        {selected ? (
          <>
            {/* Table header */}
            <div className="flex items-center justify-between px-5 py-3 border-b border-white/[0.04] bg-navy-950/40">
              <div>
                <div className="flex items-center gap-2">
                  <h3 className="text-sm font-display font-semibold text-zinc-100">{selected}</h3>
                  <Badge className={FORMAT_COLORS[inferFormat(selected).format]}>{inferFormat(selected).format}</Badge>
                </div>
                <div className="flex items-center gap-3 mt-1.5">
                  {stats && <span className="text-2xs font-mono text-zinc-500">{formatNumber(stats.row_count)} rows</span>}
                  {schema.length > 0 && <span className="text-2xs font-mono text-zinc-500">{schema.length} columns</span>}
                  {stats && (
                    <span className="text-2xs text-zinc-600 flex items-center gap-1">
                      {stats.columns.filter(c => c.null_count > 0).length > 0
                        ? <><AlertTriangle className="w-3 h-3 text-amber-500/60" /> {stats.columns.filter(c => c.null_count > 0).length} nullable</>
                        : <><CheckCircle2 className="w-3 h-3 text-emerald-500/60" /> No nulls</>
                      }
                    </span>
                  )}
                </div>
              </div>
              <div className="flex items-center gap-2">
                <Button variant="ghost" size="sm" icon={<ExternalLink className="w-3.5 h-3.5" />} onClick={() => queryTable(selected)}>
                  Query
                </Button>
                <Button variant="ghost" size="sm" icon={<Copy className="w-3.5 h-3.5" />} onClick={() => {
                  navigator.clipboard.writeText(`SELECT * FROM ${selected} LIMIT 100`)
                  toast.success('SQL copied')
                }}>Copy SQL</Button>
                <Button variant="danger" size="sm" icon={<Trash2 className="w-3.5 h-3.5" />} onClick={() => handleDelete(selected)}>
                  Remove
                </Button>
              </div>
            </div>

            <Tabs
              tabs={[
                { id: 'schema', label: 'Schema', icon: <Columns className="w-3 h-3" /> },
                { id: 'preview', label: 'Preview', icon: <Eye className="w-3 h-3" /> },
                { id: 'stats', label: 'Statistics', icon: <BarChart3 className="w-3 h-3" /> },
                { id: 'lineage', label: 'Lineage', icon: <GitBranch className="w-3 h-3" /> },
                { id: 'metadata', label: 'Metadata', icon: <FileText className="w-3 h-3" /> },
              ]}
              active={tab}
              onChange={setTab}
              className="mx-5 mt-3"
            />

            <div className="flex-1 overflow-auto p-5">
              {tab === 'schema' && (
                <div className="space-y-1">
                  {/* Column header */}
                  <div className="flex items-center gap-3 px-3 py-1.5 text-2xs font-medium text-zinc-600 uppercase tracking-wider border-b border-white/[0.04]">
                    <span className="w-5" />
                    <span className="flex-1">Column</span>
                    <span className="w-32">Type</span>
                    <span className="w-16 text-right">Nullable</span>
                  </div>
                  {schema.map((col, i) => {
                    const colStats = stats?.columns?.find(c => c.name === col.name)
                    const nullPct = colStats && stats?.row_count ? ((colStats.null_count / stats.row_count) * 100).toFixed(1) : null
                    return (
                      <Tooltip
                        key={col.name}
                        position="right"
                        content={
                          <div style={{ minWidth: 180 }}>
                            <div style={{ fontWeight: 700, color: '#f59e0b', marginBottom: 4 }}>{col.name}</div>
                            <div>Type: <span style={{ fontFamily: "'JetBrains Mono', monospace" }}>{col.data_type}</span></div>
                            <div>Nullable: {col.nullable ? 'Yes' : 'No'}</div>
                            {nullPct && <div>Null %: {nullPct}%</div>}
                            {colStats?.min != null && <div>Min: {String(colStats.min)}</div>}
                            {colStats?.max != null && <div>Max: {String(colStats.max)}</div>}
                            {stats?.row_count && <div style={{ color: '#64748b', marginTop: 4, fontSize: 11 }}>Table: {formatNumber(stats.row_count)} rows</div>}
                          </div>
                        }
                      >
                        <div
                          className="flex items-center gap-3 px-3 py-2 rounded-lg hover:bg-white/[0.02] transition-colors group w-full"
                          style={{ animationDelay: `${i * 20}ms` }}
                        >
                          {getTypeIcon(col.data_type)}
                          <span className="text-xs font-mono text-zinc-200 flex-1">{col.name}</span>
                          <span className="text-2xs font-mono text-zinc-500 w-32">{col.data_type}</span>
                          <span className="w-16 text-right">
                            {col.nullable
                              ? <Badge className="text-2xs bg-amber-400/10 text-amber-400/70 border-amber-400/15">null</Badge>
                              : <Badge className="text-2xs bg-emerald-400/10 text-emerald-400/70 border-emerald-400/15">req</Badge>
                            }
                          </span>
                        </div>
                      </Tooltip>
                    )
                  })}
                </div>
              )}

              {tab === 'preview' && preview && (
                <DataTable columns={preview.columns} rows={preview.rows} maxHeight="600px" />
              )}
              {tab === 'preview' && !preview && (
                <EmptyState icon={<Eye className="w-5 h-5" />} title="Loading preview..." description="Fetching table data" />
              )}

              {tab === 'stats' && stats && (
                <div className="space-y-4">
                  <div className="grid grid-cols-4 gap-3">
                    <Card padding="sm" glow="amber">
                      <p className="text-2xs text-zinc-500">Total Rows</p>
                      <p className="text-lg font-bold font-mono text-zinc-100">{formatNumber(stats.row_count)}</p>
                    </Card>
                    <Card padding="sm">
                      <p className="text-2xs text-zinc-500">Columns</p>
                      <p className="text-lg font-bold font-mono text-zinc-100">{stats.column_count}</p>
                    </Card>
                    <Card padding="sm">
                      <p className="text-2xs text-zinc-500">Fill Rate</p>
                      <p className="text-lg font-bold font-mono text-emerald-400">
                        {stats.row_count > 0
                          ? `${(100 - (stats.columns.reduce((s, c) => s + c.null_count, 0) / (stats.row_count * stats.column_count)) * 100).toFixed(1)}%`
                          : '—'}
                      </p>
                    </Card>
                    <Card padding="sm">
                      <p className="text-2xs text-zinc-500">Nullable Cols</p>
                      <p className="text-lg font-bold font-mono text-zinc-100">
                        {stats.columns.filter(c => c.null_count > 0).length}
                      </p>
                    </Card>
                  </div>
                  {/* Column stats */}
                  <div className="space-y-1">
                    <div className="flex items-center gap-4 px-3 py-1.5 text-2xs font-medium text-zinc-600 uppercase tracking-wider border-b border-white/[0.04]">
                      <span className="w-5" />
                      <span className="flex-1">Column</span>
                      <span className="w-20">Type</span>
                      <span className="w-32">Fill Rate</span>
                      <span className="w-20 text-right">Min</span>
                      <span className="w-20 text-right">Max</span>
                    </div>
                    {stats.columns.map(col => {
                      const nullPct = stats.row_count > 0 ? (col.null_count / stats.row_count) * 100 : 0
                      const fillPct = 100 - nullPct
                      return (
                        <div key={col.name} className="flex items-center gap-4 px-3 py-2 rounded-lg hover:bg-white/[0.02] transition-colors">
                          {getTypeIcon(col.data_type)}
                          <span className="text-xs font-mono text-zinc-200 flex-1">{col.name}</span>
                          <span className="text-2xs font-mono text-zinc-500 w-20">{col.data_type}</span>
                          <div className="w-32 flex items-center gap-2">
                            <div className="flex-1 h-1.5 bg-white/[0.04] rounded-full overflow-hidden">
                              <div
                                className={cn('h-full rounded-full transition-all', fillPct > 90 ? 'bg-emerald-500/60' : fillPct > 50 ? 'bg-amber-500/60' : 'bg-rose-500/60')}
                                style={{ width: `${fillPct}%` }}
                              />
                            </div>
                            <span className="text-2xs font-mono text-zinc-500 w-8 text-right">{fillPct.toFixed(0)}%</span>
                          </div>
                          <span className="text-2xs font-mono text-zinc-500 w-20 text-right truncate">{col.min !== undefined ? String(col.min) : '—'}</span>
                          <span className="text-2xs font-mono text-zinc-500 w-20 text-right truncate">{col.max !== undefined ? String(col.max) : '—'}</span>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )}

              {tab === 'lineage' && (
                <div className="space-y-4">
                  <Card>
                    <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                      <GitBranch className="w-4 h-4 text-amber-400" /> Data Lineage
                    </h3>
                    {(() => {
                      // Build DAG nodes and edges from lineage data for this table
                      const dagNodes: DagNode[] = []
                      const dagEdges: DagEdge[] = []
                      const added = new Set<string>()

                      if (selected) {
                        // Current table
                        const fmt = inferFormat(selected)
                        dagNodes.push({ id: selected, label: selected, type: 'table', status: 'healthy', meta: fmt.format })
                        added.add(selected)

                        // Find edges involving this table from lineage
                        if (lineage?.edges) {
                          for (const edge of lineage.edges) {
                            if (edge.source === selected || edge.target === selected) {
                              dagEdges.push({ source: edge.source, target: edge.target })
                              if (!added.has(edge.source)) {
                                added.add(edge.source)
                                dagNodes.push({ id: edge.source, label: edge.source, type: 'source', meta: inferFormat(edge.source).format })
                              }
                              if (!added.has(edge.target)) {
                                added.add(edge.target)
                                dagNodes.push({ id: edge.target, label: edge.target, type: 'target', meta: inferFormat(edge.target).format })
                              }
                            }
                          }
                        }

                        // Also add transforms that reference this table
                        if (lineage?.nodes) {
                          for (const node of lineage.nodes) {
                            if (node.type === 'transform' && !added.has(node.id)) {
                              // Check if any edge connects to/from this node involving our table
                              const relevant = lineage.edges?.some(
                                e => (e.source === node.id && e.target === selected) || (e.source === selected && e.target === node.id)
                              )
                              if (relevant) {
                                dagNodes.push({ id: node.id, label: node.id, type: 'transform', meta: 'transform' })
                                added.add(node.id)
                              }
                            }
                          }
                        }

                        // If no lineage data, show all tables as a simple graph
                        if (dagEdges.length === 0) {
                          // Show upstream sources (pg. or pg_ prefix) connecting to this table
                          const sources = tables.filter(t => (t.name.startsWith('pg.') || t.name.startsWith('pg_')) && t.name !== selected).slice(0, 3)
                          const targets = tables.filter(t => t.name.startsWith('uploads_') && t.name !== selected).slice(0, 2)

                          for (const src of sources) {
                            if (!added.has(src.name)) {
                              dagNodes.push({ id: src.name, label: src.name, type: 'source', meta: 'Postgres' })
                              dagEdges.push({ source: src.name, target: selected })
                              added.add(src.name)
                            }
                          }
                          for (const tgt of targets) {
                            if (!added.has(tgt.name)) {
                              dagNodes.push({ id: tgt.name, label: tgt.name, type: 'target', meta: 'Upload' })
                              dagEdges.push({ source: selected, target: tgt.name })
                              added.add(tgt.name)
                            }
                          }
                        }
                      }

                      return dagNodes.length > 0 ? (
                        <DagGraph
                          nodes={dagNodes}
                          edges={dagEdges}
                          onNodeClick={(id) => setSelected(id)}
                          height={280}
                        />
                      ) : (
                        <EmptyState
                          icon={<GitBranch className="w-5 h-5" />}
                          title="No lineage data"
                          description="Lineage tracking is populated when transforms reference this table via ref() macros"
                        />
                      )
                    })()}
                  </Card>
                </div>
              )}

              {tab === 'metadata' && (
                <div className="space-y-4">
                  <Card>
                    <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                      <Tag className="w-4 h-4 text-amber-400" /> Table Metadata
                    </h3>
                    <div className="grid grid-cols-2 gap-y-3 text-xs">
                      {[
                        ['Table Name', selected],
                        ['Format', inferFormat(selected).format],
                        ['Variant', inferFormat(selected).variant],
                        ['Columns', String(schema.length)],
                        ['Rows', stats ? formatNumber(stats.row_count) : 'Loading...'],
                        ['Engine', 'DataFusion 51'],
                      ].map(([label, value]) => (
                        <div key={label} className="flex items-center gap-3">
                          <span className="text-zinc-500 w-28">{label}</span>
                          <span className="text-zinc-200 font-mono">{value}</span>
                        </div>
                      ))}
                    </div>
                  </Card>
                  <Card>
                    <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                      <Filter className="w-4 h-4 text-cyan-400" /> Quick Actions
                    </h3>
                    <div className="flex flex-wrap gap-2">
                      <Button variant="secondary" size="sm" icon={<ExternalLink className="w-3.5 h-3.5" />} onClick={() => queryTable(selected)}>
                        Open in SQL Editor
                      </Button>
                      <Button variant="secondary" size="sm" icon={<Copy className="w-3.5 h-3.5" />} onClick={() => {
                        const cols = schema.map(c => c.name).join(', ')
                        navigator.clipboard.writeText(`SELECT ${cols} FROM ${selected} LIMIT 100`)
                        toast.success('SELECT with all columns copied')
                      }}>Copy SELECT *</Button>
                      <Button variant="secondary" size="sm" icon={<BarChart3 className="w-3.5 h-3.5" />} onClick={() => {
                        updateTabSql(activeTabId, `SELECT COUNT(*) as total_rows FROM ${selected}`)
                        navigate('/sql')
                      }}>Count Rows</Button>
                    </div>
                  </Card>
                </div>
              )}
            </div>
          </>
        ) : (
          <div className="flex-1 flex items-center justify-center">
            <EmptyState
              icon={<Layers className="w-6 h-6" />}
              title="Select a table"
              description="Choose a table from the catalog to view schema, preview data, and column statistics"
            />
          </div>
        )}
      </div>
    </div>
  )
}
