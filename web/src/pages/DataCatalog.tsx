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
import { getTables, getTableSchema, getTableStats, getTablePreview, deregisterTable, getLineage, getTableSnapshots, getMaintenanceStatus, compactTable, expireSnapshots, removeOrphans, getS3Configs, getConnections } from '../api/client'
import type { TableInfo, ColumnSchema, TableStatsResponse, TablePreviewResponse, LineageResponse, IcebergSnapshotResponse, MaintenanceStatus, S3Config, ConnectionEntry } from '../types'
import {
  Database, Search, Table2, Eye, BarChart3, Trash2, ChevronRight, ChevronDown,
  Columns, Hash, Type, Calendar, ToggleLeft, X, Layers, Tag,
  ArrowUpDown, Filter, Grid3X3, List, RefreshCw, FileText,
  ExternalLink, Copy, AlertTriangle, CheckCircle2, GitBranch,
  Clock, Wrench, Play, HardDrive, Cloud,
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
  const [snapshots, setSnapshots] = useState<IcebergSnapshotResponse | null>(null)
  const [maintenance, setMaintenance] = useState<MaintenanceStatus | null>(null)
  const [maintenanceLoading, setMaintenanceLoading] = useState(false)
  const [formatFilter, setFormatFilter] = useState<string | null>(null)
  const [tableDeps, setTableDeps] = useState<Record<string, { feedsInto: number; fedBy: number }>>({})
  const [viewMode, setViewMode] = useState<ViewMode>('list')
  const [sortBy, setSortBy] = useState<'name' | 'format' | 'deps'>('name')
  const [s3Configs, setS3Configs] = useState<S3Config[]>([])
  const [connections, setConnections] = useState<ConnectionEntry[]>([])
  const [collapsedSources, setCollapsedSources] = useState<Set<string>>(new Set())
  const navigate = useNavigate()
  const { updateTabSql, activeTabId } = useEditorStore()

  const toggleSource = (source: string) => {
    setCollapsedSources(prev => {
      const next = new Set(prev)
      if (next.has(source)) next.delete(source)
      else next.add(source)
      return next
    })
  }

  const load = () => {
    setLoading(true)
    getTables().then(r => {
      const raw = r.tables || []
      const normalized: TableInfo[] = raw.map((t: string | TableInfo) => typeof t === 'string' ? { name: t } : t)
      setTables(normalized)
    }).catch(() => {}).finally(() => setLoading(false))
    getLineage().then(data => {
      setLineage(data)
      const deps: Record<string, { feedsInto: number; fedBy: number }> = {}
      if (data?.edges) {
        for (const edge of data.edges) {
          if (!deps[edge.source]) deps[edge.source] = { feedsInto: 0, fedBy: 0 }
          if (!deps[edge.target]) deps[edge.target] = { feedsInto: 0, fedBy: 0 }
          deps[edge.source].feedsInto++
          deps[edge.target].fedBy++
        }
      }
      setTableDeps(deps)
    }).catch(() => {})
    getS3Configs().then(r => setS3Configs(r.configs || [])).catch(() => {})
    getConnections().then(r => setConnections(r.connections || [])).catch(() => {})
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
    getTableSnapshots(selected).then(setSnapshots).catch(() => setSnapshots(null))
    getMaintenanceStatus(selected).then(setMaintenance).catch(() => setMaintenance(null))
  }, [selected])

  const filtered = tables
    .filter(t => {
      const matchSearch = t.name.toLowerCase().includes(search.toLowerCase())
      const matchFormat = !formatFilter || inferFormat(t.name).format === formatFilter
      return matchSearch && matchFormat
    })
    .sort((a, b) => {
      if (sortBy === 'format') return inferFormat(a.name).format.localeCompare(inferFormat(b.name).format)
      if (sortBy === 'deps') {
        const aDeps = (tableDeps[a.name]?.feedsInto || 0) + (tableDeps[a.name]?.fedBy || 0)
        const bDeps = (tableDeps[b.name]?.feedsInto || 0) + (tableDeps[b.name]?.fedBy || 0)
        return bDeps - aDeps
      }
      return a.name.localeCompare(b.name)
    })

  // Build S3 tables using their actual registered names (s3_* prefix)
  const s3TableNames = new Set(s3Configs.flatMap(s3 => s3.tables || []))
  const allTableNames = new Set(tables.map(t => t.name))
  // Add S3 tables not already registered in DataFusion
  const s3OnlyTables: TableInfo[] = [...s3TableNames]
    .filter(name => !allTableNames.has(name))
    .map(name => ({ name }))
  const mergedTables = [...tables, ...s3OnlyTables]

  // Build S3 metadata lookup: table name -> { config name, bucket, format }
  const s3MetaMap = new Map<string, { configName: string; bucket: string; format: string; endpoint: string }>()
  for (const s3 of s3Configs) {
    for (const t of (s3.tables || [])) {
      s3MetaMap.set(t, {
        configName: s3.name,
        bucket: s3.bucket,
        format: (s3 as any).table_formats?.[t] || 'parquet',
        endpoint: s3.endpoint,
      })
    }
  }

  const allFiltered = mergedTables
    .filter(t => {
      const matchSearch = t.name.toLowerCase().includes(search.toLowerCase())
      const matchFormat = !formatFilter || inferFormat(t.name).format === formatFilter
      return matchSearch && matchFormat
    })
    .sort((a, b) => a.name.localeCompare(b.name))

  // Group tables by source
  type SourceGroup = { label: string; icon: React.ReactNode; color: string; tables: TableInfo[]; key: string }
  const sourceGroups: SourceGroup[] = []
  const groupMap = new Map<string, TableInfo[]>()

  const SOURCE_META: Record<string, { label: string; icon: React.ReactNode; color: string }> = {
    pg: { label: 'PostgreSQL', icon: <Database className="w-3.5 h-3.5 text-blue-400" />, color: 'text-blue-400' },
    mysql: { label: 'MySQL', icon: <Database className="w-3.5 h-3.5 text-orange-400" />, color: 'text-orange-400' },
    mongo: { label: 'MongoDB', icon: <Database className="w-3.5 h-3.5 text-emerald-400" />, color: 'text-emerald-400' },
    s3: { label: 'S3 / MinIO', icon: <Cloud className="w-3.5 h-3.5 text-violet-400" />, color: 'text-violet-400' },
    trino: { label: 'Trino', icon: <Database className="w-3.5 h-3.5 text-cyan-400" />, color: 'text-cyan-400' },
    uploads: { label: 'Uploads', icon: <HardDrive className="w-3.5 h-3.5 text-amber-400" />, color: 'text-amber-400' },
    other: { label: 'Other', icon: <Table2 className="w-3.5 h-3.5 text-zinc-400" />, color: 'text-zinc-400' },
  }

  for (const t of allFiltered) {
    let prefix: string
    const dotIdx = t.name.indexOf('.')
    if (dotIdx > 0) {
      prefix = t.name.substring(0, dotIdx)
    } else if (s3TableNames.has(t.name) || t.name.startsWith('s3_')) {
      prefix = 's3'
    } else if (t.name.startsWith('uploads_')) {
      prefix = 'uploads'
    } else {
      prefix = 'other'
    }
    if (!groupMap.has(prefix)) groupMap.set(prefix, [])
    groupMap.get(prefix)!.push(t)
  }

  for (const [prefix, tbls] of groupMap) {
    const meta = SOURCE_META[prefix] || { label: prefix, icon: <Database className="w-3.5 h-3.5 text-zinc-400" />, color: 'text-zinc-400' }
    sourceGroups.push({ key: prefix, label: meta.label, icon: meta.icon, color: meta.color, tables: tbls })
  }
  sourceGroups.sort((a, b) => a.label.localeCompare(b.label))

  const formats = [...new Set(mergedTables.map(t => inferFormat(t.name).format))]
  const formatCounts = formats.reduce((acc, f) => {
    acc[f] = mergedTables.filter(t => inferFormat(t.name).format === f).length
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
                <p className="text-2xs text-zinc-600">{mergedTables.length} tables</p>
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
            <span>/</span>
            <button onClick={() => setSortBy('deps')} className={cn('transition-colors', sortBy === 'deps' ? 'text-zinc-300' : 'hover:text-zinc-400')}>Deps</button>
          </div>
        </div>

        <div className="flex-1 overflow-y-auto">
          {viewMode === 'list' ? (
            sourceGroups.map(group => {
              const isCollapsed = collapsedSources.has(group.key)
              return (
                <div key={group.key}>
                  <button
                    onClick={() => toggleSource(group.key)}
                    className="w-full flex items-center gap-2 px-3 py-2 text-left bg-white/[0.02] border-b border-white/[0.04] hover:bg-white/[0.04] transition-all sticky top-0 z-10"
                  >
                    {isCollapsed ? <ChevronRight className="w-3 h-3 text-zinc-500" /> : <ChevronDown className="w-3 h-3 text-zinc-500" />}
                    {group.icon}
                    <span className={cn('text-xs font-semibold', group.color)}>{group.label}</span>
                    <span className="text-2xs text-zinc-600 ml-auto">{group.tables.length}</span>
                  </button>
                  {!isCollapsed && group.tables.map(t => {
                    const fmt = inferFormat(t.name)
                    const shortName = t.name.includes('.') ? t.name.substring(t.name.indexOf('.') + 1) : t.name
                    return (
                      <button
                        key={t.name}
                        onClick={() => setSelected(t.name)}
                        className={cn(
                          'w-full flex items-center gap-2.5 pl-9 pr-3 py-2 text-left border-b border-white/[0.02] transition-all',
                          selected === t.name
                            ? 'bg-amber-400/[0.06] border-l-2 border-l-amber-400'
                            : 'hover:bg-white/[0.03] border-l-2 border-l-transparent'
                        )}
                      >
                        <Table2 className="w-3.5 h-3.5 text-zinc-600 flex-shrink-0" />
                        <div className="flex-1 min-w-0">
                          <p className="text-xs font-mono text-zinc-300 truncate">{shortName}</p>
                          {tableDeps[t.name] && (tableDeps[t.name].feedsInto > 0 || tableDeps[t.name].fedBy > 0) && (
                            <span className="text-2xs text-zinc-500">
                              {tableDeps[t.name].fedBy > 0 && <span className="text-cyan-400">{tableDeps[t.name].fedBy} in</span>}
                              {tableDeps[t.name].fedBy > 0 && tableDeps[t.name].feedsInto > 0 && ' · '}
                              {tableDeps[t.name].feedsInto > 0 && <span className="text-amber-400">{tableDeps[t.name].feedsInto} out</span>}
                            </span>
                          )}
                        </div>
                        <ChevronRight className={cn('w-3 h-3 text-zinc-700 transition-transform', selected === t.name && 'rotate-90 text-amber-400/60')} />
                      </button>
                    )
                  })}
                </div>
              )
            })
          ) : (
            <div className="grid grid-cols-2 gap-1.5 p-2">
              {allFiltered.map(t => {
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
          {allFiltered.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 px-6 text-center">
              <Database className="w-8 h-8 text-zinc-700 mb-3" />
              <h3 className="text-sm font-semibold text-zinc-300 mb-1">No tables registered</h3>
              <p className="text-2xs text-zinc-500 mb-4 max-w-xs">Connect a database or upload a file to start discovering your data</p>
              <div className="flex gap-2">
                <Button variant="primary" size="sm" onClick={() => navigate('/sources')}>
                  Add Data Source
                </Button>
                <Button variant="secondary" size="sm" onClick={() => navigate('/sql')}>
                  Upload File
                </Button>
              </div>
            </div>
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
                  {(() => {
                    let prefix = selected.includes('.') ? selected.substring(0, selected.indexOf('.')) : null
                    if (!prefix && (s3TableNames.has(selected) || selected.startsWith('s3_'))) prefix = 's3'
                    if (!prefix && selected.startsWith('uploads_')) prefix = 'uploads'
                    const meta = prefix && SOURCE_META[prefix]
                    if (!meta) return null
                    return <Badge className={cn('text-2xs', meta.color, 'border-current/20 bg-current/5')}>{meta.label}</Badge>
                  })()}
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
                { id: 'history', label: 'History', icon: <Clock className="w-3 h-3" /> },
                { id: 'maintenance', label: 'Maintenance', icon: <Wrench className="w-3 h-3" /> },
                { id: 'metadata', label: 'Metadata', icon: <FileText className="w-3 h-3" /> },
              ]}
              active={tab}
              onChange={setTab}
              className="mx-5 mt-3"
            />

            <div className="flex-1 overflow-auto p-5">
              {tab === 'schema' && (
                <div className="space-y-4">
                  {schema.length > 0 ? (
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="text-2xs font-medium text-zinc-600 uppercase tracking-wider border-b border-white/[0.04]">
                          <th className="text-left py-2 px-3 w-5"></th>
                          <th className="text-left py-2 px-1">Column</th>
                          <th className="text-left py-2 px-1 w-40">Type</th>
                          <th className="text-center py-2 px-1 w-20">Nullable</th>
                        </tr>
                      </thead>
                      <tbody>
                        {schema.map((col) => {
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
                              <tr className="border-b border-white/[0.02] hover:bg-white/[0.02] transition-colors cursor-default">
                                <td className="py-2 px-3">{getTypeIcon(col.data_type)}</td>
                                <td className="py-2 px-1 font-mono text-zinc-200">{col.name}</td>
                                <td className="py-2 px-1 font-mono text-zinc-500">{col.data_type}</td>
                                <td className="py-2 px-1 text-center">
                                  {col.nullable
                                    ? <Badge className="text-2xs bg-amber-400/10 text-amber-400/70 border-amber-400/15">null</Badge>
                                    : <Badge className="text-2xs bg-emerald-400/10 text-emerald-400/70 border-emerald-400/15">req</Badge>
                                  }
                                </td>
                              </tr>
                            </Tooltip>
                          )
                        })}
                      </tbody>
                    </table>
                  ) : (
                    <div className="py-6 text-center">
                      <Columns className="w-6 h-6 text-zinc-600 mx-auto mb-2" />
                      <p className="text-xs text-zinc-400">Schema not available</p>
                      <p className="text-2xs text-zinc-600 mt-1">Query this table to discover its schema</p>
                    </div>
                  )}
                  {/* S3 metadata panel */}
                  {selected && s3MetaMap.has(selected) && (() => {
                    const meta = s3MetaMap.get(selected)!
                    return (
                      <div className="rounded-lg border border-violet-400/20 bg-violet-400/[0.03] p-4 space-y-2">
                        <div className="flex items-center gap-2 mb-2">
                          <Cloud className="w-4 h-4 text-violet-400" />
                          <span className="text-xs font-semibold text-violet-300">S3 Storage Info</span>
                        </div>
                        <div className="grid grid-cols-2 gap-2 text-xs">
                          <div>
                            <span className="text-zinc-500">Config</span>
                            <p className="text-zinc-200 font-mono">{meta.configName}</p>
                          </div>
                          <div>
                            <span className="text-zinc-500">Bucket</span>
                            <p className="text-zinc-200 font-mono">{meta.bucket}</p>
                          </div>
                          <div>
                            <span className="text-zinc-500">Format</span>
                            <p className="text-zinc-200">
                              <Badge className={meta.format === 'iceberg' ? 'text-violet-400 border-violet-400/20' : 'text-amber-400 border-amber-400/20'}>
                                {meta.format}
                              </Badge>
                            </p>
                          </div>
                          <div>
                            <span className="text-zinc-500">Endpoint</span>
                            <p className="text-zinc-200 font-mono text-2xs truncate">{meta.endpoint}</p>
                          </div>
                        </div>
                      </div>
                    )
                  })()}
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

              {tab === 'history' && (
                <div className="space-y-4">
                  {snapshots && snapshots.snapshot_count > 0 ? (
                    <>
                      <div className="flex items-center gap-3 mb-4">
                        <Badge className="bg-violet-500/10 text-violet-400 border-violet-500/20">
                          {snapshots.snapshot_count} snapshots
                        </Badge>
                        {snapshots.current_snapshot_id && (
                          <span className="text-xs text-zinc-500">
                            Current: <span className="text-zinc-300 font-mono">{snapshots.current_snapshot_id}</span>
                          </span>
                        )}
                      </div>
                      <div className="space-y-2">
                        {snapshots.snapshots.slice().reverse().map((snap, i) => (
                          <Card key={snap.snapshot_id} padding="sm">
                            <div className="flex items-center gap-3">
                              <div className={cn(
                                'w-2 h-2 rounded-full flex-shrink-0',
                                snap.snapshot_id === snapshots.current_snapshot_id ? 'bg-emerald-400' : 'bg-zinc-600'
                              )} />
                              <div className="flex-1 min-w-0">
                                <div className="flex items-center gap-2">
                                  <span className="text-xs font-mono text-zinc-300">{snap.snapshot_id}</span>
                                  <Badge className={cn(
                                    'text-2xs',
                                    snap.operation === 'append' ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20' :
                                    snap.operation === 'replace' ? 'bg-amber-500/10 text-amber-400 border-amber-500/20' :
                                    'bg-zinc-500/10 text-zinc-400 border-zinc-500/20'
                                  )}>{snap.operation}</Badge>
                                  {snap.parent_snapshot_id && (
                                    <span className="text-2xs text-zinc-600">parent: {snap.parent_snapshot_id}</span>
                                  )}
                                </div>
                                <div className="flex items-center gap-4 mt-1 text-2xs text-zinc-500">
                                  <span>{new Date(snap.timestamp_ms).toLocaleString()}</span>
                                  {snap.summary['added-records'] && <span>+{snap.summary['added-records']} rows</span>}
                                  {snap.summary['added-data-files'] && <span>{snap.summary['added-data-files']} files</span>}
                                  {snap.data_files_count > 0 && <span>{snap.data_files_count} data files</span>}
                                </div>
                              </div>
                              <Button variant="ghost" size="sm" icon={<Play className="w-3 h-3" />} onClick={() => {
                                updateTabSql(activeTabId, `SELECT * FROM ${selected} VERSION AS OF ${snap.snapshot_id} LIMIT 100`)
                                navigate('/sql')
                              }}>Query</Button>
                            </div>
                          </Card>
                        ))}
                      </div>
                    </>
                  ) : (
                    <EmptyState
                      icon={<Clock className="w-5 h-5" />}
                      title="No snapshot history"
                      description="This table has no Iceberg snapshots. Snapshot history is available for tables created via CDC pipelines."
                    />
                  )}
                </div>
              )}

              {tab === 'maintenance' && (
                <div className="space-y-4">
                  {maintenance ? (
                    <>
                      <div className="grid grid-cols-4 gap-3">
                        {[
                          { label: 'Total Files', value: maintenance.total_files, color: 'text-zinc-200' },
                          { label: 'Avg File Size', value: maintenance.avg_file_size_bytes > 0 ? `${(maintenance.avg_file_size_bytes / 1024 / 1024).toFixed(1)} MB` : '0', color: 'text-zinc-200' },
                          { label: 'Small Files', value: maintenance.small_file_count, color: maintenance.small_file_count > 5 ? 'text-amber-400' : 'text-zinc-200' },
                          { label: 'Fragmentation', value: `${(maintenance.fragmentation_score * 100).toFixed(0)}%`, color: maintenance.fragmentation_score > 0.5 ? 'text-red-400' : 'text-emerald-400' },
                        ].map(item => (
                          <Card key={item.label} padding="sm">
                            <div className="text-2xs text-zinc-500 mb-1">{item.label}</div>
                            <div className={cn('text-lg font-bold font-mono', item.color)}>{item.value}</div>
                          </Card>
                        ))}
                      </div>

                      {maintenance.recommendations.length > 0 && (
                        <Card>
                          <h4 className="text-xs font-semibold text-zinc-300 mb-2 flex items-center gap-2">
                            <AlertTriangle className="w-3.5 h-3.5 text-amber-400" /> Recommendations
                          </h4>
                          <ul className="space-y-1">
                            {maintenance.recommendations.map((rec, i) => (
                              <li key={i} className="text-xs text-zinc-400 flex items-start gap-2">
                                <span className="text-amber-400 mt-0.5">•</span> {rec}
                              </li>
                            ))}
                          </ul>
                        </Card>
                      )}

                      <Card>
                        <h4 className="text-xs font-semibold text-zinc-300 mb-3">Maintenance Actions</h4>
                        <div className="flex flex-wrap gap-2">
                          <Button
                            variant="secondary"
                            size="sm"
                            icon={<Wrench className="w-3.5 h-3.5" />}
                            disabled={maintenanceLoading}
                            onClick={async () => {
                              if (!selected) return
                              setMaintenanceLoading(true)
                              try {
                                const r = await compactTable(selected)
                                toast.success(`Compacted: ${r.input_files} → ${r.output_files} files (${r.rows_rewritten} rows)`)
                                getMaintenanceStatus(selected).then(setMaintenance).catch(() => {})
                              } catch (e) { toast.error(String(e)) }
                              setMaintenanceLoading(false)
                            }}
                          >Compact Files</Button>
                          <Button
                            variant="secondary"
                            size="sm"
                            icon={<Trash2 className="w-3.5 h-3.5" />}
                            disabled={maintenanceLoading}
                            onClick={async () => {
                              if (!selected) return
                              setMaintenanceLoading(true)
                              try {
                                const r = await expireSnapshots(selected)
                                toast.success(`Expired ${r.expired_count} snapshots`)
                                getTableSnapshots(selected).then(setSnapshots).catch(() => {})
                                getMaintenanceStatus(selected).then(setMaintenance).catch(() => {})
                              } catch (e) { toast.error(String(e)) }
                              setMaintenanceLoading(false)
                            }}
                          >Expire Snapshots</Button>
                          <Button
                            variant="secondary"
                            size="sm"
                            icon={<RefreshCw className="w-3.5 h-3.5" />}
                            disabled={maintenanceLoading}
                            onClick={async () => {
                              if (!selected) return
                              setMaintenanceLoading(true)
                              try {
                                const r = await removeOrphans(selected)
                                toast.success(`Removed ${r.orphan_files_deleted} orphan files (${(r.bytes_reclaimed / 1024 / 1024).toFixed(1)} MB reclaimed)`)
                                getMaintenanceStatus(selected).then(setMaintenance).catch(() => {})
                              } catch (e) { toast.error(String(e)) }
                              setMaintenanceLoading(false)
                            }}
                          >Remove Orphans</Button>
                        </div>
                      </Card>
                    </>
                  ) : (
                    <EmptyState
                      icon={<Wrench className="w-5 h-5" />}
                      title="No maintenance data"
                      description="Maintenance status is available for Iceberg tables with S3 data files."
                    />
                  )}
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
