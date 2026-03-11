import { useState, useEffect, useCallback } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Button } from '../components/ui/Button'
import { cn } from '../lib/utils'
import {
  ArrowLeftRight, Search, Play, Loader2, CheckCircle2, AlertCircle,
  Database, Zap, ArrowRight, Server, HardDrive, Key,
  ChevronDown, ChevronRight, Layers, Shield, Clock, Cpu,
  RefreshCw, Copy, Globe, Info, Check, X, CloudCog,
} from 'lucide-react'
import {
  getConnections, migrationDiscover, migrationRegister, migrationCompare,
  migrationCredentials, getMigrationTables, getMigrationComparisons, getMigrationReadonly,
} from '../api/client'
import type { ConnectionEntry } from '../types'
import type { MigrationTable, EngineResultM, MigrationComparison, EngineRecommendation } from '../api/client'
import toast from 'react-hot-toast'

const FORMAT_COLORS: Record<string, string> = {
  iceberg: 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20',
  hive: 'bg-amber-400/10 text-amber-400 border-amber-400/20',
  delta: 'bg-violet-400/10 text-violet-400 border-violet-400/20',
  tpch: 'bg-violet-400/10 text-violet-400 border-violet-400/20',
  jdbc: 'bg-blue-400/10 text-blue-400 border-blue-400/20',
  unknown: 'bg-zinc-400/10 text-zinc-400 border-zinc-400/20',
}

const ENGINE_COLORS: Record<string, string> = {
  'Trino': 'bg-red-400/10 text-red-400 border-red-400/20',
  'DataFusion (direct)': 'bg-amber-400/10 text-amber-400 border-amber-400/20',
  'DuckDB': 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  'Polars': 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20',
}

const STATUS_COLORS: Record<string, string> = {
  discovered: 'bg-zinc-400/10 text-zinc-400 border-zinc-400/20',
  registered: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  ready: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  blocked: 'bg-red-400/10 text-red-400 border-red-400/20',
  error: 'bg-red-400/10 text-red-400 border-red-400/20',
  pending: 'bg-amber-400/10 text-amber-400 border-amber-400/20',
}

const PATH_COLORS: Record<string, string> = {
  via_trino: 'bg-rose-400/10 text-rose-400 border-rose-400/20',
  s3_direct: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  in_memory: 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20',
}

const PATH_LABELS: Record<string, string> = {
  via_trino: 'via Trino',
  s3_direct: 'S3 direct',
  in_memory: 'in-memory',
}

type Tab = 'discover' | 's3' | 'compare' | 'plan'

interface S3Bucket {
  bucket: string
  region: string
  status: 'connected' | 'testing' | 'error' | 'idle'
  error?: string
}

export function Migration() {
  const [tab, setTab] = useState<Tab>('discover')
  const [connections, setConnections] = useState<ConnectionEntry[]>([])
  const [selectedConn, setSelectedConn] = useState('')
  const [tables, setTables] = useState<MigrationTable[]>([])
  const [catalogs, setCatalogs] = useState<string[]>([])
  const [comparisons, setComparisons] = useState<MigrationComparison[]>([])
  const [discovering, setDiscovering] = useState(false)
  const [registering, setRegistering] = useState(false)
  const [comparing, setComparing] = useState(false)
  const [compareSql, setCompareSql] = useState('')
  const [lastComparison, setLastComparison] = useState<MigrationComparison | null>(null)
  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(new Set())
  const [selectedForRegister, setSelectedForRegister] = useState<Set<string>>(new Set())

  // Read-only migration tables
  const [readOnlyTables, setReadOnlyTables] = useState<Set<string>>(new Set())

  // Native S3 toggle
  const [useNativeS3, setUseNativeS3] = useState(false)

  // S3 credentials state
  const [s3Bucket, setS3Bucket] = useState('')
  const [s3AccessKey, setS3AccessKey] = useState('')
  const [s3SecretKey, setS3SecretKey] = useState('')
  const [s3Region, setS3Region] = useState('us-east-1')
  const [s3Testing, setS3Testing] = useState(false)
  const [s3Buckets, setS3Buckets] = useState<S3Bucket[]>([])

  const loadConnections = useCallback(async () => {
    try {
      const r = await getConnections()
      const trino = (r.connections || []).filter((c: ConnectionEntry) => c.conn_type === 'trino')
      setConnections(trino)
      if (trino.length === 1) setSelectedConn(trino[0].id)
    } catch { /* skip */ }
  }, [])

  const loadTables = useCallback(async () => {
    if (!selectedConn) return
    try {
      const r = await getMigrationTables(selectedConn)
      setTables(r.tables || [])
    } catch { /* skip */ }
  }, [selectedConn])

  const loadComparisons = useCallback(async () => {
    try {
      const r = await getMigrationComparisons()
      setComparisons(r.comparisons || [])
    } catch { /* skip */ }
  }, [])

  const loadReadOnly = useCallback(async () => {
    try {
      const r = await getMigrationReadonly()
      setReadOnlyTables(new Set(r.tables || []))
    } catch { /* skip */ }
  }, [])

  useEffect(() => { loadConnections() }, [loadConnections])
  useEffect(() => { loadTables(); loadComparisons(); loadReadOnly() }, [selectedConn, loadTables, loadComparisons, loadReadOnly])

  const handleDiscover = async () => {
    if (!selectedConn) return
    setDiscovering(true)
    try {
      const r = await migrationDiscover(selectedConn)
      const cats = r.iceberg_catalogs || []
      // Derive all unique catalogs from the tables
      const allCats = [...new Set(r.tables.map(t => t.catalog))]
      toast.success(`Discovered ${r.table_count} tables across ${allCats.length} catalogs`)
      setTables(r.tables)
      setCatalogs(allCats)
      // Auto-expand all catalogs
      setExpandedCatalogs(new Set(allCats))
    } catch (e) {
      toast.error((e as Error).message)
    }
    setDiscovering(false)
  }

  const handleRegister = async (tableKeys?: string[]) => {
    if (!selectedConn) return
    setRegistering(true)
    try {
      const r = await migrationRegister(selectedConn, tableKeys)
      toast.success(`Registered ${r.registered}/${r.total} tables in Rake (read-only)`)
      if (r.errors.length > 0) toast.error(`${r.errors.length} tables failed to register`)
      await loadTables()
      await loadReadOnly()
      setSelectedForRegister(new Set())
    } catch (e) {
      toast.error((e as Error).message)
    }
    setRegistering(false)
  }

  const handleCompare = async () => {
    if (!selectedConn || !compareSql.trim()) return
    setComparing(true)
    try {
      const r = await migrationCompare(selectedConn, compareSql.trim(), undefined, useNativeS3)
      setLastComparison(r)
      toast.success(`Comparison complete — ${r.winner} wins (${r.speedup.toFixed(1)}x faster)`)
      await loadComparisons()
    } catch (e) {
      toast.error((e as Error).message)
    }
    setComparing(false)
  }

  const handleTestS3 = async () => {
    if (!s3Bucket.trim() || !s3AccessKey.trim() || !s3SecretKey.trim()) return
    setS3Testing(true)
    try {
      await migrationCredentials(s3Bucket.trim(), s3AccessKey.trim(), s3SecretKey.trim())
      setS3Buckets(prev => {
        const existing = prev.filter(b => b.bucket !== s3Bucket.trim())
        return [...existing, { bucket: s3Bucket.trim(), region: s3Region, status: 'connected' as const }]
      })
      toast.success(`Connected to s3://${s3Bucket.trim()}`)
      setS3AccessKey('')
      setS3SecretKey('')
    } catch (e) {
      setS3Buckets(prev => {
        const existing = prev.filter(b => b.bucket !== s3Bucket.trim())
        return [...existing, { bucket: s3Bucket.trim(), region: s3Region, status: 'error' as const, error: (e as Error).message }]
      })
      toast.error((e as Error).message)
    }
    setS3Testing(false)
  }

  const toggleTableSelection = (key: string) => {
    setSelectedForRegister(prev => {
      const next = new Set(prev)
      if (next.has(key)) next.delete(key); else next.add(key)
      return next
    })
  }

  // Group tables by catalog
  const catalogGroups = tables.reduce<Record<string, MigrationTable[]>>((acc, t) => {
    const key = t.catalog
    if (!acc[key]) acc[key] = []
    acc[key].push(t)
    return acc
  }, {})

  const icebergCount = tables.filter(t => t.format === 'iceberg').length
  const registeredCount = tables.filter(t => t.registered_in_rake).length
  const s3Locations = new Set(tables.filter(t => t.location).map(t => {
    const loc = t.location || ''
    const match = loc.match(/^s3[a]?:\/\/([^/]+)/)
    return match ? match[1] : null
  }).filter(Boolean))

  const tabItems: { key: Tab; label: string }[] = [
    { key: 'discover', label: 'Discover' },
    { key: 's3', label: 'Connect S3' },
    { key: 'compare', label: 'Compare' },
    { key: 'plan', label: 'Migration Plan' },
  ]

  return (
    <div className="p-6 space-y-6 max-w-[1400px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-zinc-100 flex items-center gap-2">
            <ArrowLeftRight className="w-5 h-5 text-rose-400" />
            Iceberg Migration
          </h1>
          <p className="text-sm text-zinc-500 mt-1">
            Migrate Iceberg tables from Trino to Rake — direct S3 access, no coordinator overhead
          </p>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={selectedConn}
            onChange={e => setSelectedConn(e.target.value)}
            className="bg-zinc-900 border border-zinc-700/50 rounded-lg px-3 py-2 text-sm text-zinc-300 focus:outline-none focus:border-rose-400/50"
          >
            <option value="">Select Trino connection</option>
            {connections.map(c => (
              <option key={c.id} value={c.id}>{c.name} ({c.host}:{c.port})</option>
            ))}
          </select>
        </div>
      </div>

      {/* Architecture diagram */}
      <Card className="bg-zinc-900/50 border-zinc-800/50 p-5">
        <div className="flex items-center justify-between gap-6">
          <div className="flex items-center gap-4 flex-1">
            {/* Trino + Hive */}
            <div className="flex flex-col items-center gap-1.5">
              <div className="flex items-center gap-2">
                <div className="w-11 h-11 rounded-lg bg-red-400/10 border border-red-400/20 flex items-center justify-center">
                  <Server className="w-5 h-5 text-red-400" />
                </div>
                <div className="text-left">
                  <span className="text-xs text-red-400 font-semibold block">Trino</span>
                  <span className="text-2xs text-zinc-600">+ Hive Metastore</span>
                </div>
              </div>
            </div>

            <ArrowRight className="w-4 h-4 text-zinc-600 flex-shrink-0" />

            {/* S3 (center) */}
            <div className="flex flex-col items-center gap-1.5">
              <div className="w-14 h-14 rounded-xl bg-cyan-400/10 border-2 border-cyan-400/20 flex items-center justify-center relative">
                <HardDrive className="w-6 h-6 text-cyan-400" />
                <span className="absolute -top-2 -right-2 bg-cyan-400/20 text-cyan-400 text-[9px] font-bold px-1.5 py-0.5 rounded-full border border-cyan-400/30">
                  Iceberg
                </span>
              </div>
              <span className="text-xs text-cyan-400 font-semibold">S3</span>
              <span className="text-2xs text-zinc-600">Parquet + metadata</span>
            </div>

            <ArrowRight className="w-4 h-4 text-zinc-600 flex-shrink-0 rotate-180" />

            {/* Rake */}
            <div className="flex flex-col items-center gap-1.5">
              <div className="flex items-center gap-2">
                <div className="w-11 h-11 rounded-lg bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
                  <Zap className="w-5 h-5 text-amber-400" />
                </div>
                <div className="text-left">
                  <span className="text-xs text-amber-400 font-semibold block">Rake</span>
                  <span className="text-2xs text-zinc-600">Direct S3 access</span>
                </div>
              </div>
            </div>
          </div>

          <div className="text-right text-xs text-zinc-500 max-w-[320px] space-y-1">
            <p className="text-zinc-400 font-medium">Both query the same Iceberg data on S3</p>
            <p>Rake reads Iceberg metadata + Parquet files directly,</p>
            <p>bypassing Trino coordinator for lower latency.</p>
          </div>
        </div>
      </Card>

      {/* Stats bar */}
      {tables.length > 0 && (
        <div className="grid grid-cols-4 gap-3">
          {[
            { label: 'Total Tables', value: tables.length, icon: Search, color: 'text-zinc-400' },
            { label: 'Iceberg', value: icebergCount, icon: Database, color: 'text-cyan-400' },
            { label: 'S3 Buckets', value: s3Locations.size, icon: HardDrive, color: 'text-violet-400' },
            { label: 'Registered', value: registeredCount, icon: CheckCircle2, color: 'text-emerald-400' },
          ].map(s => (
            <Card key={s.label} className="bg-zinc-900/30 border-zinc-800/50 p-3 flex items-center gap-3">
              <s.icon className={cn('w-4 h-4', s.color)} />
              <div>
                <p className="text-lg font-bold text-zinc-100">{s.value}</p>
                <p className="text-2xs text-zinc-500 uppercase tracking-wider">{s.label}</p>
              </div>
            </Card>
          ))}
        </div>
      )}

      {/* Tabs */}
      <div className="flex gap-1 border-b border-zinc-800/50">
        {tabItems.map(t => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={cn(
              'px-4 py-2 text-sm font-medium border-b-2 transition-colors',
              tab === t.key
                ? 'text-rose-400 border-rose-400'
                : 'text-zinc-500 border-transparent hover:text-zinc-300'
            )}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* ============ DISCOVER TAB ============ */}
      {tab === 'discover' && (
        <div className="space-y-4">
          <div className="flex items-center gap-3">
            <Button
              onClick={handleDiscover}
              disabled={!selectedConn || discovering}
              className="bg-rose-500/20 hover:bg-rose-500/30 text-rose-400 border-rose-500/30"
            >
              {discovering ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Search className="w-4 h-4 mr-2" />}
              Scan Catalogs
            </Button>
            {tables.length > 0 && (
              <span className="text-xs text-zinc-500 ml-auto">
                {catalogs.length} catalog{catalogs.length !== 1 ? 's' : ''} found with {icebergCount} Iceberg table{icebergCount !== 1 ? 's' : ''}
              </span>
            )}
          </div>

          {/* Catalog tree */}
          {Object.entries(catalogGroups).map(([catalog, catTables]) => {
            const isExpanded = expandedCatalogs.has(catalog)
            const icebergInCat = catTables.filter(t => t.format === 'iceberg').length
            const registeredInCat = catTables.filter(t => t.registered_in_rake).length
            const catType = catTables[0]?.format || 'unknown'
            const metastoreUri = catTables.find(t => t.metastore_uri)?.metastore_uri
            return (
              <Card key={catalog} className="bg-zinc-900/30 border-zinc-800/50 overflow-hidden" padding="none">
                <button
                  onClick={() => setExpandedCatalogs(prev => {
                    const next = new Set(prev)
                    if (next.has(catalog)) next.delete(catalog); else next.add(catalog)
                    return next
                  })}
                  className="w-full flex items-center gap-3 p-3 hover:bg-white/[0.02] transition-colors"
                >
                  {isExpanded ? <ChevronDown className="w-4 h-4 text-zinc-500" /> : <ChevronRight className="w-4 h-4 text-zinc-500" />}
                  <Database className="w-4 h-4 text-rose-400" />
                  <span className="text-sm font-semibold text-zinc-200">{catalog}</span>
                  <Badge className={FORMAT_COLORS[catType] || FORMAT_COLORS.unknown}>{catType}</Badge>
                  {metastoreUri && (
                    <span className="text-2xs text-zinc-600 font-mono truncate max-w-[200px]">{metastoreUri}</span>
                  )}
                  <span className="text-xs text-zinc-500 ml-auto mr-2">{catTables.length} tables</span>
                  {registeredInCat > 0 && (
                    <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">
                      {registeredInCat} registered
                    </Badge>
                  )}
                  {icebergInCat > 0 && (
                    <Badge className="bg-cyan-400/10 text-cyan-400 border-cyan-400/20">
                      {icebergInCat} iceberg
                    </Badge>
                  )}
                </button>
                {isExpanded && (
                  <div className="border-t border-zinc-800/30">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b border-zinc-800/30 text-zinc-500">
                          <th className="text-left px-4 py-2 font-medium w-6"></th>
                          <th className="text-left px-2 py-2 font-medium">Table</th>
                          <th className="text-left px-3 py-2 font-medium">Format</th>
                          <th className="text-right px-3 py-2 font-medium">Cols</th>
                          <th className="text-right px-3 py-2 font-medium">Rows</th>
                          <th className="text-left px-3 py-2 font-medium">S3 Location</th>
                          <th className="text-left px-3 py-2 font-medium">Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {catTables.map(t => {
                          const key = `${t.catalog}.${t.schema_name}.${t.table_name}`
                          return (
                            <tr key={key} className="border-b border-zinc-800/20 hover:bg-white/[0.01]">
                              <td className="px-4 py-2">
                                <input
                                  type="checkbox"
                                  checked={selectedForRegister.has(key)}
                                  onChange={() => toggleTableSelection(key)}
                                  className="w-3.5 h-3.5 rounded border-zinc-600 bg-zinc-800 text-rose-400 focus:ring-rose-400/20"
                                />
                              </td>
                              <td className="px-2 py-2">
                                <span className="font-mono text-zinc-300">{t.schema_name}.<span className="text-zinc-100">{t.table_name}</span></span>
                              </td>
                              <td className="px-3 py-2">
                                <Badge className={FORMAT_COLORS[t.format] || FORMAT_COLORS.unknown}>{t.format}</Badge>
                              </td>
                              <td className="px-3 py-2 text-right text-zinc-400">{t.column_count}</td>
                              <td className="px-3 py-2 text-right text-zinc-400">
                                {t.row_count != null ? t.row_count.toLocaleString() : '\u2014'}
                              </td>
                              <td className="px-3 py-2 text-zinc-500 font-mono truncate max-w-[220px]" title={t.location || undefined}>
                                {t.location || <span className="italic text-zinc-700">no S3 path</span>}
                              </td>
                              <td className="px-3 py-2">
                                {t.registered_in_rake ? (
                                  <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">
                                    <CheckCircle2 className="w-3 h-3 mr-1" />
                                    registered
                                  </Badge>
                                ) : (
                                  <Badge className={STATUS_COLORS[t.status] || STATUS_COLORS.discovered}>
                                    {t.status}
                                  </Badge>
                                )}
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </Card>
            )
          })}

          {tables.length === 0 && selectedConn && !discovering && (
            <Card className="bg-zinc-900/30 border-zinc-800/50 p-8 text-center">
              <Search className="w-8 h-8 text-zinc-700 mx-auto mb-3" />
              <p className="text-sm text-zinc-500">Click &quot;Scan Catalogs&quot; to discover Iceberg tables in your Trino connection</p>
            </Card>
          )}

          {!selectedConn && (
            <Card className="bg-zinc-900/30 border-zinc-800/50 p-8 text-center">
              <Server className="w-8 h-8 text-zinc-700 mx-auto mb-3" />
              <p className="text-sm text-zinc-500">Select a Trino connection to begin catalog discovery</p>
            </Card>
          )}
        </div>
      )}

      {/* ============ CONNECT S3 TAB ============ */}
      {tab === 's3' && (
        <div className="space-y-4">
          {/* Info callout */}
          <Card className="bg-cyan-400/[0.03] border-cyan-400/10 p-4">
            <div className="flex items-start gap-3">
              <Info className="w-5 h-5 text-cyan-400 flex-shrink-0 mt-0.5" />
              <div>
                <p className="text-sm text-cyan-300 font-medium">Direct S3 Access</p>
                <p className="text-xs text-zinc-400 mt-1">
                  Rake reads Iceberg metadata and Parquet data files directly from S3, bypassing Trino entirely.
                  This eliminates the Trino coordinator as a bottleneck and enables Arrow-native execution with zero serialization overhead.
                </p>
              </div>
            </div>
          </Card>

          <div className="grid grid-cols-2 gap-4">
            {/* Credential form */}
            <Card className="bg-zinc-900/30 border-zinc-800/50 p-5 space-y-4">
              <div className="flex items-center gap-2 mb-1">
                <Key className="w-4 h-4 text-rose-400" />
                <span className="text-sm font-semibold text-zinc-300">AWS Credentials</span>
              </div>
              <div className="space-y-3">
                <div>
                  <label className="text-2xs text-zinc-500 uppercase tracking-wider mb-1 block">S3 Bucket</label>
                  <input
                    type="text"
                    value={s3Bucket}
                    onChange={e => setS3Bucket(e.target.value)}
                    placeholder="my-data-lake-bucket"
                    className="w-full bg-zinc-950 border border-zinc-800/50 rounded-lg px-3 py-2 text-sm font-mono text-zinc-300 placeholder:text-zinc-700 focus:outline-none focus:border-rose-400/50"
                  />
                </div>
                <div>
                  <label className="text-2xs text-zinc-500 uppercase tracking-wider mb-1 block">AWS Region</label>
                  <select
                    value={s3Region}
                    onChange={e => setS3Region(e.target.value)}
                    className="w-full bg-zinc-950 border border-zinc-800/50 rounded-lg px-3 py-2 text-sm text-zinc-300 focus:outline-none focus:border-rose-400/50"
                  >
                    {['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2', 'eu-west-1', 'eu-west-2', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1'].map(r => (
                      <option key={r} value={r}>{r}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="text-2xs text-zinc-500 uppercase tracking-wider mb-1 block">Access Key ID</label>
                  <input
                    type="text"
                    value={s3AccessKey}
                    onChange={e => setS3AccessKey(e.target.value)}
                    placeholder="AKIA..."
                    className="w-full bg-zinc-950 border border-zinc-800/50 rounded-lg px-3 py-2 text-sm font-mono text-zinc-300 placeholder:text-zinc-700 focus:outline-none focus:border-rose-400/50"
                  />
                </div>
                <div>
                  <label className="text-2xs text-zinc-500 uppercase tracking-wider mb-1 block">Secret Access Key</label>
                  <input
                    type="password"
                    value={s3SecretKey}
                    onChange={e => setS3SecretKey(e.target.value)}
                    placeholder="••••••••••••"
                    className="w-full bg-zinc-950 border border-zinc-800/50 rounded-lg px-3 py-2 text-sm font-mono text-zinc-300 placeholder:text-zinc-700 focus:outline-none focus:border-rose-400/50"
                  />
                </div>
                <Button
                  onClick={handleTestS3}
                  disabled={!s3Bucket.trim() || !s3AccessKey.trim() || !s3SecretKey.trim() || s3Testing}
                  className="w-full bg-rose-500/20 hover:bg-rose-500/30 text-rose-400 border-rose-500/30"
                >
                  {s3Testing ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Shield className="w-4 h-4 mr-2" />}
                  Test Connection
                </Button>
              </div>
            </Card>

            {/* Connected buckets list */}
            <Card className="bg-zinc-900/30 border-zinc-800/50 p-5 space-y-4">
              <div className="flex items-center gap-2 mb-1">
                <CloudCog className="w-4 h-4 text-cyan-400" />
                <span className="text-sm font-semibold text-zinc-300">Configured Buckets</span>
              </div>
              {s3Buckets.length > 0 ? (
                <div className="space-y-2">
                  {s3Buckets.map(b => (
                    <div key={b.bucket} className={cn(
                      'flex items-center gap-3 p-3 rounded-lg border',
                      b.status === 'connected' ? 'bg-emerald-400/[0.03] border-emerald-400/10' : 'bg-red-400/[0.03] border-red-400/10'
                    )}>
                      <HardDrive className={cn('w-4 h-4', b.status === 'connected' ? 'text-emerald-400' : 'text-red-400')} />
                      <div className="flex-1">
                        <p className="text-sm font-mono text-zinc-200">s3://{b.bucket}</p>
                        <p className="text-2xs text-zinc-500">{b.region}</p>
                      </div>
                      {b.status === 'connected' ? (
                        <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">
                          <Check className="w-3 h-3 mr-1" />connected
                        </Badge>
                      ) : (
                        <Badge className="bg-red-400/10 text-red-400 border-red-400/20">
                          <X className="w-3 h-3 mr-1" />error
                        </Badge>
                      )}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center py-8 text-center">
                  <HardDrive className="w-8 h-8 text-zinc-700 mb-3" />
                  <p className="text-sm text-zinc-500">No S3 buckets configured yet</p>
                  <p className="text-2xs text-zinc-600 mt-1">Enter credentials to connect to your Iceberg data lake</p>
                </div>
              )}

              {/* Discovered S3 locations from tables */}
              {s3Locations.size > 0 && (
                <div className="border-t border-zinc-800/30 pt-3">
                  <p className="text-2xs text-zinc-500 uppercase tracking-wider mb-2">Discovered from Trino catalogs</p>
                  <div className="space-y-1">
                    {Array.from(s3Locations).map(loc => (
                      <div key={loc} className="flex items-center gap-2 text-xs">
                        <Globe className="w-3 h-3 text-zinc-600" />
                        <span className="font-mono text-zinc-400">s3://{loc}</span>
                        {s3Buckets.some(b => b.bucket === loc && b.status === 'connected') ? (
                          <Check className="w-3 h-3 text-emerald-400 ml-auto" />
                        ) : (
                          <button
                            onClick={() => setS3Bucket(loc || '')}
                            className="text-rose-400 hover:text-rose-300 ml-auto text-2xs"
                          >
                            configure
                          </button>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </Card>
          </div>
        </div>
      )}

      {/* ============ COMPARE TAB ============ */}
      {tab === 'compare' && (
        <div className="space-y-4">
          <Card className="bg-zinc-900/30 border-zinc-800/50 p-4 space-y-3">
            <div className="flex items-center gap-2">
              <Play className="w-4 h-4 text-rose-400" />
              <span className="text-sm font-semibold text-zinc-300">Performance Comparison</span>
              <span className="text-2xs text-zinc-600 ml-auto">
                Run the same query via Trino and directly on Rake engines
              </span>
            </div>
            <textarea
              value={compareSql}
              onChange={e => setCompareSql(e.target.value)}
              placeholder="SELECT region, COUNT(*) as cnt, SUM(total_price) as revenue&#10;FROM iceberg_catalog.tpch.orders&#10;GROUP BY region ORDER BY revenue DESC"
              className="w-full h-28 bg-zinc-950 border border-zinc-800/50 rounded-lg p-3 text-sm font-mono text-zinc-300 placeholder:text-zinc-700 focus:outline-none focus:border-rose-400/50 resize-none"
            />
            <div className="flex items-center gap-3">
              <Button
                onClick={handleCompare}
                disabled={!selectedConn || !compareSql.trim() || comparing}
                className="bg-rose-500/20 hover:bg-rose-500/30 text-rose-400 border-rose-500/30"
              >
                {comparing ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Play className="w-4 h-4 mr-2" />}
                Run Comparison
              </Button>
              <label className="flex items-center gap-2 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={useNativeS3}
                  onChange={e => setUseNativeS3(e.target.checked)}
                  className="w-3.5 h-3.5 rounded border-zinc-600 bg-zinc-800 text-rose-400 focus:ring-rose-400/20"
                />
                <span className="text-xs text-zinc-300">Use native S3 connections</span>
              </label>
              {useNativeS3 && (
                <span className="text-2xs text-zinc-600 flex items-center gap-1">
                  <Info className="w-3 h-3" />
                  Each engine reads directly from S3 using native connectors
                </span>
              )}
              {comparisons.length > 0 && (
                <span className="text-xs text-zinc-500 ml-auto">{comparisons.length} previous comparison{comparisons.length !== 1 ? 's' : ''}</span>
              )}
            </div>
          </Card>

          {/* Engine Connection Diagram */}
          <Card className="bg-zinc-900/30 border-zinc-800/50 p-4">
            <p className="text-xs text-zinc-500 font-medium uppercase tracking-wider mb-3">Engine Connection Paths</p>
            <div className="grid grid-cols-2 gap-4">
              {/* S3 paths */}
              <div className="space-y-2">
                <div className="flex items-center gap-2 mb-2">
                  <HardDrive className="w-4 h-4 text-cyan-400" />
                  <span className="text-xs font-semibold text-cyan-400">S3 Bucket</span>
                  <span className="text-2xs text-zinc-600">Iceberg / Parquet</span>
                </div>
                {[
                  { engine: 'DataFusion', connector: 'object_store', color: 'text-amber-400', borderColor: 'border-amber-400/20' },
                  { engine: 'DuckDB', connector: 'httpfs native', color: 'text-emerald-400', borderColor: 'border-emerald-400/20' },
                  { engine: 'Polars', connector: 'cloud native', color: 'text-cyan-400', borderColor: 'border-cyan-400/20' },
                ].map(e => (
                  <div key={e.engine} className="flex items-center gap-2 pl-6">
                    <div className={cn('w-px h-4 border-l border-dashed', e.borderColor)} />
                    <ArrowRight className={cn('w-3 h-3', e.color)} />
                    <span className={cn('text-xs font-medium', e.color)}>{e.engine}</span>
                    <span className="text-2xs text-zinc-600 font-mono">({e.connector})</span>
                  </div>
                ))}
              </div>
              {/* Trino path */}
              <div className="space-y-2">
                <div className="flex items-center gap-2 mb-2">
                  <Server className="w-4 h-4 text-red-400" />
                  <span className="text-xs font-semibold text-red-400">Trino</span>
                  <span className="text-2xs text-zinc-600">baseline</span>
                </div>
                <div className="flex items-center gap-2 pl-6">
                  <div className="w-px h-4 border-l border-dashed border-red-400/20" />
                  <ArrowRight className="w-3 h-3 text-red-400" />
                  <span className="text-xs font-medium text-red-400">REST API</span>
                  <span className="text-2xs text-zinc-600 font-mono">(coordinator overhead)</span>
                </div>
                <div className="mt-3 p-2 rounded bg-zinc-800/30 border border-zinc-700/30">
                  <p className="text-2xs text-zinc-500">
                    {useNativeS3
                      ? 'Native S3 mode: all engines connect to S3 directly using their own connectors.'
                      : 'Default mode: DuckDB/Polars use in-memory synced copies, DataFusion goes through Trino.'}
                  </p>
                </div>
              </div>
            </div>
          </Card>

          {/* Comparison Results */}
          {lastComparison && (
            <div className="space-y-4">
              {/* Winner banner */}
              <Card className="bg-gradient-to-r from-emerald-400/5 to-cyan-400/5 border-emerald-400/20 p-4">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <Zap className="w-6 h-6 text-emerald-400" />
                    <div>
                      <p className="text-lg font-bold text-zinc-100">
                        {lastComparison.winner} is{' '}
                        <span className="text-emerald-400">{lastComparison.speedup.toFixed(1)}x faster</span>
                        {' '}than Trino
                      </p>
                      <p className="text-xs text-zinc-500">
                        Data match: {lastComparison.data_match
                          ? <span className="text-emerald-400">verified (all engines return same row count)</span>
                          : <span className="text-amber-400">mismatch detected</span>
                        }
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="text-2xs text-zinc-500">Query</p>
                    <p className="text-xs font-mono text-zinc-400 max-w-[300px] truncate">{lastComparison.sql || compareSql}</p>
                  </div>
                </div>
              </Card>

              {/* Engine cards */}
              <div className="grid grid-cols-4 gap-3">
                {lastComparison.results.map(r => {
                  const isWinner = r.engine === lastComparison.winner
                  const isTrino = r.engine === 'Trino'
                  const trinoMs = lastComparison.results.find(x => x.engine === 'Trino')?.duration_ms || 1
                  const speedup = isTrino ? 1 : trinoMs / Math.max(r.duration_ms, 1)
                  return (
                    <Card
                      key={r.engine}
                      className={cn(
                        'p-4 border transition-all',
                        isWinner ? 'border-emerald-400/30 bg-emerald-400/[0.03] ring-1 ring-emerald-400/10' : 'bg-zinc-900/30 border-zinc-800/50',
                        r.status === 'error' && 'opacity-50'
                      )}
                      padding="none"
                    >
                      <div className="p-4">
                        <div className="flex items-center gap-2 mb-3">
                          <Badge className={ENGINE_COLORS[r.engine] || 'bg-zinc-400/10 text-zinc-400 border-zinc-400/20'}>
                            {r.engine}
                          </Badge>
                          {isWinner && <Zap className="w-3 h-3 text-emerald-400" />}
                        </div>
                        {r.status === 'success' ? (
                          <>
                            <p className={cn('text-2xl font-bold', isWinner ? 'text-emerald-400' : 'text-zinc-200')}>
                              {r.duration_ms}ms
                            </p>
                            <div className="mt-1.5">
                              <Badge className={PATH_COLORS[r.path || (isTrino ? 'via_trino' : 'in_memory')] || PATH_COLORS.in_memory}>
                                {PATH_LABELS[r.path || (isTrino ? 'via_trino' : 'in_memory')] || r.path || 'in-memory'}
                              </Badge>
                            </div>
                            <p className="text-xs text-zinc-500 mt-1">{r.row_count} rows</p>
                            {!isTrino && (
                              <p className={cn('text-xs mt-1', speedup > 5 ? 'text-emerald-400' : speedup > 2 ? 'text-amber-400' : 'text-zinc-500')}>
                                {speedup.toFixed(1)}x vs Trino
                              </p>
                            )}
                            {isTrino && <p className="text-xs text-zinc-600 mt-1">baseline</p>}
                          </>
                        ) : r.status === 'error' ? (
                          <div>
                            <p className="text-sm text-red-400">Error</p>
                            <p className="text-2xs text-zinc-600 mt-1 truncate">{r.error}</p>
                          </div>
                        ) : (
                          <p className="text-sm text-zinc-600">Unavailable</p>
                        )}
                      </div>
                    </Card>
                  )
                })}
              </div>

              {/* Visual latency bars */}
              <Card className="bg-zinc-900/30 border-zinc-800/50 p-4">
                <p className="text-xs text-zinc-500 mb-3 font-medium uppercase tracking-wider">Latency Comparison</p>
                {lastComparison.results
                  .filter(r => r.status === 'success')
                  .sort((a, b) => a.duration_ms - b.duration_ms)
                  .map(r => {
                    const maxMs = Math.max(...lastComparison.results.filter(x => x.status === 'success').map(x => x.duration_ms))
                    const pct = maxMs > 0 ? (r.duration_ms / maxMs) * 100 : 0
                    const isTrino = r.engine === 'Trino'
                    return (
                      <div key={r.engine} className="flex items-center gap-3 mb-2">
                        <span className="text-xs text-zinc-400 w-36 text-right">{r.engine}</span>
                        <div className="flex-1 h-6 bg-zinc-800/30 rounded overflow-hidden relative">
                          <div
                            className={cn(
                              'h-full rounded transition-all duration-500',
                              isTrino ? 'bg-red-400/40' : r.engine === lastComparison.winner ? 'bg-emerald-400/40' : 'bg-amber-400/30'
                            )}
                            style={{ width: `${Math.max(pct, 2)}%` }}
                          />
                          <span className="absolute inset-y-0 left-2 flex items-center text-2xs text-zinc-300 font-mono">
                            {r.duration_ms}ms
                          </span>
                        </div>
                      </div>
                    )
                  })}
              </Card>

              {/* Why Rake is Faster callout */}
              <Card className="bg-amber-400/[0.03] border-amber-400/10 p-4">
                <div className="flex items-start gap-3">
                  <Zap className="w-5 h-5 text-amber-400 flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm text-amber-300 font-medium">Why Rake is Faster</p>
                    <div className="grid grid-cols-2 gap-x-6 gap-y-1 mt-2 text-xs text-zinc-400">
                      <p><span className="text-amber-400/80">No coordinator overhead</span> — queries go straight to S3, no Trino worker scheduling</p>
                      <p><span className="text-amber-400/80">Arrow-native execution</span> — zero serialization between query stages</p>
                      <p><span className="text-amber-400/80">No JVM GC pauses</span> — Rust has no garbage collector, predictable latency</p>
                      <p><span className="text-amber-400/80">Direct S3 reads</span> — Iceberg metadata parsed natively, Parquet read in parallel</p>
                    </div>
                  </div>
                </div>
              </Card>

              {/* Rake Recommendation */}
              {(() => {
                const rec: EngineRecommendation = lastComparison.recommendation || {
                  strategy: 'single_engine',
                  primary_engine: lastComparison.winner,
                  reason: `${lastComparison.winner} is ${lastComparison.speedup.toFixed(0)}x faster than Trino for scan+aggregate queries. In-memory path provides sub-ms response.`,
                  estimated_speedup: lastComparison.speedup,
                }
                const strategies: Array<{
                  key: string
                  label: string
                  accent: string
                  border: string
                  glow: string
                  bg: string
                  description: string
                  when: string
                  flow: Array<{ label: string; color: string }>
                }> = [
                  {
                    key: 'single_engine',
                    label: 'Single Engine',
                    accent: 'text-emerald-400',
                    border: 'border-emerald-400/30',
                    glow: 'ring-1 ring-emerald-400/20 shadow-[0_0_15px_rgba(52,211,153,0.08)]',
                    bg: 'bg-emerald-400/[0.04]',
                    description: 'One engine handles scan + processing. Best for simple OLAP queries.',
                    when: rec.alternatives?.find(a => a.strategy === 'single_engine')?.when || 'simple OLAP scans',
                    flow: [
                      { label: 'S3', color: 'bg-zinc-700 text-zinc-300' },
                      { label: rec.primary_engine, color: 'bg-emerald-400/20 text-emerald-400' },
                      { label: 'Result', color: 'bg-zinc-700 text-zinc-300' },
                    ],
                  },
                  {
                    key: 'scan_handoff',
                    label: 'Scan + Handoff',
                    accent: 'text-amber-400',
                    border: 'border-amber-400/30',
                    glow: 'ring-1 ring-amber-400/20 shadow-[0_0_15px_rgba(251,191,36,0.08)]',
                    bg: 'bg-amber-400/[0.04]',
                    description: 'Fastest scanner reads data, hands Arrow batches to best executor. Best for multi-table joins.',
                    when: rec.alternatives?.find(a => a.strategy === 'scan_handoff')?.when || 'multi-table joins',
                    flow: [
                      { label: 'S3', color: 'bg-zinc-700 text-zinc-300' },
                      { label: rec.scan_engine || 'DuckDB', color: 'bg-amber-400/20 text-amber-400' },
                      { label: 'Arrow', color: 'bg-rose-400/20 text-rose-400' },
                      { label: rec.process_engine || 'DataFusion', color: 'bg-amber-400/20 text-amber-400' },
                      { label: 'Result', color: 'bg-zinc-700 text-zinc-300' },
                    ],
                  },
                  {
                    key: 'parallel_fanout',
                    label: 'Parallel Fan-Out',
                    accent: 'text-violet-400',
                    border: 'border-violet-400/30',
                    glow: 'ring-1 ring-violet-400/20 shadow-[0_0_15px_rgba(167,139,250,0.08)]',
                    bg: 'bg-violet-400/[0.04]',
                    description: 'Split partitions across engines, merge results. Best for very large datasets.',
                    when: rec.alternatives?.find(a => a.strategy === 'parallel_fanout')?.when || '>1GB scans',
                    flow: [],
                  },
                ]
                const activeStrategy = strategies.find(s => s.key === rec.strategy) || strategies[0]
                const confidenceLevel = comparisons.length >= 5 ? 'high' : comparisons.length >= 2 ? 'moderate' : 'low'
                const confidenceColor = confidenceLevel === 'high' ? 'text-emerald-400' : confidenceLevel === 'moderate' ? 'text-amber-400' : 'text-zinc-500'

                // Classify query type from SQL
                const sqlLower = (lastComparison.sql || '').toLowerCase()
                const queryType = sqlLower.includes('join') ? 'join' :
                  sqlLower.includes('group by') || sqlLower.includes('count(') || sqlLower.includes('sum(') ? 'scan_aggregate' :
                  sqlLower.includes('order by') ? 'sort' :
                  sqlLower.includes('where') ? 'filter_scan' : 'full_scan'
                const queryTypeLabel = queryType.replace(/_/g, ' ')

                return (
                  <Card className="bg-rose-400/[0.02] border-rose-400/15 p-5">
                    <div className="flex items-center gap-2.5 mb-4">
                      <Zap className="w-5 h-5 text-rose-400" />
                      <p className="text-sm font-semibold text-zinc-100">Rake Recommendation</p>
                    </div>

                    {/* Strategy + Engine + Reason */}
                    <div className="mb-4">
                      <div className="flex items-center gap-2 mb-2">
                        <span className="text-2xs text-zinc-500 uppercase tracking-wider">Strategy:</span>
                        <Badge className={cn(
                          'border',
                          activeStrategy.bg,
                          activeStrategy.border,
                          activeStrategy.accent,
                        )}>
                          {activeStrategy.label}
                        </Badge>
                      </div>
                      <div className="flex items-center gap-2 mb-3">
                        <span className="text-2xs text-zinc-500 uppercase tracking-wider">Use:</span>
                        <span className="text-sm font-medium text-zinc-200">{rec.primary_engine}</span>
                      </div>
                      <p className="text-xs text-zinc-400 italic leading-relaxed max-w-2xl">
                        &ldquo;{rec.reason}&rdquo;
                      </p>
                    </div>

                    {/* 3 Strategy cards */}
                    <div className="grid grid-cols-3 gap-3 mb-4">
                      {strategies.map(s => {
                        const isActive = s.key === rec.strategy
                        return (
                          <div
                            key={s.key}
                            className={cn(
                              'p-3 rounded-lg border transition-all',
                              isActive
                                ? cn(s.border, s.bg, s.glow)
                                : 'border-zinc-800/40 bg-zinc-900/30'
                            )}
                          >
                            <div className="flex items-center gap-1.5 mb-1.5">
                              {isActive && <Check className="w-3 h-3 flex-shrink-0" style={{ color: 'inherit' }} />}
                              <span className={cn('text-xs font-semibold', isActive ? s.accent : 'text-zinc-400')}>
                                {s.label}
                              </span>
                            </div>
                            <p className="text-2xs text-zinc-500 leading-relaxed">{s.description}</p>
                            <p className="text-2xs text-zinc-600 mt-1.5">Best for: <span className={isActive ? s.accent : 'text-zinc-500'}>{s.when}</span></p>
                          </div>
                        )
                      })}
                    </div>

                    {/* Execution Flow Diagram */}
                    <div className="mb-4">
                      <p className="text-2xs text-zinc-500 uppercase tracking-wider mb-2">Execution Flow</p>
                      {rec.strategy === 'parallel_fanout' || activeStrategy.key === 'parallel_fanout' && rec.strategy === 'parallel_fanout' ? (
                        <div className="flex items-center gap-1.5">
                          <div className="flex flex-col gap-1 items-end">
                            <div className="flex items-center gap-1.5">
                              <span className="px-2.5 py-1 rounded text-2xs font-mono bg-zinc-700 text-zinc-300">S3</span>
                              <ArrowRight className="w-3 h-3 text-zinc-600" />
                              <span className="px-2.5 py-1 rounded text-2xs font-mono bg-emerald-400/20 text-emerald-400">DuckDB</span>
                            </div>
                            <div className="flex items-center gap-1.5">
                              <span className="px-2.5 py-1 rounded text-2xs font-mono bg-zinc-700 text-zinc-300">S3</span>
                              <ArrowRight className="w-3 h-3 text-zinc-600" />
                              <span className="px-2.5 py-1 rounded text-2xs font-mono bg-cyan-400/20 text-cyan-400">Polars</span>
                            </div>
                          </div>
                          <div className="flex flex-col items-center justify-center px-1">
                            <div className="w-px h-3 bg-zinc-700" />
                            <ArrowRight className="w-3 h-3 text-zinc-600" />
                            <div className="w-px h-3 bg-zinc-700" />
                          </div>
                          <span className="px-2.5 py-1 rounded text-2xs font-mono bg-amber-400/20 text-amber-400">DataFusion (merge)</span>
                          <ArrowRight className="w-3 h-3 text-zinc-600" />
                          <span className="px-2.5 py-1 rounded text-2xs font-mono bg-zinc-700 text-zinc-300">Result</span>
                        </div>
                      ) : (
                        <div className="flex items-center gap-1.5">
                          {activeStrategy.flow.map((node, i) => (
                            <div key={i} className="flex items-center gap-1.5">
                              {i > 0 && <ArrowRight className="w-3 h-3 text-zinc-600" />}
                              <span className={cn('px-2.5 py-1 rounded text-2xs font-mono', node.color)}>{node.label}</span>
                            </div>
                          ))}
                        </div>
                      )}
                    </div>

                    {/* Footer metadata */}
                    <div className="flex items-center gap-4 pt-3 border-t border-zinc-800/30">
                      <div className="flex items-center gap-1.5">
                        <span className="text-2xs text-zinc-600">Query Type:</span>
                        <Badge className="bg-rose-400/10 text-rose-400 border-rose-400/20">{queryTypeLabel}</Badge>
                      </div>
                      <div className="flex items-center gap-1.5">
                        <span className="text-2xs text-zinc-600">Confidence:</span>
                        <span className={cn('text-2xs font-medium', confidenceColor)}>
                          {confidenceLevel} ({comparisons.length} comparison{comparisons.length !== 1 ? 's' : ''}{comparisons.length < 5 ? `, need ${5 - comparisons.length}+ for adaptive routing` : ''})
                        </span>
                      </div>
                    </div>
                  </Card>
                )
              })()}

              {/* Connection Paths Explained */}
              <Card className="bg-zinc-900/30 border-zinc-800/50 p-4">
                <p className="text-xs text-zinc-500 font-medium uppercase tracking-wider mb-3">Connection Paths</p>
                <div className="grid grid-cols-3 gap-4">
                  <div className="p-3 rounded-lg bg-cyan-400/[0.03] border border-cyan-400/10">
                    <div className="flex items-center gap-2 mb-2">
                      <Badge className={PATH_COLORS.in_memory}>{PATH_LABELS.in_memory}</Badge>
                      <span className="text-2xs text-zinc-600">fastest</span>
                    </div>
                    <p className="text-2xs text-zinc-400">
                      Data pre-loaded into engine memory. Best for repeated queries on the same data.
                    </p>
                  </div>
                  <div className="p-3 rounded-lg bg-emerald-400/[0.03] border border-emerald-400/10">
                    <div className="flex items-center gap-2 mb-2">
                      <Badge className={PATH_COLORS.s3_direct}>{PATH_LABELS.s3_direct}</Badge>
                      <span className="text-2xs text-zinc-600">production</span>
                    </div>
                    <p className="text-2xs text-zinc-400">
                      Each engine reads Parquet from S3 natively. No Trino overhead.
                    </p>
                  </div>
                  <div className="p-3 rounded-lg bg-rose-400/[0.03] border border-rose-400/10">
                    <div className="flex items-center gap-2 mb-2">
                      <Badge className={PATH_COLORS.via_trino}>{PATH_LABELS.via_trino}</Badge>
                      <span className="text-2xs text-zinc-600">baseline</span>
                    </div>
                    <p className="text-2xs text-zinc-400">
                      SQL routed through Trino coordinator. Shows current state before migration.
                    </p>
                  </div>
                </div>
              </Card>
            </div>
          )}

          {/* Previous comparisons */}
          {comparisons.length > 0 && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <p className="text-xs text-zinc-500 font-medium uppercase tracking-wider">Previous Comparisons</p>
                <Button onClick={loadComparisons} size="sm" className="text-xs bg-zinc-800 hover:bg-zinc-700 text-zinc-300 border-zinc-700">
                  <RefreshCw className="w-3 h-3 mr-1" /> Refresh
                </Button>
              </div>
              {comparisons.map(c => (
                <Card key={c.id} className="bg-zinc-900/30 border-zinc-800/50 p-3">
                  <div className="flex items-center justify-between mb-1.5">
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => { setCompareSql(c.sql); setLastComparison(c) }}
                        className="text-xs font-mono text-zinc-400 truncate max-w-[400px] hover:text-zinc-200 transition-colors text-left"
                      >
                        {c.sql}
                      </button>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">
                        {c.winner} — {c.speedup.toFixed(1)}x
                      </Badge>
                      {c.data_match
                        ? <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                        : <AlertCircle className="w-3.5 h-3.5 text-amber-400" />
                      }
                    </div>
                  </div>
                  <div className="flex gap-3">
                    {c.results.filter(r => r.status === 'success').map(r => (
                      <span key={r.engine} className="text-2xs text-zinc-500">
                        <span className={cn(r.engine === c.winner && 'text-emerald-400 font-semibold')}>{r.engine}</span>: {r.duration_ms}ms
                      </span>
                    ))}
                  </div>
                </Card>
              ))}
            </div>
          )}
        </div>
      )}

      {/* ============ MIGRATION PLAN TAB ============ */}
      {tab === 'plan' && (
        <div className="space-y-4">
          {/* Read-only notice */}
          <Card className="bg-amber-400/[0.03] border-amber-400/15 p-4">
            <div className="flex items-center gap-2.5">
              <Shield className="w-4 h-4 text-amber-400 flex-shrink-0" />
              <div>
                <p className="text-sm font-medium text-amber-400">Read-Only Migration</p>
                <p className="text-xs text-zinc-400 mt-0.5">
                  Migrated tables are registered as <span className="text-zinc-300 font-medium">read-only</span> in Rake.
                  INSERT, UPDATE, DELETE, and DROP are blocked to protect source data during comparison testing.
                  {readOnlyTables.size > 0 && <span className="text-zinc-500 ml-1">({readOnlyTables.size} table{readOnlyTables.size !== 1 ? 's' : ''} protected)</span>}
                </p>
              </div>
            </div>
          </Card>

          {/* Action bar */}
          <div className="flex items-center gap-3">
            <Button
              onClick={() => handleRegister()}
              disabled={!selectedConn || registering || tables.length === 0}
              className="bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-400 border-emerald-500/30"
            >
              {registering ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <CheckCircle2 className="w-4 h-4 mr-2" />}
              Register All in Rake (Read-Only)
            </Button>
            {selectedForRegister.size > 0 && (
              <Button
                onClick={() => handleRegister(Array.from(selectedForRegister))}
                disabled={!selectedConn || registering}
                className="bg-rose-500/20 hover:bg-rose-500/30 text-rose-400 border-rose-500/30"
              >
                {registering ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Layers className="w-4 h-4 mr-2" />}
                Register Selected ({selectedForRegister.size})
              </Button>
            )}
            {tables.length > 0 && (
              <span className="text-xs text-zinc-500 ml-auto">
                {registeredCount}/{tables.length} registered
              </span>
            )}
          </div>

          {/* Progress tracker */}
          {tables.length > 0 && (
            <Card className="bg-zinc-900/30 border-zinc-800/50 p-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-xs text-zinc-500 font-medium uppercase tracking-wider">Migration Progress</span>
                <span className="text-sm font-bold text-zinc-200">{Math.round((registeredCount / tables.length) * 100)}%</span>
              </div>
              <div className="h-2 bg-zinc-800/50 rounded-full overflow-hidden">
                <div
                  className="h-full bg-gradient-to-r from-emerald-500 to-emerald-400 rounded-full transition-all duration-500"
                  style={{ width: `${tables.length > 0 ? (registeredCount / tables.length) * 100 : 0}%` }}
                />
              </div>
              <div className="flex gap-4 mt-2 text-2xs text-zinc-500">
                <span><span className="text-emerald-400 font-semibold">{registeredCount}</span> registered</span>
                <span><span className="text-cyan-400 font-semibold">{icebergCount}</span> Iceberg</span>
                <span><span className="text-zinc-400 font-semibold">{tables.length - registeredCount}</span> remaining</span>
              </div>
            </Card>
          )}

          {/* Migration table */}
          {tables.length > 0 ? (
            <Card className="bg-zinc-900/30 border-zinc-800/50 overflow-hidden" padding="none">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-zinc-800/30 text-zinc-500 bg-zinc-900/50">
                    <th className="text-left px-4 py-2.5 font-medium">Table</th>
                    <th className="text-left px-3 py-2.5 font-medium">Format</th>
                    <th className="text-left px-3 py-2.5 font-medium">S3 Location</th>
                    <th className="text-left px-3 py-2.5 font-medium">Trino Catalog</th>
                    <th className="text-left px-3 py-2.5 font-medium">Rake Status</th>
                    <th className="text-left px-3 py-2.5 font-medium">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {tables.map(t => {
                    const key = `${t.catalog}.${t.schema_name}.${t.table_name}`
                    const isIceberg = t.format === 'iceberg'
                    const hasS3 = !!t.location
                    const canMigrate = isIceberg && hasS3
                    const blocking: string[] = []
                    if (!isIceberg) blocking.push('Not Iceberg format')
                    if (!hasS3) blocking.push('No S3 location')
                    return (
                      <tr key={key} className="border-b border-zinc-800/20 hover:bg-white/[0.01]">
                        <td className="px-4 py-2.5">
                          <span className="font-mono text-zinc-300">{t.schema_name}.<span className="text-zinc-100">{t.table_name}</span></span>
                        </td>
                        <td className="px-3 py-2.5">
                          <Badge className={FORMAT_COLORS[t.format] || FORMAT_COLORS.unknown}>{t.format}</Badge>
                        </td>
                        <td className="px-3 py-2.5 text-zinc-500 font-mono truncate max-w-[180px]" title={t.location || undefined}>
                          {t.location || <span className="italic text-zinc-700">none</span>}
                        </td>
                        <td className="px-3 py-2.5 text-zinc-400">{t.catalog}</td>
                        <td className="px-3 py-2.5">
                          {t.registered_in_rake ? (
                            <div className="flex items-center gap-1.5">
                              <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">
                                <CheckCircle2 className="w-3 h-3 mr-1" />
                                {t.rake_table_name || 'registered'}
                              </Badge>
                              {t.rake_table_name && readOnlyTables.has(t.rake_table_name) && (
                                <Badge className="bg-zinc-400/10 text-zinc-400 border-zinc-500/20">
                                  <Shield className="w-2.5 h-2.5 mr-0.5" />read-only
                                </Badge>
                              )}
                            </div>
                          ) : canMigrate ? (
                            <Badge className="bg-amber-400/10 text-amber-400 border-amber-400/20">ready</Badge>
                          ) : (
                            <span className="text-2xs text-red-400" title={blocking.join(', ')}>
                              <AlertCircle className="w-3 h-3 inline mr-1" />
                              {blocking[0]}
                            </span>
                          )}
                        </td>
                        <td className="px-3 py-2.5">
                          {t.registered_in_rake ? (
                            <span className="text-2xs text-emerald-400">Done</span>
                          ) : canMigrate ? (
                            <Button
                              size="sm"
                              onClick={() => handleRegister([key])}
                              disabled={registering}
                              className="text-2xs bg-rose-500/10 hover:bg-rose-500/20 text-rose-400 border-rose-500/20 py-1 px-2"
                            >
                              Register
                            </Button>
                          ) : (
                            <span className="text-2xs text-zinc-600">blocked</span>
                          )}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </Card>
          ) : (
            <Card className="bg-zinc-900/30 border-zinc-800/50 p-8 text-center">
              <Database className="w-8 h-8 text-zinc-700 mx-auto mb-3" />
              <p className="text-sm text-zinc-500">Discover tables first to build a migration plan</p>
            </Card>
          )}

          {/* Benefits summary */}
          <Card className="bg-gradient-to-r from-rose-400/[0.03] to-amber-400/[0.03] border-rose-400/10 p-5">
            <p className="text-sm font-semibold text-zinc-200 mb-3 flex items-center gap-2">
              <Zap className="w-4 h-4 text-rose-400" />
              What You Gain by Migrating
            </p>
            <div className="grid grid-cols-2 gap-3">
              {[
                { icon: Server, label: 'No Trino coordinator bottleneck', desc: 'Queries execute directly without coordinator scheduling overhead', color: 'text-rose-400' },
                { icon: Clock, label: 'Sub-second cold start', desc: 'Rake starts in <500ms vs 30+ second JVM warmup for Trino', color: 'text-amber-400' },
                { icon: Layers, label: 'Native Arrow processing', desc: 'Zero serialization between query stages — Arrow RecordBatch end-to-end', color: 'text-cyan-400' },
                { icon: Cpu, label: 'Multi-engine flexibility', desc: 'DataFusion, DuckDB, and Polars — right engine for each workload', color: 'text-emerald-400' },
                { icon: Copy, label: 'Single binary deployment', desc: 'cargo install rustlake — no JVM, no class loading, no dependency hell', color: 'text-violet-400' },
                { icon: Shield, label: 'Same Iceberg tables', desc: 'No data migration needed — Rake reads the same S3 data Trino does', color: 'text-zinc-400' },
              ].map(b => (
                <div key={b.label} className="flex items-start gap-3 p-2">
                  <b.icon className={cn('w-4 h-4 flex-shrink-0 mt-0.5', b.color)} />
                  <div>
                    <p className="text-xs font-medium text-zinc-200">{b.label}</p>
                    <p className="text-2xs text-zinc-500 mt-0.5">{b.desc}</p>
                  </div>
                </div>
              ))}
            </div>
          </Card>
        </div>
      )}
    </div>
  )
}
