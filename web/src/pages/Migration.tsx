import { useState, useEffect, useCallback } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Button } from '../components/ui/Button'
import { cn } from '../lib/utils'
import {
  ArrowLeftRight, Search, Play, Loader2, CheckCircle2, AlertCircle,
  Database, Zap, ArrowRight, Server, HardDrive, Key,
  ChevronDown, ChevronRight, Shield, Clock,
  RefreshCw, Check, X, Info,
} from 'lucide-react'
import {
  getConnections, migrationDiscover, migrationRegister, migrationCompare,
  migrationCredentials, getMigrationTables, getMigrationComparisons,
} from '../api/client'
import type { ConnectionEntry } from '../types'
import type { MigrationTable, MigrationWarehouse, EngineResultM, MigrationComparison } from '../api/client'
import toast from 'react-hot-toast'
import { useAppStore } from '../stores/app'

const ENGINE_COLORS: Record<string, string> = {
  'Trino': 'bg-red-400/10 text-red-400 border-red-400/20',
  'DataFusion (direct)': 'bg-amber-400/10 text-amber-400 border-amber-400/20',
  'DuckDB': 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  'Polars': 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20',
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

const AWS_REGIONS = ['us-east-1', 'us-east-2', 'us-west-1', 'us-west-2', 'eu-west-1', 'eu-west-2', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1']

interface BucketCreds {
  bucket: string
  accessKey: string
  secretKey: string
  region: string
  status: 'idle' | 'saving' | 'connected' | 'error'
  error?: string
}

export function Migration() {
  const { darkMode } = useAppStore()
  const [step, setStep] = useState<1 | 2 | 3>(1)
  const [connections, setConnections] = useState<ConnectionEntry[]>([])
  const [selectedConn, setSelectedConn] = useState('')
  const [tables, setTables] = useState<MigrationTable[]>([])
  const [warehouses, setWarehouses] = useState<MigrationWarehouse[]>([])
  const [requiredBuckets, setRequiredBuckets] = useState<string[]>([])
  const [comparisons, setComparisons] = useState<MigrationComparison[]>([])
  const [discovering, setDiscovering] = useState(false)
  const [registering, setRegistering] = useState(false)
  const [comparing, setComparing] = useState(false)
  const [compareSql, setCompareSql] = useState('')
  const [lastComparison, setLastComparison] = useState<MigrationComparison | null>(null)
  const [expandedCatalogs, setExpandedCatalogs] = useState<Set<string>>(new Set())

  // S3 credentials per bucket
  const [bucketCreds, setBucketCreds] = useState<BucketCreds[]>([])
  const [scanning, setScanning] = useState(false)

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

  useEffect(() => { loadConnections() }, [loadConnections])
  useEffect(() => { loadTables(); loadComparisons() }, [selectedConn, loadTables, loadComparisons])

  // Step 1: Discover Iceberg catalogs
  const handleDiscover = async () => {
    if (!selectedConn) return
    setDiscovering(true)
    try {
      const r = await migrationDiscover(selectedConn)
      const icebergTables = (r.tables || []).filter(t => t.format === 'iceberg')
      setTables(icebergTables)
      setWarehouses(r.warehouses || [])
      setRequiredBuckets(r.required_buckets || [])
      const cats = [...new Set(icebergTables.map(t => t.catalog))]
      setExpandedCatalogs(new Set(cats))

      if (r.required_buckets && r.required_buckets.length > 0) {
        // Initialize bucket creds for required buckets
        setBucketCreds(prev => {
          const existing = new Map(prev.map(b => [b.bucket, b]))
          return r.required_buckets.map(bucket => existing.get(bucket) || {
            bucket, accessKey: '', secretKey: '', region: 'us-east-1', status: 'idle' as const,
          })
        })
        toast.success(`Found ${icebergTables.length} Iceberg tables across ${cats.length} catalogs. ${r.required_buckets.length} bucket${r.required_buckets.length !== 1 ? 's' : ''} need credentials.`)
        setStep(2)
      } else {
        toast.success(`Found ${icebergTables.length} Iceberg tables across ${cats.length} catalogs`)
        if (icebergTables.length > 0) setStep(3)
      }
    } catch (e) {
      toast.error((e as Error).message)
    }
    setDiscovering(false)
  }

  // Step 2: Save creds then re-run discover to scan S3
  const handleSaveAndScan = async () => {
    const incomplete = bucketCreds.filter(b => !b.accessKey.trim() || !b.secretKey.trim())
    if (incomplete.length > 0) {
      toast.error(`Please fill in credentials for all ${incomplete.length} bucket${incomplete.length !== 1 ? 's' : ''}`)
      return
    }

    setScanning(true)
    // Save each bucket's credentials
    for (const cred of bucketCreds) {
      setBucketCreds(prev => prev.map(b => b.bucket === cred.bucket ? { ...b, status: 'saving' } : b))
      try {
        await migrationCredentials(cred.bucket, cred.accessKey, cred.secretKey, cred.region)
        setBucketCreds(prev => prev.map(b => b.bucket === cred.bucket ? { ...b, status: 'connected' } : b))
      } catch (e) {
        setBucketCreds(prev => prev.map(b => b.bucket === cred.bucket ? { ...b, status: 'error', error: (e as Error).message } : b))
        toast.error(`Failed to save credentials for ${cred.bucket}: ${(e as Error).message}`)
        setScanning(false)
        return
      }
    }

    // Re-run discover with creds to scan S3
    try {
      const r = await migrationDiscover(selectedConn)
      const icebergTables = (r.tables || []).filter(t => t.format === 'iceberg')
      setTables(icebergTables)
      toast.success(`S3 scan complete: found ${icebergTables.length} Iceberg tables`)
      setStep(3)
    } catch (e) {
      toast.error((e as Error).message)
    }
    setScanning(false)
  }

  const updateBucketCred = (bucket: string, field: keyof BucketCreds, value: string) => {
    setBucketCreds(prev => prev.map(b => b.bucket === bucket ? { ...b, [field]: value } : b))
  }

  // Step 3: Register all tables
  const handleRegister = async () => {
    if (!selectedConn) return
    setRegistering(true)
    try {
      const r = await migrationRegister(selectedConn)
      toast.success(`Registered ${r.registered}/${r.total} tables in Rake`)
      if (r.errors.length > 0) toast.error(`${r.errors.length} tables failed to register`)
      await loadTables()
    } catch (e) {
      toast.error((e as Error).message)
    }
    setRegistering(false)
  }

  // Step 3: Compare query
  const handleCompare = async () => {
    if (!selectedConn || !compareSql.trim()) return
    setComparing(true)
    try {
      const r = await migrationCompare(selectedConn, compareSql.trim(), undefined, true)
      setLastComparison(r)
      toast.success(`Comparison complete -- ${r.winner} wins (${r.speedup.toFixed(1)}x faster)`)
      await loadComparisons()
    } catch (e) {
      toast.error((e as Error).message)
    }
    setComparing(false)
  }

  // Group iceberg tables by catalog
  const catalogGroups = tables.reduce<Record<string, MigrationTable[]>>((acc, t) => {
    if (!acc[t.catalog]) acc[t.catalog] = []
    acc[t.catalog].push(t)
    return acc
  }, {})

  const icebergCount = tables.length
  const registeredCount = tables.filter(t => t.registered_in_rake).length

  const stepItems = [
    { step: 1 as const, label: 'Discover' },
    { step: 2 as const, label: 'S3 Credentials' },
    { step: 3 as const, label: 'Compare' },
  ]

  return (
    <div className={cn('p-6 space-y-6 max-w-[1400px] mx-auto', darkMode ? '' : 'text-zinc-900')}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className={cn('text-xl font-bold flex items-center gap-2', darkMode ? 'text-zinc-100' : 'text-zinc-900')}>
            <ArrowLeftRight className="w-5 h-5 text-rose-400" />
            Iceberg Migration
          </h1>
          <p className={cn('text-sm mt-1', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
            Discover Iceberg catalogs in Trino, connect to S3, and compare engine performance
          </p>
        </div>
        <div className="flex items-center gap-3">
          <select
            value={selectedConn}
            onChange={e => setSelectedConn(e.target.value)}
            className={cn(
              'border rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-rose-400/50',
              darkMode ? 'bg-zinc-900 border-zinc-700/50 text-zinc-300' : 'bg-white border-zinc-300 text-zinc-700'
            )}
          >
            <option value="">Select Trino connection</option>
            {connections.map(c => (
              <option key={c.id} value={c.id}>{c.name} ({c.host}:{c.port})</option>
            ))}
          </select>
        </div>
      </div>

      {/* Architecture diagram */}
      <Card className={cn('p-5', darkMode ? 'bg-zinc-900/50 border-zinc-800/50' : 'bg-white border-zinc-200')}>
        <div className="flex items-center justify-between gap-6">
          <div className="flex items-center gap-4 flex-1">
            <div className="flex flex-col items-center gap-1.5">
              <div className="flex items-center gap-2">
                <div className="w-11 h-11 rounded-lg bg-red-400/10 border border-red-400/20 flex items-center justify-center">
                  <Server className="w-5 h-5 text-red-400" />
                </div>
                <div className="text-left">
                  <span className="text-xs text-red-400 font-semibold block">Trino</span>
                  <span className={cn('text-2xs', darkMode ? 'text-zinc-600' : 'text-zinc-500')}>catalog discovery</span>
                </div>
              </div>
            </div>
            <ArrowRight className={cn('w-4 h-4 flex-shrink-0', darkMode ? 'text-zinc-600' : 'text-zinc-400')} />
            <div className="flex flex-col items-center gap-1.5">
              <div className="w-14 h-14 rounded-xl bg-cyan-400/10 border-2 border-cyan-400/20 flex items-center justify-center relative">
                <HardDrive className="w-6 h-6 text-cyan-400" />
                <span className="absolute -top-2 -right-2 bg-cyan-400/20 text-cyan-400 text-[9px] font-bold px-1.5 py-0.5 rounded-full border border-cyan-400/30">
                  Iceberg
                </span>
              </div>
              <span className="text-xs text-cyan-400 font-semibold">S3</span>
              <span className={cn('text-2xs', darkMode ? 'text-zinc-600' : 'text-zinc-500')}>Parquet + metadata</span>
            </div>
            <ArrowRight className={cn('w-4 h-4 flex-shrink-0 rotate-180', darkMode ? 'text-zinc-600' : 'text-zinc-400')} />
            <div className="flex flex-col items-center gap-1.5">
              <div className="flex items-center gap-2">
                <div className="w-11 h-11 rounded-lg bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
                  <Zap className="w-5 h-5 text-amber-400" />
                </div>
                <div className="text-left">
                  <span className="text-xs text-amber-400 font-semibold block">Rake</span>
                  <span className={cn('text-2xs', darkMode ? 'text-zinc-600' : 'text-zinc-500')}>Direct S3 access</span>
                </div>
              </div>
            </div>
          </div>
          <div className={cn('text-right text-xs max-w-[320px] space-y-1', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
            <p className={cn('font-medium', darkMode ? 'text-zinc-400' : 'text-zinc-700')}>Minimal Trino interaction</p>
            <p>Trino is only used to discover Iceberg catalogs.</p>
            <p>All data reads go directly through S3.</p>
          </div>
        </div>
      </Card>

      {/* Step indicator */}
      <div className={cn('flex gap-1 border-b', darkMode ? 'border-zinc-800/50' : 'border-zinc-200')}>
        {stepItems.map(s => (
          <button
            key={s.step}
            onClick={() => setStep(s.step)}
            className={cn(
              'px-4 py-2 text-sm font-medium border-b-2 transition-colors flex items-center gap-2',
              step === s.step
                ? 'text-rose-400 border-rose-400'
                : darkMode ? 'text-zinc-500 border-transparent hover:text-zinc-300' : 'text-zinc-400 border-transparent hover:text-zinc-700'
            )}
          >
            <span className={cn(
              'w-5 h-5 rounded-full text-xs flex items-center justify-center font-bold',
              step === s.step ? 'bg-rose-400/20 text-rose-400' :
                step > s.step ? 'bg-emerald-400/20 text-emerald-400' :
                  darkMode ? 'bg-zinc-800 text-zinc-500' : 'bg-zinc-200 text-zinc-500'
            )}>
              {step > s.step ? <Check className="w-3 h-3" /> : s.step}
            </span>
            {s.label}
          </button>
        ))}
      </div>

      {/* ============ STEP 1: DISCOVER ============ */}
      {step === 1 && (
        <div className="space-y-4">
          <div className="flex items-center gap-3">
            <Button
              onClick={handleDiscover}
              disabled={!selectedConn || discovering}
              className="bg-rose-500/20 hover:bg-rose-500/30 text-rose-400 border-rose-500/30"
            >
              {discovering ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Search className="w-4 h-4 mr-2" />}
              Discover Iceberg Catalogs
            </Button>
            {tables.length > 0 && (
              <span className={cn('text-xs ml-auto', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
                {Object.keys(catalogGroups).length} catalog{Object.keys(catalogGroups).length !== 1 ? 's' : ''} with {icebergCount} Iceberg table{icebergCount !== 1 ? 's' : ''}
              </span>
            )}
          </div>

          {/* Warehouse locations */}
          {warehouses.length > 0 && (
            <Card className={cn('p-4', darkMode ? 'bg-cyan-400/[0.03] border-cyan-400/10' : 'bg-cyan-50 border-cyan-200')}>
              <div className="flex items-start gap-3">
                <HardDrive className="w-4 h-4 text-cyan-400 flex-shrink-0 mt-0.5" />
                <div className="flex-1">
                  <p className="text-sm text-cyan-400 font-medium mb-2">Iceberg Warehouse Locations</p>
                  <div className="space-y-1.5">
                    {warehouses.map(w => (
                      <div key={`${w.catalog}-${w.warehouse}`} className="flex items-center gap-3 text-xs">
                        <Badge className="bg-cyan-400/10 text-cyan-400 border-cyan-400/20">{w.catalog}</Badge>
                        <span className={cn('font-mono truncate', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>{w.warehouse}</span>
                        <Badge className={cn(darkMode ? 'bg-zinc-800 text-zinc-400 border-zinc-700' : 'bg-zinc-100 text-zinc-600 border-zinc-300')}>
                          bucket: {w.bucket}
                        </Badge>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </Card>
          )}

          {/* Required buckets notice */}
          {requiredBuckets.length > 0 && (
            <Card className={cn('p-4', darkMode ? 'bg-amber-400/[0.03] border-amber-400/10' : 'bg-amber-50 border-amber-200')}>
              <div className="flex items-start gap-3">
                <Key className="w-4 h-4 text-amber-400 flex-shrink-0 mt-0.5" />
                <div>
                  <p className="text-sm text-amber-400 font-medium">S3 Credentials Required</p>
                  <p className={cn('text-xs mt-1', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>
                    The following bucket{requiredBuckets.length !== 1 ? 's' : ''} need AWS credentials to scan for Iceberg tables:
                  </p>
                  <div className="flex gap-2 mt-2">
                    {requiredBuckets.map(b => (
                      <Badge key={b} className="bg-amber-400/10 text-amber-400 border-amber-400/20 font-mono">{b}</Badge>
                    ))}
                  </div>
                  <Button
                    onClick={() => setStep(2)}
                    className="mt-3 bg-amber-500/20 hover:bg-amber-500/30 text-amber-400 border-amber-500/30"
                    size="sm"
                  >
                    <Key className="w-3 h-3 mr-1.5" /> Provide Credentials
                  </Button>
                </div>
              </div>
            </Card>
          )}

          {/* Catalog tree - Iceberg only */}
          {Object.entries(catalogGroups).map(([catalog, catTables]) => {
            const isExpanded = expandedCatalogs.has(catalog)
            const registeredInCat = catTables.filter(t => t.registered_in_rake).length
            return (
              <Card key={catalog} className={cn('overflow-hidden', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')} padding="none">
                <button
                  onClick={() => setExpandedCatalogs(prev => {
                    const next = new Set(prev)
                    if (next.has(catalog)) next.delete(catalog); else next.add(catalog)
                    return next
                  })}
                  className={cn('w-full flex items-center gap-3 p-3 transition-colors', darkMode ? 'hover:bg-white/[0.02]' : 'hover:bg-zinc-50')}
                >
                  {isExpanded ? <ChevronDown className="w-4 h-4 text-zinc-500" /> : <ChevronRight className="w-4 h-4 text-zinc-500" />}
                  <Database className="w-4 h-4 text-cyan-400" />
                  <span className={cn('text-sm font-semibold', darkMode ? 'text-zinc-200' : 'text-zinc-800')}>{catalog}</span>
                  <Badge className="bg-cyan-400/10 text-cyan-400 border-cyan-400/20">iceberg</Badge>
                  <span className={cn('text-xs ml-auto mr-2', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>{catTables.length} tables</span>
                  {registeredInCat > 0 && (
                    <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">
                      {registeredInCat} registered
                    </Badge>
                  )}
                </button>
                {isExpanded && (
                  <div className={cn('border-t', darkMode ? 'border-zinc-800/30' : 'border-zinc-200')}>
                    <table className="w-full text-xs">
                      <thead>
                        <tr className={cn('border-b', darkMode ? 'border-zinc-800/30 text-zinc-500' : 'border-zinc-200 text-zinc-500')}>
                          <th className="text-left px-4 py-2 font-medium">Table</th>
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
                            <tr key={key} className={cn('border-b', darkMode ? 'border-zinc-800/20 hover:bg-white/[0.01]' : 'border-zinc-100 hover:bg-zinc-50')}>
                              <td className="px-4 py-2">
                                <span className={cn('font-mono', darkMode ? 'text-zinc-300' : 'text-zinc-600')}>
                                  {t.schema_name}.<span className={darkMode ? 'text-zinc-100' : 'text-zinc-900'}>{t.table_name}</span>
                                </span>
                              </td>
                              <td className={cn('px-3 py-2 text-right', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>{t.column_count}</td>
                              <td className={cn('px-3 py-2 text-right', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>
                                {t.row_count != null ? t.row_count.toLocaleString() : '\u2014'}
                              </td>
                              <td className={cn('px-3 py-2 font-mono truncate max-w-[220px]', darkMode ? 'text-zinc-500' : 'text-zinc-500')} title={t.location || undefined}>
                                {t.location || <span className={cn('italic', darkMode ? 'text-zinc-700' : 'text-zinc-400')}>no S3 path</span>}
                              </td>
                              <td className="px-3 py-2">
                                {t.registered_in_rake ? (
                                  <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">
                                    <CheckCircle2 className="w-3 h-3 mr-1" />registered
                                  </Badge>
                                ) : (
                                  <Badge className="bg-zinc-400/10 text-zinc-400 border-zinc-400/20">discovered</Badge>
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
            <Card className={cn('p-8 text-center', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')}>
              <Search className={cn('w-8 h-8 mx-auto mb-3', darkMode ? 'text-zinc-700' : 'text-zinc-400')} />
              <p className={cn('text-sm', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
                Click &quot;Discover Iceberg Catalogs&quot; to find Iceberg tables in your Trino connection
              </p>
            </Card>
          )}

          {!selectedConn && (
            <Card className={cn('p-8 text-center', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')}>
              <Server className={cn('w-8 h-8 mx-auto mb-3', darkMode ? 'text-zinc-700' : 'text-zinc-400')} />
              <p className={cn('text-sm', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>Select a Trino connection to begin</p>
            </Card>
          )}
        </div>
      )}

      {/* ============ STEP 2: S3 CREDENTIALS + SCAN ============ */}
      {step === 2 && (
        <div className="space-y-4">
          <Card className={cn('p-4', darkMode ? 'bg-cyan-400/[0.03] border-cyan-400/10' : 'bg-cyan-50 border-cyan-200')}>
            <div className="flex items-start gap-3">
              <Info className="w-5 h-5 text-cyan-400 flex-shrink-0 mt-0.5" />
              <div>
                <p className="text-sm text-cyan-400 font-medium">Direct S3 Access</p>
                <p className={cn('text-xs mt-1', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>
                  Provide AWS credentials for each S3 bucket. Rake will scan the buckets directly to discover all Iceberg tables,
                  bypassing Trino entirely for data access.
                </p>
              </div>
            </div>
          </Card>

          {bucketCreds.length === 0 ? (
            <Card className={cn('p-8 text-center', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')}>
              <Key className={cn('w-8 h-8 mx-auto mb-3', darkMode ? 'text-zinc-700' : 'text-zinc-400')} />
              <p className={cn('text-sm', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
                Run discovery first to identify which S3 buckets need credentials
              </p>
              <Button onClick={() => setStep(1)} className="mt-3 bg-rose-500/20 hover:bg-rose-500/30 text-rose-400 border-rose-500/30" size="sm">
                <ArrowLeftRight className="w-3 h-3 mr-1.5" /> Go to Discover
              </Button>
            </Card>
          ) : (
            <div className="space-y-4">
              {bucketCreds.map(cred => (
                <Card
                  key={cred.bucket}
                  className={cn(
                    'p-5',
                    cred.status === 'connected'
                      ? darkMode ? 'bg-emerald-400/[0.03] border-emerald-400/20' : 'bg-emerald-50 border-emerald-200'
                      : cred.status === 'error'
                        ? darkMode ? 'bg-red-400/[0.03] border-red-400/20' : 'bg-red-50 border-red-200'
                        : darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200'
                  )}
                >
                  <div className="flex items-center gap-3 mb-4">
                    <HardDrive className={cn('w-4 h-4', cred.status === 'connected' ? 'text-emerald-400' : cred.status === 'error' ? 'text-red-400' : 'text-cyan-400')} />
                    <span className={cn('text-sm font-semibold font-mono', darkMode ? 'text-zinc-200' : 'text-zinc-800')}>
                      s3://{cred.bucket}
                    </span>
                    {cred.status === 'connected' && (
                      <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20 ml-auto">
                        <Check className="w-3 h-3 mr-1" />connected
                      </Badge>
                    )}
                    {cred.status === 'error' && (
                      <Badge className="bg-red-400/10 text-red-400 border-red-400/20 ml-auto">
                        <X className="w-3 h-3 mr-1" />error
                      </Badge>
                    )}
                  </div>
                  {cred.status !== 'connected' && (
                    <div className="grid grid-cols-3 gap-3">
                      <div>
                        <label className={cn('text-2xs uppercase tracking-wider mb-1 block', darkMode ? 'text-zinc-500' : 'text-zinc-500')}>Access Key ID</label>
                        <input
                          type="text"
                          value={cred.accessKey}
                          onChange={e => updateBucketCred(cred.bucket, 'accessKey', e.target.value)}
                          placeholder="AKIA..."
                          className={cn(
                            'w-full border rounded-lg px-3 py-2 text-sm font-mono focus:outline-none focus:border-rose-400/50',
                            darkMode ? 'bg-zinc-950 border-zinc-800/50 text-zinc-300 placeholder:text-zinc-700' : 'bg-white border-zinc-300 text-zinc-700 placeholder:text-zinc-400'
                          )}
                        />
                      </div>
                      <div>
                        <label className={cn('text-2xs uppercase tracking-wider mb-1 block', darkMode ? 'text-zinc-500' : 'text-zinc-500')}>Secret Access Key</label>
                        <input
                          type="password"
                          value={cred.secretKey}
                          onChange={e => updateBucketCred(cred.bucket, 'secretKey', e.target.value)}
                          placeholder="secret..."
                          className={cn(
                            'w-full border rounded-lg px-3 py-2 text-sm font-mono focus:outline-none focus:border-rose-400/50',
                            darkMode ? 'bg-zinc-950 border-zinc-800/50 text-zinc-300 placeholder:text-zinc-700' : 'bg-white border-zinc-300 text-zinc-700 placeholder:text-zinc-400'
                          )}
                        />
                      </div>
                      <div>
                        <label className={cn('text-2xs uppercase tracking-wider mb-1 block', darkMode ? 'text-zinc-500' : 'text-zinc-500')}>AWS Region</label>
                        <select
                          value={cred.region}
                          onChange={e => updateBucketCred(cred.bucket, 'region', e.target.value)}
                          className={cn(
                            'w-full border rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-rose-400/50',
                            darkMode ? 'bg-zinc-950 border-zinc-800/50 text-zinc-300' : 'bg-white border-zinc-300 text-zinc-700'
                          )}
                        >
                          {AWS_REGIONS.map(r => (
                            <option key={r} value={r}>{r}</option>
                          ))}
                        </select>
                      </div>
                    </div>
                  )}
                  {cred.error && (
                    <p className="text-xs text-red-400 mt-2">{cred.error}</p>
                  )}
                </Card>
              ))}

              <Button
                onClick={handleSaveAndScan}
                disabled={scanning || bucketCreds.some(b => !b.accessKey.trim() || !b.secretKey.trim())}
                className="bg-rose-500/20 hover:bg-rose-500/30 text-rose-400 border-rose-500/30"
              >
                {scanning ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <Shield className="w-4 h-4 mr-2" />}
                Save & Scan S3
              </Button>
            </div>
          )}

          {/* Show discovered tables if we have any */}
          {tables.length > 0 && (
            <Card className={cn('p-4', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')}>
              <div className="flex items-center gap-2 mb-2">
                <Database className="w-4 h-4 text-cyan-400" />
                <span className={cn('text-sm font-semibold', darkMode ? 'text-zinc-300' : 'text-zinc-700')}>
                  Discovered Iceberg Tables
                </span>
                <Badge className="bg-cyan-400/10 text-cyan-400 border-cyan-400/20 ml-auto">{tables.length} tables</Badge>
              </div>
              <div className="text-xs space-y-1">
                {Object.entries(catalogGroups).map(([cat, tbs]) => (
                  <div key={cat} className="flex items-center gap-2">
                    <span className={cn('font-mono', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>{cat}</span>
                    <span className={darkMode ? 'text-zinc-600' : 'text-zinc-400'}>{tbs.length} tables</span>
                  </div>
                ))}
              </div>
            </Card>
          )}
        </div>
      )}

      {/* ============ STEP 3: COMPARE ============ */}
      {step === 3 && (
        <div className="space-y-4">
          {/* Stats bar */}
          {tables.length > 0 && (
            <div className="grid grid-cols-3 gap-3">
              {[
                { label: 'Iceberg Tables', value: icebergCount, icon: Database, color: 'text-cyan-400' },
                { label: 'Registered', value: registeredCount, icon: CheckCircle2, color: 'text-emerald-400' },
                { label: 'Comparisons', value: comparisons.length, icon: Clock, color: 'text-rose-400' },
              ].map(s => (
                <Card key={s.label} className={cn('p-3 flex items-center gap-3', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')}>
                  <s.icon className={cn('w-4 h-4', s.color)} />
                  <div>
                    <p className={cn('text-lg font-bold', darkMode ? 'text-zinc-100' : 'text-zinc-900')}>{s.value}</p>
                    <p className={cn('text-2xs uppercase tracking-wider', darkMode ? 'text-zinc-500' : 'text-zinc-500')}>{s.label}</p>
                  </div>
                </Card>
              ))}
            </div>
          )}

          {/* Register all */}
          <div className="flex items-center gap-3">
            <Button
              onClick={handleRegister}
              disabled={!selectedConn || registering || tables.length === 0 || registeredCount === tables.length}
              className="bg-emerald-500/20 hover:bg-emerald-500/30 text-emerald-400 border-emerald-500/30"
            >
              {registering ? <Loader2 className="w-4 h-4 animate-spin mr-2" /> : <CheckCircle2 className="w-4 h-4 mr-2" />}
              Register All Tables ({tables.length - registeredCount} remaining)
            </Button>
            {registeredCount > 0 && (
              <span className={cn('text-xs', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
                {registeredCount}/{tables.length} registered
              </span>
            )}
          </div>

          {/* Compare form */}
          <Card className={cn('p-4 space-y-3', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')}>
            <div className="flex items-center gap-2">
              <Play className="w-4 h-4 text-rose-400" />
              <span className={cn('text-sm font-semibold', darkMode ? 'text-zinc-300' : 'text-zinc-700')}>Performance Comparison</span>
              <span className={cn('text-2xs ml-auto', darkMode ? 'text-zinc-600' : 'text-zinc-500')}>
                Trino vs DataFusion vs DuckDB vs Polars
              </span>
            </div>
            <textarea
              value={compareSql}
              onChange={e => setCompareSql(e.target.value)}
              placeholder={"SELECT region, COUNT(*) as cnt, SUM(total_price) as revenue\nFROM iceberg_catalog.tpch.orders\nGROUP BY region ORDER BY revenue DESC"}
              className={cn(
                'w-full h-28 border rounded-lg p-3 text-sm font-mono focus:outline-none focus:border-rose-400/50 resize-none',
                darkMode ? 'bg-zinc-950 border-zinc-800/50 text-zinc-300 placeholder:text-zinc-700' : 'bg-white border-zinc-300 text-zinc-700 placeholder:text-zinc-400'
              )}
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
              {comparisons.length > 0 && (
                <span className={cn('text-xs ml-auto', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
                  {comparisons.length} previous comparison{comparisons.length !== 1 ? 's' : ''}
                </span>
              )}
            </div>
          </Card>

          {/* Comparison Results */}
          {lastComparison && (
            <div className="space-y-4">
              {/* Winner banner */}
              <Card className={cn('p-4', darkMode ? 'bg-gradient-to-r from-emerald-400/5 to-cyan-400/5 border-emerald-400/20' : 'bg-gradient-to-r from-emerald-50 to-cyan-50 border-emerald-200')}>
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <Zap className="w-6 h-6 text-emerald-400" />
                    <div>
                      <p className={cn('text-lg font-bold', darkMode ? 'text-zinc-100' : 'text-zinc-900')}>
                        {lastComparison.winner} is{' '}
                        <span className="text-emerald-400">{lastComparison.speedup.toFixed(1)}x faster</span>
                        {' '}than Trino
                      </p>
                      <p className={cn('text-xs', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
                        Data match: {lastComparison.data_match
                          ? <span className="text-emerald-400">verified (all engines return same row count)</span>
                          : <span className="text-amber-400">mismatch detected</span>
                        }
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className={cn('text-2xs', darkMode ? 'text-zinc-500' : 'text-zinc-500')}>Query</p>
                    <p className={cn('text-xs font-mono max-w-[300px] truncate', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>{lastComparison.sql || compareSql}</p>
                  </div>
                </div>
              </Card>

              {/* Engine cards */}
              <div className="grid grid-cols-4 gap-3">
                {lastComparison.results.map(r => {
                  const isWinner = r.engine === lastComparison!.winner
                  const isTrino = r.engine === 'Trino'
                  const trinoMs = lastComparison!.results.find(x => x.engine === 'Trino')?.duration_ms || 1
                  const speedup = isTrino ? 1 : trinoMs / Math.max(r.duration_ms, 1)
                  return (
                    <Card
                      key={r.engine}
                      className={cn(
                        'p-4 border transition-all',
                        isWinner
                          ? 'border-emerald-400/30 bg-emerald-400/[0.03] ring-1 ring-emerald-400/10'
                          : darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200',
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
                            <p className={cn('text-2xl font-bold', isWinner ? 'text-emerald-400' : darkMode ? 'text-zinc-200' : 'text-zinc-800')}>
                              {r.duration_ms}ms
                            </p>
                            <div className="mt-1.5">
                              <Badge className={PATH_COLORS[r.path || (isTrino ? 'via_trino' : 's3_direct')] || PATH_COLORS.s3_direct}>
                                {PATH_LABELS[r.path || (isTrino ? 'via_trino' : 's3_direct')] || r.path || 'S3 direct'}
                              </Badge>
                            </div>
                            <p className={cn('text-xs mt-1', darkMode ? 'text-zinc-500' : 'text-zinc-600')}>{r.row_count} rows</p>
                            {!isTrino && (
                              <p className={cn('text-xs mt-1', speedup > 5 ? 'text-emerald-400' : speedup > 2 ? 'text-amber-400' : darkMode ? 'text-zinc-500' : 'text-zinc-600')}>
                                {speedup.toFixed(1)}x vs Trino
                              </p>
                            )}
                            {isTrino && <p className={cn('text-xs mt-1', darkMode ? 'text-zinc-600' : 'text-zinc-500')}>baseline</p>}
                          </>
                        ) : r.status === 'error' ? (
                          <div>
                            <p className="text-sm text-red-400">Error</p>
                            <p className={cn('text-2xs mt-1 truncate', darkMode ? 'text-zinc-600' : 'text-zinc-500')}>{r.error}</p>
                          </div>
                        ) : (
                          <p className={cn('text-sm', darkMode ? 'text-zinc-600' : 'text-zinc-500')}>Unavailable</p>
                        )}
                      </div>
                    </Card>
                  )
                })}
              </div>

              {/* Visual latency bars */}
              <Card className={cn('p-4', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')}>
                <p className={cn('text-xs mb-3 font-medium uppercase tracking-wider', darkMode ? 'text-zinc-500' : 'text-zinc-500')}>Latency Comparison</p>
                {lastComparison.results
                  .filter(r => r.status === 'success')
                  .sort((a, b) => a.duration_ms - b.duration_ms)
                  .map(r => {
                    const maxMs = Math.max(...lastComparison!.results.filter(x => x.status === 'success').map(x => x.duration_ms))
                    const pct = maxMs > 0 ? (r.duration_ms / maxMs) * 100 : 0
                    const isTrino = r.engine === 'Trino'
                    return (
                      <div key={r.engine} className="flex items-center gap-3 mb-2">
                        <span className={cn('text-xs w-36 text-right', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>{r.engine}</span>
                        <div className={cn('flex-1 h-6 rounded overflow-hidden relative', darkMode ? 'bg-zinc-800/30' : 'bg-zinc-100')}>
                          <div
                            className={cn(
                              'h-full rounded transition-all duration-500',
                              isTrino ? 'bg-red-400/40' : r.engine === lastComparison!.winner ? 'bg-emerald-400/40' : 'bg-amber-400/30'
                            )}
                            style={{ width: `${Math.max(pct, 2)}%` }}
                          />
                          <span className={cn('absolute inset-y-0 left-2 flex items-center text-2xs font-mono', darkMode ? 'text-zinc-300' : 'text-zinc-700')}>
                            {r.duration_ms}ms
                          </span>
                        </div>
                      </div>
                    )
                  })}
              </Card>

              {/* Why Rake is Faster */}
              <Card className={cn('p-4', darkMode ? 'bg-amber-400/[0.03] border-amber-400/10' : 'bg-amber-50 border-amber-200')}>
                <div className="flex items-start gap-3">
                  <Zap className="w-5 h-5 text-amber-400 flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="text-sm text-amber-400 font-medium">Why Rake is Faster</p>
                    <div className={cn('grid grid-cols-2 gap-x-6 gap-y-1 mt-2 text-xs', darkMode ? 'text-zinc-400' : 'text-zinc-600')}>
                      <p><span className="text-amber-400/80">No coordinator overhead</span> -- queries go straight to S3</p>
                      <p><span className="text-amber-400/80">Arrow-native execution</span> -- zero serialization between stages</p>
                      <p><span className="text-amber-400/80">No JVM GC pauses</span> -- Rust, predictable latency</p>
                      <p><span className="text-amber-400/80">Direct S3 reads</span> -- Iceberg metadata parsed natively</p>
                    </div>
                  </div>
                </div>
              </Card>
            </div>
          )}

          {/* Previous comparisons */}
          {comparisons.length > 0 && (
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <p className={cn('text-xs font-medium uppercase tracking-wider', darkMode ? 'text-zinc-500' : 'text-zinc-500')}>Previous Comparisons</p>
                <Button onClick={loadComparisons} size="sm" className={cn('text-xs', darkMode ? 'bg-zinc-800 hover:bg-zinc-700 text-zinc-300 border-zinc-700' : 'bg-zinc-100 hover:bg-zinc-200 text-zinc-700 border-zinc-300')}>
                  <RefreshCw className="w-3 h-3 mr-1" /> Refresh
                </Button>
              </div>
              {comparisons.map(c => (
                <Card key={c.id} className={cn('p-3', darkMode ? 'bg-zinc-900/30 border-zinc-800/50' : 'bg-white border-zinc-200')}>
                  <div className="flex items-center justify-between mb-1.5">
                    <button
                      onClick={() => { setCompareSql(c.sql); setLastComparison(c) }}
                      className={cn('text-xs font-mono truncate max-w-[400px] transition-colors text-left', darkMode ? 'text-zinc-400 hover:text-zinc-200' : 'text-zinc-600 hover:text-zinc-900')}
                    >
                      {c.sql}
                    </button>
                    <div className="flex items-center gap-2">
                      <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">
                        {c.winner} -- {c.speedup.toFixed(1)}x
                      </Badge>
                      {c.data_match
                        ? <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
                        : <AlertCircle className="w-3.5 h-3.5 text-amber-400" />
                      }
                    </div>
                  </div>
                  <div className="flex gap-3">
                    {c.results.filter(r => r.status === 'success').map(r => (
                      <span key={r.engine} className={cn('text-2xs', darkMode ? 'text-zinc-500' : 'text-zinc-500')}>
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
    </div>
  )
}
