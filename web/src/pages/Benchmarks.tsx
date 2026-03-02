import { useState, useEffect, useCallback } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Button } from '../components/ui/Button'
import { DataTable } from '../components/ui/DataTable'
import { cn, formatDuration } from '../lib/utils'
import {
  Timer, Play, Database, BarChart3, CheckCircle2,
  Loader2, AlertCircle, Zap, Layers, Clock, RefreshCw,
} from 'lucide-react'
import { getBenchmarkQueries, runBenchmark, getBenchmarkResults, getBootstrapStatus } from '../api/client'
import type { BenchmarkQuery, BenchmarkResult, BenchmarkRunResponse, BootstrapStatus } from '../types'
import toast from 'react-hot-toast'

const CATEGORY_COLORS: Record<string, string> = {
  'Aggregation': 'bg-amber-400/10 text-amber-400 border-amber-400/20',
  'Join + Filter': 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20',
  'Subquery': 'bg-violet-400/10 text-violet-400 border-violet-400/20',
  'Multi-Join': 'bg-blue-400/10 text-blue-400 border-blue-400/20',
  'Scan + Filter': 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  'Complex Join': 'bg-rose-400/10 text-rose-400 border-rose-400/20',
  'Join + Aggregation': 'bg-sky-400/10 text-sky-400 border-sky-400/20',
  'Case + Aggregation': 'bg-orange-400/10 text-orange-400 border-orange-400/20',
  'Left Join + Aggregation': 'bg-pink-400/10 text-pink-400 border-pink-400/20',
  'Join + Conditional': 'bg-teal-400/10 text-teal-400 border-teal-400/20',
}

export function Benchmarks() {
  const [queries, setQueries] = useState<BenchmarkQuery[]>([])
  const [results, setResults] = useState<Map<string, BenchmarkResult>>(new Map())
  const [running, setRunning] = useState<string | null>(null)
  const [runAll, setRunAll] = useState(false)
  const [lastRun, setLastRun] = useState<BenchmarkRunResponse | null>(null)
  const [tables, setTables] = useState<Record<string, number>>({})
  const [scaleFactor, setScaleFactor] = useState('')
  const [bootstrap, setBootstrap] = useState<BootstrapStatus | null>(null)

  const fetchData = useCallback(async () => {
    try {
      const [q, r, bs] = await Promise.all([
        getBenchmarkQueries(),
        getBenchmarkResults(),
        getBootstrapStatus(),
      ])
      setQueries(q.queries)
      setTables(q.tables)
      setScaleFactor(q.scale_factor)
      setBootstrap(bs)

      // Map latest result per query
      const map = new Map<string, BenchmarkResult>()
      for (const res of r.results) {
        map.set(res.query_id, res)
      }
      setResults(map)
    } catch { /* ignore */ }
  }, [])

  useEffect(() => { fetchData() }, [fetchData])

  const handleRun = async (queryId: string) => {
    setRunning(queryId)
    try {
      const res = await runBenchmark(queryId)
      setLastRun(res)
      setResults(prev => {
        const next = new Map(prev)
        next.set(queryId, {
          query_id: res.query_id,
          query_name: res.query_name,
          duration_ms: res.duration_ms,
          row_count: res.row_count,
          status: res.status,
          timestamp: new Date().toISOString(),
        })
        return next
      })
      toast.success(`${res.query_name}: ${res.duration_ms}ms`)
    } catch (e: unknown) {
      toast.error(`Failed: ${e instanceof Error ? e.message : 'Unknown error'}`)
    } finally {
      setRunning(null)
    }
  }

  const handleRunAll = async () => {
    setRunAll(true)
    for (const q of queries) {
      setRunning(q.id)
      try {
        const res = await runBenchmark(q.id)
        setLastRun(res)
        setResults(prev => {
          const next = new Map(prev)
          next.set(q.id, {
            query_id: res.query_id,
            query_name: res.query_name,
            duration_ms: res.duration_ms,
            row_count: res.row_count,
            status: res.status,
            timestamp: new Date().toISOString(),
          })
          return next
        })
      } catch { /* continue */ }
    }
    setRunning(null)
    setRunAll(false)
    toast.success('All benchmarks complete')
  }

  const totalRows = Object.values(tables).reduce((a, b) => a + b, 0)
  const completedCount = queries.filter(q => results.has(q.id)).length
  const avgMs = completedCount > 0
    ? Math.round([...results.values()].reduce((a, r) => a + r.duration_ms, 0) / completedCount)
    : 0

  const hasTpchTables = bootstrap?.registered_tables.some(t => t.startsWith('pg_tpch_')) ?? false

  return (
    <div className="flex flex-col h-full animate-fade-in">
      {/* Header */}
      <div className="px-6 py-4 border-b border-white/[0.04]">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
              <Timer className="w-4.5 h-4.5 text-amber-400" />
            </div>
            <div>
              <h1 className="text-base font-display font-bold text-zinc-100">TPC-H Benchmarks</h1>
              <p className="text-2xs text-zinc-500">
                {scaleFactor} &middot; {totalRows.toLocaleString()} total rows across {Object.keys(tables).length} tables
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="ghost" size="sm" icon={<RefreshCw className="w-3.5 h-3.5" />} onClick={fetchData}>Refresh</Button>
            <Button
              variant="primary"
              size="sm"
              icon={runAll ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Zap className="w-3.5 h-3.5" />}
              onClick={handleRunAll}
              disabled={runAll || !hasTpchTables}
            >
              Run All
            </Button>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-auto p-6">
        <div className="max-w-6xl mx-auto space-y-6">
          {/* Status banner if no TPC-H tables */}
          {!hasTpchTables && (
            <div className="p-4 rounded-xl border border-amber-400/20 bg-amber-400/5">
              <div className="flex items-center gap-3">
                <AlertCircle className="w-5 h-5 text-amber-400 flex-shrink-0" />
                <div>
                  <p className="text-xs font-semibold text-amber-400">TPC-H tables not registered</p>
                  <p className="text-2xs text-zinc-400 mt-0.5">
                    Run <code className="font-mono bg-white/[0.06] px-1 rounded">docker exec -i rustlake-postgres psql -U rustlake -d rustlake_demo &lt; docker/postgres/tpch-init.sql</code> then restart the API server or click Re-bootstrap on the About page.
                  </p>
                </div>
              </div>
            </div>
          )}

          {/* Summary stats */}
          <div className="grid grid-cols-4 gap-4">
            {[
              { label: 'Scale Factor', value: scaleFactor || 'SF0.01', icon: Database, color: 'text-cyan-400' },
              { label: 'Total Rows', value: totalRows.toLocaleString(), icon: Layers, color: 'text-amber-400' },
              { label: 'Queries Run', value: `${completedCount}/${queries.length}`, icon: CheckCircle2, color: 'text-emerald-400' },
              { label: 'Avg Duration', value: avgMs > 0 ? `${avgMs}ms` : '--', icon: Clock, color: 'text-violet-400' },
            ].map(s => (
              <Card key={s.label} padding="sm">
                <div className="flex items-center gap-2 mb-1">
                  <s.icon className={cn('w-4 h-4', s.color)} />
                  <span className="text-2xs text-zinc-500">{s.label}</span>
                </div>
                <p className="text-lg font-mono font-bold text-zinc-100">{s.value}</p>
              </Card>
            ))}
          </div>

          {/* Table sizes */}
          <Card padding="md">
            <h3 className="text-xs font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
              <Database className="w-3.5 h-3.5 text-cyan-400" /> TPC-H Tables
            </h3>
            <div className="grid grid-cols-4 gap-2">
              {Object.entries(tables).sort((a, b) => b[1] - a[1]).map(([name, count]) => (
                <div key={name} className="flex items-center justify-between p-2 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                  <span className="text-xs font-mono text-zinc-300">{name}</span>
                  <span className="text-xs font-mono text-zinc-500">{count.toLocaleString()}</span>
                </div>
              ))}
            </div>
          </Card>

          {/* Benchmark queries */}
          <div className="space-y-3">
            <h3 className="text-xs font-display font-semibold text-zinc-200 flex items-center gap-2">
              <BarChart3 className="w-3.5 h-3.5 text-amber-400" /> Benchmark Queries
            </h3>
            {queries.map(q => {
              const result = results.get(q.id)
              const isRunning = running === q.id
              return (
                <Card key={q.id} padding="md" className="group">
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <h4 className="text-sm font-display font-semibold text-zinc-200">{q.name}</h4>
                        <Badge className={CATEGORY_COLORS[q.category] || 'bg-zinc-800 text-zinc-400 border-zinc-700'}>
                          {q.category}
                        </Badge>
                        {result && (
                          <Badge className={result.status === 'success'
                            ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20'
                            : 'bg-red-400/10 text-red-400 border-red-400/20'
                          }>
                            {result.duration_ms}ms
                          </Badge>
                        )}
                      </div>
                      <p className="text-2xs text-zinc-500 mb-2">{q.description}</p>
                      <details className="group/sql">
                        <summary className="text-2xs text-zinc-600 cursor-pointer hover:text-zinc-400 transition-colors">
                          Show SQL
                        </summary>
                        <pre className="mt-2 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04] text-2xs font-mono text-zinc-400 whitespace-pre-wrap overflow-x-auto max-h-40">
                          {q.sql}
                        </pre>
                      </details>
                    </div>
                    <div className="flex items-center gap-2 flex-shrink-0">
                      {result && (
                        <span className="text-2xs text-zinc-500">{result.row_count} rows</span>
                      )}
                      <Button
                        variant="ghost"
                        size="sm"
                        icon={isRunning ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
                        onClick={() => handleRun(q.id)}
                        disabled={isRunning || runAll || !hasTpchTables}
                      >
                        {isRunning ? 'Running' : 'Run'}
                      </Button>
                    </div>
                  </div>
                </Card>
              )
            })}
          </div>

          {/* Performance bar chart */}
          {completedCount > 0 && (
            <Card padding="md">
              <h3 className="text-xs font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                <BarChart3 className="w-3.5 h-3.5 text-amber-400" /> Performance Comparison
              </h3>
              <div className="space-y-2">
                {queries.filter(q => results.has(q.id)).sort((a, b) => {
                  const ra = results.get(a.id)!
                  const rb = results.get(b.id)!
                  return rb.duration_ms - ra.duration_ms
                }).map(q => {
                  const r = results.get(q.id)!
                  const maxMs = Math.max(...[...results.values()].map(r => r.duration_ms), 1)
                  const pct = (r.duration_ms / maxMs) * 100
                  return (
                    <div key={q.id} className="flex items-center gap-3">
                      <span className="text-2xs font-mono text-zinc-400 w-16 text-right flex-shrink-0">{q.id.replace('tpch-', '')}</span>
                      <div className="flex-1 h-5 bg-white/[0.02] rounded-full overflow-hidden border border-white/[0.04]">
                        <div
                          className={cn(
                            'h-full rounded-full transition-all duration-700',
                            r.duration_ms < 50 ? 'bg-emerald-400/60' :
                            r.duration_ms < 200 ? 'bg-amber-400/60' :
                            'bg-rose-400/60'
                          )}
                          style={{ width: `${Math.max(pct, 2)}%` }}
                        />
                      </div>
                      <span className="text-2xs font-mono text-zinc-300 w-16 flex-shrink-0">{r.duration_ms}ms</span>
                    </div>
                  )
                })}
              </div>
            </Card>
          )}

          {/* Last run result table */}
          {lastRun && lastRun.rows.length > 0 && (
            <Card padding="md">
              <h3 className="text-xs font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
                <Layers className="w-3.5 h-3.5 text-cyan-400" /> Last Result: {lastRun.query_name}
                <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20 text-[10px]">
                  {lastRun.duration_ms}ms &middot; {lastRun.row_count} rows
                </Badge>
              </h3>
              <div className="max-h-64 overflow-auto">
                <DataTable columns={lastRun.columns} rows={lastRun.rows} />
              </div>
            </Card>
          )}
        </div>
      </div>
    </div>
  )
}
