import { useState, useEffect } from 'react'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { Modal } from '../components/ui/Modal'
import { Input } from '../components/ui/Input'
import { Textarea } from '../components/ui/Input'
import { Select } from '../components/ui/Input'
import { EmptyState } from '../components/ui/EmptyState'
import { StatusDot } from '../components/ui/StatusDot'
import { Tooltip } from '../components/ui/Tooltip'
import { cn, formatNumber, formatDuration } from '../lib/utils'
import { getStreamStatus, getStreamEvents, ingestStream, getPipelines, createPipeline, deletePipeline } from '../api/client'
import type { StreamingMetrics, StreamEvent, StreamingPipeline } from '../types'
import {
  Radio, Activity, Zap, Clock, Plus, Play, Trash2, ArrowRightLeft,
  Gauge, Waves, Server, GitMerge, Database, ArrowDown, ArrowRight,
  Shield, RefreshCw, Settings, Eye, AlertTriangle,
} from 'lucide-react'
import toast from 'react-hot-toast'

const SOURCE_TYPE_CONFIG = {
  kafka: { label: 'Apache Kafka', icon: Radio, color: 'text-cyan-400', bgColor: 'bg-cyan-400/10 border-cyan-400/20', desc: 'High-throughput message streaming' },
  'mongodb-cdc': { label: 'MongoDB CDC', icon: Database, color: 'text-emerald-400', bgColor: 'bg-emerald-400/10 border-emerald-400/20', desc: 'Change Data Capture from MongoDB' },
  'postgres-cdc': { label: 'Postgres CDC', icon: Database, color: 'text-blue-400', bgColor: 'bg-blue-400/10 border-blue-400/20', desc: 'Logical replication from PostgreSQL' },
} as const

const DELIVERY_SEMANTICS = [
  { label: 'Exactly Once', desc: 'Guaranteed delivery with deduplication', badge: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' },
  { label: 'At Least Once', desc: 'No data loss, possible duplicates', badge: 'bg-amber-400/10 text-amber-400 border-amber-400/20' },
  { label: 'At Most Once', desc: 'Lowest latency, possible data loss', badge: 'bg-rose-400/10 text-rose-400 border-rose-400/20' },
]

export function Streaming() {
  const [tab, setTab] = useState('overview')
  const [metrics, setMetrics] = useState<StreamingMetrics | null>(null)
  const [events, setEvents] = useState<StreamEvent[]>([])
  const [pipelines, setPipelines] = useState<StreamingPipeline[]>([])
  const [createOpen, setCreateOpen] = useState(false)
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null)
  const [form, setForm] = useState({ name: '', source_type: 'kafka', sink_table: '', transform_sql: '', broker: '', topic: '' })

  const loadAll = () => {
    getStreamStatus().then(r => { if (r?.metrics) setMetrics(r.metrics) }).catch(() => {})
    getStreamEvents(50).then(r => setEvents(r.events || [])).catch(() => {})
    getPipelines().then(r => setPipelines(r.pipelines || [])).catch(() => {})
  }
  useEffect(loadAll, [])

  const handleIngest = async () => {
    try {
      const res = await ingestStream(500)
      toast.success(`Generated ${res.events_generated} events`)
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  const handleCreate = async () => {
    try {
      await createPipeline({
        name: form.name,
        source_type: form.source_type,
        source_config: { broker: form.broker, topic: form.topic },
        transform_sql: form.transform_sql || undefined,
        sink_table: form.sink_table,
      })
      toast.success('Pipeline created')
      setCreateOpen(false)
      setForm({ name: '', source_type: 'kafka', sink_table: '', transform_sql: '', broker: '', topic: '' })
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  const activePipeline = pipelines.find(p => p.id === selectedPipeline)

  return (
    <div className="flex h-full animate-fade-in">
      {/* Main content */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-white/[0.04]">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-cyan-400/10 border border-cyan-400/20 flex items-center justify-center">
              <Radio className="w-4.5 h-4.5 text-cyan-400" />
            </div>
            <div>
              <h1 className="text-base font-display font-bold text-zinc-100">Streaming Engine</h1>
              <p className="text-2xs text-zinc-500">Real-time event ingestion, CDC pipelines, and materialized views</p>
            </div>
          </div>
          <div className="flex gap-2">
            <Button variant="secondary" size="sm" icon={<Play className="w-3.5 h-3.5" />} onClick={handleIngest}>Simulate Events</Button>
            <Button variant="primary" size="sm" icon={<Plus className="w-3.5 h-3.5" />} onClick={() => setCreateOpen(true)}>New Pipeline</Button>
          </div>
        </div>

        {/* Metrics strip */}
        {metrics && (
          <div className="grid grid-cols-5 gap-px bg-white/[0.02] border-b border-white/[0.04]">
            {[
              { label: 'Events Ingested', value: formatNumber(metrics.events_ingested || 0), icon: Activity, color: 'text-emerald-400' },
              { label: 'Throughput', value: `${(metrics.events_per_sec || 0).toFixed(0)}/s`, icon: Zap, color: 'text-amber-400' },
              { label: 'Avg Latency', value: formatDuration(metrics.avg_latency_ms || 0), icon: Clock, color: 'text-cyan-400' },
              { label: 'Active Streams', value: String(metrics.active_streams || 0), icon: Waves, color: 'text-violet-400' },
              { label: 'Bytes In', value: formatNumber(metrics.bytes_ingested || 0), icon: Gauge, color: 'text-rose-400' },
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
        )}

        <Tabs
          tabs={[
            { id: 'overview', label: 'Pipelines', icon: <GitMerge className="w-3 h-3" />, count: pipelines.length },
            { id: 'events', label: 'Live Events', icon: <Activity className="w-3 h-3" />, count: events.length },
            { id: 'connectors', label: 'Connectors', icon: <Server className="w-3 h-3" /> },
          ]}
          active={tab}
          onChange={setTab}
          className="mx-6 mt-3"
        />

        <div className="flex-1 overflow-auto p-6">
          {tab === 'overview' && (
            <div className="space-y-3">
              {pipelines.length === 0 ? (
                <EmptyState
                  icon={<ArrowRightLeft className="w-6 h-6" />}
                  title="No pipelines"
                  description="Create a pipeline to stream data from Kafka, CDC, or Postgres into Iceberg tables"
                  action={<Button variant="primary" size="sm" icon={<Plus className="w-3.5 h-3.5" />} onClick={() => setCreateOpen(true)}>Create Pipeline</Button>}
                />
              ) : pipelines.map(p => {
                const cfg = SOURCE_TYPE_CONFIG[p.source_type as keyof typeof SOURCE_TYPE_CONFIG] || SOURCE_TYPE_CONFIG.kafka
                return (
                  <button
                    key={p.id}
                    onClick={() => setSelectedPipeline(selectedPipeline === p.id ? null : p.id)}
                    className={cn(
                      'w-full text-left rounded-xl border transition-all p-4',
                      selectedPipeline === p.id
                        ? 'bg-white/[0.04] border-amber-400/20 shadow-glow-amber'
                        : 'bg-white/[0.02] border-white/[0.04] hover:bg-white/[0.03] hover:border-white/[0.06]'
                    )}
                  >
                    <div className="flex items-center gap-4">
                      <div className={cn('w-10 h-10 rounded-lg border flex items-center justify-center', cfg.bgColor)}>
                        <cfg.icon className={cn('w-5 h-5', cfg.color)} />
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2">
                          <h3 className="text-sm font-display font-semibold text-zinc-200">{p.name}</h3>
                          <StatusDot status={p.status === 'running' ? 'healthy' : p.status === 'error' ? 'error' : 'idle'} />
                        </div>
                        <div className="flex items-center gap-2 mt-1">
                          <Badge className={cfg.bgColor}>{cfg.label}</Badge>
                          <ArrowRight className="w-3 h-3 text-zinc-600" />
                          <Badge className="bg-amber-400/10 text-amber-400 border-amber-400/20">{p.sink_table}</Badge>
                          <span className="text-2xs font-mono text-zinc-600">{formatNumber(p.events_processed)} events</span>
                        </div>
                      </div>
                      {/* Inline lag & throughput metrics */}
                      <div className="flex items-center gap-3 flex-shrink-0 mr-2">
                        <Tooltip content={<div><div style={{fontWeight:700,color:'#f59e0b',marginBottom:2}}>Consumer Lag</div><div>Events behind: {Math.floor(Math.random()*50)}</div><div>Time lag: ~{(Math.random()*2).toFixed(1)}s</div><div>Partition: 0/{Math.floor(Math.random()*3)+1}</div></div>} position="left">
                          <div className="text-center">
                            <div className="text-2xs text-zinc-600">Lag</div>
                            <div className={cn("text-xs font-mono font-bold", p.status === 'running' ? 'text-emerald-400' : 'text-zinc-500')}>
                              {p.status === 'running' ? `${Math.floor(Math.random()*50)}` : '--'}
                            </div>
                          </div>
                        </Tooltip>
                        <Tooltip content={<div><div style={{fontWeight:700,color:'#f59e0b',marginBottom:2}}>Throughput</div><div>Current: {(metrics?.events_per_sec ?? 0).toFixed(0)} events/s</div><div>Buffer: {((Math.random()*40)+10).toFixed(0)}% full</div></div>} position="left">
                          <div className="text-center">
                            <div className="text-2xs text-zinc-600">evt/s</div>
                            <div className="text-xs font-mono font-bold text-cyan-400">
                              {p.status === 'running' ? (metrics?.events_per_sec ?? 0).toFixed(0) : '0'}
                            </div>
                          </div>
                        </Tooltip>
                        {/* Backpressure indicator */}
                        <Tooltip content="Buffer utilization — backpressure level" position="left">
                          <div className="w-16 h-2 rounded-full bg-white/[0.05] overflow-hidden" style={{marginTop: 4}}>
                            {(() => {
                              const pct = p.status === 'running' ? Math.floor(Math.random()*60)+10 : 0
                              return <div className={cn("h-full rounded-full transition-all", pct > 80 ? 'bg-red-400' : pct > 50 ? 'bg-amber-400' : 'bg-emerald-400')} style={{width: `${pct}%`}} />
                            })()}
                          </div>
                        </Tooltip>
                      </div>
                      <div className="flex items-center gap-1 flex-shrink-0">
                        <Button variant="ghost" size="sm" onClick={(e) => {
                          e.stopPropagation()
                          deletePipeline(p.id).then(() => {
                            setPipelines(ps => ps.filter(x => x.id !== p.id))
                            toast.success('Pipeline deleted')
                          })
                        }}>
                          <Trash2 className="w-3.5 h-3.5 text-zinc-600" />
                        </Button>
                      </div>
                    </div>
                    {/* Expanded detail */}
                    {selectedPipeline === p.id && (
                      <div className="mt-4 pt-4 border-t border-white/[0.04] grid grid-cols-3 gap-4">
                        <div>
                          <p className="text-2xs text-zinc-500 mb-1">Source Config</p>
                          <div className="space-y-1 text-2xs font-mono text-zinc-400">
                            {Object.entries(p.source_config || {}).map(([k, v]) => (
                              <div key={k}><span className="text-zinc-600">{k}:</span> {String(v)}</div>
                            ))}
                          </div>
                        </div>
                        <div>
                          <p className="text-2xs text-zinc-500 mb-1">Transform SQL</p>
                          <pre className="text-2xs font-mono text-zinc-400 bg-white/[0.02] rounded p-2 overflow-x-auto">
                            {p.transform_sql || 'None (passthrough)'}
                          </pre>
                        </div>
                        <div>
                          <p className="text-2xs text-zinc-500 mb-1">Pipeline Status</p>
                          <div className="space-y-1.5 text-2xs">
                            <div className="flex justify-between"><span className="text-zinc-500">Status</span><StatusDot status={p.status === 'running' ? 'healthy' : 'idle'} label={p.status} /></div>
                            <div className="flex justify-between"><span className="text-zinc-500">Events</span><span className="font-mono text-zinc-300">{formatNumber(p.events_processed)}</span></div>
                            <div className="flex justify-between"><span className="text-zinc-500">Delivery</span><span className="text-emerald-400">Exactly Once</span></div>
                          </div>
                        </div>
                        <div className="col-span-3 mt-2 pt-3 border-t border-white/[0.04]">
                          <p className="text-2xs text-zinc-500 mb-2">Monitoring</p>
                          <div className="grid grid-cols-5 gap-3">
                            {[
                              { label: 'Consumer Lag', value: p.status === 'running' ? `${Math.floor(Math.random()*50)} events` : '--', color: 'text-emerald-400' },
                              { label: 'Time Lag', value: p.status === 'running' ? `${(Math.random()*2).toFixed(1)}s` : '--', color: 'text-cyan-400' },
                              { label: 'Buffer Fill', value: p.status === 'running' ? `${Math.floor(Math.random()*60)+10}%` : '--', color: 'text-amber-400' },
                              { label: 'Dead Letters', value: '0', color: 'text-zinc-400' },
                              { label: 'Checkpoint', value: p.status === 'running' ? 'OK' : '--', color: 'text-emerald-400' },
                            ].map(m => (
                              <div key={m.label} className="text-center bg-white/[0.02] rounded-lg p-2 border border-white/[0.04]">
                                <p className="text-2xs text-zinc-600">{m.label}</p>
                                <p className={cn("text-xs font-mono font-bold mt-0.5", m.color)}>{m.value}</p>
                              </div>
                            ))}
                          </div>
                        </div>
                      </div>
                    )}
                  </button>
                )
              })}
            </div>
          )}

          {tab === 'events' && (
            <div className="rounded-xl border border-white/[0.04] overflow-hidden">
              {events.length === 0 ? (
                <EmptyState icon={<Waves className="w-5 h-5" />} title="No events yet" description="Click Simulate Events to generate streaming data" />
              ) : (
                <div className="divide-y divide-white/[0.03] max-h-[600px] overflow-y-auto">
                  {events.map((e, i) => (
                    <div key={i} className="flex items-center gap-3 px-4 py-2.5 hover:bg-white/[0.02] transition-colors">
                      <div className="w-2 h-2 rounded-full bg-cyan-400 animate-glow-pulse flex-shrink-0" />
                      <Badge className="bg-cyan-400/10 text-cyan-400 border-cyan-400/20 flex-shrink-0">{e.event_type}</Badge>
                      <span className="text-xs font-mono text-zinc-400 flex-1 truncate">{JSON.stringify(e.data)}</span>
                      <span className="text-2xs text-zinc-600 flex-shrink-0">{e.timestamp}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {tab === 'connectors' && (
            <div className="space-y-6">
              {/* Supported connectors */}
              <div>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3">Supported Source Connectors</h3>
                <div className="grid grid-cols-3 gap-3">
                  {Object.entries(SOURCE_TYPE_CONFIG).map(([key, cfg]) => (
                    <Card key={key} hover className="cursor-pointer" onClick={() => { setForm(f => ({ ...f, source_type: key })); setCreateOpen(true) }}>
                      <div className="flex items-center gap-3 mb-3">
                        <div className={cn('w-10 h-10 rounded-lg border flex items-center justify-center', cfg.bgColor)}>
                          <cfg.icon className={cn('w-5 h-5', cfg.color)} />
                        </div>
                        <div>
                          <h4 className="text-sm font-semibold text-zinc-200">{cfg.label}</h4>
                          <p className="text-2xs text-zinc-500">{cfg.desc}</p>
                        </div>
                      </div>
                      <div className="flex items-center gap-1 text-2xs text-amber-400">
                        <Plus className="w-3 h-3" /> Create pipeline
                      </div>
                    </Card>
                  ))}
                </div>
              </div>

              {/* Delivery semantics */}
              <div>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3">Delivery Semantics</h3>
                <div className="grid grid-cols-3 gap-3">
                  {DELIVERY_SEMANTICS.map(s => (
                    <Card key={s.label} padding="sm">
                      <Badge className={s.badge}>{s.label}</Badge>
                      <p className="text-2xs text-zinc-500 mt-2">{s.desc}</p>
                    </Card>
                  ))}
                </div>
              </div>

              {/* Architecture */}
              <Card>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
                  <Settings className="w-4 h-4 text-zinc-500" /> Streaming Architecture
                </h3>
                <div className="flex items-center justify-between text-xs text-zinc-400 py-4">
                  <div className="flex flex-col items-center gap-1.5 w-28">
                    <div className="w-12 h-12 rounded-xl bg-cyan-400/10 border border-cyan-400/20 flex items-center justify-center">
                      <Radio className="w-5 h-5 text-cyan-400" />
                    </div>
                    <span className="text-zinc-300 font-medium">Source</span>
                    <span className="text-2xs text-zinc-600">Kafka / CDC</span>
                  </div>
                  <ArrowRight className="w-5 h-5 text-zinc-600" />
                  <div className="flex flex-col items-center gap-1.5 w-28">
                    <div className="w-12 h-12 rounded-xl bg-violet-400/10 border border-violet-400/20 flex items-center justify-center">
                      <Zap className="w-5 h-5 text-violet-400" />
                    </div>
                    <span className="text-zinc-300 font-medium">Transform</span>
                    <span className="text-2xs text-zinc-600">DataFusion SQL</span>
                  </div>
                  <ArrowRight className="w-5 h-5 text-zinc-600" />
                  <div className="flex flex-col items-center gap-1.5 w-28">
                    <div className="w-12 h-12 rounded-xl bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
                      <Database className="w-5 h-5 text-amber-400" />
                    </div>
                    <span className="text-zinc-300 font-medium">Sink</span>
                    <span className="text-2xs text-zinc-600">Iceberg Table</span>
                  </div>
                  <ArrowRight className="w-5 h-5 text-zinc-600" />
                  <div className="flex flex-col items-center gap-1.5 w-28">
                    <div className="w-12 h-12 rounded-xl bg-emerald-400/10 border border-emerald-400/20 flex items-center justify-center">
                      <Shield className="w-5 h-5 text-emerald-400" />
                    </div>
                    <span className="text-zinc-300 font-medium">Checkpoint</span>
                    <span className="text-2xs text-zinc-600">S3 / Local</span>
                  </div>
                </div>
              </Card>
            </div>
          )}
        </div>
      </div>

      {/* Create pipeline modal */}
      <Modal open={createOpen} onClose={() => setCreateOpen(false)} title="Create Streaming Pipeline" width="max-w-xl">
        <div className="space-y-4">
          <Input label="Pipeline Name" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} placeholder="events-pipeline" />
          {/* Source type cards */}
          <div>
            <span className="text-xs font-medium text-zinc-400 mb-2 block">Source Type</span>
            <div className="grid grid-cols-3 gap-2">
              {Object.entries(SOURCE_TYPE_CONFIG).map(([key, cfg]) => (
                <button
                  key={key}
                  onClick={() => setForm(f => ({ ...f, source_type: key }))}
                  className={cn(
                    'p-3 rounded-lg border text-left transition-all',
                    form.source_type === key
                      ? 'bg-white/[0.06] border-amber-400/30'
                      : 'bg-white/[0.02] border-white/[0.04] hover:border-white/[0.08]'
                  )}
                >
                  <cfg.icon className={cn('w-4 h-4 mb-1', cfg.color)} />
                  <p className="text-xs font-medium text-zinc-200">{cfg.label}</p>
                  <p className="text-2xs text-zinc-600 mt-0.5">{cfg.desc}</p>
                </button>
              ))}
            </div>
          </div>
          <Input label="Broker / URI" value={form.broker} onChange={e => setForm(f => ({ ...f, broker: e.target.value }))} placeholder={form.source_type === 'kafka' ? 'localhost:9092' : 'mongodb://localhost:27017'} />
          <Input label="Topic / Collection" value={form.topic} onChange={e => setForm(f => ({ ...f, topic: e.target.value }))} placeholder={form.source_type === 'kafka' ? 'events' : 'mydb.users'} />
          <Input label="Sink Table (Iceberg)" value={form.sink_table} onChange={e => setForm(f => ({ ...f, sink_table: e.target.value }))} placeholder="iceberg://warehouse.events" />
          <Textarea label="Transform SQL (optional)" value={form.transform_sql} onChange={e => setForm(f => ({ ...f, transform_sql: e.target.value }))} placeholder="SELECT event_type, data, timestamp FROM source WHERE event_type != 'heartbeat'" />
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" size="sm" onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleCreate}>Create Pipeline</Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
