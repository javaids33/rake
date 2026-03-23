import { useState, useEffect, useMemo, useCallback } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { Drawer } from '../components/ui/Drawer'
import { Input } from '../components/ui/Input'
import { Textarea } from '../components/ui/Input'
import { Select } from '../components/ui/Input'
import { EmptyState } from '../components/ui/EmptyState'
import { StatusDot } from '../components/ui/StatusDot'
import { Tooltip } from '../components/ui/Tooltip'
import { cn, formatNumber, formatDuration } from '../lib/utils'
import { useDraftForm } from '../hooks/useDraftForm'
import { useServerEvents } from '../components/layout/Shell'
import { getStreamStatus, getStreamEvents, ingestStream, getPipelines, createPipeline, deletePipeline, getConnections, getS3Configs, startPipeline, stopPipeline } from '../api/client'
import type { StreamingMetrics, StreamEvent, StreamingPipeline, ConnectionEntry, S3Config } from '../types'
import {
  Radio, Activity, Zap, Clock, Plus, Play, Trash2, ArrowRightLeft,
  Gauge, Waves, Server, GitMerge, Database, ArrowDown, ArrowRight,
  Shield, RefreshCw, Settings, Eye, AlertTriangle,
  Search, ChevronDown, ChevronRight, Copy, MoreVertical, CheckCircle2,
  XCircle, Beaker, FolderOpen, HardDrive, Camera, Square, Terminal,
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

// Event type color mapping
const EVENT_TYPE_COLORS: Record<string, string> = {
  purchase: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20',
  page_view: 'bg-blue-400/10 text-blue-400 border-blue-400/20',
  signup: 'bg-violet-400/10 text-violet-400 border-violet-400/20',
  login: 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20',
  logout: 'bg-zinc-400/10 text-zinc-400 border-zinc-400/20',
  click: 'bg-amber-400/10 text-amber-400 border-amber-400/20',
  error: 'bg-rose-400/10 text-rose-400 border-rose-400/20',
  search: 'bg-indigo-400/10 text-indigo-400 border-indigo-400/20',
}

type SinkType = 'iceberg' | 's3_new' | 's3_existing'

interface PipelineForm {
  name: string
  source_type: string
  sink_table: string
  transform_sql: string
  broker: string
  topic: string
  connection_id: string
  collection: string
  full_document: string
  sink_type: SinkType
  s3_config_name: string
  s3_path: string
  s3_existing_table: string
  initial_snapshot: boolean
  // Kafka-specific
  group_id: string
  offset_reset: string
  schema_registry_url: string
  format: string
}

const EMPTY_FORM: PipelineForm = {
  name: '', source_type: 'kafka', sink_table: '', transform_sql: '',
  broker: '', topic: '', connection_id: '', collection: '', full_document: 'updateLookup',
  sink_type: 'iceberg', s3_config_name: '', s3_path: '', s3_existing_table: '',
  initial_snapshot: false,
  group_id: 'rustlake-consumer', offset_reset: 'earliest', schema_registry_url: '', format: 'json',
}

export function Streaming() {
  const location = useLocation()
  const navigate = useNavigate()
  const [tab, setTab] = useState('overview')
  const [metrics, setMetrics] = useState<StreamingMetrics | null>(null)
  const [events, setEvents] = useState<StreamEvent[]>([])
  const [pipelines, setPipelines] = useState<StreamingPipeline[]>([])
  const [createOpen, setCreateOpen] = useState(false)
  const [selectedPipeline, setSelectedPipeline] = useState<string | null>(null)
  const [form, setForm, { clearDraft, hasDraft }] = useDraftForm<PipelineForm>('draft:pipeline', EMPTY_FORM)

  // Connections and S3 configs for auto-populate
  const [allConnections, setAllConnections] = useState<ConnectionEntry[]>([])
  const [s3Configs, setS3Configs] = useState<S3Config[]>([])

  // Event tab state
  const [expandedEvent, setExpandedEvent] = useState<number | null>(null)
  const [eventSearch, setEventSearch] = useState('')
  const [eventTypeFilter, setEventTypeFilter] = useState<string>('all')
  const [showDevTools, setShowDevTools] = useState(false)

  // Pipeline action menu
  const [pipelineMenu, setPipelineMenu] = useState<string | null>(null)

  // Derived connection lists
  const mongoConnections = useMemo(() => allConnections.filter(c => c.conn_type === 'mongodb' || c.conn_type === 'cdc_mongodb'), [allConnections])
  const pgConnections = useMemo(() => allConnections.filter(c => c.conn_type === 'postgres' || c.conn_type === 'postgresql'), [allConnections])

  // Collections from selected connection
  const selectedConnTables = useMemo(() => {
    if (!form.connection_id) return []
    const conn = allConnections.find(c => c.id === form.connection_id)
    return conn?.tables || []
  }, [form.connection_id, allConnections])

  // S3 tables from selected S3 config
  const selectedS3Tables = useMemo(() => {
    if (!form.s3_config_name) return []
    const cfg = s3Configs.find(c => c.name === form.s3_config_name)
    return cfg?.tables || []
  }, [form.s3_config_name, s3Configs])

  // Auto-open create modal when navigated with SQL from SQL Editor
  useEffect(() => {
    const state = location.state as { sql?: string; name?: string } | null
    if (state?.sql) {
      setForm(f => ({ ...f, transform_sql: state.sql!, name: state.name || '' }))
      setTab('overview')
      setCreateOpen(true)
      window.history.replaceState({}, '')
    }
  }, [location.state])

  const loadAll = useCallback(() => {
    getStreamStatus().then(r => { if (r?.metrics) setMetrics(r.metrics) }).catch(() => {})
    getStreamEvents(100).then(r => setEvents(r.events || [])).catch(() => {})
    getPipelines().then(r => setPipelines(r.pipelines || [])).catch(() => {})
    getConnections().then(r => setAllConnections(r.connections || [])).catch(() => {})
    getS3Configs().then(r => setS3Configs(r.configs || [])).catch(() => {})
  }, [])
  useEffect(loadAll, [loadAll])

  // Real-time pipeline event updates via SSE
  const { onPipelineEvent } = useServerEvents()
  const [pipelinePhases, setPipelinePhases] = useState<Record<string, string>>({})
  useEffect(() => {
    const unsub = onPipelineEvent((event) => {
      setPipelines(prev => prev.map(p => {
        if (p.id !== event.pipeline_id) return p
        return { ...p, events_processed: event.events_processed, status: event.status }
      }))
      if (event.phase) {
        setPipelinePhases(prev => ({ ...prev, [event.pipeline_id]: event.phase! }))
      }
    })
    return unsub
  }, [onPipelineEvent])

  // Auto-select first connection when source type changes
  useEffect(() => {
    if (form.source_type === 'mongodb-cdc' && mongoConnections.length > 0 && !form.connection_id) {
      const first = mongoConnections[0]
      setForm(f => ({ ...f, connection_id: first.id, name: f.name || `${first.database}-cdc` }))
    }
    if (form.source_type === 'postgres-cdc' && pgConnections.length > 0 && !form.connection_id) {
      const first = pgConnections[0]
      setForm(f => ({
        ...f,
        connection_id: first.id,
        broker: `${first.host}:${first.port}`,
        name: f.name || `${first.database}-cdc`,
      }))
    }
  }, [form.source_type, mongoConnections, pgConnections, form.connection_id])

  // Auto-populate S3 config when switching to S3 sink
  useEffect(() => {
    if ((form.sink_type === 's3_new' || form.sink_type === 's3_existing') && s3Configs.length > 0 && !form.s3_config_name) {
      setForm(f => ({ ...f, s3_config_name: s3Configs[0].name }))
    }
  }, [form.sink_type, s3Configs, form.s3_config_name])

  const handleIngest = async () => {
    try {
      const res = await ingestStream(500)
      toast.success(`Generated ${res.events_generated} events`)
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  const resolveSinkTable = (): string => {
    if (form.sink_type === 'iceberg') return form.sink_table
    if (form.sink_type === 's3_existing') return form.s3_existing_table || form.sink_table
    if (form.sink_type === 's3_new') {
      // Try to find the S3 config to build the full path
      const cfg = s3Configs.find(c => c.name === form.s3_config_name)
      if (cfg && form.s3_path) return `s3://${cfg.bucket}/${form.s3_path}`
      // Fallback: if the config can't be found by name, try by bucket
      const cfgByBucket = s3Configs.find(c => form.s3_config_name.includes(c.bucket) || c.bucket.includes(form.s3_config_name))
      if (cfgByBucket && form.s3_path) return `s3://${cfgByBucket.bucket}/${form.s3_path}`
      // Last resort: if s3_path looks like a full path already, use it
      if (form.s3_path && form.s3_path.startsWith('s3://')) return form.s3_path
      if (form.s3_path) return `s3://${form.s3_config_name}/${form.s3_path}`
      return form.sink_table
    }
    return form.sink_table
  }

  const handleCreate = async () => {
    if (!form.name.trim()) { toast.error('Pipeline name is required'); return }
    const sinkTable = resolveSinkTable()
    if (!sinkTable.trim()) {
      toast.error(`Sink target is required (type=${form.sink_type}, config=${form.s3_config_name}, path=${form.s3_path}, configs=${s3Configs.length})`)
      return
    }
    if (form.source_type === 'kafka') {
      if (!form.broker.trim()) { toast.error('Broker address is required for Kafka'); return }
      if (!form.topic.trim()) { toast.error('Topic is required'); return }
    }
    if (form.source_type === 'mongodb-cdc') {
      if (!form.connection_id && !form.broker.trim()) { toast.error('Select a MongoDB connection or enter a URI'); return }
      if (!form.collection.trim()) { toast.error('Collection name is required'); return }
    }
    if (form.source_type === 'postgres-cdc' && !form.connection_id && !form.broker.trim()) { toast.error('Select a Postgres connection or enter host'); return }
    try {
      let sourceConfig: Record<string, unknown>
      if (form.source_type === 'mongodb-cdc') {
        const selectedConn = allConnections.find(c => c.id === form.connection_id)
        sourceConfig = {
          connection_id: form.connection_id || undefined,
          database: selectedConn?.database || form.broker.split('/').pop() || '',
          collection: form.collection,
          full_document: form.full_document,
          initial_snapshot: form.initial_snapshot,
          ...(form.broker ? { broker: form.broker } : {}),
        }
      } else if (form.source_type === 'postgres-cdc') {
        const selectedConn = allConnections.find(c => c.id === form.connection_id)
        sourceConfig = {
          connection_id: form.connection_id || undefined,
          broker: selectedConn ? `${selectedConn.host}:${selectedConn.port}` : form.broker,
          topic: form.topic || form.collection,
          database: selectedConn?.database || '',
          initial_snapshot: form.initial_snapshot,
        }
      } else {
        sourceConfig = {
          brokers: form.broker,
          topic: form.topic,
          group_id: form.group_id || 'rustlake-consumer',
          offset_reset: form.offset_reset || 'earliest',
          format: form.format || 'json',
          ...(form.schema_registry_url ? { schema_registry_url: form.schema_registry_url } : {}),
        }
      }
      await createPipeline({
        name: form.name,
        source_type: form.source_type,
        source_config: sourceConfig,
        transform_sql: form.transform_sql || undefined,
        sink_table: sinkTable,
      })
      toast.success('Pipeline created')
      setCreateOpen(false)
      clearDraft()
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  const handleStartPipeline = async (id: string) => {
    try {
      await startPipeline(id)
      toast.success('Pipeline started')
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  const handleStopPipeline = async (id: string) => {
    try {
      await stopPipeline(id)
      toast.success('Pipeline stopped')
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  // Unique event types for filter
  const eventTypes = useMemo(() => [...new Set(events.map(e => e.event_type))].sort(), [events])

  // Filtered events
  const filteredEvents = useMemo(() => {
    let result = events
    if (eventSearch) {
      const q = eventSearch.toLowerCase()
      result = result.filter(e =>
        e.event_type.toLowerCase().includes(q) ||
        e.user_id.toLowerCase().includes(q) ||
        JSON.stringify(e.data || {}).toLowerCase().includes(q)
      )
    }
    if (eventTypeFilter !== 'all') {
      result = result.filter(e => e.event_type === eventTypeFilter)
    }
    return result
  }, [events, eventSearch, eventTypeFilter])

  // Connector statuses based on pipelines
  const connectorStatuses = useMemo(() => {
    const statuses: Record<string, { count: number; running: number; events: number }> = {}
    for (const p of pipelines) {
      const key = p.source_type
      if (!statuses[key]) statuses[key] = { count: 0, running: 0, events: 0 }
      statuses[key].count++
      if (p.status === 'running') statuses[key].running++
      statuses[key].events += p.events_processed
    }
    return statuses
  }, [pipelines])

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
            {showDevTools && (
              <Button variant="secondary" size="sm" icon={<Play className="w-3.5 h-3.5" />} onClick={handleIngest}>Simulate Events</Button>
            )}
            <Tooltip content="Toggle developer tools" position="bottom">
              <button
                onClick={() => setShowDevTools(!showDevTools)}
                className={cn('p-2 rounded-lg border transition-colors', showDevTools ? 'bg-amber-400/10 border-amber-400/20 text-amber-400' : 'bg-white/[0.03] border-white/[0.06] text-zinc-600 hover:text-zinc-400')}
              >
                <Beaker className="w-3.5 h-3.5" />
              </button>
            </Tooltip>
            <Button variant="primary" size="sm" icon={<Plus className="w-3.5 h-3.5" />} onClick={() => setCreateOpen(true)}>New Pipeline</Button>
          </div>
        </div>

        {/* Metrics strip */}
        {metrics && (
          <div className="grid grid-cols-5 gap-px bg-white/[0.02] border-b border-white/[0.04]">
            {[
              { label: 'Events Ingested', value: formatNumber(metrics.events_ingested || 0), icon: Activity, color: 'text-emerald-400' },
              { label: 'Throughput', value: `${(metrics.events_per_sec ?? 0).toFixed(0)}/s`, icon: Zap, color: 'text-amber-400' },
              { label: 'Avg Latency', value: metrics.avg_latency_ms != null ? formatDuration(metrics.avg_latency_ms) : '-', icon: Clock, color: 'text-cyan-400' },
              { label: 'Active Streams', value: String(metrics.active_streams ?? 0), icon: Waves, color: 'text-violet-400' },
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
          {/* ─── Pipelines Tab ─── */}
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
                const isRunning = p.status === 'running'
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
                          <StatusDot status={p.status === 'running' ? 'healthy' : p.status === 'error' ? 'error' : 'idle'} label={p.status} />
                        </div>
                        <div className="flex items-center gap-2 mt-1">
                          <Badge className={cfg.bgColor}>{cfg.label}</Badge>
                          <ArrowRight className="w-3 h-3 text-zinc-600" />
                          <Badge className="bg-amber-400/10 text-amber-400 border-amber-400/20">{p.sink_table}</Badge>
                          <span className="text-2xs font-mono text-zinc-600">{formatNumber(p.events_processed)} events</span>
                          {pipelinePhases[p.id] === 'snapshot' && (
                            <Badge className="bg-violet-400/10 text-violet-400 border-violet-400/20 text-2xs">Snapshotting</Badge>
                          )}
                          {pipelinePhases[p.id] === 'streaming' && p.status === 'running' && (
                            <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20 text-2xs">Live CDC</Badge>
                          )}
                        </div>
                      </div>
                      <div className="flex items-center gap-1 flex-shrink-0" onClick={e => e.stopPropagation()}>
                        {/* Start / Stop button */}
                        {isRunning ? (
                          <Tooltip content="Stop pipeline" position="left">
                            <button
                              onClick={() => handleStopPipeline(p.id)}
                              className="p-1.5 rounded-lg hover:bg-rose-400/10 transition-colors"
                            >
                              <Square className="w-4 h-4 text-rose-400" />
                            </button>
                          </Tooltip>
                        ) : (
                          <Tooltip content="Start pipeline" position="left">
                            <button
                              onClick={() => handleStartPipeline(p.id)}
                              className="p-1.5 rounded-lg hover:bg-emerald-400/10 transition-colors"
                            >
                              <Play className="w-4 h-4 text-emerald-400" />
                            </button>
                          </Tooltip>
                        )}
                        <div className="relative">
                          <button
                            onClick={() => setPipelineMenu(pipelineMenu === p.id ? null : p.id)}
                            className="p-1.5 rounded-lg hover:bg-white/[0.06] transition-colors"
                          >
                            <MoreVertical className="w-4 h-4 text-zinc-600" />
                          </button>
                          {pipelineMenu === p.id && (
                            <div className="absolute right-0 top-8 z-50 w-36 bg-navy-900 border border-white/[0.08] rounded-lg shadow-xl py-1 animate-fade-in">
                              <button onClick={() => { setSelectedPipeline(p.id); setPipelineMenu(null) }}
                                className="w-full text-left px-3 py-1.5 text-xs text-zinc-300 hover:bg-white/[0.04] flex items-center gap-2">
                                <Eye className="w-3 h-3 text-zinc-400" /> View Details
                              </button>
                              <div className="border-t border-white/[0.04] my-1" />
                              <button onClick={async () => {
                                await deletePipeline(p.id)
                                setPipelines(ps => ps.filter(x => x.id !== p.id))
                                setPipelineMenu(null)
                                toast.success('Pipeline deleted')
                              }}
                                className="w-full text-left px-3 py-1.5 text-xs text-rose-400 hover:bg-rose-400/[0.06] flex items-center gap-2">
                                <Trash2 className="w-3 h-3" /> Delete
                              </button>
                            </div>
                          )}
                        </div>
                      </div>
                    </div>
                    {/* Expanded detail */}
                    {selectedPipeline === p.id && (
                      <div className="mt-4 pt-4 border-t border-white/[0.04]">
                        {/* Quick actions row */}
                        <div className="flex items-center gap-2 mb-4">
                          <Tooltip content={`SELECT * FROM ${p.name.replace(/-/g, '_').replace(/ /g, '_')} LIMIT 100`} position="bottom">
                            <button
                              onClick={(e) => { e.stopPropagation(); navigate('/sql', { state: { sql: `-- CDC sink table: ${p.sink_table}\nSELECT * FROM ${p.name.replace(/-/g, '_').replace(/ /g, '_')} LIMIT 100;` } }) }}
                              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-amber-400/10 border border-amber-400/20 text-amber-400 text-2xs font-medium hover:bg-amber-400/15 transition-colors"
                            >
                              <Terminal className="w-3 h-3" /> Query Sink
                            </button>
                          </Tooltip>
                          <Tooltip content="View in catalog" position="bottom">
                            <button
                              onClick={(e) => { e.stopPropagation(); navigate('/catalog') }}
                              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-cyan-400/10 border border-cyan-400/20 text-cyan-400 text-2xs font-medium hover:bg-cyan-400/15 transition-colors"
                            >
                              <Database className="w-3 h-3" /> Catalog
                            </button>
                          </Tooltip>
                          <Tooltip content="Schedule as recurring job" position="bottom">
                            <button
                              onClick={(e) => { e.stopPropagation(); navigate('/scheduler', { state: { name: p.name, jobType: 'pipeline' } }) }}
                              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-violet-400/10 border border-violet-400/20 text-violet-400 text-2xs font-medium hover:bg-violet-400/15 transition-colors"
                            >
                              <Clock className="w-3 h-3" /> Schedule
                            </button>
                          </Tooltip>
                          <Tooltip content="Promote to Glacier — adds quality gates, versioning, lineage tracking, and compliance auditing" position="bottom">
                            <button
                              onClick={async (e) => {
                                e.stopPropagation()
                                try {
                                  const res = await fetch('/api/v1/executable-tables/from-pipeline', {
                                    method: 'POST',
                                    headers: { 'Content-Type': 'application/json' },
                                    body: JSON.stringify({ pipeline_id: p.id }),
                                  })
                                  if (res.ok) {
                                    const data = await res.json()
                                    toast.success(`Glacier "${data.glacier}" created from pipeline`)
                                    navigate('/glaciers')
                                  } else {
                                    const err = await res.json()
                                    toast.error(err.error || 'Failed to create Glacier')
                                  }
                                } catch (err) { toast.error((err as Error).message) }
                              }}
                              className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-cyan-400/10 border border-cyan-400/20 text-cyan-300 text-2xs font-medium hover:bg-cyan-400/15 transition-colors"
                            >
                              <Zap className="w-3 h-3" /> Promote to Glacier
                            </button>
                          </Tooltip>
                          {!!(p.source_config as Record<string, unknown>)?.connection_id && (
                            <Tooltip content="View source connection" position="bottom">
                              <button
                                onClick={(e) => { e.stopPropagation(); navigate('/sources') }}
                                className="flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg bg-white/[0.04] border border-white/[0.06] text-zinc-400 text-2xs font-medium hover:bg-white/[0.06] transition-colors"
                              >
                                <Server className="w-3 h-3" /> Source
                              </button>
                            </Tooltip>
                          )}
                        </div>

                        <div className="grid grid-cols-3 gap-4">
                          <div>
                            <p className="text-2xs text-zinc-500 mb-1 font-semibold">Source Config</p>
                            <div className="space-y-1 text-2xs font-mono text-zinc-400">
                              {Object.entries(p.source_config || {}).map(([k, v]) => (
                                <div key={k}><span className="text-zinc-600">{k}:</span> {String(v)}</div>
                              ))}
                              {Object.keys(p.source_config || {}).length === 0 && <span className="text-zinc-600">No config</span>}
                            </div>
                          </div>
                          <div>
                            <p className="text-2xs text-zinc-500 mb-1 font-semibold">Transform SQL</p>
                            <pre className="text-2xs font-mono text-zinc-400 bg-white/[0.02] rounded p-2 overflow-x-auto">
                              {p.transform_sql || 'None (passthrough)'}
                            </pre>
                          </div>
                          <div>
                            <p className="text-2xs text-zinc-500 mb-1 font-semibold">Pipeline Status</p>
                            <div className="space-y-1.5 text-2xs">
                              <div className="flex justify-between"><span className="text-zinc-500">Status</span><StatusDot status={p.status === 'running' ? 'healthy' : 'idle'} label={p.status} /></div>
                              <div className="flex justify-between"><span className="text-zinc-500">Phase</span><span className={cn('font-mono', (p as any).phase === 'snapshot' ? 'text-violet-400' : 'text-emerald-400')}>{(p as any).phase || 'idle'}</span></div>
                              <div className="flex justify-between"><span className="text-zinc-500">Events</span><span className="font-mono text-zinc-300">{formatNumber(p.events_processed)}</span></div>
                              {(p as any).snapshot_docs != null && (
                                <div className="flex justify-between"><span className="text-zinc-500">Snapshot</span><span className="font-mono text-violet-400">{formatNumber((p as any).snapshot_docs)} docs</span></div>
                              )}
                              {(p as any).snapshot_completed_at && (
                                <div className="flex justify-between"><span className="text-zinc-500">Snapshot At</span><span className="text-zinc-400">{new Date((p as any).snapshot_completed_at).toLocaleString()}</span></div>
                              )}
                              {(p as any).files_written > 0 && (
                                <div className="flex justify-between"><span className="text-zinc-500">S3 Files</span><span className="font-mono text-amber-400">{(p as any).files_written} parquet</span></div>
                              )}
                              <div className="flex justify-between"><span className="text-zinc-500">Sink</span><span className="font-mono text-zinc-400 truncate max-w-[200px]">{p.sink_table}</span></div>
                              <div className="flex justify-between"><span className="text-zinc-500">Created</span><span className="text-zinc-400">{p.created_at ? new Date(p.created_at).toLocaleDateString() : '-'}</span></div>
                            </div>
                          </div>
                        </div>
                      </div>
                    )}
                  </button>
                )
              })}
            </div>
          )}

          {/* ─── Live Events Tab ─── */}
          {tab === 'events' && (
            <div className="space-y-3">
              {/* Search + filter bar */}
              <div className="flex items-center gap-2">
                <div className="relative flex-1 min-w-[200px]">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-600" />
                  <input
                    type="text"
                    value={eventSearch}
                    onChange={e => setEventSearch(e.target.value)}
                    placeholder="Search events by type, user, or payload..."
                    className="w-full pl-9 pr-3 py-2 text-xs bg-white/[0.03] border border-white/[0.06] rounded-lg text-zinc-300 placeholder-zinc-600 outline-none focus:border-cyan-400/30 transition-colors"
                  />
                </div>
                <select
                  value={eventTypeFilter}
                  onChange={e => setEventTypeFilter(e.target.value)}
                  className="text-xs bg-white/[0.03] border border-white/[0.06] rounded-lg px-3 py-2 text-zinc-400 outline-none focus:border-cyan-400/30 cursor-pointer"
                >
                  <option value="all">All Types ({events.length})</option>
                  {eventTypes.map(t => (
                    <option key={t} value={t}>{t} ({events.filter(e => e.event_type === t).length})</option>
                  ))}
                </select>
                {(eventSearch || eventTypeFilter !== 'all') && (
                  <button onClick={() => { setEventSearch(''); setEventTypeFilter('all') }}
                    className="text-2xs text-zinc-500 hover:text-zinc-300 transition-colors px-2 py-2">
                    Clear
                  </button>
                )}
                <span className="text-2xs text-zinc-600 ml-auto">
                  {filteredEvents.length === events.length ? `${events.length} events` : `${filteredEvents.length} of ${events.length}`}
                </span>
              </div>

              {/* Event list */}
              <div className="rounded-xl border border-white/[0.04] overflow-hidden">
                {filteredEvents.length === 0 ? (
                  events.length === 0 ? (
                    <EmptyState icon={<Waves className="w-5 h-5" />} title="No events yet" description={showDevTools ? 'Click Simulate Events to generate data' : 'Enable developer tools to simulate events'} />
                  ) : (
                    <div className="text-center py-8">
                      <Search className="w-5 h-5 text-zinc-700 mx-auto mb-2" />
                      <p className="text-xs text-zinc-600">No events match your filter</p>
                    </div>
                  )
                ) : (
                  <div className="divide-y divide-white/[0.03] max-h-[600px] overflow-y-auto">
                    {filteredEvents.map((e, i) => {
                      const isExpanded = expandedEvent === i
                      const colorClass = EVENT_TYPE_COLORS[e.event_type] || 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20'
                      return (
                        <div key={i}>
                          <div
                            role="button"
                            onClick={() => setExpandedEvent(isExpanded ? null : i)}
                            className="w-full flex items-center gap-3 px-4 py-2.5 hover:bg-white/[0.02] transition-colors text-left cursor-pointer"
                          >
                            {isExpanded
                              ? <ChevronDown className="w-3 h-3 text-cyan-400 flex-shrink-0" />
                              : <ChevronRight className="w-3 h-3 text-zinc-600 flex-shrink-0" />
                            }
                            <Badge className={cn('flex-shrink-0', colorClass)}>{e.event_type}</Badge>
                            <span className="text-2xs text-zinc-500 flex-shrink-0 font-mono">{e.user_id}</span>
                            <span className="text-xs font-mono text-zinc-500 flex-1 truncate">{JSON.stringify(e.data || {}).slice(0, 80)}</span>
                            <span className="text-2xs text-zinc-700 flex-shrink-0">{e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '-'}</span>
                          </div>
                          {/* Expanded payload inspector */}
                          {isExpanded && (
                            <div className="px-4 pb-3 bg-white/[0.01] border-t border-white/[0.02]">
                              <div className="grid grid-cols-4 gap-3 py-3 text-2xs border-b border-white/[0.03] mb-3">
                                <div>
                                  <span className="text-zinc-600 block">Event Type</span>
                                  <span className="text-zinc-300 font-mono">{e.event_type}</span>
                                </div>
                                <div>
                                  <span className="text-zinc-600 block">User ID</span>
                                  <span className="text-zinc-300 font-mono">{e.user_id}</span>
                                </div>
                                <div>
                                  <span className="text-zinc-600 block">Timestamp</span>
                                  <span className="text-zinc-300 font-mono">{e.timestamp}</span>
                                </div>
                                <div>
                                  <span className="text-zinc-600 block">Fields</span>
                                  <span className="text-zinc-300 font-mono">{Object.keys(e.data || {}).length} keys</span>
                                </div>
                              </div>
                              <div className="relative">
                                <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/80 rounded-lg p-3 overflow-x-auto border border-white/[0.03] max-h-[200px]">
                                  {JSON.stringify(e.data || {}, null, 2)}
                                </pre>
                                <button
                                  onClick={(ev) => { ev.stopPropagation(); navigator.clipboard.writeText(JSON.stringify(e.data || {}, null, 2)); toast.success('Payload copied') }}
                                  className="absolute top-2 right-2 p-1 rounded text-zinc-600 hover:text-zinc-300 transition-colors"
                                >
                                  <Copy className="w-3 h-3" />
                                </button>
                              </div>
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            </div>
          )}

          {/* ─── Connectors Tab ─── */}
          {tab === 'connectors' && (
            <div className="space-y-6">
              {/* Supported connectors with status */}
              <div>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3">Source Connectors</h3>
                <div className="grid grid-cols-3 gap-3">
                  {Object.entries(SOURCE_TYPE_CONFIG).map(([key, cfg]) => {
                    const status = connectorStatuses[key]
                    const hasActive = status && status.running > 0
                    return (
                      <Card key={key} hover className="cursor-pointer" onClick={() => { setForm(f => ({ ...f, source_type: key })); setCreateOpen(true) }}>
                        <div className="flex items-center gap-3 mb-3">
                          <div className={cn('w-10 h-10 rounded-lg border flex items-center justify-center', cfg.bgColor)}>
                            <cfg.icon className={cn('w-5 h-5', cfg.color)} />
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2">
                              <h4 className="text-sm font-semibold text-zinc-200">{cfg.label}</h4>
                              {status ? (
                                <Badge className={hasActive ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' : 'bg-zinc-400/10 text-zinc-400 border-zinc-400/20'}>
                                  {hasActive ? `${status.running} active` : 'Configured'}
                                </Badge>
                              ) : (
                                <Badge className="bg-zinc-800/50 text-zinc-600 border-zinc-700/30">Not configured</Badge>
                              )}
                            </div>
                            <p className="text-2xs text-zinc-500">{cfg.desc}</p>
                          </div>
                        </div>
                        {status && (
                          <div className="flex items-center gap-3 text-2xs text-zinc-500 mb-2 border-t border-white/[0.03] pt-2">
                            <span>{status.count} pipeline{status.count !== 1 ? 's' : ''}</span>
                            <span className="text-zinc-700">|</span>
                            <span>{formatNumber(status.events)} events processed</span>
                          </div>
                        )}
                        <div className="flex items-center gap-1 text-2xs text-amber-400">
                          <Plus className="w-3 h-3" /> Create pipeline
                        </div>
                      </Card>
                    )
                  })}
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
                    <span className="text-2xs text-zinc-600">Iceberg / S3</span>
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

      {/* ═══════════════ Create pipeline modal ═══════════════ */}
      <Drawer open={createOpen} onClose={() => setCreateOpen(false)} title="Create Streaming Pipeline" subtitle="Configure source, sink, and transform" width="max-w-xl" draftSaved={hasDraft}>
        <div className="space-y-4 max-h-[75vh] overflow-y-auto pr-1">
          <Input label="Pipeline Name" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} placeholder="events-pipeline" />

          {/* ── Source Type ── */}
          <div>
            <span className="text-xs font-medium text-zinc-400 mb-2 block">Source Type</span>
            <div className="grid grid-cols-3 gap-2">
              {Object.entries(SOURCE_TYPE_CONFIG).map(([key, cfg]) => (
                <button
                  key={key}
                  onClick={() => setForm(f => ({ ...f, source_type: key, connection_id: '', broker: '', topic: '', collection: '' }))}
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

          {/* ── Source Config: MongoDB CDC ── */}
          {form.source_type === 'mongodb-cdc' && (
            <>
              {mongoConnections.length > 0 ? (
                <Select
                  label="MongoDB Connection"
                  value={form.connection_id}
                  onChange={e => {
                    const conn = mongoConnections.find(c => c.id === e.target.value)
                    setForm(f => ({
                      ...f,
                      connection_id: e.target.value,
                      collection: '',
                      name: f.name || (conn ? `${conn.database}-cdc` : ''),
                    }))
                  }}
                  options={[
                    { value: '', label: 'Manual URI (below)' },
                    ...mongoConnections.map(c => ({
                      value: c.id,
                      label: `${c.name} — ${c.host}/${c.database} (${c.tables?.length || 0} collections)`,
                    })),
                  ]}
                />
              ) : (
                <div className="flex items-center gap-2 p-3 rounded-lg bg-amber-400/[0.04] border border-amber-400/10">
                  <AlertTriangle className="w-4 h-4 text-amber-400/60 flex-shrink-0" />
                  <p className="text-2xs text-amber-400/60">No MongoDB connections found. Add one in Data Sources first, or enter a URI below.</p>
                </div>
              )}
              {!form.connection_id && (
                <Input label="MongoDB URI *" value={form.broker} onChange={e => setForm(f => ({ ...f, broker: e.target.value }))} placeholder="mongodb://localhost:27017/mydb" />
              )}
              {/* Collection picker — auto-populated from connection */}
              {selectedConnTables.length > 0 ? (
                <Select
                  label="Collection *"
                  value={form.collection}
                  onChange={e => setForm(f => ({ ...f, collection: e.target.value }))}
                  options={[
                    { value: '', label: 'Select a collection...' },
                    { value: '*', label: '* (All collections)' },
                    ...selectedConnTables.map(t => ({ value: t, label: t })),
                  ]}
                />
              ) : (
                <Input label="Collection *" value={form.collection} onChange={e => setForm(f => ({ ...f, collection: e.target.value }))} placeholder="events (or * for all collections)" />
              )}
              <Select
                label="Full Document Mode"
                value={form.full_document}
                onChange={e => setForm(f => ({ ...f, full_document: e.target.value }))}
                options={[
                  { value: 'updateLookup', label: 'updateLookup (recommended)' },
                  { value: 'default', label: 'default' },
                  { value: 'whenAvailable', label: 'whenAvailable' },
                ]}
              />
              {/* Initial snapshot toggle */}
              <label className="flex items-center gap-3 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04] cursor-pointer hover:bg-white/[0.03] transition-colors">
                <input
                  type="checkbox"
                  checked={form.initial_snapshot}
                  onChange={e => setForm(f => ({ ...f, initial_snapshot: e.target.checked }))}
                  className="w-4 h-4 rounded border-zinc-600 bg-white/[0.05] accent-amber-400"
                />
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <Camera className="w-3.5 h-3.5 text-amber-400/70" />
                    <span className="text-xs font-medium text-zinc-200">Initial Snapshot</span>
                  </div>
                  <p className="text-2xs text-zinc-500 mt-0.5 ml-5.5">Copy all existing documents before starting change stream. Ensures no data is missed.</p>
                </div>
              </label>
              <div className="flex items-start gap-2 p-3 rounded-lg bg-emerald-400/[0.04] border border-emerald-400/10">
                <Database className="w-4 h-4 text-emerald-400/60 flex-shrink-0 mt-0.5" />
                <p className="text-2xs text-emerald-400/60 leading-relaxed">
                  MongoDB CDC uses change streams to capture inserts, updates, and deletes in real-time. The &quot;updateLookup&quot; mode includes the full document for update events.
                </p>
              </div>
            </>
          )}

          {/* ── Source Config: Postgres CDC ── */}
          {form.source_type === 'postgres-cdc' && (
            <>
              {pgConnections.length > 0 ? (
                <Select
                  label="Postgres Connection"
                  value={form.connection_id}
                  onChange={e => {
                    const conn = pgConnections.find(c => c.id === e.target.value)
                    setForm(f => ({
                      ...f,
                      connection_id: e.target.value,
                      broker: conn ? `${conn.host}:${conn.port}` : f.broker,
                      topic: '',
                      name: f.name || (conn ? `${conn.database}-cdc` : ''),
                    }))
                  }}
                  options={[
                    { value: '', label: 'Manual host (below)' },
                    ...pgConnections.map(c => ({
                      value: c.id,
                      label: `${c.name} — ${c.host}:${c.port}/${c.database} (${c.tables?.length || 0} tables)`,
                    })),
                  ]}
                />
              ) : (
                <div className="flex items-center gap-2 p-3 rounded-lg bg-amber-400/[0.04] border border-amber-400/10">
                  <AlertTriangle className="w-4 h-4 text-amber-400/60 flex-shrink-0" />
                  <p className="text-2xs text-amber-400/60">No Postgres connections found. Add one in Data Sources first, or enter host below.</p>
                </div>
              )}
              {!form.connection_id && (
                <Input label="Host:Port *" value={form.broker} onChange={e => setForm(f => ({ ...f, broker: e.target.value }))} placeholder="localhost:5432" />
              )}
              {/* Table picker — auto-populated from connection */}
              {selectedConnTables.length > 0 ? (
                <Select
                  label="Table / Slot *"
                  value={form.topic}
                  onChange={e => setForm(f => ({ ...f, topic: e.target.value }))}
                  options={[
                    { value: '', label: 'Select a table...' },
                    ...selectedConnTables.map(t => ({ value: t, label: t })),
                  ]}
                />
              ) : (
                <Input label="Table / Slot *" value={form.topic} onChange={e => setForm(f => ({ ...f, topic: e.target.value }))} placeholder="public.users" />
              )}
              {/* Initial snapshot toggle */}
              <label className="flex items-center gap-3 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04] cursor-pointer hover:bg-white/[0.03] transition-colors">
                <input
                  type="checkbox"
                  checked={form.initial_snapshot}
                  onChange={e => setForm(f => ({ ...f, initial_snapshot: e.target.checked }))}
                  className="w-4 h-4 rounded border-zinc-600 bg-white/[0.05] accent-amber-400"
                />
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <Camera className="w-3.5 h-3.5 text-amber-400/70" />
                    <span className="text-xs font-medium text-zinc-200">Initial Snapshot</span>
                  </div>
                  <p className="text-2xs text-zinc-500 mt-0.5 ml-5.5">Copy all existing rows before starting logical replication. Ensures complete data.</p>
                </div>
              </label>
            </>
          )}

          {/* ── Source Config: Kafka ── */}
          {form.source_type === 'kafka' && (
            <>
              <Input label="Broker Address *" value={form.broker} onChange={e => setForm(f => ({ ...f, broker: e.target.value }))} placeholder="localhost:9092" />
              <Input label="Topic *" value={form.topic} onChange={e => setForm(f => ({ ...f, topic: e.target.value }))} placeholder="events" />
              <div className="grid grid-cols-2 gap-3">
                <Input label="Consumer Group" value={form.group_id} onChange={e => setForm(f => ({ ...f, group_id: e.target.value }))} placeholder="rustlake-consumer" />
                <Select
                  label="Offset Reset"
                  value={form.offset_reset}
                  onChange={e => setForm(f => ({ ...f, offset_reset: e.target.value }))}
                  options={[{ value: 'earliest', label: 'Earliest' }, { value: 'latest', label: 'Latest' }]}
                />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <Select
                  label="Message Format"
                  value={form.format}
                  onChange={e => setForm(f => ({ ...f, format: e.target.value }))}
                  options={[
                    { value: 'json', label: 'JSON' },
                    { value: 'avro', label: 'Avro (Schema Registry)' },
                    { value: 'string', label: 'Raw String' },
                  ]}
                />
                <Input
                  label="Schema Registry URL"
                  value={form.schema_registry_url}
                  onChange={e => setForm(f => ({ ...f, schema_registry_url: e.target.value }))}
                  placeholder="http://localhost:8081"
                  disabled={form.format !== 'avro'}
                />
              </div>
            </>
          )}

          {/* ── Sink Target ── */}
          <div>
            <span className="text-xs font-medium text-zinc-400 mb-2 block">Sink Target</span>
            <div className="grid grid-cols-3 gap-2">
              {([
                { key: 'iceberg' as SinkType, label: 'Iceberg Table', icon: Database, color: 'text-amber-400', desc: 'Append to Iceberg table' },
                { key: 's3_existing' as SinkType, label: 'S3 Existing Path', icon: FolderOpen, color: 'text-cyan-400', desc: 'Write to existing S3 location' },
                { key: 's3_new' as SinkType, label: 'S3 New Path', icon: HardDrive, color: 'text-violet-400', desc: 'Create new path in S3 bucket' },
              ]).map(sink => (
                <button
                  key={sink.key}
                  onClick={() => setForm(f => ({ ...f, sink_type: sink.key }))}
                  className={cn(
                    'p-3 rounded-lg border text-left transition-all',
                    form.sink_type === sink.key
                      ? 'bg-white/[0.06] border-amber-400/30'
                      : 'bg-white/[0.02] border-white/[0.04] hover:border-white/[0.08]'
                  )}
                >
                  <sink.icon className={cn('w-4 h-4 mb-1', sink.color)} />
                  <p className="text-xs font-medium text-zinc-200">{sink.label}</p>
                  <p className="text-2xs text-zinc-600 mt-0.5">{sink.desc}</p>
                </button>
              ))}
            </div>
          </div>

          {/* Sink-specific fields */}
          {form.sink_type === 'iceberg' && (
            <Input
              label="Sink Table (Iceberg) *"
              value={form.sink_table}
              onChange={e => setForm(f => ({ ...f, sink_table: e.target.value }))}
              placeholder="iceberg://warehouse.events"
            />
          )}

          {form.sink_type === 's3_existing' && (
            <>
              {s3Configs.length > 0 ? (
                <>
                  <Select
                    label="S3 Bucket"
                    value={form.s3_config_name}
                    onChange={e => setForm(f => ({ ...f, s3_config_name: e.target.value, s3_existing_table: '' }))}
                    options={s3Configs.map(c => ({
                      value: c.name,
                      label: `${c.name} — s3://${c.bucket} (${c.tables?.length || 0} tables)`,
                    }))}
                  />
                  {selectedS3Tables.length > 0 ? (
                    <Select
                      label="Existing Table / Path *"
                      value={form.s3_existing_table}
                      onChange={e => setForm(f => ({ ...f, s3_existing_table: e.target.value }))}
                      options={[
                        { value: '', label: 'Select a table...' },
                        ...selectedS3Tables.map(t => ({ value: t, label: t })),
                      ]}
                    />
                  ) : (
                    <div className="text-2xs text-zinc-500 p-3 bg-white/[0.02] rounded-lg border border-white/[0.04]">
                      No tables discovered in this bucket yet. Tables appear after S3 scan completes.
                    </div>
                  )}
                </>
              ) : (
                <div className="flex items-center gap-2 p-3 rounded-lg bg-amber-400/[0.04] border border-amber-400/10">
                  <AlertTriangle className="w-4 h-4 text-amber-400/60 flex-shrink-0" />
                  <p className="text-2xs text-amber-400/60">No S3 buckets connected. Add one in Data Sources first.</p>
                </div>
              )}
            </>
          )}

          {form.sink_type === 's3_new' && (
            <>
              {s3Configs.length > 0 ? (
                <>
                  <Select
                    label="S3 Bucket"
                    value={form.s3_config_name}
                    onChange={e => setForm(f => ({ ...f, s3_config_name: e.target.value }))}
                    options={s3Configs.map(c => ({
                      value: c.name,
                      label: `${c.name} — s3://${c.bucket}`,
                    }))}
                  />
                  <Input
                    label="Path in Bucket *"
                    value={form.s3_path}
                    onChange={e => setForm(f => ({ ...f, s3_path: e.target.value }))}
                    placeholder="warehouse/cdc/events"
                  />
                  {form.s3_config_name && form.s3_path && (
                    <div className="text-2xs font-mono text-zinc-400 p-2 bg-white/[0.02] rounded border border-white/[0.04]">
                      <span className="text-zinc-600">Sink:</span>{' '}
                      {resolveSinkTable() || `s3://${form.s3_config_name}/${form.s3_path}`}
                    </div>
                  )}
                </>
              ) : (
                <div className="flex items-center gap-2 p-3 rounded-lg bg-amber-400/[0.04] border border-amber-400/10">
                  <AlertTriangle className="w-4 h-4 text-amber-400/60 flex-shrink-0" />
                  <p className="text-2xs text-amber-400/60">No S3 buckets connected. Add one in Data Sources first.</p>
                </div>
              )}
            </>
          )}

          {/* Transform SQL */}
          <Textarea
            label="Transform SQL (optional)"
            value={form.transform_sql}
            onChange={e => setForm(f => ({ ...f, transform_sql: e.target.value }))}
            placeholder="SELECT event_type, data, timestamp FROM source WHERE event_type != 'heartbeat'"
          />

          {/* Action buttons */}
          <div className="flex justify-end gap-2 pt-4 border-t border-white/[0.06] mt-4">
            {hasDraft && (
              <Button variant="secondary" size="sm" onClick={clearDraft}>Clear Draft</Button>
            )}
            <div className="flex-1" />
            <Button variant="secondary" size="sm" onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleCreate}>Create Pipeline</Button>
          </div>
        </div>
      </Drawer>
    </div>
  )
}
