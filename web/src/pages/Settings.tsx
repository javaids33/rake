import { useState, useEffect } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { StatusDot } from '../components/ui/StatusDot'
import { Tooltip } from '../components/ui/Tooltip'
import { cn } from '../lib/utils'
import { getSystemInfo, getFlightInfo, getClusters } from '../api/client'
import type { SystemInfoResponse, FlightInfoResponse, AlertRule } from '../types'
import {
  Settings as SettingsIcon, Cpu, Activity, Plane,
  Server, Network, Shield, Route, Gauge, Zap,
  Database, ArrowRight, Layers, Bell, Plus,
  Trash2, ChevronDown, ChevronRight, ToggleLeft, ToggleRight,
} from 'lucide-react'

const ROUTING_RULES = [
  { pattern: 'Full table scans, aggregations, joins', engine: 'DataFusion OLAP', color: 'bg-blue-400/10 text-blue-400 border-blue-400/20', icon: Database },
  { pattern: 'WHERE pk = ? point lookups on Lance', engine: 'LanceDB Direct', color: 'bg-rose-400/10 text-rose-400 border-rose-400/20', icon: Zap },
  { pattern: 'INSERT INTO ... FROM kafka_topic()', engine: 'Streaming Engine', color: 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20', icon: Activity },
  { pattern: 'SELECT vector_search(...)', engine: 'Vector Engine', color: 'bg-violet-400/10 text-violet-400 border-violet-400/20', icon: Layers },
  { pattern: 'Multi-engine federated queries', engine: 'Arrow Flight Exchange', color: 'bg-amber-400/10 text-amber-400 border-amber-400/20', icon: Plane },
]

const ARCH_TOOLTIPS: Record<string, string> = {
  'Query Engine': 'Apache DataFusion provides SQL parsing (sqlparser-rs), 30+ optimizer rules, vectorized columnar execution, and Substrait plan support. Powers all SQL workloads.',
  'Data Format': 'Apache Arrow defines the in-memory columnar layout used for zero-copy data exchange between all RustLake crates. RecordBatch is the universal data unit.',
  'Storage': 'object_store crate provides unified I/O across S3, GCS, Azure ADLS, and local filesystem with connection pooling, retry, and bandwidth throttling.',
  'Table Format': 'Apache Iceberg is the primary lakehouse table format — ACID transactions, time travel, schema evolution, partition pruning, snapshot isolation.',
  'Vector Storage': 'Lance format provides 100x faster random access than Parquet, optimized for vector similarity search (IVF-PQ, HNSW indexes) and AI/ML workloads.',
  'HTTP Server': 'Axum 0.8 serves the REST API on :3000. Arrow Flight gRPC on :50051 provides high-performance data transport for BI tools and distributed workers.',
}

const ALERT_TYPES: Array<{ value: AlertRule['type']; label: string }> = [
  { value: 'freshness', label: 'Data Freshness' },
  { value: 'query_duration', label: 'Query Duration' },
  { value: 'error_rate', label: 'Error Rate' },
  { value: 'row_count', label: 'Row Count' },
  { value: 'custom', label: 'Custom SQL' },
]

const CHANNELS = ['console', 'webhook', 'email', 'slack', 'pagerduty']

function loadAlerts(): AlertRule[] {
  try {
    const raw = localStorage.getItem('rustlake_alerts')
    return raw ? JSON.parse(raw) : []
  } catch { return [] }
}
function saveAlerts(alerts: AlertRule[]) {
  localStorage.setItem('rustlake_alerts', JSON.stringify(alerts))
}

export function Settings() {
  const [tab, setTab] = useState('system')
  const [system, setSystem] = useState<SystemInfoResponse | null>(null)
  const [flight, setFlight] = useState<FlightInfoResponse | null>(null)
  const [clusters, setClusters] = useState<Array<{ id: string; name: string; status: string; workers: number }>>([])
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [alerts, setAlerts] = useState<AlertRule[]>(loadAlerts)
  const [showNewAlert, setShowNewAlert] = useState(false)
  const [newAlert, setNewAlert] = useState<Partial<AlertRule>>({
    type: 'freshness', channel: 'console', enabled: true, threshold: 60,
  })

  useEffect(() => {
    getSystemInfo().then(setSystem).catch(() => {})
    getFlightInfo().then(setFlight).catch(() => {})
    getClusters().then(r => setClusters(r.clusters || [])).catch(() => {})
  }, [])

  useEffect(() => { saveAlerts(alerts) }, [alerts])

  const formatUptime = (s: number) => {
    const h = Math.floor(s / 3600)
    const m = Math.floor((s % 3600) / 60)
    return `${h}h ${m}m`
  }

  const addAlert = () => {
    if (!newAlert.name || !newAlert.target || !newAlert.condition) return
    const rule: AlertRule = {
      id: `alert_${Date.now()}`,
      name: newAlert.name || '',
      type: newAlert.type || 'freshness',
      target: newAlert.target || '',
      condition: newAlert.condition || '',
      threshold: newAlert.threshold || 60,
      channel: newAlert.channel || 'console',
      enabled: newAlert.enabled ?? true,
    }
    setAlerts(prev => [...prev, rule])
    setNewAlert({ type: 'freshness', channel: 'console', enabled: true, threshold: 60 })
    setShowNewAlert(false)
  }

  const toggleAlert = (id: string) => {
    setAlerts(prev => prev.map(a => a.id === id ? { ...a, enabled: !a.enabled } : a))
  }

  const deleteAlert = (id: string) => {
    setAlerts(prev => prev.filter(a => a.id !== id))
  }

  const essentialFields = system ? [
    ['Platform', system.platform],
    ['Version', system.version],
    ['Uptime', formatUptime(system.uptime_seconds)],
    ['Queries Executed', String(system.query_count)],
    ['Registered Tables', String(system.registered_tables)],
  ] : []

  const advancedFields = system ? [
    ['Arrow Version', system.arrow_version],
    ['DataFusion Version', system.datafusion_version],
  ] : []

  return (
    <div className="flex flex-col h-full animate-fade-in">
      {/* Header */}
      <div className="px-6 py-4 border-b border-white/[0.04]">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-zinc-400/10 border border-zinc-400/20 flex items-center justify-center">
            <SettingsIcon className="w-4.5 h-4.5 text-zinc-400" />
          </div>
          <div>
            <h1 className="text-base font-display font-bold text-zinc-100">Settings</h1>
            <p className="text-2xs text-zinc-500">System configuration, query routing, cluster topology, and alerts</p>
          </div>
        </div>
      </div>

      <Tabs
        tabs={[
          { id: 'system', label: 'System Info', icon: <Cpu className="w-3 h-3" /> },
          { id: 'router', label: 'Query Router', icon: <Route className="w-3 h-3" /> },
          { id: 'flight', label: 'Flight / Cluster', icon: <Plane className="w-3 h-3" /> },
          { id: 'alerts', label: 'Alerts & SLA', icon: <Bell className="w-3 h-3" /> },
        ]}
        active={tab}
        onChange={setTab}
        className="mx-6 mt-3"
      />

      <div className="flex-1 overflow-auto p-6">
        <div className="max-w-4xl mx-auto space-y-4">
          {tab === 'system' && system && (
            <>
              <Card>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                  <Activity className="w-4 h-4 text-emerald-400" /> Engine Status
                </h3>
                <div className="grid grid-cols-2 gap-y-3 text-xs">
                  {essentialFields.map(([label, value]) => (
                    <div key={label} className="flex items-center gap-3">
                      <span className="text-zinc-500 w-40">{label}</span>
                      <span className="text-zinc-200 font-mono">{value}</span>
                    </div>
                  ))}
                </div>

                {/* Advanced section */}
                <button
                  onClick={() => setShowAdvanced(!showAdvanced)}
                  className="flex items-center gap-1.5 mt-4 text-2xs text-zinc-500 hover:text-zinc-400 transition-colors"
                >
                  {showAdvanced ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
                  Advanced Details
                </button>
                {showAdvanced && (
                  <div className="grid grid-cols-2 gap-y-3 text-xs mt-3 pt-3 border-t border-white/[0.04]">
                    {advancedFields.map(([label, value]) => (
                      <div key={label} className="flex items-center gap-3">
                        <span className="text-zinc-500 w-40">{label}</span>
                        <span className="text-zinc-200 font-mono">{value}</span>
                      </div>
                    ))}
                  </div>
                )}
              </Card>

              <Card>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                  <Shield className="w-4 h-4 text-blue-400" /> Architecture Stack
                </h3>
                <div className="grid grid-cols-3 gap-3">
                  {[
                    { label: 'Query Engine', value: 'Apache DataFusion 51', desc: 'SQL parser, optimizer, executor', color: 'amber' },
                    { label: 'Data Format', value: 'Apache Arrow 57', desc: 'Zero-copy columnar memory', color: 'cyan' },
                    { label: 'Storage', value: 'object_store 0.12', desc: 'S3/GCS/ADLS/Local', color: 'emerald' },
                    { label: 'Table Format', value: 'Apache Iceberg', desc: 'Primary lakehouse format', color: 'blue' },
                    { label: 'Vector Storage', value: 'Lance', desc: '100x faster random access', color: 'rose' },
                    { label: 'HTTP Server', value: 'Axum 0.8', desc: 'REST + Flight SQL gateway', color: 'violet' },
                  ].map(c => (
                    <Tooltip key={c.label} content={ARCH_TOOLTIPS[c.label]} position="top">
                      <div className={cn('p-3 rounded-lg border bg-white/[0.02] border-white/[0.04] hover:bg-white/[0.04] transition-colors cursor-help w-full')}>
                        <p className="text-2xs text-zinc-500">{c.label}</p>
                        <p className="text-xs font-semibold text-zinc-200 font-mono mt-0.5">{c.value}</p>
                        <p className="text-2xs text-zinc-600 mt-0.5">{c.desc}</p>
                      </div>
                    </Tooltip>
                  ))}
                </div>
              </Card>
            </>
          )}

          {tab === 'router' && (
            <Card>
              <h3 className="text-sm font-display font-semibold text-zinc-200 mb-2 flex items-center gap-2">
                <Route className="w-4 h-4 text-amber-400" /> Query Router Rules
              </h3>
              <p className="text-xs text-zinc-500 mb-4">The router inspects incoming SQL AST and dispatches to the optimal execution engine.</p>
              <div className="space-y-2">
                {ROUTING_RULES.map((rule, i) => (
                  <div key={i} className="flex items-center gap-3 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04] hover:bg-white/[0.03] transition-colors">
                    <div className={cn('w-8 h-8 rounded-lg border flex items-center justify-center', rule.color)}>
                      <rule.icon className="w-4 h-4" />
                    </div>
                    <span className="text-xs text-zinc-300 flex-1">{rule.pattern}</span>
                    <ArrowRight className="w-3.5 h-3.5 text-zinc-600" />
                    <Badge className={rule.color}>{rule.engine}</Badge>
                  </div>
                ))}
              </div>
            </Card>
          )}

          {tab === 'flight' && (
            <>
              {flight && (
                <Card>
                  <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                    <Plane className="w-4 h-4 text-blue-400" /> Arrow Flight Server
                    <StatusDot
                      status={flight.status === 'running' ? 'healthy' : flight.status === 'stopped' ? 'error' : 'idle'}
                      label={flight.status}
                      pulse={flight.status === 'running'}
                    />
                  </h3>
                  <div className="grid grid-cols-2 gap-y-3 text-xs">
                    {[
                      ['Protocol', flight.protocol],
                      ['Host', flight.host],
                      ['Port', String(flight.port)],
                      ['Arrow Version', flight.arrow_version],
                      ['Max Message Size', `${(flight.max_message_size / 1024 / 1024).toFixed(0)} MB`],
                      ['Active Clients', String(flight.active_clients)],
                      ['Queries Served', String(flight.queries_served)],
                    ].map(([label, value]) => (
                      <div key={label} className="flex items-center gap-3">
                        <span className="text-zinc-500 w-40">{label}</span>
                        <span className="text-zinc-200 font-mono">{value}</span>
                      </div>
                    ))}
                  </div>
                  {flight.capabilities.length > 0 && (
                    <div className="mt-4">
                      <p className="text-2xs text-zinc-500 mb-2">Capabilities</p>
                      <div className="flex flex-wrap gap-1.5">
                        {flight.capabilities.map(c => <Badge key={c} className="bg-white/[0.04] text-zinc-400 border-white/[0.06]">{c}</Badge>)}
                      </div>
                    </div>
                  )}
                  {flight.supported_clients.length > 0 && (
                    <div className="mt-3">
                      <p className="text-2xs text-zinc-500 mb-2">Supported Clients</p>
                      <div className="flex flex-wrap gap-1.5">
                        {flight.supported_clients.map(c => <Badge key={c} className="bg-blue-400/10 text-blue-400 border-blue-400/20">{c}</Badge>)}
                      </div>
                    </div>
                  )}
                  {flight.status === 'disabled' && (
                    <div className="mt-4 p-3 rounded-lg bg-amber-400/5 border border-amber-400/10">
                      <p className="text-2xs text-amber-400/80">
                        Enable with <code className="font-mono bg-amber-400/10 px-1.5 py-0.5 rounded">RUSTLAKE_FLIGHT__ENABLED=true</code>
                      </p>
                    </div>
                  )}
                </Card>
              )}

              <Card>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                  <Network className="w-4 h-4 text-emerald-400" /> Cluster Topology
                </h3>
                {clusters.length === 0 ? (
                  <div className="space-y-3">
                    <p className="text-xs text-zinc-500">Phase 4 distributed execution is in progress. Current deployment: single-node.</p>
                    <div className="flex items-center gap-4 p-4 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                      <div className="w-12 h-12 rounded-xl bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
                        <Server className="w-6 h-6 text-amber-400" />
                      </div>
                      <div>
                        <p className="text-sm font-display font-semibold text-zinc-200">Coordinator Node</p>
                        <p className="text-2xs text-zinc-500">Query planning + execution on single process</p>
                      </div>
                      <StatusDot status="healthy" label="Running" pulse />
                    </div>
                  </div>
                ) : (
                  <div className="space-y-2">
                    {clusters.map(c => (
                      <div key={c.id} className="flex items-center gap-3 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                        <Server className="w-5 h-5 text-zinc-500" />
                        <div className="flex-1">
                          <p className="text-xs font-semibold text-zinc-200">{c.name}</p>
                          <p className="text-2xs text-zinc-500">{c.workers} workers</p>
                        </div>
                        <StatusDot status={c.status === 'active' ? 'healthy' : 'idle'} label={c.status} />
                      </div>
                    ))}
                  </div>
                )}
              </Card>
            </>
          )}

          {tab === 'alerts' && (
            <>
              <Card>
                <div className="flex items-center justify-between mb-4">
                  <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2">
                    <Bell className="w-4 h-4 text-amber-400" /> Alert Rules
                  </h3>
                  <button
                    onClick={() => setShowNewAlert(!showNewAlert)}
                    className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-amber-400/10 border border-amber-400/20 text-amber-400 text-2xs font-medium hover:bg-amber-400/20 transition-colors"
                  >
                    <Plus className="w-3 h-3" /> Add Rule
                  </button>
                </div>

                {showNewAlert && (
                  <div className="p-4 rounded-lg bg-white/[0.02] border border-amber-400/10 mb-4 space-y-3">
                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <label className="text-2xs text-zinc-500 block mb-1">Rule Name</label>
                        <input
                          className="w-full px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-xs text-zinc-200 outline-none focus:border-amber-400/30"
                          placeholder="e.g. Orders table freshness"
                          value={newAlert.name || ''}
                          onChange={e => setNewAlert(p => ({ ...p, name: e.target.value }))}
                        />
                      </div>
                      <div>
                        <label className="text-2xs text-zinc-500 block mb-1">Type</label>
                        <select
                          className="w-full px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-xs text-zinc-200 outline-none focus:border-amber-400/30"
                          value={newAlert.type}
                          onChange={e => setNewAlert(p => ({ ...p, type: e.target.value as AlertRule['type'] }))}
                        >
                          {ALERT_TYPES.map(t => <option key={t.value} value={t.value}>{t.label}</option>)}
                        </select>
                      </div>
                      <div>
                        <label className="text-2xs text-zinc-500 block mb-1">Target (table or query)</label>
                        <input
                          className="w-full px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-xs text-zinc-200 outline-none focus:border-amber-400/30"
                          placeholder="e.g. orders, SELECT COUNT(*) ..."
                          value={newAlert.target || ''}
                          onChange={e => setNewAlert(p => ({ ...p, target: e.target.value }))}
                        />
                      </div>
                      <div>
                        <label className="text-2xs text-zinc-500 block mb-1">Condition</label>
                        <input
                          className="w-full px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-xs text-zinc-200 outline-none focus:border-amber-400/30"
                          placeholder="e.g. last_updated > 1h, duration > 5s"
                          value={newAlert.condition || ''}
                          onChange={e => setNewAlert(p => ({ ...p, condition: e.target.value }))}
                        />
                      </div>
                      <div>
                        <label className="text-2xs text-zinc-500 block mb-1">Threshold</label>
                        <input
                          type="number"
                          className="w-full px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-xs text-zinc-200 outline-none focus:border-amber-400/30"
                          value={newAlert.threshold || 60}
                          onChange={e => setNewAlert(p => ({ ...p, threshold: Number(e.target.value) }))}
                        />
                      </div>
                      <div>
                        <label className="text-2xs text-zinc-500 block mb-1">Notification Channel</label>
                        <select
                          className="w-full px-3 py-1.5 rounded-lg bg-white/[0.03] border border-white/[0.06] text-xs text-zinc-200 outline-none focus:border-amber-400/30"
                          value={newAlert.channel}
                          onChange={e => setNewAlert(p => ({ ...p, channel: e.target.value }))}
                        >
                          {CHANNELS.map(c => <option key={c} value={c}>{c}</option>)}
                        </select>
                      </div>
                    </div>
                    <div className="flex justify-end gap-2 pt-1">
                      <button
                        onClick={() => setShowNewAlert(false)}
                        className="px-3 py-1.5 rounded-lg text-2xs text-zinc-400 hover:text-zinc-300 transition-colors"
                      >
                        Cancel
                      </button>
                      <button
                        onClick={addAlert}
                        disabled={!newAlert.name || !newAlert.target || !newAlert.condition}
                        className="px-4 py-1.5 rounded-lg bg-amber-400/10 border border-amber-400/20 text-amber-400 text-2xs font-medium hover:bg-amber-400/20 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                      >
                        Create Rule
                      </button>
                    </div>
                  </div>
                )}

                {alerts.length === 0 && !showNewAlert ? (
                  <div className="text-center py-8">
                    <Bell className="w-8 h-8 text-zinc-700 mx-auto mb-2" />
                    <p className="text-xs text-zinc-500">No alert rules configured</p>
                    <p className="text-2xs text-zinc-600 mt-1">Create rules to monitor data freshness, query performance, and error rates</p>
                  </div>
                ) : (
                  <div className="space-y-2">
                    {alerts.map(alert => (
                      <div key={alert.id} className={cn(
                        'flex items-center gap-3 p-3 rounded-lg border transition-colors',
                        alert.enabled
                          ? 'bg-white/[0.02] border-white/[0.04]'
                          : 'bg-white/[0.01] border-white/[0.02] opacity-60'
                      )}>
                        <button onClick={() => toggleAlert(alert.id)} className="text-zinc-400 hover:text-amber-400 transition-colors">
                          {alert.enabled
                            ? <ToggleRight className="w-5 h-5 text-amber-400" />
                            : <ToggleLeft className="w-5 h-5" />
                          }
                        </button>
                        <div className="flex-1 min-w-0">
                          <p className="text-xs font-semibold text-zinc-200 truncate">{alert.name}</p>
                          <p className="text-2xs text-zinc-500 truncate">
                            {alert.type} on <span className="font-mono text-zinc-400">{alert.target}</span> — {alert.condition}
                          </p>
                        </div>
                        <Badge className="bg-white/[0.04] text-zinc-400 border-white/[0.06]">{alert.channel}</Badge>
                        <Tooltip content={`Threshold: ${alert.threshold}`} position="left">
                          <span className="text-2xs font-mono text-zinc-500">{alert.threshold}</span>
                        </Tooltip>
                        <button onClick={() => deleteAlert(alert.id)} className="text-zinc-600 hover:text-red-400 transition-colors">
                          <Trash2 className="w-3.5 h-3.5" />
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </Card>

              <Card>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                  <Gauge className="w-4 h-4 text-cyan-400" /> SLA Thresholds
                </h3>
                <div className="space-y-3">
                  {[
                    { label: 'Query P95 Latency', value: '< 5 seconds', desc: 'Interactive queries should complete within 5s at the 95th percentile', status: 'healthy' as const },
                    { label: 'Data Freshness', value: '< 1 hour', desc: 'Tables sourced from streaming pipelines should have data no older than 1 hour', status: 'healthy' as const },
                    { label: 'Uptime SLA', value: '99.9%', desc: 'Platform availability target for production workloads', status: 'healthy' as const },
                    { label: 'Scheduler Success Rate', value: '> 95%', desc: 'Scheduled jobs should succeed at least 95% of the time', status: 'healthy' as const },
                  ].map(sla => (
                    <div key={sla.label} className="flex items-center gap-3 p-3 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                      <StatusDot status={sla.status} />
                      <div className="flex-1">
                        <p className="text-xs font-semibold text-zinc-200">{sla.label}</p>
                        <p className="text-2xs text-zinc-500">{sla.desc}</p>
                      </div>
                      <Badge className="bg-emerald-400/10 text-emerald-400 border-emerald-400/20">{sla.value}</Badge>
                    </div>
                  ))}
                </div>
              </Card>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
