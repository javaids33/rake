import { useState, useEffect, useMemo, useRef } from 'react'
import { useLocation } from 'react-router-dom'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { Modal } from '../components/ui/Modal'
import { Input, Select, Textarea } from '../components/ui/Input'
import { EmptyState } from '../components/ui/EmptyState'
import { StatusDot } from '../components/ui/StatusDot'
import { Tooltip } from '../components/ui/Tooltip'
import { cn, formatDuration, formatRelativeTime } from '../lib/utils'
import { getSchedules, createSchedule, updateSchedule, deleteSchedule, runSchedule, getScheduleRuns, getSchedulerDag } from '../api/client'
import { useEditorStore } from '../stores/editor'
import type { ScheduledJob, JobRun, DagNode, DagEdge } from '../types'
import {
  Clock, Plus, Play, Trash2, Calendar, ToggleRight, ToggleLeft,
  Timer, History, CheckCircle2, XCircle, AlertCircle, Zap, BookOpen,
  Settings, Repeat, ArrowRight, Pause, RefreshCw,
  Database, HardDrive, Layers, Eye, Snowflake, Wrench,
  GitBranch, BarChart3, Shield, Copy, FileText, Network,
  Search, MoreVertical, ChevronUp, ChevronDown, ChevronsUpDown,
  ChevronLeft, ChevronRight,
} from 'lucide-react'
import toast from 'react-hot-toast'

// ─────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────
const PAGE_SIZE = 25

function cronToHuman(cron: string): string {
  const map: Record<string, string> = {
    '* * * * *': 'Every minute',
    '*/5 * * * *': 'Every 5 min',
    '*/15 * * * *': 'Every 15 min',
    '0 * * * *': 'Hourly',
    '0 */6 * * *': 'Every 6h',
    '0 0 * * *': 'Daily midnight',
    '0 6 * * *': 'Daily 6 AM',
    '0 1 * * *': 'Daily 1 AM',
    '0 2 * * *': 'Daily 2 AM',
    '0 3 * * *': 'Daily 3 AM',
    '0 4 * * *': 'Daily 4 AM',
    '0 5 * * *': 'Daily 5 AM',
    '0 0 * * 1': 'Weekly Mon',
    '0 4 * * 1': 'Weekly Mon 4 AM',
    '0 0 * * 0': 'Weekly Sun',
    '0 3 * * 0': 'Weekly Sun 3 AM',
    '0 5 * * 0': 'Weekly Sun 5 AM',
    '0 0 1 * *': 'Monthly 1st',
    '0 0 1 */3 *': 'Quarterly',
    '30 1 * * *': 'Daily 1:30 AM',
    '*/30 * * * *': 'Every 30 min',
  }
  if (map[cron]) return map[cron]
  // Fallback patterns
  if (/^every \d+/.test(cron)) return cron.charAt(0).toUpperCase() + cron.slice(1)
  return cron
}

type SortField = 'name' | 'type' | 'status' | 'last_run'

// ─────────────────────────────────────────────────
// Job types — ETL, materialized views, transforms
// ─────────────────────────────────────────────────
const JOB_TYPE_CONFIG: Record<string, { color: string; icon: typeof Timer; desc: string; label: string }> = {
  etl_pipeline: { color: 'bg-amber-400/10 text-amber-400 border-amber-400/20', icon: ArrowRight, label: 'ETL Pipeline', desc: 'Extract from source → transform → load into Iceberg table' },
  materialized_view: { color: 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20', icon: Eye, label: 'Materialized View', desc: 'SQL-defined view refreshed on schedule, stored as Iceberg' },
  sql_query: { color: 'bg-blue-400/10 text-blue-400 border-blue-400/20', icon: Zap, label: 'SQL Query', desc: 'Execute ad-hoc or recurring SQL on schedule' },
  transform_run: { color: 'bg-violet-400/10 text-violet-400 border-violet-400/20', icon: RefreshCw, label: 'dbt Transform', desc: 'Run dbt-compatible model transformations' },
  pipeline: { color: 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20', icon: GitBranch, label: 'Stream Pipeline', desc: 'Trigger or restart a streaming CDC pipeline' },
  compaction: { color: 'bg-rose-400/10 text-rose-400 border-rose-400/20', icon: Layers, label: 'Table Compaction', desc: 'Optimize Iceberg/Delta small files, expire snapshots' },
  data_quality: { color: 'bg-sky-400/10 text-sky-400 border-sky-400/20', icon: Shield, label: 'Data Quality', desc: 'Run freshness, null %, row count, uniqueness checks' },
  snapshot: { color: 'bg-indigo-400/10 text-indigo-400 border-indigo-400/20', icon: Snowflake, label: 'Snapshot / Backup', desc: 'Iceberg snapshot export or table backup to cold storage' },
}

const CRON_PRESETS = [
  { label: 'Every minute', value: '* * * * *', desc: 'Continuous' },
  { label: 'Every 5 min', value: '*/5 * * * *', desc: 'High freq' },
  { label: 'Every 15 min', value: '*/15 * * * *', desc: 'Near real-time' },
  { label: 'Hourly', value: '0 * * * *', desc: 'On the hour' },
  { label: 'Every 6 hours', value: '0 */6 * * *', desc: '4x daily' },
  { label: 'Daily midnight', value: '0 0 * * *', desc: 'Nightly batch' },
  { label: 'Daily 6 AM', value: '0 6 * * *', desc: 'Morning batch' },
  { label: 'Weekly Mon', value: '0 0 * * 1', desc: 'Weekly' },
  { label: 'Monthly 1st', value: '0 0 1 * *', desc: 'Monthly' },
  { label: 'Quarterly', value: '0 0 1 */3 *', desc: 'Every 3 months' },
]

// ─────────────────────────────────────────────────
// ETL & Materialized View Templates
// ─────────────────────────────────────────────────
const TEMPLATES = [
  {
    category: 'ETL Pipelines',
    icon: ArrowRight,
    color: 'text-amber-400',
    items: [
      { name: 'Postgres → Iceberg (Full Sync)', type: 'etl_pipeline', cron: '0 0 * * *', target: 'CREATE TABLE iceberg.warehouse.customers AS SELECT * FROM pg.customers', desc: 'Full nightly sync from Postgres into Iceberg table', tags: 'etl,postgres,iceberg' },
      { name: 'Postgres → Iceberg (Incremental)', type: 'etl_pipeline', cron: '0 * * * *', target: 'INSERT INTO iceberg.warehouse.orders SELECT * FROM pg.orders WHERE updated_at > NOW() - INTERVAL \'1 hour\'', desc: 'Hourly incremental load using updated_at watermark', tags: 'etl,incremental,iceberg' },
      { name: 'MySQL → Iceberg (Full Sync)', type: 'etl_pipeline', cron: '0 2 * * *', target: 'CREATE TABLE iceberg.warehouse.products AS SELECT * FROM mysql.products', desc: 'Daily sync from MySQL to Iceberg at 2 AM', tags: 'etl,mysql,iceberg' },
      { name: 'S3 CSV → Iceberg (Auto Loader)', type: 'etl_pipeline', cron: '*/15 * * * *', target: 'INSERT INTO iceberg.landing.events SELECT * FROM \'s3://data-lake/incoming/*.csv\'', desc: 'Auto-load new CSV files from S3 every 15 min', tags: 'etl,s3,auto-loader' },
      { name: 'MongoDB → Iceberg (Documents)', type: 'etl_pipeline', cron: '0 3 * * *', target: 'CREATE TABLE iceberg.warehouse.user_profiles AS SELECT * FROM mongo.users', desc: 'Flatten MongoDB documents into Iceberg at 3 AM', tags: 'etl,mongodb,iceberg' },
      { name: 'Cross-Source Join → Iceberg', type: 'etl_pipeline', cron: '0 1 * * *', target: 'CREATE TABLE iceberg.analytics.enriched_orders AS SELECT o.*, c.name, c.email FROM pg.orders o JOIN mongo.customers c ON o.customer_id = c.id', desc: 'Join Postgres orders with MongoDB customers nightly', tags: 'etl,cross-source,iceberg' },
    ]
  },
  {
    category: 'Materialized Views',
    icon: Eye,
    color: 'text-cyan-400',
    items: [
      { name: 'Hourly Revenue Summary', type: 'materialized_view', cron: '0 * * * *', target: 'CREATE OR REPLACE TABLE iceberg.analytics.revenue_hourly AS SELECT date_trunc(\'hour\', order_date) as hour, SUM(total) as revenue, COUNT(*) as orders FROM orders GROUP BY 1', desc: 'Aggregate revenue by hour, refresh hourly', tags: 'mv,revenue,hourly' },
      { name: 'Daily Active Users', type: 'materialized_view', cron: '0 0 * * *', target: 'CREATE OR REPLACE TABLE iceberg.analytics.dau AS SELECT date_trunc(\'day\', event_time) as day, COUNT(DISTINCT user_id) as dau FROM events WHERE event_type = \'page_view\' GROUP BY 1', desc: 'Daily active user counts materialized at midnight', tags: 'mv,users,daily' },
      { name: 'Product Performance', type: 'materialized_view', cron: '0 6 * * *', target: 'CREATE OR REPLACE TABLE iceberg.analytics.product_metrics AS SELECT p.name, COUNT(o.id) as total_orders, SUM(o.quantity) as units_sold, SUM(o.total) as revenue FROM products p JOIN orders o ON p.id = o.product_id GROUP BY 1', desc: 'Product sales metrics refreshed daily at 6 AM', tags: 'mv,products,daily' },
      { name: 'Customer Cohort Analysis', type: 'materialized_view', cron: '0 4 * * 1', target: 'CREATE OR REPLACE TABLE iceberg.analytics.cohorts AS SELECT date_trunc(\'month\', first_order_date) as cohort, date_trunc(\'month\', order_date) as month, COUNT(DISTINCT customer_id) as customers FROM orders GROUP BY 1, 2', desc: 'Weekly cohort rebuild every Monday at 4 AM', tags: 'mv,cohorts,weekly' },
      { name: 'Real-Time Event Counts', type: 'materialized_view', cron: '*/5 * * * *', target: 'CREATE OR REPLACE TABLE iceberg.rt.event_counts AS SELECT event_type, COUNT(*) as cnt, MAX(event_time) as last_seen FROM events GROUP BY 1', desc: 'Near real-time event aggregation every 5 min', tags: 'mv,events,realtime' },
    ]
  },
  {
    category: 'Table Maintenance',
    icon: Wrench,
    color: 'text-rose-400',
    items: [
      { name: 'Iceberg Compaction (All Tables)', type: 'compaction', cron: '0 4 * * *', target: 'CALL iceberg.system.rewrite_data_files(table => \'*\', options => map(\'target-file-size-bytes\', \'536870912\'))', desc: 'Compact small files to 512MB, daily at 4 AM', tags: 'maintenance,compaction,iceberg' },
      { name: 'Expire Old Snapshots', type: 'compaction', cron: '0 5 * * 0', target: 'CALL iceberg.system.expire_snapshots(table => \'*\', older_than => NOW() - INTERVAL \'7 days\')', desc: 'Remove snapshots older than 7 days, weekly Sunday', tags: 'maintenance,snapshots,iceberg' },
      { name: 'Data Quality Check (All Tables)', type: 'data_quality', cron: '0 * * * *', target: 'SELECT table_name, row_count, null_pct, freshness_hours FROM system.data_quality_report WHERE null_pct > 10 OR freshness_hours > 24', desc: 'Hourly data quality scan, alert on anomalies', tags: 'quality,monitoring,hourly' },
      { name: 'Weekly Backup to Cold Storage', type: 'snapshot', cron: '0 3 * * 0', target: 'CALL system.export_snapshot(table => \'*\', destination => \'s3://backups/weekly/\')', desc: 'Export latest snapshots to backup bucket weekly', tags: 'backup,snapshot,weekly' },
    ]
  },
  {
    category: 'dbt & Transforms',
    icon: RefreshCw,
    color: 'text-violet-400',
    items: [
      { name: 'dbt Full Refresh', type: 'transform_run', cron: '0 1 * * *', target: 'dbt run --full-refresh', desc: 'Run all dbt models with full refresh nightly', tags: 'dbt,transforms,nightly' },
      { name: 'dbt Incremental Models', type: 'transform_run', cron: '0 * * * *', target: 'dbt run --select tag:incremental', desc: 'Run only incremental dbt models every hour', tags: 'dbt,incremental,hourly' },
      { name: 'dbt Test Suite', type: 'data_quality', cron: '30 1 * * *', target: 'dbt test', desc: 'Run dbt tests after nightly transform at 1:30 AM', tags: 'dbt,tests,quality' },
    ]
  },
]

export function Scheduler() {
  const location = useLocation()
  const [tab, setTab] = useState('jobs')
  const [jobs, setJobs] = useState<ScheduledJob[]>([])
  const [runs, setRuns] = useState<JobRun[]>([])
  const [createOpen, setCreateOpen] = useState(false)
  const [selectedJob, setSelectedJob] = useState<string | null>(null)
  const [form, setForm] = useState({
    name: '', job_type: 'etl_pipeline', cron: '0 * * * *', target: '',
    retries: '3', tags: '', timeout: '3600', sla_minutes: '60',
    source: '', sink: 'iceberg', write_mode: 'append', engine: 'auto',
  })

  const [dagNodes, setDagNodes] = useState<DagNode[]>([])
  const [dagEdges, setDagEdges] = useState<DagEdge[]>([])

  // Search, filter, sort, pagination state
  const [searchQuery, setSearchQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState<'all' | 'active' | 'paused'>('all')
  const [typeFilter, setTypeFilter] = useState<string>('all')
  const [sortBy, setSortBy] = useState<SortField>('name')
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')
  const [page, setPage] = useState(1)
  const [actionMenu, setActionMenu] = useState<string | null>(null)
  const actionMenuRef = useRef<HTMLDivElement>(null)

  // Close action menu on outside click
  useEffect(() => {
    const handleClick = (e: MouseEvent) => {
      if (actionMenuRef.current && !actionMenuRef.current.contains(e.target as Node)) {
        setActionMenu(null)
      }
    }
    if (actionMenu) document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [actionMenu])

  // Auto-open create modal when navigated with SQL from SQL Editor
  useEffect(() => {
    const state = location.state as { sql?: string; name?: string; jobType?: string } | null
    if (state?.sql) {
      setForm(f => ({
        ...f,
        target: state.sql!,
        name: state.name || '',
        job_type: state.jobType || 'sql_query',
      }))
      setCreateOpen(true)
      // Clear the location state so refreshing doesn't re-trigger
      window.history.replaceState({}, '')
    }
  }, [location.state])

  const loadAll = () => {
    getSchedules().then(r => setJobs(r.schedules || [])).catch(() => {})
    getScheduleRuns().then(r => setRuns(r.runs || [])).catch(() => {})
    getSchedulerDag().then(r => { setDagNodes(r.nodes || []); setDagEdges(r.edges || []); }).catch(() => {})
  }
  useEffect(loadAll, [])

  const handleCreate = async () => {
    try {
      await createSchedule({
        name: form.name,
        job_type: form.job_type,
        cron: form.cron,
        target: form.target,
        engine: form.engine,
        retries: parseInt(form.retries) || 3,
        tags: form.tags ? form.tags.split(',').map(s => s.trim()) : [],
      })
      toast.success('Job created')
      setCreateOpen(false)
      setForm({ name: '', job_type: 'etl_pipeline', cron: '0 * * * *', target: '', retries: '3', tags: '', timeout: '3600', sla_minutes: '60', source: '', sink: 'iceberg', write_mode: 'append', engine: 'auto' })
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  const handleRun = async (id: string) => {
    try {
      await runSchedule(id)
      toast.success('Job triggered')
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  const handleToggleEnabled = async (job: ScheduledJob) => {
    try {
      await updateSchedule(job.id, { enabled: !job.enabled })
      setJobs(js => js.map(j => j.id === job.id ? { ...j, enabled: !j.enabled } : j))
      toast.success(job.enabled ? 'Job paused' : 'Job enabled')
    } catch (e) { toast.error((e as Error).message) }
  }

  const savedQueries = useEditorStore(s => s.savedQueries)

  const applyTemplate = (t: typeof TEMPLATES[0]['items'][0]) => {
    setForm({
      ...form,
      name: t.name, job_type: t.type, cron: t.cron, target: t.target, tags: t.tags,
    })
    setCreateOpen(true)
  }

  const enabledCount = jobs.filter(j => j.enabled).length
  const successRuns = runs.filter(r => r.status === 'success').length
  const errorRuns = runs.filter(r => r.status === 'error').length
  const avgDuration = runs.length > 0 ? runs.reduce((s, r) => s + (r.duration_ms || 0), 0) / runs.length : 0
  const activeJob = jobs.find(j => j.id === selectedJob)

  const etlJobs = jobs.filter(j => j.job_type === 'etl_pipeline' || j.job_type === 'materialized_view')
  const maintenanceJobs = jobs.filter(j => j.job_type === 'compaction' || j.job_type === 'snapshot' || j.job_type === 'data_quality')

  // Build per-job run lookup
  const jobRunMap = useMemo(() => {
    const m: Record<string, JobRun[]> = {}
    for (const r of runs) {
      if (!m[r.job_name]) m[r.job_name] = []
      m[r.job_name].push(r)
    }
    return m
  }, [runs])

  // Filtered + sorted + paginated jobs
  const filteredJobs = useMemo(() => {
    let result = [...jobs]

    // Search
    if (searchQuery) {
      const q = searchQuery.toLowerCase()
      result = result.filter(j =>
        j.name.toLowerCase().includes(q) ||
        j.job_type.toLowerCase().includes(q) ||
        j.tags.some(t => t.toLowerCase().includes(q)) ||
        j.target.toLowerCase().includes(q)
      )
    }

    // Status filter
    if (statusFilter === 'active') result = result.filter(j => j.enabled)
    if (statusFilter === 'paused') result = result.filter(j => !j.enabled)

    // Type filter
    if (typeFilter !== 'all') result = result.filter(j => j.job_type === typeFilter)

    // Sort
    result.sort((a, b) => {
      let cmp = 0
      switch (sortBy) {
        case 'name': cmp = a.name.localeCompare(b.name); break
        case 'type': cmp = a.job_type.localeCompare(b.job_type); break
        case 'status': cmp = (a.enabled ? 0 : 1) - (b.enabled ? 0 : 1); break
        case 'last_run': {
          const aTime = a.last_run ? new Date(a.last_run).getTime() : 0
          const bTime = b.last_run ? new Date(b.last_run).getTime() : 0
          cmp = bTime - aTime // most recent first by default
          break
        }
      }
      return sortDir === 'asc' ? cmp : -cmp
    })

    return result
  }, [jobs, searchQuery, statusFilter, typeFilter, sortBy, sortDir])

  const totalPages = Math.max(1, Math.ceil(filteredJobs.length / PAGE_SIZE))
  const pagedJobs = filteredJobs.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE)

  // Reset page when filters change
  useEffect(() => { setPage(1) }, [searchQuery, statusFilter, typeFilter])

  // Unique job types present
  const jobTypes = useMemo(() => [...new Set(jobs.map(j => j.job_type))], [jobs])

  const handleSort = (field: SortField) => {
    if (sortBy === field) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(field)
      setSortDir(field === 'last_run' ? 'desc' : 'asc')
    }
  }

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortBy !== field) return <ChevronsUpDown className="w-3 h-3 text-zinc-700" />
    return sortDir === 'asc' ? <ChevronUp className="w-3 h-3 text-amber-400" /> : <ChevronDown className="w-3 h-3 text-amber-400" />
  }

  return (
    <div className="flex h-full animate-fade-in">
      <div className="flex-1 flex flex-col min-w-0">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-white/[0.04]">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
              <Clock className="w-4.5 h-4.5 text-amber-400" />
            </div>
            <div>
              <h1 className="text-base font-display font-bold text-zinc-100">Scheduler</h1>
              <p className="text-2xs text-zinc-500">ETL pipelines, materialized views, transforms, and table maintenance</p>
            </div>
          </div>
          <Button variant="primary" size="sm" icon={<Plus className="w-3.5 h-3.5" />} onClick={() => setCreateOpen(true)}>New Job</Button>
        </div>

        {/* Stats strip */}
        <div className="grid grid-cols-5 gap-px bg-white/[0.02] border-b border-white/[0.04]">
          {[
            { label: 'Total Jobs', value: String(jobs.length), icon: Calendar, color: 'text-amber-400' },
            { label: 'Active', value: String(enabledCount), icon: ToggleRight, color: 'text-emerald-400' },
            { label: 'Success Runs', value: String(successRuns), icon: CheckCircle2, color: 'text-blue-400' },
            { label: 'Failed Runs', value: String(errorRuns), icon: XCircle, color: 'text-rose-400' },
            { label: 'Avg Duration', value: formatDuration(avgDuration), icon: Timer, color: 'text-violet-400' },
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

        <Tabs
          tabs={[
            { id: 'jobs', label: 'All Jobs', icon: <Timer className="w-3 h-3" />, count: jobs.length },
            { id: 'etl', label: 'ETL & Views', icon: <Eye className="w-3 h-3" />, count: etlJobs.length },
            { id: 'runs', label: 'Run History', icon: <History className="w-3 h-3" />, count: runs.length },
            { id: 'dag', label: 'Pipeline DAG', icon: <Network className="w-3 h-3" /> },
            { id: 'templates', label: 'Templates', icon: <FileText className="w-3 h-3" /> },
          ]}
          active={tab}
          onChange={setTab}
          className="mx-6 mt-3"
        />

        <div className="flex-1 overflow-auto p-6">
          {/* ─── All Jobs ─── */}
          {tab === 'jobs' && (
            <div className="space-y-3">
              {jobs.length === 0 ? (
                <EmptyState
                  icon={<Calendar className="w-6 h-6" />}
                  title="No scheduled jobs"
                  description="Create ETL pipelines, materialized views, and table maintenance jobs"
                  action={<Button variant="primary" size="sm" icon={<Plus className="w-3.5 h-3.5" />} onClick={() => setCreateOpen(true)}>Create Job</Button>}
                />
              ) : (
                <>
                  {/* Search + Filter bar */}
                  <div className="flex items-center gap-2 flex-wrap">
                    <div className="relative flex-1 min-w-[200px]">
                      <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-600" />
                      <input
                        type="text"
                        value={searchQuery}
                        onChange={e => setSearchQuery(e.target.value)}
                        placeholder="Search jobs by name, type, or tag..."
                        className="w-full pl-9 pr-3 py-2 text-xs bg-white/[0.03] border border-white/[0.06] rounded-lg text-zinc-300 placeholder-zinc-600 outline-none focus:border-amber-400/30 transition-colors"
                      />
                    </div>
                    <select
                      value={statusFilter}
                      onChange={e => setStatusFilter(e.target.value as 'all' | 'active' | 'paused')}
                      className="text-xs bg-white/[0.03] border border-white/[0.06] rounded-lg px-3 py-2 text-zinc-400 outline-none focus:border-amber-400/30 cursor-pointer"
                    >
                      <option value="all">All Status</option>
                      <option value="active">Active ({jobs.filter(j => j.enabled).length})</option>
                      <option value="paused">Paused ({jobs.filter(j => !j.enabled).length})</option>
                    </select>
                    <select
                      value={typeFilter}
                      onChange={e => setTypeFilter(e.target.value)}
                      className="text-xs bg-white/[0.03] border border-white/[0.06] rounded-lg px-3 py-2 text-zinc-400 outline-none focus:border-amber-400/30 cursor-pointer"
                    >
                      <option value="all">All Types</option>
                      {jobTypes.map(t => (
                        <option key={t} value={t}>{JOB_TYPE_CONFIG[t]?.label || t}</option>
                      ))}
                    </select>
                    {(searchQuery || statusFilter !== 'all' || typeFilter !== 'all') && (
                      <button
                        onClick={() => { setSearchQuery(''); setStatusFilter('all'); setTypeFilter('all') }}
                        className="text-2xs text-zinc-500 hover:text-zinc-300 transition-colors px-2 py-2"
                      >
                        Clear filters
                      </button>
                    )}
                    <span className="text-2xs text-zinc-600 ml-auto">
                      {filteredJobs.length === jobs.length ? `${jobs.length} jobs` : `${filteredJobs.length} of ${jobs.length}`}
                    </span>
                  </div>

                  {/* Table header — sortable columns */}
                  <div className="flex items-center gap-3 px-4 py-2 text-2xs text-zinc-600 border-b border-white/[0.04]">
                    <div className="w-8" /> {/* icon space */}
                    <button onClick={() => handleSort('name')} className="flex items-center gap-1 flex-1 min-w-0 hover:text-zinc-400 transition-colors">
                      Name <SortIcon field="name" />
                    </button>
                    <button onClick={() => handleSort('type')} className="flex items-center gap-1 w-28 hover:text-zinc-400 transition-colors">
                      Type <SortIcon field="type" />
                    </button>
                    <div className="w-24">Schedule</div>
                    <button onClick={() => handleSort('last_run')} className="flex items-center gap-1 w-36 hover:text-zinc-400 transition-colors">
                      Last Run <SortIcon field="last_run" />
                    </button>
                    <button onClick={() => handleSort('status')} className="flex items-center gap-1 w-20 hover:text-zinc-400 transition-colors">
                      Status <SortIcon field="status" />
                    </button>
                    <div className="w-10" /> {/* actions space */}
                  </div>

                  {/* Job rows */}
                  <div className="space-y-1">
                    {pagedJobs.length === 0 && (
                      <div className="text-center py-8">
                        <Search className="w-5 h-5 text-zinc-700 mx-auto mb-2" />
                        <p className="text-xs text-zinc-600">No jobs match your filters</p>
                      </div>
                    )}
                    {pagedJobs.map(job => {
                      const cfg = JOB_TYPE_CONFIG[job.job_type] || JOB_TYPE_CONFIG.sql_query
                      const jRuns = jobRunMap[job.name] || []
                      const lastRun = jRuns[0]
                      const successRate = jRuns.length > 0 ? (jRuns.filter(r => r.status === 'success').length / jRuns.length * 100) : null
                      return (
                        <div key={job.id}>
                          <button
                            onClick={() => setSelectedJob(selectedJob === job.id ? null : job.id)}
                            className={cn(
                              'w-full text-left rounded-lg border transition-all px-4 py-3 flex items-center gap-3',
                              selectedJob === job.id
                                ? 'bg-white/[0.04] border-amber-400/20'
                                : 'bg-white/[0.02] border-white/[0.04] hover:bg-white/[0.03] hover:border-white/[0.06]',
                              !job.enabled && 'opacity-60'
                            )}
                          >
                            {/* Type icon */}
                            <Tooltip content={cfg.desc} position="right">
                              <div className={cn('w-8 h-8 rounded-lg border flex items-center justify-center flex-shrink-0', cfg.color)}>
                                <cfg.icon className="w-4 h-4" />
                              </div>
                            </Tooltip>

                            {/* Name + tags */}
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center gap-2">
                                <h3 className="text-xs font-semibold text-zinc-200 truncate">{job.name}</h3>
                                {job.tags.slice(0, 2).map(t => (
                                  <span key={t} className="text-[9px] text-zinc-600 bg-white/[0.03] px-1.5 py-0.5 rounded flex-shrink-0">{t}</span>
                                ))}
                                {job.tags.length > 2 && <span className="text-[9px] text-zinc-700">+{job.tags.length - 2}</span>}
                              </div>
                            </div>

                            {/* Type */}
                            <div className="w-28 flex-shrink-0">
                              <Badge className={cn('text-[9px]', cfg.color)}>{cfg.label}</Badge>
                            </div>

                            {/* Schedule */}
                            <Tooltip content={`Cron: ${job.cron}`} position="top">
                              <div className="w-24 flex-shrink-0">
                                <span className="text-2xs text-zinc-400">{cronToHuman(job.cron)}</span>
                              </div>
                            </Tooltip>

                            {/* Last run — status + time + duration */}
                            <div className="w-36 flex-shrink-0">
                              {lastRun ? (
                                <div className="flex items-center gap-1.5">
                                  {lastRun.status === 'success'
                                    ? <CheckCircle2 className="w-3 h-3 text-emerald-400 flex-shrink-0" />
                                    : <XCircle className="w-3 h-3 text-rose-400 flex-shrink-0" />
                                  }
                                  <span className="text-2xs text-zinc-400">{formatRelativeTime(lastRun.started_at)}</span>
                                  {lastRun.duration_ms != null && (
                                    <span className="text-[9px] font-mono text-zinc-600">{formatDuration(lastRun.duration_ms)}</span>
                                  )}
                                </div>
                              ) : (
                                <span className="text-2xs text-zinc-700">Never run</span>
                              )}
                              {successRate !== null && (
                                <div className="flex items-center gap-1 mt-0.5">
                                  <div className="w-12 h-1 bg-white/[0.04] rounded-full overflow-hidden">
                                    <div className={cn('h-full rounded-full', successRate >= 80 ? 'bg-emerald-400' : successRate >= 50 ? 'bg-amber-400' : 'bg-rose-400')}
                                      style={{ width: `${successRate}%` }} />
                                  </div>
                                  <span className="text-[9px] font-mono text-zinc-600">{successRate.toFixed(0)}%</span>
                                </div>
                              )}
                            </div>

                            {/* Status */}
                            <div className="w-20 flex-shrink-0">
                              {job.enabled
                                ? <StatusDot status="healthy" label="Active" />
                                : <StatusDot status="idle" label="Paused" />
                              }
                            </div>

                            {/* Action menu */}
                            <div className="w-10 flex-shrink-0 flex justify-end relative" onClick={e => e.stopPropagation()}>
                              <button
                                onClick={() => setActionMenu(actionMenu === job.id ? null : job.id)}
                                className="p-1.5 rounded-lg hover:bg-white/[0.06] transition-colors"
                              >
                                <MoreVertical className="w-4 h-4 text-zinc-600" />
                              </button>
                              {actionMenu === job.id && (
                                <div ref={actionMenuRef} className="absolute right-0 top-8 z-50 w-40 bg-navy-900 border border-white/[0.08] rounded-lg shadow-xl py-1 animate-fade-in">
                                  <button onClick={() => { handleRun(job.id); setActionMenu(null) }}
                                    className="w-full text-left px-3 py-1.5 text-xs text-zinc-300 hover:bg-white/[0.04] flex items-center gap-2">
                                    <Play className="w-3 h-3 text-emerald-400" /> Run Now
                                  </button>
                                  <button onClick={() => { setSelectedJob(job.id); setActionMenu(null) }}
                                    className="w-full text-left px-3 py-1.5 text-xs text-zinc-300 hover:bg-white/[0.04] flex items-center gap-2">
                                    <Eye className="w-3 h-3 text-zinc-400" /> View Details
                                  </button>
                                  <button onClick={() => { handleToggleEnabled(job); setActionMenu(null) }}
                                    className="w-full text-left px-3 py-1.5 text-xs text-zinc-300 hover:bg-white/[0.04] flex items-center gap-2">
                                    {job.enabled ? <><Pause className="w-3 h-3 text-amber-400" /> Pause</> : <><Play className="w-3 h-3 text-amber-400" /> Enable</>}
                                  </button>
                                  <div className="border-t border-white/[0.04] my-1" />
                                  <button onClick={async () => {
                                    await deleteSchedule(job.id)
                                    setJobs(js => js.filter(j => j.id !== job.id))
                                    setActionMenu(null)
                                    toast.success('Deleted')
                                  }}
                                    className="w-full text-left px-3 py-1.5 text-xs text-rose-400 hover:bg-rose-400/[0.06] flex items-center gap-2">
                                    <Trash2 className="w-3 h-3" /> Delete
                                  </button>
                                </div>
                              )}
                            </div>
                          </button>

                          {/* Expanded detail */}
                          {selectedJob === job.id && (
                            <div className="ml-4 mr-4 mt-1 mb-2 p-4 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                              <div className="grid grid-cols-3 gap-4 mb-4">
                                <div>
                                  <p className="text-2xs text-zinc-500 mb-1.5 font-semibold">Configuration</p>
                                  <div className="space-y-1.5 text-2xs">
                                    <div className="flex justify-between"><span className="text-zinc-500">Schedule</span><Tooltip content="Cron expression: min hour dom month dow" position="top"><span className="font-mono text-zinc-300 cursor-help">{job.cron} ({cronToHuman(job.cron)})</span></Tooltip></div>
                                    <div className="flex justify-between"><span className="text-zinc-500">Retries</span><span className="font-mono text-zinc-300">{job.retries || 0}</span></div>
                                    {job.timeout_seconds && <div className="flex justify-between"><span className="text-zinc-500">Timeout</span><span className="font-mono text-zinc-300">{job.timeout_seconds}s</span></div>}
                                  </div>
                                </div>
                                <div>
                                  <p className="text-2xs text-zinc-500 mb-1.5 font-semibold">Recent Runs ({jRuns.length})</p>
                                  {jRuns.length === 0 ? (
                                    <p className="text-2xs text-zinc-600">No runs yet</p>
                                  ) : (
                                    <div className="space-y-1">
                                      {jRuns.slice(0, 5).map(r => (
                                        <div key={r.id} className="flex items-center gap-2 text-2xs">
                                          {r.status === 'success' ? <CheckCircle2 className="w-3 h-3 text-emerald-400" /> : <XCircle className="w-3 h-3 text-rose-400" />}
                                          <span className="text-zinc-400">{formatRelativeTime(r.started_at)}</span>
                                          {r.duration_ms != null && <span className="font-mono text-zinc-600">{formatDuration(r.duration_ms)}</span>}
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                                <div>
                                  <p className="text-2xs text-zinc-500 mb-1.5 font-semibold">Stats</p>
                                  <div className="space-y-1.5 text-2xs">
                                    <div className="flex justify-between"><span className="text-zinc-500">Total Runs</span><span className="font-mono text-zinc-300">{jRuns.length}</span></div>
                                    <div className="flex justify-between"><span className="text-zinc-500">Success Rate</span><span className="font-mono text-emerald-400">{jRuns.length > 0 ? `${((jRuns.filter(r => r.status === 'success').length / jRuns.length) * 100).toFixed(0)}%` : '—'}</span></div>
                                    <div className="flex justify-between"><span className="text-zinc-500">Avg Duration</span><span className="font-mono text-zinc-300">{jRuns.length > 0 ? formatDuration(jRuns.reduce((s, r) => s + (r.duration_ms || 0), 0) / jRuns.length) : '—'}</span></div>
                                  </div>
                                </div>
                              </div>
                              {/* SQL preview */}
                              {job.target && job.target.length > 30 && (
                                <div className="relative">
                                  <pre className="text-2xs font-mono text-zinc-400 bg-navy-950/80 rounded-lg p-3 overflow-x-auto border border-white/[0.03]">{job.target}</pre>
                                  <button
                                    onClick={(e) => { e.stopPropagation(); navigator.clipboard.writeText(job.target); toast.success('Copied SQL') }}
                                    className="absolute top-2 right-2 p-1 rounded text-zinc-600 hover:text-zinc-300 transition-colors"
                                  >
                                    <Copy className="w-3 h-3" />
                                  </button>
                                </div>
                              )}
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>

                  {/* Pagination */}
                  {filteredJobs.length > PAGE_SIZE && (
                    <div className="flex items-center justify-between pt-2 border-t border-white/[0.04]">
                      <span className="text-2xs text-zinc-600">
                        Showing {(page - 1) * PAGE_SIZE + 1}–{Math.min(page * PAGE_SIZE, filteredJobs.length)} of {filteredJobs.length} jobs
                      </span>
                      <div className="flex items-center gap-1">
                        <button
                          onClick={() => setPage(p => Math.max(1, p - 1))}
                          disabled={page <= 1}
                          className="p-1.5 rounded-lg hover:bg-white/[0.04] transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
                        >
                          <ChevronLeft className="w-4 h-4 text-zinc-400" />
                        </button>
                        {Array.from({ length: totalPages }, (_, i) => i + 1).map(p => (
                          <button
                            key={p}
                            onClick={() => setPage(p)}
                            className={cn(
                              'w-7 h-7 rounded-lg text-xs font-mono transition-colors',
                              p === page ? 'bg-amber-400/10 text-amber-400 border border-amber-400/20' : 'text-zinc-500 hover:bg-white/[0.04]'
                            )}
                          >
                            {p}
                          </button>
                        ))}
                        <button
                          onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                          disabled={page >= totalPages}
                          className="p-1.5 rounded-lg hover:bg-white/[0.04] transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
                        >
                          <ChevronRight className="w-4 h-4 text-zinc-400" />
                        </button>
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          )}

          {/* ─── ETL & Materialized Views Tab ─── */}
          {tab === 'etl' && (
            <div className="space-y-6">
              {/* ETL Flow Diagram */}
              <Card className="overflow-hidden">
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
                  <ArrowRight className="w-4 h-4 text-amber-400" /> ETL Pipeline Architecture
                </h3>
                <div className="flex items-center justify-between gap-4 text-xs text-zinc-400 px-2">
                  <div className="flex flex-col items-center gap-1.5 flex-1">
                    <div className="w-full p-3 rounded-lg border border-cyan-400/20 bg-cyan-400/5 text-center">
                      <Database className="w-5 h-5 text-cyan-400 mx-auto mb-1" />
                      <p className="text-2xs font-semibold text-cyan-400">Source</p>
                      <p className="text-[10px] text-zinc-500 mt-0.5">Postgres, MySQL, MongoDB, S3, APIs</p>
                    </div>
                  </div>
                  <ArrowRight className="w-5 h-5 text-zinc-600 flex-shrink-0" />
                  <div className="flex flex-col items-center gap-1.5 flex-1">
                    <div className="w-full p-3 rounded-lg border border-violet-400/20 bg-violet-400/5 text-center">
                      <RefreshCw className="w-5 h-5 text-violet-400 mx-auto mb-1" />
                      <p className="text-2xs font-semibold text-violet-400">Transform</p>
                      <p className="text-[10px] text-zinc-500 mt-0.5">SQL, dbt models, DataFusion UDFs</p>
                    </div>
                  </div>
                  <ArrowRight className="w-5 h-5 text-zinc-600 flex-shrink-0" />
                  <div className="flex flex-col items-center gap-1.5 flex-1">
                    <div className="w-full p-3 rounded-lg border border-amber-400/20 bg-amber-400/5 text-center">
                      <Snowflake className="w-5 h-5 text-amber-400 mx-auto mb-1" />
                      <p className="text-2xs font-semibold text-amber-400">Sink</p>
                      <p className="text-[10px] text-zinc-500 mt-0.5">Iceberg, Delta, Lance, Parquet</p>
                    </div>
                  </div>
                  <ArrowRight className="w-5 h-5 text-zinc-600 flex-shrink-0" />
                  <div className="flex flex-col items-center gap-1.5 flex-1">
                    <div className="w-full p-3 rounded-lg border border-emerald-400/20 bg-emerald-400/5 text-center">
                      <Clock className="w-5 h-5 text-emerald-400 mx-auto mb-1" />
                      <p className="text-2xs font-semibold text-emerald-400">Schedule</p>
                      <p className="text-[10px] text-zinc-500 mt-0.5">Cron, interval, event-triggered</p>
                    </div>
                  </div>
                </div>
              </Card>

              {/* ETL jobs list */}
              {etlJobs.length === 0 ? (
                <EmptyState
                  icon={<ArrowRight className="w-6 h-6" />}
                  title="No ETL pipelines or materialized views"
                  description="Create ETL jobs from templates or build custom source → Iceberg pipelines"
                  action={
                    <div className="flex gap-2">
                      <Button variant="primary" size="sm" icon={<Plus className="w-3.5 h-3.5" />} onClick={() => { setForm({ ...form, job_type: 'etl_pipeline' }); setCreateOpen(true) }}>New ETL Pipeline</Button>
                      <Button variant="secondary" size="sm" icon={<Eye className="w-3.5 h-3.5" />} onClick={() => { setForm({ ...form, job_type: 'materialized_view' }); setCreateOpen(true) }}>New Materialized View</Button>
                    </div>
                  }
                />
              ) : (
                <div className="space-y-2">
                  {etlJobs.map(job => {
                    const cfg = JOB_TYPE_CONFIG[job.job_type] || JOB_TYPE_CONFIG.etl_pipeline
                    const jRuns = runs.filter(r => r.job_name === job.name)
                    return (
                      <Card key={job.id} padding="sm" className={cn('hover:bg-white/[0.02] transition-colors', !job.enabled && 'opacity-50')}>
                        <div className="flex items-center gap-4">
                          <div className={cn('w-10 h-10 rounded-lg border flex items-center justify-center', cfg.color)}>
                            <cfg.icon className="w-5 h-5" />
                          </div>
                          <div className="flex-1 min-w-0">
                            <div className="flex items-center gap-2">
                              <h3 className="text-sm font-semibold text-zinc-200">{job.name}</h3>
                              {job.enabled ? <StatusDot status="healthy" /> : <StatusDot status="idle" />}
                            </div>
                            <div className="flex items-center gap-2 mt-1">
                              <Badge className={cfg.color}>{cfg.label}</Badge>
                              <Tooltip content={`Schedule: ${job.cron}${job.cron === '0 * * * *' ? ' (hourly)' : job.cron === '0 0 * * *' ? ' (daily midnight)' : job.cron === '*/5 * * * *' ? ' (every 5 min)' : job.cron === '*/15 * * * *' ? ' (every 15 min)' : ''}`} position="top">
                                <Badge className="font-mono bg-white/[0.04] text-zinc-400 border-white/[0.06]">{job.cron}</Badge>
                              </Tooltip>
                              {jRuns.length > 0 && <span className="text-2xs text-zinc-600">{jRuns.filter(r => r.status === 'success').length}/{jRuns.length} runs OK</span>}
                            </div>
                          </div>
                          <div className="flex items-center gap-1">
                            <Tooltip content={job.enabled ? 'Pause job' : 'Enable job'} position="top">
                              <button onClick={() => handleToggleEnabled(job)} className="p-1.5 rounded-lg hover:bg-white/[0.04] transition-colors">
                                {job.enabled ? <ToggleRight className="w-5 h-5 text-amber-400" /> : <ToggleLeft className="w-5 h-5 text-zinc-600" />}
                              </button>
                            </Tooltip>
                            <Button variant="ghost" size="sm" onClick={() => handleRun(job.id)}><Play className="w-3.5 h-3.5 text-emerald-400" /></Button>
                            <Button variant="ghost" size="sm" onClick={async () => {
                              await deleteSchedule(job.id)
                              setJobs(js => js.filter(j => j.id !== job.id))
                              toast.success('Deleted')
                            }}><Trash2 className="w-3.5 h-3.5 text-zinc-600" /></Button>
                          </div>
                        </div>
                        {job.target && job.target.length > 30 && (
                          <pre className="text-[10px] font-mono text-zinc-500 mt-2 bg-navy-950/50 rounded p-2 overflow-x-auto">{job.target}</pre>
                        )}
                      </Card>
                    )
                  })}
                </div>
              )}

              {/* Write modes explanation */}
              <Card padding="sm">
                <h4 className="text-xs font-display font-semibold text-zinc-300 mb-2">Write Modes for ETL Jobs</h4>
                <div className="grid grid-cols-3 gap-3 text-2xs">
                  <div className="p-2 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                    <p className="font-semibold text-amber-400">Append</p>
                    <p className="text-zinc-500 mt-0.5">Insert new rows. Best for event logs and incremental loads.</p>
                  </div>
                  <div className="p-2 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                    <p className="font-semibold text-cyan-400">Overwrite</p>
                    <p className="text-zinc-500 mt-0.5">Replace entire table. Best for materialized views and full syncs.</p>
                  </div>
                  <div className="p-2 rounded-lg bg-white/[0.02] border border-white/[0.04]">
                    <p className="font-semibold text-emerald-400">Merge (Upsert)</p>
                    <p className="text-zinc-500 mt-0.5">Insert or update by key. Best for slowly changing dimensions.</p>
                  </div>
                </div>
              </Card>
            </div>
          )}

          {/* ─── Run History ─── */}
          {tab === 'runs' && (
            <div className="rounded-xl border border-white/[0.04] overflow-hidden">
              {runs.length === 0 ? (
                <EmptyState icon={<History className="w-5 h-5" />} title="No runs yet" description="Trigger a job to see execution history" />
              ) : (
                <div className="divide-y divide-white/[0.03] max-h-[600px] overflow-y-auto">
                  {runs.map(r => {
                    const matchedJob = jobs.find(j => j.name === r.job_name)
                    const cfg = matchedJob ? (JOB_TYPE_CONFIG[matchedJob.job_type] || JOB_TYPE_CONFIG.sql_query) : JOB_TYPE_CONFIG.sql_query
                    return (
                      <div key={r.id} className="flex items-center gap-3 px-4 py-2.5 hover:bg-white/[0.02] transition-colors">
                        {r.status === 'success' ? <CheckCircle2 className="w-4 h-4 text-emerald-400" /> :
                         r.status === 'error' ? <XCircle className="w-4 h-4 text-rose-400" /> :
                         <AlertCircle className="w-4 h-4 text-amber-400" />}
                        <span className="text-xs font-medium text-zinc-300">{r.job_name}</span>
                        <Badge className={cn('text-[9px]', cfg?.color)}>{cfg?.label || 'job'}</Badge>
                        <span className="flex-1" />
                        {r.duration_ms && <Badge className="bg-white/[0.04] text-zinc-400 border-white/[0.06]"><Zap className="w-3 h-3" /> {formatDuration(r.duration_ms)}</Badge>}
                        <span className="text-2xs text-zinc-600">{formatRelativeTime(r.started_at)}</span>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          )}

          {/* ─── Pipeline DAG ─── */}
          {tab === 'dag' && (
            <div className="space-y-4">
              {dagNodes.length === 0 ? (
                <EmptyState
                  icon={<Network className="w-6 h-6" />}
                  title="No pipeline DAG yet"
                  description="Create jobs with 'after:job_name' tags to define dependencies and visualize your DAG"
                  action={<Button variant="primary" size="sm" icon={<Plus className="w-3.5 h-3.5" />} onClick={() => setCreateOpen(true)}>Create Job</Button>}
                />
              ) : (
                <>
                  <div className="flex items-center gap-3 mb-2">
                    <h3 className="text-sm font-display font-semibold text-zinc-200">Orchestration DAG</h3>
                    <Badge className="bg-white/[0.04] text-zinc-400">{dagNodes.length} nodes</Badge>
                    {dagEdges.length > 0 && <Badge className="bg-cyan-400/10 text-cyan-400 border-cyan-400/20">{dagEdges.length} dependencies</Badge>}
                  </div>
                  <Card padding="lg">
                    <div className="space-y-1">
                      {/* Render nodes in a topological-ish order */}
                      {dagNodes.map(node => {
                        const config = JOB_TYPE_CONFIG[node.job_type] || JOB_TYPE_CONFIG.sql_query
                        const Icon = config?.icon || Timer
                        const incomingEdges = dagEdges.filter(e => e.to === node.id)
                        const outgoingEdges = dagEdges.filter(e => e.from === node.id)
                        const statusMap: Record<string, 'healthy' | 'error' | 'warning' | 'idle'> = { success: 'healthy', error: 'error', running: 'warning', pending: 'idle' }
                        return (
                          <div key={node.id} className="flex items-center gap-3 py-2.5 px-3 rounded-lg hover:bg-white/[0.02] transition-colors border border-transparent hover:border-white/[0.03]">
                            <div className={cn('w-8 h-8 rounded-lg flex items-center justify-center', config?.color || 'bg-zinc-400/10 text-zinc-400')}>
                              <Icon className="w-4 h-4" />
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center gap-2">
                                <span className="text-xs font-medium text-zinc-200">{node.name}</span>
                                <Badge className={config?.color || 'bg-zinc-400/10 text-zinc-400'}>{JOB_TYPE_CONFIG[node.job_type]?.label || node.job_type}</Badge>
                              </div>
                              <div className="flex items-center gap-3 mt-0.5">
                                <span className="text-2xs font-mono text-zinc-600">{node.cron}</span>
                                {incomingEdges.length > 0 && (
                                  <span className="text-2xs text-cyan-400/60">
                                    depends on: {incomingEdges.map(e => dagNodes.find(n => n.id === e.from)?.name || e.from).join(', ')}
                                  </span>
                                )}
                                {outgoingEdges.length > 0 && (
                                  <span className="text-2xs text-amber-400/60">
                                    blocks: {outgoingEdges.map(e => dagNodes.find(n => n.id === e.to)?.name || e.to).join(', ')}
                                  </span>
                                )}
                              </div>
                            </div>
                            <StatusDot status={statusMap[node.status] || 'idle'} label={node.status} />
                            {!node.enabled && <Badge className="bg-zinc-500/10 text-zinc-500">disabled</Badge>}
                          </div>
                        )
                      })}
                    </div>
                  </Card>
                  {dagEdges.length > 0 && (
                    <Card>
                      <h3 className="text-xs font-display font-semibold text-zinc-300 mb-3">Dependency Edges</h3>
                      <div className="space-y-1">
                        {dagEdges.map((edge, i) => (
                          <div key={i} className="flex items-center gap-2 text-xs text-zinc-400">
                            <span className="font-mono text-amber-400/70">{dagNodes.find(n => n.id === edge.from)?.name || edge.from}</span>
                            <ArrowRight className="w-3 h-3 text-zinc-600" />
                            <span className="font-mono text-cyan-400/70">{dagNodes.find(n => n.id === edge.to)?.name || edge.to}</span>
                            {edge.label && <Badge className="bg-white/[0.04] text-zinc-500 text-2xs">{edge.label}</Badge>}
                          </div>
                        ))}
                      </div>
                    </Card>
                  )}
                </>
              )}
            </div>
          )}

          {/* ─── Templates ─── */}
          {tab === 'templates' && (
            <div className="space-y-8">
              {TEMPLATES.map(cat => (
                <div key={cat.category}>
                  <h3 className={cn('text-sm font-display font-semibold mb-3 flex items-center gap-2', cat.color)}>
                    <cat.icon className="w-4 h-4" /> {cat.category}
                  </h3>
                  <div className="grid grid-cols-2 gap-3">
                    {cat.items.map(t => {
                      const cfg = JOB_TYPE_CONFIG[t.type]
                      return (
                        <button
                          key={t.name}
                          onClick={() => applyTemplate(t)}
                          className="group text-left glass glass-hover rounded-xl p-4"
                        >
                          <div className="flex items-center gap-2 mb-2">
                            <Badge className={cfg?.color || ''}>{cfg?.label || t.type}</Badge>
                            <span className="text-2xs font-mono text-zinc-600">{t.cron}</span>
                          </div>
                          <h4 className="text-sm font-semibold text-zinc-200 group-hover:text-zinc-50 transition-colors">{t.name}</h4>
                          <p className="text-2xs text-zinc-500 mt-1 leading-relaxed">{t.desc}</p>
                          {t.tags && (
                            <div className="flex gap-1 mt-2 flex-wrap">
                              {t.tags.split(',').map(tag => (
                                <span key={tag} className="text-[9px] text-zinc-600 bg-white/[0.03] px-1.5 py-0.5 rounded">{tag}</span>
                              ))}
                            </div>
                          )}
                        </button>
                      )
                    })}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* ─── Create Job Modal ─── */}
      <Modal open={createOpen} onClose={() => setCreateOpen(false)} title="Create Scheduled Job" width="max-w-2xl">
        <div className="space-y-4">
          <Input label="Job Name" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} placeholder="nightly-orders-etl" />

          {/* Job type grid */}
          <div>
            <span className="text-xs font-medium text-zinc-400 mb-2 block">Job Type</span>
            <div className="grid grid-cols-4 gap-2">
              {Object.entries(JOB_TYPE_CONFIG).map(([key, cfg]) => (
                <button
                  key={key}
                  onClick={() => setForm(f => ({ ...f, job_type: key }))}
                  className={cn(
                    'p-2.5 rounded-lg border text-left transition-all',
                    form.job_type === key
                      ? 'bg-white/[0.06] border-amber-400/30'
                      : 'bg-white/[0.02] border-white/[0.04] hover:border-white/[0.08]'
                  )}
                >
                  <cfg.icon className={cn('w-4 h-4 mb-1', cfg.color.split(' ')[1])} />
                  <p className="text-2xs font-medium text-zinc-200">{cfg.label}</p>
                </button>
              ))}
            </div>
          </div>

          {/* ETL-specific source/sink selector */}
          {(form.job_type === 'etl_pipeline' || form.job_type === 'materialized_view') && (
            <div className="p-3 rounded-lg bg-white/[0.02] border border-white/[0.04] space-y-3">
              <p className="text-2xs font-semibold text-zinc-400 flex items-center gap-1.5"><ArrowRight className="w-3 h-3" /> Pipeline Configuration</p>
              <div className="grid grid-cols-3 gap-3">
                <Select label="Source" value={form.source} onChange={e => setForm(f => ({ ...f, source: e.target.value }))}
                  options={[
                    { value: '', label: 'Select source...' },
                    { value: 'postgres', label: 'PostgreSQL' },
                    { value: 'mysql', label: 'MySQL' },
                    { value: 'mongodb', label: 'MongoDB' },
                    { value: 'clickhouse', label: 'ClickHouse' },
                    { value: 's3', label: 'Amazon S3' },
                    { value: 'minio', label: 'MinIO' },
                    { value: 'gcs', label: 'Google Cloud Storage' },
                    { value: 'kafka', label: 'Apache Kafka' },
                    { value: 'cdc_postgres', label: 'Postgres CDC' },
                    { value: 'cdc_mongodb', label: 'MongoDB CDC' },
                    { value: 'rest_api', label: 'REST API' },
                    { value: 'salesforce', label: 'Salesforce' },
                  ]}
                />
                <Select label="Sink Format" value={form.sink} onChange={e => setForm(f => ({ ...f, sink: e.target.value }))}
                  options={[
                    { value: 'iceberg', label: 'Apache Iceberg' },
                    { value: 'delta', label: 'Delta Lake' },
                    { value: 'lance', label: 'Lance (Vector)' },
                    { value: 'parquet', label: 'Parquet Files' },
                  ]}
                />
                <Select label="Write Mode" value={form.write_mode} onChange={e => setForm(f => ({ ...f, write_mode: e.target.value }))}
                  options={[
                    { value: 'append', label: 'Append' },
                    { value: 'overwrite', label: 'Overwrite' },
                    { value: 'merge', label: 'Merge (Upsert)' },
                  ]}
                />
              </div>
            </div>
          )}

          {/* Cron with presets */}
          <div>
            <Input label="Cron Expression" value={form.cron} onChange={e => setForm(f => ({ ...f, cron: e.target.value }))} placeholder="0 * * * *" />
            <div className="flex flex-wrap gap-1.5 mt-2">
              {CRON_PRESETS.map(p => (
                <button
                  key={p.value}
                  onClick={() => setForm(f => ({ ...f, cron: p.value }))}
                  className={cn(
                    'px-2 py-1 text-2xs rounded-md border transition-all',
                    form.cron === p.value
                      ? 'bg-amber-400/10 text-amber-400 border-amber-400/20'
                      : 'bg-white/[0.03] text-zinc-500 border-white/[0.04] hover:text-zinc-300'
                  )}
                >
                  {p.label}
                </button>
              ))}
            </div>
          </div>

          {/* Target SQL */}
          <div>
            <div className="flex items-center justify-between mb-1.5">
              <label className="text-xs font-medium text-zinc-400">
                {form.job_type === 'materialized_view' ? 'View SQL Definition' :
                 form.job_type === 'etl_pipeline' ? 'ETL SQL (CREATE TABLE ... AS SELECT ...)' :
                 form.job_type === 'data_quality' ? 'Quality Check SQL' :
                 'Target SQL or Command'}
              </label>
              {savedQueries.length > 0 && (
                <div className="flex items-center gap-1.5">
                  <BookOpen className="w-3 h-3 text-zinc-500" />
                  <select
                    className="text-2xs bg-white/[0.03] border border-white/[0.06] rounded-md px-2 py-1 text-zinc-400 outline-none focus:border-amber-400/30 cursor-pointer"
                    value=""
                    onChange={e => {
                      const q = savedQueries.find(q => q.id === e.target.value)
                      if (q) setForm(f => ({ ...f, target: q.sql, name: f.name || q.name }))
                    }}
                  >
                    <option value="">Load saved query...</option>
                    {savedQueries.map(q => (
                      <option key={q.id} value={q.id}>{q.name}</option>
                    ))}
                  </select>
                </div>
              )}
            </div>
            <Textarea
              value={form.target}
              onChange={e => setForm(f => ({ ...f, target: e.target.value }))}
              placeholder={
                form.job_type === 'materialized_view'
                  ? 'CREATE OR REPLACE TABLE iceberg.analytics.my_view AS\nSELECT ...\nFROM ...\nGROUP BY ...'
                  : form.job_type === 'etl_pipeline'
                  ? 'INSERT INTO iceberg.warehouse.my_table\nSELECT * FROM source_table\nWHERE updated_at > NOW() - INTERVAL \'1 hour\''
                  : 'SELECT COUNT(*) FROM my_table'
              }
              rows={4}
            />
          </div>

          <div className="grid grid-cols-5 gap-3">
            <Input label="Max Retries" type="number" value={form.retries} onChange={e => setForm(f => ({ ...f, retries: e.target.value }))} />
            <Input label="Timeout (sec)" type="number" value={form.timeout} onChange={e => setForm(f => ({ ...f, timeout: e.target.value }))} />
            <Input label="SLA (minutes)" type="number" value={form.sla_minutes} onChange={e => setForm(f => ({ ...f, sla_minutes: e.target.value }))} placeholder="60" />
            <div>
              <span className="text-xs font-medium text-zinc-400 mb-1 block">Engine</span>
              <select value={form.engine} onChange={e => setForm(f => ({ ...f, engine: e.target.value }))}
                className="w-full h-9 px-2 text-xs rounded-lg border border-white/[0.06] bg-white/[0.03] text-zinc-300 outline-none focus:border-amber-500/50 cursor-pointer">
                <option value="auto">Auto</option>
                <option value="datafusion">DataFusion</option>
                <option value="duckdb">DuckDB</option>
                <option value="polars">Polars</option>
              </select>
            </div>
            <Input label="Tags" value={form.tags} onChange={e => setForm(f => ({ ...f, tags: e.target.value }))} placeholder="etl,prod" />
          </div>

          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" size="sm" onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleCreate} icon={<Plus className="w-3.5 h-3.5" />}>Create Job</Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
