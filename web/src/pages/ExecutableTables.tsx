import { useState, useEffect, useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Button } from '../components/ui/Button'
import { Input, Textarea } from '../components/ui/Input'
import { StatusDot } from '../components/ui/StatusDot'
import {
  Database, Zap, Clock, DollarSign, Shield, Play, Plus,
  ChevronDown, ChevronUp, Code2, BarChart3, X, GitBranch,
  History, AlertTriangle, RotateCcw, Eye, Edit3, Check,
  ArrowRight, Layers, GitCommit, Copy, Search, Tag,
  ChevronRight, Hash, Timer, Rows3, CircleDot,
  FlaskConical, ShoppingBag, FileCheck,
  Network, Bug, RefreshCw,
} from 'lucide-react'
import type {
  ExecutableTable, TransformVersion, DiffLine,
  RegressionResult, ProvenanceEvent, IcebergProperties, GateResult,
  ABTestResult, DataContract, ContractValidationResult, MarketplacePackage,
  ColumnLineageEntry, CascadeReplayResult, DebugResult,
} from '../types'

const API = '/api/v1'

const transformColors: Record<string, string> = {
  sql: 'text-amber-400 bg-amber-400/10 border-amber-400/20',
  rust: 'text-orange-400 bg-orange-400/10 border-orange-400/20',
  notebook: 'text-violet-400 bg-violet-400/10 border-violet-400/20',
  python: 'text-cyan-400 bg-cyan-400/10 border-cyan-400/20',
}

const eventColors: Record<string, string> = {
  code_change: 'border-violet-400 bg-violet-400/10',
  execution: 'border-amber-400 bg-amber-400/10',
  regression_detected: 'border-rose-400 bg-rose-400/10',
  rollback: 'border-cyan-400 bg-cyan-400/10',
}

const eventDotColors: Record<string, string> = {
  code_change: 'bg-violet-400',
  execution: 'bg-amber-400',
  regression_detected: 'bg-rose-400',
  rollback: 'bg-cyan-400',
}

function formatUsd(v: number): string {
  if (v < 0.001) return `$${v.toFixed(6)}`
  if (v < 1) return `$${v.toFixed(4)}`
  return `$${v.toFixed(2)}`
}

function formatMs(v: number): string {
  if (v >= 60000) return `${(v / 1000).toFixed(0)}s`
  if (v >= 1000) return `${(v / 1000).toFixed(1)}s`
  return `${v}ms`
}

function timeAgo(ts: string): string {
  const now = Date.now()
  const then = new Date(ts).getTime()
  const diff = now - then
  if (diff < 60000) return 'just now'
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`
  return `${Math.floor(diff / 86400000)}d ago`
}

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text).catch(() => {})
}

type RightPanelTab = 'versions' | 'diff' | 'history' | 'compare' | 'contracts' | 'lineage'

function DiffViewer({ lines, fromVersion, toVersion }: { lines: DiffLine[]; fromVersion: number; toVersion: number }) {
  const added = lines.filter(l => l.change_type === 'added').length
  const removed = lines.filter(l => l.change_type === 'removed').length
  const unchanged = lines.filter(l => l.change_type === 'unchanged').length

  return (
    <div className="rounded-lg border border-white/[0.06] overflow-hidden">
      <div className="px-4 py-2.5 bg-[#0a0f1e] border-b border-white/[0.06] flex items-center gap-3 text-xs text-zinc-400">
        <GitBranch className="w-3.5 h-3.5 text-violet-400" />
        <span className="font-mono text-violet-300">v{fromVersion}</span>
        <ArrowRight className="w-3 h-3 text-zinc-600" />
        <span className="font-mono text-violet-300">v{toVersion}</span>
        <div className="ml-auto flex items-center gap-3">
          <span className="text-emerald-400 font-mono">+{added}</span>
          <span className="text-rose-400 font-mono">-{removed}</span>
          <span className="text-zinc-600 font-mono">~{unchanged}</span>
        </div>
      </div>
      <pre className="text-xs overflow-x-auto max-h-[400px]">
        {lines.map((line, i) => (
          <div
            key={i}
            className={`px-4 py-0.5 font-mono ${
              line.change_type === 'added' ? 'bg-emerald-400/10 text-emerald-300' :
              line.change_type === 'removed' ? 'bg-rose-400/10 text-rose-300' :
              'text-zinc-500'
            }`}
          >
            <span className="inline-block w-8 text-right mr-3 text-zinc-600 select-none">
              {line.line_number}
            </span>
            <span className="inline-block w-4 mr-1 select-none font-bold">
              {line.change_type === 'added' ? '+' : line.change_type === 'removed' ? '-' : ' '}
            </span>
            {line.content}
          </div>
        ))}
      </pre>
    </div>
  )
}

function RegressionReport({ result }: { result: RegressionResult }) {
  const severityStyles: Record<string, string> = {
    none: 'text-emerald-400 border-emerald-400/20 bg-emerald-400/5',
    minor: 'text-amber-400 border-amber-400/20 bg-amber-400/5',
    major: 'text-rose-400 border-rose-400/20 bg-rose-400/5',
    critical: 'text-rose-400 border-rose-400/20 bg-rose-400/5 animate-pulse',
  }

  const bannerStyles: Record<string, { bg: string; text: string; label: string }> = {
    none: { bg: 'bg-emerald-400/10 border-emerald-400/20', text: 'text-emerald-400', label: 'Safe to Deploy' },
    minor: { bg: 'bg-amber-400/10 border-amber-400/20', text: 'text-amber-400', label: 'Review Required' },
    major: { bg: 'bg-rose-400/10 border-rose-400/20', text: 'text-rose-400', label: 'Review Required' },
    critical: { bg: 'bg-rose-500/20 border-rose-500/30', text: 'text-rose-400', label: 'DO NOT DEPLOY' },
  }

  const banner = bannerStyles[result.severity] || bannerStyles.none

  return (
    <div className="space-y-3">
      <div className={`rounded-lg border p-3 ${banner.bg}`}>
        <div className="flex items-center gap-2">
          {result.has_regression
            ? <AlertTriangle className={`w-4 h-4 ${banner.text}`} />
            : <Check className={`w-4 h-4 ${banner.text}`} />}
          <span className={`text-sm font-semibold ${banner.text}`}>{banner.label}</span>
          <Badge className={severityStyles[result.severity] || ''}>
            {result.severity.toUpperCase()}
          </Badge>
        </div>
        <p className="text-xs text-zinc-400 mt-1">{result.recommendation}</p>
      </div>

      {result.metrics.length > 0 && (
        <table className="w-full text-xs">
          <thead>
            <tr className="text-zinc-500 border-b border-white/[0.06]">
              <th className="text-left py-1.5">Metric</th>
              <th className="text-right py-1.5">Old</th>
              <th className="text-right py-1.5">New</th>
              <th className="text-right py-1.5">Change</th>
              <th className="text-center py-1.5">Status</th>
            </tr>
          </thead>
          <tbody>
            {result.metrics.map(m => (
              <tr key={m.metric_name} className="border-b border-white/[0.03]">
                <td className="py-1.5 text-zinc-300">{m.metric_name}</td>
                <td className="text-right py-1.5 font-mono text-zinc-400">{m.old_value.toLocaleString()}</td>
                <td className="text-right py-1.5 font-mono text-zinc-400">{m.new_value.toLocaleString()}</td>
                <td className={`text-right py-1.5 font-mono ${m.change_pct > 0 ? 'text-amber-400' : m.change_pct < 0 ? 'text-rose-400' : 'text-zinc-500'}`}>
                  {m.change_pct > 0 ? '+' : ''}{m.change_pct.toFixed(1)}%
                </td>
                <td className="text-center py-1.5">
                  {m.is_regression
                    ? <span className="text-rose-400">Regression</span>
                    : <span className="text-emerald-400">OK</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

function UpdateDialog({ table, onClose, onUpdated }: {
  table: ExecutableTable
  onClose: () => void
  onUpdated: () => void
}) {
  const [source, setSource] = useState(table.transform.source_code)
  const [description, setDescription] = useState('')
  const [diff, setDiff] = useState<DiffLine[] | null>(null)
  const [regression, setRegression] = useState<RegressionResult | null>(null)
  const [saving, setSaving] = useState(false)

  const previewDiff = async () => {
    try {
      const res = await fetch(`${API}/executable-tables/${table.table_name}/diff`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ old_source: table.transform.source_code, new_source: source }),
      })
      if (res.ok) {
        const data = await res.json()
        setDiff(data.diff || [])
      }
    } catch { /* ignore */ }
  }

  const checkRegression = async () => {
    const lastExec = table.history[table.history.length - 1]
    if (!lastExec) return
    try {
      const res = await fetch(`${API}/executable-tables/${table.table_name}/regression`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          old_rows: lastExec.rows_produced,
          new_rows: null,
          old_duration_ms: lastExec.duration_ms,
          new_duration_ms: 0,
          old_cost: lastExec.cost_usd,
          new_cost: 0,
        }),
      })
      if (res.ok) {
        const data = await res.json()
        setRegression(data.result)
      }
    } catch { /* ignore */ }
  }

  const deploy = async () => {
    setSaving(true)
    try {
      const res = await fetch(`${API}/executable-tables/${table.table_name}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source_code: source, change_description: description || undefined }),
      })
      if (res.ok) {
        const data = await res.json()
        if (data.regression) {
          setRegression(data.regression)
        }
        onUpdated()
        if (!data.regression?.has_regression) {
          onClose()
        }
      }
    } catch { /* ignore */ }
    setSaving(false)
  }

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-[#0c1222] border border-white/[0.06] rounded-xl w-full max-w-3xl max-h-[90vh] overflow-y-auto">
        <div className="sticky top-0 bg-[#0c1222] border-b border-white/[0.06] px-6 py-4 flex items-center justify-between z-10">
          <h2 className="text-lg font-semibold text-zinc-100 flex items-center gap-2">
            <Edit3 className="w-5 h-5 text-violet-400" />
            Edit Model: {table.table_name}
          </h2>
          <button onClick={onClose} className="text-zinc-500 hover:text-zinc-300"><X className="w-5 h-5" /></button>
        </div>

        <div className="p-6 space-y-4">
          <div>
            <Textarea
              label="Transform source code"
              rows={10}
              value={source}
              onChange={e => setSource(e.target.value)}
              className="font-mono text-xs"
            />
          </div>

          <Input
            label="Change description (optional)"
            placeholder="What changed and why?"
            value={description}
            onChange={e => setDescription(e.target.value)}
          />

          <div className="flex gap-2">
            <Button size="sm" variant="secondary" icon={<Eye className="w-3 h-3" />} onClick={previewDiff}>
              Preview Diff
            </Button>
            <Button size="sm" variant="secondary" icon={<AlertTriangle className="w-3 h-3" />} onClick={checkRegression}>
              Check Regression
            </Button>
          </div>

          {diff && diff.length > 0 && (
            <DiffViewer
              lines={diff}
              fromVersion={table.versions?.length ? Math.max(...table.versions.map(v => v.version)) : 1}
              toVersion={(table.versions?.length ? Math.max(...table.versions.map(v => v.version)) : 1) + 1}
            />
          )}
          {diff && diff.length === 0 && (
            <div className="text-xs text-zinc-500 italic py-2">No changes between versions.</div>
          )}

          {regression && <RegressionReport result={regression} />}

          <div className="flex justify-end gap-2 pt-2 border-t border-white/[0.06]">
            <Button variant="secondary" onClick={onClose}>Cancel</Button>
            <Button
              variant="primary"
              loading={saving}
              onClick={deploy}
              icon={<Zap className="w-3 h-3" />}
              disabled={source === table.transform.source_code}
            >
              Deploy Update
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}

function CreateFormModal({ onCreated, onClose, initialSql, initialType }: {
  onCreated: () => void
  onClose: () => void
  initialSql?: string
  initialType?: string
}) {
  const [name, setName] = useState('')
  const [transformType, setTransformType] = useState(initialType || 'sql')
  const [source, setSource] = useState(initialSql || '')
  const [schedule, setSchedule] = useState('')
  const [inputs, setInputs] = useState('')
  const [gates, setGates] = useState<Array<{ gate_type: string; column: string; description: string }>>([])
  const [saving, setSaving] = useState(false)

  const addGate = () => setGates([...gates, { gate_type: 'not_null', column: '', description: '' }])
  const removeGate = (i: number) => setGates(gates.filter((_, idx) => idx !== i))

  const handleCreate = async () => {
    if (!name || !source) return
    setSaving(true)
    try {
      const table: ExecutableTable = {
        table_name: name,
        table_location: `s3://warehouse/${name}`,
        transform: {
          transform_type: transformType,
          source_code: source,
          source_hash: '',
          binary_path: null,
          binary_size: null,
          binary_cached: false,
        },
        schedule: schedule || null,
        quality_gates: gates.map(g => ({
          gate_type: g.gate_type,
          column: g.column || null,
          description: g.description || `${g.gate_type} check`,
        })),
        input_tables: inputs.split(',').map(s => s.trim()).filter(Boolean),
        status: { state: 'active', health: 'healthy', staleness_hours: 0, data_freshness: 'unknown' },
        history: [],
        created_at: new Date().toISOString(),
        last_refresh: null,
        next_refresh: null,
        estimated_cost_usd: 0.001,
        total_executions: 0,
        total_cost_usd: 0,
        executions_skipped: 0,
        cost_saved_usd: 0,
      }
      const res = await fetch(`${API}/executable-tables`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(table),
      })
      if (res.ok) {
        onCreated()
        onClose()
      }
    } catch { /* ignore */ }
    setSaving(false)
  }

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-[#0c1222] border border-white/[0.06] rounded-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        <div className="sticky top-0 bg-[#0c1222] border-b border-white/[0.06] px-6 py-4 flex items-center justify-between z-10">
          <h2 className="text-lg font-semibold text-zinc-100 flex items-center gap-2">
            <Plus className="w-5 h-5 text-amber-400" />
            Create Data Model
          </h2>
          <button onClick={onClose} className="text-zinc-500 hover:text-zinc-300"><X className="w-5 h-5" /></button>
        </div>

        <div className="p-6 space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <Input label="Model name" placeholder="orders_daily" value={name} onChange={e => setName(e.target.value)} />
            <div className="flex flex-col gap-1.5">
              <span className="text-xs font-medium text-zinc-400">Transform type</span>
              <select
                value={transformType}
                onChange={e => setTransformType(e.target.value)}
                className="px-3 py-2 text-sm rounded-lg bg-navy-900/60 border border-white/[0.06] text-zinc-100 focus:outline-none focus:ring-1 focus:ring-amber-400/25"
              >
                <option value="sql">SQL</option>
                <option value="rust">Rust</option>
              </select>
            </div>
          </div>

          <Textarea
            label="SQL / Rust source"
            rows={6}
            placeholder={transformType === 'sql' ? 'SELECT * FROM raw_orders WHERE ...' : 'fn main() { ... }'}
            value={source}
            onChange={e => setSource(e.target.value)}
            className="font-mono text-xs"
          />

          <div className="grid grid-cols-2 gap-4">
            <Input label="Schedule (cron)" placeholder="0 * * * * (hourly)" value={schedule} onChange={e => setSchedule(e.target.value)} />
            <Input label="Input tables (comma-separated)" placeholder="raw_orders, dim_products" value={inputs} onChange={e => setInputs(e.target.value)} />
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium text-zinc-400 flex items-center gap-1"><Shield className="w-3 h-3" /> Quality Gates</span>
              <button onClick={addGate} className="text-xs text-amber-400 hover:text-amber-300">+ Add gate</button>
            </div>
            {gates.map((g, i) => (
              <div key={i} className="flex gap-2 mb-2">
                <select
                  value={g.gate_type}
                  onChange={e => { const ng = [...gates]; ng[i] = { ...ng[i], gate_type: e.target.value }; setGates(ng) }}
                  className="px-2 py-1.5 text-xs rounded-lg bg-navy-900/60 border border-white/[0.06] text-zinc-100"
                >
                  <option value="not_null">Not Null</option>
                  <option value="unique">Unique</option>
                  <option value="range">Range</option>
                  <option value="row_count">Row Count</option>
                  <option value="custom_sql">Custom SQL</option>
                </select>
                <input
                  value={g.column}
                  onChange={e => { const ng = [...gates]; ng[i] = { ...ng[i], column: e.target.value }; setGates(ng) }}
                  placeholder="Column"
                  className="flex-1 px-2 py-1.5 text-xs rounded-lg bg-navy-900/60 border border-white/[0.06] text-zinc-100"
                />
                <input
                  value={g.description}
                  onChange={e => { const ng = [...gates]; ng[i] = { ...ng[i], description: e.target.value }; setGates(ng) }}
                  placeholder="Description"
                  className="flex-1 px-2 py-1.5 text-xs rounded-lg bg-navy-900/60 border border-white/[0.06] text-zinc-100"
                />
                <button onClick={() => removeGate(i)} className="text-zinc-500 hover:text-rose-400"><X className="w-3 h-3" /></button>
              </div>
            ))}
          </div>

          <div className="flex justify-end gap-2 pt-2 border-t border-white/[0.06]">
            <Button variant="secondary" onClick={onClose}>Cancel</Button>
            <Button variant="primary" loading={saving} onClick={handleCreate} icon={<Zap className="w-4 h-4" />} disabled={!name || !source}>
              Create Model
            </Button>
          </div>
        </div>
      </div>
    </div>
  )
}

function CommitsTab({ table, onRefresh }: { table: ExecutableTable; onRefresh: () => void }) {
  const [versions, setVersions] = useState<TransformVersion[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [executingVersion, setExecutingVersion] = useState<number | null>(null)
  const [versionResults, setVersionResults] = useState<Record<number, { status: string; duration_ms: number; rows_produced?: number | null; binary_cached: boolean; note: string; files_written?: number | null; bytes_written?: number | null; gate_results?: GateResult[]; regression?: RegressionResult | null }>>({})
  const [rollingBack, setRollingBack] = useState<number | null>(null)
  const [confirmRollback, setConfirmRollback] = useState<number | null>(null)

  useEffect(() => {
    setLoading(true)
    fetch(`${API}/executable-tables/${table.table_name}/versions`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data) setVersions(data.versions || []) })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [table.table_name, table.total_executions])

  const headVersion = versions ? Math.max(...versions.map(v => v.version), 0) : 0

  const executeVersion = async (version: number) => {
    setExecutingVersion(version)
    try {
      const res = await fetch(`${API}/executable-tables/${table.table_name}/execute-version`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version }),
      })
      if (res.ok) {
        const data = await res.json()
        setVersionResults(prev => ({ ...prev, [version]: data }))
        onRefresh()
      }
    } catch { /* ignore */ }
    setExecutingVersion(null)
  }

  const rollback = async (version: number) => {
    setRollingBack(version)
    try {
      const res = await fetch(`${API}/executable-tables/${table.table_name}/rollback`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version }),
      })
      if (res.ok) {
        onRefresh()
        const versRes = await fetch(`${API}/executable-tables/${table.table_name}/versions`)
        if (versRes.ok) {
          const data = await versRes.json()
          setVersions(data.versions || [])
        }
      }
    } catch { /* ignore */ }
    setRollingBack(null)
    setConfirmRollback(null)
  }

  if (loading) {
    return <div className="py-12 text-center text-sm text-zinc-500">Loading commit history...</div>
  }

  if (!versions || versions.length === 0) {
    return (
      <div className="py-12 text-center">
        <GitCommit className="w-8 h-8 text-zinc-600 mx-auto mb-2" />
        <p className="text-sm text-zinc-400">No commits yet</p>
        <p className="text-xs text-zinc-600 mt-1">Edit the transform to create the first version</p>
      </div>
    )
  }

  const sorted = [...versions].sort((a, b) => b.version - a.version)

  return (
    <div className="space-y-0">
      <div className="relative pl-8">
        <div className="absolute left-3 top-2 bottom-2 w-px bg-violet-400/20" />

        {sorted.map((v) => {
          const isHead = v.version === headVersion
          const result = versionResults[v.version]

          return (
            <div key={v.version} className="relative mb-4">
              <div className={`absolute left-[-20px] top-4 w-3 h-3 rounded-full border-2 ${
                isHead ? 'bg-amber-400 border-amber-400' : 'bg-[#0a0f1e] border-violet-400'
              }`} />

              <div className={`rounded-lg border p-4 transition-all ${
                isHead
                  ? 'border-amber-400/20 bg-amber-400/[0.03]'
                  : 'border-white/[0.06] bg-white/[0.01] hover:border-white/[0.1]'
              }`}>
                <div className="flex items-start gap-3">
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <Badge className="text-violet-400 border-violet-400/20 font-mono">
                        v{v.version}
                      </Badge>
                      {isHead && (
                        <Badge className="text-amber-400 border-amber-400/20 bg-amber-400/10 font-semibold">
                          HEAD
                        </Badge>
                      )}
                      <span className="text-sm text-zinc-200 font-medium truncate">
                        {v.change_description || 'Initial version'}
                      </span>
                    </div>

                    <div className="flex items-center gap-3 text-xs text-zinc-500 mt-1.5">
                      <span className="flex items-center gap-1">
                        <Hash className="w-3 h-3" />
                        <span className="font-mono text-zinc-400">{v.source_hash.slice(0, 8)}</span>
                        <button
                          onClick={() => copyToClipboard(v.source_hash)}
                          className="text-zinc-600 hover:text-zinc-400 transition-colors"
                        >
                          <Copy className="w-2.5 h-2.5" />
                        </button>
                      </span>
                      <span className="flex items-center gap-1">
                        <CircleDot className="w-3 h-3" />
                        {v.created_by}
                      </span>
                      <span className="flex items-center gap-1">
                        <Clock className="w-3 h-3" />
                        {timeAgo(v.created_at)}
                      </span>
                      {v.binary_size_bytes != null && (
                        <span className="text-zinc-600">
                          {(v.binary_size_bytes / 1024).toFixed(0)} KB
                        </span>
                      )}
                    </div>

                    {v.snapshot_ids.length > 0 && (
                      <div className="flex items-center gap-1.5 mt-2">
                        <Tag className="w-3 h-3 text-cyan-400" />
                        {v.snapshot_ids.map(sid => (
                          <Badge key={sid} className="text-cyan-400 border-cyan-400/20 text-[10px] font-mono">
                            snap:{sid}
                          </Badge>
                        ))}
                      </div>
                    )}
                  </div>

                  <div className="flex items-center gap-1.5 shrink-0">
                    <Button
                      size="sm"
                      variant="primary"
                      icon={<Play className="w-3 h-3" />}
                      loading={executingVersion === v.version}
                      onClick={() => executeVersion(v.version)}
                    >
                      Run
                    </Button>
                    {!isHead && (
                      <>
                        {confirmRollback === v.version ? (
                          <div className="flex items-center gap-1">
                            <Button
                              size="sm"
                              variant="danger"
                              loading={rollingBack === v.version}
                              onClick={() => rollback(v.version)}
                            >
                              Confirm
                            </Button>
                            <Button size="sm" variant="ghost" onClick={() => setConfirmRollback(null)}>
                              Cancel
                            </Button>
                          </div>
                        ) : (
                          <Button
                            size="sm"
                            variant="secondary"
                            icon={<RotateCcw className="w-3 h-3" />}
                            onClick={() => setConfirmRollback(v.version)}
                            className="text-cyan-400 border-cyan-400/20 hover:border-cyan-400/40"
                          >
                            Rollback
                          </Button>
                        )}
                      </>
                    )}
                  </div>
                </div>

                {result && (
                  <div className={`mt-3 pt-3 border-t ${
                    result.status === 'success' ? 'border-emerald-400/10' : 'border-rose-400/10'
                  }`}>
                    <div className="flex items-center gap-3 text-xs">
                      <Badge className={result.status === 'success' ? 'text-emerald-400 border-emerald-400/20 bg-emerald-400/10' : 'text-rose-400 border-rose-400/20 bg-rose-400/10'}>
                        {result.status === 'success' ? <Check className="w-2.5 h-2.5 mr-1" /> : <X className="w-2.5 h-2.5 mr-1" />}
                        {result.status}
                      </Badge>
                      <span className="flex items-center gap-1 text-zinc-400">
                        <Timer className="w-3 h-3" />
                        {formatMs(result.duration_ms)}
                      </span>
                      {result.rows_produced != null && (
                        <span className="flex items-center gap-1 text-zinc-400">
                          <Rows3 className="w-3 h-3" />
                          {result.rows_produced?.toLocaleString()} rows
                        </span>
                      )}
                      {result.binary_cached && <Badge className="text-emerald-400 border-emerald-400/20">cached</Badge>}
                      {result.files_written != null && (
                        <span className="flex items-center gap-1 text-zinc-500">
                          <Layers className="w-3 h-3" />
                          {result.files_written} file{result.files_written !== 1 ? 's' : ''}
                          {result.bytes_written != null && ` (${(result.bytes_written / 1024).toFixed(1)} KB)`}
                        </span>
                      )}
                      <span className="text-zinc-600 ml-auto">{result.note}</span>
                    </div>

                    {/* Quality gate results */}
                    {result.gate_results && result.gate_results.length > 0 && (
                      <div className="flex flex-wrap gap-1.5 mt-2">
                        {result.gate_results.map((g, gi) => (
                          <Badge key={gi} className={g.passed
                            ? 'text-emerald-400 border-emerald-400/20 bg-emerald-400/5'
                            : 'text-rose-400 border-rose-400/20 bg-rose-400/5'
                          }>
                            <Shield className="w-2.5 h-2.5 mr-1" />
                            {g.gate_type}{g.column ? `.${g.column}` : ''}: {g.passed ? 'pass' : 'fail'}
                          </Badge>
                        ))}
                      </div>
                    )}

                    {/* Auto-regression warning */}
                    {result.regression?.has_regression && (
                      <div className="mt-2 rounded border border-rose-400/20 bg-rose-400/5 px-3 py-2">
                        <div className="flex items-center gap-2 text-xs">
                          <AlertTriangle className="w-3.5 h-3.5 text-rose-400" />
                          <span className="text-rose-400 font-semibold">
                            Regression: {result.regression.severity.toUpperCase()}
                          </span>
                        </div>
                        <p className="text-[11px] text-zinc-400 mt-1">{result.regression.recommendation}</p>
                      </div>
                    )}
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

function DiffTab({ table }: { table: ExecutableTable }) {
  const [versions, setVersions] = useState<TransformVersion[] | null>(null)
  const [fromVer, setFromVer] = useState<number>(0)
  const [toVer, setToVer] = useState<number>(0)
  const [diffLines, setDiffLines] = useState<DiffLine[] | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    fetch(`${API}/executable-tables/${table.table_name}/versions`)
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data && data.versions) {
          const vers = data.versions as TransformVersion[]
          setVersions(vers)
          if (vers.length >= 2) {
            const sorted = [...vers].sort((a, b) => a.version - b.version)
            setFromVer(sorted[sorted.length - 2].version)
            setToVer(sorted[sorted.length - 1].version)
          }
        }
      })
      .catch(() => {})
  }, [table.table_name])

  const computeDiff = async () => {
    if (!versions || fromVer === toVer) return
    const fromV = versions.find(v => v.version === fromVer)
    const toV = versions.find(v => v.version === toVer)
    if (!fromV || !toV) return

    setLoading(true)
    try {
      const res = await fetch(`${API}/executable-tables/${table.table_name}/diff`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ old_source: fromV.source_code, new_source: toV.source_code }),
      })
      if (res.ok) {
        const data = await res.json()
        setDiffLines(data.diff || [])
      }
    } catch { /* ignore */ }
    setLoading(false)
  }

  useEffect(() => {
    if (fromVer && toVer && fromVer !== toVer && versions) {
      computeDiff()
    }
  }, [fromVer, toVer])

  if (!versions || versions.length < 2) {
    return (
      <div className="py-12 text-center">
        <GitBranch className="w-8 h-8 text-zinc-600 mx-auto mb-2" />
        <p className="text-sm text-zinc-400">Need at least 2 versions to compare</p>
      </div>
    )
  }

  const sorted = [...versions].sort((a, b) => a.version - b.version)

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 flex-1">
          <span className="text-xs text-zinc-500">Compare:</span>
          <select
            value={fromVer}
            onChange={e => setFromVer(+e.target.value)}
            className="px-3 py-1.5 text-xs rounded-lg bg-navy-900/60 border border-white/[0.06] text-zinc-100 font-mono"
          >
            {sorted.map(v => (
              <option key={v.version} value={v.version}>v{v.version} — {v.change_description || 'Initial'}</option>
            ))}
          </select>
          <ArrowRight className="w-4 h-4 text-zinc-600" />
          <select
            value={toVer}
            onChange={e => setToVer(+e.target.value)}
            className="px-3 py-1.5 text-xs rounded-lg bg-navy-900/60 border border-white/[0.06] text-zinc-100 font-mono"
          >
            {sorted.map(v => (
              <option key={v.version} value={v.version}>v{v.version} — {v.change_description || 'Initial'}</option>
            ))}
          </select>
        </div>
      </div>

      {loading && <div className="py-8 text-center text-sm text-zinc-500">Computing diff...</div>}

      {diffLines && diffLines.length > 0 && (
        <DiffViewer lines={diffLines} fromVersion={fromVer} toVersion={toVer} />
      )}

      {diffLines && diffLines.length === 0 && (
        <div className="py-8 text-center">
          <Check className="w-6 h-6 text-emerald-400 mx-auto mb-2" />
          <p className="text-sm text-zinc-400">No changes between these versions</p>
        </div>
      )}
    </div>
  )
}

function TimelineTab({ table }: { table: ExecutableTable }) {
  const [events, setEvents] = useState<ProvenanceEvent[] | null>(null)
  const [loading, setLoading] = useState(true)
  const [expandedIdx, setExpandedIdx] = useState<number | null>(null)

  useEffect(() => {
    setLoading(true)
    fetch(`${API}/executable-tables/${table.table_name}/provenance`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data) setEvents(data.timeline || []) })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [table.table_name, table.total_executions])

  if (loading) {
    return <div className="py-12 text-center text-sm text-zinc-500">Loading timeline...</div>
  }

  if (!events || events.length === 0) {
    return (
      <div className="py-12 text-center">
        <History className="w-8 h-8 text-zinc-600 mx-auto mb-2" />
        <p className="text-sm text-zinc-400">No events recorded yet</p>
      </div>
    )
  }

  return (
    <div className="relative pl-8">
      <div className="absolute left-3 top-2 bottom-2 w-px bg-white/[0.06]" />
      {events.map((event, i) => (
        <div key={i} className="relative mb-3">
          <div className={`absolute left-[-20px] top-3 w-2.5 h-2.5 rounded-full ${eventDotColors[event.event_type] || 'bg-zinc-500'}`} />
          <div
            className={`rounded-lg border p-3 cursor-pointer transition-all hover:border-white/[0.12] ${eventColors[event.event_type] || 'border-zinc-700'}`}
            onClick={() => setExpandedIdx(expandedIdx === i ? null : i)}
          >
            <div className="flex items-center gap-2">
              <Badge className={`text-xs ${
                event.event_type === 'code_change' ? 'text-violet-400 border-violet-400/20' :
                event.event_type === 'execution' ? 'text-amber-400 border-amber-400/20' :
                event.event_type === 'rollback' ? 'text-cyan-400 border-cyan-400/20' :
                'text-rose-400 border-rose-400/20'
              }`}>
                {event.event_type === 'code_change' && <Code2 className="w-2.5 h-2.5 mr-0.5" />}
                {event.event_type === 'execution' && <Play className="w-2.5 h-2.5 mr-0.5" />}
                {event.event_type === 'rollback' && <RotateCcw className="w-2.5 h-2.5 mr-0.5" />}
                {event.event_type === 'regression_detected' && <AlertTriangle className="w-2.5 h-2.5 mr-0.5" />}
                {event.event_type.replace('_', ' ')}
              </Badge>
              <span className="text-xs font-mono text-violet-400/60">v{event.version}</span>
              <span className="text-xs text-zinc-400 flex-1 truncate">{event.description}</span>
              <span className="text-[10px] text-zinc-600 shrink-0">{timeAgo(event.timestamp)}</span>
              <ChevronRight className={`w-3 h-3 text-zinc-600 transition-transform ${expandedIdx === i ? 'rotate-90' : ''}`} />
            </div>
            {expandedIdx === i && (
              <div className="mt-3 pt-3 border-t border-white/[0.06] text-xs text-zinc-500 space-y-1.5">
                <div className="flex items-center gap-2">
                  <Hash className="w-3 h-3" />
                  <span>Hash:</span>
                  <span className="font-mono text-zinc-400">{event.source_hash}</span>
                  <button onClick={() => copyToClipboard(event.source_hash)} className="text-zinc-600 hover:text-zinc-400">
                    <Copy className="w-2.5 h-2.5" />
                  </button>
                </div>
                <div className="flex items-center gap-2">
                  <Clock className="w-3 h-3" />
                  <span>Timestamp:</span>
                  <span className="text-zinc-300">{new Date(event.timestamp).toLocaleString()}</span>
                </div>
                {event.duration_ms != null && (
                  <div className="flex items-center gap-2">
                    <Timer className="w-3 h-3" />
                    <span>Duration:</span>
                    <span className="text-zinc-300">{formatMs(event.duration_ms)}</span>
                  </div>
                )}
                {event.rows_produced != null && (
                  <div className="flex items-center gap-2">
                    <Rows3 className="w-3 h-3" />
                    <span>Rows:</span>
                    <span className="text-zinc-300">{event.rows_produced?.toLocaleString()}</span>
                  </div>
                )}
                {event.cost_usd != null && (
                  <div className="flex items-center gap-2">
                    <DollarSign className="w-3 h-3" />
                    <span>Cost:</span>
                    <span className="text-zinc-300">{formatUsd(event.cost_usd)}</span>
                  </div>
                )}
                {event.binary_cached && <Badge className="text-emerald-400 border-emerald-400/20">cached binary</Badge>}
              </div>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}

function IcebergTab({ name }: { name: string }) {
  const [props, setProps] = useState<IcebergProperties | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    fetch(`${API}/executable-tables/${name}/properties`)
      .then(r => r.ok ? r.json() : null)
      .then(data => setProps(data))
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [name])

  if (loading) {
    return <div className="py-12 text-center text-sm text-zinc-500">Loading Iceberg metadata...</div>
  }

  if (!props) {
    return (
      <div className="py-12 text-center">
        <Layers className="w-8 h-8 text-zinc-600 mx-auto mb-2" />
        <p className="text-sm text-zinc-400">No Iceberg properties available</p>
      </div>
    )
  }

  const sortedEntries = Object.entries(props.properties).sort(([a], [b]) => a.localeCompare(b))

  return (
    <div className="space-y-5">
      <div className="flex items-center gap-3 flex-wrap">
        <Badge className="text-violet-400 border-violet-400/20 bg-violet-400/10">
          Format v{props.format_version}
        </Badge>
        {props.compatible_engines.map(eng => (
          <Badge key={eng} className="text-cyan-400 border-cyan-400/20">
            {eng}
          </Badge>
        ))}
      </div>

      <div className="rounded-lg border border-white/[0.06] overflow-hidden">
        <div className="px-4 py-2.5 bg-[#0a0f1e] border-b border-white/[0.06] text-xs text-zinc-400 font-medium">
          Properties ({sortedEntries.length})
        </div>
        <div className="divide-y divide-white/[0.03]">
          {sortedEntries.map(([key, value]) => (
            <div key={key} className="flex items-center px-4 py-2 text-xs hover:bg-white/[0.02]">
              <span className="text-violet-400/80 font-mono w-[45%] truncate">{key}</span>
              <span className="text-zinc-300 font-mono truncate flex-1">{value}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function ABTestTab({ table }: { table: ExecutableTable }) {
  const [verA, setVerA] = useState(1)
  const [verB, setVerB] = useState(2)
  const [result, setResult] = useState<ABTestResult | null>(null)
  const [loading, setLoading] = useState(false)
  const versions = table.versions || []

  const runTest = async () => {
    setLoading(true)
    try {
      const res = await fetch(`${API}/executable-tables/${table.table_name}/ab-test`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ version_a: verA, version_b: verB }),
      })
      if (res.ok) setResult(await res.json())
    } catch { /* ignore */ }
    setLoading(false)
  }

  useEffect(() => {
    if (versions.length >= 2) {
      const sorted = [...versions].sort((a, b) => a.version - b.version)
      setVerA(sorted[sorted.length - 2].version)
      setVerB(sorted[sorted.length - 1].version)
    }
  }, [versions.length])

  return (
    <div className="space-y-4 p-1">
      <div className="flex items-center gap-3">
        <select className="bg-[#0a0f1e] border border-white/[0.1] rounded px-2 py-1.5 text-sm text-zinc-300" value={verA} onChange={e => setVerA(+e.target.value)}>
          {versions.map(v => <option key={v.version} value={v.version}>v{v.version} — {v.change_description}</option>)}
        </select>
        <span className="text-zinc-500 text-sm">vs</span>
        <select className="bg-[#0a0f1e] border border-white/[0.1] rounded px-2 py-1.5 text-sm text-zinc-300" value={verB} onChange={e => setVerB(+e.target.value)}>
          {versions.map(v => <option key={v.version} value={v.version}>v{v.version} — {v.change_description}</option>)}
        </select>
        <Button size="sm" variant="primary" icon={<FlaskConical className="w-3 h-3" />} loading={loading} onClick={runTest} disabled={verA === verB}>
          Run A/B Test
        </Button>
      </div>

      {result && (
        <div className="space-y-3">
          {/* Winner banner */}
          <div className={`rounded-lg border p-3 ${result.winner === 'tie' ? 'border-amber-400/20 bg-amber-400/5' : 'border-emerald-400/20 bg-emerald-400/5'}`}>
            <div className="flex items-center gap-2">
              <FlaskConical className={`w-4 h-4 ${result.winner === 'tie' ? 'text-amber-400' : 'text-emerald-400'}`} />
              <span className={`text-sm font-semibold ${result.winner === 'tie' ? 'text-amber-400' : 'text-emerald-400'}`}>
                {result.winner === 'tie' ? 'Tie' : `Winner: v${result.winner === 'version_a' ? result.version_a : result.version_b}`}
              </span>
              <Badge className="text-cyan-400 border-cyan-400/20">{(result.confidence * 100).toFixed(0)}% confidence</Badge>
            </div>
            <p className="text-xs text-zinc-400 mt-1">{result.recommendation}</p>
          </div>

          {/* Side-by-side metrics */}
          <div className="grid grid-cols-2 gap-3">
            {[
              { label: `v${result.version_a}`, m: result.version_a_metrics, isWinner: result.winner === 'version_a' },
              { label: `v${result.version_b}`, m: result.version_b_metrics, isWinner: result.winner === 'version_b' },
            ].map(({ label, m, isWinner }) => (
              <div key={label} className={`rounded-lg border p-3 ${isWinner ? 'border-emerald-400/20 bg-emerald-400/5' : 'border-white/[0.06] bg-white/[0.01]'}`}>
                <div className="flex items-center gap-2 mb-2">
                  <Badge className={isWinner ? 'text-emerald-400 border-emerald-400/20' : 'text-zinc-400 border-white/[0.1]'}>{label}</Badge>
                  {isWinner && <span className="text-[10px] text-emerald-400">WINNER</span>}
                </div>
                <div className="space-y-1 text-xs">
                  <div className="flex justify-between"><span className="text-zinc-500">Rows</span><span className="text-zinc-300 font-mono">{m.rows_produced.toLocaleString()}</span></div>
                  <div className="flex justify-between"><span className="text-zinc-500">Duration</span><span className="text-zinc-300 font-mono">{formatMs(m.duration_ms)}</span></div>
                  <div className="flex justify-between"><span className="text-zinc-500">Cost</span><span className="text-zinc-300 font-mono">{formatUsd(m.cost_usd)}</span></div>
                  <div className="flex justify-between"><span className="text-zinc-500">Columns</span><span className="text-zinc-300 font-mono">{m.schema_columns.length}</span></div>
                </div>
              </div>
            ))}
          </div>

          {/* Schema comparison */}
          {!result.comparison.schema_match && (
            <div className="rounded border border-amber-400/20 bg-amber-400/5 p-3">
              <p className="text-xs text-amber-400 font-semibold mb-1">Schema Differences</p>
              {result.comparison.columns_added.length > 0 && (
                <p className="text-xs text-emerald-400">+ Added: {result.comparison.columns_added.join(', ')}</p>
              )}
              {result.comparison.columns_removed.length > 0 && (
                <p className="text-xs text-rose-400">- Removed: {result.comparison.columns_removed.join(', ')}</p>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function ContractsTab({ table }: { table: ExecutableTable }) {
  const [contracts, setContracts] = useState<DataContract[]>([])
  const [loading, setLoading] = useState(true)
  const [validating, setValidating] = useState<string | null>(null)
  const [results, setResults] = useState<Record<string, ContractValidationResult>>({})

  useEffect(() => {
    fetch(`${API}/data-contracts`)
      .then(r => r.ok ? r.json() : null)
      .then(data => {
        if (data) {
          const relevant = (data.contracts || []).filter((c: DataContract) =>
            c.producer_table === table.table_name || c.consumer_tables.includes(table.table_name)
          )
          setContracts(relevant)
        }
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [table.table_name])

  const validate = async (id: string) => {
    setValidating(id)
    try {
      const res = await fetch(`${API}/data-contracts/${id}/validate`, { method: 'POST' })
      if (res.ok) {
        const result = await res.json()
        setResults(prev => ({ ...prev, [id]: result }))
      }
    } catch { /* ignore */ }
    setValidating(null)
  }

  if (loading) return <div className="py-8 text-center text-sm text-zinc-500">Loading contracts...</div>

  return (
    <div className="space-y-3 p-1">
      {contracts.length === 0 ? (
        <div className="py-8 text-center">
          <FileCheck className="w-8 h-8 text-zinc-600 mx-auto mb-2" />
          <p className="text-sm text-zinc-400">No data contracts</p>
          <p className="text-xs text-zinc-600 mt-1">Create contracts via API: POST /api/v1/data-contracts</p>
        </div>
      ) : contracts.map(c => (
        <div key={c.id} className="rounded-lg border border-white/[0.06] p-3 space-y-2">
          <div className="flex items-center gap-2">
            <FileCheck className="w-4 h-4 text-violet-400" />
            <span className="text-sm text-zinc-200 font-medium">{c.producer_table}</span>
            <ArrowRight className="w-3 h-3 text-zinc-500" />
            <span className="text-sm text-zinc-400">{c.consumer_tables.join(', ')}</span>
            <Badge className={c.status === 'passing' ? 'text-emerald-400 border-emerald-400/20' : c.status === 'failing' ? 'text-rose-400 border-rose-400/20' : 'text-zinc-500 border-white/[0.1]'}>
              {c.status}
            </Badge>
            <Button size="sm" variant="secondary" loading={validating === c.id} onClick={() => validate(c.id)} className="ml-auto">
              Validate
            </Button>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {c.schema_checks.map((sc, i) => (
              <Badge key={i} className="text-zinc-400 border-white/[0.06] text-[10px]">
                {sc.column}: {sc.data_type} {sc.required ? '(required)' : ''}
              </Badge>
            ))}
          </div>
          {results[c.id] && (
            <div className={`rounded border p-2 mt-1 ${results[c.id].passed ? 'border-emerald-400/20 bg-emerald-400/5' : 'border-rose-400/20 bg-rose-400/5'}`}>
              <p className={`text-xs font-semibold ${results[c.id].passed ? 'text-emerald-400' : 'text-rose-400'}`}>
                {results[c.id].passed ? 'All checks passed' : `${results[c.id].violations.length} violation(s)`}
              </p>
              {results[c.id].violations.map((v, i) => (
                <p key={i} className="text-[11px] text-rose-300 mt-0.5">{v.check_type}: {v.column} — expected {v.expected}, got {v.actual}</p>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  )
}

function TableListItem({ table, selected, onClick, onCascade }: {
  table: ExecutableTable
  selected: boolean
  onClick: () => void
  onCascade: (name: string) => void
}) {
  const versionCount = table.versions?.length || 1
  const healthMap: Record<string, 'healthy' | 'warning' | 'error' | 'idle'> = {
    healthy: 'healthy', warning: 'warning', critical: 'error',
  }

  return (
    <div
      onClick={onClick}
      className={`px-4 py-3 cursor-pointer transition-all border-l-2 ${
        selected
          ? 'bg-amber-400/[0.05] border-l-amber-400'
          : 'border-l-transparent hover:bg-white/[0.02]'
      }`}
    >
      <div className="flex items-center gap-2 mb-1">
        <Database className={`w-3.5 h-3.5 ${selected ? 'text-amber-400' : 'text-zinc-500'}`} />
        <span className={`text-sm font-medium truncate ${selected ? 'text-zinc-100' : 'text-zinc-300'}`}>
          {table.table_name}
        </span>
        <StatusDot status={healthMap[table.status.health] || 'idle'} />
        <Button size="sm" variant="ghost" onClick={(e) => { e.stopPropagation(); onCascade(table.table_name) }} title="Re-derive: replay entire upstream DAG" className="ml-auto">
          <RefreshCw className="w-3.5 h-3.5" />
        </Button>
      </div>
      <div className="flex items-center gap-3 ml-5.5 text-[10px] text-zinc-500">
        <Badge className={`${transformColors[table.transform.transform_type] || 'text-zinc-400'} py-0 text-[10px]`}>
          {table.transform.transform_type.toUpperCase()}
        </Badge>
        <span className="flex items-center gap-0.5">
          <GitBranch className="w-2.5 h-2.5" />v{versionCount}
        </span>
        <span className="flex items-center gap-0.5">
          <Play className="w-2.5 h-2.5" />{table.total_executions}
        </span>
        {table.last_refresh && (
          <span className="truncate">{timeAgo(table.last_refresh)}</span>
        )}
        {table.executions_skipped > 0 && (
          <span className="text-[10px] text-emerald-400">Skipped: {table.executions_skipped} | Saved: ${table.cost_saved_usd.toFixed(4)}</span>
        )}
      </div>
    </div>
  )
}

function SelectedTableMeta({ table, onEdit }: { table: ExecutableTable; onEdit: () => void }) {
  const healthMap: Record<string, 'healthy' | 'warning' | 'error' | 'idle'> = {
    healthy: 'healthy', warning: 'warning', critical: 'error',
  }
  const versionCount = table.versions?.length || 1

  return (
    <div className="p-4 border-b border-white/[0.06]">
      <div className="flex items-center gap-2 mb-3">
        <Database className="w-4 h-4 text-amber-400" />
        <h3 className="text-base font-semibold text-zinc-100 truncate">{table.table_name}</h3>
        <Badge className={transformColors[table.transform.transform_type] || 'text-zinc-400'}>
          {table.transform.transform_type.toUpperCase()}
        </Badge>
        <StatusDot status={healthMap[table.status.health] || 'idle'} label={table.status.state} />
        <Button size="sm" variant="secondary" icon={<Edit3 className="w-3 h-3" />} onClick={onEdit} className="ml-auto">
          Edit
        </Button>
      </div>

      <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs">
        <div className="flex items-center gap-2 text-zinc-400">
          <GitBranch className="w-3 h-3 text-violet-400" />
          <span>Versions: <span className="text-zinc-200">{versionCount}</span></span>
        </div>
        <div className="flex items-center gap-2 text-zinc-400">
          <BarChart3 className="w-3 h-3 text-amber-400" />
          <span>Executions: <span className="text-zinc-200">{table.total_executions}</span></span>
        </div>
        {table.schedule && (
          <div className="flex items-center gap-2 text-zinc-400">
            <Clock className="w-3 h-3 text-cyan-400" />
            <span>Schedule: <span className="text-zinc-200 font-mono">{table.schedule}</span></span>
          </div>
        )}
        <div className="flex items-center gap-2 text-zinc-400">
          <DollarSign className="w-3 h-3 text-emerald-400" />
          <span>Total cost: <span className="text-zinc-200">{formatUsd(table.total_cost_usd)}</span></span>
        </div>
      </div>

      {table.quality_gates.length > 0 && (
        <div className="mt-3 pt-3 border-t border-white/[0.06]">
          <div className="flex items-center gap-1.5 mb-1.5">
            <Shield className="w-3 h-3 text-emerald-400" />
            <span className="text-xs text-zinc-500">Quality Gates</span>
          </div>
          <div className="flex flex-wrap gap-1">
            {table.quality_gates.map((g, i) => (
              <Badge key={i} className="text-emerald-400 border-emerald-400/20">
                {g.gate_type}{g.column ? `: ${g.column}` : ''}
              </Badge>
            ))}
          </div>
        </div>
      )}

      {table.input_tables.length > 0 && (
        <div className="mt-2 pt-2 border-t border-white/[0.06]">
          <div className="flex items-center gap-1.5 mb-1.5">
            <Database className="w-3 h-3 text-cyan-400" />
            <span className="text-xs text-zinc-500">Input Tables</span>
          </div>
          <div className="flex flex-wrap gap-1">
            {table.input_tables.map(t => (
              <Badge key={t} className="text-cyan-400 border-cyan-400/20">{t}</Badge>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

function RightPanel({ table, activeTab, onTabChange, onRefresh, lineage, fetchLineage, debugResult, debugLoading, runDebug }: {
  table: ExecutableTable
  activeTab: RightPanelTab
  onTabChange: (tab: RightPanelTab) => void
  onRefresh: () => void
  lineage: ColumnLineageEntry[]
  fetchLineage: (name: string) => void
  debugResult: DebugResult | null
  debugLoading: boolean
  runDebug: (name: string) => void
}) {
  const [showIceberg, setShowIceberg] = useState(false)
  const tabs: { key: RightPanelTab; label: string; icon: React.ReactNode }[] = [
    { key: 'versions', label: 'Versions', icon: <GitCommit className="w-3.5 h-3.5" /> },
    { key: 'diff', label: 'Diff', icon: <GitBranch className="w-3.5 h-3.5" /> },
    { key: 'history', label: 'History', icon: <History className="w-3.5 h-3.5" /> },
    { key: 'compare', label: 'Compare', icon: <FlaskConical className="w-3.5 h-3.5" /> },
    { key: 'contracts', label: 'Contracts', icon: <FileCheck className="w-3.5 h-3.5" /> },
    { key: 'lineage', label: 'Lineage', icon: <Network className="w-3.5 h-3.5" /> },
  ]

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-0 border-b border-white/[0.06] bg-[#0a0f1e]/50">
        {tabs.map(tab => (
          <button
            key={tab.key}
            onClick={() => onTabChange(tab.key)}
            className={`flex items-center gap-1.5 px-4 py-2.5 text-xs font-medium transition-all border-b-2 ${
              activeTab === tab.key
                ? 'text-amber-400 border-amber-400 bg-amber-400/[0.03]'
                : 'text-zinc-500 border-transparent hover:text-zinc-300 hover:bg-white/[0.02]'
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        {activeTab === 'versions' && (
          <div className="space-y-4">
            <CommitsTab table={table} onRefresh={onRefresh} />
            <div className="border-t border-white/[0.06] pt-4">
              <button
                onClick={() => setShowIceberg(s => !s)}
                className="flex items-center gap-2 text-xs text-zinc-500 hover:text-zinc-300 transition-colors mb-2"
              >
                <Layers className="w-3.5 h-3.5" />
                <span>Iceberg Metadata</span>
                {showIceberg ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
              </button>
              {showIceberg && <IcebergTab name={table.table_name} />}
            </div>
          </div>
        )}
        {activeTab === 'diff' && <DiffTab table={table} />}
        {activeTab === 'history' && (
          <div className="space-y-4">
            <TimelineTab table={table} />
            <div className="border-t border-white/[0.06] pt-4">
              <Button size="sm" variant="secondary" onClick={() => runDebug(table.table_name)} disabled={debugLoading}>
                <Bug className="w-3.5 h-3.5 mr-1" /> {debugLoading ? 'Analyzing...' : 'Debug Last Execution'}
              </Button>
              {debugResult && (
                <div className="space-y-3 mt-3">
                  {debugResult.root_cause_lines.length > 0 && (
                    <div className="p-2 rounded bg-rose-900/20 border border-rose-500/30">
                      <div className="text-xs font-medium text-rose-400 mb-1">Root Cause</div>
                      {debugResult.root_cause_lines.map((line, i) => (
                        <div key={i} className="text-xs text-rose-300">{line}</div>
                      ))}
                    </div>
                  )}
                  <div className="grid grid-cols-2 gap-2">
                    {debugResult.bad_execution && (
                      <div className="p-2 rounded bg-rose-900/10 border border-rose-500/20">
                        <div className="text-[10px] text-rose-400 mb-1">Bad Execution</div>
                        <div className="text-xs text-slate-300">v{debugResult.bad_execution.version} · {debugResult.bad_execution.status}</div>
                        <div className="text-[10px] text-slate-500">{debugResult.bad_execution.rows_produced ?? 0} rows · {debugResult.bad_execution.duration_ms}ms</div>
                      </div>
                    )}
                    {debugResult.good_execution && (
                      <div className="p-2 rounded bg-emerald-900/10 border border-emerald-500/20">
                        <div className="text-[10px] text-emerald-400 mb-1">Good Execution</div>
                        <div className="text-xs text-slate-300">v{debugResult.good_execution.version} · {debugResult.good_execution.status}</div>
                        <div className="text-[10px] text-slate-500">{debugResult.good_execution.rows_produced ?? 0} rows · {debugResult.good_execution.duration_ms}ms</div>
                      </div>
                    )}
                  </div>
                  {debugResult.data_diff.regressions.length > 0 && (
                    <div className="p-2 rounded bg-slate-800/50 border border-slate-700/50">
                      <div className="text-xs text-slate-400 mb-1">Data Regressions</div>
                      {debugResult.data_diff.regressions.map((r, i) => (
                        <div key={i} className="text-xs text-slate-300">
                          {r.metric_name}: {r.old_value.toFixed(1)} → {r.new_value.toFixed(1)} ({r.change_pct > 0 ? '+' : ''}{r.change_pct.toFixed(1)}%)
                        </div>
                      ))}
                    </div>
                  )}
                  {debugResult.upstream_changes.length > 0 && (
                    <div className="p-2 rounded bg-slate-800/50 border border-slate-700/50">
                      <div className="text-xs text-slate-400 mb-1">Upstream Changes</div>
                      {debugResult.upstream_changes.map((uc, i) => (
                        <div key={i} className="text-xs text-slate-300">
                          {uc.table_name}: v{uc.version_before ?? '?'} → v{uc.version_after ?? '?'}
                          {uc.changed_at && <span className="text-slate-500 ml-1">({uc.changed_at})</span>}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        )}
        {activeTab === 'compare' && <ABTestTab table={table} />}
        {activeTab === 'contracts' && <ContractsTab table={table} />}
        {activeTab === 'lineage' && (
          <div className="space-y-3">
            <Button size="sm" onClick={() => fetchLineage(table.table_name)} className="mb-2">
              <Network className="w-3.5 h-3.5 mr-1" /> Parse Lineage
            </Button>
            {lineage.length > 0 && (
              <div className="space-y-2">
                {lineage.map((entry, i) => (
                  <div key={i} className="p-2 rounded bg-slate-800/50 border border-slate-700/50">
                    <div className="flex items-center gap-2">
                      <span className="text-amber-400 font-mono text-xs">{entry.output_column}</span>
                      <ArrowRight className="w-3 h-3 text-slate-500" />
                      <span className="text-slate-400 text-xs">
                        {entry.source_table ? `${entry.source_table}.${entry.source_column || '*'}` : 'computed'}
                      </span>
                    </div>
                    <div className="text-[10px] text-slate-500 mt-1 font-mono">{entry.transform_expression}</div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}

function TemplatesModal({ onClose, onInstalled }: { onClose: () => void; onInstalled: () => void }) {
  const [packages, setPackages] = useState<MarketplacePackage[]>([])
  const [loading, setLoading] = useState(true)
  const [installing, setInstalling] = useState<string | null>(null)

  useEffect(() => {
    fetch(`${API}/marketplace`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (data) setPackages(data.packages || []) })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  const install = async (id: string) => {
    setInstalling(id)
    try {
      const res = await fetch(`${API}/marketplace/install`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ package_id: id }),
      })
      if (res.ok) {
        onInstalled()
        onClose()
      }
    } catch { /* ignore */ }
    setInstalling(null)
  }

  return (
    <div className="fixed inset-0 bg-black/60 backdrop-blur-sm z-50 flex items-center justify-center p-4">
      <div className="bg-[#0c1222] border border-white/[0.06] rounded-xl w-full max-w-2xl max-h-[80vh] overflow-y-auto">
        <div className="sticky top-0 bg-[#0c1222] border-b border-white/[0.06] px-6 py-4 flex items-center justify-between z-10">
          <h2 className="text-lg font-semibold text-zinc-100 flex items-center gap-2">
            <ShoppingBag className="w-5 h-5 text-amber-400" />
            Model Templates
          </h2>
          <button onClick={onClose} className="text-zinc-500 hover:text-zinc-300"><X className="w-5 h-5" /></button>
        </div>
        <div className="p-6">
          {loading ? (
            <p className="text-sm text-zinc-500 text-center py-8">Loading marketplace...</p>
          ) : packages.length === 0 ? (
            <div className="text-center py-8">
              <ShoppingBag className="w-10 h-10 text-zinc-600 mx-auto mb-3" />
              <p className="text-sm text-zinc-400">No templates available yet</p>
              <p className="text-xs text-zinc-600 mt-1">Publish a model via API: POST /api/v1/marketplace/publish</p>
            </div>
          ) : (
            <div className="space-y-3">
              {packages.map(pkg => (
                <div key={pkg.id} className="rounded-lg border border-white/[0.06] p-4 hover:border-amber-400/20 transition-colors">
                  <div className="flex items-start justify-between">
                    <div>
                      <h3 className="text-sm font-semibold text-zinc-200">{pkg.name}</h3>
                      <p className="text-xs text-zinc-400 mt-0.5">{pkg.description}</p>
                      <div className="flex items-center gap-2 mt-2">
                        <Badge className="text-violet-400 border-violet-400/20">{pkg.category}</Badge>
                        <span className="text-[10px] text-zinc-500">by {pkg.author}</span>
                        <span className="text-[10px] text-zinc-500">v{pkg.version}</span>
                        <span className="text-[10px] text-zinc-500">{pkg.install_count} installs</span>
                      </div>
                      {pkg.tags.length > 0 && (
                        <div className="flex gap-1 mt-1.5">
                          {pkg.tags.map(t => (
                            <Badge key={t} className="text-zinc-500 border-white/[0.06] text-[10px]">{t}</Badge>
                          ))}
                        </div>
                      )}
                    </div>
                    <Button size="sm" variant="primary" loading={installing === pkg.id} onClick={() => install(pkg.id)}>
                      Install
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export function ExecutableTables() {
  const [searchParams, setSearchParams] = useSearchParams()
  const createFromUrl = searchParams.get('create') === 'true'
  const sqlFromUrl = searchParams.get('sql') || undefined
  const typeFromUrl = searchParams.get('type') || undefined

  useEffect(() => {
    if (createFromUrl) {
      setSearchParams({}, { replace: true })
    }
  }, [])

  const [tables, setTables] = useState<ExecutableTable[]>([])
  const [loading, setLoading] = useState(true)
  const [selectedName, setSelectedName] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<RightPanelTab>('versions')
  const [showCreate, setShowCreate] = useState(createFromUrl)
  const [showUpdate, setShowUpdate] = useState(false)
  const [showMarketplace, setShowMarketplace] = useState(false)
  const [filter, setFilter] = useState('')
  const [lineage, setLineage] = useState<ColumnLineageEntry[]>([])
  const [cascadeResult, setCascadeResult] = useState<CascadeReplayResult | null>(null)
  const [cascadeLoading, setCascadeLoading] = useState(false)
  const [debugResult, setDebugResult] = useState<DebugResult | null>(null)
  const [debugLoading, setDebugLoading] = useState(false)

  const fetchTables = async () => {
    try {
      const res = await fetch(`${API}/executable-tables`)
      if (res.ok) {
        const data = await res.json()
        setTables(data.tables || [])
      }
    } catch { /* ignore */ }
    setLoading(false)
  }

  useEffect(() => { fetchTables() }, [])

  const fetchLineage = useCallback(async (name: string) => {
    try {
      const res = await fetch(`${API}/executable-tables/${name}/column-lineage`)
      if (res.ok) {
        const data = await res.json()
        setLineage(data.lineage || [])
      }
    } catch { /* ignore */ }
  }, [])

  const runCascadeReplay = useCallback(async (name: string) => {
    setCascadeLoading(true)
    try {
      const res = await fetch(`${API}/executable-tables/${name}/cascade-replay`, { method: 'POST' })
      if (res.ok) setCascadeResult(await res.json())
    } catch { /* ignore */ }
    setCascadeLoading(false)
  }, [])

  const runDebug = useCallback(async (name: string) => {
    setDebugLoading(true)
    try {
      const res = await fetch(`${API}/executable-tables/${name}/debug`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      })
      if (res.ok) setDebugResult(await res.json())
    } catch { /* ignore */ }
    setDebugLoading(false)
  }, [])

  const selectedTable = tables.find(t => t.table_name === selectedName) || null

  const filteredTables = filter
    ? tables.filter(t => t.table_name.toLowerCase().includes(filter.toLowerCase()))
    : tables

  const totalVersions = tables.reduce((sum, t) => sum + (t.versions?.length || 1), 0)
  const totalExecutions = tables.reduce((sum, t) => sum + t.total_executions, 0)

  return (
    <div className="flex flex-col h-full">
      <div className="shrink-0 px-6 py-4 border-b border-white/[0.06]">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-zinc-100 flex items-center gap-3">
              <Zap className="w-7 h-7 text-amber-400" />
              Data Models
            </h1>
            <p className="text-sm text-zinc-500 mt-1">
              Versioned transforms with quality gates, lineage, and contracts
            </p>
          </div>
          <div className="flex items-center gap-3">
            <Badge className="text-zinc-400 border-white/[0.06]" dot dotColor="bg-amber-400">
              {tables.length} {tables.length === 1 ? 'table' : 'tables'}
            </Badge>
            <Badge className="text-zinc-400 border-white/[0.06]" dot dotColor="bg-violet-400">
              {totalVersions} versions
            </Badge>
            <Badge className="text-zinc-400 border-white/[0.06]" dot dotColor="bg-emerald-400">
              {totalExecutions} executions
            </Badge>
          </div>
        </div>
      </div>

      <div className="flex-1 flex min-h-0">
        <div className="w-[40%] border-r border-white/[0.06] flex flex-col min-h-0">
          <div className="shrink-0 p-3 border-b border-white/[0.06] flex items-center gap-2">
            <div className="relative flex-1">
              <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-500" />
              <input
                value={filter}
                onChange={e => setFilter(e.target.value)}
                placeholder="Filter tables..."
                className="w-full pl-8 pr-3 py-1.5 text-xs rounded-lg bg-navy-900/60 border border-white/[0.06] text-zinc-100 placeholder-zinc-600 focus:outline-none focus:ring-1 focus:ring-amber-400/25"
              />
            </div>
            <Button size="sm" variant="primary" icon={<Plus className="w-3 h-3" />} onClick={() => setShowCreate(true)}>
              New
            </Button>
            <Button size="sm" variant="secondary" icon={<ShoppingBag className="w-3.5 h-3.5" />} onClick={() => setShowMarketplace(true)}>
              Templates
            </Button>
          </div>

          <div className="flex-1 overflow-y-auto">
            {loading ? (
              <div className="py-12 text-center text-sm text-zinc-500">Loading tables...</div>
            ) : filteredTables.length === 0 ? (
              <div className="py-12 text-center">
                <Zap className="w-8 h-8 text-zinc-600 mx-auto mb-2" />
                <p className="text-sm text-zinc-400">
                  {filter ? 'No matching models' : 'No data models yet'}
                </p>
                {!filter && (
                  <p className="text-xs text-zinc-600 mt-1">Create one to get started</p>
                )}
              </div>
            ) : (
              <div className="divide-y divide-white/[0.03]">
                {filteredTables.map(t => (
                  <TableListItem
                    key={t.table_name}
                    table={t}
                    selected={selectedName === t.table_name}
                    onClick={() => {
                      setSelectedName(t.table_name)
                      setActiveTab('versions')
                    }}
                    onCascade={runCascadeReplay}
                  />
                ))}
              </div>
            )}
          </div>

          {selectedTable && (
            <div className="shrink-0 border-t border-white/[0.06] overflow-y-auto max-h-[45%]">
              <SelectedTableMeta table={selectedTable} onEdit={() => setShowUpdate(true)} />
            </div>
          )}
        </div>

        <div className="flex-1 flex flex-col min-h-0">
          {selectedTable ? (
            <RightPanel
              table={selectedTable}
              activeTab={activeTab}
              onTabChange={setActiveTab}
              onRefresh={fetchTables}
              lineage={lineage}
              fetchLineage={fetchLineage}
              debugResult={debugResult}
              debugLoading={debugLoading}
              runDebug={runDebug}
            />
          ) : (
            <div className="flex-1 flex items-center justify-center">
              <div className="text-center">
                <GitCommit className="w-12 h-12 text-zinc-700 mx-auto mb-3" />
                <p className="text-zinc-400 text-sm">Select a model to view its version history</p>
                <p className="text-zinc-600 text-xs mt-1">Browse versions, diffs, lineage, and contracts</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {showCreate && (
        <CreateFormModal
          onCreated={fetchTables}
          onClose={() => setShowCreate(false)}
          initialSql={sqlFromUrl}
          initialType={typeFromUrl}
        />
      )}

      {showUpdate && selectedTable && (
        <UpdateDialog
          table={selectedTable}
          onClose={() => setShowUpdate(false)}
          onUpdated={fetchTables}
        />
      )}

      {showMarketplace && <TemplatesModal onClose={() => setShowMarketplace(false)} onInstalled={fetchTables} />}
    </div>
  )
}
