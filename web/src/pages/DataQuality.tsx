import { useState, useEffect, useMemo } from 'react'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { Modal } from '../components/ui/Modal'
import { Input, Select } from '../components/ui/Input'
import { StatusDot } from '../components/ui/StatusDot'
import { cn, formatNumber, formatRelativeTime } from '../lib/utils'
import { getQualityChecks, getQualityRules, createQualityRule, deleteQualityRule, getTables } from '../api/client'
import type { QualityChecksResponse, TableQualityCheck, QualityRule, ColumnNullInfo } from '../types'
import {
  ShieldCheck, BarChart3, Bell, RefreshCw, ChevronDown, ChevronRight,
  Plus, Trash2, Eye, AlertTriangle, CheckCircle2, XCircle, Loader2,
  Columns3, ToggleRight, ToggleLeft,
} from 'lucide-react'
import {
  BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import toast from 'react-hot-toast'

const COLORS = ['#fbbf24', '#22d3ee', '#10b981', '#f43f5e', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16']
const gridStroke = 'rgba(251,191,36,0.04)'
const axisStyle = { fontSize: 10, fill: '#475569' }
const tooltipStyle = {
  contentStyle: { background: '#0d1730', border: '1px solid rgba(251,191,36,0.1)', borderRadius: 10, fontSize: 12, backdropFilter: 'blur(12px)' },
  itemStyle: { color: '#94a3b8' },
  labelStyle: { color: '#e2e8f0', fontWeight: 600 },
}

function healthToStatus(health: string): 'healthy' | 'warning' | 'error' | 'idle' {
  if (health === 'healthy') return 'healthy'
  if (health === 'warning') return 'warning'
  if (health === 'critical') return 'error'
  return 'idle'
}

function nullPctColor(pct: number): string {
  if (pct < 5) return 'text-emerald-400'
  if (pct <= 25) return 'text-amber-400'
  return 'text-rose-400'
}

function nullPctBg(pct: number): string {
  if (pct < 5) return 'bg-emerald-400'
  if (pct <= 25) return 'bg-amber-400'
  return 'bg-rose-400'
}

export function DataQuality() {
  const [tab, setTab] = useState('overview')
  const [checks, setChecks] = useState<QualityChecksResponse | null>(null)
  const [rules, setRules] = useState<QualityRule[]>([])
  const [tableNames, setTableNames] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [expandedTable, setExpandedTable] = useState<string | null>(null)
  const [selectedTable, setSelectedTable] = useState('')
  const [ruleModalOpen, setRuleModalOpen] = useState(false)
  const [ruleForm, setRuleForm] = useState({ table_name: '', rule_type: 'null_threshold', threshold: '25' })

  const loadData = async () => {
    try {
      const [checksRes, rulesRes, tablesRes] = await Promise.all([
        getQualityChecks(),
        getQualityRules(),
        getTables(),
      ])
      setChecks(checksRes)
      setRules(rulesRes.rules || [])
      const names = (tablesRes.tables || []).map(t => t.name)
      setTableNames(names)
      if (!selectedTable && names.length > 0) setSelectedTable(names[0])
      if (!ruleForm.table_name && names.length > 0) {
        setRuleForm(prev => ({ ...prev, table_name: names[0] }))
      }
    } catch (e) {
      toast.error((e as Error).message)
    }
    setLoading(false)
    setRefreshing(false)
  }

  useEffect(() => { loadData() }, [])

  const handleRefresh = () => {
    setRefreshing(true)
    loadData()
  }

  const handleCreateRule = async () => {
    if (!ruleForm.table_name) return
    try {
      await createQualityRule({
        table_name: ruleForm.table_name,
        rule_type: ruleForm.rule_type,
        threshold: Number(ruleForm.threshold),
        enabled: true,
      })
      toast.success('Rule created')
      setRuleModalOpen(false)
      setRuleForm({ table_name: tableNames[0] || '', rule_type: 'null_threshold', threshold: '25' })
      const rulesRes = await getQualityRules()
      setRules(rulesRes.rules || [])
    } catch (e) {
      toast.error((e as Error).message)
    }
  }

  const handleDeleteRule = async (id: string) => {
    try {
      await deleteQualityRule(id)
      toast.success('Rule deleted')
      setRules(prev => prev.filter(r => r.id !== id))
    } catch (e) {
      toast.error((e as Error).message)
    }
  }

  const handleToggleRule = (id: string) => {
    setRules(prev => prev.map(r => r.id === id ? { ...r, enabled: !r.enabled } : r))
    toast.success('Rule toggled')
  }

  const selectedCheck = useMemo(() => {
    if (!checks || !selectedTable) return null
    return checks.checks.find(c => c.table === selectedTable) || null
  }, [checks, selectedTable])

  const pieData = useMemo(() => {
    if (!checks) return []
    return [
      { name: 'Healthy', value: checks.healthy_count, color: '#10b981' },
      { name: 'Warning', value: checks.warning_count, color: '#fbbf24' },
      { name: 'Critical', value: checks.critical_count, color: '#f43f5e' },
    ].filter(d => d.value > 0)
  }, [checks])

  const barData = useMemo(() => {
    if (!selectedCheck) return []
    return selectedCheck.null_percentages.map(c => ({
      name: c.name.length > 12 ? c.name.slice(0, 12) + '...' : c.name,
      null_pct: Number(c.null_pct.toFixed(1)),
      fullName: c.name,
    }))
  }, [selectedCheck])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-6 h-6 text-amber-400 animate-spin" />
      </div>
    )
  }

  const tableOptions = tableNames.map(n => ({ value: n, label: n }))
  const ruleTypeOptions = [
    { value: 'null_threshold', label: 'Null Threshold (%)' },
    { value: 'min_row_count', label: 'Min Row Count' },
  ]

  return (
    <div className="p-6 max-w-7xl mx-auto space-y-6 animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-emerald-400/10 border border-emerald-400/20 flex items-center justify-center">
            <ShieldCheck className="w-4.5 h-4.5 text-emerald-400" />
          </div>
          <div>
            <h1 className="text-base font-display font-bold text-zinc-100">Data Quality</h1>
            <p className="text-2xs text-zinc-500">Table health monitoring, null analysis, and alert rules</p>
          </div>
        </div>
        <Button
          variant="secondary"
          size="sm"
          icon={<RefreshCw className={cn('w-3.5 h-3.5', refreshing && 'animate-spin')} />}
          loading={refreshing}
          onClick={handleRefresh}
        >
          Refresh
        </Button>
      </div>

      {/* Tabs */}
      <Tabs
        tabs={[
          { id: 'overview', label: 'Overview', icon: <ShieldCheck className="w-3.5 h-3.5" /> },
          { id: 'columns', label: 'Column Analysis', icon: <Columns3 className="w-3.5 h-3.5" /> },
          { id: 'rules', label: 'Alert Rules', icon: <Bell className="w-3.5 h-3.5" />, count: rules.length },
        ]}
        active={tab}
        onChange={setTab}
      />

      {/* ── Tab 1: Overview ── */}
      {tab === 'overview' && checks && (
        <div className="space-y-6">
          {/* Summary cards */}
          <div className="grid grid-cols-4 gap-4">
            <Card className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-zinc-400/10 border border-zinc-400/20 flex items-center justify-center">
                <ShieldCheck className="w-5 h-5 text-zinc-400" />
              </div>
              <div>
                <p className="text-2xs text-zinc-500 uppercase tracking-wider font-medium">Total Tables</p>
                <p className="text-xl font-display font-bold text-zinc-100">{checks.total_tables}</p>
              </div>
            </Card>
            <Card className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-emerald-400/10 border border-emerald-400/20 flex items-center justify-center">
                <CheckCircle2 className="w-5 h-5 text-emerald-400" />
              </div>
              <div>
                <p className="text-2xs text-zinc-500 uppercase tracking-wider font-medium">Healthy</p>
                <p className="text-xl font-display font-bold text-emerald-400">{checks.healthy_count}</p>
              </div>
            </Card>
            <Card className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-amber-400/10 border border-amber-400/20 flex items-center justify-center">
                <AlertTriangle className="w-5 h-5 text-amber-400" />
              </div>
              <div>
                <p className="text-2xs text-zinc-500 uppercase tracking-wider font-medium">Warning</p>
                <p className="text-xl font-display font-bold text-amber-400">{checks.warning_count}</p>
              </div>
            </Card>
            <Card className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-xl bg-rose-400/10 border border-rose-400/20 flex items-center justify-center">
                <XCircle className="w-5 h-5 text-rose-400" />
              </div>
              <div>
                <p className="text-2xs text-zinc-500 uppercase tracking-wider font-medium">Critical</p>
                <p className="text-xl font-display font-bold text-rose-400">{checks.critical_count}</p>
              </div>
            </Card>
          </div>

          <div className="grid grid-cols-3 gap-6">
            {/* Pie chart */}
            <Card>
              <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4">Health Distribution</h3>
              {pieData.length > 0 ? (
                <ResponsiveContainer width="100%" height={220}>
                  <PieChart>
                    <Pie
                      data={pieData}
                      cx="50%"
                      cy="50%"
                      innerRadius={55}
                      outerRadius={85}
                      paddingAngle={3}
                      dataKey="value"
                      nameKey="name"
                    >
                      {pieData.map((entry, i) => (
                        <Cell key={i} fill={entry.color} stroke="transparent" />
                      ))}
                    </Pie>
                    <Tooltip {...tooltipStyle} />
                    <Legend
                      iconType="circle"
                      iconSize={8}
                      wrapperStyle={{ fontSize: 11, color: '#94a3b8' }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex items-center justify-center h-[220px] text-zinc-600 text-xs">
                  No data available
                </div>
              )}
            </Card>

            {/* Table list */}
            <div className="col-span-2 space-y-2">
              <h3 className="text-sm font-display font-semibold text-zinc-200">Table Health Checks</h3>
              <div className="space-y-1.5">
                {checks.checks.map((check, idx) => (
                  <div key={check.table} style={{ animationDelay: `${idx * 40}ms` }} className="animate-fade-in">
                    <Card
                      padding="sm"
                      hover
                      onClick={() => setExpandedTable(expandedTable === check.table ? null : check.table)}
                      className="group"
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          {expandedTable === check.table
                            ? <ChevronDown className="w-3.5 h-3.5 text-zinc-500" />
                            : <ChevronRight className="w-3.5 h-3.5 text-zinc-500" />
                          }
                          <span className="text-sm font-mono text-zinc-200">{check.table}</span>
                        </div>
                        <div className="flex items-center gap-3">
                          <span className="text-2xs text-zinc-500">
                            {formatNumber(check.row_count)} rows
                          </span>
                          <span className="text-2xs text-zinc-600">&middot;</span>
                          <span className="text-2xs text-zinc-500">
                            {check.column_count} cols
                          </span>
                          {check.issues.length > 0 && (
                            <Badge className="bg-rose-400/10 text-rose-400 border-rose-400/20">
                              {check.issues.length} issue{check.issues.length !== 1 ? 's' : ''}
                            </Badge>
                          )}
                          <StatusDot status={healthToStatus(check.health)} label={check.health} />
                          <Button
                            variant="ghost"
                            size="sm"
                            icon={<Eye className="w-3 h-3" />}
                            onClick={(e) => {
                              e.stopPropagation()
                              setSelectedTable(check.table)
                              setTab('columns')
                            }}
                          />
                        </div>
                      </div>
                    </Card>

                    {/* Expanded column null details */}
                    {expandedTable === check.table && (
                      <Card padding="sm" className="mt-1 ml-6 animate-fade-in">
                        {check.issues.length > 0 && (
                          <div className="mb-3 space-y-1">
                            {check.issues.map((issue, i) => (
                              <div key={i} className="flex items-center gap-2 text-2xs text-amber-400/80">
                                <AlertTriangle className="w-3 h-3 flex-shrink-0" />
                                {issue}
                              </div>
                            ))}
                          </div>
                        )}
                        <table className="w-full text-left">
                          <thead>
                            <tr className="border-b border-white/[0.04]">
                              <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider pb-2 pr-4">Column</th>
                              <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider pb-2 pr-4">Type</th>
                              <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider pb-2 pr-4 text-right">Null Count</th>
                              <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider pb-2 pr-4 text-right">Total Rows</th>
                              <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider pb-2 text-right">Null %</th>
                            </tr>
                          </thead>
                          <tbody>
                            {check.null_percentages.map(col => (
                              <tr key={col.name} className="border-b border-white/[0.02]">
                                <td className="py-1.5 pr-4 text-xs font-mono text-zinc-300">{col.name}</td>
                                <td className="py-1.5 pr-4 text-2xs text-zinc-500">{col.data_type}</td>
                                <td className="py-1.5 pr-4 text-xs text-zinc-400 text-right font-mono">
                                  {formatNumber(col.null_count)}
                                </td>
                                <td className="py-1.5 pr-4 text-xs text-zinc-400 text-right font-mono">
                                  {formatNumber(col.total_rows)}
                                </td>
                                <td className="py-1.5 text-right">
                                  <span className={cn('text-xs font-mono font-medium', nullPctColor(col.null_pct))}>
                                    {col.null_pct.toFixed(1)}%
                                  </span>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </Card>
                    )}
                  </div>
                ))}

                {checks.checks.length === 0 && (
                  <Card className="text-center py-8">
                    <p className="text-sm text-zinc-500">No tables found. Register tables to begin monitoring.</p>
                  </Card>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ── Tab 2: Column Analysis ── */}
      {tab === 'columns' && (
        <div className="space-y-6">
          <div className="flex items-center gap-4">
            <div className="w-64">
              <Select
                label="Select Table"
                options={tableOptions}
                value={selectedTable}
                onChange={e => setSelectedTable(e.target.value)}
              />
            </div>
            {selectedCheck && (
              <div className="flex items-center gap-3 mt-5">
                <StatusDot status={healthToStatus(selectedCheck.health)} label={selectedCheck.health} />
                <span className="text-2xs text-zinc-500">
                  {formatNumber(selectedCheck.row_count)} rows &middot; {selectedCheck.column_count} columns
                </span>
              </div>
            )}
          </div>

          {selectedCheck && barData.length > 0 ? (
            <>
              {/* Bar chart */}
              <Card>
                <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4">
                  Null Percentage by Column
                </h3>
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={barData} margin={{ top: 10, right: 20, bottom: 40, left: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
                    <XAxis
                      dataKey="name"
                      tick={axisStyle}
                      angle={-35}
                      textAnchor="end"
                      height={60}
                      interval={0}
                    />
                    <YAxis tick={axisStyle} domain={[0, 'auto']} unit="%" />
                    <Tooltip
                      {...tooltipStyle}
                      formatter={(value: number) => [`${value}%`, 'Null %']}
                      labelFormatter={(label: string) => {
                        const item = barData.find(d => d.name === label)
                        return item?.fullName || label
                      }}
                    />
                    <Bar dataKey="null_pct" radius={[4, 4, 0, 0]} maxBarSize={40}>
                      {barData.map((entry, i) => (
                        <Cell
                          key={i}
                          fill={entry.null_pct < 5 ? '#10b981' : entry.null_pct <= 25 ? '#fbbf24' : '#f43f5e'}
                          fillOpacity={0.8}
                        />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </Card>

              {/* Column detail table */}
              <Card padding="none">
                <div className="px-5 py-3 border-b border-white/[0.04]">
                  <h3 className="text-sm font-display font-semibold text-zinc-200">Column Details</h3>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-left">
                    <thead>
                      <tr className="border-b border-white/[0.04]">
                        <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5">Column</th>
                        <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5">Data Type</th>
                        <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5 text-right">Null Count</th>
                        <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5 text-right">Total Rows</th>
                        <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5 text-right">Null %</th>
                        <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedCheck.null_percentages.map((col, idx) => (
                        <tr
                          key={col.name}
                          className={cn(
                            'border-b border-white/[0.02] hover:bg-white/[0.01] transition-colors',
                            'animate-fade-in'
                          )}
                          style={{ animationDelay: `${idx * 30}ms` }}
                        >
                          <td className="px-5 py-2.5 text-sm font-mono text-zinc-200">{col.name}</td>
                          <td className="px-5 py-2.5">
                            <Badge className="text-zinc-400">{col.data_type}</Badge>
                          </td>
                          <td className="px-5 py-2.5 text-sm font-mono text-zinc-400 text-right">
                            {formatNumber(col.null_count)}
                          </td>
                          <td className="px-5 py-2.5 text-sm font-mono text-zinc-400 text-right">
                            {formatNumber(col.total_rows)}
                          </td>
                          <td className="px-5 py-2.5 text-right">
                            <span className={cn('text-sm font-mono font-semibold', nullPctColor(col.null_pct))}>
                              {col.null_pct.toFixed(1)}%
                            </span>
                          </td>
                          <td className="px-5 py-2.5">
                            <div className="flex items-center gap-2">
                              <div className="w-16 h-1.5 rounded-full bg-white/[0.04] overflow-hidden">
                                <div
                                  className={cn('h-full rounded-full transition-all duration-500', nullPctBg(col.null_pct))}
                                  style={{ width: `${Math.min(col.null_pct, 100)}%` }}
                                />
                              </div>
                              <span className={cn('text-2xs font-medium', nullPctColor(col.null_pct))}>
                                {col.null_pct < 5 ? 'Good' : col.null_pct <= 25 ? 'Warn' : 'Critical'}
                              </span>
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>
            </>
          ) : (
            <Card className="text-center py-12">
              <BarChart3 className="w-8 h-8 text-zinc-600 mx-auto mb-3" />
              <p className="text-sm text-zinc-500">
                {tableOptions.length === 0
                  ? 'No tables available. Register tables to analyze column quality.'
                  : 'Select a table to view column null analysis.'}
              </p>
            </Card>
          )}
        </div>
      )}

      {/* ── Tab 3: Alert Rules ── */}
      {tab === 'rules' && (
        <div className="space-y-6">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-display font-semibold text-zinc-200">Alert Rules</h3>
            <Button
              variant="primary"
              size="sm"
              icon={<Plus className="w-3.5 h-3.5" />}
              onClick={() => setRuleModalOpen(true)}
            >
              Add Rule
            </Button>
          </div>

          {rules.length > 0 ? (
            <Card padding="none">
              <div className="overflow-x-auto">
                <table className="w-full text-left">
                  <thead>
                    <tr className="border-b border-white/[0.04]">
                      <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5">Table</th>
                      <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5">Rule Type</th>
                      <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5 text-right">Threshold</th>
                      <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5 text-center">Enabled</th>
                      <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5">Created</th>
                      <th className="text-2xs font-medium text-zinc-500 uppercase tracking-wider px-5 py-2.5 text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rules.map((rule, idx) => (
                      <tr
                        key={rule.id}
                        className="border-b border-white/[0.02] hover:bg-white/[0.01] transition-colors animate-fade-in"
                        style={{ animationDelay: `${idx * 30}ms` }}
                      >
                        <td className="px-5 py-2.5 text-sm font-mono text-zinc-200">{rule.table_name}</td>
                        <td className="px-5 py-2.5">
                          <Badge className={
                            rule.rule_type === 'null_threshold'
                              ? 'bg-amber-400/10 text-amber-400 border-amber-400/20'
                              : 'bg-cyan-400/10 text-cyan-400 border-cyan-400/20'
                          }>
                            {rule.rule_type === 'null_threshold' ? 'Null Threshold' : 'Min Row Count'}
                          </Badge>
                        </td>
                        <td className="px-5 py-2.5 text-sm font-mono text-zinc-300 text-right">
                          {rule.rule_type === 'null_threshold' ? `${rule.threshold}%` : formatNumber(rule.threshold)}
                        </td>
                        <td className="px-5 py-2.5 text-center">
                          <button
                            onClick={() => handleToggleRule(rule.id)}
                            className="inline-flex items-center justify-center hover:opacity-80 transition-opacity"
                          >
                            {rule.enabled
                              ? <ToggleRight className="w-5 h-5 text-emerald-400" />
                              : <ToggleLeft className="w-5 h-5 text-zinc-600" />
                            }
                          </button>
                        </td>
                        <td className="px-5 py-2.5 text-2xs text-zinc-500">
                          {formatRelativeTime(rule.created_at)}
                        </td>
                        <td className="px-5 py-2.5 text-right">
                          <Button
                            variant="ghost"
                            size="sm"
                            icon={<Trash2 className="w-3.5 h-3.5" />}
                            onClick={() => handleDeleteRule(rule.id)}
                            className="text-rose-400 hover:text-rose-300 hover:bg-rose-400/10"
                          />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>
          ) : (
            <Card className="text-center py-12">
              <Bell className="w-8 h-8 text-zinc-600 mx-auto mb-3" />
              <p className="text-sm text-zinc-500 mb-4">No alert rules configured yet.</p>
              <Button
                variant="secondary"
                size="sm"
                icon={<Plus className="w-3.5 h-3.5" />}
                onClick={() => setRuleModalOpen(true)}
              >
                Create First Rule
              </Button>
            </Card>
          )}

          {/* Architecture info cards */}
          <div className="grid grid-cols-3 gap-4">
            <Card className="space-y-2">
              <div className="flex items-center gap-2">
                <ShieldCheck className="w-4 h-4 text-emerald-400" />
                <span className="text-xs font-display font-semibold text-zinc-200">Null Threshold</span>
              </div>
              <p className="text-2xs text-zinc-500 leading-relaxed">
                Alert when any column in a table exceeds the configured null percentage threshold. Useful for catching data pipeline regressions.
              </p>
            </Card>
            <Card className="space-y-2">
              <div className="flex items-center gap-2">
                <BarChart3 className="w-4 h-4 text-cyan-400" />
                <span className="text-xs font-display font-semibold text-zinc-200">Min Row Count</span>
              </div>
              <p className="text-2xs text-zinc-500 leading-relaxed">
                Alert when a table drops below the expected minimum row count. Detects accidental truncation or failed ingestion.
              </p>
            </Card>
            <Card className="space-y-2">
              <div className="flex items-center gap-2">
                <Bell className="w-4 h-4 text-amber-400" />
                <span className="text-xs font-display font-semibold text-zinc-200">Scheduled Checks</span>
              </div>
              <p className="text-2xs text-zinc-500 leading-relaxed">
                Combine with the Scheduler to run quality checks on a cron schedule. Violations produce alerts visible on this dashboard.
              </p>
            </Card>
          </div>
        </div>
      )}

      {/* ── Create Rule Modal ── */}
      <Modal open={ruleModalOpen} onClose={() => setRuleModalOpen(false)} title="Create Alert Rule">
        <div className="space-y-4">
          <Select
            label="Table"
            options={tableOptions}
            value={ruleForm.table_name}
            onChange={e => setRuleForm(prev => ({ ...prev, table_name: e.target.value }))}
          />
          <Select
            label="Rule Type"
            options={ruleTypeOptions}
            value={ruleForm.rule_type}
            onChange={e => setRuleForm(prev => ({ ...prev, rule_type: e.target.value }))}
          />
          <Input
            label={ruleForm.rule_type === 'null_threshold' ? 'Threshold (%)' : 'Minimum Row Count'}
            type="number"
            value={ruleForm.threshold}
            onChange={e => setRuleForm(prev => ({ ...prev, threshold: e.target.value }))}
            placeholder={ruleForm.rule_type === 'null_threshold' ? '25' : '1000'}
          />
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" size="sm" onClick={() => setRuleModalOpen(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleCreateRule}>Create Rule</Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
