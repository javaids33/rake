import { useState, useEffect, useCallback } from 'react'
import { Card } from '../components/ui/Card'
import { Badge } from '../components/ui/Badge'
import { Button } from '../components/ui/Button'
import { Input, Textarea } from '../components/ui/Input'
import {
  ShieldCheck, Plus, Search, Clock, BarChart3, DollarSign,
  CheckCircle2, XCircle, AlertTriangle, ChevronRight, Layers,
  Users, FileCheck, Activity, ArrowRight, Eye, RefreshCw,
} from 'lucide-react'
import type {
  DataProduct, DataProductAudit, FreshnessStatus,
} from '../types'

const API = '/api/v1'

function formatUsd(v: number): string {
  if (v < 0.001) return `$${v.toFixed(6)}`
  if (v < 1) return `$${v.toFixed(4)}`
  return `$${v.toFixed(2)}`
}

export function DataProducts() {
  const [products, setProducts] = useState<DataProduct[]>([])
  const [selected, setSelected] = useState<DataProduct | null>(null)
  const [audit, setAudit] = useState<DataProductAudit | null>(null)
  const [auditLoading, setAuditLoading] = useState(false)
  const [showCreate, setShowCreate] = useState(false)
  const [search, setSearch] = useState('')

  // Create form state
  const [newName, setNewName] = useState('')
  const [newTableName, setNewTableName] = useState('')
  const [newOwner, setNewOwner] = useState('')
  const [newSlaFreshness, setNewSlaFreshness] = useState('24')
  const [newSlaQuality, setNewSlaQuality] = useState('0.95')
  const [newDescription, setNewDescription] = useState('')

  const fetchProducts = useCallback(async () => {
    try {
      const res = await fetch(`${API}/data-products`)
      if (res.ok) {
        const data = await res.json()
        setProducts(data.products || [])
      }
    } catch { /* ignore */ }
  }, [])

  useEffect(() => { fetchProducts() }, [fetchProducts])

  const createProduct = async () => {
    try {
      const res = await fetch(`${API}/data-products`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: newName,
          table_name: newTableName,
          owner: newOwner,
          sla_freshness_hours: parseFloat(newSlaFreshness) || 24,
          sla_quality_score: parseFloat(newSlaQuality) || 0.95,
          description: newDescription,
        }),
      })
      if (res.ok) {
        setShowCreate(false)
        setNewName('')
        setNewTableName('')
        setNewOwner('')
        setNewDescription('')
        fetchProducts()
      }
    } catch { /* ignore */ }
  }

  const runAudit = async (name: string) => {
    setAuditLoading(true)
    try {
      const res = await fetch(`${API}/data-products/${name}/audit`)
      if (res.ok) setAudit(await res.json())
    } catch { /* ignore */ }
    setAuditLoading(false)
  }

  const selectProduct = (p: DataProduct) => {
    setSelected(p)
    runAudit(p.name)
  }

  const filtered = products.filter(p =>
    p.name.toLowerCase().includes(search.toLowerCase()) ||
    p.table_name.toLowerCase().includes(search.toLowerCase())
  )

  const certColor = (cert: string) => {
    if (cert === 'certified') return 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20'
    if (cert === 'pending') return 'bg-amber-400/10 text-amber-400 border-amber-400/20'
    return 'bg-slate-400/10 text-slate-400 border-slate-400/20'
  }

  return (
    <div className="flex h-[calc(100vh-4rem)] overflow-hidden">
      {/* Left Panel — Product List */}
      <div className="w-80 border-r border-slate-700/50 flex flex-col overflow-hidden">
        <div className="p-3 border-b border-slate-700/50 space-y-2">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-medium text-slate-200 flex items-center gap-1.5">
              <ShieldCheck className="w-4 h-4 text-emerald-400" />
              Data Products
            </h2>
            <Button size="sm" onClick={() => setShowCreate(!showCreate)}>
              <Plus className="w-3.5 h-3.5" />
            </Button>
          </div>
          <div className="relative">
            <Search className="w-3.5 h-3.5 absolute left-2 top-1/2 -translate-y-1/2 text-slate-500" />
            <Input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search products..."
              className="pl-7 h-7 text-xs"
            />
          </div>
        </div>

        {showCreate && (
          <div className="p-3 border-b border-slate-700/50 space-y-2 bg-slate-800/30">
            <Input value={newName} onChange={e => setNewName(e.target.value)} placeholder="Product name" className="h-7 text-xs" />
            <Input value={newTableName} onChange={e => setNewTableName(e.target.value)} placeholder="Table name" className="h-7 text-xs" />
            <Input value={newOwner} onChange={e => setNewOwner(e.target.value)} placeholder="Owner" className="h-7 text-xs" />
            <div className="grid grid-cols-2 gap-2">
              <Input value={newSlaFreshness} onChange={e => setNewSlaFreshness(e.target.value)} placeholder="SLA hours" className="h-7 text-xs" />
              <Input value={newSlaQuality} onChange={e => setNewSlaQuality(e.target.value)} placeholder="Quality score" className="h-7 text-xs" />
            </div>
            <Textarea value={newDescription} onChange={e => setNewDescription(e.target.value)} placeholder="Description" className="text-xs" rows={2} />
            <Button size="sm" onClick={createProduct} className="w-full">Create Product</Button>
          </div>
        )}

        <div className="flex-1 overflow-y-auto">
          {filtered.map(p => (
            <div
              key={p.id}
              onClick={() => selectProduct(p)}
              className={`p-3 border-b border-slate-700/30 cursor-pointer hover:bg-slate-800/30 transition-colors ${
                selected?.id === p.id ? 'bg-slate-800/50 border-l-2 border-l-emerald-400' : ''
              }`}
            >
              <div className="flex items-center justify-between mb-1">
                <span className="text-sm text-slate-200 font-medium">{p.name}</span>
                <Badge className={certColor(p.certification)}>{p.certification}</Badge>
              </div>
              <div className="text-[10px] text-slate-500 space-y-0.5">
                <div className="flex items-center gap-1"><Layers className="w-2.5 h-2.5" />{p.table_name}</div>
                <div className="flex items-center gap-1"><Users className="w-2.5 h-2.5" />{p.owner} · {p.consumers.length} consumers</div>
                <div className="flex items-center gap-1"><Clock className="w-2.5 h-2.5" />SLA: {p.sla_freshness_hours}h freshness</div>
              </div>
            </div>
          ))}
          {filtered.length === 0 && (
            <div className="p-4 text-center text-xs text-slate-500">
              {products.length === 0 ? 'No data products yet. Create one to get started.' : 'No matching products.'}
            </div>
          )}
        </div>
      </div>

      {/* Right Panel — Audit */}
      <div className="flex-1 overflow-y-auto p-4">
        {!selected && (
          <div className="flex items-center justify-center h-full text-slate-500 text-sm">
            Select a data product to view compliance audit
          </div>
        )}

        {selected && (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <h2 className="text-lg font-medium text-slate-200">{selected.name}</h2>
                <p className="text-xs text-slate-500">{selected.description || 'No description'}</p>
              </div>
              <Button size="sm" onClick={() => runAudit(selected.name)} disabled={auditLoading}>
                <RefreshCw className={`w-3.5 h-3.5 mr-1 ${auditLoading ? 'animate-spin' : ''}`} />
                {auditLoading ? 'Auditing...' : 'Run Audit'}
              </Button>
            </div>

            {audit && (
              <>
                {/* Certification Banner */}
                <Card className={`p-4 ${audit.certification_eligible ? 'border-emerald-500/30 bg-emerald-900/10' : 'border-amber-500/30 bg-amber-900/10'}`}>
                  <div className="flex items-center gap-3">
                    {audit.certification_eligible ? (
                      <CheckCircle2 className="w-8 h-8 text-emerald-400" />
                    ) : (
                      <AlertTriangle className="w-8 h-8 text-amber-400" />
                    )}
                    <div>
                      <div className={`text-sm font-medium ${audit.certification_eligible ? 'text-emerald-400' : 'text-amber-400'}`}>
                        {audit.certification_eligible ? 'Certification Eligible' : 'Certification Pending'}
                      </div>
                      <div className="text-xs text-slate-400">
                        {audit.compliance_issues.length === 0
                          ? 'All compliance checks passed'
                          : `${audit.compliance_issues.length} issue(s) found`}
                      </div>
                    </div>
                  </div>
                </Card>

                {/* Metrics Grid */}
                <div className="grid grid-cols-4 gap-3">
                  <Card className="p-3">
                    <div className="flex items-center gap-2 mb-1">
                      <Clock className="w-4 h-4 text-cyan-400" />
                      <span className="text-[10px] text-slate-500 uppercase">Freshness</span>
                    </div>
                    <div className={`text-lg font-medium ${audit.freshness_status.within_sla ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {audit.freshness_status.actual_hours.toFixed(1)}h
                    </div>
                    <div className="text-[10px] text-slate-500">SLA: {audit.freshness_status.sla_hours}h</div>
                  </Card>

                  <Card className="p-3">
                    <div className="flex items-center gap-2 mb-1">
                      <BarChart3 className="w-4 h-4 text-amber-400" />
                      <span className="text-[10px] text-slate-500 uppercase">Quality</span>
                    </div>
                    <div className={`text-lg font-medium ${audit.quality_score >= audit.product.sla_quality_score ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {(audit.quality_score * 100).toFixed(1)}%
                    </div>
                    <div className="text-[10px] text-slate-500">SLA: {(audit.product.sla_quality_score * 100).toFixed(0)}%</div>
                  </Card>

                  <Card className="p-3">
                    <div className="flex items-center gap-2 mb-1">
                      <Activity className="w-4 h-4 text-violet-400" />
                      <span className="text-[10px] text-slate-500 uppercase">Gate Rate</span>
                    </div>
                    <div className="text-lg font-medium text-slate-200">{(audit.gate_pass_rate * 100).toFixed(0)}%</div>
                    <div className="text-[10px] text-slate-500">{audit.cost_summary.total_executions} runs</div>
                  </Card>

                  <Card className="p-3">
                    <div className="flex items-center gap-2 mb-1">
                      <DollarSign className="w-4 h-4 text-emerald-400" />
                      <span className="text-[10px] text-slate-500 uppercase">Cost</span>
                    </div>
                    <div className="text-lg font-medium text-slate-200">{formatUsd(audit.cost_summary.total_cost_usd)}</div>
                    <div className="text-[10px] text-emerald-400">Saved: {formatUsd(audit.cost_summary.total_saved_usd)}</div>
                  </Card>
                </div>

                {/* Compliance Issues */}
                {audit.compliance_issues.length > 0 && (
                  <Card className="p-3 border-rose-500/20">
                    <div className="text-xs font-medium text-rose-400 mb-2 flex items-center gap-1">
                      <XCircle className="w-3.5 h-3.5" /> Compliance Issues
                    </div>
                    {audit.compliance_issues.map((issue, i) => (
                      <div key={i} className="text-xs text-rose-300 py-1 border-b border-rose-500/10 last:border-0">
                        {issue}
                      </div>
                    ))}
                  </Card>
                )}

                {/* Contract Validation */}
                {audit.contract_validation && (
                  <Card className="p-3">
                    <div className="text-xs font-medium text-slate-400 mb-2 flex items-center gap-1">
                      <FileCheck className="w-3.5 h-3.5" /> Contract Validation
                    </div>
                    <Badge className={audit.contract_validation.passed ? 'bg-emerald-400/10 text-emerald-400' : 'bg-rose-400/10 text-rose-400'}>
                      {audit.contract_validation.passed ? 'PASSING' : 'FAILING'}
                    </Badge>
                    {audit.contract_validation.violations.length > 0 && (
                      <div className="mt-2 space-y-1">
                        {audit.contract_validation.violations.map((v, i) => (
                          <div key={i} className="text-[10px] text-rose-300">
                            {v.check_type}: {v.column} — expected {v.expected}, got {v.actual}
                          </div>
                        ))}
                      </div>
                    )}
                  </Card>
                )}

                {/* Upstream DAG */}
                <Card className="p-3">
                  <div className="text-xs font-medium text-slate-400 mb-2 flex items-center gap-1">
                    <Layers className="w-3.5 h-3.5" /> Provenance Chain ({audit.provenance_chain_length} tables)
                  </div>
                  <div className="flex items-center gap-1 flex-wrap">
                    {audit.upstream_chain.map((name, i) => (
                      <div key={name} className="flex items-center gap-1">
                        <span className="px-2 py-0.5 rounded bg-slate-700/50 text-xs text-slate-300 font-mono">{name}</span>
                        {i < audit.upstream_chain.length - 1 && <ArrowRight className="w-3 h-3 text-slate-600" />}
                      </div>
                    ))}
                  </div>
                </Card>

                {/* Product Details */}
                <Card className="p-3">
                  <div className="text-xs font-medium text-slate-400 mb-2">Product Details</div>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div><span className="text-slate-500">Owner:</span> <span className="text-slate-300">{audit.product.owner}</span></div>
                    <div><span className="text-slate-500">Table:</span> <span className="text-slate-300 font-mono">{audit.product.table_name}</span></div>
                    <div><span className="text-slate-500">Consumers:</span> <span className="text-slate-300">{audit.product.consumers.length > 0 ? audit.product.consumers.join(', ') : 'none'}</span></div>
                    <div><span className="text-slate-500">Skipped:</span> <span className="text-emerald-400">{audit.cost_summary.total_skipped} runs</span></div>
                  </div>
                </Card>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
