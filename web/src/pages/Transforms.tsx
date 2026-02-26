import { useState, useEffect } from 'react'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { Modal } from '../components/ui/Modal'
import { Input, Textarea, Select } from '../components/ui/Input'
import { DataTable } from '../components/ui/DataTable'
import { EmptyState } from '../components/ui/EmptyState'
import { DagGraph, type DagNode, type DagEdge } from '../components/ui/DagGraph'
import { cn, formatDuration, formatRelativeTime } from '../lib/utils'
import { getTransforms, createTransform, deleteTransform, runTransform, getLineage, getDbtProject, getDbtModels, uploadDbtProject, runDbtModel, runAllDbtModels } from '../api/client'
import type { UserTransform, TransformRunResponse, LineageNode, LineageEdge, DbtModel, DbtSource, DbtRunResponse } from '../types'
import {
  GitBranch, Plus, Play, Trash2, Code2, Network,
  Table2, Eye, FileCode, Clock, ArrowRight, CheckCircle2,
  Search, Layers, RefreshCw, XCircle, Upload, Package,
} from 'lucide-react'
import toast from 'react-hot-toast'

export function Transforms() {
  const [tab, setTab] = useState('models')
  const [transforms, setTransforms] = useState<UserTransform[]>([])
  const [selected, setSelected] = useState<string | null>(null)
  const [runResult, setRunResult] = useState<TransformRunResponse | null>(null)
  const [running, setRunning] = useState(false)
  const [createOpen, setCreateOpen] = useState(false)
  const [lineageNodes, setLineageNodes] = useState<LineageNode[]>([])
  const [lineageEdges, setLineageEdges] = useState<LineageEdge[]>([])
  const [search, setSearch] = useState('')
  // dbt state
  const [dbtModels, setDbtModels] = useState<DbtModel[]>([])
  const [dbtSources, setDbtSources] = useState<DbtSource[]>([])
  const [dbtProjectName, setDbtProjectName] = useState<string | null>(null)
  const [dbtRunResults, setDbtRunResults] = useState<DbtRunResponse[]>([])
  const [dbtRunning, setDbtRunning] = useState(false)

  const [form, setForm] = useState({ name: '', sql: '', depends_on: '', materialization: 'view', description: '' })

  const loadAll = () => {
    getTransforms().then(r => setTransforms(r.transforms || [])).catch(() => {})
    getLineage().then(r => { setLineageNodes(r.nodes || []); setLineageEdges(r.edges || []) }).catch(() => {})
    getDbtProject().then(r => setDbtProjectName(r.name)).catch(() => {})
    getDbtModels().then(r => { setDbtModels(r.models || []); setDbtSources(r.sources || []) }).catch(() => {})
  }
  useEffect(loadAll, [])

  const handleRun = async (name: string) => {
    setRunning(true)
    setRunResult(null)
    try {
      const res = await runTransform(name)
      setRunResult(res)
      toast.success(`Transform completed in ${formatDuration(res.duration_ms)}`)
    } catch (e) { toast.error((e as Error).message) }
    setRunning(false)
  }

  const handleCreate = async () => {
    try {
      await createTransform({
        name: form.name,
        sql: form.sql,
        depends_on: form.depends_on ? form.depends_on.split(',').map(s => s.trim()) : [],
        materialization: form.materialization,
        description: form.description,
      })
      toast.success('Transform created')
      setCreateOpen(false)
      setForm({ name: '', sql: '', depends_on: '', materialization: 'view', description: '' })
      loadAll()
    } catch (e) { toast.error((e as Error).message) }
  }

  const selectedTransform = transforms.find(t => t.name === selected)
  const filteredTransforms = transforms.filter(t => t.name.toLowerCase().includes(search.toLowerCase()))

  return (
    <div className="flex h-full animate-fade-in">
      {/* Left — model list */}
      <div className="w-72 flex-shrink-0 border-r border-white/[0.04] flex flex-col bg-navy-950/60 backdrop-blur-sm">
        <div className="p-3 border-b border-white/[0.04] space-y-3">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="w-7 h-7 rounded-lg bg-violet-400/10 border border-violet-400/20 flex items-center justify-center">
                <GitBranch className="w-3.5 h-3.5 text-violet-400" />
              </div>
              <div>
                <h2 className="text-sm font-display font-semibold text-zinc-100">Transforms</h2>
                <p className="text-2xs text-zinc-600">{transforms.length} models</p>
              </div>
            </div>
            <Button variant="primary" size="sm" icon={<Plus className="w-3 h-3" />} onClick={() => setCreateOpen(true)}>New</Button>
          </div>
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-600" />
            <input
              className="w-full pl-8 pr-3 py-1.5 text-xs rounded-lg bg-white/[0.04] border border-white/[0.06] text-zinc-300 placeholder-zinc-600 focus:outline-none focus:ring-1 focus:ring-amber-400/25 transition-all"
              placeholder="Search models..."
              value={search}
              onChange={e => setSearch(e.target.value)}
            />
          </div>
        </div>
        <div className="flex-1 overflow-y-auto">
          {filteredTransforms.map(t => (
            <button
              key={t.name}
              onClick={() => { setSelected(t.name); setRunResult(null); setTab('models') }}
              className={cn(
                'w-full text-left px-3 py-2.5 border-b border-white/[0.02] transition-all',
                selected === t.name
                  ? 'bg-violet-400/[0.06] border-l-2 border-l-violet-400'
                  : 'hover:bg-white/[0.03] border-l-2 border-l-transparent'
              )}
            >
              <div className="flex items-center gap-2">
                <FileCode className="w-3.5 h-3.5 text-zinc-600" />
                <span className="text-xs font-mono text-zinc-300">{t.name}</span>
              </div>
              <div className="flex items-center gap-1.5 mt-1">
                <Badge className={t.materialization === 'table'
                  ? 'bg-blue-400/10 text-blue-400 border-blue-400/20'
                  : 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20'
                }>
                  {t.materialization}
                </Badge>
                {t.depends_on.length > 0 && <Badge className="bg-white/[0.04] text-zinc-500 border-white/[0.06]">{t.depends_on.length} deps</Badge>}
              </div>
            </button>
          ))}
          {filteredTransforms.length === 0 && (
            <EmptyState icon={<Code2 className="w-5 h-5" />} title="No transforms" description="Create dbt-compatible SQL transforms" />
          )}
        </div>
      </div>

      {/* Right — detail */}
      <div className="flex-1 flex flex-col min-w-0">
        {selectedTransform ? (
          <>
            <div className="flex items-center justify-between px-5 py-3 border-b border-white/[0.04] bg-navy-950/40">
              <div>
                <div className="flex items-center gap-2">
                  <h3 className="text-sm font-display font-semibold text-zinc-100">{selectedTransform.name}</h3>
                  <Badge className={selectedTransform.materialization === 'table'
                    ? 'bg-blue-400/10 text-blue-400 border-blue-400/20'
                    : 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20'
                  }>{selectedTransform.materialization}</Badge>
                </div>
                <p className="text-2xs text-zinc-500 mt-0.5">{selectedTransform.description || 'No description'}</p>
              </div>
              <div className="flex items-center gap-2">
                <Button variant="danger" size="sm" icon={<Trash2 className="w-3.5 h-3.5" />}
                  onClick={async () => {
                    await deleteTransform(selectedTransform.name)
                    setSelected(null)
                    loadAll()
                    toast.success('Deleted')
                  }}>Delete</Button>
                <Button variant="primary" size="sm" icon={<Play className="w-3.5 h-3.5" />}
                  onClick={() => handleRun(selectedTransform.name)} loading={running}>Run</Button>
              </div>
            </div>

            <Tabs
              tabs={[
                { id: 'models', label: 'SQL', icon: <Code2 className="w-3 h-3" /> },
                { id: 'lineage', label: 'Lineage', icon: <Network className="w-3 h-3" /> },
                { id: 'result', label: 'Result', icon: <Table2 className="w-3 h-3" /> },
                { id: 'dbt', label: 'dbt', icon: <Package className="w-3 h-3" />, count: dbtModels.length || undefined },
              ]}
              active={tab}
              onChange={setTab}
              className="mx-5 mt-3"
            />

            <div className="flex-1 overflow-auto p-5">
              {tab === 'models' && (
                <div className="space-y-4">
                  <div className="rounded-xl border border-white/[0.04] overflow-hidden">
                    <div className="px-4 py-2 border-b border-white/[0.04] flex items-center gap-2 bg-white/[0.02]">
                      <Code2 className="w-3.5 h-3.5 text-zinc-500" />
                      <span className="text-2xs font-display font-semibold text-zinc-500 uppercase tracking-wider">Source SQL</span>
                    </div>
                    <pre className="p-4 text-xs font-mono text-zinc-300 overflow-x-auto leading-relaxed bg-navy-950/40">{selectedTransform.sql}</pre>
                  </div>
                  <div className="grid grid-cols-3 gap-3">
                    <Card padding="sm">
                      <p className="text-2xs text-zinc-500">Materialization</p>
                      <p className="text-sm font-semibold text-zinc-200 mt-0.5">{selectedTransform.materialization}</p>
                    </Card>
                    <Card padding="sm">
                      <p className="text-2xs text-zinc-500">Dependencies</p>
                      <div className="flex flex-wrap gap-1 mt-0.5">
                        {selectedTransform.depends_on.length > 0
                          ? selectedTransform.depends_on.map(d => <Badge key={d} className="bg-white/[0.04] text-zinc-400 border-white/[0.06]">{d}</Badge>)
                          : <span className="text-sm text-zinc-500">None</span>
                        }
                      </div>
                    </Card>
                    <Card padding="sm">
                      <p className="text-2xs text-zinc-500">Created</p>
                      <p className="text-sm font-semibold text-zinc-200 mt-0.5">{formatRelativeTime(selectedTransform.created_at)}</p>
                    </Card>
                  </div>
                </div>
              )}

              {tab === 'lineage' && (
                <Card>
                  <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                    <Network className="w-4 h-4 text-violet-400" /> Lineage Graph
                  </h3>
                  {(() => {
                    // Build DAG from transforms
                    const dagNodes: DagNode[] = []
                    const dagEdges: DagEdge[] = []
                    const added = new Set<string>()

                    // Add the selected transform as the primary node
                    if (selectedTransform) {
                      dagNodes.push({
                        id: selectedTransform.name,
                        label: selectedTransform.name,
                        type: 'transform',
                        status: 'healthy',
                        meta: selectedTransform.materialization
                      })
                      added.add(selectedTransform.name)

                      // Add dependencies as source nodes
                      for (const dep of selectedTransform.depends_on || []) {
                        if (!added.has(dep)) {
                          dagNodes.push({ id: dep, label: dep, type: 'source', meta: 'source table' })
                          added.add(dep)
                        }
                        dagEdges.push({ source: dep, target: selectedTransform.name })
                      }

                      // Find transforms that depend on this one (downstream)
                      for (const t of transforms) {
                        if (t.name !== selectedTransform.name && t.depends_on?.includes(selectedTransform.name)) {
                          if (!added.has(t.name)) {
                            dagNodes.push({ id: t.name, label: t.name, type: 'target', status: 'healthy', meta: t.materialization })
                            added.add(t.name)
                          }
                          dagEdges.push({ source: selectedTransform.name, target: t.name })
                        }
                      }
                    }

                    return dagNodes.length > 1 ? (
                      <DagGraph nodes={dagNodes} edges={dagEdges} onNodeClick={(id) => {
                        const found = transforms.find(t => t.name === id)
                        if (found) setSelected(found.name)
                      }} height={300} />
                    ) : (
                      <div style={{ textAlign: 'center', padding: 40, color: '#64748b', fontSize: 13 }}>
                        {selectedTransform?.depends_on?.length ? 'Loading lineage...' : 'No dependencies — this transform has no upstream sources'}
                      </div>
                    )
                  })()}
                </Card>
              )}

              {tab === 'result' && (
                runResult ? (
                  <div className="space-y-3">
                    <Card padding="sm">
                      <div className="flex items-center gap-4 text-xs">
                        <CheckCircle2 className="w-4 h-4 text-emerald-400 flex-shrink-0" />
                        <span className="text-zinc-500">Compiled:</span>
                        <code className="text-zinc-400 font-mono flex-1 truncate">{runResult.compiled_sql}</code>
                        <Badge className="bg-white/[0.04] text-zinc-400 border-white/[0.06]"><Clock className="w-3 h-3" /> {formatDuration(runResult.duration_ms)}</Badge>
                        <Badge className="bg-white/[0.04] text-zinc-400 border-white/[0.06]">{runResult.row_count} rows</Badge>
                      </div>
                    </Card>
                    <DataTable columns={runResult.columns} rows={runResult.rows} />
                  </div>
                ) : (
                  <EmptyState icon={<Eye className="w-5 h-5" />} title="No results" description="Run the transform to see output" />
                )
              )}
            </div>
          </>
        ) : (
          <div className="flex-1 flex items-center justify-center">
            <EmptyState icon={<GitBranch className="w-6 h-6" />} title="Select a transform" description="Choose a model from the list to view SQL, lineage, and run results" />
          </div>
        )}
      </div>

      {/* dbt Tab — rendered outside the selected transform panel */}
      {tab === 'dbt' && (
        <div className="flex-1 overflow-auto p-5 space-y-4">
          {!dbtProjectName && dbtModels.length === 0 ? (
            <Card padding="lg" className="text-center">
              <Package className="w-8 h-8 text-violet-400/60 mx-auto mb-3" />
              <h3 className="text-sm font-display font-semibold text-zinc-200 mb-1">No dbt Project Loaded</h3>
              <p className="text-xs text-zinc-500 mb-4 max-w-md mx-auto">Upload a dbt project to parse models, resolve dependencies, and execute transformations in dependency order.</p>
              <Button variant="primary" size="sm" icon={<Upload className="w-3.5 h-3.5" />} onClick={() => {
                const sampleProject = {
                  name: 'rustlake_demo', version: '1.0.0', uploaded_at: new Date().toISOString(),
                  models: [
                    { name: 'stg_customers', sql: "SELECT * FROM customers", depends_on: [], materialization: 'view', description: 'Staging customers', tags: ['staging'] },
                    { name: 'stg_orders', sql: "SELECT * FROM orders", depends_on: [], materialization: 'view', description: 'Staging orders', tags: ['staging'] },
                    { name: 'fct_revenue', sql: "SELECT c.name, SUM(o.amount) as total FROM ref('stg_customers') c JOIN ref('stg_orders') o ON c.id = o.customer_id GROUP BY c.name", depends_on: ['stg_customers', 'stg_orders'], materialization: 'table', description: 'Revenue by customer', tags: ['mart'] },
                  ],
                  sources: [{ name: 'raw', schema_name: 'public', tables: ['customers', 'orders', 'products'] }],
                }
                uploadDbtProject(sampleProject).then(() => { toast.success('Demo dbt project loaded'); loadAll() }).catch(() => toast.error('Failed to upload'))
              }}>
                Load Demo Project
              </Button>
            </Card>
          ) : (
            <>
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-display font-semibold text-zinc-200 flex items-center gap-2">
                    <Package className="w-4 h-4 text-violet-400" /> dbt Project: {dbtProjectName || 'demo'}
                  </h3>
                  <p className="text-2xs text-zinc-500 mt-0.5">{dbtModels.length} models, {dbtSources.length} sources</p>
                </div>
                <Button variant="primary" size="sm" icon={<Play className="w-3.5 h-3.5" />} loading={dbtRunning} onClick={async () => {
                  setDbtRunning(true)
                  try {
                    const res = await runAllDbtModels()
                    setDbtRunResults(res.results)
                    toast.success(`dbt run complete: ${res.success_count} success, ${res.failure_count} failed`)
                  } catch { toast.error('dbt run failed') }
                  finally { setDbtRunning(false) }
                }}>
                  Run All Models
                </Button>
              </div>

              {/* Models list */}
              <Card padding="none">
                <div className="px-4 py-2.5 border-b border-white/[0.04] bg-white/[0.02]">
                  <span className="text-2xs font-display font-semibold text-zinc-500 uppercase tracking-wider">Models</span>
                </div>
                <div className="divide-y divide-white/[0.02]">
                  {dbtModels.map(model => {
                    const result = dbtRunResults.find(r => r.model === model.name)
                    return (
                      <div key={model.name} className="px-4 py-3 hover:bg-white/[0.01] transition-colors">
                        <div className="flex items-center gap-3">
                          <div className="flex items-center gap-2 flex-1 min-w-0">
                            <Code2 className="w-3.5 h-3.5 text-violet-400 flex-shrink-0" />
                            <span className="text-xs font-medium text-zinc-200">{model.name}</span>
                            <Badge className="bg-violet-400/10 text-violet-400 border-violet-400/20 text-2xs">{model.materialization}</Badge>
                            {model.tags.map(t => <Badge key={t} className="bg-white/[0.04] text-zinc-500 text-2xs">{t}</Badge>)}
                          </div>
                          {result && (
                            <Badge className={result.status === 'success' ? 'bg-emerald-400/10 text-emerald-400 border-emerald-400/20' : 'bg-rose-400/10 text-rose-400 border-rose-400/20'}>
                              {result.status === 'success' ? <CheckCircle2 className="w-3 h-3" /> : <XCircle className="w-3 h-3" />}
                              {result.status} — {formatDuration(result.duration_ms)}
                            </Badge>
                          )}
                          <Button variant="ghost" size="sm" icon={<Play className="w-3 h-3" />} onClick={async () => {
                            try {
                              const res = await runDbtModel(model.name)
                              setDbtRunResults(prev => [...prev.filter(r => r.model !== model.name), res])
                              toast.success(`${model.name}: ${res.status}`)
                            } catch { toast.error(`Failed to run ${model.name}`) }
                          }}>Run</Button>
                        </div>
                        {model.depends_on.length > 0 && (
                          <div className="mt-1 flex items-center gap-1 ml-6">
                            <span className="text-2xs text-zinc-600">depends on:</span>
                            {model.depends_on.map(d => <Badge key={d} className="bg-cyan-400/[0.06] text-cyan-400/60 text-2xs">{d}</Badge>)}
                          </div>
                        )}
                        <p className="text-2xs text-zinc-600 mt-1 ml-6">{model.description}</p>
                        {result?.error && <p className="text-2xs text-rose-400 mt-1 ml-6 font-mono">{result.error}</p>}
                      </div>
                    )
                  })}
                </div>
              </Card>

              {/* Sources */}
              {dbtSources.length > 0 && (
                <Card>
                  <h3 className="text-xs font-display font-semibold text-zinc-300 mb-3 flex items-center gap-2">
                    <Layers className="w-3.5 h-3.5 text-cyan-400" /> Sources
                  </h3>
                  <div className="space-y-2">
                    {dbtSources.map(src => (
                      <div key={src.name} className="flex items-center gap-3">
                        <Badge className="bg-cyan-400/10 text-cyan-400 border-cyan-400/20">{src.name}</Badge>
                        <span className="text-2xs text-zinc-500">schema: {src.schema_name}</span>
                        <span className="text-2xs text-zinc-600">tables: {src.tables.join(', ')}</span>
                      </div>
                    ))}
                  </div>
                </Card>
              )}
            </>
          )}
        </div>
      )}

      {/* Create modal */}
      <Modal open={createOpen} onClose={() => setCreateOpen(false)} title="Create Transform" width="max-w-xl">
        <div className="space-y-4">
          <Input label="Name" value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} placeholder="my_transform" hint="Use snake_case naming convention" />
          <Textarea label="SQL" value={form.sql} onChange={e => setForm(f => ({ ...f, sql: e.target.value }))} placeholder="SELECT * FROM {{ ref('source_table') }} WHERE ..." rows={6} />
          <Input label="Dependencies (comma-separated)" value={form.depends_on} onChange={e => setForm(f => ({ ...f, depends_on: e.target.value }))} placeholder="table1, table2" hint="Tables this transform depends on (for DAG ordering)" />
          <div className="grid grid-cols-2 gap-3">
            <Select label="Materialization" value={form.materialization} onChange={e => setForm(f => ({ ...f, materialization: e.target.value }))}
              options={[{ value: 'view', label: 'View' }, { value: 'table', label: 'Table' }]} />
            <Input label="Description" value={form.description} onChange={e => setForm(f => ({ ...f, description: e.target.value }))} placeholder="What does this do?" />
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" size="sm" onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleCreate}>Create Transform</Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
