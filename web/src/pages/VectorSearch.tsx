import { useState, useEffect } from 'react'
import { Card } from '../components/ui/Card'
import { Button } from '../components/ui/Button'
import { Badge } from '../components/ui/Badge'
import { Tabs } from '../components/ui/Tabs'
import { Modal } from '../components/ui/Modal'
import { Input, Textarea } from '../components/ui/Input'
import { EmptyState } from '../components/ui/EmptyState'
import { Tooltip } from '../components/ui/Tooltip'
import { cn, formatDuration } from '../lib/utils'
import { vectorSearch, vectorIndex, getVectorStatus } from '../api/client'
import type { VectorSearchResult, VectorStatusResponse } from '../types'
import {
  Search, Brain, Cpu, Database, Layers, Plus, Sparkles,
  ArrowRight, Gauge, Hash, Zap, Shield, Settings,
} from 'lucide-react'
import toast from 'react-hot-toast'

const INDEX_TYPES = [
  { name: 'Brute Force', desc: 'Exact nearest neighbor — small datasets', perf: 'O(n)', color: 'text-blue-400', tip: 'Scans every vector. 100% recall, but slow on large datasets. Best for < 10K vectors.' },
  { name: 'IVF-PQ', desc: 'Inverted file + Product Quantization', perf: 'O(√n)', color: 'text-emerald-400', tip: 'Clusters vectors into partitions, then compresses with PQ. Good balance of speed and recall for 10K–10M vectors.' },
  { name: 'HNSW', desc: 'Hierarchical Navigable Small World graph', perf: 'O(log n)', color: 'text-violet-400', tip: 'Graph-based index with navigable small-world layers. Fastest queries, higher memory usage. Best for > 100K vectors.' },
]

export function VectorSearch() {
  const [tab, setTab] = useState('search')
  const [query, setQuery] = useState('')
  const [k, setK] = useState(10)
  const [results, setResults] = useState<VectorSearchResult[]>([])
  const [searchTime, setSearchTime] = useState<number | null>(null)
  const [loading, setLoading] = useState(false)
  const [status, setStatus] = useState<VectorStatusResponse | null>(null)
  const [indexOpen, setIndexOpen] = useState(false)
  const [docText, setDocText] = useState('')

  useEffect(() => {
    getVectorStatus().then(setStatus).catch(() => {})
  }, [])

  const handleSearch = async () => {
    if (!query.trim()) return
    setLoading(true)
    try {
      const res = await vectorSearch(query, k)
      setResults(res.results)
      setSearchTime(res.duration_ms)
    } catch (e) { toast.error((e as Error).message) }
    setLoading(false)
  }

  const handleIndex = async () => {
    const lines = docText.trim().split('\n').filter(Boolean)
    if (!lines.length) return
    const documents = lines.map((text, i) => ({ id: `doc-${Date.now()}-${i}`, text }))
    try {
      await vectorIndex(documents)
      toast.success(`Indexed ${documents.length} documents`)
      setIndexOpen(false)
      setDocText('')
      getVectorStatus().then(setStatus)
    } catch (e) { toast.error((e as Error).message) }
  }

  return (
    <div className="flex flex-col h-full animate-fade-in">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-white/[0.04]">
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-rose-400/10 border border-rose-400/20 flex items-center justify-center">
            <Search className="w-4.5 h-4.5 text-rose-400" />
          </div>
          <div>
            <h1 className="text-base font-display font-bold text-zinc-100">Vector Search</h1>
            <p className="text-2xs text-zinc-500">Semantic similarity search with Lance vector indexes</p>
          </div>
        </div>
        <Button variant="primary" size="sm" icon={<Plus className="w-3.5 h-3.5" />} onClick={() => setIndexOpen(true)}>Index Documents</Button>
      </div>

      {/* Status strip */}
      {status && (
        <div className="grid grid-cols-4 gap-px bg-white/[0.02] border-b border-white/[0.04]">
          {[
            { label: 'Documents', value: String(status.document_count), icon: Database, color: 'text-rose-400', tip: 'Total documents indexed in the Lance vector store' },
            { label: 'Dimensions', value: String(status.dimensions), icon: Hash, color: 'text-violet-400', tip: 'Embedding vector dimensionality — determined by the model' },
            { label: 'Model', value: status.model, icon: Brain, color: 'text-cyan-400', tip: 'Embedding model used to convert text to vectors' },
            { label: 'Index Type', value: status.index_type, icon: Layers, color: 'text-emerald-400', tip: 'Index algorithm: Brute Force (exact), IVF-PQ (approximate), or HNSW (graph-based)' },
          ].map(s => (
            <Tooltip key={s.label} content={s.tip} position="bottom">
              <div className="flex items-center gap-3 px-4 py-3 bg-navy-950/60 cursor-help">
                <s.icon className={cn('w-4 h-4', s.color)} />
                <div>
                  <p className="text-sm font-bold font-mono text-zinc-100">{s.value}</p>
                  <p className="text-2xs text-zinc-600">{s.label}</p>
                </div>
              </div>
            </Tooltip>
          ))}
        </div>
      )}

      <Tabs
        tabs={[
          { id: 'search', label: 'Search', icon: <Sparkles className="w-3 h-3" /> },
          { id: 'index', label: 'Index Info', icon: <Cpu className="w-3 h-3" /> },
          { id: 'lance', label: 'Lance Format', icon: <Shield className="w-3 h-3" /> },
        ]}
        active={tab}
        onChange={setTab}
        className="mx-6 mt-3"
      />

      <div className="flex-1 overflow-auto p-6">
        {tab === 'search' && (
          <div className="max-w-4xl mx-auto space-y-4">
            {/* Search box */}
            <div className="rounded-xl border border-white/[0.06] bg-white/[0.02] p-4 flex items-center gap-3">
              <Sparkles className="w-5 h-5 text-rose-400 flex-shrink-0" />
              <input
                className="flex-1 bg-transparent border-none text-sm text-zinc-100 placeholder-zinc-600 focus:outline-none"
                placeholder="Enter natural language query for semantic search..."
                value={query}
                onChange={e => setQuery(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleSearch()}
              />
              <div className="flex items-center gap-2 flex-shrink-0">
                <Tooltip content="Number of nearest neighbors to return" position="top">
                  <span className="text-2xs text-zinc-600 cursor-help">k=</span>
                </Tooltip>
                <input
                  className="w-12 text-center text-xs font-mono rounded-lg bg-white/[0.04] border border-white/[0.06] text-zinc-300 py-1 focus:outline-none focus:ring-1 focus:ring-amber-400/25"
                  type="number"
                  value={k}
                  onChange={e => setK(parseInt(e.target.value) || 10)}
                  min={1}
                  max={100}
                />
                <Button variant="primary" size="sm" onClick={handleSearch} loading={loading} icon={<ArrowRight className="w-3.5 h-3.5" />}>
                  Search
                </Button>
              </div>
            </div>

            {/* Results meta */}
            {searchTime !== null && (
              <div className="flex items-center gap-2">
                <Badge className="bg-rose-400/10 text-rose-400 border-rose-400/20">{results.length} results</Badge>
                <Badge className="bg-white/[0.04] text-zinc-400 border-white/[0.06]"><Gauge className="w-3 h-3" /> {formatDuration(searchTime)}</Badge>
              </div>
            )}

            {/* Results */}
            {results.length === 0 && searchTime !== null ? (
              <EmptyState icon={<Search className="w-5 h-5" />} title="No results" description="Try a different query or index more documents" />
            ) : (
              <div className="space-y-2">
                {results.map((r, i) => (
                  <div
                    key={r.id}
                    className="flex items-start gap-3 p-4 rounded-xl border border-white/[0.04] bg-white/[0.02] hover:bg-white/[0.03] hover:border-white/[0.06] transition-all"
                  >
                    <div className="w-8 h-8 rounded-lg bg-white/[0.04] border border-white/[0.06] flex items-center justify-center text-xs font-mono text-zinc-500 flex-shrink-0">
                      {i + 1}
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-xs text-zinc-300 leading-relaxed">{r.text}</p>
                      {r.metadata && Object.keys(r.metadata).length > 0 && (
                        <div className="flex items-center gap-1.5 mt-2">
                          {Object.entries(r.metadata).map(([k, v]) => (
                            <Badge key={k} className="bg-white/[0.04] text-zinc-500 border-white/[0.06]">{k}: {String(v)}</Badge>
                          ))}
                        </div>
                      )}
                    </div>
                    <div className="flex-shrink-0 text-right">
                      <div className="text-sm font-mono font-bold text-rose-400">{(r.similarity_score * 100).toFixed(1)}%</div>
                      <div className="text-2xs text-zinc-600">similarity</div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {tab === 'index' && (
          <div className="max-w-4xl mx-auto space-y-4">
            <Card>
              <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4 flex items-center gap-2">
                <Settings className="w-4 h-4 text-zinc-500" /> Index Configuration
              </h3>
              <div className="grid grid-cols-2 gap-4 text-xs">
                <div className="space-y-2.5">
                  {[
                    ['Storage Format', 'Lance'],
                    ['Index Type', status?.index_type || 'IVF-PQ'],
                    ['Embedding Model', status?.model || 'simple-embed'],
                    ['Dimensions', String(status?.dimensions || 128)],
                  ].map(([label, val]) => (
                    <div key={label} className="flex justify-between">
                      <span className="text-zinc-500">{label}</span>
                      <span className="text-zinc-300 font-mono">{val}</span>
                    </div>
                  ))}
                </div>
                <div className="space-y-2.5">
                  {[
                    ['Distance Metric', 'Cosine'],
                    ['Random Access', '100x faster than Parquet'],
                    ['Document Count', String(status?.document_count || 0)],
                    ['Encoding', 'Product Quantization'],
                  ].map(([label, val]) => (
                    <div key={label} className="flex justify-between">
                      <span className="text-zinc-500">{label}</span>
                      <span className="text-zinc-300 font-mono">{val}</span>
                    </div>
                  ))}
                </div>
              </div>
            </Card>

            {/* Index type comparison */}
            <h3 className="text-sm font-display font-semibold text-zinc-200">Supported Index Types</h3>
            <div className="grid grid-cols-3 gap-3">
              {INDEX_TYPES.map(idx => (
                <Tooltip key={idx.name} content={idx.tip} position="bottom">
                  <Card padding="sm" hover className="cursor-help">
                    <h4 className={cn('text-sm font-semibold', idx.color)}>{idx.name}</h4>
                    <p className="text-2xs text-zinc-500 mt-1">{idx.desc}</p>
                    <div className="flex items-center gap-2 mt-2">
                      <Badge className="bg-white/[0.04] text-zinc-400 border-white/[0.06] font-mono">{idx.perf}</Badge>
                    </div>
                  </Card>
                </Tooltip>
              ))}
            </div>
          </div>
        )}

        {tab === 'lance' && (
          <div className="max-w-4xl mx-auto space-y-4">
            <Card>
              <h3 className="text-sm font-display font-semibold text-zinc-200 mb-4">Lance vs Parquet Comparison</h3>
              <div className="overflow-hidden rounded-lg border border-white/[0.04]">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="bg-white/[0.02]">
                      <th className="px-4 py-2.5 text-left text-zinc-400 font-medium">Feature</th>
                      <th className="px-4 py-2.5 text-left text-rose-400 font-medium">Lance</th>
                      <th className="px-4 py-2.5 text-left text-violet-400 font-medium">Parquet</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-white/[0.03]">
                    {[
                      ['Random Access', '~1ms', '~100ms'],
                      ['Vector Search', 'Native IVF-PQ / HNSW', 'N/A (brute force only)'],
                      ['Column Scan', 'Comparable', 'Optimized'],
                      ['Compression', 'Good', 'Excellent'],
                      ['Mutability', 'Update / Delete', 'Append-only'],
                      ['Multimodal', 'Images, video, audio', 'Tabular only'],
                      ['Best For', 'AI/ML workloads', 'Analytics / BI'],
                    ].map(([feat, lance, parquet]) => (
                      <tr key={feat} className="hover:bg-white/[0.02] transition-colors">
                        <td className="px-4 py-2.5 text-zinc-400">{feat}</td>
                        <td className="px-4 py-2.5 text-zinc-300 font-mono">{lance}</td>
                        <td className="px-4 py-2.5 text-zinc-300 font-mono">{parquet}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </Card>

            {/* Why dual format */}
            <Card>
              <h3 className="text-sm font-display font-semibold text-zinc-200 mb-3 flex items-center gap-2">
                <Zap className="w-4 h-4 text-amber-400" /> Why Dual-Format Lakehouse?
              </h3>
              <p className="text-xs text-zinc-400 leading-relaxed mb-4">
                RustLake runs Iceberg + Parquet for analytics and Lance for AI workloads on the same object storage. The query router inspects the SQL AST and dispatches to the optimal format automatically.
              </p>
              <div className="flex items-center justify-between text-xs py-4">
                <div className="flex flex-col items-center gap-1.5 w-24">
                  <div className="w-10 h-10 rounded-xl bg-blue-400/10 border border-blue-400/20 flex items-center justify-center">
                    <Database className="w-5 h-5 text-blue-400" />
                  </div>
                  <span className="text-zinc-400 text-2xs">Iceberg</span>
                </div>
                <div className="flex-1 border-t border-dashed border-white/[0.08] mx-4 relative">
                  <span className="absolute -top-2.5 left-1/2 -translate-x-1/2 text-2xs text-zinc-600 bg-navy-950 px-2">same S3 bucket</span>
                </div>
                <div className="flex flex-col items-center gap-1.5 w-24">
                  <div className="w-10 h-10 rounded-xl bg-rose-400/10 border border-rose-400/20 flex items-center justify-center">
                    <Layers className="w-5 h-5 text-rose-400" />
                  </div>
                  <span className="text-zinc-400 text-2xs">Lance</span>
                </div>
              </div>
            </Card>
          </div>
        )}
      </div>

      {/* Index documents modal */}
      <Modal open={indexOpen} onClose={() => setIndexOpen(false)} title="Index Documents">
        <div className="space-y-3">
          <Textarea label="Documents (one per line)" value={docText} onChange={e => setDocText(e.target.value)} placeholder="Each line will be embedded and indexed as a separate document..." rows={8} />
          <p className="text-2xs text-zinc-600">Documents are embedded using the configured model ({status?.model || 'simple-embed'}) and stored in Lance format.</p>
          <div className="flex justify-end gap-2 pt-2">
            <Button variant="secondary" size="sm" onClick={() => setIndexOpen(false)}>Cancel</Button>
            <Button variant="primary" size="sm" onClick={handleIndex}>Index</Button>
          </div>
        </div>
      </Modal>
    </div>
  )
}
