import { useState, useEffect } from 'react'
import { isPyodideLoaded, isPyodideLoading } from '../../lib/pyodide'
import { isDuckDBWasmLoaded, isDuckDBWasmLoading } from '../../lib/duckdb-wasm'
import { Cpu, Loader2, CheckCircle2, XCircle } from 'lucide-react'

interface WasmEngine {
  name: string
  loaded: boolean
  loading: boolean
  size: string
  description: string
}

export function WasmStatus() {
  const [engines, setEngines] = useState<WasmEngine[]>([])

  useEffect(() => {
    const interval = setInterval(() => {
      setEngines([
        {
          name: 'Pyodide',
          loaded: isPyodideLoaded(),
          loading: isPyodideLoading(),
          size: '~10 MB',
          description: 'Python runtime (pandas, numpy, matplotlib)',
        },
        {
          name: 'DuckDB-WASM',
          loaded: isDuckDBWasmLoaded(),
          loading: isDuckDBWasmLoading(),
          size: '~8 MB',
          description: 'Offline SQL analytics engine',
        },
        {
          name: 'SQLite-WASM',
          loaded: false,
          loading: false,
          size: '~1 MB',
          description: 'Local persistence layer',
        },
      ])
    }, 1000)
    return () => clearInterval(interval)
  }, [])

  return (
    <div className="space-y-1.5">
      <div className="text-2xs font-semibold text-zinc-500 uppercase tracking-wider px-1">WASM Engines</div>
      {engines.map(e => (
        <div key={e.name} className="flex items-center gap-2 px-2 py-1.5 rounded-lg bg-zinc-800/30 text-xs">
          <Cpu className="w-3.5 h-3.5 text-zinc-500 flex-shrink-0" />
          <span className="text-zinc-300 font-medium flex-1">{e.name}</span>
          <span className="text-zinc-600 text-2xs">{e.size}</span>
          {e.loading ? (
            <Loader2 className="w-3.5 h-3.5 text-amber-400 animate-spin" />
          ) : e.loaded ? (
            <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
          ) : (
            <XCircle className="w-3.5 h-3.5 text-zinc-600" />
          )}
        </div>
      ))}
    </div>
  )
}
