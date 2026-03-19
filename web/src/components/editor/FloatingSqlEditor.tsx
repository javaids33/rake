import { useState, useCallback, useRef } from 'react'
import { cn } from '../../lib/utils'
import { executeSql } from '../../api/client'
import { Terminal, X, Play, ChevronUp, ChevronDown, Maximize2, Minimize2 } from 'lucide-react'

interface FloatingSqlEditorProps {
  className?: string
}

export function FloatingSqlEditor({ className }: FloatingSqlEditorProps) {
  const [open, setOpen] = useState(false)
  const [expanded, setExpanded] = useState(false)
  const [sql, setSql] = useState('')
  const [result, setResult] = useState<{ rows: number; duration: number; columns: string[]; data: Record<string, unknown>[]; error?: string } | null>(null)
  const [running, setRunning] = useState(false)
  const textareaRef = useRef<HTMLTextAreaElement>(null)

  const handleRun = useCallback(async () => {
    if (!sql.trim() || running) return
    setRunning(true)
    setResult(null)
    try {
      const r = await executeSql(sql, 'auto')
      setResult({
        rows: r.row_count || 0,
        duration: r.duration_ms || 0,
        columns: r.columns || [],
        data: (r.rows || []).slice(0, 20),
      })
    } catch (e: unknown) {
      setResult({
        rows: 0, duration: 0, columns: [], data: [],
        error: (e as Error).message || 'Query failed',
      })
    } finally {
      setRunning(false)
    }
  }, [sql, running])

  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      handleRun()
    }
  }, [handleRun])

  if (!open) {
    return (
      <button
        onClick={() => { setOpen(true); setTimeout(() => textareaRef.current?.focus(), 100) }}
        className={cn(
          'fixed bottom-6 right-6 z-50 w-12 h-12 rounded-full shadow-lg',
          'bg-amber-400/90 hover:bg-amber-400 text-navy-950',
          'flex items-center justify-center transition-all hover:scale-110',
          className
        )}
        title="Quick SQL (floating editor)"
      >
        <Terminal className="w-5 h-5" />
      </button>
    )
  }

  return (
    <div className={cn(
      'fixed z-50 shadow-2xl rounded-xl border border-white/[0.08] bg-navy-950/95 backdrop-blur-sm overflow-hidden flex flex-col',
      expanded
        ? 'bottom-4 right-4 w-[700px] h-[500px]'
        : 'bottom-4 right-4 w-[480px] h-[320px]',
      className
    )}>
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 bg-white/[0.02] border-b border-white/[0.06]">
        <div className="flex items-center gap-2">
          <Terminal className="w-3.5 h-3.5 text-amber-400" />
          <span className="text-xs font-semibold text-zinc-300">Quick SQL</span>
          {result && !result.error && (
            <span className="text-2xs text-zinc-500 font-mono">
              {result.rows} rows in {result.duration}ms
            </span>
          )}
        </div>
        <div className="flex items-center gap-1">
          <button onClick={() => setExpanded(!expanded)} className="p-1 rounded hover:bg-white/[0.06] text-zinc-500 hover:text-zinc-300 transition-colors">
            {expanded ? <Minimize2 className="w-3.5 h-3.5" /> : <Maximize2 className="w-3.5 h-3.5" />}
          </button>
          <button onClick={() => setOpen(false)} className="p-1 rounded hover:bg-white/[0.06] text-zinc-500 hover:text-zinc-300 transition-colors">
            <X className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>

      {/* SQL Input */}
      <div className="relative flex-shrink-0" style={{ height: expanded ? '140px' : '80px' }}>
        <textarea
          ref={textareaRef}
          value={sql}
          onChange={e => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="SELECT * FROM my_table LIMIT 10;  (⌘+Enter to run)"
          className="w-full h-full bg-transparent text-xs font-mono text-cyan-300 placeholder:text-zinc-600 p-3 pr-12 resize-none outline-none border-b border-white/[0.04]"
          spellCheck={false}
        />
        <button
          onClick={handleRun}
          disabled={running || !sql.trim()}
          className={cn(
            'absolute top-2 right-2 p-1.5 rounded-lg transition-colors',
            running ? 'text-zinc-600' : 'text-amber-400 hover:bg-amber-400/10'
          )}
        >
          <Play className="w-4 h-4" />
        </button>
      </div>

      {/* Results */}
      <div className="flex-1 overflow-auto text-2xs font-mono">
        {result?.error && (
          <div className="p-3 text-rose-400">{result.error}</div>
        )}
        {result && !result.error && result.data.length > 0 && (
          <table className="w-full">
            <thead>
              <tr className="border-b border-white/[0.04] bg-white/[0.02]">
                {result.columns.map(col => (
                  <th key={col} className="px-2 py-1.5 text-left text-zinc-500 font-semibold whitespace-nowrap">{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.data.map((row, i) => (
                <tr key={i} className="border-b border-white/[0.02] hover:bg-white/[0.01]">
                  {result.columns.map(col => (
                    <td key={col} className="px-2 py-1 text-zinc-400 whitespace-nowrap max-w-[200px] truncate">
                      {String(row[col] ?? '')}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
        {result && !result.error && result.data.length === 0 && (
          <div className="p-3 text-zinc-600">Query executed — 0 rows returned</div>
        )}
        {!result && !running && (
          <div className="p-3 text-zinc-700">Type a query and press ⌘+Enter</div>
        )}
        {running && (
          <div className="p-3 text-amber-400/60">Running...</div>
        )}
      </div>
    </div>
  )
}
