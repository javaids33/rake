import { useState } from 'react'
import { cn } from '../../lib/utils'
import { ChevronUp, ChevronDown, Copy, Download } from 'lucide-react'
import toast from 'react-hot-toast'

interface DataTableProps {
  columns: string[]
  rows: Record<string, unknown>[]
  maxHeight?: string
  onExportCsv?: () => void
  compact?: boolean
}

export function DataTable({ columns, rows, maxHeight = '400px', onExportCsv, compact }: DataTableProps) {
  const [sortCol, setSortCol] = useState<string | null>(null)
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('asc')

  const handleSort = (col: string) => {
    if (sortCol === col) {
      setSortDir(d => d === 'asc' ? 'desc' : 'asc')
    } else {
      setSortCol(col)
      setSortDir('asc')
    }
  }

  const sorted = sortCol
    ? [...rows].sort((a, b) => {
        const av = a[sortCol], bv = b[sortCol]
        const cmp = String(av ?? '').localeCompare(String(bv ?? ''), undefined, { numeric: true })
        return sortDir === 'asc' ? cmp : -cmp
      })
    : rows

  const copyToClipboard = () => {
    const header = columns.join('\t')
    const body = sorted.map(r => columns.map(c => String(r[c] ?? '')).join('\t')).join('\n')
    navigator.clipboard.writeText(`${header}\n${body}`)
    toast.success('Copied to clipboard')
  }

  const exportCsv = () => {
    if (onExportCsv) { onExportCsv(); return }
    const header = columns.join(',')
    const body = sorted.map(r => columns.map(c => {
      const v = String(r[c] ?? '')
      return v.includes(',') ? `"${v}"` : v
    }).join(',')).join('\n')
    const blob = new Blob([`${header}\n${body}`], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = 'query-results.csv'
    a.click()
    URL.revokeObjectURL(url)
    toast.success('Downloaded CSV')
  }

  if (!columns.length) return null

  return (
    <div className="flex flex-col">
      <div className="flex items-center justify-between px-3 py-2 border-b border-white/[0.03]">
        <span className="text-2xs font-mono text-zinc-600 readout">{rows.length} row{rows.length !== 1 ? 's' : ''}</span>
        <div className="flex items-center gap-1">
          <button onClick={copyToClipboard} className="p-1.5 rounded-md text-zinc-600 hover:text-amber-400/70 hover:bg-white/[0.03] transition-colors" title="Copy">
            <Copy className="w-3.5 h-3.5" />
          </button>
          <button onClick={exportCsv} className="p-1.5 rounded-md text-zinc-600 hover:text-amber-400/70 hover:bg-white/[0.03] transition-colors" title="Export CSV">
            <Download className="w-3.5 h-3.5" />
          </button>
        </div>
      </div>
      <div className="overflow-auto" style={{ maxHeight }}>
        <table className="w-full text-left">
          <thead className="sticky top-0 z-10">
            <tr className="bg-navy-900/90 backdrop-blur-sm border-b border-white/[0.04]">
              {columns.map(col => (
                <th
                  key={col}
                  onClick={() => handleSort(col)}
                  className={cn(
                    'px-3 py-2 text-2xs font-mono font-semibold text-amber-400/50 cursor-pointer select-none whitespace-nowrap',
                    'hover:text-amber-400/80 transition-colors tracking-wider uppercase',
                    compact && 'px-2 py-1.5'
                  )}
                >
                  <span className="inline-flex items-center gap-1">
                    {col}
                    {sortCol === col && (sortDir === 'asc' ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />)}
                  </span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sorted.map((row, i) => (
              <tr key={i} className="border-b border-white/[0.02] hover:bg-white/[0.015] transition-colors">
                {columns.map(col => (
                  <td key={col} className={cn(
                    'px-3 py-1.5 text-xs font-mono text-zinc-300 whitespace-nowrap max-w-[300px] truncate',
                    compact && 'px-2 py-1',
                    typeof row[col] === 'number' && 'text-cyan-300 readout',
                    row[col] === null && 'text-zinc-700 italic'
                  )}>
                    {row[col] === null ? 'NULL' : String(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
