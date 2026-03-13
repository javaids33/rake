import { useMemo } from 'react'
import { cn } from '../../lib/utils'
import { Hash, Type, ToggleLeft, Calendar, HelpCircle } from 'lucide-react'

interface ColumnProfile {
  name: string
  inferredType: 'number' | 'string' | 'boolean' | 'date' | 'null' | 'mixed'
  totalRows: number
  nonNullCount: number
  nullCount: number
  nullPct: number
  distinctCount: number
  distinctPct: number
  min?: string | number
  max?: string | number
  mean?: number
  mostCommon?: { value: string; count: number }
  sampleValues: string[]
  // numeric-specific
  histogram?: number[] // 10 buckets for sparkline
  // string-specific
  avgLength?: number
  // boolean-specific
  trueCount?: number
  falseCount?: number
}

interface DataProfileProps {
  columns: string[]
  rows: Record<string, unknown>[]
}

function inferType(values: unknown[]): 'number' | 'string' | 'boolean' | 'date' | 'null' | 'mixed' {
  const nonNull = values.filter(v => v !== null && v !== undefined && v !== '')
  if (nonNull.length === 0) return 'null'
  let hasNum = false, hasStr = false, hasBool = false, hasDate = false
  for (const v of nonNull.slice(0, 100)) {
    if (typeof v === 'boolean') { hasBool = true; continue }
    if (typeof v === 'number') { hasNum = true; continue }
    const s = String(v)
    if (/^-?\d+(\.\d+)?$/.test(s)) { hasNum = true; continue }
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) { hasDate = true; continue }
    hasStr = true
  }
  const count = [hasNum, hasStr, hasBool, hasDate].filter(Boolean).length
  if (count > 1) return 'mixed'
  if (hasNum) return 'number'
  if (hasBool) return 'boolean'
  if (hasDate) return 'date'
  return 'string'
}

function buildHistogram(nums: number[], buckets = 10): number[] {
  if (nums.length === 0) return new Array(buckets).fill(0)
  const min = Math.min(...nums)
  const max = Math.max(...nums)
  if (min === max) { const h = new Array(buckets).fill(0); h[0] = nums.length; return h }
  const step = (max - min) / buckets
  const hist = new Array(buckets).fill(0)
  for (const n of nums) {
    const i = Math.min(Math.floor((n - min) / step), buckets - 1)
    hist[i]++
  }
  return hist
}

function profileColumn(name: string, rows: Record<string, unknown>[]): ColumnProfile {
  const values = rows.map(r => r[name])
  const nonNull = values.filter(v => v !== null && v !== undefined && v !== '')
  const totalRows = rows.length
  const nullCount = totalRows - nonNull.length
  const nullPct = totalRows > 0 ? (nullCount / totalRows) * 100 : 0
  const type = inferType(values)

  // Distinct
  const stringified = nonNull.map(v => String(v))
  const distinctSet = new Set(stringified)
  const distinctCount = distinctSet.size
  const distinctPct = nonNull.length > 0 ? (distinctCount / nonNull.length) * 100 : 0

  // Most common
  const freq: Record<string, number> = {}
  for (const s of stringified) { freq[s] = (freq[s] || 0) + 1 }
  const sorted = Object.entries(freq).sort((a, b) => b[1] - a[1])
  const mostCommon = sorted[0] ? { value: sorted[0][0], count: sorted[0][1] } : undefined

  // Samples
  const sampleValues = [...distinctSet].slice(0, 5)

  const profile: ColumnProfile = {
    name, inferredType: type, totalRows, nonNullCount: nonNull.length,
    nullCount, nullPct, distinctCount, distinctPct,
    mostCommon, sampleValues,
  }

  if (type === 'number') {
    const nums = nonNull.map(v => typeof v === 'number' ? v : parseFloat(String(v))).filter(n => !isNaN(n))
    if (nums.length > 0) {
      profile.min = Math.min(...nums)
      profile.max = Math.max(...nums)
      profile.mean = nums.reduce((a, b) => a + b, 0) / nums.length
      profile.histogram = buildHistogram(nums)
    }
  } else if (type === 'string') {
    const lengths = nonNull.map(v => String(v).length)
    profile.avgLength = lengths.length > 0 ? Math.round(lengths.reduce((a, b) => a + b, 0) / lengths.length) : 0
    profile.min = sorted.length > 0 ? sorted[sorted.length - 1][0] : undefined
    profile.max = sorted.length > 0 ? sorted[0][0] : undefined
  } else if (type === 'boolean') {
    profile.trueCount = nonNull.filter(v => v === true || String(v).toLowerCase() === 'true').length
    profile.falseCount = nonNull.length - (profile.trueCount || 0)
  } else if (type === 'date') {
    const strs = stringified.sort()
    profile.min = strs[0]
    profile.max = strs[strs.length - 1]
  }

  return profile
}

const TYPE_ICONS: Record<string, { icon: React.ReactNode; color: string; label: string }> = {
  number: { icon: <Hash className="w-3 h-3" />, color: 'text-cyan-400 bg-cyan-400/10 border-cyan-400/20', label: 'NUM' },
  string: { icon: <Type className="w-3 h-3" />, color: 'text-amber-400 bg-amber-400/10 border-amber-400/20', label: 'STR' },
  boolean: { icon: <ToggleLeft className="w-3 h-3" />, color: 'text-violet-400 bg-violet-400/10 border-violet-400/20', label: 'BOOL' },
  date: { icon: <Calendar className="w-3 h-3" />, color: 'text-emerald-400 bg-emerald-400/10 border-emerald-400/20', label: 'DATE' },
  null: { icon: <HelpCircle className="w-3 h-3" />, color: 'text-zinc-500 bg-zinc-500/10 border-zinc-500/20', label: 'NULL' },
  mixed: { icon: <HelpCircle className="w-3 h-3" />, color: 'text-rose-400 bg-rose-400/10 border-rose-400/20', label: 'MIX' },
}

function MiniBar({ value, max, color = 'bg-amber-400' }: { value: number; max: number; color?: string }) {
  const pct = max > 0 ? (value / max) * 100 : 0
  return (
    <div className="w-full h-1.5 rounded-full bg-zinc-800/60 overflow-hidden">
      <div className={cn('h-full rounded-full transition-all', color)} style={{ width: `${Math.min(pct, 100)}%` }} />
    </div>
  )
}

function Sparkline({ data }: { data: number[] }) {
  const max = Math.max(...data, 1)
  return (
    <div className="flex items-end gap-px h-5">
      {data.map((v, i) => (
        <div
          key={i}
          className="flex-1 bg-cyan-400/40 rounded-t-sm min-w-[2px] transition-all hover:bg-cyan-400/70"
          style={{ height: `${Math.max((v / max) * 100, 4)}%` }}
        />
      ))}
    </div>
  )
}

function BoolBar({ trueCount, falseCount }: { trueCount: number; falseCount: number }) {
  const total = trueCount + falseCount
  const truePct = total > 0 ? (trueCount / total) * 100 : 50
  return (
    <div className="flex items-center gap-2 w-full">
      <div className="flex-1 flex h-2 rounded-full overflow-hidden">
        <div className="bg-emerald-400/60 rounded-l-full" style={{ width: `${truePct}%` }} />
        <div className="bg-rose-400/40 rounded-r-full" style={{ width: `${100 - truePct}%` }} />
      </div>
      <span className="text-2xs text-zinc-500 font-mono whitespace-nowrap">
        {trueCount}T / {falseCount}F
      </span>
    </div>
  )
}

function formatValue(v: string | number | undefined): string {
  if (v === undefined || v === null) return '-'
  if (typeof v === 'number') {
    if (Number.isInteger(v)) return v.toLocaleString()
    return v.toLocaleString(undefined, { maximumFractionDigits: 2 })
  }
  return String(v).length > 24 ? String(v).slice(0, 24) + '...' : String(v)
}

export function DataProfile({ columns, rows }: DataProfileProps) {
  const profiles = useMemo(() => {
    return columns.map(col => profileColumn(col, rows))
  }, [columns, rows])

  if (profiles.length === 0) {
    return (
      <div className="flex items-center justify-center h-full text-zinc-600 text-xs">
        No columns to profile
      </div>
    )
  }

  return (
    <div className="p-4 overflow-auto">
      {/* Summary bar */}
      <div className="flex items-center gap-4 mb-4 px-3 py-2 rounded-lg bg-navy-950/60 border border-white/[0.04]">
        <span className="text-2xs font-semibold text-zinc-400 uppercase tracking-wider">Data Profile</span>
        <span className="text-2xs text-zinc-600 font-mono">{rows.length.toLocaleString()} rows</span>
        <span className="text-2xs text-zinc-600 font-mono">{columns.length} columns</span>
        {(() => {
          const totalNulls = profiles.reduce((a, p) => a + p.nullCount, 0)
          const totalCells = rows.length * columns.length
          const overallNullPct = totalCells > 0 ? ((totalNulls / totalCells) * 100).toFixed(1) : '0'
          return <span className="text-2xs text-zinc-600 font-mono">{overallNullPct}% null overall</span>
        })()}
      </div>

      {/* Column profiles table */}
      <div className="rounded-lg border border-white/[0.04] overflow-hidden">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-white/[0.06] bg-navy-950/40">
              <th className="text-left px-3 py-2.5 text-2xs font-semibold text-zinc-500 uppercase tracking-wider w-[140px]">Column</th>
              <th className="text-left px-3 py-2.5 text-2xs font-semibold text-zinc-500 uppercase tracking-wider w-[50px]">Type</th>
              <th className="text-center px-3 py-2.5 text-2xs font-semibold text-zinc-500 uppercase tracking-wider w-[70px]">Non-null</th>
              <th className="text-left px-3 py-2.5 text-2xs font-semibold text-zinc-500 uppercase tracking-wider w-[80px]">Null %</th>
              <th className="text-center px-3 py-2.5 text-2xs font-semibold text-zinc-500 uppercase tracking-wider w-[60px]">Distinct</th>
              <th className="text-left px-3 py-2.5 text-2xs font-semibold text-zinc-500 uppercase tracking-wider w-[100px]">Min</th>
              <th className="text-left px-3 py-2.5 text-2xs font-semibold text-zinc-500 uppercase tracking-wider w-[100px]">Max</th>
              <th className="text-left px-3 py-2.5 text-2xs font-semibold text-zinc-500 uppercase tracking-wider">Distribution</th>
            </tr>
          </thead>
          <tbody>
            {profiles.map((p, i) => {
              const typeInfo = TYPE_ICONS[p.inferredType] || TYPE_ICONS.mixed
              return (
                <tr key={p.name} className={cn(
                  'border-b border-white/[0.03] transition-colors hover:bg-white/[0.02]',
                  i % 2 === 0 ? 'bg-transparent' : 'bg-white/[0.01]'
                )}>
                  {/* Column name */}
                  <td className="px-3 py-2.5">
                    <span className="font-mono font-medium text-zinc-300 truncate block" title={p.name}>
                      {p.name}
                    </span>
                  </td>

                  {/* Type badge */}
                  <td className="px-3 py-2.5">
                    <span className={cn('inline-flex items-center gap-1 px-1.5 py-0.5 rounded border text-2xs font-medium', typeInfo.color)}>
                      {typeInfo.icon}
                      {typeInfo.label}
                    </span>
                  </td>

                  {/* Non-null count */}
                  <td className="px-3 py-2.5 text-center font-mono text-zinc-400">
                    {p.nonNullCount}
                  </td>

                  {/* Null % with mini bar */}
                  <td className="px-3 py-2.5">
                    <div className="flex items-center gap-2">
                      <div className="w-12">
                        <MiniBar
                          value={p.nullPct}
                          max={100}
                          color={p.nullPct > 50 ? 'bg-rose-400' : p.nullPct > 10 ? 'bg-amber-400' : 'bg-emerald-400'}
                        />
                      </div>
                      <span className={cn(
                        'font-mono text-2xs',
                        p.nullPct > 50 ? 'text-rose-400' : p.nullPct > 10 ? 'text-amber-400' : 'text-zinc-500'
                      )}>
                        {p.nullPct.toFixed(1)}%
                      </span>
                    </div>
                  </td>

                  {/* Distinct count */}
                  <td className="px-3 py-2.5 text-center">
                    <span className="font-mono text-zinc-400">{p.distinctCount}</span>
                    <span className="text-2xs text-zinc-600 ml-0.5">
                      ({p.distinctPct.toFixed(0)}%)
                    </span>
                  </td>

                  {/* Min */}
                  <td className="px-3 py-2.5">
                    <span className="font-mono text-zinc-500 text-2xs truncate block" title={String(p.min ?? '')}>
                      {formatValue(p.min)}
                    </span>
                  </td>

                  {/* Max */}
                  <td className="px-3 py-2.5">
                    <span className="font-mono text-zinc-500 text-2xs truncate block" title={String(p.max ?? '')}>
                      {formatValue(p.max)}
                    </span>
                  </td>

                  {/* Distribution */}
                  <td className="px-3 py-2.5">
                    {p.inferredType === 'number' && p.histogram ? (
                      <div className="w-full max-w-[160px]">
                        <Sparkline data={p.histogram} />
                        {p.mean !== undefined && (
                          <span className="text-2xs text-zinc-600 font-mono">
                            avg {formatValue(p.mean)}
                          </span>
                        )}
                      </div>
                    ) : p.inferredType === 'boolean' ? (
                      <BoolBar trueCount={p.trueCount || 0} falseCount={p.falseCount || 0} />
                    ) : p.inferredType === 'string' && p.avgLength !== undefined ? (
                      <div className="flex flex-col gap-0.5">
                        <span className="text-2xs text-zinc-600 font-mono">
                          avg len {p.avgLength}
                        </span>
                        {p.mostCommon && (
                          <span className="text-2xs text-zinc-500 truncate max-w-[140px]" title={p.mostCommon.value}>
                            top: {formatValue(p.mostCommon.value)} ({p.mostCommon.count}x)
                          </span>
                        )}
                      </div>
                    ) : p.mostCommon ? (
                      <span className="text-2xs text-zinc-600 font-mono truncate block max-w-[140px]" title={p.mostCommon.value}>
                        top: {formatValue(p.mostCommon.value)} ({p.mostCommon.count}x)
                      </span>
                    ) : (
                      <span className="text-2xs text-zinc-700">-</span>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
