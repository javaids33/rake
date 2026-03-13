import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { cn } from '../../lib/utils'
import { useAppStore } from '../../stores/app'
import {
  Search, Home, Terminal, BookOpen, Database, Radio, Layers, GitBranch,
  Clock, BarChart3, ArrowLeftRight, Gauge, Settings, Info, Table2,
  Play, Plus, Sun, Moon, Download, FileText, Zap, Command, CornerDownLeft,
  ArrowUp, ArrowDown,
} from 'lucide-react'

interface CommandItem {
  id: string
  label: string
  description?: string
  icon: React.ReactNode
  category: 'page' | 'table' | 'query' | 'action'
  action: () => void
  keywords?: string[]
}

interface CommandPaletteProps {
  open: boolean
  onClose: () => void
  tables: string[]
  savedQueries: Array<{ id: string; name: string; sql: string }>
  onRunQuery?: () => void
  onNewTab?: () => void
  onExportCsv?: () => void
  onInsertSql?: (sql: string) => void
}

function fuzzyMatch(query: string, text: string): { match: boolean; score: number } {
  const q = query.toLowerCase()
  const t = text.toLowerCase()
  if (!q) return { match: true, score: 0 }
  if (t.includes(q)) return { match: true, score: t.indexOf(q) === 0 ? 100 : 80 }
  let qi = 0
  let score = 0
  for (let ti = 0; ti < t.length && qi < q.length; ti++) {
    if (t[ti] === q[qi]) {
      score += (ti === 0 || t[ti - 1] === '.' || t[ti - 1] === '_' || t[ti - 1] === ' ') ? 10 : 5
      qi++
    }
  }
  return qi === q.length ? { match: true, score } : { match: false, score: 0 }
}

const PAGE_ITEMS: Array<{ id: string; label: string; path: string; icon: React.ReactNode; keywords: string[] }> = [
  { id: 'home', label: 'Home', path: '/', icon: <Home className="w-4 h-4" />, keywords: ['dashboard', 'overview'] },
  { id: 'sql', label: 'SQL Editor', path: '/sql', icon: <Terminal className="w-4 h-4" />, keywords: ['query', 'editor', 'write'] },
  { id: 'catalog', label: 'Data Catalog', path: '/catalog', icon: <BookOpen className="w-4 h-4" />, keywords: ['tables', 'schema', 'browse'] },
  { id: 'sources', label: 'Data Sources', path: '/sources', icon: <Database className="w-4 h-4" />, keywords: ['connections', 'postgres', 'mysql', 's3'] },
  { id: 'quality', label: 'Data Quality', path: '/quality', icon: <Zap className="w-4 h-4" />, keywords: ['health', 'nulls', 'freshness'] },
  { id: 'streaming', label: 'Streaming', path: '/streaming', icon: <Radio className="w-4 h-4" />, keywords: ['kafka', 'cdc', 'pipeline', 'events'] },
  { id: 'vector', label: 'Vector Search', path: '/vector', icon: <Layers className="w-4 h-4" />, keywords: ['embeddings', 'similarity', 'lance'] },
  { id: 'transforms', label: 'Transforms', path: '/transforms', icon: <GitBranch className="w-4 h-4" />, keywords: ['dbt', 'model', 'lineage'] },
  { id: 'scheduler', label: 'Scheduler', path: '/scheduler', icon: <Clock className="w-4 h-4" />, keywords: ['jobs', 'cron', 'etl'] },
  { id: 'benchmarks', label: 'Benchmarks', path: '/benchmarks', icon: <BarChart3 className="w-4 h-4" />, keywords: ['tpch', 'performance'] },
  { id: 'migration', label: 'Migration', path: '/migration', icon: <ArrowLeftRight className="w-4 h-4" />, keywords: ['trino', 'migrate'] },
  { id: 'metrics', label: 'Engine Metrics', path: '/metrics', icon: <Gauge className="w-4 h-4" />, keywords: ['cpu', 'memory', 'latency'] },
  { id: 'history', label: 'Query History', path: '/history', icon: <FileText className="w-4 h-4" />, keywords: ['past', 'log', 'replay'] },
  { id: 'settings', label: 'Settings', path: '/settings', icon: <Settings className="w-4 h-4" />, keywords: ['config', 'routing', 'flight'] },
  { id: 'about', label: 'About', path: '/about', icon: <Info className="w-4 h-4" />, keywords: ['architecture', 'docs'] },
]

export function CommandPalette({
  open, onClose, tables, savedQueries,
  onRunQuery, onNewTab, onExportCsv, onInsertSql,
}: CommandPaletteProps) {
  const [query, setQuery] = useState('')
  const [selectedIndex, setSelectedIndex] = useState(0)
  const navigate = useNavigate()
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)
  const { darkMode, toggleTheme } = useAppStore()

  // Build command items
  const items: CommandItem[] = useMemo(() => {
    const cmds: CommandItem[] = []

    // Pages
    for (const page of PAGE_ITEMS) {
      cmds.push({
        id: `page:${page.id}`,
        label: page.label,
        description: `Go to ${page.label}`,
        icon: page.icon,
        category: 'page',
        action: () => { navigate(page.path); onClose() },
        keywords: page.keywords,
      })
    }

    // Tables
    for (const table of tables) {
      cmds.push({
        id: `table:${table}`,
        label: table,
        description: 'Query table',
        icon: <Table2 className="w-4 h-4" />,
        category: 'table',
        action: () => {
          onInsertSql?.(`SELECT * FROM ${table} LIMIT 100;`)
          navigate('/sql')
          onClose()
        },
        keywords: [table.replace(/\./g, ' ')],
      })
    }

    // Saved queries
    for (const sq of savedQueries) {
      cmds.push({
        id: `query:${sq.id}`,
        label: sq.name,
        description: sq.sql.slice(0, 60) + (sq.sql.length > 60 ? '...' : ''),
        icon: <FileText className="w-4 h-4" />,
        category: 'query',
        action: () => {
          onInsertSql?.(sq.sql)
          navigate('/sql')
          onClose()
        },
      })
    }

    // Actions
    cmds.push(
      {
        id: 'action:run', label: 'Run Query', description: 'Execute current SQL (Cmd+Enter)',
        icon: <Play className="w-4 h-4" />, category: 'action',
        action: () => { onRunQuery?.(); onClose() },
        keywords: ['execute'],
      },
      {
        id: 'action:newtab', label: 'New Query Tab', description: 'Open a new editor tab',
        icon: <Plus className="w-4 h-4" />, category: 'action',
        action: () => { onNewTab?.(); navigate('/sql'); onClose() },
        keywords: ['tab', 'new'],
      },
      {
        id: 'action:theme', label: darkMode ? 'Switch to Light Mode' : 'Switch to Dark Mode',
        description: 'Toggle the color theme',
        icon: darkMode ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />,
        category: 'action',
        action: () => { toggleTheme(); onClose() },
        keywords: ['theme', 'dark', 'light', 'mode'],
      },
      {
        id: 'action:export', label: 'Export Results as CSV', description: 'Download current query results',
        icon: <Download className="w-4 h-4" />, category: 'action',
        action: () => { onExportCsv?.(); onClose() },
        keywords: ['download', 'csv'],
      },
    )

    return cmds
  }, [tables, savedQueries, darkMode, navigate, onClose, onInsertSql, onRunQuery, onNewTab, onExportCsv, toggleTheme])

  // Filter & sort
  const filtered = useMemo(() => {
    if (!query.trim()) {
      // Show pages + actions by default, limit tables
      return items.filter(i => i.category === 'page' || i.category === 'action' || i.category === 'query')
    }
    return items
      .map(item => {
        const nameMatch = fuzzyMatch(query, item.label)
        const descMatch = item.description ? fuzzyMatch(query, item.description) : { match: false, score: 0 }
        const kwMatch = (item.keywords || []).reduce((best, kw) => {
          const m = fuzzyMatch(query, kw)
          return m.score > best.score ? m : best
        }, { match: false, score: 0 })
        const bestScore = Math.max(nameMatch.score, descMatch.score, kwMatch.score)
        const match = nameMatch.match || descMatch.match || kwMatch.match
        return { item, score: bestScore, match }
      })
      .filter(r => r.match)
      .sort((a, b) => b.score - a.score)
      .map(r => r.item)
      .slice(0, 20)
  }, [items, query])

  // Group by category
  const grouped = useMemo(() => {
    const groups: Record<string, CommandItem[]> = {}
    const order: string[] = []
    for (const item of filtered) {
      if (!groups[item.category]) {
        groups[item.category] = []
        order.push(item.category)
      }
      groups[item.category].push(item)
    }
    return { groups, order }
  }, [filtered])

  const flatItems = useMemo(() => filtered, [filtered])

  // Reset on open
  useEffect(() => {
    if (open) {
      setQuery('')
      setSelectedIndex(0)
      setTimeout(() => inputRef.current?.focus(), 50)
    }
  }, [open])

  // Keyboard navigation
  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setSelectedIndex(i => Math.min(i + 1, flatItems.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setSelectedIndex(i => Math.max(i - 1, 0))
    } else if (e.key === 'Enter' && flatItems[selectedIndex]) {
      e.preventDefault()
      flatItems[selectedIndex].action()
    } else if (e.key === 'Escape') {
      onClose()
    }
  }, [flatItems, selectedIndex, onClose])

  // Scroll selected into view
  useEffect(() => {
    const el = listRef.current?.querySelector(`[data-index="${selectedIndex}"]`)
    el?.scrollIntoView({ block: 'nearest' })
  }, [selectedIndex])

  // Reset selection on query change
  useEffect(() => { setSelectedIndex(0) }, [query])

  // Global Cmd+K listener
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        if (open) onClose()
      }
    }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [open, onClose])

  if (!open) return null

  const CATEGORY_LABELS: Record<string, string> = {
    page: 'Pages',
    table: 'Tables',
    query: 'Saved Queries',
    action: 'Actions',
  }
  const CATEGORY_COLORS: Record<string, string> = {
    page: 'text-amber-400/60',
    table: 'text-cyan-400/60',
    query: 'text-violet-400/60',
    action: 'text-emerald-400/60',
  }

  let flatIndex = -1

  return (
    <div className="fixed inset-0 z-[100] flex items-start justify-center pt-[15vh]">
      {/* Backdrop */}
      <div className="absolute inset-0 bg-navy-950/70 backdrop-blur-sm" onClick={onClose} />

      {/* Palette */}
      <div className={cn(
        'relative w-full max-w-[560px] mx-4 rounded-xl shadow-2xl overflow-hidden',
        'border border-white/[0.08] bg-navy-950/95 backdrop-blur-xl',
        'animate-fade-in',
        // Subtle amber glow at top
        'before:absolute before:inset-x-0 before:top-0 before:h-px before:bg-gradient-to-r before:from-transparent before:via-amber-400/30 before:to-transparent'
      )}>
        {/* Search input */}
        <div className="flex items-center gap-3 px-4 py-3 border-b border-white/[0.06]">
          <Search className="w-4 h-4 text-zinc-500 flex-shrink-0" />
          <input
            ref={inputRef}
            value={query}
            onChange={e => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Search pages, tables, queries, actions..."
            className="flex-1 bg-transparent text-sm text-zinc-200 placeholder-zinc-600 outline-none font-mono"
            spellCheck={false}
            autoComplete="off"
          />
          <kbd className="hidden sm:flex items-center gap-0.5 px-1.5 py-0.5 text-2xs text-zinc-600 border border-zinc-700/50 rounded bg-zinc-800/30 font-mono">
            esc
          </kbd>
        </div>

        {/* Results */}
        <div ref={listRef} className="max-h-[50vh] overflow-y-auto overscroll-contain">
          {flatItems.length === 0 ? (
            <div className="px-4 py-8 text-center">
              <p className="text-xs text-zinc-600">No results for &ldquo;{query}&rdquo;</p>
            </div>
          ) : (
            grouped.order.map(category => (
              <div key={category}>
                <div className="px-4 pt-3 pb-1.5">
                  <span className={cn('text-2xs font-semibold uppercase tracking-wider', CATEGORY_COLORS[category])}>
                    {CATEGORY_LABELS[category]} ({grouped.groups[category].length})
                  </span>
                </div>
                {grouped.groups[category].map(item => {
                  flatIndex++
                  const idx = flatIndex
                  const isSelected = idx === selectedIndex
                  return (
                    <button
                      key={item.id}
                      data-index={idx}
                      onClick={item.action}
                      onMouseEnter={() => setSelectedIndex(idx)}
                      className={cn(
                        'w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors duration-75',
                        isSelected
                          ? 'bg-amber-400/[0.08] border-l-2 border-amber-400/60'
                          : 'border-l-2 border-transparent hover:bg-white/[0.03]'
                      )}
                    >
                      <span className={cn(
                        'flex-shrink-0',
                        isSelected ? 'text-amber-400' : 'text-zinc-600'
                      )}>
                        {item.icon}
                      </span>
                      <div className="flex-1 min-w-0">
                        <span className={cn(
                          'text-sm font-medium block truncate',
                          isSelected ? 'text-zinc-100' : 'text-zinc-400'
                        )}>
                          {item.label}
                        </span>
                        {item.description && (
                          <span className="text-2xs text-zinc-600 block truncate font-mono mt-0.5">
                            {item.description}
                          </span>
                        )}
                      </div>
                      {isSelected && (
                        <CornerDownLeft className="w-3.5 h-3.5 text-amber-400/60 flex-shrink-0" />
                      )}
                    </button>
                  )
                })}
              </div>
            ))
          )}
        </div>

        {/* Footer hints */}
        <div className="flex items-center gap-4 px-4 py-2 border-t border-white/[0.04] bg-navy-950/60">
          <span className="flex items-center gap-1.5 text-2xs text-zinc-600">
            <ArrowUp className="w-3 h-3" /><ArrowDown className="w-3 h-3" /> navigate
          </span>
          <span className="flex items-center gap-1.5 text-2xs text-zinc-600">
            <CornerDownLeft className="w-3 h-3" /> select
          </span>
          <span className="flex items-center gap-1.5 text-2xs text-zinc-600">
            <Command className="w-3 h-3" />K toggle
          </span>
          <span className="ml-auto text-2xs text-zinc-700 font-mono">
            {flatItems.length} result{flatItems.length !== 1 ? 's' : ''}
          </span>
        </div>
      </div>
    </div>
  )
}
