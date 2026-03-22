import { NavLink, useLocation } from 'react-router-dom'
import { cn } from '../../lib/utils'
import { useAppStore } from '../../stores/app'
import {
  Home, Terminal, Database, FolderInput, Radio, Search,
  GitBranch, Clock, Settings, Info, Layers, PanelLeftClose, PanelLeft,
  Gauge, ShieldCheck, Timer, Sun, Moon, ArrowLeftRight, Plus, FileText,
  Activity, Zap,
} from 'lucide-react'

interface NavItem {
  to: string
  icon: typeof Home
  label: string
  accent: string
}

interface NavSection {
  title?: string
  items: NavItem[]
}

const sections: NavSection[] = [
  {
    items: [
      { to: '/', icon: Home, label: 'Home', accent: 'amber' },
      { to: '/catalog', icon: Database, label: 'Catalog', accent: 'cyan' },
      { to: '/scheduler', icon: Clock, label: 'Jobs & Pipelines', accent: 'amber' },
      { to: '/sources', icon: FolderInput, label: 'Data Sources', accent: 'cyan' },
    ],
  },
  {
    title: 'SQL',
    items: [
      { to: '/sql', icon: Terminal, label: 'SQL Editor', accent: 'amber' },
      { to: '/notebooks', icon: FileText, label: 'Notebooks', accent: 'violet' },
      { to: '/history', icon: Layers, label: 'Query History', accent: 'emerald' },
    ],
  },
  {
    title: 'Data Engineering',
    items: [
      { to: '/streaming', icon: Radio, label: 'Streaming / CDC', accent: 'cyan' },
      { to: '/transforms', icon: GitBranch, label: 'Transforms', accent: 'violet' },
      { to: '/quality', icon: ShieldCheck, label: 'Data Quality', accent: 'cyan' },
      { to: '/glaciers', icon: Zap, label: 'Glaciers', accent: 'amber' },
      { to: '/data-products', icon: ShieldCheck, label: 'Data Products', accent: 'emerald' },
      { to: '/migration', icon: ArrowLeftRight, label: 'Migration', accent: 'rose' },
    ],
  },
  {
    title: 'AI / ML',
    items: [
      { to: '/vector', icon: Search, label: 'Vector Search', accent: 'rose' },
    ],
  },
  {
    title: 'Infrastructure',
    items: [
      { to: '/workflow', icon: Activity, label: 'Workflow Viz', accent: 'rose' },
      { to: '/metrics', icon: Gauge, label: 'Engine Metrics', accent: 'emerald' },
      { to: '/benchmarks', icon: Timer, label: 'Benchmarks', accent: 'amber' },
      { to: '/settings', icon: Settings, label: 'Settings', accent: 'zinc' },
      { to: '/about', icon: Info, label: 'About', accent: 'zinc' },
    ],
  },
]

const accentMap: Record<string, { active: string; glow: string }> = {
  amber: { active: 'text-amber-400 bg-amber-400/[0.08] border-amber-400/20 shadow-glow-amber', glow: 'bg-amber-400' },
  cyan: { active: 'text-cyan-400 bg-cyan-400/[0.08] border-cyan-400/20 shadow-glow-cyan', glow: 'bg-cyan-400' },
  rose: { active: 'text-rose-400 bg-rose-400/[0.08] border-rose-400/20 shadow-glow-rose', glow: 'bg-rose-400' },
  violet: { active: 'text-violet-400 bg-violet-400/[0.08] border-violet-400/20', glow: 'bg-violet-400' },
  emerald: { active: 'text-emerald-400 bg-emerald-400/[0.08] border-emerald-400/20 shadow-glow-emerald', glow: 'bg-emerald-400' },
  zinc: { active: 'text-zinc-300 bg-zinc-400/[0.06] border-zinc-500/20', glow: 'bg-zinc-400' },
}

export function Sidebar() {
  const location = useLocation()
  const { sidebarCollapsed, toggleSidebar, darkMode, toggleTheme } = useAppStore()

  return (
    <aside className={cn(
      'flex flex-col h-full transition-all duration-300 relative z-20',
      darkMode
        ? 'glass border-r-0 border-amber-500/[0.04]'
        : 'bg-white border-r border-slate-200',
      sidebarCollapsed ? 'w-[56px]' : 'w-[224px]'
    )}>
      {/* Right edge glow line — dark only */}
      {darkMode && <div className="absolute right-0 top-0 bottom-0 w-px bg-gradient-to-b from-transparent via-amber-400/10 to-transparent" />}

      {/* Logo + New button */}
      <div className={cn(
        'flex items-center h-14 px-3',
        darkMode ? 'border-b border-amber-500/[0.06]' : 'border-b border-slate-200',
        sidebarCollapsed ? 'justify-center' : 'gap-3'
      )}>
        <div className="relative w-8 h-8 rounded-xl bg-gradient-to-br from-amber-400 to-amber-600 flex items-center justify-center flex-shrink-0 shadow-lg shadow-amber-500/20">
          <span className="text-navy-950 font-display font-bold text-sm">R</span>
          <div className="absolute inset-0 rounded-xl bg-amber-400/20 blur-md -z-10" />
        </div>
        {!sidebarCollapsed && (
          <div className="flex flex-col min-w-0 flex-1">
            <span className={cn('text-sm font-display font-bold tracking-tight', darkMode ? 'text-zinc-50' : 'text-slate-900')}>RustLake</span>
            <span className={cn('text-2xs font-mono tracking-wider', darkMode ? 'text-amber-400/50' : 'text-amber-600/60')}>v0.4.0</span>
          </div>
        )}
      </div>

      {/* + New button */}
      <div className="px-2 pt-3 pb-1">
        <NavLink
          to="/sql"
          className={cn(
            'flex items-center gap-2 rounded-lg font-medium transition-all',
            sidebarCollapsed ? 'justify-center p-2.5' : 'px-3 py-2',
            darkMode
              ? 'bg-amber-400/10 border border-amber-400/20 text-amber-400 hover:bg-amber-400/15'
              : 'bg-amber-500 border border-amber-600 text-white hover:bg-amber-600'
          )}
          title={sidebarCollapsed ? 'New Query' : undefined}
        >
          <Plus className={cn('flex-shrink-0', sidebarCollapsed ? 'w-[18px] h-[18px]' : 'w-4 h-4')} />
          {!sidebarCollapsed && <span className="text-[13px]">New</span>}
        </NavLink>
      </div>

      {/* Nav sections */}
      <nav className="flex-1 py-1 px-2 overflow-y-auto">
        {sections.map((section, si) => (
          <div key={si} className={si > 0 ? 'mt-3' : ''}>
            {section.title && !sidebarCollapsed && (
              <div className={cn(
                'px-3 py-1.5 text-2xs font-semibold uppercase tracking-wider',
                darkMode ? 'text-zinc-600' : 'text-slate-400'
              )}>
                {section.title}
              </div>
            )}
            {section.title && sidebarCollapsed && (
              <div className={cn('mx-2 my-1 h-px', darkMode ? 'bg-white/[0.04]' : 'bg-slate-200')} />
            )}
            <div className="space-y-0.5">
              {section.items.map(item => {
                const isActive = item.to === '/'
                  ? location.pathname === '/'
                  : location.pathname.startsWith(item.to)
                const accent = accentMap[item.accent]
                return (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    className={cn(
                      'group flex items-center gap-2.5 rounded-lg transition-all duration-200 border',
                      sidebarCollapsed ? 'justify-center p-2.5' : 'px-3 py-2',
                      isActive
                        ? darkMode ? accent.active : `text-amber-700 bg-amber-50 border-amber-200`
                        : darkMode
                          ? 'text-zinc-500 border-transparent hover:text-zinc-300 hover:bg-white/[0.02] hover:border-white/[0.03]'
                          : 'text-slate-500 border-transparent hover:text-slate-700 hover:bg-slate-100 hover:border-slate-200'
                    )}
                    title={sidebarCollapsed ? item.label : undefined}
                  >
                    <div className="relative">
                      <item.icon className={cn('flex-shrink-0 transition-all duration-200', sidebarCollapsed ? 'w-[18px] h-[18px]' : 'w-4 h-4')} />
                      {isActive && (
                        <div className={cn('absolute -right-1 -top-1 w-1.5 h-1.5 rounded-full', accent.glow)} />
                      )}
                    </div>
                    {!sidebarCollapsed && (
                      <span className="text-[13px] font-medium truncate tracking-[-0.01em]">{item.label}</span>
                    )}
                  </NavLink>
                )
              })}
            </div>
          </div>
        ))}
      </nav>

      {/* Theme + Collapse toggles */}
      <div className={cn('p-2 space-y-1', darkMode ? 'border-t border-amber-500/[0.04]' : 'border-t border-slate-200')}>
        <button
          onClick={toggleTheme}
          className={cn(
            'w-full flex items-center gap-2.5 p-2 rounded-lg transition-all duration-200',
            sidebarCollapsed ? 'justify-center' : '',
            darkMode
              ? 'text-zinc-500 hover:text-amber-400/80 hover:bg-amber-400/[0.04]'
              : 'text-slate-400 hover:text-amber-600 hover:bg-amber-50'
          )}
          title={darkMode ? 'Switch to light mode' : 'Switch to dark mode'}
        >
          {darkMode ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
          {!sidebarCollapsed && <span className={cn('text-xs', darkMode ? 'text-zinc-500' : 'text-slate-500')}>{darkMode ? 'Light mode' : 'Dark mode'}</span>}
        </button>
        <button
          onClick={toggleSidebar}
          className={cn(
            'w-full flex items-center justify-center p-2 rounded-lg transition-all duration-200',
            darkMode
              ? 'text-zinc-600 hover:text-amber-400/60 hover:bg-amber-400/[0.03]'
              : 'text-slate-400 hover:text-amber-600 hover:bg-amber-50'
          )}
        >
          {sidebarCollapsed ? <PanelLeft className="w-4 h-4" /> : <PanelLeftClose className="w-4 h-4" />}
        </button>
      </div>
    </aside>
  )
}
