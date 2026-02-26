import { NavLink, useLocation } from 'react-router-dom'
import { cn } from '../../lib/utils'
import { useAppStore } from '../../stores/app'
import {
  Home, Terminal, Database, FolderInput, Radio, Search,
  GitBranch, Clock, Settings, Info, Layers, PanelLeftClose, PanelLeft,
  Gauge, ShieldCheck,
} from 'lucide-react'

const nav = [
  { to: '/', icon: Home, label: 'Home', accent: 'amber' },
  { to: '/sql', icon: Terminal, label: 'SQL Editor', accent: 'amber' },
  { to: '/catalog', icon: Database, label: 'Data Catalog', accent: 'cyan' },
  { to: '/sources', icon: FolderInput, label: 'Data Sources', accent: 'cyan' },
  { to: '/streaming', icon: Radio, label: 'Streaming', accent: 'cyan' },
  { to: '/vector', icon: Search, label: 'Vector Search', accent: 'rose' },
  { to: '/transforms', icon: GitBranch, label: 'Transforms', accent: 'violet' },
  { to: '/scheduler', icon: Clock, label: 'Scheduler', accent: 'amber' },
  { to: '/metrics', icon: Gauge, label: 'Engine Metrics', accent: 'emerald' },
  { to: '/quality', icon: ShieldCheck, label: 'Data Quality', accent: 'emerald' },
  { to: '/history', icon: Layers, label: 'Query History', accent: 'emerald' },
  { to: '/settings', icon: Settings, label: 'Settings', accent: 'zinc' },
  { to: '/about', icon: Info, label: 'About', accent: 'zinc' },
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
  const { sidebarCollapsed, toggleSidebar } = useAppStore()

  return (
    <aside className={cn(
      'flex flex-col h-full glass transition-all duration-300 relative z-20',
      'border-r-0 border-amber-500/[0.04]',
      sidebarCollapsed ? 'w-[56px]' : 'w-[224px]'
    )}>
      {/* Right edge glow line */}
      <div className="absolute right-0 top-0 bottom-0 w-px bg-gradient-to-b from-transparent via-amber-400/10 to-transparent" />

      {/* Logo */}
      <div className={cn(
        'flex items-center h-14 border-b border-amber-500/[0.06] px-3',
        sidebarCollapsed ? 'justify-center' : 'gap-3'
      )}>
        <div className="relative w-8 h-8 rounded-xl bg-gradient-to-br from-amber-400 to-amber-600 flex items-center justify-center flex-shrink-0 shadow-lg shadow-amber-500/20">
          <span className="text-navy-950 font-display font-bold text-sm">R</span>
          {/* Logo glow */}
          <div className="absolute inset-0 rounded-xl bg-amber-400/20 blur-md -z-10" />
        </div>
        {!sidebarCollapsed && (
          <div className="flex flex-col min-w-0">
            <span className="text-sm font-display font-bold text-zinc-50 tracking-tight">RustLake</span>
            <span className="text-2xs text-amber-400/50 font-mono tracking-wider">v0.4.0</span>
          </div>
        )}
      </div>

      {/* Nav */}
      <nav className="flex-1 py-3 px-2 space-y-0.5 overflow-y-auto stagger">
        {nav.map(item => {
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
                  ? accent.active
                  : 'text-zinc-500 border-transparent hover:text-zinc-300 hover:bg-white/[0.02] hover:border-white/[0.03]'
              )}
              title={sidebarCollapsed ? item.label : undefined}
            >
              <div className="relative">
                <item.icon className={cn('flex-shrink-0 transition-all duration-200', sidebarCollapsed ? 'w-[18px] h-[18px]' : 'w-4 h-4')} />
                {/* Active indicator dot */}
                {isActive && (
                  <div className={cn('absolute -right-1 -top-1 w-1.5 h-1.5 rounded-full animate-glow-pulse', accent.glow)} />
                )}
              </div>
              {!sidebarCollapsed && (
                <span className="text-[13px] font-medium truncate tracking-[-0.01em]">{item.label}</span>
              )}
            </NavLink>
          )
        })}
      </nav>

      {/* Toggle */}
      <div className="p-2 border-t border-amber-500/[0.04]">
        <button
          onClick={toggleSidebar}
          className="w-full flex items-center justify-center p-2 rounded-lg text-zinc-600 hover:text-amber-400/60 hover:bg-amber-400/[0.03] transition-all duration-200"
        >
          {sidebarCollapsed ? <PanelLeft className="w-4 h-4" /> : <PanelLeftClose className="w-4 h-4" />}
        </button>
      </div>
    </aside>
  )
}
