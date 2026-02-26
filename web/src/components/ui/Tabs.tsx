import { cn } from '../../lib/utils'

interface TabsProps {
  tabs: Array<{ id: string; label: string; icon?: React.ReactNode; count?: number }>
  active: string
  onChange: (id: string) => void
  className?: string
}

export function Tabs({ tabs, active, onChange, className }: TabsProps) {
  return (
    <div className={cn('flex gap-1 p-1 bg-navy-950/60 rounded-lg border border-white/[0.04] backdrop-blur-sm', className)}>
      {tabs.map(tab => (
        <button
          key={tab.id}
          onClick={() => onChange(tab.id)}
          className={cn(
            'flex items-center gap-2 px-3 py-1.5 text-xs font-medium rounded-md transition-all duration-200',
            active === tab.id
              ? 'bg-white/[0.06] text-zinc-100 shadow-sm border border-white/[0.05]'
              : 'text-zinc-500 hover:text-zinc-300 hover:bg-white/[0.02] border border-transparent'
          )}
        >
          {tab.icon}
          {tab.label}
          {tab.count !== undefined && (
            <span className={cn(
              'px-1.5 py-0.5 text-2xs rounded-full font-mono',
              active === tab.id ? 'bg-amber-400/10 text-amber-400' : 'bg-white/[0.04] text-zinc-600'
            )}>
              {tab.count}
            </span>
          )}
        </button>
      ))}
    </div>
  )
}
