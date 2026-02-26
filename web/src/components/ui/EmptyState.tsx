import { cn } from '../../lib/utils'

interface EmptyStateProps {
  icon: React.ReactNode
  title: string
  description: string
  action?: React.ReactNode
  className?: string
}

export function EmptyState({ icon, title, description, action, className }: EmptyStateProps) {
  return (
    <div className={cn('flex flex-col items-center justify-center py-16 px-8 text-center', className)}>
      <div className="w-12 h-12 rounded-xl bg-white/[0.03] border border-white/[0.05] flex items-center justify-center text-zinc-600 mb-4">
        {icon}
      </div>
      <h3 className="text-sm font-display font-semibold text-zinc-300 mb-1">{title}</h3>
      <p className="text-xs text-zinc-600 max-w-sm mb-4">{description}</p>
      {action}
    </div>
  )
}
