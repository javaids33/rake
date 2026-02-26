import { cn } from '../../lib/utils'

interface StatusDotProps {
  status: 'healthy' | 'warning' | 'error' | 'idle'
  label?: string
  pulse?: boolean
}

const colors = {
  healthy: 'bg-emerald-400',
  warning: 'bg-amber-400',
  error: 'bg-rose-400',
  idle: 'bg-zinc-500',
}

export function StatusDot({ status, label, pulse }: StatusDotProps) {
  return (
    <span className="inline-flex items-center gap-1.5">
      <span className="relative flex h-2 w-2">
        {(pulse || status === 'healthy') && (
          <span className={cn('absolute inset-0 rounded-full animate-ping opacity-30', colors[status])} />
        )}
        <span className={cn('relative inline-flex rounded-full h-2 w-2', colors[status])} />
        <span className={cn('absolute -inset-1 rounded-full blur-sm opacity-30', colors[status])} />
      </span>
      {label && <span className="text-2xs text-zinc-400 tracking-wide">{label}</span>}
    </span>
  )
}
