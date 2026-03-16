import { cn } from '../../lib/utils'

interface BadgeProps {
  children: React.ReactNode
  className?: string
  dot?: boolean
  dotColor?: string
}

export function Badge({ children, className, dot, dotColor = 'bg-emerald-400' }: BadgeProps) {
  return (
    <span className={cn(
      'inline-flex items-center gap-1.5 px-2 py-0.5 text-2xs font-medium rounded-md',
      'bg-white/[0.04] text-zinc-400 border border-white/[0.06]',
      className
    )}>
      {dot && (
        <span className={cn('w-1.5 h-1.5 rounded-full inline-block', dotColor)} />
      )}
      {children}
    </span>
  )
}
