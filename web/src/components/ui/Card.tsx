import { cn } from '../../lib/utils'

interface CardProps {
  children: React.ReactNode
  className?: string
  hover?: boolean
  glow?: 'amber' | 'cyan' | 'rose' | 'emerald' | 'violet'
  padding?: 'none' | 'sm' | 'md' | 'lg'
  onClick?: () => void
}

const glowMap = {
  amber: 'shadow-glow-amber border-amber-400/10',
  cyan: 'shadow-glow-cyan border-cyan-400/10',
  rose: 'shadow-glow-rose border-rose-400/10',
  emerald: 'shadow-glow-emerald border-emerald-400/10',
  violet: 'border-violet-400/10',
}

export function Card({ children, className, hover, glow, padding = 'md', onClick }: CardProps) {
  return (
    <div onClick={onClick} className={cn(
      'rounded-xl glass',
      hover && 'glass-hover cursor-pointer',
      glow && glowMap[glow],
      {
        'p-0': padding === 'none',
        'p-3': padding === 'sm',
        'p-5': padding === 'md',
        'p-6': padding === 'lg',
      },
      className
    )}>
      {children}
    </div>
  )
}
