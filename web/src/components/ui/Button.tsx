import { cn } from '../../lib/utils'
import { Loader2 } from 'lucide-react'

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'secondary' | 'ghost' | 'danger'
  size?: 'sm' | 'md' | 'lg'
  loading?: boolean
  icon?: React.ReactNode
}

export function Button({ variant = 'secondary', size = 'md', loading, icon, children, className, disabled, ...props }: ButtonProps) {
  return (
    <button
      className={cn(
        'inline-flex items-center justify-center gap-2 font-medium rounded-lg transition-all duration-200 whitespace-nowrap',
        'focus:outline-none focus:ring-2 focus:ring-amber-400/20 focus:ring-offset-1 focus:ring-offset-navy-950',
        'disabled:opacity-40 disabled:pointer-events-none',
        {
          'bg-gradient-to-r from-amber-500 to-amber-600 text-navy-950 font-semibold hover:from-amber-400 hover:to-amber-500 active:from-amber-600 active:to-amber-700 shadow-lg shadow-amber-500/20': variant === 'primary',
          'bg-white/[0.04] text-zinc-300 border border-white/[0.06] hover:bg-white/[0.07] hover:border-white/[0.1] hover:text-zinc-100': variant === 'secondary',
          'text-zinc-400 hover:text-zinc-200 hover:bg-white/[0.03]': variant === 'ghost',
          'bg-rose-500/10 text-rose-400 border border-rose-500/15 hover:bg-rose-500/20': variant === 'danger',
        },
        {
          'text-xs px-2.5 py-1.5': size === 'sm',
          'text-sm px-3.5 py-2': size === 'md',
          'text-sm px-5 py-2.5': size === 'lg',
        },
        className
      )}
      disabled={disabled || loading}
      {...props}
    >
      {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : icon}
      {children}
    </button>
  )
}
