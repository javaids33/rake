import { cn } from '../../lib/utils'

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string
  hint?: string
}

export function Input({ label, hint, className, ...props }: InputProps) {
  return (
    <label className="flex flex-col gap-1.5">
      {label && <span className="text-xs font-medium text-zinc-400">{label}</span>}
      <input
        className={cn(
          'px-3 py-2 text-sm rounded-lg bg-navy-900/60 border border-white/[0.06]',
          'text-zinc-100 placeholder-zinc-600 backdrop-blur-sm',
          'focus:outline-none focus:ring-1 focus:ring-amber-400/25 focus:border-amber-400/25',
          'transition-all duration-200',
          className
        )}
        {...props}
      />
      {hint && <span className="text-2xs text-zinc-600">{hint}</span>}
    </label>
  )
}

interface TextareaProps extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  label?: string
}

export function Textarea({ label, className, ...props }: TextareaProps) {
  return (
    <label className="flex flex-col gap-1.5">
      {label && <span className="text-xs font-medium text-zinc-400">{label}</span>}
      <textarea
        className={cn(
          'px-3 py-2 text-sm rounded-lg bg-navy-900/60 border border-white/[0.06]',
          'text-zinc-100 placeholder-zinc-600 font-mono backdrop-blur-sm',
          'focus:outline-none focus:ring-1 focus:ring-amber-400/25 focus:border-amber-400/25',
          'transition-all duration-200 resize-y min-h-[80px]',
          className
        )}
        {...props}
      />
    </label>
  )
}

interface SelectProps extends React.SelectHTMLAttributes<HTMLSelectElement> {
  label?: string
  options: Array<{ value: string; label: string }>
}

export function Select({ label, options, className, ...props }: SelectProps) {
  return (
    <label className="flex flex-col gap-1.5">
      {label && <span className="text-xs font-medium text-zinc-400">{label}</span>}
      <select
        className={cn(
          'px-3 py-2 text-sm rounded-lg bg-navy-900/60 border border-white/[0.06]',
          'text-zinc-100 focus:outline-none focus:ring-1 focus:ring-amber-400/25',
          'transition-all duration-200 backdrop-blur-sm',
          className
        )}
        {...props}
      >
        {options.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
      </select>
    </label>
  )
}
