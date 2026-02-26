import { useEffect } from 'react'
import { X } from 'lucide-react'
import { cn } from '../../lib/utils'

interface ModalProps {
  open: boolean
  onClose: () => void
  title: string
  children: React.ReactNode
  width?: string
}

export function Modal({ open, onClose, title, children, width = 'max-w-lg' }: ModalProps) {
  useEffect(() => {
    if (open) {
      document.body.style.overflow = 'hidden'
      return () => { document.body.style.overflow = '' }
    }
  }, [open])

  if (!open) return null

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="absolute inset-0 bg-navy-950/80 backdrop-blur-md" onClick={onClose} />
      <div className={cn(
        'relative w-full mx-4 glass rounded-2xl shadow-2xl animate-fade-in border-amber-400/[0.06]',
        width
      )}>
        <div className="flex items-center justify-between px-5 py-4 border-b border-white/[0.04]">
          <h3 className="text-sm font-display font-semibold text-zinc-100">{title}</h3>
          <button onClick={onClose} className="p-1.5 rounded-lg text-zinc-500 hover:text-zinc-300 hover:bg-white/[0.04] transition-all duration-200">
            <X className="w-4 h-4" />
          </button>
        </div>
        <div className="p-5">{children}</div>
      </div>
    </div>
  )
}
