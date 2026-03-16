import { useEffect, useRef, useCallback } from 'react'
import { cn } from '../../lib/utils'
import { X } from 'lucide-react'

interface DrawerProps {
  open: boolean
  onClose: () => void
  title: string
  subtitle?: string
  width?: string
  children: React.ReactNode
  /** Show a "Draft saved" indicator */
  draftSaved?: boolean
}

/**
 * Slide-out drawer panel that replaces modals for complex creation workflows.
 *
 * Unlike modals:
 * - Doesn't block the main content (you can still see the page behind)
 * - Slides in from the right edge
 * - Can be closed via Escape or clicking outside
 * - Parent is responsible for persisting form state to localStorage
 */
export function Drawer({ open, onClose, title, subtitle, width = 'max-w-xl', children, draftSaved }: DrawerProps) {
  const drawerRef = useRef<HTMLDivElement>(null)

  // Close on Escape
  useEffect(() => {
    if (!open) return
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    document.addEventListener('keydown', handleKey)
    return () => document.removeEventListener('keydown', handleKey)
  }, [open, onClose])

  // Close on click outside drawer
  const handleBackdropClick = useCallback((e: React.MouseEvent) => {
    if (drawerRef.current && !drawerRef.current.contains(e.target as Node)) {
      onClose()
    }
  }, [onClose])

  if (!open) return null

  return (
    <div
      className="fixed inset-0 z-50 flex justify-end"
      onClick={handleBackdropClick}
    >
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/30 transition-opacity" />

      {/* Drawer panel */}
      <div
        ref={drawerRef}
        className={cn(
          'relative w-full h-full flex flex-col bg-navy-950 border-l border-white/[0.06] shadow-2xl',
          'animate-slide-in-right',
          width
        )}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-white/[0.06] flex-shrink-0">
          <div>
            <h2 className="text-base font-display font-bold text-zinc-100">{title}</h2>
            {subtitle && <p className="text-2xs text-zinc-500 mt-0.5">{subtitle}</p>}
          </div>
          <div className="flex items-center gap-3">
            {draftSaved && (
              <span className="text-2xs text-zinc-600 font-mono">Draft saved</span>
            )}
            <button
              onClick={onClose}
              className="p-1.5 rounded-lg hover:bg-white/[0.06] transition-colors text-zinc-500 hover:text-zinc-300"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* Content — scrollable */}
        <div className="flex-1 overflow-y-auto px-6 py-5">
          {children}
        </div>
      </div>
    </div>
  )
}
