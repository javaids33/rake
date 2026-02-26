import { useState, useRef, useEffect, type ReactNode } from 'react'

interface TooltipProps {
  content: ReactNode
  children: ReactNode
  position?: 'top' | 'bottom' | 'left' | 'right'
  delay?: number
  maxWidth?: number
}

export function Tooltip({ content, children, position = 'top', delay = 300, maxWidth = 320 }: TooltipProps) {
  const [visible, setVisible] = useState(false)
  const [coords, setCoords] = useState({ x: 0, y: 0 })
  const ref = useRef<HTMLDivElement>(null)
  const timeoutRef = useRef<ReturnType<typeof setTimeout>>()

  const show = () => {
    timeoutRef.current = setTimeout(() => {
      if (ref.current) {
        const rect = ref.current.getBoundingClientRect()
        setCoords({ x: rect.left + rect.width / 2, y: rect.top })
      }
      setVisible(true)
    }, delay)
  }

  const hide = () => {
    clearTimeout(timeoutRef.current)
    setVisible(false)
  }

  useEffect(() => () => clearTimeout(timeoutRef.current), [])

  const positionStyles: Record<string, React.CSSProperties> = {
    top: { bottom: '100%', left: '50%', transform: 'translateX(-50%)', marginBottom: 8 },
    bottom: { top: '100%', left: '50%', transform: 'translateX(-50%)', marginTop: 8 },
    left: { right: '100%', top: '50%', transform: 'translateY(-50%)', marginRight: 8 },
    right: { left: '100%', top: '50%', transform: 'translateY(-50%)', marginLeft: 8 },
  }

  return (
    <div ref={ref} onMouseEnter={show} onMouseLeave={hide} style={{ position: 'relative', display: 'inline-flex' }}>
      {children}
      {visible && (
        <div
          style={{
            position: 'absolute',
            zIndex: 9999,
            ...positionStyles[position],
            maxWidth,
            pointerEvents: 'none',
          }}
        >
          <div
            style={{
              background: 'rgba(15, 23, 42, 0.95)',
              border: '1px solid rgba(245, 158, 11, 0.2)',
              borderRadius: 8,
              padding: '8px 12px',
              fontSize: 12,
              lineHeight: 1.5,
              color: '#e2e8f0',
              backdropFilter: 'blur(12px)',
              boxShadow: '0 8px 24px rgba(0,0,0,0.4)',
              whiteSpace: 'pre-wrap',
            }}
          >
            {content}
          </div>
        </div>
      )}
    </div>
  )
}

/** Inline tooltip label — a dim label that shows rich content on hover */
export function InfoTip({ label, children }: { label?: string; children: ReactNode }) {
  return (
    <Tooltip content={children} position="top">
      <span style={{ cursor: 'help', borderBottom: '1px dotted rgba(148,163,184,0.4)', color: '#94a3b8', fontSize: 11 }}>
        {label ?? 'ⓘ'}
      </span>
    </Tooltip>
  )
}
