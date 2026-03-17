import { createContext, useContext, useEffect, useMemo } from 'react'
import { Outlet } from 'react-router-dom'
import { Sidebar } from './Sidebar'
import { Header } from './Header'
import { useAppStore } from '../../stores/app'
import { cn } from '../../lib/utils'
import { useEventStream } from '../../hooks/useEventStream'
import type { ServerStatus, ConnectionSyncEvent, TrinoScanEvent, S3ScanEvent, PipelineEventData } from '../../hooks/useEventStream'

interface EventStreamContextValue {
  status: ServerStatus | null
  connected: boolean
  onConnectionSync: (listener: (event: ConnectionSyncEvent) => void) => () => void
  onTrinoScan: (listener: (event: TrinoScanEvent) => void) => () => void
  onS3Scan: (listener: (event: S3ScanEvent) => void) => () => void
  onPipelineEvent: (listener: (event: PipelineEventData) => void) => () => void
}

const EventStreamContext = createContext<EventStreamContextValue>({
  status: null,
  connected: false,
  onConnectionSync: () => () => {},
  onTrinoScan: () => () => {},
  onS3Scan: () => () => {},
  onPipelineEvent: () => () => {},
})

/** Use the shared SSE event stream from any component. */
export function useServerEvents() {
  return useContext(EventStreamContext)
}

export function Shell() {
  const { darkMode } = useAppStore()
  const eventStream = useEventStream()

  // Sync body class for CSS variable overrides
  useEffect(() => {
    document.body.classList.toggle('light-mode', !darkMode)
  }, [darkMode])

  // Memoize context value so child pages don't re-render when status ticks
  // The callbacks (onConnectionSync, etc.) are stable refs from useEventStream
  const contextValue = useMemo(() => eventStream, [eventStream])

  return (
    <EventStreamContext.Provider value={contextValue}>
      <div className={cn(
        'flex h-screen w-screen overflow-hidden transition-colors duration-300',
        darkMode
          ? 'bg-navy-950 atmosphere noise'
          : 'bg-slate-50'
      )}>
        {/* Ambient orbs — dark only */}
        {darkMode && (
          <div className="fixed inset-0 dot-grid pointer-events-none z-0" aria-hidden />
        )}

        <Sidebar />
        <div className="flex flex-col flex-1 min-w-0 relative z-10">
          <Header serverStatus={eventStream.status} sseConnected={eventStream.connected} />
          <main className="flex-1 overflow-auto">
            <Outlet />
          </main>
        </div>
      </div>
    </EventStreamContext.Provider>
  )
}
