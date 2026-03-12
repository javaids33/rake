import { createContext, useContext, useEffect } from 'react'
import { Outlet } from 'react-router-dom'
import { Sidebar } from './Sidebar'
import { Header } from './Header'
import { useAppStore } from '../../stores/app'
import { cn } from '../../lib/utils'
import { useEventStream } from '../../hooks/useEventStream'
import type { ServerStatus, ConnectionSyncEvent } from '../../hooks/useEventStream'

interface EventStreamContextValue {
  status: ServerStatus | null
  connected: boolean
  onConnectionSync: (listener: (event: ConnectionSyncEvent) => void) => () => void
}

const EventStreamContext = createContext<EventStreamContextValue>({
  status: null,
  connected: false,
  onConnectionSync: () => () => {},
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

  return (
    <EventStreamContext.Provider value={eventStream}>
      <div className={cn(
        'flex h-screen w-screen overflow-hidden transition-colors duration-300',
        darkMode
          ? 'bg-navy-950 atmosphere noise'
          : 'bg-slate-50'
      )}>
        {/* Ambient orbs — dark only */}
        {darkMode && (
          <>
            <div className="fixed inset-0 pointer-events-none z-0" aria-hidden>
              <div className="absolute top-[-20%] left-[-10%] w-[600px] h-[600px] rounded-full bg-amber-400/[0.02] blur-[120px]" />
              <div className="absolute bottom-[-20%] right-[-10%] w-[500px] h-[500px] rounded-full bg-cyan-400/[0.02] blur-[100px]" />
              <div className="absolute top-[40%] right-[20%] w-[300px] h-[300px] rounded-full bg-violet-400/[0.015] blur-[80px]" />
            </div>
            <div className="fixed inset-0 dot-grid pointer-events-none z-0" aria-hidden />
          </>
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
