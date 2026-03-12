import { useEffect, useRef, useState, useCallback } from 'react'

/** Combined status payload pushed by the SSE `/api/v1/events` stream. */
export interface ServerStatus {
  health: string
  cpu: number
  mem_used: number
  mem_total: number
  mem_pct: number
  load_1m: number
  total_queries: number
  uptime: number
  tables: number
  engines: Array<{ name: string; version: string; status: string }>
}

/** Connection sync update pushed via SSE. */
export interface ConnectionSyncEvent {
  id: string
  sync_status: 'syncing' | 'ready' | 'error'
  sync_error: string | null
  tables: string[]
  table_count: number
}

type SyncListener = (event: ConnectionSyncEvent) => void

/**
 * Hook that maintains a single SSE connection to `/api/v1/events`.
 *
 * Returns the latest server status (health, metrics, tables, engines)
 * and a way to subscribe to connection sync events.
 *
 * Replaces 3 polling intervals (health 15s, metrics+tables 10s, engine 5s)
 * with a single persistent connection.
 */
export function useEventStream() {
  const [status, setStatus] = useState<ServerStatus | null>(null)
  const [connected, setConnected] = useState(false)
  const syncListenersRef = useRef<Set<SyncListener>>(new Set())
  const sourceRef = useRef<EventSource | null>(null)

  // Allow components to subscribe to connection sync events
  const onConnectionSync = useCallback((listener: SyncListener) => {
    syncListenersRef.current.add(listener)
    return () => { syncListenersRef.current.delete(listener) }
  }, [])

  useEffect(() => {
    let retryTimeout: ReturnType<typeof setTimeout>
    let es: EventSource

    function connect() {
      es = new EventSource('/api/v1/events')
      sourceRef.current = es

      es.addEventListener('status', (e) => {
        try {
          const data: ServerStatus = JSON.parse(e.data)
          setStatus(data)
          setConnected(true)
        } catch { /* ignore parse errors */ }
      })

      es.addEventListener('connection_sync', (e) => {
        try {
          const data: ConnectionSyncEvent = JSON.parse(e.data)
          syncListenersRef.current.forEach(fn => fn(data))
        } catch { /* ignore parse errors */ }
      })

      es.onerror = () => {
        setConnected(false)
        es.close()
        // Reconnect after 3 seconds
        retryTimeout = setTimeout(connect, 3000)
      }
    }

    connect()

    return () => {
      clearTimeout(retryTimeout)
      es?.close()
      sourceRef.current = null
    }
  }, [])

  return { status, connected, onConnectionSync }
}
