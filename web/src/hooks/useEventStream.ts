import { useEffect, useRef, useState, useCallback, useMemo } from 'react'

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

/** Trino catalog scan progress pushed via SSE. */
export interface TrinoScanEvent {
  id: string
  phase: string | null
  sync_status: string
}

/** S3 bucket scan progress pushed via SSE. */
export interface S3ScanEvent {
  name: string
  phase: string | null
  detail: string | null
  scanned: number
  total: number
  found: number
  elapsed_ms: number
  formats: Record<string, number>
  sync_status: string
}

type SyncListener = (event: ConnectionSyncEvent) => void
type TrinoScanListener = (event: TrinoScanEvent) => void
type S3ScanListener = (event: S3ScanEvent) => void

/**
 * Hook that maintains a single SSE connection to `/api/v1/events`.
 *
 * Returns the latest server status (health, metrics, tables, engines)
 * and a way to subscribe to connection sync and trino scan events.
 *
 * Uses ref-based status to avoid re-rendering the entire component tree
 * on every SSE tick. Only updates state when values actually change.
 */
export function useEventStream() {
  const [status, setStatus] = useState<ServerStatus | null>(null)
  const [connected, setConnected] = useState(false)
  const lastStatusRef = useRef<string>('')
  const syncListenersRef = useRef<Set<SyncListener>>(new Set())
  const trinoScanListenersRef = useRef<Set<TrinoScanListener>>(new Set())
  const s3ScanListenersRef = useRef<Set<S3ScanListener>>(new Set())
  const sourceRef = useRef<EventSource | null>(null)

  // Allow components to subscribe to connection sync events
  const onConnectionSync = useCallback((listener: SyncListener) => {
    syncListenersRef.current.add(listener)
    return () => { syncListenersRef.current.delete(listener) }
  }, [])

  // Allow components to subscribe to trino scan progress events
  const onTrinoScan = useCallback((listener: TrinoScanListener) => {
    trinoScanListenersRef.current.add(listener)
    return () => { trinoScanListenersRef.current.delete(listener) }
  }, [])

  // Allow components to subscribe to S3 scan progress events
  const onS3Scan = useCallback((listener: S3ScanListener) => {
    s3ScanListenersRef.current.add(listener)
    return () => { s3ScanListenersRef.current.delete(listener) }
  }, [])

  useEffect(() => {
    let retryTimeout: ReturnType<typeof setTimeout>
    let es: EventSource

    function connect() {
      es = new EventSource('/api/v1/events')
      sourceRef.current = es

      es.addEventListener('status', (e) => {
        try {
          // Only trigger a re-render if the values actually changed
          // Compare a lightweight fingerprint instead of deep comparison
          const data: ServerStatus = JSON.parse(e.data)
          const fingerprint = `${data.health}|${data.cpu.toFixed(0)}|${Math.round(data.mem_pct)}|${data.total_queries}|${data.uptime}|${data.tables}|${data.engines.length}`
          if (fingerprint !== lastStatusRef.current) {
            lastStatusRef.current = fingerprint
            setStatus(data)
          }
          setConnected(true)
        } catch { /* ignore parse errors */ }
      })

      es.addEventListener('connection_sync', (e) => {
        try {
          const data: ConnectionSyncEvent = JSON.parse(e.data)
          syncListenersRef.current.forEach(fn => fn(data))
        } catch { /* ignore parse errors */ }
      })

      es.addEventListener('trino_scan', (e) => {
        try {
          const data: TrinoScanEvent = JSON.parse(e.data)
          trinoScanListenersRef.current.forEach(fn => fn(data))
        } catch { /* ignore parse errors */ }
      })

      es.addEventListener('s3_scan', (e) => {
        try {
          const data: S3ScanEvent = JSON.parse(e.data)
          s3ScanListenersRef.current.forEach(fn => fn(data))
        } catch { /* ignore parse errors */ }
      })

      es.onerror = () => {
        setConnected(false)
        es.close()
        // Reconnect after 5 seconds
        retryTimeout = setTimeout(connect, 5000)
      }
    }

    connect()

    return () => {
      clearTimeout(retryTimeout)
      es?.close()
      sourceRef.current = null
    }
  }, [])

  // Memoize the return value so context consumers don't re-render
  // unless the actual status/connected values change
  return useMemo(() => ({
    status, connected, onConnectionSync, onTrinoScan, onS3Scan
  }), [status, connected, onConnectionSync, onTrinoScan, onS3Scan])
}
