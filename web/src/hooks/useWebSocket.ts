import { useEffect, useRef, useState, useCallback } from 'react'

// ── Server → Client message types ──────────────────────────────────

export interface QueryStartMsg {
  type: 'query_start'
  query_id: string
  engine: string
  query_type: string
}

export interface QueryRowsMsg {
  type: 'query_rows'
  query_id: string
  columns: string[]
  rows: Record<string, unknown>[]
  chunk_index: number
}

export interface QueryCompleteMsg {
  type: 'query_complete'
  query_id: string
  row_count: number
  duration_ms: number
  parse_ms: number
  exec_ms: number
  engine: string
  query_type: string
}

export interface QueryErrorMsg {
  type: 'query_error'
  query_id: string
  error: string
  engine: string
}

export interface QueryCancelledMsg {
  type: 'query_cancelled'
  query_id: string
}

type ServerMsg =
  | QueryStartMsg
  | QueryRowsMsg
  | QueryCompleteMsg
  | QueryErrorMsg
  | QueryCancelledMsg
  | { type: 'pong' }
  | { type: 'error'; message: string }

export interface QueryCallbacks {
  onStart?: (msg: QueryStartMsg) => void
  onRows?: (msg: QueryRowsMsg) => void
  onComplete?: (msg: QueryCompleteMsg) => void
  onError?: (msg: QueryErrorMsg) => void
  onCancelled?: () => void
}

/**
 * Persistent WebSocket connection to the backend.
 *
 * Provides streaming query execution with chunked results
 * and mid-flight cancellation support.
 */
export function useWebSocket() {
  const [connected, setConnected] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)
  const callbacksRef = useRef<Map<string, QueryCallbacks>>(new Map())
  const reconnectRef = useRef<ReturnType<typeof setTimeout>>()
  const attemptRef = useRef(0)
  const mountedRef = useRef(true)

  const connect = useCallback(() => {
    if (!mountedRef.current) return

    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const url = `${proto}//${window.location.host}/api/v1/ws`

    const ws = new WebSocket(url)
    wsRef.current = ws

    ws.onopen = () => {
      setConnected(true)
      attemptRef.current = 0
    }

    ws.onmessage = (event) => {
      try {
        const msg: ServerMsg = JSON.parse(event.data)

        if (msg.type === 'pong' || msg.type === 'error') return

        // Route to query-specific callbacks
        const queryId = 'query_id' in msg ? msg.query_id : null
        if (!queryId) return

        const cbs = callbacksRef.current.get(queryId)
        if (!cbs) return

        switch (msg.type) {
          case 'query_start':
            cbs.onStart?.(msg)
            break
          case 'query_rows':
            cbs.onRows?.(msg)
            break
          case 'query_complete':
            cbs.onComplete?.(msg)
            callbacksRef.current.delete(queryId)
            break
          case 'query_error':
            cbs.onError?.(msg)
            callbacksRef.current.delete(queryId)
            break
          case 'query_cancelled':
            cbs.onCancelled?.()
            callbacksRef.current.delete(queryId)
            break
        }
      } catch {
        /* ignore parse errors */
      }
    }

    ws.onerror = () => {
      // onclose will handle reconnection
    }

    ws.onclose = () => {
      setConnected(false)
      wsRef.current = null

      if (!mountedRef.current) return

      // Exponential backoff: 1s, 2s, 4s, 8s, max 30s
      const delay = Math.min(1000 * Math.pow(2, attemptRef.current), 30000)
      attemptRef.current++
      reconnectRef.current = setTimeout(connect, delay)
    }
  }, [])

  useEffect(() => {
    mountedRef.current = true
    connect()

    // Ping every 30s to keep alive
    const pingInterval = setInterval(() => {
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        wsRef.current.send(JSON.stringify({ type: 'ping' }))
      }
    }, 30000)

    return () => {
      mountedRef.current = false
      clearInterval(pingInterval)
      clearTimeout(reconnectRef.current)
      wsRef.current?.close()
    }
  }, [connect])

  const sendQuery = useCallback(
    (
      queryId: string,
      sql: string,
      engine?: string,
      callbacks?: QueryCallbacks,
    ) => {
      if (callbacks) {
        callbacksRef.current.set(queryId, callbacks)
      }
      if (wsRef.current?.readyState === WebSocket.OPEN) {
        wsRef.current.send(
          JSON.stringify({
            type: 'query',
            query_id: queryId,
            sql,
            engine: engine || 'auto',
          }),
        )
      }
    },
    [],
  )

  const cancelQuery = useCallback((queryId: string) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(
        JSON.stringify({
          type: 'cancel',
          query_id: queryId,
        }),
      )
    }
  }, [])

  return { connected, sendQuery, cancelQuery }
}
