// DuckDB-WASM integration for browser-side SQL analytics
// Enables offline Parquet/CSV/JSON querying without server round-trips

import * as duckdb from '@duckdb/duckdb-wasm'

let db: duckdb.AsyncDuckDB | null = null
let conn: duckdb.AsyncDuckDBConnection | null = null
let loading = false
let loadPromise: Promise<void> | null = null

export function isDuckDBWasmLoaded(): boolean {
  return db !== null && conn !== null
}

export function isDuckDBWasmLoading(): boolean {
  return loading
}

export async function ensureDuckDBWasm(): Promise<void> {
  if (db && conn) return
  if (loadPromise) return loadPromise

  loading = true
  loadPromise = (async () => {
    try {
      // Use jsdelivr CDN bundles
      const CDN = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/dist'
      const DUCKDB_BUNDLES = await duckdb.selectBundle({
        mvp: {
          mainModule: `${CDN}/duckdb-mvp.wasm`,
          mainWorker: `${CDN}/duckdb-browser-mvp.worker.js`,
        },
        eh: {
          mainModule: `${CDN}/duckdb-eh.wasm`,
          mainWorker: `${CDN}/duckdb-browser-eh.worker.js`,
        },
      })

      const logger = new duckdb.ConsoleLogger()
      const worker = new Worker(DUCKDB_BUNDLES.mainWorker!)
      db = new duckdb.AsyncDuckDB(logger, worker)
      await db.instantiate(DUCKDB_BUNDLES.mainModule)

      conn = await db.connect()

      // Enable httpfs for remote file access
      await conn.query("INSTALL httpfs; LOAD httpfs;").catch(() => {
        // httpfs may not be available in all WASM builds — that's OK
        console.log('[DuckDB-WASM] httpfs not available in this build')
      })

      loading = false
      console.log('[DuckDB-WASM] Initialized successfully')
    } catch (err) {
      loading = false
      loadPromise = null
      console.warn('[DuckDB-WASM] Failed to initialize:', err)
      throw err
    }
  })()

  return loadPromise
}

export interface DuckDBWasmResult {
  columns: string[]
  rows: Record<string, unknown>[]
  rowCount: number
  durationMs: number
  error: string | null
}

// Execute SQL against DuckDB-WASM (local browser engine)
export async function executeDuckDBWasm(sql: string): Promise<DuckDBWasmResult> {
  const start = performance.now()

  try {
    await ensureDuckDBWasm()
    if (!conn) throw new Error('DuckDB-WASM connection not available')

    const result = await conn.query(sql)
    const columns = result.schema.fields.map(f => f.name)
    const rows: Record<string, unknown>[] = []

    for (let i = 0; i < result.numRows; i++) {
      const row: Record<string, unknown> = {}
      for (const col of columns) {
        const vec = result.getChild(col)
        row[col] = vec ? vec.get(i) : null
      }
      rows.push(row)
    }

    return {
      columns,
      rows,
      rowCount: result.numRows,
      durationMs: Math.round(performance.now() - start),
      error: null,
    }
  } catch (err) {
    return {
      columns: [],
      rows: [],
      rowCount: 0,
      durationMs: Math.round(performance.now() - start),
      error: String(err),
    }
  }
}

// Register a local file (drag & drop or file picker) and query it
export async function queryLocalFile(file: File, sql?: string): Promise<DuckDBWasmResult> {
  const start = performance.now()

  const fileName = file.name.toLowerCase()
  const ext = fileName.split('.').pop()

  if (!['csv', 'parquet', 'json'].includes(ext || '')) {
    return {
      columns: [],
      rows: [],
      rowCount: 0,
      durationMs: 0,
      error: `Unsupported file type: .${ext}. Supported: .csv, .parquet, .json`,
    }
  }

  try {
    await ensureDuckDBWasm()
    if (!db || !conn) throw new Error('DuckDB-WASM not available')

    // Register the file in DuckDB's virtual filesystem
    const buffer = await file.arrayBuffer()
    const uint8 = new Uint8Array(buffer)
    await db.registerFileBuffer(file.name, uint8)

    // Build query based on file type
    const tableName = file.name.replace(/[^a-zA-Z0-9_]/g, '_')
    let query: string

    if (ext === 'csv') {
      query = sql || `SELECT * FROM read_csv_auto('${file.name}') LIMIT 1000`
    } else if (ext === 'parquet') {
      query = sql || `SELECT * FROM read_parquet('${file.name}') LIMIT 1000`
    } else if (ext === 'json') {
      query = sql || `SELECT * FROM read_json_auto('${file.name}') LIMIT 1000`
    } else {
      query = sql || `SELECT * FROM '${file.name}' LIMIT 1000`
    }

    const result = await conn.query(query)
    const columns = result.schema.fields.map(f => f.name)
    const rows: Record<string, unknown>[] = []

    for (let i = 0; i < result.numRows; i++) {
      const row: Record<string, unknown> = {}
      for (const col of columns) {
        const vec = result.getChild(col)
        row[col] = vec ? vec.get(i) : null
      }
      rows.push(row)
    }

    return {
      columns,
      rows,
      rowCount: result.numRows,
      durationMs: Math.round(performance.now() - start),
      error: null,
    }
  } catch (err) {
    return {
      columns: [],
      rows: [],
      rowCount: 0,
      durationMs: Math.round(performance.now() - start),
      error: String(err),
    }
  }
}

// Get DuckDB-WASM version info
export function getDuckDBWasmVersion(): string {
  return '1.29.0'
}

// Drop all registered tables/files (cleanup)
export async function resetDuckDBWasm(): Promise<void> {
  if (conn) {
    await conn.close()
    conn = null
  }
  if (db) {
    await db.terminate()
    db = null
  }
  loadPromise = null
  loading = false
}
