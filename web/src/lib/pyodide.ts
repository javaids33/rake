// Pyodide WASM integration for browser-side Python execution
// Loads ~10MB WASM bundle from CDN, cached by browser after first load

declare global {
  interface Window {
    loadPyodide: (config: { indexURL: string }) => Promise<PyodideInterface>
  }
}

interface PyodideInterface {
  runPythonAsync: (code: string) => Promise<unknown>
  loadPackage: (packages: string[]) => Promise<void>
  globals: Map<string, unknown>
  setStdin: (opts: { stdin: () => string }) => void
  setStdout: (opts: { batched: (text: string) => void }) => void
  setStderr: (opts: { batched: (text: string) => void }) => void
  FS: { readFile: (path: string, opts?: { encoding?: string }) => Uint8Array | string }
  isPyProxy: (obj: unknown) => boolean
}

let pyodide: PyodideInterface | null = null
let loading = false
let loadPromise: Promise<PyodideInterface> | null = null

export async function ensurePyodide(): Promise<PyodideInterface> {
  if (pyodide) return pyodide
  if (loadPromise) return loadPromise

  loading = true
  loadPromise = (async () => {
    // Load Pyodide script from CDN
    if (!window.loadPyodide) {
      await new Promise<void>((resolve, reject) => {
        const script = document.createElement('script')
        script.src = 'https://cdn.jsdelivr.net/pyodide/v0.26.4/full/pyodide.js'
        script.onload = () => resolve()
        script.onerror = () => reject(new Error('Failed to load Pyodide from CDN'))
        document.head.appendChild(script)
      })
    }

    pyodide = await window.loadPyodide({
      indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.26.4/full/'
    })

    // Pre-load common data science packages
    await pyodide.loadPackage(['micropip'])
    // pandas/numpy/matplotlib load on-demand via micropip

    loading = false
    return pyodide
  })()

  return loadPromise
}

export function isPyodideLoaded(): boolean {
  return pyodide !== null
}

export function isPyodideLoading(): boolean {
  return loading
}

export interface PythonResult {
  stdout: string
  stderr: string
  result: unknown
  hasPlot: boolean
  plotDataUrl: string | null
  error: string | null
}

// Execute Python code with stdout/stderr capture and matplotlib plot detection
export async function executePython(
  code: string,
  variables?: Record<string, unknown>
): Promise<PythonResult> {
  const py = await ensurePyodide()

  let stdout = ''
  let stderr = ''

  py.setStdout({ batched: (text: string) => { stdout += text + '\n' } })
  py.setStderr({ batched: (text: string) => { stderr += text + '\n' } })

  // Inject variables into Python namespace
  if (variables) {
    for (const [key, value] of Object.entries(variables)) {
      if (typeof value === 'object' && value !== null && 'columns' in value && 'rows' in value) {
        // Convert SQL result to pandas DataFrame
        const tableData = value as { columns: string[]; rows: Record<string, unknown>[] }
        const jsonStr = JSON.stringify(tableData.rows)
        await py.runPythonAsync(`
import json
import pandas as pd
${key} = pd.DataFrame(json.loads('${jsonStr.replace(/'/g, "\\'")}'))
`)
      } else {
        py.globals.set(key, value)
      }
    }
  }

  // Add matplotlib save hook
  const wrappedCode = `
import sys
_has_plot = False
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    _original_show = plt.show
    def _rustlake_show(*args, **kwargs):
        global _has_plot
        _has_plot = True
        plt.savefig('/tmp/rustlake_plot.png', dpi=100, bbox_inches='tight', facecolor='#18181b', edgecolor='none')
    plt.show = _rustlake_show
except ImportError:
    pass

${code}

# Check if any figures were created
try:
    import matplotlib.pyplot as plt
    if plt.get_fignums() and not _has_plot:
        _has_plot = True
        plt.savefig('/tmp/rustlake_plot.png', dpi=100, bbox_inches='tight', facecolor='#18181b', edgecolor='none')
        plt.close('all')
except:
    pass
_has_plot
`

  try {
    const result = await py.runPythonAsync(wrappedCode)
    const hasPlot = result === true

    let plotDataUrl: string | null = null
    if (hasPlot) {
      try {
        const plotBytes = py.FS.readFile('/tmp/rustlake_plot.png') as Uint8Array
        const blob = new Blob([plotBytes], { type: 'image/png' })
        plotDataUrl = URL.createObjectURL(blob)
      } catch {
        /* no plot file */
      }
    }

    return {
      stdout: stdout.trim(),
      stderr: stderr.trim(),
      result: py.isPyProxy(result) ? String(result) : result,
      hasPlot,
      plotDataUrl,
      error: null,
    }
  } catch (err) {
    return {
      stdout: stdout.trim(),
      stderr: stderr.trim(),
      result: null,
      hasPlot: false,
      plotDataUrl: null,
      error: String(err),
    }
  }
}
