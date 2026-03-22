import { useRef, useCallback } from 'react'
import { Play, Trash2, ChevronUp, ChevronDown, Plus, Code2, FileText, Hash } from 'lucide-react'
import Editor from '@monaco-editor/react'
import { useNotebookStore } from '../../stores/notebook'
import { executeSql } from '../../api/client'
import { executePython } from '../../lib/pyodide'
import { CellOutput } from './CellOutput'
import type { NotebookCell as CellType } from '../../types'

interface Props {
  notebookId: string
  cell: CellType
  index: number
  totalCells: number
}

const cellTypeConfig: Record<CellType['type'], { label: string; lang: string; icon: typeof Code2; color: string }> = {
  sql: { label: 'SQL', lang: 'sql', icon: Code2, color: 'text-amber-400' },
  python: { label: 'Python', lang: 'python', icon: Hash, color: 'text-cyan-400' },
  markdown: { label: 'Markdown', lang: 'markdown', icon: FileText, color: 'text-violet-400' },
  rust: { label: 'Rust', lang: 'rust', icon: Code2, color: 'text-rose-400' },
}

export function NotebookCell({ notebookId, cell, index, totalCells }: Props) {
  const {
    updateCellSource, setCellOutput, setCellStatus, setCellExecutionOrder,
    removeCell, moveCell, addCell,
  } = useNotebookStore()
  const editorRef = useRef<unknown>(null)

  const config = cellTypeConfig[cell.type]
  const Icon = config.icon

  const runCell = useCallback(async () => {
    if (!cell.source.trim()) return

    setCellStatus(notebookId, cell.id, 'running')
    const order = setCellExecutionOrder(notebookId, cell.id)

    if (cell.type === 'sql') {
      try {
        const result = await executeSql(cell.source)
        setCellOutput(notebookId, cell.id, {
          type: 'table',
          data: { columns: result.columns, rows: result.rows, row_count: result.row_count, duration_ms: result.duration_ms },
        })
      } catch (err) {
        setCellOutput(notebookId, cell.id, { type: 'error', data: String(err) })
      }
    } else if (cell.type === 'markdown') {
      setCellOutput(notebookId, cell.id, { type: 'text', data: cell.source })
      setCellStatus(notebookId, cell.id, 'success')
    } else if (cell.type === 'python') {
      try {
        // Collect previous SQL cell results as variables
        const notebook = useNotebookStore.getState().notebooks[notebookId]
        const variables: Record<string, unknown> = {}
        if (notebook) {
          for (const c of notebook.cells) {
            if (c.executionOrder && c.output?.type === 'table' && c.id !== cell.id) {
              variables[`_result_${c.executionOrder}`] = c.output.data
            }
          }
        }

        const pyResult = await executePython(cell.source, variables)

        if (pyResult.error) {
          setCellOutput(notebookId, cell.id, { type: 'error', data: pyResult.error })
        } else if (pyResult.hasPlot && pyResult.plotDataUrl) {
          const outputParts = pyResult.stdout ? pyResult.stdout + '\n' : ''
          setCellOutput(notebookId, cell.id, { type: 'image', data: pyResult.plotDataUrl })
          if (outputParts) {
            // If there's both stdout and a plot, show stdout as text
            setCellOutput(notebookId, cell.id, { type: 'image', data: pyResult.plotDataUrl })
          }
        } else {
          const text = [pyResult.stdout, pyResult.stderr].filter(Boolean).join('\n') || String(pyResult.result ?? '(no output)')
          setCellOutput(notebookId, cell.id, { type: 'text', data: text })
        }
      } catch (err) {
        setCellOutput(notebookId, cell.id, { type: 'error', data: `Pyodide error: ${err}` })
      }
    } else if (cell.type === 'rust') {
      try {
        const resp = await fetch('/api/v1/notebook/execute-rust', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ code: cell.source }),
        })
        const result = await resp.json()

        if (!result.compiled) {
          setCellOutput(notebookId, cell.id, { type: 'error', data: result.error || result.compile_output || 'Compilation failed' })
        } else if (!result.success) {
          setCellOutput(notebookId, cell.id, { type: 'error', data: result.error || result.stderr || 'Execution failed' })
        } else {
          const output = [
            result.stdout,
            result.stderr ? `stderr: ${result.stderr}` : '',
            `(compiled in ${result.compile_ms}ms, ran in ${result.run_ms}ms)`,
          ].filter(Boolean).join('\n')
          setCellOutput(notebookId, cell.id, { type: 'text', data: output })
        }
      } catch (err) {
        setCellOutput(notebookId, cell.id, { type: 'error', data: `Rust execution error: ${err}` })
      }
    } else {
      setCellOutput(notebookId, cell.id, { type: 'text', data: '(Unknown cell type)' })
      setCellStatus(notebookId, cell.id, 'success')
    }
    void order
  }, [cell, notebookId, setCellOutput, setCellStatus, setCellExecutionOrder, updateCellSource])

  const statusBorder =
    cell.status === 'running' ? 'border-amber-400/40' :
    cell.status === 'success' ? 'border-emerald-400/30' :
    cell.status === 'error' ? 'border-red-400/30' :
    'border-zinc-700/30'

  return (
    <div className="group relative">
      {/* Cell container */}
      <div className={`border rounded-xl bg-zinc-900/50 transition-colors ${statusBorder}`}>
        {/* Toolbar */}
        <div className="flex items-center gap-2 px-3 py-1.5 border-b border-zinc-800/50">
          <div className={`flex items-center gap-1.5 text-xs font-medium ${config.color}`}>
            <Icon className="w-3.5 h-3.5" />
            <span>{config.label}</span>
          </div>

          {cell.executionOrder !== null && (
            <span className="text-zinc-600 text-xs font-mono">[{cell.executionOrder}]</span>
          )}

          <div className="flex-1" />

          {cell.status === 'running' && (
            <div className="w-3 h-3 border-2 border-amber-400/30 border-t-amber-400 rounded-full animate-spin" />
          )}

          <button
            onClick={runCell}
            className="p-1 rounded hover:bg-zinc-700/50 text-zinc-500 hover:text-emerald-400 transition-colors"
            title="Run cell (Shift+Enter)"
          >
            <Play className="w-3.5 h-3.5" />
          </button>

          <button
            onClick={() => moveCell(notebookId, cell.id, 'up')}
            disabled={index === 0}
            className="p-1 rounded hover:bg-zinc-700/50 text-zinc-500 hover:text-zinc-300 disabled:opacity-30 transition-colors"
          >
            <ChevronUp className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => moveCell(notebookId, cell.id, 'down')}
            disabled={index === totalCells - 1}
            className="p-1 rounded hover:bg-zinc-700/50 text-zinc-500 hover:text-zinc-300 disabled:opacity-30 transition-colors"
          >
            <ChevronDown className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => removeCell(notebookId, cell.id)}
            disabled={totalCells <= 1}
            className="p-1 rounded hover:bg-zinc-700/50 text-zinc-500 hover:text-red-400 disabled:opacity-30 transition-colors"
          >
            <Trash2 className="w-3.5 h-3.5" />
          </button>
        </div>

        {/* Editor */}
        <div className="min-h-[60px]">
          <Editor
            height={Math.max(60, Math.min(300, (cell.source.split('\n').length + 1) * 20))}
            language={config.lang}
            value={cell.source}
            onChange={v => updateCellSource(notebookId, cell.id, v || '')}
            theme="vs-dark"
            options={{
              minimap: { enabled: false },
              scrollBeyondLastLine: false,
              lineNumbers: 'on',
              fontSize: 13,
              fontFamily: 'JetBrains Mono, monospace',
              renderLineHighlight: 'none',
              overviewRulerBorder: false,
              hideCursorInOverviewRuler: true,
              scrollbar: { vertical: 'hidden', horizontal: 'auto' },
              padding: { top: 8, bottom: 8 },
              wordWrap: 'on',
            }}
            onMount={(editor) => {
              editorRef.current = editor
              // Shift+Enter to run
              editor.addCommand(
                // eslint-disable-next-line no-bitwise
                (window as unknown as { monaco: { KeyMod: { Shift: number }; KeyCode: { Enter: number } } }).monaco?.KeyMod?.Shift | (window as unknown as { monaco: { KeyCode: { Enter: number } } }).monaco?.KeyCode?.Enter || 0,
                () => runCell()
              )
            }}
          />
        </div>

        {/* Output */}
        {cell.output && (
          <div className="px-3 pb-3 pt-1 border-t border-zinc-800/50">
            <CellOutput output={cell.output} />
          </div>
        )}
      </div>

      {/* Add cell button (between cells) */}
      <div className="flex justify-center py-1 opacity-0 group-hover:opacity-100 transition-opacity">
        <div className="flex gap-1">
          {(['sql', 'python', 'markdown'] as const).map(type => (
            <button
              key={type}
              onClick={() => addCell(notebookId, type, cell.id)}
              className="flex items-center gap-1 px-2 py-0.5 text-2xs text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800/50 rounded transition-colors"
            >
              <Plus className="w-3 h-3" />
              {type === 'sql' ? 'SQL' : type === 'python' ? 'Python' : 'MD'}
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}
