import { useState, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { Plus, FileText, Trash2, Edit3, Check, X, Zap, Clock, Play } from 'lucide-react'
import toast from 'react-hot-toast'
import { useNotebookStore } from '../stores/notebook'
import { NotebookCell } from '../components/notebook/NotebookCell'
import { WasmStatus } from '../components/notebook/WasmStatus'

export function Notebooks() {
  const {
    notebooks, activeNotebookId, setActiveNotebook,
    createNotebook, deleteNotebook, renameNotebook, addCell,
  } = useNotebookStore()

  const [renamingId, setRenamingId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState('')
  const [showSchedule, setShowSchedule] = useState(false)
  const [scheduleValue, setScheduleValue] = useState('0 * * * *')
  const navigate = useNavigate()

  const notebookList = useMemo(
    () => Object.values(notebooks).sort((a, b) => b.createdAt.localeCompare(a.createdAt)),
    [notebooks]
  )

  const activeNotebook = activeNotebookId ? notebooks[activeNotebookId] : null

  const startRename = (id: string, name: string) => {
    setRenamingId(id)
    setRenameValue(name)
  }

  const confirmRename = () => {
    if (renamingId && renameValue.trim()) {
      renameNotebook(renamingId, renameValue.trim())
    }
    setRenamingId(null)
  }

  return (
    <div className="flex h-full overflow-hidden">
      {/* Sidebar: notebook list */}
      <div className="w-64 flex-shrink-0 border-r border-zinc-800/50 flex flex-col bg-zinc-900/30">
        <div className="p-3 border-b border-zinc-800/50">
          <button
            onClick={() => createNotebook()}
            className="w-full flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-500/10 border border-violet-500/20 text-violet-400 hover:bg-violet-500/15 text-sm font-medium transition-colors"
          >
            <Plus className="w-4 h-4" />
            New Notebook
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-2 space-y-0.5">
          {notebookList.length === 0 && (
            <div className="text-zinc-600 text-sm text-center py-8">
              No notebooks yet
            </div>
          )}
          {notebookList.map(nb => (
            <div
              key={nb.id}
              className={`group flex items-center gap-2 px-3 py-2 rounded-lg cursor-pointer transition-colors ${
                nb.id === activeNotebookId
                  ? 'bg-violet-500/10 border border-violet-500/20 text-violet-300'
                  : 'text-zinc-400 hover:bg-zinc-800/50 border border-transparent'
              }`}
              onClick={() => setActiveNotebook(nb.id)}
            >
              <FileText className="w-4 h-4 flex-shrink-0" />
              {renamingId === nb.id ? (
                <div className="flex items-center gap-1 flex-1 min-w-0">
                  <input
                    value={renameValue}
                    onChange={e => setRenameValue(e.target.value)}
                    onKeyDown={e => { if (e.key === 'Enter') confirmRename(); if (e.key === 'Escape') setRenamingId(null) }}
                    className="flex-1 bg-zinc-800 rounded px-1.5 py-0.5 text-sm text-zinc-200 outline-none border border-zinc-600"
                    autoFocus
                    onClick={e => e.stopPropagation()}
                  />
                  <button onClick={(e) => { e.stopPropagation(); confirmRename() }} className="p-0.5 text-emerald-400"><Check className="w-3 h-3" /></button>
                  <button onClick={(e) => { e.stopPropagation(); setRenamingId(null) }} className="p-0.5 text-zinc-500"><X className="w-3 h-3" /></button>
                </div>
              ) : (
                <>
                  <span className="text-sm truncate flex-1">{nb.name}</span>
                  <span className="text-2xs text-zinc-600">{nb.cells.length}c</span>
                  <div className="hidden group-hover:flex items-center gap-0.5">
                    <button
                      onClick={(e) => { e.stopPropagation(); startRename(nb.id, nb.name) }}
                      className="p-0.5 text-zinc-500 hover:text-zinc-300"
                    >
                      <Edit3 className="w-3 h-3" />
                    </button>
                    <button
                      onClick={(e) => { e.stopPropagation(); deleteNotebook(nb.id) }}
                      className="p-0.5 text-zinc-500 hover:text-red-400"
                    >
                      <Trash2 className="w-3 h-3" />
                    </button>
                  </div>
                </>
              )}
            </div>
          ))}
        </div>
        <div className="p-3 border-t border-zinc-800/50">
          <WasmStatus />
        </div>
      </div>

      {/* Main content: active notebook */}
      <div className="flex-1 overflow-y-auto">
        {!activeNotebook ? (
          <div className="flex flex-col items-center justify-center h-full text-zinc-600">
            <FileText className="w-16 h-16 mb-4 opacity-30" />
            <p className="text-lg font-medium mb-2">No notebook selected</p>
            <p className="text-sm mb-4">Create a new notebook or select one from the sidebar</p>
            <button
              onClick={() => createNotebook()}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-500/10 border border-violet-500/20 text-violet-400 hover:bg-violet-500/15 text-sm font-medium transition-colors"
            >
              <Plus className="w-4 h-4" />
              Create Notebook
            </button>
          </div>
        ) : (
          <div className="max-w-4xl mx-auto px-6 py-6">
            {/* Notebook title */}
            <div className="mb-6">
              <h1 className="text-xl font-bold text-zinc-100 mb-1">{activeNotebook.name}</h1>
              <p className="text-xs text-zinc-600">
                {activeNotebook.cells.length} cells &middot; Created {new Date(activeNotebook.createdAt).toLocaleDateString()}
                {activeNotebook.updatedAt && ` &middot; Updated ${new Date(activeNotebook.updatedAt).toLocaleDateString()}`}
              </p>
              {/* ETL Controls */}
              <div className="flex items-center gap-2 mt-3">
                <button
                  onClick={async () => {
                    if (!activeNotebook) return
                    const submission = {
                      id: activeNotebook.id,
                      name: activeNotebook.name,
                      cells: activeNotebook.cells.map(c => ({
                        id: c.id,
                        type: c.type,
                        source: c.source,
                        depends_on: [] as string[],
                      })),
                    }
                    try {
                      const resp = await fetch('/api/v1/notebooks/plan', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(submission),
                      })
                      const plan = await resp.json()
                      toast.success(`Plan: ${plan.stages?.length || 0} stages, ${plan.optimizations?.length || 0} optimizations`)
                    } catch {
                      toast.error('Failed to generate plan')
                    }
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-zinc-400 hover:text-amber-400 border border-zinc-700 hover:border-amber-500/30 rounded-lg transition-colors"
                >
                  <Zap className="w-3.5 h-3.5" />
                  View Execution Plan
                </button>
                {showSchedule ? (
                  <div className="flex items-center gap-1.5">
                    <input
                      value={scheduleValue}
                      onChange={e => setScheduleValue(e.target.value)}
                      placeholder="0 * * * *"
                      className="w-28 px-2 py-1 text-xs font-mono rounded-md bg-navy-900/60 border border-emerald-500/30 text-zinc-200 focus:outline-none focus:ring-1 focus:ring-emerald-400/40"
                      autoFocus
                      onKeyDown={e => { if (e.key === 'Escape') setShowSchedule(false) }}
                    />
                    <button
                      onClick={async () => {
                        if (!activeNotebook || !scheduleValue.trim()) return
                        try {
                          const resp = await fetch('/api/v1/notebooks/schedule', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                              notebook_id: activeNotebook.id,
                              notebook_name: activeNotebook.name,
                              schedule: scheduleValue.trim(),
                              cells_to_run: [],
                              tags: ['notebook-etl'],
                            }),
                          })
                          const result = await resp.json()
                          toast.success(`Scheduled: ${result.job_id || 'created'}`)
                          setShowSchedule(false)
                        } catch {
                          toast.error('Failed to schedule')
                        }
                      }}
                      className="px-2 py-1 text-xs text-emerald-400 border border-emerald-500/30 rounded-md hover:bg-emerald-500/10 transition-colors"
                    >
                      Schedule
                    </button>
                    <button
                      onClick={() => setShowSchedule(false)}
                      className="px-1.5 py-1 text-xs text-zinc-500 hover:text-zinc-300 transition-colors"
                    >
                      <X className="w-3 h-3" />
                    </button>
                  </div>
                ) : (
                  <button
                    onClick={() => setShowSchedule(true)}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-zinc-400 hover:text-emerald-400 border border-zinc-700 hover:border-emerald-500/30 rounded-lg transition-colors"
                  >
                    <Clock className="w-3.5 h-3.5" />
                    Schedule as Job
                  </button>
                )}
                <button
                  onClick={async () => {
                    if (!activeNotebook) return
                    const submission = {
                      id: activeNotebook.id,
                      name: activeNotebook.name,
                      cells: activeNotebook.cells.map(c => ({
                        id: c.id,
                        type: c.type,
                        source: c.source,
                        depends_on: [] as string[],
                      })),
                    }
                    try {
                      const resp = await fetch('/api/v1/notebooks/execute', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(submission),
                      })
                      const result = await resp.json()
                      toast.success(`Notebook executed: ${result.status} (${result.total_duration_ms}ms, ${result.cell_results?.length || 0} cells)`)
                    } catch {
                      toast.error('Failed to execute notebook')
                    }
                  }}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-white bg-amber-500/20 hover:bg-amber-500/30 border border-amber-500/30 rounded-lg transition-colors"
                >
                  <Play className="w-3.5 h-3.5" />
                  Run All Cells
                </button>
              </div>
            </div>

            {/* Cells */}
            <div className="space-y-1">
              {activeNotebook.cells.map((cell, i) => (
                <NotebookCell
                  key={cell.id}
                  notebookId={activeNotebook.id}
                  cell={cell}
                  index={i}
                  totalCells={activeNotebook.cells.length}
                />
              ))}
            </div>

            {/* Add cell at bottom */}
            <div className="flex justify-center gap-2 mt-4 pb-16">
              {(['sql', 'python', 'rust', 'markdown'] as const).map(type => (
                <button
                  key={type}
                  onClick={() => addCell(activeNotebook.id, type)}
                  className="flex items-center gap-1.5 px-3 py-1.5 text-xs text-zinc-500 hover:text-zinc-300 border border-zinc-800 hover:border-zinc-700 rounded-lg transition-colors"
                >
                  <Plus className="w-3 h-3" />
                  {type === 'sql' ? 'SQL Cell' : type === 'python' ? 'Python Cell' : type === 'rust' ? 'Rust Cell' : 'Markdown Cell'}
                </button>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default Notebooks
