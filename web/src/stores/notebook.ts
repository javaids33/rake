import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type { NotebookDocument, NotebookCell, CellOutput } from '../types'

interface NotebookStore {
  notebooks: Record<string, NotebookDocument>
  activeNotebookId: string | null
  pyodideReady: boolean
  executionCounter: number

  // Notebook actions
  createNotebook: (name?: string) => string
  deleteNotebook: (id: string) => void
  setActiveNotebook: (id: string | null) => void
  renameNotebook: (id: string, name: string) => void

  // Cell actions
  addCell: (notebookId: string, type: NotebookCell['type'], afterCellId?: string) => string
  removeCell: (notebookId: string, cellId: string) => void
  moveCell: (notebookId: string, cellId: string, direction: 'up' | 'down') => void
  updateCellSource: (notebookId: string, cellId: string, source: string) => void
  setCellOutput: (notebookId: string, cellId: string, output: CellOutput | null) => void
  setCellStatus: (notebookId: string, cellId: string, status: NotebookCell['status']) => void
  setCellExecutionOrder: (notebookId: string, cellId: string) => number

  // Pyodide
  setPyodideReady: (ready: boolean) => void
}

const genId = () => crypto.randomUUID()

export const useNotebookStore = create<NotebookStore>()(
  persist(
    (set, get) => ({
      notebooks: {},
      activeNotebookId: null,
      pyodideReady: false,
      executionCounter: 0,

      createNotebook: (name?: string) => {
        const id = genId()
        const now = new Date().toISOString()
        const notebook: NotebookDocument = {
          id,
          name: name || `Notebook ${Object.keys(get().notebooks).length + 1}`,
          cells: [
            { id: genId(), type: 'sql', source: '', output: null, status: 'idle', executionOrder: null },
          ],
          createdAt: now,
          updatedAt: now,
        }
        set(s => ({
          notebooks: { ...s.notebooks, [id]: notebook },
          activeNotebookId: id,
        }))
        return id
      },

      deleteNotebook: (id) => {
        set(s => {
          const { [id]: _, ...rest } = s.notebooks
          return {
            notebooks: rest,
            activeNotebookId: s.activeNotebookId === id ? null : s.activeNotebookId,
          }
        })
      },

      setActiveNotebook: (id) => set({ activeNotebookId: id }),

      renameNotebook: (id, name) => {
        set(s => {
          const nb = s.notebooks[id]
          if (!nb) return s
          return { notebooks: { ...s.notebooks, [id]: { ...nb, name, updatedAt: new Date().toISOString() } } }
        })
      },

      addCell: (notebookId, type, afterCellId) => {
        const cellId = genId()
        set(s => {
          const nb = s.notebooks[notebookId]
          if (!nb) return s
          const cell: NotebookCell = { id: cellId, type, source: '', output: null, status: 'idle', executionOrder: null }
          const cells = [...nb.cells]
          if (afterCellId) {
            const idx = cells.findIndex(c => c.id === afterCellId)
            cells.splice(idx + 1, 0, cell)
          } else {
            cells.push(cell)
          }
          return { notebooks: { ...s.notebooks, [notebookId]: { ...nb, cells, updatedAt: new Date().toISOString() } } }
        })
        return cellId
      },

      removeCell: (notebookId, cellId) => {
        set(s => {
          const nb = s.notebooks[notebookId]
          if (!nb || nb.cells.length <= 1) return s
          return {
            notebooks: {
              ...s.notebooks,
              [notebookId]: { ...nb, cells: nb.cells.filter(c => c.id !== cellId), updatedAt: new Date().toISOString() },
            },
          }
        })
      },

      moveCell: (notebookId, cellId, direction) => {
        set(s => {
          const nb = s.notebooks[notebookId]
          if (!nb) return s
          const cells = [...nb.cells]
          const idx = cells.findIndex(c => c.id === cellId)
          if (idx < 0) return s
          const swapIdx = direction === 'up' ? idx - 1 : idx + 1
          if (swapIdx < 0 || swapIdx >= cells.length) return s
          ;[cells[idx], cells[swapIdx]] = [cells[swapIdx], cells[idx]]
          return { notebooks: { ...s.notebooks, [notebookId]: { ...nb, cells, updatedAt: new Date().toISOString() } } }
        })
      },

      updateCellSource: (notebookId, cellId, source) => {
        set(s => {
          const nb = s.notebooks[notebookId]
          if (!nb) return s
          return {
            notebooks: {
              ...s.notebooks,
              [notebookId]: {
                ...nb,
                cells: nb.cells.map(c => c.id === cellId ? { ...c, source } : c),
                updatedAt: new Date().toISOString(),
              },
            },
          }
        })
      },

      setCellOutput: (notebookId, cellId, output) => {
        set(s => {
          const nb = s.notebooks[notebookId]
          if (!nb) return s
          return {
            notebooks: {
              ...s.notebooks,
              [notebookId]: {
                ...nb,
                cells: nb.cells.map(c => c.id === cellId ? { ...c, output, status: output?.type === 'error' ? 'error' : 'success' } : c),
              },
            },
          }
        })
      },

      setCellStatus: (notebookId, cellId, status) => {
        set(s => {
          const nb = s.notebooks[notebookId]
          if (!nb) return s
          return {
            notebooks: {
              ...s.notebooks,
              [notebookId]: {
                ...nb,
                cells: nb.cells.map(c => c.id === cellId ? { ...c, status } : c),
              },
            },
          }
        })
      },

      setCellExecutionOrder: (notebookId, cellId) => {
        const next = get().executionCounter + 1
        set(s => {
          const nb = s.notebooks[notebookId]
          if (!nb) return { executionCounter: next }
          return {
            executionCounter: next,
            notebooks: {
              ...s.notebooks,
              [notebookId]: {
                ...nb,
                cells: nb.cells.map(c => c.id === cellId ? { ...c, executionOrder: next } : c),
              },
            },
          }
        })
        return next
      },

      setPyodideReady: (ready) => set({ pyodideReady: ready }),
    }),
    {
      name: 'rustlake-notebooks',
      partialize: (state) => ({
        notebooks: state.notebooks,
        activeNotebookId: state.activeNotebookId,
        executionCounter: state.executionCounter,
      }),
    }
  )
)
