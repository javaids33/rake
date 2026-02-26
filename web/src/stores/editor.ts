import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import type { EditorTab, SqlResponse, ChartType } from '../types'

interface SavedQuery {
  id: string
  name: string
  sql: string
  savedAt: string
}

interface EditorState {
  tabs: EditorTab[]
  activeTabId: string
  results: Record<string, SqlResponse | null>
  errors: Record<string, string | null>
  loading: Record<string, boolean>
  chartType: ChartType
  savedQueries: SavedQuery[]

  addTab: () => void
  removeTab: (id: string) => void
  setActiveTab: (id: string) => void
  updateTabSql: (id: string, sql: string) => void
  renameTab: (id: string, name: string) => void
  setResult: (tabId: string, result: SqlResponse | null) => void
  setError: (tabId: string, error: string | null) => void
  setLoading: (tabId: string, loading: boolean) => void
  setChartType: (type: ChartType) => void
  saveQuery: (name: string, sql: string) => void
  deleteSavedQuery: (id: string) => void
}

const defaultTab: EditorTab = { id: '1', name: 'Query 1', sql: 'SELECT 1 + 1 AS result;' }

export const useEditorStore = create<EditorState>()(
  persist(
    (set, get) => ({
      tabs: [defaultTab],
      activeTabId: '1',
      results: {},
      errors: {},
      loading: {},
      chartType: 'bar',
      savedQueries: [],

      addTab: () => {
        const tabs = get().tabs
        if (tabs.length >= 8) return
        const id = String(Date.now())
        set({
          tabs: [...tabs, { id, name: `Query ${tabs.length + 1}`, sql: '' }],
          activeTabId: id,
        })
      },
      removeTab: (id) => {
        const { tabs, activeTabId } = get()
        if (tabs.length <= 1) return
        const next = tabs.filter(t => t.id !== id)
        set({
          tabs: next,
          activeTabId: activeTabId === id ? next[0].id : activeTabId,
        })
      },
      setActiveTab: (id) => set({ activeTabId: id }),
      updateTabSql: (id, sql) =>
        set((s) => ({
          tabs: s.tabs.map(t => t.id === id ? { ...t, sql } : t),
          errors: { ...s.errors, [id]: null },
        })),
      renameTab: (id, name) =>
        set((s) => ({ tabs: s.tabs.map(t => t.id === id ? { ...t, name } : t) })),
      setResult: (tabId, result) =>
        set((s) => ({ results: { ...s.results, [tabId]: result } })),
      setError: (tabId, error) =>
        set((s) => ({ errors: { ...s.errors, [tabId]: error } })),
      setLoading: (tabId, loading) =>
        set((s) => ({ loading: { ...s.loading, [tabId]: loading } })),
      setChartType: (chartType) => set({ chartType }),
      saveQuery: (name, sql) => {
        const id = String(Date.now())
        set((s) => ({
          savedQueries: [...s.savedQueries, { id, name, sql, savedAt: new Date().toISOString() }],
        }))
      },
      deleteSavedQuery: (id) =>
        set((s) => ({ savedQueries: s.savedQueries.filter(q => q.id !== id) })),
    }),
    {
      name: 'rustlake-editor',
      partialize: (s) => ({
        tabs: s.tabs,
        activeTabId: s.activeTabId,
        chartType: s.chartType,
        savedQueries: s.savedQueries,
      }),
    }
  )
)
