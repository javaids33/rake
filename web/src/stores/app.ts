import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface AppState {
  sidebarCollapsed: boolean
  toggleSidebar: () => void
}

export const useAppStore = create<AppState>()(
  persist(
    (set) => ({
      sidebarCollapsed: false,
      toggleSidebar: () => set((s) => ({ sidebarCollapsed: !s.sidebarCollapsed })),
    }),
    { name: 'rustlake-app' }
  )
)
