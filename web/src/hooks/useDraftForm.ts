import { useState, useEffect, useCallback, useRef } from 'react'

/**
 * Hook that persists form state to localStorage as a draft.
 *
 * Users can close the drawer/page and come back to their unfinished work.
 * Draft is cleared when the form is successfully submitted.
 *
 * @param key - localStorage key (e.g., "draft:pipeline", "draft:job")
 * @param initialState - default form values when no draft exists
 */
export function useDraftForm<T extends object>(
  key: string,
  initialState: T,
): [T, (updater: T | ((prev: T) => T)) => void, { clearDraft: () => void; hasDraft: boolean }] {
  const [hasDraft, setHasDraft] = useState(false)

  // Load from localStorage on mount
  const [form, setFormRaw] = useState<T>(() => {
    try {
      const saved = localStorage.getItem(key)
      if (saved) {
        const parsed = JSON.parse(saved) as T
        // Only use draft if it has at least one non-empty field beyond defaults
        const hasContent = Object.entries(parsed).some(([k, v]) => {
          const defaultVal = (initialState as Record<string, unknown>)[k]
          return v !== defaultVal && v !== '' && v !== false && v !== 0
        })
        if (hasContent) {
          setHasDraft(true)
          return { ...initialState, ...parsed }
        }
      }
    } catch { /* ignore corrupted localStorage */ }
    return initialState
  })

  // Debounce timer ref
  const saveTimer = useRef<ReturnType<typeof setTimeout>>()

  // Wrap setForm to also persist to localStorage (debounced)
  const setForm = useCallback((updater: T | ((prev: T) => T)) => {
    setFormRaw(prev => {
      const next = typeof updater === 'function' ? (updater as (prev: T) => T)(prev) : updater

      // Debounce save to avoid thrashing localStorage on every keystroke
      clearTimeout(saveTimer.current)
      saveTimer.current = setTimeout(() => {
        try {
          localStorage.setItem(key, JSON.stringify(next))
          setHasDraft(true)
        } catch { /* localStorage full or unavailable */ }
      }, 500)

      return next
    })
  }, [key])

  // Clear draft (call on successful submit)
  const clearDraft = useCallback(() => {
    localStorage.removeItem(key)
    setHasDraft(false)
    setFormRaw(initialState)
  }, [key, initialState])

  // Cleanup timer on unmount
  useEffect(() => {
    return () => clearTimeout(saveTimer.current)
  }, [])

  return [form, setForm, { clearDraft, hasDraft }]
}
