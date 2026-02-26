import Editor, { type OnMount } from '@monaco-editor/react'
import { useRef, useEffect } from 'react'
import type { editor } from 'monaco-editor'

interface SqlEditorProps {
  value: string
  onChange: (value: string) => void
  onRun: () => void
  tables?: string[]
  columns?: Record<string, Array<{ name: string; type: string }>>
}

export function SqlEditorComponent({ value, onChange, onRun, tables = [], columns = {} }: SqlEditorProps) {
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null)

  const handleMount: OnMount = (editor, monaco) => {
    editorRef.current = editor

    // Run query shortcut
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, onRun)

    // Custom theme
    monaco.editor.defineTheme('rustlake', {
      base: 'vs-dark',
      inherit: true,
      rules: [
        { token: 'keyword', foreground: 'fbbf24', fontStyle: 'bold' },
        { token: 'string', foreground: '10b981' },
        { token: 'number', foreground: '22d3ee' },
        { token: 'comment', foreground: '334155', fontStyle: 'italic' },
        { token: 'operator', foreground: '64748b' },
        { token: 'type', foreground: '818cf8' },
        { token: 'identifier', foreground: 'cbd5e1' },
      ],
      colors: {
        'editor.background': '#0a1122',
        'editor.foreground': '#cbd5e1',
        'editor.lineHighlightBackground': '#0f1a3340',
        'editor.selectionBackground': '#fbbf2420',
        'editorCursor.foreground': '#fbbf24',
        'editorLineNumber.foreground': '#1e3a5f',
        'editorLineNumber.activeForeground': '#475569',
        'editorWidget.background': '#0d1730',
        'editorWidget.border': '#1a2b52',
        'editorSuggestWidget.background': '#0d1730',
        'editorSuggestWidget.border': '#1a2b52',
        'editorSuggestWidget.selectedBackground': '#142244',
      },
    })
    monaco.editor.setTheme('rustlake')

    // Autocomplete provider
    monaco.languages.registerCompletionItemProvider('sql', {
      triggerCharacters: ['.', ' '],
      provideCompletionItems: (model: any, position: any) => {
        const word = model.getWordUntilPosition(position)
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        }

        // Check if triggered after a dot for column completion
        const lineContent = model.getLineContent(position.lineNumber)
        const textBefore = lineContent.substring(0, position.column - 1)
        const dotMatch = textBefore.match(/(\w+)\.\s*$/)

        if (dotMatch) {
          const tableName = dotMatch[1]
          const cols = columns[tableName] || []
          return {
            suggestions: cols.map(c => ({
              label: c.name,
              kind: monaco.languages.CompletionItemKind.Field,
              detail: c.type,
              insertText: c.name,
              range,
            })),
          }
        }

        const tableSuggestions = tables.map(t => ({
          label: t,
          kind: monaco.languages.CompletionItemKind.Struct,
          detail: 'Table',
          insertText: t,
          range,
        }))

        const keywords = [
          'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'FULL',
          'ON', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL',
          'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET', 'AS',
          'INSERT INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE TABLE',
          'DROP TABLE', 'ALTER TABLE', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX',
          'DISTINCT', 'UNION', 'ALL', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
          'ASC', 'DESC', 'CAST', 'COALESCE', 'NULLIF', 'WITH', 'RECURSIVE',
        ]

        const kwSuggestions = keywords.map(kw => ({
          label: kw,
          kind: monaco.languages.CompletionItemKind.Keyword,
          insertText: kw,
          range,
        }))

        return { suggestions: [...tableSuggestions, ...kwSuggestions] }
      },
    })
  }

  useEffect(() => {
    if (editorRef.current) {
      const model = editorRef.current.getModel()
      if (model && model.getValue() !== value) {
        model.setValue(value)
      }
    }
  }, [value])

  return (
    <Editor
      height="100%"
      language="sql"
      value={value}
      onChange={(v) => onChange(v || '')}
      onMount={handleMount}
      options={{
        fontSize: 13,
        fontFamily: '"JetBrains Mono", "Fira Code", monospace',
        fontLigatures: true,
        minimap: { enabled: false },
        lineNumbers: 'on',
        glyphMargin: false,
        folding: true,
        lineDecorationsWidth: 8,
        lineNumbersMinChars: 3,
        renderLineHighlight: 'line',
        scrollBeyondLastLine: false,
        wordWrap: 'on',
        padding: { top: 12, bottom: 12 },
        suggestOnTriggerCharacters: true,
        quickSuggestions: true,
        tabSize: 2,
        overviewRulerLanes: 0,
        hideCursorInOverviewRuler: true,
        overviewRulerBorder: false,
        scrollbar: {
          verticalScrollbarSize: 6,
          horizontalScrollbarSize: 6,
        },
      }}
    />
  )
}
