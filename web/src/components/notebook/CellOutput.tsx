import type { CellOutput as CellOutputType } from '../../types'
import { GraphVisualization } from '../ui/GraphVisualization'

interface Props {
  output: CellOutputType
}

export function CellOutput({ output }: Props) {
  if (output.type === 'error') {
    return (
      <div className="px-4 py-3 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 text-sm font-mono whitespace-pre-wrap">
        {String(output.data)}
      </div>
    )
  }

  // Graph visualization for Neo4j/graph results
  if (output.type === 'table') {
    const data = output.data as Record<string, unknown>
    const graph = data?.graph as { nodes?: Array<{ id: string; label: string; group: string; properties: Record<string, string>; size: number }>; edges?: Array<{ source: string; target: string; label: string; properties: Record<string, string> }> } | undefined
    if (graph?.nodes && graph.nodes.length > 0) {
      return (
        <div className="space-y-3">
          <div className="text-xs text-zinc-500 px-1">Graph: {graph.nodes.length} nodes, {(graph.edges?.length || 0)} edges</div>
          <GraphVisualization
            nodes={graph.nodes}
            edges={graph.edges || []}
            height={350}
          />
        </div>
      )
    }
  }

  if (output.type === 'text') {
    return (
      <div className="px-4 py-3 bg-zinc-800/50 border border-zinc-700/30 rounded-lg text-zinc-300 text-sm font-mono whitespace-pre-wrap">
        {String(output.data)}
      </div>
    )
  }

  if (output.type === 'image') {
    return (
      <div className="px-4 py-3 flex justify-center">
        <img src={String(output.data)} alt="Cell output" className="max-w-full rounded-lg" />
      </div>
    )
  }

  if (output.type === 'table') {
    const tableData = output.data as { columns: string[]; rows: Record<string, unknown>[] }
    if (!tableData?.columns?.length) {
      return <div className="px-4 py-2 text-zinc-500 text-sm">No results</div>
    }

    return (
      <div className="overflow-auto max-h-[400px] border border-zinc-700/30 rounded-lg">
        <table className="w-full text-sm">
          <thead className="bg-zinc-800/80 sticky top-0">
            <tr>
              {tableData.columns.map(col => (
                <th key={col} className="px-3 py-2 text-left text-zinc-400 font-medium border-b border-zinc-700/30 whitespace-nowrap">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {tableData.rows.slice(0, 200).map((row, i) => (
              <tr key={i} className="border-b border-zinc-800/50 hover:bg-zinc-800/30">
                {tableData.columns.map(col => (
                  <td key={col} className="px-3 py-1.5 text-zinc-300 font-mono text-xs whitespace-nowrap max-w-[300px] truncate">
                    {row[col] === null ? <span className="text-zinc-600 italic">null</span> : String(row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {tableData.rows.length > 200 && (
          <div className="px-3 py-2 text-zinc-500 text-xs text-center bg-zinc-800/50">
            Showing 200 of {tableData.rows.length} rows
          </div>
        )}
      </div>
    )
  }

  return null
}
