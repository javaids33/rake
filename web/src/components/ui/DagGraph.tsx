import { useMemo } from 'react'

export interface DagNode {
  id: string
  label: string
  type: 'source' | 'transform' | 'target' | 'table'
  status?: 'healthy' | 'warning' | 'error' | 'idle'
  meta?: string
}

export interface DagEdge {
  source: string
  target: string
  label?: string
}

interface DagGraphProps {
  nodes: DagNode[]
  edges: DagEdge[]
  onNodeClick?: (id: string) => void
  height?: number
}

const NODE_W = 160
const NODE_H = 48
const LAYER_GAP_X = 200
const NODE_GAP_Y = 72

const typeColors: Record<string, { bg: string; border: string; text: string; icon: string }> = {
  source: { bg: 'rgba(14,165,233,0.12)', border: 'rgba(14,165,233,0.5)', text: '#38bdf8', icon: '📥' },
  transform: { bg: 'rgba(245,158,11,0.12)', border: 'rgba(245,158,11,0.5)', text: '#f59e0b', icon: '⚙️' },
  target: { bg: 'rgba(34,197,94,0.12)', border: 'rgba(34,197,94,0.5)', text: '#22c55e', icon: '📤' },
  table: { bg: 'rgba(148,163,184,0.1)', border: 'rgba(148,163,184,0.3)', text: '#94a3b8', icon: '📋' },
}

const statusDotColors: Record<string, string> = {
  healthy: '#22c55e',
  warning: '#f59e0b',
  error: '#ef4444',
  idle: '#64748b',
}

export function DagGraph({ nodes, edges, onNodeClick, height = 340 }: DagGraphProps) {
  const layout = useMemo(() => {
    // Topological layering: assign each node to a layer based on longest path from roots
    const adj = new Map<string, string[]>()
    const inDegree = new Map<string, number>()

    for (const n of nodes) {
      adj.set(n.id, [])
      inDegree.set(n.id, 0)
    }
    for (const e of edges) {
      adj.get(e.source)?.push(e.target)
      inDegree.set(e.target, (inDegree.get(e.target) ?? 0) + 1)
    }

    // BFS from roots (in-degree 0)
    const layers = new Map<string, number>()
    const queue: string[] = []
    for (const n of nodes) {
      if ((inDegree.get(n.id) ?? 0) === 0) {
        queue.push(n.id)
        layers.set(n.id, 0)
      }
    }

    while (queue.length > 0) {
      const curr = queue.shift()!
      const currLayer = layers.get(curr) ?? 0
      for (const next of adj.get(curr) ?? []) {
        const newLayer = currLayer + 1
        if (newLayer > (layers.get(next) ?? 0)) {
          layers.set(next, newLayer)
        }
        // Only add to queue once all parents are processed (simple approach: always re-process)
        if (!queue.includes(next)) queue.push(next)
      }
    }

    // Handle disconnected nodes
    for (const n of nodes) {
      if (!layers.has(n.id)) layers.set(n.id, 0)
    }

    // Group nodes by layer
    const byLayer = new Map<number, string[]>()
    for (const [id, layer] of layers) {
      if (!byLayer.has(layer)) byLayer.set(layer, [])
      byLayer.get(layer)!.push(id)
    }

    // Position nodes
    const maxLayer = Math.max(0, ...byLayer.keys())
    const positions = new Map<string, { x: number; y: number }>()

    for (let l = 0; l <= maxLayer; l++) {
      const nodesInLayer = byLayer.get(l) ?? []
      const layerHeight = nodesInLayer.length * NODE_GAP_Y
      const startY = (height - layerHeight) / 2 + NODE_GAP_Y / 2

      for (let i = 0; i < nodesInLayer.length; i++) {
        positions.set(nodesInLayer[i], {
          x: 40 + l * LAYER_GAP_X,
          y: Math.max(20, startY + i * NODE_GAP_Y),
        })
      }
    }

    const totalWidth = 80 + (maxLayer + 1) * LAYER_GAP_X
    return { positions, totalWidth }
  }, [nodes, edges, height])

  return (
    <div style={{ width: '100%', overflowX: 'auto', overflowY: 'hidden' }}>
      <svg
        width={Math.max(layout.totalWidth, 400)}
        height={height}
        style={{ display: 'block' }}
      >
        <defs>
          <marker
            id="dag-arrow"
            viewBox="0 0 10 7"
            refX="10"
            refY="3.5"
            markerWidth="8"
            markerHeight="6"
            orient="auto"
          >
            <polygon points="0 0, 10 3.5, 0 7" fill="rgba(245,158,11,0.5)" />
          </marker>
        </defs>

        {/* Edges */}
        {edges.map((e, i) => {
          const from = layout.positions.get(e.source)
          const to = layout.positions.get(e.target)
          if (!from || !to) return null

          const x1 = from.x + NODE_W
          const y1 = from.y + NODE_H / 2
          const x2 = to.x
          const y2 = to.y + NODE_H / 2
          const cx1 = x1 + (x2 - x1) * 0.4
          const cx2 = x2 - (x2 - x1) * 0.4

          return (
            <g key={`edge-${i}`}>
              <path
                d={`M ${x1} ${y1} C ${cx1} ${y1}, ${cx2} ${y2}, ${x2} ${y2}`}
                fill="none"
                stroke="rgba(245,158,11,0.3)"
                strokeWidth={2}
                markerEnd="url(#dag-arrow)"
              />
              {e.label && (
                <text
                  x={(x1 + x2) / 2}
                  y={(y1 + y2) / 2 - 8}
                  textAnchor="middle"
                  fill="#64748b"
                  fontSize={10}
                >
                  {e.label}
                </text>
              )}
            </g>
          )
        })}

        {/* Nodes */}
        {nodes.map((n) => {
          const pos = layout.positions.get(n.id)
          if (!pos) return null
          const colors = typeColors[n.type] ?? typeColors.table

          return (
            <g
              key={n.id}
              style={{ cursor: onNodeClick ? 'pointer' : 'default' }}
              onClick={() => onNodeClick?.(n.id)}
            >
              <rect
                x={pos.x}
                y={pos.y}
                width={NODE_W}
                height={NODE_H}
                rx={8}
                ry={8}
                fill={colors.bg}
                stroke={colors.border}
                strokeWidth={1.5}
              />
              {/* Status dot */}
              {n.status && (
                <circle
                  cx={pos.x + NODE_W - 12}
                  cy={pos.y + 12}
                  r={4}
                  fill={statusDotColors[n.status] ?? '#64748b'}
                />
              )}
              {/* Icon */}
              <text x={pos.x + 12} y={pos.y + NODE_H / 2 + 1} fontSize={14} dominantBaseline="middle">
                {colors.icon}
              </text>
              {/* Label */}
              <text
                x={pos.x + 32}
                y={pos.y + NODE_H / 2 - 2}
                fill={colors.text}
                fontSize={12}
                fontWeight={600}
                fontFamily="'DM Sans', sans-serif"
                dominantBaseline="middle"
              >
                {n.label.length > 14 ? n.label.slice(0, 14) + '...' : n.label}
              </text>
              {/* Meta */}
              {n.meta && (
                <text
                  x={pos.x + 32}
                  y={pos.y + NODE_H / 2 + 12}
                  fill="#64748b"
                  fontSize={10}
                  fontFamily="'JetBrains Mono', monospace"
                  dominantBaseline="middle"
                >
                  {n.meta}
                </text>
              )}
            </g>
          )
        })}
      </svg>
    </div>
  )
}
