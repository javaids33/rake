import { useRef, useState, useEffect, useCallback } from 'react'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface GraphNode {
  id: string
  label: string
  group: string
  properties: Record<string, string>
  size: number
  // Internal simulation state
  x?: number
  y?: number
  vx?: number
  vy?: number
}

interface GraphEdge {
  source: string
  target: string
  label: string
  properties: Record<string, string>
}

interface GraphVisualizationProps {
  nodes: GraphNode[]
  edges: GraphEdge[]
  width?: number
  height?: number
  onNodeClick?: (node: GraphNode) => void
  className?: string
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const GROUP_COLORS: Record<string, string> = {
  Person: '#f59e0b',   // amber
  Company: '#06b6d4',  // cyan
  Product: '#8b5cf6',  // violet
  Location: '#10b981', // emerald
  Event: '#f43f5e',    // rose
  default: '#a1a1aa',  // zinc
}

const REPULSION = 5000
const ATTRACTION = 0.005
const DAMPING = 0.85
const CENTER_GRAVITY = 0.01
const MAX_ITERATIONS = 300
const MIN_VELOCITY = 0.01
const ARROW_SIZE = 8

function colorForGroup(group: string): string {
  return GROUP_COLORS[group] ?? GROUP_COLORS.default
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function GraphVisualization({
  nodes: inputNodes,
  edges,
  width = 800,
  height = 600,
  onNodeClick,
  className = '',
}: GraphVisualizationProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const animFrameRef = useRef<number>(0)
  const nodesRef = useRef<GraphNode[]>([])

  const [zoom, setZoom] = useState(1)
  const [pan, setPan] = useState({ x: 0, y: 0 })
  const [selectedNode, setSelectedNode] = useState<GraphNode | null>(null)
  const [hoveredNode, setHoveredNode] = useState<GraphNode | null>(null)
  const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 })

  // Dragging state refs (avoid re-renders during drag)
  const dragNodeRef = useRef<GraphNode | null>(null)
  const isPanningRef = useRef(false)
  const lastMouseRef = useRef({ x: 0, y: 0 })

  // -------------------------------------------------------------------
  // Initialise node positions
  // -------------------------------------------------------------------
  useEffect(() => {
    const simNodes: GraphNode[] = inputNodes.map((n, i) => ({
      ...n,
      x: n.x ?? width / 2 + (Math.random() - 0.5) * width * 0.6,
      y: n.y ?? height / 2 + (Math.random() - 0.5) * height * 0.6,
      vx: 0,
      vy: 0,
    }))
    nodesRef.current = simNodes
  }, [inputNodes, width, height])

  // -------------------------------------------------------------------
  // Build edge lookup
  // -------------------------------------------------------------------
  const nodeMap = useCallback(() => {
    const map = new Map<string, GraphNode>()
    for (const n of nodesRef.current) {
      map.set(n.id, n)
    }
    return map
  }, [])

  // -------------------------------------------------------------------
  // Force simulation + render loop
  // -------------------------------------------------------------------
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    let iteration = 0

    function tick() {
      const ns = nodesRef.current
      if (ns.length === 0) {
        draw(ctx!)
        return
      }

      // --- Forces ---
      // Reset accelerations (use vx/vy as accumulators then integrate)
      for (const n of ns) {
        // Center gravity
        const dx = width / 2 - (n.x ?? 0)
        const dy = height / 2 - (n.y ?? 0)
        n.vx = (n.vx ?? 0) + dx * CENTER_GRAVITY
        n.vy = (n.vy ?? 0) + dy * CENTER_GRAVITY
      }

      // Repulsion (all pairs)
      for (let i = 0; i < ns.length; i++) {
        for (let j = i + 1; j < ns.length; j++) {
          const a = ns[i]
          const b = ns[j]
          let dx = (a.x ?? 0) - (b.x ?? 0)
          let dy = (a.y ?? 0) - (b.y ?? 0)
          let dist = Math.sqrt(dx * dx + dy * dy) || 1
          const force = REPULSION / (dist * dist)
          const fx = (dx / dist) * force
          const fy = (dy / dist) * force
          a.vx = (a.vx ?? 0) + fx
          a.vy = (a.vy ?? 0) + fy
          b.vx = (b.vx ?? 0) - fx
          b.vy = (b.vy ?? 0) - fy
        }
      }

      // Attraction along edges
      const map = new Map<string, GraphNode>()
      for (const n of ns) map.set(n.id, n)

      for (const edge of edges) {
        const src = map.get(edge.source)
        const tgt = map.get(edge.target)
        if (!src || !tgt) continue
        const dx = (tgt.x ?? 0) - (src.x ?? 0)
        const dy = (tgt.y ?? 0) - (src.y ?? 0)
        const fx = dx * ATTRACTION
        const fy = dy * ATTRACTION
        src.vx = (src.vx ?? 0) + fx
        src.vy = (src.vy ?? 0) + fy
        tgt.vx = (tgt.vx ?? 0) - fx
        tgt.vy = (tgt.vy ?? 0) - fy
      }

      // Integrate
      let totalVelocity = 0
      for (const n of ns) {
        if (dragNodeRef.current && dragNodeRef.current.id === n.id) {
          n.vx = 0
          n.vy = 0
          continue
        }
        n.vx = (n.vx ?? 0) * DAMPING
        n.vy = (n.vy ?? 0) * DAMPING
        n.x = (n.x ?? 0) + (n.vx ?? 0)
        n.y = (n.y ?? 0) + (n.vy ?? 0)
        totalVelocity += Math.abs(n.vx ?? 0) + Math.abs(n.vy ?? 0)
      }

      draw(ctx!)
      iteration++

      if (iteration < MAX_ITERATIONS && totalVelocity > MIN_VELOCITY * ns.length) {
        animFrameRef.current = requestAnimationFrame(tick)
      }
    }

    function draw(ctx: CanvasRenderingContext2D) {
      const ns = nodesRef.current
      ctx.clearRect(0, 0, width, height)
      ctx.fillStyle = '#18181b' // zinc-900
      ctx.fillRect(0, 0, width, height)

      ctx.save()
      ctx.translate(pan.x, pan.y)
      ctx.scale(zoom, zoom)

      const map = new Map<string, GraphNode>()
      for (const n of ns) map.set(n.id, n)

      // --- Draw edges ---
      for (const edge of edges) {
        const src = map.get(edge.source)
        const tgt = map.get(edge.target)
        if (!src || !tgt) continue

        const sx = src.x ?? 0
        const sy = src.y ?? 0
        const tx = tgt.x ?? 0
        const ty = tgt.y ?? 0

        ctx.beginPath()
        ctx.moveTo(sx, sy)
        ctx.lineTo(tx, ty)
        ctx.strokeStyle = '#52525b' // zinc-600
        ctx.lineWidth = 1
        ctx.stroke()

        // Arrowhead
        const angle = Math.atan2(ty - sy, tx - sx)
        const tgtRadius = (tgt.size ?? 10) + 2
        const ax = tx - Math.cos(angle) * tgtRadius
        const ay = ty - Math.sin(angle) * tgtRadius
        ctx.beginPath()
        ctx.moveTo(ax, ay)
        ctx.lineTo(
          ax - ARROW_SIZE * Math.cos(angle - Math.PI / 6),
          ay - ARROW_SIZE * Math.sin(angle - Math.PI / 6)
        )
        ctx.lineTo(
          ax - ARROW_SIZE * Math.cos(angle + Math.PI / 6),
          ay - ARROW_SIZE * Math.sin(angle + Math.PI / 6)
        )
        ctx.closePath()
        ctx.fillStyle = '#52525b'
        ctx.fill()

        // Edge label at midpoint
        if (edge.label) {
          const mx = (sx + tx) / 2
          const my = (sy + ty) / 2
          ctx.font = '9px sans-serif'
          ctx.fillStyle = '#a1a1aa'
          ctx.textAlign = 'center'
          ctx.textBaseline = 'bottom'
          ctx.fillText(edge.label, mx, my - 3)
        }
      }

      // --- Draw nodes ---
      for (const n of ns) {
        const x = n.x ?? 0
        const y = n.y ?? 0
        const r = n.size ?? 10
        const color = colorForGroup(n.group)
        const isSelected = selectedNode?.id === n.id
        const isHovered = hoveredNode?.id === n.id

        // Glow for selected node
        if (isSelected) {
          ctx.beginPath()
          ctx.arc(x, y, r + 6, 0, Math.PI * 2)
          ctx.fillStyle = color + '44'
          ctx.fill()
          ctx.strokeStyle = color
          ctx.lineWidth = 2
          ctx.stroke()
        }

        // Highlight ring for hovered node
        if (isHovered && !isSelected) {
          ctx.beginPath()
          ctx.arc(x, y, r + 4, 0, Math.PI * 2)
          ctx.strokeStyle = color + '88'
          ctx.lineWidth = 2
          ctx.stroke()
        }

        // Node circle
        ctx.beginPath()
        ctx.arc(x, y, r, 0, Math.PI * 2)
        ctx.fillStyle = color
        ctx.fill()

        // Node label
        ctx.font = '10px sans-serif'
        ctx.fillStyle = '#e4e4e7' // zinc-200
        ctx.textAlign = 'center'
        ctx.textBaseline = 'top'
        ctx.fillText(n.label, x, y + r + 4, 100)
      }

      ctx.restore()
    }

    animFrameRef.current = requestAnimationFrame(tick)

    return () => {
      cancelAnimationFrame(animFrameRef.current)
    }
  }, [inputNodes, edges, width, height, zoom, pan, selectedNode, hoveredNode])

  // -------------------------------------------------------------------
  // Canvas-to-graph coordinate transform
  // -------------------------------------------------------------------
  const canvasToGraph = useCallback(
    (cx: number, cy: number) => ({
      x: (cx - pan.x) / zoom,
      y: (cy - pan.y) / zoom,
    }),
    [pan, zoom]
  )

  const findNodeAt = useCallback(
    (gx: number, gy: number): GraphNode | null => {
      // Search in reverse so top-drawn nodes are found first.
      const ns = nodesRef.current
      for (let i = ns.length - 1; i >= 0; i--) {
        const n = ns[i]
        const dx = gx - (n.x ?? 0)
        const dy = gy - (n.y ?? 0)
        const r = (n.size ?? 10) + 4
        if (dx * dx + dy * dy <= r * r) return n
      }
      return null
    },
    []
  )

  // -------------------------------------------------------------------
  // Mouse handlers
  // -------------------------------------------------------------------
  const handleMouseDown = useCallback(
    (e: React.MouseEvent<HTMLCanvasElement>) => {
      const rect = canvasRef.current?.getBoundingClientRect()
      if (!rect) return
      const cx = e.clientX - rect.left
      const cy = e.clientY - rect.top
      const { x: gx, y: gy } = canvasToGraph(cx, cy)
      const node = findNodeAt(gx, gy)

      lastMouseRef.current = { x: e.clientX, y: e.clientY }

      if (node) {
        dragNodeRef.current = node
        setSelectedNode(node)
        onNodeClick?.(node)
      } else {
        isPanningRef.current = true
        setSelectedNode(null)
      }
    },
    [canvasToGraph, findNodeAt, onNodeClick]
  )

  const handleMouseMove = useCallback(
    (e: React.MouseEvent<HTMLCanvasElement>) => {
      const rect = canvasRef.current?.getBoundingClientRect()
      if (!rect) return
      const cx = e.clientX - rect.left
      const cy = e.clientY - rect.top

      if (dragNodeRef.current) {
        const { x: gx, y: gy } = canvasToGraph(cx, cy)
        dragNodeRef.current.x = gx
        dragNodeRef.current.y = gy
      } else if (isPanningRef.current) {
        const dx = e.clientX - lastMouseRef.current.x
        const dy = e.clientY - lastMouseRef.current.y
        setPan((prev) => ({ x: prev.x + dx, y: prev.y + dy }))
      } else {
        // Hover detection
        const { x: gx, y: gy } = canvasToGraph(cx, cy)
        const node = findNodeAt(gx, gy)
        setHoveredNode(node)
        if (node) {
          setTooltipPos({ x: cx, y: cy })
        }
      }

      lastMouseRef.current = { x: e.clientX, y: e.clientY }
    },
    [canvasToGraph, findNodeAt]
  )

  const handleMouseUp = useCallback(() => {
    dragNodeRef.current = null
    isPanningRef.current = false
  }, [])

  const handleWheel = useCallback((e: React.WheelEvent<HTMLCanvasElement>) => {
    e.preventDefault()
    const delta = e.deltaY > 0 ? 0.9 : 1.1
    setZoom((prev) => Math.max(0.1, Math.min(5, prev * delta)))
  }, [])

  // -------------------------------------------------------------------
  // Render
  // -------------------------------------------------------------------
  return (
    <div className={`relative ${className}`} style={{ width, height }}>
      <canvas
        ref={canvasRef}
        width={width}
        height={height}
        style={{ cursor: dragNodeRef.current ? 'grabbing' : 'grab' }}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
        onWheel={handleWheel}
      />

      {/* Stats badge */}
      <div className="absolute top-2 left-2 bg-zinc-800/80 text-zinc-300 text-xs px-2 py-1 rounded font-mono">
        {inputNodes.length} nodes &middot; {edges.length} edges
      </div>

      {/* Zoom controls */}
      <div className="absolute top-2 right-2 flex flex-col gap-1">
        <button
          className="bg-zinc-800/80 text-zinc-300 w-7 h-7 rounded flex items-center justify-center hover:bg-zinc-700 text-sm"
          onClick={() => setZoom((z) => Math.min(5, z * 1.2))}
        >
          +
        </button>
        <button
          className="bg-zinc-800/80 text-zinc-300 w-7 h-7 rounded flex items-center justify-center hover:bg-zinc-700 text-sm"
          onClick={() => setZoom((z) => Math.max(0.1, z / 1.2))}
        >
          -
        </button>
        <button
          className="bg-zinc-800/80 text-zinc-300 w-7 h-7 rounded flex items-center justify-center hover:bg-zinc-700 text-xs"
          onClick={() => {
            setZoom(1)
            setPan({ x: 0, y: 0 })
          }}
        >
          R
        </button>
      </div>

      {/* Hover tooltip */}
      {hoveredNode && (
        <div
          className="absolute pointer-events-none bg-zinc-800 border border-zinc-700 rounded px-3 py-2 text-xs text-zinc-200 shadow-lg max-w-xs"
          style={{
            left: Math.min(tooltipPos.x + 12, width - 180),
            top: Math.min(tooltipPos.y + 12, height - 100),
          }}
        >
          <div className="font-semibold mb-1" style={{ color: colorForGroup(hoveredNode.group) }}>
            {hoveredNode.label}
          </div>
          <div className="text-zinc-400 mb-1">{hoveredNode.group}</div>
          {Object.entries(hoveredNode.properties)
            .slice(0, 6)
            .map(([k, v]) => (
              <div key={k} className="truncate">
                <span className="text-zinc-500">{k}:</span> {v}
              </div>
            ))}
          {Object.keys(hoveredNode.properties).length > 6 && (
            <div className="text-zinc-500 mt-1">
              +{Object.keys(hoveredNode.properties).length - 6} more
            </div>
          )}
        </div>
      )}

      {/* Legend */}
      <div className="absolute bottom-2 left-2 bg-zinc-800/80 rounded px-2 py-1 flex gap-3 text-xs text-zinc-400">
        {Object.entries(GROUP_COLORS)
          .filter(([k]) => k !== 'default')
          .map(([group, color]) => (
            <span key={group} className="flex items-center gap-1">
              <span
                className="inline-block w-2 h-2 rounded-full"
                style={{ backgroundColor: color }}
              />
              {group}
            </span>
          ))}
      </div>
    </div>
  )
}

export type { GraphNode, GraphEdge, GraphVisualizationProps }
