import {
  BarChart, Bar, LineChart, Line, ScatterChart, Scatter, PieChart, Pie, Cell,
  AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import type { ChartType } from '../../types'

interface QueryChartProps {
  type: ChartType
  columns: string[]
  rows: Record<string, unknown>[]
}

const COLORS = ['#fbbf24', '#22d3ee', '#10b981', '#f43f5e', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16']

const tooltipStyle = {
  contentStyle: { background: '#0d1730', border: '1px solid rgba(251,191,36,0.1)', borderRadius: 10, fontSize: 12, backdropFilter: 'blur(12px)' },
  itemStyle: { color: '#94a3b8' },
  labelStyle: { color: '#e2e8f0', fontWeight: 600 },
}

export function QueryChart({ type, columns, rows }: QueryChartProps) {
  if (!columns.length || !rows.length) return null

  const labelCol = columns[0]
  const valueCols = columns.slice(1).filter(c => rows.some(r => typeof r[c] === 'number'))
  if (!valueCols.length && type !== 'pie') return <div className="text-xs text-zinc-600 p-4">No numeric columns to chart</div>

  const data = rows.slice(0, 100).map(r => {
    const entry: Record<string, unknown> = { [labelCol]: r[labelCol] }
    valueCols.forEach(c => { entry[c] = Number(r[c]) || 0 })
    return entry
  })

  const common = { data, margin: { top: 10, right: 20, bottom: 10, left: 10 } }
  const axisStyle = { fontSize: 10, fill: '#475569' }
  const gridStroke = 'rgba(251,191,36,0.04)'

  switch (type) {
    case 'bar':
      return (
        <ResponsiveContainer width="100%" height={320}>
          <BarChart {...common}>
            <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
            <XAxis dataKey={labelCol} tick={axisStyle} />
            <YAxis tick={axisStyle} />
            <Tooltip {...tooltipStyle} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
            {valueCols.map((c, i) => (
              <Bar key={c} dataKey={c} fill={COLORS[i % COLORS.length]} radius={[4, 4, 0, 0]} fillOpacity={0.8} />
            ))}
          </BarChart>
        </ResponsiveContainer>
      )
    case 'line':
      return (
        <ResponsiveContainer width="100%" height={320}>
          <LineChart {...common}>
            <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
            <XAxis dataKey={labelCol} tick={axisStyle} />
            <YAxis tick={axisStyle} />
            <Tooltip {...tooltipStyle} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
            {valueCols.map((c, i) => (
              <Line key={c} type="monotone" dataKey={c} stroke={COLORS[i % COLORS.length]} strokeWidth={2} dot={false} />
            ))}
          </LineChart>
        </ResponsiveContainer>
      )
    case 'area':
      return (
        <ResponsiveContainer width="100%" height={320}>
          <AreaChart {...common}>
            <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
            <XAxis dataKey={labelCol} tick={axisStyle} />
            <YAxis tick={axisStyle} />
            <Tooltip {...tooltipStyle} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
            {valueCols.map((c, i) => (
              <Area key={c} type="monotone" dataKey={c} fill={COLORS[i % COLORS.length]} fillOpacity={0.1} stroke={COLORS[i % COLORS.length]} strokeWidth={2} />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      )
    case 'scatter':
      return (
        <ResponsiveContainer width="100%" height={320}>
          <ScatterChart {...common}>
            <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} />
            <XAxis dataKey={valueCols[0]} name={valueCols[0]} tick={axisStyle} />
            <YAxis dataKey={valueCols[1] || valueCols[0]} name={valueCols[1] || valueCols[0]} tick={axisStyle} />
            <Tooltip {...tooltipStyle} />
            <Scatter data={data} fill="#fbbf24" fillOpacity={0.7} />
          </ScatterChart>
        </ResponsiveContainer>
      )
    case 'pie':
      return (
        <ResponsiveContainer width="100%" height={320}>
          <PieChart>
            <Pie data={data} dataKey={valueCols[0] || labelCol} nameKey={labelCol} cx="50%" cy="50%" outerRadius={120} strokeWidth={2} stroke="#050a12">
              {data.map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} fillOpacity={0.8} />)}
            </Pie>
            <Tooltip {...tooltipStyle} />
            <Legend wrapperStyle={{ fontSize: 11, color: '#94a3b8' }} />
          </PieChart>
        </ResponsiveContainer>
      )
    default:
      return null
  }
}
