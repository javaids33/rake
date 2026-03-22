import { lazy, Suspense } from 'react'
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { Toaster } from 'react-hot-toast'
import { Shell } from './components/layout/Shell'
import { Home } from './pages/Home'

const SqlEditorPage = lazy(() => import('./pages/SqlEditorPage').then(m => ({ default: m.SqlEditorPage })))
const DataCatalog = lazy(() => import('./pages/DataCatalog').then(m => ({ default: m.DataCatalog })))
const DataSources = lazy(() => import('./pages/DataSources').then(m => ({ default: m.DataSources })))
const Streaming = lazy(() => import('./pages/Streaming').then(m => ({ default: m.Streaming })))
const VectorSearch = lazy(() => import('./pages/VectorSearch').then(m => ({ default: m.VectorSearch })))
const Transforms = lazy(() => import('./pages/Transforms').then(m => ({ default: m.Transforms })))
const Scheduler = lazy(() => import('./pages/Scheduler').then(m => ({ default: m.Scheduler })))
const Benchmarks = lazy(() => import('./pages/Benchmarks').then(m => ({ default: m.Benchmarks })))
const Migration = lazy(() => import('./pages/Migration').then(m => ({ default: m.Migration })))
const EngineMetrics = lazy(() => import('./pages/EngineMetrics').then(m => ({ default: m.EngineMetrics })))
const DataQuality = lazy(() => import('./pages/DataQuality').then(m => ({ default: m.DataQuality })))
const QueryHistory = lazy(() => import('./pages/QueryHistory').then(m => ({ default: m.QueryHistory })))
const Notebooks = lazy(() => import('./pages/Notebooks').then(m => ({ default: m.Notebooks })))
const Settings = lazy(() => import('./pages/Settings').then(m => ({ default: m.Settings })))
const WorkflowViz = lazy(() => import('./pages/WorkflowViz').then(m => ({ default: m.WorkflowViz })))
const ExecutableTables = lazy(() => import('./pages/ExecutableTables').then(m => ({ default: m.ExecutableTables })))
const DataProducts = lazy(() => import('./pages/DataProducts').then(m => ({ default: m.DataProducts })))
const About = lazy(() => import('./pages/About').then(m => ({ default: m.About })))

function PageLoader() {
  return (
    <div className="flex items-center justify-center h-full">
      <div className="w-6 h-6 border-2 border-rust-500/30 border-t-rust-500 rounded-full animate-spin" />
    </div>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<Shell />}>
          <Route index element={<Home />} />
          <Route path="sql" element={<Suspense fallback={<PageLoader />}><SqlEditorPage /></Suspense>} />
          <Route path="catalog" element={<Suspense fallback={<PageLoader />}><DataCatalog /></Suspense>} />
          <Route path="sources" element={<Suspense fallback={<PageLoader />}><DataSources /></Suspense>} />
          <Route path="streaming" element={<Suspense fallback={<PageLoader />}><Streaming /></Suspense>} />
          <Route path="vector" element={<Suspense fallback={<PageLoader />}><VectorSearch /></Suspense>} />
          <Route path="transforms" element={<Suspense fallback={<PageLoader />}><Transforms /></Suspense>} />
          <Route path="scheduler" element={<Suspense fallback={<PageLoader />}><Scheduler /></Suspense>} />
          <Route path="benchmarks" element={<Suspense fallback={<PageLoader />}><Benchmarks /></Suspense>} />
          <Route path="migration" element={<Suspense fallback={<PageLoader />}><Migration /></Suspense>} />
          <Route path="metrics" element={<Suspense fallback={<PageLoader />}><EngineMetrics /></Suspense>} />
          <Route path="quality" element={<Suspense fallback={<PageLoader />}><DataQuality /></Suspense>} />
          <Route path="notebooks" element={<Suspense fallback={<PageLoader />}><Notebooks /></Suspense>} />
          <Route path="history" element={<Suspense fallback={<PageLoader />}><QueryHistory /></Suspense>} />
          <Route path="workflow" element={<Suspense fallback={<PageLoader />}><WorkflowViz /></Suspense>} />
          <Route path="data-models" element={<Suspense fallback={<PageLoader />}><ExecutableTables /></Suspense>} />
          <Route path="executable-tables" element={<Navigate to="/data-models" replace />} />
          <Route path="data-products" element={<Suspense fallback={<PageLoader />}><DataProducts /></Suspense>} />
          <Route path="settings" element={<Suspense fallback={<PageLoader />}><Settings /></Suspense>} />
          <Route path="about" element={<Suspense fallback={<PageLoader />}><About /></Suspense>} />
        </Route>
      </Routes>
      <Toaster
        position="bottom-right"
        toastOptions={{
          style: {
            background: '#18181b',
            color: '#e4e4e7',
            border: '1px solid #27272a',
            fontSize: '13px',
            borderRadius: '10px',
          },
          success: { iconTheme: { primary: '#22c55e', secondary: '#18181b' } },
          error: { iconTheme: { primary: '#ef4444', secondary: '#18181b' } },
        }}
      />
    </BrowserRouter>
  )
}
