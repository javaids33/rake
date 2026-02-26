import { Outlet } from 'react-router-dom'
import { Sidebar } from './Sidebar'
import { Header } from './Header'

export function Shell() {
  return (
    <div className="flex h-screen w-screen overflow-hidden bg-navy-950 atmosphere noise">
      {/* Ambient orbs */}
      <div className="fixed inset-0 pointer-events-none z-0" aria-hidden>
        <div className="absolute top-[-20%] left-[-10%] w-[600px] h-[600px] rounded-full bg-amber-400/[0.02] blur-[120px]" />
        <div className="absolute bottom-[-20%] right-[-10%] w-[500px] h-[500px] rounded-full bg-cyan-400/[0.02] blur-[100px]" />
        <div className="absolute top-[40%] right-[20%] w-[300px] h-[300px] rounded-full bg-violet-400/[0.015] blur-[80px]" />
      </div>

      {/* Dot grid overlay */}
      <div className="fixed inset-0 dot-grid pointer-events-none z-0" aria-hidden />

      <Sidebar />
      <div className="flex flex-col flex-1 min-w-0 relative z-10">
        <Header />
        <main className="flex-1 overflow-auto">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
