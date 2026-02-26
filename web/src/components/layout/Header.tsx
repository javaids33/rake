import { useEffect, useState } from 'react'
import { getHealth } from '../../api/client'
import { Activity, Zap, Hexagon } from 'lucide-react'

export function Header() {
  const [healthy, setHealthy] = useState<boolean | null>(null)
  const [latency, setLatency] = useState<number | null>(null)

  useEffect(() => {
    const check = async () => {
      const start = performance.now()
      try {
        await getHealth()
        setLatency(Math.round(performance.now() - start))
        setHealthy(true)
      } catch {
        setHealthy(false)
        setLatency(null)
      }
    }
    check()
    const id = setInterval(check, 15000)
    return () => clearInterval(id)
  }, [])

  return (
    <header className="h-11 flex items-center justify-between px-5 border-b border-amber-500/[0.04] bg-navy-950/50 backdrop-blur-md relative z-10">
      {/* Bottom glow line */}
      <div className="absolute bottom-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-amber-400/8 to-transparent" />

      <div className="flex items-center gap-4">
        <div className="flex items-center gap-2 text-2xs text-zinc-500">
          <Hexagon className="w-3 h-3 text-amber-400/40" />
          <span className="font-mono tracking-wider text-zinc-400">DATAFUSION <span className="text-amber-400/60">51</span></span>
          <span className="text-amber-400/20">|</span>
          <span className="font-mono tracking-wider text-zinc-400">ARROW <span className="text-amber-400/60">57</span></span>
        </div>
      </div>

      <div className="flex items-center gap-5">
        {latency !== null && (
          <div className="flex items-center gap-1.5 text-2xs text-zinc-600">
            <Zap className="w-3 h-3 text-amber-400/40" />
            <span className="font-mono readout">{latency}ms</span>
          </div>
        )}

        <div className="flex items-center gap-2">
          {/* Status indicator */}
          <div className="relative">
            <div className={`w-2 h-2 rounded-full ${
              healthy === null ? 'bg-zinc-500' : healthy ? 'bg-emerald-400' : 'bg-rose-400'
            }`} />
            {healthy && (
              <>
                <div className="absolute inset-0 rounded-full bg-emerald-400 animate-ping opacity-30" />
                <div className="absolute -inset-1 rounded-full bg-emerald-400/10 blur-sm" />
              </>
            )}
          </div>
          <span className={`text-2xs font-medium tracking-wide ${
            healthy === null ? 'text-zinc-500' : healthy ? 'text-emerald-400/80' : 'text-rose-400'
          }`}>
            {healthy === null ? 'CHECKING' : healthy ? 'ENGINE ONLINE' : 'OFFLINE'}
          </span>
        </div>
      </div>
    </header>
  )
}
