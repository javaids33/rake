import type { Config } from 'tailwindcss'

export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  theme: {
    extend: {
      colors: {
        rust: {
          50: '#fffbeb',
          100: '#fef3c7',
          200: '#fde68a',
          300: '#fcd34d',
          400: '#fbbf24',
          500: '#f59e0b',
          600: '#d97706',
          700: '#b45309',
          800: '#92400e',
          900: '#78350f',
          950: '#451a03',
        },
        navy: {
          950: '#050a12',
          900: '#0a1122',
          850: '#0d1730',
          800: '#111d3a',
          700: '#1a2b52',
          600: '#243b6a',
        },
        surface: {
          0: '#050a12',
          1: '#0a1122',
          2: '#0f1a33',
          3: '#142244',
          4: '#1a2b52',
          5: '#213562',
        },
        glow: {
          amber: 'rgba(251, 191, 36, 0.15)',
          cyan: 'rgba(34, 211, 238, 0.15)',
          rose: 'rgba(244, 63, 94, 0.15)',
          emerald: 'rgba(16, 185, 129, 0.15)',
          violet: 'rgba(139, 92, 246, 0.15)',
        },
      },
      fontFamily: {
        display: ['"Sora"', 'system-ui', 'sans-serif'],
        sans: ['"DM Sans"', 'system-ui', 'sans-serif'],
        mono: ['"JetBrains Mono"', '"Fira Code"', 'monospace'],
      },
      fontSize: {
        '2xs': ['0.625rem', { lineHeight: '0.875rem' }],
      },
      backgroundImage: {
        'grid-pattern': `radial-gradient(circle, rgba(251,191,36,0.07) 1px, transparent 1px)`,
        'gradient-radial': 'radial-gradient(var(--tw-gradient-stops))',
      },
      backgroundSize: {
        'grid': '24px 24px',
      },
      animation: {
        'fade-in': 'fadeIn 0.5s cubic-bezier(0.16, 1, 0.3, 1)',
        'slide-up': 'slideUp 0.5s cubic-bezier(0.16, 1, 0.3, 1)',
        'slide-in': 'slideIn 0.3s cubic-bezier(0.16, 1, 0.3, 1)',
        'glow-pulse': 'glowPulse 3s ease-in-out infinite',
        'float': 'float 6s ease-in-out infinite',
        'scan': 'scan 4s linear infinite',
        'count-up': 'countUp 0.8s cubic-bezier(0.16, 1, 0.3, 1)',
      },
      keyframes: {
        fadeIn: {
          '0%': { opacity: '0' },
          '100%': { opacity: '1' },
        },
        slideUp: {
          '0%': { opacity: '0', transform: 'translateY(12px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        slideIn: {
          '0%': { opacity: '0', transform: 'translateX(-8px)' },
          '100%': { opacity: '1', transform: 'translateX(0)' },
        },
        glowPulse: {
          '0%, 100%': { opacity: '0.4' },
          '50%': { opacity: '1' },
        },
        float: {
          '0%, 100%': { transform: 'translateY(0px)' },
          '50%': { transform: 'translateY(-6px)' },
        },
        scan: {
          '0%': { transform: 'translateY(-100%)' },
          '100%': { transform: 'translateY(100%)' },
        },
        countUp: {
          '0%': { opacity: '0', transform: 'translateY(8px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
      },
      boxShadow: {
        'glow-amber': '0 0 20px rgba(251,191,36,0.12), 0 0 60px rgba(251,191,36,0.06)',
        'glow-cyan': '0 0 20px rgba(34,211,238,0.12), 0 0 60px rgba(34,211,238,0.06)',
        'glow-rose': '0 0 20px rgba(244,63,94,0.12), 0 0 60px rgba(244,63,94,0.06)',
        'glow-emerald': '0 0 20px rgba(16,185,129,0.12), 0 0 60px rgba(16,185,129,0.06)',
        'glass': '0 8px 32px rgba(0,0,0,0.3), inset 0 1px 0 rgba(255,255,255,0.05)',
        'glass-hover': '0 12px 40px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.08)',
      },
    },
  },
  plugins: [],
} satisfies Config
