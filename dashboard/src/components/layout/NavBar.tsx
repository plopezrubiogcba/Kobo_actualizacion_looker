import { NavLink } from 'react-router-dom'

const cls = ({ isActive }: { isActive: boolean }) =>
  `text-sm px-4 py-1.5 rounded font-medium transition-colors ${isActive
    ? 'bg-yellow-400 text-blue-900'
    : 'text-blue-100 hover:bg-blue-600'
  }`

export const NavBar = () => (
  <header className="bg-blue-700 shadow-md">
    <div className="px-6 py-3 flex items-center gap-6">
      <img src="/logo.png" alt="KOBO" className="h-8 w-auto" />
      <span className="text-yellow-400 font-bold text-base tracking-wide">KOBO Flash</span>
      <nav className="flex gap-1">
        <NavLink to="/" className={cls} end>Dashboard</NavLink>
        <NavLink to="/mapa" className={cls}>Mapa</NavLink>
      </nav>
      <a
        href="https://kobo-omega.vercel.app/"
        className="ml-auto bg-yellow-400 hover:bg-yellow-300 text-blue-900 font-bold text-sm px-4 py-1.5 rounded-lg shadow transition-colors"
      >
        KOBO Intervenciones ↗
      </a>
    </div>
  </header>
)
