import { useFlashStore } from './store'
import type { Granularity, Turno } from './types'

const TURNOS: Turno[] = ['TM', 'TT', 'TN']
const TURNO_LABEL: Record<Turno, string> = { TM: 'Madrugada', TT: 'Tarde', TN: 'Noche' }

const ZONA_LABEL: Record<string, string> = {
  C2:   'C2 — Recoleta',
  C14:  'C14 — Palermo',
  C13:  'C13 — Belgrano',
  C12:  'C12',
  C1A:  'C1A — Retiro/Recoleta N',
  C6:   'C6 — Caballito',
  Otro: 'Sin zona',
}

const ZONE_ORDER = ['C2', 'C14', 'C13', 'C12', 'C1A', 'C6', 'Otro']

export const FlashFilters = () => {
  const {
    desde, setDesde, hasta, setHasta,
    granularity, setGranularity,
    zonas, setZonas, allZonas,
    turnos, setTurnos,
    topN, setTopN,
  } = useFlashStore()

  const toggleTurno = (t: Turno) => {
    const next = new Set(turnos)
    next.has(t) ? next.delete(t) : next.add(t)
    setTurnos(next)
  }

  const GRANS: Granularity[] = ['day', 'week', 'month']
  const GRAN_LABEL: Record<Granularity, string> = { day: 'Día', week: 'Semana', month: 'Mes' }

  const sortedZonas = [...allZonas].sort((a, b) => {
    const ia = ZONE_ORDER.indexOf(a), ib = ZONE_ORDER.indexOf(b)
    return (ia === -1 ? 99 : ia) - (ib === -1 ? 99 : ib)
  })

  const allSelected  = sortedZonas.every(z => zonas.has(z))
  const noneSelected = zonas.size === 0

  const handleZonaChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const val = e.target.value
    if (val === '__all__') {
      setZonas(new Set(sortedZonas))
    } else {
      setZonas(new Set([val]))
    }
  }

  const dropdownValue =
    allSelected || noneSelected
      ? '__all__'
      : zonas.size === 1
        ? [...zonas][0]
        : '__multi__'

  return (
    <div className="bg-white border-b border-gray-200 px-6 py-4 flex flex-wrap gap-4 items-end text-sm shadow-sm">

      <div className="flex gap-2 items-center">
        <label className="text-gray-500 text-xs font-medium">Desde</label>
        <input
          type="date" value={desde}
          onChange={e => setDesde(e.target.value)}
          className="border border-gray-300 rounded-lg px-2 py-1.5 text-gray-800 text-xs focus:outline-none focus:ring-2 focus:ring-blue-400"
        />
        <label className="text-gray-500 text-xs font-medium">Hasta</label>
        <input
          type="date" value={hasta}
          onChange={e => setHasta(e.target.value)}
          className="border border-gray-300 rounded-lg px-2 py-1.5 text-gray-800 text-xs focus:outline-none focus:ring-2 focus:ring-blue-400"
        />
      </div>

      <div className="flex gap-1 items-center">
        <span className="text-gray-500 text-xs font-medium mr-1">Ver por</span>
        {GRANS.map(g => (
          <button
            key={g}
            onClick={() => setGranularity(g)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
              granularity === g
                ? 'bg-blue-600 text-white shadow-sm'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {GRAN_LABEL[g]}
          </button>
        ))}
      </div>

      <div className="flex gap-1 items-center">
        <span className="text-gray-500 text-xs font-medium">Períodos</span>
        <input
          type="number" min={1} max={52} value={topN}
          onChange={e => setTopN(Number(e.target.value))}
          className="border border-gray-300 rounded-lg px-2 py-1.5 w-16 text-gray-800 text-xs focus:outline-none focus:ring-2 focus:ring-blue-400"
        />
      </div>

      <div className="flex gap-1 items-center flex-wrap">
        <span className="text-gray-500 text-xs font-medium mr-1">Turno</span>
        {TURNOS.map(t => (
          <button
            key={t}
            onClick={() => toggleTurno(t)}
            className={`px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors ${
              turnos.has(t)
                ? 'bg-yellow-400 text-blue-900 shadow-sm'
                : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
            }`}
          >
            {t} <span className="hidden sm:inline opacity-70">({TURNO_LABEL[t]})</span>
          </button>
        ))}
      </div>

      {allZonas.length > 0 && (
        <div className="flex gap-2 items-center">
          <span className="text-gray-500 text-xs font-medium">Zona</span>
          <select
            value={dropdownValue}
            onChange={handleZonaChange}
            className="border border-gray-300 rounded-lg px-2 py-1.5 text-gray-800 text-xs bg-white focus:outline-none focus:ring-2 focus:ring-blue-400 min-w-[150px]"
          >
            <option value="__all__">Todas</option>
            {dropdownValue === '__multi__' && (
              <option value="__multi__" disabled>Varias seleccionadas</option>
            )}
            {sortedZonas.map(z => (
              <option key={z} value={z}>
                {ZONA_LABEL[z] ?? z}
              </option>
            ))}
          </select>
        </div>
      )}
    </div>
  )
}
