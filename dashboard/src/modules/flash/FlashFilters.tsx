import { useFlashStore } from './store'
import type { Granularity, Turno } from './types'

const TURNOS: Turno[] = ['TM', 'TT', 'TN']
const TURNO_LABEL: Record<Turno, string> = { TM: 'Madrugada', TT: 'Tarde', TN: 'Noche' }

export const FlashFilters = () => {
  const {
    desde, setDesde, hasta, setHasta,
    granularity, setGranularity,
    comunas, setComunas, allComunas,
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

  const allSelected   = allComunas.every(c => comunas.has(c))
  const noneSelected  = comunas.size === 0

  const handleComunaChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const val = e.target.value
    if (val === '__all__') {
      setComunas(new Set(allComunas))
    } else {
      setComunas(new Set([Number(val)]))
    }
  }

  const dropdownValue =
    allSelected || noneSelected
      ? '__all__'
      : comunas.size === 1
        ? String([...comunas][0])
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

      {allComunas.length > 0 && (
        <div className="flex gap-2 items-center">
          <span className="text-gray-500 text-xs font-medium">Comuna</span>
          <select
            value={dropdownValue}
            onChange={handleComunaChange}
            className="border border-gray-300 rounded-lg px-2 py-1.5 text-gray-800 text-xs bg-white focus:outline-none focus:ring-2 focus:ring-blue-400 min-w-[120px]"
          >
            <option value="__all__">Todas</option>
            {dropdownValue === '__multi__' && (
              <option value="__multi__" disabled>Varias seleccionadas</option>
            )}
            {allComunas.map(c => (
              <option key={c} value={String(c)}>
                {c === 14.5 ? 'Palermo Norte' : `Comuna ${c}`}
              </option>
            ))}
          </select>
        </div>
      )}
    </div>
  )
}
