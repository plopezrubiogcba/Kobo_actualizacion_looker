import { useEffect, useMemo, useState } from 'react'
import { fetchControl } from '@/modules/flash/api'
import type { ControlData, ControlEstado, Turno } from '@/modules/flash/types'

const TURNOS: Turno[] = ['TM', 'TT', 'TN']
const TURNO_LABEL: Record<Turno, string> = { TM: 'Mañana', TT: 'Tarde', TN: 'Noche' }
const DUPLE = [...Array.from({ length: 19 }, (_, i) => i + 1), ...Array.from({ length: 13 }, (_, i) => i + 21)]

const ESTADO_BADGE: Record<ControlEstado, { cls: string; label: string }> = {
  ok:           { cls: 'bg-emerald-100 text-emerald-700 border-emerald-300', label: 'Cumplido' },
  falta_subir:  { cls: 'bg-red-100    text-red-700   border-red-300',       label: 'Falta subir' },
  sin_declarar: { cls: 'bg-blue-100   text-blue-700  border-blue-300',      label: 'Subió sin declarar' },
}

const isoToday = () => new Date().toISOString().slice(0, 10)

const arNow = () => {
  const d = new Date(Date.now() - 3 * 3600 * 1000)
  return { date: d.toISOString().slice(0, 10), hour: d.getUTCHours() }
}

const turnoIniciado = (t: Turno, fecha: string) => {
  const now = arNow()
  if (fecha < now.date) return true
  if (fecha > now.date) return false
  if (t === 'TM') return now.hour >= 6
  if (t === 'TT') return now.hour >= 14
  return now.hour >= 22 || now.hour < 6
}

const parseISODate = (s: string) => {
  const [y, m, d] = s.split('-').map(Number)
  return new Date(y, m - 1, d)
}

const fmt = (s: string) => parseISODate(s).toLocaleDateString('es-AR', { day: '2-digit', month: '2-digit', year: 'numeric' })

export const ControlPage = () => {
  const today = isoToday()
  const d = parseISODate(today)
  d.setDate(d.getDate() - 13)
  const [desde, setDesde] = useState(d.toISOString().slice(0, 10))
  const [hasta, setHasta] = useState(today)
  const [turnos, setTurnos] = useState<Set<Turno>>(new Set(TURNOS))
  const [duplaSel, setDuplaSel] = useState('__all__')
  const [soloAlertas, setSoloAlertas] = useState(false)
  const [ocultarFuturos, setOcultarFuturos] = useState(true)

  const [data, setData] = useState<ControlData>({ rows: [], sin_dupla: 0 })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!desde || !hasta) return
    const duplas = duplaSel === '__all__' ? [] : [Number(duplaSel)]
    setLoading(true)
    fetchControl({ desde, hasta, duplas, turnos: [...turnos] })
      .then(dd => { setData(dd); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [desde, hasta, duplaSel, turnos])

  const stats = useMemo(() => {
    let declarado = 0
    let verificado = 0
    let faltantes = 0
    let sinDeclarar = 0
    let alertas = 0
    for (const r of data.rows) {
      declarado += r.sheet
      verificado += r.kobo
      const falta = r.sheet - r.kobo
      const extra = r.kobo - r.sheet
      if (falta > 0) { faltantes += falta; alertas += 1 }
      if (extra > 0) { sinDeclarar += extra; alertas += 1 }
    }
    const pct = declarado > 0 ? Math.min(100, Math.round((verificado / declarado) * 100)) : null
    return { declarado, verificado, faltantes, sinDeclarar, alertas, pct }
  }, [data.rows])

  const visibleRows = useMemo(() => {
    const rows = soloAlertas ? data.rows.filter(r => r.estado !== 'ok') : data.rows
    let filtered = rows
    if (ocultarFuturos) {
      filtered = rows.filter(r => {
        if (r.estado !== 'falta_subir') return true
        const decl = r.turnos_declarados
        if (decl.length === 0) return true
        return decl.some(t => turnoIniciado(t, r.fecha))
      })
    }
    return [...filtered].sort((a, b) =>
      a.fecha.localeCompare(b.fecha)
      || (a.dupla - b.dupla)
    )
  }, [data.rows, soloAlertas, ocultarFuturos])

  const toggleTurno = (t: Turno) => {
    const next = new Set(turnos)
    next.has(t) ? next.delete(t) : next.add(t)
    setTurnos(next)
  }

  return (
    <div className="flex flex-col flex-1">
      <div className="bg-white border-b border-gray-200 px-6 py-4 flex flex-wrap gap-4 items-end text-sm shadow-sm">
        <div className="flex gap-2 items-center">
          <label className="text-gray-500 text-xs font-medium">Desde</label>
          <input type="date" value={desde} onChange={e => setDesde(e.target.value)}
            className="border border-gray-300 rounded-lg px-2 py-1.5 text-gray-800 text-xs focus:outline-none focus:ring-2 focus:ring-blue-400" />
          <label className="text-gray-500 text-xs font-medium">Hasta</label>
          <input type="date" value={hasta} onChange={e => setHasta(e.target.value)}
            className="border border-gray-300 rounded-lg px-2 py-1.5 text-gray-800 text-xs focus:outline-none focus:ring-2 focus:ring-blue-400" />
        </div>

        <div className="flex gap-1 items-center flex-wrap">
          <span className="text-gray-500 text-xs font-medium mr-1">Turno</span>
          {TURNOS.map(t => (
            <button key={t} onClick={() => toggleTurno(t)}
              className={`px-2.5 py-1.5 rounded-lg text-xs font-medium transition-colors ${
                turnos.has(t) ? 'bg-yellow-400 text-blue-900 shadow-sm' : 'bg-gray-100 text-gray-500 hover:bg-gray-200'
              }`}>
              {t} <span className="hidden sm:inline opacity-70">({TURNO_LABEL[t]})</span>
            </button>
          ))}
        </div>

        <div className="flex gap-2 items-center">
          <span className="text-gray-500 text-xs font-medium">Dupla</span>
          <select value={duplaSel} onChange={e => setDuplaSel(e.target.value)}
            className="border border-gray-300 rounded-lg px-2 py-1.5 text-gray-800 text-xs bg-white focus:outline-none focus:ring-2 focus:ring-blue-400 min-w-[120px]">
            <option value="__all__">Todas</option>
            {DUPLE.map(n => <option key={n} value={n}>Dupla {n}</option>)}
          </select>
        </div>

        <label className="flex gap-2 items-center cursor-pointer select-none text-gray-600 text-xs font-medium">
          <input type="checkbox" checked={soloAlertas} onChange={e => setSoloAlertas(e.target.checked)}
            className="h-4 w-4 accent-red-500" />
          Solo alertas
        </label>

        <label className="flex gap-2 items-center cursor-pointer select-none text-gray-600 text-xs font-medium">
          <input type="checkbox" checked={ocultarFuturos} onChange={e => setOcultarFuturos(e.target.checked)}
            className="h-4 w-4 accent-blue-500" />
          Ocultar turnos no iniciados
        </label>
      </div>

      {error && <div className="px-6 py-4 text-red-500 text-sm">{error}</div>}

      {!error && (
        <div className="p-6 flex flex-col gap-6">
          <div className="flex gap-4 flex-wrap">
            <StatCard label="Declarado en sheet" value={stats.declarado} color="bg-sky-400" />
            <StatCard label="Verificado en Kobo" value={stats.verificado} color="bg-blue-600" />
            <StatCard
              label={stats.pct === null ? 'Cumplimiento' : `${stats.pct}% del declarado verificado`}
              value={stats.pct === null ? '—' : `${stats.pct}%`}
              color={stats.pct !== null && stats.pct < 100 ? 'bg-red-500' : 'bg-emerald-500'}
              big={false}
            />
            <StatCard label="Faltan en Kobo" value={stats.faltantes} color="bg-red-500" />
            <StatCard label="Subidos sin declarar" value={stats.sinDeclarar} color="bg-blue-400" />
            <StatCard label="Salidas con alerta" value={stats.alertas} color="bg-amber-400" />
            <StatCard label="Puntos sin dupla (GPS)" value={data.sin_dupla} color="bg-gray-400" />
          </div>

          <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
            <h2 className="text-gray-700 text-sm font-semibold mb-1">
              Registros por salida · qué declararon vs qué subieron
            </h2>
            <p className="text-xs text-gray-400 mb-4">
              Cada fila = una dupla en un turno. «Declarado» es lo que anotaron en el sheet; «Verificado» es lo que aparece en Kobo.
            </p>
            {loading ? (
              <div className="h-40 flex items-center justify-center text-gray-400">Cargando…</div>
            ) : visibleRows.length === 0 ? (
              <div className="h-40 flex items-center justify-center text-gray-400">
                {soloAlertas ? 'Sin alertas en el período. Todo cumplido.' : 'Sin datos para el filtro seleccionado'}
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-gray-500 text-xs border-b border-gray-200">
                      <th className="py-2 pr-4">Fecha</th>
                      <th className="py-2 pr-4">Turno</th>
                      <th className="py-2 pr-4">Dupla</th>
                      <th className="py-2 pr-6 text-right">Declarado</th>
                      <th className="py-2 pr-4 text-right">Verificado</th>
                      <th className="py-2 pr-4 w-[30%]">Cobertura</th>
                      <th className="py-2 pr-4">Estado</th>
                      <th className="py-2 pr-4">Recorrido</th>
                    </tr>
                  </thead>
                  <tbody>
                    {visibleRows.map(r => {
                      const badge = ESTADO_BADGE[r.estado]
                      const falta = r.sheet - r.kobo
                      return (
                        <tr key={`${r.fecha}-${r.dupla}`} className={`border-b border-gray-100 last:border-0 ${r.estado === 'ok' ? '' : 'bg-red-50/40'}`}>
                          <td className="py-2.5 pr-4 text-gray-700 whitespace-nowrap">{fmt(r.fecha)}</td>
                          <td className="py-2.5 pr-4 text-gray-700">{r.turnos.length > 0 ? r.turnos.join(' · ') : '—'}</td>
                          <td className="py-2.5 pr-4 font-semibold text-gray-800">Dupla {r.dupla}</td>
                          <td className="py-2.5 pr-6 text-right text-gray-800 font-medium">{r.sheet.toLocaleString('es-AR')}</td>
                          <td className="py-2.5 pr-4 text-right text-gray-700">{r.kobo.toLocaleString('es-AR')}</td>
                          <td className="py-2.5 pr-4">
                            <div className="flex items-center gap-2">
                              <div className="flex-1 h-2.5 bg-gray-100 rounded-full overflow-hidden">
                                <div
                                  className={`h-full rounded-full ${r.estado === 'ok' ? 'bg-emerald-500' : 'bg-red-400'}`}
                                  style={{ width: `${Math.min(100, (r.kobo / Math.max(1, r.sheet)) * 100)}%` }}
                                />
                              </div>
                              <span className="text-xs text-gray-400 w-10 text-right">
                                {r.sheet > 0 ? `${Math.min(100, Math.round((r.kobo / r.sheet) * 100))}%` : '—'}
                              </span>
                            </div>
                          </td>
                          <td className="py-2.5 pr-4">
                            <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-lg text-xs font-semibold border ${badge.cls}`}>
                              {r.estado === 'falta_subir' && <span>{badge.label} {falta > 0 && <span className="font-bold">{falta}</span>}</span>}
                              {r.estado !== 'falta_subir' && badge.label}
                            </span>
                          </td>
                          <td className="py-2.5 pr-4">
                            {r.fotos.length === 0 ? (
                              <span className="text-gray-300">—</span>
                            ) : (
                              <div className="flex flex-wrap gap-1.5">
                                {r.fotos.map((f, i) => (
                                  <a
                                    key={i}
                                    href={f.url}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="inline-flex items-center gap-1 px-2 py-0.5 rounded-md text-xs text-blue-600 border border-blue-200 bg-blue-50 hover:bg-blue-100 transition-colors"
                                  >
                                    <svg className="w-3 h-3" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                                      <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z" />
                                      <circle cx="12" cy="10" r="3" />
                                    </svg>
                                    {r.fotos.length > 1 && f.turno ? `Recorrido ${f.turno}` : 'Recorrido'}
                                  </a>
                                ))}
                              </div>
                            )}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const StatCard = ({ label, value, color, big = true }: { label: string; value: string | number; color: string; big?: boolean }) => (
  <div className="bg-white rounded-xl border border-gray-200 shadow-sm px-5 py-4 flex items-center gap-3 min-w-[180px]">
    <div className={`w-2 h-10 rounded-full ${color}`} />
    <div>
      <div className={`font-bold text-gray-900 ${big ? 'text-2xl' : 'text-xl'}`}>{value}</div>
      <div className="text-xs text-gray-500 mt-0.5">{label}</div>
    </div>
  </div>
)
