import { useEffect, useState } from 'react'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, LabelList,
} from 'recharts'
import { fetchFlashMeta, fetchFlashSummary } from './api'
import { useFlashStore } from './store'
import { FlashFilters } from './FlashFilters'
import type { FlashSummary } from './types'

const EMPTY: FlashSummary = { rows: [], totals: { puntos: 0, personas: 0 } }

export const FlashPage = () => {
  const { desde, hasta, granularity, comunas, turnos, topN, initFromMeta } = useFlashStore()
  const [data, setData] = useState<FlashSummary>(EMPTY)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [metaLoaded, setMetaLoaded] = useState(false)

  useEffect(() => {
    fetchFlashMeta()
      .then(meta => {
        initFromMeta(meta.date_min, meta.date_max, meta.comunas)
        setMetaLoaded(true)
      })
      .catch(e => setError(String(e)))
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!metaLoaded || !desde || !hasta || comunas.size === 0 || turnos.size === 0) return
    setLoading(true)
    fetchFlashSummary({
      desde, hasta, granularity,
      comunas: [...comunas],
      turnos: [...turnos],
      topN,
    })
      .then(d => { setData(d); setLoading(false) })
      .catch(e => { setError(String(e)); setLoading(false) })
  }, [metaLoaded, desde, hasta, granularity, comunas, turnos, topN])

  return (
    <div className="flex flex-col flex-1">
      <FlashFilters />

      {error && (
        <div className="px-6 py-4 text-red-500 text-sm">{error}</div>
      )}

      {!error && (
        <div className="p-6 flex flex-col gap-6">
          <div className="flex gap-4 flex-wrap">
            <StatCard label="Personas observadas (PSC)" value={data.totals.personas} color="bg-blue-600" />
            <StatCard label="Puntos relevados" value={data.totals.puntos} color="bg-sky-400" />
          </div>

          <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-5">
            <h2 className="text-gray-700 text-sm font-semibold mb-4">
              Evolución por {granularity === 'day' ? 'día' : granularity === 'week' ? 'semana' : 'mes'}
              {' '}· últimos {topN} períodos
            </h2>
            {loading ? (
              <div className="h-64 flex items-center justify-center text-gray-400">Cargando…</div>
            ) : data.rows.length === 0 ? (
              <div className="h-64 flex items-center justify-center text-gray-400">Sin datos para el filtro seleccionado</div>
            ) : (
              <ResponsiveContainer width="100%" height={320}>
                <BarChart data={data.rows} margin={{ top: 4, right: 16, left: 0, bottom: 48 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                  <XAxis
                    dataKey="bucket"
                    tick={{ fill: '#6b7280', fontSize: 11 }}
                    angle={-40}
                    textAnchor="end"
                    interval={0}
                  />
                  <YAxis tick={{ fill: '#6b7280', fontSize: 11 }} />
                  <Tooltip
                    contentStyle={{ background: '#fff', border: '1px solid #e5e7eb', borderRadius: 8 }}
                    labelStyle={{ color: '#111827', fontWeight: 600 }}
                  />
                  <Legend wrapperStyle={{ color: '#6b7280', fontSize: 12, paddingTop: 16 }} />
                  <Bar dataKey="personas" name="Personas (PSC)" fill="#2563eb" radius={[3, 3, 0, 0]}>
                    <LabelList dataKey="personas" position="top" style={{ fill: '#374151', fontSize: 10, fontWeight: 600 }} />
                  </Bar>
                  <Bar dataKey="puntos" name="Puntos" fill="#38bdf8" radius={[3, 3, 0, 0]}>
                    <LabelList dataKey="puntos" position="top" style={{ fill: '#374151', fontSize: 10, fontWeight: 600 }} />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>
      )}
    </div>
  )
}

const StatCard = ({
  label, value, color,
}: {
  label: string; value: number; color: string
}) => (
  <div className="bg-white rounded-xl border border-gray-200 shadow-sm px-5 py-4 flex items-center gap-3 min-w-[200px]">
    <div className={`w-2 h-10 rounded-full ${color}`} />
    <div>
      <div className="text-2xl font-bold text-gray-900">{value.toLocaleString('es-AR')}</div>
      <div className="text-xs text-gray-500 mt-0.5">{label}</div>
    </div>
  </div>
)
