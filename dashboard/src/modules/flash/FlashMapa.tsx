import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, GeoJSON } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { fetchFlashPoints } from './api'
import { useFlashStore } from './store'
import { FlashFilters } from './FlashFilters'
import type { FlashPoint } from './types'

const CABA_CENTER: [number, number] = [-34.615, -58.443]

const ZONA_COLOR: Record<string, string> = {
  C2:   '#f97316',
  C14:  '#3b82f6',
  C13:  '#22c55e',
  C12:  '#a855f7',
  C1A:  '#eab308',
  C6:   '#ec4899',
  Otro: '#94a3b8',
}

const ZONA_LABEL: Record<string, string> = {
  C2:   'C2 — Recoleta',
  C14:  'C14 — Palermo',
  C13:  'C13 — Belgrano',
  C12:  'C12',
  C1A:  'C1A — Retiro/Recoleta N',
  C6:   'C6 — Caballito',
  Otro: 'Sin zona',
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const zoneStyle = (feature: any) => {
  const zona = feature?.properties?.zona ?? ''
  const color = ZONA_COLOR[zona] ?? '#475569'
  return { color, weight: 1.5, fillColor: color, fillOpacity: 0.08 }
}

export const FlashMapa = () => {
  const { desde, hasta, zonas, turnos } = useFlashStore()
  const [points, setPoints]     = useState<FlashPoint[]>([])
  const [loading, setLoading]   = useState(false)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [geojson, setGeojson]   = useState<any>(null)

  useEffect(() => {
    fetch('/data/mapa_flash.geojson')
      .then(r => r.ok ? r.json() : null)
      .then(g => g && setGeojson(g))
      .catch(() => null)
  }, [])

  useEffect(() => {
    if (!desde || !hasta || zonas.size === 0 || turnos.size === 0) return
    setLoading(true)
    fetchFlashPoints({ desde, hasta, zonas: [...zonas], turnos: [...turnos] })
      .then(data => { setPoints(data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [desde, hasta, zonas, turnos])

  const maxPersonas = Math.max(1, ...points.map(p => p.personas))
  const radius = (p: FlashPoint) => 4 + (p.personas / maxPersonas) * 16
  const pointColor = (p: FlashPoint) => ZONA_COLOR[p.localizacion ?? ''] ?? '#f97316'

  return (
    <div className="flex flex-col flex-1">
      <FlashFilters />
      {loading && (
        <div className="text-gray-500 text-sm px-6 py-2">Cargando puntos…</div>
      )}
      <div className="flex-1" style={{ minHeight: '500px' }}>
        <MapContainer
          center={CABA_CENTER}
          zoom={12}
          style={{ height: '100%', width: '100%', minHeight: '500px' }}
          className="bg-slate-900"
        >
          <TileLayer
            url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            attribution='&copy; <a href="https://carto.com/">CARTO</a>'
          />
          {geojson && (
            <GeoJSON
              data={geojson}
              style={zoneStyle}
            />
          )}
          {points.map(p => (
            <CircleMarker
              key={p.id}
              center={[p.lat, p.lon]}
              radius={radius(p)}
              pathOptions={{ color: pointColor(p), fillColor: pointColor(p), fillOpacity: 0.7, weight: 1 }}
            >
              <Popup>
                <div className="text-xs">
                  <div><b>Personas:</b> {p.personas}</div>
                  <div><b>Turno:</b> {p.turno}</div>
                  <div><b>Fecha:</b> {p.fecha}</div>
                  <div><b>Zona:</b> {p.localizacion ? (ZONA_LABEL[p.localizacion] ?? p.localizacion) : '—'}</div>
                </div>
              </Popup>
            </CircleMarker>
          ))}
        </MapContainer>
      </div>
    </div>
  )
}
