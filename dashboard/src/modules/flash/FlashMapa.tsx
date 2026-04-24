import { useEffect, useState } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, GeoJSON } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'
import { fetchFlashPoints } from './api'
import { useFlashStore } from './store'
import { FlashFilters } from './FlashFilters'
import type { FlashPoint } from './types'

const CABA_CENTER: [number, number] = [-34.615, -58.443]

export const FlashMapa = () => {
  const { desde, hasta, comunas, turnos } = useFlashStore()
  const [points, setPoints]     = useState<FlashPoint[]>([])
  const [loading, setLoading]   = useState(false)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [geojson, setGeojson]   = useState<any>(null)

  useEffect(() => {
    fetch('/data/comunas.geojson')
      .then(r => r.ok ? r.json() : null)
      .then(g => g && setGeojson(g))
      .catch(() => null)
  }, [])

  useEffect(() => {
    if (!desde || !hasta || comunas.size === 0 || turnos.size === 0) return
    setLoading(true)
    fetchFlashPoints({ desde, hasta, comunas: [...comunas], turnos: [...turnos] })
      .then(data => { setPoints(data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [desde, hasta, comunas, turnos])

  const maxPersonas = Math.max(1, ...points.map(p => p.personas))
  const radius = (p: FlashPoint) => 4 + (p.personas / maxPersonas) * 16

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
              style={{ color: '#475569', weight: 1, fillOpacity: 0.05 }}
            />
          )}
          {points.map(p => (
            <CircleMarker
              key={p.id}
              center={[p.lat, p.lon]}
              radius={radius(p)}
              pathOptions={{ color: '#f97316', fillColor: '#f97316', fillOpacity: 0.6, weight: 1 }}
            >
              <Popup>
                <div className="text-xs">
                  <div><b>Personas:</b> {p.personas}</div>
                  <div><b>Turno:</b> {p.turno}</div>
                  <div><b>Fecha:</b> {p.fecha}</div>
                  {p.localizacion && <div><b>Localización:</b> {p.localizacion === 14.5 ? 'Palermo Norte' : `Comuna ${p.localizacion}`}</div>}
                </div>
              </Popup>
            </CircleMarker>
          ))}
        </MapContainer>
      </div>
    </div>
  )
}
