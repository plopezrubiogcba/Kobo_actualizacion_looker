import { useEffect, useMemo, useState } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, GeoJSON, Marker } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import { fetchFlashPoints } from './api'
import { useFlashStore } from './store'
import { FlashFilters } from './FlashFilters'
import type { FlashPoint } from './types'

const CABA_CENTER: [number, number] = [-34.615, -58.443]

const ZONA_COLOR: Record<string, string> = {
  'Frontera Norte': '#ef4444',
  'Frontera Sur-este': '#dc2626',
  C2: '#f97316',
  C14: '#3b82f6',
  C13: '#22c55e',
  C12: '#a855f7',
  C1A: '#eab308',
  'C15 Centro': '#06b6d4',
  'C5 Centro': '#ec4899',
  'C3 Centro': '#84cc16',
  'C6 Centro': '#f59e0b',
  Otro: '#94a3b8',
}

const ZONA_LABEL: Record<string, string> = {
  'Frontera Norte': 'Zona de Frontera Norte',
  'Frontera Sur-este': 'Zona de Frontera Sur',
  C2: 'C2 — Recoleta',
  C14: 'C14 — Palermo',
  C13: 'C13 — Belgrano',
  C12: 'C12',
  C1A: 'C1A — Retiro/Recoleta N',
  'C15 Centro': 'C15 Centro',
  'C5 Centro': 'C5 Centro',
  'C3 Centro': 'C3 Centro',
  'C6 Centro': 'C6 Centro — Caballito',
  Otro: 'Sin zona',
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const zoneStyle = (feature: any) => {
  const zona = feature?.properties?.zona ?? ''
  const color = ZONA_COLOR[zona] ?? '#475569'
  return { color, weight: 2.5, opacity: 0.9, fillColor: color, fillOpacity: 0.18 }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const featureCentroid = (feature: any): [number, number] | null => {
  const geom = feature?.geometry
  if (!geom) return null
  const collect = (coords: unknown): [number, number][] => {
    if (!Array.isArray(coords)) return []
    if (typeof coords[0] === 'number') return [[coords[1] as number, coords[0] as number]]
    return coords.flatMap(c => collect(c))
  }
  const pts = collect(geom.coordinates)
  if (pts.length === 0) return null
  const lat = pts.reduce((s, p) => s + p[0], 0) / pts.length
  const lon = pts.reduce((s, p) => s + p[1], 0) / pts.length
  return [lat, lon]
}

type ZoneStat = { zona: string; lat: number; lon: number; puntos: number; personas: number; color: string }

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

  const zoneStats: ZoneStat[] = useMemo(() => {
    if (!geojson?.features) return []
    const agg = new Map<string, { puntos: number; personas: number }>()
    for (const p of points) {
      const z = p.localizacion ?? 'Otro'
      const cur = agg.get(z) ?? { puntos: 0, personas: 0 }
      cur.puntos += 1
      cur.personas += p.personas || 0
      agg.set(z, cur)
    }
    const byZone = new Map<string, { lat: number; lon: number }>()
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    for (const f of geojson.features as any[]) {
      const z = f?.properties?.zona
      if (!z || byZone.has(z)) continue
      const c = featureCentroid(f)
      if (c) byZone.set(z, { lat: c[0], lon: c[1] })
    }
    const stats: ZoneStat[] = []
    for (const [zona, pos] of byZone.entries()) {
      const a = agg.get(zona) ?? { puntos: 0, personas: 0 }
      stats.push({ zona, lat: pos.lat, lon: pos.lon, puntos: a.puntos, personas: a.personas, color: ZONA_COLOR[zona] ?? '#475569' })
    }
    return stats
  }, [geojson, points])

  const makeLabelIcon = (s: ZoneStat) => L.divIcon({
    className: '',
    html: `
      <div style="
        background: rgba(15,23,42,0.92);
        color: #fff;
        border: 2px solid ${s.color};
        border-radius: 8px;
        padding: 4px 8px;
        font: 600 11px system-ui, sans-serif;
        white-space: nowrap;
        box-shadow: 0 2px 6px rgba(0,0,0,0.3);
        transform: translate(-50%, -50%);
      ">
        <div style="color:${s.color}; font-size:10px; letter-spacing:0.3px;">${s.zona}</div>
        <div>${s.puntos.toLocaleString('es-AR')} pts · ${s.personas.toLocaleString('es-AR')} PSC</div>
      </div>
    `,
    iconSize: [0, 0],
    iconAnchor: [0, 0],
  })

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
            className="dark-tiles"
            url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
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
          {zoneStats.map(s => (
            <Marker
              key={`label-${s.zona}`}
              position={[s.lat, s.lon]}
              icon={makeLabelIcon(s)}
              interactive={false}
            />
          ))}
        </MapContainer>
      </div>
    </div>
  )
}
