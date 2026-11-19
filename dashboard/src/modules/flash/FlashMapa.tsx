import { useEffect, useMemo, useRef, useState } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, GeoJSON, Marker, Pane, useMap, useMapEvents } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import { fetchFlashPoints } from './api'
import { useFlashStore } from './store'
import { useFlashMetaInit } from './useFlashMeta'
import { FlashFilters } from './FlashFilters'
import type { FlashPoint } from './types'

const CABA_CENTER: [number, number] = [-34.615, -58.443]

// CARTO pasó a exigir API key (los tiles devuelven "API KEY REQUIRED"),
// así que el basemap oscuro primario ahora es Esri (keyless).
const BASEMAPS = [
  {
    name: 'Esri Dark Gray',
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}',
    attribution: 'Tiles &copy; Esri &mdash; Esri, HERE, Garmin, &copy; OpenStreetMap contributors',
    maxNativeZoom: 16,
  },
  {
    name: 'Esri World Imagery',
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    attribution: 'Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics',
    maxNativeZoom: 19,
  },
  {
    name: 'OpenStreetMap',
    url: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
    maxNativeZoom: 19,
  },
]

const ZONA_COLOR: Record<string, string> = {
  'Frontera Norte': '#ef4444',
  'Frontera Este': '#8b5cf6',
  'Frontera Sur': '#14b8a6',
  C2: '#f97316',
  C14: '#3b82f6',
  C13: '#22c55e',
  C12: '#d946ef',
  C1A: '#eab308',
  'C15 Centro': '#06b6d4',
  'C5 Centro': '#a855f7',
  'C3 Centro': '#84cc16',
  'C6 Centro': '#f59e0b',
  Otro: '#94a3b8',
}

const ZONA_LABEL: Record<string, string> = {
  'Frontera Norte': 'Zona de Frontera Norte',
  'Frontera Este': 'Zona de Frontera Este',
  'Frontera Sur': 'Zona de Frontera Sur',
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

// Familia Frontera: borde discontinuo compartido + relleno más marcado
const ZONA_EMPHASIS: Record<string, { weight: number; dashArray?: string; fillOpacity: number }> = {
  'Frontera Norte': { weight: 4, dashArray: '8 5', fillOpacity: 0.28 },
  'Frontera Este': { weight: 4, dashArray: '8 5', fillOpacity: 0.28 },
  'Frontera Sur': { weight: 4, dashArray: '8 5', fillOpacity: 0.28 },
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const zoneStyle = (feature: any, highlight = false) => {
  const zona = feature?.properties?.zona ?? ''
  const color = ZONA_COLOR[zona] ?? '#475569'
  const em = ZONA_EMPHASIS[zona]
  return {
    color,
    weight: (em?.weight ?? 2.5) + (highlight ? 2 : 0),
    opacity: 1,
    dashArray: em?.dashArray,
    fillColor: color,
    fillOpacity: (em?.fillOpacity ?? 0.18) + (highlight ? 0.15 : 0),
  }
}

// Ancla del cartel: centro del bbox del anillo exterior más grande.
// El promedio de todos los vértices se desplaza hacia los tramos con más
// puntos y puede dejar la etiqueta fuera del polígono.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const featureAnchor = (feature: any): [number, number] | null => {
  const geom = feature?.geometry
  if (!geom) return null

  const outerRings: number[][][] = geom.type === 'Polygon'
    ? [geom.coordinates?.[0]]
    : geom.type === 'MultiPolygon'
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ? geom.coordinates.map((poly: any) => poly?.[0])
      : []
  const rings = outerRings.filter((r): r is number[][] => Array.isArray(r) && r.length > 0)
  if (rings.length === 0) return null

  let best: number[][] | null = null
  let bestArea = -1
  for (const ring of rings) {
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity
    for (const c of ring) {
      const [x, y] = c
      if (x < minX) minX = x
      if (x > maxX) maxX = x
      if (y < minY) minY = y
      if (y > maxY) maxY = y
    }
    const area = (maxX - minX) * (maxY - minY)
    if (area > bestArea) { bestArea = area; best = ring }
  }
  if (!best) return null

  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity
  for (const [x, y] of best) {
    if (x < minX) minX = x
    if (x > maxX) maxX = x
    if (y < minY) minY = y
    if (y > maxY) maxY = y
  }
  return [(minY + maxY) / 2, (minX + maxX) / 2]
}

type ZoneStat = { zona: string; lat: number; lon: number; puntos: number; personas: number; color: string }

const makeLabelIcon = (s: ZoneStat) => {
  const dim = s.puntos === 0
  const opacity = dim ? 0.55 : 1
  const nombre = ZONA_LABEL[s.zona] ?? s.zona
  return L.divIcon({
    className: '',
    html: `
      <div style="
        transform: translate(-50%, -100%);
        width: max-content;
        margin-bottom: 14px;
        opacity: ${opacity};
        filter: drop-shadow(0 3px 8px rgba(0,0,0,0.55));
      ">
        <div style="
          display: flex;
          align-items: stretch;
          background: #ffffff;
          border-radius: 8px;
          overflow: hidden;
          font-family: system-ui, sans-serif;
          white-space: nowrap;
          box-shadow: 0 0 0 1px rgba(15,23,42,0.25);
        ">
          <div style="width: 5px; background: ${s.color};"></div>
          <div style="padding: 5px 10px 6px;">
            <div style="
              display: flex;
              align-items: center;
              gap: 5px;
              color: #0f172a;
              font-size: 12px;
              font-weight: 700;
              line-height: 1.2;
            ">
              <span style="
                width: 8px; height: 8px;
                border-radius: 50%;
                background: ${s.color};
                flex-shrink: 0;
              "></span>${nombre}
            </div>
            <div style="
              color: #475569;
              font-size: 11px;
              font-weight: 600;
              margin-top: 1px;
            ">${s.puntos.toLocaleString('es-AR')} pts · ${s.personas.toLocaleString('es-AR')} PSC</div>
          </div>
        </div>
        <div style="
          width: 0; height: 0;
          margin: 0 auto;
          border-left: 7px solid transparent;
          border-right: 7px solid transparent;
          border-top: 8px solid ${s.color};
        "></div>
      </div>
    `,
    iconSize: [0, 0],
    iconAnchor: [0, 0],
  })
}

// Fuerza a Leaflet a recalcular tamaño cuando el contenedor flex se asienta.
const MapResizer = () => {
  const map = useMap()
  useEffect(() => {
    const onResize = () => map.invalidateSize()
    const timer = setTimeout(onResize, 0)
    window.addEventListener('resize', onResize)
    return () => {
      clearTimeout(timer)
      window.removeEventListener('resize', onResize)
    }
  }, [map])
  return null
}

const ZONE_LABEL_PANE_Z = 680
const LABEL_HEIGHT = 42
const LABEL_GAP = 14

// Evita carteles superpuestos: se dibujan por prioridad (más PSC primero) y
// se descarta el que colisiona con uno ya aceptado.
const ZoneLabels = ({ stats }: { stats: ZoneStat[] }) => {
  const [tick, setTick] = useState(0)
  const map = useMapEvents({
    zoomend: () => setTick(t => t + 1),
    moveend: () => setTick(t => t + 1),
    resize: () => setTick(t => t + 1),
  })
  const [visible, setVisible] = useState<Set<string>>(new Set())

  useEffect(() => {
    if (map.getZoom() < 11) {
      setVisible(new Set())
      return
    }
    const ordered = [...stats].sort(
      (a, b) => b.personas - a.personas || b.puntos - a.puntos,
    )
    const accepted: { l: number; t: number; r: number; b: number }[] = []
    const next = new Set<string>()
    for (const s of ordered) {
      const p = map.latLngToContainerPoint([s.lat, s.lon])
      const nombre = ZONA_LABEL[s.zona] ?? s.zona
      const statsTxt = `${s.puntos} pts · ${s.personas} PSC`
      const width = Math.max(nombre.length * 6.8, statsTxt.length * 5.8) + 32
      const rect = {
        l: p.x - width / 2,
        t: p.y - LABEL_HEIGHT - LABEL_GAP,
        r: p.x + width / 2,
        b: p.y - LABEL_GAP,
      }
      const collides = accepted.some(a =>
        rect.l < a.r && rect.r > a.l && rect.t < a.b && rect.b > a.t,
      )
      if (collides) continue
      accepted.push(rect)
      next.add(s.zona)
    }
    setVisible(next)
  }, [map, stats, tick])

  return (
    <Pane name="zone-labels" style={{ zIndex: ZONE_LABEL_PANE_Z, pointerEvents: 'none' }}>
      {stats.filter(s => visible.has(s.zona)).map(s => (
        <Marker
          key={`label-${s.zona}`}
          position={[s.lat, s.lon]}
          icon={makeLabelIcon(s)}
          interactive={false}
        />
      ))}
    </Pane>
  )
}

export const FlashMapa = () => {
  const { desde, hasta, zonas, turnos } = useFlashStore()
  const { ready, error: metaError } = useFlashMetaInit()
  const [points, setPoints] = useState<FlashPoint[]>([])
  const [loading, setLoading] = useState(false)
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const [geojson, setGeojson] = useState<any>(null)
  const [basemapIdx, setBasemapIdx] = useState(0)
  const tileErrors = useRef(0)

  useEffect(() => {
    fetch('/data/mapa_flash.geojson')
      .then(r => r.ok ? r.json() : null)
      .then(g => g && setGeojson(g))
      .catch(() => null)
  }, [])

  useEffect(() => {
    if (!ready || !desde || !hasta || zonas.size === 0 || turnos.size === 0) return
    setLoading(true)
    fetchFlashPoints({ desde, hasta, zonas: [...zonas], turnos: [...turnos] })
      .then(data => { setPoints(data); setLoading(false) })
      .catch(() => setLoading(false))
  }, [ready, desde, hasta, zonas, turnos])

  const maxPersonas = Math.max(1, ...points.map(p => p.personas))
  const radius = (p: FlashPoint) => 4 + (p.personas / maxPersonas) * 16
  const pointColor = (p: FlashPoint) => ZONA_COLOR[p.localizacion ?? ''] ?? '#f97316'

  const handleTileError = () => {
    tileErrors.current += 1
    if (tileErrors.current >= 5) {
      tileErrors.current = 0
      // Varios tiles fallan en el mismo tick: clampear el índice dentro del
      // updater evita saltar a un basemap inexistente.
      setBasemapIdx(i => (i < BASEMAPS.length - 1 ? i + 1 : i))
    }
  }

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
      const c = featureAnchor(f)
      if (c) byZone.set(z, { lat: c[0], lon: c[1] })
    }
    const stats: ZoneStat[] = []
    for (const [zona, pos] of byZone.entries()) {
      const a = agg.get(zona) ?? { puntos: 0, personas: 0 }
      stats.push({ zona, lat: pos.lat, lon: pos.lon, puntos: a.puntos, personas: a.personas, color: ZONA_COLOR[zona] ?? '#475569' })
    }
    return stats
  }, [geojson, points])

  const basemap = BASEMAPS[basemapIdx] ?? BASEMAPS[0]

  return (
    <div className="flex flex-col" style={{ height: 'calc(100vh - 56px)' }}>
      <FlashFilters />
      {metaError && (
        <div className="text-red-500 text-sm px-6 py-2">{metaError}</div>
      )}
      {(!ready || loading) && (
        <div className="text-gray-500 text-sm px-6 py-2">
          {!ready ? 'Cargando datos…' : 'Cargando puntos…'}
        </div>
      )}
      <div className="flex-1 relative" style={{ minHeight: '500px' }}>
        <MapContainer
          center={CABA_CENTER}
          zoom={12}
          minZoom={10}
          maxZoom={19}
          style={{ height: '100%', width: '100%', minHeight: '500px' }}
          className="bg-slate-900"
        >
          <MapResizer />
          <TileLayer
            key={basemap.url}
            url={basemap.url}
            attribution={basemap.attribution}
            maxZoom={19}
            maxNativeZoom={basemap.maxNativeZoom}
            eventHandlers={{ tileerror: handleTileError }}
          />
          {geojson && (
            <GeoJSON
              data={geojson}
              style={zoneStyle}
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              onEachFeature={(feature: any, layer: L.Layer) => {
                const path = layer as L.Path
                path.on('mouseover', () => path.setStyle(zoneStyle(feature, true)))
                path.on('mouseout', () => path.setStyle(zoneStyle(feature)))
              }}
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
          <ZoneLabels stats={zoneStats} />
        </MapContainer>
        {basemapIdx > 0 && (
          <div className="absolute top-3 left-3 z-[700] bg-slate-900/90 text-white text-[11px] px-2 py-1 rounded shadow">
            Basemap alternativo: {basemap.name}
          </div>
        )}
      </div>
    </div>
  )
}
