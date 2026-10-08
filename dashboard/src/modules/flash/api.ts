import type { FlashMeta, FlashPoint, FlashSummary, Granularity, Turno, ControlData, ControlRow, ControlEstado } from './types'

// Datos estáticos generados por scripts/generar_front_data.py y
// scripts/sync_sheet_duplas.py (dashboard/public/data/).
// El filtrado/agregación replica la semántica de los viejos endpoints SQL.

interface Point {
  id: string | null
  lat: number
  lon: number
  personas: number | null
  turno: Turno | null
  localizacion: string | null
  fecha: string // YYYY-MM-DD
  dupla: number | null
}

interface ControlSheetGroup {
  fecha: string
  dupla: number
  sheet: number
  turnos_sheet: Turno[]
  fotos: { turno: Turno | null; url: string }[]
}

interface ControlSheetFile {
  generated_at?: string
  rows: ControlSheetGroup[]
}

const cache = new Map<string, Promise<unknown>>()

const getJSON = <T>(url: string): Promise<T> => {
  let p = cache.get(url) as Promise<T> | undefined
  if (!p) {
    p = fetch(url).then(r => {
      if (!r.ok) throw new Error(`${url}: ${r.status}`)
      return r.json() as Promise<T>
    })
    cache.set(url, p)
  }
  return p
}

const loadPoints = () => getJSON<Point[]>('/data/flash_points.json')

// Mismo filtro que el WHERE de points/summary en SQL:
// - fecha_reporte BETWEEN desde/hasta
// - Localizacion NULL o en la lista (NULL pasaba siempre)
// - Turno NULL o en la lista
// - personas NULL o <= 11
const matches = (p: Point, desde: string, hasta: string, zonas: string[], turnos: Turno[]): boolean =>
  p.fecha >= desde && p.fecha <= hasta &&
  (zonas.length === 0 || p.localizacion == null || zonas.includes(p.localizacion)) &&
  (turnos.length === 0 || p.turno == null || turnos.includes(p.turno)) &&
  (p.personas == null || p.personas <= 11)

// Equivalente a date_trunc(): lunes para week (UTC, fechas ya normalizadas)
const bucketOf = (fecha: string, gran: Granularity): string => {
  if (gran === 'day') return fecha
  if (gran === 'month') return fecha.slice(0, 7)
  const d = new Date(`${fecha}T12:00:00Z`)
  d.setUTCDate(d.getUTCDate() - ((d.getUTCDay() + 6) % 7))
  return d.toISOString().slice(0, 10)
}

export const fetchFlashMeta = async (): Promise<FlashMeta> =>
  getJSON<FlashMeta>('/data/flash_meta.json')

export const fetchFlashSummary = async (opts: {
  desde: string
  hasta: string
  granularity: Granularity
  zonas: string[]
  turnos: Turno[]
  topN: number
}): Promise<FlashSummary> => {
  const points = await loadPoints()
  const rows = points.filter(p => matches(p, opts.desde, opts.hasta, opts.zonas, opts.turnos))

  const agg = new Map<string, { bucket: string; puntos: number; personas: number }>()
  for (const p of rows) {
    const b = bucketOf(p.fecha, opts.granularity)
    const cur = agg.get(b) ?? { bucket: b, puntos: 0, personas: 0 }
    cur.puntos += 1
    cur.personas += p.personas ?? 0
    agg.set(b, cur)
  }

  const buckets = [...agg.values()]
    .sort((a, b) => (a.bucket < b.bucket ? 1 : -1)) // DESC
    .slice(0, opts.topN)
    .sort((a, b) => (a.bucket < b.bucket ? -1 : 1)) // ASC

  const totals = {
    puntos: rows.length,
    personas: rows.reduce((s, p) => s + (p.personas ?? 0), 0),
  }
  return { rows: buckets, totals }
}

export const fetchFlashPoints = async (opts: {
  desde: string
  hasta: string
  zonas: string[]
  turnos: Turno[]
}): Promise<FlashPoint[]> => {
  const points = await loadPoints()
  return points
    .filter(p => matches(p, opts.desde, opts.hasta, opts.zonas, opts.turnos))
    .slice(0, 5000)
    .map(p => ({
      id: p.id ?? '',
      lat: p.lat,
      lon: p.lon,
      personas: p.personas ?? 0,
      turno: p.turno as Turno,
      localizacion: p.localizacion,
      fecha: p.fecha,
    }))
}

// FULL OUTER JOIN kobo vs sheet por (fecha, dupla), en cliente
export const fetchControl = async (opts: {
  desde: string
  hasta: string
  duplas: number[]
  turnos: Turno[]
}): Promise<ControlData> => {
  const [points, sheetFile] = await Promise.all([
    loadPoints(),
    getJSON<ControlSheetFile>('/data/control_sheet.json'),
  ])

  const inRange = (f: string) => f >= opts.desde && f <= opts.hasta
  const duplaOK = (d: number) => opts.duplas.length === 0 || opts.duplas.includes(d)

  // Lado Kobo: dupla no nula, rango, filtro duplas, personas NULL o <= 11
  // (lat/lon y fecha ya vienen no nulos en points)
  const kobo = new Map<string, { fecha: string; dupla: number; kobo: number; turnos: Set<Turno> }>()
  for (const p of points) {
    if (p.dupla == null || !inRange(p.fecha) || !duplaOK(p.dupla)) continue
    if (!(p.personas == null || p.personas <= 11)) continue
    const key = `${p.fecha}|${p.dupla}`
    let g = kobo.get(key)
    if (!g) {
      g = { fecha: p.fecha, dupla: p.dupla, kobo: 0, turnos: new Set() }
      kobo.set(key, g)
    }
    g.kobo += 1
    if (p.turno) g.turnos.add(p.turno)
  }

  // Lado Sheet: rango + filtro duplas
  const sheet = new Map<string, ControlSheetGroup>()
  for (const r of sheetFile.rows) {
    if (inRange(r.fecha) && duplaOK(r.dupla)) sheet.set(`${r.fecha}|${r.dupla}`, r)
  }

  const keys = [...new Set([...kobo.keys(), ...sheet.keys()])]
  const rows: ControlRow[] = []

  for (const key of keys) {
    const k = kobo.get(key)
    const s = sheet.get(key)
    const turnosKobo = k ? [...k.turnos] : []
    const turnosSheet = s ? s.turnos_sheet : []

    // WHERE turnos vacío o solape con kobo/sheet (array && en SQL)
    if (opts.turnos.length > 0) {
      const hit = turnosKobo.some(t => opts.turnos.includes(t)) ||
        turnosSheet.some(t => opts.turnos.includes(t))
      if (!hit) continue
    }

    const koboN = k?.kobo ?? 0
    const sheetN = s?.sheet ?? 0
    const diff = koboN - sheetN
    let estado: ControlEstado
    if (sheetN !== 0 && koboN === 0) estado = 'falta_subir'
    else if (sheetN === 0 && koboN !== 0) estado = 'sin_declarar'
    else if (diff >= 0) estado = 'ok'
    else estado = 'falta_subir'

    rows.push({
      fecha: k?.fecha ?? s?.fecha ?? '',
      dupla: k?.dupla ?? s?.dupla ?? 0,
      kobo: koboN,
      sheet: sheetN,
      diff,
      estado,
      turnos: [...new Set([...turnosKobo, ...turnosSheet])].filter(Boolean),
      turnos_declarados: [...new Set(turnosSheet)].filter(Boolean),
      fotos: s?.fotos ?? [],
    })
  }

  rows.sort((a, b) => (a.fecha === b.fecha ? a.dupla - b.dupla : a.fecha < b.fecha ? -1 : 1))

  // sin_dupla: dupla NULL, geo presente, en rango, sin filtro de personas (como el SQL)
  const sin_dupla = points.filter(p =>
    p.dupla == null &&
    inRange(p.fecha) &&
    (opts.turnos.length === 0 || (p.turno != null && opts.turnos.includes(p.turno)))
  ).length

  return { rows, sin_dupla }
}
