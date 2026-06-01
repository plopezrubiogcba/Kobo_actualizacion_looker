import { create } from 'zustand'
import type { Granularity, Turno } from './types'

const ALL_TURNOS: Set<Turno> = new Set(['TM', 'TT', 'TN'])

const readUrl = () => {
  if (typeof window === 'undefined') return {}
  const p = new URLSearchParams(window.location.search)
  return {
    desde:       p.get('desde')       ?? undefined,
    hasta:       p.get('hasta')       ?? undefined,
    granularity: (p.get('gran')       ?? undefined) as Granularity | undefined,
    zonas:       p.get('zonas')       ? new Set(p.get('zonas')!.split(',')) : undefined,
    turnos:      p.get('turnos')      ? new Set(p.get('turnos')!.split(',') as Turno[])   : undefined,
    topN:        p.get('topN')        ? Number(p.get('topN'))                               : undefined,
  }
}

const writeUrl = (s: FlashFilters) => {
  if (typeof window === 'undefined') return
  const p = new URLSearchParams()
  if (s.desde)       p.set('desde', s.desde)
  if (s.hasta)       p.set('hasta', s.hasta)
  if (s.granularity !== 'week') p.set('gran', s.granularity)
  if (s.zonas.size > 0) p.set('zonas', [...s.zonas].join(','))
  if (s.turnos.size < 4)  p.set('turnos',  [...s.turnos].join(','))
  if (s.topN !== 8) p.set('topN', String(s.topN))
  const qs = p.toString()
  window.history.replaceState(null, '', qs ? `?${qs}` : window.location.pathname)
}

export interface FlashFilters {
  desde:       string
  hasta:       string
  granularity: Granularity
  zonas:       Set<string>
  turnos:      Set<Turno>
  topN:        number
}

interface FlashStore extends FlashFilters {
  setDesde:        (v: string)          => void
  setHasta:        (v: string)          => void
  setGranularity:  (v: Granularity)     => void
  setZonas:        (v: Set<string>)     => void
  setTurnos:       (v: Set<Turno>)      => void
  setTopN:         (v: number)          => void
  initFromMeta:    (min: string, max: string, zonasList: string[]) => void
  allZonas:        string[]
}

const fromUrl = readUrl()

export const useFlashStore = create<FlashStore>((set, get) => ({
  desde:       fromUrl.desde       ?? '',
  hasta:       fromUrl.hasta       ?? '',
  granularity: fromUrl.granularity ?? 'week',
  zonas:       fromUrl.zonas       ?? new Set<string>(),
  turnos:      fromUrl.turnos      ?? new Set(ALL_TURNOS),
  topN:        fromUrl.topN        ?? 8,
  allZonas:    [],

  setDesde:       (v) => { const s = { ...get(), desde: v };        writeUrl(s); set({ desde: v }) },
  setHasta:       (v) => { const s = { ...get(), hasta: v };        writeUrl(s); set({ hasta: v }) },
  setGranularity: (v) => { const s = { ...get(), granularity: v };  writeUrl(s); set({ granularity: v }) },
  setZonas:       (v) => { const s = { ...get(), zonas: v };        writeUrl(s); set({ zonas: v }) },
  setTurnos:      (v) => { const s = { ...get(), turnos: v };       writeUrl(s); set({ turnos: v }) },
  setTopN:        (v) => { const s = { ...get(), topN: v };         writeUrl(s); set({ topN: v }) },

  initFromMeta: (min, max, zonasList) => {
    const cur = get()
    const d = new Date(max)
    d.setDate(d.getDate() - 56) // 8 semanas por defecto
    const defaultDesde = d.toISOString().slice(0, 10)
    const nextDesde = cur.desde || (min > defaultDesde ? min : defaultDesde)
    const nextHasta = cur.hasta || max
    const nextZonas = cur.zonas.size > 0 ? cur.zonas : new Set(zonasList)
    const next = { ...cur, desde: nextDesde, hasta: nextHasta, zonas: nextZonas }
    writeUrl(next)
    set({ desde: nextDesde, hasta: nextHasta, zonas: nextZonas, allZonas: zonasList })
  },
}))
