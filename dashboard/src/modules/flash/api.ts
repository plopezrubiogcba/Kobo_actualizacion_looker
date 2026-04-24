import type { FlashMeta, FlashPoint, FlashSummary, Granularity, Turno } from './types'

const buildParams = (
  params: Record<string, string | number | string[] | number[]>
) => {
  const p = new URLSearchParams()
  for (const [k, v] of Object.entries(params)) {
    if (Array.isArray(v)) p.set(k, v.join(','))
    else p.set(k, String(v))
  }
  return p.toString()
}

export const fetchFlashMeta = async (): Promise<FlashMeta> => {
  const res = await fetch('/api/flash/meta')
  if (!res.ok) throw new Error(`meta: ${res.status}`)
  return res.json()
}

export const fetchFlashSummary = async (opts: {
  desde: string
  hasta: string
  granularity: Granularity
  comunas: number[]
  turnos: Turno[]
  topN: number
}): Promise<FlashSummary> => {
  const qs = buildParams({
    desde: opts.desde,
    hasta: opts.hasta,
    gran:  opts.granularity,
    comunas: opts.comunas,
    turnos:  opts.turnos,
    topN:    opts.topN,
  })
  const res = await fetch(`/api/flash/summary?${qs}`)
  if (!res.ok) throw new Error(`summary: ${res.status}`)
  return res.json()
}

export const fetchFlashPoints = async (opts: {
  desde: string
  hasta: string
  comunas: number[]
  turnos: Turno[]
}): Promise<FlashPoint[]> => {
  const qs = buildParams({
    desde: opts.desde,
    hasta: opts.hasta,
    comunas: opts.comunas,
    turnos:  opts.turnos,
  })
  const res = await fetch(`/api/flash/points?${qs}`)
  if (!res.ok) throw new Error(`points: ${res.status}`)
  return res.json()
}
