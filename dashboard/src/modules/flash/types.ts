export type Turno = 'TM' | 'TT' | 'TN'
export type Granularity = 'day' | 'week' | 'month'

export interface FlashRow {
  bucket: string
  puntos: number
  personas: number
}

export interface FlashPoint {
  id: string
  lat: number
  lon: number
  personas: number
  turno: Turno
  localizacion: number | null
  fecha: string
}

export interface FlashSummary {
  rows: FlashRow[]
  totals: { puntos: number; personas: number }
}

export interface FlashMeta {
  date_min: string
  date_max: string
  comunas: number[]
}
