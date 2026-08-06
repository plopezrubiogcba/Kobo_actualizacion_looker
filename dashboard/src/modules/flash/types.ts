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
  localizacion: string | null
  fecha: string
}

export interface FlashSummary {
  rows: FlashRow[]
  totals: { puntos: number; personas: number }
}

export interface FlashMeta {
  date_min: string
  date_max: string
  zonas: string[]
}

export type ControlEstado = 'ok' | 'falta_subir' | 'sin_declarar'

export interface ControlRow {
  fecha: string
  turnos: Turno[]
  turnos_declarados: Turno[]
  dupla: number
  kobo: number
  sheet: number
  diff: number
  estado: ControlEstado
}

export interface ControlData {
  rows: ControlRow[]
  sin_dupla: number
}
