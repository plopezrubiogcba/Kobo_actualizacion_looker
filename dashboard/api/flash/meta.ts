import type { VercelRequest, VercelResponse } from '@vercel/node'
import { sql } from './_lib.js'

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  try {
    const [range] = await sql`
      SELECT
        to_char(MIN("fecha_reporte"), 'YYYY-MM-DD') AS date_min,
        to_char(MAX("fecha_reporte"), 'YYYY-MM-DD') AS date_max
      FROM kobo_flash_consolidado
      WHERE "fecha_reporte" IS NOT NULL
    `
    const zonasRaw = await sql`
      SELECT DISTINCT "Localizacion"
      FROM kobo_flash_consolidado
      WHERE "Localizacion" IS NOT NULL
      ORDER BY 1
    `
    const zonas = zonasRaw.map((r: Record<string, unknown>) => r.Localizacion as string)
    res.status(200).json({ date_min: range.date_min, date_max: range.date_max, zonas })
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
}
