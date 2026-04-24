import type { VercelRequest, VercelResponse } from '@vercel/node'
import { sql, parseCSVNum, parseCSV, isISODate } from './_lib.js'

export default async function handler(req: VercelRequest, res: VercelResponse) {
  const { desde, hasta, comunas: comunasQ, turnos: turnosQ } = req.query as Record<string, string>

  if (!isISODate(desde) || !isISODate(hasta)) {
    return res.status(400).json({ error: 'desde/hasta required (YYYY-MM-DD)' })
  }

  const comunas = parseCSVNum(comunasQ)
  const turnos  = parseCSV(turnosQ)

  try {
    const rows = await sql`
      SELECT
        "_id"::text AS id,
        latitude::float AS lat,
        longitude::float AS lon,
        LEAST(COALESCE("Cantidad de personas en situación de calle observadas", 0), 15)::int AS personas,
        "Turno" AS turno,
        "Localizacion" AS localizacion,
        to_char("fecha_reporte", 'YYYY-MM-DD') AS fecha
      FROM kobo_flash_consolidado
      WHERE
        latitude IS NOT NULL AND longitude IS NOT NULL
        AND "fecha_reporte" BETWEEN ${desde}::date AND ${hasta}::date
        AND (${comunas.length} = 0 OR "Localizacion" IS NULL OR "Localizacion" = ANY(${comunas}))
        AND (${turnos.length} = 0 OR "Turno" IS NULL OR "Turno" = ANY(${turnos}))
      LIMIT 5000
    `
    res.status(200).json(rows)
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
}
