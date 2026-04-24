import type { VercelRequest, VercelResponse } from '@vercel/node'
import { sql, parseCSVNum, parseCSV, isISODate } from './_lib.js'

export default async function handler(req: VercelRequest, res: VercelResponse) {
  const { desde, hasta, gran, comunas: comunasQ, turnos: turnosQ, topN: topNQ } = req.query as Record<string, string>

  if (!isISODate(desde) || !isISODate(hasta)) {
    return res.status(400).json({ error: 'desde/hasta required (YYYY-MM-DD)' })
  }

  const granularity = ['day', 'week', 'month'].includes(gran) ? gran : 'week'
  const comunas     = parseCSVNum(comunasQ)
  const turnos      = parseCSV(turnosQ)
  const topN        = Math.min(Math.max(1, Number(topNQ) || 8), 100)

  const fmt = granularity === 'month' ? 'YYYY-MM' : 'YYYY-MM-DD'

  try {
    const [rows, totalsRaw] = await Promise.all([
      sql`
        SELECT bucket, puntos, personas FROM (
          SELECT
            to_char(date_trunc(${granularity}, "fecha_reporte"), ${fmt}) AS bucket,
            COUNT(*)::int AS puntos,
            COALESCE(SUM(LEAST(COALESCE("Cantidad de personas en situación de calle observadas", 0), 15)), 0)::int AS personas
          FROM kobo_flash_consolidado
          WHERE
            "fecha_reporte" BETWEEN ${desde}::date AND ${hasta}::date
            AND (${comunas.length} = 0 OR "Localizacion" IS NULL OR "Localizacion" = ANY(${comunas}))
            AND (${turnos.length} = 0 OR "Turno" IS NULL OR "Turno" = ANY(${turnos}))
          GROUP BY 1
          ORDER BY 1 DESC
          LIMIT ${topN}
        ) t
        ORDER BY bucket ASC
      `,
      sql`
        SELECT
          COUNT(*)::int AS puntos,
          COALESCE(SUM(LEAST(COALESCE("Cantidad de personas en situación de calle observadas", 0), 15)), 0)::int AS personas
        FROM kobo_flash_consolidado
        WHERE
          "fecha_reporte" BETWEEN ${desde}::date AND ${hasta}::date
          AND (${comunas.length} = 0 OR "Localizacion" IS NULL OR "Localizacion" = ANY(${comunas}))
          AND (${turnos.length} = 0 OR "Turno" IS NULL OR "Turno" = ANY(${turnos}))
      `,
    ])

    const totals = { puntos: totalsRaw[0].puntos as number, personas: totalsRaw[0].personas as number }
    res.status(200).json({ rows, totals })
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
}
