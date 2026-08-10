import type { VercelRequest, VercelResponse } from '@vercel/node'
import { sql, parseCSV, parseCSVNum, isISODate } from './_lib.js'

export default async function handler(req: VercelRequest, res: VercelResponse) {
  const { desde, hasta, duplas: duplasQ, turnos: turnosQ } = req.query as Record<string, string>

  if (!isISODate(desde) || !isISODate(hasta)) {
    return res.status(400).json({ error: 'desde/hasta required (YYYY-MM-DD)' })
  }

  const duplas = parseCSVNum(duplasQ)
  const turnos = parseCSV(turnosQ)

  try {
    const rows = await sql`
      SELECT
        to_char(COALESCE(k.fecha, s.fecha), 'YYYY-MM-DD') AS fecha,
        COALESCE(k.dupla, s.dupla)::int AS dupla,
        COALESCE(k.kobo, 0)::int AS kobo,
        COALESCE(s.sheet, 0)::int AS sheet,
        COALESCE(s.turnos_sheet, '{}') AS turnos_sheet,
        COALESCE(s.turnos_sheet, '{}') || COALESCE(k.turnos_kobo, '{}') AS turnos,
        COALESCE(s.fotos, '[]') AS fotos
      FROM (
        SELECT
          "fecha_reporte" AS fecha,
          dupla,
          COUNT(*)::int AS kobo,
          array_agg(DISTINCT "Turno") FILTER (WHERE "Turno" IS NOT NULL) AS turnos_kobo
        FROM kobo_flash_consolidado
        WHERE
          dupla IS NOT NULL
          AND "fecha_reporte" BETWEEN ${desde}::date AND ${hasta}::date
          AND (${duplas.length} = 0 OR dupla = ANY(${duplas}))
          AND ("Cantidad de personas en situación de calle observadas" IS NULL OR "Cantidad de personas en situación de calle observadas" <= 11)
        GROUP BY 1, 2
      ) k
      FULL OUTER JOIN (
        SELECT
          fecha,
          dupla,
          SUM(registros)::int AS sheet,
          array_agg(DISTINCT turno) FILTER (WHERE turno IS NOT NULL) AS turnos_sheet,
          jsonb_agg(jsonb_build_object('turno', turno, 'url', foto_url) ORDER BY sheet_row)
            FILTER (WHERE foto_url IS NOT NULL) AS fotos
        FROM control_duplas_sheet
        WHERE
          fecha BETWEEN ${desde}::date AND ${hasta}::date
          AND (${duplas.length} = 0 OR dupla = ANY(${duplas}))
        GROUP BY 1, 2
      ) s
        ON k.fecha = s.fecha AND k.dupla = s.dupla
      WHERE
        ${turnos.length} = 0
        OR (s.turnos_sheet && ${turnos}::text[])
        OR (k.turnos_kobo && ${turnos}::text[])
      ORDER BY fecha ASC, dupla ASC
    `

    const sinDuplaRaw = await sql`
      SELECT COUNT(*)::int AS n
      FROM kobo_flash_consolidado
      WHERE
        dupla IS NULL
        AND latitude IS NOT NULL AND longitude IS NOT NULL
        AND "fecha_reporte" BETWEEN ${desde}::date AND ${hasta}::date
        AND (${turnos.length} = 0 OR "Turno" = ANY(${turnos}))
    `

    const withEstado = rows.map(r => {
      const kobo = r.kobo as number
      const sheet = r.sheet as number
      const diff = kobo - sheet
      let estado: string
      if (sheet !== 0 && kobo === 0) estado = 'falta_subir'
      else if (sheet === 0 && kobo !== 0) estado = 'sin_declarar'
      else if (diff >= 0) estado = 'ok'
      else estado = 'falta_subir'
      const turnosArr = (r.turnos as string[] | null) ?? []
      const turnosSheet = (r.turnos_sheet as string[] | null) ?? []
      return {
        fecha: r.fecha,
        dupla: r.dupla,
        kobo,
        sheet,
        diff,
        estado,
        turnos: [...new Set(turnosArr)].filter(Boolean),
        turnos_declarados: [...new Set(turnosSheet)].filter(Boolean),
        fotos: r.fotos as { turno: string | null; url: string }[] ?? [],
      }
    })

    res.status(200).json({ rows: withEstado, sin_dupla: sinDuplaRaw[0]?.n ?? 0 })
  } catch (e) {
    res.status(500).json({ error: String(e) })
  }
}
