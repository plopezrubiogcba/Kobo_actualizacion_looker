import { useEffect, useState } from 'react'
import { fetchFlashMeta, formatDateART, formatUpdatedART } from '@/modules/flash/api'
import type { FlashMeta } from '@/modules/flash/types'

export const UpdatedBadge = () => {
  const [meta, setMeta] = useState<FlashMeta | null>(null)

  useEffect(() => {
    fetchFlashMeta()
      .then(setMeta)
      .catch(() => null)
  }, [])

  const updated = formatUpdatedART(meta?.generated_at)
  if (!updated) return null

  const lastSurvey = formatDateART(meta?.date_max)

  return (
    <div
      className="hidden sm:flex flex-col items-end leading-tight mr-2"
      title={lastSurvey ? `Último relevamiento: ${lastSurvey}` : undefined}
    >
      <span className="text-[10px] uppercase tracking-wide text-blue-200">
        Última actualización
      </span>
      <span className="text-xs font-semibold text-white whitespace-nowrap">
        {updated} <span className="text-yellow-400">(ART)</span>
      </span>
    </div>
  )
}
