import { useEffect, useState } from 'react'
import { fetchFlashMeta } from './api'
import { useFlashStore } from './store'

// Inicializa los filtros desde flash_meta.json una sola vez (fetch cacheado).
// Sin esto, entrar directo a /mapa o recargar la página deja el store sin
// rango de fechas y el mapa no pide puntos ni muestra el selector de zonas.
export const useFlashMetaInit = () => {
  const initFromMeta = useFlashStore(s => s.initFromMeta)
  const [ready, setReady] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let alive = true
    fetchFlashMeta()
      .then(meta => {
        if (!alive) return
        initFromMeta(meta.date_min, meta.date_max, meta.zonas)
        setReady(true)
      })
      .catch(e => {
        if (!alive) return
        setError(String(e))
        setReady(true)
      })
    return () => { alive = false }
  }, [initFromMeta])

  return { ready, error }
}
