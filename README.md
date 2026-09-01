# Kobo Flash — ETL + Dashboard

Pipeline automatizado **KoboToolbox → Neon Postgres → React/Vercel**.
Procesa relevamientos del operativo Flash (situación de calle, CABA) y los expone en un dashboard interactivo.

---

## Pipeline

```
Kobo API v2
    │
    ▼
main_act_flash.py          ← ETL principal
    │  Descarga registros nuevos (incremental por _submission_time)
    │  Clasifica geográficamente con Zonas flash.kml
    │  Inserta en Neon Postgres
    ▼
enriquecer_base.py         ← Post-proceso
    │  Calcula fecha_reporte, inicio_semana_lunes, Turno
    ▼
Neon Postgres (kobo_flash_consolidado)
    │
    ▼
dashboard/                 ← React + Vite + Leaflet
    │  API serverless en Vercel (dashboard/api/flash/)
    ▼
Vercel (deploy automático en push a main)
```

---

## Clasificación geográfica

Fuente única: **`assets/mapas flash fronteras nuevos.kml`** — 11 zonas operativas Flash (vigente desde 2026-09-01). La columna `Tablero` del KML define el nombre final de zona.

| Zona | Nota KML (`Tipo`) |
|------|--------|
| C1A | Norte |
| C2 | Norte |
| C14 | Norte |
| C13 | Norte |
| C12 | Centro |
| Frontera Norte | Frontera Norte (antes `Frontera`) |
| Frontera Sur-este | Frontera Sur-este |
| C6 Centro | Centro (antes `C6`) |
| C5 Centro | Centro |
| C3 Centro | Centro |
| C15 Centro | Centro |

Prioridad en solapamientos: `Frontera Norte > Frontera Sur-este > C2 > C14 > C13 > C12 > C1A > C15 Centro > C5 Centro > C3 Centro > C6 Centro`.

Desde septiembre los formularios ya no incluyen `tipo_flash` (zona declarada): la clasificación es **solo por GPS** (sin override declarado).

Recorridos por dupla: `assets/recorridos flash norte.kml` (duplas 1–19) + `assets/recorridos flash centro.kml` (duplas 21–33). Dupla 20 eliminada.

---

## Estructura del repositorio

```
├── scripts/
│   ├── main_act_flash.py           # ETL principal (3 forms Kobo: Norte/Centro/Sur)
│   ├── enriquecer_base.py          # Post-proceso Neon
│   ├── reclasificar_historico.py   # Reclasificación histórica puntual
│   └── generar_overlay_dashboard.py# Regenera mapa_flash.geojson desde el KML
├── assets/
│   ├── mapas flash fronteras nuevos.kml  # Polígonos de zonas Flash (fuente de verdad)
│   ├── recorridos flash norte.kml        # Recorridos duplas 1-19
│   └── recorridos flash centro.kml       # Recorridos duplas 21-33
├── actualizar.sh                   # Helper local: ETL + deploy Vercel
├── Documentacion_Fiabilidad_Datos.md
├── requirements.txt
├── dashboard/
│   ├── api/flash/                  # Serverless functions (Vercel)
│   ├── public/data/                # GeoJSON para el mapa
│   └── src/modules/flash/          # React — página, filtros, mapa
└── .github/workflows/
    └── kobo_update.yml             # GitHub Actions (cron L-V cada hora)
```

---

## Variables de entorno / Secrets

| Variable | Dónde |
|----------|-------|
| `KOBO_TOKEN_NORTE` | GitHub Secret + `.env` local |
| `KOBO_TOKEN_CENTRO` | GitHub Secret + `.env` local |
| `KOBO_TOKEN_SUR` | GitHub Secret + `.env` local |
| `DATABASE_URL` | GitHub Secret + `.env` local |

---

## Correr localmente

```bash
# ETL Python
pip install -r requirements.txt
python scripts/main_act_flash.py

# Dashboard (requiere .env en raíz con DATABASE_URL)
cd dashboard
npm install
npm run dev        # http://localhost:5173
```

`npm run dev` levanta también las funciones `/api/*` vía plugin Vite (no hace falta `vercel dev`).

---

## Automatización

GitHub Actions ejecuta el ETL automáticamente **lunes a viernes, cada hora en el minuto 15**.
También se puede disparar manualmente desde la pestaña **Actions → Run workflow**.
