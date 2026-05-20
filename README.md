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
    │  Clasifica geográficamente con Mapas flash.geojson
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

Fuente única: **`Mapas flash.geojson`** — 6 zonas operativas Flash.

| Zona | Barrio |
|------|--------|
| C2   | Recoleta |
| C14  | Palermo |
| C13  | Belgrano-Núñez |
| C12  | Comuna 12 |
| C1A  | Retiro / Recoleta Norte |
| C6   | Caballito |

Prioridad en solapamientos: `C2 > C14 > C13 > C12 > C1A > C6`.

Desde 2026-03-17 el formulario incluye `tipo_flash` (zona declarada por el operador). Si la declaración difiere del GPS y el punto está a menos de 100 m del borde, se usa la declaración.

---

## Estructura del repositorio

```
├── main_act_flash.py          # ETL principal
├── enriquecer_base.py         # Post-proceso Neon
├── reclasificar_historico.py  # Reclasificación histórica puntual
├── Mapas flash.geojson        # Polígonos de zonas Flash
├── Documentacion_Fiabilidad_Datos.md
├── requirements.txt
├── dashboard/
│   ├── api/flash/             # Serverless functions (Vercel)
│   ├── public/data/           # GeoJSON para el mapa
│   └── src/modules/flash/     # React — página, filtros, mapa
└── .github/workflows/
    └── kobo_update.yml        # GitHub Actions (cron L-V cada hora)
```

---

## Variables de entorno / Secrets

| Variable | Dónde |
|----------|-------|
| `KOBO_TOKEN` | GitHub Secret + `.env` local |
| `DATABASE_URL` | GitHub Secret + `.env` local |

---

## Correr localmente

```bash
# ETL Python
pip install -r requirements.txt
python main_act_flash.py

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
