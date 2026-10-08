# Kobo Flash — ETL + Dashboard

Pipeline automatizado **KoboToolbox → Parquet → JSON/CSV → React/Vercel**.
Procesa relevamientos del operativo Flash (situación de calle, CABA) y los expone en un dashboard interactivo.

> **Nota:** Neon Postgres quedó **deprecado**. La fuente de verdad es ahora un parquet
> canónico (`data/kobo_flash.parquet`) y el front consume JSON/CSV estáticos.

---

## Pipeline

```
Kobo API v2 (3 forms: Norte / Centro / Sur)
    │
    ▼
actualizar_parquet.py        ← ETL principal (full pull, misma lógica de siempre)
    │  Clasifica geográficamente con Zonas flash.kml (GPS + snap 100m)
    │  Asigna dupla por polígonos de recorridos
    │  Dedup por _uuid
    ▼
data/kobo_flash.parquet      ← FUENTE DE VERDAD (versionada)
data/meta.json               ← conteos de verificación
backup/kobo_flash_backup_*.parquet  ← respaldo fechado local
    │
    ▼
generar_front_data.py        ← parquet → datos estáticos
    │
    ├── dashboard/public/data/flash_points.json   (53k filas, compacto)
    ├── dashboard/public/data/flash_meta.json     (rango de fechas + zonas)
    └── dashboard/public/data/flash.csv           (descargable, ';' + BOM)
    │
sync_sheet_duplas.py         ← Google Sheet duplas →
    └── dashboard/public/data/control_sheet.json
    │
    ▼
dashboard/                   ← React + Vite + Leaflet (filtrado en cliente)
    │  Sin API serverless: el front filtra/agrega los JSON localmente
    ▼
Vercel (deploy por push a main + deploy horario desde GitHub Actions)
```

La lógica de datos (coords/fechas, `fecha_reporte` con corte h<6, Turno TM/TT/TN,
zonas por prioridad GPS, dupla por polígono) **no cambió**: es la misma de siempre.

---

## Clasificación geográfica

Fuente única: **`assets/Mapas flash finales.kml`** — 12 zonas operativas Flash (vigente desde 2026-09-03). La columna `Tablero` del KML define el nombre final de zona.

| Zona | Nota KML (`Tipo`) |
|------|--------|
| C1A | Norte |
| C2 | Norte |
| C14 | Norte |
| C13 | Norte |
| C12 | Centro |
| Frontera Norte | Norte (antes `Frontera`) |
| Frontera Este | Centro (antes parte de `Frontera Sur-este`) |
| Frontera Sur | Centro (antes parte de `Frontera Sur-este`) |
| C6 Centro | Centro (antes `C6`) |
| C5 Centro | Centro |
| C3 Centro | Centro |
| C15 Centro | Centro |

Prioridad en solapamientos: `Frontera Norte > Frontera Este > Frontera Sur > C2 > C14 > C13 > C12 > C1A > C15 Centro > C5 Centro > C3 Centro > C6 Centro`.

Desde septiembre los formularios ya no incluyen `tipo_flash` (zona declarada): la clasificación es **solo por GPS** (sin override declarado).

Recorridos por dupla: `assets/recorridos_totales_consultora.kml` (consolidado, duplas 1–19 Norte + 21–33 Centro). Dupla 20 eliminada.

---

## Estructura del repositorio

```
├── scripts/
│   ├── actualizar_parquet.py         # ETL principal: Kobo → data/kobo_flash.parquet
│   ├── generar_front_data.py         # parquet → flash_points/meta.json + flash.csv
│   ├── sync_sheet_duplas.py          # Google Sheet duplas → control_sheet.json
│   ├── backup_kobo_parquet.py        # respaldo fechado local (comparte procesamiento)
│   └── generar_overlay_dashboard.py  # regenera mapa_flash.geojson desde el KML
├── data/
│   ├── kobo_flash.parquet            # FUENTE DE VERDAD (versionada)
│   └── meta.json                     # conteos de la última corrida
├── assets/
│   ├── Mapas flash finales.kml       # polígonos de zonas Flash (fuente de verdad)
│   └── recorridos_totales_consultora.kml
├── actualizar.sh                     # helper local: ETL + datos + deploy Vercel
├── Documentacion_Fiabilidad_Datos.md
├── requirements.txt
├── dashboard/
│   ├── public/data/                  # JSON + CSV estáticos que consume el front
│   └── src/modules/flash/            # React — página, filtros, mapa (fetch estático)
└── .github/workflows/
    └── kobo_update.yml               # GitHub Actions (cron L-V cada hora)
```

Scripts deprecados (ya no corren en el pipeline, quedan por si se necesitan):
`main_act_flash.py`, `enriquecer_base.py`, `reclasificar_historico.py`, `reclasificar_duplas.py`.

---

## Variables de entorno / Secrets

| Variable | Dónde |
|----------|-------|
| `KOBO_TOKEN_NORTE` | GitHub Secret + `.env` local |
| `KOBO_TOKEN_CENTRO` | GitHub Secret + `.env` local |
| `KOBO_TOKEN_SUR` | GitHub Secret + `.env` local |
| `GOOGLE_CREDENTIALS_JSON` | GitHub Secret + `kobo-looker-connect.json` local |
| `SHEET_DUPLAS` | GitHub Variable (id del sheet de duplas) |
| `VERCEL_TOKEN` | GitHub Secret — **deploy horario desde Actions** |

`DATABASE_URL` ya no se usa (Neon deprecado).

---

## Correr localmente

```bash
# ETL + datos del front
pip install -r requirements.txt
python scripts/actualizar_parquet.py      # Kobo → data/kobo_flash.parquet
python scripts/generar_front_data.py      # parquet → public/data JSON + CSV
python scripts/sync_sheet_duplas.py       # sheet → control_sheet.json

# O todo junto (incluye deploy a Vercel)
./actualizar.sh

# Dashboard
cd dashboard
npm install
npm run dev        # http://localhost:5173
```

---

## Automatización

GitHub Actions ejecuta el pipeline **lunes a viernes, cada hora en el minuto 15**:

1. ETL → parquet canónico
2. Genera JSON/CSV del front
3. Sincroniza sheet de duplas → JSON
4. **Commit diario** de `data/` + `dashboard/public/data/` (snapshot versionado)
5. **Deploy a Vercel** con `VERCEL_TOKEN` (datos frescos cada hora; si no hay token, solo avisa)

También se puede disparar manualmente desde la pestaña **Actions → Run workflow**.
