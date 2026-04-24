import sys
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import zipfile
import requests
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

TOKEN_KOBO = os.environ.get("KOBO_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO   = "aH2SygyBTRCkqCgBtu4m3R"

FLASH_TO_LOC  = {'1': 2.0, '2': 14.5, '3': 13.0}
FLASH_LABEL   = {'1': 'Flash Recoleta', '2': 'Flash Palermo', '3': 'Flash Belgrano'}
LOC_LABEL     = {2.0: 'Comuna 2 (Recoleta)', 14.5: 'Palermo Norte', 13.0: 'Comuna 13 (Belgrano)'}
FLASH_COMUNAS = {2, 13, 14, 15}
COLORES_COMUNAS = {2: '#dbe4ff', 13: '#d3f9d8', 14: '#fff3bf', 15: '#ffe8cc'}
NOMBRES_COMUNAS = {2: 'Recoleta', 13: 'Belgrano', 14: 'Palermo', 15: 'Paternal'}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── 1. Neon ───────────────────────────────────────────────────────────────────
print("📦 Leyendo registros con tipo_flash desde Neon...")
engine = create_engine(DATABASE_URL, connect_args={"sslmode": "require"})
with engine.connect() as conn:
    df = pd.read_sql(text('''
        SELECT _uuid, fecha_reporte, tipo_flash,
               "Localizacion", latitude, longitude, username,
               "Cantidad de personas en situación de calle observadas" as personas
        FROM kobo_flash_consolidado
        WHERE tipo_flash IS NOT NULL AND tipo_flash != '4'
        ORDER BY fecha_reporte DESC
    '''), conn)
print(f"   {len(df)} registros.")

# ── 2. Kobo (DNI) ─────────────────────────────────────────────────────────────
print("🔑 Buscando DNI en Kobo...")
headers = {"Authorization": f"Token {TOKEN_KOBO}"}
resp = requests.get(
    f"https://kf.kobotoolbox.org/api/v2/assets/{UID_KOBO}/data.json",
    headers=headers,
    params={"limit": 5000,
            "query": '{"_submission_time": {"$gte": "2026-03-17T00:00:00"}}',
            "fields": '["_uuid","datos_per/dni_operador"]'}
)
resp.raise_for_status()
dni_map = {r['_uuid']: r.get('datos_per/dni_operador', 'Sin DNI')
           for r in resp.json().get('results', [])}
df['dni'] = df['_uuid'].map(lambda u: dni_map.get(u, 'Sin DNI'))
print(f"   DNI cruzado para {len(df)} registros.")

# ── 3. Calcular coincidencia ──────────────────────────────────────────────────
df['loc_esp']      = df['tipo_flash'].map(FLASH_TO_LOC)
df['Localizacion'] = pd.to_numeric(df['Localizacion'], errors='coerce')
# Flash Palermo (tipo 2): 14.0 (toda la comuna) y 14.5 (Palermo Norte) ambos son correctos
df['coincide'] = df.apply(
    lambda r: r['Localizacion'] in (14.0, 14.5) if r['tipo_flash'] == '2'
              else r['loc_esp'] == r['Localizacion'], axis=1)
df['flash_label']  = df['tipo_flash'].map(FLASH_LABEL)
df['loc_gps_str']  = df['Localizacion'].map(
    lambda x: LOC_LABEL.get(x, f'Comuna {int(x)}' if pd.notna(x) else 'Sin clasificar'))
df['personas']     = pd.to_numeric(df['personas'], errors='coerce')
df['pers_str']     = df['personas'].apply(
    lambda x: f"{int(x)} persona{'s' if x != 1 else ''}" if pd.notna(x) and x <= 100
              else ('Sin dato' if pd.isna(x) else 'Dato inválido'))

total       = len(df)
correctos   = int(df['coincide'].sum())
incorrectos = total - correctos
fecha_str   = str(df['fecha_reporte'].max())[:10]

# ── 4. Capas geo → GeoJSON ────────────────────────────────────────────────────
print("🗺️  Cargando capas geográficas...")
ruta_kmz = next((os.path.join(r, f) for r, _, fs in os.walk(BASE_DIR)
                 for f in fs if 'palermo' in f.lower() and f.endswith('.kmz')), None)
ruta_shp = next((os.path.join(r, f) for r, _, fs in os.walk(BASE_DIR)
                 for f in fs if f == 'comunas.shp'), None)

palermo_geojson = 'null'
comunas_geojson = 'null'

if ruta_kmz:
    with zipfile.ZipFile(ruta_kmz, 'r') as kmz:
        kml_name = [f for f in kmz.namelist() if f.endswith('.kml')][0]
        with kmz.open(kml_name) as kml:
            pg = gpd.read_file(kml).to_crs("EPSG:4326")
    palermo_geojson = pg.to_json()

if ruta_shp:
    cg = gpd.read_file(ruta_shp).to_crs("EPSG:4326")
    possible_cols = ['comunas', 'COMUNAS', 'comuna', 'COMUNA', 'NAM', 'ID', 'OBJETO']
    ccol = next((c for c in possible_cols if c in cg.columns), None)
    if ccol:
        cg = cg[pd.to_numeric(cg[ccol], errors='coerce').isin(FLASH_COMUNAS)].copy()
        cg['_num']    = pd.to_numeric(cg[ccol], errors='coerce').astype(int)
        cg['_color']  = cg['_num'].map(COLORES_COMUNAS)
        cg['_nombre'] = cg['_num'].map(NOMBRES_COMUNAS)
    comunas_geojson = cg.to_json()

# ── 5. Puntos como JSON ───────────────────────────────────────────────────────
puntos = []
for _, r in df.iterrows():
    if pd.isna(r['latitude']) or pd.isna(r['longitude']):
        continue
    puntos.append({
        'lat':      float(r['latitude']),
        'lng':      float(r['longitude']),
        'coincide': bool(r['coincide']),
        'flash':    r['flash_label'],
        'gps':      r['loc_gps_str'],
        'dni':      str(r['dni']),
        'personas': r['pers_str'],
        'fecha':    str(r['fecha_reporte'])[:10],
    })
puntos_json = json.dumps(puntos, ensure_ascii=False)

# ── 6. Generar HTML ───────────────────────────────────────────────────────────
print("🖌️  Generando HTML...")

html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Discrepancias Flash — {fecha_str}</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    font-family: 'DM Sans', sans-serif;
    background: #f1f3f5;
    height: 100vh;
    overflow: hidden;
  }}

  #map {{
    position: absolute;
    inset: 0;
    z-index: 0;
  }}

  /* ── HEADER FLOTANTE CENTRADO ── */
  #header {{
    position: fixed;
    top: 14px;
    left: 50%;
    transform: translateX(-50%);
    z-index: 1000;
    background: #fff;
    border-radius: 10px;
    box-shadow: 0 2px 16px rgba(0,0,0,0.13);
    padding: 10px 20px;
    display: flex;
    align-items: center;
    gap: 14px;
    white-space: nowrap;
  }}

  .h-title {{
    font-weight: 700;
    font-size: 15px;
    color: #212529;
    letter-spacing: -.3px;
  }}
  .h-sub {{
    font-size: 12px;
    color: #adb5bd;
  }}
  .h-divider {{
    width: 1px; height: 22px;
    background: #e9ecef;
    flex-shrink: 0;
  }}

  /* badges clickeables */
  .h-badge {{
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    font-weight: 600;
    padding: 5px 13px;
    border-radius: 99px;
    cursor: pointer;
    border: 1.5px solid transparent;
    transition: opacity .15s;
    user-select: none;
  }}
  .h-badge .dot {{ width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }}
  .h-badge.ok  {{ background: #ebfbee; color: #2f9e44; border-color: #b2f2bb; }}
  .h-badge.err {{ background: #fff5f5; color: #e03131; border-color: #ffc9c9; }}
  .h-badge.ok  .dot {{ background: #2f9e44; }}
  .h-badge.err .dot {{ background: #e03131; }}
  .h-badge.off {{ opacity: .35; }}
  .h-badge:hover {{ opacity: .75; }}

  /* ── PANEL DERECHO: buscador ── */
  #side-panel {{
    position: fixed;
    top: 14px;
    right: 14px;
    z-index: 1000;
  }}

  .search-box {{
    background: #fff;
    border-radius: 8px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.11);
    padding: 7px 12px;
    display: flex;
    align-items: center;
    gap: 7px;
  }}
  .search-box svg {{ color: #adb5bd; flex-shrink: 0; }}
  .search-box input {{
    border: none; outline: none;
    font-family: 'DM Mono', monospace;
    font-size: 12px;
    color: #212529;
    width: 140px;
    background: transparent;
  }}
  .search-box input::placeholder {{ color: #ced4da; }}
  #dni-count {{
    font-family: 'DM Mono', monospace;
    font-size: 11px;
    color: #4263eb;
    min-width: 16px;
  }}

  /* ── POPUP ── */
  .leaflet-popup-content-wrapper {{
    border-radius: 10px !important;
    box-shadow: 0 4px 24px rgba(0,0,0,0.13) !important;
    padding: 0 !important;
    overflow: hidden;
    border: 1px solid #e9ecef !important;
  }}
  .leaflet-popup-tip-container {{ display: none; }}
  .leaflet-popup-content {{ margin: 0 !important; min-width: 230px; }}
  .leaflet-popup-close-button {{
    top: 8px !important; right: 10px !important;
    color: #adb5bd !important; font-size: 18px !important;
    font-weight: 300 !important;
  }}

  .pop-head {{
    padding: 10px 32px 10px 14px;
    font-weight: 700;
    font-size: 13px;
    display: flex;
    align-items: center;
    gap: 6px;
    border-bottom: 1px solid #f1f3f5;
  }}
  .pop-head.ok  {{ color: #2f9e44; background: #f4fce3; }}
  .pop-head.err {{ color: #e03131; background: #fff5f5; }}

  .pop-body {{ padding: 10px 14px 12px; background: #fff; }}
  .pop-row {{
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 4px 0;
    font-size: 12px;
    gap: 16px;
    border-bottom: 1px solid #f8f9fa;
  }}
  .pop-row:last-child {{ border-bottom: none; }}
  .pop-row .lbl {{
    color: #868e96;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .4px;
    white-space: nowrap;
    flex-shrink: 0;
  }}
  .pop-row .val {{
    font-weight: 600;
    font-family: 'DM Mono', monospace;
    font-size: 11.5px;
    color: #212529;
    text-align: right;
  }}
  .pop-row .val.err {{ color: #e03131; }}

  /* ── Zoom control ── */
  .leaflet-control-zoom {{
    border: none !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.12) !important;
    border-radius: 8px !important;
    overflow: hidden;
  }}
  .leaflet-control-zoom a {{
    background: #fff !important;
    color: #495057 !important;
    border-bottom: 1px solid #f1f3f5 !important;
    width: 32px !important; height: 32px !important; line-height: 32px !important;
    font-size: 16px !important; font-weight: 400 !important;
  }}
  .leaflet-control-zoom a:hover {{ background: #f8f9fa !important; color: #212529 !important; }}

  /* ── Tooltip ── */
  .leaflet-tooltip {{
    background: #fff !important;
    border: 1px solid #e9ecef !important;
    border-radius: 7px !important;
    color: #212529 !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 12px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.09) !important;
    padding: 5px 9px !important;
  }}
</style>
</head>
<body>

<div id="header">
  <div>
    <span class="h-title">Discrepancias Flash</span>
    <span class="h-sub">&nbsp;·&nbsp;{fecha_str}</span>
  </div>
  <div class="h-divider"></div>
  <div class="h-badge ok" id="btn-ok" onclick="toggleFiltro('ok')" title="Mostrar/ocultar correctos">
    <span class="dot"></span>{correctos} correctos
  </div>
  <div class="h-badge err" id="btn-err" onclick="toggleFiltro('err')" title="Mostrar/ocultar fuera">
    <span class="dot"></span>{incorrectos} fuera
  </div>
  <div class="h-divider"></div>
  <span class="h-sub">click para detalle</span>
</div>

<div id="side-panel">
  <div class="search-box">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2">
      <circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/>
    </svg>
    <input type="text" id="dni-input" placeholder="Buscar DNI…" oninput="actualizar()"/>
    <span id="dni-count"></span>
  </div>
</div>

<div id="map"></div>

<script>
var showOk  = true;
var showErr = true;

function toggleFiltro(tipo) {{
  if (tipo === 'ok') {{
    showOk = !showOk;
    document.getElementById('btn-ok').classList.toggle('off', !showOk);
  }} else {{
    showErr = !showErr;
    document.getElementById('btn-err').classList.toggle('off', !showErr);
  }}
  actualizar();
}}

// ── Mapa base ──
var map = L.map('map', {{ zoomControl: true, preferCanvas: true }})
  .setView([-34.597, -58.437], 14);

L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
  attribution: '&copy; OpenStreetMap &copy; CartoDB', maxZoom: 20
}}).addTo(map);

// ── Comunas ──
var comunasData = {comunas_geojson};
if (comunasData) {{
  L.geoJSON(comunasData, {{
    style: function(f) {{
      return {{
        fillColor: f.properties._color || '#f1f3f5',
        color: '#ced4da', weight: 1.5, fillOpacity: 0.35
      }};
    }},
    onEachFeature: function(f, layer) {{
      layer.bindTooltip(
        '<b>Comuna ' + f.properties._num + '</b> — ' + (f.properties._nombre || ''),
        {{ sticky: true }}
      );
    }}
  }}).addTo(map);
}}

// ── Palermo Norte ──
var palermoData = {palermo_geojson};
if (palermoData) {{
  L.geoJSON(palermoData, {{
    style: {{
      fillColor: '#4263eb', color: '#4263eb',
      weight: 2, fillOpacity: 0.07, dashArray: '6 4'
    }}
  }}).bindTooltip('<b>Polígono Palermo Norte</b>', {{ sticky: true }}).addTo(map);
}}

// ── Puntos ──
var puntosData = {puntos_json};
var markers = [];

puntosData.forEach(function(d) {{
  var ok     = d.coincide;
  var fill   = ok ? '#2f9e44' : '#e03131';
  var border = ok ? '#2b8a3e' : '#c92a2a';

  var popupHtml =
    '<div class="pop-head ' + (ok ? 'ok' : 'err') + '">'
    + (ok ? '✓ Correcto' : '✗ Fuera del flash')
    + '</div>'
    + '<div class="pop-body">'
    + '<div class="pop-row"><span class="lbl">Flash decl.</span><span class="val">' + d.flash + '</span></div>'
    + '<div class="pop-row"><span class="lbl">GPS en</span><span class="val ' + (ok ? '' : 'err') + '">' + d.gps + '</span></div>'
    + '<div class="pop-row"><span class="lbl">DNI</span><span class="val">' + d.dni + '</span></div>'
    + '<div class="pop-row"><span class="lbl">Personas</span><span class="val">' + d.personas + '</span></div>'
    + '<div class="pop-row"><span class="lbl">Fecha</span><span class="val">' + d.fecha + '</span></div>'
    + '</div>';

  var m = L.circleMarker([d.lat, d.lng], {{
    radius: ok ? 8 : 9,
    color: border, weight: 1.5,
    fillColor: fill, fillOpacity: 0.82
  }})
  .bindPopup(popupHtml, {{ maxWidth: 260, closeButton: true }})
  .bindTooltip(
    (ok ? '✓' : '✗') + ' <b>' + d.dni + '</b> · ' + d.flash
    + '<br><span style="color:#868e96">GPS: ' + d.gps + ' · ' + d.personas + '</span>',
    {{ sticky: true }}
  )
  .addTo(map);

  m._dni      = d.dni;
  m._coincide = d.coincide;
  markers.push(m);
}});

// ── Fit bounds automático ──
if (markers.length) {{
  map.fitBounds(L.featureGroup(markers).getBounds().pad(0.12));
}}

// ── Filtro ──
function actualizar() {{
  var v = (document.getElementById('dni-input').value || '').trim();
  var n = 0;
  markers.forEach(function(m) {{
    var visible = (m._coincide ? showOk : showErr) && (!v || m._dni.includes(v));
    m.setStyle({{ opacity: visible ? 1 : 0, fillOpacity: visible ? 0.82 : 0 }});
    if (visible) n++;
  }});
  document.getElementById('dni-count').textContent = v ? n + '' : '';
}}
</script>
</body>
</html>"""

salida = os.path.join(BASE_DIR, 'mapa_discrepancias.html')
with open(salida, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"✅ Mapa guardado en: {salida}")

# ── Resumen consola ───────────────────────────────────────────────────────────
print()
print("=== DISCREPANCIAS POR OPERADOR ===")
disc = df[~df['coincide']].copy()
if disc.empty:
    print("Sin discrepancias.")
else:
    resumen = (
        disc.groupby(['dni', 'flash_label', 'loc_gps_str'])
        .size().reset_index(name='cantidad')
        .sort_values('cantidad', ascending=False)
    )
    print(resumen.to_string(index=False))