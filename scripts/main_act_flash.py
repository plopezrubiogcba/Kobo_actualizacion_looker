import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import os
import json
import sys
import re
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- 1. CONFIGURACIÓN GLOBAL ---
load_dotenv()

TOKEN_KOBO = os.environ.get("KOBO_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO_1 = "aH2SygyBTRCkqCgBtu4m3R"  # Flash 1
UID_KOBO_2 = "aPou2eJThDtn45mdmfrbaA"   # Flash 2
UIDS_KOBO = [UID_KOBO_1, UID_KOBO_2]

# GOOGLE SHEETS
NOMBRE_SPREADSHEET = "puntos flash"
NOMBRE_HOJA = "Sheet4"

# NEON POSTGRESQL
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

NEON_TABLE_NAME = 'kobo_flash_consolidado'

# Mapeo tipo_flash (código Kobo) → zona Flash
FLASH_TO_LOCALIZACION = {
    '1': 'C2',   # Flash Recoleta   → Zona C2
    '2': 'C14',  # Flash Palermo    → Zona C14
    '3': 'C13',  # Flash Belgrano   → Zona C13
    '5': 'C12',  # Flash C12        → Zona C12
    '6': 'C1A',  # Flash C1/1A      → Zona C1A
    '7': 'C6',   # Flash Caballito  → Zona C6
    # '4' = Otro → solo GPS
}

# Prioridad de asignación cuando un punto cae en más de un polígono
ZONE_PRIORITY = ['Frontera', 'C2', 'C14', 'C13', 'C12', 'C1A', 'C6']

# Columna del KML que lleva el código de zona
ZONE_COL = 'Mapa Flash'
# El KML usa 'Control'; el resto del stack (DB, dashboard) usa 'C6'
ZONE_RENAME = {'Control': 'C6'}

CRS_METRICO = "EPSG:22185"  # Gauss-Kruger Faja 5, métrico para Buenos Aires
SNAP_BORDE_M = 100          # Otro a <=100m de un borde → zona priorizada más cercana

AR_TZ = 'America/Argentina/Buenos_Aires'

# --- 2. FUNCIONES DE APOYO Y EXTRACCIÓN ---

def obtener_schema_kobo(uid=UID_KOBO_2):
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    url = f"https://kf.kobotoolbox.org/api/v2/assets/{uid}/"
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get('content', {})

def expandir_select_multiple(df, schema, col_name):
    survey = schema.get('survey', [])
    choices = schema.get('choices', [])
    
    field_info = None
    for q in survey:
        xpath = q.get('$xpath', q.get('name', ''))
        if xpath == col_name or q.get('name') == col_name.split('/')[-1]:
            if q.get('type') == 'select_multiple':
                field_info = q
                break
    
    if not field_info:
        return df
    
    list_name = field_info.get('select_from_list_name') or field_info.get('list_name')
    if not list_name:
        return df
    
    relevant_choices = [c for c in choices if c.get('list_name') == list_name]
    for choice in relevant_choices:
        choice_name = choice.get('name')
        col_bool_name = f"{col_name}/{choice_name}"
        df[col_bool_name] = df[col_name].astype(str).str.contains(
            r'\b' + re.escape(choice_name) + r'\b',
            regex=True,
            na=False
        )
    return df

def extraer_kobo_completo(since_timestamp=None, uid=None):
    url_kobo = f"https://kf.kobotoolbox.org/api/v2/assets/{uid}/data.json"
    print(f"\n📋 EXTRACCIÓN DE KOBO — form: {uid}")
    schema = obtener_schema_kobo(uid)
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}

    params = {"limit": 1000}
    if since_timestamp:
        print(f"⏳ Buscando registros posteriores a: {since_timestamp}")
        params["query"] = json.dumps({"_submission_time": {"$gt": since_timestamp}})

    all_results = []
    next_url = url_kobo
    page = 1
    while next_url:
        resp = requests.get(next_url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        results = data.get('results', [])
        all_results.extend(results)
        print(f"  página {page}: {len(results)} registros (total acumulado: {len(all_results)}/{data.get('count', '?')})")
        next_url = data.get('next')
        params = {}  # params ya están en la URL de next
        page += 1

    if not all_results:
        return pd.DataFrame()

    df = pd.json_normalize(all_results)

    for q in schema.get('survey', []):
        if q.get('type') == 'select_multiple':
            field_name = q.get('$xpath', q.get('name'))
            if field_name in df.columns:
                df = expandir_select_multiple(df, schema, field_name)
    return df


def extraer_todos_los_forms(since_timestamp=None):
    dfs = []
    for uid in UIDS_KOBO:
        try:
            df = extraer_kobo_completo(since_timestamp=since_timestamp, uid=uid)
            if not df.empty:
                dfs.append(df)
        except Exception as e:
            print(f"⚠️ Error extrayendo form {uid}: {e}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

# --- 3. LÓGICA DE NEGOCIO Y GEO ---

def asignar_turno(fecha):
    if pd.isnull(fecha): return None
    h = fecha.hour
    if 6 <= h < 14: return "TM"
    elif 14 <= h < 22: return "TT"
    else: return "TN"

def procesar_coords_y_fechas(df):
    if 'geo_ref/geo_punto' in df.columns:
        split_coords = df['geo_ref/geo_punto'].astype(str).str.split(' ', expand=True)
        if split_coords.shape[1] >= 1: df['latitude'] = pd.to_numeric(split_coords[0], errors='coerce')
        if split_coords.shape[1] >= 2: df['longitude'] = pd.to_numeric(split_coords[1], errors='coerce')
        if split_coords.shape[1] >= 3: df['_Georreferenciación del punto_altitude'] = pd.to_numeric(split_coords[2], errors='coerce')
        if split_coords.shape[1] >= 4: df['_Georreferenciación del punto_precision'] = pd.to_numeric(split_coords[3], errors='coerce')
    
    if 'start' in df.columns:
        # utc=True absorbe cualquier offset (Kobo -03:00 o UTC naive sin offset)
        # luego convierte a hora AR y descarta tzinfo para almacenamiento naive
        start_time = pd.to_datetime(df['start'], utc=True, errors='coerce').dt.tz_convert(AR_TZ).dt.tz_localize(None)

        def fecha_reporte_corregida(x):
            if pd.isnull(x): return None
            return (x - pd.Timedelta(days=1)).date() if x.hour < 6 else x.date()

        df['fecha_reporte'] = start_time.apply(fecha_reporte_corregida)
        df['inicio_semana_lunes'] = df['fecha_reporte'].apply(
            lambda d: (d - pd.Timedelta(days=d.weekday())) if d else None
        )

    df['start'] = pd.to_datetime(df['start'], utc=True, errors='coerce').dt.tz_convert(AR_TZ).dt.tz_localize(None)
    if 'end' in df.columns:
        df['end'] = pd.to_datetime(df['end'], utc=True, errors='coerce').dt.tz_convert(AR_TZ).dt.tz_localize(None)
    df['Turno'] = df['start'].apply(asignar_turno)
    return df

def cargar_zonas_flash(ruta):
    """Lee el mapa de zonas (KML u otro formato) y devuelve dict zona→GeoDataFrame (EPSG:4326)."""
    gdf = gpd.read_file(ruta)
    gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")
    gdf[ZONE_COL] = gdf[ZONE_COL].replace(ZONE_RENAME)
    return {z: sub.copy() for z, sub in gdf.groupby(ZONE_COL)}


def clasificar_localizacion(puntos_gdf, zonas_dict, declared_flash=None):
    """Asigna zona Flash a cada punto según prioridad. Zonas: C2>C14>C13>C12>C1A>C6."""
    puntos_gdf = puntos_gdf.to_crs("EPSG:4326")
    puntos_gdf = puntos_gdf.copy()
    puntos_gdf['Localizacion'] = None

    # --- Clasificación base por GPS (prioridad estricta) ---
    for zona_code in ZONE_PRIORITY:
        if zona_code not in zonas_dict:
            continue
        sin_zona = puntos_gdf[puntos_gdf['Localizacion'].isna()]
        if sin_zona.empty:
            break
        zona_gdf = zonas_dict[zona_code].to_crs("EPSG:4326")
        joined = gpd.sjoin(sin_zona, zona_gdf[['geometry']], how="inner", predicate='within')
        if not joined.empty:
            puntos_gdf.loc[joined.index, 'Localizacion'] = zona_code

    puntos_gdf['Localizacion'] = puntos_gdf['Localizacion'].fillna('Otro')

    # --- Override por flash declarado (buffer 100m, CRS métrico) ---
    if declared_flash is not None and declared_flash.notna().any():
        puntos_metric = puntos_gdf.to_crs(CRS_METRICO)
        zonas_metric = {z: gdf.to_crs(CRS_METRICO) for z, gdf in zonas_dict.items()}

        overrides = 0
        for idx, flash_val in declared_flash.items():
            flash_str = str(flash_val) if pd.notna(flash_val) else None
            if flash_str not in FLASH_TO_LOCALIZACION:
                continue  # '4' (Otro) o nulo → mantener GPS

            declared_zone = FLASH_TO_LOCALIZACION[flash_str]
            gps_zone = puntos_gdf.loc[idx, 'Localizacion']

            if gps_zone == declared_zone:
                continue  # coinciden, nada que hacer

            if declared_zone not in zonas_metric:
                continue

            zone_polygon = zonas_metric[declared_zone].union_all() \
                if hasattr(zonas_metric[declared_zone], 'union_all') \
                else zonas_metric[declared_zone].unary_union

            if zone_polygon.buffer(100).contains(puntos_metric.loc[idx, 'geometry']):
                puntos_gdf.loc[idx, 'Localizacion'] = declared_zone
                overrides += 1

        if overrides:
            print(f"  📍 Flash declarado: {overrides} punto(s) reasignado(s) por proximidad al borde (<100m)")

    # --- Snap de borde: Otro a <=SNAP_BORDE_M → zona priorizada más cercana ---
    otro_mask = puntos_gdf['Localizacion'] == 'Otro'
    if otro_mask.any():
        pm = puntos_gdf.to_crs(CRS_METRICO)
        zonas_poly = {z: gdf.to_crs(CRS_METRICO).union_all()
                      for z, gdf in zonas_dict.items() if z in ZONE_PRIORITY}
        snapped = 0
        for idx in puntos_gdf.index[otro_mask]:
            g = pm.loc[idx, 'geometry']
            z_near, poly_near = min(
                zonas_poly.items(),
                key=lambda kv: (g.distance(kv[1]), ZONE_PRIORITY.index(kv[0]))
            )
            if g.distance(poly_near) <= SNAP_BORDE_M:
                puntos_gdf.loc[idx, 'Localizacion'] = z_near
                snapped += 1
        if snapped:
            print(f"  🧲 Snap de borde: {snapped} Otro reasignado(s) a zona <={SNAP_BORDE_M}m")

    return puntos_gdf['Localizacion']

# --- 4. PERSISTENCIA Y ENRIQUECIMIENTO ---

def enrich_existing_data(engine):
    """Completa las columnas fecha_reporte e inicio_semana_lunes para registros existentes usando SQL directo para eficiencia"""
    print("🔍 Enriqueciendo registros en Neon (vía SQL)...")
    
    # SQL para calcular fecha_reporte e inicio_semana_lunes directamente en la base
    # La lógica es: si start.hour < 6, se asigna al día anterior.
    # Para inicio de semana (Lunes): restar (dia_de_la_semana - 1) dias.
    # En Postgres: extract(isodow from date) devuelve 1 para Lunes, 7 para Domingo.
    
    enrich_sql = text(f"""
        UPDATE "{NEON_TABLE_NAME}"
        SET
            "fecha_reporte" = CASE
                WHEN extract(hour from ("start"::timestamp)) < 6
                THEN ("start"::timestamp)::date - interval '1 day'
                ELSE ("start"::timestamp)::date
            END,
            "inicio_semana_lunes" = (
                (CASE WHEN extract(hour from ("start"::timestamp)) < 6
                      THEN ("start"::timestamp)::date - interval '1 day'
                      ELSE ("start"::timestamp)::date END) -
                (extract(isodow from (CASE WHEN extract(hour from ("start"::timestamp)) < 6
                                           THEN ("start"::timestamp)::date - interval '1 day'
                                           ELSE ("start"::timestamp)::date END))::int - 1) * interval '1 day'
            )::date
        WHERE "fecha_reporte" IS NULL OR "inicio_semana_lunes" IS NULL
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(enrich_sql)
            conn.commit()
            print(f"✅ Enriquecimiento completado. Registros afectados: {result.rowcount}")
    except Exception as e:
        print(f"⚠️ Error en enriquecimiento SQL: {e}")
        raise e

def subir_a_neon(df, engine):
    df_neon = df.copy()
    for col in df_neon.columns:
        df_neon[col] = df_neon[col].apply(lambda x: str(x) if isinstance(x, (list, dict)) else x)
    
    df_neon = df_neon.replace([np.inf, -np.inf], np.nan)
    df_neon = df_neon.where(pd.notnull(df_neon), None)
    
    print(f"📤 Subiendo {len(df_neon)} registros a {NEON_TABLE_NAME}...")
    df_neon.to_sql(name=NEON_TABLE_NAME, con=engine, if_exists='append', index=False, method='multi')

# --- 5. MAIN ---

def main():
    print(">>> INICIO DE PROCESO <<<")
    
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL no configurada.")
        return

    engine = create_engine(DATABASE_URL)
    
    # 1. Preparar conexión
    engine = create_engine(DATABASE_URL)

    # 2. Obtener momento de corte (último registro en Neon)
    ultimo_timestamp = None
    try:
        with engine.connect() as conn:
            res = conn.execute(text(f'SELECT MAX("_submission_time") FROM "{NEON_TABLE_NAME}"'))
            ultimo_timestamp = res.scalar()
    except Exception as e:
        print(f"ℹ️ No se pudo obtener el último timestamp (posible tabla vacía): {e}")

    # 3. Extraer datos nuevos de Kobo (Flash 1 + Flash 2)
    df_raw = extraer_todos_los_forms(since_timestamp=ultimo_timestamp)
    if df_raw.empty:
        print("✅ Todo actualizado. No hay registros nuevos en Kobo.")
        return

    # 4. Refuerzo de seguridad: Filtrar por IDs existentes (doble chequeo)
    with engine.connect() as conn:
        existentes = pd.read_sql(text(f'SELECT "_uuid" FROM "{NEON_TABLE_NAME}"'), conn)['_uuid'].astype(str).tolist()
    
    df_nuevos = df_raw[~df_raw['_uuid'].astype(str).isin(existentes)].copy()
    if df_nuevos.empty:
        print("✅ Todo actualizado. No hay registros nuevos (post-filtro UUID).")
        return

    print(f"📝 Procesando {len(df_nuevos)} registros nuevos...")
    
    # 4. Procesar y clasificar
    df_nuevos = procesar_coords_y_fechas(df_nuevos)
    df_nuevos.dropna(subset=['latitude', 'longitude'], inplace=True)
    
    # Cargar capas Geo (KML de zonas Flash)
    PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ruta_kml = os.path.join(PROJ_ROOT, "Zonas flash.kml")

    if os.path.exists(ruta_kml):
        zonas_dict = cargar_zonas_flash(ruta_kml)
        puntos_gdf = gpd.GeoDataFrame(df_nuevos, geometry=gpd.points_from_xy(df_nuevos.longitude, df_nuevos.latitude), crs="EPSG:4326")
        declared_flash = df_nuevos.get('geo_ref/relevamiento_flash')
        df_nuevos['Localizacion'] = clasificar_localizacion(puntos_gdf, zonas_dict, declared_flash)
    else:
        print("⚠️ No se encontró 'Zonas flash.kml'. Saltando clasificación.")

    # 5. Formateo y Subida
    df_nuevos['hora_start'] = df_nuevos['start'].dt.strftime('%H:%M:%S')
    # Mantenemos el timestamp completo para 'start' para evitar desfasajes en enriquecimiento SQL
    df_nuevos['start'] = df_nuevos['start'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Renombrar para compatibilidad (Sheets/Neon)
    rename_map = {
        'geo_ref/geo_punto': 'Georreferenciación del punto',
        'datos_per/cant_pers': 'Cantidad de personas en situación de calle observadas',
        'caracteristicas_puntos/caracteristicas_observada': 'Características observables del punto',
        'caracteristicas_puntos/NNyA_observa': 'Se observan niños/as en el punto',
        'geo_ref/relevamiento_flash': 'tipo_flash',
        'geo_ref/relevamiento_flash_otro': 'tipo_flash_otro'
    }
    df_nuevos.rename(columns=rename_map, inplace=True)
    
    # Asegurar columnas (incluidas las nuevas)
    columnas_finales = [
        'Turno', 'start', 'hora_start', 'end', 'today', 'username', 'deviceid',
        'Georreferenciación del punto', 'latitude', 'longitude',
        '_Georreferenciación del punto_altitude', '_Georreferenciación del punto_precision',
        'Cantidad de personas en situación de calle observadas','La/s persona/s esta/n',
        'Características observables del punto', 'Se observan niños/as en el punto',
        'datos_per/sit_calle', 'fecha_reporte', 'inicio_semana_lunes',
        '_id', '_uuid', '_submission_time', '_status', '_submitted_by', 'Localizacion',
        'tipo_flash', 'tipo_flash_otro'
    ]
    # Usar las que existan en el df para evitar reindex con NaNs innecesarios si no vienen
    cols_presentes = [c for c in columnas_finales if c in df_nuevos.columns]
    df_final = df_nuevos[cols_presentes]

    try:
        subir_a_neon(df_final, engine)
        print("✅ Subida a Neon exitosa.")
        
        # 6. CIERRE DE SEGURIDAD: Enriquecer datos (asegura consistencia de lo recién subido)
        try:
            enrich_existing_data(engine)
        except Exception as e:
            print(f"⚠️ Error en enriquecimiento final: {e}")
            
    except Exception as e:
        print(f"❌ Error subiendo a Neon: {e}")
        sys.exit(1)

    # 6. Google Sheets (Opcional si hay credenciales)
    # Preservation of the logic as requested
    try:
        # Intento de conexión simplificado
        possible_creds = ['kobo-looker-connect.json', 'credenciales.json']
        ruta_creds = next((os.path.join(root, f) for root, _, files in os.walk(PROJ_ROOT) for f in files if f in possible_creds), None)
        
        if ruta_creds:
            scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(ruta_creds, scope)
            client = gspread.authorize(creds)
            sheet = client.open(NOMBRE_SPREADSHEET).worksheet(NOMBRE_HOJA)
            
            # Formatear para Sheets (sin NaNs)
            df_sheets = df_final.fillna("").astype(str)
            sheet.append_rows(df_sheets.values.tolist(), value_input_option='USER_ENTERED')
            print("✅ Datos sincronizados con Google Sheets.")
        else:
            print("ℹ️ No se encontraron credenciales de Google Sheets. Saltando paso.")
    except Exception as e:
        print(f"⚠️ Error en Google Sheets: {e}")

    print(">>> FIN DE PROCESO <<<")

if __name__ == "__main__":
    main()
