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
import zipfile
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- 1. CONFIGURACIÓN GLOBAL ---
load_dotenv()

TOKEN_KOBO = os.environ.get("KOBO_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
URL_KOBO = f"https://kf.kobotoolbox.org/api/v2/assets/{UID_KOBO}/data.json"

# GOOGLE SHEETS
NOMBRE_SPREADSHEET = "puntos flash"
NOMBRE_HOJA = "Sheet4"

# NEON POSTGRESQL
DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

NEON_TABLE_NAME = 'kobo_flash_consolidado'

# Mapeo flash declarado (código Kobo) → Localizacion
FLASH_TO_LOCALIZACION = {
    '1': 2,    # Flash Recoleta  → Comuna 2
    '2': 14.5, # Flash Palermo Norte → polígono priorizado (dentro de comuna 14)
    '3': 13,   # Flash Belgrano  → Comuna 13
    # '4' = Otro → solo GPS
}
CRS_METRICO = "EPSG:22185"  # Gauss-Kruger Faja 5, métrico para Buenos Aires

# --- 2. FUNCIONES DE APOYO Y EXTRACCIÓN ---

def obtener_schema_kobo():
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    url = f"https://kf.kobotoolbox.org/api/v2/assets/{UID_KOBO}/"
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

def extraer_kobo_completo(since_timestamp=None):
    print("\n📋 EXTRACCIÓN DE KOBO")
    schema = obtener_schema_kobo()
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    
    params = {"limit": 30000}
    if since_timestamp:
        # Kobo API v2 usa el parámetro 'query' con sintaxis MongoDB para filtrar
        # {"_submission_time": {"$gt": "2026-02-18T10:00:00"}}
        print(f"⏳ Buscando registros posteriores a: {since_timestamp}")
        params["query"] = json.dumps({"_submission_time": {"$gt": since_timestamp}})

    resp = requests.get(URL_KOBO, headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    
    if not data.get('results'):
        return pd.DataFrame()
        
    df = pd.json_normalize(data['results'])
    
    for q in schema.get('survey', []):
        if q.get('type') == 'select_multiple':
            field_name = q.get('$xpath', q.get('name'))
            if field_name in df.columns:
                df = expandir_select_multiple(df, schema, field_name)
    return df

# --- 3. LÓGICA DE NEGOCIO Y GEO ---

def asignar_turno(fecha):
    if pd.isnull(fecha): return None
    h = fecha.hour
    if 3 <= h < 8: return "TM"
    elif 8 <= h < 16: return "TO"
    elif 16 <= h < 22: return "TT"
    else: return "TN"

def procesar_coords_y_fechas(df):
    if 'geo_ref/geo_punto' in df.columns:
        split_coords = df['geo_ref/geo_punto'].astype(str).str.split(' ', expand=True)
        if split_coords.shape[1] >= 1: df['latitude'] = pd.to_numeric(split_coords[0], errors='coerce')
        if split_coords.shape[1] >= 2: df['longitude'] = pd.to_numeric(split_coords[1], errors='coerce')
        if split_coords.shape[1] >= 3: df['_Georreferenciación del punto_altitude'] = pd.to_numeric(split_coords[2], errors='coerce')
        if split_coords.shape[1] >= 4: df['_Georreferenciación del punto_precision'] = pd.to_numeric(split_coords[3], errors='coerce')
    
    if 'start' in df.columns:
        # Usamos 'start' (momento del evento) para mayor fiabilidad operativa
        start_time = pd.to_datetime(df['start'])
        
        # Si es madrugada TN (0-2hs), el reporte pertenece al día anterior
        def fecha_reporte_corregida(x):
            d = (x - pd.Timedelta(days=1)).date() if x.hour < 3 else x.date()
            return d

        df['fecha_reporte'] = start_time.apply(fecha_reporte_corregida)
        df['inicio_semana_lunes'] = df['fecha_reporte'].apply(
            lambda d: (d - pd.Timedelta(days=d.weekday()))
        )
        
    df['start'] = pd.to_datetime(df['start'])
    df['Turno'] = df['start'].apply(asignar_turno)
    return df

def clasificar_localizacion(puntos_gdf, palermo_gdf, comunas_gdf, declared_flash=None):
    puntos_gdf = puntos_gdf.to_crs("EPSG:4326")
    palermo_gdf = palermo_gdf.to_crs("EPSG:4326")
    comunas_gdf = comunas_gdf.to_crs("EPSG:4326")
    puntos_gdf['Localizacion'] = None

    # --- Clasificación base por GPS ---
    puntos_en_palermo = gpd.sjoin(puntos_gdf, palermo_gdf, how="inner", predicate='within')
    if not puntos_en_palermo.empty:
        puntos_gdf.loc[puntos_en_palermo.index, 'Localizacion'] = 14.5

    mask_clasificados = puntos_gdf['Localizacion'].notna()
    puntos_para_comunas = puntos_gdf[~mask_clasificados]

    if not puntos_para_comunas.empty:
        puntos_en_comunas = gpd.sjoin(puntos_para_comunas, comunas_gdf, how="inner", predicate='within')
        if not puntos_en_comunas.empty:
            possible_cols = ['comunas', 'COMUNAS', 'comuna', 'COMUNA', 'NAM', 'ID', 'OBJETO']
            comuna_col = next((c for c in possible_cols if c in puntos_en_comunas.columns), None)
            if comuna_col:
                puntos_gdf.loc[puntos_en_comunas.index, 'Localizacion'] = pd.to_numeric(puntos_en_comunas[comuna_col], errors='coerce')

    # --- Override por flash declarado (solo registros con el campo completo) ---
    if declared_flash is not None and declared_flash.notna().any():
        puntos_metric = puntos_gdf.to_crs(CRS_METRICO)
        palermo_metric = palermo_gdf.to_crs(CRS_METRICO)
        comunas_metric = comunas_gdf.to_crs(CRS_METRICO)

        possible_cols = ['comunas', 'COMUNAS', 'comuna', 'COMUNA', 'NAM', 'ID', 'OBJETO']
        comuna_col = next((c for c in possible_cols if c in comunas_metric.columns), None)

        overrides = 0
        for idx, flash_val in declared_flash.items():
            flash_str = str(flash_val) if pd.notna(flash_val) else None
            if flash_str not in FLASH_TO_LOCALIZACION:
                continue  # '4' (Otro) o nulo → mantener GPS

            declared_loc = FLASH_TO_LOCALIZACION[flash_str]
            gps_loc = puntos_gdf.loc[idx, 'Localizacion']

            if gps_loc == declared_loc:
                continue  # GPS y declaración coinciden, nada que hacer

            # Obtener polígono de la zona declarada en CRS métrico
            if declared_loc == 14.5:
                zone_polygon = palermo_metric.union_all() if hasattr(palermo_metric, 'union_all') else palermo_metric.unary_union
            elif comuna_col:
                zone_rows = comunas_metric[pd.to_numeric(comunas_metric[comuna_col], errors='coerce') == declared_loc]
                if zone_rows.empty:
                    continue
                zone_polygon = zone_rows.union_all() if hasattr(zone_rows, 'union_all') else zone_rows.unary_union
            else:
                continue

            # Si el punto cae dentro del buffer de 100m del borde → override
            if zone_polygon.buffer(100).contains(puntos_metric.loc[idx, 'geometry']):
                puntos_gdf.loc[idx, 'Localizacion'] = declared_loc
                overrides += 1

        if overrides:
            print(f"  📍 Flash declarado: {overrides} punto(s) reasignado(s) por proximidad al borde (<100m)")

    return puntos_gdf['Localizacion']

# --- 4. PERSISTENCIA Y ENRIQUECIMIENTO ---

def enrich_existing_data(engine):
    """Completa las columnas fecha_reporte e inicio_semana_lunes para registros existentes usando SQL directo para eficiencia"""
    print("🔍 Enriqueciendo registros en Neon (vía SQL)...")
    
    # SQL para calcular fecha_reporte e inicio_semana_lunes directamente en la base
    # La lógica es: restar 3 horas a _submission_time, luego truncar a fecha.
    # Para inicio de semana (Lunes): restar (dia_de_la_semana - 1) dias.
    # En Postgres: extract(isodow from date) devuelve 1 para Lunes, 7 para Domingo.
    
    enrich_sql = text(f"""
        UPDATE "{NEON_TABLE_NAME}"
        SET 
            "fecha_reporte" = ("start"::timestamp)::date,
            "inicio_semana_lunes" = (
                ("start"::timestamp)::date - 
                (extract(isodow from ("start"::timestamp)::date)::int - 1) * interval '1 day'
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

    # 3. Extraer datos nuevos de Kobo (filtrados por tiempo si es posible)
    df_raw = extraer_kobo_completo(since_timestamp=ultimo_timestamp)
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
    
    # Cargar capas Geo
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    ruta_kmz = next((os.path.join(root, f) for root, _, files in os.walk(BASE_DIR) for f in files if 'palermo' in f.lower() and f.endswith('.kmz')), None)
    ruta_shp = next((os.path.join(root, f) for root, _, files in os.walk(BASE_DIR) for f in files if f == 'comunas.shp'), None)
    
    if ruta_kmz and ruta_shp:
        with zipfile.ZipFile(ruta_kmz, 'r') as kmz:
            kml_name = [f for f in kmz.namelist() if f.endswith('.kml')][0]
            with kmz.open(kml_name) as kml:
                palermo_gdf = gpd.read_file(kml)
        comunas_gdf = gpd.read_file(ruta_shp)
        
        puntos_gdf = gpd.GeoDataFrame(df_nuevos, geometry=gpd.points_from_xy(df_nuevos.longitude, df_nuevos.latitude), crs="EPSG:4326")
        # Pasar flash declarado si existe (campo nuevo desde 2026-03-17, NULL en histórico)
        declared_flash = df_nuevos.get('geo_ref/relevamiento_flash')
        df_nuevos['Localizacion'] = clasificar_localizacion(puntos_gdf, palermo_gdf, comunas_gdf, declared_flash)
    else:
        print("⚠️ No se encontraron archivos geográficos. Saltando clasificación.")

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
        ruta_creds = next((os.path.join(root, f) for root, _, files in os.walk(BASE_DIR) for f in files if f in possible_creds), None)
        
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
