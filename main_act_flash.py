import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import json
import sys

# --- 1. CONFIGURACIÓN GLOBAL ---

# KOBO
TOKEN_KOBO = os.environ.get("KOBO_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
URL_KOBO = f"https://kf.kobotoolbox.org/api/v2/assets/{UID_KOBO}/data.json"

# GOOGLE SHEETS
NOMBRE_SPREADSHEET = "puntos flash"
NOMBRE_HOJA = "Sheet4"

# URL COMUNAS (Respaldo estable en GitHub - Formato GeoJSON)
URL_COMUNAS_GEOJSON = "https://raw.githubusercontent.com/macagua/example.data.geojson/master/comunas.geojson"

# CONFIGURACIÓN DE RUTAS (Buscador automático para el KML local)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_KML_ANILLO = None

print(f"--- Configurando rutas en: {BASE_DIR} ---")

# Buscamos el KML recursivamente
for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if 'recoleta' in file.lower() and file.lower().endswith('.kml'):
            RUTA_KML_ANILLO = os.path.join(root, file)
            print(f"   > KML encontrado: {RUTA_KML_ANILLO}")
            break

if not RUTA_KML_ANILLO:
    print("\nERROR CRÍTICO: No se encontró el archivo KML 'Recoleta...'.")
    print("Asegúrate de que el archivo .kml esté subido en el GitHub.")
    sys.exit(1)


# --- 2. FUNCIONES DE LÓGICA DE NEGOCIO ---

def asignar_turno(fecha):
    if pd.isnull(fecha): return None
    h = fecha.hour
    if 3 <= h < 8: return "TM"
    elif 8 <= h < 16: return "TO"
    elif 16 <= h < 22: return "TT"
    elif h >= 22 or h < 3: return "TN"
    else: return None

def clasificar_localizacion(puntos_gdf, anillo_gdf, comunas_gdf):
    print("--- Clasificando Localización ---")
    # Asegurar CRS
    puntos_gdf = puntos_gdf.to_crs("EPSG:4326")
    anillo_gdf = anillo_gdf.to_crs("EPSG:4326")
    comunas_gdf = comunas_gdf.to_crs("EPSG:4326")

    puntos_gdf['Localizacion'] = 'Fuera de Zona'

    # 1. Anillo Digital
    puntos_en_anillo = gpd.sjoin(puntos_gdf, anillo_gdf, how="inner", predicate='within')
    if not puntos_en_anillo.empty:
        puntos_gdf.loc[puntos_en_anillo.index, 'Localizacion'] = 'AD'

    # 2. Comunas
    puntos_fuera_anillo_idx = puntos_gdf[puntos_gdf['Localizacion'] != 'AD'].index
    puntos_para_comunas = puntos_gdf.loc[puntos_fuera_anillo_idx]

    if not puntos_para_comunas.empty:
        puntos_en_comunas = gpd.sjoin(puntos_para_comunas, comunas_gdf, how="inner", predicate='within')
        if not puntos_en_comunas.empty:
            # Buscamos variantes comunes del nombre de la columna en el GeoJSON
            col_comuna = next((c for c in ['COMUNAS', 'comunas', 'DOM_COMUNA', 'comuna', 'barrio', 'BARRIO'] if c in puntos_en_comunas.columns), None)
            if col_comuna:
                valores = puntos_en_comunas[col_comuna].fillna(0).astype(str)
                puntos_gdf.loc[puntos_en_comunas.index, 'Localizacion'] = valores
            else:
                print(f"Advertencia: Columnas disponibles en Comunas: {comunas_gdf.columns}")
    
    return puntos_gdf['Localizacion']

def asignar_recorrido(gdf, poligonos):
    print("--- Clasificando Recorridos ---")
    resultado = pd.Series('', index=gdf.index, dtype=object)
    for nombre, poligono in poligonos.items():
        dentro = gdf.within(poligono)
        if dentro.any():
            resultado.loc[dentro] = nombre
    return resultado

def procesar_datos_geoespaciales(df_kobo):
    # 1. Parseo Lat/Lon
    print("Separando coordenadas latitud/longitud...")
    if 'geo_ref/geo_punto' in df_kobo.columns:
        split_coords = df_kobo['geo_ref/geo_punto'].astype(str).str.split(' ', expand=True)
        if split_coords.shape[1] >= 2:
            df_kobo['latitude'] = pd.to_numeric(split_coords[0], errors='coerce')
            df_kobo['longitude'] = pd.to_numeric(split_coords[1], errors='coerce')
    
    df_kobo['start'] = pd.to_datetime(df_kobo['start'])
    df_kobo.dropna(subset=['latitude', 'longitude'], inplace=True)
    df_kobo['Turno'] = df_kobo['start'].apply(asignar_turno)

    puntos_gdf = gpd.GeoDataFrame(
        df_kobo,
        geometry=gpd.points_from_xy(df_kobo.longitude, df_kobo.latitude),
        crs="EPSG:4326"
    )

    # --- CARGA DE CAPAS ---
    anillo_gdf = None
    comunas_gdf = None

    # A. Cargar KML (Local) con manejo de errores de capa
    try:
        print(f"Cargando KML: {RUTA_KML_ANILLO}")
        anillo_gdf = gpd.read_file(RUTA_KML_ANILLO, layer='Nuevo Anillo Digital', driver='KML')
    except Exception as e:
        print(f"⚠️ Advertencia KML capa específica: {e}")
        print("Intentando cargar capa por defecto...")
        try:
            anillo_gdf = gpd.read_file(RUTA_KML_ANILLO, driver='KML')
        except Exception as e2:
            print(f"❌ ERROR FATAL KML: {e2}")
            sys.exit(1)

    if anillo_gdf is not None and anillo_gdf.crs is None:
        anillo_gdf.set_crs("EPSG:4326", inplace=True)

    # B. Cargar Comunas (Remoto)
    try:
        print(f"Descargando Comunas de: {URL_COMUNAS_GEOJSON}")
        comunas_gdf = gpd.read_file(URL_COMUNAS_GEOJSON)
    except Exception as e:
        print(f"❌ ERROR FATAL descargando Comunas: {e}")
        sys.exit(1)


    # Polígonos Hardcoded
    poligonos_recorrido = {
        'Recorrido A': Polygon([(-58.41017, -34.588232), (-58.413901, -34.594177), (-58.413904, -34.599714),(-58.400064, -34.600033), (-58.386224, -34.599855), (-58.398154, -34.59498),(-58.404592, -34.593108), (-58.386524, -34.595263), (-58.41017, -34.588232)]),
        'Recorrido B': Polygon([(-58.389185, -34.584593), (-58.395365, -34.587137), (-58.400944, -34.594168),(-58.398154, -34.59498), (-58.386524, -34.595263), (-58.383284, -34.587544),(-58.388112, -34.59256), (-58.389185, -34.584593)]),
        'Recorrido C': Polygon([(-58.400944, -34.594168), (-58.395365, -34.587137), (-58.389185, -34.584593),(-58.398455, -34.580212), (-58.407295, -34.581837), (-58.404592, -34.593108),(-58.41017, -34.588232), (-58.400944, -34.594168)])
    }

    df_kobo['Localizacion'] = clasificar_localizacion(puntos_gdf, anillo_gdf, comunas_gdf)
    df_kobo['Poligono'] = asignar_recorrido(puntos_gdf, poligonos_recorrido)

    return df_kobo


# --- 3. MAIN EJECUCIÓN ---

if __name__ == '__main__':
    print(">>> INICIO DE PROCESO INTEGRADO <<<")
    
    print("1. Conectando a Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    if "GOOGLE_CREDENTIALS_JSON" in os.environ:
        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        # Buscar credenciales locales
        ruta_creds = next((os.path.join(root, 'credenciales.json') for root, _, files in os.walk(BASE_DIR) if 'credenciales.json' in files), 'credenciales.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name(ruta_creds, scope)

    client = gspread.authorize(creds)
    try:
        sheet = client.open(NOMBRE_SPREADSHEET).worksheet(NOMBRE_HOJA)
    except Exception as e:
        print(f"Error abriendo Sheet: {e}")
        sys.exit(1)

    try:
        registros = sheet.get_all_records()
        ids_existentes = set(str(r['_uuid']) for r in registros) if registros and '_uuid' in registros[0] else set()
    except:
        ids_existentes = set()
    
    print(f"   > Registros previos: {len(ids_existentes)}")

    print("2. Descargando Kobo...")
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    try:
        resp = requests.get(URL_KOBO, headers=headers)
        resp.raise_for_status()
        df_raw = pd.json_normalize(resp.json()['results'])
    except Exception as e:
        print(f"Error Kobo: {e}")
        sys.exit(1)

    if df_raw.empty: sys.exit(0)
    
    if '_uuid' in df_raw.columns:
        df_raw['_uuid'] = df_raw['_uuid'].astype(str)
        df_nuevos = df_raw[~df_raw['_uuid'].isin(ids_existentes)].copy()
    else:
        df_nuevos = df_raw

    if df_nuevos.empty:
        print(">>> Todo actualizado. <<<")
        sys.exit(0)

    print(f"   > Procesando {len(df_nuevos)} nuevos...")

    df_procesado = procesar_datos_geoespaciales(df_nuevos)
    if df_procesado is None or df_procesado.empty: sys.exit(1)

    print("3. Formateando...")
    df_procesado['hora_start'] = df_procesado['start'].dt.strftime('%H:%M:%S')
    df_procesado['start'] = df_procesado['start'].dt.strftime('%Y-%m-%d')

    rename_map = {
        'geo_ref/geo_punto': 'Georreferenciación del punto',
        'datos_per/cant_pers': 'Cantidad de personas en situación de calle observadas',
        'caracteristicas_puntos/caracteristicas_observada': 'Características observables del punto',
        'caracteristicas_puntos/estructura': 'estructura',
        'caracteristicas_puntos/NNyA_observa': 'Se observan niños/as en el punto'
    }
    df_procesado.rename(columns=rename_map, inplace=True)

    columnas_deseadas = [
            'Turno', 'start', 'hora_start', 'end', 'today', 'username', 'deviceid', 
            'Georreferenciación del punto', 'latitude', 'longitude', 
            'Cantidad de personas en situación de calle observadas', 
            'Características observables del punto', 
            'Se observan niños/as en el punto', '_id', '_uuid', '_submission_time', 
            '_validation_status', '_notes', '_status', '_submitted_by', '__version__', 
            '_tags', 'Poligono', 'Localizacion'
    ]
    
    cols_finales = [c for c in columnas_deseadas if c in df_procesado.columns]
    df_final = df_procesado[cols_finales].copy()
    df_final = df_final.replace({np.nan: '', pd.NA: ''}).astype(str)

    print("4. Subiendo...")
    if len(ids_existentes) == 0:
        sheet.clear()
        sheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())
    else:
        encabezados = sheet.row_values(1)
        df_append = df_final.reindex(columns=encabezados).replace({np.nan: '', pd.NA: ''}).astype(str)
        sheet.append_rows(df_append.values.tolist())

    print(">>> EXITO <<<")
