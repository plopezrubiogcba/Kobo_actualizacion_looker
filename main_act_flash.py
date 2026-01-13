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
import re
import zipfile
import fiona

# --- 1. CONFIGURACIÓN GLOBAL ---
# Modificacion desde vscode

TOKEN_KOBO = os.environ.get("KOBO_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
URL_KOBO = f"https://kf.kobotoolbox.org/api/v2/assets/{UID_KOBO}/data.json"

# GOOGLE SHEETS
NOMBRE_SPREADSHEET = "puntos flash"
NOMBRE_HOJA = "Sheet4"

# --- 2. BÚSQUEDA AUTOMÁTICA DE ARCHIVOS LOCALES ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_KML_RECOLETA = None
RUTA_SHP_COMUNAS = None

print(f"--- Buscando archivos en: {BASE_DIR} ---")

for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if 'recoleta nueva operación' in file.lower() and file.lower().endswith('.kml'):
            RUTA_KML_RECOLETA = os.path.join(root, file)
            print(f"   ✅ KML Recoleta encontrado: {RUTA_KML_RECOLETA}")
        
        if file.lower() == 'comunas.shp':
            RUTA_SHP_COMUNAS = os.path.join(root, file)
            print(f"   ✅ SHP encontrado: {RUTA_SHP_COMUNAS}")

if not RUTA_KML_RECOLETA or not RUTA_SHP_COMUNAS:
    print("\n❌ ERROR CRÍTICO: Faltan archivos en el GitHub.")
    sys.exit(1)


# --- 3. FUNCIONES DE LÓGICA DE NEGOCIO ---

def asignar_turno(fecha):
    if pd.isnull(fecha): return None
    h = fecha.hour
    if 3 <= h < 8: return "TM"
    elif 8 <= h < 16: return "TO"
    elif 16 <= h < 22: return "TT"
    elif h >= 22 or h < 3: return "TN"
    else: return None

def clasificar_localizacion(puntos_gdf, recoleta_gdf, comunas_gdf):
    """
    Clasifica los puntos:
    1. Primero: Puntos dentro de Recoleta Nueva Operación -> 14.5
    2. Luego: Resto de puntos por número de comuna (1.0-15.0)
    """
    print("--- Iniciando clasificación de localización ---")
    
    # Asegurar mismo CRS
    puntos_gdf = puntos_gdf.to_crs("EPSG:4326")
    recoleta_gdf = recoleta_gdf.to_crs("EPSG:4326")
    comunas_gdf = comunas_gdf.to_crs("EPSG:4326")

    # Inicializar como None
    puntos_gdf['Localizacion'] = None

    # PASO 1: Clasificar Recoleta Nueva Operación como 14.5
    puntos_en_recoleta = gpd.sjoin(puntos_gdf, recoleta_gdf, how="inner", predicate='within')
    if not puntos_en_recoleta.empty:
        print(f"   ✅ {len(puntos_en_recoleta)} puntos clasificados como Recoleta Nueva Operación (14.5).")
        puntos_gdf.loc[puntos_en_recoleta.index, 'Localizacion'] = 14.5

    # PASO 2: Clasificar por comunas (solo los que NO son Recoleta)
    mask_recoleta = puntos_gdf['Localizacion'] == 14.5
    puntos_para_comunas = puntos_gdf[~mask_recoleta]

    if not puntos_para_comunas.empty:
        puntos_en_comunas = gpd.sjoin(puntos_para_comunas, comunas_gdf, how="inner", predicate='within')
        
        if not puntos_en_comunas.empty:
            # Buscar columna de comuna dinámicamente
            comuna_col_found = None
            possible_cols = ['comunas', 'COMUNAS', 'comuna', 'COMUNA', 'NAM', 'ID', 'OBJETO', 'barrio']
            
            for col in possible_cols:
                if col in puntos_en_comunas.columns:
                    comuna_col_found = col
                    break
            
            if comuna_col_found:
                # Convertir a float para mantener tipo numérico
                valores_numericos = pd.to_numeric(puntos_en_comunas[comuna_col_found], errors='coerce')
                puntos_gdf.loc[puntos_en_comunas.index, 'Localizacion'] = valores_numericos
                print(f"   ✅ {len(puntos_en_comunas)} puntos clasificados por comuna.")
    
    # Localizacion ahora es float (14.5 para Recoleta, 1.0-15.0 para comunas, None para fuera de zona)
    return puntos_gdf['Localizacion']

def asignar_recorrido(gdf, poligonos):
    print("--- Clasificando Recorridos ---")
    resultado = pd.Series('', index=gdf.index, dtype=object)
    for nombre, poligono in poligonos.items():
        dentro = gdf.within(poligono)
        if dentro.any():
            resultado.loc[dentro] = nombre
    return resultado

def procesar_datos_geoespaciales_total(df_kobo):
    print("Separando coordenadas latitud/longitud/altitud/precisión...")
    if 'geo_ref/geo_punto' in df_kobo.columns:
        split_coords = df_kobo['geo_ref/geo_punto'].astype(str).str.split(' ', expand=True)
        
        if split_coords.shape[1] >= 1:
            df_kobo['latitude'] = pd.to_numeric(split_coords[0], errors='coerce')
        if split_coords.shape[1] >= 2:
            df_kobo['longitude'] = pd.to_numeric(split_coords[1], errors='coerce')
        if split_coords.shape[1] >= 3:
            df_kobo['_Georreferenciación del punto_altitude'] = pd.to_numeric(split_coords[2], errors='coerce')
        else:
            df_kobo['_Georreferenciación del punto_altitude'] = 0
        
        if split_coords.shape[1] >= 4:
            df_kobo['_Georreferenciación del punto_precision'] = pd.to_numeric(split_coords[3], errors='coerce')
        else:
            df_kobo['_Georreferenciación del punto_precision'] = 0
    
    df_kobo['start'] = pd.to_datetime(df_kobo['start'])
    # Limpieza vital: Solo filas con geo válida
    df_kobo.dropna(subset=['latitude', 'longitude'], inplace=True)
    
    df_kobo['Turno'] = df_kobo['start'].apply(asignar_turno)

    puntos_gdf = gpd.GeoDataFrame(
        df_kobo,
        geometry=gpd.points_from_xy(df_kobo.longitude, df_kobo.latitude),
        crs="EPSG:4326"
    )

    try:
        # Habilitar soporte KML en fiona
        fiona.drvsupport.supported_drivers['KML'] = 'rw'
        fiona.drvsupport.supported_drivers['LIBKML'] = 'rw'
        
        # Leer KML de Recoleta Nueva Operación
        try:
            recoleta_gdf = gpd.read_file(RUTA_KML_RECOLETA, driver='KML')
        except:
            # Si es KMZ (archivo ZIP), extraer KML primero
            if RUTA_KML_RECOLETA.lower().endswith('.kmz'):
                with zipfile.ZipFile(RUTA_KML_RECOLETA, 'r') as kmz:
                    kml_files = [f for f in kmz.namelist() if f.endswith('.kml')]
                    if kml_files:
                        with kmz.open(kml_files[0]) as kml_file:
                            recoleta_gdf = gpd.read_file(kml_file)
                    else:
                        raise FileNotFoundError("No se encontró KML dentro del KMZ")
            else:
                recoleta_gdf = gpd.read_file(RUTA_KML_RECOLETA)
        
        if recoleta_gdf.crs is None: recoleta_gdf.set_crs("EPSG:4326", inplace=True)
        comunas_gdf = gpd.read_file(RUTA_SHP_COMUNAS)
        
    except Exception as e:
        print(f"❌ ERROR FATAL CARGANDO CAPAS: {e}")
        sys.exit(1)

    poligonos_recorrido = {
        'Recorrido A': Polygon([(-58.41017, -34.588232), (-58.413901, -34.594177), (-58.413904, -34.599714),(-58.400064, -34.600033), (-58.386224, -34.599855), (-58.398154, -34.59498),(-58.404592, -34.593108), (-58.386524, -34.595263), (-58.41017, -34.588232)]),
        'Recorrido B': Polygon([(-58.389185, -34.584593), (-58.395365, -34.587137), (-58.400944, -34.594168),(-58.398154, -34.59498), (-58.386524, -34.595263), (-58.383284, -34.587544),(-58.388112, -34.59256), (-58.389185, -34.584593)]),
        'Recorrido C': Polygon([(-58.400944, -34.594168), (-58.395365, -34.587137), (-58.389185, -34.584593),(-58.398455, -34.580212), (-58.407295, -34.581837), (-58.404592, -34.593108),(-58.41017, -34.588232), (-58.400944, -34.594168)])
    }

    df_kobo['Localizacion'] = clasificar_localizacion(puntos_gdf, recoleta_gdf, comunas_gdf)
    df_kobo['Poligono'] = asignar_recorrido(puntos_gdf, poligonos_recorrido)

    return df_kobo


# --- 4. MAIN EJECUCIÓN ---

if __name__ == '__main__':
    print(">>> INICIO DE PROCESO INTEGRADO (ESTRICTO + JSON COMPLIANT) <<<")
    
    # 1. KOBO
    print("1. Descargando Kobo Completo...")
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    try:
        resp = requests.get(URL_KOBO, headers=headers)
        resp.raise_for_status()
        df_raw = pd.json_normalize(resp.json()['results'])
    except Exception as e:
        print(f"Error Kobo: {e}")
        sys.exit(1)

    if df_raw.empty: sys.exit(0)

    # 2. PROCESAR GEOESPACIALMENTE
    print("2. Procesando lógica geoespacial...")
    df_procesado = procesar_datos_geoespaciales_total(df_raw)
    
    if df_procesado is None or df_procesado.empty: sys.exit(1)

    # 3. GOOGLE SHEETS & DUPLICADOS
    print("3. Verificando duplicados...")
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    if "GOOGLE_CREDENTIALS_JSON" in os.environ:
        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        ruta_creds = next((os.path.join(root, 'credenciales.json') for root, _, files in os.walk(BASE_DIR) if 'credenciales.json' in files), 'credenciales.json')
        creds = ServiceAccountCredentials.from_json_keyfile_name(ruta_creds, scope)

    client = gspread.authorize(creds)
    try:
        sheet = client.open(NOMBRE_SPREADSHEET).worksheet(NOMBRE_HOJA)
        registros = sheet.get_all_records()
        ids_existentes = set(str(r['_uuid']) for r in registros) if registros and '_uuid' in registros[0] else set()
    except:
        ids_existentes = set()
    
    # 4. FILTRAR NUEVOS
    if '_uuid' in df_procesado.columns:
        df_procesado['_uuid'] = df_procesado['_uuid'].astype(str)
        df_nuevos_final = df_procesado[~df_procesado['_uuid'].isin(ids_existentes)].copy()
    else:
        df_nuevos_final = df_procesado

    if df_nuevos_final.empty:
        print(">>> Todo actualizado. No hay registros nuevos. <<<")
        sys.exit(0)

    print(f"   > Registros NUEVOS a subir: {len(df_nuevos_final)}")

    # 5. FORMATEO ESTRICTO
    print("4. Aplicando formatos estrictos...")
    
    df_nuevos_final['hora_start'] = df_nuevos_final['start'].dt.strftime('%H:%M:%S')
    df_nuevos_final['start'] = df_nuevos_final['start'].dt.strftime('%Y-%m-%d')

    rename_map = {
        'geo_ref/geo_punto': 'Georreferenciación del punto',
        'datos_per/cant_pers': 'Cantidad de personas en situación de calle observadas',
        'caracteristicas_puntos/caracteristicas_observada': 'Características observables del punto',
        'caracteristicas_puntos/estructura': 'estructura',
        'caracteristicas_puntos/colchon': 'colchon', 
        'caracteristicas_puntos/NNyA_observa': 'Se observan niños/as en el punto'
    }
    df_nuevos_final.rename(columns=rename_map, inplace=True)

    columnas_deseadas = [
        'Turno', 'start', 'hora_start', 'end', 'today', 'username', 'deviceid',
        'Georreferenciación del punto', 'latitude', 'longitude',
        '_Georreferenciación del punto_altitude', '_Georreferenciación del punto_precision',
        'Cantidad de personas en situación de calle observadas',
        'Características observables del punto', 'estructura', 'colchon',
        'Características observables del punto/Basura, ropa, bolsos, etc',
        'Características observables del punto/No se observan cosas',
        'Se observan niños/as en el punto', '_id', '_uuid', '_submission_time', 
        '_validation_status', '_notes', '_status', '_submitted_by', '__version__', 
        '_tags', '_index', 'Poligono', 'Localizacion'
    ]
    
    df_final = df_nuevos_final.reindex(columns=columnas_deseadas)

    # FORMATO: Numéricos (Float)
    cols_float = ['latitude', 'longitude', '_Georreferenciación del punto_altitude', '_Georreferenciación del punto_precision']
    for col in cols_float:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    # FORMATO: Enteros (SOLO los que son realmente numéricos)
    # He quitado "Características..." y "Se observan niños..." porque son Texto.
    cols_enteros = ['Cantidad de personas en situación de calle observadas']
    for col_cant in cols_enteros:
        if col_cant in df_final.columns:
            df_final[col_cant] = pd.to_numeric(df_final[col_cant], errors='coerce').fillna(0).astype(int)

    # FORMATO: Localización (ya viene como float desde clasificar_localizacion)
    # 14.5 = Recoleta Nueva Operación, 1.0-15.0 = Comunas, None = Fuera de zona
    if 'Localizacion' in df_final.columns:
        df_final['Localizacion'] = pd.to_numeric(df_final['Localizacion'], errors='coerce')

    # LIMPIEZA CRÍTICA PARA JSON (Evita error 'Out of range float values' y 'list_value')
    
    def clean_complex_types(val):
        if isinstance(val, (list, dict)):
            return str(val)
        return val

    for col in df_final.columns:
        df_final[col] = df_final[col].apply(clean_complex_types)

    # 2. Reemplazar Infinito por NaN
    df_final = df_final.replace([np.inf, -np.inf], np.nan)

    # 3. Convertir DF a object para permitir None
    df_final = df_final.astype(object)

    # 4. Reemplazar NaN con None
    df_final = df_final.where(pd.notnull(df_final), None)

    print("5. Subiendo a Google Sheets...")
    if len(ids_existentes) == 0:
        sheet.clear()
        sheet.update(values=[df_final.columns.values.tolist()] + df_final.values.tolist(), value_input_option='USER_ENTERED')
    else:
        headers_sheet = sheet.row_values(1)
        if not headers_sheet: headers_sheet = columnas_deseadas
        
        df_append = df_final.reindex(columns=headers_sheet)
        df_append = df_append.astype(object)
        df_append = df_append.where(pd.notnull(df_append), None)
        
        sheet.append_rows(values=df_append.values.tolist(), value_input_option='USER_ENTERED')

    print(">>> ÉXITO: Carga completada. <<<")
