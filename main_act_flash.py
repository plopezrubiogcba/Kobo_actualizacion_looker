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

# KOBO (Intenta leer de variables de entorno de GitHub, si no, usa el valor fijo)
TOKEN_KOBO = os.environ.get("KOBO_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
URL_KOBO = f"https://kf.kobotoolbox.org/api/v2/assets/{UID_KOBO}/data.json"

# GOOGLE SHEETS
NOMBRE_SPREADSHEET = "puntos flash"
NOMBRE_HOJA = "Sheet4"

# RUTAS DE ARCHIVOS (Adaptable a Local y Nube)
# Usamos rutas relativas basadas en dónde está el script main.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # .../Flash
PROJECT_ROOT = os.path.dirname(BASE_DIR) # .../Red de Atencion

# Se definen las rutas posibles para los archivos KML y SHP.
posibles_rutas_kml = [
    os.path.join(BASE_DIR, 'capas', 'Recoleta Nueva Operación.kml'), # Busca en Flash/capas/
    os.path.join(PROJECT_ROOT, 'capas', 'Recoleta Nueva Operación.kml'), # Busca en Red de Atencion/capas/
]
posibles_rutas_shp = [
    os.path.join(PROJECT_ROOT, 'comunas.shp'), # Busca en Red de Atencion/
    os.path.join(BASE_DIR, 'comunas.shp'),     # Busca en Flash/
]

# Seleccionar ruta válida
RUTA_KML_ANILLO = next((r for r in posibles_rutas_kml if os.path.exists(r)), None)
RUTA_SHP_COMUNAS = next((r for r in posibles_rutas_shp if os.path.exists(r)), None)


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
    print("--- Iniciando clasificación de localización ---")
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
            comuna_col_found = next((col for col in ['comunas', 'COMUNAS', 'comuna', 'COMUNA'] if col in puntos_en_comunas.columns), None)
            if comuna_col_found:
                puntos_gdf.loc[puntos_en_comunas.index, 'Localizacion'] = puntos_en_comunas[comuna_col_found].astype(str)
    
    return puntos_gdf['Localizacion']

def asignar_recorrido(gdf, poligonos):
    print("--- Iniciando clasificación de Recorridos ---")
    resultado = pd.Series('', index=gdf.index, dtype=object)
    for nombre, poligono in poligonos.items():
        dentro = gdf.within(poligono)
        if dentro.any():
            resultado.loc[dentro] = nombre
    return resultado

def procesar_datos_geoespaciales(df_kobo):
    """Orquesta la transformación de datos crudos de Kobo a datos enriquecidos."""
    
    # Validación de archivos geoespaciales
    if not RUTA_KML_ANILLO or not RUTA_SHP_COMUNAS:
        print("ERROR CRÍTICO: No se encontraron los archivos KML o SHP en las rutas esperadas.")
        print(f"Buscado en: {posibles_rutas_kml} y {posibles_rutas_shp}")
        return None

    # 1. Preparación inicial de Lat/Lon
    print("Separando coordenadas latitud/longitud...")
    if 'geo_ref/geo_punto' in df_kobo.columns:
        split_coords = df_kobo['geo_ref/geo_punto'].astype(str).str.split(' ', expand=True)
        if split_coords.shape[1] >= 2:
            df_kobo['latitude'] = pd.to_numeric(split_coords[0], errors='coerce')
            df_kobo['longitude'] = pd.to_numeric(split_coords[1], errors='coerce')
    
    # Limpieza básica
    df_kobo['start'] = pd.to_datetime(df_kobo['start'])
    df_kobo.dropna(subset=['latitude', 'longitude'], inplace=True)
    
    # 2. Asignar Turno
    df_kobo['Turno'] = df_kobo['start'].apply(asignar_turno)

    # 3. GeoDataFrame y Capas
    puntos_gdf = gpd.GeoDataFrame(
        df_kobo,
        geometry=gpd.points_from_xy(df_kobo.longitude, df_kobo.latitude),
        crs="EPSG:4326"
    )

    # Cargar capas
    try:
        anillo_gdf = gpd.read_file(RUTA_KML_ANILLO, driver='KML') # A veces layer no es necesario si es único
        if anillo_gdf.crs is None: anillo_gdf.set_crs("EPSG:4326", inplace=True)
        
        comunas_gdf = gpd.read_file(RUTA_SHP_COMUNAS)
    except Exception as e:
        print(f"CRITICAL ERROR cargando capas: {e}")
        # Intentar cargar KML especificando layer si falla el general
        try:
            print("Reintentando cargar KML con layer específico...")
            anillo_gdf = gpd.read_file(RUTA_KML_ANILLO, layer='Nuevo Anillo Digital', driver='KML')
        except:
            return None

    # Polígonos Hardcoded
    poligonos_recorrido = {
        'Recorrido A': Polygon([(-58.41017, -34.588232), (-58.413901, -34.594177), (-58.413904, -34.599714),(-58.400064, -34.600033), (-58.386224, -34.599855), (-58.398154, -34.59498),(-58.404592, -34.593108), (-58.386524, -34.595263), (-58.41017, -34.588232)]),
        'Recorrido B': Polygon([(-58.389185, -34.584593), (-58.395365, -34.587137), (-58.400944, -34.594168),(-58.398154, -34.59498), (-58.386524, -34.595263), (-58.383284, -34.587544),(-58.388112, -34.59256), (-58.389185, -34.584593)]),
        'Recorrido C': Polygon([(-58.400944, -34.594168), (-58.395365, -34.587137), (-58.389185, -34.584593),(-58.398455, -34.580212), (-58.407295, -34.581837), (-58.404592, -34.593108),(-58.41017, -34.588232), (-58.400944, -34.594168)])
    }

    # 4. Ejecutar Clasificaciones
    df_kobo['Localizacion'] = clasificar_localizacion(puntos_gdf, anillo_gdf, comunas_gdf)
    df_kobo['Poligono'] = asignar_recorrido(puntos_gdf, poligonos_recorrido)

    return df_kobo


# --- 3. MAIN EJECUCIÓN ---

if __name__ == '__main__':
    print(">>> INICIO DE PROCESO INTEGRADO E INCREMENTAL <<<")
    
    # --- A. CONEXIÓN GOOGLE SHEETS ---
    print("1. Conectando a Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

    # Lógica Dual: Si existe variable de entorno (Nube) usa esa, si no busca archivo local
    if "GOOGLE_CREDENTIALS_JSON" in os.environ:
        print("   > Usando credenciales de Entorno (Cloud)")
        creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    else:
        print("   > Usando archivo local 'Flash/credenciales.json'")
        ruta_local_creds = os.path.join(BASE_DIR, "Flash", "credenciales.json")
        if not os.path.exists(ruta_local_creds):
            # Fallback por si el archivo está en la raiz
            ruta_local_creds = os.path.join(BASE_DIR, "credenciales.json")
        
        creds = ServiceAccountCredentials.from_json_keyfile_name(ruta_local_creds, scope)

    client = gspread.authorize(creds)
    
    try:
        sheet = client.open(NOMBRE_SPREADSHEET).worksheet(NOMBRE_HOJA)
    except Exception as e:
        print(f"Error al abrir la hoja de cálculo: {e}")
        sys.exit(1)

    # --- B. OBTENER REGISTROS EXISTENTES ---
    # Obtenemos todos los datos para saber qué IDs ya tenemos
    try:
        registros_existentes = sheet.get_all_records()
        # Asumimos que la columna se llama '_uuid', si no está, está vacía
        ids_existentes = set()
        if registros_existentes:
            if '_uuid' in registros_existentes[0]:
                ids_existentes = set(str(row['_uuid']) for row in registros_existentes)
        print(f"   > Se encontraron {len(ids_existentes)} registros previos en la hoja.")
    except Exception as e:
        print(f"   > Hoja vacía o error leyendo registros previos: {e}")
        ids_existentes = set()


    # --- C. EXTRACCIÓN (KOBO) ---
    print("2. Descargando datos de API Kobo...")
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    try:
        resp = requests.get(URL_KOBO, headers=headers)
        resp.raise_for_status()
        data_json = resp.json()['results']
        df_raw = pd.json_normalize(data_json)
        print(f"   > Total registros en Kobo: {len(df_raw)}")
    except Exception as e:
        print(f"Error fatal en descarga Kobo: {e}")
        sys.exit(1)

    # --- D. FILTRADO (Solo Nuevos) ---
    if not df_raw.empty and '_uuid' in df_raw.columns:
        # Asegurar que sea string para comparar
        df_raw['_uuid'] = df_raw['_uuid'].astype(str)
        # Filtramos: Solo filas cuyo UUID NO esté en el set de existentes
        df_nuevos = df_raw[~df_raw['_uuid'].isin(ids_existentes)].copy()
    else:
        df_nuevos = df_raw

    if df_nuevos.empty:
        print(">>> NO HAY REGISTROS NUEVOS. FIN DEL PROCESO. <<<")
        sys.exit(0)

    print(f">>> Procesando {len(df_nuevos)} registros NUEVOS...")


    # --- E. TRANSFORMACIÓN ---
    df_procesado = procesar_datos_geoespaciales(df_nuevos)
    
    if df_procesado is None or df_procesado.empty:
        print("Error en procesamiento o dataframe vacío tras limpieza. Abortando.")
        sys.exit(1)


    # --- F. FORMATO FINAL ---
    print("3. Formateando columnas...")
    
    # Generar columnas derivadas
    df_procesado['hora_start'] = df_procesado['start'].dt.strftime('%H:%M:%S')
    df_procesado['start'] = df_procesado['start'].dt.strftime('%Y-%m-%d')

    # Mapeo de nombres (Tu configuración original)
    rename_map = {
        'geo_ref/geo_punto': 'Georreferenciación del punto',
        'datos_per/cant_pers': 'Cantidad de personas en situación de calle observadas',
        'caracteristicas_puntos/caracteristicas_observada': 'Características observables del punto',
        'caracteristicas_puntos/estructura': 'estructura',
        'caracteristicas_puntos/NNyA_observa': 'Se observan niños/as en el punto'
    }
    df_procesado.rename(columns=rename_map, inplace=True)

    # Definir columnas deseadas
    columnas_deseadas = [
            'Turno', 'start', 'hora_start', 'end', 'today', 'username', 'deviceid', 
            'Georreferenciación del punto', 'latitude', 'longitude', 
            'Cantidad de personas en situación de calle observadas', 
            'Características observables del punto', 
            'Se observan niños/as en el punto', '_id', '_uuid', '_submission_time', 
            '_validation_status', '_notes', '_status', '_submitted_by', '__version__', 
            '_tags', 'Poligono', 'Localizacion'
    ]
    
    # Seleccionar solo columnas que existen en el DF procesado
    cols_finales_df = [c for c in columnas_deseadas if c in df_procesado.columns]
    df_final = df_procesado[cols_finales_df].copy()

    # Limpieza NaN -> String vacio
    df_final = df_final.replace({np.nan: '', pd.NA: ''}).astype(str)


    # --- G. CARGA INTELIGENTE (APPEND) ---
    print("4. Subiendo datos a Google Sheets...")

    # Caso 1: Hoja vacía (Escribimos todo, incluyendo encabezados)
    if len(ids_existentes) == 0:
        print("   > Hoja vacía. Escribiendo encabezados y datos iniciales.")
        sheet.clear()
        sheet.update([df_final.columns.values.tolist()] + df_final.values.tolist())
    
    # Caso 2: Hoja con datos (Hacemos APPEND respetando el orden de columnas del Sheet)
    else:
        print("   > Agregando datos al final (Append).")
        # Obtenemos los encabezados actuales de la hoja (Fila 1)
        encabezados_hoja = sheet.row_values(1)
        
        # Reordenamos df_final para que coincida EXACTAMENTE con la hoja
        # Si hay columnas nuevas en el DF que no están en la hoja, se ignoran (o se agregan al final si cambiamos lógica)
        # Si faltan columnas en el DF que sí están en la hoja, se rellenan con vacío
        df_append = df_final.reindex(columns=encabezados_hoja)
        df_append = df_append.replace({np.nan: '', pd.NA: ''}).astype(str)
        
        # Subimos solo los valores
        sheet.append_rows(df_append.values.tolist())

    print(">>> ÉXITO: Actualización completada. <<<")