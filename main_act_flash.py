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
from sqlalchemy import create_engine

# --- 1. CONFIGURACIÓN GLOBAL ---
# Modificacion desde vscode

# Cargar variables de entorno desde .env si existe (para ejecución local)
try:
    from dotenv import load_dotenv
    load_dotenv()  # Busca .env en el directorio actual
    print("✅ Variables de .env cargadas")
except ImportError:
    pass  # python-dotenv no instalado, usar solo variables del sistema

TOKEN_KOBO = os.environ.get("KOBO_API_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
KOBO_BASE_URL = os.environ.get("KOBO_BASE_URL", "https://kf.kobotoolbox.org")
URL_KOBO = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/data.json"
URL_KOBO_ASSET = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/"

# GOOGLE SHEETS
NOMBRE_SPREADSHEET = "puntos flash"
NOMBRE_HOJA = "Sheet4"

# NEON POSTGRESQL
DATABASE_URL = os.environ.get("DATABASE_URL")
NEON_TABLE_NAME = 'kobo_flash_consolidado'

# --- 2. BÚSQUEDA AUTOMÁTICA DE ARCHIVOS LOCALES ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RUTA_KMZ_PALERMO = None
RUTA_KML_ANILLO_DIGITAL = None
RUTA_SHP_COMUNAS = None

print(f"--- Buscando archivos en: {BASE_DIR} ---")

for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if 'palermo' in file.lower() and 'norte' in file.lower() and file.lower().endswith('.kmz'):
            RUTA_KMZ_PALERMO = os.path.join(root, file)
            print(f"   ✅ KMZ Palermo Norte encontrado: {RUTA_KMZ_PALERMO}")
        
        if 'anillo_digital' in file.lower() and file.lower().endswith('.kmz'):
            RUTA_KML_ANILLO_DIGITAL = os.path.join(root, file)
            print(f"   ✅ KML Anillo Digital encontrado: {RUTA_KML_ANILLO_DIGITAL}")
        
        if file.lower() == 'comunas.shp':
            RUTA_SHP_COMUNAS = os.path.join(root, file)
            print(f"   ✅ SHP encontrado: {RUTA_SHP_COMUNAS}")

if not RUTA_KMZ_PALERMO or not RUTA_KML_ANILLO_DIGITAL or not RUTA_SHP_COMUNAS:
    print("\n❌ ERROR CRÍTICO: Faltan archivos en el GitHub.")
    sys.exit(1)


# --- 3. FUNCIONES DE EXTRACCIÓN COMPLETA DE KOBO ---

def obtener_schema_kobo():
    """
    Obtiene el schema completo del formulario Kobo para saber qué campos esperar
    """
    try:
        headers = {"Authorization": f"Token {TOKEN_KOBO}"}
        response = requests.get(URL_KOBO_ASSET, headers=headers)
        response.raise_for_status()
        asset_data = response.json()
        return asset_data.get('content', {})
    except Exception as e:
        print(f"⚠️  No se pudo obtener schema de Kobo: {e}")
        return {}

def expandir_geopoint(df, geopoint_col):
    """
    Expande una columna geopoint en latitude, longitude, altitude, precision
    Formato Kobo: "lat lon alt precision"
    """
    if geopoint_col not in df.columns:
        return df
    
    # Separar el string "lat lon alt precision"
    coords = df[geopoint_col].astype(str).str.split(' ', expand=True)
    
    if coords.shape[1] >= 2:
        # Crear nombres de columnas basados en el nombre original
        base_name = geopoint_col.replace('/', '_')
        
        df['latitude'] = pd.to_numeric(coords[0], errors='coerce')
        df['longitude'] = pd.to_numeric(coords[1], errors='coerce')
        
        if coords.shape[1] >= 3:
            df[f'_{base_name}_altitude'] = pd.to_numeric(coords[2], errors='coerce')
        if coords.shape[1] >= 4:
            df[f'_{base_name}_precision'] = pd.to_numeric(coords[3], errors='coerce')
    
    return df

def expandir_select_multiple(df, schema, col_name):
    """
    Expande un campo select_multiple en columnas booleanas
    
    Ej: 'caracteristicas_puntos/en_lugar_hay' = 'estructura colchon basura'
    Se convierte en:
    - caracteristicas_puntos/estructura_carpa_refugio = True
    - caracteristicas_puntos/colchon_es = True
    - etc.
    """
    if not schema or col_name not in df.columns:
        return df
    
    # Buscar información del campo en el schema
    survey = schema.get('survey', [])
    field_name = col_name.split('/')[-1]
    field_info = next((q for q in survey if q.get('name') == field_name), None)
    
    if not field_info or field_info.get('type') != 'select_multiple':
        return df
    
    # Obtener las opciones (choices)
    choice_list_name = field_info.get('select_from_list_name') or field_info.get('list_name')
    if not choice_list_name:
        return df
    
    choices = schema.get('choices', [])
    relevant_choices = [c for c in choices if c.get('list_name') == choice_list_name]
    
    # Crear columna booleana para cada opción
    for choice in relevant_choices:
        choice_name = choice.get('name')
        if not choice_name:
            continue
        
        # Crear nombre de columna siguiendo patrón de Kobo
        col_bool_name = f"{col_name}/{choice_name}"
        
        # Verificar si el valor está presente en la respuesta
        df[col_bool_name] = df[col_name].astype(str).str.contains(
            r'\b' + re.escape(choice_name) + r'\b',
            regex=True,
            na=False
        )
    
    return df

def asegurar_columnas_schema(df, schema):
    """
    Asegura que TODAS las columnas del formulario existan en el DataFrame,
    incluso si no tienen datos en ningún registro
    """
    if not schema:
        return df
    
    survey = schema.get('survey', [])
    choices = schema.get('choices', [])
    
    for question in survey:
        field_name = question.get('name')
        field_type = question.get('type')
        
        if not field_name:
            continue
        
        # Para select_multiple, crear subcampos
        if field_type == 'select_multiple':
            choice_list_name = question.get('select_from_list_name') or question.get('list_name')
            if choice_list_name:
                relevant_choices = [c for c in choices if c.get('list_name') == choice_list_name]
                for choice in relevant_choices:
                    choice_name = choice.get('name')
                    if choice_name:
                        col_name = f"{field_name}/{choice_name}"
                        # Buscar por nombre final (puede tener prefijo de grupo)
                        matching_cols = [c for c in df.columns if c.endswith(f"/{field_name}/{choice_name}") or c == col_name]
                        if not matching_cols and col_name not in df.columns:
                            df[col_name] = False
                            print(f"  ℹ️  Columna booleana creada (sin datos): {col_name}")
    
    return df

def extraer_kobo_completo():
    """
    Extrae TODOS los datos de Kobo con todas las columnas expandidas:
    - Geopoints separados en lat/lon/alt/precision
    - Select_multiple expandidos en columnas booleanas
    - Todas las columnas del schema incluidas
    """
    print("📋 EXTRACCIÓN COMPLETA DE KOBO")
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    
    # 1. Obtener schema del formulario
    print("  📂 Obteniendo schema del formulario...")
    schema = obtener_schema_kobo()
    
    # 2. Obtener datos
    print("  📥 Descargando submissions...")
    try:
        response = requests.get(URL_KOBO, headers=headers, params={'query': '{}'})
        response.raise_for_status()
        submissions = response.json()['results']
        
        if not submissions:
            print("  ⚠️  No hay submissions en Kobo")
            return pd.DataFrame()
        
        # 3. Normalizar JSON (expandir anidación completa)
        print("  🔄 Normalizando datos...")
        df = pd.json_normalize(submissions, max_level=None, sep='/')
        print(f"  ✅ Columnas base extraídas: {len(df.columns)}")
        
        # 4. Expandir geopoints
        print("  🗺️  Expandiendo geopoints...")
        if schema:
            for question in schema.get('survey', []):
                if question.get('type') == 'geopoint':
                    field_name = question.get('name')
                    # Buscar columnas que terminen con este nombre
                    matching_cols = [col for col in df.columns if col.endswith(f"/{field_name}") or col == field_name]
                    for col in matching_cols:
                        df = expandir_geopoint(df, col)
                        print(f"    ✅ Geopoint expandido: {col}")
        
        # 5. Expandir select_multiple
        print("  ☑️  Expandiendo select_multiple...")
        if schema:
            for question in schema.get('survey', []):
                if question.get('type') == 'select_multiple':
                    field_name = question.get('name')
                    matching_cols = [col for col in df.columns if col.endswith(f"/{field_name}") or col == field_name]
                    for col in matching_cols:
                        df = expandir_select_multiple(df, schema, col)
                        print(f"    ✅ Select_multiple expandido: {col}")
        
        # 6. Asegurar columnas del schema
        print("  📋 Asegurando columnas completas...")
        df = asegurar_columnas_schema(df, schema)
        
        print(f"\n  🎉 Extracción completa: {len(df)} registros, {len(df.columns)} columnas")
        return df
        
    except Exception as e:
        print(f"  ❌ Error en extracción: {e}")
        raise


# --- 4. FUNCIONES DE LÓGICA DE NEGOCIO ---

def asignar_turno(fecha):
    if pd.isnull(fecha): return None
    h = fecha.hour
    if 3 <= h < 8: return "TM"
    elif 8 <= h < 16: return "TO"
    elif 16 <= h < 22: return "TT"
    elif h >= 22 or h < 3: return "TN"
    else: return None

def clasificar_localizacion(puntos_gdf, palermo_gdf, anillo_digital_gdf, comunas_gdf):
    """
    Clasifica los puntos en 3 pasos secuenciales:
    1. Palermo Norte -> 14.5
    2. Anillo Digital C2 -> 2.5
    3. Comunas -> 1.0-15.0
    """
    print("--- Iniciando clasificación de localización (3 pasos) ---")
    
    # Asegurar mismo CRS
    puntos_gdf = puntos_gdf.to_crs("EPSG:4326")
    palermo_gdf = palermo_gdf.to_crs("EPSG:4326")
    anillo_digital_gdf = anillo_digital_gdf.to_crs("EPSG:4326")
    comunas_gdf = comunas_gdf.to_crs("EPSG:4326")

    # Inicializar como None
    puntos_gdf['Localizacion'] = None

    # PASO 1: Clasificar Palermo Norte como 14.5
    puntos_en_palermo = gpd.sjoin(puntos_gdf, palermo_gdf, how="inner", predicate='within')
    if not puntos_en_palermo.empty:
        print(f"   ✅ {len(puntos_en_palermo)} puntos clasificados como Palermo Norte (14.5).")
        puntos_gdf.loc[puntos_en_palermo.index, 'Localizacion'] = 14.5

    # PASO 2: Clasificar Anillo Digital C2 como 2.5 (solo puntos NO clasificados)
    mask_palermo = puntos_gdf['Localizacion'] == 14.5
    puntos_restantes = puntos_gdf[~mask_palermo]
    
    if not puntos_restantes.empty:
        puntos_en_anillo = gpd.sjoin(puntos_restantes, anillo_digital_gdf, how="inner", predicate='within')
        if not puntos_en_anillo.empty:
            print(f"   ✅ {len(puntos_en_anillo)} puntos clasificados como Anillo Digital C2 (2.5).")
            puntos_gdf.loc[puntos_en_anillo.index, 'Localizacion'] = 2.5

    # PASO 3: Clasificar por comunas (solo puntos aún NO clasificados)
    mask_clasificados = puntos_gdf['Localizacion'].notna()
    puntos_para_comunas = puntos_gdf[~mask_clasificados]

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
    
    # Localizacion es float: 14.5=Palermo, 2.5=Anillo Digital, 1.0-15.0=Comunas, None=Fuera
    return puntos_gdf['Localizacion']

def subir_a_neon(df):
    """
    Sube el DataFrame a Neon PostgreSQL usando SQLAlchemy.
    Trabaja sobre una copia para no afectar los datos de Sheets.
    IMPORTANTE: Mantiene nombres de columnas EXACTOS (con tildes, espacios, etc.)
    ya que PostgreSQL los soporta con comillas dobles.
    """
    print("--- Preparando datos para Neon PostgreSQL ---")
    
    # 1. Validar DATABASE_URL
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL no está configurada en las variables de entorno (.env)")
    
    # 2. Clonar DataFrame
    df_neon = df.copy()
    
    # 3. Sanitización de tipos complejos (listas/diccionarios)
    # NOTA: NO limpiamos nombres de columnas porque Neon espera los nombres EXACTOS
    for col in df_neon.columns:
        df_neon[col] = df_neon[col].apply(
            lambda x: str(x) if isinstance(x, (list, dict)) else x
        )
    print(f"   ✅ Tipos de datos sanitizados")
    
    # 4. Crear engine de SQLAlchemy con SSL
    print(f"   🔌 Conectando a Neon PostgreSQL...")
    engine = create_engine(DATABASE_URL)
    
    # 5. Validar conexión
    try:
        with engine.connect() as connection:
            print(f"   ✅ Conexión a Neon PostgreSQL exitosa (SSL habilitado)")
    except Exception as e:
        raise ConnectionError(f"Error al conectar con Neon PostgreSQL: {e}")
    
    # 6. Carga a Neon PostgreSQL
    print(f"   📤 Subiendo datos a tabla: {NEON_TABLE_NAME}")
    print(f"   📋 Columnas a cargar: {len(df_neon.columns)}")
    
    df_neon.to_sql(
        name=NEON_TABLE_NAME,
        con=engine,
        if_exists='replace',
        index=False,
        method='multi'
    )
    
    print(f"   ✅ {len(df_neon)} registros cargados exitosamente a Neon PostgreSQL")
    
    # 7. Cerrar conexión
    engine.dispose()

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
        # Cargar Palermo Norte KMZ
        print("📂 Cargando archivo Palermo Norte...")
        with zipfile.ZipFile(RUTA_KMZ_PALERMO, 'r') as kmz:
            kml_files = [f for f in kmz.namelist() if f.endswith('.kml')]
            if kml_files:
                with kmz.open(kml_files[0]) as kml_file:
                    palermo_gdf = gpd.read_file(kml_file)
            else:
                raise FileNotFoundError("No se encontró KML dentro de Palermo_Norte.kmz")
        if palermo_gdf.crs is None: palermo_gdf.set_crs("EPSG:4326", inplace=True)
        
        # Cargar Anillo Digital C2 KMZ
        print("📂 Cargando archivo Anillo Digital C2...")
        with zipfile.ZipFile(RUTA_KML_ANILLO_DIGITAL, 'r') as kmz:
            kml_files = [f for f in kmz.namelist() if f.endswith('.kml')]
            if kml_files:
                with kmz.open(kml_files[0]) as kml_file:
                    anillo_digital_gdf = gpd.read_file(kml_file)
            else:
                raise FileNotFoundError("No se encontró KML dentro de anillo_digital_c2.kmz")
        if anillo_digital_gdf.crs is None: anillo_digital_gdf.set_crs("EPSG:4326", inplace=True)
        
        # Cargar comunas SHP
        print("📂 Cargando shapefile de comunas...")
        comunas_gdf = gpd.read_file(RUTA_SHP_COMUNAS)
        
    except Exception as e:
        print(f"❌ ERROR FATAL CARGANDO CAPAS: {e}")
        sys.exit(1)

    poligonos_recorrido = {
        'Recorrido A': Polygon([(-58.41017, -34.588232), (-58.413901, -34.594177), (-58.413904, -34.599714),(-58.400064, -34.600033), (-58.386224, -34.599855), (-58.398154, -34.59498),(-58.404592, -34.593108), (-58.386524, -34.595263), (-58.41017, -34.588232)]),
        'Recorrido B': Polygon([(-58.389185, -34.584593), (-58.395365, -34.587137), (-58.400944, -34.594168),(-58.398154, -34.59498), (-58.386524, -34.595263), (-58.383284, -34.587544),(-58.388112, -34.59256), (-58.389185, -34.584593)]),
        'Recorrido C': Polygon([(-58.400944, -34.594168), (-58.395365, -34.587137), (-58.389185, -34.584593),(-58.398455, -34.580212), (-58.407295, -34.581837), (-58.404592, -34.593108),(-58.41017, -34.588232), (-58.400944, -34.594168)])
    }

    df_kobo['Localizacion'] = clasificar_localizacion(puntos_gdf, palermo_gdf, anillo_digital_gdf, comunas_gdf)
    df_kobo['Poligono'] = asignar_recorrido(puntos_gdf, poligonos_recorrido)

    return df_kobo


# --- 4. MAIN EJECUCIÓN ---

if __name__ == '__main__':
    print(">>> INICIO DE PROCESO INTEGRADO (EXTRACCIÓN COMPLETA KOBO) <<<")
    
    # 1. KOBO - EXTRACCIÓN COMPLETA
    print("\n1. Descargando datos completos de Kobo...")
    try:
        df_raw = extraer_kobo_completo()
    except Exception as e:
        print(f"❌ Error en extracción de Kobo: {e}")
        sys.exit(1)

    if df_raw.empty:
        print(">>> No hay datos en Kobo <<<")
        sys.exit(0)

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
        # Buscar archivo de credenciales con múltiples nombres posibles
        possible_names = ['kobo-looker-connect.json', 'credenciales.json', 'service_account.json']
        ruta_creds = None
        
        for name in possible_names:
            for root, _, files in os.walk(BASE_DIR):
                if name in files:
                    ruta_creds = os.path.join(root, name)
                    print(f"✅ Credenciales encontradas: {name}")
                    break
            if ruta_creds:
                break
        
        if not ruta_creds:
            print("❌ ERROR: No se encontró archivo de credenciales")
            print(f"   Buscando: {', '.join(possible_names)}")
            sys.exit(1)
        
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
    'latitude': '_Georreferenciación del punto_latitude',
    'longitude': '_Georreferenciación del punto_longitude',
    
    'datos_per/cant_pers': 'Cantidad de personas en situación de calle observadas',
    'datos_per/personas_estado': 'La/s persona/s esta/n',
    'datos_per/doc_usuario': 'Ingrese número de documento del usuario que completa el formulario',
    'datos_per/doc_digitos': 'Ingrese solo los digitos de su documento',

    'caracteristicas_puntos/caracteristicas_observada': 'Características observables del punto',
    'caracteristicas_puntos/estructura': 'estructura',
    'caracteristicas_puntos/colchon': 'colchon',
    'caracteristicas_puntos/NNyA_observa': 'Se observan niños/as en el lugar',

    'caracteristicas_puntos/en_lugar_hay': 'En el lugar hay…',
    'caracteristicas_puntos/estructura_carpa_refugio': 'En el lugar hay…/Con estructura tipo carpa o refugio',
    'caracteristicas_puntos/colchon_es': 'En el lugar hay…/Colchón/es',
    'caracteristicas_puntos/basura_ropa_bolsos': 'En el lugar hay…/Basura, ropa, bolsos, etc',
    'caracteristicas_puntos/no_se_observa': 'En el lugar hay…/No se observa nada de lo anterior',
    'caracteristicas_puntos/bolsos_bolsas': 'En el lugar hay…/Bolsos y/o bolsas',
    'caracteristicas_puntos/materiales_acumulados': 'En el lugar hay…/Otro materiales acumulados (Cartón, chatarras, etc.)',
    'caracteristicas_puntos/silla_ruedas_carrito': 'En el lugar hay…/Silla de ruedas, carrito de bebé',
    'caracteristicas_puntos/carro_cartonero': 'En el lugar hay…/Carro para transportar cosas (Cartoneros)',
    
    '__version__': '_version_'
}

    df_nuevos_final.rename(columns=rename_map, inplace=True)

    columnas_deseadas = [
    # Columnas base de Kobo
    'start', 'end', 'today', 'username', 'deviceid',
    
    # Georreferenciación (NEON requiere nombres con prefijo completo)
    'Georreferenciación del punto',
    '_Georreferenciación del punto_latitude',
    '_Georreferenciación del punto_longitude',
    '_Georreferenciación del punto_altitude',
    '_Georreferenciación del punto_precision',
    
    # Datos de personas
    'Ingrese número de documento del usuario que completa el formulario',
    'Cantidad de personas en situación de calle observadas',
    'La/s persona/s esta/n',

    # En el lugar hay… (todas las variantes)
    'En el lugar hay…',
    'En el lugar hay…/Con estructura tipo carpa o refugio',
    'En el lugar hay…/Colchón/es',
    'En el lugar hay…/Basura, ropa, bolsos, etc',
    'En el lugar hay…/No se observa nada de lo anterior',
    'En el lugar hay…/Bolsos y/o bolsas',
    'En el lugar hay…/Otro materiales acumulados (Cartón, chatarras, etc.)',
    'En el lugar hay…/Silla de ruedas, carrito de bebé',
    'En el lugar hay…/Carro para transportar cosas (Cartoneros)',

    # Observación de niños/as (nombre correcto según Neon)
    'Se observan niños/as en el lugar',
    
    # Documento adicional
    'Ingrese solo los digitos de su documento',

    # Metadatos de Kobo (según esquema Neon)
    '_id', '_uuid', '_submission_time',
    '_validation_status', '_notes', '_status', '_submitted_by', '_version_',
    '_tags', '_index',
    
    # Columnas calculadas por el script (no en tabla Neon original, pero útiles)
    'Turno', 'hora_start', 'Poligono', 'Localizacion'
]
    print(f'DEVOLVER ESTO{df_nuevos_final.columns.tolist()}')
    
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
    # 14.5 = Palermo Norte, 2.5 = Anillo Digital C2, 1.0-15.0 = Comunas, None = Fuera de zona
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

    # 6. SUBIR A NEON POSTGRESQL
    print("6. Subiendo a Neon PostgreSQL...")
    try:
        subir_a_neon(df_final)
        print("   ✅ Carga a Neon PostgreSQL exitosa")
    except Exception as e:
        print(f"   ⚠️  Error en Neon PostgreSQL (no crítico): {e}")
        print(f"   ℹ️  La carga a Google Sheets se completó correctamente")

    print(">>> ÉXITO: Carga completada. <<<")
