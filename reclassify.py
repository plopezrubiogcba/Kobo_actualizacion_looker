"""
Script de Reclasificación Única - Kobo API + Google Sheets
===========================================================

Este script descarga datos directamente de la API de Kobo,
aplica la nueva lógica de clasificación espacial en 2 pasos, y 
REEMPLAZA completamente el contenido del sheet con los datos reclasificados.

IMPORTANTE:
- Extrae datos completos de Kobo con expansión de select_multiple
- Hace BACKUP automático exportando a CSV antes de modificar
- REEMPLAZA todos los datos del sheet

Clasificación en 2 pasos:
1. Palermo Norte → 14.5
2. Comunas → 1.0-15.0
"""

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
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# --- CONFIGURACIÓN KOBO ---
TOKEN_KOBO = os.environ.get("KOBO_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
URL_KOBO = f"https://kf.kobotoolbox.org/api/v2/assets/{UID_KOBO}/data.json"

# Configuración Google Sheets
NOMBRE_SPREADSHEET = "puntos flash"
NOMBRE_HOJA = "Sheet4"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

from sqlalchemy import create_engine

# Configuración Neon PostgreSQL
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    # Fallback si no está en entorno (aunque debería estar)
    print("⚠️ ADVERTENCIA: DATABASE_URL no encontrada en variables de entorno")
else:
    print("✅ DATABASE_URL cargada correctamente")

NEON_TABLE_NAME = 'kobo_flash_consolidado'


# Buscar archivos geográficos
print("🔍 Buscando archivos geográficos...")
RUTA_KMZ_PALERMO = None
RUTA_SHP_COMUNAS = None

for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if 'palermo' in file.lower() and 'norte' in file.lower() and file.lower().endswith('.kmz'):
            RUTA_KMZ_PALERMO = os.path.join(root, file)
            print(f"   ✅ Palermo Norte: {RUTA_KMZ_PALERMO}")
        
        if file.lower() == 'comunas.shp':
            RUTA_SHP_COMUNAS = os.path.join(root, file)
            print(f"   ✅ Comunas: {RUTA_SHP_COMUNAS}")

if not all([RUTA_KMZ_PALERMO, RUTA_SHP_COMUNAS]):
    print("❌ ERROR: Faltan archivos geográficos")
    sys.exit(1)


# --- FUNCIONES DE EXTRACCIÓN COMPLETA DE KOBO ---

def obtener_schema_kobo():
    """Obtiene el schema completo del formulario Kobo"""
    KOBO_BASE_URL = "https://kf.kobotoolbox.org"
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    url = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/"
    
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get('content', {})

def expandir_select_multiple(df, schema, col_name):
    """
    Expande un campo select_multiple en columnas booleanas separadas
    
    Args:
        df: DataFrame con los datos
        schema: Schema del formulario Kobo
        col_name: Nombre completo del campo select_multiple (ej: 'caracteristicas_puntos/caracteristicas_observada')
    
    Returns:
        DataFrame con columnas expandidas
    """
    survey = schema.get('survey', [])
    choices = schema.get('choices', [])
    
    # Buscar el campo en el survey
    field_info = None
    for q in survey:
        xpath = q.get('$xpath', q.get('name', ''))
        if xpath == col_name or q.get('name') == col_name.split('/')[-1]:
            if q.get('type') == 'select_multiple':
                field_info = q
                break
    
    if not field_info:
        print(f"   ⚠️  Campo {col_name} no encontrado en schema como select_multiple")
        return df
    
    # Obtener lista de opciones
    list_name = field_info.get('select_from_list_name') or field_info.get('list_name')
    if not list_name:
        print(f"   ⚠️ Campo {col_name} no tiene lista de opciones")
        return df
    
    # Filtrar opciones relevantes
    relevant_choices = [c for c in choices if c.get('list_name') == list_name]
    
    if not relevant_choices:
        print(f"   ⚠️  No se encontraron opciones para list_name={list_name}")
        return df
    
    # Crear columnas booleanas
    for choice in relevant_choices:
        choice_name = choice.get('name')
        col_bool_name = f"{col_name}/{choice_name}"
        
        # Verificar si el valor contiene esta opción
        df[col_bool_name] = df[col_name].astype(str).str.contains(
            r'\b' + re.escape(choice_name) + r'\b',
            regex=True,
            na=False
        )
    
    print(f"   ✅ Select_multiple expandido: {col_name} → {len(relevant_choices)} columnas")
    return df

def extraer_kobo_completo():
    """
    Extracción completa de datos de Kobo incluyendo expansión de select_multiple
    """
    print("\n📋 EXTRACCIÓN COMPLETA DE KOBO")
    print("  📂 Obteniendo schema del formulario...")
    
    # 1. Obtener schema
    schema = obtener_schema_kobo()
    survey = schema.get('survey', [])
    
    # 2. Descargar datos
    print("  📥 Descargando submissions...")
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    resp = requests.get(URL_KOBO, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    
    # 3. Normalizar a DataFrame
    df = pd.json_normalize(data['results'])
    print(f"  ✅ Columnas base extraídas: {len(df.columns)}")
    
    # 4. Buscar campos select_multiple y expandirlos
    print("  ☑️  Expandiendo select_multiple...")
    for q in survey:
        if q.get('type') == 'select_multiple':
            field_name = q.get('$xpath', q.get('name'))
            if field_name in df.columns:
                df = expandir_select_multiple(df, schema, field_name)
    
    print(f"\n  🎉 Extracción completa: {len(df)} registros, {len(df.columns)} columnas")
    return df



def asignar_turno(fecha):
    """Asigna turno basándose en la hora del timestamp"""
    if pd.isnull(fecha): return None
    h = fecha.hour
    if 3 <= h < 8: return "TM"
    elif 8 <= h < 16: return "TO"
    elif 16 <= h < 22: return "TT"
    elif h >= 22 or h < 3: return "TN"
    else: return None

def clasificar_localizacion(puntos_gdf, palermo_gdf, comunas_gdf):
    """
    Clasifica los puntos en 2 pasos secuenciales:
    1. Palermo Norte -> 14.5
    2. Comunas -> 1.0-15.0
    """
    print("--- Iniciando clasificación de localización (2 pasos) ---")
    
    # Asegurar mismo CRS
    puntos_gdf = puntos_gdf.to_crs("EPSG:4326")
    palermo_gdf = palermo_gdf.to_crs("EPSG:4326")
    comunas_gdf = comunas_gdf.to_crs("EPSG:4326")

    # Inicializar como None
    puntos_gdf['Localizacion'] = None

    # PASO 1: Clasificar Palermo Norte como 14.5
    puntos_en_palermo = gpd.sjoin(puntos_gdf, palermo_gdf, how="inner", predicate='within')
    if not puntos_en_palermo.empty:
        print(f"   ✅ {len(puntos_en_palermo)} puntos clasificados como Palermo Norte (14.5).")
        puntos_gdf.loc[puntos_en_palermo.index, 'Localizacion'] = 14.5

    # PASO 2: Clasificar por comunas (solo puntos aún NO clasificados)
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
    
    # Localizacion es float: 14.5=Palermo, 1.0-15.0=Comunas, None=Fuera
    return puntos_gdf['Localizacion']

def asignar_recorrido(gdf, poligonos):
    """Asigna puntos a polígonos de recorrido"""
    print("--- Clasificando Recorridos ---")
    resultado = pd.Series('', index=gdf.index, dtype=object)
    for nombre, poligono in poligonos.items():
        dentro = gdf.within(poligono)
        if dentro.any():
            resultado.loc[dentro] = nombre
    return resultado

def procesar_datos_geoespaciales_total(df_kobo):
    """Procesa datos geoespaciales completos: coordenadas, turno, localización y recorrido"""
    print("\n🗺️  Procesando datos geoespaciales...")
    print("   📍 Separando coordenadas latitud/longitud/altitud/precisión...")
    
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
    print(f"   ✅ Turno asignado")

    puntos_gdf = gpd.GeoDataFrame(
        df_kobo,
        geometry=gpd.points_from_xy(df_kobo.longitude, df_kobo.latitude),
        crs="EPSG:4326"
    )

    try:
        # Cargar Palermo Norte KMZ
        print("   📂 Cargando archivo Palermo Norte...")
        with zipfile.ZipFile(RUTA_KMZ_PALERMO, 'r') as kmz:
            kml_files = [f for f in kmz.namelist() if f.endswith('.kml')]
            if kml_files:
                with kmz.open(kml_files[0]) as kml_file:
                    palermo_gdf = gpd.read_file(kml_file)
            else:
                raise FileNotFoundError("No se encontró KML dentro de Palermo_Norte.kmz")
        if palermo_gdf.crs is None: palermo_gdf.set_crs("EPSG:4326", inplace=True)
        
        # Cargar comunas SHP
        print("   📂 Cargando shapefile de comunas...")
        comunas_gdf = gpd.read_file(RUTA_SHP_COMUNAS)
        
    except Exception as e:
        print(f"❌ ERROR FATAL CARGANDO CAPAS: {e}")
        sys.exit(1)

    poligonos_recorrido = {
        'Recorrido A': Polygon([(-58.41017, -34.588232), (-58.413901, -34.594177), (-58.413904, -34.599714),(-58.400064, -34.600033), (-58.386224, -34.599855), (-58.398154, -34.59498),(-58.404592, -34.593108), (-58.386524, -34.595263), (-58.41017, -34.588232)]),
        'Recorrido B': Polygon([(-58.389185, -34.584593), (-58.395365, -34.587137), (-58.400944, -34.594168),(-58.398154, -34.59498), (-58.386524, -34.595263), (-58.383284, -34.587544),(-58.388112, -34.59256), (-58.389185, -34.584593)]),
        'Recorrido C': Polygon([(-58.400944, -34.594168), (-58.395365, -34.587137), (-58.389185, -34.584593),(-58.398455, -34.580212), (-58.407295, -34.581837), (-58.404592, -34.593108),(-58.41017, -34.588232), (-58.400944, -34.594168)])
    }

    df_kobo['Localizacion'] = clasificar_localizacion(puntos_gdf, palermo_gdf, comunas_gdf)
    df_kobo['Poligono'] = asignar_recorrido(puntos_gdf, poligonos_recorrido)
    print("   ✅ Clasificación geoespacial completada")

    return df_kobo


def subir_a_neon(df):
    """
    Sube el DataFrame a Neon PostgreSQL usando SQLAlchemy.
    Trabaja sobre una copia para no afectar los datos de Sheets.
    IMPORTANTE: Mantiene nombres de columnas EXACTOS (con tildes, espacios, etc.)
    ya que PostgreSQL los soporta con comillas dobles.
    """
    print("\n🔄 Preparando datos para Neon PostgreSQL...")
    
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

def main():
    print("="*60)
    print("🔄 SCRIPT DE RECLASIFICACIÓN - KOBO API + GOOGLE SHEETS")
    print("="*60)
    print("\n📋 Este script va a:")
    print("1. Extraer datos completos de la API de Kobo")
    print("2. Expandir campos select_multiple")
    print("3. Aplicar clasificación geoespacial en 2 pasos:")
    print("   • Palermo Norte → 14.5")
    print("   • Comunas → 1.0-15.0")
    print("4. REEMPLAZAR completamente el contenido del Google Sheet")
    print("5. Subir a Neon PostgreSQL")
    
    respuesta = input("\n¿Continuar? (escribe 'SI' para confirmar): ")
    if respuesta.upper() != 'SI':
        print("❌ Operación cancelada por el usuario")
        sys.exit(0)
    
    # 1. EXTRACCIÓN COMPLETA DE KOBO (con expansión de select_multiple)
    try:
        print("\n" + "="*60)
        print("PASO 1: EXTRACCIÓN DE DATOS DE KOBO")
        print("="*60)
        df_raw = extraer_kobo_completo()
    except Exception as e:
        print(f"❌ Error en extracción de Kobo: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    if df_raw.empty:
        print("⚠️ No hay datos en Kobo")
        sys.exit(0)

    # Hacer backup de los datos raw de Kobo
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file_kobo = f"backup_kobo_raw_{timestamp}.csv"
    df_raw.to_csv(backup_file_kobo, index=False)
    print(f"💾 Backup de datos Kobo guardado: {backup_file_kobo}")

    # 2. PROCESAR GEOESPACIALMENTE
    print("\n" + "="*60)
    print("PASO 2: PROCESAMIENTO GEOESPACIAL")
    print("="*60)
    try:
        df_procesado = procesar_datos_geoespaciales_total(df_raw)
    except Exception as e:
        print(f"❌ Error en procesamiento geoespacial: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    if df_procesado is None or df_procesado.empty:
        print("❌ No hay datos después del procesamiento geoespacial")
        sys.exit(1)

    # 3. FORMATEO Y PREPARACIÓN PARA SHEETS
    print("\n" + "="*60)
    print("PASO 3: FORMATEO DE DATOS")
    print("="*60)
    
    # Formatear fechas y horas
    df_procesado['hora_start'] = df_procesado['start'].dt.strftime('%H:%M:%S')
    df_procesado['start'] = df_procesado['start'].dt.strftime('%Y-%m-%d')

    # Renombrar columnas para coincidir con Google Sheets
    rename_map = {
        'geo_ref/geo_punto': 'Georreferenciación del punto',
        'datos_per/cant_pers': 'Cantidad de personas en situación de calle observadas',
        'caracteristicas_puntos/caracteristicas_observada': 'Características observables del punto',
        'caracteristicas_puntos/estructura': 'estructura',
        'caracteristicas_puntos/colchon': 'colchon',
        'caracteristicas_puntos/NNyA_observa': 'Se observan niños/as en el punto'
    }
    df_procesado.rename(columns=rename_map, inplace=True)

    # Definir columnas deseadas (manteniendo el orden de main_act_flash.py)
    columnas_deseadas = [
        'Turno', 'start', 'hora_start', 'end', 'today', 'username', 'deviceid',
        'Georreferenciación del punto', 'latitude', 'longitude',
        '_Georreferenciación del punto_altitude', '_Georreferenciación del punto_precision',
        'Cantidad de personas en situación de calle observadas','La/s persona/s esta/n',
        'Características observables del punto', 'estructura', 'colchon',
        'Características observables del punto/Basura, ropa, bolsos, etc',
        'Características observables del punto/No se observan cosas',
        'Se observan niños/as en el punto',
        'datos_per/sit_calle',  # Campo sit_calle del API
        '_id', '_uuid', '_submission_time',
        '_validation_status', '_notes', '_status', '_submitted_by', '__version__',
        '_tags', '_index', 'Poligono', 'Localizacion'
    ]
    
    df_final = df_procesado.reindex(columns=columnas_deseadas)

    # FORMATO: Numéricos (Float)
    cols_float = ['latitude', 'longitude', '_Georreferenciación del punto_altitude', '_Georreferenciación del punto_precision']
    for col in cols_float:
        if col in df_final.columns:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

    # FORMATO: Enteros
    cols_enteros = ['Cantidad de personas en situación de calle observadas']
    for col_cant in cols_enteros:
        if col_cant in df_final.columns:
            df_final[col_cant] = pd.to_numeric(df_final[col_cant], errors='coerce').fillna(0).astype(int)

    # FORMATO: Localización (ya viene como float desde clasificar_localizacion)
    if 'Localizacion' in df_final.columns:
        df_final['Localizacion'] = pd.to_numeric(df_final['Localizacion'], errors='coerce')

    # LIMPIEZA CRÍTICA PARA JSON
    def clean_complex_types(val):
        if isinstance(val, (list, dict)):
            return str(val)
        return val

    for col in df_final.columns:
        df_final[col] = df_final[col].apply(clean_complex_types)

    # Reemplazar Infinito por NaN
    df_final = df_final.replace([np.inf, -np.inf], np.nan)

    # Convertir DF a object para permitir None
    df_final = df_final.astype(object)

    # Reemplazar NaN con None
    df_final = df_final.where(pd.notnull(df_final), None)

    # Mostrar estadísticas
    print("\n📊 Resultados de clasificación geoespacial:")
    print(f"   • Total registros procesados: {len(df_final)}")
    if 'Localizacion' in df_final.columns:
        print(f"   • Palermo Norte (14.5): {(df_final['Localizacion'] == 14.5).sum()}")
        print(f"   • Comuna 1-15: {((df_final['Localizacion'] >= 1) & (df_final['Localizacion'] <= 15)).sum()}")
        print(f"   • Sin clasificar: {df_final['Localizacion'].isna().sum()}")

    # 4. SUBIR A GOOGLE SHEETS
    print("\n" + "="*60)
    print("PASO 4: SUBIR A GOOGLE SHEETS")
    print("="*60)
    
    scope = [
        "https://spreadsheets.google.com/feeds",
        'https://www.googleapis.com/auth/spreadsheets',
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive"
    ]
    
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
    sheet = client.open(NOMBRE_SPREADSHEET).worksheet(NOMBRE_HOJA)
    
    # Confirmación final antes de reemplazar
    print(f"\n⚠️  A punto de REEMPLAZAR completamente el sheet '{NOMBRE_HOJA}'")
    print(f"   Registros a cargar: {len(df_final)}")
    respuesta_final = input("¿Confirmas el reemplazo del sheet? (escribe 'SI'): ")
    if respuesta_final.upper() != 'SI':
        print("❌ Operación cancelada")
        sys.exit(0)
    
    print("   🔄 Limpiando sheet...")
    sheet.clear()
    print("   📤 Subiendo datos...")
    sheet.update(
        values=[df_final.columns.values.tolist()] + df_final.values.tolist(),
        value_input_option='USER_ENTERED'
    )
    print(f"   ✅ {len(df_final)} registros cargados a Google Sheets")

    # 5. SUBIR A NEON POSTGRESQL
    print("\n" + "="*60)
    print("PASO 5: SUBIR A NEON POSTGRESQL")
    print("="*60)
    try:
        subir_a_neon(df_final)
        print("   ✅ Carga a Neon PostgreSQL exitosa")
    except Exception as e:
        print(f"   ⚠️  Error en Neon PostgreSQL (no crítico): {e}")
        print(f"   ℹ️  Los datos en Google Sheets se actualizaron correctamente")

    print("\n" + "="*60)
    print("✅ RECLASIFICACIÓN COMPLETADA EXITOSAMENTE")
    print("="*60)
    print(f"📁 Backup Kobo guardado en: {backup_file_kobo}")
    print(f"📊 Total registros procesados: {len(df_final)}")
    print(f"📊 Google Sheet: {NOMBRE_SPREADSHEET} - {NOMBRE_HOJA}")
    print(f"📊 Tabla Neon: {NEON_TABLE_NAME}")
    print("\n🎉 El Google Sheet y Neon PostgreSQL han sido actualizados con la nueva clasificación")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR FATAL: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
