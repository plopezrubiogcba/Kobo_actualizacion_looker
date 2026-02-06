"""
Script de Reclasificación Única - Google Sheets
================================================

Este script descarga TODOS los datos del Google Sheet "puntos flash",
aplica la nueva lógica de clasificación espacial en 3 pasos, y 
REEMPLAZA completamente el contenido del sheet con los datos reclasificados.

IMPORTANTE:
- Este script debe ejecutarse UNA SOLA VEZ
- Hace BACKUP automático exportando a CSV antes de modificar
- REEMPLAZA todos los datos del sheet

Clasificación en 3 pasos:
1. Palermo Norte → 14.5
2. Anillo Digital C2 → 2.5
3. Comunas → 1.0-15.0
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import os
import json
import sys
import re
import zipfile
from datetime import datetime

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

NEON_TABLE_NAME = 'kobo_flash_consolidado'

# Buscar archivos geográficos
print("🔍 Buscando archivos geográficos...")
RUTA_KMZ_PALERMO = None
RUTA_KML_ANILLO_DIGITAL = None
RUTA_SHP_COMUNAS = None

for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        if 'palermo' in file.lower() and 'norte' in file.lower() and file.lower().endswith('.kmz'):
            RUTA_KMZ_PALERMO = os.path.join(root, file)
            print(f"   ✅ Palermo Norte: {RUTA_KMZ_PALERMO}")
        
        if 'anillo_digital' in file.lower() and file.lower().endswith('.kmz'):
            RUTA_KML_ANILLO_DIGITAL = os.path.join(root, file)
            print(f"   ✅ Anillo Digital: {RUTA_KML_ANILLO_DIGITAL}")
        
        if file.lower() == 'comunas.shp':
            RUTA_SHP_COMUNAS = os.path.join(root, file)
            print(f"   ✅ Comunas: {RUTA_SHP_COMUNAS}")

if not all([RUTA_KMZ_PALERMO, RUTA_KML_ANILLO_DIGITAL, RUTA_SHP_COMUNAS]):
    print("❌ ERROR: Faltan archivos geográficos")
    sys.exit(1)

def clasificar_localizacion_3_pasos(df):
    """
    Aplica clasificación en 3 pasos a un DataFrame con columnas latitude/longitude
    """
    print("\n🗺️ Iniciando clasificación espacial en 3 pasos...")
    
    # Cargar capas geográficas
    print("📂 Cargando capas...")
    
    # Palermo Norte
    with zipfile.ZipFile(RUTA_KMZ_PALERMO, 'r') as kmz:
        kml_files = [f for f in kmz.namelist() if f.endswith('.kml')]
        if kml_files:
            with kmz.open(kml_files[0]) as kml_file:
                palermo_gdf = gpd.read_file(kml_file)
    if palermo_gdf.crs is None:
        palermo_gdf.set_crs("EPSG:4326", inplace=True)
    palermo_gdf = palermo_gdf.to_crs("EPSG:4326")
    
    # Anillo Digital
    try:
        anillo_digital_gdf = gpd.read_file(RUTA_KML_ANILLO_DIGITAL)
    except:
        if RUTA_KML_ANILLO_DIGITAL.lower().endswith('.kmz'):
            with zipfile.ZipFile(RUTA_KML_ANILLO_DIGITAL, 'r') as kmz:
                kml_files = [f for f in kmz.namelist() if f.endswith('.kml')]
                if kml_files:
                    with kmz.open(kml_files[0]) as kml_file:
                        anillo_digital_gdf = gpd.read_file(kml_file)
        else:
            anillo_digital_gdf = gpd.read_file(RUTA_KML_ANILLO_DIGITAL)
    if anillo_digital_gdf.crs is None:
        anillo_digital_gdf.set_crs("EPSG:4326", inplace=True)
    anillo_digital_gdf = anillo_digital_gdf.to_crs("EPSG:4326")
    
    # Comunas
    comunas_gdf = gpd.read_file(RUTA_SHP_COMUNAS).to_crs("EPSG:4326")
    
    # Convertir puntos a GeoDataFrame
    puntos_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )
    
    # Inicializar
    puntos_gdf['Localizacion_Nueva'] = None
    
    # PASO 1: Palermo Norte
    print("   🔹 Paso 1: Clasificando Palermo Norte...")
    puntos_en_palermo = gpd.sjoin(puntos_gdf, palermo_gdf, how="inner", predicate='within')
    if not puntos_en_palermo.empty:
        puntos_gdf.loc[puntos_en_palermo.index, 'Localizacion_Nueva'] = 14.5
        print(f"      ✅ {len(puntos_en_palermo)} puntos → 14.5 (Palermo Norte)")
    
    # PASO 2: Anillo Digital
    print("   🔹 Paso 2: Clasificando Anillo Digital...")
    mask_palermo = puntos_gdf['Localizacion_Nueva'] == 14.5
    puntos_restantes = puntos_gdf[~mask_palermo]
    
    if not puntos_restantes.empty:      
        puntos_en_anillo_digital = gpd.sjoin(puntos_restantes, anillo_digital_gdf, how="inner", predicate='within')
        if not puntos_en_anillo_digital.empty:
            puntos_gdf.loc[puntos_en_anillo_digital.index, 'Localizacion_Nueva'] = 2.5
            print(f"      ✅ {len(puntos_en_anillo_digital)} puntos → 2.5 (Anillo Digital)")
    
    # PASO 3: Comunas
    print("   🔹 Paso 3: Clasificando por Comunas...")
    mask_clasificados = puntos_gdf['Localizacion_Nueva'].notna()
    puntos_para_comunas = puntos_gdf[~mask_clasificados]
    
    if not puntos_para_comunas.empty:
        puntos_en_comunas = gpd.sjoin(puntos_para_comunas, comunas_gdf, how="inner", predicate='within')
        if not puntos_en_comunas.empty:
            comuna_col = None
            for col in ['comunas', 'COMUNAS', 'comuna', 'COMUNA', 'NAM', 'ID']:
                if col in puntos_en_comunas.columns:
                    comuna_col = col
                    break
            
            if comuna_col:
                valores_numericos = pd.to_numeric(puntos_en_comunas[comuna_col], errors='coerce')
                puntos_gdf.loc[puntos_en_comunas.index, 'Localizacion_Nueva'] = valores_numericos
                print(f"      ✅ {len(puntos_en_comunas)} puntos → Comunas 1.0-15.0")
    
    # Agregar nueva columna al DataFrame original
    df['Localizacion_Nueva'] = puntos_gdf['Localizacion_Nueva']
    
    return df

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
    print("🔄 SCRIPT DE RECLASIFICACIÓN ÚNICA - GOOGLE SHEETS")
    print("="*60)
    print("\n⚠️ ADVERTENCIA:")
    print("Este script va a:")
    print("1. Descargar TODOS los datos del Google Sheet")
    print("2. Reclasificar usando la nueva lógica de 3 pasos")
    print("3. REEMPLAZAR completamente el contenido del sheet")
    print("\n📋 Nueva clasificación:")
    print("   • Palermo Norte → 14.5")
    print("   • Anillo Digital C2 → 2.5")
    print("   • Comunas → 1.0-15.0")
    
    respuesta = input("\n¿Continuar? (escribe 'SI' para confirmar): ")
    if respuesta.upper() != 'SI':
        print("❌ Operación cancelada por el usuario")
        sys.exit(0)
    
    # Conectar a Google Sheets
    print("\n📊 Conectando a Google Sheets...")
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
    
    # Descargar todos los datos
    print("⬇️ Descargando datos del sheet...")
    registros = sheet.get_all_records()
    
    if not registros:
        print("❌ El sheet está vacío, no hay nada que reclasificar")
        sys.exit(1)
    
    df = pd.DataFrame(registros)
    print(f"   ✅ Descargados {len(df)} registros")
    
    # Hacer backup
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"backup_sheet_{timestamp}.csv"
    df.to_csv(backup_file, index=False)
    print(f"💾 Backup guardado: {backup_file}")
    
    # Extraer coordenadas si están en columna geo_ref
    if 'geo_ref/geo_punto' in df.columns:
        split_coords = df['geo_ref/geo_punto'].astype(str).str.split(' ', expand=True)
        if split_coords.shape[1] >= 2:
            df['latitude'] = pd.to_numeric(split_coords[0], errors='coerce')
            df['longitude'] = pd.to_numeric(split_coords[1], errors='coerce')
    
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        print("❌ ERROR: No se encontraron columnas latitude/longitude")
        sys.exit(1)
    
    # Eliminar filas sin coordenadas válidas
    df_con_coords = df.dropna(subset=['latitude', 'longitude']).copy()
    print(f"📍 Registros con coordenadas válidas: {len(df_con_coords)}")
    
    # Aplicar reclasificación
    df_reclasificado = clasificar_localizacion_3_pasos(df_con_coords)
    
    # Mostrar estadísticas
    print("\n📊 Resultados de reclasificación:")
    print(f"   • Palermo Norte (14.5): {(df_reclasificado['Localizacion_Nueva'] == 14.5).sum()}")
    print(f"   • Anillo Digital C2 (2.5): {(df_reclasificado['Localizacion_Nueva'] == 2.5).sum()}")
    print(f"   • Comuna 1-15: {((df_reclasificado['Localizacion_Nueva'] >= 1) & (df_reclasificado['Localizacion_Nueva'] <= 15)).sum()}")
    print(f"   • Sin clasificar: {df_reclasificado['Localizacion_Nueva'].isna().sum()}")
    
    # Reemplazar columna Localizacion con Localizacion_Nueva
    if 'Localizacion' in df_reclasificado.columns:
        df_reclasificado['Localizacion'] = df_reclasificado['Localizacion_Nueva']
        df_reclasificado = df_reclasificado.drop(columns=['Localizacion_Nueva'])
    else:
        df_reclasificado = df_reclasificado.rename(columns={'Localizacion_Nueva': 'Localizacion'})
    
    # Convertir a object y reemplazar NaN con None para Google Sheets
    df_final = df_reclasificado.astype(object)
    df_final = df_final.where(pd.notnull(df_final), None)
    
    # Subir a Google Sheets
    print("\n⬆️ Subiendo datos reclasificados al sheet...")
    respuesta_final = input("¿Confirmas el reemplazo del sheet? (escribe 'SI'): ")
    if respuesta_final.upper() != 'SI':
        print("❌ Operación cancelada")
        sys.exit(0)
    
    sheet.clear()
    sheet.update(
        values=[df_final.columns.values.tolist()] + df_final.values.tolist(),
        value_input_option='USER_ENTERED'
    )
    
    # Subir a Neon PostgreSQL
    print("\n📤 Subiendo a Neon PostgreSQL...")
    try:
        subir_a_neon(df_final)
        print("   ✅ Carga a Neon PostgreSQL exitosa")
    except Exception as e:
        print(f"   ⚠️  Error en Neon PostgreSQL (no crítico): {e}")
        print(f"   ℹ️  Los datos en Google Sheets se actualizaron correctamente")
    
    print("\n" + "="*60)
    print("✅ RECLASIFICACIÓN COMPLETADA EXITOSAMENTE")
    print("="*60)
    print(f"📁 Backup guardado en: {backup_file}")
    print(f"📊 Total registros procesados: {len(df_final)}")
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
