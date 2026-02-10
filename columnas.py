"""
Script de Validación de Columnas - Kobo Extract Complete
=========================================================

Este script sirve para:
1. Probar la extracción completa de Kobo (con expansión de select_multiple)
2. Comparar columnas entre diferentes fuentes (API, CSV, Neon)
3. Validar que todas las columnas esperadas estén presentes

Basado en la implementación de extracción completa de datos de Kobo.
"""

import pandas as pd
import requests
import os
from sqlalchemy import create_engine, inspect

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("⚠️  dotenv no disponible")

# Configuración
TOKEN_KOBO = os.environ.get("KOBO_API_TOKEN")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
KOBO_BASE_URL = os.environ.get("KOBO_BASE_URL", "https://kf.kobotoolbox.org")
DATABASE_URL = os.environ.get("DATABASE_URL")

print("="*80)
print("🔍 VALIDACIÓN DE COLUMNAS - KOBO EXTRACTION")
print("="*80)

# ============================================================================
# 1. OBTENER COLUMNAS DEL SCHEMA DE KOBO
# ============================================================================
def obtener_columnas_schema():
    """Obtiene todas las columnas definidas en el schema del formulario"""
    print("\n1️⃣ OBTENIENDO SCHEMA DEL FORMULARIO...")
    
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    url = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/"
    
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        asset = resp.json()
        
        content = asset.get('content', {})
        survey = content.get('survey', [])
        choices = content.get('choices', [])
        
        campos = []
        select_multiple_expansions = []
        
        # Procesar campos del survey
        for q in survey:
            name = q.get('name')
            tipo = q.get('type')
            
            if tipo in ['begin_group', 'end_group']:
                continue
                
            campos.append(name)
            
            # Si es select_multiple, añadir las expansiones esperadas
            if tipo == 'select_multiple':
                list_name = q.get('select_from_list_name') or q.get('list_name')
                if list_name:
                    # Buscar opciones
                    opciones = [c['name'] for c in choices if c.get('list_name') == list_name]
                    for opcion in opciones:
                        expansion = f"{name}/{opcion}"
                        select_multiple_expansions.append(expansion)
        
        print(f"   ✅ Campos base del schema: {len(campos)}")
        print(f"   ✅ Expansiones select_multiple: {len(select_multiple_expansions)}")
        
        return {
            'campos_base': campos,
            'select_multiple': select_multiple_expansions,
            'total': campos + select_multiple_expansions
        }
    
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None

# ============================================================================
# 2. OBTENER COLUMNAS DEL API (datos reales)
# ============================================================================
def obtener_columnas_api():
    """Obtiene columnas de los datos descargados del API"""
    print("\n2️⃣ OBTENIENDO COLUMNAS DEL API...")
    
    headers = {"Authorization": f"Token {TOKEN_KOBO}"}
    url = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/data.json"
    
    try:
        resp = requests.get(url, headers=headers, params={'limit': 1})
        resp.raise_for_status()
        data = resp.json()
        
        if data['results']:
            columnas = list(data['results'][0].keys())
            print(f"   ✅ Columnas en API: {len(columnas)}")
            return columnas
        else:
            print(f"   ⚠️  No hay datos en el API")
            return []
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return []

# ============================================================================
# 3. OBTENER COLUMNAS DE NEON
# ============================================================================
def obtener_columnas_neon():
    """Obtiene columnas de la tabla en Neon PostgreSQL"""
    print("\n3️⃣ OBTENIENDO COLUMNAS DE NEON...")
    
    if not DATABASE_URL:
        print("   ⚠️  DATABASE_URL no configurado")
        return []
    
    try:
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)
        
        if inspector.has_table('kobo_flash_consolidado'):
            columns = [col['name'] for col in inspector.get_columns('kobo_flash_consolidado')]
            print(f"   ✅ Columnas en Neon: {len(columns)}")
            return columns
        else:
            print("   ⚠️  Tabla kobo_flash_consolidado no existe")
            return []
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return []

# ============================================================================
# 4. COMPARAR COLUMNAS
# ============================================================================
def comparar_columnas(schema_cols, api_cols, neon_cols):
    """Compara columnas entre diferentes fuentes"""
    print("\n4️⃣ COMPARACIÓN DE COLUMNAS")
    print("-"*80)
    
    schema_set = set(schema_cols['total']) if schema_cols else set()
    api_set = set(api_cols)
    neon_set = set(neon_cols)
    
    # Columnas que deberían estar
    print("\n📋 COLUMNAS ESPERADAS DEL SCHEMA:")
    if schema_cols:
        print(f"   Campos base: {len(schema_cols['campos_base'])}")
        for campo in schema_cols['campos_base'][:10]:
            print(f"      - {campo}")
        if len(schema_cols['campos_base']) > 10:
            print(f"      ... y {len(schema_cols['campos_base']) - 10} más")
        
        print(f"\n   Expansiones select_multiple: {len(schema_cols['select_multiple'])}")
        for exp in schema_cols['select_multiple']:
            print(f"      - {exp}")
    
    # Columnas en API
    print(f"\n📡 COLUMNAS EN API: {len(api_cols)}")
    
    # Columnas en Neon
    print(f"\n🗄️ COLUMNAS EN NEON: {len(neon_cols)}")
    
    # Análisis
    if api_cols and schema_set:
        missing_in_api = schema_set - api_set
        extra_in_api = api_set - schema_set
        
        if missing_in_api:
            print(f"\n⚠️  FALTAN EN API ({len(missing_in_api)}):")
            for col in sorted(missing_in_api)[:20]:
                print(f"      - {col}")
            if len(missing_in_api) > 20:
                print(f"      ... y {len(missing_in_api) - 20} más")
        
        if extra_in_api:
            print(f"\n➕ EXTRAS EN API ({len(extra_in_api)}) [Metadatos]:")
            for col in sorted(extra_in_api)[:10]:
                print(f"      - {col}")
    
    if neon_cols and api_cols:
        missing_in_neon = api_set - neon_set
        extra_in_neon = neon_set - api_set
        
        if missing_in_neon:
            print(f"\n⚠️  FALTAN EN NEON ({len(missing_in_neon)}):")
            for col in sorted(missing_in_neon)[:20]:
                print(f"      - {col}")
        
        if extra_in_neon:
            print(f"\n➕ EXTRAS EN NEON ({len(extra_in_neon)}):")
            for col in sorted(extra_in_neon)[:20]:
                print(f"      - {col}")

# ============================================================================
# 5. VERIFICAR SELECT_MULTIPLE
# ============================================================================
def verificar_select_multiple(schema_cols, api_cols, neon_cols):
    """Verifica específicamente las columnas de select_multiple expandidas"""
    print("\n5️⃣ VERIFICACIÓN SELECT_MULTIPLE")
    print("-"*80)
    
    if not schema_cols or not schema_cols['select_multiple']:
        print("   ℹ️  No hay campos select_multiple en el schema")
        return
    
    expansiones_esperadas = schema_cols['select_multiple']
    print(f"\n📊 Expansiones esperadas: {len(expansiones_esperadas)}")
    
    # Verificar en API
    en_api = [col for col in expansiones_esperadas if col in api_cols]
    print(f"\n   API: {len(en_api)}/{len(expansiones_esperadas)}")
    if len(en_api) < len(expansiones_esperadas):
        faltantes = set(expansiones_esperadas) - set(api_cols)
        print(f"   ⚠️  Faltan {len(faltantes)} columnas expandidas en API")
        print(f"   ℹ️  Esto es NORMAL - el API no expande select_multiple automáticamente")
    
    # Verificar en Neon
    en_neon = [col for col in expansiones_esperadas if col in neon_cols]
    print(f"\n   NEON: {len(en_neon)}/{len(expansiones_esperadas)}")
    if len(en_neon) < len(expansiones_esperadas):
        faltantes = set(expansiones_esperadas) - set(neon_cols)
        print(f"   ⚠️  Faltan {len(faltantes)} columnas expandidas en Neon")
        if faltantes:
            print(f"   Faltantes:")
            for col in sorted(faltantes):
                print(f"      - {col}")

# ============================================================================
# 6. GENERAR REPORTE
# ============================================================================
def generar_reporte(schema_cols, api_cols, neon_cols):
    """Genera archivo de reporte con todas las columnas"""
    print("\n6️⃣ GENERANDO REPORTE...")
    
    with open("reporte_columnas_kobo.txt", "w", encoding="utf-8") as f:
        f.write("="*80 + "\n")
        f.write("REPORTE DE COLUMNAS - KOBO EXTRACTION\n")
        f.write("="*80 + "\n\n")
        
        # Schema
        if schema_cols:
            f.write(f"COLUMNAS DEL SCHEMA ({len(schema_cols['total'])}):\n")
            f.write("-"*80 + "\n")
            f.write(f"\nCampos base ({len(schema_cols['campos_base'])}):\n")
            for i, col in enumerate(schema_cols['campos_base'], 1):
                f.write(f"  {i:3}. {col}\n")
            
            f.write(f"\nExpansiones select_multiple ({len(schema_cols['select_multiple'])}):\n")
            for i, col in enumerate(schema_cols['select_multiple'], 1):
                f.write(f"  {i:3}. {col}\n")
            f.write("\n")
        
        # API
        f.write(f"\nCOLUMNAS EN API ({len(api_cols)}):\n")
        f.write("-"*80 + "\n")
        for i, col in enumerate(sorted(api_cols), 1):
            f.write(f"  {i:3}. {col}\n")
        f.write("\n")
        
        # Neon
        f.write(f"\nCOLUMNAS EN NEON ({len(neon_cols)}):\n")
        f.write("-"*80 + "\n")
        for i, col in enumerate(sorted(neon_cols), 1):
            f.write(f"  {i:3}. {col}\n")
    
    print("   ✅ Reporte guardado en: reporte_columnas_kobo.txt")

# ============================================================================
# MAIN
# ============================================================================
if __name__ == "__main__":
    try:
        # Obtener columnas de cada fuente
        schema_cols = obtener_columnas_schema()
        api_cols = obtener_columnas_api()
        neon_cols = obtener_columnas_neon()
        
        # Comparar
        comparar_columnas(schema_cols, api_cols, neon_cols)
        
        # Verificar select_multiple específicamente
        verificar_select_multiple(schema_cols, api_cols, neon_cols)
        
        # Generar reporte
        generar_reporte(schema_cols, api_cols, neon_cols)
        
        # Resumen final
        print("\n" + "="*80)
        print("📊 RESUMEN")
        print("="*80)
        if schema_cols:
            print(f"Schema: {len(schema_cols['total'])} columnas esperadas")
        print(f"API:    {len(api_cols)} columnas")
        print(f"Neon:   {len(neon_cols)} columnas")
        
        print("\n✅ VALIDACIÓN COMPLETADA")
        
    except Exception as e:
        print(f"\n❌ Error general: {e}")
        import traceback
        traceback.print_exc()
