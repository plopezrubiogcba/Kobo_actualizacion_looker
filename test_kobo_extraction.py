"""
Script de prueba para validar extracción completa de Kobo
========================================================

Este script prueba la función de extracción completa sin ejecutar
todo el procesamiento geoespacial.
"""

import requests
import pandas as pd
import re
import os
import sys

# Agregar el directorio actual al path para importar funciones
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Cargar .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Configuración
TOKEN_KOBO = os.environ.get("KOBO_API_TOKEN")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
KOBO_BASE_URL = os.environ.get("KOBO_BASE_URL", "https://kf.kobotoolbox.org")
URL_KOBO = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/data.json"
URL_KOBO_ASSET = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/"

# Importar funciones del script principal
from main_act_flash import (
    obtener_schema_kobo,
    expandir_geopoint,
    expandir_select_multiple,
    asegurar_columnas_schema,
    extraer_kobo_completo
)

def main():
    print("="*70)
    print("PRUEBA DE EXTRACCIÓN COMPLETA DE KOBO")
    print("="*70)
    
    # Ejecutar extracción
    try:
        df = extraer_kobo_completo()
        
        print(f"\n📊 RESULTADOS:")
        print(f"   Registros: {len(df)}")
        print(f"   Columnas totales: {len(df.columns)}")
        
        # Mostrar columnas por categoría
        print(f"\n📋 COLUMNAS DETECTADAS:")
        
        # Columnas de geolocalización
        geo_cols = [col for col in df.columns if 'latitude' in col.lower() or 'longitude' in col.lower() or 'altitude' in col.lower() or 'precision' in col.lower()]
        print(f"\n   🗺️  Geolocalización ({len(geo_cols)} columnas):")
        for col in geo_cols[:10]:  # Primeras 10
            print(f"      - {col}")
        
        # Columnas de select_multiple (booleanas)
        bool_cols = [col for col in df.columns if df[col].dtype == bool]
        print(f"\n   ☑️  Select Multiple expandidos ({len(bool_cols)} columnas):")
        for col in bool_cols[:10]:  # Primeras 10
            print(f"      - {col}")
        
        # Columnas de datos de personas
        person_cols = [col for col in df.columns if 'datos_per' in col or 'persona' in col.lower()]
        print(f"\n   👤 Datos de personas ({len(person_cols)} columnas):")
        for col in person_cols:
            print(f"      - {col}")
        
        # Columnas de características
        char_cols = [col for col in df.columns if 'caracteristicas' in col]
        print(f"\n   🏠 Características ({len(char_cols)} columnas):")
        for col in char_cols[:15]:  # Primeras 15
            print(f"      - {col}")
        
        # Metadatos
        meta_cols = [col for col in df.columns if col.startswith('_') or col.startswith('meta/')]
        print(f"\n   📄 Metadatos ({len(meta_cols)} columnas):")
        for col in meta_cols[:10]:
            print(f"      - {col}")
        
        # Guardar lista completa de columnas
        output_file = "columnas_kobo_completas.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("COLUMNAS COMPLETAS DE KOBO\n")
            f.write("="*70 + "\n\n")
            for i, col in enumerate(sorted(df.columns), 1):
                f.write(f"{i}. {col}\n")
        
        print(f"\n💾 Lista completa guardada en: {output_file}")
        
        # Guardar muestra de datos
        sample_file = "muestra_kobo.csv"
        df.head(5).to_csv(sample_file, index=False)
        print(f"💾 Muestra de datos guardada en: {sample_file}")
        
        print(f"\n✅ EXTRACCIÓN EXITOSA!")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
