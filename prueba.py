# ================================
# 🔥 COMPARACIÓN: CSV vs API KOBO
# ================================

import pandas as pd
import requests
import os

# Cargar variables de entorno
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("✅ Variables de .env cargadas")
except ImportError:
    print("⚠️  dotenv no disponible, usando variables del sistema")

# -------- CONFIGURACION --------
TOKEN_KOBO = os.environ.get("KOBO_API_TOKEN", "b6a9c8897db4c180b9eff560e890edfb394313db")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
URL_KOBO = f"https://kf.kobotoolbox.org/api/v2/assets/{UID_KOBO}/data.json"

print("\n" + "="*70)
print("📊 COMPARACIÓN: CSV EXPORTADO vs API")
print("="*70)

# -------- 1. LEER CSV EXPORTADO --------
print("\n1️⃣ LEYENDO CSV EXPORTADO DE KOBO...")
csv_file = "Formulario_Flash_Percepcion_calle_-_all_versions_-_labels_-_2026-02-05-18-34-09.csv"

try:
    # Primero detectar el delimitador correcto
    with open(csv_file, 'r', encoding='utf-8') as f:
        first_line = f.readline()
        delimiter = ';' if ';' in first_line else ','
        print(f"   🔍 Delimitador detectado: '{delimiter}'")
    
    df_csv = pd.read_csv(csv_file, delimiter=delimiter)
    print(f"   ✅ CSV leído: {len(df_csv)} registros, {len(df_csv.columns)} columnas")
    
    # Guardar columnas del CSV
    with open("columnas_csv_kobo.txt", "w", encoding="utf-8") as f:
        f.write("COLUMNAS DEL CSV EXPORTADO (con labels)\n")
        f.write("="*70 + "\n\n")
        for i, col in enumerate(df_csv.columns, 1):
            f.write(f"{i}. {col}\n")
    
    print(f"   💾 Columnas guardadas en: columnas_csv_kobo.txt")
    
except Exception as e:
    print(f"   ❌ Error leyendo CSV: {e}")
    df_csv = None

# -------- 2. DESCARGAR VIA API --------
print("\n2️⃣ DESCARGANDO VÍA API...")
headers = {"Authorization": f"Token {TOKEN_KOBO}"}

try:
    response = requests.get(URL_KOBO, headers=headers)
    response.raise_for_status()
    data = response.json()
    df_api = pd.json_normalize(data["results"])
    print(f"   ✅ API descargada: {len(df_api)} registros, {len(df_api.columns)} columnas")
    
    # Guardar columnas del API
    with open("columnas_api_kobo.txt", "w", encoding="utf-8") as f:
        f.write("COLUMNAS DEL API (nombres internos)\n")
        f.write("="*70 + "\n\n")
        for i, col in enumerate(df_api.columns, 1):
            f.write(f"{i}. {col}\n")
    
    print(f"   💾 Columnas guardadas en: columnas_api_kobo.txt")
    
except Exception as e:
    print(f"   ❌ Error con API: {e}")
    df_api = None

# -------- 3. COMPARACIÓN --------
if df_csv is not None and df_api is not None:
    print("\n3️⃣ COMPARACIÓN:")
    print(f"   📋 Columnas en CSV: {len(df_csv.columns)}")
    print(f"   � Columnas en API: {len(df_api.columns)}")
    print(f"   📊 Diferencia: {len(df_csv.columns) - len(df_api.columns)} columnas")
    
    # Mostrar primeras 20 columnas de cada uno
    print("\n   � PRIMERAS 20 COLUMNAS DEL CSV:")
    for i, col in enumerate(list(df_csv.columns)[:20], 1):
        print(f"      {i:2}. {col}")
    
    print("\n   � PRIMERAS 20 COLUMNAS DEL API:")
    for i, col in enumerate(list(df_api.columns)[:20], 1):
        print(f"      {i:2}. {col}")

    # Guardar comparación detallada
    with open("comparacion_csv_vs_api.txt", "w", encoding="utf-8") as f:
        f.write("COMPARACIÓN DETALLADA: CSV vs API\n")
        f.write("="*70 + "\n\n")
        f.write(f"Total columnas CSV: {len(df_csv.columns)}\n")
        f.write(f"Total columnas API: {len(df_api.columns)}\n")
        f.write(f"Diferencia: {len(df_csv.columns) - len(df_api.columns)}\n\n")
        
        f.write("\nCOLUMNAS SOLO EN CSV:\n")
        f.write("-"*70 + "\n")
        csv_only = set(df_csv.columns) - set(df_api.columns)
        for col in sorted(csv_only):
            f.write(f"  - {col}\n")
        
        f.write("\n\nCOLUMNAS SOLO EN API:\n")
        f.write("-"*70 + "\n")
        api_only = set(df_api.columns) - set(df_csv.columns)
        for col in sorted(api_only):
            f.write(f"  - {col}\n")
    
    print(f"\n   💾 Comparación detallada guardada en: comparacion_csv_vs_api.txt")

print("\n" + "="*70)
print("✅ ANÁLISIS COMPLETADO")
print("="*70)

