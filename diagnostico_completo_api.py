"""
DIAGNÓSTICO COMPLETO: ¿Por qué la API no devuelve todas las columnas?
======================================================================

Teorías a investigar:
1. API Key desactualizada - generada antes de agregar nuevos campos
2. Parámetros faltantes en request - necesita flags especiales
3. Endpoint incorrecto - usar /data vs /data.json vs otros
4. Versiones del formulario - API devuelve solo última versión
5. Permisos de API key - no tiene acceso a ciertos campos
"""

import requests
import json
import os
import pandas as pd

# Cargar vars
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

TOKEN_KOBO = os.environ.get("KOBO_API_TOKEN")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
KOBO_BASE_URL = "https://kf.kobotoolbox.org"

headers = {"Authorization": f"Token {TOKEN_KOBO}"}

print("="*70)
print("🔍 DIAGNÓSTICO COMPLETO DE API KOBO")
print("="*70)

# ============================================================
# TEST 1: Verificar información del Asset
# ============================================================
print("\n📋 TEST 1: Información del Asset")
print("-"*70)

asset_url = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/"
resp = requests.get(asset_url, headers=headers)
asset_data = resp.json()

print(f"   Nombre: {asset_data.get('name')}")
print(f"   UID: {asset_data.get('uid')}")
print(f"   Deployment status: {asset_data.get('deployment_status')}")
print(f"   Tiene deployment: {asset_data.get('has_deployment')}")
print(f"   Version ID actual: {asset_data.get('version_id')}")

# Guardar asset completo
with open("asset_info.json", "w", encoding="utf-8") as f:
    json.dump(asset_data, f, indent=2, ensure_ascii=False)
print(f"   💾 Asset info guardado en: asset_info.json")

# ============================================================
# TEST 2: Analizar el schema/content del formulario
# ============================================================
print("\n📋 TEST 2: Schema del Formulario")
print("-"*70)

content = asset_data.get('content', {})
survey = content.get('survey', [])

print(f"   Total preguntas en schema: {len(survey)}")
print(f"\n   Listado de campos:")

schema_fields = []
for i, q in enumerate(survey, 1):
    name = q.get('name', 'N/A')
    tipo = q.get('type', 'N/A')
    label = q.get('label')
    if isinstance(label, list):
        label = label[0] if label else 'N/A'
    elif isinstance(label, dict):
        label = label.get('English', label.get('Spanish', 'N/A'))
    
    schema_fields.append(name)
    print(f"   {i:2}. {name:40} ({tipo})")

# ============================================================
# TEST 3: Probar diferentes endpoints de datos
# ============================================================
print("\n📋 TEST 3: Probando Diferentes Endpoints")
print("-"*70)

endpoints_to_test = [
    ("/api/v2/assets/{}/data.json", "Standard JSON endpoint"),
    ("/api/v2/assets/{}/data/", "Data endpoint without format"),
    ("/api/v2/assets/{}/data/?format=json", "Data with format param"),
]

results = {}
for endpoint_template, description in endpoints_to_test:
    endpoint = endpoint_template.format(UID_KOBO)
    url = f"{KOBO_BASE_URL}{endpoint}"
    
    print(f"\n   🔹 Testing: {description}")
    print(f"      URL: {url}")
    
    try:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        
        if 'results' in data and data['results']:
            first_record = data['results'][0]
            column_count = len(first_record.keys())
            results[description] = {
                'columns': list(first_record.keys()),
                'count': column_count
            }
            print(f"      ✅ Columnas obtenidas: {column_count}")
        else:
            print(f"      ⚠️  Sin resultados")
            
    except Exception as e:
        print(f"      ❌ Error: {e}")

# ============================================================
# TEST 4: Probar con parámetros adicionales
# ============================================================
print("\n📋 TEST 4: Probando con Parámetros Adicionales")
print("-"*70)

params_to_test = [
    ({}, "Sin parámetros"),
    ({'query': '{}'}, "Con query vacío"),
    ({'fields': '[]'}, "Con fields vacío"),
    ({'format': 'json'}, "Con format=json"),
]

url_base = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/data.json"

for params, description in params_to_test:
    print(f"\n   🔹 Testing: {description}")
    print(f"      Params: {params}")
    
    try:
        resp = requests.get(url_base, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        
        if 'results' in data and data['results']:
            first_record = data['results'][0]
            column_count = len(first_record.keys())
            print(f"      ✅ Columnas: {column_count}")
            
            if column_count > 25:
                print(f"      🎉 ¡ENCONTRADO! Este devuelve más columnas")
                with open(f"api_response_{description.replace(' ', '_')}.json", "w", encoding="utf-8") as f:
                    json.dump(first_record, f, indent=2, ensure_ascii=False)
        else:
            print(f"      ⚠️  Sin resultados")
            
    except Exception as e:
        print(f"      ❌ Error: {e}")

# ============================================================
# TEST 5: Comparar campos del schema vs API
# ============================================================
print("\n📋 TEST 5: Comparación Schema vs API Actual")
print("-"*70)

# Obtener datos actuales del API
resp = requests.get(f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/data.json", headers=headers)
api_data = resp.json()['results'][0] if resp.json()['results'] else {}
api_fields = set(api_data.keys())

print(f"   Campos en schema: {len(schema_fields)}")
print(f"   Campos en API: {len(api_fields)}")

# Campos del schema que NO están en el API
missing_in_api = set(schema_fields) - api_fields
if missing_in_api:
    print(f"\n   ❌ Campos del SCHEMA que NO aparecen en API ({len(missing_in_api)}):")
    for field in sorted(missing_in_api):
        print(f"      - {field}")

# Campos del API que NO están en el schema
extra_in_api = api_fields - set(schema_fields)
if extra_in_api:
    print(f"\n   ➕ Campos del API que NO están en schema ({len(extra_in_api)}):")
    for field in sorted(extra_in_api):
        print(f"      - {field}")

# ============================================================
# TEST 6: Leer CSV y comparar nombres de campos
# ============================================================
print("\n📋 TEST 6: Comparar CSV Export con API")
print("-"*70)

csv_file = "Formulario_Flash_Percepcion_calle_-_all_versions_-_labels_-_2026-02-05-18-34-09.csv"
df_csv = pd.read_csv(csv_file, delimiter=';')
csv_columns = set(df_csv.columns)

print(f"   Columnas en CSV: {len(csv_columns)}")
print(f"   Columnas en API: {len(api_fields)}")
print(f"   Diferencia: {len(csv_columns) - len(api_fields)}")

# El CSV usa LABELS, necesitamos mapear labels a nombres internos
print(f"\n   ℹ️  CSV usa LABELS (nombres traducidos)")
print(f"   ℹ️  API usa NOMBRES INTERNOS (nombres de campo)")

# ============================================================
# RESUMEN
# ============================================================
print("\n" + "="*70)
print("📊 RESUMEN DEL DIAGNÓSTICO")
print("="*70)
print(f"✅ Campos en schema del formulario: {len(survey)}")
print(f"✅ Columnas que API devuelve: {len(api_fields)}")
print(f"✅ Columnas en CSV exportado: {len(csv_columns)}")
print(f"❌ Campos faltantes en API: {len(missing_in_api) if missing_in_api else 0}")

if missing_in_api:
    print(f"\n🔥 PROBLEMA IDENTIFICADO:")
    print(f"   El schema tiene {len(missing_in_api)} campos que la API NO devuelve")
    print(f"   Revisar archivo asset_info.json para más detalles")

print("\n💾 Archivos generados:")
print("   - asset_info.json (información completa del asset)")
print("   - Respuestas JSON de tests exitosos")
