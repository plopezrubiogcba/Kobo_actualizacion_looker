"""
Prueba de expansión de select_multiple con datos reales
"""
import pandas as pd
import requests
import os
import re

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

TOKEN_KOBO = os.environ.get("KOBO_API_TOKEN")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
KOBO_BASE_URL = "https://kf.kobotoolbox.org"

print("="*80)
print("PRUEBA: EXPANSIÓN DE SELECT_MULTIPLE")
print("="*80)

# 1. Obtener schema
headers = {"Authorization": f"Token {TOKEN_KOBO}"}
url_asset = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/"
resp = requests.get(url_asset, headers=headers)
schema = resp.json().get('content', {})
choices = schema.get('choices', [])
survey = schema.get('survey', [])

print(f"\n1️⃣ SCHEMA DEL SELECT_MULTIPLE")
print("-"*80)

# Buscar el select_multiple
select_mult_field = None
for q in survey:
    if q.get('type') == 'select_multiple':
        select_mult_field = q
        print(f"Campo: {q.get('name')}")
        print(f"Tipo: {q.get('type')}")
        print(f"Label: {q.get('label', [''])[0]}")
        print(f"List name: {q.get('select_from_list_name')}")
        break

if not select_mult_field:
    print("❌ No se encontró campo select_multiple")
    exit(1)

# Obtener opciones
list_name = select_mult_field.get('select_from_list_name')
opciones = [c for c in choices if c.get('list_name') == list_name]

print(f"\nOpciones del select_multiple ({len(opciones)}):")
for opt in opciones:
    print(f"   {opt['name']}: {opt.get('label', [''])[0]}")

# 2. Obtener datos del API
print(f"\n2️⃣ DATOS DEL API")
print("-"*80)

url_data = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/data.json"
resp_data = requests.get(url_data, headers=headers, params={'limit': 10})
results = resp_data.json()['results']

print(f"Registros obtenidos: {len(results)}")

# Normalizar
df = pd.json_normalize(results)
print(f"DataFrame shape: {df.shape}")

# Buscar la columna del select_multiple
col_name = 'caracteristicas_puntos/caracteristicas_observada'
if col_name in df.columns:
    print(f"\n✅ Columna encontrada: {col_name}")
    print(f"Valores únicos:")
    print(df[col_name].value_counts().head(10))
else:
    print(f"\n❌ Columna NO encontrada: {col_name}")
    print(f"Columnas disponibles con 'caracteristica':")
    cols_caract = [c for c in df.columns if 'caracteristica' in c.lower()]
    for c in cols_caract:
        print(f"   - {c}")

# 3. Expandir select_multiple
print(f"\n3️⃣ EXPANSIÓN A COLUMNAS BOOLEANAS")
print("-"*80)

if col_name in df.columns:
    for opt in opciones:
        opt_name = opt['name']
        col_bool_name = f"{col_name}/{opt_name}"
        
        # Crear columna booleana
        df[col_bool_name] = df[col_name].astype(str).str.contains(
            r'\b' + re.escape(opt_name) + r'\b',
            regex=True,
            na=False
        )
        
        count_true = df[col_bool_name].sum()
        print(f"   ✅ {col_bool_name}: {count_true} registros True")
    
    print(f"\n📊 RESULTADO:")
    print(f"   Columnas originales: {df.shape[1] - len(opciones)}")
    print(f"   Columnas expandidas: {len(opciones)}")
    print(f"   Total final: {df.shape[1]}")
    
    # Mostrar ejemplo
    print(f"\n📋 EJEMPLO DE DATOS EXPANDIDOS:")
    print("-"*80)
    
    cols_mostrar = [col_name] + [f"{col_name}/{opt['name']}" for opt in opciones[:3]]
    print(df[cols_mostrar].head(3).to_string())
    
    # Guardar
    df.to_csv("test_expansion_select_multiple.csv", index=False)
    print(f"\n💾 Datos guardados en: test_expansion_select_multiple.csv")
    
    print(f"\n✅ EXPANSIÓN EXITOSA")
else:
    print(f"\n❌ NO SE PUDO EXPANDIR - Columna no encontrada")
