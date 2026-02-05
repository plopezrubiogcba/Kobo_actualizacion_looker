"""
Script de Prueba para reclassify_sheet_once.py
==============================================
Este script simula la lectura de Google Sheets y aplica el mismo
procesamiento que reclassify_sheet_once.py para validar la extracción.
"""

import pandas as pd
import os

# Configuración
CSV_TEST = "Formulario_Flash_Percepcion_calle_-_all_versions_-_labels_-_2026-02-05-18-34-09.csv"

print("="*70)
print("🧪 PRUEBA DE PROCESAMIENTO - reclassify_sheet_once.py")
print("="*70)

# Leer CSV como si fuera Google Sheets
print("\n1️⃣ Leyendo CSV (simula Google Sheets)...")
df = pd.read_csv(CSV_TEST, delimiter=';')
print(f"   ✅ {len(df)} registros, {len(df.columns)} columnas")

# Mostrar columnas originales
print("\n2️⃣ Columnas originales (primeras 20):")
for i, col in enumerate(list(df.columns)[:20], 1):
    print(f"   {i:2}. {col}")

# Aplicar rename_map (el mismo que reclassify_sheet_once.py)
print("\n3️⃣ Aplicando rename_map...")

rename_map = {
    # Mapeos de nombres bonitos (ya existentes en Sheets)
    'Georreferenciación del punto': 'Georreferenciación del punto',
    'Cantidad de personas en situación de calle observadas': 'Cantidad de personas en situación de calle observadas',
    'La/s persona/s esta/n': 'La/s persona/s esta/n',
    'Ingrese número de documento del usuario que completa el formulario': 'Ingrese número de documento del usuario que completa el formulario',
    'Ingrese solo los digitos de su documento': 'Ingrese solo los digitos de su documento',
    
    # Mapeos de nombres internos Kobo (si existen)
    'geo_ref/geo_punto': 'Georreferenciación del punto',
    'datos_per/cant_pers': 'Cantidad de personas en situación de calle observadas',
    'datos_per/sit_calle': 'La/s persona/s esta/n',
    'datos_per/dni_operador': 'Ingrese número de documento del usuario que completa el formulario',
    'caracteristicas_puntos/NNyA_observa': 'Se observan niños/as en el lugar',
    'caracteristicas_puntos/caracteristicas_observada': 'En el lugar hay…',
    
    '__version__': '_version_'
}

# Aplicar solo renombrados existentes
existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
if existing_renames:
    df.rename(columns=existing_renames, inplace=True)
    print(f"   ✅ {len(existing_renames)} columnas renombradas")
else:
    print(f"   ℹ️  No hay columnas para renombrar (ya tienen nombres correctos)")

# Verificar coordenadas
print("\n4️⃣ Verificando coordenadas...")
if '_Georreferenciación del punto_latitude' in df.columns:
    print(f"   ✅ Latitude: {df['_Georreferenciación del punto_latitude'].notna().sum()} valores")
    print(f"   ✅ Longitude: {df['_Georreferenciación del punto_longitude'].notna().sum()} valores")
elif 'latitude' in df.columns:
    print(f"   ✅ latitude: {df['latitude'].notna().sum()} valores")
    print(f"   ✅ longitude: {df['longitude'].notna().sum()} valores")
else:
    print(f"   ⚠️  No se encontraron columnas de coordenadas")

# Verificar columnas select_multiple expandidas
print("\n5️⃣ Verificando columnas select_multiple expandidas...")
select_mult_cols = [col for col in df.columns if col.startswith('En el lugar hay…/')]
if select_mult_cols:
    print(f"   ✅ Encontradas {len(select_mult_cols)} columnas expandidas:")
    for col in select_mult_cols:
        non_null = df[col].notna().sum()
        print(f"      - {col}: {non_null} valores")
else:
    print(f"   ⚠️  No se encontraron columnas expandidas de select_multiple")

# Verificar otras columnas importantes
print("\n6️⃣ Verificando columnas importantes...")
important_cols = [
    'start',
    'end',
    'today',
    'username',
    'deviceid',
    'Georreferenciación del punto',
    'Cantidad de personas en situación de calle observadas',
    'Se observan niños/as en el lugar'
]

for col in important_cols:
    if col in df.columns:
        non_null = df[col].notna().sum()
        print(f"   ✅ {col}: {non_null} valores")
    else:
        print(f"   ❌ {col}: NO EXISTE")

# Columnas deseadas (las que deberían cargarse a Neon)
print("\n7️⃣ Verificando columnas para Neon...")
columnas_deseadas = [
    'start', 'end', 'today', 'username', 'deviceid',
    'Georreferenciación del punto',
    '_Georreferenciación del punto_latitude',
    '_Georreferenciación del punto_longitude',
    '_Georreferenciación del punto_altitude',
    '_Georreferenciación del punto_precision',
    'Ingrese número de documento del usuario que completa el formulario',
    'Cantidad de personas en situación de calle observadas',
    'La/s persona/s esta/n',
    'En el lugar hay…',
]

# Agregar columnas expandidas
columnas_deseadas.extend(select_mult_cols)

# Filtrar solo las que existen
columnas_finales = [col for col in columnas_deseadas if col in df.columns]
print(f"   ✅ Total columnas para Neon: {len(columnas_finales)}")

# Crear DataFrame final
df_final = df[columnas_finales].copy()

# Resumen
print("\n" + "="*70)
print("📊 RESUMEN")
print("="*70)
print(f"Registros totales: {len(df_final)}")
print(f"Columnas totales: {len(df_final.columns)}")
print(f"Columnas select_multiple: {len(select_mult_cols)}")

# Guardar muestra
df_final.head(5).to_csv("test_reclassify_output.csv", index=False)
print(f"\n💾 Muestra guardada en: test_reclassify_output.csv")

# Listar todas las columnas finales
with open("columnas_finales_reclassify.txt", "w", encoding="utf-8") as f:
    f.write("COLUMNAS FINALES PARA NEON\n")
    f.write("="*70 + "\n\n")
    for i, col in enumerate(df_final.columns, 1):
        f.write(f"{i}. {col}\n")

print(f"💾 Lista completa en: columnas_finales_reclassify.txt")

print("\n✅ PRUEBA COMPLETADA")
