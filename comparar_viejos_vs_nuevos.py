"""
Script para comparar datos VIEJOS vs NUEVOS del API
Objetivo: Ver si dni_operador existe en datos recientes
"""
import requests
import os
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

TOKEN_KOBO = os.environ.get("KOBO_API_TOKEN")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
KOBO_BASE_URL = "https://kf.kobotoolbox.org"

print("="*80)
print("COMPARACIÓN: DATOS VIEJOS VS NUEVOS")
print("="*80)

headers = {"Authorization": f"Token {TOKEN_KOBO}"}
url = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/data.json"

# Obtener primeros y últimos registros
print(f"\n📡 Obteniendo primeros 5 registros (más viejos)...")
resp_first = requests.get(url, headers=headers, params={'limit': 5, 'start': 0})
primeros = resp_first.json()['results']

print(f"📡 Obteniendo últimos 5 registros (más recientes)...")
total_count = requests.get(url, headers=headers, params={'limit': 1}).json()['count']
resp_last = requests.get(url, headers=headers, params={'limit': 5, 'start': total_count - 5})
ultimos = resp_last.json()['results']

print(f"\n📊 ANÁLISIS DE REGISTROS")
print("="*80)

def analizar_registro(reg, label):
    fecha = reg.get('start', reg.get('_submission_time', 'N/A'))
    version = reg.get('__version__', 'N/A')
    
    has_dni = any('dni' in k.lower() for k in reg.keys())
    has_dni_operador = 'datos_per/dni_operador' in reg
    
    caracteristicas = reg.get('caracteristicas_puntos/caracteristicas_observada', 'N/A')
    cant_pers = reg.get('datos_per/cant_pers', 'N/A')
    
    print(f"\n{label}:")
    print(f"   Fecha: {fecha}")
    print(f"   Versión: {version}")
    print(f"   Total columnas: {len(reg)}")
    print(f"   ¿Tiene campos con 'dni'? {'✅ SÍ' if has_dni else '❌ NO'}")
    print(f"   ¿Tiene 'datos_per/dni_operador'? {'✅ SÍ' if has_dni_operador else '❌ NO'}")
    if has_dni_operador:
        print(f"   Valor dni_operador: {reg['datos_per/dni_operador']}")
    print(f"   cant_pers: {cant_pers}")
    print(f"   caracteristicas_observada: {caracteristicas}")
    
    # Listar todos los campos datos_per
    datos_per = [k for k in reg.keys() if 'datos_per' in k]
    if datos_per:
        print(f"   Campos datos_per ({len(datos_per)}):")
        for dp in datos_per:
            print(f"      - {dp}")
    
    return has_dni_operador

print("\n" + "="*80)
print("PRIMEROS 5 REGISTROS (MÁS VIEJOS)")
print("="*80)
resultados_viejos = []
for i, reg in enumerate(primeros, 1):
    tiene_dni = analizar_registro(reg, f"Registro {i}")
    resultados_viejos.append(tiene_dni)

print("\n" + "="*80)
print("ÚLTIMOS 5 REGISTROS (MÁS RECIENTES)")
print("="*80)
resultados_nuevos = []
for i, reg in enumerate(ultimos, 1):
    tiene_dni = analizar_registro(reg, f"Registro {total_count - 5 + i}")
    resultados_nuevos.append(tiene_dni)

print("\n" + "="*80)
print("RESUMEN")
print("="*80)
print(f"Total de submissions: {total_count}")
print(f"\nRegistros VIEJOS con dni_operador: {sum(resultados_viejos)}/5")
print(f"Registros NUEVOS con dni_operador: {sum(resultados_nuevos)}/5")

if sum(resultados_viejos) > sum(resultados_nuevos):
    print(f"\n⚠️  CONCLUSIÓN: El campo dni_operador existe en datos viejos pero NO en nuevos")
    print(f"   → El campo fue ELIMINADO del formulario en alguna versión reciente")
elif sum(resultados_viejos) == 0 and sum(resultados_nuevos) == 0:
    print(f"\n❌ CONCLUSIÓN: El campo dni_operador NO EXISTE en ninguna versión")
    print(f"   → Nunca existió o fue eliminado hace mucho tiempo")
else:
    print(f"\n✅ CONCLUSIÓN: El campo dni_operador existe en datos recientes")
