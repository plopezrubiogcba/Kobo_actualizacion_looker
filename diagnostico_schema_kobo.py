"""
Diagnóstico del Schema de Kobo
===============================
Verifica qué campos están definidos en el formulario según el API
"""

import requests
import json
import os

# Cargar .env
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN_KOBO = os.environ.get("KOBO_API_TOKEN")
UID_KOBO = "aH2SygyBTRCkqCgBtu4m3R"
KOBO_BASE_URL = os.environ.get("KOBO_BASE_URL", "https://kf.kobotoolbox.org")
URL_KOBO_ASSET = f"{KOBO_BASE_URL}/api/v2/assets/{UID_KOBO}/"

print("DIAGNÓSTICO DEL SCHEMA DE KOBO")
print("="*70)

headers = {"Authorization": f"Token {TOKEN_KOBO}"}

try:
    response = requests.get(URL_KOBO_ASSET, headers=headers)
    response.raise_for_status()
    asset_data = response.json()
    
    # Guardar JSON completo para inspección
    with open("kobo_asset_complete.json", "w", encoding="utf-8") as f:
        json.dump(asset_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Asset data guardado en: kobo_asset_complete.json")
    
    content = asset_data.get('content', {})
    survey = content.get('survey', [])
    choices = content.get('choices', [])
    
    print(f"\n📋 SURVEY: {len(survey)} preguntas")
    print(f"📋 CHOICES: {len(choices)} opciones\n")
    
    # Listar todas las preguntas
    print("PREGUNTAS DEL FORMULARIO:")
    print("-"*70)
    for i, q in enumerate(survey, 1):
        name = q.get('name', 'N/A')
        tipo = q.get('type', 'N/A')
        label = q.get('label', [''])[0] if isinstance(q.get('label'), list) else q.get('label', 'N/A')
        list_name = q.get('select_from_list_name') or q.get('list_name', '---')
        
        print(f"{i:3}. {name:40} | {tipo:20} | list: {list_name}")
        if len(label) < 50:
            print(f"     '{label[:80]}'")
    
    # Listar choices
    print(f"\n\nOPCIONES (CHOICES):")
    print("-"*70)
    choice_lists = {}
    for choice in choices:
        list_name = choice.get('list_name')
        if list_name:
            if list_name not in choice_lists:
                choice_lists[list_name] = []
            choice_lists[list_name].append(choice.get('name'))
    
    for list_name, options in choice_lists.items():
        print(f"\n{list_name}:")
        for opt in options:
            print(f"   - {opt}")
    
    # Preguntas select_multiple específicas
    print(f"\n\nSELECT_MULTIPLE DETECTADOS:")
    print("-"*70)
    for q in survey:
        if q.get('type') == 'select_multiple':
            print(f"\n✓ {q.get('name')}")
            list_name = q.get('select_from_list_name') or q.get('list_name')
            if list_name and list_name in choice_lists:
                print(f"  Opciones: {', '.join(choice_lists[list_name])}")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
