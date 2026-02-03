"""
Script de prueba simple para validar la conexión a Neon PostgreSQL.
Este script verifica que la cadena de conexión funciona correctamente
antes de ejecutar el script completo.
"""
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

# Cargar variables de entorno
load_dotenv()
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("❌ ERROR: DATABASE_URL no está configurada")
    exit(1)

print("✅ DATABASE_URL configurada correctamente")
print(f"🔌 Intentando conectar a Neon PostgreSQL...")

try:
    # Crear engine
    engine = create_engine(DATABASE_URL)
    
    # Probar conexión
    with engine.connect() as connection:
        # Ejecutar query simple para validar conexión
        result = connection.execute(text("SELECT version()"))
        version = result.fetchone()[0]
        
        print(f"✅ Conexión exitosa a Neon PostgreSQL!")
        print(f"📊 Versión de PostgreSQL: {version[:50]}...")
        print(f"🔐 SSL habilitado: {DATABASE_URL.endswith('sslmode=require')}")
    
    # Cerrar conexión
    engine.dispose()
    print("\n✅ PRUEBA DE CONEXIÓN: EXITOSA")
    
except Exception as e:
    print(f"❌ Error al conectar: {e}")
    exit(1)
