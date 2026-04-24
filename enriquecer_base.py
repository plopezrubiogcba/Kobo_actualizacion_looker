import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

NEON_TABLE_NAME = 'kobo_flash_consolidado'

def enriquecer_registros():
    """
    Identifica registros con 'fecha_reporte' o 'inicio_semana_lunes' nulos 
    y los completa basándose en la columna 'start'.
    """
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL no encontrada en el archivo .env")
        return

    engine = create_engine(DATABASE_URL)
    
    print(f"🔍 Buscando registros incompletos en la tabla '{NEON_TABLE_NAME}'...")
    
    # Consulta SQL para completar las columnas faltantes
    # - fecha_reporte: se obtiene de la columna 'start'
    # - inicio_semana_lunes: se calcula restando los días necesarios para llegar al lunes previo
    enrich_sql = text(f"""
        UPDATE "{NEON_TABLE_NAME}"
        SET
            "fecha_reporte" = CASE
                WHEN extract(hour from ("start"::timestamp)) < 3
                THEN ("start"::timestamp)::date - interval '1 day'
                ELSE ("start"::timestamp)::date
            END,
            "inicio_semana_lunes" = (
                CASE
                    WHEN extract(hour from ("start"::timestamp)) < 3
                    THEN ("start"::timestamp)::date - interval '1 day'
                    ELSE ("start"::timestamp)::date
                END -
                (extract(isodow from (
                    CASE
                        WHEN extract(hour from ("start"::timestamp)) < 3
                        THEN ("start"::timestamp)::date - interval '1 day'
                        ELSE ("start"::timestamp)::date
                    END
                ))::int - 1) * interval '1 day'
            )::date
        WHERE ("fecha_reporte" IS NULL OR "inicio_semana_lunes" IS NULL)
          AND "start" IS NOT NULL
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(enrich_sql)
            conn.commit()
            if result.rowcount > 0:
                print(f"✅ Enriquecimiento completado con éxito.")
                print(f"📊 Registros actualizados: {result.rowcount}")
            else:
                print("✨ No se encontraron registros que requieran enriquecimiento.")
    except Exception as e:
        print(f"⚠️ Error durante el proceso de enriquecimiento: {e}")

if __name__ == "__main__":
    print("🚀 Iniciando script de enriquecimiento de base de datos...")
    enriquecer_registros()
    print("🏁 Proceso finalizado.")
