import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import os
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from main_act_flash import (
    cargar_recorridos, asignar_dupla, ensure_missing_columns,
    NEON_TABLE_NAME, RECORRIDOS_KML, DUPLA_COL,
)

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RECORRIDOS_KML_PATHS = [os.path.join(PROJ_ROOT, r) for r in RECORRIDOS_KML]

# Filtro opcional: reasignar solo desde una fecha_reporte en adelante.
# Ej: RECLASIF_DESDE=2026-09-01 → no toca el histórico anterior (recorridos viejos).
DESDE_FECHA = os.environ.get('RECLASIF_DESDE')


def main():
    print("🗺️  Asignación de dupla a histórico (KML de recorridos)...")
    if DESDE_FECHA:
        print(f"   ⏳ Solo registros con fecha_reporte >= {DESDE_FECHA}")

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    ensure_missing_columns(engine)

    where = 'latitude IS NOT NULL AND longitude IS NOT NULL'
    if DESDE_FECHA:
        where += f" AND \"fecha_reporte\" >= '{DESDE_FECHA}'"
    with engine.connect() as conn:
        df = pd.read_sql(
            text(f'SELECT "_uuid", latitude::float AS latitude, longitude::float AS longitude FROM "{NEON_TABLE_NAME}" WHERE {where}'),
            conn
        )
    print(f"   Registros con GPS a procesar: {len(df)}")
    if df.empty:
        print("   Nada que procesar.")
        return

    recorridos_gdf = cargar_recorridos(RECORRIDOS_KML_PATHS)

    puntos_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    df[DUPLA_COL] = asignar_dupla(puntos_gdf, recorridos_gdf)

    print("\n📊 Distribución por dupla:")
    print(df[DUPLA_COL].value_counts(dropna=False).to_string())
    pct = df[DUPLA_COL].notna().mean() * 100
    print(f"   % con dupla: {pct:.1f}%")

    print("\n💾 Actualizando Neon via temp table...")
    df_upload = df[['_uuid', DUPLA_COL]].rename(
        columns={'_uuid': 'uuid', DUPLA_COL: 'dupla'}
    )
    df_upload['dupla'] = df_upload['dupla'].where(df_upload['dupla'].notna(), None)
    df_upload.to_sql('_dupla_tmp', con=engine, if_exists='replace', index=False)

    with engine.connect() as conn:
        result = conn.execute(text(f'''
            UPDATE "{NEON_TABLE_NAME}" kfc
            SET "dupla" = t.dupla
            FROM _dupla_tmp t
            WHERE kfc."_uuid" = t.uuid
        '''))
        conn.commit()
        print(f"✅ {result.rowcount} registros actualizados.")

    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS _dupla_tmp'))
        conn.commit()


if __name__ == "__main__":
    main()
