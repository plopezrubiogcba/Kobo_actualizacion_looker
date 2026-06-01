import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import os
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from main_act_flash import cargar_zonas_flash, clasificar_localizacion, NEON_TABLE_NAME

load_dotenv()

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KML_PATH = os.path.join(PROJ_ROOT, "Zonas flash.kml")


def main():
    print("🗺️  Reclasificación histórica con nuevo GeoJSON...")

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.connect() as conn:
        df = pd.read_sql(
            text(f'SELECT "_uuid", latitude::float, longitude::float, "tipo_flash" FROM "{NEON_TABLE_NAME}" WHERE latitude IS NOT NULL AND longitude IS NOT NULL'),
            conn
        )
    print(f"   Registros a reclasificar: {len(df)}")

    zonas_dict = cargar_zonas_flash(KML_PATH)

    puntos_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326"
    )

    declared_flash = df['tipo_flash'] if 'tipo_flash' in df.columns else None
    localizacion = clasificar_localizacion(puntos_gdf, zonas_dict, declared_flash)
    df['Localizacion'] = localizacion.values

    print("\n📊 Distribución por zona:")
    print(df['Localizacion'].value_counts().to_string())

    # Bulk update via temp table (single UPDATE FROM)
    print("\n💾 Actualizando Neon via temp table...")
    df_upload = df[['_uuid', 'Localizacion']].rename(
        columns={'_uuid': 'uuid', 'Localizacion': 'localizacion'}
    )
    df_upload.to_sql('_loc_tmp', con=engine, if_exists='replace', index=False)

    with engine.connect() as conn:
        result = conn.execute(text(f'''
            UPDATE "{NEON_TABLE_NAME}" kfc
            SET "Localizacion" = t.localizacion
            FROM _loc_tmp t
            WHERE kfc."_uuid" = t.uuid
        '''))
        conn.commit()
        print(f"✅ {result.rowcount} registros actualizados.")

    with engine.connect() as conn:
        conn.execute(text('DROP TABLE IF EXISTS _loc_tmp'))
        conn.commit()


if __name__ == "__main__":
    main()
