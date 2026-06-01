"""
generar_overlay_dashboard.py
Regenera dashboard/public/data/mapa_flash.geojson desde Zonas flash.kml.
Disuelve polígonos por zona y guarda cada zona como un feature con
properties.zona = <código> — formato que espera FlashMapa.tsx.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import os
import geopandas as gpd
from main_act_flash import ZONE_COL, ZONE_RENAME

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KML_PATH  = os.path.join(BASE_DIR, "Zonas flash.kml")
OUT_PATH  = os.path.join(BASE_DIR, "dashboard", "public", "data", "mapa_flash.geojson")

def main():
    print(f"📂 Leyendo {KML_PATH} ...")
    gdf = gpd.read_file(KML_PATH)
    gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")
    gdf[ZONE_COL] = gdf[ZONE_COL].replace(ZONE_RENAME)

    # Disolver por zona → un polígono por zona
    dissolved = gdf[[ZONE_COL, 'geometry']].dissolve(by=ZONE_COL).reset_index()
    dissolved = dissolved.rename(columns={ZONE_COL: 'zona'})

    print(f"✅ Zonas: {sorted(dissolved['zona'].tolist())}")
    dissolved[['zona', 'geometry']].to_file(OUT_PATH, driver='GeoJSON')
    print(f"💾 Guardado en {OUT_PATH}")

if __name__ == "__main__":
    main()
