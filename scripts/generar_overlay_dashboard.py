"""
generar_overlay_dashboard.py
Regenera dashboard/public/data/mapa_flash.geojson desde Zonas flash.kml.
Disuelve polígonos por zona, recorta slivers de cruce por orden de prioridad
(misma lógica que clasificar_localizacion) y guarda cada zona como un feature
con properties.zona = <código> — formato que espera FlashMapa.tsx.
"""
import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import os
import geopandas as gpd
from shapely.ops import unary_union
from main_act_flash import ZONE_COL, ZONE_RENAME, ZONE_PRIORITY

PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
KML_PATH  = os.path.join(PROJ_ROOT, "assets", "mapas flash fronteras nuevos.kml")
OUT_PATH  = os.path.join(PROJ_ROOT, "dashboard", "public", "data", "mapa_flash.geojson")

def main():
    print(f"📂 Leyendo {KML_PATH} ...")
    gdf = gpd.read_file(KML_PATH)
    gdf = gdf.set_crs("EPSG:4326") if gdf.crs is None else gdf.to_crs("EPSG:4326")
    gdf[ZONE_COL] = gdf[ZONE_COL].replace(ZONE_RENAME)

    # Disolver por zona → un polígono por zona
    diss = {z: sub.union_all() for z, sub in gdf.groupby(ZONE_COL)}

    # Recortar slivers: cada zona menos las de mayor prioridad (misma regla que ZONE_PRIORITY)
    # Garantiza partición sin cruces — el mapa refleja exactamente cómo se asignan los puntos
    clipped = {}
    for i, z in enumerate(ZONE_PRIORITY):
        if z not in diss:
            continue
        higher = [diss[h] for h in ZONE_PRIORITY[:i] if h in diss]
        clipped[z] = diss[z].difference(unary_union(higher)) if higher else diss[z]

    rows = [{'zona': z, 'geometry': geom} for z, geom in clipped.items()]
    out_gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")

    print(f"✅ Zonas recortadas: {sorted(clipped.keys())}")
    out_gdf[['zona', 'geometry']].to_file(OUT_PATH, driver='GeoJSON')
    print(f"💾 Guardado en {OUT_PATH}")

if __name__ == "__main__":
    main()
