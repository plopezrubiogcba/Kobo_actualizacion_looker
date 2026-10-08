"""Backup local Kobo Flash -> parquet.

Fuente de verdad = Kobo API (Neon Azure vieja inaccesible).
Descarga completa de los 3 forms (sin filtro incremental), aplica el mismo
procesamiento del ETL (coords/fechas/zonas/duplas) y guarda:
  backup/kobo_flash_backup_YYYYMMDD_HHMM.parquet  (df_final, mismo shape que se sube a Neon)
  backup/kobo_flash_raw_YYYYMMDD_HHMM.parquet     (df_raw crudo de Kobo)
  backup/kobo_flash_backup_YYYYMMDD_HHMM.meta.json (conteos para verificar)

Uso: .venv/bin/python scripts/backup_kobo_parquet.py
"""
import os
import sys
import json
import hashlib
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv

load_dotenv()

import main_act_flash as etl

PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKUP_DIR = os.path.join(PROJ_ROOT, "backup")


def stringify_complex(df):
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object and df[col].map(lambda x: isinstance(x, (list, dict))).any():
            df[col] = df[col].map(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x)
    return df


def procesar_raw(df_raw):
    """Mismo procesamiento que el ETL: coords/fechas -> zonas -> duplas -> rename -> columnas finales.

    Devuelve (df_final, stats) sin tocar la fuente.
    """
    df = etl.procesar_coords_y_fechas(df_raw.copy())
    n_antes_geo = len(df)
    df.dropna(subset=["latitude", "longitude"], inplace=True)
    n_sin_geo = n_antes_geo - len(df)

    ruta_kml = os.path.join(PROJ_ROOT, "assets", "Mapas flash finales.kml")
    zonas_dict = etl.cargar_zonas_flash(ruta_kml)
    puntos_gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    df["Localizacion"] = etl.clasificar_localizacion(puntos_gdf, zonas_dict)

    ruta_recorridos = [os.path.join(PROJ_ROOT, r) for r in etl.RECORRIDOS_KML]
    try:
        recorridos_gdf = etl.cargar_recorridos(ruta_recorridos)
        df[etl.DUPLA_COL] = etl.asignar_dupla(puntos_gdf, recorridos_gdf)
    except Exception as e:
        print(f"WARNING asignando dupla: {e}")
        df[etl.DUPLA_COL] = None

    df["hora_start"] = df["start"].dt.strftime("%H:%M:%S")
    df["start"] = df["start"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df.rename(columns={
        "geo_ref/geo_punto": "Georreferenciación del punto",
        "datos_per/cant_pers": "Cantidad de personas en situación de calle observadas",
        "caracteristicas_puntos/caracteristicas_observada": "Características observables del punto",
        "caracteristicas_puntos/NNyA_observa": "Se observan niños/as en el punto",
        "geo_ref/dni_oper": "dni_oper",
    }, inplace=True)

    columnas_finales = [
        "Turno", "start", "hora_start", "end", "today", "username", "deviceid",
        "Georreferenciación del punto", "latitude", "longitude",
        "_Georreferenciación del punto_altitude", "_Georreferenciación del punto_precision",
        "Cantidad de personas en situación de calle observadas", "La/s persona/s esta/n",
        "Características observables del punto", "Se observan niños/as en el punto",
        "datos_per/sit_calle", "fecha_reporte", "inicio_semana_lunes",
        "_id", "_uuid", "_submission_time", "_status", "_submitted_by", "Localizacion",
        "dni_oper", etl.DUPLA_COL,
    ]
    cols = [c for c in columnas_finales if c in df.columns]
    df_final = df[cols]
    stats = {"descartados_sin_geo": int(n_sin_geo)}
    return df_final, stats


def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    os.makedirs(BACKUP_DIR, exist_ok=True)

    per_form = []
    raws = []
    for form in etl.KOBO_FORMS:
        uid = form["uid"]
        token = os.environ.get(form["token_env"], form["token_default"])
        df = etl.extraer_kobo_completo(since_timestamp=None, uid=uid, token=token)
        print(f"form {uid}: {len(df)} registros")
        per_form.append({"uid": uid, "n": int(len(df))})
        if not df.empty:
            raws.append(df)
    if not raws:
        print("ERROR: Kobo no devolvio registros.")
        sys.exit(1)

    df_raw = pd.concat(raws, ignore_index=True)
    print(f"RAW total: {len(df_raw)}")

    raw_path = os.path.join(BACKUP_DIR, f"kobo_flash_raw_{ts}.parquet")
    stringify_complex(df_raw).to_parquet(raw_path, index=False, compression="snappy")
    print(f"RAW guardado: {raw_path}")

    # --- mismo procesamiento que el ETL ---
    df_final, stats = procesar_raw(df_raw)
    n_sin_geo = stats["descartados_sin_geo"]

    out_path = os.path.join(BACKUP_DIR, f"kobo_flash_backup_{ts}.parquet")
    df_final.to_parquet(out_path, index=False, compression="snappy")

    with open(out_path, "rb") as f:
        sha = hashlib.sha256(f.read()).hexdigest()

    meta = {
        "fecha": ts,
        "fuente": "Kobo API full pull (Neon Azure inaccesible)",
        "forms": per_form,
        "raw_total": int(len(df_raw)),
        "descartados_sin_geo": int(n_sin_geo),
        "final_total": int(len(df_final)),
        "uuid_unicos": int(df_final["_uuid"].astype(str).nunique()) if "_uuid" in df_final.columns else None,
        "min_submission": str(df_final["_submission_time"].min()) if "_submission_time" in df_final.columns else None,
        "max_submission": str(df_final["_submission_time"].max()) if "_submission_time" in df_final.columns else None,
        "zonas": df_final["Localizacion"].value_counts().to_dict() if "Localizacion" in df_final.columns else None,
        "archivo": os.path.basename(out_path),
        "sha256": sha,
    }
    meta_path = out_path.replace(".parquet", ".meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"BACKUP guardado: {out_path} ({len(df_final)} filas)")
    print(f"META: {meta_path}")
    print(json.dumps({k: v for k, v in meta.items() if k != "zonas"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
