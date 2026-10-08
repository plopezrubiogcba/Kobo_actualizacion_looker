"""Parquet canonico -> datos estaticos del front.

Lee data/kobo_flash.parquet y escribe en dashboard/public/data/:
  flash_points.json  [{id, lat, lon, personas, turno, localizacion, fecha, dupla}]
  flash_meta.json    {date_min, date_max, zonas[]}
  flash.csv          mismos campos que flash_points.json (delimiter ';', UTF-8 BOM)

Replica las condiciones de los viejos endpoints SQL:
  - points: lat/lon y fecha_reporte no nulos (BETWEEN excluye NULL)
  - meta zonas: DISTINCT Localizacion no nulos (independiente del rango de fechas)
  - el filtro de personas (NULL o <= 11) lo aplica el cliente, igual que antes

Uso: .venv/bin/python scripts/generar_front_data.py
"""
import os
import sys
import json
import csv
from datetime import datetime

import pandas as pd

PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_PATH = os.path.join(PROJ_ROOT, "data", "kobo_flash.parquet")
OUT_DIR = os.path.join(PROJ_ROOT, "dashboard", "public", "data")

CSV_FIELDS = ["id", "lat", "lon", "personas", "turno", "localizacion", "fecha", "dupla"]


def serie(df, col):
    return df[col] if col in df.columns else pd.Series([None] * len(df), index=df.index)


def construir_puntos(df):
    fechas = pd.to_datetime(serie(df, "fecha_reporte"), errors="coerce")
    lat = pd.to_numeric(serie(df, "latitude"), errors="coerce")
    lon = pd.to_numeric(serie(df, "longitude"), errors="coerce")
    personas = pd.to_numeric(serie(df, "Cantidad de personas en situación de calle observadas"), errors="coerce")

    base = pd.DataFrame({
        "id": serie(df, "_id"),
        "lat": lat,
        "lon": lon,
        "personas": personas,
        "turno": serie(df, "Turno"),
        "localizacion": serie(df, "Localizacion"),
        "fecha": fechas,
        "dupla": serie(df, "dupla"),
        "_fecha_na": fechas.isna(),
        "_geo_na": lat.isna() | lon.isna(),
    })
    excluidos = int((base["_fecha_na"] | base["_geo_na"]).sum())
    base = base[~base["_fecha_na"] & ~base["_geo_na"]]

    puntos = []
    for r in base.itertuples(index=False):
        pid = r.id
        if pid is None or (not isinstance(pid, str) and pd.isna(pid)):
            pid = None
        elif not isinstance(pid, str):
            pid = str(int(pid)) if float(pid).is_integer() else str(pid)
        puntos.append({
            "id": pid,
            "lat": round(float(r.lat), 6),
            "lon": round(float(r.lon), 6),
            "personas": int(r.personas) if not pd.isna(r.personas) else None,
            "turno": r.turno if isinstance(r.turno, str) else None,
            "localizacion": r.localizacion if isinstance(r.localizacion, str) else None,
            "fecha": r.fecha.strftime("%Y-%m-%d"),
            "dupla": int(r.dupla) if not pd.isna(r.dupla) else None,
        })
    return puntos, excluidos


def construir_meta(df):
    fechas = pd.to_datetime(serie(df, "fecha_reporte"), errors="coerce").dropna()
    zonas = sorted({z for z in serie(df, "Localizacion").dropna().unique()})
    return {
        "date_min": fechas.min().strftime("%Y-%m-%d") if len(fechas) else None,
        "date_max": fechas.max().strftime("%Y-%m-%d") if len(fechas) else None,
        "zonas": zonas,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def write_points_json(path, puntos):
    # Un objeto por linea: deltas livianos en git y legible en diff
    with open(path, "w", encoding="utf-8") as f:
        f.write("[\n")
        for i, p in enumerate(puntos):
            f.write(json.dumps(p, ensure_ascii=False, separators=(",", ":")))
            f.write(",\n" if i < len(puntos) - 1 else "\n")
        f.write("]\n")


def write_csv(path, puntos):
    # ';' + BOM: Excel es-AR abre directo con acentos y decimal ok
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, delimiter=";", extrasaction="ignore")
        w.writeheader()
        for p in puntos:
            w.writerow({k: ("" if p.get(k) is None else p.get(k)) for k in CSV_FIELDS})


def main():
    if not os.path.exists(PARQUET_PATH):
        print(f"ERROR: no existe {PARQUET_PATH}. Correr scripts/actualizar_parquet.py primero.")
        sys.exit(1)

    df = pd.read_parquet(PARQUET_PATH)
    print(f"Parquet: {len(df)} filas")

    puntos, excluidos = construir_puntos(df)
    meta = construir_meta(df)

    os.makedirs(OUT_DIR, exist_ok=True)
    write_points_json(os.path.join(OUT_DIR, "flash_points.json"), puntos)
    write_csv(os.path.join(OUT_DIR, "flash.csv"), puntos)
    with open(os.path.join(OUT_DIR, "flash_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"points: {len(puntos)} filas (excluidas sin geo/fecha: {excluidos})")
    print(f"meta:   {meta['date_min']} .. {meta['date_max']}, {len(meta['zonas'])} zonas")
    print(f"CSV:    {os.path.join(OUT_DIR, 'flash.csv')}")


if __name__ == "__main__":
    main()
