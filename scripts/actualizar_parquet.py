"""ETL Kobo Flash -> parquet canonico (reemplaza Neon).

Full pull de los 3 forms (misma logica que siempre: coords/fechas, zonas por GPS
con prioridad + snap, dupla por poligonos), dedup por _uuid (replica el filtro de
existentes del ETL incremental), y escritura en:
  data/kobo_flash.parquet   (canonico, versionado)
  data/meta.json            (conteos para verificar)
  backup/kobo_flash_backup_YYYYMMDD_HHMM.parquet (+ .meta.json)  (fechado, local)

Uso: .venv/bin/python scripts/actualizar_parquet.py
"""
import os
import sys
import json
import hashlib
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

import main_act_flash as etl
from backup_kobo_parquet import procesar_raw, stringify_complex

PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJ_ROOT, "data")
BACKUP_DIR = os.path.join(PROJ_ROOT, "backup")

CANONICO_PATH = os.path.join(DATA_DIR, "kobo_flash.parquet")
META_PATH = os.path.join(DATA_DIR, "meta.json")


def extraer_forms():
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
    return df_raw, per_form


def armar_meta(df_raw, df_final, per_form, stats, archivo, sha=None):
    return {
        "fecha": datetime.now().strftime("%Y%m%d_%H%M"),
        "fuente": "Kobo API full pull -> parquet canonico (Neon deprecado)",
        "forms": per_form,
        "raw_total": int(len(df_raw)),
        "descartados_sin_geo": stats["descartados_sin_geo"],
        "duplicados_uuid": int(stats.get("duplicados_uuid", 0)),
        "final_total": int(len(df_final)),
        "uuid_unicos": int(df_final["_uuid"].astype(str).nunique()) if "_uuid" in df_final.columns else None,
        "min_submission": str(df_final["_submission_time"].min()) if "_submission_time" in df_final.columns else None,
        "max_submission": str(df_final["_submission_time"].max()) if "_submission_time" in df_final.columns else None,
        "zonas": df_final["Localizacion"].value_counts().to_dict() if "Localizacion" in df_final.columns else None,
        "archivo": archivo,
        "sha256": sha,
    }


def guardar_parquet(df, path):
    stringify_complex(df).to_parquet(path, index=False, compression="snappy")
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def main():
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(BACKUP_DIR, exist_ok=True)

    df_raw, per_form = extraer_forms()
    df_final, stats = procesar_raw(df_raw)

    # Dedup por _uuid (keep first): replica el filtro de UUIDs existentes del ETL
    # incremental sobre Neon. Kobo devuelve ~217 uuids repetidos por corrida.
    if "_uuid" in df_final.columns:
        n_antes = len(df_final)
        df_final = df_final.drop_duplicates(subset="_uuid", keep="first").reset_index(drop=True)
        stats["duplicados_uuid"] = n_antes - len(df_final)
        print(f"Dedup _uuid: {stats['duplicados_uuid']} filas repetidas eliminadas")

    # Canonico
    sha = guardar_parquet(df_final, CANONICO_PATH)
    meta = armar_meta(df_raw, df_final, per_form, stats, "data/kobo_flash.parquet", sha)
    with open(META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    print(f"CANONICO: {CANONICO_PATH} ({len(df_final)} filas)")

    # Backup fechado (copia local historica)
    backup_path = os.path.join(BACKUP_DIR, f"kobo_flash_backup_{ts}.parquet")
    backup_sha = guardar_parquet(df_final, backup_path)
    backup_meta = armar_meta(df_raw, df_final, per_form, stats,
                             os.path.basename(backup_path), backup_sha)
    with open(backup_path.replace(".parquet", ".meta.json"), "w", encoding="utf-8") as f:
        json.dump(backup_meta, f, ensure_ascii=False, indent=2)
    print(f"BACKUP:   {backup_path}")
    print(json.dumps({k: v for k, v in meta.items() if k != "zonas"},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
