import sys
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

import os
import re
import tempfile
from datetime import datetime
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATABASE_URL = os.environ.get("DATABASE_URL")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

NEON_TABLE = 'control_duplas_sheet'

SCOPES = [
    "https://spreadsheets.google.com/feeds",
    'https://www.googleapis.com/auth/spreadsheets',
    "https://www.googleapis.com/auth/drive.file",
]


def obtener_credenciales():
    """Preferencia: env GOOGLE_CREDENTIALS_JSON → archivo local en el proyecto."""
    creds_env = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    if creds_env:
        fd, path = tempfile.mkstemp(suffix='.json')
        with os.fdopen(fd, 'w') as f:
            f.write(creds_env)
        return path
    for name in ('kobo-looker-connect.json', 'credenciales.json'):
        for root, _, files in os.walk(PROJ_ROOT):
            if name in files:
                return os.path.join(root, name)
    return None


def sheet_id_desde_url(valor):
    if not valor:
        return None
    m = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', valor)
    return m.group(1) if m else (valor if not valor.startswith('http') else None)


def parse_hora_turno(valor):
    """Devuelve (hora HH:MM, turno) o (None, None). Reglas: TM 6-14, TT 14-22, TN resto."""
    if not valor:
        return None, None
    m = re.search(r'(\d{1,2}):(\d{2})', str(valor))
    if not m:
        return None, None
    h = int(m.group(1))
    turno = 'TM' if 6 <= h < 14 else 'TT' if 14 <= h < 22 else 'TN'
    return f'{h:02d}:{m.group(2)}', turno


def parse_fecha(valor, timestamp):
    """Fecha: columna Dia; si está vacía, usa la fecha del Timestamp (submission)."""
    if isinstance(valor, str) and valor.strip():
        for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d'):
            try:
                return datetime.strptime(valor.strip(), fmt).date()
            except ValueError:
                pass
        dt = pd.to_datetime(valor, errors='coerce')
        if pd.notna(dt):
            return dt.date()
    dt = pd.to_datetime(str(timestamp), dayfirst=True, errors='coerce')
    return dt.date() if pd.notna(dt) else None


def fecha_censo_sheet(timestamp, inicio_hora):
    """Misma regla de fecha que Kobo (fecha_reporte con start < 6h → día anterior).
    Sheet no tiene 'start'; usa el Timestamp de carga como ancla temporal.
    - timestamp.hour < 6 → día anterior (carga de madrugada tras el censo).
    - Inicio >= 20:00 y timestamp.hour < 12 → censo nocturno cargado hasta mediodía → día anterior.
    El campo 'Dia' del sheet es inconsistente (convenciones mezcladas) y no se usa para fechar."""
    ts = pd.to_datetime(str(timestamp), dayfirst=True, errors='coerce')
    if pd.isna(ts):
        return None
    fecha = ts.date()
    inicio_h = int(inicio_hora.split(':')[0]) if inicio_hora else None
    if ts.hour < 6 or (inicio_h is not None and inicio_h >= 20 and ts.hour < 12):
        fecha = fecha - pd.Timedelta(days=1)
    return fecha


def parse_int(valor):
    if valor is None:
        return None
    m = re.search(r'\d+', str(valor))
    return int(m.group(0)) if m else None


def main():
    if not DATABASE_URL:
        print("❌ ERROR: DATABASE_URL no configurada.")
        return

    ruta_creds = obtener_credenciales()
    if not ruta_creds:
        print("❌ ERROR: no se encontraron credenciales de Google (GOOGLE_CREDENTIALS_JSON o kobo-looker-connect.json).")
        return

    sheet_ref = os.environ.get('SHEET_DUPLAS') or os.environ.get('DUPLAS_SHEET_ID') \
        or 'https://docs.google.com/spreadsheets/d/1Fg29d97P9pO4KxTChjKrouoFpQYr8UcwSavg0uEeMt4/edit?gid=0#gid=0'
    sheet_id = sheet_id_desde_url(sheet_ref)
    if not sheet_id:
        print("❌ ERROR: SHEET_DUPLAS / DUPLAS_SHEET_ID no configurada.")
        return

    print("📊 Sincronizando sheet de duplas...")

    creds = ServiceAccountCredentials.from_json_keyfile_name(ruta_creds, SCOPES)
    client = gspread.authorize(creds)
    sh = client.open_by_key(sheet_id)
    ws = sh.sheet1
    records = ws.get_all_records()
    print(f"   Hoja '{ws.title}': {len(records)} filas con datos")

    filas = []
    for i, r in enumerate(records):
        hora, turno = parse_hora_turno(r.get('Inicio'))
        fecha = fecha_censo_sheet(r.get('Timestamp'), hora)
        if fecha is None:
            fecha = parse_fecha(r.get('Dia'), r.get('Timestamp'))
        if fecha is None:
            continue
        dupla = parse_int(r.get('Dupla'))
        registros = parse_int(r.get('Registros'))
        if dupla is None or registros is None:
            continue
        filas.append((
            fecha, turno, dupla, registros,
            str(r.get('Responsable') or '').strip(),
            str(r.get('Polígono') or '').strip(),
            i + 2,
        ))

    if not filas:
        print("   Sin filas válidas para sincronizar.")
        return

    df_out = pd.DataFrame(
        filas,
        columns=['fecha', 'turno', 'dupla', 'registros', 'responsable', 'poligono', 'sheet_row'],
    )

    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{NEON_TABLE}" (
                fecha DATE,
                turno TEXT,
                dupla INTEGER,
                registros INTEGER,
                responsable TEXT,
                poligono TEXT,
                sheet_row INTEGER,
                synced_at TIMESTAMPTZ DEFAULT now()
            )
        '''))
        conn.commit()

    fechas_lote = sorted(df_out['fecha'].astype(str).unique().tolist())
    with engine.begin() as conn:
        conn.execute(text(f'DELETE FROM "{NEON_TABLE}"'))

    df_out.to_sql(NEON_TABLE, con=engine, if_exists='append', index=False, method='multi')

    print(f"✅ {len(df_out)} filas sincronizadas (fechas: {fechas_lote[0]} a {fechas_lote[-1]}).")


if __name__ == "__main__":
    main()
