#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== 1/4 Actualizando parquet canónico ==="
cd "$SCRIPT_DIR"
python scripts/actualizar_parquet.py

echo ""
echo "=== 2/4 Generando datos del front (JSON + CSV) ==="
python scripts/generar_front_data.py

echo ""
echo "=== 3/4 Sincronizando sheet de duplas (JSON) ==="
python scripts/sync_sheet_duplas.py

echo ""
echo "=== 4/4 Deploy a Vercel ==="
cd "$SCRIPT_DIR/dashboard"
vercel --prod --yes

echo ""
echo "Listo. Dashboard actualizado en https://kobo-flash.vercel.app"
