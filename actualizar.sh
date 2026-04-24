#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== 1/3 Actualizando base de datos ==="
cd "$SCRIPT_DIR"
python main_act_flash.py

echo ""
echo "=== 2/3 Enriqueciendo registros ==="
python enriquecer_base.py

echo ""
echo "=== 3/3 Deploy a Vercel ==="
cd "$SCRIPT_DIR/dashboard"
vercel --prod --yes

echo ""
echo "Listo. Dashboard actualizado en https://kobo-flash.vercel.app"
