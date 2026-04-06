#!/bin/bash
set -e

source .venv/bin/activate

WORKERS=1
HEADLESS="--no-headless"

BIBS=(
  029033_01
  029033_02
  029033_04
  029033_05
  029033_06
  029033_07
  029033_08
  029033_09
  029033_10
  029033_11
  029033_12
  029033_13
  029033_15
  029033_16
)

for BIB in "${BIBS[@]}"
do
  echo "===================================="
  echo "🚀 Processando $BIB"
  echo "===================================="

  python main.py ocr-hires \
    --bib "$BIB" \
    --force \
    $HEADLESS \
    --workers $WORKERS \
    --keep-images | tee "log_$BIB.txt"

  echo "✅ Finalizado $BIB"
done

echo "🔥 TODOS OS ACERVOS FINALIZADOS"