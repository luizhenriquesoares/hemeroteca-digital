#!/bin/bash
# Script para comprimir e subir dados para o Railway volume.
#
# Uso:
#   1. Comprimir: ./scripts/upload_data.sh pack
#   2. Subir para algum storage (S3, GCS, etc.)
#   3. No Railway shell, baixar e descomprimir
#
# Alternativa: usar `railway volume` quando disponível.

set -e

DATA_DIR="data"

case "${1:-pack}" in
  pack)
    echo "Comprimindo dados..."
    tar -czf hemeroteca_data.tar.gz \
      --exclude='data/debug_captcha' \
      --exclude='data/benchmarks' \
      -C . \
      "$DATA_DIR"
    ls -lh hemeroteca_data.tar.gz
    echo "Arquivo pronto. Suba para S3/GCS e use 'unpack' no Railway shell."
    ;;
  unpack)
    echo "Descomprimindo dados em /data..."
    tar -xzf hemeroteca_data.tar.gz --strip-components=1 -C /data
    echo "Dados restaurados em /data"
    ;;
  *)
    echo "Uso: $0 {pack|unpack}"
    exit 1
    ;;
esac
