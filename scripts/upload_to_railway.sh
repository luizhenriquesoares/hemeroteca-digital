#!/bin/bash
# Upload dos dados para o Railway em partes via API.
#
# Uso:
#   ./scripts/upload_to_railway.sh https://SEU-APP.up.railway.app
#
# O script:
#   1. Compacta data/ em partes de 40MB
#   2. Envia cada parte via POST /api/_upload_chunk (append)
#   3. Extrai no servidor via POST /api/_extract_upload
#
set -euo pipefail

RAILWAY_URL="${1:?Uso: $0 https://SEU-APP.up.railway.app}"
RAILWAY_URL="${RAILWAY_URL%/}"  # remove trailing slash
SECRET="hemeroteca2026"
CHUNK_SIZE="40m"
DATA_DIR="data"
TMP_DIR=$(mktemp -d)
ARCHIVE="$TMP_DIR/hemeroteca_data.tar.gz"

cleanup() {
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

echo "=== Upload de dados para Railway ==="
echo "URL: $RAILWAY_URL"
echo ""

# 1. Verificar conectividade
echo "[1/4] Verificando conectividade..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$RAILWAY_URL/api/stats")
if [ "$HTTP_CODE" != "200" ]; then
    echo "ERRO: Railway retornou HTTP $HTTP_CODE. Verifique a URL."
    exit 1
fi
echo "OK (HTTP $HTTP_CODE)"

# 2. Compactar dados
echo ""
echo "[2/4] Compactando dados..."
tar -czf "$ARCHIVE" \
    --exclude='data/debug_captcha' \
    --exclude='data/benchmarks' \
    --exclude='data/.DS_Store' \
    -C . \
    "$DATA_DIR"
TOTAL_SIZE=$(stat -f%z "$ARCHIVE" 2>/dev/null || stat -c%s "$ARCHIVE")
echo "Arquivo: $(du -h "$ARCHIVE" | cut -f1) ($TOTAL_SIZE bytes)"

# 3. Dividir e enviar
echo ""
echo "[3/4] Enviando em partes de $CHUNK_SIZE..."

# Limpar upload anterior no servidor
curl -s -X POST "$RAILWAY_URL/api/_clear_upload?secret=$SECRET" > /dev/null

# Dividir o arquivo
split -b "$CHUNK_SIZE" "$ARCHIVE" "$TMP_DIR/chunk_"
CHUNKS=("$TMP_DIR"/chunk_*)
TOTAL_CHUNKS=${#CHUNKS[@]}
echo "Total de partes: $TOTAL_CHUNKS"

SENT=0
for CHUNK_FILE in "${CHUNKS[@]}"; do
    SENT=$((SENT + 1))
    CHUNK_NAME=$(basename "$CHUNK_FILE")
    CHUNK_FSIZE=$(stat -f%z "$CHUNK_FILE" 2>/dev/null || stat -c%s "$CHUNK_FILE")

    if [ "$SENT" -eq 1 ]; then
        APPEND="false"
    else
        APPEND="true"
    fi

    echo -n "  [$SENT/$TOTAL_CHUNKS] $CHUNK_NAME ($(python3 -c "v=$CHUNK_FSIZE; s='BKMG'; i=0
while v>=1024 and i<3: v/=1024; i+=1
print(f'{v:.1f}{s[i]}')" 2>/dev/null || echo "${CHUNK_FSIZE}B"))... "

    RESPONSE=$(curl -s -X POST \
        "$RAILWAY_URL/api/_upload_chunk?secret=$SECRET&append=$APPEND" \
        -F "file=@$CHUNK_FILE" \
        --max-time 300)

    STATUS=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','error'))" 2>/dev/null || echo "error")
    REMOTE_SIZE=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('size',0))" 2>/dev/null || echo "?")

    if [ "$STATUS" != "uploaded" ]; then
        echo "ERRO!"
        echo "Resposta: $RESPONSE"
        exit 1
    fi
    echo "OK (acumulado: $(python3 -c "v=$REMOTE_SIZE; s='BKMG'; i=0
while v>=1024 and i<3: v/=1024; i+=1
print(f'{v:.1f}{s[i]}')" 2>/dev/null || echo "${REMOTE_SIZE}B"))"
done

# Verificar tamanho final
echo ""
echo "Verificando integridade..."
if [ "$REMOTE_SIZE" != "$TOTAL_SIZE" ]; then
    echo "AVISO: tamanho remoto ($REMOTE_SIZE) != local ($TOTAL_SIZE)"
    echo "Diferenca pode ser de arredondamento. Continuando..."
fi

# 4. Extrair no servidor
echo ""
echo "[4/4] Extraindo dados no servidor..."
EXTRACT_RESPONSE=$(curl -s -X POST \
    "$RAILWAY_URL/api/_extract_upload?secret=$SECRET" \
    --max-time 600)
EXTRACT_STATUS=$(echo "$EXTRACT_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','error'))" 2>/dev/null || echo "error")

if [ "$EXTRACT_STATUS" != "extracted" ]; then
    echo "ERRO na extracao!"
    echo "Resposta: $EXTRACT_RESPONSE"
    exit 1
fi

echo "Dados extraidos com sucesso!"

# Verificar stats apos extracao
echo ""
echo "=== Verificando estado do servidor ==="
STATS=$(curl -s "$RAILWAY_URL/api/stats")
echo "$STATS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"  Paginas indexadas: {d.get('paginas_indexadas', '?')}\")
print(f\"  Acervos: {d.get('total_acervos', '?')}\")
print(f\"  Paginas com OCR: {d.get('total_textos', '?')}\")
" 2>/dev/null || echo "  (nao foi possivel parsear stats)"

echo ""
echo "Pronto! Acesse $RAILWAY_URL para verificar."
