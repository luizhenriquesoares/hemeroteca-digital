#!/bin/bash
# Setup do projeto Hemeroteca PE
# Execute com: bash setup.sh

set -e

echo "=== Hemeroteca PE - Setup ==="
echo ""

# 1. Verificar Homebrew
if ! command -v brew &> /dev/null; then
    echo "[!] Homebrew não encontrado. Instalando..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    echo ""
fi

# 2. Instalar Tesseract
if ! command -v tesseract &> /dev/null; then
    echo "[*] Instalando Tesseract OCR..."
    brew install tesseract
    echo "[*] Instalando dados de idioma português..."
    brew install tesseract-lang
    echo ""
else
    echo "[✓] Tesseract já instalado: $(tesseract --version 2>&1 | head -1)"
fi

# 3. Verificar Python
if ! command -v python3.12 &> /dev/null; then
    echo "[!] Python 3.12 não encontrado."
    echo "    Instale com: brew install python@3.12"
    exit 1
else
    echo "[✓] Python 3.12 encontrado"
fi

# 4. Criar virtualenv se não existir
if [ ! -d ".venv" ]; then
    echo "[*] Criando virtualenv..."
    python3.12 -m venv .venv
fi
echo "[✓] Virtualenv OK"

# 5. Instalar dependências
echo "[*] Instalando dependências Python..."
source .venv/bin/activate
pip install -q -r requirements.txt

echo ""
echo "=== Setup concluído! ==="
echo ""
echo "Para usar:"
echo "  source .venv/bin/activate"
echo "  python main.py --help"
echo ""
