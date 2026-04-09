FROM python:3.12-slim

# Deps de sistema para tesserocr e libs gerais
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libtesseract-dev \
    leptonica-progs \
    tesseract-ocr \
    tesseract-ocr-por \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código (data/ é ignorado via .dockerignore)
COPY . .

# O volume do Railway será montado em /data
# Symlink para apontar data/ -> /data (volume persistente)
RUN rm -rf /app/data && ln -s /data /app/data

EXPOSE 8000

CMD uvicorn src.web.api:app --host 0.0.0.0 --port ${PORT:-8000}
