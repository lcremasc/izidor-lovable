# ── Base ──────────────────────────────────────────────────────────────────
FROM python:3.12-slim

# ── Binários do sistema (tesseract + ghostscript + idiomas PT/EN) ──────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    ghostscript \
    tesseract-ocr \
    tesseract-ocr-por \
    tesseract-ocr-eng \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# ── Dependências Python ───────────────────────────────────────────────────
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Código ───────────────────────────────────────────────────────────────
# Copiar módulos locais (os seus arquivos Python sem alteração)
COPY pipeline_server.py .
COPY calculadora.py .
COPY montador.py .
COPY api_client.py .
# Item 2: parsers determinísticos de recebíveis
COPY parsers_recebiveis.py .
COPY ler_agenda_completa.py .
COPY validar_recebiveis.py .
COPY normalizacao.py .

# ── Start ─────────────────────────────────────────────────────────────────
EXPOSE 8000
CMD ["sh", "-c", "uvicorn pipeline_server:app --host 0.0.0.0 --port ${PORT:-8000}"]
