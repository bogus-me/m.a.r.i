FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-venv python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Arbeitsverzeichnis im Container
WORKDIR /app

# Dateien in Container kopieren
COPY plex_refresh.py .
COPY requirements.txt .
COPY entrypoint.sh .

SHELL ["/bin/bash", "-c"]

RUN python3 -m venv .venv \
 && source .venv/bin/activate \
 && pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt \
 && chmod +x /app/entrypoint.sh \
 && rm -rf /root/.cache/pip

ENTRYPOINT ["/app/entrypoint.sh"]
