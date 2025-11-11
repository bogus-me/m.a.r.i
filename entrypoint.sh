#!/bin/bash
set -e

echo "[BOOT] Starte Plex Smart-Refresher Container..."

cd /app || {
    echo "[ERROR] /app nicht gefunden!"
    exit 1
}

# === Virtuelle Umgebung aktivieren (falls vorhanden) ===
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "[OK] Virtuelle Umgebung aktiviert."
else
    echo "[WARN] Keine virtuelle Umgebung gefunden – starte mit globalem Python."
fi

echo "[RUN] Starte plex_refresh.py ..."
python3 plex_refresh.py || {
    echo "[ERROR] plex_refresh.py abgestürzt!"
    exit 1
}
