#!/bin/bash
set -e
echo "[BOOT] Starte Plex Smart-Refresher Container..."
cd /app || exit 1

if [ ! -d ".venv" ]; then
    echo "[SETUP] Erstelle virtuelle Umgebung und installiere Abhängigkeiten..."
    python3 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip
    pip install --no-cache-dir -r requirements.txt
else
    source .venv/bin/activate
    echo "[OK] Virtuelle Umgebung aktiviert."
fi

exec .venv/bin/python plex_refresh.py
