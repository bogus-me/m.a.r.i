#!/bin/bash
set -e

echo "[BOOT] Starte Plex Smart-Refresher..."
cd /app || exit 1

VENV="/app/.venv"
REQ="/app/requirements.txt"

create_venv() {
    echo "[SETUP] Erstelle virtuelle Umgebung..."
    python3 -m venv "$VENV"
    source "$VENV/bin/activate"
    pip install --upgrade pip
    pip install --no-cache-dir -r "$REQ"
}

# 1) Existiert keine venv?
if [ ! -d "$VENV" ]; then
    create_venv
else
    echo "[OK] Virtuelle Umgebung existiert."
    source "$VENV/bin/activate"

    # 2) Prüfe, ob pip korrekt funktioniert
    if ! pip --version >/dev/null 2>&1; then
        echo "[WARN] defekte venv erkannt – neu erstellen."
        rm -rf "$VENV"
        create_venv
    fi

    # 3) Prüfe fehlende Requirements
    echo "[CHECK] Prüfe fehlende Dependencies..."
    if ! pip install --no-cache-dir -r "$REQ" >/dev/null 2>&1; then
        echo "[FIX] Installiere fehlende Abhängigkeiten..."
        pip install --no-cache-dir -r "$REQ"
    fi
fi

echo "[START] Starte Script..."
exec "$VENV/bin/python" /app/plex_refresh.py
