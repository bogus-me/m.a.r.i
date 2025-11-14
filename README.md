# m.a.r.i
**M.A.R.I. – Metadata Auto Refresh Interface for Plex. Smart. Silent. Always fresh.**

M.A.R.I. ist ein autonomes, containerbasiertes System, das deine Plex-Mediathek permanent überwacht und automatisch beschädigte, fehlende oder unvollständige Metadaten repariert.  
Es arbeitet segmentiert, ressourcenschonend und vollkommen ohne manuelle Eingriffe – inklusive Eskalationslogik, Dead-Erkennung, Healthchecks und Live-Status über Discord und optional Telegram.  
Einmal eingerichtet sorgt M.A.R.I. dafür, dass Plex jederzeit sauber, konsistent und vollständig bleibt.

---

## 🧩 Discord Bot Setup Guide

### 1. Bot erstellen
1. Öffne das Discord Developer Portal: https://discord.com/developers/applications  
2. Klicke auf **New Application** und wähle einen Namen.  
3. Menü links → **Bot → Add Bot**.

---

### 2. Intents aktivieren (siehe Screenshots)
Aktiviere unter **Privileged Gateway Intents**:

- Presence Intent  
- Server Members Intent  
- Message Content Intent  

---

### 3. Berechtigungen festlegen
Gehe zu **OAuth2 → URL Generator**:

- Scope: `bot`  
- Bot Permissions: **Administrator**

Der Permissions-Integer lautet: **8**

---

### 4. Bot einladen
Nutze diesen Link und ersetze `YOUR_APPLICATION_ID` durch deine Client-ID:

https://discord.com/oauth2/authorize?client_id=YOUR_APPLICATION_ID&scope=bot&permissions=8

---

## ⚙️ Plex Refresher – Linux Setup Guide

### 1. Verzeichnisstruktur erstellen

    mkdir -p /pfad/zum/plex_refresher
    cd /pfad/zum/plex_refresher

### 2. Benötigte Dateien erstellen

    .env
    Dockerfile
    Docker_Compose.yaml
    entrypoint.sh
    plex_refresh.py
    requirements.txt

### 3. Inhalte einfügen (Beispiel)

    nano .env

Datei speichern:  
CTRL+O → ENTER → CTRL+X

### 4. Dateirechte setzen

    chown -R $USER:$USER /pfad/zum/plex_refresher

    chmod +x entrypoint.sh
    chmod 644 .env
    chmod 644 requirements.txt
    chmod 644 Docker_Compose.yaml
    chmod 644 Dockerfile
    chmod 644 plex_refresh.py

### 5. Docker-Container starten

    docker compose up -d

### 6. Logs prüfen

    docker compose logs -f

M.A.R.I. ist nun vollständig eingerichtet und läuft autonom im Hintergrund.
