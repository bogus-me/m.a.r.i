# m.a.r.i 
**M.A.R.I. – Metadata Auto Refresh Interface for Plex. Smart. Silent. Always fresh.**

M.A.R.I. ist ein autonomes, containerbasiertes System, das deine Plex-Mediathek permanent überwacht und automatisch beschädigte, fehlende oder unvollständige Metadaten repariert.
Es arbeitet segmentiert, ressourcenschonend und vollkommen ohne manuelle Eingriffe – inklusive Eskalationslogik, Dead-Erkennung, Healthchecks und Live-Status über Discord und optional Telegram.
Einmal eingerichtet sorgt M.A.R.I. dafür, dass Plex jederzeit sauber, konsistent und vollständig bleibt.

---

## 🎯 Was kann M.A.R.I.?

### Automatische Metadata-Reparatur
- Erkennt fehlende oder beschädigte Metadaten (Poster, Beschreibungen, Bewertungen, GUIDs)
- Repariert Items automatisch via Plex Refresh oder TMDB-GUID-Injection
- Intelligente Priorisierung: Neue Items und kürzlich geänderte zuerst

### Smart Failure Handling
- **Cooldown-System**: Fehlgeschlagene Items werden in gestaffelten Intervallen erneut geprüft (1d → 7d → 14d → 30d)
- **Dead-Erkennung**: Nach 5 Fehlversuchen werden Items als "unfixbar" markiert und archiviert
- **Auto-Recovery**: Erkennt automatisch, wenn zuvor fehlerhafte Items wieder funktionieren

### TMDB-Integration
- Automatische TMDB-Suche bei fehlenden GUIDs (Filme & Serien)
- Intelligente Titel-Normalisierung und Fuzzy-Matching
- Support für externe IDs (IMDb, TVDb)

### Performance & Ressourcen
- **Memory-optimiert**: Chunked Processing verhindert RAM-Überlastung bei großen Bibliotheken
- **Plex-Awareness**: Wartet automatisch wenn Plex selbst scannt oder offline ist
- **Zeitlimit**: Scans werden nach konfigurierbarer Zeit beendet (Standard: 10min)
- **Item-Limit**: Maximale Anzahl zu prüfender Items pro Run (Standard: 200)

### Monitoring & Benachrichtigungen
- **Discord-Integration**: Live-Status-Updates mit Embed-Messages
- **Telegram-Support**: Alternative/zusätzliche Benachrichtigungen
- **Performance-Logs**: Echtzeit RAM/CPU-Monitoring und detaillierte Scan-Reports
- **Health-Checks**: Automatische Überwachung der letzten erfolgreichen Runs

### Bibliotheken-Management
- **Exclude-Listen**: Bestimmte Bibliotheken vom Scan ausschließen
- **Smart Lookback**: Prüft nur Items der letzten X Tage (Standard: 30d)
- **Automatische Planung**: Konfigurierbare Scan-Intervalle (z.B. täglich um 03:00 Uhr)

---

## 📊 Performance Monitoring (NEU in v4.4)

Diese Version beinhaltet **2 zusätzliche Logs** für Performance-Überwachung:

- **`performance_live.log`** - Echtzeit RAM/CPU-Monitoring während des Scans (alle 5s)
- **`performance_summary.log`** - Detaillierter Report nach jedem Scan-Abschluss

---

## Discord Bot Setup Guide

### 1. Bot erstellen
1. Öffne das Discord Developer Portal: https://discord.com/developers/applications
2. Klicke auf New Application und wähle einen Namen.
3. Menü links → Bot → Add Bot.

---

### 2. Intents aktivieren
Aktiviere unter Privileged Gateway Intents:

- Presence Intent
- Server Members Intent
- Message Content Intent

---

### 3. Berechtigungen festlegen
Gehe zu OAuth2 → URL Generator:

- Scope: bot
- Bot Permissions: Administrator

Der Permissions-Integer lautet: 8

---

### 4. Bot einladen
Nutze diesen Link und ersetze YOUR_APPLICATION_ID durch deine Client-ID:

https://discord.com/oauth2/authorize?client_id=YOUR_APPLICATION_ID&scope=bot&permissions=8

---

## Plex Refresher – Linux Setup Guide

### 1. Verzeichnisstruktur erstellen
```bash
mkdir -p /pfad/zum/plex_refresher
cd /pfad/zum/plex_refresher
```

### 2. Benötigte Dateien erstellen
```
.env
Dockerfile
Docker_Compose.yaml
entrypoint.sh
plex_refresh.py
requirements.txt
```

### 3. Inhalte einfügen (Beispiel)
```bash
nano .env
```

Datei speichern:
CTRL+O → ENTER → CTRL+X

### 4. Dateirechte setzen
```bash
chown -R $USER:$USER /pfad/zum/plex_refresher

chmod +x entrypoint.sh
chmod 644 .env
chmod 644 requirements.txt
chmod 644 Docker_Compose.yaml
chmod 644 Dockerfile
chmod 644 plex_refresh.py
```

### 5. Docker-Container starten
```bash
docker compose up -d
```

### 6. Logs prüfen
```bash
docker compose logs -f
```

---

## Erklärung der Logs (Version 4.4)

Alle Logs liegen im Ordner `/logs/`.  
Jedes Log hat eine klare Aufgabe.

**main.log**  
Zentrales Systemlog: Start, Stop, Discord, Fehler, Status.

**failed.log**  
Items, deren Metadaten Plex nicht akzeptiert (GUID, Summary, Rating usw.).

**dead.log**  
Objekte, die mehrfach fehlgeschlagen sind und in einen Langzeit-Cooldown gehen.

**recovered.log**  
Objekte, die zuvor fehlerhaft waren und später erfolgreich aktualisiert wurden.

**tmdb_hits.log**  
Alle TMDB-Ergebnisse: Treffer oder Miss – jeweils mit Titel, Jahr, Score, ID.

**performance_live.log** ⭐ NEU  
Echtzeit-Monitoring: RAM, CPU, Status alle 5 Sekunden während des Scans.

**performance_summary.log** ⭐ NEU  
Detaillierter Report nach jedem Scan: Timing, Memory-Peaks, CPU-Auslastung, Top-Libraries.

---

## Verbesserungen und Neuerungen in Version 4.4

Version 4.4 ist ein Performance- und Monitoring-Upgrade mit Fokus auf Ressourcen-Effizienz und Transparenz.

### Performance-Optimierungen
- **Chunked Processing**: Items werden in 500er-Blöcken verarbeitet statt alle auf einmal (~60% weniger RAM bei großen Bibliotheken)
- **DB Connection Pool**: 3 wiederverwendbare SQLite-Connections (~30% schnellere DB-Operationen)
- **Batch Logging**: Logs werden gesammelt und gebündelt geschrieben (~90% weniger I/O)
- **Memory Leak Fix**: CPU-Sampler nutzt bounded deque statt unbegrenzter Liste
- **Regex-Optimierungen**: Kompilierte Patterns für Titel-Normalisierung (~20% schneller)
- **Explicit Garbage Collection**: Forciertes Aufräumen nach jedem Library-Scan

### Monitoring & Logging
- **Live Performance-Log**: Echtzeit RAM/CPU-Tracking während des Scans (alle 5s)
- **Performance Summary**: Detaillierter Report mit Timing, Memory-Peaks, Top-Libraries
- **Phase-Tracking**: Status zeigt jetzt "Loading", "Processing", "Completed" pro Library
- **Scan-End Separator**: Klare visuelle Trennung zwischen Scans im Live-Log

### Stabilität
- **Plex Offline Detection**: Wartet automatisch bis Plex wieder online ist (3× retry + 2min Boot-Window)
- **Scan Conflict Detection**: Pausiert wenn Plex selbst scannt (Check alle 10s)
- **Error Recovery**: Robustere Exception-Handling für DB, Plex und TMDB

### Code-Qualität
- Modularer, gut dokumentierter Code
- Reduzierte Redundanzen
- Bessere Fehlerbehandlung
- Vorbereitet für zukünftige Features

---

M.A.R.I. ist nun vollständig eingerichtet und läuft autonom im Hintergrund.
