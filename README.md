# m.a.r.i
**M.A.R.I. – Metadata Auto Refresh Interface for Plex. Smart. Silent. Always fresh.**

M.A.R.I. ist ein autonomes, containerbasiertes System, das deine Plex-Mediathek permanent überwacht und automatisch beschädigte, fehlende oder unvollständige Metadaten repariert.
Es arbeitet segmentiert, ressourcenschonend und vollkommen ohne manuelle Eingriffe – inklusive Eskalationslogik, Dead-Erkennung, Healthchecks und Live-Status über Discord und optional Telegram.
Einmal eingerichtet sorgt M.A.R.I. dafür, dass Plex jederzeit sauber, konsistent und vollständig bleibt.

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


---

## Erklärung der neuen Logs (Version 4.2)

Alle Logs liegen ab Version 4.2 im Ordner /logs/.  
Jedes Log hat eine klare Aufgabe.

main.log  
Zentrales Systemlog: Start, Stop, Discord, Fehler, Status.

failed.log  
Items, deren Metadaten Plex nicht akzeptiert (GUID, Summary, Rating usw.).

dead.log  
Objekte, die mehrfach fehlgeschlagen sind und in einen Langzeit-Cooldown gehen.

recovered.log  
Objekte, die zuvor fehlerhaft waren und später erfolgreich aktualisiert wurden.

warnings.log  
Warnungen für fehlende Poster, veraltete Agents, hohe TMDB-Last, langsame Sektionen.

tmdb_hits.log  
Alle TMDB-Ergebnisse: Treffer oder Miss – jeweils mit Titel, Jahr, Score, ID.

profiler.log  
Performance pro Bibliothek: Dauer, geprüfte Items, Fehler, Skips.


---

## Verbesserungen und Neuerungen in Version 4.2

Version 4.2 ist ein vollständiges Architektur-Upgrade mit Fokus auf Stabilität, Transparenz und Geschwindigkeit.  
Die wichtigsten Neuerungen:

1. Vollständig überarbeitetes Log-System  
   Alle Logs wurden auf separate Dateien aufgeteilt, was Auswertung und Fehlersuche deutlich vereinfacht.

2. Neues Warning-System  
   Erkennt fehlende Poster, deprecated Agents, ungewöhnlich langsame Bibliotheken und hohe TMDB-Last.

3. Neuer TMDB Resolver  
   Ein einheitlicher Movie/TV-Parser mit Titel-Normalisierung, Jahr-Validierung und Score-Algorithmus.

4. Auto-Skip und Problem-Item-Handling  
   Items mit fehlenden Metadaten, ungültigen GUIDs oder wiederholten Langläufern wandern automatisch in Cooldown.

5. Massiv verbesserte Titel-Normalisierung  
   Entfernt Sonderzeichen, Klammern, Jahreszahlen, doppelte Satzzeichen und Unicode-Fehler.

6. Neuer Performance-Profiler  
   Präzise Messung pro Sektion: Dauer, Treffer, Fehler, Skip-Verhalten.  
   Ideal zur Optimierung großer Plex-Bibliotheken.

7. Sauberer, modularer Code  
   Weniger Redundanzen, weniger Fetches, schnelleres JSON-Parsing, klar kommentiert.

8. Zukunftssicher vorbereitet  
   Version 4.2 enthält bereits vorbereitete Module für spätere Features wie Quick-Mode, Auto-Recover und UI-Parser.


---

M.A.R.I. ist nun vollständig eingerichtet und läuft autonom im Hintergrund.
