# m.a.r.i
M.A.R.I. – Metadata Auto Refresh Interface for Plex. Smart. Silent. Always fresh.


## 🧩 Discord Bot Setup Guide

### 1. Bot erstellen
1. Öffne das [Discord Developer Portal](https://discord.com/developers/applications)
2. Klicke auf **“New Application”**, gib einen Namen ein und erstelle deinen Bot.
3. Wechsle im Menü links zu **“Bot” → “Add Bot”**.

---

### 2. Intents aktivieren
Aktiviere im Abschnitt **“Privileged Gateway Intents”** folgende Optionen:

- ✅ **Presence Intent** – ermöglicht das Empfangen von Status-Updates
- ✅ **Server Members Intent** – erlaubt Zugriff auf Mitglieder-Informationen
- ✅ **Message Content Intent** – erlaubt das Lesen von Nachrichteninhalten

> Diese Intents sind notwendig, damit dein Bot auf Server-Ereignisse reagieren kann.

---

### 3. Berechtigungen festlegen
Gehe zu **“OAuth2 → URL Generator”** und wähle:

- **Scopes:** `bot`
- **Bot Permissions:** Aktiviere **Administrator**

Der Berechtigungswert (*Permissions Integer*) wird automatisch generiert — in deinem Beispiel ist das **8**.

---

### 4. Bot auf den Server einladen
Ersetze `YOUR_APPLICATION_ID` durch die Client-ID deiner Anwendung und öffne den folgenden Link:

https://discord.com/oauth2/authorize?client_id=YOUR_APPLICATION_ID&scope=bot&permissions=8

Dadurch kannst du deinen Bot auf deinen Discord-Server hinzufügen.

---

### ℹ️ Hinweis
Discord ändert gelegentlich die Namen einzelner Berechtigungen, aber der finale **Permissions Integer** bleibt gleich.  
Falls du andere Rechte benötigst, kannst du den Wert im URL-Generator entsprechend anpassen.


-------------------------------------------------


## ⚙️ Plex Refresher – Linux Setup Guide

### 1. Verzeichnisstruktur erstellen
```bash
sudo mkdir -p /pfad/zum/plex_refresher
cd /pfad/zum/plex_refresher
```

### 2. Dateien anlegen
Erstelle die folgenden Dateien in diesem Verzeichnis:

```
.env
Dockerfile
Docker_Compose.yaml
entrypoint.sh
plex_refresh.py
requirements.txt
```

> Die Dateien `README.md` und `Screenshot 1.png / Screenshot 2.png` werden nicht benötigt.

### 3. Dateiinhalte einfügen
Kopiere den bereitgestellten Code aus deinem Projekt oder Repository in die jeweiligen Dateien.  
Achte darauf, dass die `.env`-Datei deine API-Keys und Variablen enthält.

Beispiel:
```bash
nano .env
```
Dann den Inhalt einfügen und mit `CTRL + O`, `ENTER`, `CTRL + X` speichern.

### 4. Dateiberechtigungen setzen
Setze die korrekten Rechte und Besitzverhältnisse, damit Docker und dein System sauber arbeiten:

```bash
# Besitzer auf aktuellen Benutzer (optional anpassen)
sudo chown -R $USER:$USER /pfad/zum/plex_refresher

# Ausführrechte für Skripte
sudo chmod +x /pfad/zum/plex_refresher/entrypoint.sh
sudo chmod 644 /pfad/zum/plex_refresher/.env
sudo chmod 644 /pfad/zum/plex_refresher/requirements.txt
sudo chmod 644 /pfad/zum/plex_refresher/Docker_Compose.yaml
sudo chmod 644 /pfad/zum/plex_refresher/Dockerfile
sudo chmod 644 /pfad/zum/plex_refresher/plex_refresh.py
```

### 5. Docker-Container starten
Wechsle in das Verzeichnis und starte den Container:

```bash
cd /pfad/zum/plex_refresher
sudo docker compose up -d
```

### 6. Logs prüfen
Die Logausgabe kannst du direkt mit Portainer oder per CLI prüfen:

```bash
sudo docker compose logs -f
```

Damit ist dein **Plex Refresher** vollständig eingerichtet und betriebsbereit.
