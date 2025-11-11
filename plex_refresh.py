#!/usr/bin/env python3
import asyncio
import datetime
import os
import sys
import json
import psutil
from plexapi.server import PlexServer  # type: ignore
from dotenv import load_dotenv
import discord  # type: ignore
from discord.ext import commands  # type: ignore
from aiogram import Bot as TgBot  # type: ignore

import warnings
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# ========= WARNUNGEN UNTERDRÜCKEN (für plex.direct ohne Zertifikat) =========
warnings.simplefilter("ignore", InsecureRequestWarning)
urllib3.disable_warnings(InsecureRequestWarning)

# ========= STDOUT für Docker =========
sys.stdout.reconfigure(line_buffering=True)

# ========= .ENV LADEN =========
load_dotenv()

# ========= HILFSFUNKTIONEN FÜR ENV-VALIDIERUNG =========

def env_required(name: str) -> str:
    """Liest eine Pflicht-Umgebungsvariable, bricht mit Fehler ab, wenn sie fehlt oder leer ist."""
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        print(f"[ERROR] Umgebungsvariable {name} fehlt oder ist leer!", flush=True)
        sys.exit(1)
    return val

def env_int(name: str) -> int:
    """Pflicht-Integer aus ENV."""
    raw = env_required(name)
    try:
        return int(raw)
    except ValueError:
        print(f"[ERROR] Umgebungsvariable {name} muss eine ganze Zahl sein, ist aber: {raw!r}", flush=True)
        sys.exit(1)

def env_bool(name: str) -> bool:
    """Pflicht-Boolean aus ENV: erwartet 'true' oder 'false' (case-insensitive)."""
    raw = env_required(name).lower()
    if raw not in ("true", "false"):
        print(f"[ERROR] Umgebungsvariable {name} muss 'true' oder 'false' sein, ist aber: {raw!r}", flush=True)
        sys.exit(1)
    return raw == "true"

# ========= KONFIGURATION (ALLE OHNE FALLBACK) =========

PLEX_URL = env_required("PLEX_URL")
PLEX_TOKEN = env_required("PLEX_TOKEN")

REFRESH_INTERVAL_DAYS = env_int("REFRESH_INTERVAL_DAYS")
REFRESH_TIME = env_required("REFRESH_TIME")  # "HH:MM"

MAX_ITEMS_PER_LIBRARY = env_int("MAX_ITEMS_PER_LIBRARY")

LOG_FILE = env_required("LOG_FILE")
LOG_MAX_LINES = env_int("LOG_MAX_LINES")

HEALTH_FILE = env_required("HEALTH_FILE")
HEALTHCHECK_MAX_DAYS = env_int("HEALTHCHECK_MAX_DAYS")
HEALTHCHECK_INTERVAL_MINUTES = env_int("HEALTHCHECK_INTERVAL_MINUTES")

ENABLE_DISCORD = env_bool("ENABLE_DISCORD_NOTIFY")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")  # nur Pflicht, wenn ENABLE_DISCORD True
DISCORD_CHANNEL_ID_RAW = os.getenv("DISCORD_CHANNEL_ID")

ENABLE_TELEGRAM = env_bool("ENABLE_TELEGRAM_NOTIFY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID_RAW = os.getenv("TELEGRAM_CHAT_ID")

MSG_STATE_FILE = env_required("MSG_STATE_FILE")

# Zusätzliche Validierung je nach Flags
if ENABLE_DISCORD:
    if not DISCORD_TOKEN or not DISCORD_TOKEN.strip():
        print("[ERROR] ENABLE_DISCORD_NOTIFY=true, aber DISCORD_TOKEN ist nicht gesetzt!", flush=True)
        sys.exit(1)
    if not DISCORD_CHANNEL_ID_RAW or not DISCORD_CHANNEL_ID_RAW.strip():
        print("[ERROR] ENABLE_DISCORD_NOTIFY=true, aber DISCORD_CHANNEL_ID ist nicht gesetzt!", flush=True)
        sys.exit(1)
    try:
        DISCORD_CHANNEL_ID = int(DISCORD_CHANNEL_ID_RAW)
    except ValueError:
        print(f"[ERROR] DISCORD_CHANNEL_ID muss eine Zahl sein, ist aber: {DISCORD_CHANNEL_ID_RAW!r}", flush=True)
        sys.exit(1)
else:
    DISCORD_CHANNEL_ID = 0  # wird nicht benutzt

if ENABLE_TELEGRAM:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_BOT_TOKEN.strip():
        print("[ERROR] ENABLE_TELEGRAM_NOTIFY=true, aber TELEGRAM_BOT_TOKEN ist nicht gesetzt!", flush=True)
        sys.exit(1)
    if not TELEGRAM_CHAT_ID_RAW or not TELEGRAM_CHAT_ID_RAW.strip():
        print("[ERROR] ENABLE_TELEGRAM_NOTIFY=true, aber TELEGRAM_CHAT_ID ist nicht gesetzt!", flush=True)
        sys.exit(1)
    TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID_RAW)
else:
    TELEGRAM_CHAT_ID = 0  # wird nicht benutzt

# REFRESH_TIME validieren
try:
    _h, _m = map(int, REFRESH_TIME.split(":"))
    if not (0 <= _h < 24 and 0 <= _m < 60):
        raise ValueError
except Exception:
    print(f"[ERROR] REFRESH_TIME muss im Format HH:MM mit gültiger Stunde/Minute sein, ist aber: {REFRESH_TIME!r}", flush=True)
    sys.exit(1)

# ========= STATUS =========
status = {
    "plex_name": "—",
    "mode": "INIT",  # INIT / IDLE / REFRESH / PAUSE / ERROR
    "status_line": "⏳ Initialisiere Smart-Refresher...",
    "next_run": "— wird berechnet —",
    "health": "Noch kein Healthcheck ausgeführt.",
    "last_refresh": "Noch kein Refresh.",
    "last_refresh_details": "",
    "cpu_line": "—",
    "last_error": "",
}

def format_dur(sec: float) -> str:
    """Gibt die genaue Dauer im Format '3min' oder '1h 3min' aus (deutsch, ohne Textausschmückung)."""
    sec = int(sec)
    minuten, sekunden = divmod(sec, 60)
    stunden, minuten = divmod(minuten, 60)
    tage, stunden = divmod(stunden, 24)

    parts = []
    if tage:
        parts.append(f"{tage}d")
    if stunden:
        parts.append(f"{stunden}h")
    if minuten:
        parts.append(f"{minuten}min")
    if not parts:  # Falls weniger als 1 Minute
        parts.append(f"{sekunden}s")

    return " ".join(parts)


def next_target_datetime() -> datetime.datetime:
    """
    Berechnet den nächsten geplanten Zielzeitpunkt (datetime) basierend auf:
    - REFRESH_TIME (HH:MM)
    - REFRESH_INTERVAL_DAYS (Intervall in Tagen)
    Liefert ein datetime-Objekt (Zieldatum + -zeit).
    """
    now = datetime.datetime.now()
    h, m = map(int, REFRESH_TIME.split(":"))
    base = now.replace(hour=h, minute=m, second=0, microsecond=0)

    interval = max(1, int(REFRESH_INTERVAL_DAYS))

    # nächster Zeitpunkt > now
    while base <= now:
        base += datetime.timedelta(days=interval)

    return base

# ========= INIT HEALTH =========
if not os.path.exists(HEALTH_FILE):
    now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    with open(HEALTH_FILE, "w") as f:
        f.write("OK|" + now_str)
    status["health"] = f"OK (zuletzt: {now_str})"

# ========= MSG_STATE =========
def load_state():
    return json.load(open(MSG_STATE_FILE)) if os.path.exists(MSG_STATE_FILE) else {}

def save_state(d):
    with open(MSG_STATE_FILE, "w") as f:
        json.dump(d, f, indent=2)

msg_state = load_state()
state_lock = asyncio.Lock()

# ========= DISCORD & TELEGRAM =========
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents) if ENABLE_DISCORD else None

tg_bot: TgBot | None = None
if ENABLE_TELEGRAM:
    tg_bot = TgBot(token=TELEGRAM_BOT_TOKEN)

# ========= LOGGING =========
def log(msg, prefix="MAIN"):
    ts = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    line = f"[{ts}] [{prefix}] {msg}"
    print(line, flush=True)
    try:
        lines = open(LOG_FILE).readlines() if os.path.exists(LOG_FILE) else []
        lines.insert(0, line + "\n")
        open(LOG_FILE, "w").writelines(lines[:LOG_MAX_LINES])
    except Exception:
        pass

# ========= HEALTH =========
def write_health(ok=True):
    with open(HEALTH_FILE, "w") as f:
        f.write(("OK" if ok else "FAIL") + "|" + datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
    log(f"Health gesetzt: {'OK' if ok else 'FAIL'}", "HEALTH")

async def periodic_health():
    while True:
        try:
            st, ts = open(HEALTH_FILE).read().strip().split("|")
            last_dt = datetime.datetime.strptime(ts, "%d.%m.%Y %H:%M:%S")
            delta = (datetime.datetime.now() - last_dt).days
            if st == "FAIL" or delta > HEALTHCHECK_MAX_DAYS:
                msg = f"🚨 Letzter erfolgreicher Refresh vor {delta} Tagen!"
                status["health"] = msg
            else:
                status["health"] = f"OK (zuletzt: {ts})"
            await update_embed()
        except Exception as e:
            status["health"] = f"Fehler: {e}"
        await asyncio.sleep(HEALTHCHECK_INTERVAL_MINUTES * 60)

# ========= EMBED / TELEGRAM OUTPUT =========
async def update_embed():
    await send_discord()
    await send_tg()

async def send_discord():
    if not ENABLE_DISCORD or not bot or not DISCORD_TOKEN or not DISCORD_CHANNEL_ID:
        return

    now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    s = status

    # next_run schön darstellen
    next_lines = str(s["next_run"]).splitlines()
    if len(next_lines) >= 2:
        next_line_formatted = (
            f"🕒 **Nächster geplanter Lauf:** {next_lines[0]}\n"
            f"📅 {next_lines[1]}\n\n"
        )
    else:
        next_line_formatted = f"🕒 **Nächster geplanter Lauf:** {s['next_run']}\n\n"

    # Letzter Refresh-Text bei laufendem ersten Scan anpassen
    last_refresh_display = s["last_refresh"]
    if s["mode"] == "REFRESH" and last_refresh_display.startswith("Noch kein Refresh"):
        last_refresh_display = "🔄 Läuft gerade – erster Smart-Refresh."

    # CPU-Zeile ggf. ergänzen
    cpu_info = ""
    if s["cpu_line"] != "—":
        cpu_info = f"\n• **CPU:** {s['cpu_line']}"

    # Details / Fehler
    details = ""
    if s["last_refresh_details"]:
        details += f"\n\n{s['last_refresh_details']}"
    if s["last_error"]:
        details += f"\n\n❌ **Fehler:** {s['last_error']}"

    # Punkt nur anzeigen, wenn kein "Läuft gerade"-Text
    prefix = "" if "Läuft gerade" in last_refresh_display else "• "

    desc = (
        f"🟢 **Status:** {s['status_line']}\n"
        f"⚙️ **Modus:** {s['mode']}\n\n"
        f"{next_line_formatted}"
        f"✅ **Healthcheck:** {s['health']}\n\n"
        f"🧾 **Letzter Smart-Refresh:**\n"
        f"{prefix}{last_refresh_display}"
        f"{cpu_info}"
        f"{details}"
    )

    # Farbe nach Modus
    color = 0x00FF00  # grün
    if s["mode"] in ("REFRESH", "PAUSE"):
        color = 0xFFA500  # orange
    if s["mode"] == "ERROR":
        color = 0xFF0000  # rot

    embed = discord.Embed(description=desc, color=color)
    embed.set_footer(text=f"⏱️ Aktualisiert: {now}")

    try:
        ch = bot.get_channel(DISCORD_CHANNEL_ID) or await bot.fetch_channel(DISCORD_CHANNEL_ID)
        async with state_lock:
            mid = msg_state.get("discord_main")
            if mid:
                try:
                    msg = await ch.fetch_message(mid)
                    await msg.edit(embed=embed)
                    return
                except discord.NotFound:
                    pass
            msg = await ch.send(embed=embed)
            msg_state["discord_main"] = msg.id
            save_state(msg_state)
    except Exception as e:
        log(f"Discord Fehler: {e}", "DISCORD")

async def send_tg():
    if not ENABLE_TELEGRAM or not tg_bot or not TELEGRAM_CHAT_ID:
        return

    s = status
    now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    # Letzter Refresh-Text bei laufendem ersten Scan anpassen
    last_refresh_display = s["last_refresh"]
    if s["mode"] == "REFRESH" and last_refresh_display.startswith("Noch kein Refresh"):
        last_refresh_display = "🔄 Läuft gerade – erster Smart-Refresh."

    # Punkt nur anzeigen, wenn kein "Läuft gerade"-Text
    prefix = "" if "Läuft gerade" in last_refresh_display else "• "

    text = (
        f"🟢 <b>Status:</b> {s['status_line']}\n"
        f"⚙️ <b>Modus:</b> {s['mode']}\n\n"
        f"🕒 <b>Nächster geplanter Lauf:</b> {s['next_run']}\n\n"
        f"✅ <b>Healthcheck:</b> {s['health']}\n\n"
        f"🧾 <b>Letzter Smart-Refresh:</b>\n"
        f"{prefix}{last_refresh_display}"
    )


    if s["cpu_line"] != "—":
        text += f"\n• <b>CPU:</b> {s['cpu_line']}"
    if s["last_refresh_details"]:
        text += f"\n\n{s['last_refresh_details']}"
    if s["last_error"]:
        text += f"\n\n❌ <b>Fehler:</b> {s['last_error']}"

    text += f"\n\n⏱️ <i>Aktualisiert:</i> {now}"

    async with state_lock:
        mid = msg_state.get("telegram_main")
        try:
            if mid:
                await tg_bot.edit_message_text(
                    chat_id=TELEGRAM_CHAT_ID,
                    message_id=mid,
                    text=text,
                    parse_mode="HTML",
                )
                return
        except Exception:
            pass
        sent = await tg_bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode="HTML",
        )
        msg_state["telegram_main"] = sent.message_id
        save_state(msg_state)

# ========= SMART-REFRESH LOOP =========
async def smart_refresh():
    # Plex verbinden
    try:
        plex = PlexServer(PLEX_URL, PLEX_TOKEN)
        # Zertifikatsprüfung aus, weil plex.direct
        plex._session.verify = False
        status["plex_name"] = plex.friendlyName
        status["status_line"] = f"✅ Verbunden mit Plex: {plex.friendlyName}"
        status["mode"] = "IDLE"
        log(f"✅ Verbunden mit Plex: {plex.friendlyName}", "REFRESH")
        await update_embed()
    except Exception as e:
        msg = f"❌ Fehler bei Verbindung zu Plex: {e}"
        log(msg, "REFRESH")
        status.update(
            {
                "status_line": msg,
                "mode": "ERROR",
                "last_error": msg,
            }
        )
        write_health(False)
        await update_embed()
        return

    while True:
        # === NÄCHSTEN ZIELZEITPUNKT BERECHNEN ===
        tgt = next_target_datetime()
        now = datetime.datetime.now()
        sec = (tgt - now).total_seconds()
        if sec < 0:
            sec = 0

        # Countdown-Text
        total_min = int(sec // 60)
        h_rest, m_rest = divmod(total_min, 60)
        if h_rest > 0:
            time_str = f"in {h_rest}h {m_rest}min"
        else:
            time_str = f"in {m_rest}min"

        status.update(
            {
                "mode": "IDLE",
                "status_line": f"✅ Plex {status['plex_name']} – Bereit",
                "next_run": f"{time_str}\n{tgt:%d.%m.%Y %H:%M}",
            }
        )
        await update_embed()

        # Warten bis zur Zielzeit
        await asyncio.sleep(sec)

        # --- STARTLAUF ---
        log("=" * 80, "REFRESH")
        status.update(
            {
                "mode": "REFRESH",
                "status_line": f"🔄 Smart-Refresh läuft auf {status['plex_name']} ...",
                "next_run": "— Lauf aktiv —",
                "last_error": "",
            }
        )
        await update_embed()

        # --- Plex-Scan prüfen (best effort) ---
        try:
            is_scanning = any(
                getattr(secobj, "isScanning", False)
                for secobj in plex.library.sections()
                if hasattr(secobj, "isScanning")
            )
        except Exception:
            is_scanning = False

        if is_scanning:
            wait_mins = 5
            resume_at = datetime.datetime.now() + datetime.timedelta(minutes=wait_mins)
            resume_str = resume_at.strftime("%H:%M:%S")
            log(f"Plex-Scan erkannt – Smart-Refresh pausiert bis {resume_str}", "REFRESH")
            status.update(
                {
                    "mode": "PAUSE",
                    "status_line": f"⏸️ Plex-Scan erkannt – pausiert bis {resume_str} ({status['plex_name']})",
                    "next_run": f"Pausiert bis {resume_str}",
                }
            )
            await update_embed()
            await asyncio.sleep(wait_mins * 60)
            # danach nächste Schleifenrunde (neuer Plan)
            continue

        # --- SMART-REFRESH ---
        refreshed = []
        errors = []
        cpu_vals = []
        peak = 0.0
        start = datetime.datetime.now()

        try:
            sections = [s for s in plex.library.sections() if s.type in ["movie", "show"]]
        except Exception as e:
            msg = f"Fehler beim Laden der Bibliotheken: {e}"
            log(msg, "REFRESH")
            status.update({
                "mode": "ERROR",
                "status_line": msg,
                "last_error": msg,
            })
            write_health(False)
            await update_embed()
            continue

        total_sections = len(sections) or 1

        for idx, secobj in enumerate(sections, start=1):
            status["status_line"] = f"🔄 Smart-Refresh läuft ({idx}/{total_sections}): {secobj.title}"
            await update_embed()

            try:
                # Nur die Anzahl prüfen, die in der ENV angegeben ist
                items = secobj.recentlyAdded()[:MAX_ITEMS_PER_LIBRARY]
            except Exception as e:
                errors.append(f"{secobj.title} – Fehler beim Laden der Einträge: {e}")
                continue

            fixed_count = 0

            for itm in items:
                try:
                    title = getattr(itm, "title", "Unbekannt")
                    thumb = getattr(itm, "thumb", None)
                    summary = (getattr(itm, "summary", "") or "").strip()
                    rating = getattr(itm, "rating", None)
                    guids = getattr(itm, "guids", [])

                    # ===== SMART-LOGIK =====
                    # GUID: Fehler NUR, wenn GAR KEINE GUID vorhanden ist
                    has_guid = bool(guids)
                    missing_guid = not has_guid

                    # Cover / Beschreibung
                    missing_thumb = thumb is None
                    missing_summary = not summary

                    # Rating (nur in Kombination mit anderen Problemen relevant)
                    missing_rating = rating is None

                    # Refresh nötig, wenn:
                    # - keine GUID ODER
                    # - sowohl Cover als auch Summary fehlen ODER
                    # - Rating fehlt UND zusätzlich noch ein anderes Problem existiert
                    needs_refresh = (
                        missing_guid
                        or (missing_thumb and missing_summary)
                        or (missing_rating and (missing_guid or missing_thumb or missing_summary))
                    )

                    if not needs_refresh:
                        continue  # alles ok, nichts tun

                    itm.refresh()
                    fixed_count += 1

                    # Non-blocking CPU-Check (verhindert Discord Heartbeat-Block)
                    c = psutil.cpu_percent(interval=None)
                    cpu_vals.append(c)
                    peak = max(peak, c)
                    await asyncio.sleep(0)  # kurz den Event-Loop freigeben

                except Exception as e:
                    errors.append(f"{title} – Fehler: {e}")

            # Nur Libraries mit Änderungen ausgeben
            if fixed_count > 0:
                refreshed.append((secobj.title, fixed_count))
                log(f"{secobj.title}: {fixed_count} Eintrag{'e' if fixed_count != 1 else ''} refresht", "REFRESH")

        # --- ZUSAMMENFASSUNG ---
        dur = (datetime.datetime.now() - start).total_seconds()
        avg = sum(cpu_vals) / len(cpu_vals) if cpu_vals else 0.0

        if refreshed:
            total = sum(c for _, c in refreshed)
            # 🔹 Smarter Plural-Fix für Eintrag/Einträge
            lines = [f"• {lib}: {cnt} Eintrag{'e' if cnt != 1 else ''}" for lib, cnt in refreshed]

            status.update({
                "last_refresh": f"{start:%d.%m.%Y %H:%M:%S} – {total} Einträge in {len(refreshed)} Bibliotheken · Dauer: {format_dur(dur)}",
                "last_refresh_details": "\n".join(lines),
                "cpu_line": f"⌀ {avg:.1f}% / **Peak** {peak:.1f}%",
                "status_line": f"✅ Smart-Refresh abgeschlossen ({status['plex_name']})",
            })
        else:
            status.update({
                "last_refresh": "✅ Keine fehlerhaften Metadaten gefunden – alles aktuell.",
                "last_refresh_details": "",
                "cpu_line": "—",
                "status_line": f"✅ Plex {status['plex_name']} – alles aktuell",
            })
            log("Keine fehlerhaften Metadaten gefunden – alles aktuell.", "REFRESH")

        if errors:
            err_text = "\n".join(errors[:10])  # nicht komplett ausufern
            status["last_error"] = f"Es sind Fehler aufgetreten:\n{err_text}"
            log(status["last_error"], "REFRESH")
        else:
            status["last_error"] = ""

        write_health(True)

        # Direkt nach dem Lauf schon mal den nächsten Termin ausrechnen
        nxt = next_target_datetime()
        now2 = datetime.datetime.now()
        sec2 = (nxt - now2).total_seconds()
        if sec2 < 0:
            sec2 = 0
        total_min2 = int(sec2 // 60)
        h2, m2 = divmod(total_min2, 60)
        if h2 > 0:
            time_str2 = f"in {h2}h {m2}min"
        else:
            time_str2 = f"in {m2}min"

        status.update({
            "mode": "IDLE",
            "next_run": f"{time_str2}\n{nxt:%d.%m.%Y %H:%M}",
        })
        await update_embed()

# ========= BOT START =========
if ENABLE_DISCORD:
    @bot.event
    async def on_ready():
        log(f"Verbunden als {bot.user}", "MAIN")
        asyncio.create_task(smart_refresh())
        asyncio.create_task(periodic_health())
        log("🟢 Smart-Refresher läuft.", "MAIN")
else:
    # Wenn Discord aus ist, müssen wir den Smart-Loop selbst starten
    async def _runner_without_discord():
        log("Discord deaktiviert – starte Smart-Refresher ohne Discord-Bot.", "MAIN")
        asyncio.create_task(periodic_health())
        await smart_refresh()

def main():
    if not PLEX_URL or not PLEX_TOKEN:
        print("❌ PLEX_URL oder PLEX_TOKEN fehlen! (sollte durch ENV-Check schon aufgefallen sein)", flush=True)
        return

    if ENABLE_DISCORD:
        try:
            bot.run(DISCORD_TOKEN)
        finally:
            if tg_bot:
                try:
                    asyncio.run(tg_bot.session.close())
                except Exception:
                    pass
    else:
        # eigener Event-Loop, wenn kein Discord-Bot läuft
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(_runner_without_discord())
        finally:
            if tg_bot:
                try:
                    loop.run_until_complete(tg_bot.session.close())
                except Exception:
                    pass

if __name__ == "__main__":
    print("📦 Starte Plex Smart-Refresher ...", flush=True)
    main()
