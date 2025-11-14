#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Plex Smart-Refresher 2.0 – modularisierte Ein-Datei-Version

- Rotierender Segment-Scan pro Bibliothek (MAX_ITEMS_PER_SCAN)
- SQLite-Zustand (active / cooldown / dead) mit Eskalation & Reaktivierung
- Verbesserte CPU-Messung (prozessbasiert)
- Discord-Embed + optional Telegram-Status, beide aktualisieren statt spammen
- Healthcheck-File & periodische Health-Anzeige
- Docker-freundliches Logging (Portainer) + kompaktere Struktur
"""

import asyncio
import datetime as dt
import json
import os
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Tuple

import psutil
from dotenv import load_dotenv
from plexapi.server import PlexServer   # type: ignore

import warnings
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# -------------------------------------------------------------
# Warnungen & stdout
# -------------------------------------------------------------
warnings.simplefilter("ignore", InsecureRequestWarning)
urllib3.disable_warnings(InsecureRequestWarning)

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

load_dotenv()

# -------------------------------------------------------------
# ENV-Helper
# -------------------------------------------------------------
def env_required(name: str) -> str:
    val = os.getenv(name)
    if val is None or str(val).strip() == "":
        print(f"[ERROR] Umgebungsvariable {name} fehlt oder ist leer!", flush=True)
        sys.exit(1)
    return val


def env_int(name: str) -> int:
    raw = env_required(name)
    try:
        return int(raw)
    except ValueError:
        print(f"[ERROR] Umgebungsvariable {name} muss eine ganze Zahl sein, ist aber: {raw!r}", flush=True)
        sys.exit(1)


def env_bool(name: str) -> bool:
    raw = env_required(name).lower()
    if raw not in ("true", "false"):
        print(f"[ERROR] Umgebungsvariable {name} muss 'true' oder 'false' sein, ist aber: {raw!r}", flush=True)
        sys.exit(1)
    return raw == "true"


# -------------------------------------------------------------
# Grund-Config aus ENV
# -------------------------------------------------------------
PLEX_URL = env_required("PLEX_URL")
PLEX_TOKEN = env_required("PLEX_TOKEN")

REFRESH_INTERVAL_DAYS = env_int("REFRESH_INTERVAL_DAYS")
REFRESH_TIME = env_required("REFRESH_TIME")  # HH:MM
MAX_ITEMS_PER_SCAN = env_int("MAX_ITEMS_PER_SCAN")

LOG_FILE = env_required("LOG_FILE")
LOG_MAX_LINES = env_int("LOG_MAX_LINES")
HEALTH_FILE = env_required("HEALTH_FILE")
MSG_STATE_FILE = env_required("MSG_STATE_FILE")

HEALTHCHECK_MAX_DAYS = env_int("HEALTHCHECK_MAX_DAYS")
HEALTHCHECK_INTERVAL_MINUTES = env_int("HEALTHCHECK_INTERVAL_MINUTES")

ENABLE_DISCORD = env_bool("ENABLE_DISCORD_NOTIFY")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID_RAW = os.getenv("DISCORD_CHANNEL_ID", "")

ENABLE_TELEGRAM = env_bool("ENABLE_TELEGRAM_NOTIFY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID_RAW = os.getenv("TELEGRAM_CHAT_ID", "0")

PLEX_TIMEOUT = int(os.getenv("PLEX_TIMEOUT", "30"))  # Sekunden

if ENABLE_DISCORD:
    if not DISCORD_TOKEN.strip():
        print("[ERROR] ENABLE_DISCORD_NOTIFY=true, aber DISCORD_TOKEN fehlt!", flush=True)
        sys.exit(1)
    try:
        DISCORD_CHANNEL_ID = int(DISCORD_CHANNEL_ID_RAW)
    except Exception:
        print(f"[ERROR] DISCORD_CHANNEL_ID muss Zahl sein, ist aber: {DISCORD_CHANNEL_ID_RAW!r}", flush=True)
        sys.exit(1)
else:
    DISCORD_CHANNEL_ID = 0

if ENABLE_TELEGRAM:
    if not TELEGRAM_BOT_TOKEN.strip():
        print("[ERROR] ENABLE_TELEGRAM_NOTIFY=true, aber TELEGRAM_BOT_TOKEN fehlt!", flush=True)
        sys.exit(1)
    try:
        TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID_RAW)
    except Exception:
        print(f"[ERROR] TELEGRAM_CHAT_ID muss Zahl sein, ist aber: {TELEGRAM_CHAT_ID_RAW!r}", flush=True)
        sys.exit(1)
else:
    TELEGRAM_CHAT_ID = 0

# -------------------------------------------------------------
# Optionale Imports: Discord / Telegram
# -------------------------------------------------------------
ENABLE_DISCORD_IMPORT = False
ENABLE_TELEGRAM_IMPORT = False

try:
    import discord  # type: ignore
    from discord.ext import commands  # type: ignore

    ENABLE_DISCORD_IMPORT = True
except Exception:
    pass

try:
    from aiogram import Bot as TgBot  # type: ignore

    ENABLE_TELEGRAM_IMPORT = True
except Exception:
    pass

# -------------------------------------------------------------
# Globale Zustände
# -------------------------------------------------------------
status: Dict[str, Any] = {
    "plex_name": "—",
    "mode": "INIT",
    "status_line": "⏳ Initialisiere Smart-Refresher 2.0...",
    "next_run": "— wird berechnet —",
    "health": "Noch kein Healthcheck.",
    "last_refresh": "Noch kein Refresh.",
    "last_refresh_details": "",
    "cpu_line": "—",
    "last_error": "",
    "stats_block": "",
}

# Prozess für CPU-Messung
PROC = psutil.Process()

# Nachrichtenzustand (Discord/Telegram IDs)
def load_state() -> Dict[str, Any]:
    if os.path.exists(MSG_STATE_FILE):
        try:
            return json.load(open(MSG_STATE_FILE))
        except Exception:
            return {}
    return {}


def save_state(d: Dict[str, Any]):
    try:
        with open(MSG_STATE_FILE, "w") as f:
            json.dump(d, f, indent=2)
    except Exception:
        pass


msg_state = load_state()
state_lock = asyncio.Lock()

# Falls es einen gespeicherten Status aus einem früheren Lauf gibt,
# übernehmen wir ihn, damit nach einem Neustart der letzte Lauf
# (last_refresh, CPU, Statistik, Fehler) im Embed weiter sichtbar bleibt.
if isinstance(msg_state.get("last_status"), dict):
    ls = msg_state["last_status"]
    for k in ("last_refresh", "last_refresh_details", "cpu_line", "last_error", "stats_block"):
        if k in ls and ls[k]:
            status[k] = ls[k]

bot: Optional["commands.Bot"] = None
tg_bot: Optional["TgBot"] = None


# CPU-SAMPLER (misst während des gesamten Smart-Refresh-Laufs)
cpu_vals_global: List[float] = []
cpu_peak_global: float = 0.0

async def cpu_sampler():
    global cpu_peak_global
    while True:
        try:
            v = PROC.cpu_percent(interval=None)
            cpu_vals_global.append(v)
            cpu_peak_global = max(cpu_peak_global, v)
        except Exception:
            pass
        await asyncio.sleep(1)  # alle 1 Sekunde sampeln

# --- SAFER FETCH (verhindert Plex Disconnects) ---
async def safe_fetch(plex, rating_key, retries=3):
    for _ in range(retries):
        try:
            return plex.fetchItem(rating_key)
        except Exception:
            await asyncio.sleep(0.2)
    return None

# -------------------------------------------------------------
# Logging & Health
# -------------------------------------------------------------
def log(msg: str, prefix: str = "MAIN"):
    ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    line = f"[{ts}] [{prefix}] {msg}"
    print(line, flush=True)
    try:
        lines = open(LOG_FILE).readlines() if os.path.exists(LOG_FILE) else []
        lines.insert(0, line + "\n")
        open(LOG_FILE, "w").writelines(lines[:LOG_MAX_LINES])
    except Exception:
        pass


def write_health(ok: bool = True):
    try:
        with open(HEALTH_FILE, "w") as f:
            f.write(("OK" if ok else "FAIL") + "|" + dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
        log(f"Health gesetzt: {'OK' if ok else 'FAIL'}", "HEALTH")
    except Exception as e:
        log(f"Health-Write-Fehler: {e}", "HEALTH")


async def periodic_health(update_embed_cb):
    while True:
        try:
            if not os.path.exists(HEALTH_FILE):
                status["health"] = "Keine Health-Datei vorhanden."
            else:
                raw = open(HEALTH_FILE).read().strip()
                st, ts = raw.split("|")
                last_dt = dt.datetime.strptime(ts, "%d.%m.%Y %H:%M:%S")
                delta_days = (dt.datetime.now() - last_dt).days
                if st == "FAIL" or delta_days > HEALTHCHECK_MAX_DAYS:
                    status["health"] = f"🚨 Letzter erfolgreicher Refresh vor {delta_days} Tagen!"
                else:
                    status["health"] = f"OK (zuletzt: {ts})"
        except Exception as e:
            status["health"] = f"Fehler: {e}"
        await update_embed_cb()
        await asyncio.sleep(HEALTHCHECK_INTERVAL_MINUTES * 60)


# -------------------------------------------------------------
# Zeit-Helfer
# -------------------------------------------------------------
def format_dur(sec: float) -> str:
    sec = int(sec)
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m:
        parts.append(f"{m}min")
    if not parts:
        parts.append(f"{s}s")
    return " ".join(parts)


def next_target_datetime() -> dt.datetime:
    now = dt.datetime.now()
    h, m = map(int, REFRESH_TIME.split(":"))
    base = now.replace(hour=h, minute=m, second=0, microsecond=0)
    interval = max(1, int(REFRESH_INTERVAL_DAYS))
    while base <= now:
        base += dt.timedelta(days=interval)
    return base


def next_run_human(dt_target: dt.datetime) -> str:
    now = dt.datetime.now()
    sec = max(0, (dt_target - now).total_seconds())
    total_min = int(sec // 60)
    h, m = divmod(total_min, 60)
    if h > 0:
        t = f"in {h}h {m}min"
    else:
        t = f"in {m}min"
    return f"{t}\n{dt_target:%d.%m.%Y %H:%M}"


def iso_now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")


def iso_in_days(days: int) -> str:
    return (dt.datetime.now() + dt.timedelta(days=days)).isoformat(timespec="seconds")


# -------------------------------------------------------------
# DB
# -------------------------------------------------------------
def db_path_from_log() -> str:
    base_dir = os.path.dirname(LOG_FILE) or "/app"
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, "refresh_state.db")


DB_PATH = os.getenv("REFRESH_DB_PATH", db_path_from_log())

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS media_state (
    rating_key      TEXT PRIMARY KEY,
    library         TEXT,
    last_checked    TEXT,
    last_updated_at TEXT,
    fail_count      INTEGER DEFAULT 0,
    ignore_until    TEXT,
    state           TEXT DEFAULT 'active',
    note            TEXT
);
CREATE TABLE IF NOT EXISTS library_cursor (
    library TEXT PRIMARY KEY,
    offset  INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_media_library ON media_state(library);
"""


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def db_init():
    conn = db_connect()
    with conn:
        conn.executescript(SCHEMA_SQL)
    conn.close()


def db_get_cursor(library: str) -> int:
    conn = db_connect()
    try:
        cur = conn.execute("SELECT offset FROM library_cursor WHERE library=?", (library,))
        row = cur.fetchone()
        if row:
            return int(row["offset"])
        conn.execute("INSERT OR IGNORE INTO library_cursor(library, offset) VALUES(?,?)", (library, 0))
        return 0
    finally:
        conn.close()


def db_set_cursor(library: str, offset: int):
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO library_cursor(library, offset) VALUES(?,?) "
            "ON CONFLICT(library) DO UPDATE SET offset=excluded.offset",
            (library, offset),
        )
    finally:
        conn.close()


def db_get_media(rating_key: str) -> Optional[sqlite3.Row]:
    conn = db_connect()
    try:
        cur = conn.execute("SELECT * FROM media_state WHERE rating_key=?", (rating_key,))
        return cur.fetchone()
    finally:
        conn.close()


def db_upsert_media(
    rating_key: str,
    library: str,
    last_checked: str,
    last_updated_at: str,
    fail_count: int,
    state: str,
    ignore_until: Optional[str],
    note: Optional[str],
):
    conn = db_connect()
    try:
        conn.execute(
            """
            INSERT INTO media_state(rating_key, library, last_checked, last_updated_at, fail_count, state, ignore_until, note)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(rating_key) DO UPDATE SET
                library=excluded.library,
                last_checked=excluded.last_checked,
                last_updated_at=excluded.last_updated_at,
                fail_count=excluded.fail_count,
                state=excluded.state,
                ignore_until=excluded.ignore_until,
                note=excluded.note
            """,
            (rating_key, library, last_checked, last_updated_at, fail_count, state, ignore_until, note),
        )
    finally:
        conn.close()


def db_count_dead_total() -> int:
    conn = db_connect()
    try:
        cur = conn.execute("SELECT COUNT(*) AS c FROM media_state WHERE state='dead'")
        return int(cur.fetchone()["c"])
    finally:
        conn.close()


# -------------------------------------------------------------
# Metadaten-Logik
# -------------------------------------------------------------
MAX_FAILS = 5
COOLDOWN_STEPS_DAYS = [1, 7, 14, 30]


def needs_refresh(item) -> Tuple[bool, Dict[str, Any]]:
    """
    Kriterien bewusst simpel & transparent halten:
    - Kein GUID
    - Oder: Poster & Summary fehlen
    - Oder: Rating fehlt und gleichzeitig (kein GUID oder kein Poster oder keine Summary)
    """
    title = getattr(item, "title", "Unbekannt")
    thumb = getattr(item, "thumb", None)
    summary = (getattr(item, "summary", "") or "").strip()
    rating = getattr(item, "rating", None)
    guids = getattr(item, "guids", [])
    has_guid = bool(guids)

    missing_guid = not has_guid
    missing_thumb = thumb is None
    missing_summary = not summary
    missing_rating = rating is None

    needs = (
        missing_guid
        or (missing_thumb and missing_summary)
        or (missing_rating and (missing_guid or missing_thumb or missing_summary))
    )

    return needs, {
        "title": title,
        "missing_guid": missing_guid,
        "missing_thumb": missing_thumb,
        "missing_summary": missing_summary,
        "missing_rating": missing_rating,
    }

async def refresh_item_and_check(plex: PlexServer, item) -> bool:
    # Erst einmal normalen Refresh anstoßen
    try:
        item.refresh()
    except Exception:
        return False

    # Eventloop kurz freigeben
    await asyncio.sleep(0)

    # Jetzt MIT Retry-Logik sicher neu laden
    fresh = await safe_fetch(plex, item.ratingKey)
    if fresh is None:
        return False

    needs, _ = needs_refresh(fresh)
    return not needs

# -------------------------------------------------------------
# Discord / Telegram Embeds
# -------------------------------------------------------------
async def update_discord_embed():
    if not ENABLE_DISCORD or not ENABLE_DISCORD_IMPORT or not bot or not DISCORD_CHANNEL_ID:
        return

    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    s = status

    # Nächster Lauf hübsch auf zwei Zeilen
    next_lines = str(s["next_run"]).splitlines()
    if len(next_lines) >= 2:
        next_line_formatted = (
            f"🕒 **Nächster geplanter Lauf:** {next_lines[0]}\n"
            f"📅 {next_lines[1]}\n\n"
        )
    else:
        next_line_formatted = f"🕒 **Nächster geplanter Lauf:** {s['next_run']}\n\n"

    last_refresh_display = s["last_refresh"]
    if s["mode"] == "REFRESH" and last_refresh_display.startswith("Noch kein Refresh"):
        last_refresh_display = "🔄 Läuft gerade – erster Smart-Refresh."

    if s["cpu_line"] != "—":
        # Erwartetes Format: "⌀ X% / Peak: Y%"
        try:
            cpu_avg, cpu_peak = s["cpu_line"].split("/ Peak:")
            cpu_info = f"\n• **CPU:** {cpu_avg}/ **Peak:**{cpu_peak}"
        except ValueError:
            # Fallback, falls das Format sich irgendwann ändert
            cpu_info = f"\n• **CPU:** {s['cpu_line']}"
    else:
        cpu_info = ""

    details = ""
    if s["last_refresh_details"]:
        details += f"\n\n{s['last_refresh_details']}"
    if s.get("stats_block"):
        details += f"\n\n{s['stats_block']}"
    if s["last_error"]:
        details += f"\n\n❌ **Fehler:** {s['last_error']}"

    stripped = last_refresh_display.lstrip()
    prefix = "" if stripped.startswith(("🔄", "❌", "✅")) else "• "

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

    color = 0x00FF00
    if s["mode"] in ("REFRESH", "PAUSE"):
        color = 0xFFA500
    if s["mode"] == "ERROR":
        color = 0xFF0000

    if len(desc) > 4000:
        desc = desc[:4000] + "\n\n… (gekürzt – siehe Log)"

    embed = discord.Embed(description=desc, color=color)
    embed.set_footer(text=f"⏱️ Aktualisiert: {now}")

    try:
        ch = bot.get_channel(DISCORD_CHANNEL_ID) or await bot.fetch_channel(DISCORD_CHANNEL_ID)  # type: ignore
        async with state_lock:
            mid = msg_state.get("discord_main")
            if mid:
                try:
                    msg = await ch.fetch_message(mid)  # type: ignore
                    await msg.edit(embed=embed)
                    return
                except Exception:
                    pass

            msg = await ch.send(embed=embed)  # type: ignore
            msg_state["discord_main"] = msg.id
            save_state(msg_state)

    except Exception as e:
        log(f"Discord Fehler: {e}", "DISCORD")


async def update_telegram_message():
    if not ENABLE_TELEGRAM or not ENABLE_TELEGRAM_IMPORT or not tg_bot or not TELEGRAM_CHAT_ID:
        return

    s = status
    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    last_refresh_display = s["last_refresh"]
    if s["mode"] == "REFRESH" and last_refresh_display.startswith("Noch kein Refresh"):
        last_refresh_display = "🔄 Läuft gerade – erster Smart-Refresh."
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
        try:
            cpu_avg, cpu_peak = s["cpu_line"].split("/ Peak:")
            text += f"\n• <b>CPU:</b> {cpu_avg}/ <b>Peak:</b>{cpu_peak}"
        except ValueError:
            text += f"\n• <b>CPU:</b> {s['cpu_line']}"
    if s["last_refresh_details"]:
        text += f"\n\n{s['last_refresh_details']}"
    if s.get("stats_block"):
        text += f"\n\n{s['stats_block']}"
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


async def update_embed():
    await update_discord_embed()
    await update_telegram_message()


# -------------------------------------------------------------
# Smart-Refresh Hauptlogik
# -------------------------------------------------------------
async def smart_refresh_loop():
    db_init()
    log(f"SQLite bereit: {DB_PATH}", "DB")

    # CPU-Sampler global starten
    asyncio.create_task(cpu_sampler())

    # Plex verbinden
    try:
        plex = PlexServer(PLEX_URL, PLEX_TOKEN, timeout=PLEX_TIMEOUT)
        plex._session.verify = False  # SSL off, falls selbstsigniert
        status["plex_name"] = plex.friendlyName
        status["status_line"] = f"✅ Verbunden mit Plex: {plex.friendlyName}"
        status["mode"] = "IDLE"
        log(f"✅ Verbunden mit Plex: {plex.friendlyName}", "REFRESH")
        await update_embed()
    except Exception as e:
        msg = f"❌ Fehler bei Verbindung zu Plex: {e}"
        log(msg, "REFRESH")
        status.update({"status_line": msg, "mode": "ERROR", "last_error": msg})
        write_health(False)
        await update_embed()
        return

    # Hauptschleife
    while True:
        # Warten bis zum nächsten geplanten Lauf
        tgt = next_target_datetime()
        status.update({
            "mode": "IDLE",
            "status_line": f"✅ Plex {status['plex_name']} – Bereit",
            "next_run": next_run_human(tgt),
        })
        await update_embed()

        now = dt.datetime.now()
        sec = max(0, (tgt - now).total_seconds())
        await asyncio.sleep(sec)

        # ---------------------------------------------
        # Startlauf
        # ---------------------------------------------
        log("=" * 80, "REFRESH")
        status.update({
            "mode": "REFRESH",
            "status_line": f"🔄 Smart-Refresh läuft auf {status['plex_name']} ...",
            "next_run": "— Lauf aktiv —",
            "last_error": "",
        })
        await update_embed()

        # Plex-Scan erkannt → kurze Pause
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
            resume_at = dt.datetime.now() + dt.timedelta(minutes=wait_mins)
            resume_str = resume_at.strftime("%H:%M:%S")

            log(f"Plex-Scan erkannt – Smart-Refresh pausiert bis {resume_str}", "REFRESH")

            status.update({
                "mode": "PAUSE",
                "status_line": f"⏸️ Plex-Scan erkannt – pausiert bis {resume_str} ({status['plex_name']})",
                "next_run": f"Pausiert bis {resume_str}",
            })
            await update_embed()
            await asyncio.sleep(wait_mins * 60)
            continue

        # Lauf-Statistiken
        refreshed_lines: List[str] = []
        errors: List[str] = []
        cpu_vals: List[float] = []
        peak = 0.0
        start_ts = dt.datetime.now()

        stats_checked = 0
        stats_fixed = 0
        stats_cooldown_skipped = 0
        stats_newly_dead = 0

        # Bibliotheken
        try:
            sections = [s for s in plex.library.sections() if s.type in ["movie", "show"]]
        except Exception as e:
            msg = f"Fehler beim Laden der Bibliotheken: {e}"
            log(msg, "REFRESH")
            status.update({"mode": "ERROR", "status_line": msg, "last_error": msg})
            write_health(False)
            await update_embed()
            continue

        total_sections = len(sections) or 1

        for idx, secobj in enumerate(sections, start=1):
            lib_name = secobj.title
            status["status_line"] = f"🔄 Smart-Refresh läuft ({idx}/{total_sections}): {lib_name}"
            await update_embed()

            # Items segmentweise laden
            try:
                loop = asyncio.get_running_loop()
                all_items = await loop.run_in_executor(None, lambda: secobj.all(sort="addedAt:asc"))

                if not all_items:
                    db_set_cursor(lib_name, 0)
                    log(f"{lib_name} – keine Items gefunden (Cursor -> 0).", "REFRESH")
                    continue

                total_size = len(all_items)
                cursor = db_get_cursor(lib_name)
                if cursor >= total_size or cursor < 0:
                    cursor = 0

                segments_total = (total_size + MAX_ITEMS_PER_SCAN - 1) // MAX_ITEMS_PER_SCAN
                segment_index = (cursor // MAX_ITEMS_PER_SCAN) + 1

                start_i, end_i = cursor, cursor + MAX_ITEMS_PER_SCAN
                selected = all_items[start_i:end_i]

                log(f"{lib_name} – Segment {segment_index}/{segments_total} → prüfe {MAX_ITEMS_PER_SCAN} Items…", "REFRESH")

                if not selected:
                    cursor = 0
                    start_i, end_i = 0, MAX_ITEMS_PER_SCAN
                    selected = all_items[start_i:end_i]
                    segment_index = 1
                    log(f"{lib_name} – Cursor Wrap → Segment 1/{segments_total} → prüfe {MAX_ITEMS_PER_SCAN} Items…", "REFRESH")

                db_set_cursor(lib_name, 0 if end_i >= total_size else end_i)

            except Exception as e:
                errors.append(f"{lib_name} – Fehler beim Laden der Items: {e}")
                continue

            fixed_count = 0

            # Items prüfen
            for itm in selected:
                stats_checked += 1
                rating_key = str(getattr(itm, "ratingKey", "")) or ""
                updated_at = getattr(itm, "updatedAt", None)
                updated_iso = updated_at.isoformat() if updated_at else ""
                row = db_get_media(rating_key) if rating_key else None

                # DEAD → Reaktivierung bei geänderter updatedAt
                if row and row["state"] == "dead" and updated_iso and updated_iso != (row["last_updated_at"] or ""):
                    db_upsert_media(rating_key, lib_name, iso_now(), updated_iso, 0, "active", None, "Reaktiviert nach Änderung")

                row = db_get_media(rating_key)

                # Cooldown/Dead Skip
                if row:
                    if row["ignore_until"]:
                        try:
                            if dt.datetime.fromisoformat(row["ignore_until"]) > dt.datetime.now() and row["state"] in ("cooldown", "dead"):
                                stats_cooldown_skipped += 1
                                continue
                        except Exception:
                            pass
                    if row["state"] == "dead":
                        stats_cooldown_skipped += 1
                        continue

                # Refresh nötig?
                try:
                    need, info = needs_refresh(itm)
                except Exception as e:
                    errors.append(f"{lib_name} – Analysefehler: {e}")
                    continue

                if not need:
                    db_upsert_media(rating_key, lib_name, iso_now(), updated_iso, 0, "active", None, None)
                    continue

                # Refresh + Validierung
                try:
                    ok = await refresh_item_and_check(plex, itm)
                except Exception as e:
                    ok = False
                    errors.append(f"{info['title']} – Refresh-Fehler: {e}")

                # CPU-Messung (processebene, nicht bei jedem Item nötig, aber hier ausreichend)
                try:
                    c = PROC.cpu_percent(interval=None)
                    cpu_vals.append(c)
                    peak = max(peak, c)
                except Exception:
                    pass

                await asyncio.sleep(0)

                if ok:
                    fixed_count += 1
                    stats_fixed += 1
                    log(f"FIXED [{lib_name}] → {info['title']} (ratingKey={rating_key})", "REFRESH")
                    db_upsert_media(rating_key, lib_name, iso_now(), updated_iso, 0, "active", None, "Gefixt")
                else:
                    log(f"FAILED [{lib_name}] → {info['title']} (ratingKey={rating_key})", "REFRESH")
                    cur_fails = int((row["fail_count"] if row else 0) or 0) + 1
                    if cur_fails >= MAX_FAILS:
                        stats_newly_dead += 1
                        db_upsert_media(
                            rating_key,
                            lib_name,
                            iso_now(),
                            updated_iso,
                            cur_fails,
                            "dead",
                            iso_in_days(3650),
                            "Dauerhaft fehlerhaft – auf DEAD gestellt",
                        )
                    else:
                        step_idx = min(cur_fails - 1, len(COOLDOWN_STEPS_DAYS) - 1)
                        cooldown_days = COOLDOWN_STEPS_DAYS[step_idx]
                        db_upsert_media(
                            rating_key,
                            lib_name,
                            iso_now(),
                            updated_iso,
                            cur_fails,
                            "cooldown",
                            iso_in_days(cooldown_days),
                            f"Fehlschlag #{cur_fails} – Cooldown {cooldown_days}d",
                        )

            log(f"{lib_name} – Segment {segment_index}/{segments_total} fertig ({fixed_count} gefixt, {stats_cooldown_skipped} übersprungen)", "REFRESH")

            if fixed_count > 0:
                refreshed_lines.append(f"• {lib_name}: {fixed_count} Eintrag{'e' if fixed_count != 1 else ''}")
                log(f"{lib_name}: {fixed_count} Eintrag{'e' if fixed_count != 1 else ''} gefixt", "REFRESH")

        # ---------------------------------------------
        # Zusammenfassung
        # ---------------------------------------------
        dur = (dt.datetime.now() - start_ts).total_seconds()

        # Globale CPU-Werte seit Laufbeginn verwenden
        global cpu_vals_global, cpu_peak_global
        if cpu_vals_global:
            avg = sum(cpu_vals_global) / len(cpu_vals_global)
            peak = cpu_peak_global
        else:
            avg = 0.0
            peak = 0.0

        if refreshed_lines:
            total = sum(int(line.split(": ")[1].split(" ")[0]) for line in refreshed_lines)
            status.update({
                "last_refresh": (
                    f"{start_ts:%d.%m.%Y %H:%M:%S} – "
                    f"{total} Einträge in {len(refreshed_lines)} Bibliotheken · "
                    f"Dauer: {format_dur(dur)}"
                ),
                "last_refresh_details": "\n".join(refreshed_lines),
                "cpu_line": f"⌀ {avg:.1f}% / Peak: {peak:.1f}%",
                "status_line": f"✅ Smart-Refresh abgeschlossen ({status['plex_name']})",
            })
        else:
            status.update({
                "last_refresh": "✅ Keine fehlerhaften Metadaten gefunden – alles aktuell.",
                "last_refresh_details": "",
                "cpu_line": f"⌀ {avg:.1f}% / Peak: {peak:.1f}%",  # <<< WICHTIG
                "status_line": f"✅ Plex {status['plex_name']} – alles aktuell",
            })
            log("Keine fehlerhaften Metadaten gefunden – alles aktuell.", "REFRESH")


        if errors:
            status["last_error"] = "Es sind Fehler aufgetreten:\n" + "\n".join(errors[:10])
            log(status["last_error"], "REFRESH")
        else:
            status["last_error"] = ""

        total_dead = db_count_dead_total()
        stats_time = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        status["stats_block"] = (
            f"📊 **Statistik dieses Laufs – Stand {stats_time}**\n"
            f"• Geprüft: {stats_checked}\n"
            f"• Gefixt: {stats_fixed}\n"
            f"• Cooldown/Dead übersprungen: {stats_cooldown_skipped}\n"
            f"• Neu tot: {stats_newly_dead}\n"
            f"• Gesamt tot: {total_dead}"
        )

        write_health(True)

        # CPU-Werte für nächsten Lauf resetten
        cpu_vals_global = []
        cpu_peak_global = 0.0

        # Aktuellen Status-Snapshot persistent in msg_ids.json ablegen,
        # damit er nach einem Container-Neustart wiederhergestellt werden kann.
        async with state_lock:
            msg_state["last_status"] = {
                "last_refresh": status["last_refresh"],
                "last_refresh_details": status["last_refresh_details"],
                "cpu_line": status["cpu_line"],
                "last_error": status["last_error"],
                "stats_block": status["stats_block"],
            }
            save_state(msg_state)

        nxt = next_target_datetime()
        status.update({
            "mode": "IDLE",
            "next_run": next_run_human(nxt),
        })
        await update_embed()

# -------------------------------------------------------------
# Runner ohne Discord-Bot
# -------------------------------------------------------------
async def _runner_without_discord():
    log("Discord deaktiviert – starte Smart-Refresher ohne Discord-Bot.", "MAIN")
    asyncio.create_task(periodic_health(update_embed))
    await smart_refresh_loop()


# -------------------------------------------------------------
# main()
# -------------------------------------------------------------
def main():
    # Health initial
    if not os.path.exists(HEALTH_FILE):
        try:
            with open(HEALTH_FILE, "w") as f:
                f.write("OK|" + dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
            status["health"] = "OK (initial)"
        except Exception as e:
            log(f"Health-Init-Fehler: {e}", "HEALTH")

    # Telegram initialisieren
    global tg_bot
    if ENABLE_TELEGRAM and ENABLE_TELEGRAM_IMPORT:
        try:
            tg_bot = TgBot(token=TELEGRAM_BOT_TOKEN)  # type: ignore
        except Exception as e:
            log(f"Telegram-Init-Fehler: {e}", "TELEGRAM")

    # Discord oder Standalone
    if ENABLE_DISCORD and ENABLE_DISCORD_IMPORT:
        intents = discord.Intents.none()  # type: ignore
        # wir brauchen nur Zugriff auf Kanäle/Nachrichten, keine Member-Events
        intents.guilds = True  # type: ignore
        intents.messages = True  # type: ignore

        global bot
        bot = commands.Bot(command_prefix="!", intents=intents)  # type: ignore

        @bot.event
        async def on_ready():
            log(f"Verbunden als {bot.user}", "MAIN")  # type: ignore
            asyncio.create_task(smart_refresh_loop())
            asyncio.create_task(periodic_health(update_embed))
            log("🟢 Smart-Refresher läuft.", "MAIN")

        try:
            bot.run(DISCORD_TOKEN)  # type: ignore
        finally:
            if tg_bot:
                try:
                    asyncio.run(tg_bot.session.close())  # type: ignore
                except Exception:
                    pass

    else:
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(_runner_without_discord())
        finally:
            if tg_bot:
                try:
                    loop.run_until_complete(tg_bot.session.close())  # type: ignore
                except Exception:
                    pass


if __name__ == "__main__":
    print("📦 Starte Plex Smart-Refresher 2.0 (modular) ...", flush=True)
    main()
