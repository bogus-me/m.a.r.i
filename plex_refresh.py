#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Plex Smart-Refresher 2.0 – MODULAR VERSION
Teil 1/5 – Core, ENV, Logging, Health, CPU, Status
"""

# ==============================================================
# IMPORTS
# ==============================================================
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

warnings.simplefilter("ignore", InsecureRequestWarning)
urllib3.disable_warnings(InsecureRequestWarning)

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

load_dotenv()

# -------------------------------------------------------------
# Forward Declarations (für Pylance / IntelliSense)
# Diese Stub-Funktionen werden weiter unten vollständig definiert,
# aber Pylance benötigt sie vorab, da deine Datei modular aufgebaut ist.
# -------------------------------------------------------------

async def periodic_health(update_embed_cb):
    """
    Forward Declaration:
    Echte Implementierung folgt später im Code.
    Diese Stub-Version wird nicht ausgeführt und dient nur Pylance,
    damit Funktionen, die periodic_health benutzen, keinen Fehler werfen.
    """
    raise NotImplementedError("Forward-declared function – reale Funktion kommt später.")

async def update_embed():
    """
    Forward Declaration:
    Echte Implementierung folgt später im Code.
    Diese Stub-Version wird nie ausgeführt, verhindert aber
    Pylance-Warnungen bei modularer Struktur in einer Datei.
    """
    raise NotImplementedError("Forward-declared function – reale Funktion kommt später.")

# ==============================================================
# ENV-HELPER
# ==============================================================
def env_required(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        print(f"[ERROR] ENV fehlt: {name}", flush=True)
        sys.exit(1)
    return v

def env_int(name: str) -> int:
    raw = env_required(name)
    try: return int(raw)
    except: 
        print(f"[ERROR] {name} muss int sein, ist aber {raw!r}")
        sys.exit(1)

def env_bool(name: str) -> bool:
    raw = env_required(name).lower()
    if raw not in ("true", "false"):
        print(f"[ERROR] {name} muss true/false sein, ist aber {raw!r}")
        sys.exit(1)
    return raw == "true"

# ==============================================================
# CONFIG
# ==============================================================
PLEX_URL = env_required("PLEX_URL")
PLEX_TOKEN = env_required("PLEX_TOKEN")

REFRESH_INTERVAL_DAYS = env_int("REFRESH_INTERVAL_DAYS")
REFRESH_TIME = env_required("REFRESH_TIME")
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

PLEX_TIMEOUT = int(os.getenv("PLEX_TIMEOUT", "30"))

# Discord ID check
if ENABLE_DISCORD:
    if not DISCORD_TOKEN.strip():
        print("[ERROR] DISCORD_TOKEN fehlt", flush=True)
        sys.exit(1)
    try:
        DISCORD_CHANNEL_ID = int(DISCORD_CHANNEL_ID_RAW)
    except:
        print(f"[ERROR] DISCORD_CHANNEL_ID muss Zahl sein: {DISCORD_CHANNEL_ID_RAW!r}")
        sys.exit(1)
else:
    DISCORD_CHANNEL_ID = 0

# Telegram ID check
if ENABLE_TELEGRAM:
    if not TELEGRAM_BOT_TOKEN.strip():
        print("[ERROR] TELEGRAM_BOT_TOKEN fehlt")
        sys.exit(1)
    try:
        TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID_RAW)
    except:
        print(f"[ERROR] TELEGRAM_CHAT_ID muss Zahl sein: {TELEGRAM_CHAT_ID_RAW!r}")
        sys.exit(1)
else:
    TELEGRAM_CHAT_ID = 0

# ==============================================================
# OPTIONAL IMPORTS (Discord, Telegram)
# ==============================================================
ENABLE_DISCORD_IMPORT = False
ENABLE_TELEGRAM_IMPORT = False

try:
    import discord  # type: ignore
    from discord.ext import commands  # type: ignore
    ENABLE_DISCORD_IMPORT = True
except:
    pass

try:
    from aiogram import Bot as TgBot  # type: ignore
    ENABLE_TELEGRAM_IMPORT = True
except:
    pass

# ==============================================================
# GLOBAL STATUS
# ==============================================================
status: Dict[str, Any] = {
    "plex_name": "—",
    "mode": "INIT",
    "status_line": "⏳ Initialisiere Smart-Refresher…",
    "next_run": "— wird berechnet —",
    "health": "Noch kein Healthcheck.",
    "last_refresh": "Noch kein Refresh.",
    "last_refresh_details": "",
    "cpu_line": "—",
    "last_error": "",
    "stats_block": "",
}

# ==============================================================
# LOGGING + EXTRA LOGS
# ==============================================================
def log(msg: str, prefix="MAIN"):
    ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    line = f"[{ts}] [{prefix}] {msg}"
    print(line, flush=True)

    try:
        lines = open(LOG_FILE).readlines() if os.path.exists(LOG_FILE) else []
        lines.insert(0, line + "\n")
        open(LOG_FILE, "w").writelines(lines[:LOG_MAX_LINES])
    except:
        pass

def log_extra(path: str, msg: str):
    ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    try:
        with open(path, "a") as f:
            f.write(f"[{ts}] {msg}\n")
    except:
        pass

# ==============================================================
# HEALTH-FILE
# ==============================================================
def write_health(ok=True):
    try:
        with open(HEALTH_FILE, "w") as f:
            f.write(("OK" if ok else "FAIL") + "|" +
                    dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
        log(f"Health gesetzt: {'OK' if ok else 'FAIL'}", "HEALTH")
    except Exception as e:
        log(f"Health-Write Fehler: {e}", "HEALTH")

# ==============================================================
# MESSAGE STATE (Discord/Telegram)
# ==============================================================
def load_state() -> Dict[str, Any]:
    if os.path.exists(MSG_STATE_FILE):
        try:
            return json.load(open(MSG_STATE_FILE))
        except:
            pass
    return {}

def save_state(d: Dict[str, Any]):
    try:
        with open(MSG_STATE_FILE, "w") as f:
            json.dump(d, f, indent=2)
    except:
        pass

msg_state = load_state()
state_lock = asyncio.Lock()

# Vorherige Statuswerte wiederherstellen
if isinstance(msg_state.get("last_status"), dict):
    for k in (
        "last_refresh",
        "last_refresh_details",
        "cpu_line",
        "last_error",
        "stats_block",
    ):
        if msg_state["last_status"].get(k):
            status[k] = msg_state["last_status"][k]

# ==============================================================
# CPU SAMPLER (GLOBAL)
# ==============================================================
PROC = psutil.Process()
cpu_vals_global: List[float] = []
cpu_peak_global: float = 0.0

async def cpu_sampler():
    global cpu_peak_global
    while True:
        try:
            v = PROC.cpu_percent(interval=None)
            cpu_vals_global.append(v)
            cpu_peak_global = max(cpu_peak_global, v)
        except:
            pass
        await asyncio.sleep(1)

# ==============================================================
# SAFE PLEX FETCH
# ==============================================================
async def safe_fetch(plex, rating_key, retries=3):
    for _ in range(retries):
        try:
            return plex.fetchItem(rating_key)
        except:
            await asyncio.sleep(0.2)
    return None

"""
Plex Smart-Refresher 2.0 – MODULAR VERSION
Teil 2/5 – DB, Zeitfunktionen, Helper
"""

# ==============================================================
#   ZEIT-HELPER
# ==============================================================

def format_dur(sec: float) -> str:
    sec = int(sec)
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    if m: parts.append(f"{m}min")
    if not parts:
        parts.append(f"{s}s")
    return " ".join(parts)

def next_target_datetime() -> dt.datetime:
    now = dt.datetime.now()
    h, m = map(int, REFRESH_TIME.split(":"))
    base = now.replace(hour=h, minute=m, second=0, microsecond=0)
    interval = max(1, REFRESH_INTERVAL_DAYS)
    while base <= now:
        base += dt.timedelta(days=interval)
    return base

def next_run_human(target: dt.datetime) -> str:
    now = dt.datetime.now()
    sec = max(0, (target - now).total_seconds())
    mins = int(sec // 60)
    h, m = divmod(mins, 60)

    if h > 0:
        t = f"in {h}h {m}min"
    else:
        t = f"in {m}min"

    return f"{t}\n{target:%d.%m.%Y %H:%M}"

def iso_now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")

def iso_in_days(days: int) -> str:
    return (dt.datetime.now() + dt.timedelta(days=days)).isoformat(timespec="seconds")

# ==============================================================
#   DB-PFAD + SCHEMA
# ==============================================================

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

# ==============================================================
#   DB-FUNKTIONEN
# ==============================================================

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
        cur = conn.execute(
            "SELECT offset FROM library_cursor WHERE library=?", (library,)
        )
        row = cur.fetchone()
        if row:
            return int(row["offset"])
        conn.execute(
            "INSERT OR IGNORE INTO library_cursor(library, offset) VALUES(?,?)",
            (library, 0),
        )
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
            INSERT INTO media_state(
                rating_key, library, last_checked, last_updated_at,
                fail_count, state, ignore_until, note
            )
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
            (
                rating_key, library, last_checked, last_updated_at,
                fail_count, state, ignore_until, note,
            ),
        )
    finally:
        conn.close()

def db_count_dead_total() -> int:
    conn = db_connect()
    try:
        cur = conn.execute(
            "SELECT COUNT(*) AS c FROM media_state WHERE state='dead'"
        )
        return int(cur.fetchone()["c"])
    finally:
        conn.close()

"""
Plex Smart-Refresher 2.0 – MODULAR VERSION
Teil 3/5 – Metadaten-Analyse, Refresh-Logik, DEAD/COOLDOWN
"""

# ==============================================================
#   METADATA-REGELN
# ==============================================================

# Wie oft darf ein Item scheitern, bevor es "dead" wird
MAX_FAILS = 5

# Staffelung der Cooldowns (in Tagen) je Fehlschlag
# 1. Fail -> 1 Tag, 2. -> 7 Tage, 3. -> 14 Tage, 4.+ -> 30 Tage
COOLDOWN_STEPS_DAYS = [1, 7, 14, 30]


def needs_refresh(item) -> Tuple[bool, Dict[str, Any]]:
    """
    Entscheidet, ob ein Item Metadaten-Refresh braucht.

    Kriterien (bewusst simpel und transparent):
    - Kein GUID
    - ODER: Poster & Summary fehlen
    - ODER: Rating fehlt UND (kein GUID ODER kein Poster ODER keine Summary)
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

    info = {
        "title": title,
        "missing_guid": missing_guid,
        "missing_thumb": missing_thumb,
        "missing_summary": missing_summary,
        "missing_rating": missing_rating,
    }

    return needs, info


async def refresh_item_and_check(plex: PlexServer, item) -> bool:
    """
    Führt einen Refresh auf dem Item aus und prüft anschließend,
    ob die Metadaten jetzt OK sind (per safe_fetch + needs_refresh).
    """
    try:
        # normalen Plex-Refresh anstoßen
        item.refresh()
    except Exception:
        return False

    # Eventloop kurz freigeben
    await asyncio.sleep(0)

    # Item frisch von Plex holen (mit Retry, um Connection-Issues zu entschärfen)
    fresh = await safe_fetch(plex, item.ratingKey)
    if fresh is None:
        return False

    still_needs, _ = needs_refresh(fresh)
    return not still_needs


# ==============================================================
#   HILFSFUNKTIONEN FÜR FEHLERFÄLLE
# ==============================================================

def build_missing_reason(info: Dict[str, Any]) -> str:
    """
    Baut einen kompakten String, welche Felder fehlen (guid, thumb, summary, rating).
    """
    parts = []
    if info.get("missing_guid"):
        parts.append("guid")
    if info.get("missing_thumb"):
        parts.append("thumb")
    if info.get("missing_summary"):
        parts.append("summary")
    if info.get("missing_rating"):
        parts.append("rating")
    return ", ".join(parts) or "unknown"


def handle_failed_item(
    lib_name: str,
    rating_key: str,
    info: Dict[str, Any],
    row: Optional[sqlite3.Row],
    updated_iso: str,
) -> Tuple[int, bool]:
    """
    Behandelt einen fehlgeschlagenen Refresh:

    - schreibt kompakte Zeile in failed.log
    - eskaliert fail_count (active -> cooldown -> dead)
    - schreibt bei DEAD zusätzlich dead.log
    - aktualisiert media_state in der SQLite DB

    Rückgabe:
        cur_fails: aktueller Fail-Counter
        became_dead: True, falls Item in diesem Schritt 'dead' geworden ist
    """
    title = info.get("title", "Unbekannt")
    reason_str = build_missing_reason(info)

    # bisherige Fehlschläge
    cur_fails = int((row["fail_count"] if row else 0) or 0) + 1

    # immer in failed.log notieren, egal ob später cooldown oder dead
    log_extra(
        "failed.log",
        f"FAILED | lib={lib_name} | key={rating_key} | title={title} | "
        f"fails={cur_fails} | missing={reason_str}",
    )

    # === DEAD? ==================================================
    if cur_fails >= MAX_FAILS:
        # ins Hauptlog
        log(
            f"[DEAD] {lib_name} | key={rating_key} | {title} | "
            f"{cur_fails}x failed | missing: {reason_str}",
            "REFRESH",
        )

        # zus. in dead.log (für manuelle TMDB-Reports etc.)
        log_extra(
            "dead.log",
            f"DEAD | lib={lib_name} | key={rating_key} | title={title} | "
            f"fails={cur_fails} | missing={reason_str}",
        )

        # hart auf DEAD setzen (10 Jahre Ruhe)
        db_upsert_media(
            rating_key=rating_key,
            library=lib_name,
            last_checked=iso_now(),
            last_updated_at=updated_iso,
            fail_count=cur_fails,
            state="dead",
            ignore_until=iso_in_days(3650),
            note="Dauerhaft fehlerhaft – auf DEAD gestellt",
        )

        return cur_fails, True

    # === COOLDOWN ================================================
    step_idx = min(cur_fails - 1, len(COOLDOWN_STEPS_DAYS) - 1)
    cooldown_days = COOLDOWN_STEPS_DAYS[step_idx]

    log(
        f"[COOLDOWN] {lib_name} | key={rating_key} | {title} | "
        f"fail#{cur_fails} | wait {cooldown_days}d | missing: {reason_str}",
        "REFRESH",
    )

    db_upsert_media(
        rating_key=rating_key,
        library=lib_name,
        last_checked=iso_now(),
        last_updated_at=updated_iso,
        fail_count=cur_fails,
        state="cooldown",
        ignore_until=iso_in_days(cooldown_days),
        note=f"Fehlschlag #{cur_fails} – Cooldown {cooldown_days}d",
    )

    return cur_fails, False

"""
Plex Smart-Refresher 2.0 – MODULAR VERSION
Teil 4/5 – Discord-Embed + Telegram-Nachricht (kompakt & modular)
"""

# ==============================================================
#   DISCORD (falls aktiviert)
# ==============================================================

async def update_discord_embed():
    if not ENABLE_DISCORD or not ENABLE_DISCORD_IMPORT or not bot or not DISCORD_CHANNEL_ID:
        return

    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    s = status

    # ---------- Formatierung der Next-Run-Zeile ----------
    nr_lines = str(s["next_run"]).splitlines()
    if len(nr_lines) >= 2:
        next_run_block = f"🕒 **Nächster Lauf:** {nr_lines[0]}\n📅 {nr_lines[1]}\n\n"
    else:
        next_run_block = f"🕒 **Nächster Lauf:** {s['next_run']}\n\n"

    # ---------- Last-Refresh ----------
    last_ref = s["last_refresh"]
    if s["mode"] == "REFRESH" and last_ref.startswith("Noch kein Refresh"):
        last_ref = "🔄 Läuft gerade – erster Smart-Refresh."

    # ---------- CPU ----------
    cpu_block = ""
    if s["cpu_line"] != "—":
        cpu_block = f"\n• **CPU:** {s['cpu_line']}"

    # ---------- Details ----------
    details = ""
    if s["last_refresh_details"]:   details += f"\n\n{s['last_refresh_details']}"
    if s.get("stats_block"):        details += f"\n\n{s['stats_block']}"
    if s["last_error"]:             details += f"\n\n❌ **Fehler:** {s['last_error']}"

    # ---------- Prefix ----------
    prefix = "" if last_ref.lstrip().startswith(("🔄", "❌", "✅")) else "• "

    # ---------- Beschreibung zusammensetzen ----------
    desc = (
        f"🟢 **Status:** {s['status_line']}\n"
        f"⚙️ **Modus:** {s['mode']}\n\n"
        f"{next_run_block}"
        f"✅ **Healthcheck:** {s['health']}\n\n"
        f"🧾 **Letzter Smart-Refresh:** {last_ref.split('·')[-1].strip()}\n"
        f"{last_ref.split('–')[-1].split('·')[0].strip()}\n"
        f"{cpu_block.replace('Peak:', '**Peak:**')}\n"
        f"{details}"
    )

    # begrenzen, falls nötig
    if len(desc) > 4000:
        desc = desc[:4000] + "\n\n… (gekürzt – siehe Log)"

    # Statusfarbe
    if s["mode"] == "ERROR":
        color = 0xFF0000
    elif s["mode"] in ("REFRESH", "PAUSE"):
        color = 0xFFA500
    else:
        color = 0x00FF00

    embed = discord.Embed(description=desc, color=color)
    embed.set_footer(text=f"⏱️ Aktualisiert: {now}")

    # ---------- Nachricht aktualisieren ODER neu senden ----------
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
                    pass  # Post später neu

            msg = await ch.send(embed=embed)  # type: ignore

            msg_state["discord_main"] = msg.id
            save_state(msg_state)

    except Exception as e:
        log(f"Discord Fehler: {e}", "DISCORD")


# ==============================================================
#   TELEGRAM (falls aktiviert)
# ==============================================================

async def update_telegram_message():
    if not ENABLE_TELEGRAM or not ENABLE_TELEGRAM_IMPORT or not tg_bot or not TELEGRAM_CHAT_ID:
        return

    s = status
    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    last_ref = s["last_refresh"]
    if s["mode"] == "REFRESH" and last_ref.startswith("Noch kein Refresh"):
        last_ref = "🔄 Läuft gerade – erster Smart-Refresh."

    prefix = "" if last_ref.startswith(("🔄", "❌", "✅")) else "• "

    # Block aufbauen
    text = (
        f"🟢 <b>Status:</b> {s['status_line']}\n"
        f"⚙️ <b>Modus:</b> {s['mode']}\n\n"
        f"🕒 <b>Nächster Lauf:</b> {s['next_run']}\n\n"
        f"✅ <b>Healthcheck:</b> {s['health']}\n\n"
        f"🧾 <b>Letzter Smart-Refresh:</b>\n"
        f"{prefix}{last_ref}"
    )

    if s["cpu_line"] != "—":
        text += f"\n• <b>CPU:</b> {s['cpu_line']}"

    if s["last_refresh_details"]:
        text += f"\n\n{s['last_refresh_details']}"

    if s.get("stats_block"):
        text += f"\n\n{s['stats_block']}"

    if s["last_error"]:
        text += f"\n\n❌ <b>Fehler:</b> {s['last_error']}"

    text += f"\n\n⏱️ <i>Aktualisiert:</i> {now}"

    # ---------- Nachricht aktualisieren ----------
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


# ==============================================================
#   SINGLE CALL FÜR BEIDE KANÄLE
# ==============================================================

async def update_embed():
    await update_discord_embed()
    await update_telegram_message()

"""
Plex Smart-Refresher 2.0 – MODULAR VERSION
Teil 5/5 – Smart-Refresh-Hauptlogik (kompakt & vollständig)
"""

async def smart_refresh_loop():
    db_init()
    log(f"SQLite bereit: {DB_PATH}", "DB")

    # CPU-Sampler global starten
    asyncio.create_task(cpu_sampler())

    # Plex verbinden
    try:
        plex = PlexServer(PLEX_URL, PLEX_TOKEN, timeout=PLEX_TIMEOUT)
        plex._session.verify = False
        status["plex_name"] = plex.friendlyName
        status["status_line"] = f"✅ Verbunden mit Plex: {plex.friendlyName}"
        status["mode"] = "IDLE"
        log(f"Verbunden mit Plex: {plex.friendlyName}", "REFRESH")
        await update_embed()
    except Exception as e:
        msg = f"❌ Fehler bei Verbindung zu Plex: {e}"
        log(msg, "REFRESH")
        status.update({"mode": "ERROR", "status_line": msg, "last_error": msg})
        write_health(False)
        await update_embed()
        return

    # ==========================================================
    # ENDLOS-SCHLEIFE → alle X Tage neu starten
    # ==========================================================
    while True:
        tgt = next_target_datetime()
        status.update({
            "mode": "IDLE",
            "status_line": f"✅ Plex {status['plex_name']} – Bereit",
            "next_run": next_run_human(tgt),
        })
        await update_embed()

        await asyncio.sleep(max(0, (tgt - dt.datetime.now()).total_seconds()))

        # ==========================================================
        # START → Neuer Smart-Refresh
        # ==========================================================
        log("=" * 80, "REFRESH")
        status.update({
            "mode": "REFRESH",
            "status_line": f"🔄 Smart-Refresh läuft ({status['plex_name']})",
            "next_run": "— Lauf aktiv —",
            "last_error": "",
        })
        await update_embed()

        # Plex-Scan aktiv?
        try:
            is_scanning = any(getattr(s, "isScanning", False) for s in plex.library.sections())
        except Exception:
            is_scanning = False

        if is_scanning:
            pause = 5
            resume = (dt.datetime.now() + dt.timedelta(minutes=pause)).strftime("%H:%M:%S")
            log(f"Plex-Scan erkannt – Pause bis {resume}", "REFRESH")
            status.update({
                "mode": "PAUSE",
                "status_line": f"⏸️ Plex-Scan erkannt – pausiert bis {resume}",
                "next_run": f"Pausiert bis {resume}",
            })
            await update_embed()
            await asyncio.sleep(pause * 60)
            continue

        # ==========================================================
        # Lauf-Variablen
        # ==========================================================
        start_ts = dt.datetime.now()
        refreshed_lines = []
        errors = []

        stats_checked = 0
        stats_fixed = 0
        stats_failed = 0
        stats_skip = 0
        stats_new_dead = 0

        # ==========================================================
        # Bibliotheken laden
        # ==========================================================
        try:
            sections = [s for s in plex.library.sections() if s.type in ("movie", "show")]
        except Exception as e:
            msg = f"Fehler beim Laden der Bibliotheken: {e}"
            log(msg, "REFRESH")
            status.update({"mode": "ERROR", "status_line": msg, "last_error": msg})
            write_health(False)
            await update_embed()
            continue

        total_sections = len(sections) or 1

        # ==========================================================
        # H A U P T - S C A N
        # ==========================================================
        for idx, secobj in enumerate(sections, start=1):
            lib = secobj.title
            status["status_line"] = f"🔄 Smart-Refresh läuft ({idx}/{total_sections}): {lib}"
            await update_embed()

            # -------- Items segmentweise laden --------
            try:
                loop = asyncio.get_running_loop()
                all_items = await loop.run_in_executor(None, lambda: secobj.all(sort="addedAt:asc"))

                if not all_items:
                    db_set_cursor(lib, 0)
                    log(f"{lib} – keine Items gefunden (Cursor -> 0)", "REFRESH")
                    continue

                total = len(all_items)
                cursor = db_get_cursor(lib)
                if cursor >= total or cursor < 0:
                    cursor = 0

                seg_total = (total + MAX_ITEMS_PER_SCAN - 1) // MAX_ITEMS_PER_SCAN
                seg_idx = (cursor // MAX_ITEMS_PER_SCAN) + 1

                start_i, end_i = cursor, cursor + MAX_ITEMS_PER_SCAN
                selected = all_items[start_i:end_i]

                log(f"{lib} – Segment {seg_idx}/{seg_total} → {MAX_ITEMS_PER_SCAN} Items", "REFRESH")

                if not selected:
                    cursor = 0
                    start_i, end_i = 0, MAX_ITEMS_PER_SCAN
                    selected = all_items[start_i:end_i]
                    seg_idx = 1
                    log(f"{lib} – Cursor-Wrap → Segment 1/{seg_total}", "REFRESH")

                db_set_cursor(lib, 0 if end_i >= total else end_i)

            except Exception as e:
                errors.append(f"{lib} – Fehler beim Laden der Items: {e}")
                continue

            fixed_count = 0

            # -------- Items prüfen --------
            for itm in selected:
                stats_checked += 1

                rating_key = str(getattr(itm, "ratingKey", "")) or ""
                updated_at = getattr(itm, "updatedAt", None)
                updated_iso = updated_at.isoformat() if updated_at else ""
                row = db_get_media(rating_key)

                # DEAD → Reaktivierung
                if row and row["state"] == "dead" and updated_iso and updated_iso != (row["last_updated_at"] or ""):
                    db_upsert_media(rating_key, lib, iso_now(), updated_iso, 0, "active", None, "Reaktiviert nach Änderung")

                row = db_get_media(rating_key)

                # SKIP wegen Cooldown/Dead
                if row:
                    if row["ignore_until"]:
                        try:
                            if dt.datetime.fromisoformat(row["ignore_until"]) > dt.datetime.now() and row["state"] in ("cooldown", "dead"):
                                stats_skip += 1
                                continue
                        except Exception:
                            pass
                    if row["state"] == "dead":
                        stats_skip += 1
                        continue

                # Refresh notwendig?
                try:
                    need, info = needs_refresh(itm)
                except Exception as e:
                    errors.append(f"{lib} – Analysefehler: {e}")
                    continue

                if not need:
                    db_upsert_media(rating_key, lib, iso_now(), updated_iso, 0, "active", None, None)
                    continue

                # Refresh durchführen
                try:
                    ok = await refresh_item_and_check(plex, itm)
                except Exception as e:
                    ok = False
                    errors.append(f"{info['title']} – Refresh Error: {e}")

                await asyncio.sleep(0)

                if ok:
                    # FIX
                    fixed_count += 1
                    stats_fixed += 1
                    log(f"FIX [{lib}] {info['title']} ({rating_key})", "REFRESH")
                    db_upsert_media(rating_key, lib, iso_now(), updated_iso, 0, "active", None, "Gefixt")

                else:
                    # FAIL
                    stats_failed += 1

                    missing = []
                    if info["missing_guid"]:     missing.append("guid")
                    if info["missing_thumb"]:    missing.append("thumb")
                    if info["missing_summary"]:  missing.append("summary")
                    if info["missing_rating"]:   missing.append("rating")
                    miss_str = ", ".join(missing) or "unknown"

                    log_extra("failed.log",
                              f"FAILED | lib={lib} | key={rating_key} | title={info['title']} | missing={miss_str}")

                    fails = int((row["fail_count"] if row else 0) or 0) + 1

                    # DEAD
                    if fails >= MAX_FAILS:
                        stats_new_dead += 1

                        log(
                            f"[DEAD] {lib} | key={rating_key} | {info['title']} | "
                            f"{fails}x failed | missing: {miss_str}",
                            "REFRESH"
                        )

                        log_extra(
                            "dead.log",
                            f"DEAD | lib={lib} | key={rating_key} | title={info['title']} "
                            f"| fails={fails} | missing={miss_str}"
                        )

                        db_upsert_media(
                            rating_key, lib, iso_now(), updated_iso,
                            fails, "dead", iso_in_days(3650),
                            "Dauerhaft fehlerhaft – auf DEAD gestellt"
                        )

                    # COOLDOWN
                    else:
                        step = COOLDOWN_STEPS_DAYS[min(fails - 1, len(COOLDOWN_STEPS_DAYS) - 1)]
                        log(
                            f"[COOLDOWN] {lib} | key={rating_key} | {info['title']} "
                            f"| fail#{fails} | wait {step}d | missing: {miss_str}",
                            "REFRESH"
                        )
                        db_upsert_media(
                            rating_key, lib, iso_now(), updated_iso,
                            fails, "cooldown", iso_in_days(step),
                            f"Fehlschlag #{fails} – Cooldown {step}d"
                        )

            # Segment abgeschlossen
            log(f"{lib} – Segment {seg_idx}/{seg_total} fertig "
                f"({fixed_count} gefixt, {stats_skip} übersprungen)", "REFRESH")

            if fixed_count > 0:
                refreshed_lines.append(f"• {lib}: {fixed_count} Einträge")

        # ==========================================================
        # Zusammenfassung
        # ==========================================================
        duration = (dt.datetime.now() - start_ts).total_seconds()

        # globale CPU-Werte
        global cpu_vals_global, cpu_peak_global
        avg_cpu = (sum(cpu_vals_global) / len(cpu_vals_global)) if cpu_vals_global else 0.0
        peak_cpu = cpu_peak_global

        # Haupt-Statuslinie (Variante A)
        if stats_failed > 0 and stats_fixed == 0:
            main_line = "⚠️ Fehler gefunden, aber keine fixbaren Metadaten."
        elif stats_fixed > 0:
            main_line = f"🔧 {stats_fixed} Metadaten korrigiert in {len(refreshed_lines)} Bibliotheken"
        else:
            main_line = "✅ Keine fehlerhaften Metadaten gefunden – alles aktuell."

        # Status im Speicher
        status.update({
            "last_refresh": (
                f"{start_ts:%d.%m.%Y %H:%M:%S} – {main_line} · "
                f"Dauer: {format_dur(duration)}"
            ),
            "last_refresh_details": "\n".join(refreshed_lines),
            "cpu_line": f"⌀ {avg_cpu:.1f}% / Peak: {peak_cpu:.1f}%",
            "status_line": f"✅ Smart-Refresh abgeschlossen ({status['plex_name']})",
        })

        # Fehler
        status["last_error"] = (
            "Es sind Fehler aufgetreten:\n" + "\n".join(errors[:10])
            if errors else ""
        )

        total_dead = db_count_dead_total()
        ts_now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        status["stats_block"] = (
            f"📊 **Statistik dieses Laufs – Stand {ts_now}**\n"
            f"• Geprüft: {stats_checked}\n"
            f"• Gefixt: {stats_fixed}\n"
            f"• Fehlgeschlagen: {stats_failed}\n"
            f"• Cooldown/Dead übersprungen: {stats_skip}\n"
            f"• Neu tot: {stats_new_dead}\n"
            f"• Gesamt tot: {total_dead}"
        )

        write_health(True)

        # CPU zurücksetzen
        cpu_vals_global = []
        cpu_peak_global = 0.0

        # Status persistieren
        async with state_lock:
            msg_state["last_status"] = {
                "last_refresh": status["last_refresh"],
                "last_refresh_details": status["last_refresh_details"],
                "cpu_line": status["cpu_line"],
                "last_error": status["last_error"],
                "stats_block": status["stats_block"],
            }
            save_state(msg_state)

        # zurück zu IDLE
        nxt = next_target_datetime()
        status.update({
            "mode": "IDLE",
            "next_run": next_run_human(nxt),
        })

        await update_embed()

# ==============================================================
# PERIODIC HEALTH (ECHTE IMPLEMENTIERUNG)
# ==============================================================

async def periodic_health(update_embed_cb):
    """
    Führt regelmäßig Health-Checks aus und aktualisiert das Embed.
    Wird unabhängig vom Smart-Refresh ausgeführt.
    """
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
                    status["health"] = (
                        f"🚨 Letzter erfolgreicher Refresh vor {delta_days} Tagen!"
                    )
                else:
                    status["health"] = f"OK (zuletzt: {ts})"

        except Exception as e:
            status["health"] = f"Health-Fehler: {e}"

        # Embed aktualisieren
        await update_embed_cb()

        # warten
        await asyncio.sleep(HEALTHCHECK_INTERVAL_MINUTES * 60)

# -------------------------------------------------------------
# Runner für den Fall OHNE Discord-Bot
# -------------------------------------------------------------
async def _runner_without_discord():
    log("Discord deaktiviert – starte Smart-Refresher ohne Bot.", "MAIN")

    # Health-Task starten
    asyncio.create_task(periodic_health(update_embed))

    # Smart-Refresh-Endlosschleife
    await smart_refresh_loop()

# -------------------------------------------------------------
# main()
# -------------------------------------------------------------
def main():
    # ---------------------------------------------------------
    # Health initialisieren (falls Datei fehlt)
    # ---------------------------------------------------------
    if not os.path.exists(HEALTH_FILE):
        try:
            with open(HEALTH_FILE, "w") as f:
                f.write("OK|" + dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
            status["health"] = "OK (initial)"
        except Exception as e:
            log(f"Health-Init-Fehler: {e}", "HEALTH")

    # ---------------------------------------------------------
    # Telegram initialisieren (falls aktiviert)
    # ---------------------------------------------------------
    global tg_bot
    if ENABLE_TELEGRAM and ENABLE_TELEGRAM_IMPORT:
        try:
            tg_bot = TgBot(token=TELEGRAM_BOT_TOKEN)  # type: ignore
            log("Telegram: Bot initialisiert.", "TELEGRAM")
        except Exception as e:
            log(f"Telegram-Initialisierungsfehler: {e}", "TELEGRAM")
            tg_bot = None

    # ---------------------------------------------------------
    # Discord aktiviert?
    # ---------------------------------------------------------
    if ENABLE_DISCORD and ENABLE_DISCORD_IMPORT:
        log("Starte Discord-Modus…", "MAIN")

        intents = discord.Intents.none()  # minimal
        intents.guilds = True
        intents.messages = True

        global bot
        bot = commands.Bot(command_prefix="!", intents=intents)  # type: ignore

        @bot.event
        async def on_ready():
            log(f"Verbunden als {bot.user}", "DISCORD")  # type: ignore

            # Tasks starten
            asyncio.create_task(smart_refresh_loop())
            asyncio.create_task(periodic_health(update_embed))

            log("🟢 Smart-Refresher läuft im Discord-Modus.", "MAIN")

        # ---- BOT STARTEN ----
        try:
            bot.run(DISCORD_TOKEN)  # blocking call
        finally:
            # Schließe Telegram Session
            if tg_bot:
                try:
                    asyncio.run(tg_bot.session.close())  # type: ignore
                except Exception:
                    pass

    # ---------------------------------------------------------
    # Discord deaktiviert → Standalone Runner
    # ---------------------------------------------------------
    else:
        log("Discord deaktiviert – Standalone-Modus aktiv.", "MAIN")
        loop = asyncio.get_event_loop()

        try:
            loop.run_until_complete(_runner_without_discord())
        finally:
            if tg_bot:
                try:
                    loop.run_until_complete(tg_bot.session.close())  # type: ignore
                except Exception:
                    pass

# -------------------------------------------------------------
# SCRIPT START
# -------------------------------------------------------------
if __name__ == "__main__":
    print("📦 Starte Plex Smart-Refresher 2.0 (modular) …", flush=True)
    main()
