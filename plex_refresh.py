#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Plex Smart-Refresher 4.2 - M.A.R.I. – TMDB-Support, Offline-safe
- Log-Ordner /logs/ mit getrennten Logfiles
- TMDB-GUID-Set mit Verify
- Fail/Cooldown/Dead-Handling
- Profiler + Warnungen
"""

import asyncio
import datetime as dt
import json
import os
import sqlite3
import sys
import time
import socket
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import requests
import psutil
from dotenv import load_dotenv
from plexapi.server import PlexServer  # type: ignore

import warnings
import urllib3
from urllib3.exceptions import InsecureRequestWarning

# Optionale Imports (Discord / Telegram)
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

warnings.simplefilter("ignore", InsecureRequestWarning)
urllib3.disable_warnings(InsecureRequestWarning)

try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

load_dotenv()

# ==============================================================

MAX_ITEMS_PER_RUN = 200
SCAN_TIME_LIMIT_SECONDS = 600
SMART_LOOKBACK_DAYS = 30
PLEX_SCAN_CHECK_INTERVAL = 10

MAX_FAILS = 5
COOLDOWN_STEPS_DAYS = [1, 7, 14, 30]

# ==============================================================

def env_required(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        print(f"[ERROR] ENV fehlt: {name}", flush=True)
        sys.exit(1)
    return v.strip()

def env_int(name: str) -> int:
    raw = env_required(name)
    try:
        return int(raw)
    except Exception:
        print(f"[ERROR] {name} muss int sein, ist aber {raw!r}")
        sys.exit(1)

def env_bool(name: str) -> bool:
    raw = env_required(name).lower()
    if raw not in ("true", "false"):
        print(f"[ERROR] {name} muss true/false sein, ist aber {raw!r}")
        sys.exit(1)
    return raw == "true"

# ==============================================================

PLEX_URL = env_required("PLEX_URL")
PLEX_TOKEN = env_required("PLEX_TOKEN")
PLEX_TIMEOUT = int(os.getenv("PLEX_TIMEOUT", "30"))

TMDB_API_KEY = env_required("TMDB_API_KEY")

REFRESH_INTERVAL_DAYS = env_int("REFRESH_INTERVAL_DAYS")
REFRESH_TIME = env_required("REFRESH_TIME")

RAW_LOG_FILE = env_required("LOG_FILE")
LOG_MAX_LINES = env_int("LOG_MAX_LINES")
HEALTH_FILE = env_required("HEALTH_FILE")
MSG_STATE_FILE = env_required("MSG_STATE_FILE")

HEALTHCHECK_MAX_DAYS = env_int("HEALTHCHECK_MAX_DAYS")
HEALTHCHECK_INTERVAL_MINUTES = env_int("HEALTHCHECK_INTERVAL_MINUTES")

# Log-Ordner /logs/ unterhalb des Pfades von LOG_FILE
LOG_BASE_DIR = os.path.dirname(RAW_LOG_FILE) or "/app"
LOG_DIR = os.path.join(LOG_BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# main.log im /logs/ Ordner
LOG_FILE = os.path.join(LOG_DIR, os.path.basename(RAW_LOG_FILE) or "main.log")

def db_path_from_log() -> str:
    base_dir = LOG_DIR
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, "refresh_state.db")

DB_PATH = os.getenv("REFRESH_DB_PATH", db_path_from_log())

ENABLE_DISCORD = env_bool("ENABLE_DISCORD_NOTIFY")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "")
DISCORD_CHANNEL_ID_RAW = os.getenv("DISCORD_CHANNEL_ID", "")

ENABLE_TELEGRAM = env_bool("ENABLE_TELEGRAM_NOTIFY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID_RAW = os.getenv("TELEGRAM_CHAT_ID", "0")

if ENABLE_DISCORD:
    if not DISCORD_TOKEN.strip():
        print("[ERROR] DISCORD_TOKEN fehlt", flush=True)
        sys.exit(1)
    try:
        DISCORD_CHANNEL_ID = int(DISCORD_CHANNEL_ID_RAW)
    except Exception:
        print(f"[ERROR] DISCORD_CHANNEL_ID muss Zahl sein: {DISCORD_CHANNEL_ID_RAW!r}")
        sys.exit(1)
else:
    DISCORD_CHANNEL_ID = 0

if ENABLE_TELEGRAM:
    if not TELEGRAM_BOT_TOKEN.strip():
        print("[ERROR] TELEGRAM_BOT_TOKEN fehlt", flush=True)
        sys.exit(1)
    try:
        TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID_RAW)
    except Exception:
        print(f"[ERROR] TELEGRAM_CHAT_ID muss Zahl sein: {TELEGRAM_CHAT_ID_RAW!r}")
        sys.exit(1)
else:
    TELEGRAM_CHAT_ID = 0

bot = None
tg_bot = None

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

TMDB_STATUS = "unknown"
TMDB_LAST_ERROR = ""
TMDB_LAST_CHECK: Optional[str] = None
TMDB_LAST_LOOKUP: Optional[str] = None
TMDB_TRIES = 0
TMDB_HITS = 0

def fmt_tmdb_dt(val: Optional[str]) -> str:
    if not val:
        return "—"
    try:
        d = dt.datetime.fromisoformat(val)
        return d.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return val

# Profiler-Daten pro Library
PROFILER: Dict[str, Dict[str, Any]] = {}

# ==============================================================

def log(msg: str, prefix: str = "MAIN"):
    ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    line = f"[{ts}] [{prefix}] {msg}"
    print(line, flush=True)
    try:
        lines = open(LOG_FILE).readlines() if os.path.exists(LOG_FILE) else []
        lines.insert(0, line + "\n")
        with open(LOG_FILE, "w") as f:
            f.writelines(lines[:LOG_MAX_LINES])
    except Exception:
        pass

def log_extra(name: str, msg: str):
    ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    # relative Namen immer in LOG_DIR schreiben
    path = name
    if not os.path.isabs(path):
        path = os.path.join(LOG_DIR, name)
    try:
        with open(path, "a") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass

# ==============================================================

def write_health(ok: bool = True):
    try:
        with open(HEALTH_FILE, "w") as f:
            f.write(("OK" if ok else "FAIL") + "|" +
                    dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
        log(f"Health gesetzt: {'OK' if ok else 'FAIL'}", "HEALTH")
    except Exception as e:
        log(f"Health-Write Fehler: {e}", "HEALTH")

# ==============================================================

def load_state() -> Dict[str, Any]:
    if os.path.exists(MSG_STATE_FILE):
        try:
            return json.load(open(MSG_STATE_FILE))
        except Exception:
            pass
    return {}

def save_state(d: Dict[str, Any]):
    try:
        with open(MSG_STATE_FILE, "w") as f:
            json.dump(d, f, indent=2)
    except Exception:
        pass

msg_state = load_state()
state_lock = asyncio.Lock()

if isinstance(msg_state.get("last_status"), dict):
    for k in ("last_refresh", "last_refresh_details", "cpu_line", "last_error", "stats_block"):
        if msg_state["last_status"].get(k):
            status[k] = msg_state["last_status"][k]

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
        except Exception:
            pass
        await asyncio.sleep(1)

# ==============================================================

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

def human_until(target: dt.datetime) -> str:
    now = dt.datetime.now()
    sec = max(0, (target - now).total_seconds())
    minutes = int(sec // 60)
    h, m = divmod(minutes, 60)
    if h > 0:
        return f"in {h}h {m}min"
    return f"in {m}min"

def next_target_datetime() -> dt.datetime:
    now = dt.datetime.now()
    h, m = map(int, REFRESH_TIME.split(":"))
    base = now.replace(hour=h, minute=m, second=0, microsecond=0)
    interval = max(1, REFRESH_INTERVAL_DAYS)
    while base <= now:
        base += dt.timedelta(days=interval)
    return base

def next_run_human(target: dt.datetime) -> str:
    return f"{human_until(target)}\n{target:%d.%m.%Y %H:%M}"

def iso_now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")

def iso_in_days(days: int) -> str:
    return (dt.datetime.now() + dt.timedelta(days=days)).isoformat(timespec="seconds")

# ==============================================================

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS media_state (
    rating_key      TEXT PRIMARY KEY,
    library         TEXT,
    first_seen      TEXT,
    last_scanned    TEXT,
    last_updated_at TEXT,
    fail_count      INTEGER DEFAULT 0,
    ignore_until    TEXT,
    state           TEXT DEFAULT 'active',
    note            TEXT
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
    updated_iso: str,
    fail_count: int,
    state: str,
    ignore_until: Optional[str],
    note: Optional[str],
):
    conn = db_connect()
    try:
        now_iso = iso_now()
        row = db_get_media(rating_key)
        first_seen = row["first_seen"] if row and row["first_seen"] else now_iso
        conn.execute(
            """
            INSERT INTO media_state(
                rating_key, library, first_seen, last_scanned,
                last_updated_at, fail_count, ignore_until, state, note
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(rating_key) DO UPDATE SET
                library=excluded.library,
                last_scanned=excluded.last_scanned,
                last_updated_at=excluded.last_updated_at,
                fail_count=excluded.fail_count,
                ignore_until=excluded.ignore_until,
                state=excluded.state,
                note=excluded.note
            """,
            (
                rating_key,
                library,
                first_seen,
                now_iso,
                updated_iso,
                fail_count,
                ignore_until,
                state,
                note,
            ),
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

# ==============================================================

# ---------------------------------------------------------
# Unicode-Fixes + Titel-Normalisierung (4.2 patched)
# ---------------------------------------------------------

# Unsichtbare bidi/control characters
BIDI_CHARS = {
    "\u200e", "\u200f", "\u202a", "\u202b", "\u202c",
    "\u202d", "\u202e", "\ufeff", "\u2066", "\u2067",
    "\u2068", "\u2069"
}

# Manche Plex-Items enthalten Debug/Marker-Text wie "[U+200E]"
BIDI_MARKERS = [
    "[U+200E]", "[U+200F]", "[U+202A]", "[U+202B]",
    "[U+202C]", "[U+202D]", "[U+202E]"
]

def clean_bidi(s: str) -> str:
    """Entfernt sowohl echte unsichtbare Steuerzeichen als auch Plex-Debug-Marker."""
    if not isinstance(s, str):
        return s

    # Echte Steuerzeichen entfernen
    for ch in BIDI_CHARS:
        s = s.replace(ch, "")

    # Debug-Marker entfernen
    for m in BIDI_MARKERS:
        s = s.replace(m, "")

    return s.strip()

def normalize_title(s: str) -> str:
    """Aggressiver Titel-Normalizer für TMDB-Suche."""
    if not s:
        return ""

    # Unicode / Bidi Fixes
    s = clean_bidi(s)

    # Unicode Normalisierung (NFKD, diakritische Zeichen entfernen)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))

    s = s.lower()

    # Jahreszahlen entfernen (grob)
    for y in range(1900, 2101):
        s = s.replace(str(y), " ")

    # Sonderzeichen entfernen/reduzieren
    repl = {
        "(": " ", ")": " ", "[": " ", "]": " ",
        "{": " ", "}": " ", "_": " ", "-": " ",
        ".": " ", ":": " ", ",": " ",
    }
    for a, b in repl.items():
        s = s.replace(a, b)

    # Mehrfache Leerzeichen reduzieren
    while "  " in s:
        s = s.replace("  ", " ")

    return s.strip()

def ratio(a: str, b: str) -> float:
    """Sehr einfacher Match-Ratio, ausreichend für Vorfilterung."""
    a, b = a.lower(), b.lower()
    if not a or not b:
        return 0.0
    total = max(len(a), len(b))
    matches = sum(1 for i in range(min(len(a), len(b))) if a[i] == b[i])
    return matches / total

def smart_fuzzy(a: str, b: str) -> float:
    """Fuzzy-Matching, nutzt normalize_title + einfache Heuristiken."""
    if not a or not b:
        return 0.0

    na, nb = normalize_title(a), normalize_title(b)
    if not na or not nb:
        return 0.0

    base = ratio(na, nb)

    # enthält der eine String den anderen → vermutlich ein Treffer
    if na in nb or nb in na:
        base = max(base, 0.90)

    return base

# ==============================================================

TMDB_MOVIE_SEARCH = "https://api.themoviedb.org/3/search/movie"
TMDB_TV_SEARCH = "https://api.themoviedb.org/3/search/tv"
TMDB_FIND_EXTERNAL = "https://api.themoviedb.org/3/find/{ext_id}"

def tmdb_request(url: str, params: Dict[str, Any]) -> Optional[dict]:
    params["api_key"] = TMDB_API_KEY
    try:
        r = requests.get(url, params=params, timeout=10, verify=False)
        if r.status_code != 200:
            log(f"TMDB HTTP {r.status_code}: {url}", "TMDB")
            return None
        return r.json()
    except Exception as e:
        log(f"TMDB Request Fehler: {e}", "TMDB")
        return None

def tmdb_check_connection():
    global TMDB_STATUS, TMDB_LAST_ERROR, TMDB_LAST_CHECK
    TMDB_LAST_CHECK = dt.datetime.now().isoformat(timespec="seconds")
    url = "https://api.themoviedb.org/3/configuration"
    params = {"api_key": TMDB_API_KEY}
    try:
        r = requests.get(url, params=params, timeout=8, verify=False)
        if r.status_code == 200:
            TMDB_STATUS = "ok"
            TMDB_LAST_ERROR = ""
            log("[TMDB] Verbunden – API-Key gültig", "TMDB")
            return True
        TMDB_STATUS = "error"
        TMDB_LAST_ERROR = f"HTTP {r.status_code}"
        log(f"[TMDB] Fehler: HTTP {r.status_code}", "TMDB")
        return False
    except Exception as e:
        TMDB_STATUS = "error"
        TMDB_LAST_ERROR = str(e)
        log(f"[TMDB] Fehler: {e}", "TMDB")
        return False

def tmdb_search_movie(title: str, year: Optional[int]) -> Optional[dict]:
    params: Dict[str, Any] = {"query": title}
    if year:
        params["year"] = year
    return tmdb_request(TMDB_MOVIE_SEARCH, params)

def tmdb_search_tv(title: str, year: Optional[int]) -> Optional[dict]:
    params: Dict[str, Any] = {"query": title}
    if year:
        params["first_air_date_year"] = year
    return tmdb_request(TMDB_TV_SEARCH, params)

def tmdb_find_by_external(ext_id: str, source: str) -> Optional[dict]:
    if source == "tvdb":
        src = "tvdb_id"
    elif source == "imdb":
        src = "imdb_id"
    else:
        return None
    url = TMDB_FIND_EXTERNAL.format(ext_id=ext_id)
    return tmdb_request(url, {"external_source": src})

def extract_year(item) -> Optional[int]:
    try:
        y = getattr(item, "year", None)
        if isinstance(y, int):
            return y
    except Exception:
        pass
    return None

def try_external_lookup(item) -> Optional[int]:
    for g in getattr(item, "guids", []):
        gid = getattr(g, "id", "") or ""
        gl = gid.lower()
        if "tvdb" in gl:
            ext = gid.split("/")[-1]
            j = tmdb_find_by_external(ext, "tvdb")
            if j and j.get("tv_results"):
                return j["tv_results"][0]["id"]
        if "imdb" in gl:
            ext = gid.split("/")[-1]
            j = tmdb_find_by_external(ext, "imdb")
            if j:
                if j.get("movie_results"):
                    return j["movie_results"][0]["id"]
                if j.get("tv_results"):
                    return j["tv_results"][0]["id"]
    return None

def try_search_movie(item) -> Optional[int]:
    title = getattr(item, "title", "") or ""
    global TMDB_TRIES, TMDB_LAST_LOOKUP, TMDB_HITS
    TMDB_TRIES += 1
    TMDB_LAST_LOOKUP = dt.datetime.now().isoformat(timespec="seconds")
    year = extract_year(item)
    log(f"[TMDB] Suche Film: {title} ({year})", "TMDB")
    j = tmdb_search_movie(title, year)
    if not j:
        return None
    best_id = None
    best_score = 0.0
    for r in j.get("results", []):
        score = smart_fuzzy(title, r.get("title", "") or "")
        if year:
            try:
                r_year = int((r.get("release_date", "") or "0")[:4])
                if abs(r_year - year) > 1:
                    continue
            except Exception:
                pass
        if score >= 0.85 and score > best_score:
            best_score = score
            best_id = r["id"]
    if best_id is not None:
        log(f"[TMDB] Treffer: {title} → ID {best_id} (score={best_score:.2f})", "TMDB")
        TMDB_HITS += 1
        log_extra(
            "tmdb_hits.log",
            f"HIT | type=movie | title={clean_bidi(title)} | id={best_id} | score={best_score:.2f} | year={year}",
        )
        return best_id
    log(f"[TMDB] Kein Treffer: {title}", "TMDB")
    log_extra(
        "tmdb_hits.log",
        f"MISS | type=movie | title={clean_bidi(title)} | year={year}",
    )
    return None

def try_search_show(item) -> Optional[int]:
    title = getattr(item, "title", "") or ""
    global TMDB_TRIES, TMDB_LAST_LOOKUP, TMDB_HITS
    TMDB_TRIES += 1
    TMDB_LAST_LOOKUP = dt.datetime.now().isoformat(timespec="seconds")
    year = extract_year(item)
    log(f"[TMDB] Suche Serie: {title} ({year})", "TMDB")
    j = tmdb_search_tv(title, year)
    if not j:
        return None
    best_id = None
    best_score = 0.0
    for r in j.get("results", []):
        score = smart_fuzzy(title, r.get("name", "") or "")
        if year:
            try:
                r_year = int((r.get("first_air_date", "") or "0")[:4])
                if abs(r_year - year) > 1:
                    continue
            except Exception:
                pass
        if score >= 0.85 and score > best_score:
            best_score = score
            best_id = r["id"]
    if best_id is not None:
        log(f"[TMDB] Treffer: {title} → ID {best_id} (score={best_score:.2f})", "TMDB")
        TMDB_HITS += 1
        log_extra(
            "tmdb_hits.log",
            f"HIT | type=tv | title={clean_bidi(title)} | id={best_id} | score={best_score:.2f} | year={year}",
        )
        return best_id
    log(f"[TMDB] Kein Treffer: {title}", "TMDB")
    log_extra(
        "tmdb_hits.log",
        f"MISS | type=tv | title={clean_bidi(title)} | year={year}",
    )
    return None

def tmdb_find_guid_for_item(item) -> Optional[int]:
    tmdb_id = try_external_lookup(item)
    if tmdb_id:
        return tmdb_id
    if getattr(item, "type", "") == "movie":
        return try_search_movie(item)
    if getattr(item, "type", "") == "show":
        return try_search_show(item)
    return None

def set_guid(item, tmdb_id: int) -> bool:
    title = clean_bidi(getattr(item, "title", "???"))
    tag = f"tmdb://{tmdb_id}"

    if not hasattr(item, "editGuid") and not hasattr(item, "addGuid"):
        log(f"GUID NICHT gesetzt (kein addGuid/editGuid verfügbar): {title}", "GUID")
        return False

    try:
        if hasattr(item, "editGuid"):
            item.editGuid([tag])
        else:
            item.addGuid(tag)
    except Exception as e:
        log(f"GUID-Setzfehler: {title}: {e}", "GUID")
        return False

    try:
        fresh = item._server.fetchItem(item.ratingKey)
        guids = [getattr(g, "id", "") for g in getattr(fresh, "guids", [])]
        if tag in guids:
            log(f"GUID gesetzt: {title} -> {tag}", "GUID")
            return True
        else:
            log(f"GUID NICHT gesetzt (Plex hat GUID verworfen): {title}", "GUID")
            return False
    except Exception as e:
        log(f"GUID-Verify-Fehler: {title}: {e}", "GUID")
        return False

# ==============================================================

def is_plex_reachable(url: str) -> bool:
    try:
        h = url.split("//", 1)[-1].split("/")[0].split(":")[0]
        socket.gethostbyname(h)
        r = requests.get(url + "/identity", timeout=3, verify=False)
        return r.status_code == 200
    except Exception:
        return False

async def wait_until_plex_online(url: str):
    tries = 0
    while True:
        if is_plex_reachable(url):
            return
        tries += 1
        if tries == 3:
            log("Plex offline – warte 2 Minuten (Boot-Window)…", "REFRESH")
            status["mode"] = "PAUSE"
            status["status_line"] = "⏸️ Plex offline – warte (Boot-Fenster)."
            await update_embed()
            await asyncio.sleep(120)
            tries = 0
            continue
        log("Plex offline – retry in 5s…", "REFRESH")
        status["mode"] = "PAUSE"
        status["status_line"] = "⏸️ Plex offline – Retry…"
        await update_embed()
        await asyncio.sleep(5)

# ==============================================================

def plex_is_scanning_sync(plex: PlexServer) -> bool:
    try:
        for s in plex.library.sections():
            if getattr(s, "isScanning", False):
                return True
    except Exception:
        return False
    return False

async def plex_is_scanning_async(plex: PlexServer) -> bool:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, plex_is_scanning_sync, plex)

async def safe_fetch(plex: PlexServer, rating_key: str, retries: int = 3):
    loop = asyncio.get_running_loop()
    def _fetch():
        try:
            return plex.fetchItem(rating_key)
        except Exception:
            return None
    for _ in range(retries):
        item = await loop.run_in_executor(None, _fetch)
        if item is not None:
            return item
        await asyncio.sleep(0.2)
    return None

async def wait_until_plex_ready(plex: PlexServer):
    while True:
        try:
            if not await plex_is_scanning_async(plex):
                return
            log("Plex-Scan erkannt – warte, bis Plex fertig ist …", "REFRESH")
            status["mode"] = "PAUSE"
            status["status_line"] = "⏸️ Plex-Scan erkannt – Refresher pausiert."
            await update_embed()
        except Exception as e:
            log(f"Fehler beim Prüfen auf Plex-Scan: {e}", "REFRESH")
            return
        await asyncio.sleep(PLEX_SCAN_CHECK_INTERVAL)

async def plex_connect_async() -> PlexServer:
    log("[INIT] Verbinde mit Plex…", "REFRESH")
    await wait_until_plex_online(PLEX_URL)
    plex = PlexServer(PLEX_URL, PLEX_TOKEN, timeout=PLEX_TIMEOUT)
    plex._session.verify = False
    try:
        log("[INIT] Lade Plex-Bibliotheken…", "REFRESH")
        sections = plex.library.sections()
        relevant = [s for s in sections if s.type in ("movie", "show")]
        log(f"[INIT] Bibliotheken gefunden: {len(relevant)} relevante Sektionen", "REFRESH")
        for s in relevant:
            try:
                agent = getattr(s, "agent", "unbekannt")
                try:
                    item_count = s.totalSize
                except Exception:
                    item_count = None
                dtype = "Movie" if s.type == "movie" else "Show"
                log(
                    f"[INIT] Sektion: '{s.title}' ({dtype}) – {item_count or '??'} Items – Agent: {agent}",
                    "REFRESH",
                )
            except Exception as e:
                log(f"[INIT-WARN] Fehler beim Analysieren der Sektion '{s.title}': {e}", "REFRESH")
    except Exception as e:
        log(f"[INIT-ERROR] Fehler beim Laden der Bibliotheken: {e}", "REFRESH")
    return plex

# ==============================================================

def needs_refresh(item) -> Tuple[bool, Dict[str, Any]]:
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
    loop = asyncio.get_running_loop()
    def _refresh():
        try:
            item.refresh()
        except Exception:
            pass
    try:
        await loop.run_in_executor(None, _refresh)
    except Exception:
        return False
    def _fetch():
        try:
            return plex.fetchItem(item.ratingKey)
        except Exception:
            return None
    fresh = await loop.run_in_executor(None, _fetch)
    if fresh is None:
        return False
    still_needs, _ = needs_refresh(fresh)
    return not still_needs

def build_missing_reason(info: Dict[str, Any]) -> str:
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
    title = clean_bidi(info.get("title", "Unbekannt"))
    reason_str = build_missing_reason(info)
    cur_fails = int((row["fail_count"] if row else 0) or 0) + 1
    log_extra(
        "failed.log",
        f"FAILED | lib={lib_name} | key={rating_key} | title={title} | "
        f"fails={cur_fails} | missing={reason_str}",
    )
    if cur_fails >= MAX_FAILS:
        log(
            f"[DEAD] {lib_name} | key={rating_key} | {title} | "
            f"{cur_fails}x failed | missing: {reason_str}",
            "REFRESH",
        )
        log_extra(
            "dead.log",
            f"DEAD | lib={lib_name} | key={rating_key} | title={title} | "
            f"fails={cur_fails} | missing={reason_str}",
        )
        db_upsert_media(
            rating_key=rating_key,
            library=lib_name,
            updated_iso=updated_iso,
            fail_count=cur_fails,
            state="dead",
            ignore_until=iso_in_days(3650),
            note="Dauerhaft fehlerhaft – auf DEAD gestellt",
        )
        return cur_fails, True
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
        updated_iso=updated_iso,
        fail_count=cur_fails,
        state="cooldown",
        ignore_until=iso_in_days(cooldown_days),
        note=f"Fehlschlag #{cur_fails} – Cooldown {cooldown_days}d",
    )
    return cur_fails, False

# ==============================================================

DISCORD_UPDATE_INTERVAL = 3.0
_last_discord_update = 0.0
_pending_discord = False
discord_send_lock = asyncio.Lock()
_last_payload = ""

async def update_discord_embed():
    global _pending_discord
    _pending_discord = True
    await _discord_maybe_send()

def _build_payload() -> str:
    s = status
    nr = str(s["next_run"]).splitlines()
    line = nr[0] if nr and nr[0].strip() else "— wird berechnet —"
    date = nr[1] if len(nr) >= 2 else "—"
    return (
        f"{s['status_line']}|{s['mode']}|{line}|{date}|{s['last_refresh']}|"
        f"{s['health']}|{s['last_refresh_details']}|{s.get('stats_block')}|"
        f"{s['last_error']}|{s['cpu_line']}"
    )

async def _discord_maybe_send():
    global _pending_discord, _last_discord_update, _last_payload
    async with discord_send_lock:
        if not _pending_discord:
            return
        now = time.time()
        if now - _last_discord_update < DISCORD_UPDATE_INTERVAL:
            return
        _pending_discord = False
        _last_discord_update = now
        payload = _build_payload()
        if payload != _last_payload:
            _last_payload = payload
            await _discord_send_core()

async def _discord_send_core():
    try:
        await _discord_embed_raw()
    except Exception as e:
        try:
            if ENABLE_DISCORD_IMPORT and isinstance(e, discord.HTTPException) and getattr(e, "status", None) == 429:
                d = float(getattr(e, "retry_after", 3))
                log(f"[DISCORD] 429 – warte {d:.2f}s", "DISCORD")
                await asyncio.sleep(d)
                await _discord_embed_raw()
                return
        except Exception:
            pass
        log(f"Discord Fehler: {e}", "DISCORD")

async def _discord_embed_raw():
    if not ENABLE_DISCORD or not ENABLE_DISCORD_IMPORT or not DISCORD_CHANNEL_ID:
        return
    global bot
    if bot is None:
        return

    s = status
    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    nr = str(s["next_run"]).splitlines()
    next_line = nr[0].strip() if nr and nr[0].strip() else "— wird berechnet —"
    next_date = nr[1] if len(nr) >= 2 else "—"

    last_ref = s["last_refresh"]

    tmdb_state = "🟢 OK" if TMDB_STATUS == "ok" else f"🔴 Fehler – {TMDB_LAST_ERROR or 'unbekannt'}"
    tmdb_rate = f"{(TMDB_HITS / TMDB_TRIES * 100):.0f}%" if TMDB_TRIES > 0 else "—"

    last_lookup = fmt_tmdb_dt(TMDB_LAST_LOOKUP)
    last_check = fmt_tmdb_dt(TMDB_LAST_CHECK)

    tmdb_block = (
        f"**TMDB**\n"
        f"• Status: {tmdb_state}\n"
        f"• Trefferquote: {tmdb_rate}\n"
        f"• Letzter Lookup: {last_lookup}\n"
        f"• Letzter Check: {last_check}\n\n"
    )

    desc = (
        f"**SYSTEMSTATUS**\n"
        f"• Status: {s['status_line']}\n"
        f"• Modus: {s['mode']}\n\n"
        f"**PLANUNG**\n"
        f"• Nächster Lauf: {next_line}\n"
        f"• Datum: {next_date}\n\n"
        f"**LETZTER LAUF**\n"
        f"• {last_ref}\n\n"
        f"**HEALTH**\n"
        f"• {s['health']}\n\n"
    )

    desc += tmdb_block
    desc += (
        f"**STATS**\n"
        f"{s.get('stats_block') or 'Noch keine Stats.'}"
        + (f"\n• Fehler: {s['last_error']}" if s["last_error"] else "")
    )

    if len(desc) > 4000:
        desc = desc[:4000] + "\n… (gekürzt)"

    color = (
        0xFF0000 if s["mode"] == "ERROR"
        else 0xFFA500 if s["mode"] in ("REFRESH", "PAUSE")
        else 0x00FF00
    )

    embed = discord.Embed(description=desc, color=color)
    embed.set_footer(text=f"Aktualisiert: {now}")

    try:
        ch = bot.get_channel(DISCORD_CHANNEL_ID) or await bot.fetch_channel(DISCORD_CHANNEL_ID)
        async with state_lock:
            mid = msg_state.get("discord_main")
            if mid:
                try:
                    msg = await ch.fetch_message(mid)
                    await msg.edit(embed=embed)
                    return
                except Exception:
                    pass
            msg = await ch.send(embed=embed)
            msg_state["discord_main"] = msg.id
            save_state(msg_state)
    except Exception as e:
        log(f"Discord Fehler: {e}", "DISCORD")

async def update_telegram_message():
    if not ENABLE_TELEGRAM or not ENABLE_TELEGRAM_IMPORT or not TELEGRAM_CHAT_ID:
        return

    global tg_bot
    if tg_bot is None:
        return

    s = status
    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    last_ref = s["last_refresh"]
    prefix = "" if last_ref.startswith(("🔄", "❌", "✅")) else "• "

    text = (
        f"🟢 <b>Status:</b> {s['status_line']}\n"
        f"⚙️ <b>Modus:</b> {s['mode']}\n\n"
        f"🕒 <b>Nächster Lauf:</b> {s['next_run']}\n\n"
        f"📅 <b>Letzter Lauf:</b>\n"
        f"{prefix}{last_ref}"
    )

    tmdb_state = "🟢 OK" if TMDB_STATUS == "ok" else f"🔴 Fehler – {TMDB_LAST_ERROR or 'unbekannt'}"
    tmdb_rate = f"{(TMDB_HITS / TMDB_TRIES * 100):.0f}%" if TMDB_TRIES > 0 else "—"

    last_lookup = fmt_tmdb_dt(TMDB_LAST_LOOKUP)
    last_check = fmt_tmdb_dt(TMDB_LAST_CHECK)

    text += (
        f"\n\n🎬 <b>TMDB</b>\n"
        f"• Status: {tmdb_state}\n"
        f"• Trefferquote: {tmdb_rate}\n"
        f"• Letzter Lookup: {last_lookup}\n"
        f"• Letzter Check: {last_check}"
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
            parse_mode="HTML"
        )
        msg_state["telegram_main"] = sent.message_id
        save_state(msg_state)

async def update_embed():
    await update_discord_embed()
    await update_telegram_message()

# ==============================================================

async def smart_refresh_loop():
    global cpu_vals_global, cpu_peak_global, PROFILER
    db_init()
    log(f"SQLite bereit: {DB_PATH}", "DB")
    asyncio.create_task(cpu_sampler())
    try:
        plex = await plex_connect_async()
        status["plex_name"] = plex.friendlyName
        status["status_line"] = f"✅ Verbunden mit Plex: {plex.friendlyName}"
        tmdb_check_connection()
        status["mode"] = "IDLE"
        log(f"Verbunden mit Plex: {plex.friendlyName}", "REFRESH")
        tgt = next_target_datetime()
        status["next_run"] = next_run_human(tgt)
        try:
            if os.path.exists(HEALTH_FILE):
                raw = open(HEALTH_FILE).read().strip()
                st, ts = raw.split("|")
                last_dt = dt.datetime.strptime(ts, "%d.%m.%Y %H:%M:%S")
                delta = (dt.datetime.now() - last_dt).days
                if st == "FAIL" or delta > HEALTHCHECK_MAX_DAYS:
                    status["health"] = f"🚨 Letzter erfolgreicher Refresh vor {delta} Tagen!"
                else:
                    status["health"] = f"OK (zuletzt: {ts})"
            else:
                status["health"] = "Keine Health-Datei vorhanden."
        except Exception as e:
            status["health"] = f"Health-Fehler: {e}"
        status["last_refresh_details"] = ""
        await update_embed()
    except Exception as e:
        msg = f"❌ Fehler bei Verbindung zu Plex: {e}"
        log(msg, "REFRESH")
        status.update({"mode": "ERROR", "status_line": msg, "last_error": msg})
        write_health(False)
        await update_embed()
        return

    while True:
        tgt = next_target_datetime()
        status.update(
            {
                "mode": "IDLE",
                "status_line": f"✅ Plex {status['plex_name']} – Bereit",
                "next_run": next_run_human(tgt),
            }
        )
        log(f"Nächster Lauf: {human_until(tgt)} ({tgt:%d.%m.%Y %H:%M})", "SCHED")
        await update_embed()
        await asyncio.sleep(max(0, (tgt - dt.datetime.now()).total_seconds()))

        # Scan-Start-Block
        start_ts = dt.datetime.now()
        log("=" * 80, "REFRESH")
        log(f"SCAN START {start_ts:%d.%m.%Y %H:%M:%S}", "REFRESH")
        log_extra("profiler.log", f"SCAN START | {start_ts:%d.%m.%Y %H:%M:%S}")

        status.update(
            {
                "mode": "REFRESH",
                "status_line": f"🔄 Smart-Refresh läuft ({status['plex_name']})",
                "next_run": "— Lauf aktiv —",
                "last_error": "",
            }
        )
        await update_embed()

        # Logs für diesen Lauf leeren
        for name in ("failed.log", "dead.log", "recovered.log"):
            try:
                path = os.path.join(LOG_DIR, name)
                open(path, "w").close()
            except Exception:
                pass

        start_ts = dt.datetime.now()
        time_limit = dt.timedelta(seconds=SCAN_TIME_LIMIT_SECONDS)
        time_exceeded = False
        refreshed_lines: List[str] = []
        errors: List[str] = []
        stats_checked = stats_fixed = stats_failed = stats_skip = stats_new_dead = 0

        PROFILER.clear()

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

        for idx, secobj in enumerate(sections, start=1):
            if dt.datetime.now() - start_ts >= time_limit:
                log("Zeitlimit erreicht – Lauf beendet.", "REFRESH")
                time_exceeded = True
                break

            lib = secobj.title
            status["status_line"] = f"🔄 Smart-Refresh läuft ({idx}/{total_sections}): {lib}"
            await update_embed()
            await wait_until_plex_ready(plex)

            lib_start_ts = dt.datetime.now()

            try:
                loop = asyncio.get_running_loop()
                all_items = await loop.run_in_executor(
                    None, lambda: secobj.all(sort="updatedAt:desc")
                )
                if not all_items:
                    log(f"{lib} – Plex nicht bereit, warte 30s…", "REFRESH")
                    status["mode"] = "PAUSE"
                    status["status_line"] = "⏸️ Plex startet…"
                    await update_embed()
                    await asyncio.sleep(30)
                    continue
            except Exception:
                log(f"{lib} – Plex nicht erreichbar, warte…", "REFRESH")
                status["mode"] = "PAUSE"
                status["status_line"] = "⏸️ Plex offline – retry…"
                await update_embed()
                await asyncio.sleep(10)
                continue

            now_dt = dt.datetime.now()
            lookback_dt = now_dt - dt.timedelta(days=SMART_LOOKBACK_DAYS)
            ready_list, new_list, changed_list = [], [], []

            for itm in all_items:
                if len(ready_list) + len(new_list) + len(changed_list) >= MAX_ITEMS_PER_RUN:
                    break
                rating_key = str(getattr(itm, "ratingKey", "")) or ""
                updated_at = getattr(itm, "updatedAt", None)
                updated_iso = updated_at.isoformat() if updated_at else ""
                row = db_get_media(rating_key)

                if row and row["ignore_until"]:
                    try:
                        ignore_dt = dt.datetime.fromisoformat(row["ignore_until"])
                        if ignore_dt > now_dt and row["state"] in ("cooldown", "dead"):
                            stats_skip += 1
                            continue
                    except Exception:
                        pass

                is_new = row is None
                is_changed = (
                    updated_iso
                    and row
                    and updated_iso != (row["last_updated_at"] or "")
                    and updated_at
                    and updated_at >= lookback_dt
                )
                ready_problem = (
                    row
                    and row["state"] in ("cooldown", "dead")
                    and (
                        not row["ignore_until"]
                        or dt.datetime.fromisoformat(row["ignore_until"]) <= now_dt
                    )
                )
                if ready_problem:
                    ready_list.append(itm)
                elif is_new:
                    new_list.append(itm)
                elif is_changed:
                    changed_list.append(itm)

            selected = (ready_list + new_list + changed_list)[:MAX_ITEMS_PER_RUN]
            if not selected:
                log(f"{lib} – keine relevanten Items.", "REFRESH")
                PROFILER[lib] = {
                    "duration": (dt.datetime.now() - lib_start_ts).total_seconds(),
                    "checked": 0,
                    "fixed": 0,
                    "failed": 0,
                    "skipped": 0,
                }
                continue

            log(f"{lib} – Smart-Auswahl: {len(selected)} Items", "REFRESH")
            fixed_count = 0
            checked_lib = 0
            failed_lib = 0
            skip_lib = 0

            for itm in selected:
                if dt.datetime.now() - start_ts >= time_limit:
                    log("Zeitlimit erreicht – Library abgebrochen.", "REFRESH")
                    time_exceeded = True
                    break

                if await plex_is_scanning_async(plex):
                    log(f"{lib} – Plex scannt, Pause.", "REFRESH")
                    await wait_until_plex_ready(plex)

                stats_checked += 1
                checked_lib += 1
                rating_key = str(getattr(itm, "ratingKey", "")) or ""
                updated_at = getattr(itm, "updatedAt", None)
                updated_iso = updated_at.isoformat() if updated_at else ""
                row = db_get_media(rating_key)

                if (
                    row
                    and row["state"] == "dead"
                    and updated_iso
                    and updated_iso != (row["last_updated_at"] or "")
                ):
                    log_extra(
                        "recovered.log",
                        f"RECOVERED | lib={lib} | key={rating_key} | title={itm.title}",
                    )
                    db_upsert_media(
                        rating_key,
                        lib,
                        updated_iso,
                        0,
                        "active",
                        None,
                        "Reaktiviert nach Änderung",
                    )
                    row = db_get_media(rating_key)

                try:
                    need, info = needs_refresh(itm)
                except Exception:
                    log(f"{lib} – Plex offline während Analyse → Pause", "REFRESH")
                    status["mode"] = "PAUSE"
                    status["status_line"] = "⏸️ Plex offline – warte…"
                    await update_embed()
                    await asyncio.sleep(10)
                    continue

                if not need:
                    if row and row["state"] in ("cooldown", "dead") and row["fail_count"] > 0:
                        log_extra(
                            "recovered.log",
                            f"RECOVERED | lib={lib} | key={rating_key} | title={itm.title}",
                        )
                    db_upsert_media(
                        rating_key, lib, updated_iso, 0, "active", None, None
                    )
                    continue

                try:
                    ok = await refresh_item_and_check(plex, itm)
                except Exception:
                    log(f"{lib} – Plex offline während Refresh → Pause", "REFRESH")
                    status["mode"] = "PAUSE"
                    status["status_line"] = "⏸️ Plex offline – warte…"
                    await update_embed()
                    await asyncio.sleep(10)
                    continue

                await asyncio.sleep(0)

                if ok:
                    fixed_count += 1
                    stats_fixed += 1
                    log(f"FIX [{lib}] {clean_bidi(info['title'])} ({rating_key})", "REFRESH")
                    db_upsert_media(
                        rating_key, lib, updated_iso, 0, "active", None, "Gefixt"
                    )
                    continue

                tmdb_id = tmdb_find_guid_for_item(itm)
                if tmdb_id:
                    success = False
                    try:
                        success = set_guid(itm, tmdb_id)
                    except Exception:
                        log(f"{lib} – Plex offline bei TMDB-GUID → Pause", "REFRESH")
                        status["mode"] = "PAUSE"
                        status["status_line"] = "⏸️ Plex offline – warte…"
                        await update_embed()
                        await asyncio.sleep(10)
                        continue

                    if success:
                        fixed_count += 1
                        stats_fixed += 1
                        log(
                            f"[TMDB] GUID erfolgreich gesetzt [{lib}] {clean_bidi(info['title'])} -> tmdb://{tmdb_id}",
                            "TMDB",
                        )
                        db_upsert_media(
                            rating_key,
                            lib,
                            updated_iso,
                            0,
                            "active",
                            None,
                            f"guid-set:{tmdb_id}",
                        )
                        continue
                    else:
                        log(
                            f"[TMDB] GUID NICHT gesetzt [{lib}] {clean_bidi(info['title'])} (Plex hat nicht übernommen)",
                            "TMDB",
                        )

                stats_failed += 1
                failed_lib += 1
                row = db_get_media(rating_key)
                _, became_dead = handle_failed_item(
                    lib, rating_key, info, row, updated_iso
                )
                if became_dead:
                    stats_new_dead += 1

            if time_exceeded:
                lib_duration = (dt.datetime.now() - lib_start_ts).total_seconds()
                PROFILER[lib] = {
                    "duration": lib_duration,
                    "checked": checked_lib,
                    "fixed": fixed_count,
                    "failed": failed_lib,
                    "skipped": skip_lib,
                }
                break

            lib_duration = (dt.datetime.now() - lib_start_ts).total_seconds()
            PROFILER[lib] = {
                "duration": lib_duration,
                "checked": checked_lib,
                "fixed": fixed_count,
                "failed": failed_lib,
                "skipped": skip_lib,
            }

            log(
                f"{lib} – Lauf fertig ({fixed_count} gefixt, {stats_skip} übersprungen)",
                "REFRESH",
            )
            if fixed_count > 0:
                refreshed_lines.append(f"• {lib}: {fixed_count} Einträge")

        duration = (dt.datetime.now() - start_ts).total_seconds()
        avg_cpu = sum(cpu_vals_global) / len(cpu_vals_global) if cpu_vals_global else 0.0
        peak_cpu = cpu_peak_global if cpu_vals_global else 0.0

        # Warnings & Profiler-Logs
        warnings_list: List[str] = []
        for lib, p in PROFILER.items():
            dur = p.get("duration", 0.0)
            checked = p.get("checked", 0) or 0
            failed = p.get("failed", 0) or 0
            skipped = p.get("skipped", 0) or 0
            fixed = p.get("fixed", 0) or 0

            log_extra(
                "profiler.log",
                f"LIB | name={lib} | duration={dur:.1f}s | checked={checked} | "
                f"fixed={fixed} | failed={failed} | skipped={skipped}",
            )

            if dur >= 600:
                warnings_list.append(f"Ungewöhnlich langsam: {lib} → {format_dur(dur)}")

            if checked >= 20 and failed / max(1, checked) >= 0.30:
                warnings_list.append(
                    f"Hohe Fehlerquote: {lib} → {failed}/{checked} Items"
                )

        if TMDB_TRIES > 0:
            hit_rate = TMDB_HITS / max(1, TMDB_TRIES)
            if hit_rate < 0.5:
                warnings_list.append(
                    f"Niedrige TMDB-Trefferquote: {hit_rate*100:.0f}% "
                    f"({TMDB_HITS}/{TMDB_TRIES})"
                )

        if warnings_list:
            log_extra("warnings.log", f"SCAN WARNINGS {dt.datetime.now():%d.%m.%Y %H:%M:%S}")
            for w in warnings_list:
                log_extra("warnings.log", f"WARNING | {w}")

        if stats_failed > 0 and stats_fixed == 0:
            main_line = "⚠️ Fehler gefunden, aber keine fixbaren Metadaten."
        elif stats_fixed > 0:
            main_line = f"🔧 {stats_fixed} Metadaten korrigiert / GUIDs gesetzt."
        else:
            main_line = "✅ Keine fehlerhaften Metadaten gefunden – alles aktuell."
        main_line += f"\n• CPU: ⌀ {avg_cpu:.1f}% / Peak: {peak_cpu:.1f}%"

        if warnings_list:
            warn_short = "\n".join(f"• {w}" for w in warnings_list[:5])
            main_line += f"\n⚠️ Warnungen:\n{warn_short}"

        status.update(
            {
                "last_refresh": (
                    f"{start_ts:%d.%m.%Y %H:%M:%S} – {main_line} · Dauer: {format_dur(duration)}"
                ),
                "last_refresh_details": "\n".join(refreshed_lines),
                "cpu_line": f"⌀ {avg_cpu:.1f}% / Peak: {peak_cpu:.1f}%",
                "status_line": f"✅ Smart-Refresh abgeschlossen ({status['plex_name']})",
            }
        )

        status["last_error"] = (
            "Es sind Fehler aufgetreten:\n" + "\n".join(errors[:10]) if errors else ""
        )
        total_dead = db_count_dead_total()
        ts_now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        stats_block = (
            f"📊 Statistik dieses Laufs – Stand {ts_now}\n"
            f"• Geprüft: {stats_checked}\n"
            f"• Gefixt / GUID gesetzt: {stats_fixed}\n"
            f"• Fehlgeschlagen: {stats_failed}\n"
            f"• Cooldown/Dead übersprungen: {stats_skip}\n"
            f"• Neu tot: {stats_new_dead}\n"
            f"• Gesamt tot: {total_dead}"
        )
        if warnings_list:
            stats_block += "\n\n⚠️ WARNUNGEN\n" + "\n".join(
                f"• {w}" for w in warnings_list[:5]
            )
        status["stats_block"] = stats_block

        write_health(True)
        cpu_vals_global.clear()
        cpu_peak_global = 0.0

        async with state_lock:
            msg_state["last_status"] = {
                "last_refresh": status["last_refresh"],
                "last_refresh_details": status["last_refresh_details"],
                "cpu_line": status["cpu_line"],
                "last_error": status["last_error"],
                "stats_block": status["stats_block"],
            }
            save_state(msg_state)

        end_ts = dt.datetime.now()
        log(f"SCAN ENDE {end_ts:%d.%m.%Y %H:%M:%S} – Dauer: {format_dur(duration)}", "REFRESH")
        log("=" * 80, "REFRESH")
        log_extra("profiler.log", f"SCAN ENDE | {end_ts:%d.%m.%Y %H:%M:%S} | dur={format_dur(duration)}")

        nxt = next_target_datetime()
        status.update({"mode": "IDLE", "next_run": next_run_human(nxt)})
        await update_embed()

# ==============================================================

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
            status["health"] = f"Health-Fehler: {e}"
        await update_embed_cb()
        await asyncio.sleep(HEALTHCHECK_INTERVAL_MINUTES * 60)

# ==============================================================

async def _runner_without_discord():
    log("Discord deaktiviert – starte Smart-Refresher ohne Bot.", "MAIN")
    asyncio.create_task(periodic_health(update_embed))
    await smart_refresh_loop()

# ==============================================================

def main():
    if not os.path.exists(HEALTH_FILE):
        try:
            with open(HEALTH_FILE, "w") as f:
                f.write("OK|" + dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
            status["health"] = "OK (initial)"
        except Exception as e:
            log(f"Health-Init-Fehler: {e}", "HEALTH")

    global tg_bot
    if ENABLE_TELEGRAM and ENABLE_TELEGRAM_IMPORT:
        try:
            tg_bot = TgBot(token=TELEGRAM_BOT_TOKEN)  # type: ignore
            log("Telegram: Bot initialisiert.", "TELEGRAM")
        except Exception as e:
            log(f"Telegram-Initialisierungsfehler: {e}", "TELEGRAM")
            tg_bot = None
    else:
        tg_bot = None

    global bot
    if ENABLE_DISCORD and ENABLE_DISCORD_IMPORT:
        log("Starte Discord-Modus…", "MAIN")
        intents = discord.Intents.none()  # type: ignore
        intents.guilds = True
        intents.message_content = True
        bot = commands.Bot(command_prefix="!", intents=intents)  # type: ignore

        @bot.event
        async def on_ready():  # type: ignore
            log(f"Verbunden als {bot.user}", "DISCORD")  # type: ignore
            asyncio.create_task(smart_refresh_loop())
            asyncio.create_task(periodic_health(update_embed))
            log("🟢 Smart-Refresher läuft im Discord-Modus.", "MAIN")

        try:
            bot.run(DISCORD_TOKEN)  # type: ignore
        finally:
            if tg_bot:
                try:
                    asyncio.run(tg_bot.session.close())  # type: ignore
                except Exception:
                    pass
    else:
        log("Discord deaktiviert – Standalone-Modus aktiv.", "MAIN")
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(_runner_without_discord())
        finally:
            if tg_bot:
                try:
                    loop.run_until_complete(loop.create_task(tg_bot.session.close()))  # type: ignore
                except Exception:
                    pass

# ==============================================================

if __name__ == "__main__":
    print("📦 Starte Plex Smart-Refresher 4.2 (TMDB, Logs, Profiler) …", flush=True)
    main()
