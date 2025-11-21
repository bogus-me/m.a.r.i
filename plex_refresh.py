#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio, datetime as dt, json, os, sqlite3, sys, time, socket, unicodedata, re, gc
from typing import Any, Dict, List, Optional, Tuple, Generator
from collections import deque
from contextlib import contextmanager

import requests, psutil
from dotenv import load_dotenv
from plexapi.server import PlexServer  # type: ignore

# --- Telegram (Aiogram) ---
ENABLE_TELEGRAM_IMPORT = False
try:
    from aiogram import Bot as TgBot # type: ignore
    from aiogram.client.session.aiohttp import AiohttpSession # type: ignore
    ENABLE_TELEGRAM_IMPORT = True
except Exception:
    pass

# --- Discord ---
ENABLE_DISCORD_IMPORT = False
try:
    import discord # type: ignore
    from discord.ext import commands # type: ignore
    ENABLE_DISCORD_IMPORT = True
except Exception:
    pass

import warnings, urllib3
warnings.simplefilter("ignore", urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings()

try:
    sys.stdout.reconfigure(line_buffering=True)
except:
    pass

load_dotenv()

# =====================================================================
# EXCLUDE-LIBRARIES – werden vollständig vom Fixing ausgeschlossen
# =====================================================================

EXCLUDE_LIBRARIES = {
    "Filme - Konzerte",
    "Filme - Sport",
    "Filme - Adult",
    "Filme - O-Ton",
    "TV - Comedy",
    "TV - Reality_Soap",
}

# =====================================================================
# ENV
# =====================================================================

def env_required(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        print(f"[ERROR] ENV fehlt: {name}"); sys.exit(1)
    return v

def env_int(name: str) -> int:
    val = env_required(name)
    try: return int(val)
    except: print(f"[ERROR] {name} muss int sein"); sys.exit(1)

def env_bool(name: str) -> bool:
    v = env_required(name).lower()
    if v not in ("true","false"):
        print(f"[ERROR] {name} muss true/false sein"); sys.exit(1)
    return v == "true"

# =====================================================================
# KONFIG
# =====================================================================

PLEX_URL       = env_required("PLEX_URL")
PLEX_TOKEN     = env_required("PLEX_TOKEN")
TMDB_API_KEY   = env_required("TMDB_API_KEY")
PLEX_TIMEOUT   = int(os.getenv("PLEX_TIMEOUT", "30"))

REFRESH_INTERVAL_DAYS = env_int("REFRESH_INTERVAL_DAYS")
REFRESH_TIME          = env_required("REFRESH_TIME")

RAW_LOG_FILE = env_required("LOG_FILE")
LOG_MAX_LINES = env_int("LOG_MAX_LINES")
HEALTH_FILE = env_required("HEALTH_FILE")
MSG_STATE_FILE = env_required("MSG_STATE_FILE")
HEALTHCHECK_MAX_DAYS = env_int("HEALTHCHECK_MAX_DAYS")
HEALTHCHECK_INTERVAL_MINUTES = env_int("HEALTHCHECK_INTERVAL_MINUTES")

MAX_ITEMS_PER_RUN = 200
SCAN_TIME_LIMIT_SECONDS = 600
SMART_LOOKBACK_DAYS = 30
PLEX_SCAN_CHECK_INTERVAL = 10
MAX_FAILS = 5
COOLDOWN_STEPS_DAYS = [1, 7, 14, 30]

# Memory Optimizations
CHUNK_SIZE = 500  # Items pro Chunk
MAX_CPU_SAMPLES = 3600  # Max 1h bei 1s Intervall
FETCH_CACHE_SIZE = 1000  # LRU Cache für Plex Items

LOG_BASE = os.path.dirname(RAW_LOG_FILE) or "/app"
LOG_DIR = os.path.join(LOG_BASE, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, os.path.basename(RAW_LOG_FILE))

def db_path():
    return os.path.join(LOG_DIR, "refresh_state.db")

DB_PATH = os.getenv("REFRESH_DB_PATH", db_path())

# =====================================================================
# Discord / Telegram ENV
# =====================================================================

ENABLE_DISCORD = env_bool("ENABLE_DISCORD_NOTIFY")
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN","")
DISCORD_CH_ID_RAW = os.getenv("DISCORD_CHANNEL_ID","0")

ENABLE_TELEGRAM = env_bool("ENABLE_TELEGRAM_NOTIFY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN","")
TELEGRAM_CHAT_ID_RAW = os.getenv("TELEGRAM_CHAT_ID","0")

if ENABLE_DISCORD:
    try: DISCORD_CHANNEL_ID = int(DISCORD_CH_ID_RAW)
    except: print("[ERROR] DISCORD_CHANNEL_ID ungültig"); sys.exit(1)
else:
    DISCORD_CHANNEL_ID = 0

if ENABLE_TELEGRAM:
    try: TELEGRAM_CHAT_ID = int(TELEGRAM_CHAT_ID_RAW)
    except: print("[ERROR] TELEGRAM_CHAT_ID ungültig"); sys.exit(1)
else:
    TELEGRAM_CHAT_ID = 0

bot = None
tg_bot = None

# =====================================================================
# HIGH PERFORMANCE LOGGING
# =====================================================================

_log_lock = asyncio.Lock()

def _fast_read_head(path: str, max_lines: int) -> deque:
    """Optimiert: Verwendet deque mit maxlen"""
    lines = deque(maxlen=max_lines)
    try:
        with open(path, "r") as f:
            for ln in f:
                lines.append(ln.rstrip("\n"))
    except:
        pass
    return lines

def log_sync(msg: str, p="MAIN"):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(log(msg, p))
        else:
            ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
            print(f"[{ts}] [{p}] {msg}", flush=True)
    except:
        ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        print(f"[{ts}] [{p}] {msg}", flush=True)

async def log(msg: str, p="MAIN"):
    ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    line = f"[{ts}] [{p}] {msg}"
    print(line, flush=True)

    async with _log_lock:
        try:
            old_lines = _fast_read_head(LOG_FILE, LOG_MAX_LINES - 1)
            tmp_path = LOG_FILE + ".tmp"
            with open(tmp_path, "w") as f:
                f.write(line + "\n")
                for ln in old_lines:
                    f.write(ln + "\n")
            os.replace(tmp_path, LOG_FILE)
        except:
            pass

# Batch Log Writer für bessere Performance
_pending_logs = deque(maxlen=1000)
_log_batch_lock = asyncio.Lock()

async def log_extra_batch(name: str, msg: str):
    """Sammelt Logs und schreibt sie gebatched"""
    _pending_logs.append((name, msg))

async def batch_log_writer():
    """Background Task zum Schreiben von Logs in Batches"""
    while True:
        await asyncio.sleep(2)
        if _pending_logs:
            async with _log_batch_lock:
                batch = list(_pending_logs)
                _pending_logs.clear()
                
                ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                logs_by_file = {}
                
                for name, msg in batch:
                    path = name if os.path.isabs(name) else os.path.join(LOG_DIR, name)
                    if path not in logs_by_file:
                        logs_by_file[path] = []
                    logs_by_file[path].append(f"[{ts}] {msg}")
                
                for path, messages in logs_by_file.items():
                    try:
                        with open(path, "a") as f:
                            f.write("\n".join(messages) + "\n")
                    except:
                        pass

# Fallback für synchrone Logs (backward compatibility)
async def log_extra(name: str, msg: str):
    await log_extra_batch(name, msg)

# =====================================================================
# HEALTH, STATUS, CPU
# =====================================================================

def write_health(ok=True):
    try:
        with open(HEALTH_FILE,"w") as f:
            f.write(("OK" if ok else "FAIL")+"|"+dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
        log_sync("Health aktualisiert","HEALTH")
    except Exception as e:
        log_sync(f"Health-Fehler: {e}","HEALTH")

status = {
    "plex_name":"—",
    "mode":"INIT",
    "status_line":"⏳ Initialisiere…",
    "next_run":"—",
    "health":"Noch kein Health.",
    "last_refresh":"Noch kein Refresh.",
    "last_refresh_details":"",
    "cpu_line":"—",
    "last_error":"",
    "stats_block":"• Noch keine Statistik",
}

def load_state():
    if os.path.exists(MSG_STATE_FILE):
        try: return json.load(open(MSG_STATE_FILE))
        except: pass
    return {}

def save_state(d: Dict[str,Any]):
    """Atomic save - verhindert korruptes JSON bei Crash"""
    try:
        tmp = MSG_STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(d, f, indent=2)
        os.replace(tmp, MSG_STATE_FILE)
    except Exception as e:
        log_sync(f"State-Save Fehler: {e}", "STATE")

msg_state = load_state()
state_lock = asyncio.Lock()
if isinstance(msg_state.get("last_status"), dict):
    status.update(msg_state["last_status"])

status.setdefault("stats_block", "• Noch keine Statistik")

PROC = psutil.Process()
cpu_vals = deque(maxlen=MAX_CPU_SAMPLES)  # Optimiert: Begrenzte deque
cpu_peak = 0.0

async def cpu_sampler():
    global cpu_peak
    while True:
        try:
            v = PROC.cpu_percent(interval=None)
            cpu_vals.append(v)
            cpu_peak = max(cpu_peak, v)
        except:
            pass
        await asyncio.sleep(1)

# =====================================================================
# UTIL
# =====================================================================

def format_dur(sec: float) -> str:
    sec = int(sec)
    m, s = divmod(sec, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    out=[]
    if d: out.append(f"{d}d")
    if h: out.append(f"{h}h")
    if m: out.append(f"{m}min")
    if not out: out.append(f"{s}s")
    return " ".join(out)

def human_until(target: dt.datetime) -> str:
    now = dt.datetime.now()
    diff = max(0, int((target-now).total_seconds()))
    m = diff//60; h=m//60; m%=60
    return f"in {h}h {m}min" if h else f"in {m}min"

def next_target_datetime() -> dt.datetime:
    now = dt.datetime.now()
    h,m = map(int, REFRESH_TIME.split(":"))
    base = now.replace(hour=h, minute=m, second=0, microsecond=0)
    while base <= now:
        base += dt.timedelta(days=REFRESH_INTERVAL_DAYS)
    return base

def next_run_human(t: dt.datetime) -> str:
    return f"{human_until(t)}\n{t:%d.%m.%Y %H:%M}"

def iso_now() -> str:
    return dt.datetime.now().isoformat(timespec="seconds")

def iso_in_days(d: int) -> str:
    return (dt.datetime.now()+dt.timedelta(days=d)).isoformat(timespec="seconds")

# =====================================================================
# DB CONNECTION POOL
# =====================================================================

class DBConnectionPool:
    """Einfacher Connection Pool für SQLite"""
    def __init__(self, path: str, pool_size: int = 3):
        self.path = path
        self.pool_size = pool_size
        self._connections = deque()
        self._lock = asyncio.Lock()
        
        # Initialisiere Connections
        for _ in range(pool_size):
            conn = self._create_connection()
            self._connections.append(conn)
    
    def _create_connection(self):
        c = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        c.row_factory = sqlite3.Row
        return c
    
    @contextmanager
    def get_connection(self):
        """Context Manager für Connection aus Pool"""
        conn = None
        try:
            if self._connections:
                conn = self._connections.popleft()
            else:
                conn = self._create_connection()
            yield conn
        finally:
            if conn:
                self._connections.append(conn)
    
    def close_all(self):
        """Schließe alle Connections im Pool"""
        while self._connections:
            conn = self._connections.popleft()
            try:
                conn.close()
            except:
                pass

# Globaler Connection Pool
db_pool = None

def init_db_pool():
    global db_pool
    db_pool = DBConnectionPool(DB_PATH)

# =====================================================================
# DB OPERATIONS
# =====================================================================

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS media_state(
    rating_key TEXT PRIMARY KEY,
    library TEXT,
    first_seen TEXT,
    last_scanned TEXT,
    last_updated_at TEXT,
    fail_count INTEGER DEFAULT 0,
    ignore_until TEXT,
    state TEXT DEFAULT 'active',
    note TEXT
);
CREATE INDEX IF NOT EXISTS idx_media_lib ON media_state(library);
CREATE INDEX IF NOT EXISTS idx_media_state ON media_state(state);
CREATE INDEX IF NOT EXISTS idx_media_ignore ON media_state(ignore_until);
"""

def db_init():
    c = sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)
    with c: 
        c.executescript(SCHEMA_SQL)
    c.close()

def db_get_media(key: str):
    with db_pool.get_connection() as c:
        cur = c.execute("SELECT * FROM media_state WHERE rating_key=?", (key,))
        return cur.fetchone()

def db_upsert_media(key, lib, updated, fails, state, until, note):
    with db_pool.get_connection() as c:
        row = db_get_media(key)
        first = row["first_seen"] if row and row["first_seen"] else iso_now()
        c.execute("""
            INSERT INTO media_state(rating_key,library,first_seen,last_scanned,last_updated_at,
            fail_count,ignore_until,state,note)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(rating_key) DO UPDATE SET
                library=excluded.library,
                last_scanned=excluded.last_scanned,
                last_updated_at=excluded.last_updated_at,
                fail_count=excluded.fail_count,
                ignore_until=excluded.ignore_until,
                state=excluded.state,
                note=excluded.note
        """,(key,lib,first,iso_now(),updated,fails,until,state,note))

def db_count_dead_total()->int:
    with db_pool.get_connection() as c:
        cur = c.execute("SELECT COUNT(*) c FROM media_state WHERE state='dead'")
        return cur.fetchone()["c"]

# =====================================================================
# TITLE NORMALIZATION – OPTIMIERT
# =====================================================================

BIDI_CHARS = {
    "\u200e","\u200f","\u202a","\u202b","\u202c","\u202d","\u202e",
    "\ufeff","\u2066","\u2067","\u2068","\u2069"
}
BIDI_MARKERS = [
    "[U+200E]","[U+200F]","[U+202A]","[U+202B]","[U+202C]","[U+202D]","[U+202E]"
]

# Regex Pattern für Normalisierung (kompiliert für Performance)
NORMALIZE_PATTERN = re.compile(r'[()[\]{}_\-.:,]')
YEAR_PATTERN = re.compile(r'\b(19|20)\d{2}\b')

def clean_bidi(s: str) -> str:
    if not isinstance(s, str): 
        return s
    for ch in BIDI_CHARS: 
        s = s.replace(ch, "")
    for m in BIDI_MARKERS: 
        s = s.replace(m, "")
    return s.strip()

def normalize_title(s: str) -> str:
    """Optimierte Version mit Regex"""
    if not s: 
        return ""
    
    s = clean_bidi(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    
    # Entferne Jahre mit Regex
    s = YEAR_PATTERN.sub(' ', s)
    
    # Ersetze Sonderzeichen mit Regex
    s = NORMALIZE_PATTERN.sub(' ', s)
    
    # Entferne Mehrfach-Leerzeichen effizient
    return ' '.join(s.split())

def ratio(a: str, b: str) -> float:
    if not a or not b: 
        return 0.0
    a, b = a.lower(), b.lower()
    total = max(len(a), len(b))
    match = sum(1 for i in range(min(len(a), len(b))) if a[i] == b[i])
    return match / total

def smart_fuzzy(a: str, b: str) -> float:
    if not a or not b: 
        return 0.0
    na, nb = normalize_title(a), normalize_title(b)
    if not na or not nb: 
        return 0.0
    base = ratio(na, nb)
    if na in nb or nb in na: 
        base = max(base, 0.90)
    return base

# =====================================================================
# TMDB
# =====================================================================

TMDB_STATUS="unknown"
TMDB_LAST_ERROR=""
TMDB_LAST_CHECK=None
TMDB_LAST_LOOKUP=None
TMDB_TRIES=0
TMDB_HITS=0

TMDB_MOVIE_SEARCH="https://api.themoviedb.org/3/search/movie"
TMDB_TV_SEARCH="https://api.themoviedb.org/3/search/tv"
TMDB_FIND_EXTERNAL="https://api.themoviedb.org/3/find/{ext_id}"

# Persistent Session für TMDB (30-40% schneller)
tmdb_session = requests.Session()
tmdb_session.verify = False

def tmdb_request(url, params):
    params["api_key"] = TMDB_API_KEY
    try:
        r = tmdb_session.get(url, params=params, timeout=10)
        if r.status_code != 200:
            log_sync(f"TMDB HTTP {r.status_code}: {url}", "TMDB")
            return None
        return r.json()
    except Exception as e:
        log_sync(f"TMDB Fehler: {e}", "TMDB")
        return None

def tmdb_check_connection():
    global TMDB_STATUS, TMDB_LAST_ERROR, TMDB_LAST_CHECK
    TMDB_LAST_CHECK = iso_now()
    try:
        r = tmdb_session.get("https://api.themoviedb.org/3/configuration",
                       params={"api_key": TMDB_API_KEY}, timeout=8)
        if r.status_code == 200:
            TMDB_STATUS = "ok"
            TMDB_LAST_ERROR = ""
            log_sync("TMDB OK", "TMDB")
            return True
        TMDB_STATUS = "error"
        TMDB_LAST_ERROR = f"HTTP {r.status_code}"
        return False
    except Exception as e:
        TMDB_STATUS = "error"
        TMDB_LAST_ERROR = str(e)
        return False

def extract_year(itm) -> Optional[int]:
    try:
        y = getattr(itm, "year", None)
        return y if isinstance(y, int) else None
    except: 
        return None

def tmdb_search_movie(title, year):
    p = {"query": title}
    if year: 
        p["year"] = year
    return tmdb_request(TMDB_MOVIE_SEARCH, p)

def tmdb_search_tv(title, year):
    p = {"query": title}
    if year: 
        p["first_air_date_year"] = year
    return tmdb_request(TMDB_TV_SEARCH, p)

def tmdb_find_by_external(ext_id, src):
    if src == "tvdb": 
        e = "tvdb_id"
    elif src == "imdb": 
        e = "imdb_id"
    else: 
        return None
    url = TMDB_FIND_EXTERNAL.format(ext_id=ext_id)
    return tmdb_request(url, {"external_source": e})

def try_external_lookup(itm):
    for g in getattr(itm, "guids", []):
        gid = (getattr(g, "id", "") or "").lower()
        ext = (g.id or "").split("/")[-1]
        if "tvdb" in gid:
            j = tmdb_find_by_external(ext, "tvdb")
            if j and j.get("tv_results"):
                return j["tv_results"][0]["id"]
        if "imdb" in gid:
            j = tmdb_find_by_external(ext, "imdb")
            if j:
                if j.get("movie_results"):
                    return j["movie_results"][0]["id"]
                if j.get("tv_results"):
                    return j["tv_results"][0]["id"]
    return None

def try_search_movie(itm):
    global TMDB_TRIES, TMDB_LAST_LOOKUP, TMDB_HITS
    title = getattr(itm, "title", "")
    year = extract_year(itm)

    TMDB_TRIES += 1
    TMDB_LAST_LOOKUP = iso_now()
    log_sync(f"TMDB Suche Film: {title} ({year})", "TMDB")

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
            except: 
                pass
        if score >= 0.85 and score > best_score:
            best_score = score
            best_id = r["id"]

    if best_id:
        TMDB_HITS += 1
        asyncio.create_task(
            log_extra_batch("tmdb_hits.log",
                f"HIT | movie | {clean_bidi(title)} | id={best_id} | s={best_score:.2f}")
        )
        return best_id

    asyncio.create_task(
        log_extra_batch("tmdb_hits.log", f"MISS | movie | {clean_bidi(title)} | year={year}")
    )
    return None

def try_search_show(itm):
    global TMDB_TRIES, TMDB_LAST_LOOKUP, TMDB_HITS
    title = getattr(itm, "title", "")
    year = extract_year(itm)

    TMDB_TRIES += 1
    TMDB_LAST_LOOKUP = iso_now()
    log_sync(f"TMDB Suche Serie: {title} ({year})", "TMDB")

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
            except: 
                pass
        if score >= 0.85 and score > best_score:
            best_score = score
            best_id = r["id"]

    if best_id:
        TMDB_HITS += 1
        asyncio.create_task(
            log_extra_batch("tmdb_hits.log",
                f"HIT | tv | {clean_bidi(title)} | id={best_id} | s={best_score:.2f}")
        )
        return best_id

    asyncio.create_task(
        log_extra_batch("tmdb_hits.log", f"MISS | tv | {clean_bidi(title)} | year={year}")
    )
    return None

def tmdb_find_guid_for_item(itm):
    ext = try_external_lookup(itm)
    if ext: 
        return ext
    t = getattr(itm, "type", "")
    if t == "movie": 
        return try_search_movie(itm)
    if t == "show": 
        return try_search_show(itm)
    return None

# =====================================================================
# GUID-SETTER
# =====================================================================

def set_guid(itm, tmdb_id: int) -> bool:
    tag = f"tmdb://{tmdb_id}"
    title = clean_bidi(getattr(itm, "title", "???"))

    fn = getattr(itm, "editGuid", None) or getattr(itm, "addGuid", None)
    if not fn:
        log_sync(f"GUID nicht gesetzt (kein edit/add): {title}", "GUID")
        return False

    try:
        fn([tag])
    except Exception as e:
        log_sync(f"GUID-Fehler: {title}: {e}", "GUID")
        return False

    try:
        fresh = itm._server.fetchItem(itm.ratingKey)
        guids = [getattr(g, "id", "") for g in getattr(fresh, "guids", [])]
        if tag in guids:
            log_sync(f"GUID gesetzt: {title} -> {tag}", "GUID")
            return True
        log_sync(f"GUID verworfen von Plex: {title}", "GUID")
        return False
    except Exception as e:
        log_sync(f"GUID-Verify-Fehler: {title}: {e}", "GUID")
        return False

# =====================================================================
# REFRESH-NEEDS
# =====================================================================

def needs_refresh(itm) -> Tuple[bool, Dict[str, Any]]:
    title = getattr(itm, "title", "???")
    thumb = getattr(itm, "thumb", None)
    summary = (getattr(itm, "summary", "") or "").strip()
    rating = getattr(itm, "rating", None)
    guids = getattr(itm, "guids", [])

    missing_guid = not bool(guids)
    missing_thumb = (thumb is None)
    missing_summary = not summary
    missing_rating = (rating is None)

    need = (
        missing_guid
        or (missing_thumb and missing_summary)
        or (missing_rating and (missing_guid or missing_thumb or missing_summary))
    )

    return need, {
        "title": clean_bidi(title),
        "missing_guid": missing_guid,
        "missing_thumb": missing_thumb,
        "missing_summary": missing_summary,
        "missing_rating": missing_rating
    }

# =====================================================================
# DISCORD/TELEGRAM HELPERS
# =====================================================================

def fmt_tmdb_dt(val: Optional[str]) -> str:
    if not val:
        return "—"
    try:
        d = dt.datetime.fromisoformat(val)
        return d.strftime("%d.%m.%Y %H:%M")
    except:
        return val

# =====================================================================
# DISCORD – kompakt & stabil
# =====================================================================

DISCORD_UPDATE_INTERVAL = 3.0
_last_discord_update = 0.0
_pending_discord = False
_last_payload = ""
discord_send_lock = asyncio.Lock()

def _build_payload() -> str:
    s = status
    nr = str(s["next_run"]).splitlines()
    line = nr[0] if nr else "—"
    date = nr[1] if len(nr) >= 2 else "—"
    return "|".join([
        s["status_line"], s["mode"], line, date,
        s["last_refresh"], s["health"], s["last_refresh_details"],
        s.get("stats_block", ""), s["last_error"], s["cpu_line"]
    ])

async def update_discord_embed():
    global _pending_discord
    _pending_discord = True
    await _discord_maybe_send()

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

        # Lazy evaluation - baue Payload erst wenn nötig
        payload = _build_payload()
        if payload != _last_payload:
            _last_payload = payload
            await _discord_send_core()

async def _discord_send_core():
    try:
        await _discord_embed_raw()
    except Exception as e:
        if ENABLE_DISCORD_IMPORT and isinstance(e, discord.HTTPException) and getattr(e, "status", None) == 429:
            d = float(getattr(e, "retry_after", 3))
            log_sync(f"[DISCORD] 429 – warte {d:.1f}s", "DISCORD")
            await asyncio.sleep(d)
            await _discord_embed_raw()
        else:
            log_sync(f"Discord Fehler: {e}", "DISCORD")

async def _discord_embed_raw():
    if not (ENABLE_DISCORD and ENABLE_DISCORD_IMPORT and DISCORD_CHANNEL_ID):
        return

    global bot
    if bot is None:
        return

    s = status
    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")

    nr = str(s["next_run"]).splitlines()
    next_line = nr[0] if nr else "—"
    next_date = nr[1] if len(nr) >= 2 else "—"

    tmdb_rate = f"{(TMDB_HITS / max(1, TMDB_TRIES) * 100):.0f}%" if TMDB_TRIES else "—"

    sl = s["status_line"].splitlines()

    # ---- SYSTEM BLOCK ----
    if len(sl) >= 3:
        lib_line      = sl[0]
        progress_line = sl[1]
        eta_raw       = sl[2].replace("ETA: ", "")

        if eta_raw.lower().startswith("berechne"):
            system_block = (
                f"• {lib_line}\n"
                f"• {progress_line}\n"
                f"• ETA: {eta_raw}"
            )
        else:
            system_block = (
                f"• {lib_line}\n"
                f"• {progress_line}\n"
                f"• {s['mode']} – ETA: {eta_raw}"
            )
    else:
        main_line = sl[0] if sl else "Status unbekannt"
        system_block = (
            f"• {main_line}\n"
            f"• Modus: {s['mode']}"
        )

    # ---- STATS TITLE ----
    stats_title = (
        f"**STATS – {status['stats_timestamp']}**"
        if status.get("stats_timestamp")
        else "**STATS**"
    )

    # ---- DESC (immer gesetzt!) ----
    desc = (
        f"**SYSTEMSTATUS**\n"
        f"{system_block}\n\n"

        f"**PLANUNG**\n"
        f"• Letzter Lauf: {s['last_refresh'].split(' – ')[0].replace('–','-')}\n"
        f"• Nächster Lauf: {next_line}\n"
        f"• Datum: {next_date}\n\n"

        f"**HEALTH**\n"
        f"• {s['health']}\n\n"

        f"**TMDB**\n"
        f"• API OK – {fmt_tmdb_dt(TMDB_LAST_CHECK)}\n"
        f"• Trefferquote: {tmdb_rate}\n"
        f"• Letzter Lookup: {fmt_tmdb_dt(TMDB_LAST_LOOKUP)}\n\n"

        f"{stats_title}\n"
        f"{s.get('stats_block','• Noch keine Statistik')}\n"
    )

    if len(desc) > 4000:
        desc = desc[:4000] + "\n… (gekürzt)"

    # Farben
    color = (
        0xFF0000 if s["mode"] == "ERROR"
        else 0xFFA500 if s["mode"] != "IDLE"
        else 0x00FF00
    )

    emb = discord.Embed(description=desc, color=color)
    emb.set_footer(text=f"Aktualisiert: {now}")

    ch = bot.get_channel(DISCORD_CHANNEL_ID) or await bot.fetch_channel(DISCORD_CHANNEL_ID)

    async with state_lock:
        msg_state["last_status"] = status
        save_state(msg_state)

        mid = msg_state.get("discord_main")
        try:
            if mid:
                msg = await ch.fetch_message(mid)
                await msg.edit(embed=emb)
                return
        except:
            pass

        msg = await ch.send(embed=emb)
        msg_state["discord_main"]  = msg.id
        msg_state["last_status"]   = status
        save_state(msg_state)

# =====================================================================
# TELEGRAM – Aiogram stabil
# =====================================================================

async def update_telegram_message():
    if not (ENABLE_TELEGRAM and ENABLE_TELEGRAM_IMPORT and TELEGRAM_CHAT_ID):
        return
    global tg_bot
    if tg_bot is None: 
        return

    s = status
    now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    last_ref = s["last_refresh"]
    prefix = "" if last_ref.startswith(("🔄","❌","✅")) else "• "

    tmdb_state = "🟢 OK" if TMDB_STATUS == "ok" else f"🔴 Fehler – {TMDB_LAST_ERROR or 'unbekannt'}"
    tmdb_rate = f"{(TMDB_HITS/max(1,TMDB_TRIES)*100):.0f}%" if TMDB_TRIES else "—"

    txt = (
        f"🟢 <b>Status:</b> {s['status_line']}\n"
        f"⚙️ <b>Modus:</b> {s['mode']}\n\n"
        f"🕒 <b>Nächster Lauf:</b> {s['next_run']}\n\n"
        f"📅 <b>Letzter Lauf:</b>\n{prefix}{last_ref}\n\n"
        f"🎬 <b>TMDB</b>\n"
        f"• Status: {tmdb_state}\n"
        f"• Trefferquote: {tmdb_rate}\n"
        f"• Lookup: {fmt_tmdb_dt(TMDB_LAST_LOOKUP)}\n"
        f"• Check: {fmt_tmdb_dt(TMDB_LAST_CHECK)}"
    )

    if s["cpu_line"] != "—": 
        txt += f"\n• <b>CPU:</b> {s['cpu_line']}"
    if s["last_refresh_details"]: 
        txt += f"\n\n{s['last_refresh_details']}"
    if s["stats_block"]: 
        txt += f"\n\n{s['stats_block']}"
    if s["last_error"]: 
        txt += f"\n\n❌ <b>Fehler:</b> {s['last_error']}"
    txt += f"\n\n⏱️ <i>Aktualisiert:</i> {now}"

    async with state_lock:
        msg_state["last_status"] = status
        save_state(msg_state)

        mid = msg_state.get("telegram_main")
        try:
            if mid:
                await tg_bot.edit_message_text(
                    chat_id=TELEGRAM_CHAT_ID,
                    message_id=mid,
                    text=txt,
                    parse_mode="HTML"
                )
                return
        except:
            pass

        sent = await tg_bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=txt,
            parse_mode="HTML"
        )

        msg_state["telegram_main"] = sent.message_id
        msg_state["last_status"] = status
        save_state(msg_state)

async def update_embed():
    await update_discord_embed()
    await update_telegram_message()

# =====================================================================
# PLEX-WAIT / SCAN
# =====================================================================

def is_plex_reachable(url) -> bool:
    try:
        host = url.split("//", 1)[-1].split("/")[0].split(":")[0]
        socket.gethostbyname(host)
        r = requests.get(url + "/identity", timeout=3, verify=False)
        return r.status_code == 200
    except:
        return False

async def wait_until_plex_online(url):
    tries = 0
    while True:
        if is_plex_reachable(url): 
            return
        tries += 1
        if tries == 3:
            log_sync("Plex offline – Warte 2min", "REFRESH")
            status["mode"] = "PAUSE"
            status["status_line"] = "⏸️ Plex offline – Boot-Fenster."
            await update_embed()
            await asyncio.sleep(120)
            tries = 0
        else:
            log_sync("Plex offline – retry in 5s", "REFRESH")
            status["mode"] = "PAUSE"
            status["status_line"] = "⏸️ Plex offline – retry…"
            await update_embed()
            await asyncio.sleep(5)

def plex_is_scanning_sync(plex) -> bool:
    try:
        for s in plex.library.sections():
            if getattr(s, "isScanning", False): 
                return True
    except:
        pass
    return False

async def plex_is_scanning_async(plex) -> bool:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, plex_is_scanning_sync, plex)

async def wait_until_plex_ready(plex):
    while True:
        try:
            if not await plex_is_scanning_async(plex): 
                return
            log_sync("Plex-Scan erkannt – Pause", "REFRESH")
            status["mode"] = "PAUSE"
            status["status_line"] = "⏸️ Plex scannt…"
            await update_embed()
        except Exception as e:
            log_sync(f"Scan-Check Fehler: {e}", "REFRESH")
            return
        await asyncio.sleep(PLEX_SCAN_CHECK_INTERVAL)

async def plex_connect_async() -> PlexServer:
    log_sync("[INIT] Verbinde mit Plex…", "REFRESH")
    await wait_until_plex_online(PLEX_URL)

    plex = PlexServer(PLEX_URL, PLEX_TOKEN, timeout=PLEX_TIMEOUT)
    plex._session.verify = False

    try:
        secs = plex.library.sections()
        rel = [s for s in secs if s.type in ("movie", "show")]
        log_sync(f"[INIT] Bibliotheken geladen: {len(rel)} relevante Sektionen", "REFRESH")
    except Exception as e:
        log_sync(f"[INIT-ERROR] Fehler beim Laden der Bibliotheken: {e}", "REFRESH")

    return plex

# =====================================================================
# CHUNKED PLEX ITEM PROCESSING – MEMORY OPTIMIZED
# =====================================================================

def process_items_in_chunks(all_items, chunk_size: int = CHUNK_SIZE) -> Generator[List, None, None]:
    """
    Generator für chunk-weise Item-Verarbeitung
    Lädt alle Items, gibt sie aber in Chunks zurück
    Items werden nach Verarbeitung freigegeben
    """
    for i in range(0, len(all_items), chunk_size):
        chunk = all_items[i:i + chunk_size]
        yield chunk
        # Chunk wird nach yield automatisch freigegeben

# =====================================================================
# PERFORMANCE MONITORING
# =====================================================================

class PerformanceMonitor:
    """Überwacht Performance-Metriken während des Scans"""
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset für neuen Scan"""
        self.start_time = None
        self.end_time = None
        self.start_ram = 0
        self.peak_ram = 0
        self.end_ram = 0
        self.cpu_samples = deque(maxlen=1000)
        self.db_query_times = deque(maxlen=1000)
        self.current_library = "—"
        self.current_phase = "Initializing"  # NEU
        self.current_items = 0  # NEU
        self.library_peaks = {}
        self.is_running = False
    
    def start_scan(self):
        """Startet Performance-Monitoring"""
        self.reset()
        self.start_time = dt.datetime.now()
        self.start_ram = PROC.memory_info().rss / 1024 / 1024
        self.is_running = True
        self.current_phase = "Starting"
    
    def end_scan(self):
        """Beendet Performance-Monitoring"""
        self.end_time = dt.datetime.now()
        self.end_ram = PROC.memory_info().rss / 1024 / 1024
        self.is_running = False
        self.current_phase = "Completed"
    
    def set_phase(self, phase: str, items: int = 0):
        """Setzt aktuelle Phase (Loading, Processing, Completed)"""
        self.current_phase = phase
        self.current_items = items
    
    def update_library(self, lib_name: str, item_count: int):
        """Aktualisiert aktuelle Library"""
        self.current_library = lib_name
        self.current_items = item_count
        current_ram = PROC.memory_info().rss / 1024 / 1024
        self.library_peaks[lib_name] = {
            'ram': current_ram,
            'items': item_count
        }
    
    def sample(self):
        """Nimmt Performance-Sample"""
        ram = PROC.memory_info().rss / 1024 / 1024
        cpu = PROC.cpu_percent(interval=None)
        
        self.peak_ram = max(self.peak_ram, ram)
        self.cpu_samples.append(cpu)
        
        return ram, cpu
    
    def get_status_string(self) -> str:
        """Generiert Status-String für Live-Log"""
        if self.current_library == "—":
            return f"{self.current_library} | {self.current_phase}"
        
        # Mit Items-Info
        if self.current_items > 0:
            return f"{self.current_library} | {self.current_phase} ({self.current_items:,} Items)"
        else:
            return f"{self.current_library} | {self.current_phase}"
    
    def get_summary(self, stats: dict) -> str:
        """Generiert Performance-Summary"""
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        
        avg_cpu = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
        peak_cpu = max(self.cpu_samples) if self.cpu_samples else 0
        
        throughput = stats.get('checked', 0) / duration if duration > 0 else 0
        
        # Top 3 Libraries by RAM
        top_libs = sorted(
            self.library_peaks.items(),
            key=lambda x: x[1]['ram'],
            reverse=True
        )[:3]
        
        summary = [
            "",  # Leerzeile VOR separator
            "=" * 60,
            f"SCAN ABGESCHLOSSEN: {self.end_time.strftime('%d.%m.%Y %H:%M:%S')}",
            "=" * 60,
            "",
            "TIMING",
            f"  • Start: {self.start_time.strftime('%H:%M:%S')}",
            f"  • Ende: {self.end_time.strftime('%H:%M:%S')}",
            f"  • Dauer: {format_dur(duration)}",
            "",
            "ITEMS",
            f"  • Geprüft: {stats.get('checked', 0):,}",
            f"  • Gefixt: {stats.get('fixed', 0):,}",
            f"  • Fehlgeschlagen: {stats.get('failed', 0):,}",
            f"  • Übersprungen: {stats.get('skipped', 0):,}",
            f"  • Throughput: {throughput:.1f} Items/s",
            "",
            "MEMORY",
            f"  • Start: {self.start_ram:.0f} MB",
            f"  • Peak: {self.peak_ram:.0f} MB",
            f"  • Ende: {self.end_ram:.0f} MB",
            f"  • Delta: {self.end_ram - self.start_ram:+.0f} MB",
            "",
            "CPU",
            f"  • Durchschnitt: {avg_cpu:.1f}%",
            f"  • Peak: {peak_cpu:.1f}%",
            "",
            "DATABASE",
            f"  • Queries: {stats.get('checked', 0):,}",
            "",
            "TMDB",
            f"  • Requests: {TMDB_TRIES}",
            f"  • Hits: {TMDB_HITS}",
            f"  • Hit-Rate: {(TMDB_HITS/max(1,TMDB_TRIES)*100):.0f}%",
            "",
            "TOP LIBRARIES (by Peak RAM)"
        ]
        
        for lib_name, data in top_libs:
            summary.append(f"  • {lib_name}: {data['ram']:.0f} MB ({data['items']:,} Items)")
        
        summary.append("=" * 60)
        summary.append("")
        
        return "\n".join(summary)

# Globaler Performance Monitor
perf_monitor = PerformanceMonitor()

# =====================================================================
# LIVE PERFORMANCE LOGGER
# =====================================================================

async def live_performance_logger():
    """
    Loggt Performance-Metriken live während des Scans
    Läuft nur wenn Scan aktiv ist
    """
    log_sync("Performance-Logger gestartet", "PERF")
    
    while True:
        try:
            if perf_monitor.is_running:
                ram, cpu = perf_monitor.sample()
                
                # Generiere Status-String mit Phase-Info
                status_str = perf_monitor.get_status_string()
                
                # Direktes Schreiben statt Batch für Performance-Logs
                ts = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
                path = os.path.join(LOG_DIR, "performance_live.log")
                
                with open(path, "a") as f:
                    f.write(f"[{ts}] RAM: {ram:.1f} MB | CPU: {cpu:.1f}% | Status: {status_str}\n")
                    f.flush()  # Force write
                    
        except Exception as e:
            log_sync(f"Performance-Logger Fehler: {e}", "PERF")
        
        await asyncio.sleep(5)  # Alle 5 Sekunden

async def refresh_item_and_check(plex, itm) -> bool:
    loop = asyncio.get_running_loop()
    
    def _refresh():
        try: 
            itm.refresh()
        except: 
            pass

    await loop.run_in_executor(None, _refresh)

    def _fetch():
        try: 
            return plex.fetchItem(itm.ratingKey)
        except: 
            return None

    fresh = await loop.run_in_executor(None, _fetch)
    if fresh is None: 
        return False

    still, _ = needs_refresh(fresh)
    return not still

def handle_failed_item(lib, rk, info, row, updated_iso):
    title = clean_bidi(info.get("title", "?"))
    reason = ", ".join(k for k, v in info.items() if v and k.startswith("missing"))
    fails = int((row["fail_count"] if row else 0) or 0) + 1

    asyncio.create_task(
        log_extra_batch(
            "failed.log",
            f"FAILED | lib={lib} | key={rk} | title={title} | fails={fails} | missing={reason}"
        )
    )

    if fails >= MAX_FAILS:
        log_sync(f"[DEAD] {lib} | {title} ({rk}) {fails}x failed", "REFRESH")
        asyncio.create_task(
            log_extra_batch(
                "dead.log",
                f"DEAD | lib={lib} | key={rk} | title={title} | fails={fails}"
            )
        )
        db_upsert_media(
            rk, lib, updated_iso, fails, "dead",
            iso_in_days(3650), "Permanent fehlerhaft"
        )
        return fails, True

    cd = COOLDOWN_STEPS_DAYS[min(fails - 1, len(COOLDOWN_STEPS_DAYS) - 1)]
    log_sync(f"[COOLDOWN] {lib} | {title} ({rk}) fail#{fails} → {cd}d", "REFRESH")

    db_upsert_media(
        rk, lib, updated_iso, fails, "cooldown",
        iso_in_days(cd), f"Fail {fails} – Cooldown {cd}d"
    )

    return fails, False

# =====================================================================
# SMART REFRESH LOOP – MEMORY OPTIMIZED
# =====================================================================

async def smart_refresh_loop():
    global cpu_vals, cpu_peak

    db_init()
    init_db_pool()
    log_sync(f"SQLite bereit: {DB_PATH}", "DB")
    
    # Starte Background-Tasks
    asyncio.create_task(cpu_sampler())
    asyncio.create_task(batch_log_writer())
    asyncio.create_task(live_performance_logger())  # Performance-Logger

    try:
        plex = await plex_connect_async()
        status["plex_name"] = plex.friendlyName
        status["status_line"] = f"✅ Verbunden mit Plex: {plex.friendlyName}"
        tmdb_check_connection()
        status["mode"] = "IDLE"

        tgt = next_target_datetime()
        status["next_run"] = next_run_human(tgt)

        if os.path.exists(HEALTH_FILE):
            try:
                raw = open(HEALTH_FILE).read().strip()
                st, ts = raw.split("|")
                ld = dt.datetime.strptime(ts, "%d.%m.%Y %H:%M:%S")
                diff = (dt.datetime.now() - ld).days
                status["health"] = (
                    f"🚨 Letzter Erfolg vor {diff} Tagen!"
                    if st == "FAIL" or diff > HEALTHCHECK_MAX_DAYS
                    else f"OK (zuletzt: {ts})"
                )
            except:
                status["health"] = "Health-Datei defekt."
        else:
            status["health"] = "Keine Health-Datei."

        await update_embed()

    except Exception as e:
        msg = f"❌ Fehler bei Plex-Verbindung: {e}"
        log_sync(msg, "REFRESH")
        status.update({"mode": "ERROR", "status_line": msg, "last_error": msg})
        write_health(False)
        await update_embed()
        return

    # MAIN LOOP
    while True:
        tgt = next_target_datetime()
        status.update({
            "mode": "IDLE",
            "status_line": f"Bereit – Plex {status['plex_name']}",
            "next_run": next_run_human(tgt)
        })
        log_sync(f"Nächster Lauf: {human_until(tgt)}", "SCHED")
        await update_embed()

        await asyncio.sleep(max(0, (tgt - dt.datetime.now()).total_seconds()))

        start_ts = dt.datetime.now()
        log_sync("=" * 80, "REFRESH")
        log_sync(f"SCAN START {start_ts:%d.%m.%Y %H:%M:%S}", "REFRESH")

        # Starte Performance-Monitoring
        perf_monitor.start_scan()
        log_sync("Performance-Monitoring AKTIVIERT", "PERF")

        status.update({
            "mode": "REFRESH",
            "next_run": "— Lauf aktiv —",
            "last_error": "",
            "status_line": (
                f"🔄 Läuft ({status['plex_name']}) – Lade…\n"
                f"Fortschritt: Berechne…\n"
                f"ETA: Berechne…"
            )
        })
        await update_embed()

        for n in ("failed.log", "dead.log", "recovered.log"):
            try:
                open(os.path.join(LOG_DIR, n), "w").close()
            except:
                pass

        stats_checked = stats_fixed = stats_failed = stats_skip = stats_new_dead = 0
        refreshed_libs = []

        time_limit = dt.timedelta(seconds=SCAN_TIME_LIMIT_SECONDS)

        try:
            sections = [s for s in plex.library.sections() if s.type in ("movie", "show")]
        except Exception as e:
            msg = f"Bibliotheken-Fehler: {e}"
            log_sync(msg, "REFRESH")
            status.update({"mode": "ERROR", "status_line": msg, "last_error": msg})
            write_health(False)
            await update_embed()
            continue

        total_secs = len(sections) or 1

        for idx, sec in enumerate(sections, start=1):

            if dt.datetime.now() - start_ts >= time_limit:
                log_sync("Zeitlimit erreicht – Abbruch.", "REFRESH")
                break

            lib = sec.title
            log_sync(f"Starte Library: {lib} ({idx}/{total_secs})", "REFRESH")

            # Progress Update mit ETA
            elapsed = (dt.datetime.now() - start_ts).total_seconds()
            progress = idx / total_secs
            bar_len = 12
            filled = int(progress * bar_len)
            bar = "█" * filled + "░" * (bar_len - filled)

            if progress > 0:
                total_est = elapsed / progress
                eta_sec = max(0, int(total_est - elapsed))
            else:
                eta_sec = 0

            if eta_sec < 60:
                eta_str = f"{eta_sec}s"
            else:
                m, s = divmod(eta_sec, 60)
                eta_str = f"{m}m {s:02d}s"

            status["status_line"] = (
                f"{lib} ({idx}/{total_secs})\n"
                f"Fortschritt: {bar} {int(progress * 100)}%\n"
                f"ETA: {eta_str}"
            )
            await update_embed()

            # EXCLUDE Libraries
            if lib in EXCLUDE_LIBRARIES:
                log_sync(f"[EXCLUDE] Bibliothek übersprungen: {lib}", "REFRESH")
                continue

            # Update Phase: Loading (VOR dem Laden!)
            perf_monitor.current_library = lib
            perf_monitor.set_phase("Loading", 0)

            start_load = time.time()

            # LADEN - muss leider komplett sein (PlexAPI Limitation)
            try:
                loop = asyncio.get_running_loop()
                all_items = await loop.run_in_executor(
                    None, lambda s=sec: s.all(sort="updatedAt:desc")
                )
            except Exception as e:
                log_sync(f"{lib} – Fehler beim Laden: {e}", "REFRESH")
                perf_monitor.set_phase("Error", 0)
                await asyncio.sleep(10)
                continue

            load_time = time.time() - start_load
            item_count = len(all_items) if all_items else 0

            log_sync(
                f"{lib} geladen ({item_count} Items, {load_time:.1f}s)",
                "REFRESH"
            )

            # Update Performance Monitor - NACH dem Laden
            perf_monitor.update_library(lib, item_count)
            perf_monitor.set_phase("Processing", item_count)

            # CHUNKED PROCESSING – MEMORY OPTIMIERT
            now_dt = dt.datetime.now()
            lookback = now_dt - dt.timedelta(days=SMART_LOOKBACK_DAYS)

            ready_list = []
            new_list = []
            changed_list = []

            # Verarbeite in Chunks um Memory-Druck zu reduzieren
            for chunk in process_items_in_chunks(all_items, CHUNK_SIZE):
                for itm in chunk:
                    rk = itm.ratingKey
                    upd = getattr(itm, "updatedAt", None)
                    upd_iso = upd.isoformat() if upd else ""
                    row = db_get_media(rk)

                    if row and row["ignore_until"]:
                        try:
                            ign = dt.datetime.fromisoformat(row["ignore_until"])
                            if ign > now_dt and row["state"] in ("cooldown", "dead"):
                                stats_skip += 1
                                continue
                        except:
                            pass

                    is_new = row is None
                    is_changed = (
                        upd_iso and row and
                        upd_iso != (row["last_updated_at"] or "") and
                        upd and upd >= lookback
                    )
                    ready_problem = (
                        row and row["state"] in ("cooldown", "dead") and
                        (not row["ignore_until"] or
                         dt.datetime.fromisoformat(row["ignore_until"]) <= now_dt)
                    )

                    if ready_problem:
                        ready_list.append(itm)
                    elif is_new:
                        new_list.append(itm)
                    elif is_changed:
                        changed_list.append(itm)

                    if len(ready_list) + len(new_list) + len(changed_list) >= MAX_ITEMS_PER_RUN:
                        break

                # Chunk freigeben
                chunk.clear()

                if len(ready_list) + len(new_list) + len(changed_list) >= MAX_ITEMS_PER_RUN:
                    break

            # all_items kann jetzt freigegeben werden
            all_items.clear()
            del all_items

            selected = (ready_list + new_list + changed_list)[:MAX_ITEMS_PER_RUN]
            if not selected:
                log_sync(f"{lib}: keine Items.", "REFRESH")
                continue

            # PROCESS ITEMS
            fixed_lib = 0

            for itm in selected:

                if dt.datetime.now() - start_ts >= time_limit:
                    break

                if await plex_is_scanning_async(plex):
                    await wait_until_plex_ready(plex)

                stats_checked += 1

                rk = itm.ratingKey
                upd = getattr(itm, "updatedAt", None)
                upd_iso = upd.isoformat() if upd else ""
                row = db_get_media(rk)

                # RECOVERED
                if row and row["state"] == "dead" and upd_iso != (row["last_updated_at"] or ""):
                    asyncio.create_task(
                        log_extra_batch("recovered.log",
                                  f"RECOVERED | {lib} | {rk} | {itm.title}")
                    )
                    db_upsert_media(rk, lib, upd_iso, 0, "active", None, "recovered")
                    row = db_get_media(rk)

                need, info = needs_refresh(itm)
                if not need:
                    db_upsert_media(rk, lib, upd_iso, 0, "active", None, None)
                    continue

                ok = False
                try:
                    ok = await refresh_item_and_check(plex, itm)
                except:
                    status["mode"] = "PAUSE"
                    status["status_line"] = "⏸️ Plex offline."
                    await update_embed()
                    await asyncio.sleep(10)
                    continue

                if ok:
                    fixed_lib += 1
                    stats_fixed += 1
                    db_upsert_media(rk, lib, upd_iso, 0, "active", None, "fixed")
                    continue

                # TMDB versuchen
                tmdb_id = tmdb_find_guid_for_item(itm)
                if tmdb_id:
                    try:
                        if set_guid(itm, tmdb_id):
                            fixed_lib += 1
                            stats_fixed += 1
                            db_upsert_media(
                                rk, lib, upd_iso, 0, "active", None, f"guid:{tmdb_id}"
                            )
                            continue
                    except:
                        status["mode"] = "PAUSE"
                        status["status_line"] = "⏸️ Plex offline."
                        await update_embed()
                        await asyncio.sleep(10)
                        continue

                stats_failed += 1
                row = db_get_media(rk)
                _, dead = handle_failed_item(lib, rk, info, row, upd_iso)
                if dead:
                    stats_new_dead += 1

            # Listen freigeben
            ready_list.clear()
            new_list.clear()
            changed_list.clear()
            selected.clear()

            # Phase abgeschlossen
            perf_monitor.set_phase("Completed", item_count)

            if fixed_lib > 0:
                refreshed_libs.append(f"• {lib}: {fixed_lib} gefixt")

        # Explizites Garbage Collection nach großem Run
        gc.collect()
        
        # SQLite WAL Checkpoint (verhindert endlos wachsende WAL-Files)
        try:
            with db_pool.get_connection() as c:
                c.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            log_sync("SQLite WAL Checkpoint durchgeführt", "DB")
        except Exception as e:
            log_sync(f"WAL Checkpoint Fehler: {e}", "DB")

        # Beende Performance-Monitoring
        perf_monitor.end_scan()
        log_sync("Performance-Monitoring BEENDET", "PERF")
        
        # Schreibe Abschluss-Separator ins Live-Log
        try:
            path = os.path.join(LOG_DIR, "performance_live.log")
            separator = (
                "=" * 80 + "\n"
                f"SCAN COMPLETED | Duration: {format_dur(duration)} | "
                f"Peak RAM: {perf_monitor.peak_ram:.0f} MB | "
                f"Avg CPU: {(sum(cpu_vals) / len(cpu_vals) if cpu_vals else 0):.1f}%\n"
                + "=" * 80 + "\n\n"
            )
            with open(path, "a") as f:
                f.write(separator)
                f.flush()
        except Exception as e:
            log_sync(f"Live-Log Separator Fehler: {e}", "PERF")

        # SCAN ENDE
        end_ts = dt.datetime.now()
        duration = (end_ts - start_ts).total_seconds()

        avg_cpu = sum(cpu_vals) / len(cpu_vals) if cpu_vals else 0
        peak_cpu = cpu_peak

        cpu_vals.clear()
        cpu_peak = 0.0

        # Performance Stats sammeln
        perf_stats = {
            'checked': stats_checked,
            'fixed': stats_fixed,
            'failed': stats_failed,
            'skipped': stats_skip,
            'new_dead': stats_new_dead
        }

        # Schreibe Performance Summary (ohne Timestamp im summary selbst)
        summary = perf_monitor.get_summary(perf_stats)
        
        # Schreibe Summary direkt in Datei
        path = os.path.join(LOG_DIR, "performance_summary.log")
        with open(path, "a") as f:
            f.write(summary)
            f.flush()

        main_line = (
            f"{stats_fixed} gefixt · "
            f"{stats_failed} fehlgeschlagen · "
            f"{stats_checked} geprüft · Dauer: {format_dur(duration)}"
        )

        status["last_refresh"] = f"{start_ts:%d.%m.%Y %H:%M:%S} – {main_line}"
        status["last_refresh_details"] = "\n".join(refreshed_libs)
        status["cpu_line"] = f"⌀ {avg_cpu:.1f}% / Peak {peak_cpu:.1f}%"
        status["status_line"] = f"🏁 Refresh abgeschlossen ({status['plex_name']})"
        status["mode"] = "IDLE"

        tgt = next_target_datetime()
        status["next_run"] = next_run_human(tgt)

        total_dead = db_count_dead_total()
        ts_now = dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        status["stats_timestamp"] = ts_now

        status["stats_block"] = (
            f"• Geprüft: {stats_checked}\n"
            f"• Gefixt: {stats_fixed}\n"
            f"• Fehlgeschlagen: {stats_failed}\n"
            f"• Übersprungen: {stats_skip}\n"
            f"• Unfixbar: {stats_new_dead} neu / {total_dead} gesamt"
        )

        write_health(True)
        await update_embed()

        log_sync(f"SCAN ENDE {end_ts:%d.%m.%Y %H:%M:%S} | {main_line}", "REFRESH")
        log_sync("=" * 80, "REFRESH")

# =====================================================================
# PERIODIC HEALTH CHECK
# =====================================================================

async def periodic_health(cb):
    while True:
        try:
            if not os.path.exists(HEALTH_FILE):
                status["health"] = "Keine Health-Datei."
            else:
                raw = open(HEALTH_FILE).read().strip()
                st, ts = raw.split("|")
                ld = dt.datetime.strptime(ts, "%d.%m.%Y %H:%M:%S")
                diff = (dt.datetime.now() - ld).days
                status["health"] = (
                    f"🚨 Letzter Erfolg vor {diff} Tagen!"
                    if st == "FAIL" or diff > HEALTHCHECK_MAX_DAYS
                    else f"OK (zuletzt: {ts})"
                )
        except Exception as e:
            status["health"] = f"Health-Fehler: {e}"

        await cb()
        await asyncio.sleep(HEALTHCHECK_INTERVAL_MINUTES * 60)

# =====================================================================
# RUNNER – ohne Discord
# =====================================================================

async def _runner_no_discord():
    log_sync("Starte ohne Discord-Bot…", "MAIN")
    asyncio.create_task(periodic_health(update_embed))
    await smart_refresh_loop()

# =====================================================================
# MAIN – Discord / Telegram / Standalone Boot
# =====================================================================

def main():
    global tg_bot, bot

    if not os.path.exists(HEALTH_FILE):
        open(HEALTH_FILE, "w").write("OK|" + dt.datetime.now().strftime("%d.%m.%Y %H:%M:%S"))
        status["health"] = "OK (initial)"

    # TELEGRAM BOT
    if ENABLE_TELEGRAM and ENABLE_TELEGRAM_IMPORT:
        try:
            tg_bot = TgBot(
                token=TELEGRAM_BOT_TOKEN,
                session=AiohttpSession()
            )
            print("[TELEGRAM] Telegram Bot gestartet", flush=True)
        except Exception as e:
            print(f"[TELEGRAM] Fehler: {e}", flush=True)
            tg_bot = None

    # DISCORD BOT
    if ENABLE_DISCORD and ENABLE_DISCORD_IMPORT:
        import logging
        logging.getLogger("discord").setLevel(logging.ERROR)
        logging.getLogger("discord.client").setLevel(logging.ERROR)
        logging.getLogger("discord.http").setLevel(logging.ERROR)
        logging.getLogger("discord.gateway").disabled = True
        logging.getLogger("discord.state").disabled = True

        intents = discord.Intents.none()
        intents.guilds = True
        intents.message_content = False

        bot = commands.Bot(command_prefix="!", intents=intents)

        @bot.event
        async def on_ready():
            log_sync("Discord online.", "DISCORD")
            asyncio.create_task(smart_refresh_loop())
            asyncio.create_task(periodic_health(update_embed))

        bot.run(DISCORD_TOKEN)
        return

    # STANDALONE
    loop = asyncio.get_event_loop()
    loop.run_until_complete(_runner_no_discord())

# =====================================================================
# CLEANUP ON EXIT
# =====================================================================

import atexit

def cleanup():
    """Cleanup beim Beenden"""
    global db_pool
    if db_pool:
        db_pool.close_all()
        log_sync("DB Pool geschlossen", "CLEANUP")

atexit.register(cleanup)

# =====================================================================
# RUN
# =====================================================================

if __name__ == "__main__":
    print("🚀 Starte Plex Smart-Refresher 4.4 Optimized Edition …", flush=True)
    print("📊 Memory Optimizations:", flush=True)
    print(f"  • Chunk Size: {CHUNK_SIZE} Items", flush=True)
    print(f"  • Max CPU Samples: {MAX_CPU_SAMPLES}", flush=True)
    print(f"  • DB Connection Pool: 3 Connections", flush=True)
    print(f"  • Log Batch Writing: Enabled", flush=True)
    main()
