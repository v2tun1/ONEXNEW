# ============================================================
# Railway Ready
# Designed by @Mehtif
# ============================================================
import asyncio
import base64
import hashlib
import json
import logging
import os
import secrets
import string
import time
from collections import defaultdict, deque
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote, parse_qs
import aiofiles
import httpx
import uvicorn
from fastapi import (
    FastAPI,
    Request,
    HTTPException,
    Depends,
)
from fastapi.responses import (
    Response,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    FileResponse,
)
from fastapi.middleware.cors import CORSMiddleware

# ============================================================
# APP
# ============================================================

APP_NAME = "ONEX"
APP_VERSION = "1.3.1"

SUPPORT_USERNAME = "@V2rayTun0"
SUPPORT_URL = "https://t.me/V2rayTun0"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(APP_NAME)

# ============================================================
# TIMEZONE
# ============================================================

try:
    from zoneinfo import ZoneInfo

    IRAN_TZ = ZoneInfo("Asia/Tehran")

except Exception:
    IRAN_TZ = None


# ============================================================
# RAILWAY
# ============================================================

PORT = int(
    os.environ.get(
        "PORT",
        "8000",
    )
)

DATA_DIR = Path(
    os.environ.get(
        "RAILWAY_VOLUME_MOUNT_PATH",
        os.environ.get(
            "DATA_DIR",
            "./data",
        ),
    )
)

DATA_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

DATA_FILE = DATA_DIR / "pixonpanel_state.json"
TG_FILE = DATA_DIR / "telegram_settings.json"

# Protocol artwork shipped with the panel UI. These are local static assets
# so the protocol picker does not depend on an external image host.
PROTOCOL_ICON_DIR = Path(__file__).resolve().parent / "protocol_icons"
PROTOCOL_ICON_FILES = {
    "vless-ws": PROTOCOL_ICON_DIR / "onex-wb.png",
    "xhttp-packet-up": PROTOCOL_ICON_DIR / "onex-xhttp.png",
    "xhttp-stream-up": PROTOCOL_ICON_DIR / "onex-gamig.png",
    "xhttp-stream-one": PROTOCOL_ICON_DIR / "onex-stream.png",
}

SECRET_FILE = DATA_DIR / "pixonpanel_secret.key"

# ============================================================
# PANEL UPDATES
# ============================================================
# Public release metadata lives in the GitHub repository.
# Railway credentials stay server-side in environment variables.
UPDATE_REPO = os.environ.get("ONEX_UPDATE_REPO", "HajMeTiV2/ONEX").strip()
UPDATE_BRANCH = os.environ.get("ONEX_UPDATE_BRANCH", "main").strip() or "main"
UPDATE_VERSION_URL = f"https://raw.githubusercontent.com/{UPDATE_REPO}/{UPDATE_BRANCH}/version.json"
UPDATE_GITHUB_API = f"https://api.github.com/repos/{UPDATE_REPO}"
RAILWAY_API_URL = os.environ.get("RAILWAY_API_URL", "https://backboard.railway.com/graphql/v2").strip()
RAILWAY_API_TOKEN = os.environ.get("RAILWAY_API_TOKEN", "").strip()
RAILWAY_SERVICE_ID = os.environ.get("RAILWAY_SERVICE_ID", "").strip()
RAILWAY_ENVIRONMENT_ID = os.environ.get("RAILWAY_ENVIRONMENT_ID", "").strip()
ONEX_CURRENT_COMMIT_SHA = os.environ.get("RAILWAY_GIT_COMMIT_SHA", os.environ.get("ONEX_COMMIT_SHA", "")).strip()


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    docs_url=None,
    redoc_url=None,
)


@app.get("/api/onex-logo-3d.png", include_in_schema=False)
async def onex_logo_3d():
    """Serve the approved high-detail ONEX 3D brand mark."""
    path = BASE_DIR / "onex-logo-3d.png"
    if not path.is_file():
        raise HTTPException(status_code=404, detail="ONEX logo not found")
    return FileResponse(path, media_type="image/png", headers={"Cache-Control": "public, max-age=31536000, immutable"})


@app.get("/api/protocol-icon/{protocol_id}.png", include_in_schema=False)
async def protocol_icon(protocol_id: str):
    """Serve a bundled protocol icon for the create-config picker."""
    path = PROTOCOL_ICON_FILES.get(protocol_id)
    if not path or not path.is_file():
        raise HTTPException(status_code=404, detail="Protocol icon not found")
    return FileResponse(path, media_type="image/png", headers={"Cache-Control": "public, max-age=31536000, immutable"})

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# LOCKS
# ============================================================

SAVE_LOCK = asyncio.Lock()
LINKS_LOCK = asyncio.Lock()
SUBS_LOCK = asyncio.Lock()
SESSIONS_LOCK = asyncio.Lock()


# ============================================================
# SECRET
# ============================================================

def load_or_create_secret() -> str:
    env_secret = os.environ.get("SECRET_KEY")

    if env_secret:
        return env_secret

    try:
        if SECRET_FILE.exists():
            existing = (
                SECRET_FILE
                .read_text(
                    encoding="utf-8"
                )
                .strip()
            )

            if existing:
                return existing

        generated = secrets.token_urlsafe(48)

        SECRET_FILE.write_text(
            generated,
            encoding="utf-8",
        )

        return generated

    except Exception as exc:
        logger.warning(
            "Could not persist SECRET_KEY: %s",
            exc,
        )

        return secrets.token_urlsafe(48)


SECRET_KEY = load_or_create_secret()


# ============================================================
# CONFIG
# ============================================================

CONFIG = {
    "port": PORT,
    "secret": SECRET_KEY,
    "host": os.environ.get(
        "RAILWAY_PUBLIC_DOMAIN",
        "localhost",
    ),
}


# ============================================================
# STATE
# ============================================================

LINKS: dict = {}
SUBS: dict = {}
SESSIONS: dict = {}
connections: dict = {}
CATEGORIES: dict = {}

stats = {
    "total_bytes": 0,
    "total_requests": 0,
    "total_errors": 0,
    "upload_bytes": 0,
    "download_bytes": 0,
    "start_time": time.time(),
}

error_logs = deque(maxlen=100)
activity_logs = deque(maxlen=250)

hourly_traffic = defaultdict(int)
hourly_upload = defaultdict(int)

http_client: httpx.AsyncClient | None = None


# ============================================================
# PROTOCOL
# ============================================================

# Only advertise protocols for which this panel has a real server-side backend.
# VLESS is provided by relay_vless; XHTTP entries are added only after the
# optional xhttp_siz10 module is loaded successfully.  Do not put URL-only
# protocol names here: a generated URI is not enough to make a server support it.
PROTOCOLS: list[str] = []

PROTOCOL_LABELS = {
    "vless-ws": "ONEX WB",
    "xhttp-packet-up": "ONEX Xhttp",
    "xhttp-stream-up": "ONEX Gaming",
    "xhttp-stream-one": "ONEX Stream",
    "trojan": "Trojan",
}

PROTOCOL_ALIASES = {
    "vless": "vless-ws",
}

DEFAULT_PROTOCOL = "vless-ws"

FINGERPRINTS = (
    "chrome",
    "firefox",
    "safari",
    "ios",
    "android",
    "edge",
    "360",
    "qq",
    "random",
    "randomized",
)

DEFAULT_FINGERPRINT = "chrome"

DEFAULT_ALPN_BY_PROTOCOL = {
    "vless-ws": "http/1.1",
    "xhttp-packet-up": "h2,http/1.1",
    "xhttp-stream-up": "h2,http/1.1",
    "xhttp-stream-one": "h2,http/1.1",
}

DEFAULT_PORT = 443
MIN_PORT = 1
MAX_PORT = 65535

DEFAULT_SPEED_LIMIT = 0


def normalize_protocol(protocol: str | None) -> str:
    value = str(protocol or DEFAULT_PROTOCOL).strip().lower()
    value = PROTOCOL_ALIASES.get(value, value)
    if value in PROTOCOLS:
        return value
    return PROTOCOLS[0] if PROTOCOLS else DEFAULT_PROTOCOL


# ============================================================
# LOGGING
# ============================================================

def log_activity(
    kind: str,
    message: str,
    level: str = "info",
):
    activity_logs.append(
        {
            "kind": kind,
            "level": level,
            "message": message,
            "time": datetime.now().isoformat(),
        }
    )


# ============================================================
# HELPERS
# ============================================================

def escape_html(value) -> str:
    return (
        str(
            value
            if value is not None
            else ""
        )
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#039;")
    )


def safe_int(
    value,
    default=0,
    minimum=0,
    maximum=None,
):
    try:
        number = int(value)
    except Exception:
        number = default

    if number < minimum:
        number = minimum

    if maximum is not None and number > maximum:
        number = maximum

    return number


def safe_float(
    value,
    default=0.0,
    minimum=0.0,
):
    try:
        number = float(value)
    except Exception:
        number = default

    return max(
        minimum,
        number,
    )


def generate_uuid():
    value = secrets.token_hex(16)

    return (
        f"{value[:8]}-"
        f"{value[8:12]}-"
        f"{value[12:16]}-"
        f"{value[16:20]}-"
        f"{value[20:32]}"
    )


def random_config_name(existing=None):
    existing = existing or set()
    alphabet = string.ascii_lowercase + string.digits
    for _ in range(80):
        length = secrets.randbelow(6) + 8
        name = "".join(secrets.choice(alphabet) for _ in range(length))
        if name not in existing and name and not name[0].isdigit():
            return name
    return secrets.token_hex(6)

def sanitize_config_name(name: str) -> str:
    if not name:
        return random_config_name()
    # Keep the project/config separator so generated names remain readable.
    cleaned = "".join(
        ch for ch in str(name)
        if ch.isascii() and (ch.isalnum() or ch in "-_" )
    ).strip("-_ ")
    if not cleaned:
        return random_config_name()
    if cleaned[0].isdigit():
        cleaned = "a" + cleaned
    return cleaned[:40]

def auto_config_name() -> str:
    return random_config_name()


def project_config_name(existing=None) -> str:
    """Generate a unique config remark/name with the project prefix first."""
    existing = existing or set()
    for _ in range(80):
        name = f"{APP_NAME}-{random_config_name()}"
        if name not in existing:
            return name
    return f"{APP_NAME}-{secrets.token_hex(6)}"


def now_ir():
    if IRAN_TZ:
        return datetime.now(IRAN_TZ)

    return datetime.now()


def uptime():
    seconds = int(
        time.time()
        - stats["start_time"]
    )

    h = seconds // 3600

    m = (
        seconds
        % 3600
    ) // 60

    s = (
        seconds
        % 60
    )

    return (
        f"{h:02d}:"
        f"{m:02d}:"
        f"{s:02d}"
    )


def fmt_bytes(value: int):
    value = int(
        value or 0
    )

    if value < 1024:
        return f"{value} B"

    if value < 1024 ** 2:
        return (
            f"{value / 1024:.1f} KB"
        )

    if value < 1024 ** 3:
        return (
            f"{value / 1024 ** 2:.2f} MB"
        )

    return (
        f"{value / 1024 ** 3:.2f} GB"
    )


def parse_size_to_bytes(
    value: float,
    unit: str,
):
    if value <= 0:
        return 0

    unit = (
        unit
        or "GB"
    ).upper()

    if unit == "TB":
        return int(
            value
            * 1024 ** 4
        )

    if unit == "GB":
        return int(
            value
            * 1024 ** 3
        )

    if unit == "MB":
        return int(
            value
            * 1024 ** 2
        )

    if unit == "KB":
        return int(
            value
            * 1024
        )

    return int(value)


def parse_speed_to_bytes(
    value: float,
    unit: str,
):
    if value <= 0:
        return 0

    unit = (
        unit
        or "MBIT"
    ).upper()

    if unit == "MBIT":
        return int(
            value
            * 1024
            * 1024
            / 8
        )

    if unit == "KB":
        return int(
            value * 1024
        )

    if unit == "MB":
        return int(
            value
            * 1024
            * 1024
        )

    return int(value)


def is_link_expired(
    link: dict,
):
    expiry = link.get(
        "expires_at"
    )

    if not expiry:
        return False

    try:
        return (
            datetime.now()
            > datetime.fromisoformat(
                expiry
            )
        )

    except Exception:
        return False


def is_link_allowed(
    link: dict | None,
):
    if link is None:
        return False

    if not link.get(
        "active",
        True,
    ):
        return False

    if is_link_expired(link):
        return False

    limit = int(
        link.get(
            "limit_bytes",
            0,
        )
        or 0
    )

    used = int(
        link.get(
            "used_bytes",
            0,
        )
        or 0
    )

    if (
        limit > 0
        and used >= limit
    ):
        return False

    return True


def unique_ips_for_uuid(
    uuid: str,
):
    return {
        connection.get("ip")
        for connection in connections.values()
        if connection.get("uuid") == uuid
        and connection.get("ip")
    }


def client_ip(
    request: Request,
):
    forwarded = request.headers.get(
        "x-forwarded-for"
    )

    if forwarded:
        return (
            forwarded
            .split(",")[0]
            .strip()
        )

    real = request.headers.get(
        "x-real-ip"
    )

    if real:
        return real.strip()

    if request.client:
        return request.client.host

    return "unknown"


def is_ip_allowed(
    link: dict | None,
    uuid: str,
    ip: str,
):
    if link is None:
        return False

    limit = int(
        link.get(
            "ip_limit",
            0,
        )
        or 0
    )

    if limit <= 0:
        return True

    ips = unique_ips_for_uuid(uuid)

    if ip in ips:
        return True

    return len(ips) < limit


def get_host(
    request: Request | None = None,
) -> str:

    if request is not None:
        forwarded = request.headers.get(
            "x-forwarded-host"
        )

        normal = request.headers.get(
            "host"
        )

        host = (
            forwarded
            or normal
        )

        if host:
            host = host.split(":")[0].strip()

            CONFIG["host"] = host

            return host

    railway_domain = os.environ.get(
        "RAILWAY_PUBLIC_DOMAIN"
    )

    if railway_domain:
        return railway_domain

    return CONFIG["host"]


# ============================================================
# PASSWORD
# ============================================================

def hash_password(
    password: str,
) -> str:

    payload = (
        password
        + SECRET_KEY
    ).encode("utf-8")

    return hashlib.sha256(
        payload
    ).hexdigest()


# ONEX default owner credentials.
# First deployment starts with username=admin / password=admin.
# After the owner changes credentials from Settings, the saved values are used.
_env_pw = os.environ.get("ADMIN_PASSWORD", "").strip()
_env_user = os.environ.get("ADMIN_USERNAME", "admin").strip().lower() or "admin"
AUTH = {
    "username": _env_user,
    "password_hash": hash_password(_env_pw or "admin"),
    "password_configured": True,
    "credentials_version": 1,
}

# Sub-admin accounts (panel operators with granular permissions)
ADMIN_ACCOUNTS: dict = {}
# session_token -> {"role": "owner"|"admin", "admin_id": str|None, "username": str}
SESSION_META: dict = {}

ALL_PERMS = (
    "dash", "configs", "create", "stats", "logs",
    "settings", "support", "telegram", "news", "admins",
)
DEFAULT_PERMS = {p: True for p in ALL_PERMS}


def default_admin_record(username: str, password: str, **kwargs) -> dict:
    return {
        "id": secrets.token_hex(8),
        "username": username.strip().lower(),
        "password_hash": hash_password(password),
        "label": kwargs.get("label") or username,
        "limit_bytes": int(kwargs.get("limit_bytes") or 0),
        "used_bytes": 0,
        "expires_at": kwargs.get("expires_at"),
        "active": True,
        "blocked": False,
        "permissions": {**DEFAULT_PERMS, **(kwargs.get("permissions") or {})},
        "created_at": datetime.now().isoformat(),
    }


def find_admin_by_username(username: str):
    u = (username or "").strip().lower()
    for aid, a in ADMIN_ACCOUNTS.items():
        if a.get("username") == u:
            return aid, a
    return None, None


def admin_is_valid(admin: dict) -> bool:
    if not admin or admin.get("blocked") or not admin.get("active", True):
        return False
    exp = admin.get("expires_at")
    if exp:
        try:
            if datetime.now() > datetime.fromisoformat(str(exp)):
                return False
        except Exception:
            pass
    limit = int(admin.get("limit_bytes") or 0)
    used = int(admin.get("used_bytes") or 0)
    if limit > 0 and used >= limit:
        return False
    return True



# ============================================================
# LOGIN BRUTE-FORCE PROTECTION
# ============================================================
# Maximum failed login attempts per IP inside the rolling window.
LOGIN_MAX_ATTEMPTS = 5
LOGIN_WINDOW_SECONDS = 15 * 60
LOGIN_LOCKOUT_SECONDS = 30 * 60  # 30 minutes lockout
LOGIN_MIN_PASSWORD_LENGTH = 6

LOGIN_FAILURES = defaultdict(deque)
LOGIN_LOCKED_UNTIL = {}


def _cleanup_login_state(ip: str, now: float | None = None):
    now = now if now is not None else time.time()

    locked_until = LOGIN_LOCKED_UNTIL.get(ip, 0)
    if locked_until and locked_until <= now:
        LOGIN_LOCKED_UNTIL.pop(ip, None)

    failures = LOGIN_FAILURES.get(ip)
    if not failures:
        return

    cutoff = now - LOGIN_WINDOW_SECONDS
    while failures and failures[0] <= cutoff:
        failures.popleft()

    if not failures:
        LOGIN_FAILURES.pop(ip, None)


def login_is_blocked(ip: str):
    now = time.time()
    _cleanup_login_state(ip, now)

    locked_until = LOGIN_LOCKED_UNTIL.get(ip, 0)
    if locked_until > now:
        return True, max(1, int(locked_until - now))

    return False, 0


def register_login_failure(ip: str):
    now = time.time()
    _cleanup_login_state(ip, now)

    failures = LOGIN_FAILURES.setdefault(ip, deque())
    failures.append(now)

    if len(failures) >= LOGIN_MAX_ATTEMPTS:
        LOGIN_LOCKED_UNTIL[ip] = now + LOGIN_LOCKOUT_SECONDS
        failures.clear()
        log_activity(
            "auth",
            f"IP به دلیل تلاش‌های متعدد ورود ناموفق به مدت {LOGIN_LOCKOUT_SECONDS // 60} دقیقه مسدود شد: {ip}",
            "err",
        )
        return True, LOGIN_LOCKOUT_SECONDS

    return False, max(0, LOGIN_MAX_ATTEMPTS - len(failures))


def clear_login_failures(ip: str):
    LOGIN_FAILURES.pop(ip, None)
    LOGIN_LOCKED_UNTIL.pop(ip, None)


# ============================================================
# SESSION
# ============================================================

SESSION_COOKIE = "pixonpanel_session"

SESSION_TTL = (
    60
    * 60
    * 24
    * 365
)


async def create_session(meta: dict | None = None) -> str:

    token = secrets.token_urlsafe(48)

    async with SESSIONS_LOCK:
        SESSIONS[token] = (
            time.time()
            + SESSION_TTL
        )
        SESSION_META[token] = meta or {"role": "owner", "admin_id": None, "username": "owner"}

    return token


async def is_valid_session(
    token: str | None,
) -> bool:

    if not token:
        return False

    async with SESSIONS_LOCK:

        expiry = SESSIONS.get(token)

        if expiry is None:
            return False

        if expiry < time.time():

            SESSIONS.pop(
                token,
                None,
            )

            return False

        return True


async def destroy_session(
    token: str | None,
):
    if not token:
        return

    async with SESSIONS_LOCK:
        SESSIONS.pop(
            token,
            None,
        )
        SESSION_META.pop(token, None)


def get_session_meta(token: str | None) -> dict:
    if not token:
        return {"role": "owner", "admin_id": None, "username": "owner", "permissions": {p: True for p in ALL_PERMS}}
    meta = dict(SESSION_META.get(token) or {"role": "owner", "admin_id": None, "username": "owner"})
    if meta.get("role") == "owner":
        meta["permissions"] = {p: True for p in ALL_PERMS}
    else:
        aid = meta.get("admin_id")
        admin = ADMIN_ACCOUNTS.get(aid or "") or {}
        meta["permissions"] = {p: bool((admin.get("permissions") or {}).get(p, False)) for p in ALL_PERMS}
        meta["blocked"] = bool(admin.get("blocked"))
    return meta


def require_perm(perm: str):
    async def _dep(request: Request, token=Depends(require_auth)):
        meta = get_session_meta(token)
        if meta.get("role") == "owner":
            return token
        if not (meta.get("permissions") or {}).get(perm):
            raise HTTPException(status_code=403, detail="دسترسی به این بخش مجاز نیست")
        return token
    return _dep


async def require_auth(
    request: Request,
):
    token = request.cookies.get(
        SESSION_COOKIE
    )

    if not await is_valid_session(
        token
    ):
        raise HTTPException(
            status_code=401,
            detail="unauthorized",
        )

    meta = get_session_meta(token)
    if meta.get("role") == "admin":
        aid = meta.get("admin_id")
        admin = ADMIN_ACCOUNTS.get(aid or "")
        if not admin_is_valid(admin or {}):
            await destroy_session(token)
            raise HTTPException(status_code=401, detail="حساب منقضی یا مسدود شده است")

    return token


def set_auth_cookie(
    response,
    request: Request,
    token: str,
):
    forwarded_proto = (
        request.headers
        .get(
            "x-forwarded-proto",
            "",
        )
        .lower()
    )

    is_https = (
        forwarded_proto == "https"
        or request.url.scheme == "https"
    )

    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        max_age=SESSION_TTL,
        httponly=True,
        samesite="lax",
        path="/",
        secure=is_https,
    )


# ============================================================
# VLESS LINK GENERATION
# ============================================================

# Native sing-box listener ports are shared by all accounts.  The panel
# stores credentials per UUID while the native core keeps one listener per
# protocol.  This is what makes the "all protocols" subscription a single
# account instead of creating unrelated accounts.
def protocol_public_port(link: dict | None, protocol: str, fallback: int = DEFAULT_PORT) -> int:
    if (link or {}).get("all_protocols") and protocol not in getattr(NATIVE_CORE, "SUPPORTED", ()):
        return safe_int((link or {}).get("port", fallback), fallback, MIN_PORT, MAX_PORT)
    if protocol in {"vless-ws", "xhttp-packet-up", "xhttp-stream-up", "xhttp-stream-one"}:
        return safe_int((link or {}).get("port", fallback), fallback, MIN_PORT, MAX_PORT)
    try:
        adv_ports = ((link or {}).get("advanced") or {}).get("ports") or []
        native_supported = list(getattr(NATIVE_CORE, "SUPPORTED", ()))
        if adv_ports and protocol in native_supported:
            if (link or {}).get("all_protocols"):
                try: idx = native_supported.index(protocol)
                except ValueError: idx = 0
                if idx < len(adv_ports):
                    return safe_int(adv_ports[idx], fallback, MIN_PORT, MAX_PORT)
            else:
                return safe_int(adv_ports[0], fallback, MIN_PORT, MAX_PORT)
        ports = NATIVE_CORE.public_ports()  # type: ignore[name-defined]
        return safe_int(ports.get(protocol, fallback), fallback, MIN_PORT, MAX_PORT)
    except Exception:
        return safe_int((link or {}).get("port", fallback), fallback, MIN_PORT, MAX_PORT)

def generate_vless_link(
    uuid: str, host: str, remark: str = "ONEX",
    protocol: str = DEFAULT_PROTOCOL, fingerprint: str | None = None,
    alpn: str | None = None, port: int | None = None, link: dict | None = None,
):
    protocol = normalize_protocol(protocol)
    fp = (fingerprint or DEFAULT_FINGERPRINT).strip().lower()
    if fp not in FINGERPRINTS: fp = DEFAULT_FINGERPRINT
    port_value = protocol_public_port(link, protocol, safe_int(port, DEFAULT_PORT, MIN_PORT, MAX_PORT))
    alpn_value = (alpn or DEFAULT_ALPN_BY_PROTOCOL.get(protocol, "http/1.1")).strip()
    label = quote(str(remark or "ONEX"), safe="")
    adv = normalize_advanced_config((link or {}).get("advanced"))
    adv_host = adv["host"].get("host") or adv["host"].get("address") or host
    adv_path = adv["host"].get("path") or adv["network"].get("path")
    adv_sni = adv["tls"].get("sni") or adv["tls"].get("server_name") or host
    adv_fp = adv["fingerprint"].get("value") or fp
    adv_alpn = adv["tls"].get("alpn") or alpn_value
    security = adv["tls"].get("mode") if adv["tls"].get("enabled", True) else "none"
    if security not in {"none","tls","reality"}: security = "tls"
    # Trojan requires TLS; the four VLESS transports use the panel relay.
    if protocol == "trojan" and security == "none":
        security = "tls"
    if protocol == "vless-ws":
        path = adv_path or f"/ws/{uuid}"
        q = {"encryption":"none","security":security,"type":"ws","host":adv_host,"path":path,"sni":adv_sni,"fp":adv_fp,"alpn":adv_alpn}
        if adv["tls"].get("allow_insecure"): q["allowInsecure"] = "1"
        if security == "reality":
            r=adv["tls"]["reality"]
            if r.get("public_key"): q["pbk"]=r["public_key"]
            if r.get("short_id"): q["sid"]=r["short_id"]
        return "vless://" + uuid + "@" + host + ":" + str(port_value) + "?" + "&".join(f"{k}={quote(str(v), safe=',/') }" for k,v in q.items()) + "#" + label
    if protocol.startswith("xhttp-"):
        mode = protocol.replace("xhttp-", "")
        path = adv_path or f"/xhttp-siz10/{mode}/{uuid}"
        q = {"encryption":"none","security":security,"type":"xhttp","mode":mode,"host":adv_host,"path":path,"sni":adv_sni,"fp":adv_fp,"alpn":adv_alpn}
        if adv["tls"].get("allow_insecure"): q["allowInsecure"] = "1"
        return "vless://" + uuid + "@" + host + ":" + str(port_value) + "?" + "&".join(f"{k}={quote(str(v), safe=',/') }" for k,v in q.items()) + "#" + label
    if protocol == "trojan":
        mode = security if security in {"tls", "none"} else "tls"
        q = {"security": mode, "sni": adv_sni}
        if adv_alpn: q["alpn"] = adv_alpn
        if adv["tls"].get("allow_insecure") or getattr(NATIVE_CORE, "self_signed", False): q["allowInsecure"] = "1"
        net = str(adv["network"].get("type") or "tcp")
        if net != "tcp":
            q["type"] = net
            if adv_path: q["path"] = adv_path
            if adv_host: q["host"] = adv_host
            if adv["host"].get("service_name"): q["serviceName"] = adv["host"]["service_name"]
        return f"trojan://{uuid}@{host}:{port_value}?" + "&".join(f"{k}={quote(str(v), safe=',/') }" for k,v in q.items()) + "#" + label
    return f"vless://{uuid}@{host}:{port_value}"

def vless_link_for_link(
    link: dict,
    uid: str,
    host: str,
):
    return generate_vless_link(
        uid,
        host,
        remark=str(link.get("label") or "Config"),
        protocol=link.get(
            "protocol",
            DEFAULT_PROTOCOL,
        ),
        fingerprint=link.get(
            "fingerprint",
            DEFAULT_FINGERPRINT,
        ),
        alpn=link.get(
            "alpn"
        ),
        port=protocol_public_port(link, link.get("protocol", DEFAULT_PROTOCOL), link.get("port", DEFAULT_PORT)),
        link=link,
    )


def group_subscription_lines_for_link(
    link: dict, uid: str, host: str, protocols, used_names: set[str] | None = None,
):
    """Expand one group member into every protocol enabled for the group."""
    selected = [normalize_protocol(str(p)) for p in (protocols or [])]
    selected = [p for p in selected if p in PROTOCOLS]
    if not selected:
        return []
    names = used_names if used_names is not None else set()
    cfg_count = 1 if link.get("all_protocols") else max(1, min(40, int(link.get("config_count") or 1)))
    clean_ips = list(link.get("clean_ips") or [])
    if clean_ips:
        hosts = []
        while len(hosts) < cfg_count:
            hosts.extend(clean_ips)
        hosts = hosts[:cfg_count]
    else:
        hosts = [host] * cfg_count
    lines = []
    base_label = str(link.get("label") or "Config")
    for index, target_host in enumerate(hosts, 1):
        for proto in selected:
            remark = f"{base_label} | {PROTOCOL_LABELS.get(proto, proto)}"
            if cfg_count > 1:
                remark += f" #{index}"
            if remark in names:
                n = 2
                candidate = f"{remark} ({n})"
                while candidate in names:
                    n += 1
                    candidate = f"{remark} ({n})"
                remark = candidate
            names.add(remark)
            lines.append(generate_vless_link(
                uid, target_host, remark=remark, protocol=proto,
                fingerprint=link.get("fingerprint", DEFAULT_FINGERPRINT),
                alpn=DEFAULT_ALPN_BY_PROTOCOL.get(proto, link.get("alpn")),
                port=protocol_public_port(link, proto, link.get("port", DEFAULT_PORT)),
                link=link,
            ))
    return lines


def get_link_info(
    link: dict,
    uid: str,
    host: str,
):
    connected_count = len(unique_ips_for_uuid(uid))
    is_active = is_link_allowed(link)
    limit_b = int(link.get("limit_bytes", 0) or 0)
    used_b = int(link.get("used_bytes", 0) or 0)
    is_expired = is_link_expired(link) or (limit_b > 0 and used_b >= limit_b)
    if not is_active or is_expired:
        status_color = "red"
    elif connected_count > 0:
        status_color = "green"
    else:
        status_color = "gray"
    clean_ips = link.get("clean_ips") or []
    cfg_count = int(link.get("config_count") or 1)
    show_vless = len(clean_ips) <= 1 and cfg_count <= 1
    cat = CATEGORIES.get(str(link.get("category_id") or "0")) or {}
    return {
        "uuid": uid,
        "name": link.get("label", ""),
        "label": link.get("label", ""),
        "protocol": link.get("protocol", DEFAULT_PROTOCOL),
        "active": is_active,
        "used_bytes": used_b,
        "limit_bytes": limit_b,
        "expires_at": link.get("expires_at"),
        "ip_limit": int(link.get("ip_limit", 0) or 0),
        "speed_limit_bytes": int(link.get("speed_limit_bytes", 0) or 0),
        "connection_limit": int(link.get("connection_limit", 0) or 0),
        "fragment": link.get("fragment", "off"),
        "fingerprint": link.get("fingerprint", DEFAULT_FINGERPRINT),
        "alpn": link.get("alpn", ""),
        "port": link.get("port", DEFAULT_PORT),
        "note": link.get("note", ""),
        "clean_ips": clean_ips,
        "alarm_enabled": bool(link.get("alarm_enabled", False)),
        "category_id": str(link.get("category_id") or "0"),
        "sort_order": int(link.get("sort_order") or 0),
        "category_number": int(cat.get("number", 0)),
        "category_name": str(cat.get("name", "عمومی")),
        "sub_id": str(link.get("sub_id") or ""),
        "config_count": cfg_count,
        "status_color": status_color,
        "connected_ips": connected_count,
        "show_vless": show_vless,
        "vless": vless_link_for_link(link, uid, host) if show_vless else "",
        "vless_full": vless_link_for_link(link, uid, host),
        "sub": f"https://{host}/sub/{uid}",
        "info": f"https://{host}/info/{uid}",
        "support": SUPPORT_USERNAME,
        "advanced": normalize_advanced_config(link.get("advanced")),
    }


# ============================================================
# PERSISTENCE
# ============================================================

def _migrate_retired_protocols() -> None:
    """Keep persisted links and groups aligned with the five supported protocols.

    Older state files may contain IDs for protocols no longer shipped by this
    ONEX2 build. Those records are migrated to the default VLESS WebSocket
    protocol, and group/bundle allowlists are restricted to supported IDs.
    """
    allowed = set(PROTOCOLS) or {DEFAULT_PROTOCOL}
    for link in LINKS.values():
        if not isinstance(link, dict):
            continue
        protocol = str(link.get("protocol") or DEFAULT_PROTOCOL)
        if protocol not in allowed:
            protocol = DEFAULT_PROTOCOL
            link["protocol"] = protocol
        link["protocol_label"] = PROTOCOL_LABELS.get(protocol, protocol)
        if isinstance(link.get("bundle_protocols"), list):
            link["bundle_protocols"] = [p for p in link["bundle_protocols"] if str(p) in allowed]
    for sub in SUBS.values():
        if not isinstance(sub, dict):
            continue
        raw = sub.get("protocols")
        if isinstance(raw, list):
            cleaned = [str(proto) for proto in raw if str(proto) in allowed]
            sub["protocols"] = cleaned if cleaned or not raw else list(PROTOCOLS)
        else:
            sub["protocols"] = list(PROTOCOLS)


async def load_state():

    global AUTH

    try:

        DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        if not DATA_FILE.exists():
            return

        async with aiofiles.open(
            DATA_FILE,
            "r",
            encoding="utf-8",
        ) as file:
            raw = await file.read()

        data = json.loads(raw)

        LINKS.update(
            data.get(
                "links",
                {},
            )
        )

        SUBS.update(
            data.get(
                "subs",
                {},
            )
        )

        _migrate_retired_protocols()

        CATEGORIES.update(
            data.get(
                "categories",
                {},
            )
        )

        ADMIN_ACCOUNTS.clear()
        ADMIN_ACCOUNTS.update(data.get("admin_accounts") or {})

        stored_password = data.get(
            "password_hash"
        )
        stored_username = str(data.get("username") or "").strip().lower()
        stored_cred_version = int(data.get("credentials_version") or 0)

        # One-time migration from the old PX/ONEX setup screen.
        # Existing legacy credentials are intentionally replaced with admin/admin.
        if stored_cred_version < 1:
            AUTH["username"] = "admin"
            AUTH["password_hash"] = hash_password("admin")
            AUTH["password_configured"] = True
            AUTH["credentials_version"] = 1
            logger.info("Legacy credentials migrated to ONEX default admin/admin")
        else:
            if stored_username:
                AUTH["username"] = stored_username
            if stored_password:
                AUTH["password_hash"] = stored_password
            AUTH["password_configured"] = True
            AUTH["credentials_version"] = stored_cred_version

        # Remove the legacy automatically-created default config.
        await remove_legacy_default_links()

        # Compatibility for older records
        for uid, link in LINKS.items():

            link.setdefault(
                "protocol",
                DEFAULT_PROTOCOL,
            )

            link.setdefault(
                "fingerprint",
                DEFAULT_FINGERPRINT,
            )

            link.setdefault(
                "alpn",
                "",
            )

            link.setdefault(
                "port",
                DEFAULT_PORT,
            )

            link.setdefault(
                "ip_limit",
                0,
            )

            link.setdefault(
                "speed_limit_bytes",
                0,
            )

            link.setdefault(
                "connection_limit",
                0,
            )

            link.setdefault(
                "fragment",
                "off",
            )

            link.setdefault(
                "used_bytes",
                0,
            )
            link.setdefault("clean_ips", [])
            link.setdefault("alarm_enabled", False)
            link.setdefault("category_id", "0")
            link.setdefault("config_count", 1)
            link.setdefault("sort_order", 0)
            link.setdefault("usage_history", [])
            link["advanced"] = normalize_advanced_config(link.get("advanced"))

        logger.info(
            "State loaded: %d links / %d subscriptions",
            len(LINKS),
            len(SUBS),
        )

    except Exception as exc:

        logger.exception(
            "Could not load state: %s",
            exc,
        )


async def save_state():

    async with SAVE_LOCK:

        try:

            DATA_DIR.mkdir(
                parents=True,
                exist_ok=True,
            )

            payload = {
                "links":
                    dict(LINKS),

                "subs":
                    dict(SUBS),

                "categories":
                    dict(CATEGORIES),

                "admin_accounts":
                    dict(ADMIN_ACCOUNTS),

                "username":
                    AUTH.get("username", "admin"),

                "password_hash":
                    AUTH[
                        "password_hash"
                    ],

                "credentials_version":
                    1,

                "saved_at":
                    datetime.now().isoformat(),
            }

            temp_file = (
                DATA_FILE.with_suffix(
                    ".tmp"
                )
            )

            async with aiofiles.open(
                temp_file,
                "w",
                encoding="utf-8",
            ) as file:

                await file.write(
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        indent=2,
                    )
                )

            temp_file.replace(
                DATA_FILE
            )

        except Exception as exc:

            logger.exception(
                "Could not save state: %s",
                exc,
            )


# ============================================================
# DEFAULT LINK
# ============================================================

async def ensure_default_categories():
    # گروه‌های پیش‌فرض ساخته نمی‌شوند — کاربر خودش می‌سازد
    return


async def remove_legacy_default_links():
    """Remove the old automatically-created default config from persisted state."""
    removed = []
    async with LINKS_LOCK:
        for uid, link in list(LINKS.items()):
            if link.get("is_default") or str(link.get("label") or "").strip() == "لینک پیش‌فرض":
                removed.append(uid)
                del LINKS[uid]
    if removed:
        logger.info("Removed %d legacy default link(s)", len(removed))


# ============================================================
# LINK MANAGEMENT
# ============================================================

async def make_link(
    label: str = "لینک جدید",
    limit_bytes: int = 0,
    expires_at: str | None = None,
    note: str = "",
    sub_id: str | None = None,
    protocol: str = DEFAULT_PROTOCOL,
    fingerprint: str = DEFAULT_FINGERPRINT,
    alpn: str = "",
    port: int = DEFAULT_PORT,
    ip_limit: int = 0,
    speed_limit_bytes: int = 0,
    connection_limit: int = 0,
    fragment: str = "off",
    clean_ips=None,
    alarm_enabled: bool = False,
    category_id: str = "0",
    config_count: int = 1,
    all_protocols: bool = False,
    advanced: dict | None = None,
):

    if not PROTOCOLS:
        raise HTTPException(503, "No protocol backend is available")

    protocol = normalize_protocol(protocol)

    fingerprint = (
        fingerprint
        or DEFAULT_FINGERPRINT
    ).strip().lower()

    if fingerprint not in FINGERPRINTS:
        fingerprint = DEFAULT_FINGERPRINT

    if not (
        MIN_PORT
        <= port
        <= MAX_PORT
    ):
        port = DEFAULT_PORT

    uid = generate_uuid()

    clean_label = sanitize_config_name((label or "").strip() or random_config_name())
    project_prefix = f"{APP_NAME}-"
    if not clean_label.lower().startswith(project_prefix.lower()):
        clean_label = f"{project_prefix}{clean_label}"

    record = {
        "label":
            clean_label[:40],

        "limit_bytes":
            max(
                0,
                int(limit_bytes),
            ),

        "used_bytes":
            0,

        "created_at":
            datetime.now().isoformat(),

        "active":
            True,

        "expires_at":
            expires_at,

        "note":
            (
                note
                or ""
            ).strip()[:500],

        "sub_id":
            sub_id,

        "protocol":
            protocol,

        "fingerprint":
            fingerprint,

        "alpn":
            (
                alpn
                or ""
            ).strip()[:100],

        "port":
            port,

        "ip_limit":
            max(
                0,
                int(ip_limit),
            ),

        "speed_limit_bytes":
            max(
                0,
                int(speed_limit_bytes),
            ),

        "connection_limit":
            max(
                0,
                int(connection_limit),
            ),

        "fragment":
            (
                fragment
                or "off"
            ).strip().lower(),

        "security_profile": "balanced",
        "multi_login": False,
        "protocol_label": PROTOCOL_LABELS.get(protocol, protocol),
        "clean_ips": list(clean_ips or []),
        "alarm_enabled": bool(alarm_enabled),
        "category_id": str(category_id or "0"),
        "config_count": max(1, min(40, int(config_count or 1))),
        "all_protocols": bool(all_protocols),
        "advanced": normalize_advanced_config(advanced),
        "native_protocols": [p for p in PROTOCOLS if p == "trojan"],
        "usage_history": [],
    }

    async with LINKS_LOCK:
        LINKS[uid] = record

    if sub_id:

        async with SUBS_LOCK:

            if sub_id in SUBS:

                ids = SUBS[
                    sub_id
                ].setdefault(
                    "link_ids",
                    [],
                )

                if uid not in ids:
                    ids.append(uid)

    await save_state()

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{record['label']}» "
            f"ساخته شد"
        ),
        "ok",
    )

    return uid, record


async def remove_link(
    uid: str,
):

    async with LINKS_LOCK:

        if uid not in LINKS:
            return None

        label = LINKS[
            uid
        ].get(
            "label",
            uid,
        )

        sub_id = LINKS[
            uid
        ].get(
            "sub_id"
        )

        del LINKS[uid]

    if sub_id:

        async with SUBS_LOCK:

            if sub_id in SUBS:

                ids = SUBS[
                    sub_id
                ].get(
                    "link_ids",
                    [],
                )

                if uid in ids:
                    ids.remove(uid)

    await save_state()

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{label}» "
            f"حذف شد"
        ),
        "warn",
    )

    return label


async def set_link_active(
    uid: str,
    active: bool,
):

    async with LINKS_LOCK:

        if uid not in LINKS:
            return None

        LINKS[
            uid
        ][
            "active"
        ] = bool(active)

        record = LINKS[uid]

    await save_state()
    if NATIVE_CORE and not await sync_native_core():
        async with LINKS_LOCK:
            LINKS[uid]["active"] = not bool(active)
        await save_state()
        raise HTTPException(409, NATIVE_CORE.last_error or "Native runtime reload failed; previous state restored")

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{record['label']}» "
            f"{'فعال' if active else 'غیرفعال'} شد"
        ),
        "ok"
        if active
        else "warn",
    )

    return record


# ============================================================
# SUB GROUPS
# ============================================================

async def create_sub_group(
    name: str = "گروه جدید",
    desc: str = "",
    password: str = "",
):

    name = (
        name
        or "گروه جدید"
    ).strip()[:60]

    desc = (
        desc
        or ""
    ).strip()[:200]

    password = (
        password
        or ""
    ).strip()

    sub_id = generate_uuid()

    uuid_key = secrets.token_urlsafe(16)

    record = {
        "name":
            name,

        "desc":
            desc,

        "password_hash":
            (
                hash_password(password)
                if password
                else None
            ),

        "uuid_key":
            uuid_key,

        "created_at":
            datetime.now().isoformat(),

        # Group activity is independent from the number/state of its configs.
        # A newly-created group is active by default; configs are its contents,
        # not its on/off switch.
        "active":
            True,

        "link_ids":
            [],

        # Protocol visibility rules for this subscription group.
        # Existing groups without this field remain backward-compatible (all protocols).
        "protocols": list(PROTOCOLS),
    }

    async with SUBS_LOCK:
        SUBS[sub_id] = record

    await save_state()

    log_activity(
        "sub",
        (
            f"گروه "
            f"«{name}» "
            f"ساخته شد"
        ),
        "ok",
    )

    return (
        sub_id,
        record,
    )


async def set_link_sub(
    uid: str,
    sub_id: str | None,
):

    async with LINKS_LOCK:

        if uid not in LINKS:
            return False

        old_sub = LINKS[
            uid
        ].get(
            "sub_id"
        )

        label = LINKS[
            uid
        ].get(
            "label",
            uid,
        )

    if sub_id is not None:

        async with SUBS_LOCK:

            if sub_id not in SUBS:
                return False

    async with SUBS_LOCK:

        if (
            old_sub
            and old_sub in SUBS
        ):

            ids = SUBS[
                old_sub
            ].get(
                "link_ids",
                [],
            )

            if uid in ids:
                ids.remove(uid)

        if (
            sub_id
            and sub_id in SUBS
        ):

            ids = SUBS[
                sub_id
            ].setdefault(
                "link_ids",
                [],
            )

            if uid not in ids:
                ids.append(uid)

    async with LINKS_LOCK:

        if uid in LINKS:

            LINKS[
                uid
            ][
                "sub_id"
            ] = sub_id

    await save_state()

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{label}» "
            f"{'به گروه اضافه شد' if sub_id else 'از گروه خارج شد'}"
        ),
        "info",
    )

    return True


async def remove_sub_group(
    sub_id: str,
):

    async with SUBS_LOCK:

        if sub_id not in SUBS:
            return None

        name = SUBS[
            sub_id
        ].get(
            "name",
            sub_id,
        )

        del SUBS[sub_id]

    async with LINKS_LOCK:

        for link in LINKS.values():

            if (
                link.get("sub_id")
                == sub_id
            ):
                link["sub_id"] = None

    await save_state()

    log_activity(
        "sub",
        (
            f"گروه "
            f"«{name}» "
            f"حذف شد"
        ),
        "warn",
    )

    return name


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup():

    global http_client

    limits = httpx.Limits(
        max_connections=500,
        max_keepalive_connections=100,
    )

    timeout = httpx.Timeout(
        30.0,
        connect=10.0,
    )

    http_client = httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
        follow_redirects=True,
    )

    await load_state()
    await save_state()

    await ensure_default_categories()
    await remove_legacy_default_links()

    log_activity(
        "system",
        (
            f"{APP_NAME} "
            f"v{APP_VERSION} "
            f"راه‌اندازی شد"
        ),
        "ok",
    )

    logger.info(
        "%s v%s started on 0.0.0.0:%s",
        APP_NAME,
        APP_VERSION,
        PORT,
    )

    logger.info(
        "Data directory: %s",
        DATA_DIR,
    )


@app.on_event("shutdown")
async def shutdown():

    await save_state()

    if http_client:
        await http_client.aclose()


# ============================================================
# LANDING
# ============================================================

LANDING_HTML = r"""
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">

<title>PX Panel</title>

<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>

<link
href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;500;600;700;800;900&display=swap"
rel="stylesheet">

<style>
*{
    box-sizing:border-box;
}

html,body{
    margin:0;
    min-height:100%;
}

body{
    min-height:100vh;
    display:flex;
    justify-content:center;
    align-items:center;
    padding:20px;
    color:#fff;
    font-family:"Vazirmatn",sans-serif;

    background:
        radial-gradient(
            circle at 15% 15%,
            rgba(37,99,235,.22),
            transparent 30%
        ),
        radial-gradient(
            circle at 85% 85%,
            rgba(59,130,246,.18),
            transparent 30%
        ),
        #07070a;
}

.card{
    width:100%;
    max-width:580px;
    padding:32px;
    border-radius:28px;

    border:1px solid rgba(255,255,255,.09);

    background:
        linear-gradient(
            145deg,
            rgba(255,255,255,.07),
            rgba(255,255,255,.025)
        );

    backdrop-filter:blur(28px) saturate(150%);

    box-shadow:
        0 30px 90px rgba(0,0,0,.45);
}

.brand{
    display:flex;
    align-items:center;
    gap:12px;
}

.logo{
    width:48px;
    height:48px;
    border-radius:15px;

    display:flex;
    justify-content:center;
    align-items:center;

    font-size:18px;
    font-weight:900;

    background:
        linear-gradient(
            135deg,
            #2563eb,
            #3b82f6
        );
}

.brand-name{
    font-size:17px;
    font-weight:900;
}

.version{
    margin-top:4px;
    font-size:11px;
    color:#60a5fa;
}

.status{
    display:inline-block;
    margin-top:23px;
    padding:7px 11px;
    border-radius:999px;

    color:#86efac;
    background:rgba(34,197,94,.07);
    border:1px solid rgba(34,197,94,.15);

    font-size:11px;
}

h1{
    margin:18px 0 0;
    font-size:28px;
    line-height:1.55;
}

.desc{
    margin-top:12px;
    color:rgba(255,255,255,.52);
    line-height:2;
    font-size:13px;
}

.path{
    margin-top:22px;
    padding:15px;
    border-radius:15px;

    background:rgba(0,0,0,.18);
    border:1px solid rgba(255,255,255,.07);

    direction:ltr;
    text-align:left;
    font-family:Consolas,monospace;
    color:#93c5fd;
}

.actions{
    display:flex;
    gap:10px;
    margin-top:20px;
}

.btn{
    flex:1;
    padding:13px;
    border-radius:14px;
    text-align:center;
    text-decoration:none;

    font-size:12px;
    font-weight:800;
}

.primary{
    color:#fff;
    background:
        linear-gradient(
            135deg,
            #2563eb,
            #3b82f6
        );
}

.secondary{
    color:#fff;
    background:rgba(255,255,255,.035);
    border:1px solid rgba(255,255,255,.08);
}

.footer{
    margin-top:22px;
    padding-top:16px;
    border-top:1px solid rgba(255,255,255,.07);

    display:flex;
    justify-content:space-between;

    font-size:10px;
    color:rgba(255,255,255,.35);
}

.support{
    color:#60a5fa;
    text-decoration:none;
}



/* ONEX responsive system */
html{scroll-behavior:smooth} body{overflow-x:hidden} button,input,select,textarea{touch-action:manipulation} .modal{overscroll-behavior:contain}


@media(prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:.01ms!important;transition-duration:.01ms!important;scroll-behavior:auto!important}}

/* Toggle switch */
.switch{position:relative;display:inline-block;width:42px;height:24px;vertical-align:middle}
.switch input{opacity:0;width:0;height:0}
.slider{position:absolute;cursor:pointer;inset:0;background:rgba(255,255,255,.12);border-radius:24px;transition:.2s}
.slider:before{position:absolute;content:"";height:18px;width:18px;left:3px;bottom:3px;background:#fff;border-radius:50%;transition:.2s}
.switch input:checked+.slider{background:var(--green)}
.switch input:checked+.slider:before{transform:translateX(18px)}


.conn-badge{display:inline-flex;align-items:center;justify-content:center;min-width:22px;height:20px;padding:0 7px;border-radius:8px;font-size:10px;font-weight:800}
.conn-badge.green{background:rgba(34,197,94,.18);color:#4ade80}
.conn-badge.gray{background:rgba(148,163,184,.15);color:#94a3b8}
.conn-badge.orange{background:rgba(245,158,11,.18);color:#fbbf24}
.conn-badge.red{background:rgba(239,68,68,.18);color:#f87171}


.bottom-bulk{position:fixed;left:0;right:0;bottom:0;z-index:400;display:none;padding:12px 16px;background:var(--card);border-top:1px solid var(--card-b);backdrop-filter:blur(12px)}
.bottom-bulk.show{display:block}
.bottom-bulk-inner{max-width:960px;margin:0 auto;display:flex;flex-wrap:wrap;gap:10px;align-items:center;justify-content:center}
.bottom-bulk select{padding:8px 10px;border-radius:10px;border:1px solid var(--card-b);background:var(--input-bg);color:var(--t1);font-family:inherit;font-size:12px}

table th:first-child, table td:first-child{overflow:visible}
.cfg-chk{accent-color:var(--accent)}
#page-donate .page-title{width:100%}

<style>
/* ONEX VERSION STRIP — final responsive rules */
.dashboard-hero{grid-template-columns:minmax(0,1fr) auto;grid-template-areas:"hero version";align-items:end}
.dashboard-hero .hero-main{grid-area:hero}
.dashboard-hero .hero-version-strip{grid-area:version}
.dashboard-hero .hero-actions{display:none!important}
.hero-version-strip{justify-content:flex-start}
@media(max-width:768px){
  #page-dash .dashboard-hero{display:grid!important;grid-template-columns:minmax(0,1fr) auto!important;grid-template-areas:"hero version"!important;align-items:center!important;gap:7px!important;margin:0 0 10px!important}
  #page-dash .dashboard-hero .hero-main{grid-area:hero!important;min-width:0}
  #page-dash .dashboard-hero .hero-version-strip{grid-area:version!important;display:flex!important;flex-direction:column!important;gap:5px!important;align-self:center!important}
  #page-dash .dashboard-hero .hero-actions{display:none!important}
  #page-dash .version-mini-card{min-width:105px!important;min-height:43px!important;padding:5px 6px!important;border-radius:11px!important;gap:5px!important}
  #page-dash .version-mini-icon{width:24px!important;height:24px!important;flex-basis:24px!important;border-radius:7px!important;font-size:10px!important}
  #page-dash .version-mini-copy b{font-size:6.5px!important}
  #page-dash .version-mini-copy strong{font-size:10px!important}
  #page-dash .version-live-dot{width:5px!important;height:5px!important;flex-basis:5px!important}
}
@media(max-width:380px){
  #page-dash .dashboard-hero{grid-template-columns:minmax(0,1fr) 98px!important;gap:5px!important}
  #page-dash .version-mini-card{min-width:98px!important;padding:4px!important}
  #page-dash .version-mini-copy b{font-size:6px!important}
  #page-dash .version-mini-copy strong{font-size:9px!important}
}



<style>
/* ONEX animated 3D logo — approved high-detail brand mark */
.sb-logo-icon,
.mob-brand-icon{
  position:relative!important;
  overflow:visible!important;
  background:transparent!important;
  border:0!important;
  box-shadow:none!important;
  transform-style:preserve-3d!important;
  perspective:900px!important;
  isolation:isolate!important;
}
.sb-logo-icon:before,
.mob-brand-icon:before{
  content:""!important;
  position:absolute!important;
  inset:-18%!important;
  border-radius:50%!important;
  background:conic-gradient(from 0deg,rgba(0,210,255,0),rgba(0,210,255,.22),rgba(168,85,247,.24),rgba(255,45,140,.18),rgba(0,210,255,0))!important;
  filter:blur(14px)!important;
  opacity:.55!important;
  z-index:-1!important;
  animation:onexAura 5.5s linear infinite!important;
  pointer-events:none!important;
}
.sb-logo-icon:after,
.mob-brand-icon:after{
  content:""!important;
  position:absolute!important;
  left:10%!important;
  right:10%!important;
  bottom:-3%!important;
  height:16%!important;
  border-radius:50%!important;
  background:radial-gradient(ellipse,rgba(0,140,255,.34),rgba(168,85,247,.12) 45%,transparent 72%)!important;
  filter:blur(7px)!important;
  z-index:-1!important;
  animation:onexShadow 4.8s ease-in-out infinite!important;
  pointer-events:none!important;
}
.sb-logo-icon img,
.mob-brand-icon img{
  position:relative!important;
  z-index:2!important;
  width:100%!important;
  height:100%!important;
  object-fit:contain!important;
  object-position:center!important;
  display:block!important;
  border-radius:0!important;
  background:transparent!important;
  filter:drop-shadow(0 10px 13px rgba(0,65,255,.28)) drop-shadow(0 0 15px rgba(0,210,255,.18))!important;
  transform-style:preserve-3d!important;
  transform-origin:center center!important;
  animation:onexLogo3D 5.8s cubic-bezier(.45,.05,.55,.95) infinite!important;
  will-change:transform,filter!important;
}
@keyframes onexLogo3D{
  0%,100%{transform:perspective(900px) rotateX(2deg) rotateY(-10deg) rotateZ(-1deg) translate3d(0,0,0) scale(1);filter:drop-shadow(0 10px 13px rgba(0,65,255,.28)) drop-shadow(0 0 15px rgba(0,210,255,.18));}
  20%{transform:perspective(900px) rotateX(-4deg) rotateY(7deg) rotateZ(1deg) translate3d(1px,-2px,8px) scale(1.025);filter:drop-shadow(-4px 12px 15px rgba(0,65,255,.34)) drop-shadow(0 0 20px rgba(0,210,255,.30));}
  40%{transform:perspective(900px) rotateX(5deg) rotateY(13deg) rotateZ(0deg) translate3d(0,-5px,18px) scale(1.055);filter:drop-shadow(-7px 15px 18px rgba(0,65,255,.38)) drop-shadow(0 0 25px rgba(168,85,247,.28));}
  60%{transform:perspective(900px) rotateX(-3deg) rotateY(-5deg) rotateZ(-1deg) translate3d(-1px,-3px,12px) scale(1.035);filter:drop-shadow(5px 13px 16px rgba(0,65,255,.34)) drop-shadow(0 0 21px rgba(255,45,140,.22));}
  80%{transform:perspective(900px) rotateX(3deg) rotateY(-13deg) rotateZ(1deg) translate3d(0,-1px,6px) scale(1.018);filter:drop-shadow(7px 11px 14px rgba(0,65,255,.32)) drop-shadow(0 0 19px rgba(0,210,255,.28));}
}
@keyframes onexAura{0%{transform:rotate(0deg) scale(.88);opacity:.30}50%{transform:rotate(180deg) scale(1.12);opacity:.68}100%{transform:rotate(360deg) scale(.88);opacity:.30}}
@keyframes onexShadow{0%,100%{transform:scale(.88);opacity:.28}50%{transform:scale(1.12);opacity:.52}}
@media (prefers-reduced-motion:reduce){.sb-logo-icon:before,.sb-logo-icon:after,.mob-brand-icon:before,.mob-brand-icon:after,.sb-logo-icon img,.mob-brand-icon img{animation:none!important}}
</style>

</head>

<body>

<div class="card">

<div class="brand">

<div class="logo">P</div>

<div>
<div class="brand-name">
PX Panel
</div>

<div class="version">
13.8.0
</div>
</div>

</div>

<div class="status">
● سیستم آنلاین و فعال است
</div>

<h1>
برای ورود به پنل
<br>
ابتدا وارد شوید
</h1>

<div class="desc">
این صفحه، درگاه عمومی PX Panel است.
برای دسترسی به داشبورد مدیریت از مسیر ورود استفاده کنید.
</div>

<div class="path">
/login
</div>

<div class="actions">

<a
href="/login"
class="btn primary"
>
ورود به پنل
</a>

<a
href="https://t.me/Pixonal"
target="_blank"
rel="noopener"
class="btn secondary"
>
پشتیبانی
</a>

</div>

<div class="footer">

<span>
PX Panel · 13.8.0
</span>

<a
href="https://t.me/Pixonal"
target="_blank"
class="support"
>
@Pixonal
</a>

</div>

</div>


<div id="bottomBulkBar" class="bottom-bulk">
  <div class="bottom-bulk-inner">
    <span id="bulkCount">0 انتخاب</span>
    <select id="bulkGroup"></select>
    <button class="btn btn-sm" onclick="bulkMoveGroup()">انتقال به گروه</button>
    <button class="btn btn-sm btn-d" onclick="bulkDelete()">حذف انتخاب‌شده</button>
    <button class="btn btn-sm" onclick="clearSelection()">لغو</button>
  </div>
</div>
</body>
</html>
"""


@app.get(
    "/",
    response_class=HTMLResponse,
)
async def root(
    request: Request,
):

    if await is_valid_session(
        request.cookies.get(
            SESSION_COOKIE
        )
    ):
        return RedirectResponse(
            "/dashboard"
        )

    return HTMLResponse(
        render_login_html(),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():

    return {
        "status": "ok",
        "service": APP_NAME,
        "version": APP_VERSION,
        "connections": len(connections),
        "uptime": uptime(),
    }


# ============================================================
# LOGIN
# ============================================================

LOGIN_HTML = '<!DOCTYPE html>\n<html lang="fa" dir="rtl">\n<head>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<meta name="theme-color" content="#05060d">\n<title>ONEX · Login</title>\n<link rel="preconnect" href="https://fonts.googleapis.com">\n<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;500;700;800;900&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">\n<script>\nwindow.ONEX_THEMES={\n  aurora:{p:\'#38d9ff\',s:\'#8b5cf6\',fa:\'شفق\',en:\'Aurora\'},\n  lime:{p:\'#d4f24a\',s:\'#ffb84d\',fa:\'لیمویی\',en:\'Lime\'},\n  rose:{p:\'#ff3d8b\',s:\'#ff8a3d\',fa:\'سرخابی\',en:\'Rose\'},\n  emerald:{p:\'#34f5a0\',s:\'#22c1ee\',fa:\'زمردی\',en:\'Emerald\'},\n  violet:{p:\'#b57bff\',s:\'#ff5ecf\',fa:\'بنفش\',en:\'Violet\'},\n  gold:{p:\'#ffc94d\',s:\'#ff7a45\',fa:\'طلایی\',en:\'Gold\'}\n};\nwindow.hexToRgb=function(h){h=String(h||\'\').replace(\'#\',\'\');if(h.length===3)h=h.split(\'\').map(function(c){return c+c}).join(\'\');var n=parseInt(h,16)||0;return[(n>>16)&255,(n>>8)&255,n&255]};\nwindow.shiftHue=function(hex,deg){var c=hexToRgb(hex).map(function(v){return v/255}),r=c[0],g=c[1],b=c[2],mx=Math.max(r,g,b),mn=Math.min(r,g,b),h=0,s=0,l=(mx+mn)/2;\n  if(mx!==mn){var d=mx-mn;s=l>.5?d/(2-mx-mn):d/(mx+mn);h=mx===r?(g-b)/d+(g<b?6:0):mx===g?(b-r)/d+2:(r-g)/d+4;h*=60}\n  h=(h+deg+360)%360;s=Math.max(s,.55);l=Math.min(Math.max(l,.5),.68);\n  var q=l<.5?l*(1+s):l+s-l*s,p=2*l-q,f=function(t){t=(t+360)%360/360;if(t<1/6)return p+(q-p)*6*t;if(t<.5)return q;if(t<2/3)return p+(q-p)*(2/3-t)*6;return p};\n  return \'#\'+[f(h+120),f(h),f(h-120)].map(function(v){return Math.round(v*255).toString(16).padStart(2,\'0\')}).join(\'\')};\nwindow.getThemeColors=function(id){\n  if(id===\'custom\'){var c=localStorage.getItem(\'onex_theme_custom\')||\'#38d9ff\';return{p:c,s:shiftHue(c,55)}}\n  return ONEX_THEMES[id]||ONEX_THEMES.aurora};\nwindow.applyThemeVars=function(id){\n  var t=getThemeColors(id),P=hexToRgb(t.p),S=hexToRgb(t.s),lum=(P[0]*.299+P[1]*.587+P[2]*.114)/255,st=document.documentElement.style;\n  st.setProperty(\'--p\',t.p);st.setProperty(\'--s\',t.s);st.setProperty(\'--p-rgb\',P.join(\' \'));st.setProperty(\'--s-rgb\',S.join(\' \'));\n  st.setProperty(\'--on\',lum>.62?\'#0b0d14\':\'#ffffff\');window.__themeRGB={p:P,s:S};document.documentElement.dataset.theme=id};\n(function(){try{var l=localStorage.getItem(\'onex_lang\')||\'fa\';document.documentElement.lang=l;document.documentElement.dir=l===\'fa\'?\'rtl\':\'ltr\';applyThemeVars(localStorage.getItem(\'onex_theme\')||\'aurora\')}catch(e){applyThemeVars(\'aurora\')}})();\n</script>\n<style>\n:root{\n  --bg:#05060d;--tx:#eef2ff;--mut:#a3abc6;--dim:#6d7594;\n  --glass:rgb(18 22 40 / .62);--glass-2:rgb(28 33 58 / .55);--line:rgb(255 255 255 / .09);--line-2:rgb(255 255 255 / .16);\n  --ease:cubic-bezier(.16,1,.3,1);--ease-q:cubic-bezier(.25,1,.5,1);--mono:"JetBrains Mono",ui-monospace,monospace;\n}\n*{box-sizing:border-box;margin:0;padding:0}\n[hidden]{display:none!important}\nhtml,body{min-height:100%}\nbody{font-family:Vazirmatn,system-ui,sans-serif;background:var(--bg);color:var(--tx);overflow-x:hidden;-webkit-font-smoothing:antialiased}\ncanvas#scene{position:fixed;inset:0;display:block;z-index:0}\n.veil{position:fixed;inset:0;z-index:1;pointer-events:none;\n  background:radial-gradient(120% 90% at 50% 50%,transparent 55%,rgb(3 4 9 / .85) 100%),linear-gradient(180deg,rgb(5 6 13 / .35),transparent 20%,transparent 80%,rgb(5 6 13 / .6))}\n\n/* ---------- glass pill base ---------- */\n.pill{display:flex;align-items:center;gap:12px;height:54px;padding:0 14px;border-radius:18px;\n  background:linear-gradient(135deg,rgb(255 255 255 / .07),rgb(255 255 255 / .02));border:1px solid var(--line-2);\n  backdrop-filter:blur(18px) saturate(160%);-webkit-backdrop-filter:blur(18px) saturate(160%);\n  box-shadow:0 1px 0 rgb(255 255 255 / .1) inset,0 18px 40px -20px rgb(0 0 0 / .8)}\n\n/* ---------- top bar ---------- */\n.top{position:fixed;inset-inline:0;top:0;z-index:5;display:flex;align-items:center;justify-content:space-between;gap:14px;padding:22px 32px;opacity:0;transform:translateY(-14px)}\n.ready .top{animation:drop .9s var(--ease) .1s forwards}\n@keyframes drop{to{opacity:1;transform:none}}\n.brand{text-decoration:none;color:var(--tx);direction:ltr;padding:0 18px 0 10px}\n.logo{position:relative;width:38px;height:38px;display:grid;place-items:center}\n.logo::before{content:"";position:absolute;inset:-8px;border-radius:50%;background:radial-gradient(circle,rgb(var(--p-rgb) / .5),transparent 65%);filter:blur(6px);animation:glow 3.2s ease-in-out infinite}\n.logo svg{position:relative;width:38px;height:38px;filter:drop-shadow(0 4px 12px rgb(var(--p-rgb) / .5));transition:transform .7s var(--ease)}\n.brand:hover .logo svg{transform:rotate(60deg) scale(1.06)}\n.logo .orbit{transform-origin:24px 24px;animation:orbit 5s linear infinite}\n@keyframes orbit{to{transform:rotate(360deg)}}\n@keyframes glow{0%,100%{opacity:.55;transform:scale(.9)}50%{opacity:1;transform:scale(1.1)}}\n.word{display:flex;align-items:baseline;font:900 23px/1 Vazirmatn,system-ui;letter-spacing:.08em}\n.word b{background:linear-gradient(180deg,#fff,#c9d0ea);-webkit-background-clip:text;background-clip:text;color:transparent}\n.word i{font-style:normal;background:linear-gradient(135deg,var(--p),var(--s));-webkit-background-clip:text;background-clip:text;color:transparent}\n.word small{font:500 10px var(--mono);color:var(--dim);margin-inline-start:9px;letter-spacing:0;padding:3px 7px;border:1px solid var(--line-2);border-radius:99px}\n\n.tg{position:relative;text-decoration:none;color:var(--tx);padding:0 6px 0 6px;border-radius:99px;overflow:hidden;isolation:isolate;\n  background:linear-gradient(135deg,rgb(55 174 226 / .24),rgb(55 174 226 / .07));border-color:rgb(120 200 240 / .45);\n  animation:tgPulse 2.4s ease-in-out infinite;transition:transform .3s var(--ease-q),border-color .3s}\n.tg::before{content:"";position:absolute;inset:0;z-index:-1;background:linear-gradient(100deg,transparent 30%,rgb(255 255 255 / .3) 50%,transparent 70%);transform:translateX(130%);animation:tgShine 3.2s var(--ease) infinite}\n.tg:hover{transform:translateY(-2px) scale(1.03);border-color:#8fd6f7}\n.tg-ico{position:relative;width:40px;height:40px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(135deg,#37aee2,#1e96c8);flex:none}\n.tg-ico::after{content:"";position:absolute;inset:-4px;border-radius:50%;border:2px solid #37aee2;animation:ping 1.8s var(--ease) infinite}\n.tg-ico svg{width:21px;height:21px;margin-inline-end:2px;animation:tgWiggle 3s ease-in-out infinite}\n.tg-txt{display:flex;flex-direction:column;line-height:1.3}\n.tg-txt b{font-size:13.5px;font-weight:800}\n.tg-txt em{font-style:normal;font-size:11px;color:#b9dff2}\n.tg-go{display:flex;align-items:center;gap:6px;height:40px;padding:0 16px;border-radius:99px;background:#37aee2;color:#fff;font:800 12.5px Vazirmatn;box-shadow:0 6px 18px -6px #37aee2}\n.tg-go svg{width:14px;height:14px}\n[dir=rtl] .tg-go svg{transform:scaleX(-1)}\n@keyframes ping{0%{transform:scale(.7);opacity:.9}100%{transform:scale(1.7);opacity:0}}\n@keyframes tgPulse{0%,100%{box-shadow:0 1px 0 rgb(255 255 255 / .12) inset,0 0 0 0 rgb(55 174 226 / .55)}50%{box-shadow:0 1px 0 rgb(255 255 255 / .12) inset,0 0 0 12px rgb(55 174 226 / 0)}}\n@keyframes tgShine{0%,55%{transform:translateX(130%)}100%{transform:translateX(-130%)}}\n@keyframes tgWiggle{0%,80%,100%{transform:rotate(0)}84%{transform:rotate(-14deg)}88%{transform:rotate(12deg)}92%{transform:rotate(-8deg)}96%{transform:rotate(4deg)}}\n\n.ctrl{gap:14px;padding:0 10px}\n.seg{display:flex;padding:4px;border-radius:12px;background:rgb(0 0 0 / .3);border:1px solid var(--line);direction:ltr}\n.seg button{height:34px;min-width:46px;border:0;border-radius:9px;background:transparent;color:var(--mut);font:700 12.5px var(--mono);cursor:pointer;transition:.25s var(--ease-q)}\n.seg button:hover{color:var(--tx)}\n.seg button.on{background:linear-gradient(135deg,rgb(var(--p-rgb) / .28),rgb(var(--s-rgb) / .18));color:#fff;box-shadow:0 0 0 1px rgb(var(--p-rgb) / .55) inset,0 4px 14px -4px rgb(var(--p-rgb) / .6)}\n.sep{width:1px;height:28px;background:var(--line-2)}\n.sw{display:flex;gap:9px;align-items:center}\n.sw button,.sw label{position:relative;width:24px;height:24px;border-radius:50%;border:0;cursor:pointer;flex:none;transition:transform .25s var(--ease-q)}\n.sw button:hover,.sw label:hover{transform:scale(1.15)}\n.sw button.on,.sw label.on{box-shadow:0 0 0 2px var(--bg),0 0 0 4px #fff,0 0 16px 2px rgb(var(--p-rgb) / .7)}\n.sw label{background:conic-gradient(#ff3d8b,#ffc94d,#d4f24a,#34f5a0,#38d9ff,#8b5cf6,#ff3d8b);display:grid;place-items:center}\n.sw label::after{content:"+";font:800 14px/1 var(--mono);color:#fff;text-shadow:0 1px 3px #000}\n.sw input{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}\n\n/* ---------- stage ---------- */\nmain{position:relative;z-index:3;min-height:100vh;display:flex;align-items:center;padding:118px 7vw 84px}\n.col{width:min(440px,100%);display:flex;flex-direction:column;gap:16px;opacity:0;transform:translateY(36px)}\n.ready .col{animation:rise 1.1s var(--ease) .2s forwards}\n@keyframes rise{to{opacity:1;transform:none}}\n.tilt{perspective:1400px}\n.tilt>.flip{transform-style:preserve-3d;transition:transform .5s var(--ease-q)}\n.flip{display:grid;transform-style:preserve-3d;transition:transform 1s var(--ease)}\n.flip.on .inner{transform:rotateY(180deg)}\n.inner{display:grid;transform-style:preserve-3d;transition:transform 1s var(--ease)}\n.face{grid-area:1/1;backface-visibility:hidden;-webkit-backface-visibility:hidden;position:relative;overflow:hidden;\n  padding:38px 34px 30px;border-radius:26px;\n  background:linear-gradient(160deg,rgb(30 36 64 / .78),rgb(12 14 28 / .86));border:1px solid var(--line-2);\n  backdrop-filter:blur(22px) saturate(150%);-webkit-backdrop-filter:blur(22px) saturate(150%);\n  box-shadow:0 1px 0 rgb(255 255 255 / .12) inset,0 50px 90px -30px rgb(0 0 0 / .9),0 0 60px -20px rgb(var(--p-rgb) / .35)}\n.face.back{transform:rotateY(180deg)}\n.face::before{content:"";position:absolute;inset:0;pointer-events:none;border-radius:inherit;\n  background:radial-gradient(420px circle at var(--mx,50%) var(--my,0%),rgb(var(--p-rgb) / .1),transparent 60%)}\n.face::after{content:"";position:absolute;inset:0;border-radius:inherit;padding:1px;pointer-events:none;\n  background:linear-gradient(135deg,rgb(var(--p-rgb) / .7),transparent 35%,transparent 65%,rgb(var(--s-rgb) / .6));\n  -webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude}\n.face h1{font-size:30px;font-weight:900;letter-spacing:-.01em;display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}\n.grad{background:linear-gradient(135deg,var(--p),var(--s));-webkit-background-clip:text;background-clip:text;color:transparent;direction:ltr}\n.face .sub{font-size:14px;color:var(--mut);margin-top:8px;line-height:1.85}\n.bar-acc{width:56px;height:3px;border-radius:3px;margin-top:14px;background:linear-gradient(90deg,var(--p),var(--s))}\nform{margin-top:26px;display:flex;flex-direction:column;gap:16px}\n.field label.l{display:flex;justify-content:space-between;font-size:12.5px;font-weight:600;color:var(--mut);margin-bottom:8px}\n.field label.l em{font-style:normal;font-family:var(--mono);font-size:11px;color:var(--dim);direction:ltr}\n.ctl{position:relative;display:flex;align-items:center;height:54px;border-radius:15px;background:rgb(0 0 0 / .28);border:1px solid var(--line-2);transition:border-color .2s,background-color .2s,box-shadow .3s}\n.ctl:hover{border-color:rgb(255 255 255 / .26)}\n.ctl:focus-within{border-color:var(--p);background:rgb(var(--p-rgb) / .06);box-shadow:0 0 0 4px rgb(var(--p-rgb) / .14)}\n.ctl svg.ic{width:18px;height:18px;color:var(--dim);margin-inline:16px 4px;flex:none;transition:color .2s}\n.ctl:focus-within svg.ic{color:var(--p)}\n.ctl input{flex:1;height:100%;border:0;background:transparent;color:var(--tx);font:500 15px var(--mono);direction:ltr;text-align:left;outline:none;padding-inline:4px 16px;min-width:0}\n.ctl input::placeholder{color:var(--dim);font-family:Vazirmatn;font-weight:400}\n[dir=rtl] .ctl input::placeholder{direction:rtl;text-align:right}\n.eye{flex:none;width:42px;height:42px;margin-inline-end:6px;border:0;background:transparent;color:var(--dim);cursor:pointer;border-radius:11px;display:grid;place-items:center}\n.eye:hover{color:var(--tx);background:rgb(255 255 255 / .06)}\n.eye svg{width:18px;height:18px}\n.field.bad .ctl{border-color:#ff6b81;box-shadow:0 0 0 4px rgb(255 107 129 / .12)}\n.caps{display:flex;align-items:center;gap:8px;font-size:12px;color:#ffc36b;margin-top:8px}\n.caps svg{width:15px;height:15px}\n.row{display:flex;justify-content:space-between;align-items:center;font-size:12.5px;color:var(--mut)}\n.chk{display:flex;align-items:center;gap:9px;cursor:pointer;user-select:none}\n.chk input{appearance:none;width:18px;height:18px;border-radius:6px;border:1px solid var(--line-2);background:rgb(0 0 0 / .3);display:grid;place-items:center;cursor:pointer;transition:.2s}\n.chk input:checked{background:var(--p);border-color:var(--p)}\n.chk input:checked::after{content:"";width:9px;height:5px;border-left:2px solid var(--on);border-bottom:2px solid var(--on);transform:rotate(-45deg) translate(1px,-1px)}\n.secure{font-family:var(--mono);font-size:11px;color:var(--dim);direction:ltr;display:flex;align-items:center;gap:6px}\n.secure i{width:7px;height:7px;border-radius:50%;background:var(--p);box-shadow:0 0 10px var(--p)}\n.btn{position:relative;height:56px;border:0;border-radius:15px;background:linear-gradient(135deg,var(--p),var(--s));color:var(--on);font:800 15.5px Vazirmatn;cursor:pointer;\n  display:flex;align-items:center;justify-content:center;gap:12px;margin-top:4px;overflow:hidden;box-shadow:0 14px 30px -12px rgb(var(--p-rgb) / .8);\n  transition:transform .25s var(--ease-q),box-shadow .25s,filter .2s}\n.btn:hover{transform:translateY(-2px);box-shadow:0 20px 40px -14px rgb(var(--p-rgb) / .95);filter:saturate(1.15)}\n.btn:active{transform:scale(.985)}\n.btn:disabled{opacity:.7;cursor:wait}\n.btn svg{width:20px;height:20px}\n[dir=rtl] .btn svg.arr{transform:scaleX(-1)}\n.btn::after{content:"";position:absolute;inset:0;background:linear-gradient(100deg,transparent 35%,rgb(255 255 255 / .4) 50%,transparent 65%);transform:translateX(-120%);transition:none}\n.btn:hover::after{transform:translateX(120%);transition:transform .9s var(--ease)}\n.btn.alt{background:linear-gradient(135deg,var(--s),var(--p))}\n.msg{display:grid;grid-template-rows:0fr;transition:grid-template-rows .35s var(--ease)}\n.msg>div{overflow:hidden}\n.msg.show{grid-template-rows:1fr}\n.msg p{font-size:13px;line-height:1.8;padding:11px 14px;border-radius:12px;background:rgb(255 80 110 / .12);color:#ff9aab;border:1px solid rgb(255 107 129 / .35)}\n.chip{display:inline-flex;align-items:center;gap:8px;background:linear-gradient(135deg,var(--s),var(--p));color:var(--on);font-size:12px;font-weight:800;padding:5px 14px;border-radius:99px;margin-bottom:14px}\n.chip svg{width:14px;height:14px}\n.meter{display:flex;gap:6px;margin-top:10px;direction:ltr}\n.meter i{flex:1;height:4px;border-radius:4px;background:rgb(255 255 255 / .08);transition:background-color .3s}\n.meter[data-s="1"] i:nth-child(-n+1){background:#ff6b81}\n.meter[data-s="2"] i:nth-child(-n+2){background:#ffc36b}\n.meter[data-s="3"] i:nth-child(-n+3){background:var(--s)}\n.meter[data-s="4"] i{background:var(--p)}\n.mlabel{font-size:11.5px;color:var(--dim);margin-top:6px;min-height:1em}\n.ghost{background:none;border:0;color:var(--mut);font:600 13px Vazirmatn;cursor:pointer;align-self:center;padding:8px 14px;border-radius:9px}\n.ghost:hover{color:var(--tx);background:rgb(255 255 255 / .06)}\n.done{position:absolute;inset:0;background:linear-gradient(160deg,rgb(30 36 64 / .96),rgb(12 14 28 / .98));display:flex;flex-direction:column;align-items:center;justify-content:center;gap:14px;opacity:0;pointer-events:none;transition:opacity .5s var(--ease);border-radius:inherit;z-index:3}\n.done.show{opacity:1;pointer-events:auto}\n.done svg{width:78px;height:78px;stroke:var(--p);filter:drop-shadow(0 0 14px rgb(var(--p-rgb) / .6))}\n.done circle{stroke-dasharray:220;stroke-dashoffset:220}\n.done path{stroke-dasharray:60;stroke-dashoffset:60}\n.done.show circle{animation:draw 1s var(--ease) forwards}\n.done.show path{animation:draw .6s var(--ease) .45s forwards}\n@keyframes draw{to{stroke-dashoffset:0}}\n.done b{font-size:22px;font-weight:900}\n.done span{font-size:13px;color:var(--mut)}\n\n/* ---------- glass hint bar ---------- */\n.hintbar{position:relative;display:flex;align-items:center;gap:14px;padding:12px 14px;border-radius:20px;overflow:hidden;\n  background:linear-gradient(135deg,rgb(255 255 255 / .08),rgb(255 255 255 / .02));border:1px solid var(--line-2);\n  backdrop-filter:blur(18px) saturate(160%);-webkit-backdrop-filter:blur(18px) saturate(160%);\n  box-shadow:0 1px 0 rgb(255 255 255 / .1) inset,0 20px 40px -24px rgb(0 0 0 / .9)}\n.hintbar::before{content:"";position:absolute;inset:0;background:linear-gradient(100deg,transparent 30%,rgb(var(--p-rgb) / .12) 50%,transparent 70%);transform:translateX(-120%);animation:sweep 5s var(--ease) infinite}\n@keyframes sweep{0%,60%{transform:translateX(-120%)}100%{transform:translateX(120%)}}\n.hk{flex:none;width:42px;height:42px;border-radius:13px;display:grid;place-items:center;color:var(--p);\n  background:rgb(var(--p-rgb) / .12);border:1px solid rgb(var(--p-rgb) / .4);box-shadow:0 0 18px -4px rgb(var(--p-rgb) / .6)}\n.hk svg{width:20px;height:20px}\n.ht{display:flex;flex-direction:column;gap:3px;min-width:0;flex:1}\n.ht b{font-size:13px;font-weight:800;display:flex;gap:8px;align-items:center;flex-wrap:wrap}\n.ht code{font:700 13px var(--mono);direction:ltr;color:var(--p);background:rgb(0 0 0 / .3);padding:2px 9px;border-radius:7px;border:1px solid var(--line)}\n.ht span{font-size:11.5px;color:var(--dim)}\n.hfill{flex:none;height:34px;padding:0 12px;border-radius:10px;border:1px solid var(--line-2);background:rgb(255 255 255 / .05);color:var(--tx);font:700 12px Vazirmatn;cursor:pointer}\n.hfill:hover{border-color:var(--p);color:var(--p)}\n\n/* ---------- footer ---------- */\n.foot{position:fixed;inset-inline:0;bottom:0;z-index:4;height:56px;display:flex;align-items:center;justify-content:space-between;padding:0 32px;font-size:12px;color:var(--dim)}\n.clock{font-family:var(--mono);direction:ltr;display:flex;gap:12px;align-items:center}\n.clock b{color:var(--tx);font-weight:500}\n\n@media (max-width:1100px){.tg-txt em{display:none}}\n@media (max-width:900px){\n  main{justify-content:center;padding:150px 18px 90px}\n  .top{flex-wrap:wrap;justify-content:center;padding:14px}\n  .brand{order:1}.ctrl{order:2}.tg{order:3;width:100%;justify-content:space-between}\n  .tg-txt em{display:block}\n  .foot{position:relative;justify-content:center;gap:16px;flex-wrap:wrap;height:auto;padding:14px}\n}\n@media (max-width:520px){\n  main{padding-top:190px}\n  .face{padding:30px 22px 24px;border-radius:22px}\n  .face h1{font-size:25px}\n  .word small{display:none}\n  .sw{gap:7px}.sw button,.sw label{width:20px;height:20px}\n  .seg button{min-width:38px}\n  .ctrl{padding:0 8px;gap:10px}\n}\n@media (prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:.01ms!important;transition-duration:.01ms!important}}\n</style>\n</head>\n<body>\n<canvas id="scene" aria-hidden="true"></canvas>\n<div class="veil"></div>\n\n<header class="top">\n  <a class="pill brand" href="/" aria-label="ONEX">\n    <span class="logo">\n      <svg viewBox="0 0 48 48" fill="none" aria-hidden="true">\n        <defs>\n          <linearGradient id="lgA" x1="6" y1="4" x2="42" y2="44" gradientUnits="userSpaceOnUse"><stop style="stop-color:var(--p)"/><stop offset="1" style="stop-color:var(--s)"/></linearGradient>\n          <linearGradient id="lgB" x1="14" y1="14" x2="34" y2="34" gradientUnits="userSpaceOnUse"><stop stop-color="#0d1020"/><stop offset="1" stop-color="#1a1f38"/></linearGradient>\n        </defs>\n        <path d="M24 2.5 42.6 13.25v21.5L24 45.5 5.4 34.75v-21.5Z" fill="url(#lgA)"/>\n        <path d="M24 8.2 37.6 16.1v15.8L24 39.8 10.4 31.9V16.1Z" fill="url(#lgB)"/>\n        <path d="M17.2 17.2 30.8 30.8M30.8 17.2 17.2 30.8" stroke="url(#lgA)" stroke-width="4.2" stroke-linecap="round"/>\n        <circle cx="24" cy="24" r="2.6" fill="#0d1020" style="stroke:var(--p)" stroke-width="1.4"/>\n        <circle class="orbit" cx="24" cy="2.5" r="2.2" fill="#fff"/>\n      </svg>\n    </span>\n    <span class="word"><b>ONE</b><i>X</i><small>v__ONEX_VER__</small></span>\n  </a>\n\n  <a class="pill tg" href="https://t.me/V2rayTun0" target="_blank" rel="noopener">\n    <span class="tg-ico"><svg viewBox="0 0 24 24" aria-hidden="true"><path fill="#fff" d="M9.8 15.3 9.6 19c.4 0 .6-.2.8-.4l2-1.9 4.1 3c.8.4 1.3.2 1.5-.7l2.7-12.7c.3-1.1-.4-1.6-1.1-1.3L3.7 10.8c-1.1.4-1.1 1-.2 1.3l4.1 1.3 9.6-6c.5-.3.9-.1.5.2Z"/></svg></span>\n    <span class="tg-txt"><b data-i18n="tg_title"></b><em data-i18n="tg_sub"></em></span>\n    <span class="tg-go"><span data-i18n="tg_join"></span><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M5 12h14m-6-6 6 6-6 6"/></svg></span>\n  </a>\n\n  <div class="pill ctrl">\n    <div class="seg" role="group" aria-label="Language">\n      <button type="button" data-lang="fa">FA</button>\n      <button type="button" data-lang="en">EN</button>\n    </div>\n    <span class="sep"></span>\n    <div class="sw" id="swatches" role="group" aria-label="Theme"></div>\n  </div>\n</header>\n\n<main>\n  <div class="col">\n    <div class="tilt" id="tilt"><div class="flip" id="flip"><div class="inner">\n\n      <section class="face front" id="front">\n        <h1><span data-i18n="title"></span><span class="grad">ONEX</span></h1>\n        <p class="sub" data-i18n="sub"></p>\n        <div class="bar-acc"></div>\n        <form id="loginForm" novalidate>\n          <div class="msg" id="err1"><div><p></p></div></div>\n          <div class="field" id="fUser">\n            <label class="l" for="u" data-i18n="user"></label>\n            <div class="ctl">\n              <svg class="ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M20 21a8 8 0 0 0-16 0"/><circle cx="12" cy="7" r="4"/></svg>\n              <input id="u" name="username" autocomplete="username" autocapitalize="none" spellcheck="false" data-i18n-ph="ph_user">\n            </div>\n          </div>\n          <div class="field" id="fPw">\n            <label class="l" for="p" data-i18n="pass"></label>\n            <div class="ctl">\n              <svg class="ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="4.5" y="10" width="15" height="10.5" rx="2.5"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/></svg>\n              <input id="p" name="password" type="password" autocomplete="current-password" data-i18n-ph="ph_pass">\n              <button type="button" class="eye" data-for="p" aria-label="show"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M2 12s3.5-6.5 10-6.5S22 12 22 12s-3.5 6.5-10 6.5S2 12 2 12Z"/><circle cx="12" cy="12" r="2.8"/></svg></button>\n            </div>\n            <div class="caps" id="caps1" hidden><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3 2 20h20L12 3Z"/><path d="M12 10v4M12 17.5v.5"/></svg><span data-i18n="caps"></span></div>\n          </div>\n          <div class="row">\n            <label class="chk"><input type="checkbox" checked> <span data-i18n="remember"></span></label>\n            <span class="secure"><i></i>TLS · secure</span>\n          </div>\n          <button class="btn" id="b1" type="submit">\n            <span data-i18n="login"></span>\n            <svg class="arr" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M5 12h14m-6-6 6 6-6 6"/></svg>\n          </button>\n        </form>\n        <div class="done" id="done"><svg viewBox="0 0 80 80" fill="none" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><circle cx="40" cy="40" r="34"/><path d="m27 41 9 9 18-19"/></svg><b data-i18n="welcome"></b><span id="doneSub"></span></div>\n      </section>\n\n      <section class="face back" id="back" aria-hidden="true">\n        <span class="chip"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><rect x="4.5" y="10" width="15" height="10.5" rx="2.5"/><path d="M8 10V7a4 4 0 0 1 8 0"/></svg><span data-i18n="first_chip"></span></span>\n        <h1 data-i18n="first_title"></h1>\n        <p class="sub" data-i18n="first_sub"></p>\n        <form id="firstForm" novalidate>\n          <div class="msg" id="err2"><div><p></p></div></div>\n          <div class="field">\n            <label class="l" for="nu"><span data-i18n="nu"></span> <em>3–32</em></label>\n            <div class="ctl">\n              <svg class="ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M20 21a8 8 0 0 0-16 0"/><circle cx="12" cy="7" r="4"/></svg>\n              <input id="nu" name="username" autocomplete="username" autocapitalize="none" spellcheck="false" value="admin" tabindex="-1">\n            </div>\n          </div>\n          <div class="field">\n            <label class="l" for="np"><span data-i18n="np"></span> <em>min 8</em></label>\n            <div class="ctl">\n              <svg class="ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="4.5" y="10" width="15" height="10.5" rx="2.5"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/></svg>\n              <input id="np" name="new-password" type="password" autocomplete="new-password" data-i18n-ph="ph_np" tabindex="-1">\n              <button type="button" class="eye" data-for="np" aria-label="show" tabindex="-1"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M2 12s3.5-6.5 10-6.5S22 12 22 12s-3.5 6.5-10 6.5S2 12 2 12Z"/><circle cx="12" cy="12" r="2.8"/></svg></button>\n            </div>\n            <div class="meter" id="meter" data-s="0"><i></i><i></i><i></i><i></i></div>\n            <div class="mlabel" id="mlabel"></div>\n          </div>\n          <div class="field">\n            <label class="l" for="np2" data-i18n="np2"></label>\n            <div class="ctl">\n              <svg class="ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="m5 12.5 4.5 4.5L19 7.5"/></svg>\n              <input id="np2" name="new-password-2" type="password" autocomplete="new-password" data-i18n-ph="ph_np2" tabindex="-1">\n            </div>\n            <div class="caps" id="caps2" hidden><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3 2 20h20L12 3Z"/><path d="M12 10v4M12 17.5v.5"/></svg><span data-i18n="caps"></span></div>\n          </div>\n          <button class="btn alt" id="b2" type="submit" tabindex="-1">\n            <span data-i18n="save"></span>\n            <svg class="arr" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round"><path d="M5 12h14m-6-6 6 6-6 6"/></svg>\n          </button>\n          <button type="button" class="ghost" id="backBtn" tabindex="-1" data-i18n="back"></button>\n        </form>\n      </section>\n\n    </div></div></div>\n\n    <div class="hintbar" id="hintbar"__HINT_ATTR__>\n      <span class="hk"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><circle cx="8" cy="15" r="4"/><path d="m10.8 12.2 8.7-8.7M16 6l2.5 2.5M14 8l2 2"/></svg></span>\n      <span class="ht">\n        <b><span data-i18n="hint_label"></span><code>admin / admin</code></b>\n        <span data-i18n="hint_tail"></span>\n      </span>\n      <button type="button" class="hfill" id="hfill" data-i18n="hint_fill"></button>\n    </div>\n  </div>\n</main>\n\n<footer class="foot">\n  <span>© 2026 ONEX · Crafted by Mehtif</span>\n  <span class="clock"><b id="clockT">--:--:--</b><span id="clockD"></span></span>\n</footer>\n\n<script>\n/* ========== I18N ========== */\nconst I18N={\n fa:{title:\'ورود به\',sub:\'حساب مدیر پنل ONEX را وارد کنید.\',user:\'نام کاربری\',pass:\'رمز عبور\',ph_user:\'نام کاربری خود را وارد کنید\',ph_pass:\'رمز عبور خود را وارد کنید\',\n  remember:\'مرا به خاطر بسپار\',login:\'ورود به پنل\',caps:\'توجه: Caps Lock روشن است\',welcome:\'خوش اومدی\',entering:\'در حال ورود به داشبورد…\',saved:\'رمز ذخیره شد، {u}. در حال ورود…\',\n  tg_title:\'کانال تلگرام ONEX\',tg_sub:\'آپدیت\u200cها و کانفیگ رایگان\',tg_join:\'عضو شو\',\n  first_chip:\'ورود اول\',first_title:\'رمز خودت رو بساز\',first_sub:\'برای امنیت پنل، نام کاربری و رمز جدید تعیین کن.\',nu:\'نام کاربری جدید\',np:\'رمز جدید\',np2:\'تکرار رمز\',ph_np:\'حداقل ۸ کاراکتر\',ph_np2:\'دوباره بنویس\',save:\'ذخیره و ورود\',back:\'بازگشت به ورود\',\n  hint_label:\'ورود اول:\',hint_tail:\'بعد از ورود، رمز اختصاصی خودت رو بساز.\',hint_fill:\'پر کردن\',\n  e_empty:\'نام کاربری و رمز عبور را وارد کنید.\',e_fail:\'ورود ناموفق بود.\',e_net:\'ارتباط با سرور برقرار نشد.\',e_bad:\'نام کاربری یا رمز اشتباه است.\',e_lock:\'ورود موقتاً مسدود است؛ کمی بعد تلاش کنید.\',\n  e_user:\'نام کاربری باید ۳ تا ۳۲ کاراکتر و فقط حروف، عدد، _ یا - باشد.\',e_len:\'رمز جدید باید حداقل ۸ کاراکتر باشد.\',e_weak:\'این رمز خیلی ساده است؛ یکی دیگه انتخاب کن.\',e_match:\'تکرار رمز یکسان نیست.\',e_session:\'نشست منقضی شد؛ دوباره وارد شو.\',e_save:\'ذخیره رمز ناموفق بود.\',\n  s:[\'\',\'ضعیف\',\'متوسط\',\'خوب\',\'قوی\'],custom:\'رنگ دلخواه\',locale:\'fa-IR\'},\n en:{title:\'Sign in to\',sub:\'Enter your ONEX panel admin account.\',user:\'Username\',pass:\'Password\',ph_user:\'Enter your username\',ph_pass:\'Enter your password\',\n  remember:\'Remember me\',login:\'Sign in\',caps:\'Heads up: Caps Lock is on\',welcome:\'Welcome back\',entering:\'Opening your dashboard…\',saved:\'Password saved, {u}. Signing in…\',\n  tg_title:\'ONEX Telegram channel\',tg_sub:\'Updates & free configs\',tg_join:\'Join\',\n  first_chip:\'First login\',first_title:\'Set your own password\',first_sub:\'Secure your panel with a new username and password.\',nu:\'New username\',np:\'New password\',np2:\'Repeat password\',ph_np:\'At least 8 characters\',ph_np2:\'Type it again\',save:\'Save & sign in\',back:\'Back to sign in\',\n  hint_label:\'First login:\',hint_tail:\'After signing in, set your own password.\',hint_fill:\'Autofill\',\n  e_empty:\'Enter your username and password.\',e_fail:\'Sign in failed.\',e_net:\'Could not reach the server.\',e_bad:\'Wrong username or password.\',e_lock:\'Too many attempts. Try again later.\',\n  e_user:\'Username must be 3–32 chars: letters, digits, _ or -.\',e_len:\'New password must be at least 8 characters.\',e_weak:\'That password is too simple, pick another.\',e_match:\'Passwords do not match.\',e_session:\'Session expired, please sign in again.\',e_save:\'Could not save the password.\',\n  s:[\'\',\'Weak\',\'Fair\',\'Good\',\'Strong\'],custom:\'Custom color\',locale:\'en-GB\'}\n};\nlet LANG=(localStorage.getItem(\'onex_lang\')===\'en\')?\'en\':\'fa\';\nconst t=k=>(I18N[LANG][k]!==undefined?I18N[LANG][k]:I18N.fa[k]);\nconst $=id=>document.getElementById(id);\nfunction applyLang(l){\n  LANG=l;try{localStorage.setItem(\'onex_lang\',l)}catch(e){}\n  const h=document.documentElement;h.lang=l;h.dir=l===\'fa\'?\'rtl\':\'ltr\';\n  document.querySelectorAll(\'[data-i18n]\').forEach(el=>{el.textContent=t(el.dataset.i18n)});\n  document.querySelectorAll(\'[data-i18n-ph]\').forEach(el=>{el.placeholder=t(el.dataset.i18nPh)});\n  document.querySelectorAll(\'.seg button\').forEach(b=>b.classList.toggle(\'on\',b.dataset.lang===l));\n  document.title=(l===\'fa\'?\'ONEX · ورود\':\'ONEX · Sign in\');\n  renderSwatches();tick();updateMeter();\n}\ndocument.querySelectorAll(\'.seg button\').forEach(b=>b.addEventListener(\'click\',()=>applyLang(b.dataset.lang)));\n\n/* ========== THEMES ========== */\nlet THEME=localStorage.getItem(\'onex_theme\')||\'aurora\';\nfunction setTheme(id){THEME=id;try{localStorage.setItem(\'onex_theme\',id)}catch(e){}applyThemeVars(id);renderSwatches()}\nfunction renderSwatches(){\n  const box=$(\'swatches\');box.innerHTML=\'\';\n  Object.entries(ONEX_THEMES).forEach(([id,th])=>{\n    const b=document.createElement(\'button\');b.type=\'button\';b.title=th[LANG];b.setAttribute(\'aria-label\',th[LANG]);\n    b.style.background=`linear-gradient(135deg,${th.p},${th.s})`;b.classList.toggle(\'on\',THEME===id);\n    b.addEventListener(\'click\',()=>setTheme(id));box.appendChild(b);\n  });\n  const lb=document.createElement(\'label\');lb.title=t(\'custom\');lb.classList.toggle(\'on\',THEME===\'custom\');\n  const inp=document.createElement(\'input\');inp.type=\'color\';inp.value=localStorage.getItem(\'onex_theme_custom\')||\'#38d9ff\';inp.setAttribute(\'aria-label\',t(\'custom\'));\n  inp.addEventListener(\'input\',()=>{try{localStorage.setItem(\'onex_theme_custom\',inp.value)}catch(e){}THEME=\'custom\';try{localStorage.setItem(\'onex_theme\',\'custom\')}catch(e){}applyThemeVars(\'custom\');\n    box.querySelectorAll(\'button,label\').forEach(x=>x.classList.remove(\'on\'));lb.classList.add(\'on\')});\n  lb.appendChild(inp);box.appendChild(lb);\n}\n\n/* ========== CLOCK ========== */\nfunction tick(){\n  const d=new Date(),loc=t(\'locale\');\n  try{$(\'clockT\').textContent=d.toLocaleTimeString(loc,{hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\',hour12:false});\n      $(\'clockD\').textContent=d.toLocaleDateString(loc,{weekday:\'short\',day:\'numeric\',month:\'short\'})}catch(e){}\n}\nsetInterval(tick,1000);\n\n/* ========== 3D GLASS SCENE ========== */\n(()=>{\n  const cv=$(\'scene\'),ctx=cv.getContext(\'2d\');\n  const reduce=matchMedia(\'(prefers-reduced-motion: reduce)\').matches;\n  let W,H,F,mobile;\n  function resize(){\n    W=innerWidth;H=innerHeight;mobile=W<900;\n    const dpr=Math.min(mobile?1.25:1.75,devicePixelRatio||1);\n    cv.width=W*dpr;cv.height=H*dpr;cv.style.width=W+\'px\';cv.style.height=H+\'px\';\n    ctx.setTransform(dpr,0,0,dpr,0,0);F=Math.max(H,W*.56)*1.05;\n  }\n  resize();addEventListener(\'resize\',resize);\n\n  const R=(a,b)=>a+Math.random()*(b-a);\n  const box=(w,h,d)=>{const v=[];for(const x of[-1,1])for(const y of[-1,1])for(const z of[-1,1])v.push([x*w/2,y*h/2,z*d/2]);\n    return{v,f:[[0,1,3,2],[4,5,7,6],[0,1,5,4],[2,3,7,6],[0,2,6,4],[1,3,7,5]]}};\n  const edgesOf=f=>{const s=new Set(),e=[];for(const a of f)for(let i=0;i<a.length;i++){const p=a[i],q=a[(i+1)%a.length],k=p<q?p+\'_\'+q:q+\'_\'+p;if(!s.has(k)){s.add(k);e.push([p,q])}}return e};\n  const prism=(n,r,h)=>{const v=[],f=[[],[]];for(let i=0;i<n;i++){const a=i/n*Math.PI*2+Math.PI/6;v.push([Math.cos(a)*r,-h/2,Math.sin(a)*r])}\n    for(let i=0;i<n;i++){const a=i/n*Math.PI*2+Math.PI/6;v.push([Math.cos(a)*r,h/2,Math.sin(a)*r])}\n    for(let i=0;i<n;i++){f[0].push(i);f[1].push(n+i);const j=(i+1)%n;f.push([i,j,n+j,n+i])}return{v,f}};\n  const ring=(n,ro,ri,h)=>{const v=[],f=[],e=[];for(const [r,y] of[[ro,-h/2],[ro,h/2],[ri,-h/2],[ri,h/2]])for(let i=0;i<n;i++){const a=i/n*Math.PI*2;v.push([Math.cos(a)*r,y,Math.sin(a)*r])}\n    for(let i=0;i<n;i++){const j=(i+1)%n;f.push([i,j,2*n+j,2*n+i],[n+i,n+j,3*n+j,3*n+i],[i,j,n+j,n+i],[2*n+i,2*n+j,3*n+j,3*n+i]);for(let k=0;k<4;k++)e.push([k*n+i,k*n+j])}\n    return{v,f,e}};\n  const octa=r=>({v:[[r,0,0],[-r,0,0],[0,r,0],[0,-r,0],[0,0,r],[0,0,-r]],f:[[0,2,4],[2,1,4],[1,3,4],[3,0,4],[2,0,5],[1,2,5],[3,1,5],[0,3,5]]});\n  const xshape=()=>{const out={v:[],f:[]};for(const ang of[Math.PI/4,-Math.PI/4]){const b=box(2.9,.56,.56),c=Math.cos(ang),s=Math.sin(ang),o=out.v.length;\n    b.v.forEach(([x,y,z])=>out.v.push([x*c-y*s,x*s+y*c,z]));b.f.forEach(fc=>out.f.push(fc.map(i=>i+o)))}return out};\n  const G={hex:prism(6,1,.34),hexT:prism(6,.8,1.1),ring:ring(22,1,.7,.22),cube:box(1,1,1),oct:octa(1.05),X:xshape()};\n  for(const k in G)if(!G[k].e)G[k].e=edgesOf(G[k].f);\n\n  const layout=[[\'hex\',-5.6,-2.3,9,.9],[\'ring\',5.2,-2.6,8.5,1.05],[\'cube\',-4.4,2.5,7.6,.8],[\'oct\',5.9,2,10,.85],[\'hexT\',1.4,-3.3,12,.8],\n    [\'ring\',-1.6,3,11,.9],[\'cube\',3.4,3.1,13,.6],[\'hex\',-7.4,.4,14,1.2],[\'oct\',-3,-3.4,14,.6],[\'ring\',7.6,-.2,15,1.25],[\'cube\',.6,3.6,9.5,.45],\n    [\'hexT\',2.4,-1,17,.7],[\'oct\',-6.2,-1.2,18,.9],[\'hex\',4.4,.9,19,1]];\n  const objs=layout.filter((_,i)=>!mobile||i%2===0).map(([g,x,y,z,s])=>({g:G[g],x,y,z,s,rx:R(0,6),ry:R(0,6),rz:R(0,6),\n    vx:R(-.25,.25),vy:R(-.3,.3),vz:R(-.15,.15),ph:R(0,6),amp:R(.12,.3),c:Math.random(),ring:g===\'ring\'}));\n  const hero={g:G.X,x:-2.8,y:-.1,z:7.4,s:1.2,rx:0,ry:0,rz:0,ph:0,amp:.12,c:0,hero:true};\n  objs.push(hero);\n  const dust=Array.from({length:mobile?60:150},()=>({x:R(-11,11),y:R(-7,7),z:R(3,22),tw:R(0,6)}));\n\n  const mouse={x:0,y:0,tx:0,ty:0};window.__mouse=mouse;\n  const cam={x:0,y:0};\n  const cur={p:[56,217,255],s:[139,92,246]};\n  const L=(()=>{const v=[-.45,-.7,-.55],m=Math.hypot(...v);return v.map(a=>a/m)})();\n  const mix=(a,b,k)=>[a[0]+(b[0]-a[0])*k,a[1]+(b[1]-a[1])*k,a[2]+(b[2]-a[2])*k];\n  const rgba=(c,a)=>`rgba(${c[0]|0},${c[1]|0},${c[2]|0},${a<0?0:a>1?1:a})`;\n\n  function xf(o,T){\n    const cx=Math.cos(o.rx),sx=Math.sin(o.rx),cy=Math.cos(o.ry),sy=Math.sin(o.ry),cz=Math.cos(o.rz),sz=Math.sin(o.rz),bob=Math.sin(T*.6+o.ph)*o.amp,out=[];\n    for(const p of o.g.v){let x=p[0]*o.s,y=p[1]*o.s,z=p[2]*o.s,t;\n      t=x*cz-y*sz;y=x*sz+y*cz;x=t; t=y*cx-z*sx;z=y*sx+z*cx;y=t; t=x*cy+z*sy;z=-x*sy+z*cy;x=t;\n      out.push([x+o.x-cam.x,y+o.y+bob-cam.y,z+o.z])}\n    return out;\n  }\n  const P2=p=>{const k=F/p[2];return[W/2+p[0]*k,H/2+p[1]*k]};\n\n  let T=0,last=performance.now(),intro=0;\n  function frame(now){\n    const dt=Math.min(.05,(now-last)/1000);last=now;const sp=reduce?.15:1;T+=dt*sp;intro=Math.min(1,intro+dt/1.6);\n    const ei=1-Math.pow(1-intro,4);\n    const tg=window.__themeRGB||cur;cur.p=mix(cur.p,tg.p,.06);cur.s=mix(cur.s,tg.s,.06);\n    mouse.x+=(mouse.tx-mouse.x)*.05;mouse.y+=(mouse.ty-mouse.y)*.05;cam.x=mouse.x*.7;cam.y=mouse.y*.45;\n    const rtl=document.documentElement.dir===\'rtl\';\n    const hx=mobile?0:(rtl?-3:3),hz=mobile?11:7.4;hero.x+=(hx-hero.x)*.05;hero.z+=(hz-hero.z)*.05;\n    hero.ry=Math.sin(T*.35)*.65;hero.rx=-.12+Math.sin(T*.23)*.12;hero.rz=Math.sin(T*.17)*.05;\n\n    ctx.fillStyle=\'#05060d\';ctx.fillRect(0,0,W,H);\n    // drifting color blobs\n    for(const [c,ox,oy,r] of[[cur.p,.25,.3,.55],[cur.s,.78,.72,.6],[cur.p,.7,.15,.35]]){\n      const x=W*(ox+Math.sin(T*.13+ox*9)*.05),y=H*(oy+Math.cos(T*.11+oy*7)*.05),rr=Math.max(W,H)*r;\n      const g=ctx.createRadialGradient(x,y,0,x,y,rr);g.addColorStop(0,rgba(c,.16*ei));g.addColorStop(1,rgba(c,0));ctx.fillStyle=g;ctx.fillRect(0,0,W,H)}\n\n    // perspective floor grid\n    ctx.lineWidth=1;const fy=3.6,off=(T*1.2)%1.6;\n    for(let i=-9;i<=9;i++){const a=P2([i*1.6-cam.x,fy-cam.y,3.2]),b=P2([i*1.6-cam.x,fy-cam.y,30]);\n      const g=ctx.createLinearGradient(a[0],a[1],b[0],b[1]);g.addColorStop(0,rgba(cur.p,.2*ei));g.addColorStop(1,rgba(cur.p,0));ctx.strokeStyle=g;ctx.beginPath();ctx.moveTo(a[0],a[1]);ctx.lineTo(b[0],b[1]);ctx.stroke()}\n    for(let j=0;j<18;j++){const z=3.2+j*1.6-off;if(z<3.2)continue;const a=P2([-16-cam.x,fy-cam.y,z]),b=P2([16-cam.x,fy-cam.y,z]);\n      ctx.strokeStyle=rgba(cur.p,.18*(1-z/32)*ei);ctx.beginPath();ctx.moveTo(a[0],a[1]);ctx.lineTo(b[0],b[1]);ctx.stroke()}\n\n    // dust\n    for(const d of dust){d.y-=dt*.25*sp;if(d.y<-7)d.y=7;const p=[d.x-cam.x*.6,d.y-cam.y*.6,d.z];const q=P2(p),k=F/p[2];\n      const a=(.25+.35*Math.sin(T*2+d.tw))*(1-d.z/24)*ei;ctx.fillStyle=rgba(mix(cur.p,[255,255,255],.5),a);const r=Math.max(.6,k*.012);ctx.fillRect(q[0]-r/2,q[1]-r/2,r,r)}\n\n    // hero glow\n    {const q=P2([hero.x-cam.x,hero.y-cam.y,hero.z]),rr=F/hero.z*hero.s*2.4;const g=ctx.createRadialGradient(q[0],q[1],0,q[0],q[1],rr);\n      g.addColorStop(0,rgba(cur.p,.22*ei*(mobile?.6:1)));g.addColorStop(.5,rgba(cur.s,.08*ei));g.addColorStop(1,rgba(cur.s,0));ctx.fillStyle=g;ctx.fillRect(q[0]-rr,q[1]-rr,rr*2,rr*2)}\n\n    // geometry\n    const items=[];\n    for(const o of objs){\n      if(!o.hero){o.rx+=o.vx*dt*sp;o.ry+=o.vy*dt*sp;o.rz+=o.vz*dt*sp}\n      const w=xf(o,T),s=w.map(P2),fog=Math.max(0,Math.min(1,1.25-(o.z-5)/16))*ei*(o.hero&&mobile?.6:1);\n      const base=o.hero?null:mix(cur.p,cur.s,o.c);\n      for(const fc of o.g.f){\n        const a=w[fc[0]],b=w[fc[1]],c=w[fc[2]];const ux=b[0]-a[0],uy=b[1]-a[1],uz=b[2]-a[2],vx=c[0]-a[0],vy=c[1]-a[1],vz=c[2]-a[2];\n        let nx=uy*vz-uz*vy,ny=uz*vx-ux*vz,nz=ux*vy-uy*vx;const m=Math.hypot(nx,ny,nz)||1;nx/=m;ny/=m;nz/=m;\n        const br=Math.abs(nx*L[0]+ny*L[1]+nz*L[2]),rim=1-Math.abs(nz);let z=0;for(const i of fc)z+=w[i][2];z/=fc.length;\n        const col=o.hero?mix(cur.p,cur.s,.5+.5*nx):base;\n        items.push({t:0,z,pts:fc.map(i=>s[i]),fill:rgba(mix(col,[255,255,255],br*.25),(o.hero?.07:.035)+(o.hero?.2:.13)*br*fog+.1*rim*fog)})}\n      for(const [i,j] of o.g.e){\n        const z=(w[i][2]+w[j][2])/2,near=Math.max(0,1-(z-4)/18),col=o.hero?mix(cur.p,cur.s,(s[i][0]-s[j][0])>0?.2:.8):base;\n        items.push({t:1,z:z-.001,a:s[i],b:s[j],st:rgba(mix(col,[255,255,255],.3),((o.ring?.22:.35)+.5*near)*fog*(o.hero?1.25:1)),lw:o.hero?1.6:1})}\n    }\n    items.sort((p,q)=>q.z-p.z);\n    for(const it of items){\n      if(it.t===0){ctx.fillStyle=it.fill;ctx.beginPath();ctx.moveTo(it.pts[0][0],it.pts[0][1]);for(let k=1;k<it.pts.length;k++)ctx.lineTo(it.pts[k][0],it.pts[k][1]);ctx.closePath();ctx.fill()}\n      else{ctx.strokeStyle=it.st;ctx.lineWidth=it.lw;ctx.beginPath();ctx.moveTo(it.a[0],it.a[1]);ctx.lineTo(it.b[0],it.b[1]);ctx.stroke()}\n    }\n    requestAnimationFrame(frame);\n  }\n  requestAnimationFrame(frame);\n})();\n\n/* ========== CARD INTERACTION ========== */\nconst tilt=$(\'tilt\'),flip=$(\'flip\'),inner=flip;\nconst fine=matchMedia(\'(pointer:fine)\').matches&&!matchMedia(\'(prefers-reduced-motion: reduce)\').matches;\naddEventListener(\'pointermove\',e=>{\n  window.__mouse.tx=e.clientX/innerWidth*2-1;window.__mouse.ty=e.clientY/innerHeight*2-1;\n  if(!fine)return;const r=tilt.getBoundingClientRect();const x=(e.clientX-r.left)/r.width-.5,y=(e.clientY-r.top)/r.height-.5;\n  const k=Math.max(0,1-Math.hypot(x,y)/1.6);flip.style.transform=`rotateX(${(-y*8*k).toFixed(2)}deg) rotateY(${(x*10*k).toFixed(2)}deg)`;\n  document.querySelectorAll(\'.face\').forEach(f=>{const fr=f.getBoundingClientRect();f.style.setProperty(\'--mx\',((e.clientX-fr.left)/fr.width*100)+\'%\');f.style.setProperty(\'--my\',((e.clientY-fr.top)/fr.height*100)+\'%\')});\n});\ndocument.addEventListener(\'pointerleave\',()=>{flip.style.transform=\'\'});\n\n/* ========== FORM LOGIC ========== */\nfunction showErr(box,text){box.querySelector(\'p\').textContent=text;box.classList.add(\'show\')}\nfunction hideErr(box){box.classList.remove(\'show\')}\ndocument.querySelectorAll(\'.eye\').forEach(b=>b.addEventListener(\'click\',()=>{const i=$(b.dataset.for);i.type=i.type===\'password\'?\'text\':\'password\';i.focus()}));\nfunction setFace(back){\n  flip.classList.toggle(\'on\',back);\n  $(\'front\').setAttribute(\'aria-hidden\',back);$(\'back\').setAttribute(\'aria-hidden\',!back);\n  $(\'front\').querySelectorAll(\'input,button\').forEach(el=>el.tabIndex=back?-1:0);\n  $(\'back\').querySelectorAll(\'input,button\').forEach(el=>el.tabIndex=back?0:-1);\n  setTimeout(()=>(back?$(\'np\'):$(\'p\')).focus({preventScroll:true}),650);\n}\nfunction capsWatch(input,box){[\'keydown\',\'keyup\'].forEach(ev=>input.addEventListener(ev,e=>{if(e.getModifierState)$(box).hidden=!e.getModifierState(\'CapsLock\')}));input.addEventListener(\'blur\',()=>{$(box).hidden=true})}\ncapsWatch($(\'p\'),\'caps1\');capsWatch($(\'np\'),\'caps2\');capsWatch($(\'np2\'),\'caps2\');\n$(\'hfill\').addEventListener(\'click\',()=>{$(\'u\').value=\'admin\';$(\'p\').value=\'admin\';$(\'p\').focus()});\n\nlet firstLoginPw=\'\';\nasync function postJSON(url,data){\n  const r=await fetch(url,{method:\'POST\',credentials:\'same-origin\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(data)});\n  let j={};try{j=await r.json()}catch(_){}\n  return{ok:r.ok,status:r.status,data:j};\n}\nfunction srvErr(res,fallback){\n  if(LANG===\'en\'){if(res.status===401)return t(\'e_bad\');if(res.status===429)return t(\'e_lock\');return t(fallback)}\n  return(res.data&&res.data.detail)||t(fallback);\n}\nfunction goDashboard(msg){$(\'doneSub\').textContent=msg||t(\'entering\');$(\'done\').classList.add(\'show\');setTimeout(()=>{location.replace(\'/dashboard?login=1\')},800)}\n$(\'loginForm\').addEventListener(\'submit\',async e=>{\n  e.preventDefault();\n  const err=$(\'err1\'),u=$(\'u\').value.trim().toLowerCase(),p=$(\'p\').value;\n  hideErr(err);$(\'fUser\').classList.remove(\'bad\');$(\'fPw\').classList.remove(\'bad\');\n  if(!u||!p){if(!u)$(\'fUser\').classList.add(\'bad\');if(!p)$(\'fPw\').classList.add(\'bad\');return showErr(err,t(\'e_empty\'))}\n  $(\'b1\').disabled=true;\n  try{\n    const res=await postJSON(\'/api/login\',{username:u,password:p});\n    if(!res.ok){$(\'fPw\').classList.add(\'bad\');return showErr(err,srvErr(res,\'e_fail\'))}\n    if(res.data.must_change){firstLoginPw=p;$(\'nu\').value=u;return setFace(true)}\n    goDashboard();\n  }catch(_){showErr(err,t(\'e_net\'))}finally{$(\'b1\').disabled=false}\n});\nconst meter=$(\'meter\'),mlabel=$(\'mlabel\');\nfunction updateMeter(){const v=$(\'np\').value;let s=0;if(v.length>=8)s++;if(/[a-z]/.test(v)&&/[A-Z]/.test(v))s++;if(/\\d/.test(v))s++;if(/[^A-Za-z0-9]/.test(v))s++;if(v&&s===0)s=1;meter.dataset.s=s;mlabel.textContent=v?t(\'s\')[s]:\'\'}\n$(\'np\').addEventListener(\'input\',updateMeter);\n$(\'backBtn\').addEventListener(\'click\',()=>{hideErr($(\'err2\'));setFace(false)});\n$(\'firstForm\').addEventListener(\'submit\',async e=>{\n  e.preventDefault();\n  const err=$(\'err2\'),nu=$(\'nu\').value.trim().toLowerCase(),np=$(\'np\').value,np2=$(\'np2\').value;\n  hideErr(err);\n  if(!/^[a-z0-9_-]{3,32}$/.test(nu))return showErr(err,t(\'e_user\'));\n  if(np.length<8)return showErr(err,t(\'e_len\'));\n  if([\'admin\',nu,\'12345678\',\'password\'].includes(np.toLowerCase()))return showErr(err,t(\'e_weak\'));\n  if(np!==np2)return showErr(err,t(\'e_match\'));\n  $(\'b2\').disabled=true;\n  try{\n    const res=await postJSON(\'/api/change-password\',{new_username:nu,current_password:firstLoginPw,new_password:np,repeat_password:np2});\n    if(!res.ok){if(res.status===401){setFace(false);return showErr($(\'err1\'),t(\'e_session\'))}return showErr(err,srvErr(res,\'e_save\'))}\n    firstLoginPw=\'\';$(\'u\').value=nu;$(\'p\').value=\'\';$(\'hintbar\').hidden=true;\n    setFace(false);setTimeout(()=>goDashboard(t(\'saved\').replace(\'{u}\',nu)),700);\n  }catch(_){showErr(err,t(\'e_net\'))}finally{$(\'b2\').disabled=false}\n});\n\napplyLang(LANG);\nrequestAnimationFrame(()=>document.body.classList.add(\'ready\'));\n</script>\n</body>\n</html>\n'


def render_login_html() -> str:
    """Render the login page without pre-filling credentials.

    The username is shown only as a faint placeholder so the user knows which
    account is expected. The password is never rendered or stored in plaintext;
    the default password is mentioned only when the current password is still
    the initial admin password.
    """
    username = escape_html(str(AUTH.get("username") or "admin").strip() or "admin")
    password_placeholder = (
        "admin"
        if AUTH.get("password_hash") == hash_password("admin")
        else "رمز عبور فعلی"
    )
    hint_attr = "" if AUTH.get("password_hash") == hash_password("admin") else " hidden"
    return (
        LOGIN_HTML
        .replace("__LOGIN_USERNAME__", username)
        .replace("__LOGIN_PASSWORD_PLACEHOLDER__", password_placeholder)
        .replace("__HINT_ATTR__", hint_attr)
        .replace("__ONEX_VER__", str(APP_VERSION))
    )





def login_error_html(
    message: str,
):
    safe_message = escape_html(
        message
    )

    return LOGIN_HTML.replace(
        "</form>",
        (
            f"""
            <div class="error">
                {safe_message}
            </div>
            </form>
            """
        ),
    )



# ============================================================
# FIRST-RUN SETUP
# ============================================================

@app.get("/api/setup/status")
async def setup_status():
    return {
        "password_configured": True,
        "needs_setup": False,
        "username": AUTH.get("username", "admin"),
    }


@app.post("/api/setup/password")
async def setup_password(request: Request):
    raise HTTPException(status_code=410, detail="راه‌اندازی اولیه حذف شده است؛ از تنظیمات پنل استفاده کنید")
    if AUTH.get("password_configured") and AUTH.get("password_hash"):
        raise HTTPException(status_code=400, detail="رمز قبلاً تنظیم شده است")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر")
    pw = str(body.get("password") or "")
    rp = str(body.get("repeat_password") or body.get("confirm") or "")
    if len(pw) < 6:
        raise HTTPException(status_code=400, detail="رمز باید حداقل ۶ کاراکتر باشد")
    if pw != rp:
        raise HTTPException(status_code=400, detail="تکرار رمز یکسان نیست")
    AUTH["password_hash"] = hash_password(pw)
    AUTH["password_configured"] = True
    await save_state()
    token = await create_session()
    response = JSONResponse({"ok": True, "message": "رمز تنظیم شد"})
    set_auth_cookie(response, request, token)
    log_activity("auth", "رمز اولیه پنل تنظیم شد", "ok")
    return response


@app.get(
    "/login",
    response_class=HTMLResponse,
)
async def login_page(
    request: Request,
):

    if await is_valid_session(
        request.cookies.get(
            SESSION_COOKIE
        )
    ):
        return RedirectResponse(
            "/dashboard"
        )

    return HTMLResponse(
        render_login_html(),
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.post("/login")
async def login_form(
    request: Request,
):
    if not (AUTH.get("password_configured") and AUTH.get("password_hash")):
        return HTMLResponse(login_error_html("ورود با نام کاربری و رمز عبور انجام می‌شود"))


    try:

        content_type = (
            request.headers
            .get(
                "content-type",
                "",
            )
            .lower()
        )

        if "application/json" in content_type:

            body = await request.json()

            username = str(body.get("username", "")).strip().lower()
            password = str(
                body.get(
                    "password",
                    "",
                )
            ).strip()

        else:

            raw = await request.body()

            parsed = parse_qs(
                raw.decode(
                    "utf-8",
                    errors="ignore",
                )
            )

            username = (
                parsed.get(
                    "username",
                    [""],
                )[0]
                .strip().lower()
            )
            password = (
                parsed.get(
                    "password",
                    [""],
                )[0]
                .strip()
            )

    except Exception as exc:

        logger.exception(
            "Login parser error: %s",
            exc,
        )

        return HTMLResponse(
            login_error_html(
                "خطا در پردازش اطلاعات ورود."
            ),
            status_code=400,
        )

    ip = client_ip(request)

    blocked, retry_after = login_is_blocked(ip)
    if blocked:
        minutes = max(1, (retry_after + 59) // 60)
        return HTMLResponse(
            login_error_html(
                f"به دلیل تلاش‌های ناموفق متعدد، ورود موقتاً مسدود شده است. حدود {minutes} دقیقه دیگر دوباره تلاش کنید."
            ),
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )

    if not username or not password:
        register_login_failure(ip)
        return HTMLResponse(
            login_error_html(
                "نام کاربری و رمز عبور را وارد کنید."
            ),
            status_code=400,
        )

    if username != AUTH.get("username", "admin") or hash_password(password) != AUTH["password_hash"]:

        locked, value = register_login_failure(ip)
        if locked:
            return HTMLResponse(
                login_error_html(
                    "تعداد تلاش‌های ناموفق بیش از حد مجاز بود. این IP برای ۱۵ دقیقه مسدود شد."
                ),
                status_code=429,
                headers={"Retry-After": str(LOGIN_LOCKOUT_SECONDS)},
            )

        remaining = value
        log_activity(
            "auth",
            (
                f"تلاش ورود ناموفق از {ip}؛ "
                f"{remaining} تلاش باقی مانده"
            ),
            "err",
        )

        return HTMLResponse(
            login_error_html(
                f"رمز عبور اشتباه است. {remaining} تلاش دیگر باقی مانده است."
            ),
            status_code=401,
        )

    clear_login_failures(ip)

    token = await create_session()

    response = RedirectResponse(
        "/dashboard?login=1",
        status_code=303,
    )

    set_auth_cookie(
        response,
        request,
        token,
    )

    log_activity(
        "auth",
        (
            f"ورود موفق به پنل "
            f"از {client_ip(request)}"
        ),
        "ok",
    )

    return response


@app.post("/api/login")
async def api_login(request: Request):
    if not (AUTH.get("password_configured") and AUTH.get("password_hash")):
        raise HTTPException(status_code=400, detail="ورود نیاز به حساب کاربری دارد")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر است")
    password = str(body.get("password", "")).strip()
    username = str(body.get("username", "")).strip().lower()
    ip = client_ip(request)
    blocked, retry_after = login_is_blocked(ip)
    if blocked:
        raise HTTPException(status_code=429, detail=f"ورود موقتاً مسدود است. حدود {max(1, (retry_after + 59) // 60)} دقیقه دیگر تلاش کنید.", headers={"Retry-After": str(retry_after)})
    if not password:
        register_login_failure(ip)
        raise HTTPException(status_code=400, detail="رمز عبور الزامی است")
    meta = {"role": "owner", "admin_id": None, "username": AUTH.get("username", "admin")}
    ok = False
    if username and username == AUTH.get("username", "admin"):
        if hash_password(password) == AUTH["password_hash"]:
            ok = True
    elif username:
        aid, admin = find_admin_by_username(username)
        if admin and admin.get("password_hash") == hash_password(password):
            if not admin_is_valid(admin):
                raise HTTPException(status_code=403, detail="حساب مسدود یا منقضی شده است")
            ok = True
            meta = {"role": "admin", "admin_id": aid, "username": username}
    if not ok:
        locked, value = register_login_failure(ip)
        if locked:
            raise HTTPException(status_code=429, detail="تعداد تلاش بیش از حد. ۱۵ دقیقه صبر کنید.", headers={"Retry-After": str(LOGIN_LOCKOUT_SECONDS)})
        raise HTTPException(status_code=401, detail=f"نام کاربری یا رمز اشتباه است. {value} تلاش باقی‌مانده")
    clear_login_failures(ip)
    token = await create_session(meta)
    response = JSONResponse({"ok": True, "role": meta["role"], "username": meta["username"]})
    set_auth_cookie(response, request, token)
    log_activity("auth", f"ورود موفق ({meta['username']}) از {ip}", "ok")
    return response


@app.post("/api/logout")
async def api_logout(request: Request):
    """Destroy the current session and clear the auth cookie."""
    token = request.cookies.get(SESSION_COOKIE)
    await destroy_session(token)

    response = JSONResponse({"ok": True})
    response.delete_cookie(
        SESSION_COOKIE,
        path="/",
        httponly=True,
        samesite="lax",
    )
    return response





# ============================================================
# CHANGE PASSWORD
# ============================================================

@app.post("/api/change-password")
async def api_change_password(
    request: Request,
    token=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="اطلاعات نامعتبر است",
        )

    current_password = str(
        body.get(
            "current_password",
            "",
        )
    )
    new_username = str(body.get("new_username") or AUTH.get("username", "admin")).strip().lower()

    if (
        hash_password(current_password)
        != AUTH["password_hash"]
    ):
        raise HTTPException(
            status_code=400,
            detail="رمز فعلی اشتباه است",
        )

    new_password = str(
        body.get(
            "new_password",
            "",
        )
    )

    repeat_password = str(
        body.get(
            "repeat_password",
            "",
        )
    )

    if not new_username or len(new_username) < 3 or len(new_username) > 32 or not new_username.replace("_", "").replace("-", "").isalnum():
        raise HTTPException(status_code=400, detail="نام کاربری باید ۳ تا ۳۲ کاراکتر و فقط شامل حروف، عدد، _ یا - باشد")

    if len(new_password) < 6:
        raise HTTPException(
            status_code=400,
            detail="رمز جدید باید حداقل ۶ کاراکتر باشد",
        )

    if new_password != repeat_password:
        raise HTTPException(
            status_code=400,
            detail="تکرار رمز عبور یکسان نیست",
        )

    AUTH[
        "username"
    ] = new_username
    AUTH[
        "password_hash"
    ] = hash_password(
        new_password
    )
    AUTH["password_configured"] = True
    AUTH["credentials_version"] = 1

    async with SESSIONS_LOCK:

        SESSIONS.clear()

        SESSIONS[token] = (
            time.time()
            + SESSION_TTL
        )

    await save_state()

    log_activity(
        "auth",
        "رمز عبور پنل تغییر کرد",
        "ok",
    )

    return {
        "ok": True
    }



# ============================================================
# ADVANCED CONFIG PROFILE
# ============================================================
ADVANCED_DEFAULTS = {
    "tls": {
        "enabled": True, "mode": "tls", "sni": "", "server_name": "",
        "alpn": "", "allow_insecure": False, "min_version": "1.2", "max_version": "1.3",
        "certificate_path": "", "key_path": "",
        "reality": {"public_key": "", "private_key": "", "short_id": "", "spider_x": "", "fingerprint": "chrome", "handshake_server": "", "handshake_port": 443, "max_time_difference": ""},
    },
    "host": {"address": "", "host": "", "path": "", "service_name": "", "authority": ""},
    "fingerprint": {"enabled": True, "value": "chrome", "randomize": False},
    "network": {"type": "ws", "mode": "", "path": "", "service_name": "", "http_version": "1.1"},
    "headers": {"host": "", "user_agent": "", "extra": []},
    "routing": {"domain_strategy": "", "route": "", "proxy_protocol": False, "sniff": False, "sniff_override": False, "sniff_timeout": "300ms"},
    "transport": {"packet_encoding": "", "early_data": 0, "max_early_data": 0, "early_data_header_name": "Sec-WebSocket-Protocol", "padding": False},
    "listener": {"listen": "0.0.0.0", "bind_interface": "", "routing_mark": 0, "netns": "", "reuse_addr": True, "tcp_fast_open": False, "tcp_multi_path": False, "disable_tcp_keep_alive": False, "tcp_keep_alive": "5m", "tcp_keep_alive_interval": "75s", "udp_fragment": False, "udp_timeout": "5m"},
    "ports": [443],
}


def _advanced_copy_defaults():
    return json.loads(json.dumps(ADVANCED_DEFAULTS, ensure_ascii=False))


def normalize_advanced_config(raw):
    base = _advanced_copy_defaults()
    if not isinstance(raw, dict):
        return base
    def put(section, key, value, limit=300):
        if section in raw and isinstance(raw.get(section), dict) and key in raw[section]:
            v = raw[section].get(key)
            if isinstance(v, bool): base[section][key] = v
            elif isinstance(v, (int, float)): base[section][key] = v
            else: base[section][key] = str(v or "")[:limit]
    for sec, keys in {
        "tls": ["mode","sni","server_name","alpn","min_version","max_version","certificate_path","key_path"],
        "host": ["address","host","path","service_name","authority"],
        "fingerprint": ["value"],
        "network": ["type","mode","path","service_name","http_version"],
        "routing": ["domain_strategy","route","sniff_timeout"],
        "transport": ["packet_encoding","early_data_header_name"],
        "listener": ["listen","bind_interface","routing_mark","netns","tcp_keep_alive","tcp_keep_alive_interval","udp_timeout"],
    }.items():
        for k in keys: put(sec,k,raw.get(sec,{}).get(k) if isinstance(raw.get(sec),dict) else None)
    for sec, keys in {"tls":["enabled","allow_insecure"],"fingerprint":["enabled","randomize"],"routing":["proxy_protocol","sniff","sniff_override"],"transport":["padding"],"listener":["reuse_addr","tcp_fast_open","tcp_multi_path","disable_tcp_keep_alive","udp_fragment"]}.items():
        for k in keys:
            if isinstance(raw.get(sec),dict) and k in raw[sec]: base[sec][k] = bool(raw[sec][k])
    for sec, keys in {"transport":["early_data","max_early_data"],"listener":["routing_mark"]}.items():
        for k in keys:
            if isinstance(raw.get(sec),dict) and k in raw[sec]: base[sec][k] = safe_int(raw[sec][k],0,0,65535)
    if isinstance(raw.get("tls"),dict) and isinstance(raw["tls"].get("reality"),dict):
        r=raw["tls"]["reality"]
        for k in base["tls"]["reality"]:
            if k in r:
                base["tls"]["reality"][k] = safe_int(r.get(k), 443, 1, 65535) if k == "handshake_port" else str(r.get(k) or "")[:300]
    if isinstance(raw.get("headers"),dict):
        for k in ("host","user_agent"):
            if k in raw["headers"]: base["headers"][k]=str(raw["headers"].get(k) or "")[:300]
        extra=raw["headers"].get("extra",[])
        if isinstance(extra,list):
            base["headers"]["extra"]=[str(x)[:300] for x in extra[:30] if str(x).strip()]
    ports=raw.get("ports")
    if isinstance(ports,list):
        vals=[]
        for x in ports[:12]:
            n=safe_int(x,0,1,65535)
            if n and n not in vals: vals.append(n)
        if vals: base["ports"]=vals
    # Keep the legacy top-level fingerprint/ALPN in sync with advanced values.
    if base["fingerprint"]["value"] not in FINGERPRINTS: base["fingerprint"]["value"]="chrome"
    if base["tls"]["mode"] not in {"none","tls","reality"}: base["tls"]["mode"]="tls"
    if base["network"]["type"] not in {"ws","xhttp","grpc","tcp","http","h2","quic","kcp"}: base["network"]["type"]="ws"
    return base


# ============================================================
# CREATE LINK
# ============================================================

@app.post("/api/links")
async def create_link_api(
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()

        if not isinstance(body, dict):
            raise ValueError(
                "body is not object"
            )

    except Exception as exc:

        logger.exception(
            "Create link JSON error: %s",
            exc,
        )

        raise HTTPException(
            status_code=400,
            detail="اطلاعات ارسال‌شده معتبر نیست.",
        )

    limit_value = safe_float(
        body.get(
            "limit_value",
            0,
        )
    )

    limit_unit = str(
        body.get(
            "limit_unit",
            "GB",
        )
        or "GB"
    ).upper()

    limit_bytes = (
        0
        if limit_value <= 0
        else parse_size_to_bytes(
            limit_value,
            limit_unit,
        )
    )

    expires_days = safe_int(
        body.get(
            "expires_days",
            0,
        ),
        minimum=0,
    )

    expires_at = (
        (
            datetime.now()
            + timedelta(
                days=expires_days
            )
        ).isoformat()
        if expires_days > 0
        else None
    )

    port = safe_int(
        body.get(
            "port",
            DEFAULT_PORT,
        ),
        default=DEFAULT_PORT,
        minimum=MIN_PORT,
        maximum=MAX_PORT,
    )

    ip_limit = safe_int(
        body.get(
            "ip_limit",
            0,
        ),
        minimum=0,
    )

    speed_value = safe_float(
        body.get(
            "speed_limit_value",
            0,
        )
    )

    speed_unit = str(
        body.get(
            "speed_limit_unit",
            "MBIT",
        )
        or "MBIT"
    ).upper()

    speed_bytes = (
        0
        if speed_value <= 0
        else parse_speed_to_bytes(
            speed_value,
            speed_unit,
        )
    )

    connection_limit = safe_int(
        body.get(
            "connection_limit",
            0,
        ),
        minimum=0,
    )

    protocol = str(
        body.get(
            "protocol",
            DEFAULT_PROTOCOL,
        )
        or DEFAULT_PROTOCOL
    ).strip()

    if not PROTOCOLS:
        raise HTTPException(503, "No protocol backend is available")
    if protocol not in PROTOCOLS:
        protocol = PROTOCOLS[0]

    fingerprint = str(
        body.get(
            "fingerprint",
            DEFAULT_FINGERPRINT,
        )
        or DEFAULT_FINGERPRINT
    ).strip().lower()

    if fingerprint not in FINGERPRINTS:
        fingerprint = DEFAULT_FINGERPRINT

    fragment = str(
        body.get(
            "fragment",
            "off",
        )
        or "off"
    ).strip().lower()

    allowed_fragments = {
        "off",
        "safe",
        "balanced",
        "aggressive",
    }

    if fragment not in allowed_fragments:
        fragment = "off"

    raw_clean = body.get("clean_ips") or body.get("clean_ip") or ""
    if isinstance(raw_clean, list):
        clean_ips = [str(x).strip() for x in raw_clean if str(x).strip()]
    else:
        clean_ips = [x.strip() for x in str(raw_clean).replace(",", "\n").splitlines() if x.strip()]
    alarm_enabled = bool(body.get("alarm_enabled", False))
    category_id = str(body.get("category_id") or "0")
    if category_id not in CATEGORIES:
        category_id = "0"
    config_count = safe_int(body.get("config_count", 1), minimum=1, maximum=40)
    all_protocols = bool(body.get("all_protocols", False))
    if all_protocols:
        config_count = 1
    sub_id = str(body.get("sub_id") or "").strip() or None
    if sub_id:
        async with SUBS_LOCK:
            target_sub = deepcopy(SUBS.get(sub_id)) if sub_id in SUBS else None
        if not target_sub:
            raise HTTPException(status_code=404, detail="گروه اشتراک پیدا نشد")
        allowed = set(target_sub.get("protocols") or PROTOCOLS)
        if not all_protocols and protocol not in allowed:
            raise HTTPException(status_code=400, detail="پروتکل انتخابی در این گروه فعال نیست؛ ابتدا آن را از مدیریت گروه فعال کنید")
    advanced = normalize_advanced_config(body.get("advanced"))
    # Advanced UI is authoritative for the duplicate legacy fields when provided.
    if isinstance(body.get("advanced"), dict):
        fp_adv = advanced["fingerprint"]["value"]
        if fp_adv in FINGERPRINTS: fingerprint = fp_adv
        alpn_adv = advanced["tls"].get("alpn") or advanced["host"].get("authority")
        if alpn_adv: body["alpn"] = alpn_adv
        primary_ports = advanced.get("ports") or []
        if primary_ports and not all_protocols: port = primary_ports[0]
    cat = CATEGORIES.get(category_id) or {}
    if cat.get("limit_bytes") and limit_bytes <= 0:
        limit_bytes = int(cat["limit_bytes"])
    if cat.get("expires_days") and expires_days <= 0:
        expires_days = int(cat["expires_days"])
        expires_at = (datetime.now() + timedelta(days=expires_days)).isoformat() if expires_days > 0 else None
    if cat.get("connection_limit") and connection_limit <= 0:
        connection_limit = int(cat["connection_limit"])
    if cat.get("speed_limit_bytes") and speed_bytes <= 0:
        speed_bytes = int(cat["speed_limit_bytes"])
    if cat.get("ip_limit") and ip_limit <= 0:
        ip_limit = int(cat["ip_limit"])
    if cat.get("clean_ips") and not clean_ips:
        clean_ips = list(cat["clean_ips"])
    if cat.get("single_user"):
        if ip_limit == 0: ip_limit = 1
        if connection_limit == 0: connection_limit = 1
    label_val = body.get("label", "")
    if cat.get("random_name") or not str(label_val).strip():
        label_val = project_config_name()
    else:
        label_val = sanitize_config_name(str(label_val))

    uid, link = await make_link(
        label=label_val,
        limit_bytes=limit_bytes,
        expires_at=expires_at,
        note=body.get(
            "note",
            "",
        ),
        sub_id=sub_id,
        protocol=protocol,
        fingerprint=fingerprint,
        alpn=body.get(
            "alpn",
            DEFAULT_ALPN_BY_PROTOCOL.get(
                protocol,
                "http/1.1",
            ),
        ),
        port=port,
        ip_limit=ip_limit,
        speed_limit_bytes=speed_bytes,
        connection_limit=connection_limit,
        fragment=fragment,
        clean_ips=clean_ips,
        alarm_enabled=alarm_enabled,
        category_id=category_id,
        config_count=config_count,
        all_protocols=all_protocols,
        advanced=advanced,
    )

    host = get_host(request)

    result = {
        **get_link_info(
            link,
            uid,
            host,
        ),
        "ok": True,
    }
    native_relevant = bool(NATIVE_CORE and (all_protocols or protocol in getattr(NATIVE_CORE, "SUPPORTED", ())))
    if native_relevant:
        if not await sync_native_core():
            async with LINKS_LOCK:
                LINKS.pop(uid, None)
            await save_state()
            raise HTTPException(409, NATIVE_CORE.last_error or "Native listener deployment failed")
        # Re-read the record so generated Reality public key / runtime ports are current.
        async with LINKS_LOCK:
            link = deepcopy(LINKS.get(uid) or link)
        result = {**get_link_info(link, uid, host), "ok": True}
    elif NATIVE_CORE:
        asyncio.create_task(sync_native_core())
    if all_protocols:
        result["all_protocols"] = True
        result["protocol_count"] = len(PROTOCOLS)

    return result


# ============================================================
# AUTO CREATE
# ============================================================

@app.post("/api/links/auto")
async def create_auto_link(
    request: Request,
    _=Depends(require_auth),
):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict): body = {}
    host = get_host(request)
    protocol = normalize_protocol(body.get("protocol", DEFAULT_PROTOCOL))
    profile = str(body.get("profile", "balanced")).strip().lower()
    profiles = {
        "normal": {"ip":0,"conn":0,"speed":0,"fp":"chrome","fragment":"off"},
        "balanced": {"ip":2,"conn":4,"speed":0,"fp":"chrome","fragment":"safe"},
        "gaming": {"ip":1,"conn":2,"speed":0,"fp":"chrome","fragment":"safe"},
        "maximum": {"ip":0,"conn":0,"speed":0,"fp":"randomized","fragment":"safe"},
    }
    cfg = profiles.get(profile, profiles["balanced"])
    config_count = safe_int(body.get("config_count", 1), minimum=1, maximum=40)
    all_protocols = bool(body.get("all_protocols", False))
    if all_protocols:
        config_count = 1
    uid, link = await make_link(
        label=project_config_name(), limit_bytes=0, expires_at=None,
        ip_limit=cfg["ip"], speed_limit_bytes=cfg["speed"], connection_limit=cfg["conn"],
        note=f"Auto generated by ONEX | profile={profile}",
        protocol=protocol, fingerprint=cfg["fp"],
        alpn=DEFAULT_ALPN_BY_PROTOCOL.get(protocol, ""), port=443, fragment=cfg["fragment"],
        config_count=config_count,
        all_protocols=all_protocols,
    )
    link["security_profile"] = profile
    result = {**get_link_info(link, uid, host), "ok": True, "profile": profile}
    if NATIVE_CORE:
        asyncio.create_task(sync_native_core())
    if all_protocols:
        result["all_protocols"] = True
        result["protocol_count"] = len(PROTOCOLS)
    log_activity("link", f"کانفیگ خودکار «{link['label']}» با {PROTOCOL_LABELS.get(protocol, protocol)} ساخته شد", "ok")
    return result


# ============================================================
# LIST LINKS
# ============================================================

@app.get("/api/protocols")
async def api_protocols(request: Request):
    require_auth(request)
    native_ready = bool(NATIVE_CORE and getattr(NATIVE_CORE, "is_runtime_ready", lambda: False)())
    return {
        "protocols": [{"id": p, "label": PROTOCOL_LABELS.get(p, p), "backend": "native" if p in getattr(NATIVE_CORE, "SUPPORTED", ()) else "panel"} for p in PROTOCOLS],
        "default": PROTOCOLS[0] if PROTOCOLS else DEFAULT_PROTOCOL,
        "native_core": {"installed": bool(NATIVE_CORE and NATIVE_CORE.binary_exists()), "running": native_ready, "error": getattr(NATIVE_CORE, "last_error", "") if NATIVE_CORE else ""},
    }


# ============================================================
# ADVANCED CONFIG VALIDATION / PREVIEW
# ============================================================

def _advanced_capabilities(protocol: str) -> dict:
    native = bool(NATIVE_CORE and protocol in getattr(NATIVE_CORE, "SUPPORTED", ()))
    common = {
        "tls": True,
        "reality": False,
        "sni": True,
        "alpn": True,
        "fingerprint": True,
        "ports": True,
        "listener": native,
        "routing": native,
        "sniffing": native,
        "custom_headers": protocol == "trojan",
        "transport": native,
        "client_only": True,
    }
    return {"native": native, "supported": common}


@app.get("/api/advanced/capabilities")
async def advanced_capabilities(protocol: str = DEFAULT_PROTOCOL, token=Depends(require_auth)):
    return {"ok": True, "protocol": normalize_protocol(protocol), **_advanced_capabilities(normalize_protocol(protocol))}


def _advanced_validation_errors(advanced: dict, protocol: str) -> list[str]:
    errors = []
    a = normalize_advanced_config(advanced)
    tls, reality, net = a["tls"], a["tls"]["reality"], a["network"]
    ports = a.get("ports") or []
    if not ports: errors.append("حداقل یک پورت لازم است")
    if len(set(ports)) != len(ports): errors.append("پورت‌ها نباید تکراری باشند")
    if tls["mode"] == "reality":
        sid = str(reality.get("short_id") or "")
        if sid and (len(sid) > 8 or any(c.lower() not in '0123456789abcdef' for c in sid)): errors.append("Reality Short ID باید حداکثر ۸ کاراکتر هگزادسیمال باشد")
    try:
        if float(tls["min_version"]) > float(tls["max_version"]): errors.append("حداقل TLS نمی‌تواند از حداکثر TLS بیشتر باشد")
    except Exception: errors.append("نسخه TLS نامعتبر است")
    if net["type"] == "grpc" and not (a["host"].get("service_name") or net.get("service_name")): errors.append("برای gRPC مقدار Service Name را وارد کنید")
    if net["type"] in {"ws", "http", "h2", "xhttp"} and a["host"].get("path") and not str(a["host"]["path"]).startswith('/'): errors.append("Path باید با / شروع شود")
    if protocol == "trojan" and net["type"] not in {"tcp","ws","grpc","http","h2","httpupgrade","quic"}: errors.append(f"Network {net['type']} برای Trojan پشتیبانی نمی‌شود")
    r = reality
    if tls["mode"] == "reality" and bool(str(r.get("public_key") or "")) != bool(str(r.get("private_key") or "")):
        errors.append("Reality Public Key و Private Key باید هر دو وارد شوند یا هر دو خالی باشند")
    if a["routing"].get("route") and str(a["routing"].get("route")) not in {"direct", "block"}:
        errors.append("Final outbound فعلاً فقط direct یا block است")
    if a["listener"].get("listen") and len(str(a["listener"].get("listen"))) > 255:
        errors.append("Listen address نامعتبر است")
    return errors

@app.post("/api/advanced/validate")
async def validate_advanced_config(request: Request, token=Depends(require_auth)):
    body = await request.json()
    protocol = normalize_protocol(str(body.get("protocol") or DEFAULT_PROTOCOL))
    advanced = normalize_advanced_config(body.get("advanced"))
    errors = _advanced_validation_errors(advanced, protocol)
    warnings = []
    if advanced["network"]["type"] in {"kcp", "quic", "xhttp"} and protocol != "trojan": warnings.append("این Transport در این پروتکل به Listener بومی قابل تبدیل نیست")
    if advanced["fingerprint"]["enabled"] and advanced["tls"]["mode"] == "none": warnings.append("Fingerprint یک تنظیم کلاینتی است و بدون TLS/uTLS اثری ندارد")
    if advanced["routing"].get("proxy_protocol"): warnings.append("Proxy Protocol در این نسخه به Listener تزریق نمی‌شود")
    if advanced["host"].get("authority"): warnings.append("Authority در Listener native sing-box اعمال نمی‌شود و فقط metadata کلاینت است")
    if protocol not in getattr(NATIVE_CORE, "SUPPORTED", ()):
        warnings.append("این پروتکل توسط relay/XHTTP پنل اجرا می‌شود؛ تنظیمات Listener بومی sing-box برای آن اعمال نمی‌شود")
    preview = None
    native = bool(NATIVE_CORE and protocol in getattr(NATIVE_CORE, "SUPPORTED", ()))
    if not errors and native:
        try:
            sample = {"preview": True, "active": True, "protocol": protocol, "advanced": advanced, "port": (advanced.get("ports") or [DEFAULT_PORT])[0], "fingerprint": advanced["fingerprint"]["value"], "all_protocols": False}
            preview = await NATIVE_CORE.build_config({"preview": sample}, get_host(request))
            ok, detail = await NATIVE_CORE.validate_config(preview)
            if not ok: errors.append(detail or "sing-box config validation failed")
        except Exception as exc: warnings.append(f"پیش‌نمایش Native انجام نشد: {exc}")
    return {"ok": not errors, "protocol": protocol, "native": native, "errors": errors, "warnings": warnings, "advanced": advanced, "preview": preview}


@app.get("/api/links")
async def list_links(
    request: Request,
    _=Depends(require_auth),
):

    host = get_host(request)

    async with LINKS_LOCK:
        snapshot = dict(LINKS)

    result = []

    for uid, link in snapshot.items():

        info = get_link_info(
            link,
            uid,
            host,
        )

        result.append(
            {
                **info,

                "created_at":
                    link.get(
                        "created_at"
                    ),

                "expired":
                    is_link_expired(
                        link
                    ),

                "sub_url":
                    f"https://{host}/sub/{uid}",

                "info_url":
                    f"https://{host}/info/{uid}",

                "connected_ips":
                    len(
                        unique_ips_for_uuid(
                            uid
                        )
                    ),
            }
        )

    result = sorted(
        result,
        key=lambda item: (
            -int(item.get("sort_order") or 0),
            str(item.get("created_at") or ""),
        ),
    )

    return {
        "links": result
    }


# ============================================================
# LINK INFO API
# ============================================================

@app.get("/api/links/{uid}/info")
async def link_info_api(
    uid: str,
    request: Request,
    _=Depends(require_auth),
):

    async with LINKS_LOCK:

        link = LINKS.get(uid)

        if not link:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        snapshot = dict(link)

    host = get_host(request)

    return {
        "ok": True,
        **get_link_info(
            snapshot,
            uid,
            host,
        ),
    }


# ============================================================
# UPDATE LINK
# ============================================================



@app.post("/api/links/reorder")
async def reorder_links(request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="JSON نامعتبر")
    order = body.get("order") or body.get("ids") or []
    if not isinstance(order, list):
        raise HTTPException(400, detail="order باید آرایه باشد")
    # first item = highest priority
    n = len(order)
    async with LINKS_LOCK:
        for i, uid in enumerate(order):
            uid = str(uid)
            if uid in LINKS:
                LINKS[uid]["sort_order"] = n - i
    await save_state()
    return {"ok": True, "count": n}


@app.post("/api/links/bulk-delete")
async def bulk_delete_links(request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="JSON نامعتبر")
    ids = body.get("ids") or []
    if not isinstance(ids, list) or not ids:
        raise HTTPException(400, detail="ids خالی است")
    deleted = []
    for uid in ids:
        uid = str(uid)
        if uid in LINKS:
            await remove_link(uid)
            deleted.append(uid)
    log_activity("link", f"حذف گروهی {len(deleted)} کانفیگ", "warn")
    return {"ok": True, "deleted": len(deleted)}


@app.post("/api/links/delete-all")
async def delete_all_links(_=Depends(require_auth)):
    """Delete every saved config atomically, with native-runtime rollback on failure."""
    async with LINKS_LOCK:
        previous_links = deepcopy(LINKS)
        if not previous_links:
            return {"ok": True, "deleted": 0}
        LINKS.clear()

    async with SUBS_LOCK:
        previous_subs = deepcopy(SUBS)
        # Subscription groups in this panel are derived from saved links.
        # Once every link is deleted, their generated link lists are empty.
        for sub in SUBS.values():
            if isinstance(sub, dict):
                sub["link_ids"] = []

    await save_state()

    if NATIVE_CORE and not await sync_native_core():
        async with LINKS_LOCK:
            LINKS.clear()
            LINKS.update(previous_links)
        async with SUBS_LOCK:
            SUBS.clear()
            SUBS.update(previous_subs)
        await save_state()
        raise HTTPException(409, NATIVE_CORE.last_error or "Native runtime reload failed; previous configuration restored")

    deleted = len(previous_links)
    log_activity("link", f"حذف همه کانفیگ‌ها · {deleted} مورد", "warn")
    return {"ok": True, "deleted": deleted}


@app.post("/api/links/bulk-category")
async def bulk_category(request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="JSON نامعتبر")
    ids = body.get("ids") or []
    cid = str(body.get("category_id") or "0")
    if cid not in CATEGORIES:
        cid = "0"
    n = 0
    async with LINKS_LOCK:
        for uid in ids:
            uid = str(uid)
            if uid in LINKS:
                LINKS[uid]["category_id"] = cid
                n += 1
    await save_state()
    return {"ok": True, "updated": n}


@app.patch("/api/links/{uid}")
async def update_link(
    uid: str,
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="اطلاعات نامعتبر است",
        )

    if not isinstance(body, dict):
        raise HTTPException(
            status_code=400,
            detail="اطلاعات نامعتبر است",
        )

    async with LINKS_LOCK:

        if uid not in LINKS:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        link = LINKS[uid]
        previous_link = deepcopy(link)

        old_sub = link.get(
            "sub_id"
        )

        label = link.get(
            "label",
            uid,
        )

        if "active" in body:
            link["active"] = bool(
                body["active"]
            )

        if "category_id" in body:
            cid = str(body.get("category_id") or "0")
            if cid not in CATEGORIES:
                cid = "0"
            link["category_id"] = cid

        if "sort_order" in body:
            try:
                link["sort_order"] = int(body.get("sort_order") or 0)
            except Exception:
                pass

        if "label" in body:

            value = str(
                body["label"]
            ).strip()

            if value:
                link["label"] = value[:60]

        if "note" in body:

            link["note"] = str(
                body.get(
                    "note",
                    "",
                )
            )[:500]

        if "reset_usage" in body:

            if body.get(
                "reset_usage"
            ):
                link[
                    "used_bytes"
                ] = 0


        if "limit_value" in body:

            value = safe_float(
                body.get(
                    "limit_value",
                    0,
                )
            )

            unit = str(
                body.get(
                    "limit_unit",
                    "GB",
                )
                or "GB"
            )

            link[
                "limit_bytes"
            ] = (
                0
                if value <= 0
                else parse_size_to_bytes(
                    value,
                    unit,
                )
            )

        if "expires_at" in body:
            raw_exp = body.get("expires_at")
            link["expires_at"] = str(raw_exp).strip()[:80] if raw_exp else None
        elif "expires_days" in body:

            days = safe_int(
                body.get(
                    "expires_days",
                    0,
                ),
                minimum=0,
            )

            link[
                "expires_at"
            ] = (
                (
                    datetime.now()
                    + timedelta(
                        days=days
                    )
                ).isoformat()
                if days > 0
                else None
            )

        if "fingerprint" in body:

            fingerprint = str(
                body.get(
                    "fingerprint",
                    DEFAULT_FINGERPRINT,
                )
            ).strip().lower()

            link[
                "fingerprint"
            ] = (
                fingerprint
                if fingerprint in FINGERPRINTS
                else DEFAULT_FINGERPRINT
            )

        if "alpn" in body:

            link["alpn"] = str(
                body.get(
                    "alpn",
                    "",
                )
            )[:100]

        if "port" in body:

            p = safe_int(
                body.get(
                    "port",
                    DEFAULT_PORT,
                ),
                default=DEFAULT_PORT,
                minimum=MIN_PORT,
                maximum=MAX_PORT,
            )

            link["port"] = p

        if "ip_limit" in body:

            link["ip_limit"] = safe_int(
                body.get(
                    "ip_limit",
                    0,
                ),
                minimum=0,
            )

        if "connection_limit" in body:

            link[
                "connection_limit"
            ] = safe_int(
                body.get(
                    "connection_limit",
                    0,
                ),
                minimum=0,
            )

        if "speed_limit_value" in body:

            speed_value = safe_float(
                body.get(
                    "speed_limit_value",
                    0,
                )
            )

            speed_unit = str(
                body.get(
                    "speed_limit_unit",
                    "MBIT",
                )
                or "MBIT"
            )

            link[
                "speed_limit_bytes"
            ] = (
                0
                if speed_value <= 0
                else parse_speed_to_bytes(
                    speed_value,
                    speed_unit,
                )
            )

        if "protocol" in body:

            protocol = str(
                body.get(
                    "protocol",
                    DEFAULT_PROTOCOL,
                )
            ).strip()

            link["protocol"] = (
                protocol
                if protocol in PROTOCOLS
                else DEFAULT_PROTOCOL
            )
            link["protocol_label"] = PROTOCOL_LABELS.get(link["protocol"], link["protocol"])

        if "config_count" in body:
            link["config_count"] = safe_int(
                body.get("config_count", 1),
                default=1,
                minimum=1,
                maximum=40,
            )

        if "all_protocols" in body:
            link["all_protocols"] = bool(body.get("all_protocols"))
            if link["all_protocols"]:
                link["config_count"] = 1

        if "fragment" in body:

            fragment = str(
                body.get(
                    "fragment",
                    "off",
                )
                or "off"
            ).strip().lower()

            if fragment not in {
                "off",
                "safe",
                "balanced",
                "aggressive",
            }:
                fragment = "off"

            link["fragment"] = fragment

        if "sub_id" in body:

            link[
                "sub_id"
            ] = (
                body.get(
                    "sub_id"
                )
                or None
            )

        if "advanced" in body:
            link["advanced"] = normalize_advanced_config(body.get("advanced"))
            adv = link["advanced"]
            if adv["fingerprint"]["value"] in FINGERPRINTS:
                link["fingerprint"] = adv["fingerprint"]["value"]
            if adv.get("ports") and not link.get("all_protocols"):
                link["port"] = adv["ports"][0]
            if adv["tls"].get("alpn"):
                link["alpn"] = adv["tls"]["alpn"]

        new_sub = body.get(
            "sub_id",
            "UNCHANGED",
        )

    if new_sub != "UNCHANGED":

        async with SUBS_LOCK:

            if (
                old_sub
                and old_sub in SUBS
            ):

                ids = SUBS[
                    old_sub
                ].get(
                    "link_ids",
                    [],
                )

                if uid in ids:
                    ids.remove(uid)

            if (
                new_sub
                and new_sub in SUBS
            ):

                ids = SUBS[
                    new_sub
                ].setdefault(
                    "link_ids",
                    [],
                )

                if uid not in ids:
                    ids.append(uid)

    await save_state()

    native_relevant = bool(NATIVE_CORE and (link.get("all_protocols") or link.get("protocol") in getattr(NATIVE_CORE, "SUPPORTED", ())))
    if native_relevant and not await sync_native_core():
        async with LINKS_LOCK:
            LINKS[uid] = previous_link
        await save_state()
        raise HTTPException(409, NATIVE_CORE.last_error or "Native listener deployment failed; previous configuration restored")
    elif NATIVE_CORE:
        asyncio.create_task(sync_native_core())

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{label}» "
            f"ویرایش شد"
        ),
        "info",
    )

    return {
        "ok": True
    }


# ============================================================
# RESET USAGE
# ============================================================

@app.post(
    "/api/links/{uid}/reset-usage"
)
async def reset_link_usage(
    uid: str,
    _=Depends(require_auth),
):

    async with LINKS_LOCK:

        link = LINKS.get(uid)

        if not link:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        link["used_bytes"] = 0

        label = link.get(
            "label",
            uid,
        )

    await save_state()

    log_activity(
        "link",
        (
            f"مصرف کانفیگ "
            f"«{label}» ریست شد"
        ),
        "info",
    )

    return {
        "ok": True,
        "uuid": uid,
        "used_bytes": 0,
    }


# ============================================================
# LINK ACTION
# ============================================================

@app.post(
    "/api/links/{uid}/action"
)
async def link_action(
    uid: str,
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    action = str(
        body.get(
            "action",
            "",
        )
    ).strip().lower()

    if action == "reset":

        await reset_link_usage(
            uid,
            _
        )

        return {
            "ok": True,
            "action": "reset",
        }

    if action == "enable":

        result = await set_link_active(
            uid,
            True,
        )

        if result is None:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        return {
            "ok": True,
            "action": "enable",
        }

    if action == "disable":

        result = await set_link_active(
            uid,
            False,
        )

        if result is None:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        return {
            "ok": True,
            "action": "disable",
        }

    raise HTTPException(
        status_code=400,
        detail="unknown action",
    )


# ============================================================
# DELETE LINK
# ============================================================

@app.delete("/api/links/{uid}")
async def delete_link(
    uid: str,
    _=Depends(require_auth),
):

    async with LINKS_LOCK:
        previous = deepcopy(LINKS.get(uid)) if uid in LINKS else None
    async with SUBS_LOCK:
        previous_subs = deepcopy(SUBS)
    if previous is None:
        raise HTTPException(status_code=404, detail="link not found")
    label = await remove_link(uid)
    if NATIVE_CORE and not await sync_native_core():
        async with LINKS_LOCK:
            LINKS[uid] = previous
        async with SUBS_LOCK:
            SUBS.clear(); SUBS.update(previous_subs)
        await save_state()
        raise HTTPException(409, NATIVE_CORE.last_error or "Native runtime reload failed; previous state restored")

    return {
        "ok": True,
        "deleted": uid,
    }




def subscription_metadata_headers(used_bytes: int, limit_bytes: int, expires_at, host: str, info_url: str, title: str):
    """Standard subscription headers understood by v2rayNG/v2rayN/Hiddify and similar clients."""
    used_bytes = max(0, int(used_bytes or 0))
    limit_bytes = max(0, int(limit_bytes or 0))

    expire_unix = 0
    if expires_at:
        try:
            dt = datetime.fromisoformat(str(expires_at))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IRAN_TZ) if IRAN_TZ else dt
            expire_unix = max(0, int(dt.timestamp()))
        except Exception:
            expire_unix = 0

    userinfo = f"upload=0; download={used_bytes}; total={limit_bytes}; expire={expire_unix}"

    return {
        "profile-title": quote(title, safe=""),
        "profile-web-page-url": info_url,
        "support-url": SUPPORT_URL,
        "profile-update-interval": "12",
        "subscription-userinfo": userinfo,
        "content-disposition": 'inline; filename="subscription.txt"',
    }

# ============================================================
# SINGLE SUB
# ============================================================

@app.get("/sub/{uuid}")
async def subscription_single(
    uuid: str,
    request: Request,
):

    async with LINKS_LOCK:
        link = LINKS.get(uuid)

    if not is_link_allowed(link):
        raise HTTPException(
            status_code=404,
            detail="not found or inactive",
        )

    host = get_host(request)
    clean_ips = link.get("clean_ips") or []
    used = int(link.get("used_bytes", 0) or 0)
    limit = int(link.get("limit_bytes", 0) or 0)
    remaining = max(0, limit - used) if limit > 0 else 0
    volume_text = f"{fmt_bytes(used)}/{fmt_bytes(limit)} (باقی {fmt_bytes(remaining)})" if limit > 0 else f"{fmt_bytes(used)}/∞"
    expires_at = link.get("expires_at")
    if expires_at:
        try:
            exp_dt = datetime.fromisoformat(str(expires_at))
            now_dt = datetime.now(exp_dt.tzinfo) if getattr(exp_dt, "tzinfo", None) else datetime.now()
            secs = int((exp_dt - now_dt).total_seconds())
            if secs <= 0:
                time_text = "منقضی"
            else:
                days, rem = divmod(secs, 86400)
                hours, rem = divmod(rem, 3600)
                mins = rem // 60
                time_text = f"{days}د {hours}س" if days else (f"{hours}س {mins}د" if hours else f"{mins}د")
        except Exception:
            time_text = str(expires_at)[:16]
    else:
        time_text = "∞"
    label = str(link.get("label") or "Config")
    stats_remark = f"{label} | {volume_text} | {time_text}"
    stats_line = generate_vless_link(uuid, "0.0.0.0", remark=stats_remark, protocol=link.get("protocol", DEFAULT_PROTOCOL), fingerprint=link.get("fingerprint", DEFAULT_FINGERPRINT), alpn=link.get("alpn"), port=protocol_public_port(link, link.get("protocol", DEFAULT_PROTOCOL), link.get("port", DEFAULT_PORT)), link=link)
    lines = [stats_line]
    used_names = set()
    cfg_count = 1 if link.get("all_protocols") else max(1, min(40, int(link.get("config_count") or 1)))
    protocols = list(PROTOCOLS) if link.get("all_protocols") else [link.get("protocol", DEFAULT_PROTOCOL)]
    if clean_ips:
        hosts = list(clean_ips)
        while len(hosts) < cfg_count:
            hosts.extend(clean_ips)
        hosts = hosts[:cfg_count]
        for cip in hosts:
            for proto in protocols:
                name = project_config_name(used_names)
                used_names.add(name)
                lines.append(generate_vless_link(uuid, cip, remark=name, protocol=proto, fingerprint=link.get("fingerprint", DEFAULT_FINGERPRINT), alpn=DEFAULT_ALPN_BY_PROTOCOL.get(proto, link.get("alpn")), port=protocol_public_port(link, proto, link.get("port", DEFAULT_PORT)), link=link))
    else:
        for i in range(cfg_count):
            for proto in protocols:
                name = project_config_name(used_names)
                used_names.add(name)
                lines.append(generate_vless_link(uuid, host, remark=name, protocol=proto, fingerprint=link.get("fingerprint", DEFAULT_FINGERPRINT), alpn=DEFAULT_ALPN_BY_PROTOCOL.get(proto, link.get("alpn")), port=protocol_public_port(link, proto, link.get("port", DEFAULT_PORT)), link=link))
    content = base64.b64encode("\n".join(lines).encode()).decode()
    profile_title = f"0.0.0.0 | {stats_remark}"
    headers = subscription_metadata_headers(
        used,
        limit,
        link.get("expires_at"),
        host,
        f"https://{host}/info/{uuid}",
        profile_title,
    )

    return Response(
        content=content,
        media_type="text/plain; charset=utf-8",
        headers=headers,
    )

# ============================================================
# SUB ALL
# ============================================================

@app.get("/sub-all")
async def subscription_all(
    request: Request,
    _=Depends(require_auth),
):

    host = get_host(request)

    async with LINKS_LOCK:

        lines = [
            vless_link_for_link(
                link,
                uid,
                host,
            )

            for uid, link
            in LINKS.items()

            if is_link_allowed(link)
        ]

    content = (
        base64
        .b64encode(
            "\n".join(
                lines
            ).encode()
        )
        .decode()
    )

    return Response(
        content=content,
        media_type="text/plain",
    )


# ============================================================
# INFO PAGE
# ============================================================

@app.get(
    "/info/{uid}",
    response_class=HTMLResponse,
)
async def info_page(
    uid: str,
    request: Request,
):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if not link:
            return HTMLResponse("<html lang=\"fa\" dir=\"rtl\"><body style=\"margin:0;background:#07070a;color:#fff;font-family:sans-serif;padding:40px\"><h2>کانفیگ پیدا نشد</h2></body></html>", status_code=404)
        snapshot = dict(link)

    host = get_host(request)
    vless_url = vless_link_for_link(snapshot, uid, host)
    sub_url = f"https://{host}/sub/{uid}"
    used = int(snapshot.get("used_bytes", 0) or 0)
    limit = int(snapshot.get("limit_bytes", 0) or 0)
    if limit > 0:
        usage_percent = max(0, min(100, round((used / limit) * 100, 1)))
        usage_value = f"{fmt_bytes(used)} / {fmt_bytes(limit)}"
        remaining_value = fmt_bytes(max(0, limit - used))
    else:
        usage_percent = 0
        usage_value = f"{fmt_bytes(used)} / نامحدود"
        remaining_value = "نامحدود"

    expires_at = snapshot.get("expires_at")
    if expires_at:
        try:
            expiry_dt = datetime.fromisoformat(str(expires_at))
            now_dt = datetime.now(expiry_dt.tzinfo) if expiry_dt.tzinfo else datetime.now()
            seconds = int((expiry_dt - now_dt).total_seconds())
            if seconds <= 0:
                expiry_remaining = "منقضی شده"
            else:
                days, rem = divmod(seconds, 86400)
                hours, rem = divmod(rem, 3600)
                minutes, _ = divmod(rem, 60)
                expiry_remaining = f"{days} روز و {hours} ساعت" if days else (f"{hours} ساعت و {minutes} دقیقه" if hours else f"{minutes} دقیقه")
        except Exception:
            expiry_remaining = "نامشخص"
        expiry_display = str(expires_at)
    else:
        expiry_remaining = "نامحدود"
        expiry_display = "نامحدود"

    status_text = "فعال" if is_link_allowed(snapshot) else "غیرفعال"
    status_class = "good" if status_text == "فعال" else "bad"
    ip_limit = "نامحدود" if not snapshot.get("ip_limit", 0) else str(snapshot.get("ip_limit"))
    connection_limit = "نامحدود" if not snapshot.get("connection_limit", 0) else str(snapshot.get("connection_limit"))
    speed_limit = "نامحدود" if not snapshot.get("speed_limit_bytes", 0) else fmt_bytes(snapshot.get("speed_limit_bytes", 0)) + "/s"

    usage_history = snapshot.get("usage_history", [])
    svg_points = "0,50 300,50"
    if usage_history and len(usage_history) > 1:
        max_hist = max(usage_history) if max(usage_history) > 0 else 1
        pts = []
        step = 300 / (len(usage_history) - 1)
        for i, val in enumerate(usage_history):
            x = i * step
            y = 60 - min(60, max(4, (val / max_hist) * 52))
            pts.append(f"{x:.1f},{y:.1f}")
        svg_points = " ".join(pts)
    elif usage_history and len(usage_history) == 1:
        svg_points = f"0,50 300,{60 - min(60, max(4, (usage_history[0] / (limit if limit > 0 else max(used, 1))) * 52)):.1f}"

    status_badge_html = 'text-emerald-300 border border-emerald-400/25 bg-emerald-400/10' if status_class == 'good' else 'text-rose-300 border border-rose-400/25 bg-rose-400/10'
    label_escaped = escape_html(snapshot.get("label", "PXpanel"))
    uid_escaped = escape_html(uid)
    app_version_str = escape_html(str(APP_VERSION))
    used_bytes_str = escape_html(fmt_bytes(used))
    limit_bytes_str = escape_html(fmt_bytes(limit)) if limit > 0 else '∞'
    remaining_value_escaped = escape_html(remaining_value)
    expiry_remaining_escaped = escape_html(expiry_remaining)
    expiry_display_escaped = escape_html(expiry_display)
    ip_limit_escaped = escape_html(ip_limit)
    connection_limit_escaped = escape_html(connection_limit)
    speed_limit_escaped = escape_html(speed_limit)
    protocol_escaped = escape_html(snapshot.get("protocol", "vless-ws"))
    fingerprint_escaped = escape_html(snapshot.get("fingerprint", "chrome"))
    vless_url_escaped = escape_html(vless_url)
    sub_url_escaped = escape_html(sub_url)
    dash_calc_offset = f"{339.29 - (339.29 * min(usage_percent, 100) / 100):.1f}"

    info_html = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>{label_escaped} | INFO</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
<script src="https://cdn.tailwindcss.com"></script>
<script src="https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.min.js"></script>
<script>
  tailwind.config = {{
    theme: {{
      extend: {{
        fontFamily: {{ vazir: ['Vazirmatn','system-ui','sans-serif'] }}
      }}
    }}
  }}
</script>
<style>
  :root {{
    --bg-main: #05060a;
    --bg-card: rgba(255, 255, 255, 0.04);
    --bg-card-hover: rgba(255, 255, 255, 0.07);
    --border-color: rgba(255, 255, 255, 0.1);
    --text-main: #f1f5f9;
    --text-muted: rgba(255, 255, 255, 0.4);
    --bg-sub-card: rgba(0, 0, 0, 0.2);
    --grad-1: rgba(96,165,250,.16);
    --grad-2: rgba(96,165,250,.13);
    --grad-3: rgba(52,211,153,.08);
  }}

  body.theme-lighter {{
    --bg-main: #131722;
    --bg-card: rgba(255, 255, 255, 0.075);
    --bg-card-hover: rgba(255, 255, 255, 0.115);
    --border-color: rgba(255, 255, 255, 0.16);
    --text-main: #ffffff;
    --text-muted: rgba(255, 255, 255, 0.6);
    --bg-sub-card: rgba(0, 0, 0, 0.35);
    --grad-1: rgba(96,165,250,.24);
    --grad-2: rgba(96,165,250,.20);
    --grad-3: rgba(52,211,153,.13);
  }}

  html,body{{background:var(--bg-main); transition: background 0.3s ease, color 0.3s ease;}}
  body{{
    background:
      radial-gradient(ellipse 80% 50% at 10% -10%, var(--grad-1), transparent 50%),
      radial-gradient(ellipse 60% 40% at 95% 15%, var(--grad-2), transparent 45%),
      radial-gradient(ellipse 55% 35% at 60% 100%, var(--grad-3), transparent 40%),
      var(--bg-main);
  }}
  .status-dot{{box-shadow:0 0 10px currentColor}}
  ::-webkit-scrollbar{{width:8px;height:8px}}
  ::-webkit-scrollbar-thumb{{background:rgba(255,255,255,.12);border-radius:99px}}
  * {{ box-shadow: none !important; }}
  .copy-btn svg{{transition:none}}
  
  .dynamic-card {{
    background-color: var(--bg-card);
    border-color: var(--border-color);
    transition: background-color 0.3s ease, border-color 0.3s ease;
  }}
  .dynamic-card:hover {{
    background-color: var(--bg-card-hover);
  }}
  .sub-box {{
    background-color: var(--bg-sub-card);
  }}

  /* ============================================================
     V2rayTun0 TELEGRAM HERO — PLAN 1 / 3D NEON
     ============================================================ */
  .tg-hero{{position:relative;overflow:hidden;isolation:isolate;min-height:168px;border:1px solid rgba(0,174,255,.35);border-radius:28px;background:radial-gradient(circle at 18% 50%,rgba(0,174,255,.18),transparent 28%),radial-gradient(circle at 82% 50%,rgba(139,92,246,.18),transparent 30%),linear-gradient(135deg,rgba(5,16,35,.98),rgba(7,8,20,.98));box-shadow:0 0 0 1px rgba(70,120,255,.08) inset,0 0 34px rgba(0,153,255,.10),0 0 70px rgba(124,58,237,.07);transform:translateZ(0);}}
  .tg-hero::before{{content:"";position:absolute;inset:-2px;border-radius:30px;padding:1px;background:linear-gradient(110deg,transparent 5%,rgba(0,198,255,.85) 28%,rgba(124,58,237,.9) 55%,rgba(236,72,153,.8) 78%,transparent 95%);-webkit-mask:linear-gradient(#000 0 0) content-box,linear-gradient(#000 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:tgBorder 5s linear infinite;pointer-events:none;}}
  .tg-hero::after{{content:"";position:absolute;inset:0;background:linear-gradient(115deg,transparent 0%,rgba(255,255,255,.055) 45%,transparent 58%);transform:translateX(-120%);animation:tgSweep 5.5s ease-in-out infinite;pointer-events:none;}}
  .tg-hero-inner{{position:relative;z-index:2;display:grid;grid-template-columns:150px 1fr auto;align-items:center;gap:24px;padding:22px 28px;min-height:168px;}}
  .tg-visual{{position:relative;width:124px;height:124px;display:grid;place-items:center;justify-self:center;perspective:800px;}}
  .tg-orbit{{position:absolute;inset:3px;border:1px solid rgba(0,191,255,.48);border-radius:50%;transform:rotateX(68deg) rotateZ(-15deg);animation:tgOrbit 7s linear infinite;box-shadow:0 0 16px rgba(0,174,255,.18);}}
  .tg-orbit::before,.tg-orbit::after{{content:"";position:absolute;inset:-8px;border:1px solid rgba(96,165,250,.22);border-radius:50%;}}
  .tg-orbit::after{{inset:9px;border-color:rgba(168,85,247,.28);transform:rotate(55deg);}}
  .tg-logo-wrap{{position:relative;width:84px;height:84px;border-radius:27px;display:grid;place-items:center;color:#fff;background:linear-gradient(145deg,#21a7ff 0%,#1677ee 48%,#6844f5 100%);border:1px solid rgba(255,255,255,.32);box-shadow:-9px 10px 0 rgba(4,45,110,.55),0 12px 28px rgba(0,136,255,.45),0 0 34px rgba(0,174,255,.38);transform:rotateX(8deg) rotateY(-10deg) translateZ(20px);animation:tgFloat 3.8s ease-in-out infinite;}}
  .tg-logo-wrap::before{{content:"";position:absolute;inset:6px;border-radius:21px;border:1px solid rgba(255,255,255,.22);background:linear-gradient(135deg,rgba(255,255,255,.18),transparent 45%);pointer-events:none;}}
  .tg-logo-wrap svg{{position:relative;width:49px;height:49px;filter:drop-shadow(0 3px 4px rgba(0,0,0,.35));}}
  .tg-copy{{min-width:0;direction:rtl;}}
  .tg-kicker{{font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase;color:#67d9ff;margin-bottom:6px;}}
  .tg-title{{font-size:clamp(18px,2.4vw,25px);font-weight:900;color:#f8fbff;line-height:1.5;}}
  .tg-title em{{font-style:normal;color:#52c7ff;text-shadow:0 0 18px rgba(0,174,255,.28);}}
  .tg-desc{{margin-top:7px;font-size:11px;color:rgba(226,232,240,.52);line-height:1.8;}}
  .tg-handle{{display:inline-flex;align-items:center;gap:7px;margin-top:11px;padding:7px 12px;border-radius:999px;color:#c4b5fd;background:rgba(124,58,237,.10);border:1px solid rgba(167,139,250,.25);font-size:12px;font-weight:900;direction:ltr;box-shadow:0 0 18px rgba(124,58,237,.10);}}
  .tg-handle-dot{{width:7px;height:7px;border-radius:50%;background:#38bdf8;box-shadow:0 0 10px #38bdf8;animation:tgPulse 1.8s ease-in-out infinite;}}
  .tg-join{{position:relative;display:inline-flex;align-items:center;justify-content:center;gap:10px;min-width:188px;padding:14px 19px;border-radius:17px;text-decoration:none;color:#fff;font-size:13px;font-weight:900;background:linear-gradient(110deg,#168cff,#3b63ff 52%,#a43cff);border:1px solid rgba(255,255,255,.28);box-shadow:0 8px 0 rgba(25,45,130,.52),0 12px 28px rgba(37,99,235,.34),0 0 30px rgba(139,92,246,.20);transform:translateY(-3px);transition:transform .22s ease,filter .22s ease,box-shadow .22s ease;overflow:hidden;white-space:nowrap;}}
  .tg-join::before{{content:"";position:absolute;inset:0;background:linear-gradient(100deg,transparent 20%,rgba(255,255,255,.28) 48%,transparent 72%);transform:translateX(-120%);animation:tgButtonSweep 3.2s ease-in-out infinite;}}
  .tg-join:hover{{transform:translateY(-6px) scale(1.015);filter:saturate(1.12);box-shadow:0 11px 0 rgba(25,45,130,.45),0 18px 38px rgba(37,99,235,.42),0 0 38px rgba(139,92,246,.28);}}
  .tg-join:active{{transform:translateY(1px);box-shadow:0 3px 0 rgba(25,45,130,.45),0 8px 18px rgba(37,99,235,.25);}}
  .tg-join svg{{width:19px;height:19px;position:relative;z-index:1;}}
  .tg-join span{{position:relative;z-index:1;}}
  .tg-bell{{position:absolute;right:28px;top:20px;color:#8be9ff;opacity:.65;animation:tgBell 2.6s ease-in-out infinite;filter:drop-shadow(0 0 8px rgba(0,191,255,.55));}}
  .tg-particle{{position:absolute;border-radius:50%;pointer-events:none;opacity:.75;}}
  .tg-p1{{width:5px;height:5px;left:42%;top:17%;background:#22d3ee;box-shadow:0 0 12px #22d3ee;animation:tgParticle1 5s ease-in-out infinite;}}
  .tg-p2{{width:3px;height:3px;left:62%;bottom:18%;background:#a78bfa;box-shadow:0 0 10px #a78bfa;animation:tgParticle2 4s ease-in-out infinite;}}
  .tg-p3{{width:4px;height:4px;right:19%;top:62%;background:#f472b6;box-shadow:0 0 12px #f472b6;animation:tgParticle3 6s ease-in-out infinite;}}
  @keyframes tgFloat{{0%,100%{{transform:rotateX(8deg) rotateY(-10deg) translate3d(0,0,20px)}}50%{{transform:rotateX(-5deg) rotateY(8deg) translate3d(0,-8px,28px)}}}}
  @keyframes tgOrbit{{to{{transform:rotateX(68deg) rotateZ(345deg)}}}}
  @keyframes tgBorder{{to{{filter:hue-rotate(360deg)}}}}
  @keyframes tgSweep{{0%,30%{{transform:translateX(-120%)}}65%,100%{{transform:translateX(120%)}}}}
  @keyframes tgButtonSweep{{0%,35%{{transform:translateX(-130%)}}70%,100%{{transform:translateX(130%)}}}}
  @keyframes tgPulse{{0%,100%{{opacity:.45;transform:scale(.8)}}50%{{opacity:1;transform:scale(1.2)}}}}
  @keyframes tgBell{{0%,75%,100%{{transform:rotate(0)}}80%{{transform:rotate(10deg)}}85%{{transform:rotate(-10deg)}}90%{{transform:rotate(6deg)}}95%{{transform:rotate(-4deg)}}}}
  @keyframes tgParticle1{{0%,100%{{transform:translate(0,0);opacity:.2}}50%{{transform:translate(30px,16px);opacity:1}}}}
  @keyframes tgParticle2{{0%,100%{{transform:translate(0,0);opacity:.25}}50%{{transform:translate(-22px,-12px);opacity:1}}}}
  @keyframes tgParticle3{{0%,100%{{transform:translate(0,0);opacity:.25}}50%{{transform:translate(12px,20px);opacity:1}}}}
  @media (max-width:700px){{.tg-hero{{min-height:unset;border-radius:22px;}}.tg-hero-inner{{grid-template-columns:76px minmax(0,1fr);gap:13px;padding:16px 14px;min-height:126px;}}.tg-visual{{width:70px;height:70px;}}.tg-logo-wrap{{width:54px;height:54px;border-radius:18px;box-shadow:-5px 6px 0 rgba(4,45,110,.5),0 8px 20px rgba(0,136,255,.4),0 0 24px rgba(0,174,255,.3);}}.tg-logo-wrap svg{{width:32px;height:32px;}}.tg-orbit{{inset:2px;}}.tg-kicker{{font-size:8px;margin-bottom:2px;}}.tg-title{{font-size:15px;line-height:1.5;}}.tg-desc{{font-size:9px;margin-top:3px;line-height:1.6;}}.tg-handle{{margin-top:6px;padding:5px 9px;font-size:10px;}}.tg-join{{grid-column:1 / -1;width:100%;min-width:0;padding:11px 14px;border-radius:14px;font-size:12px;transform:none;box-shadow:0 6px 0 rgba(25,45,130,.5),0 10px 22px rgba(37,99,235,.26);}}.tg-bell{{right:10px;top:10px;transform:scale(.72);}}}}
  @media (prefers-reduced-motion:reduce){{.tg-hero::before,.tg-hero::after,.tg-orbit,.tg-logo-wrap,.tg-join::before,.tg-handle-dot,.tg-bell,.tg-particle{{animation:none!important;}}}}

  /* ============================================================
     ONEX PREMIUM GLASS SYSTEM — SUBSCRIPTION PAGE
     Deep 3D glass, luminous edges, reflections and animated depth
     ============================================================ */
  .w-full.max-w-4xl.mx-auto.space-y-5{{position:relative;}}
  .w-full.max-w-4xl.mx-auto.space-y-5::before{{
    content:"";position:fixed;inset:-25%;pointer-events:none;z-index:-1;
    background:
      radial-gradient(circle at 15% 22%,rgba(0,174,255,.13),transparent 24%),
      radial-gradient(circle at 85% 38%,rgba(124,58,237,.12),transparent 25%),
      radial-gradient(circle at 52% 86%,rgba(16,185,129,.08),transparent 22%);
    filter:blur(28px);animation:pageAura 12s ease-in-out infinite alternate;
  }}

  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card{{
    position:relative;isolation:isolate;overflow:hidden;
    border-radius:24px !important;
    background:
      linear-gradient(145deg,rgba(28,52,88,.70),rgba(8,17,34,.78) 48%,rgba(19,12,43,.70)) !important;
    border:1px solid rgba(94,183,255,.30) !important;
    box-shadow:
      inset 0 1px 0 rgba(255,255,255,.14),
      inset 0 -1px 0 rgba(0,0,0,.35),
      inset 0 0 55px rgba(45,125,255,.08),
      0 18px 45px rgba(0,0,0,.28),
      0 0 30px rgba(37,99,235,.07) !important;
    backdrop-filter:blur(24px) saturate(150%);-webkit-backdrop-filter:blur(24px) saturate(150%);
    transform:translateZ(0);transition:transform .35s ease,border-color .35s ease,box-shadow .35s ease;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card::before{{
    content:"";position:absolute;inset:0;z-index:-1;pointer-events:none;border-radius:inherit;
    background:
      radial-gradient(ellipse at 8% 15%,rgba(0,198,255,.18),transparent 28%),
      radial-gradient(ellipse at 92% 85%,rgba(124,58,237,.17),transparent 30%),
      linear-gradient(120deg,transparent 0%,rgba(255,255,255,.045) 42%,transparent 58%);
    background-size:auto,auto,220% 100%;
    animation:glassFlow 9s ease-in-out infinite;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card::after{{
    content:"";position:absolute;top:-120%;left:-35%;width:34%;height:340%;z-index:3;pointer-events:none;
    background:linear-gradient(90deg,transparent,rgba(255,255,255,.12),transparent);
    transform:rotate(22deg);animation:glassSweep 7s ease-in-out infinite;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:hover{{
    transform:translateY(-3px);
    border-color:rgba(86,190,255,.48) !important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.18),inset 0 0 65px rgba(45,125,255,.11),0 24px 55px rgba(0,0,0,.34),0 0 38px rgba(37,99,235,.12) !important;
  }}

  /* Different glass tones, like the reference concept */
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:nth-of-type(2){{
    background:linear-gradient(145deg,rgba(8,73,91,.68),rgba(7,27,47,.80),rgba(5,17,31,.78)) !important;
    border-color:rgba(34,211,238,.30) !important;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:nth-of-type(3){{
    background:linear-gradient(145deg,rgba(30,50,96,.72),rgba(10,24,53,.80),rgba(30,15,62,.68)) !important;
    border-color:rgba(96,165,250,.32) !important;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:nth-of-type(4){{
    background:linear-gradient(145deg,rgba(12,66,76,.66),rgba(8,28,45,.80),rgba(7,18,35,.80)) !important;
    border-color:rgba(45,212,191,.27) !important;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:nth-of-type(5){{
    background:linear-gradient(145deg,rgba(39,26,82,.72),rgba(17,19,50,.80),rgba(8,25,48,.76)) !important;
    border-color:rgba(168,85,247,.34) !important;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:nth-of-type(6){{
    background:linear-gradient(145deg,rgba(12,59,75,.68),rgba(8,25,44,.80),rgba(12,18,39,.78)) !important;
    border-color:rgba(45,212,191,.28) !important;
  }}

  .w-full.max-w-4xl.mx-auto.space-y-5 .sub-box{{
    position:relative;overflow:hidden;
    background:linear-gradient(145deg,rgba(28,58,99,.55),rgba(7,18,37,.72)) !important;
    border:1px solid rgba(94,170,255,.22) !important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.075),inset 0 0 28px rgba(59,130,246,.06),0 8px 24px rgba(0,0,0,.12) !important;
    transition:transform .28s ease,border-color .28s ease,box-shadow .28s ease,background .28s ease;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 .sub-box::before{{
    content:"";position:absolute;inset:0;pointer-events:none;
    background:linear-gradient(115deg,transparent 25%,rgba(255,255,255,.055) 48%,transparent 68%);
    transform:translateX(-120%);animation:subBoxShine 6s ease-in-out infinite;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 .sub-box:hover{{
    transform:translateY(-2px) perspective(700px) rotateX(1deg);
    border-color:rgba(83,180,255,.42) !important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.11),inset 0 0 34px rgba(59,130,246,.09),0 12px 28px rgba(0,0,0,.20),0 0 20px rgba(37,99,235,.08) !important;
  }}

  /* Give text/value blocks more luminous depth */
  .w-full.max-w-4xl.mx-auto.space-y-5 .text-purple-300{{text-shadow:0 0 14px rgba(168,85,247,.22);}}
  .w-full.max-w-4xl.mx-auto.space-y-5 .text-blue-300{{text-shadow:0 0 14px rgba(96,165,250,.22);}}
  .w-full.max-w-4xl.mx-auto.space-y-5 .text-emerald-300{{text-shadow:0 0 14px rgba(52,211,153,.20);}}
  .w-full.max-w-4xl.mx-auto.space-y-5 .text-amber-300{{text-shadow:0 0 14px rgba(251,191,36,.18);}}

  @keyframes pageAura{{0%{{transform:translate3d(-1%,0,0) scale(1)}}100%{{transform:translate3d(1%,-1%,0) scale(1.05)}}}}
  @keyframes glassFlow{{0%,100%{{background-position:center,center,0% 0}}50%{{background-position:center,center,120% 0}}}}
  @keyframes glassSweep{{0%,55%{{left:-35%;opacity:0}}65%{{opacity:1}}100%{{left:120%;opacity:0}}}}
  @keyframes subBoxShine{{0%,58%{{transform:translateX(-120%);opacity:0}}68%{{opacity:1}}100%{{transform:translateX(120%);opacity:0}}}}

  @media (max-width:700px){{
    .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card{{
      border-radius:20px !important;
      box-shadow:inset 0 1px 0 rgba(255,255,255,.12),inset 0 0 42px rgba(45,125,255,.075),0 14px 34px rgba(0,0,0,.26) !important;
    }}
    .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:hover{{transform:none;}}
    .w-full.max-w-4xl.mx-auto.space-y-5 .sub-box:hover{{transform:none;}}
  }}
  @media (prefers-reduced-motion:reduce){{
    .w-full.max-w-4xl.mx-auto.space-y-5::before,.w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card::before,.w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card::after,.w-full.max-w-4xl.mx-auto.space-y-5 .sub-box::before{{animation:none!important;}}
  }}


  /* ============================================================
     GLASS PANELS — COLORFUL NEON GLASS (PLAN 1 FINAL)
     ============================================================ */
  .dynamic-card{{
    position:relative;
    overflow:hidden;
    background:
      radial-gradient(120% 150% at 100% 0%,rgba(72,115,255,.105),transparent 46%),
      radial-gradient(100% 130% at 0% 100%,rgba(139,92,246,.075),transparent 48%),
      linear-gradient(135deg,rgba(18,25,43,.78),rgba(8,11,21,.82));
    border-color:rgba(105,145,255,.18);
    box-shadow:inset 0 1px 0 rgba(255,255,255,.055),inset 0 0 35px rgba(72,115,255,.025),0 14px 40px rgba(0,0,0,.16) !important;
    backdrop-filter:blur(22px) saturate(125%);
  }}
  .dynamic-card::before{{
    content:"";position:absolute;inset:0;pointer-events:none;border-radius:inherit;
    background:linear-gradient(115deg,rgba(255,255,255,.045),transparent 22%,transparent 72%,rgba(96,165,250,.055));
    opacity:.9;
  }}
  .dynamic-card:nth-of-type(2n){{
    background:
      radial-gradient(110% 150% at 0% 0%,rgba(0,174,255,.10),transparent 45%),
      radial-gradient(100% 130% at 100% 100%,rgba(37,99,235,.075),transparent 48%),
      linear-gradient(135deg,rgba(14,25,43,.80),rgba(7,11,20,.84));
    border-color:rgba(56,189,248,.17);
  }}
  .dynamic-card:nth-of-type(3n){{
    background:
      radial-gradient(120% 140% at 100% 10%,rgba(168,85,247,.105),transparent 44%),
      radial-gradient(100% 130% at 0% 100%,rgba(52,211,153,.055),transparent 46%),
      linear-gradient(135deg,rgba(24,20,43,.80),rgba(9,10,21,.84));
    border-color:rgba(167,139,250,.17);
  }}
  .sub-box{{
    background:
      linear-gradient(135deg,rgba(20,27,45,.66),rgba(9,12,22,.72));
    border-color:rgba(110,145,220,.12) !important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.028),inset 0 0 22px rgba(96,165,250,.025) !important;
  }}
  .dynamic-card:hover{{
    background:
      radial-gradient(120% 150% at 100% 0%,rgba(72,115,255,.14),transparent 46%),
      radial-gradient(100% 130% at 0% 100%,rgba(139,92,246,.10),transparent 48%),
      linear-gradient(135deg,rgba(21,29,49,.84),rgba(8,11,21,.88));
    border-color:rgba(96,165,250,.25);
  }}
  @media (max-width:700px){{
    .dynamic-card{{
      background:
        radial-gradient(120% 140% at 100% 0%,rgba(72,115,255,.095),transparent 44%),
        radial-gradient(100% 120% at 0% 100%,rgba(139,92,246,.065),transparent 46%),
        linear-gradient(135deg,rgba(17,24,40,.80),rgba(7,10,19,.86));
      border-color:rgba(96,140,245,.17);
    }}
    .sub-box{{background:linear-gradient(135deg,rgba(19,26,43,.62),rgba(8,11,20,.70));}}
  }}

  /* FORCE VISIBLE GLASS COLOR - SUBSCRIPTION PAGE */
  .dynamic-card{{
    background:
      linear-gradient(135deg,rgba(24,58,105,.72) 0%,rgba(18,31,61,.68) 45%,rgba(18,12,42,.72) 100%) !important;
    border:1px solid rgba(83,150,255,.38) !important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.12),inset 0 0 45px rgba(45,120,255,.10),0 12px 35px rgba(0,0,0,.22) !important;
    backdrop-filter:blur(24px) saturate(145%);
    -webkit-backdrop-filter:blur(24px) saturate(145%);
  }}
  .dynamic-card:nth-of-type(2n){{
    background:
      linear-gradient(135deg,rgba(20,74,94,.70) 0%,rgba(12,38,65,.68) 48%,rgba(11,22,43,.74) 100%) !important;
    border-color:rgba(34,211,238,.34) !important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.11),inset 0 0 45px rgba(34,211,238,.09),0 12px 35px rgba(0,0,0,.22) !important;
  }}
  .dynamic-card:nth-of-type(3n){{
    background:
      linear-gradient(135deg,rgba(55,34,91,.72) 0%,rgba(30,24,62,.69) 50%,rgba(18,15,42,.74) 100%) !important;
    border-color:rgba(168,85,247,.36) !important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.11),inset 0 0 45px rgba(139,92,246,.10),0 12px 35px rgba(0,0,0,.22) !important;
  }}
  .sub-box{{
    background:linear-gradient(135deg,rgba(27,49,82,.72),rgba(12,20,39,.78)) !important;
    border-color:rgba(96,165,250,.24) !important;
    box-shadow:inset 0 1px 0 rgba(255,255,255,.07),inset 0 0 28px rgba(59,130,246,.07) !important;
  }}
  .dynamic-card .text-white/40{{color:rgba(226,232,240,.58) !important;}}
  .dynamic-card .text-white/45{{color:rgba(226,232,240,.68) !important;}}
  .dynamic-card .text-white/35{{color:rgba(226,232,240,.55) !important;}}
  @media (max-width:700px){{
    .dynamic-card{{
      background:linear-gradient(135deg,rgba(23,56,100,.76),rgba(13,27,54,.72),rgba(25,15,51,.76)) !important;
      border-color:rgba(83,150,255,.40) !important;
    }}
    .dynamic-card:nth-of-type(2n){{
      background:linear-gradient(135deg,rgba(18,69,88,.75),rgba(11,35,60,.73),rgba(8,20,38,.78)) !important;
      border-color:rgba(34,211,238,.36) !important;
    }}
    .dynamic-card:nth-of-type(3n){{
      background:linear-gradient(135deg,rgba(53,32,88,.76),rgba(28,22,58,.73),rgba(17,14,39,.78)) !important;
      border-color:rgba(168,85,247,.38) !important;
    }}
    .sub-box{{background:linear-gradient(135deg,rgba(26,48,79,.72),rgba(10,18,36,.80)) !important;}}
  }}

  /* ============================================================
     ONEX GLASS PERFORMANCE PATCH
     Keep the 3D/glass look, but avoid expensive blur/compositor work.
     The Telegram hero keeps its richer animation; dashboard cards stay
     visually rich but render immediately and cheaply on mobile.
     ============================================================ */
  .w-full.max-w-4xl.mx-auto.space-y-5::before{{
    background:
      radial-gradient(circle at 12% 18%,rgba(0,174,255,.12),transparent 24%),
      radial-gradient(circle at 88% 35%,rgba(124,58,237,.10),transparent 25%),
      radial-gradient(circle at 50% 88%,rgba(16,185,129,.07),transparent 22%);
    filter:none !important;
    animation:none !important;
    opacity:.9;
  }}

  /* Realistic glass without backdrop-filter: much faster to paint. */
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card,
  .dynamic-card{{
    -webkit-backdrop-filter:none !important;
    backdrop-filter:none !important;
    background:
      linear-gradient(145deg,rgba(40,78,125,.64) 0%,rgba(16,35,65,.72) 42%,rgba(12,17,37,.82) 100%) !important;
    border:1px solid rgba(107,190,255,.34) !important;
    box-shadow:
      inset 0 1px 0 rgba(255,255,255,.17),
      inset 0 -1px 0 rgba(0,0,0,.38),
      inset 12px 0 38px rgba(40,170,255,.055),
      inset -12px 0 38px rgba(124,58,237,.045),
      0 16px 42px rgba(0,0,0,.30),
      0 0 26px rgba(37,140,255,.07) !important;
    transform:translateZ(0);
    contain:paint;
  }}

  /* Static glossy reflection = no continuous repainting. */
  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card::before,
  .dynamic-card::before{{
    content:"";
    position:absolute;
    inset:0;
    pointer-events:none;
    border-radius:inherit;
    background:
      linear-gradient(118deg,rgba(255,255,255,.10) 0%,transparent 16%,transparent 68%,rgba(74,170,255,.055) 100%),
      radial-gradient(80% 80% at 0% 0%,rgba(76,190,255,.10),transparent 55%);
    opacity:1;
    animation:none !important;
  }}

  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card::after{{
    animation:none !important;
    opacity:0 !important;
  }}

  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:nth-of-type(2),
  .dynamic-card:nth-of-type(2n){{
    background:
      linear-gradient(145deg,rgba(18,102,123,.60) 0%,rgba(10,48,70,.70) 44%,rgba(7,21,39,.82) 100%) !important;
    border-color:rgba(46,211,238,.34) !important;
    box-shadow:
      inset 0 1px 0 rgba(255,255,255,.16),
      inset 0 0 48px rgba(34,211,238,.07),
      0 16px 42px rgba(0,0,0,.30),0 0 28px rgba(34,211,238,.07) !important;
  }}

  .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:nth-of-type(3),
  .dynamic-card:nth-of-type(3n){{
    background:
      linear-gradient(145deg,rgba(61,46,119,.64) 0%,rgba(31,30,72,.70) 46%,rgba(13,15,38,.82) 100%) !important;
    border-color:rgba(167,139,250,.36) !important;
    box-shadow:
      inset 0 1px 0 rgba(255,255,255,.16),
      inset 0 0 48px rgba(139,92,246,.075),
      0 16px 42px rgba(0,0,0,.30),0 0 28px rgba(139,92,246,.07) !important;
  }}

  .w-full.max-w-4xl.mx-auto.space-y-5 .sub-box{{
    -webkit-backdrop-filter:none !important;
    backdrop-filter:none !important;
    background:
      linear-gradient(145deg,rgba(35,76,122,.48),rgba(12,27,51,.70)) !important;
    border:1px solid rgba(103,181,255,.24) !important;
    box-shadow:
      inset 0 1px 0 rgba(255,255,255,.10),
      inset 0 -10px 24px rgba(0,0,0,.14),
      0 8px 24px rgba(0,0,0,.16) !important;
    contain:paint;
  }}
  .w-full.max-w-4xl.mx-auto.space-y-5 .sub-box::before{{
    animation:none !important;
    background:linear-gradient(115deg,rgba(255,255,255,.07),transparent 24%,transparent 72%,rgba(96,165,250,.05));
    transform:none !important;
    opacity:1 !important;
  }}

  /* Mobile: no blur, no continuous card animation, same premium glass depth. */
  @media (max-width:700px){{
    .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card,
    .dynamic-card{{
      -webkit-backdrop-filter:none !important;
      backdrop-filter:none !important;
      border-radius:20px !important;
      box-shadow:
        inset 0 1px 0 rgba(255,255,255,.15),
        inset 0 -1px 0 rgba(0,0,0,.32),
        inset 0 0 38px rgba(45,140,255,.065),
        0 12px 30px rgba(0,0,0,.28) !important;
    }}
    .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card:hover,
    .dynamic-card:hover,
    .w-full.max-w-4xl.mx-auto.space-y-5 .sub-box:hover{{
      transform:none !important;
    }}
  }}

  @media (prefers-reduced-motion:reduce){{
    .w-full.max-w-4xl.mx-auto.space-y-5 > section.dynamic-card::before,
    .dynamic-card::before,
    .w-full.max-w-4xl.mx-auto.space-y-5 .sub-box::before{{animation:none!important;}}
  }}


  .onex-sub-page{{position:relative;}}
  .sub-glass{{position:relative;overflow:hidden;border:1px solid rgba(83,150,255,.24);border-radius:24px;background:linear-gradient(145deg,rgba(25,45,78,.78),rgba(10,17,34,.88) 65%,rgba(24,10,34,.78));box-shadow:inset 0 1px 0 rgba(255,255,255,.10),0 16px 40px rgba(0,0,0,.28);}}
  .sub-glass:before{{content:"";position:absolute;inset:0;pointer-events:none;background:linear-gradient(120deg,rgba(255,255,255,.07),transparent 24%,transparent 72%,rgba(96,165,250,.05));}}
  .sub-hero{{position:relative;overflow:hidden;border-radius:26px;border:1px solid rgba(255,31,92,.30);background:radial-gradient(circle at 15% 50%,rgba(255,31,92,.16),transparent 30%),radial-gradient(circle at 85% 20%,rgba(59,130,246,.16),transparent 34%),linear-gradient(135deg,#091326,#120b1e);box-shadow:inset 0 1px 0 rgba(255,255,255,.10),0 18px 50px rgba(0,0,0,.30);}}
  .sub-hero-glow{{position:absolute;width:260px;height:260px;border-radius:50%;right:-110px;top:-130px;background:rgba(255,31,92,.14);filter:blur(45px);}}
  .sub-hero-content{{position:relative;display:flex;align-items:center;gap:16px;padding:20px 22px;}}
  .sub-brand-icon,.section-icon{{display:grid;place-items:center;flex:0 0 auto;border-radius:17px;}}
  .sub-brand-icon{{width:62px;height:62px;color:#ff4778;background:linear-gradient(145deg,rgba(255,31,92,.20),rgba(90,20,50,.24));border:1px solid rgba(255,71,120,.35);box-shadow:0 0 28px rgba(255,31,92,.16);}}
  .sub-brand-icon svg{{width:32px;height:32px;}}
  .sub-hero-copy{{min-width:0;flex:1}}.sub-eyebrow,.section-kicker{{font-size:9px;letter-spacing:.16em;color:rgba(255,255,255,.38);font-weight:900;}}.sub-hero h1{{margin:4px 0 2px;font-size:21px;font-weight:900;}}.sub-hero p{{margin:0;color:rgba(255,255,255,.45);font-size:11px;}}.sub-status{{display:inline-flex;align-items:center;gap:7px;padding:8px 12px;border-radius:999px;font-size:11px;font-weight:800;white-space:nowrap;}}.sub-status span{{width:7px;height:7px;border-radius:50%;background:currentColor;box-shadow:0 0 12px currentColor;}}
  .section-head{{position:relative;display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:17px;}}.section-head h2{{margin:4px 0 0;font-size:14px;font-weight:900;}}.section-icon{{width:38px;height:38px;border:1px solid rgba(255,255,255,.09);}}.section-icon svg{{width:18px;height:18px;}}.section-icon.red{{color:#ff4778;background:rgba(255,31,92,.09);border-color:rgba(255,71,120,.22);}}.section-icon.blue{{color:#60a5fa;background:rgba(59,130,246,.09);border-color:rgba(96,165,250,.20);}}.section-icon.purple{{color:#a78bfa;background:rgba(139,92,246,.09);border-color:rgba(167,139,250,.20);}}
  .sub-url-box{{position:relative;padding:14px 15px;border-radius:16px;border:1px solid rgba(255,71,120,.17);background:linear-gradient(135deg,rgba(255,31,92,.055),rgba(5,10,20,.42));color:#f5a1b8;font:11px/1.8 ui-monospace,Consolas,monospace;word-break:break-all;box-shadow:inset 0 1px 0 rgba(255,255,255,.05);}}.sub-actions{{display:flex;gap:9px;flex-wrap:wrap;margin-top:11px;}}.sub-action{{display:inline-flex;align-items:center;justify-content:center;gap:7px;padding:10px 14px;border-radius:13px;border:1px solid rgba(96,165,250,.20);background:rgba(59,130,246,.08);color:#cfe2ff;font-size:11px;font-weight:800;cursor:pointer;transition:.18s ease;}}.sub-action svg{{width:15px;height:15px;}}.sub-action:hover{{transform:translateY(-1px);background:rgba(59,130,246,.15);}}.sub-action.primary{{color:#fff;border-color:rgba(255,71,120,.42);background:linear-gradient(135deg,#ff1f5c,#c91550);box-shadow:0 7px 22px rgba(255,31,92,.18);}}.sub-action.primary:hover{{background:linear-gradient(135deg,#ff3a70,#df1b59);}}
  .telegram-sub-card{{position:relative;display:flex;align-items:center;gap:14px;padding:17px 18px;border-radius:22px;border:1px solid rgba(255,71,120,.24);background:linear-gradient(120deg,rgba(255,31,92,.10),rgba(19,33,60,.76),rgba(10,17,32,.86));box-shadow:inset 0 1px 0 rgba(255,255,255,.08),0 14px 34px rgba(0,0,0,.25);overflow:hidden;}}.telegram-sub-card:after{{content:"";position:absolute;inset:auto -15% -60% 30%;height:130px;background:rgba(255,31,92,.10);filter:blur(40px);pointer-events:none;}}.tg-sub-icon{{width:50px;height:50px;flex:0 0 50px;border-radius:16px;display:grid;place-items:center;color:#fff;background:linear-gradient(145deg,#ff1f5c,#c91550);box-shadow:0 8px 24px rgba(255,31,92,.22);}}.tg-sub-icon svg{{width:26px;height:26px;}}.tg-sub-copy{{min-width:0;flex:1;position:relative;z-index:1;}}.tg-sub-copy>span{{display:block;font-size:8px;letter-spacing:.13em;color:rgba(255,255,255,.38);font-weight:900;}}.tg-sub-copy strong{{display:block;margin-top:3px;font-size:13px;font-weight:900;}}.tg-sub-copy small{{display:block;margin-top:3px;color:rgba(255,255,255,.45);font-size:10px;}}.tg-sub-copy b{{display:inline-block;margin-top:5px;color:#ff6b92;font-size:11px;}}.tg-sub-join{{position:relative;z-index:1;display:inline-flex;align-items:center;gap:8px;padding:10px 14px;border-radius:13px;color:#fff;text-decoration:none;font-size:11px;font-weight:900;border:1px solid rgba(255,71,120,.38);background:rgba(255,31,92,.13);}}.tg-sub-join svg{{width:15px;height:15px;}}
  .usage-row{{position:relative;display:flex;align-items:center;gap:22px;}}.usage-ring{{position:relative;width:122px;height:122px;flex:0 0 122px;}}.usage-ring>div{{position:absolute;inset:0;display:grid;place-content:center;text-align:center;}}.usage-ring b{{font-size:19px;font-weight:900;}}.usage-ring span{{display:block;margin-top:3px;color:rgba(255,255,255,.40);font-size:9px;}}.usage-main{{min-width:0;flex:1}}.usage-main>strong{{display:block;font-size:21px;font-weight:900;}}.usage-main>strong i{{font-size:12px;color:rgba(255,255,255,.40);font-style:normal;font-weight:700;}}.usage-mini{{display:flex;justify-content:space-between;gap:10px;padding:10px 0;border-bottom:1px solid rgba(255,255,255,.06);font-size:10px;}}.usage-mini span{{color:rgba(255,255,255,.42);}}.usage-mini b{{color:rgba(255,255,255,.82);font-weight:800;}}.service-list{{position:relative;display:grid;gap:0;}}.service-list div{{display:flex;justify-content:space-between;gap:12px;padding:11px 0;border-bottom:1px solid rgba(255,255,255,.06);font-size:10px;}}.service-list div:last-child{{border-bottom:0;}}.service-list span{{color:rgba(255,255,255,.42);}}.service-list b{{font-size:10px;font-weight:800;text-align:left;}}.tech-grid{{position:relative;display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;}}.tech-grid>div{{padding:12px;border-radius:15px;border:1px solid rgba(255,255,255,.06);background:rgba(3,8,17,.25);}}.tech-grid span{{display:block;color:rgba(255,255,255,.36);font-size:9px;}}.tech-grid b{{display:block;margin-top:6px;color:#d7caff;font-size:11px;word-break:break-word;}}.direct-config{{position:relative;display:flex;align-items:center;gap:10px;padding:12px;border-radius:16px;border:1px solid rgba(255,255,255,.07);background:rgba(3,8,17,.25);}}.direct-config>div{{min-width:0;flex:1;}}.direct-config span{{display:block;color:rgba(255,255,255,.36);font-size:9px;margin-bottom:5px;}}.direct-config code{{display:block;color:#f0a1ba;font:10px/1.8 ui-monospace,Consolas,monospace;word-break:break-all;}}.download-grid{{position:relative;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px;}}.download-grid a{{display:flex;align-items:center;gap:10px;min-width:0;padding:12px;border-radius:16px;border:1px solid rgba(96,165,250,.15);background:rgba(59,130,246,.055);color:#fff;text-decoration:none;transition:.18s ease;}}.download-grid a:hover{{border-color:rgba(255,71,120,.30);transform:translateY(-1px);}}.app-icon{{width:38px;height:38px;flex:0 0 38px;border-radius:12px;display:grid;place-items:center;color:#ff5d88;font-size:10px;font-weight:900;border:1px solid rgba(255,71,120,.25);background:rgba(255,31,92,.09);}}.download-grid b{{display:block;font-size:11px;}}.download-grid span{{display:block;margin-top:3px;color:rgba(255,255,255,.38);font-size:8px;line-height:1.4;}}.download-grid i{{margin-right:auto;color:#ff5d88;font-style:normal;font-size:9px;font-weight:900;white-space:nowrap;}}
  @media (max-width:700px){{.sub-hero-content{{padding:16px;gap:11px;flex-wrap:wrap;}}.sub-brand-icon{{width:50px;height:50px;border-radius:14px;}}.sub-brand-icon svg{{width:27px;height:27px;}}.sub-hero h1{{font-size:17px}}.sub-hero p{{font-size:9px}}.sub-status{{margin-right:auto;font-size:9px;padding:6px 9px}}.sub-actions{{display:grid;grid-template-columns:1fr 1fr;}}.sub-actions .sub-action:last-child{{grid-column:1/-1}}.telegram-sub-card{{align-items:flex-start;flex-wrap:wrap;padding:14px}}.tg-sub-copy{{width:calc(100% - 64px)}}.tg-sub-join{{width:100%;justify-content:center}}.usage-row{{gap:13px}}.usage-ring{{width:105px;height:105px;flex-basis:105px}}.usage-ring svg{{width:105px;height:105px}}.tech-grid{{grid-template-columns:1fr}}.direct-config{{align-items:stretch;flex-direction:column}}.direct-config .sub-action{{width:100%}}.download-grid{{grid-template-columns:1fr}}.download-grid a{{padding:11px}}.sub-glass{{border-radius:20px}}.sub-url-box{{font-size:10px;}}}}
</style>
</head>
<body class="font-vazir text-slate-100 antialiased min-h-screen py-6 px-3 sm:px-4 md:py-10">

<div class="onex-sub-page w-full max-w-4xl mx-auto space-y-4 sm:space-y-5">

  <!-- Subscription Hero -->
  <section class="sub-hero">
    <div class="sub-hero-glow"></div>
    <div class="sub-hero-content">
      <div class="sub-brand-icon" aria-hidden="true">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 3 20 7v5c0 4.8-3.1 8.1-8 9-4.9-.9-8-4.2-8-9V7l8-4Z"/><path d="m8.8 12.2 2.1 2.1 4.5-4.6"/></svg>
      </div>
      <div class="sub-hero-copy">
        <span class="sub-eyebrow">ONEX SUBSCRIPTION</span>
        <h1>{label_escaped}</h1>
        <p>اشتراک شما آماده است؛ لینک را در کلاینت دلخواه وارد کنید.</p>
      </div>
      <div class="sub-status {status_badge_html}"><span></span>{status_text}</div>
    </div>
  </section>

  <!-- Subscription link -->
  <section class="sub-glass sub-link-card">
    <div class="section-head">
      <div><span class="section-kicker">SUBSCRIPTION LINK</span><h2>لینک اشتراک</h2></div>
      <div class="section-icon red"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 0 0 7.1 0l2.8-2.8a5 5 0 0 0-7.1-7.1L11.4 4.5"/><path d="M14 11a5 5 0 0 0-7.1 0L4.1 13.8a5 5 0 0 0 7.1 7.1l1.4-1.4"/></svg></div>
    </div>
    <div class="sub-url-box" dir="ltr"><span id="subLinkText">{sub_url_escaped}</span></div>
    <div class="sub-actions">
      <button class="sub-action primary" id="subCopyBtn" type="button" onclick="copySubLink()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg><span>کپی لینک</span></button>
      <button class="sub-action" type="button" onclick="openQrFor(subUrlData,'QR اشتراک')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg><span>QR Code</span></button>
      <button class="sub-action" type="button" onclick="copySubLink(true)"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/></svg><span>کپی برای کلاینت</span></button>
    </div>
  </section>

  <!-- Telegram channel -->
  <section class="telegram-sub-card">
    <div class="tg-sub-icon"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M21.6 3.1 18.2 19c-.26 1.16-.95 1.45-1.92.9l-5.22-3.84-2.52 2.43c-.28.28-.51.51-1.05.51l.37-5.32 9.68-8.75c.42-.37-.09-.58-.65-.21L4.92 12.86.14 11.34c-1.04-.33-1.06-1.04.22-1.54L19.04 2.56c.88-.33 1.65.2 1.56.54Z"/></svg></div>
    <div class="tg-sub-copy"><span>OFFICIAL TELEGRAM CHANNEL</span><strong>عضویت در کانال تلگرام ONEX</strong><small>اخبار، آپدیت‌ها و اطلاع‌رسانی‌های سرویس را مستقیم دریافت کنید.</small><b dir="ltr">@V2rayTun0</b></div>
    <a class="tg-sub-join" href="https://t.me/V2rayTun0" target="_blank" rel="noopener noreferrer"><span>عضویت در کانال</span><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m5 12 14-9-4 18-4-7-6-2Z"/><path d="m11 14 4-6"/></svg></a>
  </section>

  <!-- Usage + service -->
  <section class="grid grid-cols-1 lg:grid-cols-[1.6fr_1fr] gap-4 sm:gap-5">
    <div class="sub-glass p-5 sm:p-6">
      <div class="section-head"><div><span class="section-kicker">TRAFFIC OVERVIEW</span><h2>مصرف سرویس</h2></div><div class="section-icon blue"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="m7 15 4-6 3 3 4-7"/></svg></div></div>
      <div class="usage-row">
        <div class="usage-ring"><svg width="122" height="122" viewBox="0 0 132 132" class="-rotate-90"><circle cx="66" cy="66" r="54" fill="none" stroke="rgba(255,255,255,.07)" stroke-width="10"/><circle cx="66" cy="66" r="54" fill="none" stroke="url(#usageRingGradient)" stroke-width="10" stroke-linecap="round" stroke-dasharray="339.29" stroke-dashoffset="{dash_calc_offset}"/><defs><linearGradient id="usageRingGradient" x1="0%" y1="0%" x2="100%" y2="100%"><stop offset="0%" stop-color="#ff1f5c"/><stop offset="100%" stop-color="#8b5cf6"/></linearGradient></defs></svg><div><b>{usage_percent}%</b><span>مصرف‌شده</span></div></div>
        <div class="usage-main"><strong>{used_bytes_str}<i> / {limit_bytes_str}</i></strong><div class="usage-mini"><span>باقی‌مانده</span><b>{remaining_value_escaped}</b></div><div class="usage-mini"><span>زمان باقی‌مانده</span><b>{expiry_remaining_escaped}</b></div></div>
      </div>
    </div>
    <div class="sub-glass p-5 sm:p-6"><div class="section-head"><div><span class="section-kicker">SERVICE</span><h2>وضعیت سرویس</h2></div><div class="section-icon purple"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v18M3 12h18"/><circle cx="12" cy="12" r="9"/></svg></div></div><div class="service-list"><div><span>انقضا</span><b>{expiry_display_escaped}</b></div><div><span>محدودیت IP</span><b>{ip_limit_escaped}</b></div><div><span>اتصال همزمان</span><b>{connection_limit_escaped}</b></div><div><span>سرعت</span><b>{speed_limit_escaped}</b></div></div></div>
  </section>

  <!-- Technical details -->
  <section class="sub-glass p-5 sm:p-6">
    <div class="section-head"><div><span class="section-kicker">CONFIGURATION</span><h2>جزئیات فنی</h2></div><div class="section-icon purple"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 21v-7M4 10V3M12 21v-11M12 6V3M20 21v-5M20 12V3"/><path d="M1 14h6M9 8h6M17 16h6"/></svg></div></div>
    <div class="tech-grid"><div><span>Protocol</span><b dir="ltr">{protocol_escaped}</b></div><div><span>Fingerprint</span><b dir="ltr">{fingerprint_escaped}</b></div><div><span>IP Limit</span><b>{ip_limit_escaped}</b></div><div><span>Connection</span><b>{connection_limit_escaped}</b></div></div>
  </section>

  <!-- Direct config -->
  <section class="sub-glass p-5 sm:p-6">
    <div class="section-head"><div><span class="section-kicker">DIRECT CONFIG</span><h2>کانفیگ مستقیم</h2></div><div class="section-icon red"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M8 9h8M8 13h5"/><path d="M5 3h14a2 2 0 0 1 2 2v10a2 2 0 0 1-2 2h-5l-4 4v-4H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2Z"/></svg></div></div>
    <div class="direct-config"><div><span>VLESS / ONEX</span><code id="vlessLinkText">{vless_url_escaped}</code></div><button class="sub-action primary" id="vlessCopyBtn" type="button" onclick="pxCopy('vlessLinkText','vlessCopyBtn')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg><span>کپی</span></button></div>
  </section>

  <!-- Downloads -->
  <section class="sub-glass p-5 sm:p-6">
    <div class="section-head"><div><span class="section-kicker">OFFICIAL RELEASES</span><h2>دانلود برنامه‌ها</h2></div><div class="section-icon blue"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 3v12"/><path d="m7 10 5 5 5-5"/><path d="M5 21h14"/></svg></div></div>
    <div class="download-grid">
      <a href="https://github.com/2dust/v2rayNG/releases/latest" target="_blank" rel="noopener noreferrer"><div class="app-icon">NG</div><div><b>v2rayNG</b><span>Android</span></div><i>دانلود</i></a>
      <a href="https://github.com/2dust/v2rayN/releases/latest" target="_blank" rel="noopener noreferrer"><div class="app-icon">N</div><div><b>v2rayN</b><span>Windows / macOS / Linux</span></div><i>دانلود</i></a>
      <a href="https://github.com/hiddify/hiddify-app/releases/latest" target="_blank" rel="noopener noreferrer"><div class="app-icon">H</div><div><b>Hiddify</b><span>Android / Windows / macOS / Linux</span></div><i>دانلود</i></a>
    </div>
  </section>

</div>

<!-- QR Code Modal Popup -->
<div id="qrModal" class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-md hidden">
  <div class="w-full max-w-sm rounded-[24px] border border-white/15 bg-[#0b0c14] p-6 text-center shadow-2xl relative">
    <button type="button" onclick="closeQrModal()" class="absolute top-4 left-4 w-8 h-8 rounded-full bg-white/5 border border-white/10 grid place-items-center text-white/60 hover:text-white">✕</button>
    <p class="text-sm font-black text-white/90 mb-2">QR Code اسکن کانفیگ</p>
    <p class="text-[11px] text-white/40 mb-4">برای اتصال سریع با گوشی موبایل</p>
    <div id="qrcodeContainer" class="bg-white p-4 rounded-2xl inline-block mx-auto mb-4 border border-white/10"></div>
    <p id="qrModalText" class="text-[10px] text-purple-300 break-all max-h-16 overflow-y-auto px-2" dir="ltr"></p>
  </div>
</div>

<script>
const vlessUrlData = "{vless_url}";

// Theme toggle logic with localStorage support (2 themes total)
function toggleTheme() {{
  const body = document.body;
  body.classList.toggle('theme-lighter');
  const isLighter = body.classList.contains('theme-lighter');
  localStorage.setItem('px_theme', isLighter ? 'lighter' : 'dark');
}}

// Initialize saved theme on load
(function() {{
  if (localStorage.getItem('px_theme') === 'lighter') {{
    document.body.classList.add('theme-lighter');
  }}
}})();

function openQrModal() {{
  var modal = document.getElementById('qrModal');
  var container = document.getElementById('qrcodeContainer');
  var txtEl = document.getElementById('qrModalText');
  container.innerHTML = "";
  txtEl.textContent = vlessUrlData;
  modal.classList.remove('hidden');
  try {{
    var typeNumber = 0;
    var errorCorrectionLevel = 'L';
    var qr = qrcode(typeNumber, errorCorrectionLevel);
    qr.addData(vlessUrlData);
    qr.make();
    container.innerHTML = qr.createImgTag(5, 8);
  }} catch (e) {{
    container.innerHTML = "<p class='text-xs text-black'>خطا در تولید QR Code</p>";
  }}
}}

function closeQrModal() {{
  document.getElementById('qrModal').classList.add('hidden');
}}

document.getElementById('qrModal').addEventListener('click', function(e) {{
  if (e.target === this) closeQrModal();
}});

const subUrlData = "{sub_url}";
function openQrFor(value,label) {{
  var modal=document.getElementById('qrModal'), container=document.getElementById('qrcodeContainer'), txt=document.getElementById('qrModalText');
  if(!modal||!container) return;
  container.innerHTML=''; txt.textContent=value;
  var title=modal.querySelector('p'); if(title) title.textContent=label||'QR Code';
  modal.classList.remove('hidden');
  try {{ var qr=qrcode(0,'L'); qr.addData(value); qr.make(); container.innerHTML=qr.createImgTag(5,8); }} catch(e) {{ container.innerHTML='<p class="text-xs text-black">خطا در تولید QR Code</p>'; }}
}}
function copySubLink() {{
  var text=subUrlData, btn=document.getElementById('subCopyBtn');
  var done=function(){{ if(!btn)return; var old=btn.innerHTML; btn.innerHTML='<span>✓ کپی شد</span>'; setTimeout(function(){{btn.innerHTML=old;}},1600); }};
  if(navigator.clipboard&&navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(done).catch(function(){{fallbackCopy(text,done);}}); else fallbackCopy(text,done);
}}

function pxCopy(textId, btnId) {{
  var el = document.getElementById(textId);
  var btn = document.getElementById(btnId);
  if (!el || !btn) return;
  var text = el.textContent.textContext || el.textContent.trim();
  var done = function() {{
    var original = btn.getAttribute('data-original');
    if (!original) {{
      original = btn.innerHTML;
      btn.setAttribute('data-original', original);
    }}
    btn.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6L9 17l-5-5"/></svg><span>کپی شد</span>';
    btn.classList.add('text-emerald-300','border-emerald-400/30','bg-emerald-400/10');
    setTimeout(function() {{
      btn.innerHTML = original;
      btn.classList.remove('text-emerald-300','border-emerald-400/30','bg-emerald-400/10');
    }}, 1700);
  }};
  if (navigator.clipboard && navigator.clipboard.writeText) {{
    navigator.clipboard.writeText(text).then(done).catch(function() {{ fallbackCopy(text, done); }});
  }} else {{
    fallbackCopy(text, done);
  }}
}}
function fallbackCopy(text, cb) {{
  var ta = document.createElement('textarea');
  ta.value = text;
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  ta.select();
  try {{ document.execCommand('copy'); }} catch (e) {{}}
  document.body.removeChild(ta);
  if (cb) cb();
}}
</script>
</body>
</html>"""
    return HTMLResponse(info_html)
# ============================================================
# SUB GROUP API
# ============================================================

@app.post("/api/subs")
async def create_sub_api(
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    sub_id, sub = await create_sub_group(
        name=body.get(
            "name",
            "گروه جدید",
        ),
        desc=body.get(
            "desc",
            "",
        ),
        password=body.get(
            "password",
            "",
        ),
    )

    host = get_host(request)

    return {
        "sub_id":
            sub_id,

        **sub,

        "password_hash":
            None,

        "public_url":
            (
                f"https://{host}"
                f"/p/{sub['uuid_key']}"
            ),

        "sub_url":
            (
                f"https://{host}"
                f"/sub-group/{sub['uuid_key']}"
            ),
    }


@app.get("/api/subs")
async def list_subs_api(
    request: Request,
    _=Depends(require_auth),
):

    host = get_host(request)

    async with SUBS_LOCK:
        snapshot_subs = dict(SUBS)

    async with LINKS_LOCK:
        snapshot_links = dict(LINKS)

    result = []

    for sid, sub in snapshot_subs.items():

        link_ids = sub.get(
            "link_ids",
            [],
        )

        # A member config is counted once; enabled group protocols only
        # control how many protocol variants are emitted to the subscription.
        visible_ids = [lid for lid in link_ids if lid in snapshot_links]
        active_count = sum(
            1
            for lid in visible_ids
            if is_link_allowed(snapshot_links.get(lid))
        )

        total_used = sum(
            snapshot_links[lid].get("used_bytes", 0)
            for lid in visible_ids
        )

        result.append(
            {
                "sub_id":
                    sid,

                **sub,

                "password_hash":
                    None,

                "has_password":
                    sub.get(
                        "password_hash"
                    ) is not None,

                "protocols": list(sub.get("protocols", PROTOCOLS)),

                "links_count":
                    len(link_ids),

                "active":
                    bool(sub.get("active", True)),

                "active_count":
                    active_count,

                "total_used_bytes":
                    total_used,

                "total_used_fmt":
                    fmt_bytes(
                        total_used
                    ),

                "public_url":
                    (
                        f"https://{host}"
                        f"/p/{sub['uuid_key']}"
                    ),

                "sub_url":
                    (
                        f"https://{host}"
                        f"/sub-group/{sub['uuid_key']}"
                    ),
            }
        )

    result.sort(
        key=lambda item:
            item.get(
                "created_at",
                "",
            ),
        reverse=True,
    )

    return {
        "subs": result
    }


@app.patch("/api/subs/{sub_id}")
async def update_sub_api(
    sub_id: str,
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    async with SUBS_LOCK:

        if sub_id not in SUBS:
            raise HTTPException(
                status_code=404,
                detail="sub not found",
            )

        sub = SUBS[sub_id]

        if "name" in body:
            sub["name"] = str(
                body["name"]
            )[:60]

        if "desc" in body:
            sub["desc"] = str(
                body["desc"]
            )[:200]

        if "password" in body:

            password = str(
                body.get(
                    "password",
                    "",
                )
            ).strip()

            sub["password_hash"] = (
                hash_password(password)
                if password
                else None
            )

        if "active" in body:
            sub["active"] = bool(body.get("active"))

        if "protocols" in body:
            raw_protocols = body.get("protocols") or []
            if not isinstance(raw_protocols, list):
                raw_protocols = []
            allowed = {str(p) for p in PROTOCOLS}
            sub["protocols"] = [str(p) for p in raw_protocols if str(p) in allowed]

    await save_state()

    return {
        "ok": True
    }


@app.delete("/api/subs/{sub_id}")
async def delete_sub_api(
    sub_id: str,
    _=Depends(require_auth),
):

    name = await remove_sub_group(
        sub_id
    )

    if name is None:
        raise HTTPException(
            status_code=404,
            detail="sub not found",
        )

    return {
        "ok": True,
        "deleted": sub_id,
    }


@app.post("/api/subs/{sub_id}/links")
async def assign_link_to_sub(
    sub_id: str,
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    link_id = str(
        body.get(
            "link_id",
            "",
        )
    )

    action = str(
        body.get(
            "action",
            "add",
        )
    )

    if action == "add":

        success = await set_link_sub(
            link_id,
            sub_id,
        )

    else:

        success = await set_link_sub(
            link_id,
            None,
        )

    if not success:
        raise HTTPException(
            status_code=404,
            detail="link or sub not found",
        )

    return {
        "ok": True
    }


@app.post("/api/subs/{sub_id}/sync")
async def sync_sub_links(
    sub_id: str,
    request: Request,
    _=Depends(require_auth),
):
    """Synchronize a subscription group's config membership in one request."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر است")

    raw_ids = body.get("link_ids") or []
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="link_ids نامعتبر است")

    desired = [str(x) for x in raw_ids if str(x)]
    async with SUBS_LOCK:
        if sub_id not in SUBS:
            raise HTTPException(status_code=404, detail="sub not found")
        previous = {str(x) for x in SUBS[sub_id].get("link_ids", [])}

    async with LINKS_LOCK:
        valid = [uid for uid in desired if uid in LINKS]
        current_owner = {uid: LINKS[uid].get("sub_id") for uid in valid}

    async with SUBS_LOCK:
        # Remove desired links from any previous subscription group.
        for other_id, other in SUBS.items():
            if other_id == sub_id:
                continue
            ids = other.get("link_ids", [])
            other["link_ids"] = [uid for uid in ids if str(uid) not in set(valid)]
        SUBS[sub_id]["link_ids"] = valid

    async with LINKS_LOCK:
        for uid in valid:
            if uid in LINKS:
                LINKS[uid]["sub_id"] = sub_id
        for uid in (previous - set(valid)):
            if uid in LINKS and LINKS[uid].get("sub_id") == sub_id:
                LINKS[uid]["sub_id"] = None

    await save_state()
    return {"ok": True, "link_ids": valid, "count": len(valid), "moved": len([u for u,v in current_owner.items() if v and v != sub_id])}


# ============================================================
# GROUP SUB
# ============================================================

@app.get("/sub-group/{uuid_key}")
async def sub_group_subscription(
    uuid_key: str,
    request: Request,
):

    async with SUBS_LOCK:

        sub = next(
            (
                item
                for item
                in SUBS.values()
                if item.get(
                    "uuid_key"
                ) == uuid_key
            ),
            None,
        )

    if not sub:
        raise HTTPException(
            status_code=404,
            detail="not found",
        )

    if not bool(sub.get("active", True)):
        raise HTTPException(
            status_code=403,
            detail="subscription group is inactive",
        )

    if sub.get(
        "password_hash"
    ):

        password = (
            request.query_params.get(
                "pw",
                "",
            )
        )

        if (
            hash_password(password)
            != sub["password_hash"]
        ):

            raise HTTPException(
                status_code=403,
                detail="wrong password",
            )

    host = get_host(request)

    async with LINKS_LOCK:

        lines = []
        used_names: set[str] = set()
        group_protocols = [p for p in sub.get("protocols", PROTOCOLS) if str(p) in PROTOCOLS]

        for link_id in sub.get("link_ids", []):
            link = LINKS.get(link_id)
            if link and is_link_allowed(link):
                lines.extend(group_subscription_lines_for_link(
                    link, link_id, host, group_protocols, used_names
                ))

    content = (
        base64
        .b64encode(
            "\n".join(
                lines
            ).encode()
        )
        .decode()
    )

    total_used = 0
    total_limit = 0
    expiries = []
    valid_ids = list(sub.get("link_ids", []))

    async with LINKS_LOCK:
        for link_id in valid_ids:
            link = LINKS.get(link_id)
            if (
                not link
                or not is_link_allowed(link)
                or link.get("protocol") not in set(sub.get("protocols", PROTOCOLS))
            ):
                continue
            total_used += int(link.get("used_bytes", 0) or 0)
            total_limit += int(link.get("limit_bytes", 0) or 0)
            if link.get("expires_at"):
                expiries.append(str(link.get("expires_at")))

    # For a group subscription, expose aggregate usage/expiry in standard headers.
    group_limit = total_limit if total_limit > 0 else 0
    group_expiry = None
    if expiries:
        try:
            group_expiry = min(
                expiries,
                key=lambda x: datetime.fromisoformat(x)
            )
        except Exception:
            group_expiry = expiries[0]

    group_volume_text = (
        f"{fmt_bytes(total_used)}/{fmt_bytes(group_limit)}"
        if group_limit > 0
        else f"{fmt_bytes(total_used)}/∞"
    )
    group_expiry_text = group_expiry or "∞"
    group_title = (
        f"0.0.0.0 | {group_volume_text} | {group_expiry_text} | "
        f"{sub['name']} | کانال تلگرام: logic_sec"
    )
    headers = subscription_metadata_headers(
        total_used,
        group_limit,
        group_expiry,
        host,
        f"https://{host}/public-sub/{uuid_key}",
        group_title,
    )

    return Response(
        content=content,
        media_type="text/plain; charset=utf-8",
        headers=headers,
    )


# ============================================================
# PUBLIC GROUP
# ============================================================

PUBLIC_SUB_HTML = r"""
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<title>ONEX Panel</title>
<style>
:root{
  --bg:#050a16;
  --panel:#081427;
  --line:rgba(90,170,255,.18);
  --blue:#55a9ff;
  --pink:#ff2d6f;
  --text:#f7f9ff;
  --muted:#9aa8bd;
}
*{box-sizing:border-box}
html,body{min-height:100%;margin:0}
body{
  min-height:100vh;
  padding:24px 16px;
  display:flex;
  align-items:center;
  justify-content:center;
  font-family:Arial,"Tahoma",sans-serif;
  color:var(--text);
  background:
    radial-gradient(circle at 85% 5%,rgba(53,116,255,.20),transparent 34%),
    radial-gradient(circle at 10% 90%,rgba(255,34,105,.10),transparent 30%),
    linear-gradient(145deg,#040710,#071326 55%,#060a14);
}
.shell{
  width:100%;
  max-width:570px;
}
.card{
  position:relative;
  overflow:hidden;
  padding:28px;
  border:1px solid rgba(105,178,255,.20);
  border-radius:28px;
  background:linear-gradient(145deg,rgba(10,25,48,.93),rgba(5,13,27,.94));
  box-shadow:0 18px 55px rgba(0,0,0,.34),0 0 35px rgba(42,126,255,.08);
}
.card:before{
  content:"";
  position:absolute;
  top:0;left:8%;right:8%;height:1px;
  background:linear-gradient(90deg,transparent,var(--blue),var(--pink),transparent);
}
.brand{
  display:flex;
  align-items:center;
  gap:14px;
  margin-bottom:22px;
}
.logo{
  width:54px;height:54px;
  display:grid;place-items:center;
  flex:none;
  border-radius:17px;
  background:linear-gradient(145deg,#257cff,#4f46e5);
  border:1px solid rgba(117,194,255,.35);
  box-shadow:0 8px 25px rgba(37,124,255,.20);
  font-size:24px;font-weight:800;
}
.brand h1{margin:0;font-size:25px;letter-spacing:.2px}
.brand p{margin:5px 0 0;color:var(--muted);font-size:12px}
.status{
  margin-right:auto;
  padding:7px 11px;
  border-radius:999px;
  color:#76f0bd;
  background:rgba(22,190,119,.10);
  border:1px solid rgba(54,224,153,.22);
  font-size:11px;
  white-space:nowrap;
}
.status i{display:inline-block;width:7px;height:7px;border-radius:50%;background:#35df96;margin-left:5px}
.message{
  padding:16px 18px;
  border:1px solid rgba(255,255,255,.07);
  border-radius:17px;
  background:rgba(255,255,255,.035);
  color:#dce4f2;
  line-height:1.9;
  font-size:14px;
}
.version{display:block;color:#71b5ff;font-size:11px;margin-top:4px}
.label{margin:22px 2px 9px;color:#8999b0;font-size:11px}
.urlbox{
  display:flex;
  align-items:stretch;
  gap:9px;
  padding:7px;
  border-radius:18px;
  background:rgba(1,7,17,.62);
  border:1px solid rgba(83,157,255,.17);
}
.url{
  flex:1;
  min-width:0;
  padding:11px 12px;
  border-radius:13px;
  color:#9ccaff;
  direction:ltr;
  text-align:left;
  word-break:break-all;
  font-family:Consolas,monospace;
  font-size:12px;
  line-height:1.65;
}
.copy{
  align-self:stretch;
  min-width:76px;
  border:1px solid rgba(255,45,111,.45);
  border-radius:13px;
  background:linear-gradient(135deg,#ff316f,#df1657);
  color:white;
  font:700 12px Arial;
  cursor:pointer;
}
.copy:active{transform:scale(.98)}
.supportbar{
  margin-top:18px;
  display:flex;
  align-items:center;
  gap:13px;
  padding:13px 15px;
  border-radius:18px;
  background:linear-gradient(110deg,rgba(255,38,106,.11),rgba(72,141,255,.09));
  border:1px solid rgba(255,55,119,.20);
  box-shadow:inset 0 1px 0 rgba(255,255,255,.035);
}
.supporticon{
  width:42px;height:42px;
  display:grid;place-items:center;
  border-radius:13px;
  background:rgba(255,45,111,.12);
  border:1px solid rgba(255,45,111,.30);
  color:#ff5d8f;
  font-size:20px;
}
.supporttext{flex:1;min-width:0}
.supporttext strong{display:block;font-size:12px;margin-bottom:4px}
.supporttext span{color:#8e9db2;font-size:11px}
.supportbar a{
  padding:9px 13px;
  border-radius:11px;
  color:#ff78a4;
  border:1px solid rgba(255,45,111,.27);
  background:rgba(255,45,111,.07);
  text-decoration:none;
  font-size:11px;
  white-space:nowrap;
}
.footer{
  margin-top:18px;
  padding-top:14px;
  border-top:1px solid rgba(255,255,255,.06);
  display:flex;
  justify-content:space-between;
  gap:12px;
  color:#66758b;
  font-size:10px;
}
@media(max-width:480px){
  body{padding:15px 11px}
  .card{padding:20px;border-radius:24px}
  .brand h1{font-size:22px}
  .logo{width:48px;height:48px;border-radius:15px}
  .status{font-size:10px;padding:6px 9px}
  .urlbox{display:block}
  .url{display:block;padding:10px 9px}
  .copy{width:100%;height:42px}
  .supportbar{align-items:flex-start}
  .supportbar a{align-self:center}
}
</style>
</head>
<body>
<main class="shell">
  <section class="card">
    <div class="brand">
      <div class="logo">N</div>
      <div>
        <h1>ONEX Panel</h1>
        <p>درگاه امن اشتراک گروه</p>
      </div>
      <div class="status"><i></i> آماده</div>
    </div>

    <div class="message">
      اشتراک شما آماده است؛ لینک زیر را در کلاینت موردنظر خود وارد کنید.
      <span class="version">نسخه سرویس __ONEX_VERSION__</span>
    </div>

    <div class="label">لینک اشتراک</div>
    <div class="urlbox">
      <div class="url" id="subUrl"></div>
      <button class="copy" id="copyBtn" type="button" onclick="copySubUrl()">کپی لینک</button>
    </div>

    <div class="supportbar">
      <div class="supporticon">✦</div>
      <div class="supporttext">
        <strong>پشتیبانی ONEX</strong>
        <span>برای راهنمایی و دریافت پشتیبانی با ما در ارتباط باشید</span>
      </div>
      <a href="https://t.me/V2rayTun0" target="_blank" rel="noopener">@V2rayTun0</a>
    </div>

    <div class="footer">
      <span>ONEX Subscription</span>
      <span>© ONEX Panel</span>
    </div>
  </section>
</main>
<script>
const url = location.origin + location.pathname.replace("/p/","/sub-group/");
document.getElementById("subUrl").textContent = url;
async function copySubUrl(){
  const btn=document.getElementById("copyBtn");
  try{
    await navigator.clipboard.writeText(url);
    btn.textContent="✓ کپی شد";
  }catch(e){
    const ta=document.createElement("textarea");
    ta.value=url;document.body.appendChild(ta);ta.select();
    document.execCommand("copy");ta.remove();btn.textContent="✓ کپی شد";
  }
  setTimeout(()=>btn.textContent="کپی لینک",1800);
}
</script>
</body>
</html>
"""


@app.get(
    "/p/{uuid_key}",
    response_class=HTMLResponse,
)
async def public_sub_page(
    uuid_key: str,
):

    async with SUBS_LOCK:

        exists = any(
            item.get(
                "uuid_key"
            ) == uuid_key
            for item in SUBS.values()
        )

    if not exists:

        return HTMLResponse(
            """
            <h2
            style="
            font-family:sans-serif;
            padding:40px;
            "
            >
            گروه پیدا نشد
            </h2>
            """,
            status_code=404,
        )

    return HTMLResponse(
        PUBLIC_SUB_HTML.replace("__ONEX_VERSION__", str(APP_VERSION))
    )


@app.get("/api/public/sub/{uuid_key}")
async def public_sub_data(
    uuid_key: str,
    request: Request,
):

    async with SUBS_LOCK:

        entry = next(
            (
                (
                    sid,
                    item,
                )

                for sid, item
                in SUBS.items()

                if item.get(
                    "uuid_key"
                ) == uuid_key
            ),
            None,
        )

    if not entry:
        raise HTTPException(
            status_code=404,
            detail="not found",
        )

    _, sub = entry

    if not bool(sub.get("active", True)):
        return JSONResponse({"active": False, "name": sub.get("name", "گروه")})

    has_password = (
        sub.get(
            "password_hash"
        ) is not None
    )

    if has_password:

        password = (
            request
            .query_params
            .get(
                "pw",
                "",
            )
        )

        if (
            hash_password(password)
            != sub[
                "password_hash"
            ]
        ):

            return JSONResponse(
                {
                    "locked": True,
                    "name":
                        sub["name"],
                }
            )

    host = get_host(request)

    async with LINKS_LOCK:
        snapshot = dict(LINKS)

    links_out = []

    active_connections = 0

    for link_id in sub.get(
        "link_ids",
        [],
    ):

        link = snapshot.get(
            link_id
        )

        if not link:
            continue

        protocols = [p for p in (sub.get("protocols") or PROTOCOLS) if str(p) in PROTOCOLS]
        allowed = is_link_allowed(
            link
        )

        connection_count = sum(
            1
            for item in connections.values()
            if item.get("uuid") == link_id
        )

        active_connections += (
            connection_count
        )

        # Mirror the real group subscription: one public item per enabled
        # protocol, while usage/limits stay attached to the same underlying config.
        for proto in protocols:
            links_out.append({
                "uuid": link_id,
                "label": link.get("label"),
                "active": allowed,
                "protocol": proto,
                "used_bytes": link.get("used_bytes", 0),
                "used_fmt": fmt_bytes(link.get("used_bytes", 0)),
                "limit_bytes": link.get("limit_bytes", 0),
                "limit_fmt": ("∞" if not link.get("limit_bytes", 0) else fmt_bytes(link["limit_bytes"])),
                "expires_at": link.get("expires_at"),
                "vless_link": generate_vless_link(
                    link_id, host,
                    remark=f"{link.get('label') or 'Config'} | {PROTOCOL_LABELS.get(proto, proto)}",
                    protocol=proto,
                    fingerprint=link.get("fingerprint", DEFAULT_FINGERPRINT),
                    alpn=DEFAULT_ALPN_BY_PROTOCOL.get(proto, link.get("alpn")),
                    port=protocol_public_port(link, proto, link.get("port", DEFAULT_PORT)),
                    link=link,
                ),
                "sub_url": f"https://{host}/sub/{link_id}",
                "info_url": f"https://{host}/info/{link_id}",
                "connections": connection_count,
                "ip_limit": link.get("ip_limit", 0),
                "speed_limit_bytes": link.get("speed_limit_bytes", 0),
                "connection_limit": link.get("connection_limit", 0),
            })

    total_used = sum(
        item["used_bytes"]
        for item in links_out
    )

    return {
        "locked": False,

        "name":
            sub["name"],

        "desc":
            sub.get(
                "desc",
                "",
            ),

        "sub_url":
            (
                f"https://{host}"
                f"/sub-group/{uuid_key}"
            ),

        "active_connections":
            active_connections,

        "total_used_fmt":
            fmt_bytes(
                total_used
            ),

        "support":
            SUPPORT_USERNAME,

        "links":
            links_out,
    }




@app.post("/api/mix-sub")
async def mix_subscription(request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر")
    ids = body.get("link_ids") or []
    if not isinstance(ids, list) or len(ids) < 2:
        raise HTTPException(status_code=400, detail="حداقل ۲ کانفیگ انتخاب کنید")
    if len(ids) > 40:
        raise HTTPException(status_code=400, detail="حداکثر ۴۰ کانفیگ")
    host = get_host(request)
    lines = []
    used_names = set()
    total_used = 0
    total_limit = 0
    labels = []
    async with LINKS_LOCK:
        for lid in ids:
            link = LINKS.get(lid)
            if not link or not is_link_allowed(link):
                continue
            labels.append(str(link.get("label") or lid[:8]))
            total_used += int(link.get("used_bytes", 0) or 0)
            total_limit += int(link.get("limit_bytes", 0) or 0)
            name = project_config_name(used_names)
            used_names.add(name)
            lines.append(generate_vless_link(
                lid, host, remark=name,
                protocol=link.get("protocol", DEFAULT_PROTOCOL),
                fingerprint=link.get("fingerprint", DEFAULT_FINGERPRINT),
                alpn=link.get("alpn"),
                port=link.get("port", DEFAULT_PORT),
            ))
    if not lines:
        raise HTTPException(status_code=400, detail="هیچ کانفیگ معتبری انتخاب نشده")
    # stats first line
    vol = f"{fmt_bytes(total_used)}/{fmt_bytes(total_limit)}" if total_limit > 0 else f"{fmt_bytes(total_used)}/∞"
    mix_label = f"{APP_NAME}-Mix-{random_config_name()[:6]}"
    stats = f"{mix_label} | {vol} | {len(lines)} configs"
    first = generate_vless_link(ids[0], "127.0.0.1", remark=stats, protocol="vless-ws")
    content = base64.b64encode(("\n".join([first] + lines)).encode()).decode()
    # store as a sub group for reuse
    sub_id, sub = await create_sub_group(name=mix_label, desc="مخلوط‌سازی کانفیگ‌ها")
    async with SUBS_LOCK:
        if sub_id in SUBS:
            SUBS[sub_id]["link_ids"] = list(ids)
    await save_state()
    return {
        "ok": True,
        "sub_url": f"https://{host}/sub-group/{sub['uuid_key']}",
        "name": mix_label,
        "count": len(lines),
        "content_preview": stats,
    }


@app.get("/api/categories")
async def list_categories(_=Depends(require_auth)):
    items = [{**cat, "id": cid} for cid, cat in CATEGORIES.items()]
    items.sort(key=lambda x: int(x.get("number", 0)))
    return {"categories": items}

@app.post("/api/categories")
async def create_category(request: Request, _=Depends(require_auth)):
    if len(CATEGORIES) >= 50:
        raise HTTPException(status_code=400, detail="حداکثر ۵۰ گروه")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر")
    name = str(body.get("name") or "دسته جدید").strip()[:40]
    used = {int(x.get("number", 0)) for x in CATEGORIES.values()}
    num = 0
    while num in used:
        num += 1
    cid = str(num)
    limit_value = safe_float(body.get("limit_value", 0))
    limit_unit = str(body.get("limit_unit") or "GB").upper()
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    speed_value = safe_float(body.get("speed_limit_value", 0))
    speed_bytes = 0 if speed_value <= 0 else parse_speed_to_bytes(speed_value, "MBIT")
    raw_clean = body.get("clean_ips") or ""
    if isinstance(raw_clean, list):
        clean_ips = [str(x).strip() for x in raw_clean if str(x).strip()]
    else:
        clean_ips = [x.strip() for x in str(raw_clean).replace(",", "\n").splitlines() if x.strip()]
    record = {
        "id": cid, "name": name, "number": num,
        "limit_bytes": limit_bytes,
        "expires_days": safe_int(body.get("expires_days", 0), minimum=0),
        "connection_limit": safe_int(body.get("connection_limit", 0), minimum=0),
        "speed_limit_bytes": speed_bytes,
        "ip_limit": safe_int(body.get("ip_limit", 0), minimum=0),
        "clean_ips": clean_ips,
        "random_name": bool(body.get("random_name", False)),
        "single_user": bool(body.get("single_user", False)),
        "created_at": datetime.now().isoformat(),
    }
    CATEGORIES[cid] = record
    await save_state()
    return {"ok": True, **record}


@app.patch("/api/categories/{cid}")
async def update_category(cid: str, request: Request, _=Depends(require_auth)):
    if cid not in CATEGORIES:
        raise HTTPException(status_code=404, detail="یافت نشد")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر")
    cat = CATEGORIES[cid]
    if "name" in body:
        cat["name"] = str(body.get("name") or cat["name"]).strip()[:40]
    if "limit_value" in body:
        lv = safe_float(body.get("limit_value", 0))
        unit = str(body.get("limit_unit") or "GB").upper()
        cat["limit_bytes"] = 0 if lv <= 0 else parse_size_to_bytes(lv, unit)
    if "expires_days" in body:
        cat["expires_days"] = safe_int(body.get("expires_days", 0), minimum=0)
    if "connection_limit" in body:
        cat["connection_limit"] = safe_int(body.get("connection_limit", 0), minimum=0)
    if "speed_limit_value" in body:
        sv = safe_float(body.get("speed_limit_value", 0))
        cat["speed_limit_bytes"] = 0 if sv <= 0 else parse_speed_to_bytes(sv, "MBIT")
    if "ip_limit" in body:
        cat["ip_limit"] = safe_int(body.get("ip_limit", 0), minimum=0)
    if "clean_ips" in body:
        raw = body.get("clean_ips") or ""
        if isinstance(raw, list):
            cat["clean_ips"] = [str(x).strip() for x in raw if str(x).strip()]
        else:
            cat["clean_ips"] = [x.strip() for x in str(raw).replace(",", "\n").splitlines() if x.strip()]
    if "random_name" in body:
        cat["random_name"] = bool(body.get("random_name"))
    if "single_user" in body:
        cat["single_user"] = bool(body.get("single_user"))
    await save_state()
    return {"ok": True, **cat}

@app.delete("/api/categories/{cid}")
async def delete_category(cid: str, _=Depends(require_auth)):
    if cid not in CATEGORIES:
        raise HTTPException(status_code=404, detail="یافت نشد")
    del CATEGORIES[cid]
    for link in LINKS.values():
        if str(link.get("category_id")) == cid:
            link["category_id"] = "0"
    await save_state()
    return {"ok": True}

# ============================================================
# STATS
# ============================================================

@app.get("/stats")
async def get_stats(
    _=Depends(require_auth),
):

    async with LINKS_LOCK:
        snapshot = dict(LINKS)

    return {
        "service":
            APP_NAME,

        "version":
            APP_VERSION,

        "active_connections":
            len(connections),

        "total_traffic_mb":
            round(
                stats[
                    "total_bytes"
                ]
                / (
                    1024 ** 2
                ),
                2,
            ),

        "total_traffic_bytes":
            stats[
                "total_bytes"
            ],

        "download_bytes":
            stats.get("download_bytes", stats["total_bytes"]),

        "upload_bytes":
            stats.get("upload_bytes", 0),

        "hourly_upload":
            dict(hourly_upload),

        "total_requests":
            stats[
                "total_requests"
            ],

        "total_errors":
            stats[
                "total_errors"
            ],

        "uptime":
            uptime(),

        "timestamp":
            datetime.now().isoformat(),

        "hourly":
            dict(
                hourly_traffic
            ),

        "recent_errors":
            list(
                error_logs
            )[-10:],

        "links_count":
            len(snapshot),

        "active_links":
            sum(
                1
                for link
                in snapshot.values()
                if is_link_allowed(
                    link
                )
            ),

        "expired_links":
            sum(
                1
                for link
                in snapshot.values()
                if is_link_expired(
                    link
                )
            ),

        "subs_count":
            len(SUBS),
    }


@app.get("/api/activity")
async def get_activity(
    _=Depends(require_perm("logs")),
):

    return {
        "logs":
            list(
                activity_logs
            )[-250:]
    }


@app.delete("/api/activity")
async def clear_activity(
    _=Depends(require_perm("logs")),
):
    """Clear the in-memory activity history without touching any other panel state."""
    count = len(activity_logs)
    activity_logs.clear()
    return {"ok": True, "cleared": count}


# ============================================================
# CONNECTIONS
# ============================================================

@app.get("/api/connections")
async def get_connections(
    _=Depends(require_auth),
):

    async with LINKS_LOCK:
        snapshot = dict(LINKS)

    grouped = {}

    for connection in connections.values():

        ip = connection.get(
            "ip",
            "نامشخص",
        )

        link = snapshot.get(
            connection.get(
                "uuid"
            )
        )

        label = (
            link.get(
                "label"
            )
            if link
            else "نامشخص"
        )

        group = grouped.get(ip)

        if group is None:

            group = {
                "ip":
                    ip,

                "sessions":
                    0,

                "bytes":
                    0,

                "labels":
                    set(),

                "transports":
                    set(),

                "first_connected_at":
                    connection.get(
                        "connected_at"
                    ),

                "last_connected_at":
                    connection.get(
                        "connected_at"
                    ),
            }

            grouped[ip] = group

        group["sessions"] += 1

        group["bytes"] += int(
            connection.get(
                "bytes",
                0,
            )
            or 0
        )

        group["labels"].add(
            label
        )

        group["transports"].add(
            connection.get(
                "transport",
                DEFAULT_PROTOCOL,
            )
        )

    result = []

    for group in grouped.values():

        result.append(
            {
                "ip":
                    group["ip"],

                "sessions":
                    group["sessions"],

                "labels":
                    sorted(
                        group["labels"]
                    ),

                "label":
                    (
                        " · ".join(
                            sorted(
                                group["labels"]
                            )
                        )
                        if group["labels"]
                        else "نامشخص"
                    ),

                "transports":
                    sorted(
                        group["transports"]
                    ),

                "bytes":
                    group["bytes"],

                "bytes_fmt":
                    fmt_bytes(
                        group["bytes"]
                    ),

                "connected_at":
                    group[
                        "first_connected_at"
                    ],

                "last_connected_at":
                    group[
                        "last_connected_at"
                    ],
            }
        )

    result.sort(
        key=lambda item:
            item.get(
                "last_connected_at"
            )
            or "",
        reverse=True,
    )

    return {
        "connections":
            result,

        "count":
            len(result),

        "raw_count":
            len(connections),
    }


# ============================================================
# NATIVE PROTOCOL CORE (sing-box)
# ============================================================

try:
    from protocol_core import NativeCore
    NATIVE_CORE = NativeCore(DATA_DIR)
    # Only Trojan is exposed from the native runtime; the other four protocols
    # remain handled by the original panel relay modules.
    for _native_protocol in getattr(NATIVE_CORE, "SUPPORTED", ()):
        if _native_protocol not in PROTOCOLS:
            PROTOCOLS.append(_native_protocol)
    logger.info("Native sing-box backend loaded: %s", ", ".join(getattr(NATIVE_CORE, "SUPPORTED", ())))
except Exception as exc:
    NATIVE_CORE = None
    logger.warning("Native protocol backend unavailable: %s", exc)


async def sync_native_core():
    if not NATIVE_CORE:
        return False
    try:
        return await NATIVE_CORE.sync(LINKS, CONFIG.get("host") or os.getenv("RAILWAY_PUBLIC_DOMAIN", "localhost"))
    except Exception as exc:
        logger.warning("Native core sync failed: %s", exc)
        return False


@app.on_event("startup")
async def start_native_core():
    if NATIVE_CORE:
        asyncio.create_task(sync_native_core())


@app.get("/api/native/status")
async def api_native_status(request: Request, token=Depends(require_auth)):
    if not NATIVE_CORE:
        return {"ok": False, "installed": False, "running": False, "error": "Native core unavailable"}
    return {"ok": True, "installed": NATIVE_CORE.binary_exists(), **NATIVE_CORE.status()}


@app.get("/api/native/config")
async def api_native_config(request: Request, token=Depends(require_auth)):
    if not NATIVE_CORE:
        raise HTTPException(503, "Native core unavailable")
    config = NATIVE_CORE.current_config()
    raw = str(request.query_params.get("raw") or "").lower() in {"1", "true", "yes"}
    meta = get_session_meta(token)
    if raw and meta.get("role") != "owner":
        raise HTTPException(403, "raw native config is owner-only")
    if not raw:
        config = NATIVE_CORE._redact_config(config)
    return {"ok": True, "config": config, "redacted": not raw}


@app.post("/api/native/validate")
async def api_native_validate(request: Request, token=Depends(require_auth)):
    if not NATIVE_CORE:
        raise HTTPException(503, "Native core unavailable")
    body = await request.json()
    config = body.get("config") if isinstance(body, dict) else None
    if not isinstance(config, dict):
        raise HTTPException(400, "config must be an object")
    ok, detail = await NATIVE_CORE.validate_config(config)
    return {"ok": ok, "detail": detail}


@app.post("/api/native/reload")
async def api_native_reload(request: Request, token=Depends(require_auth)):
    if not NATIVE_CORE:
        raise HTTPException(503, "Native core unavailable")
    host = get_host(request)
    ok = await sync_native_core()
    if not ok:
        raise HTTPException(409, NATIVE_CORE.last_error or "Native reload failed")
    await save_state()
    return {"ok": True, "host": host, "status": NATIVE_CORE.status()}


@app.post("/api/links/{uid}/advanced/reset")
async def reset_link_advanced(uid: str, request: Request, token=Depends(require_auth)):
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if not link:
            raise HTTPException(404, "Link not found")
        previous = deepcopy(link)
        link["advanced"] = normalize_advanced_config(None)
        link["port"] = link["advanced"]["ports"][0]
        link["fingerprint"] = link["advanced"]["fingerprint"]["value"]
        link["alpn"] = link["advanced"]["tls"].get("alpn") or link.get("alpn") or ""
        snapshot = deepcopy(link)
    await save_state()
    if NATIVE_CORE and (snapshot.get("all_protocols") or snapshot.get("protocol") in getattr(NATIVE_CORE, "SUPPORTED", ())):
        if not await sync_native_core():
            async with LINKS_LOCK:
                LINKS[uid] = previous
            await save_state()
            raise HTTPException(409, NATIVE_CORE.last_error or "Native reset deployment failed; previous configuration restored")
    elif NATIVE_CORE:
        asyncio.create_task(sync_native_core())
    return {"ok": True, "link": snapshot}


# ============================================================
# OPTIONAL EXISTING PROJECT MODULES
# ============================================================

# ============================================================
# IMPORTANT:
# DO NOT REPLACE THIS VLESS CORE.
# ============================================================

try:

    from relay_vless import (
        RELAY_BUF,
        parse_vless_header,
        check_and_use,
        relay_ws_to_tcp,
        relay_tcp_to_ws,
        websocket_tunnel,
    )

    app.add_api_websocket_route(
        "/ws/{uuid}",
        websocket_tunnel,
    )

    if "vless-ws" not in PROTOCOLS:
        PROTOCOLS.append("vless-ws")

    logger.info(
        "VLESS relay loaded."
    )

except Exception as exc:

    logger.warning(
        "VLESS relay module unavailable: %s",
        exc,
    )


# ============================================================
# XHTTP
# ============================================================

try:

    from xhttp_siz10 import (
        router as xhttp_router
    )

    app.include_router(
        xhttp_router
    )

    for _protocol in (
        "xhttp-packet-up",
        "xhttp-stream-up",
        "xhttp-stream-one",
    ):
        if _protocol not in PROTOCOLS:
            PROTOCOLS.append(_protocol)

    logger.info(
        "XHTTP module loaded."
    )

except Exception as exc:

    logger.warning(
        "XHTTP module unavailable: %s",
        exc,
    )

# Expose exactly the five protocols implemented by this ONEX2 project.
_PROTOCOL_ORDER = [
    "vless-ws",
    "xhttp-packet-up",
    "xhttp-stream-up",
    "xhttp-stream-one",
    "trojan",
]
PROTOCOLS[:] = [p for p in _PROTOCOL_ORDER if p in PROTOCOLS]


# ============================================================

@app.get("/api/me")
async def api_me_info(request: Request, token=Depends(require_auth)):
    meta = get_session_meta(token)
    return {
        "ok": True,
        "role": meta.get("role"),
        "username": meta.get("username"),
        "permissions": meta.get("permissions") or {p: True for p in ALL_PERMS},
        "uptime": uptime(),
    }


@app.get("/api/admins")
async def api_admins_list(token=Depends(require_perm("admins"))):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک پنل")
    out = []
    for aid, a in ADMIN_ACCOUNTS.items():
        out.append({
            "id": aid,
            "username": a.get("username"),
            "label": a.get("label"),
            "limit_bytes": int(a.get("limit_bytes") or 0),
            "used_bytes": int(a.get("used_bytes") or 0),
            "expires_at": a.get("expires_at"),
            "active": bool(a.get("active", True)),
            "blocked": bool(a.get("blocked")),
            "permissions": a.get("permissions") or {},
            "created_at": a.get("created_at"),
            "valid": admin_is_valid(a),
        })
    out.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return {"admins": out}


@app.post("/api/admins")
async def api_admins_create(request: Request, token=Depends(require_perm("admins"))):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک پنل")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="JSON نامعتبر")
    username = str(body.get("username") or "").strip().lower()
    password = str(body.get("password") or "")
    repeat = str(body.get("repeat_password") or body.get("confirm") or "")
    if not username or len(username) < 3:
        raise HTTPException(400, detail="نام کاربری حداقل ۳ کاراکتر")
    if not username.isalnum():
        raise HTTPException(400, detail="نام کاربری فقط حروف و عدد انگلیسی")
    if username in ("owner", "admin", "root"):
        raise HTTPException(400, detail="این نام کاربری رزرو شده است")
    if find_admin_by_username(username)[0]:
        raise HTTPException(400, detail="نام کاربری تکراری است")
    if len(password) < 6:
        raise HTTPException(400, detail="رمز حداقل ۶ کاراکتر")
    if password != repeat:
        raise HTTPException(400, detail="تکرار رمز یکسان نیست")
    limit_value = safe_float(body.get("limit_value", 0))
    limit_unit = str(body.get("limit_unit") or "GB").upper()
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    days = safe_int(body.get("expires_days", 0), minimum=0)
    expires_at = (datetime.now() + timedelta(days=days)).isoformat() if days > 0 else None
    perms_in = body.get("permissions") or {}
    permissions = {p: bool(perms_in.get(p, False)) for p in ALL_PERMS}
    rec = default_admin_record(username, password, limit_bytes=limit_bytes, expires_at=expires_at, permissions=permissions, label=body.get("label") or username)
    if "active" in body:
        rec["active"] = bool(body.get("active"))
    ADMIN_ACCOUNTS[rec["id"]] = rec
    await save_state()
    log_activity("admin", f"اکانت ادمین «{username}» ساخته شد", "ok")
    return {"ok": True, "id": rec["id"], "username": username}


@app.patch("/api/admins/{aid}")
async def api_admins_patch(aid: str, request: Request, token=Depends(require_perm("admins"))):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک پنل")
    if aid not in ADMIN_ACCOUNTS:
        raise HTTPException(404, detail="یافت نشد")
    body = await request.json()
    a = ADMIN_ACCOUNTS[aid]
    if "blocked" in body:
        a["blocked"] = bool(body["blocked"])
    if "active" in body:
        a["active"] = bool(body["active"])
    if "label" in body:
        a["label"] = str(body["label"])[:40]
    if "permissions" in body and isinstance(body["permissions"], dict):
        a["permissions"] = {p: bool(body["permissions"].get(p, False)) for p in ALL_PERMS}
    if "limit_value" in body:
        lv = safe_float(body.get("limit_value", 0))
        lu = str(body.get("limit_unit") or "GB").upper()
        a["limit_bytes"] = 0 if lv <= 0 else parse_size_to_bytes(lv, lu)
    if "expires_days" in body:
        days = safe_int(body.get("expires_days", 0), minimum=0)
        a["expires_at"] = (datetime.now() + timedelta(days=days)).isoformat() if days > 0 else None
    if body.get("password"):
        pw = str(body["password"])
        if len(pw) < 6:
            raise HTTPException(400, detail="رمز حداقل ۶ کاراکتر")
        a["password_hash"] = hash_password(pw)
    await save_state()
    log_activity("admin", f"اکانت ادمین «{a.get('username')}» ویرایش شد", "ok")
    return {"ok": True}


@app.delete("/api/admins/{aid}")
async def api_admins_delete(aid: str, token=Depends(require_perm("admins"))):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک پنل")
    a = ADMIN_ACCOUNTS.pop(aid, None)
    if not a:
        raise HTTPException(404, detail="یافت نشد")
    await save_state()
    log_activity("admin", f"اکانت ادمین «{a.get('username')}» حذف شد", "warn")
    return {"ok": True}


NEWS_FILE = Path(__file__).resolve().parent / "news.json"


def _version_tuple(value):
    """Return a comparable numeric version tuple such as (1, 2, 3)."""
    raw = str(value or "0").strip().lstrip("vV")
    parts = raw.split(".")
    out = []
    for part in parts[:8]:
        digits = "".join(ch for ch in part if ch.isdigit())
        out.append(int(digits or "0"))
    while len(out) < 3:
        out.append(0)
    return tuple(out)


def _is_newer_version(remote, local):
    return _version_tuple(remote) > _version_tuple(local)


async def fetch_update_info():
    """Read public release metadata and the latest GitHub commit."""
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "ONEX-Panel-Updater",
    }
    timeout = httpx.Timeout(10.0, connect=5.0)
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        version_resp = await client.get(UPDATE_VERSION_URL, headers=headers)
        version_resp.raise_for_status()
        meta = version_resp.json()
        if not isinstance(meta, dict):
            raise ValueError("version.json must contain a JSON object")

        commit_resp = await client.get(
            f"{UPDATE_GITHUB_API}/commits/{quote(UPDATE_BRANCH, safe='')}",
            headers=headers,
        )
        commit_resp.raise_for_status()
        commit_data = commit_resp.json()
        sha = str(commit_data.get("sha") or "").strip()
        return {
            "version": str(meta.get("version") or "").strip(),
            "title": str(meta.get("title") or "").strip(),
            "message": str(meta.get("message") or "").strip(),
            "changelog": meta.get("changelog") if isinstance(meta.get("changelog"), list) else [],
            "published_at": str(meta.get("published_at") or "").strip(),
            "commit_sha": sha,
            "repo": UPDATE_REPO,
            "branch": UPDATE_BRANCH,
            "release_url": str(meta.get("release_url") or f"https://github.com/{UPDATE_REPO}/commits/{UPDATE_BRANCH}").strip(),
        }


@app.get("/api/update/check")
async def api_update_check(token=Depends(require_auth)):
    try:
        remote = await fetch_update_info()
        remote_version = remote.get("version") or APP_VERSION
        version_newer = _is_newer_version(remote_version, APP_VERSION)
        commit_changed = bool(
            remote.get("commit_sha")
            and ONEX_CURRENT_COMMIT_SHA
            and remote.get("commit_sha") != ONEX_CURRENT_COMMIT_SHA
        )
        # Version metadata remains the primary signal.  A commit change is a
        # secondary signal for deployments where version.json was not bumped.
        update_available = version_newer or commit_changed
        return {
            "ok": True,
            "current_version": APP_VERSION,
            "latest_version": remote_version,
            "update_available": update_available,
            "version_changed": version_newer,
            "commit_changed": commit_changed,
            "current_commit": ONEX_CURRENT_COMMIT_SHA or None,
            "latest_commit": remote.get("commit_sha"),
            "title": remote.get("title", ""),
            "message": remote.get("message", ""),
            "changelog": remote.get("changelog", []),
            "published_at": remote.get("published_at", ""),
            "release_url": remote.get("release_url", ""),
            "configured": bool(RAILWAY_API_TOKEN and RAILWAY_SERVICE_ID and RAILWAY_ENVIRONMENT_ID),
        }
    except Exception as exc:
        logger.warning("Update check failed: %s", exc)
        return {
            "ok": False,
            "current_version": APP_VERSION,
            "latest_version": None,
            "update_available": False,
            "message": "بررسی نسخه جدید انجام نشد",
            "error": str(exc),
        }


@app.post("/api/update/deploy")
async def api_update_deploy(token=Depends(require_auth)):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک پنل می‌تواند پنل را بروزرسانی کند")

    if not (RAILWAY_API_TOKEN and RAILWAY_SERVICE_ID and RAILWAY_ENVIRONMENT_ID):
        raise HTTPException(503, detail="تنظیمات اتصال امن Railway برای بروزرسانی کامل نشده است")

    try:
        remote = await fetch_update_info()
        remote_version = remote.get("version") or APP_VERSION
        version_newer = _is_newer_version(remote_version, APP_VERSION)
        commit_changed = bool(remote.get("commit_sha") and ONEX_CURRENT_COMMIT_SHA and remote.get("commit_sha") != ONEX_CURRENT_COMMIT_SHA)
        if not (version_newer or commit_changed):
            return {
                "ok": True,
                "update_available": False,
                "message": "پنل شما آخرین نسخه را دارد",
                "current_version": APP_VERSION,
                "latest_version": remote_version,
                "latest_commit": remote.get("commit_sha"),
            }

        commit_sha = remote.get("commit_sha")
        if not commit_sha:
            raise RuntimeError("GitHub commit SHA not found")

        mutation = """
        mutation ServiceInstanceDeployV2($serviceId: String!, $environmentId: String!, $commitSha: String) {
          serviceInstanceDeployV2(serviceId: $serviceId, environmentId: $environmentId, commitSha: $commitSha)
        }
        """
        payload = {
            "query": mutation,
            "variables": {
                "serviceId": RAILWAY_SERVICE_ID,
                "environmentId": RAILWAY_ENVIRONMENT_ID,
                "commitSha": commit_sha,
            },
        }
        async with httpx.AsyncClient(timeout=httpx.Timeout(20.0, connect=8.0)) as client:
            response = await client.post(
                RAILWAY_API_URL,
                json=payload,
                headers={
                    "Authorization": f"Bearer {RAILWAY_API_TOKEN}",
                    "Content-Type": "application/json",
                    "User-Agent": "ONEX-Panel-Updater",
                },
            )
            response.raise_for_status()
            data = response.json()

        if data.get("errors"):
            raise RuntimeError(str(data["errors"]))
        deployment_id = ((data.get("data") or {}).get("serviceInstanceDeployV2") or "").strip()
        if not deployment_id:
            raise RuntimeError("Railway did not return a deployment id")

        log_activity("system", f"بروزرسانی پنل به نسخه {remote_version} شروع شد", "ok")
        return {
            "ok": True,
            "update_started": True,
            "current_version": APP_VERSION,
            "latest_version": remote_version,
            "deployment_id": deployment_id,
            "message": "بروزرسانی شروع شد؛ پنل پس از استقرار نسخه جدید دوباره در دسترس قرار می‌گیرد.",
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Panel update deployment failed")
        raise HTTPException(502, detail=f"شروع بروزرسانی ناموفق بود: {exc}")


@app.get("/api/news")
async def api_news(token=Depends(require_auth)):
    try:
        if NEWS_FILE.exists():
            data = json.loads(NEWS_FILE.read_text(encoding="utf-8"))
        else:
            data = {"enabled": False, "title": "", "message": "", "updated_at": ""}
        return {"ok": True, **data}
    except Exception as e:
        return {"ok": False, "enabled": False, "title": "", "message": str(e), "updated_at": ""}




# ============================================================
# BACKUP / RESTORE
# ============================================================


@app.get("/api/security/status")
async def security_status(token=Depends(require_auth)):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک")
    now = time.time()
    locked = []
    for ip, until in list(LOGIN_LOCKED_UNTIL.items()):
        if until > now:
            locked.append({"ip": ip, "remaining_sec": int(until - now)})
    return {
        "ok": True,
        "max_attempts": LOGIN_MAX_ATTEMPTS,
        "window_seconds": LOGIN_WINDOW_SECONDS,
        "lockout_seconds": LOGIN_LOCKOUT_SECONDS,
        "locked_ips": locked,
        "tracked_ips": len(LOGIN_FAILURES),
    }


@app.post("/api/security/unlock")
async def security_unlock(request: Request, token=Depends(require_auth)):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک")
    try:
        body = await request.json()
    except Exception:
        body = {}
    ip = str((body or {}).get("ip") or "").strip()
    if ip:
        LOGIN_FAILURES.pop(ip, None)
        LOGIN_LOCKED_UNTIL.pop(ip, None)
    else:
        LOGIN_FAILURES.clear()
        LOGIN_LOCKED_UNTIL.clear()
    log_activity("auth", f"رفع مسدودی brute-force ({ip or 'all'})", "ok")
    return {"ok": True}


@app.get("/api/backup/users")
async def backup_users(token=Depends(require_auth)):
    meta = get_session_meta(token)
    # owner always; admin needs settings perm
    if meta.get("role") != "owner":
        if not (meta.get("permissions") or {}).get("settings"):
            raise HTTPException(403, detail="دسترسی ندارید")
    payload = {
        "type": "pxpanel_users_backup",
        "version": APP_VERSION,
        "created_at": datetime.now().isoformat(),
        "links": dict(LINKS),
        "subs": dict(SUBS),
        "categories": dict(CATEGORIES),
        "admin_accounts": dict(ADMIN_ACCOUNTS),
    }
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    return Response(
        content=body,
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="pxpanel-users-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json"'
        },
    )


@app.get("/api/backup/bot")
async def backup_bot(token=Depends(require_auth)):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        if not (meta.get("permissions") or {}).get("settings"):
            raise HTTPException(403, detail="دسترسی ندارید")
    data = {}
    try:
        if TG_FILE.exists():
            data = json.loads(TG_FILE.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    payload = {
        "type": "pxpanel_bot_backup",
        "version": APP_VERSION,
        "created_at": datetime.now().isoformat(),
        "telegram": data,
    }
    body = json.dumps(payload, ensure_ascii=False, indent=2)
    return Response(
        content=body,
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="pxpanel-bot-{datetime.now().strftime("%Y%m%d-%H%M%S")}.json"'
        },
    )


@app.post("/api/restore/users")
async def restore_users(request: Request, token=Depends(require_auth)):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک پنل")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="فایل JSON نامعتبر")
    if not isinstance(body, dict):
        raise HTTPException(400, detail="فرمت نامعتبر")
    # accept either wrapper or raw state
    links = body.get("links")
    if links is None and body.get("type") == "pxpanel_users_backup":
        raise HTTPException(400, detail="لینک‌ها در بک‌آپ نیست")
    if links is None:
        raise HTTPException(400, detail="فایل بک‌آپ کاربران نیست")
    if not isinstance(links, dict):
        raise HTTPException(400, detail="links نامعتبر")
    mode = str(body.get("mode") or "merge").lower()  # merge | replace
    async with LINKS_LOCK:
        if mode == "replace":
            LINKS.clear()
            SUBS.clear()
            CATEGORIES.clear()
            ADMIN_ACCOUNTS.clear()
        LINKS.update(links)
        if isinstance(body.get("subs"), dict):
            SUBS.update(body["subs"])
        if isinstance(body.get("categories"), dict):
            CATEGORIES.update(body["categories"])
        if isinstance(body.get("admin_accounts"), dict):
            ADMIN_ACCOUNTS.update(body["admin_accounts"])
        for uid, link in list(LINKS.items()):
            if not isinstance(link, dict):
                LINKS.pop(uid, None)
                continue
            link.setdefault("protocol", DEFAULT_PROTOCOL)
            link.setdefault("fingerprint", DEFAULT_FINGERPRINT)
            link.setdefault("used_bytes", 0)
            link.setdefault("active", True)
            link.setdefault("config_count", 1)
    await save_state()
    log_activity("backup", f"بازیابی کاربران ({mode}) — {len(links)} کانفیگ", "ok")
    return {"ok": True, "links": len(LINKS), "subs": len(SUBS), "mode": mode}


@app.post("/api/restore/bot")
async def restore_bot(request: Request, token=Depends(require_auth)):
    meta = get_session_meta(token)
    if meta.get("role") != "owner":
        raise HTTPException(403, detail="فقط مالک پنل")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="فایل JSON نامعتبر")
    tg = body.get("telegram") if isinstance(body, dict) else None
    if tg is None and isinstance(body, dict) and (body.get("token") or body.get("admin_ids") is not None):
        tg = body
    if not isinstance(tg, dict):
        raise HTTPException(400, detail="فایل بک‌آپ ربات نیست")
    # merge with existing
    current = {}
    try:
        if TG_FILE.exists():
            current = json.loads(TG_FILE.read_text(encoding="utf-8"))
    except Exception:
        current = {}
    current.update({k: v for k, v in tg.items() if v is not None})
    TG_FILE.parent.mkdir(parents=True, exist_ok=True)
    TG_FILE.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
    # try activate
    try:
        from telegram_bot import configure_bot, start_bot, stop_bot, setup_webhook
        await stop_bot()
        configure_bot(current.get("token") or "", current.get("admin_ids") or "")
        host = get_host(request)
        if current.get("webhook") and host and host != "localhost":
            wh = f"https://{host}/telegram/webhook"
            await setup_webhook(wh)
            await start_bot(mode="webhook")
        else:
            await setup_webhook("")
            await start_bot(mode="polling")
    except Exception as exc:
        logger.warning("restore bot activate: %s", exc)
        log_activity("backup", f"بک‌آپ ربات ذخیره شد (فعال‌سازی: {exc})", "warn")
        return {"ok": True, "warning": str(exc)}
    log_activity("backup", "بازیابی تنظیمات ربات انجام شد", "ok")
    return {"ok": True, "message": "ربات بازیابی و فعال شد"}



# TELEGRAM DASHBOARD / MANAGEMENT API
# ============================================================

@app.get("/api/telegram/dashboard")
async def api_tg_dashboard(_=Depends(require_auth)):
    try:
        from telegram_bot import get_dashboard_snapshot
        return {"ok": True, **(await get_dashboard_snapshot())}
    except Exception as exc:
        logger.warning("telegram dashboard snapshot failed: %s", exc)
        s = load_tg_settings()
        return {"ok": True, "enabled": bool(s.get("enabled")), "users": [], "user_count": 0, "active_today": 0, "messages_today": 0, "webhook": bool(s.get("webhook")), "bot": {}, "error": str(exc)}

@app.get("/api/telegram/users")
async def api_tg_users(_=Depends(require_auth)):
    try:
        from telegram_bot import list_bot_users
        return {"ok": True, "users": list_bot_users()}
    except Exception as exc:
        return {"ok": False, "users": [], "error": str(exc)}

@app.post("/api/telegram/users/{user_id}/block")
async def api_tg_user_block(user_id: int, request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        body = {}
    try:
        from telegram_bot import set_bot_user_blocked
        ok = set_bot_user_blocked(user_id, bool(body.get("blocked", True)))
        return {"ok": bool(ok)}
    except Exception as exc:
        raise HTTPException(400, detail=str(exc))

@app.post("/api/telegram/broadcast")
async def api_tg_broadcast(request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="invalid json")
    text = str(body.get("message") or "").strip()
    audience = str(body.get("audience") or "all").strip().lower()
    if not text:
        raise HTTPException(400, detail="message is required")
    if len(text) > 4096:
        raise HTTPException(400, detail="message is too long")
    try:
        from telegram_bot import broadcast_message
        result = await broadcast_message(text, audience)
        log_activity("telegram", f"پیام همگانی ارسال شد: {result.get('sent', 0)} موفق", "ok")
        return {"ok": True, **result}
    except Exception as exc:
        raise HTTPException(502, detail=str(exc))

@app.post("/api/telegram/test")
async def api_tg_test(_=Depends(require_auth)):
    try:
        from telegram_bot import telegram_health
        return {"ok": True, **(await telegram_health())}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}

# TELEGRAM SETTINGS API
# ============================================================

def load_tg_settings():
    try:
        if TG_FILE.exists():
            return json.loads(TG_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {
        "token": os.environ.get("TELEGRAM_BOT_TOKEN", "").strip(),
        "admin_ids": os.environ.get("TELEGRAM_ADMIN_IDS", "").strip(),
        "webhook": False,
        "enabled": False,
    }


def save_tg_settings(data: dict):
    TG_FILE.parent.mkdir(parents=True, exist_ok=True)
    TG_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


@app.get("/api/telegram/settings")
async def api_tg_get(_=Depends(require_auth)):
    s = load_tg_settings()
    token = s.get("token") or ""
    masked = (token[:8] + "…" + token[-4:]) if len(token) > 14 else ("••••" if token else "")
    return {
        "token_masked": masked,
        "has_token": bool(token),
        "admin_ids": s.get("admin_ids") or "",
        "webhook": bool(s.get("webhook")),
        "enabled": bool(s.get("enabled")),
    }


@app.post("/api/telegram/settings")
async def api_tg_save(request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="invalid json")
    s = load_tg_settings()
    token = str(body.get("token") or "").strip()
    admin_ids = str(body.get("admin_ids") or "").strip()
    use_webhook = bool(body.get("webhook", True))
    if token:
        s["token"] = token
    if admin_ids is not None:
        s["admin_ids"] = admin_ids
    s["webhook"] = use_webhook
    s["enabled"] = True
    save_tg_settings(s)
    # apply runtime
    try:
        from telegram_bot import configure_bot, start_bot, stop_bot, setup_webhook
        await stop_bot()
        configure_bot(s.get("token") or "", s.get("admin_ids") or "")
        host = get_host(request)
        if use_webhook and host and host != "localhost":
            wh = f"https://{host}/telegram/webhook"
            ok = await setup_webhook(wh)
            s["webhook_url"] = wh
            s["webhook_ok"] = bool(ok)
            save_tg_settings(s)
            await start_bot(mode="webhook")
        else:
            await setup_webhook("")  # delete webhook -> polling
            await start_bot(mode="polling")
        log_activity("telegram", "ربات تلگرام پیکربندی و فعال شد", "ok")
        return {"ok": True, "webhook": use_webhook, "message": "ربات فعال شد"}
    except Exception as exc:
        logger.warning("telegram activate error: %s", exc)
        return {"ok": True, "warning": str(exc), "message": "تنظیمات ذخیره شد"}


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    try:
        from telegram_bot import process_update
        data = await request.json()
        await process_update(data)
    except Exception as exc:
        logger.warning("webhook error: %s", exc)
    return {"ok": True}


# ============================================================
# TELEGRAM
# ============================================================

try:

    from telegram_bot import (
        start_bot as _tg_start_bot,
        stop_bot as _tg_stop_bot,
    )

except Exception:

    async def _tg_start_bot():
        return None

    async def _tg_stop_bot():
        return None


@app.on_event("startup")
async def start_optional_telegram():

    try:

        await _tg_start_bot()

        logger.info(
            "Telegram module initialized."
        )

    except Exception as exc:

        logger.warning(
            "Telegram bot disabled/error: %s",
            exc,
        )


# ============================================================
# HTTP PROXY
# ============================================================

_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-encoding",
    "content-length",
}


@app.api_route(
    "/proxy/{target_url:path}",
    methods=[
        "GET",
        "POST",
        "PUT",
        "DELETE",
        "PATCH",
        "HEAD",
        "OPTIONS",
    ],
)
async def http_proxy(
    target_url: str,
    request: Request,
):

    if not target_url.startswith("http"):
        target_url = (
            "https://"
            + target_url
        )

    if http_client is None:
        raise HTTPException(
            status_code=503,
            detail="HTTP client not ready",
        )

    try:

        body = await request.body()

        headers = {
            key: value
            for key, value
            in request.headers.items()
            if (
                key.lower()
                not in _HOP
            )
            and (
                key.lower()
                != "host"
            )
        }

        request_size = len(body or b"")
        stats["upload_bytes"] = stats.get("upload_bytes", 0) + request_size
        hour_key = now_ir().strftime("%H:00")
        hourly_upload[hour_key] += request_size

        response = await http_client.request(
            method=request.method,
            url=target_url,
            headers=headers,
            content=body,
        )

        response_size = len(response.content)
        stats["total_bytes"] += response_size
        stats["download_bytes"] = stats.get("download_bytes", 0) + response_size

        stats["total_requests"] += 1

        hourly_traffic[hour_key] += response_size

        output_headers = {
            key: value
            for key, value
            in response.headers.items()
            if key.lower() not in _HOP
        }

        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=output_headers,
        )

    except Exception as exc:

        stats["total_errors"] += 1

        error_logs.append(
            {
                "error":
                    str(exc),

                "url":
                    target_url,

                "time":
                    datetime.now().isoformat(),
            }
        )

        logger.exception(
            "Proxy error: %s",
            target_url,
        )

        raise HTTPException(
            status_code=502,
            detail=(
                "Proxy error: "
                f"{exc}"
            ),
        )


# ============================================================
# DASHBOARD
# ============================================================

DASHBOARD_HTML = '\n<!DOCTYPE html>\n<html lang="fa" dir="rtl" id="htmlRoot">\n<head>\n<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">\n<title>پنل مدیریت</title>\n<link rel="preconnect" href="https://fonts.googleapis.com">\n<link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@400;500;600;700;800&family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">\n<style>\n/* ---------- Delete-all configs glass action ---------- */\n.delete-all-configs-glass{\n  width:100%;display:flex;align-items:center;gap:14px;text-align:right;direction:rtl;\n  margin:0 0 14px;padding:15px 17px;border-radius:20px;cursor:pointer;\n  color:#ff6687;background:linear-gradient(135deg,rgba(72,8,29,.56),rgba(18,7,20,.62));\n  border:1px solid rgba(255,55,101,.42);\n  box-shadow:0 12px 38px rgba(255,31,92,.10),inset 0 1px rgba(255,255,255,.07),inset 0 0 28px rgba(255,31,92,.045);\n  backdrop-filter:blur(18px) saturate(125%);-webkit-backdrop-filter:blur(18px) saturate(125%);\n  transition:transform .2s ease,border-color .2s ease,box-shadow .2s ease,background .2s ease;\n}\n.delete-all-configs-glass:hover{transform:translateY(-1px);border-color:rgba(255,65,111,.72);box-shadow:0 16px 46px rgba(255,31,92,.17),inset 0 1px rgba(255,255,255,.08),inset 0 0 34px rgba(255,31,92,.08)}\n.delete-all-icon{width:44px;height:44px;flex:0 0 44px;display:grid;place-items:center;border-radius:14px;color:#ff426e;background:rgba(255,48,94,.10);border:1px solid rgba(255,65,111,.30);box-shadow:0 0 20px rgba(255,40,90,.13)}\n.delete-all-icon svg{width:21px;height:21px}\n.delete-all-copy{display:flex;flex-direction:column;gap:2px;min-width:0}\n.delete-all-copy b{font-size:14px;font-weight:900;color:#ff6687}\n.delete-all-copy small{font-size:10px;color:rgba(255,198,210,.62)}\n.delete-all-arrow{margin-right:auto;font-size:26px;line-height:1;color:#ff6d8d;transform:translateY(-1px)}\n.delete-all-modal{position:relative;width:min(510px,100%);padding:30px 26px 24px;border-radius:28px;text-align:center;background:linear-gradient(145deg,rgba(8,21,43,.90),rgba(3,9,20,.92));border:1px solid rgba(59,166,255,.35);box-shadow:0 30px 90px rgba(0,0,0,.62),0 0 45px rgba(30,128,255,.09),inset 0 1px rgba(255,255,255,.08);backdrop-filter:blur(24px) saturate(130%);-webkit-backdrop-filter:blur(24px) saturate(130%);overflow:hidden}\n.delete-all-modal:before{content:"";position:absolute;inset:-35%;background:radial-gradient(circle at 50% 0%,rgba(255,38,92,.10),transparent 38%);pointer-events:none}\n.delete-all-modal-close{position:absolute;top:12px;left:14px;width:34px;height:34px;border:0;border-radius:10px;background:rgba(255,255,255,.04);color:#9fb6d3;font-size:26px;line-height:1;cursor:pointer}\n.delete-all-modal-icon{position:relative;z-index:1;width:76px;height:76px;margin:2px auto 15px;display:grid;place-items:center;border-radius:50%;color:#ff4d76;border:1px solid rgba(255,64,105,.70);background:rgba(255,39,91,.08);box-shadow:0 0 28px rgba(255,35,91,.25),inset 0 0 24px rgba(255,35,91,.08)}\n.delete-all-modal-icon svg{width:32px;height:32px}\n.delete-all-modal-title{position:relative;z-index:1;font-size:21px;font-weight:950;color:#f8fbff;margin-bottom:10px}\n.delete-all-modal-text{position:relative;z-index:1;color:#a9bdd5;font-size:13px;line-height:2;margin-bottom:20px}.delete-all-modal-text b{color:#ff879f;font-weight:800}\n.delete-all-modal-actions{position:relative;z-index:1;display:grid;grid-template-columns:1fr 1fr;gap:10px}\n.delete-all-cancel,.delete-all-confirm{min-height:46px;border-radius:14px;font-family:inherit;font-weight:900;cursor:pointer}\n.delete-all-cancel{background:rgba(12,31,55,.70);border:1px solid rgba(59,166,255,.45);color:#dcecff}\n.delete-all-confirm{display:flex;align-items:center;justify-content:center;gap:7px;background:linear-gradient(135deg,#ff315d,#d91f55);border:1px solid rgba(255,104,133,.75);color:white;box-shadow:0 10px 28px rgba(255,31,92,.22)}\n.delete-all-confirm svg{width:17px;height:17px}\n.delete-all-confirm:disabled{opacity:.65;cursor:wait}\n@media(max-width:600px){.delete-all-configs-glass{border-radius:17px;padding:13px 14px}.delete-all-modal{padding:27px 17px 18px;border-radius:22px}.delete-all-modal-title{font-size:18px}.delete-all-modal-text{font-size:12px}.delete-all-modal-actions{grid-template-columns:1fr 1fr}}\n\n\n:root{\n  --bg:#06060b;--bg2:#0b0b12;--bg3:#12121c;--card:rgba(18,18,28,.92);--card-b:rgba(255,255,255,.08);\n  --accent:#3b82f6;--accent2:#60a5fa;--purple:#8b5cf6;--green:#22c55e;--red:#ef4444;--amber:#f59e0b;\n  --t1:#f8fafc;--t2:rgba(248,250,252,.72);--t3:rgba(248,250,252,.42);\n  --sb:252px;--sb-c:74px;--radius:18px;--shadow:0 12px 40px rgba(0,0,0,.45);\n  --input-bg:rgba(0,0,0,.4);--hover:rgba(var(--accent-rgb),.12);\n  --glow:0 0 40px rgba(var(--accent-rgb),.12);--glass:blur(16px);\n  --accent-rgb:59,130,246;--accent2-rgb:96,165,250;--purple-rgb:139,92,246;--glass-rgb:9,22,43;--glass-deep-rgb:2,9,20;\n}\nhtml.light{\n  --bg:#eef1f8;--bg2:#ffffff;--bg3:#f1f4fa;--card:#ffffff;--card-b:color-mix(in srgb, rgb(23 23 23 / .09) 92%, var(--accent));\n  --accent:#2563eb;--accent2:#3b82f6;--purple:#7c3aed;--green:#16a34a;--red:#dc2626;--amber:#d97706;\n  --t1:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent));--t2:color-mix(in srgb, rgb(83 83 83) 82%, var(--accent));--t3:color-mix(in srgb, rgb(161 161 161) 82%, var(--accent));\n  --shadow:0 10px 32px color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent));\n  --input-bg:#f8fafc;--hover:rgba(var(--accent-rgb),.08);\n  --glow:0 0 32px rgba(var(--accent-rgb),.08);--glass:blur(12px);\n}\n*{box-sizing:border-box;margin:0;padding:0}\nhtml,body{min-height:100%}\nbody{font-family:\'Vazirmatn\',sans-serif;background:var(--bg);color:var(--t1);display:flex;min-height:100vh;overflow-x:hidden;transition:background .3s,color .3s}\nbody::before{content:\'\';position:fixed;inset:0;pointer-events:none;z-index:0;\n  background:\n    radial-gradient(ellipse 80% 50% at 100% 0%, rgba(var(--accent-rgb),.14), transparent 50%),\n    radial-gradient(ellipse 60% 40% at 0% 100%, rgba(var(--purple-rgb),.10), transparent 45%);\n}\nhtml.light body::before{\n  background:\n    radial-gradient(ellipse 80% 50% at 100% 0%, rgba(var(--accent-rgb),.08), transparent 50%),\n    radial-gradient(ellipse 60% 40% at 0% 100%, rgba(var(--purple-rgb),.06), transparent 45%);\n}\n.sidebar,.main,.mob-bar,.modal-bg,.toast{position:relative;z-index:1}\n.sidebar{z-index:300}.mob-bar{z-index:250}.modal-bg{z-index:500}.toast{z-index:999}\nbody.en{font-family:\'Inter\',system-ui,sans-serif}\n::-webkit-scrollbar{width:5px;height:5px}\n::-webkit-scrollbar-thumb{background:var(--t3);border-radius:99px}\n\n.sidebar{position:fixed;right:0;top:0;bottom:0;width:var(--sb);background:var(--bg2);border-left:1px solid var(--card-b);display:flex;flex-direction:column;z-index:300;transition:width .28s cubic-bezier(.4,0,.2,1),transform .28s,background .3s;box-shadow:var(--shadow);backdrop-filter:var(--glass)}\n.sidebar.collapsed{width:var(--sb-c)}\n.sb-toggle{position:absolute;left:-15px;top:50%;transform:translateY(-50%);width:30px;height:30px;border-radius:8px;background:var(--accent);border:2px solid var(--bg);color:#fff;display:flex;align-items:center;justify-content:center;cursor:pointer;z-index:310;box-shadow:0 4px 14px rgba(var(--accent-rgb),.4);transition:.2s}\n.sb-toggle:hover{filter:brightness(1.1);transform:translateY(-50%) scale(1.05)}\n.sb-toggle svg{width:14px;height:14px;transition:transform .28s}\n.sidebar.collapsed .sb-toggle svg{transform:rotate(180deg)}\n.sb-logo{display:flex;align-items:center;justify-content:center;padding:18px 14px;border-bottom:1px solid var(--card-b)}\n.sb-logo-icon{position:relative;width:54px;height:54px;border-radius:16px;display:grid;place-items:center;flex-shrink:0;font-size:0;font-weight:900;color:#fff;isolation:isolate;transform:perspective(260px) rotateX(7deg) rotateY(-8deg);background:linear-gradient(145deg,var(--accent) 0%,var(--accent) 48%,var(--purple) 100%);border:1px solid rgba(255,255,255,.22);box-shadow:0 16px 30px rgba(var(--accent-rgb),.35),inset 0 1px rgba(255,255,255,.32);animation:onexLogoFloat 3.2s ease-in-out infinite}\n.sb-logo-icon:before{content:\'\';position:absolute;inset:5px;border-radius:12px;background:linear-gradient(145deg,rgba(255,255,255,.28),rgba(255,255,255,.03) 45%,rgba(0,0,0,.18));border:1px solid rgba(255,255,255,.16);box-shadow:inset 0 -8px 16px rgba(0,0,0,.14),0 0 22px rgba(var(--accent2-rgb),.18);z-index:-1}\n.sb-logo-icon:after{content:\'N\';position:absolute;inset:0;display:grid;place-items:center;font:900 25px/1 Inter,system-ui,sans-serif;color:#fff;letter-spacing:-.08em;text-shadow:3px 3px 0 rgba(var(--accent-rgb),.95),6px 6px 0 color-mix(in srgb, rgb(40 40 40 / .55) 88%, var(--accent)),0 0 18px rgba(255,255,255,.38);transform:translateZ(18px);animation:onexLogoGlow 2.8s ease-in-out infinite}\n.sb-logo-text,.sb-logo-name,.sb-logo-ver{display:none!important}\n.sidebar.collapsed .sb-logo{justify-content:center;padding:14px 8px}\n.sidebar.collapsed .sb-logo-icon{margin:0 auto}\n@keyframes onexLogoFloat{0%,100%{transform:perspective(260px) rotateX(7deg) rotateY(-8deg) translateY(0)}50%{transform:perspective(260px) rotateX(10deg) rotateY(-13deg) translateY(-4px)}}\n@keyframes onexLogoGlow{0%,100%{filter:brightness(1);text-shadow:3px 3px 0 rgba(var(--accent-rgb),.95),6px 6px 0 color-mix(in srgb, rgb(40 40 40 / .55) 88%, var(--accent)),0 0 18px rgba(255,255,255,.38)}50%{filter:brightness(1.18);text-shadow:4px 4px 0 rgba(var(--accent-rgb),.95),7px 7px 0 color-mix(in srgb, rgb(40 40 40 / .5) 88%, var(--accent)),0 0 26px rgba(var(--accent2-rgb),.75)}}\n.sidebar.collapsed .sb-logo-text,\n.sidebar.collapsed .nav-label,\n.sidebar.collapsed .nav-sec,\n.sidebar.collapsed .sb-foot span{display:none!important}\n.sidebar.collapsed .sb-logo{justify-content:center;padding:16px 8px}\n.sidebar.collapsed .sb-logo-icon{margin:0 auto}\n.nav{flex:1;overflow-y:auto;padding:10px 0}\n.nav-sec{padding:14px 18px 6px;font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:var(--t3);font-weight:700}\n.nav-item{display:flex;align-items:center;gap:11px;padding:11px 16px;margin:2px 10px;border-radius:12px;color:var(--t3);cursor:pointer;transition:.15s;border:none;background:transparent;width:calc(100% - 20px);font-family:inherit;font-size:13px;font-weight:500}\n.nav-item svg{width:18px;height:18px;min-width:18px;min-height:18px;flex-shrink:0;display:block}\n.nav-item:hover{background:var(--hover);color:var(--t2)}\n.nav-item.on{background:var(--hover);color:var(--accent2);font-weight:700;box-shadow:inset -3px 0 0 var(--accent)}\n.sidebar.collapsed .nav-item{justify-content:center;align-items:center;padding:12px 0;margin:3px 10px;width:calc(100% - 20px);gap:0}\n.sidebar.collapsed .nav-item svg{margin:0 auto}\n.sidebar.collapsed .nav-item.on{box-shadow:none}\n.sidebar.collapsed .sb-foot button,.sidebar.collapsed .sb-foot a.btn{padding:10px 0;gap:0}\n.sidebar.collapsed .sb-foot button svg,.sidebar.collapsed .sb-foot a.btn svg{margin:0 auto;display:block}\n.sb-foot{padding:12px;border-top:1px solid var(--card-b);display:flex;flex-direction:column;gap:7px}\n.sb-foot button,.sb-foot a.btn{display:flex;align-items:center;justify-content:center;gap:8px;padding:10px;border-radius:11px;border:1px solid var(--card-b);background:var(--bg3);color:var(--t2);cursor:pointer;font-family:inherit;font-size:12px;width:100%;text-decoration:none;font-weight:600;transition:.15s}\n.sb-foot button:hover,.sb-foot a.btn:hover{background:var(--hover);color:var(--t1)}\n.sb-foot a.danger{background:rgba(239,68,68,.08);border-color:rgba(239,68,68,.2);color:var(--red)}\n\n.main{margin-right:var(--sb);flex:1;min-width:0;padding:28px 24px 60px;transition:margin .28s}\n.main.expanded{margin-right:var(--sb-c)}\n.page{display:none;animation:fadeIn .25s ease}\n.page.on{display:block}\n@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}\n.page-head{display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:14px;margin-bottom:22px}\n.page-title{font-size:20px;font-weight:800;display:flex;align-items:center;gap:10px;letter-spacing:-.02em}\n.page-title svg{width:22px;height:22px;color:var(--accent2)}\n.page-sub{font-size:12px;color:var(--t3);margin-top:5px}\n\n.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}\n.metric{background:var(--card);border:1px solid var(--card-b);border-radius:var(--radius);padding:18px;box-shadow:var(--shadow);transition:.25s;backdrop-filter:var(--glass)}\n.metric:hover{border-color:rgba(var(--accent-rgb),.25)}\n.metric-label{font-size:11px;color:var(--t3);margin-bottom:8px;display:flex;align-items:center;gap:6px;font-weight:600}\n.metric-val{font-size:24px;font-weight:800;letter-spacing:-.03em}\n.card{background:var(--card);border:1px solid var(--card-b);border-radius:var(--radius);padding:20px;margin-bottom:14px;box-shadow:var(--shadow);backdrop-filter:var(--glass);transition:border-color .2s,box-shadow .2s}\n.card-title{font-size:13px;font-weight:700;margin-bottom:14px;display:flex;align-items:center;gap:8px}\n.card-title svg{width:16px;height:16px;color:var(--accent2)}\n\n/* ============================================================\n   ONEX GROUP MANAGER — fast glass / neon red + blue\n   ============================================================ */\n.group-manager{min-width:0}.group-hero{display:flex;align-items:center;justify-content:space-between;gap:18px;padding:18px 20px;margin-bottom:14px;border:1px solid rgba(var(--accent-rgb),.24);border-radius:22px;background:linear-gradient(135deg,color-mix(in srgb, rgb(22 22 22 / .96) 92%, var(--accent)),color-mix(in srgb, rgb(13 13 13 / .96) 92%, var(--accent)));box-shadow:0 12px 30px rgba(0,0,0,.22),inset 0 1px rgba(255,255,255,.04)}\n.group-hero-copy{display:flex;align-items:center;gap:13px;min-width:0}.group-hero-icon{width:58px;height:58px;flex:0 0 58px;display:grid;place-items:center;border-radius:17px;color:var(--accent2);background:rgba(var(--accent-rgb),.13);border:1px solid rgba(var(--accent2-rgb),.32);box-shadow:0 0 24px rgba(var(--accent-rgb),.12)}.group-hero-icon svg{width:31px;height:31px}.group-hero-kicker{font-size:9px;letter-spacing:.16em;color:var(--accent2);font-weight:900}.group-hero h1{font-size:22px;font-weight:900;margin-top:3px}.group-hero p{font-size:10px;color:var(--t3);margin-top:4px}.group-stats{display:flex;align-items:center;gap:9px;flex-wrap:wrap;justify-content:flex-end}.group-stat{min-width:116px;height:58px;padding:9px 11px;border:1px solid rgba(var(--accent2-rgb),.18);border-radius:15px;background:color-mix(in srgb, rgb(23 23 23 / .78) 92%, var(--accent));display:grid;grid-template-columns:1fr auto;grid-template-rows:auto 1fr;column-gap:8px}.group-stat span{font-size:9px;color:var(--t3)}.group-stat b{font-size:18px;line-height:1.1;align-self:end}.group-stat i{grid-column:2;grid-row:1/3;align-self:center;font-style:normal;color:var(--accent2);font-size:20px}.group-stat:nth-child(2) i{color:#22c55e}.group-create-btn{height:58px;padding:0 20px;border:1px solid rgba(var(--accent-rgb),.8);border-radius:15px;background:linear-gradient(135deg,var(--accent),var(--accent));color:#fff;font:800 11px Vazirmatn,sans-serif;box-shadow:0 8px 24px rgba(var(--accent-rgb),.24);cursor:pointer}.group-create-btn span{font-size:18px;vertical-align:-2px;margin-left:5px}.group-create-btn:hover{filter:brightness(1.08)}\n.group-workspace{display:grid;grid-template-columns:minmax(0,1.28fr) minmax(360px,.72fr);gap:14px;align-items:stretch}.group-list-pane,.group-detail-pane{min-width:0;border:1px solid rgba(var(--accent-rgb),.18);border-radius:21px;background:linear-gradient(145deg,color-mix(in srgb, rgb(18 18 18 / .94) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .94) 92%, var(--accent)));box-shadow:0 12px 30px rgba(0,0,0,.22);overflow:hidden}.group-list-toolbar{padding:12px;border-bottom:1px solid rgba(var(--accent2-rgb),.12);display:flex;gap:10px;align-items:center;flex-wrap:wrap}.group-search{flex:1;min-width:190px;height:42px;display:flex;align-items:center;gap:8px;padding:0 12px;border:1px solid rgba(var(--accent-rgb),.32);border-radius:13px;background:color-mix(in srgb, rgb(8 8 8 / .7) 92%, var(--accent))}.group-search svg{width:17px;color:var(--accent2);flex:0 0 auto}.group-search input{width:100%;border:0;background:none;outline:0;color:var(--t1);font:600 11px Vazirmatn,sans-serif}.group-filters{display:flex;gap:5px}.group-filter{height:34px;padding:0 11px;border:1px solid rgba(var(--accent2-rgb),.16);border-radius:10px;background:rgba(255,255,255,.025);color:var(--t3);font:700 9px Vazirmatn,sans-serif;cursor:pointer}.group-filter.on{background:linear-gradient(135deg,var(--accent),var(--accent));border-color:var(--accent);color:#fff;box-shadow:0 5px 15px rgba(var(--accent-rgb),.2)}.group-filter em{display:inline-block;width:6px;height:6px;border-radius:50%;background:#22c55e;margin-right:3px}.group-filter[data-filter="inactive"] em{background:#ef4444}.group-cards-list{padding:11px;max-height:650px;overflow:auto}.group-card{position:relative;padding:14px;margin-bottom:9px;border:1px solid rgba(var(--accent-rgb),.28);border-radius:18px;background:linear-gradient(145deg,color-mix(in srgb, rgb(20 20 20 / .94) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .94) 92%, var(--accent)));cursor:pointer;transition:border-color .16s,transform .16s,box-shadow .16s}.group-card:last-child{margin-bottom:0}.group-card:hover{border-color:rgba(var(--accent2-rgb),.58);transform:translateY(-1px)}.group-card.selected{border-color:var(--accent);box-shadow:0 0 0 1px rgba(var(--accent-rgb),.16),0 8px 22px rgba(var(--accent-rgb),.12)}.group-card-top{display:flex;align-items:center;gap:10px}.group-card-icon{width:48px;height:48px;flex:0 0 48px;border-radius:14px;display:grid;place-items:center;color:var(--accent);border:1px solid rgba(var(--accent-rgb),.7);background:rgba(var(--accent-rgb),.06)}.group-card-icon svg{width:25px;height:25px}.group-card-main{min-width:0;flex:1}.group-card-title{display:flex;align-items:center;gap:7px;flex-wrap:wrap}.group-card-title b{font-size:13px}.group-status{font-size:8px;font-weight:900;padding:3px 8px;border-radius:99px;background:rgba(34,197,94,.12);color:#34d399;border:1px solid rgba(34,197,94,.22)}.group-status.off{background:rgba(239,68,68,.11);color:#fb7185;border-color:rgba(239,68,68,.22)}.group-card-desc{font-size:9px;color:var(--t3);margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.group-card-menu{font-size:18px;color:var(--t3);padding:0 3px}.group-card-meta{display:flex;gap:12px;flex-wrap:wrap;margin:9px 0 10px 58px;color:var(--t3);font-size:9px}.group-card-meta strong{color:var(--t2);font-weight:800}\n.group-detail-pane{padding:14px;overflow:auto}.group-detail{display:flex;flex-direction:column;gap:10px}.group-detail-head{display:flex;align-items:center;gap:10px;padding-bottom:10px;border-bottom:1px solid rgba(var(--accent2-rgb),.12)}.group-detail-icon{width:53px;height:53px;border-radius:15px;display:grid;place-items:center;color:var(--accent);border:1px solid rgba(var(--accent-rgb),.7);background:rgba(var(--accent-rgb),.07)}.group-detail-icon svg{width:28px;height:28px}.group-detail-title{min-width:0;flex:1}.group-detail-title h2{font-size:15px;font-weight:900}.group-detail-title p{font-size:9px;color:var(--t3);margin-top:3px}.group-three-dot{width:34px;height:34px;border:1px solid rgba(var(--accent-rgb),.25);border-radius:10px;background:rgba(255,255,255,.025);color:var(--t2);font-size:18px}.group-info-card,.group-link-card,.group-proto-card,.group-manage-card{border:1px solid rgba(var(--accent-rgb),.22);border-radius:16px;background:color-mix(in srgb, rgb(12 12 12 / .7) 92%, var(--accent));padding:12px}.group-section-head{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:9px}.group-section-head b{font-size:11px}.group-section-head span{font-size:8px;color:var(--t3)}.group-info-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px}.group-info-item{padding:8px 9px;border-radius:10px;background:color-mix(in srgb, rgb(38 38 38 / .3) 88%, var(--accent));border:1px solid rgba(var(--accent2-rgb),.1)}.group-info-item small{display:block;color:var(--t3);font-size:8px;margin-bottom:3px}.group-info-item b{font-size:10px}.group-link-line{display:grid;grid-template-columns:1fr 58px;gap:7px}.group-link-url{min-width:0;height:37px;padding:0 10px;display:flex;align-items:center;border:1px solid rgba(var(--accent-rgb),.28);border-radius:10px;background:color-mix(in srgb, rgb(8 8 8 / .7) 92%, var(--accent));color:var(--accent2);font:9px Inter,system-ui,sans-serif;direction:ltr;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.group-copy-btn{height:37px;border:1px solid rgba(var(--accent-rgb),.55);border-radius:10px;background:rgba(var(--accent-rgb),.16);color:var(--accent);font:800 9px Vazirmatn,sans-serif;cursor:pointer}.group-link-actions{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-top:7px}.group-link-actions button{height:35px;border-radius:10px;border:1px solid rgba(var(--accent2-rgb),.25);background:rgba(var(--accent-rgb),.13);color:var(--accent2);font:800 9px Vazirmatn,sans-serif;cursor:pointer}.group-link-actions button:last-child{background:rgba(var(--accent-rgb),.12);border-color:rgba(var(--accent-rgb),.42);color:var(--accent2)}.group-proto-list{display:flex;flex-direction:column}.group-proto-row{display:flex;align-items:center;gap:8px;min-height:38px;border-top:1px solid rgba(var(--accent2-rgb),.09)}.group-proto-row:first-child{border-top:0}.group-proto-icon{width:25px;height:25px;border-radius:8px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.12);color:var(--accent2);overflow:hidden}.group-proto-icon img{width:22px;height:22px;object-fit:contain}.group-proto-copy{min-width:0;flex:1}.group-proto-copy b{display:block;font-size:9px}.group-proto-copy small{font-size:7px;color:var(--t3)}.group-proto-tag{font-size:7px;padding:3px 6px;border-radius:7px;background:rgba(var(--accent-rgb),.13);color:var(--accent2)}.group-switch{position:relative;width:35px;height:20px;flex:0 0 35px}.group-switch input{display:none}.group-switch span{position:absolute;inset:0;border-radius:99px;background:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent));cursor:pointer;transition:.15s}.group-switch span:before{content:\'\';position:absolute;width:14px;height:14px;left:3px;top:3px;border-radius:50%;background:#fff;transition:.15s}.group-switch input:checked+span{background:var(--accent)}.group-switch input:checked+span:before{transform:translateX(15px)}.group-manage-actions{display:grid;grid-template-columns:1fr 1fr 1fr;gap:7px}.group-manage-actions button{height:36px;border-radius:10px;font:800 9px Vazirmatn,sans-serif;cursor:pointer;border:1px solid rgba(var(--accent-rgb),.3);background:rgba(var(--accent-rgb),.13);color:var(--accent2)}.group-manage-actions button:nth-child(2),.group-manage-actions button:nth-child(3){background:rgba(var(--accent-rgb),.12);border-color:rgba(var(--accent-rgb),.4);color:var(--accent2)}.group-configs-card{border:1px solid rgba(var(--accent-rgb),.22);border-radius:16px;background:color-mix(in srgb, rgb(12 12 12 / .7) 92%, var(--accent));padding:12px}.group-config-list{max-height:190px;overflow:auto}.group-config-row{display:flex;align-items:center;gap:8px;padding:7px 0;border-top:1px solid rgba(var(--accent2-rgb),.08);font-size:9px}.group-config-row:first-child{border-top:0}.group-config-row input{accent-color:var(--accent)}.group-config-row span{min-width:0;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.group-config-row small{color:var(--t3)}.group-config-save{margin-top:8px;width:100%;height:34px;border:1px solid rgba(var(--accent-rgb),.5);border-radius:10px;background:linear-gradient(135deg,var(--accent),var(--accent));color:#fff;font:800 9px Vazirmatn,sans-serif;cursor:pointer}.group-empty,.group-detail-empty{text-align:center;color:var(--t3);padding:45px 20px;font-size:10px}.group-detail-empty{display:flex;flex-direction:column;align-items:center;gap:6px;min-height:400px;justify-content:center}.group-detail-empty-icon{width:52px;height:52px;display:grid;place-items:center;border-radius:15px;color:var(--accent2);border:1px solid rgba(var(--accent2-rgb),.25);background:rgba(var(--accent-rgb),.1);font-size:22px;margin-bottom:5px}.group-detail-empty b{color:var(--t2);font-size:12px}.group-modal{width:min(460px,100%)}.group-modal-actions{display:flex;justify-content:flex-end;gap:8px;margin-top:15px}.modal-x{width:32px;height:32px;border:1px solid var(--card-b);border-radius:9px;background:rgba(255,255,255,.025);color:var(--t2);font-size:20px;cursor:pointer}.group-qr-modal{width:min(390px,100%)}.group-qr-box{display:grid;place-items:center;background:#fff;border-radius:15px;padding:15px;min-height:250px}.group-qr-box img{max-width:220px;height:auto}.group-qr-text{margin-top:9px;padding:9px;border-radius:10px;background:color-mix(in srgb, rgb(8 8 8 / .8) 92%, var(--accent));border:1px solid rgba(var(--accent2-rgb),.15);font:8px Inter,system-ui,sans-serif;color:var(--accent2);word-break:break-all;text-align:center}\n@media(max-width:900px){.group-hero{align-items:flex-start;flex-direction:column}.group-stats{width:100%;justify-content:stretch}.group-stat{flex:1}.group-create-btn{flex:1}.group-workspace{grid-template-columns:1fr}.group-detail-pane{min-height:520px}.group-cards-list{max-height:none}}\n@media(max-width:600px){.group-hero{padding:13px;border-radius:16px}.group-hero-icon{width:44px;height:44px;flex-basis:44px;border-radius:13px}.group-hero-icon svg{width:24px}.group-hero h1{font-size:16px}.group-hero p{font-size:8px}.group-stats{display:grid;grid-template-columns:1fr 1fr}.group-create-btn{grid-column:1/-1;width:100%}.group-workspace{gap:9px}.group-list-pane,.group-detail-pane{border-radius:15px}.group-list-toolbar{padding:8px}.group-search{min-width:100%;height:38px}.group-filters{width:100%}.group-filter{flex:1}.group-card{padding:10px;border-radius:14px}.group-card-meta{margin-right:0;margin-left:0}.group-detail-pane{padding:9px}.group-info-grid{grid-template-columns:1fr 1fr}}\n.g2{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}\n.action-card{cursor:pointer;transition:.2s;border:1px solid var(--card-b)}\n.action-card:hover{border-color:rgba(var(--accent-rgb),.4);transform:translateY(-2px);box-shadow:0 12px 28px rgba(var(--accent-rgb),.12)}\n.action-card.purple:hover{border-color:rgba(var(--purple-rgb),.45)}\n\n.btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;padding:10px 16px;border-radius:11px;border:1px solid var(--card-b);background:var(--bg3);color:var(--t2);cursor:pointer;font-family:inherit;font-size:12px;font-weight:600;transition:.15s}\n.btn:hover{color:var(--t1);border-color:var(--accent)}\n.btn-p{background:linear-gradient(135deg,var(--accent),var(--purple));border:none;color:#fff;box-shadow:0 6px 20px rgba(var(--accent-rgb),.35)}\n.btn-p:hover{filter:brightness(1.08);color:#fff}\n.btn-d{background:rgba(239,68,68,.1);border-color:rgba(239,68,68,.25);color:var(--red)}\n.btn-sm{padding:7px 11px;font-size:11px;border-radius:9px}\n.btn svg{width:15px;height:15px}\n\n.table-wrap{overflow-x:auto;border-radius:14px;border:1px solid var(--card-b)}\ntable{width:100%;border-collapse:collapse;font-size:12.5px}\nth{text-align:right;padding:12px 14px;background:var(--bg3);color:var(--t3);font-weight:700;white-space:nowrap}\ntd{padding:12px 14px;border-top:1px solid var(--card-b);vertical-align:middle}\ntr:hover td{background:var(--hover)}\n.ops{display:flex;gap:5px;flex-wrap:wrap;align-items:center}\n\n.range-tabs{display:flex;gap:4px;background:var(--bg3);padding:4px;border-radius:12px;border:1px solid var(--card-b)}\n.range-tab{padding:7px 13px;border-radius:9px;font-size:11px;font-weight:700;color:var(--t3);cursor:pointer;border:none;background:transparent;font-family:inherit;transition:.15s}\n.range-tab.on{background:var(--accent);color:#fff;box-shadow:0 2px 8px rgba(var(--accent-rgb),.35)}\n\n.field{margin-bottom:14px}\n.field label{display:block;font-size:11px;color:var(--t3);margin-bottom:6px;font-weight:700}\n.field input,.field select,.field textarea{width:100%;padding:11px 13px;border-radius:11px;border:1px solid var(--card-b);background:var(--input-bg);color:var(--t1);font-family:inherit;font-size:13px;outline:none;transition:.15s}\n.field input:focus,.field select:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(var(--accent-rgb),.15)}\n.form-row{display:grid;grid-template-columns:1fr 1fr;gap:12px}\n\n.support-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px}\n.support-tile{display:flex;align-items:center;gap:14px;padding:20px;background:var(--card);border:1px solid var(--card-b);border-radius:var(--radius);text-decoration:none;color:inherit;transition:.2s;box-shadow:var(--shadow)}\n.support-tile:hover{border-color:rgba(var(--accent-rgb),.35);transform:translateY(-3px)}\n.support-icon{width:48px;height:48px;border-radius:14px;background:var(--hover);display:flex;align-items:center;justify-content:center;flex-shrink:0}\n.support-icon svg{width:22px;height:22px;color:var(--accent2)}\n.support-label{font-size:11px;color:var(--t3);font-weight:600}\n.support-val{font-size:13px;font-weight:700;margin-top:3px}\n\n/* ============================================================\n   ONEX ACTIVITY LOG — scoped redesign\n   Everything is prefixed with .logs- / #page-logs so the rest of\n   the panel keeps its existing layout and behavior unchanged.\n   ============================================================ */\n.log-item{padding:12px 0;border-bottom:1px solid var(--card-b);font-size:12px;display:flex;gap:12px;align-items:flex-start}\n.log-time{color:var(--t3);font-size:10px;white-space:nowrap;min-width:72px;font-weight:600}\n.log-msg{color:var(--t2);flex:1;line-height:1.5}\n\n#page-logs .logs-page-sub{margin-top:4px;color:var(--t3);font-size:11px}\n#page-logs .logs-page-head{align-items:flex-start}\n#page-logs .logs-head-actions{display:flex;gap:8px;align-items:center}\n#page-logs .logs-refresh-btn{display:inline-flex;align-items:center;gap:6px}\n#page-logs .logs-toolbar{padding:14px;margin-bottom:10px}\n#page-logs .logs-search-wrap{height:46px;display:flex;align-items:center;gap:10px;border:1px solid var(--card-b);background:color-mix(in srgb, rgb(13 13 13 / .42) 92%, var(--accent));border-radius:14px;padding:0 12px}\n#page-logs .logs-search-wrap>svg{width:20px;height:20px;color:var(--accent2);flex:0 0 auto}\n#page-logs .logs-search-wrap input{border:0;outline:0;background:transparent;color:var(--t1);font:inherit;font-size:12px;width:100%;min-width:0}\n#page-logs .logs-search-wrap input::placeholder{color:var(--t3)}\n#page-logs .logs-clear-search{border:0;background:transparent;color:var(--t3);font-size:20px;line-height:1;cursor:pointer;padding:3px 6px}\n#page-logs .logs-filter-row{display:flex;gap:6px;overflow:auto;padding-top:10px;scrollbar-width:none}\n#page-logs .logs-filter-row::-webkit-scrollbar{display:none}\n#page-logs .logs-filter{border:1px solid var(--card-b);background:rgba(255,255,255,.025);color:var(--t2);padding:7px 11px;border-radius:10px;font:inherit;font-size:10px;white-space:nowrap;cursor:pointer;transition:.18s}\n#page-logs .logs-filter:hover{border-color:rgba(var(--accent2-rgb),.38);color:var(--t1)}\n#page-logs .logs-filter.on{background:linear-gradient(135deg,var(--accent),var(--accent));border-color:transparent;color:#fff;box-shadow:0 8px 22px rgba(var(--accent-rgb),.22)}\n#page-logs .logs-filter b{font-size:9px;opacity:.82;margin-right:3px}\n#page-logs .logs-advanced-row{display:none;grid-template-columns:minmax(0,1fr) minmax(0,1fr) auto;gap:8px;align-items:end;margin-top:10px;padding-top:10px;border-top:1px solid var(--card-b)}\n#page-logs .logs-advanced-row.open{display:grid}\n#page-logs .logs-advanced-row label{display:grid;gap:5px;min-width:0}\n#page-logs .logs-advanced-row label>span{font-size:9px;color:var(--t3)}\n#page-logs .logs-advanced-row select,#page-logs .logs-advanced-row input{height:36px;border:1px solid var(--card-b);border-radius:10px;background:var(--input-bg);color:var(--t1);padding:0 9px;font:inherit;font-size:10px;outline:0}\n#page-logs .logs-reset-filter{height:36px;border:1px solid var(--card-b);border-radius:10px;background:transparent;color:var(--t2);padding:0 11px;font:inherit;font-size:10px;cursor:pointer;white-space:nowrap}\n#page-logs .logs-summary{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0;padding:0;margin-bottom:10px;overflow:hidden}\n#page-logs .logs-summary-item{min-width:0;display:flex;align-items:center;gap:10px;padding:13px 14px;position:relative}\n#page-logs .logs-summary-item+ .logs-summary-item{border-right:1px solid var(--card-b)}\n#page-logs .logs-summary-icon{width:34px;height:34px;border-radius:11px;display:grid;place-items:center;flex:0 0 34px;font-weight:900;font-size:15px;background:rgba(var(--accent-rgb),.12);color:var(--accent2);border:1px solid rgba(var(--accent-rgb),.2)}\n#page-logs .logs-summary-item div{min-width:0;display:grid}\n#page-logs .logs-summary-item small{font-size:9px;color:var(--t3)}\n#page-logs .logs-summary-item strong{font-size:18px;line-height:1.15;color:var(--t1)}\n#page-logs .logs-summary-item em{font-size:8px;color:var(--t3);font-style:normal}\n#page-logs .logs-summary-ok .logs-summary-icon{color:#34e6a1;background:rgba(52,230,161,.10);border-color:rgba(52,230,161,.2)}\n#page-logs .logs-summary-change .logs-summary-icon{color:var(--purple);background:rgba(var(--purple-rgb),.11);border-color:rgba(var(--purple-rgb),.2)}\n#page-logs .logs-summary-delete .logs-summary-icon{color:#ff5f91;background:rgba(255,45,120,.10);border-color:rgba(255,45,120,.2)}\n#page-logs .logs-list-card{padding:0;overflow:hidden}\n#page-logs .logs-list-head{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:14px 16px;border-bottom:1px solid var(--card-b)}\n#page-logs .logs-list-head>div{display:grid;gap:3px}\n#page-logs .logs-list-head b{font-size:12px;color:var(--t1)}\n#page-logs .logs-list-head small{font-size:9px;color:var(--t3)}\n#page-logs .logs-live-badge{display:inline-flex;align-items:center;gap:5px;font-size:8px;color:#47e8b0;letter-spacing:.08em}\n#page-logs .logs-live-badge i{width:6px;height:6px;border-radius:50%;background:#22e6a1;box-shadow:0 0 9px rgba(34,230,161,.75);animation:pulseDot 1.8s ease-in-out infinite}\n#page-logs .logs-list{padding:0 16px}\n#page-logs .logs-event{display:grid;grid-template-columns:34px minmax(0,1fr) auto 22px;gap:10px;align-items:center;padding:12px 0;border-bottom:1px solid color-mix(in srgb, rgb(128 128 128 / .10) 82%, var(--accent));cursor:pointer;transition:.16s}\n#page-logs .logs-event:last-child{border-bottom:0}\n#page-logs .logs-event:hover{transform:translateX(-2px);background:linear-gradient(90deg,transparent,rgba(var(--accent-rgb),.055),transparent)}\n#page-logs .logs-event-icon{width:34px;height:34px;border-radius:11px;display:grid;place-items:center;font-size:15px;font-weight:900;border:1px solid rgba(var(--accent2-rgb),.18);background:rgba(var(--accent-rgb),.10);color:var(--accent2)}\n#page-logs .logs-event-main{min-width:0;display:grid;gap:3px}\n#page-logs .logs-event-title{font-size:11px;font-weight:800;color:var(--t1);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n#page-logs .logs-event-desc{font-size:9px;color:var(--t3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n#page-logs .logs-event-time{font-size:10px;color:var(--accent2);white-space:nowrap;direction:ltr}\n#page-logs .logs-event-arrow{font-size:18px;color:color-mix(in srgb, rgb(128 128 128) 82%, var(--accent));opacity:.85}\n#page-logs .logs-badge{justify-self:end;padding:4px 8px;border-radius:999px;border:1px solid rgba(var(--accent2-rgb),.20);font-size:8px;white-space:nowrap;color:var(--accent2);background:rgba(var(--accent-rgb),.08)}\n#page-logs .logs-event.ok .logs-event-icon,#page-logs .logs-event.ok .logs-badge{color:#35e8a2;border-color:rgba(53,232,162,.22);background:rgba(53,232,162,.08)}\n#page-logs .logs-event.warn .logs-event-icon,#page-logs .logs-event.warn .logs-badge{color:#ff6295;border-color:rgba(255,98,149,.22);background:rgba(255,98,149,.08)}\n#page-logs .logs-event.err .logs-event-icon,#page-logs .logs-event.err .logs-badge{color:#ff7a7a;border-color:rgba(255,100,100,.22);background:rgba(255,100,100,.08)}\n#page-logs .logs-event.admin .logs-event-icon{color:var(--purple);background:rgba(var(--purple-rgb),.09);border-color:rgba(var(--purple-rgb),.20)}\n#page-logs .logs-event.sub .logs-event-icon{color:#48e6bd;background:rgba(34,211,169,.09);border-color:rgba(34,211,169,.20)}\n#page-logs .logs-event.link .logs-event-icon{color:var(--accent2);background:rgba(var(--accent-rgb),.09)}\n#page-logs .logs-event.backup .logs-event-icon{color:#ffb85b;background:rgba(245,158,11,.09);border-color:rgba(245,158,11,.20)}\n#page-logs .logs-event.telegram .logs-event-icon{color:var(--accent);background:rgba(var(--accent-rgb),.09);border-color:rgba(var(--accent-rgb),.20)}\n#page-logs .logs-empty{padding:42px 16px;text-align:center;color:var(--t3);font-size:11px}\n#page-logs .logs-footer{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:12px 14px;margin-top:10px}\n#page-logs .logs-advanced-btn{border:1px solid var(--card-b);background:transparent;color:var(--t2);padding:8px 11px;border-radius:10px;font:inherit;font-size:10px;cursor:pointer}\n#page-logs .logs-advanced-btn span{display:inline-block;transition:.18s;margin-left:3px}\n#page-logs .logs-advanced-btn.open span{transform:rotate(180deg)}\n#page-logs .logs-clear-btn{display:inline-flex;align-items:center;gap:7px;border:0;border-radius:12px;background:linear-gradient(135deg,var(--accent),var(--accent));color:#fff;padding:10px 14px;font:inherit;font-size:10px;font-weight:800;cursor:pointer;box-shadow:0 10px 28px rgba(var(--accent-rgb),.18)}\n#page-logs .logs-clear-btn svg{width:15px;height:15px}\n#page-logs .logs-modal-bg{position:fixed;inset:0;z-index:650;display:none;align-items:center;justify-content:center;padding:16px;background:color-mix(in srgb, rgb(4 4 4 / .66) 92%, var(--accent));backdrop-filter:blur(9px);-webkit-backdrop-filter:blur(9px)}\n#page-logs .logs-modal-bg.open{display:flex}\n#page-logs .logs-detail-modal{width:min(500px,100%);max-height:88vh;overflow:auto;border:1px solid rgba(var(--accent2-rgb),.24);border-radius:22px;background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .96) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .94) 92%, var(--accent)));box-shadow:0 30px 100px rgba(0,0,0,.55),0 0 70px rgba(var(--accent-rgb),.10);padding:16px}\n#page-logs .logs-detail-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding-bottom:13px;border-bottom:1px solid var(--card-b)}\n#page-logs .logs-detail-head>div:first-child{display:flex;align-items:center;gap:10px;min-width:0}\n#page-logs .logs-detail-head b{display:block;font-size:13px;color:var(--t1)}\n#page-logs .logs-detail-head small{display:block;margin-top:3px;font-size:9px;color:var(--t3)}\n#page-logs .logs-detail-head>button{width:30px;height:30px;border:1px solid var(--card-b);border-radius:9px;background:transparent;color:var(--t2);font-size:20px;cursor:pointer}\n#page-logs .logs-detail-icon{width:38px;height:38px;border-radius:12px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.11);border:1px solid rgba(var(--accent-rgb),.20);color:var(--accent2);font-size:17px}\n#page-logs .logs-detail-body{display:grid;gap:8px;padding:14px 0}\n#page-logs .logs-detail-row{display:grid;grid-template-columns:90px minmax(0,1fr);gap:10px;padding:9px 0;border-bottom:1px solid color-mix(in srgb, rgb(128 128 128 / .09) 82%, var(--accent))}\n#page-logs .logs-detail-row:last-child{border-bottom:0}\n#page-logs .logs-detail-row small{color:var(--t3);font-size:9px}\n#page-logs .logs-detail-row b{color:var(--t1);font-size:10px;word-break:break-word;line-height:1.7}\n#page-logs .logs-detail-actions{display:flex;justify-content:flex-end;padding-top:3px}\nhtml.light #page-logs .logs-search-wrap{background:#fff}\nhtml.light #page-logs .logs-filter{background:#fff;color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))}\nhtml.light #page-logs .logs-event{border-bottom-color:color-mix(in srgb, rgb(23 23 23 / .07) 92%, var(--accent))}\nhtml.light #page-logs .logs-event:hover{background:#f8fbff}\nhtml.light #page-logs .logs-detail-modal{background:#fff;border-color:color-mix(in srgb, rgb(23 23 23 / .12) 92%, var(--accent));box-shadow:0 24px 70px color-mix(in srgb, rgb(23 23 23 / .18) 92%, var(--accent))}\nhtml.light #page-logs .logs-detail-head>button{background:#fff}\n\n.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.55);backdrop-filter:blur(6px);z-index:500;display:none;align-items:center;justify-content:center;padding:16px}\n.modal-bg.open{display:flex}\n.modal{background:var(--bg2);border:1px solid var(--card-b);border-radius:20px;width:min(520px,100%);max-height:90vh;overflow-y:auto;padding:24px;box-shadow:0 24px 64px rgba(0,0,0,.4)}\n.modal-title{font-size:17px;font-weight:800;margin-bottom:16px}\n.modal-actions{display:flex;gap:8px;justify-content:flex-end;margin-top:18px;flex-wrap:wrap}\n.link-box{background:var(--input-bg);border:1px solid var(--card-b);border-radius:12px;padding:12px;font-size:11px;word-break:break-all;color:var(--t2);margin:8px 0 12px;font-family:ui-monospace,monospace;line-height:1.6;max-height:90px;overflow:auto}\n\n.toast{position:fixed;bottom:28px;left:50%;transform:translateX(-50%) translateY(90px);background:var(--bg2);border:1px solid var(--card-b);color:var(--t1);padding:13px 22px;border-radius:14px;font-size:13px;font-weight:600;z-index:999;opacity:0;transition:.3s;pointer-events:none;box-shadow:var(--shadow)}\n.toast.show{opacity:1;transform:translateX(-50%) translateY(0)}\n\n.switch{position:relative;display:inline-block;width:44px;height:26px;vertical-align:middle}\n.switch input{opacity:0;width:0;height:0}\n.slider{position:absolute;cursor:pointer;inset:0;background:color-mix(in srgb, rgb(161 161 161 / .35) 82%, var(--accent));border-radius:26px;transition:.2s}\n.slider:before{position:absolute;content:"";height:20px;width:20px;left:3px;bottom:3px;background:#fff;border-radius:50%;transition:.2s;box-shadow:0 2px 6px rgba(0,0,0,.2)}\n.switch input:checked+.slider{background:var(--green)}\n.switch input:checked+.slider:before{transform:translateX(18px)}\n\n.mob-bar{display:none;position:fixed;top:0;left:0;right:0;height:56px;background:var(--bg2);border-bottom:1px solid var(--card-b);z-index:250;align-items:center;justify-content:space-between;padding:0 16px;box-shadow:var(--shadow)}\n.overlay{position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:290;display:none}\n.overlay.show{display:block}\n\n\n\n.spin{width:36px;height:36px;border:3px solid var(--card-b);border-top-color:var(--accent);border-radius:50%;margin:0 auto;animation:spin .8s linear infinite}\n@keyframes spin{to{transform:rotate(360deg)}}\n\n.conn-badge{display:inline-flex;align-items:center;justify-content:center;min-width:22px;height:20px;padding:0 7px;border-radius:8px;font-size:10px;font-weight:800}\n.conn-badge.green{background:rgba(34,197,94,.18);color:#4ade80}\n.conn-badge.gray{background:color-mix(in srgb, rgb(161 161 161 / .15) 82%, var(--accent));color:color-mix(in srgb, rgb(161 161 161) 82%, var(--accent))}\n.conn-badge.orange{background:rgba(245,158,11,.18);color:#fbbf24}\n.conn-badge.red{background:rgba(239,68,68,.18);color:#f87171}\n.spin{width:36px;height:36px;border:3px solid var(--card-b);border-top-color:var(--accent);border-radius:50%;margin:0 auto;animation:spin .8s linear infinite}\n@keyframes spin{to{transform:rotate(360deg)}}\n\n/* =========================================================\n   ONEX DASHBOARD REDESIGN\n   ========================================================= */\n.mob-brand{display:flex;align-items:center;gap:9px}\n.mob-brand-mark{position:relative;width:38px;height:38px;border-radius:12px;display:grid;place-items:center;color:#fff;font-size:0;font-weight:900;isolation:isolate;background:linear-gradient(145deg,var(--accent),var(--accent) 52%,var(--purple));border:1px solid rgba(255,255,255,.22);box-shadow:0 10px 24px rgba(var(--accent-rgb),.35),inset 0 1px rgba(255,255,255,.28);transform:perspective(220px) rotateX(7deg) rotateY(-8deg);animation:onexLogoFloat 3.2s ease-in-out infinite}\n.mob-brand-mark:before{content:\'\';position:absolute;inset:4px;border-radius:9px;background:linear-gradient(145deg,rgba(255,255,255,.25),rgba(255,255,255,.03) 50%,rgba(0,0,0,.18));z-index:-1}\n.mob-brand-mark:after{content:\'N\';position:absolute;inset:0;display:grid;place-items:center;font:900 18px/1 Inter,system-ui,sans-serif;color:#fff;text-shadow:2px 2px 0 rgba(var(--accent-rgb),.95),4px 4px 0 color-mix(in srgb, rgb(40 40 40 / .5) 88%, var(--accent)),0 0 13px rgba(255,255,255,.35);transform:translateZ(12px)}\n.onex-topbar{height:66px;display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:18px;padding:10px 14px 10px 16px;border:1px solid rgba(var(--accent2-rgb),.14);border-radius:20px;background:linear-gradient(180deg,color-mix(in srgb, rgb(24 24 24 / .82) 92%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .72) 92%, var(--accent)));backdrop-filter:blur(18px);box-shadow:0 12px 35px rgba(0,0,0,.28),inset 0 1px rgba(255,255,255,.04)}\n/* ONEX floating glass control dock */\n.onex-control-dock{position:relative;display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:-4px 0 18px;padding:9px;border:1px solid color-mix(in srgb, rgb(161 161 161 / .16) 82%, var(--accent));border-radius:22px;background:linear-gradient(135deg,color-mix(in srgb, rgb(23 23 23 / .72) 92%, var(--accent)),color-mix(in srgb, rgb(13 13 13 / .58) 92%, var(--accent)));backdrop-filter:blur(22px) saturate(135%);-webkit-backdrop-filter:blur(22px) saturate(135%);box-shadow:0 18px 45px rgba(0,0,0,.22),inset 0 1px rgba(255,255,255,.07)}\n.onex-control-dock:before{content:"";position:absolute;inset:0;border-radius:22px;background:linear-gradient(90deg,transparent,rgba(var(--accent2-rgb),.08),transparent);background-size:220% 100%;animation:dockSweep 5s linear infinite;pointer-events:none}\n.onex-3d-control{position:relative;min-height:62px;border:1px solid color-mix(in srgb, rgb(161 161 161 / .18) 82%, var(--accent));border-radius:17px;background:linear-gradient(145deg,color-mix(in srgb, rgb(40 40 40 / .88) 88%, var(--accent)),color-mix(in srgb, rgb(23 23 23 / .66) 92%, var(--accent)));color:var(--t1);display:flex;align-items:center;justify-content:center;gap:10px;padding:10px 14px;cursor:pointer;font-family:inherit;font-size:12px;font-weight:800;overflow:hidden;transform:translateY(0) perspective(700px) rotateX(0deg);transition:transform .22s ease,box-shadow .22s ease,border-color .22s ease,background .22s ease;box-shadow:0 9px 0 color-mix(in srgb, rgb(7 7 7 / .7) 92%, var(--accent)),0 16px 25px rgba(0,0,0,.24),inset 0 1px rgba(255,255,255,.08)}\n.onex-3d-control:after{content:"";position:absolute;top:-60%;left:-25%;width:45%;height:220%;transform:rotate(24deg);background:linear-gradient(90deg,transparent,rgba(255,255,255,.14),transparent);animation:controlShine 3.8s ease-in-out infinite}\n.onex-3d-control:hover{transform:translateY(-4px) perspective(700px) rotateX(3deg);border-color:rgba(var(--accent2-rgb),.45);box-shadow:0 13px 0 color-mix(in srgb, rgb(7 7 7 / .7) 92%, var(--accent)),0 22px 35px rgba(var(--accent-rgb),.16),inset 0 1px rgba(255,255,255,.1)}\n.onex-3d-control:active{transform:translateY(2px) perspective(700px) rotateX(0deg);box-shadow:0 5px 0 color-mix(in srgb, rgb(7 7 7 / .7) 92%, var(--accent)),0 10px 18px rgba(0,0,0,.2)}\n.onex-3d-control .control-icon{width:39px;height:39px;flex:0 0 39px;display:grid;place-items:center;border-radius:13px;background:linear-gradient(145deg,var(--accent),var(--purple));box-shadow:inset 2px 2px 4px rgba(255,255,255,.22),inset -3px -4px 7px rgba(0,0,0,.28),0 7px 18px rgba(var(--accent-rgb),.28);transform:translateZ(18px);animation:iconFloat 2.8s ease-in-out infinite}\n.onex-3d-control:nth-child(2) .control-icon{background:linear-gradient(145deg,#06b6d4,var(--accent));box-shadow:inset 2px 2px 4px rgba(255,255,255,.22),inset -3px -4px 7px rgba(0,0,0,.28),0 7px 18px rgba(6,182,212,.24);animation-delay:.35s}\n.onex-3d-control:nth-child(3) .control-icon{background:linear-gradient(145deg,#10b981,#059669);box-shadow:inset 2px 2px 4px rgba(255,255,255,.22),inset -3px -4px 7px rgba(0,0,0,.28),0 7px 18px rgba(16,185,129,.24);animation-delay:.7s}\n.onex-3d-control svg{width:19px;height:19px;filter:drop-shadow(0 2px 2px rgba(0,0,0,.28))}\n.onex-3d-control .control-copy{display:flex;flex-direction:column;align-items:flex-start;gap:3px;line-height:1.2}\n.onex-3d-control .control-title{font-size:12px}.onex-3d-control .control-sub{font-size:9px;color:var(--t3);font-weight:600;letter-spacing:.3px}\n@keyframes controlShine{0%,45%{left:-45%;opacity:0}55%{opacity:1}100%{left:120%;opacity:0}}\n@keyframes iconFloat{0%,100%{transform:translateY(0) translateZ(18px) rotate(-1deg)}50%{transform:translateY(-3px) translateZ(18px) rotate(1deg)}}\n@keyframes dockSweep{0%{background-position:200% 0}100%{background-position:-20% 0}}\n\n\n.top-server{display:flex;align-items:center;gap:12px;min-width:0}.top-dot{width:10px;height:10px;border-radius:50%;background:#22c55e;box-shadow:0 0 14px #22c55e;animation:pulseDot 1.8s ease-in-out infinite}.top-server b{font-size:13px}.top-server small{color:var(--t3);font-size:11px}.top-sep{width:1px;height:24px;background:var(--card-b)}\n.top-actions{display:flex;align-items:center;gap:8px}.top-chip{display:flex;align-items:center;gap:7px;padding:9px 12px;border:1px solid var(--card-b);border-radius:12px;background:rgba(255,255,255,.025);color:var(--t2);font-size:11px}.top-avatar{width:38px;height:38px;border-radius:12px;display:grid;place-items:center;background:linear-gradient(135deg,var(--accent),var(--purple));font-weight:900;color:#fff;box-shadow:0 0 24px rgba(var(--accent-rgb),.3)}\n.dashboard-hero{display:grid;grid-template-columns:minmax(0,1fr) auto auto;align-items:end;gap:12px;margin:0 2px 18px}.hero-main{min-width:0}.hero-version-strip{display:flex;align-items:stretch;gap:8px}.version-mini-card{min-width:128px;min-height:58px;padding:8px 10px;border:1px solid rgba(var(--accent2-rgb),.18);border-radius:15px;background:linear-gradient(145deg,color-mix(in srgb, rgb(27 27 27 / .88) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .78) 92%, var(--accent)));display:flex;align-items:center;gap:8px;box-shadow:0 10px 24px rgba(0,0,0,.20),inset 0 1px rgba(255,255,255,.06)}.version-mini-icon{width:30px;height:30px;flex:0 0 30px;border-radius:10px;display:grid;place-items:center;color:var(--accent2);background:linear-gradient(145deg,rgba(var(--accent-rgb),.34),rgba(var(--accent-rgb),.14));border:1px solid rgba(var(--accent2-rgb),.22);font-size:14px;font-weight:900}.version-mini-copy{display:flex;flex-direction:column;gap:2px;min-width:0}.version-mini-copy b{font-size:9px;color:var(--t3);font-weight:700;white-space:nowrap}.version-mini-copy strong{font-size:13px;color:var(--t1);font-weight:900;direction:ltr;text-align:left;white-space:nowrap}.version-live-dot{width:7px;height:7px;flex:0 0 7px;border-radius:50%;background:#22c55e;box-shadow:0 0 10px #22c55e}.dashboard-hero .hero-actions{display:flex;gap:8px;flex-wrap:wrap}.hero-kicker{font-size:10px;letter-spacing:.2em;color:var(--accent2);font-weight:800;text-transform:uppercase}.hero-title{font-size:27px;font-weight:900;line-height:1.25;margin-top:6px}.hero-title span{color:var(--accent2);text-shadow:0 0 22px rgba(var(--accent2-rgb),.35)}.hero-sub{margin-top:6px;color:var(--t3);font-size:12px}.hero-actions{display:flex;gap:8px;flex-wrap:wrap}\n.onex-metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-bottom:16px}.onex-metric{position:relative;overflow:hidden;min-height:122px;padding:17px;border:1px solid rgba(var(--accent2-rgb),.13);border-radius:20px;background:linear-gradient(145deg,color-mix(in srgb, rgb(24 24 24 / .92) 92%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .92) 92%, var(--accent)));box-shadow:0 12px 32px rgba(0,0,0,.28),inset 0 1px rgba(255,255,255,.035);transition:.25s}.onex-metric:hover{transform:translateY(-3px);border-color:rgba(var(--accent2-rgb),.35);box-shadow:0 16px 40px rgba(var(--accent-rgb),.14)}.onex-metric:after{content:\'\';position:absolute;right:-40px;bottom:-55px;width:140px;height:140px;border-radius:50%;background:rgba(var(--accent-rgb),.14);filter:blur(18px)}.metric-icon{width:42px;height:42px;border-radius:14px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.14);border:1px solid rgba(var(--accent2-rgb),.24);color:var(--accent2);box-shadow:0 0 20px rgba(var(--accent-rgb),.15)}.metric-icon svg{width:21px;height:21px}.onex-metric .metric-label{margin:10px 0 3px}.onex-metric .metric-val{font-size:25px}.metric-trend{position:absolute;left:15px;bottom:16px;font-size:10px;color:#34d399;font-weight:800}\n.dashboard-grid{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(260px,.72fr);gap:14px;align-items:stretch}.dashboard-grid-right{display:grid;grid-template-rows:auto 1fr;gap:14px}.onex-card{background:linear-gradient(145deg,color-mix(in srgb, rgb(20 20 20 / .92) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .92) 92%, var(--accent)));border:1px solid rgba(var(--accent2-rgb),.12);border-radius:20px;box-shadow:0 14px 38px rgba(0,0,0,.3),inset 0 1px rgba(255,255,255,.035);overflow:hidden}.onex-card-head{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:16px 18px;border-bottom:1px solid rgba(255,255,255,.055)}.onex-card-title{display:flex;align-items:center;gap:8px;font-size:13px;font-weight:800}.onex-card-title svg{color:var(--accent2)}.onex-card-body{padding:16px 18px}\n.chart-wrap{height:275px;padding:8px 12px 12px;position:relative}.traffic-svg{width:100%;height:100%;display:block}.chart-grid-line{stroke:color-mix(in srgb, rgb(161 161 161 / .1) 82%, var(--accent));stroke-width:1}.chart-fill{fill:url(#trafficFill)}.chart-line{fill:none;stroke:var(--accent);stroke-width:3;stroke-linecap:round;stroke-linejoin:round;filter:drop-shadow(0 0 7px rgba(var(--accent2-rgb),.55))}.chart-dot{fill:#fff;stroke:var(--accent);stroke-width:3;filter:drop-shadow(0 0 6px rgba(var(--accent2-rgb),.75))}.chart-labels{display:flex;justify-content:space-between;padding:0 10px;color:var(--t3);font-size:9px}.chart-badge{position:absolute;top:18px;right:25%;padding:7px 10px;border:1px solid rgba(var(--accent2-rgb),.25);background:color-mix(in srgb, rgb(11 11 11 / .88) 92%, var(--accent));border-radius:10px;font-size:10px;color:var(--accent2);box-shadow:0 0 18px rgba(var(--accent2-rgb),.1)}\n.range-mini{display:flex;gap:4px;background:rgba(255,255,255,.025);padding:3px;border-radius:10px}.range-mini button{border:0;background:transparent;color:var(--t3);font-family:inherit;font-size:9px;padding:6px 9px;border-radius:8px;cursor:pointer}.range-mini button.on{background:var(--accent);color:#fff;box-shadow:0 4px 12px rgba(var(--accent-rgb),.25)}\n.health-list{display:grid;gap:14px}.health-row{display:grid;grid-template-columns:34px 1fr 42px;gap:10px;align-items:center}.health-icon{width:34px;height:34px;border-radius:11px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.1);color:var(--accent2)}.health-name{font-size:11px;color:var(--t2);margin-bottom:5px}.health-pct{font-size:10px;color:var(--t2);text-align:left;direction:ltr}.health-track{height:7px;border-radius:99px;background:color-mix(in srgb, rgb(161 161 161 / .1) 82%, var(--accent));overflow:hidden}.health-fill{height:100%;width:var(--w);border-radius:99px;background:linear-gradient(90deg,var(--accent),#22d3ee);box-shadow:0 0 12px rgba(34,211,238,.35);animation:healthIn .9s ease both}.xray-state{display:flex;align-items:center;justify-content:space-between;padding:11px 12px;border-radius:13px;background:rgba(34,197,94,.07);border:1px solid rgba(34,197,94,.16);font-size:11px;margin-top:4px}.xray-state span:last-child{color:#34d399;font-weight:800}.xray-dot{width:7px;height:7px;border-radius:50%;background:#22c55e;display:inline-block;box-shadow:0 0 9px #22c55e;margin-left:6px}\n.telegram-card{position:relative;min-height:100%;display:flex;flex-direction:column;justify-content:space-between;padding:20px;overflow:hidden;background:radial-gradient(circle at 50% 20%,rgba(var(--accent-rgb),.24),transparent 35%),linear-gradient(145deg,color-mix(in srgb, rgb(22 22 22 / .96) 92%, var(--accent)),color-mix(in srgb, rgb(10 10 10 / .96) 92%, var(--accent)));border:1px solid rgba(var(--accent-rgb),.3);border-radius:20px;box-shadow:0 14px 38px rgba(0,0,0,.32),0 0 35px rgba(var(--accent-rgb),.08)}.tg-orbit{width:118px;height:118px;border:1px solid rgba(var(--accent-rgb),.5);border-radius:50%;margin:5px auto 12px;display:grid;place-items:center;position:relative;animation:orbitSpin 8s linear infinite}.tg-orbit:before,.tg-orbit:after{content:\'\';position:absolute;border:1px solid rgba(var(--accent2-rgb),.3);border-radius:50%}.tg-orbit:before{inset:12px;transform:rotate(55deg) scaleX(1.45)}.tg-orbit:after{inset:24px;transform:rotate(-35deg) scaleX(1.55)}.tg-logo{width:66px;height:66px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(145deg,var(--accent),var(--accent));box-shadow:0 0 28px rgba(var(--accent-rgb),.6);animation:floatY 2.8s ease-in-out infinite}.tg-logo svg{width:34px;height:34px;color:#fff}.tg-title{text-align:center;font-size:13px;color:var(--t2)}.tg-handle{text-align:center;font-size:23px;font-weight:900;color:var(--accent);margin-top:5px;text-shadow:0 0 18px rgba(var(--accent2-rgb),.3);direction:ltr}.tg-desc{text-align:center;color:var(--t3);font-size:10px;margin-top:6px}.tg-btn{display:flex;align-items:center;justify-content:center;gap:7px;margin-top:18px;padding:11px;border-radius:13px;text-decoration:none;color:#fff;background:linear-gradient(135deg,var(--accent),var(--accent));box-shadow:0 8px 24px rgba(var(--accent-rgb),.3);font-size:11px;font-weight:800}.tg-btn:hover{filter:brightness(1.08)}\n.quick-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}.quick-item{display:flex;align-items:center;gap:10px;padding:13px;border-radius:15px;background:rgba(255,255,255,.025);border:1px solid var(--card-b);cursor:pointer;transition:.2s}.quick-item:hover{transform:translateY(-2px);border-color:rgba(var(--accent2-rgb),.3);background:rgba(var(--accent-rgb),.07)}.quick-icon{width:38px;height:38px;border-radius:12px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.13);color:var(--accent2)}.quick-item:nth-child(2) .quick-icon{color:#34d399;background:rgba(34,197,94,.1)}.quick-item:nth-child(3) .quick-icon{color:var(--purple);background:rgba(var(--purple-rgb),.11)}.quick-item:nth-child(4) .quick-icon{color:#22d3ee;background:rgba(34,211,238,.1)}.quick-name{font-size:11px;font-weight:800}.quick-desc{font-size:9px;color:var(--t3);margin-top:3px}\n.recent-card{margin-top:14px}.recent-table{width:100%;border-collapse:collapse;font-size:10px}.recent-table th{font-size:9px;padding:10px 12px;background:rgba(255,255,255,.025);color:var(--t3)}.recent-table td{padding:10px 12px;border-top:1px solid rgba(255,255,255,.045);color:var(--t2)}.recent-status{display:inline-flex;align-items:center;gap:5px;color:#34d399}.recent-status i{width:6px;height:6px;border-radius:50%;background:#22c55e;box-shadow:0 0 7px #22c55e}.recent-actions{display:flex;gap:5px}.mini-action{width:27px;height:27px;border:1px solid var(--card-b);border-radius:8px;background:rgba(255,255,255,.025);color:var(--t2);display:grid;place-items:center;cursor:pointer}.mini-action:hover{color:var(--accent2);border-color:rgba(var(--accent2-rgb),.3)}\n.server-info{display:grid;gap:9px}.info-row{display:flex;align-items:center;justify-content:space-between;padding-bottom:9px;border-bottom:1px solid rgba(255,255,255,.045);font-size:10px}.info-row:last-child{border-bottom:0;padding-bottom:0}.info-row span:first-child{color:var(--t3)}.info-row span:last-child{color:var(--t2);font-weight:700}.onex-footer{display:flex;align-items:center;justify-content:space-between;margin-top:14px;padding:12px 4px;color:var(--t3);font-size:9px}.onex-footer b{color:var(--accent2)}\n@keyframes pulseDot{0%,100%{transform:scale(1);opacity:1}50%{transform:scale(.72);opacity:.65}}@keyframes orbitSpin{to{transform:rotate(360deg)}}@keyframes floatY{0%,100%{transform:translateY(0)}50%{transform:translateY(-5px)}}@keyframes healthIn{from{width:0}}\n/* HEXA model applied to the real dashboard */\n:root{--hexa-cyan:var(--accent2);--hexa-violet:var(--purple)}\n.sidebar{background:linear-gradient(180deg,color-mix(in srgb, rgb(24 24 24 / .94) 92%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .96) 92%, var(--accent)));border-left-color:rgba(var(--accent2-rgb),.20);box-shadow:0 18px 55px rgba(0,0,0,.34),inset 0 1px rgba(255,255,255,.05)}\n.sidebar .sb-toggle{left:-20px;width:42px;height:42px;border-radius:50%;background:linear-gradient(145deg,var(--hexa-cyan),var(--hexa-violet));border:3px solid var(--bg);box-shadow:0 0 0 1px rgba(var(--accent2-rgb),.45),0 10px 28px rgba(var(--accent2-rgb),.30);overflow:visible}\n.sidebar .sb-toggle:before{content:"";position:absolute;inset:-6px;border:1px dashed rgba(var(--accent2-rgb),.55);border-radius:50%;animation:hexaOrbit 8s linear infinite}\n.sidebar .sb-toggle .hexa-bars{position:relative;width:20px;height:20px;display:block;z-index:2}\n.sidebar .sb-toggle .hexa-bars i{position:absolute;left:1px;right:1px;height:2.5px;border-radius:3px;background:color-mix(in srgb, rgb(18 18 18) 92%, var(--accent));transition:transform .55s cubic-bezier(.16,1,.3,1),opacity .2s}\n.sidebar .sb-toggle .hexa-bars i:nth-child(1){top:4px}.sidebar .sb-toggle .hexa-bars i:nth-child(2){top:9px}.sidebar .sb-toggle .hexa-bars i:nth-child(3){top:14px}\n.sidebar .sb-toggle svg{display:none}\n.sidebar.collapsed .sb-toggle .hexa-bars i{background:#f7fbff}\n.sidebar.collapsed .sb-toggle .hexa-bars i:nth-child(1){transform:translateY(5px) rotate(45deg)}\n.sidebar.collapsed .sb-toggle .hexa-bars i:nth-child(2){opacity:0}\n.sidebar.collapsed .sb-toggle .hexa-bars i:nth-child(3){transform:translateY(-5px) rotate(-45deg)}\n@keyframes hexaOrbit{to{transform:rotate(360deg)}}\n.onex-topbar{background:linear-gradient(145deg,color-mix(in srgb, rgb(34 34 34 / .88) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .80) 92%, var(--accent)));border-color:rgba(var(--accent2-rgb),.20);box-shadow:0 16px 44px rgba(0,0,0,.28),inset 0 1px rgba(255,255,255,.07)}\n.dashboard-hero{padding:20px;border:1px solid rgba(var(--accent2-rgb),.16);border-radius:22px;background:linear-gradient(145deg,color-mix(in srgb, rgb(28 28 28 / .82) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .72) 92%, var(--accent)));box-shadow:0 16px 42px rgba(0,0,0,.25),inset 0 1px rgba(255,255,255,.05)}\n.dashboard-grid{grid-template-columns:minmax(0,1.72fr) minmax(280px,.82fr);gap:16px}\n.dashboard-grid-right{grid-template-rows:auto minmax(230px,1fr);gap:16px}\n.onex-card,.onex-metric,.telegram-card{border-color:rgba(var(--accent2-rgb),.16);background:linear-gradient(145deg,color-mix(in srgb, rgb(28 28 28 / .88) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .92) 92%, var(--accent)));box-shadow:0 16px 42px rgba(0,0,0,.30),inset 0 1px rgba(255,255,255,.05)}\n.onex-card:hover,.onex-metric:hover{border-color:rgba(var(--accent2-rgb),.34)}\n.onex-control-dock{background:linear-gradient(145deg,color-mix(in srgb, rgb(34 34 34 / .82) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .72) 92%, var(--accent)));border-color:rgba(var(--accent2-rgb),.18)}\n.onex-3d-control .control-icon{background:linear-gradient(145deg,var(--hexa-cyan),var(--hexa-violet))}\n.quick-grid{grid-template-columns:repeat(3,minmax(0,1fr))}\n.hexa-swatches{display:flex;align-items:center;gap:7px;padding:0 6px}\n.hexa-swatches button{width:18px;height:18px;border:0;border-radius:50%;padding:0;background:linear-gradient(135deg,var(--sw-p),var(--sw-s));box-shadow:none;transition:transform .2s,box-shadow .2s}\n.hexa-swatches button[data-p="#38d9ff"]{--sw-p:#38d9ff;--sw-s:#8b5cf6}\n.hexa-swatches button[data-p="#d4f24a"]{--sw-p:#d4f24a;--sw-s:#ffb84d}\n.hexa-swatches button[data-p="#ff3d8b"]{--sw-p:#ff3d8b;--sw-s:#ff8a3d}\n.hexa-swatches button[data-p="#34f5a0"]{--sw-p:#34f5a0;--sw-s:#22c1ee}\n.hexa-swatches button[data-p="#b57bff"]{--sw-p:#b57bff;--sw-s:#ff5ecf}\n.hexa-swatches button:hover{transform:scale(1.15)}\n.hexa-swatches button.active{box-shadow:0 0 0 2px var(--bg),0 0 0 4px var(--t1)}\n@media(min-width:901px){.main{transition:margin-right .35s cubic-bezier(.16,1,.3,1)}.main.expanded{margin-right:var(--sb-c)}}\n@media(max-width:900px){\n  .mob-menu-btn{position:relative;overflow:visible;background:linear-gradient(145deg,var(--hexa-cyan),var(--hexa-violet))!important;border-color:rgba(var(--accent2-rgb),.55)!important}\n  .mob-menu-btn .mobile-bars{position:relative;width:22px;height:22px;display:block}\n  .mob-menu-btn .mobile-bars i{position:absolute;left:1px;right:1px;height:2.5px;border-radius:3px;background:color-mix(in srgb, rgb(18 18 18) 92%, var(--accent));transition:transform .5s cubic-bezier(.16,1,.3,1),opacity .2s}\n  .mob-menu-btn .mobile-bars i:nth-child(1){top:5px}.mob-menu-btn .mobile-bars i:nth-child(2){top:10px}.mob-menu-btn .mobile-bars i:nth-child(3){top:15px}\n  .mob-menu-btn.menu-open .mobile-bars i{background:#fff}\n  .mob-menu-btn.menu-open .mobile-bars i:nth-child(1){transform:translateY(5px) rotate(45deg)}\n  .mob-menu-btn.menu-open .mobile-bars i:nth-child(2){opacity:0}\n  .mob-menu-btn.menu-open .mobile-bars i:nth-child(3){transform:translateY(-5px) rotate(-45deg)}\n  .dashboard-grid{grid-template-columns:1fr}.quick-grid{grid-template-columns:1fr}\n  .hexa-swatches{display:none}\n}\n\n\n\n\n   The mobile drawer is rebuilt independently from the legacy PX\n   sidebar rules. No desktop collapse/width styles are reused.\n   ========================================================= */\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n/* ============================================================\n   ONEX MOBILE FIT — DESKTOP VISUALS, PHONE-SAFE DIMENSIONS\n   Same components, colors and visual language. Only dimensions,\n   columns and spacing change so every element stays in the viewport.\n   ============================================================ */\n@media (max-width:640px){\n  html,body{width:100%;max-width:100%;min-width:0;overflow-x:hidden}\n  body{display:flex;min-height:100vh}\n\n  /* Desktop sidebar kept on the right, proportionally reduced. */\n  :root{--sb:128px;--sb-c:46px;--radius:14px}\n  .sidebar{width:var(--sb);overflow:hidden}\n  .sidebar.collapsed{width:var(--sb-c)}\n  .sb-toggle{width:28px;height:28px;left:-14px;border-radius:8px}\n  .sb-logo{padding:11px 8px}\n  .sb-logo-icon{width:43px;height:43px;border-radius:13px}\n  .sb-logo-icon:after{font-size:20px}\n  .nav{padding:5px 0}\n  .nav-sec{padding:9px 8px 4px;font-size:7px;letter-spacing:.06em}\n  .nav-item{gap:5px;padding:8px 6px;margin:2px 5px;width:calc(100% - 10px);border-radius:9px;font-size:8px;line-height:1.35}\n  .nav-item svg{width:16px;height:16px;min-width:16px;min-height:16px}\n  .sb-foot{padding:7px;gap:5px}\n  .sb-foot button,.sb-foot a.btn{padding:8px 4px;border-radius:9px;font-size:8px;gap:4px}\n  .sb-foot svg{width:14px;height:14px}\n\n  .main{width:auto;min-width:0;margin-right:var(--sb);padding:12px 9px 38px}\n  .main.expanded{margin-right:var(--sb-c)}\n  .mob-bar,.overlay{display:none!important}\n\n  /* Top bar: no item is allowed to force the main column wider. */\n  .onex-topbar{height:55px;margin-bottom:10px;padding:7px 8px;border-radius:13px;gap:5px;min-width:0}\n  .top-server{gap:5px;min-width:0;overflow:hidden}.top-dot{width:7px;height:7px;flex:0 0 auto}\n  .top-server b{font-size:9px;white-space:nowrap}.top-server small{font-size:6.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.top-sep{height:18px;flex:0 0 auto}\n  .top-actions{gap:3px;flex:0 0 auto}.top-chip{padding:5px 6px;border-radius:8px;font-size:6.5px;gap:3px}.top-avatar{width:28px;height:28px;border-radius:8px;font-size:8px}\n\n  .onex-control-dock{grid-template-columns:repeat(3,minmax(0,1fr));gap:5px;margin:0 0 10px;padding:5px;border-radius:13px;min-width:0}\n  .onex-3d-control{min-width:0;min-height:52px;border-radius:10px;gap:3px;padding:6px 3px;overflow:hidden}\n  .onex-3d-control .control-icon{width:25px;height:25px;flex:0 0 25px;border-radius:8px}\n  .onex-3d-control svg{width:13px;height:13px}.onex-3d-control .control-copy{min-width:0}.onex-3d-control .control-title{font-size:7px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.onex-3d-control .control-sub{font-size:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n\n  .dashboard-hero{display:flex;align-items:flex-end;justify-content:space-between;gap:5px;margin:0 1px 10px;min-width:0}\n  .dashboard-hero>div:first-child{min-width:0}.hero-kicker{font-size:7px;letter-spacing:.14em}.hero-title{font-size:18px;margin-top:3px;line-height:1.25}.hero-sub{font-size:7px;margin-top:3px;white-space:nowrap}.hero-actions{gap:4px;flex:0 0 auto}\n  .hero-actions .btn{padding:6px 7px;font-size:7px;border-radius:8px;white-space:nowrap}.hero-actions .btn svg{width:11px;height:11px}\n\n  /* Two columns preserve the desktop card style without crushing text. */\n  .onex-metrics{grid-template-columns:repeat(2,minmax(0,1fr));gap:6px;margin-bottom:10px}\n  .onex-metric{min-width:0;min-height:88px;padding:8px;border-radius:13px}\n  .metric-icon{width:27px;height:27px;border-radius:9px}.metric-icon svg{width:14px;height:14px}\n  .onex-metric .metric-label{margin:6px 0 2px;font-size:7px;line-height:1.35}.onex-metric .metric-val{font-size:15px;white-space:nowrap}.metric-trend{left:8px;bottom:7px;font-size:6px;white-space:nowrap}\n\n  /* Same desktop dashboard cards, stacked only because the phone is narrow. */\n  .dashboard-grid{grid-template-columns:minmax(0,1fr);gap:8px}\n  .dashboard-grid-right{grid-template-rows:auto auto;gap:8px}\n  .onex-card{min-width:0;border-radius:13px}.onex-card-head{gap:5px;padding:9px 10px}.onex-card-title{gap:5px;font-size:8px}.onex-card-body{padding:9px 10px}\n  .chart-wrap{height:145px;padding:5px 6px 8px}.chart-labels{padding:0 5px;font-size:6px}.chart-badge{top:9px;padding:4px 5px;font-size:5.5px}.range-mini{gap:2px;padding:2px;border-radius:7px}.range-mini button{font-size:6px;padding:4px 6px;border-radius:6px}\n\n  .health-list{gap:8px}.health-row{grid-template-columns:24px minmax(0,1fr) 28px;gap:6px}.health-icon{width:24px;height:24px;border-radius:8px}.health-icon svg{width:12px;height:12px}.health-name{font-size:7px;margin-bottom:3px}.health-pct{font-size:6.5px}.health-track{height:5px}.xray-state{padding:7px 8px;border-radius:8px;font-size:7px}\n  .telegram-card{padding:10px;border-radius:13px}.tg-orbit{width:68px;height:68px;margin:3px auto 7px}.tg-logo{width:40px;height:40px}.tg-logo svg{width:21px;height:21px}.tg-title{font-size:8px}.tg-handle{font-size:14px}.tg-desc{font-size:6px}.tg-btn{margin-top:8px;padding:7px;border-radius:8px;font-size:7px}\n  .quick-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:6px}.quick-item{gap:6px;padding:8px;border-radius:10px;min-width:0}.quick-icon{width:28px;height:28px;border-radius:8px;flex:0 0 28px}.quick-icon svg{width:14px;height:14px}.quick-name{font-size:7px}.quick-desc{font-size:6px;margin-top:1px}\n  .recent-card{margin-top:8px}.recent-table{font-size:7px}.recent-table th{font-size:6px;padding:7px 6px}.recent-table td{padding:7px 6px}.mini-action{width:21px;height:21px;border-radius:6px}\n  .server-info{gap:6px}.info-row{padding-bottom:6px;font-size:7px}.onex-footer{margin-top:8px;padding:7px 2px;font-size:6px}\n\n  /* Other pages: same desktop components, safely resized. */\n  .page-head{gap:7px;margin-bottom:10px}.page-title{font-size:14px;gap:5px}.page-title svg{width:16px;height:16px}.page-sub{font-size:7px;margin-top:3px}\n  .metrics{grid-template-columns:repeat(2,minmax(0,1fr));gap:6px;margin-bottom:10px}.metric{padding:9px;border-radius:12px;min-width:0}.metric-label{font-size:7px;margin-bottom:4px}.metric-val{font-size:15px}\n  .card{padding:10px;border-radius:12px;margin-bottom:8px}.card-title{font-size:8px;margin-bottom:8px}.card-title svg{width:13px;height:13px}.g2{grid-template-columns:1fr;gap:8px;margin-bottom:8px}\n  .support-grid{grid-template-columns:1fr 1fr;gap:6px}.support-tile{gap:6px;padding:8px;border-radius:10px}.support-icon{width:29px;height:29px;border-radius:8px}.support-icon svg{width:14px;height:14px}.support-label{font-size:6.5px}.support-val{font-size:8px;margin-top:2px}\n  #page-logs .logs-page-sub{font-size:7px}\n  #page-logs .logs-toolbar{padding:8px;border-radius:12px}\n  #page-logs .logs-search-wrap{height:38px;border-radius:10px;padding:0 9px;gap:7px}\n  #page-logs .logs-search-wrap>svg{width:16px;height:16px}\n  #page-logs .logs-search-wrap input{font-size:9px}\n  #page-logs .logs-filter-row{padding-top:7px;gap:4px}\n  #page-logs .logs-filter{padding:6px 8px;border-radius:8px;font-size:7px}\n  #page-logs .logs-filter b{font-size:7px}\n  #page-logs .logs-advanced-row{grid-template-columns:1fr 1fr;gap:5px}\n  #page-logs .logs-advanced-row label:last-of-type{grid-column:2}\n  #page-logs .logs-reset-filter{grid-column:1 / -1;width:100%;font-size:8px}\n  #page-logs .logs-summary{grid-template-columns:repeat(2,minmax(0,1fr));border-radius:12px}\n  #page-logs .logs-summary-item{padding:9px;gap:7px}\n  #page-logs .logs-summary-item:nth-child(odd){border-right:1px solid var(--card-b)}\n  #page-logs .logs-summary-item:nth-child(n+3){border-top:1px solid var(--card-b)}\n  #page-logs .logs-summary-icon{width:27px;height:27px;border-radius:8px;flex-basis:27px;font-size:12px}\n  #page-logs .logs-summary-item small{font-size:7px}\n  #page-logs .logs-summary-item strong{font-size:14px}\n  #page-logs .logs-summary-item em{font-size:6px}\n  #page-logs .logs-list-head{padding:9px 10px}\n  #page-logs .logs-list-head b{font-size:9px}\n  #page-logs .logs-list-head small{font-size:6px}\n  #page-logs .logs-list{padding:0 10px}\n  #page-logs .logs-event{grid-template-columns:27px minmax(0,1fr) auto 12px;gap:6px;padding:9px 0}\n  #page-logs .logs-event-icon{width:27px;height:27px;border-radius:8px;font-size:11px}\n  #page-logs .logs-event-title{font-size:8px}\n  #page-logs .logs-event-desc{font-size:6px}\n  #page-logs .logs-badge{padding:3px 5px;font-size:6px}\n  #page-logs .logs-event-time{font-size:6px}\n  #page-logs .logs-event-arrow{font-size:13px}\n  #page-logs .logs-footer{padding:8px 9px}\n  #page-logs .logs-advanced-btn,#page-logs .logs-clear-btn{font-size:7px;padding:7px 8px;border-radius:8px}\n  #page-logs .logs-clear-btn svg{width:12px;height:12px}\n  #page-logs .logs-detail-modal{padding:11px;border-radius:15px}\n  #page-logs .logs-detail-head b{font-size:10px}\n  #page-logs .logs-detail-head small{font-size:7px}\n  #page-logs .logs-detail-row{grid-template-columns:65px minmax(0,1fr);padding:7px 0}\n  #page-logs .logs-detail-row small{font-size:7px}\n  #page-logs .logs-detail-row b{font-size:8px}\n  .log-item{padding:7px 0;font-size:7px;gap:6px}.log-time{font-size:6px;min-width:44px}.btn{padding:7px 8px;border-radius:8px;font-size:7px}.btn-sm{padding:5px 6px;font-size:6.5px}.btn svg{width:11px;height:11px}\n  .form-row{grid-template-columns:1fr;gap:0}.field{margin-bottom:8px}.field label{font-size:7px;margin-bottom:4px}.field input,.field select,.field textarea{padding:8px;border-radius:8px;font-size:11px;min-height:34px}\n  .table-wrap{width:100%;max-width:100%;overflow-x:auto}table{font-size:7px;min-width:420px}th{padding:7px 6px}td{padding:7px 6px}.ops{gap:3px}.range-tab{padding:5px 6px;font-size:6.5px;border-radius:7px}\n  .modal-bg{padding:8px}.modal{width:calc(100vw - 16px);max-width:460px;max-height:92vh;padding:12px;border-radius:13px}.modal-title{font-size:12px;margin-bottom:9px}.modal-actions{gap:5px;margin-top:9px}.link-box{padding:8px;font-size:7px;margin:6px 0 8px;max-height:70px}.toast{bottom:10px;padding:8px 11px;border-radius:9px;font-size:8px}\n}\n\n@media (max-width:520px){\n  :root{--sb:120px;--sb-c:44px}\n  .main{margin-right:var(--sb);padding-left:7px;padding-right:7px}\n  .nav-item{font-size:7.5px;padding:8px 5px}\n  .onex-topbar{height:52px;padding:6px 7px}.top-chip{display:none}.top-server small{max-width:65px}\n  .onex-control-dock{gap:4px}.onex-3d-control{min-height:49px;padding:5px 3px}.onex-3d-control .control-icon{width:23px;height:23px;flex-basis:23px}.onex-3d-control .control-title{font-size:6.5px}.onex-3d-control .control-sub{font-size:4.5px}\n  .hero-title{font-size:16px}.hero-kicker{font-size:6.5px}.hero-sub{font-size:6.5px}.hero-actions .btn{padding:5px 6px;font-size:6.5px}\n  .onex-metric{min-height:84px;padding:7px}.onex-metric .metric-label{font-size:6.5px}.onex-metric .metric-val{font-size:14px}.metric-trend{font-size:5.5px}\n}\n\n@media (max-width:380px){\n  :root{--sb:112px;--sb-c:42px}\n  .main{padding-left:6px;padding-right:6px}\n  .nav-item{font-size:7px}.onex-metrics{gap:5px}.onex-metric{min-height:80px;padding:6px}.onex-metric .metric-val{font-size:13px}\n  .dashboard-hero{gap:3px}.hero-title{font-size:15px}.hero-actions .btn{padding:5px;font-size:6px}\n}\n\n/* ============================================================\n   ONEX PHONE MODE — FULL WIDTH CONTENT + SLIDE-IN NAV DRAWER\n   Keeps the exact panel/components; only phone layout changes.\n   ============================================================ */\n@media (max-width:640px){\n  html,body{width:100%;min-width:0;overflow-x:hidden}\n  body{display:block;min-height:100vh;padding-top:58px}\n\n  .mob-bar{display:flex!important;position:fixed;top:0;left:0;right:0;height:58px;padding:0 12px;\n    background:color-mix(in srgb, rgb(12 12 12 / .96) 92%, var(--accent));border-bottom:1px solid var(--card-b);\n    backdrop-filter:blur(16px);box-shadow:0 8px 30px rgba(0,0,0,.35);z-index:1000}\n  .mob-menu-btn{width:42px;height:42px;border:1px solid var(--card-b);border-radius:12px;\n    background:var(--bg3);color:var(--t1);display:flex;align-items:center;justify-content:center;cursor:pointer}\n  .mob-menu-btn svg{width:22px;height:22px}\n  .mob-brand{display:flex;align-items:center;gap:9px;margin-right:auto;margin-left:auto;min-width:0}\n  .mob-brand-icon{width:36px;height:36px;border-radius:11px;display:grid;place-items:center;flex:0 0 36px;\n    background:linear-gradient(145deg,var(--accent),var(--accent) 50%,var(--purple));font:900 18px Inter,sans-serif;color:#fff;\n    box-shadow:0 7px 20px rgba(var(--accent-rgb),.35)}\n  .mob-brand-text{min-width:0;line-height:1.05}\n  .mob-brand-text b{display:block;font:700 12px Inter,sans-serif;white-space:nowrap}\n  .mob-brand-text span{display:block;font-size:8px;color:var(--t3);margin-top:3px;white-space:nowrap}\n  .mob-status{display:flex;align-items:center;gap:5px;font-size:8px;color:var(--t2);white-space:nowrap}\n  .mob-status i{width:7px;height:7px;border-radius:50%;background:#22c55e;box-shadow:0 0 9px #22c55e}\n\n  .sidebar{position:fixed;top:0;right:0;bottom:0;width:min(84vw,320px)!important;\n    max-width:320px;transform:translateX(105%);transition:transform .25s ease;width:min(84vw,320px);\n    z-index:1200;box-shadow:-18px 0 50px rgba(0,0,0,.55);overflow-y:auto}\n  .sidebar.mobile-open{transform:translateX(0)}\n  .sidebar.collapsed{width:min(84vw,320px)!important}\n  .sidebar .sb-toggle{display:none}\n  .sidebar .sb-logo{padding:18px 14px}\n  .sidebar .sb-logo-icon{width:54px;height:54px;border-radius:16px}\n  .sidebar .sb-logo-icon:after{font-size:25px}\n  .sidebar .nav-item{font-size:13px;padding:11px 16px;margin:2px 10px;width:calc(100% - 20px);gap:11px;border-radius:12px}\n  .sidebar .nav-item svg{width:18px;height:18px;min-width:18px}\n  .sidebar .nav-sec{padding:14px 18px 6px;font-size:9px}\n  .sidebar .sb-foot{padding:12px}\n  .sidebar .sb-foot button,.sidebar .sb-foot a.btn{font-size:12px;padding:10px}\n  .sidebar .nav-label,.sidebar .sb-foot span,.sidebar .nav-sec,.sidebar .sb-logo-text{display:block!important}\n  .sidebar.collapsed .nav-label,.sidebar.collapsed .sb-foot span,.sidebar.collapsed .nav-sec,.sidebar.collapsed .sb-logo-text{display:block!important}\n  .overlay{display:none;position:fixed;inset:0;background:rgba(0,0,0,.58);z-index:1100}\n  .overlay.show{display:block!important}\n\n  .main,.main.expanded{width:100%;max-width:100%;min-width:0;margin:0!important;padding:12px 12px 40px}\n  .page{width:100%;max-width:100%;min-width:0;overflow:visible}\n  .page-head{width:100%;max-width:100%;align-items:flex-start}\n  .page-title{font-size:21px;line-height:1.35}\n  .page-sub{font-size:11px;line-height:1.7;max-width:100%}\n\n  .onex-topbar{width:100%;max-width:100%;height:auto;min-height:64px;padding:10px 11px;margin-bottom:10px}\n  .top-server{min-width:0;flex:1}\n  .top-server b{font-size:12px}\n  .top-server small{font-size:8px;max-width:120px}\n  .top-actions{flex:0 0 auto}\n  .top-chip{font-size:8px;padding:6px 7px}\n  .top-avatar{width:34px;height:34px;font-size:10px}\n\n  .onex-control-dock{width:100%;grid-template-columns:repeat(3,minmax(0,1fr));gap:7px;padding:7px;margin-bottom:14px}\n  .onex-3d-control{min-height:68px;padding:8px 6px;gap:6px}\n  .onex-3d-control .control-icon{width:31px;height:31px;flex-basis:31px}\n  .onex-3d-control svg{width:16px;height:16px}\n  .onex-3d-control .control-title{font-size:9px}\n  .onex-3d-control .control-sub{font-size:6px}\n\n  .dashboard-hero{width:100%;margin-bottom:14px;gap:8px;align-items:stretch;grid-template-columns:minmax(0,1fr) auto;grid-template-areas:"hero version" "actions actions"}\n  .dashboard-hero .hero-main{grid-area:hero;align-self:center}.hero-version-strip{grid-area:version;display:flex;flex-direction:column;gap:6px;align-self:stretch}.version-mini-card{min-width:116px;min-height:47px;padding:6px 7px;border-radius:12px;gap:6px}.version-mini-icon{width:25px;height:25px;flex-basis:25px;border-radius:8px;font-size:11px}.version-mini-copy b{font-size:7px}.version-mini-copy strong{font-size:11px}.version-live-dot{width:6px;height:6px;flex-basis:6px}.dashboard-hero .hero-actions{grid-area:actions;width:100%;justify-content:stretch}\n  .hero-kicker{font-size:9px}.hero-title{font-size:24px}.hero-sub{font-size:9px;white-space:normal;line-height:1.5}\n  .hero-actions{flex-wrap:wrap;justify-content:flex-end}.hero-actions .btn{font-size:9px;padding:8px 10px}\n\n  .onex-metrics,.metrics{width:100%;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}\n  .onex-metric{min-height:112px;padding:11px}.onex-metric .metric-label{font-size:9px}.onex-metric .metric-val{font-size:20px}\n  .metric{padding:12px;min-width:0}.metric-label{font-size:9px}.metric-val{font-size:20px}\n\n  .dashboard-grid,.g2{width:100%;grid-template-columns:1fr;gap:10px}\n  .dashboard-grid-right{grid-template-rows:auto}\n  .card,.onex-card,.telegram-card{width:100%;max-width:100%;min-width:0}\n  .card{padding:14px;border-radius:15px}\n  .onex-card-head{padding:12px 13px}.onex-card-body{padding:12px 13px}\n  .onex-card-title{font-size:11px}.chart-wrap{height:190px}\n\n  .form-row{grid-template-columns:1fr!important;gap:0}\n  .field{min-width:0}.field label{font-size:10px}.field input,.field select,.field textarea{width:100%;min-width:0;min-height:46px;padding:10px 12px;font-size:16px;border-radius:11px}\n  .btn{min-height:44px;font-size:10px;padding:9px 12px}.btn-sm{min-height:38px}\n  .support-grid{grid-template-columns:1fr!important}\n  .quick-grid{grid-template-columns:1fr 1fr}\n  .table-wrap{width:100%;max-width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}\n  table{min-width:600px;font-size:10px}\n  th,td{padding:10px 9px}\n  .range-tabs{width:100%;display:grid;grid-template-columns:repeat(4,1fr);gap:5px}\n  .range-tab{width:100%;padding:8px 5px;font-size:9px}\n  .modal-bg{padding:10px}.modal{width:calc(100vw - 20px);max-width:none;max-height:88vh;overflow:auto}\n  .toast{max-width:calc(100vw - 24px);font-size:10px;text-align:center}\n}\n\n@media (max-width:480px){\n  body{padding-top:56px}\n  .mob-bar{height:56px;padding:0 9px}\n  .mob-menu-btn{width:40px;height:40px}\n  .mob-brand-icon{width:32px;height:32px;flex-basis:32px;font-size:16px}\n  .mob-brand-text b{font-size:11px}.mob-brand-text span{font-size:7px}\n  .mob-status{font-size:7px}\n  .main,.main.expanded{padding:10px 9px 34px}\n  .page-title{font-size:20px}.page-sub{font-size:10px}\n  .onex-topbar{min-height:60px;padding:8px}.top-chip{display:none}.top-server b{font-size:11px}.top-server small{font-size:7px;max-width:95px}\n  .onex-control-dock{gap:5px;padding:5px}.onex-3d-control{min-height:61px;padding:7px 4px}.onex-3d-control .control-icon{width:28px;height:28px;flex-basis:28px}.onex-3d-control .control-title{font-size:8px}.onex-3d-control .control-sub{font-size:5px}\n  .hero-title{font-size:22px}.hero-kicker{font-size:8px}.version-mini-card{min-width:104px;min-height:44px;padding:5px}.version-mini-copy b{font-size:6.5px}.version-mini-copy strong{font-size:10px}.dashboard-hero .hero-actions{width:100%;justify-content:stretch}.hero-actions .btn{flex:1;font-size:9px}\n  .onex-metric{min-height:100px;padding:9px}.onex-metric .metric-label{font-size:8px}.onex-metric .metric-val{font-size:18px}\n  .card{padding:12px}.card-title{font-size:10px}.quick-grid{grid-template-columns:1fr}\n  .table-wrap table{min-width:560px}\n}\n\n\n/* ============================================================\n   ONEX 3D NAV ICONS — polished animated icon set\n   ============================================================ */\n.nav-item .nav-ico{\n  width:21px;height:21px;min-width:21px;min-height:21px;flex:0 0 21px;\n  overflow:visible;transform-origin:center;filter:drop-shadow(0 2px 4px rgba(0,0,0,.45));\n  transition:transform .28s cubic-bezier(.2,.8,.2,1),filter .28s,color .28s;\n}\n.nav-item .nav-ico path,.nav-item .nav-ico circle,.nav-item .nav-ico rect{vector-effect:non-scaling-stroke}\n.nav-item:hover .nav-ico{transform:perspective(80px) rotateY(-12deg) rotateX(8deg) translateY(-1px) scale(1.08);filter:drop-shadow(0 4px 7px rgba(var(--accent-rgb),.42))}\n.nav-item.on .nav-ico{transform:perspective(90px) rotateY(-10deg) rotateX(6deg) scale(1.06);filter:drop-shadow(0 3px 8px rgba(var(--accent-rgb),.58));animation:navIconFloat 2.8s ease-in-out infinite}\n.nav-item.on .nav-ico-dash{animation:navIconPulse 2.6s ease-in-out infinite}\n.nav-item.on .nav-ico-telegram{animation:navIconSpinSoft 4s ease-in-out infinite}\n.nav-item.on .nav-ico-settings{animation:navIconSpin 5s linear infinite}\n.nav-item .nav-ico-create{transform:rotate(-3deg)}\n.nav-item:hover .nav-ico-create{transform:perspective(80px) rotateY(-14deg) rotateX(8deg) rotate(-7deg) scale(1.1)}\n@keyframes navIconFloat{0%,100%{translate:0 0}50%{translate:0 -2px}}\n@keyframes navIconPulse{0%,100%{filter:drop-shadow(0 3px 7px rgba(var(--accent-rgb),.35))}50%{filter:drop-shadow(0 5px 13px rgba(var(--accent-rgb),.75))}}\n@keyframes navIconSpin{from{rotate:0deg}to{rotate:360deg}}\n@keyframes navIconSpinSoft{0%,100%{rotate:0deg}35%{rotate:-7deg}65%{rotate:7deg}}\n.logout-ico{width:20px!important;height:20px!important;filter:drop-shadow(0 2px 4px rgba(239,68,68,.25));transition:transform .3s cubic-bezier(.2,.8,.2,1),filter .3s}\n.sb-foot a.danger:hover .logout-ico,.sb-foot button.danger:hover .logout-ico{transform:perspective(80px) rotateY(-16deg) rotateX(8deg) scale(1.12) translateX(-2px);filter:drop-shadow(0 4px 9px rgba(239,68,68,.58))}\n.sb-foot a.danger .logout-ico,.sb-foot button.danger .logout-ico{animation:logoutFloat 2.8s ease-in-out infinite}\n@keyframes logoutFloat{0%,100%{translate:0 0}50%{translate:-2px -1px}}\n@media (max-width:640px){\n  .sidebar .nav-item .nav-ico{width:22px;height:22px;min-width:22px;min-height:22px;flex-basis:22px}\n  .sidebar .nav-item.on .nav-ico{transform:perspective(80px) rotateY(-8deg) rotateX(5deg) scale(1.04)}\n  .sidebar .sb-foot a.danger .logout-ico,.sidebar .sb-foot button.danger .logout-ico,.sb-foot button.danger .logout-ico{width:22px!important;height:22px!important}\n}\n\n\n/* ============================================================\n   ONEX MOBILE DRAWER — SAME GLASS AS LOGIN CARD\n   The opened dashboard menu intentionally uses the exact same\n   glass recipe as .login-card for a consistent visual language.\n   ============================================================ */\n.sidebar{\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .78) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .68) 92%, var(--accent))) !important;\n  border-left:1px solid rgba(var(--accent2-rgb),.14) !important;\n  box-shadow:0 18px 55px rgba(0,0,0,.48),0 0 70px rgba(var(--accent-rgb),.10) !important;\n  backdrop-filter:blur(25px) !important;\n  -webkit-backdrop-filter:blur(25px) !important;\n  overflow:hidden;\n}\n.sidebar::before{\n  content:"";position:absolute;inset:0;pointer-events:none;z-index:0;\n  background:\n    radial-gradient(circle at 15% 18%,rgba(var(--accent-rgb),.10),transparent 30%),\n    linear-gradient(145deg,rgba(255,255,255,.035),transparent 42%,rgba(var(--accent-rgb),.055));\n}\n.sidebar > *{position:relative;z-index:1;}\n@media (max-width:640px){\n  .sidebar{\n    position:fixed !important; top:0 !important; right:0 !important; bottom:0 !important; left:auto !important;\n    width:min(86vw,340px) !important; max-width:340px !important; min-width:0 !important;\n    transform:translateX(105%) !important;\n    transition:transform .24s cubic-bezier(.2,.8,.2,1) !important;\n    background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .78) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .68) 92%, var(--accent))) !important;\n    border-left:1px solid rgba(var(--accent2-rgb),.22) !important;\n    border-right:0 !important;\n    box-shadow:0 30px 100px rgba(0,0,0,.55),0 0 80px rgba(var(--accent-rgb),.10) !important;\n    backdrop-filter:blur(25px) !important;\n    -webkit-backdrop-filter:blur(25px) !important;\n    overflow-y:auto !important;\n  }\n  .sidebar.mobile-open{transform:translateX(0) !important}\n  .sidebar.collapsed{width:min(86vw,340px) !important}\n  .mob-bar{display:flex !important;position:fixed !important;top:0;left:0;right:0;height:58px;z-index:1250;\n    background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .78) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .68) 92%, var(--accent))) !important;\n    border-bottom:1px solid rgba(var(--accent2-rgb),.22) !important;\n    backdrop-filter:blur(25px) !important;-webkit-backdrop-filter:blur(25px) !important;\n    box-shadow:0 12px 35px rgba(0,0,0,.35),0 0 40px rgba(var(--accent-rgb),.08) !important;\n  }\n  .overlay{display:none !important;position:fixed !important;inset:0 !important;background:color-mix(in srgb, rgb(6 6 6 / .62) 92%, var(--accent)) !important;backdrop-filter:blur(3px);z-index:1150 !important}\n  .overlay.show{display:block !important}\n  .main,.main.expanded{width:100% !important;max-width:100% !important;margin:0 !important;padding:70px 12px 40px !important}\n}\n\n\n\n/* ============================================================\n   ONEX GLOBAL THEME SYSTEM — ALL PANEL PAGES\n   One visual language for Dashboard / Configs / Create / Stats /\n   Logs / Settings / Telegram / News / Admins / Modals / Drawer.\n   Dark = ONEX login glass. Light = clean full-white UI.\n   ============================================================ */\n\n/* ---------- DARK: same glass language as LOGIN ---------- */\nhtml:not(.light) body{\n  background:\n    radial-gradient(circle at 18% 22%,rgba(var(--accent-rgb),.17),transparent 28%),\n    radial-gradient(circle at 85% 15%,rgba(var(--accent-rgb),.12),transparent 24%),\n    linear-gradient(145deg,color-mix(in srgb, rgb(7 7 7) 92%, var(--accent)) 0%,color-mix(in srgb, rgb(18 18 18) 92%, var(--accent)) 52%,color-mix(in srgb, rgb(5 5 5) 92%, var(--accent)) 100%) !important;\n}\nhtml:not(.light) body::before{\n  background:\n    radial-gradient(ellipse 80% 50% at 100% 0%,rgba(var(--accent-rgb),.14),transparent 50%),\n    radial-gradient(ellipse 60% 40% at 0% 100%,rgba(var(--purple-rgb),.09),transparent 45%) !important;\n}\nhtml:not(.light) .sidebar,\nhtml:not(.light) .mob-bar{\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .82) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .72) 92%, var(--accent))) !important;\n  border-color:rgba(var(--accent2-rgb),.22) !important;\n  box-shadow:0 30px 100px rgba(0,0,0,.45),0 0 80px rgba(var(--accent-rgb),.08),inset 0 1px rgba(255,255,255,.08) !important;\n  backdrop-filter:blur(25px) saturate(125%) !important;\n  -webkit-backdrop-filter:blur(25px) saturate(125%) !important;\n}\nhtml:not(.light) .onex-topbar,\nhtml:not(.light) .onex-control-dock,\nhtml:not(.light) .onex-card,\nhtml:not(.light) .onex-metric,\nhtml:not(.light) .card,\nhtml:not(.light) .metric,\nhtml:not(.light) .support-tile,\nhtml:not(.light) .modal,\nhtml:not(.light) .toast{\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .78) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .68) 92%, var(--accent))) !important;\n  border-color:rgba(var(--accent2-rgb),.22) !important;\n  box-shadow:0 18px 50px rgba(0,0,0,.34),inset 0 1px rgba(255,255,255,.075),inset 0 0 38px rgba(var(--accent-rgb),.045) !important;\n  backdrop-filter:blur(25px) saturate(125%) !important;\n  -webkit-backdrop-filter:blur(25px) saturate(125%) !important;\n}\nhtml:not(.light) .onex-control-dock{\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .70) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .58) 92%, var(--accent))) !important;\n}\nhtml:not(.light) .quick-item,\nhtml:not(.light) .top-chip,\nhtml:not(.light) .range-tabs,\nhtml:not(.light) .range-mini,\nhtml:not(.light) .mini-action,\nhtml:not(.light) .sub-box,\nhtml:not(.light) .link-box,\nhtml:not(.light) .table-wrap{\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(19 19 19 / .60) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .48) 92%, var(--accent))) !important;\n  border-color:rgba(var(--accent2-rgb),.16) !important;\n  box-shadow:inset 0 1px rgba(255,255,255,.055),inset 0 0 28px rgba(var(--accent-rgb),.035) !important;\n}\nhtml:not(.light) .field input,\nhtml:not(.light) .field select,\nhtml:not(.light) .field textarea,\nhtml:not(.light) #cfgSearch{\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(10 10 10 / .68) 92%, var(--accent)),color-mix(in srgb, rgb(13 13 13 / .52) 92%, var(--accent))) !important;\n  color:#f8fbff !important;\n  border-color:rgba(var(--accent2-rgb),.20) !important;\n  box-shadow:inset 0 1px rgba(255,255,255,.035) !important;\n}\nhtml:not(.light) th{background:color-mix(in srgb, rgb(10 10 10 / .58) 92%, var(--accent)) !important;color:rgba(226,238,255,.62) !important}\nhtml:not(.light) td{border-color:rgba(var(--accent2-rgb),.11) !important}\nhtml:not(.light) tr:hover td{background:rgba(var(--accent-rgb),.055) !important}\nhtml:not(.light) .onex-card-head{border-color:rgba(var(--accent2-rgb),.12) !important}\nhtml:not(.light) .sb-foot{border-color:rgba(var(--accent2-rgb),.16) !important}\nhtml:not(.light) .sb-foot button,\nhtml:not(.light) .sb-foot a.btn,\nhtml:not(.light) .btn:not(.btn-p):not(.btn-d){\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .70) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .58) 92%, var(--accent))) !important;\n  border-color:rgba(var(--accent2-rgb),.18) !important;\n  color:var(--t2) !important;\n}\nhtml:not(.light) .modal-bg{background:color-mix(in srgb, rgb(4 4 4 / .68) 92%, var(--accent)) !important;backdrop-filter:blur(9px) !important}\nhtml:not(.light) .nav-item:hover,\nhtml:not(.light) .nav-item.on{background:rgba(var(--accent-rgb),.10) !important}\n\n/* ---------- LIGHT: genuinely white, everywhere ---------- */\nhtml.light body{\n  background:#ffffff !important;\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;\n}\nhtml.light body::before{background:none !important;opacity:0 !important}\nhtml.light .sidebar,\nhtml.light .mob-bar,\nhtml.light .main,\nhtml.light .onex-topbar,\nhtml.light .onex-control-dock,\nhtml.light .onex-card,\nhtml.light .onex-metric,\nhtml.light .card,\nhtml.light .metric,\nhtml.light .support-tile,\nhtml.light .modal,\nhtml.light .toast,\nhtml.light .quick-item,\nhtml.light .table-wrap,\nhtml.light .sub-box,\nhtml.light .link-box{\n  background:#ffffff !important;\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;\n  border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent)) !important;\n  box-shadow:0 10px 30px color-mix(in srgb, rgb(23 23 23 / .07) 92%, var(--accent)),inset 0 1px rgba(255,255,255,.95) !important;\n  backdrop-filter:none !important;\n  -webkit-backdrop-filter:none !important;\n}\nhtml.light .onex-control-dock{background:#ffffff !important}\nhtml.light .onex-3d-control{\n  background:linear-gradient(145deg,#ffffff,#f5f8fc) !important;\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;\n  border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent)) !important;\n  box-shadow:0 7px 0 color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent)),0 12px 25px color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent)),inset 0 1px #fff !important;\n}\nhtml.light .top-chip,\nhtml.light .range-tabs,\nhtml.light .range-mini,\nhtml.light .mini-action{\n  background:#ffffff !important;\n  color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent)) !important;\n  border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent)) !important;\n}\nhtml.light .field input,\nhtml.light .field select,\nhtml.light .field textarea,\nhtml.light #cfgSearch{\n  background:#ffffff !important;\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;\n  border-color:color-mix(in srgb, rgb(23 23 23 / .14) 92%, var(--accent)) !important;\n  box-shadow:inset 0 1px 2px color-mix(in srgb, rgb(23 23 23 / .025) 92%, var(--accent)) !important;\n}\nhtml.light .field input::placeholder,\nhtml.light .field textarea::placeholder{color:color-mix(in srgb, rgb(161 161 161) 82%, var(--accent)) !important}\nhtml.light th{background:#ffffff !important;color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent)) !important}\nhtml.light td{border-color:color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent)) !important;color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent)) !important}\nhtml.light tr:hover td{background:#f8fafc !important}\nhtml.light .onex-card-head{border-color:color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent)) !important}\nhtml.light .sb-foot{border-color:color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent)) !important}\nhtml.light .sb-foot button,\nhtml.light .sb-foot a.btn,\nhtml.light .btn:not(.btn-p):not(.btn-d){\n  background:#ffffff !important;\n  color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent)) !important;\n  border-color:color-mix(in srgb, rgb(23 23 23 / .12) 92%, var(--accent)) !important;\n}\nhtml.light .nav-item{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent)) !important}\nhtml.light .nav-item:hover{background:#f1f5f9 !important;color:var(--accent) !important}\nhtml.light .nav-item.on{background:#eff6ff !important;color:var(--accent) !important;box-shadow:inset -3px 0 0 var(--accent) !important}\nhtml.light .page-title,\nhtml.light .card-title,\nhtml.light .onex-card-title,\nhtml.light .quick-name,\nhtml.light .support-val,\nhtml.light .metric-val,\nhtml.light .onex-metric .metric-val{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important}\nhtml.light .page-sub,\nhtml.light .field label,\nhtml.light .metric-label,\nhtml.light .quick-desc,\nhtml.light .support-label,\nhtml.light .log-time,\nhtml.light .health-name,\nhtml.light .health-pct{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent)) !important}\nhtml.light .log-msg{color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent)) !important}\nhtml.light .modal-bg{background:color-mix(in srgb, rgb(23 23 23 / .30) 92%, var(--accent)) !important;backdrop-filter:blur(7px) !important}\nhtml.light .toast{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important}\n\n/* Inline utility backgrounds used by the secondary pages: neutralize them\n   so every page follows the selected global theme rather than its old color. */\nhtml.light .page [style*="background:rgba(255"],\nhtml.light .page [style*="background: rgba(255"],\nhtml.light .page [style*="background:#0"],\nhtml.light .page [style*="background: #0"]{background:#ffffff !important}\nhtml:not(.light) .page [style*="background:rgba(255"],\nhtml:not(.light) .page [style*="background: rgba(255"],\nhtml:not(.light) .page [style*="background:#0"],\nhtml:not(.light) .page [style*="background: #0"]{\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(19 19 19 / .60) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .48) 92%, var(--accent))) !important;\n}\n\n/* Keep the primary/danger actions visually meaningful in both themes. */\n.btn-p{color:#fff !important}\n.btn-d{color:#dc2626 !important}\nhtml:not(.light) .btn-d{color:#ff9b9b !important}\n\n/* Faster theme transition: no page-by-page repaint feeling. */\nbody,.sidebar,.main,.card,.metric,.onex-card,.onex-metric,.support-tile,.modal,.toast,\n.field input,.field select,.field textarea,.table-wrap,.quick-item,.onex-topbar,.onex-control-dock{\n  transition:background .16s ease,border-color .16s ease,color .16s ease,box-shadow .16s ease !important;\n}\n\n/* ---------- TOP LANGUAGE / NOTIFICATIONS ---------- */\n.top-setting-group{display:flex;align-items:center;gap:3px;padding:3px;border:1px solid var(--card-b);border-radius:11px;background:rgba(255,255,255,.025);box-shadow:inset 0 1px rgba(255,255,255,.04)}\n.top-setting-btn{border:0;border-radius:8px;padding:6px 8px;background:transparent;color:var(--t3);font:inherit;font-size:9px;line-height:1;cursor:pointer;transition:.16s ease;white-space:nowrap}.top-setting-btn:hover{color:var(--t1);background:rgba(var(--accent-rgb),.10)}.top-setting-btn.active{color:#fff;background:linear-gradient(135deg,var(--accent),var(--purple));box-shadow:0 4px 12px rgba(var(--accent-rgb),.25)}\n.top-notify-wrap{position:relative}.top-notify-btn{display:flex;align-items:center;gap:5px;border:1px solid var(--card-b);border-radius:11px;padding:7px 9px;background:rgba(255,255,255,.025);color:var(--t2);font:inherit;font-size:9px;cursor:pointer;position:relative;transition:.16s ease}.top-notify-btn:hover{border-color:rgba(var(--accent-rgb),.35);color:var(--t1);transform:translateY(-1px)}.notify-bell{font-size:11px;line-height:1}.notify-badge{display:none;min-width:15px;height:15px;padding:0 4px;align-items:center;justify-content:center;border-radius:999px;background:#ef4444;color:#fff;font-size:8px;font-weight:800}.notify-badge.show{display:inline-flex}\n.top-notify-panel{position:absolute;top:calc(100% + 9px);right:0;width:300px;max-width:min(300px,calc(100vw - 24px));z-index:1200;border:1px solid rgba(var(--accent2-rgb),.24);border-radius:16px;background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .96) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .94) 92%, var(--accent)));box-shadow:0 22px 70px rgba(0,0,0,.42),inset 0 1px rgba(255,255,255,.07);backdrop-filter:blur(25px) saturate(130%);overflow:hidden}.top-notify-panel[hidden]{display:none}.notify-panel-head{display:flex;align-items:center;justify-content:space-between;padding:11px 13px;border-bottom:1px solid rgba(var(--accent2-rgb),.12);color:var(--t1);font-size:12px}.notify-panel-head button{border:0;background:transparent;color:var(--t3);font-size:20px;cursor:pointer;line-height:1}.notify-list{padding:8px;max-height:360px;overflow:auto}.notify-empty{padding:18px 10px;text-align:center;color:var(--t3);font-size:11px}.notify-item{padding:11px 12px;border:1px solid rgba(var(--accent2-rgb),.15);border-radius:12px;background:rgba(var(--accent-rgb),.045);margin-bottom:7px}.notify-item-title{font-weight:800;color:var(--t1);font-size:12px;margin-bottom:5px}.notify-item-text{color:var(--t2);font-size:11px;line-height:1.8}.notify-item-meta{color:var(--t3);font-size:9px;margin-top:5px}.notify-update-btn{width:100%;border:0;border-radius:9px;padding:8px;background:linear-gradient(135deg,var(--accent),var(--purple));color:#fff;font:inherit;font-size:10px;font-weight:800;cursor:pointer;margin-top:9px}\nhtml.light .top-setting-group,html.light .top-notify-btn{background:#fff!important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))!important}html.light .top-setting-btn{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))}html.light .top-setting-btn:hover{background:#f1f5f9;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}html.light .top-notify-panel{background:#fff!important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))!important;box-shadow:0 18px 50px color-mix(in srgb, rgb(23 23 23 / .14) 92%, var(--accent))!important;backdrop-filter:none}\n@media(max-width:700px){.notify-label{display:none}.top-notify-btn{padding:7px 8px}.top-notify-panel{right:-42px;width:290px}}\n\\n.cfg-page-hero{display:flex;align-items:center;justify-content:space-between;gap:14px;margin-bottom:14px}.cfg-hero-actions{display:flex;gap:8px}.cfg-hero-actions .btn{height:42px;width:42px;padding:0;display:grid;place-items:center}.cfg-primary-btn{height:42px;padding:0 16px;border-radius:13px;border:1px solid rgba(var(--accent-rgb),.75);background:linear-gradient(135deg,var(--accent),var(--accent));color:#fff;font:900 11px Vazirmatn,sans-serif;box-shadow:0 10px 26px rgba(var(--accent-rgb),.24);cursor:pointer}.cfg-stat-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:10px;margin-bottom:12px}.cfg-stat-card{min-height:92px;padding:13px;border-radius:18px;border:1px solid rgba(var(--accent2-rgb),.24);background:linear-gradient(145deg,color-mix(in srgb, rgb(25 25 25 / .9) 88%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .86) 92%, var(--accent)));box-shadow:0 12px 28px rgba(0,0,0,.2);display:flex;align-items:center;gap:11px}.cfg-stat-icon{width:40px;height:40px;border-radius:13px;display:grid;place-items:center;font-size:20px;font-weight:900;background:rgba(var(--accent-rgb),.12);border:1px solid rgba(var(--accent2-rgb),.28);color:var(--accent2)}.cfg-stat-card.purple .cfg-stat-icon{color:var(--purple)}.cfg-stat-card.blue .cfg-stat-icon{color:var(--accent2)}.cfg-stat-card.pink .cfg-stat-icon{color:var(--accent2)}.cfg-stat-card small{display:block;color:var(--t3);font-size:9px;margin-bottom:4px}.cfg-stat-card b{display:block;font-size:20px;color:#f8fbff}.cfg-tools{display:flex;gap:9px;margin-bottom:9px;align-items:stretch}.cfg-search-box{height:48px;flex:1;min-width:0;display:flex;align-items:center;gap:10px;padding:0 14px;border:1px solid rgba(var(--accent-rgb),.3);border-radius:15px;background:linear-gradient(145deg,color-mix(in srgb, rgb(19 19 19 / .82) 92%, var(--accent)),color-mix(in srgb, rgb(9 9 9 / .68) 92%, var(--accent)));box-shadow:inset 0 1px rgba(255,255,255,.035)}.cfg-search-box>span{font-size:23px;color:var(--accent2);flex:0 0 auto}.cfg-search-box input{width:100%;min-width:0;border:0;outline:0;background:none;color:var(--t1);font:600 11px Vazirmatn,sans-serif}.cfg-filter-btn{width:48px;height:48px;flex:0 0 48px;border-radius:15px;border:1px solid rgba(var(--accent-rgb),.3);background:linear-gradient(145deg,color-mix(in srgb, rgb(19 19 19 / .82) 92%, var(--accent)),color-mix(in srgb, rgb(9 9 9 / .68) 92%, var(--accent)));color:var(--accent2);font-size:20px;cursor:pointer;transition:.18s ease}.cfg-filter-btn:hover{border-color:rgba(var(--accent2-rgb),.6);transform:translateY(-1px)}.cfg-filter-row{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-bottom:12px;max-height:100px;opacity:1;overflow:hidden;transform:none;transition:max-height .22s ease,opacity .18s ease,transform .22s ease}.cfg-filter-row.open{max-height:150px}.cfg-select{height:42px;min-width:0;border-radius:13px;border:1px solid rgba(var(--accent-rgb),.25);background:color-mix(in srgb, rgb(15 15 15 / .72) 92%, var(--accent));color:var(--t2);font:700 9px Vazirmatn,sans-serif;cursor:pointer;transition:.18s ease}.cfg-select b{color:#f8fbff;margin:0 5px}.cfg-select.on{border-color:rgba(var(--accent-rgb),.62);background:linear-gradient(135deg,rgba(var(--accent-rgb),.18),color-mix(in srgb, rgb(28 28 28 / .8) 88%, var(--accent)));box-shadow:0 7px 18px rgba(var(--accent-rgb),.08)}.cfg-filter-row .cfg-select:nth-child(n+3){display:none}.cfg-filter-row.open .cfg-select:nth-child(n+3){display:block}.cfg-list-shell{border:1px solid rgba(var(--accent-rgb),.23);border-radius:21px;background:linear-gradient(145deg,color-mix(in srgb, rgb(18 18 18 / .72) 92%, var(--accent)),color-mix(in srgb, rgb(9 9 9 / .76) 92%, var(--accent)));box-shadow:0 18px 42px rgba(0,0,0,.2),inset 0 1px rgba(255,255,255,.045);overflow:hidden;padding-bottom:10px}.cfg-list-head{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:13px 15px;border-bottom:1px solid rgba(var(--accent2-rgb),.12)}.cfg-list-head b{font-size:13px;color:#f8fbff}.cfg-list-head small{color:var(--t3);font-size:9px;margin-right:8px}.cfg-check-all{display:flex;align-items:center;gap:6px;color:var(--t3);font-size:9px;white-space:nowrap}.cfg-check-all input{width:15px;height:15px;accent-color:var(--accent)}.cfg-cards{padding:11px}.cfg-card{position:relative;display:grid;grid-template-columns:58px minmax(0,1fr) 190px 30px;align-items:center;gap:12px;padding:12px 10px;margin-bottom:9px;border:1px solid rgba(var(--accent-rgb),.38);border-radius:18px;background:linear-gradient(145deg,color-mix(in srgb, rgb(20 20 20 / .92) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .94) 92%, var(--accent)));box-shadow:inset 0 1px rgba(255,255,255,.035),0 9px 25px rgba(0,0,0,.16);transition:border-color .18s ease,box-shadow .18s ease}.cfg-card:last-child{margin-bottom:0}.cfg-card:hover{border-color:rgba(var(--accent2-rgb),.62);box-shadow:inset 0 1px rgba(255,255,255,.045),0 12px 30px rgba(0,0,0,.2)}.cfg-card.expired{border-color:rgba(255,45,112,.42)}.cfg-proto-icon{width:56px;height:56px;border-radius:16px;display:grid;place-items:center;background:color-mix(in srgb, rgb(8 8 8 / .78) 92%, var(--accent));border:1px solid rgba(var(--accent-rgb),.48);overflow:hidden}.cfg-proto-icon img{width:100%;height:100%;object-fit:contain}.cfg-main{min-width:0}.cfg-name-row{display:flex;align-items:center;gap:7px;min-width:0}.cfg-name-row b{font-size:13px;color:#f8fbff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.cfg-edit-dot{width:28px;height:28px;display:grid;place-items:center;flex:0 0 28px;border:1px solid rgba(var(--accent2-rgb),.18);border-radius:9px;background:rgba(var(--accent-rgb),.08);color:var(--accent2);cursor:pointer;transition:.18s ease}.cfg-edit-dot:hover{background:rgba(var(--accent-rgb),.16);border-color:rgba(var(--accent2-rgb),.45)}.cfg-proto{font-size:10px;color:var(--accent2);margin-top:3px}.cfg-meta{display:flex;gap:12px;flex-wrap:wrap;margin-top:9px;color:color-mix(in srgb, rgb(162 162 162) 82%, var(--accent));font-size:9px}.cfg-meta span{display:inline-flex;gap:4px}.cfg-meta i{color:var(--accent2);font-style:normal}.cfg-side{border-right:1px solid rgba(var(--accent2-rgb),.12);padding-right:13px}.cfg-status{display:inline-flex;gap:5px;padding:5px 10px;border-radius:999px;font-size:9px;font-weight:900;background:rgba(16,185,129,.12);border:1px solid rgba(52,211,153,.27);color:#38e0ad}.cfg-status i{width:6px;height:6px;border-radius:50%;background:#34e3b0}.cfg-status.bad{background:rgba(255,31,92,.11);border-color:rgba(255,69,112,.35);color:#ff7198}.cfg-status.bad i{background:#ff5b83}.cfg-usage{display:flex;align-items:center;gap:7px;margin-top:9px}.cfg-usage-ring{width:30px;height:30px;border-radius:50%;display:grid;place-items:center;background:conic-gradient(var(--purple) var(--pct),color-mix(in srgb, rgb(77 77 77 / .32) 82%, var(--accent)) 0);position:relative;flex:0 0 30px}.cfg-usage-ring:after{content:\'\';position:absolute;inset:5px;border-radius:50%;background:color-mix(in srgb, rgb(20 20 20) 92%, var(--accent))}.cfg-usage-ring span{position:relative;z-index:1;font-size:7px;color:#eaf4ff}.cfg-usage-copy{min-width:0}.cfg-usage-copy b{font-size:10px;color:#f1f6ff;white-space:nowrap}.cfg-usage-track{height:6px;width:110px;border-radius:99px;background:rgba(var(--accent-rgb),.45);overflow:hidden;margin-top:5px}.cfg-usage-fill{height:100%;border-radius:inherit;background:linear-gradient(90deg,var(--purple),#ec35b8)}.cfg-menu-btn{width:32px;height:50px;border:1px solid transparent;border-radius:10px;background:rgba(var(--accent-rgb),.04);color:var(--accent2);font-size:24px;cursor:pointer;transition:.18s ease}.cfg-menu-btn:hover,.cfg-menu-btn:focus-visible{background:rgba(var(--accent-rgb),.12);border-color:rgba(var(--accent2-rgb),.2);outline:none}.cfg-menu{position:fixed;z-index:99999;width:236px;max-width:calc(100vw - 16px);padding:8px;border:1px solid rgba(var(--accent2-rgb),.38);border-radius:18px;background:linear-gradient(145deg,color-mix(in srgb, rgb(25 25 25 / .96) 88%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .97) 92%, var(--accent)));box-shadow:0 24px 70px rgba(0,0,0,.66),0 0 35px rgba(var(--accent-rgb),.12),inset 0 1px rgba(255,255,255,.10);backdrop-filter:blur(24px) saturate(145%);-webkit-backdrop-filter:blur(24px) saturate(145%);display:none}.cfg-menu.open{display:block;animation:cfgMenuIn .14s ease-out}.cfg-menu-head{padding:8px 10px 9px;margin-bottom:3px;border-bottom:1px solid rgba(var(--accent2-rgb),.10);display:flex;flex-direction:column;gap:2px}.cfg-menu-head span{font-size:12px;font-weight:900;color:#f7fbff}.cfg-menu-head small{font-size:8px;color:rgba(var(--accent2-rgb),.48)}.cfg-menu button{width:100%;height:43px;border:0;border-radius:11px;background:transparent;color:#e4efff;text-align:right;padding:0 13px;font:800 11px Vazirmatn,sans-serif;cursor:pointer;transition:.15s ease}.cfg-menu button:hover{background:rgba(var(--accent-rgb),.14);transform:translateX(-1px)}.cfg-menu button.danger{color:#ff7898}.cfg-menu button.danger:hover{background:rgba(255,45,95,.10)}.cfg-side-top{display:flex;align-items:center;gap:7px;flex-wrap:wrap}.cfg-active-toggle{height:31px;min-width:78px;padding:0 10px;border-radius:999px;border:1px solid rgba(var(--accent-rgb),.38);background:linear-gradient(135deg,rgba(var(--accent-rgb),.20),rgba(var(--accent-rgb),.14));color:var(--accent2);display:inline-flex;align-items:center;justify-content:center;gap:6px;font:900 9px Vazirmatn,sans-serif;cursor:pointer;box-shadow:inset 0 1px rgba(255,255,255,.06),0 6px 18px rgba(var(--accent-rgb),.08);transition:.16s ease}.cfg-active-toggle:hover{transform:translateY(-1px);border-color:rgba(var(--accent2-rgb),.68);box-shadow:inset 0 1px rgba(255,255,255,.08),0 9px 24px rgba(var(--accent-rgb),.16)}.cfg-active-toggle.on{background:linear-gradient(135deg,var(--accent),var(--accent));border-color:rgba(var(--accent2-rgb),.78);color:#fff;box-shadow:0 8px 22px rgba(var(--accent-rgb),.22),inset 0 1px rgba(255,255,255,.16)}.cfg-active-dot{width:7px;height:7px;border-radius:50%;background:currentColor;box-shadow:0 0 8px currentColor}.cfg-card:not(.expired) .cfg-active-toggle.on .cfg-active-dot{background:#fff;box-shadow:0 0 9px rgba(255,255,255,.8)}@keyframes cfgMenuIn{from{opacity:0;transform:translateY(-4px) scale(.985)}to{opacity:1;transform:none}}.cfg-card-check{position:absolute;left:9px;top:9px;z-index:2}.cfg-card-check input{width:15px;height:15px;accent-color:var(--accent)}.cfg-empty{text-align:center;padding:42px 20px;color:var(--t3);font-size:11px}.cfg-page-hero{padding:14px 16px;border-radius:22px;border:1px solid rgba(var(--accent2-rgb),.20);background:linear-gradient(135deg,color-mix(in srgb, rgb(41 41 41 / .46) 88%, var(--accent)),color-mix(in srgb, rgb(16 16 16 / .34) 92%, var(--accent)));box-shadow:0 16px 38px rgba(0,0,0,.14),inset 0 1px rgba(255,255,255,.08);backdrop-filter:blur(18px) saturate(135%);-webkit-backdrop-filter:blur(18px) saturate(135%)}.cfg-page-hero .page-title{font-size:21px;font-weight:950}.cfg-page-hero .page-sub{font-size:11px;line-height:1.8}.cfg-stat-card,.cfg-search-box,.cfg-filter-btn,.cfg-list-shell,.cfg-card{backdrop-filter:blur(18px) saturate(135%);-webkit-backdrop-filter:blur(18px) saturate(135%)}.cfg-stat-card{background:linear-gradient(145deg,color-mix(in srgb, rgb(37 37 37 / .64) 88%, var(--accent)),color-mix(in srgb, rgb(14 14 14 / .58) 92%, var(--accent)));border-color:rgba(var(--accent2-rgb),.24);box-shadow:0 14px 34px rgba(0,0,0,.18),inset 0 1px rgba(255,255,255,.07)}.cfg-search-box{background:linear-gradient(145deg,color-mix(in srgb, rgb(35 35 35 / .64) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .56) 92%, var(--accent)));min-height:50px}.cfg-search-box input{font-size:13px}.cfg-filter-btn{background:linear-gradient(145deg,color-mix(in srgb, rgb(35 35 35 / .64) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .56) 92%, var(--accent)))}.cfg-list-shell{background:linear-gradient(145deg,color-mix(in srgb, rgb(28 28 28 / .58) 88%, var(--accent)),color-mix(in srgb, rgb(10 10 10 / .62) 92%, var(--accent)));border-color:rgba(var(--accent2-rgb),.25);box-shadow:0 20px 48px rgba(0,0,0,.22),inset 0 1px rgba(255,255,255,.08)}.cfg-list-head{padding:15px 17px}.cfg-list-head b{font-size:15px}.cfg-list-head small,.cfg-check-all{font-size:10px}.cfg-card{padding:14px 12px;border-radius:21px;background:linear-gradient(145deg,color-mix(in srgb, rgb(35 35 35 / .66) 88%, var(--accent)),color-mix(in srgb, rgb(13 13 13 / .68) 92%, var(--accent)));border-color:rgba(var(--accent2-rgb),.32);box-shadow:inset 0 1px rgba(255,255,255,.075),0 12px 32px rgba(0,0,0,.20)}.cfg-card:hover{box-shadow:inset 0 1px rgba(255,255,255,.10),0 16px 38px rgba(0,0,0,.24)}.cfg-name-row b{font-size:15px}.cfg-proto{font-size:11px}.cfg-meta{font-size:10px}.cfg-status{font-size:10px}.cfg-usage-copy b{font-size:11px}.cfg-list-head,.cfg-card{font-family:\'Vazirmatn\',sans-serif}.cfg-card-check input{width:18px;height:18px}.cfg-menu-btn{width:36px;height:54px;font-size:27px}.cfg-edit-dot{width:30px;height:30px;flex-basis:30px;font-size:15px}html.light .cfg-page-hero{background:linear-gradient(135deg,rgba(255,255,255,.78),rgba(236,244,255,.62));border-color:rgba(var(--accent-rgb),.12);box-shadow:0 14px 36px rgba(var(--accent-rgb),.08),inset 0 1px rgba(255,255,255,.95)}html.light .cfg-stat-card,html.light .cfg-search-box,html.light .cfg-filter-btn,html.light .cfg-list-shell{background:linear-gradient(145deg,rgba(255,255,255,.78),rgba(239,246,255,.62));border-color:rgba(var(--accent-rgb),.14);box-shadow:0 12px 30px rgba(var(--accent-rgb),.07),inset 0 1px rgba(255,255,255,.95)}html.light .cfg-card{background:linear-gradient(145deg,rgba(255,255,255,.82),rgba(235,244,255,.72));border-color:rgba(var(--accent-rgb),.16);box-shadow:inset 0 1px rgba(255,255,255,.95),0 12px 28px rgba(var(--accent-rgb),.08)}html.light .cfg-name-row b,html.light .cfg-list-head b,html.light .cfg-stat-card b{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}html.light .cfg-proto{color:var(--accent)}html.light .cfg-meta{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))}html.light .cfg-menu{background:linear-gradient(145deg,rgba(255,255,255,.96),rgba(238,246,255,.96));border-color:rgba(var(--accent-rgb),.20);box-shadow:0 24px 70px color-mix(in srgb, rgb(23 23 23 / .20) 92%, var(--accent)),0 0 30px rgba(var(--accent-rgb),.08),inset 0 1px rgba(255,255,255,.95)}html.light .cfg-menu-head span{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}html.light .cfg-menu-head small{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))}html.light .cfg-menu button{color:var(--accent)}html.light .cfg-menu button:hover{background:rgba(var(--accent-rgb),.08)}@media(max-width:800px){.cfg-stat-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.cfg-card{grid-template-columns:50px minmax(0,1fr) 118px 32px;gap:8px}.cfg-proto-icon{width:48px;height:48px}.cfg-side{padding-right:8px}.cfg-usage-track{width:78px}}@media(max-width:560px){.cfg-page-hero{padding:12px 13px}.cfg-page-hero .page-title{font-size:18px}.cfg-page-hero .page-sub{font-size:10px}.cfg-stat-card small{font-size:9px}.cfg-card{padding:12px 10px}.cfg-name-row b{font-size:13px}.cfg-proto{font-size:9px}.cfg-meta{font-size:9px}.cfg-active-toggle{height:30px;min-width:74px;font-size:8px}.cfg-status{font-size:9px}.cfg-menu{width:min(250px,calc(100vw - 14px));padding:8px;border-radius:17px}.cfg-menu button{height:45px;font-size:11px}.cfg-menu-head span{font-size:12px}.cfg-menu-head small{font-size:8px}}@media(max-width:560px){.cfg-page-hero{align-items:flex-start}.cfg-hero-actions .btn{display:none}.cfg-primary-btn{height:38px;padding:0 11px;font-size:9px}.cfg-stat-card{min-height:78px;padding:9px}.cfg-stat-icon{width:32px;height:32px;flex-basis:32px;font-size:16px}.cfg-stat-card b{font-size:16px}.cfg-stat-card small{font-size:8px}.cfg-tools{gap:7px}.cfg-search-box,.cfg-filter-btn{height:43px}.cfg-filter-btn{width:43px;flex-basis:43px}.cfg-filter-row{grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}.cfg-filter-row.open{max-height:150px}.cfg-select{height:40px;font-size:8px}.cfg-card{grid-template-columns:43px minmax(0,1fr) 32px;align-items:start;padding:11px 9px;border-radius:17px}.cfg-proto-icon{width:43px;height:43px}.cfg-side{grid-column:2/-1;border-right:0;border-top:1px solid rgba(var(--accent2-rgb),.1);padding:8px 0 0;margin-top:2px}.cfg-menu-btn{grid-column:3;grid-row:1;width:32px;height:43px}.cfg-meta{font-size:8px;gap:8px}.cfg-name-row b{font-size:11px}.cfg-proto{font-size:8px}.cfg-edit-dot{width:26px;height:26px;flex-basis:26px}.cfg-list-head{padding:11px 12px}.cfg-cards{padding:9px}.cfg-menu{width:min(220px,calc(100vw - 16px))}.cfg-menu button{height:40px;font-size:10px}}\n/* ============================================================\\n   ONEX THEME ENFORCER — SECONDARY PAGES + NESTED COMPONENTS\\n   This block intentionally comes last so old hard-coded dashboard\\n   colors cannot win over the selected global theme.\\n   ============================================================ */\\n\\n/* DARK: login glass recipe applied to every structural surface. */\\nhtml:not(.light) .page .card,\\nhtml:not(.light) .page .metric,\\nhtml:not(.light) .page .table-wrap,\\nhtml:not(.light) .page .support-tile,\\nhtml:not(.light) .page .link-box,\\nhtml:not(.light) .page .sub-box,\\nhtml:not(.light) .page .quick-item,\\nhtml:not(.light) .page .range-tabs,\\nhtml:not(.light) .page .range-mini,\\nhtml:not(.light) .page .mini-action,\\nhtml:not(.light) .page .chart-badge,\\nhtml:not(.light) .page .health-track,\\nhtml:not(.light) .page .xray-state,\\nhtml:not(.light) .page .recent-table,\\nhtml:not(.light) .page .recent-table th,\\nhtml:not(.light) .page .recent-table td,\\nhtml:not(.light) .page .field input,\\nhtml:not(.light) .page .field select,\\nhtml:not(.light) .page .field textarea{\\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .78) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .68) 92%, var(--accent))) !important;\\n  border-color:rgba(var(--accent2-rgb),.18) !important;\\n  box-shadow:inset 0 1px rgba(255,255,255,.055),inset 0 0 32px rgba(var(--accent-rgb),.035),0 14px 38px rgba(0,0,0,.18) !important;\\n  backdrop-filter:blur(25px) saturate(120%) !important;\\n  -webkit-backdrop-filter:blur(25px) saturate(120%) !important;\\n}\\nhtml:not(.light) .page .card,\\nhtml:not(.light) .page .metric{\\n  box-shadow:0 18px 50px rgba(0,0,0,.34),inset 0 1px rgba(255,255,255,.075),inset 0 0 38px rgba(var(--accent-rgb),.045) !important;\\n}\\nhtml:not(.light) .page .field input,\\nhtml:not(.light) .page .field select,\\nhtml:not(.light) .page .field textarea{\\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(10 10 10 / .68) 92%, var(--accent)),color-mix(in srgb, rgb(13 13 13 / .52) 92%, var(--accent))) !important;\\n  color:#f8fbff !important;\\n}\\nhtml:not(.light) .page .page-title,\\nhtml:not(.light) .page .card-title,\\nhtml:not(.light) .page .metric-val,\\nhtml:not(.light) .page .quick-name{color:#f8fbff !important}\\nhtml:not(.light) .page .page-sub,\\nhtml:not(.light) .page .field label,\\nhtml:not(.light) .page .metric-label,\\nhtml:not(.light) .page .quick-desc{color:rgba(248,250,252,.55) !important}\\n\\n/* Preserve intentional accent controls/badges in dark mode. */\\nhtml:not(.light) .page .btn-p,\\nhtml:not(.light) .page .btn-d,\\nhtml:not(.light) .page .range-tab.on,\\nhtml:not(.light) .page .conn-badge,\\nhtml:not(.light) .page .support-icon,\\nhtml:not(.light) .page .quick-icon,\\nhtml:not(.light) .page .metric-icon{\\n  backdrop-filter:none !important;-webkit-backdrop-filter:none !important;\\n}\\n\\n/* LIGHT: every structural panel becomes pure white, not gray. */\\nhtml.light .page,\\nhtml.light .page.on{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important}\\nhtml.light .page .card,\\nhtml.light .page .metric,\\nhtml.light .page .table-wrap,\\nhtml.light .page .support-tile,\\nhtml.light .page .link-box,\\nhtml.light .page .sub-box,\\nhtml.light .page .quick-item,\\nhtml.light .page .range-tabs,\\nhtml.light .page .range-mini,\\nhtml.light .page .mini-action,\\nhtml.light .page .chart-badge,\\nhtml.light .page .health-track,\\nhtml.light .page .xray-state,\\nhtml.light .page .recent-table,\\nhtml.light .page .recent-table th,\\nhtml.light .page .recent-table td,\\nhtml.light .page .field input,\\nhtml.light .page .field select,\\nhtml.light .page .field textarea,\\nhtml.light .page .onex-topbar,\\nhtml.light .page .onex-control-dock,\\nhtml.light .page .onex-card,\\nhtml.light .page .onex-metric{\\n  background:#fff !important;\\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;\\n  border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent)) !important;\\n  box-shadow:0 10px 30px color-mix(in srgb, rgb(23 23 23 / .07) 92%, var(--accent)),inset 0 1px rgba(255,255,255,.98) !important;\\n  backdrop-filter:none !important;\\n  -webkit-backdrop-filter:none !important;\\n}\\nhtml.light .page .field input,\\nhtml.light .page .field select,\\nhtml.light .page .field textarea{\\n  background:#fff !important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;border-color:color-mix(in srgb, rgb(23 23 23 / .14) 92%, var(--accent)) !important;\\n}\\nhtml.light .page .page-title,\\nhtml.light .page .card-title,\\nhtml.light .page .metric-val,\\nhtml.light .page .quick-name,\\nhtml.light .page .support-val{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important}\\nhtml.light .page .page-sub,\\nhtml.light .page .field label,\\nhtml.light .page .metric-label,\\nhtml.light .page .quick-desc,\\nhtml.light .page .support-label,\\nhtml.light .page .log-time,\\nhtml.light .page .health-name,\\nhtml.light .page .health-pct{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent)) !important}\\nhtml.light .page .log-msg{color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent)) !important}\\nhtml.light .page .onex-card-head,\\nhtml.light .page .sb-foot{border-color:color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent)) !important}\\nhtml.light .page th{background:#fff !important;color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent)) !important}\\nhtml.light .page td{background:#fff !important;color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent)) !important;border-color:color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent)) !important}\\nhtml.light .page tr:hover td{background:#f8fafc !important}\\n\\n/* Inline background declarations on secondary pages: normalize containers\\n   while leaving action buttons, badges and icons untouched. */\\nhtml.light .page div[style*="background:"],\\nhtml.light .page section[style*="background:"],\\nhtml.light .page article[style*="background:"],\\nhtml.light .page aside[style*="background:"]{\\n  background:#fff !important;\\n  color:inherit;\\n}\\nhtml:not(.light) .page div[style*="background:"],\\nhtml:not(.light) .page section[style*="background:"],\\nhtml:not(.light) .page article[style*="background:"],\\nhtml:not(.light) .page aside[style*="background:"]{\\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .78) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .68) 92%, var(--accent))) !important;\\n}\\n/* Re-apply accent colors to controls after the broad inline rule. */\\nhtml.light .page .btn-p{background:linear-gradient(135deg,var(--accent),var(--purple)) !important;color:#fff !important;border-color:transparent !important}\\nhtml.light .page .btn-d{background:rgba(239,68,68,.08) !important;color:#dc2626 !important;border-color:rgba(239,68,68,.20) !important}\\nhtml.light .page .range-tab.on{background:var(--accent) !important;color:#fff !important}\\nhtml.light .page .switch .slider{background:color-mix(in srgb, rgb(161 161 161 / .35) 82%, var(--accent)) !important}\\nhtml.light .page .switch input:checked + .slider{background:#16a34a !important}\\nhtml.light .page .quick-icon,\\nhtml.light .page .support-icon,\\nhtml.light .page .metric-icon{background:#f1f5f9 !important}\\n\\n/* Drawer and mobile top bar use exactly the same theme surfaces. */\\nhtml.light .sidebar,html.light .mob-bar{\\n  background:#fff !important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent)) !important;\\n  box-shadow:0 18px 50px color-mix(in srgb, rgb(23 23 23 / .12) 92%, var(--accent)) !important;backdrop-filter:none !important;-webkit-backdrop-filter:none !important;\\n}\\nhtml:not(.light) .sidebar,html:not(.light) .mob-bar{\\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .78) 92%, var(--accent)),color-mix(in srgb, rgb(8 8 8 / .68) 92%, var(--accent))) !important;\\n}\\n\\n/* Theme switch itself is instant enough that pages never look half-painted. */\\nhtml,body,.sidebar,.mob-bar,.main,.page,.page .card,.page .metric,.page .onex-card,.page .onex-metric,\\n.page .field input,.page .field select,.page .field textarea,.page .table-wrap,.page .link-box,.page .sub-box{\\n  transition:background-color .12s ease,background .12s ease,color .12s ease,border-color .12s ease,box-shadow .12s ease !important;\\n}\\n/* ============================================================\n   LIGHT STATIC 3D PROTOCOL PICKER\n   ============================================================ */\n#page-create select.protocol-native,#page-create .protocol-field select{display:none!important;position:absolute!important;left:-9999px!important;width:1px!important;height:1px!important;opacity:0!important;pointer-events:none!important;visibility:hidden!important}\n#page-create .protocol-field{position:relative}\n#page-create .protocol-trigger{width:100%;min-height:46px;display:flex!important;align-items:center;justify-content:space-between;gap:12px;padding:8px 12px;border-radius:13px;border:1px solid rgba(var(--accent2-rgb),.22);background:linear-gradient(145deg,color-mix(in srgb, rgb(30 30 30 / .88) 88%, var(--accent)),color-mix(in srgb, rgb(14 14 14 / .94) 92%, var(--accent)));color:var(--t1);cursor:pointer;position:relative;overflow:hidden;box-shadow:inset 0 1px rgba(255,255,255,.06),0 8px 22px rgba(0,0,0,.16)}\n#page-create .protocol-trigger:after{content:\'⌄\';position:absolute;inset-inline-end:10px;top:50%;transform:translateY(-50%);font-size:16px;color:var(--accent2);pointer-events:none}\n#page-create .protocol-trigger-main{display:flex;align-items:center;gap:8px;min-width:0;text-align:right}\n#page-create .protocol-trigger-icon{width:38px;height:38px;display:grid;place-items:center;flex:0 0 auto}\n#page-create .protocol-trigger-icon .protocol-option-icon{margin:0!important;width:38px!important;height:38px!important}.protocol-trigger-icon .static-icon{width:38px!important;height:38px!important}\n#page-create .protocol-trigger-text{min-width:0;display:flex;flex-direction:column;gap:1px}.protocol-trigger-name{font-size:12px;font-weight:800;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.protocol-trigger-sub{font-size:9px;color:var(--t3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n.protocol-picker-bg{position:fixed;inset:0;z-index:1200;display:none;align-items:center;justify-content:center;padding:16px;background:color-mix(in srgb, rgb(5 5 5 / .62) 92%, var(--accent));backdrop-filter:blur(5px);-webkit-backdrop-filter:blur(5px)}.protocol-picker-bg.open{display:flex!important}\n.protocol-picker{width:min(620px,calc(100vw - 24px));max-height:min(88vh,760px);overflow:hidden;border-radius:24px;border:1px solid rgba(var(--accent2-rgb),.35);background:linear-gradient(145deg,color-mix(in srgb, rgb(18 18 18 / .98) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .985) 92%, var(--accent)));box-shadow:0 30px 90px rgba(0,0,0,.55),0 0 55px rgba(var(--accent-rgb),.13);color:var(--t1);display:flex;flex-direction:column}\n.protocol-picker-head{padding:17px 18px 15px;border-bottom:1px solid color-mix(in srgb, rgb(161 161 161 / .12) 82%, var(--accent));display:flex;align-items:center;gap:12px;flex:0 0 auto}.protocol-picker-head-icon{width:45px;height:45px;border-radius:14px;display:grid;place-items:center;font-size:24px;background:linear-gradient(145deg,var(--accent),var(--accent) 55%,var(--purple));box-shadow:0 10px 26px rgba(var(--accent-rgb),.35);border:1px solid rgba(255,255,255,.2)}.protocol-picker-head-text{flex:1;min-width:0}.protocol-picker-title{font-size:17px;font-weight:900}.protocol-picker-subtitle{font-size:10px;color:var(--t3);margin-top:3px}.protocol-picker-close{width:34px;height:34px;border:1px solid color-mix(in srgb, rgb(161 161 161 / .16) 82%, var(--accent));border-radius:10px;background:rgba(255,255,255,.035);color:var(--t2);cursor:pointer;font-size:20px;display:grid;place-items:center}\n.protocol-picker-scroll{overflow:auto;padding:14px 16px 16px}.protocol-section{margin-bottom:17px}.protocol-section-title{display:flex;align-items:center;gap:9px;margin:0 2px 9px;color:var(--accent2);font-size:11px;font-weight:900}.protocol-section-title:before{content:"";height:1px;flex:1;background:linear-gradient(90deg,rgba(var(--accent-rgb),.05),rgba(var(--accent-rgb),.38));order:2}.protocol-section-title span{order:1}.protocol-section-title b{font-size:13px;order:3;font-weight:500}\n.protocol-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.protocol-option{position:relative;min-height:108px;border-radius:16px;border:1px solid rgba(var(--accent2-rgb),.16);background:linear-gradient(145deg,color-mix(in srgb, rgb(32 32 32 / .74) 88%, var(--accent)),color-mix(in srgb, rgb(15 15 15 / .82) 92%, var(--accent)));padding:7px 10px 10px;cursor:pointer;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;overflow:hidden;box-shadow:inset 0 1px rgba(255,255,255,.045)}.protocol-option:hover{border-color:rgba(var(--accent2-rgb),.42)}.protocol-option.selected{border-color:var(--accent);box-shadow:0 0 0 1px rgba(var(--accent-rgb),.18),0 0 22px rgba(var(--accent-rgb),.20);background:linear-gradient(145deg,rgba(var(--accent-rgb),.88),color-mix(in srgb, rgb(24 24 24 / .86) 88%, var(--accent)))}.protocol-option.selected:after{content:"✓";position:absolute;top:7px;right:7px;width:21px;height:21px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(145deg,var(--accent),var(--purple));color:#fff;font-size:12px;font-weight:900}.protocol-option-radio{position:absolute;top:10px;left:10px;width:16px;height:16px;border-radius:50%;border:2px solid rgba(var(--accent2-rgb),.65);background:transparent}.protocol-option.selected .protocol-option-radio{border-color:#22d3ee}\n.protocol-option-icon.proto-3d{width:76px;height:76px;display:grid;place-items:center;position:relative;z-index:1;flex:0 0 auto}.proto-3d .static-icon{width:74px;height:74px;display:block;object-fit:contain;filter:drop-shadow(0 7px 10px rgba(0,0,0,.30))}.protocol-option-name{font-size:11px;font-weight:900;position:relative;z-index:1;color:#f8fafc}.protocol-option-desc{font-size:8.5px;color:var(--t3);margin-top:2px;position:relative;z-index:1}\n.protocol-picker-section{margin:0 0 14px;padding:10px;border:1px solid rgba(var(--accent-rgb),.14);border-radius:16px;background:color-mix(in srgb, rgb(12 12 12 / .35) 92%, var(--accent))}.protocol-picker-section-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin:0 0 9px;padding:3px 4px;direction:rtl}.protocol-picker-section-head div{display:flex;flex-direction:column;gap:2px}.protocol-picker-section-head b{font-size:12px;color:#f8fbff}.protocol-picker-section-head small{font-size:8px;color:var(--accent2)}.protocol-picker-section-head>span{min-width:24px;height:24px;border-radius:9px;display:grid;place-items:center;background:rgba(var(--purple-rgb),.14);border:1px solid rgba(var(--purple-rgb),.22);color:var(--purple);font-size:9px;font-weight:900}.protocol-picker-section.vps .protocol-picker-section-head>span{background:rgba(var(--accent-rgb),.12);border-color:rgba(var(--accent-rgb),.2);color:var(--accent2)}.protocol-picker-section .protocol-grid{margin:0}.protocol-picker-section.vps .protocol-option{min-height:118px}.protocol-picker-section.railway .protocol-option{min-height:122px}.protocol-picker-section.railway{border-color:rgba(var(--accent-rgb),.18)}.protocol-picker-section.railway .protocol-picker-section-head>span{color:var(--accent2);background:rgba(var(--accent-rgb),.12);border-color:rgba(var(--accent-rgb),.2)}.protocol-picker-foot{padding:11px 16px 15px;border-top:1px solid color-mix(in srgb, rgb(161 161 161 / .12) 82%, var(--accent));background:linear-gradient(180deg,color-mix(in srgb, rgb(12 12 12 / .72) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .98) 92%, var(--accent)));display:flex;align-items:center;gap:10px;direction:rtl;flex:0 0 auto}.protocol-selected-info{flex:1;min-width:0;height:38px;border-radius:12px;border:1px solid rgba(var(--accent2-rgb),.16);background:color-mix(in srgb, rgb(32 32 32 / .55) 88%, var(--accent));display:flex;align-items:center;justify-content:center;color:var(--accent2);font-size:9px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding:0 10px}.protocol-picker-confirm{flex:0 0 auto;height:42px;padding:0 18px;border:0;border-radius:12px;background:linear-gradient(135deg,var(--accent),var(--purple));color:#fff;font-family:inherit;font-size:11px;font-weight:900;cursor:pointer;box-shadow:0 8px 20px rgba(var(--accent-rgb),.22)}\nhtml.light .protocol-picker-bg{background:color-mix(in srgb, rgb(23 23 23 / .28) 92%, var(--accent))}html.light .protocol-picker{background:linear-gradient(145deg,#fff,#f7fbff);color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}html.light .protocol-option{background:linear-gradient(145deg,#fff,#f7faff)}html.light .protocol-option-name{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}html.light .protocol-option-desc{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))}html.light .protocol-selected-info{background:#eff6ff;border-color:var(--accent2);color:var(--accent)}\n@media(max-width:560px){.protocol-picker-bg{padding:8px}.protocol-picker{width:calc(100vw - 16px);max-height:90vh;border-radius:20px}.protocol-picker-head{padding:13px 14px 12px}.protocol-picker-title{font-size:15px}.protocol-picker-scroll{padding:11px}.protocol-grid{gap:7px}.protocol-option{min-height:104px;padding:7px}.protocol-option-icon.proto-3d{width:64px;height:64px}.proto-3d .static-icon{width:62px;height:62px}.protocol-option-name{font-size:10px}.protocol-option-desc{font-size:7.5px}.protocol-picker-foot{padding:9px 11px 11px;gap:7px}.protocol-selected-info{height:34px;font-size:8px}.protocol-picker-confirm{height:40px;padding:0 12px;font-size:10px}}\n@media(max-width:360px){.protocol-grid{grid-template-columns:1fr}.protocol-option{min-height:90px}}\n#page-create .field label[data-i18n="label_proto"]:before{content:"✦ ";}\n/* Hard guarantee: the two protocol controls are custom buttons, never native selects. */\n#page-create .protocol-field{position:relative}\n#page-create .protocol-field > .protocol-trigger{display:flex!important;visibility:visible!important;opacity:1!important;position:relative!important;z-index:20!important;width:100%!important;min-height:46px!important}\n#page-create .protocol-field > select.protocol-native{display:none!important;pointer-events:none!important}\n@media(max-width:560px){#page-create .protocol-field > .protocol-trigger{min-height:48px!important;border-radius:14px!important}.protocol-picker{width:calc(100vw - 20px)!important;max-height:88vh!important}.protocol-grid{grid-template-columns:repeat(2,minmax(0,1fr))!important}}\n\n/* Final protocol-picker visibility guard */\n#page-create .field:has(> select.protocol-native) { position:relative; }\n#page-create .field > select.protocol-native + .protocol-trigger { display:flex!important; visibility:visible!important; opacity:1!important; position:relative!important; z-index:5!important; }\n.protocol-picker-bg.open { display:flex!important; }\n.protocol-picker { pointer-events:auto; }\n@media (max-width:560px){\n  .protocol-picker-bg{padding:7px!important;align-items:center!important}\n  .protocol-picker{width:min(94vw,620px)!important;max-height:92vh!important;border-radius:22px!important}\n  .protocol-grid{grid-template-columns:repeat(2,minmax(0,1fr))!important}\n  .protocol-option{min-height:88px!important}\n}\n\n\n\n/* FINAL OVERRIDE - protocol fields are NEVER native dropdowns */\n#page-create .protocol-field > select#cProto,\n#page-create .protocol-field > select#aProto {\n  display:none !important;\n  visibility:hidden !important;\n  width:0 !important; height:0 !important;\n  opacity:0 !important; pointer-events:none !important;\n}\n#page-create .protocol-field > button.protocol-trigger {\n  display:flex !important;\n  visibility:visible !important;\n  opacity:1 !important;\n  width:100% !important;\n  min-height:46px !important;\n  position:relative !important;\n  z-index:30 !important;\n  cursor:pointer !important;\n}\n.protocol-picker-bg { z-index:99999 !important; }\n.protocol-picker-bg.open { display:flex !important; visibility:visible !important; opacity:1 !important; }\n\n\n\n/* ============================================================\n   ADVANCED CONFIG — GLASS CONTROL PANEL\n   ============================================================ */\n.advanced-config-card{overflow:hidden;background:linear-gradient(145deg,color-mix(in srgb, rgb(31 31 31 / .78) 88%, var(--accent)),color-mix(in srgb, rgb(13 13 13 / .9) 92%, var(--accent)));border:1px solid rgba(var(--accent2-rgb),.22);box-shadow:inset 0 1px rgba(255,255,255,.055),0 18px 48px rgba(0,0,0,.16)}\n.advanced-toggle{width:100%;border:0;background:linear-gradient(120deg,rgba(var(--accent-rgb),.48),color-mix(in srgb, rgb(22 22 22 / .35) 92%, var(--accent)));color:var(--t1);display:flex;align-items:center;gap:12px;padding:16px;border-radius:16px;cursor:pointer;text-align:right;font-family:inherit}\n.advanced-toggle:hover{background:linear-gradient(120deg,rgba(var(--accent-rgb),.58),color-mix(in srgb, rgb(26 26 26 / .45) 88%, var(--accent)))}\n.advanced-toggle-icon{width:42px;height:42px;border-radius:13px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.14);border:1px solid rgba(var(--accent2-rgb),.28);font-size:20px;flex:none}\n.advanced-toggle-copy{min-width:0;display:flex;flex-direction:column;gap:4px;flex:1}.advanced-toggle-copy b{font-size:14px}.advanced-toggle-copy small{font-size:10px;color:var(--t3);line-height:1.7}.advanced-toggle-state{font-size:10px;color:var(--accent2);background:rgba(var(--accent-rgb),.11);border:1px solid rgba(var(--accent-rgb),.2);padding:5px 8px;border-radius:8px}.advanced-chevron{font-size:18px;transition:transform .2s}.advanced-config-card.open .advanced-chevron{transform:rotate(180deg)}\n.advanced-config-panel{padding:2px 4px 10px}.advanced-note{display:flex;gap:10px;align-items:flex-start;margin:10px 2px 14px;padding:12px;border-radius:14px;background:rgba(34,197,94,.06);border:1px solid rgba(34,197,94,.14)}.advanced-note>span{font-size:18px}.advanced-note b{display:block;font-size:11px}.advanced-note small{display:block;color:var(--t3);font-size:9px;line-height:1.8;margin-top:3px}\n.advanced-section{margin:10px 0;padding:13px;border-radius:16px;background:color-mix(in srgb, rgb(12 12 12 / .48) 92%, var(--accent));border:1px solid rgba(var(--accent-rgb),.14)}.advanced-section-head{display:flex;align-items:center;gap:9px;margin-bottom:12px}.advanced-section-icon{width:31px;height:31px;border-radius:10px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.1);border:1px solid rgba(var(--accent-rgb),.18);font-size:14px}.advanced-section-head b{font-size:11px}.advanced-section-head small{display:block;color:var(--t3);font-size:8px;margin-top:2px}.advanced-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.advanced-grid .field{min-width:0}.advanced-grid .field input,.advanced-grid .field select,.advanced-section textarea{width:100%;box-sizing:border-box}.advanced-check{min-height:42px;display:flex;align-items:center;gap:9px;padding:8px 10px;border-radius:11px;background:color-mix(in srgb, rgb(32 32 32 / .55) 88%, var(--accent));border:1px solid color-mix(in srgb, rgb(122 122 122 / .14) 82%, var(--accent));cursor:pointer}.advanced-check input{accent-color:var(--accent)}.advanced-check b{display:block;font-size:9px}.advanced-check small{display:block;color:var(--t3);font-size:8px;margin-top:2px}.advanced-subcard{margin-top:10px;padding:10px;border-radius:13px;background:color-mix(in srgb, rgb(26 26 26 / .5) 88%, var(--accent));border:1px dashed rgba(var(--accent-rgb),.18)}.advanced-subtitle{font-size:9px;font-weight:800;margin-bottom:8px;color:var(--accent2)}.port-manager{display:flex;flex-direction:column;gap:9px}.port-add-row{display:flex;gap:8px}.port-add-row input{flex:1}.advanced-port-list{display:flex;flex-wrap:wrap;gap:7px}.advanced-port-chip{display:flex;align-items:center;gap:6px;padding:6px 8px;border-radius:9px;background:rgba(var(--accent-rgb),.22);border:1px solid rgba(var(--accent-rgb),.22);font-size:9px}.advanced-port-chip b{font-size:10px}.advanced-port-chip button{border:0;background:transparent;color:color-mix(in srgb, rgb(161 161 161) 82%, var(--accent));cursor:pointer;font-size:14px}.advanced-port-chip.primary{border-color:rgba(34,197,94,.3);background:rgba(34,197,94,.07)}.advanced-help{font-size:8px;color:var(--t3);line-height:1.8}.advanced-actions{display:flex;gap:8px;justify-content:flex-end;flex-wrap:wrap;padding:8px 2px 2px}.advanced-actions .btn{min-width:125px}.advanced-validation-status{margin:8px 2px;display:none;padding:10px 12px;border-radius:12px;font-size:9px;line-height:1.8}.advanced-validation-status.ok{display:block;background:rgba(34,197,94,.07);border:1px solid rgba(34,197,94,.2);color:#86efac}.advanced-validation-status.warn{display:block;background:rgba(245,158,11,.07);border:1px solid rgba(245,158,11,.2);color:#fcd34d}.advanced-validation-status.err{display:block;background:rgba(239,68,68,.07);border:1px solid rgba(239,68,68,.2);color:#fca5a5}.advanced-preview{margin:8px 2px;border-radius:14px;overflow:hidden;border:1px solid rgba(var(--accent-rgb),.18);background:color-mix(in srgb, rgb(9 9 9 / .7) 92%, var(--accent))}.advanced-preview-head{display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border-bottom:1px solid rgba(var(--accent-rgb),.14);font-size:10px}.advanced-preview pre{margin:0;padding:12px;max-height:360px;overflow:auto;font:9px/1.7 ui-monospace,SFMono-Regular,Consolas,monospace;color:color-mix(in srgb, rgb(211 211 211) 82%, var(--accent));direction:ltr;text-align:left}@media(max-width:680px){.advanced-grid{grid-template-columns:1fr}.advanced-toggle-state{display:none}.advanced-toggle{padding:13px}.advanced-toggle-icon{width:38px;height:38px}.port-add-row{flex-direction:column}.advanced-actions .btn{flex:1;min-width:100px}}\n/* ============================================================\n   FINAL PROTOCOL UI — COLLAPSED BAR + MODAL ONLY\n   Protocol cards must never occupy the create page itself.\n   ============================================================ */\n#page-create .protocol-field .inline-protocol-picker,\n#page-create .inline-protocol-picker{\n  display:none !important;\n  visibility:hidden !important;\n  width:0 !important;\n  height:0 !important;\n  max-height:0 !important;\n  margin:0 !important;\n  padding:0 !important;\n  overflow:hidden !important;\n}\n#page-create .protocol-field{position:relative !important;}\n#page-create .protocol-field > select.protocol-native{\n  display:none !important;\n  visibility:hidden !important;\n  pointer-events:none !important;\n  position:absolute !important;\n  width:1px !important;height:1px !important;\n  opacity:0 !important;\n}\n#page-create .protocol-field > .protocol-trigger{\n  display:flex !important;\n  visibility:visible !important;\n  opacity:1 !important;\n  width:100% !important;\n  min-height:52px !important;\n  align-items:center !important;\n  justify-content:space-between !important;\n  cursor:pointer !important;\n}\n.protocol-picker-bg{\n  position:fixed !important;\n  inset:0 !important;\n  z-index:2147483000 !important;\n  display:none !important;\n  align-items:center !important;\n  justify-content:center !important;\n  padding:12px !important;\n  background:color-mix(in srgb, rgb(5 5 5 / .70) 92%, var(--accent)) !important;\n  backdrop-filter:blur(8px) !important;\n  -webkit-backdrop-filter:blur(8px) !important;\n}\n.protocol-picker-bg.open{\n  display:flex !important;\n  visibility:visible !important;\n  opacity:1 !important;\n}\n.protocol-picker{\n  width:min(680px,calc(100vw - 20px)) !important;\n  max-height:min(88vh,760px) !important;\n}\n@media(max-width:560px){\n  .protocol-picker{width:calc(100vw - 16px) !important;max-height:88vh !important;border-radius:20px !important;}\n  .protocol-picker-scroll{padding:10px !important;}\n  .protocol-grid{grid-template-columns:repeat(2,minmax(0,1fr)) !important;gap:8px !important;}\n  .protocol-option{min-height:112px !important;}\n}\n\n/* CONFIG MANAGER FINAL UI — legacy table is data-only, cards are the visible manager. */\n#linksTable{display:none!important;width:0!important;height:0!important;overflow:hidden!important;position:absolute!important;pointer-events:none!important}\n#cfgCards{display:grid!important;gap:10px!important}\n/* FINAL PROTOCOL DESIGN: one unified grid, no separator bars, true inline 3D protocol cubes */\n.protocol-picker-scroll{padding:16px!important;overflow:auto}\n.protocol-section{margin:0!important}\n.protocol-section-title{display:none!important}\n.protocol-grid-all{display:grid!important;grid-template-columns:repeat(4,minmax(0,1fr))!important;gap:12px!important}\n.protocol-option{min-height:126px!important;border-radius:18px!important;background:linear-gradient(145deg,color-mix(in srgb, rgb(25 25 25 / .92) 88%, var(--accent)),color-mix(in srgb, rgb(13 13 13 / .96) 92%, var(--accent)))!important;border:1px solid rgba(var(--accent2-rgb),.24)!important;box-shadow:inset 0 1px rgba(255,255,255,.055),0 8px 24px rgba(0,0,0,.16)!important;transition:transform .18s ease,border-color .18s ease,box-shadow .18s ease!important}\n.protocol-option:hover{transform:translateY(-3px)!important;border-color:rgba(var(--accent-rgb),.75)!important;box-shadow:0 10px 28px rgba(var(--accent-rgb),.16),inset 0 1px rgba(255,255,255,.08)!important}\n.protocol-option.selected{transform:translateY(-2px)!important;border-color:var(--accent)!important;box-shadow:0 0 0 1px rgba(var(--accent-rgb),.32),0 0 28px rgba(var(--accent-rgb),.28),inset 0 1px rgba(255,255,255,.08)!important}\n.protocol-option-icon.proto-3d{width:82px!important;height:82px!important}\n.lego-proto-svg{width:82px!important;height:82px!important;display:block;overflow:visible}\n.proto-3d .static-icon{display:none!important}\n.protocol-art-icon{width:100%;height:100%;display:block;object-fit:contain;filter:drop-shadow(0 7px 10px rgba(0,0,0,.30));}\n#page-create .protocol-trigger-icon{width:44px!important;height:44px!important}\n#page-create .protocol-trigger-icon .protocol-option-icon{width:44px!important;height:44px!important}\n#page-create .protocol-trigger-icon .lego-proto-svg{width:44px!important;height:44px!important}\n@media(max-width:700px){.protocol-grid-all{grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:10px!important}.protocol-option{min-height:124px!important}.protocol-option-icon.proto-3d{width:78px!important;height:78px!important}.lego-proto-svg{width:78px!important;height:78px!important}}\n@media(max-width:380px){.protocol-grid-all{grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:8px!important}.protocol-option{min-height:112px!important;padding:6px!important}.protocol-option-icon.proto-3d{width:68px!important;height:68px!important}.lego-proto-svg{width:68px!important;height:68px!important}.protocol-option-name{font-size:10px!important}}\n\n.all-proto-toggle{display:flex;align-items:center;justify-content:space-between;gap:12px;margin:10px 0 14px;padding:12px 14px;border:1px solid rgba(34,197,94,.22);border-radius:14px;background:rgba(34,197,94,.035);cursor:pointer;user-select:none}\n.all-proto-toggle span{display:block;min-width:0}.all-proto-toggle b{display:block;font-size:12px}.all-proto-toggle small{display:block;color:var(--t3);font-size:10px;margin-top:4px;line-height:1.6}.all-proto-toggle input{position:absolute;opacity:0;pointer-events:none}.all-proto-toggle i{position:relative;flex:0 0 48px;width:48px;height:28px;border-radius:999px;background:color-mix(in srgb, rgb(84 84 84) 82%, var(--accent));transition:.2s;box-shadow:inset 0 0 0 1px rgba(255,255,255,.12)}.all-proto-toggle i:before{content:"";position:absolute;top:4px;right:24px;width:20px;height:20px;border-radius:50%;background:#fff;box-shadow:0 2px 6px rgba(0,0,0,.35);transition:.2s}.all-proto-toggle:has(input:checked) i{background:#22c55e;box-shadow:0 0 12px rgba(34,197,94,.28)}.all-proto-toggle:has(input:checked) i:before{right:4px}.all-proto-toggle:focus-within{outline:2px solid rgba(34,197,94,.35);outline-offset:2px}\n\n\n/* ============================================================\n   ONEX X BRAND — approved panel chrome\n   Sidebar + top bar + mobile header. Kept vector-only so it stays\n   sharp on every screen density and does not depend on raster art.\n   ============================================================ */\n.onex-x-svg{display:block;width:100%;height:100%;overflow:visible}\n.onex-x-shadow{filter:drop-shadow(0 0 8px rgba(var(--accent-rgb),.52)) drop-shadow(0 0 16px rgba(var(--purple-rgb),.24))}\n.onex-brand-word{font-family:Inter,system-ui,sans-serif;font-weight:900;letter-spacing:.055em;color:#f8fbff;line-height:.9;white-space:nowrap}\n.onex-brand-word .x{background:linear-gradient(135deg,var(--accent) 5%,var(--accent) 46%,var(--purple) 92%);-webkit-background-clip:text;background-clip:text;color:transparent;text-shadow:none}\n.onex-brand-word{direction:ltr!important;unicode-bidi:isolate!important}.onex-brand-word .brand-vpn{display:inline-block;margin-left:8px;margin-inline-start:8px;font-size:.42em;font-weight:800;letter-spacing:.22em;color:#dbeafe;vertical-align:middle;opacity:.9}\nhtml.light .onex-brand-word .brand-vpn{color:#334155}\n.onex-brand-sub{font:700 7px/1.2 Inter,system-ui,sans-serif;letter-spacing:.32em;color:color-mix(in srgb, rgb(180 180 180 / .62) 82%, var(--accent));margin-top:5px;white-space:nowrap}\nhtml.light .onex-brand-word{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}\nhtml.light .onex-brand-sub{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))}\n\n/* Sidebar brand sits at the very top of the drawer/column. */\n.sb-logo.onex-approved-brand{display:flex!important;align-items:center!important;justify-content:flex-start!important;gap:11px!important;padding:16px 14px!important;min-height:82px!important;border-bottom:1px solid rgba(var(--accent2-rgb),.13)!important;background:linear-gradient(180deg,rgba(var(--accent-rgb),.09),transparent)!important;overflow:hidden}\n.onex-sidebar-mark{width:52px;height:52px;flex:0 0 52px;padding:6px;border-radius:15px;background:linear-gradient(145deg,color-mix(in srgb, rgb(18 18 18 / .92) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .86) 92%, var(--accent)));border:1px solid rgba(var(--accent2-rgb),.27);box-shadow:inset 0 1px rgba(255,255,255,.06),0 10px 25px rgba(0,0,0,.25),0 0 20px rgba(var(--accent-rgb),.10)}\n.onex-sidebar-copy{min-width:0;display:flex;flex-direction:column;align-items:flex-start}\n.onex-sidebar-copy .onex-brand-word{font-size:22px}\n.sidebar.collapsed .sb-logo.onex-approved-brand{justify-content:center!important;padding:14px 8px!important;min-height:80px!important}\n.sidebar.collapsed .onex-sidebar-mark{width:48px;height:48px;flex-basis:48px;padding:6px}\n.sidebar.collapsed .onex-sidebar-copy{display:none!important}\n\n/* Top bar logo. */\n.onex-topbar-brand{display:flex;align-items:center;gap:10px;min-width:158px;height:46px;padding:5px 11px 5px 7px;border-radius:14px;border:1px solid rgba(var(--accent2-rgb),.20);background:linear-gradient(135deg,color-mix(in srgb, rgb(26 26 26 / .54) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .30) 92%, var(--accent)));box-shadow:inset 0 1px rgba(255,255,255,.045)}\n.onex-topbar-mark{width:39px;height:39px;flex:0 0 39px;padding:3px}\n.onex-topbar-copy{display:flex;flex-direction:column;align-items:flex-start;min-width:0}\n.onex-topbar-copy .onex-brand-word{font-size:20px;letter-spacing:.045em}\n.onex-topbar-copy .onex-brand-sub{font-size:6px;letter-spacing:.25em;margin-top:4px}\nhtml.light .onex-topbar-brand{background:#fff;border-color:rgba(var(--accent-rgb),.16);box-shadow:0 5px 18px color-mix(in srgb, rgb(23 23 23 / .05) 92%, var(--accent))}\n\n/* Mobile uses the same approved X instead of the old text-only title. */\n.mob-brand.onex-approved-mobile{display:flex!important;align-items:center!important;justify-content:center!important;gap:8px!important}\n.onex-mobile-mark{width:40px;height:40px;flex:0 0 40px;padding:3px}\n.onex-mobile-copy{display:flex;flex-direction:column;align-items:flex-start}\n.onex-mobile-copy .onex-brand-word{font-size:18px}\n.onex-mobile-copy .onex-brand-sub{font-size:5.5px;letter-spacing:.22em;margin-top:3px}\n\n@media(max-width:1100px){\n  .onex-topbar-brand{min-width:132px;padding-left:5px;padding-right:8px;gap:7px}.onex-topbar-mark{width:34px;height:34px;flex-basis:34px}.onex-topbar-copy .onex-brand-word{font-size:17px}.onex-topbar-copy .onex-brand-sub{display:none}\n}\n@media(max-width:760px){\n  .onex-topbar-brand{display:none!important}\n}\n@media(max-width:640px){\n  .sidebar .sb-logo.onex-approved-brand{display:flex!important;min-height:72px!important;padding:10px 12px!important}.sidebar .onex-sidebar-mark{width:45px!important;height:45px!important;flex-basis:45px!important}.sidebar .onex-sidebar-copy .onex-brand-word{font-size:20px!important}\n}\n@media(max-width:390px){.onex-mobile-mark{width:35px;height:35px;flex-basis:35px}.onex-mobile-copy .onex-brand-word{font-size:16px}}\n\n/* ONEX 1.3.9 FINAL BRAND LAYOUT — matches approved clean mockup */\n.onex-brand-sub,.onex-mobile-copy .onex-brand-sub,.onex-sidebar-copy .onex-brand-sub,.onex-topbar-copy .onex-brand-sub{display:none!important}\n\n/* Sidebar: compact glass header with full ONEX wordmark. */\n.sb-logo.onex-approved-brand{\n  min-height:112px!important;\n  padding:16px 14px!important;\n  justify-content:center!important;\n  gap:10px!important;\n  border-bottom:1px solid rgba(var(--accent2-rgb),.16)!important;\n  background:linear-gradient(180deg,color-mix(in srgb, rgb(32 32 32 / .32) 88%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .10) 92%, var(--accent)))!important;\n}\n.onex-sidebar-mark{\n  width:58px!important;height:58px!important;flex:0 0 58px!important;padding:6px!important;\n  border-radius:17px!important;\n  background:linear-gradient(145deg,color-mix(in srgb, rgb(22 22 22 / .94) 88%, var(--accent)),color-mix(in srgb, rgb(10 10 10 / .92) 92%, var(--accent)))!important;\n  border:1px solid rgba(var(--accent2-rgb),.25)!important;\n  box-shadow:inset 0 1px rgba(255,255,255,.06),0 12px 30px rgba(0,0,0,.28),0 0 24px rgba(var(--accent-rgb),.12)!important;\n}\n.onex-sidebar-copy{align-items:flex-start!important}\n.onex-sidebar-copy .onex-brand-word{font-size:25px!important;letter-spacing:.035em!important}\n\n/* Collapsed sidebar: only the X mark, centered like the approved mockup. */\n.sidebar.collapsed .sb-logo.onex-approved-brand{min-height:82px!important;padding:12px 7px!important}\n.sidebar.collapsed .onex-sidebar-mark{width:52px!important;height:52px!important;flex-basis:52px!important;padding:6px!important}\n\n/* Desktop top bar: flat/glass ONEX wordmark, no tagline. */\n.onex-topbar-brand{\n  min-width:188px!important;height:50px!important;padding:4px 13px 4px 7px!important;\n  gap:9px!important;border-radius:15px!important;\n  border:1px solid rgba(var(--accent2-rgb),.22)!important;\n  background:linear-gradient(135deg,color-mix(in srgb, rgb(27 27 27 / .68) 88%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .42) 92%, var(--accent)))!important;\n  box-shadow:inset 0 1px rgba(255,255,255,.05),0 8px 24px rgba(0,0,0,.18)!important;\n}\n.onex-topbar-mark{width:42px!important;height:42px!important;flex-basis:42px!important;padding:3px!important}\n.onex-topbar-copy .onex-brand-word{font-size:22px!important;letter-spacing:.035em!important}\n\n/* Mobile header: X + ONEX only. */\n.mob-brand.onex-approved-mobile{gap:8px!important}\n.onex-mobile-mark{width:38px!important;height:38px!important;flex-basis:38px!important;padding:2px!important}\n.onex-mobile-copy .onex-brand-word{font-size:19px!important;letter-spacing:.035em!important}\n\n/* Keep the overall mockup proportions clean on narrow screens. */\n@media(max-width:1100px){\n  .onex-topbar-brand{min-width:158px!important;height:47px!important}\n  .onex-topbar-mark{width:39px!important;height:39px!important;flex-basis:39px!important}\n  .onex-topbar-copy .onex-brand-word{font-size:20px!important}\n}\n@media(max-width:760px){\n  .onex-topbar-brand{display:none!important}\n}\n@media(max-width:640px){\n  .sidebar .sb-logo.onex-approved-brand{min-height:78px!important;padding:10px 11px!important}\n  .sidebar .onex-sidebar-mark{width:46px!important;height:46px!important;flex-basis:46px!important}\n  .sidebar .onex-sidebar-copy .onex-brand-word{font-size:20px!important}\n  .onex-mobile-mark{width:36px!important;height:36px!important;flex-basis:36px!important}\n  .onex-mobile-copy .onex-brand-word{font-size:18px!important}\n}\n@media(max-width:390px){\n  .onex-mobile-mark{width:33px!important;height:33px!important;flex-basis:33px!important}\n  .onex-mobile-copy .onex-brand-word{font-size:16px!important}\n}\n\n\n/* Exact Hexa interactive preview styles */\n#page-dash{--bg:#070a12;--surface:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent));--surface2:color-mix(in srgb, rgb(34 34 34) 88%, var(--accent));--line:rgba(var(--accent2-rgb),.16);--text:#eff7ff;--muted:color-mix(in srgb, rgb(175 175 175) 82%, var(--accent));--dim:color-mix(in srgb, rgb(125 125 125) 82%, var(--accent));--p:#38d9ff;--s:#8b5cf6;--ok:#48e39d;--warn:#ffc857;--bad:var(--accent2);--sb:272px;--ease:cubic-bezier(.16,1,.3,1)}\n#page-dash button{font:inherit;color:inherit;cursor:pointer}#page-dash canvas{display:block}#page-dash .mono{font-family:"JetBrains Mono",monospace;direction:ltr}#page-dash .glass{background:linear-gradient(145deg,color-mix(in srgb, rgb(37 37 37 / .88) 88%, var(--accent)),color-mix(in srgb, rgb(15 15 15 / .9) 92%, var(--accent)));border:1px solid var(--line);box-shadow:0 18px 50px rgba(0,0,0,.3),inset 0 1px rgba(255,255,255,.06);backdrop-filter:blur(18px);border-radius:22px}#page-dash .badge{margin-inline-start:auto;color:var(--p);font:700 11px "JetBrains Mono"}#page-dash .bars{position:relative;width:20px;height:20px;display:block;margin:auto}#page-dash .bars i{position:absolute;left:1px;right:1px;height:2.5px;border-radius:4px;background:color-mix(in srgb, rgb(18 18 18) 92%, var(--accent));transition:transform .5s var(--ease),opacity .2s}#page-dash .bars i:nth-child(1){top:4px}#page-dash .bars i:nth-child(2){top:9px}#page-dash .bars i:nth-child(3){top:14px}@keyframes spin{to{transform:rotate(360deg)}}#page-dash .dot{width:8px;height:8px;border-radius:50%;background:var(--ok);box-shadow:0 0 12px var(--ok);flex:none}#page-dash .grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));grid-template-areas:"hero hero chart chart" "dock dock chart chart" "metrics metrics metrics metrics" "health telegram recent recent" "info quick recent recent";gap:16px}#page-dash .area{min-width:0}#page-dash .hero{grid-area:hero;padding:24px;display:flex;flex-direction:column;gap:14px}#page-dash .hero h1{font-size:clamp(25px,3vw,37px);line-height:1.35;margin:0}#page-dash .hero h1 span{color:var(--p)}#page-dash .hero p{margin:0;color:var(--muted);font-size:13px}#page-dash .versions{display:flex;gap:9px;flex-wrap:wrap}#page-dash .version{padding:9px 12px;border:1px solid var(--line);background:rgba(0,0,0,.18);border-radius:11px;font-size:11px;color:var(--muted)}#page-dash .version b{color:var(--text);font:700 12px "JetBrains Mono";direction:ltr;margin-inline-start:5px}#page-dash .alert{display:flex;align-items:center;gap:10px;padding:10px 12px;border:1px solid rgba(255,200,87,.35);background:rgba(255,200,87,.08);border-radius:12px;color:var(--muted);font-size:12px}#page-dash .alert b{color:var(--warn)}#page-dash .alert button{margin-inline-start:auto;border:0;background:var(--warn);color:#17120a;border-radius:8px;padding:7px 10px;font-weight:800;font-size:11px}#page-dash .actions{display:flex;gap:9px}#page-dash .primary{border:0!important;background:linear-gradient(135deg,var(--p),var(--s))!important;color:color-mix(in srgb, rgb(18 18 18) 92%, var(--accent))!important;font-weight:900}#page-dash .hero .actions button{padding:11px 15px;border-radius:12px}#page-dash .dock{grid-area:dock;display:grid;grid-template-columns:repeat(3,1fr);gap:10px;padding:10px}#page-dash .dock button{display:flex;align-items:center;gap:10px;text-align:start;padding:13px;border-radius:15px;border:1px solid var(--line);background:rgba(255,255,255,.035);transition:transform .25s var(--ease),border-color .2s}#page-dash .dock button:hover{transform:translateY(-3px);border-color:rgba(var(--accent2-rgb),.5)}#page-dash .dock strong{display:block;font-size:12.5px}#page-dash .dock small{display:block;color:var(--dim);font:10px "JetBrains Mono";margin-top:3px}#page-dash .icon{width:36px;height:36px;border-radius:11px;display:grid;place-items:center;background:rgba(var(--accent2-rgb),.13);color:var(--p);font-weight:900;flex:none}#page-dash .metrics{grid-area:metrics;display:grid;grid-template-columns:repeat(4,1fr);gap:1px;padding:1px}#page-dash .metric{padding:19px;border-inline-end:1px solid var(--line)}#page-dash .metric:last-child{border:0}#page-dash .metric label{display:block;color:var(--muted);font-size:12px}#page-dash .metric strong{display:block;font:700 25px "JetBrains Mono";direction:ltr;margin-top:8px}#page-dash .metric small{color:var(--dim);font-size:11px}#page-dash .chart{grid-area:chart;padding:20px}#page-dash .head{display:flex;justify-content:space-between;align-items:center;gap:10px;margin-bottom:14px}#page-dash .head h2{font-size:15px;margin:0}#page-dash .range{display:flex;gap:3px;padding:3px;border-radius:9px;background:rgba(0,0,0,.2)}#page-dash .range button{border:0;background:transparent;color:var(--muted);border-radius:7px;padding:6px 8px;font-size:11px}#page-dash .range button.active{background:rgba(var(--accent2-rgb),.18);color:var(--text)}#page-dash #traffic{width:100%;height:230px}#page-dash .health{grid-area:health;padding:20px}#page-dash .rows{display:grid;gap:13px}#page-dash .healthrow{display:grid;grid-template-columns:38px 1fr 40px;gap:10px;align-items:center}#page-dash .healthrow .tag{height:30px;border-radius:9px;background:rgba(var(--accent2-rgb),.1);border:1px solid rgba(var(--accent2-rgb),.25);display:grid;place-items:center;color:var(--p);font:700 10px "JetBrains Mono"}#page-dash .track{height:6px;background:rgba(255,255,255,.08);border-radius:9px;overflow:hidden}#page-dash .track i{display:block;height:100%;width:var(--w);background:linear-gradient(90deg,var(--p),var(--s));border-radius:inherit}#page-dash .healthrow label{font-size:12px;color:var(--muted);display:block;margin-bottom:5px}#page-dash .healthrow b{font:600 11px "JetBrains Mono";direction:ltr}#page-dash .telegram{grid-area:telegram;padding:20px;display:flex;flex-direction:column;justify-content:space-between;background:radial-gradient(circle at 50% 0,rgba(var(--accent2-rgb),.18),transparent 44%),linear-gradient(145deg,color-mix(in srgb, rgb(45 45 45 / .9) 88%, var(--accent)),color-mix(in srgb, rgb(16 16 16 / .95) 92%, var(--accent)))}#page-dash .tgmark{width:82px;height:82px;border-radius:50%;margin:5px auto 16px;display:grid;place-items:center;background:linear-gradient(145deg,var(--accent),var(--accent));box-shadow:0 0 35px rgba(var(--accent-rgb),.5);font-size:35px}#page-dash .telegram h3{text-align:center;font-size:14px;margin:0}#page-dash .telegram p{text-align:center;color:var(--muted);font-size:11px;margin:5px 0 17px}#page-dash .telegram a{display:block;text-align:center;text-decoration:none;color:#fff;background:var(--accent);border-radius:11px;padding:10px;font-size:12px;font-weight:800}#page-dash .recent{grid-area:recent;padding:20px}#page-dash .table{display:grid;gap:0}#page-dash .tr{display:grid;grid-template-columns:1.2fr 1fr 1.2fr 70px 42px;gap:12px;align-items:center;padding:13px 8px;border-bottom:1px solid var(--line);font-size:12px}#page-dash .tr.headrow{color:var(--dim);font-size:10px}#page-dash .tr:last-child{border:0}#page-dash .proto{color:var(--muted);font:11px "JetBrains Mono";direction:ltr}#page-dash .usage .bar{height:5px;background:rgba(255,255,255,.08);border-radius:8px;margin-top:6px;overflow:hidden}#page-dash .usage i{display:block;height:100%;background:var(--p);width:var(--w);border-radius:8px}#page-dash .quick{grid-area:quick;padding:20px}#page-dash .quicklist{display:grid;gap:9px}#page-dash .quickitem{display:flex;align-items:center;gap:10px;padding:12px;border:1px solid var(--line);border-radius:13px;background:rgba(255,255,255,.03);font-size:12px}#page-dash .quickitem:hover{border-color:rgba(var(--accent2-rgb),.4)}#page-dash .info{grid-area:info;padding:20px}#page-dash .kv{display:grid;grid-template-columns:auto 1fr;gap:11px;font-size:11px}#page-dash .kv dt{color:var(--dim)}#page-dash .kv dd{margin:0;text-align:left;direction:ltr;color:var(--muted);font-family:"JetBrains Mono";overflow:hidden;text-overflow:ellipsis;white-space:nowrap}@media(max-width:1150px){#page-dash .grid{grid-template-columns:repeat(2,minmax(0,1fr));grid-template-areas:"hero hero" "dock dock" "metrics metrics" "chart chart" "health telegram" "recent recent" "quick info"}}@media(max-width:800px){#page-dash .open .sidebar{transform:none}#page-dash .grid{grid-template-columns:1fr;grid-template-areas:"hero" "dock" "metrics" "chart" "health" "telegram" "recent" "quick" "info"}#page-dash .metrics{grid-template-columns:1fr 1fr}#page-dash .metric{border-bottom:1px solid var(--line)}#page-dash .dock{grid-template-columns:1fr}#page-dash .picker{display:none}}\n\n</style>\n</head>\n<body>\n<canvas id="onexDashScene" aria-hidden="true"></canvas>\n\n\n<div class="mob-bar" id="mobBar">\n  <button class="mob-menu-btn" id="mobMenuBtn" aria-label="منو">\n    <span class="mobile-bars" aria-hidden="true"><i></i><i></i><i></i></span>\n  </button>\n  <div class="mob-brand onex-approved-mobile"><div class="onex-mobile-mark"><svg class="onex-x-svg onex-x-shadow" viewBox="0 0 100 100" role="img" aria-label="ONEX logo"><defs><linearGradient id="mxa" x1="8" y1="8" x2="90" y2="92" gradientUnits="userSpaceOnUse"><stop stop-color="#2ec9ff"/><stop offset=".48" stop-color="#2376ff"/><stop offset="1" stop-color="#7b42ff"/></linearGradient><linearGradient id="mxb" x1="88" y1="10" x2="22" y2="93" gradientUnits="userSpaceOnUse"><stop stop-color="#6b50ff"/><stop offset=".62" stop-color="#843fff"/><stop offset="1" stop-color="#a855f7"/></linearGradient></defs><path d="M10 12h20l27 29-14 14L10 20z" fill="url(#mxa)"/><path d="M10 88h20l28-30-14-14L10 80z" fill="url(#mxa)"/><path d="M90 12H70L43 41l14 14 33-35z" fill="url(#mxb)"/><path d="M90 88H70L42 58l14-14 34 36z" fill="url(#mxb)"/><path d="M42 42 58 58 50 67 34 50z" fill="#0f5ce7" opacity=".78"/><path d="M58 42 42 58 50 67 66 50z" fill="#7137f0" opacity=".78"/></svg></div><div class="onex-mobile-copy"><div class="onex-brand-word">ONE<span class="x">X</span><span class="brand-vpn">PANEL</span></div></div></div>\n  <div class="mob-status"><i></i><span>آنلاین</span></div>\n</div>\n<div class="overlay" id="overlay"></div>\n\n<aside class="sidebar" id="sidebar">\n  <button class="sb-toggle" id="sbToggle" title="Toggle">\n    <span class="hexa-bars" aria-hidden="true"><i></i><i></i><i></i></span>\n  </button>\n  <div class="sb-logo onex-approved-brand" aria-label="ONEX Panel">\n    <div class="onex-sidebar-mark"><svg class="onex-x-svg onex-x-shadow" viewBox="0 0 100 100" role="img" aria-label="ONEX logo"><defs><linearGradient id="sxa" x1="8" y1="8" x2="90" y2="92" gradientUnits="userSpaceOnUse"><stop stop-color="#2ec9ff"/><stop offset=".48" stop-color="#2376ff"/><stop offset="1" stop-color="#7b42ff"/></linearGradient><linearGradient id="sxb" x1="88" y1="10" x2="22" y2="93" gradientUnits="userSpaceOnUse"><stop stop-color="#6b50ff"/><stop offset=".62" stop-color="#843fff"/><stop offset="1" stop-color="#a855f7"/></linearGradient></defs><path d="M10 12h20l27 29-14 14L10 20z" fill="url(#sxa)"/><path d="M10 88h20l28-30-14-14L10 80z" fill="url(#sxa)"/><path d="M90 12H70L43 41l14 14 33-35z" fill="url(#sxb)"/><path d="M90 88H70L42 58l14-14 34 36z" fill="url(#sxb)"/><path d="M42 42 58 58 50 67 34 50z" fill="#0f5ce7" opacity=".78"/><path d="M58 42 42 58 50 67 66 50z" fill="#7137f0" opacity=".78"/></svg></div>\n    <div class="onex-sidebar-copy"><div class="onex-brand-word">ONE<span class="x">X</span><span class="brand-vpn">PANEL</span></div></div>\n  </div>\n  <nav class="nav">\n    <div class="nav-sec" data-i18n="sec_panel">پنــــل</div>\n    <button class="nav-item on" data-page="dash" data-perm="dash">\n      <svg class="nav-ico nav-ico-dash" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 13.5 12 4l8 9.5"/><path d="M6.5 12.5V20h11v-7.5"/><path d="M9.5 20v-4.5h5V20"/></svg>\n      <span class="nav-label" data-i18n="nav_dash">داشبـورد</span>\n    </button>\n    <button class="nav-item" data-page="configs" data-perm="configs">\n      <svg class="nav-ico nav-ico-configs" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="m7 4 10 0 3 3v10l-3 3H7l-3-3V7l3-3Z"/><path d="m8 8 8 8M16 8l-8 8"/></svg>\n      <span class="nav-label" data-i18n="nav_configs">کانفیگ\u200cهـا</span>\n    </button>\n    <button class="nav-item" data-page="groups" data-perm="configs">\n      <svg class="nav-ico nav-ico-groups" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="9" r="3"/><circle cx="17" cy="10" r="2.5"/><path d="M3.5 20c.5-3.1 2.4-4.7 5.5-4.7s5 1.6 5.5 4.7"/><path d="M14 15.8c2.8-.8 5 .5 6 3.2"/></svg>\n      <span class="nav-label" data-i18n="nav_groups">گروه\u200cهـا</span>\n    </button>\n    <button class="nav-item" data-page="create" data-perm="create">\n      <svg class="nav-ico nav-ico-create" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="m14.5 3 6.5 6.5-8.5 8.5-5.5 1 1-5.5L16.5 5Z"/><path d="m13 5 6 6"/><path d="M4 20h4"/></svg>\n      <span class="nav-label" data-i18n="nav_create">ساخت کانفیـگ</span>\n    </button>\n    <button class="nav-item" data-page="stats" data-perm="stats">\n      <svg class="nav-ico nav-ico-stats" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19V5"/><path d="M4 19h16"/><path d="m7 15 3-4 3 2 5-7"/><path d="M16 6h2v2"/></svg>\n      <span class="nav-label" data-i18n="nav_stats">امـار</span>\n    </button>\n    <button class="nav-item" data-page="logs" data-perm="logs">\n      <svg class="nav-ico nav-ico-logs" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="3" width="14" height="18" rx="3"/><path d="M8.5 8h7M8.5 12h7M8.5 16h4"/><circle cx="17" cy="17" r="2.2" fill="currentColor" stroke="none"/></svg>\n      <span class="nav-label" data-i18n="nav_logs">لاگ فعالیـت</span>\n    </button>\n    <div class="nav-sec" data-i18n="sec_sys">سیستـم</div>\n    <button class="nav-item" data-page="telegram" data-perm="telegram">\n      <svg class="nav-ico nav-ico-telegram" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="m6.5 12 11-4-3.2 8-2.1-3-3.2-1Z"/><path d="m12.2 13 2.1-2.2"/></svg>\n      <span class="nav-label" data-i18n="nav_telegram">ربات ONEX</span>\n    </button>\n    <button class="nav-item" data-page="news" data-perm="news">\n      <svg class="nav-ico nav-ico-news" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="3"/><path d="M8 8h8M8 12h5M8 16h8"/><path d="m15 12 1.5 1.5L19 11"/></svg>\n      <span class="nav-label" data-i18n="nav_news">اخبـار</span>\n    </button>\n    <button class="nav-item" data-page="admins" data-perm="admins">\n      <svg class="nav-ico nav-ico-admins" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3 20 6v5c0 5-3.2 8.2-8 10-4.8-1.8-8-5-8-10V6l8-3Z"/><circle cx="12" cy="10" r="2.2"/><path d="M8.5 16c.8-2 2-2.8 3.5-2.8s2.7.8 3.5 2.8"/></svg>\n      <span class="nav-label" data-i18n="nav_admins">ادمین\u200cهـا</span>\n    </button>\n    <div class="nav-sec nav-sec-appearance" data-i18n="sec_appearance">شخصی\u200cسازی</div>\n    <button class="nav-item" data-page="theme" data-perm="settings">\n      <svg class="nav-ico nav-ico-theme" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3a9 9 0 0 0 0 18h1.5a2.5 2.5 0 0 0 0-5H12a2 2 0 0 1 0-4h1.5a2.5 2.5 0 0 0 0-4H12Z"/><circle cx="7.5" cy="9" r="1"/><circle cx="9" cy="5.8" r="1"/><circle cx="14.5" cy="6" r="1"/><circle cx="17" cy="9" r="1"/></svg>\n      <span class="nav-label" data-i18n="nav_theme">تم و ظاهر</span><span class="nav-new-badge" data-i18n="nav_theme_new">جدید</span>\n    </button>\n    <button class="nav-item" data-page="settings" data-perm="settings">\n      <svg class="nav-ico nav-ico-settings" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M5 7h14M5 12h14M5 17h14"/><circle cx="9" cy="7" r="2.2" fill="var(--bg2)"/><circle cx="15" cy="12" r="2.2" fill="var(--bg2)"/><circle cx="11" cy="17" r="2.2" fill="var(--bg2)"/></svg>\n      <span class="nav-label" data-i18n="nav_settings">تنظیمـات</span>\n    </button>\n  </nav>\n  <div class="sb-foot">\n    <button type="button" class="btn danger" id="panelLogoutBtn" onclick="logoutPanel()">\n      <svg class="logout-ico" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M9 4H5a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h4"/><path d="m14 8 4 4-4 4"/><path d="M18 12H8"/><path d="M13 4v3M13 17v3" opacity=".45"/></svg>\n      <span data-i18n="logout">خروج</span>\n    </button>\n  </div>\n</aside>\n\n<main class="main" id="main">\n\n<div class="onex-topbar">\n  <div class="onex-topbar-brand" aria-label="ONEX Panel">\n    <div class="onex-topbar-mark"><svg class="onex-x-svg onex-x-shadow" viewBox="0 0 100 100" role="img" aria-label="ONEX logo"><defs><linearGradient id="txa" x1="8" y1="8" x2="90" y2="92" gradientUnits="userSpaceOnUse"><stop stop-color="#2ec9ff"/><stop offset=".48" stop-color="#2376ff"/><stop offset="1" stop-color="#7b42ff"/></linearGradient><linearGradient id="txb" x1="88" y1="10" x2="22" y2="93" gradientUnits="userSpaceOnUse"><stop stop-color="#6b50ff"/><stop offset=".62" stop-color="#843fff"/><stop offset="1" stop-color="#a855f7"/></linearGradient></defs><path d="M10 12h20l27 29-14 14L10 20z" fill="url(#txa)"/><path d="M10 88h20l28-30-14-14L10 80z" fill="url(#txa)"/><path d="M90 12H70L43 41l14 14 33-35z" fill="url(#txb)"/><path d="M90 88H70L42 58l14-14 34 36z" fill="url(#txb)"/><path d="M42 42 58 58 50 67 34 50z" fill="#0f5ce7" opacity=".78"/><path d="M58 42 42 58 50 67 66 50z" fill="#7137f0" opacity=".78"/></svg></div>\n    <div class="onex-topbar-copy"><div class="onex-brand-word">ONE<span class="x">X</span><span class="brand-vpn">PANEL</span></div></div>\n  </div>\n  <div class="top-server"><span class="top-dot"></span><b>سرور آنلاین</b><span class="top-sep"></span><small id="topHost">—</small><span class="top-sep"></span><small id="topUptime">Uptime: —</small></div>\n  <div class="top-actions">\n    <div class="hexa-swatches" id="hexaSwatches" aria-label="رنگ\u200cبندی داشبورد">\n      <button type="button" data-p="#38d9ff" data-s="#8b5cf6" class="active" aria-label="Aurora"></button>\n      <button type="button" data-p="#d4f24a" data-s="#ffb84d" aria-label="Lime"></button>\n      <button type="button" data-p="#ff3d8b" data-s="#ff8a3d" aria-label="Rose"></button>\n      <button type="button" data-p="#34f5a0" data-s="#22c1ee" aria-label="Emerald"></button>\n      <button type="button" data-p="#b57bff" data-s="#ff5ecf" aria-label="Violet"></button>\n    </div>\n    <div class="top-setting-group" aria-label="Language controls">\n      <button type="button" class="top-setting-btn" id="topLangFa" onclick="setLang(\'fa\')">فارسی</button>\n      <button type="button" class="top-setting-btn" id="topLangEn" onclick="setLang(\'en\')">EN</button>\n    </div>\n    <div class="top-notify-wrap">\n      <button type="button" class="top-notify-btn" id="topNotifyBtn" onclick="toggleNotifications()" aria-expanded="false"><span class="notify-bell">🔔</span><span class="notify-label">اعلان\u200cها</span><span class="notify-badge" id="notifyBadge">0</span></button>\n      <div class="top-notify-panel" id="topNotifyPanel" hidden>\n        <div class="notify-panel-head"><b id="notifyPanelTitle">اعلان\u200cها</b><button type="button" onclick="toggleNotifications(false)">×</button></div>\n        <div id="notifyList" class="notify-list"><div class="notify-empty">اعلان جدیدی وجود ندارد.</div></div>\n      </div>\n    </div>\n  </div>\n</div>\n\n<div class="onex-control-dock" aria-label="کنترل\u200cهای سریع ONEX">\n  <button type="button" class="onex-3d-control" onclick="toggleTheme()">\n    <span class="control-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg></span>\n    <span class="control-copy"><span class="control-title" id="themeLabel" data-i18n="theme">تم روشن</span><span class="control-sub">THEME CONTROL</span></span>\n  </button>\n  <button type="button" class="onex-3d-control" onclick="refreshAll()">\n    <span class="control-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.5 9a9 9 0 0 1 14.1-3.4L23 10M1 14l5.4 4.4A9 9 0 0 0 20.5 15"/></svg></span>\n    <span class="control-copy"><span class="control-title" data-i18n="refresh_stats">بروزرسانی آمار</span><span class="control-sub">LIVE STATISTICS</span></span>\n  </button>\n  <button type="button" class="onex-3d-control" onclick="panelUpdate()">\n    <span class="control-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M21 12a9 9 0 0 0-9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M3 12a9 9 0 0 0 9 9 9.75 9.75 0 0 0 6.74-2.74L21 16"/><path d="M16 16h5v5"/></svg></span>\n    <span class="control-copy"><span class="control-title" data-i18n="refresh_panel">بروزرسانی پنل</span><span class="control-sub">PANEL UPDATE</span></span>\n  </button>\n</div>\n<section class="page on" id="page-dash">\n<div class="grid" id="onexHome">\n<section class="hero glass area">\n  <h1><span data-i18n="home_welcome">خوش آمدی به</span> <span class="home-brand">ONEX</span><em class="home-user" id="homeUser"></em></h1>\n  <p><span data-i18n="home_last">آخرین بروزرسانی</span> <b class="mono" id="lastUpd">--:--:--</b></p>\n  <div class="versions">\n    <div class="version"><span data-i18n="home_ver_panel">نسخه پنل</span> <b id="homePanelVer">v__ONEX_VERSION__</b></div>\n    <div class="version"><span data-i18n="home_ver_core">هسته</span> <b id="homeCoreVer">sing-box</b></div>\n    <div class="version"><span data-i18n="home_core_state">وضعیت هسته</span> <b id="homeCoreState">—</b></div>\n  </div>\n  <div class="alert" id="homeAlert" hidden>⚠ <span id="homeAlertText"></span><button type="button" onclick="homeShowExpiring()" data-i18n="home_alert_btn">مشاهده</button></div>\n  <div class="actions">\n    <button type="button" class="primary" onclick="goPage(\'create\')">＋ <span data-i18n="nav_create">ساخت کانفیگ</span></button>\n    <button type="button" id="heroRefresh" onclick="homeRefresh(true)">↻ <span data-i18n="home_refresh">بروزرسانی</span></button>\n  </div>\n</section>\n<section class="dock glass area">\n  <button type="button" onclick="toggleTheme()"><span class="icon">◐</span><span><strong id="homeThemeLbl">تم روشن</strong><small>THEME CONTROL</small></span></button>\n  <button type="button" onclick="homeRefresh(true)"><span class="icon">↻</span><span><strong data-i18n="refresh_stats">بروزرسانی آمار</strong><small>LIVE STATISTICS</small></span></button>\n  <button type="button" onclick="panelUpdate()"><span class="icon">⇩</span><span><strong data-i18n="refresh_panel">بروزرسانی پنل</strong><small>PANEL UPDATE</small></span></button>\n</section>\n<section class="metrics glass area">\n  <div class="metric"><label data-i18n="home_conns">اتصالات فعال</label><strong id="homeConns">—</strong><small data-i18n="home_live">لحظه\u200cای</small></div>\n  <div class="metric"><label data-i18n="home_traffic">ترافیک کل</label><strong id="homeTraffic">—</strong><small id="homeTrafficSub">—</small></div>\n  <div class="metric"><label data-i18n="home_links">کانفیگ\u200cها</label><strong id="homeLinks">—</strong><small id="homeLinksSub">—</small></div>\n  <div class="metric"><label data-i18n="home_uptime">آپتایم سرور</label><strong id="homeUptime">—</strong><small data-i18n="home_since">از آخرین راه\u200cاندازی</small></div>\n</section>\n<section class="chart glass area">\n  <div class="head"><h2 data-i18n="home_chart">توزیع ساعتی ترافیک</h2>\n    <div class="home-legend"><span><i class="lg-d"></i><em data-i18n="home_down">دانلود</em></span><span><i class="lg-u"></i><em data-i18n="home_up">آپلود</em></span><button type="button" class="home-link-btn" onclick="goPage(\'stats\')"><span data-i18n="home_full_stats">آمار کامل</span> ←</button></div>\n  </div>\n  <div class="home-chart-wrap"><canvas id="traffic"></canvas><div class="home-chart-tip" id="homeChartTip" hidden></div></div>\n</section>\n<section class="health glass area">\n  <div class="head"><h2 data-i18n="home_health">وضعیت سرور</h2><span class="mono home-core-pill" id="homeCorePill">NATIVE CORE</span></div>\n  <div class="rows">\n    <div class="healthrow"><span class="tag">CPU</span><div><label data-i18n="home_cpu">پردازنده</label><div class="track"><i id="homeCpuBar" style="--w:0%"></i></div></div><b id="homeCpu">—</b></div>\n    <div class="healthrow"><span class="tag">RAM</span><div><label data-i18n="home_ram">حافظه</label><div class="track"><i id="homeRamBar" style="--w:0%"></i></div></div><b id="homeRam">—</b></div>\n    <div class="healthrow"><span class="tag">SSD</span><div><label data-i18n="home_disk">دیسک</label><div class="track"><i id="homeDiskBar" style="--w:0%"></i></div></div><b id="homeDisk">—</b></div>\n    <div class="healthrow"><span class="tag">NET</span><div><label data-i18n="home_net">شبکه</label><div class="track"><i id="homeNetBar" style="--w:0%"></i></div></div><b id="homeNet">—</b></div>\n  </div>\n</section>\n<section class="telegram glass area">\n  <div><div class="tgmark">➤</div><h3 data-i18n="home_tg_title">کانال رسمی ONEX</h3><p data-i18n="home_tg_sub">اطلاعیه\u200cها، نسخه\u200cهای جدید و پشتیبانی مستقیم</p></div>\n  <a href="https://t.me/V2rayTun0" target="_blank" rel="noopener" data-i18n="home_tg_join">عضویت در کانال · @V2rayTun0</a>\n</section>\n<section class="recent glass area">\n  <div class="head"><h2 data-i18n="home_recent">کانفیگ\u200cهای اخیر</h2><button type="button" class="home-link-btn" onclick="goPage(\'configs\')"><span data-i18n="home_view_all">مشاهده همه</span> ←</button></div>\n  <div class="table">\n    <div class="tr headrow"><span data-i18n="home_col_name">نام</span><span data-i18n="home_col_proto">پروتکل</span><span data-i18n="home_col_usage">مصرف</span><span data-i18n="home_col_exp">انقضا</span><span></span></div>\n    <div id="homeRecent"><div class="home-empty">…</div></div>\n  </div>\n</section>\n<section class="quick glass area">\n  <div class="head"><h2 data-i18n="home_quick">عملیات سریع</h2></div>\n  <div class="quicklist">\n    <button type="button" class="quickitem" onclick="goPage(\'create\')"><span class="icon">＋</span><span><span data-i18n="nav_create">ساخت کانفیگ</span><br><small data-i18n="home_q_create_sub">ایجاد کانفیگ جدید</small></span></button>\n    <button type="button" class="quickitem" onclick="goPage(\'configs\')"><span class="icon">☷</span><span><span data-i18n="home_q_configs">مدیریت کانفیگ\u200cها</span><br><small data-i18n="home_q_configs_sub">مشاهده و ویرایش</small></span></button>\n    <button type="button" class="quickitem" onclick="goPage(\'groups\')"><span class="icon">◉</span><span><span data-i18n="nav_groups">گروه\u200cها</span><br><small data-i18n="home_q_groups_sub">گروه\u200cهای اشتراک</small></span></button>\n    <button type="button" class="quickitem" onclick="goPage(\'telegram\')"><span class="icon">➤</span><span><span data-i18n="nav_telegram">ربات ONEX</span><br><small data-i18n="home_q_bot_sub">مدیریت ربات</small></span></button>\n  </div>\n</section>\n<section class="info glass area">\n  <div class="head"><h2 data-i18n="home_info">اطلاعات سرور</h2></div>\n  <dl class="kv">\n    <dt data-i18n="home_host">میزبان</dt><dd id="homeHost">—</dd>\n    <dt data-i18n="home_domain">دامنه</dt><dd id="homeDomain">—</dd>\n    <dt data-i18n="home_core">هسته</dt><dd id="homeCore">—</dd>\n    <dt data-i18n="home_region">منطقه</dt><dd id="homeRegion">—</dd>\n    <dt data-i18n="home_os">سیستم</dt><dd id="homeOs">—</dd>\n  </dl>\n</section>\n</div>\n  <div class="onex-footer"><span><b>Fast · Secure · Stable</b></span><span>ساخته\u200cشده توسط <b>Mehtif</b> · کانال رسمی <b>@V2rayTun0</b></span></div>\n</section>\n<section class="page" id="page-configs">\n<div class="ocx-root">\n  <header class="ocx-top">\n    <div><h1>کانفیگ\u200cهای من</h1><p>مدیریت کانفیگ\u200cها، دقیق و سریع</p></div>\n    <button type="button" class="ocx-delall" onclick="openDeleteAllConfigs()" title="حذف همه کانفیگ\u200cها" aria-label="حذف همه کانفیگ\u200cها"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6M10 11v5M14 11v5"/></svg></button>\n  </header>\n  <section class="ocx-summary">\n    <div class="ocx-metric"><strong id="cfgStatTotal">0</strong><small>کل</small></div>\n    <div class="ocx-metric ok"><strong id="cfgStatActive">0</strong><small>فعال</small></div>\n    <div class="ocx-metric bad"><strong id="cfgStatExpired">0</strong><small>منقضی</small></div>\n    <div class="ocx-metric"><strong id="cfgStatUsed">0 B</strong><small>مصرف کل</small></div>\n  </section>\n  <label class="ocx-search"><input id="cfgSearch" dir="rtl" placeholder="جستجوی نام، پروتکل یا UUID" oninput="filterConfigs()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-3.5-3.5"/></svg></label>\n  <nav class="ocx-filters" id="cfgFilterRow">\n    <button type="button" class="ocx-filter on" data-status="all" onclick="setCfgStatus(\'all\',this)">همه</button>\n    <button type="button" class="ocx-filter" data-status="active" onclick="setCfgStatus(\'active\',this)">فعال</button>\n    <button type="button" class="ocx-filter" data-status="off" onclick="setCfgStatus(\'off\',this)">خاموش</button>\n    <button type="button" class="ocx-filter" data-status="expired" onclick="setCfgStatus(\'expired\',this)">منقضی</button>\n    <button type="button" class="ocx-filter" data-status="hi" onclick="setCfgStatus(\'hi\',this)">پرمصرف</button>\n  </nav>\n  <div class="ocx-bar">\n    <button type="button" class="ocx-new" onclick="goPage(\'create\')">+ کانفیگ جدید</button>\n    <label class="ocx-all"><input type="checkbox" id="chkAll" onchange="toggleSelectAll(this.checked);updateBulkBar()"><span>انتخاب همه</span><small id="cfgVisibleCount">0 مورد</small></label>\n  </div>\n  <div id="cfgCards" class="ocx-cards"><div class="ocx-empty">در حال بارگذاری...</div></div>\n</div>\n<table id="linksTable" style="display:none"><tbody></tbody></table>\n<style>\n#page-configs .ocx-root{\n--o-a:var(--accent,#3b82f6);--o-a2:var(--accent2,#60a5fa);--o-p:var(--purple,#8b5cf6);\n--o-text:var(--t1,#f1f4fb);--o-muted:var(--t3,#949daf);--o-sub:var(--t2,#a4acbb);\n--o-green:#4fd89a;--o-yellow:#ffbf42;--o-red:#ee7084;--o-mono:\'JetBrains Mono\',ui-monospace,monospace;\n--o-glass:color-mix(in srgb,var(--o-a) 7%,rgb(16 18 26 / .55));\n--o-glass-2:color-mix(in srgb,var(--o-a) 12%,rgb(22 25 36 / .6));\n--o-line:color-mix(in srgb,var(--o-a2) 24%,rgb(255 255 255 / .06));\n--o-line-2:color-mix(in srgb,var(--o-a2) 45%,rgb(255 255 255 / .08));\n--o-track:color-mix(in srgb,var(--o-a) 14%,rgb(255 255 255 / .07));\n--o-shine:inset 0 1px 0 rgb(255 255 255 / .08);\n--o-blur:blur(18px) saturate(140%);\ncolor:var(--o-text);font-family:Vazirmatn,system-ui,sans-serif;max-width:760px;margin:0 auto;padding:4px 2px 24px;text-align:left;direction:ltr;background:none}\nhtml.light #page-configs .ocx-root{\n--o-text:#161a24;--o-muted:#6b7385;--o-sub:#4d5566;\n--o-glass:color-mix(in srgb,var(--o-a) 6%,rgb(255 255 255 / .72));\n--o-glass-2:color-mix(in srgb,var(--o-a) 10%,rgb(255 255 255 / .82));\n--o-line:color-mix(in srgb,var(--o-a) 18%,rgb(20 24 40 / .06));\n--o-line-2:color-mix(in srgb,var(--o-a) 40%,rgb(20 24 40 / .08));\n--o-track:color-mix(in srgb,var(--o-a) 14%,rgb(20 24 40 / .08));\n--o-shine:inset 0 1px 0 rgb(255 255 255 / .9);\n--o-green:#16a36a;--o-yellow:#c98a0c;--o-red:#dc4a64}\n#page-configs .ocx-root *{box-sizing:border-box}\n#page-configs .ocx-root button{font-family:inherit;cursor:pointer}\n#page-configs .ocx-glass,#page-configs .ocx-top,#page-configs .ocx-summary,#page-configs .ocx-search,#page-configs .ocx-card,#page-configs .ocx-bar{backdrop-filter:var(--o-blur);-webkit-backdrop-filter:var(--o-blur)}\n#page-configs .ocx-top{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px;padding:14px 16px;border:1px solid var(--o-line);border-radius:20px;background:var(--o-glass);box-shadow:var(--o-shine),0 14px 34px rgb(0 0 0 / .14)}\n#page-configs .ocx-top>div{direction:rtl;text-align:left}\n#page-configs .ocx-top h1{margin:0;font-size:21px;font-weight:900;color:var(--o-text)}\n#page-configs .ocx-top p{margin:3px 0 0;color:var(--o-muted);font-size:12px}\n#page-configs .ocx-delall{width:42px;height:42px;flex:none;display:grid;place-items:center;border:1px solid color-mix(in srgb,var(--o-red) 45%,transparent);border-radius:13px;background:color-mix(in srgb,var(--o-red) 12%,transparent);color:var(--o-red);padding:0;transition:background .2s}\n#page-configs .ocx-delall svg{width:20px;height:20px}\n#page-configs .ocx-delall:hover{background:color-mix(in srgb,var(--o-red) 22%,transparent)}\n#page-configs .ocx-summary{display:flex;gap:26px;align-items:flex-end;margin-bottom:12px;padding:14px 18px;border:1px solid var(--o-line);border-radius:20px;background:var(--o-glass);box-shadow:var(--o-shine)}\n#page-configs .ocx-metric strong{display:block;font:700 24px/1.2 var(--o-mono);color:var(--o-text);white-space:nowrap}\n#page-configs .ocx-metric.ok strong{color:var(--o-green)}\n#page-configs .ocx-metric.bad strong{color:var(--o-red)}\n#page-configs .ocx-metric small{display:block;margin-top:3px;color:var(--o-muted);font-size:12px}\n#page-configs .ocx-search{display:flex;align-items:center;gap:12px;height:52px;padding:0 16px;margin-bottom:12px;border:1px solid var(--o-line);border-radius:16px;background:var(--o-glass);box-shadow:var(--o-shine);color:var(--o-muted);transition:border-color .2s}\n#page-configs .ocx-search:focus-within{border-color:var(--o-a)}\n#page-configs .ocx-search svg{width:21px;height:21px;flex:none;color:var(--o-a2)}\n#page-configs .ocx-search input{text-align:right;flex:1;min-width:0;height:100%;border:0;outline:0;background:none;color:var(--o-text);font:500 14px Vazirmatn,sans-serif;box-shadow:none;padding:0}\n#page-configs .ocx-search input::placeholder{color:var(--o-muted)}\n#page-configs .ocx-root .ocx-search input{background:transparent!important;border:0!important;box-shadow:none!important}\n#page-configs .ocx-filters{display:flex;gap:9px;overflow-x:auto;margin-bottom:14px;padding-bottom:2px;scrollbar-width:none}\n#page-configs .ocx-filter{height:42px;padding:0 18px;border-radius:99px;border:1px solid var(--o-line);background:var(--o-glass);backdrop-filter:var(--o-blur);-webkit-backdrop-filter:var(--o-blur);color:var(--o-sub);font-weight:700;font-size:13px;white-space:nowrap;transition:background .2s,border-color .2s,color .2s}\n#page-configs .ocx-filter:hover{border-color:var(--o-line-2)}\n#page-configs .ocx-filter.on{background:linear-gradient(135deg,var(--o-a),var(--o-p));border-color:transparent;color:#fff;box-shadow:0 8px 20px color-mix(in srgb,var(--o-a) 30%,transparent)}\n#page-configs .ocx-bar{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:16px;padding:8px 8px 8px 14px;border:1px solid var(--o-line);border-radius:18px;background:var(--o-glass);box-shadow:var(--o-shine)}\n#page-configs .ocx-new{height:46px;padding:0 22px;border:0;border-radius:13px;background:linear-gradient(135deg,var(--o-a),var(--o-p));color:#fff;font-weight:900;font-size:14px;box-shadow:0 10px 24px color-mix(in srgb,var(--o-a) 32%,transparent);transition:transform .15s,filter .2s}\n#page-configs .ocx-new:hover{filter:brightness(1.08);transform:translateY(-1px)}\n#page-configs .ocx-all{direction:rtl;display:flex;align-items:center;gap:7px;color:var(--o-muted);font-size:12px;cursor:pointer}\n#page-configs .ocx-all input{width:18px;height:18px;accent-color:var(--o-a);margin:0}\n#page-configs .ocx-all small{font-family:var(--o-mono);color:var(--o-muted)}\n#page-configs .ocx-cards{display:grid;gap:14px}\n#page-configs .ocx-card{position:relative;background:var(--o-glass);border:1px solid var(--o-line);border-radius:22px;padding:19px 19px 12px;box-shadow:var(--o-shine),0 16px 38px rgb(0 0 0 / .16);transition:border-color .2s,transform .25s cubic-bezier(.16,1,.3,1)}\n#page-configs .ocx-card:hover{border-color:var(--o-line-2);transform:translateY(-2px)}\n#page-configs .ocx-card.dead{opacity:.78}\n#page-configs .ocx-head{display:flex;align-items:flex-start;gap:16px}\n#page-configs .ocx-ring{--pct:0;--ring:var(--o-a);width:108px;height:108px;flex:none;border-radius:50%;display:grid;place-items:center;position:relative}\n#page-configs .ocx-ring:before{content:"";position:absolute;inset:0;border-radius:50%;background:conic-gradient(var(--ring) calc(var(--pct)*1%),var(--o-track) 0);-webkit-mask:radial-gradient(farthest-side,transparent calc(100% - 10px),#000 calc(100% - 9px));mask:radial-gradient(farthest-side,transparent calc(100% - 10px),#000 calc(100% - 9px))}\n#page-configs .ocx-ring-c{position:relative;z-index:1;text-align:center;line-height:1.15}\n#page-configs .ocx-ring-c b{display:block;font:700 24px var(--o-mono);color:var(--o-text)}\n#page-configs .ocx-ring-c small{display:block;margin-top:3px;font-size:11px;color:var(--o-muted);font-family:var(--o-mono)}\n#page-configs .ocx-info{flex:1;min-width:0;padding-top:3px;text-align:left}\n#page-configs .ocx-name{font-size:21px;font-weight:900;color:var(--o-text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;direction:auto}\n#page-configs .ocx-proto{display:inline-block;margin-top:7px;padding:5px 10px;border-radius:8px;background:color-mix(in srgb,var(--o-a) 16%,transparent);border:1px solid color-mix(in srgb,var(--o-a2) 22%,transparent);color:var(--o-a2);font:700 12px var(--o-mono);direction:ltr;letter-spacing:.03em}\n#page-configs .ocx-meta{display:flex;flex-wrap:wrap;gap:14px;margin-top:12px;color:var(--o-sub);font-size:12.5px}\n#page-configs .ocx-meta span{direction:rtl;display:inline-flex;align-items:center;gap:5px;white-space:nowrap}\n#page-configs .ocx-meta svg{width:15px;height:15px}\n#page-configs .ocx-meta .st-on{color:var(--o-green);font-weight:800}\n#page-configs .ocx-meta .st-off{color:var(--o-muted);font-weight:800}\n#page-configs .ocx-meta .st-dead{color:var(--o-red);font-weight:800}\n#page-configs .ocx-meta .warn{color:var(--o-yellow)}\n#page-configs .ocx-meta .gone{color:var(--o-red)}\n#page-configs .ocx-toggle{width:50px;height:28px;flex:none;border:1px solid var(--o-line);border-radius:99px;background:var(--o-track);position:relative;padding:0;transition:background .2s,border-color .2s}\n#page-configs .ocx-toggle:after{content:"";position:absolute;top:3px;left:3px;width:20px;height:20px;border-radius:50%;background:var(--o-muted);transition:transform .25s cubic-bezier(.16,1,.3,1),background .2s}\n#page-configs .ocx-toggle.on{background:color-mix(in srgb,var(--o-a) 35%,transparent);border-color:color-mix(in srgb,var(--o-a) 60%,transparent)}\n#page-configs .ocx-toggle.on:after{background:var(--o-a);transform:translateX(22px);box-shadow:0 0 10px color-mix(in srgb,var(--o-a) 70%,transparent)}\n#page-configs .ocx-rule{border-top:1px dashed var(--o-line);margin:18px 0 12px}\n#page-configs .ocx-actions{display:flex;align-items:center;gap:10px}\n#page-configs .ocx-dots{width:30px;height:44px;flex:none;border:0;background:transparent;color:var(--o-muted);font-size:24px;padding:0;line-height:1}\n#page-configs .ocx-dots:hover{color:var(--o-a2)}\n#page-configs .ocx-act{flex:1;min-width:0;height:46px;display:inline-flex;align-items:center;justify-content:center;gap:7px;border:1px solid var(--o-line);border-radius:14px;background:var(--o-glass-2);color:var(--o-text);font-weight:800;font-size:14px;padding:0 8px;box-shadow:var(--o-shine);transition:border-color .2s,background .2s}\n#page-configs .ocx-act svg{width:18px;height:18px;flex:none;color:var(--o-a2)}\n#page-configs .ocx-act:hover{border-color:var(--o-line-2);background:color-mix(in srgb,var(--o-a) 18%,transparent)}\n#page-configs .ocx-chk{flex:none;display:grid;place-items:center;width:34px;height:44px;cursor:pointer}\n#page-configs .ocx-chk input{width:22px;height:22px;margin:0;accent-color:var(--o-a)}\n#page-configs .ocx-menu{direction:rtl;position:absolute;right:14px;bottom:64px;width:210px;display:none;flex-direction:column;padding:7px;background:color-mix(in srgb,var(--o-a) 10%,rgb(18 20 30 / .92));border:1px solid var(--o-line-2);border-radius:15px;box-shadow:0 18px 44px rgb(0 0 0 / .45);backdrop-filter:blur(22px) saturate(150%);-webkit-backdrop-filter:blur(22px) saturate(150%);z-index:20}\nhtml.light #page-configs .ocx-menu{background:color-mix(in srgb,var(--o-a) 6%,rgb(255 255 255 / .95));box-shadow:0 18px 44px rgb(20 24 40 / .16)}\n#page-configs .ocx-menu.open{display:flex}\n#page-configs .ocx-menu button{height:40px;border:0;border-radius:10px;background:transparent;color:var(--o-text);text-align:right;padding:0 12px;font-size:13px;font-weight:700}\n#page-configs .ocx-menu button:hover{background:color-mix(in srgb,var(--o-a) 16%,transparent)}\n#page-configs .ocx-menu .danger{color:var(--o-red)}\n#page-configs .ocx-empty{direction:rtl;text-align:center;padding:46px 16px;border:1px dashed var(--o-line);border-radius:20px;background:var(--o-glass);color:var(--o-muted);font-size:13px}\n#page-configs .ocx-root :focus-visible{outline:2px solid var(--o-a2);outline-offset:2px}\n@media(max-width:560px){\n#page-configs .ocx-summary{gap:18px;padding:12px 14px}\n#page-configs .ocx-metric strong{font-size:21px}\n#page-configs .ocx-card{padding:17px 14px 11px}\n#page-configs .ocx-head{gap:13px}\n#page-configs .ocx-ring{width:96px;height:96px}\n#page-configs .ocx-ring-c b{font-size:21px}\n#page-configs .ocx-name{font-size:18px}\n#page-configs .ocx-meta{gap:10px;font-size:11.5px}\n#page-configs .ocx-actions{gap:7px}\n#page-configs .ocx-act{height:44px;font-size:13px;gap:5px}\n#page-configs .ocx-dots{width:22px}\n#page-configs .ocx-chk{width:26px}\n}\n</style>\n</section>\n\n<style>\n.create-edit-actions{display:flex;gap:8px;align-items:center}.config-edit-banner{display:flex;align-items:center;gap:10px;margin:0 0 12px;padding:11px 13px;border:1px solid rgba(var(--accent-rgb),.28);border-radius:15px;background:linear-gradient(135deg,rgba(var(--accent-rgb),.10),rgba(var(--accent-rgb),.08));color:#dcecff}.config-edit-banner-icon{width:34px;height:34px;display:grid;place-items:center;border-radius:10px;background:rgba(var(--accent-rgb),.12);border:1px solid rgba(var(--accent-rgb),.24);color:var(--accent2);font-size:16px}.config-edit-banner div{min-width:0;display:flex;flex-direction:column;gap:2px}.config-edit-banner b{font-size:11px;color:#fff}.config-edit-banner small{font-size:9px;color:var(--accent2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:48vw}.config-edit-banner-hint{margin-right:auto;color:var(--t3);font-size:8px}@media(max-width:560px){.config-edit-banner{align-items:flex-start}.config-edit-banner-hint{display:none}.create-edit-actions{margin-top:2px}.create-edit-actions .btn{font-size:9px;height:36px;padding:0 10px}}\n</style>\n<section class="page" id="page-create">\n  <div class="page-head">\n    <div><div class="page-title"><svg id="createPageTitleIcon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 5v14M5 12h14"/></svg><span id="createPageTitle" data-i18n="nav_create">ساخت کانفیگ</span></div><div class="page-sub" id="createPageSubtitle">ایجاد کانفیگ جدید با تنظیمات پایه و پیشرفته</div></div>\n    <div class="create-edit-actions"><button type="button" class="btn btn-sm" id="cancelConfigEditBtn" onclick="cancelConfigEdit()" hidden>انصراف</button></div>\n  </div>\n  <div class="config-edit-banner" id="configEditBanner" hidden><span class="config-edit-banner-icon">✎</span><div><b>در حال ویرایش کانفیگ</b><small id="configEditBannerName">—</small></div><span class="config-edit-banner-hint">تغییرات فقط با «ذخیره تغییرات» اعمال می\u200cشود</span></div>\n  <div class="cfgx" id="cfgx">\n    <div class="cfgx-card cfgx-basic">\n      <div class="cfgx-head"><span class="cfgx-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L8 18l-4 1 1-4Z"/></svg></span><div><b>اطلاعات پایه</b><small>نام کانفیگ و گروه اشتراک</small></div></div>\n      <div class="cfgx-row2">\n        <div class="field cfgx-field"><label data-i18n="label_name">نام</label>\n          <div class="cfgx-inline">\n            <input id="cName" placeholder="ONEX PANEL" autocomplete="off">\n            <button type="button" class="cfgx-icon-btn" onclick="randomName()" title="Random" aria-label="نام تصادفی">\n              <svg viewBox="0 0 24 24" width="17" height="17" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l5 5"/></svg>\n            </button>\n          </div>\n        </div>\n        <div class="field cfgx-field"><label>گروه اشتراک</label><select id="cSubGroup"><option value="">بدون گروه (عمومی)</option></select></div>\n      </div>\n      <small class="cfgx-note">با انتخاب گروه، این کانفیگ بعد از ساخت خودکار عضو همان گروه می\u200cشود.</small>\n      <div class="field protocol-field cfgx-legacy-proto" data-protocol-picker="cProto"><label data-i18n="label_proto">پروتکـل</label><select id="cProto" class="protocol-native" tabindex="-1" aria-hidden="true"></select><button type="button" class="protocol-trigger" data-for="cProto" onclick="window.openProtocolPicker&&window.openProtocolPicker(\'cProto\')"><span class="protocol-trigger-main"><span class="protocol-trigger-icon">🚀</span><span class="protocol-trigger-text"><span class="protocol-trigger-name">ONEX WB</span><span class="protocol-trigger-sub">برای تغییر پروتکل، اینجا بزنید</span></span></span><span class="protocol-trigger-arrow">⌄</span></button></div>\n    </div>\n\n    <div class="cfgx-card">\n      <div class="cfgx-head"><span class="cfgx-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2 3 7l9 5 9-5-9-5Z"/><path d="m3 12 9 5 9-5"/><path d="m3 17 9 5 9-5"/></svg></span><div><b>🚀 انتخاب پروتکل</b><small>پروتکل اصلی کانفیگ - ۶ پروتکل ONEX VIP</small></div><span class="cfgx-badge" id="cfgxProtoBadge">—</span></div>\n      <div class="cfgx-proto-grid" id="cfgxProtoGrid"><div class="cfgx-empty">در حال بارگذاری پروتکل\u200cها...</div></div>\n      <label class="all-proto-toggle cfgx-all-toggle" style="margin:10px 0 12px">\n        <span><b>ساخت همه پروتکل\u200cهای Railway</b><small>یک کانفیگ بساز و هر ۶ پروتکل ONEX را داخل یک ساب قرار بده</small></span>\n        <input id="cAllProtocols" type="checkbox">\n        <i aria-hidden="true"></i>\n      </label>\n    </div>\n\n    <div class="cfgx-card">\n      <div class="cfgx-head"><span class="cfgx-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="2"/><rect x="14" y="3" width="7" height="7" rx="2"/><rect x="3" y="14" width="7" height="7" rx="2"/><path d="M17.5 14v7M14 17.5h7"/></svg></span><div><b>🔀 ترکیب پروتکل\u200cها</b><small>هر تعداد پروتکل که انتخاب کنی، همه داخل یک ساب ساخته می\u200cشوند · ۶ پروتکل ONEX VIP</small></div><span class="cfgx-badge" id="cfgxBundleBadge">0</span></div>\n      <div id="protocolBundleOptions" class="cfgx-bundle"></div>\n      <small class="cfgx-note">پروتکل اصلی خودکار داخل ترکیب قرار می\u200cگیرد.</small>\n    </div>\n\n    <div class="cfgx-card">\n      <div class="cfgx-head"><span class="cfgx-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3a9 9 0 1 0 9 9"/><path d="M12 7v5l3 2"/></svg></span><div><b>محدودیت\u200cها</b><small>۰ یعنی نامحدود</small></div></div>\n      <div class="cfgx-limits">\n        <div class="field cfgx-field"><label data-i18n="label_limit">محدودیت حجم</label><div class="cfgx-inline"><input id="cLimit" type="number" value="0" min="0" inputmode="decimal"><select id="cUnit" class="cfgx-unit"><option>GB</option><option>MB</option><option>KB</option></select></div></div>\n        <div class="field cfgx-field"><label data-i18n="label_days">انقضـا (روز)</label><div class="cfgx-inline"><input id="cDays" type="number" value="0" min="0" inputmode="numeric"><span class="cfgx-suffix">روز</span></div></div>\n        <div class="field cfgx-field"><label data-i18n="label_ip">محدودیت IP</label><div class="cfgx-inline"><input id="cIp" type="number" value="0" min="0" inputmode="numeric"><span class="cfgx-suffix">IP</span></div></div>\n        <div class="field cfgx-field"><label data-i18n="label_speed">سرعـت (Mbps)</label><div class="cfgx-inline"><input id="cSpeed" type="number" value="0" min="0" inputmode="decimal"><span class="cfgx-suffix">Mbps</span></div></div>\n      </div>\n    </div>\n\n    <label class="all-proto-toggle cfgx-switch cfgx-card cfgx-adb"><span><b>Ad Blocker برای همین کانفیگ</b><small>فقط روی این کانفیگ اعمال می\u200cشود</small></span><input id="cAdBlockEnabled" type="checkbox"><i aria-hidden="true"></i></label>\n    <div class="card advanced-config-card">\n      <button type="button" class="advanced-toggle" id="advancedToggle" onclick="toggleAdvancedConfig()">\n        <span class="advanced-toggle-icon">⚙</span>\n        <span class="advanced-toggle-copy"><b>تنظیمات پیشرفته</b><small>TLS، SNI، اثر انگشت، پورت، شبکه، هدر، مسیر و تنظیمات تخصصی</small></span>\n        <span class="advanced-toggle-state" id="advancedToggleState">باز کردن</span>\n        <span class="advanced-chevron" id="advancedChevron">⌄</span>\n      </button>\n\n      <div id="advancedConfigPanel" class="advanced-config-panel" hidden>\n        <div class="advanced-note"><span>✦</span><div><b>کنترل دستی کامل</b><small>هر گزینه یا به Listener واقعی sing-box اعمال می\u200cشود یا در اعتبارسنجی به\u200cعنوان کلاینت\u200cمحور/پشتیبانی\u200cنشده مشخص می\u200cشود. قبل از ذخیره، اعتبارسنجی و Preview را اجرا کنید.</small></div></div>\n\n        <div class="advanced-section">\n          <div class="advanced-section-head"><span class="advanced-section-icon">🔐</span><div><b>TLS / Reality</b><small>امنیت اتصال و مشخصات TLS</small></div></div>\n          <div class="advanced-grid">\n            <div class="field"><label>حالت امنیت</label><select id="advTlsMode"><option value="tls">TLS</option><option value="reality">Reality</option><option value="none">بدون TLS</option></select></div>\n            <div class="field"><label>SNI / Server Name</label><input id="advSni" placeholder="example.com"></div>\n            <div class="field"><label>ALPN</label><input id="advAlpn" placeholder="h2,http/1.1"></div>\n            <div class="field"><label>حداقل TLS</label><select id="advTlsMin"><option>1.2</option><option>1.3</option></select></div>\n            <div class="field"><label>حداکثر TLS</label><select id="advTlsMax"><option>1.3</option><option>1.2</option></select></div>\n            <div class="field"><label>Certificate Path</label><input id="advCertPath" placeholder="/etc/ssl/cert.pem"></div>\n            <div class="field"><label>Key Path</label><input id="advKeyPath" placeholder="/etc/ssl/key.pem"></div>\n            <label class="advanced-check"><input id="advAllowInsecure" type="checkbox"><span><b>Allow Insecure</b><small>عدم اعتبارسنجی گواهی</small></span></label>\n          </div>\n          <div class="advanced-subcard" id="advRealityBox">\n            <div class="advanced-subtitle">Reality</div>\n            <div class="advanced-grid">\n              <div class="field"><label>Public Key</label><input id="advRealityPk" placeholder="Public key"></div>\n              <div class="field"><label>Private Key</label><input id="advRealitySk" type="password" placeholder="اختیاری؛ فقط سرور"></div>\n              <div class="field"><label>Short ID</label><input id="advRealitySid" placeholder="8 تا 16 رقم/حرف hex"></div>\n              <div class="field"><label>Spider X</label><input id="advRealitySpider" placeholder="/"></div>\n              <div class="field"><label>Reality Fingerprint</label><select id="advRealityFp"><option>chrome</option><option>firefox</option><option>safari</option><option>edge</option><option>ios</option><option>android</option><option>randomized</option></select></div>\n              <div class="field"><label>Reality Handshake Server</label><input id="advRealityHandshake" placeholder="www.cloudflare.com"></div>\n              <div class="field"><label>Handshake Port</label><input id="advRealityHandshakePort" type="number" value="443" min="1" max="65535"></div>\n              <div class="field"><label>Max Time Difference</label><input id="advRealityMaxDiff" placeholder="1m"></div>\n            </div>\n          </div>\n        </div>\n\n        <div class="advanced-section">\n          <div class="advanced-section-head"><span class="advanced-section-icon">🌐</span><div><b>Host / SNI / Header</b><small>آدرس مقصد، Host و اطلاعات لایه HTTP</small></div></div>\n          <div class="advanced-grid">\n            <div class="field"><label>Server Address</label><input id="advAddress" placeholder="domain.com یا IP"></div>\n            <div class="field"><label>Host Header</label><input id="advHost" placeholder="domain.com"></div>\n            <div class="field"><label>Path</label><input id="advPath" placeholder="/ws/uuid یا مسیر سفارشی"></div>\n            <div class="field"><label>Authority / :authority</label><input id="advAuthority" placeholder="domain.com"></div>\n            <div class="field"><label>User-Agent</label><input id="advUserAgent" placeholder="اختیاری"></div>\n            <div class="field"><label>Service Name</label><input id="advServiceName" placeholder="برای gRPC"></div>\n          </div>\n        </div>\n\n        <div class="advanced-section">\n          <div class="advanced-section-head"><span class="advanced-section-icon">🧬</span><div><b>Fingerprint</b><small>uTLS و اثر انگشت کلاینت</small></div></div>\n          <div class="advanced-grid">\n            <div class="field"><label>Fingerprint</label><select id="advFp"><option>chrome</option><option>firefox</option><option>safari</option><option>ios</option><option>android</option><option>edge</option><option>360</option><option>qq</option><option>random</option><option>randomized</option></select></div>\n            <label class="advanced-check"><input id="advFpEnabled" type="checkbox" checked><span><b>فعال</b><small>ارسال fingerprint در لینک</small></span></label>\n            <label class="advanced-check"><input id="advFpRandom" type="checkbox"><span><b>Randomize</b><small>اثر انگشت تصادفی</small></span></label>\n          </div>\n        </div>\n\n        <div class="advanced-section">\n          <div class="advanced-section-head"><span class="advanced-section-icon">〽</span><div><b>Network / Transport</b><small>انتخاب شبکه، مود و پارامترهای انتقال</small></div></div>\n          <div class="advanced-grid">\n            <div class="field"><label>Network</label><select id="advNetwork"><option value="ws">WebSocket</option><option value="xhttp">XHTTP</option><option value="grpc">gRPC</option><option value="tcp">TCP</option><option value="http">HTTP</option><option value="h2">HTTP/2</option><option value="quic">QUIC</option><option value="kcp">mKCP</option></select></div>\n            <div class="field"><label>XHTTP Mode</label><select id="advNetworkMode"><option value="">Auto</option><option>packet-up</option><option>stream-up</option><option>stream-one</option></select></div>\n            <div class="field"><label>HTTP Version</label><select id="advHttpVersion"><option>1.1</option><option>2</option><option>3</option></select></div>\n            <div class="field"><label>Packet Encoding</label><input id="advPacketEncoding" placeholder="xudp / packetaddr / ..."></div>\n            <div class="field"><label>Early Data</label><input id="advEarlyData" type="number" min="0" max="65535" value="0"></div>\n            <label class="advanced-check"><input id="advPadding" type="checkbox"><span><b>Padding</b><small>در صورت پشتیبانی پروتکل</small></span></label>\n          </div>\n        </div>\n\n        <div class="advanced-section">\n          <div class="advanced-section-head"><span class="advanced-section-icon">↔</span><div><b>Port Manager</b><small>پورت اصلی و پورت\u200cهای جایگزین</small></div></div>\n          <div class="port-manager">\n            <div class="port-add-row"><input id="advPortInput" type="number" min="1" max="65535" placeholder="مثلاً 443"><button type="button" class="btn btn-sm" onclick="addAdvancedPort()">＋ افزودن پورت</button></div>\n            <div id="advancedPorts" class="advanced-port-list"></div>\n            <small class="advanced-help">در این معماری، پورت اول به\u200cعنوان پورت اصلی کانفیگ استفاده می\u200cشود؛ فعال\u200cسازی واقعی چند پورت نیازمند listener جداگانه در سرور/پلتفرم است.</small>\n          </div>\n        </div>\n\n        <div class="advanced-section">\n          <div class="advanced-section-head"><span class="advanced-section-icon">🧭</span><div><b>Routing / Sniffing</b><small>مسیر، استراتژی دامنه و تشخیص ترافیک</small></div></div>\n          <div class="advanced-grid">\n            <div class="field"><label>Domain Strategy</label><select id="advDomainStrategy"><option value="">Auto</option><option value="prefer_ipv4">Prefer IPv4</option><option value="prefer_ipv6">Prefer IPv6</option><option value="ipv4_only">IPv4 Only</option><option value="ipv6_only">IPv6 Only</option></select></div>\n            <div class="field"><label>Route / Outbound</label><input id="advRoute" placeholder="direct / block / proxy"></div>\n            <label class="advanced-check"><input id="advSniff" type="checkbox"><span><b>Sniff</b><small>تشخیص مقصد از ترافیک</small></span></label>\n            <label class="advanced-check"><input id="advProxyProtocol" type="checkbox"><span><b>Proxy Protocol</b><small>در صورت پشتیبانی upstream</small></span></label>\n            <label class="advanced-check"><input id="advSniffOverride" type="checkbox"><span><b>Sniff Override</b><small>جایگزینی مقصد با دامنه تشخیص\u200cداده\u200cشده</small></span></label>\n            <div class="field"><label>Sniff Timeout</label><input id="advSniffTimeout" placeholder="300ms"></div>\n          </div>\n        </div>\n\n        <div class="advanced-section">\n          <div class="advanced-section-head"><span class="advanced-section-icon">🖥</span><div><b>Listener / Socket</b><small>تمام تنظیمات سطح Listener سرور</small></div></div>\n          <div class="advanced-grid">\n            <div class="field"><label>Listen Address</label><input id="advListen" placeholder="0.0.0.0"></div>\n            <div class="field"><label>Bind Interface</label><input id="advBindInterface" placeholder="eth0"></div>\n            <div class="field"><label>Routing Mark</label><input id="advRoutingMark" type="number" min="0" placeholder="0"></div>\n            <div class="field"><label>Network Namespace</label><input id="advNetns" placeholder="namespace/path"></div>\n            <div class="field"><label>TCP Keep Alive</label><input id="advTcpKeepAlive" placeholder="5m"></div>\n            <div class="field"><label>Keep Alive Interval</label><input id="advTcpKeepAliveInterval" placeholder="75s"></div>\n            <div class="field"><label>UDP Timeout</label><input id="advUdpTimeout" placeholder="5m"></div>\n            <label class="advanced-check"><input id="advReuseAddr" type="checkbox" checked><span><b>Reuse Address</b><small>سوکت قابل استفاده مجدد</small></span></label>\n            <label class="advanced-check"><input id="advTfo" type="checkbox"><span><b>TCP Fast Open</b><small>فعال\u200cسازی TFO</small></span></label>\n            <label class="advanced-check"><input id="advMptcp" type="checkbox"><span><b>TCP Multi Path</b><small>نیازمند پشتیبانی سیستم</small></span></label>\n            <label class="advanced-check"><input id="advDisableKeepAlive" type="checkbox"><span><b>Disable TCP Keep Alive</b><small>غیرفعال کردن keepalive</small></span></label>\n            <label class="advanced-check"><input id="advUdpFragment" type="checkbox"><span><b>UDP Fragment</b><small>برای Listenerهای UDP</small></span></label>\n          </div>\n        </div>\n\n                <div class="advanced-section">\n          <div class="advanced-section-head"><span class="advanced-section-icon">⌘</span><div><b>Custom Headers</b><small>هدرهای سفارشی برای HTTP</small></div></div>\n          <div class="field"><label>هدرهای اضافی (هر خط یک Header: Value)</label><textarea id="advExtraHeaders" rows="4" placeholder="X-Forwarded-Proto: https\nCache-Control: no-cache"></textarea></div>\n        </div>\n\n        <div id="advancedValidationStatus" class="advanced-validation-status" aria-live="polite"></div>\n        <div id="advancedCapabilityStatus" class="advanced-validation-status ok" style="display:block" aria-live="polite">وضعیت قابلیت\u200cها پس از انتخاب پروتکل نمایش داده می\u200cشود.</div>\n        <div class="advanced-preview" id="advancedPreviewBox" hidden><div class="advanced-preview-head"><b>Config Preview</b><button type="button" class="btn btn-sm" onclick="copyAdvancedPreview()">کپی</button></div><pre id="advancedPreviewCode"></pre></div>\n        <div class="advanced-actions">\n          <button type="button" class="btn btn-p" onclick="validateAdvancedConfig(true)">✓ اعتبارسنجی و پیش\u200cنمایش</button>\n          <button type="button" class="btn" onclick="saveAdvancedDraft()">💾 ذخیره تنظیمات</button>\n          <button type="button" class="btn" onclick="resetAdvancedConfig()">↺ بازنشانی</button>\n          <button type="button" class="btn" onclick="copyAdvancedJson()">{ } کپی JSON</button>\n        </div>\n      </div>\n    </div>\n\n    <div class="cfgx-card cfgx-summary" id="cfgxSummary">\n      <div class="cfgx-head"><span class="cfgx-ico"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11l3 3 8-8"/><path d="M20 12v7a2 2 0 0 1-2 2H6a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h9"/></svg></span><div><b>خلاصه و اعتبارسنجی</b><small>پیش از ساخت بررسی کنید</small></div></div>\n      <div class="cfgx-sum-rows">\n        <div><span>نام</span><b id="cfgxSumName">—</b></div>\n        <div><span>گروه</span><b id="cfgxSumGroup">—</b></div>\n        <div><span>پروتکل\u200cها</span><b id="cfgxSumProto">—</b></div>\n        <div><span>محدودیت\u200cها</span><b id="cfgxSumLimits">—</b></div>\n        <div><span>Ad Blocker</span><b id="cfgxSumAdb">خاموش</b></div>\n      </div>\n      <div class="cfgx-valid" id="cfgxValid">✓ اطلاعات معتبر است</div>\n      <button id="manualConfigSubmit" class="btn btn-p cfgx-submit" type="button" onclick="doManualCreate()">\n        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><path d="M12 5v14M5 12h14"/></svg>\n        <span data-i18n="btn_create">ساخت</span>\n      </button>\n    </div>\n\n  </div>\n</section>\n\n\n<section class="page" id="page-groups">\n  <div class="group-manager">\n    <div class="group-hero">\n      <div class="group-hero-copy">\n        <div class="group-hero-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="9" cy="9" r="3"/><circle cx="17" cy="10" r="2.5"/><path d="M3.5 20c.5-3.1 2.4-4.7 5.5-4.7s5 1.6 5.5 4.7"/><path d="M14 15.8c2.8-.8 5 .5 6 3.2"/></svg></div>\n        <div><div class="group-hero-kicker">ONEX GROUP MANAGER</div><h1>مدیریت گروه\u200cها</h1><p>ساخت گروه\u200cهای اشتراک، مدیریت کانفیگ\u200cها و کنترل پروتکل\u200cهای قابل ارائه</p></div>\n      </div>\n      <div class="group-stats">\n        <div class="group-stat"><span>کل گروه\u200cها</span><b id="groupTotal">0</b><i>◉</i></div>\n        <div class="group-stat"><span>کاربران فعال</span><b id="groupActiveUsers">0</b><i>●</i></div>\n        <button class="group-create-btn" type="button" onclick="openGroupModal()"><span>＋</span>ساخت گروه جدید</button>\n      </div>\n    </div>\n\n    <div class="group-workspace">\n      <div class="group-list-pane">\n        <div class="group-list-toolbar">\n          <div class="group-search"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/></svg><input id="groupSearch" placeholder="جستجوی گروه..." oninput="renderGroupList()"></div>\n          <div class="group-filters"><button type="button" class="group-filter on" data-filter="all" onclick="setGroupFilter(\'all\',this)">همه</button><button type="button" class="group-filter" data-filter="active" onclick="setGroupFilter(\'active\',this)">فعال <em></em></button><button type="button" class="group-filter" data-filter="inactive" onclick="setGroupFilter(\'inactive\',this)">غیرفعال <em></em></button></div>\n        </div>\n        <div id="groupsList" class="group-cards-list"><div class="group-empty">در حال بارگذاری گروه\u200cها...</div></div>\n      </div>\n\n      <aside class="group-detail-pane" id="groupDetailPane">\n        <div class="group-detail-empty"><div class="group-detail-empty-icon">◉</div><b>یک گروه را انتخاب کنید</b><span>برای مشاهده لینک اشتراک، پروتکل\u200cها و کانفیگ\u200cهای گروه</span></div>\n      </aside>\n    </div>\n  </div>\n\n  <div class="modal-bg" id="groupModal" hidden>\n    <div class="modal group-modal" role="dialog" aria-modal="true">\n      <div class="modal-head"><div><div class="card-title" id="groupModalTitle">ساخت گروه جدید</div><div class="page-sub">اطلاعات گروه اشتراک</div></div><button class="modal-x" type="button" onclick="closeGroupModal()">×</button></div>\n      <div class="field"><label>نام گروه</label><input id="groupFormName" maxlength="60" placeholder="مثلاً ONEX VIP"></div>\n      <div class="field"><label>توضیحات</label><input id="groupFormDesc" maxlength="200" placeholder="توضیح کوتاه برای این گروه"></div>\n      <div class="field"><label>رمز عبور اشتراک <small>(اختیاری)</small></label><input id="groupFormPassword" type="password" placeholder="خالی = بدون رمز"></div>\n      <div class="group-modal-actions"><button class="btn" type="button" onclick="closeGroupModal()">لغو</button><button class="btn btn-p" type="button" id="groupModalSave" onclick="saveGroupForm()">ساخت گروه</button></div>\n    </div>\n  </div>\n\n  <div class="modal-bg" id="groupQrModal" hidden>\n    <div class="modal group-qr-modal" role="dialog" aria-modal="true">\n      <div class="modal-head"><div><div class="card-title">QR Code گروه</div><div class="page-sub" id="groupQrTitle">لینک اشتراک</div></div><button class="modal-x" type="button" onclick="closeGroupQr()">×</button></div>\n      <div id="groupQrBox" class="group-qr-box"></div><div id="groupQrText" class="group-qr-text" dir="ltr"></div>\n      <div class="group-modal-actions"><button class="btn" onclick="copyText(document.getElementById(\'groupQrText\').textContent)">کپی لینک</button><button class="btn btn-p" onclick="closeGroupQr()">بستن</button></div>\n    </div>\n  </div>\n</section>\n<section class="page" id="page-stats">\n  <div class="stats-page-wrap">\n    <div class="stats-head-row">\n      <div>\n        <div class="page-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M3 3v18h18"/><path d="M7 16l4-8 4 5 5-8"/></svg><span data-i18n="nav_stats">آمار</span></div>\n        <div class="stats-subtitle">ترافیک و اتصالات، فیلتر زمانی و وضعیت لحظه\u200cای سرویس</div>\n      </div>\n      <button class="stats-refresh-btn" type="button" onclick="loadStatsDashboard(true)">↻ بروزرسانی</button>\n    </div>\n\n    <div class="stats-range" id="rangeTabs">\n      <button class="range-tab" data-r="day" onclick="setRange(\'day\',this)" data-i18n="r_day">روز</button>\n      <button class="range-tab" data-r="week" onclick="setRange(\'week\',this)" data-i18n="r_week">هفته</button>\n      <button class="range-tab on" data-r="month" onclick="setRange(\'month\',this)" data-i18n="r_month">ماه</button>\n      <button class="range-tab" data-r="all" onclick="setRange(\'all\',this)" data-i18n="r_all">کل</button>\n    </div>\n\n    <div class="stats-kpi-grid">\n      <div class="stats-kpi cyan"><span class="stats-kpi-icon">↓</span><div><small>دانلود</small><b id="stDownload">0 B</b><em id="stDownloadDelta">واقعی</em></div><div class="stats-spark" id="sparkDownload"></div></div>\n      <div class="stats-kpi pink"><span class="stats-kpi-icon">↑</span><div><small>آپلود</small><b id="stUpload">0 B</b><em id="stUploadDelta">واقعی</em></div><div class="stats-spark" id="sparkUpload"></div></div>\n      <div class="stats-kpi purple"><span class="stats-kpi-icon">↗</span><div><small>اتصالات فعال</small><b id="stConnections">0</b><em>لحظه\u200cای</em></div><div class="stats-kpi-mini-dot"></div></div>\n      <div class="stats-kpi blue"><span class="stats-kpi-icon">♣</span><div><small>کاربران فعال</small><b id="stUsers">0</b><em>کانفیگ فعال</em></div><div class="stats-kpi-mini-dot"></div></div>\n      <div class="stats-kpi violet"><span class="stats-kpi-icon">▤</span><div><small>کل کانفیگ\u200cها</small><b id="stConfigs">0</b><em id="stExpiredText">0 منقضی</em></div><div class="stats-kpi-mini-dot"></div></div>\n      <div class="stats-kpi green"><span class="stats-kpi-icon">▣</span><div><small>وضعیت سرور</small><b id="stServer">آنلاین</b><em id="stServerHost">ONEX</em></div><div class="online-pulse"></div></div>\n    </div>\n\n    <div class="stats-panel traffic-panel">\n      <div class="stats-panel-head"><div><b>نمودار ترافیک</b><small>دانلود و آپلود در ساعات ثبت\u200cشده</small></div><span class="stats-live-badge"><i></i> LIVE</span></div>\n      <div class="stats-legend"><span><i class="legend-download"></i>دانلود</span><span><i class="legend-upload"></i>آپلود</span><span id="statsChartRange">امروز</span></div>\n      <div class="traffic-chart-wrap"><svg id="trafficChart" viewBox="0 0 900 310" preserveAspectRatio="none" role="img" aria-label="نمودار ترافیک"></svg></div>\n    </div>\n\n    <div class="stats-two-col">\n      <div class="stats-panel uptime-panel"><div class="stats-panel-head"><div><b>آپتایم سرور</b><small>از زمان راه\u200cاندازی</small></div><span class="panel-icon">◷</span></div><div class="uptime-body"><div class="uptime-ring" id="uptimeRing"><span id="uptimePct">99.9%</span></div><div><small>زمان فعالیت</small><strong id="stUptime">—</strong><em id="stRequests">0 درخواست</em></div></div></div>\n      <div class="stats-panel server-panel"><div class="stats-panel-head"><div><b>وضعیت سرویس</b><small>اطلاعات لحظه\u200cای</small></div><span class="panel-icon">⌁</span></div><div class="server-stat-list"><div><span>آدرس پنل</span><b id="stHost">—</b></div><div><span>کانفیگ فعال</span><b id="stActiveConfigs">0</b></div><div><span>اتصالات</span><b id="stConn2">0</b></div><div><span>خطاها</span><b id="stErrors">0</b></div></div></div>\n    </div>\n\n    <div class="stats-panel panel-summary"><div class="stats-panel-head"><div><b>اطلاعات کل پنل</b><small>خلاصه عملکرد سرویس</small></div><span class="panel-icon">↗</span></div><div class="summary-grid"><div><span>کل کانفیگ\u200cها</span><b id="sumConfigs">0</b></div><div><span>فعال</span><b id="sumActive">0</b></div><div><span>مصرف کل</span><b id="sumTraffic">0 B</b></div><div><span>آپلود</span><b id="sumUpload">0 B</b></div><div><span>درخواست\u200cها</span><b id="sumRequests">0</b></div><div><span>گروه\u200cها</span><b id="sumGroups">0</b></div></div></div>\n  </div>\n</section>\n\n<section class="page" id="page-logs">\n  <div class="page-head logs-page-head">\n    <div>\n      <div class="page-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/></svg><span data-i18n="nav_logs">لاگ فعالیت</span></div>\n      <div class="logs-page-sub">تمام رویدادهای ثبت\u200cشده در پنل</div>\n    </div>\n    <div class="logs-head-actions">\n      <button class="btn btn-sm logs-refresh-btn" type="button" onclick="loadLogs(true)" title="بروزرسانی">\n        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="14" height="14"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.5 9a9 9 0 0 1 14.1-3.4L23 10"/></svg>\n        <span>بروزرسانی</span>\n      </button>\n    </div>\n  </div>\n\n  <div class="logs-toolbar card">\n    <div class="logs-search-wrap">\n      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="7"/><path d="m20 20-4-4"/></svg>\n      <input id="logsSearch" type="search" autocomplete="off" placeholder="جستجو در لاگ\u200cها..." oninput="applyLogFilters()">\n      <button type="button" class="logs-clear-search" onclick="clearLogSearch()" aria-label="پاک کردن جستجو">×</button>\n    </div>\n    <div class="logs-filter-row" id="logsFilterRow">\n      <button type="button" class="logs-filter on" data-log-filter="all" onclick="setLogFilter(\'all\',this)">همه <b id="logCountAll">0</b></button>\n      <button type="button" class="logs-filter" data-log-filter="link" onclick="setLogFilter(\'link\',this)">لینک\u200cها</button>\n      <button type="button" class="logs-filter" data-log-filter="sub" onclick="setLogFilter(\'sub\',this)">گروه\u200cها</button>\n      <button type="button" class="logs-filter" data-log-filter="auth" onclick="setLogFilter(\'auth\',this)">ورودها</button>\n      <button type="button" class="logs-filter" data-log-filter="admin" onclick="setLogFilter(\'admin\',this)">کاربران</button>\n      <button type="button" class="logs-filter" data-log-filter="system" onclick="setLogFilter(\'system\',this)">سیستم</button>\n    </div>\n    <div class="logs-advanced-row">\n      <label><span>سطح</span><select id="logsLevelFilter" onchange="applyLogFilters()"><option value="all">همه</option><option value="ok">موفق</option><option value="info">اطلاعات</option><option value="warn">هشدار</option><option value="err">خطا</option></select></label>\n      <label><span>تاریخ</span><input id="logsDateFilter" type="date" onchange="applyLogFilters()"></label>\n      <button type="button" class="logs-reset-filter" onclick="resetLogFilters()">حذف فیلترها</button>\n    </div>\n  </div>\n\n  <div class="logs-summary card">\n    <div class="logs-summary-item logs-summary-total"><span class="logs-summary-icon">↗</span><div><small>فعالیت امروز</small><strong id="logStatTotal">0</strong><em>رویداد</em></div></div>\n    <div class="logs-summary-item logs-summary-ok"><span class="logs-summary-icon">✓</span><div><small>موفق</small><strong id="logStatOk">0</strong><em>ثبت\u200cشده</em></div></div>\n    <div class="logs-summary-item logs-summary-change"><span class="logs-summary-icon">⚙</span><div><small>تغییر</small><strong id="logStatChange">0</strong><em>ویرایش</em></div></div>\n    <div class="logs-summary-item logs-summary-delete"><span class="logs-summary-icon">⌫</span><div><small>حذف</small><strong id="logStatDelete">0</strong><em>رویداد</em></div></div>\n  </div>\n\n  <div class="card logs-list-card">\n    <div class="logs-list-head"><div><b>رویدادهای اخیر</b><small id="logsResultText">در حال بارگذاری...</small></div><span class="logs-live-badge"><i></i> LIVE</span></div>\n    <div id="logsBox" class="logs-list"><div class="logs-empty">در حال بارگذاری...</div></div>\n  </div>\n\n  <div class="logs-footer card">\n    <button type="button" class="logs-advanced-btn" onclick="toggleLogAdvanced()"><span>⌄</span> فیلتر پیشرفته</button>\n    <button type="button" class="logs-clear-btn" onclick="clearAllLogs()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 7h16M10 11v6M14 11v6M6 7l1 13h10l1-13M9 7V4h6v3"/></svg> پاکسازی لاگ\u200cها</button>\n  </div>\n\n  <div class="logs-modal-bg" id="logsDetailBg" onclick="if(event.target===this)closeLogDetail()">\n    <div class="logs-detail-modal" role="dialog" aria-modal="true" aria-labelledby="logsDetailTitle">\n      <div class="logs-detail-head"><div><span id="logsDetailIcon" class="logs-detail-icon">•</span><div><b id="logsDetailTitle">جزئیات رویداد</b><small id="logsDetailMeta">—</small></div></div><button type="button" onclick="closeLogDetail()">×</button></div>\n      <div class="logs-detail-body" id="logsDetailBody"></div>\n      <div class="logs-detail-actions"><button type="button" class="btn btn-sm" onclick="closeLogDetail()">بستن</button></div>\n    </div>\n  </div>\n</section>\n\n<section class="page" id="page-theme">\n  <div class="page-head">\n    <div>\n      <div class="page-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3a9 9 0 0 0 0 18h1.5a2.5 2.5 0 0 0 0-5H12a2 2 0 0 1 0-4h1.5a2.5 2.5 0 0 0 0-4H12Z"/><circle cx="7.5" cy="9" r="1"/><circle cx="9" cy="5.8" r="1"/><circle cx="14.5" cy="6" r="1"/><circle cx="17" cy="9" r="1"/></svg><span data-i18n="theme_page_title">تم پنل ONEX</span></div>\n      <div class="page-sub" data-i18n="theme_page_sub">رنگ\u200cبندی پنل را انتخاب کنید؛ تغییرات به\u200cصورت زنده اعمال و در مرورگر ذخیره می\u200cشوند.</div>\n    </div>\n  </div>\n  <div class="onex-theme-layout">\n    <div class="onex-theme-preview card">\n      <div class="theme-preview-head"><div><span class="theme-kicker">ONEX PANEL</span><b data-i18n="theme_live_preview">پیش\u200cنمایش زنده</b></div><span class="theme-live-dot"><i></i> LIVE</span></div>\n      <div class="theme-preview-window">\n        <div class="theme-mini-sidebar"><div class="theme-mini-logo">N</div><span class="active"></span><span></span><span></span><span></span><span></span></div>\n        <div class="theme-mini-main">\n          <div class="theme-mini-top"><i></i><i></i><b>ONEX PANEL</b></div>\n          <div class="theme-mini-cards"><div></div><div></div><div></div></div>\n          <div class="theme-mini-content"><div class="theme-mini-chart"></div><div class="theme-mini-list"><span></span><span></span><span></span><span></span></div></div>\n        </div>\n      </div>\n      <div class="theme-current-row"><span data-i18n="theme_current">تم فعلی</span><strong id="themeCurrentName">ONEX Blue</strong><span id="themeCurrentSwatch" class="theme-current-swatch"></span></div>\n    </div>\n\n    <div class="onex-theme-panel card">\n      <div class="theme-panel-head"><div><span class="theme-kicker">COLOR PRESETS</span><h2 data-i18n="theme_presets">تم\u200cهای آماده</h2></div><button class="theme-reset-btn" type="button" onclick="resetOnexTheme()" data-i18n="theme_reset">بازنشانی</button></div>\n      <div class="theme-preset-grid" id="themePresetGrid"></div>\n      <div class="theme-custom-box">\n        <div class="theme-custom-head"><div><span class="theme-kicker">CUSTOM THEME</span><h3 data-i18n="theme_custom">رنگ\u200cبندی سفارشی</h3></div><span class="theme-custom-icon">✦</span></div>\n        <div class="theme-color-grid">\n          <label><span data-i18n="theme_primary">رنگ اصلی</span><input id="themePrimary" type="color" value="#3b82f6"></label>\n          <label><span data-i18n="theme_secondary">رنگ ثانویه</span><input id="themeSecondary" type="color" value="#8b5cf6"></label>\n          <label><span data-i18n="theme_background">پس\u200cزمینه</span><input id="themeBackground" type="color" value="#06060b"></label>\n          <label><span data-i18n="theme_card">کارت\u200cها</span><input id="themeCard" type="color" value="#12121c"></label>\n        </div>\n        <button class="theme-apply-custom" type="button" onclick="applyCustomOnexTheme()"><span data-i18n="theme_apply">اعمال رنگ سفارشی</span><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="m5 12 4 4L19 6"/></svg></button>\n      </div>\n      <div class="theme-mode-box">\n        <div><b data-i18n="theme_mode">حالت نمایش</b><small data-i18n="theme_mode_sub">تاریک، روشن یا هماهنگ با سیستم</small></div>\n        <div class="theme-mode-buttons">\n          <button type="button" data-mode="dark" onclick="setOnexThemeMode(\'dark\')">☾ <span data-i18n="theme_dark_mode">تاریک</span></button>\n          <button type="button" data-mode="light" onclick="setOnexThemeMode(\'light\')">☀ <span data-i18n="theme_light_mode">روشن</span></button>\n          <button type="button" data-mode="system" onclick="setOnexThemeMode(\'system\')">◐ <span data-i18n="theme_system_mode">خودکار</span></button>\n        </div>\n      </div>\n      <div class="theme-save-note"><span>✓</span><span data-i18n="theme_saved_note">انتخاب شما روی همین مرورگر ذخیره می\u200cشود و بعد از Refresh باقی می\u200cماند.</span></div>\n    </div>\n  </div>\n</section>\n\n<section class="page" id="page-settings">\n  <div class="page-head"><div><div class="page-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="3"/></svg><span data-i18n="nav_settings">تنظیمات</span></div></div></div>\n  <div class="card">\n    <div class="card-title" data-i18n="change_pw">تغییر نام کاربری و رمز عبور</div>\n    <div class="field"><label>نام کاربری جدید</label><input type="text" id="newUser" value="admin" autocomplete="username"></div>\n    <div class="field"><label data-i18n="pw_cur">رمز فعلـی</label><input type="password" id="pwCur"></div>\n    <div class="field"><label data-i18n="pw_new">رمـز جدیـد</label><input type="password" id="pwNew"></div>\n    <div class="field"><label data-i18n="pw_cf">تکـرار رمـز</label><input type="password" id="pwCf"></div>\n    <button class="btn btn-p" onclick="doChangePw()"><span data-i18n="btn_save">ذخیـره</span></button>\n  </div>\n  <div class="card">\n    <div class="card-title">Ad Blocker / مسدودکننده تبلیغات</div>\n    <p style="font-size:11px;color:var(--t3);line-height:1.8">دامنه\u200cهای تبلیغاتی را هر خط یک مورد وارد کنید. روی لینک\u200cهای رله\u200cای و هسته native اعمال می\u200cشود، نه فقط داخل مرورگر.</p>\n    <label class="all-proto-toggle" style="margin:12px 0"><span><b>فعال\u200cسازی مسدودکننده</b><small>مسدودکردن دامنه و زیردامنه</small></span><input id="adBlockEnabled" type="checkbox"><i aria-hidden="true"></i></label>\n    <textarea id="adBlockDomains" rows="7" placeholder="doubleclick.net&#10;googlesyndication.com&#10;ads.youtube.com&#10;ads.google.com" style="width:100%;resize:vertical"></textarea>\n    <button class="btn btn-p" style="margin-top:10px" onclick="saveAdBlocker()">ذخیره و اعمال</button>\n  </div>\n  \n  <div class="card onex-security-card">\n    <div class="onex-security-head">\n      <div>\n        <div class="card-title">امنیت بیشتر</div>\n        <p>مدیریت امنیت ورود، IPهای مسدود و نشست\u200cهای فعال پنل</p>\n      </div>\n      <span id="secHealth" class="security-health good">● خوب</span>\n    </div>\n\n    <div class="security-login-box">\n      <div class="security-section-title"><span>🔐</span><div><b>محدودیت تلاش ورود</b><small>پس از 5 تلاش ناموفق، IP به مدت 30 دقیقه مسدود می\u200cشود.</small></div></div>\n      <div class="security-login-meta">\n        <span>حداکثر تلاش: <b id="secMaxAttempts">5</b></span>\n        <span>مدت قفل: <b id="secLockMinutes">30 دقیقه</b></span>\n        <button class="btn btn-sm" onclick="loadSecurity()">↻ بروزرسانی وضعیت</button>\n      </div>\n    </div>\n\n    <div class="security-stats">\n      <div><span>⛔</span><small>IPهای مسدود شده</small><b id="secLockedCount">0</b></div>\n      <div><span>⚠</span><small>ورودهای ناموفق</small><b id="secFailedCount">0</b></div>\n      <div><span>▣</span><small>نشست\u200cهای فعال</small><b id="secSessionCount">0</b></div>\n      <button class="security-stat-action" onclick="showSecurityDetails()">☷<small>مشاهده آمار جزئی\u200cتر</small></button>\n    </div>\n\n    <div class="security-action-row">\n      <div><b>مدیریت IPهای مسدود</b><small>مشاهده و رفع مسدودی IPهایی که به دلیل تلاش ناموفق قفل شده\u200cاند.</small></div>\n      <button class="btn btn-sm" onclick="showBlockedIps()">مدیریت IPهای مسدود</button>\n    </div>\n\n    <div class="security-log-box">\n      <div class="security-section-title"><span>◷</span><div><b>ورودهای اخیر</b><small>آخرین ورودهای موفق و ناموفق به پنل</small></div><button class="btn btn-sm" onclick="showSecurityDetails()">مشاهده همه</button></div>\n      <div id="secRecentLogins" class="security-recent-list"><div class="security-empty">در حال بارگذاری...</div></div>\n    </div>\n\n    <div class="security-action-row danger-row">\n      <div><b>خروج از همه دستگاه\u200cها</b><small>با این گزینه تمام نشست\u200cهای فعال شما در سایر دستگاه\u200cها قطع می\u200cشود.</small></div>\n      <button class="btn btn-sm btn-d" onclick="revokeAllSessions()">خروج از همه دستگاه\u200cها</button>\n    </div>\n\n    <div id="secDetails" class="security-details" hidden>\n      <div class="security-details-head"><b>جزئیات امنیتی</b><button type="button" onclick="hideSecurityDetails()">×</button></div>\n      <div id="secDetailsBody"></div>\n    </div>\n  </div>\n<div class="card">\n    <div class="card-title">بــک آپ و بازیابــی ONEX</div>\n    <p style="font-size:12px;color:var(--t3);line-height:1.8;margin-bottom:14px">یک نسخه پشتیبان از اطلاعات پروژه ONEX تهیه کنید و در صورت نیاز روی پنل جدید بازیابی کنید.</p>\n\n    <div class="g2" style="margin-bottom:12px">\n      <button class="btn btn-p" style="width:100%" onclick="downloadBackup(\'full\')">دانلود بک\u200cآپ کامل ONEX</button>\n      <button class="btn btn-p" style="width:100%;background:linear-gradient(135deg,var(--purple),var(--purple))" onclick="downloadBackup(\'users\')">دانلود بک\u200cآپ کاربران</button>\n      <button class="btn btn-p" style="width:100%;background:linear-gradient(135deg,#10b981,#059669)" onclick="downloadBackup(\'bot\')">دانلود بک\u200cآپ ربات</button>\n    </div>\n\n    <div class="field">\n      <label>وارد کردن بک\u200cآپ کامل ONEX</label>\n      <input type="file" id="restoreFullFile" accept="application/json,.json" style="padding:10px">\n      <button class="btn btn-sm btn-d" style="margin-top:8px" onclick="restoreFull()">جایگزینی کامل پروژه</button>\n    </div>\n\n    <div class="field" style="margin-top:12px">\n      <label>وارد کردن بک\u200cآپ کاربران</label>\n      <input type="file" id="restoreUsersFile" accept="application/json,.json" style="padding:10px">\n      <div style="display:flex;gap:8px;margin-top:8px;flex-wrap:wrap">\n        <button class="btn btn-sm" onclick="restoreUsers(\'merge\')">ادغام با فعلی</button>\n        <button class="btn btn-sm btn-d" onclick="restoreUsers(\'replace\')">جایگزینی کامل</button>\n      </div>\n    </div>\n\n    <div class="field" style="margin-top:12px">\n      <label>وارد کردن بک\u200cآپ ربات</label>\n      <input type="file" id="restoreBotFile" accept="application/json,.json" style="padding:10px">\n      <button class="btn btn-sm" style="margin-top:8px" onclick="restoreBot()">بازیابی ربات</button>\n    </div>\n  </div>\n</section>\n\n\n<style>\n/* ONEX SECURITY CARD — scoped only to Settings > Security */\n.onex-security-card{position:relative;overflow:hidden}.onex-security-head{display:flex;align-items:flex-start;justify-content:space-between;gap:14px}.onex-security-head .card-title{margin-bottom:4px}.onex-security-head p{margin:0;color:var(--t3);font-size:11px;line-height:1.8}.security-health{white-space:nowrap;padding:7px 11px;border-radius:999px;font-size:10px;font-weight:800}.security-health.good{color:#42e3a5;background:rgba(34,197,94,.08);border:1px solid rgba(34,197,94,.22)}.security-health.warn{color:#fbbf24;background:rgba(245,158,11,.08);border:1px solid rgba(245,158,11,.22)}.security-health.bad{color:#fb7185;background:rgba(244,63,94,.08);border:1px solid rgba(244,63,94,.22)}.security-login-box,.security-log-box,.security-action-row{margin-top:13px;padding:14px;border:1px solid var(--card-b);border-radius:16px;background:color-mix(in srgb, rgb(9 9 9 / .16) 92%, var(--accent))}.security-section-title{display:flex;align-items:center;gap:10px}.security-section-title>span{width:34px;height:34px;display:grid;place-items:center;border-radius:11px;background:rgba(var(--accent-rgb),.12);font-size:15px}.security-section-title b{display:block;color:var(--t1);font-size:12px}.security-section-title small{display:block;color:var(--t3);font-size:9px;line-height:1.7;margin-top:2px}.security-section-title>.btn{margin-right:auto}.security-login-meta{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:12px;color:var(--t3);font-size:10px}.security-login-meta span{padding:7px 10px;border-radius:10px;background:rgba(var(--accent-rgb),.06);border:1px solid rgba(var(--accent2-rgb),.10)}.security-login-meta b{color:var(--t1)}.security-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:9px;margin-top:13px}.security-stats>div,.security-stat-action{min-height:74px;padding:10px;border-radius:14px;border:1px solid var(--card-b);background:color-mix(in srgb, rgb(9 9 9 / .12) 92%, var(--accent))}.security-stats>div{display:flex;flex-direction:column;justify-content:center;align-items:center;text-align:center}.security-stats>div span{font-size:15px;margin-bottom:3px}.security-stats small{color:var(--t3);font-size:8px}.security-stats b{font-size:18px;margin-top:2px}.security-stats>div:nth-child(1) b{color:var(--accent2)}.security-stats>div:nth-child(2) b{color:#fbbf24}.security-stat-action{display:flex;align-items:center;justify-content:center;gap:7px;cursor:pointer;color:var(--accent2);font:800 9px Vazirmatn,sans-serif}.security-stat-action small{color:var(--accent2)}.security-action-row{display:flex;align-items:center;justify-content:space-between;gap:12px}.security-action-row b{display:block;font-size:11px}.security-action-row small{display:block;color:var(--t3);font-size:8px;line-height:1.7;margin-top:3px}.security-recent-list{margin-top:10px;max-height:190px;overflow:auto}.security-recent-row{display:grid;grid-template-columns:76px minmax(0,1fr) auto;gap:9px;align-items:center;padding:8px 0;border-top:1px solid rgba(var(--accent2-rgb),.08);font-size:9px}.security-recent-state{padding:4px 7px;border-radius:8px;font-size:8px}.security-recent-state.ok{color:#4ade80;background:rgba(34,197,94,.08)}.security-recent-state.bad{color:#fb7185;background:rgba(244,63,94,.08)}.security-recent-meta{color:var(--t3);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.security-empty{padding:12px;text-align:center;color:var(--t3);font-size:9px}.security-details{margin-top:12px;border:1px solid rgba(var(--accent2-rgb),.15);border-radius:14px;overflow:hidden}.security-details-head{display:flex;align-items:center;justify-content:space-between;padding:10px 12px;background:rgba(var(--accent-rgb),.06)}.security-details-head button{border:0;background:none;color:var(--t3);font-size:18px;cursor:pointer}.security-detail-row{display:flex;justify-content:space-between;gap:10px;padding:9px 12px;border-top:1px solid rgba(var(--accent2-rgb),.07);font-size:9px}.security-detail-row span{color:var(--t3)}.security-detail-row b{direction:ltr;text-align:left}.danger-row{border-color:rgba(244,63,94,.16)}\n@media(max-width:650px){.security-stats{grid-template-columns:repeat(2,1fr)}.security-action-row{align-items:flex-start;flex-direction:column}.security-action-row .btn{width:100%}.security-login-meta{align-items:stretch;flex-direction:column}.security-login-meta .btn{width:100%}.security-section-title>.btn{margin-right:0;margin-left:auto}.security-recent-row{grid-template-columns:64px minmax(0,1fr);}.security-recent-state{grid-row:1 / span 2;grid-column:1}.security-recent-meta{grid-column:2}.security-health{font-size:9px}}\nhtml.light .security-login-box,html.light .security-log-box,html.light .security-action-row,html.light .security-stats>div,html.light .security-stat-action{background:rgba(248,250,252,.78)}\n.tg-page-card{overflow:hidden}.tg-head{display:flex;align-items:center;gap:14px;margin-bottom:18px}.tg-head-icon{width:62px;height:62px;position:relative;perspective:500px;flex:0 0 auto}.tg-head-icon .cube{position:absolute;inset:7px;border-radius:14px;background:linear-gradient(145deg,var(--accent2),var(--accent) 55%,var(--purple));box-shadow:inset 3px 3px 8px rgba(255,255,255,.35),inset -5px -6px 10px rgba(0,0,0,.2),0 8px 22px rgba(var(--accent-rgb),.35);transform:rotateX(-10deg) rotateY(15deg);display:flex;align-items:center;justify-content:center}.tg-head-icon svg{width:31px;height:31px;color:#fff}.tg-links{display:grid;gap:10px}.tg-link{display:flex;align-items:center;gap:13px;padding:13px 15px;border:1px solid rgba(var(--accent-rgb),.32);border-radius:17px;background:linear-gradient(135deg,color-mix(in srgb, rgb(35 35 35 / .82) 88%, var(--accent)),color-mix(in srgb, rgb(18 18 18 / .78) 92%, var(--accent)));text-decoration:none!important;transition:.2s ease}.tg-link:hover{transform:translateY(-2px);border-color:rgba(var(--accent2-rgb),.8);box-shadow:0 0 22px rgba(var(--accent-rgb),.18)}.tg-logo{width:50px;height:50px;position:relative;flex:0 0 50px;perspective:450px}.tg-logo .face,.tg-logo .back{position:absolute;width:38px;height:38px;left:6px;top:6px;border-radius:10px;display:flex;align-items:center;justify-content:center;transform:rotateX(-8deg) rotateY(12deg)}.tg-logo .face{z-index:2;background:linear-gradient(145deg,#56eaff,var(--accent) 55%,var(--purple));box-shadow:inset 2px 2px 6px rgba(255,255,255,.4),inset -3px -5px 8px rgba(0,0,0,.2),0 6px 15px rgba(var(--accent-rgb),.35)}.tg-logo .back{background:linear-gradient(145deg,var(--accent),var(--accent));transform:translate(6px,5px) rotateX(-8deg) rotateY(12deg)}.tg-logo svg{width:23px;height:23px;color:#fff}.tg-logo.github .face{background:linear-gradient(145deg,#eef4ff,color-mix(in srgb, rgb(129 129 129) 82%, var(--accent)) 52%,color-mix(in srgb, rgb(33 33 33) 88%, var(--accent)))}.tg-logo.github .back{background:linear-gradient(145deg,color-mix(in srgb, rgb(81 81 81) 82%, var(--accent)),color-mix(in srgb, rgb(25 25 25) 92%, var(--accent)))}.tg-copy{min-width:0;flex:1}.tg-copy b{display:block;color:var(--t1);font-size:14px;margin-bottom:4px}.tg-copy span{display:block;color:var(--accent2);font-size:13px;direction:ltr;text-align:right;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.tg-arrow{font-size:22px;color:var(--accent2)}@media(max-width:600px){.tg-link{padding:11px 12px}.tg-copy b{font-size:13px}.tg-copy span{font-size:12px}.tg-head-icon{width:56px;height:56px}.tg-logo{width:46px;height:46px;flex-basis:46px}}\n\n\n/* ============================================================\n   ONEX MOBILE DASHBOARD COMPACT — FIT ALL DASHBOARD BLOCKS\n   Phone layout mirrors the compact dashboard composition:\n   4 stat tiles in one row, then 2-column dashboard cards.\n   ============================================================ */\n@media (max-width:640px){\n  #page-dash{width:100%;max-width:100%;min-width:0;overflow:visible}\n  #page-dash .dashboard-hero{margin:0 0 10px;gap:8px;align-items:center}\n  #page-dash .dashboard-hero .hero-actions{display:none!important}\n  #page-dash .hero-title{font-size:18px;line-height:1.25;margin-top:0}\n  #page-dash .hero-sub{font-size:7px;margin-top:3px}\n\n  #page-dash .onex-metrics{width:100%;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px;margin-bottom:9px}\n  #page-dash .onex-metric{min-width:0;min-height:82px;padding:7px 6px;border-radius:12px;display:flex;flex-direction:column;align-items:center;text-align:center}\n  #page-dash .metric-icon{width:25px;height:25px;border-radius:8px;flex:0 0 25px}\n  #page-dash .metric-icon svg{width:13px;height:13px}\n  #page-dash .onex-metric .metric-label{margin:5px 0 2px;font-size:6.5px;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:100%}\n  #page-dash .onex-metric .metric-val{font-size:13px;line-height:1.15;max-width:100%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n  #page-dash .metric-trend{left:5px;bottom:5px;font-size:5px}\n\n  #page-dash .dashboard-grid{width:100%;grid-template-columns:repeat(2,minmax(0,1fr));grid-template-areas:"traffic health" "telegram quick" "server recent";gap:8px;align-items:stretch}\n  #page-dash .dashboard-grid > div:first-child,#page-dash .dashboard-grid-right{display:contents}\n  #page-dash .dashboard-grid > div:first-child > .onex-card:nth-child(1){grid-area:traffic}\n  #page-dash .dashboard-grid > div:first-child > .onex-card:nth-child(2){grid-area:quick}\n  #page-dash .dashboard-grid > div:first-child > .onex-card:nth-child(3){grid-area:recent}\n  #page-dash .dashboard-grid-right > .onex-card:nth-child(1){grid-area:health}\n  #page-dash .dashboard-grid-right > .telegram-card{grid-area:telegram}\n  #page-dash .dashboard-grid-right > .onex-card:nth-child(3){grid-area:server}\n\n  #page-dash .onex-card,#page-dash .telegram-card{min-width:0;width:100%;border-radius:13px}\n  #page-dash .onex-card-head{padding:9px 10px;gap:4px;min-width:0}\n  #page-dash .onex-card-title{font-size:8px;gap:4px;min-width:0;white-space:nowrap}\n  #page-dash .onex-card-title svg{width:12px;height:12px;flex:0 0 auto}\n  #page-dash .onex-card-body{padding:9px 10px}\n\n  #page-dash .chart-wrap{height:135px;padding:4px 4px 7px;min-width:0}\n  #page-dash .chart-labels{padding:0 2px;font-size:5.5px}\n  #page-dash .chart-badge{top:7px;right:24%;padding:3px 5px;font-size:5px;border-radius:7px;white-space:nowrap}\n  #page-dash .range-mini{display:none}\n\n  #page-dash .health-list{gap:7px}\n  #page-dash .health-row{grid-template-columns:23px minmax(0,1fr) 24px;gap:5px}\n  #page-dash .health-icon{width:23px;height:23px;border-radius:7px;font-size:7px}\n  #page-dash .health-name{font-size:6.5px;margin-bottom:2px}\n  #page-dash .health-pct{font-size:5.5px}\n  #page-dash .health-track{height:4px}\n  #page-dash .xray-state{padding:6px 7px;border-radius:7px;font-size:6px;margin-top:2px}\n\n  #page-dash .telegram-card{padding:9px;min-height:100%;justify-content:space-between}\n  #page-dash .tg-orbit{width:68px;height:68px;margin:2px auto 6px}\n  #page-dash .tg-logo{width:39px;height:39px}\n  #page-dash .tg-logo svg{width:20px;height:20px}\n  #page-dash .tg-title{font-size:7px}\n  #page-dash .tg-handle{font-size:13px;margin-top:3px}\n  #page-dash .tg-desc{font-size:5.5px;margin-top:4px;line-height:1.5}\n  #page-dash .tg-btn{margin-top:7px;padding:7px 5px;border-radius:8px;font-size:6.5px}\n\n  #page-dash .quick-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:5px}\n  #page-dash .quick-item{gap:4px;padding:7px;border-radius:9px;min-width:0;min-height:48px}\n  #page-dash .quick-icon{width:25px;height:25px;border-radius:7px;flex:0 0 25px;font-size:15px}\n  #page-dash .quick-name{font-size:6.5px;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n  #page-dash .quick-desc{font-size:5.2px;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n\n  #page-dash .server-info{gap:5px}\n  #page-dash .info-row{padding-bottom:5px;font-size:6.5px;gap:5px}\n  #page-dash .info-row span:last-child{max-width:58%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n\n  #page-dash .recent-card{margin-top:0;min-width:0}\n  #page-dash .recent-card .btn{font-size:5.5px;padding:4px 5px;min-height:0}\n  #page-dash .recent-table{width:100%;min-width:0!important;table-layout:fixed;font-size:5.5px}\n  #page-dash .recent-table th,#page-dash .recent-table td{padding:5px 3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n  #page-dash .recent-table th{font-size:5px}\n  #page-dash .recent-table th:nth-child(5),#page-dash .recent-table td:nth-child(5){display:none}\n  #page-dash .recent-table th:nth-child(1){width:30%}\n  #page-dash .recent-table th:nth-child(2){width:21%}\n  #page-dash .recent-table th:nth-child(3){width:22%}\n  #page-dash .recent-table th:nth-child(4){width:27%}\n  #page-dash .recent-card .onex-card-body > div{overflow:hidden!important}\n\n  #page-dash .onex-footer{margin-top:7px;padding:6px 1px;font-size:5.5px;gap:8px}\n}\n@media (max-width:380px){\n  #page-dash .onex-metrics{gap:4px}\n  #page-dash .onex-metric{min-height:78px;padding:6px 4px}\n  #page-dash .metric-icon{width:23px;height:23px;flex-basis:23px}\n  #page-dash .metric-icon svg{width:12px;height:12px}\n  #page-dash .onex-metric .metric-label{font-size:6px}\n  #page-dash .onex-metric .metric-val{font-size:12px}\n  #page-dash .dashboard-grid{gap:6px}\n  #page-dash .onex-card-head{padding:8px}\n  #page-dash .onex-card-body{padding:8px}\n  #page-dash .chart-wrap{height:125px}\n  #page-dash .tg-orbit{width:60px;height:60px}\n  #page-dash .tg-logo{width:35px;height:35px}\n  #page-dash .tg-logo svg{width:18px;height:18px}\n  #page-dash .tg-handle{font-size:12px}\n  #page-dash .quick-item{padding:6px}\n  #page-dash .quick-icon{width:23px;height:23px;flex-basis:23px}\n}\n\n\n/* ============================================================\n   ONEX DARK PERFORMANCE PATCH 1.1.2\n   Dark mode was using many backdrop-filter blurs + large shadows\n   on scrolling surfaces. On mobile Chromium this can force repeated\n   GPU compositing/repaints, making lower content appear progressively.\n   Keep the dark glass appearance, but use fast opaque-ish surfaces.\n   ============================================================ */\n\n/* Only applies to the actual panel pages, not the login screen. */\nhtml:not(.light) body:has(.page)::before{{\n  animation:none !important;\n}}\nhtml:not(.light) body:has(.page) *{{\n  -webkit-backdrop-filter:none !important;\n  backdrop-filter:none !important;\n}}\n\n/* Replace expensive 25px glass blur with lightweight dark surfaces. */\nhtml:not(.light) body:has(.page) .sidebar,\nhtml:not(.light) body:has(.page) .mob-bar,\nhtml:not(.light) body:has(.page) .onex-topbar,\nhtml:not(.light) body:has(.page) .onex-control-dock,\nhtml:not(.light) body:has(.page) .onex-card,\nhtml:not(.light) body:has(.page) .onex-metric,\nhtml:not(.light) body:has(.page) .card,\nhtml:not(.light) body:has(.page) .metric,\nhtml:not(.light) body:has(.page) .support-tile,\nhtml:not(.light) body:has(.page) .quick-item,\nhtml:not(.light) body:has(.page) .table-wrap,\nhtml:not(.light) body:has(.page) .sub-box,\nhtml:not(.light) body:has(.page) .link-box,\nhtml:not(.light) body:has(.page) .modal,\nhtml:not(.light) body:has(.page) .toast{{\n  box-shadow:\n    0 7px 20px rgba(0,0,0,.18),\n    inset 0 1px rgba(255,255,255,.045) !important;\n}}\n\n/* Keep the panel responsive while preserving the dark blue glass tone. */\nhtml:not(.light) body:has(.page) .onex-topbar,\nhtml:not(.light) body:has(.page) .onex-control-dock,\nhtml:not(.light) body:has(.page) .card,\nhtml:not(.light) body:has(.page) .metric,\nhtml:not(.light) body:has(.page) .onex-card,\nhtml:not(.light) body:has(.page) .onex-metric{{\n  background:linear-gradient(145deg,rgba(12,27,50,.96),rgba(4,12,25,.98)) !important;\n}}\n\nhtml:not(.light) body:has(.page) .quick-item,\nhtml:not(.light) body:has(.page) .table-wrap,\nhtml:not(.light) body:has(.page) .sub-box,\nhtml:not(.light) body:has(.page) .link-box{{\n  background:linear-gradient(145deg,rgba(10,24,45,.94),rgba(4,12,25,.97)) !important;\n}}\n\n/* Stop decorative continuous repainting in the panel. Functional\n   spinners/loaders are intentionally left untouched. */\nhtml:not(.light) body:has(.page) .onex-control-dock::before,\nhtml:not(.light) body:has(.page) .onex-3d-control::after,\nhtml:not(.light) body:has(.page) .nav-item.on .nav-ico,\nhtml:not(.light) body:has(.page) .nav-item.on .nav-ico-dash,\nhtml:not(.light) body:has(.page) .nav-item.on .nav-ico-telegram,\nhtml:not(.light) body:has(.page) .nav-item.on .nav-ico-settings,\nhtml:not(.light) body:has(.page) .sb-foot a.danger .logout-ico,\nhtml:not(.light) body:has(.page) .sb-foot button.danger .logout-ico{{\n  animation:none !important;\n}}\n\nhtml:not(.light) body:has(.page) .protocol-art-icon{{\n  filter:none !important;\n}}\n\n/* Theme switching should not animate large shadow layers. */\nhtml:not(.light) body:has(.page) .onex-topbar,\nhtml:not(.light) body:has(.page) .onex-control-dock,\nhtml:not(.light) body:has(.page) .onex-card,\nhtml:not(.light) body:has(.page) .onex-metric,\nhtml:not(.light) body:has(.page) .card,\nhtml:not(.light) body:has(.page) .metric,\nhtml:not(.light) body:has(.page) .support-tile,\nhtml:not(.light) body:has(.page) .quick-item,\nhtml:not(.light) body:has(.page) .table-wrap{{\n  transition:background .12s ease,border-color .12s ease,color .12s ease !important;\n}}\n\n\n/* ============================================================\n   ONEX ADMIN MANAGEMENT — COMPACT / RESPONSIVE\n   ============================================================ */\n.admin-page{max-width:1120px;margin:0 auto;padding-bottom:24px}\n.admin-page .page-head{margin-bottom:14px}\n.admin-head-actions{display:flex;gap:8px;align-items:center}\n.admin-grid-top{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(290px,.85fr);gap:14px;align-items:start}\n.admin-card{padding:15px!important;overflow:hidden}\n.admin-card-head{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:10px}\n.admin-card-title{font-size:13px;font-weight:800;display:flex;align-items:center;gap:7px}\n.admin-card-sub{font-size:10px;color:var(--t3);margin-top:3px}\n.admin-list-card{padding:0!important;overflow:hidden}\n.admin-list-head{padding:14px 16px;border-bottom:1px solid var(--card-b)}\n.admin-list-controls{display:grid;grid-template-columns:minmax(0,1fr) 125px;gap:8px;margin-top:9px}\n.admin-table-wrap{overflow:auto}\n.admin-table-head,.admin-row{min-width:620px;display:grid;grid-template-columns:1.6fr .75fr .85fr 1.15fr .95fr;align-items:center}\n.admin-table-head{padding:9px 13px;background:var(--bg3);border-bottom:1px solid var(--card-b);font-size:10px;color:var(--t3);font-weight:700}\n.admin-row{padding:10px 13px;border-bottom:1px solid var(--card-b);cursor:pointer;transition:.15s;background:transparent}\n.admin-row:hover{background:var(--hover)}\n.admin-row.selected{background:rgba(var(--accent-rgb),.08);box-shadow:inset -3px 0 0 var(--accent)}\n.admin-user{display:flex;align-items:center;gap:9px;min-width:0}\n.admin-avatar{width:34px;height:34px;border-radius:11px;display:grid;place-items:center;flex:0 0 34px;background:linear-gradient(135deg,var(--accent),var(--purple));color:#fff;font-weight:800;font-size:13px}\n.admin-user-text{min-width:0}.admin-user-name{font-weight:800;font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.admin-user-label{font-size:9px;color:var(--t3);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n.admin-badge{display:inline-flex;align-items:center;gap:4px;padding:5px 8px;border-radius:999px;font-size:9px;font-weight:800;white-space:nowrap}\n.admin-badge.active{background:rgba(34,197,94,.12);color:#22c55e;border:1px solid rgba(34,197,94,.22)}\n.admin-badge.blocked{background:rgba(239,68,68,.12);color:#ef4444;border:1px solid rgba(239,68,68,.22)}\n.admin-badge.invalid{background:rgba(245,158,11,.12);color:#f59e0b;border:1px solid rgba(245,158,11,.22)}\n.admin-role{font-size:9px;font-weight:800;padding:5px 8px;border-radius:8px;background:rgba(var(--accent-rgb),.12);color:var(--accent2);display:inline-block;white-space:nowrap}\n.admin-ops{display:flex;gap:5px;align-items:center;justify-content:flex-start;flex-wrap:wrap}\n.admin-op{width:31px;height:31px;border-radius:9px;display:inline-flex;align-items:center;justify-content:center;border:1px solid var(--card-b);cursor:pointer;transition:.15s;background:var(--bg3);color:var(--t2);padding:0}\n.admin-op svg{width:14px;height:14px}\n.admin-op:hover{transform:translateY(-1px);border-color:var(--accent);color:#fff}\n.admin-op.edit{background:rgba(var(--accent-rgb),.16);border-color:rgba(var(--accent-rgb),.35);color:var(--accent2)}\n.admin-op.block{background:rgba(245,158,11,.13);border-color:rgba(245,158,11,.30);color:#fbbf24}\n.admin-op.delete{background:rgba(239,68,68,.13);border-color:rgba(239,68,68,.30);color:#f87171}\n.admin-op.unblock{background:rgba(34,197,94,.13);border-color:rgba(34,197,94,.30);color:#4ade80}\n.admin-empty{padding:28px 16px;text-align:center;color:var(--t3);font-size:11px}\n.admin-create-head{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:12px}\n.admin-create-icon{width:36px;height:36px;border-radius:11px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.13);color:var(--accent2);border:1px solid rgba(var(--accent-rgb),.22)}\n.admin-create-icon svg{width:19px;height:19px}\n.admin-create-card .field{margin-bottom:9px}\n.admin-create-card .form-row{gap:8px}\n.admin-create-card .field input,.admin-create-card .field select{padding:9px 11px}\n.admin-active-row{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 11px;border:1px solid var(--card-b);border-radius:11px;background:var(--bg3);margin-top:2px}\n.admin-active-row span{font-size:11px;font-weight:700}\n.admin-section-wide{margin-top:14px}\n.admin-perm-card{padding:15px!important}\n.admin-perm-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:12px;flex-wrap:wrap}\n.admin-select-shell{position:relative;display:flex;align-items:center;min-width:220px}.admin-select-badge{position:absolute;right:11px;z-index:2;width:23px;height:23px;border-radius:7px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.12);border:1px solid rgba(var(--accent-rgb),.18);font-size:12px;pointer-events:none}.admin-select-shell .admin-perm-select{padding-right:42px!important}\n.admin-perm-select{width:220px;min-height:44px;padding:10px 40px 10px 14px!important;border:1px solid rgba(var(--accent-rgb),.28)!important;border-radius:13px!important;background-color:var(--bg3)!important;background-image:linear-gradient(135deg,rgba(var(--accent-rgb),.10),rgba(var(--purple-rgb),.04)),url("data:image/svg+xml,%3Csvg xmlns=\'http://www.w3.org/2000/svg\' width=\'18\' height=\'18\' viewBox=\'0 0 24 24\' fill=\'none\' stroke=\'%236b7280\' stroke-width=\'2\' stroke-linecap=\'round\' stroke-linejoin=\'round\'%3E%3Cpath d=\'m6 9 6 6 6-6\'/%3E%3C/svg%3E")!important;background-repeat:no-repeat,no-repeat!important;background-position:center right 13px,center right 12px!important;background-size:auto,18px!important;color:var(--t1)!important;font-family:inherit!important;font-size:11px!important;font-weight:700!important;cursor:pointer;appearance:none;-webkit-appearance:none;box-shadow:0 4px 14px rgba(var(--accent-rgb),.08);transition:border-color .18s,box-shadow .18s,transform .18s}.admin-perm-select:hover{border-color:rgba(var(--accent-rgb),.55)!important;box-shadow:0 6px 18px rgba(var(--accent-rgb),.13)}.admin-perm-select:focus{outline:none!important;border-color:var(--accent)!important;box-shadow:0 0 0 3px rgba(var(--accent-rgb),.14),0 7px 20px rgba(var(--accent-rgb),.12)!important}.admin-perm-select option{background:var(--bg2);color:var(--t1);font-family:inherit;font-weight:600;padding:10px}\n.admin-perm-groups{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}\n.admin-perm-group{border:1px solid var(--card-b);border-radius:12px;padding:10px;background:var(--bg3)}\n.admin-perm-group h4{margin:0 0 8px;font-size:10px;font-weight:800;color:var(--t2);padding-bottom:7px;border-bottom:1px solid var(--card-b)}\n.admin-perm-item{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:7px 2px;border-bottom:1px solid var(--card-b);font-size:10px}\n.admin-perm-item:last-child{border-bottom:0}\n.admin-perm-item .switch{width:38px;height:22px;flex:0 0 auto}.admin-perm-item .slider:before{width:16px;height:16px;bottom:3px;left:3px}.admin-perm-item .switch input:checked+.slider:before{transform:translateX(16px)}\n.admin-perm-actions{display:flex;justify-content:flex-start;gap:8px;margin-top:11px;flex-wrap:wrap}\n.admin-bottom-grid{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(300px,.75fr);gap:14px}\n.admin-activity-list{max-height:250px;overflow:auto}\n.admin-activity-item{display:grid;grid-template-columns:58px minmax(0,1fr);gap:9px;padding:9px 0;border-bottom:1px solid var(--card-b);font-size:10px}\n.admin-activity-item:last-child{border-bottom:0}.admin-activity-time{color:var(--t3);font-size:9px}.admin-activity-msg{color:var(--t2);line-height:1.6}\n.admin-details-grid{display:grid;grid-template-columns:1fr 1fr;gap:7px}\n.admin-detail-box{padding:9px;border:1px solid var(--card-b);border-radius:10px;background:var(--bg3)}\n.admin-detail-box span{display:block;color:var(--t3);font-size:8px;margin-bottom:4px}.admin-detail-box b{display:block;color:var(--t1);font-size:10px;word-break:break-word}\n.admin-detail-perms{margin-top:9px;display:flex;gap:5px;flex-wrap:wrap}.admin-detail-perm{font-size:8px;padding:4px 7px;border-radius:7px;background:rgba(var(--accent-rgb),.10);border:1px solid rgba(var(--accent-rgb),.18);color:var(--t2)}\n.admin-selected-note{padding:8px 10px;border-radius:9px;background:rgba(var(--accent-rgb),.07);border:1px solid rgba(var(--accent-rgb),.15);color:var(--t3);font-size:9px;margin-bottom:9px}\n@media(max-width:640px){\n  .admin-page{max-width:100%;padding-bottom:18px}\n  .admin-grid-top,.admin-bottom-grid{grid-template-columns:1fr;gap:8px}\n  .admin-section-wide{margin-top:8px}.admin-card{padding:11px!important}.admin-list-head{padding:11px 12px}.admin-list-controls{grid-template-columns:minmax(0,1fr) 105px;gap:6px}\n  .admin-table-head,.admin-row{min-width:560px;grid-template-columns:1.65fr .78fr .82fr 1fr .95fr;padding-left:10px;padding-right:10px}\n  .admin-table-head{font-size:8px;padding-top:8px;padding-bottom:8px}.admin-row{padding-top:9px;padding-bottom:9px}\n  .admin-avatar{width:30px;height:30px;flex-basis:30px;border-radius:9px;font-size:11px}.admin-user-name{font-size:10px}.admin-user-label{font-size:8px}.admin-badge,.admin-role{font-size:8px;padding:4px 6px}.admin-op{width:34px;height:34px;border-radius:9px}.admin-op svg{width:15px;height:15px}\n  .admin-perm-groups{grid-template-columns:1fr 1fr;gap:7px}.admin-perm-group{padding:8px}.admin-perm-item{font-size:9px;padding:6px 1px}.admin-perm-head{align-items:stretch}.admin-select-shell{width:100%;min-width:0}.admin-perm-select{width:100%}\n  .admin-activity-item{grid-template-columns:50px minmax(0,1fr);font-size:9px}.admin-detail-box{padding:8px}.admin-detail-box span{font-size:7px}.admin-detail-box b{font-size:9px}\n}\n@media(max-width:430px){\n  .admin-perm-groups{grid-template-columns:1fr}.admin-list-controls{grid-template-columns:1fr 92px}.admin-table-wrap{overflow-x:auto}.admin-op{width:32px;height:32px}.admin-create-card .form-row{grid-template-columns:1fr 1fr}.admin-create-card .field input,.admin-create-card .field select{font-size:10px}\n}\n\n\n\n/* ============================================================\n   ONEX 3D BRAND MARK — APPROVED FINAL ASSET\n   ============================================================ */\n.sb-logo-icon{font-size:0!important;background:transparent!important;border:0!important;box-shadow:none!important;overflow:visible!important}\n.sb-logo-icon:before,.sb-logo-icon:after{display:none!important}\n.sb-logo-icon img{width:78px!important;height:58px!important;object-fit:contain!important;display:block!important;filter:drop-shadow(0 5px 10px rgba(var(--accent-rgb),.30))!important}\n.mob-brand-icon{position:relative!important;display:grid!important;place-items:center!important;font-size:0!important;background:transparent!important;border:0!important;box-shadow:none!important;overflow:visible!important}\n.mob-brand-icon:before,.mob-brand-icon:after{display:none!important}\n.mob-brand-icon img{width:54px!important;height:42px!important;object-fit:contain!important;display:block!important;filter:drop-shadow(0 4px 8px rgba(var(--accent-rgb),.30))!important}\n\n/* ============================================================\n   ONEX 3D LOGO — CLEAN GLASS PRESENTATION\n   Keep the approved 3D artwork, but present it inside a\n   controlled glass badge so the raster background never looks\n   like a broken rectangular image on light/dark themes.\n   ============================================================ */\n.sb-logo{padding:14px 12px!important;min-height:88px;}\n.sb-logo-icon{position:relative!important;width:92px!important;height:66px!important;border-radius:22px!important;display:grid!important;place-items:center!important;flex:0 0 auto!important;overflow:visible!important;background:transparent!important;border:0!important;box-shadow:none!important;transform-style:preserve-3d!important;isolation:isolate!important;}\n.sb-logo-icon img{width:100%!important;height:100%!important;object-fit:contain!important;object-position:center!important;display:block!important;border-radius:0!important;}\n.sidebar.collapsed .sb-logo{padding:12px 8px!important;min-height:84px;}\n.sidebar.collapsed .sb-logo-icon{width:72px!important;height:54px!important;margin:0 auto!important;border-radius:19px!important;}\n.mob-brand{display:flex!important;align-items:center!important;gap:9px!important;}\n.mob-brand-icon{position:relative!important;width:58px!important;height:44px!important;display:grid!important;place-items:center!important;flex:0 0 auto!important;border-radius:15px!important;overflow:visible!important;background:transparent!important;border:0!important;box-shadow:none!important;transform-style:preserve-3d!important;isolation:isolate!important;}\n.mob-brand-icon img{width:100%!important;height:100%!important;object-fit:contain!important;object-position:center!important;display:block!important;border-radius:0!important;}\n@media(max-width:700px){\n  .sb-logo{min-height:82px!important;padding:12px 10px!important;}\n  .sb-logo-icon{width:88px!important;height:64px!important;border-radius:21px!important;}\n  .mob-brand-icon{width:56px!important;height:43px!important;border-radius:15px!important;}\n}\n\n/* ============================================================\n   ONEX RED ACTION PALETTE — FINAL\n   Keep the navy/blue glass surfaces, but make primary/action\n   controls red so the panel has a deliberate blue + red identity.\n   This block is intentionally last to prevent older blue rules\n   from overriding the action palette.\n   ============================================================ */\n:root{\n  --action-red:var(--accent);\n  --action-red-2:var(--accent);\n  --action-red-soft:rgba(var(--accent-rgb),.12);\n  --action-red-border:rgba(var(--accent-rgb),.62);\n  --action-red-glow:rgba(var(--accent-rgb),.24);\n}\n\n/* Primary/create/save/update buttons */\nhtml:not(.light) .btn-p,\nhtml:not(.light) .btn-primary,\nhtml:not(.light) .primary,\nhtml.light .btn-p,\nhtml.light .btn-primary,\nhtml.light .primary{\n  color:#fff !important;\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;\n  border:1px solid rgba(var(--accent2-rgb),.72) !important;\n  box-shadow:0 8px 24px var(--action-red-glow),inset 0 1px rgba(255,255,255,.14) !important;\n}\nhtml:not(.light) .btn-p:hover,\nhtml:not(.light) .btn-primary:hover,\nhtml:not(.light) .primary:hover,\nhtml.light .btn-p:hover,\nhtml.light .btn-primary:hover,\nhtml.light .primary:hover{\n  filter:brightness(1.10) !important;\n  border-color:rgba(var(--accent2-rgb),.88) !important;\n  box-shadow:0 11px 30px rgba(var(--accent-rgb),.30),inset 0 1px rgba(255,255,255,.18) !important;\n}\n\n/* Mobile hamburger */\n.mob-menu-btn{\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;\n  border-color:rgba(var(--accent-rgb),.78) !important;\n  color:#fff !important;\n  box-shadow:0 8px 24px rgba(var(--accent-rgb),.24),inset 0 1px rgba(255,255,255,.14) !important;\n}\n.mob-menu-btn:hover{filter:brightness(1.10) !important;box-shadow:0 10px 28px rgba(var(--accent-rgb),.30),inset 0 1px rgba(255,255,255,.18) !important}\n\n/* Active navigation item: blue glass stays around it, red is the action accent. */\nhtml:not(.light) .nav-item.on,\nhtml.light .nav-item.on{\n  background:linear-gradient(135deg,rgba(var(--accent-rgb),.16),rgba(var(--accent-rgb),.07)) !important;\n  color:var(--accent2) !important;\n  box-shadow:inset -3px 0 0 var(--action-red),inset 0 0 24px rgba(var(--accent-rgb),.07) !important;\n}\nhtml:not(.light) .nav-item.on .nav-ico,\nhtml.light .nav-item.on .nav-ico{color:var(--accent2) !important;filter:drop-shadow(0 3px 8px rgba(var(--accent-rgb),.46)) !important}\n\n/* Advanced settings is an action bar, so it uses the same red accent. */\n.advanced-toggle{\n  background:linear-gradient(120deg,rgba(var(--accent-rgb),.58),rgba(20,8,24,.42)) !important;\n  border:1px solid rgba(var(--accent-rgb),.42) !important;\n  color:var(--accent2) !important;\n  box-shadow:inset 0 1px rgba(255,255,255,.06),0 8px 24px rgba(var(--accent-rgb),.08) !important;\n}\n.advanced-toggle:hover{background:linear-gradient(120deg,rgba(var(--accent-rgb),.68),rgba(24,8,27,.50)) !important;border-color:rgba(var(--accent-rgb),.72) !important}\n.advanced-toggle-icon{background:rgba(var(--accent-rgb),.12) !important;border-color:rgba(var(--accent-rgb),.42) !important;color:var(--accent2) !important;box-shadow:0 0 20px rgba(var(--accent-rgb),.10) !important}\n.advanced-toggle-state{color:var(--accent2) !important;background:rgba(var(--accent-rgb),.10) !important;border-color:rgba(var(--accent-rgb),.28) !important}\n\n/* Protocol picker confirmation / other explicit action buttons */\n.protocol-picker-confirm{\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;\n  box-shadow:0 8px 22px rgba(var(--accent-rgb),.22) !important;\n}\n\n/* Any button explicitly using Tailwind blue background/gradient utilities.\n   We only recolor actual buttons; blue cards and decorative elements remain blue. */\nbutton[class*="bg-blue-"],\nbutton[class*="from-blue-"],\nbutton[class*="to-blue-"],\na.btn[class*="bg-blue-"],\na.btn[class*="from-blue-"]{\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;\n  border-color:rgba(var(--accent-rgb),.72) !important;\n  color:#fff !important;\n  box-shadow:0 8px 24px rgba(var(--accent-rgb),.22),inset 0 1px rgba(255,255,255,.12) !important;\n}\n\n/* Active range tab is a primary control too. */\nhtml:not(.light) .range-tab.on,\nhtml.light .range-tab.on{\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;\n  color:#fff !important;\n  border-color:rgba(var(--accent-rgb),.70) !important;\n  box-shadow:0 7px 20px rgba(var(--accent-rgb),.18) !important;\n}\n\n/* Delete-all keeps its existing stronger red treatment. */\n.delete-all-configs-glass,\n.delete-all-confirm{background:linear-gradient(135deg,#ff315d,#d91f55) !important;color:#fff !important}\n\n/* Light mode should retain the same blue + red visual language. */\nhtml.light .mob-menu-btn{color:#fff !important}\n\n/* ============================================================\n   ONEX RED GLASS SYSTEM — FINAL GLOBAL ACTION PALETTE\n   Blue remains the structural/decorative color; red is reserved\n   for primary actions, active controls and navigation emphasis.\n   This block is intentionally last.\n   ============================================================ */\n:root{\n  --action-red:var(--accent);\n  --action-red-2:var(--accent);\n  --action-red-bright:var(--accent);\n  --action-red-soft:rgba(var(--accent-rgb),.14);\n  --action-red-border:rgba(var(--accent-rgb),.72);\n  --action-red-glow:rgba(var(--accent-rgb),.28);\n}\n\n/* All primary/action buttons */\nhtml:not(.light) .btn-p,html:not(.light) .btn-primary,html:not(.light) .primary,\nhtml.light .btn-p,html.light .btn-primary,html.light .primary{\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;\n  border:1px solid rgba(var(--accent2-rgb),.74) !important;\n  color:#fff !important;\n  box-shadow:0 9px 26px var(--action-red-glow),inset 0 1px rgba(255,255,255,.15) !important;\n}\nhtml:not(.light) .btn-p:hover,html:not(.light) .btn-primary:hover,html:not(.light) .primary:hover,\nhtml.light .btn-p:hover,html.light .btn-primary:hover,html.light .primary:hover{\n  filter:brightness(1.10) !important;\n  border-color:rgba(var(--accent2-rgb),.92) !important;\n  box-shadow:0 12px 32px rgba(var(--accent-rgb),.36),inset 0 1px rgba(255,255,255,.20) !important;\n}\n\n/* Explicit blue utility buttons become red, while blue cards stay blue. */\nbutton[class*="bg-blue-"],button[class*="from-blue-"],button[class*="to-blue-"],\na.btn[class*="bg-blue-"],a.btn[class*="from-blue-"]{\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;\n  border-color:var(--action-red-border) !important;color:#fff !important;\n  box-shadow:0 9px 26px var(--action-red-glow),inset 0 1px rgba(255,255,255,.14) !important;\n}\n\n/* Hamburger */\n.mob-menu-btn{\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;\n  border-color:rgba(var(--accent-rgb),.82) !important;color:#fff !important;\n  box-shadow:0 9px 28px rgba(var(--accent-rgb),.34),inset 0 1px rgba(255,255,255,.18) !important;\n}\n.mob-menu-btn:hover{filter:brightness(1.10) !important}\n\n/* Active navigation: readable red glass, not a low-contrast red wash. */\nhtml:not(.light) .nav-item.on,html.light .nav-item.on{\n  background:linear-gradient(135deg,rgba(var(--accent-rgb),.22),rgba(var(--accent-rgb),.09)) !important;\n  color:var(--accent2) !important;\n  border:1px solid rgba(var(--accent-rgb),.20) !important;\n  box-shadow:inset -4px 0 0 var(--action-red),0 0 26px rgba(var(--accent-rgb),.09),inset 0 0 22px rgba(var(--accent-rgb),.06) !important;\n}\nhtml:not(.light) .nav-item.on .nav-ico,html.light .nav-item.on .nav-ico{\n  color:var(--accent2) !important;filter:drop-shadow(0 3px 8px rgba(var(--accent-rgb),.52)) !important;\n}\n\n/* Advanced settings glass bar */\n.advanced-toggle{\n  background:linear-gradient(120deg,rgba(var(--accent-rgb),.72),rgba(20,8,24,.50)) !important;\n  border:1px solid rgba(var(--accent-rgb),.55) !important;color:#ffd3dd !important;\n  box-shadow:inset 0 1px rgba(255,255,255,.07),0 10px 28px rgba(var(--accent-rgb),.10) !important;\n}\n.advanced-toggle:hover{background:linear-gradient(120deg,rgba(var(--accent-rgb),.78),rgba(27,8,30,.58)) !important;border-color:rgba(var(--accent2-rgb),.82) !important}\n.advanced-toggle-icon{background:rgba(var(--accent-rgb),.16) !important;border-color:rgba(var(--accent-rgb),.52) !important;color:var(--accent2) !important}\n.advanced-toggle-state{color:var(--accent2) !important;background:rgba(var(--accent-rgb),.12) !important;border-color:rgba(var(--accent-rgb),.34) !important}\n\n/* Range tabs / protocol confirmation / delete-all */\nhtml:not(.light) .range-tab.on,html.light .range-tab.on{\n  background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;color:#fff !important;\n  border-color:rgba(var(--accent-rgb),.74) !important;box-shadow:0 8px 22px rgba(var(--accent-rgb),.22) !important;\n}\n.protocol-picker-confirm{background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;box-shadow:0 9px 24px rgba(var(--accent-rgb),.24) !important}\n.protocol-option.selected{border-color:var(--accent) !important;box-shadow:0 0 0 1px rgba(var(--accent-rgb),.22),0 0 25px rgba(var(--accent-rgb),.18) !important}\n.protocol-option.selected:after{background:linear-gradient(145deg,var(--action-red-bright),var(--action-red-2)) !important}\n.protocol-option.selected .protocol-option-radio{border-color:var(--accent2) !important}\n.delete-all-configs-glass,.delete-all-confirm{background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;color:#fff !important;border-color:rgba(255,108,137,.78) !important;box-shadow:0 12px 30px rgba(255,31,92,.24) !important}\n\n/* Logout */\n#panelLogoutBtn,.sb-foot a.danger,.sb-foot button.danger{\n  background:linear-gradient(135deg,rgba(255,49,93,.18),rgba(217,31,85,.30)) !important;\n  border:1px solid rgba(255,82,120,.62) !important;color:#ffb1c2 !important;\n  box-shadow:inset 0 1px rgba(255,255,255,.06),0 8px 24px rgba(255,31,92,.10) !important;\n}\n#panelLogoutBtn:hover,.sb-foot a.danger:hover,.sb-foot button.danger:hover{\n  background:linear-gradient(135deg,rgba(255,49,93,.34),rgba(217,31,85,.42)) !important;color:#fff !important;\n  border-color:rgba(255,126,151,.88) !important;box-shadow:0 10px 30px rgba(255,31,92,.28),inset 0 1px rgba(255,255,255,.12) !important;\n}\n#panelLogoutBtn .logout-ico,.sb-foot a.danger .logout-ico,.sb-foot button.danger .logout-ico{color:#ff6d8d !important}\n\n/* A few secondary action surfaces that previously used strong blue fills. */\n#page-dash .range-mini button.on,.range-mini button.on{background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;color:#fff !important;box-shadow:0 6px 16px rgba(var(--accent-rgb),.20) !important}\n.notify-update-btn{background:linear-gradient(135deg,var(--action-red),var(--action-red-2)) !important;color:#fff !important;border-color:rgba(var(--accent-rgb),.72) !important}\n\n/* Mobile drawer: narrower, starts below the top bar, never hides the logo/hamburger. */\n@media (max-width:640px){\n  .sidebar{\n    top:58px !important;bottom:0 !important;right:0 !important;left:auto !important;\n    width:min(70vw,290px) !important;max-width:290px !important;min-width:0 !important;\n    height:calc(100dvh - 58px) !important;\n    transform:translateX(105%) !important;z-index:1200 !important;\n    border-top-left-radius:22px !important;\n    border-bottom-left-radius:22px !important;\n    border-top:1px solid rgba(var(--accent2-rgb),.20) !important;\n  }\n  .sidebar.mobile-open{transform:translateX(0) !important}\n  .mob-bar{z-index:1250 !important}\n  .mob-menu-btn{position:relative !important;z-index:1260 !important}\n  .sidebar .sb-logo{padding:14px 12px !important}\n  .sidebar .nav-item{font-size:12px !important;padding:10px 12px !important;margin:2px 8px !important;width:calc(100% - 16px) !important;gap:9px !important}\n  .sidebar .nav-item .nav-ico{width:20px !important;height:20px !important;min-width:20px !important}\n  .sidebar .sb-foot{padding:10px !important}\n}\n\n/* ============================================================\n   ONEX TRUE CSS 3D BRAND MARK — NO RASTER IMAGE\n   Animated depth/orbits/glass lighting. Used in header + sidebar.\n   ============================================================ */\n.sb-logo-icon,.mob-brand-icon{perspective:900px!important;transform-style:preserve-3d!important;}\n.onex-mark{position:relative;width:82px;height:82px;display:block;transform-style:preserve-3d;perspective:900px;animation:onexMarkFloat 4.8s ease-in-out infinite;}\n.mob-brand-icon .onex-mark{width:50px;height:50px;}\n.onex-core{position:absolute;inset:10%;border-radius:27% 35% 28% 34%;background:linear-gradient(145deg,var(--accent) 0%,var(--accent) 45%,color-mix(in srgb, rgb(71 71 71) 82%, var(--accent)) 72%,var(--purple) 100%);border:1px solid rgba(255,255,255,.42);box-shadow:inset 3px 4px 8px rgba(255,255,255,.25),inset -7px -9px 15px color-mix(in srgb, rgb(21 21 21 / .45) 88%, var(--accent)),0 13px 24px rgba(var(--accent-rgb),.34),0 0 28px rgba(var(--accent-rgb),.24);transform:translateZ(8px) rotateX(5deg) rotateY(-7deg);animation:onexCore 5.4s ease-in-out infinite;}\n.onex-core:before{content:"";position:absolute;inset:6%;border-radius:24% 31% 23% 29%;background:linear-gradient(135deg,rgba(255,255,255,.22),transparent 37%,rgba(0,0,0,.12));border:1px solid rgba(255,255,255,.14);box-shadow:inset 0 0 16px rgba(var(--accent2-rgb),.12);}\n.onex-n{position:absolute;inset:13%;display:grid;place-items:center;font:900 55px/1 Inter,system-ui,sans-serif;letter-spacing:-.10em;color:#e9fbff;text-shadow:2px 2px 0 var(--accent),4px 4px 0 var(--accent),7px 7px 0 color-mix(in srgb, rgb(24 24 24 / .58) 88%, var(--accent)),0 0 18px rgba(205,250,255,.72);transform:translateZ(30px) rotateX(2deg) rotateY(-7deg);animation:onexN 4.6s ease-in-out infinite;}\n.mob-brand-icon .onex-n{font-size:33px;}\n.onex-ring{position:absolute;left:50%;top:50%;width:104%;height:38%;border-radius:50%;border:2px solid rgba(var(--accent-rgb),.95);box-shadow:0 0 8px rgba(var(--accent-rgb),.75),0 0 18px rgba(var(--accent-rgb),.34),inset 0 0 5px rgba(255,255,255,.22);transform-style:preserve-3d;pointer-events:none;z-index:5;}\n.ring-a{transform:translate(-50%,-50%) rotateX(67deg) rotateZ(-17deg) translateZ(20px);animation:onexRingA 4.2s linear infinite;}\n.ring-b{width:88%;height:31%;border-color:rgba(244,63,255,.92);box-shadow:0 0 8px rgba(236,72,255,.72),0 0 18px rgba(var(--purple-rgb),.30);transform:translate(-50%,-50%) rotateY(65deg) rotateZ(25deg) translateZ(12px);animation:onexRingB 6.5s linear infinite reverse;}\n.onex-glint{position:absolute;width:30%;height:10%;left:12%;top:22%;border-radius:999px;background:linear-gradient(90deg,transparent,rgba(255,255,255,.95),transparent);filter:blur(1px);opacity:.75;transform:translateZ(42px) rotate(-20deg);animation:onexGlint 3.4s ease-in-out infinite;z-index:8;}\n.onex-mark:after{content:"";position:absolute;left:14%;right:14%;bottom:2%;height:16%;border-radius:50%;background:radial-gradient(ellipse,rgba(var(--accent-rgb),.40),rgba(var(--purple-rgb),.16) 45%,transparent 75%);filter:blur(5px);transform:translateZ(-10px);animation:onexMarkShadow 4.8s ease-in-out infinite;}\n@keyframes onexMarkFloat{0%,100%{transform:translateY(0) rotateX(2deg) rotateY(-4deg) scale(1)}50%{transform:translateY(-3px) rotateX(-4deg) rotateY(6deg) scale(1.035)}}\n@keyframes onexCore{0%,100%{transform:translateZ(8px) rotateX(5deg) rotateY(-7deg)}50%{transform:translateZ(18px) rotateX(-4deg) rotateY(9deg)}}\n@keyframes onexN{0%,100%{transform:translateZ(30px) rotateX(2deg) rotateY(-7deg)}50%{transform:translateZ(40px) rotateX(-3deg) rotateY(8deg)}}\n@keyframes onexRingA{0%{transform:translate(-50%,-50%) rotateX(67deg) rotateZ(-17deg) translateZ(20px)}100%{transform:translate(-50%,-50%) rotateX(67deg) rotateZ(343deg) translateZ(20px)}}\n@keyframes onexRingB{0%{transform:translate(-50%,-50%) rotateY(65deg) rotateZ(25deg) translateZ(12px)}100%{transform:translate(-50%,-50%) rotateY(65deg) rotateZ(-335deg) translateZ(12px)}}\n@keyframes onexGlint{0%,100%{transform:translate3d(-5px,0,42px) rotate(-20deg);opacity:.25}45%{transform:translate3d(26px,13px,52px) rotate(-20deg);opacity:.9}70%{opacity:.2}}\n@keyframes onexMarkShadow{0%,100%{transform:translateZ(-10px) scale(.88);opacity:.25}50%{transform:translateZ(-10px) scale(1.12);opacity:.55}}\n@media(max-width:700px){.onex-mark{width:76px;height:76px}.mob-brand-icon .onex-mark{width:48px;height:48px}.mob-brand-icon .onex-n{font-size:31px}.onex-ring{border-width:1.5px}}\n@media(prefers-reduced-motion:reduce){.onex-mark,.onex-core,.onex-n,.onex-ring,.onex-glint,.onex-mark:after{animation:none!important}}\n\n\n  /* ================= ONEX STATISTICS — LIVE NEON GLASS ================= */\n  .stats-page-wrap{max-width:1180px;margin:0 auto;padding-bottom:34px}\n  .stats-head-row{display:flex;align-items:flex-end;justify-content:space-between;gap:16px;margin-bottom:14px}\n  .stats-head-row .page-title{font-size:30px;font-weight:950;gap:10px}.stats-head-row .page-title svg{width:34px;height:34px;color:var(--accent2)}\n  .stats-subtitle{margin-top:6px;color:var(--t3);font-size:11px}\n  .stats-refresh-btn{height:40px;padding:0 15px;border-radius:13px;border:1px solid rgba(var(--accent2-rgb),.32);background:linear-gradient(135deg,rgba(var(--accent-rgb),.75),color-mix(in srgb, rgb(17 17 17 / .9) 92%, var(--accent)));color:var(--accent2);font:800 10px Vazirmatn,sans-serif;cursor:pointer}\n  .stats-range{display:grid;grid-template-columns:repeat(4,1fr);gap:7px;padding:5px;border:1px solid rgba(var(--accent-rgb),.24);border-radius:18px;background:color-mix(in srgb, rgb(14 14 14 / .62) 92%, var(--accent));margin-bottom:16px}\n  .stats-range .range-tab{height:46px;border:1px solid transparent;border-radius:13px;background:transparent;color:color-mix(in srgb, rgb(171 171 171) 82%, var(--accent));font:800 11px Vazirmatn,sans-serif;cursor:pointer;transition:.2s}\n  .stats-range .range-tab.on{color:#fff;background:linear-gradient(110deg,var(--accent),var(--accent));border-color:rgba(var(--accent2-rgb),.65);box-shadow:0 0 24px rgba(var(--accent-rgb),.22)}\n  .stats-kpi-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-bottom:14px}\n  .stats-kpi{position:relative;min-height:126px;padding:18px;border-radius:22px;border:1px solid rgba(var(--accent2-rgb),.36);overflow:hidden;background:linear-gradient(145deg,color-mix(in srgb, rgb(28 28 28 / .86) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .94) 92%, var(--accent)));display:grid;grid-template-columns:48px 1fr;gap:13px;align-items:start;box-shadow:inset 0 1px rgba(255,255,255,.05),0 16px 35px rgba(0,0,0,.18)}\n  .stats-kpi::after{content:"";position:absolute;inset:auto -20% -55% -10%;height:85px;background:radial-gradient(ellipse,rgba(var(--accent-rgb),.16),transparent 65%);pointer-events:none}\n  .stats-kpi.pink{border-color:rgba(var(--accent-rgb),.42);background:linear-gradient(145deg,rgba(48,13,56,.78),color-mix(in srgb, rgb(12 12 12 / .94) 92%, var(--accent)))}.stats-kpi.purple{border-color:rgba(var(--purple-rgb),.42)}.stats-kpi.violet{border-color:color-mix(in srgb, rgb(123 123 123 / .42) 82%, var(--accent))}.stats-kpi.green{border-color:rgba(35,211,164,.36)}\n  .stats-kpi-icon{width:46px;height:46px;border-radius:50%;display:grid;place-items:center;font-size:25px;font-weight:900;color:var(--accent);border:1px solid rgba(var(--accent-rgb),.45);background:rgba(var(--accent-rgb),.08);text-shadow:0 0 14px currentColor}.pink .stats-kpi-icon{color:var(--accent);border-color:rgba(var(--accent-rgb),.45)}.purple .stats-kpi-icon{color:var(--purple)}.blue .stats-kpi-icon{color:var(--accent2)}.violet .stats-kpi-icon{color:var(--purple)}.green .stats-kpi-icon{color:#2ce4aa}\n  .stats-kpi small{display:block;color:color-mix(in srgb, rgb(182 182 182) 82%, var(--accent));font-size:10px;font-weight:700}.stats-kpi b{display:block;margin-top:7px;color:#f8fbff;font-size:23px;font-weight:950;direction:ltr;text-align:right}.stats-kpi em{display:block;margin-top:5px;color:#20e7b1;font-size:9px;font-style:normal;font-weight:900}.pink em{color:#28e2ad}\n  .stats-spark{position:absolute;right:18px;bottom:12px;width:42%;height:26px;opacity:.75}.stats-spark::before{content:"";position:absolute;inset:12px 0 auto;background:linear-gradient(90deg,transparent,var(--accent),transparent);height:2px;box-shadow:0 0 12px var(--accent);transform:skewY(-7deg)}.pink .stats-spark::before{background:linear-gradient(90deg,transparent,var(--accent),transparent);box-shadow:0 0 12px var(--accent)}.stats-kpi-mini-dot,.online-pulse{position:absolute;right:18px;bottom:18px;width:11px;height:11px;border-radius:50%;background:#36e7ae;box-shadow:0 0 18px #36e7ae;animation:statsPulse 1.7s ease-in-out infinite}.stats-kpi-mini-dot{background:var(--accent);box-shadow:0 0 16px var(--accent)}.online-pulse{right:22px;bottom:22px}\n  .stats-panel{position:relative;border:1px solid rgba(var(--accent-rgb),.34);border-radius:23px;background:linear-gradient(145deg,color-mix(in srgb, rgb(23 23 23 / .84) 92%, var(--accent)),color-mix(in srgb, rgb(10 10 10 / .94) 92%, var(--accent)));overflow:hidden;box-shadow:inset 0 1px rgba(255,255,255,.045),0 17px 42px rgba(0,0,0,.18);margin-bottom:14px}.stats-panel::before{content:"";position:absolute;inset:0;background:linear-gradient(110deg,transparent 25%,rgba(255,255,255,.025) 48%,transparent 65%);pointer-events:none}\n  .stats-panel-head{position:relative;z-index:1;display:flex;justify-content:space-between;align-items:center;padding:18px 20px 10px}.stats-panel-head b{display:block;color:#f8fbff;font-size:16px;font-weight:950}.stats-panel-head small{display:block;color:color-mix(in srgb, rgb(137 137 137) 82%, var(--accent));font-size:9px;margin-top:4px}.panel-icon{color:var(--accent2);font-size:22px;text-shadow:0 0 15px rgba(var(--accent-rgb),.5)}.stats-live-badge{display:flex;align-items:center;gap:6px;color:#2ee4b0;font-size:8px;font-weight:900}.stats-live-badge i{width:7px;height:7px;border-radius:50%;background:#2ee4b0;box-shadow:0 0 12px #2ee4b0}\n  .stats-legend{display:flex;align-items:center;gap:18px;padding:5px 20px 10px;color:color-mix(in srgb, rgb(159 159 159) 82%, var(--accent));font-size:9px}.stats-legend span:last-child{margin-right:auto;color:var(--accent2)}.stats-legend i{display:inline-block;width:8px;height:8px;border-radius:50%;margin-left:5px;background:var(--accent2);box-shadow:0 0 8px var(--accent2)}.stats-legend .legend-upload{background:var(--accent);box-shadow:0 0 8px var(--accent)}\n  .traffic-chart-wrap{height:310px;padding:0 10px 14px}.traffic-chart-wrap svg{width:100%;height:100%;display:block}.traffic-grid{stroke:rgba(var(--accent2-rgb),.14);stroke-width:1}.traffic-axis{fill:color-mix(in srgb, rgb(132 132 132) 82%, var(--accent));font:11px Vazirmatn,sans-serif}.traffic-line-d{fill:none;stroke:var(--accent2);stroke-width:4;stroke-linecap:round;stroke-linejoin:round;filter:drop-shadow(0 0 5px rgba(var(--accent2-rgb),.7))}.traffic-line-u{fill:none;stroke:var(--purple);stroke-width:4;stroke-linecap:round;stroke-linejoin:round;filter:drop-shadow(0 0 5px rgba(var(--purple-rgb),.65))}.traffic-area-d{fill:url(#areaD)}.traffic-area-u{fill:url(#areaU)}.traffic-point{r:3;fill:#fff;stroke:var(--accent2);stroke-width:2}\n  .stats-two-col{display:grid;grid-template-columns:1fr 1fr;gap:14px}.stats-two-col .stats-panel{margin-bottom:14px}.uptime-body{display:flex;align-items:center;gap:20px;padding:10px 22px 23px}.uptime-ring{width:116px;height:116px;border-radius:50%;display:grid;place-items:center;background:conic-gradient(#18e6ae 0 99.9%,color-mix(in srgb, rgb(78 78 78 / .24) 82%, var(--accent)) 99.9%);position:relative;flex:0 0 116px;box-shadow:0 0 22px rgba(24,230,174,.12)}.uptime-ring::after{content:"";position:absolute;inset:9px;border-radius:50%;background:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent));border:1px solid color-mix(in srgb, rgb(123 123 123 / .16) 82%, var(--accent))}.uptime-ring span{position:relative;z-index:1;color:#eafff8;font-size:18px;font-weight:950}.uptime-body small{color:color-mix(in srgb, rgb(137 137 137) 82%, var(--accent));font-size:10px}.uptime-body strong{display:block;color:#f7fbff;font-size:23px;margin-top:5px;direction:ltr}.uptime-body em{display:block;color:var(--accent2);font-size:9px;font-style:normal;margin-top:8px}.server-stat-list{padding:8px 20px 18px}.server-stat-list div{display:flex;justify-content:space-between;gap:12px;padding:10px 0;border-bottom:1px solid color-mix(in srgb, rgb(137 137 137 / .09) 82%, var(--accent))}.server-stat-list div:last-child{border-bottom:0}.server-stat-list span{color:color-mix(in srgb, rgb(144 144 144) 82%, var(--accent));font-size:10px}.server-stat-list b{color:#edf5ff;font-size:10px;direction:ltr;max-width:65%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n  .summary-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:9px;padding:10px 18px 20px}.summary-grid div{padding:12px 10px;border-radius:14px;background:color-mix(in srgb, rgb(22 22 22 / .72) 92%, var(--accent));border:1px solid rgba(var(--accent-rgb),.16)}.summary-grid span{display:block;color:color-mix(in srgb, rgb(132 132 132) 82%, var(--accent));font-size:8px}.summary-grid b{display:block;color:#eaf3ff;font-size:13px;margin-top:6px;direction:ltr}\n  @keyframes statsPulse{0%,100%{transform:scale(.85);opacity:.7}50%{transform:scale(1.15);opacity:1}}\n  @media(max-width:800px){.stats-kpi-grid{grid-template-columns:repeat(2,1fr)}.stats-two-col{grid-template-columns:1fr}.summary-grid{grid-template-columns:repeat(3,1fr)}.stats-head-row .page-title{font-size:25px}.traffic-chart-wrap{height:250px}}\n  @media(max-width:560px){.stats-page-wrap{padding:0 0 22px}.stats-head-row{align-items:center}.stats-refresh-btn{width:40px;padding:0;font-size:0}.stats-refresh-btn:first-letter{font-size:20px}.stats-subtitle{font-size:8px}.stats-range{gap:4px;border-radius:15px}.stats-range .range-tab{height:42px;font-size:9px}.stats-kpi-grid{gap:8px}.stats-kpi{min-height:112px;padding:13px;border-radius:18px;grid-template-columns:38px 1fr;gap:9px}.stats-kpi-icon{width:38px;height:38px;font-size:20px}.stats-kpi b{font-size:17px}.stats-kpi small{font-size:8px}.stats-kpi em{font-size:7px}.stats-panel{border-radius:18px}.stats-panel-head{padding:14px 14px 8px}.stats-panel-head b{font-size:13px}.stats-panel-head small{font-size:8px}.stats-legend{padding:4px 14px 8px;font-size:8px}.traffic-chart-wrap{height:205px;padding:0 4px 9px}.traffic-axis{font-size:9px}.uptime-body{padding:8px 15px 18px;gap:14px}.uptime-ring{width:92px;height:92px;flex-basis:92px}.uptime-ring::after{inset:7px}.uptime-ring span{font-size:15px}.uptime-body strong{font-size:18px}.summary-grid{grid-template-columns:repeat(2,1fr);padding:8px 12px 14px}.summary-grid div{padding:10px}.summary-grid b{font-size:12px}}\n  @media(prefers-reduced-motion:reduce){.stats-kpi-mini-dot,.online-pulse{animation:none}}\n\n  /* Config toolbar/menu mobile fixes */\n  .cfg-filter-row{grid-template-columns:repeat(4,minmax(0,1fr)) !important;gap:7px !important}\n  .cfg-filter-row .cfg-select:nth-child(n){display:block !important}\n  .cfg-card,.cfg-list-shell{overflow:visible !important}\n  @media(max-width:560px){.cfg-filter-row{display:grid !important}.cfg-card{overflow:visible !important}.cfg-list-shell{overflow:visible !important}}\n\n/* ONEX TELEGRAM CONTROL CENTER */\n.tg-dashboard-shell{display:flex;flex-direction:column;gap:12px}.tg-hero,.tg-glass{position:relative;overflow:hidden;border:1px solid rgba(var(--accent-rgb),.24);background:linear-gradient(145deg,color-mix(in srgb, rgb(25 25 25 / .82) 88%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .88) 92%, var(--accent)));box-shadow:inset 0 1px rgba(255,255,255,.05),0 14px 34px rgba(0,0,0,.20);border-radius:22px}.tg-hero{display:flex;align-items:center;justify-content:space-between;padding:18px 20px;background:radial-gradient(circle at 12% 50%,rgba(var(--accent-rgb),.18),transparent 28%),linear-gradient(135deg,color-mix(in srgb, rgb(23 23 23 / .94) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11 / .96) 92%, var(--accent)))}.tg-hero-brand{display:flex;align-items:center;gap:14px}.tg-hero-icon{width:58px;height:58px;border-radius:18px;display:grid;place-items:center;background:linear-gradient(145deg,var(--accent),var(--accent));box-shadow:0 0 30px rgba(var(--accent-rgb),.28)}.tg-hero-icon svg{width:30px}.tg-hero-brand span,.tg-card-head span{font-size:8px;letter-spacing:.16em;color:rgba(255,255,255,.38);font-weight:900}.tg-hero h1{font-size:24px;font-weight:950;margin:3px 0}.tg-hero p{font-size:10px;color:var(--t3)}.tg-hero-status{display:flex;align-items:center;gap:7px;padding:8px 12px;border-radius:999px;background:rgba(34,197,94,.08);border:1px solid rgba(34,197,94,.22);color:#42e3a5;font-size:10px;font-weight:900}.tg-hero-status i{width:7px;height:7px;border-radius:50%;background:currentColor;box-shadow:0 0 12px currentColor}.tg-stats-grid{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));gap:9px}.tg-stat{min-height:86px;display:flex;align-items:center;gap:10px;padding:11px;border-radius:18px;border:1px solid rgba(var(--accent-rgb),.22);background:linear-gradient(145deg,color-mix(in srgb, rgb(29 29 29 / .72) 88%, var(--accent)),color-mix(in srgb, rgb(12 12 12 / .82) 92%, var(--accent)));box-shadow:inset 0 1px rgba(255,255,255,.04)}.tg-stat-icon{width:38px;height:38px;flex:0 0 38px;border-radius:13px;display:grid;place-items:center;font-size:17px;background:rgba(var(--accent-rgb),.14);border:1px solid rgba(var(--accent2-rgb),.20);color:var(--accent2)}.tg-stat small{display:block;color:var(--t3);font-size:8px}.tg-stat b{display:block;font-size:18px;margin-top:3px}.tg-stat em{font-style:normal;font-size:7px;color:#36dba1}.tg-tabs{display:flex;gap:7px;padding:7px;border:1px solid rgba(var(--accent-rgb),.20);border-radius:17px;background:color-mix(in srgb, rgb(12 12 12 / .65) 92%, var(--accent));overflow:auto}.tg-tabs button,.tg-audience button{flex:1;min-width:100px;height:38px;border-radius:12px;border:1px solid rgba(var(--accent-rgb),.18);background:rgba(var(--accent-rgb),.055);color:var(--t2);font:800 9px Vazirmatn,sans-serif;cursor:pointer}.tg-tabs button.on,.tg-audience button.on{background:linear-gradient(135deg,var(--accent),color-mix(in srgb, rgb(101 101 101) 82%, var(--accent)));border-color:rgba(var(--accent2-rgb),.55);color:#fff;box-shadow:0 7px 18px rgba(var(--accent-rgb),.18)}.tg-tab-panel{display:none;gap:12px}.tg-tab-panel.on{display:flex;flex-direction:column}.tg-grid-2{display:grid;grid-template-columns:1fr 1fr;gap:12px}.tg-glass{padding:16px}.tg-card-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:13px}.tg-card-head h2{font-size:14px;font-weight:950;margin-top:3px}.tg-card-head p{font-size:9px;color:var(--t3);margin-top:4px}.tg-card-badge{padding:5px 9px;border-radius:999px;background:rgba(34,197,94,.10);border:1px solid rgba(34,197,94,.22);color:#42e3a5;font-size:8px}.tg-card-icon{width:34px;height:34px;border-radius:11px;display:grid;place-items:center;background:rgba(var(--accent-rgb),.12);color:var(--accent2);font-size:16px}.tg-field{margin-bottom:11px}.tg-field label{display:block;color:var(--t3);font-size:9px;margin-bottom:5px}.tg-field input,.tg-input-wrap input,.tg-search input,.tg-user-tools input,.tg-user-tools select{width:100%;height:43px;border-radius:13px;border:1px solid rgba(var(--accent2-rgb),.20);background:color-mix(in srgb, rgb(9 9 9 / .62) 92%, var(--accent));color:var(--t1);padding:0 12px;font:600 10px Vazirmatn,sans-serif;outline:none}.tg-input-wrap{display:flex;gap:6px}.tg-input-wrap input{flex:1;direction:ltr;text-align:left}.tg-input-wrap button{width:50px;border-radius:12px;border:1px solid rgba(var(--accent2-rgb),.22);background:rgba(var(--accent-rgb),.10);color:var(--accent2);font:800 9px Vazirmatn,sans-serif}.tg-toggle-row,.tg-notify-list label{display:flex;align-items:center;justify-content:space-between;gap:12px;padding:10px 0;border-top:1px solid rgba(var(--accent2-rgb),.08)}.tg-toggle-row b,.tg-notify-list b{display:block;font-size:10px}.tg-toggle-row small,.tg-notify-list small{display:block;color:var(--t3);font-size:8px;margin-top:2px}.tg-actions,.tg-broadcast-foot,.tg-send-preview{display:flex;gap:8px;margin-top:12px}.tg-btn{height:42px;padding:0 15px;border-radius:12px;border:1px solid rgba(var(--accent2-rgb),.22);font:900 9px Vazirmatn,sans-serif;cursor:pointer}.tg-btn.primary{flex:1;color:#fff;background:linear-gradient(135deg,var(--accent),var(--accent));border-color:rgba(var(--accent2-rgb),.55);box-shadow:0 8px 24px rgba(var(--accent-rgb),.16)}.tg-btn.secondary{color:var(--accent2);background:rgba(var(--accent-rgb),.08)}.tg-btn.full{width:100%;margin-top:14px}.tg-link-btn{border:0;background:none;color:var(--accent2);font:800 8px Vazirmatn,sans-serif;cursor:pointer}.tg-search{display:flex;align-items:center;gap:8px;height:40px;padding:0 10px;border:1px solid rgba(var(--accent2-rgb),.20);border-radius:12px;background:color-mix(in srgb, rgb(9 9 9 / .52) 92%, var(--accent));margin-bottom:8px}.tg-search span{font-size:19px;color:var(--accent2)}.tg-search input{height:34px;border:0;background:transparent;padding:0}.tg-mini-users{display:flex;flex-direction:column;gap:6px}.tg-mini-user,.tg-user-row{display:flex;align-items:center;gap:9px;padding:9px;border-radius:13px;border:1px solid rgba(var(--accent2-rgb),.10);background:color-mix(in srgb, rgb(9 9 9 / .30) 92%, var(--accent))}.tg-avatar{width:32px;height:32px;flex:0 0 32px;border-radius:11px;display:grid;place-items:center;background:linear-gradient(145deg,var(--accent),var(--purple));font-weight:900;font-size:11px}.tg-user-copy{min-width:0;flex:1}.tg-user-copy b{display:block;font-size:9px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.tg-user-copy small{display:block;color:var(--t3);font-size:7px;margin-top:2px}.tg-user-state{font-size:7px;padding:4px 7px;border-radius:8px;color:#4ade80;background:rgba(34,197,94,.08)}.tg-user-state.bad{color:#fb7185;background:rgba(244,63,94,.08)}.tg-audience{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px}.tg-audience button{flex:0 0 auto;min-width:0;height:32px;padding:0 10px;font-size:8px}.tg-glass textarea{width:100%;min-height:112px;resize:vertical;border-radius:14px;border:1px solid rgba(var(--accent2-rgb),.20);background:color-mix(in srgb, rgb(9 9 9 / .55) 92%, var(--accent));color:var(--t1);padding:12px;font:500 10px/1.9 Vazirmatn,sans-serif;outline:none}.tg-broadcast-foot{align-items:center;justify-content:space-between}.tg-broadcast-foot span,.tg-send-preview span{font-size:8px;color:var(--t3)}.tg-broadcast-foot b,.tg-send-preview b{color:#fff;font-size:12px}.tg-activity{display:flex;flex-direction:column;gap:7px;max-height:240px;overflow:auto}.tg-activity-row{display:flex;gap:9px;align-items:flex-start;padding:8px;border-bottom:1px solid rgba(var(--accent2-rgb),.07)}.tg-activity-dot{width:7px;height:7px;border-radius:50%;margin-top:5px;background:#42e3a5;box-shadow:0 0 10px rgba(66,227,165,.35)}.tg-activity-row b{display:block;font-size:8px}.tg-activity-row small{display:block;color:var(--t3);font-size:7px;margin-top:2px}.tg-users-list{display:flex;flex-direction:column;gap:7px}.tg-user-tools{display:flex;gap:7px;min-width:280px}.tg-user-tools input{height:36px}.tg-user-tools select{height:36px;width:110px}.tg-user-row{min-height:58px}.tg-user-row .tg-avatar{width:38px;height:38px;flex-basis:38px}.tg-user-actions{display:flex;gap:5px}.tg-user-actions button{height:30px;padding:0 9px;border-radius:9px;border:1px solid rgba(var(--accent2-rgb),.18);background:rgba(var(--accent-rgb),.07);color:var(--accent2);font:800 8px Vazirmatn,sans-serif}.tg-user-actions button.danger{color:#ff7190;border-color:rgba(255,71,112,.25);background:rgba(255,31,92,.07)}.tg-config-owners{display:flex;flex-direction:column;gap:8px}.tg-config-owner{display:flex;align-items:center;justify-content:space-between;padding:10px;border-radius:12px;border:1px solid rgba(var(--accent2-rgb),.10);background:color-mix(in srgb, rgb(9 9 9 / .3) 92%, var(--accent))}.tg-config-owner b{font-size:9px}.tg-config-owner span{font-size:8px;color:var(--t3)}.tg-notify-list{display:flex;flex-direction:column}.tg-security-row{display:flex;justify-content:space-between;padding:12px 0;border-bottom:1px solid rgba(var(--accent2-rgb),.08);font-size:9px;color:var(--t3)}.tg-security-row b{color:var(--t1)}.tg-help-list{padding-right:18px;color:var(--t2);font-size:10px;line-height:2.2}.tg-empty{padding:24px;text-align:center;color:var(--t3);font-size:9px}.tg-result{margin-top:10px;padding:10px;border-radius:10px;font-size:9px;display:none}.tg-result.show{display:block;background:rgba(34,197,94,.08);border:1px solid rgba(34,197,94,.18);color:#4ade80}html.light .tg-hero,html.light .tg-glass,html.light .tg-stat{background:linear-gradient(145deg,rgba(255,255,255,.92),rgba(239,246,255,.82));border-color:rgba(var(--accent-rgb),.14);box-shadow:0 10px 28px rgba(var(--accent-rgb),.07),inset 0 1px rgba(255,255,255,.95)}html.light .tg-tabs{background:#fff;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))}html.light .tg-field input,html.light .tg-input-wrap input,html.light .tg-search,html.light .tg-search input,html.light .tg-user-tools input,html.light .tg-user-tools select,html.light .tg-glass textarea{background:#f8fafc;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent));border-color:rgba(var(--accent-rgb),.14)}html.light .tg-hero h1,html.light .tg-card-head h2{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}@media(max-width:900px){.tg-stats-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.tg-grid-2{grid-template-columns:1fr}.tg-user-tools{min-width:0}}.tg-build-badge{position:absolute;left:14px;bottom:10px;padding:4px 8px;border-radius:999px;font:800 7px/1 Vazirmatn,sans-serif;letter-spacing:.08em;color:var(--accent2);background:rgba(var(--accent-rgb),.08);border:1px solid rgba(var(--accent2-rgb),.18)}\n@media(max-width:560px){.tg-hero{padding:14px;align-items:flex-start;padding-bottom:30px}.tg-hero-icon{width:48px;height:48px}.tg-hero h1{font-size:19px}.tg-hero p{font-size:8px}.tg-hero-status{font-size:8px}.tg-stats-grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:7px}.tg-stat{min-height:72px;padding:8px}.tg-stat-icon{width:32px;height:32px;flex-basis:32px}.tg-stat b{font-size:15px}.tg-stat small{font-size:7px}.tg-tabs{overflow:auto}.tg-tabs button{min-width:90px;height:36px;font-size:8px}.tg-glass{padding:12px;border-radius:18px}.tg-card-head h2{font-size:12px}.tg-user-tools{width:100%}.tg-user-tools select{width:90px}.tg-user-row{align-items:flex-start;flex-wrap:wrap}.tg-user-actions{margin-right:auto}.tg-user-actions button{height:28px}.tg-broadcast-foot{align-items:stretch;flex-direction:column}.tg-broadcast-foot .tg-btn{width:100%}}\nhtml:not(.light) *{animation:none!important}html:not(.light) [style*="backdrop-filter"],html:not(.light) [style*="filter:blur"]{backdrop-filter:none!important;-webkit-backdrop-filter:none!important;filter:none!important}\n/* Dark-mode performance: keep the visual language, remove costly compositor effects. */\nhtml:not(.light) body{transition:none!important}html:not(.light) .sidebar{backdrop-filter:none!important;-webkit-backdrop-filter:none!important}html:not(.light) .modal-bg{backdrop-filter:none!important;-webkit-backdrop-filter:none!important}html:not(.light) .onex-card,html:not(.light) .onex-metric,html:not(.light) .card,html:not(.light) .metric,html:not(.light) .support-tile,html:not(.light) .quick-item,html:not(.light) .table-wrap,html:not(.light) .sub-box,html:not(.light) .link-box,html:not(.light) .tg-hero,html:not(.light) .tg-glass,html:not(.light) .tg-stat{backdrop-filter:none!important;-webkit-backdrop-filter:none!important;transform:none!important}html:not(.light) .hero:before,html:not(.light) .grid-floor,html:not(.light) .telegram-card:before,html:not(.light) .telegram-icon,html:not(.light) .tg-orbit,html:not(.light) .tg-logo,html:not(.light) .tg-btn:before{animation:none!important}html:not(.light) .hero:before,html:not(.light) .sub-hero-glow,html:not(.light) .telegram-sub-card:after{filter:none!important}html:not(.light) .card,html:not(.light) .onex-card,html:not(.light) .onex-metric,html:not(.light) .metric,html:not(.light) .support-tile,html:not(.light) .quick-item{box-shadow:0 10px 28px rgba(0,0,0,.24),inset 0 1px rgba(255,255,255,.035)!important}html:not(.light) .onex-topbar,html:not(.light) .onex-control-dock{backdrop-filter:none!important;-webkit-backdrop-filter:none!important}\n\n/* ============================================================\n   ONEX THEME SYSTEM — user-selectable panel palettes\n   ============================================================ */\n.nav-item{position:relative}\n.nav-new-badge{margin-right:auto;padding:3px 6px;border-radius:999px;background:linear-gradient(135deg,var(--accent),var(--purple));color:#fff;font-size:7px;font-weight:900;line-height:1;box-shadow:0 0 12px color-mix(in srgb,var(--accent) 35%,transparent)}\n.sidebar.collapsed .nav-new-badge{display:none!important}\n.sb-logo{flex-direction:column!important;gap:2px!important}\n.sb-logo-caption{display:flex;align-items:baseline;justify-content:center;gap:3px;line-height:1;direction:ltr;white-space:nowrap;perspective:500px}\n.sb-logo-caption span,.sb-logo-caption b{font-family:Inter,system-ui,sans-serif;font-style:italic;letter-spacing:-.055em;transform:skewX(-7deg) translateZ(12px);text-shadow:0 2px 0 color-mix(in srgb, rgb(23 23 23 / .9) 88%, var(--accent)),0 4px 0 color-mix(in srgb, rgb(13 13 13 / .55) 92%, var(--accent)),0 0 13px color-mix(in srgb,var(--accent) 55%,transparent)}\n.sb-logo-caption span{font-size:17px;font-weight:950;color:#e9fbff;background:linear-gradient(180deg,#fff 0%,var(--accent2) 45%,var(--accent2) 100%);-webkit-background-clip:text;background-clip:text;color:transparent}\n.sb-logo-caption b{font-size:11px;font-weight:950;color:#fff;background:linear-gradient(180deg,#fff 0%,var(--purple) 48%,var(--purple) 100%);-webkit-background-clip:text;background-clip:text;color:transparent}\n.sidebar.collapsed .sb-logo-caption{display:none!important}\n\n.onex-theme-layout{display:grid;grid-template-columns:minmax(0,1.12fr) minmax(330px,.88fr);gap:16px;align-items:start}\n.onex-theme-preview,.onex-theme-panel{padding:18px!important;border-radius:22px!important;background:linear-gradient(145deg,color-mix(in srgb, rgb(21 21 21 / .92) 92%, var(--accent)),color-mix(in srgb, rgb(9 9 9 / .94) 92%, var(--accent)));border:1px solid color-mix(in srgb,var(--accent) 25%,var(--card-b))!important;box-shadow:0 16px 44px rgba(0,0,0,.25),inset 0 1px rgba(255,255,255,.045)!important}\n.theme-preview-head,.theme-panel-head,.theme-custom-head{display:flex;align-items:center;justify-content:space-between;gap:10px}\n.theme-preview-head b,.theme-panel-head h2,.theme-custom-head h3{display:block;margin-top:4px;font-size:16px;font-weight:900;color:var(--t1)}\n.theme-kicker{display:block;color:var(--accent2);font-size:8px;font-weight:900;letter-spacing:.14em;direction:ltr}\n.theme-live-dot{display:inline-flex;align-items:center;gap:5px;color:#45e8a9;font-size:8px;font-weight:900;direction:ltr}.theme-live-dot i{width:6px;height:6px;border-radius:50%;background:#22c55e;box-shadow:0 0 10px #22c55e}\n.theme-preview-window{display:grid;grid-template-columns:74px 1fr;min-height:300px;margin-top:15px;border-radius:18px;overflow:hidden;border:1px solid color-mix(in srgb,var(--accent) 20%,transparent);background:var(--bg);box-shadow:inset 0 0 50px color-mix(in srgb,var(--accent) 6%,transparent)}\n.theme-mini-sidebar{padding:13px 10px;display:flex;flex-direction:column;align-items:center;gap:13px;background:var(--bg2);border-left:1px solid var(--card-b)}.theme-mini-sidebar span{width:34px;height:7px;border-radius:99px;background:var(--card-b)}.theme-mini-sidebar span.active{background:linear-gradient(90deg,var(--accent),var(--purple));box-shadow:0 0 12px color-mix(in srgb,var(--accent) 35%,transparent)}.theme-mini-logo{width:38px;height:38px;border-radius:12px;display:grid;place-items:center;font:900 22px Inter,sans-serif;color:#fff;background:linear-gradient(145deg,var(--accent),var(--purple));box-shadow:0 0 22px color-mix(in srgb,var(--accent) 28%,transparent)}\n.theme-mini-main{padding:14px;min-width:0}.theme-mini-top{display:flex;align-items:center;gap:5px;height:25px;margin-bottom:14px}.theme-mini-top i{width:7px;height:7px;border-radius:50%;background:var(--accent)}.theme-mini-top i:nth-child(2){background:var(--purple)}.theme-mini-top b{margin-right:auto;font-size:8px;color:var(--t2);direction:ltr}.theme-mini-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:7px}.theme-mini-cards div{height:72px;border-radius:11px;border:1px solid color-mix(in srgb,var(--accent) 20%,var(--card-b));background:linear-gradient(145deg,color-mix(in srgb,var(--accent) 12%,var(--card)),var(--card));position:relative;overflow:hidden}.theme-mini-cards div:after{content:\'\';position:absolute;right:-18px;bottom:-20px;width:55px;height:55px;border-radius:50%;background:color-mix(in srgb,var(--accent) 20%,transparent);filter:blur(12px)}.theme-mini-content{display:grid;grid-template-columns:1.35fr 1fr;gap:8px;margin-top:8px}.theme-mini-chart,.theme-mini-list{min-height:105px;border:1px solid var(--card-b);border-radius:11px;background:var(--card)}.theme-mini-chart{position:relative;overflow:hidden}.theme-mini-chart:after{content:\'\';position:absolute;left:10%;right:8%;bottom:18px;height:45px;border-bottom:2px solid var(--accent);border-radius:50%;transform:skewY(-7deg);box-shadow:0 0 12px color-mix(in srgb,var(--accent) 55%,transparent)}.theme-mini-list{padding:10px;display:grid;gap:8px;align-content:start}.theme-mini-list span{height:8px;border-radius:99px;background:var(--card-b)}.theme-mini-list span:first-child{width:80%;background:color-mix(in srgb,var(--accent) 25%,var(--card-b))}.theme-mini-list span:nth-child(2){width:62%}.theme-mini-list span:nth-child(3){width:72%;background:color-mix(in srgb,var(--purple) 20%,var(--card-b))}.theme-current-row{display:flex;align-items:center;gap:8px;margin-top:12px;padding:10px 12px;border:1px solid var(--card-b);border-radius:13px;background:rgba(255,255,255,.025);font-size:9px;color:var(--t3)}.theme-current-row strong{margin-right:auto;color:var(--t1);font-size:10px}.theme-current-swatch{width:27px;height:27px;border-radius:9px;background:var(--accent);border:1px solid rgba(255,255,255,.25);box-shadow:0 0 16px color-mix(in srgb,var(--accent) 35%,transparent)}\n.theme-reset-btn{height:31px;padding:0 10px;border:1px solid var(--card-b);border-radius:9px;background:transparent;color:var(--t2);font:700 9px Vazirmatn,sans-serif;cursor:pointer}.theme-reset-btn:hover{border-color:var(--accent);color:var(--accent2)}\n.theme-preset-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:13px}.theme-preset{position:relative;min-height:75px;padding:9px;border:1px solid var(--card-b);border-radius:14px;background:rgba(255,255,255,.02);color:var(--t2);cursor:pointer;text-align:right;font-family:inherit;transition:.18s;overflow:hidden}.theme-preset:hover{transform:translateY(-2px);border-color:color-mix(in srgb,var(--theme-preview) 60%,var(--card-b));}.theme-preset.on{border-color:var(--theme-preview);box-shadow:0 0 18px color-mix(in srgb,var(--theme-preview) 18%,transparent),inset 0 0 18px color-mix(in srgb,var(--theme-preview) 5%,transparent)}.theme-preset .theme-preset-dot{width:22px;height:22px;border-radius:50%;display:block;background:linear-gradient(145deg,var(--theme-preview),var(--theme-preview2));box-shadow:0 0 14px color-mix(in srgb,var(--theme-preview) 45%,transparent);margin-bottom:7px}.theme-preset b{display:block;font-size:9px;color:var(--t1)}.theme-preset small{display:block;font-size:7px;color:var(--t3);margin-top:2px}.theme-preset-check{position:absolute;top:8px;left:8px;width:17px;height:17px;border-radius:50%;display:grid;place-items:center;background:var(--theme-preview);color:#fff;font-size:9px;opacity:0;transform:scale(.7);transition:.15s}.theme-preset.on .theme-preset-check{opacity:1;transform:scale(1)}\n.theme-custom-box,.theme-mode-box{margin-top:13px;padding:13px;border:1px solid var(--card-b);border-radius:15px;background:rgba(255,255,255,.018)}.theme-custom-icon{width:31px;height:31px;border-radius:10px;display:grid;place-items:center;background:linear-gradient(135deg,var(--accent),var(--purple));color:#fff;box-shadow:0 0 18px color-mix(in srgb,var(--accent) 24%,transparent)}.theme-color-grid{display:grid;grid-template-columns:1fr 1fr;gap:7px;margin-top:11px}.theme-color-grid label{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:8px 9px;border:1px solid var(--card-b);border-radius:10px;background:rgba(255,255,255,.02);font-size:8px;color:var(--t3)}.theme-color-grid input{width:32px;height:25px;padding:0;border:0;background:transparent;border-radius:7px;cursor:pointer}.theme-apply-custom{width:100%;height:39px;margin-top:9px;border:1px solid color-mix(in srgb,var(--accent) 60%,transparent);border-radius:11px;background:linear-gradient(135deg,var(--accent),var(--purple));color:#fff;font:900 9px Vazirmatn,sans-serif;cursor:pointer;box-shadow:0 9px 22px color-mix(in srgb,var(--accent) 20%,transparent)}.theme-apply-custom svg{width:14px;height:14px;vertical-align:middle;margin-right:5px}.theme-mode-box{display:flex;align-items:center;justify-content:space-between;gap:10px}.theme-mode-box b{display:block;font-size:10px;color:var(--t1)}.theme-mode-box small{display:block;margin-top:3px;color:var(--t3);font-size:7px}.theme-mode-buttons{display:flex;gap:5px}.theme-mode-buttons button{height:34px;padding:0 9px;border-radius:9px;border:1px solid var(--card-b);background:transparent;color:var(--t3);font:700 8px Vazirmatn,sans-serif;cursor:pointer}.theme-mode-buttons button.on{background:color-mix(in srgb,var(--accent) 13%,transparent);border-color:color-mix(in srgb,var(--accent) 55%,var(--card-b));color:var(--accent2)}.theme-save-note{display:flex;gap:7px;align-items:center;margin-top:11px;color:var(--t3);font-size:8px;line-height:1.8}.theme-save-note>span:first-child{width:20px;height:20px;display:grid;place-items:center;border-radius:7px;color:#fff;background:linear-gradient(135deg,var(--accent),var(--purple));font-size:10px;flex:0 0 auto}\n\n/* Theme variables also recolor the existing dashboard surfaces without rewriting them. */\nhtml.onex-themed{--action-red:var(--accent);--action-red-2:var(--purple);--action-red-soft:color-mix(in srgb,var(--accent) 12%,transparent)}\nhtml.onex-themed body::before{background:radial-gradient(ellipse 80% 50% at 100% 0%,color-mix(in srgb,var(--accent) 17%,transparent),transparent 50%),radial-gradient(ellipse 60% 40% at 0% 100%,color-mix(in srgb,var(--purple) 12%,transparent),transparent 45%)}\nhtml.onex-themed .nav-item.on{background:linear-gradient(135deg,color-mix(in srgb,var(--accent) 18%,transparent),color-mix(in srgb,var(--purple) 16%,transparent))!important;color:var(--accent2)!important;box-shadow:inset -3px 0 0 var(--accent),0 0 18px color-mix(in srgb,var(--accent) 8%,transparent)!important}\nhtml.onex-themed .nav-item:hover{color:var(--accent2)!important;background:color-mix(in srgb,var(--accent) 8%,transparent)!important}\nhtml.onex-themed .btn-p,.onex-themed .btn-p,.onex-themed .range-mini button.on{background:linear-gradient(135deg,var(--accent),var(--purple))!important;box-shadow:0 8px 22px color-mix(in srgb,var(--accent) 18%,transparent)!important}\nhtml.onex-themed .page-title svg,html.onex-themed .top-server .top-dot,html.onex-themed .hero-title span{color:var(--accent2)!important}\nhtml.onex-themed .card,html.onex-themed .metric,html.onex-themed .onex-card,html.onex-themed .onex-metric{border-color:color-mix(in srgb,var(--accent) 16%,var(--card-b))}\nhtml.onex-themed .metric-icon,html.onex-themed .quick-icon{color:var(--accent2)!important;background:color-mix(in srgb,var(--accent) 12%,transparent)!important;border-color:color-mix(in srgb,var(--accent) 24%,var(--card-b))!important}\nhtml.onex-themed .sb-logo-caption span{filter:drop-shadow(0 0 6px color-mix(in srgb,var(--accent) 40%,transparent))}.onex-themed .theme-preset.on{background:color-mix(in srgb,var(--theme-preview) 4%,transparent)}\nhtml.onex-themed .input:focus,html.onex-themed input:focus,html.onex-themed select:focus,html.onex-themed textarea:focus{border-color:color-mix(in srgb,var(--accent) 65%,transparent)!important;box-shadow:0 0 0 3px color-mix(in srgb,var(--accent) 10%,transparent)!important}\nhtml.light.onex-themed .onex-theme-preview,html.light.onex-themed .onex-theme-panel{background:linear-gradient(145deg,rgba(255,255,255,.96),rgba(241,246,255,.92))}\n@media(max-width:900px){.onex-theme-layout{grid-template-columns:1fr}.onex-theme-preview{order:1}.onex-theme-panel{order:2}}\n@media(max-width:560px){.onex-theme-layout{gap:10px}.onex-theme-preview,.onex-theme-panel{padding:13px!important;border-radius:18px!important}.theme-preset-grid{grid-template-columns:repeat(2,1fr)}.theme-preview-window{grid-template-columns:58px 1fr;min-height:235px}.theme-mini-sidebar{padding:10px 7px}.theme-mini-main{padding:10px}.theme-mini-cards div{height:52px}.theme-mini-content{grid-template-columns:1fr}.theme-mini-list{display:none}.theme-mode-box{align-items:stretch;flex-direction:column}.theme-mode-buttons{width:100%}.theme-mode-buttons button{flex:1}.sb-logo-caption span{font-size:14px}.sb-logo-caption b{font-size:9px}}\n\n/* ============================================================\n   ONEX THEME GLOBAL SURFACE NORMALIZER\n   Every major panel/control consumes the active theme tokens.\n   Semantic danger/success states remain distinct; primary UI does not.\n   ============================================================ */\nhtml.onex-themed{\n  --onex-surface:color-mix(in srgb,var(--card) 94%,var(--bg));\n  --onex-surface-2:color-mix(in srgb,var(--card) 82%,var(--bg));\n  --onex-surface-3:color-mix(in srgb,var(--bg3) 86%,var(--card));\n  --onex-border:color-mix(in srgb,var(--accent) 20%,var(--card-b));\n  --onex-border-strong:color-mix(in srgb,var(--accent) 42%,var(--card-b));\n  --onex-accent-soft:color-mix(in srgb,var(--accent) 12%,transparent);\n  --onex-accent-softer:color-mix(in srgb,var(--accent) 7%,transparent);\n  --onex-shadow:0 12px 32px color-mix(in srgb,var(--accent) 7%,transparent);\n}\n\n/* Structural surfaces across secondary pages */\nhtml.onex-themed .group-hero,\nhtml.onex-themed .group-list-pane,\nhtml.onex-themed .group-detail-pane,\nhtml.onex-themed .group-info-card,\nhtml.onex-themed .group-link-card,\nhtml.onex-themed .group-proto-card,\nhtml.onex-themed .group-manage-card,\nhtml.onex-themed .group-configs-card,\nhtml.onex-themed .group-card,\nhtml.onex-themed .group-stat,\nhtml.onex-themed .group-filters,\nhtml.onex-themed .group-search,\nhtml.onex-themed .cfg-page-hero,\nhtml.onex-themed .cfg-stat-card,\nhtml.onex-themed .cfg-list-shell,\nhtml.onex-themed .cfg-card,\nhtml.onex-themed .cfg-side,\nhtml.onex-themed .cfg-menu,\nhtml.onex-themed .cfg-tools,\nhtml.onex-themed .admin-card,\nhtml.onex-themed .admin-create-card,\nhtml.onex-themed .admin-list-card,\nhtml.onex-themed .admin-perm-card,\nhtml.onex-themed .admin-activity-list,\nhtml.onex-themed .security-health,\nhtml.onex-themed .security-login-box,\nhtml.onex-themed .security-log-box,\nhtml.onex-themed .security-section,\nhtml.onex-themed .logs-list-card,\nhtml.onex-themed .sub-glass,\nhtml.onex-themed .sub-hero,\nhtml.onex-themed .telegram-sub-card,\nhtml.onex-themed .stats-kpi,\nhtml.onex-themed .stats-panel,\nhtml.onex-themed .stats-range,\nhtml.onex-themed .tg-hero,\nhtml.onex-themed .tg-glass,\nhtml.onex-themed .tg-stat,\nhtml.onex-themed .tg-tabs,\nhtml.onex-themed .backup,\nhtml.onex-themed .advanced-config-card,\nhtml.onex-themed .advanced-subcard{\n  background:var(--onex-surface) !important;\n  border-color:var(--onex-border) !important;\n  color:var(--t1) !important;\n}\n\nhtml.onex-themed .group-info-item,\nhtml.onex-themed .group-link-url,\nhtml.onex-themed .group-config-row,\nhtml.onex-themed .group-proto-row,\nhtml.onex-themed .group-search,\nhtml.onex-themed .cfg-search-box,\nhtml.onex-themed .cfg-select,\nhtml.onex-themed .cfg-filter-btn,\nhtml.onex-themed .admin-select-shell,\nhtml.onex-themed .admin-perm-select,\nhtml.onex-themed .logs-filter,\nhtml.onex-themed .tg-field input,\nhtml.onex-themed .tg-input-wrap input,\nhtml.onex-themed .tg-search,\nhtml.onex-themed .tg-user-tools input,\nhtml.onex-themed .tg-user-tools select,\nhtml.onex-themed .tg-glass textarea,\nhtml.onex-themed .sub-url-box,\nhtml.onex-themed .sub-action,\nhtml.onex-themed .range-mini,\nhtml.onex-themed .mini-action{\n  background:var(--onex-surface-2) !important;\n  border-color:var(--onex-border) !important;\n  color:var(--t2) !important;\n}\n\n/* Primary actions: no hard-coded pink/blue survives a theme selection. */\nhtml.onex-themed .btn-p,\nhtml.onex-themed .btn-primary,\nhtml.onex-themed .cfg-primary-btn,\nhtml.onex-themed .cfg-hero-actions .btn-p,\nhtml.onex-themed .group-create-btn,\nhtml.onex-themed .group-config-save,\nhtml.onex-themed .sub-action.primary,\nhtml.onex-themed .tg-btn.primary,\nhtml.onex-themed .tg-join,\nhtml.onex-themed .stats-range .range-tab.on,\nhtml.onex-themed .range-tab.on,\nhtml.onex-themed .nav-item.on,\nhtml.onex-themed .onex-3d-control.active{\n  background:linear-gradient(135deg,var(--accent),var(--purple)) !important;\n  border-color:color-mix(in srgb,var(--accent) 55%,transparent) !important;\n  color:#fff !important;\n  box-shadow:0 8px 24px color-mix(in srgb,var(--accent) 18%,transparent) !important;\n}\nhtml.onex-themed .group-create-btn:hover,\nhtml.onex-themed .btn-p:hover,\nhtml.onex-themed .cfg-primary-btn:hover,\nhtml.onex-themed .tg-btn.primary:hover,\nhtml.onex-themed .tg-join:hover{filter:brightness(1.06)}\n\n/* Secondary controls follow the active accent. */\nhtml.onex-themed .group-link-actions button,\nhtml.onex-themed .group-manage-actions button:first-child,\nhtml.onex-themed .group-copy-btn,\nhtml.onex-themed .cfg-filter-btn.active,\nhtml.onex-themed .cfg-filter-btn.on,\nhtml.onex-themed .cfg-menu-btn.active,\nhtml.onex-themed .logs-refresh-btn,\nhtml.onex-themed .logs-advanced-btn,\nhtml.onex-themed .logs-reset-filter,\nhtml.onex-themed .tg-tabs button.on,\nhtml.onex-themed .tg-audience button.on,\nhtml.onex-themed .tg-input-wrap button,\nhtml.onex-themed .tg-user-actions button:not(.danger),\nhtml.onex-themed .theme-mode-buttons button.on{\n  background:var(--onex-accent-soft) !important;\n  border-color:var(--onex-border-strong) !important;\n  color:var(--accent2) !important;\n}\n\n/* Keep destructive actions semantically red, but remove accidental pink primary styling. */\nhtml.onex-themed .btn-d,\nhtml.onex-themed .group-manage-actions button:nth-child(2),\nhtml.onex-themed .group-manage-actions button:nth-child(3),\nhtml.onex-themed .tg-user-actions button.danger,\nhtml.onex-themed .delete-all-configs-glass{\n  color:#ef4444 !important;\n  border-color:rgba(239,68,68,.24) !important;\n  background:rgba(239,68,68,.08) !important;\n}\n\n/* Theme the common icon/control accents. */\nhtml.onex-themed .page-title svg,\nhtml.onex-themed .card-title svg,\nhtml.onex-themed .panel-icon,\nhtml.onex-themed .support-icon svg,\nhtml.onex-themed .quick-icon,\nhtml.onex-themed .metric-icon,\nhtml.onex-themed .group-hero-icon,\nhtml.onex-themed .group-detail-icon,\nhtml.onex-themed .group-proto-icon,\nhtml.onex-themed .admin-create-icon,\nhtml.onex-themed .admin-op,\nhtml.onex-themed .tg-card-icon,\nhtml.onex-themed .tg-search span,\nhtml.onex-themed .sub-brand-icon,\nhtml.onex-themed .theme-kicker{\n  color:var(--accent2) !important;\n}\nhtml.onex-themed .metric-icon,\nhtml.onex-themed .quick-icon,\nhtml.onex-themed .support-icon,\nhtml.onex-themed .group-proto-icon,\nhtml.onex-themed .admin-create-icon,\nhtml.onex-themed .tg-card-icon{\n  background:var(--onex-accent-soft) !important;\n  border-color:var(--onex-border) !important;\n}\n\n/* Switches/toggles: selected state is the active theme, not fixed pink. */\nhtml.onex-themed .group-switch input:checked+span,\nhtml.onex-themed .switch input:checked + .slider{\n  background:var(--accent) !important;\n}\nhtml.onex-themed input[type="checkbox"],\nhtml.onex-themed input[type="radio"],\nhtml.onex-themed .cfg-chk,\nhtml.onex-themed .group-config-row input{accent-color:var(--accent) !important}\n\n/* Focus, search and form controls */\nhtml.onex-themed input:focus,\nhtml.onex-themed select:focus,\nhtml.onex-themed textarea:focus,\nhtml.onex-themed .field input:focus,\nhtml.onex-themed .field select:focus,\nhtml.onex-themed .field textarea:focus{\n  border-color:var(--accent) !important;\n  box-shadow:0 0 0 3px var(--onex-accent-soft) !important;\n}\nhtml.onex-themed .field input,\nhtml.onex-themed .field select,\nhtml.onex-themed .field textarea{\n  background:var(--onex-surface-2) !important;\n  color:var(--t1) !important;\n}\n\n/* Links, tabs and active indicators */\nhtml.onex-themed a:not(.btn):not(.tg-join){color:var(--accent2)}\nhtml.onex-themed .nav-item:hover{color:var(--accent2) !important;background:var(--onex-accent-softer) !important}\nhtml.onex-themed .nav-item.on{box-shadow:inset -3px 0 0 var(--accent),0 0 18px color-mix(in srgb,var(--accent) 8%,transparent) !important}\nhtml.onex-themed .sb-toggle{background:var(--accent) !important}\nhtml.onex-themed .nav-new-badge{background:linear-gradient(135deg,var(--accent),var(--purple)) !important}\n\n/* Tables, lists and recent activity */\nhtml.onex-themed .table-wrap,\nhtml.onex-themed .recent-table,\nhtml.onex-themed .admin-table-wrap{\n  background:var(--onex-surface) !important;\n  border-color:var(--onex-border) !important;\n}\nhtml.onex-themed th{background:var(--onex-surface-2) !important;color:var(--t3) !important}\nhtml.onex-themed td{border-color:var(--card-b) !important}\nhtml.onex-themed tr:hover td{background:var(--onex-accent-softer) !important}\n\n/* Stats charts use theme colors instead of always-blue/pink UI chrome. */\nhtml.onex-themed .stats-refresh-btn,\nhtml.onex-themed .stats-panel .panel-icon{color:var(--accent2) !important;border-color:var(--onex-border) !important}\nhtml.onex-themed .traffic-line-d{stroke:var(--accent) !important;filter:drop-shadow(0 0 5px color-mix(in srgb,var(--accent) 70%,transparent)) !important}\nhtml.onex-themed .traffic-line-u{stroke:var(--purple) !important;filter:drop-shadow(0 0 5px color-mix(in srgb,var(--purple) 65%,transparent)) !important}\nhtml.onex-themed .traffic-point{stroke:var(--accent) !important}\n\n/* Telegram/subscription pages had their own fixed blue/pink palette. */\nhtml.onex-themed .tg-hero,\nhtml.onex-themed .tg-join,\nhtml.onex-themed .sub-hero,\nhtml.onex-themed .sub-action.primary,\nhtml.onex-themed .telegram-sub-card{\n  border-color:var(--onex-border) !important;\n}\nhtml.onex-themed .tg-logo-wrap,\nhtml.onex-themed .tg-avatar,\nhtml.onex-themed .telegram-icon{\n  background:linear-gradient(145deg,var(--accent),var(--purple)) !important;\n  box-shadow:0 8px 24px color-mix(in srgb,var(--accent) 18%,transparent) !important;\n}\nhtml.onex-themed .tg-title,\nhtml.onex-themed .tg-card-head h2,\nhtml.onex-themed .sub-hero h1{color:var(--t1) !important}\n\n/* LIGHT MODE: eliminate dark/blue legacy surfaces everywhere. */\nhtml.light.onex-themed body,\nhtml.light.onex-themed .main,\nhtml.light.onex-themed .page,\nhtml.light.onex-themed .page.on{background:#f6f8fc !important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important}\nhtml.light.onex-themed .sidebar,\nhtml.light.onex-themed .mob-bar,\nhtml.light.onex-themed .onex-topbar,\nhtml.light.onex-themed .onex-control-dock{\n  background:#fff !important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;border-color:color-mix(in srgb, rgb(23 23 23 / .09) 92%, var(--accent)) !important;\n}\nhtml.light.onex-themed .group-hero,\nhtml.light.onex-themed .group-list-pane,\nhtml.light.onex-themed .group-detail-pane,\nhtml.light.onex-themed .group-info-card,\nhtml.light.onex-themed .group-link-card,\nhtml.light.onex-themed .group-proto-card,\nhtml.light.onex-themed .group-manage-card,\nhtml.light.onex-themed .group-configs-card,\nhtml.light.onex-themed .group-card,\nhtml.light.onex-themed .group-stat,\nhtml.light.onex-themed .cfg-page-hero,\nhtml.light.onex-themed .cfg-stat-card,\nhtml.light.onex-themed .cfg-list-shell,\nhtml.light.onex-themed .cfg-card,\nhtml.light.onex-themed .cfg-side,\nhtml.light.onex-themed .admin-card,\nhtml.light.onex-themed .admin-create-card,\nhtml.light.onex-themed .admin-list-card,\nhtml.light.onex-themed .admin-perm-card,\nhtml.light.onex-themed .admin-activity-list,\nhtml.light.onex-themed .security-health,\nhtml.light.onex-themed .security-login-box,\nhtml.light.onex-themed .security-log-box,\nhtml.light.onex-themed .logs-list-card,\nhtml.light.onex-themed .sub-glass,\nhtml.light.onex-themed .sub-hero,\nhtml.light.onex-themed .telegram-sub-card,\nhtml.light.onex-themed .stats-kpi,\nhtml.light.onex-themed .stats-panel,\nhtml.light.onex-themed .tg-hero,\nhtml.light.onex-themed .tg-glass,\nhtml.light.onex-themed .tg-stat,\nhtml.light.onex-themed .backup,\nhtml.light.onex-themed .advanced-config-card,\nhtml.light.onex-themed .advanced-subcard{\n  background:#fff !important;\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important;\n  border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent)) !important;\n  box-shadow:0 10px 30px color-mix(in srgb, rgb(23 23 23 / .07) 92%, var(--accent)) !important;\n}\nhtml.light.onex-themed .group-info-item,\nhtml.light.onex-themed .group-link-url,\nhtml.light.onex-themed .group-config-row,\nhtml.light.onex-themed .group-proto-row,\nhtml.light.onex-themed .group-search,\nhtml.light.onex-themed .cfg-search-box,\nhtml.light.onex-themed .cfg-select,\nhtml.light.onex-themed .cfg-filter-btn,\nhtml.light.onex-themed .admin-select-shell,\nhtml.light.onex-themed .admin-perm-select,\nhtml.light.onex-themed .logs-filter,\nhtml.light.onex-themed .tg-field input,\nhtml.light.onex-themed .tg-input-wrap input,\nhtml.light.onex-themed .tg-search,\nhtml.light.onex-themed .tg-user-tools input,\nhtml.light.onex-themed .tg-user-tools select,\nhtml.light.onex-themed .tg-glass textarea,\nhtml.light.onex-themed .sub-url-box,\nhtml.light.onex-themed .sub-action,\nhtml.light.onex-themed .range-mini,\nhtml.light.onex-themed .mini-action{\n  background:#f8fafc !important;color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent)) !important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent)) !important;\n}\nhtml.light.onex-themed .page-title,\nhtml.light.onex-themed .card-title,\nhtml.light.onex-themed .group-hero h1,\nhtml.light.onex-themed .group-card-title,\nhtml.light.onex-themed .group-detail-title,\nhtml.light.onex-themed .admin-card-title,\nhtml.light.onex-themed .tg-title,\nhtml.light.onex-themed .tg-card-head h2,\nhtml.light.onex-themed .sub-hero h1,\nhtml.light.onex-themed .stats-panel-head b,\nhtml.light.onex-themed .stats-kpi b{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)) !important}\nhtml.light.onex-themed .page-sub,\nhtml.light.onex-themed .group-hero p,\nhtml.light.onex-themed .group-card-desc,\nhtml.light.onex-themed .group-detail-title p,\nhtml.light.onex-themed .admin-card-sub,\nhtml.light.onex-themed .tg-desc,\nhtml.light.onex-themed .tg-card-head p,\nhtml.light.onex-themed .sub-hero p,\nhtml.light.onex-themed .stats-kpi small,\nhtml.light.onex-themed .stats-panel-head small{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent)) !important}\n\n/* Remove fixed dark inline surfaces on light mode, while primary/danger controls\n   are restored immediately below by their explicit themed selectors. */\nhtml.light.onex-themed .page div[style*="background:"],\nhtml.light.onex-themed .page section[style*="background:"],\nhtml.light.onex-themed .page article[style*="background:"],\nhtml.light.onex-themed .page aside[style*="background:"]{\n  background:#fff !important;color:inherit;\n}\nhtml.light.onex-themed .btn-p,\nhtml.light.onex-themed .btn-primary,\nhtml.light.onex-themed .cfg-primary-btn,\nhtml.light.onex-themed .group-create-btn,\nhtml.light.onex-themed .group-config-save,\nhtml.light.onex-themed .sub-action.primary,\nhtml.light.onex-themed .tg-btn.primary,\nhtml.light.onex-themed .tg-join,\nhtml.light.onex-themed .stats-range .range-tab.on,\nhtml.light.onex-themed .range-tab.on,\nhtml.light.onex-themed .nav-item.on{\n  background:linear-gradient(135deg,var(--accent),var(--purple)) !important;color:#fff !important;\n}\nhtml.light.onex-themed .btn-d,\nhtml.light.onex-themed .group-manage-actions button:nth-child(2),\nhtml.light.onex-themed .group-manage-actions button:nth-child(3),\nhtml.light.onex-themed .tg-user-actions button.danger{\n  background:rgba(239,68,68,.08) !important;color:#dc2626 !important;\n}\nhtml.light.onex-themed .group-switch input:checked+span,\nhtml.light.onex-themed .switch input:checked + .slider{background:var(--accent) !important}\nhtml.light.onex-themed .theme-current-row{background:#f8fafc !important;color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent)) !important}\nhtml.light.onex-themed .theme-custom-box,\nhtml.light.onex-themed .theme-mode-box{background:#fff !important}\n\n/* ============================================================\n   ONEX FINAL THEME AUDIT — READABILITY + FULL-PANEL CONSISTENCY\n   This is intentionally the last theme layer. It neutralizes legacy\n   component palettes only where they conflict with the selected theme.\n   Semantic success/warning/danger states remain meaningful.\n   ============================================================ */\nhtml.onex-themed{\n  color-scheme:dark;\n  --theme-text:var(--t1);\n  --theme-muted:var(--t2);\n  --theme-subtle:var(--t3);\n  --theme-surface:var(--card);\n  --theme-surface-2:var(--bg3);\n  --theme-input:var(--input-bg);\n}\nhtml.light.onex-themed{color-scheme:light}\nhtml.onex-themed body,\nhtml.onex-themed button,\nhtml.onex-themed input,\nhtml.onex-themed select,\nhtml.onex-themed textarea{font-family:\'Vazirmatn\',Tahoma,Arial,sans-serif}\nhtml.onex-themed body.en,\nhtml.onex-themed body.en button,\nhtml.onex-themed body.en input,\nhtml.onex-themed body.en select,\nhtml.onex-themed body.en textarea{font-family:\'Inter\',\'Vazirmatn\',system-ui,sans-serif}\n\n/* Base text and surfaces */\nhtml.onex-themed .page,\nhtml.onex-themed .main{color:var(--theme-text)}\nhtml.onex-themed .page-title,\nhtml.onex-themed .card-title,\nhtml.onex-themed .onex-card-title,\nhtml.onex-themed .onex-card-head,\nhtml.onex-themed .metric-label,\nhtml.onex-themed .metric-val,\nhtml.onex-themed .metric-trend,\nhtml.onex-themed .quick-name,\nhtml.onex-themed .quick-desc,\nhtml.onex-themed .page-sub,\nhtml.onex-themed .field label,\nhtml.onex-themed .info-row,\nhtml.onex-themed .form-row,\nhtml.onex-themed .health-name,\nhtml.onex-themed .health-pct,\nhtml.onex-themed .cfg-name-row b,\nhtml.onex-themed .cfg-proto,\nhtml.onex-themed .cfg-meta,\nhtml.onex-themed .cfg-status,\nhtml.onex-themed .cfg-list-head,\nhtml.onex-themed .group-card-title,\nhtml.onex-themed .group-card-desc,\nhtml.onex-themed .group-detail-title,\nhtml.onex-themed .group-detail-title p,\nhtml.onex-themed .admin-card-title,\nhtml.onex-themed .admin-card-sub,\nhtml.onex-themed .admin-user-name,\nhtml.onex-themed .tg-title,\nhtml.onex-themed .tg-desc,\nhtml.onex-themed .tg-card-head h2,\nhtml.onex-themed .sub-hero h1,\nhtml.onex-themed .sub-hero p,\nhtml.onex-themed .stats-panel-head b,\nhtml.onex-themed .stats-kpi b,\nhtml.onex-themed .stats-kpi small,\nhtml.onex-themed .logs-page-sub,\nhtml.onex-themed .logs-event-title,\nhtml.onex-themed .logs-event-desc,\nhtml.onex-themed .security-action-row,\nhtml.onex-themed .security-detail-row,\nhtml.onex-themed .security-section-title,\nhtml.onex-themed .theme-panel-head h2,\nhtml.onex-themed .theme-custom-head h3{color:var(--theme-text)!important}\nhtml.onex-themed .page-sub,\nhtml.onex-themed .quick-desc,\nhtml.onex-themed .field label,\nhtml.onex-themed .cfg-meta,\nhtml.onex-themed .admin-card-sub,\nhtml.onex-themed .admin-user-label,\nhtml.onex-themed .tg-desc,\nhtml.onex-themed .tg-card-head p,\nhtml.onex-themed .sub-hero p,\nhtml.onex-themed .stats-kpi small,\nhtml.onex-themed .stats-panel-head small,\nhtml.onex-themed .logs-event-time,\nhtml.onex-themed .security-action-row small,\nhtml.onex-themed .security-recent-meta,\nhtml.onex-themed .security-detail-row span,\nhtml.onex-themed .security-empty{color:var(--theme-subtle)!important}\n\nhtml.onex-themed .card,\nhtml.onex-themed .metric,\nhtml.onex-themed .onex-card,\nhtml.onex-themed .onex-metric,\nhtml.onex-themed .support-tile,\nhtml.onex-themed .quick-item,\nhtml.onex-themed .table-wrap,\nhtml.onex-themed .link-box,\nhtml.onex-themed .sub-box,\nhtml.onex-themed .onex-topbar,\nhtml.onex-themed .onex-control-dock,\nhtml.onex-themed .version-mini-card,\nhtml.onex-themed .dashboard-hero,\nhtml.onex-themed .recent-card,\nhtml.onex-themed .server-info,\nhtml.onex-themed .health-list,\nhtml.onex-themed .traffic-panel,\nhtml.onex-themed .uptime-panel,\nhtml.onex-themed .server-panel,\nhtml.onex-themed .panel-summary,\nhtml.onex-themed .logs-summary,\nhtml.onex-themed .logs-list-card,\nhtml.onex-themed .security-details,\nhtml.onex-themed .theme-current-row,\nhtml.onex-themed .theme-custom-box,\nhtml.onex-themed .theme-mode-box{\n  background:var(--theme-surface)!important;\n  color:var(--theme-text)!important;\n  border-color:var(--onex-border)!important;\n}\n\n/* Secondary controls / fields */\nhtml.onex-themed .field input,\nhtml.onex-themed .field select,\nhtml.onex-themed .field textarea,\nhtml.onex-themed .cfg-search-box,\nhtml.onex-themed .cfg-filter-btn,\nhtml.onex-themed .cfg-select,\nhtml.onex-themed .group-search,\nhtml.onex-themed .group-info-item,\nhtml.onex-themed .group-link-url,\nhtml.onex-themed .group-config-row,\nhtml.onex-themed .group-proto-row,\nhtml.onex-themed .admin-select-shell,\nhtml.onex-themed .admin-perm-select,\nhtml.onex-themed .logs-filter,\nhtml.onex-themed .logs-search-wrap,\nhtml.onex-themed .tg-field input,\nhtml.onex-themed .tg-input-wrap input,\nhtml.onex-themed .tg-search,\nhtml.onex-themed .tg-user-tools input,\nhtml.onex-themed .tg-user-tools select,\nhtml.onex-themed .tg-glass textarea,\nhtml.onex-themed .sub-url-box,\nhtml.onex-themed .range-mini,\nhtml.onex-themed .mini-action,\nhtml.onex-themed .advanced-subcard,\nhtml.onex-themed .advanced-toggle,\nhtml.onex-themed .advanced-preview,\nhtml.onex-themed .protocol-selected-info{\n  background:var(--theme-input)!important;\n  color:var(--theme-text)!important;\n  border-color:var(--onex-border)!important;\n}\nhtml.onex-themed input::placeholder,\nhtml.onex-themed textarea::placeholder{color:var(--theme-subtle)!important}\nhtml.onex-themed option{background:var(--bg2);color:var(--theme-text)}\nhtml.onex-themed input:focus,\nhtml.onex-themed select:focus,\nhtml.onex-themed textarea:focus{border-color:var(--onex-border-strong)!important;box-shadow:0 0 0 3px var(--onex-accent-soft)!important}\n\n/* Primary UI always uses the selected palette. */\nhtml.onex-themed .btn-p,\nhtml.onex-themed .btn-primary,\nhtml.onex-themed .primary,\nhtml.onex-themed .cfg-primary-btn,\nhtml.onex-themed .group-create-btn,\nhtml.onex-themed .group-config-save,\nhtml.onex-themed .sub-action.primary,\nhtml.onex-themed .tg-btn.primary,\nhtml.onex-themed .tg-join,\nhtml.onex-themed .protocol-picker-confirm,\nhtml.onex-themed .stats-refresh-btn,\nhtml.onex-themed .notify-update-btn,\nhtml.onex-themed .range-tab.on,\nhtml.onex-themed .range-mini button.on,\nhtml.onex-themed .cfg-filter-btn.active,\nhtml.onex-themed .cfg-filter-btn.on,\nhtml.onex-themed .nav-item.on,\nhtml.onex-themed .onex-3d-control.active{\n  background:linear-gradient(135deg,var(--accent),var(--purple))!important;\n  border-color:color-mix(in srgb,var(--accent) 58%,transparent)!important;\n  color:#fff!important;\n}\n\n/* Common legacy blue/pink buttons: only recolor when they are action controls. */\nhtml.onex-themed .cfg-menu-btn,\nhtml.onex-themed .group-copy-btn,\nhtml.onex-themed .group-link-actions button,\nhtml.onex-themed .group-manage-actions button:first-child,\nhtml.onex-themed .logs-refresh-btn,\nhtml.onex-themed .logs-advanced-btn,\nhtml.onex-themed .logs-reset-filter,\nhtml.onex-themed .tg-tabs button.on,\nhtml.onex-themed .tg-audience button.on,\nhtml.onex-themed .tg-input-wrap button,\nhtml.onex-themed .tg-user-actions button:not(.danger),\nhtml.onex-themed .security-stat-action,\nhtml.onex-themed .theme-mode-buttons button.on{\n  background:var(--onex-accent-soft)!important;\n  color:var(--accent2)!important;\n  border-color:var(--onex-border-strong)!important;\n}\n\n/* Structural legacy gradients are flattened to theme surfaces in both modes. */\nhtml.onex-themed .group-hero,\nhtml.onex-themed .group-list-pane,\nhtml.onex-themed .group-detail-pane,\nhtml.onex-themed .group-info-card,\nhtml.onex-themed .group-link-card,\nhtml.onex-themed .group-proto-card,\nhtml.onex-themed .group-manage-card,\nhtml.onex-themed .group-configs-card,\nhtml.onex-themed .group-card,\nhtml.onex-themed .group-stat,\nhtml.onex-themed .cfg-page-hero,\nhtml.onex-themed .cfg-stat-card,\nhtml.onex-themed .cfg-list-shell,\nhtml.onex-themed .cfg-card,\nhtml.onex-themed .cfg-side,\nhtml.onex-themed .admin-card,\nhtml.onex-themed .admin-create-card,\nhtml.onex-themed .admin-list-card,\nhtml.onex-themed .admin-perm-card,\nhtml.onex-themed .admin-activity-list,\nhtml.onex-themed .security-health,\nhtml.onex-themed .security-login-box,\nhtml.onex-themed .security-log-box,\nhtml.onex-themed .sub-glass,\nhtml.onex-themed .sub-hero,\nhtml.onex-themed .telegram-sub-card,\nhtml.onex-themed .stats-kpi,\nhtml.onex-themed .stats-panel,\nhtml.onex-themed .tg-hero,\nhtml.onex-themed .tg-glass,\nhtml.onex-themed .tg-stat,\nhtml.onex-themed .tg-tabs,\nhtml.onex-themed .backup{\n  background:var(--theme-surface)!important;\n  color:var(--theme-text)!important;\n  border-color:var(--onex-border)!important;\n}\n\n/* Light mode: explicitly fix fixed white-on-white and blue legacy components. */\nhtml.light.onex-themed body,\nhtml.light.onex-themed .main,\nhtml.light.onex-themed .page{background:var(--bg)!important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important}\nhtml.light.onex-themed .sidebar,\nhtml.light.onex-themed .mob-bar,\nhtml.light.onex-themed .onex-topbar,\nhtml.light.onex-themed .onex-control-dock{background:#fff!important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important}\nhtml.light.onex-themed .card,\nhtml.light.onex-themed .metric,\nhtml.light.onex-themed .onex-card,\nhtml.light.onex-themed .onex-metric,\nhtml.light.onex-themed .support-tile,\nhtml.light.onex-themed .quick-item,\nhtml.light.onex-themed .table-wrap,\nhtml.light.onex-themed .recent-card,\nhtml.light.onex-themed .dashboard-hero,\nhtml.light.onex-themed .server-info,\nhtml.light.onex-themed .health-list,\nhtml.light.onex-themed .traffic-panel,\nhtml.light.onex-themed .uptime-panel,\nhtml.light.onex-themed .server-panel,\nhtml.light.onex-themed .panel-summary,\nhtml.light.onex-themed .logs-summary,\nhtml.light.onex-themed .logs-list-card,\nhtml.light.onex-themed .group-hero,\nhtml.light.onex-themed .group-list-pane,\nhtml.light.onex-themed .group-detail-pane,\nhtml.light.onex-themed .group-info-card,\nhtml.light.onex-themed .group-link-card,\nhtml.light.onex-themed .group-proto-card,\nhtml.light.onex-themed .group-manage-card,\nhtml.light.onex-themed .group-configs-card,\nhtml.light.onex-themed .group-card,\nhtml.light.onex-themed .group-stat,\nhtml.light.onex-themed .cfg-page-hero,\nhtml.light.onex-themed .cfg-stat-card,\nhtml.light.onex-themed .cfg-list-shell,\nhtml.light.onex-themed .cfg-card,\nhtml.light.onex-themed .cfg-side,\nhtml.light.onex-themed .admin-card,\nhtml.light.onex-themed .admin-create-card,\nhtml.light.onex-themed .admin-list-card,\nhtml.light.onex-themed .admin-perm-card,\nhtml.light.onex-themed .admin-activity-list,\nhtml.light.onex-themed .security-health,\nhtml.light.onex-themed .security-login-box,\nhtml.light.onex-themed .security-log-box,\nhtml.light.onex-themed .sub-glass,\nhtml.light.onex-themed .sub-hero,\nhtml.light.onex-themed .telegram-sub-card,\nhtml.light.onex-themed .stats-kpi,\nhtml.light.onex-themed .stats-panel,\nhtml.light.onex-themed .tg-hero,\nhtml.light.onex-themed .tg-glass,\nhtml.light.onex-themed .tg-stat,\nhtml.light.onex-themed .backup{background:#fff!important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important}\n\n/* Fixed inline colors from legacy cards are the main source of unreadable light text. */\nhtml.light.onex-themed .page [style*="color:#fff"],\nhtml.light.onex-themed .page [style*="color: #fff"],\nhtml.light.onex-themed .page [style*="color:white"],\nhtml.light.onex-themed .page [style*="color: white"]{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important}\nhtml.light.onex-themed .page [style*="background:#0b"],\nhtml.light.onex-themed .page [style*="background:#05"],\nhtml.light.onex-themed .page [style*="background:#06"],\nhtml.light.onex-themed .page [style*="background:#12"],\nhtml.light.onex-themed .page [style*="background:#15"],\nhtml.light.onex-themed .page [style*="background:#16"],\nhtml.light.onex-themed .page [style*="background:#21"],\nhtml.light.onex-themed .page [style*="background:#25"],\nhtml.light.onex-themed .page [style*="background:rgba(2,"]{background:#f8fafc!important;color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important}\n/* Restore white text on actual action buttons after the broad inline-color rule. */\nhtml.light.onex-themed .btn-p,\nhtml.light.onex-themed .btn-primary,\nhtml.light.onex-themed .primary,\nhtml.light.onex-themed .cfg-primary-btn,\nhtml.light.onex-themed .group-create-btn,\nhtml.light.onex-themed .group-config-save,\nhtml.light.onex-themed .sub-action.primary,\nhtml.light.onex-themed .tg-btn.primary,\nhtml.light.onex-themed .tg-join,\nhtml.light.onex-themed .protocol-picker-confirm,\nhtml.light.onex-themed .range-tab.on,\nhtml.light.onex-themed .range-mini button.on,\nhtml.light.onex-themed .nav-item.on{color:#fff!important}\n\n/* Light tables and table-like lists */\nhtml.light.onex-themed th,\nhtml.light.onex-themed td,\nhtml.light.onex-themed .admin-table-head,\nhtml.light.onex-themed .admin-row{background:#fff!important;color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important;border-color:color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent))!important}\nhtml.light.onex-themed th{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))!important}\nhtml.light.onex-themed tr:hover td,\nhtml.light.onex-themed .admin-row:hover{background:#f8fafc!important}\nhtml.light.onex-themed .admin-row.selected{background:var(--onex-accent-soft)!important}\n\n/* Stats/graphs: labels readable, graph accents follow theme. */\nhtml.onex-themed .stats-panel-head b,\nhtml.onex-themed .stats-kpi b,\nhtml.onex-themed .stats-kpi small,\nhtml.onex-themed .stats-legend,\nhtml.onex-themed .stats-panel-head small{color:var(--theme-text)!important}\nhtml.onex-themed .traffic-line-d{stroke:var(--accent)!important}\nhtml.onex-themed .traffic-line-u{stroke:var(--purple)!important}\nhtml.onex-themed .chart-fill{fill:color-mix(in srgb,var(--accent) 18%,transparent)!important}\nhtml.onex-themed .chart-line{stroke:var(--accent)!important}\nhtml.onex-themed .chart-dot{fill:var(--accent)!important}\nhtml.onex-themed .stats-kpi-icon,\nhtml.onex-themed .metric-icon,\nhtml.onex-themed .quick-icon,\nhtml.onex-themed .cfg-stat-icon{color:var(--accent2)!important;border-color:var(--onex-border)!important;background:var(--onex-accent-soft)!important}\n\n/* Status semantics stay visible in both modes. */\nhtml.light.onex-themed .security-recent-state.ok,\nhtml.light.onex-themed .admin-badge.active{color:#15803d!important;background:rgba(34,197,94,.10)!important}\nhtml.light.onex-themed .security-recent-state.bad,\nhtml.light.onex-themed .admin-badge.blocked,\nhtml.light.onex-themed .tg-user-state.bad{color:#dc2626!important;background:rgba(239,68,68,.08)!important}\nhtml.light.onex-themed .admin-badge.invalid{color:#b45309!important;background:rgba(245,158,11,.10)!important}\nhtml.light.onex-themed .btn-d,\nhtml.light.onex-themed .danger-row,\nhtml.light.onex-themed .delete-all-configs-glass{color:#dc2626!important}\n\n/* Modals/toasts must follow the same surface and text rules. */\nhtml.onex-themed .modal,\nhtml.onex-themed .delete-all-modal,\nhtml.onex-themed .logs-detail-modal{background:var(--theme-surface)!important;color:var(--theme-text)!important;border-color:var(--onex-border)!important}\nhtml.light.onex-themed .modal,\nhtml.light.onex-themed .delete-all-modal,\nhtml.light.onex-themed .logs-detail-modal{background:#fff!important;color:#0f172a!important}\nhtml.light.onex-themed .modal-title,\nhtml.light.onex-themed .delete-all-modal-title,\nhtml.light.onex-themed .logs-detail-head{color:#0f172a!important}\n\n/* Prevent accidental white text on white cards from inherited legacy rules. */\nhtml.light.onex-themed .page .tg-copy b,\nhtml.light.onex-themed .page .tg-card-head h2,\nhtml.light.onex-themed .page .group-card-title,\nhtml.light.onex-themed .page .group-detail-title,\nhtml.light.onex-themed .page .cfg-name-row b,\nhtml.light.onex-themed .page .cfg-list-head b,\nhtml.light.onex-themed .page .stats-panel-head b,\nhtml.light.onex-themed .page .admin-card-title,\nhtml.light.onex-themed .page .admin-user-name{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important}\n\n/* Protocol picker / advanced config */\nhtml.light.onex-themed .protocol-picker,\nhtml.light.onex-themed .protocol-option,\nhtml.light.onex-themed .protocol-picker-foot,\nhtml.light.onex-themed .advanced-config-panel,\nhtml.light.onex-themed .advanced-config-card{background:#fff!important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))!important}\nhtml.light.onex-themed .protocol-option-name,\nhtml.light.onex-themed .protocol-option-desc,\nhtml.light.onex-themed .advanced-subtitle,\nhtml.light.onex-themed .advanced-note{color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important}\nhtml.light.onex-themed .protocol-option.selected{border-color:var(--accent)!important;background:var(--onex-accent-soft)!important}\n\n/* Keep intentional brand/logo artwork unchanged; only surrounding typography is themed. */\nhtml.onex-themed .sb-logo-caption span,\nhtml.onex-themed .sb-logo-caption b{filter:drop-shadow(0 0 6px color-mix(in srgb,var(--accent) 35%,transparent))}\n\n/* ============================================================\n   ONEX CONTRAST AUDIT — FINAL GLOBAL PASS\n   Goal: no unreadable text, no legacy blue/pink surfaces and no\n   white-on-white content in any theme. Semantic status colors stay.\n   ============================================================ */\nhtml.onex-themed .dashboard-hero,\nhtml.onex-themed .onex-metric,\nhtml.onex-themed .onex-card,\nhtml.onex-themed .version-mini-card,\nhtml.onex-themed .quick-item,\nhtml.onex-themed .recent-card,\nhtml.onex-themed .server-info,\nhtml.onex-themed .health-list,\nhtml.onex-themed .traffic-panel,\nhtml.onex-themed .uptime-panel,\nhtml.onex-themed .telegram-card,\nhtml.onex-themed .onex-3d-control,\nhtml.onex-themed .top-chip,\nhtml.onex-themed .panel-summary,\nhtml.onex-themed .recent-table,\nhtml.onex-themed .table-wrap,\nhtml.onex-themed .action-card,\nhtml.onex-themed .logs-summary,\nhtml.onex-themed .logs-list-card{\n  border-color:var(--onex-border)!important;\n}\n\n/* Dashboard surfaces use the selected palette instead of legacy blue glass. */\nhtml.onex-themed .onex-metric,\nhtml.onex-themed .onex-card,\nhtml.onex-themed .version-mini-card,\nhtml.onex-themed .quick-item,\nhtml.onex-themed .recent-card,\nhtml.onex-themed .server-info,\nhtml.onex-themed .health-list,\nhtml.onex-themed .traffic-panel,\nhtml.onex-themed .uptime-panel,\nhtml.onex-themed .panel-summary,\nhtml.onex-themed .action-card,\nhtml.onex-themed .telegram-card{\n  background:var(--theme-surface)!important;\n  color:var(--theme-text)!important;\n}\nhtml.onex-themed .onex-card-head{border-color:var(--onex-border)!important}\nhtml.onex-themed .onex-3d-control{\n  background:linear-gradient(145deg,var(--theme-surface-2),var(--theme-surface))!important;\n  color:var(--theme-text)!important;\n  box-shadow:0 8px 22px color-mix(in srgb,var(--accent) 9%,transparent),inset 0 1px rgba(255,255,255,.06)!important;\n}\nhtml.onex-themed .onex-3d-control .control-icon,\nhtml.onex-themed .top-avatar,\nhtml.onex-themed .sb-logo-icon,\nhtml.onex-themed .mob-brand-icon{\n  background:linear-gradient(145deg,var(--accent),var(--purple))!important;\n  box-shadow:0 8px 22px color-mix(in srgb,var(--accent) 20%,transparent)!important;\n}\nhtml.onex-themed .version-mini-icon,\nhtml.onex-themed .metric-icon,\nhtml.onex-themed .quick-icon,\nhtml.onex-themed .health-icon{\n  background:var(--onex-accent-soft)!important;\n  border-color:var(--onex-border)!important;\n  color:var(--accent2)!important;\n}\nhtml.onex-themed .quick-item:nth-child(2) .quick-icon,\nhtml.onex-themed .quick-item:nth-child(3) .quick-icon,\nhtml.onex-themed .quick-item:nth-child(4) .quick-icon{\n  background:var(--onex-accent-soft)!important;color:var(--accent2)!important;\n}\nhtml.onex-themed .hero-kicker,\nhtml.onex-themed .hero-title span,\nhtml.onex-themed .onex-footer b,\nhtml.onex-themed .version-mini-icon,\nhtml.onex-themed .tg-handle,\nhtml.onex-themed .onex-card-title svg{color:var(--accent2)!important}\nhtml.onex-themed .hero-title,\nhtml.onex-themed .hero-sub,\nhtml.onex-themed .version-mini-copy b,\nhtml.onex-themed .version-mini-copy strong,\nhtml.onex-themed .metric-label,\nhtml.onex-themed .metric-val,\nhtml.onex-themed .metric-trend,\nhtml.onex-themed .control-title,\nhtml.onex-themed .control-sub,\nhtml.onex-themed .health-name,\nhtml.onex-themed .health-pct,\nhtml.onex-themed .quick-name,\nhtml.onex-themed .quick-desc,\nhtml.onex-themed .info-row,\nhtml.onex-themed .recent-table th,\nhtml.onex-themed .recent-table td,\nhtml.onex-themed .top-server b,\nhtml.onex-themed .top-server small,\nhtml.onex-themed .top-chip{\n  color:var(--theme-text)!important;\n}\nhtml.onex-themed .metric-label,\nhtml.onex-themed .control-sub,\nhtml.onex-themed .health-name,\nhtml.onex-themed .health-pct,\nhtml.onex-themed .quick-desc,\nhtml.onex-themed .hero-sub,\nhtml.onex-themed .version-mini-copy b,\nhtml.onex-themed .recent-table th,\nhtml.onex-themed .info-row span:first-child,\nhtml.onex-themed .top-server small{color:var(--theme-subtle)!important}\n\n/* Charts and progress indicators follow the selected accent. */\nhtml.onex-themed .chart-line{stroke:var(--accent)!important;filter:drop-shadow(0 0 7px color-mix(in srgb,var(--accent) 55%,transparent))!important}\nhtml.onex-themed .chart-dot{fill:var(--accent)!important;stroke:var(--accent)!important}\nhtml.onex-themed .health-fill{background:linear-gradient(90deg,var(--accent),var(--purple))!important;box-shadow:0 0 12px color-mix(in srgb,var(--accent) 35%,transparent)!important}\nhtml.onex-themed .range-mini button.on{background:var(--accent)!important;color:#fff!important}\nhtml.onex-themed .chart-badge{border-color:var(--onex-border)!important;background:var(--theme-surface-2)!important;color:var(--theme-text)!important}\n\n/* Light mode: every dashboard text-bearing element gets a readable dark token. */\nhtml.light.onex-themed .page span,\nhtml.light.onex-themed .page p,\nhtml.light.onex-themed .page b,\nhtml.light.onex-themed .page strong,\nhtml.light.onex-themed .page small,\nhtml.light.onex-themed .page label,\nhtml.light.onex-themed .page td,\nhtml.light.onex-themed .page th,\nhtml.light.onex-themed .page h1,\nhtml.light.onex-themed .page h2,\nhtml.light.onex-themed .page h3,\nhtml.light.onex-themed .page h4,\nhtml.light.onex-themed .page h5,\nhtml.light.onex-themed .page h6{\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important;\n}\nhtml.light.onex-themed .page .page-sub,\nhtml.light.onex-themed .page .hero-sub,\nhtml.light.onex-themed .page .metric-label,\nhtml.light.onex-themed .page .control-sub,\nhtml.light.onex-themed .page .quick-desc,\nhtml.light.onex-themed .page .field label,\nhtml.light.onex-themed .page .cfg-meta,\nhtml.light.onex-themed .page .admin-card-sub,\nhtml.light.onex-themed .page .tg-desc,\nhtml.light.onex-themed .page .stats-kpi small,\nhtml.light.onex-themed .page .stats-panel-head small,\nhtml.light.onex-themed .page .logs-event-time,\nhtml.light.onex-themed .page .security-recent-meta,\nhtml.light.onex-themed .page .security-detail-row span,\nhtml.light.onex-themed .page .onex-footer,\nhtml.light.onex-themed .page .version-mini-copy b{\n  color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))!important;\n}\n/* Inline legacy white/low-alpha text is the source of most white-on-white bugs. */\nhtml.light.onex-themed .page [style*="color:#fff"],\nhtml.light.onex-themed .page [style*="color: #fff"],\nhtml.light.onex-themed .page [style*="color:white"],\nhtml.light.onex-themed .page [style*="color: white"],\nhtml.light.onex-themed .page [style*="color:rgba(255,255,255"],\nhtml.light.onex-themed .page [style*="color: rgba(255,255,255"],\nhtml.light.onex-themed .page [style*="color:rgba(248,250,252"],\nhtml.light.onex-themed .page [style*="color: rgba(248,250,252"]{\n  color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important;\n}\n/* Dashboard light surfaces: white cards, dark text, themed accents. */\nhtml.light.onex-themed .dashboard-hero,\nhtml.light.onex-themed .onex-metric,\nhtml.light.onex-themed .onex-card,\nhtml.light.onex-themed .version-mini-card,\nhtml.light.onex-themed .quick-item,\nhtml.light.onex-themed .recent-card,\nhtml.light.onex-themed .server-info,\nhtml.light.onex-themed .health-list,\nhtml.light.onex-themed .traffic-panel,\nhtml.light.onex-themed .uptime-panel,\nhtml.light.onex-themed .panel-summary,\nhtml.light.onex-themed .action-card,\nhtml.light.onex-themed .telegram-card{\n  background:#fff!important;\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important;\n  border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))!important;\n  box-shadow:0 10px 30px color-mix(in srgb, rgb(23 23 23 / .07) 92%, var(--accent))!important;\n}\nhtml.light.onex-themed .onex-card-head{border-color:color-mix(in srgb, rgb(23 23 23 / .08) 92%, var(--accent))!important}\nhtml.light.onex-themed .onex-3d-control{background:#fff!important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))!important}\nhtml.light.onex-themed .chart-grid-line{stroke:color-mix(in srgb, rgb(114 114 114 / .16) 82%, var(--accent))!important}\nhtml.light.onex-themed .chart-badge{background:#f8fafc!important;color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))!important}\nhtml.light.onex-themed .recent-table th{background:#f8fafc!important;color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))!important}\nhtml.light.onex-themed .recent-table td{color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important;border-color:color-mix(in srgb, rgb(23 23 23 / .07) 92%, var(--accent))!important}\nhtml.light.onex-themed .info-row{border-color:color-mix(in srgb, rgb(23 23 23 / .07) 92%, var(--accent))!important}\nhtml.light.onex-themed .info-row span:first-child{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))!important}\nhtml.light.onex-themed .info-row span:last-child{color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important}\nhtml.light.onex-themed .top-chip{background:#f8fafc!important;color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))!important}\nhtml.light.onex-themed .health-track{background:#e2e8f0!important}\nhtml.light.onex-themed .xray-state{background:rgba(34,197,94,.07)!important;border-color:rgba(34,197,94,.18)!important}\nhtml.light.onex-themed .xray-state span:last-child,\nhtml.light.onex-themed .recent-status{color:#15803d!important}\n\n/* Restore intentional semantic/action colors after the light contrast pass. */\nhtml.light.onex-themed .btn-p,\nhtml.light.onex-themed .btn-primary,\nhtml.light.onex-themed .primary,\nhtml.light.onex-themed .cfg-primary-btn,\nhtml.light.onex-themed .group-create-btn,\nhtml.light.onex-themed .group-config-save,\nhtml.light.onex-themed .sub-action.primary,\nhtml.light.onex-themed .tg-btn.primary,\nhtml.light.onex-themed .tg-join,\nhtml.light.onex-themed .protocol-picker-confirm,\nhtml.light.onex-themed .range-tab.on,\nhtml.light.onex-themed .range-mini button.on,\nhtml.light.onex-themed .nav-item.on,\nhtml.light.onex-themed .onex-3d-control.active{color:#fff!important}\nhtml.light.onex-themed .page a:not(.btn):not(.tg-join){color:var(--accent2)!important}\nhtml.light.onex-themed .hero-title span,\nhtml.light.onex-themed .hero-kicker,\nhtml.light.onex-themed .onex-footer b,\nhtml.light.onex-themed .tg-handle{color:var(--accent2)!important}\nhtml.light.onex-themed .btn-d,\nhtml.light.onex-themed .danger-row,\nhtml.light.onex-themed .delete-all-configs-glass,\nhtml.light.onex-themed .tg-user-actions button.danger{color:#dc2626!important}\nhtml.light.onex-themed .ok,\nhtml.light.onex-themed .good,\nhtml.light.onex-themed .recent-status,\nhtml.light.onex-themed .security-recent-state.ok,\nhtml.light.onex-themed .admin-badge.active{color:#15803d!important}\nhtml.light.onex-themed .bad,\nhtml.light.onex-themed .security-recent-state.bad,\nhtml.light.onex-themed .admin-badge.blocked{color:#dc2626!important}\nhtml.light.onex-themed .orange,\nhtml.light.onex-themed .admin-badge.invalid{color:#b45309!important}\n\nhtml.onex-themed .telegram-card{background:linear-gradient(145deg,var(--theme-surface-2),var(--theme-surface))!important;color:var(--theme-text)!important}\nhtml.onex-themed .tg-logo{background:linear-gradient(145deg,var(--accent),var(--purple))!important;box-shadow:0 0 28px color-mix(in srgb,var(--accent) 45%,transparent)!important}\nhtml.onex-themed .tg-btn{background:linear-gradient(135deg,var(--accent),var(--purple))!important;box-shadow:0 8px 24px color-mix(in srgb,var(--accent) 24%,transparent)!important;color:#fff!important}\nhtml.light.onex-themed .telegram-card{background:#fff!important;border-color:color-mix(in srgb, rgb(23 23 23 / .10) 92%, var(--accent))!important;box-shadow:0 10px 30px color-mix(in srgb, rgb(23 23 23 / .07) 92%, var(--accent))!important}\nhtml.light.onex-themed .tg-title,html.light.onex-themed .tg-desc{color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important}\nhtml.light.onex-themed .tg-handle{color:var(--accent2)!important}\n\n/* Dark mode: keep all legacy surfaces/text inside the same readable tokens. */\nhtml.onex-themed:not(.light) .page [style*="background:#fff"],\nhtml.onex-themed:not(.light) .page [style*="background: #fff"],\nhtml.onex-themed:not(.light) .page [style*="background:white"],\nhtml.onex-themed:not(.light) .page [style*="background: white"]{\n  background:var(--theme-surface)!important;\n  color:var(--theme-text)!important;\n}\nhtml.onex-themed:not(.light) .page input,\nhtml.onex-themed:not(.light) .page select,\nhtml.onex-themed:not(.light) .page textarea{color:var(--theme-text)!important}\nhtml.onex-themed:not(.light) .page input::placeholder,\nhtml.onex-themed:not(.light) .page textarea::placeholder{color:var(--theme-subtle)!important}\n\n/* Never let a theme turn the mobile/desktop menu text invisible. */\nhtml.light.onex-themed .nav-item{color:color-mix(in srgb, rgb(83 83 83) 82%, var(--accent))!important}\nhtml.light.onex-themed .nav-item.on{color:#fff!important}\nhtml.light.onex-themed .nav-sec{color:color-mix(in srgb, rgb(114 114 114) 82%, var(--accent))!important}\nhtml.light.onex-themed .sb-foot button,\nhtml.light.onex-themed .sb-foot a.btn{color:color-mix(in srgb, rgb(83 83 83) 82%, var(--accent))!important;background:#f8fafc!important}\nhtml.light.onex-themed .sb-foot a.danger{color:#dc2626!important;background:rgba(239,68,68,.07)!important}\n\n\n/* ============================================================\n   ONEX LIGHT ENVIRONMENT + SHADOW CLEANUP\n   Light mode must never expose the legacy dark canvas or black\n   halos around progress bars/cards. Dark mode remains unchanged.\n   ============================================================ */\nhtml.light.onex-themed body{\n  background:linear-gradient(180deg,#f8fafc 0%,#f3f6fb 52%,#eef2f7 100%)!important;\n  color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important;\n}\nhtml.light.onex-themed body::before{\n  background:\n    radial-gradient(ellipse 80% 50% at 100% 0%,color-mix(in srgb,var(--accent) 8%,transparent),transparent 52%),\n    radial-gradient(ellipse 60% 40% at 0% 100%,color-mix(in srgb,var(--purple) 5%,transparent),transparent 48%)!important;\n  opacity:1!important;\n}\nhtml.light.onex-themed .main,\nhtml.light.onex-themed .page,\nhtml.light.onex-themed .page.on{\n  background:transparent!important;\n}\n/* Legacy inline dark fills used by meters, mini panels and old cards. */\nhtml.light.onex-themed .page [style*="background:rgba(0,0,0"],\nhtml.light.onex-themed .page [style*="background: rgba(0,0,0"],\nhtml.light.onex-themed .page [style*="background:rgba(2,10,24"],\nhtml.light.onex-themed .page [style*="background: rgba(2,10,24"],\nhtml.light.onex-themed .page [style*="background:rgba(3,9,20"],\nhtml.light.onex-themed .page [style*="background: rgba(3,9,20"],\nhtml.light.onex-themed .page [style*="background:rgba(4,13,30"],\nhtml.light.onex-themed .page [style*="background: rgba(4,13,30"]{\n  background:#f8fafc!important;\n  color:color-mix(in srgb, rgb(63 63 63) 88%, var(--accent))!important;\n}\n/* Remove heavy black ambient shadows from light surfaces. */\nhtml.light.onex-themed .page .onex-card,\nhtml.light.onex-themed .page .onex-metric,\nhtml.light.onex-themed .page .version-mini-card,\nhtml.light.onex-themed .page .quick-item,\nhtml.light.onex-themed .page .recent-card,\nhtml.light.onex-themed .page .server-info,\nhtml.light.onex-themed .page .health-list,\nhtml.light.onex-themed .page .traffic-panel,\nhtml.light.onex-themed .page .uptime-panel,\nhtml.light.onex-themed .page .action-card,\nhtml.light.onex-themed .page .group-hero,\nhtml.light.onex-themed .page .group-list-pane,\nhtml.light.onex-themed .page .group-detail-pane,\nhtml.light.onex-themed .page .group-info-card,\nhtml.light.onex-themed .page .group-link-card,\nhtml.light.onex-themed .page .group-proto-card,\nhtml.light.onex-themed .page .group-manage-card,\nhtml.light.onex-themed .page .group-configs-card,\nhtml.light.onex-themed .page .group-card,\nhtml.light.onex-themed .page .cfg-page-hero,\nhtml.light.onex-themed .page .cfg-stat-card,\nhtml.light.onex-themed .page .cfg-list-shell,\nhtml.light.onex-themed .page .cfg-card,\nhtml.light.onex-themed .page .cfg-side,\nhtml.light.onex-themed .page .cfg-menu,\nhtml.light.onex-themed .page .cfg-tools,\nhtml.light.onex-themed .page .admin-card,\nhtml.light.onex-themed .page .admin-create-card,\nhtml.light.onex-themed .page .admin-list-card,\nhtml.light.onex-themed .page .admin-perm-card,\nhtml.light.onex-themed .page .security-health,\nhtml.light.onex-themed .page .security-login-box,\nhtml.light.onex-themed .page .security-log-box{\n  box-shadow:0 8px 24px color-mix(in srgb, rgb(23 23 23 / .055) 92%, var(--accent))!important;\n}\n/* Progress tracks must be neutral light surfaces; fills keep the theme accent. */\nhtml.light.onex-themed .health-track,\nhtml.light.onex-themed .progress-track,\nhtml.light.onex-themed .progress-bar-track,\nhtml.light.onex-themed .meter-track,\nhtml.light.onex-themed .speed-track,\nhtml.light.onex-themed .usage-track{\n  background:#e2e8f0!important;\n  box-shadow:none!important;\n}\nhtml.light.onex-themed .health-fill,\nhtml.light.onex-themed .progress-fill,\nhtml.light.onex-themed .progress-bar-fill,\nhtml.light.onex-themed .meter-fill,\nhtml.light.onex-themed .speed-fill,\nhtml.light.onex-themed .usage-fill{\n  background:linear-gradient(90deg,var(--accent),var(--purple))!important;\n  box-shadow:0 0 8px color-mix(in srgb,var(--accent) 22%,transparent)!important;\n}\n\n/* ---------- ONEX glass / 3D login visual system ---------- */\n:root{\n  --bg:#05060d;--bg2:rgba(13,16,32,.76);--bg3:rgba(22,27,48,.62);\n  --card:rgba(18,22,40,.68);--card-b:rgba(255,255,255,.13);\n  --accent:#38d9ff;--accent2:#8b5cf6;--purple:#b57bff;\n  --t1:#eef2ff;--t2:rgba(238,242,255,.72);--t3:color-mix(in srgb, rgb(172 172 172 / .55) 82%, var(--accent));\n  --input-bg:rgba(0,0,0,.28);--hover:rgba(var(--accent2-rgb),.12);\n  --shadow:0 24px 70px rgba(0,0,0,.42);--glow:0 0 50px rgba(var(--accent2-rgb),.14);--glass:blur(20px);\n}\nhtml,body{background:transparent!important}\nbody{position:relative;isolation:isolate}\nhtml:not(.light) body{background:color-mix(in srgb, rgb(6 6 6) 92%, var(--accent))!important}\nhtml:not(.light) body::before{background:radial-gradient(ellipse 70% 55% at 8% 8%,color-mix(in srgb,var(--accent) 17%,transparent),transparent 62%),radial-gradient(ellipse 65% 55% at 90% 88%,color-mix(in srgb,var(--accent2) 15%,transparent),transparent 64%),linear-gradient(180deg,color-mix(in srgb, rgb(6 6 6) 92%, var(--accent)),color-mix(in srgb, rgb(12 12 12) 92%, var(--accent)))!important;z-index:-2!important}\n#onexDashScene{position:fixed;inset:0;z-index:-1;pointer-events:none;opacity:.92}\n.sidebar,.main,.mob-bar{backdrop-filter:blur(22px) saturate(145%)!important;-webkit-backdrop-filter:blur(22px) saturate(145%)!important}\nhtml:not(.light) .sidebar{background:linear-gradient(180deg,color-mix(in srgb, rgb(17 17 17 / .82) 92%, var(--accent)),color-mix(in srgb, rgb(9 9 9 / .72) 92%, var(--accent)))!important;border-left:1px solid rgba(255,255,255,.14)!important;box-shadow:24px 0 70px rgba(0,0,0,.24),inset 1px 0 rgba(255,255,255,.04)!important}\n.main{background:transparent!important}\nhtml:not(.light) .card,html:not(.light) .panel,html:not(.light) .stat-card,html:not(.light) .dash-card,html:not(.light) .glass-card,html:not(.light) .table-wrap,html:not(.light) .chart-card,html:not(.light) .quick-card,html:not(.light) .activity-card,html:not(.light) .config-card,html:not(.light) .modal,html:not(.light) .settings-card{background:linear-gradient(145deg,color-mix(in srgb, rgb(37 37 37 / .64) 88%, var(--accent)),color-mix(in srgb, rgb(14 14 14 / .72) 92%, var(--accent)))!important;border:1px solid rgba(255,255,255,.13)!important;box-shadow:0 22px 60px -32px rgba(0,0,0,.9),inset 0 1px rgba(255,255,255,.08)!important;backdrop-filter:blur(18px) saturate(140%)!important;-webkit-backdrop-filter:blur(18px) saturate(140%)!important}\nhtml:not(.light) .card:hover,html:not(.light) .panel:hover,html:not(.light) .stat-card:hover,html:not(.light) .dash-card:hover,html:not(.light) .glass-card:hover{border-color:color-mix(in srgb,var(--accent) 42%,rgba(255,255,255,.13))!important;box-shadow:0 25px 65px -30px rgba(0,0,0,.95),0 0 32px color-mix(in srgb,var(--accent) 13%,transparent)!important}\nhtml:not(.light) .btn,html:not(.light) .button,html:not(.light) .primary-btn,html:not(.light) .save-btn,html:not(.light) .create-btn,html:not(.light) .nav-add{background:linear-gradient(135deg,var(--accent),var(--accent2))!important;color:#fff!important;border:0!important;box-shadow:0 12px 30px -12px color-mix(in srgb,var(--accent) 75%,transparent)!important}\n.nav-item.on,.nav-item:hover{background:linear-gradient(135deg,color-mix(in srgb,var(--accent) 22%,transparent),color-mix(in srgb,var(--accent2) 14%,transparent))!important;border-color:color-mix(in srgb,var(--accent) 36%,transparent)!important}\n.sb-toggle{background:linear-gradient(135deg,var(--accent),var(--accent2))!important;box-shadow:0 8px 22px color-mix(in srgb,var(--accent) 35%,transparent)!important}\nhtml:not(.light) input,html:not(.light) select,html:not(.light) textarea{background:rgba(0,0,0,.25)!important;border-color:rgba(255,255,255,.14)!important}\ninput:focus,select:focus,textarea:focus{border-color:var(--accent)!important;box-shadow:0 0 0 3px color-mix(in srgb,var(--accent) 16%,transparent)!important}\nhtml:not(.light) .onex-approved-brand,html:not(.light) .onex-approved-mobile{background:linear-gradient(135deg,rgba(255,255,255,.08),rgba(255,255,255,.02))!important;border-bottom-color:rgba(255,255,255,.12)!important}\n.page-title,.section-title{letter-spacing:-.02em;text-shadow:0 0 24px color-mix(in srgb,var(--accent) 18%,transparent)}\n@media(max-width:760px){.main{background:transparent!important}}\n\n/* ============================================================\n   ONEX GLASS TEMPLATE — unified design system (matches login)\n   Visual layer only: colors, surfaces, borders, radius, type.\n   No layout / behaviour is changed here.\n   ============================================================ */\n:root{\n  --p:#38d9ff;--s:#8b5cf6;--p-rgb:56 217 255;--s-rgb:139 92 246;--on:color-mix(in srgb, rgb(13 13 13) 92%, var(--accent));\n  --g-bg:color-mix(in srgb, rgb(6 6 6) 92%, var(--accent));--g-tx:#eef2ff;--g-mut:color-mix(in srgb, rgb(172 172 172) 82%, var(--accent));--g-dim:color-mix(in srgb, rgb(118 118 118) 82%, var(--accent));\n  --g-line:rgb(255 255 255 / .09);--g-line2:rgb(255 255 255 / .15);\n  --g-surf:linear-gradient(160deg,color-mix(in srgb, rgb(37 37 37 / .62) 88%, var(--accent)),color-mix(in srgb, rgb(15 15 15 / .78) 92%, var(--accent)));\n  --g-surf2:linear-gradient(160deg,rgb(255 255 255 / .05),rgb(255 255 255 / .018));\n  --g-shadow:0 1px 0 rgb(255 255 255 / .09) inset,0 30px 70px -40px rgb(0 0 0 / .95);\n  --g-r:22px;--g-r2:16px;--g-r3:13px;\n  --g-ease:cubic-bezier(.16,1,.3,1);\n  --mono:"JetBrains Mono",ui-monospace,monospace;\n}\n#page-dash{--p:var(--accent);--s:var(--purple);--line:var(--g-line);--text:var(--t1);--muted:var(--t2);--dim:var(--t3)}\nhtml.onex-glass body{font-family:Vazirmatn,system-ui,sans-serif!important;-webkit-font-smoothing:antialiased}\nhtml.onex-glass body.en{font-family:Inter,Vazirmatn,system-ui,sans-serif!important}\nhtml.onex-glass:not(.light) body{background:var(--g-bg)!important;color:var(--g-tx)}\nhtml.onex-glass:not(.light) body::before{background:\n  radial-gradient(ellipse 60% 50% at 6% 4%,rgb(var(--p-rgb) / .16),transparent 62%),\n  radial-gradient(ellipse 55% 50% at 96% 96%,rgb(var(--s-rgb) / .14),transparent 64%),\n  linear-gradient(180deg,color-mix(in srgb, rgb(6 6 6) 92%, var(--accent)),color-mix(in srgb, rgb(11 11 11) 92%, var(--accent)))!important}\nhtml.onex-glass ::selection{background:rgb(var(--p-rgb) / .35);color:#fff}\nhtml.onex-glass *{scrollbar-width:thin;scrollbar-color:rgb(var(--p-rgb) / .35) transparent}\nhtml.onex-glass ::-webkit-scrollbar{width:9px;height:9px}\nhtml.onex-glass ::-webkit-scrollbar-thumb{background:rgb(var(--p-rgb) / .3);border-radius:99px;border:2px solid transparent;background-clip:padding-box}\nhtml.onex-glass ::-webkit-scrollbar-track{background:transparent}\n\n/* ---------- shell: sidebar / top bar / mobile bar ---------- */\nhtml.onex-glass:not(.light) .sidebar{background:linear-gradient(180deg,color-mix(in srgb, rgb(20 20 20 / .82) 92%, var(--accent)),color-mix(in srgb, rgb(9 9 9 / .78) 92%, var(--accent)))!important;border-left-color:var(--g-line2)!important}\nhtml.onex-glass .nav-item{border-radius:14px!important;transition:background .25s var(--g-ease),color .2s,box-shadow .25s,transform .25s var(--g-ease)!important}\nhtml.onex-glass:not(.light) .nav-item:hover{background:rgb(255 255 255 / .05)!important;border-color:var(--g-line2)!important}\nhtml.onex-glass .nav-item.on{background:linear-gradient(135deg,rgb(var(--p-rgb) / .24),rgb(var(--s-rgb) / .14))!important;box-shadow:0 0 0 1px rgb(var(--p-rgb) / .45) inset,0 10px 26px -14px rgb(var(--p-rgb) / .8)!important}\nhtml.onex-glass .nav-item.on .nav-ico{color:var(--accent)!important;filter:drop-shadow(0 0 8px rgb(var(--p-rgb) / .55))}\nhtml.onex-glass .nav-sec{font-family:var(--mono);letter-spacing:.14em!important;opacity:.75}\nhtml.onex-glass .nav-new-badge{background:linear-gradient(135deg,var(--accent),var(--purple))!important;color:var(--on)!important}\nhtml.onex-glass:not(.light) .onex-topbar,html.onex-glass:not(.light) .mob-bar{background:linear-gradient(135deg,rgb(255 255 255 / .07),rgb(255 255 255 / .02))!important;border:1px solid var(--g-line2)!important;box-shadow:0 1px 0 rgb(255 255 255 / .1) inset,0 18px 40px -24px rgb(0 0 0 / .85)!important;backdrop-filter:blur(18px) saturate(160%)!important;-webkit-backdrop-filter:blur(18px) saturate(160%)!important}\nhtml.onex-glass .onex-topbar{border-radius:20px!important}\nhtml.onex-glass .top-dot,html.onex-glass .mob-status i{background:var(--accent)!important;box-shadow:0 0 10px var(--accent)!important}\nhtml.onex-glass .onex-brand-word .x{background:linear-gradient(135deg,var(--accent),var(--purple));-webkit-background-clip:text;background-clip:text;color:transparent!important}\nhtml.onex-glass .hexa-swatches{display:flex;gap:8px;align-items:center;padding:5px 8px;border-radius:99px;background:rgb(0 0 0 / .22);border:1px solid var(--g-line)}\nhtml.onex-glass .hexa-swatches button{width:22px!important;height:22px!important;border-radius:50%!important;border:0!important;cursor:pointer;background:linear-gradient(135deg,var(--sw-p,var(--accent)),var(--sw-s,var(--purple)))!important;transition:transform .25s var(--g-ease),box-shadow .25s!important}\nhtml.onex-glass .hexa-swatches button:hover{transform:scale(1.15)}\nhtml.onex-glass .hexa-swatches button.active{box-shadow:0 0 0 2px var(--g-bg),0 0 0 4px #fff,0 0 14px 2px rgb(var(--p-rgb) / .7)!important}\nhtml.onex-glass .top-setting-group{border-radius:12px!important;overflow:hidden}\nhtml.onex-glass .top-setting-btn{font-family:var(--mono)!important;font-weight:700!important}\nhtml.onex-glass .top-setting-btn.active{background:linear-gradient(135deg,rgb(var(--p-rgb) / .3),rgb(var(--s-rgb) / .2))!important;color:#fff!important;box-shadow:0 0 0 1px rgb(var(--p-rgb) / .55) inset!important}\nhtml.light.onex-glass .top-setting-btn.active{color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important}\nhtml.onex-glass .onex-3d-control{border-radius:18px!important}\nhtml.onex-glass:not(.light) .onex-3d-control{background:var(--g-surf)!important;border:1px solid var(--g-line2)!important;box-shadow:var(--g-shadow)!important}\nhtml.onex-glass .onex-3d-control .control-icon{color:var(--accent)!important;background:rgb(var(--p-rgb) / .12)!important;border:1px solid rgb(var(--p-rgb) / .35)!important}\nhtml.onex-glass .onex-3d-control:hover{border-color:rgb(var(--p-rgb) / .5)!important;transform:translateY(-2px)}\n\n/* ---------- surfaces: tier 1 (page level) ---------- */\nhtml.onex-glass:not(.light) .page .card,\nhtml.onex-glass:not(.light) #page-dash .glass,\nhtml.onex-glass:not(.light) .page .cfg-page-hero,html.onex-glass:not(.light) .page .cfg-stat-card,html.onex-glass:not(.light) .page .cfg-tools,html.onex-glass:not(.light) .page .cfg-list-shell,\nhtml.onex-glass:not(.light) .page .group-hero,html.onex-glass:not(.light) .page .group-list-pane,html.onex-glass:not(.light) .page .group-detail-pane,\nhtml.onex-glass:not(.light) .page .stats-kpi,html.onex-glass:not(.light) .page .stats-panel,html.onex-glass:not(.light) .page .traffic-panel,html.onex-glass:not(.light) .page .uptime-panel,html.onex-glass:not(.light) .page .server-panel,\nhtml.onex-glass:not(.light) .page .admin-card,html.onex-glass:not(.light) .page .admin-create-card,html.onex-glass:not(.light) .page .admin-list-card,html.onex-glass:not(.light) .page .admin-perm-card,\nhtml.onex-glass:not(.light) .page .onex-security-card,html.onex-glass:not(.light) .page .tg-hero,html.onex-glass:not(.light) .page .tg-page-card,html.onex-glass:not(.light) .page .tg-settings-card,\nhtml.onex-glass:not(.light) .page .logs-list-card,html.onex-glass:not(.light) .page .onex-theme-panel,html.onex-glass:not(.light) .page .onex-theme-preview{\n  background:var(--g-surf)!important;border:1px solid var(--g-line2)!important;border-radius:var(--g-r)!important;\n  box-shadow:var(--g-shadow)!important;backdrop-filter:blur(20px) saturate(150%)!important;-webkit-backdrop-filter:blur(20px) saturate(150%)!important;\n  transition:border-color .3s,box-shadow .3s,transform .3s var(--g-ease)}\nhtml.onex-glass:not(.light) .page .card:hover,html.onex-glass:not(.light) #page-dash .glass:hover,\nhtml.onex-glass:not(.light) .page .cfg-stat-card:hover,html.onex-glass:not(.light) .page .stats-kpi:hover{\n  border-color:rgb(var(--p-rgb) / .38)!important;box-shadow:var(--g-shadow),0 0 40px -18px rgb(var(--p-rgb) / .6)!important}\n\n/* ---------- surfaces: tier 2 (nested) ---------- */\nhtml.onex-glass:not(.light) .page .cfg-card,html.onex-glass:not(.light) .page .group-card,html.onex-glass:not(.light) .page .group-info-card,html.onex-glass:not(.light) .page .group-link-card,\nhtml.onex-glass:not(.light) .page .group-proto-card,html.onex-glass:not(.light) .page .group-manage-card,html.onex-glass:not(.light) .page .group-configs-card,html.onex-glass:not(.light) .page .group-stat,\nhtml.onex-glass:not(.light) .page .tg-stat,html.onex-glass:not(.light) .page .tg-user-card,html.onex-glass:not(.light) .page .advanced-section,html.onex-glass:not(.light) .page .advanced-subcard,\nhtml.onex-glass:not(.light) .page .theme-custom-box,html.onex-glass:not(.light) .page .theme-mode-box,html.onex-glass:not(.light) .page .security-login-box,html.onex-glass:not(.light) .page .security-log-box,\nhtml.onex-glass:not(.light) #page-dash .version,html.onex-glass:not(.light) #page-dash .quickitem,html.onex-glass:not(.light) #page-dash .dock button{\n  background:var(--g-surf2)!important;border:1px solid var(--g-line)!important;border-radius:var(--g-r2)!important;box-shadow:0 1px 0 rgb(255 255 255 / .05) inset!important}\nhtml.onex-glass:not(.light) .page .cfg-card:hover,html.onex-glass:not(.light) .page .group-card:hover,html.onex-glass:not(.light) #page-dash .quickitem:hover,html.onex-glass:not(.light) #page-dash .dock button:hover{\n  border-color:rgb(var(--p-rgb) / .4)!important;background:linear-gradient(160deg,rgb(var(--p-rgb) / .08),rgb(255 255 255 / .02))!important}\nhtml.onex-glass .page .group-card.on,html.onex-glass .page .group-card.active,html.onex-glass .page .cfg-card.selected{border-color:rgb(var(--p-rgb) / .6)!important;box-shadow:0 0 0 1px rgb(var(--p-rgb) / .35) inset,0 12px 30px -18px rgb(var(--p-rgb) / .7)!important}\n\n/* ---------- headings & text ---------- */\nhtml.onex-glass .page-title{font-weight:900!important;letter-spacing:-.01em}\nhtml.onex-glass .page-title svg{color:var(--accent)!important;filter:drop-shadow(0 0 10px rgb(var(--p-rgb) / .5))}\nhtml.onex-glass .card-title{font-weight:800!important}\nhtml.onex-glass .page-sub,html.onex-glass .stats-subtitle{color:var(--t2)!important}\nhtml.onex-glass .theme-kicker,html.onex-glass .group-hero-kicker{font-family:var(--mono)!important;color:var(--accent)!important;letter-spacing:.12em}\nhtml.onex-glass .mono,html.onex-glass code,html.onex-glass kbd{font-family:var(--mono)!important}\n\n/* ---------- form controls ---------- */\nhtml.onex-glass:not(.light) .main input:not([type=checkbox]):not([type=radio]):not([type=color]):not([type=range]):not([type=file]),\nhtml.onex-glass:not(.light) .main select,html.onex-glass:not(.light) .main textarea,\nhtml.onex-glass:not(.light) .modal input:not([type=checkbox]):not([type=radio]):not([type=color]),html.onex-glass:not(.light) .modal select,html.onex-glass:not(.light) .modal textarea{\n  background:rgb(0 0 0 / .28)!important;border:1px solid var(--g-line2)!important;border-radius:var(--g-r3)!important;color:var(--g-tx)!important;\n  transition:border-color .2s,box-shadow .25s,background-color .2s!important}\nhtml.onex-glass .main input:not([type=checkbox]):not([type=radio]):not([type=color]):hover,html.onex-glass .main select:hover,html.onex-glass .main textarea:hover{border-color:rgb(255 255 255 / .26)!important}\nhtml.onex-glass .main input:focus,html.onex-glass .main select:focus,html.onex-glass .main textarea:focus,\nhtml.onex-glass .modal input:focus,html.onex-glass .modal select:focus,html.onex-glass .modal textarea:focus{\n  outline:none!important;border-color:var(--accent)!important;box-shadow:0 0 0 4px rgb(var(--p-rgb) / .14)!important}\nhtml.onex-glass:not(.light) .main input:focus,html.onex-glass:not(.light) .main textarea:focus{background:rgb(var(--p-rgb) / .05)!important}\nhtml.onex-glass:not(.light) .main select option{background:color-mix(in srgb, rgb(19 19 19) 92%, var(--accent));color:var(--g-tx)}\nhtml.onex-glass .main input::placeholder,html.onex-glass .main textarea::placeholder{color:var(--g-dim)!important}\nhtml.onex-glass .main input[type=checkbox],html.onex-glass .main input[type=radio]{accent-color:var(--accent)}\nhtml.onex-glass .main input[type=color]{border-radius:10px;border:1px solid var(--g-line2);background:transparent;cursor:pointer}\n/* search boxes that wrap an input: the wrapper is the control */\nhtml.onex-glass:not(.light) .cfg-search-box,html.onex-glass:not(.light) .group-search,html.onex-glass:not(.light) .logs-search-wrap,html.onex-glass:not(.light) .tg-input-wrap{\n  background:rgb(0 0 0 / .28)!important;border:1px solid var(--g-line2)!important;border-radius:var(--g-r3)!important}\nhtml.onex-glass .cfg-search-box:focus-within,html.onex-glass .group-search:focus-within,html.onex-glass .logs-search-wrap:focus-within,html.onex-glass .tg-input-wrap:focus-within{border-color:var(--accent)!important;box-shadow:0 0 0 4px rgb(var(--p-rgb) / .14)!important}\nhtml.onex-glass .main .cfg-search-box input,html.onex-glass .main .group-search input,html.onex-glass .main .logs-search-wrap input,html.onex-glass .main .tg-input-wrap input{\n  background:transparent!important;border:0!important;box-shadow:none!important}\nhtml.onex-glass .field label{font-weight:600}\n\n/* ---------- buttons ---------- */\nhtml.onex-glass:not(.light) .btn:not(.btn-p):not(.btn-primary):not(.btn-d):not(.danger):not(.delete-all-confirm){\n  background:rgb(255 255 255 / .06)!important;color:var(--g-tx)!important;border:1px solid var(--g-line2)!important;box-shadow:0 1px 0 rgb(255 255 255 / .06) inset!important}\nhtml.onex-glass:not(.light) .btn:not(.btn-p):not(.btn-primary):not(.btn-d):not(.danger):not(.delete-all-confirm):hover{border-color:rgb(var(--p-rgb) / .55)!important;color:#fff!important;background:rgb(var(--p-rgb) / .1)!important}\nhtml.onex-glass .btn{border-radius:var(--g-r3)!important;font-weight:700!important;transition:transform .2s var(--g-ease),box-shadow .25s,background .2s,border-color .2s,filter .2s!important}\nhtml.onex-glass .btn:active{transform:scale(.98)}\nhtml.onex-glass .btn.btn-p,html.onex-glass .btn.btn-primary,html.onex-glass #page-dash .primary,html.onex-glass .group-create-btn,html.onex-glass .theme-apply-custom,\nhtml.onex-glass .cfg-primary-btn,html.onex-glass .tg-btn.primary{\n  background:linear-gradient(135deg,var(--accent),var(--purple))!important;color:var(--on)!important;border:0!important;\n  box-shadow:0 14px 30px -14px rgb(var(--p-rgb) / .85)!important;font-weight:800!important;position:relative;overflow:hidden}\nhtml.onex-glass .btn.btn-p:hover,html.onex-glass .btn.btn-primary:hover,html.onex-glass #page-dash .primary:hover,html.onex-glass .group-create-btn:hover,html.onex-glass .theme-apply-custom:hover,\nhtml.onex-glass .cfg-primary-btn:hover,html.onex-glass .tg-btn.primary:hover{transform:translateY(-2px);filter:saturate(1.15);box-shadow:0 20px 40px -16px rgb(var(--p-rgb) / .95)!important}\nhtml.onex-glass .btn.btn-d,html.onex-glass .btn.danger,html.onex-glass .logs-clear-btn{\n  background:rgb(255 80 110 / .1)!important;color:#ff9aab!important;border:1px solid rgb(255 107 129 / .38)!important;box-shadow:none!important}\nhtml.onex-glass .btn.btn-d:hover,html.onex-glass .btn.danger:hover,html.onex-glass .logs-clear-btn:hover{background:rgb(255 80 110 / .18)!important;border-color:rgb(255 107 129 / .7)!important;color:#ffc2cc!important}\nhtml.light.onex-glass .btn.btn-d,html.light.onex-glass .btn.danger{color:#be123c!important}\nhtml.onex-glass:not(.light) .stats-refresh-btn,html.onex-glass:not(.light) .cfg-filter-btn,html.onex-glass:not(.light) .theme-reset-btn,html.onex-glass:not(.light) .logs-advanced-btn,\nhtml.onex-glass:not(.light) .group-copy-btn,html.onex-glass:not(.light) .tg-btn.secondary,html.onex-glass:not(.light) .tg-link-btn,html.onex-glass:not(.light) .top-notify-btn,html.onex-glass:not(.light) .cfg-menu-btn{\n  background:rgb(255 255 255 / .06)!important;border:1px solid var(--g-line2)!important;color:var(--g-tx)!important;border-radius:var(--g-r3)!important}\nhtml.onex-glass .stats-refresh-btn:hover,html.onex-glass .cfg-filter-btn:hover,html.onex-glass .theme-reset-btn:hover,html.onex-glass .logs-advanced-btn:hover,\nhtml.onex-glass .group-copy-btn:hover,html.onex-glass .tg-btn.secondary:hover,html.onex-glass .tg-link-btn:hover,html.onex-glass .top-notify-btn:hover{border-color:rgb(var(--p-rgb) / .55)!important;color:var(--accent)!important}\nhtml.onex-glass .main button:focus-visible,html.onex-glass .main a:focus-visible,html.onex-glass .sidebar button:focus-visible{outline:2px solid var(--accent)!important;outline-offset:2px}\n\n/* segmented controls / tabs / chips */\nhtml.onex-glass .range-tab,html.onex-glass .group-filter,html.onex-glass .cfg-select,html.onex-glass #page-dash .range button,html.onex-glass .theme-mode-buttons button{border-radius:10px!important}\nhtml.onex-glass .range-tab.on,html.onex-glass .group-filter.on,html.onex-glass .cfg-select.on,html.onex-glass #page-dash .range button.active,html.onex-glass .theme-mode-buttons button.on,html.onex-glass .tg-tab.on,html.onex-glass .tg-tab.active{\n  background:linear-gradient(135deg,rgb(var(--p-rgb) / .28),rgb(var(--s-rgb) / .18))!important;color:var(--t1)!important;box-shadow:0 0 0 1px rgb(var(--p-rgb) / .5) inset,0 6px 16px -8px rgb(var(--p-rgb) / .7)!important}\nhtml.onex-glass .theme-preset{border-radius:var(--g-r2)!important}\nhtml.onex-glass .theme-preset.on{box-shadow:0 0 0 1px var(--accent) inset,0 12px 30px -16px rgb(var(--p-rgb) / .8)!important;border-color:var(--accent)!important}\n\n/* ---------- progress / meters ---------- */\nhtml.onex-glass .health-fill,html.onex-glass .progress-fill,html.onex-glass .progress-bar-fill,html.onex-glass .meter-fill,html.onex-glass .usage-fill,html.onex-glass .speed-fill,html.onex-glass #page-dash .track i{\n  background:linear-gradient(90deg,var(--accent),var(--purple))!important;box-shadow:0 0 10px rgb(var(--p-rgb) / .45)!important}\n\n/* ---------- modals / overlays / menus ---------- */\nhtml.onex-glass .modal-bg,html.onex-glass .logs-modal-bg{background:color-mix(in srgb, rgb(4 4 4 / .62) 92%, var(--accent))!important;backdrop-filter:blur(10px) saturate(120%)!important;-webkit-backdrop-filter:blur(10px) saturate(120%)!important}\nhtml.onex-glass:not(.light) .modal,html.onex-glass:not(.light) .delete-all-modal,html.onex-glass:not(.light) .update-prompt-modal,html.onex-glass:not(.light) .logs-detail-modal,\nhtml.onex-glass:not(.light) .cfg-menu,html.onex-glass:not(.light) .top-notify-panel,html.onex-glass:not(.light) .group-card-menu{\n  background:linear-gradient(160deg,rgb(30 36 64 / .94),rgb(10 12 24 / .97))!important;border:1px solid var(--g-line2)!important;border-radius:24px!important;\n  box-shadow:0 1px 0 rgb(255 255 255 / .1) inset,0 50px 100px -30px rgb(0 0 0 / .95),0 0 60px -24px rgb(var(--p-rgb) / .45)!important;\n  backdrop-filter:blur(24px) saturate(150%)!important;-webkit-backdrop-filter:blur(24px) saturate(150%)!important}\nhtml.onex-glass .cfg-menu,html.onex-glass .group-card-menu,html.onex-glass .top-notify-panel{border-radius:18px!important}\nhtml.onex-glass .modal-x,html.onex-glass .delete-all-modal-close{border-radius:11px!important}\nhtml.onex-glass .modal-x:hover,html.onex-glass .delete-all-modal-close:hover{background:rgb(255 255 255 / .08)!important;color:#fff!important}\nhtml.onex-glass:not(.light) .overlay.show{background:color-mix(in srgb, rgb(4 4 4 / .55) 92%, var(--accent))!important;backdrop-filter:blur(6px)}\n\n/* ---------- toast ---------- */\nhtml.onex-glass #toast{background:linear-gradient(135deg,color-mix(in srgb, rgb(37 37 37 / .92) 88%, var(--accent)),color-mix(in srgb, rgb(15 15 15 / .95) 92%, var(--accent)))!important;color:var(--g-tx)!important;border:1px solid rgb(var(--p-rgb) / .45)!important;border-radius:16px!important;\n  box-shadow:0 20px 50px -20px rgb(0 0 0 / .9),0 0 30px -10px rgb(var(--p-rgb) / .55)!important;backdrop-filter:blur(18px)!important;-webkit-backdrop-filter:blur(18px)!important;font-weight:700}\nhtml.light.onex-glass #toast{background:#fff!important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))!important}\n\n/* ---------- tables ---------- */\nhtml.onex-glass .page table{border-collapse:separate;border-spacing:0}\nhtml.onex-glass .page th{font-family:var(--mono);font-size:11px!important;letter-spacing:.06em;color:var(--t3)!important;font-weight:600!important}\nhtml.onex-glass:not(.light) .page td{border-color:var(--g-line)!important}\nhtml.onex-glass:not(.light) .page tbody tr:hover td{background:rgb(var(--p-rgb) / .05)!important}\n\n/* ---------- dashboard home (live) ---------- */\n#page-dash .grid{animation:onexRise .7s var(--g-ease) both}\n@keyframes onexRise{from{opacity:0;transform:translateY(14px)}to{opacity:1;transform:none}}\n#page-dash .hero h1 .home-brand{background:linear-gradient(135deg,var(--accent),var(--purple));-webkit-background-clip:text;background-clip:text;color:transparent;direction:ltr;display:inline-block}\n#page-dash .hero h1 .home-user{font-style:normal;color:var(--t2);font-weight:700}\n#page-dash .version b[data-state="ok"],#page-dash .home-core-pill[data-state="ok"]{color:#48e39d!important}\n#page-dash .version b[data-state="warn"],#page-dash .home-core-pill[data-state="warn"]{color:#ffc857!important}\n#page-dash .version b[data-state="bad"],#page-dash .home-core-pill[data-state="bad"]{color:var(--accent2)!important}\n#page-dash .home-core-pill{font-size:10px;color:var(--t3);padding:4px 9px;border-radius:99px;border:1px solid var(--g-line2);background:rgb(0 0 0 / .18)}\n#page-dash .alert[hidden]{display:none!important}\n#page-dash .alert[data-mode="expired"]{border-color:rgb(255 109 134 / .4);background:rgb(255 109 134 / .08)}\n#page-dash .alert[data-mode="expired"] b{color:#ff8da1}\n#page-dash #heroRefresh{border:1px solid var(--g-line2);background:rgb(255 255 255 / .05)}\n#page-dash #heroRefresh:hover{border-color:rgb(var(--p-rgb) / .55)}\n#page-dash #heroRefresh.spinning{opacity:.75;pointer-events:none}\n#page-dash .icon{background:rgb(var(--p-rgb) / .13)!important;color:var(--accent)!important;border:1px solid rgb(var(--p-rgb) / .3)}\n#page-dash .healthrow .tag{background:rgb(var(--p-rgb) / .1)!important;border-color:rgb(var(--p-rgb) / .28)!important;color:var(--accent)!important}\n#page-dash .metric strong{background:linear-gradient(180deg,var(--t1),color-mix(in srgb,var(--t1) 70%,var(--accent)));-webkit-background-clip:text;background-clip:text;color:transparent}\n#page-dash .metric strong small{-webkit-text-fill-color:var(--t3);font-size:.6em}\n#page-dash .home-legend{display:flex;align-items:center;gap:12px;font-size:11px;color:var(--t2)}\n#page-dash .home-legend span{display:flex;align-items:center;gap:6px}\n#page-dash .home-legend em{font-style:normal}\n#page-dash i.lg-d,#page-dash i.lg-u{width:9px;height:9px;border-radius:3px;display:inline-block;background:var(--accent);box-shadow:0 0 8px var(--accent)}\n#page-dash i.lg-u{background:var(--purple);box-shadow:0 0 8px var(--purple)}\n#page-dash .home-link-btn{border:1px solid var(--g-line2)!important;background:rgb(255 255 255 / .04)!important;border-radius:10px!important;padding:6px 11px!important;font-size:11px!important;color:var(--t2)!important;cursor:pointer}\n#page-dash .home-link-btn:hover{color:var(--accent)!important;border-color:rgb(var(--p-rgb) / .5)!important}\n#page-dash .home-chart-wrap{position:relative}\n#page-dash .home-chart-tip{position:absolute;top:6px;transform:translateX(-50%);display:flex;gap:10px;align-items:center;padding:6px 10px;border-radius:10px;font:500 11px var(--mono);direction:ltr;white-space:nowrap;pointer-events:none;\n  background:color-mix(in srgb, rgb(13 13 13 / .92) 92%, var(--accent));border:1px solid var(--g-line2);box-shadow:0 10px 26px -12px #000;color:var(--g-tx)}\n#page-dash .home-chart-tip[hidden]{display:none}\n#page-dash .home-chart-tip span{display:flex;align-items:center;gap:5px}\n#page-dash .home-row{cursor:pointer;border-radius:12px;transition:background .2s}\n#page-dash .home-row:hover{background:rgb(var(--p-rgb) / .06)}\n#page-dash .home-row b{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#page-dash .home-row .usage i{background:linear-gradient(90deg,var(--accent),var(--purple))!important}\n#page-dash .home-row[data-state="warn"] .home-exp{color:#ffc857}\n#page-dash .home-row[data-state="warn"] .dot{background:#ffc857;box-shadow:0 0 10px #ffc857}\n#page-dash .home-row[data-state="bad"] .home-exp{color:var(--accent2)}\n#page-dash .home-row[data-state="bad"] .dot{background:var(--accent2);box-shadow:0 0 10px var(--accent2)}\n#page-dash .home-row[data-state="off"] .dot{background:var(--t3);box-shadow:none}\n#page-dash .home-row[data-state="off"]{opacity:.65}\n#page-dash .home-empty{padding:26px 8px;text-align:center;color:var(--t3);font-size:12px;display:flex;gap:10px;justify-content:center;align-items:center;flex-wrap:wrap}\n#page-dash .telegram{background:radial-gradient(circle at 50% 0,rgba(var(--accent-rgb),.22),transparent 50%),var(--g-surf)!important}\n#page-dash .telegram a{background:linear-gradient(135deg,var(--accent),var(--accent))!important;box-shadow:0 10px 24px -10px var(--accent);transition:transform .25s var(--g-ease)}\n#page-dash .telegram a:hover{transform:translateY(-2px)}\nhtml.light #page-dash .home-chart-tip{background:#fff;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent));border-color:color-mix(in srgb, rgb(23 23 23 / .12) 92%, var(--accent))}\nhtml.light #page-dash .metric strong{-webkit-text-fill-color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent));background:none}\n@media (max-width:800px){#page-dash .home-legend span{display:none}}\n@media (prefers-reduced-motion:reduce){html.onex-glass *,html.onex-glass *::before,html.onex-glass *::after{animation-duration:.01ms!important;transition-duration:.01ms!important}}\n\n</style>\n<section class="page" id="page-news">\n  <div class="page-head">\n    <div>\n      <div class="page-title"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M21.7 3.3 18.2 20c-.27 1.2-.98 1.5-1.98.93l-5.22-3.85-2.5 2.4c-.28.28-.51.51-1.05.51l.37-5.3 9.67-8.74c.42-.38-.09-.59-.65-.21L5 12.85.2 11.32c-1.05-.33-1.06-1.05.22-1.55L19.02 2.55c.9-.33 1.67.2 1.58.75Z"/></svg><span>تلگرام</span></div>\n    </div>\n    <button class="btn btn-sm" onclick="loadNews(true)"><svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 4v6h-6M1 20v-6h6"/><path d="M3.5 9a9 9 0 0 1 14.1-3.4L23 10"/></svg> بروزرسانی</button>\n  </div>\n  <div class="card tg-page-card" id="newsCard">\n    <div class="tg-head"><div class="tg-head-icon" aria-hidden="true"><div class="cube"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M21.7 3.3 18.2 20c-.27 1.2-.98 1.5-1.98.93l-5.22-3.85-2.5 2.4c-.28.28-.51.51-1.05.51l.37-5.3 9.67-8.74c.42-.38-.09-.59-.65-.21L5 12.85.2 11.32c-1.05-.33-1.06-1.05.22-1.55L19.02 2.55c.9-.33 1.67.2 1.58.75Z"/></svg></div></div><div><div class="card-title" id="newsTitle">تلگرام</div><div style="color:var(--t3);font-size:12px">آخرین اخبار و اطلاع\u200cرسانی\u200cهای پروژه</div></div></div>\n    <div id="newsBody" class="tg-links">\n      <a class="tg-link" href="https://t.me/V2rayTun0" target="_blank" rel="noopener noreferrer"><span class="tg-logo"><span class="back"></span><span class="face"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M21.7 3.3 18.2 20c-.27 1.2-.98 1.5-1.98.93l-5.22-3.85-2.5 2.4c-.28.28-.51.51-1.05.51l.37-5.3 9.67-8.74c.42-.38-.09-.59-.65-.21L5 12.85.2 11.32c-1.05-.33-1.06-1.05.22-1.55L19.02 2.55c.9-.33 1.67.2 1.58.75Z"/></svg></span></span><span class="tg-copy"><b>کانال رسمی ONEX</b><span>@V2rayTun0</span></span><span class="tg-arrow">↗</span></a>\n      <a class="tg-link" href="https://t.me/Mehtif" target="_blank" rel="noopener noreferrer"><span class="tg-logo"><span class="back"></span><span class="face"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M21.7 3.3 18.2 20c-.27 1.2-.98 1.5-1.98.93l-5.22-3.85-2.5 2.4c-.28.28-.51.51-1.05.51l.37-5.3 9.67-8.74c.42-.38-.09-.59-.65-.21L5 12.85.2 11.32c-1.05-.33-1.06-1.05.22-1.55L19.02 2.55c.9-.33 1.67.2 1.58.75Z"/></svg></span></span><span class="tg-copy"><b>توسعه\u200cدهنده · Mehtif</b><span>@Mehtif</span></span><span class="tg-arrow">↗</span></a>\n      <a class="tg-link" href="https://github.com/HajMeTiV2/ONEX" target="_blank" rel="noopener noreferrer"><span class="tg-logo github"><span class="back"></span><span class="face"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M12 .7a11.3 11.3 0 0 0-3.57 22.02c.56.1.77-.24.77-.54v-2.1c-3.14.68-3.8-1.33-3.8-1.33-.5-1.27-1.22-1.61-1.22-1.61-1-.69.08-.67.08-.67 1.1.08 1.68 1.13 1.68 1.13.98 1.68 2.58 1.2 3.2.92.1-.71.39-1.2.7-1.48-2.51-.29-5.15-1.26-5.15-5.6 0-1.24.44-2.25 1.13-3.05-.11-.28-.49-1.44.11-3 0 0 .92-.3 3.02 1.16A10.5 10.5 0 0 1 12 6.2c.93 0 1.86.13 2.73.37 2.1-1.46 3.02-1.16 3.02-1.16.6 1.56.22 2.72.11 3 .7.8 1.13 1.81 1.13 3.05 0 4.35-2.65 5.3-5.17 5.59.4.34.75 1.02.75 2.06v3.05c0 .3.2.65.78.54A11.3 11.3 0 0 0 12 .7Z"/></svg></span></span><span class="tg-copy"><b>گیت\u200cهاب پروژه</b><span>https://github.com/HajMeTiV2/ONEX</span></span><span class="tg-arrow">↗</span></a>\n    </div>\n    <div id="newsMeta" style="margin-top:14px;font-size:11px;color:var(--t3)"></div>\n  </div>\n</section>\n\n<section class="page" id="page-admins">\n  <div class="admin-page">\n    <div class="page-head">\n      <div>\n        <div class="page-title"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M19 8v6M22 11h-6"/></svg><span data-i18n="nav_admins">مدیریت ادمین\u200cها</span></div>\n        <div class="page-sub" data-i18n="admins_sub">مدیریت کاربران مدیریتی و سطح دسترسی آن\u200cها</div>\n      </div>\n      <div class="admin-head-actions"><button class="btn btn-p" onclick="focusAdminCreate()"><span>＋</span><span>ادمین جدید</span></button></div>\n    </div>\n\n    <div class="admin-grid-top">\n      <div class="card admin-card admin-list-card">\n        <div class="admin-list-head">\n          <div class="admin-card-head" style="margin-bottom:0">\n            <div><div class="admin-card-title">لیست ادمین\u200cها</div><div class="admin-card-sub" id="adminsCountText">در حال دریافت...</div></div>\n            <button class="admin-op" type="button" onclick="loadAdmins()" title="بروزرسانی"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 11a8 8 0 1 0 2 5"/><path d="M20 4v7h-7"/></svg></button>\n          </div>\n          <div class="admin-list-controls">\n            <input id="adminSearch" class="field" style="margin:0;padding:9px 11px;border-radius:10px;border:1px solid var(--card-b);background:var(--input-bg);color:var(--t1);font-family:inherit;font-size:11px;outline:none" placeholder="جستجوی ادمین..." oninput="renderAdminList()">\n            <select id="adminStatusFilter" onchange="renderAdminList()" style="width:100%;padding:9px 10px;border-radius:10px;border:1px solid var(--card-b);background:var(--input-bg);color:var(--t1);font-family:inherit;font-size:10px;outline:none"><option value="all">همه وضعیت\u200cها</option><option value="active">فعال</option><option value="blocked">مسدود</option><option value="invalid">نامعتبر</option></select>\n          </div>\n        </div>\n        <div class="admin-table-wrap">\n          <div class="admin-table-head"><div>کاربر</div><div>نقش</div><div>وضعیت</div><div>آخرین ورود</div><div>عملیات</div></div>\n          <div id="adminsList"><div class="admin-empty">در حال دریافت...</div></div>\n        </div>\n      </div>\n\n      <div class="card admin-card admin-create-card" id="adminCreateCard">\n        <div class="admin-create-head"><div><div class="admin-card-title">افزودن ادمین جدید</div><div class="admin-card-sub">همه ادمین\u200cها با همین آدرس پنل وارد می\u200cشوند.</div></div><div class="admin-create-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 5v14M5 12h14"/></svg></div></div>\n        <div class="field"><label>نام کاربری</label><input id="adUser" placeholder="user1" autocomplete="off" style="direction:ltr;text-align:left"></div>\n        <div class="field"><label>عنوان نمایشی</label><input id="adLabel" placeholder="اپراتور فروش"></div>\n        <div class="form-row">\n          <div class="field"><label>رمز عبور</label><input id="adPw" type="password" autocomplete="new-password"></div>\n          <div class="field"><label>تکرار رمز</label><input id="adPw2" type="password" autocomplete="new-password"></div>\n        </div>\n        <div class="form-row">\n          <div class="field"><label>محدودیت حجم</label><input id="adLimit" type="number" value="0" min="0"></div>\n          <div class="field"><label>واحد</label><select id="adUnit"><option>GB</option><option>MB</option></select></div>\n        </div>\n        <div class="field"><label>انقضا (روز) — ۰ یعنی بدون انقضا</label><input id="adDays" type="number" value="0" min="0"></div>\n        <div class="field"><label>نقش اولیه</label><select id="adRole"><option value="admin" selected>Admin — مدیریت روزمره</option><option value="operator">Operator — عملیات ساخت</option><option value="super">Super Admin — همه دسترسی\u200cها</option></select></div>\n        <div style="font-size:9px;color:var(--t3);margin:-3px 0 9px">دسترسی\u200cهای دقیق بعد از ساخت از بخش «دسترسی\u200cهای ادمین» قابل تغییر است.</div>\n        <div class="admin-active-row"><span>وضعیت ادمین</span><label class="switch"><input id="adActive" type="checkbox" checked><span class="slider"></span></label></div>\n        <button class="btn btn-p" style="width:100%;margin-top:10px" onclick="createAdmin()"><span>♙</span> ساخت اکانت ادمین</button>\n      </div>\n    </div>\n\n    <div class="card admin-section-wide admin-perm-card">\n      <div class="admin-perm-head">\n        <div><div class="admin-card-title">دسترسی\u200cهای ادمین <span style="color:var(--accent2)">⚙</span></div><div class="admin-card-sub">ادمین را انتخاب کنید و دسترسی\u200cهای او را جداگانه فعال یا غیرفعال کنید.</div></div>\n        <div class="admin-select-shell"><span class="admin-select-badge">👤</span><select id="adminPermSelect" class="admin-perm-select" onchange="selectAdmin(this.value)"><option value="">انتخاب ادمین</option></select></div>\n      </div>\n      <div id="adminPermEmpty" class="admin-selected-note">ابتدا یک ادمین را از لیست انتخاب کنید.</div>\n      <div id="adminPerms" class="admin-perm-groups"></div>\n      <div class="admin-perm-actions"><button class="btn btn-p" id="saveAdminPermsBtn" onclick="saveAdminPermissions()" disabled>ذخیره دسترسی\u200cها</button><button class="btn" onclick="setAllAdminPerms(true)" id="adminAllBtn" disabled>همه</button><button class="btn" onclick="setAllAdminPerms(false)" id="adminNoneBtn" disabled>هیچ\u200cکدام</button></div>\n    </div>\n\n    <div class="admin-bottom-grid admin-section-wide">\n      <div class="card admin-card">\n        <div class="admin-card-head"><div><div class="admin-card-title">گزارش فعالیت ادمین\u200cها <span style="color:var(--accent2)">◷</span></div><div class="admin-card-sub" id="adminActivitySub">فعالیت\u200cهای ثبت\u200cشده برای ادمین انتخاب\u200cشده</div></div></div>\n        <div id="adminActivity" class="admin-activity-list"><div class="admin-empty">برای مشاهده فعالیت، یک ادمین را انتخاب کنید.</div></div>\n      </div>\n      <div class="card admin-card">\n        <div class="admin-card-head"><div><div class="admin-card-title">جزئیات ادمین <span style="color:var(--accent2)">♙</span></div><div class="admin-card-sub">اطلاعات حساب و وضعیت دسترسی</div></div></div>\n        <div id="adminDetails"><div class="admin-empty">برای مشاهده جزئیات، یک ادمین را انتخاب کنید.</div></div>\n      </div>\n    </div>\n  </div>\n</section>\n\n<section class="page" id="page-telegram" data-build="ONEX-1.3.1-TELEGRAM-CONTROL-CENTER">\n  <div class="tg-dashboard-shell">\n    <div class="tg-hero">\n      <div class="tg-hero-brand"><div class="tg-hero-icon"><svg viewBox="0 0 24 24" fill="currentColor"><path d="M21.5 3.5 18.2 20c-.25 1.17-.9 1.45-1.83.9l-5.05-3.72-2.43 2.34c-.27.27-.5.27-1.02.5l.37-5.23 9.52-8.6c.41-.37-.09-.58-.64-.21L5.35 13.2.43 11.66c-1.07-.33-1.09-1.07.22-1.58L19.9 2.52c.91-.34 1.71.21 1.6.98Z"/></svg></div><div><span>TELEGRAM CONTROL CENTER</span><h1>ربات تلگرام</h1><p>مدیریت کامل ربات، کاربران، کانفیگ\u200cها و اعلان\u200cها</p></div></div>\n      <div class="tg-hero-status" id="tgHeroStatus"><i></i><span>در حال بررسی</span></div><div class="tg-build-badge">ONEX 1.3.1</div>\n    </div>\n    <div class="tg-stats-grid">\n      <div class="tg-stat"><div class="tg-stat-icon">👥</div><div><small>تعداد کاربران</small><b id="tgUserCount">0</b><em id="tgUserDelta">—</em></div></div>\n      <div class="tg-stat"><div class="tg-stat-icon">◉</div><div><small>کاربران فعال امروز</small><b id="tgActiveToday">0</b><em>زنده</em></div></div>\n      <div class="tg-stat"><div class="tg-stat-icon">✉</div><div><small>پیام\u200cهای امروز</small><b id="tgMessagesToday">0</b><em>ارسالی/دریافتی</em></div></div>\n      <div class="tg-stat"><div class="tg-stat-icon">🔗</div><div><small>وضعیت Webhook</small><b id="tgWebhookState">—</b><em id="tgWebhookMeta">—</em></div></div>\n      <div class="tg-stat"><div class="tg-stat-icon">◷</div><div><small>آخرین ارتباط</small><b id="tgLastSeen">—</b><em id="tgMode">—</em></div></div>\n    </div>\n    <div class="tg-tabs" id="tgTabs">\n      <button class="on" data-tg-tab="overview">داشبورد ربات</button><button data-tg-tab="users">کاربران</button><button data-tg-tab="broadcast">ارسال پیام</button><button data-tg-tab="configs">کانفیگ\u200cها</button><button data-tg-tab="settings">تنظیمات</button>\n    </div>\n    <div class="tg-tab-panel on" data-tg-panel="overview">\n      <div class="tg-grid-2">\n        <div class="tg-glass tg-settings-card"><div class="tg-card-head"><div><span>BOT CONNECTION</span><h2>تنظیمات اتصال ربات</h2></div><div class="tg-card-badge" id="tgStatus">—</div></div><div class="tg-field"><label>توکن ربات (BotFather)</label><div class="tg-input-wrap"><input id="tgToken" placeholder="123456:ABC-DEF..." autocomplete="off"><button type="button" onclick="copyText(document.getElementById(\'tgToken\').value)">کپی</button></div></div><div class="tg-field"><label>آیدی عددی ادمین</label><input id="tgAdmin" placeholder="123456789" inputmode="numeric"></div><div class="tg-toggle-row"><div><b>Webhook</b><small>پیشنهادی برای Railway</small></div><label class="switch"><input type="checkbox" id="tgWebhook" checked><span class="slider"></span></label></div><div class="tg-actions"><button class="tg-btn secondary" onclick="testTelegram()">تست اتصال</button><button class="tg-btn primary" onclick="saveTelegram()">ذخیره و فعال\u200cسازی</button></div></div>\n        <div class="tg-glass tg-user-card"><div class="tg-card-head"><div><span>USER MANAGEMENT</span><h2>مدیریت کاربران</h2></div><button class="tg-link-btn" onclick="switchTgTab(\'users\')">مشاهده همه</button></div><div class="tg-search"><span>⌕</span><input id="tgUserSearchMini" oninput="renderTelegramUsers(true)" placeholder="جستجو بر اساس ID یا Username..."></div><div id="tgMiniUsers" class="tg-mini-users"><div class="tg-empty">در حال بارگذاری...</div></div></div>\n      </div>\n      <div class="tg-grid-2">\n        <div class="tg-glass"><div class="tg-card-head"><div><span>BROADCAST</span><h2>ارسال پیام همگانی</h2></div><span class="tg-card-icon">➤</span></div><div class="tg-audience"><button class="on" data-aud="all">همه کاربران</button><button data-aud="active">کاربران فعال</button><button data-aud="configs">دارندگان کانفیگ</button><button data-aud="expired">منقضی\u200cشده</button></div><textarea id="tgBroadcastMini" placeholder="متن پیام خود را بنویسید..."></textarea><div class="tg-broadcast-foot"><span>گیرندگان: <b id="tgAudienceCount">0</b></span><button class="tg-btn primary" onclick="sendTelegramBroadcast()">ارسال پیام</button></div></div>\n        <div class="tg-glass"><div class="tg-card-head"><div><span>ACTIVITY</span><h2>آخرین فعالیت\u200cها</h2></div><span class="tg-card-icon">◷</span></div><div id="tgActivity" class="tg-activity"><div class="tg-empty">در حال بارگذاری...</div></div></div>\n      </div>\n    </div>\n    <div class="tg-tab-panel" data-tg-panel="users"><div class="tg-glass"><div class="tg-card-head"><div><span>USERS</span><h2>کاربران ربات</h2></div><div class="tg-user-tools"><input id="tgUserSearch" oninput="renderTelegramUsers(false)" placeholder="ID / Username"><select id="tgUserFilter" onchange="renderTelegramUsers(false)"><option value="all">همه</option><option value="active">فعال</option><option value="blocked">مسدود</option><option value="configs">دارای کانفیگ</option></select></div></div><div id="tgUsersList" class="tg-users-list"></div></div></div>\n    <div class="tg-tab-panel" data-tg-panel="broadcast"><div class="tg-glass tg-broadcast-large"><div class="tg-card-head"><div><span>MESSAGE CENTER</span><h2>ارسال پیام به کاربران</h2><p>قبل از ارسال، تعداد گیرندگان نمایش داده می\u200cشود.</p></div></div><div class="tg-audience large"><button class="on" data-aud="all">همه کاربران</button><button data-aud="active">کاربران فعال</button><button data-aud="configs">دارندگان کانفیگ</button><button data-aud="expired">منقضی\u200cشده</button></div><textarea id="tgBroadcastText" placeholder="پیام شما..."></textarea><div class="tg-send-preview"><span>تعداد گیرندگان: <b id="tgBroadcastCount">0</b></span><button class="tg-btn primary" onclick="sendTelegramBroadcast(true)">ارسال پیام</button></div><div id="tgBroadcastResult" class="tg-result"></div></div></div>\n    <div class="tg-tab-panel" data-tg-panel="configs"><div class="tg-grid-2"><div class="tg-glass"><div class="tg-card-head"><div><span>CONFIGS</span><h2>کانفیگ\u200cهای متصل به تلگرام</h2></div></div><div id="tgConfigOwners" class="tg-config-owners"></div></div><div class="tg-glass"><div class="tg-card-head"><div><span>NOTIFICATIONS</span><h2>اعلان\u200cهای خودکار</h2></div></div><div class="tg-notify-list"><label><span><b>نزدیک شدن انقضا</b><small>ارسال هشدار قبل از پایان اعتبار</small></span><label class="switch"><input type="checkbox" id="tgNotifyExpiry" checked><span class="slider"></span></label></label><label><span><b>نزدیک شدن حجم</b><small>هشدار هنگام مصرف بالا</small></span><label class="switch"><input type="checkbox" id="tgNotifyUsage" checked><span class="slider"></span></label></label><label><span><b>تغییر وضعیت سرویس</b><small>اعلان فعال/غیرفعال شدن</small></span><label class="switch"><input type="checkbox" id="tgNotifyStatus" checked><span class="slider"></span></label></label></div></div></div></div>\n    <div class="tg-tab-panel" data-tg-panel="settings"><div class="tg-grid-2"><div class="tg-glass"><div class="tg-card-head"><div><span>ACCESS</span><h2>امنیت و دسترسی</h2></div></div><div class="tg-security-row"><span>ادمین\u200cهای مجاز</span><b id="tgAdminCount">0</b></div><div class="tg-security-row"><span>عضویت اجباری</span><b id="tgForceJoinState">خاموش</b></div><div class="tg-security-row"><span>حالت اجرا</span><b id="tgRuntimeMode">—</b></div><button class="tg-btn secondary full" onclick="openTgForceJoin()">مدیریت عضویت اجباری</button></div><div class="tg-glass"><div class="tg-card-head"><div><span>HELP</span><h2>راهنمای سریع</h2></div></div><ol class="tg-help-list"><li>از <b>@BotFather</b> ربات بساز و Token را وارد کن.</li><li>Telegram ID ادمین را در بخش اتصال ثبت کن.</li><li>Webhook را روی Railway فعال نگه دار.</li><li>کاربران پس از ارسال /start در پنل ثبت می\u200cشوند.</li></ol></div></div></div>\n  </div>\n</section>\n\n</main>\n\n<!-- Result modal after create -->\n<div class="modal-bg" id="resultModal">\n  <div class="modal">\n    <div class="modal-title" data-i18n="created_title">کانفیگ ساخته شد</div>\n    <div class="field"><label id="resVlessLabel">VLESS</label><div class="link-box" id="resVless">—</div>\n      <button class="btn btn-p btn-sm" style="width:100%" onclick="copyText(document.getElementById(\'resVless\').textContent)">\n        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>\n        <span data-i18n="copy_vless">کپی VLESS</span>\n      </button>\n    </div>\n    <div class="field" style="margin-top:14px"><label data-i18n="sub_label">سابسکریپشن</label><div class="link-box" id="resSub">—</div>\n      <button class="btn btn-sm" style="width:100%" onclick="copyText(document.getElementById(\'resSub\').textContent)">\n        <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 11a9 9 0 0 1 9 9M4 4a16 16 0 0 1 16 16"/><circle cx="5" cy="19" r="1"/></svg>\n        <span data-i18n="copy_sub">کپی ساب</span>\n      </button>\n    </div>\n    <div class="modal-actions">\n      <button class="btn" onclick="closeResult()">OK</button>\n    </div>\n  </div>\n</div>\n\n<div class="modal-bg update-prompt-bg" id="updatePromptModal" aria-hidden="true">\n  <div class="update-prompt-modal" role="dialog" aria-modal="true" aria-labelledby="updatePromptTitle">\n    <div class="update-prompt-icon">↻</div>\n    <div class="update-prompt-title" id="updatePromptTitle">بروزرسانی جدید در دسترس است</div>\n    <div class="update-prompt-text" id="updatePromptText">نسخه جدید پنل آماده است. آیا می\u200cخواهید پنل را بروزرسانی کنید؟</div>\n    <div class="update-prompt-version" id="updatePromptVersion"></div>\n    <div class="update-prompt-actions">\n      <button type="button" class="btn update-prompt-later" id="updatePromptLater">بعداً</button>\n      <button type="button" class="btn btn-primary update-prompt-confirm" id="updatePromptConfirm">بله، بروزرسانی</button>\n    </div>\n  </div>\n</div>\n\n<div class="modal-bg" id="panelModal">\n  <div class="modal">\n    <div class="modal-title" id="panelModalTitle">...</div>\n    <div id="panelModalBody" style="color:var(--t2);font-size:13px;line-height:1.8"></div>\n    <div class="modal-actions">\n      <button class="btn" onclick="document.getElementById(\'panelModal\').classList.remove(\'open\')">OK</button>\n    </div>\n  </div>\n</div>\n\n<div class="modal-bg" id="deleteAllConfigsModal" aria-hidden="true">\n  <div class="delete-all-modal" role="dialog" aria-modal="true" aria-labelledby="deleteAllConfigsTitle">\n    <button type="button" class="delete-all-modal-close" onclick="closeDeleteAllConfigs()" aria-label="بستن">×</button>\n    <div class="delete-all-modal-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6M10 11v5M14 11v5"/></svg></div>\n    <div class="delete-all-modal-title" id="deleteAllConfigsTitle">حذف همه کانفیگ\u200cها</div>\n    <div class="delete-all-modal-text">آیا مطمئن هستید که می\u200cخواهید تمام کانفیگ\u200cهای ساخته\u200cشده را حذف کنید؟<br><b>این عملیات قابل بازگشت نیست.</b></div>\n    <div class="delete-all-modal-actions">\n      <button type="button" class="btn delete-all-cancel" onclick="closeDeleteAllConfigs()">لغو</button>\n      <button type="button" class="btn delete-all-confirm" id="deleteAllConfigsConfirm" onclick="confirmDeleteAllConfigs()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6M10 11v5M14 11v5"/></svg> حذف همه</button>\n    </div>\n  </div>\n</div>\n<div class="toast" id="toast"></div>\n\n<script src="https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.min.js"></script>\n<script>\nconst I18N={\nfa:{sec_panel:\'پنل\',sec_sys:\'سیستم\',sec_appearance:\'شخصی\u200cسازی\',nav_dash:\'داشبورد\',nav_configs:\'کانفیگ\u200cها\',nav_groups:\'گروه\u200cها\',nav_create:\'ساخت کانفیگ\',nav_stats:\'آمار\',nav_logs:\'لاگ فعالیت\',nav_settings:\'تنظیمات\',nav_support:\'پشتیبانی\',nav_donate:\'حمایت مالی\',nav_news:\'تلگرام\',nav_admins:\'ادمین\u200cها\',refresh_news:\'بروزرسانی اطلاعیه\',admins_sub:\'مدیریت کاربران مدیریتی و سطح دسترسی آن\u200cها\',admin_create:\'ساخت اکانت ادمین\',admin_user:\'نام کاربری\',admin_pw:\'رمز عبور\',admin_pw2:\'تکرار رمز\',admin_perms:\'دسترسی\u200cها\',admin_btn:\'ساخت اکانت\',admin_list:\'لیست ادمین\u200cها\',refresh:\'بروزرسانی\',refresh_stats:\'بروزرسانی آمار\',refresh_panel:\'بروزرسانی پنل\',panel_version:\'نسخه پنل\',current_version:\'ورژن فعلی\',nav_telegram:\'ربات ONEX\',tg_sub:\'توکن ربات و آیدی عددی ادمین · فعال\u200cسازی خودکار و وب\u200cهوک\',tg_config:\'پیکربندی ربات\',tg_token:\'توکن ربات (BotFather)\',tg_admin:\'آیدی عددی ادمین\',tg_webhook:\'فعال\u200cسازی Webhook (پیشنهادی روی Railway)\',tg_activate:\'ذخیره و فعال\u200cسازی ربات\',tg_help:\'راهنما\',tg_h1:\'از @BotFather یک ربات بساز و توکن را کپی کن\',tg_h2:\'آیدی عددی خودت را از @userinfobot بگیر\',tg_h3:\'ذخیره کن — وب\u200cهوک خودکار روی دامنه Railway ست می\u200cشود\',logout:\'خروج\',loading:\'در حال بارگذاری...\',m_conns:\'اتصالات فعال\',m_traffic:\'ترافیک کل\',m_links:\'کانفیگ\u200cها\',m_uptime:\'آپتایم سرور\',quick_create:\'ساخت کانفیگ\',quick_create_desc:\'ساخت دستی با محدودیت ترافیک، سرعت و انقضا\',configs_sub:\'مدیریت لینک\u200cها · VLESS و ساب\',th_name:\'نام\',th_proto:\'پروتکل\',th_status:\'وضعیت\',th_usage:\'مصرف\',th_ops:\'عملیات\',manual_create:\'ساخت دستی\',label_name:\'نام\',label_proto:\'پروتکل\',label_limit:\'محدودیت حجم\',label_unit:\'واحد\',label_days:\'انقضا (روز)\',label_ip:\'محدودیت IP\',label_speed:\'سرعت (Mbps)\',btn_create:\'ساخت\',stats_sub:\'ترافیک و اتصالات · فیلتر زمانی\',r_day:\'روز\',r_week:\'هفته\',r_month:\'ماه\',r_all:\'کل\',panel_info:\'اطلاعات کل پنل\',lang_label:\'زبان\',change_pw:\'تغییر رمز عبور\',pw_cur:\'رمز فعلی\',pw_new:\'رمز جدید\',pw_cf:\'تکرار رمز\',btn_save:\'ذخیره\',github:\'گیت\u200cهاب\',telegram:\'تلگرام\',channel:\'کانال پشتیبان\',theme:\'تم\',theme_dark:\'تم تیره\',theme_light:\'تم روشن\',created_title:\'کانفیگ ساخته شد\',copy_vless:\'کپی VLESS\',copy_sub:\'کپی ساب\',sub_label:\'سابسکریپشن\',theme_page_title:\'تم پنل ONEX\',theme_page_sub:\'رنگ\u200cبندی پنل را انتخاب کنید؛ تغییرات به\u200cصورت زنده اعمال و در مرورگر ذخیره می\u200cشوند.\',theme_live_preview:\'پیش\u200cنمایش زنده\',theme_current:\'تم فعلی\',theme_presets:\'تم\u200cهای آماده\',theme_reset:\'بازنشانی\',theme_custom:\'رنگ\u200cبندی سفارشی\',theme_primary:\'رنگ اصلی\',theme_secondary:\'رنگ ثانویه\',theme_background:\'پس\u200cزمینه\',theme_card:\'کارت\u200cها\',theme_apply:\'اعمال رنگ سفارشی\',theme_mode:\'حالت نمایش\',theme_mode_sub:\'تاریک، روشن یا هماهنگ با سیستم\',theme_dark_mode:\'تاریک\',theme_light_mode:\'روشن\',theme_system_mode:\'خودکار\',theme_saved_note:\'انتخاب شما روی همین مرورگر ذخیره می\u200cشود و بعد از Refresh باقی می\u200cماند.\',created_title:\'کانفیگ ساخته شد\'},\nen:{sec_panel:\'PANEL\',sec_sys:\'SYSTEM\',sec_appearance:\'APPEARANCE\',nav_dash:\'Dashboard\',nav_configs:\'Configs\',nav_groups:\'Groups\',nav_create:\'Create Config\',nav_stats:\'Statistics\',nav_logs:\'Activity Log\',nav_settings:\'Settings\',nav_theme:\'Theme\',nav_theme_new:\'New\',nav_support:\'Support\',nav_donate:\'Donate\',nav_news:\'Telegram\',nav_admins:\'Admins\',refresh_news:\'Refresh news\',admins_sub:\'Manage admin users and their access levels\',admin_create:\'Create admin account\',admin_user:\'Username\',admin_pw:\'Password\',admin_pw2:\'Confirm password\',admin_perms:\'Permissions\',admin_btn:\'Create account\',admin_list:\'Admin list\',refresh:\'Refresh\',refresh_stats:\'Refresh stats\',refresh_panel:\'Update panel\',panel_version:\'Panel version\',current_version:\'Current version\',nav_telegram:\'ONEX Bot\',tg_sub:\'Bot token and numeric admin ID · auto activate and webhook\',tg_config:\'Bot configuration\',tg_token:\'Bot token (BotFather)\',tg_admin:\'Admin numeric ID\',tg_webhook:\'Enable Webhook (recommended on Railway)\',tg_activate:\'Save and activate bot\',tg_help:\'Guide\',tg_h1:\'Create a bot with @BotFather and copy the token\',tg_h2:\'Get your numeric ID from @userinfobot\',tg_h3:\'Save — webhook is set automatically on Railway domain\',logout:\'Logout\',loading:\'Loading...\',m_conns:\'Active connections\',m_traffic:\'Total traffic\',m_links:\'Configs\',m_uptime:\'Server uptime\',quick_create:\'Create Config\',quick_create_desc:\'Manual create with traffic, speed and expiry\',configs_sub:\'Manage links · VLESS and Sub\',th_name:\'Name\',th_proto:\'Protocol\',th_status:\'Status\',th_usage:\'Usage\',th_ops:\'Actions\',manual_create:\'Manual create\',label_name:\'Name\',label_proto:\'Protocol\',label_limit:\'Traffic limit\',label_unit:\'Unit\',label_days:\'Expiry (days)\',label_ip:\'IP limit\',label_speed:\'Speed (Mbps)\',btn_create:\'Create\',stats_sub:\'Traffic and connections · time filter\',r_day:\'Day\',r_week:\'Week\',r_month:\'Month\',r_all:\'All\',panel_info:\'Panel overview\',lang_label:\'Language\',change_pw:\'Change password\',pw_cur:\'Current password\',pw_new:\'New password\',pw_cf:\'Confirm password\',btn_save:\'Save\',github:\'GitHub\',telegram:\'Telegram\',channel:\'Support channel\',theme:\'Theme\',theme_dark:\'Dark theme\',theme_light:\'Light theme\',created_title:\'Config created\',copy_vless:\'Copy VLESS\',copy_sub:\'Copy Sub\',sub_label:\'Subscription\'}\n};\nlet lang=localStorage.getItem(\'px_lang\')||\'fa\';\nlet statRange=\'month\';\nfunction t(k){return (I18N[lang]||I18N.fa)[k]||k}\nfunction setVersionLabels(current,latest){\n  const fallback=(document.getElementById(\'panelVersionValue\')?.textContent||\'v—\').replace(/^v/i,\'\');\n  const c=current||fallback, l=latest||c;\n  const pv=document.getElementById(\'panelVersionValue\');\n  const cv=document.getElementById(\'currentVersionValue\');\n  if(pv)pv.textContent=\'v\'+c;\n  if(cv)cv.textContent=\'v\'+c;\n  const info=document.getElementById(\'panelVersionValue\');\n  if(info){info.title=(l!==c?\'Latest: v\'+l:\'Current: v\'+c);}\n}\nfunction applyLang(){\n  document.getElementById(\'htmlRoot\').lang=lang;\n  document.getElementById(\'htmlRoot\').dir=lang===\'fa\'?\'rtl\':\'ltr\';\n  document.body.classList.toggle(\'en\',lang===\'en\');\n  document.querySelectorAll(\'[data-i18n]\').forEach(el=>{const k=el.getAttribute(\'data-i18n\');if(I18N[lang][k])el.textContent=I18N[lang][k]});\n  const tl=document.getElementById(\'themeLabel\');\n  if(tl) tl.textContent=document.documentElement.classList.contains(\'light\')?t(\'theme_dark\'):t(\'theme_light\');\n  const lf=document.getElementById(\'topLangFa\'),le=document.getElementById(\'topLangEn\');\n  if(lf)lf.classList.toggle(\'active\',lang===\'fa\'); if(le)le.classList.toggle(\'active\',lang===\'en\');\n  const nb=document.getElementById(\'topNotifyBtn\'); if(nb){const label=nb.querySelector(\'.notify-label\');if(label)label.textContent=lang===\'fa\'?\'اعلان\u200cها\':\'Notifications\';}\n  const nt=document.getElementById(\'notifyPanelTitle\'); if(nt)nt.textContent=lang===\'fa\'?\'اعلان\u200cها\':\'Notifications\';\n}\nfunction setLang(l){lang=l;localStorage.setItem(\'px_lang\',l);applyLang();toast(l===\'fa\'?\'زبان فارسی\':\'English\')}\n\nfunction setTheme(mode){\n  if(mode===\'light\') document.documentElement.classList.add(\'light\');\n  else document.documentElement.classList.remove(\'light\');\n  localStorage.setItem(\'px_theme\',mode);\n  try{localStorage.setItem(\'onex_display_mode\',mode)}catch(e){}\n  applyLang();\n}\nfunction toggleTheme(){\n  const isLight=document.documentElement.classList.contains(\'light\');\n  setTheme(isLight?\'dark\':\'light\');\n}\n(function(){const th=localStorage.getItem(\'px_theme\')||\'dark\';setTheme(th)})();\n\n\n/* Dashboard home chart & live data: see "ONEX HOME" module below (real /stats data, no demo values). */\n\nconst sb=document.getElementById(\'sidebar\'),main=document.getElementById(\'main\');\nconst mobMenuBtn=document.getElementById(\'mobMenuBtn\'),overlay=document.getElementById(\'overlay\');\nconst hexaSwatches=document.getElementById(\'hexaSwatches\');\nif(hexaSwatches){\n  hexaSwatches.querySelectorAll(\'button\').forEach(btn=>btn.addEventListener(\'click\',()=>{\n    document.documentElement.style.setProperty(\'--accent\',btn.dataset.p);\n    document.documentElement.style.setProperty(\'--accent2\',btn.dataset.s);\n    document.documentElement.style.setProperty(\'--purple\',btn.dataset.s);\n    hexaSwatches.querySelectorAll(\'button\').forEach(x=>x.classList.toggle(\'active\',x===btn));\n    try{localStorage.setItem(\'onex_hexa_palette\',btn.dataset.p+\'|\'+btn.dataset.s)}catch(e){}\n  }));\n  try{\n    const saved=localStorage.getItem(\'onex_hexa_palette\');\n    if(saved){\n      const [p,s]=saved.split(\'|\'),btn=[...hexaSwatches.querySelectorAll(\'button\')].find(x=>x.dataset.p===p&&x.dataset.s===s);\n      if(btn)btn.click();\n    }\n  }catch(e){}\n}\nfunction closeMobileNav(){ if(sb) sb.classList.remove(\'mobile-open\'); if(overlay) overlay.classList.remove(\'show\'); if(mobMenuBtn) mobMenuBtn.classList.remove(\'menu-open\'); }\nfunction openMobileNav(){ if(sb) sb.classList.add(\'mobile-open\'); if(overlay) overlay.classList.add(\'show\'); if(mobMenuBtn) mobMenuBtn.classList.add(\'menu-open\'); }\nif(mobMenuBtn) mobMenuBtn.onclick=()=>{ if(sb.classList.contains(\'mobile-open\')) closeMobileNav(); else openMobileNav(); };\nif(overlay) overlay.onclick=closeMobileNav;\n\ndocument.getElementById(\'sbToggle\').onclick=()=>{\n  sb.classList.toggle(\'collapsed\');\n  main.classList.toggle(\'expanded\',sb.classList.contains(\'collapsed\'));\n  document.getElementById(\'sbToggle\').setAttribute(\'aria-label\',sb.classList.contains(\'collapsed\')?\'باز کردن منو\':\'جمع کردن منو\');\n  localStorage.setItem(\'sb_c\',sb.classList.contains(\'collapsed\')?\'1\':\'0\');\n};\nfunction syncSidebarCollapse(){const mob=window.matchMedia(\'(max-width:640px)\').matches;const c=!mob&&localStorage.getItem(\'sb_c\')===\'1\';sb.classList.toggle(\'collapsed\',c);main.classList.toggle(\'expanded\',c)}\nsyncSidebarCollapse();try{const __sbMq=window.matchMedia(\'(max-width:640px)\');(__sbMq.addEventListener?__sbMq.addEventListener(\'change\',syncSidebarCollapse):__sbMq.addListener(syncSidebarCollapse))}catch(e){}\ndocument.addEventListener(\'keydown\',e=>{\n  if(e.target.closest(\'input,textarea,select,[contenteditable="true"]\'))return;\n  if(e.key===\'[\'||e.code===\'BracketLeft\'){\n    e.preventDefault();\n    if(window.matchMedia(\'(max-width:640px)\').matches){\n      if(sb.classList.contains(\'mobile-open\')) closeMobileNav(); else openMobileNav();\n    }else document.getElementById(\'sbToggle\').click();\n  }\n});\nfunction goPage(name){\n  closeMobileNav();\n  document.querySelectorAll(\'.nav-item\').forEach(n=>n.classList.toggle(\'on\',n.dataset.page===name));\n  document.querySelectorAll(\'.page\').forEach(p=>p.classList.toggle(\'on\',p.id===\'page-\'+name));\n  window.scrollTo({top:0,behavior:\'smooth\'});\n  if(name===\'logs\')loadLogs();\n  if(name===\'groups\')loadGroups();\n  if(name===\'configs\'||name===\'dash\')refreshAll();\n  if(name===\'stats\'){refreshAll();loadStatsDashboard(false);}\n  if(name===\'theme\'){renderOnexThemePresets();loadOnexTheme();}\n}\ndocument.querySelectorAll(\'.nav-item\').forEach(el=>el.addEventListener(\'click\',()=>goPage(el.dataset.page)));\n\nfunction toast(msg){\n  const el=document.getElementById(\'toast\');\n  el.textContent=msg;el.classList.add(\'show\');\n  clearTimeout(window.__tt);window.__tt=setTimeout(()=>el.classList.remove(\'show\'),2200);\n}\n\nlet __loggingOut=false;\nasync function logoutPanel(){\n  if(__loggingOut)return;\n  __loggingOut=true;\n  const btn=document.getElementById(\'panelLogoutBtn\');\n  if(btn){btn.disabled=true;btn.setAttribute(\'aria-busy\',\'true\');}\n  try{\n    await fetch(\'/api/logout\',{\n      method:\'POST\',\n      cache:\'no-store\',\n      credentials:\'same-origin\',\n      headers:{\'X-Requested-With\':\'XMLHttpRequest\'}\n    });\n  }catch(e){}\n  window.location.replace(\'/login\');\n}\n\nasync function api(url,opts={}){\n  try{\n    const r=await fetch(url,{cache:\'no-store\',credentials:\'same-origin\',...opts});\n    if(r.status===401){location.href=\'/login\';return null}\n    let data=null;try{data=await r.json()}catch{data={ok:false}}\n    if(!r.ok){toast(data.detail||data.error||\'Error\');return null}\n    return data;\n  }catch(e){toast(lang===\'fa\'?\'ارتباط برقرار نشد\':\'Connection failed\');return null}\n}\nfunction fmtB(b){b=Number(b)||0;if(b<1024)return b+\' B\';if(b<1024**2)return (b/1024).toFixed(1)+\' KB\';if(b<1024**3)return (b/1024**2).toFixed(2)+\' MB\';return (b/1024**3).toFixed(2)+\' GB\'}\nfunction esc(s){return String(s||\'\').replace(/&/g,\'&amp;\').replace(/</g,\'&lt;\').replace(/"/g,\'&quot;\').replace(/\'/g,\'&#39;\')}\n\nasync function refreshAll(){\n  if(typeof loadCategories===\'function\') try{await loadCategories()}catch(e){}\n  const links=await api(\'/api/links\');\n  if(!links)return;\n  const arr=Array.isArray(links.links)?links.links:(Array.isArray(links)?links:[]);\n  const setRefresh=(id,v)=>{const el=document.getElementById(id);if(el)el.textContent=v};\n  setRefresh(\'mLinks\',arr.length);\n  let active=0,used=0;\n  arr.forEach(l=>{if(l.active!==false)active++;used+=Number(l.used_bytes||0)});\n  setRefresh(\'mTraffic\',fmtB(used));\n  setRefresh(\'sTraffic\',fmtB(used));\n  setRefresh(\'sActive\',active);\n  setRefresh(\'lastUpd\',(lang===\'fa\'?\'بروزرسانی: \':\'Updated: \')+new Date().toLocaleTimeString(lang===\'fa\'?\'fa-IR\':\'en-US\'));\n  try{\n    const c=await api(\'/api/connections\');\n    const cnt=(c&&c.connections)?c.connections.length:((c&&typeof c.count===\'number\')?c.count:0);\n    setRefresh(\'mConns\',cnt);\n    setRefresh(\'sConns\',cnt);\n  }catch(e){}\n  try{\n    const h=await fetch(\'/health\',{cache:\'no-store\'}).then(r=>r.json());\n    if(h&&h.uptime){\n      const mu0=document.getElementById(\'mUptime\');if(mu0)mu0.textContent=h.uptime;\n      const su=document.getElementById(\'sUptime\');if(su)su.textContent=h.uptime;\n    }\n  }catch(e){}\n    __allLinks=arr;\n  softUpdateLinks(arr);\n  const panelInfo=document.getElementById(\'panelInfo\'); if(panelInfo) panelInfo.innerHTML=lang===\'fa\'\n    ?`کل کانفیگ: <b>${arr.length}</b> · فعال: <b>${active}</b> · مصرف: <b>${fmtB(used)}</b> · بازه: <b>${statRange}</b>`\n    :`Total: <b>${arr.length}</b> · Active: <b>${active}</b> · Usage: <b>${fmtB(used)}</b> · Range: <b>${statRange}</b>`;\n  renderOnexRecent(arr);\n  const hostEl=document.getElementById(\'topHost\'); if(hostEl) hostEl.textContent=location.host||\'ONEX SERVER\';\n  const ipEl=document.getElementById(\'serverIp\'); if(ipEl) ipEl.textContent=location.hostname||\'—\';\n  const chartEl=document.getElementById(\'chartTraffic\'); if(chartEl) chartEl.textContent=fmtB(used);\n  const upEl=document.getElementById(\'topUptime\'); const mu=document.getElementById(\'mUptime\'); if(upEl && mu) upEl.textContent=\'Uptime: \'+mu.textContent;\n}\n\nfunction renderOnexRecent(arr){\n  const el=document.getElementById(\'onexRecentBody\'); if(!el) return;\n  if(!arr.length){el.innerHTML=\'<tr><td colspan=5 style="text-align:center;color:var(--t3);padding:24px">کانفیگی وجود ندارد</td></tr>\';return}\n  el.innerHTML=arr.slice(0,5).map(l=>{\n    const name=esc(l.label||l.name||String(l.uuid||l.id||\'\').slice(0,8));\n    const proto=esc(l.protocol||\'vless-ws\');\n    const usage=fmtB(l.used_bytes);\n    const active=l.active!==false&&!l.expired;\n    const uid=esc(l.uuid||l.id||\'\');\n    return `<tr><td><b>${name}</b></td><td>${proto}</td><td>${usage}</td><td><span class="recent-status"><i></i>${active?\'فعال\':\'متوقف\'}</span></td><td><div class="recent-actions"><button class="mini-action" onclick="copyLinkById(\'${uid}\')">⧉</button><button class="mini-action" onclick="copySubById(\'${uid}\')">↗</button></div></td></tr>`;\n  }).join(\'\');\n}\n\n\nfunction linkBadgeClass(l){\n  const conn=Number(l.connected_ips||0);\n  const used=Number(l.used_bytes||0), lim=Number(l.limit_bytes||0);\n  let usagePct=lim>0?(used/lim)*100:0;\n  let expWarn=false, expDead=false;\n  if(l.expires_at){try{const ms=new Date(l.expires_at)-Date.now();if(ms<=0)expDead=true;else if(ms<3*864e5)expWarn=true}catch(e){}}\n  if(expDead||usagePct>=90) return \'conn-badge red\';\n  if(expWarn||usagePct>=70) return \'conn-badge orange\';\n  if(conn>0) return \'conn-badge green\';\n  return \'conn-badge gray\';\n}\nfunction softUpdateLinks(arr){\n  // FINAL CONFIG CARDS RENDERER — never fall back to the legacy table.\n  window.__linksMap={};\n  (arr||[]).forEach(l=>{window.__linksMap[String(l.uuid||l.id||\'\')]=l});\n  renderConfigCards(getFilteredConfigs());\n}\nfunction patchLinkRow(tr, l){\n  // Legacy table patcher retained for compatibility; cards are the active UI.\n  renderConfigCards(getFilteredConfigs());\n}\nfunction renderLinks(arr){\n  window.__linksMap={};\n  (arr||[]).forEach(l=>window.__linksMap[String(l.uuid||l.id||\'\')]=l);\n  renderConfigCards(getFilteredConfigs());\n}\nfunction getLinkUrl(l){if(!l)return \'\';return l.vless_all||l.vless_full||l.vless||l.vless_link||l.link||\'\'}\nfunction getSubUrl(l){if(!l)return \'\';return l.sub||l.sub_url||l.info||\'\'}\nasync function copyText(text){\n  text=String(text||\'\').trim();\n  if(!text||text===\'—\'){toast(lang===\'fa\'?\'لینکی نیست\':\'Nothing to copy\');return}\n  try{\n    if(navigator.clipboard&&window.isSecureContext) await navigator.clipboard.writeText(text);\n    else{const ta=document.createElement(\'textarea\');ta.value=text;ta.style.cssText=\'position:fixed;left:-9999px\';document.body.appendChild(ta);ta.select();document.execCommand(\'copy\');document.body.removeChild(ta)}\n    toast(lang===\'fa\'?\'کپی شد\':\'Copied\');\n  }catch(e){toast(lang===\'fa\'?\'کپی نشد\':\'Copy failed\')}\n}\nasync function copyLinkById(uid){await copyText(getLinkUrl((window.__linksMap||{})[uid]))}\nasync function copySubById(uid){await copyText(getSubUrl((window.__linksMap||{})[uid]))}\nasync function toggleLink(uid,state){\n  // optimistic UI — رنگ بلافاصله عوض می\u200cشود\n  if(window.__linksMap && window.__linksMap[uid]){\n    window.__linksMap[uid].active = !!state;\n    if(window.__linksMap[uid].expired && state) window.__linksMap[uid].expired = false;\n  }\n  if(typeof __allLinks !== \'undefined\' && Array.isArray(__allLinks)){\n    const item = __allLinks.find(x => (x.uuid||x.id)===uid);\n    if(item) item.active = !!state;\n  }\n  const r=await api(\'/api/links/\'+uid,{method:\'PATCH\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({active:!!state})});\n  if(r===null){\n    // rollback\n    if(window.__linksMap && window.__linksMap[uid]) window.__linksMap[uid].active = !state;\n    refreshAll();\n    return;\n  }\n  toast(state?(lang===\'fa\'?\'فعال شد\':\'Enabled\'):(lang===\'fa\'?\'غیرفعال شد\':\'Disabled\'));\n}\nasync function toggleConfigActive(e,uid,state){\n  e.preventDefault();e.stopPropagation();\n  const next=!!state;\n  if(window.__linksMap&&window.__linksMap[uid])window.__linksMap[uid].active=next;\n  if(typeof __allLinks!==\'undefined\'&&Array.isArray(__allLinks)){const item=__allLinks.find(x=>String(x.uuid||x.id)===String(uid));if(item)item.active=next;}\n  const r=await api(\'/api/links/\'+encodeURIComponent(uid),{method:\'PATCH\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({active:next})});\n  if(r===null){if(window.__linksMap&&window.__linksMap[uid])window.__linksMap[uid].active=!next;const item=__allLinks?.find?.(x=>String(x.uuid||x.id)===String(uid));if(item)item.active=!next;renderConfigCards(getFilteredConfigs());return}\n  toast(next?(lang===\'fa\'?\'کانفیگ فعال شد\':\'Config enabled\'):(lang===\'fa\'?\'کانفیگ غیرفعال شد\':\'Config disabled\'));\n  renderConfigCards(getFilteredConfigs());\n}\nfunction openDeleteAllConfigs(){\n  const modal=document.getElementById(\'deleteAllConfigsModal\');\n  if(!modal)return;\n  modal.classList.add(\'open\');modal.setAttribute(\'aria-hidden\',\'false\');\n  document.body.style.overflow=\'hidden\';\n}\nfunction closeDeleteAllConfigs(){\n  const modal=document.getElementById(\'deleteAllConfigsModal\');\n  if(!modal)return;\n  modal.classList.remove(\'open\');modal.setAttribute(\'aria-hidden\',\'true\');\n  document.body.style.overflow=\'\';\n}\nasync function confirmDeleteAllConfigs(){\n  const btn=document.getElementById(\'deleteAllConfigsConfirm\');\n  if(!btn)return;\n  btn.disabled=true;\n  btn.innerHTML=\'<span class="spin" style="width:15px;height:15px;border-width:2px"></span> در حال حذف...\';\n  const r=await api(\'/api/links/delete-all\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:\'{}\'});\n  btn.disabled=false;\n  btn.innerHTML=\'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 6h18M8 6V4h8v2M19 6l-1 14H6L5 6M10 11v5M14 11v5"/></svg> حذف همه\';\n  if(r){\n    closeDeleteAllConfigs();\n    if(window.__linksMap)window.__linksMap={};\n    if(typeof __allLinks!==\'undefined\'&&Array.isArray(__allLinks))__allLinks.length=0;\n    clearSelection();\n    toast(lang===\'fa\'?`همه کانفیگ\u200cها حذف شدند (${Number(r.deleted||0)})`:`All configs deleted (${Number(r.deleted||0)})`);\n    await refreshAll();\n  }\n}\ndocument.addEventListener(\'keydown\',e=>{if(e.key===\'Escape\')closeDeleteAllConfigs()});\ndocument.getElementById(\'deleteAllConfigsModal\')?.addEventListener(\'click\',e=>{if(e.target.id===\'deleteAllConfigsModal\')closeDeleteAllConfigs()});\nasync function deleteLink(uid){\n  if(!confirm(lang===\'fa\'?\'حذف شود؟\':\'Delete?\'))return;\n  const r=await api(\'/api/links/\'+uid,{method:\'DELETE\'});\n  if(r!==null){toast(lang===\'fa\'?\'حذف شد\':\'Deleted\');refreshAll()}\n}\nfunction showResult(data){\n  if(!data)return;\n  const allCfg=getLinkUrl(data)||\'\';\n  const cfgCount=Number(data.configs_total)||(allCfg?allCfg.split(\'\\n\').filter(Boolean).length:0);\n  document.getElementById(\'resVless\').textContent=allCfg||\'—\';\n  document.getElementById(\'resSub\').textContent=getSubUrl(data)||\'—\';\n  const lbl=document.getElementById(\'resVlessLabel\');\n  if(lbl)lbl.textContent=cfgCount>1?(lang===\'fa\'?`همه کانفیگ\u200cهای ساب (${cfgCount})`:`All configs in this sub (${cfgCount})`):\'VLESS\';\n  const cb=document.querySelector(\'#resultModal [data-i18n="copy_vless"]\');\n  if(cb)cb.textContent=cfgCount>1?(lang===\'fa\'?`کپی همه کانفیگ\u200cها (${cfgCount})`:`Copy all configs (${cfgCount})`):(lang===\'fa\'?\'کپی VLESS\':\'Copy VLESS\');\n  document.getElementById(\'resultModal\').classList.add(\'open\');\n}\nfunction closeResult(){document.getElementById(\'resultModal\').classList.remove(\'open\')}\ndocument.getElementById(\'resultModal\').addEventListener(\'click\',e=>{if(e.target.id===\'resultModal\')closeResult()});\n\nfunction getAdvancedPorts(){\n  return [...document.querySelectorAll(\'#advancedPorts .advanced-port-chip\')].map(x=>Number(x.dataset.port)).filter(Boolean);\n}\nfunction addAdvancedPort(value){\n  const input=document.getElementById(\'advPortInput\');\n  const port=Number(value||input?.value||0);\n  if(!port||port<1||port>65535){toast(lang===\'fa\'?\'پورت باید بین ۱ تا ۶۵۵۳۵ باشد\':\'Port must be between 1 and 65535\');return}\n  const ports=getAdvancedPorts(); if(ports.includes(port)){if(input)input.value=\'\';return}\n  const box=document.getElementById(\'advancedPorts\'); if(!box)return;\n  const chip=document.createElement(\'span\'); chip.className=\'advanced-port-chip\'+(ports.length===0?\' primary\':\'\'); chip.dataset.port=port;\n  chip.innerHTML=`<b>${ports.length===0?\'اصلی · \':\'\'}${port}</b><button type="button" aria-label="remove" onclick="removeAdvancedPort(${port})">×</button>`;\n  box.appendChild(chip); if(input)input.value=\'\';\n}\nfunction removeAdvancedPort(port){const chip=[...document.querySelectorAll(\'#advancedPorts .advanced-port-chip\')].find(x=>Number(x.dataset.port)===Number(port));if(chip)chip.remove();const chips=[...document.querySelectorAll(\'#advancedPorts .advanced-port-chip\')];chips.forEach((x,i)=>{x.classList.toggle(\'primary\',i===0);const b=x.querySelector(\'b\');if(b)b.textContent=(i===0?\'اصلی · \':\'\')+x.dataset.port})}\nfunction toggleAdvancedConfig(force){const panel=document.getElementById(\'advancedConfigPanel\'),card=document.querySelector(\'.advanced-config-card\');if(!panel||!card)return;const open=force===undefined?!card.classList.contains(\'open\'):!!force;card.classList.toggle(\'open\',open);panel.hidden=!open;document.getElementById(\'advancedToggleState\').textContent=open?\'بستن\':\'باز کردن\';if(open)loadAdvancedDraft()}\nfunction advancedFormObject(){\n  const g=id=>document.getElementById(id); const val=id=>(g(id)?.value??\'\').trim(); const num=id=>Number(g(id)?.value)||0; const chk=id=>!!g(id)?.checked;\n  return {tls:{enabled:val(\'advTlsMode\')!==\'none\',mode:val(\'advTlsMode\'),sni:val(\'advSni\'),server_name:val(\'advSni\'),alpn:val(\'advAlpn\'),certificate_path:val(\'advCertPath\'),key_path:val(\'advKeyPath\'),allow_insecure:chk(\'advAllowInsecure\'),min_version:val(\'advTlsMin\'),max_version:val(\'advTlsMax\'),reality:{public_key:val(\'advRealityPk\'),private_key:val(\'advRealitySk\'),short_id:val(\'advRealitySid\'),spider_x:val(\'advRealitySpider\'),fingerprint:val(\'advRealityFp\'),handshake_server:val(\'advRealityHandshake\'),handshake_port:num(\'advRealityHandshakePort\'),max_time_difference:val(\'advRealityMaxDiff\')}},host:{address:val(\'advAddress\'),host:val(\'advHost\'),path:val(\'advPath\'),service_name:val(\'advServiceName\'),authority:val(\'advAuthority\')},fingerprint:{enabled:chk(\'advFpEnabled\'),value:val(\'advFp\'),randomize:chk(\'advFpRandom\')},network:{type:val(\'advNetwork\'),mode:val(\'advNetworkMode\'),path:val(\'advPath\'),service_name:val(\'advServiceName\'),http_version:val(\'advHttpVersion\')},headers:{host:val(\'advHost\'),user_agent:val(\'advUserAgent\'),extra:(g(\'advExtraHeaders\')?.value||\'\').split(/\\r?\\n/).map(x=>x.trim()).filter(Boolean)},routing:{domain_strategy:val(\'advDomainStrategy\'),route:val(\'advRoute\'),proxy_protocol:chk(\'advProxyProtocol\'),sniff:chk(\'advSniff\'),sniff_override:chk(\'advSniffOverride\'),sniff_timeout:val(\'advSniffTimeout\')},transport:{packet_encoding:val(\'advPacketEncoding\'),early_data:num(\'advEarlyData\'),max_early_data:num(\'advEarlyData\'),early_data_header_name:val(\'advEarlyDataHeader\'),padding:chk(\'advPadding\')},listener:{listen:val(\'advListen\')||\'0.0.0.0\',bind_interface:val(\'advBindInterface\'),routing_mark:num(\'advRoutingMark\'),netns:val(\'advNetns\'),reuse_addr:chk(\'advReuseAddr\'),tcp_fast_open:chk(\'advTfo\'),tcp_multi_path:chk(\'advMptcp\'),disable_tcp_keep_alive:chk(\'advDisableKeepAlive\'),tcp_keep_alive:val(\'advTcpKeepAlive\'),tcp_keep_alive_interval:val(\'advTcpKeepAliveInterval\'),udp_fragment:chk(\'advUdpFragment\'),udp_timeout:val(\'advUdpTimeout\')},ports:getAdvancedPorts()};\n}\nfunction fillAdvancedForm(a){\n  a=a||{}; const tls=a.tls||{},host=a.host||{},fp=a.fingerprint||{},net=a.network||{},routing=a.routing||{},transport=a.transport||{},listener=a.listener||{},reality=tls.reality||{},headers=a.headers||{};\n  const set=(id,v)=>{const e=document.getElementById(id);if(e)e.value=v??\'\'}; const check=(id,v)=>{const e=document.getElementById(id);if(e)e.checked=!!v};\n  set(\'advTlsMode\',tls.mode||\'tls\');set(\'advSni\',tls.sni||tls.server_name||\'\');set(\'advAlpn\',tls.alpn||\'\');set(\'advTlsMin\',tls.min_version||\'1.2\');set(\'advTlsMax\',tls.max_version||\'1.3\');set(\'advCertPath\',tls.certificate_path||\'\');set(\'advKeyPath\',tls.key_path||\'\');check(\'advAllowInsecure\',tls.allow_insecure);\n  set(\'advRealityPk\',reality.public_key||\'\');set(\'advRealitySk\',reality.private_key||\'\');set(\'advRealitySid\',reality.short_id||\'\');set(\'advRealitySpider\',reality.spider_x||\'\');set(\'advRealityFp\',reality.fingerprint||\'chrome\');set(\'advRealityHandshake\',reality.handshake_server||\'\');set(\'advRealityHandshakePort\',reality.handshake_port||443);set(\'advRealityMaxDiff\',reality.max_time_difference||\'\');\n  set(\'advAddress\',host.address||\'\');set(\'advHost\',host.host||headers.host||\'\');set(\'advPath\',host.path||net.path||\'\');set(\'advAuthority\',host.authority||\'\');set(\'advUserAgent\',headers.user_agent||\'\');set(\'advServiceName\',host.service_name||net.service_name||\'\');\n  set(\'advFp\',fp.value||\'chrome\');check(\'advFpEnabled\',fp.enabled!==false);check(\'advFpRandom\',fp.randomize);set(\'advNetwork\',net.type||\'ws\');set(\'advNetworkMode\',net.mode||\'\');set(\'advHttpVersion\',net.http_version||\'1.1\');set(\'advPacketEncoding\',transport.packet_encoding||\'\');set(\'advEarlyData\',transport.early_data||0);set(\'advEarlyDataHeader\',transport.early_data_header_name||\'Sec-WebSocket-Protocol\');check(\'advPadding\',transport.padding);set(\'advDomainStrategy\',routing.domain_strategy||\'\');set(\'advRoute\',routing.route||\'\');check(\'advSniff\',routing.sniff);check(\'advSniffOverride\',routing.sniff_override);set(\'advSniffTimeout\',routing.sniff_timeout||\'300ms\');check(\'advProxyProtocol\',routing.proxy_protocol);set(\'advExtraHeaders\',(headers.extra||[]).join(\'\\n\'));\n  set(\'advListen\',listener.listen||\'0.0.0.0\');set(\'advBindInterface\',listener.bind_interface||\'\');set(\'advRoutingMark\',listener.routing_mark||0);set(\'advNetns\',listener.netns||\'\');set(\'advTcpKeepAlive\',listener.tcp_keep_alive||\'5m\');set(\'advTcpKeepAliveInterval\',listener.tcp_keep_alive_interval||\'75s\');set(\'advUdpTimeout\',listener.udp_timeout||\'5m\');check(\'advReuseAddr\',listener.reuse_addr!==false);check(\'advTfo\',listener.tcp_fast_open);check(\'advMptcp\',listener.tcp_multi_path);check(\'advDisableKeepAlive\',listener.disable_tcp_keep_alive);check(\'advUdpFragment\',listener.udp_fragment);\n  document.getElementById(\'advancedPorts\').innerHTML=\'\'; (a.ports&&a.ports.length?a.ports:[443]).forEach(addAdvancedPort); updateRealityVisibility();\n}\nfunction updateRealityVisibility(){const box=document.getElementById(\'advRealityBox\'),mode=document.getElementById(\'advTlsMode\');if(box&&mode)box.style.display=mode.value===\'reality\'?\'block\':\'none\'}\nfunction resetAdvancedConfig(){fillAdvancedForm({ports:[Number(document.getElementById(\'cPort\')?.value)||443]});localStorage.removeItem(\'onex_advanced_draft\');toast(lang===\'fa\'?\'تنظیمات پیشرفته بازنشانی شد\':\'Advanced settings reset\')}\nfunction saveAdvancedDraft(){try{localStorage.setItem(\'onex_advanced_draft\',JSON.stringify(advancedFormObject()));toast(lang===\'fa\'?\'تنظیمات ذخیره شد\':\'Settings saved\')}catch(e){toast(lang===\'fa\'?\'ذخیره انجام نشد\':\'Save failed\')}}\nfunction loadAdvancedDraft(){try{const raw=localStorage.getItem(\'onex_advanced_draft\');if(raw)fillAdvancedForm(JSON.parse(raw));else if(!getAdvancedPorts().length)fillAdvancedForm({ports:[Number(document.getElementById(\'cPort\')?.value)||443]})}catch(e){fillAdvancedForm({ports:[443]})}}\nfunction copyAdvancedJson(){copyText(JSON.stringify(advancedFormObject(),null,2))}\nlet __advancedPreview = null;\nfunction setAdvancedValidation(kind, html){const el=document.getElementById(\'advancedValidationStatus\');if(!el)return;el.className=\'advanced-validation-status \'+kind;el.innerHTML=html;}\nasync function validateAdvancedConfig(showPreview=false){const protocol=document.getElementById(\'cProto\')?.value||\'\';const advanced=advancedFormObject();setAdvancedValidation(\'warn\',lang===\'fa\'?\'در حال اعتبارسنجی...\':\'Validating...\');const r=await api(\'/api/advanced/validate\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({protocol,advanced})});if(!r){setAdvancedValidation(\'err\',lang===\'fa\'?\'اعتبارسنجی انجام نشد\':\'Validation failed\');return false}const parts=[];if(r.ok)parts.push(\'✓ \'+(lang===\'fa\'?\'تنظیمات معتبر است\':\'Configuration is valid\'));if(r.native)parts.push(\'• \'+(lang===\'fa\'?\'Native sing-box فعال است\':\'Native sing-box is available\'));(r.warnings||[]).forEach(x=>parts.push(\'⚠ \'+esc(x)));(r.errors||[]).forEach(x=>parts.push(\'✕ \'+esc(x)));setAdvancedValidation(r.ok?(r.warnings?.length?\'warn\':\'ok\'):\'err\',parts.join(\'<br>\'));__advancedPreview=r.preview||null;const box=document.getElementById(\'advancedPreviewBox\'),pre=document.getElementById(\'advancedPreviewCode\');if(box&&pre){box.hidden=!showPreview||!__advancedPreview;if(__advancedPreview)pre.textContent=JSON.stringify(__advancedPreview,null,2)}await loadAdvancedCapabilities(protocol);return !!r.ok}\nasync function loadAdvancedCapabilities(protocol){const r=await api(\'/api/advanced/capabilities?protocol=\'+encodeURIComponent(protocol||\'\'));const el=document.getElementById(\'advancedCapabilityStatus\');if(!el||!r)return;const labels={\'vless-ws\':\'ONEX WB\',\'xhttp-packet-up\':\'ONEX Xhttp\',\'xhttp-stream-up\':\'ONEX Gaming\',\'xhttp-stream-one\':\'ONEX Stream\',\'trojan\':\'Trojan\'};el.innerHTML=Object.entries(r.supported||{}).map(([k,v])=>(v?\'✓ \':\'✕ \')+(labels[k]||k)+(v?\' · پشتیبانی\':\' · اعمال نمی\u200cشود\')).join(\' &nbsp; | &nbsp; \');el.className=\'advanced-validation-status \'+(r.native?\'ok\':\'warn\');el.style.display=\'block\'}\nfunction copyAdvancedPreview(){if(__advancedPreview)copyText(JSON.stringify(__advancedPreview,null,2));}\nlet __configEditUid=\'\';let __configEditOriginalExpiresAt=null;let __configEditOriginalDays=0;\nfunction configEditValue(id){return document.getElementById(id)?.value??\'\'}\nfunction setConfigEditValue(id,value){const el=document.getElementById(id);if(el)el.value=value??\'\'}\nfunction setConfigEditMode(on,link){\n  __configEditUid=on?(String(link?.uuid||link?.id||\'\')):\'\';if(!on){__configEditOriginalExpiresAt=null;__configEditOriginalDays=0;}\n  const title=document.getElementById(\'createPageTitle\'),sub=document.getElementById(\'createPageSubtitle\'),btn=document.getElementById(\'manualConfigSubmit\'),cancel=document.getElementById(\'cancelConfigEditBtn\'),banner=document.getElementById(\'configEditBanner\'),bannerName=document.getElementById(\'configEditBannerName\'),icon=document.getElementById(\'createPageTitleIcon\');\n  if(title)title.textContent=on?\'ویرایش کانفیگ\':\'ساخت کانفیگ\';\n  if(sub)sub.textContent=on?\'تنظیمات کانفیگ را تغییر دهید و در پایان ذخیره کنید\':\'ایجاد کانفیگ جدید با تنظیمات پایه و پیشرفته\';\n  if(btn){btn.innerHTML=on?\'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><path d="M5 12h14M13 6l6 6-6 6"/></svg><span>ذخیره تغییرات</span>\':\'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="16" height="16"><path d="M12 5v14M5 12h14"/></svg><span>ساخت</span>\';btn.setAttribute(\'onclick\',on?\'doManualCreate()\':\'doManualCreate()\')}\n  if(cancel)cancel.hidden=!on;\n  if(banner){banner.hidden=!on;if(on&&bannerName)bannerName.textContent=link?.label||link?.uuid||\'—\'}\n  if(icon)icon.innerHTML=on?\'<path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L8 18l-4 1 1-4Z"/>\':\'<path d="M12 5v14M5 12h14"/>\';\n}\nfunction cancelConfigEdit(){setConfigEditMode(false);goPage(\'configs\')}\nasync function openConfigEditor(e,uid){\n  e?.preventDefault();e?.stopPropagation();\n  const link=(window.__linksMap||{})[uid]||__allLinks.find(x=>String(x.uuid||x.id||\'\')===String(uid));\n  if(!link){toast(lang===\'fa\'?\'کانفیگ پیدا نشد\':\'Config not found\');return}\n  closeConfigMenus();\n  goPage(\'create\');\n  await Promise.all([loadProtocols(),loadCategories(),loadGroups()]);\n  setConfigEditMode(true,link);\n  setConfigEditValue(\'cName\',link.label||\'\');\n  const proto=document.getElementById(\'cProto\');if(proto){proto.value=link.protocol||\'vless-ws\';syncProtocolPicker(\'cProto\')}\n  setConfigEditValue(\'cSubGroup\',link.sub_id||\'\');\n  const limitBytes=Number(link.limit_bytes||0);let limitUnit=\'GB\',limitValue=0;\n  if(limitBytes){if(limitBytes%(1024**3)===0){limitUnit=\'GB\';limitValue=limitBytes/(1024**3)}else if(limitBytes%(1024**2)===0){limitUnit=\'MB\';limitValue=limitBytes/(1024**2)}else{limitUnit=\'KB\';limitValue=limitBytes/1024}}\n  setConfigEditValue(\'cLimit\',limitValue);setConfigEditValue(\'cUnit\',limitUnit);\n  let days=0;if(link.expires_at){const ms=new Date(link.expires_at).getTime()-Date.now();days=Math.max(0,Math.ceil(ms/86400000))}\n  __configEditOriginalExpiresAt=link.expires_at||null;__configEditOriginalDays=days;setConfigEditValue(\'cDays\',days);\n  setConfigEditValue(\'cIp\',link.ip_limit||0);\n  const speedBytes=Number(link.speed_limit_bytes||0);setConfigEditValue(\'cSpeed\',speedBytes?Math.round((speedBytes*8/(1024*1024))*100)/100:0);\n  const all=document.getElementById(\'cAllProtocols\');if(all)all.checked=!!link.all_protocols;\n  const bundleSelected=new Set(Array.isArray(link.bundle_protocols)?link.bundle_protocols:[link.protocol]);\n  document.querySelectorAll(\'#protocolBundleOptions input\').forEach(x=>{x.checked=bundleSelected.has(x.value)});\n  const adb=document.getElementById(\'cAdBlockEnabled\');if(adb)adb.checked=!!link.ad_block_enabled;\n  fillAdvancedForm(link.advanced||{ports:[Number(link.port)||443]});\n  document.getElementById(\'advancedValidationStatus\')?.replaceChildren();\n  document.getElementById(\'advancedPreviewBox\')?.setAttribute(\'hidden\',\'\');\n  window.scrollTo({top:0,behavior:\'smooth\'});\n  toast(lang===\'fa\'?\'فرم ویرایش آماده شد\':\'Edit form loaded\');\n}\nfunction collectConfigFormBody(){\n  const advanced=advancedFormObject(),ports=advanced.ports.length?advanced.ports:[Number(configEditValue(\'cPort\'))||443];\n  const bundle=[...document.querySelectorAll(\'#protocolBundleOptions input:checked\')].map(x=>x.value);\n  return {label:configEditValue(\'cName\').trim()||undefined,protocol:configEditValue(\'cProto\')||undefined,bundle_protocols:bundle,ad_block_enabled:!!document.getElementById(\'cAdBlockEnabled\')?.checked,category_id:\'0\',sub_id:configEditValue(\'cSubGroup\')||undefined,limit_value:Number(configEditValue(\'cLimit\'))||0,limit_unit:configEditValue(\'cUnit\')||\'GB\',expires_days:Number(configEditValue(\'cDays\'))||0,ip_limit:Number(configEditValue(\'cIp\'))||0,speed_limit_value:Number(configEditValue(\'cSpeed\'))||0,speed_limit_unit:\'MBIT\',all_protocols:!!document.getElementById(\'cAllProtocols\')?.checked,port:ports[0],fingerprint:advanced.fingerprint.value,alpn:advanced.tls.alpn,advanced};\n}\nasync function saveEditedConfig(){\n  const uid=__configEditUid;if(!uid)return false;\n  const valid=await validateAdvancedConfig(false);if(!valid)return false;\n  const body=collectConfigFormBody();\n  const currentDays=Number(configEditValue(\'cDays\'))||0;\n  if(currentDays===__configEditOriginalDays) body.expires_at=__configEditOriginalExpiresAt;\n  else body.expires_days=currentDays;\n  const r=await api(\'/api/links/\'+encodeURIComponent(uid),{method:\'PATCH\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(body)});\n  if(!r)return false;\n  toast(lang===\'fa\'?\'تغییرات ذخیره شد\':\'Changes saved\');\n  setConfigEditMode(false);\n  await refreshAll();\n  goPage(\'configs\');\n  return true;\n}\nasync function doManualCreate(){\n  if(__configEditUid)return saveEditedConfig();\n  const valid=await validateAdvancedConfig(false); if(!valid)return;\n  const body=collectConfigFormBody();\n  const r=await api(\'/api/links\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(body)}); if(r){showResult(r);refreshAll();saveAdvancedDraft()}\n}\ndocument.addEventListener(\'change\',e=>{if(e.target?.id===\'advTlsMode\')updateRealityVisibility();if(e.target?.id===\'cProto\')loadAdvancedCapabilities(e.target.value)});\n\nasync function doChangePw(){\n  const user=document.getElementById(\'newUser\').value.trim(),cur=document.getElementById(\'pwCur\').value,nw=document.getElementById(\'pwNew\').value,cf=document.getElementById(\'pwCf\').value;\n  if(nw!==cf){toast(lang===\'fa\'?\'رمزها یکی نیستند\':\'Passwords mismatch\');return}\n  const r=await api(\'/api/change-password\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({new_username:user,current_password:cur,new_password:nw,repeat_password:cf})});\n  if(r){toast(lang===\'fa\'?\'اطلاعات ورود تغییر کرد\':\'Credentials changed\');document.getElementById(\'pwCur\').value=\'\';document.getElementById(\'pwNew\').value=\'\';document.getElementById(\'pwCf\').value=\'\';}\n}\nlet __activityLogs=[];\nlet __logFilter=\'all\';\nlet __logAdvancedOpen=false;\n\nfunction logKindMeta(kind){\n  const map={\n    link:{label:\'لینک\u200cها\',icon:\'↗\'},\n    sub:{label:\'گروه\u200cها\',icon:\'👥\'},\n    auth:{label:\'ورود\',icon:\'⇥\'},\n    admin:{label:\'کاربران\',icon:\'●\'},\n    system:{label:\'سیستم\',icon:\'⚙\'},\n    backup:{label:\'پشتیبان\',icon:\'◫\'},\n    telegram:{label:\'تلگرام\',icon:\'➤\'}\n  };\n  return map[String(kind||\'\').toLowerCase()]||{label:\'رویداد\',icon:\'•\'};\n}\nfunction logLevelMeta(level){\n  const l=String(level||\'info\').toLowerCase();\n  if(l===\'ok\')return {label:\'موفق\',cls:\'ok\'};\n  if(l===\'warn\')return {label:\'هشدار\',cls:\'warn\'};\n  if(l===\'err\')return {label:\'خطا\',cls:\'err\'};\n  return {label:\'اطلاعات\',cls:\'info\'};\n}\nfunction logDateTime(log){\n  const raw=String(log?.time||log?.ts||\'\');\n  const d=raw?new Date(raw):null;\n  return {raw,d,valid:d&&!Number.isNaN(d.getTime())};\n}\nfunction logTime(log){\n  const x=logDateTime(log);\n  if(!x.valid)return \'—\';\n  return x.d.toLocaleTimeString(\'fa-IR\',{hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\',hour12:false});\n}\nfunction logDate(log){\n  const x=logDateTime(log);\n  if(!x.valid)return \'\';\n  return `${x.d.getFullYear()}-${String(x.d.getMonth()+1).padStart(2,\'0\')}-${String(x.d.getDate()).padStart(2,\'0\')}`;\n}\nfunction logTitle(message){\n  const m=String(message||\'—\');\n  const rules=[\n    [/ساخته شد|ساخت/i,\'ساخته شد\'],[/حذف شد|حذف/i,\'حذف شد\'],[/ویرایش شد|تغییر/i,\'تغییر انجام شد\'],[/فعال شد|فعال/i,\'فعال شد\'],[/غیرفعال شد|غیرفعال/i,\'غیرفعال شد\'],[/ورود موفق|login/i,\'کاربر وارد شد\'],[/مسدود شد|مسدودی/i,\'مسدودی امنیتی\'],[/راه\\s*اندازی شد|راه\u200cاندازی شد|شروع شد/i,\'سیستم راه\u200cاندازی شد\'],[/بروزرسانی/i,\'بروزرسانی شد\'],[/ریست شد/i,\'مصرف ریست شد\'],[/پیام همگانی/i,\'پیام همگانی ارسال شد\']\n  ];\n  for(const [re,title] of rules)if(re.test(m))return title;\n  return m.length>72?m.slice(0,72)+\'…\':m;\n}\nfunction logDesc(message,title){\n  const m=String(message||\'—\');\n  if(m===title)return \'\';\n  return m;\n}\nfunction logIsToday(log){\n  const x=logDateTime(log);if(!x.valid)return false;\n  const n=new Date();return x.d.getFullYear()===n.getFullYear()&&x.d.getMonth()===n.getMonth()&&x.d.getDate()===n.getDate();\n}\nfunction logMatchesFilter(log){\n  if(__logFilter!==\'all\'&&String(log.kind||\'\').toLowerCase()!==__logFilter)return false;\n  const q=(document.getElementById(\'logsSearch\')?.value||\'\').trim().toLowerCase();\n  if(q){const hay=[log.message,log.kind,log.level,log.time].map(v=>String(v||\'\')).join(\' \').toLowerCase();if(!hay.includes(q))return false}\n  const level=document.getElementById(\'logsLevelFilter\')?.value||\'all\';\n  if(level!==\'all\'&&String(log.level||\'info\').toLowerCase()!==level)return false;\n  const date=document.getElementById(\'logsDateFilter\')?.value||\'\';\n  if(date&&logDate(log)!==date)return false;\n  return true;\n}\nfunction renderLogStats(logs){\n  const today=logs.filter(logIsToday);\n  const set=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=String(v)};\n  set(\'logStatTotal\',today.length);\n  set(\'logStatOk\',today.filter(l=>String(l.level||\'info\')===\'ok\').length);\n  set(\'logStatChange\',today.filter(l=>/ویرایش|تغییر|فعال|غیرفعال|بروزرسانی|ریست شد/.test(String(l.message||\'\'))).length);\n  set(\'logStatDelete\',today.filter(l=>/حذف شد|حذف گروهی|حذف همه/.test(String(l.message||\'\'))).length);\n  set(\'logCountAll\',logs.length);\n}\nfunction renderLogs(){\n  const box=document.getElementById(\'logsBox\');if(!box)return;\n  const filtered=__activityLogs.filter(logMatchesFilter);\n  const ordered=filtered.slice().reverse();\n  const result=document.getElementById(\'logsResultText\');\n  if(result)result.textContent=`${filtered.length.toLocaleString(\'fa-IR\')} رویداد نمایش داده می\u200cشود · ${__activityLogs.length.toLocaleString(\'fa-IR\')} رویداد ثبت\u200cشده`;\n  if(!ordered.length){box.innerHTML=\'<div class="logs-empty">با فیلترهای فعلی لاگی پیدا نشد.</div>\';return}\n  box.innerHTML=ordered.map((l,i)=>{\n    const meta=logKindMeta(l.kind),lv=logLevelMeta(l.level),title=logTitle(l.message),desc=logDesc(l.message,title);\n    const tm=logTime(l),idx=__activityLogs.indexOf(l);\n    return `<div class="logs-event ${esc(lv.cls)} ${esc(String(l.kind||\'\'))}" onclick="openLogDetail(${idx})" role="button" tabindex="0" onkeydown="if(event.key===\'Enter\'||event.key===\' \')openLogDetail(${idx})">\n      <span class="logs-event-icon">${meta.icon}</span>\n      <span class="logs-event-main"><span class="logs-event-title">${esc(title)}</span><span class="logs-event-desc">${esc(desc||meta.label)}</span></span>\n      <span class="logs-badge">${esc(lv.label)}</span>\n      <span class="logs-event-time">${esc(tm)}</span>\n    </div>`;\n  }).join(\'\');\n}\nfunction applyLogFilters(){renderLogStats(__activityLogs);renderLogs()}\nfunction setLogFilter(filter,button){__logFilter=filter;document.querySelectorAll(\'#logsFilterRow .logs-filter\').forEach(x=>x.classList.toggle(\'on\',x===button));renderLogs()}\nfunction clearLogSearch(){const e=document.getElementById(\'logsSearch\');if(e){e.value=\'\';e.focus()}renderLogs()}\nfunction resetLogFilters(){\n  __logFilter=\'all\';\n  const s=document.getElementById(\'logsSearch\');if(s)s.value=\'\';\n  const l=document.getElementById(\'logsLevelFilter\');if(l)l.value=\'all\';\n  const d=document.getElementById(\'logsDateFilter\');if(d)d.value=\'\';\n  document.querySelectorAll(\'#logsFilterRow .logs-filter\').forEach(x=>x.classList.toggle(\'on\',x.dataset.logFilter===\'all\'));\n  renderLogs();\n}\nfunction toggleLogAdvanced(){\n  __logAdvancedOpen=!__logAdvancedOpen;\n  const row=document.querySelector(\'#page-logs .logs-advanced-row\'),btn=document.querySelector(\'#page-logs .logs-advanced-btn\');\n  if(row)row.classList.toggle(\'open\',__logAdvancedOpen);\n  if(btn)btn.classList.toggle(\'open\',__logAdvancedOpen);\n}\nfunction openLogDetail(index){\n  const l=__activityLogs[index];if(!l)return;\n  const bg=document.getElementById(\'logsDetailBg\'),body=document.getElementById(\'logsDetailBody\');if(!bg||!body)return;\n  const meta=logKindMeta(l.kind),lv=logLevelMeta(l.level);\n  const rows=[\n    [\'رویداد\',String(l.message||\'—\')],\n    [\'دسته\u200cبندی\',meta.label],\n    [\'سطح\',lv.label],\n    [\'زمان\',String(l.time||\'—\').replace(\'T\',\' \').slice(0,19)]\n  ];\n  body.innerHTML=rows.map(r=>`<div class="logs-detail-row"><small>${esc(r[0])}</small><b>${esc(r[1])}</b></div>`).join(\'\');\n  const title=document.getElementById(\'logsDetailTitle\'),m=document.getElementById(\'logsDetailMeta\'),icon=document.getElementById(\'logsDetailIcon\');\n  if(title)title.textContent=logTitle(l.message);\n  if(m)m.textContent=`${meta.label} · ${lv.label}`;\n  if(icon)icon.textContent=meta.icon;\n  bg.classList.add(\'open\');document.body.style.overflow=\'hidden\';\n}\nfunction closeLogDetail(){const bg=document.getElementById(\'logsDetailBg\');if(bg)bg.classList.remove(\'open\');document.body.style.overflow=\'\'}\nasync function clearAllLogs(){\n  if(!__activityLogs.length){toast(lang===\'fa\'?\'لاگی برای پاکسازی وجود ندارد\':\'There are no logs to clear\');return}\n  const ok=window.confirm(lang===\'fa\'?\'تمام لاگ\u200cهای فعالیت حذف شوند؟ این عملیات قابل بازگشت نیست.\':\'Clear all activity logs? This cannot be undone.\');\n  if(!ok)return;\n  const r=await api(\'/api/activity\',{method:\'DELETE\'});\n  if(r){__activityLogs=[];renderLogStats([]);renderLogs();toast(lang===\'fa\'?\'لاگ\u200cها پاک شدند\':\'Activity logs cleared\')}\n}\nasync function loadLogs(showToast=false){\n  const box=document.getElementById(\'logsBox\');\n  if(box&&!__activityLogs.length)box.innerHTML=\'<div class="logs-empty">در حال بارگذاری...</div>\';\n  const data=await api(\'/api/activity\');\n  if(!data)return;\n  __activityLogs=Array.isArray(data)?data:(data&&data.logs)||[];\n  renderLogStats(__activityLogs);renderLogs();\n  if(showToast)toast(lang===\'fa\'?\'لاگ\u200cها بروزرسانی شدند\':\'Activity logs refreshed\');\n}\ndocument.addEventListener(\'keydown\',e=>{if(e.key===\'Escape\')closeLogDetail()});\nfunction statsHoursFromMap(map){\n  const out=[];for(let h=0;h<24;h++){const key=String(h).padStart(2,\'0\')+\':00\';out.push({label:key,value:Number(map?.[key]||0)})}return out;\n}\nfunction makeSmoothPath(vals,w=900,h=250,pad=32){\n  const max=Math.max(1,...vals);const step=(w-pad*2)/Math.max(1,vals.length-1);return vals.map((v,i)=>{const x=pad+i*step,y=h-pad-(v/max)*(h-pad*2);return [x,y]}).map((p,i)=>{if(i===0)return `M ${p[0].toFixed(1)} ${p[1].toFixed(1)}`;const a=arguments;return ` L ${p[0].toFixed(1)} ${p[1].toFixed(1)}`}).join(\'\');\n}\nfunction drawTrafficChart(downloadMap,uploadMap){\n  const svg=document.getElementById(\'trafficChart\');if(!svg)return;\n  const d=statsHoursFromMap(downloadMap),u=statsHoursFromMap(uploadMap),dv=d.map(x=>x.value),uv=u.map(x=>x.value),all=[...dv,...uv],max=Math.max(1,...all);\n  const W=900,H=310,P=42, chartH=220, step=(W-P*2)/23;\n  const pts=(vals)=>vals.map((v,i)=>[P+i*step,H-P-(v/max)*chartH]);\n  const dp=pts(dv),up=pts(uv),line=(ps)=>ps.map((p,i)=>`${i?\'L\':\'M\'} ${p[0].toFixed(1)} ${p[1].toFixed(1)}`).join(\' \');\n  const area=(ps)=>`${line(ps)} L ${ps[ps.length-1][0].toFixed(1)} ${H-P} L ${ps[0][0].toFixed(1)} ${H-P} Z`;\n  let grid=\'\';for(let i=0;i<5;i++){const y=P+i*(chartH/4),v=max*(1-i/4);grid+=`<line class="traffic-grid" x1="${P}" y1="${y}" x2="${W-P}" y2="${y}"/><text class="traffic-axis" x="${P-8}" y="${y+4}" text-anchor="end">${fmtB(v)}</text>`}\n  let labels=\'\';for(let i=0;i<24;i+=4){const x=P+i*step;labels+=`<text class="traffic-axis" x="${x}" y="${H-8}" text-anchor="middle">${String(i).padStart(2,\'0\')}:00</text>`}\n  svg.innerHTML=`<defs><linearGradient id="areaD" x1="0" y1="0" x2="0" y2="1"><stop offset="0" style="stop-color:var(--accent);stop-opacity:.24"/><stop offset="1" style="stop-color:var(--accent);stop-opacity:0"/></linearGradient><linearGradient id="areaU" x1="0" y1="0" x2="0" y2="1"><stop offset="0" style="stop-color:var(--purple);stop-opacity:.20"/><stop offset="1" style="stop-color:var(--purple);stop-opacity:0"/></linearGradient></defs>${grid}${labels}<path class="traffic-area-d" d="${area(dp)}"/><path class="traffic-area-u" d="${area(up)}"/><path class="traffic-line-d" d="${line(dp)}"/><path class="traffic-line-u" d="${line(up)}"/>`;\n  const sd=document.getElementById(\'sparkDownload\'),su=document.getElementById(\'sparkUpload\');if(sd)sd.style.setProperty(\'--spark\',dv.join(\',\'));if(su)su.style.setProperty(\'--spark\',uv.join(\',\'));\n}\nfunction updateStatsUI(r,links,connections){\n  const arr=Array.isArray(links)?links:[];const active=arr.filter(l=>l.active!==false&&!configExpired(l)).length,expired=arr.filter(configExpired).length,used=arr.reduce((n,l)=>n+Number(l.used_bytes||0),0);\n  const down=Number(r.download_bytes??r.total_traffic_bytes??0),up=Number(r.upload_bytes||0),conns=Number(r.active_connections||connections||0);\n  const set=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v};\n  set(\'stDownload\',fmtB(down));set(\'stUpload\',fmtB(up));set(\'stConnections\',conns);set(\'stUsers\',active);set(\'stConfigs\',arr.length);set(\'stExpiredText\',expired+\' منقضی\');set(\'stServer\', \'آنلاین\');set(\'stServerHost\',location.host||\'ONEX\');set(\'stHost\',location.host||\'—\');set(\'stActiveConfigs\',active);set(\'stConn2\',conns);set(\'stErrors\',Number(r.total_errors||0));set(\'stRequests\',Number(r.total_requests||0).toLocaleString(\'fa-IR\')+\' درخواست\');set(\'stUptime\',r.uptime||\'—\');set(\'sumConfigs\',arr.length);set(\'sumActive\',active);set(\'sumTraffic\',fmtB(used));set(\'sumUpload\',fmtB(up));set(\'sumRequests\',Number(r.total_requests||0).toLocaleString(\'fa-IR\'));set(\'sumGroups\',Number(r.subs_count||0));\n  const ring=document.getElementById(\'uptimeRing\');if(ring)ring.style.background=\'conic-gradient(#18e6ae 0 99.9%,color-mix(in srgb, rgb(78 78 78 / .24) 82%, var(--accent)) 99.9%)\';\n  const rangeText={day:\'امروز\',week:\'این هفته\',month:\'این ماه\',all:\'کل\'}[statRange]||\'امروز\';set(\'statsChartRange\',rangeText);drawTrafficChart(r.hourly||{},r.hourly_upload||{});\n}\nasync function loadStatsDashboard(showToast=false){\n  const r=await api(\'/stats\');if(!r)return;const linksR=await api(\'/api/links\');const links=Array.isArray(linksR?.links)?linksR.links:(Array.isArray(linksR)?linksR:[]);let conn=0;try{const c=await api(\'/api/connections\');conn=Number(c?.count||c?.connections?.length||0)}catch(e){}updateStatsUI(r,links,conn);if(showToast)toast(lang===\'fa\'?\'آمار بروزرسانی شد\':\'Statistics refreshed\');\n}\n\nfunction setRange(r,el){\n  statRange=r;\n  document.querySelectorAll(\'#rangeTabs .range-tab\').forEach(t=>t.classList.toggle(\'on\',t.dataset.r===r));\n  loadStatsDashboard(false);\n  toast(t(\'r_\'+r));\n}\nfunction randomName(){\n  const chars=\'abcdefghijklmnopqrstuvwxyz0123456789\';\n  let s=\'\';\n  for(let i=0;i<10;i++) s+=chars[Math.floor(Math.random()*chars.length)];\n  if(/^[0-9]/.test(s)) s=\'a\'+s.slice(1);\n  document.getElementById(\'cName\').value=s;\n}\nlet __updateInfo=null;\nlet __updateCheckBusy=false;\nlet __updatePollTimer=null;\nfunction updateText(fa,en){return lang===\'fa\'?fa:en}\nfunction positionNotifyPanel(){const panel=document.getElementById(\'topNotifyPanel\'),btn=document.getElementById(\'topNotifyBtn\');if(!panel||!btn)return;const r=btn.getBoundingClientRect(),vw=window.innerWidth,w=Math.min(320,vw-24);panel.style.width=w+\'px\';panel.style.top=Math.round(r.bottom+8)+\'px\';let left=r.right-w;if(left<12)left=12;if(left+w>vw-12)left=vw-12-w;panel.style.left=Math.round(left)+\'px\';panel.style.right=\'auto\'}\nfunction toggleNotifications(force){const panel=document.getElementById(\'topNotifyPanel\'),btn=document.getElementById(\'topNotifyBtn\');if(!panel||!btn)return;if(panel.parentElement!==document.body)document.body.appendChild(panel);const open=typeof force===\'boolean\'?force:panel.hidden;panel.hidden=!open;btn.setAttribute(\'aria-expanded\',open?\'true\':\'false\');if(open)positionNotifyPanel()}\ndocument.addEventListener(\'click\',e=>{const panel=document.getElementById(\'topNotifyPanel\'),btn=document.getElementById(\'topNotifyBtn\');if(!panel||panel.hidden)return;if(panel.contains(e.target)||(btn&&btn.contains(e.target)))return;toggleNotifications(false)});\nwindow.addEventListener(\'resize\',()=>{const p=document.getElementById(\'topNotifyPanel\');if(p&&!p.hidden)positionNotifyPanel()});\nwindow.addEventListener(\'scroll\',()=>{const p=document.getElementById(\'topNotifyPanel\');if(p&&!p.hidden)positionNotifyPanel()},{passive:true});\ndocument.addEventListener(\'keydown\',e=>{if(e.key===\'Escape\')toggleNotifications(false)});\nfunction renderNotifications(){const list=document.getElementById(\'notifyList\'),badge=document.getElementById(\'notifyBadge\');if(!list||!badge)return;if(!__updateInfo||!__updateInfo.update_available){badge.textContent=\'0\';badge.classList.remove(\'show\');list.innerHTML=`<div class="notify-empty">${updateText(\'اعلان جدیدی وجود ندارد.\',\'No new notifications.\')}</div>`;return;}badge.textContent=\'1\';badge.classList.add(\'show\');const r=__updateInfo;const changes=Array.isArray(r.changelog)&&r.changelog.length?`<div class="notify-item-text" style="margin-top:5px">${r.changelog.slice(0,4).map(x=>`• ${esc(String(x))}`).join(\'<br>\')}</div>`:\'\';list.innerHTML=`<div class="notify-item"><div class="notify-item-title">🔄 ${esc(r.title||updateText(\'بروزرسانی جدید پنل\',\'New panel update\'))}</div><div class="notify-item-text">${esc(r.message||updateText(\'نسخه جدید پنل منتشر شده است.\',\'A new panel version is available.\'))}</div>${changes}<div class="notify-item-meta">${updateText(\'نسخه فعلی\',\'Current version\')}: ${esc(r.current_version||\'—\')} → ${esc(r.latest_version||\'—\')}</div><button type="button" class="notify-update-btn" onclick="toggleNotifications(false);panelUpdate()">${updateText(\'مشاهده و بروزرسانی\',\'View update\')}</button></div>`}\nasync function checkPanelUpdateWithNotify(showToast=false){if(__updateCheckBusy)return __updateInfo;__updateCheckBusy=true;try{const r=await api(\'/api/update/check\');if(r&&r.ok){const old=__updateInfo&&__updateInfo.latest_version;__updateInfo=r;setVersionLabels(r.current_version||\'1.0.1\',r.latest_version||r.current_version);renderNotifications();if(r.update_available&&old!==r.latest_version)showUpdatePrompt(r);if(r.update_available&&showToast&&old!==r.latest_version)toast(updateText(`نسخه جدید ${r.latest_version} آماده است`,`Version ${r.latest_version} is available`));}return r}catch(e){return null}finally{__updateCheckBusy=false}}\nfunction showUpdatePrompt(r){const modal=document.getElementById(\'updatePromptModal\');if(!modal||!r||!r.update_available)return;if(modal.parentElement!==document.body)document.body.appendChild(modal);const version=String(r.latest_version||\'\');if(!version)return;let seen=\'\';let snooze=0;try{seen=localStorage.getItem(\'onex_update_prompt_seen\')||\'\';snooze=Number(localStorage.getItem(\'onex_update_prompt_snooze\')||0)}catch(e){}if(seen===version&&Date.now()<snooze)return;const title=document.getElementById(\'updatePromptTitle\'),text=document.getElementById(\'updatePromptText\'),ver=document.getElementById(\'updatePromptVersion\'),yes=document.getElementById(\'updatePromptConfirm\'),later=document.getElementById(\'updatePromptLater\');if(title)title.textContent=r.title||updateText(\'بروزرسانی جدید در دسترس است\',\'New update is available\');if(text)text.textContent=r.message||updateText(\'نسخه جدید پنل آماده است. آیا می\u200cخواهید پنل را بروزرسانی کنید؟\',\'A new panel version is available. Would you like to update the panel?\');if(ver)ver.textContent=updateText(`نسخه فعلی: ${r.current_version||\'—\'}  →  نسخه جدید: ${version}`,`Current: ${r.current_version||\'—\'}  →  New: ${version}`);modal.classList.add(\'open\');modal.setAttribute(\'aria-hidden\',\'false\');const close=()=>{modal.classList.remove(\'open\');modal.setAttribute(\'aria-hidden\',\'true\');try{localStorage.setItem(\'onex_update_prompt_seen\',version);localStorage.setItem(\'onex_update_prompt_snooze\',String(Date.now()+6*3600*1000))}catch(e){}};if(later)later.onclick=close;if(yes)yes.onclick=()=>{try{localStorage.setItem(\'onex_update_prompt_seen\',version)}catch(e){}modal.classList.remove(\'open\');modal.setAttribute(\'aria-hidden\',\'true\');deployPanelUpdate();}}\nfunction startUpdateNotificationPolling(){if(__updatePollTimer)clearInterval(__updatePollTimer);checkPanelUpdateWithNotify(false);__updatePollTimer=setInterval(()=>checkPanelUpdateWithNotify(false),45000)}\nasync function checkPanelUpdate(showToast=true){\n  if(__updateCheckBusy)return __updateInfo;\n  __updateCheckBusy=true;\n  try{\n    const r=await api(\'/api/update/check\');\n    if(r&&r.ok){\n      const previousVersion=__updateInfo&&__updateInfo.latest_version;__updateInfo=r;setVersionLabels(r.current_version||\'1.0.1\',r.latest_version||r.current_version);renderNotifications();\n      if(r.update_available&&showToast&&previousVersion!==r.latest_version){toast(updateText(`نسخه جدید ${r.latest_version} آماده است`, `Version ${r.latest_version} is available`));}\n    }\n    return r;\n  }catch(e){return null}\n  finally{__updateCheckBusy=false}\n}\nasync function panelUpdate(){\n  const r=await checkPanelUpdate(false);\n  if(r&&r.ok&&!r.update_available){toast(updateText(\'پنل شما آخرین نسخه را دارد\',\'Panel is up to date\'));return}\n  if(r&&r.ok&&r.update_available){showUpdatePromptNow(r);return}\n  // Version check failed (network / GitHub limit): let the server try anyway.\n  if(confirm(updateText(\'بررسی نسخه انجام نشد. بروزرسانی به آخرین نسخه انجام شود؟\',\'Version check failed. Update to the latest version anyway?\')))deployPanelUpdate();\n}\nfunction showUpdatePromptNow(r){try{localStorage.removeItem(\'onex_update_prompt_seen\');localStorage.removeItem(\'onex_update_prompt_snooze\')}catch(e){}showUpdatePrompt(r)}\n\nasync function deployPanelUpdate(){\n  const btn=document.getElementById(\'panelDoUpdate\');\n  if(btn){btn.disabled=true;btn.textContent=updateText(\'در حال شروع بروزرسانی...\',\'Starting update...\')}\n  toast(updateText(\'در حال شروع بروزرسانی...\',\'Starting update...\'));\n  let r=null,errText=\'\';\n  try{\n    const res=await fetch(\'/api/update/deploy\',{method:\'POST\',cache:\'no-store\',credentials:\'same-origin\',headers:{\'Content-Type\':\'application/json\'},body:\'{}\'});\n    if(res.status===401){location.href=\'/login\';return}\n    try{r=await res.json()}catch(e){r=null}\n    if(!res.ok){errText=(r&&(r.detail||r.error))||(\'HTTP \'+res.status);r=null}\n  }catch(e){errText=updateText(\'ارتباط با سرور برقرار نشد\',\'Could not reach the server\')}\n  if(r&&r.ok&&r.update_started){\n    const b=document.getElementById(\'panelModalBody\');\n    if(b)b.innerHTML=`<div style="text-align:center;padding:18px"><div class="spin" style="margin:0 auto 14px"></div><p>${updateText(\'بروزرسانی شروع شد. پنل پس از استقرار نسخه جدید دوباره در دسترس قرار می\u200cگیرد.\',\'The update has started. The panel will become available again after the new deployment is live.\')}</p><p style="color:var(--t3);font-size:12px;margin-top:8px">${esc(r.latest_version||\'\')}</p></div>`;\n    toast(updateText(\'بروزرسانی شروع شد؛ چند دقیقه صبر کنید...\',\'Update started, please wait a few minutes...\'));\n    waitForNewVersion(r.latest_version);\n    return;\n  }\n  if(btn){btn.disabled=false;btn.textContent=updateText(\'شروع بروزرسانی پنل\',\'Update panel now\')}\n  if(r&&r.ok&&r.update_available===false){toast(r.message||updateText(\'پنل شما آخرین نسخه را دارد\',\'Panel is up to date\'));return}\n  toast(errText||(r&&r.message)||updateText(\'شروع بروزرسانی ناموفق بود\',\'Could not start the update\'));\n}\nfunction waitForNewVersion(target){let tries=0;const tick=async()=>{tries++;try{const res=await fetch(\'/api/update/check?force=1\',{cache:\'no-store\',credentials:\'same-origin\'});if(res.ok){const d=await res.json();if(d&&d.current_version&&(!target||d.current_version===target)){location.reload();return}}}catch(e){}if(tries<60)setTimeout(tick,10000);else location.reload()};setTimeout(tick,20000)}\n\nlet __tgUsers=[]; let __tgAudience=\'all\';\nfunction switchTgTab(tab){document.querySelectorAll(\'#tgTabs button\').forEach(b=>b.classList.toggle(\'on\',b.dataset.tgTab===tab));document.querySelectorAll(\'.tg-tab-panel\').forEach(p=>p.classList.toggle(\'on\',p.dataset.tgPanel===tab));if(tab===\'users\')renderTelegramUsers(false);}\ndocument.querySelectorAll(\'#tgTabs button\').forEach(b=>b.addEventListener(\'click\',()=>switchTgTab(b.dataset.tgTab)));\ndocument.querySelectorAll(\'.tg-audience button\').forEach(b=>b.addEventListener(\'click\',()=>{__tgAudience=b.dataset.aud;document.querySelectorAll(\'.tg-audience button\').forEach(x=>x.classList.toggle(\'on\',x.dataset.aud===__tgAudience));updateTelegramAudienceCount();}));\nfunction telegramAudienceUsers(){return __tgUsers.filter(u=>{if(u.blocked)return false;if(__tgAudience===\'active\')return !!u.active_today;if(__tgAudience===\'configs\')return Number(u.config_count||0)>0;if(__tgAudience===\'expired\')return Number(u.expired_count||0)>0;return true})}\nfunction updateTelegramAudienceCount(){const n=telegramAudienceUsers().length;[\'tgAudienceCount\',\'tgBroadcastCount\'].forEach(id=>{const e=document.getElementById(id);if(e)e.textContent=n})}\nfunction renderTelegramUsers(mini=false){const q=(document.getElementById(mini?\'tgUserSearchMini\':\'tgUserSearch\')?.value||\'\').trim().toLowerCase();const f=document.getElementById(\'tgUserFilter\')?.value||\'all\';let arr=__tgUsers.filter(u=>{const hay=String(u.user_id)+\' \'+String(u.username||\'\')+\' \'+String(u.first_name||\'\');if(q&&!hay.toLowerCase().includes(q))return false;if(f===\'active\'&&!u.active_today)return false;if(f===\'blocked\'&&!u.blocked)return false;if(f===\'configs\'&&!Number(u.config_count||0))return false;return true});if(mini)arr=arr.slice(0,4);const box=document.getElementById(mini?\'tgMiniUsers\':\'tgUsersList\');if(!box)return;if(!arr.length){box.innerHTML=\'<div class="tg-empty">کاربری ثبت نشده است.</div>\';return}box.innerHTML=arr.map(u=>{const initial=esc((u.first_name||u.username||String(u.user_id)).slice(0,1).toUpperCase());return `<div class="tg-${mini?\'mini-\':\'\'}user"><div class="tg-avatar">${initial}</div><div class="tg-user-copy"><b>${esc(u.first_name||u.username||\'Telegram User\')}</b><small>${u.username?\'@\'+esc(u.username)+\' · \':\'\'}${esc(u.user_id)} · ${Number(u.config_count||0)} کانفیگ</small></div><span class="tg-user-state ${u.blocked?\'bad\':\'\'}">${u.blocked?\'مسدود\':\'فعال\'}</span>${mini?\'\':`<div class="tg-user-actions"><button onclick="toggleTelegramUser(${Number(u.user_id)},${!u.blocked})" class="${u.blocked?\'\':\'danger\'}">${u.blocked?\'فعال\u200cسازی\':\'مسدود\'}</button></div>`}</div>`}).join(\'\')}\nasync function loadTelegramDashboard(){try{const r=await api(\'/api/telegram/dashboard\');if(!r)return;document.getElementById(\'tgUserCount\').textContent=r.user_count??0;document.getElementById(\'tgActiveToday\').textContent=r.active_today??0;document.getElementById(\'tgMessagesToday\').textContent=r.messages_today??0;document.getElementById(\'tgWebhookState\').textContent=r.webhook?\'متصل\':\'خاموش\';document.getElementById(\'tgWebhookMeta\').textContent=r.webhook_url||\'—\';document.getElementById(\'tgLastSeen\').textContent=r.last_seen||\'—\';document.getElementById(\'tgMode\').textContent=r.bot?.mode||\'—\';document.getElementById(\'tgAdminCount\').textContent=r.admin_count??0;document.getElementById(\'tgRuntimeMode\').textContent=r.bot?.mode||\'—\';document.getElementById(\'tgForceJoinState\').textContent=r.force_join?.enabled?\'فعال\':\'خاموش\';const hs=document.getElementById(\'tgHeroStatus\');if(hs){hs.classList.toggle(\'bad\',!r.enabled);hs.querySelector(\'span\').textContent=r.enabled?\'ربات فعال و متصل\':\'ربات غیرفعال\'}__tgUsers=Array.isArray(r.users)?r.users:[];renderTelegramUsers(true);renderTelegramUsers(false);updateTelegramAudienceCount();const act=document.getElementById(\'tgActivity\');const logs=Array.isArray(r.activity)?r.activity.slice(-8).reverse():[];act.innerHTML=logs.length?logs.map(x=>`<div class="tg-activity-row"><i class="tg-activity-dot"></i><div><b>${esc(x.message||\'—\')}</b><small>${esc(String(x.time||\'\').replace(\'T\',\' \').slice(0,19))}</small></div></div>`).join(\'\'):\'<div class="tg-empty">فعالیتی ثبت نشده است.</div>\';const owners=document.getElementById(\'tgConfigOwners\');const cfg=Array.isArray(r.config_owners)?r.config_owners:[];owners.innerHTML=cfg.length?cfg.slice(0,12).map(x=>`<div class="tg-config-owner"><b>${esc(x.label||x.uuid||\'—\')}</b><span>${esc(x.owner||\'بدون مالک\')} · ${esc(x.status||\'فعال\')}</span></div>`).join(\'\'):\'<div class="tg-empty">هنوز کانفیگی به کاربر تلگرام متصل نشده است.</div>\';}catch(e){}}\nasync function toggleTelegramUser(id,blocked){const r=await api(\'/api/telegram/users/\'+id+\'/block\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({blocked})});if(r&&r.ok){toast(blocked?\'کاربر مسدود شد\':\'کاربر فعال شد\');loadTelegramDashboard()}}\nasync function sendTelegramBroadcast(full=false){const el=document.getElementById(full?\'tgBroadcastText\':\'tgBroadcastMini\');const msg=(el?.value||\'\').trim();if(!msg){toast(\'متن پیام را وارد کنید\');return}const r=await api(\'/api/telegram/broadcast\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({message:msg,audience:__tgAudience})});if(r&&r.ok){const out=document.getElementById(\'tgBroadcastResult\');if(out){out.textContent=`ارسال تمام شد: ${r.sent} موفق · ${r.failed} ناموفق`;out.classList.add(\'show\')}toast(`ارسال شد: ${r.sent}`);el.value=\'\'}}\nasync function testTelegram(){const r=await api(\'/api/telegram/test\');if(r&&r.ok)toast(r.message||\'اتصال برقرار است\');else toast(\'اتصال ربات برقرار نشد\')}\nfunction openTgForceJoin(){toast(\'تنظیم عضویت اجباری از بخش تنظیمات ربات قابل مدیریت است\')}\nasync function saveTelegram(){\n  const token=document.getElementById(\'tgToken\').value.trim();\n  const admin=document.getElementById(\'tgAdmin\').value.trim();\n  const webhook=document.getElementById(\'tgWebhook\').checked;\n  if(!admin){toast(lang===\'fa\'?\'آیدی ادمین لازم است\':\'Admin ID required\');return}\n  toast(lang===\'fa\'?\'در حال فعال\u200cسازی...\':\'Activating...\');\n  const r=await api(\'/api/telegram/settings\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({token,admin_ids:admin,webhook})});\n  if(r){\n    document.getElementById(\'tgStatus\').textContent=r.message||(lang===\'fa\'?\'فعال شد\':\'Enabled\');\n    toast(r.message||\'OK\');\n  }\n}\nasync function loadTelegram(){\n  loadTelegramDashboard();\n  const r=await api(\'/api/telegram/settings\');\n  if(!r)return;\n  if(r.admin_ids) document.getElementById(\'tgAdmin\').value=r.admin_ids;\n  document.getElementById(\'tgWebhook\').checked=r.webhook!==false;\n  document.getElementById(\'tgStatus\').textContent=r.has_token?(lang===\'fa\'?\'توکن ذخیره شده: \':\'Token saved: \')+(r.token_masked||\'\'):\'\';\n}\nconst _goPage=goPage;\ngoPage=function(name){\n  _goPage(name);\n  if(name===\'telegram\') loadTelegram();\n  if(name===\'news\') loadNews();\n  if(name===\'admins\') loadAdmins();\n  if(name===\'groups\') loadGroups();\n  if(name===\'settings\') loadSecurity();\n};\n\nconst PERM_LABELS={\n  fa:{dash:\'داشبورد\',configs:\'کانفیگ\u200cها\',create:\'ساخت\',stats:\'آمار\',logs:\'لاگ\',settings:\'تنظیمات\',support:\'پشتیبانی\',telegram:\'ربات\',news:\'اخبار\',admins:\'ادمین\u200cها\'},\n  en:{dash:\'Dashboard\',configs:\'Configs\',create:\'Create\',stats:\'Stats\',logs:\'Logs\',settings:\'Settings\',support:\'Support\',telegram:\'Bot\',news:\'News\',admins:\'Admins\'}\n};\nlet USER_PERMS=null;\nlet USER_ROLE=\'owner\';\nfunction buildPermChecks(containerId, selected){\n  const box=document.getElementById(containerId);\n  if(!box)return;\n  const labels=PERM_LABELS[lang]||PERM_LABELS.fa;\n  box.innerHTML=Object.keys(labels).map(k=>{\n    const on=selected?!!selected[k]:([\'dash\',\'configs\',\'create\',\'stats\',\'news\'].includes(k));\n    return `<div style="display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 12px;border-radius:12px;background:var(--bg3);border:1px solid var(--card-b)">\n      <span style="font-size:12px;font-weight:600">${labels[k]}</span>\n      <label class="switch"><input type="checkbox" data-perm="${k}" ${on?\'checked\':\'\'}><span class="slider"></span></label>\n    </div>`;\n  }).join(\'\');\n}\nfunction readPermChecks(containerId){\n  const out={};\n  document.querySelectorAll(\'#\'+containerId+\' input[data-perm]\').forEach(inp=>{out[inp.getAttribute(\'data-perm\')]=inp.checked});\n  return out;\n}\nasync function loadMe(){\n  const r=await api(\'/api/me\');\n  if(!r)return;\n  USER_ROLE=r.role||\'owner\';\n  USER_PERMS=r.permissions||{};\n  const ownerUser=document.getElementById(\'newUser\');\n  if(ownerUser && USER_ROLE===\'owner\' && r.username) ownerUser.value=r.username;\n  document.querySelectorAll(\'.nav-item[data-perm]\').forEach(el=>{\n    const p=el.getAttribute(\'data-perm\');\n    if(USER_ROLE===\'owner\'){el.style.display=\'\';return}\n    el.style.display=USER_PERMS[p]?\'\':\'none\';\n  });\n  // hide admins for non-owner always if no perm\n  document.querySelectorAll(\'.nav-item[data-page="admins"]\').forEach(el=>{\n    if(USER_ROLE!==\'owner\') el.style.display=\'none\';\n  });\n}\nasync function loadNews(toastOk){\n  try{\n    const r=await api(\'/api/news\');\n    const meta=document.getElementById(\'newsMeta\');\n    if(meta && r) meta.textContent=(lang===\'fa\'?\'آخرین بروزرسانی: \':\'Last update: \')+(r.updated_at||\'—\');\n    if(toastOk) toast(lang===\'fa\'?\'اطلاعات تلگرام بروزرسانی شد\':\'Telegram info refreshed\');\n  }catch(e){\n    if(toastOk) toast(lang===\'fa\'?\'خطا در بروزرسانی\':\'Refresh failed\');\n  }\n}\nlet ADMIN_ITEMS=[];\nlet SELECTED_ADMIN_ID=\'\';\nconst ADMIN_PERM_GROUPS={\n  fa:[\n    [\'پنل و محتوا\',[\'dash\',\'configs\',\'create\',\'stats\',\'logs\']],\n    [\'سیستم و پشتیبانی\',[\'settings\',\'support\',\'telegram\',\'news\']],\n    [\'مدیریت\',[\'admins\']]\n  ],\n  en:[\n    [\'Panel & Content\',[\'dash\',\'configs\',\'create\',\'stats\',\'logs\']],\n    [\'System & Support\',[\'settings\',\'support\',\'telegram\',\'news\']],\n    [\'Management\',[\'admins\']]\n  ]\n};\nconst ADMIN_ROLE_PRESETS={\n  super:[\'dash\',\'configs\',\'create\',\'stats\',\'logs\',\'settings\',\'support\',\'telegram\',\'news\',\'admins\'],\n  admin:[\'dash\',\'configs\',\'create\',\'stats\',\'logs\',\'news\'],\n  operator:[\'dash\',\'configs\',\'create\'],\n};\nfunction adminRole(a){\n  const p=a&&a.permissions||{};\n  const keys=Object.keys(p).filter(k=>p[k]);\n  const all=ADMIN_ROLE_PRESETS.super.every(k=>p[k]);\n  const adm=ADMIN_ROLE_PRESETS.admin.every(k=>p[k]) && keys.length===ADMIN_ROLE_PRESETS.admin.length;\n  const op=ADMIN_ROLE_PRESETS.operator.every(k=>p[k]) && keys.length===ADMIN_ROLE_PRESETS.operator.length;\n  if(all)return \'Super Admin\'; if(adm)return \'Admin\'; if(op)return \'Operator\'; return \'Custom\';\n}\nfunction adminLastLogin(username){\n  const needle=String(username||\'\').toLowerCase();\n  const logs=window.__activityLogs||[];\n  for(const l of logs.slice().reverse()){\n    const m=String(l.message||\'\').toLowerCase();\n    if(needle && m.includes(needle) && (m.includes(\'ورود موفق\')||m.includes(\'login\'))) return (l.time||\'\').slice(0,19).replace(\'T\',\' \');\n  }\n  return \'—\';\n}\nasync function loadAdminActivityCache(){\n  const r=await api(\'/api/activity\');\n  window.__activityLogs=Array.isArray(r)?r:(r&&r.logs)||[];\n}\nfunction renderAdminList(){\n  const box=document.getElementById(\'adminsList\');\n  if(!box)return;\n  const q=(document.getElementById(\'adminSearch\')?.value||\'\').trim().toLowerCase();\n  const f=document.getElementById(\'adminStatusFilter\')?.value||\'all\';\n  const arr=ADMIN_ITEMS.filter(a=>{\n    const hay=((a.username||\'\')+\' \'+(a.label||\'\')).toLowerCase();\n    if(q&&!hay.includes(q))return false;\n    if(f===\'active\' && (a.blocked||!a.valid))return false;\n    if(f===\'blocked\' && !a.blocked)return false;\n    if(f===\'invalid\' && (a.blocked||a.valid))return false;\n    return true;\n  });\n  const count=document.getElementById(\'adminsCountText\');\n  if(count)count.textContent=`${arr.length} مورد نمایش داده می\u200cشود · ${ADMIN_ITEMS.length} ادمین`;\n  if(!arr.length){box.innerHTML=\'<div class="admin-empty">ادمینی با این فیلتر پیدا نشد.</div>\';return}\n  box.innerHTML=arr.map(a=>{\n    const status=a.blocked?[\'blocked\',\'مسدود\']:a.valid?[\'active\',\'فعال\']:[\'invalid\',\'نامعتبر\'];\n    const role=adminRole(a);\n    const initial=String(a.username||\'?\').slice(0,1).toUpperCase();\n    const last=adminLastLogin(a.username);\n    return `<div class="admin-row ${SELECTED_ADMIN_ID===a.id?\'selected\':\'\'}" onclick="selectAdmin(\'${esc(a.id)}\')">\n      <div class="admin-user"><div class="admin-avatar">${esc(initial)}</div><div class="admin-user-text"><div class="admin-user-name">${esc(a.username)}</div><div class="admin-user-label">${esc(a.label||\'—\')}</div></div></div>\n      <div><span class="admin-role">${esc(role)}</span></div>\n      <div><span class="admin-badge ${status[0]}"><i style="width:6px;height:6px;border-radius:50%;background:currentColor;display:inline-block"></i>${status[1]}</span></div>\n      <div style="font-size:9px;color:var(--t3)">${esc(last)}</div>\n      <div class="admin-ops" onclick="event.stopPropagation()">\n        <button class="admin-op edit" title="ویرایش / جزئیات" onclick="selectAdmin(\'${esc(a.id)}\');focusAdminDetails()"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20h9"/><path d="M16.5 3.5a2.1 2.1 0 0 1 3 3L8 18l-4 1 1-4Z"/></svg></button>\n        <button class="admin-op ${a.blocked?\'unblock\':\'block\'}" title="${a.blocked?\'رفع مسدودی\':\'مسدود کردن\'}" onclick="toggleBlockAdmin(\'${esc(a.id)}\',${!a.blocked})"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="9"/>${a.blocked?\'<path d="M8 12h8"/>\':\'<path d="M8 8l8 8M16 8l-8 8"/>\'}</svg></button>\n        <button class="admin-op delete" title="حذف" onclick="deleteAdmin(\'${esc(a.id)}\')"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 7h16M10 11v6M14 11v6M6 7l1 14h10l1-14M9 7V4h6v3"/></svg></button>\n      </div>\n    </div>`;\n  }).join(\'\');\n}\nfunction renderAdminSelectors(){\n  const sel=document.getElementById(\'adminPermSelect\');\n  if(!sel)return;\n  sel.innerHTML=\'<option value="">انتخاب ادمین</option>\'+ADMIN_ITEMS.map(a=>`<option value="${esc(a.id)}" ${a.id===SELECTED_ADMIN_ID?\'selected\':\'\'}>${esc(a.username)} — ${esc(adminRole(a))}</option>`).join(\'\');\n}\nfunction buildAdminPermEditor(a){\n  const box=document.getElementById(\'adminPerms\'),empty=document.getElementById(\'adminPermEmpty\');\n  const save=document.getElementById(\'saveAdminPermsBtn\'),all=document.getElementById(\'adminAllBtn\'),none=document.getElementById(\'adminNoneBtn\');\n  if(!a){if(box)box.innerHTML=\'\';if(empty)empty.style.display=\'block\';[save,all,none].forEach(x=>{if(x)x.disabled=true});return}\n  if(empty)empty.style.display=\'none\';[save,all,none].forEach(x=>{if(x)x.disabled=false});\n  const groups=ADMIN_PERM_GROUPS[lang]||ADMIN_PERM_GROUPS.fa, perms=a.permissions||{};\n  box.innerHTML=groups.map(([title,keys])=>`<div class="admin-perm-group"><h4>${title}</h4>${keys.map(k=>`<div class="admin-perm-item"><span>${(PERM_LABELS[lang]||PERM_LABELS.fa)[k]||k}</span><label class="switch"><input type="checkbox" data-admin-perm="${k}" ${perms[k]?\'checked\':\'\'}><span class="slider"></span></label></div>`).join(\'\')}</div>`).join(\'\');\n}\nfunction selectAdmin(id){\n  SELECTED_ADMIN_ID=id||\'\';\n  const a=ADMIN_ITEMS.find(x=>x.id===SELECTED_ADMIN_ID)||null;\n  renderAdminList();renderAdminSelectors();buildAdminPermEditor(a);renderAdminDetails(a);renderAdminActivity(a);\n}\nfunction setAllAdminPerms(on){document.querySelectorAll(\'#adminPerms input[data-admin-perm]\').forEach(x=>x.checked=!!on)}\nfunction readAdminPerms(){const out={};document.querySelectorAll(\'#adminPerms input[data-admin-perm]\').forEach(x=>out[x.getAttribute(\'data-admin-perm\')]=x.checked);return out}\nasync function saveAdminPermissions(){\n  if(!SELECTED_ADMIN_ID)return;\n  const r=await api(\'/api/admins/\'+SELECTED_ADMIN_ID,{method:\'PATCH\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({permissions:readAdminPerms()})});\n  if(r){toast(\'دسترسی\u200cها ذخیره شد\');await loadAdmins()}\n}\nfunction renderAdminDetails(a){\n  const box=document.getElementById(\'adminDetails\');if(!box)return;\n  if(!a){box.innerHTML=\'<div class="admin-empty">برای مشاهده جزئیات، یک ادمین را انتخاب کنید.</div>\';return}\n  const exp=a.expires_at?String(a.expires_at).slice(0,19).replace(\'T\',\' \'):\'بدون انقضا\';\n  const perms=Object.entries(a.permissions||{}).filter(([,v])=>v).map(([k])=>(PERM_LABELS[lang]||PERM_LABELS.fa)[k]||k);\n  box.innerHTML=`<div class="admin-details-grid">\n    <div class="admin-detail-box"><span>نام کاربری</span><b>${esc(a.username)}</b></div><div class="admin-detail-box"><span>نقش</span><b>${esc(adminRole(a))}</b></div>\n    <div class="admin-detail-box"><span>وضعیت</span><b>${a.blocked?\'🔴 مسدود\':a.valid?\'🟢 فعال\':\'🟠 نامعتبر\'}</b></div><div class="admin-detail-box"><span>آخرین ورود</span><b>${esc(adminLastLogin(a.username))}</b></div>\n    <div class="admin-detail-box"><span>حجم مصرف</span><b>${fmtB(a.used_bytes)}${a.limit_bytes?\' / \'+fmtB(a.limit_bytes):\' / ∞\'}</b></div><div class="admin-detail-box"><span>انقضا</span><b>${esc(exp)}</b></div>\n    <div class="admin-detail-box"><span>تاریخ ایجاد</span><b>${esc(String(a.created_at||\'—\').slice(0,19).replace(\'T\',\' \'))}</b></div><div class="admin-detail-box"><span>عنوان</span><b>${esc(a.label||\'—\')}</b></div>\n  </div><div class="admin-detail-perms">${perms.length?perms.map(x=>`<span class="admin-detail-perm">${esc(x)}</span>`).join(\'\'):\'<span style="font-size:9px;color:var(--t3)">بدون دسترسی فعال</span>\'}</div>`;\n}\nfunction renderAdminActivity(a){\n  const box=document.getElementById(\'adminActivity\'),sub=document.getElementById(\'adminActivitySub\');if(!box)return;\n  if(!a){box.innerHTML=\'<div class="admin-empty">برای مشاهده فعالیت، یک ادمین را انتخاب کنید.</div>\';if(sub)sub.textContent=\'فعالیت\u200cهای ثبت\u200cشده برای ادمین انتخاب\u200cشده\';return}\n  const needle=String(a.username||\'\').toLowerCase();\n  const logs=(window.__activityLogs||[]).filter(l=>String(l.message||\'\').toLowerCase().includes(needle));\n  if(sub)sub.textContent=`فعالیت\u200cهای ثبت\u200cشده برای ${a.username}`;\n  if(!logs.length){box.innerHTML=\'<div class="admin-empty">هنوز فعالیتی برای این ادمین ثبت نشده است.</div>\';return}\n  box.innerHTML=logs.slice().reverse().map(l=>`<div class="admin-activity-item"><div class="admin-activity-time">${esc(String(l.time||\'\').slice(11,19)||\'—\')}</div><div class="admin-activity-msg">${esc(l.message||\'—\')}</div></div>`).join(\'\');\n}\nfunction focusAdminDetails(){document.getElementById(\'adminDetails\')?.scrollIntoView({behavior:\'smooth\',block:\'center\'})}\nfunction focusAdminCreate(){document.getElementById(\'adminCreateCard\')?.scrollIntoView({behavior:\'smooth\',block:\'center\'});setTimeout(()=>document.getElementById(\'adUser\')?.focus(),250)}\nasync function loadAdmins(){\n  const box=document.getElementById(\'adminsList\');if(box)box.innerHTML=\'<div class="admin-empty">در حال دریافت...</div>\';\n  const r=await api(\'/api/admins\');\n  if(!r||!Array.isArray(r.admins)){if(box)box.innerHTML=\'<div class="admin-empty">دریافت لیست ادمین\u200cها ناموفق بود.</div>\';return}\n  ADMIN_ITEMS=r.admins;\n  await loadAdminActivityCache();\n  if(!SELECTED_ADMIN_ID || !ADMIN_ITEMS.some(a=>a.id===SELECTED_ADMIN_ID)) SELECTED_ADMIN_ID=ADMIN_ITEMS[0]?.id||\'\';\n  renderAdminList();renderAdminSelectors();\n  const a=ADMIN_ITEMS.find(x=>x.id===SELECTED_ADMIN_ID)||null;\n  buildAdminPermEditor(a);renderAdminDetails(a);renderAdminActivity(a);\n}\nasync function createAdmin(){\n  const user=document.getElementById(\'adUser\').value.trim(),pw=document.getElementById(\'adPw\').value,pw2=document.getElementById(\'adPw2\').value;\n  if(!user||!pw||!pw2){toast(\'نام کاربری و هر دو رمز را وارد کنید\');return}\n  if(pw!==pw2){toast(\'تکرار رمز یکسان نیست\');return}\n  const role=document.getElementById(\'adRole\')?.value||\'admin\';const permissions={};(ADMIN_ROLE_PRESETS[role]||ADMIN_ROLE_PRESETS.admin).forEach(k=>permissions[k]=true);const body={username:user,label:document.getElementById(\'adLabel\').value.trim()||user,password:pw,repeat_password:pw2,limit_value:Number(document.getElementById(\'adLimit\').value)||0,limit_unit:document.getElementById(\'adUnit\').value,expires_days:Number(document.getElementById(\'adDays\').value)||0,permissions,active:!!document.getElementById(\'adActive\')?.checked};\n  const r=await api(\'/api/admins\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(body)});\n  if(r){toast(\'اکانت ادمین ساخته شد\');[\'adUser\',\'adLabel\',\'adPw\',\'adPw2\'].forEach(id=>{const e=document.getElementById(id);if(e)e.value=\'\'});document.getElementById(\'adLimit\').value=\'0\';document.getElementById(\'adDays\').value=\'0\';document.getElementById(\'adActive\').checked=true;await loadAdmins();selectAdmin(r.id)}\n}\nasync function toggleBlockAdmin(id,blocked){\n  const r=await api(\'/api/admins/\'+id,{method:\'PATCH\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({blocked})});\n  if(r){toast(blocked?\'ادمین مسدود شد\':\'مسدودی ادمین برداشته شد\');await loadAdmins()}\n}\nasync function deleteAdmin(id){\n  const a=ADMIN_ITEMS.find(x=>x.id===id);if(!confirm(`اکانت «${a?.username||\'ادمین\'}» حذف شود؟`))return;\n  const r=await api(\'/api/admins/\'+id,{method:\'DELETE\'});\n  if(r){if(SELECTED_ADMIN_ID===id)SELECTED_ADMIN_ID=\'\';toast(\'اکانت حذف شد\');await loadAdmins()}\n}\n\n\nasync function loadProtocols(){\n  window.__protocolList=window.__protocolList||[];\n  const r=await api(\'/api/protocols\');\n  const list=(r&&r.protocols)||[]; window.__protocolList=list;\n  const def=(r&&r.default)||\'vless-ws\';\n  __protocolPickerOptions=list;\n  [\'cProto\',\'aProto\'].forEach(id=>{\n    const el=document.getElementById(id);\n    if(!el)return;\n    el.innerHTML=list.map(p=>`<option value="${esc(p.id)}" ${p.id===def?\'selected\':\'\'}>${esc(p.label||p.id)}</option>`).join(\'\')\n      ||\'<option value="vless-ws">ONEX WB</option>\';\n  });\n  setupProtocolPickers();\n  const bundle=document.getElementById(\'protocolBundleOptions\');\n  if(bundle){\n    const ids=[\'vless-ws\',\'xhttp-packet-up\',\'xhttp-stream-up\',\'xhttp-stream-one\',\'trojan\'].filter(id=>list.some(p=>p.id===id));\n    bundle.innerHTML=ids.map(id=>`<label class="cfgx-chip"><input type="checkbox" value="${id}"><img src="${PROTOCOL_ICON_DATA[id]||PROTOCOL_ICON_DATA[\'vless-ws\']}" alt="" loading="lazy" decoding="async"><b>${esc(protocolPickerShort(id))}</b><em>اصلی</em><i aria-hidden="true"></i></label>`).join(\'\');\n    if(window.cfgxSync)window.cfgxSync();\n  }\n}\nasync function loadAdBlocker(){\n  const r=await api(\'/api/ad-blocker\');if(!r)return;\n  const e=document.getElementById(\'adBlockEnabled\'),d=document.getElementById(\'adBlockDomains\');\n  if(e)e.checked=!!r.enabled;if(d)d.value=(r.domains||[]).join(\'\\n\');\n}\nasync function saveAdBlocker(){\n  const e=document.getElementById(\'adBlockEnabled\'),d=document.getElementById(\'adBlockDomains\');\n  const r=await api(\'/api/ad-blocker\',{method:\'PUT\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({enabled:!!e?.checked,domains:d?.value||\'\'})});\n  if(r)toast(\'مسدودکننده تبلیغات ذخیره شد\');\n}\nlet __allLinks=[];\nlet cfgStatusFilter=\'all\',cfgSortMode=\'newest\';\nfunction toggleConfigFilters(){document.getElementById(\'cfgFilterRow\')?.classList.toggle(\'open\')}\nfunction setCfgStatus(v,el){cfgStatusFilter=v;document.querySelectorAll(\'#cfgFilterRow [data-status]\').forEach(x=>x.classList.toggle(\'on\',x===el));renderConfigCards(getFilteredConfigs())}\nfunction setCfgSort(v,el){cfgSortMode=v;document.querySelectorAll(\'#cfgFilterRow [data-sort]\').forEach(x=>x.classList.toggle(\'on\',x===el));renderConfigCards(getFilteredConfigs())}\nfunction configExpired(l){return !!l.expired||(l.expires_at&&new Date(l.expires_at).getTime()<=Date.now())||(Number(l.limit_bytes)>0&&Number(l.used_bytes||0)>=Number(l.limit_bytes))}\nfunction getFilteredConfigs(){const q=(document.getElementById(\'cfgSearch\')?.value||\'\').trim().toLowerCase();let a=__allLinks.filter(l=>{const dead=configExpired(l),active=l.active!==false&&!dead;if(cfgStatusFilter===\'active\'&&!active)return false;if(cfgStatusFilter===\'expired\'&&!dead)return false;if(cfgStatusFilter===\'expiring\'&&!(window.homeIsExpiring&&homeIsExpiring(l)&&!dead))return false;if(!q)return true;return [l.label,l.name,l.protocol,l.protocol_label,l.uuid,l.id,l.sub,l.sub_url,l.vless,l.vless_full].map(x=>String(x||\'\').toLowerCase()).some(x=>x.includes(q))});a.sort((x,y)=>cfgSortMode===\'name\'?String(x.label||x.name||\'\').localeCompare(String(y.label||y.name||\'\')):cfgSortMode===\'usage\'?Number(y.used_bytes||0)-Number(x.used_bytes||0):String(y.created_at||\'\').localeCompare(String(x.created_at||\'\')));return a}\nfunction filterConfigs(){renderConfigCards(getFilteredConfigs())}\nfunction protocolUi(id){const m={\'vless-ws\':[\'ONEX WB\',\'/api/protocol-icon/vless-ws.png?v=1.3.5\'],\'xhttp-packet-up\':[\'ONEX Xhttp\',\'/api/protocol-icon/xhttp-packet-up.png?v=1.3.5\'],\'xhttp-stream-up\':[\'ONEX GAMING\',\'/api/protocol-icon/xhttp-stream-up.png?v=1.3.5\'],\'xhttp-stream-one\':[\'ONEX Stream\',\'/api/protocol-icon/xhttp-stream-one.png?v=1.3.5\'],\'trojan\':[\'Trojan\',\'/api/protocol-icon/trojan.png?v=1.3.5\']};return m[id]||[String(id||\'\').toUpperCase(),\'/api/protocol-icon/vless-ws.png?v=1.3.5\']}\nfunction cfgDate(v){if(!v)return \'بدون انقضا\';try{return new Date(v).toLocaleDateString(\'fa-IR\',{year:\'numeric\',month:\'2-digit\',day:\'2-digit\'})}catch(e){return String(v).slice(0,10)}}\nfunction updateConfigStats(){const total=__allLinks.length,expired=__allLinks.filter(configExpired).length,active=__allLinks.filter(l=>l.active!==false&&!configExpired(l)).length,used=__allLinks.reduce((n,l)=>n+Number(l.used_bytes||0),0),set=(id,v)=>{const e=document.getElementById(id);if(e)e.textContent=v};set(\'cfgStatTotal\',total);set(\'cfgStatUsed\',fmtB(used));set(\'cfgStatActive\',active);set(\'cfgStatExpired\',expired);set(\'cfgVisibleCount\',`${getFilteredConfigs().length} مورد`)}\nlet __openConfigMenuUid=\'\';\nfunction closeConfigMenus(){__openConfigMenuUid=\'\';document.querySelectorAll(\'.cfg-menu.open\').forEach(m=>m.classList.remove(\'open\'));document.querySelectorAll(\'.cfg-menu[data-portal="1"]\').forEach(m=>{const hostId=m.dataset.hostId;const host=hostId?document.getElementById(hostId):null;if(host&&m.parentElement===document.body)host.appendChild(m);m.style.left=\'\';m.style.top=\'\';m.style.right=\'\';m.style.bottom=\'\';m.style.display=\'\';m.dataset.portal=\'\';delete m.dataset.hostId})}\nfunction positionConfigMenu(menu,btn){if(!menu||!btn)return;const r=btn.getBoundingClientRect();const gap=8;const width=Math.min(236,window.innerWidth-16);menu.style.width=width+\'px\';menu.style.right=\'auto\';menu.style.left=Math.max(8,Math.min(window.innerWidth-width-8,r.right-width))+\'px\';menu.style.top=\'0px\';menu.style.display=\'block\';const mh=menu.offsetHeight||220;let top=r.bottom+gap;if(top+mh>window.innerHeight-8)top=r.top-mh-gap;if(top<8)top=8;menu.style.top=top+\'px\'}\nfunction toggleConfigMenu(e,uid){e.preventDefault();e.stopPropagation();const id=\'cfgMenu_\'+uid.replace(/[^a-zA-Z0-9_-]/g,\'_\'),m=document.getElementById(id);if(!m)return;const was=(__openConfigMenuUid===uid&&m.classList.contains(\'open\'));if(was){closeConfigMenus();return}closeConfigMenus();const btn=e.currentTarget||e.target.closest(\'.cfg-menu-btn\');m.dataset.hostId=id.replace(\'cfgMenu_\',\'cfgHost_\');const host=document.createElement(\'span\');host.id=m.dataset.hostId;host.hidden=true;m.parentElement.insertBefore(host,m);document.body.appendChild(m);m.dataset.portal=\'1\';m.classList.add(\'open\');__openConfigMenuUid=uid;positionConfigMenu(m,btn)}\nfunction configMenuAction(a,uid){closeConfigMenus();if(a===\'copy\')return copyLinkById(uid);if(a===\'sub\')return copySubById(uid);if(a===\'info\'){window.open(\'/info/\'+encodeURIComponent(uid),\'_blank\',\'noopener\');return}if(a===\'reset\')return resetUsage(uid);if(a===\'delete\')return deleteLink(uid)}\ndocument.addEventListener(\'click\',e=>{if(!e.target.closest(\'.cfg-menu\')&&!e.target.closest(\'.cfg-menu-btn\'))closeConfigMenus()});\nwindow.addEventListener(\'resize\',()=>{const m=document.querySelector(\'.cfg-menu.open[data-portal="1"]\');if(!m)return;const uid=(m.id||\'\').replace(/^cfgMenu_/,\'\');const btn=document.querySelector(\'.cfg-menu-btn[data-menu-uid="\'+CSS.escape(uid)+\'"]\');if(btn)positionConfigMenu(m,btn)});\nwindow.addEventListener(\'scroll\',()=>{const m=document.querySelector(\'.cfg-menu.open[data-portal="1"]\');if(!m)return;const uid=(m.id||\'\').replace(/^cfgMenu_/,\'\');const btn=document.querySelector(\'.cfg-menu-btn[data-menu-uid="\'+CSS.escape(uid)+\'"]\');if(btn)positionConfigMenu(m,btn)},true);\nfunction renderConfigCards(arr){const box=document.getElementById(\'cfgCards\');if(!box)return;\n  const selectedIds=new Set([...document.querySelectorAll(\'#cfgCards .cfg-chk:checked\')].map(c=>String(c.value)));\n  const openMenuUid=__openConfigMenuUid||\'\';\n  const oldMenu=openMenuUid?document.getElementById(\'cfgMenu_\'+openMenuUid):null;\n  if(oldMenu)oldMenu.remove();\n  updateConfigStats();\n  if(!arr.length){box.innerHTML=\'<div class="cfg-empty">\'+(lang===\'fa\'?\'کانفیگی با این فیلتر پیدا نشد\':\'No configs match the filter\')+\'</div>\';updateBulkBar();__openConfigMenuUid=\'\';return}\n  box.innerHTML=arr.map(l=>{const uid=String(l.uuid||l.id||\'\'),dead=configExpired(l),active=l.active!==false&&!dead,[pl,icon]=protocolUi(l.protocol),used=Number(l.used_bytes||0),lim=Number(l.limit_bytes||0),pct=lim>0?Math.min(100,Math.round(used/lim*100)):0,conn=Number(l.connected_ips||0),safeUid=esc(uid),mid=\'cfgMenu_\'+uid.replace(/[^a-zA-Z0-9_-]/g,\'_\'),usage=lim>0?`${fmtB(used)} / ${fmtB(lim)}`:fmtB(used),activeState=l.active!==false;return `<article class="cfg-card ${dead?\'expired\':\'\'}" draggable="true" data-uid="${safeUid}" ondragstart="cfgDragStart(event)" ondragover="cfgDragOver(event)" ondrop="cfgDrop(event)" ondragend="cfgDragEnd(event)"><label class="cfg-card-check"><input type="checkbox" class="cfg-chk" value="${safeUid}" onchange="updateBulkBar()"></label><div class="cfg-proto-icon"><img src="${icon}" alt=""></div><div class="cfg-main"><div class="cfg-name-row"><b>${esc(l.label||l.name||uid.slice(0,8))}</b><button type="button" class="cfg-edit-dot" title="ویرایش کانفیگ" aria-label="ویرایش کانفیگ" onclick="openConfigEditor(event,\'${safeUid}\')">✎</button></div><div class="cfg-proto">${esc(pl)}</div><div class="cfg-meta"><span><i>♧</i>${conn} اتصال</span><span><i>◷</i>${dead?\'منقضی شده\':(l.expires_at?\'انقضا \'+cfgDate(l.expires_at):\'بدون انقضا\')}</span></div></div><div class="cfg-side"><div class="cfg-side-top"><span class="cfg-status ${active?\'\':\'bad\'}"><i></i>${active?\'فعال\':\'غیرفعال\'}</span><button type="button" class="cfg-active-toggle ${activeState?\'on\':\'\'}" onclick="toggleConfigActive(event,\'${safeUid}\',${activeState?\'false\':\'true\'})" aria-pressed="${activeState?\'true\':\'false\'}"><span class="cfg-active-dot"></span><span>${activeState?\'فعال\':\'خاموش\'}</span></button></div><div class="cfg-usage"><div class="cfg-usage-ring" style="--pct:${pct}%"><span>${pct}%</span></div><div class="cfg-usage-copy"><b>${esc(usage)}</b>${lim>0?`<div class="cfg-usage-track"><div class="cfg-usage-fill" style="width:${pct}%"></div></div>`:\'\'}</div></div></div><button class="cfg-menu-btn" type="button" data-menu-uid="${safeUid}" onclick="toggleConfigMenu(event,\'${safeUid}\')" aria-label="عملیات">⋮</button><div class="cfg-menu" id="${mid}"><div class="cfg-menu-head"><span>عملیات کانفیگ</span><small>برای بستن بیرون منو بزنید</small></div><button type="button" onclick="configMenuAction(\'copy\',\'${safeUid}\')">کپی VLESS</button><button type="button" onclick="configMenuAction(\'sub\',\'${safeUid}\')">کپی ساب</button><button type="button" onclick="configMenuAction(\'info\',\'${safeUid}\')">صفحه اطلاعات</button><button type="button" onclick="configMenuAction(\'reset\',\'${safeUid}\')">ریست مصرف</button><button type="button" class="danger" onclick="configMenuAction(\'delete\',\'${safeUid}\')">حذف کانفیگ</button></div></article>`}).join(\'\');\n  document.querySelectorAll(\'#cfgCards .cfg-chk\').forEach(c=>{c.checked=selectedIds.has(String(c.value))});\n  updateBulkBar();\n  if(openMenuUid){const menu=document.getElementById(\'cfgMenu_\'+openMenuUid);const btn=document.querySelector(\'.cfg-menu-btn[data-menu-uid="\'+CSS.escape(openMenuUid)+\'"]\');if(menu&&btn){document.body.appendChild(menu);menu.dataset.portal=\'1\';menu.classList.add(\'open\');__openConfigMenuUid=openMenuUid;positionConfigMenu(menu,btn)}else{__openConfigMenuUid=\'\'}}\n}\nfunction renderLinks(arr){window.__linksMap={};arr.forEach(l=>window.__linksMap[String(l.uuid||l.id||\'\')]=l);renderConfigCards(getFilteredConfigs())}\nfunction softUpdateLinks(arr){window.__linksMap={};arr.forEach(l=>window.__linksMap[String(l.uuid||l.id||\'\')]=l);renderConfigCards(getFilteredConfigs())}\nfunction patchLinkRow(tr,l){renderConfigCards(getFilteredConfigs())}\nasync function resetUsage(uid){\n  if(!confirm(lang===\'fa\'?\'مصرف ریست شود؟\':\'Reset usage?\'))return;\n  const r=await api(\'/api/links/\'+uid+\'/reset-usage\',{method:\'POST\'});\n  if(r!==null){toast(lang===\'fa\'?\'مصرف ریست شد\':\'Usage reset\');refreshAll()}\n}\n\n\nlet __dragUid=null;\nfunction cfgDragStart(e){__dragUid=e.currentTarget.getAttribute(\'data-uid\');e.currentTarget.style.opacity=\'.5\';e.dataTransfer.effectAllowed=\'move\';}\nfunction cfgDragOver(e){e.preventDefault();e.dataTransfer.dropEffect=\'move\';const card=e.currentTarget;if(card&&card.classList.contains(\'cfg-card\'))card.style.boxShadow=\'0 0 0 1px rgba(var(--accent2-rgb),.55),0 12px 28px rgba(var(--accent-rgb),.14)\';}\nfunction cfgDragEnd(e){e.currentTarget.style.opacity=\'1\';document.querySelectorAll(\'.cfg-card\').forEach(card=>card.style.boxShadow=\'\');__dragUid=null;}\nasync function cfgDrop(e){e.preventDefault();const target=e.currentTarget.getAttribute(\'data-uid\');document.querySelectorAll(\'.cfg-card\').forEach(card=>card.style.boxShadow=\'\');if(!__dragUid||!target||__dragUid===target)return;const cards=[...document.querySelectorAll(\'#cfgCards .cfg-card\')],ids=cards.map(card=>card.getAttribute(\'data-uid\'));const from=ids.indexOf(__dragUid),to=ids.indexOf(target);if(from<0||to<0)return;ids.splice(from,1);ids.splice(to,0,__dragUid);const box=document.getElementById(\'cfgCards\');ids.forEach(id=>{const el=box.querySelector(\'.cfg-card[data-uid="\'+CSS.escape(id)+\'"]\');if(el)box.appendChild(el)});await api(\'/api/links/reorder\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({order:ids})});toast(lang===\'fa\'?\'ترتیب ذخیره شد\':\'Order saved\');__dragUid=null;}\n\nfunction updateBulkBar(){\n  const n=document.querySelectorAll(\'.cfg-chk:checked\').length;\n  const bar=document.getElementById(\'bottomBulkBar\');\n  const cnt=document.getElementById(\'bulkCount\');\n  if(cnt) cnt.textContent = n + (lang===\'fa\'?\' انتخاب\u200cشده\':\' selected\');\n  if(bar) bar.classList.toggle(\'show\', n>0);\n  const all=document.getElementById(\'chkAll\');\n  if(all && n===0) all.checked=false;\n}\nfunction clearSelection(){\n  document.querySelectorAll(\'.cfg-chk\').forEach(c=>c.checked=false);\n  const all=document.getElementById(\'chkAll\');\n  if(all) all.checked=false;\n  updateBulkBar();\n}\nfunction toggleSelectAll(on){\n  document.querySelectorAll(\'.cfg-chk\').forEach(c=>c.checked=!!on);\n  updateBulkBar();\n}\n\nfunction selectedCfgIds(){return [...document.querySelectorAll(\'.cfg-chk:checked\')].map(c=>c.value)}\nasync function bulkDelete(){\n  const ids=selectedCfgIds();\n  if(!ids.length){toast(lang===\'fa\'?\'چیزی انتخاب نشده\':\'Nothing selected\');return}\n  if(!confirm(lang===\'fa\'?`حذف ${ids.length} کانفیگ؟`:`Delete ${ids.length}?`))return;\n  const r=await api(\'/api/links/bulk-delete\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({ids})});\n  if(r){toast(lang===\'fa\'?`حذف شد: ${r.deleted}`:`Deleted: ${r.deleted}`);refreshAll()}\n}\nasync function bulkMoveGroup(){\n  const ids=selectedCfgIds();\n  const cid=document.getElementById(\'bulkGroup\')?.value||\'0\';\n  if(!ids.length){toast(lang===\'fa\'?\'چیزی انتخاب نشده\':\'Nothing selected\');return}\n  const r=await api(\'/api/links/bulk-category\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({ids,category_id:cid})});\n  if(r){toast(lang===\'fa\'?\'به گروه منتقل شد\':\'Moved\');refreshAll()}\n}\nasync function loadCategories(){\n  const r=await api(\'/api/categories\');\n  const list=(r&&r.categories)||[];\n  window.__catMap={};\n  list.forEach(g=>{window.__catMap[String(g.id)]=g.name||g.id});\n  const bulk=document.getElementById(\'bulkGroup\');\n  const cGroup=document.getElementById(\'cGroup\');\n  const opts=list.map(g=>`<option value="${esc(g.id)}">${esc(g.name||g.id)}</option>`).join(\'\');\n  if(bulk) bulk.innerHTML=opts||\'<option value="0">عمومی</option>\';\n  if(cGroup) cGroup.innerHTML=opts||\'<option value="0">عمومی</option>\';\n}\n\nlet __subGroups=[];\nlet __selectedSubId=\'\';\nlet __groupFilter=\'all\';\nlet __groupProtocols=[];\n\nfunction groupProtocolLabel(id){\n  const labels={\'vless-ws\':\'ONEX WB\',\'xhttp-packet-up\':\'ONEX Xhttp\',\'xhttp-stream-up\':\'ONEX Gaming\',\'xhttp-stream-one\':\'ONEX Stream\',\'trojan\':\'Trojan\'};\n  return labels[id]||id;\n}\nfunction groupProtocolIcon(id){\n  const map={\'vless-ws\':\'/api/protocol-icon/vless-ws.png?v=1.3.5\',\'xhttp-packet-up\':\'/api/protocol-icon/xhttp-packet-up.png?v=1.3.5\',\'xhttp-stream-up\':\'/api/protocol-icon/xhttp-stream-up.png?v=1.3.5\',\'xhttp-stream-one\':\'/api/protocol-icon/xhttp-stream-one.png?v=1.3.5\'};\n  return map[id]||\'\';\n}\nfunction setGroupFilter(f,btn){__groupFilter=f;document.querySelectorAll(\'.group-filter\').forEach(x=>x.classList.toggle(\'on\',x===btn));renderGroupList()}\nfunction selectedSub(){return __subGroups.find(g=>String(g.sub_id)===String(__selectedSubId))||null}\nfunction groupIsActive(g){return g ? g.active !== false : false}\nasync function loadGroups(){\n  const r=await api(\'/api/subs\');\n  __subGroups=(r&&r.subs)||[];\n  const cSubGroup=document.getElementById(\'cSubGroup\');\n  if(cSubGroup){\n    const current=cSubGroup.value;\n    cSubGroup.innerHTML=\'<option value="">بدون گروه (عمومی)</option>\'+__subGroups.map(g=>`<option value="${esc(g.sub_id)}">${esc(g.name||\'گروه\')}</option>`).join(\'\');\n    if(current && __subGroups.some(g=>String(g.sub_id)===String(current))) cSubGroup.value=current;\n  }\n  const activeUsers=__subGroups.reduce((n,g)=>n+Number(g.active_count||0),0);\n  const total=document.getElementById(\'groupTotal\'); if(total) total.textContent=__subGroups.length;\n  const au=document.getElementById(\'groupActiveUsers\'); if(au) au.textContent=activeUsers;\n  const af=document.querySelector(\'.group-filter[data-filter="active"] em\'); if(af) af.title=String(__subGroups.filter(groupIsActive).length);\n  const inf=document.querySelector(\'.group-filter[data-filter="inactive"] em\'); if(inf) inf.title=String(__subGroups.filter(g=>!groupIsActive(g)).length);\n  if(!__selectedSubId || !selectedSub()) __selectedSubId=__subGroups[0]?.sub_id||\'\';\n  renderGroupList();\n  if(__selectedSubId) await loadGroupDetail(__selectedSubId);\n  else renderGroupDetail(null);\n}\nfunction renderGroupList(){\n  const box=document.getElementById(\'groupsList\'); if(!box)return;\n  const q=(document.getElementById(\'groupSearch\')?.value||\'\').trim().toLowerCase();\n  const list=__subGroups.filter(g=>{const active=groupIsActive(g); if(__groupFilter===\'active\'&&!active)return false; if(__groupFilter===\'inactive\'&&active)return false; return !q||String(g.name||\'\').toLowerCase().includes(q)||String(g.desc||\'\').toLowerCase().includes(q)});\n  if(!list.length){box.innerHTML=\'<div class="group-empty">گروهی مطابق جستجو پیدا نشد</div>\';return}\n  box.innerHTML=list.map(g=>{\n    const on=groupIsActive(g), selected=String(g.sub_id)===String(__selectedSubId);\n    return `<article class="group-card ${selected?\'selected\':\'\'}" onclick="selectGroup(\'${esc(g.sub_id)}\')">\n      <div class="group-card-top"><div class="group-card-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="9" cy="9" r="3"/><circle cx="17" cy="10" r="2.5"/><path d="M3.5 20c.5-3.1 2.4-4.7 5.5-4.7s5 1.6 5.5 4.7"/><path d="M14 15.8c2.8-.8 5 .5 6 3.2"/></svg></div><div class="group-card-main"><div class="group-card-title"><b>${esc(g.name||\'گروه\')}</b><span class="group-status ${on?\'\':\'off\'}">${on?\'● فعال\':\'● غیرفعال\'}</span></div><div class="group-card-desc">${esc(g.desc||\'گروه اشتراک ONEX\')}</div></div><div class="group-card-menu">•••</div></div>\n      <div class="group-card-meta"><span><strong>${Number(g.active_count||0)}</strong> کاربر فعال</span><span>•</span><span><strong>${Number(g.links_count||0)}</strong> کانفیگ</span><span>•</span><span>${g.has_password?\'🔒 رمزدار\':\'عمومی\'}</span></div>\n    </article>`;\n  }).join(\'\');\n}\nasync function selectGroup(id){__selectedSubId=id;renderGroupList();await loadGroupDetail(id)}\nasync function loadGroupDetail(id){\n  const g=__subGroups.find(x=>String(x.sub_id)===String(id)); if(!g){renderGroupDetail(null);return}\n  let links=window.__allLinks;\n  if(!Array.isArray(links)||!links.length){const r=await api(\'/api/links\');links=(r&&r.links)||[];window.__allLinks=links}\n  if(!Array.isArray(window.__protocolList)||!window.__protocolList.length){try{await loadProtocols()}catch(e){}}\n  __groupProtocols=Array.isArray(g.protocols)?g.protocols.slice():((window.__protocolList||[]).map(x=>x.id));\n  renderGroupDetail(g,links||[]);\n}\nfunction renderGroupDetail(g,links){\n  const pane=document.getElementById(\'groupDetailPane\'); if(!pane)return;\n  if(!g){pane.innerHTML=\'<div class="group-detail-empty"><div class="group-detail-empty-icon">◉</div><b>یک گروه را انتخاب کنید</b><span>برای مشاهده لینک اشتراک، پروتکل\u200cها و کانفیگ\u200cهای گروه</span></div>\';return}\n  const protocols=(window.__protocolList&&window.__protocolList.length?window.__protocolList.map(x=>x.id):[\'vless-ws\',\'xhttp-packet-up\',\'xhttp-stream-up\',\'xhttp-stream-one\',\'trojan\']);\n  const current=new Set(__groupProtocols.length?__groupProtocols:protocols);\n  const memberIds=new Set((g.link_ids||[]).map(String));\n  const allLinks=Array.isArray(links)?links:[];\n  const protoRows=protocols.map(id=>{const checked=current.has(id),icon=groupProtocolIcon(id);return `<div class="group-proto-row"><div class="group-proto-icon">${icon?`<img src="${icon}" alt="">`:\'◈\'}</div><div class="group-proto-copy"><b>${esc(groupProtocolLabel(id))}</b><small>${checked?\'پروتکل مجاز برای این گروه\':\'در این گروه نمایش داده نمی\u200cشود\'}</small></div><span class="group-proto-tag">${id.startsWith(\'vless\')||id.startsWith(\'xhttp\')?\'ONEX\':\'Native\'}</span><label class="group-switch"><input type="checkbox" ${checked?\'checked\':\'\'} onchange="toggleGroupProtocol(\'${esc(id)}\',this.checked)"><span></span></label></div>`}).join(\'\');\n  const configRows=allLinks.slice().sort((a,b)=>String(a.label||\'\').localeCompare(String(b.label||\'\'))).map(l=>{const id=String(l.uuid||l.id||\'\');const checked=memberIds.has(id);return `<label class="group-config-row"><input type="checkbox" class="group-config-check" value="${esc(id)}" ${checked?\'checked\':\'\'}><span>${esc(l.label||id)}</span><small>${esc(groupProtocolLabel(l.protocol||\'\'))}</small></label>`}).join(\'\');\n  pane.innerHTML=`<div class="group-detail">\n    <div class="group-detail-head"><div class="group-detail-icon"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="9" cy="9" r="3"/><circle cx="17" cy="10" r="2.5"/><path d="M3.5 20c.5-3.1 2.4-4.7 5.5-4.7s5 1.6 5.5 4.7"/><path d="M14 15.8c2.8-.8 5 .5 6 3.2"/></svg></div><div class="group-detail-title"><h2>${esc(g.name||\'گروه\')}</h2><p>${esc(g.desc||\'گروه ویژه با بالاترین سرعت و پایداری\')}</p></div><button class="group-three-dot" onclick="openGroupModal(\'${esc(g.sub_id)}\')">•••</button></div>\n    <div class="group-info-card"><div class="group-section-head"><b>⚙ اطلاعات گروه</b><span>Group profile</span></div><div class="group-info-grid"><div class="group-info-item"><small>نام گروه</small><b>${esc(g.name||\'—\')}</b></div><div class="group-info-item"><small>وضعیت</small><b style="color:${groupIsActive(g)?\'#34d399\':\'var(--accent2)\'}">${groupIsActive(g)?\'فعال\':\'غیرفعال\'}</b></div><div class="group-info-item"><small>کاربران فعال</small><b>${Number(g.active_count||0)}</b></div><div class="group-info-item"><small>تعداد کانفیگ\u200cها</small><b>${Number(g.links_count||0)}</b></div></div></div>\n    <div class="group-link-card"><div class="group-section-head"><b>◉ لینک اشتراک گروه</b><span>${g.has_password?\'🔒 محافظت\u200cشده\':\'Public\'}</span></div><div class="group-link-line"><div class="group-link-url">${esc(g.sub_url||\'—\')}</div><button class="group-copy-btn" onclick="copyText(\'${esc(g.sub_url||\'\')}\')">کپی</button></div><div class="group-link-actions"><button onclick="openGroupQr(\'${esc(g.sub_url||\'\')}\',\'${esc(g.name||\'\')}\')">▦ QR کد</button><button onclick="window.open(\'${esc(g.public_url||g.sub_url||\'\')}\',\'_blank\')">↗ باز کردن لینک</button></div></div>\n    <div class="group-proto-card"><div class="group-section-head"><b>⚙ پروتکل\u200cهای فعال</b><span>کنترل خروجی اشتراک</span></div><div class="group-proto-list">${protoRows}</div></div>\n    <div class="group-configs-card"><div class="group-section-head"><b>کانفیگ\u200cهای گروه</b><span>${Number(g.links_count||0)} مورد</span></div><div class="group-config-list">${configRows||\'<div class="group-empty" style="padding:18px">هنوز کانفیگی در پنل وجود ندارد؛ ابتدا از بخش کانفیگ\u200cها یک کانفیگ بسازید.</div>\'}</div><button class="group-config-save" onclick="saveGroupConfigs(\'${esc(g.sub_id)}\')">ذخیره کانفیگ\u200cهای گروه</button></div>\n    <div class="group-manage-card"><div class="group-section-head"><b>مدیریت گروه</b><span>Group actions</span></div><div class="group-manage-actions"><button onclick="openGroupModal(\'${esc(g.sub_id)}\')">✎ ویرایش</button><button onclick="toggleGroupMembership(\'${esc(g.sub_id)}\')">${groupIsActive(g)?\'⏸ غیرفعال کردن\':\'▶ فعال کردن\'}</button><button onclick="deleteSubGroup(\'${esc(g.sub_id)}\')">♜ حذف</button></div></div>\n  </div>`;\n}\nasync function saveGroupConfigs(subId){\n  const ids=[...document.querySelectorAll(\'.group-config-check:checked\')].map(x=>x.value);\n  const r=await api(\'/api/subs/\'+encodeURIComponent(subId)+\'/sync\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({link_ids:ids})});\n  if(r){toast(lang===\'fa\'?\'کانفیگ\u200cهای گروه ذخیره شد\':\'Group configs saved\');await loadGroups();}\n}\nasync function toggleGroupProtocol(id,state){\n  const g=selectedSub(); if(!g)return;\n  const current=new Set(Array.isArray(g.protocols)?g.protocols:(window.__protocolList||[]).map(x=>x.id));\n  if(state) current.add(id); else current.delete(id);\n  const protocols=[...current];\n  const r=await api(\'/api/subs/\'+encodeURIComponent(g.sub_id),{method:\'PATCH\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({protocols})});\n  if(r){g.protocols=protocols;__groupProtocols=protocols;toast(state?\'پروتکل فعال شد\':\'پروتکل غیرفعال شد\');renderGroupDetail(g,window.__allLinks||[])}\n}\nasync function toggleGroupMembership(subId){\n  const g=__subGroups.find(x=>String(x.sub_id)===String(subId));if(!g)return;\n  const next=g.active===false;\n  const r=await api(\'/api/subs/\'+encodeURIComponent(subId),{method:\'PATCH\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({active:next})});\n  if(r){\n    g.active=next;\n    toast(next?\'گروه فعال شد\':\'گروه غیرفعال شد\');\n    renderGroupList();\n    renderGroupDetail(g,window.__allLinks||[]);\n  }\n}\nfunction openGroupModal(subId=\'\'){\n  const modal=document.getElementById(\'groupModal\');if(!modal)return;\n  modal.hidden=false;modal.classList.add(\'open\');\n  const g=__subGroups.find(x=>String(x.sub_id)===String(subId));\n  document.getElementById(\'groupModalTitle\').textContent=g?\'ویرایش گروه\':\'ساخت گروه جدید\';\n  document.getElementById(\'groupFormName\').value=g?.name||\'\';document.getElementById(\'groupFormDesc\').value=g?.desc||\'\';document.getElementById(\'groupFormPassword\').value=\'\';\n  modal.dataset.subId=g?.sub_id||\'\';\n}\nfunction closeGroupModal(){const m=document.getElementById(\'groupModal\');if(m){m.hidden=true;m.classList.remove(\'open\')}}\nasync function saveGroupForm(){\n  const m=document.getElementById(\'groupModal\');const name=document.getElementById(\'groupFormName\').value.trim();if(!name){toast(\'نام گروه لازم است\');return}\n  const subId=m?.dataset.subId||\'\';const body={name,desc:document.getElementById(\'groupFormDesc\').value.trim()};const pw=document.getElementById(\'groupFormPassword\').value.trim();if(pw)body.password=pw;\n  const r=await api(subId?\'/api/subs/\'+encodeURIComponent(subId):\'/api/subs\',{method:subId?\'PATCH\':\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(body)});\n  if(r){closeGroupModal();toast(subId?\'گروه ویرایش شد\':\'گروه ساخته شد\');await loadGroups()}\n}\nasync function deleteSubGroup(id){if(!confirm(\'این گروه حذف شود؟ کانفیگ\u200cها حذف نمی\u200cشوند و فقط از گروه خارج می\u200cشوند.\'))return;const r=await api(\'/api/subs/\'+encodeURIComponent(id),{method:\'DELETE\'});if(r){toast(\'گروه حذف شد\');__selectedSubId=\'\';await loadGroups()}}\nfunction openGroupQr(url,label){const m=document.getElementById(\'groupQrModal\'),box=document.getElementById(\'groupQrBox\'),txt=document.getElementById(\'groupQrText\');if(!m||!box||!url)return;txt.textContent=url;document.getElementById(\'groupQrTitle\').textContent=label||\'لینک اشتراک\';box.innerHTML=\'\';try{if(typeof qrcode===\'function\'){const qr=qrcode(0,\'M\');qr.addData(url);qr.make();box.innerHTML=qr.createImgTag(6,8)}else{box.innerHTML=\'<div style="color:#111;font:12px sans-serif;padding:30px">QR آماده نشد</div>\'}}catch(e){box.innerHTML=\'<div style="color:#111;font:12px sans-serif;padding:30px">خطا در تولید QR</div>\'}m.hidden=false;m.classList.add(\'open\')}\nfunction closeGroupQr(){const m=document.getElementById(\'groupQrModal\');if(m){m.hidden=true;m.classList.remove(\'open\')}}\n\n\nasync function loadSecurity(){\n  const r=await api(\'/api/security/status\');\n  if(!r)return;\n  const set=(id,val)=>{const e=document.getElementById(id);if(e)e.textContent=val};\n  set(\'secMaxAttempts\',r.max_attempts??5);\n  set(\'secLockMinutes\',`${Math.round((r.lockout_seconds??1800)/60)} دقیقه`);\n  set(\'secLockedCount\',(r.locked_ips||[]).length);\n  set(\'secFailedCount\',r.failed_attempts??0);\n  set(\'secSessionCount\',r.active_sessions??0);\n  const health=document.getElementById(\'secHealth\');\n  const risk=((r.locked_ips||[]).length>0)||(r.failed_attempts||0)>=Math.max(3,Math.floor((r.max_attempts||5)*.6));\n  if(health){health.className=`security-health ${risk?\'warn\':\'good\'}`;health.textContent=risk?\'● نیازمند بررسی\':\'● خوب\'}\n  const box=document.getElementById(\'secRecentLogins\');\n  if(box){\n    const rows=(r.recent_logins||[]).slice(0,8);\n    box.innerHTML=rows.length?rows.map(x=>`<div class="security-recent-row"><span class="security-recent-state ${x.success?\'ok\':\'bad\'}">${x.success?\'ورود موفق\':\'ورود ناموفق\'}</span><span class="security-recent-meta">IP: ${esc(x.ip||\'—\')} · ${esc(String(x.time||\'\').replace(\'T\',\' \').slice(0,19))}</span><span class="security-recent-meta">${esc((x.message||\'\').replace(/.*?(?:از|IP:?)[ ]*/,\'\').slice(0,35))}</span></div>`).join(\'\'):\'<div class="security-empty">هنوز ورود ثبت\u200cشده\u200cای وجود ندارد.</div>\';\n  }\n  window.__securitySnapshot=r;\n}\nfunction showSecurityDetails(){\n  const box=document.getElementById(\'secDetails\'),body=document.getElementById(\'secDetailsBody\'),r=window.__securitySnapshot;\n  if(!box||!body||!r)return;\n  const locked=(r.locked_ips||[]);\n  const sessions=(r.sessions||[]);\n  body.innerHTML=`<div class="security-detail-row"><span>IPهای در حال ردیابی</span><b>${r.tracked_ips??0}</b></div><div class="security-detail-row"><span>تلاش\u200cهای ناموفق در پنجره فعلی</span><b>${r.failed_attempts??0}</b></div><div class="security-detail-row"><span>نشست\u200cهای فعال</span><b>${sessions.length}</b></div><div class="security-detail-row"><span>IPهای مسدود</span><b>${locked.length?locked.map(x=>`${esc(x.ip)} (${Math.ceil((x.remaining_sec||0)/60)}د)`).join(\' · \'):\'—\'}</b></div>`;\n  box.hidden=false;\n}\nfunction hideSecurityDetails(){const box=document.getElementById(\'secDetails\');if(box)box.hidden=true}\nfunction showBlockedIps(){\n  const r=window.__securitySnapshot||{};\n  const locked=r.locked_ips||[];\n  if(!locked.length){toast(\'هیچ IP مسدودی وجود ندارد\');return}\n  const text=locked.map(x=>`${x.ip} — ${Math.ceil((x.remaining_sec||0)/60)} دقیقه باقی\u200cمانده`).join(\'\\n\');\n  const ip=prompt(`IP موردنظر را برای رفع مسدودی وارد کنید:\\n\\n${text}`,\'\');\n  if(ip===null)return;\n  if(!ip.trim()){toast(\'IP وارد نشد\');return}\n  unlockIp(ip.trim());\n}\nasync function unlockIp(ip){\n  const r=await api(\'/api/security/unlock\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({ip})});\n  if(r){toast(\'مسدودی IP رفع شد\');loadSecurity()}\n}\nasync function unlockAllIps(){\n  if(!confirm(\'رفع مسدودی همه IPها انجام شود؟\'))return;\n  const r=await api(\'/api/security/unlock\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({})});\n  if(r){toast(\'مسدودی همه IPها رفع شد\');loadSecurity()}\n}\nasync function revokeAllSessions(){\n  if(!confirm(\'از همه دستگاه\u200cها خارج شوید؟ نشست فعلی حفظ می\u200cشود.\'))return;\n  const r=await api(\'/api/security/sessions/revoke\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify({keep_current:true})});\n  if(r){toast(`${r.revoked||0} نشست قطع شد`);loadSecurity()}\n}\n\n\nasync function downloadBackup(kind){\n  try{\n    const url = kind===\'full\' ? \'/api/backup/full\' : (kind===\'bot\' ? \'/api/backup/bot\' : \'/api/backup/users\');\n    const r = await fetch(url, {credentials:\'same-origin\', cache:\'no-store\'});\n    if(r.status===401){ location.href=\'/login\'; return; }\n    if(!r.ok){\n      let msg=\'خطا\';\n      try{ const j=await r.json(); msg=j.detail||msg; }catch(e){}\n      toast(String(msg)); return;\n    }\n    const text = await r.text();\n    try{ JSON.parse(text); }catch(e){ toast(\'پاسخ نامعتبر\'); return; }\n    const blob = new Blob([text], {type:\'application/json;charset=utf-8\'});\n    const a = document.createElement(\'a\');\n    const stamp = new Date().toISOString().slice(0,19).replace(/[:T]/g,\'-\');\n    a.href = URL.createObjectURL(blob);\n    a.download = kind===\'full\' ? (\'ONEX-backup-\'+stamp+\'.json\') : (kind===\'bot\' ? (\'ONEX-bot-\'+stamp+\'.json\') : (\'ONEX-users-\'+stamp+\'.json\'));\n    document.body.appendChild(a);\n    a.click();\n    setTimeout(()=>{ URL.revokeObjectURL(a.href); a.remove(); }, 500);\n    toast(lang===\'fa\'?\'دانلود شد\':\'Downloaded\');\n  }catch(e){ toast(String(e.message||e)); }\n}\nasync function restoreFull(){\n  try{\n    const data = await readJsonFile(\'restoreFullFile\');\n    if(data.type!==\'onex_full_backup\'){\n      toast(lang===\'fa\'?\'این فایل بک\u200cآپ کامل ONEX نیست\':\'This is not a full ONEX backup\');\n      return;\n    }\n    if(!confirm(lang===\'fa\'?\'تمام اطلاعات فعلی پنل و تنظیمات ربات با بک\u200cآپ جایگزین می\u200cشود. مطمئنی؟\':\'All current panel and bot data will be replaced. Continue?\')) return;\n    const r = await api(\'/api/restore/full\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(data)});\n    if(r){\n      toast(r.warning || r.message || (lang===\'fa\'?\'بک\u200cآپ کامل بازیابی شد\':\'Full backup restored\'));\n      setTimeout(()=>location.reload(), 900);\n    }\n  }catch(e){ toast(e.message||String(e)); }\n}\nfunction readJsonFile(inputId){\n  return new Promise((resolve,reject)=>{\n    const inp=document.getElementById(inputId);\n    if(!inp||!inp.files||!inp.files[0]){ reject(new Error(lang===\'fa\'?\'فایل انتخاب نشده\':\'No file\')); return; }\n    const fr=new FileReader();\n    fr.onload=()=>{ try{ resolve(JSON.parse(fr.result)); }catch(e){ reject(new Error(\'JSON نامعتبر\')); } };\n    fr.onerror=()=>reject(new Error(\'خواندن فایل ناموفق\'));\n    fr.readAsText(inp.files[0],\'utf-8\');\n  });\n}\nasync function restoreUsers(mode){\n  try{\n    const data = await readJsonFile(\'restoreUsersFile\');\n    data.mode = mode||\'merge\';\n    if(mode===\'replace\' && !confirm(lang===\'fa\'?\'همه داده\u200cهای فعلی پاک و جایگزین می\u200cشود. مطمئنی؟\':\'Replace all current data?\')) return;\n    const r = await api(\'/api/restore/users\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(data)});\n    if(r){ toast(lang===\'fa\'?(\'بازیابی شد: \'+r.links+\' کانفیگ\'):(\'Restored: \'+r.links)); refreshAll(); }\n  }catch(e){ toast(e.message||String(e)); }\n}\nasync function restoreBot(){\n  try{\n    const data = await readJsonFile(\'restoreBotFile\');\n    const r = await api(\'/api/restore/bot\',{method:\'POST\',headers:{\'Content-Type\':\'application/json\'},body:JSON.stringify(data)});\n    if(r) toast(r.message||(lang===\'fa\'?\'ربات بازیابی شد\':\'Bot restored\'));\n  }catch(e){ toast(e.message||String(e)); }\n}\n\n\n/* ============================================================\n   LIGHT STATIC 3D PROTOCOL PICKER\n   ============================================================ */\nconst RAILWAY_SUB_PROTOCOLS = (window.__protocolList||[]).map(p=>p.id);\nconst PROTOCOL_PICKER_GROUPS=[{title:"پروتکل\u200cهای پروژه اصلی",ids:["vless-ws","xhttp-packet-up","xhttp-stream-up","xhttp-stream-one","trojan"]}];\nconst PROTOCOL_PICKER_NAMES={"vless-ws":"ONEX WB","xhttp-packet-up":"ONEX Xhttp","xhttp-stream-up":"ONEX Gaming","xhttp-stream-one":"ONEX Stream","trojan":"Trojan"};\nconst PROTOCOL_PICKER_DESCS={"vless-ws":"VLESS + WebSocket","xhttp-packet-up":"VLESS + XHTTP (Packet-Up)","xhttp-stream-up":"VLESS + XHTTP (Gaming / Stream-Up)","xhttp-stream-one":"VLESS + XHTTP (Stream-One)","trojan":"Trojan"};\nconst PROTOCOL_ICON_DATA={"vless-ws":"/api/protocol-icon/vless-ws.png","xhttp-packet-up":"/api/protocol-icon/xhttp-packet-up.png","xhttp-stream-up":"/api/protocol-icon/xhttp-stream-up.png","xhttp-stream-one":"/api/protocol-icon/xhttp-stream-one.png","trojan":"/api/protocol-icon/trojan.png"};\nfunction protocolPickerLabel(id){const p=__protocolPickerOptions.find(x=>x.id===id);return PROTOCOL_PICKER_NAMES[id]||p?.label||id||\'Vortex Link\'}\nfunction protocolPickerShort(id){return PROTOCOL_PICKER_NAMES[id]||id}\nfunction protocolIconMarkup(id){\n  const src=PROTOCOL_ICON_DATA[id]||PROTOCOL_ICON_DATA["vless-ws"];\n  return `<span class="protocol-option-icon proto-3d" aria-hidden="true"><img class="protocol-art-icon" src="${src}" alt="" loading="eager" decoding="async"></span>`\n}\nfunction setupProtocolPickers(){[\'cProto\',\'aProto\'].forEach(id=>{const sel=document.getElementById(id);if(!sel)return;sel.classList.add(\'protocol-native\');sel.style.setProperty(\'display\',\'none\',\'important\');sel.setAttribute(\'aria-hidden\',\'true\');let trigger=sel.parentNode.querySelector(`.protocol-trigger[data-for="${id}"]`);if(!trigger){trigger=document.createElement(\'button\');trigger.type=\'button\';trigger.className=\'protocol-trigger\';trigger.dataset.for=id;sel.parentNode.insertBefore(trigger,sel.nextSibling)}trigger.onclick=e=>{e.preventDefault();openProtocolPicker(id)};syncProtocolPicker(id)})}\nfunction syncProtocolPicker(id){const sel=document.getElementById(id),trigger=document.querySelector(`.protocol-trigger[data-for="${id}"]`);if(!sel||!trigger)return;const value=sel.value||\'vless-ws\';trigger.innerHTML=`<span class="protocol-trigger-main"><span class="protocol-trigger-icon">${protocolIconMarkup(value)}</span><span class="protocol-trigger-text"><span class="protocol-trigger-name">${esc(protocolPickerShort(value))}</span><span class="protocol-trigger-sub">${lang===\'fa\'?\'برای تغییر، انتخاب کنید\':\'Tap to choose another protocol\'}</span></span></span><span class="protocol-trigger-arrow">⌄</span>`}\nfunction syncAllProtocolToggle(){const sel=document.getElementById(\'cProto\'),all=document.getElementById(\'cAllProtocols\'),wrap=all?.closest(\'.all-proto-toggle\');if(!sel||!all)return;const railway=RAILWAY_SUB_PROTOCOLS.includes(sel.value);if(!railway){all.checked=false;all.disabled=true;if(wrap){wrap.style.opacity=\'0.48\';wrap.style.cursor=\'not-allowed\';wrap.title=lang===\'fa\'?\'این گزینه فقط برای پروتکل\u200cهای Railway است\':\'This option is only for Railway protocols\';}}else{all.disabled=false;if(wrap){wrap.style.opacity=\'1\';wrap.style.cursor=\'pointer\';wrap.title=lang===\'fa\'?\'هر ۶ پروتکل ONEX VIP در یک ساب\':\'All 6 ONEX VIP protocols in one subscription\';}}}\n\nfunction ensureProtocolPicker(){let bg=document.getElementById(\'protocolPickerBg\');if(bg)return bg;bg=document.createElement(\'div\');bg.id=\'protocolPickerBg\';bg.className=\'protocol-picker-bg\';bg.innerHTML=`<div class="protocol-picker" role="dialog" aria-modal="true"><div class="protocol-picker-head"><div class="protocol-picker-head-icon"><span>✦</span></div><div class="protocol-picker-head-text"><div class="protocol-picker-title">${lang===\'fa\'?\'انتخاب پروتکل\':\'Select Protocol\'}</div><div class="protocol-picker-subtitle">${lang===\'fa\'?\'پروتکل موردنظر را انتخاب کنید\':\'Choose the protocol you want to use\'}</div></div><button type="button" class="protocol-picker-close" id="protocolPickerClose">×</button></div><div class="protocol-picker-scroll" id="protocolPickerScroll"></div><div class="protocol-picker-foot"><div class="protocol-selected-info" id="protocolSelectedInfo">—</div><button type="button" class="protocol-picker-confirm" id="protocolPickerConfirm">${lang===\'fa\'?\'تأیید و ادامه →\':\'Confirm & Continue →\'}</button></div></div>`;document.body.appendChild(bg);bg.addEventListener(\'click\',e=>{if(e.target===bg)closeProtocolPicker()});bg.querySelector(\'#protocolPickerClose\').onclick=closeProtocolPicker;bg.querySelector(\'#protocolPickerConfirm\').onclick=confirmProtocolPicker;return bg}\nfunction openProtocolPicker(targetId){const sel=document.getElementById(targetId);if(!sel)return;const bg=ensureProtocolPicker();__protocolPickerTarget=targetId;const current=sel.value||\'vless-ws\';const available=new Set([...sel.options].map(o=>o.value));const sections=PROTOCOL_PICKER_GROUPS.map(g=>{const ids=g.ids.filter(id=>available.has(id));if(!ids.length)return \'\';return `<section class="protocol-picker-section ${g.kind||\'\'}"><div class="protocol-picker-section-head"><div><b>${esc(g.title)}</b><small>${esc(g.subtitle||\'\')}</small></div><span>${ids.length}</span></div><div class="protocol-grid protocol-grid-all">${ids.map(id=>`<button type="button" class="protocol-option ${id===current?\'selected\':\'\'}" data-proto="${id}"><span class="protocol-option-radio"></span>${protocolIconMarkup(id)}<span class="protocol-option-name">${esc(protocolPickerShort(id))}</span><span class="protocol-option-desc">${id===current?(lang===\'fa\'?\'انتخاب\u200cشده · \':\'Selected · \')+(PROTOCOL_PICKER_DESCS[id]||\'\'):(PROTOCOL_PICKER_DESCS[id]|| (lang===\'fa\'?\'برای انتخاب کلیک کنید\':\'Tap to choose\'))}</span></button>`).join(\'\')}</div></section>`}).join(\'\');const scroll=bg.querySelector(\'#protocolPickerScroll\');scroll.innerHTML=sections;scroll.querySelectorAll(\'.protocol-option\').forEach(btn=>btn.addEventListener(\'click\',()=>chooseProtocol(btn.dataset.proto)));bg.querySelector(\'#protocolSelectedInfo\').textContent=(lang===\'fa\'?\'پروتکل انتخاب\u200cشده: \':\'Selected: \')+protocolPickerShort(current);bg.classList.add(\'open\');document.body.style.overflow=\'hidden\'}\nfunction chooseProtocol(id){const sel=document.getElementById(__protocolPickerTarget),bg=document.getElementById(\'protocolPickerBg\');if(!sel||!bg)return;sel.value=id;bg.querySelectorAll(\'.protocol-option\').forEach(x=>x.classList.toggle(\'selected\',x.dataset.proto===id));bg.querySelector(\'#protocolSelectedInfo\').textContent=(lang===\'fa\'?\'پروتکل انتخاب\u200cشده: \':\'Selected: \')+protocolPickerShort(id);syncProtocolPicker(__protocolPickerTarget);sel.dispatchEvent(new Event(\'change\',{bubbles:true}));if(__protocolPickerTarget===\'cProto\')syncAllProtocolToggle()}\nfunction confirmProtocolPicker(){if(__protocolPickerTarget){const sel=document.getElementById(__protocolPickerTarget);if(sel)sel.dispatchEvent(new Event(\'change\',{bubbles:true}))}closeProtocolPicker()}\nfunction closeProtocolPicker(){const bg=document.getElementById(\'protocolPickerBg\');if(bg)bg.classList.remove(\'open\');document.body.style.overflow=\'\'}\ndocument.addEventListener(\'keydown\',e=>{if(e.key===\'Escape\')closeProtocolPicker()});\nif(document.readyState===\'loading\')document.addEventListener(\'DOMContentLoaded\',()=>{setupProtocolPickers();syncAllProtocolToggle()});else {setupProtocolPickers();syncAllProtocolToggle();}setTimeout(setupProtocolPickers,300);setTimeout(setupProtocolPickers,1000);\n\napplyLang();loadMe();loadProtocols();loadCategories();loadGroups();refreshAll();setTimeout(()=>{if(document.getElementById(\'advancedPorts\')&&!getAdvancedPorts().length)fillAdvancedForm({ports:[443]});loadAdvancedCapabilities(document.getElementById(\'cProto\')?.value||\'vless-ws\')},250);\nloadAdBlocker();\nsetTimeout(()=>{startUpdateNotificationPolling()},1200);\nsetTimeout(()=>checkPanelUpdate(true),2500);\nsetInterval(()=>checkPanelUpdate(false),10*60*1000);\n// Protocol picker bootstrap: keep the native select only as the data/control source.\nfunction bootProtocolPickers(){ try{ setupProtocolPickers(); }catch(e){ console.warn(\'Protocol picker:\',e); } }\nif(document.readyState===\'loading\') document.addEventListener(\'DOMContentLoaded\',bootProtocolPickers); else bootProtocolPickers();\nsetTimeout(bootProtocolPickers,300);\nsetTimeout(bootProtocolPickers,1000);\nsetInterval(()=>{if(document.hidden)return;const pg=document.querySelector(\'.page.on\');const id=pg?pg.id:\'\';if(id===\'page-dash\'||id===\'page-configs\'||id===\'page-stats\')refreshAll()},8000);\n\n// ============================================================\n// ONEX THEME SYSTEM — persistent presets + custom palette\n// ============================================================\nconst ONEX_THEMES={\n  blue:{name:\'ONEX Blue\',p:\'#3b82f6\',s:\'#8b5cf6\',bg:\'#06060b\',card:\'#12121c\',mode:\'dark\'},\n  purple:{name:\'Cyber Purple\',p:\'#8b5cf6\',s:\'#ec4899\',bg:\'#080611\',card:\'#171126\',mode:\'dark\'},\n  green:{name:\'Emerald\',p:\'#10b981\',s:\'#06b6d4\',bg:\'#04100c\',card:\'#0d1d18\',mode:\'dark\'},\n  cyan:{name:\'Ocean\',p:\'#06b6d4\',s:\'#3b82f6\',bg:\'#031015\',card:\'#0b1b24\',mode:\'dark\'},\n  orange:{name:\'Sunset\',p:\'#f97316\',s:\'#ef4444\',bg:\'#120906\',card:\'#21120d\',mode:\'dark\'},\n  rose:{name:\'Rose\',p:\'#f43f5e\',s:\'#ec4899\',bg:\'#12060b\',card:\'#211019\',mode:\'dark\'},\n  amber:{name:\'Amber\',p:\'#f59e0b\',s:\'#ef4444\',bg:\'#110b03\',card:\'#21170a\',mode:\'dark\'},\n  lime:{name:\'Lime\',p:\'#84cc16\',s:\'#10b981\',bg:\'#081003\',card:\'#14200a\',mode:\'dark\'},\n  indigo:{name:\'Indigo\',p:\'#6366f1\',s:\'#a855f7\',bg:\'#070714\',card:\'#11112a\',mode:\'dark\'},\n  pink:{name:\'Pink\',p:\'#ec4899\',s:\'#8b5cf6\',bg:\'#12050e\',card:\'#211020\',mode:\'dark\'},\n  midnight:{name:\'Midnight\',p:\'#60a5fa\',s:\'#6366f1\',bg:\'#030712\',card:\'#0b1222\',mode:\'dark\'},\n  light:{name:\'Light\',p:\'#2563eb\',s:\'#7c3aed\',bg:\'#eef1f8\',card:\'#ffffff\',mode:\'light\'}\n};\nconst ONEX_THEME_LABELS={blue:\'آبی ONEX\',purple:\'بنفش سایبری\',green:\'زمردی\',cyan:\'اقیانوسی\',orange:\'نارنجی غروب\',rose:\'رز\',amber:\'کهربایی\',lime:\'لیمویی\',indigo:\'نیلی\',pink:\'صورتی\',midnight:\'نیمه\u200cشب\',light:\'روشن\'};\nfunction hexRgb(hex){const h=String(hex||\'\').replace(\'#\',\'\');const v=h.length===3?h.split(\'\').map(x=>x+x).join(\'\'):h;if(!/^[0-9a-fA-F]{6}$/.test(v))return [59,130,246];return [parseInt(v.slice(0,2),16),parseInt(v.slice(2,4),16),parseInt(v.slice(4,6),16)]}\nfunction mixHex(hex,amount){const [r,g,b]=hexRgb(hex);const t=amount<0?0:255;const a=Math.abs(amount);return \'#\'+[r,g,b].map(v=>Math.round(v+(t-v)*a).toString(16).padStart(2,\'0\')).join(\'\')}\nfunction applyOnexTheme(key, opts){\n  let t=ONEX_THEMES[key]||ONEX_THEMES.blue;\n  if(opts) t={...t,...opts};\n  // Light mode must never inherit the dark preset background/card colors.\n  // Every preset ships dark bg/card values; in light mode derive a soft tinted\n  // light surface from the accent instead, so every card/section stays white.\n  if(t.mode===\'light\'){\n    const lumOf=h=>{const [r,g,b]=hexRgb(h);return (r*.299+g*.587+b*.114)/255};\n    if(lumOf(t.bg||\'#000\')<.6) t={...t,bg:mixHex(t.p||\'#2563eb\',.94)};\n    if(lumOf(t.card||\'#000\')<.6) t={...t,card:\'#ffffff\'};\n  }\n  const root=document.documentElement;\n  const [pr,pg,pb]=hexRgb(t.p);\n  const [sr,sg,sb]=hexRgb(t.s);\n  const bg2=t.mode===\'light\'?mixHex(t.bg,.7):mixHex(t.bg,.10);\n  const bg3=t.mode===\'light\'?mixHex(t.bg,.82):mixHex(t.bg,.18);\n  root.style.setProperty(\'--bg\',t.bg);root.style.setProperty(\'--bg2\',bg2);root.style.setProperty(\'--bg3\',bg3);root.style.setProperty(\'--card\',t.card);\n  root.style.setProperty(\'--accent\',t.p);root.style.setProperty(\'--accent2\',mixHex(t.p,.28));root.style.setProperty(\'--purple\',t.s);\n  root.style.setProperty(\'--hover\',`rgba(${pr},${pg},${pb},.12)`);root.style.setProperty(\'--glow\',`0 0 40px rgba(${pr},${pg},${pb},.12)`);\n  // Keep every page readable when the selected palette changes. These tokens are\n  // consumed by the final global theme layer below, including legacy cards that\n  // still contain fixed colors in their original component CSS.\n  if(t.mode===\'light\'){\n    root.style.setProperty(\'--p\',t.p);root.style.setProperty(\'--s\',t.s);root.style.setProperty(\'--p-rgb\',`${pr} ${pg} ${pb}`);root.style.setProperty(\'--s-rgb\',`${sr} ${sg} ${sb}`);\n    root.style.setProperty(\'--t1\',\'#0f172a\');root.style.setProperty(\'--t2\',\'#334155\');root.style.setProperty(\'--t3\',\'#64748b\');\n    root.style.setProperty(\'--card-b\',\'rgba(15,23,42,.10)\');root.style.setProperty(\'--input-bg\',\'#f8fafc\');\n  }else{\n    [\'--p\',\'--s\',\'--p-rgb\',\'--s-rgb\'].forEach(k=>root.style.removeProperty(k));\n    root.style.setProperty(\'--t1\',\'#f8fafc\');root.style.setProperty(\'--t2\',\'rgba(248,250,252,.72)\');root.style.setProperty(\'--t3\',\'rgba(248,250,252,.48)\');\n    root.style.setProperty(\'--card-b\',\'rgba(148,163,184,.16)\');root.style.setProperty(\'--input-bg\',\'rgba(2,10,24,.62)\');\n  }\n  root.style.setProperty(\'--action-red\',t.p);root.style.setProperty(\'--action-red-2\',t.s);root.style.setProperty(\'--action-red-soft\',`rgba(${pr},${pg},${pb},.12)`);\n  root.style.setProperty(\'--theme-preview\',t.p);root.style.setProperty(\'--theme-preview2\',t.s);\n  root.classList.add(\'onex-themed\');\n  if(t.mode===\'light\') root.classList.add(\'light\'); else root.classList.remove(\'light\');\n  document.body.dataset.onexTheme=key||\'custom\';\n  localStorage.setItem(\'onex_theme_v2\',JSON.stringify({key:key||\'custom\',...t}));\n  updateOnexThemeUI(key||\'custom\',t);\n}\nfunction applyCustomOnexTheme(){\n  const t={name:lang===\'fa\'?\'سفارشی\':\'Custom\',p:document.getElementById(\'themePrimary\')?.value||\'#3b82f6\',s:document.getElementById(\'themeSecondary\')?.value||\'#8b5cf6\',bg:document.getElementById(\'themeBackground\')?.value||\'#06060b\',card:document.getElementById(\'themeCard\')?.value||\'#12121c\',mode:document.documentElement.classList.contains(\'light\')?\'light\':\'dark\'};\n  applyOnexTheme(\'custom\',t);\n  toast(lang===\'fa\'?\'تم سفارشی اعمال شد\':\'Custom theme applied\');\n}\nfunction setOnexThemeMode(mode){\n  let saved;try{saved=JSON.parse(localStorage.getItem(\'onex_theme_v2\')||\'{}\')}catch(e){saved={}}\n  const key=saved.key&&(ONEX_THEMES[saved.key]||saved.key===\'custom\')?saved.key:\'aurora\';\n  if(mode===\'system\'){const light=window.matchMedia&&window.matchMedia(\'(prefers-color-scheme: light)\').matches;mode=light?\'light\':\'dark\'}\n  applyOnexTheme(key,{...saved,mode});\n  updateThemeModeButtons(mode);\n}\nfunction updateThemeModeButtons(mode){document.querySelectorAll(\'.theme-mode-buttons button\').forEach(b=>b.classList.toggle(\'on\',b.dataset.mode===mode||(b.dataset.mode===\'system\'&&mode===\'system\')))}\nfunction updateOnexThemeUI(key,t){\n  const name=document.getElementById(\'themeCurrentName\');if(name)name.textContent=t.name||ONEX_THEME_LABELS[key]||\'Custom\';\n  const sw=document.getElementById(\'themeCurrentSwatch\');if(sw)sw.style.background=`linear-gradient(135deg,${t.p},${t.s})`;\n  const p=document.getElementById(\'themePrimary\'),s=document.getElementById(\'themeSecondary\'),bg=document.getElementById(\'themeBackground\'),c=document.getElementById(\'themeCard\');\n  if(p)p.value=t.p;if(s)s.value=t.s;if(bg)bg.value=t.bg;if(c)c.value=t.card;\n  document.querySelectorAll(\'.theme-preset\').forEach(x=>x.classList.toggle(\'on\',x.dataset.theme===key));\n  const mode=t.mode||\'dark\';updateThemeModeButtons(mode);\n}\nfunction renderOnexThemePresets(){\n  const box=document.getElementById(\'themePresetGrid\');if(!box)return;\n  box.innerHTML=Object.entries(ONEX_THEMES).map(([key,t])=>`<button type="button" class="theme-preset" data-theme="${key}" style="--theme-preview:${t.p};--theme-preview2:${t.s}" onclick="selectOnexTheme(\'${key}\')"><span class="theme-preset-dot"></span><b>${ONEX_THEME_LABELS[key]||t.name}</b><small>${t.name}</small><span class="theme-preset-check">✓</span></button>`).join(\'\');\n  let saved={};try{saved=JSON.parse(localStorage.getItem(\'onex_theme_v2\')||\'{}\')}catch(e){}\n  const key=saved.key&&ONEX_THEMES[saved.key]?saved.key:\'blue\';box.querySelectorAll(\'.theme-preset\').forEach(x=>x.classList.toggle(\'on\',x.dataset.theme===key));\n}\nfunction selectOnexTheme(key){const t=ONEX_THEMES[key]||ONEX_THEMES.blue;applyOnexTheme(key,t);toast(lang===\'fa\'?`تم «${ONEX_THEME_LABELS[key]||t.name}» اعمال شد`:`${t.name} theme applied`)}\nfunction resetOnexTheme(){applyOnexTheme(\'blue\',ONEX_THEMES.blue);toast(lang===\'fa\'?\'تم به حالت پیش\u200cفرض برگشت\':\'Theme reset to default\')}\nfunction loadOnexTheme(){\n  let saved;try{saved=JSON.parse(localStorage.getItem(\'onex_theme_v2\')||\'{}\')}catch(e){saved=null}\n  if(saved&&saved.p){applyOnexTheme(saved.key||\'custom\',saved);return}\n  applyOnexTheme(\'blue\',ONEX_THEMES.blue);\n}\nwindow.addEventListener(\'storage\',e=>{if(e.key===\'onex_theme_v2\')loadOnexTheme()});\n/* initial theme load happens in the ONEX THEME BRIDGE below */\n\n\n/* ============================================================\n   ONEX THEME BRIDGE — one theme for login, panel & public pages\n   (login presets + panel presets share the same storage keys)\n   ============================================================ */\n(function(){\n  const LOGIN={aurora:{name:\'Aurora\',fa:\'شفق\',p:\'#38d9ff\',s:\'#8b5cf6\'},lime:{name:\'Lime\',fa:\'لیمویی\',p:\'#d4f24a\',s:\'#ffb84d\'},rose:{name:\'Rose\',fa:\'سرخابی\',p:\'#ff3d8b\',s:\'#ff8a3d\'},emerald:{name:\'Emerald\',fa:\'زمردی\',p:\'#34f5a0\',s:\'#22c1ee\'},violet:{name:\'Violet\',fa:\'بنفش\',p:\'#b57bff\',s:\'#ff5ecf\'},gold:{name:\'Gold\',fa:\'طلایی\',p:\'#ffc94d\',s:\'#ff7a45\'}};\n  // Put the login presets first, keep every existing panel preset.\n  const old=Object.assign({},ONEX_THEMES);Object.keys(ONEX_THEMES).forEach(k=>delete ONEX_THEMES[k]);\n  Object.entries(LOGIN).forEach(([k,v])=>{ONEX_THEMES[k]={name:v.name,p:v.p,s:v.s,bg:\'#05060d\',card:\'#12162a\',mode:\'dark\'};ONEX_THEME_LABELS[k]=v.fa});\n  Object.entries(old).forEach(([k,v])=>{if(!ONEX_THEMES[k])ONEX_THEMES[k]=v});\n\n  const rgb=h=>hexRgb(h).join(\' \');\n  function syncTokens(key,t){\n    const root=document.documentElement,st=root.style,[r,g,b]=hexRgb(t.p);\n    st.setProperty(\'--p\',t.p);st.setProperty(\'--s\',t.s);st.setProperty(\'--p-rgb\',rgb(t.p));st.setProperty(\'--s-rgb\',rgb(t.s));\n    st.setProperty(\'--accent2\',t.s);\n    st.setProperty(\'--on\',(r*.299+g*.587+b*.114)/255>.62?\'#0b0d14\':\'#ffffff\');\n    root.dataset.theme=key;\n    try{\n      if(LOGIN[key]&&LOGIN[key].p.toLowerCase()===String(t.p).toLowerCase())localStorage.setItem(\'onex_theme\',key);\n      else{localStorage.setItem(\'onex_theme\',\'custom\');localStorage.setItem(\'onex_theme_custom\',t.p)}\n      localStorage.removeItem(\'onex_hexa_palette\');\n    }catch(e){}\n    const sw=document.getElementById(\'hexaSwatches\');\n    if(sw)sw.querySelectorAll(\'button\').forEach(x=>x.classList.toggle(\'active\',x.dataset.theme===key));\n  }\n  const _apply=applyOnexTheme;\n  applyOnexTheme=function(key,opts){_apply.apply(this,arguments);let t;try{t=JSON.parse(localStorage.getItem(\'onex_theme_v2\')||\'{}\')}catch(e){t={}}if(t&&t.p)syncTokens(t.key||key||\'custom\',t)};\n\n  // Default = Aurora (same as login); honour a theme chosen on the login screen.\n  resetOnexTheme=function(){applyOnexTheme(\'aurora\',ONEX_THEMES.aurora);toast(lang===\'fa\'?\'تم به حالت پیش\u200cفرض برگشت\':\'Theme reset to default\')};\n  loadOnexTheme=function(){\n    let saved=null;try{saved=JSON.parse(localStorage.getItem(\'onex_theme_v2\')||\'null\')}catch(e){}\n    let loginKey=null;try{loginKey=localStorage.getItem(\'onex_theme\')}catch(e){}\n    if(loginKey&&LOGIN[loginKey]&&(!saved||!saved.p||saved.key!==loginKey)){applyOnexTheme(loginKey,{...ONEX_THEMES[loginKey],mode:(saved&&saved.mode)||\'dark\'});return}\n    if(loginKey===\'custom\'&&(!saved||!saved.p)){const c=localStorage.getItem(\'onex_theme_custom\')||\'#38d9ff\';applyOnexTheme(\'custom\',{name:\'Custom\',p:c,s:\'#8b5cf6\',bg:\'#05060d\',card:\'#12162a\',mode:\'dark\'});return}\n    if(saved&&saved.p){applyOnexTheme(saved.key||\'custom\',saved);return}\n    applyOnexTheme(\'aurora\',ONEX_THEMES.aurora);\n  };\n\n  // Top-bar swatches → the same preset system (replaces the old unsaved palette).\n  const sw=document.getElementById(\'hexaSwatches\');\n  if(sw){\n    sw.innerHTML=Object.entries(LOGIN).map(([k,v])=>`<button type="button" data-theme="${k}" data-p="${v.p}" data-s="${v.s}" style="--sw-p:${v.p};--sw-s:${v.s}" aria-label="${v.name}" title="${v.name}"></button>`).join(\'\');\n    sw.querySelectorAll(\'button\').forEach(b=>b.addEventListener(\'click\',()=>selectOnexTheme(b.dataset.theme)));\n  }\n\n  // Language: share the login\'s key so FA/EN follows you everywhere.\n  try{const L=localStorage.getItem(\'onex_lang\');if(L&&L!==lang&&(L===\'fa\'||L===\'en\')){lang=L;localStorage.setItem(\'px_lang\',L)}}catch(e){}\n  const _setLang=setLang;setLang=function(l){try{localStorage.setItem(\'onex_lang\',l)}catch(e){}return _setLang.apply(this,arguments)};\n\n  document.documentElement.classList.add(\'onex-glass\');\n  window.addEventListener(\'storage\',e=>{if(e.key===\'onex_theme\')loadOnexTheme()});\n  renderOnexThemePresets();loadOnexTheme();applyLang();\n})();\n\n\n/* ============================================================\n   ONEX THEME RGB SYNC — every glass bar / surface follows the theme\n   ============================================================ */\n(function(){\n  function toRgb(c){c=String(c||\'\').trim();if(c.charAt(0)===\'#\')return hexRgb(c);const m=c.match(/\\d+(\\.\\d+)?/g);return m&&m.length>=3?m.slice(0,3).map(Number):null}\n  function mix(a,b,t){return a.map((v,i)=>Math.round(v+(b[i]-v)*t))}\n  function syncRgb(){\n    const root=document.documentElement,st=root.style,cs=getComputedStyle(root);\n    const p=toRgb(st.getPropertyValue(\'--accent\')||cs.getPropertyValue(\'--accent\'))||[59,130,246];\n    const a2=toRgb(st.getPropertyValue(\'--accent2\')||cs.getPropertyValue(\'--accent2\'))||p;\n    const s=toRgb(st.getPropertyValue(\'--purple\')||cs.getPropertyValue(\'--purple\'))||a2;\n    let bg=toRgb(st.getPropertyValue(\'--bg\')||cs.getPropertyValue(\'--bg\'))||[6,6,11];\n    if(root.classList.contains(\'light\'))bg=[6,8,16];\n    st.setProperty(\'--accent-rgb\',p.join(\',\'));st.setProperty(\'--accent2-rgb\',a2.join(\',\'));st.setProperty(\'--purple-rgb\',s.join(\',\'));\n    st.setProperty(\'--glass-rgb\',mix(mix(bg,[0,0,0],-.35),p,.13).join(\',\'));\n    st.setProperty(\'--glass-deep-rgb\',mix(bg,p,.06).join(\',\'));\n    st.setProperty(\'--hover\',`rgba(${p.join(\',\')},.12)`);\n    let saved={};try{saved=JSON.parse(localStorage.getItem(\'onex_theme_v2\')||\'{}\')}catch(e){}\n    const userSurfaces=saved.key===\'custom\'&&saved.card&&String(saved.card).toLowerCase()!==\'#12162a\';\n    if(!root.classList.contains(\'light\')&&!userSurfaces){\n      const c=(base,t,a)=>{const v=mix(base,p,t);return a==null?`rgb(${v.join(\',\')})`:`rgba(${v.join(\',\')},${a})`};\n      st.setProperty(\'--bg\',c([5,5,8],.035));st.setProperty(\'--bg2\',c([10,10,14],.05));st.setProperty(\'--bg3\',c([18,18,24],.07));\n      st.setProperty(\'--card\',c([18,18,24],.09,.92));st.setProperty(\'--card-b\',`rgba(${p.join(\',\')},.16)`);\n      st.setProperty(\'--input-bg\',c([2,2,6],.05,.62));\n    }\n  }\n  const _a=applyOnexTheme;\n  applyOnexTheme=function(){const r=_a.apply(this,arguments);try{syncRgb()}catch(e){}return r};\n  const _st=typeof setTheme===\'function\'?setTheme:null;\n  if(_st)setTheme=function(){const r=_st.apply(this,arguments);try{syncRgb()}catch(e){}return r};\n  window.onexSyncThemeRgb=syncRgb;\n  try{syncRgb()}catch(e){}\n})();\n/* ============================================================\n   ONEX HOME — live dashboard (replaces static demo data)\n   ============================================================ */\n(function(){\n  Object.assign(I18N.fa,{home_welcome:\'خوش آمدی به\',home_last:\'آخرین بروزرسانی\',home_ver_panel:\'نسخه پنل\',home_ver_core:\'هسته\',home_core_state:\'وضعیت هسته\',home_refresh:\'بروزرسانی\',home_alert_btn:\'مشاهده\',home_conns:\'اتصالات فعال\',home_traffic:\'ترافیک کل\',home_links:\'کانفیگ\u200cهای فعال\',home_uptime:\'آپتایم سرور\',home_live:\'لحظه\u200cای\',home_since:\'از آخرین راه\u200cاندازی\',home_chart:\'توزیع ساعتی ترافیک\',home_down:\'دانلود\',home_up:\'آپلود\',home_full_stats:\'آمار کامل\',home_health:\'وضعیت سرور\',home_cpu:\'پردازنده\',home_ram:\'حافظه\',home_disk:\'دیسک\',home_net:\'شبکه\',home_tg_title:\'کانال رسمی ONEX\',home_tg_sub:\'اطلاعیه\u200cها، نسخه\u200cهای جدید و پشتیبانی مستقیم\',home_tg_join:\'عضویت در کانال · @V2rayTun0\',home_recent:\'کانفیگ\u200cهای اخیر\',home_view_all:\'مشاهده همه\',home_col_name:\'نام\',home_col_proto:\'پروتکل\',home_col_usage:\'مصرف\',home_col_exp:\'انقضا\',home_quick:\'عملیات سریع\',home_q_create_sub:\'ایجاد کانفیگ جدید\',home_q_configs:\'مدیریت کانفیگ\u200cها\',home_q_configs_sub:\'مشاهده و ویرایش\',home_q_groups_sub:\'گروه\u200cهای اشتراک\',home_q_bot_sub:\'مدیریت ربات\',home_info:\'اطلاعات سرور\',home_host:\'میزبان\',home_domain:\'دامنه\',home_core:\'هسته\',home_region:\'منطقه\',home_os:\'سیستم\'});\n  Object.assign(I18N.en,{home_welcome:\'Welcome to\',home_last:\'Last update\',home_ver_panel:\'Panel\',home_ver_core:\'Core\',home_core_state:\'Core state\',home_refresh:\'Refresh\',home_alert_btn:\'View\',home_conns:\'Active connections\',home_traffic:\'Total traffic\',home_links:\'Active configs\',home_uptime:\'Server uptime\',home_live:\'Live\',home_since:\'Since last start\',home_chart:\'Hourly traffic distribution\',home_down:\'Download\',home_up:\'Upload\',home_full_stats:\'Full stats\',home_health:\'Server health\',home_cpu:\'CPU\',home_ram:\'Memory\',home_disk:\'Disk\',home_net:\'Network\',home_tg_title:\'Official ONEX channel\',home_tg_sub:\'News, releases and direct support\',home_tg_join:\'Join channel · @V2rayTun0\',home_recent:\'Recent configs\',home_view_all:\'View all\',home_col_name:\'Name\',home_col_proto:\'Protocol\',home_col_usage:\'Usage\',home_col_exp:\'Expiry\',home_quick:\'Quick actions\',home_q_create_sub:\'Create a new config\',home_q_configs:\'Manage configs\',home_q_configs_sub:\'View and edit\',home_q_groups_sub:\'Subscription groups\',home_q_bot_sub:\'Manage the bot\',home_info:\'Server info\',home_host:\'Host\',home_domain:\'Domain\',home_core:\'Core\',home_region:\'Region\',home_os:\'System\'});\n\n  const H={links:[],stats:null,sys:null,me:null,native:null,netMax:1,busy:false,lastNative:0,chart:null};\n  const $=id=>document.getElementById(id);\n  const set=(id,v)=>{const el=$(id);if(el)el.textContent=v};\n  const fa=()=>lang===\'fa\';\n  async function jget(url){\n    try{const r=await fetch(url,{cache:\'no-store\',credentials:\'same-origin\'});\n      if(r.status===401){location.href=\'/login\';return null}\n      if(!r.ok)return null;return await r.json()}catch(e){return null}\n  }\n  const expMs=l=>{if(!l||!l.expires_at)return null;const t=new Date(l.expires_at).getTime();return isNaN(t)?null:t-Date.now()};\n  window.homeIsExpiring=function(l){const ms=expMs(l);return ms!==null&&ms>0&&ms<=48*3600e3};\n  const isDead=l=>typeof configExpired===\'function\'?configExpired(l):(!!l.expired);\n\n  function pct(v){v=Number(v);return isFinite(v)?Math.max(0,Math.min(100,v)):null}\n  function bar(id,v){const el=$(id);if(el)el.style.setProperty(\'--w\',(v==null?0:v)+\'%\')}\n\n  function renderHero(){\n    const u=H.me&&H.me.username;set(\'homeUser\',u?(fa()?\'، \':\', \')+u:\'\');\n    set(\'lastUpd\',new Date().toLocaleTimeString(fa()?\'fa-IR\':\'en-US\'));\n    if(H.sys&&H.sys.singbox_version)set(\'homeCoreVer\',\'sing-box \'+H.sys.singbox_version);\n    const st=$(\'homeCoreState\');\n    if(st){const n=H.native;let txt=\'—\',c=\'\';\n      if(n){if(n.running){txt=fa()?\'فعال\':\'Running\';c=\'ok\'}else if(n.installed===false){txt=fa()?\'نصب نشده\':\'Not installed\';c=\'warn\'}else{txt=fa()?\'متوقف\':\'Stopped\';c=\'bad\'}}\n      st.textContent=txt;st.dataset.state=c;const pill=$(\'homeCorePill\');if(pill)pill.dataset.state=c;}\n    const exp=H.links.filter(l=>homeIsExpiring(l)&&!isDead(l)).length, dead=H.links.filter(isDead).length;\n    const al=$(\'homeAlert\'),at=$(\'homeAlertText\');\n    if(al&&at){\n      if(!exp&&!dead){al.hidden=true}else{al.hidden=false;\n        const parts=[];\n        if(exp)parts.push(fa()?`<b>${exp} کانفیگ</b> تا ۴۸ ساعت دیگه تموم می\u200cشن`:`<b>${exp} config${exp>1?\'s\':\'\'}</b> expire within 48h`);\n        if(dead)parts.push(fa()?`<b>${dead} کانفیگ</b> منقضی/تمام\u200cشده`:`<b>${dead}</b> expired / depleted`);\n        at.innerHTML=parts.join(\' · \');al.dataset.mode=exp?\'expiring\':\'expired\';}\n    }\n    const tl=$(\'homeThemeLbl\');if(tl)tl.textContent=document.documentElement.classList.contains(\'light\')?t(\'theme_dark\'):t(\'theme_light\');\n  }\n  function renderMetrics(){\n    const s=H.stats||{};\n    set(\'homeConns\',s.active_connections!=null?Number(s.active_connections).toLocaleString(\'en-US\'):\'—\');\n    const total=s.total_traffic_bytes!=null?s.total_traffic_bytes:H.links.reduce((a,l)=>a+Number(l.used_bytes||0),0);\n    set(\'homeTraffic\',fmtB(total));\n    set(\'homeTrafficSub\',s.download_bytes!=null?`↓ ${fmtB(s.download_bytes)} · ↑ ${fmtB(s.upload_bytes||0)}`:\'—\');\n    const totalLinks=s.links_count!=null?s.links_count:H.links.length;\n    const act=s.active_links!=null?s.active_links:H.links.filter(l=>l.active!==false&&!isDead(l)).length;\n    const el=$(\'homeLinks\');if(el)el.innerHTML=`${act} <small>/ ${totalLinks}</small>`;\n    const ex=s.expired_links!=null?s.expired_links:H.links.filter(isDead).length;\n    set(\'homeLinksSub\',fa()?`${ex} منقضی · ${Math.max(0,totalLinks-act-ex)} غیرفعال`:`${ex} expired · ${Math.max(0,totalLinks-act-ex)} disabled`);\n    set(\'homeUptime\',s.uptime||\'—\');\n    const tu=$(\'topUptime\');if(tu&&s.uptime)tu.textContent=\'Uptime: \'+s.uptime;\n    const th=$(\'topHost\');if(th)th.textContent=location.host||\'ONEX\';\n  }\n  function renderHealth(){\n    const y=H.sys||{};\n    const c=pct(y.cpu_percent),m=pct(y.mem_percent),d=pct(y.disk_percent);\n    set(\'homeCpu\',c==null?\'—\':c.toFixed(0)+\'%\');bar(\'homeCpuBar\',c);\n    set(\'homeRam\',m==null?\'—\':m.toFixed(0)+\'%\');bar(\'homeRamBar\',m);\n    set(\'homeDisk\',d==null?\'—\':d.toFixed(0)+\'%\');bar(\'homeDiskBar\',d);\n    const rate=(Number(y.net_rx_rate)||0)+(Number(y.net_tx_rate)||0);\n    if(y.net_rx_rate==null){set(\'homeNet\',\'—\');bar(\'homeNetBar\',0)}\n    else{H.netMax=Math.max(H.netMax*0.98,rate,1024);set(\'homeNet\',fmtB(Math.round(rate))+\'/s\');bar(\'homeNetBar\',Math.round(rate/H.netMax*100))}\n    const ram=$(\'homeRam\');if(ram&&y.mem_used_bytes)ram.title=fmtB(y.mem_used_bytes)+\' / \'+fmtB(y.mem_total_bytes);\n    const dk=$(\'homeDisk\');if(dk&&y.disk_used_bytes)dk.title=fmtB(y.disk_used_bytes)+\' / \'+fmtB(y.disk_total_bytes);\n    set(\'homeHost\',location.host||\'—\');\n    set(\'homeDomain\',y.public_domain||location.hostname||\'—\');\n    set(\'homeCore\',y.singbox_version?(\'sing-box \'+y.singbox_version):\'—\');\n    set(\'homeRegion\',y.region||\'—\');\n    set(\'homeOs\',y.platform||\'—\');\n  }\n  function protoName(id){try{if(typeof protocolPickerLabel===\'function\')return protocolPickerLabel(id)}catch(e){}return id||\'—\'}\n  function renderRecent(){\n    const box=$(\'homeRecent\');if(!box)return;\n    if(!H.links.length){box.innerHTML=`<div class="home-empty">${fa()?\'هنوز کانفیگی ساخته نشده\':\'No configs yet\'} · <button type="button" class="home-link-btn" onclick="goPage(\'create\')">${fa()?\'ساخت اولین کانفیگ\':\'Create the first one\'}</button></div>`;return}\n    const arr=H.links.slice().sort((a,b)=>String(b.created_at||\'\').localeCompare(String(a.created_at||\'\'))).slice(0,5);\n    box.innerHTML=arr.map(l=>{\n      const uid=esc(l.uuid||l.id||\'\');\n      const name=esc(l.label||l.name||String(l.uuid||l.id||\'\').slice(0,8));\n      const used=Number(l.used_bytes||0),lim=Number(l.limit_bytes||0);\n      const p=lim>0?Math.min(100,Math.round(used/lim*100)):0;\n      const ms=expMs(l),dead=isDead(l),off=l.active===false;\n      let exp=fa()?\'∞\':\'∞\',warn=false;\n      if(ms!==null){if(ms<=0)exp=fa()?\'منقضی\':\'expired\';else{const dd=Math.ceil(ms/864e5);exp=fa()?`${dd} روز`:`${dd}d`;warn=ms<=3*864e5}}\n      const state=dead?\'bad\':(off?\'off\':(warn||p>=90?\'warn\':\'ok\'));\n      return `<div class="tr home-row" data-state="${state}" role="button" tabindex="0" onclick="openConfigEditor(null,\'${uid}\')" onkeydown="if(event.key===\'Enter\')openConfigEditor(null,\'${uid}\')">`+\n        `<b title="${name}">${name}</b><span class="proto">${esc(protoName(l.protocol))}</span>`+\n        `<div class="usage"><span class="mono">${fmtB(used)}${lim>0?\' / \'+fmtB(lim):\'\'}</span><div class="bar"><i style="--w:${lim>0?p:0}%"></i></div></div>`+\n        `<span class="home-exp">${exp}</span><span class="dot" title="${state}"></span></div>`;\n    }).join(\'\');\n  }\n  function drawChart(){\n    const cv=$(\'traffic\');if(!cv)return;const ctx=cv.getContext(\'2d\');if(!ctx)return;\n    const r=cv.getBoundingClientRect();if(!r.width)return;\n    const d=Math.min(devicePixelRatio||1,2);cv.width=r.width*d;cv.height=r.height*d;ctx.setTransform(d,0,0,d,0,0);\n    const w=r.width,h=r.height,padL=8,padR=8,padT=14,padB=22;\n    const s=H.stats||{},down=s.hourly||{},up=s.hourly_upload||{};\n    let cur=0;try{cur=Number(new Intl.DateTimeFormat(\'en-GB\',{hour:\'2-digit\',hour12:false,timeZone:\'Asia/Tehran\'}).format(new Date()))%24}catch(e){cur=new Date().getHours()}\n    const keys=[];for(let i=23;i>=0;i--){const hh=(cur-i+24)%24;keys.push(String(hh).padStart(2,\'0\')+\':00\')}\n    const D=keys.map(k=>Number(down[k]||0)),U=keys.map(k=>Number(up[k]||0));\n    const max=Math.max(1,...D,...U);\n    const css=getComputedStyle(document.documentElement);\n    const P=(css.getPropertyValue(\'--accent\')||\'#38d9ff\').trim(),S=(css.getPropertyValue(\'--purple\')||\'#8b5cf6\').trim();\n    const light=document.documentElement.classList.contains(\'light\');\n    ctx.clearRect(0,0,w,h);\n    ctx.strokeStyle=light?\'rgba(15,23,42,.08)\':\'rgba(255,255,255,.07)\';ctx.lineWidth=1;ctx.setLineDash([3,5]);\n    for(let i=0;i<=3;i++){const y=padT+(h-padT-padB)*i/3;ctx.beginPath();ctx.moveTo(padL,y);ctx.lineTo(w-padR,y);ctx.stroke()}\n    ctx.setLineDash([]);\n    const X=i=>padL+(w-padL-padR)*i/23,Y=v=>h-padB-(h-padT-padB)*(v/max);\n    function area(vals,col){\n      const g=ctx.createLinearGradient(0,padT,0,h-padB);\n      g.addColorStop(0,col);g.addColorStop(1,\'transparent\');\n      ctx.beginPath();vals.forEach((v,i)=>{const x=X(i),y=Y(v);if(!i)ctx.moveTo(x,y);else{const px=X(i-1),py=Y(vals[i-1]),mx=(px+x)/2;ctx.bezierCurveTo(mx,py,mx,y,x,y)}});\n      ctx.save();ctx.lineTo(X(23),h-padB);ctx.lineTo(X(0),h-padB);ctx.closePath();ctx.globalAlpha=.28;ctx.fillStyle=g;ctx.fill();ctx.restore();\n      ctx.beginPath();vals.forEach((v,i)=>{const x=X(i),y=Y(v);if(!i)ctx.moveTo(x,y);else{const px=X(i-1),py=Y(vals[i-1]),mx=(px+x)/2;ctx.bezierCurveTo(mx,py,mx,y,x,y)}});\n      ctx.strokeStyle=col;ctx.lineWidth=2.2;ctx.shadowColor=col;ctx.shadowBlur=10;ctx.stroke();ctx.shadowBlur=0;\n    }\n    area(U,S);area(D,P);\n    ctx.fillStyle=light?\'rgba(51,65,85,.7)\':\'rgba(238,242,255,.45)\';ctx.font=\'500 10px "JetBrains Mono",monospace\';ctx.textAlign=\'center\';\n    [0,6,12,18,23].forEach(i=>ctx.fillText(keys[i],Math.min(Math.max(X(i),18),w-18),h-6));\n    ctx.textAlign=\'left\';ctx.fillText(fmtB(max),padL+2,padT-3);\n    H.chart={keys,D,U,X,w,h,padT,padB};\n  }\n  function bindChartHover(){\n    const cv=$(\'traffic\'),tip=$(\'homeChartTip\');if(!cv||!tip||cv.__homeHover)return;cv.__homeHover=1;\n    cv.addEventListener(\'pointermove\',e=>{const c=H.chart;if(!c)return;const r=cv.getBoundingClientRect(),x=e.clientX-r.left;\n      let best=0,bd=1e9;c.keys.forEach((_,i)=>{const dd=Math.abs(c.X(i)-x);if(dd<bd){bd=dd;best=i}});\n      tip.hidden=false;tip.innerHTML=`<b>${c.keys[best]}</b><span><i class="lg-d"></i>${fmtB(c.D[best])}</span><span><i class="lg-u"></i>${fmtB(c.U[best])}</span>`;\n      const tx=Math.min(Math.max(c.X(best),60),r.width-60);tip.style.left=tx+\'px\'});\n    cv.addEventListener(\'pointerleave\',()=>{tip.hidden=true});\n    addEventListener(\'resize\',()=>{clearTimeout(H.rz);H.rz=setTimeout(drawChart,120)});\n  }\n  function renderAll(){renderHero();renderMetrics();renderHealth();renderRecent();drawChart()}\n  window.homeRender=renderAll;\n\n  window.homeRefresh=async function(showToast){\n    if(H.busy)return;H.busy=true;\n    const btn=$(\'heroRefresh\');if(btn)btn.classList.add(\'spinning\');\n    try{\n      const now=Date.now(),tasks=[jget(\'/api/links\'),jget(\'/stats\'),jget(\'/api/system/metrics\')];\n      tasks.push(!H.me?jget(\'/api/me\'):Promise.resolve(H.me));\n      tasks.push(now-H.lastNative>20000?jget(\'/api/native/status\'):Promise.resolve(H.native));\n      const [links,stats,sys,me,native]=await Promise.all(tasks);\n      if(links){H.links=Array.isArray(links.links)?links.links:(Array.isArray(links)?links:[]);}\n      if(stats)H.stats=stats; if(sys)H.sys=sys; if(me)H.me=me;\n      if(native&&native!==H.native){H.native=native;H.lastNative=now}\n      renderAll();\n      if(showToast)toast(fa()?\'داشبورد بروزرسانی شد\':\'Dashboard refreshed\');\n    }finally{H.busy=false;if(btn)btn.classList.remove(\'spinning\')}\n  };\n  window.homeShowExpiring=function(){\n    const mode=($(\'homeAlert\')||{}).dataset?.mode||\'expiring\';\n    goPage(\'configs\');\n    setTimeout(()=>{\n      if(mode===\'expired\'){const b=document.querySelector(\'#cfgFilterRow [data-status="expired"]\');if(typeof setCfgStatus===\'function\')setCfgStatus(\'expired\',b)}\n      else{cfgStatusFilter=\'expiring\';document.querySelectorAll(\'#cfgFilterRow [data-status]\').forEach(x=>x.classList.remove(\'on\'));if(typeof filterConfigs===\'function\')filterConfigs()}\n    },350);\n  };\n\n  // keep home in sync with language / theme / navigation\n  const _applyLang=applyLang;applyLang=function(){_applyLang.apply(this,arguments);try{renderAll()}catch(e){}};\n  const _goPage=goPage;goPage=function(name){const r=_goPage.apply(this,arguments);if(name===\'dash\')setTimeout(()=>homeRefresh(false),60);return r};\n  new MutationObserver(()=>{clearTimeout(H.mo);H.mo=setTimeout(drawChart,60)}).observe(document.documentElement,{attributes:true,attributeFilter:[\'class\',\'style\']});\n\n  bindChartHover();applyLang();homeRefresh(false);\n  setInterval(()=>{const pg=$(\'page-dash\');if(pg&&pg.classList.contains(\'on\')&&!document.hidden)homeRefresh(false)},6000);\n})();\n\n</script>\n\n<script>\n(()=>{\n  /* ONEX LITE: animated background disabled for performance */return;\n  const cv=document.getElementById(\'onexDashScene\'),ctx=cv&&cv.getContext(\'2d\');if(!cv||!ctx)return;\n  let W=0,H=0,dpr=1,tm=0,last=performance.now(),mouse={x:0,y:0,tx:0,ty:0};\n  const themes={aurora:[\'#38d9ff\',\'#8b5cf6\'],lime:[\'#d4f24a\',\'#ffb84d\'],rose:[\'#ff3d8b\',\'#ff8a3d\'],emerald:[\'#34f5a0\',\'#22c1ee\'],violet:[\'#b57bff\',\'#ff5ecf\'],gold:[\'#ffc94d\',\'#ff7a45\']};\n  function rgb(h){h=(h||\'\').replace(\'#\',\'\');let n=parseInt(h,16)||0;return[(n>>16)&255,(n>>8)&255,n&255]}\n  function colors(){let id=localStorage.getItem(\'onex_theme\')||\'aurora\',c=themes[id]||themes.aurora;if(id===\'custom\'){let p=localStorage.getItem(\'onex_theme_custom\')||\'#38d9ff\';c=[p,\'#8b5cf6\']}return[c.map(rgb)[0],c.map(rgb)[1]]}\n  let C=colors();\n  function resize(){W=innerWidth;H=innerHeight;dpr=Math.min(devicePixelRatio||1,1.5);cv.width=W*dpr;cv.height=H*dpr;cv.style.width=W+\'px\';cv.style.height=H+\'px\';ctx.setTransform(dpr,0,0,dpr,0,0)}\n  resize();addEventListener(\'resize\',resize);addEventListener(\'pointermove\',e=>{mouse.tx=e.clientX/W*2-1;mouse.ty=e.clientY/H*2-1});\n  const objs=Array.from({length:18},(_,i)=>({x:(Math.random()*2-1)*11,y:(Math.random()*2-1)*6,z:5+Math.random()*15,s:.35+Math.random()*1.1,a:Math.random()*6.28,v:.1+Math.random()*.3,k:i%3}));\n  function drawShape(o,t){let x=W/2+o.x*W/18-mouse.x*20,y=H/2+o.y*H/9-mouse.y*12+Math.sin(t*.6+o.a)*9;let r=o.s*W/16*(1.1-o.z/28),col=C[o.k%2],a=Math.max(0,.04*(1-o.z/24));ctx.save();ctx.translate(x,y);ctx.rotate(t*.12+o.a);ctx.strokeStyle=`rgba(${col[0]},${col[1]},${col[2]},${a})`;ctx.lineWidth=1.2;ctx.beginPath();for(let i=0;i<6;i++){let q=i*Math.PI/3;let xx=Math.cos(q)*r,yy=Math.sin(q)*r;i?ctx.lineTo(xx,yy):ctx.moveTo(xx,yy)}ctx.closePath();ctx.stroke();ctx.restore()}\n  function frame(now){let dt=Math.min(.05,(now-last)/1000);last=now;tm+=dt;mouse.x+=(mouse.tx-mouse.x)*.04;mouse.y+=(mouse.ty-mouse.y)*.04;let n=colors();C[0][0]+=(n[0][0]-C[0][0])*.03;C[0][1]+=(n[0][1]-C[0][1])*.03;C[0][2]+=(n[0][2]-C[0][2])*.03;C[1][0]+=(n[1][0]-C[1][0])*.03;C[1][1]+=(n[1][1]-C[1][1])*.03;C[1][2]+=(n[1][2]-C[1][2])*.03;\n    ctx.clearRect(0,0,W,H);let g=ctx.createRadialGradient(W*.18,H*.12,0,W*.18,H*.12,Math.max(W,H)*.65);g.addColorStop(0,`rgba(${C[0][0]|0},${C[0][1]|0},${C[0][2]|0},.10)`);g.addColorStop(1,\'rgba(0,0,0,0)\');ctx.fillStyle=g;ctx.fillRect(0,0,W,H);\n    for(const o of objs){o.y-=dt*.025;if(o.y<-7)o.y=7;drawShape(o,tm)}requestAnimationFrame(frame)}requestAnimationFrame(frame);\n})();\n</script>\n<!-- ============================================================\n     ONEX LITE + GLASS CONFIG BUILDER (performance layer)\n     - all CSS animations/transitions off, backdrop blur off (except top bar)\n     - theme-aware glass bar + config builder (uses --p / --s tokens)\n     ============================================================ -->\n<style id="onex-lite-layer">\n/* ---------- performance: kill motion everywhere ---------- */\nhtml,body{scroll-behavior:auto!important}\n*:not(#_lx1):not(#_lx2):not(#_lx3),*:not(#_lx1):not(#_lx2):not(#_lx3)::before,*:not(#_lx1):not(#_lx2):not(#_lx3)::after{animation-duration:0s!important;animation-delay:0s!important;animation-iteration-count:1!important;transition-duration:0s!important;transition-delay:0s!important;-webkit-backdrop-filter:none!important;backdrop-filter:none!important}\n#onexDashScene{display:none!important}\n.onex-metric:not(#_lx1):not(#_lx2):not(#_lx3):after,.onex-mark:not(#_lx1):not(#_lx2):not(#_lx3):after,.theme-mini-cards div:not(#_lx1):not(#_lx2):not(#_lx3):after,.onex-glint:not(#_lx1):not(#_lx2):not(#_lx3){display:none!important}\n\n/* ---------- glass top bar (follows theme color) ---------- */\nhtml body #mobBar:not(#_lx1):not(#_lx2):not(#_lx3),html body .onex-topbar:not(#_lx1):not(#_lx2):not(#_lx3){\n  background:linear-gradient(135deg,rgb(var(--p-rgb,56 217 255) / .16),rgb(var(--s-rgb,139 92 246) / .08) 70%),color-mix(in srgb, rgb(11 11 11 / .72) 92%, var(--accent))!important;\n  border-color:rgb(var(--p-rgb,56 217 255) / .28)!important;\n  box-shadow:inset 0 1px 0 rgba(255,255,255,.07),0 8px 22px -14px rgb(var(--p-rgb,56 217 255) / .6)!important;\n  -webkit-backdrop-filter:blur(10px)!important;backdrop-filter:blur(10px)!important}\nhtml body #mobBar:not(#_lx1):not(#_lx2):not(#_lx3){border-bottom:1px solid rgb(var(--p-rgb,56 217 255) / .28)!important}\nhtml body #mobMenuBtn:not(#_lx1):not(#_lx2):not(#_lx3){background:linear-gradient(135deg,var(--p,var(--accent2)),var(--s,var(--purple)))!important;border-color:rgb(var(--p-rgb,56 217 255) / .6)!important;box-shadow:0 6px 16px -8px rgb(var(--p-rgb,56 217 255) / .9)!important}\nhtml body .mob-bar #mxa stop:first-child,html body .sidebar #sxa stop:first-child{stop-color:var(--p,var(--accent))}\nhtml body .mob-bar #mxa stop:last-child,html body .sidebar #sxa stop:last-child{stop-color:var(--s,var(--purple))}\nhtml body .mob-bar #mxb stop,html body .sidebar #sxb stop{stop-color:var(--s,var(--purple))}\nhtml.light body #mobBar:not(#_lx1):not(#_lx2):not(#_lx3),html.light body .onex-topbar:not(#_lx1):not(#_lx2):not(#_lx3){background:linear-gradient(135deg,rgb(var(--p-rgb,37 99 235) / .12),rgb(var(--s-rgb,124 58 237) / .06) 70%),rgba(255,255,255,.82)!important;color:var(--t1,color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)))!important}\n\n/* ---------- config builder ---------- */\n#page-create .page-head{padding:14px 16px;border-radius:18px;margin-bottom:12px;\n  background:linear-gradient(135deg,rgb(var(--p-rgb,56 217 255) / .16),rgb(var(--s-rgb,139 92 246) / .08) 65%),color-mix(in srgb, rgb(15 15 15 / .7) 92%, var(--accent));\n  border:1px solid rgb(var(--p-rgb,56 217 255) / .28);box-shadow:inset 0 1px 0 rgba(255,255,255,.06)}\n#page-create .page-head .page-title svg{color:var(--p,var(--accent2))}\n#cfgx{display:flex;flex-direction:column;gap:12px;max-width:760px;margin:0 auto}\n#cfgx .cfgx-card,#cfgx .advanced-config-card{position:relative;padding:14px;border-radius:18px;margin:0;\n  background:linear-gradient(150deg,rgb(var(--p-rgb,56 217 255) / .09),rgb(var(--s-rgb,139 92 246) / .04) 55%,transparent),color-mix(in srgb, rgb(15 15 15 / .72) 92%, var(--accent))!important;\n  border:1px solid rgb(var(--p-rgb,56 217 255) / .2)!important;box-shadow:inset 0 1px 0 rgba(255,255,255,.05)!important;color:var(--t1,#f8fafc)}\n#cfgx .cfgx-head{display:flex;align-items:center;gap:10px;margin-bottom:12px}\n#cfgx .cfgx-head>div{flex:1;min-width:0}\n#cfgx .cfgx-head b{display:block;font-size:14px;font-weight:800}\n#cfgx .cfgx-head small{display:block;font-size:10.5px;color:var(--t3,color-mix(in srgb, rgb(161 161 161) 82%, var(--accent)));margin-top:2px}\n#cfgx .cfgx-ico{flex:none;width:34px;height:34px;border-radius:11px;display:grid;place-items:center;color:var(--p,var(--accent2));background:rgb(var(--p-rgb,56 217 255) / .12);border:1px solid rgb(var(--p-rgb,56 217 255) / .3)}\n#cfgx .cfgx-ico svg{width:18px;height:18px}\n#cfgx .cfgx-badge{flex:none;font:700 11px/1 Vazirmatn,system-ui;padding:7px 10px;border-radius:99px;color:#fff;background:linear-gradient(135deg,var(--p,var(--accent2)),var(--s,var(--purple)));max-width:48%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#cfgx .cfgx-note{display:block;margin-top:8px;font-size:10px;color:var(--t3,color-mix(in srgb, rgb(161 161 161) 82%, var(--accent)));line-height:1.7}\n#cfgx .cfgx-row2,#cfgx .cfgx-limits{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}\n#cfgx .cfgx-field{margin:0}\n#cfgx .cfgx-field label{display:block;font-size:11.5px;font-weight:600;color:var(--t2,color-mix(in srgb, rgb(211 211 211) 82%, var(--accent)));margin-bottom:6px}\n#cfgx .cfgx-inline{display:flex;align-items:center;gap:6px}\n#cfgx input:not([type=checkbox]),#cfgx select{width:100%;height:42px;min-width:0;padding:0 12px;border-radius:12px;font:500 13px Vazirmatn,system-ui;color:var(--t1,#f8fafc);background:rgba(0,0,0,.28)!important;border:1px solid rgba(255,255,255,.1)!important;outline:none;box-shadow:none!important}\n#cfgx input:not([type=checkbox]):focus,#cfgx select:focus{border-color:var(--p,var(--accent2))!important;box-shadow:0 0 0 3px rgb(var(--p-rgb,56 217 255) / .16)!important}\n#cfgx .cfgx-inline input{flex:1}\n#cfgx select.cfgx-unit{flex:0 0 70px;width:70px;padding:0 8px}\n#cfgx .cfgx-suffix{flex:0 0 auto;min-width:46px;height:42px;display:grid;place-items:center;padding:0 8px;border-radius:12px;font:700 11px Vazirmatn,system-ui;color:var(--t3,color-mix(in srgb, rgb(161 161 161) 82%, var(--accent)));background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08)}\n#cfgx .cfgx-icon-btn{flex:none;width:42px;height:42px;border-radius:12px;display:grid;place-items:center;color:var(--p,var(--accent2));background:rgb(var(--p-rgb,56 217 255) / .1);border:1px solid rgb(var(--p-rgb,56 217 255) / .3);cursor:pointer}\nhtml body #page-create #cfgx .cfgx-legacy-proto{display:none!important}\n\n/* protocol cards */\n#cfgx .cfgx-proto-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px}\n#cfgx .cfgx-proto{position:relative;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:5px;min-height:104px;padding:12px 6px 10px;border-radius:14px;cursor:pointer;text-align:center;color:var(--t1,#f8fafc);\n  background:rgba(255,255,255,.035);border:1px solid rgba(255,255,255,.09);font-family:Vazirmatn,system-ui}\n#cfgx .cfgx-proto img{width:40px;height:40px;object-fit:contain}\n#cfgx .cfgx-proto b{font-size:11.5px;font-weight:800;line-height:1.35;word-break:break-word}\n#cfgx .cfgx-proto small{font-size:9.5px;color:var(--t3,color-mix(in srgb, rgb(161 161 161) 82%, var(--accent)));line-height:1.3}\n#cfgx .cfgx-radio{position:absolute;top:7px;inset-inline-start:7px;width:18px;height:18px;border-radius:50%;border:1.5px solid rgba(255,255,255,.28)}\n#cfgx .cfgx-proto.on{background:linear-gradient(150deg,rgb(var(--p-rgb,56 217 255) / .2),rgb(var(--s-rgb,139 92 246) / .1));border-color:var(--p,var(--accent2));box-shadow:0 0 0 1px rgb(var(--p-rgb,56 217 255) / .35) inset}\n#cfgx .cfgx-proto.on .cfgx-radio{border:0;background:linear-gradient(135deg,var(--p,var(--accent2)),var(--s,var(--purple)))}\n#cfgx .cfgx-proto.on .cfgx-radio:after{content:"";position:absolute;left:5px;top:5px;width:7px;height:4px;border-left:2px solid #fff;border-bottom:2px solid #fff;transform:rotate(-45deg)}\n#cfgx .cfgx-more{border-style:dashed;color:var(--p,var(--accent2))}\n#cfgx .cfgx-more .cfgx-plus{width:40px;height:40px;border-radius:12px;display:grid;place-items:center;font-size:24px;background:rgb(var(--p-rgb,56 217 255) / .12)}\n#cfgx .cfgx-empty{grid-column:1/-1;padding:18px;text-align:center;font-size:12px;color:var(--t3,color-mix(in srgb, rgb(161 161 161) 82%, var(--accent)))}\n\n/* bundle chips (the real checkboxes stay inside, only restyled) */\n#cfgx .cfgx-bundle{display:grid!important;grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:8px!important}\n#cfgx .cfgx-bundle label{position:relative;display:flex!important;align-items:center;gap:8px;min-height:46px;padding:8px 10px!important;border-radius:12px!important;cursor:pointer;font-size:11px!important;\n  background:rgba(255,255,255,.035);border:1px solid rgba(255,255,255,.09)!important;color:var(--t1,#f8fafc)}\n#cfgx .cfgx-bundle label input{position:absolute;opacity:0;pointer-events:none;width:1px;height:1px}\n#cfgx .cfgx-bundle label img{width:26px;height:26px;object-fit:contain;flex:none}\n#cfgx .cfgx-bundle label b{flex:1;min-width:0;font-weight:700;line-height:1.35}\n#cfgx .cfgx-bundle label i{flex:none;width:18px;height:18px;border-radius:6px;border:1.5px solid rgba(255,255,255,.28);position:relative}\n#cfgx .cfgx-bundle label em{font-style:normal;font-size:9px;font-weight:800;padding:2px 6px;border-radius:99px;color:var(--p,var(--accent2));background:rgb(var(--p-rgb,56 217 255) / .14);display:none}\n#cfgx .cfgx-bundle label.is-main em{display:inline-block}\n#cfgx .cfgx-bundle label.is-on{background:linear-gradient(150deg,rgb(var(--p-rgb,56 217 255) / .18),rgb(var(--s-rgb,139 92 246) / .08));border-color:rgb(var(--p-rgb,56 217 255) / .7)!important}\n#cfgx .cfgx-bundle label.is-on i{border:0;background:linear-gradient(135deg,var(--p,var(--accent2)),var(--s,var(--purple)))}\n#cfgx .cfgx-bundle label.is-on i:after{content:"";position:absolute;left:5px;top:5px;width:7px;height:4px;border-left:2px solid #fff;border-bottom:2px solid #fff;transform:rotate(-45deg)}\n\n/* switches */\n#cfgx .cfgx-switch{margin:10px 0 0!important;border-radius:14px!important;background:rgba(255,255,255,.03)!important;border:1px solid rgba(255,255,255,.09)!important}\n#cfgx .cfgx-adb{margin:0!important}\n#cfgx .cfgx-switch.cfgx-adb{background:linear-gradient(150deg,rgb(var(--p-rgb,56 217 255) / .09),rgb(var(--s-rgb,139 92 246) / .04) 55%,transparent),color-mix(in srgb, rgb(15 15 15 / .72) 92%, var(--accent))!important;border-color:rgb(var(--p-rgb,56 217 255) / .2)!important}\nhtml.light #cfgx .cfgx-switch.cfgx-adb{background:rgba(255,255,255,.9)!important}\n#cfgx .all-proto-toggle i{background:rgba(255,255,255,.16)!important;box-shadow:none!important}\n#cfgx .all-proto-toggle:has(input:checked) i,#cfgx .all-proto-toggle.is-checked i{background:linear-gradient(135deg,var(--p,var(--accent2)),var(--s,var(--purple)))!important}\n#cfgx .all-proto-toggle:has(input:checked) i:before,#cfgx .all-proto-toggle.is-checked i:before{right:4px}\n#cfgx .all-proto-toggle:focus-within{outline:2px solid rgb(var(--p-rgb,56 217 255) / .35)!important}\n\n/* advanced card */\n#cfgx .advanced-config-card .advanced-toggle{background:transparent!important;border:0!important;box-shadow:none!important}\n#cfgx .advanced-config-card .advanced-toggle-icon{color:var(--p,var(--accent2))}\n#cfgx .advanced-section{background:rgba(0,0,0,.18)!important;border:1px solid rgba(255,255,255,.07)!important;box-shadow:none!important}\n\n/* summary */\n#cfgx .cfgx-summary{border-color:rgb(var(--p-rgb,56 217 255) / .4)!important}\n#cfgx .cfgx-sum-rows{display:flex;flex-direction:column;gap:7px;padding:10px 12px;border-radius:12px;background:rgba(0,0,0,.2);border:1px solid rgba(255,255,255,.06)}\n#cfgx .cfgx-sum-rows div{display:flex;justify-content:space-between;gap:12px;font-size:11.5px}\n#cfgx .cfgx-sum-rows span{color:var(--t3,color-mix(in srgb, rgb(161 161 161) 82%, var(--accent)));flex:none}\n#cfgx .cfgx-sum-rows b{font-weight:600;text-align:left;direction:auto;min-width:0;word-break:break-word}\n#cfgx .cfgx-valid{margin:10px 2px;font-size:12px;font-weight:700;color:#22c55e}\n#cfgx .cfgx-valid.warn{color:#f59e0b}\n#cfgx .cfgx-submit{width:100%;height:50px;border:0!important;border-radius:14px!important;font:800 14px Vazirmatn,system-ui;color:var(--on,#fff)!important;background:linear-gradient(135deg,var(--p,var(--accent2)),var(--s,var(--purple)))!important;box-shadow:0 10px 24px -14px rgb(var(--p-rgb,56 217 255) / .9)!important;display:flex;align-items:center;justify-content:center;gap:8px}\n\n/* light theme */\nhtml.light #page-create .page-head,html.light #cfgx .cfgx-card,html.light #cfgx .advanced-config-card{background:linear-gradient(150deg,rgb(var(--p-rgb,37 99 235) / .08),transparent 60%),rgba(255,255,255,.9)!important;color:var(--t1,color-mix(in srgb, rgb(23 23 23) 92%, var(--accent)))}\nhtml.light #cfgx input:not([type=checkbox]),html.light #cfgx select{background:#f8fafc!important;border-color:color-mix(in srgb, rgb(23 23 23 / .12) 92%, var(--accent))!important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}\nhtml.light #cfgx .cfgx-proto,html.light #cfgx .cfgx-bundle label,html.light #cfgx .cfgx-switch,html.light #cfgx .cfgx-sum-rows,html.light #cfgx .cfgx-suffix{background:color-mix(in srgb, rgb(23 23 23 / .03) 92%, var(--accent))!important;border-color:color-mix(in srgb, rgb(23 23 23 / .1) 92%, var(--accent))!important;color:color-mix(in srgb, rgb(23 23 23) 92%, var(--accent))}\nhtml.light #cfgx .advanced-section{background:color-mix(in srgb, rgb(23 23 23 / .03) 92%, var(--accent))!important}\n\n@media (max-width:420px){\n  #cfgx .cfgx-row2{grid-template-columns:1fr}\n  #cfgx .cfgx-proto{min-height:96px}\n  #cfgx .cfgx-proto img{width:34px;height:34px}\n}\n@media (min-width:900px){#cfgx .cfgx-proto-grid{grid-template-columns:repeat(4,minmax(0,1fr))}#cfgx .cfgx-bundle{grid-template-columns:repeat(3,minmax(0,1fr))!important}}\n</style>\n<script>\n/* ONEX glass config builder: pure UI layer, every value still lives in the\n   original inputs (cName, cProto, cSubGroup, protocolBundleOptions, cLimit...)\n   so doManualCreate / edit mode / backend payload are untouched. */\n(function(){\n  \'use strict\';\n  var $=function(id){return document.getElementById(id)};\n  var MAIN=[\'vless-ws\',\'xhttp-packet-up\',\'xhttp-stream-up\',\'xhttp-stream-one\',\'trojan\'];\n  function h(s){return String(s==null?\'\':s).replace(/[&<>"\']/g,function(c){return{\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\',"\'":\'&#39;\'}[c]})}\n  function nameOf(id){try{return protocolPickerShort(id)}catch(e){return id}}\n  function iconOf(id){try{return PROTOCOL_ICON_DATA[id]||PROTOCOL_ICON_DATA[\'vless-ws\']}catch(e){return \'\'}}\n  function descOf(id){try{return PROTOCOL_PICKER_DESCS[id]||\'\'}catch(e){return \'\'}}\n  var gridKey=\'\';\n  function renderGrid(){\n    var sel=$(\'cProto\'),grid=$(\'cfgxProtoGrid\');if(!sel||!grid)return;\n    var avail=[].map.call(sel.options,function(o){return o.value});if(!avail.length)return;\n    var ids=MAIN.filter(function(id){return avail.indexOf(id)>-1});if(!ids.length)ids=avail.slice(0,8);\n    var cur=sel.value;if(cur&&ids.indexOf(cur)<0)ids=[cur].concat(ids);\n    var key=ids.join(\',\')+\'|\'+avail.length;\n    if(key!==gridKey){\n      gridKey=key;\n      grid.innerHTML=ids.map(function(id){return \'<button type="button" class="cfgx-proto" data-proto="\'+h(id)+\'" aria-pressed="false"><span class="cfgx-radio"></span><img src="\'+h(iconOf(id))+\'" alt="" loading="lazy" decoding="async"><b>\'+h(nameOf(id))+\'</b><small>\'+h(descOf(id))+\'</small></button>\'}).join(\'\')+\n        \'<button type="button" class="cfgx-proto cfgx-more" data-more="1"><span class="cfgx-plus">+</span><b>همه پروتکل\u200cها</b><small>\'+avail.length+\' پروتکل</small></button>\';\n    }\n    grid.querySelectorAll(\'.cfgx-proto[data-proto]\').forEach(function(b){var on=b.getAttribute(\'data-proto\')===cur;b.classList.toggle(\'on\',on);b.setAttribute(\'aria-pressed\',on?\'true\':\'false\')});\n    var badge=$(\'cfgxProtoBadge\');if(badge)badge.textContent=cur?nameOf(cur):\'—\';\n  }\n  function bundleList(){\n    var box=$(\'protocolBundleOptions\'),sel=$(\'cProto\'),cur=sel?sel.value:\'\';if(!box)return[];\n    var list=[].map.call(box.querySelectorAll(\'input:checked\'),function(i){return i.value});\n    if(list.length&&cur&&list.indexOf(cur)<0)list.unshift(cur);\n    return list;\n  }\n  function syncBundle(){\n    var box=$(\'protocolBundleOptions\'),sel=$(\'cProto\'),cur=sel?sel.value:\'\';if(!box)return;\n    var any=!!box.querySelector(\'input:checked\');\n    box.querySelectorAll(\'label\').forEach(function(l){var i=l.querySelector(\'input\');if(!i)return;var main=i.value===cur;l.classList.toggle(\'is-main\',main);l.classList.toggle(\'is-on\',i.checked||(main&&any))});\n    var b=$(\'cfgxBundleBadge\');if(b)b.textContent=String(bundleList().length);\n    document.querySelectorAll(\'#cfgx .all-proto-toggle\').forEach(function(w){var i=w.querySelector(\'input\');w.classList.toggle(\'is-checked\',!!(i&&i.checked))});\n  }\n  function num(id){var v=Number(($(id)||{}).value);return isFinite(v)?v:NaN}\n  function syncSummary(){\n    var set=function(id,t){var e=$(id);if(e)e.textContent=t};\n    var nm=(($(\'cName\')||{}).value||\'\').trim();set(\'cfgxSumName\',nm||\'خودکار\');\n    var g=$(\'cSubGroup\');set(\'cfgxSumGroup\',g&&g.selectedIndex>-1&&g.options[g.selectedIndex]?g.options[g.selectedIndex].text:\'بدون گروه\');\n    var all=$(\'cAllProtocols\'),sel=$(\'cProto\'),pl;\n    if(all&&all.checked&&!all.disabled)pl=\'همه پروتکل\u200cهای Railway در یک ساب\';\n    else{var bl=bundleList();pl=bl.length?bl.map(nameOf).join(\'، \')+\' (\'+bl.length+\')\':(sel&&sel.value?nameOf(sel.value):\'—\')}\n    set(\'cfgxSumProto\',pl);\n    var lim=num(\'cLimit\'),unit=(($(\'cUnit\')||{}).value)||\'GB\',d=num(\'cDays\'),ip=num(\'cIp\'),sp=num(\'cSpeed\');\n    var f=function(v,s){return v>0?(v+(s?\' \'+s:\'\')):\'نامحدود\'};\n    set(\'cfgxSumLimits\',\'حجم: \'+f(lim,unit)+\' · روز: \'+f(d)+\' · IP: \'+f(ip)+\' · سرعت: \'+f(sp,\'Mbps\'));\n    var adb=$(\'cAdBlockEnabled\');set(\'cfgxSumAdb\',adb&&adb.checked?\'روشن\':\'خاموش\');\n    var bad=[];[[\'cLimit\',\'حجم\'],[\'cDays\',\'روز\'],[\'cIp\',\'IP\'],[\'cSpeed\',\'سرعت\']].forEach(function(p){var v=num(p[0]);if(isNaN(v)||v<0)bad.push(p[1])});\n    var ok=$(\'cfgxValid\');if(ok){ok.classList.toggle(\'warn\',!!bad.length);ok.textContent=bad.length?\'⚠ مقدار نامعتبر: \'+bad.join(\'، \'):\'✓ اطلاعات معتبر است\'}\n  }\n  function sync(){try{renderGrid();syncBundle();syncSummary()}catch(e){console.warn(\'cfgx\',e)}}\n  var pending=false;function schedule(){if(pending)return;pending=true;(window.requestAnimationFrame||setTimeout)(function(){pending=false;sync()})}\n  window.cfgxSync=schedule;\n\n  function wrap(name){var f=window[name];if(typeof f!==\'function\'||f.__cfgx)return;var w=function(){var r=f.apply(this,arguments);try{if(r&&typeof r.then===\'function\')r.then(schedule,schedule);else schedule()}catch(e){}return r};w.__cfgx=1;try{window[name]=w}catch(e){}}\n  [\'syncProtocolPicker\',\'syncAllProtocolToggle\',\'setConfigEditMode\',\'openConfigEditor\',\'goPage\'].forEach(wrap);\n\n  function boot(){\n    var root=$(\'cfgx\');if(!root)return;\n    root.addEventListener(\'input\',schedule);root.addEventListener(\'change\',schedule);\n    var grid=$(\'cfgxProtoGrid\');\n    if(grid)grid.addEventListener(\'click\',function(ev){\n      var b=ev.target.closest(\'.cfgx-proto\');if(!b)return;ev.preventDefault();\n      if(b.getAttribute(\'data-more\')){if(typeof openProtocolPicker===\'function\')openProtocolPicker(\'cProto\');return}\n      var sel=$(\'cProto\'),id=b.getAttribute(\'data-proto\');if(!sel||!id)return;\n      if(sel.value!==id){sel.value=id;try{syncProtocolPicker(\'cProto\')}catch(e){}sel.dispatchEvent(new Event(\'change\',{bubbles:true}));try{syncAllProtocolToggle()}catch(e){}}\n      schedule();\n    });\n    if(window.MutationObserver){var mo=new MutationObserver(schedule);[\'cProto\',\'protocolBundleOptions\',\'cSubGroup\'].forEach(function(id){var e=$(id);if(e)mo.observe(e,{childList:true})})}\n    sync();\n  }\n  if(document.readyState===\'loading\')document.addEventListener(\'DOMContentLoaded\',boot);else boot();\n})();\n</script>\n<style id="onex-light-consistency">\n#resultModal #resVless{white-space:pre-wrap;word-break:break-all;max-height:190px;overflow:auto;direction:ltr;text-align:left}\n/* ============================================================\n   ONEX LIGHT THEME CONSISTENCY LAYER\n   Loaded last: turns every leftover dark surface into a light one\n   while html.light is active. Dark mode is untouched.\n   ============================================================ */\nhtml.light{--g-surf:#ffffff;--g-line:color-mix(in srgb,rgba(15,23,42,.10) 78%,var(--accent))}\nhtml.light body{background:var(--bg)!important}\n\n/* ---- dashboard home (hero, dock, metrics, chart, health, recent, quick, info, telegram) ---- */\nhtml.light #page-dash{--surface:#ffffff;--surface2:color-mix(in srgb,#ffffff 93%,var(--accent));--text:var(--t1);--muted:var(--t2);--dim:var(--t3);--line:color-mix(in srgb,rgba(15,23,42,.10) 75%,var(--accent))}\nhtml.light #page-dash .glass{background:linear-gradient(145deg,#ffffff,color-mix(in srgb,#ffffff 95%,var(--accent)))!important;border:1px solid var(--line)!important;box-shadow:0 14px 34px -20px rgba(15,23,42,.22)!important;color:var(--t1)!important;backdrop-filter:none!important;-webkit-backdrop-filter:none!important}\nhtml.light #page-dash .telegram{background:radial-gradient(circle at 50% 0,rgba(var(--accent-rgb),.14),transparent 55%),#ffffff!important}\nhtml.light #page-dash .glass h1,html.light #page-dash .glass h2,html.light #page-dash .glass h3,html.light #page-dash .glass strong,html.light #page-dash .glass b,html.light #page-dash .glass label,html.light #page-dash .glass dd,html.light #page-dash .glass td{color:var(--t1)}\nhtml.light #page-dash .glass small,html.light #page-dash .glass dt,html.light #page-dash .glass p{color:var(--t2)}\nhtml.light #page-dash .home-user,html.light #homeUser{color:var(--accent)!important;-webkit-text-fill-color:var(--accent)!important;background:none!important}\nhtml.light #page-dash .version,html.light #page-dash .home-core-pill,html.light #page-dash .range,html.light #page-dash .proto,html.light #page-dash .kv>div{background:color-mix(in srgb,#ffffff 90%,var(--accent))!important;border-color:var(--line)!important;color:var(--t2)!important}\nhtml.light #page-dash .version b,html.light #page-dash .version span{color:var(--t1)!important}\nhtml.light #page-dash .dock button,html.light #page-dash .quickitem,html.light #page-dash .metric,html.light #page-dash .kpi{background:color-mix(in srgb,#ffffff 95%,var(--accent))!important;border-color:var(--line)!important;color:var(--t1)!important}\nhtml.light #page-dash .track,html.light #page-dash .usage .bar{background:rgba(15,23,42,.08)!important}\nhtml.light #page-dash .bars i{background:color-mix(in srgb,var(--accent) 28%,#e2e8f0)}\nhtml.light #page-dash .range button.active{background:rgba(var(--accent-rgb),.16)!important;color:var(--t1)!important}\nhtml.light #page-dash .home-link-btn{color:var(--accent)!important;background:rgba(var(--accent-rgb),.08)!important}\n\n/* ---- top bar / swatches ---- */\nhtml.light .hexa-swatches{background:color-mix(in srgb,#ffffff 88%,var(--accent))!important;border-color:var(--g-line)!important}\nhtml.light .sb-logo.onex-approved-brand{background:linear-gradient(180deg,rgba(var(--accent-rgb),.07),transparent)!important}\n\n/* ---- configs page ---- */\nhtml.light .cfg-tools,html.light .group-filters,html.light .stats-range,html.light .admin-table-wrap,html.light .tg-tabs{background:#ffffff!important;border-color:var(--g-line)!important;color:var(--t1)!important}\nhtml.light .cfg-usage-ring::after,html.light .uptime-ring::after{background:#ffffff!important;border-color:var(--g-line)!important}\n\n/* ---- links / telegram cards ---- */\nhtml.light .tg-link{background:linear-gradient(135deg,#ffffff,color-mix(in srgb,#ffffff 92%,var(--accent)))!important;border-color:rgba(var(--accent-rgb),.28)!important}\nhtml.light .tg-link b,html.light .tg-link span{color:var(--t1)!important}\n\n/* ---- result modal (config created) & every link/code box ---- */\nhtml.light .link-box,html.light .code-box,html.light .sub-box,html.light pre,html.light textarea{background:color-mix(in srgb,#ffffff 94%,var(--accent))!important;color:var(--t1)!important;border:1px solid var(--g-line)!important}\nhtml.light .link-box{white-space:pre-wrap;word-break:break-all;max-height:190px;overflow:auto;font-family:\'JetBrains Mono\',monospace;font-size:11px;line-height:1.7;direction:ltr;text-align:left}\nhtml.light .modal,html.light .update-prompt-modal{background:#ffffff!important;color:var(--t1)!important}\nhtml.light .modal-bg{background:rgba(15,23,42,.38)!important}\n\n/* ---- theme page preview ---- */\nhtml.light .theme-preview-window{background:color-mix(in srgb,#ffffff 92%,var(--accent))!important;border-color:var(--g-line)!important}\nhtml.light .theme-mini-chart{background:#ffffff!important;border-color:var(--g-line)!important}\n\n/* ---- neon text that disappears on white ---- */\nhtml.light .group-hero-kicker,html.light .theme-kicker{color:color-mix(in srgb,var(--accent) 78%,#0f172a)!important}\nhtml.light .stats-kpi em{color:#059669!important}\nhtml.light #cfgx .cfgx-bundle label em{color:var(--accent)!important;background:rgba(var(--accent-rgb),.12)!important}\nhtml.light #page-logs .logs-clear-btn{background:linear-gradient(135deg,var(--accent),var(--purple))!important;color:#ffffff!important;border:0!important}\nhtml.light .summary-grid div{background:color-mix(in srgb,#ffffff 93%,var(--accent))!important;border-color:var(--g-line)!important}\nhtml.light .summary-grid b{color:var(--t1)!important}\nhtml.light .summary-grid span{color:var(--t2)!important}\nhtml.light #homeUser,html.light #page-dash .home-user,html.light #page-dash .healthrow .tag,html.light #cfgx .cfgx-bundle label em{color:color-mix(in srgb,var(--accent) 62%,#0f172a)!important;-webkit-text-fill-color:currentColor!important}\nhtml.light [class*="live"] em,html.light .tg-live em,html.light #page-telegram em{color:#059669!important}\nhtml.light .onex-topbar-brand{background:#ffffff!important;border-color:rgba(var(--accent-rgb),.16)!important}\nhtml.light #panelLogoutBtn{background:rgba(220,38,38,.08)!important;border-color:rgba(220,38,38,.25)!important;color:#dc2626!important}\nhtml.light #panelLogoutBtn span,html.light #panelLogoutBtn svg{color:#dc2626!important}\nhtml.light .all-proto-toggle i{background:#cbd5e1!important}\nhtml.light .all-proto-toggle input:checked+i{background:linear-gradient(135deg,var(--accent),var(--purple))!important}\n</style>\n<style id="onex-neural-sidebar-final">\n/* Approved 3D neural sidebar: premium depth, restrained glow, clear active state. */\n:root{--neural-cyan:#27d7ff;--neural-violet:#8b5cf6;--neural-pink:#e445c4}\n.sidebar{position:relative;isolation:isolate;overflow:hidden;background:linear-gradient(180deg,#090b1c 0%,#0b102b 52%,#071c35 100%)!important;border-right:1px solid rgba(39,215,255,.22)!important;box-shadow:14px 0 45px rgba(3,10,35,.34),inset -1px 0 rgba(255,255,255,.05)!important}\n.sidebar:before{content:"";position:absolute;inset:0;z-index:-1;pointer-events:none;opacity:.28;background:radial-gradient(circle at 14% 12%,rgba(228,69,196,.55) 0 1px,transparent 2px),radial-gradient(circle at 18% 28%,rgba(39,215,255,.6) 0 1px,transparent 2px),radial-gradient(circle at 9% 61%,rgba(139,92,246,.55) 0 1px,transparent 2px),linear-gradient(150deg,transparent 10%,rgba(228,69,196,.08) 42%,transparent 55%,rgba(39,215,255,.08) 88%);background-size:125px 180px,170px 230px,210px 260px,100% 100%}\n.sidebar:after{content:"";position:absolute;z-index:-1;left:-25px;top:120px;width:90px;height:580px;opacity:.22;pointer-events:none;background:linear-gradient(180deg,transparent,rgba(228,69,196,.7),rgba(39,215,255,.7),transparent);filter:blur(18px);transform:rotate(12deg)}\n.sidebar .nav{position:relative;padding:10px 11px 14px;z-index:1}\n.sidebar .nav-item{position:relative;min-height:48px;margin:6px 0!important;padding:9px 13px!important;border:1px solid rgba(148,163,255,.13)!important;border-radius:15px!important;background:linear-gradient(135deg,rgba(255,255,255,.045),rgba(19,26,65,.28))!important;color:#cbd5f5!important;box-shadow:inset 0 1px rgba(255,255,255,.06),0 7px 14px rgba(0,0,0,.16)!important;transform:translateZ(0);transition:transform .25s cubic-bezier(.16,1,.3,1),border-color .25s,background .25s,box-shadow .25s,color .25s!important}\n.sidebar .nav-item:before{content:"";position:absolute;right:-1px;top:10px;bottom:10px;width:3px;border-radius:9px;background:linear-gradient(180deg,var(--neural-pink),var(--neural-cyan));opacity:0;box-shadow:0 0 13px rgba(39,215,255,.8);transition:opacity .25s}\n.sidebar .nav-item:hover{transform:translateX(-3px) translateY(-1px);border-color:rgba(39,215,255,.48)!important;background:linear-gradient(135deg,rgba(39,215,255,.12),rgba(139,92,246,.13))!important;color:#fff!important;box-shadow:inset 0 1px rgba(255,255,255,.14),0 10px 22px rgba(39,215,255,.12)!important}\n.sidebar .nav-item.on{transform:translateX(-4px);border-color:rgba(39,215,255,.82)!important;background:linear-gradient(110deg,rgba(39,215,255,.2),rgba(92,78,210,.3) 58%,rgba(228,69,196,.18))!important;color:#fff!important;box-shadow:inset 0 1px rgba(255,255,255,.22),0 0 0 1px rgba(39,215,255,.13),0 10px 28px rgba(39,215,255,.2)!important}\n.sidebar .nav-item.on:before{opacity:1}\n.sidebar .nav-ico{width:23px!important;height:23px!important;min-width:23px!important;filter:drop-shadow(0 2px 4px rgba(39,215,255,.25));color:#b6c8ff!important;transition:color .25s,filter .25s,transform .25s!important}\n.sidebar .nav-item:nth-of-type(2n) .nav-ico{color:#c79cff!important}\n.sidebar .nav-item:hover .nav-ico,.sidebar .nav-item.on .nav-ico{color:var(--neural-cyan)!important;filter:drop-shadow(0 0 7px rgba(39,215,255,.65));transform:scale(1.07)}\n.sidebar .nav-label{font-size:12px!important;font-weight:800!important;letter-spacing:.01em;color:inherit!important}\n.sidebar .nav-sec{margin:15px 5px 7px!important;padding:0 8px!important;color:rgba(181,199,255,.55)!important;font-size:9px!important;letter-spacing:.13em!important}\n.sidebar .nav-sec-appearance{display:flex;align-items:center;gap:8px;color:#c5b5ff!important}\n.sidebar .nav-sec-appearance:after{content:"";height:1px;flex:1;background:linear-gradient(90deg,rgba(139,92,246,.5),transparent)}\n.sidebar .nav-item[data-page="theme"]{border-color:rgba(228,69,196,.3)!important;background:linear-gradient(135deg,rgba(228,69,196,.12),rgba(39,215,255,.08))!important}\n.sidebar .nav-item[data-page="theme"] .nav-ico{color:#ef9ae8!important}\n.sidebar .nav-new-badge{border:1px solid rgba(228,69,196,.45)!important;background:rgba(228,69,196,.16)!important;color:#ffb7f5!important}\n.sidebar .sb-foot{position:relative;z-index:2;margin:0 11px 14px;padding-top:12px;border-top:1px solid rgba(139,92,246,.38)!important}\n.sidebar .sb-foot .danger{width:100%;min-height:46px;border-radius:15px!important;background:linear-gradient(135deg,rgba(255,76,113,.18),rgba(234,88,12,.14))!important;border:1px solid rgba(255,93,118,.48)!important;color:#ff9dac!important;box-shadow:inset 0 1px rgba(255,255,255,.08),0 8px 20px rgba(255,62,100,.12)!important}\n.sidebar .sb-foot .danger:hover{background:linear-gradient(135deg,rgba(255,76,113,.3),rgba(234,88,12,.22))!important;color:#fff!important;transform:translateY(-2px)}\nhtml.light .sidebar{background:linear-gradient(180deg,#f7f8ff,#edf3ff 55%,#eaf8ff)!important;border-right-color:rgba(37,99,235,.18)!important;box-shadow:8px 0 28px rgba(37,99,235,.1)!important}\nhtml.light .sidebar:before{opacity:.14}\nhtml.light .sidebar .nav-item{background:linear-gradient(135deg,rgba(255,255,255,.96),rgba(239,244,255,.86))!important;border-color:rgba(37,99,235,.12)!important;color:#334155!important;box-shadow:0 6px 14px rgba(30,64,175,.08)!important}\nhtml.light .sidebar .nav-item.on{background:linear-gradient(110deg,rgba(39,215,255,.2),rgba(124,58,237,.13))!important;border-color:rgba(37,99,235,.55)!important;color:#0f172a!important}\nhtml.light .sidebar .nav-item[data-page="theme"]{background:linear-gradient(135deg,rgba(236,72,153,.1),rgba(37,99,235,.08))!important}\nhtml.light .sidebar .nav-label{color:inherit!important}\n</style><script>\n/* ONEX config cards (standalone component, image design) */\n(function(){\n  var I={\n    link:\'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15V5a2 2 0 0 1 2-2h10"/></svg>\',\n    qr:\'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><path d="M14 14h3v3h-3zM20 20h1M17 20h-3"/></svg>\',\n    user:\'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="8" r="4"/><path d="M4 21a8 8 0 0 1 16 0"/></svg>\',\n    clock:\'<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg>\'\n  };\n  var SHORT={\'vless-ws\':\'VLESS·WS\',\'xhttp-packet-up\':\'XHTTP·PACKET\',\'xhttp-stream-up\':\'XHTTP·UP\',\'xhttp-stream-one\':\'XHTTP·ONE\',\'trojan\':\'TROJAN\'};\n  function uidOf(l){return String(l.uuid||l.id||\'\')}\n  function menuId(uid){return \'ocxMenu_\'+uid.replace(/[^a-zA-Z0-9_-]/g,\'_\')}\n  function daysLeft(l){if(!l.expires_at)return null;var t=new Date(l.expires_at).getTime();if(isNaN(t))return null;return Math.ceil((t-Date.now())/86400000)}\n  function pctOf(l){var lim=Number(l.limit_bytes||0);return lim>0?Math.min(100,Math.round(Number(l.used_bytes||0)/lim*100)):null}\n\n  window.setCfgStatus=function(v,el){cfgStatusFilter=v;document.querySelectorAll(\'#cfgFilterRow [data-status]\').forEach(function(x){x.classList.toggle(\'on\',x===el)});renderConfigCards(getFilteredConfigs())};\n  window.getFilteredConfigs=function(){\n    var q=((document.getElementById(\'cfgSearch\')||{}).value||\'\').trim().toLowerCase();\n    var a=(__allLinks||[]).filter(function(l){\n      var dead=configExpired(l),active=l.active!==false&&!dead,p=pctOf(l);\n      if(cfgStatusFilter===\'active\'&&!active)return false;\n      if(cfgStatusFilter===\'off\'&&(dead||l.active!==false))return false;\n      if(cfgStatusFilter===\'expired\'&&!dead)return false;\n      if(cfgStatusFilter===\'hi\'&&!(p!==null&&p>=80))return false;\n      if(!q)return true;\n      return [l.label,l.name,l.protocol,l.protocol_label,l.uuid,l.id,l.sub,l.sub_url].map(function(x){return String(x||\'\').toLowerCase()}).some(function(x){return x.indexOf(q)>-1});\n    });\n    a.sort(function(x,y){return String(y.created_at||\'\').localeCompare(String(x.created_at||\'\'))});\n    return a;\n  };\n  window.updateConfigStats=function(){\n    var all=__allLinks||[],set=function(id,v){var e=document.getElementById(id);if(e)e.textContent=v};\n    set(\'cfgStatTotal\',all.length);\n    set(\'cfgStatActive\',all.filter(function(l){return l.active!==false&&!configExpired(l)}).length);\n    set(\'cfgStatExpired\',all.filter(configExpired).length);\n    set(\'cfgStatUsed\',fmtB(all.reduce(function(n,l){return n+Number(l.used_bytes||0)},0)));\n  };\n  window.closeConfigMenus=function(){__openConfigMenuUid=\'\';document.querySelectorAll(\'#cfgCards .ocx-menu.open\').forEach(function(m){m.classList.remove(\'open\')})};\n  window.toggleConfigMenu=function(e,uid){\n    e.preventDefault();e.stopPropagation();\n    var m=document.getElementById(menuId(uid));if(!m)return;\n    var was=m.classList.contains(\'open\');closeConfigMenus();\n    if(!was){m.classList.add(\'open\');__openConfigMenuUid=uid}\n  };\n  window.showConfigQr=function(uid){\n    var l=(window.__linksMap||{})[uid]||(__allLinks||[]).find(function(x){return uidOf(x)===String(uid)});\n    if(!l)return;var url=getLinkUrl(l);\n    if(!url){toast(\'لینکی برای QR نیست\');return}\n    openGroupQr(url.split(\'\\n\')[0],l.label||l.name||\'ONEX\');\n  };\n  window.ocxMenu=function(a,uid,e){\n    closeConfigMenus();\n    if(a===\'edit\')return openConfigEditor(e,uid);\n    if(a===\'info\'){window.open(\'/info/\'+encodeURIComponent(uid),\'_blank\',\'noopener\');return}\n    if(a===\'reset\')return resetUsage(uid);\n    if(a===\'delete\')return deleteLink(uid);\n  };\n  window.renderConfigCards=function(arr){\n    var box=document.getElementById(\'cfgCards\');if(!box)return;\n    var sel=new Set([].slice.call(document.querySelectorAll(\'#cfgCards .cfg-chk:checked\')).map(function(c){return String(c.value)}));\n    var openUid=__openConfigMenuUid||\'\';\n    updateConfigStats();\n    var vc=document.getElementById(\'cfgVisibleCount\');if(vc)vc.textContent=(arr||[]).length+\' مورد\';\n    if(!arr||!arr.length){box.innerHTML=\'<div class="ocx-empty">\'+((__allLinks||[]).length?\'کانفیگی با این فیلتر پیدا نشد\':\'هنوز کانفیگی نساختی. از «کانفیگ جدید» شروع کن\')+\'</div>\';updateBulkBar();return}\n    box.innerHTML=arr.map(function(l){\n      var uid=uidOf(l),s=esc(uid),dead=configExpired(l),on=l.active!==false,p=pctOf(l),d=daysLeft(l),used=Number(l.used_bytes||0);\n      var ring=dead?\'var(--o-red)\':(p===null?\'var(--o-a2)\':(p>=80?\'var(--o-yellow)\':\'var(--o-a)\'));\n      var st=dead?\'<span class="st-dead">● منقضی</span>\':(on?\'<span class="st-on">● فعال</span>\':\'<span class="st-off">● خاموش</span>\');\n      var exp=d===null?\'<span>\'+I.clock+\' بدون انقضا</span>\':(d<=0?\'<span class="gone">\'+I.clock+\' منقضی شده</span>\':\'<span class="\'+(d<=5?\'warn\':\'\')+\'">\'+I.clock+\' \'+d+\' روز مانده</span>\');\n      var proto=SHORT[l.protocol]||String(l.protocol||\'\').toUpperCase();\n      if(l.all_protocols)proto+=\' +MIX\';\n      return \'<article class="ocx-card\'+(dead?\' dead\':\'\')+\'" data-uid="\'+s+\'">\'\n        +\'<div class="ocx-head">\'\n          +\'<div class="ocx-ring" style="--pct:\'+(p===null?100:p)+\';--ring:\'+ring+\'"><div class="ocx-ring-c"><b>\'+(p===null?\'∞\':p+\'%\')+\'</b><small>\'+esc(fmtB(used))+\'</small></div></div>\'\n          +\'<div class="ocx-info"><div class="ocx-name">\'+esc(l.label||l.name||uid.slice(0,8))+\'</div><div class="ocx-proto">\'+esc(proto)+\'</div>\'\n          +\'<div class="ocx-meta">\'+st+\'<span>\'+I.user+\' \'+Number(l.connected_ips||0)+\' اتصال</span>\'+exp+\'</div></div>\'\n          +\'<button type="button" class="ocx-toggle\'+(on?\' on\':\'\')+\'" aria-pressed="\'+on+\'" aria-label="روشن/خاموش" onclick="toggleConfigActive(event,\\\'\'+s+\'\\\',\'+(on?\'false\':\'true\')+\')"></button>\'\n        +\'</div>\'\n        +\'<div class="ocx-rule"></div>\'\n        +\'<div class="ocx-actions">\'\n          +\'<label class="ocx-chk"><input type="checkbox" class="cfg-chk" value="\'+s+\'" onchange="updateBulkBar()" aria-label="انتخاب"></label>\'\n          +\'<button type="button" class="ocx-act" onclick="copyLinkById(\\\'\'+s+\'\\\')">\'+I.link+\' لینک</button>\'\n          +\'<button type="button" class="ocx-act" onclick="copySubById(\\\'\'+s+\'\\\')">ساب</button>\'\n          +\'<button type="button" class="ocx-act" onclick="showConfigQr(\\\'\'+s+\'\\\')">\'+I.qr+\' QR</button>\'\n          +\'<button type="button" class="ocx-dots" aria-label="بیشتر" onclick="toggleConfigMenu(event,\\\'\'+s+\'\\\')">⋮</button>\'\n        +\'</div>\'\n        +\'<div class="ocx-menu" id="\'+menuId(uid)+\'" onclick="event.stopPropagation()">\'\n          +\'<button type="button" onclick="ocxMenu(\\\'edit\\\',\\\'\'+s+\'\\\',event)">ویرایش کانفیگ</button>\'\n          +\'<button type="button" onclick="ocxMenu(\\\'info\\\',\\\'\'+s+\'\\\',event)">صفحه اطلاعات</button>\'\n          +\'<button type="button" onclick="ocxMenu(\\\'reset\\\',\\\'\'+s+\'\\\',event)">ریست مصرف</button>\'\n          +\'<button type="button" class="danger" onclick="ocxMenu(\\\'delete\\\',\\\'\'+s+\'\\\',event)">حذف کانفیگ</button>\'\n        +\'</div>\'\n      +\'</article>\';\n    }).join(\'\');\n    box.querySelectorAll(\'.cfg-chk\').forEach(function(c){c.checked=sel.has(String(c.value))});\n    if(openUid){var m=document.getElementById(menuId(openUid));if(m){m.classList.add(\'open\');__openConfigMenuUid=openUid}else{__openConfigMenuUid=\'\'}}\n    updateBulkBar();\n  };\n  document.addEventListener(\'click\',function(e){if(!e.target.closest(\'.ocx-menu\')&&!e.target.closest(\'.ocx-dots\'))closeConfigMenus()});\n  try{if(document.getElementById(\'cfgCards\')&&Array.isArray(__allLinks))renderConfigCards(getFilteredConfigs())}catch(err){}\n})();\n</script>\n<style id="onex-layout-fix">\n/* ============================================================\n   ONEX LAYOUT FIX (desktop + mobile)\n   1) Desktop: the neural sidebar block forced position:relative,\n      so the sidebar sat in the flex row AND .main kept its\n      margin-right => content pushed ~2x sidebar width and the\n      sidebar scrolled away with the page. Restore fixed sidebar.\n   2) Collapse toggle was clipped by overflow:hidden.\n   3) Modals living inside .main were trapped under the sidebar\n      and positioned relative to .main (backdrop-filter).\n   4) Phone: doubled top offset (body padding + main padding).\n   ============================================================ */\n@media (min-width:641px){\n  .sidebar{position:fixed!important;top:0;right:0;bottom:0;left:auto;height:100vh;height:100dvh;\n    overflow:visible;\n    clip-path:polygon(0 0,100% 0,100% 100%,0 100%,0 calc(50% + 34px),-34px calc(50% + 34px),-34px calc(50% - 34px),0 calc(50% - 34px))}\n  .sidebar .nav{min-height:0;overflow-y:auto;overflow-x:hidden}\n  .sidebar .sb-foot{flex:0 0 auto}\n  .main{margin-right:var(--sb)}\n  .main.expanded{margin-right:var(--sb-c)}\n  .sidebar.collapsed .nav-item{padding:9px 0!important;justify-content:center}\n  .onex-topbar{min-width:0}\n  .onex-topbar .top-server{min-width:0;overflow:hidden}\n  .onex-topbar .top-server small{min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n}\n/* Nav pills: full width of the nav column (was 100%-20px with 0 margin => lopsided). */\n.sidebar .nav-item{width:100%!important}\n/* Modals inside .main: lift .main above sidebar/top bar while one is open. */\n.main:has(.modal-bg.open),.main:has(.logs-modal-bg.open){z-index:1400!important;-webkit-backdrop-filter:none!important;backdrop-filter:none!important}\n@media (max-width:640px){\n  .main,.main.expanded{padding-top:12px!important}\n}\n@media (max-width:480px){\n  .sidebar{top:56px!important;height:calc(100dvh - 56px)!important}\n  .main,.main.expanded{padding-top:10px!important}\n}\n</style>\n<style id="onex-notify-fix">/* 1.3.9: panel is moved to <body> by JS and positioned under the bell,\n   so parent stacking contexts (backdrop-filter/transform) can\'t hide it behind the dashboard. */\n.top-notify-panel{position:fixed!important;right:24px;top:70px;width:320px;max-width:calc(100vw - 24px)!important;max-height:min(70vh,520px);overflow:auto;z-index:2147483000!important}\n.top-notify-panel[hidden]{display:none!important}\n@media (max-width:700px){.top-notify-panel{right:12px;width:calc(100vw - 24px);max-width:none;top:66px}}\n.update-prompt-bg{z-index:2147483100!important;background:rgba(1,7,18,.72);backdrop-filter:blur(9px);-webkit-backdrop-filter:blur(9px)}\n.update-prompt-modal{width:min(440px,calc(100vw - 28px));padding:26px 24px 22px;border:1px solid rgba(88,180,255,.3);border-radius:22px;background:linear-gradient(160deg,rgba(18,24,42,.97),rgba(8,10,20,.98));box-shadow:0 28px 80px rgba(0,0,0,.55);text-align:center;color:var(--t1)}\n.update-prompt-icon{width:58px;height:58px;margin:0 auto 13px;border-radius:18px;display:grid;place-items:center;font-size:28px;font-weight:900;color:#fff;background:linear-gradient(135deg,var(--accent,#38d9ff),var(--accent2,#8b5cf6))}\n.update-prompt-title{font-size:18px;font-weight:900;margin-bottom:8px}.update-prompt-text{font-size:13px;line-height:1.9;color:var(--t2)}\n.update-prompt-version{margin:12px 0;padding:9px 12px;border-radius:11px;background:rgba(37,99,235,.08);border:1px solid rgba(96,165,250,.13);color:var(--t3);font-size:11px;direction:ltr}\n.update-prompt-actions{display:flex;gap:9px;margin-top:16px}.update-prompt-actions .btn{flex:1;height:42px}\n</style>\n</body>\n</html>\n'




@app.get(
    "/dashboard",
    response_class=HTMLResponse,
)
async def dashboard(
    request: Request,
):

    if not await is_valid_session(
        request.cookies.get(
            SESSION_COOKIE
        )
    ):
        return RedirectResponse(
            "/login"
        )

    await ensure_default_categories()

    dashboard_html = DASHBOARD_HTML.replace("__ONEX_VERSION__", str(APP_VERSION))

    return HTMLResponse(
        dashboard_html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


# ============================================================
# TEST
# ============================================================

@app.get(
    "/test-ws",
    response_class=HTMLResponse,
)
async def test_ws():

    return HTMLResponse(
        """
        <script>
        location.href='/dashboard'
        </script>
        """
    )


# ============================================================
# GLOBAL ERROR HANDLER
# ============================================================

@app.exception_handler(Exception)
async def global_exception_handler(
    request: Request,
    exc: Exception,
):

    stats[
        "total_errors"
    ] += 1

    error_logs.append(
        {
            "error":
                str(exc),

            "path":
                str(request.url),

            "method":
                request.method,

            "time":
                datetime.now().isoformat(),
        }
    )

    logger.exception(
        "Unhandled exception: %s %s",
        request.method,
        request.url,
    )

    # API requests
    if (
        request.url.path.startswith(
            "/api/"
        )
        or request.url.path == "/stats"
    ):

        return JSONResponse(
            {
                "ok": False,
                "error":
                    str(exc)
                or "internal server error",
            },
            status_code=500,
        )

    return HTMLResponse(
        """
        <html lang="fa" dir="rtl">
        <body style="
            background:#07070a;
            color:#fff;
            font-family:sans-serif;
            padding:40px;
        ">
            <h2>
            خطای داخلی PX Panel
            </h2>

            <p>
            لطفاً لاگ Railway را بررسی کنید.
            </p>
        </body>
        </html>
        """,
        status_code=500,
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        workers=1,
    )
