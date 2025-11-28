# config.py
import os
from pytz import timezone

# ==== TIMEZONE ====
ALMATY_TZ = timezone("Asia/Almaty")

# ==== ENV ====
def _get_env(*names, default=""):
    """Возвращает первое найденное значение переменной среды."""
    for n in names:
        v = os.getenv(n, "")
        if v:
            return v
    return default

# Facebook API
FB_ACCESS_TOKEN = _get_env("FB_ACCESS_TOKEN")
FB_APP_ID = _get_env("FB_APP_ID")
FB_APP_SECRET = _get_env("FB_APP_SECRET")

# Telegram
TELEGRAM_TOKEN = _get_env("TG_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN")
DEFAULT_REPORT_CHAT = _get_env("TG_CHAT_ID", default="-1002679045097")

# Доступ
ALLOWED_USER_IDS = {
    253181449,  # Андрей
}
ALLOWED_CHAT_IDS = {
    str(DEFAULT_REPORT_CHAT),
}

# === Currency ===
FX_RATE_OVERRIDE = float(os.getenv("FX_RATE_OVERRIDE", "0") or 0.0)

def usd_to_kzt() -> float:
    """Фиксированный курс USD→KZT."""
    if FX_RATE_OVERRIDE > 0:
        return FX_RATE_OVERRIDE
    return 540.0  # дефолт

# --- DIRECTORIES ---
DATA_DIR = os.getenv("DATA_DIR", "/data")
INSIGHTS_DIR = os.path.join(DATA_DIR, "insights_cache")

# Ensure dirs exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(INSIGHTS_DIR, exist_ok=True)

# Files
ACCOUNTS_JSON = os.path.join(DATA_DIR, "accounts.json")
REPORT_CACHE_FILE = os.path.join(DATA_DIR, "report_cache.json")
SYNC_META_FILE = os.path.join(DATA_DIR, "sync_meta.json")

REPORT_CACHE_TTL = int(os.getenv("REPORT_CACHE_TTL", "3600"))  # seconds
