# fb_report/constants.py
import os
import math
from datetime import datetime
from pytz import timezone

from facebook_business.api import FacebookAdsApi

ALMATY_TZ = timezone("Asia/Almaty")

# ======== Facebook креды =========
ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
APP_ID = os.getenv("FB_APP_ID", "1336645834088573")
APP_SECRET = os.getenv("FB_APP_SECRET", "01bf23c5f726c59da318daa82dd0e9dc")

if ACCESS_TOKEN:
    FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
# если токена нет — просто не инициализируем, чтобы локально не падать


def _get_env(*names: str, default: str = "") -> str:
    for n in names:
        v = os.getenv(n, "")
        if v:
            return v
    return default


# ======== Telegram =========
TELEGRAM_TOKEN = _get_env("TG_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN")
DEFAULT_REPORT_CHAT = os.getenv("TG_CHAT_ID", "-1002679045097")  # строка
AUTOPILOT_CHAT_ID = os.getenv("AUTOPILOT_CHAT_ID", "")  # строка

if not TELEGRAM_TOKEN or ":" not in TELEGRAM_TOKEN:
    raise RuntimeError(
        "TG_BOT_TOKEN / TELEGRAM_BOT_TOKEN / TELEGRAM_TOKEN не задан или некорректен."
    )

# Приватный доступ
ALLOWED_USER_IDS = {
    253181449,  # Andrey
}
ALLOWED_CHAT_IDS = {str(DEFAULT_REPORT_CHAT), "-1002679045097"}  # как строки

SUPERADMIN_USER_ID = int(os.getenv("SUPERADMIN_USER_ID", "253181449") or 253181449)

# ======== Пути / файлы =========
DATA_DIR = os.getenv("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

ACCOUNTS_JSON = os.getenv("ACCOUNTS_JSON_PATH", os.path.join(DATA_DIR, "accounts.json"))
REPO_ACCOUNTS_JSON = os.path.join(os.path.dirname(__file__), "accounts.json")

REPORT_CACHE_FILE = os.path.join(DATA_DIR, "report_cache.json")
REPORT_CACHE_TTL = int(os.getenv("REPORT_CACHE_TTL", "3600"))  # сек, по умолчанию 1 час

DAILY_REPORT_CACHE_FILE = os.path.join(DATA_DIR, "daily_report_cache.json")

MORNING_REPORT_STATE_FILE = os.path.join(DATA_DIR, "morning_report_state.json")
MORNING_REPORT_CACHE_FILE = os.path.join(DATA_DIR, "morning_report_cache.json")
MORNING_REPORT_CACHE_TTL = int(os.getenv("MORNING_REPORT_CACHE_TTL", "43200"))

CLIENT_GROUPS_FILE = os.path.join(DATA_DIR, "client_groups.json")
CLIENT_RATE_LIMITS_FILE = os.path.join(DATA_DIR, "client_rate_limits.json")

SYNC_META_FILE = os.path.join(DATA_DIR, "sync_meta.json")

# ========= КУРС USD→KZT =========
FX_RATE_OVERRIDE = float(os.getenv("FX_RATE_OVERRIDE", "0") or 0.0)


def usd_to_kzt() -> float:
    if FX_RATE_OVERRIDE > 0:
        return FX_RATE_OVERRIDE
    return 530.0


def kzt_round_up_1000(v: float) -> int:
    return int(math.ceil(v / 1000.0) * 1000)


# ========= ФОЛБЭКИ =========
AD_ACCOUNTS_FALLBACK = [
    "act_1415004142524014",
    "act_719853653795521",
    "act_1206987573792913",
    "act_1108417930211002",
    "act_2342025859327675",
    "act_844229314275496",
    "act_1333550570916716",
    "act_195526110289107",
    "act_2145160982589338",
    "act_508239018969999",
    "act_1357165995492721",
    "act_798205335840576",
    "act_806046635254439",
]

ACCOUNT_NAMES = {
    "act_1415004142524014": "JanymSoul - Астана",
    "act_719853653795521": "JanymSoul - Караганда",
    "act_1206987573792913": "Janym Soul – Павлодар",
    "act_1108417930211002": "Janym Soul – Актау (janymsoul/1)",
    "act_2342025859327675": "Janym Soul – Атырау (janymsoul_guw)",
    "act_844229314275496": "Janym Soul – Актобе",
    "act_1333550570916716": "Janym Soul – Алматы",
    "act_195526110289107": "JanymSoul - Тараз",
    "act_2145160982589338": "JanymSoul - Шымкент",
    "act_508239018969999": "fitness point",
    "act_1357165995492721": "Aria Stepi / Ария степи",
    "act_798205335840576": "JanymSoul – Инвестиции и франшиза",
    "act_806046635254439": "WonderStage WS",
}

EXCLUDED_AD_ACCOUNT_IDS = {"act_1042955424178074", "act_4030694587199998"}
EXCLUDED_NAME_KEYWORDS = {"kense", "кенсе"}


# ========= ВЕРСИЯ БОТА / ФУНКЦИОНАЛ =========


# ========= ГЛОБАЛЬНЫЕ ACTION TYPES =========

# Единственная правильная метрика лидов:
# - Website Submit Applications
# - SubmitApplication
# (без выбора из UI и без fallback'ов)
LEADS_ACTIONS = [
    "Website Submit Applications",
    "SubmitApplication",
]

# Backward-compatible name used across the codebase.
LEAD_ACTION_TYPES = set(LEADS_ACTIONS)

BOT_VERSION = "1.3.0"

# Список строк, которые показываются по кнопке "ℹ️ Версия" в боте.
# Сначала базовый доступный функционал, затем ключевые новые возможности.
BOT_CHANGELOG = [
    "Последние обновления (v1.3.0):",
    "— Автопилот: отдельный чат для сообщений (ENV AUTOPILOT_CHAT_ID + сохранение через кнопку/команду)",
    "— Автопилот: защита от лимита FB API (code 17) — останавливает прогон, пишет понятный статус, планирует повтор с джиттером",
    "— Меньше запросов к FB: кэш кампаний/адсетов/объявлений и bulk insights для аналитики",
    "— Heatmap AUTO_LIMITS: fallback при лимите — «данные из кэша / недоступна из-за лимита», без применения бюджетов",
    "",
    "Ранее (v1.2.1):",
    "— Тепловая карта перенесена в «Мониторинг» и объединена в один раздел (режимы: по часам / по дням недели / сводная + ИИ)",
    "— Ссылки на активные объявления Instagram теперь фильтруются по фактическому статусу показа (effective_status)",
    "",
    "Базовый функционал:",
    "— Отчёты по всем аккаунтам (сегодня / вчера / прошедшая неделя)",
    "— Отчёты по отдельному аккаунту (общий, по кампаниям, адсетам, объявлениям)",
    "— Биллинг: текущие балансы и прогноз списаний",
    "— Настройки кабинетов и CPA-алёртов по аккаунту",
    "— Ссылки на активные объявления в Instagram",
    "",
    "Мониторинг и аналитика:",
    "— Сравнительные отчёты по периодам (день / неделя / кастомные диапазоны)",
    "— Аномалии по аккаунтам",
    "— Фокус-ИИ: краткие рекомендации по аккаунту и бюджетам",
    "— Тепловая карта (💬+📩): по часам / по дням недели / сводная неделя+часы с ИИ",
    "",
    "CPA-алёрты:",
    "— CPA-алёрты по аккаунту с окном работы 10:00–22:00 (Asia/Almaty)",
    "— CPA-алёрты по кампаниям, адсетам и объявлениям с отдельными таргетами",
    "— Тихий режим для объявлений и управление креативами из алёртов",
    "",
    "Команды Telegram (доступны вам):",
    "— /start — открыть главное меню",
    "— /help — список основных команд",
    "— /billing — биллинги и прогноз списаний",
    "— /sync_accounts — синхронизация аккаунтов из BM",
    "— /whoami — показать ваш user_id и текущий chat_id",
    "— /heatmap <act_id> — тепловая карта адсетов за 7 дней по аккаунту",
    "— /version — текущая версия бота и краткое описание возможностей",
]
