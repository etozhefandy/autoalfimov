# fb_report.py - версия с логированием истории, локальным кэшем инсайтов и улучшенным сравнением периодов

import os
import json
import math
import re
import shutil
from datetime import datetime, timedelta, time

from telegram.error import BadRequest
from pytz import timezone

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from billing_watch import init_billing_watch

# === AUTOPILАТ / AUTOPILOT ===
from autopilat.engine import get_recommendations_ui, handle_autopilot_action
from autopilat.ui import (
    autopilot_main_menu,
    autopilot_submode_menu,
    build_recommendations_ui,
    recommendation_buttons,
    confirm_action_buttons,
)
from autopilat.actions import (
    apply_budget_change,
    disable_entity,
    parse_manual_input,
    can_disable,
)

# --- history_store: мягкий импорт, чтобы бот не падал, если файла нет ---
try:
    from history_store import append_snapshot, prune_old_history

    HISTORY_STORE_AVAILABLE = True
except ImportError:
    HISTORY_STORE_AVAILABLE = False

    def append_snapshot(*args, **kwargs):
        # заглушка, чтобы не ломать работу бота
        return

    def prune_old_history(*args, **kwargs):
        # заглушка, чтобы не ломать работу бота
        return


# ================== КОНСТАНТЫ / КРЕДЫ ==================

ALMATY_TZ = timezone("Asia/Almaty")

ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN", "")
APP_ID = os.getenv("FB_APP_ID", "1336645834088573")
APP_SECRET = os.getenv("FB_APP_SECRET", "01bf23c5f726c59da318daa82dd0e9dc")
if not ACCESS_TOKEN:
    pass
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)


def _get_env(*names, default=""):
    for n in names:
        v = os.getenv(n, "")
        if v:
            return v
    return default


# Telegram токен и чат
TELEGRAM_TOKEN = _get_env("TG_BOT_TOKEN", "TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN")
DEFAULT_REPORT_CHAT = os.getenv("TG_CHAT_ID", "-1002679045097")  # строка

if not TELEGRAM_TOKEN or ":" not in TELEGRAM_TOKEN:
    raise RuntimeError(
        "TG_BOT_TOKEN / TELEGRAM_BOT_TOKEN / TELEGRAM_TOKEN не задан или некорректен."
    )

# === Приватный доступ ===
ALLOWED_USER_IDS = {
    253181449,  # Andrey
}
ALLOWED_CHAT_IDS = {str(DEFAULT_REPORT_CHAT), "-1002679045097"}  # как строки

# ======= ПУТИ / ФАЙЛЫ =========
DATA_DIR = os.getenv("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

ACCOUNTS_JSON = os.getenv("ACCOUNTS_JSON_PATH", os.path.join(DATA_DIR, "accounts.json"))
REPO_ACCOUNTS_JSON = os.path.join(os.path.dirname(__file__), "accounts.json")

REPORT_CACHE_FILE = os.path.join(DATA_DIR, "report_cache.json")
REPORT_CACHE_TTL = int(os.getenv("REPORT_CACHE_TTL", "3600"))  # сек, по умолчанию 1 час

SYNC_META_FILE = os.path.join(DATA_DIR, "sync_meta.json")


def _atomic_write_json(path: str, obj: dict):
    tmp = f"{path}.tmp"
    bak = f"{path}.bak"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    try:
        if os.path.exists(path):
            shutil.copy2(path, bak)
    except Exception:
        pass
    os.replace(tmp, path)


def _ensure_accounts_file():
    if not os.path.exists(ACCOUNTS_JSON):
        if os.path.exists(REPO_ACCOUNTS_JSON):
            try:
                shutil.copy2(REPO_ACCOUNTS_JSON, ACCOUNTS_JSON)
                return
            except Exception:
                pass
        _atomic_write_json(ACCOUNTS_JSON, {})


_ensure_accounts_file()

# --- ЛОКАЛЬНОЕ ХРАНИЛИЩЕ ИНСАЙТОВ (основной источник данных) ---

INSIGHTS_DIR = os.path.join(DATA_DIR, "insights_cache")
os.makedirs(INSIGHTS_DIR, exist_ok=True)


def _insight_file(aid: str) -> str:
    safe = aid.replace("act_", "")
    return os.path.join(INSIGHTS_DIR, f"{safe}.json")


def load_local_insights(aid: str) -> dict:
    """Читает локальный файл с инсайтами аккаунта."""
    path = _insight_file(aid)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_local_insights(aid: str, d: dict):
    """Атомарно сохраняет локальный файл с инсайтами."""
    path = _insight_file(aid)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


# ========= КУРС USD→KZT =========
FX_RATE_OVERRIDE = float(os.getenv("FX_RATE_OVERRIDE", "0") or 0.0)


def usd_to_kzt() -> float:
    if FX_RATE_OVERRIDE > 0:
        return FX_RATE_OVERRIDE
    return 540.0


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


# ========== STORES / META ==========
def load_accounts() -> dict:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_accounts(d: dict):
    _atomic_write_json(ACCOUNTS_JSON, d)


def load_sync_meta() -> dict:
    try:
        with open(SYNC_META_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_sync_meta(d: dict):
    _atomic_write_json(SYNC_META_FILE, d)


def human_last_sync() -> str:
    meta = load_sync_meta()
    iso = meta.get("last_sync")
    if not iso:
        return "нет данных"
    try:
        dt = datetime.fromisoformat(iso)
        if not dt.tzinfo:
            dt = ALMATY_TZ.localize(dt)
        dt = dt.astimezone(ALMATY_TZ)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "нет данных"


def _norm_act(aid: str) -> str:
    aid = str(aid).strip()
    return aid if aid.startswith("act_") else "act_" + aid


def get_account_name(aid: str) -> str:
    store = load_accounts()
    if aid in store and store[aid].get("name"):
        return store[aid]["name"]
    return ACCOUNT_NAMES.get(aid, aid)


def get_enabled_accounts_in_order() -> list[str]:
    """
    Для отчётов и фоновых джобов:
    - сначала все включённые аккаунты,
    - потом выключенные (чтобы были внизу списков).
    """
    store = load_accounts()
    if not store:
        return AD_ACCOUNTS_FALLBACK
    enabled = [acc for acc, row in store.items() if row.get("enabled", True)]
    disabled = [acc for acc, row in store.items() if not row.get("enabled", True)]
    ordered = enabled + disabled
    return ordered or AD_ACCOUNTS_FALLBACK


def iter_enabled_accounts_only():
    """Итерируем только включённые аккаунты (enabled=True)."""
    store = load_accounts()
    ids = get_enabled_accounts_in_order()
    if not store:
        # если нет конфига, считаем все аккаунты включёнными (fallback)
        for aid in ids:
            yield aid
        return
    for aid in ids:
        if store.get(aid, {}).get("enabled", True):
            yield aid


def looks_excluded(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_NAME_KEYWORDS)


def upsert_from_bm() -> dict:
    """
    Добавляет новые аккаунты и обновляет ИМЕНА.
    Настройки enabled/metrics/alerts не затирает.
    Также сохраняет время последней синхронизации.
    """
    store = load_accounts()
    me = User(fbid="me")
    fetched = list(me.get_ad_accounts(fields=["account_id", "name", "account_status"]))
    added, updated, skipped = 0, 0, 0
    for it in fetched:
        aid = _norm_act(it.get("account_id"))
        name = it.get("name") or aid
        if aid in EXCLUDED_AD_ACCOUNT_IDS or looks_excluded(name):
            skipped += 1
            continue
        ACCOUNT_NAMES.setdefault(aid, name)
        if aid in store:
            if name and store[aid].get("name") != name:
                store[aid]["name"] = name
                updated += 1
        else:
            store[aid] = {
                "name": name,
                "enabled": True,
                "metrics": {"messaging": True, "leads": False},
                "alerts": {"enabled": False, "target_cpl": 0.0},
            }
            added += 1
    save_accounts(store)

    last_sync_iso = datetime.now(ALMATY_TZ).isoformat()
    meta = load_sync_meta()
    meta["last_sync"] = last_sync_iso
    save_sync_meta(meta)

    return {
        "added": added,
        "updated": updated,
        "skipped": skipped,
        "total": len(store),
        "last_sync": last_sync_iso,
    }


# ========== HELPERS ==========
def is_active(aid: str) -> bool:
    try:
        st = AdAccount(aid).api_get(fields=["account_status"])["account_status"]
        return st == 1
    except Exception:
        return False


def fmt_int(n) -> str:
    try:
        return f"{int(float(n)):,}".replace(",", " ")
    except Exception:
        return "0"


def extract_actions(insight) -> dict:
    acts = insight.get("actions", []) or []
    return {a.get("action_type"): float(a.get("value", 0)) for a in acts}


def metrics_flags(aid: str) -> dict:
    st = load_accounts().get(aid, {})
    m = st.get("metrics", {}) or {}
    return {
        "messaging": bool(m.get("messaging", False)),
        "leads": bool(m.get("leads", False)),
    }


def _blend_totals(ins):
    """Возвращает (spend, msg_conv, lead_conv, blended_conv, blended_cpa or None)"""
    acts = extract_actions(ins)
    spend = float(ins.get("spend", 0) or 0)
    msgs = int(
        acts.get("onsite_conversion.messaging_conversation_started_7d", 0) or 0
    )
    leads = int(
        acts.get("Website Submit Applications", 0)
        or acts.get("offsite_conversion.fb_pixel_submit_application", 0)
        or acts.get("offsite_conversion.fb_pixel_lead", 0)
        or acts.get("lead", 0)
        or 0
    )
    total = msgs + leads
    blended = (spend / total) if total > 0 else None
    return spend, msgs, leads, total, blended

# ========== ТЕПЛОВАЯ КАРТА НА ОСНОВЕ ИСТОРИИ ==========
def _history_path_for(aid: str) -> str:
    """
    Путь к history-файлу для аккаунта, в формате как в history_store.py:
    DATA_DIR/history/history_<actid>.jsonl
    """
    safe = aid.replace("act_", "")
    history_dir = os.path.join(DATA_DIR, "history")
    os.makedirs(history_dir, exist_ok=True)
    return os.path.join(history_dir, f"history_{safe}.jsonl")


def build_heatmap_for_account(aid: str, name_getter, mode: str = "7") -> str:
    """
    Строит текстовую тепловую карту заявок (переписки+лиды) по часам
    за последние N дней, на основе логов history_store (append_snapshot).

    mode:
      "7"     — 7 дней
      "14"    — 14 дней
      "month" — с начала месяца до вчера
    """
    now = datetime.now(ALMATY_TZ)
    today = now.date()

    if mode == "7":
        days = 7
        since_date = today - timedelta(days=days - 1)
        until_date = today
        title = f"за последние {days} дней"
    elif mode == "14":
        days = 14
        since_date = today - timedelta(days=days - 1)
        until_date = today
        title = f"за последние {days} дней"
    elif mode == "month":
        since_date = today.replace(day=1)
        until_date = today
        days = (until_date - since_date).days + 1
        title = f"за {since_date.strftime('%m.%Y')}"
    else:
        days = 7
        since_date = today - timedelta(days=days - 1)
        until_date = today
        title = f"за последние {days} дней"

    path = _history_path_for(aid)
    if not os.path.exists(path):
        return (
            f"🔥 Тепловая карта для {name_getter(aid)}\n"
            f"Нет истории для построения тепловой карты."
        )

    # Готовим сетку: 24 часа x N дней
    dates = [since_date + timedelta(days=i) for i in range(days)]
    date_index = {d: idx for idx, d in enumerate(dates)}
    grid = [[0 for _ in range(days)] for _ in range(24)]

    # Для корректных дельт по дню
    last_total_per_day = {}

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue

            ts_str = obj.get("ts")
            if not ts_str:
                continue
            try:
                dt = datetime.fromisoformat(ts_str)
            except Exception:
                continue

            d = dt.date()
            if d < since_date or d > until_date:
                continue

            msgs = int(obj.get("msgs", 0) or 0)
            leads = int(obj.get("leads", 0) or 0)
            total = msgs + leads

            prev = last_total_per_day.get(d, 0)
            delta = max(total - prev, 0)
            last_total_per_day[d] = total

            hour = dt.hour
            col = date_index.get(d)
            if col is None:
                continue

            grid[hour][col] += delta

    # Находим максимум для нормализации
    max_val = 0
    for h in range(24):
        for c in range(days):
            if grid[h][c] > max_val:
                max_val = grid[h][c]

    def cell_char(v: int) -> str:
        if max_val <= 0 or v <= 0:
            return "·"
        ratio = v / max_val
        if ratio < 0.25:
            return "░"
        elif ratio < 0.5:
            return "▒"
        elif ratio < 0.75:
            return "▓"
        else:
            return "█"

    header = "    " + " ".join(d.strftime("%d") for d in dates)
    lines = [header]
    for hour in range(24):
        row = f"{hour:02d}: " + " ".join(cell_char(grid[hour][c]) for c in range(days))
        lines.append(row)

    name = name_getter(aid)
    period_label = f"{since_date.strftime('%d.%m.%Y')}–{until_date.strftime('%d.%m.%Y')}"
    body = "\n".join(lines)

    return (
        f"🔥 Тепловая карта для <b>{name}</b>\n"
        f"Период: {period_label} ({title})\n"
        f"Основа: заявки (переписки+лиды) по логам за каждый час.\n\n"
        f"<pre>{body}</pre>"
    )


# ========== КЕШ ОТЧЁТОВ ==========
def _load_report_cache() -> dict:
    try:
        with open(REPORT_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_report_cache(d: dict):
    _atomic_write_json(REPORT_CACHE_FILE, d)


def _period_key(period) -> str:
    if isinstance(period, dict):
        since = period.get("since", "")
        until = period.get("until", "")
        return f"range:{since}:{until}"
    return f"preset:{str(period)}"


# ========== НОВЫЙ ИСТОЧНИК ДАННЫХ ДЛЯ ИНСАЙТОВ ==========
def fetch_insight(aid: str, period) -> tuple[str, dict | None]:
    """
    Достаёт инсайты:
    - сначала из локального кэша
    - если нет — запрашивает у Facebook
    - важно: ВСЕГДА приводим AdsInsights к обычному dict
    """

    # 1 — пробуем локальный кэш
    store = load_local_insights(aid)
    key = _period_key(period)

    if key in store:
        name = get_account_name(aid)
        return name, store[key]  # уже dict или None

    # 2 — прямой запрос к FB
    acc = AdAccount(aid)
    fields = ["impressions", "cpm", "clicks", "cpc", "spend", "actions"]

    params = {"level": "account"}
    if isinstance(period, dict):
        params["time_range"] = period
    else:
        params["date_preset"] = period

    data = acc.get_insights(fields=fields, params=params)
    name = acc.api_get(fields=["name"]).get("name", get_account_name(aid))

    if not data:
        ins_dict = None
    else:
        raw = data[0]

        # 🔥 КОНВЕРТИРУЕМ из AdsInsights → dict
        if hasattr(raw, "export_all_data"):
            ins_dict = raw.export_all_data()
        else:
            ins_dict = dict(raw)

    # 3 — Сохраняем в локальный кэш только JSON-safe данные
    store[key] = ins_dict
    save_local_insights(aid, store)

    return name, ins_dict


def get_cached_report(aid: str, period, label: str = "") -> str:
    """
    Возвращает текст отчёта из кеша, если свежий,
    иначе строит заново и обновляет кеш.
    (Текстовый кеш поверх локального хранилища инсайтов.)
    """
    key = _period_key(period)
    now_ts = datetime.now().timestamp()

    cache = _load_report_cache()
    acc_cache = cache.get(aid, {})
    item = acc_cache.get(key)

    if item and (now_ts - float(item.get("ts", 0))) <= REPORT_CACHE_TTL:
        return item.get("text", "")

    # кеша нет или устарел — строим
    text = build_report(aid, period, label)

    cache.setdefault(aid, {})
    cache[aid][key] = {"text": text, "ts": now_ts}
    _save_report_cache(cache)

    return text


def build_report(aid: str, period, label: str = "") -> str:
    try:
        name, ins = fetch_insight(aid, period)
    except Exception as e:
        err = str(e)
        # на лимитах/пермишенах просто не показываем кабинет, чтобы не спамить ошибками
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return f"⚠ Ошибка по {get_account_name(aid)}:\n\n{e}"

    badge = "🟢" if is_active(aid) else "🔴"
    hdr = f"{badge} <b>{name}</b>{(' ('+label+')') if label else ''}\n"
    if not ins:
        return hdr + "Нет данных за выбранный период"

    body: list[str] = []
    body.append(f"👁 Показы: {fmt_int(ins.get('impressions', 0))}")
    body.append(f"🎯 CPM: {round(float(ins.get('cpm', 0) or 0), 2)} $")
    body.append(f"🖱 Клики: {fmt_int(ins.get('clicks', 0))}")
    body.append(f"💸 CPC: {round(float(ins.get('cpc', 0) or 0), 2)} $")
    spend = float(ins.get("spend", 0) or 0)
    body.append(f"💵 Затраты: {round(spend, 2)} $")

    acts = extract_actions(ins)
    flags = metrics_flags(aid)

    msgs = int(
        acts.get("onsite_conversion.messaging_conversation_started_7d", 0) or 0
    )
    leads = int(
        acts.get("Website Submit Applications", 0)
        or acts.get("offsite_conversion.fb_pixel_submit_application", 0)
        or acts.get("offsite_conversion.fb_pixel_lead", 0)
        or acts.get("lead", 0)
        or 0
    )

    if flags["messaging"]:
        body.append(f"✉️ Переписки: {msgs}")
        if msgs > 0:
            body.append(f"💬💲 Цена переписки: {round(spend / msgs, 2)} $")

    if flags["leads"]:
        body.append(f"📩 Лиды: {leads}")
        if leads > 0:
            body.append(f"📩💲 Цена лида: {round(spend / leads, 2)} $")

    if flags["messaging"] and flags["leads"]:
        total = msgs + leads
        if total > 0:
            blended = round(spend / total, 2)
            body.append("—")
            body.append(f"🧮 Итого: {total} заявок, CPA = {blended} $")
        else:
            body.append("—")
            body.append("🧮 Итого: 0 заявок")

    return hdr + "\n".join(body)


# ======== ОТЧЁТ-СРАВНЕНИЕ ДВУХ ПЕРИОДОВ =========
def build_comparison_report(aid: str, period1, label1: str, period2, label2: str) -> str:
    """
    Сравнение двух периодов для одного аккаунта.

    Формат:
    1) Старый период
    2) Новый период
    3) Сравнение (новый vs старый) с процентами.
    """

    # Пытаемся определить, какой период старше, и при необходимости меняем местами
    def _extract_since(p):
        if isinstance(p, dict):
            s = p.get("since")
            try:
                return datetime.strptime(s, "%Y-%m-%d")
            except Exception:
                return None
        return None

    d1 = _extract_since(period1)
    d2 = _extract_since(period2)
    if d1 and d2 and d1 > d2:
        # period1 новее, чем period2 — меняем местами, чтобы первым был более давний
        period1, period2 = period2, period1
        label1, label2 = label2, label1

    try:
        name, ins1 = fetch_insight(aid, period1)
        _, ins2 = fetch_insight(aid, period2)
    except Exception as e:
        return f"⚠ Ошибка при получении данных: {e.__class__.__name__}: {str(e)}"

    if not ins1 and not ins2:
        return f"Нет данных по {get_account_name(aid)} за оба периода."

    flags = metrics_flags(aid)

    def _stat(ins):
        if not ins:
            return {
                "impr": 0,
                "cpm": 0.0,
                "clicks": 0,
                "cpc": 0.0,
                "spend": 0.0,
                "msgs": 0,
                "leads": 0,
                "total": 0,
                "cpa": None,
            }
        impr = int(ins.get("impressions", 0) or 0)
        cpm = float(ins.get("cpm", 0) or 0)
        clicks = int(ins.get("clicks", 0) or 0)
        cpc = float(ins.get("cpc", 0) or 0)
        spend, msgs, leads, total, blended = _blend_totals(ins)
        return {
            "impr": impr,
            "cpm": cpm,
            "clicks": clicks,
            "cpc": cpc,
            "spend": spend,
            "msgs": msgs,
            "leads": leads,
            "total": total,
            "cpa": blended,
        }

    s1 = _stat(ins1)
    s2 = _stat(ins2)

    def _fmt_money(v: float) -> str:
        return f"{v:.2f} $"

    def _fmt_cpa(cpa):
        return f"{cpa:.2f} $" if cpa is not None else "—"

    def _pct_change(old: float, new: float):
        if old == 0:
            return None
        return (new - old) / old * 100.0

    txt_lines: list[str] = []
    txt_lines.append(f"📊 <b>{get_account_name(aid)}</b>")
    txt_lines.append(f"Старый период: {label1}")
    txt_lines.append(f"Новый период: {label2}")
    txt_lines.append("")

    # 1️⃣ Старый период
    txt_lines.append(f"1️⃣ <b>{label1}</b> (старый период)")
    txt_lines.append(f"   👁 Охваты: {fmt_int(s1['impr'])}")
    txt_lines.append(f"   🖱 Клики: {fmt_int(s1['clicks'])}")
    txt_lines.append(f"   💵 Затраты: {_fmt_money(s1['spend'])}")
    txt_lines.append(f"   🎯 CPM: {s1['cpm']:.2f} $")
    txt_lines.append(f"   💸 CPC: {s1['cpc']:.2f} $")
    if flags["messaging"]:
        txt_lines.append(f"   💬 Переписки: {s1['msgs']}")
    if flags["leads"]:
        txt_lines.append(f"   📩 Лиды: {s1['leads']}")
    if flags["messaging"] or flags["leads"]:
        txt_lines.append(f"   🧮 Заявки всего: {s1['total']}")
        txt_lines.append(f"   🎯 CPA: {_fmt_cpa(s1['cpa'])}")
    txt_lines.append("")

    # 2️⃣ Новый период
    txt_lines.append(f"2️⃣ <b>{label2}</b> (новый период)")
    txt_lines.append(f"   👁 Охваты: {fmt_int(s2['impr'])}")
    txt_lines.append(f"   🖱 Клики: {fmt_int(s2['clicks'])}")
    txt_lines.append(f"   💵 Затраты: {_fmt_money(s2['spend'])}")
    txt_lines.append(f"   🎯 CPM: {s2['cpm']:.2f} $")
    txt_lines.append(f"   💸 CPC: {s2['cpc']:.2f} $")
    if flags["messaging"]:
        txt_lines.append(f"   💬 Переписки: {s2['msgs']}")
    if flags["leads"]:
        txt_lines.append(f"   📩 Лиды: {s2['leads']}")
    if flags["messaging"] or flags["leads"]:
        txt_lines.append(f"   🧮 Заявки всего: {s2['total']}")
        txt_lines.append(f"   🎯 CPA: {_fmt_cpa(s2['cpa'])}")
    txt_lines.append("")

    # 3️⃣ Сравнение (новый vs старый)
    txt_lines.append("3️⃣ <b>Сравнение (новый vs старый)</b>")

    def _add_diff(
        label: str,
        old_v: float,
        new_v: float,
        is_better_lower: bool = False,
        fmt_func=None,
        icon: str = "",
    ):
        if fmt_func is None:
            fmt_func = lambda x: str(int(x))
        base = f"{icon} {label}: {fmt_func(old_v)} → {fmt_func(new_v)}"
        pct = _pct_change(old_v, new_v)
        if pct is None:
            txt_lines.append(base + " (Δ %: н/д)")
            return
        if pct == 0:
            sign = "➡️"
        else:
            sign = (
                "📈"
                if ((not is_better_lower and pct > 0) or (is_better_lower and pct < 0))
                else "📉"
            )
        txt_lines.append(f"{base}   {sign} {pct:+.1f}%")

    # Охваты / клики / затраты
    _add_diff(
        "Охваты",
        s1["impr"],
        s2["impr"],
        is_better_lower=False,
        fmt_func=lambda v: fmt_int(v),
        icon="👁",
    )
    _add_diff(
        "Клики",
        s1["clicks"],
        s2["clicks"],
        is_better_lower=False,
        fmt_func=lambda v: fmt_int(v),
        icon="🖱",
    )
    _add_diff(
        "Затраты",
        s1["spend"],
        s2["spend"],
        is_better_lower=False,
        fmt_func=_fmt_money,
        icon="💵",
    )

    # CPM / CPC (меньше = лучше)
    _add_diff(
        "CPM",
        s1["cpm"],
        s2["cpm"],
        is_better_lower=True,
        fmt_func=lambda v: f"{v:.2f} $",
        icon="🎯",
    )
    _add_diff(
        "CPC",
        s1["cpc"],
        s2["cpc"],
        is_better_lower=True,
        fmt_func=lambda v: f"{v:.2f} $",
        icon="💸",
    )

    # Переписки / лиды
    if flags["messaging"]:
        _add_diff(
            "Переписки",
            s1["msgs"],
            s2["msgs"],
            is_better_lower=False,
            fmt_func=lambda v: str(int(v)),
            icon="💬",
        )
    if flags["leads"]:
        _add_diff(
            "Лиды",
            s1["leads"],
            s2["leads"],
            is_better_lower=False,
            fmt_func=lambda v: str(int(v)),
            icon="📩",
        )

    # Общие заявки и CPA
    if flags["messaging"] or flags["leads"]:
        _add_diff(
            "Заявки всего",
            s1["total"],
            s2["total"],
            is_better_lower=False,
            fmt_func=lambda v: str(int(v)),
            icon="🧮",
        )
        if s1["cpa"] is not None and s2["cpa"] is not None:
            _add_diff(
                "CPA",
                s1["cpa"],
                s2["cpa"],
                is_better_lower=True,
                fmt_func=_fmt_cpa,
                icon="🎯",
            )

    return "\n".join(txt_lines)


async def send_period_report(ctx, chat_id, period, label: str = ""):
    for aid in get_enabled_accounts_in_order():
        txt = get_cached_report(aid, period, label)
        if txt:
            await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")


# ============ БИЛЛИНГ ============
async def send_billing(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """Текущие биллинги: только неактивные аккаунты И только включённые (enabled=True)."""
    rate = usd_to_kzt()
    for aid in iter_enabled_accounts_only():
        try:
            info = AdAccount(aid).api_get(fields=["name", "account_status", "balance"])
        except Exception:
            continue
        if info.get("account_status") == 1:
            continue  # показываем только НЕактивные
        name = info.get("name", get_account_name(aid))
        usd = float(info.get("balance", 0) or 0) / 100.0
        kzt = kzt_round_up_1000(usd * rate)
        txt = f"🔴 <b>{name}</b>\n   💵 {usd:.2f} $  |  🇰🇿 {fmt_int(kzt)} ₸"
        await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")


def _compute_billing_forecast_for_account(
    aid: str, rate_kzt: float, lookback_days: int = 7
):
    """
    Возвращает dict с прогнозом по биллингу:
    {
      'aid', 'name', 'status', 'balance_usd', 'balance_kzt',
      'avg_daily_spend', 'days_left'
    }
    или None, если прогноз бессмыслен (нет затрат, нет баланса и т.п.).
    """
    try:
        info = AdAccount(aid).api_get(fields=["name", "account_status", "balance"])
    except Exception:
        return None

    status = info.get("account_status")
    if status != 1:
        return None

    balance_usd = float(info.get("balance", 0) or 0) / 100.0
    if balance_usd <= 0:
        return None

    acc = AdAccount(aid)
    until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
    since = until - timedelta(days=lookback_days - 1)
    params = {
        "level": "account",
        "time_range": {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        },
    }
    try:
        data = acc.get_insights(fields=["spend"], params=params)
    except Exception:
        return None

    total_spend = 0.0
    for row in data:
        try:
            total_spend += float(row.get("spend", 0) or 0)
        except Exception:
            continue

    if total_spend <= 0:
        return None

    avg_daily = total_spend / float(lookback_days)
    if avg_daily <= 0:
        return None

    days_left = balance_usd / avg_daily
    if days_left <= 0:
        return None

    name = info.get("name", get_account_name(aid))
    balance_kzt = kzt_round_up_1000(balance_usd * rate_kzt)

    return {
        "aid": aid,
        "name": name,
        "status": status,
        "balance_usd": balance_usd,
        "balance_kzt": balance_kzt,
        "avg_daily_spend": avg_daily,
        "days_left": days_left,
    }


async def send_billing_forecast(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """
    Прогноз списаний по всем активным аккаунтам (только enabled=True).
    Показываем примерную дату на день РАНЬШЕ расчёта.
    """
    rate = usd_to_kzt()
    items = []
    for aid in iter_enabled_accounts_only():
        fc = _compute_billing_forecast_for_account(aid, rate_kzt=rate)
        if fc:
            items.append(fc)

    if not items:
        await ctx.bot.send_message(
            chat_id=chat_id,
            text="🔮 Прогноз списаний: нет данных (нет трат/баланса по активным аккаунтам).",
        )
        return

    items.sort(key=lambda x: x["days_left"])

    lines = ["🔮 <b>Прогноз списаний по кабинетам</b>"]
    today = datetime.now(ALMATY_TZ).date()

    for fc in items:
        days_left = fc["days_left"]
        if days_left < 1:
            approx_days = 0
        else:
            approx_days = max(int(math.floor(days_left)) - 1, 0)
        date = today + timedelta(days=approx_days)
        if approx_days <= 0:
            when_str = "сегодня (ориентир)"
        else:
            when_str = f"через {approx_days} дн. (ориентир {date.strftime('%d.%m')})"

        lines.append(
            f"\n💳 <b>{fc['name']}</b>\n"
            f"   Баланс: {fc['balance_usd']:.2f} $  |  🇰🇿 {fmt_int(fc['balance_kzt'])} ₸\n"
            f"   Средний расход: {fc['avg_daily_spend']:.2f} $/день\n"
            f"   ⏳ Примерное списание: {when_str}"
        )

    await ctx.bot.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")


async def billing_digest_job(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Ежедневный дайджест утром:
    список аккаунтов, у которых days_left ≤ 5, отсортированный от самых “горящих”.
    Учитывает только включённые аккаунты (enabled=True).
    """
    chat_id = str(DEFAULT_REPORT_CHAT)
    if not chat_id:
        return

    rate = usd_to_kzt()
    items = []
    for aid in iter_enabled_accounts_only():
        fc = _compute_billing_forecast_for_account(aid, rate_kzt=rate)
        if fc and fc["days_left"] <= 5.0:
            items.append(fc)

    if not items:
        return

    items.sort(key=lambda x: x["days_left"])

    today = datetime.now(ALMATY_TZ).date()
    lines = ["☀️ <b>Предстоящие списания (≤ 5 дней)</b>"]

    for fc in items:
        days_left = fc["days_left"]
        if days_left < 1:
            approx_days = 0
        else:
            approx_days = max(int(math.floor(days_left)) - 1, 0)
        date = today + timedelta(days=approx_days)
        if approx_days <= 0:
            when_str = "сегодня (ориентир)"
        else:
            when_str = f"через {approx_days} дн. (ориентир {date.strftime('%d.%m')})"

        lines.append(
            f"\n💳 <b>{fc['name']}</b>\n"
            f"   Баланс: {fc['balance_usd']:.2f} $  |  🇰🇿 {fmt_int(fc['balance_kzt'])} ₸\n"
            f"   Средний расход: {fc['avg_daily_spend']:.2f} $/день\n"
            f"   ⏳ {when_str}"
        )

    await ctx.bot.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")


# ============ CPA ALERTS + ЛОГ ИСТОРИИ ============
async def cpa_alerts_job(ctx: ContextTypes.DEFAULT_TYPE):
    """
    Джоб для логирования истории и CPA-алертов.

    * История (append_snapshot) пишется 24/7.
    * prune_old_history() запускается раз в сутки около 03:00 (12 месяцев = 365 дней).
    * Уведомления в Telegram отправляются только с 10:00 до 22:00 по времени Алматы.
    """
    chat_id = "253181449"
    now = datetime.now(ALMATY_TZ)

    store = load_accounts()

    for aid in iter_enabled_accounts_only():
        row = store.get(aid, {})
        alerts = row.get("alerts", {}) or {}
        target = float(alerts.get("target_cpl", 0.0) or 0.0)

        # 1) Всегда логируем историю, если есть данные
        try:
            _, ins = fetch_insight(aid, "today")
        except Exception:
            ins = None

        if ins and HISTORY_STORE_AVAILABLE:
            spend, msgs, leads, total, blended = _blend_totals(ins)
            append_snapshot(aid, spend=spend, msgs=msgs, leads=leads, ts=now)

        # 2) Раз в сутки чистим историю старше 12 месяцев
        if now.hour == 3 and HISTORY_STORE_AVAILABLE:
            prune_old_history(max_age_days=365)

        # 3) Всё, что ниже — только для алертов (10–22)
        if not (10 <= now.hour <= 22):
            continue

        # Если алерты не включены или таргет 0 — дальше не проверяем
        if not alerts.get("enabled") or target <= 0:
            continue

        mflags = row.get("metrics", {}) or {}
        use_msg = bool(mflags.get("messaging", False))
        use_lead = bool(mflags.get("leads", False))
        if not (use_msg or use_lead):
            continue

        if not ins:
            continue

        spend, msgs, leads, total, blended = _blend_totals(ins)

        if use_msg and not use_lead:
            conv = msgs
            cpa = (spend / conv) if conv > 0 else None
            label = "Переписки"
        elif use_lead and not use_msg:
            conv = leads
            cpa = (spend / conv) if conv > 0 else None
            label = "Лиды"
        else:
            conv = total
            cpa = blended
            label = "Итого (💬+📩)"

        should_alert = False
        reason = ""
        if spend > 0 and conv == 0:
            should_alert = True
            reason = f"есть траты {spend:.2f}$, но 0 конверсий"
        elif cpa is not None and cpa > target:
            should_alert = True
            reason = f"CPA {cpa:.2f}$ > таргета {target:.2f}$"

        if should_alert:
            txt = (
                f"⚠️ <b>{get_account_name(aid)}</b> — {label}\n"
                f"💵 Затраты: {spend:.2f} $\n"
                f"📊 Конверсии: {conv}\n"
                f"🎯 Таргет CPA: {target:.2f} $\n"
                f"🧾 Причина: {reason}"
            )
            await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")


# ============ UI ============
def main_menu() -> InlineKeyboardMarkup:
    last_sync = human_last_sync()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Отчёт по всем", callback_data="rep_all_menu")],
            [InlineKeyboardButton("Биллинг", callback_data="billing")],
            [
                InlineKeyboardButton(
                    "Отчёт по аккаунту", callback_data="choose_acc_report"
                )
            ],
            [
                InlineKeyboardButton("Тепловая карта", callback_data="hm_menu")
            ],
            [InlineKeyboardButton("Настройки", callback_data="choose_acc_settings")],
            [
                InlineKeyboardButton("🤖 Автопилат", callback_data="ap_main")
            ],
            [
                InlineKeyboardButton(
                    f"Синк BM (посл. {last_sync})",
                    callback_data="sync_bm",
                )
            ],
        ]
    )



def billing_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Текущие биллинги", callback_data="billing_current"
                )
            ],
            [
                InlineKeyboardButton(
                    "Прогноз списаний", callback_data="billing_forecast"
                )
            ],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )


# 👉 ВСТАВЬ ЭТОТ БЛОК ПРЯМО СЮДА
def all_reports_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data="rep_today"),
                InlineKeyboardButton("Вчера", callback_data="rep_yday"),
            ],
            [
                InlineKeyboardButton("Прошедшая неделя", callback_data="rep_week")
            ],
            [
                InlineKeyboardButton("⬅️ В меню", callback_data="menu")
            ],
        ]
    )


# (этот уже есть)
def heatmap_menu(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("7 дней", callback_data=f"hm7|{aid}"),
                InlineKeyboardButton("14 дней", callback_data=f"hm14|{aid}"),
            ],
            [
                InlineKeyboardButton(
                    "Текущий месяц", callback_data=f"hmmonth|{aid}"
                )
            ],
            [
                InlineKeyboardButton("⬅️ Назад", callback_data="menu")
            ],
        ]
    )




def _flag_line(aid: str) -> str:
    st = load_accounts().get(aid, {})
    enabled = st.get("enabled", True)
    m = st.get("metrics", {}) or {}
    a = st.get("alerts", {}) or {}
    on = "🟢" if enabled else "🔴"
    mm = "💬" if m.get("messaging") else ""
    ll = "♿️" if m.get("leads") else ""
    aa = "⚠️" if a.get("enabled") and (a.get("target_cpl", 0) or 0) > 0 else ""
    return f"{on} {mm}{ll}{aa}".strip()


def accounts_kb(prefix: str) -> InlineKeyboardMarkup:
    store = load_accounts()
    if store:
        enabled_ids = [aid for aid, row in store.items() if row.get("enabled", True)]
        disabled_ids = [aid for aid, row in store.items() if not row.get("enabled", True)]
        ids = enabled_ids + disabled_ids
    else:
        ids = AD_ACCOUNTS_FALLBACK

    rows = []
    for aid in ids:
        rows.append(
            [
                InlineKeyboardButton(
                    f"{_flag_line(aid)}  {get_account_name(aid)}",
                    callback_data=f"{prefix}|{aid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def settings_kb(aid: str) -> InlineKeyboardMarkup:
    st = load_accounts().get(aid, {"enabled": True, "metrics": {}, "alerts": {}})
    en_text = "Выключить кабинет" if st.get("enabled", True) else "Включить кабинет"
    m_on = st.get("metrics", {}).get("messaging", True)
    l_on = st.get("metrics", {}).get("leads", False)
    a_on = st.get("alerts", {}).get("enabled", False) and (
        st.get("alerts", {}).get("target_cpl", 0) or 0
    ) > 0
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(en_text, callback_data=f"toggle_enabled|{aid}")],
            [
                InlineKeyboardButton(
                    f"💬 Переписки: {'ON' if m_on else 'OFF'}",
                    callback_data=f"toggle_m|{aid}",
                ),
                InlineKeyboardButton(
                    f"♿️ Лиды сайта: {'ON' if l_on else 'OFF'}",
                    callback_data=f"toggle_l|{aid}",
                ),
            ],
            [
                InlineKeyboardButton(
                    f"⚠️ Алерт CPA: {'ON' if a_on else 'OFF'}",
                    callback_data=f"toggle_alert|{aid}",
                )
            ],
            [
                InlineKeyboardButton(
                    "✏️ Задать target CPA", callback_data=f"set_cpa|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Назад к списку",
                    callback_data="choose_acc_settings",
                )
            ],
        ]
    )


def period_kb_for(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"one_today|{aid}"),
                InlineKeyboardButton("Вчера", callback_data=f"one_yday|{aid}"),
            ],
            [
                InlineKeyboardButton(
                    "Прошедшая неделя", callback_data=f"one_week|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Сравнить периоды", callback_data=f"cmp_menu|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "🗓 Свой диапазон", callback_data=f"one_custom|{aid}"
                )
            ],
            [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="choose_acc_report")],
        ]
    )


def compare_kb_for(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Эта неделя vs прошлая", callback_data=f"cmp_week|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Два диапазона", callback_data=f"cmp_custom|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ К периодам", callback_data=f"back_periods|{aid}"
                )
            ],
        ]
    )


# ============ PRIVACY ============
def _allowed(update: Update) -> bool:
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    user_id = update.effective_user.id if update.effective_user else None
    if chat_id in ALLOWED_CHAT_IDS:
        return True
    if user_id and user_id in ALLOWED_USER_IDS:
        return True
    return False


# ======== SERVICE CMD ========
async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = update.effective_user.id if update.effective_user else None
    await update.message.reply_text(
        f"user_id: <code>{user_id}</code>\nchat_id: <code>{chat_id}</code>",
        parse_mode="HTML",
    )


# ============ COMMANDS ============
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "⛔️ Нет доступа. Отправь /whoami и добавь свой user_id "
                "в ALLOWED_USER_IDS."
            ),
        )
        return
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🤖 Выберите действие:",
        reply_markup=main_menu(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    txt = (
    "Команды:\n"
    "/start — главное меню\n"
    "/help — список всех команд\n"
    "/billing — биллинги и прогнозы\n"
    "/sync_accounts — синхронизация BM\n"
    "/whoami — показать user_id/chat_id\n"
    "/heatmap <act_id> — тепловая карта адсетов за 7 дней\n"
    "\n"
    "🚀 Функции автопилата:\n"
    "• Автоматические рекомендации по аккаунту\n"
    "• Изменение бюджета (-20%, +20%, ручной ввод)\n"
    "• Безопасное отключение дорогих адсетов\n"
    "• Подготовка к ИИ-управлению (Пилат)\n"
)
    await update.message.reply_text(txt, reply_markup=ReplyKeyboardRemove())


async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    await update.message.reply_text(
        "Что показать по биллингу?", reply_markup=billing_menu()
    )


# =========================
# 📌 ПРАВИЛЬНАЯ ФУНКЦИЯ cmd_heatmap (ОТДЕЛЬНО)
# =========================
async def cmd_heatmap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return

    parts = update.message.text.strip().split()

    # 1) Если /heatmap без аргументов — показываем выбор аккаунтов
    if len(parts) == 1:
        await update.message.reply_text(
            "Выберите аккаунт для тепловой карты:",
            reply_markup=accounts_kb("hmacc"),
        )
        return

    # 2) Если /heatmap act_XXXX — оставляем старый режим
    aid = parts[1].strip()
    if not aid.startswith("act_"):
        aid = "act_" + aid

    context.user_data["heatmap_aid"] = aid

    await update.message.reply_text(
        f"Выберите период тепловой карты для {get_account_name(aid)}:",
        reply_markup=heatmap_menu(aid),
    )




async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    try:
        res = upsert_from_bm()
        last_sync_h = human_last_sync()
        await update.message.reply_text(
            f"✅ Синк завершён. Добавлено: {res['added']}, "
            f"обновлено: {res['updated']}, пропущено: {res['skipped']}. "
            f"Всего: {res['total']}\n"
            f"🕓 Последняя синхронизация: {last_sync_h}"
        )
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка синка: {e}")


# ======== CUSTOM RANGE INPUT ========
_RANGE_RE = re.compile(
    r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{2})\.(\d{4})\s*$"
)


def _parse_range(s: str):
    m = _RANGE_RE.match(s)
    if not m:
        return None
    d1 = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    d2 = datetime(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    if d1 > d2:
        d1, d2 = d2, d1
    return (
        {"since": d1.strftime("%Y-%m-%d"), "until": d2.strftime("%Y-%m-%d")},
        f"{d1.strftime('%d.%m')}-{d2.strftime('%d.%m')}",
    )


def _parse_two_ranges(s: str):
    """
    Формат:
    01.06.2025-07.06.2025;08.06.2025-14.06.2025
    или две строки:
    01.06.2025-07.06.2025
    08.06.2025-14.06.2025
    """
    parts = [p.strip() for p in re.split(r"[;\n]+", s) if p.strip()]
    if len(parts) != 2:
        return None
    r1 = _parse_range(parts[0])
    r2 = _parse_range(parts[1])
    if not r1 or not r2:
        return None
    return r1, r2


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    ud = context.user_data
    if "await_range_for" in ud:
        aid = ud.pop("await_range_for")
        parsed = _parse_range(update.message.text.strip())
        if not parsed:
            await update.message.reply_text(
                "Формат дат: 01.06.2025-07.06.2025. Попробуй ещё раз."
            )
            ud["await_range_for"] = aid
            return
        period, label = parsed
        txt = get_cached_report(aid, period, label)
        await update.message.reply_text(
            txt or "Нет данных/нет доступа.", parse_mode="HTML"
        )
        return


# ======= SAFE EDIT (на будущее, пока не используем везде) =======
async def safe_edit_message(q, text: str, **kwargs):
    try:
        return await q.edit_message_text(text=text, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise


# ============ CALLBACKS ДЛЯ АВТОПИЛАТА ============
async def on_cb_autopilot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if not _allowed(update):
        await q.edit_message_text("⛔️ Нет доступа.")
        return

    data = q.data or ""
    chat_id = str(q.message.chat.id)

    # 1) Главное меню автопилата
    if data == "ap_main":
        await q.edit_message_text(
            "Выберите режим автопилата:",
            reply_markup=autopilot_main_menu()
        )
        return

    # 2) Выбор основного режима (Рекомендации / Автопилат)
    if data.startswith("apmode|"):
        mode = data.split("|", 1)[1]
        context.user_data["autopilot_mode"] = mode

        await q.edit_message_text(
            f"Режим: <b>{mode}</b>\nВыберите подрежим:",
            parse_mode="HTML",
            reply_markup=autopilot_submode_menu()
        )
        return

    # 3) Подрежимы (ручной / авто)
    if data.startswith("apsub|"):
        sub = data.split("|", 1)[1]
        context.user_data["autopilot_submode"] = sub

        await q.edit_message_text(
            f"Режим: <b>{context.user_data.get('autopilot_mode')}</b>\n"
            f"Подрежим: <b>{sub}</b>\n\n"
            f"Теперь выберите аккаунт:",
            parse_mode="HTML",
            reply_markup=accounts_kb("ap_acc")
        )
        return

    # 4) Выбор аккаунта для автопилата
    if data.startswith("ap_acc|"):
        aid = data.split("|", 1)[1]
        context.user_data["ap_aid"] = aid

        ui = get_recommendations_ui(aid)
        blocks = build_recommendations_ui(ui["items"])

        text = f"🔍 <b>Рекомендации по {get_account_name(aid)}</b>\n\n{ui['text']}"
        await q.edit_message_text(text, parse_mode="HTML")

        # Отправляем каждую рекомендацию отдельным сообщением
        for block in blocks:
            await context.bot.send_message(
                chat_id,
                block["text"],
                parse_mode="HTML",
                reply_markup=block["reply_markup"]
            )
        return

      # 5) Кнопки под конкретной рекомендацией (up/down/manual/off/back)
    if data.startswith("ap|"):
        parts = data.split("|")

        # ожидаем минимум "ap|action" или "ap|action|entity_id"
        if len(parts) < 2:
            await q.edit_message_text(
                "⚠ Ошибка кнопки: некорректный формат callback_data.",
                parse_mode="HTML",
            )
            return

        _, action, *rest = parts
        entity_id = rest[0] if rest else ""

        # 👉 Обработка кнопки "Назад"
        if action == "back":
            # возвращаемся в главное меню автопилата
            await q.edit_message_text(
                "Выберите режим автопилата:",
                reply_markup=autopilot_main_menu(),
            )
            return

        # дальше всё как раньше, но с проверкой entity_id
        if not entity_id:
            await q.edit_message_text(
                "⚠ Ошибка кнопки: не передан ID сущности.\n"
                "Обнови рекомендации и попробуй ещё раз.",
                parse_mode="HTML",
            )
            return

        # Ввести вручную — ждём текст от тебя
        if action == "manual":
            context.user_data["await_manual_input"] = entity_id
            await q.edit_message_text(
                f"✍️ Введите число (например 1.2, -20, 15):\n"
                f"ID: <code>{entity_id}</code>",
                parse_mode="HTML",
            )
            return

        # Остальные действия требуют подтверждения
        await q.edit_message_text(
            f"Подтвердить действие <b>{action}</b> для <code>{entity_id}</code>?",
            parse_mode="HTML",
            reply_markup=confirm_action_buttons(action, entity_id),
        )
        return

    # 6) Подтверждение (Да/Нет)
    if data.startswith("apconfirm|"):
        _, yesno, action, entity_id = data.split("|", 3)

        if yesno == "no":
            await q.edit_message_text("Операция отменена.", parse_mode="HTML")
            return

        # down20 / up20 — фиксированные проценты
        if action in ("up20", "down20"):
            percent = 20 if action == "up20" else -20
            res = apply_budget_change(entity_id, percent)
            await q.edit_message_text(res["message"], parse_mode="HTML")
            return

        # off — выключение
        if action == "off":
            aid = context.user_data.get("ap_aid")
            if aid and not can_disable(aid, entity_id):
                await q.edit_message_text(
                    "❌ Нельзя отключить этот адсет — иначе весь аккаунт останется без трафика.",
                    parse_mode="HTML"
                )
                return

            res = disable_entity(entity_id)
            await q.edit_message_text(res["message"], parse_mode="HTML")
            return

        # иначе — это ручной процент (action = "12.5" например)
        try:
            percent = float(action.replace(",", "."))
        except Exception:
            await q.edit_message_text(
                "⚠ Не получилось прочитать процент изменения.",
                parse_mode="HTML"
            )
            return

        res = apply_budget_change(entity_id, percent)
        await q.edit_message_text(res["message"], parse_mode="HTML")
        return
    
async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not _allowed(update):
        await q.edit_message_text("⛔️ Нет доступа.")
        return

    data = q.data or ""
    chat_id = str(q.message.chat.id)

    if data == "menu":
        await q.edit_message_text("🤖 Выберите действие:", reply_markup=main_menu())
        return

    if data == "rep_all_menu":
        await q.edit_message_text("Выберите период:", reply_markup=all_reports_menu())
        return

        # общие отчёты
    if data == "rep_today":
        label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, chat_id, "today", label)
        return

    if data == "rep_yday":
        label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, chat_id, "yesterday", label)
        return

    if data == "rep_week":
        until = datetime.now(ALMATY_TZ) - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, chat_id, period, label)
        return

        # вход в тепловую карту из главного меню
    if data == "hm_menu":
        await q.edit_message_text(
            "Выберите аккаунт для тепловой карты:",
            reply_markup=accounts_kb("hmacc"),
        )
        return

    # ========== ТЕПЛОВЫЕ КАРТЫ ==========
    # сначала обработка выбора аккаунта из меню
    if data.startswith("hmacc|"):
        aid = data.split("|", 1)[1]
        context.user_data["heatmap_aid"] = aid
        await q.edit_message_text(
            f"Выберите период тепловой карты для {get_account_name(aid)}:",
            reply_markup=heatmap_menu(aid),
        )
        return

    # затем уже сами периоды
    if data.startswith("hm7|"):
        aid = data.split("|")[1]
        heat = build_heatmap_for_account(aid, get_account_name, mode="7")
        await q.edit_message_text(heat, parse_mode="HTML")
        return

    if data.startswith("hm14|"):
        aid = data.split("|")[1]
        heat = build_heatmap_for_account(aid, get_account_name, mode="14")
        await q.edit_message_text(heat, parse_mode="HTML")
        return

    if data.startswith("hmmonth|"):
        aid = data.split("|")[1]
        heat = build_heatmap_for_account(aid, get_account_name, mode="month")
        await q.edit_message_text(heat, parse_mode="HTML")
        return


    # биллинг
    if data == "billing":
        await q.edit_message_text(
            "Что показать по биллингу?", reply_markup=billing_menu()
        )
        return
    if data == "billing_current":
        await q.edit_message_text("📋 Биллинги (неактивные аккаунты):")
        await send_billing(context, chat_id)
        return
    if data == "billing_forecast":
        await q.edit_message_text("🔮 Считаю прогноз списаний…")
        await send_billing_forecast(context, chat_id)
        return

    # синк из BM из меню
    if data == "sync_bm":
        try:
            res = upsert_from_bm()
            last_sync_h = human_last_sync()
            await q.edit_message_text(
                f"✅ Синк завершён. Добавлено: {res['added']}, "
                f"обновлено: {res['updated']}, пропущено: {res['skipped']}. "
                f"Всего: {res['total']}\n"
                f"🕓 Последняя синхронизация: {last_sync_h}",
                reply_markup=main_menu(),
            )
        except Exception as e:
            await q.edit_message_text(
                f"⚠️ Ошибка синка: {e}", reply_markup=main_menu()
            )
        return

    # выбор аккаунта для отчёта
    if data == "choose_acc_report":
        await q.edit_message_text(
            "Выберите аккаунт:", reply_markup=accounts_kb("rep1")
        )
        return
    if data.startswith("rep1|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Отчёт по: {get_account_name(aid)}\nВыбери период:",
            reply_markup=period_kb_for(aid),
        )
        return
    if data.startswith("one_today|"):
        aid = data.split("|", 1)[1]
        label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        await q.edit_message_text(
            f"Отчёт по {get_account_name(aid)} за {label}:"
        )
        txt = get_cached_report(aid, "today", label)
        await context.bot.send_message(
            chat_id,
            txt or "Нет данных/нет доступа.",
            parse_mode="HTML",
        )
        return
    if data.startswith("one_yday|"):
        aid = data.split("|", 1)[1]
        label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
        await q.edit_message_text(
            f"Отчёт по {get_account_name(aid)} за {label}:"
        )
        txt = get_cached_report(aid, "yesterday", label)
        await context.bot.send_message(
            chat_id,
            txt or "Нет данных/нет доступа.",
            parse_mode="HTML",
        )
        return
    if data.startswith("one_week|"):
        aid = data.split("|", 1)[1]
        until = datetime.now(ALMATY_TZ) - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await q.edit_message_text(
            f"Отчёт по {get_account_name(aid)} за {label}:"
        )
        txt = get_cached_report(aid, period, label)
        await context.bot.send_message(
            chat_id,
            txt or "Нет данных/нет доступа.",
            parse_mode="HTML",
        )
        return
    if data.startswith("one_custom|"):
        aid = data.split("|", 1)[1]
        context.user_data["await_range_for"] = aid
        await q.edit_message_text(
            f"Введи даты для {get_account_name(aid)} форматом: 01.06.2025-07.06.2025",
            reply_markup=period_kb_for(aid),
        )
        return

    # меню сравнения периодов
    if data.startswith("cmp_menu|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Сравнение периодов для {get_account_name(aid)}:",
            reply_markup=compare_kb_for(aid),
        )
        return
    if data.startswith("back_periods|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Отчёт по: {get_account_name(aid)}\nВыбери период:",
            reply_markup=period_kb_for(aid),
        )
        return
    if data.startswith("cmp_week|"):
        aid = data.split("|", 1)[1]
        now = datetime.now(ALMATY_TZ)
        until2 = now - timedelta(days=1)
        since2 = until2 - timedelta(days=6)
        until1 = since2 - timedelta(days=1)
        since1 = until1 - timedelta(days=6)
        period1 = {
            "since": since1.strftime("%Y-%m-%d"),
            "until": until1.strftime("%Y-%m-%d"),
        }
        period2 = {
            "since": since2.strftime("%Y-%m-%d"),
            "until": until2.strftime("%Y-%m-%d"),
        }
        label1 = f"{since1.strftime('%d.%m')}-{until1.strftime('%d.%m')}"
        label2 = f"{since2.strftime('%d.%m')}-{until2.strftime('%d.%m')}"
        await q.edit_message_text(f"Сравниваю {label1} vs {label2}…")
        txt = build_comparison_report(aid, period1, label1, period2, label2)
        await context.bot.send_message(chat_id, txt, parse_mode="HTML")
        return

    if data.startswith("cmp_custom|"):
        aid = data.split("|", 1)[1]
        context.user_data["await_cmp_for"] = aid
        await q.edit_message_text(
            "Отправь два диапазона дат через ';' или с новой строки.\n"
            "Например:\n"
            "01.06.2025-07.06.2025;08.06.2025-14.06.2025",
            reply_markup=compare_kb_for(aid),
        )
        return

    # настройки
    if data == "choose_acc_settings":
        await q.edit_message_text(
            "Выберите аккаунт для настроек:",
            reply_markup=accounts_kb("set1"),
        )
        return

    if data.startswith("set1|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("toggle_enabled|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {})
        row["enabled"] = not row.get("enabled", True)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("toggle_m|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"metrics": {}})
        row["metrics"] = row.get("metrics", {})
        row["metrics"]["messaging"] = not row["metrics"].get("messaging", True)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("toggle_l|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"metrics": {}})
        row["metrics"] = row.get("metrics", {})
        row["metrics"]["leads"] = not row["metrics"].get("leads", False)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("toggle_alert|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        if alerts.get("enabled", False):
            alerts["enabled"] = False
        else:
            alerts["enabled"] = float(alerts.get("target_cpl", 0) or 0) > 0
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("set_cpa|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        current = float(alerts.get("target_cpl", 0.0) or 0.0)
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"⚠️ Текущий target CPA: {current:.2f} $.\n"
            f"Напиши в чат число (например 2.5). 0 — выключит алерты.",
            reply_markup=settings_kb(aid),
        )
        context.user_data["await_cpa_for"] = aid
        return


# ввод target CPA и кастомных диапазонов

async def on_text_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return

    # Игнорируем произвольный текст в группах/супергруппах,
    # чтобы бот не отвечал на каждое сообщение
    chat = update.effective_chat
    if chat and chat.type in ("group", "supergroup"):
        # В группах реагируем только на команды (/start, /help, и т.п.),
        # а обычный текст пропускаем
        return

    # ----- кастомный диапазон для ОДНОГО периода -----
    if "await_range_for" in context.user_data:
        await on_text(update, context)
        return

    # ----- сравнение ДВУХ диапазонов -----
    if "await_cmp_for" in context.user_data:
        aid = context.user_data.pop("await_cmp_for")
        parsed = _parse_two_ranges(update.message.text)
        if not parsed:
            # Флаг НЕ возвращаем, чтобы бот не цеплялся за любой текст дальше
            await update.message.reply_text(
                "Не распознал форматы дат.\n"
                "Пример: 01.06.2025-07.06.2025;08.06.2025-14.06.2025"
            )
            return

        (p1, label1), (p2, label2) = parsed
        txt = build_comparison_report(aid, p1, label1, p2, label2)
        await update.message.reply_text(txt, parse_mode="HTML")
        return

    # ----- ввод target CPA -----
    if "await_cpa_for" in context.user_data:
        aid = context.user_data.pop("await_cpa_for")
        try:
            val = float(update.message.text.replace(",", ".").strip())
        except Exception:
            await update.message.reply_text(
                "Введите число, например: 2.5 (или 0 чтобы выключить)"
            )
            context.user_data["await_cpa_for"] = aid
            return

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        alerts["target_cpl"] = float(val)
        alerts["enabled"] = val > 0
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        if val > 0:
            await update.message.reply_text(
                f"✅ Target CPA для {get_account_name(aid)} обновлён: {val:.2f} $ (алерты ВКЛ)"
            )
        else:
            await update.message.reply_text(
                f"✅ Target CPA для {get_account_name(aid)} установлен 0 — алерты ВЫКЛ"
            )
        return


    # ============================================================
    # 🔥 РУЧНОЙ ВВОД ДЛЯ АВТОПИЛАТА
    # ============================================================
    if "await_manual_input" in context.user_data:
        entity_id = context.user_data.pop("await_manual_input")
        raw = update.message.text.strip()

        percent = parse_manual_input(raw)
        if percent is None:
            await update.message.reply_text(
                "❌ Не получилось разобрать число. Пример: 1.2, 20, -15",
                parse_mode="HTML"
            )
            context.user_data["await_manual_input"] = entity_id
            return

        # Отправляем подтверждение через кнопки
        await update.message.reply_text(
            f"Подтвердить изменение бюджета на <b>{percent:+.1f}%</b> "
            f"для <code>{entity_id}</code>?",
            parse_mode="HTML",
            reply_markup=confirm_action_buttons(str(percent), entity_id)
        )
        return

    # ====== дальше идёт твоя старая логика текстовых вводов ======

    if "await_range_for" in context.user_data:
        aid = context.user_data.pop("await_range_for")
        parsed = _parse_range(update.message.text.strip())
        if not parsed:
            await update.message.reply_text(
                "Формат дат: 01.06.2025-07.06.2025. Попробуй ещё раз."
            )
            context.user_data["await_range_for"] = aid
            return
        period, label = parsed
        txt = get_cached_report(aid, period, label)
        await update.message.reply_text(
            txt or "Нет данных/нет доступа.", parse_mode="HTML"
        )
        return

    if "await_cmp_for" in context.user_data:
        aid = context.user_data.pop("await_cmp_for")
        parsed = _parse_two_ranges(update.message.text)
        if not parsed:
            context.user_data["await_cmp_for"] = aid
            await update.message.reply_text(
                "Не распознал форматы дат.\n"
                "Пример: 01.06.2025-07.06.2025;08.06.2025-14.06.2025"
            )
            return
        (p1, label1), (p2, label2) = parsed
        txt = build_comparison_report(aid, p1, label1, p2, label2)
        await update.message.reply_text(txt, parse_mode="HTML")
        return

    if "await_cpa_for" in context.user_data:
        aid = context.user_data.pop("await_cpa_for")
        try:
            val = float(update.message.text.replace(",", ".").strip())
        except Exception:
            await update.message.reply_text(
                "Введите число, например: 2.5 (или 0 чтобы выключить)"
            )
            context.user_data["await_cpa_for"] = aid
            return

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        alerts["target_cpl"] = float(val)
        alerts["enabled"] = val > 0
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        if val > 0:
            await update.message.reply_text(
                f"✅ Target CPA для {get_account_name(aid)} обновлён: {val:.2f} $ (алерты ВКЛ)"
            )
        else:
            await update.message.reply_text(
                f"✅ Target CPA для {get_account_name(aid)} установлен 0 — алерты ВЫКЛ"
            )
        return

# ============ JOBS ============
async def full_daily_scan_job(ctx: ContextTypes.DEFAULT_TYPE):
    """
    1 раз в день — собирает инсайты по всем включённым аккаунтам и
    сохраняет их в локальное хранилище, чтобы утром и при запросах
    по типовым периодам не дёргать FB лишний раз.

    Периоды:
    - today
    - yesterday
    - прошедшие 7 дней (до вчера включительно)
    """
    now = datetime.now(ALMATY_TZ)

    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    periods = {
        "today": "today",
        "yesterday": "yesterday",
        "week": {
            "since": (now - timedelta(days=7)).strftime("%Y-%m-%d"),
            "until": yesterday,
        },
    }

    for aid in iter_enabled_accounts_only():
        store = load_local_insights(aid)
        for _, period in periods.items():
            key = _period_key(period)
            if key in store:
                continue
            try:
                _, ins = fetch_insight(aid, period)
                store[key] = ins
            except Exception as e:
                print(f"[daily_scan] error for {aid}: {e}")
        save_local_insights(aid, store)

    print("[daily_scan] full daily scan completed")


async def daily_report_job(ctx: ContextTypes.DEFAULT_TYPE):
    if not DEFAULT_REPORT_CHAT:
        return
    label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
    await send_period_report(ctx, str(DEFAULT_REPORT_CHAT), "yesterday", label)


def schedule_cpa_alerts(app: Application):
    """
    Запускаем cpa_alerts_job каждый час (24/7).
    Внутри самого джоба уже есть логика,
    когда слать уведомления и когда только логировать историю.
    """
    app.job_queue.run_repeating(
        cpa_alerts_job,
        interval=3600,  # раз в час
        first=0,        # сразу после старта приложения
        name="cpa_alerts_job",
    )


# ============ APP ============
def build_app() -> Application:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("billing", cmd_billing))
    app.add_handler(CommandHandler("sync_accounts", cmd_sync))
    app.add_handler(CommandHandler("heatmap", cmd_heatmap))
    app.add_handler(CallbackQueryHandler(on_cb_autopilot, pattern="^ap"))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_any))

    # Плотный проход по аккаунтам чуть раньше утреннего отчёта,
    # чтобы к 9:30 данные уже были в локальном кэше.
    app.job_queue.run_daily(
        full_daily_scan_job,
        time=time(hour=9, minute=20, tzinfo=ALMATY_TZ),
    )

    app.job_queue.run_daily(
        daily_report_job,
        time=time(hour=9, minute=30, tzinfo=ALMATY_TZ),
    )

    app.job_queue.run_daily(
        billing_digest_job,
        time=time(hour=9, minute=0, tzinfo=ALMATY_TZ),
    )

    schedule_cpa_alerts(app)

    init_billing_watch(
        app,
        get_enabled_accounts=get_enabled_accounts_in_order,
        get_account_name=get_account_name,
        usd_to_kzt=usd_to_kzt,
        kzt_round_up_1000=kzt_round_up_1000,
        owner_id=253181449,
        group_chat_id=str(DEFAULT_REPORT_CHAT),
    )

    return app


if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    build_app().run_polling(allowed_updates=Update.ALL_TYPES)
