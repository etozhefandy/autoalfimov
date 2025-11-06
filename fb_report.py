# fb_report.py
import os
import json
import math
import re
import shutil
from datetime import datetime, timedelta, time

import requests
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
    253181449  # Andrey
}
ALLOWED_CHAT_IDS = {str(DEFAULT_REPORT_CHAT), "-1002679045097"}  # как строки

# ======= ПУТИ / ФАЙЛЫ =========
DATA_DIR = os.getenv("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

ACCOUNTS_JSON = os.getenv("ACCOUNTS_JSON_PATH", os.path.join(DATA_DIR, "accounts.json"))
REPO_ACCOUNTS_JSON = "./accounts.json"

FORECAST_CACHE_FILE = os.path.join(DATA_DIR, "forecast_cache.json")
FX_CACHE_FILE = os.path.join(DATA_DIR, "fx_cache.json")

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

# ========= КУРС USD→KZT =========
FX_API_KEY = os.getenv("FX_API_KEY", "LYr6odX08iC6PXKqQSTT4QtKouCFcWeF")
FX_CACHE_HOURS = 12

def _fx_load():
    try:
        with open(FX_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def _fx_save(obj: dict):
    _atomic_write_json(FX_CACHE_FILE, obj)

def usd_to_kzt() -> float:
    cache = _fx_load()
    now = datetime.now().timestamp()
    if cache.get("rate") and (now - cache.get("ts", 0) <= FX_CACHE_HOURS * 3600):
        return float(cache["rate"])
    try:
        r = requests.get(
            "https://api.apilayer.com/fixer/latest?base=USD&symbols=KZT",
            headers={"apikey": FX_API_KEY},
            timeout=10,
        )
        data = r.json()
        raw = float(data["rates"]["KZT"])
        rate = raw + 5.0  # надбавка +5
    except Exception:
        rate = 505.0
    _fx_save({"rate": rate, "ts": now})
    return rate

def kzt_round_up_1000(v: float) -> int:
    return int(math.ceil(v / 1000.0) * 1000)

# ========= ФОЛБЭКИ =========
AD_ACCOUNTS_FALLBACK = [
    "act_1415004142524014", "act_719853653795521", "act_1206987573792913",
    "act_1108417930211002", "act_2342025859327675", "act_844229314275496",
    "act_1333550570916716", "act_195526110289107", "act_2145160982589338",
    "act_508239018969999", "act_1357165995492721", "act_798205335840576",
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

# ========== STORES ==========
def load_accounts() -> dict:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_accounts(d: dict):
    _atomic_write_json(ACCOUNTS_JSON, d)

def _norm_act(aid: str) -> str:
    aid = str(aid).strip()
    return aid if aid.startswith("act_") else "act_" + aid

def get_account_name(aid: str) -> str:
    store = load_accounts()
    if aid in store and store[aid].get("name"):
        return store[aid]["name"]
    return ACCOUNT_NAMES.get(aid, aid)

def get_enabled_accounts_in_order() -> list[str]:
    store = load_accounts()
    if not store:
        return AD_ACCOUNTS_FALLBACK
    out = [acc for acc, row in store.items() if row.get("enabled", True)]
    return out or AD_ACCOUNTS_FALLBACK

def looks_excluded(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_NAME_KEYWORDS)

def upsert_from_bm() -> dict:
    """Добавляет новые аккаунты и обновляет ИМЕНА. Настройки не затирает."""
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
    return {"added": added, "updated": updated, "skipped": skipped, "total": len(store)}

# ========== HELPERS ==========
def is_active(aid: str) -> bool:
    try:
        st = AdAccount(aid).api_get(fields=["account_status"])["account_status"]
        return st == 1
    except:
        return False

def fmt_int(n) -> str:
    try:
        return f"{int(float(n)):,}".replace(",", " ")
    except:
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

def fetch_insight(aid: str, period) -> tuple[str, dict | None]:
    acc = AdAccount(aid)
    fields = ["impressions", "cpm", "clicks", "cpc", "spend", "actions"]
    params = {"level": "account"}
    if isinstance(period, dict):
        params["time_range"] = period
    else:
        params["date_preset"] = period
    data = acc.get_insights(fields=fields, params=params)
    name = acc.api_get(fields=["name"]).get("name", get_account_name(aid))
    return name, (data[0] if data else None)

def _blend_totals(ins):
    """Возвращает (spend, msg_conv, lead_conv, blended_conv, blended_cpa or None)"""
    acts = extract_actions(ins)
    spend = float(ins.get("spend", 0) or 0)
    msgs = int(acts.get("onsite_conversion.messaging_conversation_started_7d", 0) or 0)
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

def build_report(aid: str, period, label="") -> str:
    try:
        name, ins = fetch_insight(aid, period)
    except Exception as e:
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return f"⚠ Ошибка по {get_account_name(aid)}:\n\n{e}"

    badge = "🟢" if is_active(aid) else "🔴"
    hdr = f"{badge} <b>{name}</b>{(' ('+label+')') if label else ''}\n"
    if not ins:
        return hdr + "Нет данных за выбранный период"

    body = []
    body.append(f"👁 Показы: {fmt_int(ins.get('impressions', 0))}")
    body.append(f"🎯 CPM: {round(float(ins.get('cpm', 0) or 0), 2)} $")
    body.append(f"🖱 Клики: {fmt_int(ins.get('clicks', 0))}")
    body.append(f"💸 CPC: {round(float(ins.get('cpc', 0) or 0), 2)} $")
    spend = float(ins.get("spend", 0) or 0)
    body.append(f"💵 Затраты: {round(spend, 2)} $")

    acts = extract_actions(ins)
    flags = metrics_flags(aid)

    msgs = int(acts.get("onsite_conversion.messaging_conversation_started_7d", 0) or 0)
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
            body.append(f"💬💲 Цена переписки: {round(spend/msgs, 2)} $")

    if flags["leads"]:
        body.append(f"📩 Лиды: {leads}")
        if leads > 0:
            body.append(f"📩💲 Цена лида: {round(spend/leads, 2)} $")

    # Итоговая строка при обеих метриках
    if flags["messaging"] and flags["leads"]:
        total = msgs + leads
        if total > 0:
            blended = round(spend / total, 2)
            body.append(f"—")
            body.append(f"🧮 Итого: {total} заявок, CPA = {blended} $")
        else:
            body.append(f"—")
            body.append(f"🧮 Итого: 0 заявок")

    return hdr + "\n".join(body)

async def send_period_report(ctx, chat_id, period, label=""):
    for aid in get_enabled_accounts_in_order():
        txt = build_report(aid, period, label)
        if txt:
            await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")

# ============ БИЛЛИНГ ============
async def send_billing(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    rate = usd_to_kzt()
    for aid in get_enabled_accounts_in_order():
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

# ============ CPA ALERTS ============
async def cpa_alerts_job(ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = str(DEFAULT_REPORT_CHAT)
    if not chat_id:
        return
    now = datetime.now(ALMATY_TZ)
    if not (10 <= now.hour <= 22):
        return

    store = load_accounts()
    for aid in get_enabled_accounts_in_order():
        row = store.get(aid, {})
        alerts = row.get("alerts", {}) or {}
        target = float(alerts.get("target_cpl", 0.0) or 0.0)
        if not alerts.get("enabled") or target <= 0:
            continue

        mflags = row.get("metrics", {}) or {}
        use_msg = bool(mflags.get("messaging", False))
        use_lead = bool(mflags.get("leads", False))
        if not (use_msg or use_lead):
            continue

        try:
            _, ins = fetch_insight(aid, "today")
        except Exception:
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
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data="rep_today"),
         InlineKeyboardButton("Вчера", callback_data="rep_yday")],
        [InlineKeyboardButton("Прошедшая неделя", callback_data="rep_week")],
        [InlineKeyboardButton("Отчёт по аккаунту", callback_data="choose_acc_report")],
        [InlineKeyboardButton("Биллинг", callback_data="billing")],
        [InlineKeyboardButton("Настройки", callback_data="choose_acc_settings")],
        [InlineKeyboardButton("Синхронизировать кабинеты из BM", callback_data="sync_bm")],
    ])

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
    ids = list(store.keys()) if store else AD_ACCOUNTS_FALLBACK
    rows = []
    for aid in ids:
        rows.append([InlineKeyboardButton(f"{_flag_line(aid)}  {get_account_name(aid)}",
                                          callback_data=f"{prefix}|{aid}")])
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu")])
    return InlineKeyboardMarkup(rows)

def settings_kb(aid: str) -> InlineKeyboardMarkup:
    st = load_accounts().get(aid, {"enabled": True, "metrics": {}, "alerts": {}})
    en_text = "Выключить кабинет" if st.get("enabled", True) else "Включить кабинет"
    m_on = st.get("metrics", {}).get("messaging", True)
    l_on = st.get("metrics", {}).get("leads", False)
    a_on = st.get("alerts", {}).get("enabled", False) and (st.get("alerts", {}).get("target_cpl", 0) or 0) > 0
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(en_text, callback_data=f"toggle_enabled|{aid}")],
        [InlineKeyboardButton(f"💬 Переписки: {'ON' if m_on else 'OFF'}", callback_data=f"toggle_m|{aid}"),
         InlineKeyboardButton(f"♿️ Лиды сайта: {'ON' if l_on else 'OFF'}", callback_data=f"toggle_l|{aid}")],
        [InlineKeyboardButton(f"⚠️ Алерт CPA: {'ON' if a_on else 'OFF'}", callback_data=f"toggle_alert|{aid}")],
        [InlineKeyboardButton("✏️ Задать target CPA", callback_data=f"set_cpa|{aid}")],
        [InlineKeyboardButton("⬅️ Назад к списку", callback_data="choose_acc_settings")],
    ])

def period_kb_for(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data=f"one_today|{aid}"),
         InlineKeyboardButton("Вчера", callback_data=f"one_yday|{aid}")],
        [InlineKeyboardButton("Прошедшая неделя", callback_data=f"one_week|{aid}")],
        [InlineKeyboardButton("🗓 Свой диапазон", callback_data=f"one_custom|{aid}")],
        [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="choose_acc_report")],
    ])

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
        parse_mode="HTML"
    )

# ============ COMMANDS ============
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⛔️ Нет доступа. Отправь /whoami и добавь свой user_id в ALLOWED_USER_IDS."
        )
        return
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🤖 Выберите действие:",
        reply_markup=main_menu()
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    txt = (
        "Команды:\n"
        "/start — показать меню\n"
        "/help — подсказка\n"
        "/billing — список биллингов (неактивные аккаунты)\n"
        "/sync_accounts — синк кабинетов из BM\n"
        "/whoami — показать user_id и chat_id\n"
    )
    await update.message.reply_text(txt, reply_markup=ReplyKeyboardRemove())

async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    await send_billing(context, str(update.effective_chat.id))

async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    try:
        res = upsert_from_bm()
        await update.message.reply_text(
            f"✅ Синк завершён. Добавлено: {res['added']}, обновлено: {res['updated']}, "
            f"пропущено: {res['skipped']}. Всего: {res['total']}"
        )
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка синка: {e}")

# ======== CUSTOM RANGE INPUT ========
_RANGE_RE = re.compile(r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{2})\.(\d{4})\s*$")

def _parse_range(s: str):
    m = _RANGE_RE.match(s)
    if not m:
        return None
    d1 = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    d2 = datetime(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    if d1 > d2:
        d1, d2 = d2, d1
    return {"since": d1.strftime("%Y-%m-%d"), "until": d2.strftime("%Y-%m-%d")}, f"{d1.strftime('%d.%m')}-{d2.strftime('%d.%m')}"

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    ud = context.user_data
    if "await_range_for" in ud:
        aid = ud.pop("await_range_for")
        parsed = _parse_range(update.message.text.strip())
        if not parsed:
            await update.message.reply_text("Формат дат: 01.06.2025-07.06.2025. Попробуй ещё раз.")
            ud["await_range_for"] = aid
            return
        period, label = parsed
        txt = build_report(aid, period, label)
        await update.message.reply_text(txt or "Нет данных/нет доступа.", parse_mode="HTML")
        return

# ============ CALLBACKS ============
async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not _allowed(update):
        await q.edit_message_text("⛔️ Нет доступа.")
        return

    data = q.data or ""
    if data in ("menu",):
        await q.edit_message_text("🤖 Выберите действие:", reply_markup=main_menu())
        return

    if data == "rep_today":
        label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, str(q.message.chat.id), "today", label)
        return
    if data == "rep_yday":
        label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, str(q.message.chat.id), "yesterday", label)
        return
    if data == "rep_week":
        until = datetime.now(ALMATY_TZ) - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {"since": since.strftime("%Y-%m-%d"), "until": until.strftime("%Y-%m-%d")}
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, str(q.message.chat.id), period, label)
        return

    if data == "billing":
        await q.edit_message_text("📋 Биллинги (неактивные аккаунты):")
        await send_billing(context, str(q.message.chat.id))
        return

    if data == "sync_bm":
        try:
            res = upsert_from_bm()
            await q.edit_message_text(
                f"✅ Синк завершён. Добавлено: {res['added']}, обновлено: {res['updated']}, "
                f"пропущено: {res['skipped']}. Всего: {res['total']}",
                reply_markup=main_menu()
            )
        except Exception as e:
            await q.edit_message_text(f"⚠️ Ошибка синка: {e}", reply_markup=main_menu())
        return

    if data == "choose_acc_report":
        await q.edit_message_text("Выберите аккаунт:", reply_markup=accounts_kb("rep1"))
        return
    if data.startswith("rep1|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(f"Отчёт по: {get_account_name(aid)}\nВыбери период:",
                                  reply_markup=period_kb_for(aid))
        return
    if data.startswith("one_today|"):
        aid = data.split("|", 1)[1]
        label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Отчёт по {get_account_name(aid)} за {label}:")
        txt = build_report(aid, "today", label)
        await context.bot.send_message(str(q.message.chat.id), txt or "Нет данных/нет доступа.", parse_mode="HTML")
        return
    if data.startswith("one_yday|"):
        aid = data.split("|", 1)[1]
        label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Отчёт по {get_account_name(aid)} за {label}:")
        txt = build_report(aid, "yesterday", label)
        await context.bot.send_message(str(q.message.chat.id), txt or "Нет данных/нет доступа.", parse_mode="HTML")
        return
    if data.startswith("one_week|"):
        aid = data.split("|", 1)[1]
        until = datetime.now(ALMATY_TZ) - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {"since": since.strftime("%Y-%m-%d"), "until": until.strftime("%Y-%m-%d")}
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await q.edit_message_text(f"Отчёт по {get_account_name(aid)} за {label}:")
        txt = build_report(aid, period, label)
        await context.bot.send_message(str(q.message.chat.id), txt or "Нет данных/нет доступа.", parse_mode="HTML")
        return
    if data.startswith("one_custom|"):
        aid = data.split("|", 1)[1]
        context.user_data["await_range_for"] = aid
        await q.edit_message_text(
            f"Введи даты для {get_account_name(aid)} форматом: 01.06.2025-07.06.2025",
            reply_markup=period_kb_for(aid)
        )
        return

    if data == "choose_acc_settings":
        await q.edit_message_text("Выберите аккаунт для настроек:", reply_markup=accounts_kb("set1"))
        return
    if data.startswith("set1|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(f"Настройки: {get_account_name(aid)}", reply_markup=settings_kb(aid))
        return
    if data.startswith("toggle_enabled|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {})
        row["enabled"] = not row.get("enabled", True)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(f"Настройки: {get_account_name(aid)}", reply_markup=settings_kb(aid))
        return
    if data.startswith("toggle_m|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"metrics": {}})
        row["metrics"] = row.get("metrics", {})
        row["metrics"]["messaging"] = not row["metrics"].get("messaging", True)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(f"Настройки: {get_account_name(aid)}", reply_markup=settings_kb(aid))
        return
    if data.startswith("toggle_l|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"metrics": {}})
        row["metrics"] = row.get("metrics", {})
        row["metrics"]["leads"] = not row["metrics"].get("leads", False)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(f"Настройки: {get_account_name(aid)}", reply_markup=settings_kb(aid))
        return
    if data.startswith("toggle_alert|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        if alerts.get("enabled", False):
            alerts["enabled"] = False
        else:
            alerts["enabled"] = (float(alerts.get("target_cpl", 0) or 0) > 0)
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(f"Настройки: {get_account_name(aid)}", reply_markup=settings_kb(aid))
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
            reply_markup=settings_kb(aid)
        )
        context.user_data["await_cpa_for"] = aid
        return

# ввод target CPA
async def on_text_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    if "await_range_for" in context.user_data:
        return await on_text(update, context)

    if "await_cpa_for" in context.user_data:
        aid = context.user_data.pop("await_cpa_for")
        try:
            val = float(update.message.text.replace(",", ".").strip())
        except Exception:
            await update.message.reply_text("Введите число, например: 2.5 (или 0 чтобы выключить)")
            context.user_data["await_cpa_for"] = aid
            return
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        alerts["target_cpl"] = float(val)
        alerts["enabled"] = (val > 0)  # 0 — выключаем
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        if val > 0:
            await update.message.reply_text(
                f"✅ Target CPA для {get_account_name(aid)} обновлён: {val:.2f} $ (алерты ВКЛ)",
            )
        else:
            await update.message.reply_text(
                f"✅ Target CPA для {get_account_name(aid)} установлен 0 — алерты ВЫКЛ",
            )

# ============ JOBS ============
async def daily_report_job(ctx: ContextTypes.DEFAULT_TYPE):
    if not DEFAULT_REPORT_CHAT:
        return
    label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
    await send_period_report(ctx, str(DEFAULT_REPORT_CHAT), "yesterday", label)

def schedule_cpa_alerts(app: Application):
    for h in range(10, 23):
        app.job_queue.run_daily(cpa_alerts_job, time=time(hour=h, minute=0, tzinfo=ALMATY_TZ))

# ============ APP ============
def build_app() -> Application:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("billing", cmd_billing))
    app.add_handler(CommandHandler("sync_accounts", cmd_sync))
    app.add_handler(CallbackQueryHandler(on_cb))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_any))

    # Требуется установка python-telegram-bot[job-queue] в requirements.txt
    app.job_queue.run_daily(daily_report_job, time=time(hour=9, minute=30, tzinfo=ALMATY_TZ))
    schedule_cpa_alerts(app)

    return app

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    build_app().run_polling(allowed_updates=Update.ALL_TYPES)
