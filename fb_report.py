# fb_report.py
import os
import json
import math
import asyncio
from math import ceil
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
    ReplyKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ========= ЛОКАЦИЯ ДАННЫХ (персистентные файлы) =========
DATA_DIR = os.getenv("DATA_DIR", "/data")
os.makedirs(DATA_DIR, exist_ok=True)

def _p(name: str) -> str:
    """Абсолютный путь к файлам настроек/кэшей."""
    return os.path.join(DATA_DIR, name)

# ========= КРЕДЫ / ENV =========
ACCESS_TOKEN = os.getenv(
    "FB_ACCESS_TOKEN",
    "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
)
APP_ID = os.getenv("FB_APP_ID", "1336645834088573")
APP_SECRET = os.getenv("FB_APP_SECRET", "01bf23c5f726c59da318daa82dd0e9dc")
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# читаем и TELEGRAM_TOKEN, и TG_BOT_TOKEN — что задано
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN") or os.getenv("TG_BOT_TOKEN") or "PASTE_TELEGRAM_BOT_TOKEN"
CHAT_ID = os.getenv("CHAT_ID") or os.getenv("TG_CHAT_ID") or "-1002679045097"

# ========= ФАЙЛЫ / КОНСТАНТЫ =========
ACCOUNTS_JSON = _p("accounts.json")       # база кабинетов (enabled/metrics/alerts)
FORECAST_CACHE_FILE = _p("forecast_cache.json")
FX_CACHE_FILE = _p("fx_cache.json")       # кеш курса USD→KZT на 12 ч
ALMATY_TZ = timezone("Asia/Almaty")

# API курса (apilayer)
FX_API_KEY = os.getenv("FX_API_KEY", "LYr6odX08iC6PXKqQSTT4QtKouCFcWeF")
FX_CACHE_HOURS = 12

# ========= Фолбэк-список, если accounts.json пустой =========
AD_ACCOUNTS_FALLBACK = [
    "act_1415004142524014", "act_719853653795521", "act_1206987573792913",
    "act_1108417930211002", "act_2342025859327675", "act_844229314275496",
    "act_1333550570916716", "act_195526110289107", "act_2145160982589338",
    "act_508239018969999", "act_1357165995492721", "act_798205335840576",
    "act_806046635254439"
]

# Человекочитаемые имена (дополняются динамически)
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

# Исключения из BM (например, кенсе)
EXCLUDED_AD_ACCOUNT_IDS = {"act_1042955424178074", "act_4030694587199998"}
EXCLUDED_NAME_KEYWORDS = {"kense", "кенсе"}

# ========= Хелперы работы с конфигом =========
def load_accounts() -> dict:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_accounts(data: dict):
    with open(ACCOUNTS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _normalize_act_id(aid: str) -> str:
    aid = str(aid).strip()
    return aid if aid.startswith("act_") else f"act_{aid}"

def _looks_excluded_by_name(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_NAME_KEYWORDS)

def get_account_name(acc_id: str) -> str:
    store = load_accounts()
    if acc_id in store and store[acc_id].get("name"):
        return store[acc_id]["name"]
    return ACCOUNT_NAMES.get(acc_id, acc_id)

def get_enabled_accounts_in_order() -> list[str]:
    data = load_accounts()
    if not data:
        return AD_ACCOUNTS_FALLBACK
    ordered = [acc_id for acc_id, row in data.items() if row.get("enabled", True)]
    return ordered or AD_ACCOUNTS_FALLBACK

# ========= СИНХРОНИЗАЦИЯ ИЗ BM =========
def upsert_accounts_from_fb() -> dict:
    """
    Добавляет новые аккаунты и обновляет ИМЯ. Все пользовательские флаги (enabled/metrics/alerts)
    НЕ ТРОГАЕТ. Это обеспечивает «сохранение настроек после перезапуска».
    """
    data = load_accounts()
    me = User(fbid="me")
    fetched = list(me.get_ad_accounts(fields=["account_id", "name", "account_status"]))
    added, updated, skipped = 0, 0, 0
    for item in fetched:
        acc_id = _normalize_act_id(item.get("account_id"))
        name = item.get("name") or acc_id
        if acc_id in EXCLUDED_AD_ACCOUNT_IDS or _looks_excluded_by_name(name):
            skipped += 1
            continue
        ACCOUNT_NAMES.setdefault(acc_id, name)
        if acc_id in data:
            if name and data[acc_id].get("name") != name:
                data[acc_id]["name"] = name
                updated += 1
        else:
            data[acc_id] = {
                "name": name,
                "enabled": True,
                "metrics": {"messaging": False, "leads": False},
                "alerts": {"enabled": False, "target_cpl": 0.0, "target_cpm": 0.0},
            }
            added += 1
    save_accounts(data)
    return {"added": added, "updated": updated, "skipped": skipped, "total": len(data)}

# ========= Курс USD→KZT с кешем и надбавкой +5 ₸ =========
def _load_fx_cache():
    try:
        with open(FX_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def _save_fx_cache(obj: dict):
    with open(FX_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def get_usd_to_kzt() -> float:
    cache = _load_fx_cache()
    now_ts = datetime.now().timestamp()
    if cache.get("rate") and cache.get("ts") and (now_ts - cache["ts"] <= FX_CACHE_HOURS * 3600):
        return float(cache["rate"])
    try:
        url = "https://api.apilayer.com/fixer/latest?base=USD&symbols=KZT"
        headers = {"apikey": FX_API_KEY}
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        raw = float(data["rates"]["KZT"])
        rate = raw + 5.0  # надбавка +5 ₸ (твоё требование)
        _save_fx_cache({"rate": rate, "ts": now_ts})
        return rate
    except Exception:
        rate = 500.0 + 5.0
        _save_fx_cache({"rate": rate, "ts": now_ts})
        return rate

def kzt_round_up_1000(v: float) -> int:
    return int(math.ceil(v / 1000.0) * 1000)

# ========= Утилиты форматирования =========
def format_int(n) -> str:
    try:
        return f"{int(float(n)):,}".replace(",", " ")
    except:
        return "0"

def is_account_active(acc_id: str) -> bool:
    try:
        st = AdAccount(acc_id).api_get(fields=["account_status"])["account_status"]
        return st == 1
    except:
        return False

def extract_actions(insight) -> dict:
    actions = insight.get("actions", []) or []
    return {a.get("action_type"): float(a.get("value", 0)) for a in actions}

def account_metrics_flags(acc_id: str) -> dict:
    store = load_accounts()
    row = store.get(acc_id, {})
    metrics = row.get("metrics", {}) or {}
    return {"messaging": bool(metrics.get("messaging", False)),
            "leads": bool(metrics.get("leads", False))}

# ========= Facebook → отчёты =========
def get_insight(acc_id: str, period) -> tuple[str, dict]:
    account = AdAccount(acc_id)
    fields = ["impressions", "cpm", "clicks", "cpc", "spend", "actions"]
    params = {'level': 'account'}
    if isinstance(period, dict):
        params["time_range"] = period
    else:
        params["date_preset"] = period
    insights = account.get_insights(fields=fields, params=params)
    name = account.api_get(fields=['name']).get('name', get_account_name(acc_id))
    return name, insights[0] if insights else None

def build_report_text(acc_id: str, period, date_label="") -> str:
    try:
        name, insight = get_insight(acc_id, period)
    except Exception as e:
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return f"⚠ Ошибка по {get_account_name(acc_id)}:\n\n{e}"

    badge = "🟢" if is_account_active(acc_id) else "🔴"
    date_info = f" ({date_label})" if date_label else ""
    head = f"{badge} <b>{name}</b>{date_info}\n"

    if not insight:
        return head + "Нет данных за выбранный период"

    body = []
    body.append(f"👁 Показы: {format_int(insight.get('impressions', 0))}")
    body.append(f"🎯 CPM: {round(float(insight.get('cpm', 0) or 0), 2)} $")
    body.append(f"🖱 Клики: {format_int(insight.get('clicks', 0))}")
    body.append(f"💸 CPC: {round(float(insight.get('cpc', 0) or 0), 2)} $")
    body.append(f"💵 Затраты: {round(float(insight.get('spend', 0) or 0), 2)} $")

    acts = extract_actions(insight)
    flags = account_metrics_flags(acc_id)

    if flags["messaging"]:
        conv = acts.get('onsite_conversion.messaging_conversation_started_7d', 0)
        body.append(f"✉️ Переписки: {int(conv)}")
        spend = float(insight.get('spend', 0) or 0)
        if conv > 0:
            body.append(f"💬💲 Цена переписки: {round(spend/conv, 2)} $")

    if flags["leads"]:
        leads = acts.get('Website Submit Applications', 0) or \
                acts.get('offsite_conversion.fb_pixel_submit_application', 0) or \
                acts.get('offsite_conversion.fb_pixel_lead', 0) or \
                acts.get('lead', 0)
        body.append(f"📩 Лиды: {int(leads)}")
        spend = float(insight.get('spend', 0) or 0)
        if leads > 0:
            body.append(f"📩💲 Цена лида: {round(spend/leads, 2)} $")

    return head + "\n".join(body)

async def send_period_report(context, chat_id, period, date_label=""):
    for acc_id in get_enabled_accounts_in_order():
        txt = build_report_text(acc_id, period, date_label)
        if txt:
            await context.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")

# ========= Биллинг =========
async def send_billing_list(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    rate = get_usd_to_kzt()
    for acc_id in get_enabled_accounts_in_order():
        try:
            info = AdAccount(acc_id).api_get(fields=["name", "account_status", "balance"])
        except Exception:
            continue
        if info.get("account_status") == 1:
            continue
        name = info.get("name", get_account_name(acc_id))
        usd = float(info.get("balance", 0) or 0) / 100.0
        kzt = kzt_round_up_1000(usd * rate)
        badge = "🔴"
        txt = (f"{badge} <b>{name}</b>\n"
               f"   💵 {usd:.2f} $  |  🇰🇿 {format_int(kzt)} ₸")
        await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")

# ========= Меню/клавиатуры =========
def main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data="rep_today"),
         InlineKeyboardButton("Вчера", callback_data="rep_yesterday")],
        [InlineKeyboardButton("Прошедшая неделя", callback_data="rep_week")],
        [InlineKeyboardButton("Отчёт по аккаунту", callback_data="choose_acc_for_report")],
        [InlineKeyboardButton("Биллинг", callback_data="billing_now")],
        [InlineKeyboardButton("Настройки", callback_data="choose_acc_for_settings")],
        [InlineKeyboardButton("Синхронизировать из BM", callback_data="sync_from_bm")],
    ])

def _acc_flags_text(acc_id: str) -> str:
    store = load_accounts()
    row = store.get(acc_id, {})
    enabled = bool(row.get("enabled", True))
    metrics = row.get("metrics", {}) or {}
    alerts = row.get("alerts", {}) or {}
    on = "🟢" if enabled else "🔴"
    m = "💬" if metrics.get("messaging") else ""
    l = "♿️" if metrics.get("leads") else ""
    al = "⚠️" if alerts.get("enabled") else ""
    return f"{on} {m}{l}{al}".strip()

def accounts_list_kb(prefix: str) -> InlineKeyboardMarkup:
    store = load_accounts()
    acc_ids = list(store.keys()) if store else AD_ACCOUNTS_FALLBACK
    rows = []
    for acc_id in acc_ids:
        name = get_account_name(acc_id)
        flags = _acc_flags_text(acc_id)
        label = f"{flags}  {name}" if flags else name
        rows.append([InlineKeyboardButton(label, callback_data=f"{prefix}|{acc_id}")])
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu_back")])
    return InlineKeyboardMarkup(rows)

def settings_kb_for(acc_id: str) -> InlineKeyboardMarkup:
    store = load_accounts()
    row = store.get(acc_id, {"enabled": True, "metrics": {}, "alerts": {}})
    en = "Выключить кабинет" if row.get("enabled", True) else "Включить кабинет"
    m_on = row.get("metrics", {}).get("messaging", False)
    l_on = row.get("metrics", {}).get("leads", False)
    a_on = row.get("alerts", {}).get("enabled", False)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(en, callback_data=f"toggle_enabled|{acc_id}")],
        [InlineKeyboardButton(f"💬 Переписки: {'ON' if m_on else 'OFF'}",
                              callback_data=f"toggle_messaging|{acc_id}"),
         InlineKeyboardButton(f"♿️ Лиды сайта: {'ON' if l_on else 'OFF'}",
                              callback_data=f"toggle_leads|{acc_id}")],
        [InlineKeyboardButton(f"⚠️ Оповещения CPA: {'ON' if a_on else 'OFF'}",
                              callback_data=f"toggle_alerts|{acc_id}")],
        [InlineKeyboardButton("⬅️ Назад к списку", callback_data="choose_acc_for_settings")]
    ])

def one_account_period_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data=f"report_one|{acc_id}|today"),
         InlineKeyboardButton("Вчера", callback_data=f"report_one|{acc_id}|yesterday")],
        [InlineKeyboardButton("Прошедшая неделя", callback_data=f"report_one|{acc_id}|week")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="choose_acc_for_report")]
    ])

# ========= Команды =========
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🤖 Выберите действие:",
        reply_markup=main_menu_kb()
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (
        "Доступные команды:\n"
        "/start — показать меню\n"
        "/help — подсказка\n"
        "/billing — список биллингов\n"
        "/sync_accounts — подтянуть кабинеты из BM\n"
    )
    await update.message.reply_text(txt)

async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_billing_list(context, update.effective_chat.id)

async def cmd_sync_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        res = upsert_accounts_from_fb()
        msg = (f"✅ Синхронизировано.\nДобавлено: {res['added']}\n"
               f"Обновлено имён: {res['updated']}\n"
               f"Пропущено: {res['skipped']}\nВсего: {res['total']}")
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка синка: {e}")

# ========= Callback-и =========
async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""

    if data == "menu_back":
        await q.edit_message_text("🤖 Выберите действие:", reply_markup=main_menu_kb())
        return

    # общие отчёты
    if data == "rep_today":
        label = datetime.now(ALMATY_TZ).strftime('%d.%m.%Y')
        await q.edit_message_text(f"Отчёт за {label}. Готовлю…")
        await send_period_report(context, q.message.chat.id, 'today', label)
        return

    if data == "rep_yesterday":
        label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime('%d.%m.%Y')
        await q.edit_message_text(f"Отчёт за {label}. Готовлю…")
        await send_period_report(context, q.message.chat.id, 'yesterday', label)
        return

    if data == "rep_week":
        until = datetime.now(ALMATY_TZ) - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await q.edit_message_text(f"Отчёт за {label}. Готовлю…")
        await send_period_report(context, q.message.chat.id, period, label)
        return

    # выбор аккаунта → выбор периода → показ отчёта
    if data == "choose_acc_for_report":
        await q.edit_message_text("Выберите аккаунт:", reply_markup=accounts_list_kb("choose_one"))
        return

    if data.startswith("choose_one|"):
        acc_id = data.split("|", 1)[1]
        await q.edit_message_text(f"Период для {get_account_name(acc_id)}:", reply_markup=one_account_period_kb(acc_id))
        return

    if data.startswith("report_one|"):
        _, acc_id, which = data.split("|", 2)
        if which == "today":
            label = datetime.now(ALMATY_TZ).strftime('%d.%m.%Y')
            period = 'today'
        elif which == "yesterday":
            label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime('%d.%m.%Y')
            period = 'yesterday'
        else:  # week
            until = datetime.now(ALMATY_TZ) - timedelta(days=1)
            since = until - timedelta(days=6)
            period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
            label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%м')}"
        await q.edit_message_text(f"Отчёт по {get_account_name(acc_id)} ({label})…")
        txt = build_report_text(acc_id, period, label)
        if txt:
            await context.bot.send_message(q.message.chat.id, txt, parse_mode="HTML")
        else:
            await context.bot.send_message(q.message.chat.id, "Нет данных или нет доступа.")
        return

    # биллинг
    if data == "billing_now":
        await q.edit_message_text("📋 Биллинги (неактивные аккаунты):")
        await send_billing_list(context, q.message.chat.id)
        return

    # настройки
    if data == "choose_acc_for_settings":
        await q.edit_message_text("Выберите аккаунт для настроек:", reply_markup=accounts_list_kb("settings"))
        return

    if data.startswith("settings|"):
        acc_id = data.split("|", 1)[1]
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}", reply_markup=settings_kb_for(acc_id))
        return

    # переключатели настроек (постоянно сохраняются в /data/accounts.json)
    if data.startswith("toggle_enabled|"):
        acc_id = data.split("|", 1)[1]
        store = load_accounts()
        row = store.get(acc_id, {})
        row["enabled"] = not row.get("enabled", True)
        store[acc_id] = row
        save_accounts(store)
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}", reply_markup=settings_kb_for(acc_id))
        return

    if data.startswith("toggle_messaging|"):
        acc_id = data.split("|", 1)[1]
        store = load_accounts()
        row = store.get(acc_id, {"metrics": {}})
        metrics = row.get("metrics", {})
        metrics["messaging"] = not metrics.get("messaging", False)
        row["metrics"] = metrics
        store[acc_id] = row
        save_accounts(store)
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}", reply_markup=settings_kb_for(acc_id))
        return

    if data.startswith("toggle_leads|"):
        acc_id = data.split("|", 1)[1]
        store = load_accounts()
        row = store.get(acc_id, {"metrics": {}})
        metrics = row.get("metrics", {})
        metrics["leads"] = not metrics.get("leads", False)
        row["metrics"] = metrics
        store[acc_id] = row
        save_accounts(store)
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}", reply_markup=settings_kb_for(acc_id))
        return

    if data.startswith("toggle_alerts|"):
        acc_id = data.split("|", 1)[1]
        store = load_accounts()
        row = store.get(acc_id, {"alerts": {}})
        alerts = row.get("alerts", {})
        alerts["enabled"] = not alerts.get("enabled", False)
        row["alerts"] = alerts
        store[acc_id] = row
        save_accounts(store)
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}", reply_markup=settings_kb_for(acc_id))
        return

    # синк из BM через кнопку
    if data == "sync_from_bm":
        try:
            res = upsert_accounts_from_fb()
            await q.edit_message_text(
                f"✅ Синхронизировано.\nДобавлено: {res['added']}\n"
                f"Обновлено имён: {res['updated']}\nПропущено: {res['skipped']}\nВсего: {res['total']}",
                reply_markup=main_menu_kb()
            )
        except Exception as e:
            await q.edit_message_text(f"⚠️ Ошибка синка: {e}", reply_markup=main_menu_kb())
        return

# ========= Планировщик (ежедневный отчёт «вчера», 09:30) =========
async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_period_report(context, CHAT_ID, 'yesterday', label)

# ========= Boot =========
def build_app() -> Application:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # команды
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("billing", cmd_billing))
    app.add_handler(CommandHandler("sync_accounts", cmd_sync_accounts))

    # inline callbacks
    app.add_handler(CallbackQueryHandler(on_cb))

    # заглушка на текст (если потребуется)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, lambda *_: None))

    # ежедневный отчёт 09:30 по Алма-Ате
    app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=ALMATY_TZ))

    return app

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    build_app().run_polling(allowed_updates=Update.ALL_TYPES)
