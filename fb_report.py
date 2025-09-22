import asyncio
import json
import time
from math import ceil
from datetime import datetime, timedelta, time as dtime

import requests
from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# ================== НАСТРОЙКИ ==================
ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"  # группа
TZ = timezone("Asia/Almaty")

# Инициализация FB API
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# --------- Курсы валют (apilayer) ---------
API_LAYER_KEY = "LYr6odX08iC6PXKqQSTT4QtKouCFcWeF"
FX_CACHE_FILE = "fx_cache.json"
FX_TTL_SECONDS = 12 * 60 * 60  # 12 часов
FX_FALLBACK = 495.0
FX_ADD_KZT = 5.0  # «+5 тг к курсу»

def _fx_load():
    try:
        with open(FX_CACHE_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}

def _fx_save(cache: dict):
    try:
        with open(FX_CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception:
        pass

def get_usd_kzt_rate_raw() -> float:
    now = int(time.time())
    cache = _fx_load()
    if cache.get("rate") and cache.get("ts") and now - cache["ts"] < FX_TTL_SECONDS:
        return float(cache["rate"])
    try:
        url = "https://api.apilayer.com/exchangerates_data/latest"
        params = {"base": "USD", "symbols": "KZT"}
        headers = {"apikey": API_LAYER_KEY}
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        resp.raise_for_status()
        rate = float(resp.json()["rates"]["KZT"])
        _fx_save({"rate": rate, "ts": now})
        return rate
    except Exception:
        return FX_FALLBACK

def get_usd_kzt_rate_with_add() -> float:
    return get_usd_kzt_rate_raw() + FX_ADD_KZT

def usd_to_kzt(amount_usd: float) -> int:
    return int(round(amount_usd * get_usd_kzt_rate_with_add(), 0))

def format_fx_line() -> str:
    raw = get_usd_kzt_rate_raw()
    with_add = raw + FX_ADD_KZT
    return f"Курс: 1 $ = {with_add:.2f} ₸ (сыро: {raw:.2f} + {FX_ADD_KZT:.0f})"

# ----------------- Аккаунты -----------------
# Убраны оба «кенсе». Добавлены новые по твоему списку.
AD_ACCOUNTS = [
    "act_1415004142524014",  # ЖС Астана
    "act_719853653795521",   # ЖС Караганда
    "act_1206987573792913",  # ЖС Павлодар
    "act_1108417930211002",  # ЖС Актау
    "act_2342025859327675",  # ЖС Атырау
    "act_844229314275496",   # ЖС Актобе
    "act_1333550570916716",  # ЖС Юг (Алматы)
    "act_195526110289107",   # ЖС Тараз
    "act_2145160982589338",  # ЖС Шымкент
    "act_2183299115451405",  # ЖС Шымкент 2 (рядом с Шымкент)
    "act_508239018969999",   # Фитнес Поинт
    "act_1357165995492721",  # Ария Степи
    "act_798205335840576",   # Инвестиции
    "act_2310940436006402",  # Teplo Almaty
    "act_776865548258700",   # Shanghai Ташкент
    "act_1104357140269368",  # Teplo Tashkent
    "act_584782470655012",   # TM Group
    "act_353220323925035",   # Zibak.tj
]

ACCOUNT_NAMES = {
    "act_1415004142524014": "ЖС Астана",
    "act_719853653795521": "ЖС Караганда",
    "act_1206987573792913": "ЖС Павлодар",
    "act_1108417930211002": "ЖС Актау",
    "act_2342025859327675": "ЖС Атырау",
    "act_844229314275496": "ЖС Актобе",
    "act_1333550570916716": "ЖС Юг (Алматы)",
    "act_195526110289107":  "ЖС Тараз",
    "act_2145160982589338": "ЖС Шымкент",
    "act_2183299115451405": "ЖС Шымкент 2",
    "act_508239018969999":  "Фитнес Поинт",
    "act_1357165995492721": "Ария Степи",
    "act_798205335840576":  "Инвестиции",
    "act_2310940436006402": "Teplo Almaty",
    "act_776865548258700":  "Shanghai Ташкент",
    "act_1104357140269368": "Teplo Tashkent",
    "act_584782470655012":  "TM Group",
    "act_353220323925035":  "Zibak.tj",
}

# Где нужны переписки
MESSAGING_ACCOUNTS = {
    "act_1415004142524014",
    "act_1108417930211002",
    "act_2342025859327675",
    "act_1333550570916716",
    "act_844229314275496",
    "act_1206987573792913",
    "act_195526110289107",
    "act_2145160982589338",
    "act_719853653795521",
    "act_2183299115451405",
    "act_2310940436006402",
    "act_776865548258700",
    "act_1104357140269368",
    "act_584782470655012",
    "act_353220323925035",
}

# Где нужны лиды с сайта
LEAD_FORM_ACCOUNTS = {
    "act_584782470655012",  # TM Group — и лиды, и переписки
}

# ------------- Вспомогалки -------------
account_statuses = {}
FORECAST_CACHE_FILE = "forecast_cache.json"

def is_account_active(account_id) -> str:
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except Exception:
        return "🔴"

def format_number(num):
    try:
        return f"{int(float(num)):,}".replace(",", " ")
    except Exception:
        return str(num)

def safe_fb_call(func, *args, **kwargs):
    """Безопасный вызов FB API: ловим нет доступа/токен/прочее — возвращаем None."""
    try:
        return func(*args, **kwargs)
    except Exception:
        return None

# ---------- Получение отчёта по аккаунту ----------
def get_facebook_data(account_id, date_preset, date_label=''):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}

    insights = safe_fb_call(account.get_insights, fields=fields, params=params)
    name_data = safe_fb_call(account.api_get, fields=['name', 'account_status'])
    if name_data is None:
        # Нет прав/доступа — молча пропускаем
        return None

    account_name = name_data.get('name', account_id)
    date_info = f" ({date_label})" if date_label else ""
    header = f"{is_account_active(account_id)} <b>{account_name}</b>{date_info}\n"

    if not insights:
        return header + "Нет данных за выбранный период"

    insight = insights[0]
    report = (
        f"{header}"
        f"👁 Показы: {format_number(insight.get('impressions', '0'))}\n"
        f"🎯 CPM: {round(float(insight.get('cpm', 0) or 0), 2)} $\n"
        f"🖱 Клики: {format_number(insight.get('clicks', '0'))}\n"
        f"💸 CPC: {round(float(insight.get('cpc', 0) or 0), 2)} $\n"
        f"💵 Затраты: {round(float(insight.get('spend', 0) or 0), 2)} $"
    )

    actions = {a['action_type']: float(a['value']) for a in insight.get('actions', [])}

    # Переписки
    if account_id in MESSAGING_ACCOUNTS:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0)
        report += f"\n✉️ Начата переписка: {int(conv)}"
        if conv > 0:
            spend = float(insight.get('spend', 0) or 0)
            report += f"\n💬💲 Цена переписки: {round(spend / conv, 2)} $"

    # Лиды с сайта
    if account_id in LEAD_FORM_ACCOUNTS:
        leads = (
            actions.get('offsite_conversion.fb_pixel_submit_application', 0)
            or actions.get('offsite_conversion.fb_pixel_lead', 0)
            or actions.get('lead', 0)
        )
        report += f"\n📩 Заявки: {int(leads)}"
        if leads > 0:
            spend = float(insight.get('spend', 0) or 0)
            report += f"\n📩💲 Цена заявки: {round(spend / leads, 2)} $"

    return report

# ---------- Отправка отчёта по всем аккаунтам ----------
async def send_report(context, chat_id, period, date_label=''):
    for acc in AD_ACCOUNTS:
        msg = get_facebook_data(acc, period, date_label)
        if msg:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

# ---------- Мониторинг статуса аккаунтов ----------
async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in AD_ACCOUNTS:
        try:
            account = AdAccount(account_id)
            info = safe_fb_call(account.api_get, fields=['name', 'account_status', 'balance'])
            if not info:
                continue
            status = info.get('account_status')
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = info.get('name', account_id)
                balance = float(info.get('balance', 0) or 0) / 100
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"⚠️ ⚠️ ⚠️ Ахтунг! {name}! у нас биллинг - {balance:.2f} $",
                    parse_mode='HTML'
                )
            account_statuses[account_id] = status
        except Exception:
            continue

# ---------- Прогноз даты списания (как было) ----------
async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(TZ).date()
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except Exception:
        cache = {}

    for acc_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(acc_id)
            info = safe_fb_call(acc.api_get, fields=["name", "spend_cap", "amount_spent"])
            if not info:
                continue
            spend_cap = float(info.get("spend_cap", 0) or 0) / 100
            spent = float(info.get("amount_spent", 0) or 0) / 100
            available = spend_cap - spent

            # Сумма дневных бюджетов активных кампаний
            campaigns = safe_fb_call(acc.get_campaigns, fields=["name", "effective_status", "daily_budget"])
            if campaigns is None:
                continue
            daily_budget = sum(
                (int(c.get("daily_budget", 0) or 0) / 100)
                for c in campaigns
                if c.get("effective_status") == "ACTIVE"
            )
            if daily_budget == 0:
                continue

            days_left = ceil(available / daily_budget) if daily_budget > 0 else 0
            billing_date = today + timedelta(days=days_left)

            if (billing_date - today).days == 3:
                if cache.get(acc_id) == billing_date.isoformat():
                    continue
                name = ACCOUNT_NAMES.get(acc_id, acc_id.replace("act_", ""))
                msg = (
                    f"⚠️ <b>{name}</b>\n\n"
                    f"Предполагаемое списание: <b>{spend_cap:.2f} $</b>\n"
                    f"Дата: <b>{billing_date.strftime('%d.%m.%Y')}</b>\n"
                    f"До порога осталось: <b>{available:.2f} $</b>\n"
                    f"Суммарный дневной бюджет: <b>{daily_budget:.2f} $</b>\n"
                    f"Осталось дней: <b>{days_left}</b>"
                )
                await context.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode='HTML')
                cache[acc_id] = billing_date.isoformat()
        except Exception:
            continue

    try:
        with open(FORECAST_CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception:
        pass

# ---------- Список биллингов (ТОЛЬКО неактивные) ----------
async def send_billing_list(context: ContextTypes.DEFAULT_TYPE, chat_id: str):
    lines = []
    for acc_id in AD_ACCOUNTS:
        try:
            account = AdAccount(acc_id)
            info = safe_fb_call(account.api_get, fields=['name', 'account_status', 'balance'])
            if not info:
                continue
            status = info.get('account_status')
            if status == 1:
                # активные не показываем
                continue
            name = info.get('name', ACCOUNT_NAMES.get(acc_id, acc_id))
            usd = (float(info.get('balance', 0) or 0) / 100.0)
            kzt = usd_to_kzt(usd)
            lines.append(f"🔴 <b>{name}</b>\n   💵 {usd:.2f} $  |  🇰🇿 {kzt:,} ₸".replace(",", " "))
        except Exception:
            continue

    if not lines:
        text = "✅ Сейчас нет неактивных аккаунтов."
    else:
        text = "📋 <b>Биллинги (неактивные аккаунты)</b>\n\n" + "\n\n".join(lines) + f"\n\n{format_fx_line()}"

    await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')

# ================== Хендлеры команд/кнопок ==================
def main_menu_inline():
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data="today"),
                InlineKeyboardButton("Вчера", callback_data="yesterday"),
                InlineKeyboardButton("Прошлая неделя", callback_data="week"),
            ],
            [InlineKeyboardButton("Показать биллинги", callback_data="billing")],
        ]
    )

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # В группах и в личке покажем inline-кнопки под сообщением
    await update.effective_chat.send_message(
        "🤖 Выберите отчёт:",
        reply_markup=main_menu_inline()
    )

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (
        "🆘 Доступные команды:\n"
        "/today — отчёт за сегодня\n"
        "/yesterday — отчёт за вчера\n"
        "/week — отчёт за прошедшую неделю\n"
        "/billing — показать биллинги (неактивные аккаунты)\n"
        "/help — помощь\n\n"
        "Также доступны кнопки под сообщением /start."
    )
    await update.effective_chat.send_message(txt)

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    label = datetime.now(TZ).strftime('%d.%m.%Y')
    await send_report(context, update.effective_chat.id, 'today', label)

async def cmd_yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(TZ) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, update.effective_chat.id, 'yesterday', label)

async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    until = datetime.now(TZ) - timedelta(days=1)
    since = until - timedelta(days=6)
    period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
    label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
    await send_report(context, update.effective_chat.id, period, label)

async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_billing_list(context, update.effective_chat.id)

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    await query.answer()
    data = query.data
    if data == "today":
        await cmd_today(update, context)
    elif data == "yesterday":
        await cmd_yesterday(update, context)
    elif data == "week":
        await cmd_week(update, context)
    elif data == "billing":
        await cmd_billing(update, context)

# На случай, если кто-то пишет словами
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip().lower()
    if text in ("сегодня", "today"):
        await cmd_today(update, context)
    elif text in ("вчера", "yesterday"):
        await cmd_yesterday(update, context)
    elif text in ("прошедшая неделя", "прошлая неделя", "week"):
        await cmd_week(update, context)
    elif text in ("биллинги", "billing"):
        await cmd_billing(update, context)
    else:
        await cmd_help(update, context)

# ================== Планировщик задач ==================
async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(TZ) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', label)
    # следом — список биллингов
    await send_billing_list(context, CHAT_ID)

# ================== APP ==================
app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", cmd_start))
app.add_handler(CommandHandler("help", cmd_help))
app.add_handler(CommandHandler("today", cmd_today))
app.add_handler(CommandHandler("yesterday", cmd_yesterday))
app.add_handler(CommandHandler("week", cmd_week))
app.add_handler(CommandHandler("billing", cmd_billing))
app.add_handler(CallbackQueryHandler(on_button))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

# Мониторинг статуса раз в 10 минут
app.job_queue.run_repeating(check_billing, interval=600, first=10)
# Ежедневный отчёт + биллинги в 09:30
app.job_queue.run_daily(daily_report, time=dtime(hour=9, minute=30, tzinfo=TZ))
# Прогноз биллинга в 09:00
app.job_queue.run_daily(check_billing_forecast, time=dtime(hour=9, minute=0, tzinfo=TZ))

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
