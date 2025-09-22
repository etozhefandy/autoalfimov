import asyncio
import json
import math
import os
import time
from math import ceil
from datetime import datetime, timedelta, time as dtime
from pytz import timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)

# ===================== НАСТРОЙКИ FB =====================
ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# ===================== АККАУНТЫ В ОТЧЁТЕ (порядок важен) =====================
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
    "act_2183299115451405",  # ЖС Шымкент 2 (рядом с первым)
    "act_2310940436006402",  # Teplo Almaty
    "act_776865548258700",   # Shanghai Tashkent
    "act_1104357140269368",  # Teplo Tashkent
    "act_584782470655012",   # TM Group
    "act_353220323925035",   # Zibak.tj
    "act_508239018969999",   # Фитнес Поинт
    "act_1357165995492721",  # Ария степи
    "act_798205335840576",   # Инвестиции
]

# где надо показывать "Начата переписка" и её стоимость
MESSAGING_ACCOUNTS = {
    # Все ЖС
    "act_1415004142524014",
    "act_719853653795521",
    "act_1206987573792913",
    "act_1108417930211002",
    "act_2342025859327675",
    "act_844229314275496",
    "act_1333550570916716",
    "act_195526110289107",
    "act_2145160982589338",
    "act_2183299115451405",
    # «как у ЖС»:
    "act_2310940436006402",  # Teplo Almaty
    "act_776865548258700",   # Shanghai Tashkent
    "act_1104357140269368",  # Teplo Tashkent
    # Дополнительно:
    "act_353220323925035",   # Zibak.tj
    "act_584782470655012",   # TM Group (и переписки, и лиды)
}

# где надо показывать лиды с сайта (и их стоимость)
LEAD_WEBSITE_ACCOUNTS = {
    "act_584782470655012",   # TM Group
    "act_798205335840576",   # Инвестиции
    # при необходимости добавим другие
}

ACCOUNT_NAMES = {
    "act_1415004142524014": "ЖС Астана",
    "act_719853653795521": "ЖС Караганда",
    "act_1206987573792913": "ЖС Павлодар",
    "act_1108417930211002": "ЖС Актау",
    "act_2342025859327675": "ЖС Атырау",
    "act_844229314275496": "ЖС Актобе",
    "act_1333550570916716": "ЖС Юг (Алматы)",
    "act_195526110289107": "ЖС Тараз",
    "act_2145160982589338": "ЖС Шымкент",
    "act_2183299115451405": "ЖС Шымкент 2",
    "act_2310940436006402": "Teplo Almaty",
    "act_776865548258700": "Shanghai Tashkent",
    "act_1104357140269368": "Teplo Tashkent",
    "act_584782470655012": "TM Group",
    "act_353220323925035": "Zibak.tj",
    "act_508239018969999": "Фитнес Поинт",
    "act_1357165995492721": "Ария степи",
    "act_798205335840576": "Инвестиции",
}

# ===================== ТЕЛЕГРАМ =====================
TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"  # группа
ALMATY_TZ = timezone('Asia/Almaty')

# ===================== FX (курс USD→KZT) =====================
FX_CACHE_FILE = "fx_cache.json"
FX_CACHE_TTL = 60 * 60 * 12  # 12 часов
FX_BUMP_KZT = 5.0  # надбавка

def _load_fx_cache():
    try:
        with open(FX_CACHE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def _save_fx_cache(cache: dict):
    with open(FX_CACHE_FILE, "w") as f:
        json.dump(cache, f)

def get_usd_to_kzt():
    cache = _load_fx_cache()
    now = int(time.time())
    if "rate" in cache and "ts" in cache and now - cache["ts"] < FX_CACHE_TTL:
        return float(cache["rate"]) + FX_BUMP_KZT

    url = "https://api.apilayer.com/fixer/latest?base=USD&symbols=KZT"
    req = Request(url, headers={"apikey": "LYr6odX08iC6PXKqQSTT4QtKouCFcWeF"})
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            raw = float(data["rates"]["KZT"])
            _save_fx_cache({"rate": raw, "ts": now})
            return raw + FX_BUMP_KZT
    except Exception:
        # если апи недоступен — падаем на кеш, иначе дефолт
        if "rate" in cache:
            return float(cache["rate"]) + FX_BUMP_KZT
        return 500.0

def ceil_to_1000_kzt(v: float) -> int:
    return int(math.ceil(v / 1000.0) * 1000.0)

# ===================== УТИЛИТЫ =====================
account_statuses = {}

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except:
        # нет прав/ошибка — считаем как «нет доступа», в отчёт не шлём
        return None

def format_number(num):
    return f"{int(float(num)):,}".replace(",", " ")

# ===================== ОСНОВНОЙ ОТЧЁТ =====================
def get_facebook_data(account_id, date_preset, date_label=''):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}

    try:
        insights = account.get_insights(fields=fields, params=params)
        acc_info = account.api_get(fields=['name', 'account_status'])
        account_name = acc_info.get('name', ACCOUNT_NAMES.get(account_id, account_id))
        # если нет доступа — игнорируем вовсе
        if acc_info.get('account_status') is None:
            return None
    except Exception:
        return None  # игнорируем (403/нет прав/и т.п.)

    date_info = f" ({date_label})" if date_label else ""
    status_emoji = "🟢" if acc_info.get('account_status') == 1 else "🔴"
    report = f"{status_emoji} <b>{account_name}</b>{date_info}\n"

    if not insights:
        return report + "Нет данных за выбранный период"

    insight = insights[0]
    report += (
        f"👁 Показы: {format_number(insight.get('impressions', '0'))}\n"
        f"🎯 CPM: {round(float(insight.get('cpm', 0)), 2)} $\n"
        f"🖱 Клики: {format_number(insight.get('clicks', '0'))}\n"
        f"💸 CPC: {round(float(insight.get('cpc', 0)), 2)} $\n"
        f"💵 Затраты: {round(float(insight.get('spend', 0)), 2)} $"
    )

    actions = {a['action_type']: float(a['value']) for a in insight.get('actions', [])}

    # Переписки
    if account_id in MESSAGING_ACCOUNTS:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0)
        report += f"\n✉️ Начата переписка: {int(conv)}"
        if conv > 0:
            report += f"\n💬💲 Цена переписки: {round(float(insight.get('spend', 0)) / conv, 2)} $"

    # Лиды с сайта
    if account_id in LEAD_WEBSITE_ACCOUNTS:
        leads = (
            actions.get('offsite_conversion.fb_pixel_submit_application', 0) or
            actions.get('offsite_conversion.fb_pixel_lead', 0) or
            actions.get('lead', 0)
        )
        report += f"\n📩 Заявки: {int(leads)}"
        if leads > 0:
            report += f"\n📩💲 Цена заявки: {round(float(insight.get('spend', 0)) / leads, 2)} $"

    return report

async def send_report(context, chat_id, period, date_label=''):
    for acc in AD_ACCOUNTS:
        msg = get_facebook_data(acc, period, date_label)
        if msg:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

# ===================== БИЛЛИНГИ (неактивные, отдельными сообщениями) =====================
def fmt_int_spaces(n: int) -> str:
    return f"{n:,}".replace(",", " ")

def fmt_usd(usd: float) -> str:
    return f"{usd:.2f}"

async def _gather_inactive_billings():
    rate = get_usd_to_kzt()
    results = []
    for acc_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=['name', 'account_status', 'balance'])
            status = info.get('account_status')
            if status == 1:
                continue  # активные пропускаем
            name_api = info.get('name', ACCOUNT_NAMES.get(acc_id, acc_id))
            usd = float(info.get('balance', 0)) / 100.0
            kzt = ceil_to_1000_kzt(usd * rate)
            results.append((name_api, usd, kzt))
        except Exception:
            # нет прав/ошибка — игнорируем
            continue
    return results

async def send_billing_messages(context, chat_id):
    items = await _gather_inactive_billings()
    if not items:
        await context.bot.send_message(chat_id=chat_id, text="✅ Неактивных кабинетов с биллингами сейчас нет.", parse_mode='HTML')
        return
    for name_api, usd, kzt in items:
        text = (
            f"🔴 <b>{name_api}</b>\n"
            f"   💵 {fmt_usd(usd)} $  |  🇰🇿 {fmt_int_spaces(kzt)} ₸"
        )
        await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')

# ===================== ПРОГНОЗ БИЛЛИНГА (оставили как есть, с игнором ошибок) =====================
FORECAST_CACHE_FILE = "forecast_cache.json"
ACCOUNT_NAMES_FALLBACK = ACCOUNT_NAMES  # используем ту же мапу

async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(ALMATY_TZ).date()
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except:
        cache = {}

    for acc_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=["name", "spend_cap", "amount_spent", "account_status"])
            if info.get("account_status") is None:
                continue
            spend_cap = float(info.get("spend_cap", 0)) / 100
            spent = float(info.get("amount_spent", 0)) / 100
            available = spend_cap - spent
            daily_budget = sum(
                int(c.get("daily_budget", 0)) / 100
                for c in acc.get_campaigns(fields=["name", "effective_status", "daily_budget"])
                if c.get("effective_status") == "ACTIVE"
            )
            if daily_budget <= 0:
                continue
            days_left = ceil(available / daily_budget)
            billing_date = today + timedelta(days=days_left)
            if (billing_date - today).days == 3:
                if cache.get(acc_id) == billing_date.isoformat():
                    continue
                name = ACCOUNT_NAMES_FALLBACK.get(acc_id, info.get("name", acc_id))
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

    with open(FORECAST_CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ===================== БИЛЛИНГ-МОНТОР (аптайм) =====================
async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in AD_ACCOUNTS:
        try:
            account = AdAccount(account_id)
            info = account.api_get(fields=['name', 'account_status', 'balance'])
            status = info.get('account_status')
            # если раньше был активен (1), а теперь не 1 — уведомляем
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = info.get('name', ACCOUNT_NAMES.get(account_id, account_id))
                balance = float(info.get('balance', 0)) / 100
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"⚠️ ⚠️ ⚠️ Ахтунг! {name}! у нас биллинг - {balance:.2f} $",
                    parse_mode='HTML'
                )
            account_statuses[account_id] = status
        except Exception:
            # нет доступа — просто молчим
            continue

# ===================== КНОПКИ И КОМАНДЫ =====================
def make_main_keyboard_for_groups():
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("Сегодня", callback_data="today"),
        InlineKeyboardButton("Вчера", callback_data="yesterday"),
        InlineKeyboardButton("Прошлая неделя", callback_data="lastweek"),
    ], [
        InlineKeyboardButton("Биллинги", callback_data="billing")
    ]])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb = make_main_keyboard_for_groups()
    await update.message.reply_text("🤖 Выберите отчёт:", reply_markup=kb)

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    if data == "today":
        label = datetime.now().strftime('%d.%m.%Y')
        await send_report(context, chat_id, 'today', label)
    elif data == "yesterday":
        label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
        await send_report(context, chat_id, 'yesterday', label)
    elif data == "lastweek":
        until = datetime.now() - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await send_report(context, chat_id, period, label)
    elif data == "billing":
        await send_billing_messages(context, chat_id)

async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_billing_messages(context, update.effective_chat.id)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Доступные команды:\n"
        "/start — показать кнопки\n"
        "/help — помощь\n"
        "/billing — показать биллинги (неактивные аккаунты)\n\n"
        "Кнопки:\n"
        "• Сегодня\n"
        "• Вчера\n"
        "• Прошлая неделя\n"
        "• Биллинги\n"
    )
    await update.message.reply_text(text)

# Оставим текстовые команды на всякий случай (для лички)
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip().lower()
    if text in ('сегодня', 'today'):
        label = datetime.now().strftime('%d.%м.%Y'.replace('м', 'm'))  # чтобы не сломать форматирование
        label = datetime.now().strftime('%d.%m.%Y')
        await send_report(context, update.message.chat_id, 'today', label)
    elif text in ('вчера', 'yesterday'):
        label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
        await send_report(context, update.message.chat_id, 'yesterday', label)
    elif text in ('прошедшая неделя', 'last week', 'lastweek', 'прошлая неделя'):
        until = datetime.now() - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await send_report(context, update.message.chat_id, period, label)
    elif text in ('биллинги', 'billing'):
        await send_billing_messages(context, update.message.chat_id)

# ===================== ДЖОБЫ =====================
async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', label)
    await send_billing_messages(context, CHAT_ID)

# ===================== APP =====================
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Хендлеры
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("billing", cmd_billing))
    app.add_handler(CallbackQueryHandler(on_button))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    # Джобы
    app.job_queue.run_repeating(check_billing, interval=600, first=10)
    app.job_queue.run_daily(daily_report, time=dtime(hour=9, minute=30, tzinfo=ALMATY_TZ))
    app.job_queue.run_daily(check_billing_forecast, time=dtime(hour=9, minute=0, tzinfo=ALMATY_TZ))

    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
