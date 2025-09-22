# fb_report.py
import asyncio
import json
import os
from math import ceil
from datetime import datetime, timedelta, time

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
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes

# ====== НАСТРОЙКИ ======
ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"  # группа
FORECAST_CACHE_FILE = "forecast_cache.json"
FX_CACHE_FILE = "FX_CACHE.json"

FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# ====== СПИСОК АККАУНТОВ (ПОРЯДОК ОТЧЁТА) ======
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
    # --- дальше не ЖС ---
    "act_2310940436006402",  # Teplo Almaty (как ЖС)
    "act_1104357140269368",  # Teplo Tashkent (как ЖС)
    "act_776865548258700",   # Shanghai (как ЖС)
    "act_584782470655012",   # TM Group (ЖС + лиды сайта)
    "act_353220323925035",   # Zirbak RA (как ЖС)
    "act_508239018969999",   # Фитнес Поинт (оставляем)
    "act_1357165995492721",  # Ария степи
    "act_798205335840576",   # Инвестиции
]

# “Как в ЖС” = стандартные + начатые переписки и их стоимость
MESSAGING_ACCOUNTS = {
    "act_1415004142524014",  # ЖС Астана
    "act_719853653795521",   # ЖС Караганда
    "act_1206987573792913",  # ЖС Павлодар
    "act_1108417930211002",  # ЖС Актау
    "act_2342025859327675",  # ЖС Атырау
    "act_844229314275496",   # ЖС Актобе
    "act_1333550570916716",  # ЖС Юг (Алматы)
    "act_195526110289107",   # ЖС Тараз
    "act_2145160982589338",  # ЖС Шымкент
    "act_2183299115451405",  # ЖС Шымкент 2
    # похожие на ЖС:
    "act_2310940436006402",  # Teplo Almaty
    "act_1104357140269368",  # Teplo Tashkent
    "act_776865548258700",   # Shanghai
    "act_353220323925035",   # Zirbak RA
    "act_584782470655012",   # TM Group (и переписки тоже)
}

# Лиды сайта + цена лида (добавочно для TM Group, и там, где раньше было)
LEAD_FORM_ACCOUNTS = {
    "act_584782470655012",   # TM Group
    # если нужно ещё где-то включить лиды сайта — добавляем сюда act_...
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
    "act_1104357140269368": "Teplo Tashkent",
    "act_776865548258700":  "Shanghai",
    "act_584782470655012":  "TM Group",
    "act_353220323925035":  "Zirbak RA",
    "act_508239018969999":  "Фитнес Поинт",
    "act_1357165995492721": "Ария Степи",
    "act_798205335840576":  "Инвестиции",
}

# ====== ГЛОБАЛЬНЫЕ ======
account_statuses = {}

# ====== УТИЛИТЫ ======
def format_number(num) -> str:
    try:
        return f"{int(float(num)):,}".replace(",", " ")
    except:
        return str(num)

def _exc_is_permission(e: Exception) -> bool:
    # мягко определяем 403/прав на аккаунт нет — по тексту
    s = str(e).lower()
    return "(#200)" in s or "not grant ads_management" in s or "permissions" in s

def _get_usd_kzt() -> float:
    """
    Курс USD→KZT:
      1) если задан FX_KZT – используем его
      2) если есть свежий кэш (<=6ч) – берём из кэша
      3) пробуем API exchangerate.host
      4) fallback = 495.0
    """
    # 1) ручной override
    env_rate = os.getenv("FX_KZT")
    if env_rate:
        try:
            return float(env_rate)
        except:
            pass

    # 2) кэш
    try:
        with open(FX_CACHE_FILE, "r") as f:
            cached = json.load(f)
        ts = datetime.fromisoformat(cached["ts"])
        if datetime.utcnow() - ts <= timedelta(hours=6):
            return float(cached["rate"])
    except:
        pass

    # 3) API
    rate = 495.0  # fallback
    try:
        r = requests.get("https://api.exchangerate.host/latest", params={"base": "USD", "symbols": "KZT"}, timeout=8)
        if r.ok:
            data = r.json()
            rate = float(data["rates"]["KZT"])
    except:
        pass

    # сохранить кэш
    try:
        with open(FX_CACHE_FILE, "w") as f:
            json.dump({"rate": rate, "ts": datetime.utcnow().isoformat()}, f)
    except:
        pass
    return rate

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except Exception as e:
        if _exc_is_permission(e):
            return "⚪"  # нет доступа — игнор
        return "🔴"

# ====== ОСНОВНЫЕ ФУНКЦИИ ======
def get_facebook_data(account_id, date_preset, date_label=''):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}

    try:
        insights = account.get_insights(fields=fields, params=params)
        account_name = ACCOUNT_NAMES.get(account_id, account.api_get(fields=['name'])['name'])
    except Exception as e:
        if _exc_is_permission(e):
            return ""  # нет прав — молча пропускаем
        return f"⚠ Ошибка: {str(e)}"

    if not insights:
        date_info = f" ({date_label})" if date_label else ""
        return f"{is_account_active(account_id)} <b>{account_name}</b>{date_info}\nНет данных за выбранный период"

    insight = insights[0]
    date_info = f" ({date_label})" if date_label else ""
    report = f"{is_account_active(account_id)} <b>{account_name}</b>{date_info}\n"
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

    # Лиды сайта (для TM Group и др., если добавим)
    if account_id in LEAD_FORM_ACCOUNTS:
        leads = (
            actions.get('offsite_conversion.fb_pixel_submit_application', 0)
            or actions.get('offsite_conversion.fb_pixel_lead', 0)
            or actions.get('lead', 0)
        )
        report += f"\n📩 Заявки: {int(leads)}"
        if leads > 0:
            report += f"\n📩💲 Цена заявки: {round(float(insight.get('spend', 0)) / leads, 2)} $"

    return report

async def send_report(context, chat_id, period, date_label=''):
    for acc in AD_ACCOUNTS:
        msg = get_facebook_data(acc, period, date_label)
        if msg:  # пустые (нет доступа) не отправляем
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    # предупреждения, если статус упал (и есть доступ)
    global account_statuses
    for account_id in AD_ACCOUNTS:
        try:
            account = AdAccount(account_id)
            info = account.api_get(fields=['name', 'account_status', 'balance'])
            status = info.get('account_status')
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = ACCOUNT_NAMES.get(account_id, info.get('name'))
                balance = float(info.get('balance', 0)) / 100
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"⚠️ ⚠️ ⚠️ Ахтунг! {name}! у нас биллинг - {balance:.2f} $",
                    parse_mode='HTML'
                )
            account_statuses[account_id] = status
        except Exception as e:
            if _exc_is_permission(e):
                continue
            # тихая ошибка — не спамим

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(timezone('Asia/Almaty')) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', label)
    # после отчёта — список неактивных биллингов
    await cmd_billing_common(context, CHAT_ID)

# ====== БИЛЛИНГ-СПИСОК (ТОЛЬКО НЕАКТИВНЫЕ) ======
async def cmd_billing_common(context: ContextTypes.DEFAULT_TYPE, chat_id: str | int):
    rate = _get_usd_kzt()
    lines = []
    for account_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(account_id).api_get(fields=['name', 'account_status', 'balance'])
            status = acc.get('account_status')
            if status == 1:
                continue  # показываем только НЕактивные
            name = ACCOUNT_NAMES.get(account_id, acc.get('name', account_id))
            usd = float(acc.get('balance', 0)) / 100
            kzt = round(usd * rate)
            lines.append(f"🔴 <b>{name}</b>\n   💵 {usd:.2f} $  |  🇰🇿 {kzt:,} ₸ (1$ = {rate:.2f} ₸)")
        except Exception as e:
            if _exc_is_permission(e):
                continue
            # другие ошибки — пропускаем
            continue

    if not lines:
        text = "✅ Все кабинеты активны или недоступны для проверки."
    else:
        text = "Сейчас в биллингах (неактивные):\n\n" + "\n\n".join(lines)

    await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')

# ====== ПРОГНОЗ БИЛЛИНГА (оставляем, как было) ======
async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(timezone("Asia/Almaty")).date()
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except:
        cache = {}

    for acc_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=["name", "spend_cap", "amount_spent"])
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
                name = ACCOUNT_NAMES.get(acc_id, info.get("name", acc_id))
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
        except Exception as e:
            if _exc_is_permission(e):
                continue
            # прочие ошибки — тихо пропускаем

    with open(FORECAST_CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ====== ХЕНДЛЕРЫ ======
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # В группах — inline-кнопки под сообщением
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Сегодня", callback_data="today"),
             InlineKeyboardButton("Вчера", callback_data="yesterday")],
            [InlineKeyboardButton("Прошедшая неделя", callback_data="lastweek")],
            [InlineKeyboardButton("Список биллингов", callback_data="billing")],
        ])
        await update.message.reply_text("🤖 Выберите отчёт:", reply_markup=kb)
    else:
        # в личке — обычные кнопки
        keyboard = [['Сегодня', 'Вчера', 'Прошедшая неделя', 'Список биллингов']]
        await update.message.reply_text('🤖 Выберите отчёт:', reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Доступные команды:\n"
        "/start — показать кнопки\n"
        "/help — список команд\n"
        "/billing — список неактивных кабинетов с суммами ($ и ₸)\n"
        "/today — отчёт за сегодня\n"
        "/yesterday — отчёт за вчера\n"
        "/lastweek — отчёт за прошедшую неделю\n"
    )
    await update.message.reply_text(text)

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    label = datetime.now().strftime('%d.%m.%Y')
    await send_report(context, update.effective_chat.id, 'today', label)

async def cmd_yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, update.effective_chat.id, 'yesterday', label)

async def cmd_lastweek(update: Update, context: ContextTypes.DEFAULT_TYPE):
    until = datetime.now() - timedelta(days=1)
    since = until - timedelta(days=6)
    period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
    label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
    await send_report(context, update.effective_chat.id, period, label)

async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_billing_common(context, update.effective_chat.id)

async def text_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip().lower()
    if text == 'сегодня':
        await cmd_today(update, context)
    elif text == 'вчера':
        await cmd_yesterday(update, context)
    elif text == 'прошедшая неделя':
        await cmd_lastweek(update, context)
    elif text == 'список биллингов':
        await cmd_billing(update, context)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    data = q.data
    dummy_update = Update(update.update_id, message=q.message)
    if data == "today":
        await cmd_today(dummy_update, context)
    elif data == "yesterday":
        await cmd_yesterday(dummy_update, context)
    elif data == "lastweek":
        await cmd_lastweek(dummy_update, context)
    elif data == "billing":
        await cmd_billing_common(context, q.message.chat.id)

# ====== APP ======
app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", help_cmd))
app.add_handler(CommandHandler("billing", cmd_billing))
app.add_handler(CommandHandler("today", cmd_today))
app.add_handler(CommandHandler("yesterday", cmd_yesterday))
app.add_handler(CommandHandler("lastweek", cmd_lastweek))
app.add_handler(CallbackQueryHandler(on_callback))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_buttons))

# фоновые задачи
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
