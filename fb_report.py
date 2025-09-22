import asyncio
import json
from math import ceil
from datetime import datetime, timedelta, time
import os

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
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"
FORECAST_CACHE_FILE = "forecast_cache.json"
TZ = timezone("Asia/Almaty")

# Если есть ENV USD_KZT_RATE (например 490) — возьмём, иначе 490.
BASE_USD_KZT = float(os.getenv("USD_KZT_RATE", "490"))
USD_KZT_WITH_PLUS5 = BASE_USD_KZT + 5.0

# ====== КАБИНЕТЫ ======
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
    "act_508239018969999",   # Фитнес Поинт
    "act_1357165995492721",  # Ария Степи
    "act_798205335840576",   # Инвестиции
    "act_2310940436006402",  # Тепло Алматы (без доп метрик)
    "act_776865548258700",   # Шанхай Ташкент (без доп метрик)
    "act_1104357140269368",  # Тепло Ташкент (без доп метрик)
    # Новые:
    "act_584782470655012",   # TM Group (стандарт + переписки + заявки сайта)
    "act_353220323925035",   # Zibak.tj (стандарт + переписки)
    # ВНИМАНИЕ: по просьбе — аккаунты Kense удалены ранее из списка, тут их нет
]

# где показывать "переписки"
MESSAGING_ACCOUNTS = {
    "act_1415004142524014",
    "act_1108417930211002",
    "act_2342025859327675",
    "act_1333550570916716",
    "act_844229314275496",
    "act_1206987573792913",
    "act_195526110289107",
    "act_2145160982589338",
    "act_2183299115451405",
    "act_719853653795521",
    "act_353220323925035",   # Zibak.tj
    "act_584782470655012",   # TM Group
}

# где показывать "заявки сайта"
LEAD_FORM_ACCOUNTS = {
    "act_798205335840576",   # Инвестиции (оставляем)
    "act_584782470655012",   # TM Group — нужны заявки сайта
    # (Кенсе аккаунты исключены; остальные по запросу можно добавить)
}

# Читабельные названия
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
    "act_2310940436006402": "Тепло Алматы",
    "act_776865548258700":  "Шанхай Ташкент",
    "act_1104357140269368": "Тепло Ташкент",
    "act_584782470655012":  "TM Group",
    "act_353220323925035":  "Zibak.tj",
}

# ====== ВНУТРЕННЕЕ ======
account_statuses = {}

def format_number(num) -> str:
    try:
        return f"{int(float(num)):,}".replace(",", " ")
    except:
        return "0"

def is_account_active(account_id) -> str:
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except:
        # если доступа нет — просто считаем неактивным, и пропустим дальше
        return "🔴"

def _fetch_insights(account_id, fields, params):
    """Безопасный запрос инсайтов: возвращает (insights, account_name) или (None, name) при ошибке."""
    try:
        account = AdAccount(account_id)
        insights = account.get_insights(fields=fields, params=params)
        name = account.api_get(fields=['name']).get('name', account_id)
        return insights, name
    except Exception:
        return None, ACCOUNT_NAMES.get(account_id, account_id)

def _actions_to_dict(insight_row):
    """Преобразует список actions в словарь {action_type: float(value)}"""
    out = {}
    for a in insight_row.get('actions', []) or []:
        at = a.get('action_type')
        val = a.get('value')
        if at and val is not None:
            try:
                out[at] = float(val)
            except:
                pass
    return out

def get_facebook_data(account_id, date_preset, date_label=''):
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}

    insights, account_name = _fetch_insights(account_id, fields, params)
    date_info = f" ({date_label})" if date_label else ""
    header = f"{is_account_active(account_id)} <b>{account_name}</b>{date_info}\n"

    if not insights:
        # нет доступа или ошибка — молча пропускаем (ничего не отправляем)
        return None

    row = insights[0] if insights else {}
    report = (
        f"👁 Показы: {format_number(row.get('impressions', '0'))}\n"
        f"🎯 CPM: {round(float(row.get('cpm', 0) or 0), 2)} $\n"
        f"🖱 Клики: {format_number(row.get('clicks', '0'))}\n"
        f"💸 CPC: {round(float(row.get('cpc', 0) or 0), 2)} $\n"
        f"💵 Затраты: {round(float(row.get('spend', 0) or 0), 2)} $"
    )

    actions = _actions_to_dict(row)

    # Переписки
    if account_id in MESSAGING_ACCOUNTS:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0.0)
        report += f"\n✉️ Начата переписка: {int(conv)}"
        if conv > 0:
            spend = float(row.get('spend', 0) or 0)
            report += f"\n💬💲 Цена переписки: {round(spend / conv, 2)} $"

    # Заявки с сайта
    if account_id in LEAD_FORM_ACCOUNTS:
        # универсальная логика: сначала submit_application, затем lead/прочее
        leads = (
            actions.get('offsite_conversion.fb_pixel_submit_application', 0.0)
            or actions.get('offsite_conversion.fb_pixel_lead', 0.0)
            or actions.get('lead', 0.0)
        )
        report += f"\n📩 Заявки: {int(leads)}"
        if leads > 0:
            spend = float(row.get('spend', 0) or 0)
            report += f"\n📩💲 Цена заявки: {round(spend / leads, 2)} $"

    return header + report

async def send_report(context, chat_id, period, date_label=''):
    """Отправляет отчёты по всем кабинетам + затем список биллингов."""
    for acc in AD_ACCOUNTS:
        msg = get_facebook_data(acc, period, date_label)
        if msg:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
    # добиваем сводкой по биллингам
    billing_text = await build_billing_summary()
    if billing_text:
        await context.bot.send_message(chat_id=chat_id, text=billing_text, parse_mode='HTML')

async def build_billing_summary() -> str:
    """Формирует текст списка биллингов в $ и ₸ (по курсу +5₸).
       Аккаунты без доступа — молча пропускаем.
    """
    lines = ["<b>📋 Список биллингов</b>"]
    had_any = False
    for acc_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=['name', 'balance'])
            name = info.get('name', ACCOUNT_NAMES.get(acc_id, acc_id))
            balance_cents = info.get('balance', 0) or 0
            balance_usd = float(balance_cents) / 100.0
            balance_kzt = int(round(balance_usd * USD_KZT_WITH_PLUS5, 0))
            lines.append(f"• {name} — <b>{balance_usd:.2f} $</b> ≈ <b>{balance_kzt:,} ₸</b>".replace(",", " "))
            had_any = True
        except Exception:
            # нет прав / удалён из партнёров — просто пропускаем
            continue
    return "\n".join(lines) if had_any else ""

# ====== БИЛЛИНГ-ПРОГНОЗ ======
async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(TZ).date()
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except:
        cache = {}

    changed = False
    for acc_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=["name", "spend_cap", "amount_spent"])
            spend_cap = float(info.get("spend_cap", 0) or 0) / 100.0
            spent = float(info.get("amount_spent", 0) or 0) / 100.0
            available = spend_cap - spent

            # суммарный дневной бюджет активных кампаний
            daily_budget = 0.0
            for c in acc.get_campaigns(fields=["name", "effective_status", "daily_budget"]):
                if c.get("effective_status") == "ACTIVE":
                    daily_budget += (int(c.get("daily_budget", 0) or 0) / 100.0)

            if daily_budget <= 0:
                continue

            days_left = ceil(available / daily_budget) if available > 0 else 0
            billing_date = today + timedelta(days=days_left)

            if (billing_date - today).days == 3:
                if cache.get(acc_id) != billing_date.isoformat():
                    name = ACCOUNT_NAMES.get(acc_id, acc_id)
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
                    changed = True

        except Exception as e:
            # тихо пропускаем проблемы доступа
            continue

    if changed:
        with open(FORECAST_CACHE_FILE, "w") as f:
            json.dump(cache, f)

# ====== ХЕНДЛЕРЫ КОМАНД/КНОПОК ======
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "<b>Доступные команды:</b>\n"
        "/help — список команд\n"
        "/today — отчёт за сегодня\n"
        "/yesterday — отчёт за вчера\n"
        "/week — отчёт за прошедшую неделю\n"
        "/billing — список биллингов ($ и ₸, курс +5₸)\n\n"
        "В личке также работают кнопки: «Сегодня», «Вчера», «Прошедшая неделя»."
    )
    await update.effective_chat.send_message(text, parse_mode="HTML")

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
    billing_text = await build_billing_summary()
    if not billing_text:
        billing_text = "Нет доступных данных по биллингам (возможно, нет прав к аккаунтам)."
    await update.effective_chat.send_message(billing_text, parse_mode="HTML")

# старые текстовые кнопки для лички
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    if text == 'Сегодня':
        await cmd_today(update, context)
    elif text == 'Вчера':
        await cmd_yesterday(update, context)
    elif text == 'Прошедшая неделя':
        await cmd_week(update, context)

# /start — отправим inline-кнопки (работают в группах) + обычные в личке
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    kb_inline = InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data="today"),
         InlineKeyboardButton("Вчера", callback_data="yesterday"),
         InlineKeyboardButton("Неделя", callback_data="week")],
        [InlineKeyboardButton("📋 Список биллингов", callback_data="billing")]
    ])
    await update.effective_chat.send_message(
        "🤖 Выберите действие:",
        reply_markup=kb_inline
    )
    # Для лички — добавим реплай-клавиатуру (если надо)
    if update.effective_chat.type == "private":
        reply_kb = ReplyKeyboardMarkup([['Сегодня', 'Вчера', 'Прошедшая неделя']], resize_keyboard=True)
        await update.effective_chat.send_message("Или используйте клавиатуру ниже:", reply_markup=reply_kb)

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    if q.data == "today":
        await cmd_today(update, context)
    elif q.data == "yesterday":
        await cmd_yesterday(update, context)
    elif q.data == "week":
        await cmd_week(update, context)
    elif q.data == "billing":
        await cmd_billing(update, context)

# ====== РЕГУЛАРНЫЕ ДЖОБЫ ======
async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    """Мониторинг статуса кабинетов раз в 10 минут.
       Нет доступа → молча пропускаем.
    """
    for account_id in AD_ACCOUNTS:
        try:
            account = AdAccount(account_id)
            info = account.api_get(fields=['name', 'account_status', 'balance'])
            status = info.get('account_status')
            # если был активен и стал неактивен — шлём предупреждение
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = info.get('name', ACCOUNT_NAMES.get(account_id, account_id))
                balance = float(info.get('balance', 0) or 0) / 100
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"⚠️ ⚠️ ⚠️ Ахтунг! {name}! у нас биллинг - {balance:.2f} $",
                    parse_mode='HTML'
                )
            account_statuses[account_id] = status
        except Exception:
            # нет права — просто пропустили, ничего не шлём
            continue

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(TZ) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', label)

# ====== ЗАПУСК ======
app = Application.builder().token(TELEGRAM_TOKEN).build()

# команды
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", cmd_help))
app.add_handler(CommandHandler("today", cmd_today))
app.add_handler(CommandHandler("yesterday", cmd_yesterday))
app.add_handler(CommandHandler("week", cmd_week))
app.add_handler(CommandHandler("billing", cmd_billing))

# inline callbacks
app.add_handler(CallbackQueryHandler(on_callback))

# текстовые кнопки (личка)
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

# джобы
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=TZ))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=TZ))

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
