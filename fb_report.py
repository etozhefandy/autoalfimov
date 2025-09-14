import asyncio
import json
import re
from math import ceil
from datetime import datetime, timedelta, time
from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ========= CONFIG =========
ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGз8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# Порядок отчёта и актуальный список аккаунтов (КЕНСЕ удалены!)
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
    "act_2310940436006402",  # Тепло Алматы
    "act_1104357140269368",  # Тепло Ташкент
    "act_776865548258700",   # Шанхай Ташкент
]

# Аккаунты, для которых считаем переписки
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
    "act_2183299115451405",  # добавили Шымкент 2
}

# Аккаунты, для которых считаем «заявки с сайта»
# (КЕНСЕ удалены из списка)
LEAD_FORM_ACCOUNTS = {
    "act_798205335840576",  # Инвестиции (оставили как было)
    # если потребуется — добавим сюда другие аккаунты
}

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"
FORECAST_CACHE_FILE = "forecast_cache.json"

# Человеко-читаемые имена
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
    "act_508239018969999": "Фитнес Поинт",
    "act_1357165995492721": "Ария Степи",
    "act_798205335840576": "Инвестиции",
    "act_2310940436006402": "Тепло Алматы",
    "act_1104357140269368": "Тепло Ташкент",
    "act_776865548258700":  "Шанхай Ташкент",
}

account_statuses = {}

# ========= HELPERS =========
def is_perm_error(exc: Exception) -> bool:
    """Возвращает True, если ошибка — отсутствие прав/403 (#200)."""
    s = str(exc)
    return "(#200)" in s or "code\": 200" in s or "Status:  403" in s

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except Exception:
        # Если нет прав — считаем как недоступный, не шумим
        return "🔴"

def format_number(num):
    return f"{int(float(num)):,}".replace(",", " ")

# ========= CORE =========
def get_facebook_data(account_id, date_preset, date_label=''):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}

    try:
        insights = account.get_insights(fields=fields, params=params)
        account_name = ACCOUNT_NAMES.get(account_id, account.api_get(fields=['name'])['name'])
    except Exception as e:
        # Если нет прав на аккаунт — игнорируем (ничего не отправляем)
        if is_perm_error(e):
            return None
        # Иначе вернём одну строку-ошибку, чтобы понимать контекст
        return f"⚠ Ошибка по {ACCOUNT_NAMES.get(account_id, account_id)}:\n{e}"

    date_info = f" ({date_label})" if date_label else ""
    report = f"{is_account_active(account_id)} <b>{account_name}</b>{date_info}\n"

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

    # Доп. метрики по действиям (если доступны)
    actions = {a['action_type']: float(a['value']) for a in insight.get('actions', [])}

    # Переписки
    if account_id in MESSAGING_ACCOUNTS:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0)
        report += f"\n✉️ Начата переписка: {int(conv)}"
        if conv > 0:
            spend = float(insight.get('spend', 0))
            report += f"\n💬💲 Цена переписки: {round(spend / conv, 2)} $"

    # Заявки с сайта (оставили только для выбранных)
    if account_id in LEAD_FORM_ACCOUNTS:
        leads = (
            actions.get('offsite_conversion.fb_pixel_submit_application', 0) or
            actions.get('offsite_conversion.fb_pixel_lead', 0) or
            actions.get('lead', 0)
        )
        report += f"\n📩 Заявки: {int(leads)}"
        if leads > 0:
            spend = float(insight.get('spend', 0))
            report += f"\n📩💲 Цена заявки: {round(spend / leads, 2)} $"

    return report

async def send_report(context, chat_id, period, date_label=''):
    for acc in AD_ACCOUNTS:
        msg = get_facebook_data(acc, period, date_label)
        if msg:  # пропускаем аккаунты без прав/403
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
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
            # Тихо игнорим аккаунты без прав
            if is_perm_error(e):
                continue
            await context.bot.send_message(chat_id=CHAT_ID, text=f"⚠ Ошибка: {e}", parse_mode='HTML')

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(timezone('Asia/Almaty')) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', label)

# ======== ПРОГНОЗ БИЛЛИНГА ========
async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(timezone("Asia/Almaty")).date()
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except Exception:
        cache = {}

    for acc_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=["name", "spend_cap", "amount_spent"])
            spend_cap = float(info.get("spend_cap", 0)) / 100
            spent = float(info.get("amount_spent", 0)) / 100
            available = spend_cap - spent

            # Сумма дневных бюджетов активных кампаний
            daily_budget = sum(
                int(c.get("daily_budget", 0)) / 100
                for c in acc.get_campaigns(fields=["name", "effective_status", "daily_budget"])
                if c.get("effective_status") == "ACTIVE"
            )
            if daily_budget <= 0:
                continue

            days_left = ceil(available / daily_budget) if available > 0 else 0
            billing_date = today + timedelta(days=days_left)

            if (billing_date - today).days == 3:
                if cache.get(acc_id) == billing_date.isoformat():
                    continue
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

        except Exception as e:
            # Тихо игнорируем аккаунты без прав
            if is_perm_error(e):
                continue
            print(f"Ошибка прогноза по {acc_id}: {e}")

    with open(FORECAST_CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ======== TELEGRAM ========
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # защита от апдейтов без текста (стикеры/кнопки и т.д.)
    if not update.message or not update.message.text:
        return

    text = update.message.text
    if text == 'Сегодня':
        label = datetime.now().strftime('%d.%m.%Y')
        await send_report(context, update.message.chat_id, 'today', label)
    elif text == 'Вчера':
        label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
        await send_report(context, update.message.chat_id, 'yesterday', label)
    elif text == 'Прошедшая неделя':
        until = datetime.now() - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await send_report(context, update.message.chat_id, period, label)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [['Сегодня', 'Вчера', 'Прошедшая неделя']]
    await update.message.reply_text('🤖 Выберите отчёт:', reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

# ======== APP ========
app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

# фоновые задачи
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))

if __name__ == "__main__":
    print("\U0001F680 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
