import asyncio
import re
from datetime import datetime, timedelta, time
from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import json
from math import ceil

ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

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
    "act_1042955424178074",  # кенсе 1
    "act_4030694587199998",  # кенсе 2
    "act_508239018969999",   # фитнес поинт
    "act_1357165995492721",  # Ария степи
    "act_798205335840576"     # инвестиции
]

MESSAGING_ACCOUNTS = {
    "act_1415004142524014",
    "act_1108417930211002",
    "act_2342025859327675",
    "act_1333550570916716",
    "act_844229314275496",
    "act_1206987573792913",
    "act_195526110289107",
    "act_2145160982589338",
    "act_719853653795521"
}

LEAD_FORM_ACCOUNTS = {
    "act_1042955424178074",
    "act_4030694587199998",
    "act_798205335840576"
}

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"
account_statuses = {}

FORECAST_CACHE_FILE = "forecast_cache.json"

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
    "act_1042955424178074": "кенсе 1",
    "act_4030694587199998": "кенсе 2",
    "act_508239018969999": "Фитнес Поинт",
    "act_1357165995492721": "Ария Степи",
    "act_798205335840576": "Инвестиции"
}

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except:
        return "🔴"

def format_number(num):
    return f"{int(float(num)):,}".replace(",", " ")

def load_forecast_cache():
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_forecast_cache(cache):
    with open(FORECAST_CACHE_FILE, "w") as f:
        json.dump(cache, f)

def get_facebook_data(account_id, date_preset, date_label=''):
    ... # Сократим для читаемости, оставить как в твоём коде

async def send_report(context, chat_id, period, date_label=''):
    ... # как в твоём коде

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    ... # как в твоём коде

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    ... # как в твоём коде

async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    cache = load_forecast_cache()
    today = datetime.now(timezone("Asia/Almaty")).date()

    for acc_id in AD_ACCOUNTS:
        try:
            account = AdAccount(acc_id)
            data = account.api_get(fields=["name", "spend_cap", "amount_spent"])
            spend_cap = float(data.get("spend_cap", 0)) / 100
            amount_spent = float(data.get("amount_spent", 0)) / 100
            available_to_spend = spend_cap - amount_spent

            campaigns = account.get_campaigns(fields=["name", "effective_status", "daily_budget"])
            daily_budget = sum(
                int(c.get("daily_budget", 0)) / 100
                for c in campaigns
                if c["effective_status"] == "ACTIVE"
            )

            if daily_budget == 0:
                continue

            days_left = ceil(available_to_spend / daily_budget)
            billing_date = today + timedelta(days=days_left)

            if (billing_date - today).days == 3:
                cached_date_str = cache.get(acc_id)
                if cached_date_str == billing_date.isoformat():
                    continue

                acc_name = ACCOUNT_NAMES.get(acc_id, acc_id.replace("act_", ""))
                message = (
                    f"⚠️ <b>{acc_name}</b>\n\n"
                    f"Предполагаемое списание: <b>{spend_cap:.2f} $</b>\n"
                    f"Дата: <b>{billing_date.strftime('%d.%m.%Y')}</b>\n"
                    f"До порога осталось: <b>{available_to_spend:.2f} $</b>\n"
                    f"Суммарный дневной бюджет: <b>{daily_budget:.2f} $</b>\n"
                    f"Осталось дней: <b>{days_left}</b>"
                )
                await context.bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')
                cache[acc_id] = billing_date.isoformat()

        except Exception as e:
            print(f"Ошибка прогноза по {acc_id}: {e}")

    save_forecast_cache(cache)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ... # как в твоём коде

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_keyboard = [['Сегодня', 'Вчера', 'Прошедшая неделя']]
    await update.message.reply_text('🤖 Выберите отчёт:', reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))

app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))

if __name__ == "__main__":
    print("\U0001F680 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
