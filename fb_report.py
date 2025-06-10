import asyncio
import re
from datetime import datetime, timedelta, time
from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

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
    "act_719853653795521"  # ЖС Караганда теперь с переписками
}

LEAD_FORM_ACCOUNTS = {
    "act_1042955424178074",
    "act_4030694587199998",
    "act_798205335840576"
}

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"

account_statuses = {}

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except:
        return "🔴"

def format_number(num):
    return f"{int(float(num)):,}".replace(",", " ")

def get_facebook_data(account_id, date_preset, date_label=''):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}

    try:
        insights = account.get_insights(fields=fields, params=params)
        account_name = account.api_get(fields=['name'])['name']
    except Exception as e:
        return f"⚠ Ошибка: {str(e)}"

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

    actions = {a['action_type']: float(a['value']) for a in insight.get('actions', [])}

    if account_id in MESSAGING_ACCOUNTS:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0)
        report += f"\n✉️ Начата переписка: {int(conv)}"
        if conv > 0:
            report += f"\n💬💲 Цена переписки: {round(float(insight.get('spend', 0)) / conv, 2)} $"

    if account_id in LEAD_FORM_ACCOUNTS:
        if account_id == 'act_4030694587199998':
            leads = actions.get('Website Submit Applications', 0)
        else:
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
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in AD_ACCOUNTS:
        try:
            account = AdAccount(account_id)
            account_info = account.api_get(fields=['name', 'account_status', 'balance'])
            current_status = account_info.get('account_status')

            if account_id in account_statuses and account_statuses[account_id] == 1 and current_status != 1:
                account_name = account_info.get('name')
                balance = float(account_info.get('balance', 0)) / 100
                message = f"⚠️ ⚠️ ⚠️ Ахтунг! {account_name}! у нас биллинг - {balance:.2f} $"
                await context.bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')

            account_statuses[account_id] = current_status
        except Exception as e:
            await context.bot.send_message(chat_id=CHAT_ID, text=f"⚠ Ошибка: {e}", parse_mode='HTML')

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    date_label = (datetime.now(timezone('Asia/Almaty')) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', date_label)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == 'Сегодня':
        date_label = datetime.now().strftime('%d.%m.%Y')
        await send_report(context, update.message.chat_id, 'today', date_label)
    elif text == 'Вчера':
        date_label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
        await send_report(context, update.message.chat_id, 'yesterday', date_label)
    elif text == 'Прошедшая неделя':
        until = datetime.now() - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
        date_label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await send_report(context, update.message.chat_id, period, date_label)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_keyboard = [['Сегодня', 'Вчера', 'Прошедшая неделя']]
    await update.message.reply_text('🤖 Выберите отчёт:', reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))

app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))


import json
import os
from math import ceil

def load_json(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, 'w') as f:
        json.dump(data, f)

async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    ad_accounts = load_json("ad_accounts.json")
    previous_forecasts = load_json("billing_forecast.json")
    today = datetime.now(timezone('Asia/Almaty')).date()
    updated_forecasts = {}

    for acc_id, acc_name in ad_accounts.items():
        try:
            acc = AdAccount(acc_id)
            acc_info = acc.api_get(fields=["spend_cap", "amount_spent", "name"])
            campaigns = acc.get_campaigns(fields=["name", "effective_status", "daily_budget"])
            
            spend_cap = float(acc_info.get("spend_cap", 0)) / 100
            amount_spent = float(acc_info.get("amount_spent", 0)) / 100
            available = max(spend_cap - amount_spent, 0)

            daily_budget = sum(
                float(c.get("daily_budget", 0)) / 100
                for c in campaigns
                if c.get("effective_status") == "ACTIVE"
            )

            if daily_budget == 0:
                continue

            days_left = ceil(available / daily_budget)
            forecast_date = today + timedelta(days=days_left)

            updated_forecasts[acc_id] = forecast_date.isoformat()

            if (acc_id not in previous_forecasts or
                previous_forecasts[acc_id] != forecast_date.isoformat()) and \
                (forecast_date - today).days == 3:

                text = (
                    f"⚠️ <b>{acc_name}</b>\n\n"
                    f"Предполагаемое списание: ${spend_cap:.2f}\n"
                    f"Дата: {forecast_date.strftime('%d.%m.%Y')}\n"
                    f"До порога осталось: ${available:.2f}\n"
                    f"Суммарный дневной бюджет: ${daily_budget:.2f}\n"
                    f"Осталось дней: {days_left}"
                )

                await context.bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")

        except Exception as e:
            await context.bot.send_message(chat_id=CHAT_ID, text=f"⚠ Ошибка прогноза {acc_name}: {e}")

    save_json("billing_forecast.json", updated_forecasts)


if __name__ == "__main__":
    print("\U0001F680 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
