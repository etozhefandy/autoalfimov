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

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "253181449"

# Аккаунты с нужными метриками
MESSAGING_ACCOUNTS = [
    "1415004142524014", "1108417930211002", "2342025859327675",
    "1333550570916716", "844229314275496", "1206987573792913",
    "195526110289107", "2145160982589338"
]

LEAD_ACCOUNTS = [
    "1042955424178074", "4030694587199998", "798205335840576"
]

AD_ACCOUNTS = list(set([
    "1206987573792913", "1415004142524014", "1333550570916716",
    "798205335840576", "844229314275496", "1108417930211002",
    "2342025859327675", "508239018969999", "1513759385846431",
    "1042955424178074", "4030694587199998", "195526110289107",
    "2145160982589338"
]))

account_statuses = {}

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except:
        return "🔴"

def format_number(num):
    return f"{int(float(num)):,}".replace(",", " ")

def get_action_value(actions, key):
    if not actions:
        return 0
    for action in actions:
        if action.get('action_type') == key:
            return float(action.get('value', 0))
    return 0

def get_facebook_data(account_id, date_preset, date_label=''):
    account = AdAccount(f"act_{account_id}")
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}

    try:
        insights = account.get_insights(fields=fields, params=params)
        account_name = account.api_get(fields=['name'])['name']
    except Exception as e:
        return f"⚠ Ошибка: {str(e)}"

    date_info = f" ({date_label})" if date_label else ""
    report = f"{is_account_active(account_id)} <b>{account_name}</b> (act_{account_id}){date_info}\n"

    if not insights:
        return report + "Нет данных за выбранный период"

    insight = insights[0]
    actions = insight.get('actions', [])

    report += (
        f"\U0001F441\uFE0F Показы: {format_number(insight.get('impressions', '0'))}\n"
        f"\U0001F3AF CPM: {round(float(insight.get('cpm', 0)), 2)} $\n"
        f"\U0001F5B1 Клики: {format_number(insight.get('clicks', '0'))}\n"
        f"\U0001F4B8 CPC: {round(float(insight.get('cpc', 0)), 2)} $\n"
        f"\U0001F4B5 Затраты: {round(float(insight.get('spend', 0)), 2)} $\n"
    )

    if account_id in MESSAGING_ACCOUNTS:
        conv = get_action_value(actions, 'onsite_conversion.messaging_conversation_started_7d')
        cost_conv = round(float(insight.get('spend', 0)) / conv, 2) if conv else 0
        report += f"\u2709\uFE0F Начата переписка: {int(conv)}\n"
        report += f"\U0001F4AC\U0001F4B2 Цена переписки: {cost_conv} $\n"

    if account_id in LEAD_ACCOUNTS:
        leads = get_action_value(actions, 'website_submit_application')
        cost_lead = round(float(insight.get('spend', 0)) / leads, 2) if leads else 0
        report += f"\U0001F4E9 Заявки: {int(leads)}\n"
        report += f"\U0001F4E9\U0001F4B2 Цена заявки: {cost_lead} $"

    return report

async def send_report(context, chat_id, period, date_label=''):
    for acc in AD_ACCOUNTS:
        msg = get_facebook_data(acc, period, date_label)
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in AD_ACCOUNTS:
        account = AdAccount(f"act_{account_id}")
        account_info = account.api_get(fields=['name', 'account_status', 'balance'])
        current_status = account_info.get('account_status')

        if account_id in account_statuses and account_statuses[account_id] == 1 and current_status != 1:
            account_name = account_info.get('name')
            balance = float(account_info.get('balance', 0)) / 100
            message = f"⚠️ ⚠️ ⚠️ Ахтунг! {account_name}! у нас биллинг - {balance:.2f} $"
            await context.bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='HTML')

        account_statuses[account_id] = current_status

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    date_label = (datetime.now(timezone('Asia/Almaty')) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', date_label)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text if update.message else ""
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

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling()
