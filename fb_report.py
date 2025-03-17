import re
import asyncio
from datetime import datetime, timedelta
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

AD_ACCOUNTS = [
    "act_1206987573792913", "act_1415004142524014", "act_1333550570916716",
    "act_798205335840576", "act_844229314275496", "act_1108417930211002",
    "act_2342025859327675", "act_508239018969999", "act_1513759385846431",
    "act_1042955424178074"
]

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "253181449"

previous_balances = {}


def clean_text(text):
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', str(text))


def is_account_active(account_id):
    try:
        account_data = AdAccount(account_id).api_get(fields=['account_status'])
        return "🟢" if account_data['account_status'] == 1 else "🔴"
    except Exception:
        return "🔴"


def get_facebook_data(account_id, date_preset):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'actions', 'cost_per_action_type', 'spend']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}

    try:
        insights = account.get_insights(fields=fields, params=params)
        account_name = account.api_get(fields=['name'])['name']
    except Exception as e:
        return f"⚠ Ошибка: {clean_text(str(e))}"

    report = f"*{clean_text(account_name)}* {is_account_active(account_id)}\n"

    if not insights:
        return report + "_Нет данных за выбранный период_"

    insight = insights[0]
    report += (
        f"👁 Показы: {clean_text(insight.get('impressions', '0'))}\n"
        f"🎯 CPM: {clean_text(round(float(insight.get('cpm', 0)), 2))} USD\n"
        f"🖱 Клики: {clean_text(insight.get('clicks', '0'))}\n"
        f"💸 CPC: {clean_text(round(float(insight.get('cpc', 0)), 2))} USD\n"
        f"💵 Затраты: {clean_text(round(float(insight.get('spend', 0)), 2))} USD"
    )
    return report


async def send_report(context, chat_id, period):
    for acc in AD_ACCOUNTS:
        msg = get_facebook_data(acc, period)
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='MarkdownV2')


async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global previous_balances
    for account_id in AD_ACCOUNTS:
        account = AdAccount(account_id)
        billing_info = account.api_get(fields=['name', 'balance'])
        current_balance = billing_info.get('balance', '0')

        if account_id in previous_balances and previous_balances[account_id] != current_balance:
            diff = float(current_balance) - float(previous_balances[account_id])
            message = (
                f"💳 Изменение биллинга: *{clean_text(billing_info.get('name', 'Неизвестный'))}*\n"
                f"💰 Было: {previous_balances[account_id]} USD\n"
                f"💸 Стало: {current_balance} USD\n"
                f"🔔 Изменение: {round(diff, 2)} USD"
            )
            await context.bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='MarkdownV2')

        previous_balances[account_id] = current_balance


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == 'Сегодня':
        await send_report(context, update.message.chat_id, 'today')
    elif text == 'Вчера':
        await send_report(context, update.message.chat_id, 'yesterday')
    elif text == 'Прошедшая неделя':
        until = datetime.now() - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
        await send_report(context, update.message.chat_id, period)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reply_keyboard = [['Сегодня', 'Вчера', 'Прошедшая неделя']]
    await update.message.reply_text('🤖 Выберите отчёт:', reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True))


app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
app.job_queue.run_repeating(check_billing, interval=600, first=10)

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling()
