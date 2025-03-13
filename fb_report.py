import asyncio
import re
import os
from datetime import datetime
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from telegram import Bot, Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

ACCESS_TOKEN = os.getenv("EAASZCrBwhoH0BO6mUkgfM9oeDIas5gzGVKvJCl2QSFkMzMJyYK9mesXEHhFR1yPQ68A4UL54PUr5aD8iWHQSBd31CSIZCBCU5hslguZCUnhmBbbXdZCM6mLRXZAMwydyxvAQK2A72K1fvL96Mf0TEzYkjfl2z0LOysnQW8Mo6650eoUZCsQej6xvjc0ZBqZBUUR4VwZDZD")
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN, api_version='v22.0')

AD_ACCOUNTS = [
    "act_1206987573792913", "act_1415004142524014", "act_1333550570916716",
    "act_798205335840576", "act_844229314275496", "act_1108417930211002",
    "act_2342025859327675", "act_508239018969999", "act_1513759385846431",
    "act_1042955424178074"
]

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "253181449"
ALLOWED_ACTIONS = {"link_click"}

reply_keyboard = [['Сегодня', 'Вчера', 'Неделя']]
markup = ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)

def clean_text(text):
    if not isinstance(text, str):
        text = str(text)
    return re.sub(r'([*\[\]()~`>#+|{}!])', '', text)

def is_account_active(account_id):
    try:
        account_data = AdAccount(account_id).api_get(fields=['account_status'])
        return "🟢" if account_data['account_status'] == 1 else "🔴"
    except Exception:
        return "🔴"

def get_facebook_data(account_id, date_preset):
    account = AdAccount(account_id)
    fields = ['impressions', 'clicks', 'cost_per_action_type', 'spend']
    params = {'date_preset': date_preset, 'level': 'account'}

    try:
        campaigns = account.get_insights(fields=fields, params=params)
    except Exception as e:
        return f"⚠ Ошибка загрузки данных для {account_id}: {clean_text(str(e))}"

    try:
        account_name = account.api_get(fields=['name'])['name']
    except Exception:
        account_name = "Неизвестный аккаунт"

    status_emoji = is_account_active(account_id)
    date_str = datetime.now().strftime("%Y-%m-%d")
    report = f"{status_emoji} {clean_text(account_name)} ({date_str})\n"

    if not campaigns:
        report += "\n⚠ Данных за выбранный период нет"
    else:
        campaign = campaigns[0]
        report += f"\n👁️ Показы: {clean_text(campaign.get('impressions', '—'))}"
        report += f"\n🖱️ Клики: {clean_text(campaign.get('clicks', '—'))}"

        if 'cost_per_action_type' in campaign:
            for cost in campaign['cost_per_action_type']:
                if cost.get('action_type') == "link_click":
                    report += f"\n💰 Стоимость клика: {clean_text(str(round(float(cost['value']), 2)))} $"

        spend = campaign.get('spend', 0)
        report += f"\n💵 Сумма затрат: {clean_text(str(round(float(spend), 2)))} $"

    return report

async def send_to_telegram_message(context: ContextTypes.DEFAULT_TYPE, chat_id, message):
    await context.bot.send_message(chat_id=chat_id, text=message)

async def today_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Собираю данные за сегодня...")
    for account_id in AD_ACCOUNTS:
        report = get_facebook_data(account_id, 'today')
        await send_to_telegram_message(context, update.effective_chat.id, report)

async def yesterday_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Собираю данные за вчера...")
    for account_id in AD_ACCOUNTS:
        report = get_facebook_data(account_id, 'yesterday')
        await send_to_telegram_message(context, update.effective_chat.id, report)

async def week_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Собираю данные за последнюю неделю...")
    for account_id in AD_ACCOUNTS:
        report = get_facebook_data(account_id, 'last_7d')
        await send_to_telegram_message(context, update.effective_chat.id, report)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 Бот активен! Используй кнопки для получения отчета:",
        reply_markup=ReplyKeyboardMarkup(reply_keyboard, resize_keyboard=True)
    )

def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    if text == 'Сегодня':
        return await today_report(update, context)
    if text == 'Вчера':
        return await yesterday_report(update, context)
    if text == 'Неделя':
        return await week_report(update, context)

app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), start))

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling()
