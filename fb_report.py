import asyncio
import re
import hashlib
import hmac
import schedule
import time
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, CallbackContext

ACCESS_TOKEN = "EAASZCrBwhoH0BO6hvTPZBtAX3OFPcJjZARZBZCIllnjc4GkxagyhvvrylPKWdU9jMijZA051BJRRvVuV1nab4k5jtVO5q0TsDIKbXzphumaFIbqKDcJ3JMvQTmORdrNezQPZBP14pq4NKB56wpIiNJSLFa5yXFsDttiZBgUHAmVAJknN7Ig1ZBVU2q0vRyQKJtyuXXwZDZD"
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
ALLOWED_ACTIONS = {"link_click"}

bot = Bot(token=TELEGRAM_TOKEN)

def clean_text(text):
    if not isinstance(text, str):
        return str(text)
    return re.sub(r'([_*\\[\\]()~`>#+\-=|{}.!])', r'\\\1', text)

def generate_appsecret_proof():
    return hmac.new(APP_SECRET.encode(), ACCESS_TOKEN.encode(), hashlib.sha256).hexdigest()

def is_account_active(account_id):
    try:
        account_data = AdAccount(account_id).api_get(fields=['account_status'])
        return "✅" if account_data['account_status'] == 1 else "🔴"
    except Exception:
        return "🔴"

def get_facebook_data(account_id, date_preset):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'actions', 'cost_per_action_type', 'spend']
    params = {'date_preset': date_preset, 'level': 'account', 'appsecret_proof': generate_appsecret_proof()}
    
    try:
        campaigns = account.get_insights(fields=fields, params=params)
    except Exception as e:
        return f"⚠ Ошибка загрузки данных для {account_id}: {clean_text(str(e))}"
    
    try:
        account_name = account.api_get(fields=['name'])['name']
    except Exception:
        account_name = "Неизвестный аккаунт"
    
    status_emoji = is_account_active(account_id)
    report = f"{status_emoji} {clean_text(account_name)}\n"
    
    if not campaigns:
        report += "\n⚠ Данных за выбранный период нет"
    else:
        for campaign in campaigns:
            report += f"\n👁 Показы: {clean_text(campaign.get('impressions', '—'))}"
            report += f"\n🎯 CPM: {clean_text(str(round(float(campaign.get('cpm', 0)) / 100, 2)))} USD"
            report += f"\n🖱 Клики: {clean_text(campaign.get('clicks', '—'))}"
            report += f"\n💸 CPC: {clean_text(str(round(float(campaign.get('cpc', 0)), 2)))} USD"
            
            if 'cost_per_action_type' in campaign:
                for cost in campaign['cost_per_action_type']:
                    if cost['action_type'] in ALLOWED_ACTIONS:
                        report += f"\n💰 Стоимость клика: {clean_text(str(round(float(cost['value']), 2)))} USD"
            
            spend = campaign.get('spend', 0)
            report += f"\n💵 Сумма затрат: {clean_text(str(round(float(spend), 2)))} USD"
    return report

async def send_to_telegram(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="MarkdownV2")
    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")

async def send_billing_alert(account_name, billing_amount):
    message = f"🚨 Ёбушки-воробушки, у нас биллинг!\n📢 Рекламный аккаунт: *{account_name}*\n💰 Сумма биллинга: *{billing_amount} KZT*"
    print(f"📢 Уведомление о биллинге: {message}")
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="Markdown")
        print("✅ Уведомление отправлено!")
    except Exception as e:
        print(f"❌ Ошибка отправки в Телеграм: {e}")

app = Application.builder().token(TELEGRAM_TOKEN).build()

async def today_report(update: Update, context: CallbackContext):
    await update.message.reply_text("Собираю данные за сегодня...")
    for account_id in AD_ACCOUNTS:
        await send_to_telegram(get_facebook_data(account_id, "today"))

app.add_handler(CommandHandler("today_report", today_report))

async def main():
    print("📡 Bot started polling")
    await app.run_polling()

async def scheduler_loop():
    while True:
        schedule.run_pending()
        await asyncio.sleep(5)  # Проверка задач каждые 5 секунд

# **Используем `asyncio.create_task()` для уведомлений**
schedule.every().day.at("04:30").do(lambda: asyncio.create_task(send_billing_alert("Тестовый аккаунт", 5000)))

if __name__ == "__main__":
    print("🚀 Бот запущен, задачи по расписанию...")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    loop.create_task(main())  # Запуск бота
    loop.create_task(scheduler_loop())  # Запуск планировщика
    
    try:
        loop.run_forever()  # Вместо блокирующего while True
    except KeyboardInterrupt:
        print("⏹ Бот остановлен вручную")
