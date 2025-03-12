import asyncio
import re
import hashlib
import hmac
import schedule
import time
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from telegram import Bot

# ===== Настройки Facebook =====
ACCESS_TOKEN = "EAASZCrBwhoH0BO6hvTPZBtAX3OFPcJjZARZBZCIllnjc4GkxagyhvvrylPKWdU9jMijZA051BJRRvVuV1nab4k5jtVO5q0TsDIKbXzphumaFIbqKDcJ3JMvQTmORdrNezQPZBP14pq4NKB56wpIiNJSLFa5yXFsDttiZBgUHAmVAJknN7Ig1ZBVU2q0vRyQKJtyuXXwZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# ===== Список рекламных аккаунтов =====
AD_ACCOUNTS = [
    "act_1206987573792913",
    "act_1333550570916716",
    "act_798205335840576",
    "act_844229314275496",
    "act_1108417930211002",
    "act_2342025859327675",
    "act_508239018969999",
    "act_1513759385846431",
    "act_1042955424178074",
    "act_1415004142524014"
]

# ===== Настройки Telegram =====
TELEGRAM_TOKEN = "8033028841:AAGp7856PuHCrAeIXYHGN2W6q83SsCWxxXI"
CHAT_ID = "253181449"
bot = Bot(token=TELEGRAM_TOKEN)

# ===== Оставленные метрики =====
ALLOWED_ACTIONS = {"link_click"}

# ===== Функция для удаления проблемных символов =====
def clean_text(text):
    if not isinstance(text, str):
        return str(text)
    text = re.sub(r'([_\*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)
    return text

# ===== Функция для вычисления appsecret_proof =====
def generate_appsecret_proof():
    return hmac.new(APP_SECRET.encode(), ACCESS_TOKEN.encode(), hashlib.sha256).hexdigest()

# ===== Функция для проверки, активен ли рекламный кабинет =====
def is_account_active(account_id):
    try:
        account_data = AdAccount(account_id).api_get(fields=['account_status'])
        return "✅" if account_data['account_status'] == 1 else "🔴"
    except Exception:
        return "🔴"

# ===== Функция для получения данных из Facebook =====
def get_facebook_data(account_id):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'actions', 'cost_per_action_type', 'spend']
    params = {'date_preset': 'yesterday', 'level': 'account', 'appsecret_proof': generate_appsecret_proof()}
    
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
        report += "\n⚠ Данных за вчера нет"
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

# ===== Функция для отправки отчёта в Telegram =====
async def send_to_telegram(message):
    print(f"Отправка сообщения в Telegram: {message}")  # Отладочный вывод
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="MarkdownV2")
        print("✅ Сообщение отправлено!")
    except Exception as e:
        print(f"❌ Ошибка отправки в Telegram: {e}")  # Вывод ошибки
async def send_to_telegram(message):
    print(f"Отправка сообщения в Telegram: {message}")
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message, parse_mode="MarkdownV2")
    except Exception as e:
        print(f"Ошибка отправки в Telegram: {e}")
# ===== Основная асинхронная функция =====
async def main():
    print("🔍 Запуск main() — проверка отправки сообщений в Telegram")  # Отладка
    for account_id in AD_ACCOUNTS:
        print(f"📡 Получение данных для аккаунта: {account_id}")  # Отладка
        report = get_facebook_data(account_id)
        print(f"📤 Отправка отчёта: {report}")  # Отладка перед отправкой
        await send_to_telegram(report) 
# ===== Запуск по расписанию =====
def run_bot():
    print("Запуск run_bot()")
print("🚀 Вызов main() — отправка сообщений началась!")    
asyncio.run(main())

# Запускать каждый день в 9:30 утра
schedule.every().day.at("04:30").do(run_bot) # Вместо 09:30 ставим 04:30 (по UTC)

if __name__ == "__main__":
    print("Скрипт стартовал, будет запускать задачи по расписанию")
    while True:
        schedule.run_pending()
        print("Скрипт работает, ждет следующего запуска...")
        time.sleep(60)

# ===== Храним отправленные предупреждения =====
sent_warnings = set()

# ===== Функция для проверки статуса рекламных аккаунтов и платежей =====
def check_billing_and_status():
    global sent_warnings
    problem_accounts = []

    for account_id in AD_ACCOUNTS:
        try:
            account = AdAccount(account_id)
            account_info = account.api_get(fields=['account_status', 'name', 'disable_reason'])

            account_name = account_info.get('name', 'Неизвестный аккаунт')
            status = account_info.get('account_status', 0)
            disable_reason = account_info.get('disable_reason', None)

            # Если аккаунт отключен или есть проблема с платежом
            if status != 1 or disable_reason:
                if account_id not in sent_warnings:
                    problem_accounts.append(f"🚨 Внимание! Рекламный аккаунт *{account_name}* отключен или есть проблемы с платежом!")
                    sent_warnings.add(account_id)  # Запоминаем, что отправили предупреждение

            # Если аккаунт снова активен, убираем его из списка предупреждений
            elif account_id in sent_warnings:
                problem_accounts.append(f"✅ Рекламный аккаунт *{account_name}* снова активен!")
                sent_warnings.remove(account_id)

        except Exception as e:
            problem_accounts.append(f"❌ Ошибка при проверке аккаунта {account_id}: {clean_text(str(e))}")

    # Если есть проблемы, отправляем уведомление
    if problem_accounts:
        message = "\n".join(problem_accounts)
        asyncio.run(send_to_telegram(message))

# Запуск проверки биллинга и статуса аккаунтов каждые 30 минут
schedule.every(30).minutes.do(check_billing_and_status)
