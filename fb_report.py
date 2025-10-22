import asyncio
import json
from math import ceil
from datetime import datetime, timedelta, time
from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)

ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"
ACCOUNTS_JSON = "accounts.json"
FORECAST_CACHE_FILE = "forecast_cache.json"

def load_accounts():
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_accounts(data):
    with open(ACCOUNTS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def get_enabled_accounts():
    data = load_accounts()
    return {k: v for k, v in data.items() if v.get("enabled", True)}

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except:
        return "🔴"

def format_number(num):
    return f"{int(float(num)):,}".replace(",", " ")

def get_report(account_id, period):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'date_preset': period, 'level': 'account'}
    try:
        insights = account.get_insights(fields=fields, params=params)
        name = account.api_get(fields=['name'])['name']
    except Exception as e:
        if "200" in str(e) or "403" in str(e):
            return ""
        return f"⚠️ Ошибка: {e}"
    if not insights:
        return f"{is_account_active(account_id)} <b>{name}</b>\nНет данных"
    i = insights[0]
    report = f"{is_account_active(account_id)} <b>{name}</b>\n"
    report += f"👁 {format_number(i.get('impressions','0'))} показов\n"
    report += f"🖱 {format_number(i.get('clicks','0'))} кликов\n"
    report += f"💸 {round(float(i.get('spend',0)),2)} $\n"
    return report

# === Telegram handlers ===

async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("📋 Отчёт по всем", callback_data="report_all")],
        [InlineKeyboardButton("📊 Отчёт по аккаунту", callback_data="choose_acc")],
        [InlineKeyboardButton("♿️ Метрика: Лид с сайта", callback_data="toggle_lead_metric")],
        [InlineKeyboardButton("💰 Проверить биллинги", callback_data="billing_list")],
        [InlineKeyboardButton("🔁 Синхронизировать аккаунты", callback_data="sync_accs")]
    ]
    await update.message.reply_text("Выберите действие:", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    await query.answer()
    if data == "report_all":
        accounts = get_enabled_accounts()
        for acc_id in accounts.keys():
            msg = get_report(acc_id, "today")
            if msg:
                await query.message.reply_text(msg, parse_mode='HTML')
    elif data == "choose_acc":
        accounts = list(get_enabled_accounts().items())
        buttons = [
            [InlineKeyboardButton(v["name"], callback_data=f"rep:{k}")]
            for k, v in accounts
        ]
        await query.message.reply_text("Выберите аккаунт:", reply_markup=InlineKeyboardMarkup(buttons))
    elif data.startswith("rep:"):
        acc_id = data.split(":")[1]
        periods = [
            [InlineKeyboardButton("Сегодня", callback_data=f"per:{acc_id}:today"),
             InlineKeyboardButton("Вчера", callback_data=f"per:{acc_id}:yesterday")],
            [InlineKeyboardButton("Прошлая неделя", callback_data=f"per:{acc_id}:last_week")]
        ]
        await query.message.reply_text("Выберите период:", reply_markup=InlineKeyboardMarkup(periods))
    elif data.startswith("per:"):
        _, acc_id, period = data.split(":")
        msg = get_report(acc_id, period)
        await query.message.reply_text(msg or "Нет данных", parse_mode='HTML')
    elif data == "toggle_lead_metric":
        data = load_accounts()
        msg = "♿️ Статус метрики 'Лид с сайта':\n\n"
        buttons = []
        for acc_id, v in data.items():
            state = "✅" if v.get("metrics", {}).get("leads") else "❌"
            msg += f"{state} {v['name']}\n"
            buttons.append([InlineKeyboardButton(f"{state} {v['name']}", callback_data=f"lead:{acc_id}")])
        await query.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(buttons))
    elif data.startswith("lead:"):
        acc_id = data.split(":")[1]
        data = load_accounts()
        acc = data.get(acc_id)
        if acc:
            acc["metrics"]["leads"] = not acc["metrics"].get("leads", False)
            save_accounts(data)
            new_state = "✅" if acc["metrics"]["leads"] else "❌"
            await query.message.reply_text(f"♿️ {acc['name']}: теперь {new_state}")
    elif data == "billing_list":
        await query.message.reply_text("💰 Биллинги будут добавлены позже.")
    elif data == "sync_accs":
        user = User(fbid="me")
        fetched = list(user.get_ad_accounts(fields=["account_id", "name"]))
        data = load_accounts()
        for i in fetched:
            acc_id = f"act_{i['account_id']}"
            data.setdefault(acc_id, {"name": i["name"], "enabled": True, "metrics": {"leads": False}})
        save_accounts(data)
        await query.message.reply_text("🔁 Синхронизация завершена.")

app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(CommandHandler("menu", menu))
app.add_handler(CallbackQueryHandler(handle_buttons))
print("🚀 Бот запущен и ожидает команд.")
app.run_polling()
