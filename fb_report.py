import asyncio
import json
import re
from math import ceil
from datetime import datetime, timedelta, time
from pytz import timezone

import requests  # ⬅️ для автокурса USD→KZT

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# ====== Твои креды (как было) ======
ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# ====== Наборы аккаунтов (как было) ======
AD_ACCOUNTS = [
    "act_1415004142524014", "act_719853653795521", "act_1206987573792913", "act_1108417930211002",
    "act_2342025859327675", "act_844229314275496", "act_1333550570916716", "act_195526110289107",
    "act_2145160982589338", "act_1042955424178074", "act_4030694587199998", "act_508239018969999",
    "act_1357165995492721", "act_798205335840576"
]

MESSAGING_ACCOUNTS = {
    "act_1415004142524014", "act_1108417930211002", "act_2342025859327675", "act_1333550570916716",
    "act_844229314275496", "act_1206987573792913", "act_195526110289107", "act_2145160982589338",
    "act_719853653795521"
}

LEAD_FORM_ACCOUNTS = {
    "act_1042955424178074", "act_4030694587199998", "act_798205335840576"
}

ACCOUNT_NAMES = {
    "act_1415004142524014": "ЖС Астана", "act_719853653795521": "ЖС Караганда",
    "act_1206987573792913": "ЖС Павлодар", "act_1108417930211002": "ЖС Актау",
    "act_2342025859327675": "ЖС Атырау", "act_844229314275496": "ЖС Актобе",
    "act_1333550570916716": "ЖС Юг (Алматы)", "act_195526110289107": "ЖС Тараз",
    "act_2145160982589338": "ЖС Шымкент", "act_1042955424178074": "кенсе 1",
    "act_4030694587199998": "кенсе 2", "act_508239018969999": "Фитнес Поинт",
    "act_1357165995492721": "Ария Степи", "act_798205335840576": "Инвестиции"
}

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"  # твоя группа
FORECAST_CACHE_FILE = "forecast_cache.json"
FX_CACHE_FILE = "fx_cache.json"

account_statuses = {}

# ========= ВСПОМОГАТЕЛЬНОЕ =========

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except:
        return "🔴"

def format_number(num):
    return f"{int(float(num)):,}".replace(",", " ")

def get_usd_kzt_rate(spread_tenge: float = 5.0) -> float:
    """
    Возвращает курс USD→KZT (exchangerate.host), кэш ~6ч, +5₸ сверху.
    Фолбеки: кэш -> дефолт 490.
    """
    now = datetime.utcnow()
    try:
        with open(FX_CACHE_FILE, "r") as f:
            fx = json.load(f)
        ts = datetime.fromisoformat(fx.get("ts"))
        if (now - ts) < timedelta(hours=6) and fx.get("usd_kzt"):
            return float(fx["usd_kzt"]) + spread_tenge
    except Exception:
        pass

    try:
        resp = requests.get(
            "https://api.exchangerate.host/latest",
            params={"base": "USD", "symbols": "KZT"},
            timeout=10,
        )
        resp.raise_for_status()
        rate = float(resp.json()["rates"]["KZT"])
        with open(FX_CACHE_FILE, "w") as f:
            json.dump({"ts": now.isoformat(), "usd_kzt": rate}, f)
        return rate + spread_tenge
    except Exception:
        try:
            with open(FX_CACHE_FILE, "r") as f:
                fx = json.load(f)
            return float(fx["usd_kzt"]) + spread_tenge
        except Exception:
            return 490.0 + spread_tenge

# ========= ОСНОВНОЙ ОТЧЁТ =========

def get_facebook_data(account_id, date_preset, date_label=''):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'time_range': date_preset, 'level': 'account'} if isinstance(date_preset, dict) else {'date_preset': date_preset, 'level': 'account'}
    try:
        insights = account.get_insights(fields=fields, params=params)
        account_name = account.api_get(fields=['name'])['name']
    except Exception as e:
        # 🔇 Если нет доступа — просто ничего не шлём по этому аккаунту
        err = str(e)
        if "(#200)" in err or "code\": 200" in err or "403" in err:
            return None
        return (
            "⚠ <b>Ошибка:</b>\n\n"
            f"<pre>Message: {e}</pre>"
        )

    date_info = f" ({date_label})" if date_label else ""
    header = f"{is_account_active(account_id)} <b>{account_name}</b>{date_info}\n"

    if not insights:
        return header + "Нет данных за выбранный период"

    insight = insights[0]
    core = (
        f"👁 Показы: <b>{format_number(insight.get('impressions', '0'))}</b>\n"
        f"🎯 CPM: <b>{round(float(insight.get('cpm', 0)), 2)} $</b>\n"
        f"🖱 Клики: <b>{format_number(insight.get('clicks', '0'))}</b>\n"
        f"💸 CPC: <b>{round(float(insight.get('cpc', 0)), 2)} $</b>\n"
        f"💵 Затраты: <b>{round(float(insight.get('spend', 0)), 2)} $</b>"
    )

    actions = {a['action_type']: float(a['value']) for a in insight.get('actions', [])}

    extra_parts = []

    if account_id in MESSAGING_ACCOUNTS:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0)
        line = f"✉️ Начата переписка: <b>{int(conv)}</b>"
        if conv > 0:
            line += f"\n💬💲 Цена переписки: <b>{round(float(insight.get('spend', 0)) / conv, 2)} $</b>"
        extra_parts.append(line)

    if account_id in LEAD_FORM_ACCOUNTS:
        if account_id == 'act_4030694587199998':
            leads = actions.get('Website Submit Applications', 0)
        else:
            leads = (
                actions.get('offsite_conversion.fb_pixel_submit_application', 0) or
                actions.get('offsite_conversion.fb_pixel_lead', 0) or
                actions.get('lead', 0)
            )
        line = f"📩 Заявки: <b>{int(leads)}</b>"
        if leads > 0:
            line += f"\n📩💲 Цена заявки: <b>{round(float(insight.get('spend', 0)) / leads, 2)} $</b>"
        extra_parts.append(line)

    tail = ("\n" + "\n".join(extra_parts)) if extra_parts else ""
    return header + core + tail

async def send_report(context, chat_id, period, date_label=''):
    for acc in AD_ACCOUNTS:
        msg = get_facebook_data(acc, period, date_label)
        if msg:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

# ========= БИЛЛИНГИ =========

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in AD_ACCOUNTS:
        try:
            account = AdAccount(account_id)
            info = account.api_get(fields=['name', 'account_status', 'balance'])
            status = info.get('account_status')
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = info.get('name')
                balance = float(info.get('balance', 0)) / 100
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"⚠️⚠️⚠️ <b>Ахтунг!</b> {name}\nБиллинг: <b>{balance:.2f} $</b>",
                    parse_mode='HTML'
                )
            account_statuses[account_id] = status
        except Exception as e:
            # игнорим 403/нет доступа
            err = str(e)
            if "(#200)" in err or "code\": 200" in err or "403" in err:
                continue
            await context.bot.send_message(chat_id=CHAT_ID, text=f"⚠ Ошибка: {e}", parse_mode='HTML')

async def send_billing_list(context: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """
    По одному сообщению на аккаунт: имя, $ и ₸ (по автокурсу +5₸).
    """
    usd_kzt = get_usd_kzt_rate(spread_tenge=5.0)
    rate_str = f"{usd_kzt:.2f} ₸"

    for account_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(account_id)
            info = acc.api_get(fields=['name', 'account_status', 'balance'])
            name = info.get('name', ACCOUNT_NAMES.get(account_id, account_id))
            status = info.get('account_status', 0)
            balance_usd = float(info.get('balance', 0)) / 100.0
            balance_kzt = round(balance_usd * usd_kzt, 2)
            mark = "🟢" if status == 1 else "🔴"

            msg = (
                f"{mark} <b>{name}</b>\n"
                f"💵 <b>{balance_usd:.2f} $</b>  |  🇰🇿 <b>{balance_kzt:.2f} ₸</b>\n"
                f"<i>Курс: 1 $ = {rate_str}</i>"
            )
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
        except Exception as e:
            # молча игнорим нет доступа
            err = str(e)
            if "(#200)" in err or "code\": 200" in err or "403" in err:
                continue
            # иначе можно подсказать что сломалось
            # await context.bot.send_message(chat_id=chat_id, text=f"⚠ Ошибка по {account_id}: {e}")
            continue

# ========= ПРОГНОЗ БИЛЛИНГА (как было) =========

async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(timezone("Asia/Almaty")).date()
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except:
        cache = {}

    for acc_id in AD_ACCOUNTS:
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=["name", "spend_cap", "amount_spent"])
            spend_cap = float(info.get("spend_cap", 0)) / 100
            spent = float(info.get("amount_spent", 0)) / 100
            available = spend_cap - spent
            daily_budget = sum(
                int(c.get("daily_budget", 0)) / 100
                for c in acc.get_campaigns(fields=["name", "effective_status", "daily_budget"])
                if c.get("effective_status") == "ACTIVE"
            )
            if daily_budget == 0:
                continue
            days_left = ceil(available / daily_budget)
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
            # тихий лог
            print(f"Ошибка прогноза по {acc_id}: {e}")

    with open(FORECAST_CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ========= ПЛАНОВЫЕ ДЖОБЫ =========

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(timezone('Asia/Almaty')) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', label)
    # Следом — список биллингов отдельными сообщениями (с автокурсом)
    await send_billing_list(context, CHAT_ID)

# ========= ХЭНДЛЕРЫ КОМАНД И КНОПОК =========

async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    label = datetime.now().strftime('%d.%m.%Y')
    await send_report(context, update.effective_chat.id, 'today', label)

async def cmd_yesterday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, update.effective_chat.id, 'yesterday', label)

async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE):
    until = datetime.now() - timedelta(days=1)
    since = until - timedelta(days=6)
    period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
    label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
    await send_report(context, update.effective_chat.id, period, label)

async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_billing_list(context, update.effective_chat.id)

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "🆘 <b>Доступные команды</b>\n\n"
        "• <code>/today</code> — отчёт за сегодня\n"
        "• <code>/yesterday</code> — отчёт за вчера\n"
        "• <code>/week</code> — отчёт за прошедшую неделю\n"
        "• <code>/billing</code> — список биллингов по аккаунтам (в $ и ₸)\n\n"
        "В группах доступны инлайн-кнопки под сообщением /start."
    )
    await update.message.reply_text(text, parse_mode="HTML")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Инлайн для групп
    ikb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Сегодня", callback_data="today"),
            InlineKeyboardButton("Вчера", callback_data="yesterday"),
        ],
        [
            InlineKeyboardButton("Прошедшая неделя", callback_data="week"),
        ],
        [
            InlineKeyboardButton("Биллинг", callback_data="billing_list"),
        ],
    ])
    # Реплай для лички (оставим и его)
    rkb = ReplyKeyboardMarkup([['Сегодня', 'Вчера', 'Прошедшая неделя']], resize_keyboard=True)

    if update.effective_chat.type in ("group", "supergroup"):
        await update.message.reply_text("🤖 Выберите отчёт:", reply_markup=ikb)
    else:
        await update.message.reply_text("🤖 Выберите отчёт:", reply_markup=rkb)

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data

    if data == "today":
        await cmd_today(update, context)
    elif data == "yesterday":
        await cmd_yesterday(update, context)
    elif data == "week":
        await cmd_week(update, context)
    elif data == "billing_list":
        await send_billing_list(context, q.message.chat_id)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # старое поведение кнопок-реплаев в личке
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip().lower()
    if text == 'сегодня':
        await cmd_today(update, context)
    elif text == 'вчера':
        await cmd_yesterday(update, context)
    elif text == 'прошедшая неделя':
        await cmd_week(update, context)

# ========= APP =========

app = Application.builder().token(TELEGRAM_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", cmd_help))
app.add_handler(CommandHandler("today", cmd_today))
app.add_handler(CommandHandler("yesterday", cmd_yesterday))
app.add_handler(CommandHandler("week", cmd_week))
app.add_handler(CommandHandler("billing", cmd_billing))
app.add_handler(CallbackQueryHandler(on_button))

app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

# джобы как были
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))

if __name__ == "__main__":
    print("\U0001F680 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
