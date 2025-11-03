# fb_report.py
import asyncio
import json
from math import ceil
from datetime import datetime, timedelta, time
from pytz import timezone

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ==== ТВОИ КРЕДЫ ====
ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# ==== Телеграм ====
TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"  # группа для ежедневного отчёта

# ==== Файлы ====
ACCOUNTS_JSON = "accounts.json"
FORECAST_CACHE_FILE = "forecast_cache.json"

# ==== Фоллбек-список, если нет accounts.json ====
AD_ACCOUNTS_FALLBACK = [
    "act_1415004142524014", "act_719853653795521", "act_1206987573792913", "act_1108417930211002",
    "act_2342025859327675", "act_844229314275496", "act_1333550570916716", "act_195526110289107",
    "act_2145160982589338", "act_508239018969999",
    "act_1357165995492721", "act_798205335840576"
]

# ==== Человекочитаемые имена (подхватываются и обновляются из БМ) ====
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
    "act_508239018969999":  "Фитнес Поинт",
    "act_1357165995492721": "Ария Степи",
    "act_798205335840576":  "Инвестиции",
}

# ==== Исключаем «кенсе» ====
EXCLUDED_AD_ACCOUNT_IDS = {"act_1042955424178074", "act_4030694587199998"}
EXCLUDED_NAME_KEYWORDS = {"kense", "кенсе"}

def _normalize_act_id(aid: str) -> str:
    aid = str(aid).strip()
    return aid if aid.startswith("act_") else f"act_{aid}"

def load_accounts() -> dict:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_accounts(data: dict):
    with open(ACCOUNTS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _looks_excluded_by_name(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_NAME_KEYWORDS)

def upsert_accounts_from_fb() -> dict:
    """
    Тянем me/adaccounts, исключаем 'кенсе', добавляем/обновляем в accounts.json.
    Метрики не трогаем (кроме того, что messaging везде включим миграцией ниже).
    """
    data = load_accounts()
    me = User(fbid="me")
    fetched = list(me.get_ad_accounts(fields=["account_id", "name", "account_status"]))

    added, updated, skipped = 0, 0, 0
    for item in fetched:
        acc_id = _normalize_act_id(item.get("account_id"))
        name = item.get("name") or acc_id

        if acc_id in EXCLUDED_AD_ACCOUNT_IDS or _looks_excluded_by_name(name):
            skipped += 1
            continue

        ACCOUNT_NAMES.setdefault(acc_id, name)

        if acc_id in data:
            if name and data[acc_id].get("name") != name:
                data[acc_id]["name"] = name
                updated += 1
        else:
            # дефолты (enabled=True; metrics.messaging = True принудительно ниже миграцией)
            data[acc_id] = {
                "name": name,
                "enabled": True,
                "metrics": {"messaging": True, "leads": False},
                "alerts": {"enabled": False, "target_cpl": 0.0}
            }
            added += 1

    save_accounts(data)
    return {"added": added, "updated": updated, "skipped": skipped, "total": len(data)}

def ensure_messaging_on_everywhere(force: bool = True):
    """
    Проставляет metrics.messaging = True ВСЕМ аккаунтам в accounts.json.
    force=True — ставим True даже если раньше было False.
    """
    data = load_accounts()
    changed = False
    for acc_id, row in data.items():
        metrics = row.get("metrics") or {}
        if force:
            if metrics.get("messaging") is not True:
                metrics["messaging"] = True
                row["metrics"] = metrics
                data[acc_id] = row
                changed = True
        else:
            if "messaging" not in metrics:
                metrics["messaging"] = True
                row["metrics"] = metrics
                data[acc_id] = row
                changed = True
    if changed:
        save_accounts(data)

def get_enabled_accounts_in_order() -> list[str]:
    data = load_accounts()
    if not data:
        return AD_ACCOUNTS_FALLBACK
    ordered = []
    for acc_id, row in data.items():
        if row.get("enabled", True):
            ordered.append(acc_id)
    return ordered or AD_ACCOUNTS_FALLBACK

def is_account_active(account_id) -> str:
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except Exception:
        return "🔴"

def format_number(num):
    try:
        return f"{int(float(num)):,}".replace(",", " ")
    except Exception:
        return "0"

def _get_metrics_flags(acc_id: str) -> tuple[bool, bool]:
    """
    Возвращает (messaging_on, leads_on) из accounts.json.
    Если записи нет — messaging=True по умолчанию, leads=False.
    """
    data = load_accounts()
    row = data.get(acc_id, {})
    metrics = row.get("metrics", {})
    messaging_on = metrics.get("messaging", True)  # по умолчанию True
    leads_on = metrics.get("leads", False)
    return messaging_on, leads_on

def _pretty_name(acc_id: str, fallback: str = "") -> str:
    if acc_id in ACCOUNT_NAMES:
        return ACCOUNT_NAMES[acc_id]
    try:
        name = AdAccount(acc_id).api_get(fields=['name']).get('name')
        if name:
            ACCOUNT_NAMES[acc_id] = name
            return name
    except Exception:
        pass
    return fallback or acc_id

def get_facebook_data(account_id, date_preset, date_label=''):
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = (
        {'time_range': date_preset, 'level': 'account'}
        if isinstance(date_preset, dict)
        else {'date_preset': date_preset, 'level': 'account'}
    )

    try:
        insights = account.get_insights(fields=fields, params=params)
    except Exception as e:
        err = str(e)
        # молча игнорируем недоступные (403/200/permissions) — никакого спама
        if "code: 200" in err or "403" in err or "permission" in err.lower():
            return ""
        return f"⚠ Ошибка: {str(e)}"

    name = _pretty_name(account_id)
    date_info = f" ({date_label})" if date_label else ""
    report = f"{is_account_active(account_id)} <b>{name}</b>{date_info}\n"

    if not insights:
        return report + "Нет данных за выбранный период"

    insight = insights[0]
    report += (
        f"👁 Показы: {format_number(insight.get('impressions', '0'))}\n"
        f"🎯 CPM: {round(float(insight.get('cpm', 0) or 0), 2)} $\n"
        f"🖱 Клики: {format_number(insight.get('clicks', '0'))}\n"
        f"💸 CPC: {round(float(insight.get('cpc', 0) or 0), 2)} $\n"
        f"💵 Затраты: {round(float(insight.get('spend', 0) or 0), 2)} $"
    )

    actions = {a['action_type']: float(a['value']) for a in insight.get('actions', [])}
    messaging_on, leads_on = _get_metrics_flags(account_id)

    if messaging_on:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0)
        report += f"\n✉️ Переписки: {int(conv)}"
        spend = float(insight.get('spend', 0) or 0)
        if conv > 0:
            report += f"\n💬💲 Цена переписки: {round(spend / conv, 2)} $"

    if leads_on:
        # Особый случай "Website Submit Applications" для одного аккаунта у тебя раньше был.
        leads = (
            actions.get('offsite_conversion.fb_pixel_submit_application', 0)
            or actions.get('offsite_conversion.fb_pixel_lead', 0)
            or actions.get('lead', 0)
            or actions.get('Website Submit Applications', 0)
        )
        report += f"\n📩 Заявки: {int(leads)}"
        spend = float(insight.get('spend', 0) or 0)
        if leads > 0:
            report += f"\n📩💲 Цена заявки: {round(spend / leads, 2)} $"

    return report

async def send_report(context, chat_id, period, date_label=''):
    for acc in get_enabled_accounts_in_order():
        msg = get_facebook_data(acc, period, date_label)
        if msg:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

# ===== Биллинг: уведомление если аккаунт ушёл из ACTIVE =====
account_statuses = {}

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in get_enabled_accounts_in_order():
        try:
            account = AdAccount(account_id)
            info = account.api_get(fields=['name', 'account_status', 'balance'])
            status = info.get('account_status')
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = info.get('name') or _pretty_name(account_id)
                balance = float(info.get('balance', 0)) / 100
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"⚠️ Ахтунг! <b>{name}</b> — биллинг {balance:.2f} $",
                    parse_mode='HTML'
                )
            account_statuses[account_id] = status
        except Exception:
            continue

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(timezone('Asia/Almaty')) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', label)

async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(timezone("Asia/Almaty")).date()
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except Exception:
        cache = {}

    for acc_id in get_enabled_accounts_in_order():
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=["name", "spend_cap", "amount_spent"])
            spend_cap = float(info.get("spend_cap", 0) or 0) / 100
            spent = float(info.get("amount_spent", 0) or 0) / 100
            available = spend_cap - spent
            daily_budget = sum(
                int(c.get("daily_budget", 0) or 0) / 100
                for c in acc.get_campaigns(fields=["name", "effective_status", "daily_budget"])
                if c.get("effective_status") == "ACTIVE"
            )
            if daily_budget <= 0:
                continue
            days_left = ceil(available / daily_budget) if daily_budget else 0
            billing_date = today + timedelta(days=days_left)
            if (billing_date - today).days == 3:
                if cache.get(acc_id) == billing_date.isoformat():
                    continue
                name = _pretty_name(acc_id)
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
        except Exception:
            continue

    with open(FORECAST_CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ===== Кнопки и команды =====

def _main_keyboard():
    return ReplyKeyboardMarkup(
        [['Сегодня', 'Вчера'], ['Прошедшая неделя', 'Биллинг']],
        resize_keyboard=True
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text('🤖 Выберите отчёт:', reply_markup=_main_keyboard())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "Доступные команды:\n"
        "/start — показать кнопки\n"
        "/help — это сообщение\n"
        "/sync_accounts — подтянуть кабинеты из БМ\n"
    )
    await update.message.reply_text(msg)

def is_admin(user_id: int) -> bool:
    # при необходимости — подставь список админов
    return True

async def cmd_sync_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        res = upsert_accounts_from_fb()
        # после синка ещё раз включаем messaging всем (на случай новых)
        ensure_messaging_on_everywhere(force=True)
        msg = (
            "✅ Синхронизация завершена\n"
            f"Добавлено: {res['added']}\n"
            f"Обновлено имён: {res['updated']}\n"
            f"Пропущено (исключено): {res['skipped']}\n"
            f"Итого в конфиге: {res['total']}"
        )
        await update.message.reply_text(msg)
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка синхронизации: {e}")

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()

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
    elif text == 'Биллинг':
        # разово прогнать check_billing и уведомить только про неактивные
        for account_id in get_enabled_accounts_in_order():
            try:
                info = AdAccount(account_id).api_get(fields=['name', 'account_status', 'balance'])
                if info.get('account_status') != 1:
                    name = info.get('name') or _pretty_name(account_id)
                    balance = float(info.get('balance', 0)) / 100
                    await update.message.reply_text(
                        f"🔴 <b>{name}</b>\n💵 {balance:.2f} $",
                        parse_mode='HTML'
                    )
            except Exception:
                continue
    else:
        await update.message.reply_text('🤖 Выберите отчёт:', reply_markup=_main_keyboard())

# ===== Bootstrap =====
ensure_messaging_on_everywhere(force=True)  # <- ВКЛЮЧАЕМ ПЕРЕПИСКИ ВЕЗДЕ

app = Application.builder().token(TELEGRAM_TOKEN).build()

app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", help_cmd))
app.add_handler(CommandHandler("sync_accounts", cmd_sync_accounts))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

# фоновые задачи
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
