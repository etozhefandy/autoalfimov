import asyncio
import json
from math import ceil
from datetime import datetime, timedelta, time
from typing import Dict, Any, List

from pytz import timezone

# --- Facebook SDK
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi

# --- Telegram
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

# ==========================
#   КОНФИГ / ТОКЕНЫ
# ==========================

# Заполни своими значениями или выставь переменные окружения в Railway
ACCESS_TOKEN = "PASTE_FACEBOOK_ACCESS_TOKEN"
APP_ID = "PASTE_APP_ID"
APP_SECRET = "PASTE_APP_SECRET"

TELEGRAM_TOKEN = "PASTE_TELEGRAM_BOT_TOKEN"
CHAT_ID = "-1002679045097"  # твоя группа для ежедневных отчётов

# Инициализация FB API
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

# Файлы кэша/конфига
FORECAST_CACHE_FILE = "forecast_cache.json"
ACCOUNTS_JSON = "accounts.json"

# Фоллбек-список (если нет accounts.json)
AD_ACCOUNTS_FALLBACK = [
    "act_1415004142524014", "act_719853653795521", "act_1206987573792913", "act_1108417930211002",
    "act_2342025859327675", "act_844229314275496", "act_1333550570916716", "act_195526110289107",
    "act_2145160982589338", "act_508239018969999", "act_1357165995492721", "act_798205335840576",
]

# Читабельные имена (дополняется во время синка)
ACCOUNT_NAMES: Dict[str, str] = {
    "act_1415004142524014": "ЖС Астана", "act_719853653795521": "ЖС Караганда",
    "act_1206987573792913": "ЖС Павлодар", "act_1108417930211002": "ЖС Актау",
    "act_2342025859327675": "ЖС Атырау", "act_844229314275496": "ЖС Актобе",
    "act_1333550570916716": "ЖС Юг (Алматы)", "act_195526110289107": "ЖС Тараз",
    "act_2145160982589338": "ЖС Шымкент", "act_508239018969999": "Фитнес Поинт",
    "act_1357165995492721": "Ария Степи", "act_798205335840576": "Инвестиции",
}

# Исключаем из авто-синка (например «Кенсе»)
EXCLUDED_AD_ACCOUNT_IDS = {"act_1042955424178074", "act_4030694587199998"}
EXCLUDED_NAME_KEYWORDS = {"kense", "кенсе"}

# ==========================
#   УТИЛИТЫ accounts.json
# ==========================

def load_accounts() -> Dict[str, Any]:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_accounts(data: Dict[str, Any]) -> None:
    with open(ACCOUNTS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _normalize_act_id(aid: str) -> str:
    aid = str(aid).strip()
    return aid if aid.startswith("act_") else f"act_{aid}"

def _looks_excluded_by_name(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_NAME_KEYWORDS)

def upsert_accounts_from_fb() -> Dict[str, int]:
    """
    Тянем me/adaccounts, отбрасываем исключения, объединяем в accounts.json.
    Переписки включены ВСЕГДА (не настраиваются), тумблер только для ♿️ leads.
    """
    data = load_accounts()
    me = User(fbid="me")
    fetched = list(me.get_ad_accounts(fields=["account_id", "name", "account_status"]))

    added = updated = skipped = 0
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
            data[acc_id] = {
                "name": name,
                "enabled": True,
                "metrics": {"leads": False}  # переписки ALWAYS ON
            }
            added += 1

    save_accounts(data)
    return {"added": added, "updated": updated, "skipped": skipped, "total": len(data)}

def get_enabled_accounts_in_order() -> List[str]:
    data = load_accounts()
    if not data:
        return AD_ACCOUNTS_FALLBACK
    # порядок — как в файле (dict в Py3.7+ упорядочен по вставке)
    return [aid for aid, row in data.items() if row.get("enabled", True)] or AD_ACCOUNTS_FALLBACK

def leads_enabled(acc_id: str) -> bool:
    cfg = load_accounts().get(acc_id, {})
    return bool(cfg.get("metrics", {}).get("leads", False))

# ==========================
#   FB / ОТЧЁТЫ
# ==========================

account_statuses: Dict[str, int] = {}

def is_account_active(account_id: str) -> str:
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except Exception:
        return "🔴"

def format_number(num) -> str:
    try:
        return f"{int(float(num)):,}".replace(",", " ")
    except Exception:
        return "0"

def _period_to_params(period) -> Dict[str, Any]:
    if isinstance(period, dict):
        return {'time_range': period, 'level': 'account'}
    return {'date_preset': period, 'level': 'account'}

def get_facebook_data(account_id: str, period, date_label: str = "") -> str:
    """
    Возвращает HTML-сообщение по аккаунту или "" если аккаунт недоступен (403/permissions).
    Переписки — всегда показываем.
    Лиды — только если включены (♿️).
    """
    account = AdAccount(account_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = _period_to_params(period)
    try:
        insights = account.get_insights(fields=fields, params=params)
        info = account.api_get(fields=['name'])
        account_name = info.get('name', ACCOUNT_NAMES.get(account_id, account_id))
    except Exception as e:
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""  # молча пропускаем
        return f"⚠ Ошибка: {e}"

    date_info = f" ({date_label})" if date_label else ""
    header = f"{is_account_active(account_id)} <b>{account_name}</b>{date_info}"

    if not insights:
        return f"{header}\nНет данных за выбранный период"

    ins = insights[0]
    report = [
        header,
        f"👁 Показы: {format_number(ins.get('impressions', '0'))}",
        f"🎯 CPM: {round(float(ins.get('cpm', 0) or 0), 2)} $",
        f"🖱 Клики: {format_number(ins.get('clicks', '0'))}",
        f"💸 CPC: {round(float(ins.get('cpc', 0) or 0), 2)} $",
        f"💵 Затраты: {round(float(ins.get('spend', 0) or 0), 2)} $",
    ]

    # Собираем действия
    act_map = {a['action_type']: float(a['value']) for a in ins.get('actions', [])}

    # Переписки — ВСЕГДА
    conv = act_map.get('onsite_conversion.messaging_conversation_started_7d', 0.0)
    report.append(f"✉️ Переписки: {int(conv)}")
    if conv > 0:
        report.append(f"💬💲 Цена переписки: {round(float(ins.get('spend', 0) or 0) / conv, 2)} $")

    # Лиды — по тумблеру ♿️
    if leads_enabled(account_id):
        leads = (
            act_map.get('Website Submit Applications', 0.0) or
            act_map.get('offsite_conversion.fb_pixel_submit_application', 0.0) or
            act_map.get('offsite_conversion.fb_pixel_lead', 0.0) or
            act_map.get('lead', 0.0)
        )
        report.append(f"📩 Лиды: {int(leads)}")
        if leads > 0:
            report.append(f"📩💲 Цена лида: {round(float(ins.get('spend', 0) or 0) / leads, 2)} $")

    return "\n".join(report)

async def send_report(context: ContextTypes.DEFAULT_TYPE, chat_id: int | str, period, date_label: str = ""):
    for acc in get_enabled_accounts_in_order():
        msg = get_facebook_data(acc, period, date_label)
        if msg:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in get_enabled_accounts_in_order():
        try:
            account = AdAccount(account_id)
            info = account.api_get(fields=['name', 'account_status', 'balance'])
            status = info.get('account_status')
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = info.get('name', ACCOUNT_NAMES.get(account_id, account_id))
                balance = float(info.get('balance', 0) or 0) / 100
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"⚠️ ⚠️ ⚠️ Ахтунг! {name}! у нас биллинг — {balance:.2f} $",
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
            if daily_budget == 0:
                continue
            days_left = ceil(available / daily_budget) if daily_budget else 0
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
        except Exception:
            continue

    with open(FORECAST_CACHE_FILE, "w") as f:
        json.dump(cache, f)

# ==========================
#   UI / МЕНЮ
# ==========================

def kb_main_private() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data="rpt:all:today"),
         InlineKeyboardButton("Вчера", callback_data="rpt:all:yesterday")],
        [InlineKeyboardButton("Прошедшая неделя", callback_data="rpt:all:week")],
        [InlineKeyboardButton("📊 Отчёт по аккаунту", callback_data="pick:account")],
        [InlineKeyboardButton("💳 Биллинг", callback_data="billing:list")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="settings:root")],
    ])

def kb_main_group() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data="rpt:all:today"),
         InlineKeyboardButton("Вчера", callback_data="rpt:all:yesterday"),
         InlineKeyboardButton("Неделя", callback_data="rpt:all:week")],
        [InlineKeyboardButton("Биллинг", callback_data="billing:list"),
         InlineKeyboardButton("Аккаунт ▶︎", callback_data="pick:account")],
    ])

def kb_period_for_account(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Сегодня", callback_data=f"rpt:{acc_id}:today"),
         InlineKeyboardButton("Вчера", callback_data=f"rpt:{acc_id}:yesterday")],
        [InlineKeyboardButton("Прошедшая неделя", callback_data=f"rpt:{acc_id}:week")],
        [InlineKeyboardButton("← Назад", callback_data="pick:account")],
    ])

def kb_settings_account(acc_id: str, leads_on: bool) -> InlineKeyboardMarkup:
    label_leads = f"♿️ Лид с сайта: {'ВКЛ' if leads_on else 'ВЫКЛ'}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(label_leads, callback_data=f"toggle:leads:{acc_id}")],
        [InlineKeyboardButton("← Назад", callback_data="settings:root")],
    ])

# ==========================
#   ХЕНДЛЕРЫ
# ==========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type == "private":
        await update.message.reply_text("🤖 Выберите действие:", reply_markup=kb_main_private())
    else:
        await update.message.reply_text("🤖 Выберите отчёт:", reply_markup=kb_main_group())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (
        "Доступные команды:\n"
        "/start — меню\n"
        "/help — это сообщение\n"
        "/sync_accounts — подтянуть кабинеты из БМ\n"
        "/accounts — список/вкл/выкл аккаунтов\n"
    )
    await update.message.reply_text(txt)

def is_admin(user_id: int) -> bool:
    # при необходимости — ограничь список
    return True

async def cmd_sync_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    try:
        res = upsert_accounts_from_fb()
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

async def cmd_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = load_accounts()
    if not data:
        await update.message.reply_text("Конфиг пуст. Сначала /sync_accounts")
        return
    lines = []
    for acc_id, row in data.items():
        name = row.get("name") or ACCOUNT_NAMES.get(acc_id, acc_id)
        enabled = "ВКЛ" if row.get("enabled", True) else "ВЫКЛ"
        leads = "ВКЛ" if row.get("metrics", {}).get("leads", False) else "ВЫКЛ"
        lines.append(f"• {name}  —  аккаунт: {enabled}  /  ♿️ лид: {leads}")
    await update.message.reply_text("Текущие настройки:\n" + "\n".join(lines))

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Лёгкая клавиатура на текст «Сегодня/Вчера/Неделя/Биллинг» — по желанию
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
        await check_billing(context)
    else:
        if update.effective_chat.type == "private":
            keyboard = [['Сегодня', 'Вчера'], ['Прошедшая неделя', 'Биллинг']]
            await update.message.reply_text('🤖 Выберите отчёт:',
                                            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))

async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    # Отчёты (все/один)
    if data.startswith("rpt:"):
        _, target, period = data.split(":")
        if period == "today":
            label = datetime.now().strftime('%d.%m.%Y')
            if target == "all":
                await send_report(context, query.message.chat_id, 'today', label)
            else:
                msg = get_facebook_data(target, 'today', label)
                if msg:
                    await context.bot.send_message(query.message.chat_id, msg, parse_mode='HTML')
        elif period == "yesterday":
            label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
            if target == "all":
                await send_report(context, query.message.chat_id, 'yesterday', label)
            else:
                msg = get_facebook_data(target, 'yesterday', label)
                if msg:
                    await context.bot.send_message(query.message.chat_id, msg, parse_mode='HTML')
        elif period == "week":
            until = datetime.now() - timedelta(days=1)
            since = until - timedelta(days=6)
            period_obj = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
            label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
            if target == "all":
                await send_report(context, query.message.chat_id, period_obj, label)
            else:
                msg = get_facebook_data(target, period_obj, label)
                if msg:
                    await context.bot.send_message(query.message.chat_id, msg, parse_mode='HTML')
        return

    # Биллинг
    if data == "billing:list":
        await check_billing(context)
        return

    # Выбор аккаунта для отчёта
    if data == "pick:account":
        rows = []
        for acc in get_enabled_accounts_in_order():
            dot = is_account_active(acc)
            name = ACCOUNT_NAMES.get(acc, acc)
            rows.append([InlineKeyboardButton(f"{dot} {name}", callback_data=f"pickp:{acc}")])
        rows.append([InlineKeyboardButton("Закрыть", callback_data="noop")])
        await query.message.reply_text("Выберите аккаунт:", reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("pickp:"):
        acc_id = data.split(":", 1)[1]
        await query.message.reply_text("Выберите период:", reply_markup=kb_period_for_account(acc_id))
        return

    # Настройки
    if data == "settings:root":
        rows = []
        data_map = load_accounts()
        if not data_map:
            await query.message.reply_text("Сначала выполните /sync_accounts")
            return
        for acc_id, row in data_map.items():
            name = row.get("name") or ACCOUNT_NAMES.get(acc_id, acc_id)
            leads_on = bool(row.get("metrics", {}).get("leads", False))
            rows.append([InlineKeyboardButton(f"{name} • ♿️ {'ВКЛ' if leads_on else 'ВЫКЛ'}",
                                              callback_data=f"settings:acc:{acc_id}")])
        rows.append([InlineKeyboardButton("Закрыть", callback_data="noop")])
        await query.message.reply_text("Настройки метрик (переписки всегда ВКЛ):",
                                       reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("settings:acc:"):
        acc_id = data.split(":")[-1]
        accs = load_accounts()
        row = accs.get(acc_id, {"metrics": {}})
        leads_on = bool(row.get("metrics", {}).get("leads", False))
        await query.message.reply_text(f"Настройки: {ACCOUNT_NAMES.get(acc_id, acc_id)}",
                                       reply_markup=kb_settings_account(acc_id, leads_on))
        return

    if data.startswith("toggle:leads:"):
        acc_id = data.split(":")[-1]
        accs = load_accounts()
        r = accs.get(acc_id)
        if not r:
            await query.message.reply_text("Не найдено в конфиге.")
            return
        m = r.setdefault("metrics", {})
        m["leads"] = not bool(m.get("leads", False))
        save_accounts(accs)
        await query.message.reply_text("✅ Сохранено.",
                                       reply_markup=kb_settings_account(acc_id, m["leads"]))
        return

    if data == "noop":
        return

# ==========================
#   BOOTSTRAP
# ==========================

app = Application.builder().token(TELEGRAM_TOKEN).build()

# Команды
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", help_cmd))
app.add_handler(CommandHandler("sync_accounts", cmd_sync_accounts))
app.add_handler(CommandHandler("accounts", cmd_accounts))

# Кнопки-инлайн
app.add_handler(CallbackQueryHandler(callback_router))

# Текстовые кнопки (reply-keyboard) – опционально
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

# Джобы
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
