import asyncio
import json
from math import ceil
from datetime import datetime, timedelta, time
from pytz import timezone

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ==== Креды ====
ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTзаполни_своим_валидным_токеном"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"  # группа для плановых отчётов
FORECAST_CACHE_FILE = "forecast_cache.json"
ACCOUNTS_JSON = "accounts.json"

# ==== Запасной список (если accounts.json пустой) ====
AD_ACCOUNTS_FALLBACK = [
    "act_1415004142524014", "act_719853653795521", "act_1206987573792913", "act_1108417930211002",
    "act_2342025859327675", "act_844229314275496", "act_1333550570916716", "act_195526110289107",
    "act_2145160982589338", "act_1042955424178074", "act_4030694587199998", "act_508239018969999",
    "act_1357165995492721", "act_798205335840576"
]

# ==== Метрики «по умолчанию» для обратной совместимости ====
MESSAGING_ACCOUNTS = {
    "act_1415004142524014","act_1108417930211002","act_2342025859327675","act_1333550570916716",
    "act_844229314275496","act_1206987573792913","act_195526110289107","act_2145160982589338","act_719853653795521"
}
LEADS_ACCOUNTS = {"act_1042955424178074","act_4030694587199998","act_798205335840576"}

ACCOUNT_NAMES = {
    "act_1415004142524014": "ЖС Астана", "act_719853653795521": "ЖС Караганда",
    "act_1206987573792913": "ЖС Павлодар", "act_1108417930211002": "ЖС Актау",
    "act_2342025859327675": "ЖС Атырау", "act_844229314275496": "ЖС Актобе",
    "act_1333550570916716": "ЖС Юг (Алматы)", "act_195526110289107": "ЖС Тараз",
    "act_2145160982589338": "ЖС Шымкент", "act_1042955424178074": "Кенсе 1",
    "act_4030694587199998": "Кенсе 2", "act_508239018969999": "Фитнес Поинт",
    "act_1357165995492721": "Ария Степи", "act_798205335840576": "Инвестиции"
}

# Исключаемые из автосинка
EXCLUDED_AD_ACCOUNT_IDS = {"act_1042955424178074","act_4030694587199998"}  # кенсе
EXCLUDED_NAME_KEYWORDS = {"kense","кенсе"}

# ===== helpers: accounts.json =====
def load_accounts() -> dict:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_accounts(data: dict):
    with open(ACCOUNTS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _normalize_act_id(aid: str) -> str:
    aid = str(aid).strip()
    return aid if aid.startswith("act_") else f"act_{aid}"

def _looks_excluded_by_name(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_NAME_KEYWORDS)

def upsert_accounts_from_fb() -> dict:
    data = load_accounts()
    me = User(fbid="me")
    fetched = list(me.get_ad_accounts(fields=["account_id","name","account_status"]))
    added, updated, skipped = 0, 0, 0
    for item in fetched:
        acc_id = _normalize_act_id(item.get("account_id"))
        name = item.get("name") or acc_id
        if acc_id in EXCLUDED_AD_ACCOUNT_IDS or _looks_excluded_by_name(name):
            skipped += 1
            continue
        # если нет в accounts.json — создаём с настройками по умолчанию
        if acc_id not in data:
            data[acc_id] = {
                "name": name,
                "enabled": True,
                "metrics": {"messaging": acc_id in MESSAGING_ACCOUNTS, "leads": acc_id in LEADS_ACCOUNTS}
            }
            added += 1
        else:
            if name and data[acc_id].get("name") != name:
                data[acc_id]["name"] = name
                updated += 1
        ACCOUNT_NAMES.setdefault(acc_id, name)
    save_accounts(data)
    return {"added": added, "updated": updated, "skipped": skipped, "total": len(data)}

def get_enabled_accounts_in_order() -> list[str]:
    data = load_accounts()
    if not data:
        return AD_ACCOUNTS_FALLBACK
    # порядок — как в файле (dict сохраняет порядок вставки)
    return [aid for aid, row in data.items() if row.get("enabled", True)]

def acc_name(aid: str) -> str:
    data = load_accounts()
    return data.get(aid, {}).get("name") or ACCOUNT_NAMES.get(aid, aid)

# ===== отчетная логика =====
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
    # подтягиваем флаги метрик из accounts.json
    acc_cfg = load_accounts().get(account_id, {"metrics": {}})
    want_msg = acc_cfg.get("metrics", {}).get("messaging", False)
    want_leads = acc_cfg.get("metrics", {}).get("leads", False)

    account = AdAccount(account_id)
    fields = ['impressions','cpm','clicks','cpc','spend','actions']
    params = {'time_range': date_preset,'level':'account'} if isinstance(date_preset, dict) else {'date_preset':date_preset,'level':'account'}
    try:
        insights = account.get_insights(fields=fields, params=params)
        name = account.api_get(fields=['name']).get('name', acc_name(account_id))
    except Exception as e:
        err = str(e)
        # тихо игнорим недоступные (403/200 permissions)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return f"⚠ Ошибка: {str(e)}"

    date_info = f" ({date_label})" if date_label else ""
    report = f"{is_account_active(account_id)} <b>{name}</b>{date_info}\n"
    if not insights:
        return report + "Нет данных за выбранный период"

    insight = insights[0]
    report += (
        f"👁 Показы: {format_number(insight.get('impressions','0'))}\n"
        f"🎯 CPM: {round(float(insight.get('cpm',0)),2)} $\n"
        f"🖱 Клики: {format_number(insight.get('clicks','0'))}\n"
        f"💸 CPC: {round(float(insight.get('cpc',0)),2)} $\n"
        f"💵 Затраты: {round(float(insight.get('spend',0)),2)} $"
    )

    actions = {a['action_type']: float(a['value']) for a in insight.get('actions', [])}

    if want_msg:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0)
        report += f"\n✉️ Начата переписка: {int(conv)}"
        if conv > 0:
            report += f"\n💬💲 Цена переписки: {round(float(insight.get('spend',0))/conv,2)} $"

    if want_leads:
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
            report += f"\n📩💲 Цена заявки: {round(float(insight.get('spend',0))/leads,2)} $"

    return report

async def send_report(context, chat_id, period, date_label=''):
    for acc in get_enabled_accounts_in_order():
        msg = get_facebook_data(acc, period, date_label)
        if msg:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in get_enabled_accounts_in_order():
        try:
            account = AdAccount(account_id)
            info = account.api_get(fields=['name','account_status','balance'])
            status = info.get('account_status')
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = info.get('name') or acc_name(account_id)
                balance = float(info.get('balance', 0))/100
                await context.bot.send_message(chat_id=CHAT_ID, text=f"⚠️ ⚠️ ⚠️ Ахтунг! {name}! у нас биллинг - {balance:.2f} $", parse_mode='HTML')
            account_statuses[account_id] = status
        except Exception:
            continue

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(timezone('Asia/Almaty')) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, CHAT_ID, 'yesterday', label)
    # после отчётов — список «красных» кабинетов по одному сообщению
    await show_billing_list(context, CHAT_ID)

async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(timezone("Asia/Almaty")).date()
    try:
        with open(FORECAST_CACHE_FILE,"r") as f:
            cache = json.load(f)
    except:
        cache = {}
    for acc_id in get_enabled_accounts_in_order():
        try:
            acc = AdAccount(acc_id)
            info = acc.api_get(fields=["name","spend_cap","amount_spent"])
            spend_cap = float(info.get("spend_cap",0))/100
            spent = float(info.get("amount_spent",0))/100
            available = spend_cap - spent
            daily_budget = sum(
                int(c.get("daily_budget",0))/100
                for c in acc.get_campaigns(fields=["name","effective_status","daily_budget"])
                if c.get("effective_status") == "ACTIVE"
            )
            if daily_budget == 0:
                continue
            days_left = ceil(available/daily_budget)
            billing_date = today + timedelta(days=days_left)
            if (billing_date - today).days == 3:
                if cache.get(acc_id) == billing_date.isoformat():
                    continue
                name = acc_name(acc_id)
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
    with open(FORECAST_CACHE_FILE,"w") as f:
        json.dump(cache, f)

# ===== Меню / inline кнопки =====
def main_menu_kbd() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Сегодня", callback_data="menu:today"),
         InlineKeyboardButton("📅 Вчера", callback_data="menu:yesterday")],
        [InlineKeyboardButton("🗓 Прошедшая неделя", callback_data="menu:lastweek")],
        [InlineKeyboardButton("🔄 Синхронизировать кабинеты", callback_data="menu:sync")],
        [InlineKeyboardButton("🧾 Кабинеты (управление)", callback_data="menu:accounts")],
        [InlineKeyboardButton("💳 Проверить биллинги", callback_data="menu:billing")]
    ])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Показываем меню кнопок (в группе это inline-кнопки)
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🤖 Выберите действие:",
        reply_markup=main_menu_kbd()
    )

# безопасная проверка текста (если вдруг кто-то напишет словами)
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip().lower()
    if text in ("сегодня","today"):
        await on_today(update.effective_chat.id, context)
    elif text in ("вчера","yesterday"):
        await on_yesterday(update.effective_chat.id, context)
    elif text in ("прошедшая неделя","last week"):
        await on_lastweek(update.effective_chat.id, context)
    else:
        # подсказка — показать меню
        await start(update, context)

async def on_today(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    label = datetime.now().strftime('%d.%m.%Y')
    await send_report(context, chat_id, 'today', label)

async def on_yesterday(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report(context, chat_id, 'yesterday', label)

async def on_lastweek(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    until = datetime.now() - timedelta(days=1)
    since = until - timedelta(days=6)
    period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
    label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
    await send_report(context, chat_id, period, label)

# ===== Управление кабинетами =====
def account_list_kbd(page: int = 0, per_page: int = 6) -> InlineKeyboardMarkup:
    data = load_accounts()
    ids = list(data.keys())
    total = len(ids)
    start_i = page * per_page
    end_i = min(start_i + per_page, total)
    rows = []
    for aid in ids[start_i:end_i]:
        row = data[aid]
        name = row.get("name", aid)
        enabled = row.get("enabled", True)
        m = row.get("metrics", {})
        msg_on = "✅" if m.get("messaging") else "❌"
        leads_on = "✅" if m.get("leads") else "❌"
        title = f"{'🟢' if enabled else '🔴'} {name}"
        rows.append([InlineKeyboardButton(title, callback_data=f"acc:manage:{aid}")])
        rows.append([
            InlineKeyboardButton(f"{'Выкл' if enabled else 'Вкл'}", callback_data=f"acc:toggle:{aid}"),
            InlineKeyboardButton(f"✉ {msg_on}", callback_data=f"acc:metric:msg:{aid}"),
            InlineKeyboardButton(f"📩 {leads_on}", callback_data=f"acc:metric:leads:{aid}"),
        ])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("« Назад", callback_data=f"acc:page:{page-1}"))
    if end_i < total:
        nav.append(InlineKeyboardButton("Вперёд »", callback_data=f"acc:page:{page+1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu:root")])
    return InlineKeyboardMarkup(rows)

async def show_accounts(chat_id: int, context: ContextTypes.DEFAULT_TYPE, page: int = 0):
    if not load_accounts():
        # Если пусто — сначала синк
        res = upsert_accounts_from_fb()
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"Синхронизировал: добавлено {res['added']}, обновлено {res['updated']}.",
        )
    await context.bot.send_message(
        chat_id=chat_id,
        text="🧾 Управление кабинетами:",
        reply_markup=account_list_kbd(page)
    )

def toggle_enabled(aid: str):
    data = load_accounts()
    if aid in data:
        data[aid]["enabled"] = not data[aid].get("enabled", True)
        save_accounts(data)

def toggle_metric(aid: str, key: str):
    data = load_accounts()
    if aid in data:
        data[aid].setdefault("metrics", {})
        data[aid]["metrics"][key] = not data[aid]["metrics"].get(key, False)
        save_accounts(data)

# ===== Биллинги (список «красных») =====
async def show_billing_list(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    # выводим по одному сообщению на «красный» кабинет
    for aid in get_enabled_accounts_in_order():
        try:
            info = AdAccount(aid).api_get(fields=['name','account_status','balance'])
            if info.get("account_status") != 1:
                name = info.get("name") or acc_name(aid)
                # баланс приходит в центах
                bal_usd = float(info.get("balance", 0)) / 100.0
                # без автоматического курса (его ты позже вернул для другого места) — просто USD
                text = f"🔴 <b>{name}</b>\n💵 {bal_usd:.2f} $"
                await context.bot.send_message(chat_id=chat_id, text=text, parse_mode='HTML')
        except Exception:
            continue

# ===== Обработчик inline-кнопок =====
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""
    chat_id = update.effective_chat.id

    if data == "menu:root":
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=q.message.message_id,
            text="🤖 Выберите действие:",
            reply_markup=main_menu_kbd()
        )
        return

    if data == "menu:today":
        await on_today(chat_id, context); return
    if data == "menu:yesterday":
        await on_yesterday(chat_id, context); return
    if data == "menu:lastweek":
        await on_lastweek(chat_id, context); return

    if data == "menu:sync":
        res = upsert_accounts_from_fb()
        await context.bot.send_message(
            chat_id=chat_id,
            text=(f"✅ Синхронизация завершена\n"
                  f"Добавлено: {res['added']}\n"
                  f"Обновлено имён: {res['updated']}\n"
                  f"Пропущено: {res['skipped']}\n"
                  f"Всего в конфиге: {res['total']}")
        )
        await show_accounts(chat_id, context, page=0)
        return

    if data == "menu:accounts":
        await show_accounts(chat_id, context, page=0); return

    if data.startswith("acc:page:"):
        page = int(data.split(":")[2])
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=q.message.message_id,
            reply_markup=account_list_kbd(page)
        )
        return

    if data.startswith("acc:toggle:"):
        aid = data.split(":")[2]
        toggle_enabled(aid)
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=q.message.message_id,
            reply_markup=account_list_kbd(0)
        )
        return

    if data.startswith("acc:metric:"):
        _, _, which, aid = data.split(":")
        if which == "msg":
            toggle_metric(aid, "messaging")
        elif which == "leads":
            toggle_metric(aid, "leads")
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=q.message.message_id,
            reply_markup=account_list_kbd(0)
        )
        return

    if data == "menu:billing":
        await show_billing_list(context, chat_id); return

# ===== /help =====
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = (
        "Доступные действия:\n"
        "• 📊 Сегодня — отчёт за сегодня\n"
        "• 📅 Вчера — отчёт за вчера\n"
        "• 🗓 Прошедшая неделя — сводка за 7 дней\n"
        "• 🔄 Синхронизировать кабинеты — подтянуть кабинеты из БМ\n"
        "• 🧾 Кабинеты (управление) — вкл/выкл в отчёте, метрики\n"
        "• 💳 Проверить биллинги — покажет «красные» кабинеты\n\n"
        "Подсказка: все кнопки есть в /start."
    )
    await update.message.reply_text(txt)

# ===== Bootstrap =====
app = Application.builder().token(TELEGRAM_TOKEN).build()

# Кнопки и команды
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", cmd_help))
app.add_handler(CallbackQueryHandler(on_callback))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

# Джобы
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
