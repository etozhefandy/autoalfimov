import asyncio
import json
import re
from math import ceil
from datetime import datetime, timedelta, time
from typing import Dict, Any, Optional, List

from pytz import timezone

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.user import User
from facebook_business.api import FacebookAdsApi

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    filters, ContextTypes
)

# ==== КРЕДЫ ====
ACCESS_TOKEN = "EAASZCrBwhoH0BO7xBXr2h2sGTzvWzUyViJjnrXIvmI5w3uRQOszdntxDiFYxXH4hrKTmZBaPKtuthKuNx3rexRev5zAkby2XbrM5UmwzRGz8a2Q4WBDKp3d1ZCZAAhZCeWFBObQayL4XPwrOFQUtuPcGP5XVYubaXjZCsNT467yKBg90O71oVPZCbI0FrWcZAZC4GtgZDZD"
APP_ID = "1336645834088573"
APP_SECRET = "01bf23c5f726c59da318daa82dd0e9dc"
FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"
CHAT_ID = "-1002679045097"

# ===== ФАЙЛЫ =====
ACCOUNTS_JSON = "accounts.json"
FORECAST_CACHE_FILE = "forecast_cache.json"

# ==== Фоллбек, если нет accounts.json ====
AD_ACCOUNTS_FALLBACK = [
    "act_1415004142524014", "act_719853653795521", "act_1206987573792913", "act_1108417930211002",
    "act_2342025859327675", "act_844229314275496", "act_1333550570916716", "act_195526110289107",
    "act_2145160982589338", "act_1042955424178074", "act_4030694587199998", "act_508239018969999",
    "act_1357165995492721", "act_798205335840576", "act_806046635254439"
]

# Исключаем «кенсе»
EXCLUDED_AD_ACCOUNT_IDS = {"act_1042955424178074", "act_4030694587199998"}
EXCLUDED_NAME_KEYWORDS = {"kense", "кенсе"}

# Человеческие названия (резерв)
ACCOUNT_NAMES: Dict[str, str] = {
    "act_1415004142524014": "ЖС Астана",
    "act_719853653795521": "ЖС Караганда",
    "act_1206987573792913": "ЖС Павлодар",
    "act_1108417930211002": "ЖС Актау",
    "act_2342025859327675": "ЖС Атырау",
    "act_844229314275496": "ЖС Актобе",
    "act_1333550570916716": "ЖС Юг (Алматы)",
    "act_195526110289107": "ЖС Тараз",
    "act_2145160982589338": "ЖС Шымкент",
    "act_508239018969999": "Фитнес Пойнт",
    "act_1357165995492721": "Ария Степи",
    "act_798205335840576": "Инвестиции",
    "act_806046635254439": "WonderStage",
}

# ====== Хелперы по файлам ======
def load_accounts() -> Dict[str, Any]:
    try:
        with open(ACCOUNTS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}

def save_accounts(data: Dict[str, Any]):
    with open(ACCOUNTS_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def _normalize_act_id(aid: str) -> str:
    aid = str(aid).strip()
    return aid if aid.startswith("act_") else f"act_{aid}"

def _looks_excluded_by_name(name: str) -> bool:
    n = (name or "").lower()
    return any(k in n for k in EXCLUDED_NAME_KEYWORDS)

def ensure_account_row(acc_id: str):
    store = load_accounts()
    if acc_id not in store:
        store[acc_id] = {
            "name": ACCOUNT_NAMES.get(acc_id, acc_id),
            "enabled": True,
            "metrics": {"messaging": False, "leads": False},
            "alerts": {"enabled": False, "targets": {"messaging": None, "leads": None}}
        }
        save_accounts(store)

def upsert_accounts_from_fb() -> dict:
    """
    Тянем me/adaccounts, исключаем кенсе, мержим в accounts.json.
    Новые получают:
      enabled=True
      metrics.messaging/leads=False
      alerts.enabled=False; targets None
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

        if acc_id in data:
            if name and data[acc_id].get("name") != name:
                data[acc_id]["name"] = name
                updated += 1
        else:
            data[acc_id] = {
                "name": name,
                "enabled": True,
                "metrics": {"messaging": False, "leads": False},
                "alerts": {"enabled": False, "targets": {"messaging": None, "leads": None}}
            }
            added += 1

        ACCOUNT_NAMES.setdefault(acc_id, name)

    save_accounts(data)
    return {"added": added, "updated": updated, "skipped": skipped, "total": len(data)}

def get_enabled_accounts_in_order() -> List[str]:
    data = load_accounts()
    if not data:
        return AD_ACCOUNTS_FALLBACK
    return [acc_id for acc_id, row in data.items() if row.get("enabled", True)]

def get_all_accounts_in_order() -> List[str]:
    data = load_accounts()
    if not data:
        return AD_ACCOUNTS_FALLBACK
    return list(data.keys())

def get_account_name(acc_id: str) -> str:
    data = load_accounts()
    if acc_id in data and data[acc_id].get("name"):
        return data[acc_id]["name"]
    return ACCOUNT_NAMES.get(acc_id, acc_id)

# ====== Метрики и отчёты ======
account_statuses: Dict[str, int] = {}

def is_account_active(account_id):
    try:
        status = AdAccount(account_id).api_get(fields=['account_status'])['account_status']
        return "🟢" if status == 1 else "🔴"
    except:
        return "🔴"

def format_number(num):
    try:
        return f"{int(float(num)):,}".replace(",", " ")
    except:
        return "0"

def _get_actions_map(insight: Dict[str, Any]) -> Dict[str, float]:
    actions = insight.get('actions', []) or []
    out = {}
    for a in actions:
        try:
            out[a['action_type']] = float(a['value'])
        except:
            continue
    return out

def _get_cpat_map(insight: Dict[str, Any]) -> Dict[str, float]:
    # cost_per_action_type -> CPA по каждому action_type
    arr = insight.get('cost_per_action_type', []) or []
    out = {}
    for a in arr:
        try:
            out[a['action_type']] = float(a['value'])
        except:
            continue
    return out

def _flags_metrics(acc_id: str) -> Dict[str, bool]:
    store = load_accounts()
    m = (store.get(acc_id, {}).get("metrics") or {})
    return {"messaging": bool(m.get("messaging")), "leads": bool(m.get("leads"))}

def build_report_for_account(acc_id: str, period_param, date_label='') -> Optional[str]:
    account = AdAccount(acc_id)
    fields = ['impressions', 'cpm', 'clicks', 'cpc', 'spend', 'actions']
    params = {'level': 'account'}
    params.update({'time_range': period_param} if isinstance(period_param, dict) else {'date_preset': period_param})

    try:
        insights = account.get_insights(fields=fields, params=params)
        account_name = get_account_name(acc_id)
    except Exception as e:
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return None
        return f"⚠ Ошибка по {get_account_name(acc_id)}:\n\n{e}"

    date_info = f" ({date_label})" if date_label else ""
    header = f"{is_account_active(acc_id)} <b>{account_name}</b>{date_info}\n"
    if not insights:
        return header + "Нет данных за выбранный период"

    i = insights[0]
    report = (
        f"{header}"
        f"👁 Показы: {format_number(i.get('impressions', '0'))}\n"
        f"🎯 CPM: {round(float(i.get('cpm', 0) or 0), 2)} $\n"
        f"🖱 Клики: {format_number(i.get('clicks', '0'))}\n"
        f"💸 CPC: {round(float(i.get('cpc', 0) or 0), 2)} $\n"
        f"💵 Затраты: {round(float(i.get('spend', 0) or 0), 2)} $"
    )

    actions = _get_actions_map(i)
    flags = _flags_metrics(acc_id)

    if flags["messaging"]:
        conv = actions.get('onsite_conversion.messaging_conversation_started_7d', 0.0)
        report += f"\n✉️ Начата переписка: {int(conv)}"
        if conv > 0:
            spend = float(i.get('spend', 0) or 0)
            report += f"\n💬💲 Цена переписки: {round(spend / conv, 2)} $"

    if flags["leads"]:
        leads = (
            actions.get('lead', 0.0)
            or actions.get('offsite_conversion.fb_pixel_lead', 0.0)
            or actions.get('offsite_conversion.fb_pixel_submit_application', 0.0)
        )
        report += f"\n📩 Заявки: {int(leads)}"
        if leads > 0:
            spend = float(i.get('spend', 0) or 0)
            report += f"\n📩💲 Цена заявки: {round(spend / leads, 2)} $"

    return report

async def send_report_all(context: ContextTypes.DEFAULT_TYPE, chat_id, period, label=''):
    for acc in get_enabled_accounts_in_order():
        msg = build_report_for_account(acc, period, label)
        if msg:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')

# ====== Плановые задачи (старое) ======
async def check_billing(context: ContextTypes.DEFAULT_TYPE):
    global account_statuses
    for account_id in get_enabled_accounts_in_order():
        try:
            account = AdAccount(account_id)
            info = account.api_get(fields=['name', 'account_status', 'balance'])
            status = info.get('account_status')
            if account_id in account_statuses and account_statuses[account_id] == 1 and status != 1:
                name = info.get('name') or get_account_name(account_id)
                balance = float(info.get('balance', 0) or 0) / 100
                await context.bot.send_message(
                    chat_id=CHAT_ID,
                    text=f"⚠️ ⚠️ ⚠️ Ахтунг! {name}! у нас биллинг - {balance:.2f} $",
                    parse_mode='HTML'
                )
            account_statuses[account_id] = status
        except Exception:
            continue

async def daily_report(context: ContextTypes.DEFAULT_TYPE):
    label = (datetime.now(timezone('Asia/Almaty')) - timedelta(days=1)).strftime('%d.%m.%Y')
    await send_report_all(context, CHAT_ID, 'yesterday', label)

async def check_billing_forecast(context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now(timezone("Asia/Almaty")).date()
    try:
        with open(FORECAST_CACHE_FILE, "r") as f:
            cache = json.load(f)
    except:
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
            days_left = ceil(available / daily_budget)
            billing_date = today + timedelta(days=days_left)
            if (billing_date - today).days == 3:
                if cache.get(acc_id) == billing_date.isoformat():
                    continue
                name = get_account_name(acc_id)
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

# ====== Оповещения по целевой цене (CPA) ======

ALERT_TIMES = [
    time(hour=11, minute=0, tzinfo=timezone('Asia/Almaty')),
    time(hour=14, minute=0, tzinfo=timezone('Asia/Almaty')),
    time(hour=17, minute=0, tzinfo=timezone('Asia/Almaty')),
    time(hour=19, minute=0, tzinfo=timezone('Asia/Almaty')),
]

# action keys
A_MSG = 'onsite_conversion.messaging_conversation_started_7d'
A_LEAD_PRI = 'lead'
A_LEAD_PX = 'offsite_conversion.fb_pixel_lead'
A_LEAD_PX_SUBMIT = 'offsite_conversion.fb_pixel_submit_application'

def _get_target_config(acc_id: str) -> Dict[str, Any]:
    store = load_accounts()
    row = store.get(acc_id, {})
    alerts = row.get("alerts", {}) or {}
    targets = alerts.get("targets", {}) or {}
    return {
        "enabled": bool(alerts.get("enabled", False)),
        "t_msg": targets.get("messaging"),  # float | None
        "t_lead": targets.get("leads"),     # float | None
    }

def _first_present(keys: List[str], mapping: Dict[str, float]) -> Optional[float]:
    for k in keys:
        if k in mapping:
            return mapping[k]
    return None

async def check_cpa_alerts(context: ContextTypes.DEFAULT_TYPE):
    """
    Проверяем сегодня (date_preset='today') на уровне adset:
      - cost_per_action_type для нужных action_type
      - если CPA > целевого — шлём алерт по каждому проблемному адсету
    """
    for acc_id in get_enabled_accounts_in_order():
        cfg = _get_target_config(acc_id)
        flags = _flags_metrics(acc_id)
        if not cfg["enabled"]:
            continue
        # если ни одна метрика не включена и/или нет целей — пропускаем
        need_msg = flags["messaging"] and isinstance(cfg["t_msg"], (int, float))
        need_lead = flags["leads"] and isinstance(cfg["t_lead"], (int, float))
        if not (need_msg or need_lead):
            continue

        try:
            acc = AdAccount(acc_id)
            fields = [
                'campaign_name', 'adset_name',
                'actions', 'cost_per_action_type'
            ]
            params = {'level': 'adset', 'date_preset': 'today'}
            insights = list(acc.get_insights(fields=fields, params=params))
        except Exception as e:
            # нет прав — игнорим
            continue

        if not insights:
            continue

        acc_name = get_account_name(acc_id)
        chunks = []
        for ins in insights:
            actions = _get_actions_map(ins)
            cpat = _get_cpat_map(ins)

            # переписки
            if need_msg:
                cpa_msg = cpat.get(A_MSG)
                conv = actions.get(A_MSG, 0.0)
                if cpa_msg is not None and conv and cpa_msg > float(cfg["t_msg"]):
                    chunks.append(
                        f"🔔 <b>{acc_name}</b>\n"
                        f"📣 {ins.get('campaign_name','')}\n"
                        f"📦 {ins.get('adset_name','')}\n"
                        f"💬 CPA переписки: <b>{cpa_msg:.2f}$</b> (цель {cfg['t_msg']:.2f}$), кол-во {int(conv)}"
                    )

            # лиды
            if need_lead:
                # берём первый из известных
                cpa_lead = _first_present([A_LEAD_PRI, A_LEAD_PX, A_LEAD_PX_SUBMIT], cpat)
                conv_lead = _first_present([A_LEAD_PRI, A_LEAD_PX, A_LEAD_PX_SUBMIT], actions) or 0.0
                if cpa_lead is not None and conv_lead and cpa_lead > float(cfg["t_lead"]):
                    chunks.append(
                        f"🔔 <b>{acc_name}</b>\n"
                        f"📣 {ins.get('campaign_name','')}\n"
                        f"📦 {ins.get('adset_name','')}\n"
                        f"📩 CPA лида: <b>{cpa_lead:.2f}$</b> (цель {cfg['t_lead']:.2f}$), кол-во {int(conv_lead)}"
                    )

        for m in chunks:
            await context.bot.send_message(chat_id=CHAT_ID, text=m, parse_mode='HTML')

# ====== Меню/кнопки ======
def main_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("📋 Отчёт по всем", callback_data="menu_report_all"),
            InlineKeyboardButton("📊 Отчёт по аккаунту", callback_data="menu_report_by")
        ],
        [
            InlineKeyboardButton("⚙️ Настройки (метрики/кабинеты/опов.)", callback_data="menu_settings"),
        ],
        [
            InlineKeyboardButton("🔁 Синхронизация аккаунтов", callback_data="menu_sync"),
        ],
    ]
    return InlineKeyboardMarkup(rows)

def period_menu_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Сегодня", callback_data=f"period|{acc_id}|today"),
            InlineKeyboardButton("Вчера", callback_data=f"period|{acc_id}|yesterday"),
        ],
        [
            InlineKeyboardButton("Прошлая неделя", callback_data=f"period|{acc_id}|last7"),
            InlineKeyboardButton("🗓 Свой период", callback_data=f"period|{acc_id}|custom"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu_report_by")]
    ])

def accounts_list_kb(prefix: str) -> InlineKeyboardMarkup:
    """
    prefix: choose_acc_for_report | choose_acc_for_settings
    показываем ВСЕ известные аккаунты (не только enabled),
    чтобы можно было включать/выключать.
    """
    buttons = []
    for acc_id in get_all_accounts_in_order():
        name = get_account_name(acc_id)
        buttons.append([InlineKeyboardButton(name, callback_data=f"{prefix}|{acc_id}")])
    buttons.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu_back")])
    return InlineKeyboardMarkup(buttons)

def settings_kb(acc_id: str) -> InlineKeyboardMarkup:
    store = load_accounts()
    row = store.get(acc_id, {})
    enabled = bool(row.get("enabled", True))
    metrics = row.get("metrics", {}) or {}
    alerts = row.get("alerts", {}) or {}
    m_on = "✅" if metrics.get("messaging") else "❌"
    l_on = "✅" if metrics.get("leads") else "❌"
    al_on = "✅" if alerts.get("enabled") else "❌"
    t_msg = alerts.get("targets", {}).get("messaging")
    t_lead = alerts.get("targets", {}).get("leads")
    t_msg_txt = f"{t_msg:.2f}$" if isinstance(t_msg, (int, float)) else "не задано"
    t_lead_txt = f"{t_lead:.2f}$" if isinstance(t_lead, (int, float)) else "не задано"

    en_btn = "🔴 Выключить кабинет" if enabled else "🟢 Включить кабинет"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(en_btn, callback_data=f"set_toggle_enabled|{acc_id}")],
        [InlineKeyboardButton(f"💬 Переписки: {m_on}", callback_data=f"set_toggle|{acc_id}|messaging")],
        [InlineKeyboardButton(f"♿️ Лид с сайта: {l_on}", callback_data=f"set_toggle|{acc_id}|leads")],
        [InlineKeyboardButton(f"⚠️ Оповещения: {al_on}", callback_data=f"alerts_toggle|{acc_id}")],
        [InlineKeyboardButton(f"🎯 Цель переписки: {t_msg_txt}", callback_data=f"alerts_set|{acc_id}|messaging")],
        [InlineKeyboardButton(f"🎯 Цель лида: {t_lead_txt}", callback_data=f"alerts_set|{acc_id}|leads")],
        [InlineKeyboardButton("⬅️ Выбор аккаунта", callback_data="menu_settings")]
    ])

# ====== Команды ======
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Главное меню:", reply_markup=main_menu_kb())

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Главное меню:", reply_markup=main_menu_kb())

# ====== Текстовые сообщения (для «свой период» и ввода целей) ======
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return
    text = update.message.text.strip()
    ud = context.user_data

    # Шаги: запрос дат для «свой период»
    if ud.get("await_custom_from"):
        m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", text)
        if not m:
            await update.message.reply_text("Формат даты должен быть ДД.ММ.ГГГГ. Попробуйте ещё раз:")
            return
        d, mth, y = map(int, m.groups())
        try:
            ud["custom_from"] = datetime(y, mth, d)
            ud.pop("await_custom_from", None)
            ud["await_custom_to"] = True
            await update.message.reply_text("Ок. Теперь введите дату «по» (ДД.ММ.ГГГГ):")
        except:
            await update.message.reply_text("Неверная дата. Попробуйте ещё раз:")
        return

    if ud.get("await_custom_to"):
        m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", text)
        if not m:
            await update.message.reply_text("Формат даты должен быть ДД.ММ.ГГГГ. Попробуйте ещё раз:")
            return
        d, mth, y = map(int, m.groups())
        try:
            to_dt = datetime(y, mth, d)
            from_dt = ud.get("custom_from")
            acc_id = ud.get("custom_acc")
            if not (from_dt and acc_id):
                await update.message.reply_text("Что-то пошло не так, откройте меню заново.")
            else:
                if to_dt < from_dt:
                    await update.message.reply_text("Дата «по» раньше «с». Начните заново.")
                else:
                    since = from_dt.strftime("%Y-%m-%d")
                    until = to_dt.strftime("%Y-%m-%d")
                    label = f"{from_dt.strftime('%d.%m.%Y')}–{to_dt.strftime('%d.%m.%Y')}"
                    period = {"since": since, "until": until}
                    msg = build_report_for_account(acc_id, period, label)
                    if msg:
                        await update.message.reply_text(msg, parse_mode='HTML')
            for k in ("await_custom_to", "custom_from", "custom_acc"):
                ud.pop(k, None)
        except:
            await update.message.reply_text("Неверная дата. Попробуйте ещё раз:")
        return

    # Шаги: ввод целевой цены для алертов
    if ud.get("await_target_for"):
        acc_id = ud.get("await_target_acc")
        metric = ud.get("await_target_for")
        if not re.match(r"^\d+(\.\d+)?$", text.replace(",", ".")):
            await update.message.reply_text("Введите число, например 1.5")
            return
        val = float(text.replace(",", "."))
        store = load_accounts()
        row = store.get(acc_id, {})
        alerts = row.get("alerts", {}) or {"enabled": False, "targets": {"messaging": None, "leads": None}}
        targets = alerts.get("targets", {})
        if metric == "messaging":
            targets["messaging"] = val
        else:
            targets["leads"] = val
        alerts["targets"] = targets
        row["alerts"] = alerts
        store[acc_id] = row
        save_accounts(store)
        ud.pop("await_target_for", None)
        ud.pop("await_target_acc", None)
        await update.message.reply_text("Целевая стоимость обновлена.")
        return

# ====== Callback-кнопки ======
async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data or ""

    if data == "menu_back" or data == "menu":
        await q.edit_message_text("Главное меню:", reply_markup=main_menu_kb())
        return

    if data == "menu_report_all":
        label = datetime.now().strftime('%d.%m.%Y')
        await q.edit_message_text("Отчёт по всем аккаунтам (сегодня).")
        await send_report_all(context, q.message.chat_id, 'today', label)
        return

    if data == "menu_report_by":
        await q.edit_message_text("Выберите аккаунт:", reply_markup=accounts_list_kb("choose_acc_for_report"))
        return

    if data.startswith("choose_acc_for_report|"):
        acc_id = data.split("|", 1)[1]
        ensure_account_row(acc_id)
        await q.edit_message_text(f"Аккаунт: {get_account_name(acc_id)}\nВыберите период:",
                                  reply_markup=period_menu_kb(acc_id))
        return

    if data.startswith("period|"):
        _, acc_id, kind = data.split("|", 2)
        if kind == "today":
            label = datetime.now().strftime('%d.%m.%Y')
            msg = build_report_for_account(acc_id, 'today', label)
            await q.edit_message_text(msg or "Нет данных или доступа.", parse_mode='HTML' if msg else None)
            return
        if kind == "yesterday":
            label = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
            msg = build_report_for_account(acc_id, 'yesterday', label)
            await q.edit_message_text(msg or "Нет данных или доступа.", parse_mode='HTML' if msg else None)
            return
        if kind == "last7":
            until = datetime.now() - timedelta(days=1)
            since = until - timedelta(days=6)
            period = {'since': since.strftime('%Y-%m-%d'), 'until': until.strftime('%Y-%m-%d')}
            label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
            msg = build_report_for_account(acc_id, period, label)
            await q.edit_message_text(msg or "Нет данных или доступа.", parse_mode='HTML' if msg else None)
            return
        if kind == "custom":
            context.user_data["await_custom_from"] = True
            context.user_data["custom_acc"] = acc_id
            await q.edit_message_text(
                f"Аккаунт: {get_account_name(acc_id)}\nВведите дату «с» в формате ДД.ММ.ГГГГ:"
            )
            return

    # ===== Настройки =====
    if data == "menu_settings":
        await q.edit_message_text("Выберите аккаунт для настройки:",
                                  reply_markup=accounts_list_kb("choose_acc_for_settings"))
        return

    if data.startswith("choose_acc_for_settings|"):
        acc_id = data.split("|", 1)[1]
        ensure_account_row(acc_id)
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}",
                                  reply_markup=settings_kb(acc_id))
        return

    if data.startswith("set_toggle_enabled|"):
        _, acc_id = data.split("|", 1)
        store = load_accounts()
        row = store.get(acc_id, {})
        row["enabled"] = not bool(row.get("enabled", True))
        store[acc_id] = row
        save_accounts(store)
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}", reply_markup=settings_kb(acc_id))
        return

    if data.startswith("set_toggle|"):
        _, acc_id, metric = data.split("|", 2)
        store = load_accounts()
        row = store.get(acc_id, {})
        m = row.get("metrics", {}) or {}
        if metric not in ("messaging", "leads"):
            await q.answer("Неизвестная метрика")
            return
        m[metric] = not bool(m.get(metric, False))
        row["metrics"] = m
        store[acc_id] = row
        save_accounts(store)
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}", reply_markup=settings_kb(acc_id))
        return

    if data.startswith("alerts_toggle|"):
        _, acc_id = data.split("|", 1)
        store = load_accounts()
        row = store.get(acc_id, {})
        al = row.get("alerts", {}) or {"enabled": False, "targets": {"messaging": None, "leads": None}}
        al["enabled"] = not bool(al.get("enabled", False))
        row["alerts"] = al
        store[acc_id] = row
        save_accounts(store)
        await q.edit_message_text(f"Настройки: {get_account_name(acc_id)}", reply_markup=settings_kb(acc_id))
        return

    if data.startswith("alerts_set|"):
        _, acc_id, metric = data.split("|", 2)
        # просим ввести число
        context.user_data["await_target_for"] = metric
        context.user_data["await_target_acc"] = acc_id
        pretty = "переписки" if metric == "messaging" else "лида"
        await q.edit_message_text(f"Введите целевую стоимость {pretty} в $ (например 1.5):")
        return

    # ===== Синхронизация =====
    if data == "menu_sync":
        try:
            res = upsert_accounts_from_fb()
            msg = (
                "✅ Синхронизация завершена\n"
                f"Добавлено: {res['added']}\n"
                f"Обновлено имён: {res['updated']}\n"
                f"Пропущено (исключено): {res['skipped']}\n"
                f"Итого в конфиге: {res['total']}"
            )
        except Exception as e:
            msg = f"⚠️ Ошибка синхронизации: {e}"
        await q.edit_message_text(msg, reply_markup=main_menu_kb())
        return

# ====== BOOTSTRAP ======
app = Application.builder().token(TELEGRAM_TOKEN).build()

# Команды
app.add_handler(CommandHandler("start", cmd_start))
app.add_handler(CommandHandler("menu", cmd_menu))

# Кнопки-инлайн
app.add_handler(CallbackQueryHandler(on_callback))

# Текст (ввод дат и целевых значений)
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

# Планировщики
app.job_queue.run_repeating(check_billing, interval=600, first=10)
app.job_queue.run_daily(daily_report, time=time(hour=9, minute=30, tzinfo=timezone('Asia/Almaty')))
app.job_queue.run_daily(check_billing_forecast, time=time(hour=9, minute=0, tzinfo=timezone('Asia/Almaty')))

# Оповещения CPA по расписанию
for t in ALERT_TIMES:
    app.job_queue.run_daily(check_cpa_alerts, time=t)

if __name__ == "__main__":
    print("\U0001F680 Бот запущен и ожидает команд.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
