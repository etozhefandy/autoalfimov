from datetime import datetime, timedelta, time

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

from billing_watch import init_billing_watch

from .constants import (
    ALMATY_TZ,
    TELEGRAM_TOKEN,
    DEFAULT_REPORT_CHAT,
    ALLOWED_USER_IDS,
    ALLOWED_CHAT_IDS,
    usd_to_kzt,
    kzt_round_up_1000,
)
from .storage import (
    load_accounts,
    save_accounts,
    get_account_name,
    get_enabled_accounts_in_order,
    human_last_sync,
    upsert_from_bm,
)
from .reporting import (
    get_cached_report,
    build_comparison_report,
    send_period_report,
    parse_range,
    parse_two_ranges,
)
from .insights import build_heatmap_for_account
from .adsets import send_adset_report
from .billing import send_billing, send_billing_forecast, billing_digest_job
from .jobs import full_daily_scan_job, daily_report_job, schedule_cpa_alerts

# === АВТОПИЛАТ ===
from autopilat.engine import get_recommendations_ui
from autopilat.ui import (
    autopilot_main_menu,
    autopilot_submode_menu,
    confirm_action_buttons,
)
from autopilat.actions import (
    apply_budget_change,
    disable_entity,
    parse_manual_input,
    can_disable,
)


# ============ PRIVACY ============
def _allowed(update: Update) -> bool:
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    user_id = update.effective_user.id if update.effective_user else None
    if chat_id in ALLOWED_CHAT_IDS:
        return True
    if user_id and user_id in ALLOWED_USER_IDS:
        return True
    return False


# ======= SAFE EDIT =======
async def safe_edit_message(q, text: str, **kwargs):
    try:
        return await q.edit_message_text(text=text, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise


# ============ UI ============
def main_menu() -> InlineKeyboardMarkup:
    last_sync = human_last_sync()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Отчёт по всем", callback_data="rep_all_menu")],
            [InlineKeyboardButton("Биллинг", callback_data="billing")],
            [
                InlineKeyboardButton(
                    "Отчёт по аккаунту", callback_data="choose_acc_report"
                )
            ],
            [
                InlineKeyboardButton("Тепловая карта", callback_data="hm_menu")
            ],
            [
                InlineKeyboardButton("Настройки", callback_data="choose_acc_settings")
            ],
            [
                InlineKeyboardButton("🤖 Автопилат", callback_data="ap_main")
            ],
            [
                InlineKeyboardButton(
                    f"Синк BM (посл. {last_sync})",
                    callback_data="sync_bm",
                )
            ],
        ]
    )


def billing_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Текущие биллинги", callback_data="billing_current")],
            [InlineKeyboardButton("Прогноз списаний", callback_data="billing_forecast")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )


def all_reports_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data="rep_today"),
                InlineKeyboardButton("Вчера", callback_data="rep_yday"),
            ],
            [InlineKeyboardButton("Прошедшая неделя", callback_data="rep_week")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )


def heatmap_menu(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("7 дней", callback_data=f"hm7|{aid}"),
                InlineKeyboardButton("14 дней", callback_data=f"hm14|{aid}"),
            ],
            [
                InlineKeyboardButton(
                    "Текущий месяц", callback_data=f"hmmonth|{aid}"
                )
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu")],
        ]
    )


def _flag_line(aid: str) -> str:
    st = load_accounts().get(aid, {})
    enabled = st.get("enabled", True)
    m = st.get("metrics", {}) or {}
    a = st.get("alerts", {}) or {}
    on = "🟢" if enabled else "🔴"
    mm = "💬" if m.get("messaging") else ""
    ll = "♿️" if m.get("leads") else ""
    aa = "⚠️" if a.get("enabled") and (a.get("target_cpl", 0) or 0) > 0 else ""
    return f"{on} {mm}{ll}{aa}".strip()


def accounts_kb(prefix: str) -> InlineKeyboardMarkup:
    store = load_accounts()
    if store:
        enabled_ids = [aid for aid, row in store.items() if row.get("enabled", True)]
        disabled_ids = [aid for aid, row in store.items() if not row.get("enabled", True)]
        ids = enabled_ids + disabled_ids
    else:
        from .constants import AD_ACCOUNTS_FALLBACK

        ids = AD_ACCOUNTS_FALLBACK

    rows = []
    for aid in ids:
        rows.append(
            [
                InlineKeyboardButton(
                    f"{_flag_line(aid)}  {get_account_name(aid)}",
                    callback_data=f"{prefix}|{aid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu")])
    return InlineKeyboardMarkup(rows)


def settings_kb(aid: str) -> InlineKeyboardMarkup:
    st = load_accounts().get(aid, {"enabled": True, "metrics": {}, "alerts": {}})
    en_text = "Выключить кабинет" if st.get("enabled", True) else "Включить кабинет"
    m_on = st.get("metrics", {}).get("messaging", True)
    l_on = st.get("metrics", {}).get("leads", False)
    a_on = st.get("alerts", {}).get("enabled", False) and (
        st.get("alerts", {}).get("target_cpl", 0) or 0
    ) > 0
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(en_text, callback_data=f"toggle_enabled|{aid}")],
            [
                InlineKeyboardButton(
                    f"💬 Переписки: {'ON' if m_on else 'OFF'}",
                    callback_data=f"toggle_m|{aid}",
                ),
                InlineKeyboardButton(
                    f"♿️ Лиды сайта: {'ON' if l_on else 'OFF'}",
                    callback_data=f"toggle_l|{aid}",
                ),
            ],
            [
                InlineKeyboardButton(
                    f"⚠️ Алерт CPA: {'ON' if a_on else 'OFF'}",
                    callback_data=f"toggle_alert|{aid}",
                )
            ],
            [
                InlineKeyboardButton(
                    "✏️ Задать target CPA", callback_data=f"set_cpa|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Назад к списку",
                    callback_data="choose_acc_settings",
                )
            ],
        ]
    )


def period_kb_for(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"one_today|{aid}"),
                InlineKeyboardButton("Вчера", callback_data=f"one_yday|{aid}"),
            ],
            [InlineKeyboardButton("Прошедшая неделя", callback_data=f"one_week|{aid}")],
            [
                InlineKeyboardButton(
                    "Сравнить периоды", callback_data=f"cmp_menu|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "🗓 Свой диапазон", callback_data=f"one_custom|{aid}"
                )
            ],
            [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="choose_acc_report")],
        ]
    )


def compare_kb_for(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Эта неделя vs прошлая", callback_data=f"cmp_week|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Два диапазона", callback_data=f"cmp_custom|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ К периодам", callback_data=f"back_periods|{aid}"
                )
            ],
        ]
    )


def account_report_kind_kb(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Отчёт по аккаунту", callback_data=f"rep_acc|{aid}"
                )
            ],
            [
                InlineKeyboardButton(
                    "Отчёт по адсетам", callback_data=f"rep_adsets|{aid}"
                )
            ],
            [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="choose_acc_report")],
        ]
    )


# ======== COMMANDS ============
async def cmd_whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    user_id = update.effective_user.id if update.effective_user else None
    await update.message.reply_text(
        f"user_id: <code>{user_id}</code>\nchat_id: <code>{chat_id}</code>",
        parse_mode="HTML",
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=(
                "⛔️ Нет доступа. Отправь /whoami и добавь свой user_id "
                "в ALLOWED_USER_IDS."
            ),
        )
        return
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="🤖 Выберите действие:",
        reply_markup=main_menu(),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    txt = (
        "Команды:\n"
        "/start — главное меню\n"
        "/help — список всех команд\n"
        "/billing — биллинги и прогнозы\n"
        "/sync_accounts — синхронизация BM\n"
        "/whoami — показать user_id/chat_id\n"
        "/heatmap <act_id> — тепловая карта адсетов за 7 дней\n"
        "\n"
        "🚀 Функции автопилата:\n"
        "• Автоматические рекомендации по аккаунту\n"
        "• Изменение бюджета (-20%, +20%, ручной ввод)\n"
        "• Безопасное отключение дорогих адсетов\n"
        "• Подготовка к ИИ-управлению (Пилат)\n"
    )
    await update.message.reply_text(txt, reply_markup=ReplyKeyboardRemove())


async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    await update.message.reply_text(
        "Что показать по биллингу?", reply_markup=billing_menu()
    )


async def cmd_heatmap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return

    parts = update.message.text.strip().split()

    if len(parts) == 1:
        await update.message.reply_text(
            "Выберите аккаунт для тепловой карты:",
            reply_markup=accounts_kb("hmacc"),
        )
        return

    aid = parts[1].strip()
    if not aid.startswith("act_"):
        aid = "act_" + aid

    context.user_data["heatmap_aid"] = aid

    await update.message.reply_text(
        f"Выберите период тепловой карты для {get_account_name(aid)}:",
        reply_markup=heatmap_menu(aid),
    )


async def cmd_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    try:
        res = upsert_from_bm()
        last_sync_h = human_last_sync()
        await update.message.reply_text(
            f"✅ Синк завершён. Добавлено: {res['added']}, "
            f"обновлено: {res['updated']}, пропущено: {res['skipped']}. "
            f"Всего: {res['total']}\n"
            f"🕓 Последняя синхронизация: {last_sync_h}"
        )
    except Exception as e:
        await update.message.reply_text(f"⚠️ Ошибка синка: {e}")


# ============ CALLBACKS ДЛЯ АВТОПИЛАТА ============
async def on_cb_autopilot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if not _allowed(update):
        await q.edit_message_text("⛔️ Нет доступа.")
        return

    data = q.data or ""
    chat_id = str(q.message.chat.id)

    if data == "ap_main":
        await q.edit_message_text(
            "Выберите режим автопилата:",
            reply_markup=autopilot_main_menu()
        )
        return

    if data.startswith("apmode|"):
        mode = data.split("|", 1)[1]
        context.user_data["autopilot_mode"] = mode

        await q.edit_message_text(
            f"Режим: <b>{mode}</b>\nВыберите подрежим:",
            parse_mode="HTML",
            reply_markup=autopilot_submode_menu()
        )
        return

    if data.startswith("apsub|"):
        sub = data.split("|", 1)[1]
        context.user_data["autopilot_submode"] = sub

        await q.edit_message_text(
            f"Режим: <b>{context.user_data.get('autopilot_mode')}</b>\n"
            f"Подрежим: <b>{sub}</b>\n\n"
            f"Теперь выберите аккаунт:",
            parse_mode="HTML",
            reply_markup=accounts_kb("ap_acc")
        )
        return

    if data.startswith("ap_acc|"):
        aid = data.split("|", 1)[1]
        context.user_data["ap_aid"] = aid

        ui = get_recommendations_ui(aid)
        text = f"🔍 <b>Рекомендации по {get_account_name(aid)}</b>\n\n{ui['text']}"
        await q.edit_message_text(text, parse_mode="HTML")

        from autopilat.ui import build_recommendations_ui

        blocks = build_recommendations_ui(ui["items"])
        for block in blocks:
            await context.bot.send_message(
                chat_id,
                block["text"],
                parse_mode="HTML",
                reply_markup=block["reply_markup"]
            )
        return

    if data.startswith("ap|"):
        parts = data.split("|")
        if len(parts) < 2:
            await q.edit_message_text(
                "⚠ Ошибка кнопки: некорректный формат callback_data.",
                parse_mode="HTML",
            )
            return

        _, action, *rest = parts
        entity_id = rest[0] if rest else ""

        if action == "back":
            await q.edit_message_text(
                "Выберите режим автопилата:",
                reply_markup=autopilot_main_menu(),
            )
            return

        if not entity_id:
            await q.edit_message_text(
                "⚠ Ошибка кнопки: не передан ID сущности.\n"
                "Обнови рекомендации и попробуй ещё раз.",
                parse_mode="HTML",
            )
            return

        if action == "manual":
            context.user_data["await_manual_input"] = entity_id
            await q.edit_message_text(
                f"✍️ Введите число (например 1.2, -20, 15):\n"
                f"ID: <code>{entity_id}</code>",
                parse_mode="HTML",
            )
            return

        await q.edit_message_text(
            f"Подтвердить действие <b>{action}</b> для <code>{entity_id}</code>?",
            parse_mode="HTML",
            reply_markup=confirm_action_buttons(action, entity_id),
        )
        return

    if data.startswith("apconfirm|"):
        _, yesno, action, entity_id = data.split("|", 3)

        if yesno == "no":
            await q.edit_message_text("Операция отменена.", parse_mode="HTML")
            return

        if action in ("up20", "down20"):
            percent = 20 if action == "up20" else -20
            res = apply_budget_change(entity_id, percent)
            await q.edit_message_text(res["message"], parse_mode="HTML")
            return

        if action == "off":
            aid = context.user_data.get("ap_aid")
            if aid and not can_disable(aid, entity_id):
                await q.edit_message_text(
                    "❌ Нельзя отключить этот адсет — иначе весь аккаунт останется без трафика.",
                    parse_mode="HTML"
                )
                return

            res = disable_entity(entity_id)
            await q.edit_message_text(res["message"], parse_mode="HTML")
            return

        try:
            percent = float(action.replace(",", "."))
        except Exception:
            await q.edit_message_text(
                "⚠ Не получилось прочитать процент изменения.",
                parse_mode="HTML"
            )
            return

        res = apply_budget_change(entity_id, percent)
        await q.edit_message_text(res["message"], parse_mode="HTML")
        return


# ============ ОБЩИЕ CALLBACK'и ============
async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not _allowed(update):
        await q.edit_message_text("⛔️ Нет доступа.")
        return

    data = q.data or ""
    chat_id = str(q.message.chat.id)

    if data == "menu":
        await q.edit_message_text("🤖 Выберите действие:", reply_markup=main_menu())
        return

    if data == "rep_all_menu":
        await q.edit_message_text("Выберите период:", reply_markup=all_reports_menu())
        return

    # ===== ОТЧЁТ ПО АДСЕТАМ (из главного меню) =====
    if data == "adsets_menu":
        await q.edit_message_text(
            "Выберите аккаунт для отчёта по адсетам:",
            reply_markup=accounts_kb("adrep"),
        )
        return

    if data.startswith("adrep|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Готовлю отчёт по адсетам для {get_account_name(aid)} "
            f"за последние 7 дней…"
        )
        await send_adset_report(context, chat_id, aid)
        return

    # ===== ОБЩИЕ ОТЧЁТЫ =====
    if data == "rep_today":
        label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, chat_id, "today", label)
        return

    if data == "rep_yday":
        label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, chat_id, "yesterday", label)
        return

    if data == "rep_week":
        until = datetime.now(ALMATY_TZ) - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, chat_id, period, label)
        return

    # ===== ТЕПЛОВАЯ КАРТА =====
    if data == "hm_menu":
        await q.edit_message_text(
            "Выберите аккаунт для тепловой карты:",
            reply_markup=accounts_kb("hmacc"),
        )
        return

    if data.startswith("hmacc|"):
        aid = data.split("|", 1)[1]
        context.user_data["heatmap_aid"] = aid
        await q.edit_message_text(
            f"Выберите период тепловой карты для {get_account_name(aid)}:",
            reply_markup=heatmap_menu(aid),
        )
        return

    if data.startswith("hm7|"):
        aid = data.split("|")[1]
        heat = build_heatmap_for_account(aid, get_account_name, mode="7")
        await q.edit_message_text(heat, parse_mode="HTML")
        return

    if data.startswith("hm14|"):
        aid = data.split("|")[1]
        heat = build_heatmap_for_account(aid, get_account_name, mode="14")
        await q.edit_message_text(heat, parse_mode="HTML")
        return

    if data.startswith("hmmonth|"):
        aid = data.split("|")[1]
        heat = build_heatmap_for_account(aid, get_account_name, mode="month")
        await q.edit_message_text(heat, parse_mode="HTML")
        return

    # ===== БИЛЛИНГ =====
    if data == "billing":
        await q.edit_message_text(
            "Что показать по биллингу?", reply_markup=billing_menu()
        )
        return
    if data == "billing_current":
        await q.edit_message_text("📋 Биллинги (неактивные аккаунты):")
        await send_billing(context, chat_id)
        return
    if data == "billing_forecast":
        await q.edit_message_text("🔮 Считаю прогноз списаний…")
        await send_billing_forecast(context, chat_id)
        return

    # ===== СИНК BM =====
    if data == "sync_bm":
        try:
            res = upsert_from_bm()
            last_sync_h = human_last_sync()
            await q.edit_message_text(
                f"✅ Синк завершён. Добавлено: {res['added']}, "
                f"обновлено: {res['updated']}, пропущено: {res['skipped']}. "
                f"Всего: {res['total']}\n"
                f"🕓 Последняя синхронизация: {last_sync_h}",
                reply_markup=main_menu(),
            )
        except Exception as e:
            await q.edit_message_text(
                f"⚠️ Ошибка синка: {e}", reply_markup=main_menu()
            )
        return

    # ===== ОТЧЁТ ПО АККАУНТУ (ИНДИВИДУАЛЬНЫЙ) =====
    if data == "choose_acc_report":
        await q.edit_message_text(
            "Выберите аккаунт:", reply_markup=accounts_kb("rep_choose")
        )
        return

    if data.startswith("rep_choose|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Отчёт индивидуальный:\n"
            f"Выберите вид отчёта для {get_account_name(aid)}:",
            reply_markup=account_report_kind_kb(aid),
        )
        return

    if data.startswith("rep_adsets|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Готовлю отчёт по адсетам для {get_account_name(aid)} "
            f"за последние 7 дней…"
        )
        await send_adset_report(context, chat_id, aid)
        return

    if data.startswith("rep_acc|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Отчёт по: {get_account_name(aid)}\nВыбери период:",
            reply_markup=period_kb_for(aid),
        )
        return

    if data.startswith("one_today|"):
        aid = data.split("|", 1)[1]
        label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        await q.edit_message_text(
            f"Отчёт по {get_account_name(aid)} за {label}:"
        )
        txt = get_cached_report(aid, "today", label)
        await context.bot.send_message(
            chat_id,
            txt or "Нет данных/нет доступа.",
            parse_mode="HTML",
        )
        return

    if data.startswith("one_yday|"):
        aid = data.split("|", 1)[1]
        label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
        await q.edit_message_text(
            f"Отчёт по {get_account_name(aid)} за {label}:"
        )
        txt = get_cached_report(aid, "yesterday", label)
        await context.bot.send_message(
            chat_id,
            txt or "Нет данных/нет доступа.",
            parse_mode="HTML",
        )
        return

    if data.startswith("one_week|"):
        aid = data.split("|", 1)[1]
        until = datetime.now(ALMATY_TZ) - timedelta(days=1)
        since = until - timedelta(days=6)
        period = {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        }
        label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
        await q.edit_message_text(
            f"Отчёт по {get_account_name(aid)} за {label}:"
        )
        txt = get_cached_report(aid, period, label)
        await context.bot.send_message(
            chat_id,
            txt or "Нет данных/нет доступа.",
            parse_mode="HTML",
        )
        return

    if data.startswith("one_custom|"):
        aid = data.split("|", 1)[1]
        context.user_data["await_range_for"] = aid
        await q.edit_message_text(
            f"Введи даты для {get_account_name(aid)} форматом: 01.06.2025-07.06.2025",
            reply_markup=period_kb_for(aid),
        )
        return

    # ===== СРАВНЕНИЕ ПЕРИОДОВ =====
    if data.startswith("cmp_menu|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Сравнение периодов для {get_account_name(aid)}:",
            reply_markup=compare_kb_for(aid),
        )
        return

    if data.startswith("back_periods|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Отчёт по: {get_account_name(aid)}\nВыбери период:",
            reply_markup=period_kb_for(aid),
        )
        return

    if data.startswith("cmp_week|"):
        aid = data.split("|", 1)[1]
        now = datetime.now(ALMATY_TZ)
        until2 = now - timedelta(days=1)
        since2 = until2 - timedelta(days=6)
        until1 = since2 - timedelta(days=1)
        since1 = until1 - timedelta(days=6)
        period1 = {
            "since": since1.strftime("%Y-%m-%d"),
            "until": until1.strftime("%Y-%m-%d"),
        }
        period2 = {
            "since": since2.strftime("%Y-%m-%d"),
            "until": until2.strftime("%Y-%m-%d"),
        }
        label1 = f"{since1.strftime('%d.%m')}-{until1.strftime('%d.%m')}"
        label2 = f"{since2.strftime('%d.%m')}-{until2.strftime('%d.%m')}"
        await q.edit_message_text(f"Сравниваю {label1} vs {label2}…")
        txt = build_comparison_report(aid, period1, label1, period2, label2)
        await context.bot.send_message(chat_id, txt, parse_mode="HTML")
        return

    if data.startswith("cmp_custom|"):
        aid = data.split("|", 1)[1]
        context.user_data["await_cmp_for"] = aid
        await q.edit_message_text(
            "Отправь два диапазона дат через ';' или с новой строки.\n"
            "Например:\n"
            "01.06.2025-07.06.2025;08.06.2025-14.06.2025",
            reply_markup=compare_kb_for(aid),
        )
        return

    # ===== НАСТРОЙКИ =====
    if data == "choose_acc_settings":
        await q.edit_message_text(
            "Выберите аккаунт для настроек:",
            reply_markup=accounts_kb("set1"),
        )
        return

    if data.startswith("set1|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("toggle_enabled|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {})
        row["enabled"] = not row.get("enabled", True)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("toggle_m|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"metrics": {}})
        row["metrics"] = row.get("metrics", {})
        row["metrics"]["messaging"] = not row["metrics"].get("messaging", True)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("toggle_l|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"metrics": {}})
        row["metrics"] = row.get("metrics", {})
        row["metrics"]["leads"] = not row["metrics"].get("leads", False)
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("toggle_alert|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        if alerts.get("enabled", False):
            alerts["enabled"] = False
        else:
            alerts["enabled"] = float(alerts.get("target_cpl", 0) or 0) > 0
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
        return

    if data.startswith("set_cpa|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        current = float(alerts.get("target_cpl", 0.0) or 0.0)
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        await q.edit_message_text(
            f"⚠️ Текущий target CPA: {current:.2f} $.\n"
            f"Напиши в чат число (например 2.5). 0 — выключит алерты.",
            reply_markup=settings_kb(aid),
        )
        context.user_data["await_cpa_for"] = aid
        return


# ============ TEXT INPUT ============
async def on_text_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return

    chat = update.effective_chat
    if chat and chat.type in ("group", "supergroup"):
        return

    text = update.message.text.strip()

    # кастомный диапазон для одного периода
    if "await_range_for" in context.user_data:
        aid = context.user_data.pop("await_range_for")
        parsed = parse_range(text)
        if not parsed:
            await update.message.reply_text(
                "Формат дат: 01.06.2025-07.06.2025. Попробуй ещё раз."
            )
            context.user_data["await_range_for"] = aid
            return
        period, label = parsed
        txt = get_cached_report(aid, period, label)
        await update.message.reply_text(
            txt or "Нет данных/нет доступа.", parse_mode="HTML"
        )
        return

    # сравнение двух диапазонов
    if "await_cmp_for" in context.user_data:
        aid = context.user_data.pop("await_cmp_for")
        parsed = parse_two_ranges(text)
        if not parsed:
            await update.message.reply_text(
                "Не распознал форматы дат.\n"
                "Пример: 01.06.2025-07.06.2025;08.06.2025-14.06.2025"
            )
            return
        (p1, label1), (p2, label2) = parsed
        txt = build_comparison_report(aid, p1, label1, p2, label2)
        await update.message.reply_text(txt, parse_mode="HTML")
        return

    # ввод target CPA
    if "await_cpa_for" in context.user_data:
        aid = context.user_data.pop("await_cpa_for")
        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text(
                "Введите число, например: 2.5 (или 0 чтобы выключить)"
            )
            context.user_data["await_cpa_for"] = aid
            return

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {})
        alerts["target_cpl"] = float(val)
        alerts["enabled"] = val > 0
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        if val > 0:
            await update.message.reply_text(
                f"✅ Target CPA для {get_account_name(aid)} обновлён: {val:.2f} $ (алерты ВКЛ)"
            )
        else:
            await update.message.reply_text(
                f"✅ Target CPA для {get_account_name(aid)} установлен 0 — алерты ВЫКЛ"
            )
        return

    # ручной ввод для автопилата
    if "await_manual_input" in context.user_data:
        entity_id = context.user_data.pop("await_manual_input")
        percent = parse_manual_input(text)
        if percent is None:
            await update.message.reply_text(
                "❌ Не получилось разобрать число. Пример: 1.2, 20, -15",
                parse_mode="HTML"
            )
            context.user_data["await_manual_input"] = entity_id
            return

        await update.message.reply_text(
            f"Подтвердить изменение бюджета на <b>{percent:+.1f}%</b> "
            f"для <code>{entity_id}</code>?",
            parse_mode="HTML",
            reply_markup=confirm_action_buttons(str(percent), entity_id)
        )
        return


# ============ APP ============
def build_app() -> Application:
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("billing", cmd_billing))
    app.add_handler(CommandHandler("sync_accounts", cmd_sync))
    app.add_handler(CommandHandler("heatmap", cmd_heatmap))

    app.add_handler(CallbackQueryHandler(on_cb_autopilot, pattern="^ap"))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_any))

    # Джобы
    app.job_queue.run_daily(
        full_daily_scan_job,
        time=time(hour=9, minute=20, tzinfo=ALMATY_TZ),
    )

    app.job_queue.run_daily(
        daily_report_job,
        time=time(hour=9, minute=30, tzinfo=ALMATY_TZ),
    )

    app.job_queue.run_daily(
        billing_digest_job,
        time=time(hour=9, minute=0, tzinfo=ALMATY_TZ),
    )

    schedule_cpa_alerts(app)

    init_billing_watch(
        app,
        get_enabled_accounts=get_enabled_accounts_in_order,
        get_account_name=get_account_name,
        usd_to_kzt=usd_to_kzt,
        kzt_round_up_1000=kzt_round_up_1000,
        owner_id=253181449,
        group_chat_id=str(DEFAULT_REPORT_CHAT),
    )

    return app
