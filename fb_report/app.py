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
    BOT_VERSION,
    BOT_CHANGELOG,
)
from .storage import (
    load_accounts,
    save_accounts,
    get_account_name,
    get_enabled_accounts_in_order,
    human_last_sync,
    upsert_from_bm,
    metrics_flags,
)
from .reporting import (
    fmt_int,
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
from services.analytics import analyze_campaigns, analyze_adsets


def _allowed(update: Update) -> bool:
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    user_id = update.effective_user.id if update.effective_user else None
    if chat_id in ALLOWED_CHAT_IDS:
        return True
    if user_id and user_id in ALLOWED_USER_IDS:
        return True
    return False


async def safe_edit_message(q, text: str, **kwargs):
    try:
        return await q.edit_message_text(text=text, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise


def _build_version_text() -> str:
    """Текст для команды /version и кнопки "Версия".

    Использует BOT_VERSION и BOT_CHANGELOG: базовые функции + последние значимые
    обновления. Косметические вещи можно не добавлять в BOT_CHANGELOG, тогда
    они не попадут в этот текст автоматически.
    """
    lines = [f"Версия бота: {BOT_VERSION}", ""]
    lines.extend(BOT_CHANGELOG)
    return "\n".join(lines)


def main_menu() -> InlineKeyboardMarkup:
    last_sync = human_last_sync()
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📊 Отчёты", callback_data="reports_menu"
                ),
            ],
            [
                InlineKeyboardButton(
                    "🆘 Мониторинг", callback_data="monitoring_menu"
                )
            ],
            [InlineKeyboardButton("💳 Биллинг", callback_data="billing")],
            [InlineKeyboardButton("🔥 Тепловая карта", callback_data="hm_menu")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data="choose_acc_settings")],
            [InlineKeyboardButton("🤖 Автопилат", callback_data="ap_main")],
            [
                InlineKeyboardButton(
                    f"🔁 Синк BM (посл. {last_sync})",
                    callback_data="sync_bm",
                )
            ],
            [InlineKeyboardButton("ℹ️ Версия", callback_data="version")],
        ]
    )


def monitoring_menu_kb() -> InlineKeyboardMarkup:
    """Подменю раздела мониторинга.

    Основные режимы сравнения + настройки мониторинга и заглушка плана заявок.
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Вчера vs позавчера", callback_data="mon_yday_vs_byday"
                )
            ],
            [
                InlineKeyboardButton(
                    "Прошлая неделя vs позапрошлая",
                    callback_data="mon_lastweek_vs_prevweek",
                )
            ],
            [
                InlineKeyboardButton(
                    "Текущая неделя vs прошлая (по вчера)",
                    callback_data="mon_curweek_vs_lastweek",
                )
            ],
            [
                InlineKeyboardButton(
                    "Кастомный период", callback_data="mon_custom_period"
                )
            ],
            [
                InlineKeyboardButton(
                    "⚙️ Настройки мониторинга",
                    callback_data="mon_settings",
                )
            ],
            [
                InlineKeyboardButton(
                    "📈 План заявок (скоро)", callback_data="leads_plan_soon"
                )
            ],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )


def account_reports_level_kb(aid: str) -> InlineKeyboardMarkup:
    """Выбор уровня отчёта по аккаунту: общий, кампании, адсеты."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Общий отчёт",
                    callback_data=f"rep_acc_mode|{aid}|general",
                )
            ],
            [
                InlineKeyboardButton(
                    "По кампаниям",
                    callback_data=f"rep_acc_mode|{aid}|campaigns",
                )
            ],
            [
                InlineKeyboardButton(
                    "По адсетам",
                    callback_data=f"rep_acc_mode|{aid}|adsets",
                )
            ],
            [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="report_one")],
        ]
    )


def account_reports_periods_kb(aid: str, mode: str) -> InlineKeyboardMarkup:
    """Выбор периода для отчёта по аккаунту на выбранном уровне.

    Пункты: Сегодня, Вчера, Прошлая неделя, Сравнение периодов, Назад.
    """
    base = f"rep_acc_p|{aid}|{mode}"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"{base}|today"),
                InlineKeyboardButton("Вчера", callback_data=f"{base}|yday"),
            ],
            [
                InlineKeyboardButton(
                    "Прошлая неделя", callback_data=f"{base}|week"
                )
            ],
            [
                InlineKeyboardButton(
                    "Сравнение периодов", callback_data=f"{base}|compare"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=f"rep_acc_back|{aid}|{mode}",
                )
            ],
        ]
    )


def reports_accounts_kb(prefix: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора аккаунтов для раздела "Отчёты".

    Отличается от общей accounts_kb только кнопкой "Назад", которая
    возвращает в подменю отчётов, а не сразу в главное меню.
    """
    store = load_accounts()
    if store:
        enabled_ids = [aid for aid, row in store.items() if row.get("enabled", True)]
        disabled_ids = [
            aid for aid, row in store.items() if not row.get("enabled", True)
        ]
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
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="reports_menu")])
    return InlineKeyboardMarkup(rows)


def billing_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Текущие биллинги", callback_data="billing_current")],
            [InlineKeyboardButton("Прогноз списаний", callback_data="billing_forecast")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )


def reports_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Общий отчёт", callback_data="report_all")],
            [InlineKeyboardButton("Отчёт по аккаунту", callback_data="report_one")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )


def reports_periods_kb(prefix: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора периода для раздела "Отчёты".

    prefix задаёт основу callback'ов, например "rep_all" → rep_all_today, ...
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"{prefix}_today"),
                InlineKeyboardButton("Вчера", callback_data=f"{prefix}_yday"),
            ],
            [InlineKeyboardButton("Прошедшая неделя", callback_data=f"{prefix}_week")],
            [InlineKeyboardButton("Свой диапазон", callback_data=f"{prefix}_custom")],
            [InlineKeyboardButton("Сравнить периоды", callback_data=f"{prefix}_compare")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="reports_menu")],
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
            [
                InlineKeyboardButton(
                    "🗓 Свой диапазон", callback_data=f"hmcustom|{aid}"
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
        disabled_ids = [
            aid for aid, row in store.items() if not row.get("enabled", True)
        ]
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


def account_report_mode_kb(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📊 Отчёт по аккаунту",
                    callback_data=f"one_mode_acc|{aid}",
                )
            ],
            [
                InlineKeyboardButton(
                    "📂 Отчёт по адсетам",
                    callback_data=f"one_mode_adsets|{aid}",
                )
            ],
            [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="choose_acc_report")],
        ]
    )


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
        "/version — показать текущую версию бота и краткое описание\n"
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


async def cmd_version(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    text = _build_version_text()
    await update.message.reply_text(text, reply_markup=main_menu())


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


async def on_cb_autopilot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if not _allowed(update):
        await safe_edit_message(q, "⛔️ Нет доступа.")
        return

    data = q.data or ""
    chat_id = str(q.message.chat.id)

    if data == "ap_main":
        await safe_edit_message(
            q,
            "Выберите режим автопилата:",
            reply_markup=autopilot_main_menu(),
        )
        return

    if data.startswith("apmode|"):
        mode = data.split("|", 1)[1]
        context.user_data["autopilot_mode"] = mode

        await safe_edit_message(
            q,
            f"Режим: <b>{mode}</b>\nВыберите подрежим:",
            parse_mode="HTML",
            reply_markup=autopilot_submode_menu(),
        )
        return

    if data.startswith("apsub|"):
        sub = data.split("|", 1)[1]
        context.user_data["autopilot_submode"] = sub

        await safe_edit_message(
            q,
            f"Режим: <b>{context.user_data.get('autopilot_mode')}</b>\n"
            f"Подрежим: <b>{sub}</b>\n\n"
            f"Теперь выберите аккаунт:",
            parse_mode="HTML",
            reply_markup=accounts_kb("ap_acc"),
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
            await safe_edit_message(
                q,
                "⚠ Ошибка кнопки: некорректный формат callback_data.",
                parse_mode="HTML",
            )
            return

        _, action, *rest = parts
        entity_id = rest[0] if rest else ""

        if action == "back":
            await safe_edit_message(
                q,
                "Выберите режим автопилата:",
                reply_markup=autopilot_main_menu(),
            )
            return

        if not entity_id:
            await safe_edit_message(
                q,
                "⚠ Ошибка кнопки: не передан ID сущности.\n"
                "Обнови рекомендации и попробуй ещё раз.",
                parse_mode="HTML",
            )
            return

        if action == "manual":
            context.user_data["await_manual_input"] = entity_id
            await safe_edit_message(
                q,
                f"✍️ Введите число (например 1.2, -20, 15):\n"
                f"ID: <code>{entity_id}</code>",
                parse_mode="HTML",
            )
            return

        await safe_edit_message(
            q,
            f"Подтвердить действие <b>{action}</b> для <code>{entity_id}</code>?",
            parse_mode="HTML",
            reply_markup=confirm_action_buttons(action, entity_id),
        )
        return

    if data.startswith("apconfirm|"):
        _, yesno, action, entity_id = data.split("|", 3)

        if yesno == "no":
            await safe_edit_message(q, "Операция отменена.", parse_mode="HTML")
            return

        if action in ("up20", "down20"):
            percent = 20 if action == "up20" else -20
            res = apply_budget_change(entity_id, percent)
            await safe_edit_message(q, res["message"], parse_mode="HTML")
            return

        if action == "off":
            aid = context.user_data.get("ap_aid")
            if aid and not can_disable(aid, entity_id):
                await safe_edit_message(
                    q,
                    "❌ Нельзя отключить этот адсет — иначе весь аккаунт останется без трафика.",
                    parse_mode="HTML",
                )
                return

            res = disable_entity(entity_id)
            await safe_edit_message(q, res["message"], parse_mode="HTML")
            return

        try:
            percent = float(action.replace(",", "."))
        except Exception:
            await safe_edit_message(
                q,
                "⚠ Не получилось прочитать процент изменения.",
                parse_mode="HTML",
            )
            return

        res = apply_budget_change(entity_id, percent)
        await safe_edit_message(q, res["message"], parse_mode="HTML")
        return


async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not _allowed(update):
        await q.edit_message_text("⛔️ Нет доступа.")
        return

    data = q.data or ""
    chat_id = str(q.message.chat.id)

    if data == "version":
        text = _build_version_text()
        await context.bot.send_message(chat_id, text)
        return

    if data == "menu":
        await safe_edit_message(q, "🤖 Выберите действие:", reply_markup=main_menu())
        return

    if data == "monitoring_menu":
        await safe_edit_message(
            q,
            "Раздел мониторинга. Выберите пункт:",
            reply_markup=monitoring_menu_kb(),
        )
        return

    if data == "reports_menu":
        await safe_edit_message(
            q,
            "Выберите тип отчёта:",
            reply_markup=reports_menu_kb(),
        )
        return

    # ======= НОВЫЙ РАЗДЕЛ "ОТЧЁТЫ" =======
    # Совместимость: старый callback rep_all_menu ведём в новый report_all.
    if data in {"report_all", "rep_all_menu"}:
        await safe_edit_message(
            q,
            "Выберите период:",
            reply_markup=reports_periods_kb("rep_all"),
        )
        return

    if data == "report_one":
        await safe_edit_message(
            q,
            "Выберите аккаунт для отчёта по аккаунту:",
            reply_markup=reports_accounts_kb("rep_one_acc"),
        )
        return

    if data == "adsets_menu":
        await safe_edit_message(
            q,
            "Выберите аккаунт для отчёта по адсетам:",
            reply_markup=accounts_kb("adrep"),
        )
        return

    if data.startswith("rep_one_acc|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            f"Отчёт по: {get_account_name(aid)}\nВыберите уровень отчёта:",
            reply_markup=account_reports_level_kb(aid),
        )
        return
    
    if data.startswith("rep_acc_mode|"):
        _, aid, mode = data.split("|", 2)
        await safe_edit_message(
            q,
            f"Отчёт по: {get_account_name(aid)}\nВыберите период:",
            reply_markup=account_reports_periods_kb(aid, mode),
        )
        return

    if data.startswith("rep_acc_back|"):
        _, aid, _mode = data.split("|", 2)
        await safe_edit_message(
            q,
            f"Отчёт по: {get_account_name(aid)}\nВыберите уровень отчёта:",
            reply_markup=account_reports_level_kb(aid),
        )
        return

    if data.startswith("rep_acc_p|"):
        # Формат: rep_acc_p|{aid}|{mode}|{kind}
        _, aid, mode, kind = data.split("|", 3)

        # Общий отчёт по аккаунту — используем существующую логику one_*.
        if mode == "general":
            if kind == "today":
                label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
                await safe_edit_message(
                    q,
                    f"Отчёт по {get_account_name(aid)} за {label}:",
                )
                txt = get_cached_report(aid, "today", label)
                await context.bot.send_message(
                    chat_id,
                    txt or "Нет данных/нет доступа.",
                    parse_mode="HTML",
                )
                return

            if kind == "yday":
                label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime(
                    "%d.%m.%Y"
                )
                await safe_edit_message(
                    q,
                    f"Отчёт по {get_account_name(aid)} за {label}:",
                )
                txt = get_cached_report(aid, "yesterday", label)
                await context.bot.send_message(
                    chat_id,
                    txt or "Нет данных/нет доступа.",
                    parse_mode="HTML",
                )
                return

            if kind == "week":
                until = datetime.now(ALMATY_TZ) - timedelta(days=1)
                since = until - timedelta(days=6)
                period = {
                    "since": since.strftime("%Y-%m-%d"),
                    "until": until.strftime("%Y-%m-%d"),
                }
                label = f"{since.strftime('%d.%m')}-{until.strftime('%d.%m')}"
                await safe_edit_message(
                    q,
                    f"Отчёт по {get_account_name(aid)} за {label}:",
                )
                txt = get_cached_report(aid, period, label)
                await context.bot.send_message(
                    chat_id,
                    txt or "Нет данных/нет доступа.",
                    parse_mode="HTML",
                )
                return

            if kind == "compare":
                await safe_edit_message(
                    q,
                    f"Сравнение периодов для {get_account_name(aid)}:",
                    reply_markup=compare_kb_for(aid),
                )
                return

        # Кампании / адсеты: используем analyze_campaigns/analyze_adsets
        # и выбранный пресет периода.
        from .storage import metrics_flags

        flags = metrics_flags(aid)

        # Определяем количество дней и человекочитаемый лейбл
        if kind == "today":
            days = 1
            label = "сегодня"
        elif kind == "yday":
            days = 1
            label = "вчера"
        elif kind == "week":
            days = 7
            label = "последние 7 дней"
        else:
            # Для кампаний/адсетов сравнение периодов пока не поддерживаем
            await safe_edit_message(
                q,
                "Сравнение периодов пока доступно только для общего отчёта по аккаунту.",
            )
            return

        name = get_account_name(aid)

        if mode == "campaigns":
            await safe_edit_message(
                q,
                f"Готовлю отчёт по кампаниям для {name} ({label})…",
            )
            camps = analyze_campaigns(aid, days=days)
            if not camps:
                await context.bot.send_message(
                    chat_id,
                    f"Нет данных по кампаниям для {name} ({label}).",
                )
                return

            lines = [f"📊 Кампании — {name} ({label})"]
            for idx, c in enumerate(camps, start=1):
                spend = c.get("spend", 0.0) or 0.0
                impr = c.get("impr", 0) or 0
                clicks = c.get("clicks", 0) or 0
                msgs = c.get("msgs", 0) or 0
                leads = c.get("leads", 0) or 0

                eff_msgs = msgs if flags.get("messaging") else 0
                eff_leads = leads if flags.get("leads") else 0
                eff_total = eff_msgs + eff_leads
                cpa_eff = (spend / eff_total) if eff_total > 0 else None

                parts = [
                    f"{idx}. {c.get('name')}",
                    f"   👀 {impr}  🔍 {clicks}  💵 {spend:.2f} $",
                ]
                if flags.get("messaging"):
                    parts.append(f"   💬 {msgs}")
                if flags.get("leads"):
                    parts.append(f"   📩 {leads}")
                if flags.get("messaging") or flags.get("leads"):
                    parts.append(
                        f"   Итого: {eff_total}  CPA: {cpa_eff:.2f}$"
                        if cpa_eff is not None
                        else f"   Итого: {eff_total}  CPA: —"
                    )

                lines.append("\n".join(parts))

            await context.bot.send_message(chat_id, "\n".join(lines))
            return

        if mode == "adsets":
            await safe_edit_message(
                q,
                f"Готовлю отчёт по адсетам для {name} ({label})…",
            )
            adsets = analyze_adsets(aid, days=days)
            if not adsets:
                await context.bot.send_message(
                    chat_id,
                    f"Нет данных по адсетам для {name} ({label}).",
                )
                return

            # сортируем по spend по убыванию
            adsets_sorted = sorted(
                adsets, key=lambda x: x.get("spend", 0.0), reverse=True
            )

            lines = [f"📊 Адсеты — {name} ({label})"]
            for idx, a in enumerate(adsets_sorted, start=1):
                spend = a.get("spend", 0.0) or 0.0
                impr = a.get("impr", 0) or 0
                clicks = a.get("clicks", 0) or 0
                msgs = a.get("msgs", 0) or 0
                leads = a.get("leads", 0) or 0

                eff_msgs = msgs if flags.get("messaging") else 0
                eff_leads = leads if flags.get("leads") else 0
                eff_total = eff_msgs + eff_leads
                cpa_eff = (spend / eff_total) if eff_total > 0 else None

                parts = [
                    f"{idx}. {a.get('name')}",
                    f"   👀 {impr}  🔍 {clicks}  💵 {spend:.2f} $",
                ]
                if flags.get("messaging"):
                    parts.append(f"   💬 {msgs}")
                if flags.get("leads"):
                    parts.append(f"   📩 {leads}")
                if flags.get("messaging") or flags.get("leads"):
                    parts.append(
                        f"   Итого: {eff_total}  CPA: {cpa_eff:.2f}$"
                        if cpa_eff is not None
                        else f"   Итого: {eff_total}  CPA: —"
                    )

                lines.append("\n".join(parts))

            await context.bot.send_message(chat_id, "\n".join(lines))
            return

    if data.startswith("adrep|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            f"Готовлю отчёт по адсетам для {get_account_name(aid)} "
            f"за последние 7 дней…",
        )
        await send_adset_report(context, chat_id, aid)
        return

    # Старые callback'и rep_today/rep_yday/rep_week считаем синонимами
    # новых rep_all_today/rep_all_yday/rep_all_week.
    if data in {"rep_all_today", "rep_today"}:
        label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        await safe_edit_message(q, f"Готовлю отчёт за {label}…")
        await send_period_report(context, chat_id, "today", label)
        return

    if data in {"rep_all_yday", "rep_yday"}:
        label = (datetime.now(ALMATY_TZ) - timedelta(days=1)).strftime("%d.%m.%Y")
        await q.edit_message_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, chat_id, "yesterday", label)
        return

    if data in {"rep_all_week", "rep_week"}:
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

    if data == "rep_all_custom":
        context.user_data["await_all_range_for"] = True
        await safe_edit_message(
            q,
            "Введи даты форматом: 01.06.2025-07.06.2025",
            reply_markup=reports_periods_kb("rep_all"),
        )
        return

    if data == "rep_all_compare":
        context.user_data["await_all_cmp_for"] = True
        await safe_edit_message(
            q,
            "Отправь два диапазона дат через ';' или с новой строки.\n"
            "Пример: 01.06.2025-07.06.2025;08.06.2025-14.06.2025",
            reply_markup=reports_periods_kb("rep_all"),
        )
        return

    if data == "hm_menu":
        await safe_edit_message(
            q,
            "Выберите аккаунт для тепловой карты:",
            reply_markup=accounts_kb("hmacc"),
        )
        return

    if data.startswith("hmacc|"):
        aid = data.split("|", 1)[1]
        context.user_data["heatmap_aid"] = aid
        await safe_edit_message(
            q,
            f"Выберите период тепловой карты для {get_account_name(aid)}:",
            reply_markup=heatmap_menu(aid),
        )
        return

    if data.startswith("hm7|"):
        aid = data.split("|")[1]
        heat = build_heatmap_for_account(aid, get_account_name, mode="7")
        await safe_edit_message(q, heat, parse_mode="HTML")
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

    if data == "billing":
        await safe_edit_message(
            q,
            "Что показать по биллингу?",
            reply_markup=billing_menu(),
        )
        return
    if data == "billing_current":
        await safe_edit_message(q, "📋 Биллинги (неактивные аккаунты):")
        await send_billing(context, chat_id)
        return
    if data == "billing_forecast":
        await safe_edit_message(q, "🔮 Считаю прогноз списаний…")
        await send_billing_forecast(context, chat_id)
        return

    if data == "leads_plan_soon":
        text = (
            "📈 План заявок\n\n"
            "В этом разделе позже будет аналитика: план заявок на месяц/неделю и "
            "сравнение с фактом — на сколько отстаём или перевыполняем план.\n\n"
            "Пока это информационная кнопка, функционал в разработке."
        )
        await safe_edit_message(q, text, reply_markup=monitoring_menu_kb())
        return

    # ====== Мониторинг: заглушки режимов сравнения и настроек ======

    if data == "mon_yday_vs_byday":
        await safe_edit_message(
            q,
            "Вчера vs позавчера — мониторинг пока в разработке.\n"
            "В финальной версии здесь будет сравнение всех ключевых метрик за вчера "
            "против позавчера по каждому включённому аккаунту.",
            reply_markup=monitoring_menu_kb(),
        )
        return

    if data == "mon_lastweek_vs_prevweek":
        await safe_edit_message(
            q,
            "Прошлая неделя vs позапрошлая — мониторинг пока в разработке.\n"
            "Позже здесь будет сравнение по неделям (пн–вс) с подсветкой изменений.",
            reply_markup=monitoring_menu_kb(),
        )
        return

    if data == "mon_curweek_vs_lastweek":
        await safe_edit_message(
            q,
            "Текущая неделя vs прошлая (по вчера) — в разработке.\n"
            "План: сравнение накопленных метрик с понедельника по вчерашний день "
            "против такого же диапазона прошлой недели.",
            reply_markup=monitoring_menu_kb(),
        )
        return

    if data == "mon_custom_period":
        await safe_edit_message(
            q,
            "Кастомный период мониторинга пока не реализован.\n"
            "Дальше здесь появится выбор диапазона дат и сравнение с таким же по "
            "длине предыдущим периодом.",
            reply_markup=monitoring_menu_kb(),
        )
        return

    if data == "mon_settings":
        await safe_edit_message(
            q,
            "⚙️ Настройки мониторинга пока в разработке.\n"
            "Планируется настройка курса USD→KZT и месячных бюджетов по аккаунтам.",
            reply_markup=monitoring_menu_kb(),
        )
        return

    if data == "sync_bm":
        try:
            res = upsert_from_bm()
            last_sync_h = human_last_sync()
            await safe_edit_message(
                q,
                f"✅ Синк завершён. Добавлено: {res['added']}, "
                f"обновлено: {res['updated']}, пропущено: {res['skipped']}. "
                f"Всего: {res['total']}\n"
                f"🕓 Последняя синхронизация: {last_sync_h}",
                reply_markup=main_menu(),
            )
        except Exception as e:
            await safe_editMessage(
                q,
                f"⚠️ Ошибка синка: {e}",
                reply_markup=main_menu(),
            )
        return

    if data == "choose_acc_report":
        await safe_edit_message(
            q,
            "Выберите аккаунт:",
            reply_markup=accounts_kb("rep1"),
        )
        return

    if data.startswith("rep1|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            f"Отчёт по: {get_account_name(aid)}\nВыберите тип отчёта:",
            reply_markup=account_report_mode_kb(aid),
        )
        return

    if data.startswith("one_mode_acc|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            f"Отчёт по: {get_account_name(aid)}\nВыбери период:",
            reply_markup=period_kb_for(aid),
        )
        return

    if data.startswith("one_mode_adsets|"):
        aid = data.split("|", 1)[1]
        await q.edit_message_text(
            f"Готовлю отчёт по адсетам для {get_account_name(aid)} "
            f"за последние 7 дней…"
        )
        await send_adset_report(context, chat_id, aid)
        return

    if data.startswith("one_today|"):
        aid = data.split("|", 1)[1]
        label = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")
        await safe_edit_message(
            q,
            f"Отчёт по {get_account_name(aid)} за {label}:",
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
        await safe_edit_message(
            q,
            f"Введи даты для {get_account_name(aid)} форматом: 01.06.2025-07.06.2025",
            reply_markup=period_kb_for(aid),
        )
        return

    if data.startswith("cmp_menu|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
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
        await safe_edit_message(q, f"Сравниваю {label1} vs {label2}…")
        txt = build_comparison_report(aid, period1, label1, period2, label2)
        await context.bot.send_message(chat_id, txt, parse_mode="HTML")
        return

    if data.startswith("cmp_custom|"):
        aid = data.split("|", 1)[1]
        context.user_data["await_cmp_for"] = aid
        await safe_edit_message(
            q,
            "Отправь два диапазона дат через ';' или с новой строки.\n"
            "Например:\n"
            "01.06.2025-07.06.2025;08.06.2025-14.06.2025",
            reply_markup=compare_kb_for(aid),
        )
        return

    if data.startswith("hmcustom|"):
        aid = data.split("|", 1)[1]
        context.user_data["await_heatmap_range_for"] = aid
        await safe_edit_message(
            q,
            "Введи даты для тепловой карты форматом: 01.06.2025-07.06.2025",
            reply_markup=heatmap_menu(aid),
        )
        return

    if data == "choose_acc_settings":
        await safe_edit_message(
            q,
            "Выберите аккаунт для настроек:",
            reply_markup=accounts_kb("set1"),
        )
        return

    if data.startswith("set1|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
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
        await safe_edit_message(
            q,
            f"⚠️ Текущий target CPA: {current:.2f} $.\n"
            f"Напиши в чат число (например 2.5). 0 — выключит алерты.",
            reply_markup=settings_kb(aid),
        )
        context.user_data["await_cpa_for"] = aid
        return


async def on_text_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return

    chat = update.effective_chat
    if chat and chat.type in ("group", "supergroup"):
        return

    text = update.message.text.strip()

    # Кастомный диапазон для отчёта "по всем" (rep_all_custom)
    if context.user_data.get("await_all_range_for"):
        context.user_data.pop("await_all_range_for", None)
        parsed = parse_range(text)
        if not parsed:
            await update.message.reply_text(
                "Формат дат: 01.06.2025-07.06.2025. Попробуй ещё раз."
            )
            context.user_data["await_all_range_for"] = True
            return

        period, label = parsed
        await update.message.reply_text(f"Готовлю отчёт за {label}…")
        await send_period_report(context, str(DEFAULT_REPORT_CHAT), period, label)
        return

    # Сравнение периодов для отчёта "по всем" (rep_all_compare)
    if context.user_data.get("await_all_cmp_for"):
        context.user_data.pop("await_all_cmp_for", None)
        parsed = parse_two_ranges(text)
        if not parsed:
            await update.message.reply_text(
                "Не распознал форматы дат.\n"
                "Пример: 01.06.2025-07.06.2025;08.06.2025-14.06.2025"
            )
            context.user_data["await_all_cmp_for"] = True
            return

        (p1, label1), (p2, label2) = parsed
        await update.message.reply_text(f"Готовлю отчёты за {label1} и {label2}…")
        # Отправляем два отдельных отчёта по всем аккаунтам.
        await send_period_report(context, str(DEFAULT_REPORT_CHAT), p1, label1)
        await send_period_report(context, str(DEFAULT_REPORT_CHAT), p2, label2)
        return

    # Кастомный период для тепловой карты
    if "await_heatmap_range_for" in context.user_data:
        aid = context.user_data.pop("await_heatmap_range_for")
        parsed = parse_range(text)
        if not parsed:
            await update.message.reply_text(
                "Формат дат: 01.06.2025-07.06.2025. Попробуй ещё раз."
            )
            context.user_data["await_heatmap_range_for"] = aid
            return

        period, label = parsed
        from .insights import build_heatmap_for_account

        # Пока build_heatmap_for_account умеет только пресеты (7/14/месяц),
        # используем режим "7" и подменяем строку с периодом.
        heat = build_heatmap_for_account(aid, get_account_name, mode="7")
        lines = heat.splitlines()
        if len(lines) >= 2:
            lines[1] = f"Период: {label}"
        await update.message.reply_text("\n".join(lines))
        return

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
