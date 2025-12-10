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
from autopilat.actions import apply_budget_change

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
from .creatives import fetch_instagram_active_ads_links, format_instagram_ads_links
from .adsets import send_adset_report
from .billing import send_billing, send_billing_forecast, billing_digest_job
from .jobs import full_daily_scan_job, daily_report_job, schedule_cpa_alerts, _resolve_account_cpa

from services.analytics import analyze_campaigns, analyze_adsets, analyze_account, analyze_ads
from services.facebook_api import pause_ad
from services.ai_focus import get_focus_comment, ask_deepseek
from monitor_anomalies import build_anomaly_messages_for_account
import json
import asyncio


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
            [InlineKeyboardButton("🔗 Ссылки на рекламу", callback_data="insta_links_menu")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data="choose_acc_settings")],
            [
                InlineKeyboardButton(
                    f"🔁 Синк BM (посл. {last_sync})",
                    callback_data="sync_bm",
                )
            ],
            [InlineKeyboardButton("ℹ️ Версия", callback_data="version")],
        ]
    )


def focus_ai_period_kb(level: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора периода для разового отчёта Фокус-ИИ."""
    base = f"focus_ai_now_period|{level}"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"{base}|today"),
                InlineKeyboardButton("Вчера", callback_data=f"{base}|yday"),
            ],
            [
                InlineKeyboardButton("7 дней", callback_data=f"{base}|7d"),
                InlineKeyboardButton("30 дней", callback_data=f"{base}|30d"),
            ],
            [
                InlineKeyboardButton("🗓 Свой период", callback_data=f"{base}|custom"),
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="focus_ai_now")],
        ]
    )


def focus_ai_recommendation_kb(
    level: str,
    recommendation: str,
    delta: float,
    objects: list | None = None,
) -> InlineKeyboardMarkup:
    """Клавиатура под отчётом Фокус-ИИ с кнопкой действия и ручным вводом.

    Пока действия не применяют реальные изменения бюджета, а служат как подсказка.
    """

    buttons = []

    if recommendation == "increase_budget" and delta > 0:
        buttons.append(
            InlineKeyboardButton(
                f"⬆️ Увеличить бюджет на {delta:.0f}%",
                callback_data=f"focus_ai_action|{level}|inc|{int(delta)}",
            )
        )
    elif recommendation == "decrease_budget" and delta < 0:
        buttons.append(
            InlineKeyboardButton(
                f"⬇️ Понизить бюджет на {abs(delta):.0f}%",
                callback_data=f"focus_ai_action|{level}|dec|{int(abs(delta))}",
            )
        )
    elif recommendation == "keep":
        buttons.append(
            InlineKeyboardButton(
                "✅ Оставить как есть",
                callback_data=f"focus_ai_action|{level}|keep|0",
            )
        )

    rows = []
    if buttons:
        rows.append(buttons)

    rows.append(
        [
            InlineKeyboardButton(
                "✏️ Ручной ввод",
                callback_data=f"focus_ai_action|{level}|manual|0",
            )
        ]
    )

    # Пер-объектные рекомендации (минимум по адсетам).
    objs = objects or []
    for obj in objs:
        obj_level = obj.get("level") or ""
        obj_id = str(obj.get("id") or "")
        obj_name = str(obj.get("name") or obj_id)
        obj_rec = obj.get("recommendation") or "keep"
        obj_delta = float(obj.get("suggested_change_percent") or 0)

        # Бюджетные действия только для adset-уровня.
        if obj_level != "adset":
            continue

        if obj_rec == "increase_budget" and obj_delta > 0:
            action = "inc"
            sign = "⬆️"
            label = f"{sign} {obj_name}: +{obj_delta:.0f}%"
        elif obj_rec == "decrease_budget" and obj_delta < 0:
            action = "dec"
            sign = "⬇️"
            label = f"{sign} {obj_name}: {obj_delta:.0f}%"
        else:
            continue

        rows.append(
            [
                InlineKeyboardButton(
                    label,
                    callback_data=f"focus_ai_obj|adset|{obj_id}|{action}|{int(obj_delta)}",
                )
            ]
        )

    rows.append([InlineKeyboardButton("⬅️ Мониторинг", callback_data="monitoring_menu")])

    return InlineKeyboardMarkup(rows)


def monitoring_menu_kb() -> InlineKeyboardMarkup:
    """Подменю раздела мониторинга.

    Основные режимы сравнения + настройки мониторинга и заглушка плана заявок.
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🎯 Фокус-ИИ", callback_data="focus_ai_menu"
                )
            ],
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
                    "⚠️ Аномалии",
                    callback_data="anomalies_menu",
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


def focus_ai_main_kb() -> InlineKeyboardMarkup:
    """Промежуточное меню Фокус-ИИ."""
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⚙️ Настройки", callback_data="focus_ai_settings"
                )
            ],
            [
                InlineKeyboardButton(
                    "📊 Запросить отчёт сейчас", callback_data="focus_ai_now"
                )
            ],
            [InlineKeyboardButton("⬅️ Мониторинг", callback_data="monitoring_menu")],
        ]
    )


def focus_ai_level_kb_settings() -> InlineKeyboardMarkup:
    """Клавиатура выбора уровня для сценария настроек Фокус-ИИ.

    Пока реально поддерживаем только уровень "Аккаунт".
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Аккаунт", callback_data="focus_ai_set_level|account"
                )
            ],
            [
                InlineKeyboardButton(
                    "Кампания", callback_data="focus_ai_set_level|campaign"
                )
            ],
            [
                InlineKeyboardButton(
                    "Адсет", callback_data="focus_ai_set_level|adset"
                )
            ],
            [
                InlineKeyboardButton(
                    "Объявление", callback_data="focus_ai_set_level|ad"
                )
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="focus_ai_settings")],
        ]
    )


def focus_ai_level_kb_now() -> InlineKeyboardMarkup:
    """Клавиатура выбора уровня для разового отчёта Фокус-ИИ.

    Пока вся логика отчёта остаётся заглушкой, но уровни уже отражены в UI.
    """
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Аккаунт", callback_data="focus_ai_now_level|account"
                )
            ],
            [
                InlineKeyboardButton(
                    "Кампания", callback_data="focus_ai_now_level|campaign"
                )
            ],
            [
                InlineKeyboardButton(
                    "Адсет", callback_data="focus_ai_now_level|adset"
                )
            ],
            [
                InlineKeyboardButton(
                    "Объявление", callback_data="focus_ai_now_level|ad"
                )
            ],
            [InlineKeyboardButton("⬅️ Назад", callback_data="focus_ai_now")],
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
            [
                InlineKeyboardButton(
                    "По объявлениям",
                    callback_data=f"rep_acc_mode|{aid}|ads",
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


def _human_cpa_freq(freq: str) -> str:
    if freq == "hourly":
        return "Каждый час 10:00–22:00"
    return "3 раза в день"


def _weekday_label(key: str) -> str:
    return {
        "mon": "Пн",
        "tue": "Вт",
        "wed": "Ср",
        "thu": "Чт",
        "fri": "Пт",
        "sat": "Сб",
        "sun": "Вс",
    }.get(key, key)


def cpa_settings_kb(aid: str):
    st = load_accounts().get(aid, {"alerts": {}})
    alerts = st.get("alerts", {}) or {}

    account_cpa = float(alerts.get("account_cpa", alerts.get("target_cpl", 0.0)) or 0.0)
    freq = alerts.get("freq", "3x")
    days = alerts.get("days") or []
    ai_on = bool(alerts.get("ai_enabled", True))

    # Статусные строки
    days_labels = [
        _weekday_label(d)
        for d in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        if d in days
    ]
    days_str = ", ".join(days_labels) if days_labels else "не выбраны"
    ai_str = "ВКЛ" if ai_on else "ВЫКЛ"

    text = (
        f"Настройки CPA-алёртов для {get_account_name(aid)}:\n\n"
        f"• Target CPA аккаунта: {account_cpa:.2f} $\n"
        f"• Частота: {_human_cpa_freq(freq)}\n"
        f"• Дни недели: {days_str}\n"
        f"• ИИ-анализ: {ai_str}"
    )

    # Кнопка ИИ-анализ
    ai_btn_text = "🟢 ИИ-анализ: ВКЛ" if ai_on else "🔴 ИИ-анализ: ВЫКЛ"

    # Кнопки частоты
    freq_3x_selected = freq != "hourly"
    freq_hourly_selected = freq == "hourly"
    freq_3x_text = ("✅ " if freq_3x_selected else "") + "3 раза в день"
    freq_hourly_text = ("✅ " if freq_hourly_selected else "") + "Каждый час 10:00–22:00"

    # Кнопки дней недели (2 ряда по 4 и 3 кнопки)
    all_keys = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
    day_buttons = []
    for key in all_keys:
        label = _weekday_label(key)
        selected = key in days
        txt = ("✅ " if selected else "") + label
        day_buttons.append(
            InlineKeyboardButton(txt, callback_data=f"cpa_day|{aid}|{key}")
        )

    rows = [
        [InlineKeyboardButton(ai_btn_text, callback_data=f"cpa_ai|{aid}")],
        [
            InlineKeyboardButton(
                freq_3x_text, callback_data=f"cpa_freq|{aid}|3x"
            ),
            InlineKeyboardButton(
                freq_hourly_text, callback_data=f"cpa_freq|{aid}|hourly"
            ),
        ],
        day_buttons[0:4],
        day_buttons[4:7],
        [InlineKeyboardButton("Каждый день", callback_data=f"cpa_days_all|{aid}")],
        [
            InlineKeyboardButton(
                "📁 CPA по кампаниям", callback_data=f"cpa_campaigns|{aid}"
            )
        ],
        [
            InlineKeyboardButton(
                "📂 CPA по адсетам", callback_data=f"cpa_adsets|{aid}"
            )
        ],
        [
            InlineKeyboardButton(
                "📁 CPA по объявлениям", callback_data=f"cpa_ads|{aid}"
            )
        ],
        [
            InlineKeyboardButton(
                "⬅️ Назад к аккаунту", callback_data=f"set1|{aid}"
            )
        ],
    ]

    return text, InlineKeyboardMarkup(rows)


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
                    "⚙️ Настройки CPA-алёртов", callback_data=f"cpa_settings|{aid}"
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


def _user_has_focus_settings(user_id: str) -> bool:
    """Проверка, есть ли у пользователя какие-либо сохранённые настройки Фокус-ИИ."""
    st = load_accounts()
    for row in st.values():
        focus = row.get("focus") or {}
        if user_id in focus:
            return True
    return False


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
        "🚀 Функции автопилота:\n"
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
    # ... (rest of the function remains the same)


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

    # ==== CPA-алёрты по объявлениям: тихий режим и выключение ====

    if data.startswith("cpa_ad_silent|"):
        # Формат: cpa_ad_silent|{aid}|{ad_id}
        try:
            _p, aid, ad_id = data.split("|", 2)
        except ValueError:
            await q.answer("Некорректные данные для тихого режима.", show_alert=True)
            return

        st = load_accounts()
        row = st.get(aid) or {}
        alerts = row.get("alerts") or {}
        ad_alerts = alerts.get("ad_alerts") or {}
        cfg = ad_alerts.get(ad_id) or {}

        current = bool(cfg.get("silent", False))
        cfg["silent"] = not current
        ad_alerts[ad_id] = cfg
        alerts["ad_alerts"] = ad_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        if cfg["silent"]:
            await q.answer("Тихий режим включён для объявления.", show_alert=False)
        else:
            await q.answer("Тихий режим выключен для объявления.", show_alert=False)
        return

    if data.startswith("cpa_ad_off|"):
        # Формат: cpa_ad_off|{aid}|{ad_id}
        try:
            _p, aid, ad_id = data.split("|", 2)
        except ValueError:
            await q.answer("Некорректные данные для выключения объявления.", show_alert=True)
            return

        res = pause_ad(ad_id)
        status = res.get("status")
        msg = res.get("message") or ""

        if status != "ok":
            # При ошибке API просто сообщаем пользователю и, если есть альтернативы,
            # даём кнопку для ручного открытия объявления в Ads Manager.
            await q.answer(f"Ошибка при выключении: {msg}", show_alert=True)

            try:
                # Проверяем наличие альтернатив за последние 7 дней.
                now = datetime.now(ALMATY_TZ)
                period_7d = {
                    "since": (now - timedelta(days=7)).strftime("%Y-%m-%d"),
                    "until": now.strftime("%Y-%m-%d"),
                }
                ads_7d = analyze_ads(aid, period=period_7d) or []

                # Находим adset для этого объявления и проверяем, есть ли другие объявления с spend>0.
                adset_id = None
                for ad in ads_7d:
                    if ad.get("ad_id") == ad_id:
                        adset_id = ad.get("adset_id")
                        break

                has_alternative = False
                if adset_id:
                    for ad in ads_7d:
                        if ad.get("ad_id") == ad_id:
                            continue
                        if ad.get("adset_id") != adset_id:
                            continue
                        if float(ad.get("spend", 0.0) or 0.0) > 0:
                            has_alternative = True
                            break

                if has_alternative:
                    open_url = f"https://www.facebook.com/adsmanager/manage/ad/?ad={ad_id}"
                    text = (
                        "Не удалось автоматически выключить объявление через API. "
                        "Открой его вручную в Ads Manager и отключи там:"
                    )
                    kb = InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    "Открыть объявление",
                                    url=open_url,
                                )
                            ]
                        ]
                    )
                    await context.bot.send_message(chat_id, text, reply_markup=kb)
            except Exception:
                # Вспомогательный блок не должен ломать основной обработчик.
                pass

            return

        st = load_accounts()
        row = st.get(aid) or {}
        alerts = row.get("alerts") or {}
        ad_alerts = alerts.get("ad_alerts") or {}
        cfg = ad_alerts.get(ad_id) or {}
        cfg["enabled"] = False
        ad_alerts[ad_id] = cfg
        alerts["ad_alerts"] = ad_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        await q.answer(
            "Объявление выключено, алёрты по нему больше не будут приходить.",
            show_alert=False,
        )
        return

    if data == "insta_links_menu":
        # Сценарий получения ссылок на активную инста-рекламу.
        await safe_edit_message(
            q,
            "Выберите рекламный аккаунт для получения ссылок на активную рекламу в Instagram:",
            reply_markup=accounts_kb("insta_links_acc"),
        )
        return

    if data == "monitoring_menu":
        await safe_edit_message(
            q,
            "Раздел мониторинга. Выберите пункт:",
            reply_markup=monitoring_menu_kb(),
        )
        return

    if data == "focus_ai_menu":
        await safe_edit_message(
            q,
            "🎯 Фокус-ИИ\n\n"
            "Выберите режим:",
            reply_markup=focus_ai_main_kb(),
        )
        return

    if data == "anomalies_menu":
        # Выбор аккаунта для проверки аномалий по адсетам.
        await safe_edit_message(
            q,
            "Выберите аккаунт для анализа аномалий по адсетам:",
            reply_markup=accounts_kb("anomalies_acc"),
        )
        return

    if data.startswith("insta_links_acc|"):
        aid = data.split("|", 1)[1]
        account_name = get_account_name(aid)

        await safe_edit_message(
            q,
            f"🔗 Ссылки на рекламу — {account_name}\n\n"
            "Собираю активные инста-объявления...",
        )

        items = fetch_instagram_active_ads_links(aid)
        messages = format_instagram_ads_links(items)

        for msg in messages:
            await context.bot.send_message(chat_id, msg)
            # Небольшая пауза, чтобы не заддосить Telegram при большом количестве ссылок
            await asyncio.sleep(0.3)
        return

    if data.startswith("anomalies_acc|"):
        aid = data.split("|", 1)[1]
        account_name = get_account_name(aid)

        await safe_edit_message(
            q,
            f"⚠️ Анализ аномалий по адсетам для {account_name}…",
        )

        messages = build_anomaly_messages_for_account(aid)

        if not messages:
            await context.bot.send_message(
                chat_id,
                f"⚠️ Для аккаунта {account_name} аномалий по адсетам не обнаружено.",
            )
            return

        for msg in messages:
            await context.bot.send_message(chat_id, msg)
            await asyncio.sleep(0.3)
        return

    # ==== Фокус-ИИ: сценарий настроек ====

    if data == "focus_ai_settings":
        await safe_edit_message(
            q,
            "🎯 Фокус-ИИ — настройки\n\n"
            "Сначала выбери рекламный аккаунт, для которого будем настраивать Фокус-ИИ:",
            reply_markup=accounts_kb("focus_ai_acc"),
        )
        return

    if data.startswith("focus_ai_acc|"):
        aid = data.split("|", 1)[1]
        context.user_data["focus_ai_settings_aid"] = aid
        await safe_edit_message(
            q,
            f"🎯 Фокус-ИИ — настройки для {get_account_name(aid)}\n\n"
            "Выбери уровень, на котором будет работать Фокус-ИИ:",
            reply_markup=focus_ai_level_kb_settings(),
        )
        return

    if data.startswith("focus_ai_set_level|"):
        _prefix, level = data.split("|", 1)
        aid = context.user_data.get("focus_ai_settings_aid")
        if not aid:
            await safe_edit_message(
                q,
                "Не удалось определить аккаунт для настроек Фокус-ИИ. Вернись назад и выбери аккаунт ещё раз.",
                reply_markup=accounts_kb("focus_ai_acc"),
            )
            return

        if level != "account":
            level_human = {
                "campaign": "Кампании",
                "adset": "Адсеты",
                "ad": "Объявления",
            }.get(level, level)
            await safe_edit_message(
                q,
                f"Уровень '{level_human}' пока в разработке.\n\n"
                "Сейчас можно включить Фокус-ИИ только на уровне всего аккаунта.",
                reply_markup=focus_ai_level_kb_settings(),
            )
            return

        # Сохраняем простейшую настройку Фокус-ИИ: пользователь → уровень "account" по aid
        st = load_accounts()
        row = st.get(aid, {})
        focus = row.get("focus") or {}
        uid = str(update.effective_user.id)
        focus[uid] = {"level": "account", "enabled": True}
        row["focus"] = focus
        st[aid] = row
        save_accounts(st)

        await safe_edit_message(
            q,
            f"🎯 Фокус-ИИ включён для аккаунта {get_account_name(aid)} на уровне всего аккаунта.\n\n"
            "Дальше Фокус-ИИ будет использоваться при почасовом мониторинге и разовых отчётах.",
            reply_markup=focus_ai_main_kb(),
        )
        return

    # ==== Фокус-ИИ: разовый отчёт ====

    if data == "focus_ai_now":
        uid = str(update.effective_user.id)
        if _user_has_focus_settings(uid):
            await safe_edit_message(
                q,
                "📊 Разовый отчёт Фокус-ИИ по уже настроенным объектам пока в разработке.\n\n"
                "План: бот возьмёт текущие цели Фокус-ИИ, сравнит несколько периодов и предложит действия.",
                reply_markup=focus_ai_main_kb(),
            )
            return

        await safe_edit_message(
            q,
            "📊 Разовый отчёт Фокус-ИИ\n\n"
            "Сначала выбери аккаунт, по которому нужен отчёт:",
            reply_markup=accounts_kb("focus_ai_now_acc"),
        )
        return

    if data.startswith("focus_ai_now_acc|"):
        aid = data.split("|", 1)[1]
        context.user_data["focus_ai_now_aid"] = aid
        await safe_edit_message(
            q,
            f"📊 Разовый отчёт Фокус-ИИ для {get_account_name(aid)}\n\n"
            "Выбери уровень, по которому хотешь посмотреть отчёт:",
            reply_markup=focus_ai_level_kb_now(),
        )
        return

    if data.startswith("focus_ai_now_level|"):
        _prefix, level = data.split("|", 1)
        aid = context.user_data.get("focus_ai_now_aid")
        if not aid:
            await safe_edit_message(
                q,
                "Не удалось определить аккаунт для отчёта Фокус-ИИ. Вернись назад и выбери аккаунт ещё раз.",
                reply_markup=accounts_kb("focus_ai_now_acc"),
            )
            return

        # Сохраняем уровень и предлагаем выбрать период.
        context.user_data["focus_ai_now_level"] = level
        level_human = {
            "account": "Аккаунт",
            "campaign": "Кампании",
            "adset": "Адсеты",
            "ad": "Объявления",
        }.get(level, level)

        await safe_edit_message(
            q,
            "📊 Разовый отчёт Фокус-ИИ\n\n"
            f"Объект: {get_account_name(aid)} — уровень: {level_human}.\n\n"
            "Выбери период для анализа:",
            reply_markup=focus_ai_period_kb(level),
        )
        return

    if data.startswith("focus_ai_now_period|"):
        # Формат: focus_ai_now_period|{level}|{mode}
        _, level, mode = data.split("|", 2)
        aid = context.user_data.get("focus_ai_now_aid")
        if not aid:
            await safe_edit_message(
                q,
                "Не удалось определить аккаунт для отчёта Фокус-ИИ. Вернись назад и выбери аккаунт ещё раз.",
                reply_markup=accounts_kb("focus_ai_now_acc"),
            )
            return

        level_human = {
            "account": "Аккаунт",
            "campaign": "Кампании",
            "adset": "Адсеты",
            "ad": "Объявления",
        }.get(level, level)

        period_human = {
            "today": "Сегодня",
            "yday": "Вчера",
            "7d": "Последние 7 дней",
            "30d": "Последние 30 дней",
            "custom": "Свой период",
        }.get(mode, "Последние 7 дней")

        # Для custom сначала просим ввести диапазон дат в свободном вводе.
        if mode == "custom":
            context.user_data["focus_ai_now_custom_ctx"] = {
                "aid": aid,
                "level": level,
            }
            await safe_edit_message(
                q,
                "🗓 Фокус-ИИ — свой период\n\n"
                f"Объект: {get_account_name(aid)} — уровень: {level_human}.\n\n"
                "Введи даты форматом: 01.06.2025-07.06.2025",
                reply_markup=focus_ai_period_kb(level),
            )
            return

        # Показываем пользователю понятный индикатор, что Фокус-ИИ работает.
        await safe_edit_message(
            q,
            "🧠 Фокус-ИИ думает...\n"
            f"Анализирую данные по аккаунту и уровню '{level_human}' за период: {period_human}...",
        )

        # Собираем данные по выбранному уровню и периоду.
        from services.analytics import _make_period_for_mode  # локальный импорт, чтобы избежать циклов

        # Для custom пока используем fallback 7 дней, но передаём маркер в контекст.
        mode_for_period = mode if mode in {"today", "yday", "7d", "30d"} else "7d"
        period_dict = _make_period_for_mode(mode_for_period)

        if level == "account":
            base_analysis = analyze_account(aid, period=period_dict)
            heat = build_heatmap_for_account(aid, get_account_name, mode="7")

            data_for_analysis = {
                "scope": "account",
                "account_id": aid,
                "account_name": get_account_name(aid),
                "period_mode": mode,
                "period_label": period_human,
                "period": period_dict,
                "metrics": base_analysis.get("metrics"),
                "heatmap_7d": heat,
            }
        elif level == "campaign":
            camps = analyze_campaigns(aid, period=period_dict) or []
            data_for_analysis = {
                "scope": "campaign",
                "account_id": aid,
                "account_name": get_account_name(aid),
                "period_mode": mode,
                "period_label": period_human,
                "period": period_dict,
                "campaigns": camps,
            }
        elif level == "adset":
            adsets = analyze_adsets(aid, period=period_dict) or []
            data_for_analysis = {
                "scope": "adset",
                "account_id": aid,
                "account_name": get_account_name(aid),
                "period_mode": mode,
                "period_label": period_human,
                "period": period_dict,
                "adsets": adsets,
            }
        elif level == "ad":
            ads = analyze_ads(aid, period=period_dict) or []
            data_for_analysis = {
                "scope": "ad",
                "account_id": aid,
                "account_name": get_account_name(aid),
                "period_mode": mode,
                "period_label": period_human,
                "period": period_dict,
                "ads": ads,
            }
        else:
            await safe_edit_message(
                q,
                "Неизвестный уровень для Фокус-ИИ.",
                reply_markup=focus_ai_main_kb(),
            )
            return

        system_msg = (
            "Ты — продвинутый аналитик для Facebook Ads (Focus-ИИ). "
            "Отвечай ТОЛЬКО на русском языке, понятным маркетологу. "
            "Используй русские названия метрик: показы, клики, расходы, сообщения/заявки, стоимость заявки (CPA), CTR, частота показов. "
            "Тебе переданы данные по аккаунту и объектам рекламной структуры (аккаунт/кампании/адсеты/объявления) за указанный период. "
            "Нужно выявить тренды, аномалии и дать рекомендацию по бюджету и действиям. "
            "Если передан список кампаний/адсетов/объявлений, опиши коротко каждый объект отдельным абзацем внутри поля 'analysis' (по одному абзацу на объект), "
            "а затем добавь общий вывод. "
            "Если по объектам есть отдельные рекомендации, добавь поле 'objects' — массив объектов вида: "
            "[{""id"":""..."", ""name"":""..."", ""level"":""campaign""|""adset""|""ad"", ""recommendation"":""increase_budget""|""decrease_budget""|""keep""|""check_creatives"", ""suggested_change_percent"":число, ""confidence"":0-100}]. "
            "Всегда отвечай СТРОГО одним JSON-объектом со структурой: "
            "{""status"":""ok""|""error"", ""analysis"":""..."", ""reason"":""..."", ""recommendation"":""increase_budget""|""decrease_budget""|""keep""|""check_creatives"", ""confidence"":0-100, ""suggested_change_percent"":число, ""objects"":[...]}. "
            "Не добавляй никакого текста вне JSON."
        )

        user_msg = json.dumps(data_for_analysis, ensure_ascii=False)

        try:
            ds_resp = await ask_deepseek(
                [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                json_mode=True,
            )

            choice = (ds_resp.get("choices") or [{}])[0]
            content = (choice.get("message") or {}).get("content") or ""
            parsed = json.loads(content)
        except Exception as e:
            parsed = {
                "status": "error",
                "analysis": "Фокус-ИИ временно недоступен. Используй стандартные отчёты по аккаунту.",
                "reason": f"DeepSeek error: {e}",
                "recommendation": "keep",
                "confidence": 0,
                "suggested_change_percent": 0,
            }

        status = parsed.get("status", "ok")
        analysis_text = parsed.get("analysis") or "Без текста анализа."
        reason_text = parsed.get("reason") or "Причина не указана."
        rec = parsed.get("recommendation") or "keep"
        conf = parsed.get("confidence") or 0
        delta = parsed.get("suggested_change_percent") or 0
        objects = parsed.get("objects") or []

        # Пытаемся вывести человекочитаемый текст периода, если он есть в данных.
        period_label = data_for_analysis.get("period_label") or period_human

        text_lines = [
            "📊 Разовый отчёт Фокус-ИИ",
            "",
            f"Объект: {get_account_name(aid)} — уровень: {level_human}.",
            f"Период: {period_human}",
            "",
            f"Период: {period_label}",
            "",
            f"Анализ: {analysis_text}",
            f"Причина: {reason_text}",
            f"Рекомендация: {rec} ({delta:+}%)",
            f"Уверенность: {conf}%",
        ]

        if status != "ok":
            text_lines.append("\n⚠️ При обработке запроса возникла ошибка, проверь данные вручную.")

        await update.message.reply_text(
            "\n".join(text_lines),
            reply_markup=focus_ai_recommendation_kb(level, rec, float(delta), objects),
        )
        return

    if data.startswith("focus_ai_action|"):
        # Пока только подтверждаем получение действия от пользователя.
        # В следующих итерациях сюда будет добавлена реальная логика изменения бюджетов.
        _prefix, lvl, action, delta_str = data.split("|", 3)
        delta_val = 0
        try:
            delta_val = int(delta_str)
        except Exception:
            delta_val = 0

        human_action = {
            "inc": "увеличение бюджета",
            "dec": "снижение бюджета",
            "keep": "оставить как есть",
            "manual": "ручной ввод",
        }.get(action, action)

        await safe_edit_message(
            q,
            f"Фокус-ИИ: получено действие '{human_action}' для уровня '{lvl}' (Δ={delta_val}%).\n"
            "Реальные изменения бюджета будут добавлены на следующем этапе.",
            reply_markup=focus_ai_main_kb(),
        )
        return

    if data.startswith("focus_ai_obj|"):
        # Формат: focus_ai_obj|adset|{adset_id}|inc|20
        _prefix, obj_level, obj_id, action, delta_str = data.split("|", 4)
        try:
            delta_val = int(delta_str)
        except Exception:
            delta_val = 0

        if obj_level != "adset":
            await q.answer("Пока можно применять бюджеты только на уровне адсета.", show_alert=True)
            return

        # Подтверждение перед реальным изменением бюджета.
        text = (
            "Подтверждение действия Фокус-ИИ:\n\n"
            f"Объект: adset {obj_id}\n"
            f"Действие: {'увеличить' if action == 'inc' else 'уменьшить'} бюджет на {delta_val:+d}%\n\n"
            "Применить изменение бюджета?"
        )
        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "✅ Да",
                        callback_data=f"focus_ai_obj_confirm|adset|{obj_id}|{action}|{delta_val}",
                    ),
                    InlineKeyboardButton(
                        "❌ Отмена",
                        callback_data="focus_ai_obj_cancel",
                    ),
                ]
            ]
        )

        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("focus_ai_obj_confirm|"):
        # Формат: focus_ai_obj_confirm|adset|{adset_id}|inc|20
        _p, obj_level, obj_id, action, delta_str = data.split("|", 4)
        try:
            delta_val = float(delta_str)
        except Exception:
            delta_val = 0.0

        if obj_level != "adset":
            await safe_edit_message(
                q,
                "Можно подтверждать только изменения бюджета на уровне адсета.",
            )
            return

        # Если рекомендация была на снижение (dec), передаём отрицательный процент.
        if action == "dec" and delta_val > 0:
            delta_val = -delta_val

        res = apply_budget_change(obj_id, delta_val)
        status = res.get("status")
        msg = res.get("message") or "Бюджет обновлён."

        if status != "ok":
            text = f"⚠️ Не удалось применить изменение бюджета: {msg}"
        else:
            old_b = res.get("old_budget")
            new_b = res.get("new_budget")
            text = (
                "✅ Изменение бюджета применено.\n\n"
                f"Adset: {obj_id}\n"
                f"Старый бюджет: {old_b:.2f} $\n"
                f"Новый бюджет: {new_b:.2f} $\n"
                f"Δ: {delta_val:+.0f}%"
            )

        await safe_edit_message(q, text)
        return

    if data == "focus_ai_obj_cancel":
        await safe_edit_message(q, "Действие Фокус-ИИ отменено.")
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

        # Кампании / адсеты / объявления: используем analyze_*/fetch_instagram_active_ads_links
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

    if data.startswith("cpa_settings|"):
        aid = data.split("|", 1)[1]
        text, kb = cpa_settings_kb(aid)
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("cpa_campaigns|"):
        aid = data.split("|", 1)[1]

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        campaign_alerts = alerts.get("campaign_alerts", {}) or {}

        try:
            camps = analyze_campaigns(aid, days=7) or []
        except Exception:
            camps = []

        kb_rows = []
        for camp in camps:
            cid = camp.get("campaign_id")
            if not cid:
                continue
            name = camp.get("name") or cid
            cfg_c = (campaign_alerts.get(cid) or {}) if cid in campaign_alerts else {}
            target = float(cfg_c.get("target_cpa") or 0.0)
            label_suffix = (
                f"[CPA {target:.2f}$]" if target > 0 else "[CPA аккаунта]"
            )
            text_btn = f"{name} {label_suffix}".strip()

            kb_rows.append(
                [
                    InlineKeyboardButton(
                        text_btn,
                        callback_data=f"cpa_campaign|{aid}|{cid}",
                    )
                ]
            )

        kb_rows.append(
            [
                InlineKeyboardButton(
                    "⬅️ Назад", callback_data=f"cpa_settings|{aid}"
                )
            ]
        )

        text = "Выбери кампанию для настройки CPA-алёртов."
        await safe_edit_message(q, text, reply_markup=InlineKeyboardMarkup(kb_rows))
        return

    if data.startswith("cpa_campaign|"):
        _, aid, campaign_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        campaign_alerts = alerts.setdefault("campaign_alerts", {})
        cfg = campaign_alerts.get(campaign_id) or {}

        try:
            camps = analyze_campaigns(aid, days=7) or []
        except Exception:
            camps = []

        camp_name = campaign_id
        for camp in camps:
            if camp.get("campaign_id") == campaign_id:
                camp_name = camp.get("name") or campaign_id
                break

        account_cpa = _resolve_account_cpa(alerts)
        target_cpa = float(cfg.get("target_cpa") or 0.0)
        effective_target = target_cpa if target_cpa > 0 else account_cpa
        enabled = bool(cfg.get("enabled", True))

        mode_str = "свой таргет" if target_cpa > 0 else "наследует CPA аккаунта"
        status_str = "ВКЛ" if enabled else "ВЫКЛ"

        text = (
            "CPA-алёрты для кампании:\n\n"
            f"{camp_name}\n\n"
            f"Эффективный target CPA: {effective_target:.2f} $ ({mode_str})\n"
            f"Статус CPA-алёртов кампании: {status_str}"
        )

        toggle_text = (
            "⚠️ CPA-алёрты кампании: ON" if enabled else "⚠️ CPA-алёрты кампании: OFF"
        )

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        toggle_text,
                        callback_data=f"cpa_campaign_toggle|{aid}|{campaign_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "✏️ Задать CPA для кампании",
                        callback_data=f"cpa_campaign_set|{aid}|{campaign_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "↩️ Наследовать CPA аккаунта",
                        callback_data=f"cpa_campaign_inherit|{aid}|{campaign_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Назад к списку кампаний",
                        callback_data=f"cpa_campaigns|{aid}",
                    )
                ],
            ]
        )

        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("cpa_campaign_toggle|"):
        _, aid, campaign_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        campaign_alerts = alerts.setdefault("campaign_alerts", {})
        cfg = campaign_alerts.get(campaign_id) or {}

        cfg["enabled"] = not bool(cfg.get("enabled", True))
        campaign_alerts[campaign_id] = cfg
        alerts["campaign_alerts"] = campaign_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        data = f"cpa_campaign|{aid}|{campaign_id}"
        update.callback_query.data = data
        await on_cb(update, context)
        return

    if data.startswith("cpa_campaign_set|"):
        _, aid, campaign_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        campaign_alerts = alerts.setdefault("campaign_alerts", {})
        cfg = campaign_alerts.get(campaign_id) or {}

        current = float(cfg.get("target_cpa") or 0.0)

        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        await safe_edit_message(
            q,
            (
                f"⚠️ Текущий CPA для кампании: {current:.2f} $.\n"
                "Напиши в чат число в долларах (например 1.2). 0 — будет наследовать CPA аккаунта."
            ),
        )

        context.user_data["await_cpa_campaign_for"] = {
            "aid": aid,
            "campaign_id": campaign_id,
        }
        return

    if data.startswith("cpa_campaign_inherit|"):
        _, aid, campaign_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        campaign_alerts = alerts.setdefault("campaign_alerts", {})
        cfg = campaign_alerts.get(campaign_id) or {}

        cfg["target_cpa"] = 0.0
        campaign_alerts[campaign_id] = cfg
        alerts["campaign_alerts"] = campaign_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        data = f"cpa_campaign|{aid}|{campaign_id}"
        update.callback_query.data = data
        await on_cb(update, context)
        return

    if data.startswith("cpa_ai|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        alerts["ai_enabled"] = not bool(alerts.get("ai_enabled", True))
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        text, kb = cpa_settings_kb(aid)
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("cpa_freq|"):
        _, aid, freq = data.split("|", 2)
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        alerts["freq"] = freq if freq in ("3x", "hourly") else "3x"
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        text, kb = cpa_settings_kb(aid)
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("cpa_day|"):
        _, aid, day_key = data.split("|", 2)
        all_days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        if day_key not in all_days:
            return
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        days = alerts.get("days") or []
        if day_key in days:
            days = [d for d in days if d != day_key]
        else:
            days = list({*days, day_key})
        alerts["days"] = days
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        text, kb = cpa_settings_kb(aid)
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("cpa_days_all|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        alerts["days"] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        text, kb = cpa_settings_kb(aid)
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("cpa_adsets|"):
        aid = data.split("|", 1)[1]

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        adset_alerts = alerts.get("adset_alerts", {}) or {}

        # Для списка адсетов переиспользуем send_adset_report-источник:
        # модуль adsets уже работает с актуальными данными, здесь берём
        # только имена/ID через вспомогательную функцию.
        from .adsets import list_adsets_for_account

        adsets = list_adsets_for_account(aid)

        kb_rows = []
        for it in adsets:
            adset_id = it.get("id")
            name = it.get("name", adset_id)
            cfg = (adset_alerts.get(adset_id) or {}) if adset_id else {}

            target = float(cfg.get("target_cpa") or 0.0)
            label_suffix = (
                f"[CPA {target:.2f}$]" if target > 0 else "[CPA аккаунта]"
            )
            text_btn = f"{name} {label_suffix}".strip()

            kb_rows.append(
                [
                    InlineKeyboardButton(
                        text_btn, callback_data=f"cpa_adset|{aid}|{adset_id}"
                    )
                ]
            )

        kb_rows.append(
            [
                InlineKeyboardButton(
                    "⬅️ Назад", callback_data=f"cpa_settings|{aid}"
                )
            ]
        )

        text = "Выбери адсет для настройки CPA-алёртов."
        await safe_edit_message(q, text, reply_markup=InlineKeyboardMarkup(kb_rows))
        return

    if data.startswith("cpa_adset|"):
        _, aid, adset_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        adset_alerts = alerts.setdefault("adset_alerts", {})
        cfg = adset_alerts.get(adset_id) or {}

        from .adsets import get_adset_name

        adset_name = get_adset_name(aid, adset_id)

        account_cpa = float(
            alerts.get("account_cpa", alerts.get("target_cpl", 0.0)) or 0.0
        )
        adset_target = float(cfg.get("target_cpa") or 0.0)
        effective_target = adset_target if adset_target > 0 else account_cpa

        enabled = bool(cfg.get("enabled", True))

        mode_str = "свой таргет" if adset_target > 0 else "наследует CPA аккаунта"
        status_str = "ВКЛ" if enabled else "ВЫКЛ"

        text = (
            f"CPA-алёрты для адсета:\n\n"
            f"{adset_name}\n\n"
            f"Эффективный target CPA: {effective_target:.2f} $ ({mode_str})\n"
            f"Статус: CPA-алёрты адсета: {status_str}"
        )

        toggle_text = (
            "⚠️ CPA-алёрты адсета: ON" if enabled else "⚠️ CPA-алёрты адсета: OFF"
        )

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        toggle_text,
                        callback_data=f"cpa_adset_toggle|{aid}|{adset_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "✏️ Задать CPA для адсета",
                        callback_data=f"cpa_adset_set|{aid}|{adset_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "↩️ Наследовать CPA аккаунта",
                        callback_data=f"cpa_adset_inherit|{aid}|{adset_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Назад к списку адсетов",
                        callback_data=f"cpa_adsets|{aid}",
                    )
                ],
            ]
        )

        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("cpa_adset_toggle|"):
        _, aid, adset_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        adset_alerts = alerts.setdefault("adset_alerts", {})
        cfg = adset_alerts.get(adset_id) or {}

        cfg["enabled"] = not bool(cfg.get("enabled", True))
        adset_alerts[adset_id] = cfg
        alerts["adset_alerts"] = adset_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        # Перерисовываем экран настроек адсета
        data = f"cpa_adset|{aid}|{adset_id}"
        update.callback_query.data = data
        await on_cb(update, context)
        return

    if data.startswith("cpa_ads|"):
        aid = data.split("|", 1)[1]

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.get("ad_alerts", {}) or {}

        try:
            ads = analyze_ads(aid, days=7) or []
        except Exception:
            ads = []

        kb_rows = []
        for ad in ads:
            ad_id = ad.get("ad_id") or ad.get("id")
            if not ad_id:
                continue

            spend = float(ad.get("spend", 0.0) or 0.0)
            if ad_id not in ad_alerts and spend <= 0:
                continue

            name = ad.get("name") or ad_id
            cfg = ad_alerts.get(ad_id) or {}
            target = float(cfg.get("target_cpa") or 0.0)
            label_suffix = (
                f"[CPA {target:.2f}$]" if target > 0 else "[CPA вышестоящего уровня]"
            )
            text_btn = f"{name} {label_suffix}".strip()

            kb_rows.append(
                [
                    InlineKeyboardButton(
                        text_btn,
                        callback_data=f"cpa_ad_cfg|{aid}|{ad_id}",
                    )
                ]
            )

        kb_rows.append(
            [
                InlineKeyboardButton(
                    "⬅️ Назад", callback_data=f"cpa_settings|{aid}"
                )
            ]
        )

        text = "Выбери объявление для настройки CPA-алёртов."
        await safe_edit_message(q, text, reply_markup=InlineKeyboardMarkup(kb_rows))
        return

    if data.startswith("cpa_ad_cfg|"):
        _, aid, ad_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.setdefault("ad_alerts", {})
        cfg = ad_alerts.get(ad_id) or {}

        try:
            ads = analyze_ads(aid, days=7) or []
        except Exception:
            ads = []

        ad_name = ad_id
        for ad in ads:
            if (ad.get("ad_id") or ad.get("id")) == ad_id:
                ad_name = ad.get("name") or ad_id
                break

        enabled = bool(cfg.get("enabled", True))
        target_cpa = float(cfg.get("target_cpa") or 0.0)
        silent = bool(cfg.get("silent", False))

        mode_str = (
            "свой таргет" if target_cpa > 0 else "наследует CPA вышестоящего уровня"
        )
        effective_str = f"{target_cpa:.2f} $" if target_cpa > 0 else "—"
        status_str = "ВКЛ" if enabled else "ВЫКЛ"
        silent_str = "ВКЛ" if silent else "ВЫКЛ"

        text = (
            "CPA-алёрты для объявления:\n\n"
            f"{ad_name}\n\n"
            f"Эффективный target CPA: {effective_str} ({mode_str})\n"
            f"Статус CPA-алёртов: {status_str}\n"
            f"Тихий режим: {silent_str}"
        )

        toggle_text = (
            "⚠️ CPA-алёрты объявления: ON"
            if enabled
            else "⚠️ CPA-алёрты объявления: OFF"
        )
        silent_btn_text = (
            "🔕 Тихий режим: OFF" if silent else "🔕 Тихий режим: ON"
        )

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        toggle_text,
                        callback_data=f"cpa_ad_cfg_toggle|{aid}|{ad_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "✏️ Задать CPA объявления",
                        callback_data=f"cpa_ad_cfg_set|{aid}|{ad_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "↩️ Наследовать CPA вышестоящего уровня",
                        callback_data=f"cpa_ad_cfg_inherit|{aid}|{ad_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        silent_btn_text,
                        callback_data=f"cpa_ad_silent|{aid}|{ad_id}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Назад к списку объявлений",
                        callback_data=f"cpa_ads|{aid}",
                    )
                ],
            ]
        )

        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("cpa_ad_cfg_toggle|"):
        _, aid, ad_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.setdefault("ad_alerts", {})
        cfg = ad_alerts.get(ad_id) or {}

        cfg["enabled"] = not bool(cfg.get("enabled", True))
        ad_alerts[ad_id] = cfg
        alerts["ad_alerts"] = ad_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        data = f"cpa_ad_cfg|{aid}|{ad_id}"
        update.callback_query.data = data
        await on_cb(update, context)
        return

    if data.startswith("cpa_ad_cfg_set|"):
        _, aid, ad_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.setdefault("ad_alerts", {})
        cfg = ad_alerts.get(ad_id) or {}

        current = float(cfg.get("target_cpa") or 0.0)

        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        await safe_edit_message(
            q,
            (
                f"⚠️ Текущий CPA для объявления: {current:.2f} $.\n"
                "Напиши в чат число в долларах (например 1.2). 0 — будет наследовать CPA вышестоящего уровня."
            ),
        )

        context.user_data["await_cpa_ad_for"] = {"aid": aid, "ad_id": ad_id}
        return

    if data.startswith("cpa_ad_cfg_inherit|"):
        _, aid, ad_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.setdefault("ad_alerts", {})
        cfg = ad_alerts.get(ad_id) or {}

        cfg["target_cpa"] = 0.0
        ad_alerts[ad_id] = cfg
        alerts["ad_alerts"] = ad_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        data = f"cpa_ad_cfg|{aid}|{ad_id}"
        update.callback_query.data = data
        await on_cb(update, context)
        return

    if data.startswith("cpa_adset_set|"):
        _, aid, adset_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        adset_alerts = alerts.setdefault("adset_alerts", {})
        cfg = adset_alerts.get(adset_id) or {}

        current = float(cfg.get("target_cpa") or 0.0)

        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        await safe_edit_message(
            q,
            (
                f"⚠️ Текущий CPA для адсета: {current:.2f} $.\n"
                f"Напиши в чат число в долларах (например 1.2). 0 — будет наследовать CPA аккаунта."
            ),
        )

        context.user_data["await_cpa_adset_for"] = {"aid": aid, "adset_id": adset_id}
        return

    if data.startswith("cpa_adset_inherit|"):
        _, aid, adset_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        adset_alerts = alerts.setdefault("adset_alerts", {})
        cfg = adset_alerts.get(adset_id) or {}

        # Наследование CPA аккаунта: обнуляем собственный таргет.
        cfg["target_cpa"] = 0.0
        adset_alerts[adset_id] = cfg
        alerts["adset_alerts"] = adset_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        data = f"cpa_adset|{aid}|{adset_id}"
        update.callback_query.data = data
        await on_cb(update, context)
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
        alerts = row.get("alerts", {}) or {}

        # Переключатель включает/выключает алёрты целиком.
        # Логика включения: есть ли ненулевой таргет CPA (account_cpa/target_cpl).
        if alerts.get("enabled", False):
            alerts["enabled"] = False
        else:
            acc_cpa = float(alerts.get("account_cpa", 0.0) or 0.0)
            old = float(alerts.get("target_cpl", 0.0) or 0.0)
            alerts["enabled"] = (acc_cpa > 0) or (old > 0)

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
        alerts = row.get("alerts", {}) or {}
        current = float(
            alerts.get("account_cpa", alerts.get("target_cpl", 0.0)) or 0.0
        )
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
        alerts = row.get("alerts", {}) or {}

        new_cpa = float(val)
        # Пишем и в новое поле account_cpa, и в старое target_cpl для совместимости.
        alerts["account_cpa"] = new_cpa
        alerts["target_cpl"] = new_cpa
        alerts["enabled"] = new_cpa > 0

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

    if "await_cpa_campaign_for" in context.user_data:
        payload = context.user_data.pop("await_cpa_campaign_for")
        aid = payload.get("aid")
        campaign_id = payload.get("campaign_id")

        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text(
                "Введите число, например: 1.2 (или 0 чтобы наследовать CPA аккаунта)"
            )
            context.user_data["await_cpa_campaign_for"] = payload
            return

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        campaign_alerts = alerts.setdefault("campaign_alerts", {})
        cfg = campaign_alerts.get(campaign_id) or {}

        new_cpa = float(val)
        cfg["target_cpa"] = new_cpa
        if new_cpa > 0:
            cfg["enabled"] = True

        campaign_alerts[campaign_id] = cfg
        alerts["campaign_alerts"] = campaign_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        try:
            camps = analyze_campaigns(aid, days=7) or []
        except Exception:
            camps = []

        name = campaign_id
        for camp in camps:
            if camp.get("campaign_id") == campaign_id:
                name = camp.get("name") or campaign_id
                break

        if new_cpa > 0:
            await update.message.reply_text(
                f"✅ CPA для кампании '{name}' обновлён: {new_cpa:.2f} $ (алерты ВКЛ)"
            )
        else:
            await update.message.reply_text(
                f"✅ CPA для кампании '{name}' установлен 0 — будет наследовать CPA аккаунта"
            )
        return

    if "await_cpa_adset_for" in context.user_data:
        payload = context.user_data.pop("await_cpa_adset_for")
        aid = payload.get("aid")
        adset_id = payload.get("adset_id")

        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text(
                "Введите число, например: 1.2 (или 0 чтобы наследовать CPA аккаунта)"
            )
            context.user_data["await_cpa_adset_for"] = payload
            return

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        adset_alerts = alerts.setdefault("adset_alerts", {})
        cfg = adset_alerts.get(adset_id) or {}

        new_cpa = float(val)
        cfg["target_cpa"] = new_cpa
        # По умолчанию адсет считается включённым, если есть свой CPA > 0.
        if new_cpa > 0:
            cfg["enabled"] = True

        adset_alerts[adset_id] = cfg
        alerts["adset_alerts"] = adset_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        from .adsets import get_adset_name

        name = get_adset_name(aid, adset_id)

        if new_cpa > 0:
            await update.message.reply_text(
                f"✅ CPA для адсета '{name}' обновлён: {new_cpa:.2f} $ (алерты ВКЛ)"
            )
        else:
            await update.message.reply_text(
                f"✅ CPA для адсета '{name}' установлен 0 — будет наследовать CPA аккаунта"
            )
        return

    if "await_cpa_ad_for" in context.user_data:
        payload = context.user_data.pop("await_cpa_ad_for")
        aid = payload.get("aid")
        ad_id = payload.get("ad_id")

        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text(
                "Введите число, например: 1.2 (или 0 чтобы наследовать CPA вышестоящего уровня)"
            )
            context.user_data["await_cpa_ad_for"] = payload
            return

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.setdefault("ad_alerts", {})
        cfg = ad_alerts.get(ad_id) or {}

        new_cpa = float(val)
        cfg["target_cpa"] = new_cpa
        if new_cpa > 0:
            cfg["enabled"] = True

        ad_alerts[ad_id] = cfg
        alerts["ad_alerts"] = ad_alerts
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)

        try:
            ads = analyze_ads(aid, days=7) or []
        except Exception:
            ads = []

        name = ad_id
        for ad in ads:
            if (ad.get("ad_id") or ad.get("id")) == ad_id:
                name = ad.get("name") or ad_id
                break

        if new_cpa > 0:
            await update.message.reply_text(
                f"✅ CPA для объявления '{name}' обновлён: {new_cpa:.2f} $ (алерты ВКЛ)"
            )
        else:
            await update.message.reply_text(
                f"✅ CPA для объявления '{name}' установлен 0 — будет наследовать CPA вышестоящего уровня"
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

    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_any))

    app.job_queue.run_daily(
        daily_report_job,
        time=time(hour=9, minute=30, tzinfo=ALMATY_TZ),
    )

    app.job_queue.run_daily(
        billing_digest_job,
        time=time(hour=9, minute=45, tzinfo=ALMATY_TZ),
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
