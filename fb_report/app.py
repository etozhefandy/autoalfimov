from datetime import datetime, timedelta, time
import calendar
import re

from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardRemove,
)
from telegram.constants import ChatAction
from telegram.error import BadRequest, NetworkError, TimedOut, RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

import logging
from collections import Counter

from billing_watch import init_billing_watch
from autopilat.actions import apply_budget_change, set_adset_budget, disable_entity, can_disable, parse_manual_input
from history_store import append_autopilot_event, read_autopilot_events

from .constants import (
    ALMATY_TZ,
    TELEGRAM_TOKEN,
    DEFAULT_REPORT_CHAT,
    AUTOPILOT_CHAT_ID,
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
    get_lead_metric_for_account,
    set_lead_metric_for_account,
    clear_lead_metric_for_account,
    get_lead_metric_catalog_for_account,
    set_lead_metric_catalog_for_account,
    set_autopilot_chat_id,
    resolve_autopilot_chat_id,
)
from .reporting import (
    fmt_int,
    get_cached_report,
    build_comparison_report,
    build_report_debug,
    send_period_report,
    parse_range,
    parse_two_ranges,
    build_account_report,
)
from .insights import (
    build_heatmap_for_account,
    build_hourly_heatmap_for_account,
    build_weekday_heatmap_for_account,
    build_heatmap_monitoring_summary,
)
from .creatives import fetch_instagram_active_ads_links, format_instagram_ads_links
from .adsets import send_adset_report
from .billing import send_billing, send_billing_forecast, billing_digest_job
from .jobs import (
    schedule_cpa_alerts,
    _resolve_account_cpa,
    build_heatmap_status_text,
    run_heatmap_snapshot_collector_once,
)
from .autopilot_format import ap_action_text

from services.facebook_api import (
    pause_ad,
    fetch_insights_bulk,
    get_last_api_error_info,
    is_rate_limited_now,
    rate_limit_retry_after_seconds,
    classify_api_error,
    allow_fb_api_calls,
    deny_fb_api_calls,
)
from services.ai_focus import get_focus_comment, ask_deepseek, sanitize_ai_text
from fb_report.cpa_monitoring import build_anomaly_messages_for_account
from services.heatmap_store import (
    get_heatmap_dataset,
    prev_full_hour_window,
    sum_ready_spend_for_date,
    load_snapshot,
    list_snapshot_hours,
)
import json
import asyncio
import time as pytime
import uuid


def _allowed(update: Update) -> bool:
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    user_id = update.effective_user.id if update.effective_user else None
    if chat_id in ALLOWED_CHAT_IDS:
        return True
    if user_id and user_id in ALLOWED_USER_IDS:
        return True
    return False


def _resolve_autopilot_chat_id_logged(*, reason: str) -> tuple[str, str]:
    cid, src = resolve_autopilot_chat_id()
    logging.getLogger(__name__).info(
        "autopilot_chat_resolve autopilot_chat_id=%s source=%s reason=%s",
        str(cid),
        str(src),
        str(reason),
    )
    return str(cid), str(src)


def _autopilot_menu_kb() -> InlineKeyboardMarkup:
    kb = accounts_kb("autopilot_acc")
    rows = list(kb.inline_keyboard)
    rows.insert(0, [InlineKeyboardButton("📌 Сделать этот чат автопилотом", callback_data="ap_set_chat")])
    return InlineKeyboardMarkup(rows)


async def safe_edit_message(q, text: str, **kwargs):
    try:
        return await q.edit_message_text(text=text, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        raise


async def _typing_loop(bot, chat_id: str, stop_event: "asyncio.Event") -> None:
    """Показывает анимацию "бот печатает" пока не будет установлен stop_event.

    Ограничение по времени ~30 секунд, чтобы не спамить action'ами.
    """

    start = datetime.now(ALMATY_TZ)
    while not stop_event.is_set():
        try:
            await bot.send_chat_action(chat_id, ChatAction.TYPING)
        except Exception:
            break

        await asyncio.sleep(2.0)

        if (datetime.now(ALMATY_TZ) - start).total_seconds() > 30:
            break


def _build_version_text() -> str:
    """Текст для команды /version и кнопки "Версия".

    Использует BOT_VERSION и BOT_CHANGELOG: базовые функции + последние значимые
    обновления. Косметические вещи можно не добавлять в BOT_CHANGELOG, тогда
    они не попадут в этот текст автоматически.
    """
    lines = [f"Версия бота: {BOT_VERSION}", ""]
    lines.extend(BOT_CHANGELOG)
    return "\n".join(lines)


def _ap_reason_human(code: str) -> str:
    m = {
        "no_goal_target_cpl": "цель CPL не задана (работаю от базы 3 дня)",
        "insufficient_volume": "недостаточно объёма (мало конверсий/переписок)",
        "no_spend": "нет расходов за период",
        "low_volume": "слишком малый расход/объём для уверенных выводов",
        "no_snapshot": "нет слепка (heatmap cache) — подожду сборщик",
        "snapshot_collecting": "слепок собирается (heatmap cache)",
        "snapshot_failed": "слепок не собран (ошибка)",
        "rate_limit": "лимит Facebook API — сборщик слепков временно не трогает API",
        "cache_used": "📌 источник: heatmap cache",
        "fb_auth_error": "токен/доступ (code 190)",
        "fb_invalid_param": "неверный параметр (code 100)",
        "fb_permission_error": "нет прав/доступа (FB permissions)",
        "fb_unknown_api_error": "неизвестная ошибка Facebook API",
        "api_error": "данные из Facebook временно недоступны",
        "no_campaigns_in_group": "в группе нет кампаний",
        "no_active_entities": "нет активных сущностей/строк в слепке",
        "all_candidates_blocked_by_limits": "все кандидаты вне лимитов режима AUTO_LIMITS",
        "mode_not_supported": "режим не поддерживает почасовой прогон",
        "no_monitored_groups": "нет мониторимых групп",
    }
    return m.get(str(code), str(code))


def _ap_top_reasons(reasons: list[str] | None, *, n: int = 2) -> str:
    if not reasons:
        return ""
    c = Counter([str(x) for x in reasons if x])
    top = [k for k, _v in c.most_common(max(1, int(n)))]
    if not top:
        return ""
    return "; ".join(_ap_reason_human(x) for x in top)


def _autopilot_analysis_kb(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Обновить", callback_data=f"ap_analyze|{aid}")],
            [
                InlineKeyboardButton(
                    "📌 Собрать слепок предыдущего часа",
                    callback_data=f"hm_collect_prev|{aid}",
                )
            ],
            [InlineKeyboardButton("💡 Подобрать действия", callback_data=f"ap_suggest|{aid}")],
            [InlineKeyboardButton("🧪 Dry-run", callback_data=f"ap_dry|{aid}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="ap_menu")],
        ]
    )


def monitoring_compare_accounts_kb(prefix: str) -> InlineKeyboardMarkup:
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
                    f"{get_account_name(aid)}",
                    callback_data=f"{prefix}|{aid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ Мониторинг", callback_data="monitoring_menu")])
    return InlineKeyboardMarkup(rows)


def monitoring_accounts_kb() -> InlineKeyboardMarkup:
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
        row = store.get(aid, {}) if store else {}
        mon = (row.get("monitoring") or {}) if isinstance(row, dict) else {}
        selected = bool(mon.get("compare_enabled", True)) if isinstance(mon, dict) else True
        prefix = "✅ " if selected else ""
        rows.append(
            [
                InlineKeyboardButton(
                    f"{prefix}{_flag_line(aid)}  {get_account_name(aid)}",
                    callback_data=f"mon_acc_toggle|{aid}",
                )
            ]
        )

    rows.append([InlineKeyboardButton("⬅️ Мониторинг", callback_data="monitoring_menu")])
    return InlineKeyboardMarkup(rows)


def _autopilot_hm_kb(aid: str) -> InlineKeyboardMarkup:
    store = load_accounts() or {}
    row = store.get(str(aid)) or {}
    hm = (row or {}).get("heatmap") or {}
    if not isinstance(hm, dict):
        hm = {}
    include_paused = bool(hm.get("include_paused", False))
    toggle_label = f"Показывать PAUSED: {'ON' if include_paused else 'OFF'}"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"ap_hm_p|{aid}|today"),
                InlineKeyboardButton("Вчера", callback_data=f"ap_hm_p|{aid}|yday"),
            ],
            [InlineKeyboardButton("7 дней", callback_data=f"ap_hm_p|{aid}|7d")],
            [InlineKeyboardButton(toggle_label, callback_data=f"ap_hm_toggle_paused|{aid}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"ap_analyze|{aid}")],
            [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="autopilot_menu")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )


def _autopilot_hm_summary(summary: dict) -> str:
    days = (summary or {}).get("days") or []
    if not days:
        return "🕒 Рекомендации по часам: нет данных (нужно накопить hourly_stats)."

    totals = [0 for _ in range(24)]
    for d in days:
        vals = (d or {}).get("totals_per_hour") or []
        for i in range(min(24, len(vals))):
            try:
                totals[i] += int(vals[i] or 0)
            except Exception:
                continue

    total_all = sum(totals)
    if total_all <= 0:
        return "🕒 Рекомендации по часам: за период нет заявок (💬+📩)."

    ranked = sorted([(i, totals[i]) for i in range(24)], key=lambda x: x[1], reverse=True)
    best = [x for x in ranked if x[1] > 0][:4]
    worst = sorted([(i, totals[i]) for i in range(24)], key=lambda x: x[1])[:4]

    def _fmt(xs):
        return ", ".join([f"{h:02d}:00 ({v})" for h, v in xs]) if xs else "—"

    lines = [
        "🕒 Рекомендации по часам (по заявкам 💬+📩)",
        f"Лучшие часы: {_fmt(best)}",
        f"Слабые часы: {_fmt(worst)}",
        "",
        "Идея v1: усиливать показы/бюджет в лучшие часы и аккуратно снижать в слабые.",
        "(Автоприменения нет — только рекомендация.)",
    ]
    return "\n".join(lines)


def _ap_action_kb(*, allow_apply: bool, token: str, allow_edit: bool) -> InlineKeyboardMarkup:
    rows = []
    if allow_apply:
        row = [InlineKeyboardButton("✅ Применить", callback_data=f"apdo|apply|{token}")]
        if allow_edit:
            row.append(InlineKeyboardButton("✏️ Изменить", callback_data=f"apdo|edit|{token}"))
        row.append(InlineKeyboardButton("❌ Отменить", callback_data=f"apdo|cancel|{token}"))
        rows.append(row)
    else:
        rows.append([InlineKeyboardButton("✅ Понял", callback_data=f"apdo|ack|{token}")])
    return InlineKeyboardMarkup(rows)


def _ap_action_text(action: dict) -> str:
    return ap_action_text(action)


def _ap_generate_actions(
    aid: str,
    *,
    eff: dict | None = None,
    debug: bool = False,
) -> list[dict] | tuple[list[dict], list[str]]:
    ap = _autopilot_get(aid)
    mode = str(ap.get("mode") or "OFF").upper()
    eff = eff or _autopilot_effective_config(aid)
    lead_action_type = eff.get("lead_action_type")
    kpi_mode = str(eff.get("kpi") or "total")
    group_campaign_ids = eff.get("campaign_ids")
    group_campaign_set = set(str(x) for x in (group_campaign_ids or []) if x)

    debug_reasons: list[str] = []

    if eff.get("group_id") and not group_campaign_set:
        debug_reasons.append("no_campaigns_in_group")
        return ([], debug_reasons) if debug else []

    # Source of truth: heatmap snapshots.
    debug_reasons.append("cache_used")

    limits = ap.get("limits") or {}

    try:
        max_step = float(limits.get("max_budget_step_pct") or 20)
    except Exception:
        max_step = 20.0
    if max_step <= 0:
        max_step = 20.0
    if max_step > 30:
        max_step = 30.0

    allow_pause_ads = bool(limits.get("allow_pause_ads", True))
    allow_pause_adsets = bool(limits.get("allow_pause_adsets", False))

    win = prev_full_hour_window()
    date_str = str(win.get("date") or "")
    hour_int = int(win.get("hour") or 0)
    window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"

    with deny_fb_api_calls(reason="autopilot_generate_actions"):
        ds, ds_status, ds_reason, ds_meta = get_heatmap_dataset(
            str(aid),
            date_str=date_str,
            hours=[hour_int],
        )

    if ds_status != "ready" or not ds:
        if ds_status == "missing":
            debug_reasons.append("no_snapshot")
        elif ds_status == "collecting":
            debug_reasons.append(str(ds_reason or "snapshot_collecting"))
        else:
            debug_reasons.append(str(ds_reason or "snapshot_failed"))
        return ([], debug_reasons) if debug else []

    rows_src = list((ds or {}).get("rows") or [])

    def _in_group(r: dict) -> bool:
        if not group_campaign_set:
            return True
        return str((r or {}).get("campaign_id") or "") in group_campaign_set

    rows_src = [r for r in rows_src if isinstance(r, dict) and str(r.get("adset_id") or "") and _in_group(r)]

    if not rows_src:
        debug_reasons.append("no_active_entities")
        return ([], debug_reasons) if debug else []

    label_main = f"Окно {window_label}" if window_label.strip("–") else "Предыдущий час"
    label_base = label_main

    def _to_float(v):
        try:
            return float(v)
        except Exception:
            return 0.0

    def _to_int(v):
        try:
            return int(float(v))
        except Exception:
            return 0

    def _cpl(spend: float, leads: int):
        if leads <= 0:
            return None
        if spend <= 0:
            return 0.0
        return float(spend) / float(leads)

    def _kpi_count(row: dict) -> int:
        if kpi_mode == "msgs":
            return _to_int((row or {}).get("msgs"))
        if kpi_mode == "leads":
            return _to_int((row or {}).get("leads"))
        return _to_int((row or {}).get("total"))

    target_cpl = (eff.get("goals") or {}).get("target_cpl")
    try:
        target_cpl_f = float(target_cpl) if target_cpl not in (None, "") else None
    except Exception:
        target_cpl_f = None
    if target_cpl_f is not None and target_cpl_f <= 0:
        target_cpl_f = None

    if target_cpl_f is None:
        debug_reasons.append("no_goal_target_cpl")

    rows: list[dict] = []
    any_low_spend = False
    for r in rows_src:
        adset_id = str((r or {}).get("adset_id") or "")
        if not adset_id:
            continue
        name = (r or {}).get("name") or adset_id

        sp_t = _to_float((r or {}).get("spend"))
        ld_t = _kpi_count(r)
        # В heatmap cache поле cpl может быть blended (msgs+leads).
        # Для KPI=leads считаем CPL строго по лидам.
        cpl_t = None
        if kpi_mode == "leads":
            cpl_t = _cpl(sp_t, _to_int((r or {}).get("leads")))
        elif kpi_mode == "msgs":
            cpl_t = _cpl(sp_t, _to_int((r or {}).get("msgs")))
        else:
            cpl_t = _cpl(sp_t, _to_int((r or {}).get("total")))

        if sp_t <= 0:
            continue

        if float(sp_t) < 5.0:
            any_low_spend = True
            continue

        if ld_t <= 0:
            rows.append(
                {
                    "kind": "note",
                    "adset_id": adset_id,
                    "name": name,
                    "spend_today": sp_t,
                    "leads_today": ld_t,
                    "cpl_today": cpl_t,
                    "cpl_3d": None,
                    "period_label_main": label_main,
                    "period_label_base": label_base,
                    "reason": "Есть расход, но нет лидов/сообщений в этом часовом окне.",
                    "score": sp_t,
                    "snapshot": {"date": date_str, "hour": int(hour_int), "window": window_label},
                }
            )
            continue

        if target_cpl_f is None or target_cpl_f <= 0:
            continue

        if cpl_t is None:
            continue

        try:
            ratio = float(cpl_t) / float(target_cpl_f)
        except Exception:
            continue

        if ratio <= 1.05:
            rows.append(
                {
                    "kind": "budget_pct",
                    "adset_id": adset_id,
                    "name": name,
                    "percent": +max_step,
                    "spend_today": sp_t,
                    "leads_today": ld_t,
                    "cpl_today": cpl_t,
                    "cpl_3d": None,
                    "period_label_main": label_main,
                    "period_label_base": label_base,
                    "reason": "CPL в норме/лучше цели в часовом окне.",
                    "score": sp_t,
                    "snapshot": {"date": date_str, "hour": int(hour_int), "window": window_label},
                }
            )
        elif ratio >= 1.30:
            rows.append(
                {
                    "kind": "budget_pct",
                    "adset_id": adset_id,
                    "name": name,
                    "percent": -max_step,
                    "spend_today": sp_t,
                    "leads_today": ld_t,
                    "cpl_today": cpl_t,
                    "cpl_3d": None,
                    "period_label_main": label_main,
                    "period_label_base": label_base,
                    "reason": "CPL хуже цели в часовом окне.",
                    "score": sp_t,
                    "snapshot": {"date": date_str, "hour": int(hour_int), "window": window_label},
                }
            )

    if not rows and any_low_spend:
        debug_reasons.append("low_volume")

    rows.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    rows = rows[:8]

    allow_apply = mode != "ADVISOR"
    for r in rows:
        r["allow_apply"] = allow_apply and (str(r.get("kind") or "") not in {"note"})
    return rows


def _ap_daily_budget_limit_usd(aid: str) -> float | None:
    ap = _autopilot_get(aid)
    goals = ap.get("goals") or {}

    planned = goals.get("planned_budget")
    try:
        planned_f = float(planned) if planned not in (None, "") else None
    except Exception:
        planned_f = None
    if planned_f is None or planned_f <= 0:
        return None

    period = str(goals.get("period") or "day")
    today = datetime.now(ALMATY_TZ).date()

    if period == "day":
        return float(planned_f)

    if period == "week":
        return float(planned_f) / 7.0

    if period == "month":
        days_in_month = calendar.monthrange(today.year, today.month)[1]
        return float(planned_f) / float(days_in_month)

    if period == "until":
        until_raw = goals.get("until")
        try:
            until_dt = datetime.strptime(str(until_raw or ""), "%d.%m.%Y").date()
        except Exception:
            return None
        days_left = (until_dt - today).days + 1
        if days_left < 1:
            days_left = 1
        return float(planned_f) / float(days_left)

    return None


def _ap_spend_today_usd(aid: str) -> float:
    # Spend is derived only from snapshots (heatmap cache).
    now = datetime.now(ALMATY_TZ)
    date_str = now.strftime("%Y-%m-%d")
    hours = list(range(0, int(now.strftime("%H")) + 1))
    with deny_fb_api_calls(reason="autopilot_spend_today"):
        total, st, _reason = sum_ready_spend_for_date(str(aid), date_str=date_str, hours=hours)
    if st != "ready" or total is None:
        return 0.0
    try:
        return float(total)
    except Exception:
        return 0.0


def _ap_limits(aid: str) -> dict:
    ap = _autopilot_get(aid)
    limits = ap.get("limits") or {}
    return limits if isinstance(limits, dict) else {}


def _ap_within_limits_for_auto(aid: str, act: dict) -> tuple[bool, str]:
    limits = _ap_limits(aid)

    try:
        max_step = float(limits.get("max_budget_step_pct") or 20)
    except Exception:
        max_step = 20.0
    if max_step <= 0:
        max_step = 20.0

    try:
        max_risk = float(limits.get("max_daily_risk_pct") or 0)
    except Exception:
        max_risk = 0.0
    if max_risk < 0:
        max_risk = 0.0

    kind = str((act or {}).get("kind") or "")

    # NOTE: действия note никогда не автоприменяем.
    if kind == "note":
        return False, "note"

    if kind == "budget_pct":
        try:
            pct = float((act or {}).get("percent") or 0.0)
        except Exception:
            pct = 0.0

        if abs(pct) > float(max_step):
            return False, f"step>{max_step:.0f}%"

        # Ограничение дневного риска применяем только для увеличений.
        if pct > 0:
            daily_limit = _ap_daily_budget_limit_usd(aid)
            if daily_limit is None:
                return False, "no_daily_limit"
            spend_today = _ap_spend_today_usd(aid)
            allowed = float(daily_limit) * (1.0 + float(max_risk) / 100.0)
            if spend_today > allowed:
                return False, f"risk>{max_risk:.0f}%"

        return True, "ok"

    if kind in {"pause_ad", "pause_adset"}:
        return True, "ok"

    return False, "unknown_kind"


async def _autopilot_hourly_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id, _src = _resolve_autopilot_chat_id_logged(reason="autopilot_hourly_job")
    now = datetime.now(ALMATY_TZ)
    hour = int(now.strftime("%H"))
    quiet = (hour >= 22) or (hour < 10)

    win = prev_full_hour_window(now=now)
    window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"

    log = logging.getLogger(__name__)
    store = load_accounts() or {}
    for aid, row in store.items():
        if not (row or {}).get("enabled", True):
            continue
        ap = (row or {}).get("autopilot") or {}
        if not isinstance(ap, dict):
            continue
        mode = str(ap.get("mode") or "OFF").upper()
        gids = _autopilot_active_group_ids(aid)

        log.info(
            "autopilot_hourly_start aid=%s mode=%s active_group_ids=%s groups_count=%s",
            str(aid),
            str(mode),
            ",".join([str(x) for x in (gids or [])]) if gids else "",
            int(len(gids or [])),
        )

        if mode not in {"AUTO_LIMITS", "SEMI"}:
            log.info("autopilot_hourly_skip aid=%s reason=mode_not_supported mode=%s", str(aid), str(mode))
            continue

        # SEMI always means recommendations-only.
        dry_run = (mode == "SEMI")

        if not gids:
            log.info("autopilot_hourly_skip aid=%s reason=no_monitored_groups", str(aid))
            continue

        # Snapshot status for UX.
        with deny_fb_api_calls(reason="autopilot_hourly_dataset"):
            _ds, ds_status, ds_reason, ds_meta = get_heatmap_dataset(
                str(aid),
                date_str=str(win.get("date") or ""),
                hours=[int(win.get("hour") or 0)],
            )

        actions_total = 0
        applied_total = 0
        skipped_reason_counts: Counter[str] = Counter()
        semi_blocks: list[str] = []

        for gid in gids:
            eff = _autopilot_effective_config_for_group(aid, gid)
            try:
                with deny_fb_api_calls(reason="autopilot_hourly_generate"):
                    actions, reasons = _ap_generate_actions(aid, eff=eff, debug=True)  # type: ignore[misc]
            except Exception:
                actions, reasons = ([], ["api_error"])

            actions_total += int(len(actions or []))
            if not actions:
                skipped_reason_counts.update([str(x) for x in (reasons or []) if x])
                continue

            if dry_run:
                gname = eff.get("group_name") or str(gid)
                shown = []
                for act in (actions or [])[:10]:
                    shown.append(_ap_action_text(act))
                semi_blocks.append(
                    f"🤖 Группа: {gname} — рекомендации: {len(actions)}\n\n" + "\n\n---\n\n".join(shown)
                )
                continue

            applied_msgs = []
            blocked_by_limits = 0
            for act in actions:
                ok, _why = _ap_within_limits_for_auto(aid, act)
                if not ok:
                    blocked_by_limits += 1
                    continue
                kind = str(act.get("kind") or "")

                if kind == "budget_pct":
                    try:
                        pct_f = float(act.get("percent") or 0.0)
                    except Exception:
                        pct_f = 0.0
                    with allow_fb_api_calls(reason="autopilot_apply"):
                        res = apply_budget_change(str(act.get("adset_id") or ""), pct_f)
                    if str(res.get("status") or "").lower() in {"ok", "success"}:
                        applied_msgs.append(str(res.get("message") or "") + "\n\n" + _ap_action_text(act))
                        applied_total += 1
                        append_autopilot_event(
                            aid,
                            {
                                "type": "hourly_auto_apply",
                                "group_id": str(gid),
                                "kind": kind,
                                "adset_id": str(act.get("adset_id") or ""),
                                "percent": pct_f,
                                "status": res.get("status"),
                                "message": res.get("message"),
                                "chat_id": str(chat_id),
                            },
                        )
                    continue

                if kind == "pause_ad":
                    ad_id = str(act.get("ad_id") or "")
                    with allow_fb_api_calls(reason="autopilot_apply"):
                        res = pause_ad(ad_id)
                    if str(res.get("status") or "").lower() in {"ok", "success"}:
                        applied_msgs.append(str(res.get("message") or res.get("exception") or "") + "\n\n" + _ap_action_text(act))
                        applied_total += 1
                        append_autopilot_event(
                            aid,
                            {
                                "type": "hourly_auto_apply",
                                "group_id": str(gid),
                                "kind": kind,
                                "adset_id": str(act.get("adset_id") or ""),
                                "ad_id": ad_id,
                                "status": res.get("status"),
                                "message": res.get("message") or res.get("exception"),
                                "chat_id": str(chat_id),
                            },
                        )
                    continue

                if kind == "pause_adset":
                    with allow_fb_api_calls(reason="autopilot_apply"):
                        res = disable_entity(str(act.get("adset_id") or ""))
                    if str(res.get("status") or "").lower() in {"ok", "success"}:
                        applied_msgs.append(str(res.get("message") or "") + "\n\n" + _ap_action_text(act))
                        append_autopilot_event(
                            aid,
                            {
                                "type": "hourly_auto_apply",
                                "group_id": str(gid),
                                "kind": kind,
                                "adset_id": str(act.get("adset_id") or ""),
                                "status": res.get("status"),
                                "message": res.get("message"),
                                "chat_id": str(chat_id),
                            },
                        )
                    continue

            if applied_msgs:
                gname = eff.get("group_name") or str(gid)
                header = f"🤖 Автопилот (группа: {gname}) — применено: {len(applied_msgs)}\n\n"
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=header + "\n\n---\n\n".join(applied_msgs),
                    disable_notification=bool(quiet),
                )

            if actions and blocked_by_limits >= int(len(actions)):
                skipped_reason_counts.update(["all_candidates_blocked_by_limits"])

        log.info(
            "autopilot_hourly_finish aid=%s actions_total=%s applied_total=%s skipped_reason_counts=%s",
            str(aid),
            int(actions_total),
            int(applied_total),
            dict(skipped_reason_counts),
        )

        snap_line = f"Слепок: {ds_status}"
        if ds_status != "ready":
            snap_line = snap_line + f" ({ds_reason})"
        banner = f"Источник данных: heatmap cache\nОкно: {window_label}\n{snap_line}"

        if dry_run and semi_blocks:
            banner = banner + "\nИзменения не применяются, только рекомендации"
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🤖 Автопилот — почасовой прогон ({'SEMI' if mode == 'SEMI' else 'AUTO_LIMITS'}) ({get_account_name(aid)})\n"
                    + banner
                    + "\n\n"
                    + "\n\n".join(semi_blocks)
                ),
                disable_notification=bool(quiet),
            )

        if int(actions_total) <= 0:
            reason_txt = _ap_top_reasons(list(skipped_reason_counts.elements()) or [], n=2)
            if reason_txt:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🤖 Автопилот — почасовой прогон ({'SEMI' if dry_run else 'AUTO_LIMITS'}) ({get_account_name(aid)})\n"
                        + banner
                        + "\n"
                        + ("Изменения не применяются, только рекомендации\n" if dry_run else "")
                        + "Действий: 0\n"
                        + f"Причина: {reason_txt}"
                    ),
                    disable_notification=bool(quiet),
                )


async def _autopilot_warmup_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id, _src = _resolve_autopilot_chat_id_logged(reason="autopilot_warmup_job")
    now = datetime.now(ALMATY_TZ)
    hour = int(now.strftime("%H"))
    quiet = (hour >= 22) or (hour < 10)

    win = prev_full_hour_window(now=now)
    window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"

    store = load_accounts() or {}
    any_lines = []
    diag_lines = []
    total_groups = 0
    total_actions = 0
    reason_counts: Counter[str] = Counter()

    for aid, row in store.items():
        if not (row or {}).get("enabled", True):
            continue

        ap = (row or {}).get("autopilot") or {}
        if not isinstance(ap, dict):
            continue
        mode = str(ap.get("mode") or "OFF").upper()
        if mode == "OFF":
            continue

        gids = _autopilot_active_group_ids(aid)
        if not gids:
            continue

        acc_name = get_account_name(aid)
        any_lines.append(f"\n🏢 {acc_name}")

        for gid in gids:
            total_groups += 1
            eff = _autopilot_effective_config_for_group(aid, gid)
            gname = eff.get("group_name") or str(gid)

            try:
                with deny_fb_api_calls(reason="autopilot_warmup_generate"):
                    actions, reasons = _ap_generate_actions(aid, eff=eff, debug=True)  # type: ignore[misc]
            except Exception:
                actions, reasons = ([], ["api_error"])

            shown = []
            for act in (actions or [])[:5]:
                shown.append(_ap_action_text(act))
            total_actions += len(actions or [])

            if shown:
                goals = eff.get("goals") or {}
                baseline = bool(isinstance(goals, dict) and (goals.get("target_cpl") in (None, "")))
                suffix = " (база 3 дня)" if baseline else ""
                any_lines.append(f"\n🤖 Группа: {gname} — идеи: {len(actions)}{suffix}")
                any_lines.append("\n\n---\n\n".join(shown))
            else:
                reason_counts.update([str(x) for x in (reasons or []) if x])
                reason_txt = _ap_top_reasons(reasons or [], n=2)
                any_lines.append(f"\n🤖 Группа: {gname} — действий нет")
                if reason_txt:
                    any_lines.append(f"Причина: {reason_txt}")

    header = "🤖 Автопилот — первичный прогон после старта\n"
    header = header + "Источник данных: heatmap cache\n"
    header = header + f"Окно: {window_label}\n"
    # Snapshot status: take first enabled account if exists.
    snap_status = "missing"
    snap_reason = "missing_snapshot"
    for _aid, _row in (store or {}).items():
        if not (_row or {}).get("enabled", True):
            continue
        with deny_fb_api_calls(reason="autopilot_warmup_dataset"):
            _ds, st, rs, _meta = get_heatmap_dataset(
                str(_aid),
                date_str=str(win.get("date") or ""),
                hours=[int(win.get("hour") or 0)],
            )
        snap_status = str(st)
        snap_reason = str(rs)
        break
    header = header + f"Слепок: {snap_status} ({snap_reason})\n"
    header = header + "Это не часовой срез. Ничего не применяю, только подсказываю идеи.\n"
    header = header + f"Активных групп: {total_groups} | Рекомендаций: {total_actions}\n"

    if int(total_actions) <= 0:
        top_reason = _ap_top_reasons(list(reason_counts.elements()) or [], n=2)
        if top_reason:
            header = header + f"Причина: {top_reason}\n"

    if diag_lines:
        diag = "\n" + "\n".join(diag_lines[:3])
        if len(diag_lines) > 3:
            diag += f"\n(+{len(diag_lines) - 3})"
        header = header + diag + "\n"

    if total_groups == 0:
        text = header + "\nСейчас нет активных групп (или автопилот выключен)."
    else:
        text = header + "\n" + "\n".join(any_lines).strip()

    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        disable_notification=bool(quiet),
    )


def _ap_force_kb(token: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Применить сверх лимитов", callback_data=f"apdo|force|{token}"),
                InlineKeyboardButton("❌ Отмена", callback_data=f"apdo|cancel|{token}"),
            ]
        ]
    )


def _autopilot_analysis_text(aid: str) -> str:
    now = datetime.now(ALMATY_TZ)
    eff = _autopilot_effective_config(aid)
    kpi_mode = str(eff.get("kpi") or "total")
    group_campaign_ids = eff.get("campaign_ids")
    group_campaign_set = set(str(x) for x in (group_campaign_ids or []) if x)

    win = prev_full_hour_window(now=now)
    window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"

    with deny_fb_api_calls(reason="autopilot_analysis_text"):
        ds, st, reason, meta = get_heatmap_dataset(
            str(aid),
            date_str=str(win.get("date") or ""),
            hours=[int(win.get("hour") or 0)],
        )

    scope_line = ""
    if eff.get("group_id"):
        gname = eff.get("group_name") or eff.get("group_id")
        scope_line = f" (группа: {gname})"

    lines = [
        f"📊 Автопилат — анализ (heatmap cache): {get_account_name(aid)}" + scope_line,
        "",
        "Источник: heatmap cache",
        f"Окно: {window_label}",
        f"Слепок: {st} ({reason})",
        "",
    ]

    if st != "ready" or not ds:
        lines.append("Данных пока нет.")
        lines.append(f"Причина: {_ap_reason_human(str(reason))}")
        try:
            nxt = int((meta or {}).get("next_try_in_min") or 0)
        except Exception:
            nxt = 0
        if nxt > 0:
            lines.append(f"Повторю через ~{nxt} мин (сборщик слепков).")
        return "\n".join(lines).strip()

    def _kpi_count_row(row: dict) -> int:
        if kpi_mode == "msgs":
            try:
                return int(float((row or {}).get("msgs") or 0) or 0)
            except Exception:
                return 0
        if kpi_mode == "leads":
            try:
                return int(float((row or {}).get("leads") or 0) or 0)
            except Exception:
                return 0
        try:
            return int(float((row or {}).get("total") or 0) or 0)
        except Exception:
            return 0

    def _fmt_money(v):
        if v is None:
            return "—"
        try:
            return f"{float(v):.2f} $"
        except Exception:
            return "—"

    rows = list((ds or {}).get("rows") or [])
    if group_campaign_set:
        rows = [r for r in rows if str((r or {}).get("campaign_id") or "") in group_campaign_set]

    def _row_spend_for_kpi(row: dict) -> float:
        try:
            if kpi_mode == "msgs":
                return float((row or {}).get("spend_for_msgs") or (row or {}).get("spend") or 0.0)
            if kpi_mode == "leads":
                return float((row or {}).get("spend_for_leads") or (row or {}).get("spend") or 0.0)
            return float((row or {}).get("spend_for_total") or (row or {}).get("spend") or 0.0)
        except Exception:
            return float((row or {}).get("spend") or 0.0)

    sum_spend = 0.0
    sum_kpi = 0
    for r in rows:
        kpi_v = int(_kpi_count_row(r))
        if kpi_mode == "leads" and kpi_v <= 0:
            continue
        if kpi_mode == "msgs" and kpi_v <= 0:
            continue
        try:
            sum_spend += float(_row_spend_for_kpi(r) or 0.0)
        except Exception:
            pass
        sum_kpi += int(kpi_v)
    cpl = (float(sum_spend) / float(sum_kpi)) if (sum_spend > 0 and sum_kpi > 0) else None

    lines.append(f"Итого: spend {_fmt_money(sum_spend)} | results {sum_kpi} | CPL {_fmt_money(cpl)}")
    lines.append("")

    if not rows:
        lines.append("Нет строк в слепке (после фильтров).")
        return "\n".join(lines).strip()

    lines.append("Топ adset по spend:")
    for r in rows[:12]:
        name = str((r or {}).get("name") or (r or {}).get("adset_id") or "")
        sp = float(_row_spend_for_kpi(r) or 0.0)
        kpi = _kpi_count_row(r)
        cpl_r = (sp / float(kpi)) if (sp > 0 and kpi > 0) else None
        lines.append(f"• {name}: spend {_fmt_money(sp)} | results {kpi} | CPL {_fmt_money(cpl_r)}")

    return "\n".join(lines).strip()


FOCUS_AI_DATA_TIMEOUT_S = 120
FOCUS_AI_DEEPSEEK_TIMEOUT_S = 240
FOCUS_AI_MAX_OBJECTS = 40


def main_menu() -> InlineKeyboardMarkup:
    last_sync = human_last_sync()
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📊 Отчёты", callback_data="reports_menu")],
            [InlineKeyboardButton("🆘 Мониторинг", callback_data="monitoring_menu")],
            [InlineKeyboardButton("🤖 Автопилат", callback_data="autopilot_menu")],
            [InlineKeyboardButton("💳 Биллинг", callback_data="billing")],
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


def _lead_metric_label_for_action_type(action_type: str) -> str:
    at = str(action_type or "").strip()
    if not at:
        return "(пусто)"

    known = {
        "onsite_web_lead": "Заявка с сайта",
        "lead": "Лид",
        "submit_application": "Отправка заявки",
        "website_submit_application": "Отправка заявки (сайт)",
        "Website Submit Applications": "Отправка заявки (сайт)",
        "offsite_conversion.fb_pixel_submit_application": "Отправка заявки (Pixel)",
        "offsite_conversion.fb_pixel_lead": "Лид (Pixel)",
    }
    if at in known:
        return known[at]

    if at.startswith("offsite_conversion.custom"):
        suffix = at.split(".")[-1]
        return f"Заявка с сайта — {suffix}" if suffix else "Заявка с сайта"

    if at.startswith("offsite_conversion"):
        return at.replace("offsite_conversion.", "Offsite conversion: ")

    if "_" in at:
        return at.replace("_", " ").strip().capitalize()

    return at


LEAD_METRIC_LOOKBACK_DAYS = 30
LEAD_METRIC_CATALOG_TTL_S = 12 * 3600
LEAD_METRIC_PAGE_SIZE = 12


def _lead_metric_is_pixel_conversion_action_type(action_type: str) -> bool:
    at = str(action_type or "").strip()
    if not at:
        return False
    low = at.lower()
    if low.startswith("offsite_conversion"):
        return True
    if low.startswith("website"):
        return True
    if low.startswith("omni_"):
        return True
    if low in {
        "lead",
        "submit_application",
        "complete_registration",
        "purchase",
        "add_payment_info",
        "initiate_checkout",
        "contact",
        "schedule",
        "view_content",
    }:
        return True
    return False


def _lead_metric_discover_catalog_from_insights(
    aid: str,
    *,
    lookback_days: int = LEAD_METRIC_LOOKBACK_DAYS,
    level: str = "adset",
) -> list[dict]:
    now = datetime.now(ALMATY_TZ).date()
    until = now.strftime("%Y-%m-%d")
    since = (now - timedelta(days=max(1, int(lookback_days)) - 1)).strftime("%Y-%m-%d")
    period = {"since": since, "until": until}
    rows: list[dict] = []
    with allow_fb_api_calls(reason="lead_metric_catalog_discover"):
        rows = fetch_insights_bulk(
            str(aid),
            period=period,
            level=str(level),
            fields=["actions", "action_values"],
            params_extra={"action_report_time": "conversion", "use_unified_attribution_setting": True},
        )

    uniq: dict[str, str] = {}
    for r in (rows or []):
        for a in (r or {}).get("actions") or []:
            if not isinstance(a, dict):
                continue
            at = str(a.get("action_type") or "").strip()
            if not at:
                continue
            if not _lead_metric_is_pixel_conversion_action_type(at):
                continue
            if at not in uniq:
                uniq[at] = _lead_metric_label_for_action_type(at)

    out = [{"action_type": k, "label": v} for k, v in uniq.items()]
    out.sort(key=lambda x: (str(x.get("label") or ""), str(x.get("action_type") or "")))
    return out


def _lead_metric_get_catalog_cached(
    aid: str,
    *,
    force_refresh: bool = False,
    lookback_days: int = LEAD_METRIC_LOOKBACK_DAYS,
) -> tuple[list[dict], str, float | None]:
    cat = get_lead_metric_catalog_for_account(str(aid))
    now_ts = float(pytime.time())
    if cat and not force_refresh:
        try:
            age_s = now_ts - float(cat.get("ts") or 0.0)
        except Exception:
            age_s = None
        if age_s is not None and age_s <= float(LEAD_METRIC_CATALOG_TTL_S):
            return list(cat.get("items") or []), "cache", age_s

    try:
        items = _lead_metric_discover_catalog_from_insights(
            str(aid),
            lookback_days=int(lookback_days),
            level="adset",
        )
        if items:
            set_lead_metric_catalog_for_account(str(aid), items=items, lookback_days=int(lookback_days))
            return items, "fb", 0.0
    except Exception:
        pass

    if cat:
        try:
            age_s = now_ts - float(cat.get("ts") or 0.0)
        except Exception:
            age_s = None
        return list(cat.get("items") or []), "stale_cache", age_s
    return [], "empty", None


def _lead_metric_human_cache_age(age_s: float | None) -> str:
    if age_s is None:
        return "unknown"
    if age_s < 60:
        return f"{int(age_s)}s"
    if age_s < 3600:
        return f"{int(age_s // 60)}m"
    return f"{int(age_s // 3600)}h"


def _lead_metric_choose_page(
    *,
    aid: str,
    items: list[dict],
    page: int,
    query: str,
) -> tuple[str, InlineKeyboardMarkup]:
    q = str(query or "").strip().lower()
    cur = get_lead_metric_for_account(str(aid))
    cur_at = str((cur or {}).get("action_type") or "").strip() if cur else ""
    filtered = []
    for it in (items or []):
        at = str((it or {}).get("action_type") or "").strip()
        label = str((it or {}).get("label") or at).strip()
        if not at:
            continue
        if q and (q not in at.lower()) and (q not in label.lower()):
            continue
        filtered.append({"action_type": at, "label": label})

    total = len(filtered)
    if total <= 0:
        text = "Не найдено конверсий по запросу." if q else "Не найдено конверсий (actions) за lookback."
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🔄 Обновить список", callback_data=f"lead_metric_refresh|{aid}")],
                [InlineKeyboardButton("🔎 Поиск", callback_data=f"lead_metric_search|{aid}")],
                [InlineKeyboardButton("⬅️ Назад", callback_data=f"lead_metric|{aid}")],
            ]
        )
        return text, kb

    max_page = max(0, (total - 1) // int(LEAD_METRIC_PAGE_SIZE))
    p = max(0, min(int(page), int(max_page)))
    start = p * int(LEAD_METRIC_PAGE_SIZE)
    end = start + int(LEAD_METRIC_PAGE_SIZE)
    slice_items = filtered[start:end]

    rows = []
    for i, it in enumerate(slice_items):
        idx = start + i
        label = str(it.get("label") or it.get("action_type"))
        if cur_at and str(it.get("action_type") or "") == cur_at:
            label = f"✅ {label}"
        rows.append(
            [
                InlineKeyboardButton(
                    label,
                    callback_data=f"lead_metric_set2|{aid}|{idx}",
                )
            ]
        )

    nav = []
    if p > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"lead_metric_page|{aid}|{p-1}"))
    nav.append(InlineKeyboardButton(f"{p+1}/{max_page+1}", callback_data=f"lead_metric_page|{aid}|{p}"))
    if p < max_page:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"lead_metric_page|{aid}|{p+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("🔄 Обновить список", callback_data=f"lead_metric_refresh|{aid}")])
    rows.append([InlineKeyboardButton("🔎 Поиск", callback_data=f"lead_metric_search|{aid}")])
    if q:
        rows.append([InlineKeyboardButton("✖️ Сбросить поиск", callback_data=f"lead_metric_choose|{aid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"lead_metric|{aid}")])

    text = "Выбери конверсию (Pixel/website) для метрики лидов."
    if q:
        text += f"\n\nПоиск: {query}"
    return text, InlineKeyboardMarkup(rows)


def _autopilot_get(aid: str) -> dict:
    st = load_accounts().get(str(aid), {})
    ap = st.get("autopilot") or {}
    return ap if isinstance(ap, dict) else {}


def _autopilot_active_group(aid: str) -> tuple[str | None, dict | None]:
    ap = _autopilot_get(aid)
    gid = ap.get("active_group_id")
    if not gid:
        return None, None
    groups = ap.get("campaign_groups") or {}
    if not isinstance(groups, dict):
        return None, None
    grp = groups.get(str(gid))
    return (str(gid), grp) if isinstance(grp, dict) else (None, None)


def _autopilot_active_group_ids(aid: str) -> list[str]:
    ap = _autopilot_get(aid)
    ids = ap.get("active_group_ids")
    if not isinstance(ids, list):
        ids = []
    out = [str(x) for x in ids if str(x).strip()]

    gid = ap.get("active_group_id")
    if gid and str(gid).strip() and str(gid) not in set(out):
        out.append(str(gid))
    return out


def _autopilot_set_active_group_ids(aid: str, ids: list[str]) -> None:
    uniq = []
    seen = set()
    for x in (ids or []):
        s = str(x).strip()
        if not s or s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    _autopilot_set(aid, {"active_group_ids": uniq})


def _autopilot_effective_config(aid: str) -> dict:
    ap = _autopilot_get(aid)
    gid, grp = _autopilot_active_group(aid)

    out = {
        "group_id": gid,
        "group_name": None,
        "campaign_ids": None,
        "kpi": None,
        "lead_action_type": None,
        "goals": ap.get("goals") or {},
    }

    if gid and isinstance(grp, dict):
        out["group_name"] = grp.get("name")
        cids = grp.get("campaign_ids")
        out["campaign_ids"] = [str(x) for x in (cids or []) if x]
        out["kpi"] = str(grp.get("kpi") or "total")

        g_goals = grp.get("goals")
        if isinstance(g_goals, dict):
            out["goals"] = g_goals

        lm = grp.get("lead_metric")
        if isinstance(lm, dict):
            out["lead_action_type"] = lm.get("action_type")
        elif isinstance(lm, str):
            out["lead_action_type"] = lm

    if not out.get("kpi"):
        out["kpi"] = "total"

    goals = out.get("goals")
    out["goals"] = goals if isinstance(goals, dict) else {}
    out["lead_action_type"] = (str(out.get("lead_action_type") or "").strip() or None)
    return out


def _autopilot_effective_config_for_group(aid: str, gid: str) -> dict:
    ap = _autopilot_get(aid)
    groups = ap.get("campaign_groups") or {}
    if not isinstance(groups, dict):
        groups = {}
    grp = groups.get(str(gid))
    grp = grp if isinstance(grp, dict) else {}

    out = {
        "group_id": str(gid),
        "group_name": grp.get("name"),
        "campaign_ids": [str(x) for x in ((grp.get("campaign_ids") or []) or []) if x],
        "kpi": str(grp.get("kpi") or "total"),
        "lead_action_type": None,
        "goals": ap.get("goals") or {},
    }

    g_goals = grp.get("goals")
    if isinstance(g_goals, dict):
        out["goals"] = g_goals

    lm = grp.get("lead_metric")
    if isinstance(lm, dict):
        out["lead_action_type"] = lm.get("action_type")
    elif isinstance(lm, str):
        out["lead_action_type"] = lm

    if not out.get("kpi"):
        out["kpi"] = "total"

    goals = out.get("goals")
    out["goals"] = goals if isinstance(goals, dict) else {}
    out["lead_action_type"] = (str(out.get("lead_action_type") or "").strip() or None)
    return out


def _autopilot_set(aid: str, patch: dict) -> None:
    aid = str(aid)
    st = load_accounts()
    row = st.get(aid, {})
    ap = row.get("autopilot") or {}
    if not isinstance(ap, dict):
        ap = {}
    for k, v in (patch or {}).items():
        ap[k] = v
    row["autopilot"] = ap
    st[aid] = row
    save_accounts(st)


def _autopilot_group_get(aid: str, gid: str) -> dict:
    ap = _autopilot_get(aid)
    groups = ap.get("campaign_groups") or {}
    if not isinstance(groups, dict):
        return {}
    grp = groups.get(str(gid))
    return grp if isinstance(grp, dict) else {}


def _autopilot_group_set(aid: str, gid: str, grp: dict) -> None:
    ap = _autopilot_get(aid)
    groups = ap.get("campaign_groups") or {}
    if not isinstance(groups, dict):
        groups = {}
    groups[str(gid)] = grp
    ap["campaign_groups"] = groups
    _autopilot_set(aid, ap)


def _autopilot_group_delete(aid: str, gid: str) -> None:
    ap = _autopilot_get(aid)
    groups = ap.get("campaign_groups") or {}
    if not isinstance(groups, dict):
        groups = {}
    groups.pop(str(gid), None)
    ap["campaign_groups"] = groups
    if str(ap.get("active_group_id") or "") == str(gid):
        ap["active_group_id"] = None
    _autopilot_set(aid, ap)


def _autopilot_tracked_group_names(aid: str) -> list[str]:
    ap = _autopilot_get(aid)
    groups = ap.get("campaign_groups") or {}
    if not isinstance(groups, dict):
        groups = {}

    out = []
    for gid in _autopilot_active_group_ids(aid):
        grp = groups.get(str(gid))
        name = (grp or {}).get("name") if isinstance(grp, dict) else None
        out.append(str(name or gid))
    return out


def _autopilot_tracked_group_names_human(aid: str) -> str:
    names = _autopilot_tracked_group_names(aid)
    if not names:
        return "—"
    if len(names) <= 3:
        return ", ".join(names)
    return ", ".join(names[:3]) + f" (+{len(names) - 3})"


def _autopilot_groups_menu_text(aid: str) -> str:
    tracked = _autopilot_tracked_group_names_human(aid)
    extra = (
        f"\n🤖 Автопилот следит за: {tracked}\n" if tracked != "—" else "\n🤖 Автопилот: слежение выключено\n"
    )
    return (
        f"🏷️ Группы кампаний — {get_account_name(aid)}\n"
        + extra
        + "\nВыберите группу или создайте новую:"
    )


def _autopilot_groups_kb(aid: str) -> InlineKeyboardMarkup:
    ap = _autopilot_get(aid)
    groups = ap.get("campaign_groups") or {}
    if not isinstance(groups, dict):
        groups = {}
    active = str(ap.get("active_group_id") or "").strip()
    tracked = set(_autopilot_active_group_ids(aid))

    rows = []
    for gid, grp in groups.items():
        name = (grp or {}).get("name") if isinstance(grp, dict) else None
        label = str(name or gid)
        prefix = "✅ " if str(gid) in tracked else ""
        rows.append(
            [InlineKeyboardButton(prefix + label, callback_data=f"ap_group_open|{aid}|{gid}")]
        )

    rows.append([InlineKeyboardButton("➕ Создать группу", callback_data=f"ap_group_create|{aid}")])
    if tracked:
        rows.append([InlineKeyboardButton("🚫 Отключить все группы", callback_data=f"ap_group_off|{aid}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"autopilot_acc|{aid}")])
    return InlineKeyboardMarkup(rows)


def _autopilot_group_menu_text(aid: str, gid: str) -> str:
    grp = _autopilot_group_get(aid, gid)
    name = grp.get("name") or gid
    kpi_map = {"total": "Всего (переписки+заявки)", "msgs": "Переписки", "leads": "Заявки"}
    kpi = kpi_map.get(str(grp.get("kpi") or "total"), str(grp.get("kpi") or "total"))
    cids = grp.get("campaign_ids") or []
    try:
        cnt = len(list(cids))
    except Exception:
        cnt = 0

    lm = grp.get("lead_metric")
    if isinstance(lm, dict):
        lm_label = lm.get("label") or lm.get("action_type")
    else:
        lm_label = None

    goals = grp.get("goals") or {}
    if not isinstance(goals, dict):
        goals = {}

    def _fmt_money(v):
        try:
            vv = float(v)
        except Exception:
            return "—"
        return f"{vv:.2f} $"

    def _fmt_int(v):
        try:
            return str(int(float(v)))
        except Exception:
            return "—"

    lines = [
        f"🏷️ Группа: {name}",
        f"KPI: {kpi}",
        f"Кампаний в группе: {cnt}",
        f"Метрика заявок: {lm_label or 'стандартная (по аккаунту)'}",
        "",
        "🎯 Цели группы:",
        f"• Лиды: {_fmt_int(goals.get('leads'))}",
        f"• Целевой CPL: {_fmt_money(goals.get('target_cpl'))}",
        f"• Плановый бюджет: {_fmt_money(goals.get('planned_budget'))}",
        f"• Период: {str(goals.get('period') or 'day')}",
    ]
    return "\n".join(lines)


def _autopilot_group_kb(aid: str, gid: str) -> InlineKeyboardMarkup:
    tracked = set(_autopilot_active_group_ids(aid))
    is_on = str(gid) in tracked
    rows = [
        [
            InlineKeyboardButton(
                ("✅ Следить за группой" if not is_on else "🚫 Не следить за группой"),
                callback_data=f"ap_group_toggle|{aid}|{gid}",
            )
        ],
        [InlineKeyboardButton("🟦 Открыть в Автопилоте", callback_data=f"ap_group_select|{aid}|{gid}")],
        [InlineKeyboardButton("✏️ Переименовать", callback_data=f"ap_group_rename|{aid}|{gid}")],
        [InlineKeyboardButton("📌 Кампании", callback_data=f"ap_group_campaigns|{aid}|{gid}")],
        [InlineKeyboardButton("📊 KPI группы", callback_data=f"ap_group_kpi|{aid}|{gid}")],
        [InlineKeyboardButton("📊 Метрика заявок", callback_data=f"ap_group_leadmetric|{aid}|{gid}")],
        [InlineKeyboardButton("🗑 Удалить группу", callback_data=f"ap_group_delete|{aid}|{gid}")],
        [InlineKeyboardButton("⬅️ К группам", callback_data=f"ap_groups|{aid}")],
    ]
    return InlineKeyboardMarkup(rows)


def _autopilot_group_kpi_kb(aid: str, gid: str) -> InlineKeyboardMarkup:
    grp = _autopilot_group_get(aid, gid)
    cur = str(grp.get("kpi") or "total")
    def b(code: str, label: str) -> InlineKeyboardButton:
        prefix = "✅ " if cur == code else ""
        return InlineKeyboardButton(prefix + label, callback_data=f"ap_group_kpi_set|{aid}|{gid}|{code}")
    rows = [
        [b("total", "Всего (переписки+заявки)")],
        [b("msgs", "Переписки")],
        [b("leads", "Заявки")],
        [InlineKeyboardButton("⬅️ Назад", callback_data=f"ap_group_open|{aid}|{gid}")],
    ]
    return InlineKeyboardMarkup(rows)


def _b36_encode_int(n: int) -> str:
    if n < 0:
        raise ValueError("negative")
    if n == 0:
        return "0"
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = []
    x = n
    while x:
        x, r = divmod(x, 36)
        out.append(chars[r])
    return "".join(reversed(out))


def _b36_decode_int(s: str) -> int:
    return int(str(s).strip().lower(), 36)


def _campaign_id_to_token(cid: str) -> str:
    return _b36_encode_int(int(str(cid).strip()))


def _campaign_token_to_id(tok: str) -> str:
    return str(_b36_decode_int(tok))


def _autopilot_group_campaigns_kb_active_only(
    aid: str,
    gid: str,
) -> tuple[InlineKeyboardMarkup, set[str]]:
    grp = _autopilot_group_get(aid, gid)
    selected = set(str(x) for x in (grp.get("campaign_ids") or []) if x)

    opts = _campaign_options_from_snapshots(aid)
    active_ids = set(str((c or {}).get("id") or "") for c in (opts or []) if (c or {}).get("id"))

    rows = []
    for c in (opts or []):
        cid = str((c or {}).get("id") or "")
        if not cid:
            continue
        name = (c or {}).get("name") or cid
        prefix = "✅ " if cid in selected else ""
        tok = _campaign_id_to_token(cid)
        rows.append(
            [
                InlineKeyboardButton(
                    prefix + str(name),
                    callback_data=f"ap_group_camp_toggle|{aid}|{gid}|{tok}",
                )
            ]
        )

    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"ap_group_open|{aid}|{gid}")])
    return InlineKeyboardMarkup(rows), active_ids


def _autopilot_group_campaigns_kb(aid: str, gid: str) -> InlineKeyboardMarkup:
    kb, _active_ids = _autopilot_group_campaigns_kb_active_only(aid, gid)
    return kb


def _autopilot_human_mode(mode: str) -> str:
    m = str(mode or "OFF").upper()
    if m == "ADVISOR":
        return "🧠 Советник"
    if m == "SEMI":
        return "🟡 Полуавто"
    if m == "AUTO_LIMITS":
        return "🤖 Авто с лимитами"
    return "🔴 Выключен"


def _autopilot_dashboard_text(aid: str) -> str:
    ap = _autopilot_get(aid)
    mode = str(ap.get("mode") or "OFF").upper()
    eff = _autopilot_effective_config(aid)
    goals = eff.get("goals") or {}
    limits = ap.get("limits") or {}

    leads = goals.get("leads")
    period = str(goals.get("period") or "day")
    until = goals.get("until")
    target_cpl = goals.get("target_cpl")
    planned_budget = goals.get("planned_budget")

    max_step = limits.get("max_budget_step_pct")
    max_risk = limits.get("max_daily_risk_pct")
    allow_pause_ads = bool(limits.get("allow_pause_ads", True))
    allow_pause_adsets = bool(limits.get("allow_pause_adsets", False))
    allow_redist = bool(limits.get("allow_redistribute", True))
    allow_reenable = bool(limits.get("allow_reenable_ads", False))

    period_map = {
        "day": "день",
        "week": "неделя",
        "month": "месяц",
        "until": "до даты",
    }
    period_h = period_map.get(period, period)

    def _fmt_money(v):
        try:
            vv = float(v)
        except Exception:
            return "—"
        return f"{vv:.2f} $"

    def _fmt_int(v):
        try:
            return str(int(float(v)))
        except Exception:
            return "—"

    extra = ""
    if period == "month" and planned_budget not in (None, ""):
        try:
            today = datetime.now(ALMATY_TZ).date()
            days_in_month = calendar.monthrange(today.year, today.month)[1]
            daily = float(planned_budget) / float(days_in_month)
            extra = f"\n• Дневной лимит (месяц): {daily:.2f} $"
        except Exception:
            extra = ""

    group_line = ""
    if eff.get("group_id"):
        gname = eff.get("group_name") or eff.get("group_id")
        group_line = f"\nГруппа: {gname}"

    tracked_h = _autopilot_tracked_group_names_human(aid)
    if tracked_h != "—":
        group_line += f"\nАвтопилот следит: {tracked_h}"

    kpi_map = {
        "total": "Всего (переписки+заявки)",
        "msgs": "Переписки",
        "leads": "Заявки",
    }
    kpi_line = f"\nKPI: {kpi_map.get(str(eff.get('kpi') or ''), str(eff.get('kpi') or ''))}"

    lines = [
        f"🤖 Автопилат — {get_account_name(aid)}" + group_line + kpi_line,
        "",
        f"Статус: {_autopilot_human_mode(mode)}",
        "",
        "🎯 Цели:",
        f"• Лиды: {_fmt_int(leads)}",
        f"• Период: {period_h}" + (f" ({until})" if (period == "until" and until) else ""),
        f"• Целевой CPL: {_fmt_money(target_cpl)}",
        f"• Плановый бюджет: {_fmt_money(planned_budget)}" + extra,
        "",
        "🧩 Лимиты:",
        f"• Шаг бюджета: ±{_fmt_int(max_step)}%",
        f"• Допустимый риск/день: +{_fmt_int(max_risk)}%",
        f"• Отключать объявления: {'✅' if allow_pause_ads else '❌'}",
        f"• Останавливать adset: {'✅' if allow_pause_adsets else '❌'}",
        f"• Перераспределение: {'✅' if allow_redist else '❌'}",
        f"• Включать обратно объявления: {'✅' if allow_reenable else '❌'}",
    ]
    return "\n".join(lines)


def _autopilot_kb(aid: str) -> InlineKeyboardMarkup:
    ap = _autopilot_get(aid)
    mode = str(ap.get("mode") or "OFF").upper()
    limits = ap.get("limits") or {}
    allow_reenable = bool(limits.get("allow_reenable_ads", False))
    allow_pause_adsets = bool(limits.get("allow_pause_adsets", False))

    eff = _autopilot_effective_config(aid)
    gid = eff.get("group_id")
    gname = eff.get("group_name") or gid
    tracked_cnt = len(_autopilot_active_group_ids(aid))
    grp_label = f"🏷️ Группы кампаний" if not gid else f"🏷️ Группа: {gname}"
    if tracked_cnt > 0:
        grp_label += f" (следит: {tracked_cnt})"

    rows = [
        [
            InlineKeyboardButton(
                ("🧠 Советник ✅" if mode == "ADVISOR" else "🧠 Советник"),
                callback_data=f"ap_mode|{aid}|ADVISOR",
            ),
        ],
        [
            InlineKeyboardButton(
                ("🟡 Полуавто ✅" if mode == "SEMI" else "🟡 Полуавто"),
                callback_data=f"ap_mode|{aid}|SEMI",
            ),
        ],
        [
            InlineKeyboardButton(
                ("🤖 Авто с лимитами ✅" if mode == "AUTO_LIMITS" else "🤖 Авто с лимитами"),
                callback_data=f"ap_mode|{aid}|AUTO_LIMITS",
            ),
        ],
        [
            InlineKeyboardButton(
                ("🔴 Выключить ✅" if mode == "OFF" else "🔴 Выключить"),
                callback_data=f"ap_mode|{aid}|OFF",
            ),
        ],
        [
            InlineKeyboardButton("🎯 Лиды (цель)", callback_data=f"ap_set_leads|{aid}"),
            InlineKeyboardButton("💰 CPL (цель)", callback_data=f"ap_set_cpl|{aid}"),
        ],
        [
            InlineKeyboardButton("💵 Бюджет (план)", callback_data=f"ap_set_budget|{aid}"),
            InlineKeyboardButton("🗓 Период", callback_data=f"ap_period|{aid}"),
        ],
        [InlineKeyboardButton(grp_label, callback_data=f"ap_groups|{aid}")],
        [
            InlineKeyboardButton(
                ("🔁 Включать обратно объявления: ВКЛ" if allow_reenable else "🔁 Включать обратно объявления: ВЫКЛ"),
                callback_data=f"ap_toggle_reenable|{aid}",
            ),
        ],
        [
            InlineKeyboardButton(
                ("🧩 Останавливать adset: ВКЛ" if allow_pause_adsets else "🧩 Останавливать adset: ВЫКЛ"),
                callback_data=f"ap_toggle_pause_adsets|{aid}",
            )
        ],
        [InlineKeyboardButton("📊 Анализ (сегодня vs 3 дня)", callback_data=f"ap_analyze|{aid}")],
        [InlineKeyboardButton("🕒 Часы (тепловая карта)", callback_data=f"ap_hm|{aid}")],
        [InlineKeyboardButton("🧾 История", callback_data=f"ap_history|{aid}")],
        [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="autopilot_menu")],
        [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
    ]
    return InlineKeyboardMarkup(rows)


def _autopilot_period_kb(aid: str) -> InlineKeyboardMarkup:
    ap = _autopilot_get(aid)
    goals = ap.get("goals") or {}
    cur = str(goals.get("period") or "day")

    def b(code: str, label: str) -> InlineKeyboardButton:
        prefix = "✅ " if cur == code else ""
        return InlineKeyboardButton(prefix + label, callback_data=f"ap_period_set|{aid}|{code}")

    return InlineKeyboardMarkup(
        [
            [b("day", "День"), b("week", "Неделя")],
            [b("month", "Месяц"), b("until", "До даты")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"autopilot_acc|{aid}")],
        ]
    )


def _discover_actions_for_account(aid: str) -> list[dict]:
    now = datetime.now(ALMATY_TZ)
    yday = (now - timedelta(days=1)).date()
    date_str = yday.strftime("%Y-%m-%d")

    # Snapshots-only: derive action_types from hourly heatmap snapshots.
    actions_map: dict[str, float] = {}
    with deny_fb_api_calls(reason="lead_metric_discover_actions"):
        for h in list_snapshot_hours(str(aid), date_str=str(date_str)):
            snap = load_snapshot(str(aid), date_str=str(date_str), hour=int(h)) or {}
            if str(snap.get("status") or "") not in {"ready", "ready_low_confidence"}:
                continue
            for r in (snap.get("rows") or []):
                if not isinstance(r, dict):
                    continue
                # We only know msgs/leads from snapshots; map to pseudo action_types.
                try:
                    v = float(r.get("msgs") or 0)
                    if v > 0:
                        actions_map["onsite_conversion.messaging_conversation_started_7d"] = (
                            actions_map.get("onsite_conversion.messaging_conversation_started_7d", 0.0) + v
                        )
                except Exception:
                    pass
                try:
                    v = float(r.get("leads") or 0)
                    if v > 0:
                        actions_map["lead"] = actions_map.get("lead", 0.0) + v
                except Exception:
                    pass

    actions = [{"action_type": k, "value": v} for k, v in actions_map.items()]

    out: list[dict] = []
    seen = set()
    for a in actions:
        at = (a or {}).get("action_type")
        if not at:
            continue
        try:
            v = float((a or {}).get("value", 0) or 0)
        except Exception:
            v = 0.0
        if v <= 0:
            continue
        if at in seen:
            continue
        seen.add(at)
        out.append({"action_type": str(at), "value": float(v)})

    return out


def _is_blacklisted_lead_action_type(action_type: str) -> bool:
    at = str(action_type or "").strip().lower()
    if not at:
        return True

    if at.startswith("onsite_conversion.messaging_"):
        return True

    if at.startswith("post_interaction"):
        return True

    banned_exact = {
        "link_click",
        "landing_page_view",
        "view_content",
        "video_view",
        "page_engagement",
        "post_engagement",
        "reaction",
        "comment",
        "post",
        "message",
        "reply",
        "connection",
        "pixel_view_content",
    }
    if at in banned_exact:
        return True

    banned_substrings = [
        "engagement",
        "video",
        "view",
        "click",
        "reaction",
        "comment",
        "message",
        "reply",
        "connection",
    ]
    return any(s in at for s in banned_substrings)


def _is_site_lead_custom_conversion_name(name: str) -> bool:
    n = str(name or "").strip().lower()
    if not n:
        return False
    keys = ["lead", "заяв", "application", "form", "request"]
    return any(k in n for k in keys)


def _discover_lead_metrics_for_account(aid: str) -> list[dict]:
    actions = _discover_actions_for_account(aid)

    whitelist_exact_lower = {
        "onsite_web_lead",
        "lead",
        "submit_application",
        "website_submit_application",
        "offsite_conversion.fb_pixel_lead",
        "offsite_conversion.fb_pixel_submit_application",
    }
    whitelist_exact_mixed = {
        "Website Submit Applications",
    }

    out: list[dict] = []
    for row in actions:
        at = str((row or {}).get("action_type") or "").strip()
        if not at:
            continue

        at_lower = at.lower()

        if _is_blacklisted_lead_action_type(at):
            continue

        if at.startswith("offsite_conversion.custom"):
            # Legacy: required FB API lookup for CustomConversion name.
            # Snapshots-only policy: skip these options.
            continue

        if at not in whitelist_exact_mixed and at_lower not in whitelist_exact_lower:
            continue

        out.append({"action_type": at, "label": _lead_metric_label_for_action_type(at)})

    out.sort(key=lambda x: (x.get("label") or x.get("action_type") or ""))
    return out


def _snapshot_rows_last_7d(aid: str) -> list[dict]:
    now = datetime.now(ALMATY_TZ)
    end = (now - timedelta(days=1)).date()
    start = end - timedelta(days=6)
    all_rows: list[dict] = []
    with deny_fb_api_calls(reason="snapshot_rows_last_7d"):
        cur = start
        while cur <= end:
            d = cur.strftime("%Y-%m-%d")
            for h in list_snapshot_hours(str(aid), date_str=str(d)):
                snap = load_snapshot(str(aid), date_str=str(d), hour=int(h)) or {}
                if str(snap.get("status") or "") not in {"ready", "ready_low_confidence"}:
                    continue
                for r in (snap.get("rows") or []):
                    if isinstance(r, dict):
                        all_rows.append(r)
            cur = cur + timedelta(days=1)
    return all_rows


def _campaign_options_from_snapshots(aid: str) -> list[dict]:
    all_rows = _snapshot_rows_last_7d(aid)
    seen: dict[str, str] = {}
    for r in all_rows:
        cid = str((r or {}).get("campaign_id") or "")
        if not cid:
            continue
        nm = str((r or {}).get("campaign_name") or cid)
        if cid not in seen:
            seen[cid] = nm
    out = [{"id": cid, "name": nm} for cid, nm in seen.items()]
    out.sort(key=lambda x: str((x or {}).get("name") or ""))
    return out


def _campaign_name_from_snapshots(aid: str, campaign_id: str) -> str:
    cid = str(campaign_id or "")
    if not cid:
        return ""
    for r in _snapshot_rows_last_7d(aid):
        if str((r or {}).get("campaign_id") or "") == cid:
            return str((r or {}).get("campaign_name") or cid)
    return cid


def _adset_name_from_snapshots(aid: str, adset_id: str) -> str:
    sid = str(adset_id or "")
    if not sid:
        return ""
    for r in _snapshot_rows_last_7d(aid):
        if str((r or {}).get("adset_id") or "") == sid:
            return str((r or {}).get("name") or sid)
    return sid


def heatmap_monitoring_accounts_kb() -> InlineKeyboardMarkup:
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
                    callback_data=f"mon_hm_acc|{aid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ Мониторинг", callback_data="monitoring_menu")])
    return InlineKeyboardMarkup(rows)


def _ai_budget_kb(aid: str, adset_id: str, new_budget: float, current_budget: float | None) -> InlineKeyboardMarkup:
    cents = int(round(float(new_budget or 0.0) * 100))
    cb = float(current_budget) if current_budget is not None else None
    pct = None
    if cb and cb > 0:
        pct = (float(new_budget) - cb) / cb * 100.0

    if pct is None:
        auto_text = f"✅ Применить • ${float(new_budget):.2f}"
    elif pct > 0.5:
        auto_text = f"⬆️ Увеличить на {pct:.0f}% • ${float(new_budget):.2f}"
    elif pct < -0.5:
        auto_text = f"⬇️ Снизить на {abs(pct):.0f}% • ${float(new_budget):.2f}"
    else:
        auto_text = "⏸ Оставить без изменений"

    manual_suffix = f" • ${cb:.2f}" if cb is not None else ""

    rows = [
        [InlineKeyboardButton(auto_text, callback_data=f"ai_bud_apply|{aid}|{adset_id}|{cents}")],
        [InlineKeyboardButton(f"✏️ Ручной ввод{manual_suffix}", callback_data=f"ai_bud_manual|{aid}|{adset_id}")],
    ]
    return InlineKeyboardMarkup(rows)


def _ai_ad_pause_kb(aid: str, ad_id: str, adset_id: str, spent: float | None = None) -> InlineKeyboardMarkup:
    suffix = f" • ${float(spent):.2f}" if spent is not None else ""
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(f"🛑 Отключить объявление{suffix}", callback_data=f"ai_ad_pause|{aid}|{ad_id}|{adset_id}")]]
    )


def _get_adset_budget_map(aid: str) -> dict:
    return {}


def _get_ads_map(aid: str) -> dict:
    return {}


def _count_active_ads_in_adset(aid: str, adset_id: str) -> int:
    return 0


async def _send_comparison_for_all(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: str,
    period_old,
    label_old: str,
    period_new,
    label_new: str,
) -> None:
    store = load_accounts()
    any_sent = False
    selected = []
    for aid in get_enabled_accounts_in_order():
        row = store.get(aid, {}) or {}
        if not row.get("enabled", True):
            continue
        mon = row.get("monitoring") or {}
        if isinstance(mon, dict) and bool(mon.get("compare_enabled", True)):
            selected.append(aid)

    logging.getLogger(__name__).info(
        "[monitoring_compare] selected_accounts=%d chat_id=%s",
        len(selected),
        str(chat_id),
    )

    # Если выбор пустой (все сняты) — ничего не шлём, чтобы не спамить.
    # Пользователь может включить нужные аккаунты в настройках мониторинга.
    if not selected:
        logging.getLogger(__name__).warning(
            "[monitoring_compare] no accounts selected for comparison"
        )
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚙️ Мониторинг: не выбраны аккаунты для сравнения. "
                "Зайди в 'Настройки мониторинга' и включи нужные аккаунты."
            ),
        )
        return

    for aid in selected:
        txt = build_comparison_report(aid, period_old, label_old, period_new, label_new)
        if not txt:
            continue
        any_sent = True
        await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")

    if not any_sent:
        await ctx.bot.send_message(chat_id=chat_id, text="Нет данных/нет доступа.")


def heatmap_monitoring_modes_kb(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🕒 По часам", callback_data=f"mon_hmh|{aid}")],
            [InlineKeyboardButton("📅 По дням недели", callback_data=f"mon_hmdow|{aid}")],
            [InlineKeyboardButton("🧠 Сводная + ИИ", callback_data=f"mon_hmsum|{aid}")],
            [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="mon_heatmap_menu")],
        ]
    )


def heatmap_monitoring_hourly_periods_kb(aid: str) -> InlineKeyboardMarkup:
    base = f"mon_hmh_p|{aid}"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"{base}|today"),
                InlineKeyboardButton("Вчера", callback_data=f"{base}|yday"),
            ],
            [InlineKeyboardButton("Последние 7 дней", callback_data=f"{base}|7d")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"mon_hm_acc|{aid}")],
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
                    "🟦 Статус теплокарты",
                    callback_data="heatmap_status_menu",
                )
            ],
            [
                InlineKeyboardButton(
                    "🔥 Тепловая карта",
                    callback_data="mon_heatmap_menu",
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


def heatmap_hourly_accounts_kb() -> InlineKeyboardMarkup:
    """Выбор аккаунта для почасовой тепловой карты (из меню мониторинга)."""

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
                    callback_data=f"hmh_acc|{aid}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ Мониторинг", callback_data="monitoring_menu")])
    return InlineKeyboardMarkup(rows)


def heatmap_hourly_periods_kb(aid: str) -> InlineKeyboardMarkup:
    base = f"hmh_p|{aid}"
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"{base}|today"),
                InlineKeyboardButton("Вчера", callback_data=f"{base}|yday"),
            ],
            [
                InlineKeyboardButton(
                    "Последние 7 дней", callback_data=f"{base}|7d"
                )
            ],
            [
                InlineKeyboardButton(
                    "⬅️ К аккаунтам",
                    callback_data="hm_hourly_menu",
                )
            ],
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
                    "Свой диапазон", callback_data=f"{base}|custom"
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
    ai_ads_on = bool(alerts.get("ai_cpa_ads_enabled", False))

    # Статусные строки
    days_labels = [
        _weekday_label(d)
        for d in ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
        if d in days
    ]
    days_str = ", ".join(days_labels) if days_labels else "не выбраны"
    ai_str = "ВКЛ" if ai_on else "ВЫКЛ"
    ai_ads_str = "ВКЛ" if ai_ads_on else "ВЫКЛ"

    text = (
        f"Настройки CPA-алёртов для {get_account_name(aid)}:\n\n"
        f"• Target CPA аккаунта: {account_cpa:.2f} $\n"
        f"• Частота: {_human_cpa_freq(freq)}\n"
        f"• Дни недели: {days_str}\n"
        f"• ИИ-анализ: {ai_str}\n"
        f"• AI CPA-диагностика креативов: {ai_ads_str}"
    )

    # Кнопка ИИ-анализ
    ai_btn_text = "🟢 ИИ-анализ: ВКЛ" if ai_on else "🔴 ИИ-анализ: ВЫКЛ"
    ai_ads_btn_text = "🟢 AI CPA креативы: ВКЛ" if ai_ads_on else "🔴 AI CPA креативы: ВЫКЛ"

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
        [InlineKeyboardButton(ai_ads_btn_text, callback_data=f"cpa_ai_ads|{aid}")],
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


def cpa_campaigns_kb(aid: str) -> InlineKeyboardMarkup:
    """Список кампаний для настроек CPA-алёртов."""

    st = load_accounts()
    row = st.get(aid, {"alerts": {}})
    alerts = row.get("alerts", {}) or {}
    campaign_alerts = alerts.get("campaign_alerts", {}) or {}

    def _campaign_rows_from_snapshots() -> list[dict]:
        all_rows = _snapshot_rows_last_7d(aid)

        agg: dict[str, dict] = {}
        for r in all_rows:
            cid = str((r or {}).get("campaign_id") or "")
            if not cid:
                continue
            it = agg.setdefault(
                cid,
                {"campaign_id": cid, "name": (r or {}).get("campaign_name") or cid, "spend": 0.0, "msgs": 0, "leads": 0, "total": 0},
            )
            try:
                it["spend"] = float(it.get("spend") or 0.0) + float((r or {}).get("spend") or 0.0)
            except Exception:
                pass
            try:
                it["msgs"] = int(it.get("msgs") or 0) + int((r or {}).get("msgs") or 0)
            except Exception:
                pass
            try:
                it["leads"] = int(it.get("leads") or 0) + int((r or {}).get("leads") or 0)
            except Exception:
                pass
            try:
                t = (r or {}).get("total")
                if t is None:
                    t = int((r or {}).get("msgs") or 0) + int((r or {}).get("leads") or 0)
                it["total"] = int(it.get("total") or 0) + int(t or 0)
            except Exception:
                pass

        out = list(agg.values())
        out.sort(key=lambda x: float((x or {}).get("spend") or 0.0), reverse=True)
        return out

    camps = _campaign_rows_from_snapshots()
    allowed_campaign_ids = {str((c or {}).get("campaign_id") or "") for c in camps if (c or {}).get("campaign_id")}

    kb_rows = []
    for camp in camps:
        cid = camp.get("campaign_id")
        if not cid:
            continue
        if str(cid) not in allowed_campaign_ids:
            continue
        name = camp.get("name") or cid
        cfg_c = (campaign_alerts.get(cid) or {}) if cid in campaign_alerts else {}
        target = float(cfg_c.get("target_cpa") or 0.0)
        label_suffix = (
            f"[CPA {target:.2f}$]" if target > 0 else "[CPA аккаунта]"
        )
        enabled_c = bool(cfg_c.get("enabled", False))
        indicator = "⚠️ " if enabled_c else ""
        text_btn = f"{indicator}{name} {label_suffix}".strip()

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

    return InlineKeyboardMarkup(kb_rows)


def cpa_adsets_kb(aid: str) -> InlineKeyboardMarkup:
    """Список адсетов для настроек CPA-алёртов."""

    st = load_accounts()
    row = st.get(aid, {"alerts": {}})
    alerts = row.get("alerts", {}) or {}
    adset_alerts = alerts.get("adset_alerts", {}) or {}

    def _adsets_from_snapshots() -> list[dict]:
        seen: dict[str, str] = {}
        for r in _snapshot_rows_last_7d(aid):
            adset_id = str((r or {}).get("adset_id") or "")
            if not adset_id:
                continue
            nm = (r or {}).get("name") or adset_id
            if adset_id not in seen:
                seen[adset_id] = str(nm)
        return [{"id": i, "name": n} for i, n in seen.items()]

    adsets = _adsets_from_snapshots()
    active_adset_ids = {str((r or {}).get("id") or "") for r in adsets if (r or {}).get("id")}

    kb_rows = []
    for it in adsets:
        adset_id = it.get("id")
        name = it.get("name", adset_id)
        if adset_id not in active_adset_ids:
            continue
        cfg = (adset_alerts.get(adset_id) or {}) if adset_id else {}

        target = float(cfg.get("target_cpa") or 0.0)
        label_suffix = (
            f"[CPA {target:.2f}$]" if target > 0 else "[CPA аккаунта]"
        )
        enabled_a = bool(cfg.get("enabled", False))
        indicator = "⚠️ " if enabled_a else ""
        text_btn = f"{indicator}{name} {label_suffix}".strip()

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

    return InlineKeyboardMarkup(kb_rows)


def cpa_ads_kb(aid: str) -> InlineKeyboardMarkup:
    """Список объявлений для настроек CPA-алёртов."""

    st = load_accounts()
    row = st.get(aid, {"alerts": {}})
    alerts = row.get("alerts", {}) or {}
    ad_alerts = alerts.get("ad_alerts", {}) or {}

    # No ad-level rows in snapshots. Keep UI stable: show an empty list.
    ads: list[dict] = []
    active_adset_ids: set[str] = set()
    ad_status: dict[str, str] = {}
    ad_to_adset: dict[str, str] = {}

    kb_rows = []
    for ad in ads:
        ad_id = ad.get("ad_id") or ad.get("id")
        if not ad_id:
            continue

        spend = float(ad.get("spend", 0.0) or 0.0)
        if ad_id not in ad_alerts and spend <= 0:
            continue

        status = ad_status.get(str(ad_id), "")
        adset_id = str(ad.get("adset_id") or ad_to_adset.get(str(ad_id)) or "")
        adset_active = adset_id in active_adset_ids
        if status != "ACTIVE" or not adset_active:
            continue

        name = ad.get("name") or ad_id
        cfg = ad_alerts.get(ad_id) or {}
        enabled_ad = bool(cfg.get("enabled", False))
        target = float(cfg.get("target_cpa") or 0.0)
        label_suffix = (
            f"[CPA {target:.2f}$]" if target > 0 else "[CPA вышестоящего уровня]"
        )
        indicator = "⚠️ " if enabled_ad else ""
        text_btn = f"{indicator}{name} {label_suffix}".strip()

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

    return InlineKeyboardMarkup(kb_rows)


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
    # CPA-индикатор: включён ли CPA-алёрт на любом уровне (аккаунт/кампания/адсет/объявление).
    account_cpa_val = float(a.get("account_cpa", a.get("target_cpl", 0.0)) or 0.0)
    base_enabled = bool(a.get("enabled", False)) and account_cpa_val > 0

    camp_alerts = a.get("campaign_alerts", {}) or {}
    adset_alerts = a.get("adset_alerts", {}) or {}
    ad_alerts = a.get("ad_alerts", {}) or {}

    camp_on = any(bool((cfg or {}).get("enabled", False)) for cfg in camp_alerts.values())
    adset_on = any(bool((cfg or {}).get("enabled", False)) for cfg in adset_alerts.values())
    ad_on = any(bool((cfg or {}).get("enabled", False)) for cfg in ad_alerts.values())

    aa = "⚠️" if (base_enabled or camp_on or adset_on or ad_on) else ""
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

    mr = st.get("morning_report") or {}
    level = str(mr.get("level", "ACCOUNT")).upper()
    level_human = {
        "OFF": "Выкл",
        "ACCOUNT": "Аккаунт",
        "CAMPAIGN": "Кампании",
        "ADSET": "Адсеты",
    }.get(level, "Аккаунт")

    mr_text = f"🌅 Утренний отчёт: {level_human}"

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
                    mr_text,
                    callback_data=f"mr_menu|{aid}",
                )
            ],
            [
                InlineKeyboardButton(
                    "📊 Метрика лидов",
                    callback_data=f"lead_metric|{aid}",
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
        "🤖 Команды:\n"
        "/start — главное меню\n"
        "/help — список всех команд\n"
        "/billing — биллинги и прогнозы\n"
        "/sync_accounts — синхронизация BM\n"
        "/whoami — показать user_id/chat_id\n"
        "/heatmap <act_id> — тепловая карта адсетов за 7 дней\n"
        "/heatmap_status <act_id> — статус слепка теплокарты за предыдущий полный час\n"
        "/heatmap_debug_last <act_id> — отладка: последний слепок + суммы + coverage(today/yday)\n"
        "/report_debug <act_id> yday general — отладка отчёта (params/time_range/tz/attribution/sums)\n"
        "/version — показать текущую версию бота и краткое описание\n"
        "\n"
        "🚀 Функции автопилота:\n"
        "• Автоматические рекомендации по аккаунту\n"
        "• Изменение бюджета (-20%, +20%, ручной ввод)\n"
        "• Безопасное отключение дорогих адсетов\n"
        "• Подготовка к ИИ-управлению (Пилат)\n"
    )
    await update.message.reply_text(txt)


async def cmd_report_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    parts = (update.message.text or "").strip().split()
    if len(parts) < 2:
        await update.message.reply_text("Формат: /report_debug act_xxx yday general")
        return
    aid = str(parts[1] or "").strip()
    if not aid.startswith("act_"):
        aid = "act_" + aid
    kind = str(parts[2] if len(parts) >= 3 else "yday")
    mode = str(parts[3] if len(parts) >= 4 else "general")
    try:
        txt = build_report_debug(str(aid), str(kind), str(mode))
    except Exception as e:
        txt = f"report_debug_error: {type(e).__name__}: {e}"
    await update.message.reply_text(txt)


async def cmd_ap_here(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    if not chat_id:
        return
    set_autopilot_chat_id(chat_id)
    await update.message.reply_text("✅ Этот чат установлен как чат Автопилота")


async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    await update.message.reply_text(
        "Что показать по биллингу?", reply_markup=billing_menu()
    )


async def cmd_billing_debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return
    try:
        import os
        import json
        from fb_report.constants import DATA_DIR
        from datetime import datetime

        path = os.path.join(DATA_DIR, "billing_followups.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            obj = {}

        followups = obj.get("followups") if isinstance(obj, dict) else None
        if not isinstance(followups, dict):
            followups = {}

        items = []
        for aid, it in followups.items():
            if not isinstance(it, dict):
                continue
            due = str(it.get("due_at") or "")
            items.append((str(aid), due))
        items.sort(key=lambda x: x[1])

        lines = []
        lines.append(f"FB_API_DEFAULT_DENY={str(os.getenv('FB_API_DEFAULT_DENY','1'))}")
        lines.append(f"BILLING_COOLDOWN_HOURS={str(os.getenv('BILLING_COOLDOWN_HOURS','8'))}")
        lines.append(f"followups_file={path}")
        lines.append(f"pending_followups={len(items)}")
        if items:
            lines.append("next_due:")
            for aid, due in items[:10]:
                lines.append(f"- {aid}: {due}")
        lines.append(f"now={datetime.now(ALMATY_TZ).isoformat()}")

        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        await update.message.reply_text(f"billing_debug_error: {type(e).__name__}: {e}")


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


async def cmd_heatmap_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return

    parts = update.message.text.strip().split()
    if len(parts) == 1:
        await update.message.reply_text(
            "Выберите аккаунт для статуса теплокарты:",
            reply_markup=accounts_kb("heatmap_status_acc"),
        )
        return

    aid = parts[1].strip()
    if not aid.startswith("act_"):
        aid = "act_" + aid
    text = build_heatmap_status_text(aid=aid)
    await update.message.reply_text(text)


def _build_heatmap_debug_last_text(*, aid: str) -> str:
    st = load_accounts() or {}
    row = st.get(str(aid)) or {}
    hm = (row or {}).get("heatmap") or {}
    if not isinstance(hm, dict):
        hm = {}
    include_paused = bool(hm.get("include_paused", False))
    result_mode = str(hm.get("result_mode") or "").strip().lower() or None

    now = datetime.now(ALMATY_TZ)
    max_lookback = 48
    last_snap = None
    cur = now.replace(minute=0, second=0, microsecond=0)
    for _ in range(int(max_lookback)):
        cur = cur - timedelta(hours=1)
        date_str = cur.strftime("%Y-%m-%d")
        hour_int = int(cur.strftime("%H"))
        with deny_fb_api_calls(reason="heatmap_debug_last"):
            s = load_snapshot(str(aid), date_str=str(date_str), hour=int(hour_int))
        if s:
            last_snap = s
            break

    def _sum_rows(rows: list[dict], *, active_only: bool) -> tuple[int, int, float, int]:
        started = 0
        website = 0
        spend = 0.0
        used = 0
        for r in (rows or []):
            if not isinstance(r, dict):
                continue
            if active_only:
                stt = r.get("adset_status")
                try:
                    st = str(stt).upper() if stt is not None else ""
                    if st and st not in {"ACTIVE", "UNKNOWN"}:
                        continue
                except Exception:
                    pass
            used += 1
            try:
                started += int(r.get("started_conversations") or r.get("msgs") or 0)
            except Exception:
                pass
            try:
                website += int(r.get("website_submit_applications") or r.get("leads") or 0)
            except Exception:
                pass
            try:
                spend += float(r.get("spend") or 0.0)
            except Exception:
                pass
        return int(started), int(website), float(spend), int(used)

    def _coverage_for(date_s: str) -> tuple[int, list[str]]:
        cov = 0
        missing: list[str] = []
        with deny_fb_api_calls(reason="heatmap_debug_coverage"):
            have_hours = set(list_snapshot_hours(str(aid), date_str=str(date_s)) or [])
            for h in range(24):
                if h not in have_hours:
                    missing.append(f"{h:02d}")
                    continue
                snap = load_snapshot(str(aid), date_str=str(date_s), hour=int(h)) or {}
                stt = str(snap.get("status") or "")
                if stt in {"ready", "ready_low_confidence"}:
                    cov += 1
                else:
                    missing.append(f"{h:02d}")
        return int(cov), list(missing)

    today_s = now.strftime("%Y-%m-%d")
    yday_s = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    cov_today, miss_today = _coverage_for(today_s)
    cov_yday, miss_yday = _coverage_for(yday_s)

    last_failed = None
    try:
        cur2 = now.replace(minute=0, second=0, microsecond=0)
        for _ in range(int(max_lookback)):
            cur2 = cur2 - timedelta(hours=1)
            ds2 = cur2.strftime("%Y-%m-%d")
            hh2 = int(cur2.strftime("%H"))
            with deny_fb_api_calls(reason="heatmap_debug_last_failed"):
                s2 = load_snapshot(str(aid), date_str=str(ds2), hour=int(hh2))
            if not s2:
                continue
            if str(s2.get("status") or "") != "failed":
                continue
            last_failed = s2
            break
    except Exception:
        last_failed = None

    lines: list[str] = []
    lines.append(f"🧪 heatmap_debug_last — {get_account_name(aid)}")
    lines.append(f"account_id={aid}")
    lines.append(f"include_paused={'true' if include_paused else 'false'}")
    if result_mode:
        lines.append(f"result_mode={result_mode}")
    lines.append("")
    lines.append(f"coverage_today={cov_today}/24 missing_hours={','.join(miss_today)}")
    lines.append(f"coverage_yday={cov_yday}/24 missing_hours={','.join(miss_yday)}")
    if last_failed:
        try:
            lf_date = str(last_failed.get("date") or "")
            lf_hour = int(last_failed.get("hour") or 0)
            lf_reason = str(last_failed.get("reason") or "snapshot_failed")
            lines.append(f"last_failed_hour={lf_date} {lf_hour:02d} reason={lf_reason}")
        except Exception:
            pass

    if not last_snap:
        lines.append("")
        lines.append("last_snapshot=missing")
        return "\n".join(lines)

    snap_date = str(last_snap.get("date") or "")
    snap_hour = str(last_snap.get("hour") or "")
    snap_status = str(last_snap.get("status") or "")
    snap_reason = str(last_snap.get("reason") or "")
    snap_attempts = str(last_snap.get("attempts") or "")
    snap_last_try = str(last_snap.get("last_try_at") or "")
    snap_next_try = str(last_snap.get("next_try_at") or "")
    snap_deadline = str(last_snap.get("deadline_at") or "")
    snap_meta = last_snap.get("meta") if isinstance(last_snap.get("meta"), dict) else {}
    if not isinstance(snap_meta, dict):
        snap_meta = {}
    snap_err = last_snap.get("error") if isinstance(last_snap.get("error"), dict) else {}
    if not isinstance(snap_err, dict):
        snap_err = {}
    snap_rows = last_snap.get("rows") or []

    st_all, ws_all, sp_all, used_all = _sum_rows(list(snap_rows), active_only=False)
    st_act, ws_act, sp_act, used_act = _sum_rows(list(snap_rows), active_only=True)

    computed_status = "ready"
    computed_reason = ""
    if int(st_all) <= 0 and int(ws_all) <= 0:
        computed_status = "ready_low_confidence"
        computed_reason = "no_actions"

    lines.append("")
    lines.append(f"last_snapshot_date={snap_date} hour={snap_hour}")
    lines.append(f"snapshot_status={snap_status} snapshot_reason={snap_reason}")
    lines.append(f"attempts={snap_attempts} last_try_at={snap_last_try} next_try_at={snap_next_try}")
    if snap_deadline:
        lines.append(f"deadline_at={snap_deadline}")

    # Diagnostics: snapshots-only, no FB calls.
    try:
        fb_code = snap_meta.get("fb_code") if snap_meta else None
        if fb_code in (None, ""):
            fb_code = snap_err.get("fb_code")
        fbtrace_id = snap_meta.get("fbtrace_id") if snap_meta else None
        if fbtrace_id in (None, ""):
            fbtrace_id = snap_err.get("fbtrace_id")
        msg = snap_meta.get("message") if snap_meta else None
        if msg in (None, ""):
            msg = snap_err.get("message")
        endpoint = snap_meta.get("endpoint") if snap_meta else None
        fields = snap_meta.get("fields") if snap_meta else None
        params = snap_meta.get("params") if snap_meta else None
        http_status = snap_meta.get("last_http_status") if snap_meta else None
        if http_status in (None, ""):
            http_status = snap_err.get("http_status")

        if any(x not in (None, "") for x in [endpoint, fields, params, http_status, fb_code, fbtrace_id, msg]):
            lines.append("diagnostics:")
            if endpoint:
                lines.append(f"  endpoint={endpoint}")
            if fields not in (None, ""):
                lines.append(f"  fields={fields}")
            if params not in (None, ""):
                lines.append(f"  params={params}")
            if http_status not in (None, ""):
                lines.append(f"  last_http_status={http_status}")
            if fb_code not in (None, ""):
                lines.append(f"  fb_code={fb_code}")
            if fbtrace_id not in (None, ""):
                lines.append(f"  fbtrace_id={fbtrace_id}")
            if msg not in (None, ""):
                lines.append(f"  message={msg}")
    except Exception:
        pass
    lines.append(f"computed_status={computed_status} computed_reason={computed_reason}")
    lines.append(f"rows_count={int(len(snap_rows) or 0)}")

    lines.append(
        f"sums_all started_conversations={st_all} website_submit_applications={ws_all} blended={int(st_all + ws_all)} spend={sp_all:.2f} rows_used={used_all}"
    )
    lines.append(
        f"sums_active_only started_conversations={st_act} website_submit_applications={ws_act} blended={int(st_act + ws_act)} spend={sp_act:.2f} rows_used={used_act}"
    )

    return "\n".join(lines)


async def cmd_heatmap_debug_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return

    parts = update.message.text.strip().split()
    if len(parts) == 1:
        await update.message.reply_text(
            "Выберите аккаунт для heatmap debug:",
            reply_markup=accounts_kb("hmdebug"),
        )
        return

    aid = parts[1].strip()
    if not aid.startswith("act_"):
        aid = "act_" + aid

    txt = _build_heatmap_debug_last_text(aid=str(aid))
    await update.message.reply_text(txt)


async def cmd_heatmap_debug_hour(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _allowed(update):
        return

    parts = (update.message.text or "").strip().split()
    if len(parts) < 4:
        await update.message.reply_text(
            "Формат: /heatmap_debug_hour act_xxx YYYY-MM-DD HH"
        )
        return

    aid = str(parts[1] or "").strip()
    if not aid.startswith("act_"):
        aid = "act_" + aid
    date_s = str(parts[2] or "").strip()
    hour_raw = str(parts[3] or "").strip()
    try:
        hour_i = int(hour_raw)
    except Exception:
        hour_i = 0
    if hour_i < 0:
        hour_i = 0
    if hour_i > 23:
        hour_i = 23

    with deny_fb_api_calls(reason="heatmap_debug_hour"):
        snap = load_snapshot(str(aid), date_str=str(date_s), hour=int(hour_i))

    lines: list[str] = []
    lines.append(f"🧪 heatmap_debug_hour — {get_account_name(aid)}")
    lines.append(f"account_id={aid}")
    lines.append(f"date={date_s} hour={hour_i:02d}")

    if not snap:
        lines.append("")
        lines.append("snapshot=missing")
        await update.message.reply_text("\n".join(lines))
        return

    stt = str(snap.get("status") or "")
    rsn = str(snap.get("reason") or "")
    attempts = str(snap.get("attempts") or "")
    last_try = str(snap.get("last_try_at") or "")
    next_try = str(snap.get("next_try_at") or "")
    deadline = str(snap.get("deadline_at") or "")
    meta = snap.get("meta") if isinstance(snap.get("meta"), dict) else {}
    if not isinstance(meta, dict):
        meta = {}
    err = snap.get("error") if isinstance(snap.get("error"), dict) else {}
    if not isinstance(err, dict):
        err = {}
    rows = snap.get("rows") or []

    lines.append("")
    lines.append(f"snapshot_status={stt} snapshot_reason={rsn}")
    lines.append(f"attempts={attempts} last_try_at={last_try} next_try_at={next_try}")
    if deadline:
        lines.append(f"deadline_at={deadline}")

    try:
        endpoint = meta.get("endpoint")
        fields = meta.get("fields")
        params = meta.get("params")
        http_status = meta.get("last_http_status")
        fb_code = meta.get("fb_code")
        fbtrace_id = meta.get("fbtrace_id")
        msg = meta.get("message")

        if http_status in (None, ""):
            http_status = err.get("http_status")
        if fb_code in (None, ""):
            fb_code = err.get("fb_code")
        if fbtrace_id in (None, ""):
            fbtrace_id = err.get("fbtrace_id")
        if msg in (None, ""):
            msg = err.get("message")

        if any(x not in (None, "") for x in [endpoint, fields, params, http_status, fb_code, fbtrace_id, msg]):
            lines.append("diagnostics:")
            if endpoint:
                lines.append(f"  endpoint={endpoint}")
            if fields not in (None, ""):
                lines.append(f"  fields={fields}")
            if params not in (None, ""):
                lines.append(f"  params={params}")
            if http_status not in (None, ""):
                lines.append(f"  last_http_status={http_status}")
            if fb_code not in (None, ""):
                lines.append(f"  fb_code={fb_code}")
            if fbtrace_id not in (None, ""):
                lines.append(f"  fbtrace_id={fbtrace_id}")
            if msg not in (None, ""):
                lines.append(f"  message={msg}")
    except Exception:
        pass

    try:
        started = 0
        website = 0
        spend = 0.0
        used = 0
        for r in (rows or []):
            if not isinstance(r, dict):
                continue
            used += 1
            try:
                started += int(r.get("started_conversations") or r.get("msgs") or 0)
            except Exception:
                pass
            try:
                website += int(r.get("website_submit_applications") or r.get("leads") or 0)
            except Exception:
                pass
            try:
                spend += float(r.get("spend") or 0.0)
            except Exception:
                pass
        lines.append(f"rows_count={int(len(rows) or 0)}")
        lines.append(
            f"sums started_conversations={int(started)} website_submit_applications={int(website)} blended={int(started + website)} spend={float(spend):.2f} rows_used={int(used)}"
        )
    except Exception:
        pass

    await update.message.reply_text("\n".join(lines))


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

    try:
        await _on_cb_internal(update, context, q, chat_id, data)
    except Exception as e:
        logging.getLogger(__name__).exception(
            "callback_error scope=autopilot chat_id=%s data=%s",
            str(chat_id),
            str(data),
            exc_info=e,
        )
        try:
            await q.answer("⚠️ Ошибка обработки кнопки (см. логи)", show_alert=True)
        except Exception:
            pass
        try:
            await safe_edit_message(q, "⚠️ Ошибка обработки кнопки, смотри логи.")
        except Exception:
            pass
    return


async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if not _allowed(update):
        await q.edit_message_text("⛔️ Нет доступа.")
        return

    data = q.data or ""
    chat_id = str(q.message.chat.id)

    try:
        await _on_cb_internal(update, context, q, chat_id, data)
    except Exception as e:
        logging.getLogger(__name__).exception(
            "callback_error scope=general chat_id=%s data=%s",
            str(chat_id),
            str(data),
            exc_info=e,
        )
        try:
            await q.answer("⚠️ Ошибка обработки кнопки (см. логи)", show_alert=True)
        except Exception:
            pass
        try:
            await safe_edit_message(q, "⚠️ Ошибка обработки кнопки, смотри логи.")
        except Exception:
            pass


async def _on_cb_internal(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    q,
    chat_id: str,
    data: str,
):
    if data == "noop":
        await q.answer("Ок", show_alert=False)
        return

    if data == "version":
        text = _build_version_text()
        await context.bot.send_message(chat_id, text)
        return

    if data == "menu":
        await safe_edit_message(q, "🤖 Выберите действие:", reply_markup=main_menu())
        return

    if data == "autopilot_menu":
        await safe_edit_message(
            q,
            "Выберите кабинет для Автопилота:",
            reply_markup=_autopilot_menu_kb(),
        )
        return

    if data == "ap_set_chat":
        try:
            set_autopilot_chat_id(chat_id)
        except Exception:
            pass
        await q.answer("Чат Автопилота обновлён")
        await safe_edit_message(
            q,
            "✅ Этот чат установлен как чат Автопилота\n\nВыберите кабинет для Автопилота:",
            reply_markup=_autopilot_menu_kb(),
        )
        return

    if data.startswith("autopilot_acc|"):
        aid = data.split("|", 1)[1]
        text = _autopilot_dashboard_text(aid)
        await safe_edit_message(q, text, reply_markup=_autopilot_kb(aid))
        return

    if data.startswith("ap_groups|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            _autopilot_groups_menu_text(aid),
            reply_markup=_autopilot_groups_kb(aid),
        )
        return

    if data.startswith("ap_group_create|"):
        aid = data.split("|", 1)[1]
        gid = uuid.uuid4().hex[:8]
        grp = {
            "name": f"Группа {gid}",
            "campaign_ids": [],
            "kpi": "total",
            "lead_metric": None,
            "goals": {"leads": None, "period": "day", "until": None, "target_cpl": None, "planned_budget": None},
        }
        ap = _autopilot_get(aid)
        groups = ap.get("campaign_groups") or {}
        if not isinstance(groups, dict):
            groups = {}
        groups[gid] = grp
        ap["campaign_groups"] = groups
        ap["active_group_id"] = gid
        _autopilot_set(aid, ap)
        await q.answer("Группа создана")
        await safe_edit_message(
            q,
            _autopilot_group_menu_text(aid, gid)
            + "\n\nПодсказка: нажми '📌 Кампании', чтобы добавить рекламные кампании в группу."
            + "\nЕсли хочешь — напиши название группы в чат.",
            reply_markup=_autopilot_group_kb(aid, gid),
        )
        context.user_data["await_ap_group_rename"] = {"aid": aid, "gid": gid}
        return

    if data.startswith("ap_group_open|"):
        _p, aid, gid = data.split("|", 2)
        await safe_edit_message(
            q,
            _autopilot_group_menu_text(aid, gid),
            reply_markup=_autopilot_group_kb(aid, gid),
        )
        return

    if data.startswith("ap_group_rename|"):
        _p, aid, gid = data.split("|", 2)
        grp = _autopilot_group_get(aid, gid)
        cur_name = grp.get("name") or gid
        await safe_edit_message(
            q,
            f"✏️ Переименовать группу\n\nТекущее название: {cur_name}\n\nНапиши новое название в чат.",
            reply_markup=_autopilot_group_kb(aid, gid),
        )
        context.user_data["await_ap_group_rename"] = {"aid": aid, "gid": gid}
        return

    if data.startswith("ap_group_select|"):
        _p, aid, gid = data.split("|", 2)
        _autopilot_set(aid, {"active_group_id": str(gid)})
        await q.answer("Активная группа выбрана")
        await safe_edit_message(
            q,
            _autopilot_dashboard_text(aid),
            reply_markup=_autopilot_kb(aid),
        )
        return

    if data.startswith("ap_group_toggle|"):
        _p, aid, gid = data.split("|", 2)
        cur = set(_autopilot_active_group_ids(aid))
        if str(gid) in cur:
            cur.remove(str(gid))
            await q.answer("Отключил слежение за группой")
        else:
            cur.add(str(gid))
            await q.answer("Включил слежение за группой")
        _autopilot_set_active_group_ids(aid, sorted(cur))
        await safe_edit_message(
            q,
            _autopilot_group_menu_text(aid, gid),
            reply_markup=_autopilot_group_kb(aid, gid),
        )
        return

    if data.startswith("ap_group_off|"):
        aid = data.split("|", 1)[1]
        _autopilot_set(aid, {"active_group_id": None, "active_group_ids": []})
        await q.answer("Группы отключены")
        await safe_edit_message(
            q,
            _autopilot_dashboard_text(aid),
            reply_markup=_autopilot_kb(aid),
        )
        return

    if data.startswith("ap_group_delete|"):
        _p, aid, gid = data.split("|", 2)
        _autopilot_group_delete(aid, gid)
        await q.answer("Группа удалена")
        await safe_edit_message(
            q,
            _autopilot_groups_menu_text(aid),
            reply_markup=_autopilot_groups_kb(aid),
        )
        return

    if data.startswith("ap_group_kpi|"):
        _p, aid, gid = data.split("|", 2)
        await safe_edit_message(
            q,
            f"📊 KPI группы — {get_account_name(aid)}\n\nВыберите, что считать KPI:",
            reply_markup=_autopilot_group_kpi_kb(aid, gid),
        )
        return

    if data.startswith("ap_group_kpi_set|"):
        _p, aid, gid, code = data.split("|", 3)
        grp = _autopilot_group_get(aid, gid)
        grp["kpi"] = str(code)
        _autopilot_group_set(aid, gid, grp)
        await q.answer("KPI обновлён")
        await safe_edit_message(
            q,
            f"📊 KPI группы — {get_account_name(aid)}\n\nВыберите, что считать KPI:",
            reply_markup=_autopilot_group_kpi_kb(aid, gid),
        )
        return

    if data.startswith("ap_group_campaigns|"):
        _p, aid, gid = data.split("|", 2)
        kb, active_ids = _autopilot_group_campaigns_kb_active_only(aid, gid)
        grp = _autopilot_group_get(aid, gid)
        cur = set(str(x) for x in (grp.get("campaign_ids") or []) if x)
        cleaned = set(x for x in cur if x in active_ids)
        if cleaned != cur:
            grp["campaign_ids"] = sorted(cleaned)
            _autopilot_group_set(aid, gid, grp)
            kb, _active_ids = _autopilot_group_campaigns_kb_active_only(aid, gid)
        await safe_edit_message(
            q,
            f"📌 Кампании группы — {get_account_name(aid)}\n\nОтметьте 2–10 кампаний:",
            reply_markup=kb,
        )
        return

    if data.startswith("ap_group_camp_toggle|"):
        _p, aid, gid, tok = data.split("|", 3)
        try:
            cid = _campaign_token_to_id(tok)
        except Exception:
            await q.answer("Кампания не найдена. Открой 'Кампании' ещё раз.", show_alert=True)
            return

        grp = _autopilot_group_get(aid, gid)
        cur = set(str(x) for x in (grp.get("campaign_ids") or []) if x)
        if str(cid) in cur:
            cur.remove(str(cid))
        else:
            _kb_tmp, active_ids = _autopilot_group_campaigns_kb_active_only(aid, gid)
            if str(cid) not in set(str(x) for x in (active_ids or set()) if x):
                await q.answer(
                    "Кампания недоступна в слепках за 7 дней. "
                    "Собери слепок и открой список ещё раз.",
                    show_alert=True,
                )
                return
            cur.add(str(cid))
        grp["campaign_ids"] = sorted(cur)
        _autopilot_group_set(aid, gid, grp)
        kb, _active_ids = _autopilot_group_campaigns_kb_active_only(aid, gid)
        await safe_edit_message(
            q,
            f"📌 Кампании группы — {get_account_name(aid)}\n\nОтметьте 2–10 кампаний:",
            reply_markup=kb,
        )
        return

    if data.startswith("ap_group_leadmetric|"):
        _p, aid, gid = data.split("|", 2)
        grp = _autopilot_group_get(aid, gid)
        options = _discover_lead_metrics_for_account(aid)

        current = grp.get("lead_metric")
        current_at = (current or {}).get("action_type") if isinstance(current, dict) else None

        if not options:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data=f"ap_group_open|{aid}|{gid}")]]
            )
            await safe_edit_message(
                q,
                "❗️Не найдено метрик заявок с сайта (за вчера).\n"
                "Проверь события в Ads Manager.",
                reply_markup=kb,
            )
            return

        mapping = {str(i): it for i, it in enumerate(options)}
        context.user_data["ap_group_lead_metric_options"] = {
            "aid": aid,
            "gid": gid,
            "items": mapping,
        }

        rows = []
        for i, it in mapping.items():
            label = it.get("label") or it.get("action_type")
            if current_at and it.get("action_type") == current_at:
                label = f"✅ {label}"
            rows.append(
                [
                    InlineKeyboardButton(
                        str(label),
                        callback_data=f"ap_group_leadmetric_set|{aid}|{gid}|{i}",
                    )
                ]
            )
        rows.append(
            [
                InlineKeyboardButton(
                    "Сбросить (по аккаунту)",
                    callback_data=f"ap_group_leadmetric_clear|{aid}|{gid}",
                )
            ]
        )
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"ap_group_open|{aid}|{gid}")])
        await safe_edit_message(q, "📊 Метрика заявок группы (за вчера):", reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("ap_group_leadmetric_clear|"):
        _p, aid, gid = data.split("|", 2)
        grp = _autopilot_group_get(aid, gid)
        grp["lead_metric"] = None
        _autopilot_group_set(aid, gid, grp)
        await q.answer("Метрика сброшена")
        await safe_edit_message(
            q,
            _autopilot_group_menu_text(aid, gid),
            reply_markup=_autopilot_group_kb(aid, gid),
        )
        return

    if data.startswith("ap_group_leadmetric_set|"):
        _p, aid, gid, idx = data.split("|", 3)

        stash = context.user_data.get("ap_group_lead_metric_options") or {}
        if stash.get("aid") != aid or stash.get("gid") != gid:
            await q.answer("Список метрик устарел. Открой заново.", show_alert=True)
            return

        items = stash.get("items") or {}
        it = items.get(str(idx))
        if not it:
            await q.answer("Метрика не найдена. Открой заново.", show_alert=True)
            return

        action_type = it.get("action_type")
        label = it.get("label")
        if not action_type:
            await q.answer("Пустой action_type.", show_alert=True)
            return

        grp = _autopilot_group_get(aid, gid)
        grp["lead_metric"] = {
            "action_type": str(action_type),
            "label": str(label or action_type),
        }
        _autopilot_group_set(aid, gid, grp)
        await q.answer("Метрика обновлена")
        await safe_edit_message(
            q,
            _autopilot_group_menu_text(aid, gid),
            reply_markup=_autopilot_group_kb(aid, gid),
        )
        return

    if data.startswith("ap_mode|"):
        try:
            _p, aid, mode = data.split("|", 2)
        except ValueError:
            await q.answer("Некорректные данные режима.", show_alert=True)
            return

        ap = _autopilot_get(aid)
        old = str(ap.get("mode") or "OFF").upper()
        new = str(mode or "OFF").upper()

        _autopilot_set(aid, {"mode": new})
        append_autopilot_event(
            aid,
            {
                "type": "mode_change",
                "from": old,
                "to": new,
                "chat_id": str(chat_id),
            },
        )

        await q.answer(f"Режим: {_autopilot_human_mode(new)}")
        text = _autopilot_dashboard_text(aid)
        await safe_edit_message(q, text, reply_markup=_autopilot_kb(aid))
        return

    if data.startswith("ap_set_leads|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            "🎯 Цель по лидам\n\n"
            "Напиши в чат число лидов (например 20).\n"
            "0 — сбросить цель.",
            reply_markup=_autopilot_kb(aid),
        )
        gid = str(_autopilot_get(aid).get("active_group_id") or "").strip() or None
        context.user_data["await_ap_leads_for"] = {"aid": aid, "gid": gid}
        return

    if data.startswith("ap_set_cpl|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            "💰 Целевой CPL\n\n"
            "Напиши в чат число в $ (например 1.2).\n"
            "0 — сбросить цель.",
            reply_markup=_autopilot_kb(aid),
        )
        gid = str(_autopilot_get(aid).get("active_group_id") or "").strip() or None
        context.user_data["await_ap_cpl_for"] = {"aid": aid, "gid": gid}
        return

    if data.startswith("ap_set_budget|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            "💵 Плановый бюджет\n\n"
            "Напиши в чат число в $ (например 30).\n"
            "0 — сбросить план.",
            reply_markup=_autopilot_kb(aid),
        )
        gid = str(_autopilot_get(aid).get("active_group_id") or "").strip() or None
        context.user_data["await_ap_budget_for"] = {"aid": aid, "gid": gid}
        return

    if data.startswith("ap_period|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(q, "Выберите период цели:", reply_markup=_autopilot_period_kb(aid))
        return

    if data.startswith("ap_period_set|"):
        try:
            _p, aid, code = data.split("|", 2)
        except ValueError:
            await q.answer("Некорректные данные периода.", show_alert=True)
            return

        ap = _autopilot_get(aid)
        active_gid = str(ap.get("active_group_id") or "").strip() or None
        if active_gid:
            grp = _autopilot_group_get(aid, active_gid)
            goals = grp.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
        else:
            goals = ap.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}

        code = str(code or "day")
        goals["period"] = code
        if code != "until":
            goals["until"] = None

        if active_gid:
            grp["goals"] = goals
            _autopilot_group_set(aid, active_gid, grp)
        else:
            _autopilot_set(aid, {"goals": goals})
        append_autopilot_event(
            aid,
            {
                "type": "period_set",
                "period": code,
                "chat_id": str(chat_id),
            },
        )

        if code == "until":
            await q.answer("Период: до даты")
            await context.bot.send_message(
                chat_id,
                "Введите дату в формате ДД.ММ.ГГГГ (например 25.01.2026)",
            )
            context.user_data["await_ap_until_for"] = {"aid": aid, "gid": active_gid}
            return

        await q.answer("Период обновлён")
        text = _autopilot_dashboard_text(aid)
        await safe_edit_message(q, text, reply_markup=_autopilot_kb(aid))
        return

    if data.startswith("ap_toggle_reenable|"):
        aid = data.split("|", 1)[1]
        ap = _autopilot_get(aid)
        limits = ap.get("limits") or {}
        if not isinstance(limits, dict):
            limits = {}
        cur = bool(limits.get("allow_reenable_ads", False))
        limits["allow_reenable_ads"] = not cur
        _autopilot_set(aid, {"limits": limits})
        append_autopilot_event(
            aid,
            {
                "type": "toggle",
                "key": "allow_reenable_ads",
                "value": bool(limits.get("allow_reenable_ads")),
                "chat_id": str(chat_id),
            },
        )
        text = _autopilot_dashboard_text(aid)
        await safe_edit_message(q, text, reply_markup=_autopilot_kb(aid))
        return

    if data.startswith("ap_toggle_pause_adsets|"):
        aid = data.split("|", 1)[1]
        ap = _autopilot_get(aid)
        limits = ap.get("limits") or {}
        if not isinstance(limits, dict):
            limits = {}
        cur = bool(limits.get("allow_pause_adsets", False))
        limits["allow_pause_adsets"] = not cur
        _autopilot_set(aid, {"limits": limits})
        append_autopilot_event(
            aid,
            {
                "type": "toggle",
                "key": "allow_pause_adsets",
                "value": bool(limits.get("allow_pause_adsets")),
                "chat_id": str(chat_id),
            },
        )
        text = _autopilot_dashboard_text(aid)
        await safe_edit_message(q, text, reply_markup=_autopilot_kb(aid))
        return

    if data.startswith("ap_analyze|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(q, f"Считаю анализ для {get_account_name(aid)}…")

        append_autopilot_event(
            aid,
            {
                "type": "analysis_run",
                "scope": "adset",
                "chat_id": str(chat_id),
            },
        )

        text = _autopilot_analysis_text(aid)
        await safe_edit_message(q, text, reply_markup=_autopilot_analysis_kb(aid))
        return

    if data.startswith("ap_hm|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            "Выберите период для рекомендаций по часам:",
            reply_markup=_autopilot_hm_kb(aid),
        )
        return

    if data.startswith("ap_hm_p|"):
        try:
            _p, aid, mode = data.split("|", 2)
        except ValueError:
            await q.answer("Некорректный период.", show_alert=True)
            return

        context.user_data["ap_hm_last"] = {"aid": str(aid), "mode": str(mode)}

        await safe_edit_message(q, f"Строю heatmap для {get_account_name(aid)}…")

        append_autopilot_event(
            aid,
            {
                "type": "heatmap_view",
                "mode": str(mode),
                "chat_id": str(chat_id),
            },
        )

        try:
            heat_txt, summary = build_hourly_heatmap_for_account(aid, get_account_name_fn=get_account_name, mode=str(mode))
        except Exception:
            heat_txt, summary = ("Не удалось построить тепловую карту.", {})

        extra = _autopilot_hm_summary(summary or {})
        text = str(heat_txt or "") + "\n\n" + str(extra or "")
        await safe_edit_message(q, text, reply_markup=_autopilot_hm_kb(aid))
        return

    if data.startswith("ap_hm_toggle_paused|"):
        aid = data.split("|", 1)[1]
        st = load_accounts() or {}
        row = st.get(str(aid)) or {}
        hm = (row or {}).get("heatmap") or {}
        if not isinstance(hm, dict):
            hm = {}
        cur = bool(hm.get("include_paused", False))
        hm["include_paused"] = not cur
        row["heatmap"] = hm
        st[str(aid)] = row
        save_accounts(st)

        await q.answer("Фильтр обновлён")

        last = context.user_data.get("ap_hm_last") or {}
        last_aid = str((last or {}).get("aid") or "")
        last_mode = str((last or {}).get("mode") or "")
        if last_aid == str(aid) and last_mode:
            try:
                heat_txt, summary = build_hourly_heatmap_for_account(
                    aid,
                    get_account_name_fn=get_account_name,
                    mode=str(last_mode),
                )
            except Exception:
                heat_txt, summary = ("Не удалось построить тепловую карту.", {})
            extra = _autopilot_hm_summary(summary or {})
            text = str(heat_txt or "") + "\n\n" + str(extra or "")
            await safe_edit_message(q, text, reply_markup=_autopilot_hm_kb(aid))
            return

        await safe_edit_message(
            q,
            "Фильтр обновлён. Выберите период для рекомендаций по часам:",
            reply_markup=_autopilot_hm_kb(aid),
        )
        return

    if data.startswith("aphmforce|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(str(aid), {})
        ap = (row or {}).get("autopilot") or {}
        if not isinstance(ap, dict):
            ap = {}

        mode = str(ap.get("mode") or "OFF").upper()
        if mode != "AUTO_LIMITS":
            await q.answer("Force доступен только в режиме 'Авто с лимитами'.", show_alert=True)
            return

        hs = ap.get("heatmap_state") or {}
        if not isinstance(hs, dict):
            hs = {}

        until = (datetime.now(ALMATY_TZ) + timedelta(hours=1)).isoformat()
        hs["force_until"] = until
        ap["heatmap_state"] = hs
        row["autopilot"] = ap
        st[str(aid)] = row
        save_accounts(st)

        append_autopilot_event(
            aid,
            {
                "type": "heatmap_force_granted",
                "until": until,
                "chat_id": str(chat_id),
            },
        )

        await safe_edit_message(
            q,
            f"✅ Разрешил heatmap-применения сверх лимитов до {datetime.now(ALMATY_TZ).strftime('%H:%M')}+1ч.",
            reply_markup=_autopilot_kb(aid),
        )
        return

    if data.startswith("ap_suggest|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(q, f"Генерирую действия для {get_account_name(aid)}…")

        ap_chat_id, _src = _resolve_autopilot_chat_id_logged(reason="ap_suggest")

        win = prev_full_hour_window()
        window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"
        with deny_fb_api_calls(reason="autopilot_suggest_dataset"):
            _ds, ds_status, ds_reason, ds_meta = get_heatmap_dataset(
                str(aid),
                date_str=str(win.get("date") or ""),
                hours=[int(win.get("hour") or 0)],
            )

        ap = _autopilot_get(aid)
        mode = str(ap.get("mode") or "OFF").upper()
        try:
            with deny_fb_api_calls(reason="autopilot_suggest_generate"):
                actions, reasons = _ap_generate_actions(aid, debug=True)  # type: ignore[misc]
        except Exception:
            actions, reasons = ([], ["api_error"])
        append_autopilot_event(
            aid,
            {
                "type": "actions_generated",
                "count": int(len(actions)),
                "chat_id": str(chat_id),
            },
        )

        if not actions:
            reason_txt = _ap_top_reasons(reasons or [], n=2)
            await safe_edit_message(
                q,
                "Нет подходящих действий по текущим данным.\n\n"
                + "Источник данных: heatmap cache\n"
                + f"Окно: {window_label}\n"
                + f"Слепок: {ds_status} ({ds_reason})\n\n"
                + (f"Причина: {reason_txt}\n\n" if reason_txt else "")
                + "Подсказка: проверь, что есть spend сегодня и что adset ACTIVE/SCHEDULED.",
                reply_markup=_autopilot_analysis_kb(aid),
            )
            return

        pending = context.bot_data.setdefault("ap_pending_actions", {})
        auto_applied = 0
        for act in actions:
            act["aid"] = str(aid)

            # ADVISOR: только рекомендации/notice.
            if mode == "ADVISOR":
                token = uuid.uuid4().hex[:10]
                act["token"] = token
                pending[token] = act
                kb = _ap_action_kb(allow_apply=False, token=token, allow_edit=False)
                await context.bot.send_message(ap_chat_id, _ap_action_text(act), reply_markup=kb)
                continue

            # AUTO_LIMITS: автоприменяем только строго в рамках лимитов.
            if mode == "AUTO_LIMITS":
                ok, why = _ap_within_limits_for_auto(aid, act)
                if ok:
                    kind = str(act.get("kind") or "")
                    token = uuid.uuid4().hex[:10]
                    act["token"] = token

                    if kind == "budget_pct":
                        try:
                            pct_f = float(act.get("percent") or 0.0)
                        except Exception:
                            pct_f = 0.0
                        res = apply_budget_change(str(act.get("adset_id") or ""), pct_f)
                        append_autopilot_event(
                            aid,
                            {
                                "type": "action_auto_apply",
                                "token": token,
                                "kind": kind,
                                "adset_id": str(act.get("adset_id") or ""),
                                "percent": pct_f,
                                "status": res.get("status"),
                                "message": res.get("message"),
                                "chat_id": str(chat_id),
                            },
                        )
                        await context.bot.send_message(
                            ap_chat_id,
                            "🤖 Авто с лимитами: применено автоматически\n\n" + str(res.get("message") or "") + "\n\n" + _ap_action_text(act),
                        )
                        auto_applied += 1
                        continue

                    if kind == "pause_ad":
                        ad_id = str(act.get("ad_id") or "")
                        adset_id = str(act.get("adset_id") or "")
                        try:
                            active_cnt = _count_active_ads_in_adset(aid, adset_id)
                        except Exception:
                            active_cnt = 0

                        if active_cnt <= 1:
                            ok = False
                            why = "single_active_ad"
                        else:
                            with allow_fb_api_calls(reason="ap_suggest_apply"):
                                res = pause_ad(ad_id)
                            append_autopilot_event(
                                aid,
                                {
                                    "type": "action_auto_apply",
                                    "token": token,
                                    "kind": kind,
                                    "adset_id": adset_id,
                                    "ad_id": ad_id,
                                    "status": res.get("status"),
                                    "message": res.get("message") or res.get("exception"),
                                    "chat_id": str(chat_id),
                                },
                            )
                            await context.bot.send_message(
                                ap_chat_id,
                                "🤖 Авто с лимитами: применено автоматически\n\n" + str(res.get("message") or res.get("exception") or "") + "\n\n" + _ap_action_text(act),
                            )
                            auto_applied += 1
                            continue

                    if kind == "pause_adset":
                        # В AUTO_LIMITS всё равно только если явно включено allow_pause_adsets (генератор уже отфильтровал).
                        with allow_fb_api_calls(reason="ap_suggest_apply"):
                            res = disable_entity(str(act.get("adset_id") or ""))
                        append_autopilot_event(
                            aid,
                            {
                                "type": "action_auto_apply",
                                "token": token,
                                "kind": kind,
                                "adset_id": str(act.get("adset_id") or ""),
                                "status": res.get("status"),
                                "message": res.get("message"),
                                "chat_id": str(chat_id),
                            },
                        )
                        await context.bot.send_message(
                            ap_chat_id,
                            "🤖 Авто с лимитами: применено автоматически\n\n" + (res.get("message") or "") + "\n\n" + _ap_action_text(act),
                        )
                        auto_applied += 1
                        continue

                # не в лимитах -> отправляем как обычную рекомендацию с кнопками
                token = uuid.uuid4().hex[:10]
                act["token"] = token
                pending[token] = act
                kind = str(act.get("kind") or "")
                allow_edit = kind == "budget_pct"
                kb = _ap_action_kb(allow_apply=True, token=token, allow_edit=allow_edit)
                await context.bot.send_message(
                    ap_chat_id,
                    _ap_action_text(act)
                    + f"\n\n⚠️ Вне лимитов режима 'Авто с лимитами': {why}",
                    reply_markup=kb,
                )
                continue

            # SEMI / OFF: SEMI — подтверждение вручную; OFF — по факту тоже не должен предлагать, но оставим безопасно.
            token = uuid.uuid4().hex[:10]
            act["token"] = token
            pending[token] = act

            kind = str(act.get("kind") or "")
            allow_edit = kind == "budget_pct"
            kb = _ap_action_kb(
                allow_apply=bool(act.get("allow_apply")), token=token, allow_edit=allow_edit
            )
            await context.bot.send_message(chat_id, _ap_action_text(act), reply_markup=kb)

        await safe_edit_message(
            q,
            f"Отправил действий: {len(actions)}\n"
            + (f"Автоприменено: {auto_applied}\n" if auto_applied else "")
            + "Каждое действие отдельным сообщением ниже.",
            reply_markup=_autopilot_analysis_kb(aid),
        )
        return

    if data.startswith("ap_dry|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(q, f"🧪 Dry-run: {get_account_name(aid)} — считаю, что бы сделал…")

        ap_chat_id, _src = _resolve_autopilot_chat_id_logged(reason="ap_dry")

        ap = _autopilot_get(aid)
        mode = str(ap.get("mode") or "OFF").upper()
        gids = _autopilot_active_group_ids(aid)

        win = prev_full_hour_window()
        window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"

        with deny_fb_api_calls(reason="autopilot_dry_dataset"):
            _ds, ds_status, ds_reason, ds_meta = get_heatmap_dataset(
                str(aid),
                date_str=str(win.get("date") or ""),
                hours=[int(win.get("hour") or 0)],
            )

        lines = [
            f"🧪 Dry-run автопилота: {get_account_name(aid)}",
            f"Режим: {mode}",
            "Источник данных: heatmap cache",
            f"Окно: {window_label}",
            f"Слепок: {ds_status} ({ds_reason})",
            "",
        ]
        if mode not in {"AUTO_LIMITS", "SEMI", "ADVISOR", "OFF"}:
            lines.append(f"⚠️ Неизвестный режим: {mode}")
            lines.append("")

        if not gids:
            lines.append("Действий: 0")
            lines.append(f"Причина: {_ap_reason_human('no_monitored_groups')}")
            await context.bot.send_message(ap_chat_id, "\n".join(lines), disable_notification=False)
            await safe_edit_message(q, "🧪 Dry-run завершён.", reply_markup=_autopilot_analysis_kb(aid))
            return

        total_actions = 0
        reason_counts: Counter[str] = Counter()
        for gid in gids:
            eff = _autopilot_effective_config_for_group(aid, gid)
            gname = eff.get("group_name") or str(gid)
            try:
                with deny_fb_api_calls(reason="autopilot_dry_generate"):
                    actions, reasons = _ap_generate_actions(aid, eff=eff, debug=True)  # type: ignore[misc]
            except Exception:
                actions, reasons = ([], ["api_error"])

            total_actions += int(len(actions or []))
            if actions:
                lines.append(f"🤖 Группа: {gname} — идеи: {len(actions)}")
                for act in (actions or [])[:5]:
                    lines.append("")
                    lines.append(_ap_action_text(act))
            else:
                reason_counts.update([str(x) for x in (reasons or []) if x])
                reason_txt = _ap_top_reasons(reasons or [], n=2)
                lines.append(f"🤖 Группа: {gname} — действий 0")
                if reason_txt:
                    lines.append(f"Причина: {reason_txt}")
            lines.append("")

        if total_actions <= 0:
            top = _ap_top_reasons(list(reason_counts.elements()) or [], n=2)
            if top:
                lines.append(f"Итого причина: {top}")

        await context.bot.send_message(ap_chat_id, "\n".join(lines), disable_notification=False)
        await safe_edit_message(q, "🧪 Dry-run завершён.", reply_markup=_autopilot_analysis_kb(aid))
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

    if data == "heatmap_status_menu":
        await safe_edit_message(
            q,
            "Выберите аккаунт для статуса теплокарты:",
            reply_markup=accounts_kb("heatmap_status_acc"),
        )
        return

    if data.startswith("heatmap_status_acc|"):
        aid = data.split("|", 1)[1]
        text = build_heatmap_status_text(aid=aid)
        await safe_edit_message(q, text, reply_markup=monitoring_menu_kb())
        return

    if data.startswith("hm_collect_prev|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            f"📌 Собираю слепок предыдущего часа для {get_account_name(aid)}…",
            reply_markup=_autopilot_analysis_kb(aid),
        )
        try:
            await run_heatmap_snapshot_collector_once(context, aid=str(aid))
        except Exception:
            pass
        text = build_heatmap_status_text(aid=aid)
        await safe_edit_message(q, text, reply_markup=_autopilot_analysis_kb(aid))
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

        log = logging.getLogger(__name__)
        t_all = pytime.monotonic()
        log.info(
            "[focus_ai_now] start aid=%s level=%s mode=%s",
            aid,
            level,
            mode,
        )

        win = prev_full_hour_window(now=datetime.now(ALMATY_TZ))
        date_str = str(win.get("date") or "")
        hour_int = int(win.get("hour") or 0)
        window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"

        with deny_fb_api_calls(reason="ai_focus_now_dataset"):
            ds, ds_status, ds_reason, ds_meta = get_heatmap_dataset(
                str(aid),
                date_str=date_str,
                hours=[hour_int],
            )

        if ds_status != "ready" or not ds or not (ds.get("rows") or []):
            attempts = (ds_meta or {}).get("attempts")
            last_try_at = (ds_meta or {}).get("last_try_at")
            next_try_at = (ds_meta or {}).get("next_try_at")
            txt = (
                "📊 Разовый отчёт Фокус-ИИ\n"
                f"Объект: {get_account_name(aid)}\n"
                f"Уровень: {level_human}\n"
                f"Источник данных: heatmap cache\n"
                f"Окно: {date_str} {window_label}\n"
                f"Слепок: {ds_status} ({ds_reason})\n"
            )
            if attempts is not None:
                txt += f"Попытки: {attempts}\n"
            if last_try_at:
                txt += f"Последняя попытка: {last_try_at}\n"
            if next_try_at:
                txt += f"Следующая попытка: {next_try_at}\n"
            txt += "\nЕсли слепка нет или он собирается — нажми '📌 Собрать слепок предыдущего часа' и повтори."

            await safe_edit_message(q, txt, reply_markup=focus_ai_main_kb())
            return

        with deny_fb_api_calls(reason="ai_focus_now_dataset"):
            rows = list(ds.get("rows") or [])
            rows.sort(key=lambda x: float((x or {}).get("spend") or 0.0), reverse=True)
            rows = rows[:FOCUS_AI_MAX_OBJECTS]

            spend_sum = 0.0
            msgs_sum = 0
            leads_sum = 0
            total_sum = 0
            spend_for_msgs_sum = 0.0
            spend_for_leads_sum = 0.0
            spend_for_total_sum = 0.0
            for r in rows:
                try:
                    spend_sum += float((r or {}).get("spend") or 0.0)
                except Exception:
                    pass
                try:
                    msgs_sum += int((r or {}).get("msgs") or 0)
                except Exception:
                    pass
                try:
                    leads_sum += int((r or {}).get("leads") or 0)
                except Exception:
                    pass
                try:
                    total_sum += int((r or {}).get("total") or 0)
                except Exception:
                    pass

                try:
                    spend_for_msgs_sum += float((r or {}).get("spend_for_msgs") or (float((r or {}).get("spend") or 0.0) if int((r or {}).get("msgs") or 0) > 0 else 0.0))
                except Exception:
                    pass
                try:
                    spend_for_leads_sum += float((r or {}).get("spend_for_leads") or (float((r or {}).get("spend") or 0.0) if int((r or {}).get("leads") or 0) > 0 else 0.0))
                except Exception:
                    pass
                try:
                    spend_for_total_sum += float((r or {}).get("spend_for_total") or (float((r or {}).get("spend") or 0.0) if int((r or {}).get("total") or 0) > 0 else 0.0))
                except Exception:
                    pass

            cpl_total = (spend_for_total_sum / float(total_sum)) if (total_sum > 0 and spend_for_total_sum > 0) else None

            if level == "account":
                data_for_analysis = {
                    "scope": "account",
                    "account_id": aid,
                    "account_name": get_account_name(aid),
                    "requested_period_mode": mode,
                    "requested_period_label": period_human,
                    "source": "heatmap_cache",
                    "snapshot": {
                        "date": date_str,
                        "hour": int(hour_int),
                        "window": window_label,
                        "status": ds_status,
                        "reason": ds_reason,
                        "meta": ds_meta,
                    },
                    "totals": {
                        "spend": spend_sum,
                        "spend_for_msgs": spend_for_msgs_sum,
                        "spend_for_leads": spend_for_leads_sum,
                        "spend_for_total": spend_for_total_sum,
                        "msgs": msgs_sum,
                        "leads": leads_sum,
                        "total": total_sum,
                        "cpl": cpl_total,
                    },
                    "adsets": rows,
                }
            elif level == "adset":
                data_for_analysis = {
                    "scope": "adset",
                    "account_id": aid,
                    "account_name": get_account_name(aid),
                    "requested_period_mode": mode,
                    "requested_period_label": period_human,
                    "source": "heatmap_cache",
                    "snapshot": {
                        "date": date_str,
                        "hour": int(hour_int),
                        "window": window_label,
                        "status": ds_status,
                        "reason": ds_reason,
                        "meta": ds_meta,
                    },
                    "adsets": rows,
                }
            elif level == "campaign":
                by_camp = {}
                for r in rows:
                    cid = str((r or {}).get("campaign_id") or "")
                    if not cid:
                        continue
                    it = by_camp.setdefault(
                        cid,
                        {
                            "campaign_id": cid,
                            "name": (r or {}).get("campaign_name") or cid,
                            "spend": 0.0,
                            "spend_for_msgs": 0.0,
                            "spend_for_leads": 0.0,
                            "spend_for_total": 0.0,
                            "msgs": 0,
                            "leads": 0,
                            "total": 0,
                        },
                    )
                    it["spend"] = float(it.get("spend") or 0.0) + float((r or {}).get("spend") or 0.0)
                    it["spend_for_msgs"] = float(it.get("spend_for_msgs") or 0.0) + float((r or {}).get("spend_for_msgs") or (float((r or {}).get("spend") or 0.0) if int((r or {}).get("msgs") or 0) > 0 else 0.0))
                    it["spend_for_leads"] = float(it.get("spend_for_leads") or 0.0) + float((r or {}).get("spend_for_leads") or (float((r or {}).get("spend") or 0.0) if int((r or {}).get("leads") or 0) > 0 else 0.0))
                    it["spend_for_total"] = float(it.get("spend_for_total") or 0.0) + float((r or {}).get("spend_for_total") or (float((r or {}).get("spend") or 0.0) if int((r or {}).get("total") or 0) > 0 else 0.0))
                    it["msgs"] = int(it.get("msgs") or 0) + int((r or {}).get("msgs") or 0)
                    it["leads"] = int(it.get("leads") or 0) + int((r or {}).get("leads") or 0)
                    it["total"] = int(it.get("total") or 0) + int((r or {}).get("total") or 0)

                camps = list(by_camp.values())
                for c in camps:
                    sp = float(c.get("spend_for_total") or 0.0)
                    tot = int(c.get("total") or 0)
                    c["cpl"] = (sp / float(tot)) if (tot > 0 and sp > 0) else None
                camps.sort(key=lambda x: float((x or {}).get("spend") or 0.0), reverse=True)
                camps = camps[:FOCUS_AI_MAX_OBJECTS]
                data_for_analysis = {
                    "scope": "campaign",
                    "account_id": aid,
                    "account_name": get_account_name(aid),
                    "requested_period_mode": mode,
                    "requested_period_label": period_human,
                    "source": "heatmap_cache",
                    "snapshot": {
                        "date": date_str,
                        "hour": int(hour_int),
                        "window": window_label,
                        "status": ds_status,
                        "reason": ds_reason,
                        "meta": ds_meta,
                    },
                    "campaigns": camps,
                }
            elif level == "ad":
                data_for_analysis = None
            else:
                data_for_analysis = None

            user_msg = json.dumps(data_for_analysis, ensure_ascii=False) if data_for_analysis else ""

        if level == "ad":
            await safe_edit_message(
                q,
                "📊 Разовый отчёт Фокус-ИИ\n"
                f"Объект: {get_account_name(aid)}\n"
                "Уровень: Объявления\n\n"
                "Для уровня 'Объявления' сейчас нет данных в слепках (нет ad_id в heatmap cache).",
                reply_markup=focus_ai_main_kb(),
            )
            return

        if not user_msg:
            await safe_edit_message(
                q,
                "Неизвестный уровень для Фокус-ИИ.",
                reply_markup=focus_ai_main_kb(),
            )
            return

        system_msg = (
            "Ты — продвинутый аналитик по Facebook Ads (Фокус-ИИ). "
            "Отвечай ТОЛЬКО на русском языке. "
            "Сделай отчёт, который читается сканированием, без простыней. "
            "Каждый объект (кампания/адсет/объявление) — отдельным блоком. "
            "В конце — короткое итоговое резюме + конкретные рекомендации (действия), без абстракций. "
            "\n\n"
            "ЛЕГЕНДА ЭМОДЗИ (ФИКСИРОВАННАЯ, ДРУГИЕ НЕ ИСПОЛЬЗОВАТЬ):\n"
            "🟢 — хорошо / эффективно\n"
            "🟡 — нормально, но есть нюансы\n"
            "🟠 — риск / требует внимания\n"
            "🔴 — плохо / аномалия\n"
            "\n"
            "ЗАПРЕЩЕНЫ СЛОВА (не используй ни в каком виде): check_creatives, optimize, consider.\n"
            "\n"
            "ФОРМАТ report_text:\n"
            "- После разделителя '────────────────────' дай блоки по объектам.\n"
            "- Каждый блок начинается с эмодзи из легенды + название объекта.\n"
            "- Затем 1 строка метрик: Сообщения | Лиды | Всего (msgs+leads) | Расход $ | CPL.\n"
            "- Далее 2–3 коротких подпункта: 'Сильная сторона', 'Зона внимания' или 'Проблема/Риск' (по ситуации).\n"
            "- Затем строка: '👉 Что сделать' и 1 конкретное действие (например: оставить, увеличить бюджет на 20%, снизить бюджет на 20%, остановить).\n"
            "- Между объектами ставь '────────────────────'.\n"
            "- В конце: '📌 Итоговое резюме' (3–5 строк) + '📈 Рекомендация' (1–2 строки) + '🔍 Уверенность анализа: N%'.\n"
            "\n"
            "JSON-ОТВЕТ (СТРОГО один объект, без текста вокруг):\n"
            "{"
            "\"status\":\"ok\"|\"error\"," 
            "\"report_text\":\"...\"," 
            "\"recommendation\":\"increase_budget\"|\"decrease_budget\"|\"keep\"," 
            "\"suggested_change_percent\":число," 
            "\"confidence\":0-100," 
            "\"objects\":[{\"id\":\"...\",\"name\":\"...\",\"level\":\"campaign\"|\"adset\"|\"ad\",\"recommendation\":\"increase_budget\"|\"decrease_budget\"|\"keep\",\"suggested_change_percent\":число,\"confidence\":0-100}],"
            "\"budget_actions\":[{\"level\":\"adset\",\"campaign_id\":\"...\",\"adset_id\":\"...\",\"old_budget\":5.0,\"new_budget\":5.5,\"reason\":\"...\"}],"
            "\"ads_actions\":[{\"type\":\"pause_ad\"|\"notify_only\",\"campaign_id\":\"...\",\"adset_id\":\"...\",\"ad_id\":\"...\",\"reason\":\"...\",\"confidence\":0.0}]"
            "}"
        )


        content = ""
        json_ok = False
        try:
            t0 = pytime.monotonic()

            ds_resp = await asyncio.wait_for(
                ask_deepseek(
                    [
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    json_mode=True,
                    andrey_tone=True,
                ),
                timeout=FOCUS_AI_DEEPSEEK_TIMEOUT_S,
            )

            choice = (ds_resp.get("choices") or [{}])[0]
            content = (choice.get("message") or {}).get("content") or ""
            if not content.strip():
                raise ValueError("empty_deepseek_content")

            try:
                parsed = json.loads(content)
            except Exception:
                cleaned = content.strip()
                if cleaned.startswith("```"):
                    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
                    cleaned = re.sub(r"\s*```$", "", cleaned)

                try:
                    parsed = json.loads(cleaned)
                except Exception:
                    lb = cleaned.find("{")
                    rb = cleaned.rfind("}")
                    if lb >= 0 and rb > lb:
                        parsed = json.loads(cleaned[lb : rb + 1])
                    else:
                        raise

            json_ok = True

            log.info(
                "[focus_ai_now] deepseek ok elapsed=%.2fs total=%.2fs",
                pytime.monotonic() - t0,
                pytime.monotonic() - t_all,
            )
        except asyncio.TimeoutError:
            log.warning("[focus_ai_now] deepseek timeout total=%.2fs", pytime.monotonic() - t_all)
            parsed = {
                "status": "error",
                "analysis": "Фокус-ИИ не ответил вовремя. Попробуй повторить запрос позже или выбери период 7/30 дней.",
                "recommendation": "keep",
                "confidence": 0,
                "suggested_change_percent": 0,
            }
        except Exception as e:
            extracted_report_text = ""
            if content.strip():
                cleaned = content.strip()
                if cleaned.startswith("```"):
                    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
                    cleaned = re.sub(r"\s*```$", "", cleaned)

                m = re.search(
                    r"\"report_text\"\s*:\s*\"((?:\\.|[^\"\\])*)\"",
                    cleaned,
                    flags=re.DOTALL,
                )
                if m:
                    raw_val = m.group(1)
                    try:
                        extracted_report_text = json.loads('"' + raw_val + '"')
                    except Exception:
                        try:
                            extracted_report_text = bytes(raw_val, "utf-8").decode("unicode_escape")
                        except Exception:
                            extracted_report_text = (
                                raw_val.replace(r"\\n", "\n")
                                .replace(r"\\t", "\t")
                                .replace(r"\\\"", '"')
                                .replace(r"\\\\", "\\")
                            )

            # Если JSON-режим не дал ничего (часто при сетевом обрыве), пробуем короткий текстовый режим.
            if not extracted_report_text.strip() and not content.strip():
                system_msg_text = (
                    "Ты — продвинутый аналитик по Facebook Ads (Фокус-ИИ). "
                    "Отвечай ТОЛЬКО на русском языке. "
                    "Сделай отчёт, который читается сканированием, без простыней. "
                    "Каждый объект (кампания/адсет/объявление) — отдельным блоком. "
                    "Между объектами ставь разделитель '────────────────────'. "
                    "В каждом блоке: эмодзи статуса (🟢/🟡/🟠/🔴) + название, затем 1 строка метрик: Показы | Клики | Сообщения/Лиды | Расход | CPA. "
                    "Все денежные суммы указывай строго в долларах США и используй символ '$' (никаких €, EUR, рублей и т.п.). "
                    "Далее 2–3 коротких строки по делу и строка '👉 Что сделать' с 1 конкретным действием. "
                    "В конце: '📌 Итоговое резюме' (3–5 строк) + '📈 Рекомендация' (1–2 строки). "
                    "Запрещено: JSON, фигурные скобки, код, Markdown (**/__/#/`), ссылки и форматирование."
                )
                try:
                    ds_txt = await asyncio.wait_for(
                        ask_deepseek(
                            [
                                {"role": "system", "content": system_msg_text},
                                {"role": "user", "content": user_msg},
                            ],
                            json_mode=False,
                            andrey_tone=True,
                            temperature=0.4,
                            max_tokens=500,
                        ),
                        timeout=FOCUS_AI_DEEPSEEK_TIMEOUT_S,
                    )
                    ch = (ds_txt.get("choices") or [{}])[0]
                    txt = (ch.get("message") or {}).get("content") or ""
                    cleaned_txt = sanitize_ai_text(txt)
                    cleaned_txt = cleaned_txt.replace("**", "").replace("__", "")
                    cleaned_txt = (
                        cleaned_txt.replace("€", "$")
                        .replace("EUR", "$")
                        .replace("eur", "$")
                        .replace("евро", "$")
                        .replace("Евро", "$")
                    )
                    extracted_report_text = cleaned_txt
                except Exception:
                    extracted_report_text = ""

            if extracted_report_text.strip():
                parsed = {
                    "status": "ok",
                    "report_text": extracted_report_text,
                    "recommendation": "keep",
                    "confidence": 0,
                    "suggested_change_percent": 0,
                    "objects": [],
                    "budget_actions": [],
                    "ads_actions": [],
                }
                log.warning(
                    "[focus_ai_now] deepseek returned broken JSON; extracted report_text fallback (%s)",
                    type(e).__name__,
                )
            else:
                if isinstance(e, ValueError) and str(e) == "empty_deepseek_content":
                    log.warning("[focus_ai_now] deepseek empty content")
                else:
                    log.warning(
                        "[focus_ai_now] deepseek error: %s", type(e).__name__, exc_info=e
                    )
                parsed = {
                    "status": "error",
                    "analysis": "Фокус-ИИ временно недоступен. Попробуй ещё раз чуть позже.",
                    "reason": f"DeepSeek error: {e}",
                    "recommendation": "keep",
                    "confidence": 0,
                    "suggested_change_percent": 0,
                }

        status = parsed.get("status", "ok")
        report_text = parsed.get("report_text") or ""
        rec = parsed.get("recommendation") or "keep"
        conf = parsed.get("confidence") or 0
        delta = parsed.get("suggested_change_percent") or 0
        objects = parsed.get("objects") or []
        budget_actions = parsed.get("budget_actions") or []
        ads_actions = parsed.get("ads_actions") or []

        allowed_recs = {"increase_budget", "decrease_budget", "keep"}
        if rec not in allowed_recs:
            rec = "keep"
        try:
            delta = int(delta)
        except Exception:
            delta = 0
        try:
            conf = int(conf)
        except Exception:
            conf = 0

        period_label = data_for_analysis.get("requested_period_label") or period_human

        header_lines = [
            "📊 Разовый отчёт Фокус-ИИ",
            f"Объект: {get_account_name(aid)}",
            f"Уровень: {level_human}",
            f"Период: {period_label}",
            "Источник данных: heatmap cache",
            f"Окно: {date_str} {window_label}",
            "",
        ]

        if status != "ok":
            text_out = (
                "\n".join(header_lines)
                + "⚠️ Фокус-ИИ временно недоступен. Используй стандартные отчёты по аккаунту.\n"
            )
        else:
            if not report_text:
                report_text = "Фокус-ИИ вернул пустой отчёт. Попробуй повторить запрос позже."
            cleaned = sanitize_ai_text(report_text)
            if cleaned.lstrip().startswith("{"):
                cleaned = "Фокус-ИИ вернул некорректный формат ответа. Попробуй повторить запрос позже."
            if not cleaned:
                cleaned = "Фокус-ИИ вернул пустой отчёт. Попробуй повторить запрос позже."
            text_out = "\n".join(header_lines) + cleaned.strip()

        # Мы находимся внутри callback-хэндлера, поэтому update.message == None.
        # Отправляем ответ через bot.send_message в текущий чат.
        reply_markup = None
        if json_ok:
            reply_markup = focus_ai_recommendation_kb(level, rec, float(delta), objects)

        await context.bot.send_message(
            chat_id,
            text_out,
            reply_markup=reply_markup,
        )
        return

    if data.startswith("ai_bud_apply|"):
        # Формат: ai_bud_apply|{aid}|{adset_id}|{cents}
        try:
            _p, aid, adset_id, cents_s = data.split("|", 3)
            cents = int(cents_s)
        except Exception:
            await q.answer("Некорректные данные действия.", show_alert=True)
            return

        new_budget = float(cents) / 100.0
        reasons = context.user_data.get("ai_action_reasons") or {}
        reason = reasons.get(f"bud:{aid}:{adset_id}:{cents}") or ""

        with allow_fb_api_calls(reason="ai_focus_apply_budget"):
            res = set_adset_budget(adset_id, new_budget)
        if res.get("status") != "ok":
            msg = res.get("message") or ""
            await context.bot.send_message(chat_id, f"❌ Не удалось применить действие: {msg}")
            return

        old_b = res.get("old_budget")
        new_b = res.get("new_budget")
        lines = [
            "✅ Бюджет обновлён",
            "",
            f"Adset: {adset_id}",
        ]
        try:
            if old_b is not None and new_b is not None:
                lines.append(f"Было: ${float(old_b):.2f}")
                lines.append(f"Стало: ${float(new_b):.2f}")
        except Exception:
            pass
        if reason:
            lines.append(f"Причина: {reason}")

        await context.bot.send_message(chat_id, "\n".join(lines))
        return

    if data.startswith("ai_bud_manual|"):
        # Формат: ai_bud_manual|{aid}|{adset_id}
        try:
            _p, aid, adset_id = data.split("|", 2)
        except Exception:
            await q.answer("Некорректные данные.", show_alert=True)
            return

        cur = None

        context.user_data["await_ai_budget_for"] = {"aid": aid, "adset_id": adset_id}
        suffix = f" Текущий: ${cur:.2f}." if cur is not None else ""
        await context.bot.send_message(
            chat_id,
            f"Введи новый дневной бюджет для adset {adset_id} в $ (например 5.5).{suffix}",
        )
        return

    if data.startswith("ai_ad_pause|"):
        # Формат: ai_ad_pause|{aid}|{ad_id}|{adset_id}
        try:
            _p, aid, ad_id, adset_id = data.split("|", 3)
        except Exception:
            await q.answer("Некорректные данные.", show_alert=True)
            return

        reasons = context.user_data.get("ai_action_reasons") or {}
        reason = reasons.get(f"adpause:{aid}:{ad_id}:{adset_id}") or ""

        with allow_fb_api_calls(reason="ai_focus_pause_ad"):
            res = pause_ad(ad_id)
        if res.get("status") != "ok":
            msg = res.get("message") or res.get("exception") or ""
            await context.bot.send_message(chat_id, f"❌ Не удалось применить действие: {msg}")
            return

        lines = [
            "✅ Объявление отключено",
            f"Ad: {ad_id}",
            "Статус: ACTIVE → PAUSED",
        ]
        if reason:
            lines.append(f"Причина: {reason}")
        await context.bot.send_message(chat_id, "\n".join(lines))
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

    if data.startswith("cpa_ai_ads|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        alerts["ai_cpa_ads_enabled"] = not bool(alerts.get("ai_cpa_ads_enabled", False))
        row["alerts"] = alerts
        st[aid] = row
        save_accounts(st)
        text, kb = cpa_settings_kb(aid)
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

        with allow_fb_api_calls(reason="ai_focus_apply_budget"):
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

        if kind == "custom":
            context.user_data["await_rep_acc_range_for"] = {"aid": aid, "mode": mode}
            mode_human = {
                "general": "общий отчёт",
                "campaigns": "по кампаниям",
                "adsets": "по адсетам",
                "ads": "по объявлениям",
            }.get(str(mode), str(mode))
            await safe_edit_message(
                q,
                (
                    f"🗓 Отчёт по: {get_account_name(aid)}\n"
                    f"Уровень: {mode_human}\n\n"
                    "Введи даты форматом: 01.06.2025-07.06.2025"
                ),
                reply_markup=account_reports_periods_kb(aid, mode),
            )
            return

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

        if kind == "today":
            period = "today"
            label = "сегодня"
        elif kind == "yday":
            period = "yesterday"
            label = "вчера"
        elif kind == "week":
            until = datetime.now(ALMATY_TZ) - timedelta(days=1)
            since = until - timedelta(days=6)
            period = {
                "since": since.strftime("%Y-%m-%d"),
                "until": until.strftime("%Y-%m-%d"),
            }
            label = "последние 7 дней"
        else:
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
            txt = build_account_report(aid, period, "CAMPAIGN", label=label)
            await context.bot.send_message(
                chat_id,
                txt or "Нет данных/нет доступа.",
                parse_mode="HTML",
            )
            return

        if mode == "adsets":
            await safe_edit_message(
                q,
                f"Готовлю отчёт по адсетам для {name} ({label})…",
            )
            txt = build_account_report(aid, period, "ADSET", label=label)
            await context.bot.send_message(
                chat_id,
                txt or "Нет данных/нет доступа.",
                parse_mode="HTML",
            )
            return

        if mode == "ads":
            await safe_edit_message(
                q,
                f"Готовлю отчёт по объявлениям для {name} ({label})…",
            )
            txt = build_account_report(aid, period, "AD", label=label)
            await context.bot.send_message(
                chat_id,
                txt or "Нет данных/нет доступа.",
                parse_mode="HTML",
            )
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

    if data.startswith("hmdebug|"):
        aid = data.split("|", 1)[1]
        if not aid.startswith("act_"):
            aid = "act_" + aid
        txt = _build_heatmap_debug_last_text(aid=str(aid))
        await safe_edit_message(q, txt, reply_markup=main_menu())
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
        await safe_edit_message(q, "📋 Биллинги (текущие):")
        await send_billing(context, chat_id, only_inactive=False)
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
            "Выберите аккаунт для сравнения: вчера vs позавчера",
            reply_markup=monitoring_compare_accounts_kb("moncmp_yday"),
        )
        return

    if data == "mon_lastweek_vs_prevweek":
        await safe_edit_message(
            q,
            "Выберите аккаунт для сравнения: прошлая неделя vs позапрошлая",
            reply_markup=monitoring_compare_accounts_kb("moncmp_lastweek"),
        )
        return

    if data == "mon_curweek_vs_lastweek":
        await safe_edit_message(
            q,
            "Выберите аккаунт для сравнения: текущая неделя vs прошлая (по вчера)",
            reply_markup=monitoring_compare_accounts_kb("moncmp_curweek"),
        )
        return

    if data.startswith("moncmp_yday|"):
        aid = data.split("|", 1)[1]
        now = datetime.now(ALMATY_TZ)
        yday = (now - timedelta(days=1)).date()
        byday = (now - timedelta(days=2)).date()

        period_old = {"since": byday.strftime("%Y-%m-%d"), "until": byday.strftime("%Y-%m-%d")}
        period_new = {"since": yday.strftime("%Y-%m-%d"), "until": yday.strftime("%Y-%m-%d")}
        label_old = byday.strftime("%d.%m.%Y")
        label_new = yday.strftime("%d.%m.%Y")

        await safe_edit_message(
            q,
            f"Сравниваю {get_account_name(aid)}: {label_new} vs {label_old}…",
            reply_markup=monitoring_menu_kb(),
        )

        txt = build_comparison_report(aid, period_old, label_old, period_new, label_new)
        if not txt:
            await context.bot.send_message(chat_id=chat_id, text="Нет данных/нет доступа.")
            return
        await context.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")
        return

    if data.startswith("moncmp_lastweek|"):
        aid = data.split("|", 1)[1]

        # Полные недели (пн–вс): прошлая vs позапрошлая.
        now = datetime.now(ALMATY_TZ)
        start_this_week = (now - timedelta(days=now.weekday())).date()
        start_last_week = start_this_week - timedelta(days=7)
        start_prev_week = start_this_week - timedelta(days=14)
        end_last_week = start_this_week - timedelta(days=1)
        end_prev_week = start_last_week - timedelta(days=1)

        period_old = {"since": start_prev_week.strftime("%Y-%m-%d"), "until": end_prev_week.strftime("%Y-%m-%d")}
        period_new = {"since": start_last_week.strftime("%Y-%m-%d"), "until": end_last_week.strftime("%Y-%m-%d")}
        label_old = f"{start_prev_week.strftime('%d.%m')}-{end_prev_week.strftime('%d.%m')}"
        label_new = f"{start_last_week.strftime('%d.%m')}-{end_last_week.strftime('%d.%m')}"

        await safe_edit_message(
            q,
            f"Сравниваю недели {get_account_name(aid)}: {label_new} vs {label_old}…",
            reply_markup=monitoring_menu_kb(),
        )

        txt = build_comparison_report(aid, period_old, label_old, period_new, label_new)
        if not txt:
            await context.bot.send_message(chat_id=chat_id, text="Нет данных/нет доступа.")
            return
        await context.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")
        return

    if data.startswith("moncmp_curweek|"):
        aid = data.split("|", 1)[1]

        now = datetime.now(ALMATY_TZ)
        yday = (now - timedelta(days=1)).date()
        start_this_week = (now - timedelta(days=now.weekday())).date()
        start_last_week = start_this_week - timedelta(days=7)

        # Текущая неделя: пн..вчера
        # Прошлая неделя: пн..(пн+N), где N соответствует "вчера" в текущей неделе
        days_since_monday = (yday - start_this_week).days
        if days_since_monday < 0:
            days_since_monday = 0
        end_last_week = start_last_week + timedelta(days=days_since_monday)

        period_old = {"since": start_last_week.strftime("%Y-%m-%d"), "until": end_last_week.strftime("%Y-%m-%d")}
        period_new = {"since": start_this_week.strftime("%Y-%m-%d"), "until": yday.strftime("%Y-%m-%d")}
        label_old = f"{start_last_week.strftime('%d.%m')}-{end_last_week.strftime('%d.%m')}"
        label_new = f"{start_this_week.strftime('%d.%m')}-{yday.strftime('%d.%m')}"

        await safe_edit_message(
            q,
            f"Сравниваю накопление {get_account_name(aid)}: {label_new} vs {label_old}…",
            reply_markup=monitoring_menu_kb(),
        )

        txt = build_comparison_report(aid, period_old, label_old, period_new, label_new)
        if not txt:
            await context.bot.send_message(chat_id=chat_id, text="Нет данных/нет доступа.")
            return
        await context.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")
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
            "⚙️ Мониторинг — выбери аккаунты для сравнения периодов:\n\n"
            "✅ — включён\n"
            "(Если снять со всех — сравнение не будет отправлять отчёты)",
            reply_markup=monitoring_accounts_kb(),
        )
        return

    if data.startswith("mon_acc_toggle|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {}) or {}
        mon = row.get("monitoring") or {}
        if not isinstance(mon, dict):
            mon = {}

        cur = bool(mon.get("compare_enabled", True))
        mon["compare_enabled"] = not cur
        row["monitoring"] = mon
        st[aid] = row
        save_accounts(st)

        await safe_edit_message(
            q,
            "⚙️ Мониторинг — выбери аккаунты для сравнения периодов:\n\n"
            "✅ — включён\n"
            "(Если снять со всех — сравнение не будет отправлять отчёты)",
            reply_markup=monitoring_accounts_kb(),
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

    if data == "hm_hourly_menu":
        await safe_edit_message(
            q,
            "Выберите аккаунт для почасовой тепловой карты:",
            reply_markup=heatmap_hourly_accounts_kb(),
        )
        return

    if data == "mon_heatmap_menu":
        await safe_edit_message(
            q,
            "Выберите аккаунт для тепловой карты:",
            reply_markup=heatmap_monitoring_accounts_kb(),
        )
        return

    if data.startswith("mon_hm_acc|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            f"🔥 Тепловая карта — {get_account_name(aid)}\nВыберите режим:",
            reply_markup=heatmap_monitoring_modes_kb(aid),
        )
        return

    if data.startswith("mon_hmh|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            f"Выберите период для почасовой тепловой карты по {get_account_name(aid)}:",
            reply_markup=heatmap_monitoring_hourly_periods_kb(aid),
        )
        return

    if data.startswith("mon_hmh_p|"):
        _, aid, mode = data.split("|", 2)

        text_hm, summary = build_hourly_heatmap_for_account(aid, get_account_name, mode)

        await safe_edit_message(q, text_hm)

        try:
            total_convs_all = int((summary or {}).get("total_conversions_all", 0) or 0)
            total_spend_all = float((summary or {}).get("total_spend_all", 0.0) or 0.0)
            live_today = (summary or {}).get("live_today") or {}
            live_spend = float((live_today or {}).get("spend", 0.0) or 0.0)
            live_total = int((live_today or {}).get("total_conversions", 0) or 0)
        except Exception:
            total_convs_all = 0
            total_spend_all = 0.0
            live_spend = 0.0
            live_total = 0

        if total_convs_all <= 0 and total_spend_all <= 0 and live_spend <= 0 and live_total <= 0:
            return

        chat_id = str(q.message.chat.id)
        stop_event = asyncio.Event()
        typing_task = asyncio.create_task(_typing_loop(context.bot, chat_id, stop_event))

        focus_comment = None
        try:
            system_msg = (
                "Ты — продвинутый аналитик по почасовой активности рекламы. "
                "Отвечай ТОЛЬКО на русском языке. "
                "Тебе дана матрица заявок по дням и часам, а также суммарные заявки и затраты. "
                "Определи лучшие часы по заявкам, 'мёртвые' часы, различия между буднями и выходными (если есть) "
                "и предложи 2–3 практических рекомендации по бюджетам/ставкам. "
                "Отвечай кратко (до 5–7 строк обычного текста), без JSON."
            )

            summary_for_ai = dict(summary or {})
            user_msg = json.dumps(summary_for_ai, ensure_ascii=False)

            ds_resp = await ask_deepseek(
                [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                json_mode=False,
                andrey_tone=True,
            )
            choice = (ds_resp.get("choices") or [{}])[0]
            focus_comment = (choice.get("message") or {}).get("content")
        except Exception as e:
            focus_comment = (
                "Фокус-ИИ по тепловой карте сейчас недоступен (ошибка ИИ-сервиса). "
                f"Причина: {type(e).__name__}. Данные выше показаны без анализа."
            )
        finally:
            stop_event.set()
            try:
                await typing_task
            except Exception:
                pass

            return

        if focus_comment:
            await context.bot.send_message(
                chat_id,
                f"🤖 Анализ почасовой тепловой карты:\n{focus_comment.strip()}",
            )
        return

    if data.startswith("mon_hmdow|"):
        aid = data.split("|", 1)[1]
        text_dow, _summary = build_weekday_heatmap_for_account(aid, get_account_name)
        await safe_edit_message(q, text_dow)
        return

    if data.startswith("mon_hmsum|"):
        aid = data.split("|", 1)[1]
        text_sum, summary = build_heatmap_monitoring_summary(aid, get_account_name)
        await safe_edit_message(q, text_sum)

        chat_id = str(q.message.chat.id)
        stop_event = asyncio.Event()
        typing_task = asyncio.create_task(
            _typing_loop(context.bot, chat_id, stop_event)
        )

        focus_comment = None
        try:
            system_msg = (
                "Ты — продвинутый аналитик по недельной и почасовой активности рекламы. "
                "Отвечай ТОЛЬКО на русском языке. "
                "Тебе дана сводка по дням недели и по часам (агрегаты заявок и затрат). "
                "Сформулируй рекомендации: какие дни недели и часы усиливать, какие можно отключать/снижать, "
                "и как перераспределить бюджеты в течение недели. "
                "Отвечай кратко (до 7–10 строк обычного текста), без JSON."
            )

            user_msg = json.dumps(summary or {}, ensure_ascii=False)
            ds_resp = await ask_deepseek(
                [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                json_mode=False,
                andrey_tone=True,
            )
            choice = (ds_resp.get("choices") or [{}])[0]
            focus_comment = (choice.get("message") or {}).get("content")
        except Exception as e:
            focus_comment = (
                "Фокус-ИИ по тепловой карте сейчас недоступен (ошибка ИИ-сервиса). "
                f"Причина: {type(e).__name__}. Данные выше показаны без анализа."
            )
        finally:
            stop_event.set()
            try:
                await typing_task
            except Exception:
                pass

        if focus_comment:
            await context.bot.send_message(
                chat_id,
                f"🤖 Рекомендации по тепловой карте:\n{focus_comment.strip()}",
            )
        return

    if data.startswith("hmh_acc|"):
        aid = data.split("|", 1)[1]
        await safe_edit_message(
            q,
            f"Выберите период для почасовой тепловой карты по {get_account_name(aid)}:",
            reply_markup=heatmap_hourly_periods_kb(aid),
        )
        return

    if data.startswith("hmh_p|"):
        _, aid, mode = data.split("|", 2)

        text_hm, summary = build_hourly_heatmap_for_account(aid, get_account_name, mode)

        await safe_edit_message(q, text_hm)

        try:
            total_convs_all = int((summary or {}).get("total_conversions_all", 0) or 0)
            total_spend_all = float((summary or {}).get("total_spend_all", 0.0) or 0.0)
            live_today = (summary or {}).get("live_today") or {}
            live_spend = float((live_today or {}).get("spend", 0.0) or 0.0)
            live_total = int((live_today or {}).get("total_conversions", 0) or 0)
        except Exception:
            total_convs_all = 0
            total_spend_all = 0.0
            live_spend = 0.0
            live_total = 0

        if total_convs_all <= 0 and total_spend_all <= 0 and live_spend <= 0 and live_total <= 0:
            return

        # ИИ-анализ почасовой карты с анимацией "бот печатает"
        chat_id = str(q.message.chat.id)
        stop_event = asyncio.Event()
        typing_task = asyncio.create_task(
            _typing_loop(context.bot, chat_id, stop_event)
        )

        focus_comment = None
        try:
            system_msg = (
                "Ты — продвинутый аналитик по почасовой активности рекламы. "
                "Отвечай ТОЛЬКО на русском языке. "
                "Тебе дана матрица заявок по дням и часам, а также суммарные заявки и затраты. "
                "Определи лучшие часы по заявкам, 'мёртвые' часы, различия между буднями и выходными (если есть) "
                "и предложи 2–3 практических рекомендации по бюджетам/ставкам. "
                "Отвечай кратко (до 5–7 строк обычного текста), без JSON."
            )

            summary_for_ai = dict(summary or {})
            try:
                raw = json.dumps(summary_for_ai, ensure_ascii=False)
                if len(raw) > 30000:
                    days = summary_for_ai.get("days") or []
                    summary_for_ai["days"] = days[-3:]
                    raw = json.dumps(summary_for_ai, ensure_ascii=False)
            except Exception:
                raw = json.dumps(summary, ensure_ascii=False)

            user_msg = raw

            ds_resp = await ask_deepseek(
                [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                json_mode=False,
                andrey_tone=True,
            )

            choice = (ds_resp.get("choices") or [{}])[0]
            focus_comment = (choice.get("message") or {}).get("content")
        except Exception as e:
            # Явно помечаем, что ИИ-анализ недоступен, чтобы пользователь видел причину.
            focus_comment = (
                "Фокус-ИИ по тепловой карте сейчас недоступен (ошибка ИИ-сервиса). "
                f"Причина: {type(e).__name__}. Данные выше показаны без анализа."
            )
        finally:
            stop_event.set()
            try:
                await typing_task
            except Exception:
                pass

        if focus_comment:
            await context.bot.send_message(
                chat_id,
                f"🤖 Анализ почасовой тепловой карты:\n{focus_comment.strip()}",
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

    if data.startswith("lead_metric|"):
        aid = data.split("|", 1)[1]
        sel = get_lead_metric_for_account(aid)
        if sel:
            current = f"✅ {sel.get('label') or sel.get('action_type')}"
        else:
            current = "Стандартная (по умолчанию)"

        _items, src, age_s = _lead_metric_get_catalog_cached(aid, force_refresh=False)
        src_str = str(src)
        age_h = _lead_metric_human_cache_age(age_s)

        text = (
            f"📊 Метрика лидов — {get_account_name(aid)}\n\n"
            f"Текущая метрика: {current}\n\n"
            "Если метрика не выбрана, бот считает лиды по стандартным action_type.\n\n"
            f"Источник списка конверсий: {src_str} (age={age_h})"
        )

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Сменить",
                        callback_data=f"lead_metric_choose|{aid}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Сбросить (стандартная)",
                        callback_data=f"lead_metric_clear|{aid}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🔄 Обновить список",
                        callback_data=f"lead_metric_refresh|{aid}",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "Показать action_type (debug)",
                        callback_data=f"lead_metric_debug|{aid}",
                    )
                ],
                [InlineKeyboardButton("⬅️ Назад", callback_data=f"set1|{aid}")],
            ]
        )
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("lead_metric_clear|"):
        aid = data.split("|", 1)[1]
        clear_lead_metric_for_account(aid)
        await q.answer("Метрика лидов сброшена.")
        new_data = f"lead_metric|{aid}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
        return

    if data.startswith("lead_metric_debug|"):
        aid = data.split("|", 1)[1]
        sel = get_lead_metric_for_account(aid)
        items, src, age_s = _lead_metric_get_catalog_cached(aid, force_refresh=False)
        lines = [f"lead_metric debug — {get_account_name(aid)}", ""]
        if sel:
            lines.append(f"selected_action_type={str(sel.get('action_type') or '')}")
        else:
            lines.append("selected_action_type=(default)")
        lines.append(f"catalog_source={str(src)}")
        lines.append(f"catalog_age={_lead_metric_human_cache_age(age_s)}")
        lines.append(f"catalog_items={int(len(items or []))}")
        if items:
            lines.append("")
            lines.append("first_items:")
            for it in list(items or [])[:20]:
                at = str((it or {}).get("action_type") or "").strip()
                if at:
                    lines.append(f"- {at}")
        text = "\n".join(lines)
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data=f"lead_metric|{aid}")]]
        )
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("lead_metric_choose|"):
        aid = data.split("|", 1)[1]
        items, src, age_s = _lead_metric_get_catalog_cached(aid, force_refresh=False)
        context.user_data["lead_metric_catalog"] = {
            "aid": str(aid),
            "items": list(items or []),
            "query": "",
            "page": 0,
            "source": str(src),
            "age_s": age_s,
        }
        text, kb = _lead_metric_choose_page(aid=str(aid), items=list(items or []), page=0, query="")
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("lead_metric_refresh|"):
        aid = data.split("|", 1)[1]
        items, src, age_s = _lead_metric_get_catalog_cached(aid, force_refresh=True)
        context.user_data["lead_metric_catalog"] = {
            "aid": str(aid),
            "items": list(items or []),
            "query": "",
            "page": 0,
            "source": str(src),
            "age_s": age_s,
        }
        try:
            await q.answer("Список обновлён.")
        except Exception:
            pass
        text, kb = _lead_metric_choose_page(aid=str(aid), items=list(items or []), page=0, query="")
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("lead_metric_page|"):
        try:
            _p, aid, page_s = data.split("|", 2)
        except ValueError:
            return
        stash = context.user_data.get("lead_metric_catalog") or {}
        if str(stash.get("aid") or "") != str(aid):
            await q.answer("Список устарел. Нажми 'Сменить' ещё раз.", show_alert=True)
            return
        items = list(stash.get("items") or [])
        query = str(stash.get("query") or "")
        try:
            page_i = int(page_s)
        except Exception:
            page_i = 0
        stash["page"] = page_i
        context.user_data["lead_metric_catalog"] = stash
        text, kb = _lead_metric_choose_page(aid=str(aid), items=items, page=page_i, query=query)
        await safe_edit_message(q, text, reply_markup=kb)
        return

    if data.startswith("lead_metric_search|"):
        aid = data.split("|", 1)[1]
        context.user_data["await_lead_metric_search_for"] = {"aid": str(aid)}
        await safe_edit_message(q, "Напиши текст для поиска по label/action_type (например lead или submit).")
        return

    if data.startswith("lead_metric_set2|"):
        try:
            _p, aid, idx_s = data.split("|", 2)
        except ValueError:
            await q.answer("Некорректные данные выбора метрики.", show_alert=True)
            return
        stash = context.user_data.get("lead_metric_catalog") or {}
        if str(stash.get("aid") or "") != str(aid):
            await q.answer("Список устарел. Нажми 'Сменить' ещё раз.", show_alert=True)
            return
        items = list(stash.get("items") or [])
        query = str(stash.get("query") or "")
        filtered = []
        qlow = query.strip().lower()
        for it in (items or []):
            at = str((it or {}).get("action_type") or "").strip()
            label = str((it or {}).get("label") or at).strip()
            if not at:
                continue
            if qlow and (qlow not in at.lower()) and (qlow not in label.lower()):
                continue
            filtered.append({"action_type": at, "label": label})
        try:
            idx = int(idx_s)
        except Exception:
            idx = -1
        if idx < 0 or idx >= len(filtered):
            await q.answer("Метрика не найдена. Обнови список.", show_alert=True)
            return
        it = filtered[idx]
        action_type = it.get("action_type")
        label = it.get("label")
        if not action_type:
            await q.answer("Пустой action_type.", show_alert=True)
            return
        set_lead_metric_for_account(aid, action_type=str(action_type), label=str(label or action_type))
        await q.answer("Метрика лидов обновлена.")
        await context.bot.send_message(chat_id, "Метрика лидов обновлена. Все отчёты и ИИ теперь считают по ней.")
        new_data = f"lead_metric|{aid}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
        return

    if data.startswith("lead_metric_set|"):
        try:
            _p, aid, idx = data.split("|", 2)
        except ValueError:
            await q.answer("Некорректные данные выбора метрики.", show_alert=True)
            return

        stash = context.user_data.get("lead_metric_options") or {}
        if stash.get("aid") != aid:
            await q.answer("Список метрик устарел. Нажми 'Сменить' ещё раз.", show_alert=True)
            return

        items = stash.get("items") or {}
        it = items.get(str(idx))
        if not it:
            await q.answer("Метрика не найдена. Нажми 'Сменить' ещё раз.", show_alert=True)
            return

        action_type = it.get("action_type")
        label = it.get("label")
        if not action_type:
            await q.answer("Пустой action_type.", show_alert=True)
            return

        set_lead_metric_for_account(aid, action_type=str(action_type), label=str(label or action_type))
        await q.answer("Метрика лидов обновлена.")
        await context.bot.send_message(
            chat_id,
            "Метрика лидов обновлена. Все отчёты и ИИ теперь считают по ней.",
        )
        new_data = f"lead_metric|{aid}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
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

        kb_rows = []
        for camp in _campaign_options_from_snapshots(aid):
            cid = (camp or {}).get("id")
            if not cid:
                continue
            name = (camp or {}).get("name") or cid
            cfg_c = (campaign_alerts.get(cid) or {}) if cid in campaign_alerts else {}
            target = float(cfg_c.get("target_cpa") or 0.0)
            label_suffix = (
                f"[CPA {target:.2f}$]" if target > 0 else "[CPA аккаунта]"
            )
            enabled_c = bool(cfg_c.get("enabled", False))
            # В списке кампаний: один индикатор ⚠️, если CPA-алёрт для кампании включён.
            indicator = "⚠️ " if enabled_c else ""
            text_btn = f"{indicator}{name} {label_suffix}".strip()

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
        await safe_edit_message(
            q,
            text,
            reply_markup=InlineKeyboardMarkup(kb_rows),
        )
        return

    if data.startswith("cpa_campaign|"):
        _, aid, campaign_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        campaign_alerts = alerts.setdefault("campaign_alerts", {})
        cfg = campaign_alerts.get(campaign_id) or {}

        camp_name = _campaign_name_from_snapshots(aid, campaign_id) or campaign_id

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

        await safe_edit_message(
            q,
            text,
            reply_markup=kb,
        )
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

        # После переключения статуса возвращаемся к списку кампаний
        new_data = f"cpa_campaigns|{aid}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
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

        new_data = f"cpa_campaign|{aid}|{campaign_id}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
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

        text = "Выбери адсет для настройки CPA-алёртов."
        await safe_edit_message(q, text, reply_markup=cpa_adsets_kb(aid))
        return

    if data.startswith("cpa_adset|"):
        _, aid, adset_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        adset_alerts = alerts.setdefault("adset_alerts", {})
        cfg = adset_alerts.get(adset_id) or {}

        adset_name = _adset_name_from_snapshots(aid, adset_id) or adset_id

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

        await safe_edit_message(
            q,
            text,
            reply_markup=kb,
        )
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
        new_data = f"cpa_adset|{aid}|{adset_id}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
        return

    if data.startswith("cpa_ads|"):
        aid = data.split("|", 1)[1]

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.get("ad_alerts", {}) or {}

        # No ad-level rows in snapshots.
        ads: list[dict] = []
        ad_status: dict[str, str] = {}
        ad_to_adset: dict[str, str] = {}
        allowed_adset_ids: set[str] = set()

        kb_rows = []
        for ad in ads:
            ad_id = ad.get("ad_id") or ad.get("id")
            if not ad_id:
                continue

            spend = float(ad.get("spend", 0.0) or 0.0)
            if ad_id not in ad_alerts and spend <= 0:
                continue

            status = ad_status.get(str(ad_id), "")
            adset_id = str(ad.get("adset_id") or ad_to_adset.get(str(ad_id)) or "")
            adset_active = adset_id in allowed_adset_ids

            # В списке объявлений показываем только активные креативы с активным адсетом.
            if str(status or "").upper() not in {"ACTIVE", "SCHEDULED"} or not adset_active:
                continue

            name = ad.get("name") or ad_id
            cfg = ad_alerts.get(ad_id) or {}
            enabled_ad = bool(cfg.get("enabled", False))
            target = float(cfg.get("target_cpa") or 0.0)
            label_suffix = (
                f"[CPA {target:.2f}$]" if target > 0 else "[CPA вышестоящего уровня]"
            )
            # Индикатор ⚠️ только если объявление активно, его адсет активен и алёрт включён.
            indicator = "⚠️ " if enabled_ad else ""
            text_btn = f"{indicator}{name} {label_suffix}".strip()

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
        await safe_edit_message(
            q,
            text,
            reply_markup=InlineKeyboardMarkup(kb_rows),
        )
        return

    if data.startswith("cpa_ad_cfg|"):
        _, aid, ad_id = data.split("|", 2)

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.setdefault("ad_alerts", {})
        cfg = ad_alerts.get(ad_id) or {}

        ad_name = ad_id

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

        await safe_edit_message(
            q,
            text,
            reply_markup=kb,
        )
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

        # После переключения статуса возвращаемся к списку объявлений
        new_data = f"cpa_ads|{aid}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
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

        new_data = f"cpa_adset|{aid}|{adset_id}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
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

    if "await_lead_metric_search_for" in context.user_data:
        payload = context.user_data.pop("await_lead_metric_search_for") or {}
        aid = str(payload.get("aid") or "")
        if not aid:
            await update.message.reply_text("Не удалось применить поиск: контекст аккаунта потерян.")
            return
        query = str(text or "").strip()
        stash = context.user_data.get("lead_metric_catalog") or {}
        if str(stash.get("aid") or "") != str(aid):
            items, src, age_s = _lead_metric_get_catalog_cached(aid, force_refresh=False)
            stash = {
                "aid": str(aid),
                "items": list(items or []),
                "query": "",
                "page": 0,
                "source": str(src),
                "age_s": age_s,
            }
        stash["query"] = query
        stash["page"] = 0
        context.user_data["lead_metric_catalog"] = stash

        try:
            text_list, kb = _lead_metric_choose_page(
                aid=str(aid),
                items=list(stash.get("items") or []),
                page=0,
                query=query,
            )
            await update.message.reply_text(text_list, reply_markup=kb)
        except Exception:
            await update.message.reply_text("Поиск применён. Открой 'Сменить' ещё раз, чтобы увидеть список.")
        return

    if "await_ap_group_rename" in context.user_data:
        payload = context.user_data.pop("await_ap_group_rename") or {}
        aid = payload.get("aid")
        gid = payload.get("gid")

        name = str(text or "").strip()
        if not name:
            await update.message.reply_text("Введите название текстом.")
            context.user_data["await_ap_group_rename"] = payload
            return

        if len(name) > 48:
            await update.message.reply_text("Слишком длинное название (до 48 символов).")
            context.user_data["await_ap_group_rename"] = payload
            return

        if not aid or not gid:
            await update.message.reply_text("Не удалось сохранить: контекст группы потерян.")
            return

        grp = _autopilot_group_get(aid, gid)
        if not grp:
            await update.message.reply_text("Группа не найдена. Открой 'Группы кампаний' ещё раз.")
            return

        grp["name"] = name
        _autopilot_group_set(aid, gid, grp)
        await update.message.reply_text("✅ Название группы сохранено")
        await update.message.reply_text(
            _autopilot_group_menu_text(aid, gid),
            reply_markup=_autopilot_group_kb(aid, gid),
        )
        return

    if "await_ap_action_edit" in context.user_data:
        payload = context.user_data.pop("await_ap_action_edit") or {}
        token = payload.get("token")
        chat_id = payload.get("chat_id")
        msg_id = payload.get("message_id")

        pct = parse_manual_input(text)
        if pct is None:
            await update.message.reply_text("Введите число процента, например -10 или 15")
            context.user_data["await_ap_action_edit"] = payload
            return

        pending = context.bot_data.get("ap_pending_actions") or {}
        act = pending.get(token)
        if not act:
            await update.message.reply_text("Действие устарело. Сгенерируй заново.")
            return

        if str(act.get("kind") or "") != "budget_pct":
            await update.message.reply_text("Это действие не поддерживает изменение процента.")
            return

        act["percent"] = float(pct)
        pending[token] = act

        aid = str(act.get("aid") or "")
        append_autopilot_event(
            aid,
            {
                "type": "action_edit",
                "token": str(token),
                "kind": "budget_pct",
                "percent": float(pct),
                "chat_id": str(chat_id or ""),
            },
        )

        try:
            kb = _ap_action_kb(allow_apply=bool(act.get("allow_apply")), token=str(token), allow_edit=True)
            if chat_id and msg_id:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=int(msg_id),
                    text=_ap_action_text(act),
                    reply_markup=kb,
                )
        except Exception:
            pass

        await update.message.reply_text("✅ Процент обновлён")
        return

    if "await_ap_leads_for" in context.user_data:
        payload = context.user_data.pop("await_ap_leads_for") or {}
        aid = payload.get("aid")
        gid = payload.get("gid")
        try:
            val = int(float(text.replace(",", ".")))
        except Exception:
            await update.message.reply_text("Введите число, например 20 (или 0 чтобы сбросить)")
            context.user_data["await_ap_leads_for"] = payload
            return

        if gid:
            grp = _autopilot_group_get(aid, gid)
            goals = grp.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
            goals["leads"] = None if val <= 0 else int(val)
            grp["goals"] = goals
            _autopilot_group_set(aid, gid, grp)
        else:
            ap = _autopilot_get(aid)
            goals = ap.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
            goals["leads"] = None if val <= 0 else int(val)
            _autopilot_set(aid, {"goals": goals})
        append_autopilot_event(
            aid,
            {"type": "goal_set", "key": "leads", "value": goals.get("leads")},
        )
        await update.message.reply_text("✅ Цель по лидам обновлена")
        return

    if "await_ap_cpl_for" in context.user_data:
        payload = context.user_data.pop("await_ap_cpl_for") or {}
        aid = payload.get("aid")
        gid = payload.get("gid")
        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text("Введите число в $ (например 1.2) или 0 чтобы сбросить")
            context.user_data["await_ap_cpl_for"] = payload
            return

        if gid:
            grp = _autopilot_group_get(aid, gid)
            goals = grp.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
            goals["target_cpl"] = None if val <= 0 else float(val)
            grp["goals"] = goals
            _autopilot_group_set(aid, gid, grp)
        else:
            ap = _autopilot_get(aid)
            goals = ap.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
            goals["target_cpl"] = None if val <= 0 else float(val)
            _autopilot_set(aid, {"goals": goals})
        append_autopilot_event(
            aid,
            {"type": "goal_set", "key": "target_cpl", "value": goals.get("target_cpl")},
        )
        await update.message.reply_text("✅ Целевой CPL обновлён")
        return

    if "await_ap_budget_for" in context.user_data:
        payload = context.user_data.pop("await_ap_budget_for") or {}
        aid = payload.get("aid")
        gid = payload.get("gid")
        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text("Введите число в $ (например 30) или 0 чтобы сбросить")
            context.user_data["await_ap_budget_for"] = payload
            return

        if gid:
            grp = _autopilot_group_get(aid, gid)
            goals = grp.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
            goals["planned_budget"] = None if val <= 0 else float(val)
            grp["goals"] = goals
            _autopilot_group_set(aid, gid, grp)
        else:
            ap = _autopilot_get(aid)
            goals = ap.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
            goals["planned_budget"] = None if val <= 0 else float(val)
            _autopilot_set(aid, {"goals": goals})
        append_autopilot_event(
            aid,
            {"type": "goal_set", "key": "planned_budget", "value": goals.get("planned_budget")},
        )
        await update.message.reply_text("✅ Плановый бюджет обновлён")
        return

    if "await_ap_until_for" in context.user_data:
        payload = context.user_data.pop("await_ap_until_for") or {}
        aid = payload.get("aid")
        gid = payload.get("gid")

        try:
            dt = datetime.strptime(text.strip(), "%d.%m.%Y").date()
        except Exception:
            await update.message.reply_text("Формат даты: ДД.ММ.ГГГГ (например 25.01.2026). Попробуй ещё раз.")
            context.user_data["await_ap_until_for"] = payload
            return

        if gid:
            grp = _autopilot_group_get(aid, gid)
            goals = grp.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
        else:
            ap = _autopilot_get(aid)
            goals = ap.get("goals") or {}
            if not isinstance(goals, dict):
                goals = {}
        goals["period"] = "until"
        goals["until"] = dt.strftime("%d.%m.%Y")

        if gid:
            grp["goals"] = goals
            _autopilot_group_set(aid, gid, grp)
        else:
            _autopilot_set(aid, {"goals": goals})
        append_autopilot_event(
            aid,
            {"type": "goal_set", "key": "until", "value": goals.get("until")},
        )
        await update.message.reply_text("✅ Период 'до даты' сохранён")
        return

    if "await_ai_budget_for" in context.user_data:
        payload = context.user_data.pop("await_ai_budget_for")
        aid = payload.get("aid")
        adset_id = payload.get("adset_id")

        if not aid or not adset_id:
            await update.message.reply_text(
                "❌ Не удалось применить действие: не найден контекст adset.")
            return

        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text(
                "Введите число в $ (например 5.5). Попробуй ещё раз.")
            context.user_data["await_ai_budget_for"] = payload
            return

        with allow_fb_api_calls(reason="ai_focus_apply_budget"):
            res = set_adset_budget(str(adset_id), float(val))
        if res.get("status") != "ok":
            msg = res.get("message") or ""
            await update.message.reply_text(f"❌ Не удалось применить действие: {msg}")
            return

        old_b = res.get("old_budget")
        new_b = res.get("new_budget")
        lines = [
            "✅ Бюджет обновлён",
            "",
            f"Adset: {adset_id}",
        ]
        try:
            if old_b is not None and new_b is not None:
                lines.append(f"Было: ${float(old_b):.2f}")
                lines.append(f"Стало: ${float(new_b):.2f}")
        except Exception:
            pass
        await update.message.reply_text("\n".join(lines))
        return

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

    # Кастомный диапазон для отчёта по одному аккаунту на выбранном уровне (rep_acc_p|...|custom)
    if context.user_data.get("await_rep_acc_range_for"):
        payload = context.user_data.pop("await_rep_acc_range_for", None) or {}
        aid = str(payload.get("aid") or "")
        mode = str(payload.get("mode") or "general")
        parsed = parse_range(text)
        if not parsed:
            await update.message.reply_text(
                "Формат дат: 01.06.2025-07.06.2025. Попробуй ещё раз."
            )
            context.user_data["await_rep_acc_range_for"] = payload
            return

        period, label = parsed
        name = get_account_name(aid) if aid else ""
        if not aid:
            await update.message.reply_text("❌ Не удалось построить отчёт: не найден аккаунт.")
            return

        if mode == "general":
            await update.message.reply_text(f"Готовлю отчёт по {name} за {label}…")
            txt = get_cached_report(aid, period, label)
            await update.message.reply_text(
                txt or "Нет данных/нет доступа.",
                parse_mode="HTML",
            )
            return

        if mode == "campaigns":
            await update.message.reply_text(f"Готовлю отчёт по кампаниям для {name} ({label})…")
            txt = build_account_report(aid, period, "CAMPAIGN", label=label)
            await update.message.reply_text(
                txt or "Нет данных/нет доступа.",
                parse_mode="HTML",
            )
            return

        if mode == "adsets":
            await update.message.reply_text(f"Готовлю отчёт по адсетам для {name} ({label})…")
            txt = build_account_report(aid, period, "ADSET", label=label)
            await update.message.reply_text(
                txt or "Нет данных/нет доступа.",
                parse_mode="HTML",
            )
            return

        if mode == "ads":
            await update.message.reply_text(f"Готовлю отчёт по объявлениям для {name} ({label})…")
            txt = build_account_report(aid, period, "AD", label=label)
            await update.message.reply_text(
                txt or "Нет данных/нет доступа.",
                parse_mode="HTML",
            )
            return

        await update.message.reply_text("❌ Не удалось построить отчёт: неизвестный режим.")
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

        # После ввода CPA сразу возвращаемся к списку кампаний.
        await update.message.reply_text(
            "✅ CPA для кампании обновлён.", reply_markup=cpa_campaigns_kb(aid)
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

        # После ввода CPA сразу возвращаемся к списку адсетов.
        await update.message.reply_text(
            "✅ CPA для адсета обновлён.", reply_markup=cpa_adsets_kb(aid)
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

        # После ввода CPA сразу возвращаемся к списку объявлений.
        await update.message.reply_text(
            "✅ CPA для объявления обновлён.", reply_markup=cpa_ads_kb(aid)
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
    builder = Application.builder().token(TELEGRAM_TOKEN)

    # Настройка таймаутов getUpdates через ApplicationBuilder (PTB>=20.7).
    # Это заменяет deprecated-параметры connect_timeout/read_timeout/write_timeout/pool_timeout в run_polling.
    try:
        builder = (
            builder.get_updates_connect_timeout(20)
            .get_updates_read_timeout(45)
            .get_updates_write_timeout(30)
            .get_updates_pool_timeout(30)
        )
    except (AttributeError, TypeError) as e:
        logging.getLogger(__name__).warning(
            "PTB ApplicationBuilder.get_updates_*_timeout is not available (%s). "
            "Upgrade python-telegram-bot to remove run_polling timeout deprecation warning.",
            type(e).__name__,
        )

    app = builder.build()

    async def _on_error(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        err = context.error
        if isinstance(err, (NetworkError, TimedOut, RetryAfter)):
            logging.getLogger(__name__).warning(
                "Telegram transient error: %s: %s",
                type(err).__name__,
                err,
            )
            return

        logging.getLogger(__name__).exception(
            "Unhandled error while processing update",
            exc_info=err,
        )

    app.add_error_handler(_on_error)

    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("version", cmd_version))
    app.add_handler(CommandHandler("ap_here", cmd_ap_here))
    app.add_handler(CommandHandler("billing", cmd_billing))
    app.add_handler(CommandHandler("billing_debug", cmd_billing_debug))
    app.add_handler(CommandHandler("sync_accounts", cmd_sync))
    app.add_handler(CommandHandler("heatmap", cmd_heatmap))
    app.add_handler(CommandHandler("heatmap_status", cmd_heatmap_status))
    app.add_handler(CommandHandler("heatmap_debug_last", cmd_heatmap_debug_last))
    app.add_handler(CommandHandler("heatmap_debug_hour", cmd_heatmap_debug_hour))
    app.add_handler(CommandHandler("report_debug", cmd_report_debug))

    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_any))

    app.job_queue.run_daily(
        billing_digest_job,
        time=time(hour=9, minute=45, tzinfo=ALMATY_TZ),
    )

    schedule_cpa_alerts(app)

    try:
        _resolve_autopilot_chat_id_logged(reason="scheduler_startup")
    except Exception:
        pass

    # Stagger hourly jobs to reduce FB burst: hourly autopilot starts at :05 each hour.
    try:
        now = datetime.now(ALMATY_TZ)
        first_at = now.replace(minute=5, second=0, microsecond=0)
        if first_at <= now:
            first_at = first_at + timedelta(hours=1)
    except Exception:
        first_at = timedelta(minutes=5)
    app.job_queue.run_repeating(
        _autopilot_hourly_job,
        interval=timedelta(hours=1),
        first=first_at,
    )

    app.job_queue.run_once(
        _autopilot_warmup_job,
        when=timedelta(minutes=10),
        name="autopilot_warmup",
    )

    init_billing_watch(
        app,
        get_enabled_accounts=get_enabled_accounts_in_order,
        get_account_name=get_account_name,
        usd_to_kzt=usd_to_kzt,
        kzt_round_up_1000=kzt_round_up_1000,
        owner_id=253181449,
        group_chat_id=str(DEFAULT_REPORT_CHAT),
    )

    logging.getLogger(__name__).info("🟢 Scheduler started, jobs registered")

    return app
