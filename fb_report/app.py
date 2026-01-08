from datetime import datetime, timedelta, time
import calendar

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

from billing_watch import init_billing_watch
from autopilat.actions import apply_budget_change, set_adset_budget, disable_entity, can_disable, parse_manual_input
from history_store import append_autopilot_event, read_autopilot_events

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
    get_lead_metric_for_account,
    set_lead_metric_for_account,
    clear_lead_metric_for_account,
)
from .reporting import (
    fmt_int,
    get_cached_report,
    build_comparison_report,
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
from .jobs import full_daily_scan_job, daily_report_job, schedule_cpa_alerts, _resolve_account_cpa

from services.analytics import analyze_campaigns, analyze_adsets, analyze_account, analyze_ads
from services.facebook_api import pause_ad, fetch_adsets, fetch_ads, fetch_insights, fetch_campaigns
from services.ai_focus import get_focus_comment, ask_deepseek, sanitize_ai_text
from fb_report.cpa_monitoring import build_anomaly_messages_for_account
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


def _autopilot_analysis_kb(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 Обновить", callback_data=f"ap_analyze|{aid}")],
            [InlineKeyboardButton("🛠 Предложить действия", callback_data=f"ap_suggest|{aid}")],
            [InlineKeyboardButton("🕒 Часы (heatmap)", callback_data=f"ap_hm|{aid}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data=f"autopilot_acc|{aid}")],
            [InlineKeyboardButton("⬅️ К аккаунтам", callback_data="autopilot_menu")],
            [InlineKeyboardButton("⬅️ В меню", callback_data="menu")],
        ]
    )


def _autopilot_hm_kb(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Сегодня", callback_data=f"ap_hm_p|{aid}|today"),
                InlineKeyboardButton("Вчера", callback_data=f"ap_hm_p|{aid}|yday"),
            ],
            [InlineKeyboardButton("7 дней", callback_data=f"ap_hm_p|{aid}|7d")],
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
    kind = str(action.get("kind") or "")
    name = action.get("name") or action.get("adset_id")
    reason = action.get("reason") or ""
    sp_t = action.get("spend_today")
    ld_t = action.get("leads_today")
    cpl_t = action.get("cpl_today")
    cpl_3 = action.get("cpl_3d")

    def _fmt_money(v):
        if v is None:
            return "—"
        try:
            return f"{float(v):.2f} $"
        except Exception:
            return "—"

    def _fmt_int(v):
        try:
            return str(int(float(v)))
        except Exception:
            return "0"

    lines = [f"🧭 Автопилат: действие для adset", f"{name}", f"ID: {action.get('adset_id')}", ""]
    lines.append(f"Сегодня: spend {_fmt_money(sp_t)} | leads {_fmt_int(ld_t)} | CPL {_fmt_money(cpl_t)}")
    lines.append(f"Rolling 3d: CPL {_fmt_money(cpl_3)}")
    lines.append("")

    if kind == "budget_pct":
        pct = action.get("percent")
        try:
            pct_f = float(pct)
        except Exception:
            pct_f = 0.0
        sign = "+" if pct_f >= 0 else ""
        lines.append(f"👉 Предложение: изменить бюджет на {sign}{pct_f:.0f}%")
    elif kind == "pause_adset":
        lines.append("👉 Предложение: остановить adset")
    elif kind == "pause_ad":
        ad_name = action.get("ad_name") or action.get("ad_id")
        lines.append(f"👉 Предложение: отключить объявление ({ad_name})")
    elif kind == "note":
        lines.append("ℹ️ Рекомендация без кнопки применения")
    else:
        lines.append("👉 Предложение: (неизвестно)")

    if reason:
        lines.append(f"Причина: {reason}")

    return "\n".join(lines)


def _ap_generate_actions(aid: str) -> list[dict]:
    ap = _autopilot_get(aid)
    mode = str(ap.get("mode") or "OFF").upper()
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

    now = datetime.now(ALMATY_TZ)
    yday = (now - timedelta(days=1)).date()
    period_3d = {
        "since": (yday - timedelta(days=2)).strftime("%Y-%m-%d"),
        "until": yday.strftime("%Y-%m-%d"),
    }

    try:
        today_rows = analyze_adsets(aid, period="today") or []
    except Exception:
        today_rows = []

    try:
        d3_rows = analyze_adsets(aid, period=period_3d) or []
    except Exception:
        d3_rows = []

    try:
        today_ads = analyze_ads(aid, period="today") or []
    except Exception:
        today_ads = []

    ads_by_adset: dict[str, list[dict]] = {}
    for a in (today_ads or []):
        adset_id = str((a or {}).get("adset_id") or "")
        if not adset_id:
            continue
        st = str((a or {}).get("effective_status") or (a or {}).get("status") or "").upper()
        if st != "ACTIVE":
            continue
        if float((a or {}).get("spend", 0.0) or 0.0) <= 0:
            continue
        ads_by_adset.setdefault(adset_id, []).append(a)

    def _allowed_row(r: dict) -> bool:
        st = str((r or {}).get("effective_status") or (r or {}).get("status") or "").upper()
        return st in {"ACTIVE", "SCHEDULED"}

    today_map = {str(r.get("adset_id")): r for r in (today_rows or []) if r.get("adset_id") and _allowed_row(r)}
    d3_map = {str(r.get("adset_id")): r for r in (d3_rows or []) if r.get("adset_id") and _allowed_row(r)}

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

    target_cpl = (ap.get("goals") or {}).get("target_cpl")
    try:
        target_cpl_f = float(target_cpl) if target_cpl not in (None, "") else None
    except Exception:
        target_cpl_f = None
    if target_cpl_f is not None and target_cpl_f <= 0:
        target_cpl_f = None

    keys = sorted(set(today_map.keys()) | set(d3_map.keys()))
    rows = []
    for k in keys:
        t = today_map.get(k) or {}
        d = d3_map.get(k) or {}
        name = t.get("name") or d.get("name") or k

        sp_t = _to_float(t.get("spend"))
        ld_t = _to_int(t.get("leads"))
        cpl_t = _cpl(sp_t, ld_t)

        sp_3 = _to_float(d.get("spend"))
        ld_3 = _to_int(d.get("leads"))
        cpl_3 = _cpl(sp_3, ld_3)

        if sp_t <= 0:
            continue

        if ld_t <= 0:
            # В v1 pausing adset разрешаем ТОЛЬКО отдельным флагом.
            if allow_pause_adsets and can_disable(aid, k):
                rows.append(
                    {
                        "kind": "pause_adset",
                        "adset_id": k,
                        "name": name,
                        "spend_today": sp_t,
                        "leads_today": ld_t,
                        "cpl_today": cpl_t,
                        "cpl_3d": cpl_3,
                        "reason": "Сегодня есть расход, но нет лидов.",
                        "score": sp_t,
                    }
                )
                continue

            # Основной сценарий v1: предлагаем отключить объявление внутри adset.
            # Кнопка только если >1 активного объявления.
            try:
                active_cnt = _count_active_ads_in_adset(aid, k)
            except Exception:
                active_cnt = 0

            if allow_pause_ads and active_cnt > 1:
                cands = ads_by_adset.get(str(k)) or []
                cands.sort(key=lambda x: float((x or {}).get("spend", 0.0) or 0.0), reverse=True)
                cand = cands[0] if cands else None
                ad_id = str((cand or {}).get("ad_id") or "")
                ad_name = (cand or {}).get("name") if cand else None
                if ad_id:
                    rows.append(
                        {
                            "kind": "pause_ad",
                            "ad_id": ad_id,
                            "ad_name": ad_name,
                            "adset_id": k,
                            "name": name,
                            "spend_today": sp_t,
                            "leads_today": ld_t,
                            "cpl_today": cpl_t,
                            "cpl_3d": cpl_3,
                            "reason": "Сегодня есть расход, но нет лидов. В adset >1 активного объявления.",
                            "score": sp_t,
                        }
                    )
                    continue

            # Если объявление одно — только рекомендация (без кнопки отключения).
            rows.append(
                {
                    "kind": "note",
                    "adset_id": k,
                    "name": name,
                    "spend_today": sp_t,
                    "leads_today": ld_t,
                    "cpl_today": cpl_t,
                    "cpl_3d": cpl_3,
                    "reason": "Сегодня есть расход, но нет лидов. В adset единственное активное объявление — стоит заменить/отключить вручную.",
                    "score": sp_t,
                }
            )
            continue

        if cpl_t is None:
            continue

        if target_cpl_f is not None and target_cpl_f > 0:
            ratio = float(cpl_t) / float(target_cpl_f)
        elif cpl_3 is not None and float(cpl_3) > 0:
            ratio = float(cpl_t) / float(cpl_3)
        else:
            ratio = None

        if ratio is None:
            continue

        if ratio <= 1.05:
            rows.append(
                {
                    "kind": "budget_pct",
                    "adset_id": k,
                    "name": name,
                    "percent": +max_step,
                    "spend_today": sp_t,
                    "leads_today": ld_t,
                    "cpl_today": cpl_t,
                    "cpl_3d": cpl_3,
                    "reason": "CPL в норме/лучше бенчмарка.",
                    "score": sp_t,
                }
            )
        elif ratio >= 1.30:
            rows.append(
                {
                    "kind": "budget_pct",
                    "adset_id": k,
                    "name": name,
                    "percent": -max_step,
                    "spend_today": sp_t,
                    "leads_today": ld_t,
                    "cpl_today": cpl_t,
                    "cpl_3d": cpl_3,
                    "reason": "CPL хуже бенчмарка.",
                    "score": sp_t,
                }
            )

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
    try:
        ins = fetch_insights(aid, "today") or {}
    except Exception:
        ins = {}
    try:
        return float((ins or {}).get("spend", 0) or 0)
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
    yday = (now - timedelta(days=1)).date()
    period_3d = {
        "since": (yday - timedelta(days=2)).strftime("%Y-%m-%d"),
        "until": yday.strftime("%Y-%m-%d"),
    }

    try:
        today_rows = analyze_adsets(aid, period="today") or []
    except Exception:
        today_rows = []

    try:
        d3_rows = analyze_adsets(aid, period=period_3d) or []
    except Exception:
        d3_rows = []

    def _allowed_row(r: dict) -> bool:
        st = str((r or {}).get("effective_status") or (r or {}).get("status") or "").upper()
        return st in {"ACTIVE", "SCHEDULED"}

    today_map = {str(r.get("adset_id")): r for r in (today_rows or []) if r.get("adset_id") and _allowed_row(r)}
    d3_map = {str(r.get("adset_id")): r for r in (d3_rows or []) if r.get("adset_id") and _allowed_row(r)}

    ap = _autopilot_get(aid)
    goals = ap.get("goals") or {}
    target_cpl = goals.get("target_cpl")
    try:
        target_cpl_f = float(target_cpl) if target_cpl not in (None, "") else None
    except Exception:
        target_cpl_f = None
    if target_cpl_f is not None and target_cpl_f <= 0:
        target_cpl_f = None

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

    def _fmt_money(v):
        if v is None:
            return "—"
        try:
            return f"{float(v):.2f} $"
        except Exception:
            return "—"

    def _cpl(spend: float, leads: int):
        if leads <= 0:
            return None
        if spend <= 0:
            return 0.0
        return float(spend) / float(leads)

    def _status(sp_t: float, ld_t: int, cpl_t, cpl_3):
        # Пустая статистика сегодня
        if (sp_t or 0.0) <= 0:
            return "🟡"
        if ld_t <= 0:
            return "🔴"

        if target_cpl_f is not None and cpl_t is not None:
            ratio = float(cpl_t) / float(target_cpl_f) if target_cpl_f > 0 else 999
        elif cpl_3 is not None and cpl_t is not None and float(cpl_3) > 0:
            ratio = float(cpl_t) / float(cpl_3)
        else:
            return "🟡"

        if ratio <= 1.05:
            return "🟢"
        if ratio <= 1.30:
            return "🟡"
        if ratio <= 1.70:
            return "🟠"
        return "🔴"

    keys = sorted(set(today_map.keys()) | set(d3_map.keys()))
    merged = []
    for k in keys:
        t = today_map.get(k) or {}
        d = d3_map.get(k) or {}
        name = t.get("name") or d.get("name") or k

        sp_t = _to_float(t.get("spend"))
        ld_t = _to_int(t.get("leads"))
        cpl_t = _cpl(sp_t, ld_t)

        sp_3 = _to_float(d.get("spend"))
        ld_3 = _to_int(d.get("leads"))
        cpl_3 = _cpl(sp_3, ld_3)

        emoji = _status(sp_t, ld_t, cpl_t, cpl_3)
        merged.append(
            {
                "id": k,
                "name": str(name),
                "emoji": emoji,
                "sp_t": sp_t,
                "ld_t": ld_t,
                "cpl_t": cpl_t,
                "sp_3": sp_3,
                "ld_3": ld_3,
                "cpl_3": cpl_3,
            }
        )

    merged.sort(key=lambda x: float(x.get("sp_t") or 0.0), reverse=True)

    sum_sp_t = sum(float(x.get("sp_t") or 0.0) for x in merged)
    sum_ld_t = sum(int(x.get("ld_t") or 0) for x in merged)
    sum_cpl_t = _cpl(sum_sp_t, sum_ld_t)

    sum_sp_3 = sum(float(x.get("sp_3") or 0.0) for x in merged)
    sum_ld_3 = sum(int(x.get("ld_3") or 0) for x in merged)
    sum_cpl_3 = _cpl(sum_sp_3, sum_ld_3)

    lines = [
        f"📊 Автопилат — анализ adset: {get_account_name(aid)}",
        "",
        f"Сегодня: spend {_fmt_money(sum_sp_t)} | leads {sum_ld_t} | CPL {_fmt_money(sum_cpl_t)}",
        f"Rolling 3d (до вчера): spend {_fmt_money(sum_sp_3)} | leads {sum_ld_3} | CPL {_fmt_money(sum_cpl_3)}",
    ]
    if target_cpl_f is not None:
        lines.append(f"Целевой CPL: {_fmt_money(target_cpl_f)}")

    lines.append("")
    if not merged:
        lines.append("Нет данных по ACTIVE/SCHEDULED адсетам.")
        return "\n".join(lines)

    lines.append("Топ adset по spend сегодня:")
    for x in merged[:12]:
        lines.extend(
            [
                f"{x['emoji']} {x['name']}",
                f"• today: spend {_fmt_money(x['sp_t'])} | leads {x['ld_t']} | CPL {_fmt_money(x['cpl_t'])}",
                f"• 3d: spend {_fmt_money(x['sp_3'])} | leads {x['ld_3']} | CPL {_fmt_money(x['cpl_3'])}",
                "",
            ]
        )

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


def _autopilot_get(aid: str) -> dict:
    st = load_accounts().get(str(aid), {})
    ap = st.get("autopilot") or {}
    return ap if isinstance(ap, dict) else {}


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
    goals = ap.get("goals") or {}
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

    lines = [
        f"🤖 Автопилат — {get_account_name(aid)}",
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
        f"• Pause ads: {'✅' if allow_pause_ads else '❌'}",
        f"• Pause adsets: {'✅' if allow_pause_adsets else '❌'}",
        f"• Перераспределение: {'✅' if allow_redist else '❌'}",
        f"• Re-enable ads: {'✅' if allow_reenable else '❌'}",
    ]
    return "\n".join(lines)


def _autopilot_kb(aid: str) -> InlineKeyboardMarkup:
    ap = _autopilot_get(aid)
    mode = str(ap.get("mode") or "OFF").upper()
    limits = ap.get("limits") or {}
    allow_reenable = bool(limits.get("allow_reenable_ads", False))
    allow_pause_adsets = bool(limits.get("allow_pause_adsets", False))

    rows = [
        [
            InlineKeyboardButton(
                ("✅ Советник" if mode == "ADVISOR" else "🧠 Советник"),
                callback_data=f"ap_mode|{aid}|ADVISOR",
            ),
        ],
        [
            InlineKeyboardButton(
                ("✅ Полуавто" if mode == "SEMI" else "🟡 Полуавто"),
                callback_data=f"ap_mode|{aid}|SEMI",
            ),
        ],
        [
            InlineKeyboardButton(
                ("✅ Авто с лимитами" if mode == "AUTO_LIMITS" else "🤖 Авто с лимитами"),
                callback_data=f"ap_mode|{aid}|AUTO_LIMITS",
            ),
        ],
        [
            InlineKeyboardButton(
                ("✅ Выключен" if mode == "OFF" else "🔴 Выключить"),
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
        [
            InlineKeyboardButton(
                ("🔁 Re-enable ads: ON" if allow_reenable else "🔁 Re-enable ads: OFF"),
                callback_data=f"ap_toggle_reenable|{aid}",
            ),
        ],
        [
            InlineKeyboardButton(
                ("🧩 Pause adsets: ON" if allow_pause_adsets else "🧩 Pause adsets: OFF"),
                callback_data=f"ap_toggle_pause_adsets|{aid}",
            )
        ],
        [InlineKeyboardButton("📊 Анализ (today vs 3d)", callback_data=f"ap_analyze|{aid}")],
        [InlineKeyboardButton("🕒 Часы (heatmap)", callback_data=f"ap_hm|{aid}")],
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
    period = {
        "since": yday.strftime("%Y-%m-%d"),
        "until": yday.strftime("%Y-%m-%d"),
    }
    ins = fetch_insights(aid, period) or {}
    actions = (ins or {}).get("actions") or []

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
            suffix = at.split(".")[-1]
            if not suffix.isdigit():
                continue
            try:
                from facebook_business.adobjects.customconversion import CustomConversion

                name = CustomConversion(suffix).api_get(fields=["name"]).get("name")
            except Exception:
                name = None

            if not _is_site_lead_custom_conversion_name(name or ""):
                continue

            label = f"Заявка с сайта — {name}" if name else "Заявка с сайта"
            out.append({"action_type": at, "label": label})
            continue

        if at not in whitelist_exact_mixed and at_lower not in whitelist_exact_lower:
            continue

        out.append({"action_type": at, "label": _lead_metric_label_for_action_type(at)})

    out.sort(key=lambda x: (x.get("label") or x.get("action_type") or ""))
    return out


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
    out = {}
    for row in fetch_adsets(aid) or []:
        adset_id = row.get("id")
        if not adset_id:
            continue
        out[str(adset_id)] = row
    return out


def _get_ads_map(aid: str) -> dict:
    out = {}
    for row in fetch_ads(aid) or []:
        ad_id = row.get("id")
        if not ad_id:
            continue
        out[str(ad_id)] = row
    return out


def _count_active_ads_in_adset(aid: str, adset_id: str) -> int:
    cnt = 0
    for row in fetch_ads(aid) or []:
        if str(row.get("adset_id") or "") != str(adset_id):
            continue
        if str(row.get("status") or "").upper() == "ACTIVE":
            cnt += 1
    return int(cnt)


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
    for aid in get_enabled_accounts_in_order():
        if not store.get(aid, {}).get("enabled", True):
            continue
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

    try:
        fb_campaigns = fetch_campaigns(aid) or []
    except Exception:
        fb_campaigns = []

    allowed_campaign_ids = {
        str(r.get("id"))
        for r in fb_campaigns
        if str((r or {}).get("effective_status") or (r or {}).get("status") or "").upper()
        in {"ACTIVE", "SCHEDULED"}
        and r.get("id")
    }

    try:
        camps = analyze_campaigns(aid, days=7) or []
    except Exception:
        camps = []

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

    from .adsets import list_adsets_for_account

    adsets = list_adsets_for_account(aid)

    try:
        fb_adsets = fetch_adsets(aid) or []
    except Exception:
        fb_adsets = []

    active_adset_ids = {
        str(r.get("id"))
        for r in fb_adsets
        if str((r or {}).get("effective_status") or (r or {}).get("status") or "").upper()
        in {"ACTIVE", "SCHEDULED"}
        and r.get("id")
    }

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

    try:
        ads = analyze_ads(aid, days=7) or []
    except Exception:
        ads = []

    try:
        fb_ads = fetch_ads(aid) or []
    except Exception:
        fb_ads = []

    ad_status: dict[str, str] = {}
    ad_to_adset: dict[str, str] = {}
    for r in fb_ads:
        ad_id_raw = str(r.get("id") or "")
        if not ad_id_raw:
            continue
        ad_status[ad_id_raw] = r.get("status") or ""
        ad_to_adset[ad_id_raw] = str(r.get("adset_id") or "")

    try:
        fb_adsets = fetch_adsets(aid) or []
    except Exception:
        fb_adsets = []

    active_adset_ids = {
        str(r.get("id"))
        for r in fb_adsets
        if (r or {}).get("status") == "ACTIVE" and r.get("id")
    }

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

    await _on_cb_internal(update, context, q, chat_id, data)


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
            "Выберите кабинет для Автопилата:",
            reply_markup=accounts_kb("autopilot_acc"),
        )
        return

    if data.startswith("autopilot_acc|"):
        aid = data.split("|", 1)[1]
        text = _autopilot_dashboard_text(aid)
        await safe_edit_message(q, text, reply_markup=_autopilot_kb(aid))
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
        context.user_data["await_ap_leads_for"] = {"aid": aid}
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
        context.user_data["await_ap_cpl_for"] = {"aid": aid}
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
        context.user_data["await_ap_budget_for"] = {"aid": aid}
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
        goals = ap.get("goals") or {}
        if not isinstance(goals, dict):
            goals = {}

        code = str(code or "day")
        goals["period"] = code
        if code != "until":
            goals["until"] = None

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
            context.user_data["await_ap_until_for"] = {"aid": aid}
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

    if data.startswith("aphmforce|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(str(aid), {})
        ap = (row or {}).get("autopilot") or {}
        if not isinstance(ap, dict):
            ap = {}

        mode = str(ap.get("mode") or "OFF").upper()
        if mode != "AUTO_LIMITS":
            await q.answer("Force доступен только в AUTO_LIMITS.", show_alert=True)
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

        ap = _autopilot_get(aid)
        mode = str(ap.get("mode") or "OFF").upper()
        actions = _ap_generate_actions(aid) or []
        append_autopilot_event(
            aid,
            {
                "type": "actions_generated",
                "count": int(len(actions)),
                "chat_id": str(chat_id),
            },
        )

        if not actions:
            await safe_edit_message(
                q,
                "Нет подходящих действий по текущим данным.\n\n"
                "Подсказка: проверь, что есть spend сегодня и что adset ACTIVE/SCHEDULED.",
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
                await context.bot.send_message(chat_id, _ap_action_text(act), reply_markup=kb)
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
                            chat_id,
                            "🤖 AUTO_LIMITS: автоприменено\n\n" + str(res.get("message") or "") + "\n\n" + _ap_action_text(act),
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
                                chat_id,
                                "🤖 AUTO_LIMITS: автоприменено\n\n" + str(res.get("message") or res.get("exception") or "") + "\n\n" + _ap_action_text(act),
                            )
                            auto_applied += 1
                            continue

                    if kind == "pause_adset":
                        # В AUTO_LIMITS всё равно только если явно включено allow_pause_adsets (генератор уже отфильтровал).
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
                            chat_id,
                            "🤖 AUTO_LIMITS: автоприменено\n\n" + str(res.get("message") or "") + "\n\n" + _ap_action_text(act),
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
                await context.bot.send_message(chat_id, _ap_action_text(act) + f"\n\n⚠️ Вне лимитов AUTO_LIMITS: {why}", reply_markup=kb)
                continue

            # SEMI / OFF: SEMI — подтверждение вручную; OFF — по факту тоже не должен предлагать, но оставим безопасно.
            token = uuid.uuid4().hex[:10]
            act["token"] = token
            pending[token] = act

            kind = str(act.get("kind") or "")
            allow_edit = kind == "budget_pct"
            kb = _ap_action_kb(allow_apply=bool(act.get("allow_apply")), token=token, allow_edit=allow_edit)
            await context.bot.send_message(chat_id, _ap_action_text(act), reply_markup=kb)

        await safe_edit_message(
            q,
            f"Отправил действий: {len(actions)}\n"
            + (f"Автоприменено: {auto_applied}\n" if auto_applied else "")
            + "Каждое действие отдельным сообщением ниже.",
            reply_markup=_autopilot_analysis_kb(aid),
        )
        return

    if data.startswith("apdo|"):
        parts = data.split("|", 2)
        if len(parts) < 3:
            await q.answer("Некорректная кнопка.", show_alert=True)
            return
        _p, op, token = parts
        orig_op = op

        pending = context.bot_data.get("ap_pending_actions") or {}
        act = pending.get(token)
        if not act:
            await q.answer("Действие устарело. Сгенерируй заново.", show_alert=True)
            return

        aid = str(act.get("aid") or "")
        kind = str(act.get("kind") or "")

        if op == "force":
            ap = _autopilot_get(aid)
            mode = str(ap.get("mode") or "OFF").upper()
            if mode != "AUTO_LIMITS":
                await q.answer("Force подтверждение доступно только в AUTO_LIMITS.", show_alert=True)
                return

            op = "apply"

        allow_apply = bool(act.get("allow_apply"))
        if not allow_apply and op in {"apply", "edit"}:
            await q.answer("Режим Советник: применение отключено.", show_alert=True)
            return

        if op == "cancel":
            append_autopilot_event(
                aid,
                {"type": "action_cancel", "token": token, "kind": kind, "chat_id": str(chat_id)},
            )
            pending.pop(token, None)
            await safe_edit_message(q, "❌ Отменено\n\n" + _ap_action_text(act))
            return

        if op == "ack":
            append_autopilot_event(
                aid,
                {"type": "action_ack", "token": token, "kind": kind, "chat_id": str(chat_id)},
            )
            pending.pop(token, None)
            await safe_edit_message(q, "✅ Ок\n\n" + _ap_action_text(act))
            return

        if op == "edit":
            if kind != "budget_pct":
                await q.answer("Для этого действия редактирование не поддерживается.", show_alert=True)
                return

            context.user_data["await_ap_action_edit"] = {
                "token": token,
                "chat_id": str(chat_id),
                "message_id": int(getattr(q.message, "message_id", 0) or 0),
            }
            await safe_edit_message(
                q,
                _ap_action_text(act)
                + "\n\n✍️ Введите новый процент изменения бюджета (например -10 или 15):",
            )
            return

        if op == "apply":
            adset_id = str(act.get("adset_id") or "")
            if not adset_id:
                await q.answer("Нет adset_id.", show_alert=True)
                return

            ap = _autopilot_get(aid)
            mode = str(ap.get("mode") or "OFF").upper()
            if mode == "ADVISOR":
                await q.answer("Режим Советник: применение отключено.", show_alert=True)
                return

            if kind == "budget_pct":
                pct = act.get("percent")
                try:
                    pct_f = float(pct)
                except Exception:
                    pct_f = 0.0

                if mode == "AUTO_LIMITS":
                    ok, why = _ap_within_limits_for_auto(aid, act)
                    if not ok and orig_op != "force":
                        append_autopilot_event(
                            aid,
                            {"type": "action_over_limit", "token": token, "kind": kind, "why": why, "chat_id": str(chat_id)},
                        )
                        await safe_edit_message(
                            q,
                            _ap_action_text(act) + f"\n\n⚠️ Выходит за лимиты AUTO_LIMITS: {why}\nПодтвердить сверх лимитов?",
                            reply_markup=_ap_force_kb(token),
                        )
                        return

                res = apply_budget_change(adset_id, pct_f)
                append_autopilot_event(
                    aid,
                    {
                        "type": "action_apply",
                        "token": token,
                        "kind": kind,
                        "adset_id": adset_id,
                        "percent": pct_f,
                        "status": res.get("status"),
                        "message": res.get("message"),
                        "chat_id": str(chat_id),
                    },
                )
                pending.pop(token, None)
                await safe_edit_message(q, "✅ Применено\n\n" + (res.get("message") or "") + "\n\n" + _ap_action_text(act))
                return

            if kind == "pause_ad":
                ad_id = str(act.get("ad_id") or "")
                if not ad_id:
                    await q.answer("Нет ad_id.", show_alert=True)
                    return

                try:
                    active_cnt = _count_active_ads_in_adset(aid, adset_id)
                except Exception:
                    active_cnt = 0

                if active_cnt <= 1:
                    await safe_edit_message(
                        q,
                        "❌ Нельзя отключить объявление — оно единственное активное в adset.\n\n" + _ap_action_text(act),
                    )
                    return

                res = pause_ad(ad_id)
                append_autopilot_event(
                    aid,
                    {
                        "type": "action_apply",
                        "token": token,
                        "kind": kind,
                        "adset_id": adset_id,
                        "ad_id": ad_id,
                        "status": res.get("status"),
                        "message": res.get("message") or res.get("exception"),
                        "chat_id": str(chat_id),
                    },
                )

                pending.pop(token, None)
                if res.get("status") != "ok":
                    await safe_edit_message(
                        q,
                        "⚠️ Не удалось применить\n\n" + str(res.get("message") or res.get("exception") or "") + "\n\n" + _ap_action_text(act),
                    )
                    return

                await safe_edit_message(
                    q,
                    "✅ Применено\n\n" + str(res.get("message") or "") + "\n\n" + _ap_action_text(act),
                )
                return

            if kind == "pause_adset":
                if not can_disable(aid, adset_id):
                    await safe_edit_message(
                        q,
                        "❌ Нельзя остановить adset — иначе аккаунт останется без активных adset.\n\n" + _ap_action_text(act),
                    )
                    return

                res = disable_entity(adset_id)
                append_autopilot_event(
                    aid,
                    {
                        "type": "action_apply",
                        "token": token,
                        "kind": kind,
                        "adset_id": adset_id,
                        "status": res.get("status"),
                        "message": res.get("message"),
                        "chat_id": str(chat_id),
                    },
                )
                pending.pop(token, None)
                await safe_edit_message(q, "✅ Применено\n\n" + (res.get("message") or "") + "\n\n" + _ap_action_text(act))
                return

            await q.answer("Неизвестный тип действия.", show_alert=True)
            return

    if data.startswith("ap_history|"):
        aid = data.split("|", 1)[1]
        events = read_autopilot_events(aid, limit=20) or []
        lines = [f"🧾 История Автопилата — {get_account_name(aid)}", ""]
        if not events:
            lines.append("(пока пусто)")
        else:
            for ev in events:
                ts = (ev or {}).get("ts")
                t = (ev or {}).get("type")
                if t == "mode_change":
                    lines.append(f"{ts}: mode {ev.get('from')} → {ev.get('to')}")
                elif t == "goal_set":
                    lines.append(f"{ts}: goal {ev.get('key')} = {ev.get('value')}")
                elif t == "period_set":
                    lines.append(f"{ts}: period = {ev.get('period')}")
                elif t == "toggle":
                    lines.append(f"{ts}: {ev.get('key')} = {ev.get('value')}")
                else:
                    lines.append(f"{ts}: {t}")

        await safe_edit_message(q, "\n".join(lines), reply_markup=_autopilot_kb(aid))
        return

    if data.startswith("mr_menu|"):
        aid = data.split("|", 1)[1]
        st = load_accounts()
        row = st.get(aid, {})
        mr = row.get("morning_report") or {}
        level = str(mr.get("level", "ACCOUNT")).upper()

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🏦 Аккаунт",
                        callback_data=f"mr_level|{aid}|ACCOUNT",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "📣 Кампании",
                        callback_data=f"mr_level|{aid}|CAMPAIGN",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🧩 Адсеты",
                        callback_data=f"mr_level|{aid}|ADSET",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Назад",
                        callback_data=f"set1|{aid}",
                    )
                ],
            ]
        )

        await safe_edit_message(
            q,
            "Выберите уровень утреннего отчёта:",
            reply_markup=kb,
        )
        return

    if data.startswith("mr_level|"):
        try:
            _prefix, aid, lvl = data.split("|", 2)
        except ValueError:
            await q.answer("Некорректные данные уровня утреннего отчёта.", show_alert=True)
            return

        lvl = str(lvl).upper()
        if lvl == "OFF":
            await q.answer(
                "Недоступно. Используй 'Выключить кабинет'.",
                show_alert=True,
            )
            return

        if lvl not in {"ACCOUNT", "CAMPAIGN", "ADSET"}:
            await q.answer("Неизвестный уровень утреннего отчёта.", show_alert=True)
            return

        st = load_accounts()
        row = st.get(aid, {})
        mr = row.get("morning_report") or {}
        mr["level"] = lvl
        row["morning_report"] = mr
        st[aid] = row
        save_accounts(st)

        human = {
            "ACCOUNT": "Аккаунт",
            "CAMPAIGN": "Кампании",
            "ADSET": "Адсеты",
        }.get(lvl, "Аккаунт")

        await q.answer(f"Уровень утреннего отчёта: {human}")
        await safe_edit_message(
            q,
            f"Настройки: {get_account_name(aid)}",
            reply_markup=settings_kb(aid),
        )
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

        paused = context.application.bot_data.setdefault("cpa_ai_paused", set())
        key = f"{aid}:{ad_id}"
        if key in paused:
            await q.answer("Уже отключено.", show_alert=False)
            return

        # Пытаемся определить adset_id для safety-check
        adset_id = None
        try:
            ads_map = _get_ads_map(aid)
            adset_id = (ads_map.get(str(ad_id)) or {}).get("adset_id")
        except Exception:
            adset_id = None

        if adset_id:
            try:
                active_cnt = _count_active_ads_in_adset(aid, str(adset_id))
            except Exception:
                active_cnt = 0
            if active_cnt <= 1:
                await q.answer(
                    "Нельзя отключить: единственное активное объявление в adset.",
                    show_alert=True,
                )
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

        log = logging.getLogger(__name__)
        t_all = pytime.monotonic()
        log.info(
            "[focus_ai_now] start aid=%s level=%s mode=%s",
            aid,
            level,
            mode,
        )

        # Собираем данные по выбранному уровню и периоду.
        from services.analytics import _make_period_for_mode  # локальный импорт, чтобы избежать циклов

        # Для custom пока используем fallback 7 дней, но передаём маркер в контекст.
        mode_for_period = mode if mode in {"today", "yday", "7d", "30d"} else "7d"
        period_dict = _make_period_for_mode(mode_for_period)

        if level == "account":
            try:
                t0 = pytime.monotonic()
                base_analysis = await asyncio.wait_for(
                    asyncio.to_thread(analyze_account, aid, period=period_dict),
                    timeout=FOCUS_AI_DATA_TIMEOUT_S,
                )
                log.info(
                    "[focus_ai_now] analyze_account ok elapsed=%.2fs",
                    pytime.monotonic() - t0,
                )
            except asyncio.TimeoutError:
                log.warning("[focus_ai_now] analyze_account timeout")
                await safe_edit_message(
                    q,
                    "⚠️ Фокус-ИИ: сбор данных занял слишком много времени. "
                    "Попробуй период '7 дней' или повтори запрос позже.",
                    reply_markup=focus_ai_main_kb(),
                )
                return

            # Теплокарта может быть тяжёлой — тоже под таймаут.
            try:
                t0 = pytime.monotonic()
                heat = await asyncio.wait_for(
                    asyncio.to_thread(build_heatmap_for_account, aid, get_account_name, mode="7"),
                    timeout=FOCUS_AI_DATA_TIMEOUT_S,
                )
                log.info(
                    "[focus_ai_now] build_heatmap_for_account ok elapsed=%.2fs",
                    pytime.monotonic() - t0,
                )
            except asyncio.TimeoutError:
                log.warning("[focus_ai_now] build_heatmap_for_account timeout")
                heat = {}

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
            try:
                t0 = pytime.monotonic()
                camps = await asyncio.wait_for(
                    asyncio.to_thread(analyze_campaigns, aid, period=period_dict),
                    timeout=FOCUS_AI_DATA_TIMEOUT_S,
                )
                log.info(
                    "[focus_ai_now] analyze_campaigns ok elapsed=%.2fs count=%s",
                    pytime.monotonic() - t0,
                    len(camps or []),
                )
            except asyncio.TimeoutError:
                log.warning("[focus_ai_now] analyze_campaigns timeout")
                await safe_edit_message(
                    q,
                    "⚠️ Фокус-ИИ: сбор данных по кампаниям занял слишком много времени. "
                    "Попробуй период '7 дней' или повтори запрос позже.",
                    reply_markup=focus_ai_main_kb(),
                )
                return

            camps = (camps or [])[:FOCUS_AI_MAX_OBJECTS]
            data_for_analysis = {
                "scope": "campaign",
                "account_id": aid,
                "account_name": get_account_name(aid),
                "period_mode": mode,
                "period_label": period_human,
                "period": period_dict,
                "campaigns": camps,
                "truncated": True if (camps and len(camps) >= FOCUS_AI_MAX_OBJECTS) else False,
            }
        elif level == "adset":
            try:
                t0 = pytime.monotonic()
                adsets = await asyncio.wait_for(
                    asyncio.to_thread(analyze_adsets, aid, period=period_dict),
                    timeout=FOCUS_AI_DATA_TIMEOUT_S,
                )
                log.info(
                    "[focus_ai_now] analyze_adsets ok elapsed=%.2fs count=%s",
                    pytime.monotonic() - t0,
                    len(adsets or []),
                )
            except asyncio.TimeoutError:
                log.warning("[focus_ai_now] analyze_adsets timeout")
                await safe_edit_message(
                    q,
                    "⚠️ Фокус-ИИ: сбор данных по адсетам занял слишком много времени. "
                    "Попробуй период '7 дней' или повтори запрос позже.",
                    reply_markup=focus_ai_main_kb(),
                )
                return

            adsets = (adsets or [])[:FOCUS_AI_MAX_OBJECTS]
            data_for_analysis = {
                "scope": "adset",
                "account_id": aid,
                "account_name": get_account_name(aid),
                "period_mode": mode,
                "period_label": period_human,
                "period": period_dict,
                "adsets": adsets,
                "truncated": True if (adsets and len(adsets) >= FOCUS_AI_MAX_OBJECTS) else False,
            }
        elif level == "ad":
            try:
                t0 = pytime.monotonic()
                ads = await asyncio.wait_for(
                    asyncio.to_thread(analyze_ads, aid, period=period_dict),
                    timeout=FOCUS_AI_DATA_TIMEOUT_S,
                )
                log.info(
                    "[focus_ai_now] analyze_ads ok elapsed=%.2fs count=%s",
                    pytime.monotonic() - t0,
                    len(ads or []),
                )
            except asyncio.TimeoutError:
                log.warning("[focus_ai_now] analyze_ads timeout")
                await safe_edit_message(
                    q,
                    "⚠️ Фокус-ИИ: сбор данных по объявлениям занял слишком много времени. "
                    "Попробуй период '7 дней' или повтори запрос позже.",
                    reply_markup=focus_ai_main_kb(),
                )
                return

            ads = (ads or [])[:FOCUS_AI_MAX_OBJECTS]
            data_for_analysis = {
                "scope": "ad",
                "account_id": aid,
                "account_name": get_account_name(aid),
                "period_mode": mode,
                "period_label": period_human,
                "period": period_dict,
                "ads": ads,
                "truncated": True if (ads and len(ads) >= FOCUS_AI_MAX_OBJECTS) else False,
            }
        else:
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
            "- Затем 1 строка метрик: Показы | Клики | Сообщения/Лиды (что есть) | Расход | CPA.\n"
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

        user_msg = json.dumps(data_for_analysis, ensure_ascii=False)

        try:
            t0 = pytime.monotonic()
            ds_resp = await asyncio.wait_for(
                ask_deepseek(
                    [
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    json_mode=True,
                ),
                timeout=FOCUS_AI_DEEPSEEK_TIMEOUT_S,
            )
            log.info(
                "[focus_ai_now] deepseek ok elapsed=%.2fs total=%.2fs",
                pytime.monotonic() - t0,
                pytime.monotonic() - t_all,
            )

            choice = (ds_resp.get("choices") or [{}])[0]
            content = (choice.get("message") or {}).get("content") or ""
            parsed = json.loads(content)
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
            log.exception("[focus_ai_now] deepseek error: %s", type(e).__name__)
            parsed = {
                "status": "error",
                "analysis": "Фокус-ИИ временно недоступен. Используй стандартные отчёты по аккаунту.",
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

        period_label = data_for_analysis.get("period_label") or period_human

        header_lines = [
            "📊 Разовый отчёт Фокус-ИИ",
            f"Объект: {get_account_name(aid)}",
            f"Уровень: {level_human}",
            f"Период: {period_label}",
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
            if not cleaned:
                cleaned = "Фокус-ИИ вернул пустой отчёт. Попробуй повторить запрос позже."
            text_out = "\n".join(header_lines) + cleaned.strip()

        # Мы находимся внутри callback-хэндлера, поэтому update.message == None.
        # Отправляем ответ через bot.send_message в текущий чат.
        await context.bot.send_message(
            chat_id,
            text_out,
            reply_markup=focus_ai_recommendation_kb(level, rec, float(delta), objects),
        )

        # ====== Управляемые действия (кнопки) ======
        reasons = context.user_data.get("ai_action_reasons")
        if not isinstance(reasons, dict):
            reasons = {}
        context.user_data["ai_action_reasons"] = reasons

        try:
            adsets_map = _get_adset_budget_map(aid)
        except Exception:
            adsets_map = {}

        # Бюджеты: всегда применяем на уровне adset.
        if isinstance(budget_actions, list):
            for act in budget_actions:
                if not isinstance(act, dict):
                    continue
                if str(act.get("level") or "").lower() != "adset":
                    continue
                adset_id = str(act.get("adset_id") or "").strip()
                if not adset_id:
                    continue

                try:
                    new_budget = float(act.get("new_budget"))
                except Exception:
                    continue

                row = adsets_map.get(adset_id) or {}
                adset_name = row.get("name") or adset_id
                current_budget = row.get("daily_budget")
                try:
                    current_budget = float(current_budget) if current_budget is not None else None
                except Exception:
                    current_budget = None

                reason = str(act.get("reason") or "").strip()
                cents = int(round(new_budget * 100))
                reasons[f"bud:{aid}:{adset_id}:{cents}"] = reason

                lines = [
                    f"<b>{adset_name}</b>",
                ]
                if current_budget is not None:
                    lines.append(f"Текущий бюджет: ${current_budget:.2f}")
                lines.extend(
                    [
                        "",
                        "Рекомендация ИИ:",
                        f"— установить бюджет: ${new_budget:.2f}",
                    ]
                )
                if reason:
                    lines.append(f"— причина: {reason}")

                await context.bot.send_message(
                    chat_id,
                    "\n".join(lines),
                    parse_mode="HTML",
                    reply_markup=_ai_budget_kb(aid, adset_id, new_budget, current_budget),
                )

        # Объявления: кнопка PAUSE (если не единственное).
        if isinstance(ads_actions, list):
            for act in ads_actions:
                if not isinstance(act, dict):
                    continue
                a_type = str(act.get("type") or "").strip()
                ad_id = str(act.get("ad_id") or "").strip()
                adset_id = str(act.get("adset_id") or "").strip()
                if not ad_id:
                    continue

                reason = str(act.get("reason") or "").strip()
                try:
                    conf01 = float(act.get("confidence"))
                except Exception:
                    conf01 = None

                if a_type == "notify_only":
                    txt = reason or "ℹ️ Действие только для уведомления."
                    await context.bot.send_message(chat_id, txt)
                    continue

                if a_type != "pause_ad":
                    continue

                # Safety: если единственное активное объявление в adset — не показываем кнопку.
                allow_pause = True
                if adset_id:
                    try:
                        active_cnt = _count_active_ads_in_adset(aid, adset_id)
                        allow_pause = active_cnt > 1
                    except Exception:
                        allow_pause = False

                ads_map = {}
                try:
                    ads_map = _get_ads_map(aid)
                except Exception:
                    ads_map = {}
                ad_name = (ads_map.get(ad_id) or {}).get("name") or ad_id

                lines = [f"🔴 Объявление: <b>{ad_name}</b>"]
                if adset_id:
                    lines.append(f"Adset: <code>{adset_id}</code>")
                if reason:
                    lines.append("")
                    lines.append("Почему отключить:")
                    lines.append(f"— {reason}")
                if conf01 is not None:
                    lines.append(f"\nУверенность: {conf01:.2f}")

                key = f"adpause:{aid}:{ad_id}:{adset_id}"
                reasons[key] = reason

                if allow_pause and adset_id:
                    await context.bot.send_message(
                        chat_id,
                        "\n".join(lines),
                        parse_mode="HTML",
                        reply_markup=_ai_ad_pause_kb(aid, ad_id, adset_id),
                    )
                else:
                    await context.bot.send_message(
                        chat_id,
                        "\n".join(lines)
                        + "\n\nℹ️ Единственное объявление в adset — отключение не рекомендовано",
                        parse_mode="HTML",
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
        try:
            cur = (_get_adset_budget_map(aid).get(adset_id) or {}).get("daily_budget")
            cur = float(cur) if cur is not None else None
        except Exception:
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

        # Safety-check перед применением
        try:
            active_cnt = _count_active_ads_in_adset(aid, adset_id)
        except Exception:
            active_cnt = 0

        if active_cnt <= 1:
            await context.bot.send_message(
                chat_id,
                "❌ Не удалось применить действие: единственное активное объявление в adset — отключение запрещено.",
            )
            return

        reasons = context.user_data.get("ai_action_reasons") or {}
        reason = reasons.get(f"adpause:{aid}:{ad_id}:{adset_id}") or ""

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
        now = datetime.now(ALMATY_TZ)
        yday = (now - timedelta(days=1)).date()
        byday = (now - timedelta(days=2)).date()

        period_old = {"since": byday.strftime("%Y-%m-%d"), "until": byday.strftime("%Y-%m-%d")}
        period_new = {"since": yday.strftime("%Y-%m-%d"), "until": yday.strftime("%Y-%m-%d")}
        label_old = byday.strftime("%d.%m.%Y")
        label_new = yday.strftime("%d.%m.%Y")

        await safe_edit_message(q, f"Сравниваю: {label_new} vs {label_old}…", reply_markup=monitoring_menu_kb())
        await _send_comparison_for_all(context, chat_id, period_old, label_old, period_new, label_new)
        return

    if data == "mon_lastweek_vs_prevweek":
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

        await safe_edit_message(q, f"Сравниваю недели: {label_new} vs {label_old}…", reply_markup=monitoring_menu_kb())
        await _send_comparison_for_all(context, chat_id, period_old, label_old, period_new, label_new)
        return

    if data == "mon_curweek_vs_lastweek":
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

        await safe_edit_message(q, f"Сравниваю накопление: {label_new} vs {label_old}…", reply_markup=monitoring_menu_kb())
        await _send_comparison_for_all(context, chat_id, period_old, label_old, period_new, label_new)
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
        typing_task = asyncio.create_task(_typing_loop(context.bot, chat_id, stop_event))

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

        text = (
            f"📊 Метрика лидов — {get_account_name(aid)}\n\n"
            f"Текущая метрика: {current}\n\n"
            "Если метрика не выбрана, бот считает лиды по стандартным action_type."
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
        raw = _discover_actions_for_account(aid)
        if not raw:
            text = (
                f"action_type за вчера — {get_account_name(aid)}\n\n"
                "Нет ненулевых action_type за вчера (или нет доступа)."
            )
        else:
            lines = [f"action_type за вчера — {get_account_name(aid)}", ""]
            for it in raw:
                at = (it or {}).get("action_type")
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
        options = _discover_lead_metrics_for_account(aid)
        if not options:
            kb = InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data=f"lead_metric|{aid}")]]
            )
            await safe_edit_message(
                q,
                "❗️Не найдено метрик лидов с сайта за вчера.\n"
                "Проверь выбранный период или события в Ads Manager.",
                reply_markup=kb,
            )
            return

        if len(options) == 1:
            it = options[0] or {}
            action_type = it.get("action_type")
            label = it.get("label")
            if action_type:
                set_lead_metric_for_account(
                    aid,
                    action_type=str(action_type),
                    label=str(label or action_type),
                )
                try:
                    await q.answer("Метрика выбрана автоматически.")
                except Exception:
                    pass
                await context.bot.send_message(
                    chat_id,
                    f"Метрика лидов выбрана автоматически: {label or action_type}",
                )
                new_data = f"lead_metric|{aid}"
                await _on_cb_internal(update, context, q, chat_id, new_data)
                return

        mapping = {str(i): it for i, it in enumerate(options)}
        context.user_data["lead_metric_options"] = {"aid": aid, "items": mapping}

        current = get_lead_metric_for_account(aid)
        current_at = (current or {}).get("action_type") if current else None

        rows = []
        for i, it in mapping.items():
            label = it.get("label") or it.get("action_type")
            if current_at and it.get("action_type") == current_at:
                label = f"✅ {label}"
            rows.append(
                [
                    InlineKeyboardButton(
                        str(label),
                        callback_data=f"lead_metric_set|{aid}|{i}",
                    )
                ]
            )
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"lead_metric|{aid}")])
        await safe_edit_message(q, "Выбери метрику лидов (за вчера):", reply_markup=InlineKeyboardMarkup(rows))
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

        try:
            fb_campaigns = fetch_campaigns(aid) or []
        except Exception:
            fb_campaigns = []

        allowed_campaign_ids = {
            str(r.get("id"))
            for r in fb_campaigns
            if str((r or {}).get("effective_status") or (r or {}).get("status") or "").upper()
            in {"ACTIVE", "SCHEDULED"}
            and r.get("id")
        }

        try:
            camps = analyze_campaigns(aid, days=7) or []
        except Exception:
            camps = []

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

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        adset_alerts = alerts.get("adset_alerts", {}) or {}

        # Для списка адсетов переиспользуем send_adset_report-источник:
        # модуль adsets уже работает с актуальными данными, здесь берём
        # только имена/ID через вспомогательную функцию.
        from .adsets import list_adsets_for_account

        adsets = list_adsets_for_account(aid)

        # Берём статусы адсетов из Facebook API, чтобы понимать активность.
        try:
            fb_adsets = fetch_adsets(aid) or []
        except Exception:
            fb_adsets = []

        allowed_adset_ids = {
            str(row.get("id"))
            for row in fb_adsets
            if str((row or {}).get("effective_status") or (row or {}).get("status") or "").upper()
            in {"ACTIVE", "SCHEDULED"}
            and row.get("id")
        }

        kb_rows = []
        for it in adsets:
            adset_id = it.get("id")
            name = it.get("name", adset_id)
            if adset_id not in allowed_adset_ids:
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
        new_data = f"cpa_adset|{aid}|{adset_id}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
        return

    if data.startswith("cpa_ads|"):
        aid = data.split("|", 1)[1]

        st = load_accounts()
        row = st.get(aid, {"alerts": {}})
        alerts = row.get("alerts", {}) or {}
        ad_alerts = alerts.get("ad_alerts", {}) or {}

        # Метрики по объявлениям для CPA и названий
        try:
            ads = analyze_ads(aid, days=7) or []
        except Exception:
            ads = []

        # Метаданные объявлений и адсетов для статуса активности
        try:
            fb_ads = fetch_ads(aid) or []
        except Exception:
            fb_ads = []

        ad_status: dict[str, str] = {}
        ad_to_adset: dict[str, str] = {}
        for row in fb_ads:
            ad_id_raw = str(row.get("id") or "")
            if not ad_id_raw:
                continue
            ad_status[ad_id_raw] = row.get("effective_status") or row.get("status") or ""
            ad_to_adset[ad_id_raw] = str(row.get("adset_id") or "")

        # Статусы адсетов
        try:
            fb_adsets = fetch_adsets(aid) or []
        except Exception:
            fb_adsets = []

        allowed_adset_ids = {
            str(row.get("id"))
            for row in fb_adsets
            if str((row or {}).get("effective_status") or (row or {}).get("status") or "").upper()
            in {"ACTIVE", "SCHEDULED"}
            and row.get("id")
        }

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

        new_data = f"cpa_ad_cfg|{aid}|{ad_id}"
        await _on_cb_internal(update, context, q, chat_id, new_data)
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
        try:
            val = int(float(text.replace(",", ".")))
        except Exception:
            await update.message.reply_text("Введите число, например 20 (или 0 чтобы сбросить)")
            context.user_data["await_ap_leads_for"] = payload
            return

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
        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text("Введите число в $ (например 1.2) или 0 чтобы сбросить")
            context.user_data["await_ap_cpl_for"] = payload
            return

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
        try:
            val = float(text.replace(",", "."))
        except Exception:
            await update.message.reply_text("Введите число в $ (например 30) или 0 чтобы сбросить")
            context.user_data["await_ap_budget_for"] = payload
            return

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

        try:
            dt = datetime.strptime(text.strip(), "%d.%m.%Y").date()
        except Exception:
            await update.message.reply_text("Формат даты: ДД.ММ.ГГГГ (например 25.01.2026). Попробуй ещё раз.")
            context.user_data["await_ap_until_for"] = payload
            return

        ap = _autopilot_get(aid)
        goals = ap.get("goals") or {}
        if not isinstance(goals, dict):
            goals = {}
        goals["period"] = "until"
        goals["until"] = dt.strftime("%d.%m.%Y")
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
