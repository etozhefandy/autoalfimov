# fb_report/jobs.py

from datetime import datetime, timedelta, time
import asyncio
import re
import json
import logging
import os
import uuid

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, Application

from .constants import ALMATY_TZ, DEFAULT_REPORT_CHAT, AUTOPILOT_CHAT_ID, ALLOWED_USER_IDS
from .storage import load_accounts, get_account_name, resolve_autopilot_chat_id
from .reporting import send_period_report, get_cached_report, build_account_report
from .cpa_monitoring import (
    build_monitor_snapshot,
    evaluate_rules,
    format_cpa_anomaly_message,
)
from .adsets import fetch_adset_insights_7d
from .insights import build_hourly_heatmap_for_account
from .autopilot_format import ap_action_text

try:  # pragma: no cover
    from services.heatmap_store import load_snapshot, prev_full_hour_window
except Exception:  # noqa: BLE001
    def load_snapshot(_aid: str, *, date_str: str, hour: int):  # type: ignore[override]
        return None

    def prev_full_hour_window(now: datetime | None = None):  # type: ignore[override]
        return {}

# Для Railway могут быть разные пути импорта services.*.
# Пытаемся взять реальные функции, а при ошибке делаем мягкие заглушки,
# чтобы бот не падал при старте.
try:  # pragma: no cover - защитный импорт для продакшена
    from services.storage import load_hourly_stats, save_hourly_stats
except Exception:  # noqa: BLE001 - нам важен ЛЮБОЙ ImportError/RuntimeError
    def load_hourly_stats() -> dict:  # type: ignore[override]
        return {}

    def save_hourly_stats(_stats: dict) -> None:  # type: ignore[override]
        return None

try:  # pragma: no cover
    from services.facebook_api import (
        fetch_insights,
        safe_api_call,
        _normalize_insight,
        _period_to_params,
    )
    from services.analytics import (
        parse_insight,
        analyze_account,
        analyze_campaigns,
        analyze_adsets,
        analyze_ads,
    )
    from services.ai_focus import ask_deepseek
    from services.facebook_api import fetch_adsets
    from services.facebook_api import (
        is_rate_limited_now,
        rate_limit_retry_after_seconds,
        get_last_api_error_info,
        classify_api_error,
        allow_fb_api_calls,
        deny_fb_api_calls,
    )
except Exception:  # noqa: BLE001
    fetch_insights = None  # type: ignore[assignment]

    def safe_api_call(_fn, *args, **kwargs):  # type: ignore[override]
        return None

    def _normalize_insight(_row):  # type: ignore[override]
        return {}

    def _period_to_params(_period):  # type: ignore[override]
        return {}

    def parse_insight(_ins: dict, **_kwargs) -> dict:  # type: ignore[override]
        return {"msgs": 0, "leads": 0, "total": 0, "spend": 0.0, "cpa": None}

    def is_rate_limited_now() -> bool:  # type: ignore[override]
        return False

    def rate_limit_retry_after_seconds() -> int:  # type: ignore[override]
        return 0

    def get_last_api_error_info() -> dict:  # type: ignore[override]
        return {}

    def classify_api_error(_info: dict) -> str:  # type: ignore[override]
        return "api_error"

    class _Allow:
        def __enter__(self):
            return None

        def __exit__(self, _t, _v, _tb):
            return False

    def allow_fb_api_calls(_reason: str | None = None):  # type: ignore[override]
        return _Allow()

    def deny_fb_api_calls(_reason: str | None = None):  # type: ignore[override]
        return _Allow()

    def analyze_account(_aid: str, days: int = 7, period=None):  # type: ignore[override]
        return {"aid": _aid, "metrics": None}

    def analyze_campaigns(_aid: str, days: int = 7, period=None):  # type: ignore[override]
        return []

    def analyze_adsets(_aid: str, days: int = 7, period=None):  # type: ignore[override]
        return []

    def analyze_ads(_aid: str, days: int = 7, period=None):  # type: ignore[override]
        return []

    async def ask_deepseek(
        _messages,
        json_mode: bool = False,
        *,
        andrey_tone: bool = False,
        temperature: float | None = None,
        max_tokens: int | None = None,
    ):  # type: ignore[override]
        raise RuntimeError("DeepSeek is not available in this environment")

    def fetch_adsets(_aid: str):  # type: ignore[override]
        return []


try:  # pragma: no cover
    from services.heatmap_store import (
        find_latest_ready_snapshots,
        get_heatmap_dataset,
        prev_full_hour_window,
    )
except Exception:  # noqa: BLE001
    def find_latest_ready_snapshots(  # type: ignore[override]
        _aid: str,
        *,
        max_hours: int,
        now: datetime | None = None,
    ):
        return []

    def get_heatmap_dataset(_aid: str, *, date_str: str, hours: list[int]):  # type: ignore[override]
        return None, "missing", "no_snapshot", {}

    def prev_full_hour_window(now: datetime | None = None):  # type: ignore[override]
        return {}


def _cpa_agg_from_rows(rows: list[dict]) -> tuple[float, int]:
    spend = 0.0
    total = 0
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        try:
            spend += float(r.get("spend") or 0.0)
        except Exception:
            pass
        try:
            t = r.get("total")
            if t is None:
                t = int(r.get("msgs") or 0) + int(r.get("leads") or 0)
            total += int(t or 0)
        except Exception:
            pass
    return float(spend), int(total)


def _cpa_series_from_snapshots(snaps: list[dict]) -> tuple[list[float | None], list[float], list[int]]:
    cpa_series: list[float | None] = []
    spend_series: list[float] = []
    total_series: list[int] = []
    for snap in snaps or []:
        rows = (snap or {}).get("rows") or []
        if not isinstance(rows, list):
            rows = []
        sp, tot = _cpa_agg_from_rows(rows)
        spend_series.append(float(sp))
        total_series.append(int(tot))
        if tot > 0 and sp > 0:
            cpa_series.append(float(sp) / float(tot))
        else:
            cpa_series.append(None)
    return cpa_series, spend_series, total_series


def _delta_pct(first: float | None, last: float | None) -> int | None:
    if first is None or last is None or first <= 0:
        return None
    try:
        return int(round(((float(last) - float(first)) / float(first)) * 100.0))
    except Exception:
        return None


async def _cpa_alerts_job_snapshots_only(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    now: datetime,
    accounts: dict,
    chat_id: str,
) -> None:
    try:
        min_spend = float(os.getenv("CPA_ALERTS_MIN_SPEND", "20") or 20)
    except Exception:
        min_spend = 20.0
    try:
        min_actions = int(float(os.getenv("CPA_ALERTS_MIN_ACTIONS", "5") or 5))
    except Exception:
        min_actions = 5

    win = prev_full_hour_window(now=now) or {}
    win_label = f"{((win.get('window') or {}).get('start') or '')}–{((win.get('window') or {}).get('end') or '')}"

    for aid, row in (accounts or {}).items():
        alerts = (row or {}).get("alerts") or {}
        if not isinstance(alerts, dict):
            alerts = {}

        if not bool(alerts.get("enabled", False)):
            continue
        if not _is_day_enabled(alerts, now):
            continue

        freq = alerts.get("freq", "3x")
        if freq == "3x":
            current_time = now.replace(second=0, microsecond=0).time()
            if current_time not in CPA_ALERT_TIMES:
                continue
        elif freq == "hourly":
            if not (CPA_HOURLY_START <= now.hour <= CPA_HOURLY_END):
                continue
        else:
            continue

        with deny_fb_api_calls(reason="cpa_alerts_prev_hour"):
            _ds, ds_status, ds_reason, _ds_meta = get_heatmap_dataset(
                str(aid),
                date_str=str(win.get("date") or ""),
                hours=[int(win.get("hour") or 0)],
            )
        if str(ds_status) not in {"ready", "ready_low_confidence"}:
            continue

        snaps_desc = find_latest_ready_snapshots(str(aid), max_hours=4, now=now) or []
        snaps = list(reversed(snaps_desc))
        if not snaps:
            continue

        account_target = _resolve_account_cpa(alerts)
        if float(account_target) <= 0:
            continue

        campaign_alerts = alerts.get("campaign_alerts", {}) or {}
        adset_alerts = alerts.get("adset_alerts", {}) or {}

        account_cpa_series, account_spend_series, account_total_series = _cpa_series_from_snapshots(snaps)
        last_cpa = account_cpa_series[-1] if account_cpa_series else None
        last_spend = float(account_spend_series[-1] if account_spend_series else 0.0)
        last_total = int(account_total_series[-1] if account_total_series else 0)
        first_cpa = next((v for v in account_cpa_series if v is not None), None)
        dp = _delta_pct(first_cpa, last_cpa)

        if (
            last_cpa is not None
            and float(last_cpa) > float(account_target)
            and last_spend >= float(min_spend)
            and last_total >= int(min_actions)
        ):
            rules = [{"rule": "cpa_above_target", "severity": "high", "should_notify": True}]
            if dp is not None and int(dp) >= 50:
                rules.append({"rule": "cpa_spike", "severity": "high", "should_notify": True})

            snap_msg = {
                "account_id": str(aid),
                "account_name": get_account_name(str(aid)),
                "entity_id": None,
                "level": "account",
                "history_days": 1,
                "cpa_series": account_cpa_series,
                "delta_pct": dp,
                "target_cpa": float(account_target),
            }

            try:
                msg_lines = [
                    "Источник данных: heatmap cache",
                    f"Окно: {win_label}",
                    f"Слепок: {ds_status} ({ds_reason})",
                    "",
                    format_cpa_anomaly_message(
                        snapshot=snap_msg,
                        entity_name=get_account_name(str(aid)),
                        level_human="Аккаунт",
                        triggered_rules=rules,
                        ai_text=None,
                        ai_confidence=None,
                    ),
                ]
                await context.bot.send_message(chat_id, "\n".join(msg_lines))
                await asyncio.sleep(0.3)
            except Exception:
                pass

        # Campaign/adset alerts (use campaign_id/adset_id from snapshot rows)
        camp_series: dict[str, list[float | None]] = {}
        camp_spend: dict[str, list[float]] = {}
        camp_total: dict[str, list[int]] = {}
        adset_series: dict[str, list[float | None]] = {}
        adset_spend: dict[str, list[float]] = {}
        adset_total: dict[str, list[int]] = {}
        adset_names: dict[str, str] = {}
        adset_campaign: dict[str, str] = {}

        for snap in snaps:
            rows = (snap or {}).get("rows") or []
            if not isinstance(rows, list):
                rows = []

            by_camp: dict[str, list[dict]] = {}
            by_adset: dict[str, list[dict]] = {}
            for r in rows:
                if not isinstance(r, dict):
                    continue
                cid = str(r.get("campaign_id") or "")
                if cid:
                    by_camp.setdefault(cid, []).append(r)
                adset_id = str(r.get("adset_id") or "")
                if adset_id:
                    by_adset.setdefault(adset_id, []).append(r)
                    if adset_id not in adset_names:
                        adset_names[adset_id] = str(r.get("name") or adset_id)
                    if cid and adset_id not in adset_campaign:
                        adset_campaign[adset_id] = cid

            for cid, rr in by_camp.items():
                sp, tot = _cpa_agg_from_rows(rr)
                camp_spend.setdefault(cid, []).append(float(sp))
                camp_total.setdefault(cid, []).append(int(tot))
                camp_series.setdefault(cid, []).append((float(sp) / float(tot)) if tot > 0 and sp > 0 else None)

            for adset_id, rr in by_adset.items():
                sp, tot = _cpa_agg_from_rows(rr)
                adset_spend.setdefault(adset_id, []).append(float(sp))
                adset_total.setdefault(adset_id, []).append(int(tot))
                adset_series.setdefault(adset_id, []).append((float(sp) / float(tot)) if tot > 0 and sp > 0 else None)

        for cid, series in (camp_series or {}).items():
            cfg = (campaign_alerts.get(cid) or {}) if isinstance(campaign_alerts, dict) else {}
            if cfg and cfg.get("enabled") is False:
                continue
            tgt = float(cfg.get("target_cpa") or 0.0)
            effective_target = tgt if tgt > 0 else float(account_target)
            if effective_target <= 0:
                continue
            sp_series = camp_spend.get(cid) or []
            t_series = camp_total.get(cid) or []
            if not sp_series or not t_series or len(series) != len(sp_series) or len(series) != len(t_series):
                continue
            last_val = series[-1]
            if last_val is None:
                continue
            last_sp = float(sp_series[-1] or 0.0)
            last_tot = int(t_series[-1] or 0)
            if last_sp < float(min_spend) or last_tot < int(min_actions):
                continue
            if float(last_val) <= float(effective_target):
                continue
            first_val = next((v for v in series if v is not None), None)
            cdp = _delta_pct(first_val, last_val)
            rules = [{"rule": "cpa_above_target", "severity": "high", "should_notify": True}]
            if cdp is not None and int(cdp) >= 50:
                rules.append({"rule": "cpa_spike", "severity": "high", "should_notify": True})

            snap_msg = {
                "account_id": str(aid),
                "account_name": get_account_name(str(aid)),
                "entity_id": str(cid),
                "level": "campaign",
                "history_days": 1,
                "cpa_series": series,
                "delta_pct": cdp,
                "target_cpa": float(effective_target),
            }
            try:
                msg_lines = [
                    "Источник данных: heatmap cache",
                    f"Окно: {win_label}",
                    f"Слепок: {ds_status} ({ds_reason})",
                    "",
                    format_cpa_anomaly_message(
                        snapshot=snap_msg,
                        entity_name=str(cid),
                        level_human="Кампания",
                        triggered_rules=rules,
                        ai_text=None,
                        ai_confidence=None,
                    ),
                ]
                await context.bot.send_message(chat_id, "\n".join(msg_lines))
                await asyncio.sleep(0.3)
            except Exception:
                pass

        for adset_id, series in (adset_series or {}).items():
            cfg = (adset_alerts.get(adset_id) or {}) if isinstance(adset_alerts, dict) else {}
            if cfg and cfg.get("enabled") is False:
                continue
            tgt = float(cfg.get("target_cpa") or 0.0)
            camp_id = str(adset_campaign.get(adset_id) or "")
            camp_target = 0.0
            if camp_id and isinstance(campaign_alerts, dict) and camp_id in campaign_alerts:
                try:
                    camp_target = float((campaign_alerts.get(camp_id) or {}).get("target_cpa") or 0.0)
                except Exception:
                    camp_target = 0.0
            effective_target = tgt if tgt > 0 else camp_target if camp_target > 0 else float(account_target)
            if effective_target <= 0:
                continue
            sp_series = adset_spend.get(adset_id) or []
            t_series = adset_total.get(adset_id) or []
            if not sp_series or not t_series or len(series) != len(sp_series) or len(series) != len(t_series):
                continue
            last_val = series[-1]
            if last_val is None:
                continue
            last_sp = float(sp_series[-1] or 0.0)
            last_tot = int(t_series[-1] or 0)
            if last_sp < float(min_spend) or last_tot < int(min_actions):
                continue
            if float(last_val) <= float(effective_target):
                continue
            first_val = next((v for v in series if v is not None), None)
            adp = _delta_pct(first_val, last_val)
            rules = [{"rule": "cpa_above_target", "severity": "high", "should_notify": True}]
            if adp is not None and int(adp) >= 50:
                rules.append({"rule": "cpa_spike", "severity": "high", "should_notify": True})

            snap_msg = {
                "account_id": str(aid),
                "account_name": get_account_name(str(aid)),
                "entity_id": str(adset_id),
                "level": "adset",
                "history_days": 1,
                "cpa_series": series,
                "delta_pct": adp,
                "target_cpa": float(effective_target),
            }
            try:
                msg_lines = [
                    "Источник данных: heatmap cache",
                    f"Окно: {win_label}",
                    f"Слепок: {ds_status} ({ds_reason})",
                    "",
                    format_cpa_anomaly_message(
                        snapshot=snap_msg,
                        entity_name=str(adset_names.get(adset_id) or adset_id),
                        level_human="Адсет",
                        triggered_rules=rules,
                        ai_text=None,
                        ai_confidence=None,
                    ),
                ]
                await context.bot.send_message(chat_id, "\n".join(msg_lines))
                await asyncio.sleep(0.3)
            except Exception:
                pass


def _autopilot_report_chat_id() -> str:
    # Backward-compat wrapper (kept to avoid invasive refactor).
    cid, _src = resolve_autopilot_chat_id()
    return str(cid)


def _resolve_autopilot_chat_id_logged(*, reason: str) -> tuple[str, str]:
    cid, src = resolve_autopilot_chat_id()
    logging.getLogger(__name__).info(
        "autopilot_chat_resolve autopilot_chat_id=%s source=%s reason=%s",
        str(cid),
        str(src),
        str(reason),
    )
    return str(cid), str(src)


try:  # pragma: no cover
    from history_store import append_autopilot_event
except Exception:  # noqa: BLE001
    def append_autopilot_event(_aid: str, _event: dict) -> None:  # type: ignore[override]
        return None


try:  # pragma: no cover
    from autopilat.actions import set_adset_budget
except Exception:  # noqa: BLE001
    def set_adset_budget(_adset_id: str, _new_budget: float) -> dict:  # type: ignore[override]
        return {"status": "error", "message": "set_adset_budget unavailable"}


def _heatmap_force_kb(aid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ Разрешить на 1 час", callback_data=f"aphmforce|{aid}")]]
    )


def _ap_force_prompt_due(ap: dict, now: datetime, *, minutes: int = 60) -> bool:
    state = (ap or {}).get("heatmap_state") or {}
    if not isinstance(state, dict):
        return True
    last_iso = state.get("last_force_prompt")
    if not last_iso:
        return True
    try:
        dt = datetime.fromisoformat(str(last_iso))
        if not dt.tzinfo:
            dt = ALMATY_TZ.localize(dt)
        dt = dt.astimezone(ALMATY_TZ)
    except Exception:
        return True
    return (now - dt) >= timedelta(minutes=int(minutes))


def _ap_force_button_allowed(now: datetime) -> bool:
    h = int(now.strftime("%H"))
    return 10 <= h <= 22


def _yesterday_period():
    now = datetime.now(ALMATY_TZ)
    until = now - timedelta(days=1)
    since = until
    period = {
        "since": since.strftime("%Y-%m-%d"),
        "until": until.strftime("%Y-%m-%d"),
    }
    label = until.strftime("%d.%m.%Y")
    return period, label


async def full_daily_scan_job(context: ContextTypes.DEFAULT_TYPE):
    """Утренний отчёт (🌅): вчера vs позавчера по уровням.

    Настройки берутся из row["morning_report"]["level"], где level один из
    OFF / ACCOUNT / CAMPAIGN / ADSET:

    - OFF      — отчёт не отправляется;
    - ACCOUNT  — только итоговый блок по аккаунту;
    - CAMPAIGN — аккаунт + проблемные кампании;
    - ADSET    — аккаунт + проблемные кампании + проблемные адсеты.

    Пороги ухудшения фиксированы:
    - 🔴 CPA вырос ≥25% или лиды упали ≥25%;
    - 🟡 CPA вырос ≥10% или лиды упали ≥10%;
    - иначе 🟢.
    """

    chat_id = str(DEFAULT_REPORT_CHAT)

    now = datetime.now(ALMATY_TZ).date()
    yday = now - timedelta(days=1)

    period_yday = {
        "since": yday.strftime("%Y-%m-%d"),
        "until": yday.strftime("%Y-%m-%d"),
    }

    store = load_accounts() or {}

    for aid, row in store.items():
        if not (row or {}).get("enabled", True):
            continue

        mr = (row or {}).get("morning_report") or {}
        level = str(mr.get("level", "ACCOUNT")).upper()

        if level == "OFF":
            continue

        label = yday.strftime("%d.%m.%Y")
        body = build_account_report(aid, period_yday, level, label=label)
        if not body:
            continue

        try:
            await context.bot.send_message(chat_id, body, parse_mode="HTML")
            await asyncio.sleep(0.5)
        except Exception:
            # Утренний отчёт не должен ломать остальные джобы.
            continue


async def daily_report_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id = str(DEFAULT_REPORT_CHAT)

    period, label = _yesterday_period()

    try:
        await send_period_report(context, chat_id, period, label)
        logging.getLogger(__name__).info("🟢 Daily report job executed successfully")
    except Exception as e:
        await context.bot.send_message(
            chat_id,
            f"⚠️ daily_report_job: ошибка дневного отчёта: {e}",
        )


def _parse_totals_from_report_text(txt: str):
    """
    Парсим ОДИН текстовый отчёт по аккаунту и вытаскиваем:
    - messages (✉️ / 💬)
    - leads (📩 / ♿️)
    - total_conversions (из строки 'Итого: N заявок', если есть)
    - spend (💵)
    """
    total_messages = 0
    total_leads = 0
    spend = 0.0
    total_from_line = None

    msg_pattern = re.compile(r"(?:💬|✉️)[^0-9]*?(\d+)")
    lead_pattern = re.compile(r"(?:📩|♿️)[^0-9]*?(\d+)")
    spend_pattern = re.compile(r"💵[^0-9]*?([0-9]+[.,]?[0-9]*)")
    total_pattern = re.compile(r"Итого:\s*([0-9]+)\s+заяв", re.IGNORECASE)

    for line in txt.splitlines():
        m_msg = msg_pattern.search(line)
        if m_msg:
            try:
                total_messages += int(m_msg.group(1))
            except Exception:
                pass

        m_lead = lead_pattern.search(line)
        if m_lead:
            try:
                total_leads += int(m_lead.group(1))
            except Exception:
                pass

        m_spend = spend_pattern.search(line)
        if m_spend:
            try:
                spend = float(m_spend.group(1).replace(",", "."))
            except Exception:
                pass

        m_total = total_pattern.search(line)
        if m_total:
            try:
                total_from_line = int(m_total.group(1))
            except Exception:
                pass

    total_convs = total_messages + total_leads

    if total_from_line is not None and total_from_line > 0:
        total_convs = total_from_line

    cpa = None
    if total_convs > 0 and spend > 0:
        cpa = spend / total_convs

    return {
        "messages": total_messages,
        "leads": total_leads,
        "total_conversions": total_convs,
        "spend": spend,
        "cpa": cpa,
    }


CPA_ALERT_TIMES = (
    # Временные слоты для режима "3 раза в день" (по Алмате)
    time(hour=11, minute=0, tzinfo=ALMATY_TZ),
    time(hour=15, minute=0, tzinfo=ALMATY_TZ),
    time(hour=19, minute=0, tzinfo=ALMATY_TZ),
)

CPA_HOURLY_START = 10
CPA_HOURLY_END = 22

WEEKDAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


def _ap_daily_budget_limit_usd(goals: dict, now: datetime) -> float | None:
    planned = (goals or {}).get("planned_budget")
    try:
        planned_f = float(planned) if planned not in (None, "") else None
    except Exception:
        planned_f = None
    if planned_f is None or planned_f <= 0:
        return None

    period = str((goals or {}).get("period") or "day")
    today = now.date()

    if period == "day":
        return float(planned_f)
    if period == "week":
        return float(planned_f) / 7.0
    if period == "month":
        # 28–31 days safe approximation, enough for limiting
        days_in_month = (today.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        return float(planned_f) / float(days_in_month.day)
    if period == "until":
        until_raw = (goals or {}).get("until")
        try:
            until_dt = datetime.strptime(str(until_raw or ""), "%d.%m.%Y").date()
        except Exception:
            return None
        days_left = (until_dt - today).days + 1
        if days_left < 1:
            days_left = 1
        return float(planned_f) / float(days_left)
    return None


def _ap_period_spend_limit(goals: dict, now: datetime) -> tuple[float | None, dict | None]:
    planned = (goals or {}).get("planned_budget")
    try:
        planned_f = float(planned) if planned not in (None, "") else None
    except Exception:
        planned_f = None
    if planned_f is None or planned_f <= 0:
        return None, None

    period = str((goals or {}).get("period") or "day")
    today = now.date()
    if period == "week":
        since = today - timedelta(days=today.weekday())
        until = today
        return float(planned_f), {"since": since.strftime("%Y-%m-%d"), "until": until.strftime("%Y-%m-%d")}
    if period == "month":
        since = today.replace(day=1)
        until = today
        return float(planned_f), {"since": since.strftime("%Y-%m-%d"), "until": until.strftime("%Y-%m-%d")}
    # day/until: period limit == daily or unknown window; do not enforce cumulative here.
    return float(planned_f), None


def _ap_is_heatmap_due(ap: dict, now: datetime) -> bool:
    limits = (ap or {}).get("limits") or {}
    try:
        min_minutes = int(float((limits or {}).get("heatmap_min_interval_minutes") or 60))
    except Exception:
        min_minutes = 60
    if min_minutes < 1:
        min_minutes = 1

    state = (ap or {}).get("heatmap_state") or {}
    last_iso = (state or {}).get("last_apply")
    if not last_iso:
        return True
    try:
        dt = datetime.fromisoformat(str(last_iso))
        if not dt.tzinfo:
            dt = ALMATY_TZ.localize(dt)
        dt = dt.astimezone(ALMATY_TZ)
    except Exception:
        return True

    return (now - dt) >= timedelta(minutes=min_minutes)


def _ap_heatmap_due_meta(ap: dict, now: datetime) -> tuple[bool, dict]:
    limits = (ap or {}).get("limits") or {}
    try:
        min_minutes = int(float((limits or {}).get("heatmap_min_interval_minutes") or 60))
    except Exception:
        min_minutes = 60
    if min_minutes < 1:
        min_minutes = 1

    state = (ap or {}).get("heatmap_state") or {}
    last_iso = (state or {}).get("last_apply")
    if not last_iso:
        return True, {"min_minutes": int(min_minutes), "last_apply": None}

    try:
        dt = datetime.fromisoformat(str(last_iso))
        if not dt.tzinfo:
            dt = ALMATY_TZ.localize(dt)
        dt = dt.astimezone(ALMATY_TZ)
    except Exception:
        return True, {"min_minutes": int(min_minutes), "last_apply": str(last_iso)}

    due = (now - dt) >= timedelta(minutes=min_minutes)
    next_at = (dt + timedelta(minutes=min_minutes)).isoformat()
    return bool(due), {"min_minutes": int(min_minutes), "last_apply": dt.isoformat(), "next_apply": next_at}


def _ap_heatmap_force_active(ap: dict, now: datetime) -> bool:
    state = (ap or {}).get("heatmap_state") or {}
    until_iso = (state or {}).get("force_until")
    if not until_iso:
        return False
    try:
        dt = datetime.fromisoformat(str(until_iso))
        if not dt.tzinfo:
            dt = ALMATY_TZ.localize(dt)
        dt = dt.astimezone(ALMATY_TZ)
    except Exception:
        return False
    return now <= dt


def _ap_heatmap_force_until(ap: dict) -> str:
    state = (ap or {}).get("heatmap_state") or {}
    return str((state or {}).get("force_until") or "")


def _ap_hourly_bucket(stats: dict, *, section: str, aid: str, entity_id: str, date_key: str, hour_key: str) -> dict:
    try:
        root = (stats or {}).get(section) or {}
        a = (root or {}).get(str(aid)) or {}
        e = (a or {}).get(str(entity_id)) or {}
        d = (e or {}).get(str(date_key)) or {}
        b = (d or {}).get(str(hour_key)) or {}
        return b if isinstance(b, dict) else {}
    except Exception:
        return {}


def _ap_hourly_agg(stats: dict, *, section: str, aid: str, entity_id: str, now: datetime, hour_key: str, days: int) -> dict:
    spend = 0.0
    total = 0
    msgs = 0
    leads = 0
    for i in range(int(days)):
        d = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        b = _ap_hourly_bucket(stats, section=section, aid=aid, entity_id=entity_id, date_key=d, hour_key=hour_key)
        try:
            spend += float((b or {}).get("spend", 0.0) or 0.0)
        except Exception:
            pass
        try:
            total += int((b or {}).get("total", 0) or 0)
        except Exception:
            pass
        try:
            msgs += int((b or {}).get("messages", 0) or 0)
        except Exception:
            pass
        try:
            leads += int((b or {}).get("leads", 0) or 0)
        except Exception:
            pass

    cpl = (spend / float(total)) if total > 0 and spend > 0 else None
    return {"spend": spend, "total": total, "messages": msgs, "leads": leads, "cpl": cpl}


def _ap_select_hourly_good_bad_adsets(
    stats: dict,
    *,
    aid: str,
    adset_ids: list[str],
    now: datetime,
    hour_key: str,
    target_cpl: float | None,
    cpa_3d: float | None,
) -> tuple[set[str], set[str], dict]:
    rows = []
    for adset_id in adset_ids:
        agg = _ap_hourly_agg(
            stats,
            section="_adset",
            aid=aid,
            entity_id=adset_id,
            now=now,
            hour_key=hour_key,
            days=7,
        )
        spend_7d = float((agg or {}).get("spend", 0.0) or 0.0)
        total_7d = int((agg or {}).get("total", 0) or 0)
        cpl_7d = (agg or {}).get("cpl")

        if not isinstance(cpl_7d, (int, float)):
            continue
        if total_7d < 2 or spend_7d < 5.0:
            continue

        score = float(cpl_7d)
        rows.append({"adset_id": adset_id, "cpl_7d": float(cpl_7d), "spend_7d": spend_7d, "total_7d": total_7d, "score": score})

    if len(rows) < 3:
        return set(), set(), {"used": False, "reason": "insufficient_hourly", "count": len(rows)}

    rows.sort(key=lambda r: r["score"])
    k = max(1, min(5, int(round(len(rows) * 0.25))))

    good = [r for r in rows[:k]]
    bad = [r for r in rows[-k:]]

    good_ids = set(r["adset_id"] for r in good)
    bad_ids = set(r["adset_id"] for r in bad if r["adset_id"] not in good_ids)

    meta = {
        "used": True,
        "count": len(rows),
        "k": k,
        "good": [{"id": r["adset_id"], "cpl_7d": r["cpl_7d"], "total_7d": r["total_7d"], "spend_7d": r["spend_7d"]} for r in good],
        "bad": [{"id": r["adset_id"], "cpl_7d": r["cpl_7d"], "total_7d": r["total_7d"], "spend_7d": r["spend_7d"]} for r in bad],
    }

    if target_cpl is not None and target_cpl > 0:
        good_ids = set(
            r["adset_id"]
            for r in good
            if float(r["cpl_7d"]) <= float(target_cpl) * 1.20
        )
        bad_ids = set(
            r["adset_id"]
            for r in bad
            if float(r["cpl_7d"]) >= float(target_cpl) * 1.50
        )
        meta["filtered_by"] = "target"
    elif cpa_3d is not None and cpa_3d > 0:
        bad_ids = set(
            r["adset_id"]
            for r in bad
            if float(r["cpl_7d"]) >= float(cpa_3d) * 1.50
        )
        meta["filtered_by"] = "cpa_3d"

    return set(good_ids), set(bad_ids), meta


def _ap_find_worst_ad_in_hour(stats: dict, *, aid: str, adset_id: str, now: datetime, hour_key: str) -> dict:
    root = (stats or {}).get("_ad") or {}
    a = (root or {}).get(str(aid)) or {}
    if not isinstance(a, dict) or not a:
        return {}

    worst = None
    worst_key = None
    date_key = now.strftime("%Y-%m-%d")
    for ad_id, ad_days in a.items():
        if not isinstance(ad_days, dict):
            continue
        day = ad_days.get(date_key) or {}
        if not isinstance(day, dict):
            continue
        b = day.get(str(hour_key)) or {}
        if not isinstance(b, dict) or not b:
            continue
        if str((b or {}).get("adset_id") or "") != str(adset_id):
            continue

        spend = float((b or {}).get("spend", 0.0) or 0.0)
        total = int((b or {}).get("total", 0) or 0)
        if total <= 0 or spend <= 0:
            continue
        if total < 2 and spend < 3.0:
            continue

        cpl_today = spend / float(total)
        agg_7d = _ap_hourly_agg(
            stats,
            section="_ad",
            aid=aid,
            entity_id=str(ad_id),
            now=now,
            hour_key=str(hour_key),
            days=7,
        )
        cpl_7d = agg_7d.get("cpl")

        if not (isinstance(cpl_7d, (int, float)) and float(cpl_7d) > 0):
            continue
        ratio = float(cpl_today) / float(cpl_7d)
        if ratio < 2.0:
            continue
        if float(cpl_today) < float(cpl_7d) + 4.0:
            continue
        key = (ratio, cpl_today, spend)

        if worst is None or key > worst_key:
            worst = {
                "ad_id": str(ad_id),
                "spend": spend,
                "total": total,
                "cpl_today": cpl_today,
                "cpl_7d": cpl_7d,
                "ratio": ratio,
            }
            worst_key = key

    return worst or {}


def _ap_heatmap_profile(summary: dict) -> tuple[list[int], list[int]]:
    days = (summary or {}).get("days") or []
    totals = [0 for _ in range(24)]
    spends = [0.0 for _ in range(24)]

    if not days:
        return [], []

    for d in days:
        vals = (d or {}).get("totals_per_hour") or []
        sp_h = (d or {}).get("spend_per_hour") or []
        for i in range(min(24, len(vals))):
            try:
                totals[i] += int(vals[i] or 0)
            except Exception:
                continue
            try:
                spends[i] += float(sp_h[i] or 0.0) if i < len(sp_h) else 0.0
            except Exception:
                continue

    scored = []
    for h in range(24):
        t = totals[h]
        sp = spends[h]
        if t >= 2 and sp > 0:
            score = float(t) / float(sp)
        else:
            score = float(t)
        scored.append((h, score, t))

    scored.sort(key=lambda x: (x[1], x[2]), reverse=True)
    top = [h for h, _s, t in scored if t > 0][:4]

    scored_low = sorted(scored, key=lambda x: (x[2], x[1]))
    low = [h for h, _s, _t in scored_low][:4]
    return top, low


async def _heatmap_snapshot_collector_job(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    manual: bool = False,
    manual_aid: str | None = None,
):
    from services.heatmap_store import load_snapshot, save_snapshot, build_snapshot_shell

    now = datetime.now(ALMATY_TZ)
    end_dt = now.replace(minute=0, second=0, microsecond=0)
    start_dt = end_dt - timedelta(hours=1)
    date_str = start_dt.strftime("%Y-%m-%d")
    hour_int = int(start_dt.strftime("%H"))
    deadline_dt = end_dt + timedelta(minutes=30)

    try:
        min_rows_required = int(os.getenv("HEATMAP_MIN_ROWS_REQUIRED", "30") or 30)
    except Exception:
        min_rows_required = 30

    try:
        max_calls = int(os.getenv("FB_MAX_CALLS_PER_ATTEMPT", "1") or 1)
    except Exception:
        max_calls = 1
    max_calls = 1

    try:
        max_attempts = int(os.getenv("FB_MAX_ATTEMPTS_PER_HOUR", "3") or 3)
    except Exception:
        max_attempts = 3
    if max_attempts < 1:
        max_attempts = 1

    log = logging.getLogger(__name__)

    accounts = load_accounts() or {}
    if manual_aid:
        accounts = {str(manual_aid): accounts.get(str(manual_aid))}

    for aid, row in (accounts or {}).items():
        if not row:
            continue
        if not (row or {}).get("enabled", True):
            continue

        snap = load_snapshot(str(aid), date_str=date_str, hour=hour_int)
        if not snap:
            snap = build_snapshot_shell(
                str(aid),
                date_str=date_str,
                hour=hour_int,
                start_dt=start_dt,
                end_dt=end_dt,
                deadline_dt=deadline_dt,
                min_rows_required=min_rows_required,
            )

        # Ensure snapshot schema fields exist.
        snap["source"] = "heatmap_cache"
        if "reason" not in snap:
            snap["reason"] = "snapshot_collecting"
        if "rows_count" not in snap:
            try:
                snap["rows_count"] = int(len(snap.get("rows") or []))
            except Exception:
                snap["rows_count"] = 0
        if "spend" not in snap:
            snap["spend"] = 0.0

        if str((snap or {}).get("status") or "") == "ready":
            continue

        attempt_next = int((snap or {}).get("attempts") or 0) + 1
        try:
            window_label = f"{start_dt.strftime('%Y-%m-%d %H:00')}–{end_dt.strftime('%H:00')}"
        except Exception:
            window_label = f"{date_str} {hour_int:02d}:00–{(hour_int + 1) % 24:02d}:00"

        log.info(
            "🟦 FB COLLECTOR START aid=%s window=%s attempt=%s/%s allow_fb_api_calls=TRUE",
            str(aid),
            str(window_label),
            str(attempt_next),
            str(max_attempts),
        )

        try:
            dl_raw = (snap or {}).get("deadline_at")
            dl = datetime.fromisoformat(str(dl_raw)) if dl_raw else deadline_dt
        except Exception:
            dl = deadline_dt

        if now > dl:
            err = (snap or {}).get("error")
            if not isinstance(err, dict):
                err = {"type": "api_error"}

            has_any = False
            try:
                has_any = bool(snap.get("rows")) and float(snap.get("spend") or 0.0) > 0.0
            except Exception:
                has_any = False

            if has_any:
                snap["status"] = "ready_low_confidence"
                snap["reason"] = "low_volume"
                snap["error"] = None
                snap["next_try_at"] = None
            else:
                snap["status"] = "failed"
                et = str((err or {}).get("type") or "api_error")
                snap["reason"] = "rate_limit" if et == "rate_limit" else "snapshot_failed"
                snap["error"] = err
            save_snapshot(snap)

            hh = f"{int(hour_int):02d}"
            snap_path = os.path.join(
                str(os.getenv("DATA_DIR", "")) or "DATA_DIR",
                "heatmap_snapshots",
                str(aid),
                str(date_str),
                hh,
                "snapshot.json",
            )
            log.info(
                "🟦 SNAPSHOT SAVED aid=%s status=%s rows=%s spend=%s path=%s",
                str(aid),
                str(snap.get("status") or ""),
                str(int(snap.get("rows_count") or 0)),
                str(float(snap.get("spend") or 0.0)),
                str(snap_path),
            )
            continue
        try:
            ntry = str((snap or {}).get("next_try_at") or "")
            if ntry:
                ntd = datetime.fromisoformat(ntry)
                if not ntd.tzinfo:
                    ntd = ALMATY_TZ.localize(ntd)
                ntd = ntd.astimezone(ALMATY_TZ)
                if now < ntd:
                    mins = int(max(0.0, (ntd - now).total_seconds()) / 60.0)
                    log.info(
                        "🟦 FB COLLECTOR WAIT aid=%s until=%s in_min=%s",
                        str(aid),
                        str(ntd.isoformat()),
                        str(mins),
                    )
                    continue
        except Exception:
            pass
        try:
            attempts_done = int((snap or {}).get("attempts") or 0)
        except Exception:
            attempts_done = 0
        if attempts_done >= int(max_attempts):
            snap["status"] = "collecting"
            snap["reason"] = "snapshot_collecting"
            snap["error"] = {"type": "attempts_exceeded"}
            snap["last_try_at"] = snap.get("last_try_at")
            snap["next_try_at"] = snap.get("deadline_at")
            save_snapshot(snap)
            log.info(
                "🟦 FB COLLECTOR SKIP aid=%s reason=attempts_exceeded attempts=%s/%s",
                str(aid),
                str(attempts_done),
                str(max_attempts),
            )
            continue

        snap["status"] = "collecting"
        snap["reason"] = "snapshot_collecting"
        snap["last_try_at"] = now.isoformat()
        try:
            delay_min = random.randint(2, 5)
        except Exception:
            delay_min = 3
        try:
            snap["next_try_at"] = (now + timedelta(minutes=int(delay_min))).isoformat()
        except Exception:
            snap["next_try_at"] = None

        if is_rate_limited_now():
            info = get_last_api_error_info() or {}
            snap["error"] = {
                "type": "rate_limit",
                "fb_code": (info or {}).get("code") or 17,
                "fb_subcode": (info or {}).get("subcode"),
                "fbtrace_id": (info or {}).get("fbtrace_id"),
            }
            snap["reason"] = "rate_limit"
            save_snapshot(snap)

            try:
                log.warning(
                    "🟦 FB RATE LIMIT aid=%s retry_after=%ss",
                    str(aid),
                    str(rate_limit_retry_after_seconds()),
                )
            except Exception:
                pass
            continue

        try:
            snap["attempts"] = int((snap or {}).get("attempts") or 0) + 1
        except Exception:
            snap["attempts"] = 1

        calls_used = 0

        lead_action_type = None
        try:
            from fb_report.storage import get_lead_metric_for_account

            sel = get_lead_metric_for_account(str(aid))
            if isinstance(sel, dict):
                lead_action_type = sel.get("action_type")
        except Exception:
            lead_action_type = None

        data = None
        with allow_fb_api_calls(reason="heatmap_snapshot_collector"):
            if max_calls > 0:
                calls_used += 1
                try:
                    from facebook_business.adobjects.adaccount import AdAccount

                    acc = AdAccount(str(aid))
                    params = {
                        "level": "adset",
                        "time_range": {"since": date_str, "until": date_str},
                        "breakdowns": ["hourly_stats_aggregated_by_advertiser_time_zone"],
                        "time_increment": 1,
                    }
                    fields = [
                        "spend",
                        "actions",
                        "cost_per_action_type",
                        "impressions",
                        "clicks",
                        "frequency",
                        "adset_id",
                        "adset_name",
                        "campaign_id",
                        "campaign_name",
                    ]
                    data = safe_api_call(
                        acc.get_insights,
                        fields=fields,
                        params=params,
                        _meta={"endpoint": "insights/adset/hourly", "params": params},
                        _caller="heatmap_snapshot_collector",
                    )
                except Exception:
                    data = None

        rows_out: list[dict] = []
        if data:
            for rr in (data or []):
                d = _normalize_insight(rr)
                raw_hour = str((d or {}).get("hourly_stats_aggregated_by_advertiser_time_zone") or "")
                if not raw_hour:
                    continue
                hh_ok = raw_hour.startswith(f"{hour_int:02d}") or (f" {hour_int:02d}:" in raw_hour)
                if not hh_ok:
                    continue

                try:
                    parsed = parse_insight(d or {}, aid=str(aid), lead_action_type=lead_action_type)
                except Exception:
                    parsed = {"msgs": 0, "leads": 0, "total": 0, "spend": 0.0, "cpa": None}

                adset_id = str((d or {}).get("adset_id") or "")
                if not adset_id:
                    continue
                total = int(parsed.get("total") or 0)
                spend = float(parsed.get("spend") or 0.0)
                rows_out.append(
                    {
                        "adset_id": adset_id,
                        "name": (d or {}).get("adset_name") or (d or {}).get("name"),
                        "campaign_id": (d or {}).get("campaign_id"),
                        "campaign_name": (d or {}).get("campaign_name"),
                        "spend": spend,
                        "msgs": int(parsed.get("msgs") or 0),
                        "leads": int(parsed.get("leads") or 0),
                        "total": total,
                        "results": total,
                        "cpl": parsed.get("cpa"),
                        "hour": int(hour_int),
                    }
                )

        try:
            log.info(
                "🟦 FB RESPONSE aid=%s rows=%s spend=%s",
                str(aid),
                str(int(len(rows_out or []))),
                str(float(sum(float((r or {}).get("spend") or 0.0) for r in (rows_out or [])))),
            )
        except Exception:
            pass

        snap["rows"] = rows_out
        snap["collected_rows"] = int(len(rows_out))
        snap["rows_count"] = int(len(rows_out))
        try:
            snap["spend"] = float(sum(float((r or {}).get("spend") or 0.0) for r in (rows_out or [])))
        except Exception:
            snap["spend"] = 0.0

        try:
            rows_cnt = int(len(rows_out or []))
        except Exception:
            rows_cnt = 0
        try:
            spend_total = float(snap.get("spend") or 0.0)
        except Exception:
            spend_total = 0.0

        min_rows = int(snap.get("min_rows_required") or min_rows_required)

        if rows_cnt > 0 and spend_total > 0 and rows_cnt >= min_rows:
            snap["status"] = "ready"
            snap["reason"] = ""
            snap["next_try_at"] = None
            snap["error"] = None
        elif rows_cnt > 0 and spend_total > 0:
            snap["status"] = "ready_low_confidence"
            snap["reason"] = "low_volume"
            snap["next_try_at"] = None
            snap["error"] = None
        else:
            info = get_last_api_error_info() or {}
            et = "api_error"
            try:
                et = str(classify_api_error(info))
            except Exception:
                et = "api_error"
            snap["error"] = {
                "type": et,
                "fb_code": (info or {}).get("code"),
                "fb_subcode": (info or {}).get("subcode"),
                "fbtrace_id": (info or {}).get("fbtrace_id"),
            }
            snap["reason"] = "rate_limit" if et == "rate_limit" else "snapshot_collecting"

        save_snapshot(snap)

        hh = f"{int(hour_int):02d}"
        snap_path = os.path.join(
            str(os.getenv("DATA_DIR", "")) or "DATA_DIR",
            "heatmap_snapshots",
            str(aid),
            str(date_str),
            hh,
            "snapshot.json",
        )
        log.info(
            "🟦 SNAPSHOT SAVED aid=%s status=%s rows=%s spend=%s path=%s",
            str(aid),
            str(snap.get("status") or ""),
            str(int(snap.get("rows_count") or 0)),
            str(float(snap.get("spend") or 0.0)),
            str(snap_path),
        )

        if manual and manual_aid:
            # One attempt in manual mode.
            break


async def run_heatmap_snapshot_collector_once(
    context: ContextTypes.DEFAULT_TYPE,
    *,
    aid: str,
) -> None:
    await _heatmap_snapshot_collector_job(context, manual=True, manual_aid=str(aid))


async def _autopilot_heatmap_job(context: ContextTypes.DEFAULT_TYPE):
    chat_id, _src = _resolve_autopilot_chat_id_logged(reason="autopilot_heatmap_job")
    now = datetime.now(ALMATY_TZ)
    hour = int(now.strftime("%H"))

    # При rate limit не добиваем FB API и не применяем изменения — только пишем статус.
    if is_rate_limited_now():
        after_s = rate_limit_retry_after_seconds()
        mins = max(1, int(round(float(after_s) / 60.0)))
        stats_cache = load_hourly_stats() or {}

        lines = [
            "🤖 Heatmap AUTO_LIMITS — почасовой прогон",
            "⚠️ FB лимит запросов (code 17). Данных из API нет.",
            f"Повторю через ~{mins} мин.",
            "",
        ]

        accounts = load_accounts() or {}
        shown_any = False
        for aid, row in accounts.items():
            ap = (row or {}).get("autopilot") or {}
            if not isinstance(ap, dict):
                continue
            mode = str(ap.get("mode") or "OFF").upper()
            if mode != "AUTO_LIMITS":
                continue
            due, _due_meta = _ap_heatmap_due_meta(ap, now)
            if not due:
                continue

            acc_stats = stats_cache.get(aid) or {}
            has_cache = False
            try:
                has_cache = isinstance(acc_stats, dict) and any(str(k).startswith("20") for k in acc_stats.keys())
            except Exception:
                has_cache = False

            if has_cache:
                lines.append(f"🏢 {get_account_name(aid)}: 📌 данные из кэша (hourly_stats), обновлю позже")
            else:
                lines.append(f"🏢 {get_account_name(aid)}: heatmap недоступна из-за лимита API (кэша нет)")
            shown_any = True

        if not shown_any:
            lines.append("Нет аккаунтов с включённым Heatmap AUTO_LIMITS.")

        try:
            await context.bot.send_message(chat_id, "\n".join(lines))
        except Exception:
            pass

        try:
            token = str(uuid.uuid4().hex[:8])
            jitter = int(uuid.uuid4().int % 6)
            context.job_queue.run_once(
                _autopilot_heatmap_job,
                when=timedelta(minutes=max(5, min(35, mins + jitter))),
                name=f"autopilot_heatmap_retry_{token}",
            )
        except Exception:
            pass
        return

    accounts = load_accounts() or {}

    for aid, row in accounts.items():
        # Если во время цикла словили rate limit — прекращаем, чтобы не добивать API.
        if is_rate_limited_now():
            break

        ap = (row or {}).get("autopilot") or {}
        if not isinstance(ap, dict):
            continue

        mode = str(ap.get("mode") or "OFF").upper()
        if mode != "AUTO_LIMITS":
            continue

        due, due_meta = _ap_heatmap_due_meta(ap, now)
        if not due:
            append_autopilot_event(
                aid,
                {
                    "type": "heatmap_auto_skipped",
                    "hour": hour,
                    "reason": "frequency_limit",
                    "frequency": due_meta,
                    "chat_id": chat_id,
                },
            )
            continue

        goals = ap.get("goals") or {}
        limits = ap.get("limits") or {}

        allow_redist = bool((limits or {}).get("allow_redistribute", True))
        if not allow_redist:
            continue

        try:
            max_step = float((limits or {}).get("max_budget_step_pct") or 20)
        except Exception:
            max_step = 20.0
        if max_step <= 0:
            max_step = 20.0

        try:
            max_risk = float((limits or {}).get("max_daily_risk_pct") or 0)
        except Exception:
            max_risk = 0.0
        if max_risk < 0:
            max_risk = 0.0

        # Строим профиль лучших/слабых часов на основе 7 дней.
        try:
            _txt, summary = build_hourly_heatmap_for_account(aid, get_account_name_fn=get_account_name, mode="7d")
        except Exception:
            summary = {}

        top_hours, low_hours = _ap_heatmap_profile(summary or {})
        if not top_hours and not low_hours:
            continue

        is_top = hour in set(top_hours)
        is_low = hour in set(low_hours)

        hour_tag = "TOP" if is_top else ("LOW" if is_low else "NEUTRAL")

        # Anti-panic:
        # если сегодня плохо, но 3d ок — не делаем агрессивных увеличений.
        target = (goals or {}).get("target_cpl")
        try:
            target_f = float(target) if target not in (None, "") else None
        except Exception:
            target_f = None

        planned_daily_for_antipanic = _ap_daily_budget_limit_usd(goals, now)

        today_ins = None
        try:
            today_ins = fetch_insights(aid, "today") or {}
        except Exception:
            today_ins = {}
        today_m = parse_insight(today_ins or {}, aid=aid)
        try:
            if int((get_last_api_error_info() or {}).get("code") or 0) == 17:
                cache_mode = True
        except Exception:
            pass
        today_cpa = today_m.get("cpa")
        spend_today = float(today_m.get("spend") or 0.0)
        conv_today = int(today_m.get("total") or 0)

        spend_min = 20.0
        if planned_daily_for_antipanic is not None and planned_daily_for_antipanic > 0:
            spend_min = max(float(spend_min), float(planned_daily_for_antipanic) * 0.5)

        antipanic_data_ok = bool(spend_today >= float(spend_min) and conv_today >= 2)

        # rolling 3d: yday-2..yday
        yday = (now - timedelta(days=1)).date()
        period_3d = {
            "since": (yday - timedelta(days=2)).strftime("%Y-%m-%d"),
            "until": yday.strftime("%Y-%m-%d"),
        }
        acc_3d = analyze_account(aid, period=period_3d) or {}
        cpa_3d = ((acc_3d.get("metrics") or {}) if isinstance(acc_3d, dict) else {}).get("cpa")

        aggressive = False
        if (
            antipanic_data_ok
            and target_f is not None
            and isinstance(today_cpa, (int, float))
            and today_cpa is not None
        ):
            if today_cpa >= float(target_f) * 4.0:
                aggressive = True

        soft_mode = False
        if (
            antipanic_data_ok
            and (not aggressive)
            and isinstance(today_cpa, (int, float))
            and isinstance(cpa_3d, (int, float))
        ):
            if today_cpa > float(cpa_3d) * 1.25:
                soft_mode = True

        antipanic_mode = "NORMAL"
        if aggressive:
            antipanic_mode = "HARD"
        elif soft_mode:
            antipanic_mode = "SOFT"

        step_eff = float(max_step)
        if aggressive:
            step_eff = min(35.0, float(max_step) * 1.5)
        elif soft_mode:
            step_eff = max(5.0, float(max_step) / 2.0)

        # Текущие бюджеты — база.
        adsets = fetch_adsets(aid) or []
        adset_name = {}
        active = []
        active_ids = []
        for a in adsets:
            st = str((a or {}).get("effective_status") or (a or {}).get("status") or "").upper()
            if st in {"ACTIVE", "SCHEDULED"}:
                active.append(a)
                _id = str((a or {}).get("id") or "")
                adset_name[_id] = str((a or {}).get("name") or "")
                if _id:
                    active_ids.append(_id)

        if not active:
            continue

        current_total = sum(float((a or {}).get("daily_budget") or 0.0) for a in active)
        if current_total <= 0:
            continue

        ap_state = ap.get("heatmap_state") or {}
        if not isinstance(ap_state, dict):
            ap_state = {}

        date_key = now.strftime("%Y-%m-%d")
        base_date = str(ap_state.get("baseline_date") or "")
        base_total = ap_state.get("baseline_total")
        try:
            base_total_f = float(base_total) if base_total not in (None, "") else None
        except Exception:
            base_total_f = None

        if base_date != date_key or base_total_f is None or base_total_f <= 0:
            base_total_f = float(current_total)
            ap_state["baseline_date"] = date_key
            ap_state["baseline_total"] = float(base_total_f)

        # planned_budget: потолок total_budget (через daily_limit)
        daily_limit = _ap_daily_budget_limit_usd(goals, now)
        total_cap = float(base_total_f)
        if daily_limit is not None and daily_limit > 0:
            total_cap = min(float(total_cap), float(daily_limit))

        allow_increase = True
        blocked_by_risk = False
        blocked_by_planned = False
        if daily_limit is not None and daily_limit > 0:
            if spend_today > float(daily_limit) * (1.0 + float(max_risk) / 100.0):
                allow_increase = False
                blocked_by_risk = True

        # Если weekly/month planned и уже выбрали план — тоже запрещаем увеличение.
        planned_total, period_range = _ap_period_spend_limit(goals, now)
        if period_range and planned_total is not None:
            try:
                ins_p = fetch_insights(aid, period_range) or {}
            except Exception:
                ins_p = {}
            spend_p = float((ins_p or {}).get("spend", 0) or 0)
            if spend_p >= float(planned_total):
                allow_increase = False
                blocked_by_planned = True

        # В soft_mode вообще не делаем увеличений (только ужимаем слабые часы).
        if soft_mode or aggressive:
            allow_increase = False

        if (
            (not allow_increase)
            and (not soft_mode)
            and (not aggressive)
            and (blocked_by_planned or blocked_by_risk)
            and _ap_heatmap_force_active(ap, now)
        ):
            allow_increase = True

        force_active = _ap_heatmap_force_active(ap, now)
        force_until = _ap_heatmap_force_until(ap)

        # Определяем кандидатов good/bad по сегодняшнему CPL vs 3d (через analyze_adsets).
        # NB: analyze_adsets может быть тяжёлым, но это hourly job и список ограничен.
        good_ids: set[str] = set()
        bad_ids: set[str] = set()
        try:
            rows_main = analyze_adsets(aid, period="last_3d", adset_ids=list(active_ids)) or []
            rows_base = analyze_adsets(aid, period="last_7d", adset_ids=list(active_ids)) or []

            if not rows_main or len(rows_main) < 3:
                rows_main = analyze_adsets(aid, period="today", adset_ids=list(active_ids)) or []
            if not rows_base:
                rows_base = list(rows_main or [])

            map_base = {str(r.get("id") or ""): r for r in rows_base if r}
            for r in rows_main:
                adset_id = str((r or {}).get("id") or "")
                if not adset_id:
                    continue
                cpl_t = float((r or {}).get("cpl") or 0.0)
                cpl_b = float((map_base.get(adset_id) or {}).get("cpl") or 0.0)
                if cpl_t <= 0 or cpl_b <= 0:
                    continue
                if target_f is not None and target_f > 0:
                    if cpl_t <= float(target_f) * 1.05 and cpl_t <= cpl_b * 0.95:
                        good_ids.add(adset_id)
                    elif cpl_t >= float(target_f) * 1.5 and cpl_t >= cpl_b * 1.15:
                        bad_ids.add(adset_id)
                else:
                    if cpl_t <= cpl_b * 0.90:
                        good_ids.add(adset_id)
                    elif cpl_t >= cpl_b * 1.20:
                        bad_ids.add(adset_id)
        except Exception:
            good_ids = set()
            bad_ids = set()

        try:
            if int((get_last_api_error_info() or {}).get("code") or 0) == 17:
                cache_mode = True
        except Exception:
            pass

        stats_cache = load_hourly_stats() or {}
        hour_key = f"{hour:02d}"

        hourly_good: set[str] = set()
        hourly_bad: set[str] = set()
        hourly_meta: dict = {"used": False}
        try:
            cpa_3d_f = float(cpa_3d) if isinstance(cpa_3d, (int, float)) else None
        except Exception:
            cpa_3d_f = None

        if active_ids:
            hourly_good, hourly_bad, hourly_meta = _ap_select_hourly_good_bad_adsets(
                stats_cache,
                aid=str(aid),
                adset_ids=list(active_ids),
                now=now,
                hour_key=hour_key,
                target_cpl=target_f,
                cpa_3d=cpa_3d_f,
            )

        use_hourly = bool(hourly_meta.get("used")) and (is_top or is_low)
        if use_hourly:
            good_ids = set(hourly_good)
            bad_ids = set(hourly_bad)

        changes: list[dict] = []

        if is_low:
            for a in active:
                adset_id = str((a or {}).get("id") or "")
                old_b = float((a or {}).get("daily_budget") or 0.0)
                if use_hourly and adset_id in good_ids:
                    factor = 1.0 - (float(step_eff) * 0.50) / 100.0
                elif use_hourly and adset_id in bad_ids:
                    factor = 1.0 - float(step_eff) / 100.0
                elif use_hourly:
                    factor = 1.0 - (float(step_eff) * 0.75) / 100.0
                else:
                    factor = 1.0 - float(step_eff) / 100.0
                new_b = max(1.0, old_b * float(factor))
                changes.append({"adset_id": adset_id, "old": old_b, "new": new_b})

        elif is_top:
            # если нельзя увеличивать — в топ-час просто не трогаем
            if not allow_increase:
                if soft_mode or aggressive:
                    append_autopilot_event(
                        aid,
                        {
                            "type": "heatmap_auto_skipped",
                            "hour": hour,
                            "hour_tag": hour_tag,
                            "reason": "antipanic_no_increase",
                            "antipanic": {
                                "mode": antipanic_mode,
                                "data_ok": bool(antipanic_data_ok),
                                "spend_today": float(spend_today),
                                "spend_min": float(spend_min),
                                "conversions_today": int(conv_today),
                                "today_cpa": float(today_cpa) if isinstance(today_cpa, (int, float)) else None,
                                "cpa_3d": float(cpa_3d) if isinstance(cpa_3d, (int, float)) else None,
                                "target_cpl": float(target_f) if target_f is not None else None,
                                "step_eff": float(step_eff),
                            },
                            "chat_id": chat_id,
                        },
                    )
                    continue

                if blocked_by_planned or blocked_by_risk:
                    if _ap_force_prompt_due(ap, now, minutes=60):
                        ap_state = ap.get("heatmap_state") or {}
                        if not isinstance(ap_state, dict):
                            ap_state = {}
                        ap_state["last_force_prompt"] = now.isoformat()
                        ap["heatmap_state"] = ap_state
                        row["autopilot"] = ap
                        accounts[aid] = row

                        reason_label = ""
                        if blocked_by_planned and blocked_by_risk:
                            reason_label = "planned_budget + daily_limit"
                        elif blocked_by_planned:
                            reason_label = "planned_budget"
                        elif blocked_by_risk:
                            reason_label = "daily_limit"

                        msg = f"⚠️ Heatmap: упёрся в {reason_label} для {get_account_name(aid)}. Нужно одобрение, чтобы продолжать увеличения."
                        try:
                            if _ap_force_button_allowed(now):
                                await context.bot.send_message(chat_id, msg, reply_markup=_heatmap_force_kb(aid))
                            else:
                                await context.bot.send_message(chat_id, msg)
                            await asyncio.sleep(0.3)
                        except Exception:
                            pass

                        append_autopilot_event(
                            aid,
                            {
                                "type": "heatmap_force_needed",
                                "hour": hour,
                                "hour_tag": hour_tag,
                                "top_hours": top_hours,
                                "low_hours": low_hours,
                                "chat_id": chat_id,
                                "button_allowed": bool(_ap_force_button_allowed(now)),
                                "blocked": {
                                    "risk": bool(blocked_by_risk),
                                    "planned": bool(blocked_by_planned),
                                    "soft_mode": bool(soft_mode),
                                    "hard_mode": bool(aggressive),
                                    "force_active": bool(force_active),
                                    "force_until": force_until,
                                },
                                "caps": {
                                    "current_total": float(current_total),
                                    "baseline_total": float(base_total_f),
                                    "daily_limit": float(daily_limit) if daily_limit is not None else None,
                                    "planned_total": float(planned_total) if planned_total is not None else None,
                                    "total_cap": float(total_cap),
                                },
                                "antipanic": {
                                    "mode": antipanic_mode,
                                    "data_ok": bool(antipanic_data_ok),
                                    "spend_today": float(spend_today),
                                    "spend_min": float(spend_min),
                                    "conversions_today": int(conv_today),
                                    "today_cpa": float(today_cpa) if isinstance(today_cpa, (int, float)) else None,
                                    "cpa_3d": float(cpa_3d) if isinstance(cpa_3d, (int, float)) else None,
                                    "target_cpl": float(target_f) if target_f is not None else None,
                                    "step_eff": float(step_eff),
                                },
                            },
                        )
                    else:
                        append_autopilot_event(
                            aid,
                            {
                                "type": "heatmap_auto_skipped",
                                "hour": hour,
                                "hour_tag": hour_tag,
                                "reason": "force_prompt_cooldown",
                                "blocked": {
                                    "risk": bool(blocked_by_risk),
                                    "planned": bool(blocked_by_planned),
                                },
                                "chat_id": chat_id,
                            },
                        )
                continue

            if not good_ids:
                factor = 1.0 + float(step_eff) / 100.0
                for a in active:
                    adset_id = str((a or {}).get("id") or "")
                    old_b = float((a or {}).get("daily_budget") or 0.0)
                    new_b = max(1.0, old_b * factor)
                    changes.append({"adset_id": adset_id, "old": old_b, "new": new_b})
            else:
                # Ужимаем bad (если есть), увеличиваем good
                for a in active:
                    adset_id = str((a or {}).get("id") or "")
                    old_b = float((a or {}).get("daily_budget") or 0.0)
                    if adset_id in bad_ids:
                        new_b = max(1.0, old_b * (1.0 - float(step_eff) / 100.0))
                    elif adset_id in good_ids:
                        new_b = max(1.0, old_b * (1.0 + float(step_eff) / 100.0))
                    else:
                        new_b = old_b
                    changes.append({"adset_id": adset_id, "old": old_b, "new": new_b})
        else:
            continue

        blocked_by_cut_guard = False
        cut_guard = {
            "enabled": True,
            "max_cut_pct": 35.0,
            "cut_used": 0.0,
            "cut_max": float(base_total_f) * 0.35,
            "cut_planned": 0.0,
            "cut_applied": 0.0,
            "cut_remaining": 0.0,
        }

        try:
            cut_date = str(ap_state.get("cut_date") or "")
            cut_used = float(ap_state.get("cut_used") or 0.0)
        except Exception:
            cut_date = ""
            cut_used = 0.0
        if cut_date != date_key:
            cut_used = 0.0
            ap_state["cut_date"] = date_key
            ap_state["cut_used"] = 0.0

        cut_guard["cut_used"] = float(cut_used)
        cut_guard["cut_remaining"] = max(0.0, float(cut_guard["cut_max"]) - float(cut_used))

        new_total_before_scale = sum(c["new"] for c in changes)
        old_total = sum(c["old"] for c in changes)
        cut_planned = max(0.0, float(old_total) - float(new_total_before_scale))
        cut_guard["cut_planned"] = float(cut_planned)

        if cut_planned > 0:
            remaining = float(cut_guard["cut_remaining"])
            if remaining <= 0.0:
                blocked_by_cut_guard = True
            elif cut_planned > remaining:
                k = float(remaining) / float(cut_planned)
                for c in changes:
                    old_b = float(c["old"])
                    new_b = float(c["new"])
                    if new_b < old_b:
                        c["new"] = max(1.0, old_b - (old_b - new_b) * k)

                new_total_before_scale = sum(c["new"] for c in changes)
                cut_guard["cut_applied"] = float(max(0.0, float(old_total) - float(new_total_before_scale)))
                ap_state["cut_used"] = float(cut_used) + float(cut_guard["cut_applied"])
                cut_guard["cut_used"] = float(ap_state.get("cut_used") or 0.0)
                cut_guard["cut_remaining"] = max(0.0, float(cut_guard["cut_max"]) - float(cut_guard["cut_used"]))
            else:
                cut_guard["cut_applied"] = float(cut_planned)
                ap_state["cut_used"] = float(cut_used) + float(cut_planned)
                cut_guard["cut_used"] = float(ap_state.get("cut_used") or 0.0)
                cut_guard["cut_remaining"] = max(0.0, float(cut_guard["cut_max"]) - float(cut_guard["cut_used"]))

        if blocked_by_cut_guard:
            ap["heatmap_state"] = ap_state
            row["autopilot"] = ap
            accounts[aid] = row

            append_autopilot_event(
                aid,
                {
                    "type": "heatmap_auto_blocked",
                    "hour": hour,
                    "hour_tag": hour_tag,
                    "reason": "daily_cut_guard",
                    "cut_guard": cut_guard,
                    "antipanic": {
                        "mode": antipanic_mode,
                        "data_ok": bool(antipanic_data_ok),
                        "spend_today": float(spend_today),
                        "spend_min": float(spend_min),
                        "conversions_today": int(conv_today),
                        "today_cpa": float(today_cpa) if isinstance(today_cpa, (int, float)) else None,
                        "cpa_3d": float(cpa_3d) if isinstance(cpa_3d, (int, float)) else None,
                        "target_cpl": float(target_f) if target_f is not None else None,
                    },
                    "chat_id": chat_id,
                },
            )

            title = f"🤖 Heatmap AUTO_LIMITS: {get_account_name(aid)}"
            reason = f"Час {hour:02d}:00 ({hour_tag}). Top={','.join([f'{h:02d}' for h in top_hours]) or '-'}; Low={','.join([f'{h:02d}' for h in low_hours]) or '-'}"
            mode_line = "Режим: AUTO_LIMITS"
            anti = f"Anti-panic: {antipanic_mode} (data_ok={bool(antipanic_data_ok)} spend_min={float(spend_min):.2f}$ conv_today={int(conv_today)})"
            cap_lines = [
                f"Caps: current={float(current_total):.2f}$ baseline={float(base_total_f):.2f}$ cap={float(total_cap):.2f}$",
                f"CutGuard: used={float(cut_guard['cut_used']):.2f}$ max={float(cut_guard['cut_max']):.2f}$ planned_cut={float(cut_guard['cut_planned']):.2f}$",
                "Blocked: daily_cut_guard",
            ]
            try:
                await context.bot.send_message(chat_id, "\n".join([title, mode_line, anti, reason] + cap_lines))
                await asyncio.sleep(0.3)
            except Exception:
                pass
            continue

        new_total = float(new_total_before_scale)
        if new_total <= 0:
            continue

        scale = 1.0
        if new_total > float(total_cap):
            scale = float(total_cap) / float(new_total)
            for c in changes:
                c["new"] = max(1.0, float(c["new"]) * scale)

        new_total_after_scale = sum(c["new"] for c in changes)

        applied = []
        date_key = now.strftime("%Y-%m-%d")
        for c in changes:
            adset_id = c["adset_id"]
            old_b = float(c["old"])
            new_b = float(c["new"])
            if abs(new_b - old_b) < 0.5:
                continue

            res = set_adset_budget(adset_id, new_b)

            hm_today = _ap_hourly_bucket(
                stats_cache,
                section="_adset",
                aid=aid,
                entity_id=adset_id,
                date_key=date_key,
                hour_key=hour_key,
            )
            today_spend = float((hm_today or {}).get("spend", 0.0) or 0.0)
            today_total = int((hm_today or {}).get("total", 0) or 0)
            today_cpl = (today_spend / float(today_total)) if today_total > 0 and today_spend > 0 else None

            hm_7d = _ap_hourly_agg(
                stats_cache,
                section="_adset",
                aid=aid,
                entity_id=adset_id,
                now=now,
                hour_key=hour_key,
                days=7,
            )
            worst_ad = _ap_find_worst_ad_in_hour(
                stats_cache,
                aid=aid,
                adset_id=adset_id,
                now=now,
                hour_key=hour_key,
            )

            applied.append(
                {
                    "adset_id": adset_id,
                    "adset_name": adset_name.get(str(adset_id), ""),
                    "old": old_b,
                    "new": new_b,
                    "status": res.get("status"),
                    "msg": res.get("message"),
                    "hour": hour,
                    "hm_today": {"spend": today_spend, "total": today_total, "cpl": today_cpl},
                    "hm_7d": hm_7d,
                    "worst_ad": worst_ad,
                    "decision": {"use_hourly": bool(use_hourly), "hourly_meta": hourly_meta},
                }
            )

        if not applied:
            continue

        ap_state["last_apply"] = now.isoformat()
        ap["heatmap_state"] = ap_state
        row["autopilot"] = ap
        accounts[aid] = row

        append_autopilot_event(
            aid,
            {
                "type": "heatmap_auto_apply",
                "hour": hour,
                "hour_tag": hour_tag,
                "top_hours": top_hours,
                "low_hours": low_hours,
                "soft_mode": bool(soft_mode),
                "aggressive": bool(aggressive),
                "antipanic": {
                    "mode": antipanic_mode,
                    "data_ok": bool(antipanic_data_ok),
                    "spend_today": float(spend_today),
                    "spend_min": float(spend_min),
                    "conversions_today": int(conv_today),
                    "today_cpa": float(today_cpa) if isinstance(today_cpa, (int, float)) else None,
                    "cpa_3d": float(cpa_3d) if isinstance(cpa_3d, (int, float)) else None,
                    "target_cpl": float(target_f) if target_f is not None else None,
                    "step_eff": float(step_eff),
                },
                "blocked": {
                    "risk": bool(blocked_by_risk),
                    "planned": bool(blocked_by_planned),
                    "soft_mode": bool(soft_mode),
                    "hard_mode": bool(aggressive),
                    "force_active": bool(force_active),
                    "force_until": force_until,
                },
                "caps": {
                    "current_total": float(current_total),
                    "baseline_total": float(base_total_f),
                    "daily_limit": float(daily_limit) if daily_limit is not None else None,
                    "planned_total": float(planned_total) if planned_total is not None else None,
                    "total_cap": float(total_cap),
                    "new_total_before_scale": float(new_total_before_scale),
                    "scale": float(scale),
                    "new_total_after_scale": float(new_total_after_scale),
                },
                "cut_guard": cut_guard,
                "decision": {"use_hourly": bool(use_hourly), "hourly_meta": hourly_meta},
                "applied": applied,
                "chat_id": chat_id,
            },
        )

        title = f"🤖 Heatmap AUTO_LIMITS: {get_account_name(aid)}"
        reason = f"Час {hour:02d}:00 ({hour_tag}). Top={','.join([f'{h:02d}' for h in top_hours]) or '-'}; Low={','.join([f'{h:02d}' for h in low_hours]) or '-'}"
        mode_line = "Режим: AUTO_LIMITS"
        anti = f"Anti-panic: {antipanic_mode} (data_ok={bool(antipanic_data_ok)} spend_min={float(spend_min):.2f}$ conv_today={int(conv_today)} step={float(step_eff):.1f}%)"

        blocked = []
        if blocked_by_risk:
            blocked.append("risk")
        if blocked_by_planned:
            blocked.append("planned")
        if soft_mode:
            blocked.append("soft_mode")
        if aggressive:
            blocked.append("hard_mode")
        if (not blocked) and force_active:
            blocked.append("force_active")

        cap_lines = [
            f"Caps: current={float(current_total):.2f}$ baseline={float(base_total_f):.2f}$ cap={float(total_cap):.2f}$",
            f"Plan: daily_limit={(float(daily_limit) if daily_limit is not None else None)} planned_total={(float(planned_total) if planned_total is not None else None)}",
            f"Scale: before={float(new_total_before_scale):.2f}$ scale={float(scale):.3f} after={float(new_total_after_scale):.2f}$",
        ]

        cap_lines.append(
            f"CutGuard: used={float(cut_guard['cut_used']):.2f}$ max={float(cut_guard['cut_max']):.2f}$ applied={float(cut_guard.get('cut_applied') or 0.0):.2f}$ remaining={float(cut_guard['cut_remaining']):.2f}$"
        )

        if force_active and force_until:
            cap_lines.append(f"Force: active_until={force_until}")
        if blocked:
            cap_lines.append(f"Blocked: {', '.join(blocked)}")

        if use_hourly:
            cap_lines.append(
                f"Decision: hourly_adset used (k={hourly_meta.get('k')}, rows={hourly_meta.get('count')})"
            )
        else:
            cap_lines.append("Decision: fallback (today vs 3d)")

        lines = [title, mode_line, anti, reason]
        lines.extend(cap_lines)
        lines.append("")
        lines.append("Изменения бюджетов:")
        for a in applied[:25]:
            nm = str(a.get("adset_name") or "").strip()
            act = {
                "kind": "budget_abs",
                "adset_id": str(a.get("adset_id") or ""),
                "name": nm or str(a.get("adset_id") or ""),
                "old_budget": float(a.get("old") or 0.0),
                "new_budget": float(a.get("new") or 0.0),
                "reason": f"heatmap hour={hour:02d}:00 ({hour_tag})",
            }
            # Переиспользуем автопилотный формат сообщений (единый UX).
            try:
                lines.append(ap_action_text(act))
            except Exception:
                lines.append(
                    f"- {act['adset_id']}: {float(act.get('old_budget') or 0.0):.2f} → {float(act.get('new_budget') or 0.0):.2f} $"
                )

            ht = a.get("hm_today") or {}
            h7 = a.get("hm_7d") or {}

            t_total = int((ht or {}).get("total", 0) or 0)
            t_spend = float((ht or {}).get("spend", 0.0) or 0.0)
            t_cpl = (ht or {}).get("cpl")
            t_cpl_s = f"{float(t_cpl):.2f}$" if isinstance(t_cpl, (int, float)) else "—"

            s7_total = int((h7 or {}).get("total", 0) or 0)
            s7_spend = float((h7 or {}).get("spend", 0.0) or 0.0)
            s7_cpl = (h7 or {}).get("cpl")
            s7_cpl_s = f"{float(s7_cpl):.2f}$" if isinstance(s7_cpl, (int, float)) else "—"

            lines.append(
                f"  почему: hour {hour:02d}:00 — today {t_total} conv, {t_spend:.2f}$, CPL {t_cpl_s}; "
                f"7d(hour) {s7_total} conv, {s7_spend:.2f}$, CPL {s7_cpl_s}"
            )

            wa = a.get("worst_ad") or {}
            if wa.get("ad_id"):
                wa_sp = float(wa.get("spend", 0.0) or 0.0)
                wa_t = int(wa.get("total", 0) or 0)
                wa_ct = wa.get("cpl_today")
                wa_c7 = wa.get("cpl_7d")
                wa_r = wa.get("ratio")
                wa_ct_s = f"{float(wa_ct):.2f}$" if isinstance(wa_ct, (int, float)) else "—"
                wa_c7_s = f"{float(wa_c7):.2f}$" if isinstance(wa_c7, (int, float)) else "—"
                wa_r_s = f"{float(wa_r):.2f}×" if isinstance(wa_r, (int, float)) else "—"
                lines.append(
                    f"  ad перегрев: {wa['ad_id']} — {wa_t} conv, {wa_sp:.2f}$, CPL {wa_ct_s} vs 7d(hour) {wa_c7_s} ({wa_r_s})"
                )
        if len(applied) > 25:
            lines.append(f"… ещё {len(applied) - 25} adset")

        try:
            await context.bot.send_message(chat_id, "\n".join(lines))
            await asyncio.sleep(0.3)
        except Exception:
            pass

    # Сохраняем обновлённый accounts (last_apply)
    try:
        from .storage import save_accounts

        save_accounts(accounts)
    except Exception:
        pass

    logging.getLogger(__name__).info("🟢 Autopilot heatmap job executed successfully")


def _is_day_enabled(alerts: dict, now: datetime) -> bool:
    days = alerts.get("days") or []
    if not days:
        return False
    key = WEEKDAY_KEYS[now.weekday()]
    return key in days


def _resolve_account_cpa(alerts: dict) -> float:
    """Возвращает таргет CPA для аккаунта с приоритетом новой схемы.

    1) alerts["account_cpa"]
    2) alerts["target_cpl"] (старое поле)
    3) глобальный дефолт 3.0
    """

    acc_cpa = float(alerts.get("account_cpa", 0.0) or 0.0)
    if acc_cpa > 0:
        return acc_cpa
    old = float(alerts.get("target_cpl", 0.0) or 0.0)
    if old > 0:
        return old
    return 3.0


def build_heatmap_status_text(*, aid: str, now: datetime | None = None) -> str:
    now = now or datetime.now(ALMATY_TZ)
    win = prev_full_hour_window(now=now) or {}
    date_str = str(win.get("date") or "")
    hour_int = int(win.get("hour") or 0)
    window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"

    snap = load_snapshot(str(aid), date_str=date_str, hour=hour_int) or {}
    status = str(snap.get("status") or "missing")
    reason = str(snap.get("reason") or "no_snapshot")
    attempts = int(snap.get("attempts") or 0)
    rows = int(snap.get("rows_count") or 0)
    spend = float(snap.get("spend") or 0.0)
    last_try = str(snap.get("last_try_at") or "")
    next_try = str(snap.get("next_try_at") or "")

    fb_requests = "были" if attempts > 0 else "не были"
    last_collector = "OK" if status in {"ready", "ready_low_confidence"} else "FAIL"

    lines = [
        "🟦 Heatmap Status",
        f"aid={str(aid)}",
        f"Окно: {date_str} {window_label}",
        f"Последний collector запуск: {last_collector}",
        f"FB запросы: {fb_requests}",
        "Snapshot:",
        f"  status={status}",
        f"  reason={reason}",
        f"  rows={rows}",
        f"  spend={spend:.2f}",
        f"  attempts={attempts}",
        f"  last_try={last_try}",
    ]
    if next_try:
        lines.append(f"  next_try={next_try}")
    return "\n".join(lines)


async def _cpa_alerts_job(context: ContextTypes.DEFAULT_TYPE):
    """CPA-алёрты по новой схеме частоты и дней недели.

    Уровни:
    - аккаунт: глобальный переключатель alerts["enabled"], дни и частота;
    - адсет: adset_alerts[adset_id] с приоритетным target_cpa.

    Поведение по умолчанию (обратная совместимость):
    - если adset_alerts пустой, используется только account_cpa как раньше.
    """

    now = datetime.now(ALMATY_TZ)
    accounts = load_accounts() or {}

    # Алёрты шлём напрямую владельцу в личку (первый ID из ALLOWED_USER_IDS).
    # Если по какой-то причине список пуст, используем дефолтный чат как фолбэк.
    owner_id = None
    try:
        owner_id = next(iter(ALLOWED_USER_IDS))
    except StopIteration:
        owner_id = None

    chat_id = owner_id if owner_id is not None else str(DEFAULT_REPORT_CHAT)

    with deny_fb_api_calls(reason="cpa_alerts_job"):
        await _cpa_alerts_job_snapshots_only(
            context,
            now=now,
            accounts=accounts,
            chat_id=str(chat_id),
        )
    return

    # Для алёрта берём текущее состояние за today,
    # чтобы видеть актуальный CPA на момент часа.
    period = "today"
    label = now.strftime("%d.%m.%Y")
    period_dict = {
        "since": now.strftime("%Y-%m-%d"),
        "until": now.strftime("%Y-%m-%d"),
    }

    for aid, row in accounts.items():
        alerts = (row or {}).get("alerts") or {}
        if not isinstance(alerts, dict):
            alerts = {}

        # Глобальный флаг включения алёртов по аккаунту
        if not bool(alerts.get("enabled", False)):
            continue

        # Проверяем, включён ли текущий день недели
        if not _is_day_enabled(alerts, now):
            continue

        freq = alerts.get("freq", "3x")

        if freq == "3x":
            # Срабатываем только в определённые времена.
            # Округляем текущее время до минуты и сравниваем с допустимыми слотами.
            current_time = now.replace(second=0, microsecond=0).time()
            if current_time not in CPA_ALERT_TIMES:
                continue
        elif freq == "hourly":
            # Каждый час в окне 10–22
            if not (CPA_HOURLY_START <= now.hour <= CPA_HOURLY_END):
                continue
        else:
            # Неизвестный режим частоты — пропускаем аккаунт
            continue

        # Таргет на уровне аккаунта остаётся как базовый.
        # ВАЖНО: даже если он <= 0, мы всё равно продолжаем обработку,
        # чтобы объекты с собственным target_cpa могли работать независимо.
        account_target = _resolve_account_cpa(alerts)

        campaign_alerts = alerts.get("campaign_alerts", {}) or {}
        adset_alerts = alerts.get("adset_alerts", {}) or {}
        ad_alerts = alerts.get("ad_alerts", {}) or {}

        try:
            txt = get_cached_report(aid, period, label)
        except Exception:
            txt = None

        if not txt:
            continue

        totals = _parse_totals_from_report_text(txt)

        total_convs = totals["total_conversions"]
        spend = totals["spend"]
        cpa = totals["cpa"]

        # ====== 1) Старый аккаунтный алёрт (оставляем как есть) ======

        if cpa and total_convs > 0 and spend > 0 and account_target > 0:
            acc_name = get_account_name(aid)

            effective_target_acc = account_target

            if cpa > effective_target_acc:
                header = f"⚠️ {acc_name} — Итого (💬+📩)"
                body_lines = [
                    f"💵 Затраты: {spend:.2f} $",
                    f"📊 Заявки (💬+📩): {total_convs}",
                    f"🎯 Таргет CPA: {effective_target_acc:.2f} $",
                    f"🧾 Причина: CPA {cpa:.2f}$ > таргета {effective_target_acc:.2f}$",
                ]
                body = "\n".join(body_lines)

                text = f"{header}\n{body}"

                try:
                    await context.bot.send_message(chat_id, text)
                    await asyncio.sleep(1.0)
                except Exception:
                    # Не прерываем обработку адсетов, даже если аккаунтный алёрт не ушёл
                    pass

        # ====== 2) Новый алёрт по кампаниям ======

        acc_name = get_account_name(aid)

        # Сохраняем метрики кампаний в словарь, чтобы переиспользовать для
        # сообщений по объявлениям (CPA кампании и её таргет).
        campaign_stats: dict[str, dict] = {}

        try:
            camp_metrics = analyze_campaigns(aid, period=period_dict) or []
        except Exception:
            camp_metrics = []

        for camp in camp_metrics:
            cid = camp.get("campaign_id")
            if not cid:
                continue

            cfg_c = (campaign_alerts.get(cid) or {}) if cid in campaign_alerts else {}
            enabled_c = cfg_c.get("enabled", True)
            if not enabled_c:
                continue

            camp_target = float(cfg_c.get("target_cpa") or 0.0)
            effective_target_c = camp_target if camp_target > 0 else account_target
            if effective_target_c <= 0:
                continue

            c_spend = float(camp.get("spend", 0.0) or 0.0)
            c_total = int(camp.get("total", 0) or 0)
            c_cpa = camp.get("cpa")
            if not c_cpa or c_spend <= 0 or c_total <= 0:
                continue

            # Новый Monitoring Engine + Rules (3 дня по умолчанию)
            try:
                snap = build_monitor_snapshot(
                    aid=aid,
                    entity_id=str(cid),
                    level="campaign",
                    history_days=3,
                    target_cpa=effective_target_c,
                )
                rules = evaluate_rules(snap)
            except Exception:
                rules = []
                snap = {}

            if not rules:
                continue

            # Сохраняем статистику кампании для последующего использования в
            # мультимесседж-формате по объявлениям.
            cname = camp.get("name") or cid
            cpa_series = snap.get("cpa_series") or []
            last_cpa = next((v for v in reversed(cpa_series) if v is not None), None)
            campaign_stats[str(cid)] = {
                "name": cname,
                "cpa": float(last_cpa) if last_cpa is not None else None,
                "target": float(effective_target_c),
            }

            ai_text = None
            ai_conf = None
            if alerts.get("ai_enabled", True):
                try:
                    from services.ai_focus import get_focus_comment

                    ai_ctx = {
                        "entity": {"id": str(cid), "name": cname, "level": "campaign"},
                        "metrics": {
                            "cpa_series": snap.get("cpa_series"),
                            "delta_pct": snap.get("delta_pct"),
                            "frequency": snap.get("frequency"),
                            "spend_trend": snap.get("spend_trend"),
                        },
                        "triggered_rules": [r.get("rule") for r in rules if r.get("rule")],
                    }
                    ai_text = get_focus_comment(ai_ctx)
                    if snap.get("spike"):
                        ai_conf = 82
                    elif snap.get("violates_target"):
                        ai_conf = 75
                    else:
                        ai_conf = 70
                except Exception:
                    ai_text = None
                    ai_conf = None

            try:
                text_msg = format_cpa_anomaly_message(
                    snapshot=snap,
                    entity_name=str(cname),
                    level_human="Кампания",
                    triggered_rules=rules,
                    ai_text=ai_text,
                    ai_confidence=ai_conf,
                )
                await context.bot.send_message(chat_id, text_msg)
                await asyncio.sleep(0.5)
            except Exception:
                pass

        # ====== 3) Новый алёрт по адсетам ======

        # Статистика по адсетам (для сообщений по объявлениям)
        adset_stats: dict[str, dict] = {}

        try:
            campaigns, _since, _until = fetch_adset_insights_7d(aid)
        except Exception:
            campaigns = []

        for camp in campaigns:
            for ad in camp.get("adsets", []) or []:
                adset_id = ad.get("id")
                if not adset_id:
                    continue

                cid = ad.get("campaign_id")

                cfg_a = (adset_alerts.get(adset_id) or {}) if adset_id in adset_alerts else {}
                enabled_a = cfg_a.get("enabled", True)
                if not enabled_a:
                    continue

                adset_target = float(cfg_a.get("target_cpa") or 0.0)

                # Приоритет: adset → campaign → account
                camp_target = 0.0
                if cid and cid in campaign_alerts:
                    camp_target = float((campaign_alerts.get(cid) or {}).get("target_cpa") or 0.0)

                effective_target_a = (
                    adset_target
                    if adset_target > 0
                    else camp_target
                    if camp_target > 0
                    else account_target
                )

                if effective_target_a <= 0:
                    continue

                # Новый Monitoring Engine + Rules (3 дня по умолчанию)
                try:
                    snap = build_monitor_snapshot(
                        aid=aid,
                        entity_id=str(adset_id),
                        level="adset",
                        history_days=3,
                        target_cpa=effective_target_a,
                    )
                    rules = evaluate_rules(snap)
                except Exception:
                    rules = []
                    snap = {}

                if not rules:
                    continue

                # Сохраняем статистику адсета для последующего использования
                # в мультимесседж-формате по объявлениям.
                adset_name = ad.get("name") or adset_id
                a_series = snap.get("cpa_series") or []
                a_last = next((v for v in reversed(a_series) if v is not None), None)
                adset_stats[str(adset_id)] = {
                    "name": adset_name,
                    "cpa": float(a_last) if a_last is not None else None,
                    "target": float(effective_target_a),
                }

                ai_text = None
                ai_conf = None
                if alerts.get("ai_enabled", True):
                    try:
                        from services.ai_focus import get_focus_comment

                        ai_ctx = {
                            "entity": {"id": str(adset_id), "name": adset_name, "level": "adset"},
                            "metrics": {
                                "cpa_series": snap.get("cpa_series"),
                                "delta_pct": snap.get("delta_pct"),
                                "frequency": snap.get("frequency"),
                                "spend_trend": snap.get("spend_trend"),
                            },
                            "triggered_rules": [r.get("rule") for r in rules if r.get("rule")],
                        }
                        ai_text = get_focus_comment(ai_ctx)
                        if snap.get("spike"):
                            ai_conf = 82
                        elif snap.get("violates_target"):
                            ai_conf = 75
                        else:
                            ai_conf = 70
                    except Exception:
                        ai_text = None
                        ai_conf = None

                try:
                    text_msg = format_cpa_anomaly_message(
                        snapshot=snap,
                        entity_name=str(adset_name),
                        level_human="Адсет",
                        triggered_rules=rules,
                        ai_text=ai_text,
                        ai_confidence=ai_conf,
                    )
                    await context.bot.send_message(chat_id, text_msg)
                    await asyncio.sleep(0.5)
                except Exception:
                    pass

        # ====== 4) Новый алёрт по объявлениям ======

        # a) Загружаем метрики по объявлениям за today (для CPA и таргетов)
        try:
            ad_metrics_today = analyze_ads(aid, period=period_dict) or []
        except Exception:
            ad_metrics_today = []

        # b) Отдельно считаем альтернативы за последние 7 дней
        try:
            period_7d = {
                "since": (now - timedelta(days=7)).strftime("%Y-%m-%d"),
                "until": now.strftime("%Y-%m-%d"),
            }
            ad_metrics_7d = analyze_ads(aid, period=period_7d) or []
        except Exception:
            ad_metrics_7d = []

        ads_by_adset_7d: dict[str, list[dict]] = {}
        for ad7 in ad_metrics_7d:
            ad_id7 = ad7.get("ad_id")
            if not ad_id7:
                continue
            adset_id7 = ad7.get("adset_id") or ""
            if not adset_id7:
                continue
            a_spend7 = float(ad7.get("spend", 0.0) or 0.0)
            if a_spend7 <= 0:
                continue
            bucket7 = ads_by_adset_7d.setdefault(str(adset_id7), [])
            bucket7.append(ad7)

        # c) Группируем ПРОБЛЕМНЫЕ объявления по кампании/адсету
        problems_by_campaign: dict[str, dict] = {}

        for ad in ad_metrics_today:
            ad_id = ad.get("ad_id")
            if not ad_id:
                continue

            cfg_ad = (ad_alerts.get(ad_id) or {}) if ad_id in ad_alerts else {}
            enabled_ad = cfg_ad.get("enabled", True)
            silent_ad = cfg_ad.get("silent", False)

            if not enabled_ad:
                continue

            ad_target = float(cfg_ad.get("target_cpa") or 0.0)

            # Иерархия: ad → adset → campaign → account
            adset_id = ad.get("adset_id")
            camp_id = ad.get("campaign_id")

            adset_target2 = 0.0
            if adset_id and adset_id in adset_alerts:
                adset_target2 = float((adset_alerts.get(adset_id) or {}).get("target_cpa") or 0.0)

            camp_target2 = 0.0
            if camp_id and camp_id in campaign_alerts:
                camp_target2 = float((campaign_alerts.get(camp_id) or {}).get("target_cpa") or 0.0)

            effective_target_ad = (
                ad_target
                if ad_target > 0
                else adset_target2
                if adset_target2 > 0
                else camp_target2
                if camp_target2 > 0
                else account_target
            )

            if effective_target_ad <= 0:
                continue

            a_spend = float(ad.get("spend", 0.0) or 0.0)
            a_total = int(ad.get("total", 0) or 0)
            a_cpa = ad.get("cpa")
            if not a_cpa or a_spend <= 0 or a_total <= 0:
                continue

            # Новый Monitoring Engine + Rules (3 дня по умолчанию)
            try:
                snap = build_monitor_snapshot(
                    aid=aid,
                    entity_id=str(ad_id),
                    level="ad",
                    history_days=3,
                    target_cpa=effective_target_ad,
                )
                rules = evaluate_rules(snap)
            except Exception:
                rules = []
                snap = {}

            if not rules:
                continue

            # Есть ли альтернативы внутри того же адсета за последние 7 дней
            has_alternative = False
            if adset_id:
                all_in_adset7 = ads_by_adset_7d.get(str(adset_id)) or []
                for other in all_in_adset7:
                    if other.get("ad_id") == ad_id:
                        continue
                    if float(other.get("spend", 0.0) or 0.0) > 0:
                        has_alternative = True
                        break

            # Если объявление в тихом режиме — считаем CPA, но не добавляем в список
            if silent_ad:
                continue

            ad_name = ad.get("name") or ad_id
            adset_name = ad.get("adset_name") or adset_id or "?"
            camp_name = ad.get("campaign_name") or camp_id or "?"

            camp_key = str(camp_id or "?")
            camp_entry = problems_by_campaign.setdefault(
                camp_key,
                {"name": camp_name, "adsets": {}},
            )

            adset_key = str(adset_id or "?")
            adsets_map = camp_entry["adsets"]
            adset_entry = adsets_map.setdefault(
                adset_key,
                {"name": adset_name, "ads": []},
            )

            adset_entry["ads"].append(
                {
                    "ad_id": ad_id,
                    "ad_name": ad_name,
                    "cpa": float(a_cpa),
                    "target": float(effective_target_ad),
                    "has_alternative_in_adset": bool(has_alternative),
                    "snap": snap,
                    "rules": rules,
                }
            )

        # d) Мультимесседж-формат: Кампания → Адсет → Объявления
        # Не шлём кампанию/адсет, если внутри нет проблемных объявлений
        for camp_key in sorted(problems_by_campaign.keys()):
            camp_entry = problems_by_campaign[camp_key]
            adsets_map = camp_entry.get("adsets") or {}

            # Считаем общее количество проблемных объявлений в кампании
            total_ads_in_camp = sum(
                len(adset_entry.get("ads") or []) for adset_entry in adsets_map.values()
            )
            if total_ads_in_camp <= 0:
                continue

            # Сообщение по кампании
            camp_stat = campaign_stats.get(camp_key) or {}
            camp_cpa_val = camp_stat.get("cpa")
            camp_tgt_val = camp_stat.get("target")
            camp_cpa_str = f"{camp_cpa_val:.2f}$" if camp_cpa_val is not None else "н/д"
            camp_tgt_str = f"{camp_tgt_val:.2f}$" if camp_tgt_val is not None else "н/д"

            cname = camp_entry.get("name") or camp_key
            camp_lines = [
                f"🟩 Кампания: {cname}",
                f"CPA кампании: {camp_cpa_str} (таргет: {camp_tgt_str})",
                "⚠️ Проблемные элементы внутри → см. сообщения ниже",
            ]
            try:
                await context.bot.send_message(chat_id, "\n".join(camp_lines))
                await asyncio.sleep(0.3)
            except Exception:
                pass

            # Сообщения по адсетам и объявлениям
            for adset_key in sorted(adsets_map.keys()):
                adset_entry = adsets_map[adset_key]
                ads_list = adset_entry.get("ads") or []
                if not ads_list:
                    continue

                as_name = adset_entry.get("name") or adset_key
                adset_stat = adset_stats.get(adset_key) or {}
                adset_cpa_val = adset_stat.get("cpa")
                adset_tgt_val = adset_stat.get("target")
                adset_cpa_str = (
                    f"{adset_cpa_val:.2f}$" if adset_cpa_val is not None else "н/д"
                )
                adset_tgt_str = (
                    f"{adset_tgt_val:.2f}$" if adset_tgt_val is not None else "н/д"
                )

                adset_lines = [
                    f"🟦 Адсет: {as_name}",
                    f"CPA адсета: {adset_cpa_str} (таргет: {adset_tgt_str})",
                    "⚠️ Внутри объявления, которые превышают CPA → следующие сообщения",
                ]
                try:
                    await context.bot.send_message(chat_id, "\n".join(adset_lines))
                    await asyncio.sleep(0.3)
                except Exception:
                    pass

                # Для каждого объявления — отдельное сообщение с собственными кнопками
                for ad_info in ads_list:
                    ad_id = ad_info.get("ad_id")
                    ad_name_txt = ad_info.get("ad_name") or ad_id
                    cpa_val = float(ad_info.get("cpa", 0.0) or 0.0)
                    tgt_val = float(ad_info.get("target", 0.0) or 0.0)
                    snap = ad_info.get("snap") or {}
                    rules = ad_info.get("rules") or []
                    has_alt_flag = bool(ad_info.get("has_alternative_in_adset"))

                    alt_str = "да" if has_alt_flag else "нет"

                    ai_text = None
                    ai_conf = None
                    if alerts.get("ai_enabled", True):
                        try:
                            from services.ai_focus import get_focus_comment

                            ai_ctx = {
                                "entity": {"id": str(ad_id), "name": ad_name_txt, "level": "ad"},
                                "metrics": {
                                    "cpa_series": snap.get("cpa_series"),
                                    "delta_pct": snap.get("delta_pct"),
                                    "frequency": snap.get("frequency"),
                                    "spend_trend": snap.get("spend_trend"),
                                },
                                "triggered_rules": [r.get("rule") for r in rules if r.get("rule")],
                            }
                            ai_text = get_focus_comment(ai_ctx)
                            if snap.get("spike"):
                                ai_conf = 82
                            elif snap.get("violates_target"):
                                ai_conf = 75
                            else:
                                ai_conf = 70
                        except Exception:
                            ai_text = None
                            ai_conf = None

                    ad_lines = [
                        format_cpa_anomaly_message(
                            snapshot=snap,
                            entity_name=str(ad_name_txt),
                            level_human="Объявление",
                            triggered_rules=rules,
                            ai_text=ai_text,
                            ai_confidence=ai_conf,
                        ),
                        "",
                        f"Есть альтернативы в адсете: {alt_str}",
                    ]

                    kb_row: list[InlineKeyboardButton] = []
                    if has_alt_flag and ad_id:
                        kb_row.append(
                            InlineKeyboardButton(
                                "Выключить",
                                callback_data=f"cpa_ad_off|{aid}|{ad_id}",
                            )
                        )
                    if ad_id:
                        kb_row.append(
                            InlineKeyboardButton(
                                "Тихий режим",
                                callback_data=f"cpa_ad_silent|{aid}|{ad_id}",
                            )
                        )

                    try:
                        await context.bot.send_message(
                            chat_id,
                            "\n".join(ad_lines),
                            reply_markup=InlineKeyboardMarkup([kb_row]) if kb_row else None,
                        )
                        await asyncio.sleep(0.3)
                    except Exception:
                        pass

        # NB: ИИ-комментарии добавляются на уровне конкретных алёртов (кампания/адсет/объявление)
        # и не должны ломать мониторинг.

    logging.getLogger(__name__).info("🟢 CPA alerts job executed successfully")


async def _hourly_snapshot_job(context: ContextTypes.DEFAULT_TYPE):
    """Раз в час снимаем инсайты за today и сохраняем дельту в hour buckets.

    - один запрос fetch_insights(aid, "today") на аккаунт;
    - дельта по messages/leads/total/spend пишется в hourly_stats.json;
    - храним историю ~2 года по дням и часам.
    """
    logging.getLogger(__name__).info(
        "hourly_snapshot_job_disabled reason=heatmap_snapshots_single_source_of_truth"
    )
    return

    now = datetime.now(ALMATY_TZ)
    date_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    hour_int = int(now.strftime("%H"))

    accounts = load_accounts() or {}
    stats = load_hourly_stats() or {}
    acc_section = stats.setdefault("_acc", {})
    acc_adset_section = stats.setdefault("_acc_adset", {})
    acc_ad_section = stats.setdefault("_acc_ad", {})

    adset_section = stats.setdefault("_adset", {})
    ad_section = stats.setdefault("_ad", {})

    # Порог хранения ~2 года
    cutoff_date = (now - timedelta(days=730)).strftime("%Y-%m-%d")

    for aid, row in accounts.items():
        if not (row or {}).get("enabled", True):
            continue

        # Инсайты за today — всегда живые, без кэша (см. fetch_insights).
        try:
            ins = fetch_insights(aid, "today") or {}
        except Exception:
            continue

        metrics = parse_insight(ins)

        cur_msgs = int(metrics.get("msgs", 0) or 0)
        cur_leads = int(metrics.get("leads", 0) or 0)
        cur_total = int(metrics.get("total", 0) or 0)
        cur_spend = float(metrics.get("spend", 0.0) or 0.0)

        prev = acc_section.get(aid, {"date": date_str, "msgs": 0, "leads": 0, "total": 0, "spend": 0.0})
        if str(prev.get("date") or "") != date_str:
            prev = {"date": date_str, "msgs": 0, "leads": 0, "total": 0, "spend": 0.0}

        d_msgs = max(0, cur_msgs - int(prev.get("msgs", 0) or 0))
        d_leads = max(0, cur_leads - int(prev.get("leads", 0) or 0))
        d_total = max(0, cur_total - int(prev.get("total", 0) or 0))
        d_spend = max(0.0, cur_spend - float(prev.get("spend", 0.0) or 0.0))

        if any([d_msgs, d_leads, d_total, d_spend]):
            acc_stats = stats.setdefault(aid, {})
            day_stats = acc_stats.setdefault(date_str, {})
            hour_bucket = day_stats.setdefault(
                hour_str,
                {"messages": 0, "leads": 0, "total": 0, "spend": 0.0},
            )

            hour_bucket["messages"] += d_msgs
            hour_bucket["leads"] += d_leads
            hour_bucket["total"] += d_total
            hour_bucket["spend"] += d_spend

        # Обновляем аккумулятор для следующего часа
        acc_section[aid] = {
            "date": date_str,
            "msgs": cur_msgs,
            "leads": cur_leads,
            "total": cur_total,
            "spend": cur_spend,
        }

        try:
            from facebook_business.adobjects.adaccount import AdAccount

            def _fetch_level_rows(level: str):
                acc = AdAccount(aid)
                params = _period_to_params("today")
                params["level"] = level
                fields = [
                    "spend",
                    "actions",
                    "cost_per_action_type",
                    "impressions",
                    "clicks",
                    "adset_id",
                    "campaign_id",
                ]
                data = safe_api_call(acc.get_insights, fields=fields, params=params)
                return data or []

            adset_rows = _fetch_level_rows("adset")
            ad_rows = _fetch_level_rows("ad") if (hour_int % 3 == 0) else []

            acc_adset_section.setdefault(aid, {})
            acc_ad_section.setdefault(aid, {})
            adset_section.setdefault(aid, {})
            ad_section.setdefault(aid, {})

            for rr in adset_rows:
                row_d = _normalize_insight(rr)
                adset_id = str(row_d.get("adset_id") or "")
                if not adset_id:
                    continue
                parsed = parse_insight(row_d, aid=aid)
                cur_m = int(parsed.get("msgs", 0) or 0)
                cur_l = int(parsed.get("leads", 0) or 0)
                cur_t = int(parsed.get("total", 0) or 0)
                cur_s = float(parsed.get("spend", 0.0) or 0.0)

                prev_a = (acc_adset_section.get(aid) or {}).get(
                    adset_id,
                    {"date": date_str, "msgs": 0, "leads": 0, "total": 0, "spend": 0.0},
                )
                if str((prev_a or {}).get("date") or "") != date_str:
                    prev_a = {"date": date_str, "msgs": 0, "leads": 0, "total": 0, "spend": 0.0}

                d_m = max(0, cur_m - int((prev_a or {}).get("msgs", 0) or 0))
                d_l = max(0, cur_l - int((prev_a or {}).get("leads", 0) or 0))
                d_t = max(0, cur_t - int((prev_a or {}).get("total", 0) or 0))
                d_s = max(0.0, cur_s - float((prev_a or {}).get("spend", 0.0) or 0.0))

                if any([d_m, d_l, d_t, d_s]):
                    asec = adset_section[aid].setdefault(adset_id, {})
                    dsec = asec.setdefault(date_str, {})
                    b = dsec.setdefault(hour_str, {"messages": 0, "leads": 0, "total": 0, "spend": 0.0})
                    b["messages"] += d_m
                    b["leads"] += d_l
                    b["total"] += d_t
                    b["spend"] += d_s

                acc_adset_section[aid][adset_id] = {
                    "date": date_str,
                    "msgs": cur_m,
                    "leads": cur_l,
                    "total": cur_t,
                    "spend": cur_s,
                }

            for rr in ad_rows:
                row_d = _normalize_insight(rr)
                ad_id = str(row_d.get("ad_id") or "")
                if not ad_id:
                    continue
                adset_id = str(row_d.get("adset_id") or "")
                campaign_id = str(row_d.get("campaign_id") or "")
                parsed = parse_insight(row_d, aid=aid)
                cur_m = int(parsed.get("msgs", 0) or 0)
                cur_l = int(parsed.get("leads", 0) or 0)
                cur_t = int(parsed.get("total", 0) or 0)
                cur_s = float(parsed.get("spend", 0.0) or 0.0)

                prev_a = (acc_ad_section.get(aid) or {}).get(
                    ad_id,
                    {"date": date_str, "msgs": 0, "leads": 0, "total": 0, "spend": 0.0},
                )
                if str((prev_a or {}).get("date") or "") != date_str:
                    prev_a = {"date": date_str, "msgs": 0, "leads": 0, "total": 0, "spend": 0.0}

                d_m = max(0, cur_m - int((prev_a or {}).get("msgs", 0) or 0))
                d_l = max(0, cur_l - int((prev_a or {}).get("leads", 0) or 0))
                d_t = max(0, cur_t - int((prev_a or {}).get("total", 0) or 0))
                d_s = max(0.0, cur_s - float((prev_a or {}).get("spend", 0.0) or 0.0))

                if any([d_m, d_l, d_t, d_s]):
                    asec = ad_section[aid].setdefault(ad_id, {})
                    dsec = asec.setdefault(date_str, {})
                    b = dsec.setdefault(hour_str, {"messages": 0, "leads": 0, "total": 0, "spend": 0.0})
                    b["messages"] += d_m
                    b["leads"] += d_l
                    b["total"] += d_t
                    b["spend"] += d_s
                    if adset_id:
                        b["adset_id"] = adset_id
                    if campaign_id:
                        b["campaign_id"] = campaign_id

                acc_ad_section[aid][ad_id] = {
                    "date": date_str,
                    "msgs": cur_m,
                    "leads": cur_l,
                    "total": cur_t,
                    "spend": cur_s,
                }
        except Exception:
            pass

    # Обрезаем историю старше cutoff_date
    for aid, acc_stats in list(stats.items()):
        if str(aid).startswith("_"):
            continue
        if not isinstance(acc_stats, dict):
            continue
        for d in list(acc_stats.keys()):
            if d < cutoff_date:
                del acc_stats[d]

    for aid, by_adset in list((stats.get("_adset") or {}).items()):
        if not isinstance(by_adset, dict):
            continue
        for adset_id, adset_days in list(by_adset.items()):
            if not isinstance(adset_days, dict):
                continue
            for d in list(adset_days.keys()):
                if d < cutoff_date:
                    del adset_days[d]
            if not adset_days:
                del by_adset[adset_id]
        if not by_adset:
            del stats["_adset"][aid]

    for aid, by_ad in list((stats.get("_ad") or {}).items()):
        if not isinstance(by_ad, dict):
            continue
        for ad_id, ad_days in list(by_ad.items()):
            if not isinstance(ad_days, dict):
                continue
            for d in list(ad_days.keys()):
                if d < cutoff_date:
                    del ad_days[d]
            if not ad_days:
                del by_ad[ad_id]
        if not by_ad:
            del stats["_ad"][aid]

    save_hourly_stats(stats)

    logging.getLogger(__name__).info("🟢 Hourly snapshot job executed successfully")


def schedule_cpa_alerts(app: Application):
    # Планировщик CPA-алёртов: единый повторяющийся джоб раз в час.
    # Внутри _cpa_alerts_job уже учитывает days/freq и решает,
    # нужно ли слать уведомления в этот час.
    # Stagger jobs to reduce FB burst.
    try:
        now = datetime.now(ALMATY_TZ)
        first_cpa = now.replace(minute=25, second=0, microsecond=0)
        if first_cpa <= now:
            first_cpa = first_cpa + timedelta(hours=1)
    except Exception:
        first_cpa = timedelta(minutes=25)
    app.job_queue.run_repeating(
        _cpa_alerts_job,
        interval=timedelta(hours=1),
        first=first_cpa,
    )

    # Часовой снимок инсайтов за today для часового кэша
    # Heatmap snapshot collector: единственный компонент, который ходит в FB.
    # Запускаем раз в 10 минут; он собирает предыдущий полный час и дособирает до дедлайна.
    try:
        now = datetime.now(ALMATY_TZ)
        first_col = now.replace(minute=(int(now.minute / 10) * 10), second=0, microsecond=0)
        if first_col <= now:
            first_col = first_col + timedelta(minutes=10)
    except Exception:
        first_col = timedelta(minutes=10)
    app.job_queue.run_repeating(
        _heatmap_snapshot_collector_job,
        interval=timedelta(minutes=10),
        first=first_col,
    )

    # NOTE: _autopilot_heatmap_job is intentionally not scheduled here.
    # It will be re-enabled after it is migrated to snapshots-only data.
