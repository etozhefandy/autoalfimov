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
from .cpa_monitoring import format_cpa_anomaly_message
from .autopilot_format import ap_action_text

try:  # pragma: no cover
    from services.heatmap_store import load_snapshot, prev_full_hour_window
except Exception:  # noqa: BLE001
    def load_snapshot(_aid: str, *, date_str: str, hour: int):  # type: ignore[override]
        return None

    def prev_full_hour_window(now: datetime | None = None):  # type: ignore[override]
        return {}

try:  # pragma: no cover
    from services.facebook_api import safe_api_call, _normalize_insight
    from services.analytics import parse_insight
    from services.facebook_api import (
        is_rate_limited_now,
        rate_limit_retry_after_seconds,
        get_last_api_error_info,
        classify_api_error,
        allow_fb_api_calls,
        deny_fb_api_calls,
    )
except Exception:  # noqa: BLE001
    def safe_api_call(_fn, *args, **kwargs):  # type: ignore[override]
        return None

    def _normalize_insight(_row):  # type: ignore[override]
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
    logging.getLogger(__name__).info(
        "full_daily_scan_job_disabled reason=heatmap_snapshots_single_source_of_truth"
    )
    return


async def daily_report_job(context: ContextTypes.DEFAULT_TYPE):
    logging.getLogger(__name__).info(
        "daily_report_job_disabled reason=heatmap_snapshots_single_source_of_truth"
    )
    return


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
    logging.getLogger(__name__).info(
        "autopilot_heatmap_job_disabled reason=heatmap_snapshots_single_source_of_truth"
    )
    return


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


async def _hourly_snapshot_job(context: ContextTypes.DEFAULT_TYPE):
    logging.getLogger(__name__).info(
        "hourly_snapshot_job_disabled reason=heatmap_snapshots_single_source_of_truth"
    )
    return


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
