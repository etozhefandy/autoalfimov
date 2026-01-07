from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fb_report.constants import ALMATY_TZ
from fb_report.insights import extract_actions, extract_costs
from fb_report.storage import get_account_name
from services.analytics import analyze_adsets
from services.analytics import fetch_insights_by_level
from services.analytics import lead_cost_and_count
from services.facebook_api import fetch_insights


MSG_ACTION = "onsite_conversion.messaging_conversation_started_7d"


def _build_day_period(day: datetime) -> Dict[str, str]:
    d = day.date()
    s = d.strftime("%Y-%m-%d")
    return {"since": s, "until": s}


def _iter_last_days(days: int) -> List[Dict[str, str]]:
    now = datetime.now(ALMATY_TZ)
    # включаем сегодня и N-1 предыдущих дней
    return [_build_day_period(now - timedelta(days=i)) for i in range(max(1, int(days)))]


def compute_effective_cpa(insight: Dict[str, Any], *, aid: str) -> Tuple[Optional[float], int]:
    """CPA по cost_per_action_type для (переписки + лиды).

    Возвращает (cpa, total_actions). Если total_actions=0 или нет cost-данных → (None, total_actions).
    """

    if not insight:
        return None, 0

    acts = extract_actions(insight)
    costs = extract_costs(insight)

    total_actions = 0
    total_cost = 0.0

    msg_cnt = int(acts.get(MSG_ACTION, 0) or 0)
    if msg_cnt > 0:
        total_actions += msg_cnt
        msg_cpa = costs.get(MSG_ACTION)
        if msg_cpa is not None and float(msg_cpa) > 0:
            total_cost += float(msg_cpa) * float(msg_cnt)

    lead_cnt, lead_cost = lead_cost_and_count(acts, costs, aid=aid)
    if lead_cnt > 0:
        total_actions += int(lead_cnt)
        total_cost += float(lead_cost)

    if total_actions <= 0:
        return None, 0

    if total_cost <= 0:
        return None, total_actions

    return total_cost / float(total_actions), total_actions


def _delta_pct(first: Optional[float], last: Optional[float]) -> Optional[int]:
    if first is None or last is None or first <= 0:
        return None
    try:
        return int(round(((last - first) / first) * 100.0))
    except Exception:
        return None


def _trend(series: List[Optional[float]]) -> str:
    vals = [v for v in series if v is not None]
    if len(vals) < 2:
        return "flat"
    if vals[-1] > vals[0]:
        return "up"
    if vals[-1] < vals[0]:
        return "down"
    return "flat"


def _trend_num(series: List[float]) -> str:
    vals = [float(v) for v in (series or [])]
    if len(vals) < 2:
        return "flat"
    if vals[-1] > vals[0]:
        return "up"
    if vals[-1] < vals[0]:
        return "down"
    return "flat"


def build_monitor_snapshot(
    *,
    aid: str,
    entity_id: Optional[str],
    level: str,
    history_days: int,
    target_cpa: Optional[float] = None,
) -> Dict[str, Any]:
    lvl = str(level or "account").lower()
    periods = list(reversed(_iter_last_days(history_days)))

    series: List[Optional[float]] = []
    totals: List[int] = []
    spend_series: List[float] = []
    freq_series: List[Optional[float]] = []

    for p in periods:
        ins: Dict[str, Any] | None
        if lvl == "account":
            ins = fetch_insights(aid, p)
        else:
            if not entity_id:
                ins = None
            else:
                ins = fetch_insights_by_level(aid, str(entity_id), p, level=lvl)

        cpa, total_actions = compute_effective_cpa(ins or {}, aid=aid)
        series.append(cpa)
        totals.append(int(total_actions or 0))
        spend_series.append(float((ins or {}).get("spend", 0.0) or 0.0))
        try:
            freq_series.append(float((ins or {}).get("frequency", 0.0) or 0.0))
        except Exception:
            freq_series.append(None)

    first = next((v for v in series if v is not None), None)
    last = next((v for v in reversed(series) if v is not None), None)
    delta_pct = _delta_pct(first, last)

    spike = False
    if delta_pct is not None and delta_pct >= 50:
        spike = True

    tgt = float(target_cpa) if target_cpa is not None and float(target_cpa) > 0 else None
    violates_target = bool(tgt is not None and last is not None and last > tgt)

    return {
        "account_id": str(aid),
        "account_name": get_account_name(aid),
        "entity_id": str(entity_id) if entity_id is not None else None,
        "level": lvl,
        "history_days": int(history_days),
        "cpa_series": series,
        "total_actions_series": totals,
        "spend_series": spend_series,
        "spend_trend": _trend_num(spend_series),
        "frequency": next((v for v in reversed(freq_series) if v is not None), None),
        "delta_pct": delta_pct,
        "trend": _trend(series),
        "spike": spike,
        "target_cpa": tgt,
        "violates_target": violates_target,
    }


def rule_cpa_spike(snapshot: Dict[str, Any], *, min_delta_pct: int = 50) -> Optional[Dict[str, Any]]:
    dp = snapshot.get("delta_pct")
    if dp is None:
        return None
    if int(dp) < int(min_delta_pct):
        return None
    return {"rule": "cpa_spike", "severity": "high", "should_notify": True}


def rule_cpa_above_target(snapshot: Dict[str, Any], *, consecutive_days: int = 2) -> Optional[Dict[str, Any]]:
    tgt = snapshot.get("target_cpa")
    if tgt is None or float(tgt) <= 0:
        return None

    series = snapshot.get("cpa_series") or []
    vals = [v for v in series if v is not None]
    if len(vals) < int(consecutive_days):
        return None

    tail = vals[-int(consecutive_days) :]
    if all(v > float(tgt) for v in tail):
        return {"rule": "cpa_above_target", "severity": "high", "should_notify": True}
    return None


def evaluate_rules(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    r1 = rule_cpa_spike(snapshot)
    if r1:
        out.append(r1)
    r2 = rule_cpa_above_target(snapshot)
    if r2:
        out.append(r2)
    return out


def format_cpa_anomaly_message(
    *,
    snapshot: Dict[str, Any],
    entity_name: str,
    level_human: str,
    triggered_rules: List[Dict[str, Any]],
    ai_text: Optional[str] = None,
    ai_confidence: Optional[int] = None,
) -> str:
    series = snapshot.get("cpa_series") or []
    last = next((v for v in reversed(series) if v is not None), None)
    dp = snapshot.get("delta_pct")
    history_days = snapshot.get("history_days")
    target = snapshot.get("target_cpa")

    delta_str = ""
    if dp is not None:
        sign = "+" if int(dp) >= 0 else ""
        delta_str = f" ({sign}{int(dp)}%)"

    lines: List[str] = [
        "🚨 CPA аномалия обнаружена",
        "",
        f"Объект: {entity_name}",
        f"Уровень: {level_human}",
        f"Период анализа: {history_days} дня",
        "",
    ]

    if last is not None:
        lines.append(f"CPA: {float(last):.2f} $" + delta_str)
    else:
        lines.append("CPA: —")

    if target is not None:
        lines.append(f"Целевой CPA: {float(target):.2f} $")

    if ai_text:
        lines.append("")
        lines.append("🤖 Фокус-ИИ:")
        lines.append(str(ai_text).strip())

    if ai_confidence is not None:
        lines.append("")
        lines.append(f"Уверенность: {int(ai_confidence)}%")

    return "\n".join(lines)


def build_anomaly_messages_for_account(aid: str) -> List[str]:
    """Единый entrypoint для ручного меню "Аномалии".

    Источник истины — этот модуль (monitoring engine + rules). Старый
    monitor_anomalies.py должен быть только wrapper.
    """

    try:
        now = datetime.now(ALMATY_TZ)
        period_dict = {
            "since": now.strftime("%Y-%m-%d"),
            "until": now.strftime("%Y-%m-%d"),
        }
        rows = analyze_adsets(aid, period=period_dict) or []
    except Exception:
        rows = []

    items: List[Tuple[str, str]] = []
    for r in rows:
        adset_id = str(r.get("adset_id") or r.get("id") or "")
        if not adset_id:
            continue
        name = str(r.get("name") or adset_id)
        items.append((adset_id, name))

    messages: List[str] = []
    acc_name = get_account_name(aid)
    header = f"⚠️ Анализ аномалий (CPA) по адсетам для {acc_name}"
    messages.append(header)

    sent_any = False
    for adset_id, name in items:
        try:
            snap = build_monitor_snapshot(
                aid=aid,
                entity_id=adset_id,
                level="adset",
                history_days=3,
                target_cpa=None,
            )
            rules = evaluate_rules(snap)
        except Exception:
            continue

        if not rules:
            continue

        sent_any = True
        messages.append(
            format_cpa_anomaly_message(
                snapshot=snap,
                entity_name=name,
                level_human="Адсет",
                triggered_rules=rules,
                ai_text=None,
                ai_confidence=None,
            )
        )

    if not sent_any:
        return []

    return messages
