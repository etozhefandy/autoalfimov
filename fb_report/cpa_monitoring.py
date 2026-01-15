from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from fb_report.constants import ALMATY_TZ
from fb_report.storage import get_account_name
from services.facebook_api import deny_fb_api_calls
from services.heatmap_store import find_latest_ready_snapshots, get_heatmap_dataset, prev_full_hour_window


def _build_day_period(day: datetime) -> Dict[str, str]:
    d = day.date()
    s = d.strftime("%Y-%m-%d")
    return {"since": s, "until": s}


def _iter_last_days(days: int) -> List[Dict[str, str]]:
    now = datetime.now(ALMATY_TZ)
    # включаем сегодня и N-1 предыдущих дней
    return [_build_day_period(now - timedelta(days=i)) for i in range(max(1, int(days)))]


def compute_effective_cpa(insight: Dict[str, Any], *, aid: str) -> Tuple[Optional[float], int]:
    if not insight:
        return None, 0

    try:
        spend = float((insight or {}).get("spend") or 0.0)
    except Exception:
        spend = 0.0

    try:
        total = (insight or {}).get("total")
        if total is None:
            total = int((insight or {}).get("msgs") or 0) + int((insight or {}).get("leads") or 0)
        total_actions = int(total or 0)
    except Exception:
        total_actions = 0

    if total_actions <= 0:
        return None, 0
    if spend <= 0:
        return None, total_actions
    return float(spend) / float(total_actions), total_actions


def _sum_row_metrics(rows: List[Dict[str, Any]]) -> Tuple[float, int, int, int]:
    spend = 0.0
    msgs = 0
    leads = 0
    total = 0
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        try:
            spend += float(r.get("spend") or 0.0)
        except Exception:
            pass
        try:
            msgs += int(r.get("msgs") or 0)
        except Exception:
            pass
        try:
            leads += int(r.get("leads") or 0)
        except Exception:
            pass
        try:
            t = r.get("total")
            if t is None:
                t = int(r.get("msgs") or 0) + int(r.get("leads") or 0)
            total += int(t or 0)
        except Exception:
            pass
    return float(spend), int(msgs), int(leads), int(total)


def _filter_rows_for_scope(
    rows: List[Dict[str, Any]],
    *,
    lvl: str,
    entity_id: Optional[str],
) -> List[Dict[str, Any]]:
    if lvl == "account":
        return list(rows or [])
    if not entity_id:
        return []
    eid = str(entity_id)
    if lvl == "campaign":
        return [r for r in (rows or []) if str((r or {}).get("campaign_id") or "") == eid]
    if lvl == "adset":
        return [r for r in (rows or []) if str((r or {}).get("adset_id") or "") == eid]
    return []


def _aggregate_day_from_snapshots(
    snaps: List[Dict[str, Any]],
    *,
    lvl: str,
    entity_id: Optional[str],
) -> Dict[str, Any]:
    day_rows: List[Dict[str, Any]] = []
    for snap in snaps or []:
        rows = (snap or {}).get("rows") or []
        if not isinstance(rows, list):
            continue
        day_rows.extend(_filter_rows_for_scope(rows, lvl=lvl, entity_id=entity_id))

    spend, msgs, leads, total = _sum_row_metrics(day_rows)
    return {
        "spend": spend,
        "msgs": msgs,
        "leads": leads,
        "total": total,
    }


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

    now = datetime.now(ALMATY_TZ)
    max_hours = max(1, int(history_days) * 24 + 48)
    with deny_fb_api_calls(reason="anomalies_snapshot_series"):
        snaps = find_latest_ready_snapshots(str(aid), max_hours=max_hours, now=now)

    snaps_by_date: Dict[str, List[Dict[str, Any]]] = {}
    for s in snaps or []:
        d = str((s or {}).get("date") or "")
        if not d:
            continue
        snaps_by_date.setdefault(d, []).append(s)

    for d, items in snaps_by_date.items():
        try:
            items.sort(key=lambda x: int((x or {}).get("hour") or 0))
        except Exception:
            pass

    series: List[Optional[float]] = []
    totals: List[int] = []
    spend_series: List[float] = []
    freq_series: List[Optional[float]] = []

    for p in periods:
        date_str = str((p or {}).get("since") or "")
        daily = _aggregate_day_from_snapshots(
            snaps_by_date.get(date_str, []),
            lvl=lvl,
            entity_id=str(entity_id) if entity_id is not None else None,
        )
        cpa, total_actions = compute_effective_cpa(daily or {}, aid=aid)
        series.append(cpa)
        totals.append(int(total_actions or 0))
        spend_series.append(float((daily or {}).get("spend", 0.0) or 0.0))
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

    win = prev_full_hour_window(now=datetime.now(ALMATY_TZ))
    date_str = str(win.get("date") or "")
    hour_int = int(win.get("hour") or 0)
    with deny_fb_api_calls(reason="anomalies_list_adsets"):
        ds, st, reason, meta = get_heatmap_dataset(str(aid), date_str=date_str, hours=[hour_int])
    rows = list((ds or {}).get("rows") or []) if (st == "ready" and ds) else []

    if st != "ready" or not rows:
        window_label = f"{(win.get('window') or {}).get('start','')}–{(win.get('window') or {}).get('end','')}"
        attempts = (meta or {}).get("attempts")
        last_try_at = (meta or {}).get("last_try_at")
        next_try_at = (meta or {}).get("next_try_at")

        txt = (
            "⚠️ Анализ аномалий (CPA)\n"
            f"Аккаунт: {get_account_name(aid)}\n"
            "Источник данных: heatmap cache\n"
            f"Окно: {date_str} {window_label}\n"
            f"Слепок: {st} ({reason})\n"
        )
        if attempts is not None:
            txt += f"Попытки: {attempts}\n"
        if last_try_at:
            txt += f"Последняя попытка: {last_try_at}\n"
        if next_try_at:
            txt += f"Следующая попытка: {next_try_at}\n"
        txt += "\nЕсли слепка нет или он собирается — нажми '📌 Собрать слепок предыдущего часа' и повтори."
        return [txt]

    items: List[Tuple[str, str]] = []
    for r in rows:
        adset_id = str(r.get("adset_id") or r.get("id") or "")
        if not adset_id:
            continue
        name = str(r.get("adset_name") or r.get("name") or adset_id)
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
