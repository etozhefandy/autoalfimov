from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from config import ALMATY_TZ
from services.analytics import analyze_adsets, safe_div


"""Модуль анализа аномалий по адсетам.

Сравнивает два однодневных периода:
- Период 0 (baseline): позавчера 00:00–23:59
- Период 1 (current):  вчера 00:00–23:59

Строит отчёт по аномалиям метрик CPA/CPM/CTR/CPC/CVR с фильтрацией по
объёму данных, чтобы избежать бессмысленных -90..-100% на единичных
кликах/конверсиях.
"""


# ========================
# Константы порогов
# ========================

MIN_IMPRESSIONS = 100
MIN_CLICKS = 10
MIN_CONVERSIONS = 3

THRESHOLD_CPA = 0.30  # ±30%
THRESHOLD_CPC = 0.25
THRESHOLD_CPM = 0.25
THRESHOLD_CTR = 0.20
THRESHOLD_CVR = 0.30


@dataclass
class PeriodMetrics:
    spend: float
    impr: int
    clicks: int
    conversions: int
    cpa: Optional[float]
    cpm: float
    cpc: float
    ctr: float  # %
    cvr: float  # %


def _build_periods_for_yday_vs_byday() -> Tuple[Dict[str, str], Dict[str, str]]:
    """Возвращает (period_0, period_1) для позавчера и вчера (ALMATY_TZ)."""

    today = datetime.now(ALMATY_TZ).date()
    day1 = today - timedelta(days=1)  # вчера
    day0 = today - timedelta(days=2)  # позавчера

    p0 = {"since": day0.strftime("%Y-%m-%d"), "until": day0.strftime("%Y-%m-%d")}
    p1 = {"since": day1.strftime("%Y-%m-%d"), "until": day1.strftime("%Y-%m-%d")}
    return p0, p1


def _extract_metrics(row: Dict[str, Any]) -> PeriodMetrics:
    """Строит набор метрик для периода на основе строки из analyze_adsets."""

    spend = float(row.get("spend") or 0.0)
    impr = int(row.get("impr") or 0)
    clicks = int(row.get("clicks") or 0)
    conversions = int(row.get("total") or 0)  # msgs + leads

    # Базовые метрики
    cpa = (spend / conversions) if conversions > 0 else None
    cpm = safe_div(spend * 1000.0, impr)
    cpc = safe_div(spend, clicks)
    ctr = safe_div(clicks, impr) * 100.0
    cvr = safe_div(conversions, clicks) * 100.0

    return PeriodMetrics(
        spend=spend,
        impr=impr,
        clicks=clicks,
        conversions=conversions,
        cpa=cpa,
        cpm=cpm,
        cpc=cpc,
        ctr=ctr,
        cvr=cvr,
    )


def _delta_rel(base: float, current: float) -> float:
    """Относительное изменение current vs base (в долях, не в %).

    Возвращает 0.0, если база <= 0.
    """

    if base <= 0:
        return 0.0
    try:
        return (current - base) / base
    except Exception:
        return 0.0


def _format_value(v: Optional[float], as_percent: bool = False) -> str:
    if v is None:
        return "—"
    if as_percent:
        return f"{v:.1f}%"
    return f"{v:.2f}"


def _classify_metric_change(
    name: str,
    m0: PeriodMetrics,
    m1: PeriodMetrics,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """Возвращает (описание_аномалии_или_None, low_data_flag).

    low_data_flag=True означает, что по объёму данных метрику считать
    аномальной нельзя (мало показов/кликов/конверсий).
    """

    # Подбор метрики
    base_val: Optional[float]
    cur_val: Optional[float]
    thresh: float
    need_impr = False
    need_clicks = False
    need_conv = False
    as_percent = False

    if name == "CPA":
        base_val, cur_val, thresh = m0.cpa, m1.cpa, THRESHOLD_CPA
        need_conv = True
    elif name == "CPC":
        base_val, cur_val, thresh = m0.cpc, m1.cpc, THRESHOLD_CPC
        need_clicks = True
    elif name == "CPM":
        base_val, cur_val, thresh = m0.cpm, m1.cpm, THRESHOLD_CPM
        need_impr = True
    elif name == "CTR":
        base_val, cur_val, thresh = m0.ctr, m1.ctr, THRESHOLD_CTR
        need_impr = True
        need_clicks = True
        as_percent = True
    elif name == "CVR":
        base_val, cur_val, thresh = m0.cvr, m1.cvr, THRESHOLD_CVR
        need_clicks = True
        need_conv = True
        as_percent = True
    else:
        return None, False

    # Фильтр по объёму данных
    if need_impr and (m0.impr < MIN_IMPRESSIONS or m1.impr < MIN_IMPRESSIONS):
        return None, True
    if need_clicks and (m0.clicks < MIN_CLICKS or m1.clicks < MIN_CLICKS):
        return None, True
    if need_conv and (m0.conversions < MIN_CONVERSIONS or m1.conversions < MIN_CONVERSIONS):
        return None, True

    # Особые случаи появления / исчезновения
    if (base_val or 0) == 0 and (cur_val or 0) > 0:
        return {
            "metric": name,
            "kind": "appeared",
            "from": base_val,
            "to": cur_val,
            "as_percent": as_percent,
        }, False

    if (base_val or 0) > 0 and (cur_val or 0) == 0:
        return {
            "metric": name,
            "kind": "disappeared",
            "from": base_val,
            "to": cur_val,
            "as_percent": as_percent,
        }, False

    if base_val is None or cur_val is None or base_val <= 0:
        return None, True

    dr = _delta_rel(base_val, cur_val)
    if abs(dr) < thresh:
        return None, False

    return {
        "metric": name,
        "kind": "delta",
        "from": base_val,
        "to": cur_val,
        "delta_rel": dr,
        "as_percent": as_percent,
    }, False


def _guess_reason_from_changes(changes: Dict[str, Dict[str, Any]]) -> str:
    """Эвристика для текста "Возможная причина" на основе направлений.

    changes: словарь metric -> описание (kind, from, to, delta_rel).
    """

    def _delta_sign(metric: str) -> float:
        info = changes.get(metric)
        if not info:
            return 0.0
        if info.get("kind") != "delta":
            return 0.0
        return float(info.get("delta_rel") or 0.0)

    cpa = _delta_sign("CPA")
    cpc = _delta_sign("CPC")
    cpm = _delta_sign("CPM")
    ctr = _delta_sign("CTR")
    cvr = _delta_sign("CVR")

    # Упрощённые критерии "примерно без изменений"
    def approx_zero(x: float, tol: float = 0.05) -> bool:
        return abs(x) < tol

    # Негативные сценарии
    if cpa > 0 and cpc > 0 and ctr < 0:
        return "Возможная причина: рост стоимости клика и ухудшение реакции на креатив."

    if cpa > 0 and (cpc <= 0 or approx_zero(cpc)) and cvr < 0:
        return "Возможная причина: ухудшение конверсии после клика (сайт/воронка)."

    if cpm > 0 and (ctr < 0 or approx_zero(ctr)):
        return "Возможная причина: рост стоимости показов (конкуренция в аукционе)."

    if ctr < 0 and approx_zero(cpm):
        return "Возможная причина: креатив стал хуже цеплять аудиторию."

    # Позитивный сценарий
    if cpa < 0 and ctr > 0 and cvr > 0:
        return "Возможная причина: улучшение креативов и воронки — позитивная аномалия."

    return (
        "Возможная причина: комплексное изменение трафика и конверсий, "
        "требует дополнительного анализа."
    )


def detect_adset_anomalies(aid: str) -> List[Dict[str, Any]]:
    """Ищет аномалии по адсетам аккаунта для вчера vs позавчера.

    Возвращает список словарей по адсетам с описанием метрик и причин.
    """

    period0, period1 = _build_periods_for_yday_vs_byday()

    # собираем метрики по адсетам для каждого периода
    base_rows = analyze_adsets(aid, period=period0)
    cur_rows = analyze_adsets(aid, period=period1)

    by_id_base: Dict[str, Dict[str, Any]] = {}
    for row in base_rows:
        adset_id = str(row.get("adset_id") or row.get("id") or "")
        if not adset_id:
            continue
        by_id_base[adset_id] = row

    anomalies: List[Dict[str, Any]] = []

    for cur in cur_rows:
        adset_id = str(cur.get("adset_id") or cur.get("id") or "")
        if not adset_id:
            continue

        base = by_id_base.get(adset_id)
        if not base:
            # нет базы позавчера — пропускаем
            continue

        m0 = _extract_metrics(base)
        m1 = _extract_metrics(cur)

        changes: Dict[str, Dict[str, Any]] = {}
        low_data_for: List[str] = []

        for metric_name in ["CPA", "CTR", "CPC", "CPM", "CVR"]:
            desc, low_data = _classify_metric_change(metric_name, m0, m1)
            if low_data:
                low_data_for.append(metric_name)
            if desc:
                changes[metric_name] = desc

        if not changes:
            # ни одной значимой аномалии
            continue

        name = cur.get("name") or base.get("name") or adset_id

        # Оценка, позитивная или негативная аномалия (по CPA)
        cpa_info = changes.get("CPA")
        is_positive = False
        cpa_severity = 0.0
        if cpa_info and cpa_info.get("kind") == "delta":
            dr = float(cpa_info.get("delta_rel") or 0.0)
            cpa_severity = abs(dr)
            is_positive = dr < 0

        reason = _guess_reason_from_changes(changes)

        anomalies.append(
            {
                "adset_id": adset_id,
                "name": name,
                "period0": period0,
                "period1": period1,
                "changes": changes,
                "low_data_for": sorted(set(low_data_for)),
                "reason": reason,
                "is_positive": is_positive,
                "cpa_severity": cpa_severity,
            }
        )

    # сортируем: сначала по серьёзности изменения CPA, затем по имени
    def _sort_key(a: Dict[str, Any]):
        return (-(a.get("cpa_severity") or 0.0), str(a.get("name") or ""))

    anomalies.sort(key=_sort_key)
    return anomalies


def _format_metric_line(info: Dict[str, Any]) -> str:
    metric = info["metric"]
    kind = info["kind"]
    as_percent = bool(info.get("as_percent"))
    v_from = info.get("from")
    v_to = info.get("to")

    if kind == "appeared":
        return (
            f"{metric}: было {_format_value(v_from, as_percent)} → "
            f"стало {_format_value(v_to, as_percent)} (метрика появилась)"
        )

    if kind == "disappeared":
        return (
            f"{metric}: было {_format_value(v_from, as_percent)} → "
            f"стало {_format_value(v_to, as_percent)} (метрика пропала)"
        )

    # kind == "delta"
    dr = float(info.get("delta_rel") or 0.0)
    sign = "+" if dr > 0 else "-"
    pct = abs(dr) * 100.0
    return (
        f"{metric}: {_format_value(v_from, as_percent)} → "
        f"{_format_value(v_to, as_percent)} ({sign}{pct:.0f}%)"
    )


def format_anomaly_message(anom: Dict[str, Any]) -> str:
    """Формирует текст уведомления по одной аномалии адсета.

    Формат примера:

    ⚠️ Аномалия в адсете "<name>"
    Периоды: 01.06.2025 → 02.06.2025

    CPA:  4.10 → 7.85  (+91%)
    CTR:  1.8% → 1.2% (-33%)

    Мало данных для: CVR, CPM

    Возможная причина: ...
    """

    name = anom.get("name") or anom.get("adset_id") or "<без названия>"
    period0 = anom.get("period0") or {}
    period1 = anom.get("period1") or {}

    d0 = period0.get("since") or "?"
    d1 = period1.get("since") or "?"

    is_positive = bool(anom.get("is_positive"))
    emoji = "✅ Позитивная аномалия" if is_positive else "⚠️ Аномалия"

    changes: Dict[str, Dict[str, Any]] = anom.get("changes") or {}
    low_data_for: List[str] = anom.get("low_data_for") or []
    reason: str = anom.get("reason") or ""

    lines: List[str] = [f"{emoji} в адсете \"{name}\"", f"Периоды: {d0} → {d1}"]

    # выводим метрики в фиксированном порядке
    for metric_name in ["CPA", "CTR", "CPC", "CPM", "CVR"]:
        info = changes.get(metric_name)
        if not info:
            continue
        lines.append(_format_metric_line(info))

    if low_data_for:
        low_str = ", ".join(sorted(set(low_data_for)))
        lines.append("")
        lines.append(f"Мало данных для: {low_str}")

    if reason:
        lines.append("")
        lines.append(reason)

    return "\n".join(lines)


def build_anomaly_messages_for_account(aid: str) -> List[str]:
    """Высокоуровневая функция: найти аномалии и подготовить тексты для Telegram.

    Строит заголовок по аккаунту и далее возвращает список сообщений по
    каждому адсету.
    """

    period0, period1 = _build_periods_for_yday_vs_byday()
    d0 = period0["since"]
    d1 = period1["since"]

    anomalies = detect_adset_anomalies(aid)
    if not anomalies:
        return []

    header = (
        f"⚠️ Анализ аномалий по адсетам для {aid}\n"
        f"Периоды: {d0} → {d1}\n"
        f"Найдено аномалий: {len(anomalies)}"
    )

    messages = [header]
    for a in anomalies:
        messages.append(format_anomaly_message(a))

    return messages
        if abs(d_cpm) > 20.0 and base_cpm not in (None, 0):
            issues.append({"metric": "CPM", "delta_pct": d_cpm})

        base_cpc = base.get("cpc")
        cur_cpc = cur.get("cpc")
        d_cpc = _delta_percent(base_cpc, cur_cpc)
        # CPC напрямую в ТЗ не фигурирует по порогу, но можно подсвечивать
        if abs(d_cpc) > 20.0 and base_cpc not in (None, 0):
            issues.append({"metric": "CPC", "delta_pct": d_cpc})

        # Частота — только по текущему периоду
        freq = float(cur.get("freq") or 0.0)
        if freq > 4.0:
            issues.append({"metric": "Frequency", "value": freq})

        base_cvr = _compute_cvr(base)
        cur_cvr = _compute_cvr(cur)
        d_cvr = _delta_percent(base_cvr, cur_cvr)
        if d_cvr < -20.0 and base_cvr > 0:
            issues.append({"metric": "CVR", "delta_pct": d_cvr})

        if not issues:
            continue

        name = cur.get("name") or base.get("name") or adset_id

        reason = _guess_reason(issues)

        anomalies.append(
            {
                "adset_id": adset_id,
                "name": name,
                "issues": issues,
                "reason": reason,
            }
        )

    return anomalies


def _guess_reason(issues: List[Dict[str, Any]]) -> Optional[str]:
    """Простые эвристики для текста "Возможная причина"."""

    has_cpa_up = any(
        i.get("metric") == "CPA" and (i.get("delta_pct") or 0) > 20.0 for i in issues
    )
    has_ctr_down = any(
        i.get("metric") == "CTR" and (i.get("delta_pct") or 0) < -15.0 for i in issues
    )
    has_cvr_down = any(
        i.get("metric") == "CVR" and (i.get("delta_pct") or 0) < -20.0 for i in issues
    )
    has_freq_high = any(i.get("metric") == "Frequency" for i in issues)

    if has_freq_high and (has_ctr_down or has_cvr_down):
        return "Возможная причина: выгорание аудитории"

    if has_cpa_up and has_ctr_down:
        return "Возможная причина: снижение эффективности креатива"

    if has_cpa_up and not has_ctr_down:
        return "Возможная причина: рост стоимости трафика или ухудшение конверсии"

    if has_cvr_down and not has_ctr_down:
        return "Возможная причина: проблемы с воронкой после клика"

    return None


def format_anomaly_message(anom: Dict[str, Any]) -> str:
    """Формирует текст уведомления по одной аномалии адсета.

    Формат:
    ⚠️ Аномалия в адсете <название>:
    • CPA: +27%
    • CTR: −15%
    • Frequency: 4.7 (>4)
    • Возможная причина: выгорание аудитории
    """

    name = anom.get("name") or anom.get("adset_id") or "<без названия>"
    issues: List[Dict[str, Any]] = anom.get("issues") or []
    reason: Optional[str] = anom.get("reason")

    lines = [f"⚠️ Аномалия в адсете {name}:"]

    for issue in issues:
        metric = issue.get("metric")
        if metric == "Frequency":
            value = float(issue.get("value") or 0.0)
            lines.append(f"• Frequency: {value:.2f} (>4)")
            continue

        delta = float(issue.get("delta_pct") or 0.0)
        sign = "+" if delta > 0 else "−"
        lines.append(f"• {metric}: {sign}{abs(delta):.0f}%")

    if reason:
        lines.append(reason)

    return "\n".join(lines)


def build_anomaly_messages_for_account(
    aid: str,
    *,
    baseline_days: int = 7,
    current_days: int = 1,
) -> List[str]:
    """Высокоуровневая функция: найти аномалии и подготовить тексты для Telegram."""

    anomalies = detect_adset_anomalies(aid, baseline_days=baseline_days, current_days=current_days)
    return [format_anomaly_message(a) for a in anomalies]
