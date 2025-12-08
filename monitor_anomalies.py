from __future__ import annotations

from typing import Any, Dict, List, Optional

from services.analytics import analyze_adsets, safe_div


def _compute_cvr(row: Dict[str, Any]) -> float:
    """Конверсия (CVR) в %: total_conversions / clicks * 100.

    total = msgs + leads (поле total в analytics.parse_insight).
    """

    clicks = float(row.get("clicks") or 0.0)
    total = float(row.get("total") or 0.0)
    if clicks <= 0:
        return 0.0
    return safe_div(total * 100.0, clicks)


def _delta_percent(base: Optional[float], current: Optional[float]) -> float:
    """Процентное изменение current относительно base.

    Возвращает 0.0, если база некорректна или <= 0.
    """

    if base is None or current is None:
        return 0.0
    if base == 0:
        return 0.0
    try:
        return (float(current) - float(base)) / float(base) * 100.0
    except Exception:
        return 0.0


def detect_adset_anomalies(
    aid: str,
    *,
    baseline_days: int = 7,
    current_days: int = 1,
) -> List[Dict[str, Any]]:
    """Ищет аномалии по адсетам аккаунта.

    База: метрики за baseline_days (по умолчанию 7 дней до вчера).
    Текущие: метрики за current_days (по умолчанию 1 день).

    Метрики:
      - CPA
      - CPM
      - CTR
      - CPC
      - Frequency
      - CVR (total / clicks)

    Условия аномалий:
      - |ΔCPA| > 20%
      - падение CTR > 15%
      - |ΔCPM| > 20%
      - Frequency > 4
      - падение CVR > 20%

    Возвращает список словарей вида:
      {
        "adset_id": "...",
        "name": "...",
        "issues": [
          {"metric": "CPA", "delta_pct": +27.3},
          {"metric": "CTR", "delta_pct": -16.1},
          ...
        ],
        "reason": "Возможная причина: выгорание аудитории",
      }
    """

    baseline = analyze_adsets(aid, days=baseline_days)
    current = analyze_adsets(aid, days=current_days)

    base_by_id: Dict[str, Dict[str, Any]] = {}
    for row in baseline:
        adset_id = row.get("adset_id") or row.get("id")
        if not adset_id:
            continue
        base_by_id[str(adset_id)] = row

    anomalies: List[Dict[str, Any]] = []

    for cur in current:
        adset_id = cur.get("adset_id") or cur.get("id")
        if not adset_id:
            continue
        adset_id = str(adset_id)

        base = base_by_id.get(adset_id)
        if not base:
            # Нет базы для сравнения — пропускаем
            continue

        issues: List[Dict[str, Any]] = []

        base_cpa = base.get("cpa")
        cur_cpa = cur.get("cpa")
        d_cpa = _delta_percent(base_cpa, cur_cpa)
        if abs(d_cpa) > 20.0 and base_cpa not in (None, 0):
            issues.append({"metric": "CPA", "delta_pct": d_cpa})

        base_ctr = base.get("ctr")
        cur_ctr = cur.get("ctr")
        d_ctr = _delta_percent(base_ctr, cur_ctr)
        # нас интересует именно падение CTR
        if d_ctr < -15.0 and base_ctr not in (None, 0):
            issues.append({"metric": "CTR", "delta_pct": d_ctr})

        base_cpm = base.get("cpm")
        cur_cpm = cur.get("cpm")
        d_cpm = _delta_percent(base_cpm, cur_cpm)
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
