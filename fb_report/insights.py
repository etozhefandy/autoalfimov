# fb_report/insights.py

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any

from .constants import ALMATY_TZ


def load_local_insights(
    aid: str,
    period: Dict[str, str],
    label: str,
) -> Optional[Dict[str, Any]]:
    return None


def save_local_insights(
    aid: str,
    period: Dict[str, str],
    label: str,
    data: Dict[str, Any],
):
    return None


def extract_actions(insights: Optional[Dict[str, Any]]) -> List[str]:
    return []


def _build_day_period(day: datetime) -> Tuple[Dict[str, str], str]:
    day = day.replace(hour=0, minute=0, second=0, microsecond=0)
    period = {
        "since": day.strftime("%Y-%m-%d"),
        "until": day.strftime("%Y-%m-%d"),
    }
    label = day.strftime("%d.%m.%Y")
    return period, label


def _iter_days_for_mode(mode: str) -> List[datetime]:
    now = datetime.now(ALMATY_TZ)
    yesterday = (now - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    if mode == "14":
        return [yesterday - timedelta(days=i) for i in range(14)][::-1]
    elif mode == "month":
        first = yesterday.replace(day=1)
        count = (yesterday - first).days + 1
        return [first + timedelta(days=i) for i in range(count)]
    else:
        return [yesterday - timedelta(days=i) for i in range(7)][::-1]


def _load_daily_totals_for_account(
    aid: str,
    mode: str,
) -> List[Dict[str, Optional[float]]]:
    from .reporting import get_cached_report
    from .jobs import _parse_totals_from_report_text

    days = _iter_days_for_mode(mode)
    result: List[Dict[str, Optional[float]]] = []

    for day in days:
        period, label = _build_day_period(day)
        try:
            txt = get_cached_report(aid, period, label)
        except Exception:
            txt = None

        if not txt:
            result.append(
                {
                    "date": day,
                    "messages": 0,
                    "leads": 0,
                    "total_conversions": 0,
                    "spend": 0.0,
                }
            )
            continue

        totals = _parse_totals_from_report_text(txt) or {}
        result.append(
            {
                "date": day,
                "messages": int(totals.get("messages") or 0),
                "leads": int(totals.get("leads") or 0),
                "total_conversions": int(totals.get("total_conversions") or 0),
                "spend": float(totals.get("spend") or 0.0),
            }
        )

    return result


def _heat_symbol(convs: int, max_convs: int) -> str:
    if max_convs <= 0 or convs <= 0:
        return "⬜"

    r = convs / max_convs

    if r <= 0.25:
        return "▢"
    elif r <= 0.50:
        return "▤"
    elif r <= 0.75:
        return "▦"
    return "▩"


def _mode_label(mode: str) -> str:
    if mode == "14":
        return "последние 14 дней"
    if mode == "month":
        return "текущий месяц"
    return "последние 7 дней"


def build_heatmap_for_account(
    aid: str,
    get_account_name,
    mode: str = "7",
) -> str:
    acc_name = get_account_name(aid)
    mode_label = _mode_label(mode)

    daily = _load_daily_totals_for_account(aid, mode)

    if not daily:
        return f"🔥 Тепловая карта — {acc_name}\n(нет данных)"

    max_convs = max(d["total_conversions"] for d in daily) or 0
    total_msgs = sum(d["messages"] for d in daily)
    total_leads = sum(d["leads"] for d in daily)
    total_convs = sum(d["total_conversions"] for d in daily)
    total_spend = sum(d["spend"] for d in daily)

    valid_days = len([d for d in daily if d["total_conversions"] > 0])
    avg_daily = total_convs / valid_days if valid_days else 0

    lines: List[str] = []
    lines.append(f"🔥 Тепловая карта заявок (💬+📩) — {acc_name}")
    lines.append(f"Период: {mode_label}")
    lines.append("")
    if total_convs == 0:
        lines.append("За выбранный период нет заявок (💬+📩).")
        return "\n".join(lines)

    lines.append(
        f"Итого: {total_convs} заявок "
        f"(💬 {total_msgs} + ♿️ {total_leads}), "
        f"затраты {total_spend:.2f} $"
    )
    lines.append(f"Среднее заявок/день (по дням с трафиком): {avg_daily:.2f}")
    lines.append("")
    header = "Дата       Инт.  Заявки  💬   ♿️   💵"
    lines.append(header)
    lines.append("-" * len(header))

    for row in daily:
        d = row["date"].strftime("%d.%m")
        symbol = _heat_symbol(row["total_conversions"], max_convs)
        lines.append(
            f"{d:<10} {symbol}   "
            f"{row['total_conversions']:>3}   {row['messages']:>3}  "
            f"{row['leads']:>3}  {row['spend']:>6.2f} $"
        )

    lines.append("")
    lines.append("⬜ — нет заявок")
    lines.append("▢ — низкая активность")
    lines.append("▤ — средняя активность")
    lines.append("▦ — высокая активность")
    lines.append("▩ — пик")

    return "\n".join(lines)
