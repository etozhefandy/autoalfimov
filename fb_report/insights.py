# fb_report/insights.py

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

from .constants import ALMATY_TZ


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
        days = 14
        return [yesterday - timedelta(days=i) for i in range(days)][::-1]
    elif mode == "month":
        first_of_month = yesterday.replace(day=1)
        days_delta = (yesterday - first_of_month).days + 1
        return [first_of_month + timedelta(days=i) for i in range(days_delta)]
    else:
        days = 7
        return [yesterday - timedelta(days=i) for i in range(days)][::-1]


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


def _heat_symbol(
    convs: int,
    max_convs: int,
) -> str:
    if max_convs <= 0:
        return "⬜"
    if convs <= 0:
        return "⬜"

    ratio = convs / max_convs

    if ratio <= 0.25:
        return "▢"
    elif ratio <= 0.50:
        return "▤"
    elif ratio <= 0.75:
        return "▦"
    else:
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
        return f"🔥 Тепловая карта — {acc_name}\nЗа период ({mode_label}) нет данных."

    max_convs = max(d["total_conversions"] for d in daily) or 0
    total_convs_all = sum(d["total_conversions"] for d in daily)
    total_msgs_all = sum(d["messages"] for d in daily)
    total_leads_all = sum(d["leads"] for d in daily)
    total_spend_all = sum(d["spend"] for d in daily)

    days_with_data = len([d for d in daily if d["total_conversions"] > 0])
    avg_convs = total_convs_all / days_with_data if days_with_data > 0 else 0.0

    lines: List[str] = []

    lines.append(f"🔥 Тепловая карта заявок (💬+📩) — {acc_name}")
    lines.append(f"Период: {mode_label}")
    lines.append("")

    if total_convs_all == 0:
        lines.append("За выбранный период нет заявок (💬+📩).")
        return "\n".join(lines)

    lines.append(
        f"Итого за период: {total_convs_all} заявок "
        f"(💬 {total_msgs_all} + ♿️ {total_leads_all}), "
        f"затраты: {total_spend_all:.2f} $"
    )
    if days_with_data > 0:
        lines.append(f"Среднее заявок в день (по дням с трафиком): {avg_convs:.2f}")
    lines.append("")

    header = "Дата       Инт.  Заявки  💬   ♿️   💵"
    lines.append(header)
    lines.append("-" * len(header))

    for row in daily:
        day = row["date"]
        convs = row["total_conversions"]
        msgs = row["messages"]
        leads = row["leads"]
        spend = row["spend"]

        symbol = _heat_symbol(convs, max_convs)
        date_str = day.strftime("%d.%m")

        lines.append(
            f"{date_str:<10} {symbol}   {convs:>3}   {msgs:>3}  {leads:>3}  {spend:>6.2f} $"
        )

    lines.append("")
    lines.append("Легенда интенсивности:")
    lines.append("⬜ — нет заявок")
    lines.append("▢ — низкая активность")
    lines.append("▤ — средняя активность")
    lines.append("▦ — высокая активность")
    lines.append("▩ — пиковая активность")

    return "\n".join(lines)
