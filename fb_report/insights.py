# fb_report/insights.py

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

from services.facebook_api import fetch_insights
from services.storage import (
    load_local_insights as _load_local_insights,
    save_local_insights as _save_local_insights,
)

from .constants import ALMATY_TZ
from .storage import get_account_name


# ================== ЛОКАЛЬНЫЙ КЭШ ИНСАЙТОВ ==================
def load_local_insights(aid: str) -> dict:
    """
    Читает локальный файл с инсайтами аккаунта через services.storage.
    Совместимо со старым интерфейсом fb_report.
    """
    return _load_local_insights(aid) or {}


def save_local_insights(aid: str, store: dict) -> None:
    """Атомарно сохраняет инсайты аккаунта через services.storage."""
    _save_local_insights(aid, store)


# ================== ОБРАБОТКА ACTIONS / ЗАЯВОК ==================
def extract_actions(insight: dict) -> Dict[str, float]:
    """
    Старое поведение: берём массив actions и делаем dict {action_type: value}.
    Это 1-в-1 логика из твоего старого fb_report.py.
    """
    acts = insight.get("actions", []) or []
    out: Dict[str, float] = {}
    for a in acts:
        at = a.get("action_type")
        if not at:
            continue
        try:
            val = float(a.get("value", 0) or 0)
        except Exception:
            val = 0.0
        out[at] = val
    return out


def _blend_totals(ins: dict):
    """
    Полностью как в старом боте:

    - msgs = onsite_conversion.messaging_conversation_started_7d
    - leads = Website Submit Applications
              или offsite_conversion.fb_pixel_submit_application
              или offsite_conversion.fb_pixel_lead
              или lead
    - total = msgs + leads
    - blended = spend / total (если total > 0), иначе None

    Возвращает (spend, msgs, leads, total, blended).
    """
    acts = extract_actions(ins)
    spend = float(ins.get("spend", 0) or 0)

    msgs = int(
        acts.get("onsite_conversion.messaging_conversation_started_7d", 0) or 0
    )

    leads = int(
        acts.get("Website Submit Applications", 0)
        or acts.get("offsite_conversion.fb_pixel_submit_application", 0)
        or acts.get("offsite_conversion.fb_pixel_lead", 0)
        or acts.get("lead", 0)
        or 0
    )

    total = msgs + leads
    blended = (spend / total) if total > 0 else None

    return spend, msgs, leads, total, blended


# ================== ВСПОМОГАТЕЛЬНОЕ ДЛЯ ДНЕЙ ==================
def _build_day_period(day: datetime) -> Tuple[Dict[str, str], str]:
    """Формирует period/label для одного дня (как в дневном отчёте)."""
    day = day.replace(hour=0, minute=0, second=0, microsecond=0)
    period = {
        "since": day.strftime("%Y-%m-%d"),
        "until": day.strftime("%Y-%m-%d"),
    }
    label = day.strftime("%d.%m.%Y")
    return period, label


def _iter_days_for_mode(mode: str) -> List[datetime]:
    """
    mode: "7" | "14" | "month"
    Возвращает список дат (datetime) ДЛЯ ПРОШЕДШИХ дней
    (с вчерашнего назад до нужного количества).
    """
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
        # по умолчанию 7 дней
        days = 7
        return [yesterday - timedelta(days=i) for i in range(days)][::-1]


def _fetch_daily_insight(aid: str, day: datetime) -> Optional[dict]:
    """
    Точечный запрос инсайта за один день для аккаунта.
    Использует общий fetch_insights из services.facebook_api,
    который сам работает с локальным кешем инсайтов.
    """
    since_until = day.strftime("%Y-%m-%d")
    period = {"since": since_until, "until": since_until}
    return fetch_insights(aid, period)


def _load_daily_totals_for_account(
    aid: str, mode: str
) -> List[Dict[str, Optional[float]]]:
    """
    Для каждого дня периода вытаскивает инсайты по аккаунту
    и парсит из них:
    - messages
    - leads
    - total_conversions (💬+📩)
    - spend
    """
    days = _iter_days_for_mode(mode)
    result: List[Dict[str, Optional[float]]] = []

    for day in days:
        ins = _fetch_daily_insight(aid, day)

        if not ins:
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

        spend, msgs, leads, total, _ = _blend_totals(ins)
        result.append(
            {
                "date": day,
                "messages": msgs,
                "leads": leads,
                "total_conversions": total,
                "spend": spend,
            }
        )

    return result


# ================== ВИЗУАЛ ТЕПЛОВОЙ КАРТЫ ==================
def _heat_symbol(convs: int, max_convs: int) -> str:
    """
    4 стадии «теплоты» + пустой квадрат при 0:
    0          -> ⬜
    >0..25%    -> ▢
    >25..50%   -> ▤
    >50..75%   -> ▦
    >75..100%  -> ▩
    """
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
    get_account_name_fn=get_account_name,
    mode: str = "7",
) -> str:
    """
    Строит «тепловую карту» по дням для аккаунта:
    - берёт инсайты за каждый день периода
    - считает заявки через старый _blend_totals (💬+📩)
    - отображает интенсивность по 4 уровням
    - показывает средние заявки в день
    """
    acc_name = get_account_name_fn(aid)
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
    avg_convs = (
        total_convs_all / days_with_data if days_with_data > 0 else 0.0
    )

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
