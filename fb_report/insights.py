# fb_report/insights.py

from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any

from services.storage import (
    load_local_insights as _load_local_insights,
    save_local_insights as _save_local_insights,
)

from services.heatmap_store import load_snapshot, list_snapshot_hours
from services.facebook_api import deny_fb_api_calls

from .constants import ALMATY_TZ
from .storage import get_account_name

from services.analytics import count_leads_from_actions


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


def extract_costs(insight: dict) -> Dict[str, float]:
    costs = insight.get("cost_per_action_type", []) or []
    out: Dict[str, float] = {}
    for c in costs:
        at = (c or {}).get("action_type")
        if not at:
            continue
        try:
            val = float((c or {}).get("value", 0) or 0)
        except Exception:
            val = 0.0
        out[at] = val
    return out


def _blend_totals(ins: dict, *, aid: Optional[str] = None):
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

    leads = count_leads_from_actions(acts, aid=aid)

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
    Legacy stub (FB API reads removed).
    """
    return None


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
        daily_from_snapshots = _get_daily_stats_from_snapshots(aid, day)
        if daily_from_snapshots is not None:
            result.append(daily_from_snapshots)
        else:
            result.append(
                {
                    "date": day,
                    "messages": 0,
                    "leads": 0,
                    "total_conversions": 0,
                    "spend": 0.0,
                }
            )

    return result


def _get_daily_stats_from_snapshots(aid: str, day: datetime) -> Optional[Dict[str, Any]]:
    date_str = day.strftime("%Y-%m-%d")

    msgs = 0
    leads = 0
    total = 0
    spend = 0.0
    any_ready = False

    with deny_fb_api_calls(reason="insights_daily_from_snapshots"):
        for h in list_snapshot_hours(str(aid), date_str=str(date_str)):
            snap = load_snapshot(str(aid), date_str=str(date_str), hour=int(h)) or {}
            if str(snap.get("status") or "") not in {"ready", "ready_low_confidence"}:
                continue
            any_ready = True
            for r in (snap.get("rows") or []):
                if not isinstance(r, dict):
                    continue
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
                try:
                    spend += float(r.get("spend") or 0.0)
                except Exception:
                    pass

    if not any_ready:
        return None
    return {
        "date": day,
        "messages": int(msgs or 0),
        "leads": int(leads or 0),
        "total_conversions": int(total or 0),
        "spend": float(spend or 0.0),
    }


def _iter_days_for_hourly_mode(mode: str) -> List[datetime]:
    """Возвращает список дат для почасовой тепловой карты.

    mode: "today" | "yday" | "7d" (по умолчанию 7 дней).
    """

    now = datetime.now(ALMATY_TZ)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    if mode == "today":
        return [today]
    if mode == "yday":
        return [today - timedelta(days=1)]

    # Последние 7 дней, включая сегодня
    days = 7
    return [today - timedelta(days=i) for i in range(days)][::-1]


def _hourly_mode_label(mode: str) -> str:
    if mode == "today":
        return "сегодня"
    if mode == "yday":
        return "вчера"
    return "последние 7 дней"


def build_hourly_heatmap_for_account(
    aid: str,
    get_account_name_fn=get_account_name,
    mode: str = "7d",
) -> Tuple[str, Dict[str, Any]]:
    """Строит почасовую тепловую карту для аккаунта на базе heatmap snapshots.

    Возвращает:
      - готовый текст для Telegram
      - summary-словарь для ИИ-анализа (матрица день×час и агрегаты).
    """

    acc_name = get_account_name_fn(aid)
    mode_label = _hourly_mode_label(mode)

    def _get_hour_bucket(date_str: str, hour_key: str) -> dict:
        try:
            h_int = int(str(hour_key))
        except Exception:
            return {}

        with deny_fb_api_calls(reason="insights_hour_bucket"):
            snap = load_snapshot(str(aid), date_str=str(date_str), hour=int(h_int)) or {}
        if str(snap.get("status") or "") not in {"ready", "ready_low_confidence"}:
            return {}

        msgs = 0
        leads = 0
        total = 0
        spend = 0.0
        for r in (snap.get("rows") or []):
            if not isinstance(r, dict):
                continue
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
            try:
                spend += float(r.get("spend") or 0.0)
            except Exception:
                pass

        return {
            "messages": int(msgs or 0),
            "leads": int(leads or 0),
            "total": int(total or 0),
            "spend": float(spend or 0.0),
        }

    days = _iter_days_for_hourly_mode(mode)
    hours = [f"{h:02d}" for h in range(24)]

    matrix: List[Dict[str, Any]] = []
    max_convs = 0
    total_convs_all = 0
    total_spend_all = 0.0

    for day in days:
        day_key = day.strftime("%Y-%m-%d")

        row_totals: List[int] = []
        row_spends: List[float] = []
        day_total = 0
        day_spend = 0.0

        for h in hours:
            bucket = _get_hour_bucket(day_key, h)
            val = int(bucket.get("total", 0) or 0)
            sp = float(bucket.get("spend", 0.0) or 0.0)
            row_totals.append(val)
            row_spends.append(sp)
            day_total += val
            day_spend += sp
            if val > max_convs:
                max_convs = val

        total_convs_all += day_total
        total_spend_all += day_spend

        matrix.append(
            {
                "date": day,
                "date_key": day_key,
                "totals_per_hour": row_totals,
                "spend_per_hour": row_spends,
                "total_conversions": day_total,
                "spend": day_spend,
            }
        )

    # Текстовая визуализация
    lines: List[str] = []
    lines.append(f"🔥 Тепловая карта по часам (заявки 💬+📩) — {acc_name}")
    lines.append(f"Период: {mode_label}")
    lines.append("")

    if not matrix or total_convs_all == 0:
        lines.append("За выбранный период нет заявок (💬+📩) по часам.")
    else:
        lines.append(
            f"Итого за период: {total_convs_all} заявок, затраты: {total_spend_all:.2f} $"
        )
        lines.append("")
        lines.append("Строки — дни, символы — часы 00–23:")
        lines.append("")

        for row in matrix:
            day_dt: datetime = row["date"]
            date_str = day_dt.strftime("%d.%m")
            vals: List[int] = row["totals_per_hour"]
            symbols = "".join(_heat_symbol(v, max_convs) for v in vals)
            lines.append(f"{date_str}: {symbols}")

        lines.append("")
        lines.append("Легенда интенсивности:")
        lines.append("⬜ — нет заявок")
        lines.append("▢ — низкая активность")
        lines.append("▤ — средняя активность")
        lines.append("▦ — высокая активность")
        lines.append("▩ — пиковая активность")

    text = "\n".join(lines)

    summary: Dict[str, Any] = {
        "account_id": aid,
        "account_name": acc_name,
        "mode": mode,
        "mode_label": mode_label,
        "days": [
            {
                "date": row["date_key"],
                "totals_per_hour": row["totals_per_hour"],
                "spend_per_hour": row.get("spend_per_hour") or [],
                "total_conversions": row["total_conversions"],
                "spend": row["spend"],
            }
            for row in matrix
        ],
        "total_conversions_all": total_convs_all,
        "total_spend_all": total_spend_all,
        "live_today": {},
    }

    return text, summary


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


def build_weekday_heatmap_for_account(
    aid: str,
    get_account_name_fn=get_account_name,
    mode: str = "7",
) -> Tuple[str, Dict[str, Any]]:
    acc_name = get_account_name_fn(aid)

    daily = _load_daily_totals_for_account(aid, mode)

    # 0=Mon..6=Sun
    by_wd: Dict[int, Dict[str, Any]] = {
        i: {"convs": 0, "spend": 0.0, "days": 0} for i in range(7)
    }
    for row in daily:
        day: datetime = row["date"]
        wd = int(day.weekday())
        by_wd[wd]["convs"] += int(row.get("total_conversions", 0) or 0)
        by_wd[wd]["spend"] += float(row.get("spend", 0.0) or 0.0)
        by_wd[wd]["days"] += 1

    wd_labels = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    wd_rows: List[Dict[str, Any]] = []
    max_convs = 0
    total_convs_all = 0
    total_spend_all = 0.0
    for i in range(7):
        convs = int(by_wd[i]["convs"] or 0)
        spend = float(by_wd[i]["spend"] or 0.0)
        days_cnt = int(by_wd[i]["days"] or 0)
        avg = (convs / float(days_cnt)) if days_cnt > 0 else 0.0
        if convs > max_convs:
            max_convs = convs
        total_convs_all += convs
        total_spend_all += spend
        wd_rows.append(
            {
                "weekday": i,
                "weekday_label": wd_labels[i],
                "conversions": convs,
                "spend": spend,
                "days": days_cnt,
                "avg_conversions": avg,
            }
        )

    lines: List[str] = []
    lines.append(f"📅 Тепловая карта по дням недели (💬+📩) — {acc_name}")
    lines.append(f"Период: { _mode_label(mode) }")
    lines.append("")

    if total_convs_all <= 0:
        lines.append("За выбранный период нет заявок (💬+📩).")
    else:
        lines.append(
            f"Итого: {total_convs_all} заявок, затраты: {total_spend_all:.2f} $"
        )
        lines.append("")
        lines.append("День  Инт.  Заявки  Ср/день  💵")
        lines.append("-" * 28)
        for r in wd_rows:
            symbol = _heat_symbol(int(r["conversions"]), max_convs)
            lines.append(
                f"{r['weekday_label']:<3}  {symbol}   {int(r['conversions']):>3}     {float(r['avg_conversions']):>5.1f}  {float(r['spend']):>6.2f} $"
            )

        lines.append("")
        lines.append("Легенда интенсивности:")
        lines.append("⬜ — нет заявок")
        lines.append("▢ — низкая активность")
        lines.append("▤ — средняя активность")
        lines.append("▦ — высокая активность")
        lines.append("▩ — пиковая активность")

    summary: Dict[str, Any] = {
        "account_id": aid,
        "account_name": acc_name,
        "mode": mode,
        "mode_label": _mode_label(mode),
        "weekdays": wd_rows,
        "total_conversions_all": total_convs_all,
        "total_spend_all": total_spend_all,
    }

    return "\n".join(lines), summary


def build_heatmap_monitoring_summary(
    aid: str,
    get_account_name_fn=get_account_name,
) -> Tuple[str, Dict[str, Any]]:
    acc_name = get_account_name_fn(aid)

    text_wd, summary_wd = build_weekday_heatmap_for_account(aid, get_account_name_fn, mode="7")
    text_hr, summary_hr = build_hourly_heatmap_for_account(aid, get_account_name_fn, mode="7d")

    summary: Dict[str, Any] = {
        "account_id": aid,
        "account_name": acc_name,
        "weekday": summary_wd,
        "hourly": summary_hr,
    }

    lines: List[str] = []
    lines.append(f"🔥 Тепловая карта — сводная (неделя + часы) — {acc_name}")
    lines.append("")
    lines.append("=== По дням недели ===")
    lines.append(text_wd)
    lines.append("")
    lines.append("=== По часам ===")
    lines.append(text_hr)

    return "\n".join(lines), summary
