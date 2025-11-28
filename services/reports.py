# services/reports.py
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timedelta

from config import ALMATY_TZ
from services.facebook_api import fetch_insights
from services.storage import (
    load_accounts,
    save_accounts,
    get_cached_report_entry,
    set_cached_report_entry,
    is_cache_fresh,
    period_key,
)
from services.storage import load_accounts
from config import ALMATY_TZ


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def fmt_int(v: Any) -> str:
    """Форматирование чисел с пробелами."""
    try:
        return f"{int(float(v)):,}".replace(",", " ")
    except Exception:
        return "0"


def extract_actions(ins: Dict[str, Any]) -> Dict[str, float]:
    """
    Вытаскиваем actions в виде dict:
    {
      "onsite_conversion.messaging_conversation_started_7d": 12,
      "Website Submit Applications": 3,
      ...
    }
    """
    acts = ins.get("actions", []) or []
    out = {}
    for a in acts:
        t = a.get("action_type")
        v = float(a.get("value", 0) or 0)
        out[t] = v
    return out


def blend_totals(ins: Dict[str, Any]) -> Tuple[float, int, int, int, Optional[float]]:
    """
    Возвращает:
    (spend, msgs, leads, total, blended_cpa)
    """
    spend = float(ins.get("spend", 0) or 0)
    acts = extract_actions(ins)

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


def get_metrics_flags(aid: str) -> Dict[str, bool]:
    """
    Читаем настройки аккаунта:
    - показывать переписки?
    - показывать лиды?
    """
    store = load_accounts()
    row = store.get(aid, {})

    m = row.get("metrics", {}) or {}
    return {
        "messaging": bool(m.get("messaging", False)),
        "leads": bool(m.get("leads", False)),
    }


def get_account_name(aid: str) -> str:
    """Имя аккаунта из accounts.json."""
    store = load_accounts()
    row = store.get(aid, {})
    name = row.get("name")
    return name or aid


def is_active_account(ins: Dict[str, Any]) -> bool:
    """
    Для простоты: если есть хотя бы spend OR impressions —
    считаем активным. (Настоящий статус получается из FB API,
    но здесь нам нужно только для бейджа.)
    """
    if not ins:
        return False
    spend = float(ins.get("spend", 0) or 0)
    impr = float(ins.get("impressions", 0) or 0)
    return (spend > 0) or (impr > 0)


# ========== ОСНОВНОЙ ОТЧЁТ ПО ОДНОМУ АККАУНТУ ==========

def build_report(aid: str, period: Any, label: str = "") -> str:
    """
    Главный отчёт по одному аккаунту:
    - формирует строку
    - вытаскивает инсайты
    - форматирует основные метрики
    """
    ins = fetch_insights(aid, period)
    name = get_account_name(aid)

    if ins is None:
        badge = "🔴"
        return f"{badge} <b>{name}</b> — Нет данных за выбранный период"

    badge = "🟢" if is_active_account(ins) else "🔴"

    lines = []
    title = f"{badge} <b>{name}</b>"
    if label:
        title += f" ({label})"
    lines.append(title)

    # Основные метрики
    lines.append(f"👁 Показы: {fmt_int(ins.get('impressions', 0))}")
    lines.append(f"🎯 CPM: {round(float(ins.get('cpm', 0) or 0), 2)} $")
    lines.append(f"🖱 Клики: {fmt_int(ins.get('clicks', 0))}")
    lines.append(f"💸 CPC: {round(float(ins.get('cpc', 0) or 0), 2)} $")

    spend, msgs, leads, total, blended = blend_totals(ins)
    lines.append(f"💵 Затраты: {round(spend, 2)} $")

    flags = get_metrics_flags(aid)

    if flags["messaging"]:
        lines.append(f"✉️ Переписки: {msgs}")
        if msgs > 0:
            lines.append(f"💬💲 Цена переписки: {round(spend / msgs, 2)} $")

    if flags["leads"]:
        lines.append(f"📩 Лиды: {leads}")
        if leads > 0:
            lines.append(f"📩💲 Цена лида: {round(spend / leads, 2)} $")

    if flags["messaging"] and flags["leads"]:
        lines.append("—")
        if total > 0:
            lines.append(f"🧮 Итого: {total} заявок, CPA = {round(blended, 2)} $")
        else:
            lines.append("🧮 Итого: 0 заявок")

    return "\n".join(lines)


# ========== ОБЁРТКА С КЭШЕМ ==========

def get_cached_report(aid: str, period: Any, label: str = "") -> str:
    """
    Возвращает отчёт, используя:
    - текстовый кэш,
    - или строит заново,
    - сохраняет в кэш.
    """
    key = period_key(period)
    entry = get_cached_report_entry(aid, key)

    if entry and is_cache_fresh(entry):
        return entry.get("text", "")

    txt = build_report(aid, period, label)
    set_cached_report_entry(aid, key, txt)
    return txt


# ========== ОТЧЁТ СРАВНЕНИЯ ДВУХ ПЕРИОДОВ ==========

def build_comparison_report(
    aid: str,
    period1: Any, label1: str,
    period2: Any, label2: str
) -> str:
    """
    Сравнение двух периодов:
    - старый период
    - новый период
    - дельта
    """
    name = get_account_name(aid)

    ins1 = fetch_insights(aid, period1)
    ins2 = fetch_insights(aid, period2)

    if ins1 is None and ins2 is None:
        return f"Нет данных по <b>{name}</b> за выбранные периоды."

    flags = get_metrics_flags(aid)

    # Маленькая функция для аккуратной статистики
    def _stat(ins: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not ins:
            return {
                "impr": 0, "spend": 0, "clicks": 0, "cpm": 0, "cpc": 0,
                "msgs": 0, "leads": 0, "total": 0, "cpa": None,
            }
        spend, msgs, leads, total, cpa = blend_totals(ins)
        return {
            "impr": int(ins.get("impressions", 0) or 0),
            "clicks": int(ins.get("clicks", 0) or 0),
            "cpm": float(ins.get("cpm", 0) or 0),
            "cpc": float(ins.get("cpc", 0) or 0),
            "spend": spend,
            "msgs": msgs,
            "leads": leads,
            "total": total,
            "cpa": cpa,
        }

    s1 = _stat(ins1)
    s2 = _stat(ins2)

    # Функция проц. изменения
    def pct(old: float, new: float):
        if old == 0:
            return None
        return (new - old) / old * 100.0

    lines = []
    lines.append(f"📊 <b>{name}</b>")
    lines.append(f"Старый период: {label1}")
    lines.append(f"Новый период: {label2}")
    lines.append("")

    # Блок старого периода
    lines.append(f"1️⃣ <b>{label1}</b>")
    lines.append(f"   👁 Охваты: {fmt_int(s1['impr'])}")
    lines.append(f"   🖱 Клики: {fmt_int(s1['clicks'])}")
    lines.append(f"   💵 Затраты: {s1['spend']:.2f} $")
    lines.append(f"   🎯 CPM: {s1['cpm']:.2f} $")
    lines.append(f"   💸 CPC: {s1['cpc']:.2f} $")
    if flags["messaging"]: lines.append(f"   💬 Переписки: {s1['msgs']}")
    if flags["leads"]:     lines.append(f"   📩 Лиды: {s1['leads']}")
    if flags["messaging"] or flags["leads"]:
        lines.append(f"   🧮 Итого заявок: {s1['total']}")
        lines.append(f"   🎯 CPA: {s1['cpa'] if s1['cpa'] is not None else '—'}")
    lines.append("")

    # Новый период
    lines.append(f"2️⃣ <b>{label2}</b>")
    lines.append(f"   👁 Охваты: {fmt_int(s2['impr'])}")
    lines.append(f"   🖱 Клики: {fmt_int(s2['clicks'])}")
    lines.append(f"   💵 Затраты: {s2['spend']:.2f} $")
    lines.append(f"   🎯 CPM: {s2['cpm']:.2f} $")
    lines.append(f"   💸 CPC: {s2['cpc']:.2f} $")
    if flags["messaging"]: lines.append(f"   💬 Переписки: {s2['msgs']}")
    if flags["leads"]:     lines.append(f"   📩 Лиды: {s2['leads']}")
    if flags["messaging"] or flags["leads"]:
        lines.append(f"   🧮 Итого заявок: {s2['total']}")
        lines.append(f"   🎯 CPA: {s2['cpa'] if s2['cpa'] is not None else '—'}")
    lines.append("")

    # Дельты
    lines.append("3️⃣ <b>Сравнение</b>")
    fields = [
        ("Охваты", "impr", False, "👁"),
        ("Клики", "clicks", False, "🖱"),
        ("Затраты", "spend", False, "💵"),
        ("CPM", "cpm", True, "🎯"),
        ("CPC", "cpc", True, "💸"),
    ]

    for label, field, lower_is_better, icon in fields:
        old = s1[field]
        new = s2[field]
        base = f"{icon} {label}: {fmt_int(old)} → {fmt_int(new)}"
        diff = pct(old, new)

        if diff is None:
            lines.append(base + " (Δ н/д)")
            continue

        sign = "📈" if ((not lower_is_better and diff > 0) or (lower_is_better and diff < 0)) else "📉"
        lines.append(f"{base}   {sign} {diff:+.1f}%")

    # заявки / CPA
    if flags["messaging"]:
        old, new = s1["msgs"], s2["msgs"]
        diff = pct(old, new)
        lines.append(f"💬 Переписки: {old} → {new} ({diff:+.1f}%)" if diff else f"💬 Переписки: {old} → {new}")

    if flags["leads"]:
        old, new = s1["leads"], s2["leads"]
        diff = pct(old, new)
        lines.append(f"📩 Лиды: {old} → {new} ({diff:+.1f}%)" if diff else f"📩 Лиды: {old} → {new}")

    if flags["messaging"] or flags["leads"]:
        old, new = s1["total"], s2["total"]
        diff = pct(old, new)
        lines.append(f"🧮 Заявки: {old} → {new} ({diff:+.1f}%)" if diff else f"🧮 Заявки: {old} → {new}")

        old_cpa, new_cpa = s1["cpa"], s2["cpa"]
        if old_cpa is not None and new_cpa is not None:
            diff = pct(old_cpa, new_cpa)
            lines.append(f"🎯 CPA: {old_cpa:.2f} $ → {new_cpa:.2f} $ ({diff:+.1f}%)")

    return "\n".join(lines)
