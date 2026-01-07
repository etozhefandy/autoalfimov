# services/reports.py
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timedelta

from config import ALMATY_TZ
from services.facebook_api import fetch_insights
from services.analytics import count_leads_from_actions
from services.storage import (
    load_accounts,
    save_accounts,
    get_cached_report_entry,
    set_cached_report_entry,
    is_cache_fresh,
    period_key,
)


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

def fmt_int(v: Any) -> str:
    """Форматирование целых чисел с разделителями пробелами (как в старом отчёте)."""
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


def blend_totals(
    ins: Dict[str, Any],
    *,
    aid: Optional[str] = None,
) -> Tuple[float, int, int, int, Optional[float]]:
    """
    Возвращает:
    (spend, msgs, leads, total, blended_cpa)
    """
    spend = float(ins.get("spend", 0) or 0)
    acts = extract_actions(ins)

    msgs = int(
        acts.get("onsite_conversion.messaging_conversation_started_7d", 0) or 0
    )
    leads = count_leads_from_actions(acts, aid=aid)

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
    Главный отчёт по одному аккаунту.

    Поведение и формат текста приведены к старой рабочей версии
    fb_report/reporting.build_report:
    - показы, CPM
    - клики (все) + CTR по всем кликам
    - "Клики" по ссылке (link_click) + CTR по ссылке
    - CPC / затраты
    - переписки / лиды / blended CPA (💬+📩)
    """
    name = get_account_name(aid)

    try:
        ins = fetch_insights(aid, period)
    except Exception as e:  # максимально бережно, как в старой реализации
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return f"⚠ Ошибка по {name}:\n\n{e}"

    badge = "🟢" if is_active_account(ins or {}) else "🔴"
    header = f"{badge} <b>{name}</b>" + (f" ({label})" if label else "") + "\n"

    if not ins:
        return header + "Нет данных за выбранный период"

    # Базовые метрики
    impressions = int(ins.get("impressions", 0) or 0)
    cpm = float(ins.get("cpm", 0) or 0)
    clicks_all = int(ins.get("clicks", 0) or 0)
    cpc = float(ins.get("cpc", 0) or 0)
    spend, msgs, leads, total_conv, blended_cpa = blend_totals(ins, aid=aid)

    # actions → link_clicks
    acts = extract_actions(ins)
    flags = get_metrics_flags(aid)

    link_clicks = int(acts.get("link_click", 0) or 0)

    # CTR'ы считаем сами
    ctr_all = (clicks_all / impressions * 100.0) if impressions > 0 else 0.0
    ctr_link = (link_clicks / impressions * 100.0) if impressions > 0 else 0.0

    body: list[str] = []
    body.append(f"👁 Показы: {fmt_int(impressions)}")
    body.append(f"🎯 CPM: {cpm:.2f} $")
    body.append(f"🖱 Клики (все): {fmt_int(clicks_all)}")
    body.append(f"📈 CTR (все клики): {ctr_all:.2f} %")
    body.append(f"🔗 Клики: {fmt_int(link_clicks)}")
    body.append(f"📈 CTR (по ссылке): {ctr_link:.2f} %")
    body.append(f"💸 CPC: {cpc:.2f} $")
    body.append(f"💵 Затраты: {spend:.2f} $")

    if flags["messaging"]:
        body.append(f"✉️ Переписки: {msgs}")
        if msgs > 0:
            body.append(f"💬💲 Цена переписки: {(spend / msgs):.2f} $")

    if flags["leads"]:
        body.append(f"📩 Лиды: {leads}")
        if leads > 0:
            body.append(f"📩💲 Цена лида: {(spend / leads):.2f} $")

    if flags["messaging"] and flags["leads"]:
        body.append("—")
        if total_conv > 0:
            body.append(
                f"🧮 Итого: {total_conv} заявок, CPA = {blended_cpa:.2f} $"
            )
        else:
            body.append("🧮 Итого: 0 заявок")

    return header + "\n".join(body)


# ========== ОБЁРТКА С КЭШЕМ ==========

def get_cached_report(aid: str, period: Any, label: str = "") -> str:
    """
    Возвращает отчёт, используя:
    - текстовый кэш,
    - или строит заново,
    - сохраняет в кэш.
    """
    # Для периода "today" всегда берём живые данные без кэша,
    # чтобы отчёт отражал текущую ситуацию.
    if period == "today":
        return build_report(aid, period, label)

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
    period2: Any, label2: str,
) -> str:
    """Сравнение двух периодов (поведение, как в fb_report/reporting)."""
    name = get_account_name(aid)

    ins1 = fetch_insights(aid, period1)
    ins2 = fetch_insights(aid, period2)

    if ins1 is None and ins2 is None:
        return f"Нет данных по <b>{name}</b> за выбранные периоды."

    flags = get_metrics_flags(aid)

    def _stat(ins: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not ins:
            return {
                "impr": 0,
                "cpm": 0.0,
                "clicks": 0,
                "cpc": 0.0,
                "spend": 0.0,
                "msgs": 0,
                "leads": 0,
                "total": 0,
                "cpa": None,
            }
        impr = int(ins.get("impressions", 0) or 0)
        cpm = float(ins.get("cpm", 0) or 0)
        clicks = int(ins.get("clicks", 0) or 0)
        cpc = float(ins.get("cpc", 0) or 0)
        spend, msgs, leads, total, blended = blend_totals(ins, aid=aid)
        return {
            "impr": impr,
            "cpm": cpm,
            "clicks": clicks,
            "cpc": cpc,
            "spend": spend,
            "msgs": msgs,
            "leads": leads,
            "total": total,
            "cpa": blended,
        }

    s1 = _stat(ins1)
    s2 = _stat(ins2)

    def _fmt_money(v: float) -> str:
        return f"{v:.2f} $"

    def _fmt_cpa(cpa: Optional[float]) -> str:
        return f"{cpa:.2f} $" if cpa is not None else "—"

    def _pct(old: float, new: float) -> Optional[float]:
        if old == 0:
            return None
        return (new - old) / old * 100.0

    lines: list[str] = []
    lines.append(f"📊 <b>{name}</b>")
    lines.append(f"Старый период: {label1}")
    lines.append(f"Новый период: {label2}")
    lines.append("")

    # 1️⃣ Старый
    lines.append(f"1️⃣ <b>{label1}</b> (старый период)")
    lines.append(f"   👁 Охваты: {fmt_int(s1['impr'])}")
    lines.append(f"   🖱 Клики: {fmt_int(s1['clicks'])}")
    lines.append(f"   💵 Затраты: {_fmt_money(s1['spend'])}")
    lines.append(f"   🎯 CPM: {s1['cpm']:.2f} $")
    lines.append(f"   💸 CPC: {s1['cpc']:.2f} $")
    if flags["messaging"]:
        lines.append(f"   💬 Переписки: {s1['msgs']}")
    if flags["leads"]:
        lines.append(f"   📩 Лиды: {s1['leads']}")
    if flags["messaging"] or flags["leads"]:
        lines.append(f"   🧮 Заявки всего: {s1['total']}")
        lines.append(f"   🎯 CPA: {_fmt_cpa(s1['cpa'])}")
    lines.append("")

    # 2️⃣ Новый
    lines.append(f"2️⃣ <b>{label2}</b> (новый период)")
    lines.append(f"   👁 Охваты: {fmt_int(s2['impr'])}")
    lines.append(f"   🖱 Клики: {fmt_int(s2['clicks'])}")
    lines.append(f"   💵 Затраты: {_fmt_money(s2['spend'])}")
    lines.append(f"   🎯 CPM: {s2['cpm']:.2f} $")
    lines.append(f"   💸 CPC: {s2['cpc']:.2f} $")
    if flags["messaging"]:
        lines.append(f"   💬 Переписки: {s2['msgs']}")
    if flags["leads"]:
        lines.append(f"   📩 Лиды: {s2['leads']}")
    if flags["messaging"] or flags["leads"]:
        lines.append(f"   🧮 Заявки всего: {s2['total']}")
        lines.append(f"   🎯 CPA: {_fmt_cpa(s2['cpa'])}")
    lines.append("")

    # 3️⃣ Сравнение (новый vs старый)
    lines.append("3️⃣ <b>Сравнение (новый vs старый)</b>")

    def _add_diff(
        label: str,
        old_v: float,
        new_v: float,
        is_better_lower: bool = False,
        fmt_func=None,
        icon: str = "",
    ) -> None:
        if fmt_func is None:
            fmt_func = lambda x: str(int(x))
        base = f"{icon} {label}: {fmt_func(old_v)} → {fmt_func(new_v)}"
        pct = _pct(old_v, new_v)
        if pct is None:
            lines.append(base + " (Δ %: н/д)")
            return
        if pct == 0:
            sign = "➡️"
        else:
            sign = (
                "📈"
                if ((not is_better_lower and pct > 0) or (is_better_lower and pct < 0))
                else "📉"
            )
        lines.append(f"{base}   {sign} {pct:+.1f}%")

    _add_diff("Охваты", s1["impr"], s2["impr"], False, fmt_int, "👁")
    _add_diff("Клики", s1["clicks"], s2["clicks"], False, fmt_int, "🖱")
    _add_diff("Затраты", s1["spend"], s2["spend"], False, _fmt_money, "💵")
    _add_diff("CPM", s1["cpm"], s2["cpm"], True, lambda v: f"{v:.2f} $", "🎯")
    _add_diff("CPC", s1["cpc"], s2["cpc"], True, lambda v: f"{v:.2f} $", "💸")

    if flags["messaging"]:
        _add_diff(
            "Переписки",
            s1["msgs"],
            s2["msgs"],
            False,
            lambda v: str(int(v)),
            "💬",
        )
    if flags["leads"]:
        _add_diff(
            "Лиды",
            s1["leads"],
            s2["leads"],
            False,
            lambda v: str(int(v)),
            "📩",
        )

    if flags["messaging"] or flags["leads"]:
        _add_diff(
            "Заявки всего",
            s1["total"],
            s2["total"],
            False,
            lambda v: str(int(v)),
            "🧮",
        )
        if s1["cpa"] is not None and s2["cpa"] is not None:
            _add_diff("CPA", s1["cpa"], s2["cpa"], True, _fmt_cpa, "🎯")

    return "\n".join(lines)
