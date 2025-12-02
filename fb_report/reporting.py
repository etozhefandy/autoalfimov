# fb_report/reporting.py
import json
from datetime import datetime, timedelta
import re
from typing import Tuple, Dict, Any

from facebook_business.adobjects.adaccount import AdAccount
from telegram.ext import ContextTypes

from .constants import (
    ALMATY_TZ,
    REPORT_CACHE_FILE,
    REPORT_CACHE_TTL,
)
from .storage import (
    get_account_name,
    metrics_flags,
    is_active,
    load_accounts,
)
from .insights import (
    load_local_insights,
    save_local_insights,
    extract_actions,
    _blend_totals,
)
from .constants import DEFAULT_REPORT_CHAT


def fmt_int(n) -> str:
    try:
        return f"{int(float(n)):,}".replace(",", " ")
    except Exception:
        return "0"


def _load_report_cache() -> dict:
    try:
        with open(REPORT_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_report_cache(d: dict):
    from .storage import _atomic_write_json

    _atomic_write_json(REPORT_CACHE_FILE, d)


def period_key(period) -> str:
    if isinstance(period, dict):
        since = period.get("since", "")
        until = period.get("until", "")
        return f"range:{since}:{until}"
    return f"preset:{str(period)}"


def fetch_insight(aid: str, period):
    store = load_local_insights(aid)
    key = period_key(period)

    if key in store:
        name = get_account_name(aid)
        return name, store[key]

    acc = AdAccount(aid)
    fields = [
        "impressions",
        "cpm",
        "clicks",
        "cpc",
        "spend",
        "actions",
        "results",
        "cost_per_result",
        "link_clicks",
        "ctr",
        "link_ctr",
    ]

    params = {"level": "account"}
    if isinstance(period, dict):
        params["time_range"] = period
    else:
        params["date_preset"] = period

    data = acc.get_insights(fields=fields, params=params)
    name = acc.api_get(fields=["name"]).get("name", get_account_name(aid))

    if not data:
        ins_dict = None
    else:
        raw = data[0]
        if hasattr(raw, "export_all_data"):
            ins_dict = raw.export_all_data()
        else:
            ins_dict = dict(raw)

    store[key] = ins_dict
    save_local_insights(aid, store)

    return name, ins_dict


def get_cached_report(aid: str, period, label: str = "") -> str:
    key = period_key(period)
    now_ts = datetime.now().timestamp()

    cache = _load_report_cache()
    acc_cache = cache.get(aid, {})
    item = acc_cache.get(key)

    if item and (now_ts - float(item.get("ts", 0))) <= REPORT_CACHE_TTL:
        return item.get("text", "")

    text = build_report(aid, period, label)

    cache.setdefault(aid, {})
    cache[aid][key] = {"text": text, "ts": now_ts}
    _save_report_cache(cache)

    return text


def build_report(aid: str, period, label: str = "") -> str:
    from .storage import get_account_name as _get_name

    try:
        name, ins = fetch_insight(aid, period)
    except Exception as e:
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return f"⚠ Ошибка по {_get_name(aid)}:\n\n{e}"

    badge = "🟢" if is_active(aid) else "🔴"
    hdr = f"{badge} <b>{name}</b>{(' (' + label + ')') if label else ''}\n"
    if not ins:
        return hdr + "Нет данных за выбранный период"

    body = []

    impressions = int(ins.get("impressions", 0) or 0)
    cpm = float(ins.get("cpm", 0) or 0)
    clicks_all = int(ins.get("clicks", 0) or 0)
    cpc = float(ins.get("cpc", 0) or 0)
    spend = float(ins.get("spend", 0) or 0)
    link_clicks = int(ins.get("link_clicks", 0) or 0)

    try:
        ctr_all = float(ins.get("ctr", 0) or 0)
    except Exception:
        ctr_all = 0.0

    try:
        ctr_link = float(ins.get("link_ctr", 0) or 0)
    except Exception:
        ctr_link = 0.0

    body.append(f"👁 Показы: {fmt_int(impressions)}")
    body.append(f"🎯 CPM: {cpm:.2f} $")
    body.append(f"🖱 Клики(все): {fmt_int(clicks_all)}")
    body.append(f"🔗 Клики: {fmt_int(link_clicks)}")
    body.append(f"📈 CTR(все): {ctr_all:.2f} %")
    body.append(f"📈 CTR(клики): {ctr_link:.2f} %")
    body.append(f"💸 CPC: {cpc:.2f} $")
    body.append(f"💵 Затраты: {spend:.2f} $")

    acts = extract_actions(ins)
    flags = metrics_flags(aid)

    show_msg = bool(flags.get("messaging", True))
    show_leads = bool(flags.get("leads", False))
    show_total = bool(flags.get("total", True))

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

    if show_msg:
        body.append(f"✉️ Переписки: {msgs}")
        if msgs > 0 and spend > 0:
            body.append(f"💬💲 Цена переписки: {spend / msgs:.2f} $")

    if show_leads:
        body.append(f"📩 Лиды: {leads}")
        if leads > 0 and spend > 0:
            body.append(f"📩💲 Цена лида: {spend / leads:.2f} $")

    total = msgs + leads

    results_raw = 0
    try:
        results_raw = int(ins.get("results", 0) or 0)
    except Exception:
        results_raw = 0

    if total == 0 and results_raw > 0:
        total = results_raw

    if show_total:
        body.append("—")
        if total > 0 and spend > 0:
            blended = spend / total
            body.append(f"🧮 Итого: {total} заявок, CPA = {blended:.2f} $")
        else:
            body.append("🧮 Итого: 0 заявок")

    return hdr + "\n".join(body)


async def send_period_report(
    ctx: ContextTypes.DEFAULT_TYPE, chat_id: str, period, label: str = ""
):
    from .storage import load_accounts, get_enabled_accounts_in_order

    store = load_accounts()

    for aid in get_enabled_accounts_in_order():
        if not store.get(aid, {}).get("enabled", True):
            continue

        if period == "today":
            txt = build_report(aid, period, label)
        else:
            txt = get_cached_report(aid, period, label)

        if txt:
            await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")


def build_comparison_report(
    aid: str, period1, label1: str, period2, label2: str
) -> str:
    from .storage import get_account_name

    def _extract_since(p):
        if isinstance(p, dict):
            s = p.get("since")
            try:
                return datetime.strptime(s, "%Y-%m-%d")
            except Exception:
                return None
        return None

    d1 = _extract_since(period1)
    d2 = _extract_since(period2)
    if d1 and d2 and d1 > d2:
        period1, period2 = period2, period1
        label1, label2 = label2, label1

    try:
        _, ins1 = fetch_insight(aid, period1)
        _, ins2 = fetch_insight(aid, period2)
    except Exception as e:
        return f"⚠ Ошибка при получении данных: {e.__class__.__name__}: {str(e)}"

    if not ins1 and not ins2:
        return f"Нет данных по {get_account_name(aid)} за оба периода."

    flags = metrics_flags(aid)

    def _stat(ins):
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
        spend, msgs, leads, total, blended = _blend_totals(ins)
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

    def _fmt_cpa(cpa):
        return f"{cpa:.2f} $" if cpa is not None else "—"

    def _pct_change(old: float, new: float):
        if old == 0:
            return None
        return (new - old) / old * 100.0

    txt_lines = []
    txt_lines.append(f"📊 <b>{get_account_name(aid)}</b>")
    txt_lines.append(f"Старый период: {label1}")
    txt_lines.append(f"Новый период: {label2}")
    txt_lines.append("")

    txt_lines.append(f"1️⃣ <b>{label1}</b> (старый период)")
    txt_lines.append(f"   👁 Охваты: {fmt_int(s1['impr'])}")
    txt_lines.append(f"   🖱 Клики: {fmt_int(s1['clicks'])}")
    txt_lines.append(f"   💵 Затраты: {_fmt_money(s1['spend'])}")
    txt_lines.append(f"   🎯 CPM: {s1['cpm']:.2f} $")
    txt_lines.append(f"   💸 CPC: {s1['cpc']:.2f} $")
    if flags.get("messaging", True):
        txt_lines.append(f"   💬 Переписки: {s1['msgs']}")
    if flags.get("leads", False):
        txt_lines.append(f"   📩 Лиды: {s1['leads']}")
    if flags.get("messaging", True) or flags.get("leads", False):
        txt_lines.append(f"   🧮 Заявки всего: {s1['total']}")
        txt_lines.append(f"   🎯 CPA: {_fmt_cpa(s1['cpa'])}")
    txt_lines.append("")

    txt_lines.append(f"2️⃣ <b>{label2}</b> (новый период)")
    txt_lines.append(f"   👁 Охваты: {fmt_int(s2['impr'])}")
    txt_lines.append(f"   🖱 Клики: {fmt_int(s2['clicks'])}")
    txt_lines.append(f"   💵 Затраты: {_fmt_money(s2['spend'])}")
    txt_lines.append(f"   🎯 CPM: {s2['cpm']:.2f} $")
    txt_lines.append(f"   💸 CPC: {s2['cpc']:.2f} $")
    if flags.get("messaging", True):
        txt_lines.append(f"   💬 Переписки: {s2['msgs']}")
    if flags.get("leads", False):
        txt_lines.append(f"   📩 Лиды: {s2['leads']}")
    if flags.get("messaging", True) or flags.get("leads", False):
        txt_lines.append(f"   🧮 Заявки всего: {s2['total']}")
        txt_lines.append(f"   🎯 CPA: {_fmt_cpa(s2['cpa'])}")
    txt_lines.append("")

    txt_lines.append("3️⃣ <b>Сравнение (новый vs старый)</b>")

    def _add_diff(
        label: str,
        old_v: float,
        new_v: float,
        is_better_lower: bool = False,
        fmt_func=None,
        icon: str = "",
    ):
        if fmt_func is None:
            fmt_func = lambda x: str(int(x))
        base = f"{icon} {label}: {fmt_func(old_v)} → {fmt_func(new_v)}"
        pct = _pct_change(old_v, new_v)
        if pct is None:
            txt_lines.append(base + " (Δ %: н/д)")
            return
        if pct == 0:
            sign = "➡️"
        else:
            sign = (
                "📈"
                if ((not is_better_lower and pct > 0) or (is_better_lower and pct < 0))
                else "📉"
            )
        txt_lines.append(f"{base}   {sign} {pct:+.1f}%")

    _add_diff("Охваты", s1["impr"], s2["impr"], False, fmt_int, "👁")
    _add_diff("Клики", s1["clicks"], s2["clicks"], False, fmt_int, "🖱")
    _add_diff("Затраты", s1["spend"], s2["spend"], False, _fmt_money, "💵")
    _add_diff("CPM", s1["cpm"], s2["cpm"], True, lambda v: f"{v:.2f} $", "🎯")
    _add_diff("CPC", s1["cpc"], s2["cpc"], True, lambda v: f"{v:.2f} $", "💸")

    if flags.get("messaging", True):
        _add_diff(
            "Переписки",
            s1["msgs"],
            s2["msgs"],
            False,
            lambda v: str(int(v)),
            "💬",
        )
    if flags.get("leads", False):
        _add_diff(
            "Лиды",
            s1["leads"],
            s2["leads"],
            False,
            lambda v: str(int(v)),
            "📩",
        )

    if flags.get("messaging", True) or flags.get("leads", False):
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

    return "\n".join(txt_lines)


_RANGE_RE = re.compile(
    r"^\s*(\d{2})\.(\d{2})\.(\d{4})\s*-\s*(\d{2})\.(\d{2})\.(\d{4})\s*$"
)


def parse_range(s: str):
    m = _RANGE_RE.match(s)
    if not m:
        return None
    d1 = datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
    d2 = datetime(int(m.group(6)), int(m.group(5)), int(m.group(4)))
    if d1 > d2:
        d1, d2 = d2, d1
    return (
        {"since": d1.strftime("%Y-%m-%d"), "until": d2.strftime("%Y-%m-%d")},
        f"{d1.strftime('%d.%m')}-{d2.strftime('%d.%m')}",
    )


def parse_two_ranges(s: str):
    parts = [p.strip() for p in re.split(r"[;\n]+", s) if p.strip()]
    if len(parts) != 2:
        return None
    r1 = parse_range(parts[0])
    r2 = parse_range(parts[1])
    if not r1 or not r2:
        return None
    return r1, r2
