# fb_report/reporting.py

import json
from datetime import datetime
import re
from typing import Any

from facebook_business.adobjects.adaccount import AdAccount
from telegram.ext import ContextTypes

from .constants import (
    ALMATY_TZ,
    REPORT_CACHE_FILE,
    REPORT_CACHE_TTL,
    DEFAULT_REPORT_CHAT,
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
    extract_costs,
    _blend_totals,
)

from services.analytics import analyze_campaigns, analyze_adsets


# ========= Утилиты форматирования =========
def fmt_int(n) -> str:
    try:
        return f"{int(float(n)):,}".replace(",", " ")
    except Exception:
        return "0"


# ========= КЕШ ОТЧЁТОВ =========
def _load_report_cache() -> dict:
    try:
        with open(REPORT_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_report_cache(d: dict):
    # локальный импорт, чтобы не создавать циклы
    from .storage import _atomic_write_json

    _atomic_write_json(REPORT_CACHE_FILE, d)


def period_key(period) -> str:
    """
    Единый ключ для любых периодов:
    - dict с since/until → range:YYYY-MM-DD:YYYY-MM-DD
    - пресет ("today", "yesterday", "last_7d" etc) → preset:NAME
    """
    if isinstance(period, dict):
        since = period.get("since", "")
        until = period.get("until", "")
        return f"range:{since}:{until}"
    return f"preset:{str(period)}"


# ========== ИНСАЙТЫ (сырые данные) ==========
def fetch_insight(aid: str, period):
    """
    Достаёт инсайты:
    - сначала из локального кэша (load_local_insights)
    - если нет — запрашивает у Facebook
    - важно: ВСЕГДА приводим AdsInsights к обычному dict

    ВНИМАНИЕ: тут НЕТ полей link_clicks / link_ctr / results / cost_per_result,
    чтобы не ловить (#100) от Graph API.
    """
    store = load_local_insights(aid) or {}
    key = period_key(period)

    # Для периода "today" всегда берём свежие данные из API,
    # игнорируя имеющуюся запись в локальном кеше.
    use_cache = not (isinstance(period, str) and period == "today")

    if use_cache and key in store:
        name = get_account_name(aid)
        return name, store[key]

    acc = AdAccount(aid)
    # минимальный набор полей, с которыми у нас всё работает
    fields = [
        "impressions",
        "cpm",
        "clicks",
        "cpc",
        "spend",
        "actions",
        "cost_per_action_type",
    ]

    params: dict[str, Any] = {"level": "account"}
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


# ========== КЭШ ТЕКСТОВЫХ ОТЧЁТОВ ==========
def get_cached_report(aid: str, period, label: str = "") -> str:
    """
    Возвращает текст отчёта из кеша, если свежий,
    иначе строит заново и обновляет кеш.
    """
    # Для "today" всегда считаем отчёт на лету, без использования кэша.
    if period == "today":
        return build_report(aid, period, label)

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


# ========== СБОРКА ОТЧЁТА ПО АККАУНТУ ==========
def build_report(aid: str, period, label: str = "") -> str:
    """
    Базовый отчёт по аккаунту:
    - показы, CPM
    - клики (все) + CTR по всем кликам
    - "Клики" по ссылке (link_click) + CTR по ссылке
    - CPC / затраты
    - переписки / лиды / blended CPA (как в старом боте)
    """
    try:
        name, ins = fetch_insight(aid, period)
    except Exception as e:
        err = str(e)
        if "code: 200" in err or "403" in err or "permissions" in err.lower():
            return ""
        return f"⚠ Ошибка по {get_account_name(aid)}:\n\n{e}"

    badge = "🟢" if is_active(aid) else "🔴"
    hdr = f"{badge} <b>{name}</b>{(' (' + label + ')') if label else ''}\n"
    if not ins:
        return hdr + "Нет данных за выбранный период"

    # Базовые метрики
    impressions = int(ins.get("impressions", 0) or 0)
    cpm = float(ins.get("cpm", 0) or 0)
    clicks_all = int(ins.get("clicks", 0) or 0)
    cpc = float(ins.get("cpc", 0) or 0)
    spend = float(ins.get("spend", 0) or 0)

    acts = extract_actions(ins)
    costs = extract_costs(ins)
    flags = metrics_flags(aid)

    # link_click берём из actions (action_type="link_click"),
    # не трогая fields=... у запроса, чтобы не ломать insights.
    link_clicks = int(acts.get("link_click", 0) or 0)

    # CTR'ы считаем сами
    ctr_all = (clicks_all / impressions * 100.0) if impressions > 0 else 0.0
    ctr_link = (link_clicks / impressions * 100.0) if impressions > 0 else 0.0

    # Заявки (как в старом боте)
    # msgs + leads и blended CPA рассчитываем через _blend_totals
    # (он уже использует те же action_type, что и раньше).
    _, msgs, leads, total_conv, blended_cpa = _blend_totals(ins)

    msg_action = "onsite_conversion.messaging_conversation_started_7d"
    msg_cpa = costs.get(msg_action)
    if msgs <= 0:
        msg_cpa = None

    lead_actions = [
        "Website Submit Applications",
        "offsite_conversion.fb_pixel_submit_application",
        "offsite_conversion.fb_pixel_lead",
        "lead",
    ]
    leads_cost_total = 0.0
    leads_count_total = 0
    for lt in lead_actions:
        cnt = int(acts.get(lt, 0) or 0)
        if cnt <= 0:
            continue
        leads_count_total += cnt
        cpa_val = costs.get(lt)
        if cpa_val is not None and float(cpa_val) > 0:
            leads_cost_total += float(cpa_val) * float(cnt)
    lead_cpa = (
        (leads_cost_total / float(leads_count_total))
        if leads_count_total > 0 and leads_cost_total > 0
        else None
    )

    body = []
    body.append(f"👁 Показы: {fmt_int(impressions)}")
    body.append(f"🎯 CPM: {cpm:.2f} $")
    body.append(f"🖱 Клики (все): {fmt_int(clicks_all)}")
    body.append(f"📈 CTR (все клики): {ctr_all:.2f} %")

    body.append(f"🔗 Клики (по ссылке): {fmt_int(link_clicks)}")
    body.append(f"📈 CTR (по ссылке): {ctr_link:.2f} %")

    if cpc > 0:
        body.append(f"💸 CPC: {cpc:.2f} $")
    else:
        body.append("💸 CPC: —")

    if spend > 0:
        body.append(f"💵 Затраты: {spend:.2f} $")
    else:
        body.append("💵 Затраты: —")

    if flags["messaging"]:
        body.append(f"✉️ Переписки: {msgs}")
        if msg_cpa is not None and float(msg_cpa) > 0:
            body.append(f"💬💲 Цена переписки: {float(msg_cpa):.2f} $")
        else:
            body.append("💬💲 Цена переписки: —")

    if flags["leads"]:
        body.append(f"♿️ Лиды: {leads}")
        if lead_cpa is not None and float(lead_cpa) > 0:
            body.append(f"♿️💲 Цена лида: $ {float(lead_cpa):.2f}")
        else:
            body.append("♿️💲 Цена лида: —")

    # Blended CPA показываем только при включённых переписках и лидах одновременно
    # и когда обе метрики реально > 0.
    if flags.get("messaging") and flags.get("leads") and msgs > 0 and leads > 0:
        body.extend(format_blended_block(spend, msgs, leads).split("\n"))

    return hdr + "\n".join(body)


def format_blended_block(total_spend: float, msgs: int, leads: int) -> str:
    total_actions = int(msgs or 0) + int(leads or 0)
    spend = float(total_spend or 0.0)
    if total_actions > 0:
        blended_cpa = spend / float(total_actions)
        cpa_line = f"CPA: $ {blended_cpa:.2f}"
    else:
        cpa_line = "CPA: —"

    return "\n".join(
        [
            "────────────",
            "🧮 Blended CPA",
            f"Заявок: {total_actions}",
            cpa_line,
            f"Затраты: $ {spend:.2f}",
        ]
    )


def _strip_leading_separator(block: str) -> str:
    if not block:
        return block
    lines = block.split("\n")
    if lines and lines[0].strip() == "────────────":
        lines = lines[1:]
    return "\n".join(lines)


def _collapse_double_separators(text: str) -> str:
    if not text:
        return text
    out: list[str] = []
    for line in text.split("\n"):
        if out and out[-1].strip() == "────────────" and line.strip() == "────────────":
            continue
        out.append(line)
    return "\n".join(out)


def get_account_blended_totals(aid: str, period) -> tuple[float, int, int]:
    try:
        _, ins = fetch_insight(aid, period)
    except Exception:
        return (0.0, 0, 0)

    spend = float((ins or {}).get("spend", 0) or 0)
    _, msgs, leads, _, _ = _blend_totals(ins or {})
    return (spend, int(msgs or 0), int(leads or 0))


def format_entity_line(
    idx: int,
    name: str,
    spend: float,
    msgs: int,
    leads: int,
    msg_cpa: float | None,
    lead_cpa: float | None,
    flags: dict,
) -> str | None:
    eff_msgs = int(msgs or 0) if flags.get("messaging") else 0
    eff_leads = int(leads or 0) if flags.get("leads") else 0

    # Если обе цели выключены или по ним 0 — строку всё равно показываем,
    # чтобы кампании/адсеты со spend>0 не пропадали из отчёта.

    spend_f = float(spend or 0.0)
    parts = [f"{idx}) {name}", f"$ {spend_f:.2f}"]

    # Одна цель на строку: доминирующая по количеству, при равенстве — лиды.
    if eff_leads >= eff_msgs:
        # При равенстве (в т.ч. 0/0) — лиды.
        parts.append(f"♿️ лиды {eff_leads}")
        if lead_cpa is not None and float(lead_cpa) > 0:
            parts.append(f"цена лида $ {float(lead_cpa):.2f}")
        else:
            parts.append("цена лида —")
    else:
        parts.append(f"переписки {eff_msgs}")
        if msg_cpa is not None and float(msg_cpa) > 0:
            parts.append(f"цена переписки $ {float(msg_cpa):.2f}")
        else:
            parts.append("цена переписки —")

    return " — ".join(parts)


def _format_entity_block(
    name: str,
    spend: float,
    msgs: int,
    leads: int,
    msg_cpa: float | None,
    lead_cpa: float | None,
    flags: dict,
) -> str:
    lines: list[str] = []
    lines.append(str(name or "<без названия>"))
    lines.append(f"Затраты: $ {float(spend or 0.0):.2f}")

    if flags.get("messaging") and int(msgs or 0) > 0:
        cpa_part = f" (CPA $ {float(msg_cpa):.2f})" if msg_cpa is not None and float(msg_cpa) > 0 else " (CPA —)"
        lines.append(f"Переписки: {int(msgs or 0)}{cpa_part}")

    if flags.get("leads") and int(leads or 0) > 0:
        cpa_part = f" (CPA $ {float(lead_cpa):.2f})" if lead_cpa is not None and float(lead_cpa) > 0 else " (CPA —)"
        lines.append(f"Лиды: {int(leads or 0)}{cpa_part}")

    return "\n".join(lines)


def _truncate_entity_blocks(
    *,
    header: str,
    entities: list[dict],
    flags: dict,
    max_chars: int,
    current_chars: int,
    kind: str,
) -> tuple[str, int]:
    shown_blocks: list[str] = []
    for e in entities:
        name = str((e or {}).get("name") or "<без названия>")
        spend = float((e or {}).get("spend", 0.0) or 0.0)
        msgs = int((e or {}).get("msgs", 0) or 0)
        leads = int((e or {}).get("leads", 0) or 0)
        block = _format_entity_block(
            name,
            spend,
            msgs,
            leads,
            (e or {}).get("msg_cpa"),
            (e or {}).get("lead_cpa"),
            flags,
        )

        candidate = header
        if shown_blocks:
            candidate += "\n\n" + "\n\n".join(shown_blocks)
        candidate += "\n\n" + block

        if current_chars + len(candidate) > max_chars:
            break
        shown_blocks.append(block)

    remaining = max(0, len(entities) - len(shown_blocks))
    if not shown_blocks:
        return header + "\nнет данных за период", remaining

    text = header + "\n\n" + "\n\n".join(shown_blocks)
    if remaining > 0:
        tail = f"\n\n…и ещё {remaining} {kind}"
        if current_chars + len(text) + len(tail) <= max_chars:
            text += tail
    return text, remaining


def build_account_report(
    aid: str,
    period,
    level: str,
    label: str = "",
    top_n: int = 5,
) -> str:
    lvl = str(level or "ACCOUNT").upper()
    if lvl == "OFF":
        return ""

    base = build_report(aid, period, label)
    if not base:
        return ""

    flags = metrics_flags(aid)

    acc_spend, acc_msgs, acc_leads = get_account_blended_totals(aid, period)
    acc_blended_block = format_blended_block(acc_spend, acc_msgs, acc_leads)
    acc_blended_after_sections = _strip_leading_separator(acc_blended_block)

    from .storage import load_accounts

    store = load_accounts()
    mr = (store.get(aid, {}) or {}).get("morning_report", {}) or {}
    show_blended_after_sections = mr.get("show_blended_after_sections", True)

    # Blended показываем только при включённых переписках и лидах одновременно
    # и когда обе метрики реально > 0.
    show_blended = (
        bool(flags.get("messaging"))
        and bool(flags.get("leads"))
        and int(acc_msgs or 0) > 0
        and int(acc_leads or 0) > 0
    )

    if lvl == "ACCOUNT":
        return base

    sep = "\n────────────\n"

    tg_max_chars = 3900
    current_chars = len(base)

    chunks: list[str] = []

    camps: list[dict] = []
    try:
        camps = analyze_campaigns(aid, period=period) or []
    except Exception:
        camps = []

    # Единственный обязательный фильтр: spend > 0
    camps_spend = [c for c in (camps or []) if float((c or {}).get("spend", 0.0) or 0.0) > 0]
    camps_text, _ = _truncate_entity_blocks(
        header="📣 Кампании",
        entities=camps_spend,
        flags=flags,
        max_chars=tg_max_chars,
        current_chars=current_chars + len(sep),
        kind="кампаний",
    )
    chunks.append(camps_text)
    current_chars += len(sep) + len(camps_text)
    if show_blended_after_sections and show_blended:
        if current_chars + len(sep) + len(acc_blended_after_sections) <= tg_max_chars:
            chunks.append(acc_blended_after_sections)
            current_chars += len(sep) + len(acc_blended_after_sections)

    if lvl == "ADSET":
        adsets: list[dict] = []
        try:
            adsets = analyze_adsets(aid, period=period) or []
        except Exception:
            adsets = []

        # Единственный обязательный фильтр: spend > 0
        adsets_spend = [a for a in (adsets or []) if float((a or {}).get("spend", 0.0) or 0.0) > 0]
        adsets_text, _ = _truncate_entity_blocks(
            header="🧩 Адсеты",
            entities=adsets_spend,
            flags=flags,
            max_chars=tg_max_chars,
            current_chars=current_chars + len(sep),
            kind="адсетов",
        )
        chunks.append(adsets_text)
        current_chars += len(sep) + len(adsets_text)
        if show_blended_after_sections and show_blended:
            if current_chars + len(sep) + len(acc_blended_after_sections) <= tg_max_chars:
                chunks.append(acc_blended_after_sections)
                current_chars += len(sep) + len(acc_blended_after_sections)

    # Разделитель обязателен между блоками.
    out = base + sep + sep.join(chunks)
    return _collapse_double_separators(out)


async def send_period_report(
    ctx: ContextTypes.DEFAULT_TYPE,
    chat_id: str,
    period,
    label: str = "",
):
    """
    Всегда шлём отчёты ТОЛЬКО по enabled=True аккаунтам.
    За 'today' — всегда живые данные (build_report),
    за остальные периоды — через кеш.
    """
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
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=txt,
                parse_mode="HTML",
            )


# ======== Сравнение периодов =========
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

    # 1️⃣ Старый
    txt_lines.append(f"1️⃣ <b>{label1}</b> (старый период)")
    txt_lines.append(f"   👁 Охваты: {fmt_int(s1['impr'])}")
    txt_lines.append(f"   🖱 Клики: {fmt_int(s1['clicks'])}")
    txt_lines.append(f"   💵 Затраты: {_fmt_money(s1['spend'])}")
    txt_lines.append(f"   🎯 CPM: {s1['cpm']:.2f} $")
    txt_lines.append(f"   💸 CPC: {s1['cpc']:.2f} $")
    if flags["messaging"]:
        txt_lines.append(f"   💬 Переписки: {s1['msgs']}")
    if flags["leads"]:
        txt_lines.append(f"   📩 Лиды: {s1['leads']}")
    if flags["messaging"] or flags["leads"]:
        txt_lines.append(f"   🧮 Заявки всего: {s1['total']}")
        txt_lines.append(f"   🎯 CPA: {_fmt_cpa(s1['cpa'])}")
    txt_lines.append("")

    # 2️⃣ Новый
    txt_lines.append(f"2️⃣ <b>{label2}</b> (новый период)")
    txt_lines.append(f"   👁 Охваты: {fmt_int(s2['impr'])}")
    txt_lines.append(f"   🖱 Клики: {fmt_int(s2['clicks'])}")
    txt_lines.append(f"   💵 Затраты: {_fmt_money(s2['spend'])}")
    txt_lines.append(f"   🎯 CPM: {s2['cpm']:.2f} $")
    txt_lines.append(f"   💸 CPC: {s2['cpc']:.2f} $")
    if flags["messaging"]:
        txt_lines.append(f"   💬 Переписки: {s2['msgs']}")
    if flags["leads"]:
        txt_lines.append(f"   📩 Лиды: {s2['leads']}")
    if flags["messaging"] or flags["leads"]:
        txt_lines.append(f"   🧮 Заявки всего: {s2['total']}")
        txt_lines.append(f"   🎯 CPA: {_fmt_cpa(s2['cpa'])}")
    txt_lines.append("")

    # 3️⃣ Сравнение
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

    return "\n".join(txt_lines)


# ======== парсинг дат для кастомных диапазонов =========
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
