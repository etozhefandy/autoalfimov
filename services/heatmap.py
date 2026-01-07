import math
from datetime import datetime, timedelta
from facebook_business.adobjects.adaccount import AdAccount

from services.analytics import count_leads_from_actions


def _extract_actions(insight):
    acts = insight.get("actions", []) or []
    out = {}
    for a in acts:
        t = a.get("action_type")
        v = float(a.get("value", 0) or 0)
        out[t] = v
    return out


def _calculate_cpa(spend, msgs, leads):
    total = msgs + leads
    if total <= 0:
        return None
    return spend / total


def _make_period(mode: str):
    """
    mode:
        "7"  → последние 7 дней
        "14" → последние 14 дней
        "month" → текущий месяц по календарю
    """

    now = datetime.now()
    yesterday = now - timedelta(days=1)

    if mode == "7":
        since = yesterday - timedelta(days=6)
        return since, yesterday

    if mode == "14":
        since = yesterday - timedelta(days=13)
        return since, yesterday

    if mode == "month":
        since = datetime(now.year, now.month, 1)
        return since, yesterday

    # fallback = 7 дней
    since = yesterday - timedelta(days=6)
    return since, yesterday


def build_heatmap_for_account(aid, get_account_name, mode: str = "7"):
    """
    mode = "7" / "14" / "month"
    """

    since, until = _make_period(mode)

    acc = AdAccount(aid)
    params = {
        "level": "adset",
        "time_range": {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d")
        },
        # Добавляем показы и частоту, чтобы Фокус-ИИ видел, где аудитория выгорает.
        "fields": "spend,actions,name,impressions,frequency"
    }

    try:
        rows = acc.get_insights(params=params)
    except Exception as e:
        return f"⚠ Ошибка получения данных: {e}"

    HOT, MEDIUM, COLD = [], [], []

    for row in rows:
        name = row.get("name", "Без названия")
        spend = float(row.get("spend", 0) or 0)
        acts = _extract_actions(row)

        msgs = int(acts.get("onsite_conversion.messaging_conversation_started_7d", 0))
        leads = count_leads_from_actions(acts, aid=aid)

        cpa = _calculate_cpa(spend, msgs, leads)
        freq = float(row.get("frequency", 0) or 0)

        if cpa is None:
            COLD.append(f"❄️ {name} — 0 заявок, {spend:.2f}$ трат, частота {freq:.1f}")
            continue

        if cpa < 3:
            HOT.append(f"🔥 {name} — CPA {cpa:.2f}$, частота {freq:.1f}")
        elif 3 <= cpa <= 7:
            MEDIUM.append(f"🟡 {name} — CPA {cpa:.2f}$, частота {freq:.1f}")
        else:
            COLD.append(f"❄️ {name} — CPA {cpa:.2f}$, частота {freq:.1f}")

    # Название режима для текста
    mode_text = {
        "7": "Последние 7 дней",
        "14": "Последние 14 дней",
        "month": "Текущий месяц"
    }.get(mode, "Последние 7 дней")

    text = (
        f"📊 <b>Тепловая карта — {get_account_name(aid)}</b>\n"
        f"{mode_text}\n"
        f"{since.strftime('%d.%m')}—{until.strftime('%d.%m')}\n\n"
    )

    if HOT:
        text += "<b>🔥 HOT — лучшие:</b>\n" + "\n".join(HOT) + "\n\n"
    else:
        text += "<b>🔥 HOT — нет</b>\n\n"

    if MEDIUM:
        text += "<b>🟡 MEDIUM — средние:</b>\n" + "\n".join(MEDIUM) + "\n\n"
    else:
        text += "<b>🟡 MEDIUM — нет</b>\n\n"

    if COLD:
        text += "<b>❄️ COLD — слабые:</b>\n" + "\n".join(COLD)
    else:
        text += "<b>❄️ COLD — нет</b>"

    return text
