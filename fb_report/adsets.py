# fb_report/adsets.py
from datetime import datetime, timedelta

from facebook_business.adobjects.adaccount import AdAccount
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

from .constants import ALMATY_TZ
from .insights import _blend_totals
from .storage import get_account_name, metrics_flags
from .reporting import fmt_int


def fetch_adset_insights_7d(aid: str):
    """
    Возвращает:
    - campaigns: список кампаний с метриками и вложенными адсетами
    - since, until: даты периода
    """
    acc = AdAccount(aid)

    until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
    since = until - timedelta(days=6)

    params = {
        "level": "adset",
        "time_range": {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        },
    }
    fields = [
        "campaign_id",
        "campaign_name",
        "adset_id",
        "adset_name",
        "impressions",
        "clicks",
        "spend",
        "actions",
    ]

    try:
        data = acc.get_insights(fields=fields, params=params)
    except Exception as e:
        print(f"[adset_report] error for {aid}: {e}")
        return [], since, until

    campaigns: dict[str, dict] = {}

    for row in data:
        cid = row.get("campaign_id") or "unknown"
        cname = row.get("campaign_name") or "(без названия)"
        adset_id = row.get("adset_id")
        adset_name = row.get("adset_name") or adset_id or "(adset)"

        spend, msgs, leads, total, blended = _blend_totals(row)
        impr = int(row.get("impressions", 0) or 0)
        clicks = int(row.get("clicks", 0) or 0)

        camp = campaigns.setdefault(
            cid,
            {
                "id": cid,
                "name": cname,
                "spend": 0.0,
                "impr": 0,
                "clicks": 0,
                "msgs": 0,
                "leads": 0,
                "total": 0,
                "cpa": None,
                "adsets": [],
            },
        )

        camp["spend"] += spend
        camp["impr"] += impr
        camp["clicks"] += clicks
        camp["msgs"] += msgs
        camp["leads"] += leads
        camp["total"] += total

        camp["adsets"].append(
            {
                "id": adset_id,
                "name": adset_name,
                "spend": spend,
                "impr": impr,
                "clicks": clicks,
                "msgs": msgs,
                "leads": leads,
                "total": total,
                "cpa": blended,
            }
        )

    for camp in campaigns.values():
        if camp["total"] > 0:
            camp["cpa"] = camp["spend"] / camp["total"]
        else:
            camp["cpa"] = None

    return list(campaigns.values()), since, until


async def send_adset_report(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str, aid: str):
    campaigns, since, until = fetch_adset_insights_7d(aid)
    if not campaigns:
        await ctx.bot.send_message(
            chat_id,
            f"По {get_account_name(aid)} нет данных по адсетам за последние 7 дней.",
            parse_mode="HTML",
        )
        return

    period_label = f"{since.strftime('%d.%m.%Y')}–{until.strftime('%d.%m.%Y')}"
    flags = metrics_flags(aid)

    # 1) Общий вброс по кампаниям
    for camp in campaigns:
        lines = [
            f"🎯 Кампания: <b>{camp['name']}</b>",
            f"Период: {period_label} (последние 7 дней)",
            "",
            f"👁 Показы: {fmt_int(camp['impr'])}",
            f"🖱 Клики: {fmt_int(camp['clicks'])}",
            f"💵 Затраты: {camp['spend']:.2f} $",
        ]
        if flags["messaging"]:
            lines.append(f"💬 Переписки: {camp['msgs']}")
        if flags["leads"]:
            lines.append(f"📩 Лиды: {camp['leads']}")
        if flags["messaging"] or flags["leads"]:
            lines.append(f"🧮 Заявки всего: {camp['total']}")
            if camp["cpa"] is not None:
                lines.append(f"🎯 CPA: {camp['cpa']:.2f} $")
            else:
                lines.append("🎯 CPA: —")

        txt = "\n".join(lines)
        await ctx.bot.send_message(chat_id, txt, parse_mode="HTML")

        from autopilat.ui import recommendation_buttons  # если нужно — можно прикрутить

        # 2) Детализация по адсетам этой кампании
        from autopilat.actions import can_disable  # используется в UI автопилота

        for ad in camp["adsets"]:
            if not ad["id"]:
                continue

            lines = [
                f"📦 Кампания: <b>{camp['name']}</b>",
                f"🎯 Адсет: <b>{ad['name']}</b>",
                f"ID: <code>{ad['id']}</code>",
                f"Период: {period_label}",
                "",
                f"👁 Показы: {fmt_int(ad['impr'])}",
                f"🖱 Клики: {fmt_int(ad['clicks'])}",
                f"💵 Затраты: {ad['spend']:.2f} $",
            ]
            if flags["messaging"]:
                lines.append(f"💬 Переписки: {ad['msgs']}")
            if flags["leads"]:
                lines.append(f"📩 Лиды: {ad['leads']}")
            if flags["messaging"] or flags["leads"]:
                lines.append(f"🧮 Заявки всего: {ad['total']}")
                if ad["cpa"] is not None:
                    lines.append(f"🎯 CPA: {ad['cpa']:.2f} $")
                else:
                    lines.append("🎯 CPA: —")

            txt = "\n".join(lines)

            kb = InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "✍️ Редактировать бюджет",
                            callback_data=f"ap|manual|{ad['id']}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "🔴 Выключить",
                            callback_data=f"ap|off|{ad['id']}",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ Назад", callback_data="ap|back"
                        )
                    ],
                ]
            )

            await ctx.bot.send_message(
                chat_id, txt, parse_mode="HTML", reply_markup=kb
            )
