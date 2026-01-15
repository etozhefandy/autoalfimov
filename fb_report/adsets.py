# fb_report/adsets.py
from datetime import datetime, timedelta

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
    until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
    since = until - timedelta(days=6)

    # Legacy FB-read path removed (collector-only policy).
    return [], since, until


def list_adsets_for_account(aid: str) -> list[dict]:
    """Возвращает плоский список адсетов аккаунта (id, name).

    Используется в UI настроек CPA-алёртов по адсетам.
    Берём последние 7 дней и собираем уникальные пары (id, name).
    """

    return []


def get_adset_name(aid: str, adset_id: str) -> str:
    """Возвращает имя адсета по ID для данного аккаунта.

    Используется в экране настроек CPA-алёртов для конкретного адсета.
    """

    return adset_id or "(adset)"


async def send_adset_report(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str, aid: str):
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
