# fb_report/billing.py
import math
from datetime import datetime, timedelta

from facebook_business.adobjects.adaccount import AdAccount
from telegram.ext import ContextTypes

from .constants import ALMATY_TZ, usd_to_kzt, kzt_round_up_1000
from .storage import iter_enabled_accounts_only, get_account_name
from .reporting import fmt_int


async def send_billing(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """Текущие биллинги: только неактивные аккаунты И только включённые (enabled=True)."""
    rate = usd_to_kzt()
    for aid in iter_enabled_accounts_only():
        try:
            info = AdAccount(aid).api_get(fields=["name", "account_status", "balance"])
        except Exception:
            continue
        if info.get("account_status") == 1:
            continue  # показываем только НЕактивные
        name = info.get("name", get_account_name(aid))
        usd = float(info.get("balance", 0) or 0) / 100.0
        kzt = kzt_round_up_1000(usd * rate)
        txt = f"🔴 <b>{name}</b>\n   💵 {usd:.2f} $  |  🇰🇿 {fmt_int(kzt)} ₸"
        await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")


def _compute_billing_forecast_for_account(
    aid: str, rate_kzt: float, lookback_days: int = 7
):
    try:
        info = AdAccount(aid).api_get(fields=["name", "account_status", "balance"])
    except Exception:
        return None

    status = info.get("account_status")
    if status != 1:
        return None

    balance_usd = float(info.get("balance", 0) or 0) / 100.0
    if balance_usd <= 0:
        return None

    acc = AdAccount(aid)
    until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
    since = until - timedelta(days=lookback_days - 1)
    params = {
        "level": "account",
        "time_range": {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        },
    }
    try:
        data = acc.get_insights(fields=["spend"], params=params)
    except Exception:
        return None

    total_spend = 0.0
    for row in data:
        try:
            total_spend += float(row.get("spend", 0) or 0)
        except Exception:
            continue

    if total_spend <= 0:
        return None

    avg_daily = total_spend / float(lookback_days)
    if avg_daily <= 0:
        return None

    days_left = balance_usd / avg_daily
    if days_left <= 0:
        return None

    name = info.get("name", get_account_name(aid))
    balance_kzt = kzt_round_up_1000(balance_usd * rate_kzt)

    return {
        "aid": aid,
        "name": name,
        "status": status,
        "balance_usd": balance_usd,
        "balance_kzt": balance_kzt,
        "avg_daily_spend": avg_daily,
        "days_left": days_left,
    }


async def send_billing_forecast(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """
    Прогноз списаний по всем активным аккаунтам (только enabled=True).
    Показываем примерную дату на день РАНЬШЕ расчёта.
    """
    rate = usd_to_kzt()
    items = []
    for aid in iter_enabled_accounts_only():
        fc = _compute_billing_forecast_for_account(aid, rate_kzt=rate)
        if fc:
            items.append(fc)

    if not items:
        await ctx.bot.send_message(
            chat_id=chat_id,
            text="🔮 Прогноз списаний: нет данных (нет трат/баланса по активным аккаунтам).",
        )
        return

    items.sort(key=lambda x: x["days_left"])

    lines = ["🔮 <b>Прогноз списаний по кабинетам</b>"]
    today = datetime.now(ALMATY_TZ).date()

    for fc in items:
        days_left = fc["days_left"]
        if days_left < 1:
            approx_days = 0
        else:
            approx_days = max(int(math.floor(days_left)) - 1, 0)
        date = today + timedelta(days=approx_days)
        if approx_days <= 0:
            when_str = "сегодня (ориентир)"
        else:
            when_str = f"через {approx_days} дн. (ориентир {date.strftime('%d.%m')})"

        lines.append(
            f"\n💳 <b>{fc['name']}</b>\n"
            f"   Баланс: {fc['balance_usd']:.2f} $  |  🇰🇿 {fmt_int(fc['balance_kzt'])} ₸\n"
            f"   Средний расход: {fc['avg_daily_spend']:.2f} $/день\n"
            f"   ⏳ Примерное списание: {when_str}"
        )

    await ctx.bot.send_message(chat_id=chat_id, text="\n".join(lines), parse_mode="HTML")


async def billing_digest_job(ctx: ContextTypes.DEFAULT_TYPE):
<<<<<<< HEAD
    """
    Ежедневный дайджест утром:
    список неактивных аккаунтов (как в send_billing), чтобы напомнить о пополнении.
=======
    """Ежедневный дайджест утром: список НЕактивных аккаунтов.

    Поведение такое же, как при нажатии кнопки "Текущие биллинги":
    сначала заголовок "📋 Биллинги (неактивные аккаунты):",
    затем вывод всех неактивных кабинетов через send_billing.
>>>>>>> fff35b0 (update)
    """
    from .constants import DEFAULT_REPORT_CHAT

    chat_id = str(DEFAULT_REPORT_CHAT)
    if not chat_id:
        return

    # Заголовок утреннего сообщения
    await ctx.bot.send_message(
        chat_id=chat_id,
        text="📋 Биллинги (неактивные аккаунты):",
    )

    # Далее используем существующую логику send_billing, которая выводит
    # сами неактивные аккаунты.
    await send_billing(ctx, chat_id)
