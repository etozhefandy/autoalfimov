import math
from datetime import datetime, timedelta

from facebook_business.adobjects.adaccount import AdAccount
from telegram.ext import ContextTypes

from .constants import ALMATY_TZ, usd_to_kzt, kzt_round_up_1000
from .storage import iter_enabled_accounts_only, get_account_name
from .reporting import fmt_int

from services.facebook_api import allow_fb_api_calls, safe_api_call


async def send_billing(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """Текущие биллинги: только неактивные аккаунты И только включённые (enabled=True)."""
    rate = float(usd_to_kzt() or 0.0)
    with allow_fb_api_calls(reason="billing_current"):
        for aid in iter_enabled_accounts_only():
            info = safe_api_call(
                AdAccount(str(aid)).api_get,
                fields=["name", "account_status", "balance"],
                params={},
                _meta={"endpoint": "adaccount", "params": {"fields": "name,account_status,balance"}},
                _caller="billing_current",
            )
            if not isinstance(info, dict):
                continue
            if info.get("account_status") == 1:
                continue  # показываем только НЕактивные
            name = info.get("name", get_account_name(aid))
            usd = float(info.get("balance", 0) or 0) / 100.0
            kzt = kzt_round_up_1000(float(usd) * float(rate)) if rate > 0 else 0
            txt = f"🔴 <b>{name}</b>\n   💵 {usd:.2f} $  |  🇰🇿 {fmt_int(kzt)} ₸"
            await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")


def _compute_billing_forecast_for_account(aid: str, rate_kzt: float, lookback_days: int = 7):
    with allow_fb_api_calls(reason="billing_forecast"):
        info = safe_api_call(
            AdAccount(str(aid)).api_get,
            fields=["name", "account_status", "balance"],
            params={},
            _meta={"endpoint": "adaccount", "params": {"fields": "name,account_status,balance"}},
            _caller="billing_forecast_account",
        )
    if not isinstance(info, dict):
        return None

    status = info.get("account_status")
    if status != 1:
        return None

    balance_usd = float(info.get("balance", 0) or 0) / 100.0
    if balance_usd <= 0:
        return None

    acc = AdAccount(str(aid))
    until = (datetime.now(ALMATY_TZ) - timedelta(days=1)).date()
    since = until - timedelta(days=int(lookback_days) - 1)
    params = {
        "level": "account",
        "time_range": {
            "since": since.strftime("%Y-%m-%d"),
            "until": until.strftime("%Y-%m-%d"),
        },
    }
    with allow_fb_api_calls(reason="billing_forecast"):
        data = safe_api_call(
            acc.get_insights,
            fields=["spend"],
            params=params,
            _meta={"endpoint": "insights/account", "params": params},
            _caller="billing_forecast_insights",
        )
    if not data:
        return None

    total_spend = 0.0
    for row in (data or []):
        try:
            total_spend += float((row or {}).get("spend", 0) or 0)
        except Exception:
            continue

    if total_spend <= 0:
        return None

    avg_daily = total_spend / float(max(1, int(lookback_days)))
    if avg_daily <= 0:
        return None

    days_left = balance_usd / avg_daily
    if days_left <= 0:
        return None

    name = info.get("name", get_account_name(aid))
    balance_kzt = kzt_round_up_1000(balance_usd * float(rate_kzt))

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
    """Прогноз списаний по всем активным аккаунтам (только enabled=True)."""
    rate = float(usd_to_kzt() or 0.0)
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
    """Ежедневный дайджест утром: список НЕактивных аккаунтов.

    Поведение такое же, как при нажатии кнопки "Текущие биллинги":
    сначала заголовок "📋 Биллинги (неактивные аккаунты):",
    затем вывод всех неактивных кабинетов через send_billing.
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
