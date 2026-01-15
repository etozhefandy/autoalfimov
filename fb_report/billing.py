# fb_report/billing.py
import math
from datetime import datetime, timedelta

from telegram.ext import ContextTypes

from .constants import ALMATY_TZ, usd_to_kzt, kzt_round_up_1000
from .storage import iter_enabled_accounts_only, get_account_name
from .reporting import fmt_int


async def send_billing(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """Текущие биллинги: только неактивные аккаунты И только включённые (enabled=True)."""
    await ctx.bot.send_message(
        chat_id=chat_id,
        text="🟦 Биллинги временно отключены (snapshots-only режим).",
    )


def _compute_billing_forecast_for_account(
    aid: str, rate_kzt: float, lookback_days: int = 7
):
    return None


async def send_billing_forecast(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str):
    """
    Прогноз списаний по всем активным аккаунтам (только enabled=True).
    Показываем примерную дату на день РАНЬШЕ расчёта.
    """
    await ctx.bot.send_message(
        chat_id=chat_id,
        text="🟦 Прогноз списаний временно отключён (snapshots-only режим).",
    )


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
