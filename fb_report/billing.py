import math
from datetime import datetime, timedelta
import logging

from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError
from telegram.ext import ContextTypes

from .constants import ALMATY_TZ, usd_to_kzt, kzt_round_up_1000
from .storage import iter_enabled_accounts_only, get_account_name
from .reporting import fmt_int

from services.facebook_api import allow_fb_api_calls


def _is_no_access_error(http_status: int | None, message: str | None) -> bool:
    if int(http_status or 0) != 403:
        return False
    msg_l = str(message or "").lower()
    if "has not granted" not in msg_l:
        return False
    if ("ads_read" not in msg_l) and ("ads_management" not in msg_l):
        return False
    return True


async def send_billing(ctx: ContextTypes.DEFAULT_TYPE, chat_id: str, only_inactive: bool = False):
    """Текущие биллинги: работает только по аккаунтам enabled=True из настроек."""
    enabled_ids = list(iter_enabled_accounts_only())
    if not enabled_ids:
        await ctx.bot.send_message(
            chat_id=chat_id,
            text="📋 Биллинги: нет включённых аккаунтов (enabled).",
        )
        return

    try:
        rate = float(usd_to_kzt() or 0.0)
    except Exception:
        rate = 0.0

    detected = 0
    no_access: list[str] = []
    failed: list[str] = []

    with allow_fb_api_calls(reason="billing_current"):
        for aid in enabled_ids:
            try:
                info = AdAccount(str(aid)).api_get(fields=["name", "account_status", "balance"])
                if hasattr(info, "export_all_data"):
                    info = info.export_all_data()
            except FacebookRequestError as e:
                http_status = None
                fb_code = None
                try:
                    http_status = int(getattr(e, "http_status", None)() if callable(getattr(e, "http_status", None)) else getattr(e, "http_status", None))
                except Exception:
                    http_status = None
                try:
                    fb_code = int(e.api_error_code())
                except Exception:
                    fb_code = None
                msg = None
                try:
                    msg = str(e)
                except Exception:
                    msg = None

                logging.getLogger(__name__).warning(
                    "billing_api_error caller=billing_current aid=%s http_status=%s fb_code=%s message=%s",
                    str(aid),
                    str(http_status),
                    str(fb_code),
                    str(msg or ""),
                )

                if _is_no_access_error(http_status, msg):
                    no_access.append(str(aid))
                else:
                    failed.append(str(aid))
                continue
            except Exception as e:
                logging.getLogger(__name__).warning(
                    "billing_api_error caller=billing_current aid=%s http_status=%s fb_code=%s message=%s",
                    str(aid),
                    "",
                    "",
                    str(e),
                )
                failed.append(str(aid))
                continue

            if not isinstance(info, dict):
                failed.append(str(aid))
                continue

            name = str(info.get("name") or get_account_name(aid))
            try:
                status = int(info.get("account_status"))
            except Exception:
                status = None
            try:
                usd = float(info.get("balance", 0) or 0) / 100.0
            except Exception:
                usd = 0.0

            billing_detected = (status != 1) or (usd < 0)
            if bool(only_inactive) and status == 1:
                continue
            if not billing_detected:
                continue

            detected += 1
            try:
                kzt = kzt_round_up_1000(float(usd) * float(rate)) if rate > 0 else 0
            except Exception:
                kzt = 0

            txt = f"🔴 <b>{name}</b>\n💵 {usd:.2f} $ | 🇰🇿 {fmt_int(int(kzt))} ₸"
            await ctx.bot.send_message(chat_id=chat_id, text=txt, parse_mode="HTML")

    lines = []
    if detected <= 0:
        lines.append("📋 Биллинги: биллингов не обнаружено.")
    if no_access:
        lines.append("⚠️ Нет доступа (ads_read): " + ", ".join(no_access))
    if failed:
        lines.append("⚠️ Ошибки API: " + ", ".join(failed))
    lines.append(
        f"debug: enabled={len(enabled_ids)} detected={int(detected)} no_access={len(no_access)} failed={len(failed)}"
    )
    await ctx.bot.send_message(chat_id=chat_id, text="\n".join(lines))


def _compute_billing_forecast_for_account(aid: str, rate_kzt: float, lookback_days: int = 7):
    try:
        with allow_fb_api_calls(reason="billing_forecast"):
            info = AdAccount(str(aid)).api_get(fields=["name", "account_status", "balance"])
            if hasattr(info, "export_all_data"):
                info = info.export_all_data()
    except Exception:
        return None
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
    try:
        with allow_fb_api_calls(reason="billing_forecast"):
            data = acc.get_insights(fields=["spend"], params=params)
    except Exception:
        data = None
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
    await send_billing(ctx, chat_id, only_inactive=True)
