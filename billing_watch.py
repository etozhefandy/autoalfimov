"""Простой watcher биллингов.

Логика аналогична старому скрипту:
- каждые 10 минут опрашиваем аккаунты;
- если account_status меняется с 1 (ACTIVE) на любое другое значение,
  отправляем тревожное сообщение в групповой чат;
- через ~20 минут после первого алёрта по аккаунту опрашиваем баланс ещё раз
  и шлём уточнённую сумму.
"""

from typing import Callable, Iterable, Optional, Dict, Any
from datetime import datetime, timedelta

from facebook_business.adobjects.adaccount import AdAccount
from telegram.ext import Application, ContextTypes

from fb_report.constants import ALMATY_TZ


_last_status: Dict[str, Any] = {}
_pending_recheck: Dict[str, Dict[str, Any]] = {}


async def _billing_watch_job(
    context: ContextTypes.DEFAULT_TYPE,
    get_enabled_accounts: Callable[[], Iterable[str]],
    get_account_name: Callable[[str], str],
    usd_to_kzt,
    kzt_round_up_1000,
    group_chat_id: Optional[str],
) -> None:
    if not group_chat_id:
        return

    global _last_status, _pending_recheck

    now = datetime.now(ALMATY_TZ)

    for aid in get_enabled_accounts():
        try:
            acc = AdAccount(aid)
            info = acc.api_get(fields=["name", "account_status", "balance"])
        except Exception:
            continue

        status = info.get("account_status")
        name = info.get("name", get_account_name(aid))
        balance_usd = float(info.get("balance", 0) or 0) / 100.0

        prev_status = _last_status.get(aid)
        _last_status[aid] = status

        # Переход из ACTIVE (1) в любой другой статус → алёрт о первичной сумме биллинга
        if prev_status == 1 and status != 1:
            # Первый алёрт: подчёркиваем, что это предварительная сумма
            lines = [
                "⚠️ ⚠️ ⚠️ Ахтунг! Биллинг в {name}".format(name=name),
                f"Предварительная сумма: {balance_usd:.2f} $",
            ]

            if usd_to_kzt and kzt_round_up_1000:
                try:
                    rate = float(usd_to_kzt())
                    kzt = kzt_round_up_1000(balance_usd * rate)
                    lines.append(f"Примерно: ≈ {kzt} ₸")
                except Exception:
                    pass

            lines.append("Через 20 минут выдам сумму с корректировками.")

            text = "\n".join(lines)
            await context.bot.send_message(chat_id=group_chat_id, text=text)

            # Запланировать уточнение через ~20 минут
            _pending_recheck[aid] = {
                "at": now + timedelta(minutes=20),
                "first_usd": balance_usd,
                "name": name,
            }

    # Вторая фаза: уточнения
    for aid, meta in list(_pending_recheck.items()):
        ts = meta.get("at")
        if not ts or now < ts:
            continue

        name = meta.get("name", get_account_name(aid))
        first_usd = float(meta.get("first_usd", 0.0) or 0.0)

        try:
            acc = AdAccount(aid)
            info = acc.api_get(fields=["balance"])
            cur_usd = float(info.get("balance", 0) or 0) / 100.0
        except Exception:
            cur_usd = first_usd

        parts = [
            f"🚨 {name}! у нас биллинг",
            f"Итоговая сумма: {cur_usd:.2f} $",
        ]

        if usd_to_kzt and kzt_round_up_1000:
            try:
                rate = float(usd_to_kzt())
                kzt = kzt_round_up_1000(cur_usd * rate)
                parts.append(f"≈ {kzt} ₸")
            except Exception:
                pass

        text = " — ".join(parts)
        await context.bot.send_message(chat_id=group_chat_id, text=text)

        del _pending_recheck[aid]


def init_billing_watch(
    app: Application,
    get_enabled_accounts: Callable[[], Iterable[str]],
    get_account_name: Callable[[str], str],
    usd_to_kzt=None,
    kzt_round_up_1000=None,
    owner_id: Optional[int] = None,
    group_chat_id: Optional[str] = None,
) -> None:
    # Мониторинг биллингов каждые 10 минут, как в старом скрипте.
    app.job_queue.run_repeating(
        lambda ctx: _billing_watch_job(
            ctx,
            get_enabled_accounts,
            get_account_name,
            usd_to_kzt,
            kzt_round_up_1000,
            group_chat_id,
        ),
        interval=600,
        first=10,
    )

