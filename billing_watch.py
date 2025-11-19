# billing_watch.py
"""
Модуль для отслеживания биллингов Facebook Ads.

Что делает:
- Раз в N минут обходит включённые аккаунты.
- Смотрит смену статуса account_status: было 1 (ACTIVE) → стало != 1.
  Это и есть "момент биллинга / блокировки".
- В момент события отправляет алерт в группу с текстом про биллинг
  и суммой долга в $ и ₸.
- Параллельно ставит follow-up через ~20 минут, чтобы получить уже
  откорректированный баланс и выдать текст, который можно переслать клиенту.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Iterable, Dict, Any

from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount

# Локальное время
ALMATY_TZ = timezone("Asia/Almaty")


def fmt_int(n) -> str:
    """Красивый формат целых чисел: 12345 -> '12 345'."""
    try:
        return f"{int(float(n)):,}".replace(",", " ")
    except Exception:
        return "0"


class BillingWatcher:
    """
    Класс-обёртка над логикой биллингов, чтобы удобно повесить
    его .job на JobQueue как callback.
    """

    def __init__(
        self,
        get_enabled_accounts: Callable[[], Iterable[str]],
        get_account_name: Callable[[str], str],
        usd_to_kzt: Callable[[], float],
        kzt_round_up_1000: Callable[[float], int],
        group_chat_id: str,
    ) -> None:
        self.get_enabled_accounts = get_enabled_accounts
        self.get_account_name = get_account_name
        self.usd_to_kzt = usd_to_kzt
        self.kzt_round_up_1000 = kzt_round_up_1000
        self.group_chat_id = str(group_chat_id)

        # сюда запоминаем предыдущие значения account_status
        # { "act_123": 1, "act_456": 2, ... }
        self._last_status: Dict[str, int] = {}

    # ---------- ВСПОМОГАТЕЛЬНОЕ ----------

    def _get_account_info(self, aid: str) -> dict | None:
        """Аккуратно достаём name, account_status, balance."""
        try:
            info = AdAccount(aid).api_get(
                fields=["name", "account_status", "balance"]
            )
            return info
        except Exception:
            return None

    # ---------- ОСНОВНОЙ JOB ДЛЯ JOBQUEUE ----------

    async def job(self, ctx) -> None:
        """
        Основной callback, который вешается на JobQueue через run_repeating.

        ctx: telegram.ext.CallbackContext (в async-версии).
        """
        bot = ctx.bot
        rate = self.usd_to_kzt()

        for aid in self.get_enabled_accounts():
            info = self._get_account_info(aid)
            if not info:
                continue

            name = info.get("name") or self.get_account_name(aid)
            status = int(info.get("account_status", 0) or 0)
            balance_cents = float(info.get("balance", 0) or 0)
            balance_usd = balance_cents / 100.0

            prev_status = self._last_status.get(aid)

            # первый запуск по этому аккаунту — просто запоминаем
            self._last_status[aid] = status

            # нас интересует только момент: БЫЛ активен (1), СТАЛ неактивен (!=1)
            if prev_status == 1 and status != 1:
                # момент биллинга
                kzt_val = self.kzt_round_up_1000(balance_usd * rate)

                # Первое сообщение — техническое, не для клиента.
                text = (
                    f"🚨 У аккаунта <b>{name}</b> биллинг!\n"
                    f"Сумма неудавшегося биллинга: {balance_usd:.2f} $ / "
                    f"{fmt_int(kzt_val)} ₸\n\n"
                    f"⚠️ Подожди, не отправляй это сообщение заказчику — "
                    f"через ~20 минут баланс обновится, и я пришлю текст, "
                    f"который можно переслать клиенту."
                )

                try:
                    await bot.send_message(
                        chat_id=self.group_chat_id,
                        text=text,
                        parse_mode="HTML",
                    )
                except Exception:
                    # если не смогли отправить — просто молча продолжаем
                    pass

                # Ставим follow-up через 20 минут
                try:
                    if ctx.application and ctx.application.job_queue:
                        ctx.application.job_queue.run_once(
                            self._followup_job,
                            when=20 * 60,  # 20 минут
                            data={"aid": aid, "name": name},
                        )
                except Exception:
                    pass

    # ---------- FOLLOW-UP ЧЕРЕЗ 20 МИН ----------

    async def _followup_job(self, ctx) -> None:
        """
        Через ~20 минут после биллинга ещё раз смотрим баланс и даём
        аккуратный текст, который уже можно пересылать клиенту.
        """
        bot = ctx.bot
        data: Dict[str, Any] = ctx.job.data or {}
        aid = data.get("aid")
        name = data.get("name") or (aid and self.get_account_name(aid)) or "Аккаунт"

        if not aid:
            return

        info = self._get_account_info(aid)
        if not info:
            return

        # Баланс через 20 минут — должен быть более корректным после частичных списаний
        balance_cents = float(info.get("balance", 0) or 0)
        balance_usd = balance_cents / 100.0

        rate = self.usd_to_kzt()
        kzt_val = self.kzt_round_up_1000(balance_usd * rate)

        # Текст, который можно копировать заказчику
        today = datetime.now(ALMATY_TZ).strftime("%d.%m.%Y")

        client_text = (
            f"Добрый день!\n\n"
            f"По аккаунту <b>{name}</b> на {today} есть задолженность "
            f"перед Facebook: <b>{balance_usd:.2f} $ / {fmt_int(kzt_val)} ₸</b>.\n"
            f"Нужно пополнить рекламный кабинет, чтобы объявления продолжили крутиться."
        )

        final_text = (
            f"✅ Обновлённый долг по аккаунту <b>{name}</b>:\n"
            f"{balance_usd:.2f} $ / {fmt_int(kzt_val)} ₸\n\n"
            f"📝 Текст для отправки заказчику:\n\n"
            f"{client_text}"
        )

        try:
            await bot.send_message(
                chat_id=self.group_chat_id,
                text=final_text,
                parse_mode="HTML",
            )
        except Exception:
            pass
