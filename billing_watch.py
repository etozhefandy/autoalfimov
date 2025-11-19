# billing_watch.py
from datetime import datetime, timedelta
from math import floor
from typing import Any, Dict, Callable

from pytz import timezone
from facebook_business.adobjects.adaccount import AdAccount
from telegram.ext import Application, ContextTypes

ALMATY_TZ = timezone("Asia/Almaty")


async def _billing_followup_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Повторное уточнение суммы долга через ~20 минут,
    чтобы можно было текстом отправить заказчику.
    """
    data: Dict[str, Any] = context.job.data or {}
    aid: str = data["aid"]
    get_account_name: Callable[[str], str] = data["get_account_name"]
    usd_to_kzt: Callable[[], float] = data["usd_to_kzt"]
    kzt_round_up_1000: Callable[[float], int] = data["kzt_round_up_1000"]
    group_chat_id: str = data["group_chat_id"]

    rate = usd_to_kzt()

    try:
        info = AdAccount(aid).api_get(fields=["name", "balance"])
    except Exception:
        return

    name = info.get("name", get_account_name(aid))
    balance_usd = float(info.get("balance", 0) or 0) / 100.0
    balance_kzt = kzt_round_up_1000(balance_usd * rate)
    balance_kzt_str = f"{balance_kzt:,}".replace(",", " ")

    text = (
        f"🔁 Обновлённый биллинг по аккаунту <b>{name}</b>:\n"
        f"Текущая сумма к оплате: {balance_usd:.2f} $ / {balance_kzt_str} ₸\n\n"
        f"Этот текст можно отправить заказчику для пополнения."
    )

    await context.bot.send_message(
        chat_id=group_chat_id,
        text=text,
        parse_mode="HTML",
    )


async def _billing_check_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Периодически проверяет статусы кабинетов и ловит момент,
    когда активный кабинет стал неактивным и появился долг (balance > 0).

    1) Сразу пишет в группу «Подожди, не отправляй заказчику…»
    2) Ставит follow-up через 20 минут с уточнённой суммой.
    """
    job = context.job
    data: Dict[str, Any] = job.data or {}

    get_enabled_accounts: Callable[[], list[str]] = data["get_enabled_accounts"]
    get_account_name: Callable[[str], str] = data["get_account_name"]
    usd_to_kzt: Callable[[], float] = data["usd_to_kzt"]
    kzt_round_up_1000: Callable[[float], int] = data["kzt_round_up_1000"]
    group_chat_id: str = data["group_chat_id"]

    # В state держим предыдущий статус кабинета
    state: Dict[str, Any] = data.setdefault("state", {})
    rate = usd_to_kzt()

    for aid in get_enabled_accounts():
        try:
            info = AdAccount(aid).api_get(
                fields=["name", "account_status", "balance"]
            )
        except Exception:
            continue

        name = info.get("name", get_account_name(aid))
        status = info.get("account_status")
        balance_usd = float(info.get("balance", 0) or 0) / 100.0
        balance_kzt = kzt_round_up_1000(balance_usd * rate)
        balance_kzt_str = f"{balance_kzt:,}".replace(",", " ")

        prev = state.get(aid, {})
        prev_status = prev.get("status")

        # Логика «момента биллинга»:
        # было активно (1) -> стало неактивно (!=1) И есть долг (balance_usd > 0)
        if prev_status == 1 and status != 1 and balance_usd > 0:
            text = (
                f"🚨 У аккаунта <b>{name}</b> биллинг!\n"
                f"Неудавшееся списание: {balance_usd:.2f} $ / {balance_kzt_str} ₸\n\n"
                f"Подожди, не отправляй заказчику — сумма ещё может скорректироваться."
            )
            await context.bot.send_message(
                chat_id=group_chat_id,
                text=text,
                parse_mode="HTML",
            )

            # через 20 минут уточняем сумму долга
            when = datetime.now(ALMATY_TZ) + timedelta(minutes=20)
            context.job_queue.run_once(
                _billing_followup_job,
                when=when,
                data={
                    "aid": aid,
                    "get_account_name": get_account_name,
                    "usd_to_kzt": usd_to_kzt,
                    "kzt_round_up_1000": kzt_round_up_1000,
                    "group_chat_id": group_chat_id,
                },
            )

        # обновляем state
        state[aid] = {
            "status": status,
            "balance_usd": balance_usd,
        }

    job.data = data  # сохраняем обновлённый state обратно в джобу


def init_billing_watch(
    app: Application,
    get_enabled_accounts,
    get_account_name,
    usd_to_kzt,
    kzt_round_up_1000,
    owner_id: int,
    group_chat_id: str,
) -> None:
    """
    Инициализация повторяющейся джобы проверки биллингов.
    Вызывается из fb_report.py в build_app().
    owner_id пока не используем (на будущее — если захочешь дубли в личку).
    """
    job_data = {
        "get_enabled_accounts": get_enabled_accounts,
        "get_account_name": get_account_name,
        "usd_to_kzt": usd_to_kzt,
        "kzt_round_up_1000": kzt_round_up_1000,
        "group_chat_id": group_chat_id,
        "owner_id": owner_id,
        "state": {},  # тут храним предыдущие статусы аккаунтов
    }

    # каждые 10 минут, с небольшим задержкой старта
    app.job_queue.run_repeating(
        _billing_check_job,
        interval=600,   # 10 минут
        first=60,       # первая проверка через минуту после старта
        data=job_data,
    )
