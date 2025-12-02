# fb_report/billing_watch.py

from typing import Callable, Iterable, Optional

from telegram.ext import Application


def init_billing_watch(
    app: Application,
    get_enabled_accounts: Callable[[], Iterable[str]],
    get_account_name: Callable[[str], str],
    usd_to_kzt=None,
    kzt_round_up_1000=None,
    owner_id: Optional[int] = None,
    group_chat_id: Optional[str] = None,
) -> None:
    """
    Упрощённый вариант модуля биллингового «watcher’а».

    Раньше здесь была отдельная сложная логика с периодическим опросом биллингов,
    курсами валют и т.п., которая сейчас:
    - нам не нужна (основная логика уведомлений вынесена в billing.py);
    - ломалась из-за того, что usd_to_kzt мог быть None.

    Сейчас init_billing_watch ничего не планирует в job_queue и служит только
    как «заглушка», чтобы не было ошибок при импорте и вызове.
    Вся актуальная логика уведомлений по биллингу живёт в:
      - fb_report/billing.py (send_billing, send_billing_forecast, billing_digest_job)
      - fb_report/jobs.py (расписание billing_digest_job)
    """
    return
