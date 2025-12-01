# billing_watch.py
from telegram.ext import Application, ContextTypes


async def job_wrapper(context: ContextTypes.DEFAULT_TYPE):
    # Заглушка: фонового мониторинга биллингов здесь сейчас нет.
    # При желании можно будет реализовать отдельный аккуратный джоб.
    return


def init_billing_watch(
    app: Application,
    get_enabled_accounts=None,
    get_account_name=None,
    usd_to_kzt=None,
    kzt_round_up_1000=None,
    owner_id=None,
    group_chat_id=None,
):
    # Ничего не планируем в job_queue, просто чтобы импорт не падал.
    return
