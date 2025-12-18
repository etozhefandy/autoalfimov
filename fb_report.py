# fb_report.py — точка входа, только запуск бота

import logging
import time

from telegram import Update
from telegram.error import NetworkError, TimedOut, RetryAfter

from fb_report.app import build_app

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    while True:
        try:
            app = build_app()
            try:
                app.run_polling(
                    allowed_updates=Update.ALL_TYPES,
                    bootstrap_retries=-1,
                    connect_timeout=20,
                    read_timeout=30,
                    write_timeout=30,
                    pool_timeout=30,
                )
            except TypeError:
                app.run_polling(
                    allowed_updates=Update.ALL_TYPES,
                    connect_timeout=20,
                    read_timeout=30,
                    write_timeout=30,
                    pool_timeout=30,
                )
        except (NetworkError, TimedOut, RetryAfter) as e:
            log.warning(
                "Telegram polling transient error: %s: %s. Restarting polling in 5s...",
                type(e).__name__,
                e,
            )
            time.sleep(5)
            continue
        except Exception as e:
            log.exception(
                "Telegram polling crashed with unexpected error (%s). Restarting in 10s...",
                type(e).__name__,
                exc_info=e,
            )
            time.sleep(10)
            continue
