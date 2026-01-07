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

    # Важно: Application создаём один раз, чтобы не плодить scheduler/jobs и хэндлеры.
    # Если build_app() упадёт — это должно быть фатально (while-restart не поможет).
    app = build_app()

    while True:
        try:
            try:
                app.run_polling(
                    allowed_updates=Update.ALL_TYPES,
                    bootstrap_retries=-1,
                    close_loop=False,
                )
            except TypeError:
                # Для старых версий PTB, где нет bootstrap_retries/close_loop
                try:
                    app.run_polling(
                        allowed_updates=Update.ALL_TYPES,
                        close_loop=False,
                    )
                except TypeError:
                    app.run_polling(
                        allowed_updates=Update.ALL_TYPES,
                    )

            # Если run_polling вернулся без исключений — выходим.
            break

        except RetryAfter as e:
            sleep_s = max(int(getattr(e, "retry_after", 0) or 0), 5)
            log.warning(
                "Telegram polling rate-limited (RetryAfter=%ss). Restarting polling in %ss...",
                getattr(e, "retry_after", None),
                sleep_s,
            )
            time.sleep(sleep_s)
            continue
        except (NetworkError, TimedOut) as e:
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
            )
            time.sleep(10)
            continue
