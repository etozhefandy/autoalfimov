# fb_report.py — точка входа, только запуск бота

from telegram import Update
from fb_report.app import build_app

if __name__ == "__main__":
    print("🚀 Бот запущен и ожидает команд.")
    build_app().run_polling(allowed_updates=Update.ALL_TYPES)
