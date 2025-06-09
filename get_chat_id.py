from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

TELEGRAM_TOKEN = "8033028841:AAGud3hSZdR8KQiOSaAcwfbkv8P0p-P3Dt4"

async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"Chat ID: {update.effective_chat.id}")
    await update.message.reply_text(f"✅ Chat ID: {update.effective_chat.id}")

app = Application.builder().token(TELEGRAM_TOKEN).build()
app.add_handler(MessageHandler(filters.ALL, handler))

print("Бот ждёт сообщение...")
app.run_polling()
