from telethon import TelegramClient
import asyncio
import sqlite3
import os 

# Папка сессий
SESSION_DIR = "sessions/"
os.makedirs(SESSION_DIR, exist_ok=True)  # Создаём папку, если её нет

# Настройки API Telegram (получите API_ID и API_HASH на https://my.telegram.org/)
API_ID = 1  # Замените на свой
API_HASH = ""  # Замените на свой
MESSAGE_TEXT = "Привет! Это тестовое сообщение."

import os
if not os.path.exists("sessions"):
    os.makedirs("sessions")


db_path = "users.db"
if os.path.exists(db_path):
    print(f"Файл {db_path} существует.")
else:
    print(f"Файл {db_path} не найден! Нужно его создать.")


conn = sqlite3.connect("users.db")
cursor = conn.cursor()

# Создание таблицы пользователей
cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT UNIQUE NOT NULL
    )
""")

# Добавление тестовых пользователей
users = [("+79955717985",)]
cursor.executemany("INSERT OR IGNORE INTO users (phone) VALUES (?)", users)

conn.commit()
conn.close()

# Файл сессий пользователей
SESSION_FILE = "sessions/"

async def send_message(client, user_id, message):
    """Отправляет сообщение в Telegram."""
    try:
        await client.send_message(user_id, message)
        print(f"Сообщение отправлено {user_id}")
    except Exception as e:
        print(f"Ошибка отправки сообщения {user_id}: {e}")

async def main():
    """Основная функция для работы с пользователями."""
    conn = sqlite3.connect("users.db")
    cursor = conn.cursor()
    cursor.execute("SELECT phone FROM users")
    users = cursor.fetchall()
    conn.close()

    for (phone,) in users:
        session_file = os.path.join(SESSION_DIR, phone)  # Файл сессии

        # Используем существующую сессию
        client = TelegramClient(session_file, API_ID, API_HASH)
        
        async with client:
            if not await client.is_user_authorized():
                print(f"❌ Сессия {phone} не авторизована. Требуется ввод кода вручную!")
                await client.send_code_request(phone)  # Отправляем код
                code = input(f"Введите код для {phone}: ")  # Пользователь вводит код вручную
                await client.sign_in(phone, code)  # Авторизуем

            me = await client.get_me()
            print(f"✅ Авторизован как {me.username} ({phone})")
            await send_message(client, "@s4tchik", MESSAGE_TEXT)
        
        await asyncio.sleep(5)  # Пауза между запросами

if __name__ == "__main__":
    asyncio.run(main())
