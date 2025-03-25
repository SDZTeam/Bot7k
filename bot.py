from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, \
    InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient
from telethon.errors import *
import json
import logging
import os
from datetime import datetime
from config import TELEGRAM_BOT_TOKEN, API_ID, API_HASH, ADMIN_ID

# Предустановленные сообщения (50 штук)
PREDEFINED_MESSAGES = [
    f"Сообщение {i + 1}" for i in range(50)
]

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='bot.log'
)

# Инициализация бота
storage = MemoryStorage()
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(storage=storage)

# Папка для сессий
SESSION_DIR = "sessions/"
os.makedirs(SESSION_DIR, exist_ok=True)


# База данных аккаунтов
class AccountManager:
    def __init__(self):
        self.accounts = []
        self.load_accounts()

    def load_accounts(self):
        try:
            with open("accounts.json", "r") as f:
                data = json.load(f)
                self.accounts = data.get("accounts", [])
        except FileNotFoundError:
            self.save_accounts()

    def save_accounts(self):
        with open("accounts.json", "w") as f:
            json.dump({"accounts": self.accounts}, f, indent=4)

    def add_account(self, phone, session_file):
        if not self.get_account(phone):
            self.accounts.append({
                "phone": phone,
                "session_file": session_file,
                "last_used": datetime.now().isoformat(),
                "message_history": []
            })
            self.save_accounts()

    def get_account(self, phone):
        for acc in self.accounts:
            if acc["phone"] == phone:
                return acc
        return None

    def get_all_accounts(self):
        return self.accounts

    def update_account(self, phone, **kwargs):
        for acc in self.accounts:
            if acc["phone"] == phone:
                acc.update(kwargs)
                self.save_accounts()
                return


account_manager = AccountManager()


# Состояния FSM
class Form(StatesGroup):
    enter_phone = State()
    enter_code = State()
    enter_password = State()
    select_account = State()
    select_target = State()  # Новое состояние для выбора целевого пользователя
    select_message = State()  # Новое состояние для выбора сообщения


# Клавиатуры
main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Управление аккаунтами")],
        [KeyboardButton(text="Отправить сообщение")]
    ],
    resize_keyboard=True
)

account_management_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Добавить аккаунт")],
        [KeyboardButton(text="Список аккаунтов")],
        [KeyboardButton(text="Главное меню")]
    ],
    resize_keyboard=True
)


# Создание клавиатуры с предустановленными сообщениями
def create_message_keyboard():
    keyboard = []
    for i in range(0, 50, 5):  # 5 кнопок в ряду
        row = [
            InlineKeyboardButton(text=f"#{num + 1}", callback_data=f"msg_{num}")
            for num in range(i, min(i + 5, 50))
        ]
        keyboard.append(row)
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# Обработчик команды /start
@dp.message(Command("start"))
async def start_command(message: Message):
    if message.from_user.id == ADMIN_ID:
        await message.answer("Добро пожаловать в Telegram Account Manager!", reply_markup=main_menu)
    else:
        await message.answer("У вас нет доступа к этому боту.")


# Управление аккаунтами
@dp.message(F.text == "Управление аккаунтами")
async def account_management(message: Message):
    await message.answer("Выберите действие:", reply_markup=account_management_menu)


# Добавление аккаунта
@dp.message(F.text == "Добавить аккаунт")
async def add_account(message: Message, state: FSMContext):
    await state.set_state(Form.enter_phone)
    await message.answer("Введите номер телефона в международном формате (например, +79123456789):")


@dp.message(Form.enter_phone)
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    session_file = os.path.join(SESSION_DIR, phone)
    if account_manager.get_account(phone):
        await message.answer("Этот аккаунт уже добавлен!")
        return
    client = TelegramClient(session_file, API_ID, API_HASH)
    try:
        await client.connect()
        await client.send_code_request(phone)
        await state.update_data(phone=phone, client=client)
        await message.answer("Код отправлен. Введите полученный код:")
        await state.set_state(Form.enter_code)
    except FloodWaitError as e:
        await message.answer(f"Слишком много попыток. Подождите {e.seconds} секунд.")
    except Exception as e:
        await message.answer(f"Ошибка: {e}")


@dp.message(Form.enter_code)
async def process_code(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    phone = data.get("phone")
    client = data.get("client")
    try:
        await client.sign_in(phone, code)
        account_manager.add_account(phone, client.session.filename)
        await message.answer("Аккаунт успешно авторизован!")
    except PhoneCodeInvalidError:
        await message.answer("Неверный код! Попробуйте еще раз:")
    except PhoneCodeExpiredError:
        await message.answer("Срок действия кода истек. Запросите новый код.")
        await client.send_code_request(phone)
    except SessionPasswordNeededError:
        await message.answer("Требуется пароль двухфакторной аутентификации:")
        await state.set_state(Form.enter_password)
    except Exception as e:
        await message.answer(f"Ошибка: {e}")
    finally:
        await state.clear()


@dp.message(Form.enter_password)
async def process_password(message: Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    phone = data.get("phone")
    client = data.get("client")
    try:
        await client.sign_in(password=password)
        account_manager.add_account(phone, client.session.filename)
        await message.answer("Аккаунт успешно авторизован с двухфакторной аутентификацией!")
    except Exception as e:
        await message.answer(f"Ошибка: {e}")
    finally:
        await state.clear()


# Список аккаунтов
@dp.message(F.text == "Список аккаунтов")
async def list_accounts(message: Message):
    accounts = account_manager.get_all_accounts()
    if not accounts:
        await message.answer("Список аккаунтов пуст.")
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=acc["phone"], callback_data=f"account_{acc['phone']}")]
        for acc in accounts
    ])
    await message.answer("Выберите аккаунт:", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("account_"))
async def show_account_info(callback: CallbackQuery):
    phone = callback.data.split("_")[1]
    account = account_manager.get_account(phone)
    if not account:
        await callback.answer("Аккаунт не найден!")
        return
    info = f"Аккаунт: {phone}\n"
    info += f"Последнее использование: {account['last_used']}\n"
    info += "История сообщений:\n"
    info += "\n".join(account["message_history"]) or "Нет сообщений"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Удалить аккаунт", callback_data=f"delete_{phone}")]
    ])
    await callback.message.answer(info, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("delete_"))
async def delete_account(callback: CallbackQuery):
    phone = callback.data.split("_")[1]
    account = account_manager.get_account(phone)
    if account:
        os.remove(account["session_file"])
        account_manager.accounts = [acc for acc in account_manager.accounts if acc["phone"] != phone]
        account_manager.save_accounts()
        await callback.answer("Аккаунт удален!")
    else:
        await callback.answer("Аккаунт не найден!")


# Отправка сообщения
@dp.message(F.text == "Отправить сообщение")
async def send_message_menu(message: Message, state: FSMContext):
    accounts = account_manager.get_all_accounts()
    if not accounts:
        await message.answer("Нет доступных аккаунтов!")
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=acc["phone"], callback_data=f"select_sender_{acc['phone']}")]
        for acc in accounts
    ])
    await message.answer("Выберите аккаунт для отправки:", reply_markup=keyboard)
    await state.set_state(Form.select_account)


@dp.callback_query(F.data.startswith("select_sender_"))
async def select_sender(callback: CallbackQuery, state: FSMContext):
    phone = callback.data.split("_")[2]
    await state.update_data(sender_phone=phone)
    await callback.message.answer("Введите @username или ID получателя:")
    await state.set_state(Form.select_target)


@dp.message(Form.select_target)
async def process_target(message: Message, state: FSMContext):
    target = message.text.strip()
    await state.update_data(target=target)
    await message.answer("Выберите сообщение для отправки:", reply_markup=create_message_keyboard())
    await state.set_state(Form.select_message)


@dp.callback_query(F.data.startswith("msg_"))
async def process_message_selection(callback: CallbackQuery, state: FSMContext):
    try:
        msg_index = int(callback.data.split("_")[1])
        data = await state.get_data()
        phone = data.get("sender_phone")
        target = data.get("target")
        message_text = PREDEFINED_MESSAGES[msg_index]

        account = account_manager.get_account(phone)
        client = TelegramClient(account["session_file"], API_ID, API_HASH)
        await client.connect()

        if not await client.is_user_authorized():
            await callback.answer("Аккаунт не авторизован!")
            return

        # Резолвинг получателя
        try:
            if target.isdigit() or (target.startswith('-') and target[1:].isdigit()):
                entity = await client.get_entity(int(target))  # ID чата/канала
            elif target.startswith('@'):
                entity = await client.get_entity(target)  # Username
            else:
                raise ValueError("Неверный формат получателя")

            # Отправка сообщения
            await client.send_message(entity, message_text)
            await callback.answer(f"Сообщение {msg_index + 1} отправлено!")

            # Обновляем историю
            account["message_history"].append(f"To {target}: {message_text}")
            account["last_used"] = datetime.now().isoformat()
            account_manager.save_accounts()

            await callback.message.answer(f"Сообщение #{msg_index + 1} успешно отправлено!")

        except Exception as e:
            await callback.answer(f"Ошибка отправки: {e}")

    except Exception as e:
        await callback.answer(f"Ошибка выбора сообщения: {e}")
    finally:
        await state.clear()


if __name__ == "__main__":
    dp.run_polling(bot)