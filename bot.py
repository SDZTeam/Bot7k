from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import json
import logging
from config import TELEGRAM_BOT_TOKEN, ADMIN_ID

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Инициализация бота
storage = MemoryStorage()
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(storage=storage)

# База данных для хранения групп, чатов и аккаунтов
class Database:
    def __init__(self):
        self.data = {
            "groups": {},  # Группы: {"Группа 1": [link1, link2], ...}
            "accounts": []  # Аккаунты: [{"token": "...", "messages": ["msg1", "msg2", ...]}, ...]
        }
        self.load_data()

    def load_data(self):
        try:
            with open("data.json", "r") as f:
                self.data = json.load(f)
        except FileNotFoundError:
            self.save_data()  # Создаем файл, если его нет

    def save_data(self):
        with open("data.json", "w") as f:
            json.dump(self.data, f, indent=4)

    def add_chat(self, group_name, invite_link):
        if group_name not in self.data["groups"]:
            self.data["groups"][group_name] = []
        if invite_link not in self.data["groups"][group_name]:
            self.data["groups"][group_name].append(invite_link)
            self.save_data()

    def remove_chat(self, group_name, invite_link):
        if group_name in self.data["groups"] and invite_link in self.data["groups"][group_name]:
            self.data["groups"][group_name].remove(invite_link)
            self.save_data()

    def get_groups(self):
        return self.data["groups"]

    def add_account(self, token):
        if token not in [acc["token"] for acc in self.data["accounts"]]:
            self.data["accounts"].append({"token": token, "messages": []})
            self.save_data()

    def add_message(self, account_index, message):
        if 0 <= account_index < len(self.data["accounts"]) and len(self.data["accounts"][account_index]["messages"]) < 5:
            self.data["accounts"][account_index]["messages"].append(message)
            self.save_data()

    def get_accounts(self):
        return self.data["accounts"]

db = Database()

# Главное меню
main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Чаты"), KeyboardButton(text="Аккаунты")]
    ],
    resize_keyboard=True
)

# Меню чатов
chats_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Добавить чат"), KeyboardButton(text="Удалить чат")],
        [KeyboardButton(text="Просмотреть чаты"), KeyboardButton(text="Главное меню")]
    ],
    resize_keyboard=True
)

# Меню аккаунтов
accounts_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Создать аккаунт"), KeyboardButton(text="Просмотреть аккаунты")],
        [KeyboardButton(text="Главное меню")]
    ],
    resize_keyboard=True
)

# Состояния для FSM
class Form(StatesGroup):
    add_group_chat = State()  # Добавление чата в группу
    remove_group_chat = State()  # Удаление чата из группы
    create_account = State()  # Создание аккаунта
    add_message = State()  # Добавление сообщения к аккаунту

# Обработчик команды /start
@dp.message(Command("start"))
async def start_command(message: Message):
    if message.from_user.id == ADMIN_ID:
        await message.answer("Вы вошли как администратор.", reply_markup=main_menu)
    else:
        await message.answer("Вы не имеете доступа к этому боту.")

# Обработчик кнопки "Главное меню"
@dp.message(F.text == "Главное меню", StateFilter("*"))
async def back_to_main_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Вы вернулись в главное меню.", reply_markup=main_menu)

# Обработчик кнопки "Чаты"
@dp.message(F.text == "Чаты", StateFilter("*"))
async def chats_menu_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Выберите действие:", reply_markup=chats_menu)

# Обработчик кнопки "Добавить чат"
@dp.message(F.text == "Добавить чат", StateFilter("*"))
async def add_chat_handler(message: Message, state: FSMContext):
    await state.set_state(Form.add_group_chat)
    await message.answer("Введите название группы и пригласительную ссылку через пробел (например, 'Группа 1 https://t.me/your_group_name').")

@dp.message(Form.add_group_chat)
async def process_add_group_chat(message: Message, state: FSMContext):
    try:
        group_name, invite_link = message.text.split(maxsplit=1)
        db.add_chat(group_name.strip(), invite_link.strip())
        await message.answer(f"Группа '{group_name}' успешно добавлена.")
    except ValueError:
        await message.answer("Неверный формат ввода. Введите название группы и пригласительную ссылку через пробел.")
    finally:
        await state.clear()

# Обработчик кнопки "Удалить чат"
@dp.message(F.text == "Удалить чат", StateFilter("*"))
async def remove_chat_handler(message: Message):
    groups = db.get_groups()
    if groups:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{group_name}: {chat_link}", callback_data=f"remove_{group_name}_{chat_link}")]
            for group_name, chat_links in groups.items() for chat_link in chat_links
        ])
        await message.answer("Выберите чат для удаления:", reply_markup=keyboard)
    else:
        await message.answer("Список чатов пуст.")

@dp.callback_query(F.data.startswith("remove_"))
async def process_remove_chat(callback_query: CallbackQuery):
    _, group_name, invite_link = callback_query.data.split("_", maxsplit=2)
    db.remove_chat(group_name, invite_link)
    await callback_query.answer(f"Чат {invite_link} удален из группы {group_name}.")
    await bot.send_message(callback_query.from_user.id, f"Чат {invite_link} успешно удален из группы {group_name}.")

# Обработчик кнопки "Просмотреть чаты"
@dp.message(F.text == "Просмотреть чаты", StateFilter("*"))
async def list_chats_handler(message: Message):
    groups = db.get_groups()
    if groups:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{group_name}", url=chat_link)]
            for group_name, chat_links in groups.items() for chat_link in chat_links
        ])
        await message.answer("Список групп:", reply_markup=keyboard)
    else:
        await message.answer("Список групп пуст.")

# Обработчик кнопки "Аккаунты"
@dp.message(F.text == "Аккаунты", StateFilter("*"))
async def accounts_menu_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Выберите действие:", reply_markup=accounts_menu)

# Обработчик кнопки "Создать аккаунт"
@dp.message(F.text == "Создать аккаунт", StateFilter("*"))
async def create_account_handler(message: Message, state: FSMContext):
    await state.set_state(Form.create_account)
    await message.answer("Введите токен нового аккаунта.")

@dp.message(Form.create_account)
async def process_create_account(message: Message, state: FSMContext):
    token = message.text.strip()
    db.add_account(token)
    await message.answer(f"Аккаунт с токеном {token[:5]}... успешно создан.")
    await state.clear()

# Обработчик кнопки "Просмотреть аккаунты"
@dp.message(F.text == "Просмотреть аккаунты", StateFilter("*"))
async def list_accounts_handler(message: Message):
    accounts = db.get_accounts()
    if accounts:
        response = "Список аккаунтов:\n"
        for i, account in enumerate(accounts, start=1):
            response += f"{i}. Токен: {account['token'][:5]}...\n"
            for j, message_text in enumerate(account["messages"], start=(i - 1) * 5 + 1):
                response += f"   {j}. {message_text}\n"
    else:
        response = "Список аккаунтов пуст."
    await message.answer(response)

# Обработчик добавления сообщений
@dp.message(Command("add_message"), StateFilter("*"))
async def add_message_handler(message: Message, state: FSMContext):
    await state.set_state(Form.add_message)
    await message.answer("Введите номер аккаунта и текст сообщения через пробел (например, '1 Привет!').")

@dp.message(Form.add_message)
async def process_add_message(message: Message, state: FSMContext):
    try:
        account_index, message_text = message.text.split(maxsplit=1)
        account_index = int(account_index) - 1  # Преобразуем номер аккаунта в индекс
        db.add_message(account_index, message_text.strip())
        await message.answer(f"Сообщение '{message_text}' успешно добавлено к аккаунту {account_index + 1}.")
    except ValueError:
        await message.answer("Неверный формат ввода. Введите номер аккаунта и текст сообщения через пробел.")
    finally:
        await state.clear()

# Запуск бота
if __name__ == "__main__":
    dp.run_polling(bot)