from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
import json
from config import TELEGRAM_BOT_TOKEN, ADMIN_ID

# Инициализация бота
storage = MemoryStorage()
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot, storage=storage)

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
            pass

    def save_data(self):
        with open("data.json", "w") as f:
            json.dump(self.data, f)

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
main_menu = ReplyKeyboardMarkup(resize_keyboard=True)
main_menu.add(KeyboardButton("Чаты"), KeyboardButton("Аккаунты"))

# Меню чатов
chats_menu = ReplyKeyboardMarkup(resize_keyboard=True)
chats_menu.add(KeyboardButton("Добавить чат"), KeyboardButton("Удалить чат"))
chats_menu.add(KeyboardButton("Просмотреть чаты"), KeyboardButton("Главное меню"))

# Меню аккаунтов
accounts_menu = ReplyKeyboardMarkup(resize_keyboard=True)
accounts_menu.add(KeyboardButton("Создать аккаунт"), KeyboardButton("Просмотреть аккаунты"))
accounts_menu.add(KeyboardButton("Главное меню"))

# Состояния для FSM
class Form(StatesGroup):
    add_group_chat = State()  # Добавление чата в группу
    remove_group_chat = State()  # Удаление чата из группы
    create_account = State()  # Создание аккаунта
    add_message = State()  # Добавление сообщения к аккаунту

# Обработчик команды /start
@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
    if message.from_user.id == ADMIN_ID:
        await message.answer("Вы вошли как администратор.", reply_markup=main_menu)
    else:
        await message.answer("Вы не имеете доступа к этому боту.")

# Обработчик кнопки "Главное меню"
@dp.message_handler(lambda message: message.text == "Главное меню", state="*")
async def back_to_main_menu(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Вы вернулись в главное меню.", reply_markup=main_menu)

# Обработчик кнопки "Чаты"
@dp.message_handler(lambda message: message.text == "Чаты", state="*")
async def chats_menu_handler(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Выберите действие:", reply_markup=chats_menu)

# Обработчик кнопки "Добавить чат"
@dp.message_handler(lambda message: message.text == "Добавить чат", state="*")
async def add_chat_handler(message: types.Message):
    await Form.add_group_chat.set()
    await message.answer("Введите название группы и пригласительную ссылку через пробел (например, 'Группа 1 https://t.me/your_group_name').")

@dp.message_handler(state=Form.add_group_chat)
async def process_add_group_chat(message: types.Message, state: FSMContext):
    try:
        group_name, invite_link = message.text.split()
        db.add_chat(group_name, invite_link)  # Сохраняем название группы и пригласительную ссылку
        await message.answer(f"Группа '{group_name}' успешно добавлена.")
    except ValueError:
        await message.answer("Неверный формат ввода. Введите название группы и пригласительную ссылку через пробел.")
    finally:
        await state.finish()

# Обработчик кнопки "Удалить чат"
@dp.message_handler(lambda message: message.text == "Удалить чат", state="*")
async def remove_chat_handler(message: types.Message):
    groups = db.get_groups()
    if groups:
        keyboard = InlineKeyboardMarkup(row_width=1)
        for group_name, chat_links in groups.items():
            for chat_link in chat_links:
                callback_data = f"remove_{group_name}_{chat_link}"
                keyboard.add(InlineKeyboardButton(f"{group_name}: {chat_link}", callback_data=callback_data))
        await message.answer("Выберите чат для удаления:", reply_markup=keyboard)
    else:
        await message.answer("Список чатов пуст.")

@dp.callback_query_handler(lambda c: c.data.startswith("remove_"))
async def process_remove_chat(callback_query: types.CallbackQuery):
    _, group_name, invite_link = callback_query.data.split("_")
    db.remove_chat(group_name, invite_link)
    await bot.answer_callback_query(callback_query.id, text=f"Чат {invite_link} удален из группы {group_name}.")
    await bot.send_message(callback_query.from_user.id, f"Чат {invite_link} успешно удален из группы {group_name}.")

# Обработчик кнопки "Просмотреть чаты"
@dp.message_handler(lambda message: message.text == "Просмотреть чаты", state="*")
async def list_chats_handler(message: types.Message):
    groups = db.get_groups()
    if groups:
        keyboard = InlineKeyboardMarkup(row_width=1)
        for group_name, chat_links in groups.items():
            for chat_link in chat_links:
                # Создаем кнопку с текстом "Группа: Ссылка" и URL-ссылкой
                url_button = InlineKeyboardButton(
                    text=f"{group_name}",
                    url=chat_link  # Используем сохраненную пригласительную ссылку
                )
                keyboard.add(url_button)
        await message.answer("Список групп:", reply_markup=keyboard)
    else:
        await message.answer("Список групп пуст.")

# Обработчик кнопки "Аккаунты"
@dp.message_handler(lambda message: message.text == "Аккаунты", state="*")
async def accounts_menu_handler(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Выберите действие:", reply_markup=accounts_menu)

# Обработчик кнопки "Создать аккаунт"
@dp.message_handler(lambda message: message.text == "Создать аккаунт", state="*")
async def create_account_handler(message: types.Message):
    await Form.create_account.set()
    await message.answer("Введите токен нового аккаунта.")

@dp.message_handler(state=Form.create_account)
async def process_create_account(message: types.Message, state: FSMContext):
    token = message.text
    db.add_account(token)
    await message.answer(f"Аккаунт с токеном {token[:5]}... успешно создан.")
    await state.finish()

# Обработчик кнопки "Просмотреть аккаунты"
@dp.message_handler(lambda message: message.text == "Просмотреть аккаунты", state="*")
async def list_accounts_handler(message: types.Message):
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
@dp.message_handler(commands=['add_message'], state="*")
async def add_message_handler(message: types.Message):
    await Form.add_message.set()
    await message.answer("Введите номер аккаунта и текст сообщения через пробел (например, '1 Привет!').")

@dp.message_handler(state=Form.add_message)
async def process_add_message(message: types.Message, state: FSMContext):
    try:
        account_index, message_text = message.text.split(maxsplit=1)
        account_index = int(account_index) - 1  # Преобразуем номер аккаунта в индекс
        db.add_message(account_index, message_text)
        await message.answer(f"Сообщение '{message_text}' успешно добавлено к аккаунту {account_index + 1}.")
    except ValueError:
        await message.answer("Неверный формат ввода. Введите номер аккаунта и текст сообщения через пробел.")
    finally:
        await state.finish()

if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)