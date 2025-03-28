import asyncio
from multiprocessing import connection
import aiofiles
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from tenacity import retry, wait_fixed, stop_after_attempt
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, \
    InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient, connection, types
from telethon.errors import *
import json
import logging
import os
from datetime import datetime
from config import TELEGRAM_BOT_TOKEN, API_ID, API_HASH, ADMIN_ID

# Предустановленные сообщения (50 штук)
PREDEFINED_MESSAGES = [f"Сообщение {i + 1}" for i in range(50)]

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

# Папки для данных
SESSION_DIR = "sessions/"
DATA_DIR = "data/"
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)


class AccountManager:
    def __init__(self):
        self.accounts = []
        self.lock = asyncio.Semaphore(1)  # Ограничение одновременного доступа
        asyncio.run(self.load_accounts())  # Вызов асинхронного метода

    @retry(wait=wait_fixed(0.5), stop=stop_after_attempt(5))
    async def load_accounts(self):
        async with self.lock:  # Блокировка доступа
            try:
                async with aiofiles.open(f"{DATA_DIR}accounts.json", "r") as f:
                    content = await f.read()
                    data = json.loads(content)
                    self.accounts = data.get("accounts", [])
            except FileNotFoundError:
                await self.save_accounts()
            except Exception as e:
                if "database is locked" in str(e).lower():
                    logging.warning("База данных заблокирована. Повторная попытка...")
                    raise
                logging.error(f"Ошибка загрузки аккаунтов: {e}")
                raise

    @retry(wait=wait_fixed(0.5), stop=stop_after_attempt(5))
    async def save_accounts(self):
        async with self.lock:  # Блокировка доступа
            try:
                async with aiofiles.open(f"{DATA_DIR}accounts.json", "w") as f:
                    await f.write(json.dumps({"accounts": self.accounts}, indent=4))
            except Exception as e:
                if "database is locked" in str(e).lower():
                    logging.warning("База данных заблокирована. Повторная попытка...")
                    raise
                logging.error(f"Ошибка сохранения аккаунтов: {e}")
                raise



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


class GroupManager:
    def __init__(self):
        self.groups = []
        self.lock = asyncio.Semaphore(1)  # Ограничение одновременного доступа
        asyncio.run(self.load_groups())

    @retry(wait=wait_fixed(0.5), stop=stop_after_attempt(5))
    async def load_groups(self):
        async with self.lock:  # Блокировка доступа
            try:
                async with aiofiles.open(f"{DATA_DIR}groups.json", "r") as f:
                    content = await f.read()
                    self.groups = json.loads(content).get("groups", [])
            except FileNotFoundError:
                await self.save_groups()
            except Exception as e:
                if "database is locked" in str(e).lower():
                    logging.warning("База данных заблокирована. Повторная попытка...")
                    raise
                logging.error(f"Ошибка загрузки групп: {e}")
                raise

    @retry(wait=wait_fixed(0.5), stop=stop_after_attempt(5))
    async def save_groups(self):
        async with self.lock:  # Блокировка доступа
            try:
                async with aiofiles.open(f"{DATA_DIR}groups.json", "w") as f:
                    await f.write(json.dumps({"groups": self.groups}, indent=4))
            except Exception as e:
                if "database is locked" in str(e).lower():
                    logging.warning("База данных заблокирована. Повторная попытка...")
                    raise
                logging.error(f"Ошибка сохранения групп: {e}")
                raise

    def add_group(self, group_id, title, username=None, invite_link=None):
        if not any(g['id'] == group_id for g in self.groups):
            self.groups.append({
                "id": group_id,
                "title": title,
                "username": username,
                "invite_link": invite_link,
                "added_at": datetime.now().isoformat()
            })
            self.save_groups()
            return True
        return False

    def remove_group(self, group_id):
        initial_count = len(self.groups)
        self.groups = [g for g in self.groups if g['id'] != group_id]
        if len(self.groups) != initial_count:
            self.save_groups()
            return True
        return False

    def get_group(self, group_id):
        for group in self.groups:
            if group['id'] == group_id:
                return group
        return None

    def get_groups_page(self, page=0, per_page=5):
        start = page * per_page
        end = start + per_page
        return self.groups[start:end]


group_manager = GroupManager()


class Form(StatesGroup):
    enter_phone = State()
    enter_code = State()
    enter_password = State()
    select_account = State()
    select_target = State()
    select_message = State()
    add_group = State()
    create_message = State()
    select_group_action = State()
    confirm_group_deletion = State()


def create_main_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Аккаунты"), KeyboardButton(text="Чаты")],
            [KeyboardButton(text="Отправить сообщение")]
        ],
        resize_keyboard=True
    )


def create_accounts_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить аккаунт"), KeyboardButton(text="Список аккаунтов")],
            [KeyboardButton(text="Создать сообщение"), KeyboardButton(text="Главное меню")]
        ],
        resize_keyboard=True
    )

active_clients = {}

async def get_client(phone: str) -> TelegramClient:
    if phone not in active_clients:
        account = account_manager.get_account(phone)
        client = TelegramClient(
            account["session_file"],
            API_ID,
            API_HASH,
            connection=connection.ConnectionTcpFull,
            auto_reconnect=True,
            retry_delay=10
        )
        await client.connect()
        active_clients[phone] = client
    return active_clients[phone]

def create_groups_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить группу"), KeyboardButton(text="Список групп")],
            [KeyboardButton(text="Главное меню")]
        ],
        resize_keyboard=True
    )


def create_pagination_keyboard(page=0, total_pages=1, prefix="groups"):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="<<", callback_data=f"{prefix}_first"),
            InlineKeyboardButton(text="<", callback_data=f"{prefix}_prev_{page}"),
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="current"),
            InlineKeyboardButton(text=">", callback_data=f"{prefix}_next_{page}"),
            InlineKeyboardButton(text=">>", callback_data=f"{prefix}_last")
        ]
    ])


def create_message_keyboard(page=0, per_page=5):
    total_pages = (len(PREDEFINED_MESSAGES) + per_page - 1) // per_page
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, len(PREDEFINED_MESSAGES))

    keyboard = []

    # Добавляем кнопки сообщений для текущей страницы
    for i in range(start_idx, end_idx):
        keyboard.append([InlineKeyboardButton(
            text=f"Сообщение {i + 1}",
            callback_data=f"msg_{i}"
        )])

    # Добавляем кнопки пагинации
    pagination_buttons = []
    if page > 0:
        pagination_buttons.append(InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"msgpage_{page - 1}"
        ))
    if page < total_pages - 1:
        pagination_buttons.append(InlineKeyboardButton(
            text="Вперёд ➡️",
            callback_data=f"msgpage_{page + 1}"
        ))

    if pagination_buttons:
        keyboard.append(pagination_buttons)

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def create_group_actions_keyboard(group_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Перейти", callback_data=f"group_join_{group_id}"),
            InlineKeyboardButton(text="Удалить", callback_data=f"group_delete_{group_id}")
        ],
        [InlineKeyboardButton(text="Назад", callback_data="groups_back")]
    ])


@dp.message(Command("start"))
async def start_command(message: Message):
    if message.from_user.id == ADMIN_ID:
        await message.answer("Добро пожаловать в Telegram Account Manager!", reply_markup=create_main_menu())
    else:
        await message.answer("У вас нет доступа к этому боту.")


@dp.message(F.text == "Главное меню")
async def main_menu_handler(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню:", reply_markup=create_main_menu())


@dp.message(F.text == "Аккаунты")
async def account_management(message: Message):
    await message.answer("Управление аккаунтами:", reply_markup=create_accounts_menu())


@dp.message(F.text == "Чаты")
async def chats_management(message: Message):
    await message.answer("Управление чатами:", reply_markup=create_groups_menu())


@dp.message(F.text == "Добавить группу")
async def add_group_handler(message: Message, state: FSMContext):
    await state.set_state(Form.add_group)
    await message.answer(
        "Введите данные группы в одном из форматов:\n"
        "- @username группы\n"
        "- Ссылка-приглашение\n"
        "- ID группы (например, -100123456789)\n\n"
        "Или перешлите сообщение из нужной группы:"
    )

@dp.message(Form.add_group)
async def process_add_group(message: Message, state: FSMContext):
    try:
        group_input = message.text.strip()
        accounts = account_manager.get_all_accounts()
        if not accounts:
            await message.answer("Нет доступных аккаунтов для проверки группы!")
            await state.clear()
            return

        account = accounts[0]
        client = TelegramClient(
            account["session_file"],
            API_ID,
            API_HASH,
            connection=connection.ConnectionTcpFull,
            auto_reconnect=True,
            retry_delay=10
        )
        await client.connect()

        if not await client.is_user_authorized():
            await message.answer("Ошибка: аккаунт не авторизован!")
            await state.clear()
            return

        try:
            # Попытка присоединиться к группе
            if "joinchat" in group_input or "+C" in group_input:
                hash_part = group_input.split("/")[-1]
                await client(ImportChatInviteRequest(hash_part))

            entity = await client.get_entity(group_input)

            if isinstance(entity, (types.Channel, types.Chat)):
                full_chat = await client(types.channels.GetFullChannelRequest(channel=entity))
                group_added = group_manager.add_group(
                    group_id=entity.id,
                    title=entity.title,
                    username=getattr(entity, 'username', None),
                    invite_link=getattr(full_chat.chat, 'exported_invite', None)
                )
                await group_manager.save_groups()  # Сохраняем группы с блокировкой
                await message.answer(
                    f"Группа успешно добавлена:\n"
                    f"Название: {entity.title}\n"
                    f"ID: {entity.id}\n"
                    f"Username: @{entity.username if hasattr(entity, 'username') else 'нет'}\n"
                    f"Ссылка: {getattr(full_chat.chat, 'exported_invite', 'нет')}",
                    reply_markup=create_groups_menu()
                )
            else:
                await message.answer("Указанный объект не является группой/чатом/каналом!", reply_markup=create_groups_menu())
        except Exception as e:
            await message.answer(f"Ошибка при получении информации о группе: {e}", reply_markup=create_groups_menu())
    except Exception as e:
        await message.answer(f"Ошибка: {e}", reply_markup=create_groups_menu())
    finally:
        await state.clear()

@dp.message(F.text == "Список групп")
async def list_groups(message: Message):
    total_groups = len(group_manager.groups)
    if total_groups == 0:
        return await message.answer("Список групп пуст. Добавьте группы через меню.", reply_markup=create_groups_menu())

    total_pages = (total_groups + 4) // 5
    current_page = 0
    groups = group_manager.get_groups_page(current_page)

    response = "Список групп:\n" + "\n".join(
        [f"{idx + 1}. {group['title']} (ID: {group['id']})"
         for idx, group in enumerate(groups)]
    )

    await message.answer(
        response,
        reply_markup=create_pagination_keyboard(
            page=current_page,
            total_pages=total_pages,
            prefix="groups"
        )
    )


@dp.callback_query(F.data.startswith(("groups_prev_", "groups_next_", "groups_first", "groups_last")))
async def groups_pagination_handler(callback: CallbackQuery):
    data = callback.data
    current_page = int(data.split("_")[-1]) if "_" in data and data.split("_")[-1].isdigit() else 0
    total_groups = len(group_manager.groups)
    total_pages = (total_groups + 4) // 5

    if "prev" in data:
        new_page = max(0, current_page - 1)
    elif "next" in data:
        new_page = min(total_pages - 1, current_page + 1)
    elif "first" in data:
        new_page = 0
    elif "last" in data:
        new_page = total_pages - 1

    groups = group_manager.get_groups_page(new_page)
    response = "Список групп:\n" + "\n".join(
        [f"{idx + 1}. {group['title']} (ID: {group['id']})"
         for idx, group in enumerate(groups)]
    )

    await callback.message.edit_text(
        response,
        reply_markup=create_pagination_keyboard(
            page=new_page,
            total_pages=total_pages,
            prefix="groups"
        )
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("group_select_"))
async def select_group_action(callback: CallbackQuery):
    group_id = int(callback.data.split("_")[-1])
    group = group_manager.get_group(group_id)

    if not group:
        await callback.answer("Группа не найдена!")
        return

    await callback.message.answer(
        f"Группа: {group['title']}\n"
        f"ID: {group['id']}\n"
        f"Username: @{group.get('username', 'нет')}\n"
        f"Ссылка: {group.get('invite_link', 'нет')}",
        reply_markup=create_group_actions_keyboard(group_id)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("group_join_"))
async def join_group(callback: CallbackQuery):
    group_id = int(callback.data.split("_")[-1])
    group = group_manager.get_group(group_id)

    if not group:
        await callback.answer("Группа не найдена!")
        return

    if group.get('username'):
        link = f"https://t.me/{group['username']}"
    elif group.get('invite_link'):
        link = group['invite_link']
    else:
        link = None

    if link:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Перейти в группу", url=link)],
            [InlineKeyboardButton(text="Назад к списку", callback_data="groups_back")]
        ])
        await callback.message.answer(
            f"Ссылка для перехода в группу {group['title']}:",
            reply_markup=keyboard
        )
    else:
        await callback.message.answer(
            f"Для группы {group['title']} нет доступной ссылки. "
            f"Используйте ID: {group['id']} для доступа.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Назад к списку", callback_data="groups_back")]
            ])
        )

    await callback.answer()


@dp.callback_query(F.data.startswith("group_delete_"))
async def delete_group_handler(callback: CallbackQuery, state: FSMContext):
    group_id = int(callback.data.split("_")[-1])
    group = group_manager.get_group(group_id)

    if not group:
        await callback.answer("Группа не найдена!")
        return

    await state.set_state(Form.confirm_group_deletion)
    await state.update_data(group_id=group_id)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Да", callback_data="confirm_delete"),
            InlineKeyboardButton(text="Нет", callback_data="cancel_delete")
        ]
    ])

    await callback.message.answer(
        f"Вы уверены, что хотите удалить группу {group['title']}?",
        reply_markup=keyboard
    )
    await callback.answer()


@dp.callback_query(F.data == "confirm_delete", Form.confirm_group_deletion)
async def confirm_group_deletion(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    group_id = data.get("group_id")

    if group_manager.remove_group(group_id):
        await callback.message.answer("Группа успешно удалена!", reply_markup=create_groups_menu())
    else:
        await callback.message.answer("Ошибка при удалении группы!", reply_markup=create_groups_menu())

    await state.clear()
    await callback.answer()


@dp.callback_query(F.data == "cancel_delete", Form.confirm_group_deletion)
async def cancel_group_deletion(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Удаление отменено.", reply_markup=create_groups_menu())
    await state.clear()
    await callback.answer()


@dp.callback_query(F.data == "groups_back")
async def back_to_groups_list(callback: CallbackQuery):
    await callback.message.delete()
    await list_groups(callback.message)
    await callback.answer()


@dp.message(F.text == "Добавить аккаунт")
async def add_account(message: Message, state: FSMContext):
    await state.set_state(Form.enter_phone)
    await message.answer(
        "Введите номер телефона в международном формате (например, +79123456789):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )


@dp.message(F.text == "Отмена", Form.enter_phone)
async def cancel_add_account(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Добавление аккаунта отменено.", reply_markup=create_accounts_menu())


@dp.message(Form.enter_phone)
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    session_file = os.path.join(SESSION_DIR, phone)
    if account_manager.get_account(phone):
        await message.answer("Этот аккаунт уже добавлен!", reply_markup=create_accounts_menu())
        await state.clear()
        return

    # СУУУУУУУУУУУУУУУУУУУУУУУУУУУУКА ТУТ
    # СУУУУУУУУУУУУУУУУУУУУУУУУУУУУКА ТУТ
    # СУУУУУУУУУУУУУУУУУУУУУУУУУУУУКА ТУТ
    # СУУУУУУУУУУУУУУУУУУУУУУУУУУУУКА ТУТ
    # СУУУУУУУУУУУУУУУУУУУУУУУУУУУУКА ТУТ
    # СУУУУУУУУУУУУУУУУУУУУУУУУУУУУКА ТУТ
    # СУУУУУУУУУУУУУУУУУУУУУУУУУУУУКА ТУТ
    client = TelegramClient(session_file, API_ID, API_HASH)
    try:
        await client.connect()
        await client.send_code_request(phone)
        await state.update_data(phone=phone, client=client)
        await message.answer(
            "Код отправлен. Введите полученный код:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(Form.enter_code)
    except FloodWaitError as e:
        await message.answer(
            f"Слишком много попыток. Подождите {e.seconds} секунд.",
            reply_markup=create_accounts_menu()
        )
        await state.clear()
    except Exception as e:
        await message.answer(
            f"Ошибка: {str(e)}",
            reply_markup=create_accounts_menu()
        )
        await state.clear()


@dp.message(F.text == "Отмена", Form.enter_code)
async def cancel_add_account_code(message: Message, state: FSMContext):
    data = await state.get_data()
    client = data.get("client")
    await state.clear()
    await message.answer("Добавление аккаунта отменено.", reply_markup=create_accounts_menu())


@dp.message(Form.enter_code)
async def process_code(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    phone = data.get("phone")
    client = data.get("client")

    try:
        await client.sign_in(phone, code)
        account_manager.add_account(phone, client.session.filename)
        await message.answer("✅ Аккаунт успешно авторизован!", reply_markup=create_accounts_menu())
    except PhoneCodeInvalidError:
        await message.answer("❌ Неверный код! Попробуйте еще раз:")
        return
    except PhoneCodeExpiredError:
        await message.answer("⌛ Срок действия кода истек. Запросите новый код.")
        await client.send_code_request(phone)
        return
    except SessionPasswordNeededError:
        await message.answer(
            "🔒 Требуется пароль двухфакторной аутентификации:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Отмена")]],
                resize_keyboard=True
            )
        )
        await state.set_state(Form.enter_password)
        return
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=create_accounts_menu())

    await state.clear()


@dp.message(F.text == "Отмена", Form.enter_password)
async def cancel_add_account_password(message: Message, state: FSMContext):
    data = await state.get_data()
    client = data.get("client")
    await state.clear()
    await message.answer("Добавление аккаунта отменено.", reply_markup=create_accounts_menu())


@dp.message(Form.enter_password)
async def process_password(message: Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    phone = data.get("phone")
    client = data.get("client")

    try:
        await client.sign_in(password=password)
        account_manager.add_account(phone, client.session.filename)
        await message.answer(
            "✅ Аккаунт успешно авторизован с двухфакторной аутентификацией!",
            reply_markup=create_accounts_menu()
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=create_accounts_menu())

    await state.clear()


@dp.message(F.text == "Список аккаунтов")
async def list_accounts(message: Message):
    accounts = account_manager.get_all_accounts()
    if not accounts:
        await message.answer("📭 Список аккаунтов пуст.", reply_markup=create_accounts_menu())
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{acc['phone']} ({len(acc['message_history'])} сообщ.)",
            callback_data=f"account_detail_{acc['phone']}"
        )] for acc in accounts
    ])

    await message.answer("📋 Список аккаунтов:", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("account_detail_"))
async def show_account_detail(callback: CallbackQuery):
    phone = callback.data.split("_")[2]
    account = account_manager.get_account(phone)

    if not account:
        await callback.answer("Аккаунт не найден!")
        return

    info = (
        f"📱 Аккаунт: {phone}\n"
        f"⏳ Последняя активность: {account['last_used']}\n"
        f"📨 Отправлено сообщений: {len(account['message_history'])}\n"
        f"📝 История сообщений:\n"
    )

    last_messages = account["message_history"][-5:]
    for msg in last_messages:
        info += f"  - {msg}\n"

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"delete_account_{phone}")
        ],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="accounts_back")]
    ])

    await callback.message.edit_text(info, reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data == "accounts_back")
async def back_to_accounts_list(callback: CallbackQuery):
    await callback.message.delete()
    await list_accounts(callback.message)
    await callback.answer()


@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account_handler(callback: CallbackQuery):
    phone = callback.data.split("_")[2]
    account = account_manager.get_account(phone)

    if not account:
        await callback.answer("Аккаунт не найден!")
        return

    try:
        os.remove(account["session_file"])
        account_manager.accounts = [acc for acc in account_manager.accounts if acc["phone"] != phone]
        await account_manager.save_accounts()
        await callback.answer("✅ Аккаунт удален!")
        await back_to_accounts_list(callback)
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}")


@dp.message(F.text == "Отправить сообщение")


@dp.callback_query(F.data == "send_msg_back")
async def back_from_send_message(callback: CallbackQuery):
    await callback.message.delete()
    await callback.message.answer("Главное меню:", reply_markup=create_main_menu())
    await callback.answer()


@dp.callback_query(F.data.startswith("select_sender_"))
async def select_sender(callback: CallbackQuery, state: FSMContext):
    phone = callback.data.split("_")[2]
    await state.update_data(sender_phone=phone)
    await callback.message.answer(
        "👥 Введите получателя в одном из форматов:\n"
        "- @username\n"
        "- ID чата (например, -100123456789)\n"
        "- Номер телефона (для контактов)\n"
        "- Ссылка на чат\n\n"
        "Или введите 'отмена' для возврата",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(Form.select_target)
    await callback.answer()


@dp.message(F.text.lower() == "отмена", Form.select_target)
async def cancel_send_message_target(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отправка сообщения отменена.", reply_markup=create_main_menu())


@dp.message(Form.select_target)
async def process_target(message: Message, state: FSMContext):
    target = message.text.strip()
    await state.update_data(target=target)
    await message.answer(
        "📩 Выберите сообщение для отправки:",
        reply_markup=create_message_keyboard(page=0)
    )
    await state.set_state(Form.select_message)


@dp.callback_query(F.data.startswith("msgpage_"))
async def handle_message_pagination(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[1])
    await callback.message.edit_reply_markup(
        reply_markup=create_message_keyboard(page=page)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("msg_"), Form.select_message)
async def process_message_selection(callback: CallbackQuery, state: FSMContext):
    try:
        msg_index = int(callback.data.split("_")[1])
        data = await state.get_data()
        phone = data.get("sender_phone")
        target = data.get("target")
        
        if msg_index < 0 or msg_index >= len(PREDEFINED_MESSAGES):
            await callback.answer("❌ Неверный номер сообщения!")
            return
            
        message_text = PREDEFINED_MESSAGES[msg_index]
        account = account_manager.get_account(phone)
        
        if not account:
            await callback.answer("❌ Аккаунт не найден!")
            await state.clear()
            return
            
        # Создаем клиент с правильным соединением
        client = TelegramClient(
            account["session_file"],
            API_ID,
            API_HASH,
            connection=connection.ConnectionTcpFull,  # Используем стандартное соединение
            auto_reconnect=True,
            retry_delay=10
        )
        
        await client.connect()
        
        if not await client.is_user_authorized():
            await callback.answer("❌ Аккаунт не авторизован!")
            await state.clear()
            return
            
        try:
            entity = await client.get_entity(target)
            await client.send_message(entity, message_text)
            
            account["message_history"].append(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M')} -> {target}: {message_text}"
            )
            account["last_used"] = datetime.now().isoformat()
            await account_manager.save_accounts()
            
            await callback.answer(f"✅ Сообщение #{msg_index + 1} отправлено!")
            await callback.message.answer(
                f"📨 Сообщение успешно отправлено!\n"
                f"👤 Отправитель: {phone}\n"
                f"👥 Получатель: {target}\n"
                f"📝 Текст: {message_text}",
                reply_markup=create_main_menu()
            )
        except ValueError:
            await callback.answer("❌ Неверный формат получателя!")
        except Exception as e:
            await callback.answer(f"❌ Ошибка отправки: {str(e)}")
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}")
    finally:
        await state.clear()
        
@dp.message(F.text == "Создать сообщение")
async def create_message_handler(message: Message, state: FSMContext):
    await state.set_state(Form.create_message)
    await message.answer(
        "✍️ Введите текст нового сообщения (макс. 1000 символов):\n"
        "Можно использовать разметку Markdown: *жирный*, _курсив_, `код`\n\n"
        "Или введите 'отмена' для возврата",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )


@dp.message(F.text.lower() == "отмена", Form.create_message)
async def cancel_create_message(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Создание сообщения отменено.", reply_markup=create_accounts_menu())


@dp.message(Form.create_message)
async def process_create_message(message: Message, state: FSMContext):
    if len(message.text) > 1000:
        await message.answer(
            "❌ Сообщение слишком длинное (макс. 1000 символов)!",
            reply_markup=create_accounts_menu()
        )
        return

    PREDEFINED_MESSAGES.append(message.text)
    await message.answer(
        f"✅ Сообщение #{len(PREDEFINED_MESSAGES)} успешно создано!",
        reply_markup=create_accounts_menu()
    )
    await state.clear()

from aiogram import exceptions

@dp.errors()
async def handle_errors(update, exception):
    if isinstance(exception, exceptions.TelegramUnauthorizedError):
        logging.error("Ошибка авторизации бота! Проверьте токен.")
        return True
    elif isinstance(exception, FloodWaitError):
        wait_time = exception.seconds
        logging.warning(f"FloodWaitError: ждем {wait_time} секунд")
        await asyncio.sleep(wait_time)
        return True
    return False

async def connect_all_accounts():
    for acc in account_manager.get_all_accounts():
        try:
            client = TelegramClient(
                acc["session_file"],
                API_ID,
                API_HASH,
                connection=connection.ConnectionTcpFull,
                auto_reconnect=True,
                retry_delay=10
            )
            await client.connect()
            if not await client.is_user_authorized():
                logging.error(f"Аккаунт {acc['phone']} не авторизован!")
        except Exception as e:
            logging.error(f"Ошибка подключения {acc['phone']}: {e}")

async def keep_alive():
    while True:
        await asyncio.sleep(300)  # Каждые 5 минут
        for phone in list(active_clients.keys()):
            try:
                client = await get_client(phone)
                await client.get_me()  # Проверка активности
                logging.info(f"Keep-alive для {phone}")
            except Exception as e:
                logging.error(f"Keep-alive ошибка {phone}: {e}")

# В основном блоке
async def main():
    await connect_all_accounts()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен пользователем")