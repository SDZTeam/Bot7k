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
from datetime import datetime, timedelta
from config import TELEGRAM_BOT_TOKEN, API_ID, API_HASH, ADMIN_ID

# Настройки рассылки
MESSAGE_TYPES = 5  # Количество видов рассылки
ACCOUNT_DELAY = timedelta(minutes=30)  # Задержка между аккаунтами
MESSAGE_INTERVAL = timedelta(hours=3)  # Интервал между сообщениями одного типа
CHAT_DELAY = timedelta(minutes=15)  # Задержка между чатами для одного аккаунта

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

# Глобальные переменные для рассылки
scheduler_running = False
scheduler_task = None


class AccountManager:
    def __init__(self):
        self.accounts = []
        self.lock = asyncio.Semaphore(1)
        asyncio.run(self.load_accounts())

    @retry(wait=wait_fixed(0.5), stop=stop_after_attempt(5))
    async def load_accounts(self):
        async with self.lock:
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
        async with self.lock:
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
        self.lock = asyncio.Semaphore(1)
        asyncio.run(self.load_groups())

    @retry(wait=wait_fixed(0.5), stop=stop_after_attempt(5))
    async def load_groups(self):
        async with self.lock:
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
        async with self.lock:
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
            [KeyboardButton(text="Управление рассылкой")]
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

    for i in range(start_idx, end_idx):
        keyboard.append([InlineKeyboardButton(
            text=f"Сообщение {i + 1}",
            callback_data=f"msg_{i}"
        )])

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


async def send_message_to_group(account, group, message_type):
    try:
        client = await get_client(account["phone"])
        if not await client.is_user_authorized():
            logging.error(f"Аккаунт {account['phone']} не авторизован!")
            return False

        message_index = (message_type % MESSAGE_TYPES) % len(PREDEFINED_MESSAGES)
        message_text = PREDEFINED_MESSAGES[message_index]

        entity = await client.get_entity(group["id"])
        await client.send_message(entity, message_text)

        account["message_history"].append(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M')} -> {group['title']}: {message_text}"
        )
        account["last_used"] = datetime.now().isoformat()
        await account_manager.save_accounts()

        logging.info(f"Сообщение #{message_index + 1} отправлено в {group['title']} от {account['phone']}")
        return True
    except Exception as e:
        logging.error(f"Ошибка отправки сообщения от {account['phone']} в {group['title']}: {e}")
        return False


async def message_scheduler():
    global scheduler_running
    scheduler_running = True
    message_type = 0
    last_message_time = {}

    while scheduler_running:
        try:
            accounts = account_manager.get_all_accounts()
            groups = group_manager.groups

            if not accounts or not groups:
                await asyncio.sleep(60)
                continue

            current_time = datetime.now()

            # Проверяем, нужно ли отправлять новую партию сообщений
            if message_type not in last_message_time or \
                    (current_time - last_message_time.get(message_type, datetime.min)) >= MESSAGE_INTERVAL:

                for i, account in enumerate(accounts):
                    start_time = current_time + i * ACCOUNT_DELAY

                    for j, group in enumerate(groups):
                        send_time = start_time + j * CHAT_DELAY

                        if send_time > current_time:
                            await asyncio.sleep((send_time - current_time).total_seconds())

                        await send_message_to_group(account, group, message_type + i)
                        await asyncio.sleep(1)  # Небольшая задержка между сообщениями

                last_message_time[message_type] = current_time
                message_type = (message_type + 1) % MESSAGE_TYPES

            await asyncio.sleep(60)  # Проверяем каждую минуту

        except Exception as e:
            logging.error(f"Ошибка в планировщике: {e}")
            await asyncio.sleep(60)


@dp.message(F.text == "Управление рассылкой")
async def manage_scheduler(message: Message):
    global scheduler_running, scheduler_task

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="⏸ Остановить" if scheduler_running else "▶️ Запустить",
                callback_data="toggle_scheduler"
            )
        ],
        [
            InlineKeyboardButton(text="🔄 Статус", callback_data="scheduler_status"),
            InlineKeyboardButton(text="❌ Закрыть", callback_data="close_scheduler_menu")
        ]
    ])

    status = "активна" if scheduler_running else "остановлена"
    await message.answer(
        f"Текущий статус рассылки: {status}\n"
        f"Настройки:\n"
        f"- Типов сообщений: {MESSAGE_TYPES}\n"
        f"- Интервал между типами: {MESSAGE_INTERVAL}\n"
        f"- Задержка между аккаунтами: {ACCOUNT_DELAY}\n"
        f"- Задержка между чатами: {CHAT_DELAY}",
        reply_markup=keyboard
    )


@dp.callback_query(F.data == "toggle_scheduler")
async def toggle_scheduler(callback: CallbackQuery):
    global scheduler_running, scheduler_task

    if scheduler_running:
        scheduler_running = False
        if scheduler_task:
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass
            scheduler_task = None
        await callback.answer("Рассылка остановлена")
    else:
        scheduler_running = True
        scheduler_task = asyncio.create_task(message_scheduler())
        await callback.answer("Рассылка запущена")

    await manage_scheduler(callback.message)


@dp.callback_query(F.data == "scheduler_status")
async def scheduler_status(callback: CallbackQuery):
    accounts = account_manager.get_all_accounts()
    groups = group_manager.groups

    status = (
        f"Статус рассылки:\n"
        f"- Аккаунтов: {len(accounts)}\n"
        f"- Чатов: {len(groups)}\n"
        f"- Сообщений: {len(PREDEFINED_MESSAGES)}\n"
        f"- Работает: {'да' if scheduler_running else 'нет'}"
    )

    await callback.answer(status, show_alert=True)


@dp.callback_query(F.data == "close_scheduler_menu")
async def close_scheduler_menu(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()


@dp.message(Command("start"))
async def start_command(message: Message):
    try:
        if message.from_user.id == ADMIN_ID:
            await message.answer(
                "🚀 Добро пожаловать в Telegram Account Manager!\n\n"
                "📌 Функции бота:\n"
                "- Управление аккаунтами (добавление/удаление)\n"
                "- Управление чатами/группами\n"
                "- Автоматическая рассылка сообщений по расписанию\n\n"
                "👇 Используйте меню ниже для работы с ботом",
                reply_markup=create_main_menu()
            )
            logging.info(f"Бот запущен для администратора {message.from_user.id}")
        else:
            await message.answer("⛔ У вас нет доступа к этому боту.")
            logging.warning(f"Попытка доступа от пользователя {message.from_user.id}")
    except Exception as e:
        logging.error(f"Ошибка в обработчике /start: {e}")
        await message.answer("⚠️ Произошла ошибка при запуске бота. Попробуйте позже.")


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
                await group_manager.save_groups()
                await message.answer(
                    f"Группа успешно добавлена:\n"
                    f"Название: {entity.title}\n"
                    f"ID: {entity.id}\n"
                    f"Username: @{entity.username if hasattr(entity, 'username') else 'нет'}\n"
                    f"Ссылка: {getattr(full_chat.chat, 'exported_invite', 'нет')}",
                    reply_markup=create_groups_menu()
                )
            else:
                await message.answer("Указанный объект не является группой/чатом/каналом!",
                                     reply_markup=create_groups_menu())
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


async def main():
    await connect_all_accounts()
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен пользователем")