import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from telethon import TelegramClient, types
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.functions.auth import ResetAuthorizationsRequest
from telethon.errors import (
    FloodWaitError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    SessionPasswordNeededError
)

from tenacity import retry, wait_fixed, stop_after_attempt
from aiogram import Bot, Dispatcher, F, exceptions
from aiogram.filters import Command
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config import TELEGRAM_BOT_TOKEN, API_ID, API_HASH, ADMIN_ID

# Constants
MESSAGE_TYPES = 5
ACCOUNT_DELAY = timedelta(minutes=1)
MESSAGE_INTERVAL = timedelta(minutes=2)
CHAT_DELAY = timedelta(minutes=3)
PREDEFINED_MESSAGES = [f"Сообщение {i + 1}" for i in range(50)]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='bot.log'
)
logger = logging.getLogger(__name__)

# Initialize bot
storage = MemoryStorage()
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(storage=storage)

# Data directories
SESSION_DIR = "sessions/"
DB_FILE = "data/bot_data.db"
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)

# Global variables
scheduler_running = False
scheduler_task: Optional[asyncio.Task] = None
active_clients: Dict[str, TelegramClient] = {}
client_locks: Dict[str, asyncio.Lock] = {}


class Database:
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.conn = None
        self._initialize_db()

    def _initialize_db(self):
        """Initialize database tables"""
        self.conn = sqlite3.connect(self.db_file)
        cursor = self.conn.cursor()

        # Create accounts table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS accounts (
            phone TEXT PRIMARY KEY,
            session_file TEXT NOT NULL,
            last_used TEXT,
            password TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # Create account_sessions table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS account_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phone TEXT NOT NULL,
            session_file TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_used TEXT,
            FOREIGN KEY (phone) REFERENCES accounts(phone)
        )
        ''')

        # Create groups table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            username TEXT,
            invite_link TEXT,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # Create messages table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_phone TEXT NOT NULL,
            group_id INTEGER NOT NULL,
            message_text TEXT NOT NULL,
            sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_phone) REFERENCES accounts(phone),
            FOREIGN KEY (group_id) REFERENCES groups(id)
        )
        ''')

        # Create predefined_messages table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS predefined_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # Insert initial predefined messages if table is empty
        cursor.execute('SELECT COUNT(*) FROM predefined_messages')
        if cursor.fetchone()[0] == 0:
            for i, msg in enumerate(PREDEFINED_MESSAGES):
                cursor.execute(
                    'INSERT INTO predefined_messages (text) VALUES (?)',
                    (msg,)
                )

        self.conn.commit()

    def execute(self, query: str, params: tuple = (), commit: bool = False):
        """Execute SQL query"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            if commit:
                self.conn.commit()
            return cursor
        except sqlite3.Error as e:
            logger.error(f"Database error: {e}")
            self.conn.rollback()
            raise

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


class AccountManager:
    def __init__(self, db: Database):
        self.db = db

    async def add_account(self, phone: str, session_file: str, password: str = None) -> bool:
        """Add new account to database"""
        try:
            self.db.execute(
                'INSERT INTO accounts (phone, session_file, last_used, password) VALUES (?, ?, ?, ?)',
                (phone, session_file, datetime.now().isoformat(), password),
                commit=True
            )
            return True
        except sqlite3.IntegrityError:
            return False  # Account already exists

    async def get_account(self, phone: str) -> Optional[dict]:
        """Get account by phone number"""
        cursor = self.db.execute(
            'SELECT phone, session_file, last_used FROM accounts WHERE phone = ?',
            (phone,)
        )
        row = cursor.fetchone()
        if row:
            return {
                'phone': row[0],
                'session_file': row[1],
                'last_used': row[2]
            }
        return None

    async def get_all_accounts(self) -> List[dict]:
        """Get all accounts"""
        cursor = self.db.execute('SELECT phone, session_file, last_used FROM accounts')
        return [
            {
                'phone': row[0],
                'session_file': row[1],
                'last_used': row[2]
            } for row in cursor.fetchall()
        ]

    async def update_account(self, phone: str, **kwargs) -> bool:
        """Update account data"""
        if not kwargs:
            return False

        set_clause = ', '.join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values())
        values.append(phone)

        self.db.execute(
            f'UPDATE accounts SET {set_clause} WHERE phone = ?',
            tuple(values),
            commit=True
        )
        return True

    async def remove_account(self, phone: str) -> bool:
        """Remove account from database"""
        self.db.execute(
            'DELETE FROM accounts WHERE phone = ?',
            (phone,),
            commit=True
        )
        return True

    async def add_session(self, phone: str, session_file: str) -> bool:
        """Add new session for account"""
        try:
            self.db.execute(
                'INSERT INTO account_sessions (phone, session_file, last_used) VALUES (?, ?, ?)',
                (phone, session_file, datetime.now().isoformat()),
                commit=True
            )
            return True
        except sqlite3.Error:
            return False

    async def get_account_password(self, phone: str) -> Optional[str]:
        """Get account password if exists"""
        cursor = self.db.execute(
            'SELECT password FROM accounts WHERE phone = ?',
            (phone,)
        )
        row = cursor.fetchone()
        return row[0] if row else None


class GroupManager:
    def __init__(self, db: Database):
        self.db = db

    async def add_group(self, group_data: dict) -> bool:
        """Add new group to database"""
        try:
            self.db.execute(
                '''INSERT INTO groups (id, title, username, invite_link) 
                VALUES (?, ?, ?, ?)''',
                (
                    group_data['id'],
                    group_data['title'],
                    group_data.get('username'),
                    group_data.get('invite_link')
                ),
                commit=True
            )
            return True
        except sqlite3.IntegrityError:
            return False  # Group already exists

    async def remove_group(self, group_id: int) -> bool:
        """Remove group from database"""
        self.db.execute(
            'DELETE FROM groups WHERE id = ?',
            (group_id,),
            commit=True
        )
        return True

    async def get_group(self, group_id: int) -> Optional[dict]:
        """Get group by ID"""
        cursor = self.db.execute(
            'SELECT id, title, username, invite_link FROM groups WHERE id = ?',
            (group_id,)
        )
        row = cursor.fetchone()
        if row:
            return {
                'id': row[0],
                'title': row[1],
                'username': row[2],
                'invite_link': row[3]
            }
        return None

    async def get_groups_page(self, page: int = 0, per_page: int = 5) -> List[dict]:
        """Get paginated list of groups"""
        offset = page * per_page
        cursor = self.db.execute(
            'SELECT id, title, username, invite_link FROM groups LIMIT ? OFFSET ?',
            (per_page, offset)
        )
        return [
            {
                'id': row[0],
                'title': row[1],
                'username': row[2],
                'invite_link': row[3]
            } for row in cursor.fetchall()
        ]

    async def get_all_groups(self) -> List[dict]:
        """Get all groups"""
        cursor = self.db.execute(
            'SELECT id, title, username, invite_link FROM groups'
        )
        return [
            {
                'id': row[0],
                'title': row[1],
                'username': row[2],
                'invite_link': row[3]
            } for row in cursor.fetchall()
        ]

    async def group_exists(self, group_id: int) -> bool:
        """Check if group exists"""
        cursor = self.db.execute(
            'SELECT 1 FROM groups WHERE id = ?',
            (group_id,)
        )
        return cursor.fetchone() is not None


class MessageManager:
    def __init__(self, db: Database):
        self.db = db

    async def add_message(self, account_phone: str, group_id: int, message_text: str) -> bool:
        """Add sent message to history"""
        try:
            self.db.execute(
                '''INSERT INTO messages (account_phone, group_id, message_text) 
                VALUES (?, ?, ?)''',
                (account_phone, group_id, message_text),
                commit=True
            )
            return True
        except sqlite3.Error:
            return False

    async def get_account_messages(self, phone: str, limit: int = 5) -> List[dict]:
        """Get message history for account"""
        cursor = self.db.execute(
            '''SELECT m.message_text, m.sent_at, g.title 
            FROM messages m
            JOIN groups g ON m.group_id = g.id
            WHERE m.account_phone = ?
            ORDER BY m.sent_at DESC
            LIMIT ?''',
            (phone, limit)
        )
        return [
            {
                'text': row[0],
                'sent_at': row[1],
                'group_title': row[2]
            } for row in cursor.fetchall()
        ]

    async def add_predefined_message(self, text: str) -> bool:
        """Add new predefined message"""
        try:
            self.db.execute(
                'INSERT INTO predefined_messages (text) VALUES (?)',
                (text,),
                commit=True
            )
            return True
        except sqlite3.Error:
            return False

    async def get_predefined_messages(self) -> List[str]:
        """Get all predefined messages"""
        cursor = self.db.execute(
            'SELECT text FROM predefined_messages ORDER BY id'
        )
        return [row[0] for row in cursor.fetchall()]


# Initialize database and managers
db = Database(DB_FILE)
account_manager = AccountManager(db)
group_manager = GroupManager(db)
message_manager = MessageManager(db)


class Form(StatesGroup):
    enter_phone = State()
    enter_code = State()
    enter_password = State()
    add_group = State()
    create_message = State()
    confirm_group_deletion = State()


def create_main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Аккаунты"), KeyboardButton(text="Чаты")],
            [KeyboardButton(text="Управление рассылкой")]
        ],
        resize_keyboard=True
    )


def create_accounts_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить аккаунт"), KeyboardButton(text="Список аккаунтов")],
            [KeyboardButton(text="Создать сообщение"), KeyboardButton(text="Главное меню")]
        ],
        resize_keyboard=True
    )


def create_groups_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить группу"), KeyboardButton(text="Список групп")],
            [KeyboardButton(text="Главное меню")]
        ],
        resize_keyboard=True
    )


def create_pagination_keyboard(page: int = 0, total_pages: int = 1, prefix: str = "groups") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="<<", callback_data=f"{prefix}_first"),
            InlineKeyboardButton(text="<", callback_data=f"{prefix}_prev_{page}"),
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="current"),
            InlineKeyboardButton(text=">", callback_data=f"{prefix}_next_{page}"),
            InlineKeyboardButton(text=">>", callback_data=f"{prefix}_last")
        ]
    ])


def create_message_keyboard(page: int = 0, per_page: int = 5) -> InlineKeyboardMarkup:
    messages = PREDEFINED_MESSAGES
    total_pages = (len(messages) + per_page - 1) // per_page
    start_idx = page * per_page
    end_idx = min(start_idx + per_page, len(messages))

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


def create_group_actions_keyboard(group_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Перейти", callback_data=f"group_join_{group_id}"),
            InlineKeyboardButton(text="Удалить", callback_data=f"group_delete_{group_id}")
        ],
        [InlineKeyboardButton(text="Назад", callback_data="groups_back")]
    ])


@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
async def get_client(phone: str) -> TelegramClient:
    from telethon.network.connection.tcpfull import ConnectionTcpFull

    if phone not in active_clients:
        account = await account_manager.get_account(phone)
        if not account:
            raise ValueError(f"Account {phone} not found")

        if phone not in client_locks:
            client_locks[phone] = asyncio.Lock()

        async with client_locks[phone]:
            session_file = os.path.join(SESSION_DIR, f"{phone}.session")
            client = TelegramClient(
                session_file,
                API_ID,
                API_HASH,
                connection=ConnectionTcpFull,  # Исправлено здесь
                auto_reconnect=True,
                retry_delay=10,
                device_model="PersistentSession",
                system_version="10",
                app_version="10.0",
                lang_code="en",
                system_lang_code="en"
            )

            try:
                await client.connect()
                await client(ResetAuthorizationsRequest())

                if not await client.is_user_authorized():
                    logger.error(f"Account {phone} not authorized!")
                    raise ValueError(f"Account {phone} not authorized")

                active_clients[phone] = client
                await account_manager.update_account(phone, last_used=datetime.now().isoformat())
                await account_manager.add_session(phone, session_file)

            except Exception as e:
                logger.error(f"Error creating client for {phone}: {e}")
                await client.disconnect()
                raise

    client = active_clients[phone]
    await account_manager.update_account(phone, last_used=datetime.now().isoformat())
    return client


async def reconnect_client(phone: str) -> TelegramClient:
    try:
        if phone in active_clients:
            client = active_clients[phone]
            await client.disconnect()
            del active_clients[phone]

        client = await get_client(phone)
        await client.connect()

        if not await client.is_user_authorized():
            logger.error(f"Account {phone} not authorized! Reauthorizing...")
            raise ConnectionError(f"Account {phone} not authorized")

        return client
    except Exception as e:
        logger.error(f"Error reconnecting {phone}: {str(e)}")
        raise


async def send_message_to_group(account: Dict, group: Dict, message_type: int) -> bool:
    try:
        client = await get_client(account["phone"])
        messages = await message_manager.get_predefined_messages()
        message_text = messages[message_type % len(messages)]

        try:
            if group.get('username'):
                entity = await client.get_entity(group['username'])
            elif group.get('invite_link'):
                if group['invite_link'].startswith("https://t.me/+"):
                    hash_part = group['invite_link'].split("+")[1]
                    await client(ImportChatInviteRequest(hash_part))
                    await asyncio.sleep(2)
                entity = await client.get_entity(group['invite_link'])

            await client.send_message(entity, message_text)
            await message_manager.add_message(account["phone"], group['id'], message_text)
            return True
        except Exception as e:
            logger.error(f"Error sending to group {group['title']}: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Client error: {str(e)}")
        return False


async def message_scheduler():
    global scheduler_running
    scheduler_running = True
    message_type = 0
    last_message_time = {}

    while scheduler_running:
        try:
            accounts = await account_manager.get_all_accounts()
            groups = await group_manager.get_all_groups()

            if not accounts or not groups:
                await asyncio.sleep(60)
                continue

            current_time = datetime.now()

            if message_type not in last_message_time or \
                    (current_time - last_message_time.get(message_type, datetime.min)) >= MESSAGE_INTERVAL:

                for i, account in enumerate(accounts):
                    start_time = current_time + i * ACCOUNT_DELAY

                    for j, group in enumerate(groups):
                        send_time = start_time + j * CHAT_DELAY

                        if send_time > current_time:
                            await asyncio.sleep((send_time - current_time).total_seconds())

                        await send_message_to_group(account, group, message_type + i)
                        await asyncio.sleep(1)

                last_message_time[message_type] = current_time
                message_type = (message_type + 1) % MESSAGE_TYPES

            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            await asyncio.sleep(60)


@dp.message(Command("start"))
async def start_command(message: Message):
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
    else:
        await message.answer("⛔ У вас нет доступа к этому боту.")


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
        "Перешлите любое сообщение из группы, которую хотите добавить, "
        "или отправьте публичную ссылку на группу (например, https://t.me/groupname):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )


@dp.message(F.text == "Отмена", Form.add_group)
async def cancel_add_group(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Добавление группы отменено.", reply_markup=create_groups_menu())


async def _prepare_group_data(message: Message) -> Optional[Dict]:
    try:
        if message.forward_from_chat:
            chat = message.forward_from_chat
            if isinstance(chat, (types.Chat, types.Channel)):
                return {
                    'id': chat.id,
                    'title': chat.title,
                    'username': getattr(chat, 'username', None),
                    'invite_link': getattr(chat, 'invite_link', None),
                    'added_at': datetime.now().isoformat()
                }
            return None

        group_input = message.text.strip()

        if group_input.startswith(("https://t.me/+", "https://t.me/joinchat")):
            return {
                'id': abs(hash(group_input)),
                'title': "Группа (по инвайт-ссылке)",
                'username': None,
                'invite_link': group_input,
                'added_at': datetime.now().isoformat()
            }

        if group_input.startswith("https://t.me/"):
            username = group_input.split("/")[-1]
            if not username:
                return None
            return {
                'id': abs(hash(username)),
                'title': f"Группа @{username}",
                'username': username,
                'invite_link': group_input,
                'added_at': datetime.now().isoformat()
            }

        if group_input.startswith("@"):
            username = group_input[1:]
            return {
                'id': abs(hash(username)),
                'title': f"Группа @{username}",
                'username': username,
                'invite_link': f"https://t.me/{username}",
                'added_at': datetime.now().isoformat()
            }

        return None
    except Exception as e:
        logger.error(f"Error preparing group data: {e}")
        return None


@dp.message(Form.add_group)
async def process_add_group(message: Message, state: FSMContext):
    try:
        group_data = await _prepare_group_data(message)
        if not group_data:
            await message.answer("❌ Неверный формат данных. Используйте ссылку или перешлите сообщение.")
            await state.clear()
            return

        if await group_manager.add_group(group_data):
            response = (
                f"✅ Группа добавлена:\n"
                f"Название: {group_data['title']}\n"
                f"ID: {group_data['id']}\n"
                f"Ссылка: {group_data.get('invite_link', 'отсутствует')}"
            )
            await message.answer(response, reply_markup=create_groups_menu())
        else:
            await message.answer("❌ Группа уже существует или произошла ошибка сохранения.")

    except Exception as e:
        logger.error(f"Unexpected error in process_add_group: {e}")
        await message.answer("❌ Произошла непредвиденная ошибка.")
    finally:
        await state.clear()


@dp.message(F.text == "Список групп")
async def list_groups(message: Message):
    groups = await group_manager.get_groups_page()
    all_groups = await group_manager.get_all_groups()
    total_groups = len(all_groups)

    if not groups:
        return await message.answer("Список групп пуст. Добавьте группы через меню.",
                                    reply_markup=create_groups_menu())

    total_pages = (total_groups + 4) // 5
    current_page = 0

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
    all_groups = await group_manager.get_all_groups()
    total_groups = len(all_groups)
    total_pages = (total_groups + 4) // 5

    if "prev" in data:
        new_page = max(0, current_page - 1)
    elif "next" in data:
        new_page = min(total_pages - 1, current_page + 1)
    elif "first" in data:
        new_page = 0
    elif "last" in data:
        new_page = total_pages - 1

    groups = await group_manager.get_groups_page(new_page)
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
    group = await group_manager.get_group(group_id)

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
    group = await group_manager.get_group(group_id)

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
    group = await group_manager.get_group(group_id)

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

    if await group_manager.remove_group(group_id):
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
    from telethon.network.connection.tcpfull import ConnectionTcpFull  # Добавьте этот импорт

    phone = message.text.strip()
    session_file = os.path.join(SESSION_DIR, f"{phone}.session")

    if await account_manager.get_account(phone):
        await message.answer("Этот аккаунт уже добавлен!", reply_markup=create_accounts_menu())
        await state.clear()
        return

    client = TelegramClient(
        session_file,
        API_ID,
        API_HASH,
        connection=ConnectionTcpFull,  # Исправлено здесь
        auto_reconnect=True,
        retry_delay=10
    )

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
    if client:
        await client.disconnect()
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
        await account_manager.add_account(phone, client.session.filename)
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
    if client:
        await client.disconnect()
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
        await account_manager.add_account(phone, client.session.filename, password)
        await message.answer(
            "✅ Аккаунт успешно авторизован с двухфакторной аутентификацией!",
            reply_markup=create_accounts_menu()
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=create_accounts_menu())

    await state.clear()


@dp.message(F.text == "Список аккаунтов")
async def list_accounts(message: Message):
    accounts = await account_manager.get_all_accounts()
    if not accounts:
        await message.answer("📭 Список аккаунтов пуст.", reply_markup=create_accounts_menu())
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{acc['phone']}",
            callback_data=f"account_detail_{acc['phone']}"
        )] for acc in accounts
    ])

    await message.answer("📋 Список аккаунтов:", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("account_detail_"))
async def show_account_detail(callback: CallbackQuery):
    phone = callback.data.split("_")[2]
    account = await account_manager.get_account(phone)
    messages = await message_manager.get_account_messages(phone)

    if not account:
        await callback.answer("Аккаунт не найден!")
        return

    info = (
        f"📱 Аккаунт: {phone}\n"
        f"⏳ Последняя активность: {account['last_used']}\n"
        f"📨 Отправлено сообщений: {len(messages)}\n"
        f"📝 Последние сообщения:\n"
    )

    for msg in messages:
        info += f"  - {msg['sent_at']}: {msg['text']} ({msg['group_title']})\n"

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
    account = await account_manager.get_account(phone)

    if not account:
        await callback.answer("Аккаунт не найден!")
        return

    try:
        if phone in active_clients:
            client = active_clients[phone]
            await client.disconnect()
            del active_clients[phone]

        session_file = account["session_file"]
        if os.path.exists(session_file):
            os.remove(session_file)

        await account_manager.remove_account(phone)
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

    if await message_manager.add_predefined_message(message.text):
        await message.answer(
            f"✅ Сообщение успешно добавлено!",
            reply_markup=create_accounts_menu()
        )
    else:
        await message.answer(
            "❌ Ошибка при сохранении сообщения!",
            reply_markup=create_accounts_menu()
        )

    await state.clear()


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
    accounts = await account_manager.get_all_accounts()
    groups = await group_manager.get_all_groups()
    messages = await message_manager.get_predefined_messages()

    status = (
        f"Статус рассылки:\n"
        f"- Аккаунтов: {len(accounts)}\n"
        f"- Чатов: {len(groups)}\n"
        f"- Сообщений: {len(messages)}\n"
        f"- Работает: {'да' if scheduler_running else 'нет'}"
    )

    await callback.answer(status, show_alert=True)


@dp.callback_query(F.data == "close_scheduler_menu")
async def close_scheduler_menu(callback: CallbackQuery):
    await callback.message.delete()
    await callback.answer()


async def session_keeper():
    """Keep sessions active"""
    while True:
        try:
            accounts = await account_manager.get_all_accounts()
            for account in accounts:
                phone = account["phone"]
                try:
                    client = await get_client(phone)
                    try:
                        me = await client.get_me()
                        logger.info(f"Session active for {phone} ({me.first_name})")
                    except Exception as e:
                        logger.warning(f"Session check failed for {phone}, reconnecting...")
                        await reconnect_client(phone)
                except Exception as e:
                    logger.error(f"Session error for {phone}: {type(e).__name__}: {str(e)}")
                    await asyncio.sleep(60)

            await asyncio.sleep(300)  # Check every 5 minutes

        except Exception as e:
            logger.error(f"Session keeper main loop error: {str(e)}")
            await asyncio.sleep(60)


async def connect_all_accounts():
    """Connect all accounts on startup"""
    accounts = await account_manager.get_all_accounts()
    for account in accounts:
        try:
            await get_client(account["phone"])
        except Exception as e:
            logger.error(f"Error connecting {account['phone']}: {e}")


async def on_startup():
    """Initialize application"""
    await connect_all_accounts()
    asyncio.create_task(session_keeper())


async def on_shutdown():
    """Cleanup before shutdown"""
    global scheduler_running, scheduler_task

    scheduler_running = False
    if scheduler_task:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass

    for client in active_clients.values():
        await client.disconnect()

    db.close()


async def main():
    """Main application entry point"""
    try:
        # Initialize
        await on_startup()

        # Start bot
        await dp.start_polling(bot)

    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
    finally:
        # Cleanup
        await on_shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)