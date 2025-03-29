import asyncio
import logging
import os
import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from telethon import TelegramClient, types
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.errors import (
    FloodWaitError,
    PhoneCodeInvalidError,
    PhoneCodeExpiredError,
    SessionPasswordNeededError,
    AuthKeyUnregisteredError,
    AuthKeyDuplicatedError
)
from telethon.network.connection.tcpfull import ConnectionTcpFull

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
ACCOUNT_DELAY = timedelta(minutes=1)
CHAT_DELAY = timedelta(minutes=3)

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
MESSAGE_FILE = "data/messages.json"
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
os.makedirs(os.path.dirname(MESSAGE_FILE), exist_ok=True)

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

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS mailing_settings (
            id INTEGER PRIMARY KEY DEFAULT 1,
            account_interval INTEGER DEFAULT 30,  -- между аккаунтами (минуты)
            group_interval INTEGER DEFAULT 5,    -- между чатами (секунды)
            message_interval INTEGER DEFAULT 10, -- между сообщениями (минуты)
            current_message_idx INTEGER DEFAULT 0,
            CHECK(id = 1)
        )
        ''')

        cursor.execute('INSERT OR IGNORE INTO mailing_settings DEFAULT VALUES')

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

        # Insert initial settings if not exists
        cursor.execute('INSERT OR IGNORE INTO mailing_settings DEFAULT VALUES')
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

    def is_closed(self) -> bool:
        """Check if database connection is closed"""
        return self.conn is None or self.conn.closed

    def reconnect(self):
        """Reconnect to database if closed"""
        if self.is_closed():
            self._initialize_db()


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
            'SELECT phone, session_file, last_used, password FROM accounts WHERE phone = ?',
            (phone,)
        )
        row = cursor.fetchone()
        if row:
            return {
                'phone': row[0],
                'session_file': row[1],
                'last_used': row[2],
                'password': row[3]
            }
        return None

    async def get_all_accounts(self) -> List[dict]:
        """Get all accounts"""
        cursor = self.db.execute('SELECT phone, session_file, last_used, password FROM accounts')
        return [
            {
                'phone': row[0],
                'session_file': row[1],
                'last_used': row[2],
                'password': row[3]
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
        account = await self.get_account(phone)
        return account.get('password') if account else None


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


class MessageManager:
    def __init__(self, db: Database):
        self.db = db
        self._load_messages()

    def _load_messages(self):
        """Load messages from JSON file"""
        try:
            if os.path.exists(MESSAGE_FILE):
                with open(MESSAGE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('messages', [])
        except Exception as e:
            logger.error(f"Error loading messages: {e}")
        return []

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

    async def get_mailing_settings(self) -> dict:
        """Get current mailing settings"""
        cursor = self.db.execute(
            'SELECT account_interval, group_interval, message_interval, current_message_idx FROM mailing_settings WHERE id = 1'
        )
        row = cursor.fetchone()
        if row:
            return {
                'account_interval': row[0],
                'group_interval': row[1],
                'message_interval': row[2],
                'current_message_idx': row[3]
            }
        return {
            'account_interval': 30,
            'group_interval': 5,
            'message_interval': 10,
            'current_message_idx': 0
        }

    async def update_mailing_settings(self, message_idx: int = None, account_interval: int = None,
                                      group_interval: int = None, message_interval: int = None) -> bool:
        """Update mailing settings"""
        updates = {}
        if message_idx is not None:
            updates['current_message_idx'] = message_idx
        if account_interval is not None:
            updates['account_interval'] = account_interval
        if group_interval is not None:
            updates['group_interval'] = group_interval
        if message_interval is not None:
            updates['message_interval'] = message_interval

        if not updates:
            return False

        set_clause = ', '.join([f"{k} = ?" for k in updates.keys()])
        values = list(updates.values())

        self.db.execute(
            f'UPDATE mailing_settings SET {set_clause} WHERE id = 1',
            tuple(values),
            commit=True
        )
        return True

    async def get_predefined_messages(self) -> List[str]:
        """Get all predefined messages"""
        return self._load_messages()

    async def add_predefined_message(self, text: str) -> bool:
        """Add new predefined message"""
        try:
            messages = self._load_messages()
            messages.append(text)
            with open(MESSAGE_FILE, 'w', encoding='utf-8') as f:
                json.dump({'messages': messages}, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"Error saving messages: {e}")
            return False


class MailingSettingsManager:
    def __init__(self, db: Database):
        self.db = db

    async def get_settings(self) -> dict:
        cursor = self.db.execute('''
        SELECT 
            account_interval,
            group_interval,
            message_interval
        FROM mailing_settings WHERE id = 1
        ''')
        row = cursor.fetchone()
        return {
            'account_interval': row[0],
            'group_interval': row[1],
            'message_interval': row[2]
        }

    async def update_settings(self, account_interval=None, group_interval=None, message_interval=None):
        updates = []
        params = []
        if account_interval is not None:
            updates.append("account_interval = ?")
            params.append(account_interval)
        if group_interval is not None:
            updates.append("group_interval = ?")
            params.append(group_interval)
        if message_interval is not None:
            updates.append("message_interval = ?")
            params.append(message_interval)
        if not updates:
            return False
        query = f"UPDATE mailing_settings SET {', '.join(updates)} WHERE id = 1"
        self.db.execute(query, tuple(params), commit=True)
        return True


# Initialize database and managers
db = Database(DB_FILE)
account_manager = AccountManager(db)
group_manager = GroupManager(db)
message_manager = MessageManager(db)
mailing_settings_manager = MailingSettingsManager(db)


class Form(StatesGroup):
    enter_phone = State()
    enter_code = State()
    enter_password = State()
    add_group = State()
    create_message = State()
    confirm_group_deletion = State()
    select_mailing_message = State()
    set_mailing_interval = State()
    set_interval = State()  # Добавляем новое состояние


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


def create_mailing_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Выбрать сообщение"), KeyboardButton(text="Установить интервалы рассылки")],
            [KeyboardButton(text="Запустить рассылку"), KeyboardButton(text="Остановить рассылку")],
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


def create_message_selection_keyboard(messages: List[str], page: int = 0) -> InlineKeyboardMarkup:
    total_messages = len(messages)
    total_pages = (total_messages + 4) // 5  # 5 сообщений на страницу

    start_idx = page * 5
    end_idx = min(start_idx + 5, total_messages)

    keyboard = []

    # Добавляем кнопки для сообщений на текущей странице
    for i in range(start_idx, end_idx):
        msg_preview = messages[i][:30] + "..." if len(messages[i]) > 30 else messages[i]
        keyboard.append([InlineKeyboardButton(
            text=f"Сообщение {i + 1}: {msg_preview}",
            callback_data=f"select_msg_{i}"
        )])

    # Добавляем кнопки пагинации
    pagination_buttons = []

    if page > 0:
        pagination_buttons.append(InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"msg_page_{page - 1}"
        ))

    if page < total_pages - 1:
        pagination_buttons.append(InlineKeyboardButton(
            text="Вперёд ➡️",
            callback_data=f"msg_page_{page + 1}"
        ))

    if pagination_buttons:
        keyboard.append(pagination_buttons)

    # Добавляем кнопку для добавления нового сообщения
    keyboard.append([InlineKeyboardButton(
        text="➕ Добавить сообщение",
        callback_data="add_new_message"
    )])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def create_group_actions_keyboard(group_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Перейти", callback_data=f"group_join_{group_id}"),
            InlineKeyboardButton(text="Удалить", callback_data=f"group_delete_{group_id}")
        ],
        [InlineKeyboardButton(text="Назад", callback_data="groups_back")]
    ])


async def authorize_client(phone: str, client: TelegramClient) -> bool:
    """Полная процедура авторизации с обработкой всех случаев"""
    try:
        if not client.is_connected():
            await client.connect()

        # Проверяем существующую авторизацию
        if await client.is_user_authorized():
            return True

        # Если есть пароль в базе, пробуем им авторизоваться
        password = await account_manager.get_account_password(phone)
        if password:
            try:
                await client.sign_in(password=password)
                return True
            except Exception as e:
                logger.warning(f"Password auth failed for {phone}: {str(e)}")

        # Запрашиваем код
        sent_code = await client.send_code_request(phone)
        code = await ask_for_code(phone)  # Функция запроса кода у пользователя

        try:
            await client.sign_in(phone, code)
            return True
        except SessionPasswordNeededError:
            password = await ask_for_password(phone)  # Функция запроса пароля
            await client.sign_in(password=password)
            await account_manager.update_account(phone, password=password)
            return True

    except Exception as e:
        logger.error(f"Authorization failed for {phone}: {str(e)}")
        return False

@retry(wait=wait_fixed(5), stop=stop_after_attempt(5))

async def get_client(phone: str) -> TelegramClient:
    """Получение авторизованного клиента"""
    if phone in active_clients:
        client = active_clients[phone]
        if client.is_connected() and await client.is_user_authorized():
            return client
        else:
            await client.disconnect()
            del active_clients[phone]

    session_file = os.path.join(SESSION_DIR, f"{phone}.session")
    client = TelegramClient(
        session=session_file,
        api_id=API_ID,
        api_hash=API_HASH,
        connection=ConnectionTcpFull
    )

    if not await authorize_client(phone, client):
        raise ValueError(f"Authorization failed for {phone}")

    active_clients[phone] = client
    asyncio.create_task(_keep_client_alive(client, phone))
    return client


async def _keep_client_alive(client: TelegramClient, phone: str):
    """Поддержание активного соединения"""
    while phone in active_clients:
        try:
            if not client.is_connected():
                await client.connect()

            if not await client.is_user_authorized():
                if not await authorize_client(phone, client):
                    break

            await client.get_me()  # Проверка активности
            await asyncio.sleep(300)  # Проверка каждые 5 минут

        except Exception as e:
            logger.error(f"Keep-alive error for {phone}: {str(e)}")
            await asyncio.sleep(60)


async def reconnect_client(phone: str) -> TelegramClient:
    try:
        if phone in active_clients:
            client = active_clients[phone]
            # Просто переподключаемся без новой авторизации
            await client.connect()
            return client
        client = await get_client(phone)
        await client.connect()
        return client
    except Exception as e:
        logger.error(f"Error reconnecting {phone}: {str(e)}")
        raise


async def safe_connect(client: TelegramClient):
    """Простое переподключение без лишних параметров"""
    if client.is_connected():
        return

    try:
        await client.disconnect()
    except:
        pass

    await client.connect()


async def get_entity_safe(client: TelegramClient, entity):
    """Safe entity getter with retries"""
    for attempt in range(3):
        try:
            return await client.get_entity(entity)
        except Exception as e:
            logger.warning(f"Entity get attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(2 ** attempt)
    raise Exception("Failed to get entity after multiple attempts")


async def send_message_to_group(account: Dict, group: Dict, message_text: str) -> bool:
    try:
        client = await get_client(account["phone"])

        if not client.is_connected():
            await safe_connect(client)

        entity = await resolve_entity(client, group)
        if not entity:
            return False

        await client.send_message(entity, message_text)
        await message_manager.add_message(account["phone"], group['id'], message_text)
        return True

    except FloodWaitError as e:
        logger.warning(f"Flood wait for {account['phone']}: {e.seconds} sec")
        await asyncio.sleep(e.seconds + 5)
        return False

    except Exception as e:
        logger.error(f"Send message error: {str(e)}")
        return False


async def resolve_entity(client, group):
    """Универсальное разрешение сущности группы"""
    try:
        if group.get('username'):
            return await client.get_entity(group['username'])
        elif group.get('invite_link'):
            if group['invite_link'].startswith("https://t.me/+"):
                hash_part = group['invite_link'].split("+")[1]
                await client(ImportChatInviteRequest(hash_part))
            return await client.get_entity(group['invite_link'])
    except Exception as e:
        logger.error(f"Resolve entity error: {str(e)}")
        return None


async def message_scheduler():
    global scheduler_running
    scheduler_running = True
    while scheduler_running:
        try:
            settings = await mailing_settings_manager.get_settings()
            messages = await message_manager.get_predefined_messages()
            accounts = await account_manager.get_all_accounts()
            groups = await group_manager.get_all_groups()

            if not messages or not accounts or not groups:
                await asyncio.sleep(60)
                continue

            for account_idx, account in enumerate(accounts):
                for group in groups:
                    for message_idx, message_text in enumerate(messages):
                        delay = (
                                account_idx * settings['account_interval'] * 60 +  # Задержка между аккаунтами
                                message_idx * settings['message_interval'] * 60  # Задержка между сообщениями
                        )

                        asyncio.create_task(
                            send_scheduled_message(
                                account=account,
                                group=group,
                                message_text=message_text,
                                delay=delay
                            )
                        )
                        await asyncio.sleep(settings['group_interval'])  # Задержка между группами

            # Ждем завершения цикла перед следующей итерацией
            await asyncio.sleep(max(settings['account_interval'], settings['message_interval']) * 60)

        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            await asyncio.sleep(60)


async def send_scheduled_message(account: Dict, group: Dict, message_text: str, delay: int):
    await asyncio.sleep(delay)
    success = await send_message_to_group(account, group, message_text)
    if not success:
        logger.warning(f"Failed to send message from {account['phone']} to {group['title']}")


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


@dp.message(F.text == "Управление рассылкой")
async def mailing_management(message: Message):
    settings = await mailing_settings_manager.get_settings()
    messages = await message_manager.get_predefined_messages()

    status = "активна" if scheduler_running else "остановлена"
    current_msg_idx = settings.get('current_message_idx', 0)
    current_msg = messages[current_msg_idx][:50] + "..." if messages and current_msg_idx < len(messages) else "нет сообщений"

    await message.answer(
        f"Управление рассылкой:\n"
        f"Статус: {status}\n"
        f"Текущее сообщение: {current_msg}\n"
        f"Интервалы:\n"
        f"- Между аккаунтами: {settings['account_interval']} мин\n"
        f"- Между чатами: {settings['group_interval']} сек\n"
        f"- Между сообщениями: {settings['message_interval']} мин",
        reply_markup=create_mailing_menu()
    )


@dp.message(F.text == "Установить интервалы рассылки")
async def set_intervals(message: Message, state: FSMContext):
    await state.set_state(Form.set_mailing_interval)  # Устанавливаем состояние
    settings = await mailing_settings_manager.get_settings()
    await message.answer(
        f"Текущие настройки интервалов:\n"
        f"⏳ Между аккаунтами: {settings['account_interval']} мин\n"
        f"⏳ Между чатами: {settings['group_interval']} сек\n"
        f"⏳ Между сообщениями: {settings['message_interval']} мин\n\n"
        f"Выберите, какой интервал изменить:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Между аккаунтами", callback_data="set_account_interval"),
                InlineKeyboardButton(text="Между чатами", callback_data="set_group_interval")
            ],
            [
                InlineKeyboardButton(text="Между сообщениями", callback_data="set_message_interval")
            ]
        ])
    )

@dp.message(F.text == "Выбрать сообщение")
async def select_mailing_message(message: Message, state: FSMContext):
    messages = await message_manager.get_predefined_messages()
    if not messages:
        await message.answer("Нет доступных сообщений. Добавьте сообщения сначала.")
        return

    await state.set_state(Form.select_mailing_message)
    await message.answer(
        "Выберите сообщение для рассылки:",
        reply_markup=create_message_selection_keyboard(messages)
    )


@dp.callback_query(F.data.startswith("msg_page_"), Form.select_mailing_message)
async def message_pagination_handler(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split("_")[2])
    messages = await message_manager.get_predefined_messages()

    await callback.message.edit_reply_markup(
        reply_markup=create_message_selection_keyboard(messages, page)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("select_msg_"), Form.select_mailing_message)
async def select_message_handler(callback: CallbackQuery, state: FSMContext):
    message_idx = int(callback.data.split("_")[2])
    messages = await message_manager.get_predefined_messages()

    if 0 <= message_idx < len(messages):
        await message_manager.update_mailing_settings(message_idx=message_idx)
        await callback.answer(f"Выбрано сообщение {message_idx + 1}")
        await callback.message.edit_text(
            f"Выбрано сообщение для рассылки:\n{messages[message_idx][:100]}...",
            reply_markup=None
        )
        await state.clear()
        await mailing_management(callback.message)
    else:
        await callback.answer("Неверный индекс сообщения")


@dp.callback_query(F.data == "add_new_message", Form.select_mailing_message)
async def add_new_message_handler(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "Введите текст нового сообщения:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(Form.create_message)
    await callback.answer()


@dp.message(F.text == "Установить интервалы рассылки")
async def set_intervals(message: Message, state: FSMContext):
    settings = await mailing_settings_manager.get_settings()
    await message.answer(
        f"Текущие настройки интервалов:\n"
        f"⏳ Между аккаунтами: {settings['account_interval']} мин\n"
        f"⏳ Между чатами: {settings['group_interval']} сек\n"
        f"⏳ Между сообщениями: {settings['message_interval']} мин\n\n"
        f"Выберите, какой интервал изменить:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Между аккаунтами", callback_data="set_account_interval"),
                InlineKeyboardButton(text="Между чатами", callback_data="set_group_interval")
            ],
            [
                InlineKeyboardButton(text="Между сообщениями", callback_data="set_message_interval")
            ]
        ])
    )


@dp.callback_query(F.data.startswith("set_"))
async def set_interval_handler(callback: CallbackQuery, state: FSMContext):
    interval_type = callback.data.split("_")[1]
    await state.update_data(interval_type=interval_type)
    units = "минут" if interval_type != "group" else "секунд"
    await callback.message.answer(
        f"Введите новое значение интервала ({units}):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )
    await state.set_state(Form.set_interval)
    await callback.answer()


@dp.message(Form.set_interval)
async def process_set_interval(message: Message, state: FSMContext):
    try:
        value = int(message.text)
        if value < 1:
            raise ValueError("Interval must be at least 1")
        data = await state.get_data()
        interval_type = data.get("interval_type")
        if interval_type == "account":
            await mailing_settings_manager.update_settings(account_interval=value)
        elif interval_type == "group":
            await mailing_settings_manager.update_settings(group_interval=value)
        elif interval_type == "message":
            await mailing_settings_manager.update_settings(message_interval=value)
        await message.answer(
            f"✅ Интервал успешно установлен!",
            reply_markup=create_mailing_menu()
        )
    except ValueError:
        await message.answer(
            "❌ Неверный формат. Введите целое число больше 0.",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="Отмена")]],
                resize_keyboard=True
            )
        )
        return
    await state.clear()


@dp.message(F.text == "Запустить рассылку")
async def start_mailing(message: Message):
    global scheduler_running, scheduler_task

    if scheduler_running:
        await message.answer("Рассылка уже запущена!")
        return

    messages = await message_manager.get_predefined_messages()
    if not messages:
        await message.answer("Нет сообщений для рассылки! Добавьте сообщения сначала.")
        return

    accounts = await account_manager.get_all_accounts()
    if not accounts:
        await message.answer("Нет аккаунтов для рассылки! Добавьте аккаунты сначала.")
        return

    groups = await group_manager.get_all_groups()
    if not groups:
        await message.answer("Нет групп для рассылки! Добавьте группы сначала.")
        return

    scheduler_task = asyncio.create_task(message_scheduler())
    await message.answer("✅ Рассылка запущена!", reply_markup=create_mailing_menu())


@dp.message(F.text == "Остановить рассылку")
async def stop_mailing(message: Message):
    global scheduler_running, scheduler_task

    if not scheduler_running:
        await message.answer("Рассылка не запущена!")
        return

    scheduler_running = False
    if scheduler_task:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass
        scheduler_task = None

    await message.answer("⏸ Рассылка остановлена", reply_markup=create_mailing_menu())


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
    phone = message.text.strip()
    session_file = os.path.join(SESSION_DIR, f"{phone}.session")

    if await account_manager.get_account(phone):
        await message.answer("Этот аккаунт уже добавлен!", reply_markup=create_accounts_menu())
        await state.clear()
        return

    client = TelegramClient(
        session=session_file,
        api_id=API_ID,
        api_hash=API_HASH,
        connection=ConnectionTcpFull,
        timeout=60,  # Общий таймаут для операций
        connection_retries=5,
        retry_delay=10,
        auto_reconnect=True
    )

    # Подключаемся без параметра timeout
    await client.connect()

    try:
        await safe_connect(client)  # Используем безопасное подключение
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
    # Не отключаем клиента!
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


class PersistentClient:
    def __init__(self, phone, session_file):
        self.phone = phone
        self.client = TelegramClient(
            session=session_file,
            api_id=API_ID,
            api_hash=API_HASH,
            connection=ConnectionTcpFull,
            auto_reconnect=True,
            connection_retries=float('inf'),  # Бесконечные попытки
            retry_delay=5,
            timeout=60
        )
        self.lock = asyncio.Lock()
        self._keep_alive_task = None

    async def ensure_connection(self):
        async with self.lock:
            if not self.client.is_connected():
                await self.client.connect()
            if not await self.client.is_user_authorized():
                await self.reauthorize()

    async def reauthorize(self):
        # Ваша логика авторизации
        pass

    async def start_keep_alive(self):
        if not self._keep_alive_task:
            self._keep_alive_task = asyncio.create_task(self._keep_alive_loop())

    async def _keep_alive_loop(self):
        while True:
            try:
                await self.ensure_connection()
                await self.client.get_me()  # Проверка активности
                await asyncio.sleep(300)  # Каждые 5 минут
            except Exception as e:
                logger.error(f"Keep-alive error: {e}")
                await asyncio.sleep(60)

class SessionManager:
    _instance = None
    clients = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def get_client(self, phone):
        if phone not in self.clients:
            session_file = os.path.join(SESSION_DIR, f"{phone}.session")
            client = PersistentClient(phone, session_file)
            await client.ensure_connection()
            await client.start_keep_alive()
            self.clients[phone] = client
        return self.clients[phone].client


async def send_message_to_group(account, group, message_text):
    try:
        manager = SessionManager()
        client = await manager.get_client(account["phone"])

        entity = await resolve_entity(client, group)
        await client.send_message(entity, message_text)
        return True
    except Exception as e:
        logger.error(f"Send error: {e}")
        return False

def handle_exception(loop, context):
    if "exception" in context:
        logger.error(f"Event loop error: {context['exception']}")
    loop.default_exception_handler(context)

loop = asyncio.get_event_loop()
loop.set_exception_handler(handle_exception)

@dp.message(F.text == "Отмена", Form.enter_password)
async def cancel_add_account_password(message: Message, state: FSMContext):
    data = await state.get_data()
    # Не отключаем клиента!
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
            del active_clients[phone]
        await account_manager.remove_account(phone)
        await callback.answer("✅ Аккаунт удален из списка!")
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


async def session_monitor():
    """Enhanced session monitor with persistent connections"""
    while True:
        try:
            accounts = await account_manager.get_all_accounts()
            for account in accounts:
                phone = account["phone"]
                try:
                    if phone not in active_clients:
                        logger.info(f"Connecting account {phone}...")
                        await get_client(phone)
                    else:
                        client = active_clients[phone]
                        try:
                            # Проверяем соединение
                            if not client.is_connected():
                                logger.info(f"Reconnecting {phone}...")
                                await safe_connect(client)

                            # Проверяем авторизацию
                            if not await client.is_user_authorized():
                                logger.warning(f"Session unauthorized for {phone}, reauthorizing...")
                                password = await account_manager.get_account_password(phone)
                                if password:
                                    await client.sign_in(password=password)
                                else:
                                    logger.error(f"No password for {phone}, can't reauthorize")
                                    continue

                            # Простое действие для поддержания соединения
                            await client.get_me()
                            logger.debug(f"Session active for {phone}")

                        except Exception as e:
                            logger.warning(f"Session check failed for {phone}: {e}")
                            await reconnect_client(phone)
                except Exception as e:
                    logger.error(f"Session error for {phone}: {e}")
                    await asyncio.sleep(30)

            await asyncio.sleep(60)  # Проверка каждую минуту

        except Exception as e:
            logger.error(f"Session monitor error: {e}")
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
    global db, account_manager, group_manager, message_manager

    # Initialize database connection
    db = Database(DB_FILE)

    # Initialize managers
    account_manager = AccountManager(db)
    group_manager = GroupManager(db)
    message_manager = MessageManager(db)

    # Connect all accounts
    await connect_all_accounts()

    # Start session monitor
    asyncio.create_task(session_monitor())


async def on_shutdown():
    global scheduler_running, scheduler_task

    scheduler_running = False
    if scheduler_task:
        scheduler_task.cancel()
        try:
            await scheduler_task
        except asyncio.CancelledError:
            pass

    # Больше не выходим из сессий, просто закрываем соединения
    for phone, client in list(active_clients.items()):
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting {phone}: {e}")
        finally:
            del active_clients[phone]

    db.close()

async def main():
    """Main application entry point"""
    try:
        # Initialize
        await on_startup()

        # Start bot
        await dp.start_polling(bot)

    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user")

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