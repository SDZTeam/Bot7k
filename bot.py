import asyncio
import aiofiles
import json
import logging
import os
import shutil
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
ACCOUNT_DELAY = timedelta(minutes=30)
MESSAGE_INTERVAL = timedelta(hours=3)
CHAT_DELAY = timedelta(minutes=15)
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
DATA_DIR = "data/"
os.makedirs(SESSION_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Check write permissions
if not os.access(DATA_DIR, os.W_OK):
    logger.critical("No write permissions to data directory!")
    exit(1)

# Global variables
scheduler_running = False
scheduler_task: Optional[asyncio.Task] = None
active_clients: Dict[str, TelegramClient] = {}
client_locks: Dict[str, asyncio.Lock] = {}


class AccountManager:
    def __init__(self):
        self.accounts: List[Dict] = []
        self.lock = asyncio.Lock()
        self.file_path = os.path.join(DATA_DIR, "accounts.json")
        self.load_task: Optional[asyncio.Task] = None

    async def start(self):
        """Initialize account manager"""
        await self._load_accounts()
        self.load_task = asyncio.create_task(self._auto_save())

    async def _load_accounts(self):
        """Load accounts from file"""
        try:
            if os.path.exists(self.file_path):
                async with aiofiles.open(self.file_path, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    if content.strip():
                        data = json.loads(content)
                        self.accounts = data.get("accounts", [])
        except (json.JSONDecodeError, KeyError):
            logger.error("Invalid accounts.json file. Creating new one.")
            await self._save_accounts()
        except Exception as e:
            logger.error(f"Error loading accounts: {e}")
            self.accounts = []

    async def _auto_save(self):
        """Auto-save accounts every 30 seconds"""
        while True:
            await asyncio.sleep(30)
            await self._save_accounts()

    async def _save_accounts(self):
        """Safe save with backup"""
        try:
            temp_path = self.file_path + ".tmp"
            backup_path = self.file_path + ".bak"

            async with self.lock:
                async with aiofiles.open(temp_path, 'w', encoding='utf-8') as f:
                    await f.write(json.dumps(
                        {"accounts": self.accounts},
                        indent=4,
                        ensure_ascii=False
                    ))

                if os.path.exists(self.file_path):
                    os.replace(self.file_path, backup_path)

                os.replace(temp_path, self.file_path)

                if os.path.exists(backup_path):
                    os.remove(backup_path)

        except Exception as e:
            logger.error(f"Error saving accounts: {e}")

    async def add_account(self, phone: str, session_file: str) -> bool:
        """Add new account"""
        async with self.lock:
            if any(acc["phone"] == phone for acc in self.accounts):
                return False

            self.accounts.append({
                "phone": phone,
                "session_file": session_file,
                "last_used": datetime.now().isoformat(),
                "message_history": [],
                "sessions": []
            })
            await self._save_accounts()
            return True

    async def get_account(self, phone: str) -> Optional[Dict]:
        """Get account by phone"""
        async with self.lock:
            return next((acc for acc in self.accounts if acc["phone"] == phone), None)

    async def get_all_accounts(self) -> List[Dict]:
        """Get all accounts"""
        async with self.lock:
            return self.accounts.copy()

    async def update_account(self, phone: str, **kwargs) -> bool:
        """Update account data"""
        async with self.lock:
            for acc in self.accounts:
                if acc["phone"] == phone:
                    acc.update(kwargs)
                    await self._save_accounts()
                    return True
            return False

    async def remove_account(self, phone: str) -> bool:
        """Remove account"""
        async with self.lock:
            initial_count = len(self.accounts)
            self.accounts = [acc for acc in self.accounts if acc["phone"] != phone]
            if len(self.accounts) != initial_count:
                await self._save_accounts()
                return True
            return False


account_manager = AccountManager()


class GroupManager:
    def __init__(self):
        self.groups: List[Dict] = []
        self.lock = asyncio.Lock()
        self.file_path = os.path.join(DATA_DIR, "groups.json")
        self.backup_path = os.path.join(DATA_DIR, "groups_backup.json")
        self.load_task: Optional[asyncio.Task] = None

    async def start(self):
        """Initialize group manager"""
        await self._load_groups()
        self.load_task = asyncio.create_task(self._auto_save())

    async def _load_groups(self):
        """Load groups from file"""
        try:
            if os.path.exists(self.file_path):
                async with aiofiles.open(self.file_path, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    if content.strip():
                        data = json.loads(content)
                        self.groups = data.get("groups", [])
            else:
                # Попробуем загрузить из резервной копии
                await self._restore_from_backup()
        except (json.JSONDecodeError, KeyError):
            logger.error("Invalid groups.json file. Trying backup...")
            await self._restore_from_backup()
        except Exception as e:
            logger.error(f"Error loading groups: {e}")
            self.groups = []

    async def _restore_from_backup(self) -> bool:
        """Restore groups from backup file"""
        if os.path.exists(self.backup_path):
            try:
                async with aiofiles.open(self.backup_path, 'r', encoding='utf-8') as f:
                    data = json.loads(await f.read())
                    self.groups = data.get("groups", [])
                    await self._save_groups()
                logger.info("Groups restored from backup")
                return True
            except Exception as e:
                logger.error(f"Error restoring from backup: {e}")
        return False

    async def _auto_save(self):
        """Auto-save groups every 30 seconds"""
        while True:
            await asyncio.sleep(30)
            await self._save_groups()

    async def _save_groups(self) -> bool:
        """Improved group saving with backup and retries"""
        for attempt in range(3):  # 3 попытки сохранения
            try:
                temp_path = self.file_path + ".tmp"

                # Создаем временную папку, если не существует
                os.makedirs(os.path.dirname(self.file_path), exist_ok=True)

                # Сохраняем во временный файл
                async with self.lock:
                    data = {"groups": self.groups}
                    async with aiofiles.open(temp_path, 'w', encoding='utf-8') as f:
                        await f.write(json.dumps(data, indent=2, ensure_ascii=False))

                # Проверяем целостность файла
                async with aiofiles.open(temp_path, 'r', encoding='utf-8') as f:
                    content = await f.read()
                    json.loads(content)  # Проверка валидности JSON

                # Создаем резервную копию
                if os.path.exists(self.file_path):
                    shutil.copy2(self.file_path, self.backup_path)

                # Атомарная замена файла
                os.replace(temp_path, self.file_path)

                logger.info("Groups saved successfully")
                return True

            except json.JSONDecodeError as e:
                logger.error(f"JSON error (attempt {attempt + 1}): {e}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception as e:
                logger.error(f"Save error (attempt {attempt + 1}): {e}")

            await asyncio.sleep(1)  # Задержка между попытками

        logger.error("Failed to save groups after 3 attempts")
        return False

    async def add_group(self, group_data: Dict) -> bool:
        """Add new group with full error handling"""
        async with self.lock:
            try:
                # Проверка на дубликаты
                if any(g['id'] == group_data['id'] for g in self.groups):
                    logger.warning(f"Group {group_data['id']} already exists")
                    return False

                self.groups.append(group_data)
                if not await self._save_groups():
                    self.groups.remove(group_data)  # Откат при ошибке
                    return False
                return True
            except Exception as e:
                logger.error(f"Critical error adding group: {e}")
                return False

    async def remove_group(self, group_id: int) -> bool:
        """Remove group"""
        async with self.lock:
            initial_count = len(self.groups)
            self.groups = [g for g in self.groups if g['id'] != group_id]
            if len(self.groups) != initial_count:
                if not await self._save_groups():
                    # Восстанавливаем предыдущее состояние при ошибке сохранения
                    await self._load_groups()
                    return False
                return True
            return False

    async def get_group(self, group_id: int) -> Optional[Dict]:
        """Get group by ID"""
        async with self.lock:
            return next((g for g in self.groups if g['id'] == group_id), None)

    async def get_groups_page(self, page: int = 0, per_page: int = 5) -> List[Dict]:
        """Get paginated groups list"""
        async with self.lock:
            start = page * per_page
            end = start + per_page
            return self.groups[start:end]

    async def get_all_groups(self) -> List[Dict]:
        """Get all groups"""
        async with self.lock:
            return self.groups.copy()


group_manager = GroupManager()


class Form(StatesGroup):
    enter_phone = State()
    enter_code = State()
    enter_password = State()
    add_group = State()
    create_message = State()
    confirm_group_deletion = State()


def create_main_menu() -> ReplyKeyboardMarkup:
    """Create main menu keyboard"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Аккаунты"), KeyboardButton(text="Чаты")],
            [KeyboardButton(text="Управление рассылкой")]
        ],
        resize_keyboard=True
    )


def create_accounts_menu() -> ReplyKeyboardMarkup:
    """Create accounts menu keyboard"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить аккаунт"), KeyboardButton(text="Список аккаунтов")],
            [KeyboardButton(text="Создать сообщение"), KeyboardButton(text="Главное меню")]
        ],
        resize_keyboard=True
    )


def create_groups_menu() -> ReplyKeyboardMarkup:
    """Create groups menu keyboard"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить группу"), KeyboardButton(text="Список групп")],
            [KeyboardButton(text="Главное меню")]
        ],
        resize_keyboard=True
    )


def create_pagination_keyboard(page: int = 0, total_pages: int = 1, prefix: str = "groups") -> InlineKeyboardMarkup:
    """Create pagination keyboard"""
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
    """Create messages selection keyboard"""
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


def create_group_actions_keyboard(group_id: int) -> InlineKeyboardMarkup:
    """Create group actions keyboard"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Перейти", callback_data=f"group_join_{group_id}"),
            InlineKeyboardButton(text="Удалить", callback_data=f"group_delete_{group_id}")
        ],
        [InlineKeyboardButton(text="Назад", callback_data="groups_back")]
    ])


@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
async def get_client(phone: str) -> TelegramClient:
    """Get or create Telegram client for account"""
    if phone not in active_clients:
        account = await account_manager.get_account(phone)
        if not account:
            raise ValueError(f"Account {phone} not found")

        if phone not in client_locks:
            client_locks[phone] = asyncio.Lock()

        async with client_locks[phone]:
            session_file = f"{account['session_file']}_{int(datetime.now().timestamp())}"
            client = TelegramClient(
                session_file,
                API_ID,
                API_HASH,
                connection='tcp_full',
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

                await account_manager.update_account(
                    phone,
                    sessions=account.get("sessions", []) + [{
                        "session_file": session_file,
                        "created_at": datetime.now().isoformat(),
                        "last_used": datetime.now().isoformat()
                    }]
                )

            except Exception as e:
                logger.error(f"Error creating client for {phone}: {e}")
                raise

    client = active_clients[phone]

    account = await account_manager.get_account(phone)
    if account:
        sessions = account.get("sessions", [])
        for session in sessions:
            if session["session_file"] == client.session.filename:
                session["last_used"] = datetime.now().isoformat()
        await account_manager.update_account(phone, sessions=sessions)

    return client


async def reconnect_client(phone: str) -> TelegramClient:
    """Reconnect client for account"""
    try:
        if phone in active_clients:
            del active_clients[phone]

        client = await get_client(phone)
        await client(ResetAuthorizationsRequest())
        return client
    except Exception as e:
        logger.error(f"Error reconnecting {phone}: {e}")
        raise


async def send_message_to_group(account: Dict, group: Dict, message_type: int) -> bool:
    """Send message to group using account"""
    try:
        client = await get_client(account["phone"])
        message_text = PREDEFINED_MESSAGES[message_type % len(PREDEFINED_MESSAGES)]

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

            await account_manager.update_account(
                account["phone"],
                message_history=account.get("message_history", []) + [
                    f"{datetime.now().strftime('%Y-%m-%d %H:%M')} -> {group['title']}: {message_text}"
                ]
            )
            return True
        except Exception as e:
            logger.error(f"Error sending to group {group['title']}: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"Client error: {str(e)}")
        return False


async def message_scheduler():
    """Message scheduler task"""
    global scheduler_running
    scheduler_running = True
    message_type = 0
    last_message_time = {}

    while scheduler_running:
        try:
            accounts = await account_manager.get_all_accounts()
            groups = await group_manager.get_groups_page()

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
    """Start command handler"""
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
    """Main menu handler"""
    await state.clear()
    await message.answer("Главное меню:", reply_markup=create_main_menu())


@dp.message(F.text == "Аккаунты")
async def account_management(message: Message):
    """Accounts management menu"""
    await message.answer("Управление аккаунтами:", reply_markup=create_accounts_menu())


@dp.message(F.text == "Чаты")
async def chats_management(message: Message):
    """Chats management menu"""
    await message.answer("Управление чатами:", reply_markup=create_groups_menu())


@dp.message(F.text == "Добавить группу")
async def add_group_handler(message: Message, state: FSMContext):
    """Add group handler"""
    await state.set_state(Form.add_group)
    await message.answer(
        "Перешлите любое сообщение из группы, которую хотите добавить, "
        "или отправьте публичную ссылку на группу (например, https://t.me/groupname):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Отмена")]],
            resize_keyboard=True
        )
    )


async def _prepare_group_data(message: Message) -> Optional[Dict]:
    """Prepare group data from message"""
    try:
        if message.forward_from_chat:
            chat = message.forward_from_chat
            if not isinstance(chat, (types.Chat, types.Channel)):
                return None

            return {
                'id': chat.id,
                'title': chat.title,
                'username': getattr(chat, 'username', None),
                'invite_link': None,
                'added_at': datetime.now().isoformat()
            }
        else:
            group_input = message.text.strip()
            if group_input.startswith(("https://t.me/+", "https://t.me/joinchat")):
                return {
                    'id': abs(hash(group_input)),
                    'title': "Группа (по инвайт-ссылке)",
                    'username': None,
                    'invite_link': group_input,
                    'added_at': datetime.now().isoformat()
                }
            elif group_input.startswith("https://t.me/"):
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
            elif group_input.startswith("@"):
                username = group_input[1:]
                return {
                    'id': abs(hash(username)),
                    'title': f"Группа @{username}",
                    'username': username,
                    'invite_link': f"https://t.me/{username}",
                    'added_at': datetime.now().isoformat()
                }
    except Exception as e:
        logger.error(f"Error preparing group data: {e}")
    return None


@dp.message(Form.add_group)
async def process_add_group(message: Message, state: FSMContext):
    """Process adding new group"""
    try:
        # Prepare group data
        group_data = await _prepare_group_data(message)
        if not group_data:
            await message.answer("❌ Неверный формат данных. Используйте ссылку или перешлите сообщение.")
            await state.clear()
            return

        # Add group
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
        logger.error(f"Error in process_add_group: {e}", exc_info=True)
        await message.answer("❌ Произошла непредвиденная ошибка. Попробуйте позже.")
    finally:
        await state.clear()


@dp.message(F.text == "Список групп")
async def list_groups(message: Message):
    """List groups handler"""
    groups = await group_manager.get_groups_page()
    total_groups = len(await group_manager.get_all_groups())

    if not groups:
        return await message.answer("Список групп пуст. Добавьте группы через меню.", reply_markup=create_groups_menu())

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
    """Groups pagination handler"""
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
    """Group selection handler"""
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
    """Join group handler"""
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
    """Group deletion handler"""
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
    """Confirm group deletion"""
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
    """Cancel group deletion"""
    await callback.message.answer("Удаление отменено.", reply_markup=create_groups_menu())
    await state.clear()
    await callback.answer()


@dp.callback_query(F.data == "groups_back")
async def back_to_groups_list(callback: CallbackQuery):
    """Back to groups list"""
    await callback.message.delete()
    await list_groups(callback.message)
    await callback.answer()


@dp.message(F.text == "Добавить аккаунт")
async def add_account(message: Message, state: FSMContext):
    """Add account handler"""
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
    """Cancel account addition"""
    await state.clear()
    await message.answer("Добавление аккаунта отменено.", reply_markup=create_accounts_menu())


@dp.message(Form.enter_phone)
async def process_phone(message: Message, state: FSMContext):
    """Process phone number input"""
    phone = message.text.strip()
    session_file = os.path.join(SESSION_DIR, phone)
    if await account_manager.get_account(phone):
        await message.answer("Этот аккаунт уже добавлен!", reply_markup=create_accounts_menu())
        await state.clear()
        return

    client = TelegramClient(
        session_file,
        API_ID,
        API_HASH,
        connection='tcp_full',
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
    """Cancel account addition at code stage"""
    data = await state.get_data()
    client = data.get("client")
    await state.clear()
    await message.answer("Добавление аккаунта отменено.", reply_markup=create_accounts_menu())


@dp.message(Form.enter_code)
async def process_code(message: Message, state: FSMContext):
    """Process verification code"""
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
    """Cancel account addition at password stage"""
    data = await state.get_data()
    client = data.get("client")
    await state.clear()
    await message.answer("Добавление аккаунта отменено.", reply_markup=create_accounts_menu())


@dp.message(Form.enter_password)
async def process_password(message: Message, state: FSMContext):
    """Process 2FA password"""
    password = message.text.strip()
    data = await state.get_data()
    phone = data.get("phone")
    client = data.get("client")

    try:
        await client.sign_in(password=password)
        await account_manager.add_account(phone, client.session.filename)
        await message.answer(
            "✅ Аккаунт успешно авторизован с двухфакторной аутентификацией!",
            reply_markup=create_accounts_menu()
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}", reply_markup=create_accounts_menu())

    await state.clear()


@dp.message(F.text == "Список аккаунтов")
async def list_accounts(message: Message):
    """List accounts handler"""
    accounts = await account_manager.get_all_accounts()
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
    """Show account details"""
    phone = callback.data.split("_")[2]
    account = await account_manager.get_account(phone)

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
    """Back to accounts list"""
    await callback.message.delete()
    await list_accounts(callback.message)
    await callback.answer()


@dp.callback_query(F.data.startswith("delete_account_"))
async def delete_account_handler(callback: CallbackQuery):
    """Delete account handler"""
    phone = callback.data.split("_")[2]
    account = await account_manager.get_account(phone)

    if not account:
        await callback.answer("Аккаунт не найден!")
        return

    try:
        if phone in active_clients:
            del active_clients[phone]

        if os.path.exists(account["session_file"]):
            os.remove(account["session_file"])

        await account_manager.remove_account(phone)
        await callback.answer("✅ Аккаунт удален!")
        await back_to_accounts_list(callback)
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}")


@dp.message(F.text == "Создать сообщение")
async def create_message_handler(message: Message, state: FSMContext):
    """Create message handler"""
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
    """Cancel message creation"""
    await state.clear()
    await message.answer("Создание сообщения отменено.", reply_markup=create_accounts_menu())


@dp.message(Form.create_message)
async def process_create_message(message: Message, state: FSMContext):
    """Process message creation"""
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


@dp.message(F.text == "Управление рассылкой")
async def manage_scheduler(message: Message):
    """Manage scheduler handler"""
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
    """Toggle scheduler handler"""
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
    """Scheduler status handler"""
    accounts = await account_manager.get_all_accounts()
    groups = await group_manager.get_all_groups()

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
    """Close scheduler menu"""
    await callback.message.delete()
    await callback.answer()


async def session_keeper():
    """Maintain active sessions"""
    while True:
        try:
            accounts = await account_manager.get_all_accounts()
            for account in accounts:
                phone = account["phone"]
                try:
                    client = await get_client(phone)
                    await client.get_me()
                except Exception as e:
                    logger.error(f"Session error for {phone}: {e}")
                    try:
                        await reconnect_client(phone)
                    except Exception as e:
                        logger.error(f"Failed to reconnect {phone}: {e}")

            await asyncio.sleep(300)

        except Exception as e:
            logger.error(f"Session keeper error: {e}")
            await asyncio.sleep(60)


async def keep_alive():
    """Keep connections alive"""
    while True:
        await asyncio.sleep(300)
        for phone in list(active_clients.keys()):
            try:
                client = active_clients[phone]
                await client.get_me()
                logger.info(f"Keep-alive for {phone}")
            except Exception as e:
                logger.error(f"Keep-alive error {phone}: {e}")
                try:
                    await reconnect_client(phone)
                except:
                    pass


async def connect_all_accounts():
    """Connect all accounts on startup"""
    accounts = await account_manager.get_all_accounts()
    for account in accounts:
        try:
            await get_client(account["phone"])
        except Exception as e:
            logger.error(f"Error connecting {account['phone']}: {e}")


async def main():
    """Main application entry point"""
    try:
        # Initialize managers
        await account_manager.start()
        await group_manager.start()

        # Connect accounts
        await connect_all_accounts()

        # Start background tasks
        asyncio.create_task(session_keeper())
        asyncio.create_task(keep_alive())

        # Start bot
        await dp.start_polling(bot)

    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
    finally:
        # Cleanup
        for client in active_clients.values():
            await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)