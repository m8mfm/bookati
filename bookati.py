import os
import logging
import asyncio
import aiosqlite
from datetime import datetime
from dotenv import load_dotenv 
import sqlite3  
import tempfile 


from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
    InputMediaDocument, FSInputFile, Update
)
from aiogram.filters import CommandStart, StateFilter, and_f, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.enums import ChatMemberStatus
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter

load_dotenv()


BOT_TOKEN = os.getenv("TOKEN")

ADMIN_IDS = [int(admin_id.strip()) for admin_id in os.getenv("ADMIN_IDS", "").split(',') if admin_id.strip()]
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME") 
BOOKS_CHANNEL = os.getenv("CHANNEL_ID")


REQUIRED_CHANNELS = [f"@{CHANNEL_USERNAME}"] if CHANNEL_USERNAME else []


LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"bot_errors_{datetime.now().strftime('%Y-%m-%d')}.log")),
        logging.StreamHandler() 
    ]
)
logger = logging.getLogger(__name__)


info_logger = logging.getLogger('info_logger')
info_logger.setLevel(logging.INFO)
info_logger.addHandler(logging.FileHandler(os.path.join(LOG_DIR, f"bot_info_{datetime.now().strftime('%Y-%m-%d')}.log")))
info_logger.addHandler(logging.StreamHandler()) 

def log_error(message: str, exc_info: bool = False, context: dict = None):
    """
    Logs an error message with optional exception information and additional context.
    :param message: The primary error message.
    :param exc_info: If True, includes current exception information in the log.
    :param context: A dictionary of additional key-value pairs for context (e.g., user_id, book_id).
    """
    full_message = message
    if context:
        full_message += f" | Context: {context}"
    logger.error(full_message, exc_info=exc_info)

def log_info(message: str, context: dict = None):
    """
    Logs an informational message with optional additional context.
    :param message: The primary info message.
    :param context: A dictionary of additional key-value pairs for context.
    """
    full_message = message
    if context:
        full_message += f" | Context: {context}"
    info_logger.info(full_message)


if not BOT_TOKEN:
    log_error("BOT_TOKEN environment variable is not set. The bot cannot start.")
    exit("BOT_TOKEN is missing. Please set it in your .env file.")


if not ADMIN_IDS:
    log_info("ADMIN_IDS are not set. Admin features will not be available.")


if not REQUIRED_CHANNELS:
    log_info("REQUIRED_CHANNELS are not set. Subscription check will be skipped.")


DB_NAME = 'library.db'


async def add_is_banned_column(db_path: str = "library.db"):
    """
    يضيف عمود is_banned إلى جدول المستخدمين إذا لم يكن موجودًا بالفعل.
    """
    try:
        async with aiosqlite.connect(db_path) as db:

            cursor = await db.execute(f"PRAGMA table_info(users);")
            columns = await cursor.fetchall()
            column_names = [col[1] for col in columns]

            if "is_banned" not in column_names:
                await db.execute("ALTER TABLE users ADD COLUMN is_banned INTEGER DEFAULT 0;")
                await db.commit()
                print("تمت إضافة العمود 'is_banned' إلى جدول 'users' بنجاح.")
            else:
                print("العمود 'is_banned' موجود بالفعل في جدول 'users'.")
    except Exception as e:
        print(f"حدث خطأ أثناء إضافة العمود: {e}")

async def init_db():
    """
    Initializes the SQLite database and creates necessary tables if they don't already exist.
    Includes 'is_banned' column in 'users' table and 'ON DELETE CASCADE' for 'user_books'.
    """
    try:
        async with aiosqlite.connect(DB_NAME) as db:

            await db.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    is_admin INTEGER DEFAULT 0,
                    is_banned INTEGER DEFAULT 0 -- 0 for not banned, 1 for banned
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    author TEXT NOT NULL,
                    category TEXT,
                    file_id TEXT, -- Telegram File ID for the book PDF
                    description TEXT,
                    upload_date TEXT,
                    average_rating REAL DEFAULT 0.0,
                    rating_count INTEGER DEFAULT 0
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS user_books (
                    user_id INTEGER,
                    book_id INTEGER,
                    is_read INTEGER DEFAULT 0, -- 0 for not read, 1 for read
                    rating INTEGER DEFAULT 0, -- 1-5 rating
                    PRIMARY KEY (user_id, book_id),
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE -- Automatically delete related entries if book is deleted
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS book_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    requested_book_title TEXT NOT NULL,
                    has_file INTEGER DEFAULT 0, -- 0 if user doesn't have file, 1 if they provided one
                    file_id TEXT, -- Optional Telegram file ID if user sent it
                    status TEXT DEFAULT 'pending', -- 'pending', 'approved', 'rejected'
                    request_date TEXT
                )
            ''')
            await db.commit() 
        log_info("Database initialized successfully.")
    except Exception as e:
        log_error(f"Error initializing database: {e}", exc_info=True)

async def db_query(query: str, params: tuple = (), fetchone: bool = False, fetchall: bool = False, commit: bool = False):
    """
    A centralized asynchronous helper function for executing SQLite database queries.
    Ensures foreign key constraints are enforced and provides robust error handling.
    :param query: The SQL query string to execute.
    :param params: A tuple of parameters to safely bind to the query (prevents SQL injection).
    :param fetchone: If True, fetches and returns a single row result.
    :param fetchall: If True, fetches and returns all rows as a list of tuples.
    :param commit: If True, commits the transaction after execution (for INSERT, UPDATE, DELETE).
    :return: The query result (single row, list of rows, or None) or None on error.
    """
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("PRAGMA foreign_keys = ON") 
            cursor = await db.execute(query, params)
            if commit:
                await db.commit()
            if fetchone:
                return await cursor.fetchone()
            if fetchall:
                return await cursor.fetchall()
            return None 
    except aiosqlite.Error as e:
        log_error(f"Database error: {e} | Query: {query} | Params: {params}", exc_info=True)
        return None
    except Exception as e:
        log_error(f"Unexpected error in db_query: {e} | Query: {query} | Params: {params}", exc_info=True)
        return None

async def create_db_backup(source_db_path: str, backup_file_path: str):
    """
    Creates a consistent backup of the SQLite database by performing the copy
    operation in a separate thread to avoid blocking the main event loop.
    :param source_db_path: The file path of the source database to back up.
    :param backup_file_path: The destination file path for the backup.
    :return: True if the backup was successful, False otherwise.
    """
    try:
        def backup_sync_thread():
            """Synchronous backup operation, designed to run in a dedicated thread."""
            source_conn_sync = None
            dest_conn_sync = None
            try:

                source_conn_sync = sqlite3.connect(source_db_path)
                dest_conn_sync = sqlite3.connect(backup_file_path)

                with source_conn_sync:
                    source_conn_sync.backup(dest_conn_sync)
                log_info(f"Database backup created successfully by sync thread at: {backup_file_path}")
                return True
            except Exception as e:
                log_error(f"Failed to create database backup in sync thread: {e}", exc_info=True, context={"source_db": source_db_path, "backup_path": backup_file_path})
                return False
            finally:
             
                if dest_conn_sync:
                    dest_conn_sync.close()
                if source_conn_sync:
                    source_conn_sync.close()


        result = await asyncio.to_thread(backup_sync_thread)
        return result
    except Exception as e:
        log_error(f"Failed to initiate database backup (asyncio.to_thread issue): {e}", exc_info=True, context={"source_db": source_db_path, "backup_path": backup_file_path})
        return False

async def is_admin(user_id: int) -> bool:
    """
    Checks if a given user ID belongs to an administrator.
    :param user_id: The Telegram user ID to check.
    :return: True if the user is an admin, False otherwise.
    """
    return user_id in ADMIN_IDS

async def is_user_banned(user_id: int) -> bool:
    """
    Checks the database to determine if a user is currently banned.
    :param user_id: The Telegram user ID to check.
    :return: True if the user is banned, False if not banned or an error occurs.
    """
    try:
        user_data = await db_query("SELECT is_banned FROM users WHERE id = ?", (user_id,), fetchone=True)

        return user_data is not None and user_data[0] == 1
    except Exception as e:
        log_error(f"Error checking ban status for user {user_id}: {e}", exc_info=True)
        return False 

async def check_if_banned_and_respond(message_or_callback: Message | CallbackQuery, bot: Bot) -> bool:
    """
    A wrapper function to check if a user is banned. If banned, sends a message and returns True.
    Otherwise, returns False. This should be called at the start of most handlers.
    :param message_or_callback: The Aiogram Message or CallbackQuery object.
    :param bot: The Aiogram Bot instance.
    :return: True if the user is banned and a message was sent, False otherwise.
    """
    user_id = message_or_callback.from_user.id
    if await is_user_banned(user_id):
        try:
            if isinstance(message_or_callback, Message):
                await message_or_callback.answer("🚫 عذراً، لا يمكنك استخدام البوت حالياً. لقد تم حظرك.")
            elif isinstance(message_or_callback, CallbackQuery):
                await message_or_callback.answer("🚫 عذراً، لا يمكنك استخدام البوت حالياً. لقد تم حظرك.", show_alert=True)
                await message_or_callback.message.edit_text("🚫 عذراً، لا يمكنك استخدام البوت حالياً. لقد تم حظرك.")
            log_info(f"Banned user {user_id} attempted to interact with the bot.", context={"user_id": user_id})
        except Exception as e:
            log_error(f"Error sending ban message to user {user_id}: {e}", exc_info=True)
        return True
    return False

async def register_user(message: Message):
    """
    Registers a new user in the database or updates an existing user's information.
    Preserves the 'is_banned' status if the user already exists.
    :param message: The Aiogram Message object from which user data is extracted.
    """
    user = message.from_user
    user_id = user.id
    username = user.username if user.username else ""
    first_name = user.first_name if user.first_name else ""
    last_name = user.last_name if user.last_name else ""

    try:

        existing_user = await db_query("SELECT id FROM users WHERE id = ?", (user_id,), fetchone=True)
        if existing_user:

            await db_query(
                "UPDATE users SET username = ?, first_name = ?, last_name = ? WHERE id = ?",
                (username, first_name, last_name, user_id),
                commit=True
            )
            log_info(f"User {user_id} information updated.", context={"user_id": user_id})
        else:

            await db_query(
                "INSERT INTO users (id, username, first_name, last_name, is_admin, is_banned) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, username, first_name, last_name, 0, 0),
                commit=True
            )
            log_info(f"New user {user_id} registered.", context={"user_id": user_id})
    except Exception as e:
        log_error(f"Error registering/updating user {user_id}: {e}", exc_info=True)

async def check_user_subscription(user_id: int, bot: Bot) -> bool:
    """
    Verifies if the user is subscribed to all channels listed in REQUIRED_CHANNELS.
    If REQUIRED_CHANNELS is empty, it always returns True.
    :param user_id: The Telegram user ID.
    :param bot: The Aiogram Bot instance.
    :return: True if subscribed to all required channels or no channels are required, False otherwise.
    """
    if not REQUIRED_CHANNELS:
        log_info("No required channels configured. Subscription check skipped.")
        return True 

    for channel in REQUIRED_CHANNELS:
        try:
            member = await bot.get_chat_member(chat_id=channel, user_id=user_id)
         
            if member.status not in [ChatMemberStatus.MEMBER, ChatMemberStatus.CREATOR, ChatMemberStatus.ADMINISTRATOR]:
                return False
        except Exception as e:
            log_error(f"Error checking subscription for user {user_id} in {channel}: {e}", exc_info=True)
            return False
    return True

async def send_subscription_required_message(chat_id: int, bot: Bot):
    """
    Sends a message to the user prompting them to subscribe to the required channels.
    :param chat_id: The chat ID where the message should be sent.
    :param bot: The Aiogram Bot instance.
    """
    if not REQUIRED_CHANNELS:
        log_info(f"Attempted to send subscription message to {chat_id}, but no required channels are configured.")
        return

    buttons = []
    for channel in REQUIRED_CHANNELS:

        channel_link = channel.replace('@', 'https://t.me/')
        buttons.append([InlineKeyboardButton(text=f"اشترك في {channel}", url=channel_link)])
    
    buttons.append([InlineKeyboardButton(text="✅ لقد اشتركت", callback_data="check_subscription")])
    
    try:
        await bot.send_message(
            chat_id=chat_id,
            text="📢 يجب الاشتراك في القنوات التالية لاستخدام البوت:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
        )
    except Exception as e:
        log_error(f"Error sending subscription required message to {chat_id}: {e}", exc_info=True)

async def send_book_notification(bot: Bot, book_data: dict):
    """
    Sends a notification about a newly added book to the configured BOOKS_CHANNEL.
    Includes an inline button that deep-links back to the bot to get the book.
    :param bot: The Aiogram Bot instance.
    :param book_data: A dictionary containing 'id', 'title', and 'author' of the new book.
    """
    try:
        if not BOOKS_CHANNEL:
            log_info("BOOKS_CHANNEL is not set. Skipping new book notification.")
            return


        bot_info = await bot.get_me()
        bot_username = bot_info.username

        text = f"📢 كتاب جديد: {book_data['title']}\n"
        text += f"✍️ الكاتب: {book_data['author']}"
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text="📥 احصل على الكتاب",
                url=f"https://t.me/{bot_username}?start=send_{book_data['id']}" 
            )]
        ])
        
        await bot.send_message(
            chat_id=BOOKS_CHANNEL,
            text=text,
            reply_markup=keyboard
        )
        log_info(f"New book notification sent for book ID {book_data['id']}.", context={"book_id": book_data['id']})
    except Exception as e:
        log_error(f"Error sending new book notification for book ID {book_data.get('id', 'N/A')}: {e}", exc_info=True)

async def send_book_details_and_download_option(message: Message, book_id: int, bot: Bot):
    """
    Sends detailed information about a book to the user, along with a download button.
    Used for deep links (e.g., from channel notifications) and search results.
    :param message: The Aiogram Message object to reply to.
    :param book_id: The ID of the book to display.
    :param bot: The Aiogram Bot instance.
    """
    user_id = message.from_user.id
    if await check_if_banned_and_respond(message, bot):
        return


    if not await check_user_subscription(user_id, bot):
        await send_subscription_required_message(message.chat.id, bot)
        return

    try:
        book = await db_query(
            "SELECT id, title, author, category, description, file_id FROM books WHERE id = ?",
            (book_id,),
            fetchone=True
        )

        if not book:
            await message.answer("⚠️ الكتاب غير متوفر حالياً أو تم حذفه.")
            log_info(f"User {user_id} requested details for non-existent book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})
            return

        book_id, title, author, category, description, file_id = book
        
        message_text = (
            f"📚 **العنوان:** {title}\n"
            f"✍️ **الكاتب:** {author}\n"
            f"🏷️ **التصنيف:** {category if category else 'غير محدد'}\n"
            f"📝 **الوصف:** {description if description else 'لا يوجد وصف.'}\n"
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬇️ الحصول على الكتاب", callback_data=f"get_book_{book_id}")],
            [InlineKeyboardButton(text="🔙 العودة للقائمة الرئيسية", callback_data="main_menu")]
        ])
        
        await message.answer(
            text=message_text,
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        log_info(f"User {user_id} viewed details for book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})

    except Exception as e:
        await message.answer("عذراً، حدث خطأ أثناء جلب تفاصيل الكتاب.")
        log_error(f"Error sending book details for book ID {book_id} to user {user_id}: {e}", exc_info=True)


def get_main_keyboard(is_admin_user: bool = False):
    """
    Generates the main inline keyboard for users, with an additional button for admins.
    :param is_admin_user: Boolean indicating if the current user is an admin.
    :return: InlineKeyboardMarkup object.
    """
    buttons = [
        [InlineKeyboardButton(text="📚 تصفح المكتبة", callback_data="browse_library")],
        [InlineKeyboardButton(text="🔍 بحث عن كتاب", callback_data="search_book_start")],
        [InlineKeyboardButton(text="🎲 كتاب عشوائي", callback_data="random_book")],
        [InlineKeyboardButton(text="⭐️ الكتب الأعلى تقييماً", callback_data="top_rated_books")],
        [InlineKeyboardButton(text="📥 كتبي التي نزلتها", callback_data="my_downloads")],
        [InlineKeyboardButton(text="💡 اقترح كتاباً", callback_data="suggest_book_start")]
    ]
    if is_admin_user:
        buttons.append([InlineKeyboardButton(text="⚙️ لوحة التحكم (للمدير)", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_admin_keyboard():
    """
    Generates the inline keyboard for the admin panel.
    :return: InlineKeyboardMarkup object.
    """
    buttons = [
        [InlineKeyboardButton(text="➕ (كامل) إضافة كتاب", callback_data="admin_add_book_start")],
        [InlineKeyboardButton(text="📥 إضافة كتاب (سريع)", callback_data="admin_add_book_start_simple")],
        [InlineKeyboardButton(text="🗑️ حذف كتاب", callback_data="admin_delete_book_start")],
        [InlineKeyboardButton(text="✏️ تعديل كتاب", callback_data="admin_edit_book_start")],
        [InlineKeyboardButton(text="🚫 حظر مستخدم", callback_data="admin_ban_user_start")],
        [InlineKeyboardButton(text="📄 طلبات الكتب", callback_data="admin_view_requests_start")],
        [InlineKeyboardButton(text="💾 نسخ احتياطي للقاعدة", callback_data="admin_backup_db")],
        [InlineKeyboardButton(text="🔙 العودة للقائمة الرئيسية", callback_data="main_menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_book_details_keyboard(book_id: int):
    """
    Generates the inline keyboard displayed with book details, including a download button.
    :param book_id: The ID of the book.
    :return: InlineKeyboardMarkup object.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬇️ الحصول على الكتاب", callback_data=f"get_book_{book_id}")],
        [InlineKeyboardButton(text="🔙 العودة للقائمة الرئيسية", callback_data="main_menu")]
    ])

def get_book_rating_keyboard(book_id: int, current_rating: int = 0, is_read: int = 0):
    """
    Generates the inline keyboard for managing user's downloaded books (read status and rating).
    :param book_id: The ID of the book.
    :param current_rating: The user's current rating for the book (0-5).
    :param is_read: The user's current read status (0 or 1).
    :return: InlineKeyboardMarkup object.
    """
    read_status_text = "✅ قرأته" if is_read else "❌ لم أقرأه"
 
    rating_buttons = []
    for i in range(1, 6):
        star = "⭐" if i <= current_rating else "☆" 
        rating_buttons.append(InlineKeyboardButton(text=f"{i}{star}", callback_data=f"rate_book_{book_id}_{i}"))

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=read_status_text, callback_data=f"toggle_read_status_{book_id}")],
        rating_buttons,
        [InlineKeyboardButton(text="🔙 العودة لكتبي", callback_data="my_downloads")]
    ])

def get_request_action_keyboard(request_id: int):
    """
    Generates the inline keyboard for admin actions on a book request (approve/reject).
    :param request_id: The ID of the book request.
    :return: InlineKeyboardMarkup object.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ موافقة", callback_data=f"approve_request_{request_id}"),
            InlineKeyboardButton(text="❌ رفض", callback_data=f"reject_request_{request_id}")
        ],
        [InlineKeyboardButton(text="🔙 العودة للطلبات", callback_data="admin_view_requests_start")]
    ])

def get_cancel_keyboard():
    """
    Generates a simple inline keyboard with an 'Cancel' button.
    :return: InlineKeyboardMarkup object.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="إلغاء", callback_data="cancel_conversation")]
    ])


class UserStates(StatesGroup):
    """States for regular user interactions."""
    SEARCH_QUERY = State()
    SUGGEST_BOOK_TITLE = State()
    SUGGEST_BOOK_FILE_CONFIRM = State() 
    SUGGEST_BOOK_FILE_UPLOAD = State() 
    VIEWING_REQUEST_DETAILS = State() 

class AdminStates(StatesGroup):
    """States for administrator interactions."""
 
    ADD_BOOK_TITLE = State()
    ADD_BOOK_AUTHOR = State()
    ADD_BOOK_CATEGORY = State()
    ADD_BOOK_DESCRIPTION = State()
    ADD_BOOK_FILE = State()

   
    ADD_BOOK_SIMPLE_TITLE = State()
    ADD_BOOK_SIMPLE_AUTHOR = State()
    ADD_BOOK_SIMPLE_FILE = State()


    DELETE_BOOK_INPUT = State() 
    DELETE_BOOK_CONFIRM = State() 


    EDIT_BOOK_INPUT = State()
    EDIT_BOOK_SELECT_FIELD = State() 
    EDIT_BOOK_NEW_VALUE = State() 


    BAN_USER_INPUT_ID = State()


async def start_command(message: Message, state: FSMContext, bot: Bot) -> None:
    """
    Handles the /start command. Registers the user, checks subscription, and displays the main menu.
    Also handles deep links for direct book access from channel notifications.
    :param message: The Aiogram Message object.
    :param state: The FSMContext for managing conversation state.
    :param bot: The Aiogram Bot instance.
    """
    user_id = message.from_user.id
    log_info(f"User {user_id} sent /start command.", context={"user_id": user_id})


    if await check_if_banned_and_respond(message, bot):
        return

    if len(message.text.split()) > 1 and message.text.split()[1].startswith('send_'):
        try:
            book_id = int(message.text.split()[1].split('_')[1])
            await send_book_details_and_download_option(message, book_id, bot)
            await state.clear() 
            return
        except ValueError:
            log_error(f"Invalid book ID in deep link: {message.text}", context={"user_id": user_id, "message_text": message.text})
            await message.answer("⚠️ رابط الكتاب غير صالح.")
        except Exception as e:
            log_error(f"Error handling deep link for user {user_id}: {e}", exc_info=True)
            await message.answer("عذراً، حدث خطأ أثناء معالجة طلبك من الرابط.")


    await register_user(message)


    if not await check_user_subscription(user_id, bot):
        await send_subscription_required_message(message.chat.id, bot)
        return


    is_admin_user = await is_admin(user_id)
    keyboard = get_main_keyboard(is_admin_user)
    await message.answer(
        "أهلاً بك في مكتبة الكتب! 📚\nاختر من القائمة:",
        reply_markup=keyboard
    )
    await state.clear() 

async def main_menu_callback(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Handles the callback to return to the main menu.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext for managing conversation state.
    """
    user_id = callback.from_user.id
    log_info(f"User {user_id} returned to main menu.", context={"user_id": user_id})


    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    is_admin_user = await is_admin(user_id)
    keyboard = get_main_keyboard(is_admin_user)
    try:
        await callback.message.edit_text(
            "أهلاً بك في مكتبة الكتب! 📚\nاختر من القائمة:",
            reply_markup=keyboard
        )
    except Exception as e:
        log_error(f"Error editing message to main menu for user {user_id}: {e}", exc_info=True)
        await callback.message.answer("أهلاً بك في مكتبة الكتب! 📚\nاختر من القائمة:", reply_markup=keyboard)
    await state.clear() 

async def error_handler(update: Update, exception: Exception):
    """
    Centralized error handler for the bot. Logs the error and sends a user-friendly message.
    :param update: The Aiogram Update object that caused the error.
    :param exception: The Exception object.
    """
    try:
        # Get user info safely
        user_id = update.effective_user.id if update.effective_user else 'N/A'
        chat_id = update.effective_chat.id if update.effective_chat else 'N/A'
        
        # Special handling for TelegramBadRequest (like expired callback queries)
        if isinstance(exception, TelegramBadRequest):
            if "query is too old" in str(exception):
                logging.warning(f"Expired callback query from user {user_id}")
                return True  # This prevents propagation of the error
            
        # Log the error
        logging.error(
            f"Update {update.update_id} from user {user_id} caused error: {exception}",
            exc_info=True,
            extra={"user_id": user_id, "error_type": type(exception).__name__}
        )

        # Send error message only for certain types of errors
        if update.effective_chat and not isinstance(exception, (TelegramBadRequest, TelegramRetryAfter)):
            try:
                await Bot(BOT_TOKEN).send_message(
                    chat_id=chat_id,
                    text="عذراً، حدث خطأ ما! 😅 يرجى المحاولة مرة أخرى أو التواصل مع المسؤول."
                )
            except Exception as e:
                logging.error(f"Failed to send error message to user {user_id}: {e}", exc_info=True)
        
        return True  # This prevents propagation to the default error handler
        
    except Exception as e:
        logging.critical(f"Error in error_handler: {e}", exc_info=True)
        return False
        
    except Exception as e:
        logging.critical(f"Error in error_handler: {e}", exc_info=True)
        return False      

async def cancel_conversation(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Cancels any ongoing conversation state and returns the user to the main menu.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext for managing conversation state.
    """
    user_id = callback.from_user.id
    log_info(f"User {user_id} cancelled a conversation.", context={"user_id": user_id})


    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer("تم إلغاء العملية.") 
    await state.clear() 
    is_admin_user = await is_admin(user_id)
    try:
        await callback.message.edit_text("تم إلغاء العملية.", reply_markup=get_main_keyboard(is_admin_user))
    except Exception as e:
        log_error(f"Error editing message after cancel for user {user_id}: {e}", exc_info=True)
        await callback.message.answer("تم إلغاء العملية.", reply_markup=get_main_keyboard(is_admin_user))

async def check_subscription_callback(callback: CallbackQuery, bot: Bot):
    """
    Handles the 'check_subscription' callback, re-verifying user subscription status.
    :param callback: The Aiogram CallbackQuery object.
    :param bot: The Aiogram Bot instance.
    """
    user_id = callback.from_user.id
    log_info(f"User {user_id} clicked 'Check Subscription'.", context={"user_id": user_id})


    if await check_if_banned_and_respond(callback, bot):
        return

    await callback.answer("🔍 جاري التحقق من اشتراكك...") 

    if await check_user_subscription(user_id, bot):
        is_admin_user = await is_admin(user_id)
        keyboard = get_main_keyboard(is_admin_user)
        try:
            await callback.message.edit_text(
                "أهلاً بك في مكتبة الكتب! 📚\nاختر من القائمة:",
                reply_markup=keyboard
            )
        except Exception as e:
            log_error(f"Error editing message after subscription check for user {user_id}: {e}", exc_info=True)
            await callback.message.answer("أهلاً بك في مكتبة الكتب! 📚\nاختر من القائمة:", reply_markup=keyboard)
    else:

        await send_subscription_required_message(callback.message.chat.id, bot)


async def browse_library(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Handles browsing the library with pagination.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext for managing pagination state.
    """
    user_id = callback.from_user.id
    log_info(f"User {user_id} initiated library browsing.", context={"user_id": user_id})


    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()

    current_page = (await state.get_data()).get('browse_page', 0)
    books_per_page = 5

    try:
        books = await db_query("SELECT id, title, author FROM books ORDER BY title", fetchall=True)

        if not books:
            await callback.message.edit_text("لا توجد كتب في المكتبة حالياً.", reply_markup=get_main_keyboard(await is_admin(user_id)))
            await state.clear()
            return

        total_pages = (len(books) + books_per_page - 1) // books_per_page

        if current_page >= total_pages and total_pages > 0:
            current_page = total_pages - 1
            await state.update_data(browse_page=current_page)
        elif total_pages == 0:
            current_page = 0
            await state.update_data(browse_page=current_page)


        start_index = current_page * books_per_page
        end_index = start_index + books_per_page
        current_page_books = books[start_index:end_index]

        keyboard_buttons = []
        for book_id, title, author in current_page_books:
            keyboard_buttons.append([InlineKeyboardButton(text=f"{title} - {author}", callback_data=f"show_book_details_{book_id}")])

        pagination_buttons = []
        if current_page > 0:
            pagination_buttons.append(InlineKeyboardButton(text="⬅️ السابق", callback_data="prev_page_browse"))
        if current_page < total_pages - 1:
            pagination_buttons.append(InlineKeyboardButton(text="التالي ➡️", callback_data="next_page_browse"))
        if pagination_buttons:
            keyboard_buttons.append(pagination_buttons)

        keyboard_buttons.append([InlineKeyboardButton(text="🔙 العودة للقائمة الرئيسية", callback_data="main_menu")])

        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await callback.message.edit_text("تصفح المكتبة:\nاختر كتاباً لعرض تفاصيله:", reply_markup=reply_markup)
        log_info(f"User {user_id} browsing library on page {current_page}.", context={"user_id": user_id, "page": current_page})
        await state.set_state(None) 
    except Exception as e:
        log_error(f"Error in browse_library for user {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء تصفح المكتبة.", reply_markup=get_main_keyboard(await is_admin(user_id)))
        await state.clear()

async def prev_page_browse(callback: CallbackQuery, state: FSMContext):
    """Handles navigation to the previous page in library browsing."""
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return
    await callback.answer()
    current_page = (await state.get_data()).get('browse_page', 0)
    if current_page > 0:
        await state.update_data(browse_page=current_page - 1)
    await browse_library(callback, state)

async def next_page_browse(callback: CallbackQuery, state: FSMContext):
    """Handles navigation to the next page in library browsing."""
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return
    await callback.answer()
    current_page = (await state.get_data()).get('browse_page', 0)
    await state.update_data(browse_page=current_page + 1)
    await browse_library(callback, state)

async def show_book_details(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Displays detailed information for a specific book selected from browsing or search results.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    
    try:
        book_id = int(callback.data.split('_')[-1])

        book = await db_query("SELECT title, author, category, description FROM books WHERE id = ?", (book_id,), fetchone=True)

        if book:
            title, author, category, description = book
            message_text = (
                f"📚 **العنوان:** {title}\n"
                f"✍️ **الكاتب:** {author}\n"
                f"🏷️ **التصنيف:** {category if category else 'غير محدد'}\n"
                f"📝 **الوصف:** {description if description else 'لا يوجد وصف.'}\n"
            )
            await callback.message.edit_text(
                message_text,
                reply_markup=get_book_details_keyboard(book_id),
                parse_mode='Markdown'
            )
            log_info(f"User {user_id} viewed details for book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})
        else:
            await callback.message.edit_text("عذراً، لم يتم العثور على معلومات عن هذا الكتاب.", reply_markup=get_main_keyboard(await is_admin(user_id)))
            log_error(f"Book ID {book_id} not found when trying to show details to user {user_id}.", context={"user_id": user_id, "book_id": book_id})
        await state.set_state(None)
    except ValueError:
        log_error(f"Invalid book ID in callback data: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الكتاب غير صالحة.", reply_markup=get_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        log_error(f"Error showing book details for user {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء عرض تفاصيل الكتاب.", reply_markup=get_main_keyboard(await is_admin(user_id)))

async def get_book(callback: CallbackQuery, bot: Bot, state: FSMContext) -> None:
    """
    Sends the book file to the user and records the download in user_books table.
    :param callback: The Aiogram CallbackQuery object.
    :param bot: The Aiogram Bot instance.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    log_info(f"User {user_id} requested to get a book.", context={"user_id": user_id})


    if await check_if_banned_and_respond(callback, bot):
        return


    if not await check_user_subscription(user_id, bot):
        await send_subscription_required_message(callback.message.chat.id, bot)
        await callback.answer("يجب الاشتراك في القنوات المطلوبة أولاً.", show_alert=True)
        return

    await callback.answer("جاري إرسال الكتاب...") 

    try:
        book_id = int(callback.data.split('_')[-1])
        book = await db_query("SELECT title, file_id FROM books WHERE id = ?", (book_id,), fetchone=True)

        if book and book[1]: 
            title, file_id = book
            try:
                await bot.send_document(chat_id=user_id, document=file_id, caption=f"📚 {title}")

             
                await db_query(
                    "INSERT OR IGNORE INTO user_books (user_id, book_id) VALUES (?, ?)",
                    (user_id, book_id),
                    commit=True
                )
                await callback.message.edit_text(
                    f"تم إرسال كتاب '{title}' بنجاح! 🎉\n"
                    "يمكنك الآن تقييم الكتاب أو تحديد حالة قراءته من قائمة 'كتبي التي نزلتها'.",
                    reply_markup=get_main_keyboard(await is_admin(user_id))
                )
                log_info(f"User {user_id} downloaded book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})
            except Exception as e:
                await callback.message.edit_text("عذراً، حدث خطأ أثناء إرسال الكتاب. يرجى المحاولة لاحقاً.", reply_markup=get_main_keyboard(await is_admin(user_id)))
                log_error(f"Error sending book ID {book_id} to user {user_id}: {e}", exc_info=True)
        else:
            await callback.message.edit_text("عذراً، لا يمكن الحصول على هذا الكتاب حالياً (ربما لا يوجد ملف له).", reply_markup=get_main_keyboard(await is_admin(user_id)))
            log_info(f"Attempted to get book ID {book_id} but no file_id found or book doesn't exist for user {user_id}.", context={"user_id": user_id, "book_id": book_id})
        await state.set_state(None)
    except ValueError:
        log_error(f"Invalid book ID in callback data for get_book: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الكتاب غير صالحة.", reply_markup=get_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        log_error(f"Error in get_book for user {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ غير متوقع أثناء معالجة طلب الكتاب.", reply_markup=get_main_keyboard(await is_admin(user_id)))


async def search_book_start(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Initiates the book search process by asking the user for a search query.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext to set the state for receiving the query.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    await callback.message.edit_text(
        "الرجاء إدخال كلمة مفتاحية للبحث (عنوان كتاب أو اسم كاتب):",
        reply_markup=get_cancel_keyboard() 
    )
    await state.set_state(UserStates.SEARCH_QUERY)
    log_info(f"User {user_id} started book search.", context={"user_id": user_id})

async def search_book_results(message: Message, state: FSMContext) -> None:
    """
    Processes the user's search query and displays matching books.
    :param message: The Aiogram Message object containing the search query.
    :param state: The FSMContext.
    """
    user_id = message.from_user.id
    if await check_if_banned_and_respond(message, message.bot):
        return

    search_query = message.text.strip()
    log_info(f"User {user_id} searched for: '{search_query}'", context={"user_id": user_id, "query": search_query})

    try:
        books = await db_query(
            "SELECT id, title, author FROM books WHERE title LIKE ? OR author LIKE ? ORDER BY title",
            (f"%{search_query}%", f"%{search_query}%"),
            fetchall=True
        )

        if not books:
            await message.answer(
                f"عذراً، لم يتم العثور على كتب تطابق '{search_query}'.",
                reply_markup=get_main_keyboard(await is_admin(user_id))
            )
            await state.clear()
            return

        keyboard_buttons = []
        for book_id, title, author in books:
            keyboard_buttons.append([InlineKeyboardButton(text=f"{title} - {author}", callback_data=f"show_book_details_{book_id}")])

        keyboard_buttons.append([InlineKeyboardButton(text="🔙 العودة للقائمة الرئيسية", callback_data="main_menu")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await message.answer(
            f"نتائج البحث عن '{search_query}':\nاختر كتاباً لعرض تفاصيله:",
            reply_markup=reply_markup
        )
        await state.clear()
    except Exception as e:
        log_error(f"Error processing search query '{search_query}' for user {user_id}: {e}", exc_info=True)
        await message.answer("عذراً، حدث خطأ أثناء البحث عن الكتب.", reply_markup=get_main_keyboard(await is_admin(user_id)))
        await state.clear()


async def random_book(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Selects and displays a random book from the library.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer("جاري البحث عن كتاب عشوائي...") 

    try:
        book = await db_query("SELECT id, title, author, category, description FROM books ORDER BY RANDOM() LIMIT 1", fetchone=True)

        if book:
            book_id, title, author, category, description = book
            message_text = (
                f"📚 **كتاب عشوائي لك:**\n"
                f"**العنوان:** {title}\n"
                f"**الكاتب:** {author}\n"
                f"**التصنيف:** {category if category else 'غير محدد'}\n"
                f"**الوصف:** {description if description else 'لا يوجد وصف.'}\n"
            )
            await callback.message.edit_text(
                message_text,
                reply_markup=get_book_details_keyboard(book_id),
                parse_mode='Markdown'
            )
            log_info(f"User {user_id} requested random book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})
        else:
            await callback.message.edit_text("عذراً، لا توجد كتب في المكتبة حالياً لعرض كتاب عشوائي.", reply_markup=get_main_keyboard(await is_admin(user_id)))
            log_info(f"No books available for random book request for user {user_id}.", context={"user_id": user_id})
        await state.set_state(None)
    except Exception as e:
        log_error(f"Error getting random book for user {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء جلب كتاب عشوائي.", reply_markup=get_main_keyboard(await is_admin(user_id)))


async def top_rated_books(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Displays a list of books ordered by their average rating.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 

    try:
        books = await db_query(
            "SELECT id, title, author, average_rating FROM books WHERE average_rating > 0 ORDER BY average_rating DESC, rating_count DESC LIMIT 10",
            fetchall=True
        )

        if not books:
            await callback.message.edit_text(
                "لا توجد كتب مقيّمة حالياً.",
                reply_markup=get_main_keyboard(await is_admin(user_id))
            )
            await state.clear()
            return

        message_text = "⭐️ **الكتب الأعلى تقييماً:**\n\n"
        keyboard_buttons = []
        for rank, (book_id, title, author, rating) in enumerate(books, 1):
            message_text += f"{rank}. {title} - {author} (التقييم: {rating:.1f}/5)\n"
            keyboard_buttons.append([InlineKeyboardButton(text=f"{title} - {author} ({rating:.1f}⭐️)", callback_data=f"show_book_details_{book_id}")])

        keyboard_buttons.append([InlineKeyboardButton(text="🔙 العودة للقائمة الرئيسية", callback_data="main_menu")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await callback.message.edit_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        log_info(f"User {user_id} viewed top rated books.", context={"user_id": user_id})
        await state.set_state(None)
    except Exception as e:
        log_error(f"Error getting top rated books for user {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء جلب الكتب الأعلى تقييماً.", reply_markup=get_main_keyboard(await is_admin(user_id)))


async def my_downloads(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Displays a list of books downloaded by the user, along with their read status and rating.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()

    try:
        downloaded_books = await db_query(
            """
            SELECT b.id, b.title, b.author, ub.is_read, ub.rating
            FROM user_books ub
            JOIN books b ON ub.book_id = b.id
            WHERE ub.user_id = ?
            ORDER BY b.title
            """,
            (user_id,),
            fetchall=True
        )

        if not downloaded_books:
            await callback.message.edit_text(
                "لم تقم بتنزيل أي كتب بعد.",
                reply_markup=get_main_keyboard(await is_admin(user_id))
            )
            await state.clear()
            return

        keyboard_buttons = []
        message_text = "📥 **كتبي التي نزلتها:**\n\n"
        for book_id, title, author, is_read, rating in downloaded_books:
            read_status = "✅ قرأته" if is_read else "❌ لم أقرأه"
            rating_display = f"⭐️ {rating}/5" if rating > 0 else "غير مقيّم"
            message_text += f"• {title} - {author} ({read_status}, {rating_display})\n"
            keyboard_buttons.append([InlineKeyboardButton(text=f"{title} - {author}", callback_data=f"view_my_book_status_{book_id}")])

        keyboard_buttons.append([InlineKeyboardButton(text="🔙 العودة للقائمة الرئيسية", callback_data="main_menu")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await callback.message.edit_text(
            message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        log_info(f"User {user_id} viewed their downloaded books.", context={"user_id": user_id})
        await state.set_state(None)
    except Exception as e:
        log_error(f"Error getting user {user_id}'s downloaded books: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء جلب قائمة كتبك.", reply_markup=get_main_keyboard(await is_admin(user_id)))

async def view_my_book_status(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Displays detailed status (read, rating) for a specific downloaded book and allows interaction.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    
    try:
        book_id = int(callback.data.split('_')[-1])

        user_book_info = await db_query(
            "SELECT b.title, b.author, ub.is_read, ub.rating FROM user_books ub JOIN books b ON ub.book_id = b.id WHERE ub.user_id = ? AND ub.book_id = ?",
            (user_id, book_id),
            fetchone=True
        )

        if user_book_info:
            title, author, is_read, rating = user_book_info
            read_status = "✅ قرأته" if is_read else "❌ لم أقرأه"
            rating_display = f"{rating}/5" if rating > 0 else "غير مقيّم"
            message_text = (
                f"📚 **إدارة كتاب:** {title} - {author}\n"
                f"**الحالة:** {read_status}\n"
                f"**تقييمك:** {rating_display}\n\n"
                "اختر لتقييم الكتاب أو لتغيير حالة القراءة:"
            )
            await callback.message.edit_text(
                message_text,
                reply_markup=get_book_rating_keyboard(book_id, rating, is_read),
                parse_mode='Markdown'
            )
            log_info(f"User {user_id} viewed status for downloaded book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})
        else:
            await callback.message.edit_text("عذراً، لم يتم العثور على معلومات عن هذا الكتاب في قائمة كتبك.", reply_markup=get_main_keyboard(await is_admin(user_id)))
            log_info(f"User {user_id} tried to view status for non-downloaded book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})
        await state.set_state(None)
    except ValueError:
        log_error(f"Invalid book ID in callback data for view_my_book_status: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الكتاب غير صالحة.", reply_markup=get_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        log_error(f"Error viewing my book status for user {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء جلب حالة الكتاب.", reply_markup=get_main_keyboard(await is_admin(user_id)))

async def toggle_read_status(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Toggles the 'is_read' status of a book for the current user.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()
    
    try:
        book_id = int(callback.data.split('_')[-1])

        current_info = await db_query("SELECT is_read, rating FROM user_books WHERE user_id = ? AND book_id = ?", (user_id, book_id), fetchone=True)
        if current_info:
            new_status = 1 if current_info[0] == 0 else 0 
            await db_query(
                "UPDATE user_books SET is_read = ? WHERE user_id = ? AND book_id = ?",
                (new_status, user_id, book_id),
                commit=True
            )

            await callback.message.edit_reply_markup(reply_markup=get_book_rating_keyboard(book_id, current_info[1], new_status))
            await callback.message.answer(f"تم تحديث حالة الكتاب بنجاح إلى {'**قرأته**' if new_status else '**لم أقرأه**'}.", parse_mode='Markdown')
            log_info(f"User {user_id} toggled read status for book ID {book_id} to {new_status}.", context={"user_id": user_id, "book_id": book_id, "new_status": new_status})
        else:
            await callback.message.answer("عذراً، لا يمكن تحديث حالة هذا الكتاب (غير موجود في قائمة كتبك).")
            log_info(f"User {user_id} tried to toggle read status for non-existent user_book entry {book_id}.", context={"user_id": user_id, "book_id": book_id})
        await state.set_state(None)
    except ValueError:
        log_error(f"Invalid book ID in callback data for toggle_read_status: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.answer("⚠️ بيانات الكتاب غير صالحة.")
    except Exception as e:
        log_error(f"Error toggling read status for user {user_id}: {e}", exc_info=True)
        await callback.message.answer("عذراً، حدث خطأ أثناء تحديث حالة القراءة.")

async def rate_book(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Allows a user to rate a book and updates the book's average rating.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    
    try:
    
        _, _, book_id_str, rating_str = callback.data.split('_')
        book_id = int(book_id_str)
        rating = int(rating_str)

        current_info = await db_query("SELECT is_read FROM user_books WHERE user_id = ? AND book_id = ?", (user_id, book_id), fetchone=True)
        if current_info:
            await db_query(
                "UPDATE user_books SET rating = ? WHERE user_id = ? AND book_id = ?",
                (rating, user_id, book_id),
                commit=True
            )
            
   
            ratings_data = await db_query("SELECT rating FROM user_books WHERE book_id = ? AND rating > 0", (book_id,), fetchall=True)
            if ratings_data:
                total_rating = sum(r[0] for r in ratings_data)
                num_ratings = len(ratings_data)
                new_avg_rating = total_rating / num_ratings
                await db_query(
                    "UPDATE books SET average_rating = ?, rating_count = ? WHERE id = ?",
                    (new_avg_rating, num_ratings, book_id),
                    commit=True
                )
            
  
            await callback.message.edit_reply_markup(reply_markup=get_book_rating_keyboard(book_id, rating, current_info[0]))
            await callback.message.answer(f"تم تسجيل تقييمك **{rating} نجوم** للكتاب بنجاح! 🌟", parse_mode='Markdown')
            log_info(f"User {user_id} rated book ID {book_id} with {rating} stars.", context={"user_id": user_id, "book_id": book_id, "rating": rating})
        else:
            await callback.message.answer("عذراً، لا يمكن تقييم هذا الكتاب (غير موجود في قائمة كتبك).")
            log_info(f"User {user_id} tried to rate non-existent user_book entry {book_id}.", context={"user_id": user_id, "book_id": book_id})
        await state.set_state(None)
    except ValueError:
        log_error(f"Invalid book ID or rating in callback data for rate_book: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.answer("⚠️ بيانات التقييم غير صالحة.")
    except Exception as e:
        log_error(f"Error rating book for user {user_id}: {e}", exc_info=True)
        await callback.message.answer("عذراً، حدث خطأ أثناء تسجيل تقييمك.")


async def suggest_book_start(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Initiates the conversation flow for a user to suggest a new book.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext to set the state.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    await callback.message.edit_text(
        "أدخل عنوان الكتاب الذي تود اقتراحه (أو اكتب 'إلغاء' للإلغاء):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(UserStates.SUGGEST_BOOK_TITLE)
    log_info(f"User {user_id} started book suggestion.", context={"user_id": user_id})

async def suggest_book_title_received(message: Message, state: FSMContext) -> None:
    """
    Receives the suggested book title from the user.
    :param message: The Aiogram Message object containing the title.
    :param state: The FSMContext to store the title and advance the state.
    """
    user_id = message.from_user.id
    if await check_if_banned_and_respond(message, message.bot):
        return

    title = message.text.strip()
    if title.lower() == 'إلغاء':
        await message.answer("تم إلغاء اقتراح الكتاب.", reply_markup=get_main_keyboard(await is_admin(user_id)))
        await state.clear()
        return

    await state.update_data(suggested_book_title=title)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="نعم، لدي ملف PDF", callback_data="yes_has_file")],
        [InlineKeyboardButton(text="لا، ليس لدي ملف", callback_data="no_has_file")]
    ])
    await message.answer(
        f"هل لديك ملف PDF لكتاب '{title}'؟ (حجم أقل من 20 ميجا بايت)",
        reply_markup=keyboard
    )
    await state.set_state(UserStates.SUGGEST_BOOK_FILE_CONFIRM)
    log_info(f"User {user_id} provided suggested book title: '{title}'.", context={"user_id": user_id, "title": title})

async def suggest_book_file_confirm(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """
    Handles user's confirmation about whether they have a PDF file for the suggested book.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext to store the confirmation and advance the state.
    :param bot: The Aiogram Bot instance for sending admin notifications.
    """
    user_id = callback.from_user.id
    if await check_if_banned_and_respond(callback, bot):
        return

    await callback.answer()
    has_file = 1 if callback.data == "yes_has_file" else 0
    await state.update_data(has_file_for_request=has_file)
    user_data = await state.get_data()
    book_title = user_data.get('suggested_book_title')

    if has_file:
        await callback.message.edit_text(
            "الرجاء إرسال ملف PDF الآن (يجب أن يكون أقل من 20 ميجا بايت).",
            reply_markup=get_cancel_keyboard()
        )
        await state.set_state(UserStates.SUGGEST_BOOK_FILE_UPLOAD)
        log_info(f"User {user_id} confirmed having a file for '{book_title}'.", context={"user_id": user_id, "title": book_title})
    else:
        try:
       
            await db_query(
                "INSERT INTO book_requests (user_id, requested_book_title, has_file, request_date) VALUES (?, ?, ?, ?)",
                (user_id, book_title, 0, datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                commit=True
            )
        
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        chat_id=admin_id,
                        text=f"🚨 طلب كتاب جديد من المستخدم {user_id}:\nالعنوان: {book_title}\nليس لديه ملف."
                    )
                except Exception as e:
                    log_error(f"Could not send request notification to admin {admin_id}: {e}", exc_info=True)

            await callback.message.edit_text(
                "شكراً لاقتراحك! سيتم مراجعة طلبك من قبل المدراء.",
                reply_markup=get_main_keyboard(await is_admin(user_id))
            )
            log_info(f"User {user_id} suggested book '{book_title}' without file.", context={"user_id": user_id, "title": book_title})
            await state.clear()
        except Exception as e:
            log_error(f"Error saving book request without file for user {user_id}: {e}", exc_info=True)
            await callback.message.edit_text("عذراً، حدث خطأ أثناء تسجيل طلبك.", reply_markup=get_main_keyboard(await is_admin(user_id)))
            await state.clear()

async def suggest_book_file_uploaded(message: Message, state: FSMContext, bot: Bot) -> None:
    """
    Receives the PDF file for the suggested book from the user.
    :param message: The Aiogram Message object containing the document.
    :param state: The FSMContext.
    :param bot: The Aiogram Bot instance for sending admin notifications.
    """
    user_id = message.from_user.id
    if await check_if_banned_and_respond(message, bot):
        return

    user_data = await state.get_data()
    book_title = user_data.get('suggested_book_title')

    if message.document:
        document = message.document
       
        if document.mime_type == 'application/pdf' and document.file_size <= 20 * 1024 * 1024:
            file_id = document.file_id
            try:
                await db_query(
                    "INSERT INTO book_requests (user_id, requested_book_title, has_file, file_id, request_date) VALUES (?, ?, ?, ?, ?)",
                    (user_id, book_title, 1, file_id, datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    commit=True
                )
            
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(
                            chat_id=admin_id,
                            text=f"🚨 طلب كتاب جديد من المستخدم {user_id}:\nالعنوان: {book_title}\n**لديه ملف PDF مرفق** (يرجى مراجعته)."
                        )
                        await bot.send_document(chat_id=admin_id, document=file_id, caption=f"ملف مقترح لكتاب: {book_title}")
                    except Exception as e:
                        log_error(f"Could not send request with file notification to admin {admin_id}: {e}", exc_info=True)

                await message.answer(
                    "شكراً لاقتراحك! تم استلام الملف وسيتم مراجعة طلبك من قبل المدراء.",
                    reply_markup=get_main_keyboard(await is_admin(user_id))
                )
                log_info(f"User {user_id} suggested book '{book_title}' with file.", context={"user_id": user_id, "title": book_title, "file_id": file_id})
                await state.clear()
            except Exception as e:
                log_error(f"Error saving book request with file for user {user_id}: {e}", exc_info=True)
                await message.answer("عذراً، حدث خطأ أثناء تسجيل طلبك مع الملف.", reply_markup=get_main_keyboard(await is_admin(user_id)))
                await state.clear()
        else:
            await message.answer(
                "عذراً، يجب أن يكون الملف بصيغة PDF وأقل من 20 ميجا بايت. يرجى إعادة المحاولة أو إلغاء.",
                reply_markup=get_cancel_keyboard()
            )
            log_info(f"User {user_id} sent invalid file type/size for book suggestion.", context={"user_id": user_id, "file_type": document.mime_type, "file_size": document.file_size})
    else:
        await message.answer(
            "الرجاء إرسال ملف PDF. إذا كنت لا ترغب في إرسال ملف، يمكنك إلغاء الاقتراح.",
            reply_markup=get_cancel_keyboard()
        )
        log_info(f"User {user_id} sent non-document message while expecting PDF for suggestion.", context={"user_id": user_id})

async def admin_panel(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Displays the admin panel keyboard to authorized administrators.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    log_info(f"User {user_id} attempted to access admin panel.", context={"user_id": user_id})


    if not await is_admin(user_id):
        await callback.answer("عذراً، أنت لست مديراً. 🛑", show_alert=True)
        await callback.message.edit_text("عذراً، أنت لست مديراً. 🛑", reply_markup=get_main_keyboard(False))
        log_error(f"Non-admin user {user_id} tried to access admin panel.", context={"user_id": user_id})
        await state.clear()
        return


    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    try:
        await callback.message.edit_text("لوحة التحكم الخاصة بالمدير:\nاختر إجراءً:", reply_markup=get_admin_keyboard())
        log_info(f"Admin {user_id} accessed admin panel successfully.", context={"user_id": user_id})
    except Exception as e:
        log_error(f"Error displaying admin panel for admin {user_id}: {e}", exc_info=True)
        await callback.message.answer("عذراً، حدث خطأ أثناء عرض لوحة التحكم.", reply_markup=get_main_keyboard(True))
    await state.set_state(None) 


async def admin_add_book_start(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Starts the multi-step process for adding a new book with full details.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext to manage the book addition flow.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id):
        await callback.answer("عذراً، أنت لست مديراً. 🛑", show_alert=True)
        await callback.message.edit_text("عذراً، أنت لست مديراً. 🛑", reply_markup=get_main_keyboard(False))
        await state.clear()
        return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()
    await callback.message.edit_text(
        "أدخل عنوان الكتاب (أو اكتب 'إلغاء' للإلغاء):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AdminStates.ADD_BOOK_TITLE)
    await state.update_data(add_book_data={})
    log_info(f"Admin {user_id} started adding a book (full mode).", context={"user_id": user_id})

async def admin_add_book_title(message: Message, state: FSMContext) -> None:
    """Receives the book title during the full book addition process."""
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, message.bot): return

    if message.text.lower() == 'إلغاء':
        await message.answer("تم إلغاء إضافة الكتاب.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    await state.update_data(add_book_data={'title': message.text.strip()})
    await message.answer("أدخل اسم الكاتب:")
    await state.set_state(AdminStates.ADD_BOOK_AUTHOR)

async def admin_add_book_author(message: Message, state: FSMContext) -> None:
    """Receives the book author during the full book addition process."""
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, message.bot): return

    if message.text.lower() == 'إلغاء':
        await message.answer("تم إلغاء إضافة الكتاب.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    current_data = await state.get_data()
    add_book_data = current_data.get('add_book_data', {})
    add_book_data['author'] = message.text.strip()
    await state.update_data(add_book_data=add_book_data)
    await message.answer("أدخل تصنيف الكتاب (مثل: 'خيال علمي', 'تاريخ', 'روايات'):",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="تخطي", callback_data="skip_category")]])
                        )
    await state.set_state(AdminStates.ADD_BOOK_CATEGORY)

async def admin_add_book_category(message: Message, state: FSMContext) -> None:
    """Receives the book category during the full book addition process."""
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, message.bot): return

    if message.text.lower() == 'إلغاء':
        await message.answer("تم إلغاء إضافة الكتاب.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    current_data = await state.get_data()
    add_book_data = current_data.get('add_book_data', {})
    add_book_data['category'] = message.text.strip()
    await state.update_data(add_book_data=add_book_data)
    await message.answer("أدخل وصفاً قصيراً للكتاب:",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="تخطي", callback_data="skip_description")]])
                        )
    await state.set_state(AdminStates.ADD_BOOK_DESCRIPTION)

async def skip_category(callback: CallbackQuery, state: FSMContext) -> None:
    """Allows admin to skip entering the book category."""
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, callback.bot): return

    await callback.answer()
    current_data = await state.get_data()
    add_book_data = current_data.get('add_book_data', {})
    add_book_data['category'] = None 
    await state.update_data(add_book_data=add_book_data)
    await callback.message.edit_text("أدخل وصفاً قصيراً للكتاب:",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="تخطي", callback_data="skip_description")]])
                                    )
    await state.set_state(AdminStates.ADD_BOOK_DESCRIPTION)

async def admin_add_book_description(message: Message, state: FSMContext) -> None:
    """Receives the book description during the full book addition process."""
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, message.bot): return

    if message.text.lower() == 'إلغاء':
        await message.answer("تم إلغاء إضافة الكتاب.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    current_data = await state.get_data()
    add_book_data = current_data.get('add_book_data', {})
    add_book_data['description'] = message.text.strip()
    await state.update_data(add_book_data=add_book_data)
    await message.answer("الرجاء إرسال ملف PDF للكتاب (أقل من 20 ميجا بايت):")
    await state.set_state(AdminStates.ADD_BOOK_FILE)

async def skip_description(callback: CallbackQuery, state: FSMContext) -> None:
    """Allows admin to skip entering the book description."""
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, callback.bot): return

    await callback.answer()
    current_data = await state.get_data()
    add_book_data = current_data.get('add_book_data', {})
    add_book_data['description'] = None 
    await state.update_data(add_book_data=add_book_data)
    await callback.message.edit_text("الرجاء إرسال ملف PDF للكتاب (أقل من 20 ميجا بايت):")
    await state.set_state(AdminStates.ADD_BOOK_FILE)

async def admin_add_book_file(message: Message, state: FSMContext, bot: Bot) -> None:
    """
    Receives the PDF file for the new book and saves all book details to the database.
    Sends a notification to the BOOKS_CHANNEL upon successful addition.
    :param message: The Aiogram Message object containing the document.
    :param state: The FSMContext.
    :param bot: The Aiogram Bot instance for sending notifications.
    """
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, bot): return

    if message.document:
        document = message.document

        if document.mime_type == 'application/pdf' and document.file_size <= 20 * 1024 * 1024:
            file_id = document.file_id
            book_data = (await state.get_data()).get('add_book_data', {})
            title = book_data.get('title')
            author = book_data.get('author')
            category = book_data.get('category')
            description = book_data.get('description')

            try:

                await db_query(
                    "INSERT INTO books (title, author, category, file_id, description, upload_date) VALUES (?, ?, ?, ?, ?, ?)",
                    (title, author, category, file_id, description, datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    commit=True
                )

                new_book = await db_query("SELECT id FROM books WHERE title = ? AND author = ?", (title, author), fetchone=True)
                if new_book:
                    new_book_id = new_book[0]
                    await send_book_notification(bot, {"id": new_book_id, "title": title, "author": author})

                await message.answer("تمت إضافة الكتاب بنجاح! 🎉", reply_markup=get_admin_keyboard())
                log_info(f"Admin {user_id} added book '{title}' (full mode).", context={"user_id": user_id, "title": title})
                await state.clear()
            except Exception as e:
                log_error(f"Error saving book '{title}' to DB for admin {user_id}: {e}", exc_info=True)
                await message.answer("عذراً، حدث خطأ أثناء حفظ الكتاب في قاعدة البيانات.", reply_markup=get_admin_keyboard())
                await state.clear()
        else:
            await message.answer("عذراً، يجب أن يكون الملف بصيغة PDF وأقل من 20 ميجا بايت. يرجى إعادة المحاولة.",
                                 reply_markup=get_cancel_keyboard()
                                )
            log_info(f"Admin {user_id} sent invalid file type/size for full book addition.", context={"user_id": user_id, "file_type": document.mime_type, "file_size": document.file_size})
    else:
        await message.answer("الرجاء إرسال ملف PDF.",
                             reply_markup=get_cancel_keyboard()
                            )
        log_info(f"Admin {user_id} sent non-document message while expecting PDF for full book addition.", context={"user_id": user_id})


async def admin_add_book_start_simple(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Starts the simplified process for adding a new book (title, author, file only).
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext to manage the simple book addition flow.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id):
        await callback.answer("عذراً، أنت لست مديراً. 🛑", show_alert=True)
        await callback.message.edit_text("عذراً، أنت لست مديراً. 🛑", reply_markup=get_main_keyboard(False))
        await state.clear()
        return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()
    await callback.message.edit_text(
        "أدخل عنوان الكتاب (أو اكتب 'إلغاء' للإلغاء):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AdminStates.ADD_BOOK_SIMPLE_TITLE)
    await state.update_data(add_book_data={}) 
    log_info(f"Admin {user_id} started simple book addition.", context={"user_id": user_id})

async def admin_add_book_simple_title(message: Message, state: FSMContext) -> None:
    """Receives the book title during the simple book addition process."""
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, message.bot): return

    if message.text.lower() == 'إلغاء':
        await message.answer("تم إلغاء إضافة الكتاب.", reply_markup=get_admin_keyboard())
        await state.clear()
        return
    await state.update_data(add_book_data={'title': message.text.strip()})
    await message.answer("أدخل اسم الكاتب:",
                         reply_markup=get_cancel_keyboard()
                        )
    await state.set_state(AdminStates.ADD_BOOK_SIMPLE_AUTHOR)

async def admin_add_book_simple_author(message: Message, state: FSMContext) -> None:
    """Receives the book author during the simple book addition process."""
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, message.bot): return

    if message.text.lower() == 'إلغاء':
        await message.answer("تم إلغاء إضافة الكتاب.", reply_markup=get_admin_keyboard())
        await state.clear()
        return
    current_data = await state.get_data()
    add_book_data = current_data.get('add_book_data', {})
    add_book_data['author'] = message.text.strip()
    await state.update_data(add_book_data=add_book_data)
    await message.answer("الرجاء إرسال ملف PDF للكتاب (أقل من 20 ميجا بايت):",
                         reply_markup=get_cancel_keyboard()
                        )
    await state.set_state(AdminStates.ADD_BOOK_SIMPLE_FILE)

async def admin_add_book_simple_file(message: Message, state: FSMContext, bot: Bot) -> None:
    """
    Receives the PDF file for the new book and saves minimal book details to the database (simple mode).
    Sends a notification to the BOOKS_CHANNEL upon successful addition.
    :param message: The Aiogram Message object containing the document.
    :param state: The FSMContext.
    :param bot: The Aiogram Bot instance for sending notifications.
    """
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, bot): return

    if message.document:
        document = message.document
   
        if document.mime_type == 'application/pdf' and document.file_size <= 20 * 1024 * 1024:
            file_id = document.file_id
            book_data = (await state.get_data()).get('add_book_data', {})
            title = book_data.get('title')
            author = book_data.get('author')

            try:
              
                await db_query(
                    "INSERT INTO books (title, author, category, file_id, description, upload_date) VALUES (?, ?, ?, ?, ?, ?)",
                    (title, author, None, file_id, None, datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    commit=True
                )
     
                new_book = await db_query("SELECT id FROM books WHERE title = ? AND author = ?", (title, author), fetchone=True)
                if new_book:
                    new_book_id = new_book[0]
                    await send_book_notification(bot, {"id": new_book_id, "title": title, "author": author})

                await message.answer("تمت إضافة الكتاب بنجاح (وضع سريع)! 🎉", reply_markup=get_admin_keyboard())
                log_info(f"Admin {user_id} added book '{title}' in simple mode.", context={"user_id": user_id, "title": title})
                await state.clear()
            except Exception as e:
                log_error(f"Error saving book '{title}' (simple mode) to DB for admin {user_id}: {e}", exc_info=True)
                await message.answer("عذراً، حدث خطأ أثناء حفظ الكتاب في قاعدة البيانات.", reply_markup=get_admin_keyboard())
                await state.clear()
        else:
            await message.answer("عذراً، يجب أن يكون الملف بصيغة PDF وأقل من 20 ميجا بايت. يرجى إعادة المحاولة.",
                                 reply_markup=get_cancel_keyboard()
                                )
            log_info(f"Admin {user_id} sent invalid file type/size for simple book addition.", context={"user_id": user_id, "file_type": document.mime_type, "file_size": document.file_size})
    else:
        await message.answer("الرجاء إرسال ملف PDF.",
                             reply_markup=get_cancel_keyboard()
                            )
        log_info(f"Admin {user_id} sent non-document message while expecting PDF for simple book addition.", context={"user_id": user_id})


async def admin_delete_book_start(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Starts the process for deleting a book by prompting the admin for a title or author.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id):
        await callback.answer("عذراً، أنت لست مديراً. 🛑", show_alert=True)
        await callback.message.edit_text("عذراً، أنت لست مديراً. 🛑", reply_markup=get_main_keyboard(False))
        await state.clear()
        return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()
    await callback.message.edit_text(
        "أدخل عنوان الكتاب أو اسم الكاتب الذي تريد حذفه (أو اكتب 'إلغاء' للإلغاء):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AdminStates.DELETE_BOOK_INPUT)
    log_info(f"Admin {user_id} started book deletion process.", context={"user_id": user_id})

async def admin_delete_book_input(message: Message, state: FSMContext) -> None:
    """
    Receives the book title/author for deletion, searches, and lists results for confirmation.
    :param message: The Aiogram Message object containing the search query.
    :param state: The FSMContext.
    """
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, message.bot): return

    search_query = message.text.strip()
    if search_query.lower() == 'إلغاء':
        await message.answer("تم إلغاء عملية الحذف.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    try:
        books = await db_query(
            "SELECT id, title, author FROM books WHERE title LIKE ? OR author LIKE ? ORDER BY title",
            (f"%{search_query}%", f"%{search_query}%"),
            fetchall=True
        )

        if not books:
            await message.answer(
                f"عذراً، لم يتم العثور على كتب تطابق '{search_query}'.",
                reply_markup=get_admin_keyboard()
            )
            await state.clear()
            return


        await state.update_data(delete_search_results=[{"id": b[0], "title": b[1], "author": b[2]} for b in books])

        keyboard_buttons = []
        message_text = f"تم العثور على الكتب التالية لـ '{search_query}':\n\n"
        for idx, (book_id, title, author) in enumerate(books):
            message_text += f"{idx+1}. **{title}** by {author}\n"
            keyboard_buttons.append([InlineKeyboardButton(text=f"حذف: {title} - {author}", callback_data=f"delete_book_confirm_{book_id}")])

        keyboard_buttons.append([InlineKeyboardButton(text="🔙 العودة للوحة التحكم", callback_data="admin_panel")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await message.answer(
            message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        await state.set_state(AdminStates.DELETE_BOOK_CONFIRM) 
        log_info(f"Admin {user_id} searched for books to delete: '{search_query}'. Found {len(books)}.", context={"user_id": user_id, "query": search_query})

    except Exception as e:
        log_error(f"Error searching for books to delete for admin {user_id} with query '{search_query}': {e}", exc_info=True)
        await message.answer("عذراً، حدث خطأ أثناء البحث عن الكتب للحذف.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_delete_book_confirm(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Confirms the deletion of a specific book after selection.
    :param callback: The Aiogram CallbackQuery object containing the book ID.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, callback.bot): return

    await callback.answer() 
    try:
        book_id = int(callback.data.split('_')[-1])

        book_info = await db_query("SELECT title, author FROM books WHERE id = ?", (book_id,), fetchone=True)
        if not book_info:
            await callback.message.edit_text("الكتاب غير موجود أو تم حذفه بالفعل.", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        title, author = book_info
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ تأكيد الحذف", callback_data=f"delete_book_execute_{book_id}")],
            [InlineKeyboardButton(text="❌ إلغاء", callback_data="admin_panel")] 
        ])
        await callback.message.edit_text(
            f"هل أنت متأكد أنك تريد حذف الكتاب:\n**{title} - {author}**؟\n\n"
            "ملاحظة: سيتم حذف جميع سجلات المستخدمين المتعلقة بهذا الكتاب أيضاً.",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        log_info(f"Admin {user_id} confirming deletion of book ID {book_id}: '{title}'.", context={"user_id": user_id, "book_id": book_id})
        await state.set_state(None) 
    except ValueError:
        log_error(f"Invalid book ID in callback data for delete_book_confirm: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الكتاب غير صالحة.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        log_error(f"Error confirming book deletion for admin {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء تأكيد الحذف.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_delete_book_execute(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Executes the deletion of a book from the database.
    :param callback: The Aiogram CallbackQuery object containing the book ID.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer("جاري حذف الكتاب...")
    try:
        book_id = int(callback.data.split('_')[-1])

        book_info = await db_query("SELECT title FROM books WHERE id = ?", (book_id,), fetchone=True)
        if book_info:
            title = book_info[0]

            await db_query("DELETE FROM books WHERE id = ?", (book_id,), commit=True)
            await callback.message.edit_text(f"تم حذف الكتاب **'{title}'** بنجاح.🗑️", reply_markup=get_admin_keyboard(), parse_mode='Markdown')
            log_info(f"Admin {user_id} successfully deleted book ID {book_id}: '{title}'.", context={"user_id": user_id, "book_id": book_id})
        else:
            await callback.message.edit_text("عذراً، لم يتم العثور على الكتاب للحذف أو تم حذفه مسبقاً.", reply_markup=get_admin_keyboard())
            log_info(f"Admin {user_id} tried to delete non-existent book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})
        await state.clear()
    except ValueError:
        log_error(f"Invalid book ID in callback data for delete_book_execute: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الكتاب غير صالحة.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        log_error(f"Error executing book deletion for admin {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء حذف الكتاب.", reply_markup=get_admin_keyboard())
        await state.clear()


async def admin_edit_book_start(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Starts the process for editing a book by prompting the admin for a title or author.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id):
        await callback.answer("عذراً، أنت لست مديراً. 🛑", show_alert=True)
        await callback.message.edit_text("عذراً، أنت لست مديراً. 🛑", reply_markup=get_main_keyboard(False))
        await state.clear()
        return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()
    await callback.message.edit_text(
        "أدخل عنوان الكتاب أو اسم الكاتب الذي تريد تعديله (أو اكتب 'إلغاء' للإلغاء):",
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AdminStates.EDIT_BOOK_INPUT)
    log_info(f"Admin {user_id} started book editing process.", context={"user_id": user_id})

async def admin_edit_book_input(message: Message, state: FSMContext) -> None:
    """
    Receives the book title/author for editing, searches, and lists results for selection.
    :param message: The Aiogram Message object containing the search query.
    :param state: The FSMContext.
    """
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, message.bot): return

    search_query = message.text.strip()
    if search_query.lower() == 'إلغاء':
        await message.answer("تم إلغاء عملية التعديل.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    try:
        books = await db_query(
            "SELECT id, title, author FROM books WHERE title LIKE ? OR author LIKE ? ORDER BY title",
            (f"%{search_query}%", f"%{search_query}%"),
            fetchall=True
        )

        if not books:
            await message.answer(
                f"عذراً، لم يتم العثور على كتب تطابق '{search_query}'.",
                reply_markup=get_admin_keyboard()
            )
            await state.clear()
            return

        keyboard_buttons = []
        message_text = f"تم العثور على الكتب التالية لـ '{search_query}':\n\n"
        for idx, (book_id, title, author) in enumerate(books):
            message_text += f"{idx+1}. **{title}** by {author}\n"
            keyboard_buttons.append([InlineKeyboardButton(text=f"تعديل: {title} - {author}", callback_data=f"edit_book_select_{book_id}")])

        keyboard_buttons.append([InlineKeyboardButton(text="🔙 العودة للوحة التحكم", callback_data="admin_panel")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await message.answer(
            message_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

        log_info(f"Admin {user_id} searched for books to edit: '{search_query}'. Found {len(books)}.", context={"user_id": user_id, "query": search_query})

    except Exception as e:
        log_error(f"Error searching for books to edit for admin {user_id} with query '{search_query}': {e}", exc_info=True)
        await message.answer("عذراً، حدث خطأ أثناء البحث عن الكتب للتعديل.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_edit_book_select(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Allows admin to select which field of the chosen book to edit.
    :param callback: The Aiogram CallbackQuery object containing the book ID.
    :param state: The FSMContext to store the selected book ID.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    try:
        book_id = int(callback.data.split('_')[-1])
        await state.update_data(edit_book_id=book_id) 

        book = await db_query("SELECT title, author, category, description FROM books WHERE id = ?", (book_id,), fetchone=True)
        if not book:
            await callback.message.edit_text("الكتاب غير موجود.", reply_markup=get_admin_keyboard())
            await state.clear()
            return
        
        title, author, category, description = book

        message_text = (
            f"تعديل كتاب: **{title} - {author}**\n\n"
            f"العنوان الحالي: {title}\n"
            f"الكاتب الحالي: {author}\n"
            f"التصنيف الحالي: {category if category else 'غير محدد'}\n"
            f"الوصف الحالي: {description if description else 'لا يوجد'}\n\n"
            "اختر الحقل الذي تريد تعديله:"
        )

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="تعديل العنوان", callback_data="edit_field_title")],
            [InlineKeyboardButton(text="تعديل الكاتب", callback_data="edit_field_author")],
            [InlineKeyboardButton(text="تعديل التصنيف", callback_data="edit_field_category")],
            [InlineKeyboardButton(text="تعديل الوصف", callback_data="edit_field_description")],
            [InlineKeyboardButton(text="تعديل ملف PDF", callback_data="edit_field_file_id")],
            [InlineKeyboardButton(text="🔙 العودة للبحث", callback_data="admin_edit_book_start")] 
        ])
        await callback.message.edit_text(message_text, reply_markup=keyboard, parse_mode='Markdown')
        await state.set_state(AdminStates.EDIT_BOOK_SELECT_FIELD) 
        log_info(f"Admin {user_id} selected book ID {book_id} for editing.", context={"user_id": user_id, "book_id": book_id})
    except ValueError:
        log_error(f"Invalid book ID in callback data for edit_book_select: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الكتاب غير صالحة.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        log_error(f"Error selecting book for editing for admin {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء اختيار الكتاب للتعديل.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_edit_book_field(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Prompts the admin for the new value for the selected book field.
    :param callback: The Aiogram CallbackQuery object containing the field to edit.
    :param state: The FSMContext to store the selected field and advance the state.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer() 
    field = callback.data.split('_')[-1] 
    await state.update_data(edit_field=field)

    prompts = {
        'title': "أدخل العنوان الجديد:",
        'author': "أدخل اسم الكاتب الجديد:",
        'category': "أدخل التصنيف الجديد (أو 'حذف' للإزالة):",
        'description': "أدخل الوصف الجديد (أو 'حذف' للإزالة):",
        'file_id': "الرجاء إرسال ملف PDF الجديد للكتاب (أقل من 20 ميجا بايت):"
    }
    
    keyboard = get_cancel_keyboard()
    try:
        await callback.message.edit_text(prompts.get(field, "قيمة غير صالحة."), reply_markup=keyboard)
        await state.set_state(AdminStates.EDIT_BOOK_NEW_VALUE) 
        log_info(f"Admin {user_id} selected field '{field}' to edit for book ID {(await state.get_data()).get('edit_book_id')}.", context={"user_id": user_id, "field": field})
    except Exception as e:
        log_error(f"Error prompting for new value for field '{field}' for admin {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء طلب القيمة الجديدة.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_edit_book_new_value(message: Message, state: FSMContext, bot: Bot) -> None:
    """
    Updates the selected book field with the new value provided by the admin.
    Handles both text input and PDF file uploads for 'file_id' field.
    :param message: The Aiogram Message object containing the new value (text or document).
    :param state: The FSMContext.
    :param bot: The Aiogram Bot instance.
    """
    user_id = message.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(message, bot): return

    current_data = await state.get_data()
    book_id = current_data.get('edit_book_id')
    field = current_data.get('edit_field')

    if message.text and message.text.lower() == 'إلغاء':
        await message.answer("تم إلغاء التعديل.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    new_value = None 

    if field == 'file_id':
        if message.document and message.document.mime_type == 'application/pdf' and message.document.file_size <= 20 * 1024 * 1024:
            new_value = message.document.file_id
            log_info(f"Admin {user_id} uploaded new PDF for book ID {book_id}.", context={"user_id": user_id, "book_id": book_id})
        else:
            await message.answer("الرجاء إرسال ملف PDF صالح (أقل من 20 ميجا بايت).",
                                 reply_markup=get_cancel_keyboard()
                                )
            log_info(f"Admin {user_id} sent invalid file for editing book ID {book_id}'s file_id.", context={"user_id": user_id, "book_id": book_id, "file_type": message.document.mime_type if message.document else 'N/A'})
            return 
    elif message.text: 
        new_value = message.text.strip()
        if field in ['category', 'description'] and new_value.lower() == 'حذف':
            new_value = None 
        log_info(f"Admin {user_id} provided new value for field '{field}' for book ID {book_id}.", context={"user_id": user_id, "book_id": book_id, "field": field, "new_value": new_value})
    else:

        await message.answer("الرجاء إرسال قيمة صالحة (نص أو ملف PDF).",
                             reply_markup=get_cancel_keyboard()
                            )
        log_info(f"Admin {user_id} sent unexpected message type for editing field '{field}' for book ID {book_id}.", context={"user_id": user_id, "book_id": book_id, "message_type": message.content_type})
        return

    try:
   
        await db_query(
            f"UPDATE books SET {field} = ? WHERE id = ?",
            (new_value, book_id),
            commit=True
        )
        await message.answer(f"تم تحديث حقل '{field}' للكتاب بنجاح! ✅", reply_markup=get_admin_keyboard())
        log_info(f"Admin {user_id} successfully updated field '{field}' for book ID {book_id}.", context={"user_id": user_id, "book_id": book_id, "field": field, "new_value": new_value})
        await state.clear() 
    except Exception as e:
        log_error(f"Error updating field '{field}' for book ID {book_id} for admin {user_id}: {e}", exc_info=True)
        await message.answer("عذراً، حدث خطأ أثناء تحديث الكتاب.", reply_markup=get_admin_keyboard())
        await state.clear()


async def admin_ban_user_start(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Starts the process to ban a user by prompting the admin for a user ID.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id):
        await callback.answer("عذراً، أنت لست مديراً. 🛑", show_alert=True)
        await callback.message.edit_text("عذراً، أنت لست مديراً. 🛑", reply_markup=get_main_keyboard(False))
        await state.clear()
        return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()
    await callback.message.edit_text(
        "الرجاء إدخال **معرف المستخدم (User ID)** الذي تريد حظره (أو اكتب 'إلغاء' للإلغاء):\n"
        "يمكنك الحصول على معرف المستخدم من خلال إعادة توجيه أي رسالة منه إلى @userinfobot.",
        parse_mode='Markdown',
        reply_markup=get_cancel_keyboard()
    )
    await state.set_state(AdminStates.BAN_USER_INPUT_ID)
    log_info(f"Admin {user_id} started user banning process.", context={"user_id": user_id})

async def admin_ban_user_input_id(message: Message, state: FSMContext, bot: Bot) -> None:
    """
    Receives the user ID to ban, verifies it, and prompts for confirmation.
    :param message: The Aiogram Message object containing the user ID.
    :param state: The FSMContext.
    :param bot: The Aiogram Bot instance.
    """
    admin_id = message.from_user.id
    if not await is_admin(admin_id): return
    if await check_if_banned_and_respond(message, bot): return

    user_id_str = message.text.strip()
    if user_id_str.lower() == 'إلغاء':
        await message.answer("تم إلغاء عملية الحظر.", reply_markup=get_admin_keyboard())
        await state.clear()
        return

    try:
        user_id_to_ban = int(user_id_str)
     
        if user_id_to_ban == admin_id:
            await message.answer("لا يمكنك حظر نفسك!", reply_markup=get_cancel_keyboard())
            return
        if await is_admin(user_id_to_ban):
            await message.answer("لا يمكنك حظر مديراً آخر!", reply_markup=get_cancel_keyboard())
            return

        user_info = await db_query("SELECT username, first_name, is_banned FROM users WHERE id = ?", (user_id_to_ban,), fetchone=True)

        display_name = f"ID: {user_id_to_ban}"
        current_ban_status = "غير محظور"
        if user_info:
            username, first_name, is_banned = user_info
            display_name = username if username else first_name
            current_ban_status = "محظور حالياً" if is_banned else "غير محظور"
        else:
  
            await db_query(
                "INSERT OR IGNORE INTO users (id, username, first_name, last_name, is_banned) VALUES (?, ?, ?, ?, ?)",
                (user_id_to_ban, None, None, None, 0), 
                commit=True
            )
            log_info(f"User {user_id_to_ban} not found in DB, added as new user for banning check.", context={"admin_id": admin_id, "target_user_id": user_id_to_ban})

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ تأكيد الحظر", callback_data=f"ban_user_execute_{user_id_to_ban}")],
            [InlineKeyboardButton(text="❌ إلغاء", callback_data="admin_panel")]
        ])
        await message.answer(
            f"هل أنت متأكد أنك تريد حظر المستخدم:\n**{display_name} (ID: {user_id_to_ban})**؟\n"
            f"الحالة الحالية: {current_ban_status}\n\n"
            "ملاحظة: هذا سيمنع المستخدم من استخدام البوت، وقد يحاول البوت حظره من قناة الكتب أيضاً.",
            reply_markup=keyboard,
            parse_mode='Markdown'
        )
        log_info(f"Admin {admin_id} confirming ban of user ID {user_id_to_ban}.", context={"admin_id": admin_id, "target_user_id": user_id_to_ban})
        await state.set_state(None) 
    except ValueError:
        await message.answer("معرف المستخدم غير صالح. الرجاء إدخال رقم صحيح.", reply_markup=get_cancel_keyboard())
        log_info(f"Admin {admin_id} entered invalid user ID for ban: '{user_id_str}'.", context={"admin_id": admin_id, "input": user_id_str})
    except Exception as e:
        log_error(f"Error processing ban user ID input for admin {admin_id}: {e}", exc_info=True)
        await message.answer("عذراً، حدث خطأ أثناء التحقق من المستخدم.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_ban_user_execute(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """
    Executes the user ban by updating the database and attempting to ban from the channel.
    :param callback: The Aiogram CallbackQuery object containing the user ID to ban.
    :param state: The FSMContext.
    :param bot: The Aiogram Bot instance.
    """
    admin_id = callback.from_user.id
    if not await is_admin(admin_id): return
    if await check_if_banned_and_respond(callback, bot):
        return

    await callback.answer("جاري حظر المستخدم...") 
    try:
        user_id_to_ban = int(callback.data.split('_')[-1])

      
        await db_query(
            "UPDATE users SET is_banned = 1 WHERE id = ?",
            (user_id_to_ban,),
            commit=True
        )
        
        await callback.message.edit_text(
            f"تم حظر المستخدم (ID: {user_id_to_ban}) بنجاح. 🚫",
            reply_markup=get_admin_keyboard(),
            parse_mode='Markdown'
        )
        log_info(f"Admin {admin_id} successfully banned user ID {user_id_to_ban} in DB.", context={"admin_id": admin_id, "banned_user_id": user_id_to_ban})


        if BOOKS_CHANNEL:
            try:
                await bot.ban_chat_member(chat_id=BOOKS_CHANNEL, user_id=user_id_to_ban)
                await callback.message.answer(f"وتم حظر المستخدم {user_id_to_ban} من القناة الخاصة بالكتب أيضاً.")
                log_info(f"User {user_id_to_ban} successfully banned from channel {BOOKS_CHANNEL}.", context={"admin_id": admin_id, "banned_user_id": user_id_to_ban, "channel": BOOKS_CHANNEL})
            except Exception as e:
                log_error(f"Could not ban user {user_id_to_ban} from channel {BOOKS_CHANNEL}: {e}", exc_info=True)
                await callback.message.answer(f"⚠️ لم يتمكن البوت من حظر المستخدم {user_id_to_ban} من القناة. يرجى التأكد من صلاحيات البوت (كإدارة المستخدمين).")
        
    except Exception as e:
        await callback.message.edit_text("عذراً، حدث خطأ أثناء محاولة حظر المستخدم.", reply_markup=get_admin_keyboard())
        log_error(f"Error executing ban for user ID {user_id_to_ban} for admin {admin_id}: {e}", exc_info=True)
    await state.clear()


async def admin_view_requests_start(callback: CallbackQuery, state: FSMContext) -> None:
    """
    Admin views pending book requests, listing them with options to view details.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id):
        await callback.answer("عذراً، أنت لست مديراً. 🛑", show_alert=True)
        await callback.message.edit_text("عذراً، أنت لست مديراً. 🛑", reply_markup=get_main_keyboard(False))
        await state.clear()
        return
    if await check_if_banned_and_respond(callback, callback.bot):
        return

    await callback.answer()
    try:
        requests = await db_query(
            """
            SELECT br.id, br.requested_book_title, br.has_file, br.request_date, u.username, u.first_name, br.user_id
            FROM book_requests br
            JOIN users u ON br.user_id = u.id
            WHERE br.status = 'pending'
            ORDER BY br.request_date DESC
            """,
            fetchall=True
        )

        if not requests:
            await callback.message.edit_text("لا توجد طلبات كتب معلقة حالياً. ✅", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        message_text = "📄 طلبات الكتب المعلقة:\n\n"
        keyboard_buttons = []
        for req_id, title, has_file, req_date, username, first_name, requesting_user_id in requests:
            user_display = username if username else first_name
            file_status = "✅ يوجد ملف" if has_file else "❌ لا يوجد ملف"
            message_text += f"• #{req_id} - '{title}' من {user_display} (ID: {requesting_user_id}) ({file_status}) بتاريخ {req_date}\n"

            keyboard_buttons.append([InlineKeyboardButton(text=f"عرض/إدارة الطلب #{req_id}", callback_data=f"view_request_details_{req_id}")])
                
        keyboard_buttons.append([InlineKeyboardButton(text="🔙 العودة للوحة التحكم", callback_data="admin_panel")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await callback.message.edit_text(message_text, reply_markup=reply_markup)
        log_info(f"Admin {user_id} viewed pending book requests.", context={"user_id": user_id})
        await state.set_state(UserStates.VIEWING_REQUEST_DETAILS) 
    except Exception as e:
        log_error(f"Error viewing book requests for admin {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء جلب طلبات الكتب.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_view_request_details(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """
    Admin views detailed information about a specific book request, including the option to download the file.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext to store the current request ID.
    :param bot: The Aiogram Bot instance.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, bot):
        return

    await callback.answer()
    try:
        request_id = int(callback.data.split('_')[-1])
        await state.update_data(current_request_id=request_id) 

        request_info = await db_query(
            """
            SELECT br.requested_book_title, br.has_file, br.file_id, br.user_id, u.username, u.first_name
            FROM book_requests br
            JOIN users u ON br.user_id = u.id
            WHERE br.id = ? AND br.status = 'pending' -- Only show pending requests
            """,
            (request_id,),
            fetchone=True
        )

        if not request_info:
            await callback.message.edit_text("الطلب غير موجود أو تمت معالجته.", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        title, has_file, file_id, user_requesting_id, username, first_name = request_info
        user_display = username if username else first_name

        message_text = (
            f"📝 تفاصيل طلب الكتاب #{request_id}:**\n"
            f"العنوان المطلوب: {title}\n"
            f"من المستخدم: {user_display} (ID: {user_requesting_id})\n"
            f"حالة الملف: {'✅ مرفق' if has_file else '❌ غير مرفق'}\n\n"
            "الإجراءات المتاحة:"
        )

        keyboard_buttons = []
        if has_file and file_id:
            keyboard_buttons.append([InlineKeyboardButton(text="⬇️ تحميل ملف الطلب", callback_data=f"download_request_file_{request_id}")])
        
        keyboard_buttons.append([
            InlineKeyboardButton(text="✅ موافقة الطلب", callback_data=f"approve_request_{request_id}"),
            InlineKeyboardButton(text="❌ رفض الطلب", callback_data=f"reject_request_{request_id}")
        ])
        keyboard_buttons.append([InlineKeyboardButton(text="🔙 العودة للطلبات", callback_data="admin_view_requests_start")])
        reply_markup = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await callback.message.edit_text(message_text, reply_markup=reply_markup)
        log_info(f"Admin {user_id} viewed details for request ID {request_id}.", context={"user_id": user_id, "request_id": request_id})
    except ValueError:
        log_error(f"Invalid request ID in callback data for view_request_details: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الطلب غير صالحة.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        log_error(f"Error viewing request details for admin {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء جلب تفاصيل الطلب.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_download_request_file(callback: CallbackQuery, bot: Bot, state: FSMContext) -> None:
    """
    Allows an admin to download the PDF file attached to a book request.
    :param callback: The Aiogram CallbackQuery object.
    :param bot: The Aiogram Bot instance.
    :param state: The FSMContext.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, bot):
        return

    await callback.answer("جاري إرسال ملف الطلب...") 
    try:
        request_id = int(callback.data.split('_')[-1])

        request_info = await db_query("SELECT requested_book_title, file_id FROM book_requests WHERE id = ? AND has_file = 1", (request_id,), fetchone=True)

        if request_info and request_info[1]:
            title, file_id = request_info
            try:
                await bot.send_document(chat_id=user_id, document=file_id, caption=f"📚 ملف الطلب: {title}")
                await callback.message.answer(f"تم إرسال ملف الطلب '{title}' بنجاح.")
                log_info(f"Admin {user_id} downloaded request file for request ID {request_id}.", context={"user_id": user_id, "request_id": request_id})
            except Exception as e:
                await callback.message.answer("عذراً، حدث خطأ أثناء إرسال ملف الطلب.")
                log_error(f"Error sending request file for request ID {request_id} to admin {user_id}: {e}", exc_info=True)
        else:
            await callback.message.answer("عذراً، لا يوجد ملف مرفق لهذا الطلب أو الطلب غير موجود.", show_alert=True)
            log_info(f"Admin {user_id} tried to download non-existent or file-less request {request_id}.", context={"user_id": user_id, "request_id": request_id})
    except ValueError:
        log_error(f"Invalid request ID in callback data for download_request_file: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.answer("⚠️ بيانات الطلب غير صالحة.")
    except Exception as e:
        log_error(f"Error in admin_download_request_file for admin {user_id}: {e}", exc_info=True)
        await callback.message.answer("عذراً، حدث خطأ غير متوقع أثناء معالجة طلب تحميل الملف.")

async def admin_approve_request(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """
    Admin approves a book request, updating its status in the database and notifying the requesting user.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    :param bot: The Aiogram Bot instance for notifications.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, bot):
        return

    await callback.answer("جاري الموافقة على الطلب...")
    try:
        request_id = int(callback.data.split('_')[-1])
        admin_id = callback.from_user.id

        request_info = await db_query(
            "SELECT requested_book_title, user_id FROM book_requests WHERE id = ? AND status = 'pending'",
            (request_id,),
            fetchone=True
        )

        if not request_info:
            await callback.message.edit_text("الطلب غير موجود أو تمت معالجته بالفعل.", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        title, requesting_user_id = request_info

        await db_query(
            "UPDATE book_requests SET status = 'approved' WHERE id = ?",
            (request_id,),
            commit=True
        )
        
        await callback.message.edit_text(f"تمت الموافقة على طلب كتاب '{title}' بنجاح. ✅", reply_markup=get_admin_keyboard())
        log_info(f"Admin {admin_id} approved request ID {request_id}: '{title}'.", context={"user_id": admin_id, "request_id": request_id})
        
   
        try:
            await bot.send_message(
                chat_id=requesting_user_id,
                text=f"🎉 تهانينا! تمت الموافقة على طلب كتابك '{title}'. سيتم إضافته إلى المكتبة قريباً."
            )
            log_info(f"Notified user {requesting_user_id} about approved request {request_id}.", context={"requesting_user_id": requesting_user_id, "request_id": request_id})
        except Exception as e:
            log_error(f"Could not notify user {requesting_user_id} about approved request {request_id}: {e}", exc_info=True)

        await state.clear()
    except ValueError:
        log_error(f"Invalid request ID in callback data for approve_request: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الطلب غير صالحة.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        log_error(f"Error approving request ID {request_id} for admin {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء الموافقة على الطلب.", reply_markup=get_admin_keyboard())
        await state.clear()

async def admin_reject_request(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """
    Admin rejects a book request, updating its status in the database and notifying the requesting user.
    :param callback: The Aiogram CallbackQuery object.
    :param state: The FSMContext.
    :param bot: The Aiogram Bot instance for notifications.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id): return
    if await check_if_banned_and_respond(callback, bot):
        return

    await callback.answer("جاري رفض الطلب...") 
    try:
        request_id = int(callback.data.split('_')[-1])
        admin_id = callback.from_user.id

        request_info = await db_query(
            "SELECT requested_book_title, user_id FROM book_requests WHERE id = ? AND status = 'pending'",
            (request_id,),
            fetchone=True
        )

        if not request_info:
            await callback.message.edit_text("الطلب غير موجود أو تمت معالجته بالفعل.", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        title, requesting_user_id = request_info

        await db_query(
            "UPDATE book_requests SET status = 'rejected' WHERE id = ?",
            (request_id,),
            commit=True
        )

        await callback.message.edit_text(f"تم رفض طلب كتاب '{title}' بنجاح. ❌", reply_markup=get_admin_keyboard())
        log_info(f"Admin {admin_id} rejected request ID {request_id}: '{title}'.", context={"user_id": admin_id, "request_id": request_id})
        

        try:
            await bot.send_message(
                chat_id=requesting_user_id,
                text=f"😔 نأسف لإعلامك بأنه تم رفض طلب كتابك '{title}'. شكراً لتفهمك."
            )
            log_info(f"Notified user {requesting_user_id} about rejected request {request_id}.", context={"requesting_user_id": requesting_user_id, "request_id": request_id})
        except Exception as e:
            log_error(f"Could not notify user {requesting_user_id} about rejected request {request_id}: {e}", exc_info=True)

        await state.clear()
    except ValueError:
        log_error(f"Invalid request ID in callback data for reject_request: {callback.data}", context={"user_id": user_id, "callback_data": callback.data})
        await callback.message.edit_text("⚠️ بيانات الطلب غير صالحة.", reply_markup=get_admin_keyboard())
        await state.clear()
    except Exception as e:
        log_error(f"Error rejecting request ID {request_id} for admin {user_id}: {e}", exc_info=True)
        await callback.message.edit_text("عذراً، حدث خطأ أثناء رفض الطلب.", reply_markup=get_admin_keyboard())
        await state.clear()

async def backup_database_handler(callback: CallbackQuery, bot: Bot):
    """
    Handles the database backup command from admin. Creates a backup file and sends it to the admin.
    :param callback: The Aiogram CallbackQuery object.
    :param bot: The Aiogram Bot instance.
    """
    user_id = callback.from_user.id
    if not await is_admin(user_id):
        await callback.answer("عذراً، أنت لست مخولاً للقيام بهذا الإجراء.", show_alert=True)
        await callback.message.answer("عذراً، أنت لست مخولاً للقيام بهذا الإجراء.")
        log_info(f"Non-admin user {user_id} attempted to access backup functionality.", context={"user_id": user_id})
        return
    if await check_if_banned_and_respond(callback, bot):
        return

    await callback.answer("بدء عملية النسخ الاحتياطي...")
    try:
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True) 
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file_name = f"library_backup_{timestamp}.db"
        backup_file_path = os.path.join(backup_dir, backup_file_name)

        success = await create_db_backup(DB_NAME, backup_file_path)

        if success:
            try:
            
                await bot.send_document(
                    chat_id=user_id,
                    document=FSInputFile(backup_file_path),
                    caption="✅ تم إنشاء نسخة احتياطية لقاعدة البيانات بنجاح!"
                )
                await callback.message.answer("تم إرسال ملف النسخة الاحتياطية إليك.")
                log_info(f"Admin {user_id} successfully received database backup.", context={"user_id": user_id})
            except Exception as e:
                await callback.message.answer("⚠️ تم إنشاء النسخة الاحتياطية لكن حدث خطأ في إرسالها إليك. تحقق من سجل الأخطاء.")
                log_error(f"Error sending backup file to admin {user_id}: {e}", exc_info=True)
            finally:
               
                if os.path.exists(backup_file_path):
                    os.remove(backup_file_path)
                    log_info(f"Deleted temporary backup file: {backup_file_path}")
        else:
            await callback.message.answer("❌ فشل إنشاء النسخة الاحتياطية لقاعدة البيانات. تحقق من سجل الأخطاء.")
            log_error(f"Admin {user_id} failed to receive database backup due to creation error.", context={"user_id": user_id})
    except Exception as e:
        log_error(f"Unexpected error in backup_database_handler for admin {user_id}: {e}", exc_info=True)
        await callback.message.answer("عذراً، حدث خطأ غير متوقع أثناء عملية النسخ الاحتياطي.")




async def main():
    """
    Main function to initialize the bot, dispatcher, database, and register all handlers.
    Starts the bot's polling loop.
    """
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()


    await add_is_banned_column()
    await init_db()

 
    dp.message.register(start_command, CommandStart())
    dp.callback_query.register(main_menu_callback, F.data == "main_menu")
    dp.callback_query.register(cancel_conversation, F.data == "cancel_conversation")

   
    dp.errors.register(error_handler)

   
    dp.callback_query.register(check_subscription_callback, F.data == "check_subscription")
    dp.callback_query.register(browse_library, F.data == "browse_library")
    dp.callback_query.register(prev_page_browse, F.data == "prev_page_browse")
    dp.callback_query.register(next_page_browse, F.data == "next_page_browse")
    dp.callback_query.register(show_book_details, F.data.startswith("show_book_details_"))
    dp.callback_query.register(get_book, F.data.startswith("get_book_")) # Handles actual book file sending

    dp.callback_query.register(search_book_start, F.data == "search_book_start")
    dp.message.register(search_book_results, UserStates.SEARCH_QUERY)

    dp.callback_query.register(random_book, F.data == "random_book")
    dp.callback_query.register(top_rated_books, F.data == "top_rated_books")

    dp.callback_query.register(my_downloads, F.data == "my_downloads")
    dp.callback_query.register(view_my_book_status, F.data.startswith("view_my_book_status_"))
    dp.callback_query.register(toggle_read_status, F.data.startswith("toggle_read_status_"))
    dp.callback_query.register(rate_book, F.data.startswith("rate_book_"))

    dp.callback_query.register(suggest_book_start, F.data == "suggest_book_start")
    dp.message.register(suggest_book_title_received, UserStates.SUGGEST_BOOK_TITLE)
    dp.callback_query.register(suggest_book_file_confirm, UserStates.SUGGEST_BOOK_FILE_CONFIRM)
  
    dp.message.register(suggest_book_file_uploaded, UserStates.SUGGEST_BOOK_FILE_UPLOAD, F.document)
    dp.message.register(suggest_book_file_uploaded, UserStates.SUGGEST_BOOK_FILE_UPLOAD, ~F.document)


 
    dp.callback_query.register(admin_panel, F.data == "admin_panel")

   
    dp.callback_query.register(admin_add_book_start, F.data == "admin_add_book_start")
    dp.message.register(admin_add_book_title, AdminStates.ADD_BOOK_TITLE)
    dp.message.register(admin_add_book_author, AdminStates.ADD_BOOK_AUTHOR)
    dp.message.register(admin_add_book_category, AdminStates.ADD_BOOK_CATEGORY)
    dp.callback_query.register(skip_category, F.data == "skip_category", AdminStates.ADD_BOOK_CATEGORY)
    dp.message.register(admin_add_book_description, AdminStates.ADD_BOOK_DESCRIPTION)
    dp.callback_query.register(skip_description, F.data == "skip_description", AdminStates.ADD_BOOK_DESCRIPTION)
    dp.message.register(admin_add_book_file, AdminStates.ADD_BOOK_FILE, F.document)
    dp.message.register(admin_add_book_file, AdminStates.ADD_BOOK_FILE, ~F.document) # Handle non-document messages for error feedback


    dp.callback_query.register(admin_add_book_start_simple, F.data == "admin_add_book_start_simple")
    dp.message.register(admin_add_book_simple_title, AdminStates.ADD_BOOK_SIMPLE_TITLE)
    dp.message.register(admin_add_book_simple_author, AdminStates.ADD_BOOK_SIMPLE_AUTHOR)
    dp.message.register(admin_add_book_simple_file, AdminStates.ADD_BOOK_SIMPLE_FILE, F.document)
    dp.message.register(admin_add_book_simple_file, AdminStates.ADD_BOOK_SIMPLE_FILE, ~F.document) # Handle non-document messages

  
    dp.callback_query.register(admin_delete_book_start, F.data == "admin_delete_book_start")
    dp.message.register(admin_delete_book_input, AdminStates.DELETE_BOOK_INPUT)
    dp.callback_query.register(admin_delete_book_confirm, F.data.startswith("delete_book_confirm_"))
    dp.callback_query.register(admin_delete_book_execute, F.data.startswith("delete_book_execute_"))


    dp.callback_query.register(admin_edit_book_start, F.data == "admin_edit_book_start")
    dp.message.register(admin_edit_book_input, AdminStates.EDIT_BOOK_INPUT)
    dp.callback_query.register(admin_edit_book_select, F.data.startswith("edit_book_select_"))
    dp.callback_query.register(admin_edit_book_field, AdminStates.EDIT_BOOK_SELECT_FIELD)

    dp.message.register(admin_edit_book_new_value, AdminStates.EDIT_BOOK_NEW_VALUE, F.text | F.document)

 
    dp.callback_query.register(admin_ban_user_start, F.data == "admin_ban_user_start")
    dp.message.register(admin_ban_user_input_id, AdminStates.BAN_USER_INPUT_ID)
    dp.callback_query.register(admin_ban_user_execute, F.data.startswith("ban_user_execute_"))

    dp.callback_query.register(admin_view_requests_start, F.data == "admin_view_requests_start")
    dp.callback_query.register(admin_view_request_details, F.data.startswith("view_request_details_"), StateFilter(UserStates.VIEWING_REQUEST_DETAILS))
    dp.callback_query.register(admin_download_request_file, F.data.startswith("download_request_file_"))
    dp.callback_query.register(admin_approve_request, F.data.startswith("approve_request_"))
    dp.callback_query.register(admin_reject_request, F.data.startswith("reject_request_"))

   
    dp.callback_query.register(backup_database_handler, F.data == "admin_backup_db", and_f(lambda cb: cb.from_user.id in ADMIN_IDS))




    @dp.message(Command("help")) 
    async def help_command(message: Message):
        """
        Displays a help message to the users, explaining bot functionalities.
        :param message: The Aiogram Message object.
        """
        user_id = message.from_user.id
        log_info(f"User {user_id} requested help.", context={"user_id": user_id})


        if await check_if_banned_and_respond(message, message.bot):
            return

        help_text = (
            "أهلاً بك في **Bookati**! أنا مكتبتك الرقمية الشخصية 📚.\n"
            "إليك ما يمكنك فعله:\n\n"
            "**📚 تصفح الكتب**\n"
            "اضغط على زر 'تصنيفات' لتكتشف الكتب حسب فئاتها.\n\n"
            "**🔍 البحث عن كتاب**\n"
            "اضغط على زر 'بحث عن كتاب' وابدأ بكتابة اسم الكتاب أو الكاتب.\n\n"
            "**🎲 كتاب عشوائي**\n"
            "احصل على كتاب عشوائي لتصفحه.\n\n"
            "**⭐️ الكتب الأعلى تقييماً**\n"
            "شاهد قائمة بالكتب التي نالت أعلى التقييمات من المستخدمين.\n\n"
            "**📥 كتبي التي نزلتها**\n"
            "تتبع الكتب التي قمت بتنزيلها، وقم بتقييمها أو تحديث حالة قراءتها.\n\n"
            "**💡 اقترح كتاباً**\n"
            "إذا لم تجد كتاباً، يمكنك اقتراحه ليتم إضافته إلى المكتبة.\n\n"
            "**رجوع للقائمة الرئيسية:**\n"
            "في أي وقت، يمكنك إرسال الأمر /start للعودة للقائمة الرئيسية."
        )
        try:
            await message.answer(help_text, parse_mode="Markdown")
        except Exception as e:
            log_error(f"Error in sending Help Info to {user_id}: {e}", exc_info=True)
    dp.message.register(help_command, Command("help"))


    try:
        log_info("Bot is starting polling...")
        await bot.delete_webhook(drop_pending_updates=True) 
        await dp.start_polling(bot)
    finally:
        log_info("Bot is shutting down. Closing bot session.")
        await bot.session.close() 

if __name__ == "__main__":
    asyncio.run(main())

# Copyright (c) 2025 m8mfm. All rights reserved.
