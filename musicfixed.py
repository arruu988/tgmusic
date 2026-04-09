from __future__ import annotations
from pyrogram import filters

import asyncio
import contextlib
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import requests
import yt_dlp
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait, RPCError
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message


API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_ID = int(os.getenv("OWNER_ID", "0"))

BASE_DIR = Path(__file__).resolve().parent
DOWNLOAD_DIR = BASE_DIR / "music_downloads"
DATABASE_PATH = BASE_DIR / "music_data.json"
LOG_PATH = BASE_DIR / "music_bot.log"
SESSION_NAME = "musicbot_session"
SESSION_WORKDIR = "."
MAX_LOCAL_FILES = 20
SEARCH_LIMIT = 6
SEARCH_TTL = 600
SUPPORT_CONTACT = "@proplayerhuladle"
CHAT_FILTER = filters.private | filters.group

LOCKED_TEXT = "🚫 Bot is currently locked by owner.\nContact admin: @proplayerhuladle"
BANNED_TEXT = "❌ You are blocked by admin.\nContact: @proplayerhuladle"


def validate_config() -> None:
    if API_ID <= 0 or not API_HASH or not BOT_TOKEN or OWNER_ID <= 0:
        raise RuntimeError("Set API_ID, API_HASH, BOT_TOKEN, and OWNER_ID before running the bot.")


def setup_logging() -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(LOG_PATH, encoding="utf-8"))
    except OSError:
        pass
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=handlers,
    )


def format_duration(seconds: Optional[int]) -> str:
    if not seconds:
        return "Unknown"
    total = int(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def format_uptime(seconds: int) -> str:
    days, remainder = divmod(int(seconds), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


def shorten(text: str, limit: int = 34) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)].rstrip() + "..."


def display_name(user: Any) -> str:
    if not user:
        return "Unknown User"
    if getattr(user, "username", None):
        return f"@{user.username}"
    names = [getattr(user, "first_name", "") or "", getattr(user, "last_name", "") or ""]
    label = " ".join(part for part in names if part).strip()
    return label or str(getattr(user, "id", "Unknown"))


def split_text(text: str, limit: int = 3900) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    current: List[str] = []
    size = 0
    for line in text.splitlines(True):
        if size + len(line) > limit and current:
            chunks.append("".join(current))
            current = [line]
            size = len(line)
        else:
            current.append(line)
            size += len(line)
    if current:
        chunks.append("".join(current))
    return chunks


@dataclass
class Track:
    title: str
    duration: int
    duration_text: str
    webpage_url: str
    thumbnail_url: str
    uploader: str
    requested_by_id: int
    requested_by_name: str
    source_id: str
    original_query: str = ""
    file_path: str = ""
    audio_file_id: str = ""
    unique_id: str = field(default_factory=lambda: uuid.uuid4().hex[:10])


@dataclass
class SearchPayload:
    chat_id: int
    user_id: int
    query: str
    results: List[Dict[str, Any]]
    created_at: float = field(default_factory=time.time)


@dataclass
class ChatSession:
    queue: List[Track] = field(default_factory=list)
    current: Optional[Track] = None
    worker_task: Optional[asyncio.Task] = None
    skip_event: asyncio.Event = field(default_factory=asyncio.Event)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class JsonDatabase:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = asyncio.Lock()
        self.data = self.default_data()

    @staticmethod
    def default_data() -> Dict[str, Any]:
        return {
            "users": {},
            "groups": {},
            "banned_users": [],
            "banned_groups": [],
            "bot_locked": False,
            "stats": {
                "songs_played": 0,
                "commands_used": 0,
                "created_at": int(time.time()),
            },
            "files": [],
        }

    async def load(self) -> None:
        async with self.lock:
            if not self.path.exists():
                self.data = self.default_data()
                await self._save_locked()
                return
            self.data = await asyncio.to_thread(self._read_sync)
            self._merge_defaults_locked()
            await self._save_locked()

    def _read_sync(self) -> Dict[str, Any]:
        try:
            with self.path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict):
                return payload
        except (OSError, json.JSONDecodeError):
            logging.exception("Database read failed, recreating music_data.json.")
            with contextlib.suppress(OSError):
                backup = self.path.with_name(f"{self.path.stem}.corrupt.{int(time.time())}.json")
                if self.path.exists():
                    self.path.replace(backup)
        return self.default_data()

    def _merge_defaults_locked(self) -> None:
        defaults = self.default_data()
        for key, value in defaults.items():
            if key not in self.data:
                self.data[key] = value
        if not isinstance(self.data.get("stats"), dict):
            self.data["stats"] = defaults["stats"]
        for key, value in defaults["stats"].items():
            self.data["stats"].setdefault(key, value)
        self.data["users"] = self.data.get("users") or {}
        self.data["groups"] = self.data.get("groups") or {}
        self.data["banned_users"] = list(self.data.get("banned_users") or [])
        self.data["banned_groups"] = list(self.data.get("banned_groups") or [])
        self.data["files"] = list(self.data.get("files") or [])
        self.data["bot_locked"] = bool(self.data.get("bot_locked", False))

    async def _save_locked(self) -> None:
        await asyncio.to_thread(self._write_sync, self.data)

    def _write_sync(self, payload: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(".tmp")
        with temp_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
        temp_path.replace(self.path)

    async def read_copy(self) -> Dict[str, Any]:
        async with self.lock:
            return json.loads(json.dumps(self.data))

    async def record_command(self) -> None:
        async with self.lock:
            self.data["stats"]["commands_used"] = int(self.data["stats"].get("commands_used", 0)) + 1
            await self._save_locked()

    async def record_user(self, user: Any) -> None:
        if not user:
            return
        async with self.lock:
            self.data["users"][str(user.id)] = {
                "username": user.username or "",
                "name": display_name(user),
                "last_seen": int(time.time()),
            }
            await self._save_locked()

    async def record_group(self, chat: Any) -> None:
        if not chat:
            return
        async with self.lock:
            self.data["groups"][str(chat.id)] = {
                "group_name": chat.title or "Unknown Group",
                "username": chat.username or "",
                "last_seen": int(time.time()),
            }
            await self._save_locked()

    async def is_locked(self) -> bool:
        async with self.lock:
            return bool(self.data.get("bot_locked", False))

    async def set_locked(self, value: bool) -> None:
        async with self.lock:
            self.data["bot_locked"] = bool(value)
            await self._save_locked()

    async def is_banned_user(self, user_id: int) -> bool:
        async with self.lock:
            return int(user_id) in {int(item) for item in self.data["banned_users"]}

    async def is_banned_group(self, group_id: int) -> bool:
        async with self.lock:
            return int(group_id) in {int(item) for item in self.data["banned_groups"]}

    async def ban_user(self, user_id: int) -> bool:
        async with self.lock:
            banned = {int(item) for item in self.data["banned_users"]}
            if int(user_id) in banned:
                return False
            self.data["banned_users"].append(int(user_id))
            await self._save_locked()
            return True

    async def ban_group(self, group_id: int) -> bool:
        async with self.lock:
            banned = {int(item) for item in self.data["banned_groups"]}
            if int(group_id) in banned:
                return False
            self.data["banned_groups"].append(int(group_id))
            await self._save_locked()
            return True

    async def increment_songs_played(self) -> None:
        async with self.lock:
            self.data["stats"]["songs_played"] = int(self.data["stats"].get("songs_played", 0)) + 1
            await self._save_locked()

    async def register_file(self, file_path: str) -> None:
        async with self.lock:
            files = [item for item in self.data["files"] if item.get("path") != file_path]
            files.append({"path": file_path, "created_at": int(time.time())})
            files.sort(key=lambda item: int(item.get("created_at", 0)))
            self.data["files"] = files
            await self._save_locked()

    async def cleanup_file_index(self, protected: Set[str], limit: int) -> List[str]:
        async with self.lock:
            cleaned: List[Dict[str, Any]] = []
            for item in self.data["files"]:
                path = str(item.get("path", "")).strip()
                if path and Path(path).exists():
                    cleaned.append({"path": path, "created_at": int(item.get("created_at", 0))})
            cleaned.sort(key=lambda item: int(item.get("created_at", 0)))
            removable: List[str] = []
            while len(cleaned) > limit:
                candidate = next((entry for entry in cleaned if entry["path"] not in protected), None)
                if not candidate:
                    break
                removable.append(candidate["path"])
                cleaned.remove(candidate)
            self.data["files"] = cleaned
            await self._save_locked()
            return removable


class MusicBot:
    def __init__(self) -> None:
        self.boot_time = int(time.time())
        self.db = JsonDatabase(DATABASE_PATH)
        self.sessions: Dict[int, ChatSession] = {}
        self.search_cache: Dict[str, SearchPayload] = {}
        self.app = Client(
            name=SESSION_NAME,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workdir=SESSION_WORKDIR,
        )
        self.register_handlers()

    def register_handlers(self) -> None:
        self.app.add_handler(MessageHandler(self.start_handler, filters.command(["start"])))
        self.app.add_handler(MessageHandler(self.help_handler, filters.command(["help"])))
        self.app.add_handler(MessageHandler(self.play_handler, filters.command(["play"])))
        self.app.add_handler(MessageHandler(self.lyrics_handler, filters.command(["lyrics"])))
        self.app.add_handler(MessageHandler(self.lock_handler, filters.command(["lock"])))
        self.app.add_handler(MessageHandler(self.unlock_handler, filters.command(["unlock"])))
        self.app.add_handler(MessageHandler(self.admin_handler, filters.command(["admin"])))
        self.app.add_handler(MessageHandler(self.ban_handler, filters.command(["ban"])))
        self.app.add_handler(MessageHandler(self.broadcast_handler, filters.command(["broadcast"])))
        self.app.add_handler(CallbackQueryHandler(self.callback_handler))

    async def run(self) -> None:
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
        await self.db.load()
        await self.cleanup_local_files()
        await self.app.start()
        me = await self.app.get_me()
        logging.info("Started bot as @%s", me.username)
        await idle()
        await self.app.stop()

    async def track_context(self, message: Message) -> None:
        if message.from_user:
            await self.db.record_user(message.from_user)
        if message.chat and message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP}:
            await self.db.record_group(message.chat)

    async def safe_reply(self, message: Message, text: str, **kwargs: Any) -> Optional[Message]:
        try:
            return await message.reply_text(text, disable_web_page_preview=True, **kwargs)
        except FloodWait as exc:
            await asyncio.sleep(int(exc.value) + 1)
            try:
                return await message.reply_text(text, disable_web_page_preview=True, **kwargs)
            except RPCError:
                logging.exception("reply_text failed after FloodWait")
        except RPCError:
            logging.exception("reply_text failed")
        return None

    async def safe_send(self, chat_id: int, text: str, **kwargs: Any) -> Optional[Message]:
        try:
            return await self.app.send_message(chat_id, text, disable_web_page_preview=True, **kwargs)
        except FloodWait as exc:
            await asyncio.sleep(int(exc.value) + 1)
            try:
                return await self.app.send_message(chat_id, text, disable_web_page_preview=True, **kwargs)
            except RPCError:
                logging.exception("send_message failed after FloodWait")
        except RPCError:
            logging.exception("send_message failed")
        return None

    async def safe_answer(self, query: CallbackQuery, text: str, show_alert: bool = False) -> None:
        try:
            await query.answer(text[:180], show_alert=show_alert)
        except FloodWait as exc:
            await asyncio.sleep(int(exc.value) + 1)
            with contextlib.suppress(RPCError):
                await query.answer(text[:180], show_alert=show_alert)
        except RPCError:
            logging.exception("callback answer failed")

    async def send_long(self, chat_id: int, text: str, reply_to_message_id: Optional[int] = None) -> None:
        for chunk in split_text(text):
            await self.safe_send(chat_id, chunk, reply_to_message_id=reply_to_message_id)

    async def ensure_access(self, message: Message) -> bool:
        await self.track_context(message)
        user_id = message.from_user.id if message.from_user else 0
        chat_id = message.chat.id
        is_owner = user_id == OWNER_ID

        if not is_owner and await self.db.is_locked():
            await self.safe_reply(message, LOCKED_TEXT)
            return False

        if not is_owner and user_id and await self.db.is_banned_user(user_id):
            await self.safe_reply(message, BANNED_TEXT)
            return False

        if message.chat.type in {ChatType.GROUP, ChatType.SUPERGROUP} and await self.db.is_banned_group(chat_id):
            return False

        return True

    async def ensure_owner(self, message: Message) -> bool:
        await self.track_context(message)
        if not message.from_user or message.from_user.id != OWNER_ID:
            await self.safe_reply(message, "🚫 Only the owner can use this command.")
            return False
        return True

    async def ensure_owner_callback(self, query: CallbackQuery) -> bool:
        if not query.from_user or query.from_user.id != OWNER_ID:
            await self.safe_answer(query, "Only the owner can use this control.", show_alert=True)
            return False
        return True

    async def start_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_access(message):
            return
        text = (
            "🎵 Welcome to Pro Music Bot\n\n"
            "Use /play <song name> to search and send music directly in Telegram.\n"
            "Use /help to see all commands.\n"
            f"Support: {SUPPORT_CONTACT}"
        )
        await self.safe_reply(message, text)

    async def help_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_access(message):
            return
        text = (
            "📖 Commands\n\n"
            "/start - Start the bot\n"
            "/help - Show help\n"
            "/play <song name> - Search songs and queue one\n"
            "/lyrics <song name> - Get lyrics\n\n"
            "Owner Commands:\n"
            "/lock - Lock bot globally\n"
            "/unlock - Unlock bot globally\n"
            "/admin - View stats, users, groups\n"
            "/ban <id> - Ban user or group\n"
            "/broadcast <message> - Broadcast to tracked chats\n\n"
            "Player Controls:\n"
            "⏭️ Skip | ❌ Stop | 📋 Queue | 📥 Download | 📝 Lyrics\n\n"
            f"Support: {SUPPORT_CONTACT}"
        )
        await self.safe_reply(message, text)

    async def play_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_access(message):
            return
        if len(message.command) < 2:
            await self.safe_reply(message, "🎵 Usage: /play <song name>")
            return

        query = message.text.split(maxsplit=1)[1].strip()
        if not query:
            await self.safe_reply(message, "🎵 Usage: /play <song name>")
            return

        status = await self.safe_reply(message, f"🔎 Searching for: {query}")
        results = await self.search_youtube(query)
        if not results:
            if status:
                with contextlib.suppress(RPCError):
                    await status.edit_text("❌ No matching songs found. Try another title.")
                    return
            await self.safe_reply(message, "❌ No matching songs found. Try another title.")
            return

        search_id = uuid.uuid4().hex[:8]
        self.cleanup_expired_searches()
        self.search_cache[search_id] = SearchPayload(
            chat_id=message.chat.id,
            user_id=message.from_user.id if message.from_user else 0,
            query=query,
            results=results,
        )

        keyboard = [
            [
                InlineKeyboardButton(
                    f"{index}. {shorten(item['title'], 28)} ({item['duration_text']})",
                    callback_data=f"pick:{search_id}:{index - 1}",
                )
            ]
            for index, item in enumerate(results, start=1)
        ]
        keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data=f"pick:{search_id}:cancel")])

        text = f"🎧 Search Results for: {query}\n\nSelect one track from the top 6 results below."
        if status:
            with contextlib.suppress(RPCError):
                await status.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard))
                return
        await self.safe_reply(message, text, reply_markup=InlineKeyboardMarkup(keyboard))

    async def lyrics_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_access(message):
            return
        if len(message.command) < 2:
            await self.safe_reply(message, "📝 Usage: /lyrics <song name>")
            return
        query = message.text.split(maxsplit=1)[1].strip()
        await self.send_lyrics(message.chat.id, query, reply_to_message_id=message.id)

    async def lock_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_owner(message):
            return
        await self.db.set_locked(True)
        await self.safe_reply(message, "🔒 Bot locked globally.")

    async def unlock_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_owner(message):
            return
        await self.db.set_locked(False)
        await self.safe_reply(message, "🔓 Bot unlocked globally.")

    async def admin_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_owner(message):
            return

        data = await self.db.read_copy()
        total_users = len(data["users"])
        total_groups = len(data["groups"])
        songs_played = int(data["stats"].get("songs_played", 0))
        uptime = format_uptime(int(time.time()) - self.boot_time)

        users = []
        for user_id, info in sorted(data["users"].items(), key=lambda item: item[1].get("last_seen", 0), reverse=True):
            username = f"@{info.get('username')}" if info.get("username") else "No username"
            users.append(f"{user_id} | {info.get('name', 'Unknown')} | {username}")
        if not users:
            users.append("No users tracked yet.")

        groups = []
        for group_id, info in sorted(data["groups"].items(), key=lambda item: item[1].get("last_seen", 0), reverse=True):
            username = f"@{info.get('username')}" if info.get("username") else "No username"
            groups.append(f"{info.get('group_name', 'Unknown Group')} | {group_id} | {username}")
        if not groups:
            groups.append("No groups tracked yet.")

        panel = (
            "👑 Admin Panel\n\n"
            "📊 Stats:\n"
            f"Total users: {total_users}\n"
            f"Total groups: {total_groups}\n"
            f"Uptime: {uptime}\n"
            f"Total songs played: {songs_played}\n"
            f"Bot locked: {'Yes' if data.get('bot_locked') else 'No'}\n"
            f"Banned users: {len(data['banned_users'])}\n"
            f"Banned groups: {len(data['banned_groups'])}\n\n"
            "👤 Users List:\n"
            + "\n".join(users)
            + "\n\n👥 Groups List:\n"
            + "\n".join(groups)
        )
        await self.send_long(message.chat.id, panel, reply_to_message_id=message.id)

    async def ban_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_owner(message):
            return
        if len(message.command) < 2:
            await self.safe_reply(message, "🚫 Usage: /ban <user_id or group_id>")
            return

        raw_target = message.command[1].strip()
        try:
            target_id = int(raw_target)
        except ValueError:
            await self.safe_reply(message, "❌ Invalid ID. Please provide a numeric user ID or group ID.")
            return

        if target_id < 0:
            added = await self.db.ban_group(target_id)
            if not added:
                await self.safe_reply(message, f"🚫 Group {target_id} is already banned.")
                return
            await self.safe_reply(message, f"🚫 Group banned: {target_id}")
            with contextlib.suppress(RPCError):
                await self.app.leave_chat(target_id)
        else:
            added = await self.db.ban_user(target_id)
            if not added:
                await self.safe_reply(message, f"🚫 User {target_id} is already banned.")
                return
            await self.safe_reply(message, f"🚫 User banned: {target_id}")
            with contextlib.suppress(RPCError):
                await self.safe_send(target_id, BANNED_TEXT)

    async def broadcast_handler(self, _: Client, message: Message) -> None:
        await self.db.record_command()
        if not await self.ensure_owner(message):
            return
        if len(message.command) < 2:
            await self.safe_reply(message, "📢 Usage: /broadcast <message>")
            return

        text = message.text.split(maxsplit=1)[1].strip()
        if not text:
            await self.safe_reply(message, "📢 Usage: /broadcast <message>")
            return

        data = await self.db.read_copy()
        targets = {int(item) for item in data["users"].keys()}
        targets.update(int(item) for item in data["groups"].keys())

        notice = await self.safe_reply(message, f"📢 Broadcasting to {len(targets)} chats...")
        success = 0
        failed = 0

        for chat_id in targets:
            try:
                await self.app.send_message(chat_id, text, disable_web_page_preview=True)
                success += 1
            except FloodWait as exc:
                await asyncio.sleep(int(exc.value) + 1)
                try:
                    await self.app.send_message(chat_id, text, disable_web_page_preview=True)
                    success += 1
                except RPCError:
                    failed += 1
            except RPCError:
                failed += 1

        result = f"✅ Success: {success}\n❌ Failed: {failed}"
        if notice:
            with contextlib.suppress(RPCError):
                await notice.edit_text(result)
                return
        await self.safe_reply(message, result)

    async def callback_handler(self, _: Client, query: CallbackQuery) -> None:
        data = query.data or ""
        try:
            if data.startswith("pick:"):
                await self.handle_pick_callback(query)
            elif data.startswith("ctl:"):
                await self.handle_control_callback(query)
            else:
                await self.safe_answer(query, "Unknown action.", show_alert=False)
        except Exception:
            logging.exception("callback handler failed")
            await self.safe_answer(query, "Something went wrong.", show_alert=True)

    async def handle_pick_callback(self, query: CallbackQuery) -> None:
        parts = (query.data or "").split(":")
        if len(parts) != 3:
            await self.safe_answer(query, "Invalid selection.", show_alert=True)
            return

        _, search_id, raw_index = parts
        payload = self.search_cache.get(search_id)
        if not payload:
            await self.safe_answer(query, "This search session expired.", show_alert=True)
            return

        if query.from_user and query.from_user.id != payload.user_id:
            await self.safe_answer(query, "Only the requester can select a result.", show_alert=True)
            return

        if query.from_user and query.from_user.id != OWNER_ID and await self.db.is_locked():
            await self.safe_answer(query, "Bot is locked by owner.", show_alert=True)
            return

        if query.from_user and query.from_user.id != OWNER_ID and await self.db.is_banned_user(query.from_user.id):
            await self.safe_answer(query, "You are blocked by admin.", show_alert=True)
            return

        if await self.db.is_banned_group(payload.chat_id):
            await self.safe_answer(query, "This group is blocked.", show_alert=True)
            return

        if raw_index == "cancel":
            self.search_cache.pop(search_id, None)
            await self.safe_answer(query, "Selection cancelled.")
            with contextlib.suppress(RPCError):
                await query.message.edit_text("❌ Search cancelled.")
            return

        try:
            index = int(raw_index)
            result = payload.results[index]
        except (ValueError, IndexError):
            await self.safe_answer(query, "Invalid result selected.", show_alert=True)
            return

        track = Track(
            title=result["title"],
            duration=int(result.get("duration") or 0),
            duration_text=result["duration_text"],
            webpage_url=result["webpage_url"],
            thumbnail_url=result.get("thumbnail_url", ""),
            uploader=result.get("uploader", "Unknown Artist"),
            requested_by_id=query.from_user.id if query.from_user else 0,
            requested_by_name=display_name(query.from_user),
            source_id=result.get("id") or uuid.uuid4().hex[:8],
            original_query=payload.query,
        )

        start_now, position = await self.enqueue_track(payload.chat_id, track)
        self.search_cache.pop(search_id, None)

        text = (
            f"🎶 Selected: {track.title}\n"
            f"⏱ Duration: {track.duration_text}\n"
            f"{'▶️ Playing now...' if start_now else f'📋 Added to queue at position {position}.'}"
        )
        await self.safe_answer(query, "Track queued.")
        with contextlib.suppress(RPCError):
            await query.message.edit_text(text)

    async def handle_control_callback(self, query: CallbackQuery) -> None:
        if not await self.ensure_owner_callback(query):
            return

        parts = (query.data or "").split(":")
        if len(parts) != 3:
            await self.safe_answer(query, "Invalid control.", show_alert=True)
            return

        _, action, raw_chat_id = parts
        try:
            chat_id = int(raw_chat_id)
        except ValueError:
            await self.safe_answer(query, "Invalid chat target.", show_alert=True)
            return

        session = self.sessions.get(chat_id)
        if not session:
            await self.safe_answer(query, "No active player for this chat.", show_alert=True)
            return

        if action == "skip":
            session.skip_event.set()
            await self.safe_answer(query, "Skipping current track...")
        elif action == "stop":
            session.stop_event.set()
            session.skip_event.set()
            async with session.lock:
                session.queue.clear()
            await self.safe_answer(query, "Playback stopped.")
            await self.safe_send(chat_id, "❌ Playback stopped and queue cleared.")
        elif action == "queue":
            await self.safe_answer(query, "Queue sent.")
            await self.safe_send(chat_id, await self.build_queue_text(chat_id))
        elif action == "download":
            await self.safe_answer(query, "Sending current audio...")
            await self.resend_current_audio(chat_id)
        elif action == "lyrics":
            current = session.current
            await self.safe_answer(query, "Fetching lyrics...")
            if not current:
                await self.safe_send(chat_id, "📝 No active song for lyrics.")
                return
            await self.send_lyrics(chat_id, f"{current.uploader} - {current.title}")
        else:
            await self.safe_answer(query, "Unknown control.", show_alert=True)

    async def enqueue_track(self, chat_id: int, track: Track) -> Tuple[bool, int]:
        session = self.sessions.setdefault(chat_id, ChatSession())
        async with session.lock:
            start_now = session.current is None and not session.queue
            session.queue.append(track)
            position = len(session.queue)
            if session.worker_task is None or session.worker_task.done():
                session.worker_task = asyncio.create_task(self.player_worker(chat_id))
        return start_now, position

    async def player_worker(self, chat_id: int) -> None:
        session = self.sessions.setdefault(chat_id, ChatSession())
        stopped_manually = False

        while True:
            async with session.lock:
                if session.stop_event.is_set() and not session.current:
                    session.stop_event.clear()
                    session.queue.clear()
                    stopped_manually = True
                    break

                if session.current is None:
                    if not session.queue:
                        break
                    session.current = session.queue.pop(0)
                    session.skip_event.clear()
                    session.stop_event.clear()
                    track = session.current
                else:
                    track = session.current

            if not track:
                break

            try:
                await self.play_track(chat_id, track, session)
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("track playback failed for chat %s", chat_id)
                await self.safe_send(chat_id, f"⚠️ Failed to play: {track.title}\nSkipping to the next item.")
                async with session.lock:
                    session.current = None
                    session.skip_event.clear()
                    if session.stop_event.is_set():
                        session.stop_event.clear()
                        session.queue.clear()
                        stopped_manually = True
                        break
                continue

        async with session.lock:
            queue_empty = not session.queue
            session.current = None
            session.worker_task = None
            session.skip_event.clear()
            session.stop_event.clear()

        if queue_empty and not stopped_manually:
            await self.safe_send(chat_id, "✅ Queue finished.")

    async def play_track(self, chat_id: int, track: Track, session: ChatSession) -> None:
        if not track.file_path or not Path(track.file_path).exists():
            await self.safe_send(chat_id, f"⬇️ Downloading: {track.title}")
            track.file_path = await self.download_audio(track)
            await self.db.register_file(track.file_path)
            await self.cleanup_local_files()

        caption = (
            f"🎵 Title: {track.title}\n"
            f"⏱ Duration: {track.duration_text}\n"
            f"👤 Requested by: {track.requested_by_name}"
        )

        thumb_path = await self.download_thumbnail(track.thumbnail_url, track.source_id)
        send_kwargs: Dict[str, Any] = {
            "chat_id": chat_id,
            "audio": track.file_path,
            "caption": caption,
            "duration": track.duration or 0,
            "performer": track.uploader,
            "title": track.title,
            "reply_markup": self.player_controls(chat_id),
        }
        if thumb_path:
            send_kwargs["thumb"] = thumb_path

        sent: Optional[Message] = None
        try:
            sent = await self.app.send_audio(**send_kwargs)
        except RPCError:
            logging.exception("send_audio failed with thumb, retrying without thumb")
            send_kwargs.pop("thumb", None)
            sent = await self.app.send_audio(**send_kwargs)
        finally:
            if thumb_path:
                await asyncio.to_thread(self.remove_file, thumb_path)

        if sent and sent.audio:
            track.audio_file_id = sent.audio.file_id

        await self.db.increment_songs_played()
        outcome = await self.wait_for_track_end(track.duration or 180, session)

        async with session.lock:
            session.current = None
            if outcome == "stop":
                session.queue.clear()
                session.stop_event.clear()
                session.skip_event.clear()
            elif outcome == "skip":
                session.skip_event.clear()

        if outcome == "skip":
            await self.safe_send(chat_id, f"⏭️ Skipped: {track.title}")

    async def wait_for_track_end(self, duration: int, session: ChatSession) -> str:
        delay = max(int(duration), 1)
        sleep_task = asyncio.create_task(asyncio.sleep(delay))
        skip_task = asyncio.create_task(session.skip_event.wait())
        stop_task = asyncio.create_task(session.stop_event.wait())
        done, pending = await asyncio.wait(
            {sleep_task, skip_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*pending, return_exceptions=True)

        if stop_task in done and stop_task.result():
            return "stop"
        if skip_task in done and skip_task.result():
            return "skip"
        return "done"

    async def resend_current_audio(self, chat_id: int) -> None:
        session = self.sessions.get(chat_id)
        current = session.current if session else None
        if not current:
            await self.safe_send(chat_id, "📥 No current song to resend.")
            return

        source = current.audio_file_id or current.file_path
        if not source:
            await self.safe_send(chat_id, "📥 Audio source is unavailable.")
            return

        caption = (
            f"📥 Download\n"
            f"🎵 Title: {current.title}\n"
            f"⏱ Duration: {current.duration_text}\n"
            f"👤 Requested by: {current.requested_by_name}"
        )

        try:
            await self.app.send_audio(
                chat_id=chat_id,
                audio=source,
                caption=caption,
                duration=current.duration or 0,
                performer=current.uploader,
                title=current.title,
            )
        except RPCError:
            logging.exception("failed to resend audio")
            await self.safe_send(chat_id, "⚠️ Failed to resend the current audio.")

    async def build_queue_text(self, chat_id: int) -> str:
        session = self.sessions.get(chat_id)
        if not session:
            return "📋 Queue is empty."

        lines = ["📋 Current Queue"]
        if session.current:
            lines.append(f"Now: {session.current.title} ({session.current.duration_text})")
        else:
            lines.append("Now: Nothing playing")

        if session.queue:
            for index, track in enumerate(session.queue, start=1):
                lines.append(f"{index}. {track.title} ({track.duration_text})")
        else:
            lines.append("No songs queued.")
        return "\n".join(lines)

    def player_controls(self, chat_id: int) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("⏭️ Skip", callback_data=f"ctl:skip:{chat_id}"),
                    InlineKeyboardButton("❌ Stop", callback_data=f"ctl:stop:{chat_id}"),
                ],
                [
                    InlineKeyboardButton("📋 Queue", callback_data=f"ctl:queue:{chat_id}"),
                    InlineKeyboardButton("📥 Download", callback_data=f"ctl:download:{chat_id}"),
                ],
                [
                    InlineKeyboardButton("📝 Lyrics", callback_data=f"ctl:lyrics:{chat_id}"),
                ],
            ]
        )

    async def search_youtube(self, query: str) -> List[Dict[str, Any]]:
        try:
            return await asyncio.to_thread(self.search_youtube_sync, query)
        except Exception:
            logging.exception("youtube search failed for query: %s", query)
            return []

    def search_youtube_sync(self, query: str) -> List[Dict[str, Any]]:
        options = {
            "quiet": True,
            "skip_download": True,
            "noplaylist": True,
            "default_search": "ytsearch",
            "extract_flat": False,
            "socket_timeout": 20,
        }
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(f"ytsearch{SEARCH_LIMIT}:{query}", download=False)

        results: List[Dict[str, Any]] = []
        for entry in (info.get("entries") or [])[:SEARCH_LIMIT]:
            if not entry:
                continue
            duration = int(entry.get("duration") or 0)
            results.append(
                {
                    "id": entry.get("id") or "",
                    "title": entry.get("title") or "Unknown Title",
                    "duration": duration,
                    "duration_text": format_duration(duration),
                    "webpage_url": entry.get("webpage_url") or f"https://www.youtube.com/watch?v={entry.get('id')}",
                    "thumbnail_url": self.extract_thumbnail(entry),
                    "uploader": entry.get("uploader") or entry.get("channel") or "Unknown Artist",
                }
            )
        return results

    async def download_audio(self, track: Track) -> str:
        return await asyncio.to_thread(self.download_audio_sync, track)

    def download_audio_sync(self, track: Track) -> str:
        unique_prefix = f"{track.source_id}_{track.unique_id}"
        output_template = str(DOWNLOAD_DIR / f"{unique_prefix}.%(ext)s")
        options = {
            "format": "bestaudio[ext=m4a]/bestaudio[acodec!=none]/bestaudio/best",
            "quiet": True,
            "noplaylist": True,
            "outtmpl": output_template,
            "restrictfilenames": True,
            "socket_timeout": 30,
            "writethumbnail": False,
        }
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(track.webpage_url, download=True)
            requested = info.get("requested_downloads") or []
            if requested and requested[0].get("filepath"):
                return requested[0]["filepath"]
            if info.get("_filename"):
                return str(info["_filename"])

            ext = info.get("ext") or "m4a"
            fallback = DOWNLOAD_DIR / f"{unique_prefix}.{ext}"
            if fallback.exists():
                return str(fallback)

            matches = list(DOWNLOAD_DIR.glob(f"{unique_prefix}.*"))
            if matches:
                return str(matches[0])

        raise RuntimeError(f"Download failed for {track.title}")

    async def download_thumbnail(self, url: str, source_id: str) -> Optional[str]:
        if not url:
            return None
        try:
            return await asyncio.to_thread(self.download_thumbnail_sync, url, source_id)
        except Exception:
            logging.exception("thumbnail download failed")
            return None

    def download_thumbnail_sync(self, url: str, source_id: str) -> Optional[str]:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        thumb_path = DOWNLOAD_DIR / f"{source_id}_{uuid.uuid4().hex[:6]}.jpg"
        with thumb_path.open("wb") as handle:
            handle.write(response.content)
        return str(thumb_path)

    async def send_lyrics(
        self,
        chat_id: int,
        query: str,
        reply_to_message_id: Optional[int] = None,
    ) -> None:
        notice = await self.safe_send(
            chat_id,
            f"📝 Searching lyrics for: {query}",
            reply_to_message_id=reply_to_message_id,
        )
        title, artist, lyrics = await self.fetch_lyrics(query)
        if lyrics:
            text = f"📝 Lyrics: {title}\n👤 Artist: {artist}\n\n{lyrics}"
        else:
            text = "❌ Lyrics not found."

        if notice:
            with contextlib.suppress(RPCError):
                await notice.edit_text(text)
                return
        await self.send_long(chat_id, text, reply_to_message_id=reply_to_message_id)

    async def fetch_lyrics(self, query: str) -> Tuple[str, str, Optional[str]]:
        try:
            return await asyncio.to_thread(self.fetch_lyrics_sync, query)
        except Exception:
            logging.exception("lyrics fetch failed for query: %s", query)
            return query, "Unknown", None

    def fetch_lyrics_sync(self, query: str) -> Tuple[str, str, Optional[str]]:
        artist = ""
        title = query

        if " - " in query:
            artist, title = [part.strip() for part in query.split(" - ", 1)]

        if not artist:
            suggest_url = f"https://api.lyrics.ovh/suggest/{quote(query)}"
            suggest_response = requests.get(suggest_url, timeout=20)
            suggest_response.raise_for_status()
            suggestions = suggest_response.json().get("data") or []
            if not suggestions:
                return query, "Unknown", None
            artist = suggestions[0].get("artist", {}).get("name", "") or "Unknown"
            title = suggestions[0].get("title", title)

        lyrics_url = f"https://api.lyrics.ovh/v1/{quote(artist)}/{quote(title)}"
        lyrics_response = requests.get(lyrics_url, timeout=20)
        lyrics_response.raise_for_status()
        lyrics = lyrics_response.json().get("lyrics")
        if not lyrics:
            return title, artist, None
        return title, artist, lyrics.strip()

    def cleanup_expired_searches(self) -> None:
        now = time.time()
        expired = [key for key, item in self.search_cache.items() if now - item.created_at > SEARCH_TTL]
        for key in expired:
            self.search_cache.pop(key, None)

    async def cleanup_local_files(self) -> None:
        protected = self.collect_protected_files()
        removable = await self.db.cleanup_file_index(protected, MAX_LOCAL_FILES)
        for path in removable:
            await asyncio.to_thread(self.remove_file, path)

    def collect_protected_files(self) -> Set[str]:
        protected: Set[str] = set()
        for session in self.sessions.values():
            if session.current and session.current.file_path:
                protected.add(session.current.file_path)
            for track in session.queue:
                if track.file_path:
                    protected.add(track.file_path)
        return protected

    @staticmethod
    def extract_thumbnail(entry: Dict[str, Any]) -> str:
        if entry.get("thumbnail"):
            return str(entry["thumbnail"])
        thumbnails = entry.get("thumbnails") or []
        if thumbnails:
            return str(thumbnails[-1].get("url") or "")
        return ""

    @staticmethod
    def remove_file(path: str) -> None:
        with contextlib.suppress(OSError):
            target = Path(path)
            if target.exists():
                target.unlink()


def main() -> None:
    validate_config()
    setup_logging()
    bot = MusicBot()
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
