#!/usr/bin/env python3
"""
Neptun NFT Profile Parser
Только поиск по профилю: владельцы с NFT в профиле и 0 лотов на маркете.
"""

import asyncio
import logging
import os
import random
import re
import sqlite3
import time
from contextvars import ContextVar
from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telethon import TelegramClient
from telethon.errors import (
    AuthKeyDuplicatedError,
    AuthKeyError,
    AuthKeyUnregisteredError,
    FloodWaitError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.payments import GetSavedStarGiftsRequest, GetStarGiftsRequest

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_ID = 36101343
API_HASH = "116195fa5e0459d25a9a6266b40807d7"
BOT_TOKEN = "8790434095:AAG5eA6OzMcC2-VdLeTeITahdUi_6KiIRiw"
OWNER_ID = 7186944876

ROOT = os.path.dirname(os.path.abspath(__file__)) or "."
DATA_DIR = os.path.join(ROOT, "data")
SESSION_FILE = os.path.join(DATA_DIR, "session", "nft_session")
SESSION_STRING = os.path.join(ROOT, "telethon_auth.string")
DB_PATH = os.path.join(DATA_DIR, f"user_{OWNER_ID}", "gifts.db")
LOCK_PATH = os.path.join(ROOT, "parser.lock")

REQUEST_GAP = float(os.environ.get("TG_REQUEST_GAP", "2.0"))
DEFAULT_LIMIT = 30
DEFAULT_MIN_GIFTS = 1

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("parser")

os.makedirs(os.path.dirname(SESSION_FILE), exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ── STATE ─────────────────────────────────────────────────────────────────────
is_searching = False
ALL_COLLECTIONS = []  # [(gift_id, title), ...]
_lock_fp = None
_api_lock = None
_last_api = 0.0
_db = None
_seen_owners = set()
_seen_gifts = set()
_current_uid: ContextVar = ContextVar("uid", default=None)

# ── HELPERS ───────────────────────────────────────────────────────────────────
def esc(t):
    return str(t or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _is_owner(uid) -> bool:
    try:
        return int(uid) == int(OWNER_ID)
    except Exception:
        return False


DENY = (
    "<b>❌ ОШИБКА</b>\n"
    "Парсер только для владельца.\n"
    "Твой ID: <code>{uid}</code>\n"
    "<b>Доступ запрещён.</b>"
)


class OwnerOnly(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user is None or not _is_owner(user.id):
            uid = getattr(user, "id", 0) if user else 0
            try:
                if isinstance(event, Message):
                    await event.answer(DENY.format(uid=uid), parse_mode="HTML")
                elif isinstance(event, CallbackQuery):
                    await event.answer("❌ Доступ запрещён", show_alert=True)
            except Exception:
                pass
            return None
        tok = _current_uid.set(int(user.id))
        try:
            return await handler(event, data)
        finally:
            _current_uid.reset(tok)


def acquire_lock():
    global _lock_fp
    _lock_fp = open(LOCK_PATH, "a+")
    try:
        import fcntl
        fcntl.flock(_lock_fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("ERROR: parser already running. Stop the other instance.")
    except ImportError:
        pass
    _lock_fp.seek(0)
    _lock_fp.truncate()
    _lock_fp.write(str(os.getpid()))
    _lock_fp.flush()
    log.info("lock ok pid=%s", os.getpid())


# ── TELETHON SESSION ──────────────────────────────────────────────────────────
def _read_session():
    for key in ("TELETHON_SESSION", "TG_SESSION"):
        v = (os.environ.get(key) or "").strip()
        if len(v) > 20:
            return v
    if os.path.isfile(SESSION_STRING):
        with open(SESSION_STRING, "r", encoding="utf-8") as f:
            v = f.read().strip()
        if len(v) > 20:
            return v
    return None


def _write_session(s: str):
    s = (s or "").strip()
    if len(s) < 20:
        return
    tmp = SESSION_STRING + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(s)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, SESSION_STRING)


_sess = _read_session()
tg = TelegramClient(
    StringSession(_sess) if _sess else SESSION_FILE,
    API_ID,
    API_HASH,
    connection_retries=5,
    request_retries=2,
    flood_sleep_threshold=20,
    retry_delay=2,
)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
dp.message.middleware(OwnerOnly())
dp.callback_query.middleware(OwnerOnly())


def _get_api_lock():
    global _api_lock
    if _api_lock is None:
        _api_lock = asyncio.Lock()
    return _api_lock


async def tg_call(req):
    """Пауза перед каждым RPC — иначе Telegram сносит сессию."""
    global _last_api
    async with _get_api_lock():
        gap = REQUEST_GAP - (time.time() - _last_api)
        if gap > 0:
            await asyncio.sleep(gap)
        try:
            return await tg(req)
        except FloodWaitError as e:
            wait = min(int(getattr(e, "seconds", 30) or 30), 300)
            log.warning("FloodWait %ss", wait)
            await asyncio.sleep(wait)
            raise
        finally:
            _last_api = time.time()


async def tg_do(name, *args, **kwargs):
    global _last_api
    async with _get_api_lock():
        gap = REQUEST_GAP - (time.time() - _last_api)
        if gap > 0:
            await asyncio.sleep(gap)
        try:
            return await getattr(tg, name)(*args, **kwargs)
        except FloodWaitError as e:
            wait = min(int(getattr(e, "seconds", 30) or 30), 300)
            log.warning("FloodWait %ss", wait)
            await asyncio.sleep(wait)
            raise
        finally:
            _last_api = time.time()


def persist_session():
    try:
        tg.session.save()
    except Exception:
        pass
    try:
        raw = tg.session
        if isinstance(raw, StringSession):
            _write_session(raw.save())
        else:
            ss = StringSession()
            try:
                ss.set_dc(raw.dc_id, raw.server_address, raw.port)
                if getattr(raw, "auth_key", None):
                    ss.auth_key = raw.auth_key
            except Exception:
                pass
            _write_session(ss.save())
    except Exception as e:
        log.warning("persist session: %s", e)


async def ensure_connected() -> bool:
    try:
        if not tg.is_connected():
            await tg.connect()
        if await tg.is_user_authorized():
            await tg_do("get_me")
            persist_session()
            return True
        return False
    except (AuthKeyDuplicatedError, AuthKeyUnregisteredError, AuthKeyError) as e:
        log.error("session dead: %s", e)
        try:
            await bot.send_message(
                OWNER_ID,
                "<b>❌ Сессия убита Telegram</b>\n"
                "Оставь ОДИН процесс и заново /admin → авторизация.",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return False
    except Exception as e:
        log.warning("ensure_connected: %s", e)
        return False


# ── DB ────────────────────────────────────────────────────────────────────────
def db():
    global _db
    if _db is not None:
        return _db
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _db = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    _db.execute("PRAGMA journal_mode=WAL")
    _db.executescript(
        """
        CREATE TABLE IF NOT EXISTS owners (
            username TEXT PRIMARY KEY,
            uid INTEGER,
            name TEXT,
            ts INTEGER
        );
        CREATE TABLE IF NOT EXISTS seen (
            key TEXT PRIMARY KEY,
            ts INTEGER
        );
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT
        );
        """
    )
    _db.commit()
    return _db


def load_seen():
    global _seen_owners, _seen_gifts
    c = db()
    _seen_owners = {r[0] for r in c.execute("SELECT key FROM seen WHERE key LIKE 'u:%' OR key LIKE 'id:%'")}
    _seen_gifts = {r[0] for r in c.execute("SELECT key FROM seen WHERE key LIKE 'g:%'")}
    # also load plain
    for r in c.execute("SELECT key FROM seen"):
        k = r[0]
        if k.startswith("g:"):
            _seen_gifts.add(k[2:])
        else:
            _seen_owners.add(k)


def mark_seen(uid=None, username=None, nft_url=None):
    c = db()
    now = int(time.time())
    keys = []
    if uid:
        keys.append(f"id:{int(uid)}")
    if username:
        keys.append(f"u:{username.lstrip('@').lower()}")
    for k in keys:
        _seen_owners.add(k)
        c.execute("INSERT OR IGNORE INTO seen(key,ts) VALUES(?,?)", (k, now))
    if nft_url:
        slug = nft_url.rstrip("/").split("/")[-1]
        if slug:
            _seen_gifts.add(slug)
            c.execute("INSERT OR IGNORE INTO seen(key,ts) VALUES(?,?)", (f"g:{slug}", now))
    c.commit()


def is_seen(uid=None, username=None) -> bool:
    if uid and f"id:{int(uid)}" in _seen_owners:
        return True
    if username and f"u:{username.lstrip('@').lower()}" in _seen_owners:
        return True
    return False


def add_owner(username, uid=None, name=None):
    if not username:
        return
    u = username.lstrip("@").lower()
    db().execute(
        "INSERT OR REPLACE INTO owners(username, uid, name, ts) VALUES(?,?,?,?)",
        (u, uid, name or u, int(time.time())),
    )
    db().commit()


def random_owners(limit=300):
    rows = db().execute(
        "SELECT username, uid, name FROM owners ORDER BY RANDOM() LIMIT ?",
        (limit,),
    ).fetchall()
    return [{"username": r[0], "uid": r[1], "name": r[2]} for r in rows]


def db_stats():
    c = db()
    n = c.execute("SELECT COUNT(*) FROM owners").fetchone()[0]
    s = c.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
    return {"owners": n, "seen": s}


# ── GIRL FILTER (простой) ─────────────────────────────────────────────────────
GIRL_NAMES = {
    "анна", "аня", "мария", "маша", "елена", "лена", "ольга", "оля", "наталья", "настя",
    "татьяна", "таня", "ирина", "юлия", "юля", "алина", "виктория", "вика", "дарья", "даша",
    "полина", "ксения", "ксюша", "валерия", "лера", "александра", "саша", "диана", "кристина",
    "светлана", "милана", "арина", "карина", "софия", "софья", "соня", "ева", "кира", "яна",
    "вероника", "алиса", "злата", "елизавета", "лиза", "ульяна", "варвара", "марина",
    "anna", "maria", "elena", "olga", "natasha", "tanya", "irina", "yulia", "julia",
    "alina", "victoria", "dasha", "masha", "vika", "polina", "ksenia", "diana", "sofia",
    "sophia", "kate", "katya", "lisa", "eva", "kira", "yana", "mila", "arina", "karina",
    "jessica", "emily", "olivia", "emma", "mia", "chloe", "lily", "ava", "sophia",
}
BOY_NAMES = {
    "александр", "алексей", "андрей", "антон", "артем", "артём", "дмитрий", "иван", "игорь",
    "илья", "кирилл", "максим", "михаил", "никита", "олег", "павел", "роман", "сергей",
    "тимур", "юрий", "егор", "денис", "вадим", "владимир", "дима", "ваня", "миша", "костя",
    "alex", "alexander", "andrey", "anton", "artem", "dmitry", "ivan", "igor", "ilya",
    "kirill", "maxim", "mikhail", "nikita", "oleg", "pavel", "roman", "sergey", "timur",
    "john", "mike", "david", "james", "robert", "daniel", "chris", "jack",
}


def is_girl(username=None, name=None) -> bool:
    raw = f"{username or ''} {name or ''}".lower()
    tokens = set(re.findall(r"[a-zа-яёіїєґ]+", raw))
    if tokens & BOY_NAMES and not (tokens & GIRL_NAMES):
        return False
    if tokens & GIRL_NAMES:
        return True
    # женские окончания
    for t in tokens:
        if len(t) >= 4 and t not in BOY_NAMES and t[-1] in ("a", "а", "я"):
            if t.endswith(("ina", "yna", "ella", "на", "ья", "ия", "ка", "ша")):
                return True
    if any(x in raw for x in ("girl", "lady", "девушка", "she/her", "♀", "💅", "🌸")):
        return True
    return False


# ── NFT HELPERS ───────────────────────────────────────────────────────────────
def _norm(s):
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def nft_url(gift) -> Optional[str]:
    inner = getattr(gift, "gift", None) or gift
    slug = getattr(gift, "slug", None) or getattr(inner, "slug", None)
    if slug:
        return f"https://t.me/nft/{slug}"
    return None


def on_market(gift) -> bool:
    inner = getattr(gift, "gift", None)
    for obj in (gift, inner):
        if obj is None:
            continue
        for fl in ("resale_ton_only", "on_sale", "for_sale"):
            if getattr(obj, fl, False):
                return True
        for attr in ("resell_amount", "resale_amount", "resale_stars", "resell_stars", "resell_price", "resale_price"):
            val = getattr(obj, attr, None)
            if not val:
                continue
            if isinstance(val, (list, tuple)) and len(val) > 0:
                return True
            amt = getattr(val, "amount", None)
            if amt is not None:
                try:
                    if int(amt) > 0:
                        return True
                except Exception:
                    return True
    return False


async def load_collections():
    global ALL_COLLECTIONS
    ALL_COLLECTIONS = []
    try:
        res = await tg_call(GetStarGiftsRequest(hash=0))
        seen = set()
        for g in (getattr(res, "gifts", None) or []):
            gid = getattr(g, "id", None)
            if gid is None or gid in seen:
                continue
            seen.add(gid)
            title = getattr(g, "title", None) or f"Gift #{gid}"
            ALL_COLLECTIONS.append((int(gid), str(title)))
        ALL_COLLECTIONS.sort(key=lambda x: x[1].lower())
        log.info("collections: %s", len(ALL_COLLECTIONS))
    except Exception as e:
        log.error("load_collections: %s", e)
    return ALL_COLLECTIONS


async def fetch_profile_gifts(peer, max_pages=5):
    """
    Гифты профиля.
    Возвращает None если есть хоть 1 на маркете.
    Иначе список скрытых (не на маркете) NFT.
    """
    try:
        entity = await tg_do("get_input_entity", peer)
    except Exception as e:
        log.debug("peer %s: %s", peer, e)
        return []

    items = []
    offset = ""
    for _ in range(max_pages):
        try:
            res = await tg_call(GetSavedStarGiftsRequest(
                peer=entity,
                offset=offset,
                limit=100,
                exclude_unlimited=True,
            ))
        except FloodWaitError:
            return None
        except Exception as e:
            log.debug("saved gifts: %s", e)
            break
        gifts = getattr(res, "gifts", None) or []
        if not gifts:
            break
        for gift in gifts:
            if on_market(gift):
                return None  # строго 0 на маркете
            url = nft_url(gift)
            inner = getattr(gift, "gift", None)
            title = getattr(inner, "title", None) or getattr(gift, "title", None) or "NFT"
            num = getattr(gift, "num", None) or getattr(inner, "num", None) or "?"
            if url:
                items.append({"title": str(title), "num": num, "nft_url": url})
        offset = getattr(res, "next_offset", "") or ""
        if not offset:
            break
    return items


# ── UI ────────────────────────────────────────────────────────────────────────
def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по профилю", callback_data="search")],
        [
            InlineKeyboardButton(text="👩 Только девушки", callback_data="search_girls"),
        ],
        [InlineKeyboardButton(text="📦 Собрать базу (маркет→юзеры)", callback_data="seed_db")],
        [InlineKeyboardButton(text="⚙️ Админ", callback_data="admin")],
    ])


def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ СТОП", callback_data="stop")],
    ])


def admin_kb():
    st = db_stats()
    ok = "да" if (tg.is_connected() and False) else "?"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔐 Авторизация TG", callback_data="auth")],
        [InlineKeyboardButton(text="🔄 Обновить коллекции", callback_data="reload_cols")],
        [InlineKeyboardButton(text="🧹 Сброс антидубля", callback_data="clear_seen")],
        [InlineKeyboardButton(text=f"📊 БД: {st['owners']} юзеров / seen {st['seen']}", callback_data="stats")],
        [InlineKeyboardButton(text="⬅️ Меню", callback_data="menu")],
    ])


# ── AUTH FSM ──────────────────────────────────────────────────────────────────
class Auth(StatesGroup):
    phone = State()
    code = State()
    password = State()


# ── HANDLERS ──────────────────────────────────────────────────────────────────
@dp.message(Command("start", "menu"))
async def cmd_start(message: Message, state: FSMContext):
    global is_searching
    is_searching = False
    await state.clear()
    st = db_stats()
    await message.answer(
        "<b>Neptun Profile Parser</b>\n\n"
        "Режим: <b>только профиль</b>\n"
        "Условие: <b>0 NFT на маркете</b>\n\n"
        f"В базе юзеров: <b>{st['owners']}</b>\n"
        f"Коллекций: <b>{len(ALL_COLLECTIONS)}</b>\n\n"
        "1) /admin → авторизация Telethon\n"
        "2) Собрать базу\n"
        "3) Поиск по профилю",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )


@dp.message(Command("clear"))
async def cmd_clear(message: Message, state: FSMContext):
    global is_searching
    is_searching = False
    await state.clear()
    await message.answer("<b>Остановлено</b>", parse_mode="HTML", reply_markup=main_kb())


@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"<b>ID: <code>{message.from_user.id}</code></b>", parse_mode="HTML")


@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    ok = await ensure_connected()
    await message.answer(
        f"<b>Админ</b>\nTelethon: {'✅' if ok else '❌ не авторизован'}\n"
        f"Коллекций: {len(ALL_COLLECTIONS)}\n"
        f"БД: {db_stats()}",
        parse_mode="HTML",
        reply_markup=admin_kb(),
    )


@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.answer()
    st = db_stats()
    await cb.message.answer(
        f"<b>Меню</b>\nБД: {st['owners']} юзеров | коллекций: {len(ALL_COLLECTIONS)}",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )


@dp.callback_query(F.data == "admin")
async def cb_admin(cb: CallbackQuery):
    await cb.answer()
    ok = await ensure_connected()
    await cb.message.answer(
        f"<b>Админ</b>\nTelethon: {'✅' if ok else '❌'}\nБД: {db_stats()}",
        parse_mode="HTML",
        reply_markup=admin_kb(),
    )


@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    await cb.answer()
    st = db_stats()
    await cb.message.answer(
        f"<b>Статистика</b>\nЮзеров в БД: {st['owners']}\nSeen: {st['seen']}\n"
        f"Коллекций: {len(ALL_COLLECTIONS)}",
        parse_mode="HTML",
        reply_markup=admin_kb(),
    )


@dp.callback_query(F.data == "reload_cols")
async def cb_reload(cb: CallbackQuery):
    await cb.answer("Обновляю...")
    if not await ensure_connected():
        await cb.message.answer("<b>Сначала авторизуй Telethon</b>", parse_mode="HTML")
        return
    await load_collections()
    await cb.message.answer(
        f"<b>Коллекций: {len(ALL_COLLECTIONS)}</b>",
        parse_mode="HTML",
        reply_markup=admin_kb(),
    )


@dp.callback_query(F.data == "clear_seen")
async def cb_clear_seen(cb: CallbackQuery):
    global _seen_owners, _seen_gifts
    db().execute("DELETE FROM seen")
    db().commit()
    _seen_owners.clear()
    _seen_gifts.clear()
    await cb.answer("Сброшено")
    await cb.message.answer("<b>Антидубль очищен</b>", parse_mode="HTML", reply_markup=admin_kb())


@dp.callback_query(F.data == "stop")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    is_searching = False
    await cb.answer("Стоп")
    await cb.message.answer("<b>Поиск остановлен</b>", parse_mode="HTML", reply_markup=main_kb())


# ── AUTH ──────────────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "auth")
async def cb_auth(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await state.set_state(Auth.phone)
    await cb.message.answer(
        "<b>Введи номер телефона</b>\nФормат: <code>+79001234567</code>",
        parse_mode="HTML",
    )


@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    phone = (message.text or "").strip()
    if not phone.startswith("+"):
        await message.answer("<b>Формат: +79001234567</b>", parse_mode="HTML")
        return
    try:
        if not tg.is_connected():
            await tg.connect()
        # свежая сессия если старая убита
        try:
            if not await tg.is_user_authorized():
                pass
        except Exception:
            tg.session = StringSession()
            await tg.connect()
        res = await tg.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=res.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("<b>Код отправлен. Введи код:</b>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"<b>Ошибка:</b> <code>{esc(e)}</code>", parse_mode="HTML")
        await state.clear()


@dp.message(Auth.code)
async def auth_code(message: Message, state: FSMContext):
    code = (message.text or "").replace(" ", "").strip()
    data = await state.get_data()
    try:
        await tg.sign_in(phone=data["phone"], code=code, phone_code_hash=data["phone_code_hash"])
        persist_session()
        me = await tg_do("get_me")
        await state.clear()
        await load_collections()
        await message.answer(
            f"<b>✅ Авторизован как @{esc(me.username or me.first_name)}</b>\n"
            f"Коллекций: {len(ALL_COLLECTIONS)}\n"
            "Сессия сохранена в <code>telethon_auth.string</code>",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("<b>Введи пароль 2FA:</b>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"<b>Ошибка:</b> <code>{esc(e)}</code>", parse_mode="HTML")


@dp.message(Auth.password)
async def auth_password(message: Message, state: FSMContext):
    try:
        await tg.sign_in(password=(message.text or "").strip())
        persist_session()
        me = await tg_do("get_me")
        await state.clear()
        await load_collections()
        await message.answer(
            f"<b>✅ Авторизован как @{esc(me.username or me.first_name)}</b>\n"
            f"Коллекций: {len(ALL_COLLECTIONS)}",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
    except Exception as e:
        await message.answer(f"<b>Неверный пароль:</b> <code>{esc(e)}</code>", parse_mode="HTML")


# ── SEED DB FROM MARKET (лёгкий, только чтобы были юзеры для профиля) ─────────
@dp.callback_query(F.data == "seed_db")
async def cb_seed(cb: CallbackQuery):
    global is_searching
    if is_searching:
        await cb.answer("Уже идёт работа", show_alert=True)
        return
    if not await ensure_connected():
        await cb.answer("Сначала авторизация", show_alert=True)
        return
    if not ALL_COLLECTIONS:
        await load_collections()
    await cb.answer("Собираю...")
    is_searching = True
    status = await cb.message.answer(
        "<b>📦 Собираю юзеров с маркета в БД...</b>\n(нужны как кандидаты для профиля)",
        parse_mode="HTML",
        reply_markup=stop_kb(),
    )
    added = 0
    try:
        from telethon.tl.functions.payments import GetResaleStarGiftsRequest
        cols = list(ALL_COLLECTIONS)
        random.shuffle(cols)
        for i, (gid, title) in enumerate(cols[:80]):
            if not is_searching:
                break
            try:
                res = await tg_call(GetResaleStarGiftsRequest(
                    gift_id=gid, offset="", limit=50,
                ))
            except Exception:
                continue
            users = {int(u.id): u for u in (getattr(res, "users", None) or [])}
            for gift in (getattr(res, "gifts", None) or []):
                oid = getattr(gift, "owner_id", None)
                if hasattr(oid, "user_id"):
                    oid = oid.user_id
                owner = users.get(int(oid)) if oid else None
                uname = getattr(owner, "username", None) if owner else None
                if not uname:
                    continue
                fn = (getattr(owner, "first_name", "") or "")
                ln = (getattr(owner, "last_name", "") or "")
                add_owner(uname, uid=int(oid) if oid else None, name=(fn + " " + ln).strip())
                added += 1
            if i % 10 == 0:
                try:
                    await status.edit_text(
                        f"<b>📦 База:</b> +{added} | кол. {i}/{min(80, len(cols))}\n"
                        f"Всего в БД: {db_stats()['owners']}",
                        parse_mode="HTML",
                        reply_markup=stop_kb(),
                    )
                except Exception:
                    pass
    finally:
        is_searching = False
    await status.edit_text(
        f"<b>✅ Готово</b>\nДобавлено проходов: {added}\nВ БД юзеров: {db_stats()['owners']}",
        parse_mode="HTML",
        reply_markup=main_kb(),
    )


# ── PROFILE SEARCH ────────────────────────────────────────────────────────────
async def run_profile_search(status: Message, girls_only=False, limit=DEFAULT_LIMIT, min_gifts=DEFAULT_MIN_GIFTS):
    global is_searching
    is_searching = True
    found = 0
    checked = 0
    try:
        cands = random_owners(limit=max(limit * 20, 400))
        random.shuffle(cands)
        if not cands:
            await status.edit_text(
                "<b>БД пустая.</b>\nСначала нажми «Собрать базу».",
                parse_mode="HTML",
                reply_markup=main_kb(),
            )
            return 0

        await status.edit_text(
            f"<b>🔍 Профиль / {'девушки' if girls_only else 'все'}</b>\n"
            f"Кандидатов: {len(cands)}\n"
            f"Условие: <b>0 NFT на маркете</b>\n"
            f"Лимит: {limit}",
            parse_mode="HTML",
            reply_markup=stop_kb(),
        )

        for h in cands:
            if not is_searching or found >= limit:
                break
            uname = h.get("username")
            uid = h.get("uid")
            name = h.get("name") or uname or ""
            if not uname and not uid:
                continue
            if is_seen(uid, uname):
                continue
            if girls_only and not is_girl(uname, name):
                continue

            checked += 1
            peer = uid or uname
            gifts = await fetch_profile_gifts(peer, max_pages=5)
            if gifts is None:
                continue  # есть на маркете
            if not gifts or len(gifts) < min_gifts:
                continue

            # антидубль гифтов
            fresh = [g for g in gifts if g["nft_url"].rstrip("/").split("/")[-1] not in _seen_gifts]
            if not fresh:
                continue

            found += 1
            mark_seen(uid, uname, fresh[0]["nft_url"])

            lines = []
            for g in fresh[:5]:
                lines.append(f'\n<a href="{g["nft_url"]}">{esc(g["title"])} #{esc(g["num"])}</a>')
            owner_s = f"@{esc(uname)}" if uname else f"id:{uid}"
            if name and name != uname:
                owner_s += f" · {esc(name)}"
            txt = (
                f"<b>{owner_s}\n"
                f"NFT в профиле (0 на маркете): {len(fresh)}</b>"
                + "".join(lines)
            )
            kb_rows = []
            if uname:
                kb_rows.append([InlineKeyboardButton(text=f"@{uname}", url=f"https://t.me/{uname}")])
            kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
            try:
                await status.bot.send_message(
                    status.chat.id, txt, parse_mode="HTML",
                    reply_markup=kb, disable_web_page_preview=True,
                )
            except Exception as e:
                log.warning("send: %s", e)
                found -= 1

            if checked % 15 == 0:
                try:
                    await status.edit_text(
                        f"<b>🔍 Профиль...</b> найдено {found}/{limit} | проверено {checked}",
                        parse_mode="HTML",
                        reply_markup=stop_kb(),
                    )
                except Exception:
                    pass

        return found
    finally:
        is_searching = False


@dp.callback_query(F.data.in_({"search", "search_girls"}))
async def cb_search(cb: CallbackQuery):
    global is_searching
    if is_searching:
        await cb.answer("Уже идёт поиск", show_alert=True)
        return
    if not await ensure_connected():
        await cb.answer("Сначала /admin → авторизация", show_alert=True)
        return
    girls = cb.data == "search_girls"
    await cb.answer("Ищу...")
    status = await cb.message.answer(
        f"<b>🔍 Старт поиска по профилю{' / девушки' if girls else ''}...</b>",
        parse_mode="HTML",
        reply_markup=stop_kb(),
    )
    found = await run_profile_search(status, girls_only=girls)
    try:
        await status.edit_text(
            f"<b>✅ Готово</b>\nНайдено: {found}",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )
    except Exception:
        await cb.message.answer(
            f"<b>✅ Готово</b>\nНайдено: {found}",
            parse_mode="HTML",
            reply_markup=main_kb(),
        )


# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    acquire_lock()
    db()
    load_seen()
    await ensure_connected()
    log.info("Profile parser started owner=%s gap=%.1fs", OWNER_ID, REQUEST_GAP)
    if await tg.is_user_authorized():
        persist_session()
        await load_collections()
        log.info("authorized, collections=%s db=%s", len(ALL_COLLECTIONS), db_stats())
    else:
        log.warning("not authorized — /admin")
    try:
        await dp.start_polling(bot)
    finally:
        try:
            persist_session()
        except Exception:
            pass
        try:
            await tg.disconnect()
        except Exception:
            pass
        try:
            await bot.session.close()
        except Exception:
            pass
        if _db is not None:
            try:
                _db.commit()
                _db.close()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
