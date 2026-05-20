import asyncio
import logging
import urllib.parse
import os
import json
import time
from telethon import TelegramClient
from telethon.tl.functions.payments import GetResaleStarGiftsRequest, GetStarGiftsRequest, GetSavedStarGiftsRequest
from telethon.tl.functions.users import GetUsersRequest
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ======================== ДАННЫЕ ========================
API_ID    = 28687552
API_HASH  = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN = "8406363273:AAF2L-LfRhUVMrbVLeZBLLI7IgkFoMtyfGM"
ADMIN_ID  = "8726084830"
SESSION_NAME = "nft_session"
USERS_FILE   = "users.json"
# ========================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot       = Bot(token=BOT_TOKEN)
dp        = Dispatcher(storage=MemoryStorage())
tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

stats        = {"checks": 0, "found": 0}
is_searching = False
ALL_GIFT_IDS     = []
NFT_COLLECTIONS  = {}
MARKET_CACHE: dict[int, int] = {}  # gift_id -> median price

PRICE_CATEGORIES = {
    "cheap": {
        "label": "Дешёвые",
        "desc": "до 2000 звёзд",
        "min": None, "max": 2000,
        "tol_min": None, "tol_max": 2300,
    },
    "mid": {
        "label": "Средние",
        "desc": "2000-5000 звёзд",
        "min": 2000, "max": 5000,
        "tol_min": 1500, "tol_max": 5500,
    },
    "hard": {
        "label": "Сложные",
        "desc": "5000-20000 звёзд",
        "min": 5000, "max": 20000,
        "tol_min": 4500, "tol_max": 21000,
    },
    "ultra": {
        "label": "Хард",
        "desc": "20000-100000 звёзд",
        "min": 20000, "max": 100000,
        "tol_min": 19000, "tol_max": 103000,
    },
    "extreme": {
        "label": "Экстрим",
        "desc": "от 100000 звёзд",
        "min": 100000, "max": None,
        "tol_min": 95000, "tol_max": None,
    },
}

GIRL_NAMES = {
    "анна","мария","екатерина","анастасия","наталья","ольга","елена","татьяна","ирина",
    "юлия","алина","виктория","дарья","полина","ксения","валерия","александра","надежда",
    "людмила","галина","лиза","диана","софья","софия","кристина","светлана","милана",
    "арина","вера","жанна","ангелина","карина","оксана","нина","лариса","регина",
    "anna","maria","kate","natasha","olga","elena","tatiana","irina","julia","alina",
    "victoria","dasha","polina","ksenia","valeria","alexandra","diana","sophia","sofia",
    "lisa","christina","sveta","milana","arina","vera","zhanna","angela","angelina",
    "karina","oksana","nina","larisa","regina","natalia","ekaterina","anastasia",
}
GIRL_KW = [
    'girl','lady','princess','queen','baby','cute','sweetie','babe','honey','cutie',
    'барби','принцесса','королева','девочка','красотка',
]


# ===================== USERS =====================
def load_users() -> dict:
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return {str(uid): {"username": None, "joined": 0} for uid in data}
            return data
    return {}

def save_users(u: dict):
    with open(USERS_FILE, "w") as f:
        json.dump(u, f, ensure_ascii=False, indent=2)

def add_user(uid: int, username: str = None, first_name: str = None, last_name: str = None):
    u = load_users()
    key = str(uid)
    if key not in u:
        u[key] = {
            "username":   username or "",
            "first_name": first_name or "",
            "last_name":  last_name or "",
            "joined":     int(time.time()),
        }
    else:
        if isinstance(u[key], dict):
            if username:
                u[key]["username"] = username
            if first_name:
                u[key]["first_name"] = first_name
            if last_name:
                u[key]["last_name"] = last_name
    save_users(u)

def get_user_count() -> int:
    return len(load_users())


# ===================== STATES =====================
class Auth(StatesGroup):
    phone    = State()
    code     = State()
    password = State()

class Broadcast(StatesGroup):
    message = State()


# ===================== HELPERS =====================
def is_admin(uid: int) -> bool:
    return int(uid) == int(ADMIN_ID)

async def check_authorized() -> bool:
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
        return await tg_client.is_user_authorized()
    except Exception:
        return False

def is_girl(owner) -> bool:
    if not owner:
        return False
    first = (getattr(owner, 'first_name', '') or '').lower().strip()
    last  = (getattr(owner, 'last_name',  '') or '').lower().strip()
    uname = (getattr(owner, 'username',   '') or '').lower().strip()
    full  = f"{first} {last} {uname}"
    for name in GIRL_NAMES:
        if first.startswith(name) or last.startswith(name) or uname.startswith(name):
            return True
    for kw in GIRL_KW:
        if kw in full:
            return True
    return False

def get_price(gift) -> int | None:
    ra = getattr(gift, 'resell_amount', None)
    if ra is not None:
        lst = ra if isinstance(ra, (list, tuple)) else [ra]
        for item in lst:
            a = getattr(item, 'amount', None)
            if a is not None:
                try:
                    v = int(a)
                    if 0 < v < 100_000_000:
                        return v
                except Exception:
                    pass
            try:
                v = int(item)
                if 0 < v < 100_000_000:
                    return v
            except Exception:
                pass
    for field in ['convert_stars','stars','star_count','cost','amount']:
        val = getattr(gift, field, None)
        if val is not None:
            try:
                v = int(val)
                if 0 < v < 100_000_000:
                    return v
            except Exception:
                pass
    return None

def get_owner(gift, users_map: dict):
    obj = getattr(gift, 'owner_id', None)
    if obj is None:
        return None, None
    uid = getattr(obj, 'user_id', None) or getattr(obj, 'id', None)
    if uid is None and isinstance(obj, int):
        uid = obj
    if uid is None:
        return None, None
    uid = int(uid)
    return users_map.get(uid), uid

def fmt_owner(owner, username, name) -> str:
    if name and username:
        return f"{name} (@{username})"
    if username:
        return f"@{username}"
    if name:
        return name
    return "Скрыт"

def price_in_category(price: int, cat: str) -> bool:
    if cat not in PRICE_CATEGORIES:
        return True
    c = PRICE_CATEGORIES[cat]
    tmin = c["tol_min"]
    tmax = c["tol_max"]
    if tmin is not None and price < tmin:
        return False
    if tmax is not None and price > tmax:
        return False
    return True

def fmt_timestamp(ts: int) -> str:
    if not ts:
        return "неизвестно"
    import datetime
    dt = datetime.datetime.fromtimestamp(ts)
    return dt.strftime("%d.%m.%Y %H:%M")


# ===================== MARKET PRICE (медиана коллекции) =====================
async def get_median_price(gift_id: int) -> int | None:
    if gift_id in MARKET_CACHE:
        return MARKET_CACHE[gift_id]
    try:
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gift_id, offset="", limit=50,
        ))
        gifts  = getattr(result, 'gifts', None) or []
        prices = sorted([p for g in gifts if (p := get_price(g)) and p > 0])
        if not prices:
            return None
        median = prices[len(prices) // 2]
        MARKET_CACHE[gift_id] = median
        return median
    except Exception as e:
        logger.error(f"get_median_price gid={gift_id}: {e}")
        return None


# ===================== COLLECTIONS =====================
async def load_collections():
    global ALL_GIFT_IDS, NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        ALL_GIFT_IDS    = []
        NFT_COLLECTIONS = {}
        for gift in result.gifts:
            gid   = getattr(gift, 'id',    None)
            title = getattr(gift, 'title', None)
            if gid is None:
                continue
            label = title or f"Gift #{gid}"
            ALL_GIFT_IDS.append((gid, label))
            if title:
                NFT_COLLECTIONS[title] = gid
        logger.info(f"Коллекций: {len(ALL_GIFT_IDS)}")
    except Exception as e:
        logger.error(f"load_collections: {e}")


# ===================== FETCH PAGE (РЫНОК — resale) =====================
async def fetch_market_page(gift_id: int, offset: str, limit: int = 100) -> tuple[list, str]:
    """Гифты выставленные на продажу. Цена берётся из resell_amount."""
    try:
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gift_id, offset=offset, limit=limit,
        ))
        users_map = {int(u.id): u for u in (getattr(result, 'users', None) or [])}
        gifts     = getattr(result, 'gifts', None) or []
        items     = []

        for gift in gifts:
            owner, owner_uid = get_owner(gift, users_map)
            username = getattr(owner, 'username', None) if owner else None
            fn = (getattr(owner, 'first_name', '') or '') if owner else ''
            ln = (getattr(owner, 'last_name',  '') or '') if owner else ''
            name  = f"{fn} {ln}".strip()
            title = getattr(gift, 'title', '?')
            slug  = (getattr(gift, 'slug', None)
                     or getattr(gift, 'unique_id', None)
                     or str(getattr(gift, 'num', '')))
            num   = getattr(gift, 'num', '?')

            # Реальная рыночная цена из resell_amount
            price = None
            ra = getattr(gift, 'resell_amount', None)
            if ra is not None:
                lst = ra if isinstance(ra, (list, tuple)) else [ra]
                for item_ra in lst:
                    a = getattr(item_ra, 'amount', None)
                    if a is not None:
                        try:
                            v = int(a)
                            if 0 < v < 100_000_000:
                                price = v
                                break
                        except Exception:
                            pass
                    try:
                        v = int(item_ra)
                        if 0 < v < 100_000_000:
                            price = v
                            break
                    except Exception:
                        pass

            nft_url = None
            if slug and slug not in ('None', '', 'nan', '0'):
                try:
                    int(slug)
                except ValueError:
                    nft_url = f"https://t.me/nft/{slug}"

            profile_url = None
            if username:
                profile_url = f"https://t.me/{username}"
            elif owner_uid:
                profile_url = f"tg://user?id={owner_uid}"

            items.append({
                "owner": owner, "owner_id": owner_uid,
                "username": username, "name": name,
                "title": title, "slug": slug, "num": num,
                "price": price, "nft_url": nft_url,
                "profile_url": profile_url,
                "gift_id": gift_id,
                "mode": "market",
            })

        next_offset = getattr(result, 'next_offset', "") or ""
        return items, next_offset

    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds}s")
        await asyncio.sleep(e.seconds + 2)
        return [], ""
    except Exception as e:
        logger.error(f"fetch_market_page gid={gift_id}: {e}")
        return [], ""


# ===================== FETCH PROFILE GIFTS (SavedStarGifts) =====================
async def fetch_saved_gifts_page(user_id: int, offset: str, limit: int = 50) -> tuple[list, str]:
    """
    Получаем сохранённые (не выставленные) гифты конкретного пользователя
    через GetSavedStarGiftsRequest. Это реальные гифты из профиля,
    не связанные с рынком.
    """
    try:
        result = await tg_client(GetSavedStarGiftsRequest(
            peer=await tg_client.get_input_entity(user_id),
            offset=offset,
            limit=limit,
        ))
        gifts = getattr(result, 'gifts', None) or []
        items = []

        for gift in gifts:
            inner  = getattr(gift, 'gift', None)
            title  = getattr(inner, 'title', None) or getattr(gift, 'title', '?')
            slug   = getattr(gift, 'slug', None) or getattr(gift, 'unique_id', None) or ''
            num    = getattr(gift, 'num', '?')
            # Оригинальная стоимость гифта (сколько заплатили при покупке)
            price  = None
            for field in ['convert_stars', 'stars', 'star_count']:
                val = getattr(inner, field, None) if inner else None
                if val is None:
                    val = getattr(gift, field, None)
                if val is not None:
                    try:
                        v = int(val)
                        if 0 < v < 100_000_000:
                            price = v
                            break
                    except Exception:
                        pass

            nft_url = None
            if slug and slug not in ('None', '', 'nan', '0'):
                try:
                    int(slug)
                except ValueError:
                    nft_url = f"https://t.me/nft/{slug}"

            items.append({
                "title": title, "slug": slug, "num": num,
                "price": price, "nft_url": nft_url,
                "gift_id": getattr(inner, 'id', None),
            })

        next_offset = getattr(result, 'next_offset', "") or ""
        return items, next_offset
    except Exception as e:
        logger.error(f"fetch_saved_gifts uid={user_id}: {e}")
        return [], ""


# ===================== KEYBOARDS =====================

# Главное меню
def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_menu")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
    ])

# Меню поиска — выбор режима
def search_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 По ценам",      callback_data="mode_price")],
        [InlineKeyboardButton(text="👧 Девушки",       callback_data="mode_girls")],
        [InlineKeyboardButton(text="🏪 По рынку",      callback_data="mode_market")],
        [InlineKeyboardButton(text="👤 С профиля",     callback_data="mode_profile")],
        [InlineKeyboardButton(text="🗂 По коллекции",  callback_data="mode_col")],
        [InlineKeyboardButton(text="◀️ Назад",         callback_data="menu")],
    ])

# По ценам — выбор диапазона (рынок + профиль вместе)
def price_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000)",       callback_data="price_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000-5000)",     callback_data="price_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000-20000)",    callback_data="price_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20000-100000)",     callback_data="price_ultra")],
        [InlineKeyboardButton(text="💀 Экстрим (100000+)",       callback_data="price_extreme")],
        [InlineKeyboardButton(text="◀️ Назад",                   callback_data="search_menu")],
    ])

# Девушки — выбор источника
def girls_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏪 Девушки на рынке",    callback_data="girls_market")],
        [InlineKeyboardButton(text="👤 Девушки в профилях",  callback_data="girls_profile")],
        [InlineKeyboardButton(text="◀️ Назад",               callback_data="search_menu")],
    ])

# По рынку — выбор фильтра
def market_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все NFT (2+ гифта)",      callback_data="mkt_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000)",       callback_data="mkt_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000-5000)",     callback_data="mkt_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000-20000)",    callback_data="mkt_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20000-100000)",     callback_data="mkt_ultra")],
        [InlineKeyboardButton(text="💀 Экстрим (100000+)",       callback_data="mkt_extreme")],
        [InlineKeyboardButton(text="◀️ Назад",                   callback_data="search_menu")],
    ])

# С профиля — выбор фильтра
def profile_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все профили (2+ гифта)",  callback_data="prf_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000)",       callback_data="prf_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000-5000)",     callback_data="prf_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000-20000)",    callback_data="prf_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20000-100000)",     callback_data="prf_ultra")],
        [InlineKeyboardButton(text="💀 Экстрим (100000+)",       callback_data="prf_extreme")],
        [InlineKeyboardButton(text="◀️ Назад",                   callback_data="search_menu")],
    ])

# По коллекции — выбор источника
def col_source_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏪 Рынок по коллекции",   callback_data="col_market")],
        [InlineKeyboardButton(text="👤 Профиль по коллекции", callback_data="col_profile")],
        [InlineKeyboardButton(text="◀️ Назад",                callback_data="search_menu")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ СТОП", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_menu")],
        [InlineKeyboardButton(text="🏠 Меню",  callback_data="menu")],
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка",       callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 Пользователи",   callback_data="admin_users")],
        [InlineKeyboardButton(text="📊 Статистика",     callback_data="admin_stats")],
        [InlineKeyboardButton(text="🔐 Авторизация TG", callback_data="admin_auth")],
        [InlineKeyboardButton(text="🚪 Выйти из TG",    callback_data="admin_logout")],
        [InlineKeyboardButton(text="◀️ В меню",         callback_data="menu")],
    ])

def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")],
    ])

def confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="admin_broadcast_confirm")],
        [InlineKeyboardButton(text="❌ Отмена",    callback_data="admin_cancel")],
    ])

def col_kb(names: list, prefix: str, back: str) -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(names), 2):
        row = [InlineKeyboardButton(text=names[i], callback_data=f"{prefix}{i}")]
        if i + 1 < len(names):
            row.append(InlineKeyboardButton(text=names[i+1], callback_data=f"{prefix}{i+1}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def owner_gifts_kb(items: list, username: str | None, profile_url: str | None) -> InlineKeyboardMarkup | None:
    """Клавиатура для блока владельца с несколькими гифтами"""
    btns = []
    if profile_url:
        label = f"👤 @{username}" if username else "👤 Профиль"
        btns.append([InlineKeyboardButton(text=label, url=profile_url)])
    if username:
        write_text = "Привет! Хочу купить твои NFT"
        btns.append([InlineKeyboardButton(
            text="✉️ Написать",
            url=f"https://t.me/{username}?text={urllib.parse.quote(write_text)}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

def nft_single_kb(item: dict) -> InlineKeyboardMarkup | None:
    """Клавиатура для одного гифта (поиск по цене)"""
    username    = item.get("username")
    nft_url     = item.get("nft_url")
    profile_url = item.get("profile_url")
    title       = item.get("title", "")
    num         = item.get("num", "")
    btns        = []
    if nft_url:
        btns.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=nft_url)])
    if profile_url:
        label = f"👤 @{username}" if username else "👤 Профиль"
        btns.append([InlineKeyboardButton(text=label, url=profile_url)])
    if username:
        write_text = f"Привет! Хочу купить твой NFT {nft_url or (title + ' #' + str(num))}"
        btns.append([InlineKeyboardButton(
            text="✉️ Написать",
            url=f"https://t.me/{username}?text={urllib.parse.quote(write_text)}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None


# ===================== CORE SEARCH — РЫНОК (ROUND-ROBIN) =====================
async def do_market_search(
    status_msg: Message,
    gift_ids: list[int],
    max_results: int = 100,
    girls_only: bool = False,
    cat: str | None = None,
) -> int:
    """
    Поиск гифтов на рынке.
    Режим "все NFT" (cat=None): показываем только владельцев у которых 2+ гифта на продаже,
    выводим все их гифты. NFT ссылка в тексте сообщения.
    Режим с категорией: обычная фильтрация по цене, показываем каждый гифт.
    Round-robin по коллекциям.
    """
    global is_searching
    is_searching = True
    found            = 0
    seen_slugs       = set()
    has_price_filter = cat is not None

    # Для режима "все NFT" — собираем гифты по владельцам
    # owner_id -> {"owner": obj, "username": str, "name": str, "profile_url": str, "items": []}
    owner_buckets: dict[int, dict] = {}
    # Для режима с ценой — просто показываем каждый подходящий гифт
    owner_count_cat: dict[int, int] = {}
    MAX_PER_OWNER_CAT = 3  # в режиме по цене — не более 3 с одного

    async def send_owner_block(owner_uid: int, bucket: dict):
        """Отправляет все гифты владельца одним или несколькими сообщениями"""
        nonlocal found
        items       = bucket["items"]
        owner_str   = fmt_owner(bucket["owner"], bucket["username"], bucket["name"])
        profile_url = bucket["profile_url"]
        username    = bucket["username"]

        # Формируем список ссылок на NFT
        nft_lines = []
        for it in items:
            title   = it.get("title", "?")
            num     = it.get("num", "?")
            price   = it.get("price")
            nft_url = it.get("nft_url")
            price_str = f"{price:,}".replace(",", " ") if price else "?"
            if nft_url:
                nft_lines.append(f'<a href="{nft_url}">{title} #{num}</a> - {price_str} звёзд')
            else:
                nft_lines.append(f"{title} #{num} - {price_str} звёзд")

        nft_block = "\n".join(nft_lines)

        btns = []
        if profile_url:
            label = f"@{username}" if username else "Профиль"
            btns.append([InlineKeyboardButton(text=label, url=profile_url)])
        if username:
            write_text = f"Привет! Хочу купить твои NFT"
            btns.append([InlineKeyboardButton(
                text="Написать",
                url=f"https://t.me/{username}?text={urllib.parse.quote(write_text)}"
            )])
        kb = owner_gifts_kb(items, username, profile_url)

        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id,
                text=(
                    f"<b>👤 {owner_str}</b>\n"
                    f"Гифтов на продаже: <b>{len(items)}</b>\n\n"
                    f"{nft_block}"
                ),
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            found += len(items)
            stats["found"] += len(items)
        except Exception as e:
            logger.warning(f"send_owner_block: {e}")

    async def send_nft_single(item: dict):
        """Отправляет один гифт с ценовым фильтром"""
        nonlocal found
        price     = item["price"]
        owner_str = fmt_owner(item["owner"], item["username"], item["name"])
        price_str = f"{price:,} звёзд".replace(",", " ") if price else "цена неизвестна"
        nft_url   = item.get("nft_url")
        title     = item.get("title", "?")
        num       = item.get("num", "?")

        nft_link = f'\n<a href="{nft_url}">{title} #{num}</a>' if nft_url else f"\n{title} #{num}"

        username    = item.get("username")
        profile_url = item.get("profile_url")
        btns = []
        if nft_url:
            btns.append([InlineKeyboardButton(text="Открыть NFT", url=nft_url)])
        if profile_url:
            label = f"@{username}" if username else "Профиль"
            btns.append([InlineKeyboardButton(text=label, url=profile_url)])
        if username:
            write_text = f"Привет! Хочу купить твой NFT {nft_url or title}"
            btns.append([InlineKeyboardButton(
                text="Написать",
                url=f"https://t.me/{username}?text={urllib.parse.quote(write_text)}"
            )])
        kb = nft_single_kb(item)

        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id,
                text=(
                    f"<b>👤 {owner_str}</b>"
                    f"{nft_link}\n"
                    f"💰 Цена: <b>{price_str}</b>"
                ),
                parse_mode="HTML",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
            found         += 1
            stats["found"] += 1
        except Exception as e:
            logger.warning(f"send_nft_single: {e}")

    try:
        offsets: dict[int, str | None] = {gid: "" for gid in gift_ids}
        buffers: dict[int, list]       = {gid: [] for gid in gift_ids}
        last_status_update = 0

        while is_searching and (found < max_results if has_price_filter else True):
            active_gids = [gid for gid, off in offsets.items() if off is not None or buffers[gid]]
            if not active_gids:
                break

            made_progress = False

            for gid in list(active_gids):
                if not is_searching:
                    break
                if has_price_filter and found >= max_results:
                    break

                # Подгружаем страницу если буфер пуст
                if not buffers[gid] and offsets.get(gid) is not None:
                    items, next_offset = await fetch_market_page(gid, offsets[gid], limit=50)
                    offsets[gid] = next_offset if next_offset else None

                    for item in items:
                        slug = item["slug"] or ""
                        if slug and slug in seen_slugs:
                            continue
                        if slug:
                            seen_slugs.add(slug)

                        if girls_only and not is_girl(item["owner"]):
                            continue

                        price = item.get("price")

                        if has_price_filter:
                            # Режим по цене: фильтруем и сразу в буфер
                            if price is not None and not price_in_category(price, cat):
                                continue
                            owner_id = item["owner_id"]
                            if owner_id:
                                cnt = owner_count_cat.get(owner_id, 0)
                                if cnt >= MAX_PER_OWNER_CAT:
                                    continue
                                owner_count_cat[owner_id] = cnt + 1
                            buffers[gid].append(item)
                        else:
                            # Режим "все NFT": группируем по владельцу
                            owner_id = item["owner_id"]
                            if owner_id:
                                if owner_id not in owner_buckets:
                                    profile_url = None
                                    username = item.get("username")
                                    if username:
                                        profile_url = f"https://t.me/{username}"
                                    elif owner_id:
                                        profile_url = f"tg://user?id={owner_id}"
                                    owner_buckets[owner_id] = {
                                        "owner": item["owner"],
                                        "username": username,
                                        "name": item["name"],
                                        "profile_url": profile_url,
                                        "items": [],
                                    }
                                owner_buckets[owner_id]["items"].append(item)

                if has_price_filter and buffers[gid]:
                    item = buffers[gid].pop(0)
                    made_progress = True
                    await send_nft_single(item)
                    await asyncio.sleep(0.05)

            # В режиме "все NFT" — после каждого прохода по всем коллекциям
            # отправляем владельцев у которых уже 2+ гифта
            if not has_price_filter:
                to_send = [
                    (uid, bucket) for uid, bucket in list(owner_buckets.items())
                    if len(bucket["items"]) >= 2
                ]
                for uid, bucket in to_send:
                    if not is_searching:
                        break
                    await send_owner_block(uid, bucket)
                    del owner_buckets[uid]
                    made_progress = True
                    await asyncio.sleep(0.1)

            now = asyncio.get_event_loop().time()
            if now - last_status_update > 3:
                try:
                    active_count = sum(1 for v in offsets.values() if v is not None)
                    lbl = "девушек" if girls_only else "NFT"
                    pending = sum(len(b["items"]) for b in owner_buckets.values())
                    await status_msg.edit_text(
                        f"Ищу на рынке... (коллекций: {active_count})\n"
                        f"Найдено {lbl}: <b>{found}</b>"
                        + (f"\nВ ожидании: {pending}" if not has_price_filter else ""),
                        parse_mode="HTML", reply_markup=stop_kb(),
                    )
                    last_status_update = now
                except Exception:
                    pass

            if not made_progress:
                break

        # В режиме "все NFT" — в конце отправляем оставшихся с 2+ гифтами
        if not has_price_filter and is_searching:
            for uid, bucket in list(owner_buckets.items()):
                if not is_searching:
                    break
                if len(bucket["items"]) >= 2:
                    await send_owner_block(uid, bucket)
                    await asyncio.sleep(0.1)

    except Exception as e:
        logger.error(f"do_market_search error: {e}")
    finally:
        is_searching = False

    return found


# ===================== CORE SEARCH — ПРОФИЛИ (SavedStarGifts) =====================
async def do_profile_search(
    status_msg: Message,
    gift_ids: list[int],
    max_results: int = 100,
    girls_only: bool = False,
    cat: str | None = None,
) -> int:
    """
    Поиск по профилям через SavedStarGifts.
    Сначала собираем владельцев с рынка (они публичны),
    затем для каждого владельца смотрим его сохранённые гифты
    (НЕ выставленные на продажу) через GetSavedStarGiftsRequest.
    Round-robin по коллекциям.
    """
    global is_searching
    is_searching = True
    found        = 0
    seen_slugs   = set()
    seen_owners  = set()   # уже обработанные владельцы
    owner_gift_count: dict[int, int] = {}
    MAX_PER_OWNER    = 2
    has_price_filter = cat is not None

    async def send_profile_nft(item: dict, owner, username: str | None,
                                name: str, owner_uid: int | None, market_price: int | None):
        owner_str   = fmt_owner(owner, username, name)
        price       = item.get("price")
        # Показываем рыночную медиану если нет собственной цены
        display_price = price or market_price
        price_str   = f"{display_price:,} звёзд".replace(",", " ") if display_price else "цена неизвестна"
        title       = item.get("title", "?")
        num         = item.get("num", "?")
        nft_url     = item.get("nft_url")

        profile_url = None
        if username:
            profile_url = f"https://t.me/{username}"
        elif owner_uid:
            profile_url = f"tg://user?id={owner_uid}"

        btns = []
        if nft_url:
            btns.append([InlineKeyboardButton(text="Открыть NFT", url=nft_url)])
        if profile_url:
            label = f"@{username}" if username else "Профиль"
            btns.append([InlineKeyboardButton(text=label, url=profile_url)])
            btns.append([InlineKeyboardButton(text="Все гифты", url=profile_url)])
        if username:
            write_text = f"Привет! Хочу купить твой NFT {title} #{num}"
            btns.append([InlineKeyboardButton(
                text="Написать",
                url=f"https://t.me/{username}?text={urllib.parse.quote(write_text)}"
            )])

        kb = InlineKeyboardMarkup(inline_keyboard=btns) if btns else None
        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id,
                text=(
                    f"<b>{title} #{num}</b>\n"
                    f"Владелец: {owner_str}\n"
                    f"Цена: {price_str} (в профиле, не продаётся)"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
        except Exception as e:
            logger.warning(f"send_profile: {e}")

    try:
        # Шаг 1: фильтруем коллекции по медианной цене (если нужна категория)
        filtered_ids = list(gift_ids)
        if cat:
            c = PRICE_CATEGORIES[cat]
            tmin, tmax = c["tol_min"], c["tol_max"]
            checked = []
            for i in range(0, len(filtered_ids), 5):
                if not is_searching:
                    break
                batch = filtered_ids[i:i+5]
                results = await asyncio.gather(*[get_median_price(gid) for gid in batch])
                for gid, median in zip(batch, results):
                    if median is None:
                        continue
                    if tmin is not None and median < tmin:
                        continue
                    if tmax is not None and median > tmax:
                        continue
                    checked.append(gid)
                await asyncio.sleep(0.2)
            filtered_ids = checked
            if not filtered_ids:
                is_searching = False
                return 0

        # Шаг 2: собираем владельцев с рынка (round-robin по коллекциям)
        # Буфер: owner_uid -> (owner_obj, username, name, [gift_ids_of_interest])
        owner_queue: list[tuple] = []  # (owner_uid, owner_obj, username, name, gift_id)
        offsets: dict[int, str | None] = {gid: "" for gid in filtered_ids}
        owner_seen_market: set[int] = set()
        last_status_update = 0

        # Сначала наполняем очередь владельцев, затем запрашиваем их профили
        OWNER_BATCH = 30  # сколько владельцев собрать прежде чем начать обход профилей

        async def collect_owners_round(n: int) -> bool:
            """Собрать n владельцев из рынка (round-robin). Возвращает True если что-то собрали."""
            collected = 0
            active_gids = [g for g, o in offsets.items() if o is not None]
            if not active_gids:
                return False

            for gid in active_gids:
                if collected >= n:
                    break
                if offsets.get(gid) is None:
                    continue
                items, next_offset = await fetch_market_page(gid, offsets[gid], limit=50)
                offsets[gid] = next_offset if next_offset else None

                for item in items:
                    owner_uid = item.get("owner_id")
                    if not owner_uid or owner_uid in owner_seen_market:
                        continue
                    if girls_only and not is_girl(item["owner"]):
                        continue
                    owner_seen_market.add(owner_uid)
                    owner_queue.append((
                        owner_uid,
                        item["owner"],
                        item["username"],
                        item["name"],
                        gid,
                    ))
                    collected += 1

            return collected > 0

        # Основной цикл
        while is_searching and found < max_results:
            # Собираем пачку владельцев
            if len(owner_queue) < 5:
                got = await collect_owners_round(OWNER_BATCH)
                if not got and not owner_queue:
                    break

            if not owner_queue:
                break

            # Обрабатываем владельцев из очереди
            batch_owners = owner_queue[:5]
            owner_queue  = owner_queue[5:]

            for (owner_uid, owner_obj, username, name, source_gid) in batch_owners:
                if not is_searching or found >= max_results:
                    break
                if owner_uid in seen_owners:
                    continue
                seen_owners.add(owner_uid)

                # Запрашиваем сохранённые гифты пользователя
                saved_offset = ""
                pages_fetched = 0
                MAX_PAGES = 3  # не более 3 страниц на владельца

                while is_searching and found < max_results and pages_fetched < MAX_PAGES:
                    saved_items, saved_next = await fetch_saved_gifts_page(owner_uid, saved_offset, limit=50)
                    pages_fetched += 1
                    saved_offset = saved_next

                    market_price = MARKET_CACHE.get(source_gid)

                    for gift_item in saved_items:
                        if not is_searching or found >= max_results:
                            break

                        slug = gift_item.get("slug") or ""
                        if slug and slug in seen_slugs:
                            continue
                        if slug:
                            seen_slugs.add(slug)

                        cnt = owner_gift_count.get(owner_uid, 0)
                        if cnt >= MAX_PER_OWNER:
                            break
                        owner_gift_count[owner_uid] = cnt + 1

                        price = gift_item.get("price")
                        if cat and price is not None:
                            if not price_in_category(price, cat):
                                continue

                        found         += 1
                        stats["found"] += 1
                        await send_profile_nft(gift_item, owner_obj, username, name, owner_uid, market_price)
                        await asyncio.sleep(0.05)

                    if not saved_next:
                        break

                await asyncio.sleep(0.1)

            now = asyncio.get_event_loop().time()
            if now - last_status_update > 3:
                try:
                    active_count = sum(1 for v in offsets.values() if v is not None)
                    lbl = "девушек" if girls_only else "профилей"
                    await status_msg.edit_text(
                        f"Ищу по профилям... (коллекций: {active_count}, в очереди: {len(owner_queue)})\n"
                        f"Найдено {lbl}: <b>{found}</b>",
                        parse_mode="HTML", reply_markup=stop_kb(),
                    )
                    last_status_update = now
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"do_profile_search error: {e}")
    finally:
        is_searching = False

    return found


# ===================== RUNNERS =====================
async def start_market_search(cb: CallbackQuery, cat: str | None = None, ids: list | None = None, girls: bool = False):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
        return

    label = "Все NFT на рынке"
    if girls:
        label = "Девушки на рынке"
    elif cat and cat in PRICE_CATEGORIES:
        c = PRICE_CATEGORIES[cat]
        label = f"{c['label']} ({c['desc']})"

    await cb.answer("Запускаю...")
    stats["checks"] += 1

    if ids is None:
        if not ALL_GIFT_IDS:
            await load_collections()
        ids = [gid for gid, _ in ALL_GIFT_IDS]

    if not ids:
        await cb.message.answer("Коллекции не загружены.", reply_markup=menu_kb())
        return

    status = await cb.message.answer(
        f"<b>{label}</b>\n\nНайдено: 0",
        parse_mode="HTML", reply_markup=stop_kb()
    )
    found = await do_market_search(status, ids, cat=cat, girls_only=girls)

    try:
        await status.edit_text(
            f"<b>Готово!</b>\n{label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


async def start_profile_search(cb: CallbackQuery, cat: str | None = None, ids: list | None = None, girls: bool = False):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
        return

    label = "Все профили"
    if girls:
        label = "Девушки (профили)"
    elif cat and cat in PRICE_CATEGORIES:
        c = PRICE_CATEGORIES[cat]
        label = f"{c['label']} ({c['desc']})"

    await cb.answer("Ищу по профилям...")
    stats["checks"] += 1

    if ids is None:
        if not ALL_GIFT_IDS:
            await load_collections()
        ids = [gid for gid, _ in ALL_GIFT_IDS]

    if not ids:
        await cb.message.answer("Коллекции не загружены.", reply_markup=menu_kb())
        return

    status_text = (
        f"<b>{label}</b>\n\nАнализирую цены коллекций..."
        if cat else
        f"<b>{label}</b>\n\nНайдено: 0"
    )
    status = await cb.message.answer(status_text, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_profile_search(status, ids, cat=cat, girls_only=girls)

    try:
        await status.edit_text(
            f"<b>Готово!</b>\n{label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


# ===================== COMMANDS =====================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    add_user(
        message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
        last_name=message.from_user.last_name,
    )
    if not await check_authorized():
        if is_admin(message.from_user.id):
            await message.answer(
                "Нужна авторизация Telegram\n\nВведи номер телефона:\n<code>+79001234567</code>",
                parse_mode="HTML"
            )
            await state.set_state(Auth.phone)
            return
    await message.answer(
        "<b>🌊 Neptun Parser</b>\n"
        "<i>лучший парсер для поиска мамонтёнка!</i>\n\n"
        "Нажми кнопку ниже чтобы начать поиск NFT гифтов:",
        parse_mode="HTML", reply_markup=main_kb()
    )


@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(f"Нет доступа.\n\nТвой ID: <code>{message.from_user.id}</code>", parse_mode="HTML")
        return
    await state.clear()
    users = load_users()
    ok    = await check_authorized()
    await message.answer(
        f"<b>Админ панель</b>\n\n"
        f"Telethon: <b>{'Авторизован' if ok else 'Не авторизован'}</b>\n"
        f"Коллекций: <b>{len(ALL_GIFT_IDS)}</b>\n"
        f"Пользователей: <b>{len(users)}</b>\n"
        f"Поисков: <b>{stats['checks']}</b>\n"
        f"Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"Твой ID: <code>{message.from_user.id}</code>", parse_mode="HTML")

@dp.message(Command("cols"))
async def cmd_cols(message: Message):
    if not is_admin(message.from_user.id):
        return
    if not ALL_GIFT_IDS:
        await load_collections()
    lines = [f"<b>Коллекций: {len(ALL_GIFT_IDS)}</b>"]
    for gid, label in ALL_GIFT_IDS[:50]:
        mp = MARKET_CACHE.get(gid, "нет данных")
        lines.append(f"- {label} (id={gid}) | рынок: {mp} звёзд")
    await message.answer("\n".join(lines), parse_mode="HTML")

@dp.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.", reply_markup=main_kb())


# ===================== CALLBACKS — МЕНЮ =====================
@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer(
        "<b>🌊 Neptun Parser</b>\n"
        "<i>лучший парсер для поиска мамонтёнка!</i>\n\n"
        "Нажми кнопку ниже чтобы начать поиск NFT гифтов:",
        parse_mode="HTML", reply_markup=main_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "search_menu")
async def cb_search_menu(cb: CallbackQuery):
    await cb.message.answer(
        "<b>🔍 Выбери режим поиска:</b>",
        parse_mode="HTML", reply_markup=search_menu_kb()
    )
    await cb.answer()

# --- По ценам ---
@dp.callback_query(F.data == "mode_price")
async def cb_mode_price(cb: CallbackQuery):
    await cb.message.answer("<b>💰 Поиск по ценам</b>\n\nВыбери диапазон:", parse_mode="HTML", reply_markup=price_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "price_cheap")
async def cb_price_cheap(cb: CallbackQuery):   await start_market_search(cb, "cheap")
@dp.callback_query(F.data == "price_mid")
async def cb_price_mid(cb: CallbackQuery):     await start_market_search(cb, "mid")
@dp.callback_query(F.data == "price_hard")
async def cb_price_hard(cb: CallbackQuery):    await start_market_search(cb, "hard")
@dp.callback_query(F.data == "price_ultra")
async def cb_price_ultra(cb: CallbackQuery):   await start_market_search(cb, "ultra")
@dp.callback_query(F.data == "price_extreme")
async def cb_price_extreme(cb: CallbackQuery): await start_market_search(cb, "extreme")

# --- Девушки ---
@dp.callback_query(F.data == "mode_girls")
async def cb_mode_girls(cb: CallbackQuery):
    await cb.message.answer("<b>👧 Поиск девушек</b>\n\nВыбери источник:", parse_mode="HTML", reply_markup=girls_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "girls_market")
async def cb_girls_market(cb: CallbackQuery):  await start_market_search(cb, girls=True)
@dp.callback_query(F.data == "girls_profile")
async def cb_girls_profile(cb: CallbackQuery): await start_profile_search(cb, girls=True)

# --- По рынку ---
@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.message.answer("<b>🏪 Поиск по рынку</b>\n\nВыбери фильтр:", parse_mode="HTML", reply_markup=market_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "mkt_all")
async def cb_mkt_all(cb: CallbackQuery):     await start_market_search(cb)
@dp.callback_query(F.data == "mkt_cheap")
async def cb_mkt_cheap(cb: CallbackQuery):   await start_market_search(cb, "cheap")
@dp.callback_query(F.data == "mkt_mid")
async def cb_mkt_mid(cb: CallbackQuery):     await start_market_search(cb, "mid")
@dp.callback_query(F.data == "mkt_hard")
async def cb_mkt_hard(cb: CallbackQuery):    await start_market_search(cb, "hard")
@dp.callback_query(F.data == "mkt_ultra")
async def cb_mkt_ultra(cb: CallbackQuery):   await start_market_search(cb, "ultra")
@dp.callback_query(F.data == "mkt_extreme")
async def cb_mkt_extreme(cb: CallbackQuery): await start_market_search(cb, "extreme")

# --- С профиля ---
@dp.callback_query(F.data == "mode_profile")
async def cb_mode_profile(cb: CallbackQuery):
    await cb.message.answer("<b>👤 Поиск по профилям</b>\n\nВыбери фильтр:", parse_mode="HTML", reply_markup=profile_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "prf_all")
async def cb_prf_all(cb: CallbackQuery):     await start_profile_search(cb)
@dp.callback_query(F.data == "prf_cheap")
async def cb_prf_cheap(cb: CallbackQuery):   await start_profile_search(cb, "cheap")
@dp.callback_query(F.data == "prf_mid")
async def cb_prf_mid(cb: CallbackQuery):     await start_profile_search(cb, "mid")
@dp.callback_query(F.data == "prf_hard")
async def cb_prf_hard(cb: CallbackQuery):    await start_profile_search(cb, "hard")
@dp.callback_query(F.data == "prf_ultra")
async def cb_prf_ultra(cb: CallbackQuery):   await start_profile_search(cb, "ultra")
@dp.callback_query(F.data == "prf_extreme")
async def cb_prf_extreme(cb: CallbackQuery): await start_profile_search(cb, "extreme")

# --- По коллекции ---
@dp.callback_query(F.data == "mode_col")
async def cb_mode_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("Коллекции не загружены", reply_markup=menu_kb())
        await cb.answer()
        return
    await cb.message.answer("<b>🗂 По коллекции</b>\n\nВыбери источник:", parse_mode="HTML", reply_markup=col_source_kb())
    await cb.answer()

@dp.callback_query(F.data == "col_market")
async def cb_col_market(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    await cb.message.answer("🏪 <b>Выбери коллекцию (рынок):</b>", parse_mode="HTML",
                             reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "mktcol_", "mode_col"))
    await cb.answer()

@dp.callback_query(F.data == "col_profile")
async def cb_col_profile(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    await cb.message.answer("👤 <b>Выбери коллекцию (профили):</b>", parse_mode="HTML",
                             reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "prfcol_", "mode_col"))
    await cb.answer()

@dp.callback_query(F.data.startswith("mktcol_"))
async def cb_mktcol(cb: CallbackQuery):
    idx = int(cb.data[7:])
    lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst): await cb.answer("Не найдено", show_alert=True); return
    await start_market_search(cb, ids=[lst[idx][1]])

@dp.callback_query(F.data.startswith("prfcol_"))
async def cb_prfcol(cb: CallbackQuery):
    idx = int(cb.data[7:])
    lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst): await cb.answer("Не найдено", show_alert=True); return
    await start_profile_search(cb, ids=[lst[idx][1]])


# ===================== CALLBACKS — СТОП / СТАТИСТИКА =====================
@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    is_searching = False
    await cb.answer("Останавливаю...")
    try:
        await cb.message.edit_text("Поиск остановлен", parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        pass

@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    await cb.message.answer(
        f"<b>Статистика</b>\n\n"
        f"Поисков: <b>{stats['checks']}</b>\n"
        f"Найдено: <b>{stats['found']}</b>\n"
        f"Пользователей: <b>{get_user_count()}</b>",
        parse_mode="HTML"
    )
    await cb.answer()


# ===================== ADMIN CALLBACKS =====================
@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await show_users_page(cb.message, page=0, edit=False)
    await cb.answer()

@dp.callback_query(F.data.startswith("users_page_"))
async def cb_users_page(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    page = int(cb.data[len("users_page_"):])
    await show_users_page(cb.message, page=page, edit=True)
    await cb.answer()

async def show_users_page(message: Message, page: int, edit: bool):
    users = load_users()
    all_items = list(users.items())
    total = len(all_items)
    PAGE = 20

    if total == 0:
        text = "Пользователей пока нет."
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_panel")]])
        if edit:
            await message.edit_text(text, reply_markup=kb)
        else:
            await message.answer(text, reply_markup=kb)
        return

    start = page * PAGE
    end   = min(start + PAGE, total)
    chunk = all_items[start:end]

    lines = [f"<b>Пользователи {start+1}-{end} из {total}</b>\n"]
    for i, (uid, info) in enumerate(chunk, start + 1):
        if isinstance(info, dict):
            username  = info.get("username") or ""
            first     = info.get("first_name") or ""
            last      = info.get("last_name") or ""
            joined    = info.get("joined", 0)
        else:
            username = first = last = ""
            joined = 0

        name_parts = [p for p in [first, last] if p]
        name_str   = " ".join(name_parts) if name_parts else ""
        uname_str  = f"@{username}" if username else ""
        date_str   = fmt_timestamp(joined)

        card = f"{i}. <code>{uid}</code>"
        if uname_str:
            card += f" | {uname_str}"
        if name_str:
            card += f" | {name_str}"
        card += f"\n    {date_str}"
        lines.append(card)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="Назад", callback_data=f"users_page_{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="Вперёд", callback_data=f"users_page_{page+1}"))

    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="Админ", callback_data="admin_panel")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)

    text = "\n".join(lines)
    if edit:
        await message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await message.answer(text, parse_mode="HTML", reply_markup=kb)


@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    users = load_users()
    ok    = await check_authorized()
    await cb.message.answer(
        f"<b>Админ панель</b>\n\n"
        f"Telethon: <b>{'Авторизован' if ok else 'Не авторизован'}</b>\n"
        f"Коллекций: <b>{len(ALL_GIFT_IDS)}</b>\n"
        f"Пользователей: <b>{len(users)}</b>\n"
        f"Поисков: <b>{stats['checks']}</b>\n"
        f"Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await cb.message.answer("Отправь сообщение для рассылки.\n/cancel - отмена",
                             parse_mode="HTML", reply_markup=cancel_kb())
    await cb.answer()

@dp.message(Broadcast.message)
async def broadcast_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(mid=message.message_id, cid=message.chat.id)
    await state.set_state(None)
    await message.answer("Подтверди отправку:", reply_markup=confirm_kb())

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def cb_broadcast_send(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    data = await state.get_data()
    mid, cid = data.get("mid"), data.get("cid")
    if not mid:
        await cb.answer("Нет сообщения", show_alert=True)
        return
    users  = load_users()
    uids   = list(users.keys())
    status = await cb.message.answer(f"Отправляю {len(uids)} пользователям...")
    await cb.answer()
    ok = fail = 0
    for i, uid in enumerate(uids):
        try:
            await bot.copy_message(int(uid), cid, mid)
            ok += 1
        except Exception:
            fail += 1
        if (i + 1) % 20 == 0:
            try:
                await status.edit_text(f"{i+1}/{len(uids)}...")
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await status.edit_text(
        f"Отправлено: <b>{ok}</b>\nОшибок: <b>{fail}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    u = load_users()
    await cb.message.answer(
        f"<b>Статистика</b>\n\n"
        f"Пользователей: <b>{len(u)}</b>\n"
        f"Поисков: <b>{stats['checks']}</b>\n"
        f"Найдено: <b>{stats['found']}</b>\n"
        f"Коллекций: <b>{len(ALL_GIFT_IDS)}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "admin_auth")
async def cb_admin_auth(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.answer("Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)
    await cb.answer()

@dp.callback_query(F.data == "admin_logout")
async def cb_admin_logout(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    try:
        await tg_client.log_out()
    except Exception:
        pass
    await cb.message.answer("Вышел из TG.", reply_markup=admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.answer("Отменено", reply_markup=admin_kb())
    await cb.answer()


# ===================== AUTH =====================
@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("Формат: <code>+79001234567</code>", parse_mode="HTML")
        return
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
            await asyncio.sleep(1)
        res = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=res.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("Код отправлен. Введи без пробелов: <code>12345</code>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"Ошибка: <code>{e}</code>", parse_mode="HTML")
        await state.clear()

@dp.message(Auth.code)
async def auth_code(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    code = message.text.strip().replace(" ", "")
    data = await state.get_data()
    try:
        await tg_client.sign_in(phone=data["phone"], code=code, phone_code_hash=data["phone_code_hash"])
        me = await tg_client.get_me()
        await state.clear()
        await load_collections()
        await message.answer(
            f"Авторизован как @{me.username or me.first_name}!\n"
            f"Коллекций: <b>{len(ALL_GIFT_IDS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("Введи пароль 2FA:")
    except Exception as e:
        await message.answer(f"Ошибка: <code>{e}</code>", parse_mode="HTML")

@dp.message(Auth.password)
async def auth_password(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    try:
        await tg_client.sign_in(password=message.text.strip())
        me = await tg_client.get_me()
        await state.clear()
        await load_collections()
        await message.answer(
            f"Авторизован как @{me.username or me.first_name}!\n"
            f"Коллекций: <b>{len(ALL_GIFT_IDS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except Exception as e:
        await message.answer(f"Неверный пароль: <code>{e}</code>", parse_mode="HTML")


# ===================== MAIN =====================
async def main():
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("NFT Bot запущен!")
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info(f"Авторизован, коллекций: {len(ALL_GIFT_IDS)}")
        else:
            logger.warning("Не авторизован - пройди авторизацию через /start")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
