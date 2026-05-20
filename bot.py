import asyncio
import logging
import urllib.parse
import os
import json
import time
import datetime
from collections import defaultdict
from telethon import TelegramClient
from telethon.tl.functions.payments import GetResaleStarGiftsRequest, GetStarGiftsRequest, GetSavedStarGiftsRequest
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ======================== ДАННЫЕ ========================
API_ID       = 28687552
API_HASH     = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN    = "8406363273:AAF2L-LfRhUVMrbVLeZBLLI7IgkFoMtyfGM"
ADMIN_ID     = "8726084830"
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
ALL_GIFT_IDS    = []   # [(gid, label), ...]
NFT_COLLECTIONS = {}   # title -> gid

# Кеш медианных цен: gid -> floor_price (нижний квартиль первых 20)
PRICE_FLOOR_CACHE: dict[int, int] = {}

# Диапазон поиска для пользователей (user_id -> процент от floor)
# По умолчанию 100% — показываем гифты в диапазоне floor*0.7 .. floor*1.5
# Можно буст: от 10% до 200%
USER_BOOST: dict[int, int] = {}  # user_id -> boost_percent (10..200)
DEFAULT_BOOST = 100  # 100% = стандартный диапазон

# Категории цен по floor
PRICE_CATEGORIES = {
    "cheap":   {"label": "Дешёвые",  "desc": "до 2 000",        "floor_min": None,   "floor_max": 2000},
    "mid":     {"label": "Средние",  "desc": "2 000 - 5 000",   "floor_min": 2000,   "floor_max": 5000},
    "hard":    {"label": "Сложные",  "desc": "5 000 - 20 000",  "floor_min": 5000,   "floor_max": 20000},
    "ultra":   {"label": "Хард",     "desc": "20 000 - 100 000","floor_min": 20000,  "floor_max": 100000},
    "extreme": {"label": "Экстрим",  "desc": "от 100 000",      "floor_min": 100000, "floor_max": None},
}

GIRL_NAMES = {
    # Русские
    "анна","мария","екатерина","анастасия","наталья","ольга","елена","татьяна","ирина",
    "юлия","алина","виктория","дарья","полина","ксения","валерия","александра","надежда",
    "людмила","галина","лиза","диана","sofya","софия","кристина","светлана","милана",
    "арина","вера","жанна","ангелина","карина","оксана","нина","лариса","регина",
    "маша","катя","даша","саша","оля","лена","юля","настя","аля","поля","ксюша",
    "вика","соня","таня","ира","надя","галя","люда","вера","жени","аня","ника",
    "алиса","злата","ева","эвелина","карина","камилла","диана","яна","влада","руслана",
    # Английские
    "anna","maria","kate","natasha","olga","elena","tatiana","irina","julia","alina",
    "victoria","dasha","polina","ksenia","valeria","alexandra","diana","sophia","sofia",
    "lisa","christina","sveta","milana","arina","vera","zhanna","angela","angelina",
    "karina","oksana","nina","larisa","regina","natalia","ekaterina","anastasia",
    "alice","eva","eva","emma","mia","lily","rose","sara","sarah","kate","katie",
    "jessica","ashley","emily","olivia","ava","isabella","mia","abby","madison",
}
GIRL_KW = [
    'girl','lady','princess','queen','baby','cute','sweetie','babe','honey','cutie',
    'beautiful','pretty','lovely','darling','goddess','angel','bunny','kitty',
    'барби','принцесса','королева','девочка','красотка','кошечка','зайка','лапочка',
    'милашка','красавица','ангелочек','богиня','малышка',
]
# Признаки мужских имён/ников — исключаем их
BOY_KW = [
    'boss','king','boy','man','bro','dude','male','guy','master','lord',
    'sultan','caesar','alex_m','ivan','roman','dmitri','sergey','andrey',
    'паша','коля','вася','петя','саша_м','женя_м','миша','гриша','стас',
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
        u[key] = {"username": username or "", "first_name": first_name or "",
                  "last_name": last_name or "", "joined": int(time.time())}
    else:
        if isinstance(u[key], dict):
            if username:   u[key]["username"]   = username
            if first_name: u[key]["first_name"] = first_name
            if last_name:  u[key]["last_name"]  = last_name
    save_users(u)

def get_user_count() -> int:
    return len(load_users())

def get_boost(uid: int) -> int:
    return USER_BOOST.get(uid, DEFAULT_BOOST)


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
    """Улучшенная проверка: сначала исключаем явно мужские, потом ищем женские признаки."""
    if not owner:
        return False
    first = (getattr(owner, 'first_name', '') or '').lower().strip()
    last  = (getattr(owner, 'last_name',  '') or '').lower().strip()
    uname = (getattr(owner, 'username',   '') or '').lower().strip()
    full  = f"{first} {last} {uname}"

    # Исключаем мужские
    for kw in BOY_KW:
        if kw in full:
            return False

    # Проверяем имена — точное совпадение начала
    for name in GIRL_NAMES:
        if first.startswith(name) or last.startswith(name):
            return True
        # Ник может содержать имя: "anna_123" -> "anna"
        if len(name) >= 3 and name in uname:
            return True

    # Проверяем ключевые слова
    for kw in GIRL_KW:
        if kw in full:
            return True

    return False

def get_resell_price(gift) -> int | None:
    """Цена выставления на продажу из resell_amount."""
    ra = getattr(gift, 'resell_amount', None)
    if ra is None:
        return None
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
    return users_map.get(int(uid)), int(uid)

def fmt_owner(owner, username, name) -> str:
    if name and username:
        return f"{name} (@{username})"
    if username:
        return f"@{username}"
    if name:
        return name
    return "Скрыт"

def fmt_timestamp(ts: int) -> str:
    if not ts:
        return "неизвестно"
    dt = datetime.datetime.fromtimestamp(ts)
    return dt.strftime("%d.%m.%Y %H:%M")

def make_nft_url(gift) -> str | None:
    slug = (getattr(gift, 'slug', None)
            or getattr(gift, 'unique_id', None)
            or '')
    slug = str(slug).strip()
    if slug and slug not in ('None', '', 'nan', '0'):
        try:
            int(slug)
        except ValueError:
            return f"https://t.me/nft/{slug}"
    return None


# ===================== FLOOR PRICE (первые 20 гифтов) =====================
async def get_floor_price(gift_id: int) -> int | None:
    """
    Берём первые 20 гифтов коллекции на рынке и считаем нижний квартиль.
    Это реальная рыночная цена (floor), не средняя.
    Кешируем результат.
    """
    if gift_id in PRICE_FLOOR_CACHE:
        return PRICE_FLOOR_CACHE[gift_id]
    try:
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gift_id, offset="", limit=20,
        ))
        gifts  = getattr(result, 'gifts', None) or []
        prices = []
        for g in gifts:
            p = get_resell_price(g)
            if p and p > 0:
                prices.append(p)
        if not prices:
            return None
        prices.sort()
        # Берём 25-й перцентиль как floor
        floor = prices[max(0, len(prices) // 4)]
        PRICE_FLOOR_CACHE[gift_id] = floor
        logger.info(f"Floor gid={gift_id}: {floor} (из {len(prices)} гифтов)")
        return floor
    except Exception as e:
        logger.error(f"get_floor_price gid={gift_id}: {e}")
        return None

def floor_in_category(floor: int, cat: str) -> bool:
    """Проверяет попадает ли floor-цена коллекции в категорию."""
    c = PRICE_CATEGORIES.get(cat)
    if not c:
        return True
    fmin = c["floor_min"]
    fmax = c["floor_max"]
    if fmin is not None and floor < fmin:
        return False
    if fmax is not None and floor > fmax:
        return False
    return True

def price_ok_for_floor(price: int, floor: int, boost: int) -> bool:
    """
    Проверяет что цена гифта в разумном диапазоне от floor коллекции.
    boost=100 -> диапазон floor*0.7 .. floor*(1 + boost/100)
    boost=200 -> диапазон floor*0.7 .. floor*3.0
    """
    factor = boost / 100.0
    low  = floor * 0.7
    high = floor * (1.0 + factor)
    return low <= price <= high


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
        logger.info(f"Коллекций загружено: {len(ALL_GIFT_IDS)}")
    except Exception as e:
        logger.error(f"load_collections: {e}")


# ===================== FETCH MARKET PAGE =====================
async def fetch_market_page(gift_id: int, offset: str, limit: int = 50) -> tuple[list, str]:
    """Гифты с рынка. Цена = resell_amount."""
    try:
        result    = await tg_client(GetResaleStarGiftsRequest(gift_id=gift_id, offset=offset, limit=limit))
        users_map = {int(u.id): u for u in (getattr(result, 'users', None) or [])}
        gifts     = getattr(result, 'gifts', None) or []
        items     = []
        for gift in gifts:
            owner, owner_uid = get_owner(gift, users_map)
            username = getattr(owner, 'username', None) if owner else None
            fn   = (getattr(owner, 'first_name', '') or '') if owner else ''
            ln   = (getattr(owner, 'last_name',  '') or '') if owner else ''
            name = f"{fn} {ln}".strip()
            nft_url     = make_nft_url(gift)
            profile_url = (f"https://t.me/{username}" if username
                           else (f"tg://user?id={owner_uid}" if owner_uid else None))
            items.append({
                "owner": owner, "owner_id": owner_uid,
                "username": username, "name": name,
                "title": getattr(gift, 'title', '?'),
                "num":   getattr(gift, 'num',   '?'),
                "price": get_resell_price(gift),
                "nft_url": nft_url,
                "profile_url": profile_url,
                "gift_id": gift_id,
            })
        return items, getattr(result, 'next_offset', "") or ""
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds}s")
        await asyncio.sleep(e.seconds + 3)
        return [], ""
    except Exception as e:
        logger.error(f"fetch_market_page gid={gift_id}: {e}")
        return [], ""


# ===================== FETCH PROFILE GIFTS =====================
async def fetch_saved_gifts_page(user_id: int, offset: str, limit: int = 50) -> tuple[list, str]:
    """Гифты из профиля через GetSavedStarGiftsRequest. Без маркета."""
    try:
        result = await tg_client(GetSavedStarGiftsRequest(
            peer=await tg_client.get_input_entity(user_id),
            offset=offset, limit=limit,
        ))
        gifts = getattr(result, 'gifts', None) or []
        items = []
        for gift in gifts:
            nft_url = make_nft_url(gift)
            if not nft_url:
                # Пробуем из вложенного gift объекта
                inner = getattr(gift, 'gift', None)
                if inner:
                    nft_url = make_nft_url(inner)
            inner = getattr(gift, 'gift', None)
            title = getattr(inner, 'title', None) or getattr(gift, 'title', '?')
            num   = getattr(gift, 'num', '?')
            items.append({
                "title": title, "num": num,
                "nft_url": nft_url,
                "gift_id": getattr(inner, 'id', None) if inner else None,
            })
        return items, getattr(result, 'next_offset', "") or ""
    except Exception as e:
        logger.error(f"fetch_saved_gifts uid={user_id}: {e}")
        return [], ""


# ===================== KEYBOARDS =====================

def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_menu")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
    ])

def search_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 По ценам",     callback_data="mode_price")],
        [InlineKeyboardButton(text="👧 Девушки",      callback_data="mode_girls")],
        [InlineKeyboardButton(text="🏪 По рынку",     callback_data="mode_market")],
        [InlineKeyboardButton(text="👤 С профиля",    callback_data="mode_profile")],
        [InlineKeyboardButton(text="🗂 По коллекции", callback_data="mode_col")],
        [InlineKeyboardButton(text="◀️ Назад",        callback_data="menu")],
    ])

def price_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💚 Дешёвые (до 2 000)",       callback_data="price_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2 000-5 000)",    callback_data="price_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5 000-20 000)",   callback_data="price_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20 000-100 000)",    callback_data="price_ultra")],
        [InlineKeyboardButton(text="💀 Экстрим (100 000+)",       callback_data="price_extreme")],
        [InlineKeyboardButton(text="◀️ Назад",                    callback_data="search_menu")],
    ])

def girls_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏪 Девушки на рынке",   callback_data="girls_market")],
        [InlineKeyboardButton(text="👤 Девушки в профилях", callback_data="girls_profile")],
        [InlineKeyboardButton(text="◀️ Назад",              callback_data="search_menu")],
    ])

def market_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все NFT (2+ гифта)",     callback_data="mkt_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2 000)",     callback_data="mkt_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2 000-5 000)",  callback_data="mkt_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5 000-20 000)", callback_data="mkt_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20 000-100 000)",  callback_data="mkt_ultra")],
        [InlineKeyboardButton(text="💀 Экстрим (100 000+)",     callback_data="mkt_extreme")],
        [InlineKeyboardButton(text="◀️ Назад",                  callback_data="search_menu")],
    ])

def profile_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все профили (2+ гифта)",  callback_data="prf_all")],
        [InlineKeyboardButton(text="◀️ Назад",                   callback_data="search_menu")],
    ])

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

def neptun_panel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📈 Буст x1.5 (150%)", callback_data="boost_150")],
        [InlineKeyboardButton(text="📈 Буст x2 (200%)",   callback_data="boost_200")],
        [InlineKeyboardButton(text="📉 Стандарт (100%)",  callback_data="boost_100")],
        [InlineKeyboardButton(text="📉 Мини (50%)",       callback_data="boost_50")],
        [InlineKeyboardButton(text="📉 Мини (30%)",       callback_data="boost_30")],
        [InlineKeyboardButton(text="◀️ В меню",           callback_data="menu")],
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

def owner_kb(username: str | None, profile_url: str | None) -> InlineKeyboardMarkup | None:
    btns = []
    if profile_url:
        label = f"👤 @{username}" if username else "👤 Профиль"
        btns.append([InlineKeyboardButton(text=label, url=profile_url)])
    if username:
        btns.append([InlineKeyboardButton(
            text="✉️ Написать",
            url=f"https://t.me/{username}?text={urllib.parse.quote('Привет! Хочу купить твои NFT')}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

def nft_single_kb(username: str | None, profile_url: str | None,
                  nft_url: str | None, title: str, num) -> InlineKeyboardMarkup | None:
    btns = []
    if nft_url:
        btns.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=nft_url)])
    if profile_url:
        label = f"👤 @{username}" if username else "👤 Профиль"
        btns.append([InlineKeyboardButton(text=label, url=profile_url)])
    if username:
        txt = f"Привет! Хочу купить твой NFT {nft_url or f'{title} #{num}'}"
        btns.append([InlineKeyboardButton(
            text="✉️ Написать",
            url=f"https://t.me/{username}?text={urllib.parse.quote(txt)}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None


# ===================== CORE: ПОИСК ПО РЫНКУ =====================
async def do_market_search(
    status_msg: Message,
    gift_ids: list[int],
    cat: str | None = None,
    girls_only: bool = False,
    max_results: int = 150,
    boost: int = 100,
) -> int:
    """
    Поиск на рынке.
    - Сначала для каждой коллекции берём floor (первые 20 гифтов).
    - Фильтруем коллекции по категории цен.
    - Round-robin по коллекциям.
    - Режим "все" (cat=None): группируем по владельцу, показываем 2+.
    - Режим по цене: каждый гифт отдельно, цена должна быть близко к floor.
    """
    global is_searching
    is_searching  = True
    found         = 0
    seen_slugs    = set()
    has_cat       = cat is not None

    # Шаг 1: загружаем floor и фильтруем коллекции
    await status_msg.edit_text(
        "Анализирую коллекции...", reply_markup=stop_kb()
    )
    valid_gids: list[tuple[int, int]] = []  # (gid, floor)
    for i in range(0, len(gift_ids), 5):
        if not is_searching:
            break
        batch   = gift_ids[i:i+5]
        floors  = await asyncio.gather(*[get_floor_price(g) for g in batch])
        for gid, floor in zip(batch, floors):
            if floor is None:
                continue
            if cat and not floor_in_category(floor, cat):
                continue
            valid_gids.append((gid, floor))
        await asyncio.sleep(0.3)

    if not valid_gids:
        is_searching = False
        return 0

    # Шаг 2: поиск round-robin
    offsets: dict[int, str | None] = {gid: "" for gid, _ in valid_gids}
    floor_map: dict[int, int]      = {gid: fl for gid, fl in valid_gids}
    buffers: dict[int, list]       = {gid: [] for gid, _ in valid_gids}
    # Для режима "все" — группируем по владельцу
    owner_buckets: dict[int, dict] = {}
    owner_count_cat: dict[int, int] = {}
    MAX_PER_OWNER_CAT = 3
    last_upd = 0.0

    async def flush_owners():
        """Отправляем владельцев у которых 2+ гифта."""
        nonlocal found
        ready = [(uid, b) for uid, b in list(owner_buckets.items()) if len(b["items"]) >= 2]
        for uid, bucket in ready:
            if not is_searching:
                break
            lines = []
            for it in bucket["items"]:
                title = it.get("title", "?")
                num   = it.get("num", "?")
                price = it.get("price")
                nft_url = it.get("nft_url")
                price_str = f"{price:,}".replace(",", " ") if price else "?"
                if nft_url:
                    lines.append(f'<a href="{nft_url}">{title} #{num}</a> - {price_str} zv')
                else:
                    lines.append(f"{title} #{num} - {price_str} zv")

            owner_str   = fmt_owner(bucket["owner"], bucket["username"], bucket["name"])
            profile_url = bucket["profile_url"]
            username    = bucket["username"]
            kb = owner_kb(username, profile_url)
            try:
                await status_msg.bot.send_message(
                    chat_id=status_msg.chat.id,
                    text=(
                        f"<b>👤 {owner_str}</b>\n"
                        f"Гифтов: <b>{len(bucket['items'])}</b>\n\n"
                        + "\n".join(lines)
                    ),
                    parse_mode="HTML",
                    reply_markup=kb,
                    disable_web_page_preview=True,
                )
                found += len(bucket["items"])
                stats["found"] += len(bucket["items"])
            except Exception as e:
                logger.warning(f"flush_owners: {e}")
            del owner_buckets[uid]
            await asyncio.sleep(0.07)

    try:
        while is_searching and found < max_results:
            active = [gid for gid, off in offsets.items() if off is not None or buffers[gid]]
            if not active:
                break
            made = False

            for gid in list(active):
                if not is_searching or found >= max_results:
                    break
                floor = floor_map[gid]

                # Подгружаем страницу
                if not buffers[gid] and offsets.get(gid) is not None:
                    items, nxt     = await fetch_market_page(gid, offsets[gid], limit=50)
                    offsets[gid]   = nxt if nxt else None

                    for item in items:
                        nft_url = item.get("nft_url") or ""
                        slug    = nft_url.split("/")[-1] if nft_url else ""
                        if slug and slug in seen_slugs:
                            continue
                        if slug:
                            seen_slugs.add(slug)

                        if girls_only and not is_girl(item["owner"]):
                            continue

                        price = item.get("price")
                        if price is None:
                            continue

                        if has_cat:
                            # Цена должна быть в разумном диапазоне от floor
                            if not price_ok_for_floor(price, floor, boost):
                                continue
                            owner_id = item["owner_id"]
                            if owner_id:
                                if owner_count_cat.get(owner_id, 0) >= MAX_PER_OWNER_CAT:
                                    continue
                                owner_count_cat[owner_id] = owner_count_cat.get(owner_id, 0) + 1
                            buffers[gid].append(item)
                        else:
                            # Режим "все" — группируем по владельцу
                            owner_id = item["owner_id"]
                            if owner_id:
                                if owner_id not in owner_buckets:
                                    owner_buckets[owner_id] = {
                                        "owner": item["owner"],
                                        "username": item["username"],
                                        "name": item["name"],
                                        "profile_url": item["profile_url"],
                                        "items": [],
                                    }
                                owner_buckets[owner_id]["items"].append(item)

                # Отправляем из буфера (режим по цене)
                if has_cat and buffers[gid]:
                    item = buffers[gid].pop(0)
                    price    = item["price"]
                    username = item["username"]
                    profile_url = item["profile_url"]
                    nft_url  = item["nft_url"]
                    title    = item["title"]
                    num      = item["num"]
                    owner_str = fmt_owner(item["owner"], username, item["name"])
                    price_str = f"{price:,}".replace(",", " ") if price else "?"
                    nft_line  = f'\n<a href="{nft_url}">{title} #{num}</a>' if nft_url else f"\n{title} #{num}"
                    kb = nft_single_kb(username, profile_url, nft_url, title, num)
                    try:
                        await status_msg.bot.send_message(
                            chat_id=status_msg.chat.id,
                            text=(
                                f"<b>👤 {owner_str}</b>"
                                f"{nft_line}\n"
                                f"💰 <b>{price_str} zv</b>"
                            ),
                            parse_mode="HTML",
                            reply_markup=kb,
                            disable_web_page_preview=True,
                        )
                        found += 1
                        stats["found"] += 1
                        made = True
                    except Exception as e:
                        logger.warning(f"send_single: {e}")
                    await asyncio.sleep(0.05)

            # Режим "все" — сбрасываем готовых владельцев
            if not has_cat:
                await flush_owners()
                made = True  # продолжаем пока есть страницы

            now = asyncio.get_event_loop().time()
            if now - last_upd > 3:
                try:
                    active_cnt = sum(1 for v in offsets.values() if v is not None)
                    lbl = "девушек" if girls_only else "NFT"
                    pending = sum(len(b["items"]) for b in owner_buckets.values())
                    await status_msg.edit_text(
                        f"Ищу на рынке... (коллекций: {active_cnt})\n"
                        f"Найдено {lbl}: <b>{found}</b>"
                        + (f"\nВ буфере: {pending}" if not has_cat else ""),
                        parse_mode="HTML", reply_markup=stop_kb(),
                    )
                    last_upd = now
                except Exception:
                    pass

            if not active:
                break
            if not made and has_cat:
                break

        # Финальный сброс для режима "все"
        if not has_cat and is_searching:
            await flush_owners()

    except Exception as e:
        logger.error(f"do_market_search: {e}")
    finally:
        is_searching = False
    return found


# ===================== CORE: ПОИСК ПО ПРОФИЛЯМ =====================
async def do_profile_search(
    status_msg: Message,
    gift_ids: list[int],
    girls_only: bool = False,
    max_results: int = 150,
) -> int:
    """
    Поиск по профилям:
    1. Собираем уникальных владельцев с рынка (round-robin).
    2. Для каждого — запрашиваем SavedStarGifts (гифты из профиля).
    3. Если 2+ гифта — показываем блоком с ссылками в тексте.
    Без цен, без маркета. Только ник и ссылки на NFT.
    """
    global is_searching
    is_searching = True
    found        = 0
    seen_owners: set[int] = set()
    owner_queue: list[tuple] = []  # (uid, owner_obj, username, name)
    market_offsets: dict[int, str | None] = {gid: "" for gid in gift_ids}
    seen_market_slugs: set[str] = set()
    last_upd = 0.0

    async def collect_owners(n: int) -> bool:
        """Собрать n новых уникальных владельцев с рынка."""
        collected = 0
        active = [g for g, o in market_offsets.items() if o is not None]
        if not active:
            return False
        for gid in active:
            if collected >= n:
                break
            if market_offsets.get(gid) is None:
                continue
            items, nxt = await fetch_market_page(gid, market_offsets[gid], limit=50)
            market_offsets[gid] = nxt if nxt else None
            for item in items:
                uid = item.get("owner_id")
                if not uid or uid in seen_owners:
                    continue
                if girls_only and not is_girl(item["owner"]):
                    continue
                seen_owners.add(uid)
                owner_queue.append((uid, item["owner"], item["username"], item["name"]))
                collected += 1
        return collected > 0

    try:
        while is_searching and found < max_results:
            # Пополняем очередь владельцев
            if len(owner_queue) < 10:
                has_more = await collect_owners(30)
                if not has_more and not owner_queue:
                    break

            if not owner_queue:
                break

            # Берём пачку владельцев
            batch = owner_queue[:5]
            owner_queue = owner_queue[5:]

            for (uid, owner_obj, username, name) in batch:
                if not is_searching or found >= max_results:
                    break

                # Запрашиваем гифты из профиля
                all_gifts = []
                offset    = ""
                for _ in range(3):  # макс 3 страницы
                    g_items, g_next = await fetch_saved_gifts_page(uid, offset, limit=50)
                    all_gifts.extend(g_items)
                    offset = g_next
                    if not g_next:
                        break
                    await asyncio.sleep(0.1)

                # Только гифты с NFT ссылкой
                nft_gifts = [g for g in all_gifts if g.get("nft_url")]

                if len(nft_gifts) < 2:
                    continue  # меньше 2 нфт — пропускаем

                profile_url = (f"https://t.me/{username}" if username
                               else f"tg://user?id={uid}")
                owner_str   = fmt_owner(owner_obj, username, name)

                # Формируем список ссылок
                lines = []
                for g in nft_gifts:
                    title   = g.get("title", "?")
                    num     = g.get("num", "?")
                    nft_url = g.get("nft_url")
                    lines.append(f'<a href="{nft_url}">{title} #{num}</a>')

                kb = owner_kb(username, profile_url)
                try:
                    await status_msg.bot.send_message(
                        chat_id=status_msg.chat.id,
                        text=(
                            f"<b>👤 {owner_str}</b>\n"
                            f"NFT в профиле: <b>{len(nft_gifts)}</b>\n\n"
                            + "\n".join(lines)
                        ),
                        parse_mode="HTML",
                        reply_markup=kb,
                        disable_web_page_preview=True,
                    )
                    found += len(nft_gifts)
                    stats["found"] += len(nft_gifts)
                except Exception as e:
                    logger.warning(f"send_profile_block: {e}")

                await asyncio.sleep(0.15)

            now = asyncio.get_event_loop().time()
            if now - last_upd > 3:
                try:
                    active_cnt = sum(1 for v in market_offsets.values() if v is not None)
                    lbl = "девушек" if girls_only else "профилей"
                    await status_msg.edit_text(
                        f"Ищу по профилям... (коллекций: {active_cnt}, в очереди: {len(owner_queue)})\n"
                        f"Найдено {lbl}: <b>{found}</b>",
                        parse_mode="HTML", reply_markup=stop_kb(),
                    )
                    last_upd = now
                except Exception:
                    pass

    except Exception as e:
        logger.error(f"do_profile_search: {e}")
    finally:
        is_searching = False
    return found


# ===================== RUNNERS =====================
async def ensure_collections():
    if not ALL_GIFT_IDS:
        await load_collections()
    return [gid for gid, _ in ALL_GIFT_IDS]

async def run_market(cb: CallbackQuery, cat: str | None = None,
                     girls: bool = False, ids: list | None = None):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
        return
    await cb.answer("Запускаю...")
    stats["checks"] += 1
    if ids is None:
        ids = await ensure_collections()
    if not ids:
        await cb.message.answer("Коллекции не загружены.", reply_markup=menu_kb())
        return
    boost = get_boost(cb.from_user.id)
    label = "Девушки на рынке" if girls else (
        PRICE_CATEGORIES[cat]["label"] if cat else "Все NFT (2+ гифта)"
    )
    status = await cb.message.answer(
        f"<b>{label}</b>\nБуст: {boost}%\n\nНайдено: 0",
        parse_mode="HTML", reply_markup=stop_kb()
    )
    found = await do_market_search(status, ids, cat=cat, girls_only=girls, boost=boost)
    try:
        await status.edit_text(
            f"<b>Готово!</b> {label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass

async def run_profile(cb: CallbackQuery, girls: bool = False, ids: list | None = None):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
        return
    await cb.answer("Ищу по профилям...")
    stats["checks"] += 1
    if ids is None:
        ids = await ensure_collections()
    if not ids:
        await cb.message.answer("Коллекции не загружены.", reply_markup=menu_kb())
        return
    label = "Девушки (профили)" if girls else "Все профили (2+ NFT)"
    status = await cb.message.answer(
        f"<b>{label}</b>\n\nСобираю владельцев...",
        parse_mode="HTML", reply_markup=stop_kb()
    )
    found = await do_profile_search(status, ids, girls_only=girls)
    try:
        await status.edit_text(
            f"<b>Готово!</b> {label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


# ===================== COMMANDS =====================
WELCOME_TEXT = (
    "<b>🌊 Neptun Parser</b>\n"
    "<i>лучший парсер для поиска мамонтёнка!</i>\n\n"
    "Нажми кнопку ниже чтобы начать поиск NFT гифтов:"
)

@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    add_user(message.from_user.id, message.from_user.username,
             message.from_user.first_name, message.from_user.last_name)
    if not await check_authorized() and is_admin(message.from_user.id):
        await message.answer(
            "Нужна авторизация Telegram\n\nВведи номер: <code>+79001234567</code>",
            parse_mode="HTML"
        )
        await state.set_state(Auth.phone)
        return
    await message.answer(WELCOME_TEXT, parse_mode="HTML", reply_markup=main_kb())

@dp.message(Command("clear"))
async def cmd_clear(message: Message):
    global is_searching
    is_searching = False
    await message.answer("Поиск остановлен.", reply_markup=menu_kb())

@dp.message(Command("neptunteam"))
async def cmd_neptunteam(message: Message):
    boost = get_boost(message.from_user.id)
    await message.answer(
        f"<b>🌊 Neptun Team Panel</b>\n\n"
        f"Текущий буст диапазона: <b>{boost}%</b>\n\n"
        f"Буст влияет на диапазон цен при поиске:\n"
        f"100% = floor x1.0 .. floor x2.0\n"
        f"150% = floor x1.0 .. floor x2.5\n"
        f"200% = floor x1.0 .. floor x3.0\n"
        f"50%  = floor x1.0 .. floor x1.5 (точнее)\n\n"
        f"Выбери диапазон:",
        parse_mode="HTML", reply_markup=neptun_panel_kb()
    )

@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(f"Нет доступа. ID: <code>{message.from_user.id}</code>", parse_mode="HTML")
        return
    await state.clear()
    users = load_users()
    ok    = await check_authorized()
    await message.answer(
        f"<b>Админ панель</b>\n\n"
        f"Telethon: <b>{'Авторизован' if ok else 'Не авторизован'}</b>\n"
        f"Коллекций: <b>{len(ALL_GIFT_IDS)}</b>\n"
        f"Пользователей: <b>{len(users)}</b>\n"
        f"Поисков: <b>{stats['checks']}</b> | Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"ID: <code>{message.from_user.id}</code>", parse_mode="HTML")

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


# ===================== NEPTUN PANEL BOOSTS =====================
@dp.callback_query(F.data.startswith("boost_"))
async def cb_boost(cb: CallbackQuery):
    val = int(cb.data.split("_")[1])
    val = max(10, min(200, val))
    USER_BOOST[cb.from_user.id] = val
    await cb.answer(f"Буст установлен: {val}%", show_alert=True)
    await cb.message.edit_text(
        f"<b>🌊 Neptun Team Panel</b>\n\n"
        f"Буст установлен: <b>{val}%</b>\n\n"
        f"Теперь поиск будет искать гифты с диапазоном цен: floor x1.0 .. floor x{1 + val/100:.1f}",
        parse_mode="HTML", reply_markup=neptun_panel_kb()
    )


# ===================== CALLBACKS — МЕНЮ =====================
@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer(WELCOME_TEXT, parse_mode="HTML", reply_markup=main_kb())
    await cb.answer()

@dp.callback_query(F.data == "search_menu")
async def cb_search_menu(cb: CallbackQuery):
    await cb.message.answer("<b>Выбери режим поиска:</b>", parse_mode="HTML", reply_markup=search_menu_kb())
    await cb.answer()

# --- По ценам ---
@dp.callback_query(F.data == "mode_price")
async def cb_mode_price(cb: CallbackQuery):
    boost = get_boost(cb.from_user.id)
    await cb.message.answer(
        f"<b>💰 Поиск по ценам</b>\nБуст: {boost}%\n\nВыбери диапазон:",
        parse_mode="HTML", reply_markup=price_menu_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "price_cheap")
async def cb_pc(cb): await run_market(cb, "cheap")
@dp.callback_query(F.data == "price_mid")
async def cb_pm(cb): await run_market(cb, "mid")
@dp.callback_query(F.data == "price_hard")
async def cb_ph(cb): await run_market(cb, "hard")
@dp.callback_query(F.data == "price_ultra")
async def cb_pu(cb): await run_market(cb, "ultra")
@dp.callback_query(F.data == "price_extreme")
async def cb_pe(cb): await run_market(cb, "extreme")

# --- Девушки ---
@dp.callback_query(F.data == "mode_girls")
async def cb_mode_girls(cb: CallbackQuery):
    await cb.message.answer("<b>👧 Девушки</b>\n\nВыбери источник:", parse_mode="HTML", reply_markup=girls_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "girls_market")
async def cb_gm(cb): await run_market(cb, girls=True)
@dp.callback_query(F.data == "girls_profile")
async def cb_gp(cb): await run_profile(cb, girls=True)

# --- По рынку ---
@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.message.answer("<b>🏪 По рынку</b>\n\nВыбери фильтр:", parse_mode="HTML", reply_markup=market_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "mkt_all")
async def cb_ma(cb): await run_market(cb)
@dp.callback_query(F.data == "mkt_cheap")
async def cb_mc(cb): await run_market(cb, "cheap")
@dp.callback_query(F.data == "mkt_mid")
async def cb_mm(cb): await run_market(cb, "mid")
@dp.callback_query(F.data == "mkt_hard")
async def cb_mh(cb): await run_market(cb, "hard")
@dp.callback_query(F.data == "mkt_ultra")
async def cb_mu(cb): await run_market(cb, "ultra")
@dp.callback_query(F.data == "mkt_extreme")
async def cb_me(cb): await run_market(cb, "extreme")

# --- С профиля ---
@dp.callback_query(F.data == "mode_profile")
async def cb_mode_profile(cb: CallbackQuery):
    await cb.message.answer("<b>👤 С профиля</b>\n\nПоказываю только тех у кого 2+ NFT:", parse_mode="HTML", reply_markup=profile_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "prf_all")
async def cb_pa(cb): await run_profile(cb)

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
    await cb.message.answer(
        "🏪 Выбери коллекцию (рынок):",
        reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "mktcol_", "mode_col")
    )
    await cb.answer()

@dp.callback_query(F.data == "col_profile")
async def cb_col_profile(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    await cb.message.answer(
        "👤 Выбери коллекцию (профили):",
        reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "prfcol_", "mode_col")
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("mktcol_"))
async def cb_mktcol(cb: CallbackQuery):
    idx = int(cb.data[7:])
    lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst):
        await cb.answer("Не найдено", show_alert=True)
        return
    await run_market(cb, ids=[lst[idx][1]])

@dp.callback_query(F.data.startswith("prfcol_"))
async def cb_prfcol(cb: CallbackQuery):
    idx = int(cb.data[7:])
    lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst):
        await cb.answer("Не найдено", show_alert=True)
        return
    await run_profile(cb, ids=[lst[idx][1]])


# ===================== CALLBACKS — СТОП / СТАТИСТИКА =====================
@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    is_searching = False
    await cb.answer("Останавливаю...")
    try:
        await cb.message.edit_text("Поиск остановлен.", reply_markup=menu_kb())
    except Exception:
        pass

@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    await cb.message.answer(
        f"<b>📊 Статистика</b>\n\n"
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
    users     = load_users()
    all_items = list(users.items())
    total     = len(all_items)
    PAGE      = 20
    if total == 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_panel")]])
        if edit:
            await message.edit_text("Пользователей нет.", reply_markup=kb)
        else:
            await message.answer("Пользователей нет.", reply_markup=kb)
        return
    start = page * PAGE
    end   = min(start + PAGE, total)
    chunk = all_items[start:end]
    lines = [f"<b>Пользователи {start+1}-{end} из {total}</b>\n"]
    for i, (uid, info) in enumerate(chunk, start + 1):
        if isinstance(info, dict):
            uname  = info.get("username") or ""
            first  = info.get("first_name") or ""
            last   = info.get("last_name") or ""
            joined = info.get("joined", 0)
        else:
            uname = first = last = ""
            joined = 0
        name = " ".join(p for p in [first, last] if p)
        card = f"{i}. <code>{uid}</code>"
        if uname: card += f" @{uname}"
        if name:  card += f" | {name}"
        card += f"\n    {fmt_timestamp(joined)}"
        lines.append(card)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="Назад", callback_data=f"users_page_{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="Вперёд", callback_data=f"users_page_{page+1}"))
    rows = [nav] if nav else []
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
        f"Пользователей: <b>{len(users)}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await cb.message.answer("Отправь сообщение для рассылки. /cancel - отмена", reply_markup=cancel_kb())
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
    status = await cb.message.answer(f"Рассылка {len(uids)} пользователям...")
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
        f"<b>Статистика</b>\n\nПользователей: <b>{len(u)}</b>\n"
        f"Поисков: <b>{stats['checks']}</b>\nНайдено: <b>{stats['found']}</b>\n"
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
        await message.answer("Код отправлен. Введи: <code>12345</code>", parse_mode="HTML")
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
            f"Авторизован как @{me.username or me.first_name}!\nКоллекций: <b>{len(ALL_GIFT_IDS)}</b>",
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
            f"Авторизован как @{me.username or me.first_name}!\nКоллекций: <b>{len(ALL_GIFT_IDS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except Exception as e:
        await message.answer(f"Неверный пароль: <code>{e}</code>", parse_mode="HTML")


# ===================== MAIN =====================
async def main():
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("Neptun Parser запущен!")
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info(f"Авторизован, коллекций: {len(ALL_GIFT_IDS)}")
        else:
            logger.warning("Не авторизован - пройди /start")
    except Exception as e:
        logger.error(f"Ошибка старта: {e}")
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
