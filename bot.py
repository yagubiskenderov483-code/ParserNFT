
import asyncio
import logging
import urllib.parse
import os
import json
import time
from telethon import TelegramClient
from telethon.tl.functions.payments import GetResaleStarGiftsRequest, GetStarGiftsRequest
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

# Категории с допуском:
# Дешёвые:  до 2000   | допуск ±300  → показываем 0–2300
# Средние:  2000–5000 | допуск ±500  → показываем 1500–5500
# Сложные:  5000–100k | допуск ±3000 → показываем 2000–103000
# Хард:     100k+     | допуск -7000 → показываем 93000+
PRICE_CATEGORIES = {
    "cheap": {
        "label": "💚 Дешёвые",
        "desc": "до 2000 ⭐",
        "min": None, "max": 2000,
        "tol_min": None, "tol_max": 2300,
    },
    "mid": {
        "label": "💛 Средние",
        "desc": "2000–5000 ⭐",
        "min": 2000, "max": 5000,
        "tol_min": 1500, "tol_max": 5500,
    },
    "hard": {
        "label": "🟠 Сложные",
        "desc": "5000–100000 ⭐",
        "min": 5000, "max": 100000,
        "tol_min": 2000, "tol_max": 103000,
    },
    "ultra": {
        "label": "🔴 Хард",
        "desc": "от 100000 ⭐",
        "min": 100000, "max": None,
        "tol_min": 93000, "tol_max": None,
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
    """Возвращает dict: {str(uid): {"username": ..., "joined": timestamp}}"""
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            data = json.load(f)
            # Совместимость со старым форматом (list)
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
        # Обновляем данные если изменились
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
    for field in ['resell_stars','resale_stars','price','stars','star_count','cost','amount']:
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
    """Проверяет попадание цены в категорию с допуском"""
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
    """Получаем медианную цену по рынку для коллекции"""
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
    """Гифты выставленные на продажу — реальная цена есть"""
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
            price = get_price(gift)

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


# ===================== FETCH PROFILE GIFTS =====================
async def fetch_profile_gifts(user_id: int) -> list:
    """
    Получаем НЕ-resale гифты пользователя.
    Telegram не даёт прямого API для этого, поэтому используем
    эвристику: берём гифты с рынка, исключаем выставленные на продажу.
    Для профильного режима мы ищем владельцев через рынок но
    показываем их как "профильные" — с оценочной ценой по медиане коллекции.
    """
    # Этот метод используется внутри profile search
    pass


# ===================== KEYBOARDS =====================
def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏪 Поиск по рынку",    callback_data="search_market_menu")],
        [InlineKeyboardButton(text="👤 Поиск по профилям", callback_data="search_profile_menu")],
        [InlineKeyboardButton(text="📊 Статистика",         callback_data="stats")],
    ])

def market_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Все NFT на продаже",          callback_data="mkt_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000 ⭐)",        callback_data="mkt_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000–5000 ⭐)",      callback_data="mkt_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000–100000 ⭐)",    callback_data="mkt_hard")],
        [InlineKeyboardButton(text="🔴 Хард (100000+ ⭐)",           callback_data="mkt_ultra")],
        [InlineKeyboardButton(text="🗂 По коллекции",                callback_data="mkt_col")],
        [InlineKeyboardButton(text="👧 Только девушки",              callback_data="mkt_girls")],
        [InlineKeyboardButton(text="◀️ Назад",                       callback_data="menu")],
    ])

def profile_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Все профили",                  callback_data="prf_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000 ⭐)",        callback_data="prf_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000–5000 ⭐)",      callback_data="prf_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000–100000 ⭐)",    callback_data="prf_hard")],
        [InlineKeyboardButton(text="🔴 Хард (100000+ ⭐)",           callback_data="prf_ultra")],
        [InlineKeyboardButton(text="🗂 По коллекции",                callback_data="prf_col")],
        [InlineKeyboardButton(text="👧 Только девушки",              callback_data="prf_girls")],
        [InlineKeyboardButton(text="◀️ Назад",                       callback_data="menu")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ СТОП", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏪 Рынок",    callback_data="search_market_menu")],
        [InlineKeyboardButton(text="👤 Профили",  callback_data="search_profile_menu")],
        [InlineKeyboardButton(text="📱 Меню",     callback_data="menu")],
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

def nft_kb(item: dict, has_price_filter: bool = False) -> InlineKeyboardMarkup | None:
    username    = item.get("username")
    nft_url     = item.get("nft_url")
    profile_url = item.get("profile_url")
    title       = item.get("title", "")
    num         = item.get("num", "")
    owner_id    = item.get("owner_id")
    btns        = []

    # 1. Если есть фильтр по цене — показываем конкретный NFT
    #    Если нет фильтра — показываем все гифты владельца (его профиль)
    if has_price_filter:
        if nft_url:
            btns.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=nft_url)])
        elif title:
            fragment_url = f"https://fragment.com/gifts/{urllib.parse.quote(title)}"
            btns.append([InlineKeyboardButton(text="🔎 Найти на Fragment", url=fragment_url)])
    else:
        # Кнопка на все гифты — идём в профиль (там видны все его NFT)
        if profile_url:
            btns.append([InlineKeyboardButton(text="📦 Все гифты владельца", url=profile_url)])

    # 2. Профиль владельца
    if profile_url:
        label = f"👤 @{username}" if username else "👤 Профиль"
        btns.append([InlineKeyboardButton(text=label, url=profile_url)])

    # 3. Написать — только если есть username
    if username:
        if nft_url and has_price_filter:
            write_text = f"Привет! Хочу купить твой NFT 👉 {nft_url}"
        else:
            write_text = f"Привет! Хочу купить твой NFT {title} #{num}"
        btns.append([InlineKeyboardButton(
            text="✉️ Написать",
            url=f"https://t.me/{username}?text={urllib.parse.quote(write_text)}"
        )])

    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None


# ===================== CORE SEARCH — РЫНОК =====================
async def do_market_search(
    status_msg: Message,
    gift_ids: list[int],
    max_results: int = 100,
    girls_only: bool = False,
    cat: str | None = None,
) -> int:
    """
    Поиск гифтов выставленных на продажу (рынок).
    Реальная цена берётся из resell_amount.
    Фильтрация по категории с допуском.
    Максимум 2 гифта с одного владельца.
    """
    global is_searching
    is_searching = True
    found        = 0
    seen_slugs   = set()
    owner_count: dict[int, int] = {}
    MAX_PER_OWNER  = 2
    has_price_filter = cat is not None

    async def send_nft(item):
        price     = item["price"]
        owner_str = fmt_owner(item["owner"], item["username"], item["name"])
        price_str = f"⭐ {price:,}".replace(",", " ") if price else "цена неизвестна"
        kb = nft_kb(item, has_price_filter=has_price_filter)
        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id,
                text=(
                    f"🏪 <b>{item['title']} #{item['num']}</b>\n"
                    f"👤 {owner_str}\n"
                    f"💰 {price_str} (на продаже)"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
        except Exception as e:
            logger.warning(f"send: {e}")

    try:
        offsets = {gid: "" for gid in gift_ids}
        last_status_update = 0

        while is_searching and found < max_results:
            active = [gid for gid, off in offsets.items() if off is not None]
            if not active:
                break

            got_anything = False

            for batch_start in range(0, len(active), 3):
                if not is_searching or found >= max_results:
                    break

                batch = active[batch_start:batch_start+3]
                pages = await asyncio.gather(*[
                    fetch_market_page(gid, offsets[gid], limit=50) for gid in batch
                ])

                for gid, (items, next_offset) in zip(batch, pages):
                    offsets[gid] = next_offset if next_offset else None

                    for item in items:
                        if not is_searching or found >= max_results:
                            break

                        slug     = item["slug"] or ""
                        owner_id = item["owner_id"]

                        # Дедупликация по slug
                        if slug and slug in seen_slugs:
                            continue
                        if slug:
                            seen_slugs.add(slug)

                        # Лимит 2 гифта с владельца
                        if owner_id:
                            cnt = owner_count.get(owner_id, 0)
                            if cnt >= MAX_PER_OWNER:
                                continue
                            owner_count[owner_id] = cnt + 1

                        # Фильтр по полу
                        if girls_only and not is_girl(item["owner"]):
                            continue

                        # Фильтр по цене с допуском
                        price = item.get("price")
                        if cat and price is not None:
                            if not price_in_category(price, cat):
                                continue

                        found         += 1
                        stats["found"] += 1
                        got_anything   = True

                        await send_nft(item)
                        await asyncio.sleep(0.03)

            now = asyncio.get_event_loop().time()
            if now - last_status_update > 3:
                try:
                    active_count = sum(1 for v in offsets.values() if v is not None)
                    lbl = "👧" if girls_only else "NFT"
                    await status_msg.edit_text(
                        f"🏪 Ищу на рынке... (коллекций: {active_count})\n"
                        f"Найдено {lbl}: <b>{found}</b>",
                        parse_mode="HTML", reply_markup=stop_kb(),
                    )
                    last_status_update = now
                except Exception:
                    pass

            if not got_anything:
                break

    except Exception as e:
        logger.error(f"do_market_search error: {e}")
    finally:
        is_searching = False

    return found


# ===================== CORE SEARCH — ПРОФИЛИ =====================
async def do_profile_search(
    status_msg: Message,
    gift_ids: list[int],
    max_results: int = 100,
    girls_only: bool = False,
    cat: str | None = None,
) -> int:
    """
    Поиск по профилям: берём владельцев с рынка, но показываем их
    показываем их как "профильных" — цена определяется по медиане коллекции.
    Гифты НЕ на продаже эмулируются: показываем владельца + медианную цену.
    Все гифты владельца выводятся подряд.
    """
    global is_searching
    is_searching = True
    found        = 0
    seen_slugs   = set()
    owner_count: dict[int, int] = {}
    MAX_PER_OWNER    = 2
    has_price_filter = cat is not None

    # Сначала фильтруем коллекции по медианной цене
    if cat:
        c = PRICE_CATEGORIES[cat]
        tmin = c["tol_min"]
        tmax = c["tol_max"]

        async def check_col(gid):
            median = await get_median_price(gid)
            if median is None:
                return None
            if tmin is not None and median < tmin:
                return None
            if tmax is not None and median > tmax:
                return None
            return gid

        filtered = []
        for i in range(0, len(gift_ids), 5):
            if not is_searching:
                break
            batch   = gift_ids[i:i+5]
            results = await asyncio.gather(*[check_col(gid) for gid in batch])
            filtered.extend([r for r in results if r is not None])
            await asyncio.sleep(0.2)

        gift_ids = filtered
        if not gift_ids:
            is_searching = False
            return 0

    async def send_profile_nft(item, est_price: int | None):
        owner_str = fmt_owner(item["owner"], item["username"], item["name"])
        price_str = f"⭐ ~{est_price:,}".replace(",", " ") if est_price else "цена по рынку"
        nft_url     = item.get("nft_url")
        profile_url = item.get("profile_url")
        username    = item.get("username")
        title       = item.get("title", "")
        num         = item.get("num", "")

        btns = []

        # 1. Если фильтр по цене — конкретный NFT, иначе — все гифты (профиль)
        if has_price_filter:
            if nft_url:
                btns.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=nft_url)])
            elif title:
                fragment_url = f"https://fragment.com/gifts/{urllib.parse.quote(title)}"
                btns.append([InlineKeyboardButton(text="🔎 Найти на Fragment", url=fragment_url)])
        else:
            if profile_url:
                btns.append([InlineKeyboardButton(text="📦 Все гифты владельца", url=profile_url)])

        # 2. Профиль
        if profile_url:
            label = f"👤 @{username}" if username else "👤 Профиль"
            btns.append([InlineKeyboardButton(text=label, url=profile_url)])

        # 3. Написать
        if username:
            if nft_url and has_price_filter:
                write_text = f"Привет! Хочу купить твой NFT 👉 {nft_url}"
            else:
                write_text = f"Привет! Хочу купить твой NFT {title} #{num}"
            btns.append([InlineKeyboardButton(
                text="✉️ Написать",
                url=f"https://t.me/{username}?text={urllib.parse.quote(write_text)}"
            )])

        kb = InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id,
                text=(
                    f"👤 <b>{title} #{num}</b>\n"
                    f"👤 {owner_str}\n"
                    f"💰 {price_str} (оценка по рынку)"
                ),
                parse_mode="HTML",
                reply_markup=kb,
            )
        except Exception as e:
            logger.warning(f"send_profile: {e}")


    try:
        offsets = {gid: "" for gid in gift_ids}
        last_status_update = 0

        while is_searching and found < max_results:
            active = [gid for gid, off in offsets.items() if off is not None]
            if not active:
                break

            got_anything = False

            for batch_start in range(0, len(active), 3):
                if not is_searching or found >= max_results:
                    break

                batch = active[batch_start:batch_start+3]
                pages = await asyncio.gather(*[
                    fetch_market_page(gid, offsets[gid], limit=50) for gid in batch
                ])

                for gid, (items, next_offset) in zip(batch, pages):
                    offsets[gid] = next_offset if next_offset else None
                    est_price = MARKET_CACHE.get(gid)

                    for item in items:
                        if not is_searching or found >= max_results:
                            break

                        slug = item.get("slug") or ""
                        if slug and slug in seen_slugs:
                            continue
                        if slug:
                            seen_slugs.add(slug)

                        # Лимит 2 гифта с владельца
                        owner_id = item.get("owner_id")
                        if owner_id:
                            cnt = owner_count.get(owner_id, 0)
                            if cnt >= MAX_PER_OWNER:
                                continue
                            owner_count[owner_id] = cnt + 1

                        if girls_only and not is_girl(item["owner"]):
                            continue

                        found         += 1
                        stats["found"] += 1
                        got_anything   = True

                        await send_profile_nft(item, est_price)
                        await asyncio.sleep(0.03)

            now = asyncio.get_event_loop().time()
            if now - last_status_update > 3:
                try:
                    active_count = sum(1 for v in offsets.values() if v is not None)
                    lbl = "👧" if girls_only else "профилей"
                    await status_msg.edit_text(
                        f"👤 Ищу по профилям... (коллекций: {active_count})\n"
                        f"Найдено {lbl}: <b>{found}</b>",
                        parse_mode="HTML", reply_markup=stop_kb(),
                    )
                    last_status_update = now
                except Exception:
                    pass

            if not got_anything:
                break

    except Exception as e:
        logger.error(f"do_profile_search error: {e}")
    finally:
        is_searching = False

    return found


# ===================== RUNNERS =====================
async def start_market_search(cb: CallbackQuery, cat: str | None = None, ids: list | None = None, girls: bool = False):
    global is_searching
    if is_searching:
        await cb.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    label = "🏪 Все NFT на рынке"
    if girls:
        label = "👧 Девушки на рынке"
    elif cat and cat in PRICE_CATEGORIES:
        c = PRICE_CATEGORIES[cat]
        label = f"🏪 {c['label']} — {c['desc']}"

    await cb.answer("🔍 Запускаю...")
    stats["checks"] += 1

    if ids is None:
        if not ALL_GIFT_IDS:
            await load_collections()
        ids = [gid for gid, _ in ALL_GIFT_IDS]

    if not ids:
        await cb.message.answer("❌ Коллекции не загружены.", reply_markup=menu_kb())
        return

    status = await cb.message.answer(
        f"<b>{label}</b>\n\nНайдено: 0",
        parse_mode="HTML", reply_markup=stop_kb()
    )
    found = await do_market_search(status, ids, cat=cat, girls_only=girls)

    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n{label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


async def start_profile_search(cb: CallbackQuery, cat: str | None = None, ids: list | None = None, girls: bool = False):
    global is_searching
    if is_searching:
        await cb.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    label = "👤 Все профили"
    if girls:
        label = "👧 Девушки (профили)"
    elif cat and cat in PRICE_CATEGORIES:
        c = PRICE_CATEGORIES[cat]
        label = f"👤 {c['label']} — {c['desc']}"

    await cb.answer("👤 Ищу по профилям...")
    stats["checks"] += 1

    if ids is None:
        if not ALL_GIFT_IDS:
            await load_collections()
        ids = [gid for gid, _ in ALL_GIFT_IDS]

    if not ids:
        await cb.message.answer("❌ Коллекции не загружены.", reply_markup=menu_kb())
        return

    has_filter = cat is not None
    status_text = (
        f"<b>{label}</b>\n\n⏳ Анализирую цены коллекций..."
        if has_filter else
        f"<b>{label}</b>\n\nНайдено: 0"
    )
    status = await cb.message.answer(status_text, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_profile_search(status, ids, cat=cat, girls_only=girls)

    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n{label}\nНайдено: <b>{found}</b>",
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
                "⚙️ <b>Нужна авторизация Telegram</b>\n\nВведи номер телефона:\n<code>+79001234567</code>",
                parse_mode="HTML"
            )
            await state.set_state(Auth.phone)
            return
    await message.answer(
        "🎁 <b>NFT Market Parser</b>\n\n"
        "🏪 <b>Рынок</b> — гифты выставленные на продажу\n"
        "👤 <b>Профили</b> — владельцы с оценкой по рынку\n\n"
        "👇 Выбери режим:",
        parse_mode="HTML", reply_markup=main_kb()
    )


@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(f"❌ Нет доступа.\n\nТвой ID: <code>{message.from_user.id}</code>", parse_mode="HTML")
        return
    await state.clear()
    users = load_users()
    ok    = await check_authorized()
    await message.answer(
        f"👑 <b>Админ панель</b>\n\n"
        f"🔐 Telethon: <b>{'✅ Авторизован' if ok else '❌ Не авторизован'}</b>\n"
        f"📦 Коллекций: <b>{len(ALL_GIFT_IDS)}</b>\n"
        f"👥 Пользователей: <b>{len(users)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"🆔 Твой ID: <code>{message.from_user.id}</code>", parse_mode="HTML")

@dp.message(Command("cols"))
async def cmd_cols(message: Message):
    if not is_admin(message.from_user.id):
        return
    if not ALL_GIFT_IDS:
        await load_collections()
    lines = [f"📦 <b>Коллекций: {len(ALL_GIFT_IDS)}</b>"]
    for gid, label in ALL_GIFT_IDS[:50]:
        mp = MARKET_CACHE.get(gid, "нет данных")
        lines.append(f"• {label} (id={gid}) | рынок: {mp} ⭐")
    await message.answer("\n".join(lines), parse_mode="HTML")

@dp.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Отменено.", reply_markup=main_kb())


# ===================== CALLBACKS — МЕНЮ =====================
@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer(
        "🎁 <b>NFT Market Parser</b>\n\n"
        "🏪 <b>Рынок</b> — гифты на продаже\n"
        "👤 <b>Профили</b> — владельцы с оценкой\n\n"
        "👇 Выбери режим:",
        parse_mode="HTML", reply_markup=main_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "search_market_menu")
async def cb_market_menu(cb: CallbackQuery):
    await cb.message.answer("🏪 <b>Поиск по рынку</b>\n\nВыбери категорию:", parse_mode="HTML", reply_markup=market_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "search_profile_menu")
async def cb_profile_menu(cb: CallbackQuery):
    await cb.message.answer("👤 <b>Поиск по профилям</b>\n\nВыбери категорию:", parse_mode="HTML", reply_markup=profile_menu_kb())
    await cb.answer()


# ===================== CALLBACKS — РЫНОК =====================
@dp.callback_query(F.data == "mkt_all")
async def cb_mkt_all(cb: CallbackQuery):   await start_market_search(cb)
@dp.callback_query(F.data == "mkt_cheap")
async def cb_mkt_cheap(cb: CallbackQuery): await start_market_search(cb, "cheap")
@dp.callback_query(F.data == "mkt_mid")
async def cb_mkt_mid(cb: CallbackQuery):   await start_market_search(cb, "mid")
@dp.callback_query(F.data == "mkt_hard")
async def cb_mkt_hard(cb: CallbackQuery):  await start_market_search(cb, "hard")
@dp.callback_query(F.data == "mkt_ultra")
async def cb_mkt_ultra(cb: CallbackQuery): await start_market_search(cb, "ultra")
@dp.callback_query(F.data == "mkt_girls")
async def cb_mkt_girls(cb: CallbackQuery): await start_market_search(cb, girls=True)

@dp.callback_query(F.data == "mkt_col")
async def cb_mkt_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("❌ Коллекции не загружены", reply_markup=menu_kb())
        await cb.answer()
        return
    await cb.message.answer("🗂 <b>Выбери коллекцию:</b>", parse_mode="HTML",
                             reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "mktcol_", "search_market_menu"))
    await cb.answer()

@dp.callback_query(F.data.startswith("mktcol_"))
async def cb_mktcol(cb: CallbackQuery):
    idx = int(cb.data[7:])
    lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst): await cb.answer("❌ Не найдено", show_alert=True); return
    await start_market_search(cb, ids=[lst[idx][1]])


# ===================== CALLBACKS — ПРОФИЛИ =====================
@dp.callback_query(F.data == "prf_all")
async def cb_prf_all(cb: CallbackQuery):   await start_profile_search(cb)
@dp.callback_query(F.data == "prf_cheap")
async def cb_prf_cheap(cb: CallbackQuery): await start_profile_search(cb, "cheap")
@dp.callback_query(F.data == "prf_mid")
async def cb_prf_mid(cb: CallbackQuery):   await start_profile_search(cb, "mid")
@dp.callback_query(F.data == "prf_hard")
async def cb_prf_hard(cb: CallbackQuery):  await start_profile_search(cb, "hard")
@dp.callback_query(F.data == "prf_ultra")
async def cb_prf_ultra(cb: CallbackQuery): await start_profile_search(cb, "ultra")
@dp.callback_query(F.data == "prf_girls")
async def cb_prf_girls(cb: CallbackQuery): await start_profile_search(cb, girls=True)

@dp.callback_query(F.data == "prf_col")
async def cb_prf_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("❌ Коллекции не загружены", reply_markup=menu_kb())
        await cb.answer()
        return
    await cb.message.answer("🗂 <b>Выбери коллекцию:</b>", parse_mode="HTML",
                             reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "prfcol_", "search_profile_menu"))
    await cb.answer()

@dp.callback_query(F.data.startswith("prfcol_"))
async def cb_prfcol(cb: CallbackQuery):
    idx = int(cb.data[7:])
    lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst): await cb.answer("❌ Не найдено", show_alert=True); return
    await start_profile_search(cb, ids=[lst[idx][1]])


# ===================== CALLBACKS — СТОП / СТАТИСТИКА =====================
@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    is_searching = False
    await cb.answer("⏹ Останавливаю...")
    try:
        await cb.message.edit_text("⏹ <b>Поиск остановлен</b>", parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        pass

@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    await cb.message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено: <b>{stats['found']}</b>\n"
        f"👥 Пользователей: <b>{get_user_count()}</b>",
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
        text = "👥 Пользователей пока нет."
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin_panel")]])
        if edit:
            await message.edit_text(text, reply_markup=kb)
        else:
            await message.answer(text, reply_markup=kb)
        return

    start = page * PAGE
    end   = min(start + PAGE, total)
    chunk = all_items[start:end]

    lines = [f"👥 <b>Пользователи {start+1}–{end} из {total}</b>\n"]
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

        # Строка карточки
        card = f"{i}. <code>{uid}</code>"
        if uname_str:
            card += f" | {uname_str}"
        if name_str:
            card += f" | {name_str}"
        card += f"\n    📅 {date_str}"
        lines.append(card)

    # Навигация
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"users_page_{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"users_page_{page+1}"))

    rows = []
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="🔙 Админ", callback_data="admin_panel")])
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
        f"👑 <b>Админ панель</b>\n\n"
        f"🔐 Telethon: <b>{'✅ Авторизован' if ok else '❌ Не авторизован'}</b>\n"
        f"📦 Коллекций: <b>{len(ALL_GIFT_IDS)}</b>\n"
        f"👥 Пользователей: <b>{len(users)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await cb.answer()



@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await cb.message.answer("📢 <b>Рассылка</b>\n\nОтправь сообщение.\n/cancel — отмена",
                             parse_mode="HTML", reply_markup=cancel_kb())
    await cb.answer()


@dp.message(Broadcast.message)
async def broadcast_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(mid=message.message_id, cid=message.chat.id)
    await state.set_state(None)
    await message.answer("Подтверди:", reply_markup=confirm_kb())

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def cb_broadcast_send(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    data = await state.get_data()
    mid, cid = data.get("mid"), data.get("cid")
    if not mid:
        await cb.answer("❌ Нет сообщения", show_alert=True)
        return
    users  = load_users()
    uids   = list(users.keys())
    status = await cb.message.answer(f"📢 Отправляю {len(uids)} пользователям...")
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
                await status.edit_text(f"📢 {i+1}/{len(uids)}...")
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await status.edit_text(
        f"✅ Отправлено: <b>{ok}</b>\n❌ Ошибок: <b>{fail}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    u = load_users()
    await cb.message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: <b>{len(u)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено: <b>{stats['found']}</b>\n"
        f"📦 Коллекций: <b>{len(ALL_GIFT_IDS)}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "admin_auth")
async def cb_admin_auth(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
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
    await cb.message.answer("✅ Вышел из TG.", reply_markup=admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.answer("❌ Отменено", reply_markup=admin_kb())
    await cb.answer()


# ===================== AUTH =====================
@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Формат: <code>+79001234567</code>", parse_mode="HTML")
        return
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
            await asyncio.sleep(1)
        res = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=res.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("📨 Код отправлен. Введи без пробелов: <code>12345</code>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")
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
            f"✅ <b>Авторизован как @{me.username or me.first_name}!</b>\n"
            f"Коллекций: <b>{len(ALL_GIFT_IDS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("🔐 Введи пароль 2FA:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")

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
            f"✅ <b>Авторизован как @{me.username or me.first_name}!</b>\n"
            f"Коллекций: <b>{len(ALL_GIFT_IDS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: <code>{e}</code>", parse_mode="HTML")


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
            logger.warning("Не авторизован — пройди авторизацию через /start")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
