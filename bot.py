import asyncio
import logging
import urllib.parse
import os
import json
import time
import datetime
import re
from telethon import TelegramClient
from telethon.tl.functions.payments import (
    GetResaleStarGiftsRequest, GetStarGiftsRequest, GetSavedStarGiftsRequest
)
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

API_ID       = 28687552
API_HASH     = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN    = "8406363273:AAF2L-LfRhUVMrbVLeZBLLI7IgkFoMtyfGM"
ADMIN_ID     = "8726084830"
SESSION_NAME = "nft_session"
USERS_FILE   = "users.json"
PRICENFT_BOT = "pricenftbot"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot       = Bot(token=BOT_TOKEN)
dp        = Dispatcher(storage=MemoryStorage())
tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

stats        = {"checks": 0, "found": 0}
is_searching = False
ALL_GIFT_IDS    = []
NFT_COLLECTIONS = {}
PRICE_FLOOR_CACHE = {}
NFT_CACHE = {}
USER_BOOST = {}
USER_MIN_GIFTS = {}
USER_MAX_GIFTS = {}
DEFAULT_BOOST     = 100
DEFAULT_MIN_GIFTS = 2
DEFAULT_MAX_GIFTS = 0

PRICE_CATEGORIES = {
    "cheap":   {"label": "Дешёвые",  "floor_min": None,   "floor_max": 2000},
    "mid":     {"label": "Средние",  "floor_min": 2000,   "floor_max": 5000},
    "hard":    {"label": "Сложные",  "floor_min": 5000,   "floor_max": 20000},
    "ultra":   {"label": "Хард",     "floor_min": 20000,  "floor_max": 100000},
    "extreme": {"label": "Экстрим",  "floor_min": 100000, "floor_max": None},
}

GIRL_NAMES = {
    "анна","мария","екатерина","анастасия","наталья","ольга","елена","татьяна",
    "ирина","юлия","алина","виктория","дарья","полина","ксения","валерия",
    "александра","надежда","людмила","галина","лиза","диана","sofia","софия",
    "кристина","светлана","милана","арина","вера","жанна","ангелина","карина",
    "оксана","нина","лариса","регина","маша","катя","даша","оля","лена",
    "юля","настя","поля","ксюша","вика","соня","таня","надя","галя","аня",
    "ника","алиса","злата","ева","эвелина","камилла","яна","влада","руслана",
    "anna","maria","kate","natasha","olga","elena","tatiana","irina","julia",
    "alina","victoria","dasha","polina","ksenia","valeria","alexandra","diana",
    "sophia","lisa","christina","sveta","milana","arina","vera","zhanna",
    "angela","angelina","karina","oksana","nina","larisa","regina","natalia",
    "ekaterina","anastasia","alice","eva","emma","mia","lily","rose","sara",
    "sarah","katie","jessica","ashley","emily","olivia","ava","isabella",
    "abby","madison",
}
GIRL_KW = [
    "girl","lady","princess","queen","baby","cute","sweetie","babe","honey",
    "cutie","beautiful","pretty","lovely","darling","goddess","angel","bunny",
    "kitty","барби","принцесса","королева","девочка","красотка","кошечка",
    "зайка","лапочка","милашка","красавица","ангелочек","богиня","малышка",
]
BOY_KW = [
    "boss","king","boy","man","bro","dude","male","guy","master","lord",
    "sultan","caesar","ivan","roman","dmitri","sergey","andrey",
    "паша","коля","вася","петя","миша","гриша","стас",
]


def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return {str(uid): {"username": None, "joined": 0} for uid in data}
            return data
    return {}

def save_users(u):
    with open(USERS_FILE, "w") as f:
        json.dump(u, f, ensure_ascii=False, indent=2)

def add_user(uid, username=None, first_name=None, last_name=None):
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

def get_user_count(): return len(load_users())
def get_boost(uid):     return USER_BOOST.get(uid, DEFAULT_BOOST)
def get_min_gifts(uid): return USER_MIN_GIFTS.get(uid, DEFAULT_MIN_GIFTS)
def get_max_gifts(uid): return USER_MAX_GIFTS.get(uid, DEFAULT_MAX_GIFTS)


class Auth(StatesGroup):
    phone    = State()
    code     = State()
    password = State()

class Broadcast(StatesGroup):
    message = State()


def is_admin(uid): return int(uid) == int(ADMIN_ID)

async def check_authorized():
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
        return await tg_client.is_user_authorized()
    except Exception:
        return False

def is_girl(owner):
    if not owner:
        return False
    first = (getattr(owner, "first_name", "") or "").lower().strip()
    last  = (getattr(owner, "last_name",  "") or "").lower().strip()
    uname = (getattr(owner, "username",   "") or "").lower().strip()
    full  = f"{first} {last} {uname}"
    for kw in BOY_KW:
        if kw in full: return False
    for name in GIRL_NAMES:
        if first.startswith(name) or last.startswith(name): return True
        if len(name) >= 3 and name in uname: return True
    for kw in GIRL_KW:
        if kw in full: return True
    return False

def get_resell_price(gift):
    ra = getattr(gift, "resell_amount", None)
    if ra is None: return None
    lst = ra if isinstance(ra, (list, tuple)) else [ra]
    for item in lst:
        a = getattr(item, "amount", None)
        if a is not None:
            try:
                v = int(a)
                if 0 < v < 100_000_000: return v
            except: pass
        try:
            v = int(item)
            if 0 < v < 100_000_000: return v
        except: pass
    return None

def get_owner(gift, users_map):
    obj = getattr(gift, "owner_id", None)
    if obj is None: return None, None
    uid = getattr(obj, "user_id", None) or getattr(obj, "id", None)
    if uid is None and isinstance(obj, int): uid = obj
    if uid is None: return None, None
    return users_map.get(int(uid)), int(uid)

def fmt_owner(owner, username, name):
    if name and username: return f"{name} (@{username})"
    if username: return f"@{username}"
    if name: return name
    return "Скрыт"

def fmt_timestamp(ts):
    if not ts: return "неизвестно"
    return datetime.datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")

def make_nft_url(gift):
    slug = (getattr(gift, "slug", None) or getattr(gift, "unique_id", None) or "")
    slug = str(slug).strip()
    if slug and slug not in ("None", "", "nan", "0"):
        try:
            int(slug)
        except ValueError:
            return f"https://t.me/nft/{slug}"
    return None

def gifts_in_range(count, mn, mx):
    if count < mn: return False
    if mx > 0 and count > mx: return False
    return True

def cache_owner(uid, owner, username, name, profile_url, items):
    NFT_CACHE[uid] = {"owner": owner, "username": username,
                      "name": name, "profile_url": profile_url, "items": items}


async def get_floor_price(gift_id):
    if gift_id in PRICE_FLOOR_CACHE: return PRICE_FLOOR_CACHE[gift_id]
    try:
        result = await tg_client(GetResaleStarGiftsRequest(gift_id=gift_id, offset="", limit=20))
        gifts  = getattr(result, "gifts", None) or []
        prices = [p for g in gifts for p in [get_resell_price(g)] if p and p > 0]
        if not prices: return None
        prices.sort()
        floor = prices[max(0, len(prices) // 4)]
        PRICE_FLOOR_CACHE[gift_id] = floor
        return floor
    except Exception as e:
        logger.error(f"floor gid={gift_id}: {e}"); return None

def floor_in_category(floor, cat):
    c = PRICE_CATEGORIES.get(cat)
    if not c: return True
    if c["floor_min"] is not None and floor < c["floor_min"]: return False
    if c["floor_max"] is not None and floor > c["floor_max"]: return False
    return True

def price_ok_for_floor(price, floor, boost):
    return floor * 0.7 <= price <= floor * (1.0 + boost / 100.0)


async def load_collections():
    global ALL_GIFT_IDS, NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        ALL_GIFT_IDS = []; NFT_COLLECTIONS = {}
        for gift in result.gifts:
            gid = getattr(gift, "id", None); title = getattr(gift, "title", None)
            if gid is None or not title: continue  # только NFT с title
            ALL_GIFT_IDS.append((gid, title)); NFT_COLLECTIONS[title] = gid
        logger.info(f"NFT коллекций: {len(ALL_GIFT_IDS)}")
    except Exception as e: logger.error(f"load_collections: {e}")


async def fetch_market_page(gift_id, offset, limit=50):
    try:
        result    = await tg_client(GetResaleStarGiftsRequest(gift_id=gift_id, offset=offset, limit=limit))
        users_map = {int(u.id): u for u in (getattr(result, "users", None) or [])}
        gifts     = getattr(result, "gifts", None) or []
        items = []
        for gift in gifts:
            owner, owner_uid = get_owner(gift, users_map)
            username = getattr(owner, "username", None) if owner else None
            fn = (getattr(owner, "first_name", "") or "") if owner else ""
            ln = (getattr(owner, "last_name",  "") or "") if owner else ""
            name = f"{fn} {ln}".strip()
            nft_url = make_nft_url(gift)
            profile_url = (f"https://t.me/{username}" if username
                           else (f"tg://user?id={owner_uid}" if owner_uid else None))
            items.append({"owner": owner, "owner_id": owner_uid, "username": username,
                          "name": name, "title": getattr(gift, "title", "?"),
                          "num": getattr(gift, "num", "?"), "price": get_resell_price(gift),
                          "nft_url": nft_url, "profile_url": profile_url})
        return items, getattr(result, "next_offset", "") or ""
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds}s"); await asyncio.sleep(e.seconds + 3); return [], ""
    except Exception as e:
        logger.error(f"fetch_market gid={gift_id}: {e}"); return [], ""


async def fetch_profile_via_pricenftbot(username, timeout=12.0):
    try:
        bot_entity = await tg_client.get_entity(PRICENFT_BOT)
        await tg_client.send_message(bot_entity, f"https://t.me/{username}")
        deadline = asyncio.get_event_loop().time() + timeout
        nfts = []
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(2.0)
            messages = await tg_client.get_messages(bot_entity, limit=5)
            for msg in messages:
                if msg.out: continue
                if msg.date and (time.time() - msg.date.timestamp()) > timeout + 10: continue
                text = msg.text or ""
                if msg.reply_markup:
                    for row in (getattr(msg.reply_markup, "rows", None) or []):
                        for btn in (getattr(row, "buttons", None) or []):
                            url = getattr(btn, "url", None)
                            label = (getattr(btn, "text", "") or "").strip()
                            if url and "t.me/nft/" in url:
                                m = re.match(r"^(.+?)\s*#(\w+)$", label)
                                if m: nfts.append({"title": m.group(1).strip(), "num": m.group(2), "nft_url": url})
                                else: nfts.append({"title": label or url.split("/")[-1], "num": "?", "nft_url": url})
                if not nfts:
                    for m in re.finditer(r"https://t\.me/nft/([A-Za-z0-9_-]+)", text):
                        nfts.append({"title": m.group(1), "num": "?", "nft_url": m.group(0)})
                if nfts or ("нет" in text.lower() and "nft" in text.lower()): return nfts
            if nfts: break
        return nfts
    except Exception as e:
        logger.warning(f"pricenftbot {username}: {e}"); return []


async def fetch_saved_gifts_all(user_id, max_pages=5):
    all_items = []; offset = ""
    for _ in range(max_pages):
        try:
            result = await tg_client(GetSavedStarGiftsRequest(
                peer=await tg_client.get_input_entity(user_id), offset=offset, limit=50))
            for gift in (getattr(result, "gifts", None) or []):
                nft_url = make_nft_url(gift)
                if not nft_url:
                    inner = getattr(gift, "gift", None)
                    if inner: nft_url = make_nft_url(inner)
                inner = getattr(gift, "gift", None)
                title = getattr(inner, "title", None) or getattr(gift, "title", "?")
                num   = getattr(gift, "num", "?")
                all_items.append({"title": title, "num": num, "nft_url": nft_url})
            offset = getattr(result, "next_offset", "") or ""
            if not offset: break
            await asyncio.sleep(0.1)
        except Exception as e: logger.error(f"saved_gifts uid={user_id}: {e}"); break
    return all_items


async def get_profile_nfts(user_id, username):
    if username:
        nfts = await fetch_profile_via_pricenftbot(username)
        if nfts: return nfts
    gifts = await fetch_saved_gifts_all(user_id)
    return [g for g in gifts if g.get("nft_url")]


def bottom_menu_kb():
    return ReplyKeyboardMarkup(keyboard=[[
        KeyboardButton(text="🚀 Старт"),
        KeyboardButton(text="🔍 Поиск"),
        KeyboardButton(text="☰ Меню"),
    ]], resize_keyboard=True, persistent=True)

def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск NFT",  callback_data="search_menu")],
        [InlineKeyboardButton(text="⚙️ Настройки",  callback_data="settings_menu")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
    ])

def search_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 По ценам",     callback_data="mode_price")],
        [InlineKeyboardButton(text="👧 Девушки",      callback_data="mode_girls")],
        [InlineKeyboardButton(text="🏪 По рынку",     callback_data="mode_market")],
        [InlineKeyboardButton(text="👤 По профилям",  callback_data="mode_profile")],
        [InlineKeyboardButton(text="🗂 По коллекции", callback_data="mode_col")],
        [InlineKeyboardButton(text="◀️ Назад",        callback_data="menu")],
    ])

def settings_menu_kb(uid):
    mg = get_min_gifts(uid); mx = get_max_gifts(uid); bst = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "∞"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📉 Мин. гифтов: {mg}",      callback_data="set_min_gifts")],
        [InlineKeyboardButton(text=f"📈 Макс. гифтов: {mx_str}", callback_data="set_max_gifts")],
        [InlineKeyboardButton(text=f"🚀 Буст цен: {bst}%",       callback_data="set_boost")],
        [InlineKeyboardButton(text="◀️ Назад",                    callback_data="menu")],
    ])

def min_gifts_picker_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1",  callback_data="mingifts_1"),
         InlineKeyboardButton(text="2",  callback_data="mingifts_2"),
         InlineKeyboardButton(text="3",  callback_data="mingifts_3"),
         InlineKeyboardButton(text="5",  callback_data="mingifts_5")],
        [InlineKeyboardButton(text="10", callback_data="mingifts_10"),
         InlineKeyboardButton(text="15", callback_data="mingifts_15"),
         InlineKeyboardButton(text="20", callback_data="mingifts_20"),
         InlineKeyboardButton(text="50", callback_data="mingifts_50")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="settings_menu")],
    ])

def max_gifts_picker_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="♾ Без лимита", callback_data="maxgifts_0")],
        [InlineKeyboardButton(text="5",  callback_data="maxgifts_5"),
         InlineKeyboardButton(text="10", callback_data="maxgifts_10"),
         InlineKeyboardButton(text="20", callback_data="maxgifts_20")],
        [InlineKeyboardButton(text="50",  callback_data="maxgifts_50"),
         InlineKeyboardButton(text="100", callback_data="maxgifts_100"),
         InlineKeyboardButton(text="200", callback_data="maxgifts_200")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="settings_menu")],
    ])

def boost_picker_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="30%",  callback_data="boost_30"),
         InlineKeyboardButton(text="50%",  callback_data="boost_50"),
         InlineKeyboardButton(text="100%", callback_data="boost_100")],
        [InlineKeyboardButton(text="150%", callback_data="boost_150"),
         InlineKeyboardButton(text="200%", callback_data="boost_200")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="settings_menu")],
    ])

def price_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💚 Дешёвые (до 2 000)",      callback_data="price_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2 000-5 000)",   callback_data="price_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5 000-20 000)",  callback_data="price_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20 000-100 000)",   callback_data="price_ultra")],
        [InlineKeyboardButton(text="💀 Экстрим (100 000+)",      callback_data="price_extreme")],
        [InlineKeyboardButton(text="◀️ Назад",                   callback_data="search_menu")],
    ])

def girls_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏪 Девушки на рынке",   callback_data="girls_market")],
        [InlineKeyboardButton(text="👤 Девушки в профилях", callback_data="girls_profile")],
        [InlineKeyboardButton(text="◀️ Назад",              callback_data="search_menu")],
    ])

def market_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все NFT",            callback_data="mkt_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2 000)", callback_data="mkt_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2-5 тыс)",  callback_data="mkt_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5-20 тыс)", callback_data="mkt_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20-100 тыс)",  callback_data="mkt_ultra")],
        [InlineKeyboardButton(text="💀 Экстрим (100к+)",    callback_data="mkt_extreme")],
        [InlineKeyboardButton(text="◀️ Назад",              callback_data="search_menu")],
    ])

def profile_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Все профили", callback_data="prf_all")],
        [InlineKeyboardButton(text="◀️ Назад",       callback_data="search_menu")],
    ])

def col_source_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏪 Рынок",   callback_data="col_market")],
        [InlineKeyboardButton(text="👤 Профили", callback_data="col_profile")],
        [InlineKeyboardButton(text="◀️ Назад",   callback_data="search_menu")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⏹ СТОП", callback_data="stop_search")]])

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
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")]])

def confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="admin_broadcast_confirm")],
        [InlineKeyboardButton(text="❌ Отмена",    callback_data="admin_cancel")],
    ])

def col_kb(names, prefix, back):
    rows = []
    for i in range(0, len(names), 2):
        row = [InlineKeyboardButton(text=names[i], callback_data=f"{prefix}{i}")]
        if i + 1 < len(names):
            row.append(InlineKeyboardButton(text=names[i+1], callback_data=f"{prefix}{i+1}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def owner_card_kb(username, profile_url, owner_uid):
    btns = []
    if profile_url:
        btns.append([InlineKeyboardButton(text=f"👤 @{username}" if username else "👤 Профиль", url=profile_url)])
    if username:
        btns.append([InlineKeyboardButton(text="✉️ Написать",
            url=f"https://t.me/{username}?text={urllib.parse.quote('Привет! Хочу купить твои NFT')}")])
    btns.append([InlineKeyboardButton(text="🎁 Показать все NFT", callback_data=f"shownft_{owner_uid}")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def nft_list_kb(nft_items, username, profile_url):
    btns = []
    for g in nft_items:
        url = g.get("nft_url"); title = g.get("title", "?"); num = g.get("num", "?"); price = g.get("price")
        if url:
            label = f"🎁 {title} #{num}"
            if price: label += f" — {price:,}".replace(",", " ") + " zv"
            btns.append([InlineKeyboardButton(text=label, url=url)])
    if profile_url:
        btns.append([InlineKeyboardButton(text=f"👤 @{username}" if username else "👤 Профиль", url=profile_url)])
    if username:
        btns.append([InlineKeyboardButton(text="✉️ Написать",
            url=f"https://t.me/{username}?text={urllib.parse.quote('Привет! Хочу купить твои NFT')}")])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None


async def do_market_search(status_msg, gift_ids, cat=None, girls_only=False,
                           max_results=200, boost=100, min_gifts=2, max_gifts=0):
    global is_searching
    is_searching = True; found = 0; seen_slugs = set(); has_cat = cat is not None
    await status_msg.edit_text("<b>🔎 Анализирую коллекции...</b>", reply_markup=stop_kb())
    valid_gids = []
    for i in range(0, len(gift_ids), 5):
        if not is_searching: break
        batch = gift_ids[i:i+5]
        floors = await asyncio.gather(*[get_floor_price(g) for g in batch])
        for gid, floor in zip(batch, floors):
            if floor is None: continue
            if cat and not floor_in_category(floor, cat): continue
            valid_gids.append((gid, floor))
        await asyncio.sleep(0.3)
    if not valid_gids: is_searching = False; return 0
    offsets = {gid: "" for gid, _ in valid_gids}
    floor_map = {gid: fl for gid, fl in valid_gids}
    buffers = {gid: [] for gid, _ in valid_gids}
    owner_buckets = {}; owner_count_cat = {}
    MAX_PER_OWNER_CAT = 3; last_upd = 0.0

    async def flush_owners():
        nonlocal found
        for uid, bucket in [(u, b) for u, b in list(owner_buckets.items())
                             if gifts_in_range(len(b["items"]), min_gifts, max_gifts)]:
            if not is_searching: break
            items = bucket["items"]; username = bucket["username"]; profile_url = bucket["profile_url"]
            owner_str = fmt_owner(bucket["owner"], username, bucket["name"])
            cache_owner(uid, bucket["owner"], username, bucket["name"], profile_url, items)
            kb = owner_card_kb(username, profile_url, uid)
            try:
                await status_msg.bot.send_message(chat_id=status_msg.chat.id,
                    text=f"<b>👤 {owner_str}</b>
<b>NFT на рынке: {len(items)}</b>",
                    parse_mode="HTML", reply_markup=kb)
                found += len(items); stats["found"] += len(items)
            except Exception as e: logger.warning(f"flush: {e}")
            del owner_buckets[uid]; await asyncio.sleep(0.07)

    try:
        while is_searching and found < max_results:
            active = [gid for gid, off in offsets.items() if off is not None or buffers[gid]]
            if not active: break
            made = False
            for gid in list(active):
                if not is_searching or found >= max_results: break
                floor = floor_map[gid]
                if not buffers[gid] and offsets.get(gid) is not None:
                    items, nxt = await fetch_market_page(gid, offsets[gid], limit=50)
                    offsets[gid] = nxt if nxt else None
                    for item in items:
                        nft_url = item.get("nft_url") or ""
                        slug = nft_url.split("/")[-1] if nft_url else ""
                        if slug and slug in seen_slugs: continue
                        if slug: seen_slugs.add(slug)
                        if girls_only and not is_girl(item["owner"]): continue
                        price = item.get("price")
                        if price is None: continue
                        if has_cat:
                            if not price_ok_for_floor(price, floor, boost): continue
                            oid = item["owner_id"]
                            if oid:
                                if owner_count_cat.get(oid, 0) >= MAX_PER_OWNER_CAT: continue
                                owner_count_cat[oid] = owner_count_cat.get(oid, 0) + 1
                            buffers[gid].append(item)
                        else:
                            oid = item["owner_id"]
                            if oid:
                                if oid not in owner_buckets:
                                    owner_buckets[oid] = {"owner": item["owner"], "username": item["username"],
                                        "name": item["name"], "profile_url": item["profile_url"], "items": []}
                                owner_buckets[oid]["items"].append(item)
                if has_cat and buffers[gid]:
                    item = buffers[gid].pop(0)
                    price = item["price"]; username = item["username"]; profile_url = item["profile_url"]
                    nft_url = item["nft_url"]; title = item["title"]; num = item["num"]
                    owner_uid = item["owner_id"] or 0
                    owner_str = fmt_owner(item["owner"], username, item["name"])
                    price_str = f"{price:,}".replace(",", " ") if price else "?"
                    cache_owner(owner_uid, item["owner"], username, item["name"], profile_url, [item])
                    kb = owner_card_kb(username, profile_url, owner_uid)
                    nft_line = (f"
<b><a href="{nft_url}">{title} #{num}</a></b>" if nft_url else f"
<b>{title} #{num}</b>")
                    try:
                        await status_msg.bot.send_message(chat_id=status_msg.chat.id,
                            text=f"<b>👤 {owner_str}</b>{nft_line}
<b>💰 {price_str} zv</b>",
                            parse_mode="HTML", reply_markup=kb, disable_web_page_preview=True)
                        found += 1; stats["found"] += 1; made = True
                    except Exception as e: logger.warning(f"send: {e}")
                    await asyncio.sleep(0.05)
            if not has_cat: await flush_owners(); made = True
            now = asyncio.get_event_loop().time()
            if now - last_upd > 3:
                try:
                    act = sum(1 for v in offsets.values() if v is not None)
                    lbl = "девушек" if girls_only else "NFT"
                    await status_msg.edit_text(
                        f"<b>🔎 Ищу на рынке... (коллекций: {act})</b>
<b>Найдено {lbl}: {found}</b>",
                        parse_mode="HTML", reply_markup=stop_kb())
                    last_upd = now
                except Exception: pass
            if not active: break
            if not made and has_cat: break
        if not has_cat and is_searching: await flush_owners()
    except Exception as e: logger.error(f"do_market_search: {e}")
    finally: is_searching = False
    return found


async def do_profile_search(status_msg, gift_ids, girls_only=False,
                             max_results=200, min_gifts=2, max_gifts=0):
    global is_searching
    is_searching = True; found = 0
    seen_owners = {}; owner_queue = []
    market_offsets = {gid: "" for gid in gift_ids}; last_upd = 0.0

    async def collect_more(n):
        collected = 0
        active = [g for g, o in market_offsets.items() if o is not None]
        if not active: return False
        for gid in active:
            if collected >= n: break
            if market_offsets.get(gid) is None: continue
            items, nxt = await fetch_market_page(gid, market_offsets[gid], limit=50)
            market_offsets[gid] = nxt if nxt else None
            for item in items:
                uid = item.get("owner_id")
                if not uid or uid in seen_owners: continue
                owner_obj = item["owner"]
                if girls_only and not is_girl(owner_obj): continue
                seen_owners[uid] = (owner_obj, item["username"], item["name"])
                owner_queue.append((uid, owner_obj, item["username"], item["name"]))
                collected += 1
            await asyncio.sleep(0.1)
        return collected > 0

    try:
        while is_searching and found < max_results:
            if len(owner_queue) < 5:
                has_more = await collect_more(30)
                if not has_more and not owner_queue: break
            if not owner_queue: break
            batch = owner_queue[:5]; owner_queue[:] = owner_queue[5:]
            for (uid, owner_obj, username, name) in batch:
                if not is_searching or found >= max_results: break
                nft_gifts = await get_profile_nfts(uid, username)
                if not gifts_in_range(len(nft_gifts), min_gifts, max_gifts): continue
                profile_url = f"https://t.me/{username}" if username else f"tg://user?id={uid}"
                owner_str = fmt_owner(owner_obj, username, name)
                cache_owner(uid, owner_obj, username, name, profile_url, nft_gifts)
                kb = owner_card_kb(username, profile_url, uid)
                try:
                    await status_msg.bot.send_message(chat_id=status_msg.chat.id,
                        text=f"<b>👤 {owner_str}</b>
<b>NFT в профиле: {len(nft_gifts)}</b>",
                        parse_mode="HTML", reply_markup=kb)
                    found += len(nft_gifts); stats["found"] += len(nft_gifts)
                except Exception as e: logger.warning(f"profile block: {e}")
                await asyncio.sleep(0.2)
            now = asyncio.get_event_loop().time()
            if now - last_upd > 3:
                try:
                    act = sum(1 for v in market_offsets.values() if v is not None)
                    lbl = "девушек" if girls_only else "профилей"
                    await status_msg.edit_text(
                        f"<b>🔎 Ищу по профилям... (коллекций: {act}, очередь: {len(owner_queue)})</b>
"
                        f"<b>Просмотрено: {len(seen_owners)} | Найдено {lbl}: {found}</b>",
                        parse_mode="HTML", reply_markup=stop_kb())
                    last_upd = now
                except Exception: pass
    except Exception as e: logger.error(f"do_profile_search: {e}")
    finally: is_searching = False
    return found


async def ensure_collections():
    if not ALL_GIFT_IDS: await load_collections()
    return [gid for gid, _ in ALL_GIFT_IDS]

async def run_market(cb, cat=None, girls=False, ids=None):
    global is_searching
    if is_searching: await cb.answer("Поиск уже идёт!", show_alert=True); return
    await cb.answer("Запускаю..."); stats["checks"] += 1
    uid = cb.from_user.id
    if ids is None: ids = await ensure_collections()
    if not ids: await cb.message.answer("<b>Коллекции не загружены.</b>", parse_mode="HTML", reply_markup=menu_kb()); return
    boost = get_boost(uid); mn = get_min_gifts(uid); mx = get_max_gifts(uid)
    mx_str = str(mx) if mx > 0 else "∞"
    label = "👧 Девушки (рынок)" if girls else (PRICE_CATEGORIES[cat]["label"] if cat else "📦 Все NFT")
    status = await cb.message.answer(
        f"<b>{label}</b>
<b>Гифтов: от {mn} до {mx_str} | Буст: {boost}%</b>

<b>Найдено: 0</b>",
        parse_mode="HTML", reply_markup=stop_kb())
    found = await do_market_search(status, ids, cat=cat, girls_only=girls, boost=boost, min_gifts=mn, max_gifts=mx)
    try: await status.edit_text(f"<b>✅ Готово! {label}</b>
<b>Найдено: {found}</b>", parse_mode="HTML", reply_markup=menu_kb())
    except Exception: pass

async def run_profile(cb, girls=False, ids=None):
    global is_searching
    if is_searching: await cb.answer("Поиск уже идёт!", show_alert=True); return
    await cb.answer("Запускаю..."); stats["checks"] += 1
    uid = cb.from_user.id
    if ids is None: ids = await ensure_collections()
    if not ids: await cb.message.answer("<b>Коллекции не загружены.</b>", parse_mode="HTML", reply_markup=menu_kb()); return
    mn = get_min_gifts(uid); mx = get_max_gifts(uid); mx_str = str(mx) if mx > 0 else "∞"
    label = "👧 Девушки (профили)" if girls else "👤 Все профили"
    status = await cb.message.answer(
        f"<b>{label}</b>
<b>Гифтов: от {mn} до {mx_str}</b>

<b>Собираю владельцев...</b>",
        parse_mode="HTML", reply_markup=stop_kb())
    found = await do_profile_search(status, ids, girls_only=girls, min_gifts=mn, max_gifts=mx)
    try: await status.edit_text(f"<b>✅ Готово! {label}</b>
<b>Найдено: {found}</b>", parse_mode="HTML", reply_markup=menu_kb())
    except Exception: pass


WELCOME_TEXT = "<b>🌊 Neptun Parser</b>
<b>лучший парсер для поиска мамонтёнка!</b>

<b>Нажми Поиск NFT чтобы начать:</b>"

@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    add_user(message.from_user.id, message.from_user.username, message.from_user.first_name, message.from_user.last_name)
    if not await check_authorized() and is_admin(message.from_user.id):
        await message.answer("<b>Нужна авторизация</b>
<b>Введи номер:</b> <code>+79001234567</code>", parse_mode="HTML", reply_markup=bottom_menu_kb())
        await state.set_state(Auth.phone); return
    await message.answer(WELCOME_TEXT, parse_mode="HTML", reply_markup=bottom_menu_kb())
    await message.answer("<b>Выбери действие:</b>", parse_mode="HTML", reply_markup=main_kb())

@dp.message(F.text == "🚀 Старт")
async def btn_start(message: Message):
    await message.answer(WELCOME_TEXT, parse_mode="HTML", reply_markup=main_kb())

@dp.message(F.text == "🔍 Поиск")
async def btn_search(message: Message):
    await message.answer("<b>Выбери режим поиска:</b>", parse_mode="HTML", reply_markup=search_menu_kb())

@dp.message(F.text == "☰ Меню")
async def btn_menu(message: Message):
    uid = message.from_user.id; mn = get_min_gifts(uid); mx = get_max_gifts(uid); bst = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "∞"
    await message.answer(
        f"<b>🌊 Neptun Parser</b>

<b>⚙️ Настройки:</b>
<b>  Мин: {mn} | Макс: {mx_str} | Буст: {bst}%</b>

<b>Поисков: {stats['checks']} | Найдено: {stats['found']}</b>",
        parse_mode="HTML", reply_markup=main_kb())

@dp.message(Command("clear"))
async def cmd_clear(message: Message):
    global is_searching; is_searching = False
    await message.answer("<b>Поиск остановлен.</b>", parse_mode="HTML", reply_markup=menu_kb())

@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(f"<b>Нет доступа. ID:</b> <code>{message.from_user.id}</code>", parse_mode="HTML"); return
    await state.clear(); users = load_users(); ok = await check_authorized()
    await message.answer(
        f"<b>Админ панель</b>

<b>Telethon: {'✅ Авторизован' if ok else '❌ Не авторизован'}</b>
<b>Коллекций: {len(ALL_GIFT_IDS)}</b>
<b>Пользователей: {len(users)}</b>
<b>Поисков: {stats['checks']} | Найдено: {stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb())

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"<b>ID:</b> <code>{message.from_user.id}</code>", parse_mode="HTML")

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear(); await message.answer("<b>Отменено.</b>", parse_mode="HTML", reply_markup=main_kb())


@dp.callback_query(F.data.startswith("shownft_"))
async def cb_show_nft(cb: CallbackQuery):
    uid = int(cb.data[8:]); cached = NFT_CACHE.get(uid)
    if not cached:
        await cb.answer("Загружаю NFT...", show_alert=False)
        nfts = await get_profile_nfts(uid, None)
        if not nfts: await cb.answer("NFT не найдены или профиль закрыт", show_alert=True); return
        NFT_CACHE[uid] = {"owner": None, "username": None, "name": None, "profile_url": f"tg://user?id={uid}", "items": nfts}
        cached = NFT_CACHE[uid]
    else: await cb.answer()
    items = cached.get("items", []); username = cached.get("username"); profile_url = cached.get("profile_url")
    owner_str = fmt_owner(cached.get("owner"), username, cached.get("name"))
    if not items: await cb.answer("Список пуст", show_alert=True); return
    kb = nft_list_kb(items, username, profile_url)
    await cb.message.answer(f"<b>🎁 NFT — {owner_str}</b>
<b>Всего: {len(items)}</b>", parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "settings_menu")
async def cb_settings(cb: CallbackQuery):
    uid = cb.from_user.id; mn = get_min_gifts(uid); mx = get_max_gifts(uid); bst = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "∞"
    await cb.message.answer(f"<b>⚙️ Настройки</b>

<b>Мин. гифтов: {mn}</b>
<b>Макс. гифтов: {mx_str}</b>
<b>Буст: {bst}%</b>", parse_mode="HTML", reply_markup=settings_menu_kb(uid)); await cb.answer()

@dp.callback_query(F.data == "set_min_gifts")
async def cb_set_min(cb: CallbackQuery):
    await cb.message.answer("<b>📉 Минимум гифтов у владельца:</b>", parse_mode="HTML", reply_markup=min_gifts_picker_kb()); await cb.answer()

@dp.callback_query(F.data == "set_max_gifts")
async def cb_set_max(cb: CallbackQuery):
    await cb.message.answer("<b>📈 Максимум гифтов (0=без лимита):</b>", parse_mode="HTML", reply_markup=max_gifts_picker_kb()); await cb.answer()

@dp.callback_query(F.data == "set_boost")
async def cb_set_boost(cb: CallbackQuery):
    await cb.message.answer("<b>🚀 Буст цен</b>

<b>100%=x0.7..x2.0 | 150%=x2.5 | 200%=x3.0</b>", parse_mode="HTML", reply_markup=boost_picker_kb()); await cb.answer()

@dp.callback_query(F.data.startswith("mingifts_"))
async def cb_mingifts(cb: CallbackQuery):
    val = int(cb.data.split("_")[1]); USER_MIN_GIFTS[cb.from_user.id] = val
    await cb.answer(f"Мин. гифтов: {val} ✅", show_alert=True); await cb.message.edit_reply_markup(reply_markup=None)

@dp.callback_query(F.data.startswith("maxgifts_"))
async def cb_maxgifts(cb: CallbackQuery):
    val = int(cb.data.split("_")[1]); USER_MAX_GIFTS[cb.from_user.id] = val
    label = "без лимита" if val == 0 else str(val)
    await cb.answer(f"Макс. гифтов: {label} ✅", show_alert=True); await cb.message.edit_reply_markup(reply_markup=None)

@dp.callback_query(F.data.startswith("boost_"))
async def cb_boost(cb: CallbackQuery):
    val = int(cb.data.split("_")[1]); USER_BOOST[cb.from_user.id] = val
    await cb.answer(f"Буст: {val}% ✅", show_alert=True); await cb.message.edit_reply_markup(reply_markup=None)

@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear(); await cb.message.answer(WELCOME_TEXT, parse_mode="HTML", reply_markup=main_kb()); await cb.answer()

@dp.callback_query(F.data == "search_menu")
async def cb_search_menu(cb: CallbackQuery):
    await cb.message.answer("<b>Выбери режим поиска:</b>", parse_mode="HTML", reply_markup=search_menu_kb()); await cb.answer()

@dp.callback_query(F.data == "mode_price")
async def cb_mode_price(cb: CallbackQuery):
    await cb.message.answer("<b>💰 По ценам</b>

<b>Выбери диапазон:</b>", parse_mode="HTML", reply_markup=price_menu_kb()); await cb.answer()

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

@dp.callback_query(F.data == "mode_girls")
async def cb_mode_girls(cb: CallbackQuery):
    await cb.message.answer("<b>👧 Девушки</b>

<b>Выбери источник:</b>", parse_mode="HTML", reply_markup=girls_menu_kb()); await cb.answer()

@dp.callback_query(F.data == "girls_market")
async def cb_gm(cb): await run_market(cb, girls=True)
@dp.callback_query(F.data == "girls_profile")
async def cb_gp(cb): await run_profile(cb, girls=True)

@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.message.answer("<b>🏪 По рынку</b>

<b>Выбери фильтр:</b>", parse_mode="HTML", reply_markup=market_menu_kb()); await cb.answer()

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

@dp.callback_query(F.data == "mode_profile")
async def cb_mode_profile(cb: CallbackQuery):
    await cb.message.answer("<b>👤 По профилям</b>", parse_mode="HTML", reply_markup=profile_menu_kb()); await cb.answer()

@dp.callback_query(F.data == "prf_all")
async def cb_pa(cb): await run_profile(cb)

@dp.callback_query(F.data == "mode_col")
async def cb_mode_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS: await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("<b>Коллекции не загружены</b>", parse_mode="HTML", reply_markup=menu_kb()); await cb.answer(); return
    await cb.message.answer("<b>🗂 По коллекции</b>

<b>Выбери источник:</b>", parse_mode="HTML", reply_markup=col_source_kb()); await cb.answer()

@dp.callback_query(F.data == "col_market")
async def cb_col_market(cb: CallbackQuery):
    if not NFT_COLLECTIONS: await load_collections()
    await cb.message.answer("<b>🏪 Выбери коллекцию (рынок):</b>", parse_mode="HTML",
        reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "mktcol_", "mode_col")); await cb.answer()

@dp.callback_query(F.data == "col_profile")
async def cb_col_profile(cb: CallbackQuery):
    if not NFT_COLLECTIONS: await load_collections()
    await cb.message.answer("<b>👤 Выбери коллекцию (профили):</b>", parse_mode="HTML",
        reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "prfcol_", "mode_col")); await cb.answer()

@dp.callback_query(F.data.startswith("mktcol_"))
async def cb_mktcol(cb: CallbackQuery):
    idx = int(cb.data[7:]); lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst): await cb.answer("Не найдено", show_alert=True); return
    await run_market(cb, ids=[lst[idx][1]])

@dp.callback_query(F.data.startswith("prfcol_"))
async def cb_prfcol(cb: CallbackQuery):
    idx = int(cb.data[7:]); lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst): await cb.answer("Не найдено", show_alert=True); return
    await run_profile(cb, ids=[lst[idx][1]])

@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching; is_searching = False; await cb.answer("Останавливаю...")
    try: await cb.message.edit_text("<b>⏹ Поиск остановлен.</b>", parse_mode="HTML", reply_markup=menu_kb())
    except Exception: pass

@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    uid = cb.from_user.id; mn = get_min_gifts(uid); mx = get_max_gifts(uid); bst = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "∞"
    await cb.message.answer(
        f"<b>📊 Статистика</b>

<b>Поисков: {stats['checks']}</b>
<b>Найдено: {stats['found']}</b>
<b>Пользователей: {get_user_count()}</b>

<b>⚙️ Настройки:</b>
<b>Мин: {mn} | Макс: {mx_str} | Буст: {bst}%</b>",
        parse_mode="HTML"); await cb.answer()

@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    await show_users_page(cb.message, 0, False); await cb.answer()

@dp.callback_query(F.data.startswith("users_page_"))
async def cb_users_page(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    await show_users_page(cb.message, int(cb.data[len("users_page_"):]), True); await cb.answer()

async def show_users_page(message, page, edit):
    users = load_users(); all_items = list(users.items()); total = len(all_items); PAGE = 20
    if total == 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_panel")]])
        fn = message.edit_text if edit else message.answer
        await fn("<b>Пользователей нет.</b>", parse_mode="HTML", reply_markup=kb); return
    start = page * PAGE; end = min(start + PAGE, total); chunk = all_items[start:end]
    lines = [f"<b>Пользователи {start+1}-{end} из {total}</b>
"]
    for i, (uid, info) in enumerate(chunk, start + 1):
        if isinstance(info, dict):
            uname = info.get("username") or ""; first = info.get("first_name") or ""
            last = info.get("last_name") or ""; joined = info.get("joined", 0)
        else: uname = first = last = ""; joined = 0
        name = " ".join(p for p in [first, last] if p)
        card = f"<b>{i}. <code>{uid}</code>"
        if uname: card += f" @{uname}"
        if name: card += f" | {name}"
        card += f"
    {fmt_timestamp(joined)}</b>"
        lines.append(card)
    nav = []
    if page > 0: nav.append(InlineKeyboardButton(text="◀ Назад", callback_data=f"users_page_{page-1}"))
    if end < total: nav.append(InlineKeyboardButton(text="Вперёд ▶", callback_data=f"users_page_{page+1}"))
    rows = [nav] if nav else []
    rows.append([InlineKeyboardButton(text="Админ", callback_data="admin_panel")])
    fn = message.edit_text if edit else message.answer
    await fn("
".join(lines), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    users = load_users(); ok = await check_authorized()
    await cb.message.answer(f"<b>Админ панель</b>

<b>Telethon: {'✅ Авторизован' if ok else '❌ Не авторизован'}</b>
<b>Коллекций: {len(ALL_GIFT_IDS)}</b>
<b>Пользователей: {len(users)}</b>", parse_mode="HTML", reply_markup=admin_kb()); await cb.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id): return
    await state.set_state(Broadcast.message)
    await cb.message.answer("<b>Отправь сообщение для рассылки. /cancel — отмена</b>", parse_mode="HTML", reply_markup=cancel_kb()); await cb.answer()

@dp.message(Broadcast.message)
async def broadcast_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.update_data(mid=message.message_id, cid=message.chat.id); await state.set_state(None)
    await message.answer("<b>Подтверди отправку:</b>", parse_mode="HTML", reply_markup=confirm_kb())

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def cb_broadcast_send(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id): return
    data = await state.get_data(); mid, cid = data.get("mid"), data.get("cid")
    if not mid: await cb.answer("Нет сообщения", show_alert=True); return
    users = load_users(); uids = list(users.keys())
    status = await cb.message.answer(f"<b>Рассылка {len(uids)} пользователям...</b>", parse_mode="HTML"); await cb.answer()
    ok = fail = 0
    for i, uid in enumerate(uids):
        try: await bot.copy_message(int(uid), cid, mid); ok += 1
        except Exception: fail += 1
        if (i + 1) % 20 == 0:
            try: await status.edit_text(f"<b>{i+1}/{len(uids)}...</b>", parse_mode="HTML")
            except Exception: pass
        await asyncio.sleep(0.05)
    await status.edit_text(f"<b>Отправлено: {ok}</b>
<b>Ошибок: {fail}</b>", parse_mode="HTML", reply_markup=admin_kb())
    await state.clear()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    u = load_users()
    await cb.message.answer(f"<b>Статистика</b>

<b>Пользователей: {len(u)}</b>
<b>Поисков: {stats['checks']}</b>
<b>Найдено: {stats['found']}</b>
<b>Коллекций: {len(ALL_GIFT_IDS)}</b>", parse_mode="HTML", reply_markup=admin_kb()); await cb.answer()

@dp.callback_query(F.data == "admin_auth")
async def cb_admin_auth(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id): return
    await state.clear(); await cb.message.answer("<b>Введи номер:</b> <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone); await cb.answer()

@dp.callback_query(F.data == "admin_logout")
async def cb_admin_logout(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    try: await tg_client.log_out()
    except Exception: pass
    await cb.message.answer("<b>Вышел из TG.</b>", parse_mode="HTML", reply_markup=admin_kb()); await cb.answer()

@dp.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id): return
    await state.clear(); await cb.message.answer("<b>Отменено</b>", parse_mode="HTML", reply_markup=admin_kb()); await cb.answer()


@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    phone = message.text.strip()
    if not phone.startswith("+"): await message.answer("<b>Формат:</b> <code>+79001234567</code>", parse_mode="HTML"); return
    try:
        if not tg_client.is_connected(): await tg_client.connect(); await asyncio.sleep(1)
        res = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=res.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("<b>Код отправлен. Введи:</b> <code>1 2 3 4 5</code>", parse_mode="HTML")
    except Exception as e: await message.answer(f"<b>Ошибка:</b> <code>{e}</code>", parse_mode="HTML"); await state.clear()

@dp.message(Auth.code)
async def auth_code(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    code = message.text.strip().replace(" ", ""); data = await state.get_data()
    try:
        await tg_client.sign_in(phone=data["phone"], code=code, phone_code_hash=data["phone_code_hash"])
        me = await tg_client.get_me(); await state.clear(); await load_collections()
        await message.answer(f"<b>✅ Авторизован как @{me.username or me.first_name}!</b>
<b>Коллекций: {len(ALL_GIFT_IDS)}</b>", parse_mode="HTML", reply_markup=main_kb())
    except SessionPasswordNeededError: await state.set_state(Auth.password); await message.answer("<b>Введи пароль 2FA:</b>", parse_mode="HTML")
    except Exception as e: await message.answer(f"<b>Ошибка:</b> <code>{e}</code>", parse_mode="HTML")

@dp.message(Auth.password)
async def auth_password(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try:
        await tg_client.sign_in(password=message.text.strip())
        me = await tg_client.get_me(); await state.clear(); await load_collections()
        await message.answer(f"<b>✅ Авторизован как @{me.username or me.first_name}!</b>
<b>Коллекций: {len(ALL_GIFT_IDS)}</b>", parse_mode="HTML", reply_markup=main_kb())
    except Exception as e: await message.answer(f"<b>Неверный пароль:</b> <code>{e}</code>", parse_mode="HTML")


async def main():
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("Neptun Parser запущен!")
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info(f"Авторизован, NFT коллекций: {len(ALL_GIFT_IDS)}")
        else:
            logger.warning("Не авторизован — пройди /start")
    except Exception as e:
        logger.error(f"Ошибка старта: {e}")
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
