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
    CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

API_ID       = 28687552
API_HASH     = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN    = "8406363273:AAG-ucchhMA09n8j_XSGFtE02iu3Oiwzj_0"
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

# Флаги онбординга (первый запуск)
ONBOARDING_DONE = set()

PRICE_CATEGORIES = {
    "cheap":   {"label": "Дешевые",  "floor_min": None,   "floor_max": 2000},
    "mid":     {"label": "Средние",  "floor_min": 2000,   "floor_max": 5000},
    "hard":    {"label": "Сложные",  "floor_min": 5000,   "floor_max": 20000},
    "ultra":   {"label": "Хард",     "floor_min": 20000,  "floor_max": 100000},
    "extreme": {"label": "Экстрим",  "floor_min": 100000, "floor_max": None},
}

GIRL_NAMES = {
    "анна","мария","екатерина","анастасия","наталья","ольга","елена","татьяна","ирина",
    "юлия","алина","виктория","дарья","полина","ксения","валерия","александра","надежда",
    "людмила","галина","лиза","диана","sofia","софия","кристина","светлана","милана",
    "арина","вера","жанна","ангелина","карина","оксана","нина","лариса","регина",
    "маша","катя","даша","оля","лена","юля","настя","поля","ксюша","вика","соня",
    "таня","надя","галя","аня","ника","алиса","злата","ева","эвелина","камилла",
    "яна","влада","руслана",
    "anna","maria","kate","natasha","olga","elena","tatiana","irina","julia","alina",
    "victoria","dasha","polina","ksenia","valeria","alexandra","diana","sophia",
    "lisa","christina","sveta","milana","arina","vera","zhanna","angela","angelina",
    "karina","oksana","nina","larisa","regina","natalia","ekaterina","anastasia",
    "alice","eva","emma","mia","lily","rose","sara","sarah","katie",
    "jessica","ashley","emily","olivia","ava","isabella","abby","madison",
}
GIRL_KW = [
    "girl","lady","princess","queen","baby","cute","sweetie","babe","honey","cutie",
    "beautiful","pretty","lovely","darling","goddess","angel","bunny","kitty",
    "барби","принцесса","королева","девочка","красотка","кошечка","зайка","лапочка",
    "милашка","красавица","ангелочек","богиня","малышка",
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
        save_users(u)
        return True  # новый пользователь
    else:
        if isinstance(u[key], dict):
            if username:   u[key]["username"]   = username
            if first_name: u[key]["first_name"] = first_name
            if last_name:  u[key]["last_name"]  = last_name
        save_users(u)
        return False  # уже был

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

class Onboarding(StatesGroup):
    min_gifts = State()
    max_gifts = State()
    boost     = State()

class SetGifts(StatesGroup):
    min_gifts = State()
    max_gifts = State()


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
    full  = first + " " + last + " " + uname
    for kw in BOY_KW:
        if kw in full:
            return False
    for name in GIRL_NAMES:
        if first.startswith(name) or last.startswith(name):
            return True
        if len(name) >= 3 and name in uname:
            return True
    for kw in GIRL_KW:
        if kw in full:
            return True
    return False

def get_resell_price(gift):
    ra = getattr(gift, "resell_amount", None)
    if ra is None:
        return None
    lst = ra if isinstance(ra, (list, tuple)) else [ra]
    for item in lst:
        a = getattr(item, "amount", None)
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

def get_owner(gift, users_map):
    obj = getattr(gift, "owner_id", None)
    if obj is None:
        return None, None
    uid = getattr(obj, "user_id", None) or getattr(obj, "id", None)
    if uid is None and isinstance(obj, int):
        uid = obj
    if uid is None:
        return None, None
    return users_map.get(int(uid)), int(uid)

def fmt_owner(owner, username, name):
    if name and username:
        return name + " (@" + username + ")"
    if username:
        return "@" + username
    if name:
        return name
    return "Скрыт"

def fmt_timestamp(ts):
    if not ts:
        return "неизвестно"
    return datetime.datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")

def make_nft_url(gift):
    slug = (getattr(gift, "slug", None) or getattr(gift, "unique_id", None) or "")
    slug = str(slug).strip()
    if slug and slug not in ("None", "", "nan", "0"):
        try:
            int(slug)
        except ValueError:
            return "https://t.me/nft/" + slug
    return None

def gifts_in_range(count, mn, mx):
    if count < mn:
        return False
    if mx > 0 and count > mx:
        return False
    return True

def cache_owner(uid, owner, username, name, profile_url, items):
    NFT_CACHE[uid] = {
        "owner": owner, "username": username,
        "name": name, "profile_url": profile_url, "items": items,
    }


async def get_floor_price(gift_id):
    if gift_id in PRICE_FLOOR_CACHE:
        return PRICE_FLOOR_CACHE[gift_id]
    try:
        result = await tg_client(GetResaleStarGiftsRequest(gift_id=gift_id, offset="", limit=20))
        gifts  = getattr(result, "gifts", None) or []
        prices = []
        for g in gifts:
            p = get_resell_price(g)
            if p and p > 0:
                prices.append(p)
        if not prices:
            return None
        prices.sort()
        floor = prices[max(0, len(prices) // 4)]
        PRICE_FLOOR_CACHE[gift_id] = floor
        return floor
    except Exception as e:
        logger.error("floor gid=%s: %s", gift_id, e)
        return None

def floor_in_category(floor, cat):
    c = PRICE_CATEGORIES.get(cat)
    if not c:
        return True
    if c["floor_min"] is not None and floor < c["floor_min"]:
        return False
    if c["floor_max"] is not None and floor > c["floor_max"]:
        return False
    return True

def price_ok_for_floor(price, floor, boost):
    return floor * 0.7 <= price <= floor * (1.0 + boost / 100.0)


async def load_collections():
    global ALL_GIFT_IDS, NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        ALL_GIFT_IDS    = []
        NFT_COLLECTIONS = {}
        for gift in result.gifts:
            gid   = getattr(gift, "id",    None)
            title = getattr(gift, "title", None)
            if gid is None:
                continue
            label = title if title else ("Gift #" + str(gid))
            ALL_GIFT_IDS.append((gid, label))
            if title:
                NFT_COLLECTIONS[title] = gid
        logger.info("Коллекций загружено: %d", len(ALL_GIFT_IDS))
    except Exception as e:
        logger.error("load_collections: %s", e)


async def fetch_market_page(gift_id, offset, limit=100):
    try:
        result    = await tg_client(GetResaleStarGiftsRequest(gift_id=gift_id, offset=offset, limit=limit))
        users_map = {int(u.id): u for u in (getattr(result, "users", None) or [])}
        gifts     = getattr(result, "gifts", None) or []
        items     = []
        for gift in gifts:
            owner, owner_uid = get_owner(gift, users_map)
            username = getattr(owner, "username", None) if owner else None
            fn = (getattr(owner, "first_name", "") or "") if owner else ""
            ln = (getattr(owner, "last_name",  "") or "") if owner else ""
            name = (fn + " " + ln).strip()
            nft_url = make_nft_url(gift)
            if username:
                profile_url = "https://t.me/" + username
            elif owner_uid:
                profile_url = "tg://user?id=" + str(owner_uid)
            else:
                profile_url = None
            items.append({
                "owner": owner, "owner_id": owner_uid,
                "username": username, "name": name,
                "title": getattr(gift, "title", "?"),
                "num":   getattr(gift, "num",   "?"),
                "price": get_resell_price(gift),
                "nft_url": nft_url,
                "profile_url": profile_url,
            })
        return items, getattr(result, "next_offset", "") or ""
    except FloodWaitError as e:
        logger.warning("FloodWait %ds", e.seconds)
        await asyncio.sleep(e.seconds + 1)
        return [], ""
    except Exception as e:
        logger.error("fetch_market gid=%s: %s", gift_id, e)
        return [], ""


async def fetch_profile_via_pricenftbot(username, timeout=12.0):
    try:
        bot_entity = await tg_client.get_entity(PRICENFT_BOT)
        await tg_client.send_message(bot_entity, "https://t.me/" + username)
        deadline = asyncio.get_event_loop().time() + timeout
        nfts = []
        while asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(2.0)
            messages = await tg_client.get_messages(bot_entity, limit=5)
            for msg in messages:
                if msg.out:
                    continue
                if msg.date and (time.time() - msg.date.timestamp()) > timeout + 10:
                    continue
                text = msg.text or ""
                if msg.reply_markup:
                    for row in (getattr(msg.reply_markup, "rows", None) or []):
                        for btn in (getattr(row, "buttons", None) or []):
                            url   = getattr(btn, "url", None)
                            label = (getattr(btn, "text", "") or "").strip()
                            if url and "t.me/nft/" in url:
                                m = re.match(r"^(.+?)\s*#(\w+)$", label)
                                if m:
                                    nfts.append({"title": m.group(1).strip(), "num": m.group(2), "nft_url": url})
                                else:
                                    nfts.append({"title": label or url.split("/")[-1], "num": "?", "nft_url": url})
                if not nfts:
                    for m in re.finditer(r"https://t\.me/nft/([A-Za-z0-9_-]+)", text):
                        nfts.append({"title": m.group(1), "num": "?", "nft_url": m.group(0)})
                if nfts or ("нет" in text.lower() and "nft" in text.lower()):
                    return nfts
            if nfts:
                break
        return nfts
    except Exception as e:
        logger.warning("pricenftbot %s: %s", username, e)
        return []


async def fetch_saved_gifts_all(user_id, max_pages=5):
    all_items = []
    offset    = ""
    for _ in range(max_pages):
        try:
            result = await tg_client(GetSavedStarGiftsRequest(
                peer=await tg_client.get_input_entity(user_id),
                offset=offset, limit=50,
            ))
            for gift in (getattr(result, "gifts", None) or []):
                nft_url = make_nft_url(gift)
                if not nft_url:
                    inner = getattr(gift, "gift", None)
                    if inner:
                        nft_url = make_nft_url(inner)
                inner = getattr(gift, "gift", None)
                title = getattr(inner, "title", None) or getattr(gift, "title", "?")
                num   = getattr(gift, "num", "?")
                all_items.append({"title": title, "num": num, "nft_url": nft_url})
            offset = getattr(result, "next_offset", "") or ""
            if not offset:
                break
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error("saved_gifts uid=%s: %s", user_id, e)
            break
    return all_items


async def get_profile_nfts(user_id, username):
    if username:
        nfts = await fetch_profile_via_pricenftbot(username)
        if nfts:
            return nfts
    gifts = await fetch_saved_gifts_all(user_id)
    return [g for g in gifts if g.get("nft_url")]


# ===================== KEYBOARDS =====================

def main_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Старт")]],
        resize_keyboard=True,
        one_time_keyboard=False,
    )

# Шаг 1: выбор режима поиска (рынок или профиль)
def search_mode_select_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="По рынку",    callback_data="mode_market")],
        [InlineKeyboardButton(text="По профилю",  callback_data="mode_profile")],
        [InlineKeyboardButton(text="Настройки",   callback_data="settings_menu")],
        [InlineKeyboardButton(text="Статистика",  callback_data="stats")],
    ])

# Шаг 2а: категории рынка
def market_cat_kb():
    rows = [
        [InlineKeyboardButton(text="Все NFT",          callback_data="cat_all")],
        [InlineKeyboardButton(text="Дешевые (до 2К)",  callback_data="cat_cheap")],
        [InlineKeyboardButton(text="Средние (2-5К)",   callback_data="cat_mid")],
        [InlineKeyboardButton(text="Сложные (5-20К)",  callback_data="cat_hard")],
        [InlineKeyboardButton(text="Хард (20-100К)",   callback_data="cat_ultra")],
        [InlineKeyboardButton(text="Экстрим (100К+)",  callback_data="cat_extreme")],
        [InlineKeyboardButton(text="Назад",             callback_data="search_mode_select")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# Шаг 2б: категории профиля — те же что и рынок
def profile_cat_kb():
    rows = [
        [InlineKeyboardButton(text="Все профили",      callback_data="pcat_all")],
        [InlineKeyboardButton(text="Дешевые (до 2К)",  callback_data="pcat_cheap")],
        [InlineKeyboardButton(text="Средние (2-5К)",   callback_data="pcat_mid")],
        [InlineKeyboardButton(text="Сложные (5-20К)",  callback_data="pcat_hard")],
        [InlineKeyboardButton(text="Хард (20-100К)",   callback_data="pcat_ultra")],
        [InlineKeyboardButton(text="Экстрим (100К+)",  callback_data="pcat_extreme")],
        [InlineKeyboardButton(text="Назад",             callback_data="search_mode_select")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# Шаг 3: кого искать (после выбора категории)
def who_search_kb(cat, mode):
    # mode: "mkt" или "prf"
    prefix = mode + "_" + cat + "_"
    back   = "mode_market" if mode == "mkt" else "mode_profile"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Всех",    callback_data=prefix + "all")],
        [InlineKeyboardButton(text="Девушек", callback_data=prefix + "girls")],
        [InlineKeyboardButton(text="Назад",   callback_data=back)],
    ])

def settings_menu_kb(uid):
    mg     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    bst    = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мин. гифтов: " + str(mg),   callback_data="set_min_gifts")],
        [InlineKeyboardButton(text="Макс. гифтов: " + mx_str,   callback_data="set_max_gifts")],
        [InlineKeyboardButton(text="Буст цен: " + str(bst) + "%", callback_data="set_boost")],
        [InlineKeyboardButton(text="Назад",                       callback_data="menu")],
    ])

def gifts_input_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="settings_menu")],
    ])

def boost_picker_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="30%",  callback_data="boost_30"),
            InlineKeyboardButton(text="50%",  callback_data="boost_50"),
            InlineKeyboardButton(text="100%", callback_data="boost_100"),
        ],
        [
            InlineKeyboardButton(text="150%", callback_data="boost_150"),
            InlineKeyboardButton(text="200%", callback_data="boost_200"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="settings_menu")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="СТОП", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Поиск",  callback_data="search_mode_select")],
        [InlineKeyboardButton(text="Меню",   callback_data="menu")],
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Рассылка",       callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Пользователи",   callback_data="admin_users")],
        [InlineKeyboardButton(text="Статистика",     callback_data="admin_stats")],
        [InlineKeyboardButton(text="Авторизация TG", callback_data="admin_auth")],
        [InlineKeyboardButton(text="Выйти из TG",    callback_data="admin_logout")],
        [InlineKeyboardButton(text="В меню",         callback_data="menu")],
    ])

def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="admin_cancel")],
    ])

def confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отправить", callback_data="admin_broadcast_confirm")],
        [InlineKeyboardButton(text="Отмена",    callback_data="admin_cancel")],
    ])

def col_source_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Рынок",   callback_data="col_market")],
        [InlineKeyboardButton(text="Профили", callback_data="col_profile")],
        [InlineKeyboardButton(text="Назад",   callback_data="search_mode_select")],
    ])

def col_kb(names, prefix, back):
    rows = []
    for i in range(0, len(names), 2):
        row = [InlineKeyboardButton(text=names[i], callback_data=prefix + str(i))]
        if i + 1 < len(names):
            row.append(InlineKeyboardButton(text=names[i+1], callback_data=prefix + str(i+1)))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="🔙 Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def owner_card_kb(username, profile_url, owner_uid):
    btns = []
    if profile_url:
        label = "@" + username if username else "Профиль"
        btns.append([InlineKeyboardButton(text=label, url=profile_url)])
    if username:
        msg = urllib.parse.quote("Привет! Хочу купить твои NFT")
        btns.append([InlineKeyboardButton(text="✉️ Написать", url="https://t.me/" + username + "?text=" + msg)])
    btns.append([InlineKeyboardButton(text="Показать все NFT", callback_data="shownft_" + str(owner_uid))])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def nft_list_kb(nft_items, username, profile_url):
    btns = []
    for g in nft_items:
        url   = g.get("nft_url")
        title = g.get("title", "?")
        num   = g.get("num", "?")
        price = g.get("price")
        if url:
            label = str(title) + " #" + str(num)
            if price:
                label += " " + str(price) + " звезд"
            btns.append([InlineKeyboardButton(text=label, url=url)])
    if profile_url:
        label = "@" + username if username else "Профиль"
        btns.append([InlineKeyboardButton(text=label, url=profile_url)])
    if username:
        msg = urllib.parse.quote("Привет! Хочу купить твои NFT")
        btns.append([InlineKeyboardButton(text="✉️ Написать", url="https://t.me/" + username + "?text=" + msg)])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

# Онбординг кнопки
def onboarding_min_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Пропустить (2 по умолчанию)", callback_data="ob_min_2")],
    ])

def onboarding_max_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Без лимита", callback_data="ob_max_0")],
        [InlineKeyboardButton(text="Пропустить", callback_data="ob_max_0")],
    ])

def onboarding_boost_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="30%",  callback_data="ob_boost_30"),
            InlineKeyboardButton(text="50%",  callback_data="ob_boost_50"),
            InlineKeyboardButton(text="100%", callback_data="ob_boost_100"),
        ],
        [
            InlineKeyboardButton(text="150%", callback_data="ob_boost_150"),
            InlineKeyboardButton(text="200%", callback_data="ob_boost_200"),
        ],
    ])


# ===================== CORE: РЫНОК =====================
async def do_market_search(
    status_msg,
    gift_ids,
    cat=None,
    girls_only=False,
    max_results=200,
    boost=100,
    min_gifts=2,
    max_gifts=0,
    profile_only=False,  # True = только те у кого нет лота на рынке
):
    global is_searching
    is_searching = True
    found        = 0
    seen_slugs   = set()
    has_cat      = cat is not None

    await status_msg.edit_text("<b>Анализирую коллекции...</b>", reply_markup=stop_kb())

    # Параллельная загрузка флоров для скорости
    async def batch_floors(gids, size=20):
        valid = []
        for i in range(0, len(gids), size):
            if not is_searching:
                break
            batch  = gids[i:i+size]
            floors = await asyncio.gather(*[get_floor_price(g) for g in batch])
            for gid, floor in zip(batch, floors):
                if floor is None:
                    continue
                if cat and not floor_in_category(floor, cat):
                    continue
                valid.append((gid, floor))
            await asyncio.sleep(0.05)
        return valid

    valid_gids = await batch_floors(gift_ids)

    if not valid_gids:
        is_searching = False
        return 0

    offsets         = {gid: "" for gid, _ in valid_gids}
    floor_map       = {gid: fl for gid, fl in valid_gids}
    buffers         = {gid: [] for gid, _ in valid_gids}
    owner_buckets   = {}
    owner_count_cat = {}
    owner_on_market = set()  # владельцы у кого есть лот на рынке
    MAX_PER_OWNER   = 3
    last_upd        = 0.0

    async def flush_owners():
        nonlocal found
        ready = [
            (uid, bk) for uid, bk in list(owner_buckets.items())
            if gifts_in_range(len(bk["items"]), min_gifts, max_gifts)
        ]
        for uid, bucket in ready:
            if not is_searching:
                break
            items       = bucket["items"]
            username    = bucket["username"]
            profile_url = bucket["profile_url"]
            owner_str   = fmt_owner(bucket["owner"], username, bucket["name"])
            cache_owner(uid, bucket["owner"], username, bucket["name"], profile_url, items)
            kb  = owner_card_kb(username, profile_url, uid)
            txt = "<b>Владелец: " + owner_str + "\nNFT на рынке: " + str(len(items)) + "</b>"
            try:
                await status_msg.bot.send_message(
                    chat_id=status_msg.chat.id,
                    text=txt,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
                found += len(items)
                stats["found"] += len(items)
            except Exception as e:
                logger.warning("flush: %s", e)
            del owner_buckets[uid]
            await asyncio.sleep(0.03)

    try:
        while is_searching and found < max_results:
            active = [gid for gid, off in offsets.items() if off is not None or buffers[gid]]
            if not active:
                break
            made = False

            # Параллельная загрузка страниц для нескольких коллекций
            fetch_tasks = []
            fetch_gids  = []
            for gid in list(active):
                if not buffers[gid] and offsets.get(gid) is not None:
                    fetch_tasks.append(fetch_market_page(gid, offsets[gid], limit=100))
                    fetch_gids.append(gid)
                if len(fetch_tasks) >= 10:
                    break

            if fetch_tasks:
                results = await asyncio.gather(*fetch_tasks)
                for gid, (items, nxt) in zip(fetch_gids, results):
                    offsets[gid] = nxt if nxt else None
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
                        oid = item["owner_id"]
                        if oid:
                            owner_on_market.add(oid)
                        floor = floor_map[gid]
                        if has_cat:
                            if not price_ok_for_floor(price, floor, boost):
                                continue
                            if oid:
                                if owner_count_cat.get(oid, 0) >= MAX_PER_OWNER:
                                    continue
                                owner_count_cat[oid] = owner_count_cat.get(oid, 0) + 1
                            buffers[gid].append(item)
                        else:
                            if oid:
                                if oid not in owner_buckets:
                                    owner_buckets[oid] = {
                                        "owner": item["owner"],
                                        "username": item["username"],
                                        "name": item["name"],
                                        "profile_url": item["profile_url"],
                                        "items": [],
                                    }
                                owner_buckets[oid]["items"].append(item)

            # Отправляем из буферов
            for gid in list(active):
                if not is_searching or found >= max_results:
                    break
                if has_cat and buffers[gid]:
                    item        = buffers[gid].pop(0)
                    price       = item["price"]
                    username    = item["username"]
                    profile_url = item["profile_url"]
                    nft_url     = item["nft_url"]
                    title       = item["title"]
                    num         = item["num"]
                    owner_uid   = item["owner_id"] or 0
                    owner_str   = fmt_owner(item["owner"], username, item["name"])
                    price_str   = str(price) + " звезд" if price else "?"
                    cache_owner(owner_uid, item["owner"], username, item["name"], profile_url, [item])
                    kb = owner_card_kb(username, profile_url, owner_uid)
                    if nft_url:
                        nft_line = '\n<b><a href="' + nft_url + '">' + str(title) + " #" + str(num) + "</a></b>"
                    else:
                        nft_line = "\n<b>" + str(title) + " #" + str(num) + "</b>"
                    txt = "<b>Владелец: " + owner_str + "</b>" + nft_line + "\n<b>Цена: " + price_str + "</b>"
                    try:
                        await status_msg.bot.send_message(
                            chat_id=status_msg.chat.id,
                            text=txt,
                            parse_mode="HTML",
                            reply_markup=kb,
                            disable_web_page_preview=True,
                        )
                        found += 1
                        stats["found"] += 1
                        made = True
                    except Exception as e:
                        logger.warning("send: %s", e)
                    await asyncio.sleep(0.01)

            if not has_cat:
                await flush_owners()
                made = True

            now = asyncio.get_event_loop().time()
            if now - last_upd > 2:
                try:
                    act = sum(1 for v in offsets.values() if v is not None)
                    lbl = "девушек" if girls_only else "NFT"
                    txt = "<b>Ищу на рынке... коллекций: " + str(act) + "\nНайдено " + lbl + ": " + str(found) + "</b>"
                    await status_msg.edit_text(txt, parse_mode="HTML", reply_markup=stop_kb())
                    last_upd = now
                except Exception:
                    pass

            if not active:
                break
            if not made and has_cat:
                break

        if not has_cat and is_searching:
            await flush_owners()

    except Exception as e:
        logger.error("do_market_search: %s", e)
    finally:
        is_searching = False
    return found


# ===================== CORE: ПРОФИЛИ (без лота на рынке) =====================
async def do_profile_search(
    status_msg,
    gift_ids,
    girls_only=False,
    max_results=200,
    min_gifts=2,
    max_gifts=0,
):
    """
    Ищет владельцев у которых НЕТ ни одного NFT выставленного на рынке,
    но есть NFT в профиле (сохранённые гифты).
    """
    global is_searching
    is_searching = True
    found        = 0
    seen_owners  = {}
    owner_queue  = []
    owners_on_market = set()  # у кого есть лот на рынке — пропускаем
    market_offsets = {gid: "" for gid in gift_ids}
    last_upd     = 0.0

    async def collect_more_market_owners(n):
        """Собираем владельцев с рынка (у них есть лоты - их скипаем при профильном поиске)."""
        collected = 0
        active    = [g for g, o in market_offsets.items() if o is not None]
        if not active:
            return False

        fetch_gids  = active[:8]
        fetch_tasks = [fetch_market_page(gid, market_offsets[gid], limit=100) for gid in fetch_gids]
        results     = await asyncio.gather(*fetch_tasks)

        for gid, (items, nxt) in zip(fetch_gids, results):
            market_offsets[gid] = nxt if nxt else None
            for item in items:
                uid = item.get("owner_id")
                if not uid:
                    continue
                # Помечаем как "есть на рынке"
                owners_on_market.add(uid)
                # Добавляем в очередь для профильного просмотра только тех кого ещё не видели
                if uid not in seen_owners:
                    owner_obj = item["owner"]
                    if girls_only and not is_girl(owner_obj):
                        continue
                    seen_owners[uid] = (owner_obj, item["username"], item["name"])
                    # В профильном режиме нас интересуют те у кого лотов на рынке нет,
                    # но мы сначала соберём всех с рынка а потом найдём у кого их нет.
                    # Для этого используем другой подход — ищем в saved_gifts напрямую
                    collected += 1
        await asyncio.sleep(0.05)
        return collected > 0

    # Для профильного поиска "без лота на рынке" нам нужно:
    # 1. Получить список всех владельцев с рынка (чтобы знать кого ИСКЛЮЧИТЬ)
    # 2. Затем пробовать загружать saved_gifts случайных пользователей
    # Но т.к. у нас нет общего списка юзеров — идём иначе:
    # Берём владельцев с рынка, смотрим их saved_gifts, и если владелец
    # встречается в рынке ТОЛЬКО в одной коллекции (= возможно у него есть и другие) — берём.
    # На практике: ищем юзеров у которых суммарно на рынке 0 лотов
    # (они не появятся в market_offsets). Для этого делаем reverse-поиск через saved_gifts.

    # Упрощённый подход: берём владельцев с рынка и проверяем их профили.
    # Если в профиле больше NFT чем на рынке — показываем.
    # Если человек вообще не на рынке — тоже показываем (если у него есть NFT в профиле).

    owner_market_count = {}  # uid -> кол-во лотов на рынке

    async def build_market_index():
        """Строим индекс: у кого сколько лотов на рынке."""
        active = [g for g, o in market_offsets.items() if o is not None]
        while active and is_searching:
            fetch_gids  = active[:12]
            fetch_tasks = [fetch_market_page(gid, market_offsets[gid], limit=100) for gid in fetch_gids]
            results     = await asyncio.gather(*fetch_tasks)
            for gid, (items, nxt) in zip(fetch_gids, results):
                market_offsets[gid] = nxt if nxt else None
                for item in items:
                    uid = item.get("owner_id")
                    if uid:
                        owners_on_market.add(uid)
                        owner_market_count[uid] = owner_market_count.get(uid, 0) + 1
                        if uid not in seen_owners:
                            seen_owners[uid] = (item["owner"], item["username"], item["name"])
            active = [g for g, o in market_offsets.items() if o is not None]
            await asyncio.sleep(0.05)

    try:
        # Сначала строим индекс рынка
        await status_msg.edit_text(
            "<b>Сканирую рынок для поиска профилей без листингов...</b>",
            reply_markup=stop_kb()
        )
        await build_market_index()

        if not is_searching:
            return 0

        # Теперь проходим по всем найденным владельцам
        all_owners = list(seen_owners.items())
        total      = len(all_owners)
        checked    = 0

        await status_msg.edit_text(
            "<b>Найдено владельцев: " + str(total) + ". Проверяю профили...</b>",
            reply_markup=stop_kb()
        )

        for uid, (owner_obj, username, name) in all_owners:
            if not is_searching or found >= max_results:
                break

            checked += 1
            market_cnt = owner_market_count.get(uid, 0)

            if girls_only and not is_girl(owner_obj):
                continue

            # Загружаем saved_gifts
            nft_gifts = await get_profile_nfts(uid, username)
            profile_cnt = len(nft_gifts)

            if profile_cnt == 0:
                continue

            # Для профильного поиска "без лота на рынке":
            # Показываем только тех у кого profile_cnt > market_cnt
            # (есть NFT в профиле которые НЕ выставлены)
            hidden_nfts = profile_cnt - market_cnt
            if hidden_nfts <= 0:
                continue

            if not gifts_in_range(hidden_nfts, min_gifts, max_gifts):
                continue

            profile_url = ("https://t.me/" + username) if username else ("tg://user?id=" + str(uid))
            owner_str   = fmt_owner(owner_obj, username, name)

            cache_owner(uid, owner_obj, username, name, profile_url, nft_gifts)
            kb  = owner_card_kb(username, profile_url, uid)
            txt = (
                "<b>Владелец: " + owner_str + "\n"
                "NFT в профиле: " + str(profile_cnt) + ", На рынке: " + str(market_cnt) + ", Скрыто: " + str(hidden_nfts) + "</b>"
            )
            try:
                await status_msg.bot.send_message(
                    chat_id=status_msg.chat.id,
                    text=txt,
                    parse_mode="HTML",
                    reply_markup=kb,
                )
                found += hidden_nfts
                stats["found"] += hidden_nfts
            except Exception as e:
                logger.warning("profile block: %s", e)

            await asyncio.sleep(0.1)

            now = asyncio.get_event_loop().time()
            if now - last_upd > 2:
                try:
                    lbl = "девушек" if girls_only else "профилей"
                    txt = (
                        "<b>Проверено: " + str(checked) + "/" + str(total) + "\n"
                        "Найдено " + lbl + ": " + str(found) + "</b>"
                    )
                    await status_msg.edit_text(txt, parse_mode="HTML", reply_markup=stop_kb())
                    last_upd = now
                except Exception:
                    pass

    except Exception as e:
        logger.error("do_profile_search: %s", e)
    finally:
        is_searching = False
    return found


async def ensure_collections():
    if not ALL_GIFT_IDS:
        await load_collections()
    return [gid for gid, _ in ALL_GIFT_IDS]

async def run_market(cb, cat=None, girls=False, ids=None):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идет!", show_alert=True)
        return
    await cb.answer("Запускаю...")
    stats["checks"] += 1
    uid = cb.from_user.id
    if ids is None:
        ids = await ensure_collections()
    if not ids:
        await cb.message.answer("<b>Коллекции не загружены.</b>", parse_mode="HTML", reply_markup=menu_kb())
        return
    boost  = get_boost(uid)
    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    if girls:
        label = "Девушки — рынок"
    elif cat:
        label = PRICE_CATEGORIES[cat]["label"]
    else:
        label = "Все NFT"
    txt = (
        "<b>" + label + "\n"
        "Гифтов: от " + str(mn) + " до " + mx_str + ", Буст: " + str(boost) + "%\n\n"
        "Найдено: 0</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_market_search(status, ids, cat=cat, girls_only=girls, boost=boost, min_gifts=mn, max_gifts=mx)
    try:
        await status.edit_text(
            "<b>Готово! " + label + "\nНайдено: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass

async def run_profile(cb, girls=False, ids=None):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идет!", show_alert=True)
        return
    await cb.answer("Запускаю...")
    stats["checks"] += 1
    uid = cb.from_user.id
    if ids is None:
        ids = await ensure_collections()
    if not ids:
        await cb.message.answer("<b>Коллекции не загружены.</b>", parse_mode="HTML", reply_markup=menu_kb())
        return
    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    label  = "Девушки — профили" if girls else "Профили без листингов"
    txt = (
        "<b>" + label + "\n"
        "Гифтов скрытых: от " + str(mn) + " до " + mx_str + "\n\n"
        "Сканирую рынок...</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_profile_search(status, ids, girls_only=girls, min_gifts=mn, max_gifts=mx)
    try:
        await status.edit_text(
            "<b>Готово! " + label + "\nНайдено скрытых NFT: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


WELCOME = "<b>Neptun Parser\n\nВыбери действие:</b>"

# ===================== ОНБОРДИНГ (первый запуск) =====================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    is_new = add_user(message.from_user.id, message.from_user.username,
                      message.from_user.first_name, message.from_user.last_name)

    if not await check_authorized() and is_admin(message.from_user.id):
        await message.answer(
            "<b>Нужна авторизация Telegram</b>\nВведи номер: <code>+79001234567</code>",
            parse_mode="HTML"
        )
        await state.set_state(Auth.phone)
        return

    if is_new and message.from_user.id not in ONBOARDING_DONE:
        ONBOARDING_DONE.add(message.from_user.id)
        await message.answer(
            "<b>Привет! Добро пожаловать в Neptun Parser\n\n"
            "Давай настроим поиск.\n\n"
            "Шаг 1/3. Минимум гифтов у владельца:\n"
            "Напиши число (например 2):</b>",
            parse_mode="HTML",
            reply_markup=onboarding_min_kb()
        )
        await state.set_state(Onboarding.min_gifts)
        return

    await message.answer(WELCOME, parse_mode="HTML", reply_markup=main_kb())
    await message.answer(
        "<b>Что хочешь сделать?</b>",
        parse_mode="HTML",
        reply_markup=search_mode_select_kb()
    )

@dp.message(F.text == "Старт")
async def cmd_start_btn(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    bst = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    await message.answer(
        "<b>Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Буст: " + str(bst) + "%\n\n"
        "Выбери действие:</b>",
        parse_mode="HTML",
        reply_markup=search_mode_select_kb()
    )

# Онбординг шаг 1 — мин гифтов (кнопка)
@dp.callback_query(F.data.startswith("ob_min_"))
async def ob_min(cb: CallbackQuery, state: FSMContext):
    val = int(cb.data.split("_")[2])
    USER_MIN_GIFTS[cb.from_user.id] = val
    await cb.answer("Мин. гифтов: " + str(val))
    await cb.message.edit_text(
        "<b>Минимум гифтов: " + str(val) + "\n\n"
        "Шаг 2/3. Максимум гифтов:\n"
        "Напиши число или нажми кнопку (0 = без лимита):</b>",
        parse_mode="HTML",
        reply_markup=onboarding_max_kb()
    )
    await state.set_state(Onboarding.max_gifts)

# Онбординг шаг 1 — мин гифтов (текст)
@dp.message(Onboarding.min_gifts)
async def ob_min_text(message: Message, state: FSMContext):
    try:
        val = int(message.text.strip())
        if val < 1:
            raise ValueError
    except ValueError:
        await message.answer("<b>Введи число от 1 и выше:</b>", parse_mode="HTML")
        return
    USER_MIN_GIFTS[message.from_user.id] = val
    await message.answer(
        "<b>Минимум гифтов: " + str(val) + "\n\n"
        "Шаг 2/3. Максимум гифтов:\n"
        "Напиши число или нажми кнопку (0 = без лимита):</b>",
        parse_mode="HTML",
        reply_markup=onboarding_max_kb()
    )
    await state.set_state(Onboarding.max_gifts)

# Онбординг шаг 2 — макс гифтов (кнопка)
@dp.callback_query(F.data.startswith("ob_max_"))
async def ob_max(cb: CallbackQuery, state: FSMContext):
    val = int(cb.data.split("_")[2])
    USER_MAX_GIFTS[cb.from_user.id] = val
    label = "без лимита" if val == 0 else str(val)
    await cb.answer("Макс. гифтов: " + label)
    await cb.message.edit_text(
        "<b>Максимум гифтов: " + label + "\n\n"
        "Шаг 3/3. Буст цен (для поиска по рынку):\n"
        "Насколько выше флора ищем NFT?\n\n"
        "100% = цена до x2 от флора\n"
        "200% = цена до x3 от флора</b>",
        parse_mode="HTML",
        reply_markup=onboarding_boost_kb()
    )
    await state.set_state(Onboarding.boost)

# Онбординг шаг 2 — макс гифтов (текст)
@dp.message(Onboarding.max_gifts)
async def ob_max_text(message: Message, state: FSMContext):
    try:
        val = int(message.text.strip())
        if val < 0:
            raise ValueError
    except ValueError:
        await message.answer("<b>Введи число (0 = без лимита):</b>", parse_mode="HTML")
        return
    USER_MAX_GIFTS[message.from_user.id] = val
    label = "без лимита" if val == 0 else str(val)
    await message.answer(
        "<b>Максимум гифтов: " + label + "\n\n"
        "Шаг 3/3. Буст цен (для поиска по рынку):\n"
        "Насколько выше флора ищем NFT?\n\n"
        "100% = цена до x2 от флора\n"
        "200% = цена до x3 от флора</b>",
        parse_mode="HTML",
        reply_markup=onboarding_boost_kb()
    )
    await state.set_state(Onboarding.boost)

# Онбординг шаг 3 — буст
@dp.callback_query(F.data.startswith("ob_boost_"))
async def ob_boost(cb: CallbackQuery, state: FSMContext):
    val = int(cb.data.split("_")[2])
    USER_BOOST[cb.from_user.id] = val
    await cb.answer("Буст: " + str(val) + "%")
    uid = cb.from_user.id
    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    await state.clear()
    await cb.message.edit_text(
        "<b>Настройка завершена!\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Буст: " + str(val) + "%\n\n"
        "Всё можно изменить в Настройки</b>",
        parse_mode="HTML",
        reply_markup=None
    )
    await cb.message.answer(WELCOME, parse_mode="HTML", reply_markup=main_kb())
    await cb.message.answer(
        "<b>Что хочешь сделать?</b>",
        parse_mode="HTML",
        reply_markup=search_mode_select_kb()
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("<b>Отменено.</b>", parse_mode="HTML", reply_markup=main_kb())

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer("<b>Твой ID:</b> <code>" + str(message.from_user.id) + "</code>", parse_mode="HTML")

@dp.message(Command("clear"))
async def cmd_clear(message: Message):
    global is_searching
    is_searching = False
    await message.answer("<b>Поиск остановлен.</b>", parse_mode="HTML", reply_markup=menu_kb())

@dp.message(Command("neptunteam"))
async def cmd_neptunteam(message: Message):
    uid    = message.from_user.id
    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    bst    = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    txt = (
        "<b>Neptun Parser. Справка по настройкам\n\n"
        "Мин. гифтов — минимальное кол-во NFT у владельца\n"
        "У кого меньше — не попадёт в результаты\n"
        "Пример: мин=2, пропускаем всех у кого 1 NFT\n\n"
        "Макс. гифтов — максимальное кол-во NFT у владельца\n"
        "0 = без лимита\n"
        "Пример: макс=10, не показываем тех у кого 11 и более\n\n"
        "Буст цен — насколько выше флора ищем (только рынок)\n"
        "Флор = минимальная цена в коллекции (нижние 25%)\n"
        "Формула: цена от 0.7 флора до (1 + буст/100) флора\n\n"
        "Примеры буста:\n"
        "30% = цена до 1.3 флора\n"
        "100% = цена до 2.0 флора\n"
        "200% = цена до 3.0 флора\n\n"
        "Режимы поиска:\n"
        "По рынку — ищет NFT выставленные на продажу\n"
        "По профилю — ищет владельцев у которых NFT в профиле, но не на рынке\n\n"
        "Текущие настройки:\n"
        "Мин: " + str(mn) + ", Макс: " + mx_str + ", Буст: " + str(bst) + "%\n\n"
        "Меняй в Настройки или через /start</b>"
    )
    await message.answer(txt, parse_mode="HTML", reply_markup=main_kb())

@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("<b>Нет доступа. ID:</b> <code>" + str(message.from_user.id) + "</code>", parse_mode="HTML")
        return
    await state.clear()
    users = load_users()
    ok    = await check_authorized()
    txt = (
        "<b>Админ панель</b>\n\n"
        "<b>Telethon: " + ("✅ Авторизован" if ok else "❌ Не авторизован") + "</b>\n"
        "<b>Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>\n"
        "<b>Пользователей: " + str(len(users)) + "</b>\n"
        "<b>Поисков: " + str(stats["checks"]) + " | Найдено: " + str(stats["found"]) + "</b>"
    )
    await message.answer(txt, parse_mode="HTML", reply_markup=admin_kb())


# ===================== CALLBACKS =====================

@dp.callback_query(F.data.startswith("shownft_"))
async def cb_show_nft(cb: CallbackQuery):
    uid    = int(cb.data[8:])
    cached = NFT_CACHE.get(uid)
    if not cached:
        await cb.answer("Загружаю NFT...", show_alert=False)
        nfts = await get_profile_nfts(uid, None)
        if not nfts:
            await cb.answer("NFT не найдены или профиль закрыт", show_alert=True)
            return
        NFT_CACHE[uid] = {
            "owner": None, "username": None, "name": None,
            "profile_url": "tg://user?id=" + str(uid), "items": nfts,
        }
        cached = NFT_CACHE[uid]
    else:
        await cb.answer()
    items       = cached.get("items", [])
    username    = cached.get("username")
    profile_url = cached.get("profile_url")
    owner_str   = fmt_owner(cached.get("owner"), username, cached.get("name"))
    if not items:
        await cb.answer("Список пуст", show_alert=True)
        return
    kb  = nft_list_kb(items, username, profile_url)
    txt = "<b>NFT — " + owner_str + "\nВсего: " + str(len(items)) + "</b>"
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "search_mode_select")
async def cb_search_mode_select(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Выберите режим поиска:\n\n"
        "По рынку — NFT выставленные на продажу\n"
        "По профилю — владельцы у которых NFT скрыты (не на рынке)</b>",
        parse_mode="HTML",
        reply_markup=search_mode_select_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Поиск по рынку. Выбери категорию:</b>",
        parse_mode="HTML",
        reply_markup=market_cat_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "mode_profile")
async def cb_mode_profile(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Поиск по профилю. Выбери категорию:</b>",
        parse_mode="HTML",
        reply_markup=profile_cat_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "settings_menu")
async def cb_settings(cb: CallbackQuery):
    uid    = cb.from_user.id
    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    bst    = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    txt = (
        "<b>Настройки поиска\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Буст цен: " + str(bst) + "%</b>"
    )
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=settings_menu_kb(uid))
    await cb.answer()

@dp.callback_query(F.data == "set_min_gifts")
async def cb_set_min(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "<b>Минимум гифтов у владельца:\n"
        "Напиши число (например 2):</b>",
        parse_mode="HTML",
        reply_markup=gifts_input_cancel_kb()
    )
    await state.set_state(SetGifts.min_gifts)
    await cb.answer()

@dp.message(SetGifts.min_gifts)
async def set_min_text(message: Message, state: FSMContext):
    try:
        val = int(message.text.strip())
        if val < 1:
            raise ValueError
    except ValueError:
        await message.answer("<b>Введи число от 1 и выше:</b>", parse_mode="HTML")
        return
    USER_MIN_GIFTS[message.from_user.id] = val
    await state.clear()
    await message.answer(
        "<b>Минимум гифтов установлен: " + str(val) + "</b>",
        parse_mode="HTML",
        reply_markup=settings_menu_kb(message.from_user.id)
    )

@dp.callback_query(F.data == "set_max_gifts")
async def cb_set_max(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "<b>Максимум гифтов у владельца:\n"
        "Напиши число (0 = без лимита):</b>",
        parse_mode="HTML",
        reply_markup=gifts_input_cancel_kb()
    )
    await state.set_state(SetGifts.max_gifts)
    await cb.answer()

@dp.message(SetGifts.max_gifts)
async def set_max_text(message: Message, state: FSMContext):
    try:
        val = int(message.text.strip())
        if val < 0:
            raise ValueError
    except ValueError:
        await message.answer("<b>Введи число (0 = без лимита):</b>", parse_mode="HTML")
        return
    USER_MAX_GIFTS[message.from_user.id] = val
    label = "без лимита" if val == 0 else str(val)
    await state.clear()
    await message.answer(
        "<b>Максимум гифтов установлен: " + label + "</b>",
        parse_mode="HTML",
        reply_markup=settings_menu_kb(message.from_user.id)
    )

@dp.callback_query(F.data == "set_boost")
async def cb_set_boost(cb: CallbackQuery):
    txt = "<b>Буст цен</b>\n\n<b>100% = до x2 флора, 150% = до x2.5, 200% = до x3</b>"
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=boost_picker_kb())
    await cb.answer()

@dp.callback_query(F.data.startswith("boost_"))
async def cb_boost(cb: CallbackQuery):
    val = int(cb.data.split("_")[1])
    USER_BOOST[cb.from_user.id] = val
    await cb.answer("Буст: " + str(val) + "%", show_alert=True)
    await cb.message.edit_reply_markup(reply_markup=None)

@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer(WELCOME, parse_mode="HTML", reply_markup=main_kb())
    await cb.answer()

# Рынок — выбор категории -> показ "кого искать"
@dp.callback_query(F.data.startswith("cat_"))
async def cb_cat_select(cb: CallbackQuery):
    cat = cb.data[4:]  # all, cheap, mid, hard, ultra, extreme
    cat_labels = {
        "all": "Все NFT", "cheap": "Дешевые (до 2К)", "mid": "Средние (2-5К)",
        "hard": "Сложные (5-20К)", "ultra": "Хард (20-100К)", "extreme": "Экстрим (100К+)",
    }
    label = cat_labels.get(cat, cat)
    await cb.message.answer(
        "<b>" + label + "\nКого искать?</b>",
        parse_mode="HTML",
        reply_markup=who_search_kb(cat, "mkt")
    )
    await cb.answer()

# Профиль — выбор категории -> показ "кого искать"
@dp.callback_query(F.data.startswith("pcat_"))
async def cb_pcat_select(cb: CallbackQuery):
    cat = cb.data[5:]  # all, cheap, mid, hard, ultra, extreme
    cat_labels = {
        "all": "Все профили", "cheap": "Дешевые (до 2К)", "mid": "Средние (2-5К)",
        "hard": "Сложные (5-20К)", "ultra": "Хард (20-100К)", "extreme": "Экстрим (100К+)",
    }
    label = cat_labels.get(cat, cat)
    await cb.message.answer(
        "<b>" + label + "\nКого искать?</b>",
        parse_mode="HTML",
        reply_markup=who_search_kb(cat, "prf")
    )
    await cb.answer()

# Рынок — запуск после выбора "кого" (mkt_<cat>_all / mkt_<cat>_girls)
@dp.callback_query(F.data.startswith("mkt_"))
async def cb_mkt_who(cb: CallbackQuery):
    parts = cb.data.split("_")  # ["mkt", cat, who]
    if len(parts) < 3:
        await cb.answer()
        return
    cat   = parts[1]
    who   = parts[2]
    real_cat = None if cat == "all" else cat
    await run_market(cb, cat=real_cat, girls=(who == "girls"))

# Профиль — запуск после выбора "кого" (prf_<cat>_all / prf_<cat>_girls)
@dp.callback_query(F.data.startswith("prf_"))
async def cb_prf_who(cb: CallbackQuery):
    parts = cb.data.split("_")  # ["prf", cat, who]
    if len(parts) < 3:
        await cb.answer()
        return
    who   = parts[2]
    await run_profile(cb, girls=(who == "girls"))

# По коллекции
@dp.callback_query(F.data == "mode_col")
async def cb_mode_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("<b>Коллекции не загружены</b>", parse_mode="HTML", reply_markup=menu_kb())
        await cb.answer()
        return
    await cb.message.answer("<b>По коллекции — выбери источник:</b>", parse_mode="HTML", reply_markup=col_source_kb())
    await cb.answer()

@dp.callback_query(F.data == "col_market")
async def cb_col_market(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    await cb.message.answer("<b>Выбери коллекцию (рынок):</b>", parse_mode="HTML",
                            reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "mktcol_", "mode_col"))
    await cb.answer()

@dp.callback_query(F.data == "col_profile")
async def cb_col_profile(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    await cb.message.answer("<b>Выбери коллекцию (профили):</b>", parse_mode="HTML",
                            reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "prfcol_", "mode_col"))
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

@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    is_searching = False
    await cb.answer("Останавливаю...")
    try:
        await cb.message.edit_text("<b>⏹ Поиск остановлен.</b>", parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        pass

@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    uid    = cb.from_user.id
    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    bst    = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "∞"
    txt = (
        "<b>📊 Статистика</b>\n\n"
        "<b>Поисков: " + str(stats["checks"]) + "</b>\n"
        "<b>Найдено: " + str(stats["found"]) + "</b>\n"
        "<b>Пользователей: " + str(get_user_count()) + "</b>\n\n"
        "<b>Настройки:</b>\n"
        "<b>Мин: " + str(mn) + " | Макс: " + mx_str + " | Буст: " + str(bst) + "%</b>"
    )
    await cb.message.answer(txt, parse_mode="HTML")
    await cb.answer()

# ADMIN
@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await show_users_page(cb.message, 0, False)
    await cb.answer()

@dp.callback_query(F.data.startswith("users_page_"))
async def cb_users_page(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    page = int(cb.data[len("users_page_"):])
    await show_users_page(cb.message, page, True)
    await cb.answer()

async def show_users_page(message, page, edit):
    users     = load_users()
    all_items = list(users.items())
    total     = len(all_items)
    PAGE      = 20
    if total == 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_panel")]])
        fn = message.edit_text if edit else message.answer
        await fn("<b>Пользователей нет.</b>", parse_mode="HTML", reply_markup=kb)
        return
    start = page * PAGE
    end   = min(start + PAGE, total)
    chunk = all_items[start:end]
    lines = ["<b>Пользователи " + str(start+1) + "-" + str(end) + " из " + str(total) + "</b>\n"]
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
        card = "<b>" + str(i) + ". <code>" + str(uid) + "</code>"
        if uname:
            card += " @" + uname
        if name:
            card += " | " + name
        card += "\n    " + fmt_timestamp(joined) + "</b>"
        lines.append(card)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data="users_page_" + str(page-1)))
    if end < total:
        nav.append(InlineKeyboardButton(text="Вперед ▶️", callback_data="users_page_" + str(page+1)))
    rows = [nav] if nav else []
    rows.append([InlineKeyboardButton(text="Админ", callback_data="admin_panel")])
    fn = message.edit_text if edit else message.answer
    await fn("\n".join(lines), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    users = load_users()
    ok    = await check_authorized()
    txt = (
        "<b>Админ панель</b>\n\n"
        "<b>Telethon: " + ("✅ Авторизован" if ok else "❌ Не авторизован") + "</b>\n"
        "<b>Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>\n"
        "<b>Пользователей: " + str(len(users)) + "</b>"
    )
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await cb.message.answer("<b>Отправь сообщение для рассылки. /cancel — отмена</b>", parse_mode="HTML", reply_markup=cancel_kb())
    await cb.answer()

@dp.message(Broadcast.message)
async def broadcast_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(mid=message.message_id, cid=message.chat.id)
    await state.set_state(None)
    await message.answer("<b>Подтверди отправку:</b>", parse_mode="HTML", reply_markup=confirm_kb())

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def cb_broadcast_send(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    data     = await state.get_data()
    mid, cid = data.get("mid"), data.get("cid")
    if not mid:
        await cb.answer("Нет сообщения", show_alert=True)
        return
    users  = load_users()
    uids   = list(users.keys())
    status = await cb.message.answer("<b>📢 Рассылка " + str(len(uids)) + " пользователям...</b>", parse_mode="HTML")
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
                await status.edit_text("<b>" + str(i+1) + "/" + str(len(uids)) + "...</b>", parse_mode="HTML")
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await status.edit_text(
        "<b>✅ Отправлено: " + str(ok) + "</b>\n<b>❌ Ошибок: " + str(fail) + "</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    u = load_users()
    txt = (
        "<b>📊 Статистика</b>\n\n"
        "<b>Пользователей: " + str(len(u)) + "</b>\n"
        "<b>Поисков: " + str(stats["checks"]) + "</b>\n"
        "<b>Найдено: " + str(stats["found"]) + "</b>\n"
        "<b>Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>"
    )
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_auth")
async def cb_admin_auth(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.answer("<b>Введи номер:</b> <code>+79001234567</code>", parse_mode="HTML")
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
    await cb.message.answer("<b>Вышел из TG.</b>", parse_mode="HTML", reply_markup=admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.clear()
    await cb.message.answer("<b>Отменено</b>", parse_mode="HTML", reply_markup=admin_kb())
    await cb.answer()


# AUTH
@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("<b>Формат:</b> <code>+79001234567</code>", parse_mode="HTML")
        return
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
            await asyncio.sleep(1)
        res = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=res.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("<b>Код отправлен. Введи:</b> <code>1 2 3 4 5</code>", parse_mode="HTML")
    except Exception as e:
        await message.answer("<b>Ошибка:</b> <code>" + str(e) + "</code>", parse_mode="HTML")
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
            "<b>✅ Авторизован как @" + str(me.username or me.first_name) + "!</b>\n<b>Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("<b>Введи пароль 2FA:</b>", parse_mode="HTML")
    except Exception as e:
        await message.answer("<b>Ошибка:</b> <code>" + str(e) + "</code>", parse_mode="HTML")

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
            "<b>✅ Авторизован как @" + str(me.username or me.first_name) + "!</b>\n<b>Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except Exception as e:
        await message.answer("<b>Неверный пароль:</b> <code>" + str(e) + "</code>", parse_mode="HTML")


async def main():
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("Neptun Parser запущен!")
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info("Авторизован, коллекций: %d", len(ALL_GIFT_IDS))
        else:
            logger.warning("Не авторизован — пройди /start")
    except Exception as e:
        logger.error("Ошибка старта: %s", e)
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
