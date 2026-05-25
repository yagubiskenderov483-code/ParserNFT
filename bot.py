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

API_ID       = 36101343
API_HASH     = "116195fa5e0459d25a9a6266b40807d7"
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
USER_SEEN_OWNERS = {}  # uid -> set of owner_ids которых уже показали
DEFAULT_BOOST     = 100
DEFAULT_MIN_GIFTS = 2
DEFAULT_MAX_GIFTS = 0
DEFAULT_REGION    = "any"
DEFAULT_LIMIT     = 50

USER_REGION = {}
USER_LIMIT  = {}

REGIONS = {
    "any":   {"label": "🌍 Все",       "langs": []},
    "ru":    {"label": "🇷🇺 Россия",   "langs": ["ru"], "names": ["а","о","е","и","й","я","ь","ъ","ы","ю","э","ё","ж","ш","щ","ч","ц","з","х","г","ф","п","д","б","в","л","м","н","р","с","т","к"]},
    "ua":    {"label": "🇺🇦 Украина",  "langs": ["uk"], "names": ["а","о","е","і","й","я","ь","и","є","ї","ю"]},
    "us":    {"label": "🇺🇸 США",      "langs": ["en"], "names": ["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"]},
    "uk":    {"label": "🇬🇧 Великобритания", "langs": ["en"]},
    "de":    {"label": "🇩🇪 Германия", "langs": ["de"], "kw": ["de","deutsch","german","berlin","münchen","frankfurt","hamburg"]},
    "fr":    {"label": "🇫🇷 Франция",  "langs": ["fr"], "kw": ["fr","french","paris","france","française"]},
    "es":    {"label": "🇪🇸 Испания",  "langs": ["es"], "kw": ["es","spain","español","madrid","barcelona"]},
    "tr":    {"label": "🇹🇷 Турция",   "langs": ["tr"], "kw": ["tr","turkey","türk","istanbul","ankara"]},
    "cn":    {"label": "🇨🇳 Китай",    "langs": ["zh"], "kw": ["cn","china","chinese","beijing","shanghai"]},
    "jp":    {"label": "🇯🇵 Япония",   "langs": ["ja"], "kw": ["jp","japan","japanese","tokyo","osaka"]},
    "in":    {"label": "🇮🇳 Индия",    "langs": ["hi","en"], "kw": ["india","indian","delhi","mumbai","hindi"]},
    "br":    {"label": "🇧🇷 Бразилия", "langs": ["pt"], "kw": ["br","brazil","brasil","português","rio","são paulo"]},
    "ca":    {"label": "🇨🇦 Канада",   "langs": ["en","fr"], "kw": ["canada","canadian","toronto","montreal","vancouver"]},
    "au":    {"label": "🇦🇺 Австралия","langs": ["en"], "kw": ["australia","australian","sydney","melbourne"]},
    "kz":    {"label": "🇰🇿 Казахстан","langs": ["kk","ru"], "kw": ["kz","казах","астана","алмат","казахстан"]},
    "by":    {"label": "🇧🇾 Беларусь", "langs": ["be","ru"], "kw": ["by","беларусь","минск","белор"]},
    "uz":    {"label": "🇺🇿 Узбекистан","langs": ["uz","ru"],"kw": ["uz","ташкент","узбек","самарканд"]},
    "ae":    {"label": "🇦🇪 ОАЭ",     "langs": ["ar","en"], "kw": ["uae","dubai","abu dhabi","эмират"]},
}

RU_LETTERS = set("абвгдеёжзийклмнопрстуфхцчшщъыьэюяіїєґ")
EN_LETTERS = set("abcdefghijklmnopqrstuvwxyz")

def detect_script(text):
    """Определяет преобладающий скрипт в тексте."""
    if not text:
        return "any"
    text = text.lower()
    ru = sum(1 for c in text if c in RU_LETTERS)
    en = sum(1 for c in text if c in EN_LETTERS)
    if ru > en and ru > 2:
        return "cyrillic"
    if en > ru and en > 2:
        return "latin"
    return "any"

def region_match(owner, username, name, region_key):
    """Проверяет соответствие владельца выбранному региону."""
    if region_key == "any" or not region_key:
        return True
    reg = REGIONS.get(region_key, {})
    kws = reg.get("kw", [])
    # Собираем весь текст
    bio   = ""
    uname = (username or "").lower()
    fname = ""
    lname = ""
    if owner:
        bio   = (getattr(owner, "bio", "") or "").lower()
        uname = uname or (getattr(owner, "username", "") or "").lower()
        fname = (getattr(owner, "first_name", "") or "").lower()
        lname = (getattr(owner, "last_name", "") or "").lower()
    if name and not fname:
        parts = name.lower().strip().split()
        fname = parts[0] if parts else ""
        lname = parts[1] if len(parts) > 1 else ""
    name_text    = (fname + " " + lname).strip()
    full         = (bio + " " + name_text + " " + uname).strip()
    name_script  = detect_script(name_text)
    uname_script = detect_script(uname)
    bio_script   = detect_script(bio)
    has_cyrillic = (name_script == "cyrillic" or uname_script == "cyrillic" or
                    any(c in RU_LETTERS for c in bio[:50]))

    # Для CN — иероглифы обязательны, кириллица = стоп
    if region_key == "cn":
        if has_cyrillic:
            return False
        return any('\u4e00' <= c <= '\u9fff' for c in full)

    # Для JP — японские символы обязательны, кириллица = стоп
    if region_key == "jp":
        if has_cyrillic:
            return False
        return any('\u3040' <= c <= '\u30ff' for c in full)

    # Для AE — арабский или ключевые слова, кириллица = стоп
    if region_key == "ae":
        if has_cyrillic:
            return False
        if any('\u0600' <= c <= '\u06ff' for c in full):
            return True
        for kw in kws:
            if kw in full:
                return True
        return False

    # Для RU/UA — кириллица в имени ИЛИ нике = матч
    if region_key in ("ru", "ua"):
        if name_script == "cyrillic" or uname_script == "cyrillic":
            return True
        for kw in kws:
            if kw in full:
                return True
        return False

    # Для BY/KZ/UZ — кириллица или ключевые слова
    if region_key in ("by", "kz", "uz"):
        if has_cyrillic:
            return True
        for kw in kws:
            if kw in full:
                return True
        return False

    # Для EN-стран (US/UK/CA/AU) — кириллица = СТОП, нужна латиница
    if region_key in ("us", "uk", "ca", "au"):
        if has_cyrillic:
            return False
        if name_script == "latin" or uname_script == "latin":
            return True
        for kw in kws:
            if kw in full:
                return True
        return False

    # Для DE/FR/ES/TR/BR/IN — кириллица = стоп, проверяем ключевые слова
    if region_key in ("de", "fr", "es", "tr", "br", "in"):
        if has_cyrillic:
            return False
        for kw in kws:
            if kw in full:
                return True
        if name_script == "latin" or uname_script == "latin":
            return True
        return False

    # Fallback
    for kw in kws:
        if kw in full:
            return True
    return False

# Флаги онбординга (первый запуск) — хранится в памяти, заполняется из users.json
ONBOARDING_DONE = set()
ONBOARDING_FILE = "onboarding_done.json"

def load_onboarding():
    if os.path.exists(ONBOARDING_FILE):
        with open(ONBOARDING_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_onboarding(s):
    with open(ONBOARDING_FILE, "w") as f:
        json.dump(list(s), f)

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
    boost     = State()

class SearchLimit(StatesGroup):
    limit = State()

class SetLimit(StatesGroup):
    limit = State()

def get_region(uid): return USER_REGION.get(uid, DEFAULT_REGION)
def get_limit(uid):  return USER_LIMIT.get(uid, DEFAULT_LIMIT)


def is_admin(uid): return int(uid) == int(ADMIN_ID)

async def check_authorized():
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
        return await tg_client.is_user_authorized()
    except Exception:
        return False

def is_girl(owner, username_str=None, name_str=None):
    if not owner and not username_str and not name_str:
        return False
    if owner:
        first = (getattr(owner, "first_name", "") or "").lower().strip()
        last  = (getattr(owner, "last_name",  "") or "").lower().strip()
        uname = (getattr(owner, "username",   "") or "").lower().strip()
    else:
        first = ""
        last  = ""
        uname = (username_str or "").lower().strip()
    # Если есть name_str — парсим из него first/last
    if name_str and not first and not last:
        parts = name_str.lower().strip().split()
        first = parts[0] if parts else ""
        last  = parts[1] if len(parts) > 1 else ""
    full = first + " " + last + " " + uname
    # Проверяем мужские ключевые слова как отдельные слова
    for kw in BOY_KW:
        if re.search(r'\b' + re.escape(kw) + r'\b', full):
            return False
    # Проверяем женские имена
    for name in GIRL_NAMES:
        if first.startswith(name) or last.startswith(name):
            return True
        if len(name) >= 4 and name in uname:
            return True
    # Проверяем женские ключевые слова
    for kw in GIRL_KW:
        if kw in full:
            return True
    return False

MODEL_KW = [
    # English
    "model","onlyfans","of","only fans","blogger","content creator",
    "influencer","photo","photoshoot","nsfw","18+","exclusive","subscribe",
    "fans","adult","vip","premium","creator","OF","link in bio","linktree",
    "babe","hottie","sexy","goddess","naughty","spicy","thirst",
    # Russian
    "модель","блогер","блогерша","инфлюенсер","фото","фотосессия",
    "контент","эксклюзив","подпишись","подписка","взрослый","для взрослых",
    "горячая","пикантный","откровенный","фотомодель","видео","18+",
    # Username patterns — часть слова достаточно
    "mdl","xo","xx","69","hot","babe","vip","fan",
]

# Имена которые чаще всего встречаются у моделей
MODEL_NAMES = {
    "mia","luna","lana","lola","kira","nina","tina","diana","alisa","bella",
    "stella","victoria","angelina","kristina","valeria","natasha","vera",
    "катя","настя","лена","маша","вика","даша","юля","лиза","милана","арина",
}

def is_model(owner, username=None, name=None):
    bio   = ""
    uname = ""
    first = ""
    last  = ""
    if owner:
        bio   = (getattr(owner, "bio",        "") or "").lower()
        uname = (getattr(owner, "username",   "") or "").lower()
        first = (getattr(owner, "first_name", "") or "").lower()
        last  = (getattr(owner, "last_name",  "") or "").lower()
    if username:
        uname = uname or username.lower()
    if name:
        parts = name.lower().strip().split()
        first = first or (parts[0] if parts else "")
        last  = last  or (parts[1] if len(parts) > 1 else "")
    full = bio + " " + uname + " " + first + " " + last
    # Проверяем ключевые слова
    for kw in MODEL_KW:
        if kw in full:
            return True
    # Проверяем имена моделей
    for mn in MODEL_NAMES:
        if first.startswith(mn) or last.startswith(mn):
            return True
        if len(mn) >= 4 and mn in uname:
            return True
    # Если нет bio — смягчаем критерии: девушка + любой контентный намёк
    if not bio:
        if is_girl(owner, username, name):
            return True  # без bio все девушки потенциальные модели в этом режиме
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
        ALL_GIFT_IDS    = []
        NFT_COLLECTIONS = {}
        # GetStarGiftsRequest возвращает все коллекции за один вызов
        result = await tg_client(GetStarGiftsRequest(hash=0))
        gifts  = getattr(result, "gifts", None) or []
        seen   = set()
        for gift in gifts:
            gid   = getattr(gift, "id",    None)
            title = getattr(gift, "title", None)
            if gid is None or gid in seen:
                continue
            seen.add(gid)
            label = title if title else ("Gift #" + str(gid))
            ALL_GIFT_IDS.append((gid, label))
            if title:
                NFT_COLLECTIONS[title] = gid
        logger.info("Коллекций загружено: %d", len(ALL_GIFT_IDS))
    except Exception as e:
        logger.error("load_collections: %s", e)


async def fetch_market_page(gift_id, offset, limit=100):
    for attempt in range(3):
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
            wait = e.seconds + 2
            logger.warning("FloodWait %ds на gid=%s, жду...", wait, gift_id)
            await asyncio.sleep(wait)
        except Exception as e:
            logger.error("fetch_market gid=%s: %s", gift_id, e)
            return [], ""
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
    # Берём только гифты из профиля (НЕ с маркета)
    gifts = await fetch_saved_gifts_all(user_id)
    return [g for g in gifts if g.get("nft_url")]


# ===================== KEYBOARDS =====================

def main_kb():
    return None  # Меню через /start BotCommand

# Главное меню
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Поиск",      callback_data="search_mode_select")],
        [InlineKeyboardButton(text="Настройки",  callback_data="settings_menu"),
         InlineKeyboardButton(text="Статистика", callback_data="stats")],
    ])

# Шаг 1: выбор режима поиска
def search_mode_select_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Поиск по профилю", callback_data="mode_profile")],
        [InlineKeyboardButton(text="Поиск по маркету", callback_data="mode_market")],
        [InlineKeyboardButton(text="Поиск по модели",  callback_data="mode_model")],
        [InlineKeyboardButton(text="Назад",             callback_data="menu")],
    ])

# Шаг 2а: категории рынка
def market_cat_kb():
    rows = [
        [InlineKeyboardButton(text="Дешевые (до 2К)",  callback_data="cat_cheap")],
        [InlineKeyboardButton(text="Средние (2-5К)",   callback_data="cat_mid")],
        [InlineKeyboardButton(text="Сложные (5-20К)",  callback_data="cat_hard")],
        [InlineKeyboardButton(text="Хард (20-100К)",   callback_data="cat_ultra")],
        [InlineKeyboardButton(text="Назад",             callback_data="search_mode_select")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# Шаг 2б: категории профиля
def profile_cat_kb():
    rows = [
        [InlineKeyboardButton(text="Дешевые (до 2К)",  callback_data="pcat_cheap")],
        [InlineKeyboardButton(text="Средние (2-5К)",   callback_data="pcat_mid")],
        [InlineKeyboardButton(text="Сложные (5-20К)",  callback_data="pcat_hard")],
        [InlineKeyboardButton(text="Хард (20-100К)",   callback_data="pcat_ultra")],
        [InlineKeyboardButton(text="Назад",             callback_data="search_mode_select")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)

# Шаг 3: кого искать (после выбора категории)
def who_search_kb(cat, mode):
    # mode: "mkt" или "prf"
    prefix = mode + "_" + cat + "_"
    back   = "mode_market" if mode == "mkt" else "mode_profile"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Всех",      callback_data=prefix + "all")],
        [InlineKeyboardButton(text="Девушек",   callback_data=prefix + "girls")],
        [InlineKeyboardButton(text="По модели", callback_data=prefix + "model")],
        [InlineKeyboardButton(text="Назад",     callback_data=back)],
    ])

def settings_menu_kb(uid):
    mg     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    bst    = get_boost(uid)
    reg    = get_region(uid)
    lim    = get_limit(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    reg_label = REGIONS.get(reg, {}).get("label", "🌍 Все")
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мин. гифтов: " + str(mg),     callback_data="set_min_gifts")],
        [InlineKeyboardButton(text="Макс. гифтов: " + mx_str,     callback_data="set_max_gifts")],
        [InlineKeyboardButton(text="Буст цен: " + str(bst) + "%", callback_data="set_boost")],
        [InlineKeyboardButton(text="Лимит выдачи: " + str(lim),   callback_data="set_limit")],
        [InlineKeyboardButton(text="Регион: " + reg_label,        callback_data="set_region")],
        [InlineKeyboardButton(text="Назад",                        callback_data="menu")],
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
            InlineKeyboardButton(text="300%", callback_data="boost_300"),
        ],
        [InlineKeyboardButton(text="✏️ Ввести вручную", callback_data="boost_custom")],
        [InlineKeyboardButton(text="Назад", callback_data="settings_menu")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ СТОП", callback_data="stop_search")],
    ])

def limit_picker_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="10",  callback_data="lim_10"),
            InlineKeyboardButton(text="20",  callback_data="lim_20"),
            InlineKeyboardButton(text="30",  callback_data="lim_30"),
            InlineKeyboardButton(text="50",  callback_data="lim_50"),
        ],
        [
            InlineKeyboardButton(text="70",  callback_data="lim_70"),
            InlineKeyboardButton(text="100", callback_data="lim_100"),
            InlineKeyboardButton(text="120", callback_data="lim_120"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="settings_menu")],
    ])

def limit_picker_settings_kb(current=50):
    def lbl(v): return str(v) + (" ✅" if v == current else "")
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=lbl(10),  callback_data="lim_10"),
            InlineKeyboardButton(text=lbl(20),  callback_data="lim_20"),
            InlineKeyboardButton(text=lbl(30),  callback_data="lim_30"),
            InlineKeyboardButton(text=lbl(50),  callback_data="lim_50"),
        ],
        [
            InlineKeyboardButton(text=lbl(70),  callback_data="lim_70"),
            InlineKeyboardButton(text=lbl(100), callback_data="lim_100"),
            InlineKeyboardButton(text=lbl(120), callback_data="lim_120"),
        ],
        [InlineKeyboardButton(text="Назад", callback_data="settings_menu")],
    ])

def region_kb(current="any"):
    rows = []
    items = list(REGIONS.items())
    for i in range(0, len(items), 2):
        row = []
        for key, val in items[i:i+2]:
            label = val["label"] + (" ✅" if key == current else "")
            row.append(InlineKeyboardButton(text=label, callback_data="region_" + key))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="Назад", callback_data="settings_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

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
    rows.append([InlineKeyboardButton(text="Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

MODEL_COL_PAGE_SIZE = 20

def model_col_kb(names, page=0, who="model"):
    """Клавиатура коллекций для поиска по модели с пагинацией."""
    start = page * MODEL_COL_PAGE_SIZE
    end   = min(start + MODEL_COL_PAGE_SIZE, len(names))
    chunk = names[start:end]
    rows  = []
    for i in range(0, len(chunk), 2):
        idx1 = start + i
        row  = [InlineKeyboardButton(text=chunk[i], callback_data="mdlcol_" + str(idx1) + "_" + who)]
        if i + 1 < len(chunk):
            idx2 = start + i + 1
            row.append(InlineKeyboardButton(text=chunk[i+1], callback_data="mdlcol_" + str(idx2) + "_" + who))
        rows.append(row)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="Назад", callback_data="mdlpage_" + str(page - 1) + "_" + who))
    if end < len(names):
        nav.append(InlineKeyboardButton(text="Далее", callback_data="mdlpage_" + str(page + 1) + "_" + who))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="Назад в меню", callback_data="mode_model")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def owner_card_kb(username, profile_url, owner_uid):
    btns = []
    # Кнопка профиля — прямой переход
    if username:
        btns.append([InlineKeyboardButton(text="👤 @" + username, url="https://t.me/" + username)])
    elif profile_url:
        btns.append([InlineKeyboardButton(text="👤 Профиль", url=profile_url)])
    # Написать
    if username:
        msg = urllib.parse.quote("Привет! Хочу купить твои NFT")
        btns.append([InlineKeyboardButton(text="✉️ Написать", url="https://t.me/" + username + "?text=" + msg)])
    # Показать все NFT
    btns.append([InlineKeyboardButton(text="🎁 Все NFT", callback_data="shownft_" + str(owner_uid))])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def model_nft_kb(nft_url, username, title, num):
    """Кнопки для карточки NFT в режиме поиска по модели."""
    import urllib.parse as _up
    btns = []
    if nft_url:
        btns.append([InlineKeyboardButton(text="🔗 Смотреть NFT", url=nft_url)])
    if username:
        btns.append([InlineKeyboardButton(text="👤 @" + username, url="https://t.me/" + username)])
        nft_link = nft_url or ""
        msg = _up.quote(
            "Привет! Хочу купить твой " + str(title) + " #" + str(num) +
            ("\n" + nft_link if nft_link else "")
        )
        btns.append([InlineKeyboardButton(
            text="✉️ Написать",
            url="https://t.me/" + username + "?text=" + msg
        )])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

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
# Онбординг — только текстовый ввод, клавиатуры убраны
def onboarding_min_kb():   return None
def onboarding_max_kb():   return None
def onboarding_boost_kb(): return None


# ===================== CORE: РЫНОК =====================
async def do_market_search(
    status_msg,
    gift_ids,
    cat=None,
    girls_only=False,
    model_only=False,
    max_results=50,
    boost=100,
    min_gifts=1,
    max_gifts=0,
    profile_only=False,
    region="any",
    user_seen_owners=None,
):
    global is_searching
    is_searching  = True
    found         = 0
    seen_slugs    = set()
    # seen_owners — дедупликация владельцев внутри одного поиска (для girls/all режима)
    seen_owners   = set(user_seen_owners) if user_seen_owners is not None else set()
    owner_buckets = {}   # uid -> bucket (для режима all без cat)
    owner_seen    = {}   # uid -> count (для has_cat дедупликации)
    last_upd      = 0.0
    has_cat       = cat is not None
    # В model-режиме min_gifts всегда 1 (каждый NFT отдельно)
    effective_min = 1 if (model_only or girls_only) else min_gifts

    send_lock = asyncio.Lock()

    async def send_msg(txt, kb):
        nonlocal found
        if not is_searching or found >= max_results:
            return False
        async with send_lock:
            if not is_searching or found >= max_results:
                return False
            try:
                await status_msg.bot.send_message(
                    chat_id=status_msg.chat.id, text=txt,
                    parse_mode="HTML", reply_markup=kb,
                    disable_web_page_preview=True,
                )
                found += 1
                stats["found"] += 1
                return True
            except Exception as e:
                logger.warning("send_msg: %s", e)
                return False

    async def send_owner_card(uid, bucket):
        if uid in seen_owners:
            return
        seen_owners.add(uid)
        if user_seen_owners is not None:
            user_seen_owners.add(uid)
        items       = bucket["items"]
        username    = bucket["username"]
        profile_url = bucket["profile_url"]
        owner_str   = fmt_owner(bucket["owner"], username, bucket["name"])
        cache_owner(uid, bucket["owner"], username, bucket["name"], profile_url, items)
        kb  = owner_card_kb(username, profile_url, uid)
        txt = "<b>Владелец: " + owner_str + "\nNFT на рынке: " + str(len(items)) + "</b>"
        await send_msg(txt, kb)

    async def process_item(item, floor):
        nft_url = item.get("nft_url") or ""
        slug    = nft_url.split("/")[-1] if nft_url else ""
        if slug and slug in seen_slugs:
            return
        if slug:
            seen_slugs.add(slug)

        price    = item.get("price")
        oid      = item["owner_id"]
        username = item.get("username")
        name     = item.get("name", "")
        title    = item.get("title", "?")
        num      = item.get("num", "?")
        nft_url2 = item.get("nft_url")

        # Фильтр по региону (быстрая проверка)
        if not region_match(item["owner"], username, name, region):
            return

        # Фильтр по полу/модели
        if girls_only and not is_girl(item["owner"], username, name):
            return
        if model_only and not is_model(item["owner"], username, name):
            return

        # Фильтр по цене (только если есть floor)
        if has_cat and floor and price:
            if not price_ok_for_floor(price, floor, boost):
                return

        # ── РЕЖИМ "по модели" ─ каждый NFT отдельной карточкой ──
        if model_only:
            prof_url2 = item.get("profile_url") or ("https://t.me/" + username if username else "")
            prof_txt  = ("\n<b>👤 <a href=\"" + prof_url2 + "\">" + ("@"+username) + "</a></b>") if username else ""
            nft_txt   = ("\n<b>🔗 <a href=\"" + str(nft_url2) + "\">" + str(title) + " #" + str(num) + "</a></b>") if nft_url2 else ("\n<b>" + str(title) + " #" + str(num) + "</b>")
            price_txt = ("<b>Цена: " + str(price) + " ⭐</b>") if price else ""
            txt = price_txt + nft_txt + prof_txt
            if oid:
                cache_owner(oid, item["owner"], username, name, item.get("profile_url"), [item])
            kb = model_nft_kb(nft_url2, username, title, num)
            await send_msg(txt, kb)
            return

        # ── РЕЖИМ "девушки" или "все" с категорией ─ одна карточка на NFT ──
        if has_cat or girls_only:
            if not oid or oid in seen_owners:
                return
            if price is None:
                return
            if has_cat and owner_seen.get(oid, 0) >= 3:
                return
            if has_cat:
                owner_seen[oid] = owner_seen.get(oid, 0) + 1

            profile_url = item.get("profile_url")
            owner_str   = fmt_owner(item["owner"], username, name)
            nft_line = ("\n<b><a href=\"" + nft_url2 + "\">" + str(title) + " #" + str(num) + "</a></b>") if nft_url2 else ("\n<b>" + str(title) + " #" + str(num) + "</b>")
            prof_line = ("\n<b>👤 <a href=\"" + profile_url + "\">" + ("@" + username if username else "открыть") + "</a></b>") if profile_url else ""
            txt = "<b>Владелец: " + owner_str + "</b>" + nft_line + "\n<b>Цена: " + str(price) + " ⭐</b>" + prof_line
            if oid:
                cache_owner(oid, item["owner"], username, name, profile_url, [item])
            kb = owner_card_kb(username, profile_url, oid or 0)
            ok = await send_msg(txt, kb)
            if ok and oid:
                seen_owners.add(oid)
                if user_seen_owners is not None:
                    user_seen_owners.add(oid)
            return

        # ── РЕЖИМ "все" без категории — bucket по владельцу ──
        if not oid or oid in seen_owners:
            return
        if oid not in owner_buckets:
            owner_buckets[oid] = {
                "owner": item["owner"], "username": username,
                "name": name, "profile_url": item.get("profile_url"), "items": [],
            }
        if nft_url2:
            owner_buckets[oid]["items"].append(item)
        bucket = owner_buckets[oid]
        if gifts_in_range(len(bucket["items"]), effective_min, max_gifts) and oid not in seen_owners:
            if is_searching and found < max_results:
                owner_buckets.pop(oid, None)
                await send_owner_card(oid, bucket)

    async def flush_ready_owners():
        ready = [(uid, bk) for uid, bk in list(owner_buckets.items())
                 if gifts_in_range(len(bk["items"]), effective_min, max_gifts) and uid not in seen_owners]
        for uid, bk in ready:
            if not is_searching or found >= max_results:
                break
            owner_buckets.pop(uid, None)
            await send_owner_card(uid, bk)

    # ── Параллельный скан коллекции ──
    async def scan_collection(gid):
        """Сканирует одну коллекцию и обрабатывает все item'ы."""
        floor = None
        if has_cat:
            floor = PRICE_FLOOR_CACHE.get(gid)
            if floor is None:
                try:
                    result = await tg_client(GetResaleStarGiftsRequest(gift_id=gid, offset="", limit=20))
                    prices = [p for g in (getattr(result, "gifts", None) or [])
                              if (p := get_resell_price(g)) and p > 0]
                    if prices:
                        prices.sort()
                        floor = prices[max(0, len(prices) // 4)]
                        PRICE_FLOOR_CACHE[gid] = floor
                except FloodWaitError as e:
                    await asyncio.sleep(e.seconds + 1)
                except Exception:
                    pass
            if floor is None:
                return
            if not floor_in_category(floor, cat):
                return
        try:
            items, _ = await fetch_market_page(gid, "", limit=100)
        except Exception:
            return
        for item in items:
            if not is_searching or found >= max_results:
                return
            await process_item(item, floor)

    try:
        await status_msg.edit_text("<b>🔍 Поиск запущен...</b>", reply_markup=stop_kb())

        import random as _random
        total_ids = list(gift_ids)
        _random.shuffle(total_ids)

        # Параллельный скан — батчами по 8 коллекций одновременно
        BATCH = 8
        scanned = 0
        for i in range(0, len(total_ids), BATCH):
            if not is_searching or found >= max_results:
                break
            batch = total_ids[i:i+BATCH]
            await asyncio.gather(*[scan_collection(gid) for gid in batch])
            scanned += len(batch)

            now = asyncio.get_event_loop().time()
            if now - last_upd > 1.5:
                try:
                    lbl = "девушек" if girls_only else ("моделей" if model_only else "NFT")
                    await status_msg.edit_text(
                        "<b>Проверено: " + str(min(scanned, len(total_ids))) + "/" + str(len(total_ids)) + "\n"
                        "Найдено " + lbl + ": " + str(found) + "</b>",
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                except Exception:
                    pass
                last_upd = now

        await flush_ready_owners()

    except Exception as e:
        logger.error("do_market_search: %s", e)
    finally:
        is_searching = False
    return found



# ===================== CORE: ПРОФИЛИ (быстрый поиск по юзам с маркета) =====================
async def do_profile_search(
    status_msg,
    gift_ids,
    girls_only=False,
    model_only=False,
    max_results=50,
    min_gifts=2,
    max_gifts=0,
    region="any",
    user_seen_owners=None,
):
    global is_searching
    is_searching = True
    found        = 0
    seen_uids    = set(user_seen_owners) if user_seen_owners is not None else set()
    last_upd     = 0.0

    # uid -> {"owner", "username", "name", "profile_url", "items": [...]}
    uid_data     = {}

    # Шаг 1: собираем ВСЕХ юзеров — параллельный скан батчами
    async def fetch_one(gid):
        try:
            items, _ = await fetch_market_page(gid, "", limit=100)
        except Exception:
            return
        for item in items:
            uid = item.get("owner_id")
            if not uid:
                continue
            if uid not in uid_data:
                uid_data[uid] = {
                    "owner":       item["owner"],
                    "username":    item.get("username"),
                    "name":        item.get("name", ""),
                    "profile_url": item.get("profile_url"),
                    "items":       [],
                }
            nft_url = item.get("nft_url")
            if nft_url:
                uid_data[uid]["items"].append({
                    "title":   item.get("title","?"),
                    "num":     item.get("num","?"),
                    "nft_url": nft_url,
                    "price":   item.get("price"),
                })

    async def collect_all():
        import random as _random
        ids = list(gift_ids)
        _random.shuffle(ids)
        BATCH = 10
        scanned = 0
        for i in range(0, len(ids), BATCH):
            if not is_searching:
                break
            batch = ids[i:i+BATCH]
            await asyncio.gather(*[fetch_one(gid) for gid in batch])
            scanned += len(batch)
            now = asyncio.get_event_loop().time()
            nonlocal last_upd
            if now - last_upd > 1.5:
                try:
                    await status_msg.edit_text(
                        "<b>🔍 Сканирую: " + str(min(scanned, len(ids))) + "/" + str(len(ids)) + "\n"
                        "Юзеров: " + str(len(uid_data)) + "</b>",
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                except Exception:
                    pass
                last_upd = now

    # Шаг 2: фильтруем и показываем
    async def process_uid(uid, info):
        nonlocal found
        if found >= max_results:
            return
        if uid in seen_uids:
            return
        owner_obj   = info["owner"]
        username    = info["username"]
        name        = info["name"]
        profile_url = info["profile_url"] or (("https://t.me/" + username) if username else ("tg://user?id=" + str(uid)))
        items       = info["items"]

        if not region_match(owner_obj, username, name, region):
            return
        if girls_only and not is_girl(owner_obj, username, name):
            return
        if model_only and not is_model(owner_obj, username, name):
            return
        if not gifts_in_range(len(items), min_gifts, max_gifts):
            return

        owner_str = fmt_owner(owner_obj, username, name)
        cache_owner(uid, owner_obj, username, name, profile_url, items)

        # NFT список со ссылками (до 5)
        nft_lines = ""
        for g in items[:5]:
            nu = g.get("nft_url")
            t  = g.get("title","?")
            n  = g.get("num","?")
            p  = g.get("price")
            p_str = (" — " + str(p) + " ⭐") if p else ""
            if nu:
                nft_lines += "\n<b>• <a href=\"" + nu + "\">" + str(t) + " #" + str(n) + "</a>" + p_str + "</b>"
            else:
                nft_lines += "\n<b>• " + str(t) + " #" + str(n) + p_str + "</b>"
        if len(items) > 5:
            nft_lines += "\n<b>  ... ещё " + str(len(items)-5) + "</b>"

        prof_link = "<b><a href=\"" + profile_url + "\">" + ("@"+username if username else "Профиль") + "</a></b>"
        txt = (
            "<b>Владелец: " + owner_str + "</b>\n"
            + prof_link + "\n"
            "<b>NFT: " + str(len(items)) + " шт.</b>"
            + nft_lines
        )
        kb = owner_card_kb(username, profile_url, uid)
        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id, text=txt,
                parse_mode="HTML", reply_markup=kb,
                disable_web_page_preview=True,
            )
            found += 1
            stats["found"] += 1
            seen_uids.add(uid)
            if user_seen_owners is not None:
                user_seen_owners.add(uid)
        except Exception as e:
            logger.warning("profile send: %s", e)

    try:
        await status_msg.edit_text("<b>🔍 Поиск запущен...</b>", reply_markup=stop_kb())
        await collect_all()

        if not is_searching:
            is_searching = False
            return 0

        all_uids = list(uid_data.items())
        total    = len(all_uids)

        if total == 0:
            return 0

        await status_msg.edit_text(
            "<b>Собрано юзеров: " + str(total) + "\nФильтрую и показываю...</b>",
            reply_markup=stop_kb()
        )

        for uid, info in all_uids:
            if not is_searching or found >= max_results:
                break
            await process_uid(uid, info)
            now = asyncio.get_event_loop().time()
            if now - last_upd > 1.5:
                try:
                    lbl = "девушек" if girls_only else ("моделей" if model_only else "профилей")
                    await status_msg.edit_text(
                        "<b>Найдено " + lbl + ": " + str(found) + "/" + str(max_results) + "</b>",
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                except Exception:
                    pass
                last_upd = now

    except Exception as e:
        logger.error("do_profile_search: %s", e)
    finally:
        is_searching = False
    return found



async def ensure_collections():
    if not ALL_GIFT_IDS:
        await load_collections()
    return [gid for gid, _ in ALL_GIFT_IDS]

async def run_market(cb, cat=None, girls=False, model=False, ids=None, col_name=None, limit=None):
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
    reg    = get_region(uid)
    lim    = limit if limit else get_limit(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    reg_label = REGIONS.get(reg, {}).get("label", "Все")
    # Уже виденные владельцы для этого юзера
    user_seen = USER_SEEN_OWNERS.setdefault(uid, set())
    if col_name and model:
        label = "Модели. " + col_name
    elif girls:
        label = "Девушки. Маркет"
    elif model:
        label = "По модели. Маркет"
    elif cat:
        label = PRICE_CATEGORIES[cat]["label"] + ". Маркет"
    else:
        label = "Маркет"
    txt = (
        "<b>" + label + "\n"
        "Регион: " + reg_label + " | Лимит: " + str(lim) + "\n"
        "Гифтов: от " + str(mn) + " до " + mx_str + ", Буст: " + str(boost) + "%\n\n"
        "Найдено: 0</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_market_search(status, ids, cat=cat, girls_only=girls, model_only=model,
                                   boost=boost, min_gifts=mn, max_gifts=mx,
                                   max_results=lim, region=reg, user_seen_owners=user_seen)
    try:
        await status.edit_text(
            "<b>Готово! " + label + "\nНайдено: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass

async def run_profile(cb, girls=False, model=False, ids=None, limit=None):
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
    reg    = get_region(uid)
    lim    = limit if limit else get_limit(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    reg_label = REGIONS.get(reg, {}).get("label", "Все")
    user_seen = USER_SEEN_OWNERS.setdefault(uid, set())
    if girls:
        label = "Девушки. Профиль"
    elif model:
        label = "По модели. Профиль"
    else:
        label = "Все. Профиль"
    txt = (
        "<b>" + label + "\n"
        "Регион: " + reg_label + " | Лимит: " + str(lim) + "\n"
        "Гифтов скрытых: от " + str(mn) + " до " + mx_str + "\n\n"
        "Сканирую...</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_profile_search(status, ids, girls_only=girls, model_only=model,
                                    min_gifts=mn, max_gifts=mx,
                                    max_results=lim, region=reg, user_seen_owners=user_seen)
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
    uid    = message.from_user.id
    is_new = add_user(uid, message.from_user.username,
                      message.from_user.first_name, message.from_user.last_name)

    if not await check_authorized() and is_admin(uid):
        await message.answer(
            "<b>Нужна авторизация Telegram\nВведи номер телефона:</b>",
            parse_mode="HTML"
        )
        await state.set_state(Auth.phone)
        return

    # Онбординг обязателен - пока не пройден, никуда не пускаем
    if uid not in ONBOARDING_DONE:
        await message.answer(
            "<b>Добро пожаловать в Neptun Parser\n\n"
            "Настроим поиск — 3 быстрых шага.\n\n"
            "Шаг 1 из 3\n"
            "Минимум гифтов у владельца\n"
            "Введи число (например: 2):</b>",
            parse_mode="HTML"
        )
        await state.set_state(Onboarding.min_gifts)
        return

    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    bst    = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    await message.answer(
        "<b>Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Буст: " + str(bst) + "%\n\n"
        "Выбери действие:</b>",
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )

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
        "<b>Мин. гифтов: " + str(val) + "\n\n"
        "Шаг 2 из 3. Максимум гифтов\n"
        "Введи число (0 = без лимита):</b>",
        parse_mode="HTML"
    )
    await state.set_state(Onboarding.max_gifts)

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
        "<b>Макс. гифтов: " + label + "\n\n"
        "Шаг 3 из 3. Буст цен\n"
        "Введи число в % (например: 100)\n"
        "100 = цена до x2 от флора, 200 = до x3</b>",
        parse_mode="HTML"
    )
    await state.set_state(Onboarding.boost)

# Онбординг шаг 3 — буст (текст)
@dp.message(Onboarding.boost)
async def ob_boost_text(message: Message, state: FSMContext):
    try:
        val = int(message.text.strip())
        if val < 1 or val > 9999:
            raise ValueError
    except ValueError:
        await message.answer("<b>Введи число от 1 до 9999:</b>", parse_mode="HTML")
        return
    uid = message.from_user.id
    USER_BOOST[uid] = val
    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    await state.clear()
    await message.answer(
        "<b>Настройка завершена!\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Буст: " + str(val) + "%\n\n"
        "Можно изменить в Настройки</b>",
        parse_mode="HTML"
    )
    ONBOARDING_DONE.add(uid)
    save_onboarding(ONBOARDING_DONE)
    await message.answer(
        "<b>Neptun Parser\n\nВыбери действие:</b>",
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("<b>Отменено.</b>", parse_mode="HTML", reply_markup=main_menu_kb())

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
    reg       = get_region(uid)
    reg_label = REGIONS.get(reg, {}).get("label", "Все")
    txt = (
        "<b>Neptun Parser\n\n"
        "Бот ищет владельцев NFT-гифтов в Telegram по заданным фильтрам.\n\n"
        "РЕЖИМЫ ПОИСКА\n\n"
        "Поиск по маркету\n"
        "Ищет NFT которые сейчас выставлены на продажу.\n"
        "Фильтрует по цене относительно флора коллекции.\n"
        "Флор = минимальная цена в коллекции (нижние 25%).\n"
        "Буст определяет насколько выше флора смотреть.\n\n"
        "Поиск по профилю\n"
        "Ищет владельцев у которых NFT есть в профиле, но скрыты от рынка.\n"
        "Это люди которые держат гифты и не продают.\n\n"
        "Поиск по модели\n"
        "Показывает каждый NFT отдельно — владельцы похожи на моделей/блогеров.\n\n"
        "ФИЛЬТРЫ\n\n"
        "Всех — показывает всех владельцев без фильтра.\n\n"
        "Девушек — фильтрует по имени и ключевым словам в профиле.\n\n"
        "По модели — ищет по onlyfans, model, content, nsfw и другим признакам.\n\n"
        "НАСТРОЙКИ\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Минимальное количество NFT у владельца.\n\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Максимальное количество. 0 = без лимита.\n\n"
        "Буст цен: " + str(bst) + "%\n"
        "Только для поиска по маркету. 100% = до x2 флора.\n\n"
        "Регион: " + reg_label + "\n"
        "Фильтр по языку профиля владельца.\n\n"
        "КОМАНДЫ\n\n"
        "/start — главное меню\n"
        "/clear — остановить поиск\n"
        "/neptunteam — это сообщение</b>"
    )
    await message.answer(txt, parse_mode="HTML", reply_markup=main_menu_kb())

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
    txt = "<b>NFT: " + owner_str + "\nВсего: " + str(len(items)) + "</b>"
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=kb)

@dp.callback_query(F.data == "search_mode_select")
async def cb_search_mode_select(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Выбери режим поиска:</b>",
        parse_mode="HTML",
        reply_markup=search_mode_select_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery):
    uid    = cb.from_user.id
    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    bst    = get_boost(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    await cb.message.answer(
        "<b>Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Буст: " + str(bst) + "%\n\n"
        "Выбери действие:</b>",
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Поиск по маркету. Выбери категорию цены:</b>",
        parse_mode="HTML",
        reply_markup=market_cat_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "mode_profile")
async def cb_mode_profile(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Поиск по профилю. Выбери категорию цены:</b>",
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
        "Напиши число:</b>",
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
async def cb_set_boost(cb: CallbackQuery, state: FSMContext):
    txt = "<b>Буст цен</b>\n\n<b>100% = до x2 флора, 150% = до x2.5, 200% = до x3</b>"
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=boost_picker_kb())
    await cb.answer()

@dp.callback_query(F.data == "set_limit")
async def cb_set_limit(cb: CallbackQuery):
    lim = get_limit(cb.from_user.id)
    await cb.message.answer(
        "<b>Лимит выдачи результатов\n\n"
        "Выбери сколько результатов показывать за один поиск:</b>",
        parse_mode="HTML",
        reply_markup=limit_picker_settings_kb(lim)
    )
    await cb.answer()

@dp.callback_query(F.data == "set_region")
async def cb_set_region(cb: CallbackQuery):
    reg = get_region(cb.from_user.id)
    await cb.message.answer(
        "<b>Выбери регион поиска:\n"
        "Бот будет фильтровать владельцев по языку профиля</b>",
        parse_mode="HTML",
        reply_markup=region_kb(reg)
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("region_"))
async def cb_region_pick(cb: CallbackQuery):
    key = cb.data[7:]
    if key not in REGIONS:
        await cb.answer("Неизвестный регион", show_alert=True)
        return
    USER_REGION[cb.from_user.id] = key
    label = REGIONS[key]["label"]
    await cb.answer("Регион: " + label, show_alert=False)
    await cb.message.edit_reply_markup(reply_markup=region_kb(key))

@dp.callback_query(F.data == "boost_custom")
async def cb_boost_custom(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "<b>Введи буст цен вручную (число от 1 до 9999):</b>",
        parse_mode="HTML",
        reply_markup=gifts_input_cancel_kb()
    )
    await state.set_state(SetGifts.boost)
    await cb.answer()

@dp.message(SetGifts.boost)
async def set_boost_text(message: Message, state: FSMContext):
    try:
        val = int(message.text.strip())
        if val < 1 or val > 9999:
            raise ValueError
    except ValueError:
        await message.answer("<b>Введи число от 1 до 9999:</b>", parse_mode="HTML")
        return
    USER_BOOST[message.from_user.id] = val
    await state.clear()
    await message.answer(
        "<b>Буст установлен: " + str(val) + "%</b>",
        parse_mode="HTML",
        reply_markup=settings_menu_kb(message.from_user.id)
    )

@dp.callback_query(F.data.startswith("boost_"))
async def cb_boost(cb: CallbackQuery):
    val = int(cb.data.split("_")[1])
    USER_BOOST[cb.from_user.id] = val
    await cb.answer("Буст: " + str(val) + "%", show_alert=True)
    await cb.message.edit_reply_markup(reply_markup=None)


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

# Рынок — запуск после выбора "кого" (mkt_<cat>_all / mkt_<cat>_girls / mkt_<cat>_model)
@dp.callback_query(F.data.startswith("mkt_"))
async def cb_mkt_who(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split("_")  # ["mkt", cat, who]
    if len(parts) < 3:
        await cb.answer()
        return
    cat      = parts[1]
    who      = parts[2]
    real_cat = None if cat == "all" else cat
    girls    = (who == "girls")
    model    = (who == "model")
    await cb.answer()
    await run_market(cb, cat=real_cat, girls=girls, model=model)

# Профиль — запуск после выбора "кого" (prf_<cat>_all / prf_<cat>_girls / prf_<cat>_model)
@dp.callback_query(F.data.startswith("prf_"))
async def cb_prf_who(cb: CallbackQuery, state: FSMContext):
    parts = cb.data.split("_")  # ["prf", cat, who]
    if len(parts) < 3:
        await cb.answer()
        return
    who   = parts[2]
    girls = (who == "girls")
    model = (who == "model")
    await cb.answer()
    await run_profile(cb, girls=girls, model=model)

# По коллекции
@dp.callback_query(F.data == "mode_col")
async def cb_mode_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("<b>Коллекции не загружены</b>", parse_mode="HTML", reply_markup=menu_kb())
        await cb.answer()
        return
    await cb.message.answer("<b>По коллекции: выбери источник:</b>", parse_mode="HTML", reply_markup=col_source_kb())
    await cb.answer()

# Поиск по модели — сначала выбираем кого искать
@dp.callback_query(F.data == "mode_model")
async def cb_mode_model(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Поиск по маркету.\nКого искать?</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Всех",      callback_data="mdlwho_all")],
            [InlineKeyboardButton(text="Девушек",   callback_data="mdlwho_girls")],
            [InlineKeyboardButton(text="По модели", callback_data="mdlwho_model")],
            [InlineKeyboardButton(text="Назад",     callback_data="search_mode_select")],
        ])
    )
    await cb.answer()

# Выбор кого — переходим к выбору коллекции
@dp.callback_query(F.data.startswith("mdlwho_"))
async def cb_mdlwho(cb: CallbackQuery):
    who = cb.data[7:]  # all, girls, model
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("<b>Коллекции не загружены</b>", parse_mode="HTML", reply_markup=menu_kb())
        await cb.answer()
        return
    names = list(NFT_COLLECTIONS.keys())
    total = len(names)
    lbl = {"all": "Все", "girls": "Девушки", "model": "По модели"}.get(who, who)
    await cb.message.answer(
        "<b>Поиск: " + lbl + ". Выбери коллекцию NFT:\n"
        "Всего коллекций: " + str(total) + "</b>",
        parse_mode="HTML",
        reply_markup=model_col_kb(names, page=0, who=who)
    )
    await cb.answer()

# Пагинация списка коллекций для поиска по модели
@dp.callback_query(F.data.startswith("mdlpage_"))
async def cb_mdlpage(cb: CallbackQuery):
    parts = cb.data[8:].split("_")
    page  = int(parts[0])
    who   = parts[1] if len(parts) > 1 else "model"
    names = list(NFT_COLLECTIONS.keys())
    total = len(names)
    lbl = {"all": "Все", "girls": "Девушки", "model": "По модели"}.get(who, who)
    await cb.message.edit_text(
        "<b>Поиск: " + lbl + ". Выбери коллекцию NFT:\n"
        "Всего коллекций: " + str(total) + "</b>",
        parse_mode="HTML",
        reply_markup=model_col_kb(names, page=page, who=who)
    )
    await cb.answer()

# Выбор коллекции — запускаем маркет
@dp.callback_query(F.data.startswith("mdlcol_"))
async def cb_mdlcol(cb: CallbackQuery, state: FSMContext):
    parts    = cb.data[7:].split("_")
    idx      = int(parts[0])
    who      = parts[1] if len(parts) > 1 else "model"
    lst      = list(NFT_COLLECTIONS.items())
    if idx >= len(lst):
        await cb.answer("Не найдено", show_alert=True)
        return
    col_name, col_id = lst[idx]
    girls = (who == "girls")
    model = (who == "model")
    await cb.answer()
    await run_market(cb, ids=[col_id], girls=girls, model=model, col_name=col_name)

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

# lim_ callback — для настройки лимита из меню настроек
@dp.callback_query(F.data.startswith("lim_"))
async def cb_limit_pick(cb: CallbackQuery, state: FSMContext):
    lim = int(cb.data[4:])
    USER_LIMIT[cb.from_user.id] = lim
    await cb.answer("Лимит сохранён: " + str(lim), show_alert=False)
    await cb.message.edit_reply_markup(reply_markup=limit_picker_settings_kb(lim))

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
    mx_str = str(mx) if mx > 0 else "без лимита"
    txt = (
        "<b>Статистика\n\n"
        "Поисков: " + str(stats["checks"]) + "\n"
        "Найдено: " + str(stats["found"]) + "\n"
        "Пользователей: " + str(get_user_count()) + "\n\n"
        "Настройки:\n"
        "Мин: " + str(mn) + ", Макс: " + mx_str + ", Буст: " + str(bst) + "%</b>"
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
    await cb.message.answer("<b>Отправь сообщение для рассылки. /cancel - отмена</b>", parse_mode="HTML", reply_markup=cancel_kb())
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
    await cb.message.answer("<b>Введи номер телефона:</b>", parse_mode="HTML")
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
        await message.answer("<b>Формат: +71234567890</b>", parse_mode="HTML")
        return
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
            await asyncio.sleep(1)
        res = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=res.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("<b>Код отправлен. Введи код:</b>", parse_mode="HTML")
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
            parse_mode="HTML", reply_markup=main_menu_kb()
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
            parse_mode="HTML", reply_markup=main_menu_kb()
        )
    except Exception as e:
        await message.answer("<b>Неверный пароль:</b> <code>" + str(e) + "</code>", parse_mode="HTML")


async def main():
    global ONBOARDING_DONE
    ONBOARDING_DONE = load_onboarding()
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("Neptun Parser запущен!")
    # Регистрируем /start как команду — появится голубая кнопка "Меню" слева от поля ввода
    from aiogram.types import BotCommand
    await bot.set_my_commands([BotCommand(command="start", description="Главное меню")])
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info("Авторизован, коллекций: %d", len(ALL_GIFT_IDS))
        else:
            logger.warning("Не авторизован, пройди /start")
    except Exception as e:
        logger.error("Ошибка старта: %s", e)
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
