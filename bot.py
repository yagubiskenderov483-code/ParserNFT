import asyncio
import logging
import urllib.parse
import os
import json
import time
import datetime
import re
import random
from telethon import TelegramClient
from telethon.tl.functions.payments import (
    GetResaleStarGiftsRequest, GetStarGiftsRequest, GetSavedStarGiftsRequest
)
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

API_ID       = 36101343
API_HASH     = "116195fa5e0459d25a9a6266b40807d7"
BOT_TOKEN    = "8406363273:AAG-ucchhMA09n8j_XSGFtE02iu3Oiwzj_0"
ADMIN_ID     = 8726084830
SESSION_NAME = "nft_session"
USERS_FILE   = "users.json"
ONBOARDING_FILE = "onboarding_done.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot       = Bot(token=BOT_TOKEN)
dp        = Dispatcher(storage=MemoryStorage())
tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

stats             = {"checks": 0, "found": 0}
is_searching      = False
ALL_GIFT_IDS      = []   # list of (gid, title)
NFT_COLLECTIONS   = {}   # title -> gid
PRICE_FLOOR_CACHE = {}
NFT_CACHE         = {}
USER_BOOST        = {}
USER_MIN_GIFTS    = {}
USER_MAX_GIFTS    = {}
USER_LIMIT        = {}
USER_REGION       = {}
ONBOARDING_DONE   = set()

DEFAULT_BOOST     = 100
DEFAULT_MIN_GIFTS = 2
DEFAULT_MAX_GIFTS = 0
DEFAULT_LIMIT     = 30
DEFAULT_REGION    = "any"

# ===================== REGIONS =====================
REGIONS = {
    "any": {"label": "Все страны"},
    "ru":  {"label": "Россия"},
    "ua":  {"label": "Украина"},
    "by":  {"label": "Беларусь"},
    "us":  {"label": "США"},
    "uk":  {"label": "Великобритания"},
    "de":  {"label": "Германия"},
    "fr":  {"label": "Франция"},
    "es":  {"label": "Испания"},
    "tr":  {"label": "Турция"},
    "ae":  {"label": "ОАЭ"},
    "cn":  {"label": "Китай"},
    "jp":  {"label": "Япония"},
    "in":  {"label": "Индия"},
}

RU_LETTERS = set("абвгдеёжзийклмнопрстуфхцчшщъыьэюяіїєґ")

def _has_cyrillic(text):
    t = text.lower()
    return sum(1 for c in t if c in RU_LETTERS) > 2

def _has_latin(text):
    t = text.lower()
    return sum(1 for c in t if 'a' <= c <= 'z') > 2

def region_match(owner, username, name, region_key):
    if not region_key or region_key == "any":
        return True
    bio   = (getattr(owner, "bio",        "") or "") if owner else ""
    uname = (getattr(owner, "username",   "") or "") if owner else username or ""
    fname = (getattr(owner, "first_name", "") or "") if owner else ""
    lname = (getattr(owner, "last_name",  "") or "") if owner else ""
    if not fname and name:
        parts = name.strip().split()
        fname = parts[0] if parts else ""
        lname = parts[1] if len(parts) > 1 else ""
    full = (bio + " " + uname + " " + fname + " " + lname).lower().strip()
    if not full:
        return True  # профиль закрыт — не исключаем

    cyrillic = _has_cyrillic(full)
    latin    = _has_latin(full)

    if region_key in ("ru", "ua", "by"):
        return cyrillic

    if region_key in ("us", "uk"):
        if cyrillic:
            return False
        return latin

    if region_key == "de":
        if cyrillic:
            return False
        de_chars = set("äöüÄÖÜß")
        if any(c in de_chars for c in full):
            return True
        return any(kw in full for kw in ["german","deutsch","berlin","münchen","hamburg","frankfurt","de"])

    if region_key == "fr":
        if cyrillic:
            return False
        fr_chars = set("àâæçéèêëîïôœùûüÿ")
        if any(c in fr_chars for c in full):
            return True
        return any(kw in full for kw in ["french","paris","france","française","fr"])

    if region_key == "es":
        if cyrillic:
            return False
        es_chars = set("áéíóúüñ¿¡")
        if any(c in es_chars for c in full):
            return True
        return any(kw in full for kw in ["spain","español","madrid","barcelona","es"])

    if region_key == "tr":
        if cyrillic:
            return False
        tr_chars = set("ğüşıöçĞÜŞİÖÇ")
        if any(c in tr_chars for c in full):
            return True
        return any(kw in full for kw in ["turkey","türk","istanbul","ankara","tr"])

    if region_key == "ae":
        if cyrillic:
            return False
        if any('\u0600' <= c <= '\u06ff' for c in full):
            return True
        return any(kw in full for kw in ["dubai","uae","emirates","أبوظبي","الإمارات"])

    if region_key == "cn":
        return any('\u4e00' <= c <= '\u9fff' for c in full)

    if region_key == "jp":
        if any('\u3040' <= c <= '\u30ff' for c in full):
            return True
        if any('\u4e00' <= c <= '\u9fff' for c in full):
            return True
        return any(kw in full for kw in ["japan","tokyo","osaka","jp"])

    if region_key == "in":
        if any('\u0900' <= c <= '\u097f' for c in full):
            return True
        return any(kw in full for kw in ["india","delhi","mumbai","hindi","indian"])

    return True


# ===================== GIRL DETECTION =====================
GIRL_NAMES_SET = {
    "анна","мария","екатерина","елена","ольга","наталья","татьяна","ирина",
    "юлия","алина","виктория","дарья","полина","ксения","валерия","александра",
    "надежда","людмила","галина","лиза","диана","кристина","светлана","милана",
    "арина","вера","жанна","ангелина","карина","оксана","нина","лариса","регина",
    "маша","катя","даша","оля","лена","юля","настя","поля","ксюша","вика","соня",
    "таня","надя","галя","аня","ника","алиса","злата","ева","эвелина","камилла",
    "яна","влада","руслана","женя","сашенька","вероника","кира","стелла","белла",
    "амина","зара","ясмин","нора","рита","мила","тамара","жанна","инна","зоя",
    "anna","maria","kate","elena","olga","natasha","tatiana","irina","diana",
    "alina","dasha","masha","vika","lena","anya","yulia","lisa","sasha","tanya",
    "sonya","arina","karina","milana","zlata","eva","yana","veronika","kira",
    "stella","bella","nina","tina","vera","sofia","sophia","victoria","kristina",
    "valeria","natalia","angelina","jessica","ashley","emily","olivia","ava",
    "isabella","mia","abigail","madison","elizabeth","taylor","hannah","samantha",
    "lauren","grace","lily","ella","amber","kayla","vanessa","chloe","jade",
    "ruby","rose","violet","daisy","aurora","aria","luna","scarlett","zoey",
    "penelope","layla","riley","nora","maya","claire","savannah","eleanor",
    "camila","alexa","leah","aubrey","ariana","alice","lana","lola","zara",
}
BOY_NAMES_SET = {
    "александр","алексей","андрей","антон","артем","борис","вадим","василий",
    "виктор","владимир","вячеслав","геннадий","георгий","григорий","даниил",
    "денис","дмитрий","евгений","иван","игорь","илья","кирилл","константин",
    "леонид","максим","михаил","никита","николай","олег","павел","петр","роман",
    "руслан","сергей","степан","тимур","федор","юрий","яков",
    "alex","alexander","andrey","anton","artem","boris","victor","vladimir",
    "dmitri","dmitry","evgeny","ivan","igor","ilya","kirill","konstantin",
    "maxim","mikhail","nikita","nikolai","oleg","pavel","roman","ruslan",
    "sergey","timur","yuri",
}
GIRL_SIGNALS = [
    "girl","lady","woman","she","her","♀","👩","👸","💃","🌸","💖","💕",
    "💗","👄","💄","🌺","🦋","🌷","🌹","💅","🦄","💫","✨","🍑",
    "девушка","она","женщина","мама","дочь","принцесса","королева","богиня",
    "красотка","кошечка","зайка","лапочка","милашка","красавица","малышка",
    "onlyfans","of","model","модель","content","nsfw","18+",
]
BOY_SIGNALS = [
    "king","boss","bro","dude","male","guy","lord","sultan","парень","мужик",
    "мужчина","чел","братан","он ","сын ","брат ","папа","отец","муж ","дядя",
]

def is_girl(owner, username=None, name=None):
    bio   = (getattr(owner, "bio",        "") or "").lower() if owner else ""
    uname = (getattr(owner, "username",   "") or "").lower() if owner else (username or "").lower()
    fname = (getattr(owner, "first_name", "") or "").lower() if owner else ""
    lname = (getattr(owner, "last_name",  "") or "").lower() if owner else ""
    if not fname and name:
        parts = name.lower().split()
        fname = parts[0] if parts else ""
        lname = parts[1] if len(parts) > 1 else ""
    full = (bio + " " + uname + " " + fname + " " + lname).strip()

    # Мужские имена — точный отказ
    for bn in BOY_NAMES_SET:
        if fname == bn or fname.startswith(bn + " "):
            return False
    # Мужские сигналы
    for sig in BOY_SIGNALS:
        if sig in full:
            return False

    # Женские имена — точное совпадение first_name
    for gn in GIRL_NAMES_SET:
        if fname == gn or (len(gn) >= 4 and fname.startswith(gn)):
            return True

    # Женские сигналы в bio/username
    for sig in GIRL_SIGNALS:
        if sig in full:
            return True

    return False


# ===================== USERS =====================
def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE) as f:
            data = json.load(f)
            if isinstance(data, list):
                return {str(u): {"username": "", "joined": 0} for u in data}
            return data
    return {}

def save_users(u):
    with open(USERS_FILE, "w") as f:
        json.dump(u, f, ensure_ascii=False, indent=2)

def add_user(uid, username=None, first_name=None, last_name=None):
    u   = load_users()
    key = str(uid)
    if key not in u:
        u[key] = {"username": username or "", "first_name": first_name or "",
                  "last_name": last_name or "", "joined": int(time.time())}
        save_users(u)
        return True
    else:
        changed = False
        if username:   u[key]["username"]   = username;   changed = True
        if first_name: u[key]["first_name"] = first_name; changed = True
        if last_name:  u[key]["last_name"]  = last_name;  changed = True
        if changed:    save_users(u)
        return False

def get_user_count(): return len(load_users())

def load_onboarding():
    if os.path.exists(ONBOARDING_FILE):
        with open(ONBOARDING_FILE) as f:
            return set(json.load(f))
    return set()

def save_onboarding():
    with open(ONBOARDING_FILE, "w") as f:
        json.dump(list(ONBOARDING_DONE), f)

def get_boost(uid):     return USER_BOOST.get(uid, DEFAULT_BOOST)
def get_min_gifts(uid): return USER_MIN_GIFTS.get(uid, DEFAULT_MIN_GIFTS)
def get_max_gifts(uid): return USER_MAX_GIFTS.get(uid, DEFAULT_MAX_GIFTS)
def get_limit(uid):     return USER_LIMIT.get(uid, DEFAULT_LIMIT)
def get_region(uid):    return USER_REGION.get(uid, DEFAULT_REGION)
def is_admin(uid):      return int(uid) == ADMIN_ID


# ===================== FSM STATES =====================
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
    limit     = State()

class SetMin(StatesGroup):
    value = State()

class SetMax(StatesGroup):
    value = State()

class SetBoost(StatesGroup):
    value = State()


# ===================== HELPERS =====================
async def check_authorized():
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
        return await tg_client.is_user_authorized()
    except Exception:
        return False

def esc(t):
    return str(t).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

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
        return esc(name) + " (@" + esc(username) + ")"
    if username:
        return "@" + esc(username)
    if name:
        return esc(name)
    return "Скрыт"

def fmt_ts(ts):
    if not ts:
        return "неизвестно"
    return datetime.datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")

def make_nft_url(gift):
    slug = str(getattr(gift, "slug", None) or getattr(gift, "unique_id", None) or "").strip()
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

def floor_in_cat(floor, cat):
    if not cat or cat == "all":
        return True
    CATS = {
        "cheap":   (None,  2000),
        "mid":     (2000,  5000),
        "hard":    (5000,  20000),
        "ultra":   (20000, 100000),
        "extreme": (100000, None),
    }
    c = CATS.get(cat)
    if not c:
        return True
    mn, mx = c
    if mn and floor < mn:
        return False
    if mx and floor > mx:
        return False
    return True

def price_ok(price, floor, boost):
    return floor * 0.7 <= price <= floor * (1.0 + boost / 100.0)

def cache_owner(uid, owner, username, name, profile_url, items):
    NFT_CACHE[uid] = {"owner": owner, "username": username,
                      "name": name, "profile_url": profile_url, "items": items}

CAT_LABELS = {
    "all":     "Все",
    "cheap":   "Дешевые до 2000",
    "mid":     "Средние 2000-5000",
    "hard":    "Сложные 5000-20000",
    "ultra":   "Хард 20000-100000",
    "extreme": "Экстрим от 100000",
}
WHO_LABELS = {"all": "Все", "girls": "Девушки"}


# ===================== KEYBOARDS =====================
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Поиск",      callback_data="search_mode_select")],
        [InlineKeyboardButton(text="Настройки",  callback_data="settings_menu"),
         InlineKeyboardButton(text="Статистика", callback_data="stats")],
    ])

def search_mode_select_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="По маркету",  callback_data="mode_market")],
        [InlineKeyboardButton(text="По профилю",  callback_data="mode_profile")],
        [InlineKeyboardButton(text="Назад",       callback_data="menu")],
    ])

def cat_kb(mode):
    p = "mc_" if mode == "market" else "pc_"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Все",                 callback_data=p+"all")],
        [InlineKeyboardButton(text="Дешевые до 2000",     callback_data=p+"cheap")],
        [InlineKeyboardButton(text="Средние 2000-5000",   callback_data=p+"mid")],
        [InlineKeyboardButton(text="Сложные 5000-20000",  callback_data=p+"hard")],
        [InlineKeyboardButton(text="Хард 20000-100000",   callback_data=p+"ultra")],
        [InlineKeyboardButton(text="Экстрим от 100000",   callback_data=p+"extreme")],
        [InlineKeyboardButton(text="Назад",               callback_data="search_mode_select")],
    ])

def who_kb(mode, cat):
    # mode: market / profile  cat: all/cheap/mid/hard/ultra/extreme
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Всех",    callback_data="run_" + mode + "_" + cat + "_all")],
        [InlineKeyboardButton(text="Девушек", callback_data="run_" + mode + "_" + cat + "_girls")],
        [InlineKeyboardButton(text="Назад",   callback_data=("mode_market" if mode=="market" else "mode_profile"))],
    ])

def settings_menu_kb(uid):
    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    bst = get_boost(uid)
    lim = get_limit(uid)
    reg = get_region(uid)
    mx_str  = str(mx) if mx > 0 else "без лимита"
    reg_lbl = REGIONS.get(reg, {}).get("label", "Все страны")
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мин. гифтов: " + str(mn),      callback_data="set_min")],
        [InlineKeyboardButton(text="Макс. гифтов: " + mx_str,      callback_data="set_max")],
        [InlineKeyboardButton(text="Буст: " + str(bst) + "%",      callback_data="set_boost")],
        [InlineKeyboardButton(text="Лимит выдачи: " + str(lim),    callback_data="set_limit")],
        [InlineKeyboardButton(text="Регион: " + reg_lbl,           callback_data="set_region")],
        [InlineKeyboardButton(text="Назад",                         callback_data="menu")],
    ])

def boost_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="30%",  callback_data="bst_30"),
         InlineKeyboardButton(text="50%",  callback_data="bst_50"),
         InlineKeyboardButton(text="100%", callback_data="bst_100")],
        [InlineKeyboardButton(text="150%", callback_data="bst_150"),
         InlineKeyboardButton(text="200%", callback_data="bst_200"),
         InlineKeyboardButton(text="300%", callback_data="bst_300")],
        [InlineKeyboardButton(text="Ввести вручную", callback_data="bst_custom")],
        [InlineKeyboardButton(text="Назад", callback_data="settings_menu")],
    ])

def limit_kb(current=30):
    def l(v): return str(v) + (" ✓" if v == current else "")
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=l(10), callback_data="lim_10"),
         InlineKeyboardButton(text=l(20), callback_data="lim_20"),
         InlineKeyboardButton(text=l(30), callback_data="lim_30"),
         InlineKeyboardButton(text=l(50), callback_data="lim_50")],
        [InlineKeyboardButton(text=l(70),  callback_data="lim_70"),
         InlineKeyboardButton(text=l(100), callback_data="lim_100")],
        [InlineKeyboardButton(text="Назад", callback_data="settings_menu")],
    ])

def region_kb(current="any"):
    rows = []
    items = list(REGIONS.items())
    for i in range(0, len(items), 2):
        row = []
        for key, val in items[i:i+2]:
            lbl = val["label"] + (" ✓" if key == current else "")
            row.append(InlineKeyboardButton(text=lbl, callback_data="reg_" + key))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="Назад", callback_data="settings_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="СТОП", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Поиск", callback_data="search_mode_select")],
        [InlineKeyboardButton(text="Меню",  callback_data="menu")],
    ])

def owner_card_kb(username, profile_url, owner_uid):
    btns = []
    if username:
        btns.append([InlineKeyboardButton(text="@" + username, url="https://t.me/" + username)])
        msg = urllib.parse.quote("Привет хочу купить твои NFT")
        btns.append([InlineKeyboardButton(text="Написать", url="https://t.me/" + username + "?text=" + msg)])
    elif profile_url:
        btns.append([InlineKeyboardButton(text="Профиль", url=profile_url)])
    btns.append([InlineKeyboardButton(text="Все NFT", callback_data="shownft_" + str(owner_uid))])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def nft_list_kb(items, username, profile_url):
    btns = []
    for g in items:
        url = g.get("nft_url")
        if url:
            lbl = str(g.get("title","?")) + " #" + str(g.get("num","?"))
            btns.append([InlineKeyboardButton(text=lbl, url=url)])
    if username:
        btns.append([InlineKeyboardButton(text="@" + username, url="https://t.me/" + username)])
        msg = urllib.parse.quote("Привет хочу купить твои NFT")
        btns.append([InlineKeyboardButton(text="Написать", url="https://t.me/" + username + "?text=" + msg)])
    elif profile_url:
        btns.append([InlineKeyboardButton(text="Профиль", url=profile_url)])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="admin_cancel")],
    ])

def confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отправить", callback_data="admin_broadcast_confirm")],
        [InlineKeyboardButton(text="Отмена",    callback_data="admin_cancel")],
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Рассылка",           callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Пользователи",       callback_data="admin_users")],
        [InlineKeyboardButton(text="Статистика",         callback_data="admin_stats")],
        [InlineKeyboardButton(text="Авторизация TG",     callback_data="admin_auth")],
        [InlineKeyboardButton(text="Обновить коллекции", callback_data="admin_reload_cols")],
        [InlineKeyboardButton(text="Выйти из TG",        callback_data="admin_logout")],
        [InlineKeyboardButton(text="В меню",             callback_data="menu")],
    ])

def input_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="settings_menu")],
    ])


# ===================== COLLECTIONS =====================
async def load_collections():
    global ALL_GIFT_IDS, NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        ALL_GIFT_IDS    = []
        NFT_COLLECTIONS = {}
        seen = set()
        for gift in (getattr(result, "gifts", None) or []):
            gid   = getattr(gift, "id", None)
            title = getattr(gift, "title", None)
            if gid is None or gid in seen:
                continue
            if not title:
                continue  # берём только именованные NFT коллекции
            seen.add(gid)
            ALL_GIFT_IDS.append((gid, title))
            NFT_COLLECTIONS[title] = gid
        logger.info("Коллекций загружено: %d", len(ALL_GIFT_IDS))
    except Exception as e:
        logger.error("load_collections: %s", e)

async def ensure_collections():
    if not ALL_GIFT_IDS:
        await load_collections()
    return [gid for gid, _ in ALL_GIFT_IDS]


# ===================== API CALLS =====================
async def get_floor(gid):
    if gid in PRICE_FLOOR_CACHE:
        return PRICE_FLOOR_CACHE[gid]
    try:
        result = await tg_client(GetResaleStarGiftsRequest(gift_id=gid, offset="", limit=20))
        prices = []
        for g in (getattr(result, "gifts", None) or []):
            p = get_resell_price(g)
            if p and p > 0:
                prices.append(p)
        if not prices:
            return None
        prices.sort()
        floor = prices[max(0, len(prices) // 4)]
        PRICE_FLOOR_CACHE[gid] = floor
        return floor
    except Exception:
        return None

async def fetch_market_page(gid, offset, limit=100):
    for attempt in range(2):
        try:
            result    = await tg_client(GetResaleStarGiftsRequest(gift_id=gid, offset=offset, limit=limit))
            users_map = {int(u.id): u for u in (getattr(result, "users", None) or [])}
            items     = []
            for gift in (getattr(result, "gifts", None) or []):
                owner, oid = get_owner(gift, users_map)
                username   = getattr(owner, "username", None) if owner else None
                fn = (getattr(owner, "first_name", "") or "") if owner else ""
                ln = (getattr(owner, "last_name",  "") or "") if owner else ""
                name = (fn + " " + ln).strip()
                nft_url = make_nft_url(gift)
                profile_url = ("https://t.me/" + username) if username else (("tg://user?id=" + str(oid)) if oid else None)
                items.append({
                    "owner": owner, "owner_id": oid,
                    "username": username, "name": name,
                    "title": getattr(gift, "title", "?"),
                    "num":   getattr(gift, "num",   "?"),
                    "price": get_resell_price(gift),
                    "nft_url": nft_url,
                    "profile_url": profile_url,
                })
            return items, getattr(result, "next_offset", "") or ""
        except FloodWaitError as e:
            await asyncio.sleep(max(e.seconds, 2))
        except Exception as e:
            logger.error("fetch_market gid=%s: %s", gid, e)
            return [], ""
    return [], ""

async def fetch_saved_gifts(uid, max_pages=10):
    all_items = []
    offset    = ""
    for _ in range(max_pages):
        try:
            result = await tg_client(GetSavedStarGiftsRequest(
                peer=await tg_client.get_input_entity(uid),
                offset=offset, limit=100,
            ))
            for gift in (getattr(result, "gifts", None) or []):
                nft_url = make_nft_url(gift)
                if not nft_url:
                    inner = getattr(gift, "gift", None)
                    if inner:
                        nft_url = make_nft_url(inner)
                inner = getattr(gift, "gift", None)
                title = (getattr(inner, "title", None) or getattr(gift, "title", "?"))
                num   = getattr(gift, "num", "?")
                on_market = bool(getattr(gift, "resell_amount", None))
                all_items.append({"title": title, "num": num, "nft_url": nft_url, "on_market": on_market})
            offset = getattr(result, "next_offset", "") or ""
            if not offset:
                break
        except Exception as e:
            logger.error("saved_gifts uid=%s: %s", uid, e)
            break
    return all_items


# ===================== SEARCH: MARKET =====================
# Ищет NFT выставленные на маркет. Группирует по владельцу (min_gifts, max_gifts).
# Для каждого владельца показывает карточку с кнопками.
async def do_market_search(status_msg, gift_ids, cat=None, girls_only=False,
                           boost=100, min_gifts=2, max_gifts=0,
                           max_results=30, region="any"):
    global is_searching
    is_searching = True
    found        = 0       # кол-во отправленных карточек владельцев
    total_nft    = 0       # кол-во NFT
    seen_slugs   = set()
    # uid -> {owner, username, name, profile_url, items:[]}
    owner_map    = {}
    sent_owners  = set()

    async def flush_owners(force=False):
        nonlocal found
        for uid in list(owner_map.keys()):
            if uid in sent_owners:
                continue
            bucket = owner_map[uid]
            cnt    = len(bucket["items"])
            if force or gifts_in_range(cnt, min_gifts, max_gifts):
                sent_owners.add(uid)
                owner_map.pop(uid, None)
                cache_owner(uid, bucket["owner"], bucket["username"],
                            bucket["name"], bucket["profile_url"], bucket["items"])
                kb       = owner_card_kb(bucket["username"], bucket["profile_url"], uid)
                owner_s  = fmt_owner(bucket["owner"], bucket["username"], bucket["name"])
                # список NFT
                nft_lines = ""
                for it in bucket["items"][:5]:
                    nu = it.get("nft_url")
                    t  = esc(str(it.get("title","?")))
                    n  = esc(str(it.get("num","?")))
                    p  = it.get("price")
                    price_s = " " + str(p) + " zv" if p else ""
                    if nu:
                        nft_lines += '\n<a href="' + nu + '"><b>' + t + " #" + n + price_s + "</b></a>"
                    else:
                        nft_lines += "\n<b>" + t + " #" + n + price_s + "</b>"
                if len(bucket["items"]) > 5:
                    nft_lines += "\n<b>... ещё " + str(len(bucket["items"]) - 5) + "</b>"
                txt = "<b>" + owner_s + "\nNFT на маркете: " + str(cnt) + "</b>" + nft_lines
                try:
                    await status_msg.bot.send_message(
                        chat_id=status_msg.chat.id, text=txt,
                        parse_mode="HTML", reply_markup=kb,
                        disable_web_page_preview=True,
                    )
                    found += 1
                    stats["found"] += 1
                except Exception as e:
                    logger.warning("flush owner: %s", e)
                if found >= max_results:
                    break

    try:
        await status_msg.edit_text("<b>Загружаю данные коллекций...</b>", reply_markup=stop_kb())

        # Фильтруем коллекции по категории (загружаем флоры параллельно)
        if cat and cat != "all":
            floors = await asyncio.gather(*[get_floor(gid) for gid in gift_ids])
            valid_ids = [gid for gid, fl in zip(gift_ids, floors)
                         if fl is not None and floor_in_cat(fl, cat)]
        else:
            valid_ids = list(gift_ids)
            floors    = [None] * len(valid_ids)

        floor_map = {}
        if cat and cat != "all":
            floor_map = {gid: fl for gid, fl in zip(gift_ids, floors) if fl is not None}

        if not valid_ids:
            is_searching = False
            return 0

        random.shuffle(valid_ids)
        last_upd = 0.0

        # Параллельный скан коллекций батчами по 8
        BATCH = 8
        for i in range(0, len(valid_ids), BATCH):
            if not is_searching or found >= max_results:
                break
            batch = valid_ids[i:i+BATCH]
            pages = await asyncio.gather(*[fetch_market_page(gid, "", limit=100) for gid in batch])

            for gid, (items, _) in zip(batch, pages):
                if not is_searching:
                    break
                fl = floor_map.get(gid)
                for item in items:
                    nft_url = item.get("nft_url") or ""
                    slug    = nft_url.split("/")[-1] if nft_url else ""
                    if slug and slug in seen_slugs:
                        continue
                    if slug:
                        seen_slugs.add(slug)

                    price = item.get("price")
                    oid   = item["owner_id"]
                    if not oid:
                        continue

                    # Фильтр по цене (только если категория задана)
                    if cat and cat != "all" and fl and price:
                        if not price_ok(price, fl, boost):
                            continue

                    # Фильтр по региону
                    if not region_match(item["owner"], item["username"], item["name"], region):
                        continue

                    # Фильтр по полу
                    if girls_only and not is_girl(item["owner"], item["username"], item["name"]):
                        continue

                    if oid in sent_owners:
                        continue

                    if oid not in owner_map:
                        owner_map[oid] = {
                            "owner": item["owner"], "username": item["username"],
                            "name": item["name"], "profile_url": item["profile_url"],
                            "items": [],
                        }
                    owner_map[oid]["items"].append(item)

            # Отправляем готовых
            await flush_owners(force=False)
            if found >= max_results:
                break

            now = asyncio.get_event_loop().time()
            if now - last_upd > 2:
                try:
                    lbl = "девушек" if girls_only else "владельцев"
                    await status_msg.edit_text(
                        "<b>Проверено коллекций: " + str(min(i + BATCH, len(valid_ids))) +
                        " из " + str(len(valid_ids)) +
                        "\nНайдено " + lbl + ": " + str(found) + " из " + str(max_results) + "</b>",
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                    last_upd = now
                except Exception:
                    pass

        # Финальный flush с force=True для тех у кого меньше min_gifts но хоть что-то есть
        await flush_owners(force=False)

    except Exception as e:
        logger.error("do_market_search: %s", e)
    finally:
        is_searching = False
    return found


# ===================== SEARCH: PROFILE =====================
# Ищет владельцев у которых NFT есть в профиле, но НЕ выставлены на маркет.
async def do_profile_search(status_msg, gift_ids, cat=None, girls_only=False,
                            min_gifts=2, max_gifts=0,
                            max_results=30, region="any", boost=100):
    global is_searching
    is_searching = True
    found        = 0
    # uid -> {owner, username, name, profile_url}
    owners_index  = {}
    # uid -> count на маркете
    owner_on_mkt  = {}
    seen_sent     = set()
    last_upd      = 0.0

    try:
        await status_msg.edit_text("<b>Сканирую маркет...</b>", reply_markup=stop_kb())

        # 1. Собираем всех владельцев с маркета
        random.shuffle(gift_ids)
        BATCH = 8
        for i in range(0, len(gift_ids), BATCH):
            if not is_searching:
                break
            batch = gift_ids[i:i+BATCH]
            pages = await asyncio.gather(*[fetch_market_page(gid, "", limit=100) for gid in batch])
            for gid, (items, _) in zip(batch, pages):
                for item in items:
                    oid = item["owner_id"]
                    if not oid:
                        continue
                    owner_on_mkt[oid] = owner_on_mkt.get(oid, 0) + 1
                    if oid not in owners_index:
                        owners_index[oid] = {
                            "owner": item["owner"], "username": item["username"],
                            "name": item["name"], "profile_url": item["profile_url"],
                        }

        total = len(owners_index)
        await status_msg.edit_text(
            "<b>Найдено владельцев: " + str(total) + "\nПроверяю профили...</b>",
            parse_mode="HTML", reply_markup=stop_kb()
        )

        # 2. Для каждого проверяем saved_gifts
        owner_list = list(owners_index.items())
        random.shuffle(owner_list)
        checked = 0

        for i in range(0, len(owner_list), 5):
            if not is_searching or found >= max_results:
                break
            batch = owner_list[i:i+5]

            async def check_one(uid, info):
                nonlocal found
                if not is_searching or found >= max_results:
                    return
                if uid in seen_sent:
                    return

                owner_obj   = info["owner"]
                username    = info["username"]
                name        = info["name"]
                profile_url = info["profile_url"] or (("https://t.me/" + username) if username else ("tg://user?id=" + str(uid)))

                if not region_match(owner_obj, username, name, region):
                    return
                if girls_only and not is_girl(owner_obj, username, name):
                    return

                saved = await fetch_saved_gifts(uid, max_pages=5)
                # Берём только NFT которые НЕ на маркете
                hidden = [g for g in saved if g.get("nft_url") and not g.get("on_market")]

                if not gifts_in_range(len(hidden), min_gifts, max_gifts):
                    return
                if uid in seen_sent:
                    return

                seen_sent.add(uid)
                cache_owner(uid, owner_obj, username, name, profile_url, hidden)
                kb  = owner_card_kb(username, profile_url, uid)

                owner_s   = fmt_owner(owner_obj, username, name)
                nft_lines = ""
                for g in hidden[:5]:
                    nu = g.get("nft_url")
                    t  = esc(str(g.get("title","?")))
                    n  = esc(str(g.get("num","?")))
                    if nu:
                        nft_lines += '\n<a href="' + nu + '"><b>' + t + " #" + n + "</b></a>"
                    else:
                        nft_lines += "\n<b>" + t + " #" + n + "</b>"
                if len(hidden) > 5:
                    nft_lines += "\n<b>... ещё " + str(len(hidden) - 5) + "</b>"

                txt = (
                    "<b>" + owner_s + "</b>\n"
                    "<b>Скрытых NFT: " + str(len(hidden)) + "  На маркете: " + str(owner_on_mkt.get(uid,0)) + "</b>"
                    + nft_lines
                )
                try:
                    await status_msg.bot.send_message(
                        chat_id=status_msg.chat.id, text=txt,
                        parse_mode="HTML", reply_markup=kb,
                        disable_web_page_preview=True,
                    )
                    found += 1
                    stats["found"] += 1
                except Exception as e:
                    logger.warning("profile send: %s", e)

            await asyncio.gather(*[check_one(uid, info) for uid, info in batch])
            checked += len(batch)

            now = asyncio.get_event_loop().time()
            if now - last_upd > 2:
                try:
                    lbl = "девушек" if girls_only else "профилей"
                    await status_msg.edit_text(
                        "<b>Проверено: " + str(checked) + " из " + str(total) +
                        "\nНайдено " + lbl + ": " + str(found) + " из " + str(max_results) + "</b>",
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                    last_upd = now
                except Exception:
                    pass

    except Exception as e:
        logger.error("do_profile_search: %s", e)
    finally:
        is_searching = False
    return found


# ===================== RUN HELPERS =====================
async def run_market(cb, cat=None, girls=False, ids=None):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
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
    lim    = get_limit(uid)
    reg    = get_region(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    cat_l  = CAT_LABELS.get(cat or "all", "Все")
    who_l  = "Девушки" if girls else "Все"
    reg_l  = REGIONS.get(reg, {}).get("label", "Все страны")
    txt = (
        "<b>Маркет / " + cat_l + " / " + who_l + "\n"
        "Регион: " + reg_l + "\n"
        "Гифтов: от " + str(mn) + " до " + mx_str + "  Буст: " + str(boost) + "%\n"
        "Лимит: " + str(lim) + "</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_market_search(status, ids, cat=cat, girls_only=girls,
                                   boost=boost, min_gifts=mn, max_gifts=mx,
                                   max_results=lim, region=reg)
    try:
        await status.edit_text(
            "<b>Готово. Маркет / " + cat_l + " / " + who_l + "\nНайдено: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass

async def run_profile(cb, cat=None, girls=False, ids=None):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
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
    lim    = get_limit(uid)
    reg    = get_region(uid)
    mx_str = str(mx) if mx > 0 else "без лимита"
    cat_l  = CAT_LABELS.get(cat or "all", "Все")
    who_l  = "Девушки" if girls else "Все"
    reg_l  = REGIONS.get(reg, {}).get("label", "Все страны")
    txt = (
        "<b>Профиль / " + cat_l + " / " + who_l + "\n"
        "Регион: " + reg_l + "\n"
        "Скрытых гифтов: от " + str(mn) + " до " + mx_str + "\n"
        "Лимит: " + str(lim) + "</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_profile_search(status, ids, cat=cat, girls_only=girls,
                                    min_gifts=mn, max_gifts=mx,
                                    max_results=lim, region=reg, boost=boost)
    try:
        await status.edit_text(
            "<b>Готово. Профиль / " + cat_l + " / " + who_l + "\nНайдено: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


# ===================== ONBOARDING =====================
WELCOME_TXT = "<b>Neptun Parser\n\nВыбери действие:</b>"

@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    uid    = message.from_user.id
    is_new = add_user(uid, message.from_user.username,
                      message.from_user.first_name, message.from_user.last_name)

    if not await check_authorized() and is_admin(uid):
        await message.answer("<b>Нужна авторизация Telegram\nВведи номер телефона:</b>", parse_mode="HTML")
        await state.set_state(Auth.phone)
        return

    if uid not in ONBOARDING_DONE:
        await message.answer(
            "<b>Добро пожаловать в Neptun Parser\n\n"
            "Шаг 1 из 4\n"
            "Минимум гифтов у владельца.\n"
            "Введи число (например 2):</b>",
            parse_mode="HTML"
        )
        await state.set_state(Onboarding.min_gifts)
        return

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
        reply_markup=main_menu_kb()
    )

@dp.message(Onboarding.min_gifts)
async def ob_min(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit() or int(message.text.strip()) < 1:
        await message.answer("<b>Введи число от 1:</b>", parse_mode="HTML")
        return
    val = int(message.text.strip())
    USER_MIN_GIFTS[message.from_user.id] = val
    await message.answer(
        "<b>Мин. гифтов: " + str(val) + "\n\n"
        "Шаг 2 из 4\n"
        "Максимум гифтов (0 = без лимита).\n"
        "Введи число:</b>",
        parse_mode="HTML"
    )
    await state.set_state(Onboarding.max_gifts)

@dp.message(Onboarding.max_gifts)
async def ob_max(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().lstrip("-").isdigit():
        await message.answer("<b>Введи число (0 = без лимита):</b>", parse_mode="HTML")
        return
    val = max(0, int(message.text.strip()))
    USER_MAX_GIFTS[message.from_user.id] = val
    lbl = "без лимита" if val == 0 else str(val)
    await message.answer(
        "<b>Макс. гифтов: " + lbl + "\n\n"
        "Шаг 3 из 4\n"
        "Буст цен для маркета.\n"
        "100 = цена до x2 флора. Введи число %:</b>",
        parse_mode="HTML"
    )
    await state.set_state(Onboarding.boost)

@dp.message(Onboarding.boost)
async def ob_boost(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await message.answer("<b>Введи число от 1:</b>", parse_mode="HTML")
        return
    val = max(1, int(message.text.strip()))
    USER_BOOST[message.from_user.id] = val
    await message.answer(
        "<b>Буст: " + str(val) + "%\n\n"
        "Шаг 4 из 4\n"
        "Лимит выдачи результатов за один поиск.\n"
        "Введи число от 5 до 100:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="10", callback_data="ob_lim_10"),
             InlineKeyboardButton(text="20", callback_data="ob_lim_20"),
             InlineKeyboardButton(text="30", callback_data="ob_lim_30")],
            [InlineKeyboardButton(text="50", callback_data="ob_lim_50"),
             InlineKeyboardButton(text="70", callback_data="ob_lim_70")],
        ])
    )
    await state.set_state(Onboarding.limit)

@dp.callback_query(F.data.startswith("ob_lim_"))
async def ob_lim_btn(cb: CallbackQuery, state: FSMContext):
    if await state.get_state() != Onboarding.limit.state:
        await cb.answer()
        return
    val = int(cb.data[7:])
    await _finish_onboarding(cb.from_user.id, val, state)
    await cb.message.edit_reply_markup(reply_markup=None)
    mn  = get_min_gifts(cb.from_user.id)
    mx  = get_max_gifts(cb.from_user.id)
    bst = get_boost(cb.from_user.id)
    mx_str = str(mx) if mx > 0 else "без лимита"
    await cb.message.answer(
        "<b>Настройка завершена\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Буст: " + str(bst) + "%\n"
        "Лимит выдачи: " + str(val) + "\n\n"
        "Менять можно в Настройках</b>",
        parse_mode="HTML"
    )
    await cb.message.answer(WELCOME_TXT, parse_mode="HTML", reply_markup=main_menu_kb())
    await cb.answer()

@dp.message(Onboarding.limit)
async def ob_lim_text(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await message.answer("<b>Введи число от 5 до 100:</b>", parse_mode="HTML")
        return
    val = max(5, min(100, int(message.text.strip())))
    await _finish_onboarding(message.from_user.id, val, state)
    mn  = get_min_gifts(message.from_user.id)
    mx  = get_max_gifts(message.from_user.id)
    bst = get_boost(message.from_user.id)
    mx_str = str(mx) if mx > 0 else "без лимита"
    await message.answer(
        "<b>Настройка завершена\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_str + "\n"
        "Буст: " + str(bst) + "%\n"
        "Лимит выдачи: " + str(val) + "\n\n"
        "Менять можно в Настройках</b>",
        parse_mode="HTML"
    )
    await message.answer(WELCOME_TXT, parse_mode="HTML", reply_markup=main_menu_kb())

async def _finish_onboarding(uid, limit_val, state):
    USER_LIMIT[uid] = limit_val
    ONBOARDING_DONE.add(uid)
    save_onboarding()
    await state.clear()


# ===================== COMMANDS =====================
@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    if message.from_user.id not in ONBOARDING_DONE:
        await message.answer("<b>Сначала пройди настройку. Напиши /start</b>", parse_mode="HTML")
        return
    await message.answer("<b>Отменено.</b>", parse_mode="HTML", reply_markup=main_menu_kb())

@dp.message(Command("clear"))
async def cmd_clear(message: Message):
    global is_searching
    is_searching = False
    await message.answer("<b>Поиск остановлен.</b>", parse_mode="HTML", reply_markup=menu_kb())

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer("<b>ID: <code>" + str(message.from_user.id) + "</code></b>", parse_mode="HTML")

@dp.message(Command("neptunteam"))
async def cmd_neptunteam(message: Message):
    uid  = message.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    bst  = get_boost(uid)
    lim  = get_limit(uid)
    reg  = get_region(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    reg_l= REGIONS.get(reg, {}).get("label", "Все страны")
    txt = (
        "<b>Neptun Parser — справка\n\n"
        "РЕЖИМЫ ПОИСКА\n\n"
        "По маркету\n"
        "Ищет NFT выставленные на продажу.\n"
        "Фильтрует по цене относительно флора коллекции.\n"
        "Флор = минимальная цена (нижние 25%).\n\n"
        "По профилю\n"
        "Ищет владельцев у которых NFT есть в профиле но не выставлены на маркет.\n\n"
        "НАСТРОЙКИ\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Минимум NFT у владельца чтобы попасть в результаты.\n\n"
        "Макс. гифтов: " + mx_s + "\n"
        "Максимум NFT. 0 = без лимита.\n\n"
        "Буст цен: " + str(bst) + "%\n"
        "Только для маркета. 100% = до x2 флора.\n"
        "30% = до x1.3  100% = до x2  200% = до x3\n\n"
        "Лимит выдачи: " + str(lim) + "\n"
        "Сколько результатов показывает один поиск.\n\n"
        "Регион: " + reg_l + "\n"
        "Фильтрует по языку/стране профиля.\n\n"
        "КОМАНДЫ\n"
        "/start — главное меню\n"
        "/clear — остановить поиск\n"
        "/neptunteam — эта справка</b>"
    )
    await message.answer(txt, parse_mode="HTML", reply_markup=main_menu_kb())

@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("<b>Нет доступа. ID: <code>" + str(message.from_user.id) + "</code></b>", parse_mode="HTML")
        return
    await state.clear()
    users = load_users()
    ok    = await check_authorized()
    await message.answer(
        "<b>Админ панель\n\n"
        "Telethon: " + ("авторизован" if ok else "не авторизован") + "\n"
        "Коллекций: " + str(len(ALL_GIFT_IDS)) + "\n"
        "Пользователей: " + str(len(users)) + "\n"
        "Поисков: " + str(stats["checks"]) + "  Найдено: " + str(stats["found"]) + "</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )


# ===================== CALLBACKS: NAVIGATION =====================
@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    uid  = cb.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    bst  = get_boost(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await cb.message.answer(
        "<b>Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n"
        "Буст: " + str(bst) + "%\n\n"
        "Выбери действие:</b>",
        parse_mode="HTML", reply_markup=main_menu_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "search_mode_select")
async def cb_search_mode(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Выбери режим поиска:</b>",
        parse_mode="HTML", reply_markup=search_mode_select_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Маркет — выбери ценовую категорию:</b>",
        parse_mode="HTML", reply_markup=cat_kb("market")
    )
    await cb.answer()

@dp.callback_query(F.data == "mode_profile")
async def cb_mode_profile(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Профиль — выбери ценовую категорию:</b>",
        parse_mode="HTML", reply_markup=cat_kb("profile")
    )
    await cb.answer()

# mc_<cat> — маркет выбрал категорию
@dp.callback_query(F.data.startswith("mc_"))
async def cb_mc(cb: CallbackQuery):
    cat = cb.data[3:]
    lbl = CAT_LABELS.get(cat, cat)
    await cb.message.answer(
        "<b>Маркет / " + lbl + "\nКого искать?</b>",
        parse_mode="HTML", reply_markup=who_kb("market", cat)
    )
    await cb.answer()

# pc_<cat> — профиль выбрал категорию
@dp.callback_query(F.data.startswith("pc_"))
async def cb_pc(cb: CallbackQuery):
    cat = cb.data[3:]
    lbl = CAT_LABELS.get(cat, cat)
    await cb.message.answer(
        "<b>Профиль / " + lbl + "\nКого искать?</b>",
        parse_mode="HTML", reply_markup=who_kb("profile", cat)
    )
    await cb.answer()

# run_<mode>_<cat>_<who>
@dp.callback_query(F.data.startswith("run_"))
async def cb_run(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    parts = cb.data.split("_")   # ["run", mode, cat, who]
    if len(parts) < 4:
        await cb.answer()
        return
    mode  = parts[1]
    cat   = parts[2]
    who   = parts[3]
    girls = (who == "girls")
    real_cat = None if cat == "all" else cat
    await cb.answer()
    if mode == "market":
        await run_market(cb, cat=real_cat, girls=girls)
    else:
        await run_profile(cb, cat=real_cat, girls=girls)

@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    is_searching = False
    await cb.answer("Останавливаю...")
    try:
        await cb.message.edit_text("<b>Поиск остановлен.</b>", parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        pass

@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    uid  = cb.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    bst  = get_boost(uid)
    lim  = get_limit(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    reg  = REGIONS.get(get_region(uid), {}).get("label", "Все страны")
    await cb.message.answer(
        "<b>Статистика\n\n"
        "Поисков: " + str(stats["checks"]) + "\n"
        "Найдено: " + str(stats["found"]) + "\n"
        "Пользователей: " + str(get_user_count()) + "\n\n"
        "Настройки:\n"
        "Мин: " + str(mn) + "  Макс: " + mx_s + "\n"
        "Буст: " + str(bst) + "%  Лимит: " + str(lim) + "\n"
        "Регион: " + reg + "</b>",
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("shownft_"))
async def cb_show_nft(cb: CallbackQuery):
    uid    = int(cb.data[8:])
    cached = NFT_CACHE.get(uid)
    if not cached:
        await cb.answer("Загружаю NFT...", show_alert=False)
        saved = await fetch_saved_gifts(uid)
        nfts  = [g for g in saved if g.get("nft_url")]
        if not nfts:
            await cb.answer("NFT не найдены или профиль закрыт", show_alert=True)
            return
        NFT_CACHE[uid] = {"owner": None, "username": None, "name": None,
                          "profile_url": "tg://user?id=" + str(uid), "items": nfts}
        cached = NFT_CACHE[uid]
    else:
        await cb.answer()
    items    = cached.get("items", [])
    username = cached.get("username")
    p_url    = cached.get("profile_url")
    owner_s  = fmt_owner(cached.get("owner"), username, cached.get("name"))
    if not items:
        await cb.answer("Список пуст", show_alert=True)
        return
    kb  = nft_list_kb(items, username, p_url)
    txt = "<b>NFT " + owner_s + "\nВсего: " + str(len(items)) + "</b>"
    await cb.message.answer(txt, parse_mode="HTML", reply_markup=kb)


# ===================== CALLBACKS: SETTINGS =====================
@dp.callback_query(F.data == "settings_menu")
async def cb_settings(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    uid  = cb.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    bst  = get_boost(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await cb.message.answer(
        "<b>Настройки поиска\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n"
        "Буст цен: " + str(bst) + "%</b>",
        parse_mode="HTML", reply_markup=settings_menu_kb(uid)
    )
    await cb.answer()

@dp.callback_query(F.data == "set_min")
async def cb_set_min(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "<b>Введи минимум гифтов у владельца (число от 1):</b>",
        parse_mode="HTML", reply_markup=input_cancel_kb()
    )
    await state.set_state(SetMin.value)
    await cb.answer()

@dp.message(SetMin.value)
async def set_min_txt(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit() or int(message.text.strip()) < 1:
        await message.answer("<b>Введи число от 1:</b>", parse_mode="HTML")
        return
    val = int(message.text.strip())
    USER_MIN_GIFTS[message.from_user.id] = val
    await state.clear()
    await message.answer("<b>Мин. гифтов: " + str(val) + "</b>", parse_mode="HTML",
                         reply_markup=settings_menu_kb(message.from_user.id))

@dp.callback_query(F.data == "set_max")
async def cb_set_max(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "<b>Введи максимум гифтов (0 = без лимита):</b>",
        parse_mode="HTML", reply_markup=input_cancel_kb()
    )
    await state.set_state(SetMax.value)
    await cb.answer()

@dp.message(SetMax.value)
async def set_max_txt(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().lstrip("-").isdigit():
        await message.answer("<b>Введи число (0 = без лимита):</b>", parse_mode="HTML")
        return
    val = max(0, int(message.text.strip()))
    USER_MAX_GIFTS[message.from_user.id] = val
    lbl = "без лимита" if val == 0 else str(val)
    await state.clear()
    await message.answer("<b>Макс. гифтов: " + lbl + "</b>", parse_mode="HTML",
                         reply_markup=settings_menu_kb(message.from_user.id))

@dp.callback_query(F.data == "set_boost")
async def cb_set_boost(cb: CallbackQuery):
    await cb.message.answer(
        "<b>Буст цен\n100% = до x2 флора  200% = до x3</b>",
        parse_mode="HTML", reply_markup=boost_kb()
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("bst_"))
async def cb_bst(cb: CallbackQuery, state: FSMContext):
    raw = cb.data[4:]
    if raw == "custom":
        await cb.message.answer(
            "<b>Введи буст вручную (число %):</b>",
            parse_mode="HTML", reply_markup=input_cancel_kb()
        )
        await state.set_state(SetBoost.value)
        await cb.answer()
        return
    val = int(raw)
    USER_BOOST[cb.from_user.id] = val
    await cb.answer("Буст: " + str(val) + "%", show_alert=True)
    await cb.message.edit_reply_markup(reply_markup=None)

@dp.message(SetBoost.value)
async def set_boost_txt(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await message.answer("<b>Введи число:</b>", parse_mode="HTML")
        return
    val = max(1, int(message.text.strip()))
    USER_BOOST[message.from_user.id] = val
    await state.clear()
    await message.answer("<b>Буст: " + str(val) + "%</b>", parse_mode="HTML",
                         reply_markup=settings_menu_kb(message.from_user.id))

@dp.callback_query(F.data == "set_limit")
async def cb_set_limit(cb: CallbackQuery):
    lim = get_limit(cb.from_user.id)
    await cb.message.answer(
        "<b>Лимит выдачи результатов:</b>",
        parse_mode="HTML", reply_markup=limit_kb(lim)
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("lim_"))
async def cb_lim(cb: CallbackQuery):
    val = int(cb.data[4:])
    USER_LIMIT[cb.from_user.id] = val
    await cb.answer("Лимит: " + str(val), show_alert=False)
    await cb.message.edit_reply_markup(reply_markup=limit_kb(val))

@dp.callback_query(F.data == "set_region")
async def cb_set_region(cb: CallbackQuery):
    reg = get_region(cb.from_user.id)
    await cb.message.answer(
        "<b>Выбери регион поиска:</b>",
        parse_mode="HTML", reply_markup=region_kb(reg)
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("reg_"))
async def cb_reg(cb: CallbackQuery):
    key = cb.data[4:]
    if key not in REGIONS:
        await cb.answer("Неизвестный регион", show_alert=True)
        return
    USER_REGION[cb.from_user.id] = key
    lbl = REGIONS[key]["label"]
    await cb.answer("Регион: " + lbl, show_alert=False)
    await cb.message.edit_reply_markup(reply_markup=region_kb(key))


# ===================== CALLBACKS: ADMIN =====================
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

async def show_users_page(msg, page, edit):
    users     = load_users()
    all_items = list(users.items())
    total     = len(all_items)
    PAGE      = 20
    if total == 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Назад", callback_data="admin_panel")]])
        fn = msg.edit_text if edit else msg.answer
        await fn("<b>Пользователей нет.</b>", parse_mode="HTML", reply_markup=kb)
        return
    start = page * PAGE
    end   = min(start + PAGE, total)
    chunk = all_items[start:end]
    lines = ["<b>Пользователи " + str(start+1) + " - " + str(end) + " из " + str(total) + "</b>\n"]
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
            card += " @" + esc(uname)
        if name:
            card += " " + esc(name)
        card += "\n" + fmt_ts(joined) + "</b>"
        lines.append(card)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="Назад", callback_data="users_page_" + str(page-1)))
    if end < total:
        nav.append(InlineKeyboardButton(text="Вперед", callback_data="users_page_" + str(page+1)))
    rows = [nav] if nav else []
    rows.append([InlineKeyboardButton(text="Админ", callback_data="admin_panel")])
    fn = msg.edit_text if edit else msg.answer
    await fn("\n".join(lines), parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.callback_query(F.data == "admin_panel")
async def cb_admin_panel(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    users = load_users()
    ok    = await check_authorized()
    await cb.message.answer(
        "<b>Админ панель\n\n"
        "Telethon: " + ("авторизован" if ok else "не авторизован") + "\n"
        "Коллекций: " + str(len(ALL_GIFT_IDS)) + "\n"
        "Пользователей: " + str(len(users)) + "</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "admin_reload_cols")
async def cb_reload_cols(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    await cb.answer("Обновляю...")
    await load_collections()
    await cb.message.answer(
        "<b>Коллекции обновлены: " + str(len(ALL_GIFT_IDS)) + " шт.</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await cb.message.answer("<b>Отправь сообщение для рассылки. /cancel - отмена</b>",
                            parse_mode="HTML", reply_markup=cancel_kb())
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
    status = await cb.message.answer("<b>Рассылка " + str(len(uids)) + " пользователям...</b>", parse_mode="HTML")
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
                await status.edit_text("<b>" + str(i+1) + " из " + str(len(uids)) + "...</b>", parse_mode="HTML")
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await status.edit_text(
        "<b>Отправлено: " + str(ok) + "\nОшибок: " + str(fail) + "</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    u = load_users()
    await cb.message.answer(
        "<b>Статистика\n\n"
        "Пользователей: " + str(len(u)) + "\n"
        "Поисков: " + str(stats["checks"]) + "\n"
        "Найдено: " + str(stats["found"]) + "\n"
        "Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
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


# ===================== AUTH =====================
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
        await message.answer("<b>Код отправлен. Введи код (с пробелами или без):</b>", parse_mode="HTML")
    except Exception as e:
        await message.answer("<b>Ошибка: <code>" + esc(str(e)) + "</code></b>", parse_mode="HTML")
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
            "<b>Авторизован как @" + esc(str(me.username or me.first_name)) + "\n"
            "Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>",
            parse_mode="HTML", reply_markup=main_menu_kb()
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("<b>Введи пароль 2FA:</b>", parse_mode="HTML")
    except Exception as e:
        await message.answer("<b>Ошибка: <code>" + esc(str(e)) + "</code></b>", parse_mode="HTML")

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
            "<b>Авторизован как @" + esc(str(me.username or me.first_name)) + "\n"
            "Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>",
            parse_mode="HTML", reply_markup=main_menu_kb()
        )
    except Exception as e:
        await message.answer("<b>Неверный пароль: <code>" + esc(str(e)) + "</code></b>", parse_mode="HTML")


# ===================== SYNTAX CHECK =====================
# (run at import time — будет ошибка Python если что-то не так)

async def main():
    global ONBOARDING_DONE
    ONBOARDING_DONE = load_onboarding()
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("Neptun Parser запущен!")
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start",       description="Главное меню"),
        BotCommand(command="clear",       description="Остановить поиск"),
        BotCommand(command="neptunteam",  description="Справка"),
    ])
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
