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
ALL_GIFT_IDS      = []
NFT_COLLECTIONS   = {}
PRICE_FLOOR_CACHE = {}
NFT_CACHE         = {}
USER_BOOST        = {}
USER_MIN_GIFTS    = {}
USER_MAX_GIFTS    = {}
USER_LIMIT        = {}
USER_REGION       = {}
ONBOARDING_DONE   = set()

DEFAULT_BOOST     = 100
DEFAULT_MIN_GIFTS = 1
DEFAULT_MAX_GIFTS = 5
DEFAULT_LIMIT     = 30
DEFAULT_REGION    = "any"

# ── REGIONS ──────────────────────────────────────────────────────────────────
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

RU_LETTERS  = set("абвгдеёжзийклмнопрстуфхцчшщъыьэюяіїєґ")
UK_UA_ONLY  = set("іїєґ")   # только украинские/белорусские буквы

def _cyr_count(text):
    return sum(1 for c in text.lower() if c in RU_LETTERS)

def _lat_count(text):
    return sum(1 for c in text.lower() if 'a' <= c <= 'z')

def region_match(owner, username, name, region_key, bio_override=None):
    """Жёсткая проверка региона — требуем минимум 2 явных признака."""
    if not region_key or region_key == "any":
        return True

    bio   = bio_override if bio_override is not None else ((getattr(owner, "bio", "") or "") if owner else "")
    uname = (getattr(owner, "username",   "") or "") if owner else (username or "")
    fname = (getattr(owner, "first_name", "") or "") if owner else ""
    lname = (getattr(owner, "last_name",  "") or "") if owner else ""
    if not fname and name:
        parts = name.strip().split()
        fname = parts[0] if parts else ""
        lname = parts[1] if len(parts) > 1 else ""

    full_raw = (bio + " " + uname + " " + fname + " " + lname).strip()
    full     = full_raw.lower()

    if not full or len(full) < 3:
        return False

    cyr = _cyr_count(full)
    lat = _lat_count(full)
    has_cyr = cyr >= 4
    has_lat = lat >= 4

    def score_ru():
        s = 0
        if has_cyr: s += 1
        ua_signs = any(c in UK_UA_ONLY for c in full) or any(k in full for k in ["ukraine","київ","kyiv","слава україні","ukraine"])
        by_signs = any(k in full for k in ["беларус","минск","белорус"])
        if ua_signs or by_signs: return -1
        kw = ["россия","россиянин","москва","питер","санкт","рф","russia","moscow","spb","ru ","краснодар","новосибирск","екатеринбург","уфа","казань","нижний","ростов"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_ua():
        s = 0
        if has_cyr: s += 1
        ua_specific = sum(1 for c in full if c in UK_UA_ONLY)
        s += min(ua_specific, 2)
        kw = ["ukraine","ukrainian","київ","kyiv","харків","одеса","львів","ua ","з україни","слава україні","дніпро"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_by():
        s = 0
        if has_cyr: s += 1
        kw = ["беларус","минск","белорус","by ","беларь"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_us():
        if has_cyr: return -1
        s = 0
        if has_lat: s += 1
        kw = ["usa","us ","america","american","new york","los angeles","chicago","houston","miami","california","texas","new york","nyc","la ","english","english speaking"]
        for k in kw:
            if k in full: s += 1
        de_chars = set("äöüÄÖÜß"); fr_chars = set("àâæçéèêëîïôœùûüÿ"); es_chars = set("áéíóúüñ"); tr_chars = set("ğüşıöçĞÜŞİÖÇ")
        if any(c in de_chars | fr_chars | es_chars | tr_chars for c in full_raw): s -= 1
        return s

    def score_uk():
        if has_cyr: return -1
        s = 0
        if has_lat: s += 1
        kw = ["uk ","united kingdom","britain","british","england","london","manchester","birmingham","glasgow","scotland","wales","liverpool","english","pounds","sterling"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_de():
        if has_cyr: return -1
        s = 0
        de_chars = set("äöüÄÖÜß")
        found_chars = sum(1 for c in full_raw if c in de_chars)
        s += min(found_chars, 2)
        kw = ["german","deutsch","berlin","münchen","hamburg","frankfurt","köln","deutschland","de ","austria","schweiz"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_fr():
        if has_cyr: return -1
        s = 0
        fr_chars = set("àâæçéèêëîïôœùûüÿÀÂÆÇÉÈÊËÎÏÔŒÙÛÜŸ")
        found_chars = sum(1 for c in full_raw if c in fr_chars)
        s += min(found_chars, 2)
        kw = ["french","france","paris","française","fr ","lyon","marseille","bordeaux","belgique","suisse"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_es():
        if has_cyr: return -1
        s = 0
        es_chars = set("áéíóúüñÁÉÍÓÚÜÑ")
        found_chars = sum(1 for c in full_raw if c in es_chars)
        s += min(found_chars, 2)
        kw = ["spain","español","madrid","barcelona","españa","es ","valencia","sevilla","mexico","argentina","colombia","chile","peru","cubano","venezolano"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_tr():
        if has_cyr: return -1
        s = 0
        tr_chars = set("ğüşıöçĞÜŞİÖÇ")
        found_chars = sum(1 for c in full_raw if c in tr_chars)
        s += min(found_chars, 2)
        kw = ["turkey","türk","türkiye","istanbul","ankara","izmir","tr ","antalya"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_ae():
        if has_cyr: return -1
        s = 0
        ar_chars = sum(1 for c in full_raw if '\u0600' <= c <= '\u06ff')
        s += min(ar_chars, 2)
        kw = ["dubai","uae","emirates","sharjah","abu dhabi","دبي","الإمارات","emirat","abudhabi"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_cn():
        cn_chars = sum(1 for c in full_raw if '\u4e00' <= c <= '\u9fff')
        return min(cn_chars, 3)

    def score_jp():
        s = 0
        hi = sum(1 for c in full_raw if '\u3040' <= c <= '\u309f')
        ka = sum(1 for c in full_raw if '\u30a0' <= c <= '\u30ff')
        s += min(hi + ka, 2)
        kw = ["japan","tokyo","osaka","japanese","jp ","kyoto","yokohama"]
        for k in kw:
            if k in full: s += 1
        return s

    def score_in():
        s = 0
        dev = sum(1 for c in full_raw if '\u0900' <= c <= '\u097f')
        s += min(dev, 2)
        kw = ["india","indian","delhi","mumbai","bangalore","hindi","₹","rupee","pakistan","bangladesh"]
        for k in kw:
            if k in full: s += 1
        return s

    scorers = {
        "ru": score_ru, "ua": score_ua, "by": score_by,
        "us": score_us, "uk": score_uk, "de": score_de,
        "fr": score_fr, "es": score_es, "tr": score_tr,
        "ae": score_ae, "cn": score_cn, "jp": score_jp, "in": score_in,
    }
    fn = scorers.get(region_key)
    if not fn:
        return False
    # Требуем минимум 2 очка — жёсткая проверка
    return fn() >= 2


async def region_match_async(owner, username, name, region_key, uid=None):
    """Async версия с получением bio."""
    if not region_key or region_key == "any":
        return True
    bio = ""
    if uid:
        bio = await get_user_bio(uid)
    return region_match(owner, username, name, region_key, bio_override=bio)


# ── GIRL DETECTION ────────────────────────────────────────────────────────────
GIRL_NAMES_SET = {
    "анна","мария","екатерина","елена","ольга","наталья","татьяна","ирина",
    "юлия","алина","виктория","дарья","полина","ксения","валерия","александра",
    "надежда","людмила","галина","лиза","диана","кристина","светлана","милана",
    "арина","вера","жанна","ангелина","карина","оксана","нина","лариса","регина",
    "маша","катя","даша","оля","лена","юля","настя","поля","ксюша","вика","соня",
    "таня","надя","галя","аня","ника","алиса","злата","ева","эвелина","камилла",
    "яна","влада","руслана","женя","вероника","кира","стелла","белла","амина",
    "зара","рита","мила","тамара","инна","зоя","нора","лала","милена","ясмин",
    "anna","maria","kate","elena","olga","natasha","tatiana","irina","diana",
    "alina","dasha","masha","vika","lena","anya","yulia","lisa","sasha","tanya",
    "sonya","arina","karina","milana","zlata","eva","yana","veronika","kira",
    "stella","bella","nina","tina","vera","sofia","sophia","victoria","kristina",
    "valeria","natalia","angelina","jessica","ashley","emily","olivia","ava",
    "isabella","mia","abigail","madison","elizabeth","taylor","hannah","samantha",
    "lauren","grace","lily","ella","amber","kayla","chloe","jade","ruby","rose",
    "violet","daisy","aurora","aria","luna","scarlett","zoey","penelope","layla",
    "riley","nora","maya","claire","savannah","eleanor","camila","alexa","leah",
    "aubrey","ariana","alice","lana","lola","zara","candy","honey","cherry",
}
BOY_NAMES_SET = {
    "александр","алексей","андрей","антон","артем","борис","вадим","василий",
    "виктор","владимир","вячеслав","геннадий","георгий","григорий","даниил",
    "денис","дмитрий","евгений","иван","игорь","илья","кирилл","константин",
    "леонид","максим","михаил","никита","николай","олег","павел","петр","роман",
    "руслан","сергей","степан","тимур","федор","юрий","яков","аркадий",
    "alex","alexander","andrey","anton","artem","boris","victor","vladimir",
    "dmitri","dmitry","evgeny","ivan","igor","ilya","kirill","konstantin",
    "maxim","mikhail","nikita","nikolai","oleg","pavel","roman","ruslan",
    "sergey","timur","yuri","george","michael","james","john","robert","david",
    "william","richard","charles","joseph","thomas","mark","paul","andrew",
}
GIRL_SIGNALS = [
    "girl","lady","woman","she","her","female","♀",
    "👩","👸","💃","🌸","💖","💕","💗","👄","💄","🌺","🦋","🌷","🌹","💅","🦄","💫","✨","🍑","👑",
    "девушка","она","женщина","мама","дочь","принцесса","королева","богиня",
    "красотка","кошечка","зайка","лапочка","милашка","красавица","малышка",
    "onlyfans","model","модель","content","nsfw","18+",
]
BOY_SIGNALS = [
    "king","boss","bro","dude","male","guy","lord","sultan","парень","мужик",
    "мужчина","он ","сын ","брат ","папа","отец","муж ","дядя",
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

    score = 0

    # Явные мужские сигналы → сразу False
    for bn in BOY_NAMES_SET:
        if fname == bn or fname.startswith(bn + " "):
            return False
    for sig in BOY_SIGNALS:
        if sig in full:
            return False

    # Женское имя — сильный признак
    for gn in GIRL_NAMES_SET:
        if fname == gn or (len(gn) >= 4 and fname.startswith(gn)):
            score += 2
            break

    # Женские сигналы в тексте
    for sig in GIRL_SIGNALS:
        if sig in full:
            score += 1

    # Требуем минимум 2 очка
    return score >= 2


async def is_girl_async(owner, username=None, name=None, uid=None):
    bio_extra = ""
    if uid:
        bio_extra = await get_user_bio(uid)
    # Подмешиваем bio в owner временно через monkey-patch замену
    # Лучше передадим bio_extra как часть full текста
    bio   = bio_extra or ((getattr(owner, "bio", "") or "").lower() if owner else "")
    uname = (getattr(owner, "username",   "") or "").lower() if owner else (username or "").lower()
    fname = (getattr(owner, "first_name", "") or "").lower() if owner else ""
    lname = (getattr(owner, "last_name",  "") or "").lower() if owner else ""
    if not fname and name:
        parts = name.lower().split()
        fname = parts[0] if parts else ""
        lname = parts[1] if len(parts) > 1 else ""
    full = (bio + " " + uname + " " + fname + " " + lname).strip()

    score = 0
    for bn in BOY_NAMES_SET:
        if fname == bn or fname.startswith(bn + " "):
            return False
    for sig in BOY_SIGNALS:
        if sig in full:
            return False
    for gn in GIRL_NAMES_SET:
        if fname == gn or (len(gn) >= 4 and fname.startswith(gn)):
            score += 2
            break
    for sig in GIRL_SIGNALS:
        if sig in full:
            score += 1
    return score >= 2


# ── MODEL DETECTION ───────────────────────────────────────────────────────────
MODEL_KW = [
    "onlyfans","only fans","of.com","fansly","fanvue","nsfw","18+",
    "model","модель","content creator","blogger","блогер","influencer",
    "adult","vip content","premium","link in bio","linktr","linktree",
    "sexy","babe","goddess","spicy","naughty",
    "фотомодель","контент","взрослый контент","для взрослых",
]
MODEL_EMOJI = ["💋","🔥","👄","💦","🍑","💎","🌟","⭐","✨","💫"]

def is_model(owner, username=None, name=None):
    bio   = (getattr(owner, "bio",        "") or "").lower() if owner else ""
    uname = (getattr(owner, "username",   "") or "").lower() if owner else (username or "").lower()
    fname = (getattr(owner, "first_name", "") or "").lower() if owner else ""
    lname = (getattr(owner, "last_name",  "") or "").lower() if owner else ""
    if not fname and name:
        parts = name.lower().split()
        fname = parts[0] if parts else ""
    full = (bio + " " + uname + " " + fname + " " + lname).strip()
    raw_bio = (getattr(owner, "bio", "") or "") if owner else ""
    raw_uname = (getattr(owner, "username", "") or "") if owner else (username or "")

    for kw in MODEL_KW:
        if kw in full:
            return True
    for em in MODEL_EMOJI:
        if em in raw_bio or em in raw_uname:
            return True
    return False


# ── USERS ─────────────────────────────────────────────────────────────────────
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


# ── FSM ───────────────────────────────────────────────────────────────────────
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


# ── HELPERS ───────────────────────────────────────────────────────────────────
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
    # Ищем в диапазоне: от 70% флора до флора * (1 + boost/100)
    return floor * 0.7 <= price <= floor * (1.0 + boost / 100.0)

def cache_owner(uid, owner, username, name, profile_url, items):
    NFT_CACHE[uid] = {"owner": owner, "username": username,
                      "name": name, "profile_url": profile_url, "items": items}

CAT_LABELS = {
    "cheap":   "Дешевые до 2000",
    "mid":     "Средние 2000-5000",
    "hard":    "Сложные 5000-20000",
    "ultra":   "Хард 20000-100000",
    "extreme": "Экстрим от 100000",
}


# ── KEYBOARDS ─────────────────────────────────────────────────────────────────
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
        [InlineKeyboardButton(text="По модели",   callback_data="mode_model")],
        [InlineKeyboardButton(text="Назад",       callback_data="menu")],
    ])

def cat_kb(mode):
    # prefix: mc_ = market, pc_ = profile, mm_ = model
    if mode == "market":
        p = "mc_"
    elif mode == "profile":
        p = "pc_"
    else:
        p = "mm_"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Дешевые до 2000",    callback_data=p+"cheap")],
        [InlineKeyboardButton(text="Средние 2000-5000",  callback_data=p+"mid")],
        [InlineKeyboardButton(text="Сложные 5000-20000", callback_data=p+"hard")],
        [InlineKeyboardButton(text="Хард 20000-100000",  callback_data=p+"ultra")],
        [InlineKeyboardButton(text="Экстрим от 100000",  callback_data=p+"extreme")],
        [InlineKeyboardButton(text="Назад",              callback_data="search_mode_select")],
    ])

def who_kb(mode, cat):
    # mode: market / profile    cat: cheap/mid/hard/ultra/extreme
    back = "mode_" + mode
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Всех",    callback_data="go_" + mode + "_" + cat + "_all")],
        [InlineKeyboardButton(text="Девушек", callback_data="go_" + mode + "_" + cat + "_girls")],
        [InlineKeyboardButton(text="Назад",   callback_data=back)],
    ])

def who_model_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Всех моделей",    callback_data="mdl_who_all")],
        [InlineKeyboardButton(text="Только девушек", callback_data="mdl_who_girls")],
        [InlineKeyboardButton(text="Назад",           callback_data="search_mode_select")],
    ])

def model_col_kb(who, collections):
    """Инлайн кнопки всех коллекций для поиска по модели."""
    rows = []
    row = []
    for title, gid in collections:
        lbl = str(title)
        row.append(InlineKeyboardButton(text=lbl, callback_data="mdlrun_" + who + "_" + str(gid)))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="Все коллекции", callback_data="mdlrun_" + who + "_all")])
    rows.append([InlineKeyboardButton(text="Назад", callback_data="mode_model")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def settings_menu_kb(uid):
    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    lim = get_limit(uid)
    reg = get_region(uid)
    mx_s    = str(mx) if mx > 0 else "без лимита"
    reg_lbl = REGIONS.get(reg, {}).get("label", "Все страны")
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мин. гифтов: " + str(mn),   callback_data="set_min")],
        [InlineKeyboardButton(text="Макс. гифтов: " + mx_s,     callback_data="set_max")],
        [InlineKeyboardButton(text="Лимит выдачи: " + str(lim), callback_data="set_limit")],
        [InlineKeyboardButton(text="Регион: " + reg_lbl,        callback_data="set_region")],
        [InlineKeyboardButton(text="Назад",                      callback_data="menu")],
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
         InlineKeyboardButton(text=l(40), callback_data="lim_40"),
         InlineKeyboardButton(text=l(50), callback_data="lim_50")],
        [InlineKeyboardButton(text=l(60), callback_data="lim_60"),
         InlineKeyboardButton(text=l(70), callback_data="lim_70"),
         InlineKeyboardButton(text=l(80), callback_data="lim_80"),
         InlineKeyboardButton(text=l(90), callback_data="lim_90"),
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


# ── COLLECTIONS ───────────────────────────────────────────────────────────────
async def load_collections():
    global ALL_GIFT_IDS, NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        ALL_GIFT_IDS    = []
        NFT_COLLECTIONS = {}
        seen = set()
        for gift in (getattr(result, "gifts", None) or []):
            gid   = getattr(gift, "id", None)
            if gid is None or gid in seen:
                continue
            title = getattr(gift, "title", None) or ("Gift #" + str(gid))
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


# ── API ───────────────────────────────────────────────────────────────────────
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
    for _ in range(2):
        try:
            result    = await tg_client(GetResaleStarGiftsRequest(gift_id=gid, offset=offset, limit=limit))
            users_map = {int(u.id): u for u in (getattr(result, "users", None) or [])}
            # Название коллекции из кэша
            col_title = next((t for t, i in NFT_COLLECTIONS.items() if i == gid), None)
            items     = []
            for gift in (getattr(result, "gifts", None) or []):
                owner, oid = get_owner(gift, users_map)
                username   = getattr(owner, "username", None) if owner else None
                fn = (getattr(owner, "first_name", "") or "") if owner else ""
                ln = (getattr(owner, "last_name",  "") or "") if owner else ""
                name = (fn + " " + ln).strip()
                nft_url = make_nft_url(gift)
                profile_url = ("https://t.me/" + username) if username else (("tg://user?id=" + str(oid)) if oid else None)
                # title: сначала из gift.title, потом из коллекции
                raw_title = getattr(gift, "title", None)
                if not raw_title or str(raw_title).strip() in ("", "?", "None"):
                    raw_title = col_title or "NFT"
                items.append({
                    "owner": owner, "owner_id": oid,
                    "username": username, "name": name,
                    "title": str(raw_title),
                    "num":   getattr(gift, "num", "?"),
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

USER_BIO_CACHE = {}  # uid -> bio string

async def get_user_bio(uid):
    """Получает bio пользователя через GetFullUser, с кэшем."""
    if uid in USER_BIO_CACHE:
        return USER_BIO_CACHE[uid]
    try:
        full = await tg_client(
            __import__("telethon.tl.functions.users", fromlist=["GetFullUserRequest"]).GetFullUserRequest(uid)
        )
        bio = getattr(getattr(full, "full_user", None), "about", None) or ""
        USER_BIO_CACHE[uid] = bio
        return bio
    except Exception:
        USER_BIO_CACHE[uid] = ""
        return ""
    all_items = []
    offset    = ""
    for _ in range(max_pages):
        try:
            result = await tg_client(GetSavedStarGiftsRequest(
                peer=await tg_client.get_input_entity(uid),
                offset=offset, limit=100,
            ))
            for gift in (getattr(result, "gifts", None) or []):
                nft_url   = make_nft_url(gift)
                inner     = getattr(gift, "gift", None)
                if not nft_url and inner:
                    nft_url = make_nft_url(inner)
                title     = (getattr(inner, "title", None) or getattr(gift, "title", "?"))
                num       = getattr(gift, "num", "?")
                on_market = bool(getattr(gift, "resell_amount", None))
                all_items.append({"title": title, "num": num, "nft_url": nft_url, "on_market": on_market})
            offset = getattr(result, "next_offset", "") or ""
            if not offset:
                break
        except Exception as e:
            logger.error("saved_gifts uid=%s: %s", uid, e)
            break
    return all_items


# ── SEARCH CORE ───────────────────────────────────────────────────────────────
def _make_nft_lines(items):
    lines = ""
    for it in items[:5]:
        nu = it.get("nft_url")
        t  = esc(str(it.get("title","?")))
        n  = esc(str(it.get("num","?")))
        p  = it.get("price")
        ps = " — " + str(p) + " ⭐" if p else ""
        if nu:
            lines += '\n<a href="' + nu + '">' + t + " #" + n + ps + "</a>"
        else:
            lines += "\n" + t + " #" + n + ps
    if len(items) > 5:
        lines += "\n+ ещё " + str(len(items) - 5) + " NFT"
    return lines


async def do_market_search(status_msg, gift_ids, cat=None, girls_only=False,
                           boost=100, min_gifts=2, max_gifts=0,
                           max_results=30, region="any"):
    global is_searching
    is_searching = True

    # Общее состояние — защищаем lock-ом только критические мутации
    lock        = asyncio.Lock()
    found       = [0]          # список чтобы nonlocal не нужен был
    seen_slugs  = set()
    owner_map   = {}
    sent_owners = set()

    async def send_owner(uid, bucket):
        async with lock:
            if uid in sent_owners or found[0] >= max_results:
                return
            sent_owners.add(uid)
        cnt     = len(bucket["items"])
        kb      = owner_card_kb(bucket["username"], bucket["profile_url"], uid)
        owner_s = fmt_owner(bucket["owner"], bucket["username"], bucket["name"])
        cache_owner(uid, bucket["owner"], bucket["username"],
                    bucket["name"], bucket["profile_url"], bucket["items"])
        txt = ("<b>" + owner_s + "\nNFT на маркете: " + str(cnt) + "</b>"
               + _make_nft_lines(bucket["items"]))
        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id, text=txt,
                parse_mode="HTML", reply_markup=kb,
                disable_web_page_preview=True,
            )
            async with lock:
                found[0] += 1
            stats["found"] += 1
        except Exception as e:
            logger.warning("send_owner: %s", e)

    async def flush_ready():
        async with lock:
            uids = list(owner_map.keys())
        for uid in uids:
            async with lock:
                if uid in sent_owners or found[0] >= max_results:
                    continue
                b = owner_map.pop(uid, None)
            if b and gifts_in_range(len(b["items"]), min_gifts, max_gifts):
                await send_owner(uid, b)

    async def scan_col(gid):
        fl = None
        if cat:
            fl = await get_floor(gid)
            if fl is not None and not floor_in_cat(fl, cat):
                return
        offset = ""
        while is_searching and found[0] < max_results:
            items, nxt = await fetch_market_page(gid, offset, limit=100)
            if not items:
                break
            for item in items:
                if not is_searching or found[0] >= max_results:
                    return
                nft_url = item.get("nft_url") or ""
                slug    = nft_url.split("/")[-1] if nft_url else ""

                async with lock:
                    if slug:
                        if slug in seen_slugs:
                            continue
                        seen_slugs.add(slug)
                    oid = item["owner_id"]
                    if not oid or oid in sent_owners:
                        continue

                price = item.get("price")
                if cat and fl and price and not price_ok(price, fl, boost):
                    continue
                if not await region_match_async(item["owner"], item["username"], item["name"], region, uid=oid):
                    continue
                if girls_only and not await is_girl_async(item["owner"], item["username"], item["name"], uid=oid):
                    continue

                async with lock:
                    if oid in sent_owners:
                        continue
                    if oid not in owner_map:
                        owner_map[oid] = {
                            "owner": item["owner"], "username": item["username"],
                            "name": item["name"], "profile_url": item["profile_url"],
                            "items": [],
                        }
                    owner_map[oid]["items"].append(item)
                    cnt = len(owner_map[oid]["items"])
                    should_send = gifts_in_range(cnt, min_gifts, max_gifts)
                    if should_send:
                        b = owner_map.pop(oid)
                    else:
                        b = None

                if b:
                    await send_owner(oid, b)
                    if found[0] >= max_results:
                        return
            if not nxt:
                break
            offset = nxt

    try:
        await status_msg.edit_text("<b>Идёт парсинг подарка, ожидай</b>", parse_mode="HTML", reply_markup=stop_kb())

        valid_ids = list(gift_ids)
        random.shuffle(valid_ids)
        scanned  = 0
        PARALLEL = 10

        for i in range(0, len(valid_ids), PARALLEL):
            if not is_searching or found[0] >= max_results:
                break
            batch = valid_ids[i:i+PARALLEL]
            await asyncio.gather(*[scan_col(gid) for gid in batch])
            await flush_ready()
            scanned += len(batch)

        await flush_ready()
    except Exception as e:
        logger.error("do_market_search: %s", e)
    finally:
        is_searching = False
    return found[0]


async def do_profile_search(status_msg, gift_ids, cat=None, girls_only=False,
                            min_gifts=2, max_gifts=0,
                            max_results=30, region="any"):
    global is_searching
    is_searching = True
    lock      = asyncio.Lock()
    found     = [0]
    seen_sent = set()

    try:
        await status_msg.edit_text("<b>Собираю владельцев NFT...</b>", parse_mode="HTML", reply_markup=stop_kb())

        # Фаза 1: параллельно собираем уникальных владельцев с первых страниц коллекций
        PARALLEL = 10
        owners_index = {}
        shuffled_ids = list(gift_ids)
        random.shuffle(shuffled_ids)

        for i in range(0, len(shuffled_ids), PARALLEL):
            if not is_searching:
                break
            batch = shuffled_ids[i:i+PARALLEL]
            pages = await asyncio.gather(*[fetch_market_page(gid, "", limit=100) for gid in batch])
            for _gid, (items, _) in zip(batch, pages):
                for item in items:
                    oid = item["owner_id"]
                    if not oid or oid in owners_index:
                        continue
                    owners_index[oid] = {
                        "owner": item["owner"], "username": item["username"],
                        "name": item["name"], "profile_url": item["profile_url"],
                    }
            if len(owners_index) >= max_results * 10:
                break

        total = len(owners_index)
        await status_msg.edit_text("<b>Идёт парсинг подарка, ожидай</b>", parse_mode="HTML", reply_markup=stop_kb())

        owner_list = list(owners_index.items())
        random.shuffle(owner_list)
        checked = [0]

        # Фаза 2: параллельно проверяем профили каждого владельца
        async def check_one(uid, info):
            if not is_searching or found[0] >= max_results:
                return
            async with lock:
                if uid in seen_sent:
                    return

            owner_obj   = info["owner"]
            username    = info["username"]
            name        = info["name"]
            profile_url = (info["profile_url"]
                           or ("https://t.me/" + username if username else "tg://user?id=" + str(uid)))

            if not await region_match_async(owner_obj, username, name, region, uid=uid):
                return
            if girls_only and not await is_girl_async(owner_obj, username, name, uid=uid):
                return

            saved  = await fetch_saved_gifts(uid, max_pages=5)
            hidden = [g for g in saved if g.get("nft_url") and not g.get("on_market")]

            async with lock:
                if uid in seen_sent or not gifts_in_range(len(hidden), min_gifts, max_gifts):
                    return
                if found[0] >= max_results:
                    return
                seen_sent.add(uid)

            cache_owner(uid, owner_obj, username, name, profile_url, hidden)
            owner_s = fmt_owner(owner_obj, username, name)
            txt = (
                "<b>" + owner_s + "\nСкрытых NFT: " + str(len(hidden)) + "</b>"
                + _make_nft_lines(hidden)
            )
            kb = owner_card_kb(username, profile_url, uid)
            try:
                await status_msg.bot.send_message(
                    chat_id=status_msg.chat.id, text=txt,
                    parse_mode="HTML", reply_markup=kb,
                    disable_web_page_preview=True,
                )
                async with lock:
                    found[0] += 1
                stats["found"] += 1
            except Exception as e:
                logger.warning("profile send: %s", e)

        PARALLEL_CHECK = 10
        for i in range(0, len(owner_list), PARALLEL_CHECK):
            if not is_searching or found[0] >= max_results:
                break
            batch = owner_list[i:i+PARALLEL_CHECK]
            await asyncio.gather(*[check_one(uid, info) for uid, info in batch])
            checked[0] += len(batch)
    except Exception as e:
        logger.error("do_profile_search: %s", e)
    finally:
        is_searching = False
    return found[0]


async def do_model_search(status_msg, gift_ids, girls_only=False,
                          max_results=30, region="any"):
    global is_searching
    is_searching = True
    lock        = asyncio.Lock()
    found       = [0]
    seen_slugs  = set()
    seen_owners = set()

    async def scan_col(gid):
        offset = ""
        while is_searching and found[0] < max_results:
            items, nxt = await fetch_market_page(gid, offset, limit=100)
            if not items:
                break
            for item in items:
                if not is_searching or found[0] >= max_results:
                    return
                oid     = item["owner_id"]
                nft_url = item.get("nft_url") or ""
                slug    = nft_url.split("/")[-1] if nft_url else ""

                async with lock:
                    if slug and slug in seen_slugs:
                        continue
                    if not oid or oid in seen_owners:
                        continue
                    if slug:
                        seen_slugs.add(slug)

                if not await region_match_async(item["owner"], item["username"], item["name"], region, uid=oid):
                    continue
                if girls_only and not await is_girl_async(item["owner"], item["username"], item["name"], uid=oid):
                    continue
                if not is_model(item["owner"], item["username"], item["name"]):
                    continue

                async with lock:
                    if oid in seen_owners or found[0] >= max_results:
                        continue
                    seen_owners.add(oid)

                username = item["username"]
                name     = item["name"]
                p_url    = item["profile_url"]
                price    = item.get("price")
                title    = esc(str(item.get("title", "?")))
                num      = esc(str(item.get("num", "?")))
                price_s  = str(price) + " ⭐" if price else "нет цены"
                owner_s  = fmt_owner(item["owner"], username, name)
                nft_line = ('\n<a href="' + nft_url + '">' + title + " #" + num + "</a>"
                            if nft_url else "\n" + title + " #" + num)
                txt = "<b>" + owner_s + "</b>" + nft_line + "\n" + price_s
                cache_owner(oid, item["owner"], username, name, p_url, [item])
                kb = owner_card_kb(username, p_url, oid)
                try:
                    await status_msg.bot.send_message(
                        chat_id=status_msg.chat.id, text=txt,
                        parse_mode="HTML", reply_markup=kb,
                        disable_web_page_preview=True,
                    )
                    async with lock:
                        found[0] += 1
                    stats["found"] += 1
                except Exception as e:
                    logger.warning("model send: %s", e)
            if not nxt:
                break
            offset = nxt

    try:
        await status_msg.edit_text("<b>Идёт парсинг подарка, ожидай</b>", parse_mode="HTML", reply_markup=stop_kb())
        valid_ids = list(gift_ids)
        random.shuffle(valid_ids)
        scanned  = 0
        PARALLEL = 10

        for i in range(0, len(valid_ids), PARALLEL):
            if not is_searching or found[0] >= max_results:
                break
            batch = valid_ids[i:i+PARALLEL]
            await asyncio.gather(*[scan_col(gid) for gid in batch])
            scanned += len(batch)
    except Exception as e:
        logger.error("do_model_search: %s", e)
    finally:
        is_searching = False
    return found[0]


# ── RUN HELPERS ───────────────────────────────────────────────────────────────
async def _start_market(cb, cat, girls):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
        return
    await cb.answer("Запускаю...")
    stats["checks"] += 1
    uid    = cb.from_user.id
    ids    = await ensure_collections()
    if not ids:
        await cb.message.answer("<b>Коллекции не загружены.</b>", parse_mode="HTML", reply_markup=menu_kb())
        return
    boost  = get_boost(uid)
    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    lim    = get_limit(uid)
    reg    = get_region(uid)
    mx_s   = str(mx) if mx > 0 else "без лимита"
    cat_l  = CAT_LABELS.get(cat, "Все")
    who_l  = "Девушки" if girls else "Все"
    reg_l  = REGIONS.get(reg, {}).get("label", "Все страны")
    txt = (
        "<b>Маркет / " + cat_l + " / " + who_l + "\n"
        "Регион: " + reg_l + "\n"
        "Гифтов: от " + str(mn) + " до " + mx_s + "\n"
        "Лимит: " + str(lim) + "</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    try:
        found = await asyncio.wait_for(
            do_market_search(status, ids, cat=cat, girls_only=girls,
                             boost=boost, min_gifts=mn, max_gifts=mx,
                             max_results=lim, region=reg),
            timeout=300
        )
    except asyncio.TimeoutError:
        is_searching = False
        found = 0
    try:
        await status.edit_text(
            "<b>Готово. Маркет / " + cat_l + " / " + who_l + "\nНайдено: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass

async def _start_profile(cb, cat, girls):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
        return
    await cb.answer("Запускаю...")
    stats["checks"] += 1
    uid    = cb.from_user.id
    ids    = await ensure_collections()
    if not ids:
        await cb.message.answer("<b>Коллекции не загружены.</b>", parse_mode="HTML", reply_markup=menu_kb())
        return
    mn     = get_min_gifts(uid)
    mx     = get_max_gifts(uid)
    lim    = get_limit(uid)
    reg    = get_region(uid)
    mx_s   = str(mx) if mx > 0 else "без лимита"
    cat_l  = CAT_LABELS.get(cat, "Все")
    who_l  = "Девушки" if girls else "Все"
    reg_l  = REGIONS.get(reg, {}).get("label", "Все страны")
    txt = (
        "<b>Профиль / " + cat_l + " / " + who_l + "\n"
        "Регион: " + reg_l + "\n"
        "Скрытых гифтов: от " + str(mn) + " до " + mx_s + "\n"
        "Лимит: " + str(lim) + "</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_profile_search(status, ids, cat=cat, girls_only=girls,
                                    min_gifts=mn, max_gifts=mx,
                                    max_results=lim, region=reg)
    try:
        await status.edit_text(
            "<b>Готово. Профиль / " + cat_l + " / " + who_l + "\nНайдено: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass

async def _start_model(cb, girls=False, single_gid=None):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
        return
    await cb.answer("Запускаю...")
    stats["checks"] += 1
    uid    = cb.from_user.id
    ids    = await ensure_collections()
    if not ids:
        await cb.message.answer("<b>Коллекции не загружены.</b>", parse_mode="HTML", reply_markup=menu_kb())
        return
    # Если выбрана конкретная коллекция — ищем только в ней
    if single_gid is not None:
        ids = [single_gid]
    lim    = get_limit(uid)
    reg    = get_region(uid)
    who_l  = "Девушки-модели" if girls else "Все модели"
    reg_l  = REGIONS.get(reg, {}).get("label", "Все страны")
    col_l  = "все коллекции" if single_gid is None else "1 коллекция"
    txt = (
        "<b>" + who_l + "\n"
        "Коллекция: " + col_l + "\n"
        "Регион: " + reg_l + "\n"
        "Лимит: " + str(lim) + "</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_model_search(status, ids, girls_only=girls,
                                  max_results=lim, region=reg)
    try:
        await status.edit_text(
            "<b>Готово. " + who_l + "\nНайдено: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


WELCOME_TXT = "<b>Neptun Parser\n\nВыбери действие:</b>"


# ── ONBOARDING ────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    uid    = message.from_user.id
    add_user(uid, message.from_user.username,
             message.from_user.first_name, message.from_user.last_name)

    if not await check_authorized() and is_admin(uid):
        await message.answer("<b>Нужна авторизация Telegram\nВведи номер телефона:</b>", parse_mode="HTML")
        await state.set_state(Auth.phone)
        return

    if uid not in ONBOARDING_DONE:
        await message.answer(
            "<b>Добро пожаловать в Neptun Parser\n\n"
            "Шаг 1 из 3\n"
            "Минимум гифтов у владельца.\n"
            "Введи число (например 2):</b>",
            parse_mode="HTML"
        )
        await state.set_state(Onboarding.min_gifts)
        return

    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await message.answer(
        "<b>Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n\n"
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
        "Шаг 2 из 3\n"
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
        "Шаг 3 из 3\n"
        "Лимит выдачи результатов за один поиск.\n"
        "Введи число от 5 до 100:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="10", callback_data="oblim_10"),
             InlineKeyboardButton(text="20", callback_data="oblim_20"),
             InlineKeyboardButton(text="30", callback_data="oblim_30"),
             InlineKeyboardButton(text="40", callback_data="oblim_40"),
             InlineKeyboardButton(text="50", callback_data="oblim_50")],
            [InlineKeyboardButton(text="60", callback_data="oblim_60"),
             InlineKeyboardButton(text="70", callback_data="oblim_70"),
             InlineKeyboardButton(text="80", callback_data="oblim_80"),
             InlineKeyboardButton(text="90", callback_data="oblim_90"),
             InlineKeyboardButton(text="100", callback_data="oblim_100")],
        ])
    )
    await state.set_state(Onboarding.limit)

@dp.message(Onboarding.boost)
async def ob_boost(message: Message, state: FSMContext):
    # Буст убран из онбординга — пропускаем
    if not message.text or not message.text.strip().isdigit():
        await message.answer("<b>Введи число от 5 до 100:</b>", parse_mode="HTML")
        return
    val = max(5, min(100, int(message.text.strip())))
    await _finish_onboarding(message.from_user.id, val, state, message)

@dp.callback_query(F.data.startswith("oblim_"))
async def ob_lim_btn(cb: CallbackQuery, state: FSMContext):
    if await state.get_state() != Onboarding.limit.state:
        await cb.answer()
        return
    val = int(cb.data[6:])
    await _finish_onboarding(cb.from_user.id, val, state, cb.message)
    await cb.answer()

@dp.message(Onboarding.limit)
async def ob_lim_text(message: Message, state: FSMContext):
    if not message.text or not message.text.strip().isdigit():
        await message.answer("<b>Введи число от 5 до 100:</b>", parse_mode="HTML")
        return
    val = max(5, min(100, int(message.text.strip())))
    await _finish_onboarding(message.from_user.id, val, state, message)

async def _finish_onboarding(uid, limit_val, state, msg_or_cb_msg):
    USER_LIMIT[uid] = limit_val
    ONBOARDING_DONE.add(uid)
    save_onboarding()
    await state.clear()
    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await msg_or_cb_msg.answer(
        "<b>Настройка завершена\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n"
        "Лимит: " + str(limit_val) + "\n\n"
        "Менять можно в Настройках</b>",
        parse_mode="HTML"
    )
    await msg_or_cb_msg.answer(WELCOME_TXT, parse_mode="HTML", reply_markup=main_menu_kb())


# ── COMMANDS ──────────────────────────────────────────────────────────────────
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
    # Работает только если онбординг пройден
    uid  = message.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    lim  = get_limit(uid)
    reg  = get_region(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    reg_l= REGIONS.get(reg, {}).get("label", "Все страны")
    txt = (
        "<b>Neptun Parser — справка\n\n"
        "РЕЖИМЫ ПОИСКА\n\n"
        "По маркету\n"
        "Ищет NFT выставленные на продажу.\n"
        "Фильтрует по цене относительно флора коллекции.\n\n"
        "По профилю\n"
        "Ищет владельцев у которых NFT есть в профиле но не выставлены на маркет.\n\n"
        "По модели\n"
        "Показывает NFT у владельцев-моделей и блогеров.\n\n"
        "НАСТРОЙКИ\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Минимум NFT у владельца чтобы попасть в результаты.\n\n"
        "Макс. гифтов: " + mx_s + "\n"
        "Максимум NFT. 0 = без лимита.\n\n"
        "Лимит выдачи: " + str(lim) + "\n"
        "Сколько результатов показывает один поиск.\n\n"
        "Регион: " + reg_l + "\n"
        "Жёсткий фильтр по языку и признакам страны в профиле.\n\n"
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


# ── CALLBACKS: NAVIGATION ─────────────────────────────────────────────────────
@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    uid  = cb.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await cb.message.answer(
        "<b>Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n\n"
        "Выбери действие:</b>",
        parse_mode="HTML", reply_markup=main_menu_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "search_mode_select")
async def cb_search_mode(cb: CallbackQuery):
    await cb.message.answer("<b>Выбери режим поиска:</b>",
                            parse_mode="HTML", reply_markup=search_mode_select_kb())
    await cb.answer()

@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.message.answer("<b>Маркет   выбери ценовую категорию:</b>",
                            parse_mode="HTML", reply_markup=cat_kb("market"))
    await cb.answer()

@dp.callback_query(F.data == "mode_profile")
async def cb_mode_profile(cb: CallbackQuery):
    await cb.message.answer("<b>Профиль   выбери ценовую категорию:</b>",
                            parse_mode="HTML", reply_markup=cat_kb("profile"))
    await cb.answer()

@dp.callback_query(F.data == "mode_model")
async def cb_mode_model(cb: CallbackQuery):
    await cb.message.answer("<b>По модели — кого искать?</b>",
                            parse_mode="HTML", reply_markup=who_model_kb())
    await cb.answer()

# mc_<cat>   маркет категория -> кого искать
@dp.callback_query(F.data.startswith("mc_"))
async def cb_mc(cb: CallbackQuery):
    cat = cb.data[3:]
    lbl = CAT_LABELS.get(cat, cat)
    await cb.message.answer("<b>Маркет / " + lbl + "\nКого искать?</b>",
                            parse_mode="HTML", reply_markup=who_kb("market", cat))
    await cb.answer()

# pc_<cat>   профиль категория -> кого искать
@dp.callback_query(F.data.startswith("pc_"))
async def cb_pc(cb: CallbackQuery):
    cat = cb.data[3:]
    lbl = CAT_LABELS.get(cat, cat)
    await cb.message.answer("<b>Профиль / " + lbl + "\nКого искать?</b>",
                            parse_mode="HTML", reply_markup=who_kb("profile", cat))
    await cb.answer()

# go_market_<cat>_<who>   запуск маркета
@dp.callback_query(F.data.startswith("go_market_"))
async def cb_go_market(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    rest  = cb.data[len("go_market_"):]          # "cheap_all" or "cheap_girls"
    parts = rest.rsplit("_", 1)
    cat   = parts[0] if len(parts) == 2 else rest
    who   = parts[1] if len(parts) == 2 else "all"
    await _start_market(cb, cat=cat, girls=(who == "girls"))

# go_profile_<cat>_<who>   запуск профиля
@dp.callback_query(F.data.startswith("go_profile_"))
async def cb_go_profile(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    rest  = cb.data[len("go_profile_"):]
    parts = rest.rsplit("_", 1)
    cat   = parts[0] if len(parts) == 2 else rest
    who   = parts[1] if len(parts) == 2 else "all"
    await _start_profile(cb, cat=cat, girls=(who == "girls"))

# mdl_who_<who>   выбор кого искать → показываем коллекции
@dp.callback_query(F.data.startswith("mdl_who_"))
async def cb_mdl_who(cb: CallbackQuery):
    who = cb.data[len("mdl_who_"):]   # all / girls
    if not ALL_GIFT_IDS:
        await cb.answer("Коллекции не загружены", show_alert=True)
        return
    lbl = "Девушки-модели" if who == "girls" else "Все модели"
    await cb.message.answer(
        "<b>" + lbl + " — выбери коллекцию или все сразу:</b>",
        parse_mode="HTML",
        reply_markup=model_col_kb(who, ALL_GIFT_IDS)
    )
    await cb.answer()

# mdlrun_<who>_<gid|all>   запуск поиска по модели
@dp.callback_query(F.data.startswith("mdlrun_"))
async def cb_mdlrun(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    rest = cb.data[len("mdlrun_"):]
    parts = rest.split("_", 1)
    if len(parts) != 2:
        await cb.answer()
        return
    who, gid_s = parts
    gid = None if gid_s == "all" else int(gid_s)
    await _start_model(cb, girls=(who == "girls"), single_gid=gid)

# gomdl_<who>   старый коллбэк для совместимости
@dp.callback_query(F.data.startswith("gomdl_"))
async def cb_go_model(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    who = cb.data[len("gomdl_"):]
    await _start_model(cb, girls=(who == "girls"), single_gid=None)

@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    if not is_searching:
        await cb.answer("У вас нет активного поиска", show_alert=True)
        return
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
        "Лимит: " + str(lim) + "\n"
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


# ── CALLBACKS: SETTINGS ───────────────────────────────────────────────────────
@dp.callback_query(F.data == "settings_menu")
async def cb_settings(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    uid  = cb.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await cb.message.answer(
        "<b>Настройки поиска\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "</b>",
        parse_mode="HTML", reply_markup=settings_menu_kb(uid)
    )
    await cb.answer()

@dp.callback_query(F.data == "set_min")
async def cb_set_min(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("<b>Введи минимум гифтов (число от 1):</b>",
                            parse_mode="HTML", reply_markup=input_cancel_kb())
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
    await cb.message.answer("<b>Введи максимум гифтов (0 = без лимита):</b>",
                            parse_mode="HTML", reply_markup=input_cancel_kb())
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
    await cb.message.answer("<b>Буст цен\n100% = до x2 флора  200% = до x3</b>",
                            parse_mode="HTML", reply_markup=boost_kb())
    await cb.answer()

@dp.callback_query(F.data.startswith("bst_"))
async def cb_bst(cb: CallbackQuery, state: FSMContext):
    raw = cb.data[4:]
    if raw == "custom":
        await cb.message.answer("<b>Введи буст вручную (число %):</b>",
                                parse_mode="HTML", reply_markup=input_cancel_kb())
        await state.set_state(SetBoost.value)
        await cb.answer()
        return
    val = int(raw)
    USER_BOOST[cb.from_user.id] = val
    await cb.answer("Буст: " + str(val) + "%", show_alert=True)
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

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
    await cb.message.answer("<b>Лимит выдачи результатов:</b>",
                            parse_mode="HTML", reply_markup=limit_kb(lim))
    await cb.answer()

@dp.callback_query(F.data.startswith("lim_"))
async def cb_lim(cb: CallbackQuery):
    val = int(cb.data[4:])
    USER_LIMIT[cb.from_user.id] = val
    await cb.answer("Лимит: " + str(val), show_alert=False)
    try:
        await cb.message.edit_reply_markup(reply_markup=limit_kb(val))
    except Exception:
        pass

@dp.callback_query(F.data == "set_region")
async def cb_set_region(cb: CallbackQuery):
    reg = get_region(cb.from_user.id)
    await cb.message.answer("<b>Выбери регион поиска:</b>",
                            parse_mode="HTML", reply_markup=region_kb(reg))
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
    try:
        await cb.message.edit_reply_markup(reply_markup=region_kb(key))
    except Exception:
        pass


# ── CALLBACKS: ADMIN ──────────────────────────────────────────────────────────
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
    await cb.message.answer("<b>Коллекции обновлены: " + str(len(ALL_GIFT_IDS)) + " шт.</b>",
                            parse_mode="HTML", reply_markup=admin_kb())

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await cb.message.answer("<b>Отправь сообщение для рассылки. /cancel   отмена</b>",
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


# ── AUTH ──────────────────────────────────────────────────────────────────────
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


# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    global ONBOARDING_DONE
    ONBOARDING_DONE = load_onboarding()
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("Neptun Parser запущен!")
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start",      description="Главное меню"),
        BotCommand(command="clear",      description="Остановить поиск"),
        BotCommand(command="neptunteam", description="Справка"),
    ])
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info("Авторизован, коллекций: %d", len(ALL_GIFT_IDS))
        else:
            logger.warning("Не авторизован   пройди /start")
    except Exception as e:
        logger.error("Ошибка старта: %s", e)
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
