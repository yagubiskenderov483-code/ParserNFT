import asyncio
import logging
import urllib.parse
import os
import json
import sqlite3
import time
import datetime
import re
import random
from telethon import TelegramClient
from telethon.tl.functions.payments import (
    GetResaleStarGiftsRequest, GetStarGiftsRequest, GetSavedStarGiftsRequest
)
from telethon.tl.functions.messages import GetHistoryRequest, SearchRequest, GetInlineBotResultsRequest
from telethon.tl.types import InputPeerEmpty, MessageService, InputPeerSelf
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
BOT_TOKEN    = "8790434095:AAG5eA6OzMcC2-VdLeTeITahdUi_6KiIRiw"
ADMIN_ID     = 7186944876
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
DB_TARGET_USERS   = 100_000   # цель локальной БД владельцев

# Глобальный антидубль (персистентный, между поисками)
SEEN_GLOBAL: set = set()          # owner keys: uid или u:username
SEEN_GIFTS: set = set()           # nft slugs
SEEN_GLOBAL_MAX = 9_000_000
# Счётчик выдач по коллекции в текущем поиске
MAX_PER_COLLECTION = 2
# Трейдер/whale — только явные сигналы
TRADER_NAME_KW = (
    "whale", "resell", "flipper", "giftbot", "stars market",
    "tonnel", "fragment", "p2p market", "nft trade", "nft market",
)
PRICENFT_BOT = "PriceNFTbot"
PRICENFT_DB_FILE = "pricenft_db.json"   # legacy, мигрируем в sqlite
GIFTS_DB_FILE = "gifts.db"             # быстрая БД (миллионы записей)
PRICENFT_MSG_CD = 3.0
WRITE_MSG = "привет, ты продаешь свой нфт подарок или нет"
_pricenft_collecting = False
_pricenft_stop = False
_pricenft_task = None
_pricenft_entity = None                 # кэш peer — без ResolveUsername
_pricenft_flood_until = 0.0             # unix time, пока нельзя трогать PriceNFT
_bootstrap_task = None
_keeper_task = None
_search_started_at = 0.0
_db_conn = None
_db_lock = None
_db_pending = 0
_DB_COMMIT_EVERY = 200

# ── REGIONS ───────────────────────────────────────────────────────────────────
REGIONS = {
    "any": {"label": "Все страны"},
    "ru":  {"label": "Россия"},
    "ua":  {"label": "Украина"},
    "by":  {"label": "Беларусь"},
    "kz":  {"label": "Казахстан"},
    "uz":  {"label": "Узбекистан"},
    "us":  {"label": "США"},
    "uk":  {"label": "Великобритания"},
    "de":  {"label": "Германия"},
    "fr":  {"label": "Франция"},
    "es":  {"label": "Испания"},
    "it":  {"label": "Италия"},
    "pl":  {"label": "Польша"},
    "tr":  {"label": "Турция"},
    "ae":  {"label": "ОАЭ"},
    "cn":  {"label": "Китай"},
    "jp":  {"label": "Япония"},
    "in":  {"label": "Индия"},
}

RU_LETTERS  = set("абвгдеёжзийклмнопрстуфхцчшщъыьэюяіїєґ")
UK_UA_ONLY  = set("іїєґ")

# Типичные славянские/русские имена латиницей — для быстрого матча региона RU/BY/UA/KZ
CIS_LAT_NAMES = {
    "anna","maria","olga","elena","irina","nata","natasha","tanya","tanya","dasha",
    "masha","katya","anya","alina","arina","karina","milana","polina","ksenia","kseniya",
    "yulia","ulia","victoria","viktoria","valeria","diana","kristina","svetlana","marina",
    "ekaterina","aleksandra","alexandra","sofia","sophia","vera","nina","lara","lada",
    "alex","alexander","alexey","aleksey","andrey","anton","artem","dmitry","dmitri",
    "ivan","igor","ilya","kirill","maxim","mikhail","nikita","oleg","pavel","roman",
    "ruslan","sergey","sergei","timur","vladimir","vlad","yuri","denis","egor","maksim",
    "nastya","nastia","sonya","sonia","lera","vika","ksusha","olya","lena","yana",
    "zhanna","regina","amina","zara","rita","mila","tamara","inna","angelina","veronika",
    "kira","bella","eva","zlata","camilla","kamilla","elizaveta","elizabeth","liza",
}

def _cyr_count(text):
    return sum(1 for c in text.lower() if c in RU_LETTERS)

def _lat_count(text):
    return sum(1 for c in text.lower() if 'a' <= c <= 'z')


def _name_tokens(*parts):
    tokens = set()
    for p in parts:
        if not p:
            continue
        for t in re.split(r"[^a-zа-яёіїєґ]+", str(p).lower()):
            if len(t) >= 2:
                tokens.add(t)
    return tokens


# ── РАСШИРЕННАЯ ПРОВЕРКА РЕГИОНА ──────────────────────────────────────────────
def region_match_full(owner, username, name, region_key, gift_senders_langs=None):
    if not region_key or region_key == "any":
        return True

    uname = (getattr(owner, "username",   "") or "") if owner else (username or "")
    fname = (getattr(owner, "first_name", "") or "") if owner else ""
    lname = (getattr(owner, "last_name",  "") or "") if owner else ""
    bio   = (getattr(owner, "bio",        "") or "") if owner else ""
    if not fname and name:
        parts = name.strip().split()
        fname = parts[0] if parts else ""
        lname = parts[1] if len(parts) > 1 else ""

    full_raw = (uname + " " + fname + " " + lname + " " + bio).strip()
    full     = full_raw.lower()

    senders_text = ""
    if gift_senders_langs:
        senders_text = " ".join(gift_senders_langs).lower()

    combined = full + " " + senders_text
    tokens = _name_tokens(uname, fname, lname, name)

    # Пустой/неизвестный профиль — не режем (иначе маркет даёт 0 результатов)
    if len(full.strip()) < 2:
        return True

    RU_LETTERS_SET = set("абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ")
    UK_UA_ONLY_SET = set("іїєґІЇЄҐ")

    cyr = sum(1 for c in combined if c in RU_LETTERS_SET)
    lat = sum(1 for c in combined if 'a' <= c.lower() <= 'z')
    has_cyr = cyr >= 1
    has_lat = lat >= 2
    has_cis_name = bool(tokens & CIS_LAT_NAMES)

    if region_key in ("ru", "ua", "by", "kz", "uz"):
        ua_chars = sum(1 for c in combined if c in UK_UA_ONLY_SET)
        ua_words = any(k in combined for k in [
            "ukraine","kyiv","київ","харків","одеса","львів","укр","ua_",
            "украин","україн","odessa","kharkiv","lviv","dnipro","zaporiz",
        ])
        by_words = any(k in combined for k in [
            "беларус","минск","белорус","belarus","minsk","gomel","grodno",
            "витебск","брест","могилев","беларускі","мінск",
        ])
        kz_words = any(k in combined for k in [
            "казах","kazakhstan","almaty","алматы","астана","astana","shymkent",
            "нурсултан","nursultan","aqtobe","karaganda","караганд",
        ])
        uz_words = any(k in combined for k in [
            "узбек","uzbekistan","tashkent","ташкент","samarkand","самарканд",
            "bukhara","бухар","namangan","ферган",
        ])
        ru_words = any(k in combined for k in [
            "russia","russian","moscow","москва","спб","spb","питер","petersburg",
            "россия","русск","rf_","_rf","novosibirsk","ekaterinburg","казань","kazan",
            "самар","samara","ростов","rostov","краснодар","krasnodar","sochi","сочи",
            "нижний","nizhniy","челябинск","омск","омск","воронеж","perm","пермь",
        ])

        if region_key == "ua":
            if ua_chars >= 1 or ua_words or (has_cyr and "ua" in combined):
                return True
            return True
        if region_key == "by":
            if by_words or (has_cyr and any(k in combined for k in ["by_", "_by", "бел"])):
                return True
            return True
        if region_key == "kz":
            if kz_words or any(k in combined for k in ["kz_", "_kz"]):
                return True
            return True
        if region_key == "uz":
            if uz_words or any(k in combined for k in ["uz_", "_uz"]):
                return True
            return True
        # Россия: режем явные ua/by/kz/uz; неизвестных пропускаем
        if ua_chars >= 2 or ua_words or by_words or kz_words or uz_words:
            return False
        return True

    de_c = set("äöüÄÖÜß")
    fr_c = set("àâæçéèêëîïôœùûüÿÀÂÆÇÉÈÊËÎÏÔŒÙÛÜŸ")
    es_c = set("áéíóúüñÁÉÍÓÚÜÑ¿¡")
    tr_c = set("ğüşıöçĞÜŞİÖÇ")
    has_de = any(c in de_c for c in full_raw)
    has_fr = any(c in fr_c for c in full_raw)
    has_es = any(c in es_c for c in full_raw)
    has_tr = any(c in tr_c for c in full_raw)

    if region_key == "de":
        if has_de or any(k in combined for k in [
            "berlin","munich","hamburg","frankfurt","deutsch","german","germany",
            "münchen","köln","deutschland","düsseldorf","stuttgart","dortmund",
            "de_", "_de", "österreich","austria","wien","vienna","schweiz","zurich",
        ]):
            return True
        return True
    if region_key == "fr":
        if has_fr or any(k in combined for k in [
            "paris","france","french","lyon","marseille","française","fr_",
            "bordeaux","strasbourg","nantes","toulouse","nice","lille","monaco",
        ]):
            return True
        return True
    if region_key == "es":
        if has_es or any(k in combined for k in [
            "spain","madrid","barcelona","español","españa","mexico","méxico",
            "argentina","colombia","valencia","sevilla","bilbao","latinoam",
            "es_", "chile","peru","perú","venezuela","miami latina",
        ]):
            return True
        return True
    if region_key == "it":
        if any(k in combined for k in [
            "italy","italia","italian","italiano","roma","rome","milan","milano",
            "napoli","naples","torino","florence","firenze","it_", "_it",
        ]):
            return True
        return True
    if region_key == "pl":
        if any(k in combined for k in [
            "poland","polska","polish","warsaw","warszawa","krakow","kraków",
            "wroclaw","gdansk","poznan","pl_", "_pl", "łódź","lodz",
        ]) or any(c in "ąćęłńóśźżĄĆĘŁŃÓŚŹŻ" for c in full_raw):
            return True
        return True
    if region_key == "tr":
        if has_tr or any(k in combined for k in [
            "turkey","istanbul","ankara","türk","türkiye","izmir","turkiye",
            "antalya","bursa","adana","gaziantep","tr_", "_tr",
        ]):
            return True
        return True
    if region_key == "ae":
        ar = sum(1 for c in full_raw if '\u0600' <= c <= '\u06ff')
        if ar >= 2 or any(k in combined for k in [
            "dubai","uae","emirates","sharjah","abu dhabi","abudhabi",
            "ajman","fujairah","ras al","dxb","ae_",
        ]):
            return True
        return True
    if region_key == "cn":
        zh = sum(1 for c in full_raw if '\u4e00' <= c <= '\u9fff')
        if zh >= 1 or any(k in combined for k in [
            "china","chinese","beijing","shanghai","guangzhou","shenzhen","cn_",
        ]):
            return True
        return True
    if region_key == "jp":
        hi = sum(1 for c in full_raw if '\u3040' <= c <= '\u309f')
        ka = sum(1 for c in full_raw if '\u30a0' <= c <= '\u30ff')
        if (hi + ka) >= 1 or any(k in combined for k in [
            "japan","tokyo","osaka","japanese","kyoto","yokohama","nagoya","sapporo","jp_",
        ]):
            return True
        return True
    if region_key == "in":
        dev = sum(1 for c in full_raw if '\u0900' <= c <= '\u097f')
        if dev >= 1 or any(k in combined for k in [
            "india","indian","delhi","mumbai","bangalore","pakistan",
            "bangladesh","chennai","kolkata","hyderabad","pune","ahmedabad","in_",
        ]):
            return True
        return True
    if region_key == "uk":
        if has_de or has_fr or has_es or has_tr:
            return False
        if has_cyr and not has_lat:
            return False
        # не путать с ukraine
        if any(k in combined for k in ["ukraine", "украин", "україн", "kyiv", "київ"]):
            return False
        if any(k in combined for k in [
            "london","britain","british","england","scotland","uk_", "_uk",
            "wales","manchester","liverpool","glasgow","birmingham",
            "leeds","sheffield","newcastle","edinburgh","britain","brit ",
        ]) or (" uk " in (" " + combined + " ")):
            return True
        return True  # неизвестно — не режем
    if region_key == "us":
        if has_de or has_fr or has_es or has_tr:
            return False
        if has_cyr and not has_lat:
            return False
        if any(k in combined for k in [
            "usa","america","american","newyork","nyc","california","us_",
            "texas","miami","chicago","houston","losangeles","new york",
            "los angeles","seattle","boston","denver","atlanta","phoenix",
            "brooklyn","manhattan","dallas","portland","las vegas","lasvegas",
            "florida","usa_",
        ]) or bool(tokens & {
            "jessica","ashley","emily","olivia","ava","isabella","mia","madison",
            "hannah","samantha","chloe","amber","kayla","brooklyn","destiny",
        }):
            return True
        return True  # неизвестно — не режем
    # Нет явных маркеров региона — не блокируем (иначе маркет пустой)
    return True


async def region_match_async(owner, username, name, region_key, uid=None, gift_senders=None):
    return region_match_full(owner, username, name, region_key, gift_senders)

async def is_girl_async(owner, username=None, name=None, uid=None):
    return is_girl(owner, username, name)


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
    "марина","елизавета","ульяна","варвара","снежана","лилия","аделина","дарина",
    "софия","софья","марьяна","ярослава","всеслава","люба","любовь","снежа",
    "ксания","ксеша","настенька","катенька","машенька","дашенька","иришка",
    "анна","аня","анютка","тоня","тоня","ксения","олеся","леся","настя",
    "anna","maria","kate","elena","olga","natasha","tatiana","irina","diana",
    "alina","dasha","masha","vika","lena","anya","yulia","julia","lisa","tanya",
    "sonya","arina","karina","milana","zlata","eva","yana","veronika","kira",
    "stella","bella","nina","tina","vera","sofia","sophia","victoria","kristina",
    "valeria","natalia","angelina","jessica","ashley","emily","olivia","ava",
    "isabella","mia","abigail","madison","elizabeth","taylor","hannah","samantha",
    "lauren","grace","lily","ella","amber","kayla","chloe","jade","ruby","rose",
    "violet","daisy","aurora","aria","luna","scarlett","zoey","penelope","layla",
    "riley","nora","maya","claire","savannah","eleanor","camila","alexa","leah",
    "aubrey","ariana","alice","lana","lola","zara","candy","honey","cherry",
    "nastya","ksenia","kseniya","polina","katya","olya","lera","ksusha",
    "marina","elizaveta","uliana","ulyana","varvara","adelina","darina","olesya",
    "sveta","svetlana","nastia","anastasia","ekaterina","aleksandra","alexandra",
    "daria","darya","viktoria","viktoriya","valeriya","natalya","nataliya",
    "zhanna","regina","amina","mila","rita","liza","sonia","tonya","lesya",
    "kamilla","camilla","evelina","milena","yasmin","lara","lada","mila",
    "dashka","mashka","katusha","yulya","uliya","nastenka","polinka","alenka",
    "karinka",
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
    "egor","maksim","vlad","danil","daniil","petya","serezha","kostya",
}
GIRL_SIGNALS = [
    "girl","lady","woman","she/her","she her","♀",
    "👩","👸","💃","🌸","💖","💕","💗","👄","💄","🌺","🦋","🌷","🌹","💅","🦄","💫","✨","🍑","👑","🎀","💋","❤","❤️","🩷","😻",
    "девушка","женщина","принцесса","королева","богиня",
    "красотка","кошечка","зайка","лапочка","милашка","красавица","малышка",
    "onlyfans","girlfriend",
]
BOY_SIGNAL_WORDS = {
    "bro","guy","male","man","boy","king","boss","dude","lord","sultan",
    "парень","мужик","мужчина","папа","отец","дядя","сын","брат","муж","он",
    "he","him",
}
MALE_NAME_EXCEPTIONS_A = {
    # мужские имена на -a/-я
    "dima","дима","nikita","никита","ilya","илья","ilja","foma","фома",
    "luka","лука","kostya","костя","vanya","ваня","tolya","толя","petya","петя",
    "seryozha","сережа","serezha","misha","миша","sasha","саша","zhenechka",
    "мустафа","mustafa","joshua","cuba","dakota",
}

def is_girl(owner, username=None, name=None):
    """Детект девушки: имена, окончания, эмодзи, username. Без ложных male/king подстрок."""
    bio_raw   = (getattr(owner, "bio",        "") or "") if owner else ""
    uname_raw = (getattr(owner, "username",   "") or "") if owner else (username or "")
    fname_raw = (getattr(owner, "first_name", "") or "") if owner else ""
    lname_raw = (getattr(owner, "last_name",  "") or "") if owner else ""
    if not fname_raw and name:
        parts = str(name).strip().split()
        fname_raw = parts[0] if parts else ""
        lname_raw = parts[1] if len(parts) > 1 else ""

    def _clean(s):
        s = (s or "").lower().strip()
        s = re.sub(r"[0-9_./\\|+\-]+", " ", s)
        s = re.sub(r"[^\w\sа-яёіїєґa-z]", " ", s, flags=re.IGNORECASE)
        return re.sub(r"\s+", " ", s).strip()

    uname  = (uname_raw or "").lower()
    fname  = _clean(fname_raw)
    lname  = _clean(lname_raw)
    fname0 = fname.split()[0] if fname else ""
    tokens = _name_tokens(fname, lname, uname, name, fname0)
    full   = ((bio_raw or "").lower() + " " + uname + " " + fname + " " + lname).strip()
    full_tokens = set(re.findall(r"[a-zа-яёіїєґ]+", full))

    # Женские маркеры заранее — чтобы не срезать female/girl
    has_girl_kw = any(x in full for x in ("girl", "female", "woman", "lady", "девушка", "женщина", "she/her"))

    # Мужские слова только как целые токены
    if (full_tokens & BOY_SIGNAL_WORDS) and not has_girl_kw:
        if fname0 not in GIRL_NAMES_SET and not (tokens & GIRL_NAMES_SET):
            return False

    # Точное мужское имя
    if fname0 in BOY_NAMES_SET and fname0 not in GIRL_NAMES_SET:
        return False
    if (tokens & BOY_NAMES_SET) and not (tokens & GIRL_NAMES_SET) and fname0 not in GIRL_NAMES_SET:
        # если токен мужской и нет женского
        if any(t in BOY_NAMES_SET and t not in GIRL_NAMES_SET for t in tokens):
            return False

    score = 0

    # Известное женское имя
    if fname0 in GIRL_NAMES_SET or (tokens & GIRL_NAMES_SET):
        score += 3
    else:
        for gn in GIRL_NAMES_SET:
            if len(gn) >= 3 and fname0 and len(fname0) >= 3 and (fname0.startswith(gn) or gn.startswith(fname0)):
                score += 3
                break

    # Окончания имён
    GIRL_ENDINGS = ("на","ья","ия","ая","яя","га","за","са","ша","ча","жа","ца","ка","ла","ва","ня","ся","та","ра","да","ма","па")
    LAT_GIRL_ENDINGS = ("ia","ya","na","ra","la","sa","ta","ka","va","ina","ella","ette","elle","ine","lyn","ey","ie")
    if fname0 and len(fname0) >= 3:
        if any(fname0.endswith(e) for e in GIRL_ENDINGS):
            score += 2
        elif any(fname0.endswith(e) for e in LAT_GIRL_ENDINGS):
            score += 2
        # эвристика: имя на -a/-я (кроме мужских исключений)
        elif fname0[-1] in ("a", "а", "я", "я") and fname0 not in MALE_NAME_EXCEPTIONS_A:
            if fname0 not in BOY_NAMES_SET:
                score += 1

    # Эмодзи / сигналы
    for sig in GIRL_SIGNALS:
        if sig and (sig in full or sig in (bio_raw or "") or sig in uname_raw or sig in fname_raw):
            score += 2
            break
    GIRL_CHARS = set("💅👩👸💃🌸💖💕💗👄💄🌺🦋🌷🌹🦄💫✨🍑👑♀🎀❤😻💋🩷❤️💕💞💘💝")
    if any(ch in (bio_raw or "") or ch in uname_raw or ch in fname_raw for ch in GIRL_CHARS):
        score += 2

    # username
    if any(x in uname for x in ("girl", "lady", "miss", "princess", "queen", "babe", "fem", "wife")):
        score += 2
    for gn in GIRL_NAMES_SET:
        if len(gn) >= 4 and gn in uname:
            score += 2
            break

    return score >= 1 and bool(fname0 or uname)


# ── MODEL DETECTION ───────────────────────────────────────────────────────────
# Модельный поиск не требует is_model — ищем всех владельцев NFT (с фильтром девушки если надо)
# is_model используется опционально для дополнительного ранжирования
MODEL_KW = [
    "onlyfans","only fans","of.com","fansly","fanvue","nsfw","18+",
    "model","модель","content creator","blogger","блогер","influencer",
    "adult","vip content","premium","link in bio","linktr","linktree",
    "sexy","babe","goddess","spicy","naughty",
    "фотомодель","контент","взрослый контент","для взрослых",
    "фото","photo","pics","subscribe","подпишись","creator",
]
MODEL_EMOJI = ["💋","🔥","👄","💦","🍑","💎","🌟","⭐","✨","💫","🦋","💅","👑","🌸"]

def is_model(owner, username=None, name=None):
    bio   = (getattr(owner, "bio",        "") or "").lower() if owner else ""
    uname = (getattr(owner, "username",   "") or "").lower() if owner else (username or "").lower()
    fname = (getattr(owner, "first_name", "") or "").lower() if owner else ""
    lname = (getattr(owner, "last_name",  "") or "").lower() if owner else ""
    if not fname and name:
        parts = name.lower().split()
        fname = parts[0] if parts else ""
    full = (bio + " " + uname + " " + fname + " " + lname).strip()
    raw_bio   = (getattr(owner, "bio",      "") or "") if owner else ""
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
    limit     = State()
    region    = State()

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
    uid = getattr(obj, "user_id", None) or getattr(obj, "channel_id", None) or getattr(obj, "id", None)
    if uid is None and isinstance(obj, int):
        uid = obj
    if uid is None:
        return None, None
    try:
        uid = int(uid)
    except Exception:
        return None, None
    return users_map.get(uid), uid

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

def price_in_cat(price, cat):
    """Фильтр по цене лота (не по floor коллекции) — иначе маркет пустой."""
    if not cat or cat in ("any", "all", "none"):
        return True
    if price is None:
        return True
    try:
        price = int(price)
    except Exception:
        return True
    CATS = {
        "cheap":   (0,      2000),
        "mid":     (2000,   5000),
        "hard":    (5000,   20000),
        "ultra":   (20000,  100000),
        "extreme": (100000, 10**12),
    }
    c = CATS.get(cat)
    if not c:
        return True
    mn, mx = c
    return mn <= price <= mx

def price_ok(price, floor, boost):
    if not price or not floor:
        return True
    # Шире окно — иначе свежие лоты часто отсекаются
    lo = floor * 0.5
    hi = floor * (1.0 + max(boost, 50) / 100.0) * 1.5
    return lo <= price <= hi

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
async def safe_edit(msg, text, reply_markup=None):
    """Надёжное обновление сообщения — кнопки всегда остаются кликабельными."""
    try:
        await msg.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
        return True
    except Exception:
        try:
            await msg.answer(text, parse_mode="HTML", reply_markup=reply_markup)
            return True
        except Exception:
            return False

def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск",      callback_data="search_mode_select")],
        [InlineKeyboardButton(text="⚙️ Настройки",  callback_data="settings_menu"),
         InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
    ])

def search_mode_select_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 По маркету",  callback_data="mode_market")],
        [InlineKeyboardButton(text="👤 По профилю",  callback_data="mode_profile")],
        [InlineKeyboardButton(text="⭐ По модели",   callback_data="mode_model")],
        [InlineKeyboardButton(text="⬅️ Назад",       callback_data="menu")],
    ])

def cat_kb(mode):
    if mode == "market":
        p = "mc_"
    elif mode == "profile":
        p = "pc_"
    else:
        p = "mm_"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Дешевые до 2000",    callback_data=p+"cheap")],
        [InlineKeyboardButton(text="💎 Средние 2000-5000",  callback_data=p+"mid")],
        [InlineKeyboardButton(text="🔥 Сложные 5000-20000", callback_data=p+"hard")],
        [InlineKeyboardButton(text="⚡️ Хард 20000-100000",  callback_data=p+"ultra")],
        [InlineKeyboardButton(text="🚀 Экстрим от 100000",  callback_data=p+"extreme")],
        [InlineKeyboardButton(text="⬅️ Назад",              callback_data="search_mode_select")],
    ])

def who_kb(mode, cat):
    back = "mode_" + mode
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Всех",      callback_data="go_" + mode + "_" + cat + "_all")],
        [InlineKeyboardButton(text="👩 Девушек",   callback_data="go_" + mode + "_" + cat + "_girls")],
        [InlineKeyboardButton(text="⬅️ Назад",     callback_data=back)],
    ])

def who_model_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Всех моделей",   callback_data="mdl_who_all")],
        [InlineKeyboardButton(text="👩 Только девушек", callback_data="mdl_who_girls")],
        [InlineKeyboardButton(text="⬅️ Назад",          callback_data="search_mode_select")],
    ])

def model_search_type_kb():
    """Выбор режима поиска моделей: по маркету или по профилю."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 По маркету",  callback_data="mdltype_market")],
        [InlineKeyboardButton(text="👤 По профилю",  callback_data="mdltype_profile")],
        [InlineKeyboardButton(text="⬅️ Назад",       callback_data="search_mode_select")],
    ])

def model_who_kb(search_type):
    """Выбор кого искать (все/девушки) для модели."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Всех",           callback_data="mdlwho_" + search_type + "_all")],
        [InlineKeyboardButton(text="👩 Только девушек", callback_data="mdlwho_" + search_type + "_girls")],
        [InlineKeyboardButton(text="⬅️ Назад",          callback_data="mode_model")],
    ])

COL_PAGE_SIZE = 40  # 2 колонки × 20 рядов + служебльные кнопки < 100

def model_col_kb(who, search_type, collections, page=0):
    """Кнопки коллекций с пагинацией (лимит Telegram — 100 кнопок)."""
    cols = list(collections)
    total_pages = max(1, (len(cols) + COL_PAGE_SIZE - 1) // COL_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * COL_PAGE_SIZE
    chunk = cols[start:start + COL_PAGE_SIZE]

    rows = []
    row = []
    for gid, title in chunk:
        lbl = str(title) if title else "NFT"
        if len(lbl) > 18:
            lbl = lbl[:16] + ".."
        # короткий callback чтобы не превысить 64 байта
        row.append(InlineKeyboardButton(
            text=lbl,
            callback_data="mdlrun_" + who[0] + "_" + search_type[0] + "_" + str(gid)
        ))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(
        text="📦 Все коллекции",
        callback_data="mdlrun_" + who[0] + "_" + search_type[0] + "_all"
    )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(
            text="⬅️",
            callback_data="mdlpage_" + who[0] + "_" + search_type[0] + "_" + str(page - 1)
        ))
    if total_pages > 1:
        nav.append(InlineKeyboardButton(
            text=str(page + 1) + "/" + str(total_pages),
            callback_data="noop"
        ))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton(
            text="➡️",
            callback_data="mdlpage_" + who[0] + "_" + search_type[0] + "_" + str(page + 1)
        ))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(
        text="⬅️ Назад",
        callback_data="mdlwho_" + search_type + "_" + who
    )])
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
        [InlineKeyboardButton(text="🌍 Регион: " + reg_lbl,     callback_data="set_region")],
        [InlineKeyboardButton(text="⬅️ Назад",                  callback_data="menu")],
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
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings_menu")],
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
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="settings_menu")],
    ])

def region_kb(current="any"):
    rows = []
    items = list(REGIONS.items())
    for i in range(0, len(items), 2):
        row = []
        for key, val in items[i:i+2]:
            lbl = ("✅ " if key == current else "") + val["label"]
            row.append(InlineKeyboardButton(text=lbl, callback_data="reg_" + key))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="settings_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ СТОП", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск", callback_data="search_mode_select")],
        [InlineKeyboardButton(text="🏠 Меню",  callback_data="menu")],
    ])

def _expand_who(ch):
    return "girls" if ch in ("g", "girls") else "all"

def _expand_stype(ch):
    return "profile" if ch in ("p", "profile") else "market"

def owner_card_kb(username, profile_url, owner_uid, nft_url_for_msg=None, nft_count=0):
    btns = []
    if username:
        btns.append([InlineKeyboardButton(text="@" + username, url="https://t.me/" + username)])
        msg = urllib.parse.quote(WRITE_MSG)
        btns.append([InlineKeyboardButton(text="Написать", url="https://t.me/" + username + "?text=" + msg)])
    elif profile_url:
        btns.append([InlineKeyboardButton(text="Профиль", url=profile_url)])
    btns.append([InlineKeyboardButton(text="Все NFT", callback_data="shownft_" + str(owner_uid))])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def model_card_kb(username, profile_url, owner_uid, nft_url, nft_count=1):
    btns = []
    if username:
        btns.append([InlineKeyboardButton(text="@" + username, url="https://t.me/" + username)])
        msg = urllib.parse.quote(WRITE_MSG)
        btns.append([InlineKeyboardButton(text="Написать", url="https://t.me/" + username + "?text=" + msg)])
    elif profile_url:
        btns.append([InlineKeyboardButton(text="Профиль", url=profile_url)])
    if nft_url:
        btns.append([InlineKeyboardButton(text="NFT", url=nft_url)])
    if owner_uid:
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
        msg = urllib.parse.quote(WRITE_MSG)
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
    st = {}
    try:
        st = pricenft_db_stats()
    except Exception:
        st = {"models": 0, "users": 0}
    users_n = int(st.get("users", 0) or 0)
    seen_n = int(st.get("seen_owners", 0) or 0)
    if _pricenft_collecting:
        db_btn = InlineKeyboardButton(
            text="⏹ Стоп сбор БД (" + str(users_n) + "/" + str(DB_TARGET_USERS) + ")",
            callback_data="admin_pricenft_stop",
        )
    else:
        db_btn = InlineKeyboardButton(
            text="📦 База данных (" + str(users_n) + " / " + str(DB_TARGET_USERS) + ")",
            callback_data="admin_pricenft_collect",
        )
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Рассылка",           callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="Пользователи",       callback_data="admin_users")],
        [InlineKeyboardButton(text="Статистика",         callback_data="admin_stats")],
        [InlineKeyboardButton(text="Авторизация TG",     callback_data="admin_auth")],
        [InlineKeyboardButton(text="Обновить коллекции", callback_data="admin_reload_cols")],
        [db_btn],
        [InlineKeyboardButton(text="🧹 Сброс антидубля (" + str(seen_n) + ")", callback_data="admin_clear_seen")],
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
def _gift_on_market(gift, inner=None):
    """Гифт выставлен на маркет: resell_amount / resale_ton_only у unique gift."""
    if inner is None:
        inner = getattr(gift, "gift", None)
    for obj in (gift, inner):
        if obj is None:
            continue
        # Флаг «только за TON» ставится только у лотов на ресейле
        if getattr(obj, "resale_ton_only", False):
            return True
        # Цена ресейла (StarsAmount или Vector<StarsAmount>)
        if get_resell_price(obj) is not None:
            return True
        for attr in ("resell_amount", "resale_amount", "resale_stars", "resell_stars"):
            val = getattr(obj, attr, None)
            if not val:
                continue
            if isinstance(val, (list, tuple)):
                if len(val) > 0:
                    return True
            else:
                amt = getattr(val, "amount", None)
                if amt is not None:
                    try:
                        if int(amt) > 0:
                            return True
                    except Exception:
                        return True
                else:
                    try:
                        if int(val) > 0:
                            return True
                    except Exception:
                        return True
    return False


def begin_search():
    """Старт поиска. Антидубль НЕ сбрасываем — люди/гифты не повторяются."""
    global is_searching, _search_started_at
    now = time.time()
    if is_searching and _search_started_at and (now - _search_started_at) > 720:
        logger.warning("force-reset stuck is_searching")
        is_searching = False
    if is_searching:
        return False
    is_searching = True
    _search_started_at = now
    return True

def owner_seen_key(uid=None, username=None):
    if uid:
        return "id:" + str(int(uid))
    if username:
        return "u:" + str(username).lstrip("@").lower()
    return None

def gift_slug_of(nft_url_or_slug):
    if not nft_url_or_slug:
        return None
    s = str(nft_url_or_slug)
    if "/" in s:
        s = s.rstrip("/").split("/")[-1]
    s = s.strip()
    return s or None

def is_owner_seen(uid=None, username=None):
    k = owner_seen_key(uid, username)
    return bool(k and k in SEEN_GLOBAL)

def is_gift_seen(nft_url_or_slug):
    slug = gift_slug_of(nft_url_or_slug)
    return bool(slug and slug in SEEN_GIFTS)

def mark_seen(uid=None, username=None, nft_url=None):
    """Пометить аккаунт и гифт как выданные (память + sqlite)."""
    global SEEN_GLOBAL, SEEN_GIFTS
    keys = []
    k = owner_seen_key(uid, username)
    if k and k not in SEEN_GLOBAL:
        SEEN_GLOBAL.add(k)
        keys.append(k)
    slug = gift_slug_of(nft_url)
    new_slug = None
    if slug and slug not in SEEN_GIFTS:
        SEEN_GIFTS.add(slug)
        new_slug = slug
    # persist async-ish (sync fast insert)
    try:
        db_mark_seen(keys, new_slug)
    except Exception:
        pass
    # защита от раздувания RAM
    if len(SEEN_GLOBAL) > SEEN_GLOBAL_MAX:
        # оставляем половину самых новых через sqlite reload
        pass

def is_trader_account(owner, username=None, name=None):
    """Отсекаем трейдерские/whale аккаунты с «высоким рейтингом» торговли."""
    uname = ((getattr(owner, "username", None) if owner else None) or username or "").lower()
    fname = ((getattr(owner, "first_name", None) if owner else None) or "")
    lname = ((getattr(owner, "last_name", None) if owner else None) or "")
    if not fname and name:
        fname = str(name)
    full = (uname + " " + fname + " " + lname).lower()
    if any(k in full for k in TRADER_NAME_KW):
        return True
    # много цифр в username — часто боты/фарм
    if uname and sum(c.isdigit() for c in uname) >= 5 and any(k in uname for k in ("nft", "gift", "star", "ton")):
        return True
    return False

async def get_floor(gid):
    if gid in PRICE_FLOOR_CACHE:
        return PRICE_FLOOR_CACHE[gid]
    try:
        # для флора сортируем по цене
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gid, offset="", limit=20, sort_by_price=True
        ))
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

async def fetch_market_page(gid, offset, limit=100, newest=True):
    """
    Страница маркета.
    newest=True  — свежие лоты (по умолчанию API сортирует по дате изменения цены desc)
    newest=False — сортировка по цене (для флора и т.п.)
    """
    for _ in range(2):
        try:
            kwargs = dict(gift_id=gid, offset=offset, limit=limit)
            if not newest:
                kwargs["sort_by_price"] = True
            result    = await tg_client(GetResaleStarGiftsRequest(**kwargs))
            users_map = {int(u.id): u for u in (getattr(result, "users", None) or [])}
            col_title = next((t for t, i in NFT_COLLECTIONS.items() if i == gid), None)
            items     = []
            for gift in (getattr(result, "gifts", None) or []):
                owner, oid = get_owner(gift, users_map)
                username   = getattr(owner, "username", None) if owner else None
                fn = (getattr(owner, "first_name", "") or "") if owner else ""
                ln = (getattr(owner, "last_name",  "") or "") if owner else ""
                name = (fn + " " + ln).strip()
                # fallback: owner_name с маркета если нет User-объекта
                if not name:
                    name = (getattr(gift, "owner_name", None) or "") or ""
                nft_url = make_nft_url(gift)
                profile_url = ("https://t.me/" + username) if username else (("tg://user?id=" + str(oid)) if oid else None)
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
                    "gift_id": gid,
                })
            return items, getattr(result, "next_offset", "") or ""
        except FloodWaitError as e:
            await asyncio.sleep(max(min(e.seconds, 8), 1))
        except Exception as e:
            logger.error("fetch_market gid=%s: %s", gid, e)
            return [], ""
    return [], ""

async def fetch_saved_gifts(uid_or_username, max_pages=2, only_off_market=False, require_zero_on_market=False):
    """
    Загружает сохранённые гифты по uid или username.
    only_off_market=True — в результат только НЕ на маркете.
    require_zero_on_market=True — если хоть 1 гифт на маркете, вернуть None (профиль отбракован).
    """
    all_items = []
    seen_slugs = set()
    offset    = ""
    pages = max(max_pages, 2) if require_zero_on_market else max_pages
    try:
        peer = await tg_client.get_input_entity(uid_or_username)
    except Exception as e:
        logger.debug("saved_gifts peer=%s: %s", uid_or_username, e)
        return []
    for _ in range(pages):
        try:
            result = await tg_client(GetSavedStarGiftsRequest(
                peer=peer,
                offset=offset, limit=100,
                exclude_unlimited=True,  # только уникальные NFT
            ))
            for gift in (getattr(result, "gifts", None) or []):
                nft_url   = make_nft_url(gift)
                inner     = getattr(gift, "gift", None)
                if not nft_url and inner:
                    nft_url = make_nft_url(inner)
                slug = nft_url.split("/")[-1] if nft_url else (getattr(inner, "slug", None) or "")
                if slug and slug in seen_slugs:
                    continue
                if slug:
                    seen_slugs.add(slug)
                title     = (getattr(inner, "title", None) or getattr(gift, "title", None) or "NFT")
                num       = getattr(gift, "num", None) or getattr(gift, "gift_num", None) or getattr(inner, "num", "?")
                on_market = _gift_on_market(gift, inner)
                # Строго: любой лот на маркете = профиль не подходит
                if on_market and require_zero_on_market:
                    return None
                if only_off_market and on_market:
                    continue
                if not nft_url and not getattr(inner, "slug", None):
                    if only_off_market or require_zero_on_market:
                        continue
                all_items.append({
                    "title": str(title), "num": num,
                    "nft_url": nft_url, "on_market": on_market,
                })
            offset = getattr(result, "next_offset", "") or ""
            if not offset:
                break
        except FloodWaitError as e:
            await asyncio.sleep(min(e.seconds, 2))
            break
        except Exception as e:
            logger.debug("saved_gifts uid=%s: %s", uid_or_username, e)
            break
    return all_items



# ── GIFTS DB (SQLite, быстрая, миллионы записей) + PriceNFTbot ─────────────────
_pricenft_lock = asyncio.Lock()
_pricenft_last_send = 0.0

def _db():
    """Один shared connection, WAL, быстрые вставки."""
    global _db_conn, _db_lock
    if _db_lock is None:
        _db_lock = asyncio.Lock() if False else None  # sync lock via threading not needed; sqlite check_same_thread=False
    if _db_conn is not None:
        return _db_conn
    conn = sqlite3.connect(GIFTS_DB_FILE, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-64000")  # ~64MB
    conn.execute("PRAGMA mmap_size=268435456")
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS gifts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        slug TEXT UNIQUE,
        url TEXT,
        model TEXT,
        username TEXT,
        uid INTEGER,
        ts INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_gifts_model ON gifts(model);
    CREATE INDEX IF NOT EXISTS idx_gifts_user ON gifts(username);
    CREATE INDEX IF NOT EXISTS idx_gifts_model_id ON gifts(model, id);

    CREATE TABLE IF NOT EXISTS seen_owners (
        key TEXT PRIMARY KEY,
        ts INTEGER
    );
    CREATE TABLE IF NOT EXISTS seen_gifts (
        slug TEXT PRIMARY KEY,
        ts INTEGER
    );

    CREATE TABLE IF NOT EXISTS meta (
        k TEXT PRIMARY KEY,
        v TEXT
    );
    """)
    conn.commit()
    _db_conn = conn
    return _db_conn

def db_mark_seen(owner_keys, slug=None):
    conn = _db()
    now = int(time.time())
    if owner_keys:
        conn.executemany(
            "INSERT OR IGNORE INTO seen_owners(key, ts) VALUES (?, ?)",
            [(k, now) for k in owner_keys],
        )
    if slug:
        conn.execute("INSERT OR IGNORE INTO seen_gifts(slug, ts) VALUES (?, ?)", (slug, now))
    conn.commit()

def load_seen_into_memory():
    """Быстрая загрузка антидубля в RAM при старте."""
    global SEEN_GLOBAL, SEEN_GIFTS
    conn = _db()
    SEEN_GLOBAL = {r[0] for r in conn.execute("SELECT key FROM seen_owners")}
    SEEN_GIFTS = {r[0] for r in conn.execute("SELECT slug FROM seen_gifts")}
    logger.info("Seen loaded: owners=%s gifts=%s", len(SEEN_GLOBAL), len(SEEN_GIFTS))

def clear_seen_db():
    global SEEN_GLOBAL, SEEN_GIFTS
    conn = _db()
    conn.execute("DELETE FROM seen_owners")
    conn.execute("DELETE FROM seen_gifts")
    conn.commit()
    SEEN_GLOBAL.clear()
    SEEN_GIFTS.clear()

def load_pricenft_db(force=False):
    """Инициализация sqlite (+миграция со старого json один раз)."""
    conn = _db()
    # migrate legacy json once
    try:
        row = conn.execute("SELECT v FROM meta WHERE k='json_migrated'").fetchone()
        if not row and os.path.exists(PRICENFT_DB_FILE):
            with open(PRICENFT_DB_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            models = (data or {}).get("models") or {}
            batch = []
            now = int(time.time())
            for model, payload in models.items():
                users = (payload or {}).get("users") or {}
                for lu, info in users.items():
                    uname = (info or {}).get("username") or lu
                    for url in ((info or {}).get("nft_urls") or [None]):
                        slug = gift_slug_of(url) if url else None
                        if not slug:
                            # synthetic slug per user+model to keep row
                            slug = ("user:" + str(uname).lower() + ":" + str(model))[:120]
                            url = None
                        batch.append((slug, url, str(model), str(uname).lstrip("@"), None, now))
            if batch:
                conn.executemany(
                    "INSERT OR IGNORE INTO gifts(slug, url, model, username, uid, ts) VALUES (?,?,?,?,?,?)",
                    batch,
                )
                conn.commit()
            conn.execute("INSERT OR REPLACE INTO meta(k,v) VALUES ('json_migrated','1')")
            conn.commit()
            logger.info("Migrated JSON -> SQLite: %s rows", len(batch))
    except Exception as e:
        logger.warning("json migrate: %s", e)
    return True

def save_pricenft_db():
    """SQLite уже на диске; делаем checkpoint WAL."""
    try:
        conn = _db()
        conn.commit()
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
    except Exception as e:
        logger.warning("save_pricenft_db: %s", e)

def pricenft_db_stats():
    try:
        conn = _db()
        models = conn.execute("SELECT COUNT(DISTINCT model) FROM gifts").fetchone()[0] or 0
        users = conn.execute("SELECT COUNT(DISTINCT lower(username)) FROM gifts WHERE username IS NOT NULL AND username!=''").fetchone()[0] or 0
        nfts = conn.execute("SELECT COUNT(*) FROM gifts").fetchone()[0] or 0
        return {
            "models": int(models),
            "users": int(users),
            "nfts": int(nfts),
            "updated_at": int(time.time()),
            "seen_owners": len(SEEN_GLOBAL),
            "seen_gifts": len(SEEN_GIFTS),
        }
    except Exception as e:
        logger.warning("pricenft_db_stats: %s", e)
        return {"models": 0, "users": 0, "nfts": 0, "updated_at": 0, "seen_owners": 0, "seen_gifts": 0}

def db_flush(force=False):
    """Коммит пачки вставок в sqlite."""
    global _db_pending
    if not force and _db_pending <= 0:
        return
    try:
        conn = _db()
        conn.commit()
        _db_pending = 0
    except Exception as e:
        logger.debug("db_flush: %s", e)

def pricenft_add_user(model, username, nft_urls=None, uid=None, commit=True):
    """Добавить юзера/гифты в sqlite (очень быстро, пачками)."""
    global _db_pending
    if not model or not username:
        return False
    model = str(model).strip()
    username = str(username).lstrip("@").strip()
    if not model or not username:
        return False
    urls = [u for u in (nft_urls or []) if u]
    now = int(time.time())
    rows = []
    if urls:
        for url in urls[:20]:
            slug = gift_slug_of(url)
            if not slug:
                continue
            rows.append((slug, url, model, username, uid, now))
    else:
        slug = ("user:" + username.lower() + ":" + model)[:120]
        rows.append((slug, None, model, username, uid, now))
    if not rows:
        return False
    try:
        conn = _db()
        conn.executemany(
            "INSERT OR IGNORE INTO gifts(slug, url, model, username, uid, ts) VALUES (?,?,?,?,?,?)",
            rows,
        )
        _db_pending += len(rows)
        if commit or _db_pending >= _DB_COMMIT_EVERY:
            db_flush(force=True)
        return True
    except Exception as e:
        logger.debug("pricenft_add_user: %s", e)
        return False

def seed_pricenft_from_item(item, commit=False):
    """Пассивно наполняем БД из маркета/модели (без commit на каждую строку)."""
    try:
        uname = (item.get("username") or "").lstrip("@")
        title = item.get("title") or item.get("model") or ""
        nft_url = item.get("nft_url")
        uid = item.get("owner_id")
        if not uname or not title or str(title) in ("?", "NFT", "None"):
            return
        pricenft_add_user(
            str(title), uname, [nft_url] if nft_url else None,
            uid=uid, commit=commit,
        )
    except Exception:
        pass

def random_from_pricenft_db(limit=25, model=None):
    """
    Быстрый рандом из sqlite.
    Разные модели / без уже виденных аккаунтов и гифтов.
    """
    load_pricenft_db(force=False)
    conn = _db()
    results = []
    seen_u = set()
    try:
        if model:
            # точное/like
            rows = conn.execute(
                "SELECT slug, url, model, username, uid FROM gifts "
                "WHERE model LIKE ? ORDER BY RANDOM() LIMIT ?",
                ("%" + str(model) + "%", max(limit * 12, 120)),
            ).fetchall()
            if not rows:
                m2 = str(model).replace(" ", "")
                rows = conn.execute(
                    "SELECT slug, url, model, username, uid FROM gifts "
                    "WHERE REPLACE(model,' ','') LIKE ? ORDER BY RANDOM() LIMIT ?",
                    ("%" + m2 + "%", max(limit * 12, 120)),
                ).fetchall()
        else:
            # много разных моделей, по 1-2 юзера — цикл разнообразия
            models = [r[0] for r in conn.execute(
                "SELECT DISTINCT model FROM gifts ORDER BY RANDOM() LIMIT ?",
                (max(limit * 3, 100),),
            ).fetchall()]
            rows = []
            for mk in models:
                part = conn.execute(
                    "SELECT slug, url, model, username, uid FROM gifts "
                    "WHERE model=? ORDER BY RANDOM() LIMIT 2",
                    (mk,),
                ).fetchall()
                rows.extend(part)
            random.shuffle(rows)

        for slug, url, mk, uname, uid in rows:
            if not uname:
                continue
            lu = uname.lower()
            if lu in seen_u:
                continue
            if is_owner_seen(uid, uname):
                continue
            if slug and is_gift_seen(slug):
                continue
            if slug and str(slug).startswith("user:"):
                url = None
            seen_u.add(lu)
            results.append({
                "owner": None, "owner_id": uid,
                "username": uname, "name": uname,
                "nft_url": url,
                "profile_url": "https://t.me/" + uname,
                "title": mk, "model": mk,
                "price": None, "gift_id": None,
                "source": "pricenft_db",
            })
            if len(results) >= limit:
                break
    except Exception as e:
        logger.warning("random_from_pricenft_db: %s", e)
    return results

def random_users_from_db(limit=400):
    """Случайные уникальные username из БД для профиль-поиска."""
    load_pricenft_db(force=False)
    out = []
    try:
        conn = _db()
        rows = conn.execute(
            "SELECT username, uid, model, url, slug FROM gifts "
            "WHERE username IS NOT NULL AND username!='' "
            "ORDER BY RANDOM() LIMIT ?",
            (max(limit * 4, 800),),
        ).fetchall()
        seen = set()
        for uname, uid, model, url, slug in rows:
            lu = str(uname).lstrip("@").lower()
            if not lu or lu in seen:
                continue
            if is_owner_seen(uid, uname):
                continue
            seen.add(lu)
            out.append({
                "username": str(uname).lstrip("@"),
                "owner_id": uid,
                "model": model,
                "nft_url": url if url else None,
                "profile_url": "https://t.me/" + str(uname).lstrip("@"),
            })
            if len(out) >= limit:
                break
    except Exception as e:
        logger.warning("random_users_from_db: %s", e)
    return out

def _parse_pricenft_text(text):
    """Достаём @username и nft-ссылки из ответа PriceNFTbot."""
    text = text or ""
    users = []
    nfts = []
    for m in re.finditer(r"@([A-Za-z][A-Za-z0-9_]{3,})", text):
        u = m.group(1)
        if u.lower() in ("pricenftbot", "gift_alerts", "tonnel_network_bot", "username", "telegram"):
            continue
        users.append(u)
    for m in re.finditer(r"(?:https?://)?t\.me/nft/([A-Za-z0-9_\-]+)", text, re.I):
        nfts.append("https://t.me/nft/" + m.group(1))
    seen_u, out_u = set(), []
    for u in users:
        lu = u.lower()
        if lu not in seen_u:
            seen_u.add(lu)
            out_u.append(u)
    seen_n, out_n = set(), []
    for n in nfts:
        if n not in seen_n:
            seen_n.add(n)
            out_n.append(n)
    return out_u, out_n

async def _pricenft_wait_cd():
    """CD 3 сек перед каждым сообщением к PriceNFTbot."""
    global _pricenft_last_send
    now = time.time()
    wait = PRICENFT_MSG_CD - (now - _pricenft_last_send)
    if wait > 0:
        await asyncio.sleep(wait)
    _pricenft_last_send = time.time()

def _pricenft_flood_active():
    return time.time() < float(_pricenft_flood_until or 0)

def _set_pricenft_flood(seconds):
    """Запомнить FloodWait, не резолвить username снова часами."""
    global _pricenft_flood_until
    sec = int(max(0, seconds or 0))
    # не ждём 20 часов в UI — просто блокируем PriceNFT до этого момента
    _pricenft_flood_until = time.time() + sec
    try:
        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES ('pricenft_flood_until', ?)",
            (str(int(_pricenft_flood_until)),),
        )
        conn.commit()
    except Exception:
        pass
    logger.warning("PriceNFT flood until +%ss (~%.1fh)", sec, sec / 3600.0)

def _load_pricenft_flood():
    global _pricenft_flood_until
    try:
        row = _db().execute("SELECT v FROM meta WHERE k='pricenft_flood_until'").fetchone()
        if row:
            _pricenft_flood_until = float(row[0] or 0)
    except Exception:
        pass

def _save_pricenft_peer_id(ent_or_uid, access_hash=None):
    """Сохраняем id+access_hash, чтобы не вызывать ResolveUsername снова."""
    try:
        uid = None
        ah = access_hash
        if hasattr(ent_or_uid, "id") or hasattr(ent_or_uid, "user_id"):
            ent = ent_or_uid
            uid = getattr(ent, "user_id", None) or getattr(ent, "id", None)
            if ah is None:
                ah = getattr(ent, "access_hash", None)
        else:
            uid = int(ent_or_uid)
        if not uid:
            return
        val = str(int(uid)) + ((":" + str(int(ah))) if ah is not None else "")
        conn = _db()
        conn.execute(
            "INSERT OR REPLACE INTO meta(k,v) VALUES ('pricenft_peer_id', ?)",
            (val,),
        )
        conn.commit()
    except Exception:
        pass

class PriceNftFloodError(Exception):
    def __init__(self, seconds=0):
        self.seconds = int(seconds or 0)
        super().__init__("PriceNFT FloodWait " + str(self.seconds) + "s")

async def _pricenft_peer():
    """
    Peer @PriceNFTbot БЕЗ повторного ResolveUsernameRequest.
    1) RAM-кэш  2) id+hash из sqlite  3) диалоги  4) один resolve
    """
    global _pricenft_entity
    if _pricenft_entity is not None:
        return _pricenft_entity
    if _pricenft_flood_active():
        left = max(1, int(_pricenft_flood_until - time.time()))
        raise PriceNftFloodError(left)

    # 1) id+hash из БД — InputPeerUser без ResolveUsername
    try:
        from telethon.tl.types import InputPeerUser
        row = _db().execute("SELECT v FROM meta WHERE k='pricenft_peer_id'").fetchone()
        if row and row[0]:
            parts = str(row[0]).split(":")
            uid = int(parts[0])
            if len(parts) >= 2 and parts[1]:
                ah = int(parts[1])
                _pricenft_entity = InputPeerUser(uid, ah)
                return _pricenft_entity
            try:
                _pricenft_entity = await tg_client.get_input_entity(uid)
                return _pricenft_entity
            except Exception:
                pass
    except FloodWaitError as e:
        _set_pricenft_flood(getattr(e, "seconds", 0) or 0)
        raise PriceNftFloodError(getattr(e, "seconds", 0) or 0)
    except Exception:
        pass

    # 2) уже открытый диалог
    try:
        async for d in tg_client.iter_dialogs(limit=80):
            ent = getattr(d, "entity", None)
            uname = (getattr(ent, "username", None) or "").lower()
            if uname == "pricenftbot":
                _pricenft_entity = ent
                _save_pricenft_peer_id(ent)
                return _pricenft_entity
    except FloodWaitError as e:
        _set_pricenft_flood(getattr(e, "seconds", 0) or 0)
        raise PriceNftFloodError(getattr(e, "seconds", 0) or 0)
    except Exception as e:
        logger.debug("pricenft dialogs: %s", e)

    # 3) один раз resolve username
    try:
        ent = await tg_client.get_entity(PRICENFT_BOT)
        _pricenft_entity = ent
        _save_pricenft_peer_id(ent)
        return _pricenft_entity
    except FloodWaitError as e:
        _set_pricenft_flood(getattr(e, "seconds", 0) or 0)
        raise PriceNftFloodError(getattr(e, "seconds", 0) or 0)

def _btn_texts_from_msg(msg):
    """Все подписи кнопок (reply + inline) из сообщения."""
    texts = []
    # reply keyboard в peer может быть не в msg — смотрим markup сообщения
    markup = getattr(msg, "reply_markup", None)
    if not markup:
        return texts
    rows = getattr(markup, "rows", None) or []
    for row in rows:
        for btn in (getattr(row, "buttons", None) or []):
            t = getattr(btn, "text", None)
            if t:
                texts.append(str(t))
    return texts

def _find_btn_text(texts, keywords):
    for t in texts:
        low = t.lower()
        if any(k in low for k in keywords):
            return t
    return None

async def _pricenft_send(text):
    await _pricenft_wait_cd()
    peer = await _pricenft_peer()
    return await tg_client.send_message(peer, text)

async def _pricenft_latest(limit=6):
    peer = await _pricenft_peer()
    return await tg_client.get_messages(peer, limit=limit)

async def _pricenft_click_or_send(msg, text):
    """Кликаем inline/reply кнопку, иначе шлём текст."""
    if msg is not None:
        try:
            # Telethon Message.click по тексту
            await _pricenft_wait_cd()
            await msg.click(text=text)
            return True
        except Exception:
            try:
                # по индексу среди кнопок
                texts = _btn_texts_from_msg(msg)
                if text in texts:
                    await _pricenft_wait_cd()
                    await msg.click(texts.index(text))
                    return True
            except Exception:
                pass
    await _pricenft_send(text)
    return True

async def fill_db_from_market_fast(progress_cb=None, parallel=24, target_users=None):
    """
    Быстрое наполнение БД с маркета до цели (по умолчанию 100к юзеров).
    Несколько кругов по коллекциям + пагинация. Можно Стопнуть.
    """
    if target_users is None:
        target_users = DB_TARGET_USERS
    await ensure_collections()
    load_pricenft_db()
    pairs = list(ALL_GIFT_IDS)
    if not pairs:
        return {"ok": False, "error": "no_collections", **pricenft_db_stats()}
    total = 0
    rounds = 0

    async def prog(t):
        if progress_cb:
            try:
                await progress_cb(t)
            except Exception:
                pass

    # круги пока не набрали цель / не стоп
    while True:
        if _pricenft_stop:
            break
        st0 = pricenft_db_stats()
        users0 = int(st0.get("users", 0) or 0)
        if users0 >= target_users:
            await prog("✅ Цель БД достигнута: " + str(users0) + " / " + str(target_users))
            break
        rounds += 1
        random.shuffle(pairs)
        await prog(
            "Маркет→БД круг " + str(rounds)
            + "\nЮзеров: " + str(users0) + " / " + str(target_users)
            + "\nКоллекций: " + str(len(pairs))
        )
        for i in range(0, len(pairs), parallel):
            if _pricenft_stop:
                break
            if int(pricenft_db_stats().get("users", 0) or 0) >= target_users:
                break
            chunk = pairs[i:i + parallel]

            async def one(gid, title):
                n = 0
                offset = ""
                for _page in range(3):
                    try:
                        items, nxt = await fetch_market_page(gid, offset, limit=100, newest=True)
                    except Exception:
                        break
                    if not items:
                        break
                    for it in items:
                        it = dict(it)
                        if title and (not it.get("title") or str(it.get("title")) in ("?", "NFT")):
                            it["title"] = title
                        it["gift_id"] = gid
                        seed_pricenft_from_item(it, commit=False)
                        n += 1
                    if not nxt:
                        break
                    offset = nxt
                return n

            parts = await asyncio.gather(
                *[one(gid, title) for gid, title in chunk],
                return_exceptions=True,
            )
            for p in parts:
                if isinstance(p, int):
                    total += p
            db_flush(force=True)
            if (i // parallel) % 2 == 0:
                st = pricenft_db_stats()
                await prog(
                    "Маркет→БД круг " + str(rounds) + ": "
                    + str(min(i + parallel, len(pairs))) + "/" + str(len(pairs))
                    + "\nЮзеров: " + str(st.get("users", 0)) + " / " + str(target_users)
                    + " | NFT: " + str(st.get("nfts", 0))
                )
        db_flush(force=True)
        save_pricenft_db()
        # защита от бесконечного круга если юзеры не растут
        st1 = pricenft_db_stats()
        if int(st1.get("users", 0) or 0) <= users0 and rounds >= 3:
            await prog("Рост юзеров остановился на " + str(st1.get("users", 0)))
            break
        if rounds >= 30:
            break

    db_flush(force=True)
    save_pricenft_db()
    st = pricenft_db_stats()
    return {"ok": True, "seeded": total, "rounds": rounds, "target": target_users, **st}

async def _pricenft_ingest_messages(model_name, messages):
    """Парсим ответы бота и кладём юзеров в БД под model_name."""
    added = 0
    for msg in messages or []:
        if getattr(msg, "out", False):
            continue
        blob = getattr(msg, "message", "") or ""
        for ent in (getattr(msg, "entities", None) or []):
            url = getattr(ent, "url", None)
            if url:
                blob += "\n" + url
            # text mention / url mention
            user_id = getattr(ent, "user_id", None)
            if user_id and hasattr(ent, "offset") and hasattr(ent, "length"):
                try:
                    mention = blob[ent.offset:ent.offset + ent.length]
                    if mention.startswith("@"):
                        blob += "\n" + mention
                except Exception:
                    pass
        users, nfts = _parse_pricenft_text(blob)
        # если модель не задана — пробуем вытащить из заголовка/текста
        mname = model_name
        if not mname:
            # часто первая строка — название модели
            first = (blob.strip().splitlines() or [""])[0].strip()
            if first and len(first) < 60 and not first.startswith("@"):
                mname = first
        if not mname:
            mname = "Unknown"
        for u in users:
            if pricenft_add_user(mname, u, nfts, commit=False):
                added += 1
        # nft slug → model hint (PlushPepe-123)
        for nu in nfts:
            slug = nu.rsplit("/", 1)[-1]
            base = re.sub(r"-\d+$", "", slug)
            base = re.sub(r"(?<!^)([A-Z])", r" \1", base).strip()
            if base and users:
                for u in users:
                    pricenft_add_user(base, u, [nu], commit=False)
    db_flush(force=True)
    return added

async def resolve_pricenft_hits(hits, limit=None):
    """Резолвим username → entity/id для выдачи в боте."""
    out = []
    for h in hits:
        if limit and len(out) >= limit:
            break
        uname = h.get("username")
        if not uname:
            continue
        owner = h.get("owner")
        oid = h.get("owner_id")
        if owner is None or oid is None:
            try:
                owner = await tg_client.get_entity(uname)
                oid = int(owner.id)
            except Exception:
                # без entity тоже можно показать @username
                oid = None
        fn = (getattr(owner, "first_name", "") or "") if owner else ""
        ln = (getattr(owner, "last_name", "") or "") if owner else ""
        name = (fn + " " + ln).strip() or uname
        out.append({
            **h,
            "owner": owner,
            "owner_id": oid,
            "name": name,
            "profile_url": "https://t.me/" + uname,
        })
    return out

async def search_pricenftbot(query=None, limit=25, resolve=True):
    """
    Выдача из локальной БД (рандом по моделям).
    resolve=False — не резолвить entity (быстрее, без FloodWait).
    """
    model = None
    q = (query or "").strip()
    if q and q.lower() not in ("nft", "gift", "owner", "ton", "stars", "market", "girl", "model"):
        model = q
    hits = random_from_pricenft_db(limit=limit, model=model)
    if not resolve:
        return hits
    return await resolve_pricenft_hits(hits, limit=limit)

async def collect_pricenft_db(progress_cb=None, max_models=0):
    """
    Сборщик владельцев из @PriceNFTbot в локальную БД.
    max_models=0 — без лимита (пока не нажмут Стоп).
    Можно остановить в любой момент: stop_pricenft_collect().
    """
    global _pricenft_collecting, _pricenft_stop
    if _pricenft_collecting:
        return {"ok": False, "error": "already_running"}
    _pricenft_collecting = True
    _pricenft_stop = False
    load_pricenft_db()
    stats = {"models_clicked": 0, "added": 0, "errors": 0, "stopped": False}

    async def prog(text):
        if progress_cb:
            try:
                await progress_cb(text)
            except Exception:
                pass

    def stopped():
        return _pricenft_stop or not _pricenft_collecting

    try:
        if not await check_authorized():
            return {"ok": False, "error": "not_authorized"}

        _load_pricenft_flood()
        if _pricenft_flood_active():
            left = max(1, int(_pricenft_flood_until - time.time()))
            hrs = left / 3600.0
            await prog(
                "⏳ Telegram FloodWait ~" + str(int(hrs)) + "ч\n"
                "PriceNFTbot временно недоступен.\n"
                "Переключаюсь на быстрый сбор с маркета..."
            )
            m = await fill_db_from_market_fast(progress_cb=progress_cb)
            return {
                "ok": True,
                "flood_fallback": True,
                "flood_wait": left,
                **stats,
                **m,
            }

        async with _pricenft_lock:
            # один раз резолвим peer в кэш
            await prog("PriceNFTbot: подключение (кэш peer)...")
            await _pricenft_peer()

            await prog("PriceNFTbot: /start ...")
            await _pricenft_send("/start")
            await asyncio.sleep(PRICENFT_MSG_CD)
            if stopped():
                stats["stopped"] = True
                st = pricenft_db_stats()
                return {"ok": True, **stats, **st}

            await prog("PriceNFTbot: /search ...")
            await _pricenft_send("/search")
            await asyncio.sleep(PRICENFT_MSG_CD)
            msgs = await _pricenft_latest(5)
            last = next((m for m in msgs if not getattr(m, "out", False)), None)
            btns = _btn_texts_from_msg(last) if last else []
            logger.info("PriceNFT buttons after /search: %s", btns)

            cat_map = [
                ("model", ("модель", "model", "models")),
                ("bg", ("фон", "background", "backdrop", "фончик")),
                ("pattern", ("узор", "pattern", "symbol", "символ", "паттерн")),
            ]

            async def open_category(keys):
                nonlocal last, btns
                if stopped():
                    return False
                msgs = await _pricenft_latest(4)
                last = next((m for m in msgs if not getattr(m, "out", False)), None)
                btns = _btn_texts_from_msg(last) if last else []
                target = _find_btn_text(btns, keys)
                if target:
                    await _pricenft_click_or_send(last, target)
                    await asyncio.sleep(PRICENFT_MSG_CD)
                    return True
                for k in keys:
                    await _pricenft_send(k.capitalize() if k.isalpha() else k)
                    await asyncio.sleep(PRICENFT_MSG_CD)
                    break
                return True

            await prog("PriceNFTbot: открываю Модель...")
            await open_category(cat_map[0][1])
            if stopped():
                stats["stopped"] = True
                st = pricenft_db_stats()
                return {"ok": True, **stats, **st}

            model_names = []
            seen_btn = set()
            for page in range(40):
                if stopped():
                    break
                msgs = await _pricenft_latest(3)
                last = next((m for m in msgs if not getattr(m, "out", False)), None)
                btns = _btn_texts_from_msg(last) if last else []
                nav_keys = ("далее", "next", "ещё", "еще", "more", "назад", "back",
                            "меню", "menu", "отмена", "cancel", "поиск", "search",
                            "модель", "model", "фон", "узор", "pattern", "background")
                page_models = []
                for t in btns:
                    low = t.lower().strip()
                    if any(k in low for k in nav_keys):
                        continue
                    if low in seen_btn:
                        continue
                    if len(t) < 2 or len(t) > 64:
                        continue
                    seen_btn.add(low)
                    page_models.append(t)
                model_names.extend(page_models)
                if max_models and len(model_names) >= max_models:
                    break
                nxt = _find_btn_text(btns, ("далее", "next", "ещё", "еще", "more", "→", "»"))
                if not nxt:
                    break
                await prog("PriceNFTbot: страница моделей " + str(page + 2) + "...")
                await _pricenft_click_or_send(last, nxt)
                await asyncio.sleep(PRICENFT_MSG_CD)

            await ensure_collections()
            col_titles = [t for _, t in ALL_GIFT_IDS]
            random.shuffle(col_titles)
            seen_m = {x.lower() for x in model_names}
            for t in col_titles:
                if t.lower() in seen_m:
                    continue
                model_names.append(t)
                seen_m.add(t.lower())
                if max_models and len(model_names) >= max_models:
                    break
            if not model_names:
                model_names = col_titles[:]
            if max_models:
                model_names = model_names[:max_models]
            random.shuffle(model_names)
            await prog("Моделей к сбору: " + str(len(model_names)) + "\nМожно нажать Стоп в любой момент")

            for i, mname in enumerate(model_names):
                if stopped():
                    stats["stopped"] = True
                    break
                await prog(
                    "Модель " + str(i + 1) + "/" + str(len(model_names)) + ": "
                    + mname + "\nВ БД юзеров: " + str(pricenft_db_stats()["users"])
                    + "\n⏹ Можно остановить"
                )
                try:
                    msgs = await _pricenft_latest(3)
                    last = next((m for m in msgs if not getattr(m, "out", False)), None)
                    btns = _btn_texts_from_msg(last) if last else []
                    if mname in btns:
                        await _pricenft_click_or_send(last, mname)
                    else:
                        await _pricenft_send(mname)
                    await asyncio.sleep(PRICENFT_MSG_CD)
                    await asyncio.sleep(1.0)
                    msgs = await _pricenft_latest(10)
                    added = await _pricenft_ingest_messages(mname, msgs)
                    stats["added"] += added
                    stats["models_clicked"] += 1
                    msgs = await _pricenft_latest(2)
                    last = next((m for m in msgs if not getattr(m, "out", False)), None)
                    btns = _btn_texts_from_msg(last) if last else []
                    back = _find_btn_text(btns, ("назад", "back", "←"))
                    if back:
                        await _pricenft_click_or_send(last, back)
                        await asyncio.sleep(PRICENFT_MSG_CD)
                except Exception as e:
                    stats["errors"] += 1
                    logger.warning("collect model %s: %s", mname, e)
                if (i + 1) % 5 == 0:
                    save_pricenft_db()

            # Фон/Узор — только если не стопнули
            if not stopped():
                for cat_key, keys in cat_map[1:]:
                    if stopped():
                        stats["stopped"] = True
                        break
                    try:
                        await prog("PriceNFTbot: категория " + cat_key + "...")
                        await _pricenft_send("/search")
                        await asyncio.sleep(PRICENFT_MSG_CD)
                        await open_category(keys)
                        msgs = await _pricenft_latest(4)
                        last = next((m for m in msgs if not getattr(m, "out", False)), None)
                        btns = _btn_texts_from_msg(last) if last else []
                        options = []
                        for t in btns:
                            low = t.lower()
                            if any(k in low for k in ("назад", "back", "далее", "next", "меню", "menu", "поиск")):
                                continue
                            options.append(t)
                        for t in options[:20]:
                            if stopped():
                                stats["stopped"] = True
                                break
                            await prog(cat_key + ": " + t + "\nЮзеров: " + str(pricenft_db_stats()["users"]))
                            await _pricenft_click_or_send(last, t)
                            await asyncio.sleep(PRICENFT_MSG_CD)
                            await asyncio.sleep(1.0)
                            msgs = await _pricenft_latest(8)
                            stats["added"] += await _pricenft_ingest_messages(t, msgs)
                            msgs = await _pricenft_latest(2)
                            last2 = next((m for m in msgs if not getattr(m, "out", False)), None)
                            btns2 = _btn_texts_from_msg(last2) if last2 else []
                            back = _find_btn_text(btns2, ("назад", "back", "←"))
                            if back:
                                await _pricenft_click_or_send(last2, back)
                                await asyncio.sleep(PRICENFT_MSG_CD)
                            msgs = await _pricenft_latest(3)
                            last = next((m for m in msgs if not getattr(m, "out", False)), None)
                    except Exception as e:
                        stats["errors"] += 1
                        logger.warning("collect cat %s: %s", cat_key, e)

            save_pricenft_db()
            st = pricenft_db_stats()
            prefix = "⏹ Сбор остановлен\n" if stats["stopped"] else "✅ Сбор готов\n"
            await prog(
                prefix
                + "Моделей в БД: " + str(st["models"]) + "\n"
                + "Юзеров: " + str(st["users"]) + "\n"
                + "NFT-ссылок: " + str(st["nfts"]) + "\n"
                + "Кликов: " + str(stats["models_clicked"]) + " | +записей: " + str(stats["added"])
            )
            return {"ok": True, **stats, **st}
    except (FloodWaitError, PriceNftFloodError) as e:
        sec = int(getattr(e, "seconds", 0) or 0)
        _set_pricenft_flood(sec)
        save_pricenft_db()
        hrs = max(1, sec // 3600)
        await prog(
            "⏳ FloodWait ~" + str(hrs) + "ч на ResolveUsername\n"
            "Сохранил что есть. Добиваю БД с маркета..."
        )
        try:
            m = await fill_db_from_market_fast(progress_cb=progress_cb)
            return {
                "ok": True,
                "flood_fallback": True,
                "flood_wait": sec,
                **stats,
                **m,
            }
        except Exception as e2:
            return {
                "ok": False,
                "error": "flood_wait_" + str(sec) + "s: " + str(e2),
                "flood_wait": sec,
                **stats,
                **pricenft_db_stats(),
            }
    except Exception as e:
        logger.error("collect_pricenft_db: %s", e)
        save_pricenft_db()
        # если это flood в тексте — тоже fallback
        msg = str(e)
        if "FloodWait" in msg or "wait of" in msg.lower():
            mnum = re.search(r"(\d+)\s*seconds?", msg, re.I)
            sec = int(mnum.group(1)) if mnum else 3600
            _set_pricenft_flood(sec)
            try:
                await prog("⏳ FloodWait — добиваю БД с маркета...")
                m = await fill_db_from_market_fast(progress_cb=progress_cb)
                return {"ok": True, "flood_fallback": True, "flood_wait": sec, **stats, **m}
            except Exception:
                pass
        return {"ok": False, "error": str(e), **stats}
    finally:
        _pricenft_collecting = False
        _pricenft_stop = False


def stop_pricenft_collect():
    """Остановить сбор БД из PriceNFTbot."""
    global _pricenft_collecting, _pricenft_stop
    _pricenft_stop = True
    # не сбрасываем _pricenft_collecting сразу — цикл сам выйдет и сделает finally
    return True



_bootstrap_task = None

async def bootstrap_gifts_db(notify_chat_id=None):
    """
    Автосохранение гифтов в БД после авторизации:
    1) Быстрый проход по маркету всех коллекций
    2) Фоновый сбор через @PriceNFTbot
    """
    global _pricenft_collecting
    if not await check_authorized():
        return {"ok": False, "error": "not_authorized"}
    await ensure_collections()
    load_pricenft_db()
    before = pricenft_db_stats()

    async def _notify(txt):
        if not notify_chat_id:
            return
        try:
            await bot.send_message(int(notify_chat_id), "<b>" + txt + "</b>", parse_mode="HTML")
        except Exception:
            pass

    await _notify(
        "📦 Сохраняю гифты в БД...\n"
        "Коллекций: " + str(len(ALL_GIFT_IDS)) + "\n"
        "Сейчас: " + str(before.get("users", 0)) + " юзеров"
    )

    pairs = list(ALL_GIFT_IDS)
    random.shuffle(pairs)
    PARALLEL = 15
    for i in range(0, len(pairs), PARALLEL):
        chunk = pairs[i:i+PARALLEL]
        async def one(gid, title):
            try:
                items, _ = await fetch_market_page(gid, "", limit=80, newest=True)
            except Exception:
                return 0
            for it in items:
                it = dict(it)
                if title and (not it.get("title") or str(it.get("title")) in ("?", "NFT")):
                    it["title"] = title
                it["gift_id"] = gid
                seed_pricenft_from_item(it, commit=False)
            return len(items)
        await asyncio.gather(*[one(gid, title) for gid, title in chunk], return_exceptions=True)
        db_flush(force=True)
        if (i // PARALLEL) % 4 == 0:
            save_pricenft_db()
            mid = pricenft_db_stats()
            await _notify(
                "📦 БД: " + str(mid.get("users", 0)) + " юзеров / "
                + str(mid.get("models", 0)) + " моделей\n"
                + str(min(i + PARALLEL, len(pairs))) + "/" + str(len(pairs))
            )

    db_flush(force=True)
    save_pricenft_db()
    after_m = pricenft_db_stats()
    await _notify(
        "✅ Гифты с маркета в БД\n"
        "Моделей: " + str(after_m.get("models", 0)) + "\n"
        "Юзеров: " + str(after_m.get("users", 0)) + "\n"
        "Дальше @PriceNFTbot в фоне..."
    )

    async def _bg_price():
        try:
            await collect_pricenft_db(progress_cb=None, max_models=80)
            st = pricenft_db_stats()
            await _notify("✅ PriceNFT в БД: " + str(st.get("models", 0)) + " моделей / " + str(st.get("users", 0)) + " юзеров")
        except Exception as e:
            logger.warning("bg PriceNFT: %s", e)

    if not _pricenft_collecting:
        asyncio.create_task(_bg_price())
    return {"ok": True, **after_m}


def start_bootstrap_gifts_db(notify_chat_id=None):
    global _bootstrap_task
    try:
        if _bootstrap_task and not _bootstrap_task.done():
            return False
    except NameError:
        pass
    _bootstrap_task = asyncio.create_task(bootstrap_gifts_db(notify_chat_id))
    return True


async def background_db_keeper():
    """
    Основной фоновый режим: постоянно быстро пишет маркет в sqlite,
    периодически дособирает через @PriceNFTbot (CD 3с).
    Работает всегда после авторизации — даже когда поиск не идёт.
    """
    logger.info("background_db_keeper started")
    await asyncio.sleep(5)
    cycle = 0
    while True:
        try:
            if not await check_authorized():
                await asyncio.sleep(30)
                continue
            # во время поиска не конкурируем за API — маркет и так сидится из выдачи
            if is_searching:
                await asyncio.sleep(3)
                continue
            await ensure_collections()
            pairs = list(ALL_GIFT_IDS) or []
            if not pairs:
                await asyncio.sleep(20)
                continue
            random.shuffle(pairs)
            PARALLEL = 22
            # быстрый проход маркета — основной объём БД до 100к
            st0 = pricenft_db_stats()
            if int(st0.get("users", 0) or 0) < DB_TARGET_USERS:
                # глубокий fill раз в несколько циклов
                if cycle % 3 == 0:
                    try:
                        await fill_db_from_market_fast(
                            progress_cb=None, parallel=22, target_users=DB_TARGET_USERS
                        )
                    except Exception as e:
                        logger.warning("keeper fill: %s", e)
            for i in range(0, len(pairs), PARALLEL):
                if is_searching:
                    break
                chunk = pairs[i:i + PARALLEL]

                async def one(gid, title):
                    try:
                        items, _ = await fetch_market_page(gid, "", limit=100, newest=True)
                    except Exception:
                        return 0
                    for it in items:
                        it = dict(it)
                        if title and (not it.get("title") or str(it.get("title")) in ("?", "NFT")):
                            it["title"] = title
                        it["gift_id"] = gid
                        seed_pricenft_from_item(it, commit=False)
                    return len(items)

                await asyncio.gather(
                    *[one(gid, title) for gid, title in chunk],
                    return_exceptions=True,
                )
                db_flush(force=True)
                # не душим API: короткая пауза между чанками
                await asyncio.sleep(0.15)

            db_flush(force=True)
            save_pricenft_db()
            cycle += 1
            st = pricenft_db_stats()
            logger.info(
                "DB keeper cycle=%s models=%s users=%s nfts=%s",
                cycle, st.get("models"), st.get("users"), st.get("nfts"),
            )

            # PriceNFTbot — только если нет FloodWait
            if (
                cycle % 2 == 0
                and not _pricenft_collecting
                and not is_searching
                and not _pricenft_flood_active()
            ):
                try:
                    await collect_pricenft_db(progress_cb=None, max_models=40)
                    db_flush(force=True)
                    save_pricenft_db()
                except Exception as e:
                    logger.warning("keeper PriceNFT: %s", e)

            await asyncio.sleep(8)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("background_db_keeper: %s", e)
            await asyncio.sleep(15)


def start_background_db_keeper():
    global _keeper_task
    try:
        if _keeper_task and not _keeper_task.done():
            return False
    except NameError:
        pass
    _keeper_task = asyncio.create_task(background_db_keeper())
    return True


async def deliver_pricenft_random(status_msg, max_results=20, girls_only=False, region="any",
                                  with_cd=False):
    """
    Рандомная выдача из БД PriceNFT по моделям.
    with_cd — пауза только если нужно (по умолчанию выкл, быстро).
    """
    global is_searching
    load_pricenft_db()
    st = pricenft_db_stats()
    if st.get("users", 0) <= 0:
        # БД опциональна — не ломаем поиск, просто пропускаем
        return 0

    hits = await search_pricenftbot(limit=max(max_results * 3, 30), resolve=True)
    found = 0
    for h in hits:
        if not is_searching or found >= max_results:
            break
        uname = h.get("username")
        oid = h.get("owner_id")
        owner = h.get("owner")
        name = h.get("name") or ""
        nft_url = h.get("nft_url")
        if is_owner_seen(oid, uname) or is_gift_seen(nft_url):
            continue
        if is_trader_account(owner, uname, name):
            mark_seen(oid, uname, None)
            continue
        if girls_only and not is_girl(owner, uname, name):
            continue
        if region and region != "any":
            if not region_match_full(owner, uname, name, region):
                continue
        model = h.get("model") or h.get("title") or "NFT"
        p_url = h.get("profile_url") or (("https://t.me/" + uname) if uname else None)
        owner_s = fmt_owner(owner, uname, name)
        user_line = ("\n👤 @" + esc(uname)) if uname else ""
        model_line = "\n🎨 Модель: <b>" + esc(str(model)) + "</b>"
        nft_line = ""
        if nft_url:
            nft_line = '\n<a href="' + nft_url + '">' + esc(str(model)) + "</a>"
        txt = "<b>" + owner_s + "</b>" + user_line + model_line + nft_line
        mark_seen(oid, uname, nft_url)
        if oid:
            cache_owner(oid, owner, uname, name, p_url, [h])
            kb = model_card_kb(uname, p_url, oid, nft_url, nft_count=1)
        else:
            # без id — только ссылка на username
            btns = []
            if uname:
                btns.append([InlineKeyboardButton(text="@" + uname, url="https://t.me/" + uname)])
                msg = urllib.parse.quote(WRITE_MSG)
                btns.append([InlineKeyboardButton(text="Написать", url="https://t.me/" + uname + "?text=" + msg)])
            kb = InlineKeyboardMarkup(inline_keyboard=btns) if btns else None
        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id, text=txt,
                parse_mode="HTML", reply_markup=kb,
                disable_web_page_preview=True,
            )
            found += 1
            stats["found"] += 1
        except Exception as e:
            logger.warning("pricenft deliver: %s", e)
            continue
        if with_cd and found < max_results and is_searching:
            await asyncio.sleep(PRICENFT_MSG_CD)
    return found


# ── PROFILE SEARCH ────────────────────────────────────────────────────────────
# Список чатов — короткий, только рабочие NFT/Stars (быстрее профиль-поиск)
NFT_SCAN_CHATS = [
    "tgstargifts", "stargiftsru", "stargifts_market", "giftstars",
    "starnftmarket", "nft_stars_market", "telegramnft", "cryptogifts_ton",
    "tonstarsgifts", "giftsmarket_ton", "nftgifts_market", "starsgifts_ton",
    "ton_gifts_market", "giftstonmarket", "nft_gift_ton", "stargifts_exchange",
]

async def get_chat_members_with_gifts(chat_username, max_users=500):
    results = []
    try:
        entity = await tg_client.get_entity(chat_username)
        try:
            from telethon.tl.functions.channels import GetParticipantsRequest
            from telethon.tl.types import ChannelParticipantsSearch
            offset = 0
            limit  = 200
            while len(results) < max_users:
                part = await tg_client(GetParticipantsRequest(
                    channel=entity,
                    filter=ChannelParticipantsSearch(""),
                    offset=offset, limit=limit, hash=0
                ))
                users = getattr(part, "users", []) or []
                if not users:
                    break
                for u in users:
                    if getattr(u, "bot", False):
                        continue
                    results.append((u, int(u.id)))
                if len(users) < limit:
                    break
                offset += len(users)
                await asyncio.sleep(0.05)
        except Exception:
            history = await tg_client(GetHistoryRequest(
                peer=entity, offset_id=0, offset_date=None,
                add_offset=0, limit=200, max_id=0, min_id=0, hash=0
            ))
            msgs = getattr(history, "messages", []) or []
            users_map = {int(u.id): u for u in (getattr(history, "users", []) or [])}
            seen = set()
            for m in msgs:
                fid = getattr(m, "from_id", None)
                if fid is None:
                    continue
                uid = getattr(fid, "user_id", None)
                if not uid or uid in seen:
                    continue
                seen.add(uid)
                u = users_map.get(uid)
                if u and not getattr(u, "bot", False):
                    results.append((u, uid))
    except Exception as e:
        logger.debug("get_chat_members %s: %s", chat_username, e)
    return results


async def do_profile_search(status_msg, gift_ids, cat=None, girls_only=False,
                            min_gifts=1, max_gifts=0,
                            max_results=30, region="any"):
    """
    Профиль: аккаунты с 0 гифтов на маркете.
    Кандидаты: рандом из локальной БД + чаты (НЕ текущие продавцы маркета).
    seen помечаем только при реальной выдаче.
    """
    global is_searching
    is_searching = True
    lock      = asyncio.Lock()
    found     = [0]
    seen_sent = set()
    checked   = set()
    check_q = asyncio.Queue()
    feeder_done = asyncio.Event()
    stats_skip = {"market": 0, "empty": 0, "girl": 0, "range": 0, "trader": 0, "seen": 0}

    async def emit_cand(key, info):
        if not key or key in checked:
            return
        checked.add(key)
        uname = (info or {}).get("username")
        uid = (info or {}).get("owner_id") or (info or {}).get("uid")
        if is_owner_seen(uid, uname):
            stats_skip["seen"] += 1
            return
        await check_q.put(info)

    async def check_one(info):
        if not is_searching or found[0] >= max_results:
            return
        owner_obj = info.get("owner")
        username  = (info.get("username") or "").lstrip("@") or None
        uid       = info.get("owner_id") or info.get("uid")
        name      = info.get("name") or ""
        peer      = uid or username
        if not peer:
            return
        profile_url = info.get("profile_url") or (
            ("https://t.me/" + username) if username else ("tg://user?id=" + str(uid) if uid else None)
        )
        if is_owner_seen(uid, username):
            stats_skip["seen"] += 1
            return
        async with lock:
            sk = uid or ("u:" + username.lower())
            if sk in seen_sent:
                return

        if is_trader_account(owner_obj, username, name):
            stats_skip["trader"] += 1
            return
        if girls_only and not is_girl(owner_obj, username, name):
            stats_skip["girl"] += 1
            return
        if region and region != "any":
            if not region_match_full(owner_obj, username, name, region):
                return

        saved = await fetch_saved_gifts(
            peer, max_pages=1, only_off_market=True, require_zero_on_market=True
        )
        if saved is None:
            stats_skip["market"] += 1
            return
        if not saved:
            stats_skip["empty"] += 1
            return

        hidden = []
        for g in saved:
            if g.get("on_market") or not g.get("nft_url"):
                continue
            if is_gift_seen(g.get("nft_url")):
                continue
            hidden.append(g)
        if not hidden:
            stats_skip["empty"] += 1
            return
        if len(hidden) < min_gifts:
            stats_skip["range"] += 1
            return
        if max_gifts and max_gifts > 0 and len(hidden) > max_gifts:
            stats_skip["range"] += 1
            return

        # резолвим id если нет
        if uid is None and username:
            try:
                ent = await tg_client.get_entity(username)
                uid = int(ent.id)
                owner_obj = ent
                fn = (getattr(ent, "first_name", "") or "")
                ln = (getattr(ent, "last_name", "") or "")
                name = (fn + " " + ln).strip() or name or username
            except Exception:
                pass

        async with lock:
            sk = uid or ("u:" + (username or "").lower())
            if sk in seen_sent or found[0] >= max_results:
                return
            seen_sent.add(sk)
            found[0] += 1

        mark_seen(uid, username, hidden[0].get("nft_url") if hidden else None)
        for g in hidden[1:8]:
            mark_seen(None, None, g.get("nft_url"))

        if uid:
            cache_owner(uid, owner_obj, username, name, profile_url, hidden)
        owner_s = fmt_owner(owner_obj, username, name)
        first_nft_url = hidden[0].get("nft_url")
        nft_count = len(hidden)
        txt = ("<b>" + owner_s + "\nNFT в профиле (0 на маркете): " + str(nft_count) + "</b>"
               + _make_nft_lines(hidden))
        kb = owner_card_kb(username, profile_url, uid or 0,
                           nft_url_for_msg=first_nft_url if nft_count == 1 else None,
                           nft_count=nft_count)
        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id, text=txt,
                parse_mode="HTML", reply_markup=kb,
                disable_web_page_preview=True,
            )
            stats["found"] += 1
        except Exception as e:
            logger.warning("profile send: %s", e)

    async def worker():
        while True:
            if not is_searching or found[0] >= max_results:
                return
            try:
                info = await asyncio.wait_for(check_q.get(), timeout=0.4)
            except asyncio.TimeoutError:
                if feeder_done.is_set() and check_q.empty():
                    return
                continue
            try:
                await check_one(info)
            finally:
                check_q.task_done()

    try:
        await status_msg.edit_text(
            "<b>Ищу профили с 0 гифтов на маркете...</b>",
            parse_mode="HTML", reply_markup=stop_kb()
        )
        workers = [asyncio.create_task(worker()) for _ in range(22)]

        # 1) Быстрый рандом из БД
        if is_searching and found[0] < max_results:
            st = pricenft_db_stats()
            try:
                await status_msg.edit_text(
                    "<b>Профиль:</b> БД (" + str(st.get("users", 0)) + " юзеров)...",
                    parse_mode="HTML", reply_markup=stop_kb()
                )
            except Exception:
                pass
            cands = random_users_from_db(limit=max(250, max_results * 12))
            random.shuffle(cands)
            for h in cands:
                if not is_searching or found[0] >= max_results:
                    break
                uname = h.get("username")
                if not uname:
                    continue
                key = "u:" + uname.lower()
                await emit_cand(key, {
                    "owner": None,
                    "owner_id": h.get("owner_id"),
                    "username": uname,
                    "name": uname,
                    "profile_url": h.get("profile_url"),
                })

        # 2) Чаты
        if is_searching and found[0] < max_results:
            try:
                await status_msg.edit_text(
                    "<b>Профиль:</b> чаты...\nНайдено: " + str(found[0]),
                    parse_mode="HTML", reply_markup=stop_kb()
                )
            except Exception:
                pass
            for ch in NFT_SCAN_CHATS[:10]:
                if not is_searching or found[0] >= max_results:
                    break
                try:
                    res = await asyncio.wait_for(
                        get_chat_members_with_gifts(ch, max_users=150), timeout=6
                    )
                except Exception:
                    continue
                for (u_obj, uid) in res:
                    if not is_searching or found[0] >= max_results:
                        break
                    fn = (getattr(u_obj, "first_name", "") or "")
                    ln = (getattr(u_obj, "last_name", "") or "")
                    uname = getattr(u_obj, "username", None)
                    name = (fn + " " + ln).strip()
                    if girls_only and not is_girl(u_obj, uname, name):
                        continue
                    p_url = ("https://t.me/" + uname) if uname else ("tg://user?id=" + str(uid))
                    await emit_cand(uid, {
                        "owner": u_obj, "owner_id": uid,
                        "username": uname, "name": name, "profile_url": p_url,
                    })

        feeder_done.set()
        # дождаться первой волны
        try:
            await asyncio.wait_for(asyncio.gather(*workers, return_exceptions=True), timeout=150)
        except asyncio.TimeoutError:
            for w in workers:
                w.cancel()

        # если мало результатов — второй проход: достаточно скрытых NFT (не строго 0 на маркете)
        if is_searching and found[0] < max(3, max_results // 2):
            try:
                await status_msg.edit_text(
                    "<b>Профиль:</b> добор (скрытые NFT)...\nНайдено: " + str(found[0]),
                    parse_mode="HTML", reply_markup=stop_kb()
                )
            except Exception:
                pass
            # перезапускаем воркеры для soft-pass
            soft_q = asyncio.Queue()
            feeder_done.clear()

            async def soft_worker():
                while True:
                    if not is_searching or found[0] >= max_results:
                        return
                    try:
                        info = await asyncio.wait_for(soft_q.get(), timeout=0.4)
                    except asyncio.TimeoutError:
                        if feeder_done.is_set() and soft_q.empty():
                            return
                        continue
                    try:
                        if not is_searching or found[0] >= max_results:
                            continue
                        owner_obj = info.get("owner")
                        username = (info.get("username") or "").lstrip("@") or None
                        uid = info.get("owner_id") or info.get("uid")
                        name = info.get("name") or ""
                        peer = uid or username
                        if not peer or is_owner_seen(uid, username):
                            continue
                        if girls_only and not is_girl(owner_obj, username, name):
                            continue
                        saved = await fetch_saved_gifts(
                            peer, max_pages=1, only_off_market=True, require_zero_on_market=False
                        )
                        if not saved:
                            continue
                        hidden = [g for g in saved if g.get("nft_url") and not g.get("on_market") and not is_gift_seen(g.get("nft_url"))]
                        if len(hidden) < min_gifts:
                            continue
                        if max_gifts and max_gifts > 0 and len(hidden) > max_gifts:
                            continue
                        if uid is None and username:
                            try:
                                ent = await tg_client.get_entity(username)
                                uid = int(ent.id)
                                owner_obj = ent
                            except Exception:
                                pass
                        async with lock:
                            sk = uid or ("u:" + (username or "").lower())
                            if sk in seen_sent or found[0] >= max_results:
                                continue
                            seen_sent.add(sk)
                            found[0] += 1
                        mark_seen(uid, username, hidden[0].get("nft_url"))
                        profile_url = info.get("profile_url") or (("https://t.me/" + username) if username else None)
                        if uid:
                            cache_owner(uid, owner_obj, username, name, profile_url, hidden)
                        txt = ("<b>" + fmt_owner(owner_obj, username, name)
                               + "\nNFT в профиле (скрытые): " + str(len(hidden)) + "</b>"
                               + _make_nft_lines(hidden))
                        kb = owner_card_kb(username, profile_url, uid or 0,
                                           nft_url_for_msg=hidden[0].get("nft_url") if len(hidden) == 1 else None,
                                           nft_count=len(hidden))
                        await status_msg.bot.send_message(
                            chat_id=status_msg.chat.id, text=txt,
                            parse_mode="HTML", reply_markup=kb,
                            disable_web_page_preview=True,
                        )
                        stats["found"] += 1
                    except Exception as e:
                        logger.debug("soft profile: %s", e)
                    finally:
                        soft_q.task_done()

            sw = [asyncio.create_task(soft_worker()) for _ in range(18)]
            more = random_users_from_db(limit=max(200, max_results * 10))
            random.shuffle(more)
            for h in more:
                if not is_searching or found[0] >= max_results:
                    break
                uname = h.get("username")
                if not uname:
                    continue
                key = "soft:" + uname.lower()
                if key in checked:
                    continue
                checked.add(key)
                await soft_q.put({
                    "owner": None, "owner_id": h.get("owner_id"),
                    "username": uname, "name": uname,
                    "profile_url": h.get("profile_url"),
                })
            feeder_done.set()
            try:
                await asyncio.wait_for(asyncio.gather(*sw, return_exceptions=True), timeout=120)
            except asyncio.TimeoutError:
                for w in sw:
                    w.cancel()

        logger.info("profile_search done found=%s skips=%s checked=%s", found[0], stats_skip, len(checked))
    except Exception as e:
        logger.error("do_profile_search: %s", e)
    finally:
        feeder_done.set()
        is_searching = False
    return found[0]



def _make_nft_lines(items):
    lines = ""
    seen = set()
    count = 0
    for it in items:
        nu = it.get("nft_url")
        slug = nu.split("/")[-1] if nu else ""
        if slug and slug in seen:
            continue
        if slug:
            seen.add(slug)
        if count >= 5:
            break
        t  = esc(str(it.get("title","?")))
        p  = it.get("price")
        ps = " — " + str(p) + " ⭐" if p else ""
        if nu:
            lines += '\n<a href="' + nu + '">' + t + ps + "</a>"
        else:
            lines += "\n" + t + ps
        count += 1
    extra = len(items) - count
    if extra > 0:
        lines += "\n+ ещё " + str(extra) + " NFT"
    return lines


# ── SEARCH CORE: MARKET ───────────────────────────────────────────────────────
async def do_market_search(status_msg, gift_ids, cat=None, girls_only=False,
                           boost=100, min_gifts=1, max_gifts=0,
                           max_results=30, region="any"):
    """Маркет: быстрый стрим лотов. Жёсткий лимит выдачи. Цена лота по категории."""
    global is_searching
    is_searching = True
    try:
        max_results = max(1, int(max_results or 1))
    except Exception:
        max_results = 30

    lock        = asyncio.Lock()
    found       = [0]
    seen_slugs  = set()
    seen_owners = set()

    async def send_one(item, ignore_global=False):
        oid = item.get("owner_id")
        uname = item.get("username")
        if not oid and not uname:
            return False
        nft_url = item.get("nft_url")
        slug = gift_slug_of(nft_url)
        ok_key = oid if oid else ("u:" + str(uname).lower())
        if not ignore_global:
            if is_owner_seen(oid, uname) or is_gift_seen(slug):
                return False
        async with lock:
            if found[0] >= max_results:
                return False
            if ok_key in seen_owners:
                return False
            if slug and slug in seen_slugs:
                return False
            seen_owners.add(ok_key)
            if slug:
                seen_slugs.add(slug)
            found[0] += 1
            slot = found[0]
        # жёсткий стоп после резерва
        if slot > max_results:
            async with lock:
                found[0] = max(0, found[0] - 1)
            return False
        mark_seen(oid, uname, nft_url)
        try:
            seed_pricenft_from_item(item, commit=False)
        except Exception:
            pass
        name  = item.get("name") or ""
        p_url = item.get("profile_url")
        title = esc(str(item.get("title") or "?"))
        price = item.get("price")
        owner_s = fmt_owner(item.get("owner"), uname, name)
        user_line = ("\n👤 @" + esc(uname)) if uname else ""
        price_s = (" — " + str(price) + " ⭐") if price else ""
        nft_line = ('\n<a href="' + nft_url + '">' + title + price_s + "</a>") if nft_url else ("\n" + title + price_s)
        txt = "<b>" + owner_s + "\nСвежий лот на маркете</b>" + user_line + nft_line
        if oid:
            cache_owner(oid, item.get("owner"), uname, name, p_url, [item])
        kb = owner_card_kb(uname, p_url, oid or 0, nft_url_for_msg=nft_url, nft_count=1)
        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id, text=txt,
                parse_mode="HTML", reply_markup=kb,
                disable_web_page_preview=True,
            )
            stats["found"] += 1
            return True
        except Exception as e:
            logger.warning("market send: %s", e)
            async with lock:
                found[0] = max(0, found[0] - 1)
                seen_owners.discard(ok_key)
                if slug:
                    seen_slugs.discard(slug)
            return False

    async def scan_col(gid, title="", ignore_global=False):
        if not is_searching or found[0] >= max_results:
            return
        offset = ""
        for _page in range(2):
            if not is_searching or found[0] >= max_results:
                return
            try:
                items, nxt = await fetch_market_page(gid, offset, limit=60, newest=True)
            except Exception as e:
                logger.debug("scan_col %s: %s", gid, e)
                return
            if not items:
                return
            random.shuffle(items)
            for item in items:
                if not is_searching or found[0] >= max_results:
                    return
                item = dict(item)
                item["gift_id"] = gid
                if title and (not item.get("title") or str(item.get("title")) in ("?", "NFT")):
                    item["title"] = title
                price = item.get("price")
                # фильтр по цене ЛОТА в категории (не floor коллекции)
                if not price_in_cat(price, cat):
                    continue
                if girls_only and not is_girl(item.get("owner"), item.get("username"), item.get("name")):
                    continue
                if region and region != "any":
                    if not region_match_full(item.get("owner"), item.get("username"), item.get("name"), region):
                        continue
                await send_one(item, ignore_global=ignore_global)
            if not nxt:
                return
            offset = nxt

    try:
        await status_msg.edit_text(
            "<b>Ищу свежие лоты на маркете...\nЛимит выдачи: " + str(max_results) + "</b>",
            parse_mode="HTML", reply_markup=stop_kb()
        )
        id_set = set(gift_ids)
        valid_pairs = [(gid, title) for gid, title in ALL_GIFT_IDS if gid in id_set] if ALL_GIFT_IDS else [(gid, "") for gid in gift_ids]
        if not valid_pairs:
            valid_pairs = [(gid, "") for gid in gift_ids]
        random.shuffle(valid_pairs)

        PARALLEL = 20
        async def run_pass(ignore_global=False):
            for i in range(0, len(valid_pairs), PARALLEL):
                if not is_searching or found[0] >= max_results:
                    break
                chunk = valid_pairs[i:i+PARALLEL]
                await asyncio.gather(
                    *[scan_col(gid, t, ignore_global=ignore_global) for gid, t in chunk],
                    return_exceptions=True,
                )
                try:
                    await status_msg.edit_text(
                        "<b>Маркет:</b> " + str(found[0]) + "/" + str(max_results),
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                except Exception:
                    pass

        await run_pass(ignore_global=False)
        # если антидубль всё съел — второй проход без глобального seen (только в рамках поиска)
        if is_searching and found[0] < max_results:
            await run_pass(ignore_global=True)

        db_flush(force=True)
        try:
            save_pricenft_db()
        except Exception:
            pass
    except Exception as e:
        logger.error("do_market_search: %s", e)
    finally:
        is_searching = False
    return min(found[0], max_results)


# ── SEARCH CORE: MODEL ────────────────────────────────────────────────────────
async def do_model_search(status_msg, gift_ids, girls_only=False,
                          max_results=30, region="any"):
    """Модель: быстрый стрим + добор из БД. Жёсткий лимит выдачи."""
    global is_searching
    is_searching = True
    try:
        max_results = max(1, int(max_results or 1))
    except Exception:
        max_results = 30

    lock        = asyncio.Lock()
    found       = [0]
    seen_slugs  = set()
    seen_owners = set()
    title_by_gid = {gid: title for gid, title in ALL_GIFT_IDS}
    for gid in gift_ids:
        if gid not in title_by_gid:
            for t, i in NFT_COLLECTIONS.items():
                if i == gid:
                    title_by_gid[gid] = t
                    break

    async def send_item(item, ignore_global=False):
        oid = item.get("owner_id")
        uname = item.get("username")
        nft_url = item.get("nft_url")
        slug = gift_slug_of(nft_url)
        if not oid and not uname:
            return False
        ok_key = oid if oid else ("u:" + str(uname).lower())
        if not ignore_global:
            if is_owner_seen(oid, uname) or is_gift_seen(slug):
                return False
        async with lock:
            if found[0] >= max_results:
                return False
            if ok_key in seen_owners:
                return False
            if slug and slug in seen_slugs:
                return False
            seen_owners.add(ok_key)
            if slug:
                seen_slugs.add(slug)
            found[0] += 1
            slot = found[0]
        if slot > max_results:
            async with lock:
                found[0] = max(0, found[0] - 1)
            return False
        mark_seen(oid, uname, nft_url)
        try:
            seed_pricenft_from_item(item, commit=False)
        except Exception:
            pass
        name = item.get("name") or ""
        p_url = item.get("profile_url") or (("https://t.me/" + uname) if uname else None)
        title = esc(str(item.get("title") or item.get("model") or "?"))
        price = item.get("price")
        owner_s = fmt_owner(item.get("owner"), uname, name)
        user_line = ("\n👤 @" + esc(uname)) if uname else ""
        price_s = ("\n" + str(price) + " ⭐") if price else ""
        nft_line = ('\n<a href="' + nft_url + '">' + title + "</a>") if nft_url else ("\n" + title)
        txt = "<b>" + owner_s + "</b>" + user_line + "\n🎨 " + title + nft_line + price_s
        if oid:
            cache_owner(oid, item.get("owner"), uname, name, p_url, [item])
            kb = model_card_kb(uname, p_url, oid, nft_url, nft_count=1)
        else:
            btns = []
            if uname:
                btns.append([InlineKeyboardButton(text="@" + uname, url="https://t.me/" + uname)])
                msg = urllib.parse.quote(WRITE_MSG)
                btns.append([InlineKeyboardButton(text="Написать", url="https://t.me/" + uname + "?text=" + msg)])
            if nft_url:
                btns.append([InlineKeyboardButton(text="NFT", url=nft_url)])
            kb = InlineKeyboardMarkup(inline_keyboard=btns) if btns else None
        try:
            await status_msg.bot.send_message(
                chat_id=status_msg.chat.id, text=txt,
                parse_mode="HTML", reply_markup=kb,
                disable_web_page_preview=True,
            )
            stats["found"] += 1
            return True
        except Exception as e:
            logger.warning("model send: %s", e)
            async with lock:
                found[0] = max(0, found[0] - 1)
                seen_owners.discard(ok_key)
                if slug:
                    seen_slugs.discard(slug)
            return False

    async def scan_col(gid, ignore_global=False):
        title = title_by_gid.get(gid) or ""
        offset = ""
        for _ in range(2):
            if not is_searching or found[0] >= max_results:
                return
            try:
                items, nxt = await fetch_market_page(gid, offset, limit=80, newest=True)
            except Exception:
                return
            if not items:
                return
            random.shuffle(items)
            for it in items:
                if not is_searching or found[0] >= max_results:
                    return
                it = dict(it)
                it["gift_id"] = gid
                if title:
                    it["title"] = title
                if girls_only and not is_girl(it.get("owner"), it.get("username"), it.get("name")):
                    continue
                if region and region != "any":
                    if not region_match_full(it.get("owner"), it.get("username"), it.get("name"), region):
                        continue
                await send_item(it, ignore_global=ignore_global)
            if not nxt:
                return
            offset = nxt

    try:
        label = title_by_gid.get(gift_ids[0], "модели") if len(gift_ids) == 1 else (str(len(gift_ids)) + " коллекций")
        await status_msg.edit_text(
            "<b>Ищу по модели: " + esc(str(label))
            + "\nЛимит выдачи: " + str(max_results) + "</b>",
            parse_mode="HTML", reply_markup=stop_kb()
        )

        gids = list(gift_ids) if gift_ids else [gid for gid, _ in ALL_GIFT_IDS]
        random.shuffle(gids)
        PARALLEL = 16

        async def run_pass(ignore_global=False):
            for i in range(0, len(gids), PARALLEL):
                if not is_searching or found[0] >= max_results:
                    break
                await asyncio.gather(
                    *[scan_col(g, ignore_global=ignore_global) for g in gids[i:i+PARALLEL]],
                    return_exceptions=True,
                )
                try:
                    await status_msg.edit_text(
                        "<b>Модель:</b> " + str(min(found[0], max_results)) + "/" + str(max_results),
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                except Exception:
                    pass

        await run_pass(ignore_global=False)

        # добор из БД строго до лимита
        if is_searching and found[0] < max_results:
            titles = [title_by_gid[g] for g in gift_ids if title_by_gid.get(g)]
            need = max_results - found[0]
            hits = []
            if titles:
                for t in titles:
                    hits.extend(random_from_pricenft_db(limit=max(need * 4, 40), model=t))
            else:
                hits = random_from_pricenft_db(limit=max(need * 5, 60))
            random.shuffle(hits)
            for h in hits:
                if not is_searching or found[0] >= max_results:
                    break
                if girls_only and not is_girl(None, h.get("username"), h.get("name")):
                    continue
                await send_item(h, ignore_global=False)

        if is_searching and found[0] < max_results:
            await run_pass(ignore_global=True)

        db_flush(force=True)
        try:
            save_pricenft_db()
        except Exception:
            pass
    except Exception as e:
        logger.error("do_model_search: %s", e)
    finally:
        is_searching = False
    return min(found[0], max_results)


# ── SEARCH CORE: MODEL BY PROFILE ─────────────────────────────────────────────
async def do_profile_model_search(status_msg, gift_ids, girls_only=False,
                                  max_results=30, region="any"):
    """Модели по профилю: только скрытые NFT, 0 лотов на маркете."""
    # Переиспользуем обычный profile search с фильтром девушек/моделей на уровне выдачи
    return await do_profile_search(
        status_msg, gift_ids, cat=None, girls_only=girls_only,
        min_gifts=1, max_gifts=0, max_results=max_results, region=region,
    )



async def _start_market(cb, cat, girls):
    global is_searching
    if not begin_search():
        await cb.answer("Поиск уже идёт! Нажми Стоп или /clear", show_alert=True)
        return
    # begin_search уже поставил is_searching=True; do_market_search тоже ставит — ок
    await cb.answer("Запускаю...")
    stats["checks"] += 1
    uid    = cb.from_user.id
    chat_id = cb.message.chat.id
    ids    = await ensure_collections()
    if not ids:
        is_searching = False
        await bot.send_message(chat_id, "<b>Коллекции не загружены. Авторизуй Telethon в /admin</b>",
                               parse_mode="HTML", reply_markup=menu_kb())
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
        "Режим: свежие лоты\n"
        "Регион: " + reg_l + "\n"
        "Гифтов в профиле: от " + str(mn) + " до " + mx_s + "\n"
        "Лимит выдачи: " + str(lim) + "</b>"
    )
    status = await bot.send_message(chat_id, txt, parse_mode="HTML", reply_markup=stop_kb())
    try:
        found = await asyncio.wait_for(
            do_market_search(status, ids, cat=cat, girls_only=girls,
                             boost=boost, min_gifts=mn, max_gifts=mx,
                             max_results=lim, region=reg),
            timeout=600
        )
    except asyncio.TimeoutError:
        is_searching = False
        found = 0
    except Exception as e:
        is_searching = False
        found = 0
        logger.error("_start_market: %s", e)
    done = "<b>✅ Поиск закончен\nМаркет / " + cat_l + " / " + who_l + "\nНайдено: " + str(found) + "</b>"
    try:
        await status.edit_text(done, parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        try:
            await bot.send_message(chat_id, done, parse_mode="HTML", reply_markup=menu_kb())
        except Exception:
            pass

async def _start_profile(cb, cat, girls):
    global is_searching
    if not begin_search():
        await cb.answer("Поиск уже идёт! Нажми Стоп или /clear", show_alert=True)
        return
    await cb.answer("Запускаю...")
    stats["checks"] += 1
    uid    = cb.from_user.id
    ids    = await ensure_collections()
    if not ids:
        is_searching = False
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
        "Режим: строго 0 гифтов на маркете\n"
        "Регион: " + reg_l + "\n"
        "Мин. NFT в профиле: " + str(mn) + "\n"
        "Лимит выдачи: " + str(lim) + "</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_profile_search(status, ids, cat=cat, girls_only=girls,
                                    min_gifts=mn, max_gifts=mx,
                                    max_results=lim, region=reg)
    done = "<b>✅ Поиск закончен\nПрофиль / " + cat_l + " / " + who_l + "\nНайдено: " + str(found) + "</b>"
    try:
        await status.edit_text(done, parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        try:
            await bot.send_message(status.chat.id, done, parse_mode="HTML", reply_markup=menu_kb())
        except Exception:
            pass

async def _start_model(cb, girls=False, single_gid=None, search_type="market",
                       already_answered=False, skip_begin=False):
    global is_searching
    if not skip_begin:
        if not begin_search():
            if not already_answered:
                try:
                    await cb.answer("Поиск уже идёт! Нажми Стоп или /clear", show_alert=True)
                except Exception:
                    pass
            return
    else:
        # флаг уже выставлен в cb_mdlrun
        is_searching = True
    if not already_answered:
        try:
            await cb.answer("Запускаю...")
        except Exception:
            pass
    stats["checks"] += 1
    uid = cb.from_user.id
    chat_id = cb.message.chat.id if cb.message else cb.from_user.id
    try:
        ids = await ensure_collections()
        if not ids:
            is_searching = False
            await bot.send_message(chat_id, "<b>Коллекции не загружены. Авторизуй Telethon в /admin</b>",
                                   parse_mode="HTML", reply_markup=menu_kb())
            return
        if single_gid is not None:
            ids = [int(single_gid)]
        lim    = get_limit(uid)
        reg    = get_region(uid)
        who_l  = "Девушки" if girls else "Все"
        reg_l  = REGIONS.get(reg, {}).get("label", "Все страны")
        col_l  = "все коллекции" if single_gid is None else "1 коллекция"
        type_l = "маркет" if search_type == "market" else "профиль"
        txt = (
            "<b>Модели / " + who_l + " / " + type_l + "\n"
            "Коллекция: " + col_l + "\n"
            "Регион: " + reg_l + "\n"
            "Лимит выдачи: " + str(lim) + "</b>"
        )
        status = await bot.send_message(chat_id, txt, parse_mode="HTML", reply_markup=stop_kb())
        if search_type == "profile":
            found = await do_profile_model_search(status, ids, girls_only=girls,
                                                  max_results=lim, region=reg)
        else:
            found = await do_model_search(status, ids, girls_only=girls,
                                          max_results=lim, region=reg)
        try:
            done = "<b>✅ Поиск закончен\nМодели / " + who_l + " / " + type_l + "\nНайдено: " + str(found) + "</b>"
            await status.edit_text(done, parse_mode="HTML", reply_markup=menu_kb())
        except Exception:
            try:
                await bot.send_message(chat_id, done, parse_mode="HTML", reply_markup=menu_kb())
            except Exception:
                pass
    except Exception as e:
        is_searching = False
        logger.error("_start_model: %s", e)
        try:
            await bot.send_message(chat_id, "<b>Ошибка: " + esc(str(e)) + "</b>",
                                   parse_mode="HTML", reply_markup=menu_kb())
        except Exception:
            pass


def ob_min_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1",  callback_data="obmin_1"),
         InlineKeyboardButton(text="2",  callback_data="obmin_2"),
         InlineKeyboardButton(text="3",  callback_data="obmin_3"),
         InlineKeyboardButton(text="5",  callback_data="obmin_5"),
         InlineKeyboardButton(text="10", callback_data="obmin_10")],
    ])

def ob_max_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="5",          callback_data="obmax_5"),
         InlineKeyboardButton(text="10",         callback_data="obmax_10"),
         InlineKeyboardButton(text="20",         callback_data="obmax_20"),
         InlineKeyboardButton(text="50",         callback_data="obmax_50")],
        [InlineKeyboardButton(text="Без лимита", callback_data="obmax_0")],
    ])

def ob_lim_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="10", callback_data="oblim_10"),
         InlineKeyboardButton(text="20", callback_data="oblim_20"),
         InlineKeyboardButton(text="30", callback_data="oblim_30"),
         InlineKeyboardButton(text="40", callback_data="oblim_40"),
         InlineKeyboardButton(text="50", callback_data="oblim_50")],
        [InlineKeyboardButton(text="60", callback_data="oblim_60"),
         InlineKeyboardButton(text="70", callback_data="oblim_70"),
         InlineKeyboardButton(text="80", callback_data="oblim_80"),
         InlineKeyboardButton(text="90", callback_data="oblim_90"),
         InlineKeyboardButton(text="100",callback_data="oblim_100")],
    ])

def ob_region_kb():
    rows = []
    items = list(REGIONS.items())
    for i in range(0, len(items), 3):
        row = []
        for key, val in items[i:i+3]:
            row.append(InlineKeyboardButton(text=val["label"], callback_data="obreg_" + key))
        rows.append(row)
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── ONBOARDING ────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    global is_searching
    is_searching = False
    await state.clear()
    uid = message.from_user.id
    add_user(uid, message.from_user.username,
             message.from_user.first_name, message.from_user.last_name)

    if not await check_authorized() and is_admin(uid):
        await message.answer("<b>Нужна авторизация Telegram\nВведи номер телефона:</b>", parse_mode="HTML")
        await state.set_state(Auth.phone)
        return

    if uid not in ONBOARDING_DONE:
        await message.answer(
            "<b>Добро пожаловать в Neptun Parser\n\n"
            "Шаг 1 из 4 — Минимум гифтов у владельца\nВыбери:</b>",
            parse_mode="HTML", reply_markup=ob_min_kb()
        )
        await state.set_state(Onboarding.min_gifts)
        return

    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await message.answer(
        "<b>Neptun Parser\n\nМин. гифтов: " + str(mn) + "\nМакс. гифтов: " + mx_s + "\n\nВыбери действие:</b>",
        parse_mode="HTML", reply_markup=main_menu_kb()
    )


@dp.callback_query(F.data.startswith("obmin_"))
async def ob_min_btn(cb: CallbackQuery, state: FSMContext):
    if await state.get_state() != Onboarding.min_gifts.state:
        await cb.answer()
        return
    val = int(cb.data[6:])
    USER_MIN_GIFTS[cb.from_user.id] = val
    try:
        await cb.message.edit_text(
            "<b>Мин. гифтов: " + str(val) + "\n\n"
            "Шаг 2 из 4 — Максимум гифтов\nВыбери:</b>",
            parse_mode="HTML", reply_markup=ob_max_kb()
        )
    except Exception:
        await cb.message.answer(
            "<b>Шаг 2 из 4 — Максимум гифтов\nВыбери:</b>",
            parse_mode="HTML", reply_markup=ob_max_kb()
        )
    await state.set_state(Onboarding.max_gifts)
    await cb.answer()


@dp.callback_query(F.data.startswith("obmax_"))
async def ob_max_btn(cb: CallbackQuery, state: FSMContext):
    if await state.get_state() != Onboarding.max_gifts.state:
        await cb.answer()
        return
    val = int(cb.data[6:])
    USER_MAX_GIFTS[cb.from_user.id] = val
    lbl = "без лимита" if val == 0 else str(val)
    try:
        await cb.message.edit_text(
            "<b>Макс. гифтов: " + lbl + "\n\n"
            "Шаг 3 из 4 — Лимит выдачи результатов\nВыбери:</b>",
            parse_mode="HTML", reply_markup=ob_lim_kb()
        )
    except Exception:
        await cb.message.answer(
            "<b>Шаг 3 из 4 — Лимит выдачи\nВыбери:</b>",
            parse_mode="HTML", reply_markup=ob_lim_kb()
        )
    await state.set_state(Onboarding.limit)
    await cb.answer()


@dp.callback_query(F.data.startswith("oblim_"))
async def ob_lim_btn(cb: CallbackQuery, state: FSMContext):
    if await state.get_state() != Onboarding.limit.state:
        await cb.answer()
        return
    val = int(cb.data[6:])
    USER_LIMIT[cb.from_user.id] = val
    try:
        await cb.message.edit_text(
            "<b>Лимит: " + str(val) + "\n\n"
            "Шаг 4 из 4 — Регион поиска\nВыбери:</b>",
            parse_mode="HTML", reply_markup=ob_region_kb()
        )
    except Exception:
        await cb.message.answer(
            "<b>Шаг 4 из 4 — Регион поиска\nВыбери:</b>",
            parse_mode="HTML", reply_markup=ob_region_kb()
        )
    await state.set_state(Onboarding.region)
    await cb.answer()


@dp.callback_query(F.data.startswith("obreg_"))
async def ob_reg_btn(cb: CallbackQuery, state: FSMContext):
    if await state.get_state() != Onboarding.region.state:
        await cb.answer()
        return
    key = cb.data[6:]
    if key not in REGIONS:
        await cb.answer()
        return
    USER_REGION[cb.from_user.id] = key
    await _finish_onboarding(cb.from_user.id, state, cb.message)
    await cb.answer()


async def _finish_onboarding(uid, state, msg):
    ONBOARDING_DONE.add(uid)
    save_onboarding()
    await state.clear()
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    reg  = REGIONS.get(get_region(uid), {}).get("label", "Все страны")
    try:
        await msg.edit_text(
            "<b>Настройка завершена!\n\n"
            "Мин. гифтов: " + str(mn) + "\n"
            "Макс. гифтов: " + mx_s + "\n"
            "Лимит: " + str(get_limit(uid)) + "\n"
            "Регион: " + reg + "\n\n"
            "Менять можно в Настройках</b>",
            parse_mode="HTML", reply_markup=main_menu_kb()
        )
    except Exception:
        await msg.answer(
            "<b>Настройка завершена!\n\n"
            "Мин. гифтов: " + str(mn) + "\nМакс. гифтов: " + mx_s + "\n"
            "Лимит: " + str(get_limit(uid)) + "\nРегион: " + reg + "</b>",
            parse_mode="HTML", reply_markup=main_menu_kb()
        )


# ── /clear прерывает онбординг / поиск ───────────────────────────────────────
@dp.message(Command("clear"))
async def cmd_clear(message: Message, state: FSMContext):
    global is_searching
    cur_state = await state.get_state()
    # Прерываем любое состояние FSM включая онбординг
    await state.clear()
    # Если был онбординг — выходим с дефолтами
    if cur_state and cur_state.startswith("Onboarding:"):
        uid = message.from_user.id
        if uid not in USER_MIN_GIFTS:
            USER_MIN_GIFTS[uid] = DEFAULT_MIN_GIFTS
        if uid not in USER_MAX_GIFTS:
            USER_MAX_GIFTS[uid] = DEFAULT_MAX_GIFTS
        if uid not in USER_LIMIT:
            USER_LIMIT[uid] = DEFAULT_LIMIT
        ONBOARDING_DONE.add(uid)
        save_onboarding()
        await message.answer(
            "<b>Настройка пропущена. Используются значения по умолчанию.</b>",
            parse_mode="HTML",
            reply_markup=main_menu_kb()
        )
        return
    if is_searching:
        is_searching = False
        await message.answer("<b>Поиск остановлен.</b>", parse_mode="HTML", reply_markup=menu_kb())
    else:
        await message.answer("<b>Поиск не идёт.</b>", parse_mode="HTML", reply_markup=menu_kb())

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer("<b>ID: <code>" + str(message.from_user.id) + "</code></b>", parse_mode="HTML")

@dp.message(Command("neptunteam"))
async def cmd_neptunteam(message: Message, state: FSMContext):
    # Команда работает в любом состоянии — очищаем FSM
    await state.clear()
    uid  = message.from_user.id
    # Если был онбординг — ставим дефолты
    if uid not in ONBOARDING_DONE:
        if uid not in USER_MIN_GIFTS:
            USER_MIN_GIFTS[uid] = DEFAULT_MIN_GIFTS
        if uid not in USER_MAX_GIFTS:
            USER_MAX_GIFTS[uid] = DEFAULT_MAX_GIFTS
        if uid not in USER_LIMIT:
            USER_LIMIT[uid] = DEFAULT_LIMIT
        ONBOARDING_DONE.add(uid)
        save_onboarding()
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
        "Ищет свежие лоты — разные коллекции (round-robin).\n"
        "Параллельно наполняет БД PriceNFT.\n\n"
        "По профилю\n"
        "Ищет аккаунты с NFT и строго 0 лотов на маркете.\n"
        "Быстрый источник: БД PriceNFT + чаты.\n\n"
        "По модели\n"
        "Сначала БД @PriceNFTbot по модели, потом маркет.\n"
        "Кнопка «Написать» включает ссылку на NFT.\n\n"
        "НАСТРОЙКИ\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n"
        "Лимит выдачи: " + str(lim) + "\n"
        "Регион: " + reg_l + "\n\n"
        "КОМАНДЫ\n"
        "/start — главное меню\n"
        "/clear — остановить поиск / прервать настройку\n"
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


# ── ONBOARDING HANDLERS ───────────────────────────────────────────────────────


# ── CALLBACKS: NAVIGATION ─────────────────────────────────────────────────────
@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    global is_searching
    is_searching = False
    await state.clear()
    uid  = cb.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await cb.answer()
    await safe_edit(
        cb.message,
        "<b>Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n\n"
        "Выбери действие:</b>",
        reply_markup=main_menu_kb(),
    )

@dp.callback_query(F.data == "search_mode_select")
async def cb_search_mode(cb: CallbackQuery):
    await cb.answer()
    await safe_edit(cb.message, "<b>Выбери режим поиска:</b>", reply_markup=search_mode_select_kb())

@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.answer()
    await safe_edit(cb.message, "<b>Маркет — выбери ценовую категорию:</b>", reply_markup=cat_kb("market"))

@dp.callback_query(F.data == "mode_profile")
async def cb_mode_profile(cb: CallbackQuery):
    await cb.answer()
    await safe_edit(cb.message, "<b>Профиль — выбери ценовую категорию:</b>", reply_markup=cat_kb("profile"))

@dp.callback_query(F.data == "mode_model")
async def cb_mode_model(cb: CallbackQuery):
    if not ALL_GIFT_IDS:
        await cb.answer("Коллекции не загружены", show_alert=True)
        return
    await cb.answer()
    await safe_edit(cb.message, "<b>По модели — выбери тип поиска:</b>", reply_markup=model_search_type_kb())

@dp.callback_query(F.data.startswith("mc_"))
async def cb_mc(cb: CallbackQuery):
    cat = cb.data[3:]
    lbl = CAT_LABELS.get(cat, cat)
    await cb.answer()
    await safe_edit(cb.message, "<b>Маркет / " + lbl + "\nКого искать?</b>", reply_markup=who_kb("market", cat))

@dp.callback_query(F.data.startswith("pc_"))
async def cb_pc(cb: CallbackQuery):
    cat = cb.data[3:]
    lbl = CAT_LABELS.get(cat, cat)
    await cb.answer()
    await safe_edit(cb.message, "<b>Профиль / " + lbl + "\nКого искать?</b>", reply_markup=who_kb("profile", cat))

@dp.callback_query(F.data.startswith("go_market_"))
async def cb_go_market(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    rest  = cb.data[len("go_market_"):]
    parts = rest.rsplit("_", 1)
    cat   = parts[0] if len(parts) == 2 else rest
    who   = parts[1] if len(parts) == 2 else "all"
    await _start_market(cb, cat=cat, girls=(who == "girls"))

@dp.callback_query(F.data.startswith("go_profile_"))
async def cb_go_profile(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    rest  = cb.data[len("go_profile_"):]
    parts = rest.rsplit("_", 1)
    cat   = parts[0] if len(parts) == 2 else rest
    who   = parts[1] if len(parts) == 2 else "all"
    await _start_profile(cb, cat=cat, girls=(who == "girls"))

@dp.callback_query(F.data.startswith("mdltype_"))
async def cb_mdltype(cb: CallbackQuery):
    search_type = cb.data[len("mdltype_"):]  # "market" or "profile"
    if not ALL_GIFT_IDS:
        await cb.answer("Коллекции не загружены", show_alert=True)
        return
    lbl = "маркету" if search_type == "market" else "профилю"
    await cb.answer()
    await safe_edit(cb.message, "<b>По модели / " + lbl + " — кого искать?</b>", reply_markup=model_who_kb(search_type))

@dp.callback_query(F.data.startswith("mdlwho_"))
async def cb_mdlwho(cb: CallbackQuery):
    # mdlwho_{search_type}_{who}
    rest = cb.data[len("mdlwho_"):]
    idx = rest.find("_")
    if idx == -1:
        await cb.answer()
        return
    search_type = rest[:idx]
    who = rest[idx+1:]
    if not ALL_GIFT_IDS:
        await cb.answer("Коллекции не загружены", show_alert=True)
        return
    lbl = "Девушки-модели" if who == "girls" else "Все модели"
    lbl2 = "маркету" if search_type == "market" else "профилю"
    await cb.answer()
    await safe_edit(
        cb.message,
        "<b>" + lbl + " / по " + lbl2 + " — выбери коллекцию:</b>",
        reply_markup=model_col_kb(who, search_type, ALL_GIFT_IDS, page=0),
    )

@dp.callback_query(F.data.startswith("mdlpage_"))
async def cb_mdlpage(cb: CallbackQuery):
    # mdlpage_{w}_{t}_{page}  где w=a/g, t=m/p
    rest = cb.data[len("mdlpage_"):]
    parts = rest.split("_")
    if len(parts) < 3:
        await cb.answer()
        return
    who = _expand_who(parts[0])
    search_type = _expand_stype(parts[1])
    try:
        page = int(parts[2])
    except ValueError:
        await cb.answer()
        return
    if not ALL_GIFT_IDS:
        await cb.answer("Коллекции не загружены", show_alert=True)
        return
    await cb.answer()
    lbl = "Девушки-модели" if who == "girls" else "Все модели"
    lbl2 = "маркету" if search_type == "market" else "профилю"
    await safe_edit(
        cb.message,
        "<b>" + lbl + " / по " + lbl2 + " — выбери коллекцию:</b>",
        reply_markup=model_col_kb(who, search_type, ALL_GIFT_IDS, page=page),
    )

@dp.callback_query(F.data == "noop")
async def cb_noop(cb: CallbackQuery):
    await cb.answer()

@dp.callback_query(F.data.startswith("mdlrun_"))
async def cb_mdlrun(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    rest = cb.data[len("mdlrun_"):]
    # Формат: mdlrun_{w}_{t}_{gid|all}
    parts = rest.split("_")
    if len(parts) < 3:
        await cb.answer("Ошибка кнопки", show_alert=True)
        return
    who         = _expand_who(parts[0])
    search_type = _expand_stype(parts[1])
    gid_s       = "_".join(parts[2:])
    gid = None
    if gid_s != "all":
        try:
            gid = int(gid_s)
        except ValueError:
            await cb.answer("Некорректная коллекция", show_alert=True)
            return
    if not begin_search():
        await cb.answer("Поиск уже идёт! Стоп или /clear", show_alert=True)
        return
    await cb.answer("Запускаю...")
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    # begin_search уже включил флаг — _start_model не должен звать begin_search снова
    await _start_model(cb, girls=(who == "girls"), single_gid=gid, search_type=search_type,
                       already_answered=True, skip_begin=True)

# Оставляем старый mdl_who_ для совместимости (на случай кешированных сообщений)
@dp.callback_query(F.data.startswith("mdl_who_"))
async def cb_mdl_who_legacy(cb: CallbackQuery):
    who = cb.data[len("mdl_who_"):]
    if not ALL_GIFT_IDS:
        await cb.answer("Коллекции не загружены", show_alert=True)
        return
    lbl = "Девушки-модели" if who == "girls" else "Все модели"
    await cb.answer()
    await safe_edit(
        cb.message,
        "<b>" + lbl + " — выбери коллекцию:</b>",
        reply_markup=model_col_kb(who, "market", ALL_GIFT_IDS, page=0),
    )

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
    await cb.answer()
    await safe_edit(
        cb.message,
        "<b>Статистика\n\n"
        "Поисков: " + str(stats["checks"]) + "\n"
        "Найдено: " + str(stats["found"]) + "\n"
        "Пользователей: " + str(get_user_count()) + "\n\n"
        "Настройки:\n"
        "Мин: " + str(mn) + "  Макс: " + mx_s + "\n"
        "Лимит: " + str(lim) + "\n"
        "Регион: " + reg + "</b>",
        reply_markup=main_menu_kb(),
    )

@dp.callback_query(F.data.startswith("shownft_"))
async def cb_show_nft(cb: CallbackQuery):
    uid    = int(cb.data[8:])
    cached = NFT_CACHE.get(uid)
    if not cached:
        await cb.answer("Загружаю NFT...")
        saved = await fetch_saved_gifts(uid)
        if not saved:
            await cb.answer("NFT не найдены или профиль закрыт", show_alert=True)
            return
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
    await cb.answer()
    await safe_edit(
        cb.message,
        "<b>Настройки поиска\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "</b>",
        reply_markup=settings_menu_kb(uid),
    )

@dp.callback_query(F.data == "set_min")
async def cb_set_min(cb: CallbackQuery, state: FSMContext):
    await cb.answer()
    await safe_edit(cb.message, "<b>Введи минимум гифтов (число от 1):</b>", reply_markup=input_cancel_kb())
    await state.set_state(SetMin.value)

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
    await cb.answer()
    await safe_edit(cb.message, "<b>Введи максимум гифтов (0 = без лимита):</b>", reply_markup=input_cancel_kb())
    await state.set_state(SetMax.value)

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
    await cb.answer()
    await safe_edit(cb.message, "<b>Буст цен\n100% = до x2 флора  200% = до x3</b>", reply_markup=boost_kb())

@dp.callback_query(F.data.startswith("bst_"))
async def cb_bst(cb: CallbackQuery, state: FSMContext):
    raw = cb.data[4:]
    if raw == "custom":
        await cb.answer()
        await safe_edit(cb.message, "<b>Введи буст вручную (число %):</b>", reply_markup=input_cancel_kb())
        await state.set_state(SetBoost.value)
        return
    val = int(raw)
    USER_BOOST[cb.from_user.id] = val
    await cb.answer("Буст: " + str(val) + "%")
    uid = cb.from_user.id
    await safe_edit(
        cb.message,
        "<b>Буст: " + str(val) + "%\n\nНастройки поиска</b>",
        reply_markup=settings_menu_kb(uid),
    )

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
    await cb.answer()
    await safe_edit(cb.message, "<b>Лимит выдачи результатов:</b>", reply_markup=limit_kb(lim))

@dp.callback_query(F.data.startswith("lim_"))
async def cb_lim(cb: CallbackQuery):
    val = int(cb.data[4:])
    USER_LIMIT[cb.from_user.id] = val
    await cb.answer("Лимит: " + str(val))
    await safe_edit(
        cb.message,
        "<b>Лимит: " + str(val) + "\n\nНастройки поиска</b>",
        reply_markup=settings_menu_kb(cb.from_user.id),
    )

@dp.callback_query(F.data == "set_region")
async def cb_set_region(cb: CallbackQuery):
    reg = get_region(cb.from_user.id)
    await cb.answer()
    await safe_edit(cb.message, "<b>Выбери регион поиска:</b>", reply_markup=region_kb(reg))

@dp.callback_query(F.data.startswith("reg_"))
async def cb_reg(cb: CallbackQuery):
    key = cb.data[4:]
    if key not in REGIONS:
        await cb.answer("Неизвестный регион", show_alert=True)
        return
    USER_REGION[cb.from_user.id] = key
    lbl = REGIONS[key]["label"]
    await cb.answer("Регион: " + lbl)
    await safe_edit(
        cb.message,
        "<b>Регион: " + lbl + "\n\nНастройки поиска</b>",
        reply_markup=settings_menu_kb(cb.from_user.id),
    )


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

@dp.callback_query(F.data == "admin_pricenft_collect")
async def cb_pricenft_collect(cb: CallbackQuery):
    """Отдельная кнопка БД: сбор всех владельцев из @PriceNFTbot, можно Стопнуть."""
    if not is_admin(cb.from_user.id):
        return
    global _pricenft_collecting, _pricenft_task
    if _pricenft_collecting:
        await cb.answer("Сбор уже идёт — жми Стоп", show_alert=True)
        return
    if not await check_authorized():
        await cb.answer("Сначала авторизуй Telethon", show_alert=True)
        return
    await cb.answer("Стартую сбор БД...")
    stop_kb_db = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ Стоп сбор БД", callback_data="admin_pricenft_stop")],
        [InlineKeyboardButton(text="Админ", callback_data="admin_panel")],
    ])
    status = await cb.message.answer(
        "<b>📦 База данных</b>\n"
        "Цель: " + str(DB_TARGET_USERS) + " юзеров\n"
        "Сейчас: " + str(pricenft_db_stats().get("users", 0)) + "\n"
        "1) Быстрый маркет→БД\n"
        "2) @PriceNFTbot (если нет FloodWait)\n"
        "Можно остановить в любой момент",
        parse_mode="HTML",
        reply_markup=stop_kb_db,
    )

    async def prog(text):
        try:
            await status.edit_text(
                "<b>📦 База данных</b>\n" + esc(text),
                parse_mode="HTML",
                reply_markup=stop_kb_db,
            )
        except Exception:
            pass

    async def runner():
        global _pricenft_collecting, _pricenft_stop
        _pricenft_collecting = True
        _pricenft_stop = False
        try:
            # сначала быстро набираем с маркета к 100к
            await prog("Старт: маркет→БД до " + str(DB_TARGET_USERS) + " юзеров...")
            mres = await fill_db_from_market_fast(progress_cb=prog, target_users=DB_TARGET_USERS)
            if _pricenft_stop:
                try:
                    await status.edit_text(
                        "<b>⏹ Сбор остановлен</b>\n"
                        "Юзеров: " + str(mres.get("users", 0)) + " / " + str(DB_TARGET_USERS) + "\n"
                        "NFT: " + str(mres.get("nfts", 0)),
                        parse_mode="HTML", reply_markup=admin_kb()
                    )
                except Exception:
                    pass
                return
            # collect_pricenft_db сам ставит flag — временно снимем чтобы не got already_running
            _pricenft_collecting = False
            result = await collect_pricenft_db(progress_cb=prog, max_models=0)
            _pricenft_collecting = True
            # ещё раз маркет если не добили
            if int(pricenft_db_stats().get("users", 0) or 0) < DB_TARGET_USERS and not _pricenft_stop:
                await fill_db_from_market_fast(progress_cb=prog, target_users=DB_TARGET_USERS)
                result = {**(result or {}), **pricenft_db_stats()}
            try:
                if result.get("ok") or mres.get("ok"):
                    if result.get("flood_fallback"):
                        wait = int(result.get("flood_wait", 0) or 0)
                        hrs = max(1, wait // 3600) if wait else "?"
                        await status.edit_text(
                            "<b>⚠️ PriceNFT FloodWait ~" + str(hrs) + "ч</b>\n"
                            "БД наполнена с маркета:\n"
                            "Юзеров: " + str(result.get("users", mres.get("users", 0))) + " / " + str(DB_TARGET_USERS) + "\n"
                            "NFT: " + str(result.get("nfts", mres.get("nfts", 0))),
                            parse_mode="HTML", reply_markup=admin_kb()
                        )
                    else:
                        prefix = "⏹ Сбор остановлен" if result.get("stopped") else "✅ БД обновлена"
                        st = pricenft_db_stats()
                        await status.edit_text(
                            "<b>" + prefix + "</b>\n"
                            "Моделей: " + str(st.get("models", 0)) + "\n"
                            "Юзеров: " + str(st.get("users", 0)) + " / " + str(DB_TARGET_USERS) + "\n"
                            "NFT: " + str(st.get("nfts", 0)),
                            parse_mode="HTML", reply_markup=admin_kb()
                        )
                else:
                    await status.edit_text(
                        "<b>❌ Сбор не удался:</b> " + esc(str((result or {}).get("error", "?"))),
                        parse_mode="HTML", reply_markup=admin_kb()
                    )
            except Exception:
                pass
        finally:
            _pricenft_collecting = False
            _pricenft_stop = False

    _pricenft_task = asyncio.create_task(runner())

@dp.callback_query(F.data == "admin_pricenft_stop")
async def cb_pricenft_stop(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    if not _pricenft_collecting:
        await cb.answer("Сбор не идёт", show_alert=True)
        return
    stop_pricenft_collect()
    await cb.answer("Останавливаю сбор...")
    try:
        await cb.message.answer(
            "<b>⏹ Останавливаю сбор БД...</b>\nДождусь текущего запроса к PriceNFTbot.",
            parse_mode="HTML", reply_markup=admin_kb()
        )
    except Exception:
        pass

@dp.callback_query(F.data == "admin_clear_seen")
async def cb_admin_clear_seen(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    clear_seen_db()
    await cb.message.answer(
        "<b>🧹 Антидубль сброшен</b>\nАккаунты и гифты снова могут попадаться в выдаче.",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await cb.answer("Ок")

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await cb.message.answer("<b>Отправь сообщение для рассылки.</b>",
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
    pn = pricenft_db_stats()
    await cb.message.answer(
        "<b>Статистика\n\n"
        "Пользователей: " + str(len(u)) + "\n"
        "Поисков: " + str(stats["checks"]) + "\n"
        "Найдено: " + str(stats["found"]) + "\n"
        "Коллекций: " + str(len(ALL_GIFT_IDS)) + "\n"
        "PriceNFT БД: " + str(pn.get("models", 0)) + " моделей / "
        + str(pn.get("users", 0)) + " юзеров (цель " + str(DB_TARGET_USERS) + ") / "
        + str(pn.get("nfts", 0)) + " nft\n"
        "Антидубль: " + str(pn.get("seen_owners", 0)) + " аккаунтов / "
        + str(pn.get("seen_gifts", 0)) + " гифтов</b>",
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
            "Коллекций: " + str(len(ALL_GIFT_IDS)) + "\n"
            "Сохраняю все гифты в БД...</b>",
            parse_mode="HTML", reply_markup=main_menu_kb()
        )
        start_bootstrap_gifts_db(notify_chat_id=message.chat.id)
        start_background_db_keeper()
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
            "Коллекций: " + str(len(ALL_GIFT_IDS)) + "\n"
            "Сохраняю все гифты в БД...</b>",
            parse_mode="HTML", reply_markup=main_menu_kb()
        )
        start_bootstrap_gifts_db(notify_chat_id=message.chat.id)
        start_background_db_keeper()
    except Exception as e:
        await message.answer("<b>Неверный пароль: <code>" + esc(str(e)) + "</code></b>", parse_mode="HTML")



# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    global ONBOARDING_DONE
    ONBOARDING_DONE = load_onboarding()
    load_pricenft_db()
    try:
        _load_pricenft_flood()
    except Exception:
        pass
    try:
        load_seen_into_memory()
    except Exception as e:
        logger.warning("load_seen: %s", e)
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("Neptun Parser запущен!")
    st = pricenft_db_stats()
    logger.info(
        "PriceNFT БД: models=%s users=%s nfts=%s seen=%s/%s",
        st.get("models"), st.get("users"), st.get("nfts"),
        st.get("seen_owners"), st.get("seen_gifts"),
    )
    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start",      description="Главное меню"),
        BotCommand(command="clear",      description="Остановить поиск / прервать настройку"),
        BotCommand(command="neptunteam", description="Справка"),
    ])
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info("Авторизован, коллекций: %d", len(ALL_GIFT_IDS))
            st0 = pricenft_db_stats()
            if st0.get("users", 0) < 50:
                logger.info("БД мала (%s) — автосбор гифтов", st0.get("users"))
                start_bootstrap_gifts_db(notify_chat_id=ADMIN_ID)
            # основной режим: постоянно пишем маркет + PriceNFT в локальную БД
            start_background_db_keeper()
        else:
            logger.warning("Не авторизован — пройди /start")
    except Exception as e:
        logger.error("Ошибка старта: %s", e)
    try:
        await dp.start_polling(bot)
    finally:
        try:
            db_flush(force=True)
            save_pricenft_db()
        except Exception:
            pass
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
