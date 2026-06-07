import asyncio
import logging
import urllib.parse
import os
import json
import time
import datetime
import random

from telethon import TelegramClient
from telethon.tl.functions.payments import (
    GetResaleStarGiftsRequest, GetStarGiftsRequest
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

# ── CONFIG ────────────────────────────────────────────────────────────────────
API_ID       = 36101343
API_HASH     = "116195fa5e0459d25a9a6266b40807d7"
BOT_TOKEN    = "8406363273:AAG-ucchhMA09n8j_XSGFtE02iu3Oiwzj_0"
ADMIN_ID     = 8726084830
SESSION_NAME = "nft_session"
USERS_FILE   = "users.json"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot       = Bot(token=BOT_TOKEN)
dp        = Dispatcher(storage=MemoryStorage())
tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

# ── STATE ─────────────────────────────────────────────────────────────────────
stats           = {"checks": 0, "found": 0}
is_searching    = False
ALL_GIFT_IDS    = []        # [(gid, title), ...]
NFT_COLLECTIONS = {}        # title -> gid
PRICE_FLOOR_CACHE = {}
NFT_CACHE       = {}        # uid -> {...}

USER_BOOST      = {}
USER_MIN_GIFTS  = {}
USER_MAX_GIFTS  = {}
USER_LIMIT      = {}

DEFAULT_BOOST     = 100
DEFAULT_MIN_GIFTS = 1
DEFAULT_MAX_GIFTS = 5
DEFAULT_LIMIT     = 30

CAT_LABELS = {
    "cheap":   "Дешевые до 2000",
    "mid":     "Средние 2000-5000",
    "hard":    "Сложные 5000-20000",
    "ultra":   "Хард 20000-100000",
    "extreme": "Экстрим от 100000",
}

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
        u[key] = {
            "username": username or "",
            "first_name": first_name or "",
            "last_name": last_name or "",
            "joined": int(time.time()),
        }
        save_users(u)
        return True
    changed = False
    if username:   u[key]["username"]   = username;   changed = True
    if first_name: u[key]["first_name"] = first_name; changed = True
    if last_name:  u[key]["last_name"]  = last_name;  changed = True
    if changed:    save_users(u)
    return False

def get_user_count():
    return len(load_users())

def get_boost(uid):     return USER_BOOST.get(uid, DEFAULT_BOOST)
def get_min_gifts(uid): return USER_MIN_GIFTS.get(uid, DEFAULT_MIN_GIFTS)
def get_max_gifts(uid): return USER_MAX_GIFTS.get(uid, DEFAULT_MAX_GIFTS)
def get_limit(uid):     return USER_LIMIT.get(uid, DEFAULT_LIMIT)
def is_admin(uid):      return int(uid) == ADMIN_ID

# ── FSM ───────────────────────────────────────────────────────────────────────
class Auth(StatesGroup):
    phone    = State()
    code     = State()
    password = State()

class Broadcast(StatesGroup):
    message = State()

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
    return str(t).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

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
    return floor * 0.7 <= price <= floor * (1.0 + boost / 100.0)

def cache_owner(uid, owner, username, name, profile_url, items):
    NFT_CACHE[uid] = {
        "owner": owner, "username": username,
        "name": name, "profile_url": profile_url, "items": items,
    }

# ── KEYBOARDS ─────────────────────────────────────────────────────────────────
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск по маркету",     callback_data="mode_market")],
        [InlineKeyboardButton(text="👧 Поиск девушек",         callback_data="mode_girls")],
        [InlineKeyboardButton(text="⚙️ Настройки",            callback_data="settings_menu"),
         InlineKeyboardButton(text="📊 Статистика",           callback_data="stats")],
    ])

def cat_kb(mode):
    p = "mc_" if mode == "market" else "gc_"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Дешевые до 2000",    callback_data=p + "cheap")],
        [InlineKeyboardButton(text="Средние 2000-5000",  callback_data=p + "mid")],
        [InlineKeyboardButton(text="Сложные 5000-20000", callback_data=p + "hard")],
        [InlineKeyboardButton(text="Хард 20000-100000",  callback_data=p + "ultra")],
        [InlineKeyboardButton(text="Экстрим от 100000",  callback_data=p + "extreme")],
        [InlineKeyboardButton(text="Все коллекции",      callback_data=p + "all")],
        [InlineKeyboardButton(text="◀️ Назад",           callback_data="menu")],
    ])

def settings_menu_kb(uid):
    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    lim = get_limit(uid)
    bst = get_boost(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мин. гифтов: " + str(mn),   callback_data="set_min")],
        [InlineKeyboardButton(text="Макс. гифтов: " + mx_s,     callback_data="set_max")],
        [InlineKeyboardButton(text="Лимит выдачи: " + str(lim), callback_data="set_limit")],
        [InlineKeyboardButton(text="Буст цен: " + str(bst) + "%", callback_data="set_boost")],
        [InlineKeyboardButton(text="◀️ Назад",                   callback_data="menu")],
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
        [InlineKeyboardButton(text="◀️ Назад",        callback_data="settings_menu")],
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
        [InlineKeyboardButton(text="◀️ Назад", callback_data="settings_menu")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⛔ СТОП", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Поиск",   callback_data="mode_market"),
         InlineKeyboardButton(text="👧 Девушки", callback_data="mode_girls")],
        [InlineKeyboardButton(text="🏠 Меню",    callback_data="menu")],
    ])

def owner_card_kb(username, profile_url, owner_uid):
    btns = []
    if username:
        btns.append([InlineKeyboardButton(text="@" + username, url="https://t.me/" + username)])
        msg = urllib.parse.quote("Привет хочу купить твои NFT")
        btns.append([InlineKeyboardButton(text="✉️ Написать", url="https://t.me/" + username + "?text=" + msg)])
    elif profile_url:
        btns.append([InlineKeyboardButton(text="Профиль", url=profile_url)])
    btns.append([InlineKeyboardButton(text="📦 Все NFT", callback_data="shownft_" + str(owner_uid))])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def nft_list_kb(items, username, profile_url):
    btns = []
    for g in items:
        url = g.get("nft_url")
        if url:
            lbl = str(g.get("title", "?")) + " #" + str(g.get("num", "?"))
            btns.append([InlineKeyboardButton(text=lbl, url=url)])
    if username:
        btns.append([InlineKeyboardButton(text="@" + username, url="https://t.me/" + username)])
        msg = urllib.parse.quote("Привет хочу купить твои NFT")
        btns.append([InlineKeyboardButton(text="✉️ Написать", url="https://t.me/" + username + "?text=" + msg)])
    elif profile_url:
        btns.append([InlineKeyboardButton(text="Профиль", url=profile_url)])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

def input_cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Отмена", callback_data="settings_menu")],
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

# ── COLLECTIONS ───────────────────────────────────────────────────────────────
async def load_collections():
    global ALL_GIFT_IDS, NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        ALL_GIFT_IDS    = []
        NFT_COLLECTIONS = {}
        seen = set()
        for gift in (getattr(result, "gifts", None) or []):
            gid = getattr(gift, "id", None)
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
            col_title = next((t for t, i in NFT_COLLECTIONS.items() if i == gid), None)
            items     = []
            for gift in (getattr(result, "gifts", None) or []):
                owner, oid = get_owner(gift, users_map)
                username   = getattr(owner, "username", None) if owner else None
                fn = (getattr(owner, "first_name", "") or "") if owner else ""
                ln = (getattr(owner, "last_name",  "") or "") if owner else ""
                name = (fn + " " + ln).strip()
                nft_url     = make_nft_url(gift)
                profile_url = ("https://t.me/" + username) if username else (("tg://user?id=" + str(oid)) if oid else None)
                raw_title   = getattr(gift, "title", None)
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

def _make_nft_lines(items):
    lines = ""
    for it in items[:5]:
        nu = it.get("nft_url")
        t  = esc(str(it.get("title", "?")))
        n  = esc(str(it.get("num", "?")))
        p  = it.get("price")
        ps = " — " + str(p) + " ⭐" if p else ""
        if nu:
            lines += '\n<a href="' + nu + '">' + t + " #" + n + ps + "</a>"
        else:
            lines += "\n" + t + " #" + n + ps
    if len(items) > 5:
        lines += "\n+ ещё " + str(len(items) - 5) + " NFT"
    return lines

# ── SEARCH CORE ───────────────────────────────────────────────────────────────
async def do_market_search(status_msg, gift_ids, cat=None, girls_only=False,
                           boost=100, min_gifts=1, max_gifts=5, max_results=30):
    """
    Поиск по маркету с жёсткой дедупликацией:
      - seen_nft_slugs: каждый NFT (slug) считается только 1 раз
      - seen_owners: каждый владелец отправляется только 1 раз
      - owner_map: накапливаем NFT по владельцу, отправляем когда min_gifts достигнут
    """
    global is_searching
    is_searching = True

    lock        = asyncio.Lock()
    found       = [0]
    seen_nft_slugs = set()   # дедупликация отдельных NFT
    seen_owners    = set()   # дедупликация владельцев (отправленных)
    owner_map      = {}      # uid -> {owner, username, name, profile_url, items:[]}

    async def try_send_owner(uid):
        """Отправляет карточку владельца если выполнены условия."""
        async with lock:
            if uid in seen_owners or found[0] >= max_results:
                return
            bucket = owner_map.get(uid)
            if not bucket:
                return
            cnt = len(bucket["items"])
            if not gifts_in_range(cnt, min_gifts, max_gifts):
                return
            # Снимаем из map, помечаем как отправленного
            owner_map.pop(uid, None)
            seen_owners.add(uid)
            to_send = bucket

        cnt     = len(to_send["items"])
        owner_s = fmt_owner(to_send["owner"], to_send["username"], to_send["name"])
        cache_owner(uid, to_send["owner"], to_send["username"],
                    to_send["name"], to_send["profile_url"], to_send["items"])
        txt = (
            "<b>" + owner_s + "\nNFT на маркете: " + str(cnt) + "</b>"
            + _make_nft_lines(to_send["items"])
        )
        kb = owner_card_kb(to_send["username"], to_send["profile_url"], uid)
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

    async def scan_col(gid):
        fl = None
        if cat and cat != "all":
            fl = await get_floor(gid)
            if fl is not None and not floor_in_cat(fl, cat):
                return  # коллекция не в нужном ценовом диапазоне

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

                # Пропускаем уже обработанные NFT и владельцев
                async with lock:
                    if slug and slug in seen_nft_slugs:
                        continue
                    if not oid or oid in seen_owners:
                        continue
                    if slug:
                        seen_nft_slugs.add(slug)

                # Фильтр по цене
                price = item.get("price")
                if cat and cat != "all" and fl and price and not price_ok(price, fl, boost):
                    continue

                # Фильтр по девушкам
                if girls_only and not is_girl(item["owner"], item["username"], item["name"]):
                    continue

                # Накапливаем NFT для этого владельца
                async with lock:
                    if oid in seen_owners:
                        continue
                    if oid not in owner_map:
                        owner_map[oid] = {
                            "owner":       item["owner"],
                            "username":    item["username"],
                            "name":        item["name"],
                            "profile_url": item["profile_url"],
                            "items":       [],
                        }
                    owner_map[oid]["items"].append(item)
                    cnt = len(owner_map[oid]["items"])
                    ready = gifts_in_range(cnt, min_gifts, max_gifts)

                if ready:
                    await try_send_owner(oid)
                    if found[0] >= max_results:
                        return

            if not nxt:
                break
            offset = nxt

    # Финальный проход: отправляем накопленных владельцев которые не были отправлены
    async def flush_pending():
        async with lock:
            pending = list(owner_map.keys())
        for uid in pending:
            if not is_searching or found[0] >= max_results:
                break
            await try_send_owner(uid)

    try:
        await status_msg.edit_text(
            "<b>🔍 Идёт поиск по маркету...</b>",
            parse_mode="HTML", reply_markup=stop_kb()
        )

        valid_ids = list(gift_ids)
        random.shuffle(valid_ids)
        PARALLEL = 10

        for i in range(0, len(valid_ids), PARALLEL):
            if not is_searching or found[0] >= max_results:
                break
            batch = valid_ids[i:i + PARALLEL]
            await asyncio.gather(*[scan_col(gid) for gid in batch])
            await flush_pending()

        await flush_pending()

    except Exception as e:
        logger.error("do_market_search: %s", e)
    finally:
        is_searching = False

    return found[0]

# ── RUN HELPERS ───────────────────────────────────────────────────────────────
async def _start_search(cb, cat, girls_only):
    global is_searching
    if is_searching:
        await cb.answer("Поиск уже идёт!", show_alert=True)
        return
    await cb.answer("Запускаю...")
    stats["checks"] += 1

    uid   = cb.from_user.id
    ids   = await ensure_collections()
    if not ids:
        await cb.message.answer("<b>Коллекции не загружены.</b>", parse_mode="HTML", reply_markup=menu_kb())
        return

    boost = get_boost(uid)
    mn    = get_min_gifts(uid)
    mx    = get_max_gifts(uid)
    lim   = get_limit(uid)
    mx_s  = str(mx) if mx > 0 else "без лимита"
    cat_l = CAT_LABELS.get(cat, "Все коллекции") if cat and cat != "all" else "Все коллекции"
    who_l = "👧 Девушки" if girls_only else "👤 Все"

    txt = (
        "<b>" + who_l + " / " + cat_l + "\n"
        "Гифтов: от " + str(mn) + " до " + mx_s + "\n"
        "Буст: " + str(boost) + "%  |  Лимит: " + str(lim) + "</b>"
    )
    status = await cb.message.answer(txt, parse_mode="HTML", reply_markup=stop_kb())

    try:
        found = await asyncio.wait_for(
            do_market_search(
                status, ids,
                cat=cat if cat != "all" else None,
                girls_only=girls_only,
                boost=boost,
                min_gifts=mn,
                max_gifts=mx,
                max_results=lim,
            ),
            timeout=300
        )
    except asyncio.TimeoutError:
        is_searching = False
        found = 0

    try:
        await status.edit_text(
            "<b>✅ Готово. " + who_l + " / " + cat_l + "\nНайдено: " + str(found) + "</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass

# ── COMMANDS ──────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    add_user(uid, message.from_user.username,
             message.from_user.first_name, message.from_user.last_name)

    if not await check_authorized() and is_admin(uid):
        await message.answer(
            "<b>Нужна авторизация Telegram\nВведи номер телефона:</b>",
            parse_mode="HTML"
        )
        await state.set_state(Auth.phone)
        return

    mn  = get_min_gifts(uid)
    mx  = get_max_gifts(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await message.answer(
        "<b>🌊 Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n\n"
        "Выбери режим поиска:</b>",
        parse_mode="HTML",
        reply_markup=main_menu_kb()
    )

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("<b>Отменено.</b>", parse_mode="HTML", reply_markup=main_menu_kb())

@dp.message(Command("clear"))
async def cmd_clear(message: Message):
    global is_searching
    if is_searching:
        is_searching = False
        await message.answer("<b>Поиск остановлен.</b>", parse_mode="HTML", reply_markup=menu_kb())
    else:
        await message.answer("<b>Поиск не идёт.</b>", parse_mode="HTML", reply_markup=menu_kb())

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(
        "<b>ID: <code>" + str(message.from_user.id) + "</code></b>",
        parse_mode="HTML"
    )

@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(
            "<b>Нет доступа. ID: <code>" + str(message.from_user.id) + "</code></b>",
            parse_mode="HTML"
        )
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
        "<b>🌊 Neptun Parser\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "\n\n"
        "Выбери режим поиска:</b>",
        parse_mode="HTML", reply_markup=main_menu_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "mode_market")
async def cb_mode_market(cb: CallbackQuery):
    await cb.message.answer(
        "<b>🔍 Поиск по маркету\nВыбери ценовую категорию:</b>",
        parse_mode="HTML", reply_markup=cat_kb("market")
    )
    await cb.answer()

@dp.callback_query(F.data == "mode_girls")
async def cb_mode_girls(cb: CallbackQuery):
    await cb.message.answer(
        "<b>👧 Поиск девушек по маркету\nВыбери ценовую категорию:</b>",
        parse_mode="HTML", reply_markup=cat_kb("girls")
    )
    await cb.answer()

# mc_<cat> — маркет, все
@dp.callback_query(F.data.startswith("mc_"))
async def cb_mc(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    cat = cb.data[3:]
    await _start_search(cb, cat=cat, girls_only=False)

# gc_<cat> — маркет, девушки
@dp.callback_query(F.data.startswith("gc_"))
async def cb_gc(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    cat = cb.data[3:]
    await _start_search(cb, cat=cat, girls_only=True)

@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    if not is_searching:
        await cb.answer("У вас нет активного поиска", show_alert=True)
        return
    is_searching = False
    await cb.answer("Останавливаю...")
    try:
        await cb.message.edit_text(
            "<b>⛔ Поиск остановлен.</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass

@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    uid  = cb.from_user.id
    mn   = get_min_gifts(uid)
    mx   = get_max_gifts(uid)
    lim  = get_limit(uid)
    bst  = get_boost(uid)
    mx_s = str(mx) if mx > 0 else "без лимита"
    await cb.message.answer(
        "<b>📊 Статистика\n\n"
        "Поисков: " + str(stats["checks"]) + "\n"
        "Найдено: " + str(stats["found"]) + "\n"
        "Пользователей: " + str(get_user_count()) + "\n\n"
        "Настройки:\n"
        "Мин: " + str(mn) + "  Макс: " + mx_s + "\n"
        "Лимит: " + str(lim) + "  Буст: " + str(bst) + "%</b>",
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("shownft_"))
async def cb_show_nft(cb: CallbackQuery):
    uid    = int(cb.data[8:])
    cached = NFT_CACHE.get(uid)
    if not cached:
        await cb.answer("Нет данных в кэше", show_alert=True)
        return
    await cb.answer()
    items    = cached.get("items", [])
    username = cached.get("username")
    p_url    = cached.get("profile_url")
    owner_s  = fmt_owner(cached.get("owner"), username, cached.get("name"))
    if not items:
        await cb.answer("Список пуст", show_alert=True)
        return
    kb  = nft_list_kb(items, username, p_url)
    txt = "<b>📦 NFT " + owner_s + "\nВсего: " + str(len(items)) + "</b>"
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
        "<b>⚙️ Настройки поиска\n\n"
        "Мин. гифтов: " + str(mn) + "\n"
        "Макс. гифтов: " + mx_s + "</b>",
        parse_mode="HTML", reply_markup=settings_menu_kb(uid)
    )
    await cb.answer()

@dp.callback_query(F.data == "set_min")
async def cb_set_min(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer(
        "<b>Введи минимум гифтов (число от 1):</b>",
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
    await message.answer(
        "<b>✅ Мин. гифтов: " + str(val) + "</b>",
        parse_mode="HTML", reply_markup=settings_menu_kb(message.from_user.id)
    )

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
    await message.answer(
        "<b>✅ Макс. гифтов: " + lbl + "</b>",
        parse_mode="HTML", reply_markup=settings_menu_kb(message.from_user.id)
    )

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
    await message.answer(
        "<b>✅ Буст: " + str(val) + "%</b>",
        parse_mode="HTML", reply_markup=settings_menu_kb(message.from_user.id)
    )

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
    try:
        await cb.message.edit_reply_markup(reply_markup=limit_kb(val))
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
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Назад", callback_data="admin_panel")
        ]])
        fn = msg.edit_text if edit else msg.answer
        await fn("<b>Пользователей нет.</b>", parse_mode="HTML", reply_markup=kb)
        return
    start = page * PAGE
    end   = min(start + PAGE, total)
    chunk = all_items[start:end]
    lines = ["<b>Пользователи " + str(start + 1) + " - " + str(end) + " из " + str(total) + "</b>\n"]
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
        nav.append(InlineKeyboardButton(text="◀️", callback_data="users_page_" + str(page - 1)))
    if end < total:
        nav.append(InlineKeyboardButton(text="▶️", callback_data="users_page_" + str(page + 1)))
    rows = [nav] if nav else []
    rows.append([InlineKeyboardButton(text="Админ", callback_data="admin_panel")])
    fn = msg.edit_text if edit else msg.answer
    await fn("\n".join(lines), parse_mode="HTML",
             reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

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
        "<b>✅ Коллекции обновлены: " + str(len(ALL_GIFT_IDS)) + " шт.</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )

@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await cb.message.answer(
        "<b>Отправь сообщение для рассылки. /cancel — отмена</b>",
        parse_mode="HTML", reply_markup=cancel_kb()
    )
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
    status = await cb.message.answer(
        "<b>Рассылка " + str(len(uids)) + " пользователям...</b>",
        parse_mode="HTML"
    )
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
                await status.edit_text(
                    "<b>" + str(i + 1) + " из " + str(len(uids)) + "...</b>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await status.edit_text(
        "<b>✅ Отправлено: " + str(ok) + "\nОшибок: " + str(fail) + "</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id):
        return
    u = load_users()
    await cb.message.answer(
        "<b>📊 Статистика\n\n"
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
        await tg_client.sign_in(
            phone=data["phone"], code=code,
            phone_code_hash=data["phone_code_hash"]
        )
        me = await tg_client.get_me()
        await state.clear()
        await load_collections()
        await message.answer(
            "<b>✅ Авторизован как @" + esc(str(me.username or me.first_name)) + "\n"
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
            "<b>✅ Авторизован как @" + esc(str(me.username or me.first_name)) + "\n"
            "Коллекций: " + str(len(ALL_GIFT_IDS)) + "</b>",
            parse_mode="HTML", reply_markup=main_menu_kb()
        )
    except Exception as e:
        await message.answer(
            "<b>Неверный пароль: <code>" + esc(str(e)) + "</code></b>",
            parse_mode="HTML"
        )

# ── MAIN ──────────────────────────────────────────────────────────────────────
async def main():
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("Neptun Parser запущен!")

    from aiogram.types import BotCommand
    await bot.set_my_commands([
        BotCommand(command="start",  description="Главное меню"),
        BotCommand(command="clear",  description="Остановить поиск"),
        BotCommand(command="myid",   description="Мой ID"),
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
