import asyncio
import logging
import urllib.parse
from telethon import TelegramClient
from telethon.tl.functions.payments import GetResaleStarGiftsRequest, GetStarGiftsRequest
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
import json
import os

# ========================
API_ID = 28687552
API_HASH = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN = "8406363273:AAF36kxfkOJiLvYPs1FBBWmPUgNcd_kX140"
ADMIN_ID = 8726084830
SESSION_NAME = "nft_session"
USERS_FILE = "users.json"
# ========================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

stats = {"checks": 0, "found": 0}
is_searching = False
NFT_COLLECTIONS = {}  # {title: gift_id}
_fields_logged = False  # логируем поля один раз

PRICE_CATEGORIES = {
    "cheap": {"label": "💚 Дешёвые",  "min": None,  "max": 2000,  "desc": "до 2000 ⭐️"},
    "mid":   {"label": "💛 Средние",  "min": 2001,  "max": 5000,  "desc": "2000–5000 ⭐️"},
    "hard":  {"label": "🟠 Сложные",  "min": 5001,  "max": 20000, "desc": "5000–20000 ⭐️"},
    "ultra": {"label": "🔴 Хард",     "min": 20001, "max": None,  "desc": "от 20000 ⭐️"},
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


# ===================== USERS =====================
def load_users() -> set:
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_users(users: set):
    with open(USERS_FILE, "w") as f:
        json.dump(list(users), f)

def add_user_to_db(uid: int):
    users = load_users()
    users.add(uid)
    save_users(users)


# ===================== STATES =====================
class Auth(StatesGroup):
    phone = State()
    code = State()
    password = State()

class Broadcast(StatesGroup):
    message = State()


# ===================== AUTH =====================
async def check_authorized() -> bool:
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
        return await tg_client.is_user_authorized()
    except Exception:
        return False

def is_admin(uid: int) -> bool:
    return int(uid) == int(ADMIN_ID)


# ===================== PRICE =====================
def get_price(gift) -> int | None:
    """
    Полный перебор всех способов достать цену из объекта подарка.
    Логирует структуру один раз при старте.
    """
    global _fields_logged
    d = getattr(gift, '__dict__', {})

    if not _fields_logged:
        _fields_logged = True
        info = {}
        for k, v in d.items():
            if k.startswith('_'):
                continue
            try:
                info[k] = repr(v)[:120]
            except Exception:
                info[k] = "?"
        logger.info("=== GIFT FIELDS ===\n" + "\n".join(f"  {k}: {v}" for k, v in info.items()))

    # --- resell_amount: может быть списком StarsAmount или одним объектом ---
    ra = getattr(gift, 'resell_amount', None)
    if ra is not None:
        if isinstance(ra, (list, tuple)):
            for item in ra:
                a = getattr(item, 'amount', None)
                if a is not None:
                    try:
                        v = int(a)
                        if v > 0:
                            return v
                    except Exception:
                        pass
        else:
            a = getattr(ra, 'amount', None)
            if a is not None:
                try:
                    v = int(a)
                    if v > 0:
                        return v
                except Exception:
                    pass
            # может быть просто числом
            try:
                v = int(ra)
                if v > 0:
                    return v
            except Exception:
                pass

    # --- простые числовые поля ---
    for field in [
        'resell_stars', 'resale_stars', 'resale_amount_stars',
        'price', 'stars', 'cost', 'amount',
        'convert_stars', 'star_count', 'total_amount',
        'availability_resale_stars', 'nft_price',
    ]:
        val = getattr(gift, field, None)
        if val is None:
            continue
        try:
            v = int(val)
            if v > 0:
                return v
        except Exception:
            pass

    # --- fallback: любое int поле > 50, кроме служебных ---
    SKIP = {'id','num','hash','date','access_hash','dc_id','flags','flags2',
            'gift_id','sticker_id','pattern_id','backdrop_id','availability_total',
            'availability_remains','first_sale_date','last_sale_date'}
    for k, v in d.items():
        if k.startswith('_') or k in SKIP:
            continue
        if isinstance(v, int) and v > 50:
            logger.info(f"Fallback цена: {k}={v}")
            return v

    return None


# ===================== OWNER =====================
def get_owner(gift, users_map: dict):
    """Возвращает (owner_object, user_id) или (None, None)."""
    oid_obj = getattr(gift, 'owner_id', None)
    if oid_obj is None:
        return None, None

    uid = getattr(oid_obj, 'user_id', None)
    if uid is None:
        uid = getattr(oid_obj, 'id', None)
    if uid is None and isinstance(oid_obj, int):
        uid = oid_obj
    if uid is None:
        return None, None

    uid = int(uid)
    owner = users_map.get(uid)
    return owner, uid


def fmt_owner(owner, username: str | None, name: str) -> str:
    """Форматирует строку владельца."""
    if username and name:
        return f"{name} (@{username})"
    if username:
        return f"@{username}"
    if name:
        return f"👤 {name}"
    return "👤 Скрыт"


# ===================== GIRL CHECK =====================
def is_girl(owner) -> bool:
    if not owner:
        return False
    first = (getattr(owner, 'first_name', '') or '').lower().strip()
    last  = (getattr(owner, 'last_name',  '') or '').lower().strip()
    uname = (getattr(owner, 'username',   '') or '').lower().strip()
    for name in GIRL_NAMES:
        if first.startswith(name) or last.startswith(name) or uname.startswith(name):
            return True
    for kw in ['girl','lady','princess','queen','барби','принцесса','королева',
               'baby','cute','sweetie','babe','honey','cutie']:
        if kw in first or kw in last or kw in uname:
            return True
    return False


# ===================== KEYBOARDS =====================
def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Искать NFT",     callback_data="search_nft_menu")],
        [InlineKeyboardButton(text="👧 Искать девушек",  callback_data="search_girls_menu")],
        [InlineKeyboardButton(text="📊 Статистика",      callback_data="stats")],
    ])

def nft_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Все NFT",                  callback_data="nft_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000 ⭐️)",    callback_data="nft_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000–5000 ⭐️)",  callback_data="nft_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000–20000 ⭐️)", callback_data="nft_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20000+ ⭐️)",        callback_data="nft_ultra")],
        [InlineKeyboardButton(text="🗂 По коллекции",             callback_data="market_col")],
        [InlineKeyboardButton(text="◀️ Назад",                    callback_data="menu")],
    ])

def girl_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Все девушки",              callback_data="girl_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000 ⭐️)",    callback_data="girl_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000–5000 ⭐️)",  callback_data="girl_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000–20000 ⭐️)", callback_data="girl_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20000+ ⭐️)",        callback_data="girl_ultra")],
        [InlineKeyboardButton(text="🗂 По коллекции",             callback_data="girl_col")],
        [InlineKeyboardButton(text="◀️ Назад",                    callback_data="menu")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ СТОП", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Искать ещё", callback_data="search_nft_menu")],
        [InlineKeyboardButton(text="📱 Меню",        callback_data="menu")],
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

def nft_kb(username: str | None, slug: str | None, title: str, num) -> InlineKeyboardMarkup | None:
    """Кнопки под NFT: профиль, открыть, написать."""
    btns = []
    nft_url = f"https://t.me/nft/{slug}" if slug else None

    if username:
        btns.append([InlineKeyboardButton(text=f"👤 @{username}", url=f"https://t.me/{username}")])
    if nft_url:
        btns.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=nft_url)])
    if username:
        txt = f"Привет! Я хочу купить твой NFT {title} #{num}"
        if nft_url:
            txt += f" 👉 {nft_url}"
        btns.append([InlineKeyboardButton(text="✉️ Написать", url=f"https://t.me/{username}?text={urllib.parse.quote(txt)}")])

    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None

def col_kb(items: list, prefix: str, back: str) -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(items), 2):
        row = [InlineKeyboardButton(text=items[i], callback_data=f"{prefix}{i}")]
        if i + 1 < len(items):
            row.append(InlineKeyboardButton(text=items[i+1], callback_data=f"{prefix}{i+1}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ===================== COLLECTIONS =====================
async def load_collections():
    global NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        NFT_COLLECTIONS = {}
        for gift in result.gifts:
            title   = getattr(gift, 'title', None)
            gift_id = getattr(gift, 'id',    None)
            if title and gift_id:
                NFT_COLLECTIONS[title] = gift_id
        logger.info(f"✅ Коллекций: {len(NFT_COLLECTIONS)} → {list(NFT_COLLECTIONS.keys())}")
    except Exception as e:
        logger.error(f"❌ load_collections: {e}")


# ===================== FETCH ONE PAGE =====================
async def fetch_page(gift_id: int, offset: str, limit: int = 100) -> tuple[list, str]:
    try:
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gift_id,
            offset=offset,
            limit=limit,
        ))
        users_map = {int(u.id): u for u in (getattr(result, 'users', None) or [])}
        gifts     = getattr(result, 'gifts', None) or []
        items     = []

        for gift in gifts:
            owner, owner_uid = get_owner(gift, users_map)
            username = getattr(owner, 'username', None) if owner else None
            name = ""
            if owner:
                fn = getattr(owner, 'first_name', '') or ''
                ln = getattr(owner, 'last_name',  '') or ''
                name = f"{fn} {ln}".strip()

            title = getattr(gift, 'title', '?')
            slug  = (getattr(gift, 'slug', None)
                     or getattr(gift, 'unique_id', None)
                     or str(getattr(gift, 'num', '')))
            num   = getattr(gift, 'num', '?')
            price = get_price(gift)

            items.append({
                "owner":    owner,
                "owner_id": owner_uid,
                "username": username,
                "name":     name,
                "title":    title,
                "slug":     slug,
                "num":      num,
                "price":    price,
            })

        next_offset = getattr(result, 'next_offset', "") or ""
        return items, next_offset

    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds}s")
        await asyncio.sleep(e.seconds + 2)
        return [], ""
    except Exception as e:
        logger.error(f"fetch_page gid={gift_id}: {e}")
        return [], ""


# ===================== SEARCH ENGINE =====================
async def do_search(
    status_msg: Message,
    gift_ids: list,
    max_results: int = 100,
    girls_only: bool = False,
    price_min: int | None = None,
    price_max: int | None = None,
) -> int:
    global is_searching
    is_searching = True
    found = 0
    seen_slugs    = set()
    seen_girl_ids = set()

    logger.info(f"do_search: {len(gift_ids)} коллекций, цена={price_min}-{price_max}, girls={girls_only}")

    try:
        for gid in gift_ids:
            if not is_searching or found >= max_results:
                break

            offset      = ""
            empty_pages = 0

            while is_searching and found < max_results:
                items, next_offset = await fetch_page(gid, offset)

                if not items:
                    empty_pages += 1
                    if empty_pages >= 2:
                        break
                    await asyncio.sleep(0.5)
                    continue

                empty_pages = 0

                for item in items:
                    if not is_searching or found >= max_results:
                        break

                    slug  = item["slug"]
                    price = item["price"]

                    # дедупликация
                    if slug and slug in seen_slugs:
                        continue
                    if slug:
                        seen_slugs.add(slug)

                    # фильтр цены — только если задан хотя бы один порог
                    if price_min is not None or price_max is not None:
                        if price is None:
                            continue
                        if price_min is not None and price < price_min:
                            continue
                        if price_max is not None and price > price_max:
                            continue

                    # фильтр девушек
                    if girls_only:
                        if not is_girl(item["owner"]):
                            continue
                        oid = item["owner_id"]
                        if oid:
                            if oid in seen_girl_ids:
                                continue
                            seen_girl_ids.add(oid)

                    found += 1
                    stats["found"] += 1

                    price_str = f"⭐️ {price:,}".replace(",", " ") if price else "цена неизвестна"
                    owner_str = fmt_owner(item["owner"], item["username"], item["name"])
                    prefix    = "👧 " if girls_only else ""
                    kb        = nft_kb(item["username"], slug, item["title"], item["num"])

                    try:
                        await status_msg.bot.send_message(
                            chat_id=status_msg.chat.id,
                            text=(
                                f"{prefix}🎁 <b>{item['title']} #{item['num']}</b>\n"
                                f"👤 {owner_str}\n"
                                f"💰 {price_str}"
                            ),
                            parse_mode="HTML",
                            reply_markup=kb,
                        )
                    except Exception as e:
                        logger.warning(f"send: {e}")

                    await asyncio.sleep(0.05)

                # обновляем счётчик в статус-сообщении
                try:
                    lbl = "👧 Девушек" if girls_only else "NFT"
                    await status_msg.edit_text(
                        f"🔍 Ищу...\nНайдено {lbl}: <b>{found}</b>",
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                except Exception:
                    pass

                if not next_offset:
                    break
                offset = next_offset
                await asyncio.sleep(0.3)

    except Exception as e:
        logger.error(f"do_search error: {e}")
    finally:
        is_searching = False

    logger.info(f"do_search done: found={found}")
    return found


# ===================== SEARCH RUNNERS =====================
async def start_nft_search(cb: CallbackQuery, cat: str | None = None, ids: list | None = None):
    global is_searching
    if is_searching:
        await cb.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    pmin, pmax, label = None, None, "🎁 Все NFT"
    if cat and cat in PRICE_CATEGORIES:
        c     = PRICE_CATEGORIES[cat]
        pmin  = c["min"]
        pmax  = c["max"]
        label = f"🎁 {c['label']} ({c['desc']})"

    await cb.answer("🔍 Запускаю...")
    stats["checks"] += 1

    if not ids:
        if not NFT_COLLECTIONS:
            await load_collections()
        ids = list(NFT_COLLECTIONS.values())

    if not ids:
        await cb.message.answer("❌ Коллекции не загружены.", reply_markup=menu_kb())
        return

    status = await cb.message.answer(
        f"<b>{label}</b>\n\nНайдено: 0", parse_mode="HTML", reply_markup=stop_kb()
    )
    found = await do_search(status, ids, price_min=pmin, price_max=pmax)
    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n{label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


async def start_girl_search(cb: CallbackQuery, cat: str | None = None, ids: list | None = None):
    global is_searching
    if is_searching:
        await cb.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    pmin, pmax, label = None, None, "👧 Девушки — все"
    if cat and cat in PRICE_CATEGORIES:
        c     = PRICE_CATEGORIES[cat]
        pmin  = c["min"]
        pmax  = c["max"]
        label = f"👧 Девушки — {c['label']} ({c['desc']})"

    await cb.answer("👧 Ищу девушек...")
    stats["checks"] += 1

    if not ids:
        if not NFT_COLLECTIONS:
            await load_collections()
        ids = list(NFT_COLLECTIONS.values())

    if not ids:
        await cb.message.answer("❌ Коллекции не загружены.", reply_markup=menu_kb())
        return

    status = await cb.message.answer(
        f"<b>{label}</b>\n\nНайдено: 0", parse_mode="HTML", reply_markup=stop_kb()
    )
    found = await do_search(status, ids, girls_only=True, price_min=pmin, price_max=pmax)
    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n{label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


# ===================== COMMAND HANDLERS =====================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    add_user_to_db(message.from_user.id)
    if not await check_authorized():
        if is_admin(message.from_user.id):
            await message.answer(
                "⚙️ <b>Нужна авторизация Telegram</b>\n\n"
                "Введи номер телефона аккаунта для парсинга:\n<code>+79001234567</code>",
                parse_mode="HTML"
            )
            await state.set_state(Auth.phone)
        else:
            await message.answer("⏳ <b>Бот настраивается</b>\n\nПопробуй позже.", parse_mode="HTML")
        return
    await message.answer("🎁 <b>NFT Market Parser</b>\n\n👇 Выбери действие:", parse_mode="HTML", reply_markup=main_kb())


@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(f"❌ Нет доступа.\n\nТвой ID: <code>{message.from_user.id}</code>", parse_mode="HTML")
        return
    await state.clear()
    users      = load_users()
    authorized = await check_authorized()
    await message.answer(
        f"👑 <b>Админ панель</b>\n\n"
        f"🔐 Telethon: <b>{'✅ Авторизован' if authorized else '❌ Не авторизован'}</b>\n"
        f"📦 Коллекций: <b>{len(NFT_COLLECTIONS)}</b>\n"
        f"👥 Пользователей: <b>{len(users)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено NFT: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"🆔 Твой ID: <code>{message.from_user.id}</code>", parse_mode="HTML")

@dp.message(Command("cols"))
async def cmd_cols(message: Message):
    if not is_admin(message.from_user.id): return
    if not NFT_COLLECTIONS:
        await load_collections()
    lines = [f"📦 Коллекций: {len(NFT_COLLECTIONS)}\n"]
    for name, gid in NFT_COLLECTIONS.items():
        lines.append(f"• {name} (id={gid})")
    await message.answer("\n".join(lines), parse_mode="HTML")

@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Отменено.", reply_markup=main_kb())


# ===================== CALLBACK HANDLERS =====================
@dp.callback_query(F.data == "menu")
async def cb_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("🎁 <b>NFT Market Parser</b>\n\n👇 Выбери действие:", parse_mode="HTML", reply_markup=main_kb())
    await cb.answer()

@dp.callback_query(F.data == "search_nft_menu")
async def cb_nft_menu(cb: CallbackQuery):
    await cb.message.answer("🎁 <b>Искать NFT</b>\n\nВыбери категорию:", parse_mode="HTML", reply_markup=nft_menu_kb())
    await cb.answer()

@dp.callback_query(F.data == "search_girls_menu")
async def cb_girl_menu(cb: CallbackQuery):
    await cb.message.answer("👧 <b>Искать девушек</b>\n\nВыбери категорию:", parse_mode="HTML", reply_markup=girl_menu_kb())
    await cb.answer()

# NFT поиск
@dp.callback_query(F.data == "nft_all")
async def cb_nft_all(cb: CallbackQuery): await start_nft_search(cb)

@dp.callback_query(F.data == "nft_cheap")
async def cb_nft_cheap(cb: CallbackQuery): await start_nft_search(cb, "cheap")

@dp.callback_query(F.data == "nft_mid")
async def cb_nft_mid(cb: CallbackQuery): await start_nft_search(cb, "mid")

@dp.callback_query(F.data == "nft_hard")
async def cb_nft_hard(cb: CallbackQuery): await start_nft_search(cb, "hard")

@dp.callback_query(F.data == "nft_ultra")
async def cb_nft_ultra(cb: CallbackQuery): await start_nft_search(cb, "ultra")

# Девушки поиск
@dp.callback_query(F.data == "girl_all")
async def cb_girl_all(cb: CallbackQuery): await start_girl_search(cb)

@dp.callback_query(F.data == "girl_cheap")
async def cb_girl_cheap(cb: CallbackQuery): await start_girl_search(cb, "cheap")

@dp.callback_query(F.data == "girl_mid")
async def cb_girl_mid(cb: CallbackQuery): await start_girl_search(cb, "mid")

@dp.callback_query(F.data == "girl_hard")
async def cb_girl_hard(cb: CallbackQuery): await start_girl_search(cb, "hard")

@dp.callback_query(F.data == "girl_ultra")
async def cb_girl_ultra(cb: CallbackQuery): await start_girl_search(cb, "ultra")

# Коллекции для NFT
@dp.callback_query(F.data == "market_col")
async def cb_market_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("❌ Коллекции не загружены", reply_markup=menu_kb())
        await cb.answer()
        return
    await cb.message.answer("🗂 <b>Выбери коллекцию:</b>", parse_mode="HTML",
                             reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "mcol_", "search_nft_menu"))
    await cb.answer()

@dp.callback_query(F.data.startswith("mcol_"))
async def cb_mcol(cb: CallbackQuery):
    idx   = int(cb.data[5:])
    items = list(NFT_COLLECTIONS.items())
    if idx >= len(items):
        await cb.answer("❌ Не найдено", show_alert=True)
        return
    _, gid = items[idx]
    await start_nft_search(cb, ids=[gid])

# Коллекции для девушек
@dp.callback_query(F.data == "girl_col")
async def cb_girl_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("❌ Коллекции не загружены", reply_markup=menu_kb())
        await cb.answer()
        return
    await cb.message.answer("🗂 <b>Выбери коллекцию:</b>", parse_mode="HTML",
                             reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "gcol_", "search_girls_menu"))
    await cb.answer()

@dp.callback_query(F.data.startswith("gcol_"))
async def cb_gcol(cb: CallbackQuery):
    idx   = int(cb.data[5:])
    items = list(NFT_COLLECTIONS.items())
    if idx >= len(items):
        await cb.answer("❌ Не найдено", show_alert=True)
        return
    _, gid = items[idx]
    await start_girl_search(cb, ids=[gid])

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
        f"📊 <b>Статистика</b>\n\n🔍 Поисков: <b>{stats['checks']}</b>\n🎁 Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML"
    )
    await cb.answer()


# ===================== ADMIN CALLBACKS =====================
@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id): return
    await state.set_state(Broadcast.message)
    await cb.message.answer("📢 <b>Рассылка</b>\n\nОтправь сообщение.\n/cancel — отмена",
                             parse_mode="HTML", reply_markup=cancel_kb())
    await cb.answer()

@dp.message(Broadcast.message)
async def broadcast_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.update_data(mid=message.message_id, cid=message.chat.id)
    await state.set_state(None)
    await message.answer("Подтверди:", reply_markup=confirm_kb())

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def cb_broadcast_send(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id): return
    data = await state.get_data()
    mid, cid = data.get("mid"), data.get("cid")
    if not mid:
        await cb.answer("❌ Нет сообщения", show_alert=True)
        return
    users  = load_users()
    status = await cb.message.answer(f"📢 Отправляю {len(users)} пользователям...", parse_mode="HTML")
    await cb.answer()
    ok = fail = 0
    for i, uid in enumerate(users):
        try:
            await bot.copy_message(uid, cid, mid)
            ok += 1
        except Exception:
            fail += 1
        if (i+1) % 20 == 0:
            try: await status.edit_text(f"📢 {i+1}/{len(users)}...")
            except Exception: pass
        await asyncio.sleep(0.05)
    await status.edit_text(
        f"✅ Отправлено: <b>{ok}</b>\n❌ Ошибок: <b>{fail}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    await cb.message.answer(f"👥 Пользователей: <b>{len(load_users())}</b>", parse_mode="HTML", reply_markup=admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    u = load_users()
    await cb.message.answer(
        f"📊 <b>Статистика</b>\n\n👥 Пользователей: <b>{len(u)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n🎁 Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await cb.answer()

@dp.callback_query(F.data == "admin_auth")
async def cb_admin_auth(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id): return
    await state.clear()
    await cb.message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)
    await cb.answer()

@dp.callback_query(F.data == "admin_logout")
async def cb_admin_logout(cb: CallbackQuery):
    if not is_admin(cb.from_user.id): return
    try: await tg_client.log_out()
    except Exception: pass
    await cb.message.answer("✅ Вышел из TG.", reply_markup=admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(cb: CallbackQuery, state: FSMContext):
    if not is_admin(cb.from_user.id): return
    await state.clear()
    await cb.message.answer("❌ Отменено", reply_markup=admin_kb())
    await cb.answer()


# ===================== AUTH HANDLERS =====================
@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
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
    if not is_admin(message.from_user.id): return
    code = message.text.strip().replace(" ", "")
    data = await state.get_data()
    try:
        await tg_client.sign_in(phone=data["phone"], code=code, phone_code_hash=data["phone_code_hash"])
        me = await tg_client.get_me()
        await state.clear()
        await load_collections()
        await message.answer(
            f"✅ <b>Авторизован как @{me.username or me.first_name}!</b>\n"
            f"Коллекций загружено: <b>{len(NFT_COLLECTIONS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("🔐 Введи пароль 2FA:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")

@dp.message(Auth.password)
async def auth_password(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try:
        await tg_client.sign_in(password=message.text.strip())
        me = await tg_client.get_me()
        await state.clear()
        await load_collections()
        await message.answer(
            f"✅ <b>Авторизован как @{me.username or me.first_name}!</b>\n"
            f"Коллекций загружено: <b>{len(NFT_COLLECTIONS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: <code>{e}</code>", parse_mode="HTML")

@dp.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()
    await message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)


# ===================== MAIN =====================
async def main():
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("🎁 NFT Bot запущен!")
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info(f"✅ Авторизован, коллекций: {len(NFT_COLLECTIONS)}")
        else:
            logger.warning("⚠️ Не авторизован — пройди авторизацию через /start")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
