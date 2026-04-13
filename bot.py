import asyncio
import logging
import urllib.parse
import os
import json
from telethon import TelegramClient
from telethon.tl.functions.payments import GetResaleStarGiftsRequest, GetStarGiftsRequest
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# ======================== ENV ========================
# ======================== ENV ========================
API_ID       = 28687552
API_HASH     = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN    = "8406363273:AAF2L-LfRhUVMrbVLeZBLLI7IgkFoMtyfGM"
ADMIN_ID     = 8726084830
SESSION_NAME = "nft_session"
USERS_FILE   = "users.json"
# =====================================================
# =====================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot      = Bot(token=BOT_TOKEN)
dp       = Dispatcher(storage=MemoryStorage())
tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

stats = {"checks": 0, "found": 0}
is_searching = False
ALL_GIFT_IDS    = []   # [(gift_id, title), ...]
NFT_COLLECTIONS = {}   # {title: gift_id}
MARKET_PRICE_CACHE: dict[int, int] = {}

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
GIRL_KW = [
    'girl','lady','princess','queen','baby','cute','sweetie','babe','honey','cutie',
    'барби','принцесса','королева','девочка','красотка',
]


# ===================== USERS =====================
def load_users() -> set:
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, "r") as f:
            return set(json.load(f))
    return set()

def save_users(u: set):
    with open(USERS_FILE, "w") as f:
        json.dump(list(u), f)

def add_user(uid: int):
    u = load_users(); u.add(uid); save_users(u)


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
        return f"👤 {name}"
    return "👤 Скрыт"


# ===================== MARKET PRICE =====================
async def get_median_price(gift_id: int) -> int | None:
    if gift_id in MARKET_PRICE_CACHE:
        return MARKET_PRICE_CACHE[gift_id]
    try:
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gift_id, offset="", limit=50,
        ))
        gifts  = getattr(result, 'gifts', None) or []
        prices = sorted([p for g in gifts if (p := get_price(g)) and p > 0])
        if not prices:
            return None
        median = prices[len(prices) // 2]
        MARKET_PRICE_CACHE[gift_id] = median
        logger.info(f"Медиана gid={gift_id}: {median} ⭐️ ({len(prices)} цен)")
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
        logger.info(f"✅ gift_id: {len(ALL_GIFT_IDS)}, с title: {len(NFT_COLLECTIONS)}")
        logger.info(f"Коллекции: {[t for _, t in ALL_GIFT_IDS]}")
    except Exception as e:
        logger.error(f"❌ load_collections: {e}")


# ===================== FETCH =====================
async def fetch_page(gift_id: int, offset: str, limit: int = 100) -> tuple[list, str]:
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
            nft_url = f"https://t.me/nft/{slug}" if slug and slug not in ('None', '', 'nan') else None

            items.append({
                "owner": owner, "owner_id": owner_uid,
                "username": username, "name": name,
                "title": title, "slug": slug, "num": num,
                "price": price, "nft_url": nft_url,
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

def col_kb(names: list, prefix: str, back: str) -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(names), 2):
        row = [InlineKeyboardButton(text=names[i], callback_data=f"{prefix}{i}")]
        if i + 1 < len(names):
            row.append(InlineKeyboardButton(text=names[i+1], callback_data=f"{prefix}{i+1}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data=back)])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def nft_kb(username: str | None, nft_url: str | None) -> InlineKeyboardMarkup | None:
    btns = []
    if username:
        btns.append([InlineKeyboardButton(text=f"👤 @{username}", url=f"https://t.me/{username}")])
    if nft_url:
        btns.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=nft_url)])
    if username and nft_url:
        write_text = f"Привет! Хочу купить твой NFT 👉 {nft_url}"
        btns.append([InlineKeyboardButton(
            text="✉️ Написать",
            url=f"https://t.me/{username}?text={urllib.parse.quote(write_text)}"
        )])
    return InlineKeyboardMarkup(inline_keyboard=btns) if btns else None


# ===================== SEARCH =====================
async def do_search(
    status_msg: Message,
    gift_ids: list[int],
    max_results: int = 100,
    girls_only: bool = False,
    price_min: int | None = None,
    price_max: int | None = None,
    use_market_price: bool = False,
) -> int:
    global is_searching
    is_searching  = True
    found         = 0
    seen_slugs    = set()
    seen_girl_ids = set()

    # Фильтруем коллекции по рыночной медиане
    if use_market_price and (price_min is not None or price_max is not None):
        filtered = []
        for gid in gift_ids:
            if not is_searching:
                break
            median = await get_median_price(gid)
            if median is None:
                continue
            if price_min is not None and median < price_min:
                continue
            if price_max is not None and median > price_max:
                continue
            filtered.append(gid)
            await asyncio.sleep(0.2)
        gift_ids = filtered
        logger.info(f"После фильтра по рынку: {len(gift_ids)} коллекций")

    if not gift_ids:
        is_searching = False
        return 0

    offsets = {gid: "" for gid in gift_ids}

    try:
        while is_searching and found < max_results:
            active = [gid for gid, off in offsets.items() if off is not None]
            if not active:
                break

            got_anything = False

            for gid in active:
                if not is_searching or found >= max_results:
                    break

                items, next_offset = await fetch_page(gid, offsets[gid], limit=20)
                offsets[gid] = next_offset if next_offset else None

                if not items:
                    continue

                for item in items:
                    if not is_searching or found >= max_results:
                        break

                    slug  = item["slug"] or ""
                    price = item["price"]

                    if slug and slug in seen_slugs:
                        continue
                    if slug:
                        seen_slugs.add(slug)

                    if not use_market_price:
                        if price_min is not None or price_max is not None:
                            if price is None:
                                continue
                            if price_min is not None and price < price_min:
                                continue
                            if price_max is not None and price > price_max:
                                continue

                    if girls_only:
                        if not is_girl(item["owner"]):
                            continue
                        oid = item["owner_id"]
                        if oid:
                            if oid in seen_girl_ids:
                                continue
                            seen_girl_ids.add(oid)

                    found         += 1
                    stats["found"] += 1
                    got_anything   = True

                    price_str = f"⭐️ {price:,}".replace(",", " ") if price else "цена неизвестна"
                    owner_str = fmt_owner(item["owner"], item["username"], item["name"])
                    prefix    = "👧 " if girls_only else ""
                    kb        = nft_kb(item["username"], item["nft_url"])

                    extra = ""
                    if use_market_price and price:
                        median = MARKET_PRICE_CACHE.get(gid)
                        if median:
                            extra = f"\n📊 Рынок: ⭐️ {median:,}".replace(",", " ")

                    try:
                        await status_msg.bot.send_message(
                            chat_id=status_msg.chat.id,
                            text=(
                                f"{prefix}🎁 <b>{item['title']} #{item['num']}</b>\n"
                                f"👤 {owner_str}\n"
                                f"💰 {price_str}{extra}"
                            ),
                            parse_mode="HTML",
                            reply_markup=kb,
                        )
                    except Exception as e:
                        logger.warning(f"send: {e}")

                    await asyncio.sleep(0.05)

                await asyncio.sleep(0.15)

            try:
                active_count = sum(1 for v in offsets.values() if v is not None)
                lbl = "👧 Девушек" if girls_only else "NFT"
                await status_msg.edit_text(
                    f"🔍 Ищу... (коллекций: {active_count})\n"
                    f"Найдено {lbl}: <b>{found}</b>",
                    parse_mode="HTML", reply_markup=stop_kb(),
                )
            except Exception:
                pass

            if not got_anything:
                break

    except Exception as e:
        logger.error(f"do_search error: {e}")
    finally:
        is_searching = False

    return found


# ===================== RUNNERS =====================
async def start_nft_search(cb: CallbackQuery, cat: str | None = None, ids: list | None = None):
    global is_searching
    if is_searching:
        await cb.answer("⏳ Поиск уже идёт!", show_alert=True); return

    pmin, pmax, label = None, None, "🎁 Все NFT"
    if cat and cat in PRICE_CATEGORIES:
        c = PRICE_CATEGORIES[cat]
        pmin, pmax = c["min"], c["max"]
        label = f"🎁 {c['label']} ({c['desc']})"

    await cb.answer("🔍 Запускаю...")
    stats["checks"] += 1

    if ids is None:
        if not ALL_GIFT_IDS: await load_collections()
        ids = [gid for gid, _ in ALL_GIFT_IDS]

    if not ids:
        await cb.message.answer("❌ Коллекции не загружены.", reply_markup=menu_kb()); return

    use_market = (pmin is not None or pmax is not None)
    status_text = (
        f"<b>{label}</b>\n\n⏳ Анализирую рыночные цены..." if use_market
        else f"<b>{label}</b>\n\nНайдено: 0"
    )
    status = await cb.message.answer(status_text, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_search(status, ids, price_min=pmin, price_max=pmax, use_market_price=use_market)
    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n{label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception: pass


async def start_girl_search(cb: CallbackQuery, cat: str | None = None, ids: list | None = None):
    global is_searching
    if is_searching:
        await cb.answer("⏳ Поиск уже идёт!", show_alert=True); return

    pmin, pmax, label = None, None, "👧 Девушки — все"
    if cat and cat in PRICE_CATEGORIES:
        c = PRICE_CATEGORIES[cat]
        pmin, pmax = c["min"], c["max"]
        label = f"👧 Девушки — {c['label']} ({c['desc']})"

    await cb.answer("👧 Ищу девушек...")
    stats["checks"] += 1

    if ids is None:
        if not ALL_GIFT_IDS: await load_collections()
        ids = [gid for gid, _ in ALL_GIFT_IDS]

    if not ids:
        await cb.message.answer("❌ Коллекции не загружены.", reply_markup=menu_kb()); return

    use_market = (pmin is not None or pmax is not None)
    status_text = (
        f"<b>{label}</b>\n\n⏳ Анализирую рыночные цены..." if use_market
        else f"<b>{label}</b>\n\nНайдено: 0"
    )
    status = await cb.message.answer(status_text, parse_mode="HTML", reply_markup=stop_kb())
    found = await do_search(status, ids, girls_only=True, price_min=pmin, price_max=pmax, use_market_price=use_market)
    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n{label}\nНайдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception: pass


# ===================== COMMANDS =====================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    add_user(message.from_user.id)
    if not await check_authorized():
        if is_admin(message.from_user.id):
            await message.answer(
                "⚙️ <b>Нужна авторизация Telegram</b>\n\nВведи номер телефона:\n<code>+79001234567</code>",
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
    if not is_admin(message.from_user.id): return
    if not ALL_GIFT_IDS: await load_collections()
    lines = [f"📦 <b>Коллекций: {len(ALL_GIFT_IDS)}</b>"]
    for gid, label in ALL_GIFT_IDS[:50]:
        mp = MARKET_PRICE_CACHE.get(gid, "—")
        lines.append(f"• {label} (id={gid}) | рынок: {mp}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@dp.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()
    await message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("❌ Отменено.", reply_markup=main_kb())


# ===================== CALLBACKS =====================
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

@dp.callback_query(F.data == "nft_all")
async def cb_nft_all(cb: CallbackQuery):   await start_nft_search(cb)
@dp.callback_query(F.data == "nft_cheap")
async def cb_nft_cheap(cb: CallbackQuery): await start_nft_search(cb, "cheap")
@dp.callback_query(F.data == "nft_mid")
async def cb_nft_mid(cb: CallbackQuery):   await start_nft_search(cb, "mid")
@dp.callback_query(F.data == "nft_hard")
async def cb_nft_hard(cb: CallbackQuery):  await start_nft_search(cb, "hard")
@dp.callback_query(F.data == "nft_ultra")
async def cb_nft_ultra(cb: CallbackQuery): await start_nft_search(cb, "ultra")

@dp.callback_query(F.data == "girl_all")
async def cb_girl_all(cb: CallbackQuery):   await start_girl_search(cb)
@dp.callback_query(F.data == "girl_cheap")
async def cb_girl_cheap(cb: CallbackQuery): await start_girl_search(cb, "cheap")
@dp.callback_query(F.data == "girl_mid")
async def cb_girl_mid(cb: CallbackQuery):   await start_girl_search(cb, "mid")
@dp.callback_query(F.data == "girl_hard")
async def cb_girl_hard(cb: CallbackQuery):  await start_girl_search(cb, "hard")
@dp.callback_query(F.data == "girl_ultra")
async def cb_girl_ultra(cb: CallbackQuery): await start_girl_search(cb, "ultra")

@dp.callback_query(F.data == "market_col")
async def cb_market_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS: await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("❌ Коллекции не загружены", reply_markup=menu_kb())
        await cb.answer(); return
    await cb.message.answer("🗂 <b>Выбери коллекцию:</b>", parse_mode="HTML",
                             reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "mcol_", "search_nft_menu"))
    await cb.answer()

@dp.callback_query(F.data.startswith("mcol_"))
async def cb_mcol(cb: CallbackQuery):
    idx = int(cb.data[5:])
    lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst): await cb.answer("❌ Не найдено", show_alert=True); return
    await start_nft_search(cb, ids=[lst[idx][1]])

@dp.callback_query(F.data == "girl_col")
async def cb_girl_col(cb: CallbackQuery):
    if not NFT_COLLECTIONS: await load_collections()
    if not NFT_COLLECTIONS:
        await cb.message.answer("❌ Коллекции не загружены", reply_markup=menu_kb())
        await cb.answer(); return
    await cb.message.answer("🗂 <b>Выбери коллекцию:</b>", parse_mode="HTML",
                             reply_markup=col_kb(list(NFT_COLLECTIONS.keys()), "gcol_", "search_girls_menu"))
    await cb.answer()

@dp.callback_query(F.data.startswith("gcol_"))
async def cb_gcol(cb: CallbackQuery):
    idx = int(cb.data[5:])
    lst = list(NFT_COLLECTIONS.items())
    if idx >= len(lst): await cb.answer("❌ Не найдено", show_alert=True); return
    await start_girl_search(cb, ids=[lst[idx][1]])

@dp.callback_query(F.data == "stop_search")
async def cb_stop(cb: CallbackQuery):
    global is_searching
    is_searching = False
    await cb.answer("⏹ Останавливаю...")
    try:
        await cb.message.edit_text("⏹ <b>Поиск остановлен</b>", parse_mode="HTML", reply_markup=menu_kb())
    except Exception: pass

@dp.callback_query(F.data == "stats")
async def cb_stats(cb: CallbackQuery):
    await cb.message.answer(
        f"📊 <b>Статистика</b>\n\n🔍 Поисков: <b>{stats['checks']}</b>\n🎁 Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML"
    )
    await cb.answer()

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
    if not mid: await cb.answer("❌ Нет сообщения", show_alert=True); return
    users  = load_users()
    status = await cb.message.answer(f"📢 Отправляю {len(users)} пользователям...")
    await cb.answer()
    ok = fail = 0
    for i, uid in enumerate(users):
        try: await bot.copy_message(uid, cid, mid); ok += 1
        except Exception: fail += 1
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


# ===================== AUTH =====================
@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Формат: <code>+79001234567</code>", parse_mode="HTML"); return
    try:
        if not tg_client.is_connected():
            await tg_client.connect(); await asyncio.sleep(1)
        res = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=res.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("📨 Код отправлен. Введи без пробелов: <code>12345</code>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML"); await state.clear()

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
    if not is_admin(message.from_user.id): return
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
    logger.info("🎁 NFT Bot запущен!")
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info(f"✅ Авторизован, коллекций: {len(ALL_GIFT_IDS)}")
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
