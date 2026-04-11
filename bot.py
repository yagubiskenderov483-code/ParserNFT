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
NFT_COLLECTIONS = {}

PRICE_CATEGORIES = {
    "cheap": {"label": "💚 Дешёвые",  "min": 0,     "max": 2000,  "desc": "до 2000 ⭐️"},
    "mid":   {"label": "💛 Средние",  "min": 2001,  "max": 5000,  "desc": "2000–5000 ⭐️"},
    "hard":  {"label": "🟠 Сложные",  "min": 5001,  "max": 20000, "desc": "5000–20000 ⭐️"},
    "ultra": {"label": "🔴 Хард",     "min": 20001, "max": None,  "desc": "от 20000 ⭐️"},
}

GIRL_NAMES = {
    "анна", "мария", "екатерина", "анастасия", "наталья", "ольга", "елена",
    "татьяна", "ирина", "юлия", "алина", "виктория", "дарья", "полина",
    "ксения", "валерия", "александра", "надежда", "людмила", "галина",
    "christina", "anna", "maria", "kate", "natasha", "olga", "elena",
    "tatiana", "irina", "julia", "alina", "victoria", "dasha", "polina",
    "ksenia", "valeria", "alexandra", "diana", "sophia", "sofia", "lisa",
    "лиза", "диана", "софья", "софия", "кристина", "светлана", "sveta",
    "милана", "milana", "арина", "arina", "вера", "vera", "жанна", "zhanna",
    "angela", "ангелина", "angelina", "карина", "karina",
    "оксана", "oksana", "нина", "nina", "лариса", "larisa", "регина", "regina"
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


# ===================== AUTH CHECK =====================
async def check_authorized() -> bool:
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
        return await tg_client.is_user_authorized()
    except Exception:
        return False


# ===================== HELPERS =====================
def is_admin(uid: int) -> bool:
    return int(uid) == int(ADMIN_ID)

def is_girl(user) -> bool:
    first = (getattr(user, 'first_name', '') or '').lower().strip()
    last  = (getattr(user, 'last_name',  '') or '').lower().strip()
    uname = (getattr(user, 'username',   '') or '').lower().strip()
    for name in GIRL_NAMES:
        if first.startswith(name) or last.startswith(name) or uname.startswith(name):
            return True
    for kw in ['girl','lady','princess','queen','барби','принцесса','королева','baby','cute','sweetie']:
        if kw in first or kw in last or kw in uname:
            return True
    return False


def extract_price(gift) -> int | None:
    """
    Извлекает цену перепродажи из NFT объекта.
    Ищет только конкретные поля цены, НЕ кэширует поле глобально.
    Ограничение: цена не может быть > 10 млн звёзд (исключает id/num/hash).
    """
    PRICE_FIELDS = [
        'resell_stars',
        'resale_amount',
        'availability_resale_stars',
        'stars',
        'price',
        'cost',
        'amount',
        'convert_stars',
        'star_count',
    ]
    MAX_SANE_PRICE = 10_000_000  # 10 млн звёзд — потолок реальной цены

    for field in PRICE_FIELDS:
        val = getattr(gift, field, None)
        if val is not None:
            try:
                iv = int(val)
                if 0 < iv <= MAX_SANE_PRICE:
                    return iv
            except Exception:
                pass
    return None


# ===================== KEYBOARDS =====================
def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Искать NFT",     callback_data="search_nft_menu")],
        [InlineKeyboardButton(text="👧 Искать девушек",  callback_data="search_girls_menu")],
        [InlineKeyboardButton(text="📊 Статистика",      callback_data="stats")],
    ])

def nft_difficulty_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Все NFT",                  callback_data="nft_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000 ⭐️)",    callback_data="nft_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000–5000 ⭐️)",  callback_data="nft_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000–20000 ⭐️)", callback_data="nft_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20000+ ⭐️)",        callback_data="nft_ultra")],
        [InlineKeyboardButton(text="🗂 По коллекции",             callback_data="market_col")],
        [InlineKeyboardButton(text="◀️ Назад",                    callback_data="menu")],
    ])

def girls_difficulty_kb():
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

def confirm_broadcast_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="admin_broadcast_confirm")],
        [InlineKeyboardButton(text="❌ Отмена",    callback_data="admin_cancel")],
    ])

def girls_col_kb():
    if not NFT_COLLECTIONS:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="search_girls_menu")]
        ])
    items = list(NFT_COLLECTIONS.keys())
    rows = []
    for i in range(0, len(items), 2):
        row = [InlineKeyboardButton(text=items[i], callback_data=f"gcol_{i}")]
        if i + 1 < len(items):
            row.append(InlineKeyboardButton(text=items[i+1], callback_data=f"gcol_{i+1}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="search_girls_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def user_nft_kb(username: str, slug: str, title: str, num):
    """
    Кнопки для NFT.
    Кнопка 'Написать' — только ссылка на NFT, без названия и #номера.
    """
    buttons = []
    nft_url = f"https://t.me/nft/{slug}" if slug else None

    if username:
        buttons.append([InlineKeyboardButton(text=f"👤 @{username}", url=f"https://t.me/{username}")])
    if nft_url:
        buttons.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=nft_url)])
    if username:
        if nft_url:
            msg_text = f"Привет! Хочу купить твой NFT 👉 {nft_url}"
        else:
            msg_text = "Привет! Хочу купить твой NFT"
        encoded = urllib.parse.quote(msg_text)
        buttons.append([InlineKeyboardButton(text="✉️ Написать", url=f"https://t.me/{username}?text={encoded}")])

    return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None


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
        logger.info(f"✅ Коллекций загружено: {len(NFT_COLLECTIONS)}: {list(NFT_COLLECTIONS.keys())}")
    except Exception as e:
        logger.error(f"❌ load_collections: {e}")


# ===================== FETCH =====================
async def fetch_market_gifts(gift_id: int, offset: str = "", limit: int = 100) -> tuple:
    try:
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gift_id, offset=offset, limit=limit,
        ))
        users_map = {u.id: u for u in (getattr(result, 'users', None) or [])}
        gifts     = getattr(result, 'gifts', None) or []
        items     = []
        for gift in gifts:
            oid_obj = getattr(gift, 'owner_id', None)
            opid = None
            if oid_obj is not None:
                opid = (getattr(oid_obj, 'user_id', None) or getattr(oid_obj, 'id', None))
                if opid is None and isinstance(oid_obj, int):
                    opid = oid_obj
            owner    = users_map.get(opid) if opid else None
            username = getattr(owner, 'username', None) if owner else None
            name     = ""
            if owner:
                name = f"{owner.first_name or ''} {owner.last_name or ''}".strip()
            title = getattr(gift, 'title', '?')
            slug  = (getattr(gift, 'slug', None)
                     or getattr(gift, 'unique_id', None)
                     or str(getattr(gift, 'num', '')))
            num   = getattr(gift, 'num', '?')
            price = extract_price(gift)

            # Логируем поля первого подарка каждой коллекции для отладки цены
            if not items:
                d = getattr(gift, '__dict__', {})
                safe = {k: v for k, v in d.items() if not k.startswith('_') and isinstance(v, (int, str, float, bool, type(None)))}
                logger.info(f"[DEBUG] gid={gift_id} first gift fields: {safe}")

            items.append({
                "owner": owner, "owner_id": opid,
                "username": username, "name": name,
                "title": title, "slug": slug,
                "num": num, "price": price,
            })
        next_offset = getattr(result, 'next_offset', "") or ""
        return items, next_offset
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds}s")
        await asyncio.sleep(e.seconds + 2)
        return [], ""
    except Exception as e:
        logger.error(f"fetch_market_gifts gid={gift_id}: {e}")
        return [], ""


# ===================== SEARCH =====================
async def search_market(
    status_msg: Message,
    gift_ids_list: list = None,
    max_results: int = 100,
    girls_only: bool = False,
    price_min: int = None,
    price_max: int = None,
) -> int:
    global is_searching
    is_searching = True
    found = 0
    seen_slugs    = set()
    seen_girl_ids = set()

    if not gift_ids_list:
        if not NFT_COLLECTIONS:
            await load_collections()
        gift_ids_list = list(NFT_COLLECTIONS.values())

    if not gift_ids_list:
        is_searching = False
        return 0

    logger.info(f"🔍 Поиск: {len(gift_ids_list)} коллекций, price={price_min}-{price_max}, girls={girls_only}")

    try:
        for gid in gift_ids_list:
            if not is_searching or found >= max_results:
                break
            offset = ""
            empty_streak = 0
            while is_searching and found < max_results:
                items, next_offset = await fetch_market_gifts(gift_id=gid, offset=offset, limit=100)
                if not items:
                    empty_streak += 1
                    if empty_streak >= 3:
                        break
                    await asyncio.sleep(1)
                    continue
                empty_streak = 0
                for item in items:
                    if not is_searching or found >= max_results:
                        break
                    slug = item.get("slug", "")
                    if slug and slug in seen_slugs:
                        continue
                    if slug:
                        seen_slugs.add(slug)

                    price = item.get("price")

                    # Фильтр по цене
                    if price_min is not None or price_max is not None:
                        if price is None:
                            continue
                        if price_min is not None and price < price_min:
                            continue
                        if price_max is not None and price > price_max:
                            continue

                    # Фильтр по девушкам
                    if girls_only:
                        owner = item.get("owner")
                        if not owner or not is_girl(owner):
                            continue
                        oid = item.get("owner_id")
                        if oid and oid in seen_girl_ids:
                            continue
                        if oid:
                            seen_girl_ids.add(oid)

                    found += 1
                    stats["found"] += 1
                    price_text = f"⭐️ {price:,}".replace(",", " ") if price else "цена неизвестна"
                    owner_text = f"@{item['username']}" if item['username'] else f"👤 {item['name'] or 'Скрыт'}"
                    prefix = "👧 " if girls_only else ""
                    kb = user_nft_kb(item['username'], slug, item['title'], item['num'])
                    try:
                        await status_msg.bot.send_message(
                            chat_id=status_msg.chat.id,
                            text=(
                                f"{prefix}🎁 <b>{item['title']} #{item['num']}</b>\n"
                                f"👤 {owner_text}\n"
                                f"💰 {price_text}"
                            ),
                            parse_mode="HTML",
                            reply_markup=kb
                        )
                    except Exception as e:
                        logger.warning(f"send: {e}")
                    await asyncio.sleep(0.1)

                try:
                    lbl = "👧 Девушек" if girls_only else "NFT"
                    await status_msg.edit_text(
                        f"🔍 Ищу...\n🎁 Найдено {lbl}: <b>{found}</b>",
                        parse_mode="HTML", reply_markup=stop_kb()
                    )
                except Exception:
                    pass
                if not next_offset:
                    break
                offset = next_offset
                await asyncio.sleep(0.5)
    except Exception as e:
        logger.error(f"search_market: {e}")
    finally:
        is_searching = False
    return found


# ===================== RUNNERS =====================
async def run_nft_search(callback: CallbackQuery, cat_key: str = None, gift_ids_list: list = None):
    global is_searching
    if is_searching:
        await callback.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    price_min, price_max, label = None, None, "🎁 Все NFT"
    if cat_key and cat_key in PRICE_CATEGORIES:
        cat = PRICE_CATEGORIES[cat_key]
        price_min = cat["min"] if cat["min"] > 0 else None
        price_max = cat["max"]
        label = f"🎁 {cat['label']} ({cat['desc']})"

    await callback.answer("🔍 Запускаю поиск...")
    stats["checks"] += 1

    if not gift_ids_list:
        if not NFT_COLLECTIONS:
            await load_collections()
        gift_ids_list = list(NFT_COLLECTIONS.values())

    if not gift_ids_list:
        await callback.message.answer("❌ Не удалось загрузить коллекции.", reply_markup=menu_kb())
        return

    status = await callback.message.answer(
        f"<b>{label}</b>\n\n🔍 Найдено: 0", parse_mode="HTML", reply_markup=stop_kb()
    )
    found = await search_market(status, gift_ids_list=gift_ids_list,
                                max_results=100, price_min=price_min, price_max=price_max)
    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n{label}\n🎁 Найдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


async def run_girl_search(callback: CallbackQuery, cat_key: str = None, gift_ids_list: list = None):
    global is_searching
    if is_searching:
        await callback.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    price_min, price_max, label = None, None, "👧 Девушки — все цены"
    if cat_key and cat_key in PRICE_CATEGORIES:
        cat = PRICE_CATEGORIES[cat_key]
        price_min = cat["min"] if cat["min"] > 0 else None
        price_max = cat["max"]
        label = f"👧 Девушки — {cat['label']} ({cat['desc']})"

    await callback.answer("👧 Ищу девушек...")
    stats["checks"] += 1

    if not gift_ids_list:
        if not NFT_COLLECTIONS:
            await load_collections()
        gift_ids_list = list(NFT_COLLECTIONS.values())

    if not gift_ids_list:
        await callback.message.answer("❌ Не удалось загрузить коллекции.", reply_markup=menu_kb())
        return

    status = await callback.message.answer(
        f"<b>{label}</b>\n\n🔍 Найдено: 0", parse_mode="HTML", reply_markup=stop_kb()
    )
    found = await search_market(status, gift_ids_list=gift_ids_list, max_results=100,
                                girls_only=True, price_min=price_min, price_max=price_max)
    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n{label}\n👧 Найдено: <b>{found}</b>",
            parse_mode="HTML", reply_markup=menu_kb()
        )
    except Exception:
        pass


# ===================== /START =====================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    add_user_to_db(message.from_user.id)

    authorized = await check_authorized()

    if not authorized:
        if is_admin(message.from_user.id):
            await message.answer(
                "⚙️ <b>Нужна авторизация Telegram</b>\n\n"
                "Введи номер телефона аккаунта для парсинга:\n"
                "<code>+79001234567</code>",
                parse_mode="HTML"
            )
            await state.set_state(Auth.phone)
        else:
            await message.answer(
                "⏳ <b>Бот настраивается</b>\n\nПопробуй позже.",
                parse_mode="HTML"
            )
        return

    await message.answer(
        "🎁 <b>NFT Market Parser</b>\n\nПарсю маркет Telegram\n\n👇 Выбери действие:",
        parse_mode="HTML", reply_markup=main_kb()
    )


# ===================== /ADMIN =====================
@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(
            f"❌ Нет доступа.\n\nТвой ID: <code>{message.from_user.id}</code>",
            parse_mode="HTML"
        )
        return
    await state.clear()
    users = load_users()
    authorized = await check_authorized()
    auth_status = "✅ Авторизован" if authorized else "❌ Не авторизован"
    await message.answer(
        f"👑 <b>Админ панель</b>\n\n"
        f"🔐 Telethon: <b>{auth_status}</b>\n"
        f"👥 Пользователей: <b>{len(users)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено NFT: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )


# ===================== MENU =====================
@dp.callback_query(F.data == "menu")
async def cb_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "🎁 <b>NFT Market Parser</b>\n\n👇 Выбери действие:",
        parse_mode="HTML", reply_markup=main_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "do_auth")
async def cb_do_auth(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Только для администратора", show_alert=True)
        return
    await state.set_state(Auth.phone)
    await callback.message.answer(
        "📱 Введи номер телефона: <code>+79001234567</code>", parse_mode="HTML"
    )
    await callback.answer()

@dp.callback_query(F.data == "search_nft_menu")
async def cb_search_nft_menu(callback: CallbackQuery):
    await callback.message.answer("🎁 <b>Искать NFT</b>\n\nВыбери категорию:", parse_mode="HTML", reply_markup=nft_difficulty_kb())
    await callback.answer()

@dp.callback_query(F.data == "search_girls_menu")
async def cb_search_girls_menu(callback: CallbackQuery):
    await callback.message.answer("👧 <b>Искать девушек</b>\n\nВыбери категорию:", parse_mode="HTML", reply_markup=girls_difficulty_kb())
    await callback.answer()

@dp.callback_query(F.data == "nft_all")
async def cb_nft_all(callback: CallbackQuery):
    await run_nft_search(callback)

@dp.callback_query(F.data == "nft_cheap")
async def cb_nft_cheap(callback: CallbackQuery):
    await run_nft_search(callback, "cheap")

@dp.callback_query(F.data == "nft_mid")
async def cb_nft_mid(callback: CallbackQuery):
    await run_nft_search(callback, "mid")

@dp.callback_query(F.data == "nft_hard")
async def cb_nft_hard(callback: CallbackQuery):
    await run_nft_search(callback, "hard")

@dp.callback_query(F.data == "nft_ultra")
async def cb_nft_ultra(callback: CallbackQuery):
    await run_nft_search(callback, "ultra")

@dp.callback_query(F.data == "girl_all")
async def cb_girl_all(callback: CallbackQuery):
    await run_girl_search(callback)

@dp.callback_query(F.data == "girl_cheap")
async def cb_girl_cheap(callback: CallbackQuery):
    await run_girl_search(callback, "cheap")

@dp.callback_query(F.data == "girl_mid")
async def cb_girl_mid(callback: CallbackQuery):
    await run_girl_search(callback, "mid")

@dp.callback_query(F.data == "girl_hard")
async def cb_girl_hard(callback: CallbackQuery):
    await run_girl_search(callback, "hard")

@dp.callback_query(F.data == "girl_ultra")
async def cb_girl_ultra(callback: CallbackQuery):
    await run_girl_search(callback, "ultra")

@dp.callback_query(F.data == "girl_col")
async def cb_girl_col(callback: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    await callback.message.answer("🗂 <b>Выбери коллекцию:</b>", parse_mode="HTML", reply_markup=girls_col_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("gcol_"))
async def cb_gcol(callback: CallbackQuery):
    idx = int(callback.data[5:])
    items = list(NFT_COLLECTIONS.items())
    if idx >= len(items):
        await callback.answer("❌ Коллекция не найдена", show_alert=True)
        return
    _, gift_id = items[idx]
    await run_girl_search(callback, gift_ids_list=[gift_id])

@dp.callback_query(F.data == "market_col")
async def cb_market_col(callback: CallbackQuery):
    if not NFT_COLLECTIONS:
        await load_collections()
    if not NFT_COLLECTIONS:
        await callback.message.answer("❌ Не удалось загрузить коллекции", reply_markup=menu_kb())
        await callback.answer()
        return
    items = list(NFT_COLLECTIONS.keys())
    rows = []
    for i in range(0, len(items), 2):
        row = [InlineKeyboardButton(text=items[i], callback_data=f"mcol_{i}")]
        if i + 1 < len(items):
            row.append(InlineKeyboardButton(text=items[i+1], callback_data=f"mcol_{i+1}"))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="search_nft_menu")])
    await callback.message.answer("🗂 <b>Выбери коллекцию:</b>", parse_mode="HTML",
                                   reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()

@dp.callback_query(F.data.startswith("mcol_"))
async def cb_mcol(callback: CallbackQuery):
    idx = int(callback.data[5:])
    items = list(NFT_COLLECTIONS.items())
    if idx >= len(items):
        await callback.answer("❌ Коллекция не найдена", show_alert=True)
        return
    _, gift_id = items[idx]
    await run_nft_search(callback, gift_ids_list=[gift_id])

@dp.callback_query(F.data == "stop_search")
async def cb_stop(callback: CallbackQuery):
    global is_searching
    is_searching = False
    await callback.answer("⏹ Останавливаю...")
    try:
        await callback.message.edit_text("⏹ <b>Поиск остановлен</b>", parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        pass

@dp.callback_query(F.data == "stats")
async def cb_stats(callback: CallbackQuery):
    await callback.message.answer(
        f"📊 <b>Статистика</b>\n\n🔍 Поисков: <b>{stats['checks']}</b>\n🎁 Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML"
    )
    await callback.answer()


# ===================== ADMIN CALLBACKS =====================
@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(Broadcast.message)
    await callback.message.answer(
        "📢 <b>Рассылка</b>\n\nОтправь сообщение (текст, фото, видео).\n\n/cancel — отмена",
        parse_mode="HTML", reply_markup=cancel_kb()
    )
    await callback.answer()

@dp.message(Broadcast.message)
async def broadcast_get_msg(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    await state.update_data(broadcast_msg_id=message.message_id, broadcast_chat_id=message.chat.id)
    await state.set_state(None)
    await message.answer("✅ Подтверди рассылку:", reply_markup=confirm_broadcast_kb())

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def cb_broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    data = await state.get_data()
    msg_id  = data.get("broadcast_msg_id")
    chat_id = data.get("broadcast_chat_id")
    if not msg_id:
        await callback.answer("❌ Сообщение не найдено", show_alert=True)
        return
    users = load_users()
    status = await callback.message.answer(
        f"📢 Рассылка для <b>{len(users)}</b> пользователей...", parse_mode="HTML"
    )
    await callback.answer()
    ok, fail = 0, 0
    for i, uid in enumerate(users):
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=chat_id, message_id=msg_id)
            ok += 1
        except Exception as e:
            logger.warning(f"Broadcast {uid}: {e}")
            fail += 1
        if (i + 1) % 20 == 0:
            try:
                await status.edit_text(f"📢 Отправляю... ({i+1}/{len(users)})")
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await status.edit_text(
        f"✅ <b>Готово!</b>\n✅ Отправлено: <b>{ok}</b>\n❌ Ошибок: <b>{fail}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    users = load_users()
    await callback.message.answer(
        f"👥 <b>Пользователей в базе: {len(users)}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    users = load_users()
    await callback.message.answer(
        f"📊 <b>Статистика</b>\n\n👥 Пользователей: <b>{len(users)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n🎁 Найдено: <b>{stats['found']}</b>",
        parse_mode="HTML", reply_markup=admin_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_auth")
async def cb_admin_auth(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)
    await callback.answer()

@dp.callback_query(F.data == "admin_logout")
async def cb_admin_logout(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    try:
        await tg_client.log_out()
    except Exception:
        pass
    await callback.message.answer("✅ Вышел из TG аккаунта.", reply_markup=admin_kb())
    await callback.answer()

@dp.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.clear()
    await callback.message.answer("❌ Отменено", reply_markup=admin_kb())
    await callback.answer()


# ===================== AUTH HANDLERS =====================
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
        result = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=result.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer(
            "📨 Код отправлен в Telegram.\n\nВведи код без пробелов: <code>12345</code>",
            parse_mode="HTML"
        )
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
            f"✅ <b>Авторизован как @{me.username or me.first_name}!</b>\n\n"
            f"Загружено коллекций: <b>{len(NFT_COLLECTIONS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer(
            "🔐 Требуется пароль 2FA.\n\nВведи пароль следующим сообщением:",
            parse_mode="HTML"
        )
    except Exception as e:
        await message.answer(
            f"❌ Неверный код: <code>{e}</code>\n\nПопробуй снова:",
            parse_mode="HTML"
        )
        # Не сбрасываем state — ждём правильный код

@dp.message(Auth.password)
async def auth_password(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    pwd = message.text.strip()
    try:
        await tg_client.sign_in(password=pwd)
        me = await tg_client.get_me()
        await state.clear()
        await load_collections()
        await message.answer(
            f"✅ <b>Авторизован как @{me.username or me.first_name}!</b>\n\n"
            f"Загружено коллекций: <b>{len(NFT_COLLECTIONS)}</b>",
            parse_mode="HTML", reply_markup=main_kb()
        )
    except Exception as e:
        # Не сбрасываем state — даём попробовать пароль снова
        await message.answer(
            f"❌ Неверный пароль 2FA: <code>{e}</code>\n\nПопробуй ещё раз:",
            parse_mode="HTML"
        )

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

@dp.message(Command("myid"))
async def cmd_myid(message: Message):
    await message.answer(f"🆔 Твой ID: <code>{message.from_user.id}</code>", parse_mode="HTML")

@dp.message(Command("collections"))
async def cmd_collections(message: Message):
    """Показать загруженные коллекции — для отладки"""
    if not is_admin(message.from_user.id):
        return
    if not NFT_COLLECTIONS:
        await load_collections()
    text = f"📦 <b>Коллекций: {len(NFT_COLLECTIONS)}</b>\n\n"
    for name, gid in list(NFT_COLLECTIONS.items())[:30]:
        text += f"• {name} (id={gid})\n"
    await message.answer(text, parse_mode="HTML")


# ===================== MAIN =====================
async def main():
    if not tg_client.is_connected():
        await tg_client.connect()
    logger.info("🎁 NFT Market Parser запущен!")
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
            logger.info(f"✅ Telethon авторизован, коллекций: {len(NFT_COLLECTIONS)}")
        else:
            logger.warning("⚠️ Telethon не авторизован — нужно пройти авторизацию через /start или /admin")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
