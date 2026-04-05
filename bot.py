import asyncio
import logging
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
BOT_TOKEN = "8746204744:AAFlTI76SnDdnkRAuZPC6Fy6OeqaXP370Uk"
ADMIN_ID = 7602363090
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
    "hard":  {"label": "🟠 Сложные", "min": 5001,  "max": 20000, "desc": "5000–20000 ⭐️"},
    "ultra": {"label": "🔴 Хард",    "min": 20001, "max": None,  "desc": "от 20000 ⭐️"},
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


# ===================== USERS DB =====================
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


# ===================== GIRL DETECTION =====================
def is_girl(user) -> bool:
    first = (getattr(user, 'first_name', '') or '').lower().strip()
    last = (getattr(user, 'last_name', '') or '').lower().strip()
    username = (getattr(user, 'username', '') or '').lower().strip()
    for name in GIRL_NAMES:
        if first.startswith(name) or last.startswith(name) or username.startswith(name):
            return True
    for kw in ['girl', 'lady', 'princess', 'queen', 'барби', 'принцесса', 'королева', 'baby', 'cute', 'sweetie']:
        if kw in first or kw in last or kw in username:
            return True
    return False


# ===================== KEYBOARDS =====================
def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎁 Искать NFT", callback_data="search_nft_menu")],
        [InlineKeyboardButton(text="👧 Искать девушек", callback_data="search_girls_menu")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
    ])

def nft_difficulty_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Все NFT (без фильтра цены)", callback_data="nft_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000 ⭐️)", callback_data="nft_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000–5000 ⭐️)", callback_data="nft_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000–20000 ⭐️)", callback_data="nft_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20000+ ⭐️)", callback_data="nft_ultra")],
        [InlineKeyboardButton(text="🗂 По коллекции", callback_data="market_col")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")],
    ])

def girls_difficulty_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Все девушки (без фильтра)", callback_data="girl_all")],
        [InlineKeyboardButton(text="💚 Дешёвые (до 2000 ⭐️)", callback_data="girl_cheap")],
        [InlineKeyboardButton(text="💛 Средние (2000–5000 ⭐️)", callback_data="girl_mid")],
        [InlineKeyboardButton(text="🟠 Сложные (5000–20000 ⭐️)", callback_data="girl_hard")],
        [InlineKeyboardButton(text="🔴 Хард (20000+ ⭐️)", callback_data="girl_ultra")],
        [InlineKeyboardButton(text="🗂 По коллекции", callback_data="girl_col")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")],
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

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="🔐 Авторизация TG", callback_data="admin_auth")],
        [InlineKeyboardButton(text="🚪 Выйти из TG", callback_data="admin_logout")],
        [InlineKeyboardButton(text="◀️ В меню", callback_data="menu")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ СТОП", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Искать ещё", callback_data="search_nft_menu")],
        [InlineKeyboardButton(text="📱 Меню", callback_data="menu")],
    ])

def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")],
    ])

def confirm_broadcast_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Отправить", callback_data="admin_broadcast_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")],
    ])

def user_nft_kb(username: str, slug: str):
    buttons = []
    if username:
        buttons.append([InlineKeyboardButton(text=f"👤 @{username}", url=f"https://t.me/{username}")])
    buttons.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=f"https://t.me/nft/{slug}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ===================== COLLECTIONS =====================
async def load_collections():
    global NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        for gift in result.gifts:
            title = getattr(gift, 'title', None)
            gift_id = getattr(gift, 'id', None)
            if title and gift_id:
                NFT_COLLECTIONS[title] = gift_id
        logger.info(f"Загружено коллекций: {len(NFT_COLLECTIONS)}")
    except Exception as e:
        logger.error(f"Ошибка загрузки коллекций: {e}")


# ===================== FETCH =====================
def extract_price(gift) -> int | None:
    for field in ['resell_stars', 'resale_amount', 'availability_resale_stars', 'stars', 'price']:
        val = getattr(gift, field, None)
        if val is not None:
            try:
                return int(val)
            except Exception:
                pass
    # Пробуем через __dict__
    d = getattr(gift, '__dict__', {})
    for k, v in d.items():
        if 'star' in k.lower() or 'price' in k.lower() or 'resale' in k.lower():
            try:
                return int(v)
            except Exception:
                pass
    return None


async def fetch_market_gifts(gift_id: int, offset: str = "", limit: int = 100) -> tuple:
    try:
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gift_id,
            offset=offset,
            limit=limit,
        ))
        items = []
        users_map = {u.id: u for u in (result.users or [])}
        for gift in (result.gifts or []):
            owner_id = getattr(gift, 'owner_id', None)
            owner_peer_id = getattr(owner_id, 'user_id', None) if owner_id else None
            owner = users_map.get(owner_peer_id) if owner_peer_id else None
            username = getattr(owner, 'username', None) if owner else None
            name = ""
            if owner:
                name = f"{owner.first_name or ''} {owner.last_name or ''}".strip()
            title = getattr(gift, 'title', '?')
            slug = getattr(gift, 'slug', None) or getattr(gift, 'unique_id', None) or str(getattr(gift, 'num', ''))
            num = getattr(gift, 'num', '?')
            price = extract_price(gift)

            items.append({
                "owner": owner,
                "owner_id": owner_peer_id,
                "username": username,
                "name": name,
                "title": title,
                "slug": slug,
                "num": num,
                "price": price,
            })
        next_offset = getattr(result, 'next_offset', "")
        return items, next_offset
    except FloodWaitError as e:
        logger.warning(f"FloodWait {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        return [], ""
    except Exception as e:
        logger.error(f"fetch error gid={gift_id}: {e}")
        return [], ""


# ===================== SEARCH =====================
async def search_market(
    status_msg: Message,
    gift_ids_list: list = None,
    max_results: int = 100,
    girls_only: bool = False,
    price_min: int = None,
    price_max: int = None,
):
    global is_searching
    is_searching = True
    found = 0
    seen_slugs = set()
    seen_girl_ids = set()
    price_logged = False

    if not gift_ids_list:
        if not NFT_COLLECTIONS:
            await load_collections()
        gift_ids_list = list(NFT_COLLECTIONS.values())

    try:
        for gid in gift_ids_list:
            if not is_searching or found >= max_results:
                break
            offset = ""
            empty_streak = 0

            while is_searching and found < max_results:
                items, next_offset = await fetch_market_gifts(gift_id=gid, offset=offset)

                if not items:
                    empty_streak += 1
                    if empty_streak >= 2:
                        break
                    await asyncio.sleep(0.5)
                    continue
                empty_streak = 0

                # Логируем цену первого NFT для диагностики
                if not price_logged and items:
                    logger.info(f"Sample price={items[0]['price']} title={items[0]['title']}")
                    price_logged = True

                for item in items:
                    if not is_searching or found >= max_results:
                        break

                    slug = item.get("slug", "")

                    # Пропускаем дубли по slug
                    if slug and slug in seen_slugs:
                        continue
                    if slug:
                        seen_slugs.add(slug)

                    price = item.get("price")

                    # Фильтр по цене — если цена None, пропускаем только если фильтр задан
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
                        owner_id = item.get("owner_id")
                        if owner_id and owner_id in seen_girl_ids:
                            continue
                        if owner_id:
                            seen_girl_ids.add(owner_id)

                    found += 1
                    stats["found"] += 1

                    price_text = f"⭐️ {price}" if price else "цена неизвестна"
                    owner_text = f"@{item['username']}" if item['username'] else f"👤 {item['name'] or 'Скрыт'}"
                    girl_tag = "👧 " if girls_only else ""

                    await status_msg.bot.send_message(
                        chat_id=status_msg.chat.id,
                        text=f"{girl_tag}🎁 <b>{item['title']} #{item['num']}</b>\n"
                             f"👤 {owner_text}\n"
                             f"💰 {price_text}",
                        parse_mode="HTML",
                        reply_markup=user_nft_kb(item['username'], slug)
                    )
                    await asyncio.sleep(0.15)

                try:
                    label = "👧 Девушек" if girls_only else "NFT"
                    await status_msg.edit_text(
                        f"🔍 Ищу...\n🎁 Найдено {label}: <b>{found}</b>",
                        parse_mode="HTML",
                        reply_markup=stop_kb()
                    )
                except Exception:
                    pass

                if not next_offset:
                    break
                offset = next_offset
                await asyncio.sleep(0.3)

    finally:
        is_searching = False

    return found


# ===================== SEARCH HELPERS =====================
async def run_nft_search(callback: CallbackQuery, cat_key: str = None, gift_ids_list: list = None):
    global is_searching
    if is_searching:
        await callback.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    if cat_key and cat_key in PRICE_CATEGORIES:
        cat = PRICE_CATEGORIES[cat_key]
        price_min = cat["min"] if cat["min"] > 0 else None
        price_max = cat["max"]
        label = f"🎁 {cat['label']} ({cat['desc']})"
    else:
        price_min = None
        price_max = None
        label = "🎁 Все NFT"

    await callback.answer("🔍 Запускаю поиск...")
    stats["checks"] += 1

    if not gift_ids_list:
        if not NFT_COLLECTIONS:
            await load_collections()
        gift_ids_list = list(NFT_COLLECTIONS.values())

    status = await callback.message.answer(
        f"<b>{label}</b>\n\n🔍 Найдено: 0",
        parse_mode="HTML",
        reply_markup=stop_kb()
    )

    found = await search_market(
        status,
        gift_ids_list=gift_ids_list,
        max_results=100,
        price_min=price_min,
        price_max=price_max,
    )

    result_text = (
        f"⏹ <b>Остановлено</b>\n{label}\n🎁 Найдено: <b>{found}</b>"
        if not is_searching else
        f"✅ <b>Готово!</b>\n{label}\n🎁 Найдено: <b>{found}</b>"
    )
    try:
        await status.edit_text(result_text, parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        pass


async def run_girl_search(callback: CallbackQuery, cat_key: str = None, gift_ids_list: list = None):
    global is_searching
    if is_searching:
        await callback.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    if cat_key and cat_key in PRICE_CATEGORIES:
        cat = PRICE_CATEGORIES[cat_key]
        price_min = cat["min"] if cat["min"] > 0 else None
        price_max = cat["max"]
        label = f"👧 Девушки — {cat['label']} ({cat['desc']})"
    else:
        price_min = None
        price_max = None
        label = "👧 Девушки — все цены"

    await callback.answer("👧 Ищу девушек...")
    stats["checks"] += 1

    if not gift_ids_list:
        if not NFT_COLLECTIONS:
            await load_collections()
        gift_ids_list = list(NFT_COLLECTIONS.values())

    status = await callback.message.answer(
        f"<b>{label}</b>\n\n🔍 Найдено: 0",
        parse_mode="HTML",
        reply_markup=stop_kb()
    )

    found = await search_market(
        status,
        gift_ids_list=gift_ids_list,
        max_results=100,
        girls_only=True,
        price_min=price_min,
        price_max=price_max,
    )

    result_text = (
        f"⏹ <b>Остановлено</b>\n{label}\n👧 Найдено: <b>{found}</b>"
        if not is_searching else
        f"✅ <b>Готово!</b>\n{label}\n👧 Найдено: <b>{found}</b>"
    )
    try:
        await status.edit_text(result_text, parse_mode="HTML", reply_markup=menu_kb())
    except Exception:
        pass


# ===================== /START & /ADMIN =====================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    add_user_to_db(message.from_user.id)
    authorized = False
    try:
        if tg_client.is_connected():
            authorized = await tg_client.is_user_authorized()
    except Exception:
        pass
    if not authorized:
        if message.from_user.id == ADMIN_ID:
            await message.answer(
                "⚙️ <b>Первый запуск — нужна авторизация</b>\n\n"
                "📱 Введи номер телефона: <code>+79001234567</code>",
                parse_mode="HTML"
            )
            await state.set_state(Auth.phone)
        else:
            await message.answer("⏳ Бот настраивается. Попробуй позже.")
        return
    await message.answer(
        "🎁 <b>NFT Market Parser</b>\n\nПарсю маркет Telegram\n\n👇 Выбери действие:",
        parse_mode="HTML",
        reply_markup=main_kb()
    )


@dp.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Нет доступа")
        return
    await state.clear()
    users = load_users()
    await message.answer(
        f"👑 <b>Админ панель</b>\n\n"
        f"👥 Пользователей: <b>{len(users)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено NFT: <b>{stats['found']}</b>",
        parse_mode="HTML",
        reply_markup=admin_kb()
    )


# ===================== MENU =====================
@dp.callback_query(F.data == "menu")
async def cb_menu(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "🎁 <b>NFT Market Parser</b>\n\n👇 Выбери действие:",
        parse_mode="HTML",
        reply_markup=main_kb()
    )
    await callback.answer()


@dp.callback_query(F.data == "search_nft_menu")
async def cb_search_nft_menu(callback: CallbackQuery):
    await callback.message.answer(
        "🎁 <b>Искать NFT</b>\n\nВыбери категорию:",
        parse_mode="HTML",
        reply_markup=nft_difficulty_kb()
    )
    await callback.answer()


@dp.callback_query(F.data == "search_girls_menu")
async def cb_search_girls_menu(callback: CallbackQuery):
    await callback.message.answer(
        "👧 <b>Искать девушек</b>\n\nВыбери категорию:",
        parse_mode="HTML",
        reply_markup=girls_difficulty_kb()
    )
    await callback.answer()


# ===================== NFT CALLBACKS =====================
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


# ===================== GIRL CALLBACKS =====================
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
    await callback.message.answer(
        "🗂 <b>Выбери коллекцию для поиска девушек:</b>",
        parse_mode="HTML",
        reply_markup=girls_col_kb()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("gcol_"))
async def cb_gcol(callback: CallbackQuery):
    idx = int(callback.data[5:])
    items = list(NFT_COLLECTIONS.items())
    col_name, gift_id = items[idx]
    await run_girl_search(callback, gift_ids_list=[gift_id])


# ===================== NFT COLLECTION CALLBACKS =====================
@dp.callback_query(F.data == "market_col")
async def cb_market_col(callback: CallbackQuery):
    if not NFT_COLLECTIONS:
        await callback.answer("⏳ Загружаю...", show_alert=False)
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
    await callback.message.answer(
        "🗂 <b>Выбери коллекцию:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("mcol_"))
async def cb_mcol(callback: CallbackQuery):
    idx = int(callback.data[5:])
    items = list(NFT_COLLECTIONS.items())
    col_name, gift_id = items[idx]
    await run_nft_search(callback, gift_ids_list=[gift_id])


# ===================== STOP =====================
@dp.callback_query(F.data == "stop_search")
async def cb_stop(callback: CallbackQuery):
    global is_searching
    is_searching = False
    await callback.answer("⏹ Останавливаю...", show_alert=False)
    try:
        await callback.message.edit_text(
            "⏹ <b>Поиск остановлен</b>",
            parse_mode="HTML",
            reply_markup=menu_kb()
        )
    except Exception:
        pass


# ===================== STATS =====================
@dp.callback_query(F.data == "stats")
async def cb_stats(callback: CallbackQuery):
    await callback.message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено NFT: <b>{stats['found']}</b>",
        parse_mode="HTML"
    )
    await callback.answer()


# ===================== ADMIN =====================
@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.set_state(Broadcast.message)
    await callback.message.answer(
        "📢 <b>Рассылка</b>\n\nОтправь сообщение для рассылки.",
        parse_mode="HTML",
        reply_markup=cancel_kb()
    )
    await callback.answer()

@dp.message(Broadcast.message)
async def broadcast_message(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.update_data(broadcast_msg_id=message.message_id, broadcast_chat_id=message.chat.id)
    await state.set_state(None)
    await message.answer("✅ Подтверди рассылку:", reply_markup=confirm_broadcast_kb())

@dp.callback_query(F.data == "admin_broadcast_confirm")
async def cb_broadcast_confirm(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    data = await state.get_data()
    msg_id = data.get("broadcast_msg_id")
    chat_id = data.get("broadcast_chat_id")
    if not msg_id:
        await callback.answer("❌ Сообщение не найдено", show_alert=True)
        return
    users = load_users()
    await callback.message.answer(f"📢 Рассылка для <b>{len(users)}</b> пользователей...", parse_mode="HTML")
    await callback.answer()
    success = 0
    failed = 0
    for uid in users:
        try:
            await bot.copy_message(chat_id=uid, from_chat_id=chat_id, message_id=msg_id)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.warning(f"Broadcast failed {uid}: {e}")
            failed += 1
    await callback.message.answer(
        f"✅ <b>Рассылка завершена!</b>\n\n✅ Успешно: <b>{success}</b>\n❌ Не доставлено: <b>{failed}</b>",
        parse_mode="HTML",
        reply_markup=admin_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "admin_users")
async def cb_admin_users(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    users = load_users()
    await callback.message.answer(
        f"👥 <b>Пользователи</b>\n\nВсего: <b>{len(users)}</b>\n\n"
        f"ID: {', '.join(str(u) for u in list(users)[:50])}{'...' if len(users) > 50 else ''}",
        parse_mode="HTML",
        reply_markup=admin_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    users = load_users()
    await callback.message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: <b>{len(users)}</b>\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено NFT: <b>{stats['found']}</b>",
        parse_mode="HTML",
        reply_markup=admin_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_auth")
async def cb_admin_auth(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)
    await callback.answer()

@dp.callback_query(F.data == "admin_logout")
async def cb_admin_logout(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        return
    try:
        await tg_client.log_out()
    except Exception:
        pass
    await callback.message.answer("✅ Вышел из TG аккаунта.", reply_markup=admin_kb())
    await callback.answer()

@dp.callback_query(F.data == "admin_cancel")
async def cb_admin_cancel(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await callback.message.answer("❌ Отменено", reply_markup=admin_kb())
    await callback.answer()


# ===================== AUTH =====================
@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Формат: <code>+79001234567</code>", parse_mode="HTML")
        return
    await message.answer("⏳ Подключаюсь...")
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
            await asyncio.sleep(1)
        result = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=result.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("📨 Код отправлен.\n\nВведи без пробелов: <code>12345</code>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")
        await state.clear()

@dp.message(Auth.code)
async def auth_code(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    code = message.text.strip().replace(" ", "")
    data = await state.get_data()
    try:
        await tg_client.sign_in(phone=data["phone"], code=code, phone_code_hash=data["phone_code_hash"])
        me = await tg_client.get_me()
        await state.clear()
        await load_collections()
        await message.answer(f"✅ Авторизован как @{me.username or me.first_name}", reply_markup=main_kb())
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("🔐 Введи пароль 2FA:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: <code>{e}</code>", parse_mode="HTML")
        await state.clear()

@dp.message(Auth.password)
async def auth_password(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        await tg_client.sign_in(password=message.text.strip())
        me = await tg_client.get_me()
        await state.clear()
        await load_collections()
        await message.answer(f"✅ Авторизован как @{me.username or me.first_name}", reply_markup=main_kb())
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: <code>{e}</code>", parse_mode="HTML")

@dp.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await state.clear()
    await message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)


# ===================== MAIN =====================
async def main():
    await tg_client.connect()
    logger.info("🎁 NFT Market Parser запущен!")
    try:
        if await tg_client.is_user_authorized():
            await load_collections()
    except Exception:
        pass
    try:
        await dp.start_polling(bot)
    finally:
        await tg_client.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
