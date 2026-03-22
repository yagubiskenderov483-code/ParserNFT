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

# ========================
API_ID = 28687552
API_HASH = "1abf9a58d0c22f62437bec89bd6b27a3"
BOT_TOKEN = "8629977687:AAEv-QHXC-9Bh8lyixVrMp2bIlwxT_yAs2k"
ADMIN_ID = 174415647
SESSION_NAME = "nft_session"
# ========================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
tg_client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

stats = {"checks": 0, "found": 0}
is_searching = False

# ID коллекций — берём из getStarGifts
NFT_COLLECTIONS = {}  # будет заполнено при старте: {title: gift_id}


class Auth(StatesGroup):
    phone = State()
    code = State()
    password = State()


def main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🛒 Маркет — все NFT на продаже", callback_data="market_all")],
        [InlineKeyboardButton(text="🗂 Выбрать коллекцию", callback_data="market_col")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="stats")],
    ])

def stop_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹ Стоп", callback_data="stop_search")],
    ])

def menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Ещё", callback_data="market_all")],
        [InlineKeyboardButton(text="📱 Меню", callback_data="menu")],
    ])

def user_nft_kb(username: str, slug: str):
    buttons = []
    if username:
        buttons.append([InlineKeyboardButton(text=f"👤 @{username}", url=f"https://t.me/{username}")])
    buttons.append([InlineKeyboardButton(text="🎁 Открыть NFT", url=f"https://t.me/nft/{slug}")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


async def load_collections():
    """Загружает список коллекций из Telegram API"""
    global NFT_COLLECTIONS
    try:
        result = await tg_client(GetStarGiftsRequest(hash=0))
        for gift in result.gifts:
            title = getattr(gift, 'title', None)
            gift_id = getattr(gift, 'id', None)
            # Только уникальные (NFT) подарки имеют title
            if title and gift_id:
                NFT_COLLECTIONS[title] = gift_id
        logger.info(f"Загружено коллекций: {len(NFT_COLLECTIONS)}")
    except Exception as e:
        logger.error(f"Ошибка загрузки коллекций: {e}")


async def fetch_market_gifts(gift_id: int = None, offset: str = "", limit: int = 50) -> tuple:
    """
    Получает NFT с маркета через payments.getResaleStarGifts.
    Возвращает (список подарков с владельцами, next_offset)
    """
    try:
        result = await tg_client(GetResaleStarGiftsRequest(
            gift_id=gift_id,
            offset=offset,
            limit=limit,
        ))

        items = []
        users_map = {u.id: u for u in (result.users or [])}

        for gift in (result.gifts or []):
            # Получаем владельца
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
            price = getattr(gift, 'resell_stars', None) or getattr(gift, 'availability_resale_stars', None)

            items.append({
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
        logger.error(f"getResaleStarGifts error: {e}")
        return [], ""


async def search_market(status_msg: Message, gift_id: int = None, max_results: int = 30):
    """Парсит маркет и выдаёт NFT на продаже"""
    global is_searching
    is_searching = True
    found = 0

    # Если gift_id не задан — берём все коллекции по очереди
    if gift_id is not None:
        gift_ids = [gift_id]
    else:
        if not NFT_COLLECTIONS:
            await load_collections()
        gift_ids = list(NFT_COLLECTIONS.values())

    try:
        for gid in gift_ids:
            if not is_searching or found >= max_results:
                break
            offset = ""
            while is_searching and found < max_results:
                items, next_offset = await fetch_market_gifts(gift_id=gid, offset=offset)

                if not items:
                    break

                for item in items:
                    if not is_searching or found >= max_results:
                        break

                    found += 1
                    stats["found"] += 1

                    price_text = f"⭐️ {item['price']}" if item['price'] else "цена неизвестна"
                    owner_text = f"@{item['username']}" if item['username'] else f"👤 {item['name'] or 'Скрыт'}"
                    slug = item['slug'] or f"{item['title']}-{item['num']}".replace(" ", "")

                    await status_msg.bot.send_message(
                        chat_id=status_msg.chat.id,
                        text=f"🎁 <b>{item['title']} #{item['num']}</b>\n"
                             f"👤 {owner_text}\n"
                             f"💰 {price_text}",
                        parse_mode="HTML",
                        reply_markup=user_nft_kb(item['username'], slug)
                    )
                    await asyncio.sleep(0.2)

                # Обновляем статус
                try:
                    await status_msg.edit_text(
                        f"🛒 Парсю маркет...\n🎁 Найдено: <b>{found}</b>",
                        parse_mode="HTML",
                        reply_markup=stop_kb()
                    )
                except Exception:
                    pass

                if not next_offset:
                    break
                offset = next_offset
                await asyncio.sleep(0.5)

    finally:
        is_searching = False

    return found


# ===================== /START =====================
@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id

    authorized = False
    try:
        if tg_client.is_connected():
            authorized = await tg_client.is_user_authorized()
    except Exception:
        pass

    if not authorized:
        if uid == ADMIN_ID:
            await message.answer(
                "⚙️ <b>Первый запуск — нужна авторизация</b>\n\n"
                "📱 Введи номер: <code>+79001234567</code>",
                parse_mode="HTML"
            )
            await state.set_state(Auth.phone)
        else:
            await message.answer("⏳ Бот настраивается. Попробуй позже.")
        return

    await message.answer(
        "🎁 <b>NFT Market Parser</b>\n\n"
        "Парсю маркет Telegram — NFT которые сейчас продаются\n"
        "Получаю юзернейм продавца + ссылку на NFT\n\n"
        "👇 Выбери действие:",
        parse_mode="HTML",
        reply_markup=main_kb()
    )


# ===================== AUTH =====================
@dp.message(Auth.phone)
async def auth_phone(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    phone = message.text.strip()
    if not phone.startswith("+"):
        await message.answer("❌ Формат: <code>+79001234567</code>", parse_mode="HTML")
        return
    try:
        if not tg_client.is_connected():
            await tg_client.connect()
        result = await tg_client.send_code_request(phone)
        await state.update_data(phone=phone, phone_code_hash=result.phone_code_hash)
        await state.set_state(Auth.code)
        await message.answer("📨 Введи код: <code>12345</code>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")


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
        await message.answer(f"✅ Авторизован как @{me.username}", reply_markup=main_kb())
    except SessionPasswordNeededError:
        await state.set_state(Auth.password)
        await message.answer("🔐 Введи пароль 2FA:")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")
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
        await message.answer(f"✅ Авторизован как @{me.username}", reply_markup=main_kb())
    except Exception as e:
        await message.answer(f"❌ Неверный пароль: {e}")


@dp.message(Command("auth"))
async def cmd_auth(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("📱 Введи номер: <code>+79001234567</code>", parse_mode="HTML")
    await state.set_state(Auth.phone)


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


@dp.callback_query(F.data == "stats")
async def cb_stats(callback: CallbackQuery):
    await callback.message.answer(
        f"📊 <b>Статистика</b>\n\n"
        f"🔍 Поисков: <b>{stats['checks']}</b>\n"
        f"🎁 Найдено NFT: <b>{stats['found']}</b>",
        parse_mode="HTML"
    )
    await callback.answer()


# ===================== MARKET =====================
@dp.callback_query(F.data == "market_all")
async def cb_market_all(callback: CallbackQuery):
    global is_searching
    if is_searching:
        await callback.answer("⏳ Поиск уже идёт!", show_alert=True)
        return
    await callback.answer("🛒 Загружаю маркет...")
    stats["checks"] += 1

    status = await callback.message.answer(
        "🛒 Парсю маркет Telegram...\n🎁 Найдено: 0",
        parse_mode="HTML",
        reply_markup=stop_kb()
    )

    found = await search_market(status, max_results=30)

    try:
        await status.edit_text(
            f"✅ <b>Готово!</b>\n\n🎁 Показано NFT: <b>{found}</b>",
            parse_mode="HTML",
            reply_markup=menu_kb()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "market_col")
async def cb_market_col(callback: CallbackQuery):
    if not NFT_COLLECTIONS:
        await callback.answer("⏳ Загружаю коллекции...", show_alert=False)
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
    rows.append([InlineKeyboardButton(text="📱 Меню", callback_data="menu")])

    await callback.message.answer(
        "🗂 <b>Выбери коллекцию:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("mcol_"))
async def cb_mcol(callback: CallbackQuery):
    global is_searching
    if is_searching:
        await callback.answer("⏳ Поиск уже идёт!", show_alert=True)
        return

    idx = int(callback.data[5:])
    items = list(NFT_COLLECTIONS.items())
    col_name, gift_id = items[idx]

    await callback.answer(f"🔍 {col_name}")
    stats["checks"] += 1

    status = await callback.message.answer(
        f"🛒 Ищу <b>{col_name}</b> на маркете...\n🎁 Найдено: 0",
        parse_mode="HTML",
        reply_markup=stop_kb()
    )

    found = await search_market(status, gift_id=gift_id, max_results=30)

    try:
        await status.edit_text(
            f"✅ <b>{col_name}</b>\n\n🎁 Показано: <b>{found}</b>",
            parse_mode="HTML",
            reply_markup=menu_kb()
        )
    except Exception:
        pass


@dp.callback_query(F.data == "stop_search")
async def cb_stop(callback: CallbackQuery):
    global is_searching
    is_searching = False
    await callback.answer("⏹ Останавливаю...")
    try:
        await callback.message.edit_reply_markup(reply_markup=menu_kb())
    except Exception:
        pass


# ===================== MAIN =====================
async def main():
    await tg_client.connect()
    logger.info("🎁 NFT Market Parser запущен!")
    # Загружаем коллекции если уже авторизованы
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
