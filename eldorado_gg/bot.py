import logging, json, os, math, html, time
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from urllib.parse import urlencode, quote
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, WebAppInfo
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOT_TOKEN    = "8879343383:AAGO3viGf3PERRFA-c5Jx0Wz3cqm-tIj6J4"
ADMIN_IDS    = {8726084830, 90283607, 7186944876}
BOT_USERNAME = "EldoradoGGRobot"
MANAGER_URL  = "https://t.me/EldoradoGGManager"
MANAGER_TAG  = "@EldoradoGGManager"
SUPPORT_URL  = "https://t.me/EldoradoGGSupport"
REVIEWS_URL  = "https://t.me/EldoradoProofs"
MINI_APP_URL = "https://www.eldorado.gg/"
CRYPTO_ADDR  = "UQDGN5pfjPxorFyjN2xha84bapuADDtPcRofNDJ4dK2YXxZd"
CRYPTO_BOT   = "https://t.me/send?start=IVbfPL7Tk4XA"
USDT_MASTER  = "EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs"
CARD_NUM     = "+79041751408"
CARD_NAME    = "Александр Ф."
CARD_BANK_RU = "ВТБ"
CARD_BANK_EN = "VTB"
DB_FILE      = "db.json"

def ce(eid, fb): return f"<tg-emoji emoji-id='{eid}'>{fb}</tg-emoji>"

E = {
    "user":       ce("5199552030615558774", "👤"),
    "star":       ce("5267500801240092311", "⭐"),
    "shield":     ce("5197434882321567830", "🛡"),
    "gift":       ce("5197369495739455200", "🎁"),
    "lock":       ce("5197161121106123533", "🔒"),
    "globe":      ce("5377746319601324795", "🌍"),
    "premium":    ce("5377620962390857342", "⭐"),
    "pencil":     ce("5197371802136892976", "✏️"),
    "card":       ce("5445353829304387411", "💳"),
    "cross":      ce("5443127283898405358", "📥"),
    "rocket":     ce("5444856076954520455", "🚀"),
    "sticker":    ce("5294167145079395967", "🛍"),
    "fire":       ce("5303138782004924588", "🔥"),
    "bell":       ce("5312361253610475399", "🔔"),
    "deal":       ce("5445221832074483553", "💼"),
    "trophy":     ce("5332455502917949981", "🏆"),
    "check":      ce("5274055917766202507", "✅"),
    "money":      ce("5278467510604160626", "💰"),
    "diamond":    ce("5264713049637409446", "💎"),
    "nft":        ce("5193177581888755275", "🖼"),
    "bag":        ce("5377660214096974712", "🛍"),
    "medal":      ce("5463289097336405244", "🥇"),
    "gem":        ce("5258203794772085854", "💎"),
    "clock":      ce("5429651785352501917", "⏰"),
    "handshake":  ce("5287231198098117669", "🤝"),
    "crystal":    ce("5195033767969839232", "🔮"),
    "safe":       ce("5262517101578443800", "🔐"),
    "chart":      ce("5382194935057372936", "📊"),
    "spark":      ce("5902449142575141204", "✨"),
    "target":     ce("5893081007153746175", "❌"),
    "pin":        ce("5893297890117292323", "📞"),
    "wallet":     ce("5893382531037794941", "👛"),
    "num1":       ce("5794164805065514131", "1️⃣"),
    "num2":       ce("5794085322400733645", "2️⃣"),
    "num3":       ce("5794280000383358988", "3️⃣"),
    "num4":       ce("5794241397217304511", "4️⃣"),
    "bank":       ce("5238132025323444613", "🏦"),
    "banknote":   ce("5201873447554145566", "💵"),
    "link":       ce("5902449142575141204", "🔗"),
    "shine":      ce("5235630047959727475", "✨"),
    "store":      ce("4988289890769699938", "🏪"),
    "tonkeeper":  ce("5397829221605191505", "💎"),
    "top_medal":  ce("5188344996356448758", "🏆"),
    "stars_deal": ce("5321485469249198987", "⭐"),
    "joined":     ce("5902335789798265487", "🤝"),
    "security_e": ce("5197288647275071607", "🔰"),
    "deal_link":  ce("5972261808747057065", "🔗"),
    "warning":    ce("5447644880824181073", "⚠️"),
    "stats":      ce("5028746137645876535", "📊"),
    "requisites": ce("5242631901214171852", "📋"),
    "cryptobot":  ce("5242606681166220600", "🤖"),
    "welcome":    ce("5251340119205501791", "👋"),
    "balance_e":  ce("5424976816530014958", "💰"),
}

CD  = ce("5264713049637409446", "💎")
CM  = ce("5278467510604160626", "💰")
CDL = ce("5445221832074483553", "💼")
CSH = ce("5197434882321567830", "🛡")
CL  = ce("5197161121106123533", "🔒")
CG  = ce("5197369495739455200", "🎁")
CF  = ce("5303138782004924588", "🔥")
CS  = ce("5267500801240092311", "⭐")
CR  = ce("5195033767969839232", "🔮")

Eu   = E["user"];       Est  = E["stars_deal"]; Edl  = E["deal"]
Ech  = E["check"];      Emn  = E["money"];      Edm  = E["diamond"]
Enft = E["nft"];        Eprem= E["premium"];    Epen = E["pencil"]
Ebl  = E["bell"];       Ewrn = E["warning"];    Esec = E["security_e"]
Eln  = E["link"];       Edln = E["deal_link"];  Eton = E["tonkeeper"]
Ecbt = E["cryptobot"];  Ereq = E["requisites"]; Ewlt = E["wallet"]
Esrk = E["spark"];      Estr = E["stats"];      Etph = E["trophy"]
Ejn  = E["joined"];     Ewlc = E["welcome"];    Ebal = E["balance_e"]
Egm  = E["gem"];        En1  = E["num1"];       En2  = E["num2"]
En3  = E["num3"];       En4  = E["num4"];       Ebag = E["bag"]
Eslr = E["shield"];     Efire= E["fire"];       Elck = E["lock"]
Egft = E["gift"];       Erkt = E["crystal"];    Epct = E["handshake"]
Ecrd = E["card"];       Ephn = E["pin"];        Emdl = E["medal"]
Ecwn = E["safe"];       Ebnk = E["bank"];       Ebnk2= E["banknote"]
Ecrss= E["cross"];      Eshne= E["shine"];      Echart=E["chart"]
Etgt = E["target"];     Estck= E["sticker"]
Eusdt= ce("5406841020769936275", "💵")

# ─── Типы сделок ──────────────────────────────────────────────────────────────
TNAMES_RU = {
    "nft":      f"{Enft} NFT подарок",
    "username": "Username",
    "stars":    f"{Est} Звёзды Telegram",
    "crypto":   f"{Edm} Крипта (TON/USDT)",
    "premium":  f"{Egm} Telegram Premium",
}
TNAMES_EN = {
    "nft":      f"{Enft} NFT Gift",
    "username": "Username",
    "stars":    f"{Est} Telegram Stars",
    "crypto":   f"{Edm} Crypto (TON/USDT)",
    "premium":  f"{Egm} Telegram Premium",
}
TNAMES_PLAIN_RU = {
    "nft":"NFT подарок","username":"Username","stars":"Звёзды Telegram",
    "crypto":"Крипта (TON/USDT)","premium":"Telegram Premium",
}
TNAMES_PLAIN_EN = {
    "nft":"NFT Gift","username":"Username","stars":"Telegram Stars",
    "crypto":"Crypto (TON/USDT)","premium":"Telegram Premium",
}

def tname(t, lang="ru"): return TNAMES_EN.get(t, t) if lang=="en" else TNAMES_RU.get(t, t)
def tname_plain(t, lang="ru"): return TNAMES_PLAIN_EN.get(t,t) if lang=="en" else TNAMES_PLAIN_RU.get(t,t)

# ─── Валюты ───────────────────────────────────────────────────────────────────
CUR_PLAIN_RU = {
    "TON":"TON","USDT":"USDT","Stars":"Звёзды",
    "RUB":"Рубли","KZT":"Теңге","AZN":"Manat","KGS":"Сом",
    "UZS":"So'm","TJS":"Сомонӣ","BYN":"Рубли (BYN)","UAH":"Гривні","GEL":"ლარი",
}
CUR_PLAIN_EN = {
    "TON":"TON","USDT":"USDT","Stars":"Stars",
    "RUB":"Rubles","KZT":"Tenge","AZN":"Manat","KGS":"Som",
    "UZS":"So'm","TJS":"Somoni","BYN":"Rubles (BYN)","UAH":"Hryvnia","GEL":"Lari",
}
CUR_EMOJI = {
    "TON":"💎","USDT":"💵","Stars":"⭐","RUB":"🇷🇺","KZT":"🇰🇿",
    "AZN":"🇦🇿","KGS":"🇰🇬","UZS":"🇺🇿","TJS":"🇹🇯","BYN":"🇧🇾",
    "UAH":"🇺🇦","GEL":"🇬🇪",
}
CURMAP = {
    "cur_ton":"TON","cur_usdt":"USDT","cur_rub":"RUB","cur_stars":"Stars","cur_uah":"UAH",
}
DEAL_CURRENCIES = ("TON","USDT","RUB","Stars","UAH")
CUR_BTN_ICON = {
    "TON":"5406976471153545018",
    "USDT":"5406841020769936275",
    "Stars":"5406812184359507637",
    "RUB":"5377472000040115969",
    "UAH":"5375587209476843297",
}

CUR_FLAG = {
    "TON":Eton,"USDT":Eusdt,"Stars":Est,
    "RUB":Ebnk,"UAH":Ebnk,
}
FIAT_CURRENCIES = {"RUB","UAH"}
CUR_FORMS_RU = {
    "RUB": ("рубль", "рубля", "рублей"),
    "Stars": ("звезда", "звезды", "звёзд"),
    "UAH": ("гривна", "гривны", "гривен"),
}

def cur_plain(code, lang="ru"):
    if lang=="en": return CUR_PLAIN_EN.get(code, code)
    return CUR_PLAIN_RU.get(code, code)

def cur_button_text(code, lang="ru"):
    return cur_plain(code, lang)

def ru_plural(n, one, few, many):
    try:
        value=Decimal(str(n).replace(",",".").replace(" ",""))
        if value!=value.to_integral_value():
            return many
        n_int=abs(int(value))
    except Exception:
        return many
    n100=n_int%100; n10=n_int%10
    if 11<=n100<=14: return many
    if n10==1: return one
    if 2<=n10<=4: return few
    return many

def cur_word(amount, code, lang="ru"):
    if lang!="ru":
        return cur_plain(code, lang)
    forms=CUR_FORMS_RU.get(code)
    if not forms:
        return cur_plain(code, lang)
    return ru_plural(amount, *forms)

def cur_amount_phrase(amount, code, lang="ru"):
    return f"{amount} {cur_word(amount, code, lang)}"

def cur_amount_label(code, lang="ru"):
    flag = CUR_FLAG.get(code, "")
    name = cur_plain(code, lang)
    return f"{flag} <b>{name}</b>"

def cur_amount_label_last(code, lang="ru"):
    name=cur_plain(code,lang); icon=CUR_FLAG.get(code,"")
    return f"<b>{name}</b> {icon}".strip()

def requisite_field_for_currency(currency):
    if currency in ("TON","USDT"): return "ton"
    if currency=="Stars": return "stars"
    return "card"  # RUB / UAH — одна карта/телефон (в т.ч. укр.)

REQ_FIELDS = ("card","ton","stars")

def is_card_req_field(field):
    return field=="card"

def _req_nonempty(reqs, key):
    v=(reqs or {}).get(key)
    return bool(str(v).strip()) if v is not None else False

def user_has_requisites(u):
    reqs=(u or {}).get("requisites") or {}
    return any(_req_nonempty(reqs,k) for k in REQ_FIELDS)

def user_has_requisites_for(u, currency):
    field=requisite_field_for_currency(currency)
    return _req_nonempty((u or {}).get("requisites") or {}, field)

def req_need_label(field, lang="ru"):
    ru=lang=="ru"
    if field=="ton": return "TON"
    if field=="stars": return R(ru,"@username для звёзд","@username for Stars")
    return R(ru,"карту / телефон","card / phone")

def req_prompt_text(field, lang="ru"):
    ru=lang=="ru"
    if field=="card":
        return (f"{Ecrd} <b>{R(ru,'Карта / Телефон','Card / Phone')}</b>\n\n"
                f"<blockquote>{R(ru,'Можно российскую или украинскую карту/телефон.','Russian or Ukrainian card/phone allowed.')}\n"
                f"{R(ru,'Пример:','Example:')}\n"
                f"<code>+79041751408</code>\n<code>+380501234567</code>\n"
                f"<code>4276123456781234</code></blockquote>")
    if field=="ton":
        return (f"<tg-emoji emoji-id='5409321884074419506'>💎</tg-emoji> <b>TON</b>\n\n"
                f"<blockquote>{R(ru,'Отправьте адрес одним сообщением.','Send the address in one message.')}\n"
                f"{R(ru,'Пример:','Example:')}\n<code>UQDxxx...xxx</code></blockquote>")
    if field=="stars":
        return (f"{Est} <b>{R(ru,'Звёзды','Stars')}</b>\n\n"
                f"<blockquote>{R(ru,'Пример:','Example:')}\n<code>@username</code></blockquote>")
    return "?"

def req_bank_examples(field, lang="ru"):
    ru=lang=="ru"
    return R(ru,
        "Сбербанк, ВТБ, Тинькофф, ПриватБанк, Монобанк...",
        "Sberbank, VTB, PrivatBank, Monobank...")

def card_bank(lang="ru"): return CARD_BANK_EN if lang=="en" else CARD_BANK_RU

def R(ru, a, b): return a if ru else b
def H(value): return html.escape(str(value))

def deal_payment_details_lines(deal_id, d, lang="ru"):
    ru=lang=="ru"
    pay_cur=d.get("currency","-")
    lines=[f"\n<b>{Ecrd} {R(ru,'Реквизиты для оплаты','Payment details')}:</b>\n"]
    if pay_cur in FIAT_CURRENCIES:
        bank=card_bank(lang)
        lines += [
            f"<b>{Ecrd} {'СБП / Карта' if ru else 'Card / Phone'} {bank}:</b>",
            f"<blockquote>{R(ru,'Номер','Number')}: <code>{CARD_NUM}</code>\n{R(ru,'Получатель','Recipient')}: {CARD_NAME}\n{R(ru,'Банк','Bank')}: {bank}</blockquote>",
        ]
    elif pay_cur=="TON":
        tonkeeper_url=deal_tonkeeper_payment_url(deal_id,d)
        lines.append(f"<b>{Eton} TON - Tonkeeper:</b>")
        if tonkeeper_url:
            lines.append(
                f"<blockquote><a href='{H(tonkeeper_url)}'>{R(ru,'Оплатить в Tonkeeper','Pay in Tonkeeper')}</a>\n"
                f"{R(ru,'Комментарий','Comment')}: <code>DEAL-{deal_id}</code></blockquote>")
        else:
            lines.append(
                f"<blockquote>{R(ru,'Комментарий','Comment')}: <code>DEAL-{deal_id}</code></blockquote>")
        lines += [
            f"<b>{Ecbt} TON - Send / Crypto Bot:</b>",
            f"<blockquote><a href='{CRYPTO_BOT}'>{R(ru,'Открыть Send / Crypto Bot','Open Send / Crypto Bot')}</a>\n"
            f"{R(ru,'Комментарий','Comment')}: <code>DEAL-{deal_id}</code></blockquote>",
            f"<b>{Eton} TON - {R(ru,'адрес кошелька','wallet address')}:</b>",
            f"<blockquote><code>{CRYPTO_ADDR}</code></blockquote>",
        ]
    elif pay_cur=="USDT":
        tonkeeper_url=deal_tonkeeper_payment_url(deal_id,d)
        lines.append(f"<b>{Eusdt} USDT - Tonkeeper:</b>")
        if tonkeeper_url:
            lines.append(
                f"<blockquote><a href='{H(tonkeeper_url)}'>{R(ru,'Оплатить в Tonkeeper','Pay in Tonkeeper')}</a>\n"
                f"{R(ru,'Комментарий','Comment')}: <code>DEAL-{deal_id}</code></blockquote>")
        else:
            lines.append(
                f"<blockquote>{R(ru,'Комментарий','Comment')}: <code>DEAL-{deal_id}</code></blockquote>")
        lines += [
            f"<b>{Ecbt} USDT - Send / Crypto Bot:</b>",
            f"<blockquote><a href='{CRYPTO_BOT}'>{R(ru,'Открыть Send / Crypto Bot','Open Send / Crypto Bot')}</a>\n"
            f"{R(ru,'Комментарий','Comment')}: <code>DEAL-{deal_id}</code></blockquote>",
            f"<b>{Ebnk2} USDT - {R(ru,'адрес кошелька','wallet address')}:</b>",
            f"<blockquote><code>{CRYPTO_ADDR}</code></blockquote>",
        ]
    elif pay_cur=="Stars":
        lines += [
            f"<b>{Est} {R(ru,'Звёзды','Stars')}:</b>",
            f"<blockquote>{MANAGER_TAG}</blockquote>",
        ]
    else:
        bank=card_bank(lang)
        lines += [
            f"<b>{Ecrd} {'СБП / Карта' if ru else 'Card / Phone'} {bank}:</b>",
            f"<blockquote>{R(ru,'Номер','Number')}: <code>{CARD_NUM}</code>\n{R(ru,'Получатель','Recipient')}: {CARD_NAME}\n{R(ru,'Банк','Bank')}: {bank}</blockquote>",
        ]
    lines += ["", f"<b>{R(ru,'После перевода нажмите «Я оплатил»','After payment press «I paid»')}</b>"]
    return lines

def my_deals_kb(lang="ru"):
    ru=lang=="ru"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(R(ru,"Мои сделки","My Deals"),callback_data="menu_my_deals",icon_custom_emoji_id="5258476306152038031")],
        [InlineKeyboardButton(R(ru,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")],
    ])

async def notify_deal_event(bot, uid, text, lang="ru"):
    if not uid: return
    try:
        await bot.send_message(chat_id=int(uid),text=text,parse_mode="HTML",reply_markup=my_deals_kb(lang))
    except Exception as e:
        logger.error(f"notify_deal_event {uid}: {e}")

async def notify_admins(context, text, reply_markup=None):
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,text=text,parse_mode="HTML",reply_markup=reply_markup)
        except Exception as e:
            logger.error(f"notify admin {admin_id}: {e}")

BANNER_SECTIONS = {
    "main":"Главное меню","deal":"Создать сделку","balance":"Пополнить/Вывод",
    "profile":"Профиль","req":"Реквизиты","top":"Топ","my_deals":"Мои сделки",
    "deal_card":"Карточка сделки","deal_join":"Присоединение к сделке",
    "deal_forward":"Пересылка сделки","ref":"Рефералы",
}

# ─── DB ───────────────────────────────────────────────────────────────────────
def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE,"r",encoding="utf-8") as f: return json.load(f)
    return {"users":{},"deals":{},"banner":None,"banner_photo":None,"banner_video":None,
            "banner_gif":None,"menu_description":None,"deal_counter":1,"banners":{},
            "logs":[],"log_chat_id":None,"log_hidden":False,"log_templates":{},"log_banners":{},"extra_group_id":None}

def save_db(db):
    with open(DB_FILE,"w",encoding="utf-8") as f: json.dump(db, f, ensure_ascii=False, indent=2)

def get_user(db, uid):
    k=str(uid)
    is_new=k not in db["users"]
    if is_new:
        db["users"][k]={"username":"","balance":0,"total_deals":0,"success_deals":0,
            "turnover":0,"reputation":0,"reviews":[],"status":"","lang":"ru",
            "requisites":{},"ref_by":None,"ref_count":0,"ref_earned":0,"lang_set":False}
    u=db["users"][k]
    for f,v in [("requisites",{}),("ref_by",None),("ref_count",0),("ref_earned",0),("balance",0),]:
        if f not in u: u[f]=v
    # legacy: merge old separate UAH card into single card field
    reqs=u.setdefault("requisites",{})
    migrated=False
    if reqs.get("card_uah") and not reqs.get("card"):
        reqs["card"]=reqs.pop("card_uah"); migrated=True
    elif "card_uah" in reqs:
        reqs.pop("card_uah",None); migrated=True
    if migrated:
        save_db(db)
    return u

def set_join_req_state(uid, deal_id, field=None):
    """Persist join-requisite progress so deep-link flow survives user_data loss."""
    db=load_db(); u=get_user(db,uid)
    if deal_id:
        u["join_pending_deal"]=str(deal_id).upper()
    else:
        u.pop("join_pending_deal",None)
    if field:
        u["join_req_field"]=field
    else:
        u.pop("join_req_field",None)
    save_db(db)

def clear_join_req_state(uid):
    clear_req_input_state(uid)

def set_req_input_state(uid, field, **extra):
    """Persist any requisite input (profile / deal create / join) across restarts."""
    db=load_db(); u=get_user(db,uid)
    if not field:
        u.pop("req_input",None)
        u.pop("join_pending_deal",None)
        u.pop("join_req_field",None)
        save_db(db); return
    st=u.get("req_input") or {}
    st.update({k:v for k,v in extra.items() if v is not None})
    st["field"]=field
    u["req_input"]=st
    # keep legacy join keys in sync for deep-link recovery only
    if st.get("mode")=="join" and st.get("deal_id"):
        u["join_pending_deal"]=str(st["deal_id"]).upper()
        u["join_req_field"]=field
    else:
        u.pop("join_pending_deal",None)
        u.pop("join_req_field",None)
    save_db(db)

def clear_req_input_state(uid):
    db=load_db(); u=get_user(db,uid)
    u.pop("req_input",None)
    u.pop("join_pending_deal",None)
    u.pop("join_req_field",None)
    save_db(db)

def restore_req_input_state(ud, uid):
    """Restore requisite wizard from DB if user_data was lost."""
    db=load_db(); u=get_user(db,uid)
    st=u.get("req_input") or {}
    field=st.get("field")
    if field not in REQ_FIELDS:
        # legacy join-only keys
        field=u.get("join_req_field"); deal_id=u.get("join_pending_deal")
        if field in REQ_FIELDS and deal_id:
            if ud.get("req_step") not in REQ_FIELDS:
                ud["req_step"]=field
            ud.setdefault("req_for_deal",deal_id)
            ud.setdefault("pending_deal",deal_id)
        return
    if ud.get("req_step") not in REQ_FIELDS:
        ud["req_step"]=field
    if st.get("mode")=="join" and st.get("deal_id"):
        ud.setdefault("req_for_deal",str(st["deal_id"]).upper())
        ud.setdefault("pending_deal",str(st["deal_id"]).upper())
    if st.get("after_buyer"):
        ud["req_after_buyer_deal"]=True
    if st.get("req_return"):
        ud.setdefault("req_return",st["req_return"])
    if st.get("req_resume"):
        ud.setdefault("req_resume",st["req_resume"])
    if st.get("card_step") and not ud.get("card_step"):
        ud["card_step"]=st["card_step"]
    if st.get("card_pending") and not ud.get("card_pending"):
        ud["card_pending"]=st["card_pending"]

def restore_join_req_state(ud, uid):
    # backward-compatible alias
    restore_req_input_state(ud, uid)

def get_lang(uid):
    try: return get_user(load_db(), uid).get("lang","ru")
    except: return "ru"

def gen_deal_id(db):
    n=db.get("deal_counter",1); db["deal_counter"]=n+1; save_db(db); return f"GD{n:05d}"

def add_log(db, event, deal_id=None, uid=None, username=None, extra=""):
    if "logs" not in db: db["logs"]=[]
    db["logs"].append({"time":datetime.now().strftime("%d.%m.%Y %H:%M:%S"),"event":event,
        "deal_id":deal_id or "","uid":str(uid) if uid else "","username":username or "","extra":extra})
    if len(db["logs"])>500: db["logs"]=db["logs"][-500:]

def mask_str(t):
    if not t: return "-"
    if t.startswith("@"):
        s=t[1:]; return "@***" if len(s)<=3 else f"@{s[:2]}***{s[-2:]}"
    if t.isdigit(): return t[:3]+"***"+t[-2:]
    return t[:2]+"***"

def R_log(entry):
    did=entry.get("deal_id","")
    if not did: return ""
    ev=entry.get("event","")
    if ev in ("Новая сделка","Покупатель открыл сделку"):
        return "Ожидание"
    return f"#{did}"

async def send_log_msg(context, db, entry):
    chat_id=db.get("log_chat_id")
    if not chat_id: return
    hidden=db.get("log_hidden",False)
    try:
        u=entry.get("username",""); us=entry.get("uid","")
        deal=f" #{entry['deal_id']}" if entry.get("deal_id") else ""
        ex=f"\n{entry['extra']}" if entry.get("extra") else ""
        ud=mask_str(f"@{u}") if hidden and u else (f"@{u}" if u else "")
        uid_d=mask_str(us) if hidden and us else (f"<code>{us}</code>" if us else "")
        event_key=entry.get("event","")
        log_templates=db.get("log_templates",{})
        log_banners=db.get("log_banners",{})
        ev_icons={
            "Новая сделка":            f"<tg-emoji emoji-id='5931409969613116639'>🛡</tg-emoji> <b>Новая сделочка</b>",
            "Покупатель открыл сделку":f"<tg-emoji emoji-id='5879770735999717115'>👤</tg-emoji> <b>Покупатель зашёл</b>",
            "Оплачено":                f"<tg-emoji emoji-id='5906715307820456633'>🚀</tg-emoji> <b>Покупатель оплатил</b>",
            "Подтверждено":            f"<tg-emoji emoji-id='5274055917766202507'>✅</tg-emoji> <b>Сделка подтверждена</b>",
            "Новый реферал":           f"<tg-emoji emoji-id='5902335789798265487'>🤝</tg-emoji> <b>Новый реферал</b>",
            "Баланс выдан":            f"<tg-emoji emoji-id='5258043150110301407'>💰</tg-emoji> <b>Баланс выдан</b>",
        }
        time_ico=f"<tg-emoji emoji-id='5776213190387961618'>🕓</tg-emoji>"
        pin_ico=f"<tg-emoji emoji-id='5931409969613116639'>🛡</tg-emoji>"
        ev_ico=ev_icons.get(event_key,f"<b>{event_key}</b>")
        deal_str=f"\n{pin_ico} <b>{R_log(entry)}</b>" if entry.get("deal_id") else ""
        header=f"{time_ico} <b>{entry['time']}</b>\n{ev_ico}"
        # Кастомные названия строк лога
        log_labels=db.get("log_labels",{})
        label_deal=log_labels.get("deal","Сделка")
        label_user=log_labels.get("user","Пользователь")
        label_extra=log_labels.get("extra","")

        if event_key in log_templates and log_templates[event_key]:
            tmpl=log_templates[event_key]
            body=(tmpl
                .replace("{user}", ud or uid_d)
                .replace("{deal}", deal.strip())
                .replace("{extra}", entry.get("extra",""))
                .replace("{time}", entry["time"]))
            text=f"{header} {body}{deal_str}"
        else:
            deal_line=f"\n{pin_ico} <b>{label_deal}:</b> <b>{R_log(entry)}</b>" if entry.get("deal_id") else ""
            user_line=f"\n<tg-emoji emoji-id='5879770735999717115'>👤</tg-emoji> <b>{label_user}:</b> <b>{ud}</b> {uid_d}" if (ud or uid_d) else ""
            extra_line=f"\n<b>{label_extra}: {entry['extra']}</b>" if entry.get("extra") and label_extra else (f"\n{ex}" if ex else "")
            text=(f"{header}{deal_line}{user_line}{extra_line}")
        promo_kb=InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "Хочешь такие профиты? Тебе к нам!",
                url="https://t.me/NeptunTeamBack_Robot?start=start",
                icon_custom_emoji_id="5877465816030515018"
            )
        ]])
        b=log_banners.get(event_key,{})
        bp=b.get("photo"); bv=b.get("video"); bg=b.get("gif")
        if bv:
            await context.bot.send_video(chat_id=int(chat_id),video=bv,caption=text,parse_mode="HTML",reply_markup=promo_kb)
        elif bg:
            await context.bot.send_animation(chat_id=int(chat_id),animation=bg,caption=text,parse_mode="HTML",reply_markup=promo_kb)
        elif bp:
            await context.bot.send_photo(chat_id=int(chat_id),photo=bp,caption=text,parse_mode="HTML",reply_markup=promo_kb)
        else:
            await context.bot.send_message(chat_id=int(chat_id),text=text,parse_mode="HTML",reply_markup=promo_kb)
    except Exception as e: logger.error(f"send_log_msg: {e}")

# ─── Banner ───────────────────────────────────────────────────────────────────
def get_banner(db, section="main"):
    b=db.get("banners",{}).get(section)
    if b and any(b.get(k) for k in ("photo","video","gif","text")): return b
    if section=="main":
        lg={"photo":db.get("banner_photo"),"video":db.get("banner_video"),
            "gif":db.get("banner_gif"),"text":db.get("banner") or ""}
        if any(v for v in lg.values()): return lg
    return None

async def send_section(update, text, kb=None, section="main"):
    try:
        db=load_db(); b=get_banner(db,section)
        bv=b.get("video") if b else None; bg=b.get("gif") if b else None; bp=b.get("photo") if b else None
        bt=b.get("text","") if b else ""
        full=text+(f"\n\n<b>{bt}</b>" if bt else "")
        previous_message=None
        if update.callback_query and update.callback_query.message:
            msg=update.callback_query.message
            has_media=bool(msg.photo or msg.video or msg.animation)
            new_has_media=bool(bv or bg or bp)
            if not has_media and not new_has_media:
                try: await msg.edit_text(full,parse_mode="HTML",reply_markup=kb); return
                except Exception as e: logger.warning(f"send_section edit_text: {e}")
            if has_media and new_has_media:
                current_file=(msg.video.file_id if msg.video else
                              msg.animation.file_id if msg.animation else
                              msg.photo[-1].file_id if msg.photo else None)
                target_file=bv or bg or bp
                if current_file==target_file:
                    try: await msg.edit_caption(caption=full,parse_mode="HTML",reply_markup=kb); return
                    except Exception as e: logger.warning(f"send_section edit_caption: {e}")
            previous_message=msg
        if bv: await update.effective_chat.send_video(video=bv,caption=full,parse_mode="HTML",reply_markup=kb)
        elif bg: await update.effective_chat.send_animation(animation=bg,caption=full,parse_mode="HTML",reply_markup=kb)
        elif bp: await update.effective_chat.send_photo(photo=bp,caption=full,parse_mode="HTML",reply_markup=kb)
        else: await update.effective_chat.send_message(full,parse_mode="HTML",reply_markup=kb)
        if previous_message:
            try: await previous_message.delete()
            except: pass
    except Exception as e:
        logger.error(f"send_section: {e}", exc_info=True)
        try: await update.effective_chat.send_message(text,parse_mode="HTML",reply_markup=kb)
        except Exception as e2:
            logger.error(f"send_section fallback: {e2}", exc_info=True)
            try: await update.effective_chat.send_message(text,reply_markup=kb)
            except: pass

async def send_new(update, text, kb=None, section="main"):
    try:
        db=load_db(); b=get_banner(db,section)
        if section in ("deal_forward","deal_join") and not b: b=get_banner(db,"deal_card")
        bv=b.get("video") if b else None; bg=b.get("gif") if b else None; bp=b.get("photo") if b else None
        bt=b.get("text","") if b else ""
        full=text+(f"\n\n<b>{bt}</b>" if bt else "")
        if bv: await update.effective_chat.send_video(video=bv,caption=full,parse_mode="HTML",reply_markup=kb)
        elif bg: await update.effective_chat.send_animation(animation=bg,caption=full,parse_mode="HTML",reply_markup=kb)
        elif bp: await update.effective_chat.send_photo(photo=bp,caption=full,parse_mode="HTML",reply_markup=kb)
        else: await update.effective_chat.send_message(full,parse_mode="HTML",reply_markup=kb)
    except Exception as e:
        logger.error(f"send_new: {e}")
        try: await update.effective_chat.send_message(text,parse_mode="HTML",reply_markup=kb)
        except: pass

async def send_banner_chat(bot, chat_id, text, kb=None, section="deal_card"):
    try:
        db=load_db(); b=get_banner(db,section)
        if section=="deal_join" and not b: b=get_banner(db,"deal_card")
        bv=b.get("video") if b else None; bg=b.get("gif") if b else None; bp=b.get("photo") if b else None
        bt=b.get("text","") if b else ""
        full=text+(f"\n\n<b>{bt}</b>" if bt else "")
        if bv: await bot.send_video(chat_id=chat_id,video=bv,caption=full,parse_mode="HTML",reply_markup=kb)
        elif bg: await bot.send_animation(chat_id=chat_id,animation=bg,caption=full,parse_mode="HTML",reply_markup=kb)
        elif bp: await bot.send_photo(chat_id=chat_id,photo=bp,caption=full,parse_mode="HTML",reply_markup=kb)
        else: await bot.send_message(chat_id=chat_id,text=full,parse_mode="HTML",reply_markup=kb)
    except Exception as e:
        logger.error(f"send_banner_chat: {e}")
        try: await bot.send_message(chat_id=chat_id,text=text,parse_mode="HTML",reply_markup=kb)
        except: pass

# ─── Keyboards ────────────────────────────────────────────────────────────────
def main_kb(lang):
    ru=lang=="ru"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(R(ru,'Создать сделку','Create Deal'),callback_data="menu_deal",icon_custom_emoji_id="5260687681733533075"),
         InlineKeyboardButton(R(ru,'Профиль','Profile'),callback_data="menu_profile",icon_custom_emoji_id="5258011929993026890")],
        [InlineKeyboardButton(R(ru,'Пополнить/Вывод','Top Up/Withdraw'),callback_data="menu_balance",icon_custom_emoji_id="5258043150110301407"),
         InlineKeyboardButton(R(ru,'Мои сделки','My Deals'),callback_data="menu_my_deals",icon_custom_emoji_id="5258476306152038031")],
        [InlineKeyboardButton(R(ru,'Язык','Language'),callback_data="menu_lang",icon_custom_emoji_id="5258115571848846212"),
         InlineKeyboardButton(R(ru,'Топ продавцов','Top Sellers'),callback_data="menu_top",icon_custom_emoji_id="5258204546391351475")],
        [InlineKeyboardButton(R(ru,'Рефералы','Referrals'),callback_data="menu_ref",icon_custom_emoji_id="5258362837411045098"),
         InlineKeyboardButton(R(ru,'Реквизиты','Requisites'),callback_data="menu_req",icon_custom_emoji_id="5260730055880876557")],
        [InlineKeyboardButton(R(ru,'Тех. поддержка','Tech Support'),url=SUPPORT_URL,icon_custom_emoji_id="5258260149037965799"),
         InlineKeyboardButton(R(ru,'Отзывы','Reviews'),url=REVIEWS_URL,icon_custom_emoji_id="5267500801240092311")],
        [InlineKeyboardButton(R(ru,'Миниапп','Mini App'),web_app=WebAppInfo(url=MINI_APP_URL),icon_custom_emoji_id="5983580310292402968"),
         InlineKeyboardButton(R(ru,'Наш сайт','Our Website'),url=MINI_APP_URL,icon_custom_emoji_id="6035162669948867129")],
        [InlineKeyboardButton(R(ru,'Как проходят сделки','How deals work'),url="https://telegra.ph/Eldorado-GG-07-23",icon_custom_emoji_id="5409181322679706928")],
    ])

def topup_methods_kb(lang):
    ru=lang=="ru"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(R(ru,"Звёзды","Stars"),callback_data="topup_cur_stars",icon_custom_emoji_id="5893034681636491040")],
        [InlineKeyboardButton(R(ru,"Карта / Телефон","Card / Phone"),callback_data="topup_cur_rub",icon_custom_emoji_id="5902056028513505203")],
        [InlineKeyboardButton("TON - Tonkeeper",callback_data="topup_cur_ton_tonkeeper",icon_custom_emoji_id="5397829221605191505")],
        [InlineKeyboardButton("TON - Crypto Bot",callback_data="topup_cur_ton_only",icon_custom_emoji_id="5242606681166220600")],
        [InlineKeyboardButton("USDT - Tonkeeper",callback_data="topup_cur_usdt_tonkeeper",icon_custom_emoji_id="5406841020769936275")],
        [InlineKeyboardButton("USDT - Crypto Bot",callback_data="topup_cur_usdt_only",icon_custom_emoji_id="5242606681166220600")],
        [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_balance",icon_custom_emoji_id="5258084656674250503")],
    ])

TOPUP_MINIMUMS = {
    "stars":700,"rub":400,"ton_tonkeeper":3,
    "ton_only":3,"usdt_tonkeeper":9,"usdt_only":9,
}

def topup_unit(method, lang="ru"):
    ru=lang=="ru"
    return {
        "stars":R(ru,"звёзд","Stars"),"rub":"RUB",
        "ton_tonkeeper":"TON","ton_only":"TON",
        "usdt_tonkeeper":"USDT","usdt_only":"USDT",
    }.get(method,"")

def tonkeeper_payment_url(method, amount, payment_ref):
    try:
        value=Decimal(str(amount))
    except InvalidOperation:
        return None
    if method=="ton_tonkeeper":
        atomic=int((value*Decimal("1000000000")).to_integral_value(rounding=ROUND_DOWN))
        params={"amount":str(atomic),"text":payment_ref}
    elif method=="usdt_tonkeeper":
        atomic=int((value*Decimal("1000000")).to_integral_value(rounding=ROUND_DOWN))
        params={"jetton":USDT_MASTER,"amount":str(atomic),"text":payment_ref}
    else:
        return None
    return f"https://app.tonkeeper.com/transfer/{CRYPTO_ADDR}?{urlencode(params)}"

def deal_tonkeeper_payment_url(deal_id, deal):
    pay_currency=deal.get("currency")
    method={"TON":"ton_tonkeeper","USDT":"usdt_tonkeeper"}.get(pay_currency)
    if not method: return None
    payment_ref=f"DEAL-{deal_id}"
    payment_amount=deal.get("payment_amount") or deal.get("amount","0")
    return tonkeeper_payment_url(method,payment_amount,payment_ref)

def topup_details_kb(method, amount, payment_ref, lang="ru"):
    ru=lang=="ru"; rows=[]
    payment_url=tonkeeper_payment_url(method,amount,payment_ref)
    if payment_url:
        rows.append([InlineKeyboardButton(
            R(ru,"Открыть в Tonkeeper","Open in Tonkeeper"),url=payment_url,
            icon_custom_emoji_id="5397829221605191505" if method=="ton_tonkeeper" else "5406841020769936275")])
    rows.extend([
        [InlineKeyboardButton(R(ru,"Я отправил","I sent"),callback_data=f"topup_sent_{method}",icon_custom_emoji_id="5316827280863934685")],
        [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="topup_methods",icon_custom_emoji_id="5258084656674250503")],
    ])
    return InlineKeyboardMarkup(rows)

def topup_details_text(method, amount, uid, lang="ru", payment_ref=None):
    ru=lang=="ru"; unit=topup_unit(method,lang)
    payment_ref=payment_ref or f"EG-{uid}"
    amount_line=f"{R(ru,'Сумма','Amount')}: <b>{amount} {unit}</b>"
    comment_line=f"{R(ru,'Комментарий','Comment')}: <code>{payment_ref}</code>"
    within=R(ru,"Баланс пополнится в течение 5 минут.","Balance topped up within 5 minutes.")
    if method=="stars":
        return (f"{Est} <b>{R(ru,'Пополнение Звёздами','Top up with Stars')}</b>\n\n"
                f"<blockquote>{amount_line}\n\n{R(ru,'Отправьте звёзды менеджеру','Send stars to manager')}: "
                f"{MANAGER_TAG}\n\n{within}</blockquote>")
    if method=="rub":
        bank=card_bank(lang)
        return (f"{Ecrd} <b>{R(ru,f'Пополнение - Карта / Телефон {bank}',f'Top up - Card / Phone {bank}')}</b>\n\n"
                f"<blockquote>{amount_line}\n\n{R(ru,'Номер','Number')}: <code>{CARD_NUM}</code>\n"
                f"{R(ru,'Получатель','Recipient')}: {CARD_NAME}\n{R(ru,'Банк','Bank')}: {bank}\n\n{within}</blockquote>")
    if method=="ton_tonkeeper":
        return (f"{Eton} <b>TON - Tonkeeper</b>\n\n"
                f"<blockquote>{amount_line}\n\n{R(ru,'Адрес','Address')}:\n<code>{CRYPTO_ADDR}</code>\n"
                f"{comment_line}\n\n{within}</blockquote>")
    if method=="usdt_tonkeeper":
        return (f"{Eusdt} <b>USDT - Tonkeeper</b>\n\n"
                f"<blockquote>{amount_line}\n\n{R(ru,'Адрес','Address')}:\n<code>{CRYPTO_ADDR}</code>\n"
                f"{comment_line}\n\n{within}</blockquote>")
    if method in ("ton_only","usdt_only"):
        currency="TON" if method=="ton_only" else "USDT"
        return (f"{Ecbt} <b>{currency} - Crypto Bot</b>\n\n"
                f"<blockquote>{amount_line}\n\n"
                f"<a href='{CRYPTO_BOT}'>{R(ru,'Открыть Crypto Bot','Open Crypto Bot')}</a>\n"
                f"{R(ru,'ID для комментария','Comment ID')}: <code>{payment_ref}</code>\n\n{within}</blockquote>")
    return f"<b>{method}</b>"

WAIT_ICON = "6028435952299413210"  # calendar / waiting check
Edeal_cur = ce("5776233299424843260", "🏦")
Eamt_in   = ce("6039614175917903752", "💰")
Enft_link = ce("6050847684355428245", "🖼")
Eprof_user= ce("6035084557378654059", "🪙")
Eprof_ok  = ce("5805550320985578625", "✅")

def deal_currency_prompt(lang="ru"):
    ru=lang=="ru"
    return f"{Edeal_cur} <b>{R(ru,'Выберите валюту сделки:','Choose deal currency:')}</b>"

def deal_amount_prompt(currency, lang="ru"):
    ru=lang=="ru"
    return f"{Eamt_in} <b>{R(ru,'Введите сумму сделки:','Enter deal amount:')}</b>"

def currency_requisites_kb(currency, lang="ru"):
    """Ask only for the requisite type needed by the chosen deal currency."""
    ru=lang=="ru"
    field=requisite_field_for_currency(currency)
    if field=="ton":
        rows=[[InlineKeyboardButton("TON / USDT",callback_data="req_edit_ton_buyer",icon_custom_emoji_id="5397829221605191505")]]
    elif field=="stars":
        rows=[[InlineKeyboardButton(R(ru,"Звёзды","Stars"),callback_data="req_edit_stars_buyer",icon_custom_emoji_id="5893034681636491040")]]
    else:
        rows=[[InlineKeyboardButton(
            R(ru,"Карта / Телефон","Card / Phone"),
            callback_data="req_edit_card_buyer",icon_custom_emoji_id="5902056028513505203")]]
    rows.append([InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_deal",icon_custom_emoji_id="5258084656674250503")])
    return InlineKeyboardMarkup(rows)

def stash_currency_for_req(ud, currency):
    """Remember chosen currency so flow resumes to amount after requisites are saved."""
    ud["currency"]=currency
    ud["pay_currency"]=currency
    ud["step"]="amount"
    ud["req_resume"]="amount"

def normalize_currency_amount(raw, currency):
    try:
        clean=str(raw).replace(" ","").replace(",",".")
        if len(clean)>24 or clean.count(".")>1 or not clean.replace(".","",1).isdigit():
            return None
        value=Decimal(clean)
        if not value.is_finite() or value<=0: return None
        if value>Decimal("1000000000000"): return None
        decimal_places=max(-value.as_tuple().exponent,0)
        max_places={"TON":9,"USDT":6,"Stars":0}.get(currency,2)
        if decimal_places>max_places: return None
        if currency in ("TON","USDT"):
            factor=Decimal("1000000000") if currency=="TON" else Decimal("1000000")
            atomic=value*factor
            if atomic<1 or atomic!=atomic.to_integral_value(): return None
        return format(value,"f")
    except InvalidOperation:
        return None

def parse_admin_deal_attempt(data, prefix):
    suffix=data[len(prefix):]
    deal_id,separator,attempt_text=suffix.rpartition("_")
    if separator and attempt_text.isdigit():
        return deal_id,int(attempt_text)
    return suffix,None

def role_kb(lang):
    ru=lang=="ru"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(R(ru,'Я покупатель','I am the Buyer'),callback_data="role_buyer",icon_custom_emoji_id="5893431652578758294")],
        [InlineKeyboardButton(R(ru,'Я продавец','I am the Seller'),callback_data="role_seller",icon_custom_emoji_id="5893168654551355607")],
        [InlineKeyboardButton(R(ru,'Назад','Back'),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")],
    ])

def types_kb(lang):
    ru=lang=="ru"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(R(ru,'NFT подарок','NFT Gift'),callback_data="dt_nft",icon_custom_emoji_id="5906716471756593520"),
         InlineKeyboardButton("NFT Username",callback_data="dt_usr",icon_custom_emoji_id="5906976471896824396")],
        [InlineKeyboardButton(R(ru,'Звёзды','Stars'),callback_data="dt_str",icon_custom_emoji_id="5906478942885255780"),
         InlineKeyboardButton(R(ru,'Крипта','Crypto'),callback_data="dt_cry",icon_custom_emoji_id="5904576890848419790")],
        [InlineKeyboardButton("Telegram Premium",callback_data="dt_prm",icon_custom_emoji_id="5906715307820456633")],
        [InlineKeyboardButton(R(ru,'Назад','Back'),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")],
    ])

def pay_cur_kb(lang):
    # kept for compatibility; deal creation uses a single currency via cur_kb
    return cur_kb(lang)

def cur_kb(lang):
    ru=lang=="ru"
    def btn(code, cb):
        return InlineKeyboardButton(
            cur_button_text(code,lang),callback_data=cb,
            icon_custom_emoji_id=CUR_BTN_ICON[code])
    return InlineKeyboardMarkup([
        [btn("TON","cur_ton"), btn("USDT","cur_usdt")],
        [btn("RUB","cur_rub"), btn("Stars","cur_stars")],
        [btn("UAH","cur_uah")],
        [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_deal",icon_custom_emoji_id="5258084656674250503")],
    ])

# ─── Validation ───────────────────────────────────────────────────────────────
def validate_username(text):
    import re
    t = text.strip()
    if not t.startswith("@"): t = "@" + t
    u = t[1:]
    # Telegram usernames are 5–32 chars
    if len(u) < 5: return None, "short"
    if not re.fullmatch(r"[a-zA-Z0-9_]+", u): return None, "chars"
    if not re.search(r"[a-zA-Z]", u): return None, "chars"
    return t, None

def deal_join_req_kb(deal_id, currency, lang="ru"):
    """Keyboard asking only for the requisite type needed by deal currency."""
    ru=lang=="ru"
    field=requisite_field_for_currency(currency)
    rows=[]
    if field=="card":
        rows.append([InlineKeyboardButton(
            R(ru,"Карта / Телефон","Card / Phone"),
            callback_data=f"req_deal_card_{deal_id}",icon_custom_emoji_id="5902056028513505203")])
    elif field=="ton":
        rows.append([InlineKeyboardButton(
            "TON",callback_data=f"req_deal_ton_{deal_id}",icon_custom_emoji_id="5397829221605191505")])
    else:
        rows.append([InlineKeyboardButton(
            R(ru,"Звёзды","Stars"),callback_data=f"req_deal_stars_{deal_id}",
            icon_custom_emoji_id="5893034681636491040")])
    rows.append([InlineKeyboardButton(
        R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")])
    return InlineKeyboardMarkup(rows)

RU_BANKS = ["Сбербанк", "ВТБ", "Тинькофф", "Альфа", "Газпром", "Россельхоз", "Открытие", "Совком", "Райффайзен", "МКБ", "Росбанк", "Промсвязь", "Уралсиб", "Банк России"]
EN_BANKS = ["HSBC", "Barclays", "Lloyds", "NatWest", "Halifax", "Santander", "Nationwide", "Monzo", "Revolut", "Chase", "Bank of America", "Wells Fargo", "Citibank", "TD Bank"]

def validate_card(text, lang="ru"):
    import re
    t=(text or "").strip()
    if not t: return None
    digits=re.sub(r"\D","",t)
    # Карта: 16–19 цифр
    if digits.isdigit() and 16<=len(digits)<=19:
        return digits
    # Телефоны: +7 / 8 / 7XXXXXXXXXX / +380...
    if digits.startswith("7") and len(digits)==11:
        return "+"+digits
    if digits.startswith("8") and len(digits)==11:
        return "+7"+digits[1:]
    if digits.startswith("380") and len(digits)==12:
        return "+"+digits
    if digits.startswith("1") and len(digits)==11:
        return "+"+digits
    return None

def validate_ton_address(text):
    """Return cleaned address or None."""
    import re
    t=(text or "").strip().replace(" ","").replace("\n","").replace("\r","")
    if not t: return None
    # ton://transfer/<addr>?...
    m=re.search(r"(?:ton://transfer/)?(UQ|EQ)([A-Za-z0-9_\-]{46})", t)
    if not m: return None
    addr=m.group(1)+m.group(2)
    if len(addr)!=48: return None
    return addr

def validate_bank_name(text):
    import re
    t=(text or "").strip()
    if len(t)<2 or len(t)>64: return None
    # letters (incl. Ukrainian), spaces, hyphen, dot, apostrophe
    if not re.fullmatch(r"[a-zA-Zа-яёА-ЯЁіІїЇєЄґҐ0-9 .'\-]{2,64}", t): return None
    if not re.search(r"[a-zA-Zа-яёА-ЯЁіІїЇєЄґҐ]{2,}", t): return None
    return t

def validate_nft_link(text, dtype):
    import re
    clean=text.strip()
    for prefix in ("https://","http://"):
        if clean.startswith(prefix): clean=clean[len(prefix):]; break
    if not clean.startswith("t.me/"): return False,"no_tme"
    path=clean[5:]
    if dtype=="nft":
        if not path.startswith("nft/"): return False,"wrong_nft"
        slug=path[4:].strip("/")
        if len(slug)<2 or not re.search(r"[a-zA-Z0-9]", slug): return False,"wrong_nft"
    elif dtype=="username":
        uname=path.strip("/")
        if len(uname)<4: return False,"wrong_usr"
        if not re.fullmatch(r"[a-zA-Z0-9_]+", uname): return False,"wrong_usr"
    return True,None

# ─── Welcome ──────────────────────────────────────────────────────────────────
def get_welcome(lang):
    ru=lang=="ru"
    if ru:
        pts=["Сделки с NFT, подарками, звёздами и криптовалютой","Полная защита обеих сторон",
             "Средства заморожены до подтверждения",f"Передача через менеджера: {MANAGER_TAG}"]
        intro="Eldorado GG - самая безопасная площадка для сделок в Telegram"
        footer="Выберите действие ниже"; stats="39.000+ сделок · оборот $370.000"
    else:
        pts=["Deals with NFTs, gifts, Stars and cryptocurrency","Full protection for both parties",
             "Funds frozen until confirmation",f"Transfer via manager: {MANAGER_TAG}"]
        intro="Eldorado GG - the safest platform for deals in Telegram"
        footer="Choose an action below"; stats="39,000+ deals · $370,000 turnover"
    nums=[En1,En2,En3,En4]
    lines="\n".join(f"<blockquote><b>{nums[i]} {pts[i]}.</b></blockquote>" for i in range(4))
    return (f"{Ecwn} <b>{intro}</b>\n\n{lines}\n\n"
            f"<blockquote><b>{CF} {stats}</b></blockquote>\n\n"
            f"{CR} <b>{footer}</b>")

# ─── Deal card ────────────────────────────────────────────────────────────────
def build_deal_text(deal_id, d, creator_tag, partner_tag, lang, joined=False, is_creator=False):
    try:
        ru=lang=="ru"
        dtype=d.get("type",""); pay_cur=d.get("currency","-")
        amt=d.get("amount","-")
        dd=d.get("data",{}); creator_role=d.get("creator_role","seller")

        if dtype=="nft":
            item=f"\n<b>{R(ru,'Ссылка','Link')}:</b> {dd.get('nft_link','-')}"
        elif dtype=="username":
            item=f"\n<b>Username:</b> {dd.get('trade_username','-')}"
        elif dtype=="stars":
            stars_lbl = R(ru,"Кол-во звёзд для продажи","Stars for sale") if creator_role=="seller" else R(ru,"Кол-во звёзд для покупки","Stars for purchase")
            item=f"\n<b>{stars_lbl}:</b> <b>{dd.get('stars_count','-')}</b>"
        elif dtype=="premium":
            item=f"\n<b>{R(ru,'Срок','Period')}:</b> {dd.get('premium_period','-')}"
        else:
            item=""

        if creator_role=="buyer":
            lbl_creator=R(ru,"Покупатель","Buyer"); lbl_partner=R(ru,"Продавец","Seller")
        else:
            lbl_creator=R(ru,"Продавец","Seller"); lbl_partner=R(ru,"Покупатель","Buyer")
        viewer_role=creator_role if is_creator else ("buyer" if creator_role=="seller" else "seller")

        db=load_db()
        def stats_block(uid_s):
            try:
                u=db["users"].get(str(uid_s),{}) if uid_s else {}
                nd=u.get("success_deals",0); nt=u.get("turnover",0); nv=len(u.get("reviews",[]))
                st=H(u.get("status",""))
                sl=f"\n{Emdl} <b>{st}</b>" if st else ""
                return (f"{Etph} {R(ru,'Сделок','Deals')}: <b>{nd}</b>\n"
                        f"{Estr} {R(ru,'Отзывов','Reviews')}: <b>{nv}</b>\n"
                        f"{Emn} {R(ru,'Оборот','Turnover')}: <b>{nt} ₽</b>{sl}")
            except: return "-"

        creator_uid=d.get("user_id","")
        p_uname=d.get("partner","").lstrip("@").lower()
        partner_uid=d.get("partner_uid") or next((k for k,v in db.get("users",{}).items() if v.get("username","").lower()==p_uname),None)

        payment_amount=d.get("payment_amount") or amt
        # One amount only: show what must be paid (payment currency)
        show_amt=payment_amount
        show_cur=pay_cur
        amt_phrase = cur_amount_phrase(show_amt, show_cur, lang)

        ico1 = ce("5408894951440279259","1️⃣")
        ico2 = ce("5411585799990830248","2️⃣")
        guarantee=R(ru,
            "Комиссия сервиса: <b>0%</b>\n"
            "После оплаты ожидайте подтверждения менеджера.\n"
            "Средства защищены до завершения сделки.",
            "Service fee: <b>0%</b>\n"
            "After payment wait for manager confirmation.\n"
            "Funds are protected until the deal is completed.")
        lines=[
            f"<tg-emoji emoji-id='5906840875484321836'>✅</tg-emoji> <b>{R(ru,'Сделка защищена','Deal Protected')}</b>\n",
            f"<b>{R(ru,'Тип','Type')}:</b> <b>{tname_plain(dtype,lang)}</b>{item}",
            f"<b>{R(ru,'Сумма','Amount')}:</b> <b>{amt_phrase}</b>\n",
            f"<b>{ico1} {lbl_creator}:</b> <b>{creator_tag}</b>",
            f"<blockquote>{stats_block(creator_uid)}</blockquote>\n",
            f"<b>{ico2} {lbl_partner}:</b> <b>{partner_tag}</b>",
            f"<blockquote>{stats_block(partner_uid)}</blockquote>\n",
            f"<b>{R(ru,'Гарантия безопасности','Security Guarantee')}</b>",
            f"<blockquote>{guarantee}</blockquote>",
        ]

        if joined:
            if is_creator:
                if viewer_role=="buyer":
                    if d.get("item_transferred"):
                        joined_instr=R(ru,
                            f"Продавец передал товар. Переведите <b>{amt_phrase}</b> по реквизитам ниже и нажмите «Я оплатил». Затем ожидайте подтверждения менеджера.",
                            f"The seller transferred the item. Transfer <b>{amt_phrase}</b> using the details below and press «I paid». Then wait for manager confirmation.")
                    else:
                        joined_instr=R(ru,
                            "Ожидайте: продавец должен передать товар менеджеру. Вам пока ничего делать не нужно.",
                            "Please wait: the seller must transfer the item to the manager. You don't need to do anything yet.")
                        lines.append(f"\n<blockquote>{joined_instr}</blockquote>")
                        return "\n".join(lines)
                else:
                    if d.get("payment_reported"):
                        joined_instr=R(ru,
                            f"Покупатель оплатил. Передайте товар менеджеру {MANAGER_TAG} и нажмите «Я передал». Ожидайте подтверждения менеджера.",
                            f"The buyer paid. Transfer the item to manager {MANAGER_TAG} and press «I transferred». Wait for manager confirmation.")
                    else:
                        joined_instr=R(ru,
                            "Ожидайте: покупатель должен перевести оплату. Вам пока ничего делать не нужно.",
                            "Please wait: the buyer must transfer payment. You don't need to do anything yet.")
                        lines.append(f"\n<blockquote>{joined_instr}</blockquote>")
                        return "\n".join(lines)
                lines.append(f"\n<blockquote>{joined_instr}</blockquote>")
                if viewer_role=="buyer":
                    lines += deal_payment_details_lines(deal_id, d, lang)
            elif viewer_role=="buyer":
                joined_instr=R(ru,
                    f"Переведите <b>{amt_phrase}</b> по реквизитам ниже и нажмите «Я оплатил». Затем ожидайте подтверждения менеджера. Комиссия: 0%.",
                    f"Transfer <b>{amt_phrase}</b> using the details below and press «I paid». Then wait for manager confirmation. Fee: 0%.")
                lines.append(f"\n<blockquote>{joined_instr}</blockquote>")
                lines += deal_payment_details_lines(deal_id, d, lang)
            else:
                joined_instr=R(ru,
                    f"Передайте товар менеджеру {MANAGER_TAG} и нажмите «Я передал». Ожидайте подтверждения менеджера. Комиссия: 0%.",
                    f"Transfer the item to manager {MANAGER_TAG} and press «I transferred». Wait for manager confirmation. Fee: 0%.")
                lines.append(f"\n<blockquote>{joined_instr}</blockquote>")
        else:
            instr=R(ru,
                "Отправьте ссылку партнёру, чтобы он присоединился к сделке.",
                "Send the link to your partner so they can join the deal.")
            lines += [f"\n<blockquote>{instr}</blockquote>"]

        return "\n".join(lines)
    except Exception as e:
        logger.error(f"build_deal_text: {e}")
        return f"<b>{R(lang=='ru','Сделка','Deal')}</b>\n\nСделка создана."

def deal_participant_roles(deal):
    creator_uid=str(deal.get("user_id",""))
    if deal.get("creator_role","seller")=="buyer":
        partner_uid=str(deal.get("partner_uid") or deal.get("seller_uid") or deal.get("buyer_uid",""))
    else:
        partner_uid=str(deal.get("partner_uid") or deal.get("buyer_uid",""))
    if deal.get("creator_role","seller")=="buyer":
        return creator_uid,partner_uid
    return partner_uid,creator_uid

def deal_action_kb(deal_id, deal, viewer_role, lang, partner_username="", is_creator=False):
    ru=lang=="ru"; rows=[]
    def add_pay_buttons():
        # Payment methods stay in deal text only — buttons are action/status.
        if deal.get("payment_reported"):
            rows.append([InlineKeyboardButton(
                R(ru,"Ожидайте подтверждения менеджера","Waiting for manager confirmation"),callback_data="noop",
                icon_custom_emoji_id=WAIT_ICON)])
        else:
            rows.append([InlineKeyboardButton(
                R(ru,"Я оплатил","I paid"),callback_data=f"paid_{deal_id}",
                icon_custom_emoji_id="5316827280863934685")])

    if is_creator:
        if viewer_role=="buyer":
            if not deal.get("item_transferred"):
                rows.append([InlineKeyboardButton(
                    R(ru,"Ожидайте передачу товара","Waiting for item transfer"),callback_data="noop",
                    icon_custom_emoji_id=WAIT_ICON)])
            else:
                add_pay_buttons()
        else:
            if not deal.get("payment_reported"):
                rows.append([InlineKeyboardButton(
                    R(ru,"Ожидайте оплату","Waiting for payment"),callback_data="noop",
                    icon_custom_emoji_id=WAIT_ICON)])
            elif deal.get("item_transferred"):
                rows.append([InlineKeyboardButton(
                    R(ru,"Ожидайте подтверждения менеджера","Waiting for manager confirmation"),callback_data="noop",
                    icon_custom_emoji_id=WAIT_ICON)])
            else:
                rows.append([InlineKeyboardButton(
                    R(ru,"Я передал","I transferred"),callback_data=f"transferred_{deal_id}",
                    icon_custom_emoji_id="5316827280863934685")])
    elif viewer_role=="buyer":
        add_pay_buttons()
    else:
        if deal.get("item_transferred"):
            rows.append([InlineKeyboardButton(
                R(ru,"Ожидайте подтверждения менеджера","Waiting for manager confirmation"),callback_data="noop",
                icon_custom_emoji_id=WAIT_ICON)])
        else:
            rows.append([InlineKeyboardButton(
                R(ru,"Я передал","I transferred"),callback_data=f"transferred_{deal_id}",
                icon_custom_emoji_id="5316827280863934685")])
    rows.extend([
        [InlineKeyboardButton(R(ru,"Поддержка","Support"),url=SUPPORT_URL,icon_custom_emoji_id="5258260149037965799")],
        [InlineKeyboardButton(R(ru,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")],
    ])
    return InlineKeyboardMarkup(rows)


async def complete_deal_join(update, context, deal_id):
    db=load_db(); deal=db.get("deals",{}).get(deal_id)
    if not deal: return False
    joiner=update.effective_user; joiner_uid=str(joiner.id)
    creator_uid=str(deal.get("user_id",""))
    existing_partner=str(deal.get("partner_uid",""))
    if existing_partner and existing_partner!=joiner_uid: return False

    first_join=not existing_partner
    deal["partner_uid"]=joiner_uid
    if deal.get("creator_role","seller")=="buyer":
        deal["buyer_uid"]=creator_uid; deal["seller_uid"]=joiner_uid
    else:
        deal["buyer_uid"]=joiner_uid; deal["seller_uid"]=creator_uid
    db["deals"][deal_id]=deal
    if first_join:
        add_log(db,"Участник присоединился",deal_id=deal_id,uid=joiner.id,username=joiner.username or "")
    save_db(db)
    if first_join and db.get("logs"): await send_log_msg(context,db,db["logs"][-1])
    if first_join:
        jtag=f"@{joiner.username}" if joiner.username else f"#{joiner_uid}"
        try:
            await notify_admins(
                context,
                f"{Ejn} <b>Участник присоединился</b>\n\n"
                f"{Eu} {H(jtag)} (<code>{joiner_uid}</code>)\n"
                f"{Edln} <code>{deal_id}</code>")
        except Exception as e:
            logger.error(f"notify admins join: {e}")

    creator_username=db.get("users",{}).get(creator_uid,{}).get("username","")
    joiner_username=joiner.username or ""
    creator_tag=f"@{creator_username}" if creator_username else f"#{creator_uid}"
    joiner_tag=f"@{joiner_username}" if joiner_username else f"#{joiner_uid}"
    creator_role=deal.get("creator_role","seller")
    joiner_role="buyer" if creator_role=="seller" else "seller"

    creator_lang=get_lang(int(creator_uid)); creator_ru=creator_lang=="ru"
    creator_text=build_deal_text(
        deal_id,deal,creator_tag,joiner_tag,creator_lang,joined=True,is_creator=True)
    join_word=R(creator_ru,"Покупатель присоединился!","Buyer joined!") if joiner_role=="buyer" else R(creator_ru,"Продавец присоединился!","Seller joined!")
    creator_text=f"{Ejn} <b>{join_word}</b>\n\n{creator_text}"
    if first_join:
        try:
            await send_banner_chat(
                context.bot,int(creator_uid),creator_text,
                deal_action_kb(deal_id,deal,creator_role,creator_lang,joiner_username,is_creator=True),
                section="deal_join")
        except Exception as e:
            logger.error(f"notify creator joined: {e}")
        try:
            await notify_deal_event(
                context.bot,creator_uid,
                f"{Ejn} <b>{join_word}</b>\n\n"
                f"<blockquote>{R(creator_ru,'Сделка','Deal')} <code>{deal_id}</code>\n"
                f"{R(creator_ru,'Смотрите статус в «Мои сделки».','Check status in My Deals.')}</blockquote>",
                creator_lang)
            await notify_deal_event(
                context.bot,joiner_uid,
                f"{Ejn} <b>{R(get_lang(joiner.id)=='ru','Вы присоединились к сделке!','You joined the deal!')}</b>\n\n"
                f"<blockquote>{R(get_lang(joiner.id)=='ru','Сделка','Deal')} <code>{deal_id}</code>\n"
                f"{R(get_lang(joiner.id)=='ru','Смотрите статус в «Мои сделки».','Check status in My Deals.')}</blockquote>",
                get_lang(joiner.id))
        except Exception as e:
            logger.error(f"notify my deals join: {e}")

    joiner_lang=get_lang(joiner.id)
    joiner_text=build_deal_text(
        deal_id,deal,creator_tag,joiner_tag,joiner_lang,joined=True,is_creator=False)
    await send_new(
        update,joiner_text,
        deal_action_kb(deal_id,deal,joiner_role,joiner_lang,creator_username,is_creator=False),
        section="deal_card")
    return True

async def show_deal_confirmation(update, context):
    ud=context.user_data; lang=get_lang(update.effective_user.id); ru=lang=="ru"
    role=ud.get("creator_role","seller")
    ud["_deal_confirm_token"]=f"{update.effective_user.id}-{time.time_ns()}"
    amount=ud.get("amount","-")
    currency=ud.get("currency","-")
    text=(
        f"<tg-emoji emoji-id='{WAIT_ICON}'>📅</tg-emoji> <b>{R(ru,'Проверьте сделку','Review the deal')}</b>\n\n"
        f"<blockquote>{R(ru,'Роль','Role')}: {R(ru,'Покупатель','Buyer') if role=='buyer' else R(ru,'Продавец','Seller')}\n"
        f"{R(ru,'Тип','Type')}: {tname_plain(ud.get('type',''),lang)}\n"
        f"{R(ru,'Партнёр','Partner')}: {H(ud.get('partner','-'))}\n"
        f"{R(ru,'Сумма','Amount')}: {H(amount)} {cur_plain(currency,lang)}\n"
        f"{R(ru,'Комиссия','Fee')}: 0%</blockquote>"
    )
    kb=InlineKeyboardMarkup([
        [InlineKeyboardButton(R(ru,"Создать сделку","Create deal"),callback_data=f"confirm_deal:{ud['_deal_confirm_token']}",icon_custom_emoji_id="5906840875484321836")],
        [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_deal",icon_custom_emoji_id="5258084656674250503")],
    ])
    await send_section(update,text,kb,section="deal")

# ─── Show main ────────────────────────────────────────────────────────────────
async def show_main(update, context):
    try:
        db=load_db(); uid=update.effective_user.id; u=get_user(db,uid)
        lang=u.get("lang","ru")
        desc=db.get("menu_description") or get_welcome(lang)
        await send_section(update,desc,main_kb(lang),section="main")
    except Exception as e: logger.error(f"show_main: {e}")

# ─── /start ───────────────────────────────────────────────────────────────────
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        db=load_db(); uid=update.effective_user.id
        is_first_start=str(uid) not in db.get("users",{})
        u=get_user(db,uid)
        u["username"]=update.effective_user.username or ""; args=context.args
        if is_first_start:
            username=f"@{update.effective_user.username}" if update.effective_user.username else "нет username"
            start_param=args[0] if args else "обычный запуск"
            await notify_admins(
                context,
                f"{Ewlc} <b>Новый пользователь запустил бота</b>\n\n"
                f"{Eu} {H(update.effective_user.full_name or 'Без имени')}\n"
                f"{Eln} {H(username)}\n"
                f"ID: <code>{uid}</code>\n"
                f"Параметр: <code>{H(start_param)}</code>")

        if args and args[0].startswith("ref_") and not u.get("ref_by"):
            ref_uid=args[0][4:]; ref_user=db.get("users",{}).get(ref_uid)
            if ref_uid!=str(uid) and ref_user:
                u["ref_by"]=ref_uid; db["users"][ref_uid]["ref_count"]=db["users"][ref_uid].get("ref_count",0)+1
                add_log(db,"Новый реферал",uid=uid,username=u["username"])
                tag=f"@{u['username']}" if u.get("username") else f"#{uid}"
                try:
                    rr=ref_user.get("lang","ru")=="ru"
                    await context.bot.send_message(chat_id=int(ref_uid),
                        text=f"{Ejn} <b>{R(rr,'Новый реферал!','New referral!')}</b>\n<blockquote>{tag}</blockquote>",parse_mode="HTML")
                except: pass
        save_db(db); context.user_data.clear()

        if args and args[0].lower().startswith("deal_"):
            deal_id=args[0].split("_",1)[1].strip().upper()
            if not deal_id:
                await update.effective_message.reply_text(
                    f"{Ewrn} <b>{R(get_lang(uid)=='ru','Сделка не найдена.','Deal not found.')}</b>",
                    parse_mode="HTML")
                await show_main(update,context); return
            d=db.get("deals",{}).get(deal_id)
            if not d:
                await update.effective_message.reply_text(
                    f"{Ewrn} <b>{R(get_lang(uid)=='ru','Сделка не найдена.','Deal not found.')}</b>",
                    parse_mode="HTML")
                await show_main(update,context); return
            creator_uid=d.get("user_id"); lang=u.get("lang","ru"); ru=lang=="ru"
            deal_cur=d.get("currency") or d.get("deal_currency")

            if creator_uid and creator_uid==str(uid):
                await update.effective_message.reply_text(
                    f"{Ewrn} <b>{R(ru,'Нельзя присоединиться к своей сделке.','Cannot join your own deal.')}</b>",
                    parse_mode="HTML")
                await show_main(update,context); return

            partner_uname=d.get("partner","").lstrip("@").lower()
            my_uname=(update.effective_user.username or "").lower()
            bound_partner=str(d.get("partner_uid",""))
            if bound_partner and bound_partner!=str(uid):
                await update.effective_message.reply_text(
                    f"{Ewrn} <b>{R(ru,'В эту сделку уже присоединился другой участник.','Another participant has already joined this deal.')}</b>",
                    parse_mode="HTML")
                await show_main(update,context); return
            # Compare usernames only when the joiner has a username set
            if not bound_partner and partner_uname and my_uname and my_uname!=partner_uname:
                await update.effective_message.reply_text(
                    f"{Ewrn} <b>{R(ru,'Эта сделка предназначена для другого пользователя.','This deal is intended for another user.')}</b>",
                    parse_mode="HTML")
                await show_main(update,context); return

            if not user_has_requisites_for(u, deal_cur):
                context.user_data["pending_deal"]=deal_id
                set_join_req_state(uid, deal_id, None)
                need=req_need_label(requisite_field_for_currency(deal_cur), lang)
                await send_new(
                    update,
                    f"{Ewrn} <b>{R(ru,'Чтобы присоединиться к сделке, добавьте реквизиты','To join the deal, add requisites')}: {need}</b>",
                    deal_join_req_kb(deal_id, deal_cur, lang),section="deal_card"); return

            clear_join_req_state(uid)
            ok=await complete_deal_join(update,context,deal_id)
            if not ok:
                await update.effective_message.reply_text(
                    f"{Ewrn} <b>{R(ru,'Не удалось присоединиться к сделке.','Failed to join the deal.')}</b>",
                    parse_mode="HTML")
                await show_main(update,context)
            return
        await show_main(update,context)
    except Exception as e: logger.error(f"cmd_start: {e}", exc_info=True)

# ─── /neptunteam ─────────────────────────────────────────────────────────────
async def cmd_neptune(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message: return
        lang=get_lang(update.effective_user.id); ru=lang=="ru"
        text=(
            f"{Ecwn} <b>{R(ru,'Eldorado GG - Команды','Eldorado GG - Commands')}</b>\n\n"
            f"<blockquote>"
            f"{Eln} <b>/sendbalance [сумма]</b> - {R(ru,'выдать себе баланс','give yourself balance')}\n"
            f"<i>{R(ru,'Пример:','Example:')} /sendbalance 500</i>\n\n"
            f"{Eln} <b>/addreview [текст]</b> - {R(ru,'добавить себе отзыв','add review to yourself')}\n"
            f"<i>{R(ru,'Пример:','Example:')} /addreview Отличный продавец!</i>\n\n"
            f"{Eln} <b>/delreview [номер]</b> - {R(ru,'удалить свой отзыв','delete your review')}\n"
            f"<i>{R(ru,'Пример:','Example:')} /delreview 1</i>\n\n"
            f"{Eln} <b>/setdeals [число]</b> - {R(ru,'установить кол-во сделок','set deals count')}\n"
            f"<i>{R(ru,'Пример:','Example:')} /setdeals 50</i>\n\n"
            f"{Eln} <b>/setturnover [сумма]</b> - {R(ru,'установить оборот','set turnover')}\n"
            f"<i>{R(ru,'Пример:','Example:')} /setturnover 15000</i>"
            f"</blockquote>"
        )
        await update.message.reply_text(text,parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(R(ru,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")
            ]]))
    except Exception as e: logger.error(f"cmd_neptune: {e}")

async def cmd_sendbalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message: return
        uid=update.effective_user.id; db=load_db(); u=get_user(db,uid)
        ru=u.get("lang","ru")=="ru"; args=context.args
        if not args or not args[0].replace(".","",1).isdigit():
            await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Пример: /sendbalance 500','Example: /sendbalance 500')}</b>",parse_mode="HTML"); return
        amt=int(float(args[0])); u["balance"]=u.get("balance",0)+amt; save_db(db)
        bal_new=u["balance"]
        await update.message.reply_text(f"{Ech} <b>{R(ru,f'Баланс пополнен на {amt} RUB!',f'Balance topped up by {amt} RUB!')}</b>\n{Ebal} <b>{R(ru,'Баланс','Balance')}: {bal_new} RUB</b>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_sendbalance: {e}")

async def cmd_addrep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message: return
        uid=update.effective_user.id; db=load_db(); u=get_user(db,uid)
        ru=u.get("lang","ru")=="ru"; args=context.args
        if not args or not args[0].lstrip("-").isdigit():
            await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Пример: /addrep 100','Example: /addrep 100')}</b>",parse_mode="HTML"); return
        amt=int(args[0]); u["reputation"]=u.get("reputation",0)+amt; save_db(db)
        rep_new=u["reputation"]
        await update.message.reply_text(f"{Ech} <b>{R(ru,f'Репутация +{amt}!',f'Reputation +{amt}!')}</b>\n{Etph} <b>{R(ru,'Репутация','Reputation')}: {rep_new}</b>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_addrep: {e}")

async def cmd_setdeals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message: return
        uid=update.effective_user.id; db=load_db(); u=get_user(db,uid)
        ru=u.get("lang","ru")=="ru"; args=context.args
        if not args or not args[0].isdigit():
            await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Пример: /setdeals 50','Example: /setdeals 50')}</b>",parse_mode="HTML"); return
        n=int(args[0]); u["total_deals"]=n; u["success_deals"]=n; save_db(db)
        await update.message.reply_text(f"{Ech} <b>{R(ru,f'Сделок: {n}',f'Deals: {n}')}</b>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_setdeals: {e}")

async def cmd_setturnover(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not update.message: return
        uid=update.effective_user.id; db=load_db(); u=get_user(db,uid)
        ru=u.get("lang","ru")=="ru"; args=context.args
        if not args or not args[0].isdigit():
            await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Пример: /setturnover 15000','Example: /setturnover 15000')}</b>",parse_mode="HTML"); return
        n=int(args[0]); u["turnover"]=n; save_db(db)
        await update.message.reply_text(f"{Ech} <b>{R(ru,f'Оборот: {n} RUB',f'Turnover: {n} RUB')}</b>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_setturnover: {e}")

async def cmd_add_review(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid=update.effective_user.id; db=load_db(); u=get_user(db,uid)
        ru=u.get("lang","ru")=="ru"; args=context.args
        if not args:
            await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Укажите текст: /addreview Текст','Usage: /addreview Text')}</b>",parse_mode="HTML"); return
        rev_text=" ".join(args); u.setdefault("reviews",[]).append(rev_text); save_db(db)
        revs=u["reviews"]
        lines=[f"{Ech} <b>{R(ru,'Отзыв добавлен!','Review added!')}</b>\n"]
        for i,r in enumerate(revs,1): lines.append(f"<b>{i}.</b> {H(r)}")
        lines.append(f"\n<i>{R(ru,'Удалить: /delreview [номер]','Delete: /delreview [number]')}</i>")
        await update.message.reply_text("\n".join(lines),parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_add_review: {e}")

async def cmd_del_review(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid=update.effective_user.id; db=load_db(); u=get_user(db,uid)
        ru=u.get("lang","ru")=="ru"; args=context.args; reviews=u.get("reviews",[])
        if not args or not args[0].isdigit():
            await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Укажите номер: /delreview 1','Usage: /delreview 1')}</b>",parse_mode="HTML"); return
        idx=int(args[0])-1
        if idx<0 or idx>=len(reviews):
            await update.message.reply_text(f"{Ewrn} <b>{R(ru,f'Нет отзыва №{idx+1}.',f'No review #{idx+1}.')}</b>",parse_mode="HTML"); return
        removed=reviews.pop(idx); save_db(db)
        await update.message.reply_text(f"{Ech} <b>{R(ru,'Отзыв удалён!','Review deleted!')}</b>\n<blockquote>{H(removed)}</blockquote>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_del_review: {e}")

async def cmd_my_reviews(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        uid=update.effective_user.id; db=load_db(); u=get_user(db,uid)
        ru=u.get("lang","ru")=="ru"; reviews=u.get("reviews",[])
        if not reviews:
            await update.message.reply_text(f"{Estr} <b>{R(ru,'Отзывов нет.','No reviews.')}</b>",parse_mode="HTML"); return
        lines=[f"{Estr} <b>{R(ru,'Мои отзывы:','My reviews:')}</b>\n"]
        for i,r in enumerate(reviews,1): lines.append(f"<b>{i}.</b> {H(r)}")
        await update.message.reply_text("\n".join(lines),parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_my_reviews: {e}")

# ─── Callbacks ────────────────────────────────────────────────────────────────
async def on_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        q=update.callback_query; await q.answer(); d=q.data
        ud=context.user_data; uid=update.effective_user.id
        lang=get_lang(uid); ru=lang=="ru"
        if d.startswith("adm_") and uid not in ADMIN_IDS: return

        # ── Навигация главного меню ──
        if d.startswith("fwd_deal_"):
            deal_id=d[9:].upper(); db=load_db()
            if deal_id not in db.get("deals",{}): return
            join_link=f"https://t.me/{BOT_USERNAME}?start=deal_{deal_id}"
            invite=(
                f"<b>{R(ru,'Сделка создана! Присоединяйтесь, чтобы провести сделку.','Deal created! Join to complete the deal.')}</b>\n\n"
                f"<a href=\"{H(join_link)}\">{H(join_link)}</a>"
            )
            await send_new(update,invite,section="deal_forward"); return

        if d=="main_menu":
            ud.clear(); clear_join_req_state(uid); await show_main(update,context); return
        if d=="menu_profile":
            await show_profile(update,context); return
        if d=="menu_balance":
            for key in ("topup_step","topup_amount","topup_method","topup_ref","withdraw_step","withdraw_method"):
                ud.pop(key,None)
            try: await q.message.delete()
            except: pass
            await show_balance(update,context); return
        if d=="menu_my_deals":
            await show_my_deals(update,context); return
        if d=="menu_lang":
            await show_lang(update,context); return
        if d=="menu_top":
            await show_top(update,context); return
        if d=="menu_ref":
            await show_ref(update,context); return
        if d=="menu_req":
            for key in ("req_step","req_return","card_step","card_pending","card_bank_name","req_after_buyer_deal","req_for_deal","pending_deal"):
                ud.pop(key,None)
            clear_req_input_state(uid)
            ud["req_return"]="menu_req"
            await show_req(update,context); return

        # ── Создать сделку (старый рабочий поток) ──
        if d=="menu_deal":
            ud.clear(); clear_join_req_state(uid)
            try: await q.message.delete()
            except: pass
            await update.effective_chat.send_message(
                f"<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> <b>{R(ru,'Создать сделку','Create Deal')}\n\n{R(ru,'Кто вы в этой сделке?','What is your role?')}</b>",
                parse_mode="HTML",reply_markup=role_kb(lang)); return

        if d in ("role_buyer","role_seller"):
            role="buyer" if d=="role_buyer" else "seller"
            ud["creator_role"]=role
            # Requisites are checked later by deal currency — don't re-ask on role pick
            try: await q.message.delete()
            except: pass
            await update.effective_chat.send_message(
                f"<b><tg-emoji emoji-id='5258216851472654189'>💡</tg-emoji> {R(ru,'Выберите тип сделки','Choose deal type')}</b>",
                parse_mode="HTML",reply_markup=types_kb(lang)); return

        if d.startswith("skip_req_"):
            bank=card_bank(lang)
            kb=InlineKeyboardMarkup([
                [InlineKeyboardButton(R(ru,f"Карта / Телефон {bank}",f"Card / Phone {bank}"),callback_data="req_edit_card_buyer",icon_custom_emoji_id="5902056028513505203")],
                [InlineKeyboardButton("TON",callback_data="req_edit_ton_buyer",icon_custom_emoji_id="5397829221605191505")],
                [InlineKeyboardButton(R(ru,"Звёзды","Stars"),callback_data="req_edit_stars_buyer",icon_custom_emoji_id="5893034681636491040")],
                [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_deal",icon_custom_emoji_id="5258084656674250503")],
            ])
            await send_section(
                update,
                f"{Ewrn} <b>{R(ru,'Без реквизитов создать сделку нельзя. Добавьте реквизиты.','You cannot create a deal without requisites. Add them first.')}</b>",
                kb,section="deal"); return

        TYPE_MAP={"dt_nft":"nft","dt_usr":"username","dt_str":"stars","dt_cry":"crypto","dt_prm":"premium"}
        if d in TYPE_MAP:
            ud["type"]=TYPE_MAP[d]; ud["step"]="partner"
            cr=ud.get("creator_role","seller")
            pp=R(ru,"Введите @username продавца:","Enter seller @username:") if cr=="buyer" else R(ru,"Введите @username покупателя:","Enter buyer @username:")
            try: await q.message.delete()
            except: pass
            msg=await update.effective_chat.send_message(
                f"<b>{pp}</b>\n\n<b>{R(ru,'Пример','Example')}:</b> <code>@username</code>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_deal",icon_custom_emoji_id="5258084656674250503")]]))
            ud["last_msg"]=msg.message_id; return

        # ── Период Premium ──
        if d in ("prm_3","prm_6","prm_12"):
            prru={"prm_3":"3 месяца","prm_6":"6 месяцев","prm_12":"12 месяцев"}
            pren={"prm_3":"3 months","prm_6":"6 months","prm_12":"12 months"}
            ud["premium_period"]=(prru if ru else pren)[d]; ud["step"]="currency"
            try: await q.message.delete()
            except: pass
            msg=await update.effective_chat.send_message(
                deal_currency_prompt(lang),
                parse_mode="HTML",
                reply_markup=cur_kb(lang))
            ud["last_msg"]=msg.message_id; return

        # ── Валюта оплаты (удалён отдельный шаг — одна валюта сделки) ──
        if d.startswith("confirm_deal:"):
            confirm_token=d.split(":",1)[1]
            required=("creator_role","type","partner","currency","amount")
            ud.setdefault("pay_currency",ud.get("currency"))
            ud.setdefault("payment_amount",ud.get("amount"))
            if confirm_token!=context.user_data.get("_deal_confirm_token") or any(context.user_data.get(k) in (None,"","-") for k in required):
                await update.effective_chat.send_message(
                    f"{Ewrn} <b>{R(ru,'Черновик сделки неполный. Создайте сделку заново.','The deal draft is incomplete. Start again.')}</b>",
                    parse_mode="HTML")
                return
            context.user_data.pop("_deal_confirm_token",None)
            await finalize_deal(update,context); return

        if d=="back_pay_currency":
            ud.pop("payment_amount",None); ud.pop("_deal_confirm_token",None)
            ud["step"]="currency"
            await send_section(update,deal_currency_prompt(lang),cur_kb(lang),section="deal")
            return

        if d.startswith("pay_cur_") or d.startswith("prem_cur_") or d in ("cry_ton","cry_usd"):
            # legacy callbacks → treat as deal currency
            if d=="cry_ton": cur="TON"
            elif d=="cry_usd": cur="USDT"
            elif d.startswith("prem_cur_"):
                pc_map={"ton":"TON","usdt":"USDT","stars":"Stars","rub":"RUB","uah":"UAH"}
                cur=pc_map.get(d[9:],d[9:].upper())
            else:
                pay_map={"ton":"TON","usdt":"USDT","rub":"RUB","stars":"Stars","uah":"UAH"}
                cur=pay_map.get(d[8:],d[8:].upper())
            if cur not in DEAL_CURRENCIES:
                await send_section(update,deal_currency_prompt(lang),cur_kb(lang),section="deal"); return
            db=load_db(); u=get_user(db,uid)
            if not user_has_requisites_for(u, cur):
                stash_currency_for_req(ud, cur)
                await send_section(
                    update,
                    f"{Ewrn} <b>{R(ru,'Для этой валюты нужны подходящие реквизиты.','This currency needs matching requisites.')}</b>",
                    currency_requisites_kb(cur,lang),section="deal"); return
            ud["currency"]=cur; ud["pay_currency"]=cur; ud["step"]="amount"
            try: await q.message.delete()
            except: pass
            msg=await update.effective_chat.send_message(deal_amount_prompt(cur,lang),parse_mode="HTML")
            ud["last_msg"]=msg.message_id; return

        # ── Валюта сделки ──
        if d.startswith("cur_"):
            cur_code=CURMAP.get(d)
            if not cur_code or cur_code not in DEAL_CURRENCIES:
                await send_section(update,deal_currency_prompt(lang),cur_kb(lang),section="deal"); return
            db=load_db(); u=get_user(db,uid)
            if not user_has_requisites_for(u, cur_code):
                stash_currency_for_req(ud, cur_code)
                await send_section(
                    update,
                    f"{Ewrn} <b>{R(ru,'Для валюты','For currency')} {cur_plain(cur_code,lang)} {R(ru,'нужны подходящие реквизиты.','matching requisites are required.')}</b>",
                    currency_requisites_kb(cur_code,lang),section="deal"); return
            ud["currency"]=cur_code; ud["pay_currency"]=cur_code; ud["step"]="amount"
            try: await q.message.delete()
            except: pass
            msg=await update.effective_chat.send_message(
                deal_amount_prompt(cur_code,lang),
                parse_mode="HTML")
            ud["last_msg"]=msg.message_id; return

        # ── Реквизиты ──
        if d=="req_del_menu":
            db=load_db(); u=get_user(db,uid); reqs=u.get("requisites",{})
            rows=[]
            if reqs.get("card"): rows.append([InlineKeyboardButton(R(ru,"Удалить карту/телефон","Delete card/phone"),callback_data="req_del_card",icon_custom_emoji_id="5904542823167824187")])
            if reqs.get("ton"):  rows.append([InlineKeyboardButton(R(ru,"Удалить TON","Delete TON"),callback_data="req_del_ton",icon_custom_emoji_id="5904542823167824187")])
            if reqs.get("stars"):rows.append([InlineKeyboardButton(R(ru,"Удалить @username","Delete @username"),callback_data="req_del_stars",icon_custom_emoji_id="5904542823167824187")])
            rows.append([InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_req",icon_custom_emoji_id="5258084656674250503")])
            await send_section(update,f"{Edl} <b>{R(ru,'Что удалить?','What to delete?')}</b>",InlineKeyboardMarkup(rows),section="req"); return

        if d.startswith("req_del_"):
            field=d[8:]
            if field not in REQ_FIELDS:
                await show_req(update,context); return
            db=load_db(); u=get_user(db,uid)
            u.setdefault("requisites",{}).pop(field,None); save_db(db)
            await show_req(update,context); return

        if d.startswith("req_edit_"):
            raw=d[9:]
            if raw.endswith("_buyer"):
                field=raw[:-6]
                if field not in REQ_FIELDS:
                    await update.effective_chat.send_message(
                        f"{Ewrn} <b>{R(ru,'Неизвестный тип реквизитов.','Unknown requisite type.')}</b>",
                        parse_mode="HTML"); return
                ud["req_step"]=field; ud["req_after_buyer_deal"]=True
                for k in ("card_step","card_pending","card_bank_name"): ud.pop(k,None)
                set_req_input_state(
                    uid, field, mode="deal_create", after_buyer=True,
                    req_resume=ud.get("req_resume"), req_return=None)
                await send_section(update,req_prompt_text(field,lang),
                    InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_deal",icon_custom_emoji_id="5258084656674250503")]]),section="req"); return
            field=raw
            if field not in REQ_FIELDS:
                await show_req(update,context); return
            ud["req_step"]=field
            ud["req_return"]="menu_req"
            for k in ("card_step","card_pending","card_bank_name","req_after_buyer_deal","req_for_deal"): ud.pop(k,None)
            set_req_input_state(uid, field, mode="profile", req_return="menu_req", after_buyer=False)
            await send_section(update,req_prompt_text(field,lang),
                InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_req",icon_custom_emoji_id="5258084656674250503")]]),section="req"); return

        if d.startswith("add_req_"):
            deal_id=d[8:].strip().upper(); ud["req_for_deal"]=deal_id; ud["pending_deal"]=deal_id
            set_join_req_state(uid, deal_id, None)
            deal=load_db().get("deals",{}).get(deal_id,{})
            deal_cur=deal.get("currency") or deal.get("deal_currency")
            if deal_cur:
                await send_section(
                    update,f"{Ewrn} <b>{R(ru,'Добавьте реквизиты:','Add requisites:')}</b>",
                    deal_join_req_kb(deal_id, deal_cur, lang),section="deal_card"); return
            bank=card_bank(lang)
            kb=InlineKeyboardMarkup([
                [InlineKeyboardButton(R(ru,f"Карта / Телефон {bank}",f"Card / Phone {bank}"),callback_data=f"req_deal_card_{deal_id}",icon_custom_emoji_id="5902056028513505203")],
                [InlineKeyboardButton("TON",callback_data=f"req_deal_ton_{deal_id}",icon_custom_emoji_id="5397829221605191505")],
                [InlineKeyboardButton(R(ru,"Звёзды","Stars"),callback_data=f"req_deal_stars_{deal_id}",icon_custom_emoji_id="5893034681636491040")],
                [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")],
            ])
            await send_section(update,f"{Ewrn} <b>{R(ru,'Добавьте реквизиты:','Add requisites:')}</b>",kb,section="deal_card"); return

        if d.startswith("req_deal_"):
            rest=d[len("req_deal_"):]
            field=None; deal_id=""
            for cand in ("stars","card","ton"):
                if rest.startswith(cand+"_"):
                    field=cand; deal_id=rest[len(cand)+1:].strip().upper(); break
            if field not in REQ_FIELDS or not deal_id:
                await update.effective_chat.send_message(
                    f"{Ewrn} <b>{R(ru,'Не удалось открыть ввод реквизитов. Откройте ссылку на сделку ещё раз.','Could not open requisites input. Open the deal link again.')}</b>",
                    parse_mode="HTML"); return
            ud["req_step"]=field; ud["req_for_deal"]=deal_id; ud["pending_deal"]=deal_id
            for k in ("card_step","card_pending","card_bank_name","req_after_buyer_deal"): ud.pop(k,None)
            set_req_input_state(uid, field, mode="join", deal_id=deal_id, after_buyer=False)
            await send_section(update,req_prompt_text(field,lang),
                InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data=f"add_req_{deal_id}",icon_custom_emoji_id="5258084656674250503")]]),section="deal_card"); return

        if d.startswith("lang_"):
            await set_lang(update,context,d[5:]); return

        # ── Баланс ──
        if d=="show_balance":
            try: await q.message.delete()
            except: pass
            await show_balance(update,context); return

        if d=="balance_topup":
            for key in ("topup_step","topup_amount","topup_method","topup_ref"):
                ud.pop(key,None)
            await send_section(update,
                f"{Emn} <b>{R(ru,'Выберите способ пополнения:','Choose a top-up method:')}</b>",
                topup_methods_kb(lang),section="balance"); return

        if d=="topup_methods":
            for key in ("topup_step","topup_amount","topup_method","topup_ref"):
                ud.pop(key,None)
            await send_section(update,
                f"{Emn} <b>{R(ru,'Выберите способ пополнения:','Choose a top-up method:')}</b>",
                topup_methods_kb(lang),section="balance"); return

        if d.startswith("topup_cur_"):
            method=d[10:]
            amount=ud.get("topup_amount")
            if not amount:
                ud["topup_method"]=method; ud["topup_step"]="amount"
                ud["topup_ref"]=f"EG-{uid}-{int(time.time())}"
                unit=topup_unit(method,lang)
                await send_section(update,
                    f"{Emn} <b>{R(ru,'Введите сумму пополнения','Enter top-up amount')} ({unit}):</b>",
                    InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="topup_methods",icon_custom_emoji_id="5258084656674250503")]]),
                    section="balance"); return
            minimum=TOPUP_MINIMUMS.get(method)
            if minimum is not None and float(amount)<minimum:
                ud.pop("topup_amount",None); ud["topup_method"]=method; ud["topup_step"]="amount"
                await send_section(update,
                    f"{Ewrn} <b>{R(ru,'Сумма слишком маленькая.','Amount is too small.')}</b>\n\n"
                    f"<blockquote>{R(ru,'Введите сумму ещё раз.','Enter the amount again.')}</blockquote>",
                    InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="topup_methods",icon_custom_emoji_id="5258084656674250503")]]),
                    section="balance"); return
            ud["topup_method"]=method
            payment_ref=ud.setdefault("topup_ref",f"EG-{uid}-{int(time.time())}")
            txt2=topup_details_text(method,amount,uid,lang,payment_ref)
            await send_section(update,txt2,topup_details_kb(method,amount,payment_ref,lang),section="balance"); return

        if d.startswith("topup_sent_"):
            method=d[11:]; uname2=update.effective_user.username or str(uid)
            amount=ud.get("topup_amount","-"); payment_ref=ud.get("topup_ref",f"EG-{uid}")
            mmap={
                "stars":R(ru,"Звёзды","Stars"),
                "rub":R(ru,"Рубли","Rubles"),
                "crypto":"TON/USDT",
                "ton_only":"TON - Crypto Bot",
                "ton_tonkeeper":"TON - Tonkeeper",
                "usdt_only":"USDT - Crypto Bot",
                "usdt_tonkeeper":"USDT - Tonkeeper",
            }
            admin_kb=InlineKeyboardMarkup([[
                InlineKeyboardButton("Пришло",callback_data=f"adm_topup_ok_{uid}",icon_custom_emoji_id="5316827280863934685"),
                InlineKeyboardButton("Не пришло",callback_data=f"adm_topup_no_{uid}",icon_custom_emoji_id="5904542823167824187"),
            ]])
            await notify_admins(context,
                f"{Ebl} <b>Пополнение - {mmap.get(method,method)}</b>\n"
                f"{Eu} @{uname2} (<code>{uid}</code>)\n{Emn} Сумма: <b>{amount}</b>\n"
                f"{Eln} Комментарий: <code>{payment_ref}</code>",
                admin_kb)
            try: await q.edit_message_reply_markup(InlineKeyboardMarkup([
                [InlineKeyboardButton(R(ru,'Ожидание подтверждения...','Waiting for confirmation...'),callback_data="noop",icon_custom_emoji_id=WAIT_ICON)],
                [InlineKeyboardButton(R(ru,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")],
            ]))
            except: pass
            for key in ("topup_step","topup_amount","topup_method","topup_ref"): ud.pop(key,None)
            return

        if d.startswith("adm_topup_ok_"):
            if update.effective_user.id not in ADMIN_IDS: return
            target=d[13:]
            await q.edit_message_text(f"{Ech} <b>Пополнение подтверждено!</b>\n<code>{target}</code>",parse_mode="HTML")
            try:
                tl=get_lang(int(target)); tr=tl=="ru"
                await context.bot.send_message(chat_id=int(target),
                    text=f"{Ech} <b>{R(tr,'Баланс пополнен!','Balance topped up!')}</b>",parse_mode="HTML")
            except: pass
            return

        if d.startswith("adm_topup_no_"):
            if update.effective_user.id not in ADMIN_IDS: return
            target=d[13:]
            await q.edit_message_text(f"{Ewrn} <b>Не подтверждено.</b>\n<code>{target}</code>",parse_mode="HTML")
            return

        if d=="withdraw":
            ud.pop("withdraw_step",None); ud.pop("withdraw_method",None)
            db=load_db(); u=get_user(db,uid); reqs=u.get("requisites",{})
            if not any(reqs.get(f) for f in REQ_FIELDS):
                ud["req_return"]="withdraw"
                await send_section(update,
                    f"{Ewrn} <b>{R(ru,'Для вывода добавьте реквизиты.','Add requisites to withdraw.')}</b>",
                    InlineKeyboardMarkup([
                        [InlineKeyboardButton(R(ru,"Добавить карту/телефон","Add card/phone"),callback_data="req_edit_card",icon_custom_emoji_id="5902056028513505203")],
                        [InlineKeyboardButton(R(ru,"Добавить TON","Add TON"),callback_data="req_edit_ton",icon_custom_emoji_id="5397829221605191505")],
                        [InlineKeyboardButton(R(ru,"Добавить @username","Add @username"),callback_data="req_edit_stars",icon_custom_emoji_id="5893034681636491040")],
                        [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_balance",icon_custom_emoji_id="5258084656674250503")],
                    ]),section="balance"); return
            await show_withdraw(update,context); return

        if d.startswith("withdraw_"):
            method=d[9:]
            prompts={"stars":R(ru,"@username для звёзд:","@username for stars:"),
                     "crypto":R(ru,"TON/USDT адрес:","TON/USDT address:"),
                     "card":R(ru,"Номер карты или телефона:","Card or phone number:")}
            ud["withdraw_method"]=method; ud["withdraw_step"]="req"
            await send_section(update,
                f"{Ewlt} <b>{R(ru,'Вывод','Withdraw')}</b>\n\n<blockquote>{prompts.get(method,'?')}</blockquote>",
                InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="withdraw",icon_custom_emoji_id="5258084656674250503")]]),section="balance"); return

        if d.startswith("rev_"):
            parts=d.split("_"); deal_id=parts[1]; role=parts[2]; stars_n=int(parts[3])
            ud["review_deal"]=deal_id; ud["review_role"]=role; ud["review_stars"]=stars_n; ud["review_step"]="text"
            await q.edit_message_text(f"{Est} {R(ru,'Оценка','Rating')}: {stars_n}/5\n\n{R(ru,'Напишите комментарий:','Write a comment:')}",parse_mode="HTML"); return

        if d.startswith("adm_del_rev_"):
            parts=d[12:].split("_",1); target_uid=parts[0]; ridx=int(parts[1]) if len(parts)>1 else -1
            db=load_db()
            if target_uid in db["users"] and 0<=ridx<len(db["users"][target_uid].get("reviews",[])):
                db["users"][target_uid]["reviews"].pop(ridx); save_db(db); await q.answer("Удалено")
                revs=db["users"][target_uid].get("reviews",[]); u2=db["users"][target_uid]; uname2=u2.get("username","?")
                if not revs:
                    await q.edit_message_text(f"<b>@{uname2}: отзывов нет</b>",parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="adm_back",icon_custom_emoji_id="5258084656674250503")]])); return
                lines=[f"{Estr} <b>Отзывы @{uname2} ({len(revs)}):</b>"]; rows2=[]
                for i,r in enumerate(revs):
                    lines.append(f"\n{i+1}. {H(r)}")
                    rows2.append([InlineKeyboardButton(f"#{i+1}",callback_data=f"adm_del_rev_{target_uid}_{i}",icon_custom_emoji_id="5904542823167824187")])
                rows2.append([InlineKeyboardButton("Назад",callback_data="adm_back",icon_custom_emoji_id="5258084656674250503")])
                await q.edit_message_text("\n".join(lines),parse_mode="HTML",reply_markup=InlineKeyboardMarkup(rows2)); return
            return

        if d.startswith("paid_"): await on_paid(update,context); return
        if d.startswith("transferred_"): await on_transferred(update,context); return
        if d=="noop": return
        if d.startswith("adm_confirm_"): await adm_confirm(update,context); return
        if d.startswith("adm_decline_"): await adm_decline(update,context); return
        if d=="adm_back":
            for key in list(ud):
                if key.startswith("adm_"): ud.pop(key,None)
            try: await q.message.edit_text(f"{Edl} <b>Панель администратора</b>",parse_mode="HTML",reply_markup=adm_kb())
            except: await q.message.reply_text(f"{Edl} <b>Панель администратора</b>",parse_mode="HTML",reply_markup=adm_kb())
            return
        if d.startswith("adm_"): await handle_adm_cb(update,context); return

    except Exception as e:
        logger.error(f"on_cb ERROR d={q.data if 'q' in dir() else '?'}: {e}", exc_info=True)
        try:
            await update.effective_chat.send_message(f"Ошибка кнопки: {e}")
        except: pass

# ─── Messages ─────────────────────────────────────────────────────────────────
async def on_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        ud=context.user_data; uid=update.effective_user.id; lang=get_lang(uid); ru=lang=="ru"
        text=update.message.text.strip() if update.message.text else ""
        if uid in ADMIN_IDS and ud.get("adm_step"): await handle_adm_msg(update,context); return
        # Restore requisite wizard if user_data was lost (profile / deal / join)
        restore_req_input_state(ud, uid)

        if ud.get("topup_step")=="amount":
            raw_amount=text.replace(" ","").replace(",",".")
            try:
                value=float(raw_amount)
                if value<=0 or not math.isfinite(value): raise ValueError
            except ValueError:
                await update.message.reply_text(
                    f"{Ewrn} <b>{R(ru,'Введите число больше 0. Пример: 3','Enter a number greater than 0. Example: 3')}</b>",
                    parse_mode="HTML"); return
            method=ud.get("topup_method")
            if method in TOPUP_MINIMUMS and value<TOPUP_MINIMUMS[method]:
                await update.message.reply_text(
                    f"{Ewrn} <b>{R(ru,'Сумма слишком маленькая.','Amount is too small.')}</b>\n\n"
                    f"<blockquote>{R(ru,'Введите сумму ещё раз.','Enter the amount again.')}</blockquote>",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                        R(ru,"Назад","Back"),callback_data="topup_methods",
                        icon_custom_emoji_id="5258084656674250503")]])); return
            ud["topup_amount"]=raw_amount; ud.pop("topup_step",None)
            if method in TOPUP_MINIMUMS:
                payment_ref=ud.setdefault("topup_ref",f"EG-{uid}-{int(time.time())}")
                await send_section(update,
                    topup_details_text(method,raw_amount,uid,lang,payment_ref),
                    topup_details_kb(method,raw_amount,payment_ref,lang),
                    section="balance"); return
            await send_section(update,
                f"{Emn} <b>{R(ru,'Выберите способ пополнения:','Choose a top-up method:')}</b>",
                topup_methods_kb(lang),section="balance"); return

        if ud.get("req_step") in REQ_FIELDS:
            field=ud["req_step"]; db=load_db(); u=get_user(db,uid)
            err=None
            if not text:
                await update.message.reply_text(
                    f"{Ewrn} <b>{R(ru,'Отправьте текст реквизитов одним сообщением.','Send the requisites as one text message.')}</b>",
                    parse_mode="HTML"); return
            if is_card_req_field(field):
                if ud.get("card_step")=="bank":
                    bank_ok=validate_bank_name(text)
                    if not bank_ok:
                        bank_ex=req_bank_examples(field, lang)
                        await update.message.reply_text(
                            f"{Ewrn} <b>{R(ru,'Введите корректное название банка (минимум 2 буквы):','Enter a valid bank name (at least 2 letters):')}</b>\n<blockquote>{bank_ex}</blockquote>",
                            parse_mode="HTML"); return
                    card_val=ud.pop("card_pending","")
                    text=f"{card_val}|{bank_ok}"
                    ud.pop("card_step",None)
                    set_req_input_state(uid, field, card_step=None, card_pending=None)
                else:
                    r=validate_card(text, lang)
                    if r is None:
                        if ru:
                            err=("Неверный формат. Введите телефон (+7… / +380…) или номер карты (16–19 цифр).\n\n"
                                 "<b>Примеры:</b>\n<code>+79041751408</code>\n<code>+380501234567</code>\n<code>4276123456781234</code>")
                        else:
                            err=("Invalid format. Enter phone (+7… / +380… / +1…) or card number (16–19 digits).\n\n"
                                 "<b>Examples:</b>\n<code>+79041751408</code>\n<code>+380501234567</code>\n<code>4111111111111111</code>")
                    else:
                        ud["card_pending"]=r; ud["card_step"]="bank"
                        set_req_input_state(uid, field, card_step="bank", card_pending=r)
                        bank_ex=req_bank_examples(field, lang)
                        await update.message.reply_text(
                            f"{Ecrd} <b>{R(ru,'Введите название банка:','Enter your bank name:')}</b>\n\n<blockquote>{R(ru,'Пример:','Example:')} {bank_ex}</blockquote>",
                            parse_mode="HTML"); return
            elif field=="ton":
                ton_addr=validate_ton_address(text)
                if not ton_addr:
                    err=R(ru,"Неверный TON адрес. Нужен адрес на UQ или EQ (48 символов).\n\n<b>Пример:</b>\n<code>UQDxxx...xxx</code>",
                          "Invalid TON address. Need UQ/EQ address (48 chars).\n\n<b>Example:</b>\n<code>UQDxxx...xxx</code>")
                else:
                    text=ton_addr
            elif field=="stars":
                t2=text if text.startswith("@") else f"@{text}"
                cl,ec=validate_username(t2)
                if ec: err=R(ru,"Неверный формат. Введите @username (минимум 5 символов, только латиница, цифры и _).\n\n<b>Пример:</b>\n<code>@username</code>",
                              "Invalid format. Enter @username (min 5 chars, latin letters, digits and _ only).\n\n<b>Example:</b>\n<code>@username</code>")
                else: text=cl
            if err:
                await update.message.reply_text(f"{Ewrn} {err}",parse_mode="HTML"); return

            u.setdefault("requisites",{})[field]=text
            save_db(db)
            ud.pop("req_step",None)
            for k in ("card_step","card_pending","card_bank_name"): ud.pop(k,None)

            if ud.pop("req_after_buyer_deal",None):
                clear_req_input_state(uid)
                await update.message.reply_text(f"<b><tg-emoji emoji-id='5260341314095947411'>👀</tg-emoji> {R(ru,'Реквизиты сохранены!','Requisites saved!')}</b>",parse_mode="HTML")
                resume=ud.pop("req_resume",None)
                # Resume where the user was blocked (currency → amount / confirmation)
                if resume=="amount" or (ud.get("currency") and ud.get("type") and ud.get("partner")):
                    cur=ud.get("currency")
                    if cur and not user_has_requisites_for(u, cur):
                        ud["req_resume"]="amount"
                        await update.effective_chat.send_message(
                            f"{Ewrn} <b>{R(ru,'Для этой валюты нужны подходящие реквизиты.','This currency needs matching requisites.')}</b>",
                            parse_mode="HTML",reply_markup=currency_requisites_kb(cur,lang)); return
                    if ud.get("amount") not in (None,"","-"):
                        ud.setdefault("pay_currency",cur)
                        ud.setdefault("payment_amount",ud.get("amount"))
                        await show_deal_confirmation(update,context); return
                    ud["step"]="amount"; ud.setdefault("pay_currency",cur)
                    msg=await update.effective_chat.send_message(
                        deal_amount_prompt(cur,lang),parse_mode="HTML")
                    ud["last_msg"]=msg.message_id; return
                # Type already chosen → continue to partner username
                if resume=="partner" or (ud.get("type") and not ud.get("partner") and ud.get("creator_role")):
                    ud["step"]="partner"
                    cr=ud.get("creator_role","seller")
                    pp=R(ru,"Введите @username продавца:","Enter seller @username:") if cr=="buyer" else R(ru,"Введите @username покупателя:","Enter buyer @username:")
                    msg=await update.effective_chat.send_message(
                        f"<b>{pp}</b>\n\n<b>{R(ru,'Пример','Example')}:</b> <code>@username</code>",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_deal",icon_custom_emoji_id="5258084656674250503")]]))
                    ud["last_msg"]=msg.message_id; return
                if not ud.get("creator_role"):
                    await update.effective_chat.send_message(
                        f"<tg-emoji emoji-id='5879841310902324730'>✏️</tg-emoji> <b>{R(ru,'Создать сделку','Create Deal')}\n\n{R(ru,'Кто вы в этой сделке?','What is your role?')}</b>",
                        parse_mode="HTML",reply_markup=role_kb(lang)); return
                await update.effective_chat.send_message(
                    f"<b><tg-emoji emoji-id='5258216851472654189'>💡</tg-emoji> {R(ru,'Выберите тип сделки','Choose deal type')}</b>",
                    parse_mode="HTML",reply_markup=types_kb(lang)); return

            pending=ud.pop("req_for_deal",None) or ud.pop("pending_deal",None)
            if not pending:
                _st=(get_user(load_db(),uid).get("req_input") or {})
                if _st.get("mode")=="join":
                    pending=_st.get("deal_id") or get_user(load_db(),uid).get("join_pending_deal")
            if pending:
                pending=str(pending).strip().upper()
                deal_pending=load_db().get("deals",{}).get(pending)
                await update.message.reply_text(
                    f"<b><tg-emoji emoji-id='5260341314095947411'>👀</tg-emoji> {R(ru,'Реквизиты сохранены!','Requisites saved!')}</b>",
                    parse_mode="HTML")
                if not deal_pending:
                    clear_join_req_state(uid)
                    await update.message.reply_text(
                        f"{Ewrn} <b>{R(ru,'Сделка не найдена. Откройте ссылку ещё раз.','Deal not found. Open the link again.')}</b>",
                        parse_mode="HTML"); return
                deal_cur=deal_pending.get("currency") or deal_pending.get("deal_currency")
                # reload user after save
                u=get_user(load_db(),uid)
                if not user_has_requisites_for(u, deal_cur):
                    context.user_data["pending_deal"]=pending
                    set_join_req_state(uid, pending, None)
                    need=req_need_label(requisite_field_for_currency(deal_cur), lang)
                    await update.effective_chat.send_message(
                        f"{Ewrn} <b>{R(ru,'Для этой сделки нужны реквизиты','This deal needs requisites')}: {need}</b>",
                        parse_mode="HTML",reply_markup=deal_join_req_kb(pending, deal_cur, lang)); return
                try:
                    ok=await complete_deal_join(update,context,pending)
                except Exception as join_err:
                    logger.error(f"complete_deal_join after req: {join_err}", exc_info=True)
                    ok=False
                clear_join_req_state(uid)
                if not ok:
                    await update.message.reply_text(
                        f"{Ewrn} <b>{R(ru,'Не удалось присоединиться к сделке. Откройте ссылку ещё раз или напишите в поддержку.','Failed to join the deal. Open the link again or contact support.')}</b>",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Поддержка","Support"),url=SUPPORT_URL,icon_custom_emoji_id="5258260149037965799")]]))
                return

            clear_join_req_state(uid)
            req_return=ud.pop("req_return","menu_req")
            await update.message.reply_text(
                f"<b><tg-emoji emoji-id='5260341314095947411'>👀</tg-emoji> {R(ru,'Реквизиты сохранены!','Requisites saved!')}</b>",
                parse_mode="HTML")
            if req_return=="pay_currency":
                await show_deal_confirmation(update,context); return
            if req_return=="withdraw":
                await show_withdraw(update,context); return
            await show_req(update,context); return

        if ud.get("withdraw_step")=="req":
            method=ud.get("withdraw_method","?"); db=load_db()
            u=get_user(db,uid); bal=u.get("balance",0); uname3=update.effective_user.username or str(uid)
            mnames={"stars":R(ru,"Звёзды","Stars"),"crypto":R(ru,"Крипта","Crypto"),"card":R(ru,"Карта","Card")}
            mname=mnames.get(method,method)
            await notify_admins(context,
                f"{Edm} <b>Вывод - {mname}</b>\n{Eu} @{uname3} (<code>{uid}</code>)\n"
                f"{Emn} {bal} RUB\n\nРеквизиты: <code>{H(text)}</code>")
            ud.pop("withdraw_step",None); ud.pop("withdraw_method",None)
            await update.message.reply_text(
                f"{Ech} <b>{R(ru,'Запрос отправлен!','Request sent!')}</b>\n\n<blockquote>{R(ru,'Менеджер свяжется с вами.','Manager will contact you.')}</blockquote>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(R(ru,"Менеджер","Manager"),url=MANAGER_URL,icon_custom_emoji_id="5316600120043649556")],
                    [InlineKeyboardButton(R(ru,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")],
                ])); return

        if ud.get("review_step")=="text":
            deal_id=ud.get("review_deal"); role=ud.get("review_role"); stars_r=ud.get("review_stars",5)
            db=load_db(); deal=db.get("deals",{}).get(deal_id,{})
            rev_text=f"{stars_r}/5 - {text}"
            saved=False
            if role=="s":
                buname=deal.get("partner","").lstrip("@").lower()
                buid=next((k for k,v in db.get("users",{}).items() if v.get("username","").lower()==buname),None)
                if not buid and deal.get("buyer_uid"): buid=deal.get("buyer_uid")
                if buid and buid in db["users"]:
                    db["users"][buid].setdefault("reviews",[]).append(rev_text); save_db(db); saved=True
            elif role=="b":
                suid=deal.get("user_id")
                if suid and suid in db.get("users",{}):
                    db["users"][suid].setdefault("reviews",[]).append(rev_text); save_db(db); saved=True
            for k in ("review_step","review_deal","review_role","review_stars"): ud.pop(k,None)
            await update.message.reply_text(f"{Ech} <b>{R(ru,'Отзыв сохранён!' if saved else 'Принят!','Review saved!' if saved else 'Received!')}</b>",parse_mode="HTML"); return

        dtype=ud.get("type"); step=ud.get("step")
        if not dtype or not step: return

        async def del_prev():
            try: await update.message.delete()
            except: pass
            if ud.get("last_msg"):
                try: await context.bot.delete_message(chat_id=update.effective_chat.id,message_id=ud["last_msg"])
                except: pass

        async def send_step(t2, kb=None):
            await del_prev()
            msg=await update.effective_chat.send_message(t2,parse_mode="HTML",reply_markup=kb)
            ud["last_msg"]=msg.message_id

        if step=="partner":
            t_raw = text.strip()
            if not t_raw.startswith("@"): t_raw = "@" + t_raw
            cl_p, ec_p = validate_username(t_raw)
            if ec_p:
                err_msg = R(ru,
                    "Некорректный @username. Минимум 5 символов, только латиница/цифры/подчёркивание.",
                    "Invalid @username. Min 5 chars, only latin/digits/underscore.")
                await update.message.reply_text(f"{Ewrn} <b>{err_msg}</b>\n\n<b>{R(ru,'Пример','Example')}:</b> <code>@username</code>",parse_mode="HTML"); return
            ud["partner"]=cl_p
            if dtype=="nft":
                ud["step"]="nft_link"
                await send_step(f"{Enft_link} <b>{R(ru,'Вставьте ссылку на NFT:','Paste NFT link:')}</b>\n\n<code>t.me/nft/...</code>")
            elif dtype=="username":
                ud["step"]="trade_usr"
                await send_step(f"{Eu} <b>{R(ru,'Введите ссылку (t.me/...):','Enter link (t.me/...):')}</b>")
            elif dtype=="stars":
                ud["step"]="stars_cnt"
                cr3=ud.get("creator_role","seller")
                stars_q=R(ru,"Введите сумму звёзд для продажи","Enter stars amount for sale") if cr3=="seller" else R(ru,"Введите сумму звёзд для покупки","Enter stars amount for purchase")
                await send_step(f"{Eamt_in} <b>{stars_q}</b>")
            elif dtype=="crypto":
                ud["step"]="currency"
                await send_step(deal_currency_prompt(lang),cur_kb(lang))
            elif dtype=="premium":
                ud["step"]="prem_period"
                await send_step(f"{Eprem} <b>Telegram Premium\n\n{R(ru,'Выберите срок:','Choose period:')}</b>",
                    InlineKeyboardMarkup([[
                        InlineKeyboardButton("3 "+R(ru,"мес.","mo"),callback_data="prm_3",icon_custom_emoji_id="5906715307820456633"),
                        InlineKeyboardButton("6 "+R(ru,"мес.","mo"),callback_data="prm_6",icon_custom_emoji_id="5906715307820456633"),
                        InlineKeyboardButton("12 "+R(ru,"мес.","mo"),callback_data="prm_12",icon_custom_emoji_id="5906715307820456633")]]))
            return

        if step=="nft_link":
            ok,em=validate_nft_link(text,dtype)
            if not ok:
                await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Некорректная ссылка. Формат: t.me/nft/НазваниеНФТ','Invalid link. Format: t.me/nft/NFTName')}</b>",parse_mode="HTML"); return
            clean_link=text.strip()
            for prefix in ("https://","http://"):
                if clean_link.startswith(prefix): clean_link=clean_link[len(prefix):]; break
            if not clean_link.startswith("t.me/"): clean_link="t.me/"+clean_link
            ud["nft_link"]=clean_link; ud["step"]="currency"
            await send_step(deal_currency_prompt(lang),cur_kb(lang)); return

        if step=="trade_usr":
            cl=text.strip().replace("https://","").replace("http://","")
            import re as _re
            ok_link = cl.startswith("t.me/") and len(cl[5:].strip("/"))>=5 and _re.fullmatch(r"[a-zA-Z0-9_]+", cl[5:].strip("/"))
            ok_at   = text.strip().startswith("@") and len(text.strip()[1:])>=5 and _re.fullmatch(r"[a-zA-Z0-9_]+", text.strip()[1:])
            if not ok_link and not ok_at:
                await update.message.reply_text(
                    f"{Ewrn} <b>{R(ru,'Введите корректную ссылку t.me/username или @username (мин. 5 символов).','Enter valid t.me/username or @username (min 5 chars).')}</b>",
                    parse_mode="HTML"); return
            ud["trade_username"]=text.strip(); ud["step"]="currency"
            await send_step(deal_currency_prompt(lang),cur_kb(lang)); return

        if step=="stars_cnt":
            if not text.isdigit():
                await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Только цифры!','Numbers only!')}</b>",parse_mode="HTML"); return
            ud["stars_count"]=text; ud["step"]="currency"
            await send_step(deal_currency_prompt(lang),cur_kb(lang)); return

        if step=="payment_amount":
            # legacy: keep one amount equal to deal amount
            ud["payment_amount"]=ud.get("amount")
            ud["pay_currency"]=ud.get("currency")
            await show_deal_confirmation(update,context); return

        if step in ("cry_currency","prem_period","prem_currency","currency","pay_currency"):
            await update.message.reply_text(
                f"{Ewrn} <b>{R(ru,'Выберите вариант из кнопок выше.','Please choose an option from the buttons above.')}</b>",
                parse_mode="HTML"); return

        if step=="amount":
            ca=normalize_currency_amount(text,ud.get("currency"))
            if ca is None:
                await update.message.reply_text(f"{Ewrn} <b>{R(ru,'Введите корректную сумму больше 0 с допустимой точностью.','Enter a valid amount greater than 0 with supported precision.')}</b>",parse_mode="HTML")
                return
            ud["amount"]=ca
            ud["pay_currency"]=ud.get("currency")
            ud["payment_amount"]=ca
            ud.pop("step",None)
            await del_prev()
            await show_deal_confirmation(update,context)
            return

    except Exception as e: logger.error(f"on_msg ERROR: {e}", exc_info=True)

# ─── Finalize deal ────────────────────────────────────────────────────────────
async def finalize_deal(update, context):
    try:
        ud=context.user_data; db=load_db()
        if ud.get("_finalizing_deal"): return
        required=("creator_role","type","partner","currency","amount")
        if any(ud.get(key) in (None,"","-") for key in required):
            raise ValueError("incomplete deal draft")
        user=update.effective_user
        u_check=get_user(db,user.id)
        currency=ud.get("currency","-")
        if not user_has_requisites_for(u_check, currency):
            lang=get_lang(user.id); ru=lang=="ru"
            ud["req_resume"]="amount"
            await update.effective_chat.send_message(
                f"{Ewrn} <b>{R(ru,'Без реквизитов создать сделку нельзя.','You cannot create a deal without requisites.')}</b>",
                parse_mode="HTML",reply_markup=currency_requisites_kb(currency,lang)); return
        ud["_finalizing_deal"]=True
        dtype=ud.get("type","?"); partner=ud.get("partner","-")
        amount=ud.get("amount","-")
        pay_currency=ud.get("pay_currency") or currency
        payment_amount=ud.get("payment_amount") or amount
        creator_role=ud.get("creator_role","seller")

        data={}
        for key in ("nft_link","trade_username","stars_count","premium_period"):
            if ud.get(key) is not None: data[key]=ud[key]

        deal_id=gen_deal_id(db)
        db["deals"][deal_id]={
            "user_id":str(user.id),"type":dtype,"partner":partner,
            "currency":pay_currency,"amount":amount,"status":"pending",
            "created":datetime.now().isoformat(),"data":data,"creator_role":creator_role,
            "deal_currency":currency,"payment_amount":payment_amount,
        }
        add_log(db,"Новая сделка",deal_id=deal_id,uid=user.id,username=user.username or "",
            extra=f"{dtype} | {amount} {currency} | {creator_role}")
        save_db(db)
        if db.get("logs"): await send_log_msg(context,db,db["logs"][-1])

        cu=db["users"].get(str(user.id),{}).get("username","")
        creator_tag=f"@{cu}" if cu else f"@{user.username or str(user.id)}"
        partner_tag=partner
        lang=get_lang(user.id); ru=lang=="ru"
        uname=f"@{user.username}" if user.username else f"#{user.id}"
        await notify_admins(
            context,
            f"{Edl} <b>Новая сделка</b>\n\n"
            f"{Eu} {H(uname)} (<code>{user.id}</code>)\n"
            f"{Edln} <code>{deal_id}</code>\n"
            f"Тип: {dtype}\n"
            f"Роль: {creator_role}\n"
            f"Партнёр: {H(partner)}\n"
            f"{Emn} {H(amount)} {cur_plain(currency,'ru')}")

        join_link_f=f"https://t.me/{BOT_USERNAME}?start=deal_{deal_id}"
        share_text=R(ru,
            "Сделка создана! Присоединяйтесь, чтобы провести сделку:",
            "Deal created! Join to complete the deal:")
        share_msg=R(ru,"Сделка создана! Присоединяйтесь.","Deal created! Join now.")
        share_url="https://t.me/share/url?"+urlencode({
            "url":join_link_f,
            "text":share_msg,
        }, quote_via=quote)
        text_out=(
            f"<tg-emoji emoji-id='5906840875484321836'>✅</tg-emoji> <b>{R(ru,'Сделка создана!','Deal created!')}</b>\n\n"
            f"{share_text}\n<a href=\"{H(join_link_f)}\">{H(join_link_f)}</a>"
        )
        kb=InlineKeyboardMarkup([
            [InlineKeyboardButton(R(ru,"Переслать партнёру","Forward to partner"),url=share_url,icon_custom_emoji_id="5316600120043649556")],
            [InlineKeyboardButton(R(ru,"Мои сделки","My Deals"),callback_data="menu_my_deals",icon_custom_emoji_id="5258476306152038031")],
            [InlineKeyboardButton(R(ru,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")],
        ])
        await send_new(update,text_out,kb,section="deal_card")
        try:
            await notify_deal_event(
                context.bot,user.id,
                f"<tg-emoji emoji-id='5906840875484321836'>✅</tg-emoji> <b>{R(ru,'Сделка создана!','Deal created!')}</b>\n\n"
                f"<blockquote>{R(ru,'Сделка','Deal')} <code>{deal_id}</code>\n"
                f"{R(ru,'Сумма','Amount')}: {cur_amount_phrase(amount,currency,lang)}\n"
                f"{R(ru,'Смотрите в «Мои сделки».','See it in My Deals.')}</blockquote>",
                lang)
        except Exception as e:
            logger.error(f"notify create my deals: {e}")

        pname=partner.lstrip("@").lower() if partner.startswith("@") else None
        if pname:
            puid=next((k for k,v in db["users"].items() if v.get("username","").lower()==pname),None)
            if puid:
                try:
                    pl=get_lang(int(puid)); pr=pl=="ru"
                    join_link=f"https://t.me/{BOT_USERNAME}?start=deal_{deal_id}"
                    txt2=(
                        f"<tg-emoji emoji-id='5906840875484321836'>✅</tg-emoji> <b>{R(pr,'Сделка создана! Присоединяйтесь, чтобы провести сделку.','Deal created! Join to complete the deal.')}</b>\n\n"
                        f"<a href=\"{H(join_link)}\">{H(join_link)}</a>"
                    )
                    kb2=InlineKeyboardMarkup([
                        [InlineKeyboardButton(R(pr,"Присоединиться","Join"),url=join_link,icon_custom_emoji_id="5893431652578758294")],
                        [InlineKeyboardButton(R(pr,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")]
                    ])
                    await send_banner_chat(context.bot,int(puid),txt2,kb2,section="deal_forward")
                except Exception as e: logger.error(f"notify partner: {e}")

        context.user_data.clear()
    except Exception as e:
        context.user_data.pop("_finalizing_deal",None)
        logger.error(f"finalize_deal: {e}", exc_info=True)

# ─── Participant actions ──────────────────────────────────────────────────────
async def on_transferred(update, context):
    try:
        q=update.callback_query; seller=update.effective_user
        deal_id=q.data[12:]; db=load_db(); deal=db.get("deals",{}).get(deal_id,{})
        buyer_uid,seller_uid=deal_participant_roles(deal)
        if str(seller.id)!=seller_uid:
            await update.effective_chat.send_message(
                f"{Ewrn} <b>{R(get_lang(seller.id)=='ru','Эта кнопка доступна продавцу.','This button is for the seller.')}</b>",
                parse_mode="HTML"); return
        if deal.get("item_transferred"): return
        deal["item_transferred"]=True; db["deals"][deal_id]=deal
        add_log(db,"Товар передан",deal_id=deal_id,uid=seller.id,username=seller.username or "")
        save_db(db)
        if db.get("logs"): await send_log_msg(context,db,db["logs"][-1])
        seller_tag=f"@{seller.username}" if seller.username else f"#{seller.id}"
        payment_attempt=int(deal.get("payment_attempt",0))
        admin_kb=InlineKeyboardMarkup([[
            InlineKeyboardButton("Подтвердить сделку",callback_data=f"adm_confirm_{deal_id}_{payment_attempt}",icon_custom_emoji_id="5316827280863934685")
        ]])
        await notify_admins(
            context,
            f"{Ech} <b>Продавец передал товар</b>\n\n{Eu} {seller_tag}\n{Edl} <code>{deal_id}</code>",
            admin_kb)
        if buyer_uid:
            try:
                buyer_lang=get_lang(int(buyer_uid)); buyer_ru=buyer_lang=="ru"
                db2=load_db(); deal2=db2.get("deals",{}).get(deal_id,deal)
                creator_uid=str(deal2.get("user_id",""))
                c_uname=db2.get("users",{}).get(creator_uid,{}).get("username","")
                s_uname=seller.username or ""
                creator_tag=f"@{c_uname}" if c_uname else f"#{creator_uid}"
                seller_tag2=f"@{s_uname}" if s_uname else f"#{seller.id}"
                is_buyer_creator=creator_uid==str(buyer_uid)
                deal_txt=build_deal_text(
                    deal_id,deal2,creator_tag,seller_tag2 if is_buyer_creator else creator_tag,
                    buyer_lang,joined=True,is_creator=is_buyer_creator)
                partner_uname=s_uname if is_buyer_creator else c_uname
                await send_banner_chat(
                    context.bot,int(buyer_uid),
                    f"{Ech} <b>{R(buyer_ru,'Продавец передал товар. Можно оплачивать.','Seller transferred the item. You can pay now.')}</b>\n\n{deal_txt}",
                    deal_action_kb(deal_id,deal2,"buyer",buyer_lang,partner_uname,is_creator=is_buyer_creator),
                    section="deal_join" if is_buyer_creator else "deal_card")
            except Exception as e:
                logger.error(f"notify buyer transferred: {e}")
        lang=get_lang(seller.id); ru=lang=="ru"
        await q.edit_message_reply_markup(InlineKeyboardMarkup([
                [InlineKeyboardButton(R(ru,"Ожидайте подтверждения менеджера","Waiting for manager confirmation"),callback_data="noop",icon_custom_emoji_id=WAIT_ICON)],
            [InlineKeyboardButton(R(ru,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")],
        ]))
    except Exception as e:
        logger.error(f"on_transferred: {e}")

async def on_paid(update, context):
    try:
        q=update.callback_query; buyer=update.effective_user
        bl=get_lang(buyer.id); rb=bl=="ru"
        deal_id=q.data[5:]; btag=f"@{buyer.username}" if buyer.username else str(buyer.id)
        db=load_db(); d=db.get("deals",{}).get(deal_id,{})
        buyer_uid,seller_uid=deal_participant_roles(d)
        if str(buyer.id)!=buyer_uid:
            await update.effective_chat.send_message(
                f"{Ewrn} <b>{R(rb,'Эта кнопка доступна покупателю.','This button is for the buyer.')}</b>",
                parse_mode="HTML"); return
        if d.get("payment_reported"): return
        payment_attempt=int(d.get("payment_attempt",0))+1
        d["payment_attempt"]=payment_attempt
        d["payment_reported"]=True; db["deals"][deal_id]=d
        amt=d.get("amount","-"); cur=d.get("currency","-")
        deal_cur=d.get("deal_currency") or cur
        payment_amount=d.get("payment_amount") or amt
        suid=seller_uid; sl2=get_lang(int(suid)) if suid else "ru"; rs2=sl2=="ru"
        paid_text=(f"{Ebl} <b>'Я оплатил'</b>\n\n{Ecrd} {btag} (<code>{buyer.id}</code>)\n"
                   f"{Emn} Сумма: {payment_amount} {cur}\n\nПроверьте оплату:")
        paid_kb=InlineKeyboardMarkup([[
            InlineKeyboardButton("Пришла",callback_data=f"adm_confirm_{deal_id}_{payment_attempt}",icon_custom_emoji_id="5316827280863934685"),
            InlineKeyboardButton("Не пришла",callback_data=f"adm_decline_{deal_id}_{payment_attempt}",icon_custom_emoji_id="5904542823167824187")
        ]])
        add_log(db,"Оплачено",deal_id=deal_id,uid=buyer.id,username=buyer.username or "",extra=f"{payment_amount} {cur}")
        save_db(db)
        if db.get("logs"): await send_log_msg(context,db,db["logs"][-1])
        await notify_admins(context,paid_text,paid_kb)
        seller=suid
        if seller and seller!=str(buyer.id):
            try:
                db2=load_db(); deal2=db2.get("deals",{}).get(deal_id,d)
                creator_uid=str(deal2.get("user_id",""))
                is_seller_creator=creator_uid==str(seller)
                c_uname=db2.get("users",{}).get(creator_uid,{}).get("username","")
                b_uname=buyer.username or ""
                creator_tag=f"@{c_uname}" if c_uname else f"#{creator_uid}"
                buyer_tag=f"@{b_uname}" if b_uname else f"#{buyer.id}"
                deal_txt=build_deal_text(
                    deal_id,deal2,
                    creator_tag if is_seller_creator else buyer_tag,
                    buyer_tag if is_seller_creator else creator_tag,
                    sl2,joined=True,is_creator=is_seller_creator)
                partner_uname=b_uname if is_seller_creator else c_uname
                await send_banner_chat(
                    context.bot,int(seller),
                    f"{Ebl} <b>{R(rs2,'Покупатель оплатил! Можно передавать товар.','Buyer paid! You can transfer the item.')}</b>\n\n{deal_txt}",
                    deal_action_kb(deal_id,deal2,"seller",sl2,partner_uname,is_creator=is_seller_creator),
                    section="deal_join" if is_seller_creator else "deal_card")
            except: pass
        try:
            await q.edit_message_reply_markup(InlineKeyboardMarkup([
                [InlineKeyboardButton(R(rb,'Ожидайте подтверждения менеджера','Waiting for manager confirmation'),callback_data="noop",icon_custom_emoji_id=WAIT_ICON)],
                [InlineKeyboardButton(R(rb,"Главное меню","Main menu"),callback_data="main_menu",icon_custom_emoji_id="5316887736823591263")]
            ]))
        except: pass
    except Exception as e: logger.error(f"on_paid: {e}")

# ─── adm_confirm / decline ────────────────────────────────────────────────────
async def adm_confirm(update, context):
    try:
        q=update.callback_query; await q.answer()
        if update.effective_user.id not in ADMIN_IDS: return
        deal_id,payment_attempt=parse_admin_deal_attempt(q.data,"adm_confirm_"); db=load_db()
        if deal_id not in db.get("deals",{}): return
        if db["deals"][deal_id].get("status")=="confirmed": return
        d=db["deals"][deal_id]
        current_attempt=int(d.get("payment_attempt",0))
        if payment_attempt is None and current_attempt>0: return
        if payment_attempt is not None and payment_attempt!=current_attempt: return
        if not d.get("payment_reported") or not d.get("item_transferred"):
            missing=[]
            if not d.get("payment_reported"): missing.append("оплата покупателя")
            if not d.get("item_transferred"): missing.append("передача товара продавцом")
            await q.edit_message_text(
                f"{Ewrn} <b>Сделка ещё не готова к завершению.</b>\n\n"
                f"<blockquote>Ожидается: {', '.join(missing)}</blockquote>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("Проверить ещё раз",callback_data=f"adm_confirm_{deal_id}_{int(d.get('payment_attempt',0))}",icon_custom_emoji_id="5316827280863934685")
                ]]))
            return
        db["deals"][deal_id]["status"]="confirmed"; d=db["deals"][deal_id]
        buyer_uid,seller_uid=deal_participant_roles(d)
        s=seller_uid; amt_str=d.get("amount","0"); dtype=d.get("type",""); dd=d.get("data",{})
        deal_currency=d.get("deal_currency") or d.get("currency","")
        payment_amount=d.get("payment_amount") or amt_str; payment_currency=d.get("currency","")
        try: amt_num=Decimal(str(amt_str))
        except InvalidOperation: amt_num=Decimal("0")
        if s and s in db["users"]:
            db["users"][s]["success_deals"]=db["users"][s].get("success_deals",0)+1
            db["users"][s]["total_deals"]=db["users"][s].get("total_deals",0)+1
            if deal_currency=="RUB":
                db["users"][s]["turnover"]=db["users"][s].get("turnover",0)+int(amt_num)
        ilink=""
        if dtype=="nft" and dd.get("nft_link"): ilink=f"\n{Eln} {dd['nft_link']}"
        elif dtype=="username" and dd.get("trade_username"): ilink=f"\n{Eln} {dd['trade_username']}"
        seller_uname=db["users"].get(s,{}).get("username","?") if s else "?"
        add_log(db,"Подтверждено",deal_id=deal_id,uid=s,username=seller_uname,
            extra=f"{amt_str} {deal_currency} | {payment_amount} {payment_currency}")
        if s and s in db["users"]:
            ref_uid=db["users"][s].get("ref_by")
            if ref_uid and ref_uid in db["users"] and amt_num>0 and deal_currency=="RUB":
                bonus=int(amt_num*Decimal("0.03"))
                if bonus>0:
                    db["users"][ref_uid]["ref_earned"]=db["users"][ref_uid].get("ref_earned",0)+bonus
                    db["users"][ref_uid]["balance"]=db["users"][ref_uid].get("balance",0)+bonus
                    try:
                        rl=get_lang(int(ref_uid)); rr=rl=="ru"
                        await context.bot.send_message(chat_id=int(ref_uid),
                            text=f"{Emn} <b>{R(rr,'Реферальный бонус!','Referral bonus!')}</b>\n<blockquote>+{bonus} RUB (3%)</blockquote>",parse_mode="HTML")
                    except: pass
        save_db(db)
        if db.get("logs"): await send_log_msg(context,db,db["logs"][-1])
        try:
            log_chat=db.get("log_chat_id")
            if log_chat:
                buyer_uid_post=d.get("buyer_uid")
                if not buyer_uid_post:
                    for u_p,ud_p in db.get("users",{}).items():
                        if ud_p.get("username","").lower()==d.get("partner","").lstrip("@").lower():
                            buyer_uid_post=u_p; break
                buyer_uname_post=db["users"].get(buyer_uid_post,{}).get("username","") if buyer_uid_post else ""
                buyer_link_post=f"@{buyer_uname_post}" if buyer_uname_post else d.get("partner","?")
                nft_link_post=dd.get("nft_link","") if dtype=="nft" else dd.get("trade_username","") if dtype=="username" else ""
                link_str=f"\n{Eln} {nft_link_post}" if nft_link_post else ""
                post_text=(
                    f"{ce('5258262708838472996','🔥')} <b>Новый мамонтёнок!</b>\n\n"
                    f"{Eu} {buyer_link_post}\n"
                    f"{Emn} <b>{amt_str} {deal_currency}</b>"
                    f"{link_str}"
                )
                await context.bot.send_message(chat_id=int(log_chat),text=post_text,parse_mode="HTML")
                extra_grp=db.get("extra_group_id")
                if extra_grp:
                    try:
                        await context.bot.send_message(chat_id=int(extra_grp),text=post_text,parse_mode="HTML")
                    except Exception as eg: logger.error(f"extra_group post: {eg}")
        except Exception as e: logger.error(f"confirm group post: {e}")
        try: await q.edit_message_text(
            f"{Ech} <b>Подтверждено!</b>\n"
            f"{amt_str} {deal_currency}\n{payment_amount} {payment_currency}{ilink}",
            parse_mode="HTML")
        except: pass
        if s:
            try:
                sl=get_lang(int(s)); rs=sl=="ru"
                await context.bot.send_message(chat_id=int(s),
                    text=f"{Ech} <b>{R(rs,'Сделка завершена!','Deal completed!')}</b>",parse_mode="HTML")
            except: pass
        if buyer_uid:
            try:
                bl2=get_lang(int(buyer_uid)); rb2=bl2=="ru"
                await context.bot.send_message(chat_id=int(buyer_uid),
                    text=f"{Ech} <b>{R(rb2,'Оплата подтверждена!','Payment confirmed!')}</b>",parse_mode="HTML")
            except: pass
    except Exception as e: logger.error(f"adm_confirm: {e}")

async def adm_decline(update, context):
    try:
        q=update.callback_query; await q.answer()
        if update.effective_user.id not in ADMIN_IDS: return
        deal_id,payment_attempt=parse_admin_deal_attempt(q.data,"adm_decline_")
        db=load_db(); d=db.get("deals",{}).get(deal_id,{})
        if not d or d.get("status")=="confirmed" or not d.get("payment_reported"): return
        current_attempt=int(d.get("payment_attempt",0))
        if payment_attempt is None and current_attempt>0: return
        if payment_attempt is not None and payment_attempt!=current_attempt: return
        d["payment_reported"]=False
        d["payment_declined_at"]=datetime.now().isoformat()
        db["deals"][deal_id]=d; save_db(db)
        try:
            await q.edit_message_text(
                f"{Ewrn} <b>Не подтверждено.</b>\n<code>{deal_id}</code>\n"
                f"{Emn} {d.get('payment_amount',d.get('amount','-'))} {d.get('currency','-')}",
                parse_mode="HTML",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Всё же пришла",callback_data=f"adm_confirm_{deal_id}_{int(d.get('payment_attempt',0))}",icon_custom_emoji_id="5316827280863934685")]]))
        except: pass
        # Уведомить участников сделки
        buyer_uid_d,seller_uid_d = deal_participant_roles(d)
        deal_link_d = f"https://t.me/{BOT_USERNAME}?start=deal_{deal_id}"
        back_kb_d = InlineKeyboardMarkup([[InlineKeyboardButton(
            "Вернуться к сделке",
            url=deal_link_d,
            icon_custom_emoji_id="5893161718179173515"
        )]])
        failed_amount=d.get("payment_amount",d.get("amount","-")); failed_currency=d.get("currency","-")
        msg_fail_ru = f"{Ewrn} <b>Оплата не прошла.</b>\n\n{Emn} {failed_amount} {failed_currency}"
        msg_fail_en = f"{Ewrn} <b>Payment failed.</b>\n\n{Emn} {failed_amount} {failed_currency}"
        if seller_uid_d:
            try:
                sl_d=get_lang(int(seller_uid_d))
                await context.bot.send_message(chat_id=int(seller_uid_d),
                    text=msg_fail_ru if sl_d=="ru" else msg_fail_en,
                    parse_mode="HTML",reply_markup=back_kb_d)
            except: pass
        if buyer_uid_d:
            try:
                bl_d=get_lang(int(buyer_uid_d))
                await context.bot.send_message(chat_id=int(buyer_uid_d),
                    text=msg_fail_ru if bl_d=="ru" else msg_fail_en,
                    parse_mode="HTML",reply_markup=back_kb_d)
            except: pass
    except Exception as e: logger.error(f"adm_decline: {e}")

# ─── Sections ─────────────────────────────────────────────────────────────────
async def show_balance(update, context):
    try:
        db=load_db(); uid=update.effective_user.id; u=get_user(db,uid)
        lang=get_lang(uid); ru=lang=="ru"; bal=u.get("balance",0)
        await send_section(update,
            f"{Ewlt} <b>{R(ru,'Пополнить / Вывод','Top Up / Withdraw')}</b>\n\n"
            f"<blockquote>{Ebal} <b>{R(ru,'Баланс','Balance')}: {bal} RUB</b></blockquote>",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(R(ru,"Пополнить","Top Up"),callback_data="balance_topup",icon_custom_emoji_id="5810051751654460532")],
                [InlineKeyboardButton(R(ru,"Вывод","Withdraw"),callback_data="withdraw",icon_custom_emoji_id="5807626765874499116")],
                [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")],
            ]),section="balance")
    except Exception as e: logger.error(f"show_balance: {e}")

async def show_lang(update, context):
    try:
        uid=update.effective_user.id; lang=get_lang(uid); ru=lang=="ru"
        rows=[
            [InlineKeyboardButton("Русский",callback_data="lang_ru",icon_custom_emoji_id="5377472000040115969")],
            [InlineKeyboardButton("English",callback_data="lang_en",icon_custom_emoji_id="5375544401537803855")],
            [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")],
        ]
        await send_section(update,
            f"<b>{ce('5447410659077661506','🌐')} {R(ru,'Выберите язык:','Select language:')}</b>",
            InlineKeyboardMarkup(rows),section="main")
    except Exception as e: logger.error(f"show_lang: {e}")

async def set_lang(update, context, lang):
    try:
        db=load_db(); u=get_user(db,update.effective_user.id); u["lang"]=lang; save_db(db)
        await update.callback_query.answer("OK")
        await show_main(update,context)
    except Exception as e: logger.error(f"set_lang: {e}")

async def show_profile(update, context):
    try:
        db=load_db(); uid=update.effective_user.id; u=get_user(db,uid)
        lang=get_lang(uid); ru=lang=="ru"
        uname=update.effective_user.username or "-"
        status=H(u.get("status",""))
        sl=f"\n<blockquote>{R(ru,'Статус','Status')}: {status}</blockquote>" if status else ""
        reviews=u.get("reviews",[])
        rv=""
        if reviews:
            rv_lines=[]
            for r in reviews[-10:]:
                import re as _re2
                m=_re2.search(r'(\d)/5',r)
                stars_num=int(m.group(1)) if m else 5
                star_str=ce("5321485469249198987","⭐")*stars_num
                rv_lines.append(f"{star_str} {H(r)}")
            rv=f"\n\n{Estr} <b>{R(ru,f'Отзывы ({len(reviews)})',f'Reviews ({len(reviews)})')}</b>\n<blockquote>"+'\n'.join(rv_lines)+'</blockquote>'
        text=(f"{Ecwn} <b>{R(ru,'Профиль','Profile')}</b>{sl}\n\n"
              f"{Eprof_user} @{uname}\n"
              f"{Ebal} {R(ru,'Баланс','Balance')}: <b>{u.get('balance',0)} RUB</b>\n"
              f"{Estr} {R(ru,'Сделок','Deals')}: <b>{u.get('total_deals',0)}</b>\n"
              f"{Eprof_ok} {R(ru,'Успешных','Successful')}: <b>{u.get('success_deals',0)}</b>\n"
              f"{Emn} {R(ru,'Оборот','Turnover')}: <b>{u.get('turnover',0)} RUB</b>{rv}")
        await send_section(update,text,InlineKeyboardMarkup([
            [InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")]
        ]),section="profile")
    except Exception as e: logger.error(f"show_profile: {e}")

async def show_ref(update, context):
    try:
        db=load_db(); uid=update.effective_user.id; u=get_user(db,uid); save_db(db)
        db=load_db(); u=db["users"][str(uid)]; lang=get_lang(uid); ru=lang=="ru"
        ref_link=f"https://t.me/{BOT_USERNAME}?start=ref_{uid}"
        rc=u.get("ref_count",0); re=u.get("ref_earned",0)
        refs=[v.get("username","?") for v in db.get("users",{}).values() if v.get("ref_by")==str(uid)]
        refs_str=""
        if refs: refs_str="\n\n"+R(ru,"Рефералы","Referrals")+":\n"+"\n".join(f"{Esrk} @{r}" if r and r!="?" else f"{Esrk} #?" for r in refs[-10:])
        text=(f"{Ejn} <b>{R(ru,'Реферальная программа','Referral Program')}</b>\n\n"
              f"<blockquote>{Epct} {R(ru,'Приглашайте друзей - 3% с каждой их сделки!','Invite friends - 3% from each deal!')}\n\n"
              f"{Eu} {R(ru,'Приглашено','Invited')}: <b>{rc}</b>\n"
              f"{Ebal} {R(ru,'Заработано','Earned')}: <b>{re} RUB</b>{refs_str}</blockquote>\n\n"
              f"{Esrk} {R(ru,'Ваша ссылка:','Your link:')}\n<code>{ref_link}</code>")
        await send_section(update,text,InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")]]),section="ref")
    except Exception as e: logger.error(f"show_ref: {e}")

async def show_req(update, context):
    try:
        db=load_db(); uid=update.effective_user.id; u=get_user(db,uid)
        lang=get_lang(uid); ru=lang=="ru"; reqs=u.get("requisites",{})
        card=reqs.get("card"); ton=reqs.get("ton"); stars=reqs.get("stars")
        bank=card_bank(lang)

        lines=[f"{Ecwn} <b>{R(ru,'Мои реквизиты','My Requisites')}</b>\n"]
        lines.append(f"{Ecrd} <b>{R(ru,'Карта / Телефон','Card / Phone')}:</b>")
        if card:
            if "|" in card:
                card_num,card_bnk=card.split("|",1)
            else:
                card_num=card; card_bnk=bank
            lines.append(f"<blockquote>{R(ru,'Номер','Number')}: <code>{card_num}</code>\n{R(ru,'Банк','Bank')}: {card_bnk}</blockquote>")
        else:
            lines.append(f"<blockquote>{R(ru,'Не добавлена','Not added')}</blockquote>")
        lines.append(f"\n{Eton} <b>TON:</b>")
        lines.append(f"<blockquote><code>{ton}</code></blockquote>" if ton else f"<blockquote>{R(ru,'Не добавлен','Not added')}</blockquote>")
        lines.append(f"\n{Est} <b>{R(ru,'Звёзды','Stars')}:</b>")
        lines.append(f"<blockquote><code>{stars}</code></blockquote>" if stars else f"<blockquote>{R(ru,'Не добавлен','Not added')}</blockquote>")

        rows=[]
        if card:
            rows.append([InlineKeyboardButton(R(ru,"Изменить карту","Edit card"),callback_data="req_edit_card",icon_custom_emoji_id="5879841310902324730"),
                         InlineKeyboardButton(R(ru,"Удалить карту","Delete card"),callback_data="req_del_card",icon_custom_emoji_id="5904542823167824187")])
        else:
            rows.append([InlineKeyboardButton(R(ru,"Добавить карту / телефон","Add card / phone"),callback_data="req_edit_card",icon_custom_emoji_id="5902056028513505203")])
        if ton:
            rows.append([InlineKeyboardButton(R(ru,"Изменить TON","Edit TON"),callback_data="req_edit_ton",icon_custom_emoji_id="5879841310902324730"),
                         InlineKeyboardButton(R(ru,"Удалить TON","Delete TON"),callback_data="req_del_ton",icon_custom_emoji_id="5904542823167824187")])
        else:
            rows.append([InlineKeyboardButton(R(ru,"Добавить TON","Add TON"),callback_data="req_edit_ton",icon_custom_emoji_id="5397829221605191505")])
        if stars:
            rows.append([InlineKeyboardButton(R(ru,"Изменить Звёзды","Edit Stars"),callback_data="req_edit_stars",icon_custom_emoji_id="5879841310902324730"),
                         InlineKeyboardButton(R(ru,"Удалить Звёзды","Delete Stars"),callback_data="req_del_stars",icon_custom_emoji_id="5904542823167824187")])
        else:
            rows.append([InlineKeyboardButton(R(ru,"Добавить Звёзды","Add Stars"),callback_data="req_edit_stars",icon_custom_emoji_id="5893034681636491040")])
        rows.append([InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")])
        await send_section(update,"\n".join(lines),InlineKeyboardMarkup(rows),section="req")
    except Exception as e:
        logger.error(f"show_req: {e}", exc_info=True)
        try:
            await update.effective_chat.send_message(
                "Реквизиты временно недоступны. Попробуйте ещё раз.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")]]))
        except: pass

async def show_my_deals(update, context):
    try:
        db=load_db(); uid=str(update.effective_user.id); lang=get_lang(int(uid)); ru=lang=="ru"
        deals={}
        for k,v in db.get("deals",{}).items():
            if (str(v.get("user_id",""))==uid
                or str(v.get("partner_uid",""))==uid
                or str(v.get("buyer_uid",""))==uid
                or str(v.get("seller_uid",""))==uid):
                deals[k]=v
        if not deals:
            await send_section(update,
                f"{Edl} <b>{R(ru,'Мои сделки','My Deals')}\n\n{R(ru,'Пока нет сделок.','No deals yet.')}</b>",
                InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")]]),section="my_deals"); return
        SNAMES={
            "pending":   R(ru,f"{Esrk} Ожидает",  f"{Esrk} Pending"),
            "confirmed": R(ru,f"{Ech} Завершена",  f"{Ech} Completed"),
        }
        lines=[f"{Edl} <b>{R(ru,'Мои сделки','My Deals')} ({len(deals)}):</b>\n"]
        for i,(did,dv) in enumerate(list(deals.items())[-10:],start=1):
            tn=tname(dv.get("type",""),lang)
            cur_d=cur_plain(dv.get("currency",""),lang)
            amt=dv.get("payment_amount") or dv.get("amount")
            s=SNAMES.get(dv.get("status",""),dv.get("status",""))
            lines.append(f"<b>{i}. {tn} · {amt} {cur_d} · {s}</b>")
        await send_section(update,"\n".join(lines),
            InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")]]),section="my_deals")
    except Exception as e: logger.error(f"show_my_deals: {e}")

async def show_top(update, context):
    try:
        lang=get_lang(update.effective_user.id); ru=lang=="ru"
        TOP=[
            ("@xK7***q2",13000,312),("@vR3***p9",11800,286),("@mZ8***t4",10400,251),
            ("@qL2***k7",9200,224),("@hT5***n1",8100,197),("@bW9***x3",6900,165),
            ("@jD4***m6",5700,139),("@yF1***c8",4500,108),("@nP6***z2",3200,76),("@cG3***v5",2100,48)
        ]
        dw=R(ru,"сделок","deals")
        lines=[f"<b>{Ecwn} {R(ru,'Топ продавцов Eldorado GG','Eldorado GG Top Sellers')}</b>\n"]
        for i,(u2,a,dd) in enumerate(TOP):
            medal = Emdl if i<3 else f"{i+1}."
            lines.append(f"<b>{medal} {u2} - ${a} · {dd} {dw}</b>")
        lines.append(f"\n<b>{CF} {R(ru,'39.000+ сделок · оборот $370.000','39,000+ deals · $370,000 turnover')}</b>")
        await send_section(update,"\n".join(lines),
            InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="main_menu",icon_custom_emoji_id="5258084656674250503")]]),section="top")
    except Exception as e: logger.error(f"show_top: {e}")

async def show_withdraw(update, context):
    try:
        db=load_db(); uid=update.effective_user.id; u=get_user(db,uid)
        lang=get_lang(uid); ru=lang=="ru"; bal=u.get("balance",0)
        if bal<=0:
            await send_section(update,
                f"{Ewrn} <b>{R(ru,'Недостаточно средств.','Insufficient balance.')}</b>\n\n<blockquote>{R(ru,'Баланс','Balance')}: {bal} RUB</blockquote>",
                InlineKeyboardMarkup([[InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_balance",icon_custom_emoji_id="5258084656674250503")]]),section="balance"); return
        reqs=u.get("requisites",{})
        rows=[]
        if reqs.get("ton"): rows.append([InlineKeyboardButton("TON/USDT → "+reqs["ton"][:12]+"...",callback_data="withdraw_crypto",icon_custom_emoji_id="5409321884074419506")])
        else: rows.append([InlineKeyboardButton("TON / USDT",callback_data="withdraw_crypto",icon_custom_emoji_id="5409321884074419506")])
        if reqs.get("stars"): rows.append([InlineKeyboardButton(R(ru,"Звёзды → ","Stars → ")+reqs["stars"],callback_data="withdraw_stars",icon_custom_emoji_id="5893034681636491040")])
        else: rows.append([InlineKeyboardButton(R(ru,"Звёзды","Stars"),callback_data="withdraw_stars",icon_custom_emoji_id="5893034681636491040")])
        if reqs.get("card"): rows.append([InlineKeyboardButton(R(ru,"Карта → ","Card → ")+reqs["card"][:12]+"...",callback_data="withdraw_card",icon_custom_emoji_id="5902056028513505203")])
        else: rows.append([InlineKeyboardButton(R(ru,"Карта / Телефон","Card / Phone"),callback_data="withdraw_card",icon_custom_emoji_id="5902056028513505203")])
        rows.append([InlineKeyboardButton(R(ru,"Назад","Back"),callback_data="menu_balance",icon_custom_emoji_id="5258084656674250503")])
        await send_section(update,
            f"{Ewlt} <b>{R(ru,'Вывод средств','Withdraw')}</b>\n\n<blockquote>{Ebal} {R(ru,'Баланс','Balance')}: {bal} RUB</blockquote>",
            InlineKeyboardMarkup(rows),section="balance")
    except Exception as e: logger.error(f"show_withdraw: {e}")

# ─── Admin ────────────────────────────────────────────────────────────────────
def adm_kb():
    db=load_db(); hidden=db.get("log_hidden",False)
    tl="Логи: открыты" if not hidden else "Логи: скрыты"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Управление пользователем",callback_data="adm_user")],
        [InlineKeyboardButton("Баннеры",callback_data="adm_banners")],
        [InlineKeyboardButton("Описание меню",callback_data="adm_menu_desc")],
        [InlineKeyboardButton("Список сделок",callback_data="adm_deals")],
        [InlineKeyboardButton("Логи",callback_data="adm_logs"),InlineKeyboardButton(tl,callback_data="adm_toggle_hidden")],
        [InlineKeyboardButton("Лог-канал",callback_data="adm_log_channel"),InlineKeyboardButton("Шаблоны логов",callback_data="adm_log_templates")],
    ])

def adm_banners_kb(db=None):
    if db is None: db=load_db()
    banners=db.get("banners",{})
    rows=[]
    for key,name in BANNER_SECTIONS.items():
        b=banners.get(key) or {}
        has=bool(b.get("photo") or b.get("video") or b.get("gif") or b.get("text"))
        if not has and key=="main":
            has=bool(db.get("banner_photo") or db.get("banner_video") or db.get("banner_gif") or db.get("banner"))
        status="+" if has else "-"
        rows.append([
            InlineKeyboardButton(f"{status} {name}",callback_data=f"adm_banner_{key}"),
            InlineKeyboardButton("X",callback_data=f"adm_banner_del_{key}") if has else InlineKeyboardButton(" ",callback_data="noop"),
        ])
    rows.append([InlineKeyboardButton("Назад",callback_data="adm_back")])
    return InlineKeyboardMarkup(rows)

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message: return
    if update.effective_user.id not in ADMIN_IDS: return
    context.user_data.clear(); context.user_data["adm"]=True
    await update.message.reply_text(f"{Edl} <b>Панель администратора</b>",parse_mode="HTML",reply_markup=adm_kb())

async def handle_adm_cb(update, context):
    try:
        q=update.callback_query; d=q.data; ud=context.user_data
        if update.effective_user.id not in ADMIN_IDS: return

        if d=="adm_user":
            ud["adm_step"]="get_user"
            await q.message.edit_text("<b>Введите @юзернейм или числовой ID:</b>",parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="adm_back")]])); return

        if d=="adm_banners":
            await q.message.edit_text(f"{Egft} <b>Баннеры</b>\n\n<blockquote>+ есть / - нет / X удалить</blockquote>",
                parse_mode="HTML",reply_markup=adm_banners_kb()); return

        if d.startswith("adm_banner_del_"):
            section=d[15:]
            if section in BANNER_SECTIONS:
                db=load_db()
                if not db.get("banners"): db["banners"]={}
                db["banners"][section]={}
                if section=="main": db["banner"]=db["banner_photo"]=db["banner_video"]=db["banner_gif"]=None
                save_db(db); await q.answer("Удалено")
                await q.message.edit_text(f"{Egft} <b>Баннеры</b>",parse_mode="HTML",reply_markup=adm_banners_kb()); return

        if d.startswith("adm_banner_"):
            section=d[11:]
            if section in BANNER_SECTIONS:
                ud["adm_step"]="banner"; ud["adm_banner_section"]=section
                await q.message.edit_text(f"<b>Баннер {BANNER_SECTIONS[section]}\n\nОтправьте фото/видео/GIF/текст. off - удалить.</b>",
                    parse_mode="HTML",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена",callback_data="adm_banners")]])); return

        if d=="adm_log_channel":
            db=load_db(); ci=db.get("log_chat_id","не задан"); lh=db.get("log_hidden",False)
            eg=db.get("extra_group_id","не задан"); ms="Скрыто" if lh else "Открыто"
            await q.message.edit_text(
                f"{Ebl} <b>Лог-канал</b>\n\n<blockquote>Chat ID: <code>{ci}</code>\nДанные: {ms}\n\nДоп. группа (мамонтята): <code>{eg}</code></blockquote>\n\nОтправьте новый chat_id лог-канала:",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Открыть" if lh else "Скрыть",callback_data="adm_log_toggle_mask")],
                    [InlineKeyboardButton("Доп. группа",callback_data="adm_set_extra_group")],
                    [InlineKeyboardButton("Назад",callback_data="adm_back")]
                ]))
            ud["adm_step"]="set_log_chat"; return

        if d=="adm_set_extra_group":
            ud["adm_step"]="set_extra_group"
            db=load_db(); eg=db.get("extra_group_id","не задан")
            await q.message.edit_text(
                f"<b>Дополнительная группа для мамонтят</b>\n\n<blockquote>Текущий ID: <code>{eg}</code></blockquote>\n\nОтправьте chat_id группы (или <code>off</code> для отключения):",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="adm_log_channel")]])); return

        if d=="adm_log_toggle_mask":
            db=load_db(); db["log_hidden"]=not db.get("log_hidden",False); save_db(db)
            lh=db["log_hidden"]; ci=db.get("log_chat_id","не задан"); ms="Скрыто" if lh else "Открыто"
            await q.message.edit_text(
                f"{Ebl} <b>Лог-канал</b>\n\n<blockquote>Chat ID: <code>{ci}</code>\nДанные: {ms}</blockquote>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Открыть" if lh else "Скрыть",callback_data="adm_log_toggle_mask")],
                    [InlineKeyboardButton("Назад",callback_data="adm_back")]
                ]))
            await q.answer("OK"); return

        if d=="adm_toggle_hidden":
            db=load_db(); db["log_hidden"]=not db.get("log_hidden",False); save_db(db)
            await q.answer("Скрыто" if db["log_hidden"] else "Открыто")
            try: await q.message.edit_text(f"{Edl} <b>Панель администратора</b>",parse_mode="HTML",reply_markup=adm_kb())
            except: pass
            return

        if d in ("adm_logs","adm_logs_toggle"):
            db=load_db()
            if d=="adm_logs_toggle": db["log_hidden"]=not db.get("log_hidden",False); save_db(db)
            hidden=db.get("log_hidden",False); logs=db.get("logs",[])[-20:][::-1]
            if not logs:
                await q.message.edit_text("<b>Логов нет.</b>",parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="adm_back")]])); return
            st="Скрыты" if hidden else "Открыты"
            lines=[f"<b>События | {st}:</b>\n"]
            for log in logs:
                if hidden:
                    un=mask_str(f"@{log['username']}") if log.get('username') else ""
                    us=mask_str(log['uid']) if log.get('uid') else ""
                    deal=" #***" if log.get('deal_id') else ""
                else:
                    un=f"@{log['username']}" if log.get('username') else ""
                    us=f"<code>{log['uid']}</code>" if log.get('uid') else ""
                    deal=f" #{log['deal_id']}" if log.get('deal_id') else ""
                ex=f" - {log['extra']}" if log.get('extra') else ""
                lines.append(f"<b>{log['time']}</b> {log['event']}{deal}\n{un} {us}{ex}\n")
            txt="\n".join(lines)[:4000]; tl2="Открыть" if hidden else "Скрыть"
            await q.message.edit_text(txt,parse_mode="HTML",reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton(tl2,callback_data="adm_logs_toggle")],
                [InlineKeyboardButton("Обновить",callback_data="adm_logs")],
                [InlineKeyboardButton("Назад",callback_data="adm_back")]
            ])); return

        if d=="adm_menu_desc":
            ud["adm_step"]="menu_desc"
            await q.message.edit_text("<b>Введите новое описание меню:</b>",parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена",callback_data="adm_back")]])); return

        if d=="adm_log_labels":
            db=load_db(); ll2=db.get("log_labels",{})
            rows_ll=[]
            for lk,lname in [("deal","Название строки Сделка"),("user","Название строки Пользователь"),("extra","Название строки Доп.инфо")]:
                cur_v=ll2.get(lk,"-")
                rows_ll.append([InlineKeyboardButton(f"{lname}: {cur_v}",callback_data=f"adm_ll_edit_{lk}")])
            rows_ll.append([InlineKeyboardButton("Назад",callback_data="adm_log_templates")])
            await q.message.edit_text("<b>Названия строк в логах</b>\n\nНастройте как называются строки в логах:",
                parse_mode="HTML",reply_markup=InlineKeyboardMarkup(rows_ll)); return

        if d.startswith("adm_ll_edit_"):
            lk=d[12:]; ud["adm_step"]="ll_edit"; ud["adm_ll_key"]=lk
            lnames={"deal":"Сделка","user":"Пользователь","extra":"Доп.инфо"}
            await q.message.edit_text(f"<b>Название строки: {lnames.get(lk,lk)}</b>\n\nВведите новое название или <code>off</code> для сброса:",
                parse_mode="HTML",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена",callback_data="adm_log_labels")]])); return

        if d=="adm_log_templates":
            db=load_db(); lt=db.get("log_templates",{})
            rows=[]
            events=["Новая сделка","Покупатель открыл сделку","Оплачено","Подтверждено","Новый реферал","Баланс выдан"]
            lb=db.get("log_banners",{})
            for ev in events:
                has_t="+" if lt.get(ev) else "-"
                has_b="B+" if lb.get(ev) else "B"
                rows.append([
                    InlineKeyboardButton(f"{has_t} {ev}",callback_data=f"adm_lt_edit_{ev}"),
                    InlineKeyboardButton(has_b,callback_data=f"adm_lt_banner_{ev}"),
                ])
            rows.append([InlineKeyboardButton("Названия строк",callback_data="adm_log_labels")])
            rows.append([InlineKeyboardButton("Назад",callback_data="adm_back")])
            await q.message.edit_text(
                "<b>Шаблоны логов</b>\n\n<blockquote>Переменные: {user} {deal} {extra} {time}</blockquote>\n+ = задан  - = нет шаблона  B = баннер",
                parse_mode="HTML",reply_markup=InlineKeyboardMarkup(rows)); return

        if d.startswith("adm_lt_edit_"):
            event_name=d[12:]
            ud["adm_step"]="lt_edit"; ud["adm_lt_event"]=event_name
            db=load_db(); cur_tmpl=db.get("log_templates",{}).get(event_name,"")
            await q.message.edit_text(
                f"<b>Шаблон для: {event_name}</b>\n\n<blockquote>Текущий:\n{cur_tmpl or 'стандартный'}</blockquote>\n\nВведите новый шаблон или <code>off</code> для сброса:\n<code>Переменные: {{user}} {{deal}} {{extra}} {{time}}</code>",
                parse_mode="HTML",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена",callback_data="adm_log_templates")]])); return

        if d.startswith("adm_lt_banner_"):
            event_name=d[14:]
            ud["adm_step"]="lt_banner"; ud["adm_lt_event"]=event_name
            await q.message.edit_text(
                f"<b>Баннер для лога: {event_name}</b>\n\nОтправьте фото/видео/GIF или <code>off</code> для удаления",
                parse_mode="HTML",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена",callback_data="adm_log_templates")]])); return

        if d=="adm_deals":
            db=load_db(); deals=db.get("deals",{})
            if not deals:
                await q.message.edit_text("<b>Сделок нет.</b>",parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="adm_back")]])); return
            text="<b>Последние 10 сделок:</b>\n"
            for did,dv in list(deals.items())[-10:]:
                text+=f"\n<b>{did}</b> | {tname(dv.get('type',''))} | {dv.get('amount')} {dv.get('currency')} | {dv.get('status')}"
            await q.message.edit_text(text,parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="adm_back")]])); return

        am={"adm_add_review":("review","Введите текст отзыва:"),
            "adm_set_deals":("total_deals","Введите количество сделок:"),
            "adm_set_success":("success_deals","Введите количество успешных сделок:"),
            "adm_set_turnover":("turnover","Введите оборот:"),
            "adm_set_rep":("reputation","Введите репутацию:"),
            "adm_set_status":("status","Введите статус:"),
            "adm_add_bal":("add_balance","Введите сумму для начисления (RUB):"),
            "adm_take_bal":("take_balance","Введите сумму для списания (RUB):")}
        if d in am:
            field,prompt=am[d]; ud["adm_field"]=field; ud["adm_step"]="set_value"
            await q.message.edit_text(f"<b>{prompt}</b>",parse_mode="HTML"); return

        if d=="adm_reviews":
            target=ud.get("adm_target")
            if not target: return
            db=load_db(); u2=db["users"].get(target,{}); uname2=u2.get("username","?")
            revs=u2.get("reviews",[])
            if not revs:
                await q.message.edit_text(f"<b>@{uname2}: отзывов нет</b>",parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="adm_back")]])); return
            lines=[f"{Estr} <b>Отзывы @{uname2} ({len(revs)}):</b>"]; rows2=[]
            for i,r in enumerate(revs):
                lines.append(f"\n{i+1}. {H(r)}")
                rows2.append([InlineKeyboardButton(f"X #{i+1}",callback_data=f"adm_del_rev_{target}_{i}")])
            rows2.append([InlineKeyboardButton("Назад",callback_data="adm_back")])
            await q.message.edit_text("\n".join(lines),parse_mode="HTML",reply_markup=InlineKeyboardMarkup(rows2)); return

        sm={"adm_status_verified":"Проверенный","adm_status_garant":"Гарант",
            "adm_status_caution":"Осторожно","adm_status_scammer":"Мошенник","adm_status_clear":""}
        if d in sm:
            target=ud.get("adm_target")
            if target:
                db=load_db(); u2=db["users"].get(target,{})
                u2["status"]=sm[d]; db["users"][target]=u2; save_db(db)
                await q.answer(f"Статус: {sm[d] or 'убран'}")
                try: await q.edit_message_reply_markup(reply_markup=None)
                except: pass

    except Exception as e: logger.error(f"handle_adm_cb: {e}")

async def handle_adm_msg(update, context):
    try:
        ud=context.user_data; step=ud.get("adm_step")
        if not step: return
        text=update.message.text.strip() if update.message and update.message.text else ""
        db=load_db(); ok_kb=InlineKeyboardMarkup([[InlineKeyboardButton("Панель",callback_data="adm_back")]])

        if step=="set_log_chat":
            c2=text.strip()
            if not c2.lstrip("-").isdigit():
                await update.message.reply_text("<b>Неверный chat ID. Пример: -1001234567890</b>",parse_mode="HTML"); return
            db["log_chat_id"]=c2; save_db(db)
            await update.message.reply_text(f"{Ech} <b>Лог-канал установлен!</b>\n<code>{c2}</code>",parse_mode="HTML",reply_markup=ok_kb)
            ud["adm_step"]=None; return

        if step=="set_extra_group":
            c3=text.strip()
            if c3.lower()=="off":
                db["extra_group_id"]=None; save_db(db)
                await update.message.reply_text(f"{Ech} <b>Доп. группа отключена!</b>",parse_mode="HTML",reply_markup=ok_kb)
            elif not c3.lstrip("-").isdigit():
                await update.message.reply_text("<b>Неверный chat ID. Пример: -1001234567890</b>",parse_mode="HTML"); return
            else:
                db["extra_group_id"]=c3; save_db(db)
                await update.message.reply_text(f"{Ech} <b>Доп. группа установлена!</b>\n<code>{c3}</code>",parse_mode="HTML",reply_markup=ok_kb)
            ud["adm_step"]=None; return

        if step=="get_user":
            uname=text.lstrip("@").lower()
            found=next((k for k,v in db["users"].items() if v.get("username","").lower()==uname),None)
            if not found and text.lstrip("@").isdigit():
                c2=text.lstrip("@"); found=c2 if c2 in db["users"] else None
            if not found:
                sim=[v.get("username","") for v in db["users"].values() if len(uname)>=3 and uname[:3] in v.get("username","").lower() and v.get("username","")]
                hint=f"\n\nПохожие: {', '.join('@'+s for s in sim[:5])}" if sim else f"\n\nВсего: {len(db['users'])}"
                await update.message.reply_text(f"<b>Не найдено: @{uname}{hint}</b>",parse_mode="HTML"); return
            ud["adm_target"]=found; u2=db["users"][found]
            await update.message.reply_text(
                f"<b>@{u2.get('username','-')} (<code>{found}</code>)\n"
                f"Сделок: {u2.get('total_deals',0)} | Реп: {u2.get('reputation',0)}\n"
                f"Баланс: {u2.get('balance',0)} RUB\nСтатус: {H(u2.get('status','-'))}</b>",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Отзыв",callback_data="adm_add_review"),InlineKeyboardButton("Отзывы",callback_data="adm_reviews")],
                    [InlineKeyboardButton("Сделок",callback_data="adm_set_deals"),InlineKeyboardButton("Успешных",callback_data="adm_set_success")],
                    [InlineKeyboardButton("Оборот",callback_data="adm_set_turnover"),InlineKeyboardButton("Репутация",callback_data="adm_set_rep")],
                    [InlineKeyboardButton("Выдать баланс",callback_data="adm_add_bal"),InlineKeyboardButton("Забрать",callback_data="adm_take_bal")],
                    [InlineKeyboardButton("Статус",callback_data="adm_set_status")],
                    [InlineKeyboardButton("Проверенный",callback_data="adm_status_verified"),InlineKeyboardButton("Гарант",callback_data="adm_status_garant")],
                    [InlineKeyboardButton("Осторожно",callback_data="adm_status_caution"),InlineKeyboardButton("Мошенник",callback_data="adm_status_scammer")],
                    [InlineKeyboardButton("Убрать статус",callback_data="adm_status_clear")],
                    [InlineKeyboardButton("Назад",callback_data="adm_back")]
                ]))
            ud["adm_step"]=None; return

        if step=="banner":
            section=ud.get("adm_banner_section","main")
            if not db.get("banners"): db["banners"]={}
            cap=update.message.caption or "" if update.message else ""
            if update.message and update.message.photo:
                db["banners"][section]={"photo":update.message.photo[-1].file_id,"video":None,"gif":None,"text":cap}; save_db(db)
            elif update.message and update.message.animation:
                db["banners"][section]={"photo":None,"video":None,"gif":update.message.animation.file_id,"text":cap}; save_db(db)
            elif update.message and update.message.video:
                db["banners"][section]={"photo":None,"video":update.message.video.file_id,"gif":None,"text":cap}; save_db(db)
            elif text.lower()=="off":
                db["banners"][section]={}
                if section=="main": db["banner"]=db["banner_photo"]=db["banner_video"]=db["banner_gif"]=None
                save_db(db)
            else:
                db["banners"][section]={"photo":None,"video":None,"gif":None,"text":text}; save_db(db)
            ud["adm_step"]=None; ud.pop("adm_banner_section",None)
            await update.message.reply_text(f"{Ech} <b>Баннер {BANNER_SECTIONS.get(section,section)} обновлён!</b>",
                parse_mode="HTML",reply_markup=adm_banners_kb(load_db())); return

        if step=="menu_desc":
            db["menu_description"]=text; save_db(db)
            await update.message.reply_text(f"{Ech} <b>Описание обновлено!</b>",parse_mode="HTML",reply_markup=ok_kb)
            ud["adm_step"]=None; return

        if step=="ll_edit":
            lk=ud.get("adm_ll_key","")
            if not db.get("log_labels"): db["log_labels"]={}
            if text.lower()=="off": db["log_labels"].pop(lk,None)
            else: db["log_labels"][lk]=text
            save_db(db)
            await update.message.reply_text(f"{Ech} <b>Название обновлено!</b>",parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="adm_log_labels")]]))
            ud["adm_step"]=None; return

        if step=="lt_edit":
            event_name=ud.get("adm_lt_event","")
            if not db.get("log_templates"): db["log_templates"]={}
            if text.lower()=="off":
                db["log_templates"].pop(event_name,None)
            else:
                db["log_templates"][event_name]=text
            save_db(db)
            await update.message.reply_text(f"{Ech} <b>Шаблон обновлён!</b>",parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад к шаблонам",callback_data="adm_log_templates")]]))
            ud["adm_step"]=None; return

        if step=="lt_banner":
            event_name=ud.get("adm_lt_event","")
            if not db.get("log_banners"): db["log_banners"]={}
            cap=update.message.caption or "" if update.message else ""
            if update.message and update.message.photo:
                db["log_banners"][event_name]={"photo":update.message.photo[-1].file_id,"video":None,"gif":None}
            elif update.message and update.message.animation:
                db["log_banners"][event_name]={"photo":None,"video":None,"gif":update.message.animation.file_id}
            elif update.message and update.message.video:
                db["log_banners"][event_name]={"photo":None,"video":update.message.video.file_id,"gif":None}
            elif text.lower()=="off":
                db["log_banners"].pop(event_name,None)
            save_db(db)
            await update.message.reply_text(f"{Ech} <b>Баннер лога обновлён!</b>",parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад к шаблонам",callback_data="adm_log_templates")]]))
            ud["adm_step"]=None; return

        if step=="set_value":
            field=ud.get("adm_field"); target=ud.get("adm_target")
            if not field or not target: return
            u2=db["users"].get(target,{})
            if field=="review": u2.setdefault("reviews",[]).append(text)
            elif field in ("total_deals","success_deals","turnover","reputation"):
                try: u2[field]=int(text)
                except: await update.message.reply_text("<b>Введите число!</b>",parse_mode="HTML"); return
            elif field=="add_balance":
                try: amt2=int(text)
                except: await update.message.reply_text("<b>Введите число!</b>",parse_mode="HTML"); return
                u2["balance"]=u2.get("balance",0)+amt2
                add_log(db,"Баланс выдан",uid=target,username=u2.get("username",""),extra=f"+{amt2} RUB")
                try:
                    tl=get_lang(int(target)); tr=tl=="ru"
                    await context.bot.send_message(chat_id=int(target),
                        text=f"{Ech} <b>{R(tr,'Баланс пополнен!','Balance topped up!')}</b>\n<blockquote>+{amt2} RUB</blockquote>",parse_mode="HTML")
                except: pass
            elif field=="take_balance":
                try: amt2=int(text)
                except: await update.message.reply_text("<b>Введите число!</b>",parse_mode="HTML"); return
                u2["balance"]=max(0,u2.get("balance",0)-amt2)
                add_log(db,"Баланс списан",uid=target,username=u2.get("username",""),extra=f"-{amt2} RUB")
            else: u2[field]=text
            db["users"][target]=u2; save_db(db)
            await update.message.reply_text(f"{Ech} <b>Обновлено! Баланс: {u2.get('balance',0)} RUB</b>",parse_mode="HTML",reply_markup=ok_kb)
            ud["adm_step"]=None; return

    except Exception as e: logger.error(f"handle_adm_msg: {e}")

# ─── Extra commands ───────────────────────────────────────────────────────────
async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_user.id not in ADMIN_IDS: return
        args=context.args
        if not args: await update.message.reply_text("<b>Пример: /buy GD00001</b>",parse_mode="HTML"); return
        deal_id=args[0].upper(); db=load_db()
        if deal_id not in db.get("deals",{}): await update.message.reply_text("<b>Не найдено.</b>",parse_mode="HTML"); return
        db["deals"][deal_id]["status"]="confirmed"
        s=db["deals"][deal_id].get("user_id")
        if s and s in db["users"]:
            db["users"][s]["success_deals"]=db["users"][s].get("success_deals",0)+1
            db["users"][s]["total_deals"]=db["users"][s].get("total_deals",0)+1
        save_db(db)
        await update.message.reply_text(f"{Ech} <b>Сделка {deal_id} подтверждена!</b>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_buy: {e}")

async def cmd_set_deals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args=context.args
        if not args or not args[0].isdigit(): await update.message.reply_text("<b>Пример: /set_my_deals 100</b>",parse_mode="HTML"); return
        db=load_db(); u=get_user(db,str(update.effective_user.id))
        u["success_deals"]=u["total_deals"]=int(args[0]); save_db(db)
        await update.message.reply_text(f"{Ech} <b>Обновлено!</b>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_set_deals: {e}")

async def cmd_set_amount(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args=context.args
        if not args: await update.message.reply_text("<b>Пример: /set_my_amount 15000</b>",parse_mode="HTML"); return
        try: amt=int(args[0])
        except: await update.message.reply_text("<b>Введите число!</b>",parse_mode="HTML"); return
        db=load_db(); u=get_user(db,str(update.effective_user.id)); u["turnover"]=amt; save_db(db)
        await update.message.reply_text(f"{Ech} <b>Оборот: {amt} RUB</b>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_set_amount: {e}")

async def cmd_add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_user.id not in ADMIN_IDS: return
        args=context.args
        if len(args)<2: await update.message.reply_text("<b>Пример: /add_balance 174415647 500</b>",parse_mode="HTML"); return
        target=args[0].lstrip("@")
        try: amount=int(args[1])
        except: await update.message.reply_text("<b>Сумма должна быть числом!</b>",parse_mode="HTML"); return
        db=load_db()
        if not target.isdigit():
            found=next((k for k,v in db["users"].items() if v.get("username","").lower()==target.lower()),None)
            if not found: await update.message.reply_text("<b>Пользователь не найден.</b>",parse_mode="HTML"); return
            target=found
        u=get_user(db,target); u["balance"]=u.get("balance",0)+amount; save_db(db)
        add_log(db,"Баланс выдан (cmd)",uid=target,username=u.get("username",""),extra=f"+{amount} RUB")
        await update.message.reply_text(f"{Ech} <b>+{amount} RUB → @{u.get('username','?')} (<code>{target}</code>)\nБаланс: {u['balance']} RUB</b>",parse_mode="HTML")
        try:
            tl=get_lang(int(target)); tr=tl=="ru"
            await context.bot.send_message(chat_id=int(target),
                text=f"{Ech} <b>{R(tr,'Баланс пополнен!','Balance topped up!')}</b>\n<blockquote>+{amount} RUB</blockquote>",parse_mode="HTML")
        except: pass
    except Exception as e: logger.error(f"cmd_add_balance: {e}")

async def cmd_take_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.effective_user.id not in ADMIN_IDS: return
        args=context.args
        if len(args)<2: await update.message.reply_text("<b>Пример: /take_balance 174415647 200</b>",parse_mode="HTML"); return
        target=args[0].lstrip("@")
        try: amount=int(args[1])
        except: await update.message.reply_text("<b>Сумма должна быть числом!</b>",parse_mode="HTML"); return
        db=load_db()
        if not target.isdigit():
            found=next((k for k,v in db["users"].items() if v.get("username","").lower()==target.lower()),None)
            if not found: await update.message.reply_text("<b>Пользователь не найден.</b>",parse_mode="HTML"); return
            target=found
        u=get_user(db,target); u["balance"]=max(0,u.get("balance",0)-amount); save_db(db)
        add_log(db,"Баланс списан (cmd)",uid=target,username=u.get("username",""),extra=f"-{amount} RUB")
        await update.message.reply_text(f"{Ech} <b>-{amount} RUB ← @{u.get('username','?')} (<code>{target}</code>)\nБаланс: {u['balance']} RUB</b>",parse_mode="HTML")
    except Exception as e: logger.error(f"cmd_take_balance: {e}")

# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    db=load_db()
    if not db.get("banners"): db["banners"]={}
    lp=db.get("banner_photo"); lv=db.get("banner_video"); lg=db.get("banner_gif"); lt=db.get("banner") or ""
    if (lp or lv or lg or lt) and not db["banners"].get("main"):
        db["banners"]["main"]={"photo":lp,"video":lv,"gif":lg,"text":lt}
        db["banner_photo"]=db["banner_video"]=db["banner_gif"]=db["banner"]=None
        save_db(db)

    app=Application.builder().token(BOT_TOKEN).build()
    async def post_init(application):
        await application.bot.set_my_commands([BotCommand("start","Главное меню")])
        await application.bot.set_my_commands([BotCommand("start","Main menu")], language_code="en")
    app.post_init=post_init

    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CommandHandler("admin",cmd_admin))
    app.add_handler(CommandHandler("neptunteam",cmd_neptune))
    app.add_handler(CommandHandler("sendbalance",cmd_sendbalance))
    app.add_handler(CommandHandler("setdeals",cmd_setdeals))
    app.add_handler(CommandHandler("setturnover",cmd_setturnover))
    app.add_handler(CommandHandler("addrep",cmd_addrep))       # FIX: добавлена регистрация
    app.add_handler(CommandHandler("buy",cmd_buy))
    app.add_handler(CommandHandler("set_my_deals",cmd_set_deals))
    app.add_handler(CommandHandler("set_my_amount",cmd_set_amount))
    app.add_handler(CommandHandler("add_balance",cmd_add_balance))
    app.add_handler(CommandHandler("take_balance",cmd_take_balance))
    app.add_handler(CommandHandler("addreview",cmd_add_review))
    app.add_handler(CommandHandler("delreview",cmd_del_review))
    app.add_handler(CommandHandler("my_reviews",cmd_my_reviews))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND,on_msg))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO | filters.ANIMATION,handle_adm_msg))

    print(f"Bot @{BOT_USERNAME} started!")
    app.run_polling()

if __name__=="__main__":
    main()
