import logging
import asyncio
import re
import time
import gc
import pytz
from datetime import datetime, timedelta
from hydrogram.errors import FloodWait
from hydrogram import enums

from info import ADMINS, IS_PREMIUM, TIME_ZONE
from database.users_chats_db import db

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 🧠 TEMP RUNTIME STORAGE (Central context bucket)
# ─────────────────────────────────────────────
class temp(object):
    START_TIME = 0
    BANNED_USERS, BANNED_CHATS = [], []
    ME, BOT, U_NAME, B_NAME = None, None, None, None
    CANCEL = False 
    FILES, PM_FILES = {}, {}
    USER_SESSIONS, REG_PENDING = {}, {}

# ─────────────────────────────────────────────
# 🛡️ RATE LIMITER UTILITY (Aggressive RAM Flush Sync)
# ─────────────────────────────────────────────
_rate_limits = {}

def is_rate_limited(user_id, action, seconds):
    """हैवी प्रीमियम कमांड्स पर स्पैम रोकता है और रैम को फ़ोर्स फ्लश करता है।"""
    key = f"{user_id}:{action}"
    now = time.time()
    
    # स्मार्ट पीरियोдिक क्लीनअप (Koyeb RAM Safe Protection)
    if len(_rate_limits) > 300: # रैम लीक गार्ड थ्रेशोल्ड लिमिट 300 पर लॉक
        cutoff = now - 60 
        expired_keys = [k for k, v in _rate_limits.items() if v < cutoff]
        for k in expired_keys:
            _rate_limits.pop(k, None)
        # ✅ FIX: अन-रेफ़रेंस्ड ऑब्जेक्ट्स को कोएब की रैम से तुरंत साफ़ करने के लिए गारबेज कलेक्शन
        gc.collect()
            
    if key in _rate_limits and now - _rate_limits[key] < seconds:
        return True
        
    _rate_limits[key] = now
    return False

# ─────────────────────────────────────────────
# 👮 BOT CHAT ADMIN LOOKUP GUARD
# ─────────────────────────────────────────────
async def is_check_admin(bot, chat_id, user_id):
    try:
        return (await bot.get_chat_member(chat_id, user_id)).status in (
            enums.ChatMemberStatus.ADMINISTRATOR, 
            enums.ChatMemberStatus.OWNER
        )
    except: 
        return False

# ─────────────────────────────────────────────
# 🌍 SHARED LOCAL-TIME HELPER (naive datetime, TIME_ZONE-aware)
# ─────────────────────────────────────────────
# ⚠️ नोट: database/users_chats_db.py में भी एक get_local_now() है, लेकिन वो
# tzinfo-AWARE datetime लौटाता है (delete-queue scheduling के लिए)। यह वाला
# जानबूझकर naive (tzinfo-stripped) रखा गया है क्योंकि premium plan के 'expire'
# स्ट्रिंग्स strptime से naive datetime के रूप में पार्स होते हैं और naive vs
# tz-aware datetime compare करने पर Python TypeError देता है। इसलिए दोनों को
# जानबूझकर अलग रखा गया है, मर्ज मत करना।
def get_local_now():
    tz = pytz.timezone(TIME_ZONE)
    return datetime.now(tz).replace(tzinfo=None)

# ─────────────────────────────────────────────
# 💎 PREMIUM AUTO-VALIDATOR (Perfect info.py TIME_ZONE Sync)
# ─────────────────────────────────────────────
async def is_premium(user_id, bot=None):
    # एडमिन को हमेशा लाइफटाइम बाईपास अनलॉक रहेगा
    if not IS_PREMIUM or user_id in ADMINS: 
        return True
        
    mp = await db.get_plan(user_id)
    if not mp.get("premium"): 
        return False
    
    expire = mp.get("expire")
    if expire:
        if isinstance(expire, str):
            try: 
                expire = datetime.strptime(expire, "%Y-%m-%d %H:%M:%S")
            except: 
                expire = None
        
        # ✅ FIX: हार्डकोडिंग हटाकर 'info.py' के कस्टमाइज्ड 'TIME_ZONE' से शुद्ध सिंक कॉम्पैरिजन
        now_local = get_local_now()
        
        if not expire or expire < now_local:
            if bot:
                try: 
                    await bot.send_message(user_id, f"❌ <b>Your Premium Membership Plan has Expired!</b>\n\nContact Admin or use /plan to activate again.")
                except: 
                    pass
            
            # प्रीमियम ख़त्म होते ही डेटाबेस में सारे रिमाइंडर फ़्लैग्स और स्टेटस को तुरंत रिफ्रेश/रीसेट करें
            await db.update_plan(user_id, {
                "expire": None, 
                "plan": "", 
                "premium": False,
                "reminded_12h": False, 
                "reminded_6h": False, 
                "reminded_3h": False, 
                "reminded_1h": False, 
                "reminded_30m": False, 
                "reminded_10m": False,
                "last_reminder_id": 0
            })
            return False
    return True

# ❌ FIX: 'broadcast_messages' (पुराना भारी ब्रॉडकास्ट इंजन कबाड़) पूरी तरह डिलीटेड। 
# इससे नो-रिपिटिशन रूल और क्लीन आर्किटेक्चर लागू होता है।

# ─────────────────────────────────────────────
# ⚙️ TTL SETTINGS CACHE (Bounded Memory Leak Proof)
# ─────────────────────────────────────────────
_settings_cache = {}
_CACHE_TTL = 300 

async def get_settings(group_id):
    now = time.time()
    if group_id in _settings_cache:
        data, ts = _settings_cache[group_id]
        if now - ts < _CACHE_TTL:
            return data
            
    data = await db.get_settings(group_id)
    
    # ✅ FIX: इन-मेमोरी डिक्शनरी को अनकैप्ड बढ़ने से रोकने के लिए बाउंडेड कैशे गार्ड
    if len(_settings_cache) > 200:
        _settings_cache.clear()
        gc.collect()
        
    _settings_cache[group_id] = (data, now)
    return data

async def save_group_settings(group_id, key, value):
    data = await get_settings(group_id)
    data[key] = value
    _settings_cache[group_id] = (data, time.time())
    await db.update_settings(group_id, data)

# ─────────────────────────────────────────────
# 📦 FORMATTING & RAIN TIME UTILS
# ─────────────────────────────────────────────
def get_size(size):
    units = ["Bytes", "KB", "MB", "GB", "TB"]
    size, i = float(size), 0
    while size >= 1024 and i < 4:
        size, i = size / 1024, i + 1
    return f"{size:.2f} {units[i]}"

def get_readable_time(seconds):
    res, periods = "", [('d', 86400), ('h', 3600), ('m', 60), ('s', 1)]
    for name, sec in periods:
        if seconds >= sec:
            val, seconds = divmod(seconds, sec)
            res += f"{int(val)}{name} "
    return res.strip() or "0s"

def get_duration(seconds):
    """मीडिया की लंबाई (seconds) को वीडियो-स्टाइल टाइमकोड में बदलता है —
    1h+ के लिए '1:23:45', वरना '23:45'। Telegram के Video/Audio ऑब्जेक्ट से
    मिला `duration` int होता है; पुराने/नॉन-मीडिया डॉक्युमेंट्स में यह फील्ड
    मौजूद ही नहीं होती, इसलिए गलत/खाली/नेगेटिव इनपुट पर '' लौटता है
    (ताकि UI में '0:00' जैसा बेकार टेक्स्ट न छपे)।"""
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return ""
    if seconds <= 0:
        return ""
    hours, rem = divmod(seconds, 3600)
    mins, secs = divmod(rem, 60)
    return f"{hours}:{mins:02d}:{secs:02d}" if hours else f"{mins}:{secs:02d}"

def get_media_meta(file_doc, sep=" • "):
    """DB डॉक्युमेंट (या किसी भी dict) से size + duration का तैयार डिस्प्ले
    टेक्स्ट बनाता है — जैसे '1.20 GB • 2:14:09'। duration न हो तो सिर्फ़ size।
    एक ही जगह लिखा गया है ताकि Telegram बटन-लिस्ट, कैप्शन टेम्पलेट और web
    कार्ड्स सब जगह एक जैसा फॉर्मेट दिखे (डुप्लिकेशन नहीं)।"""
    if not isinstance(file_doc, dict):
        file_doc = {}
    size = get_size(file_doc.get("file_size", 0) or 0)
    dur = get_duration(file_doc.get("duration"))
    return f"{size}{sep}{dur}" if dur else size

def get_wish():
    # ✅ FIX: कचरा टेक्स्ट और अशुद्धियों को हटाकर कस्टमाइज्ड टाइमज़ोन विश इंजन सिंक किया गया
    tz = pytz.timezone(TIME_ZONE)
    h = datetime.now(tz).hour
    return "ɢᴏᴏᴅ ᴍᴏʀɴɪɴɢ 🌞" if h < 12 else "ɢᴏᴏᴅ ᴀꜰᴛᴇʀɴᴏᴏɴ 🌗" if h < 18 else "ɢᴏᴏᴅ ᴇᴠᴇɴɪɴɢ 🌘"

async def get_seconds(time_string):
    # ✅ BUG FIX: पहले सिर्फ़ पूरे शब्द (min/hour/day) ही मान्य थे, आम shorthand
    # जैसे "1m"/"1h"/"1d" (जो admins आमतौर पर टाइप करते हैं, और जो खुद
    # get_readable_time() भी आउटपुट में इस्तेमाल करता है) रेगेक्स से मैच ही नहीं
    # होते थे — चुपचाप 0 return होता, जिससे "5m" वाला default delay लग जाता और
    # keyword में गलती से यूज़र का दिया समय (जैसे "1m") जुड़ जाता।
    # ⚠️ ऑर्डर ज़रूरी है: लंबे शब्द ("month","hour") पहले लिखे हैं ताकि रेगेक्स
    # उन्हें छोटे prefix ("m","h") के तौर पर गलती से आधा-अधूरा मैच न कर ले।
    match = re.match(
        r"(\d+)\s*(month|min|mo|m|hour|hr|h|day|d|year|y|sec|s)",
        time_string.strip(),
        re.IGNORECASE
    )
    if not match:
        return 0
    unit = match.group(2).lower()
    return int(match.group(1)) * {
        "s": 1, "sec": 1,
        "m": 60, "min": 60,
        "h": 3600, "hr": 3600, "hour": 3600,
        "d": 86400, "day": 86400,
        "mo": 2592000, "month": 2592000,
        "y": 31536000, "year": 31536000,
    }.get(unit, 0)

# 🛠️ PREMIUM LIFECYCLE TIME PARSERS
def parse_expire_time(e):
    if isinstance(e, datetime): 
        return e
    try: 
        return datetime.strptime(e, "%Y-%m-%d %H:%M:%S") if e else None
    except: 
        return None

def get_ist_str(dt):
    if not dt:
        return "Unknown"
    # dt already local (naive), just format
    try:
        return dt.strftime("%d %B %Y, %I:%M %p")
    except:
        return str(dt)

async def safe_del(c, cid, mids):
    try: 
        await c.delete_messages(cid, mids)
    except: 
        pass
