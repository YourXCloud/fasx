import logging
import hashlib
import random
import copy
from datetime import datetime, timedelta
import pytz
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import DuplicateKeyError
from info import (DATABASE_URL, DATABASE_NAME, FILE_CAPTION, 
                  SPELL_CHECK, PROTECT_CONTENT, AUTO_DELETE, TIME_ZONE)

logger = logging.getLogger(__name__)

# =========================================
# 🌍 TIMEZONE HELPER ENGINE
# =========================================
def get_local_now():
    """info.py के TIME_ZONE के अनुसार लाइव लोकल टाइम देता है"""
    tz = pytz.timezone(TIME_ZONE)
    return datetime.now(tz)

# =========================================
# 🌐 WEB AUTHENTICATION DATABASE (RAM Protected)
# =========================================
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

class WebAuthDB:
    def __init__(self, db):
        self.col = db["web_users"] 
        
    async def create_user(self, tg_id, email, password):
        if await self.col.find_one({"$or": [{"tg_id": tg_id}, {"email": email}]}, {"_id": 1}):
            return False, "Telegram ID or Email already registered!"
        user_data = {
            "tg_id": tg_id,
            "email": email,
            "password": hash_password(password),
            "joined_date": get_local_now()
        }
        try:
            await self.col.insert_one(user_data)
        except DuplicateKeyError:
            return False, "Telegram ID or Email already registered!"
        return True, "Account Created Successfully!"

    async def verify_login(self, email, password):
        # वेबसाइट लॉगिन सिक्योरिटी ट्यूनिंग
        return await self.col.find_one({"email": email, "password": hash_password(password)})

    async def update_profile(self, tg_id, new_email, new_password=None):
        update_data = {"email": new_email}
        if new_password:
            update_data["password"] = hash_password(new_password)
        await self.col.update_one({"tg_id": tg_id}, {"$set": update_data})

    async def generate_otp(self, tg_id):
        user = await self.col.find_one({"tg_id": tg_id}, {"_id": 1})
        if not user: return None
        
        otp = str(random.randint(100000, 999999))
        expiry = get_local_now() + timedelta(minutes=10)
        await self.col.update_one({"tg_id": tg_id}, {"$set": {"otp": otp, "otp_expiry": expiry}})
        return otp

    async def verify_otp_and_reset(self, tg_id, otp, new_password):
        user = await self.col.find_one({"tg_id": tg_id, "otp": otp}, {"otp_expiry": 1})
        if user and user.get("otp_expiry", get_local_now()) > get_local_now():
            await self.col.update_one(
                {"tg_id": tg_id}, 
                {"$set": {"password": hash_password(new_password)}, "$unset": {"otp": "", "otp_expiry": ""}}
            )
            return True
        return False


# =========================================
# 🤖 BOT & MAIN DATABASE — RAM & Security Guarded
# =========================================
class Database:
    def __init__(self):
        # ✅ MOTOR ENGINE: कोएब आइडल थ्रॉटलिंग और कनेक्शन DROP से सुरक्षित ट्यूनिंग
        self.client = AsyncIOMotorClient(
            DATABASE_URL, 
            minPoolSize=0,            # आइडल टाइम पर 0 कनेक्शन (RAM 100% सेफ)
            maxPoolSize=15,           # हैवी ट्रैफिक के लिए 15 कनेक्शंस पूल
            maxIdleTimeMS=30000,      # 30 सेकंड बाद कनेक्शन ऑटो-कूलडाउन
            serverSelectionTimeoutMS=5000
        )
        self.db = self.client[DATABASE_NAME]
        
        # Collections
        self.users, self.groups, self.premium = self.db.Users, self.db.Groups, self.db.Premiums
        self.settings, self.warns = self.db.Settings, self.db.Warns
        self.delete_queue = self.db.AutoDeleteQueue 

    async def _ensure_indexes(self):
        # बोट कलेक्शंस के लिए इंडेक्स सिंक
        for col in [self.users, self.groups, self.premium, self.settings]:
            try: 
                await col.create_index("id", unique=True)
            except Exception as e: 
                logger.warning(f"Index warn: {e}")
        
        # ✅ NEW UPGRADE: WebAuthDB (web_users) के लिए स्ट्रिक्ट यूनिक इंडेक्सेस ट्यूनिंग (रैम और COLLSCAN से सुरक्षा)
        try:
            web_users = self.db["web_users"]
            await web_users.create_index("tg_id", unique=True)
            await web_users.create_index("email", unique=True)
            logger.info("✅ Web Auth unique indexes (tg_id, email) initialized successfully.")
        except Exception as e:
            if "already exists" not in str(e):
                logger.warning(f"Web Auth Index warn: {e}")
        
        # अगर डेटाबेस कतार में पुरानी 'id_1' अवैध इंडेक्स मौजूद है, तो उसे पूरी तरह से डिस्ट्रॉय करो
        try:
            await self.delete_queue.drop_index("id_1")
            logger.info("🗑️ Old defective unique index 'id_1' dropped from AutoDeleteQueue.")
        except Exception:
            pass # अगर पहले से मौजूद नहीं है तो सेफ बाईपास

        # ऑटो-डिलीट कतार के लिए इंडेक्स सिंक
        try:
            await self.delete_queue.create_index([("delete_at", 1)])
        except: pass

    # ⚙️ Default Global Settings Config
    df_set = {"file_secure": PROTECT_CONTENT, "spell_check": SPELL_CHECK, "auto_delete": AUTO_DELETE, "caption": FILE_CAPTION, "search_enabled": True, "blacklist": [], "dlink": {}, "notes": {}}
    
    df_prm = {
        "expire": None, 
        "trial": False, 
        "plan": "", 
        "premium": False, 
        "reminded_12h": False, 
        "reminded_6h": False, 
        "reminded_3h": False, 
        "reminded_1h": False, 
        "reminded_30m": False, 
        "reminded_10m": False,
        "last_reminder_id": 0
    }
    
    df_ban = {"is_banned": False, "ban_reason": ""}
    df_chat = {"is_disabled": False, "reason": ""}

    # ───────────────── USERS (Strict Premium Model) ─────────────────
    async def add_user(self, uid, name): 
        await self.users.update_one({"id": int(uid)}, {"$set": {"name": name}, "$setOnInsert": {"ban_status": self.df_ban}}, upsert=True)
        
    async def is_user_exist(self, uid): 
        return bool(await self.users.find_one({"id": int(uid)}, {"_id": 1}))
        
    async def total_users_count(self): 
        return await self.users.count_documents({})
    
    # 'get_all_users' (पुराना ब्रॉडकास्ट कर्सर) पूरी तरह हटा दिया गया है ताकि रैम लीक न हो।
    
    async def delete_user(self, uid): 
        await self.users.delete_many({"id": int(uid)})
    
    async def ban_user(self, uid, rsn="No Reason"): 
        await self.users.update_one({"id": int(uid)}, {"$set": {"ban_status": {"is_banned": True, "ban_reason": rsn}}}, upsert=True)
        
    async def unban_user(self, uid): 
        await self.users.update_one({"id": int(uid)}, {"$set": {"ban_status": self.df_ban}})
        
    async def get_ban_status(self, uid): 
        return (await self.users.find_one({"id": int(uid)}, {"ban_status": 1}) or {}).get("ban_status", self.df_ban)

    # ───────────────── GROUPS ─────────────────
    async def add_chat(self, gid, title): 
        await self.groups.update_one({"id": int(gid)}, {"$set": {"title": title}, "$setOnInsert": {"settings": self.df_set, "chat_status": self.df_chat}}, upsert=True)
        
    async def get_chat(self, gid): 
        return (await self.groups.find_one({"id": int(gid)}, {"chat_status": 1}) or {}).get("chat_status", None)
        
    async def total_chat_count(self): 
        return await self.groups.count_documents({})
    
    async def get_all_chats(self): 
        return self.groups.find({}, {"id": 1})
    
    async def disable_chat(self, gid, rsn="No Reason"): 
        await self.groups.update_one({"id": int(gid)}, {"$set": {"chat_status": {"is_disabled": True, "reason": rsn}}})
        
    async def re_enable_chat(self, gid): 
        await self.groups.update_one({"id": int(gid)}, {"$set": {"chat_status": self.df_chat}})

    # ───────────────── SETTINGS & INLINE UI MGMT ─────────────────
    async def update_settings(self, gid, st): 
        await self.groups.update_one({"id": int(gid)}, {"$set": {"settings": st}}, upsert=True)
        
    async def get_settings(self, gid): 
        base = copy.deepcopy(self.df_set)
        db_settings = ((await self.groups.find_one({"id": int(gid)}, {"settings": 1})) or {}).get("settings", {})
        base.update(db_settings)
        return base
    
    async def get_warn(self, uid, cid): 
        return await self.warns.find_one({"user_id": uid, "chat_id": cid}, {"count": 1}) or {"count": 0}
        
    async def set_warn(self, uid, cid, data): 
        await self.warns.update_one({"user_id": uid, "chat_id": cid}, {"$set": data}, upsert=True)
        
    async def clear_warn(self, uid, cid): 
        await self.warns.delete_one({"user_id": uid, "chat_id": cid})

    async def get_all_notes(self, cid): 
        return ((await self.groups.find_one({"id": int(cid)}, {"settings.notes": 1})) or {}).get("settings", {}).get("notes", {})
        
    async def save_note(self, cid, name, data): 
        await self.groups.update_one({"id": int(cid)}, {"$set": {f"settings.notes.{name}": data}}, upsert=True)
        
    async def delete_note(self, cid, name): 
        await self.groups.update_one({"id": int(cid)}, {"$unset": {f"settings.notes.{name}": ""}})

    # ───────────────── PREMIUM INTEGRITY SYSTEM ─────────────────
    async def get_plan(self, uid): 
        base = copy.deepcopy(self.df_prm)
        db_status = ((await self.premium.find_one({"id": int(uid)}, {"status": 1})) or {}).get("status", {})
        base.update(db_status)
        return base
        
    async def update_plan(self, uid, data): 
        await self.premium.update_one({"id": int(uid)}, {"$set": {"status": data}}, upsert=True)
        
    async def get_premium_users(self): 
        # प्रीमियम लिस्ट एक्सपोर्ट करते समय केवल काम के फील्ड्स प्रोजेक्ट करें (Zero RAM Overhead)
        return self.premium.find({}, {"id": 1, "status": 1})

    # ───────────────── SECURITY STATS BREAKDOWN ─────────────────
    async def get_banned(self):
        banned_users = [u["id"] async for u in self.users.find({"ban_status.is_banned": True}, {"id": 1})]
        banned_groups = [g["id"] async for g in self.groups.find({"chat_status.is_disabled": True}, {"id": 1})]
        return banned_users, banned_groups

    # ───────────────── ⏳ PERSISTENT AUTO-DELETE QUEUE ENGINE ─────────────────
    async def add_to_delete_queue(self, chat_id, message_id, delay_seconds):
        if not chat_id or not message_id:
            return False 
            
        delete_at = get_local_now() + timedelta(seconds=delay_seconds) # ग्लोबल टाइमज़ोन सिंक
        
        # चैट आईडी और मैसेज आईडी का एकदम यूनिक कॉम्बो स्ट्रिंग बनाओ ताकि 'id: null' का लफड़ा हमेशा के लिए खत्म हो जाए!
        task_id = f"{int(chat_id)}_{int(message_id)}"
        
        await self.delete_queue.update_one(
            {"_id": task_id},
            {
                "$set": {
                    "_id": task_id,
                    "chat_id": int(chat_id),
                    "message_id": int(message_id),
                    "delete_at": delete_at
                }
            },
            upsert=True
        )

    async def get_expired_delete_tasks(self):
        now = get_local_now()
        return self.delete_queue.find({"delete_at": {"$lte": now}})

    async def remove_from_delete_queue(self, chat_id, message_id):
        # डिलीट टास्क रिमूव करते समय भी सीधे यूनिक कॉम्बो की को टारगेट करें
        task_id = f"{int(chat_id)}_{int(message_id)}"
        await self.delete_queue.delete_one({"_id": task_id})

    # ───────────────── 🎥 VIDEO STREAM ANALYTICS COUNTER ─────────────────
    async def track_video_play(self):
        """वेबसाइट / मिनी-ऐप पर स्ट्रीम प्ले होने की ग्लोबल काउंटिंग रिकॉर्ड करता है"""
        try:
            await self.settings.update_one(
                {"id": "global_stream_stats"},
                {"$inc": {"total_web_plays": 1}},
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Error in track_video_play pipeline: {e}")
            return False

    # ───────────────── 👥 LIVE DASHBOARD WEB LOGINERS COUNTERS ─────────────────
    async def get_today_logged_in_users_count(self):
        """आज वेबसाइट पर लॉगिन करने वाले एक्टिव यूज़र्स की संख्या देता है"""
        try:
            from utils import temp
            ram_users = set()
            
            if hasattr(temp, "USER_SESSIONS"):
                import time
                now = time.time()
                for session_id, session_data in temp.USER_SESSIONS.items():
                    if session_data.get("expiry", 0) > now:
                        tid = session_data.get("tg_id")
                        if tid:
                            ram_users.add(tid)

            # 2. Database `web_users` कलेक्शन से पिछले 24 घंटे की लॉगिन हिस्ट्री चेक करें
            # ✅ BUG FIX: पहले यहाँ naive datetime.now() इस्तेमाल होता था, जबकि
            # 'last_login' (login_routes.py में) इसी फाइल के tz-aware get_local_now()
            # से लिखा जाता है। MongoDB tz-aware datetime को असली UTC में convert करके
            # स्टोर करता है, पर naive datetime को ज्यों-का-त्यों UTC मान लेता है —
            # नतीजा IST में करीब साढ़े 5 घंटे का mismatch (गलत count) आता था।
            # अब write और read दोनों जगह वही get_local_now() इस्तेमाल हो रहा है।
            today_start = get_local_now() - timedelta(days=1)
            db_cursor = self.db["web_users"].find({"last_login": {"$gte": today_start}}, {"tg_id": 1})
            async for user in db_cursor:
                tid = user.get("tg_id")
                if tid:
                    ram_users.add(tid)

            return len(ram_users)
        except Exception as e:
            logger.error(f"Error counting logged in users: {e}")
            return 0

    async def get_premium_users_count(self):
        """डेटाबेस में कुल एक्टिव प्रीमियम यूज़र्स की सटीक संख्या देता है"""
        try:
            return await self.premium.count_documents({"status.premium": True})
        except Exception as e:
            logger.error(f"Error counting premium users: {e}")
            return 0

# =========================================
# 🚀 INITIALIZE DATABASES
# =========================================
db = Database()
web_db = WebAuthDB(db.db)
