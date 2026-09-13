import logging
import re
import base64
import asyncio
import time
from struct import pack
from bson.objectid import ObjectId
import motor.motor_asyncio
from hydrogram.file_id import FileId
from hydrogram.errors import FloodWait
from info import DATABASE_URL, DATABASE_NAME, USE_CAPTION_FILTER, DELETE_CHANNEL
from utils import temp

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# ⚙️ MOTOR CONNECTION — Memory-Leak & RAM Guard Optimized
# ─────────────────────────────────────────────────────────
client = motor.motor_asyncio.AsyncIOMotorClient(
    DATABASE_URL,
    maxPoolSize=15,             
    minPoolSize=0,              
    maxIdleTimeMS=30000,        
    serverSelectionTimeoutMS=5000,
    connectTimeoutMS=10000,
    socketTimeoutMS=20000,
    retryWrites=True,
    retryReads=True,
)
db = client[DATABASE_NAME]

primary = db["Primary"]
cloud   = db["Cloud"]
archive = db["Archive"]
actors  = db["Actors"]  # 🎭 एक्टर प्रोफाइल के लिए डेटाबेस कलेक्शन
posts   = db["Posts"]   # 📝 नया पोस्ट्स (CMS) कलेक्शन

COLLECTIONS = {
    "primary": primary,
    "cloud":   cloud,
    "archive": archive,
    "actors":  actors,   
}

# ⚡ GLOBAL STATUS EXPENSIVE COUNT CACHE
_stats_cache = None
_stats_cache_time = 0
STATS_CACHE_TTL = 60  

# ─────────────────────────────────────────────────────────
# ⚡ INDEXES — Dynamic Configuration
# ─────────────────────────────────────────────────────────
async def ensure_indexes():
    # 1. Files & Actors Indexes
    for name, col in COLLECTIONS.items():
        try:
            if name == "actors":
                continue

            if USE_CAPTION_FILTER:
                await col.create_index([("file_name", "text"), ("caption", "text")], name=f"{name}_text")
            else:
                await col.create_index([("file_name", "text")], name=f"{name}_text")
            
            await col.create_index("file_name", name=f"{name}_filename_idx")
            await col.create_index([("added_on", -1)], name=f"{name}_added_on_idx")
            logger.info(f"✅ Fast Search & Non-Bloated Indexes OK: {name}")
        except Exception as e:
            if "already exists" in str(e) or "IndexKeySpecsConflict" in str(e): pass
            else: logger.warning(f"Index warning [{name}]: {e}")

    # 2. Actors Specific Indexes
    try:
        await actors.create_index([("name", "text")], name="actors_name_text")
        logger.info("✅ Actor Profile System Indexes OK")
    except Exception as e:
        if "already exists" not in str(e) and "IndexKeySpecsConflict" not in str(e):
            logger.warning(f"Actor Index warning: {e}")

    # 3. Posts Specific Indexes (NEW)
    try:
        await posts.create_index([("title", "text"), ("tags", "text")], name="posts_search_idx")
        logger.info("✅ Posts CMS Search Indexes OK")
    except Exception as e:
        if "already exists" not in str(e) and "IndexKeySpecsConflict" not in str(e):
            logger.warning(f"Posts Index warning: {e}")

# ─────────────────────────────────────────────────────────
# 📊 DB STATS
# ─────────────────────────────────────────────────────────
async def db_count_documents():
    global _stats_cache, _stats_cache_time
    now = time.time()
    if _stats_cache and (now - _stats_cache_time < STATS_CACHE_TTL):
        return _stats_cache

    try:
        p_task = primary.estimated_document_count()
        c_task = cloud.estimated_document_count()
        a_task = archive.estimated_document_count()
        
        thumb_query = {"thumb_url": {"$exists": True, "$type": "string", "$ne": "NO_THUMB"}}
        pt_task = primary.count_documents(thumb_query)
        ct_task = cloud.count_documents(thumb_query)
        at_task = archive.count_documents(thumb_query)

        p, c, a, pt, ct, at = await asyncio.gather(p_task, c_task, a_task, pt_task, ct_task, at_task)
        
        _stats_cache = {
            "primary": p, "cloud": c, "archive": a, "total": p + c + a,
            "primary_thumb": pt, "cloud_thumb": ct, "archive_thumb": at, "total_thumb": pt + ct + at
        }
        _stats_cache_time = now
        return _stats_cache
    except Exception as e:
        logger.error(f"Count Breakdown error: {e}")
        return {"primary": 0, "cloud": 0, "archive": 0, "total": 0, "primary_thumb": 0, "cloud_thumb": 0, "archive_thumb": 0, "total_thumb": 0}

# ─────────────────────────────────────────────────────────
# 💾 SAVE FILE
# ─────────────────────────────────────────────────────────
async def save_file(media, collection_type="primary"):
    try:
        file_id = unpack_new_file_id(media.file_id)
        if not file_id: return "err"

        f_name  = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(media.file_name or "")).strip()
        caption = re.sub(r"@\w+|(_|\-|\.|\+)", " ", str(media.caption  or "")).strip()
        file_type = type(media).__name__.lower()
        col = COLLECTIONS.get(collection_type, primary)
        
        existing_doc = await col.find_one({"_id": file_id}, {"_id": 1})
        if existing_doc:
            return "dup"

        update_set = {"file_ref":  media.file_id, "file_name": f_name, "file_size": media.file_size, "file_type": file_type}

        update_payload = {"$set": update_set, "$setOnInsert": {"added_on": time.time()}}
        unset_payload = {}

        if USE_CAPTION_FILTER and caption: update_set["caption"] = caption
        else: unset_payload["caption"] = ""

        if unset_payload: update_payload["$unset"] = unset_payload

        await col.update_one({"_id": file_id}, update_payload, upsert=True)
        return "suc"
    except Exception as e:
        logger.error(f"save_file error: {e}")
        return "err"

# ─────────────────────────────────────────────────────────
# 🔍 REGEX BUILDER WITH SHORT-QUERY SHIELD
# ─────────────────────────────────────────────────────────
ALLOWED_SHORT = {"hd", "4k", "3d", "8k", "5.1", "7.1", "kg", "rr", "uhd", "hevc", "x265", "x264"}

def _build_regex(query: str):
    query = query.strip()
    if not query: return None
    q_lower = query.lower()
    
    if len(query) < 2 or (len(query) == 2 and q_lower not in ALLOWED_SHORT): return None
    if ' ' not in query: raw = r'(\b|[\.\+\-_])' + re.escape(query) + r'(\b|[\.\+\-_])'
    else: raw = re.escape(query).replace(r'\ ', r'.*[\s\.\+\-_]')

    try: return re.compile(raw, flags=re.IGNORECASE)
    except Exception: return re.compile(re.escape(query), flags=re.IGNORECASE)

# ─────────────────────────────────────────────────────────
# 🚀 SMART SEARCH
# ─────────────────────────────────────────────────────────
async def _search(col, raw_query: str, regex, offset: int, limit: int, lang=None, bypass_count=False):
    clean_query = raw_query.replace('"', '').replace("'", "").strip()
    words = clean_query.split() if clean_query else []
    strict_query = " ".join(f'"{word}"' for word in words) if words else ""

    if strict_query:
        text_flt = {"$text": {"$search": strict_query}}
        if lang: text_flt = {"$and": [text_flt, {"file_name": re.compile(lang, re.IGNORECASE)}]}

        cursor = col.find(text_flt, {"_id": 1, "file_name": 1, "file_size": 1, "file_type": 1, "file_ref": 1, "caption": 1, "thumb_url": 1, "score": {"$meta": "textScore"}})
        cursor.sort([("score", {"$meta": "textScore"})])
        cursor.skip(offset).limit(limit)
        docs = await cursor.to_list(length=limit)
        if docs:
            for doc in docs: 
                doc["file_id"] = doc["_id"] 
                doc["source_col"] = col.name.lower()
            count = 0 if bypass_count else await col.count_documents(text_flt)
            return docs, count

    if not regex: return [], 0
    reg_flt = {"$or": [{"file_name": regex}, {"caption": regex}]} if USE_CAPTION_FILTER else {"file_name": regex}
    if lang: reg_flt = {"$and": [reg_flt, {"file_name": re.compile(lang, re.IGNORECASE)}]}

    cursor = col.find(reg_flt, {"_id": 1, "file_name": 1, "file_size": 1, "file_type": 1, "file_ref": 1, "caption": 1, "thumb_url": 1}).sort('_id', -1)
    cursor.skip(offset).limit(limit)
    docs = await cursor.to_list(length=limit)
    for doc in docs: 
        doc["file_id"] = doc["_id"]
        doc["source_col"] = col.name.lower()

    count = 0 if bypass_count else (await col.count_documents(reg_flt) if docs else 0)
    return docs, count

async def get_count(col, flt, bypass):
    if bypass: return 1000
    return await col.count_documents(flt)

# ─────────────────────────────────────────────────────────
# 🌐 PUBLIC SEARCH API (NEW UPGRADE: CROSS-COLLECTION MERGE)
# ─────────────────────────────────────────────────────────
async def get_search_results(query, max_results, offset=0, lang=None, collection_type="primary", bypass_count=False, cached_counts=None, counts_out=None):
    if not query: return [], "", 0, collection_type
    raw_query  = str(query).strip()
    regex      = _build_regex(raw_query)

    if not raw_query.replace('"', '').replace("'", "").strip().split() and not regex:
        return [], "", 0, collection_type

    results, total, actual_src = [], 0, collection_type

    if collection_type == "all":
        clean_query = raw_query.replace('"', '').replace("'", "").strip()
        words = clean_query.split() if clean_query else []
        strict_query = " ".join(f'"{word}"' for word in words) if words else ""

        flt = None
        is_text = False

        if strict_query:
            is_text = True
            text_flt = {"$text": {"$search": strict_query}}
            flt = {"$and": [text_flt, {"file_name": re.compile(lang, re.IGNORECASE)}]} if lang else text_flt
        elif regex:
            reg_flt = {"$or": [{"file_name": regex}, {"caption": regex}]} if USE_CAPTION_FILTER else {"file_name": regex}
            flt = {"$and": [reg_flt, {"file_name": re.compile(lang, re.IGNORECASE)}]} if lang else reg_flt

        if not flt: return [], "", 0, collection_type

        # ✅ FIX: filter.py (bot साइड) पहले से computed counts (cached_counts) भेजता है
        # ताकि सिर्फ़ page बदलने पर तीनों collections को दोबारा count_documents ना
        # करना पड़े (fast pagination)। bypass_count यहाँ हमेशा False रहता है (bot
        # हमेशा असली गिनती चाहता है, web अलग से bypass_count=True भेजता है)।
        if cached_counts and all(k in cached_counts for k in ("primary", "cloud", "archive")):
            cnt_p, cnt_c, cnt_a = cached_counts["primary"], cached_counts["cloud"], cached_counts["archive"]
        else:
            cnt_p, cnt_c, cnt_a = await asyncio.gather(
                get_count(primary, flt, bypass_count),
                get_count(cloud, flt, bypass_count),
                get_count(archive, flt, bypass_count)
            )

        # ✅ FIX: caller (bot) को असली breakdown counts वापस दो ताकि UI में दिखा सके
        # और अगली बार cached_counts के तौर पर वापस भेज सके
        if counts_out is not None:
            counts_out["primary"], counts_out["cloud"], counts_out["archive"] = cnt_p, cnt_c, cnt_a

        total = cnt_p + cnt_c + cnt_a

        sources = []
        if cnt_p > 0: sources.append("Primary")
        if cnt_c > 0: sources.append("Cloud")
        if cnt_a > 0: sources.append("Archive")

        if len(sources) > 1:
            actual_src = "All"
        elif len(sources) == 1:
            actual_src = sources[0]
        else:
            actual_src = "None"

        rem_limit = max_results
        curr_offset = offset

        for col, cnt in [(primary, cnt_p), (cloud, cnt_c), (archive, cnt_a)]:
            if cnt == 0 or rem_limit <= 0: continue

            if curr_offset >= cnt:
                curr_offset -= cnt
                continue

            cursor = col.find(flt, {"_id": 1, "file_name": 1, "file_size": 1, "file_type": 1, "file_ref": 1, "caption": 1, "thumb_url": 1})
            if is_text:
                cursor = cursor.sort([("score", {"$meta": "textScore"})])
            else:
                cursor = cursor.sort('_id', -1)

            cursor.skip(curr_offset).limit(rem_limit)
            docs = await cursor.to_list(length=rem_limit)

            for doc in docs:
                doc["file_id"] = doc["_id"]
                doc["source_col"] = col.name.lower()
            results.extend(docs)

            rem_limit -= len(docs)
            curr_offset = 0

    else:
        col = COLLECTIONS.get(collection_type, primary)
        # ✅ FIX: single collection ke liye bhi cached count reuse (fast pagination)
        if cached_counts and collection_type in cached_counts and not bypass_count:
            results, _ = await _search(col, raw_query, regex, offset, max_results, lang, bypass_count=True)
            total = cached_counts[collection_type]
        else:
            results, total = await _search(col, raw_query, regex, offset, max_results, lang, bypass_count=bypass_count)
        actual_src = collection_type.capitalize()
        if not results: total = 0
        if counts_out is not None:
            counts_out[collection_type] = total

    if bypass_count:
        has_more = len(results) == max_results
        next_offset = offset + max_results if has_more else ""
        total = offset + len(results) + (1 if has_more else 0)
    else:
        next_offset = offset + max_results
        next_offset = "" if next_offset >= total else next_offset

    return results, next_offset, total, actual_src


# ─────────────────────────────────────────────────────────
# 🧠 DB-BASED SPELL SUGGESTIONS (Google Suggest के बजाय अपने ही
# catalog से "Did you mean" सुझाव — इसलिए सुझाया गया नाम हमेशा
# वाकई मौजूद कंटेंट से जुड़ा होता है, Google जैसा "फिर भी नहीं मिला"
# वाला case नहीं आता, और कोई external API call भी नहीं लगती)
# ─────────────────────────────────────────────────────────
def _clean_title_guess(file_name: str) -> str:
    """✅ FIX: पहले यहाँ quality/resolution/year/extension जैसे tokens को
    ढूंढकर काट दिया जाता था और फिर casing भी बदली जाती थी — इससे suggestion
    का टेक्स्ट DB में save असली file_name से अलग दिखता था (जैसे "l" की जगह
    "L", या "1080p l Test" वाला हिस्सा पूरी तरह गायब)। अब suggestion हमेशा
    file_name जैसा DB में है बिल्कुल वैसा ही (सिर्फ extra/multiple spaces
    normalize करके) दिखाया जाता है — कोई trimming, cleaning या case-change
    नहीं।"""
    if not file_name: return ""
    return re.sub(r'\s+', ' ', file_name).strip()

async def get_db_spell_suggestions(query, limit=6, collection_type="all"):
    q = str(query or "").strip()
    if not q: return []

    cols = [primary, cloud, archive] if collection_type == "all" else [COLLECTIONS.get(collection_type, primary)]
    seen = {q.lower().strip()}
    suggestions = []

    # helper to add titles from docs
    def _add_from_docs(docs):
        for doc in docs:
            if len(suggestions) >= limit:
                break
            title = _clean_title_guess(doc.get("file_name", ""))
            key = title.lower()
            if not title or key in seen:
                continue
            seen.add(key)
            suggestions.append(title)

    # 1️⃣ $text search - OR style, closest match
    try:
        tasks = [
            col.find(
                {"$text": {"$search": q}},
                {"file_name": 1, "score": {"$meta": "textScore"}}
            ).sort([("score", {"$meta": "textScore"})]).limit(20).to_list(length=20)
            for col in cols
        ]
        results_lists = await asyncio.gather(*tasks, return_exceptions=True)
        candidates = []
        for res in results_lists:
            if isinstance(res, Exception):
                continue
            candidates.extend(res)
        candidates.sort(key=lambda d: d.get("score", 0), reverse=True)
        _add_from_docs(candidates)
    except Exception as e:
        logger.debug(f"DB spell text-search failed: {e}")

    # 2️⃣ Prefix fallback - first 3 chars, always try to fill up to limit
    if len(suggestions) < limit and len(q) >= 2:
        try:
            pref = re.escape(q[:3] if len(q) >= 3 else q[:2])
            prefix_regex = re.compile(r'(\b|[\s.\-_])' + pref, re.IGNORECASE)
            tasks = [
                col.find({"file_name": prefix_regex}, {"file_name": 1}).limit(20).to_list(length=20)
                for col in cols
            ]
            prefix_results = await asyncio.gather(*tasks, return_exceptions=True)
            pc = []
            for res in prefix_results:
                if isinstance(res, Exception):
                    continue
                pc.extend(res)
            # random shuffle nahi, bas as-is
            _add_from_docs(pc)
        except Exception as e:
            logger.debug(f"DB spell prefix fallback failed: {e}")

    # 3️⃣ Word-level regex - query ke har word se alag search, taaki kam se kam koi word match ho
    if len(suggestions) < limit:
        words = [w for w in re.split(r'\s+', q) if len(w) >= 2]
        for w in words[:3]:  # max 3 words try
            if len(suggestions) >= limit:
                break
            try:
                w_regex = re.compile(re.escape(w), re.IGNORECASE)
                tasks = [
                    col.find({"file_name": w_regex}, {"file_name": 1}).limit(15).to_list(length=15)
                    for col in cols
                ]
                w_results = await asyncio.gather(*tasks, return_exceptions=True)
                wc = []
                for res in w_results:
                    if isinstance(res, Exception):
                        continue
                    wc.extend(res)
                _add_from_docs(wc)
            except Exception:
                continue

    # 4️⃣ Last resort - recent files se bhar do taaki hamesha 5-6 suggestions DB se milen, search kabhi fail na ho
    if len(suggestions) < limit:
        try:
            # recent files se 10 lao, jo already DB me hain
            for col in cols:
                if len(suggestions) >= limit:
                    break
                try:
                    cursor = col.find({}, {"file_name": 1}).sort([('added_on', -1)]).limit(10)
                    docs = await cursor.to_list(length=10)
                    _add_from_docs(docs)
                except:
                    continue
        except Exception as e:
            logger.debug(f"DB spell recent fallback failed: {e}")

    return suggestions[:limit]


# ─────────────────────────────────────────────────────────
# 🆕 RECENT FILES (कोई query ना हो तब dashboard पर दिखाने के लिए
# — सबसे नई अपलोड की गई फाइलें, ताकि पेज खाली ना लगे)
# ─────────────────────────────────────────────────────────
async def get_recent_files(max_results, offset=0, collection_type="all"):
    proj = {"_id": 1, "file_name": 1, "file_size": 1, "file_type": 1, "file_ref": 1, "caption": 1, "thumb_url": 1, "added_on": 1}

    if collection_type == "all":
        take = offset + max_results + 1  # +1 ताकि has_more पता चल सके
        merged = []
        for col in (primary, cloud, archive):
            cursor = col.find({}, proj).sort([('added_on', -1), ('_id', -1)]).limit(take)
            docs = await cursor.to_list(length=take)
            for doc in docs:
                doc["file_id"] = doc["_id"]
                doc["source_col"] = col.name.lower()
            merged.extend(docs)

        merged.sort(key=lambda d: (d.get("added_on") or 0, str(d["_id"])), reverse=True)
        has_more = len(merged) > offset + max_results
        page = merged[offset: offset + max_results]
        next_offset = offset + max_results if has_more else ""
        return page, next_offset

    col = COLLECTIONS.get(collection_type, primary)
    cursor = col.find({}, proj).sort([('added_on', -1), ('_id', -1)]).skip(offset).limit(max_results)
    docs = await cursor.to_list(length=max_results)
    for doc in docs:
        doc["file_id"] = doc["_id"]
        doc["source_col"] = collection_type
    has_more = len(docs) == max_results
    next_offset = offset + max_results if has_more else ""
    return docs, next_offset


# ─────────────────────────────────────────────────────────
# 🗑 DELETE FILES  (✅ DELETE_CHANNEL बैकअप इंजन)
# सिर्फ़ /delete (regex targeted delete) और web के /api/delete में डिलीट होने से
# पहले फाइल DELETE_CHANNEL में फॉरवर्ड होती है। /delete_all (पूरा collection wipe)
# में बैकअप जानबूझकर स्किप किया गया है।
# ─────────────────────────────────────────────────────────
async def _backup_before_delete(doc):
    """किसी फाइल डॉक्युमेंट को डिलीट से पहले DELETE_CHANNEL में बैकअप भेजता है।
    बैकअप फेल हो जाए तो भी डिलीट को नहीं रोकता — बस warning log करके आगे बढ़ता है
    (ताकि Telegram flood/network दिक्कत की वजह से admin का delete operation अटके नहीं)।"""
    if not DELETE_CHANNEL or not getattr(temp, "BOT", None):
        return
    file_ref = doc.get("file_ref")
    if not file_ref:
        return
    caption = f"🗑 <b>Deleted File Backup</b>\n\n📄 <code>{doc.get('file_name', 'Unknown')}</code>"
    for attempt in range(2):  # 1 असली कोशिश + 1 FloodWait के बाद रिट्राई
        try:
            await temp.BOT.send_cached_media(chat_id=DELETE_CHANNEL, file_id=file_ref, caption=caption)
            return
        except FloodWait as e:
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.warning(f"Delete-backup failed for {doc.get('_id')}: {e}")
            return


async def delete_files(query, collection_type="all"):
    deleted = 0
    try:
        cols = [col for name, col in COLLECTIONS.items() if (collection_type == "all" or name == collection_type) and name != "actors"]

        if query == "*":
            # ✅ /delete_all (पूरा collection wipe) — यहाँ DELETE_CHANNEL में बैकअप
            # जानबूझकर नहीं भेजा जाता (हज़ारों फाइलें हो सकती हैं, flood/समय दोनों
            # की समस्या होगी)। सिर्फ़ /delete (regex-आधारित) और web से backup होता है।
            flt = {}
            for col in cols:
                res = await col.delete_many(flt)
                deleted += res.deleted_count
            return deleted

        regex = _build_regex(str(query))
        if not regex: return 0
        flt = {"file_name": regex}

        for col in cols:
            # ✅ FIX: backup ke beech thoda gap taaki flood na ho
            async for doc in col.find(flt, {"_id": 1, "file_name": 1, "file_ref": 1}):
                await _backup_before_delete(doc)
                await asyncio.sleep(0.35)
            res = await col.delete_many(flt)
            deleted += res.deleted_count
        return deleted
    except Exception as e:
        logger.error(f"delete_files error: {e}")
        return deleted


async def delete_single_file(file_id, collection_type="primary"):
    """एक फाइल को डिलीट करने से पहले DELETE_CHANNEL में बैकअप भेजता है, फिर DB से हटाता है।
    web/search_api.py के /api/delete endpoint से इस्तेमाल होता है (delete_files की
    तरह ही _backup_before_delete reuse करता है, दोबारा नहीं लिखा)।"""
    try:
        col = COLLECTIONS.get(collection_type)
        if col is None:
            return False
        doc = await col.find_one({"_id": file_id}, {"_id": 1, "file_name": 1, "file_ref": 1})
        if not doc:
            return False
        await _backup_before_delete(doc)
        res = await col.delete_one({"_id": file_id})
        return bool(res.deleted_count)
    except Exception as e:
        logger.error(f"delete_single_file error: {e}")
        return False

async def get_file_details(file_id):
    try:
        for col in [primary, cloud, archive]:
            doc = await col.find_one({"_id": file_id}, {"_id": 1, "file_name": 1, "file_size": 1, "file_ref": 1, "caption": 1, "thumb_url": 1})
            if doc:
                doc["file_id"] = doc["_id"]  
                return doc
        return None
    except Exception as e:
        logger.error(f"get_file_details error: {e}")
        return None

def encode_file_id(s: bytes) -> str:
    r, n = b"", 0
    for i in s + bytes([22]) + bytes([4]):
        if i == 0: n += 1
        else:
            if n: r += b"\x00" + bytes([n]); n = 0
            r += bytes([i])
    return base64.urlsafe_b64encode(r).decode().rstrip("=")

def unpack_new_file_id(new_file_id: str):
    try:
        decoded = FileId.decode(new_file_id)
        return encode_file_id(pack("<iiqq", int(decoded.file_type), decoded.dc_id, decoded.media_id, decoded.access_hash))
    except Exception as e:
        logger.error(f"unpack_new_file_id error: {e}")
        return None

# ─────────────────────────────────────────────────────────
# 🎭 ACTOR TAGS MULTI-PIPELINE SEARCH ENGINE (100% ISOLATED & DOUBLE CHECKED)
# ─────────────────────────────────────────────────────────
async def get_actor_search_results(actor_name, tags_list, max_results, offset=0, collection_type="all"):
    """नाम और सभी कस्टमाइज्ड टैग्स को मिलाकर सिंक करता है ताकि वाइल्डकार्ड क्रैश न हो।"""
    all_terms = []
    
    if actor_name and str(actor_name).strip():
        all_terms.append(str(actor_name).strip())
        
    if tags_list and isinstance(tags_list, list):
        for t in tags_list:
            if t and str(t).strip():
                all_terms.append(str(t).strip())
                
    if not all_terms:
        return [], ""
                
    escaped_terms = [re.escape(term) for term in all_terms if term]
    combined_raw = r'(' + '|'.join(escaped_terms) + r')'
    
    try:
        regex = re.compile(combined_raw, flags=re.IGNORECASE)
    except Exception:
        regex = re.compile(re.escape(actor_name) if actor_name else "NO_ACTOR_MATCH_FOUND", flags=re.IGNORECASE)
        
    reg_flt = {"$or": [{"file_name": regex}, {"caption": regex}]} if USE_CAPTION_FILTER else {"file_name": regex}
    results = []
    cols = [primary, cloud, archive] if collection_type == "all" else [COLLECTIONS.get(collection_type, primary)]
    
    for col in cols:
        cursor = col.find(reg_flt, {"_id": 1, "file_name": 1, "file_size": 1, "file_type": 1, "file_ref": 1, "caption": 1, "thumb_url": 1}).sort('_id', -1)
        cursor.skip(offset).limit(max_results)
        docs = await cursor.to_list(length=max_results)
        if docs:
            for doc in docs:
                doc["file_id"] = doc["_id"]
                doc["source_col"] = col.name.lower()
            results.extend(docs)
            if len(results) >= max_results:
                results = results[:max_results]
                break

    has_more = len(results) == max_results
    next_offset = offset + max_results if has_more else ""
    return results, next_offset

# ─────────────────────────────────────────────────────────
# 🗑️ ACTOR PROFILE & GALLERY ELEMENT PURGE PIPELINE (NEW UPGRADE)
# ─────────────────────────────────────────────────────────
async def delete_actor_profile(actor_id):
    """डेटाबेस से एक्टर की पूरी प्रोफाइल डिलीट करता है।"""
    try:
        res = await actors.delete_one({"_id": ObjectId(actor_id)})
        return bool(res.deleted_count)
    except Exception as e:
        logger.error(f"delete_actor_profile error: {e}")
        return False

async def delete_gallery_image_by_index(actor_id, index: int):
    """गैलरी एरे में से स्पेसिफिक इंडेक्स वाली इमेज को हटाता है (index-safe, $pull nahi)."""
    try:
        doc = await actors.find_one({"_id": ObjectId(actor_id)}, {"gallery": 1})
        if not doc or "gallery" not in doc:
            return False
        gallery = doc["gallery"]
        if index < 0 or index >= len(gallery):
            return False
        gallery.pop(index)
        res = await actors.update_one({"_id": ObjectId(actor_id)}, {"$set": {"gallery": gallery}})
        return bool(res.modified_count or res.matched_count)
    except Exception as e:
        logger.error(f"delete_gallery_image error: {e}")
        return False
