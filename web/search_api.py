import io
import os
import gc
import re
import json
import time
import hmac
import hashlib
import asyncio
import logging
import urllib.parse
import orjson
from lru import LRU
from aiohttp import web

# कस्टमाइज्ड कोर यूटिल्स और कन्फर्म कंट्रोल्स इम्पोर्ट्स
from utils import temp, get_size, is_premium
# ✅ SYNC: THUMBNAIL_STORAGE_CHANNEL को इम्पोर्ट किया गया है पृथक स्टोरेज के लिए
from info import BIN_CHANNEL, ADMINS, BOT_TOKEN, MAX_WEB_RESULTS, MAX_THUMB_CACHE, IS_PREMIUM, USE_CAPTION_FILTER, THUMBNAIL_STORAGE_CHANNEL
# यहाँ db_stats के लिए 'db as filter_db' ऐड किया गया है
from database.ia_filterdb import COLLECTIONS, get_search_results, get_recent_files, db as filter_db, delete_single_file
from database.users_chats_db import db
# ✅ SYNC FIX: cookie-session identity check अब यहाँ दोबारा नहीं लिखा, web_assets से reuse हो रहा है
from web.web_assets import get_auth as web_get_auth

logger = logging.getLogger(__name__)

search_routes = web.RouteTableDef()

# ─────────────────────────────────────────────────────────
# ⚡ ULTRA-FAST ORJSON DUMP FUNCTION
# ─────────────────────────────────────────────────────────
def fast_json(data):
    """orjson बाइट्स (bytes) में डेटा देता है, aiohttp के लिए इसे स्ट्रिंग में डिकोड करना होता है"""
    return orjson.dumps(data).decode('utf-8')

# ✅ BUG FIX: यह duplicate function हटाया गया।
# ia_filterdb.py के get_search_results()/_search() पहले से ही raw query से
# strict "word" "word" $text-query बना लेते हैं। यहाँ पहले से quote-wrapped
# string भेजने से get_search_results के अंदर regex fallback (_build_regex)
# को वही quote marks literal characters की तरह मिल जाते थे, जिससे fallback
# regex कभी किसी real filename से match ही नहीं करता था। अब raw `q` सीधे
# get_search_results को भेजा जाता है (नीचे देखें)।

# ─────────────────────────────────────────────────────────
# 📸 TRUE LRU THUMBNAIL STORAGE (C-Based LRU-Dict)
# ─────────────────────────────────────────────────────────
MAX_CACHE = MAX_THUMB_CACHE
thumb_semaphore = asyncio.Semaphore(15)
thumb_cache = LRU(MAX_CACHE)  # ✅ C-लेंग्वेज आधारित सुपरफास्ट कैशे (Size Fixed)
thumb_locks = {}

# KOYEB OPTIMIZATION: Limits reduced to 40 to protect 512MB RAM limits
PREFETCH_CACHE = LRU(40)  
TRENDING_CACHE = LRU(40)  
TRENDING_CACHE_TTL = 300

# ─────────────────────────────────────────────────────────
# 📸 OPTIMIZED THUMBNAIL ENGINE (Bytes-in-RAM True LRU)
# ─────────────────────────────────────────────────────────
async def _get_or_fetch_thumb(fid, col_name="primary", file_ref=None, is_retry=False):
    cache_key = f"{col_name}:{fid}"

    if is_retry:
        if cache_key in thumb_cache and thumb_cache[cache_key] == "NO_THUMB":
            del thumb_cache[cache_key]

    if cache_key in thumb_cache:
        cached_val = thumb_cache[cache_key]
        return None if cached_val == "NO_THUMB" else cached_val

    lock = thumb_locks.setdefault(cache_key, asyncio.Lock())

    try:
        async with lock:
            if cache_key in thumb_cache:
                cached_val = thumb_cache[cache_key]
                return None if cached_val == "NO_THUMB" else cached_val

            async def _fetch():
                target_collection = COLLECTIONS.get(col_name, COLLECTIONS["primary"])
                # fetch thumb_url + file_ref together
                existing = await target_collection.find_one({"_id": fid}, {"thumb_url": 1, "file_ref": 1})

                if existing and existing.get("thumb_url", "").startswith("TG_ID:"):
                    saved_thumb_id = existing["thumb_url"].replace("TG_ID:", "")
                    try:
                        file_data = await temp.BOT.download_media(saved_thumb_id, in_memory=True)
                        if file_data:
                            img_bytes = file_data.getvalue()
                            thumb_cache[cache_key] = img_bytes
                            return img_bytes
                    except Exception:
                        pass

                # resolve file_id to use for send_cached_media
                send_id = file_ref or (existing.get("file_ref") if existing else None) or fid

                for attempt in range(5):
                    try:
                        msg = await temp.BOT.send_cached_media(chat_id=BIN_CHANNEL, file_id=send_id)
                        thumb_id = None
                        if msg.video and msg.video.thumbs and len(msg.video.thumbs) > 0:
                            thumb_id = msg.video.thumbs[0].file_id
                        elif msg.document and msg.document.thumbs and len(msg.document.thumbs) > 0:
                            thumb_id = msg.document.thumbs[0].file_id

                        if thumb_id:
                            file_data = await temp.BOT.download_media(thumb_id, in_memory=True)
                            if file_data:
                                img_bytes = file_data.getvalue()
                                thumb_cache[cache_key] = img_bytes
                                await target_collection.update_one(
                                    {"_id": fid},
                                    {"$set": {"thumb_url": f"TG_ID:{thumb_id}"}}
                                )
                                await db.add_to_delete_queue(BIN_CHANNEL, msg.id, 5)
                                return img_bytes
                        else:
                            thumb_cache[cache_key] = "NO_THUMB"
                            await db.add_to_delete_queue(BIN_CHANNEL, msg.id, 5)
                            return None
                    except Exception as e:
                        err_text = str(e)
                        if "FLOOD_WAIT" in err_text or "420" in err_text:
                            m = re.search(r'wait of (\d+) second', err_text)
                            wait_time = int(m.group(1)) if m else 20
                            await asyncio.sleep(wait_time + 2)
                            continue
                        await asyncio.sleep(1)
                        continue
                return None

            async with thumb_semaphore:
                return await _fetch()
    finally:
        thumb_locks.pop(cache_key, None)


# ─────────────────────────────────────────────────────────
# 🔄 BACKGROUND PRE-FETCH WORKER (Controlled Warmup Load)
# ─────────────────────────────────────────────────────────
async def bg_prefetch_worker(tg_id, q, col, mode, prefetch_offset, lim):
    try:
        cache_key = f"{tg_id}_{q}_{col}_{mode}_{prefetch_offset}"
        if cache_key in PREFETCH_CACHE:
            return

        docs, next_off, _, _ = await get_search_results(
            q, lim, offset=prefetch_offset, collection_type=col, bypass_count=True
        )

        if docs:
            PREFETCH_CACHE[cache_key] = (docs, next_off)
            if mode != "none":
                warmup_docs = docs if tg_id in ADMINS else docs[:5]
                for doc in warmup_docs:
                    asyncio.create_task(
                        _get_or_fetch_thumb(doc["_id"], col_name=doc.get("source_col", "primary"), file_ref=doc.get("file_ref"))
                    )
                    await asyncio.sleep(0.01) 

    except Exception as e:
        logger.error(f"❌ Prefetch worker execution failed: {e}")


# ─────────────────────────────────────────────────────────
# 🔒 STRICT SECURITY: Telegram initData HMAC Verification
# ─────────────────────────────────────────────────────────
def verify_telegram_init_data(init_data: str) -> dict | None:
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop("hash", None)
        if not received_hash:
            return None
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        expected_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected_hash, received_hash):
            return None
        user_str = parsed.get("user", "{}")
        return json.loads(user_str)
    except Exception:
        return None


# ✅ SYNC FIX: यह छोटा helper दोनों branches (init_data + cookie) में एक जैसा
# admin/premium/IS_PREMIUM-fallback logic apply करता है, ताकि दोनों branch में
# rule अलग-अलग न हो जाए (पहले cookie-branch में IS_PREMIUM fallback मिसिंग था)।
async def _resolve_role_for_tg(tg_id):
    if tg_id in ADMINS: return "admin"
    # ℹ️ सुधार: पहले यहाँ लिखा था कि bot arg required है और is_premium(tg_id) क्रैश
    # करेगा — यह गलत था। utils.py में is_premium(user_id, bot=None) है, bot optional
    # है, इसलिए 1-arg कॉल पहले भी सुरक्षित था। premium.py में एक अलग (dead/unused)
    # is_premium(uid, bot) कॉपी है जो कहीं import ही नहीं होती। यहाँ temp.BOT भेजना
    # सिर्फ इसलिए रखा है ताकि plan expire होने पर user को notify भी मिल जाए
    # (filter.py/commands.py जैसा), यह जरूरी bugfix नहीं बल्कि एक छोटा सुधार है।
    if await is_premium(tg_id, temp.BOT): return "user"
    if not IS_PREMIUM: return "user"
    return None


async def get_user_role(req):
    init_data = req.headers.get("X-Telegram-Init-Data", "").strip()
    if init_data:
        user = verify_telegram_init_data(init_data)
        if user:
            tg_id = int(user.get("id", 0))
            if tg_id:
                role = await _resolve_role_for_tg(tg_id)
                if role: return role, tg_id
        return None, None

    # ✅ DUPLICATION FIX: cookie/session पढ़ने वाला logic अब web_assets.get_auth() से
    # ही आता है (वही function जो /dashboard, /actors, /posts पेज इस्तेमाल करते हैं),
    # यहाँ दोबारा वही कोड नहीं लिखा गया। सिर्फ premium resolution अलग से जोड़ी गई है
    # क्योंकि API endpoints को JSON 403 चाहिए, redirect नहीं।
    role, tg_id = await web_get_auth(req)
    if not role:
        return None, None
    if role == "admin":
        return "admin", tg_id
    resolved = await _resolve_role_for_tg(tg_id)
    return (resolved, tg_id) if resolved else (None, None)


# ─────────────────────────────────────────────────────────
# 🔍 SEARCH API — Smart Pre-fetch Grid Engine (orjson dumps)
# ─────────────────────────────────────────────────────────
def _build_results_list(all_m, mode):
    results_list = []
    for d in all_m:
        fid = d.get("file_ref") or d.get("_id")
        db_id = d.get("_id")
        source_collection_name = d.get("source_col", "primary")

        if mode == "none":
            tg_thumb = ""
            poster_url = ""
        else:
            raw_thumb = d.get("thumb_url", "")
            v_salt = raw_thumb[-8:] if (raw_thumb and raw_thumb.startswith("TG_ID:")) else "0"
            tg_thumb = f"/api/thumb?file_id={db_id}&col={source_collection_name}&v={v_salt}"
            poster_url = tg_thumb

        results_list.append({
            "file_id": db_id,
            "name": d.get("file_name", "Unknown File"),
            "size": get_size(d.get("file_size", 0)),
            "type": d.get("file_type", "document").upper(),
            "source": source_collection_name.capitalize(),
            "raw_collection": source_collection_name,
            "poster": poster_url,
            "tg_thumb": tg_thumb,
            "watch": f"/setup_stream?file_id={fid}&mode=watch",
            "download": f"/setup_stream?file_id={fid}&mode=download",
            "caption": d.get("caption", ""),  # ✅ यहाँ नया बदलाव किया गया है
        })
    return results_list


@search_routes.get("/api/search")
async def api_search(req):
    role, tg_id = await get_user_role(req)
    if not role:
        return web.json_response({"error": "Unauthorized Access!"}, status=403, dumps=fast_json)

    q = req.query.get("q", "").strip()
    off = req.query.get("offset", "0")
    col = req.query.get("col", "all").lower()
    mode = req.query.get("mode", "tg").lower()

    try:
        off = max(0, int(off))
    except Exception:
        off = 0

    lim = MAX_WEB_RESULTS

    # 🆕 कोई query नहीं दी गई — dashboard पर खाली स्क्रीन दिखाने की बजाय
    # सबसे नई अपलोड की गई फाइलें (Recently Added) दिखाओ
    if not q:
        recent_docs, recent_next_offset = await get_recent_files(lim, offset=off, collection_type=col)
        results_list = _build_results_list(recent_docs, mode)
        has_more = bool(recent_next_offset)
        return web.json_response({
            "results": results_list,
            "total": off + len(results_list) + (1 if has_more else 0),
            "next_offset": recent_next_offset,
            "is_admin": role == "admin",
            "is_recent": True,
        }, dumps=fast_json)

    if off == 0:
        trend_key = f"{col}_{mode}_{q.lower()}"
        now_ts = time.time()
        if trend_key in TRENDING_CACHE and TRENDING_CACHE[trend_key]["expiry"] > now_ts:
            cached = TRENDING_CACHE[trend_key]
            
            if cached["next_offset"]:
                asyncio.create_task(bg_prefetch_worker(tg_id, q, col, mode, cached["next_offset"], lim))

            return web.json_response({
                "results": cached["results"],
                "total": off + len(cached["results"]) + (1 if cached["next_offset"] else 0),
                "next_offset": cached["next_offset"],
                "is_admin": role == "admin"
            }, dumps=fast_json)

    current_cache_key = f"{tg_id}_{q}_{col}_{mode}_{off}"
    all_m = []
    next_offset = ""

    if current_cache_key in PREFETCH_CACHE:
        all_m, next_offset = PREFETCH_CACHE[current_cache_key]
        del PREFETCH_CACHE[current_cache_key]

    if not all_m:
        all_m, next_offset, _, _ = await get_search_results(
            q, lim, offset=off, collection_type=col, bypass_count=True
        )

    has_more = bool(next_offset)

    if has_more:
        asyncio.create_task(bg_prefetch_worker(tg_id, q, col, mode, next_offset, lim))

    results_list = _build_results_list(all_m, mode)

    if off == 0 and results_list:
        trend_key = f"{col}_{mode}_{q.lower()}"
        TRENDING_CACHE[trend_key] = {
            "results": results_list,
            "next_offset": next_offset,
            "expiry": time.time() + TRENDING_CACHE_TTL
        }

    return web.json_response({
        "results": results_list,
        "total": off + len(results_list) + (1 if has_more else 0),
        "next_offset": next_offset,
        "is_admin": role == "admin",
    }, dumps=fast_json)


# ─────────────────────────────────────────────────────────
# 📸 THUMBNAIL API
# ─────────────────────────────────────────────────────────
@search_routes.get("/api/thumb")
async def get_telegram_thumb(req):
    fid = req.query.get("file_id")
    col_name = req.query.get("col", "primary").lower()
    is_retry = req.query.get("retry", "false").lower() == "true"
    if not fid:
        return web.Response(status=400)

    headers = {
        "Content-Disposition": 'inline; filename="poster.jpg"',
        "Cache-Control": "max-age=86400"
    }

    res = await _get_or_fetch_thumb(fid, col_name=col_name, is_retry=is_retry)
    if res is None:
        return web.Response(status=404)

    return web.Response(body=res, content_type="image/jpeg", headers=headers)


# ─────────────────────────────────────────────────────────
# 🎥 STREAM SETUP PIPELINE
# ─────────────────────────────────────────────────────────
@search_routes.get("/setup_stream")
async def setup_stream(req):
    role, _ = await get_user_role(req)
    if not role:
        return web.Response(text="❌ Unauthorized Access Denied!", status=403)
    fid = req.query.get("file_id")
    mode = req.query.get("mode", "watch")
    if not fid:
        return web.Response(text="❌ Missing file_id!", status=400)
    try:
        msg = await temp.BOT.send_cached_media(chat_id=BIN_CHANNEL, file_id=fid)
        await db.add_to_delete_queue(BIN_CHANNEL, msg.id, 3600)
        if mode == "watch":
            await db.track_video_play()
        return web.HTTPFound(f"/{'download' if mode == 'download' else 'watch'}/{msg.id}")
    except Exception as e:
        return web.Response(text=f"❌ Error Tunneling Stream: {e}", status=500)


@search_routes.post("/setup_stream")
async def setup_stream_post(req):
    role, _ = await get_user_role(req)
    if not role:
        return web.json_response({"error": "Unauthorized Web Access!"}, status=403, dumps=fast_json)
    try:
        data = await req.json()
        fid = data.get("file_id")
        mode = data.get("mode", "watch")
    except Exception:
        fid = req.query.get("file_id")
        mode = req.query.get("mode", "watch")
    if not fid:
        return web.json_response({"error": "Missing file_id!"}, status=400, dumps=fast_json)
    try:
        msg = await temp.BOT.send_cached_media(chat_id=BIN_CHANNEL, file_id=fid)
        await db.add_to_delete_queue(BIN_CHANNEL, msg.id, 3600)
        if mode == "watch":
            await db.track_video_play()
        return web.json_response({"url": f"/{'download' if mode == 'download' else 'watch'}/{msg.id}"}, dumps=fast_json)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500, dumps=fast_json)


# ─────────────────────────────────────────────────────────
# ⚖️ ADMIN CONTROLS: EDIT, ADD CAPTION & TRANSFER PIPELINE
# ─────────────────────────────────────────────────────────
@search_routes.post("/api/delete")
async def api_delete(req):
    role, _ = await get_user_role(req)
    if role != "admin":
        return web.json_response({"error": "Core Admin Authorization Required!"}, status=403, dumps=fast_json)
    try:
        data = await req.json()
        fid = data.get("file_id")
        col = data.get("collection", "primary").lower()
        if col not in COLLECTIONS:
            return web.json_response({"error": "Invalid target collection!"}, status=400, dumps=fast_json)
        # ✅ NEW: डिलीट से पहले DELETE_CHANNEL में बैकअप भेजा जाता है (delete_single_file के अंदर)
        success = await delete_single_file(fid, col)
        return web.json_response({"success": success}, dumps=fast_json)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500, dumps=fast_json)


@search_routes.post("/api/edit_name")
async def api_edit_name(req):
    role, _ = await get_user_role(req)
    if role != "admin":
        return web.json_response({"error": "Core Admin Authorization Required!"}, status=403, dumps=fast_json)
    try:
        data = await req.json()
        fid = data.get("file_id")
        col = data.get("collection", "primary").lower()
        
        new_name = data.get("new_name", "").strip()
        add_caption = data.get("add_caption", "").strip()
        target_col = data.get("target_collection", col).lower()

        if not fid or col not in COLLECTIONS or target_col not in COLLECTIONS:
            return web.json_response({"error": "Missing structural inputs!"}, status=400, dumps=fast_json)

        doc = await COLLECTIONS[col].find_one({"_id": fid})
        if not doc:
            return web.json_response({"error": "File not found in database!"}, status=404, dumps=fast_json)

        update_fields = {}
        if new_name:
            update_fields["file_name"] = new_name

        # ✅ यहाँ बदलाव किया गया है: अब नया टैग पुराने को पूरी तरह रिप्लेस करेगा
        update_fields["caption"] = add_caption

        if col != target_col:
            doc.update(update_fields)  
            await COLLECTIONS[target_col].insert_one(doc)
            await COLLECTIONS[col].delete_one({"_id": fid})
        else:
            if update_fields:
                await COLLECTIONS[col].update_one({"_id": fid}, {"$set": update_fields})
        
        PREFETCH_CACHE.clear()
        TRENDING_CACHE.clear()

        return web.json_response({"success": True}, dumps=fast_json)
    except Exception as e:
        logger.error(f"Edit/Transfer Error: {e}")
        return web.json_response({"error": str(e)}, status=500, dumps=fast_json)


# ─────────────────────────────────────────────────────────
# 📥 NATIVE THUMBNAIL UPLOAD & CACHE BUSTER API
# ─────────────────────────────────────────────────────────
@search_routes.post("/api/upload_thumb")
async def api_upload_thumb(req):
    role, _ = await get_user_role(req)
    if role != "admin":
        return web.json_response({"error": "Core Admin Authorization Required!"}, status=403, dumps=fast_json)
    try:
        reader = await req.multipart()
        file_id_field, collection_field, image_bytes = None, None, None
        while True:
            part = await reader.next()
            if part is None:
                break
            if part.name == 'file_id':
                file_id_field = (await part.read()).decode().strip()
            elif part.name == 'collection':
                collection_field = (await part.read()).decode().strip().lower()
            elif part.name == 'image':
                image_bytes = await part.read()

        if not file_id_field or not collection_field or not image_bytes:
            return web.json_response({"error": "Missing required assets!"}, status=400, dumps=fast_json)
        if collection_field not in COLLECTIONS:
            return web.json_response({"error": "Target collection missing!"}, status=400, dumps=fast_json)

        cache_k = f"{collection_field}:{file_id_field}"
        if cache_k in thumb_cache:
            del thumb_cache[cache_k]

        with io.BytesIO(image_bytes) as img_buffer:
            img_buffer.name = "poster.jpg"
            # ✅ UPGRADE: पुराने मिक्स्ड 'BIN_CHANNEL' के बजाय पृथक 'THUMBNAIL_STORAGE_CHANNEL' का उपयोग
            msg = await temp.BOT.send_photo(chat_id=THUMBNAIL_STORAGE_CHANNEL, photo=img_buffer)

        if not msg or not msg.photo:
            return web.json_response({"error": "Telegram Node failed!"}, status=500, dumps=fast_json)

        try:
            new_thumb_id = (
                msg.photo.sizes[-1].file_id
                if hasattr(msg.photo, "sizes") and msg.photo.sizes
                else msg.photo.file_id
            )
        except Exception:
            new_thumb_id = msg.photo.file_id

        db_save_value = f"TG_ID:{new_thumb_id}"
        # ✅ UPGRADE: डेटाबेस में वेब कस्टमाइज्ड सिंक लॉक 'thumb_source: web' और 'is_thumb_permanent: True' लॉक किया गया
        await COLLECTIONS[collection_field].update_one(
            {"_id": file_id_field},
            {"$set": {"thumb_url": db_save_value, "thumb_source": "web", "is_thumb_permanent": True}}
        )
        
        # ❌ (Removed auto-delete so permanent thumbnails stay in channel)
        # await db.add_to_delete_queue(THUMBNAIL_STORAGE_CHANNEL, msg.id, 5)
        
        PREFETCH_CACHE.clear()
        TRENDING_CACHE.clear()

        return web.json_response({"success": True}, dumps=fast_json)

    except Exception as e:
        logger.error(f"❌ Upload thumb endpoint crash: {e}")
        return web.json_response({"error": str(e)}, status=500, dumps=fast_json)


@search_routes.get("/api/db_stats")
async def api_db_stats(req):
    role, _ = await get_user_role(req)
    if role != "admin":
        return web.json_response({"error": "Admin Authorization Required!"}, status=403, dumps=fast_json)
    
    try:
        stats = await filter_db.command("dbstats")
        used_bytes = stats.get("storageSize", 0) + stats.get("indexSize", 0)
        limit_bytes = 512 * 1024 * 1024
        percent = (used_bytes / limit_bytes) * 100
        
        return web.json_response({
            "used": get_size(used_bytes),
            "total": "512.0 MB",
            "percent": min(round(percent, 2), 100) 
        }, dumps=fast_json)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500, dumps=fast_json)


# ─────────────────────────────────────────────
# 🧹 ADMIN: RAM CACHE FLUSH (stats.py के "Flush RAM Cache" बटन के लिए)
# ✅ FIX: पहले यह बटन सिर्फ़ UI में setTimeout से "✅ Cleared!" दिखा देता था,
# असल में कोई cache clear नहीं होता था। अब यहाँ पहले से मौजूद LRU caches
# (thumb_cache, PREFETCH_CACHE, TRENDING_CACHE - जो api_edit_name/api_upload_thumb
# में भी clear होते हैं) को ही दोबारा इस्तेमाल किया गया है, नया कैशे सिस्टम नहीं बनाया।
# ─────────────────────────────────────────────
@search_routes.post("/api/flush_cache")
async def api_flush_cache(req):
    role, _ = await get_user_role(req)
    if role != "admin":
        return web.json_response({"error": "Admin Authorization Required!"}, status=403, dumps=fast_json)
    try:
        thumb_cache.clear()
        PREFETCH_CACHE.clear()
        TRENDING_CACHE.clear()
        gc.collect()
        return web.json_response({"success": True}, dumps=fast_json)
    except Exception as e:
        return web.json_response({"error": str(e)}, status=500, dumps=fast_json)


@search_routes.get("/miniapp")
async def miniapp_page(req):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    html_path = os.path.join(base_dir, "web", "miniapp.html")
    if not os.path.exists(html_path):
        html_path = os.path.join(base_dir, "Web", "miniapp.html")
    if not os.path.exists(html_path):
        return web.Response(text="miniapp.html page template not found.", status=404)
    return web.FileResponse(html_path)
