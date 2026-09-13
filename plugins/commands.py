import os
import random
import asyncio
import logging
from datetime import datetime
from time import time as time_now
from hydrogram import Client, filters, enums
from hydrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

from Script import script
# ✅ FIX: actors कलेक्शन को इम्पोर्ट किया गया ताकि हम डायरेक्टरी की गिनती कर सकें
from database.ia_filterdb import db_count_documents, get_file_details, delete_files, actors, get_directory_counts, get_post_stats as db_post_stats
from database.users_chats_db import db
from info import (
    IS_PREMIUM, URL, BIN_CHANNEL, ADMINS,
    LOG_CHANNEL, PICS, IS_STREAM, REACTIONS, PM_FILE_DELETE_TIME
)
from utils import (
    is_premium, get_settings, get_size, temp,
    get_readable_time, get_wish
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# ✅ MINI APP URL - HTTPS Auto-Fix Sync
# ─────────────────────────────────────────────
def _build_mini_app_url(base_url: str) -> str:
    url = base_url.strip() if base_url else ""
    if not url:
        return ""
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    if not url.startswith("https://"):
        url = f"https://{url}"
    return f"{url.rstrip('/')}/miniapp"

MINI_APP_URL = _build_mini_app_url(URL)


async def _get_post_stats():
    # centralized helper reuse
    total, movies, webseries, appvid, porn, _ = await db_post_stats()
    return total, movies, webseries, appvid, porn


# ─────────────────────────────────────────────
# 🚀 /start COMMAND HANDLER
# ─────────────────────────────────────────────
@Client.on_message(filters.command("start") & filters.incoming)
async def start(client, message):
    if message.chat.type in [enums.ChatType.GROUP, enums.ChatType.SUPERGROUP]:
        if not await db.get_chat(message.chat.id):
            total = await client.get_chat_members_count(message.chat.id)
            await client.send_message(LOG_CHANNEL, script.NEW_GROUP_TXT.format(
                message.chat.title, message.chat.id,
                f"@{message.chat.username or 'Private'}", total
            ))
            await db.add_chat(message.chat.id, message.chat.title)
        return await message.reply(
            f"<b>Hey {message.from_user.mention}, <i>{get_wish()}</i>\nHow can I help you?</b>"
        )

    if REACTIONS:
        try: await message.react(random.choice(REACTIONS), big=True)
        except: pass

    if not await db.is_user_exist(message.from_user.id):
        await db.add_user(message.from_user.id, message.from_user.first_name)
        await client.send_message(LOG_CHANNEL, script.NEW_USER_TXT.format(
            message.from_user.mention, message.from_user.id
        ))

    if IS_PREMIUM and message.from_user.id not in ADMINS and not await is_premium(message.from_user.id, client):
        return await message.reply_photo(
            random.choice(PICS),
            caption=script.PLAN_TXT.format(10, "@admin"),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("💎 Buy Premium Plan", callback_data="activate_plan")
            ]])
        )

    if len(message.command) > 1 and message.command[1] != "premium":
        try:
            parts = message.command[1].split("_")
            if len(parts) >= 3:
                try: await message.delete()
                except: pass

                grp_id, file_id = int(parts[1]), "_".join(parts[2:])
                file = await get_file_details(file_id)
                if not file:
                    return await message.reply("❌ File Not Found!")

                settings = await get_settings(grp_id)
                cap_template = settings.get('caption', script.FILE_CAPTION)
                try:
                    caption = cap_template.format(
                        file_name=str(file.get('file_name', 'File')),
                        file_size=get_size(file.get('file_size', 0))
                    )
                except Exception:
                    caption = f"<b>{file.get('file_name', 'File')}</b>"

                btn = [[InlineKeyboardButton('❌ Close', callback_data=f'close_{message.from_user.id}')]]
                if IS_STREAM:
                    btn.insert(0, [InlineKeyboardButton("▶️ Watch / Download", callback_data=f"stream#{file_id}")])

                target_media = file.get('file_ref')
                if not target_media:
                    return await message.reply("❌ File reference missing, please re-index!")

                msg = await client.send_cached_media(
                    message.chat.id,
                    target_media,
                    caption=caption,
                    reply_markup=InlineKeyboardMarkup(btn)
                )

                if PM_FILE_DELETE_TIME > 0:
                    del_msg = await msg.reply(
                        f"⚠️ This message will delete in {get_readable_time(PM_FILE_DELETE_TIME)}."
                    )
                    
                    await db.add_to_delete_queue(message.chat.id, msg.id, PM_FILE_DELETE_TIME)
                    await db.add_to_delete_queue(message.chat.id, del_msg.id, PM_FILE_DELETE_TIME)
                    
                    if not hasattr(temp, 'PM_FILES'):
                        temp.PM_FILES = {}
                    temp.PM_FILES[msg.id] = {'file_msg': msg.id, 'note_msg': del_msg.id}
                
                return
                
        except Exception as e:
            logger.error(f"Start File Extraction Error: {e}")
            return
        return

    btn = [
        [InlineKeyboardButton("🍿 Open Mini App", web_app=WebAppInfo(url=MINI_APP_URL))],
        [InlineKeyboardButton("+ Add to Group +", url=f"https://t.me/{temp.U_NAME}?startgroup=start")],
        [InlineKeyboardButton("👨‍🚒 Help Menu", callback_data="help"), InlineKeyboardButton("📊 Global Stats", callback_data="stats")]
    ]
    if message.from_user.id not in ADMINS:
        btn.append([InlineKeyboardButton("💎 Premium Duration", callback_data="myplan")])

    await message.reply_photo(
        random.choice(PICS),
        caption=script.START_TXT.format(message.from_user.mention, get_wish()),
        reply_markup=InlineKeyboardMarkup(btn)
    )


# ─────────────────────────────────────────────
# 📊 /stats COMMAND HANDLER (Admin Only) - 100% SECURE FAIL-SAFE
# ─────────────────────────────────────────────
@Client.on_message(filters.command("stats") & filters.user(ADMINS))
async def stats(_, message):
    msg = await message.reply("🔄 Fetching Advanced Database Metrics...")
    
    try:
        try:
            files = await db_count_documents()
            f = files if isinstance(files, dict) else {}
        except Exception as e:
            f = {}
            logger.error(f"File Stats Error: {e}")

        try: users = await db.total_users_count()
        except: users = 0

        try: chats = await db.total_chat_count()
        except: chats = 0

        try: premium = await db.premium.count_documents({"status.premium": True})
        except: premium = 0

        # 🗂️ Universal Directory Fetch - explicit counts
        try:
            tot_dir = await actors.count_documents({})
            app_dir = await actors.count_documents({"category": "app"})
            web_dir = await actors.count_documents({"category": "website"})
            act_dir = await actors.count_documents({"category": "actor"})
        except Exception as e:
            tot_dir = app_dir = web_dir = act_dir = 0
            logger.error(f"Directory Stats Error: {e}")

        post_total, post_movies, post_webseries, post_appvid, post_porn = await _get_post_stats()

        # ✅ FIX: 21 Formatting Args Required for STATUS_TXT
        stats_text = script.STATUS_TXT.format(
            users, chats, premium,
            f.get('total', 0),
            f.get('primary', 0), f.get('primary_thumb', 0),
            f.get('cloud', 0), f.get('cloud_thumb', 0),
            f.get('archive', 0), f.get('archive_thumb', 0),
            tot_dir, act_dir, app_dir, web_dir,
            post_total, post_movies, post_webseries, post_appvid, post_porn,
            f.get('total_thumb', 0),
            get_readable_time(time_now() - temp.START_TIME)
        )

        buttons = [
            [InlineKeyboardButton("❌ CLOSE PANEL", callback_data=f"close_{message.from_user.id}")]
        ]
        await msg.edit(stats_text, reply_markup=InlineKeyboardMarkup(buttons))
        
    except Exception as ex:
        await msg.edit(f"❌ **System Error during Stats generation:**\n\n<code>{ex}</code>\n\n_Please check your script.py placeholders._")


# ─────────────────────────────────────────────
# 🗑 FILE DELETION LOGICS
# ─────────────────────────────────────────────
@Client.on_message(filters.command("delete") & filters.user(ADMINS))
async def delete_file_cmd(client, message):
    if len(message.command) < 3:
        return await message.reply("Usage: `/delete primary Avengers.mkv`")
    storage = message.command[1].lower()
    if storage not in ["primary", "cloud", "archive"]:
        return await message.reply("❌ Invalid Storage! Use: primary, cloud, archive")

    msg = await message.reply("🗑 Deleting target strings...")
    count = await delete_files(" ".join(message.command[2:]), storage)
    await msg.edit(
        f"✅ Deleted `{count}` files from `{storage}`." if count else "❌ No files found match."
    )


@Client.on_message(filters.command("delete_all") & filters.user(ADMINS))
async def delete_all_cmd(client, message):
    if len(message.command) < 2:
        return await message.reply("Usage: `/delete_all primary`")
    storage = message.command[1].lower()
    if storage not in ["primary", "cloud", "archive", "all"]:
        return await message.reply("❌ Invalid Collection Target!")

    await message.reply(
        f"⚠️ <b>DANGER ZONE WARNING!</b>\n\nYou are wiping out ALL documents from `{storage}`.\nAre you absolutely sure?",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("💥 CONFIRM DESTROY ALL", callback_data=f"confirm_del#{storage}"),
            InlineKeyboardButton("❌ ABORT", callback_data=f"close_{message.from_user.id}")
        ]])
    )


# ─────────────────────────────────────────────
# 🔗 LINK GENERATOR (Stream Routing Tunnel)
# ─────────────────────────────────────────────
@Client.on_message(filters.command("link"))
async def link_generator(client, message):
    if IS_PREMIUM and message.from_user.id not in ADMINS and not await is_premium(message.from_user.id, client):
        return await message.reply(
            "🔒 **Premium Feature**\n\nOnly Admins and active Premium Members can generate direct links.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("💎 Buy Premium Plan", callback_data="activate_plan")
            ]]),
            quote=True
        )

    media = (
        getattr(message.reply_to_message, 'document', None) or
        getattr(message.reply_to_message, 'video', None) or
        getattr(message.reply_to_message, 'audio', None)
    )
    if not media:
        return await message.reply("❌ **No streamable media found in the replied message.**", quote=True)

    msg = await message.reply("⏳ **Injecting into Stream Tunnel...**", quote=True)
    try:
        copied = await message.reply_to_message.copy(BIN_CHANNEL)
        await db.add_to_delete_queue(BIN_CHANNEL, copied.id, 3600)
        btn = [
            [
                InlineKeyboardButton("🍿 WATCH ONLINE", url=f"{URL}watch/{copied.id}"),
                InlineKeyboardButton("📥 FAST DOWNLOAD", url=f"{URL}download/{copied.id}")
            ],
            [InlineKeyboardButton("❌ CLOSE ❌", callback_data=f"close_{message.from_user.id}")]
        ]
        await msg.edit_text("<i><b>Direct High-Speed Pipeline Ready ⚡</b></i>", reply_markup=InlineKeyboardMarkup(btn))
    except Exception as e:
        await msg.edit_text(f"❌ **Error generating links:** `{e}`")


# ─────────────────────────────────────────────
# 🎨 CENTRAL BUTTONS INLINE UI CALLBACKS - SECURE
# ─────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r"^(help|user_cmds|admin_cmds|stats|back_start)$"))
async def ui_cb(client, query):
    data = query.data
    buttons_markup = None

    if data == "back_start":
        text = script.START_TXT.format(query.from_user.mention, get_wish())
        btn = [
            [InlineKeyboardButton("🍿 Open Mini App", web_app=WebAppInfo(url=MINI_APP_URL))],
            [InlineKeyboardButton("+ Add to Group +", url=f"https://t.me/{temp.U_NAME}?startgroup=start")],
            [InlineKeyboardButton("👨‍🚒 Help Menu", callback_data="help"), InlineKeyboardButton("📊 Global Stats", callback_data="stats")]
        ]
        if query.from_user.id not in ADMINS:
            btn.append([InlineKeyboardButton("💎 Premium Duration", callback_data="myplan")])
        buttons_markup = InlineKeyboardMarkup(btn)

    elif data == "help":
        text = script.HELP_TXT.format(query.from_user.mention)
        btn = [[InlineKeyboardButton("👨‍💻 User Commands", callback_data="user_cmds")]]
        if query.from_user.id in ADMINS:
            btn[0].append(InlineKeyboardButton("👮‍♂️ Admin Commands", callback_data="admin_cmds"))
        btn.append([InlineKeyboardButton("⬅️ Back Menu", callback_data="back_start")])
        buttons_markup = InlineKeyboardMarkup(btn)

    elif data == "user_cmds":
        text = script.USER_COMMAND_TXT
        btn = [[InlineKeyboardButton("⬅️ Back Menu", callback_data="help")]]
        buttons_markup = InlineKeyboardMarkup(btn)

    elif data == "admin_cmds":
        if query.from_user.id not in ADMINS:
            return await query.answer("❌ You are not an Admin!", show_alert=True)
        text = script.ADMIN_COMMAND_TXT
        btn = [[InlineKeyboardButton("⬅️ Back Menu", callback_data="help")]]
        buttons_markup = InlineKeyboardMarkup(btn)

    elif data == "stats":
        try:
            try: files = await db_count_documents()
            except: files = {}
            f = files if isinstance(files, dict) else {}
            
            uptime = get_readable_time(time_now() - temp.START_TIME)

            try:
                tot_dir = await actors.count_documents({})
                app_dir = await actors.count_documents({"category": "app"})
                web_dir = await actors.count_documents({"category": "website"})
                act_dir = await actors.count_documents({"category": "actor"})
            except:
                tot_dir = app_dir = web_dir = act_dir = 0

            post_total, post_movies, post_webseries, post_appvid, post_porn = await _get_post_stats()

            if query.from_user.id in ADMINS:
                try: users = await db.total_users_count()
                except: users = 0
                try: chats = await db.total_chat_count()
                except: chats = 0
                try: premium = await db.premium.count_documents({"status.premium": True})
                except: premium = 0

                # 21 Args for STATUS_TXT
                text = script.STATUS_TXT.format(
                    users, chats, premium,
                    f.get('total',0),
                    f.get('primary',0), f.get('primary_thumb',0),
                    f.get('cloud',0), f.get('cloud_thumb',0),
                    f.get('archive',0), f.get('archive_thumb',0),
                    tot_dir, act_dir, app_dir, web_dir,
                    post_total, post_movies, post_webseries, post_appvid, post_porn,
                    f.get('total_thumb',0), uptime
                )
                btn = [
                    [InlineKeyboardButton("⬅️ Back Menu", callback_data="back_start")]
                ]
            else:
                # 10 Args for USER_STATUS_TXT
                text = script.USER_STATUS_TXT.format(
                    f.get('total',0), f.get('primary',0), f.get('cloud',0), f.get('archive',0),
                    tot_dir, act_dir, app_dir, web_dir, post_total, uptime
                )
                btn = [[InlineKeyboardButton("⬅️ Back Menu", callback_data="back_start")]]
                
            buttons_markup = InlineKeyboardMarkup(btn)
        except Exception as ex:
            return await query.answer(f"❌ Error displaying stats: {ex}", show_alert=True)

    try:
        await query.message.edit_caption(
            caption=text,
            reply_markup=buttons_markup
        )
    except Exception:
        try: await query.message.edit_text(text=text, reply_markup=buttons_markup)
        except: pass


# ─────────────────────────────────────────────
# 📤 EXTRA OPERATIONAL CALLBAK LOGICS
# ─────────────────────────────────────────────
@Client.on_callback_query(filters.regex(r"^confirm_del#"))
async def confirm_del(client, query):
    if query.from_user.id not in ADMINS:
        return await query.answer("❌ You are not an Admin!", show_alert=True)

    storage = query.data.split("#")[1]
    await query.message.edit("🗑 Destroying collection blocks... Please stand by.")
    count = await delete_files("*", storage)
    await query.message.edit(f"✅ Successfully Wiped `{count}` files from `{storage}`.")


@Client.on_callback_query(filters.regex(r"^stream#"))
async def stream_cb(client, query):
    file_id = query.data.split("#")[1]
    await query.answer("🔗 Generating Video Stream Tunnel Link...", show_alert=False)
    try:
        file = await get_file_details(file_id)
        if not file:
            return await query.answer("❌ File removed or structural ID broken!", show_alert=True)
            
        target_media = file.get('file_ref')
        if not target_media:
            return await query.answer("❌ File reference missing!", show_alert=True)
        msg = await client.send_cached_media(BIN_CHANNEL, target_media)
        await db.add_to_delete_queue(BIN_CHANNEL, msg.id, 3600)
        btn = [
            [
                InlineKeyboardButton("🎬 Stream Online", url=f"{URL}watch/{msg.id}"),
                InlineKeyboardButton("⚡ Download File", url=f"{URL}download/{msg.id}")
            ],
            [InlineKeyboardButton("❌ Close Panel", callback_data=f"close_{query.from_user.id}")]
        ]
        await query.message.edit_reply_markup(InlineKeyboardMarkup(btn))
    except Exception as e:
        await query.answer(f"Error: {e}", show_alert=True)


# close handler moved to filter.py (merged)
