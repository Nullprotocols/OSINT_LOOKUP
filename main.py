import logging
import os
import json
import asyncio
import httpx
import secrets
import csv
import tempfile
import shutil
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton, 
    FSInputFile, InputMediaPhoto, InputMediaVideo,
    InputMediaAudio, InputMediaDocument, ReplyKeyboardRemove
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv
from database import (
    init_db, add_user, get_user, update_credits, 
    create_redeem_code, redeem_code_db, get_all_users, 
    set_ban_status, get_bot_stats, get_users_in_range,
    add_admin, remove_admin, get_all_admins, is_admin,
    get_expired_codes, delete_redeem_code, get_top_referrers,
    deactivate_code, get_all_codes, parse_time_string,
    get_user_by_username, update_username, get_user_stats,
    get_recent_users, get_active_codes, get_inactive_codes,
    delete_user, reset_user_credits, get_user_by_id,
    search_users, get_daily_stats, log_lookup,
    get_lookup_stats, get_total_lookups, get_user_lookups,
    get_premium_users, get_low_credit_users, get_inactive_users,
    update_last_active, get_user_activity, get_leaderboard,
    bulk_update_credits, get_code_usage_stats
)

# --- CONFIGURATION ---
load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.getenv("OWNER_ID"))
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x]

# Channels Config
CHANNELS = [int(x) for x in os.getenv("FORCE_JOIN_CHANNELS", "").split(",") if x]
CHANNEL_LINKS = os.getenv("FORCE_JOIN_LINKS", "").split(",")

# Log Channels
LOG_CHANNELS = {
    'num': os.getenv("LOG_CHANNEL_NUM"),
    'ifsc': os.getenv("LOG_CHANNEL_IFSC"),
    'email': os.getenv("LOG_CHANNEL_EMAIL"),
    'gst': os.getenv("LOG_CHANNEL_GST"),
    'vehicle': os.getenv("LOG_CHANNEL_VEHICLE"),
    'pincode': os.getenv("LOG_CHANNEL_PINCODE")
}

# APIs
APIS = {
    'num': os.getenv("API_NUM"),
    'ifsc': os.getenv("API_IFSC"),
    'email': os.getenv("API_EMAIL"),
    'gst': os.getenv("API_GST"),
    'vehicle': os.getenv("API_VEHICLE"),
    'pincode': os.getenv("API_PINCODE")
}

# Setup
bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
logging.basicConfig(level=logging.INFO)

# --- FSM STATES ---
class Form(StatesGroup):
    waiting_for_redeem = State()
    waiting_for_broadcast = State()
    waiting_for_direct_message = State()
    waiting_for_dm_user = State()
    waiting_for_dm_content = State()
    waiting_for_custom_code = State()
    waiting_for_stats_range = State()
    waiting_for_code_deactivate = State()
    waiting_for_api_input = State()
    waiting_for_api_type = State()
    waiting_for_username = State()
    waiting_for_delete_user = State()
    waiting_for_reset_credits = State()
    waiting_for_bulk_message = State()
    waiting_for_code_stats = State()
    waiting_for_user_lookups = State()
    waiting_for_bulk_gift = State()
    waiting_for_user_search = State()

# --- HELPERS ---
def get_branding():
    return {
         "meta":{
            "developer": "@Nullprotocol_X",
            "powered_by": "NULL PROTOCOL"
        }
    }

async def is_user_owner(user_id):
    return user_id == OWNER_ID

async def is_user_admin(user_id):
    if user_id == OWNER_ID:
        return 'owner'
    if user_id in ADMIN_IDS:
        return 'admin'
    db_admin = await is_admin(user_id)
    return db_admin

async def is_user_banned(user_id):
    user = await get_user(user_id)
    if user and user[5] == 1:
        return True
    return False

async def check_membership(user_id):
    admin_level = await is_user_admin(user_id)
    if admin_level: 
        return True
    try:
        for channel_id in CHANNELS:
            member = await bot.get_chat_member(channel_id, user_id)
            if member.status in ['left', 'kicked', 'restricted']:
                return False
        return True
    except:
        return False

def get_join_keyboard():
    buttons = []
    for i, link in enumerate(CHANNEL_LINKS):
        buttons.append([InlineKeyboardButton(text=f"📢 Join Channel {i+1}", url=link)])
    buttons.append([InlineKeyboardButton(text="✅ Verify Join", callback_data="check_join")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- UPDATED MAIN MENU (2 buttons per row) ---
def get_main_menu(user_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        # Row 1
        [
            InlineKeyboardButton(text="📱 Number", callback_data="api_num"),
            InlineKeyboardButton(text="🏦 IFSC", callback_data="api_ifsc")
        ],
        # Row 2
        [
            InlineKeyboardButton(text="📧 Email", callback_data="api_email"),
            InlineKeyboardButton(text="📋 GST", callback_data="api_gst")
        ],
        # Row 3
        [
            InlineKeyboardButton(text="🚗 Vehicle", callback_data="api_vehicle"),
            InlineKeyboardButton(text="📮 Pincode", callback_data="api_pincode")
        ],
        # Row 4
        [
            InlineKeyboardButton(text="🎁 Redeem", callback_data="redeem"),
            InlineKeyboardButton(text="🔗 Refer & earn", callback_data="refer_earn")
        ],
        # Row 5
        [
            InlineKeyboardButton(text="👤 Profile", callback_data="profile"),
            InlineKeyboardButton(text="💳 Buy Credits", url="https://t.me/Nullprotocol_X")
        ]
    ])

# --- START & JOIN ---
@dp.message(CommandStart())
async def start_command(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    
    if await is_user_banned(user_id):
        await message.answer("🚫 <b>You are BANNED from using this bot.</b>", parse_mode="HTML")
        return

    existing_user = await get_user(user_id)
    if not existing_user:
        referrer_id = None
        args = command.args
        if args and args.startswith("ref_"):
            try:
                referrer_id = int(args.split("_")[1])
                if referrer_id == user_id: 
                    referrer_id = None
            except: 
                pass
        
        await add_user(user_id, message.from_user.username, referrer_id)
        if referrer_id:
            await update_credits(referrer_id, 3)
            try: 
                await bot.send_message(referrer_id, "🎉 <b>Referral +3 Credits!</b>", parse_mode="HTML")
            except: 
                pass

    if not await check_membership(user_id):
        await message.answer(
            "👋 <b>Welcome to OSINT LOOKUP</b>\n\n"
            "⚠️ <b>Bot use karne ke liye channels join karein:</b>",
            reply_markup=get_join_keyboard(), 
            parse_mode="HTML"
        )
        return

    welcome_msg = f"""
🔓 <b>Access Granted!</b>

Welcome <b>{message.from_user.first_name}</b>,

<b>OSINT LOOKUP</b> - Premium Lookup Services
Select a service from menu below:
"""
    
    await message.answer(
        welcome_msg,
        reply_markup=get_main_menu(user_id), 
        parse_mode="HTML"
    )
    await update_last_active(user_id)

@dp.callback_query(F.data == "check_join")
async def verify_join(callback: types.CallbackQuery):
    if await check_membership(callback.from_user.id):
        await callback.message.delete()
        await callback.message.answer("✅ <b>Verified!</b>", 
                                    reply_markup=get_main_menu(callback.from_user.id), 
                                    parse_mode="HTML")
    else:
        await callback.answer("❌ Abhi bhi kuch channels join nahi kiye!", show_alert=True)

# --- PROFILE ---
@dp.callback_query(F.data == "profile")
async def show_profile(callback: types.CallbackQuery):
    user_data = await get_user(callback.from_user.id)
    if not user_data: 
        return
    
    admin_level = await is_user_admin(callback.from_user.id)
    credits = "♾️ Unlimited" if admin_level else user_data[2]
    
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{user_data[0]}"
    
    # Get user stats
    stats = await get_user_stats(callback.from_user.id)
    referrals = stats[0] if stats else 0
    codes_claimed = stats[1] if stats else 0
    total_from_codes = stats[2] if stats else 0
    
    msg = (f"👤 <b>User Profile</b>\n\n"
           f"🆔 <b>ID:</b> <code>{user_data[0]}</code>\n"
           f"👤 <b>Username:</b> @{user_data[1] or 'N/A'}\n"
           f"💰 <b>Credits:</b> {credits}\n"
           f"📊 <b>Total Earned:</b> {user_data[6]}\n"
           f"👥 <b>Referrals:</b> {referrals}\n"
           f"🎫 <b>Codes Claimed:</b> {codes_claimed}\n"
           f"📅 <b>Joined:</b> {datetime.fromtimestamp(float(user_data[3])).strftime('%d-%m-%Y')}\n"
           f"🔗 <b>Referral Link:</b>\n<code>{link}</code>")
    
    await callback.message.edit_text(msg, parse_mode="HTML", 
                                   reply_markup=get_main_menu(callback.from_user.id))

# --- REFERRAL SECTION ---
@dp.callback_query(F.data == "refer_earn")
async def refer_earn_handler(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"
    
    msg = (
        "🔗 <b>Refer & Earn Program</b>\n\n"
        "Apne dosto ko invite karein aur free credits paayein!\n"
        "Per Referral: <b>+3 Credits</b>\n\n"
        "👇 <b>Your Link:</b>\n"
        f"<code>{link}</code>\n\n"
        "📊 <b>How it works:</b>\n"
        "1. Apna link share karein\n"
        "2. Jo bhi is link se join karega\n"
        "3. Aapko milenge <b>3 credits</b>"
    )
    
    back_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Back", callback_data="back_home")]
    ])
    await callback.message.edit_text(msg, parse_mode="HTML", reply_markup=back_kb)

@dp.callback_query(F.data == "back_home")
async def go_home(callback: types.CallbackQuery):
    await callback.message.edit_text(
        f"🔓 <b>Main Menu</b>",
        reply_markup=get_main_menu(callback.from_user.id), parse_mode="HTML"
    )

# --- REDEEM SYSTEM ---
@dp.callback_query(F.data == "redeem")
async def redeem_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🎁 <b>Redeem Code</b>\n\n"
        "Enter your redeem code below:\n\n"
        "📌 <i>Note: Each code can be used only once per user</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_redeem")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_redeem)
    await callback.answer()

@dp.callback_query(F.data == "cancel_redeem")
async def cancel_redeem_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("❌ Operation Cancelled.", 
                                reply_markup=get_main_menu(callback.from_user.id))

# --- API PROCESSING FUNCTION ---
async def process_api_call(message: types.Message, api_type: str, input_data: str):
    user_id = message.from_user.id
    
    if await is_user_banned(user_id): 
        return

    user = await get_user(user_id)
    admin_level = await is_user_admin(user_id)
    
    if not admin_level and user[2] < 1:
        await message.reply("❌ <b>Insufficient Credits!</b>", parse_mode="HTML")
        return

    status_msg = await message.reply("🔄 <b>Fetching Data...</b>", parse_mode="HTML")
    
    try:
        async with httpx.AsyncClient() as client:
            url = f"{APIS[api_type]}{input_data}"
            resp = await client.get(url, timeout=30)
            
            try:
                raw_data = resp.json()
            except:
                raw_data = {"error": "Invalid JSON response", "raw": resp.text[:500]}
            
            # Remove unwanted credits from GST API
            if api_type == 'gst' and isinstance(raw_data, dict):
                keys_to_remove = []
                for key, value in raw_data.items():
                    if isinstance(value, str) and ('t.me/anshapi' in value or 'credit' in value.lower()):
                        keys_to_remove.append(key)
                for key in keys_to_remove:
                    del raw_data[key]
            
            # Add branding to all APIs
            if isinstance(raw_data, dict):
                raw_data.update(get_branding())
            elif isinstance(raw_data, list):
                data = {"results": raw_data}
                data.update(get_branding())
                raw_data = data
            else:
                data = {"data": str(raw_data)}
                data.update(get_branding())
                raw_data = data

    except Exception as e:
        raw_data = {"error": "Server Error", "details": str(e)}
        raw_data.update(get_branding())

    # Format JSON with colors
    formatted_json = json.dumps(raw_data, indent=4, ensure_ascii=False)
    formatted_json = formatted_json.replace('<', '&lt;').replace('>', '&gt;')
    
    # Create colored JSON output
    colored_json = f"""🔍 <b>{api_type.upper()} Lookup Results</b>

<pre><code class="language-json">{formatted_json}</code></pre>

📝 <b>Note:</b> Data is for informational purposes only
👨‍💻 <b>Developer:</b> @Nullprotocol_X
⚡ <b>Powered by:</b> NULL PROTOCOL"""

    await status_msg.edit_text(colored_json, parse_mode="HTML")

    # Deduct credit for non-admins
    if not admin_level:
        await update_credits(user_id, -1)
    
    # Log lookup
    await log_lookup(user_id, api_type, input_data, formatted_json)
    await update_last_active(user_id)

    # Log to channel
    log_channel = LOG_CHANNELS.get(api_type)
    if log_channel:
        try:
            await bot.send_message(
                log_channel,
                f"👤 <b>User:</b> {user_id} (@{message.from_user.username or 'N/A'})\n"
                f"🔎 <b>Type:</b> {api_type}\n"
                f"⌨️ <b>Input:</b> <code>{input_data}</code>\n"
                f"📄 <b>Result:</b>\n<pre>{formatted_json}</pre>",
                parse_mode="HTML"
            )
        except Exception as e:
            logging.error(f"Failed to log to channel: {e}")

# --- INPUT HANDLERS FOR APIs ---
@dp.callback_query(F.data.startswith("api_"))
async def ask_api_input(callback: types.CallbackQuery, state: FSMContext):
    if await is_user_banned(callback.from_user.id): 
        return
    if not await check_membership(callback.from_user.id):
        await callback.answer("❌ Join channels first!", show_alert=True)
        return
    
    api_type = callback.data.split('_')[1]
    
    # Set state for API input
    await state.set_state(Form.waiting_for_api_input)
    await state.update_data(api_type=api_type)
    
    api_map = {
        'num': "📱 Enter Mobile Number (10 digits)",
        'ifsc': "🏦 Enter IFSC Code (11 characters)",
        'email': "📧 Enter Email Address",
        'gst': "📋 Enter GST Number (15 characters)",
        'vehicle': "🚗 Enter Vehicle RC Number",
        'pincode': "📮 Enter Pincode (6 digits)"
    }
    
    if api_type in api_map:
        await callback.message.answer(
            f"<b>{api_map[api_type]}</b>\n\n"
            f"<i>Type /cancel to cancel</i>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Cancel", callback_data="cancel_api")]
            ])
        )

@dp.callback_query(F.data == "cancel_api")
async def cancel_api_handler(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.message.answer("❌ Operation Cancelled.", 
                                reply_markup=get_main_menu(callback.from_user.id))

# --- FIXED BROADCAST HANDLER ---
@dp.message(Form.waiting_for_broadcast)
async def broadcast_message(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        await state.clear()
        return
    
    users = await get_all_users()
    sent = 0
    failed = 0
    status = await message.answer("🚀 Broadcasting to all users...")
    
    # Handle different message types
    for uid in users:
        try:
            # Copy the message (works for all types: text, photo, video, etc.)
            await message.copy_to(uid)
            sent += 1
            await asyncio.sleep(0.05)  # Small delay to avoid flood
        except Exception as e:
            failed += 1
    
    await status.edit_text(
        f"✅ <b>Broadcast Complete!</b>\n\n"
        f"✅ Sent: <b>{sent}</b>\n"
        f"❌ Failed: <b>{failed}</b>\n"
        f"👥 Total Users: <b>{len(users)}</b>",
        parse_mode="HTML"
    )
    await state.clear()

# --- MESSAGE HANDLER FOR ALL INPUTS ---
@dp.message(F.text & ~F.text.startswith("/"))
async def handle_inputs(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    if await is_user_banned(user_id): 
        return
    
    current_state = await state.get_state()
    
    # API input state
    if current_state == Form.waiting_for_api_input.state:
        data = await state.get_data()
        api_type = data.get('api_type')
        
        if api_type:
            await process_api_call(message, api_type, message.text.strip())
        await state.clear()
        return
    
    # Redeem code state
    elif current_state == Form.waiting_for_redeem.state:
        code = message.text.strip().upper()
        result = await redeem_code_db(user_id, code)
        
        if isinstance(result, int):  # Success, returns amount
            user_data = await get_user(user_id)
            new_balance = user_data[2] + result if user_data else result
            await message.answer(
                f"✅ <b>Code Redeemed Successfully!</b>\n"
                f"➕ <b>{result} Credits</b> added to your account.\n\n"
                f"💰 <b>New Balance:</b> {new_balance}",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "already_claimed":
            await message.answer(
                "❌ <b>You have already claimed this code!</b>\n"
                "Each user can claim a code only once.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "invalid":
            await message.answer(
                "❌ <b>Invalid Code!</b>\n"
                "Please check the code and try again.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "inactive":
            await message.answer(
                "❌ <b>Code is Inactive!</b>\n"
                "This code has been deactivated by admin.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "limit_reached":
            await message.answer(
                "❌ <b>Code Limit Reached!</b>\n"
                "This code has been used by maximum users.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        elif result == "expired":
            await message.answer(
                "❌ <b>Code Expired!</b>\n"
                "This code is no longer valid.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        else:
            await message.answer(
                "❌ <b>Error processing code!</b>\n"
                "Please try again later.",
                parse_mode="HTML",
                reply_markup=get_main_menu(user_id)
            )
        
        await state.clear()
        return
    
    # Direct message states
    elif current_state == Form.waiting_for_dm_user.state:
        try:
            target_id = int(message.text.strip())
            await state.update_data(dm_user_id=target_id)
            await message.answer(f"📨 Now send the message for user {target_id}:")
            await state.set_state(Form.waiting_for_dm_content)
        except:
            await message.answer("❌ Invalid user ID. Please enter a numeric ID.")
        return
    
    elif current_state == Form.waiting_for_dm_content.state:
        data = await state.get_data()
        target_id = data.get('dm_user_id')
        
        if target_id:
            try:
                await message.copy_to(target_id)
                await message.answer(f"✅ Message sent to user {target_id}")
            except Exception as e:
                await message.answer(f"❌ Failed to send message: {str(e)}")
        
        await state.clear()
        return
    
    # Custom code creation state
    elif current_state == Form.waiting_for_custom_code.state:
        try:
            parts = message.text.strip().split()
            if len(parts) < 3:
                raise ValueError("Minimum 3 arguments required")
            
            code = parts[0].upper()
            amt = int(parts[1])
            uses = int(parts[2])
            
            expiry_minutes = None
            if len(parts) >= 4:
                expiry_minutes = parse_time_string(parts[3])
            
            await create_redeem_code(code, amt, uses, expiry_minutes)
            
            # Format expiry text
            expiry_text = ""
            if expiry_minutes:
                if expiry_minutes < 60:
                    expiry_text = f"⏰ Expires in: {expiry_minutes} minutes"
                else:
                    hours = expiry_minutes // 60
                    mins = expiry_minutes % 60
                    expiry_text = f"⏰ Expires in: {hours}h {mins}m"
            else:
                expiry_text = "⏰ No expiry"
            
            await message.answer(
                f"✅ <b>Code Created!</b>\n\n"
                f"🎫 <b>Code:</b> <code>{code}</code>\n"
                f"💰 <b>Amount:</b> {amt} credits\n"
                f"👥 <b>Max Uses:</b> {uses}\n"
                f"{expiry_text}\n\n"
                f"📝 <i>Note: Each user can claim only once</i>",
                parse_mode="HTML"
            )
        except Exception as e:
            await message.answer(
                f"❌ <b>Error:</b> {str(e)}\n\n"
                f"<b>Format:</b> <code>CODE AMOUNT USES [TIME]</code>\n"
                f"<b>Examples:</b>\n"
                f"• <code>WELCOME50 50 10</code>\n"
                f"• <code>FLASH100 100 5 15m</code>\n"
                f"• <code>SPECIAL200 200 3 1h</code>",
                parse_mode="HTML"
            )
        await state.clear()
        return
    
    # Stats range state
    elif current_state == Form.waiting_for_stats_range.state:
        try:
            days = int(message.text.strip())
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            users = await get_users_in_range(start_date.timestamp(), end_date.timestamp())
            
            if not users:
                await message.answer(f"❌ No users found in last {days} days.")
                return
            
            # Create CSV file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['User ID', 'Username', 'Credits', 'Join Date'])
                for user in users:
                    join_date = datetime.fromtimestamp(float(user[3])).strftime('%Y-%m-%d %H:%M:%S')
                    writer.writerow([user[0], user[1] or 'N/A', user[2], join_date])
                temp_file = f.name
            
            await message.reply_document(
                FSInputFile(temp_file),
                caption=f"📊 Users data for last {days} days\nTotal users: {len(users)}"
            )
            
            # Clean up
            os.unlink(temp_file)
            
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        
        await state.clear()
        return
    
    # Code deactivate state
    elif current_state == Form.waiting_for_code_deactivate.state:
        try:
            code = message.text.strip().upper()
            await deactivate_code(code)
            await message.answer(f"✅ Code <code>{code}</code> has been deactivated.", parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # Username search state
    elif current_state == Form.waiting_for_username.state:
        username = message.text.strip()
        user_id = await get_user_by_username(username)
        
        if user_id:
            user_data = await get_user(user_id)
            msg = (f"👤 <b>User Found</b>\n\n"
                   f"🆔 <b>ID:</b> <code>{user_data[0]}</code>\n"
                   f"👤 <b>Username:</b> @{user_data[1] or 'N/A'}\n"
                   f"💰 <b>Credits:</b> {user_data[2]}\n"
                   f"📊 <b>Total Earned:</b> {user_data[6]}\n"
                   f"🚫 <b>Banned:</b> {'Yes' if user_data[5] else 'No'}")
            await message.answer(msg, parse_mode="HTML")
        else:
            await message.answer("❌ User not found.")
        
        await state.clear()
        return
    
    # Delete user state
    elif current_state == Form.waiting_for_delete_user.state:
        try:
            uid = int(message.text.strip())
            await delete_user(uid)
            await message.answer(f"✅ User {uid} deleted successfully.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # Reset credits state
    elif current_state == Form.waiting_for_reset_credits.state:
        try:
            uid = int(message.text.strip())
            await reset_user_credits(uid)
            await message.answer(f"✅ Credits reset for user {uid}.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # Code stats state
    elif current_state == Form.waiting_for_code_stats.state:
        try:
            code = message.text.strip().upper()
            stats = await get_code_usage_stats(code)
            
            if stats:
                amount, max_uses, current_uses, unique_users, user_ids = stats
                msg = (f"📊 <b>Code Statistics: {code}</b>\n\n"
                       f"💰 <b>Amount:</b> {amount} credits\n"
                       f"🎯 <b>Uses:</b> {current_uses}/{max_uses}\n"
                       f"👥 <b>Unique Users:</b> {unique_users}\n"
                       f"🆔 <b>Users:</b> {user_ids or 'None'}")
                await message.answer(msg, parse_mode="HTML")
            else:
                await message.answer(f"❌ Code {code} not found.")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # User lookups state
    elif current_state == Form.waiting_for_user_lookups.state:
        try:
            uid = int(message.text.strip())
            lookups = await get_user_lookups(uid, 20)
            
            if not lookups:
                await message.answer(f"❌ No lookups found for user {uid}.")
                return
            
            text = f"📊 <b>Recent Lookups for User {uid}</b>\n\n"
            for i, (api_type, input_data, lookup_date) in enumerate(lookups, 1):
                date_str = datetime.fromisoformat(lookup_date).strftime('%d/%m %H:%M')
                text += f"{i}. {api_type.upper()}: {input_data} - {date_str}\n"
            
            if len(text) > 4000:
                # Send as file if too long
                with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
                    f.write(text)
                    temp_file = f.name
                
                await message.reply_document(
                    FSInputFile(temp_file),
                    caption=f"Lookup history for user {uid}"
                )
                os.unlink(temp_file)
            else:
                await message.answer(text, parse_mode="HTML")
                
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # Bulk gift state
    elif current_state == Form.waiting_for_bulk_gift.state:
        try:
            parts = message.text.strip().split()
            if len(parts) < 2:
                raise ValueError("Format: AMOUNT USERID1 USERID2 ...")
            
            amount = int(parts[0])
            user_ids = [int(uid) for uid in parts[1:]]
            
            await bulk_update_credits(user_ids, amount)
            
            msg = f"✅ Gifted {amount} credits to {len(user_ids)} users:\n"
            for uid in user_ids[:10]:  # Show first 10
                msg += f"• <code>{uid}</code>\n"
            if len(user_ids) > 10:
                msg += f"... and {len(user_ids) - 10} more"
            
            await message.answer(msg, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Error: {str(e)}")
        await state.clear()
        return
    
    # User search state
    elif current_state == Form.waiting_for_user_search.state:
        query = message.text.strip()
        users = await search_users(query)
        
        if not users:
            await message.answer("❌ No users found.")
            return
        
        text = f"🔍 <b>Search Results for '{query}'</b>\n\n"
        for user_id, username, credits in users[:15]:
            text += f"🆔 <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
        
        if len(users) > 15:
            text += f"\n... and {len(users) - 15} more results"
        
        await message.answer(text, parse_mode="HTML")
        await state.clear()
        return
    
    # If no state and user sends random text, show menu
    else:
        # Only respond if user is in no state and sends text
        if message.text.strip():
            await message.answer(
                "Please use the menu buttons to select an option.",
                reply_markup=get_main_menu(user_id)
            )

# Handle media messages in broadcast
@dp.message(Form.waiting_for_broadcast, F.content_type.in_({'photo', 'video', 'audio', 'document'}))
async def broadcast_media(message: types.Message, state: FSMContext):
    # This will be handled by the broadcast_message function
    pass

# --- CANCEL COMMAND ---
@dp.message(Command("cancel"))
async def cancel_command(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ No active operation to cancel.")
        return
    
    await state.clear()
    await message.answer("✅ Operation cancelled.", reply_markup=get_main_menu(message.from_user.id))

# --- ENHANCED ADMIN PANEL ---
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    panel_text = "🛠 <b>ADMIN CONTROL PANEL</b>\n\n"
    
    # Basic commands for all admins
    panel_text += "<b>📊 User Management:</b>\n"
    panel_text += "📢 <code>/broadcast</code> - Send to all users\n"
    panel_text += "📨 <code>/dm</code> - Direct message to user\n"
    panel_text += "🎁 <code>/gift ID AMOUNT</code> - Add credits\n"
    panel_text += "🎁 <code>/bulkgift AMOUNT ID1 ID2...</code> - Bulk gift\n"
    panel_text += "📉 <code>/removecredits ID AMOUNT</code> - Remove credits\n"
    panel_text += "🔄 <code>/resetcredits ID</code> - Reset user credits to 0\n"
    panel_text += "🚫 <code>/ban ID</code> - Ban user\n"
    panel_text += "🟢 <code>/unban ID</code> - Unban user\n"
    panel_text += "🗑 <code>/deleteuser ID</code> - Delete user\n"
    panel_text += "🔍 <code>/searchuser QUERY</code> - Search users\n"
    panel_text += "👥 <code>/users [PAGE]</code> - List users (10 per page)\n"
    panel_text += "📈 <code>/recentusers DAYS</code> - Recent users\n"
    panel_text += "📊 <code>/userlookups ID</code> - User lookup history\n"
    panel_text += "🏆 <code>/leaderboard</code> - Credits leaderboard\n"
    panel_text += "💰 <code>/premiumusers</code> - Premium users (100+ credits)\n"
    panel_text += "📉 <code>/lowcreditusers</code> - Users with low credits\n"
    panel_text += "⏰ <code>/inactiveusers DAYS</code> - Inactive users\n\n"
    
    # Redeem Code Management
    panel_text += "<b>🎫 Code Management:</b>\n"
    panel_text += "🎲 <code>/gencode AMOUNT USES [TIME]</code> - Random code\n"
    panel_text += "🎫 <code>/customcode CODE AMOUNT USES [TIME]</code> - Custom code\n"
    panel_text += "📋 <code>/listcodes</code> - List all codes\n"
    panel_text += "✅ <code>/activecodes</code> - List active codes\n"
    panel_text += "❌ <code>/inactivecodes</code> - List inactive codes\n"
    panel_text += "🚫 <code>/deactivatecode CODE</code> - Deactivate code\n"
    panel_text += "📊 <code>/codestats CODE</code> - Code usage statistics\n"
    panel_text += "⌛️ <code>/checkexpired</code> - Check expired codes\n"
    panel_text += "🧹 <code>/cleanexpired</code> - Remove expired codes\n\n"
    
    # Statistics
    panel_text += "<b>📈 Statistics:</b>\n"
    panel_text += "📊 <code>/stats</code> - Bot statistics\n"
    panel_text += "📅 <code>/dailystats DAYS</code> - Daily statistics\n"
    panel_text += "🔍 <code>/lookupstats</code> - Lookup statistics\n"
    panel_text += "💾 <code>/backup DAYS</code> - Download user data\n"
    panel_text += "🏆 <code>/topref [LIMIT]</code> - Top referrers\n\n"
    
    # Owner-only commands
    if admin_level == 'owner':
        panel_text += "<b>👑 Owner Commands:</b>\n"
        panel_text += "➕ <code>/addadmin ID</code> - Add admin\n"
        panel_text += "➖ <code>/removeadmin ID</code> - Remove admin\n"
        panel_text += "👥 <code>/listadmins</code> - List all admins\n"
        panel_text += "⚙️ <code>/settings</code> - Bot settings\n"
        panel_text += "💾 <code>/fulldbbackup</code> - Full database backup\n"
    
    # Time format examples
    panel_text += "\n<b>⏰ Time Formats:</b>\n"
    panel_text += "• <code>30m</code> = 30 minutes\n"
    panel_text += "• <code>2h</code> = 2 hours\n"
    panel_text += "• <code>1h30m</code> = 1.5 hours\n"
    panel_text += "• <code>1d</code> = 24 hours\n"
    
    # Add quick action buttons
    buttons = [
        [InlineKeyboardButton(text="📊 Quick Stats", callback_data="quick_stats"),
         InlineKeyboardButton(text="👥 Recent Users", callback_data="recent_users")],
        [InlineKeyboardButton(text="🎫 Active Codes", callback_data="active_codes"),
         InlineKeyboardButton(text="🏆 Top Referrers", callback_data="top_ref")],
        [InlineKeyboardButton(text="🚀 Broadcast", callback_data="broadcast_now"),
         InlineKeyboardButton(text="❌ Close", callback_data="close_panel")]
    ]
    
    await message.answer(panel_text, parse_mode="HTML", 
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

# --- BROADCAST COMMAND ---
@dp.message(Command("broadcast"))
async def broadcast_trigger(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer(
        "📢 <b>Send message to broadcast</b> (text, photo, video, audio, document, poll, sticker):\n\n"
        "This will be sent to all users.",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_broadcast)

# --- DIRECT MESSAGE COMMAND ---
@dp.message(Command("dm"))
async def dm_trigger(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("👤 <b>Enter user ID to send message:</b>")
    await state.set_state(Form.waiting_for_dm_user)

# --- NEW ADVANCED COMMANDS ---

# Users list with pagination
@dp.message(Command("users"))
async def users_list(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    page = 1
    if command.args and command.args.isdigit():
        page = int(command.args)
    
    users = await get_all_users()
    total_users = len(users)
    per_page = 10
    total_pages = (total_users + per_page - 1) // per_page
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    text = f"👥 <b>Users List (Page {page}/{total_pages})</b>\n\n"
    
    for i, user_id in enumerate(users[start_idx:end_idx], start=start_idx+1):
        user_data = await get_user(user_id)
        if user_data:
            text += f"{i}. <code>{user_data[0]}</code> - @{user_data[1] or 'N/A'} - {user_data[2]} credits\n"
    
    text += f"\nTotal Users: {total_users}"
    
    # Pagination buttons
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"users_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"users_{page+1}"))
    
    if buttons:
        await message.answer(text, parse_mode="HTML", 
                           reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]))
    else:
        await message.answer(text, parse_mode="HTML")

# Search user
@dp.message(Command("searchuser"))
async def search_user_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("🔍 <b>Enter username or user ID to search:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_user_search)

# Delete user
@dp.message(Command("deleteuser"))
async def delete_user_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("🗑 <b>Enter user ID to delete:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_delete_user)

# Reset credits
@dp.message(Command("resetcredits"))
async def reset_credits_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("🔄 <b>Enter user ID to reset credits:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_reset_credits)

# Recent users
@dp.message(Command("recentusers"))
async def recent_users_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    days = 7
    if command.args and command.args.isdigit():
        days = int(command.args)
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    users = await get_users_in_range(start_date.timestamp(), end_date.timestamp())
    
    text = f"📅 <b>Recent Users (Last {days} days)</b>\n\n"
    
    if not users:
        text += "No users found."
    else:
        for user in users[:20]:  # Show first 20
            join_date = datetime.fromtimestamp(float(user[3])).strftime('%d-%m-%Y')
            text += f"• <code>{user[0]}</code> - @{user[1] or 'N/A'} - {join_date}\n"
        
        if len(users) > 20:
            text += f"\n... and {len(users) - 20} more"
    
    await message.answer(text, parse_mode="HTML")

# Active codes
@dp.message(Command("activecodes"))
async def active_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    codes = await get_active_codes()
    
    if not codes:
        await message.reply("✅ No active codes found.")
        return
    
    text = "✅ <b>Active Redeem Codes</b>\n\n"
    
    for code_data in codes[:10]:  # Show first 10
        code, amount, max_uses, current_uses = code_data
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    
    if len(codes) > 10:
        text += f"\n... and {len(codes) - 10} more active codes"
    
    await message.reply(text, parse_mode="HTML")

# Inactive codes
@dp.message(Command("inactivecodes"))
async def inactive_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    codes = await get_inactive_codes()
    
    if not codes:
        await message.reply("❌ No inactive codes found.")
        return
    
    text = "❌ <b>Inactive Redeem Codes</b>\n\n"
    
    for code_data in codes[:10]:  # Show first 10
        code, amount, max_uses, current_uses = code_data
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    
    if len(codes) > 10:
        text += f"\n... and {len(codes) - 10} more inactive codes"
    
    await message.reply(text, parse_mode="HTML")

# Leaderboard
@dp.message(Command("leaderboard"))
async def leaderboard_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    leaderboard = await get_leaderboard(10)
    
    if not leaderboard:
        await message.reply("❌ No users found.")
        return
    
    text = "🏆 <b>Credits Leaderboard</b>\n\n"
    
    for i, (user_id, username, credits) in enumerate(leaderboard, 1):
        medal = "🥇" if i == 1 else ("🥈" if i == 2 else ("🥉" if i == 3 else f"{i}."))
        text += f"{medal} <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    
    await message.reply(text, parse_mode="HTML")

# Daily stats
@dp.message(Command("dailystats"))
async def daily_stats_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    days = 7
    if command.args and command.args.isdigit():
        days = int(command.args)
    
    stats = await get_daily_stats(days)
    
    text = f"📈 <b>Daily Statistics (Last {days} days)</b>\n\n"
    
    if not stats:
        text += "No statistics available."
    else:
        for date, new_users, lookups in stats:
            text += f"📅 {date}: +{new_users} users, {lookups} lookups\n"
    
    await message.reply(text, parse_mode="HTML")

# Lookup stats
@dp.message(Command("lookupstats"))
async def lookup_stats_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    total_lookups = await get_total_lookups()
    api_stats = await get_lookup_stats()
    
    text = f"🔍 <b>Lookup Statistics</b>\n\n"
    text += f"📊 <b>Total Lookups:</b> {total_lookups}\n\n"
    
    if api_stats:
        text += "<b>By API Type:</b>\n"
        for api_type, count in api_stats:
            text += f"• {api_type.upper()}: {count} lookups\n"
    
    await message.reply(text, parse_mode="HTML")

# User lookups
@dp.message(Command("userlookups"))
async def user_lookups_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("🔍 <b>Enter user ID to view lookup history:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_user_lookups)

# Code stats
@dp.message(Command("codestats"))
async def code_stats_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("📊 <b>Enter code to view statistics:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_code_stats)

# Premium users
@dp.message(Command("premiumusers"))
async def premium_users_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    users = await get_premium_users()
    
    if not users:
        await message.reply("❌ No premium users found.")
        return
    
    text = "💰 <b>Premium Users (100+ credits)</b>\n\n"
    
    for user_id, username, credits in users[:20]:
        text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    
    if len(users) > 20:
        text += f"\n... and {len(users) - 20} more premium users"
    
    await message.reply(text, parse_mode="HTML")

# Low credit users
@dp.message(Command("lowcreditusers"))
async def low_credit_users_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    users = await get_low_credit_users()
    
    if not users:
        await message.reply("✅ No users with low credits.")
        return
    
    text = "📉 <b>Users with Low Credits (≤5 credits)</b>\n\n"
    
    for user_id, username, credits in users[:20]:
        text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {credits} credits\n"
    
    if len(users) > 20:
        text += f"\n... and {len(users) - 20} more users"
    
    await message.reply(text, parse_mode="HTML")

# Inactive users
@dp.message(Command("inactiveusers"))
async def inactive_users_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    days = 30
    if command.args and command.args.isdigit():
        days = int(command.args)
    
    users = await get_inactive_users(days)
    
    if not users:
        await message.reply(f"✅ No inactive users found (last {days} days).")
        return
    
    text = f"⏰ <b>Inactive Users (Last {days} days)</b>\n\n"
    
    for user_id, username, last_active in users[:15]:
        last_active_dt = datetime.fromisoformat(last_active)
        days_ago = (datetime.now() - last_active_dt).days
        text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {days_ago} days ago\n"
    
    if len(users) > 15:
        text += f"\n... and {len(users) - 15} more inactive users"
    
    await message.reply(text, parse_mode="HTML")

# Bulk gift
@dp.message(Command("bulkgift"))
async def bulk_gift_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer(
        "🎁 <b>Bulk Gift Credits</b>\n\n"
        "Format: <code>/bulkgift AMOUNT USERID1 USERID2 USERID3 ...</code>\n\n"
        "Example: <code>/bulkgift 50 123456 789012 345678</code>\n\n"
        "Enter the amount and user IDs separated by spaces:",
        parse_mode="HTML"
    )
    await state.set_state(Form.waiting_for_bulk_gift)

# Full database backup
@dp.message(Command("fulldbbackup"))
async def full_db_backup(message: types.Message):
    if not await is_user_owner(message.from_user.id):
        return
    
    try:
        # Create backup of SQLite database
        backup_name = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        shutil.copy2("nullprotocol.db", backup_name)
        
        await message.reply_document(
            FSInputFile(backup_name),
            caption="💾 Full database backup"
        )
        
        # Clean up
        os.remove(backup_name)
    except Exception as e:
        await message.reply(f"❌ Backup failed: {str(e)}")

# --- EXISTING COMMANDS ---

@dp.message(Command("gift"))
async def gift_credits(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        args = command.args.split()
        uid, amt = int(args[0]), int(args[1])
        await update_credits(uid, amt)
        await message.reply(f"✅ Added {amt} credits to user {uid}")
        
        try:
            await bot.send_message(uid, f"🎁 <b>Admin Gifted You {amt} Credits!</b>", parse_mode="HTML")
        except:
            pass
    except:
        await message.reply("Usage: /gift <user_id> <amount>")

@dp.message(Command("removecredits"))
async def remove_credits(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        args = command.args.split()
        uid, amt = int(args[0]), int(args[1])
        await update_credits(uid, -amt)
        await message.reply(f"✅ Removed {amt} credits from user {uid}")
        
        try:
            await bot.send_message(uid, f"⚠️ <b>Admin Removed {amt} Credits From Your Account!</b>", parse_mode="HTML")
        except:
            pass
    except:
        await message.reply("Usage: /removecredits <user_id> <amount>")

@dp.message(Command("gencode"))
async def generate_random_code(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        args = command.args.split()
        
        if len(args) < 2:
            raise ValueError("Minimum 2 arguments required")
        
        # Format: /gencode AMOUNT USES [TIME]
        amt = int(args[0])
        uses = int(args[1])
        
        expiry_minutes = None
        if len(args) >= 3:
            expiry_minutes = parse_time_string(args[2])
        
        # Generate random code
        code = f"PRO-{secrets.token_hex(3).upper()}"
        
        await create_redeem_code(code, amt, uses, expiry_minutes)
        
        # Format expiry text
        expiry_text = ""
        if expiry_minutes:
            if expiry_minutes < 60:
                expiry_text = f"⏰ Expires in: {expiry_minutes} minutes"
            else:
                hours = expiry_minutes // 60
                mins = expiry_minutes % 60
                expiry_text = f"⏰ Expires in: {hours}h {mins}m"
        else:
            expiry_text = "⏰ No expiry"
        
        await message.reply(
            f"✅ <b>Code Created!</b>\n\n"
            f"🎫 <b>Code:</b> <code>{code}</code>\n"
            f"💰 <b>Amount:</b> {amt} credits\n"
            f"👥 <b>Max Uses:</b> {uses}\n"
            f"{expiry_text}\n\n"
            f"📝 <i>Note: Each user can claim only once</i>",
            parse_mode="HTML"
        )
        
    except Exception as e:
        await message.reply(
            f"❌ <b>Usage:</b> <code>/gencode AMOUNT USES [TIME]</code>\n\n"
            f"<b>Examples:</b>\n"
            f"• <code>/gencode 50 10</code> - No expiry\n"
            f"• <code>/gencode 100 5 30m</code> - 30 minutes expiry\n"
            f"• <code>/gencode 200 3 2h</code> - 2 hours expiry\n"
            f"• <code>/gencode 500 1 1h30m</code> - 1.5 hours expiry\n\n"
            f"<b>Error:</b> {str(e)}",
            parse_mode="HTML"
        )

@dp.message(Command("customcode"))
async def custom_code_command(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer(
        "🎫 <b>Enter code details:</b>\n"
        "Format: <code>CODE AMOUNT USES [TIME]</code>\n\n"
        "Examples:\n"
        "• <code>WELCOME50 50 10</code>\n"
        "• <code>FLASH100 100 5 15m</code>\n"
        "• <code>SPECIAL200 200 3 1h</code>\n\n"
        "Time formats: 30m, 2h, 1h30m",
        parse_mode="HTML"
    )
    await Form.waiting_for_custom_code.set()

@dp.message(Command("listcodes"))
async def list_codes_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    codes = await get_all_codes()
    
    if not codes:
        await message.reply("❌ No redeem codes found.")
        return
    
    text = "🎫 <b>All Redeem Codes</b>\n\n"
    
    for code_data in codes:
        code, amount, max_uses, current_uses, expiry_minutes, created_date, is_active = code_data
        
        status = "✅ Active" if is_active else "❌ Inactive"
        
        expiry_text = ""
        if expiry_minutes:
            created_dt = datetime.fromisoformat(created_date)
            expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
            
            if expiry_dt > datetime.now():
                time_left = expiry_dt - datetime.now()
                hours = time_left.seconds // 3600
                minutes = (time_left.seconds % 3600) // 60
                expiry_text = f"⏳ {hours}h {minutes}m left"
            else:
                expiry_text = "⌛️ Expired"
        else:
            expiry_text = "♾️ No expiry"
        
        text += (
            f"🎟 <b>{code}</b> ({status})\n"
            f"💰 Amount: {amount} | 👥 Uses: {current_uses}/{max_uses}\n"
            f"{expiry_text}\n"
            f"📅 Created: {datetime.fromisoformat(created_date).strftime('%d/%m/%y %H:%M')}\n"
            f"{'-'*30}\n"
        )
    
    # Split if too long
    if len(text) > 4000:
        parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
        for part in parts:
            await message.answer(part, parse_mode="HTML")
    else:
        await message.reply(text, parse_mode="HTML")

@dp.message(Command("deactivatecode"))
async def deactivate_code_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("❌ <b>Enter code to deactivate:</b>", parse_mode="HTML")
    await Form.waiting_for_code_deactivate.set()

@dp.message(Command("checkexpired"))
async def check_expired_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    expired = await get_expired_codes()
    
    if not expired:
        await message.reply("✅ No expired codes found.")
        return
    
    text = "⌛️ <b>Expired Codes</b>\n\n"
    
    for code_data in expired:
        code, amount, current_uses, max_uses, expiry_minutes, created_date = code_data
        
        created_dt = datetime.fromisoformat(created_date)
        expiry_dt = created_dt + timedelta(minutes=expiry_minutes)
        
        text += (
            f"🎟 <code>{code}</code>\n"
            f"💰 Amount: {amount} | 👥 Used: {current_uses}/{max_uses}\n"
            f"⏰ Expired on: {expiry_dt.strftime('%d/%m/%y %H:%M')}\n"
            f"{'-'*20}\n"
        )
    
    text += f"\nTotal: {len(expired)} expired codes"
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("ban"))
async def ban_user_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        uid = int(command.args)
        await set_ban_status(uid, 1)
        await message.reply(f"🚫 User {uid} banned.")
    except:
        await message.reply("Usage: /ban <user_id>")

@dp.message(Command("unban"))
async def unban_user_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    try:
        uid = int(command.args)
        await set_ban_status(uid, 0)
        await message.reply(f"🟢 User {uid} unbanned.")
    except:
        await message.reply("Usage: /unban <user_id>")

@dp.message(Command("stats"))
async def stats_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    stats = await get_bot_stats()
    top_ref = await get_top_referrers(5)
    total_lookups = await get_total_lookups()
    
    stats_text = f"📊 <b>Bot Statistics</b>\n\n"
    stats_text += f"👥 <b>Total Users:</b> {stats['total_users']}\n"
    stats_text += f"📈 <b>Active Users:</b> {stats['active_users']}\n"
    stats_text += f"💰 <b>Total Credits in System:</b> {stats['total_credits']}\n"
    stats_text += f"🎁 <b>Credits Distributed:</b> {stats['credits_distributed']}\n"
    stats_text += f"🔍 <b>Total Lookups:</b> {total_lookups}\n\n"
    
    if top_ref:
        stats_text += "🏆 <b>Top 5 Referrers:</b>\n"
        for i, (ref_id, count) in enumerate(top_ref, 1):
            stats_text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    
    await message.reply(stats_text, parse_mode="HTML")

@dp.message(Command("backup"))
async def backup_cmd(message: types.Message, state: FSMContext):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    await message.answer("📅 <b>Enter number of days for data:</b>\n"
                       "Example: 7 (for last 7 days)\n"
                       "0 for all data")
    await state.set_state(Form.waiting_for_stats_range)

@dp.message(Command("topref"))
async def top_ref_cmd(message: types.Message, command: CommandObject):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    limit = 10
    if command.args and command.args.isdigit():
        limit = int(command.args)
    
    top_ref = await get_top_referrers(limit)
    
    if not top_ref:
        await message.reply("❌ No referrals yet.")
        return
    
    text = f"🏆 <b>Top {limit} Referrers</b>\n\n"
    for i, (ref_id, count) in enumerate(top_ref, 1):
        text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    
    await message.reply(text, parse_mode="HTML")

@dp.message(Command("cleanexpired"))
async def clean_expired_cmd(message: types.Message):
    if not await is_user_owner(message.from_user.id):
        return
    
    expired = await get_expired_codes()
    
    if not expired:
        await message.reply("✅ No expired codes found.")
        return
    
    deleted = 0
    for code_data in expired:
        await delete_redeem_code(code_data[0])
        deleted += 1
    
    await message.reply(f"🧹 Cleaned {deleted} expired codes.")

@dp.message(Command("addadmin"))
async def add_admin_cmd(message: types.Message, command: CommandObject):
    if not await is_user_owner(message.from_user.id):
        return
    
    try:
        uid = int(command.args)
        await add_admin(uid)
        await message.reply(f"✅ User {uid} added as admin.")
    except:
        await message.reply("Usage: /addadmin <user_id>")

@dp.message(Command("removeadmin"))
async def remove_admin_cmd(message: types.Message, command: CommandObject):
    if not await is_user_owner(message.from_user.id):
        return
    
    try:
        uid = int(command.args)
        if uid == OWNER_ID:
            await message.reply("❌ Cannot remove owner!")
            return
        
        await remove_admin(uid)
        await message.reply(f"✅ Admin {uid} removed.")
    except:
        await message.reply("Usage: /removeadmin <user_id>")

@dp.message(Command("listadmins"))
async def list_admins_cmd(message: types.Message):
    admin_level = await is_user_admin(message.from_user.id)
    if not admin_level:
        return
    
    admins = await get_all_admins()
    
    text = "👥 <b>Admin List</b>\n\n"
    
    # Add owner
    text += f"👑 <b>Owner:</b> <code>{OWNER_ID}</code>\n\n"
    
    # Add static admins
    text += "⚙️ <b>Static Admins:</b>\n"
    for admin_id in ADMIN_IDS:
        if admin_id != OWNER_ID:
            text += f"• <code>{admin_id}</code>\n"
    
    # Add database admins
    if admins:
        text += "\n🗃️ <b>Database Admins:</b>\n"
        for user_id, level in admins:
            text += f"• <code>{user_id}</code> - {level}\n"
    
    await message.reply(text, parse_mode="HTML")

# --- ADMIN CALLBACK QUERIES ---
@dp.callback_query(F.data == "quick_stats")
async def quick_stats_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    stats = await get_bot_stats()
    top_ref = await get_top_referrers(3)
    total_lookups = await get_total_lookups()
    
    stats_text = f"📊 <b>Quick Stats</b>\n\n"
    stats_text += f"👥 <b>Total Users:</b> {stats['total_users']}\n"
    stats_text += f"📈 <b>Active Users:</b> {stats['active_users']}\n"
    stats_text += f"💰 <b>Total Credits:</b> {stats['total_credits']}\n"
    stats_text += f"🔍 <b>Total Lookups:</b> {total_lookups}\n\n"
    
    if top_ref:
        stats_text += "🏆 <b>Top 3 Referrers:</b>\n"
        for i, (ref_id, count) in enumerate(top_ref, 1):
            stats_text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    
    await callback.message.edit_text(stats_text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "close_panel")
async def close_panel_callback(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(F.data == "recent_users")
async def recent_users_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    users = await get_recent_users(10)
    
    text = "📅 <b>Recent Users (Last 10)</b>\n\n"
    
    if not users:
        text += "No recent users."
    else:
        for user_id, username, joined_date in users:
            join_dt = datetime.fromtimestamp(float(joined_date))
            text += f"• <code>{user_id}</code> - @{username or 'N/A'} - {join_dt.strftime('%d/%m %H:%M')}\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "active_codes")
async def active_codes_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    codes = await get_active_codes()
    
    if not codes:
        await callback.answer("✅ No active codes found.", show_alert=True)
        return
    
    text = "✅ <b>Active Codes</b>\n\n"
    
    for code, amount, max_uses, current_uses in codes[:5]:
        text += f"🎟 <code>{code}</code> - {amount} credits ({current_uses}/{max_uses})\n"
    
    if len(codes) > 5:
        text += f"\n... and {len(codes) - 5} more"
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "top_ref")
async def top_ref_callback(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    top_ref = await get_top_referrers(5)
    
    if not top_ref:
        await callback.answer("❌ No referrals yet.", show_alert=True)
        return
    
    text = "🏆 <b>Top 5 Referrers</b>\n\n"
    
    for i, (ref_id, count) in enumerate(top_ref, 1):
        text += f"{i}. User <code>{ref_id}</code>: {count} referrals\n"
    
    await callback.message.edit_text(text, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(F.data == "broadcast_now")
async def broadcast_now_callback(callback: types.CallbackQuery, state: FSMContext):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    await callback.message.answer("📢 <b>Send message to broadcast:</b>", parse_mode="HTML")
    await state.set_state(Form.waiting_for_broadcast)
    await callback.answer()

# Pagination for users
@dp.callback_query(F.data.startswith("users_"))
async def users_pagination(callback: types.CallbackQuery):
    admin_level = await is_user_admin(callback.from_user.id)
    if not admin_level:
        return
    
    page = int(callback.data.split("_")[1])
    
    users = await get_all_users()
    total_users = len(users)
    per_page = 10
    total_pages = (total_users + per_page - 1) // per_page
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    
    text = f"👥 <b>Users List (Page {page}/{total_pages})</b>\n\n"
    
    for i, user_id in enumerate(users[start_idx:end_idx], start=start_idx+1):
        user_data = await get_user(user_id)
        if user_data:
            text += f"{i}. <code>{user_data[0]}</code> - @{user_data[1] or 'N/A'} - {user_data[2]} credits\n"
    
    text += f"\nTotal Users: {total_users}"
    
    # Pagination buttons
    buttons = []
    if page > 1:
        buttons.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"users_{page-1}"))
    if page < total_pages:
        buttons.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"users_{page+1}"))
    
    await callback.message.edit_text(text, parse_mode="HTML", 
                                   reply_markup=InlineKeyboardMarkup(inline_keyboard=[buttons]))
    await callback.answer()

# --- MAIN FUNCTION ---
async def main():
    await init_db()
    
    # Initialize static admins
    for admin_id in ADMIN_IDS:
        if admin_id != OWNER_ID:  # Don't add owner as admin again
            await add_admin(admin_id)
    
    print("🚀 OSINT LOOKUP Pro Bot Started...")
    print(f"👑 Owner ID: {OWNER_ID}")
    print(f"👥 Static Admins: {ADMIN_IDS}")
    print(f"🔍 APIs Loaded: {len(APIS)}")
    print(f"📊 Log Channels: {len(LOG_CHANNELS)}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("📱 /start - Start the bot")
    print("🛠️ /admin - Admin panel")
    print("❌ /cancel - Cancel current operation")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())