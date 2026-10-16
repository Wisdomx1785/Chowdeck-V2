#!/usr/bin/env python3
""" Chowdeck Multi-Account Promo Sniper - Telegram bot + HTTP bridge receiver. Rewrite notes ------------- * PROMPT MODE ONLY. The legacy "auto" mode and every stored / pre-configured security answer are gone. When a security-question-enabled code is detected the bot relays the question and captures the user's pasted answer instantly. * Code-priority parsing: a pasted promo code is never swallowed as a security answer. Explicit /ans and /code overrides exist for edge cases. * Fixed admin check: admins are only admins when ADMIN_CHAT_ID is set AND matches (the old logic granted admin to everyone when the var was missing). * Authenticated inbound webhook: POST /tweet and GET /status require WEBHOOK_SECRET (fail closed unless ALLOW_INSECURE_WEBHOOK=true). * Low latency: shared keep-alive HTTP clients, in-memory JSON cache with debounced write-behind to disk, trimmed Telegram chatter. * Burst / hedged claiming (CLAIM_MAX_ATTEMPTS, CLAIM_WINDOW_SECONDS, HEDGE_REQUESTS) to punch through "voucher is in high demand" responses. * Hardened error handling: bounded concurrency, per-request timeouts, no unbounded retry loops, Markdown-safe Telegram sends. * X detection is admin-only: a code detected on the bridge is auto-served to the admin chat plus the admin's linked accounts (all enabled slots fire in parallel, no per-user throttle) and any security question is relayed immediately. Regular users copy the code from the channel broadcast and claim it themselves; the channel broadcast can be toggled from the admin panel. """

import asyncio
import base64
import hmac
import json
import os
import random
import re
import threading
import time
import unicodedata
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip() or None
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()
ALLOW_INSECURE_WEBHOOK = os.getenv("ALLOW_INSECURE_WEBHOOK", "").strip().lower() in (
    "1", "true", "yes", "on",
)
PORT = int(os.getenv("PORT", "8080"))
OCR_API_KEY = os.getenv("OCR_API_KEY", "helloworld").strip() or "helloworld"

ADMIN_MAX_SLOTS = max(1, int(os.getenv("ADMIN_MAX_SLOTS", "20")))
USER_MAX_SLOTS = max(1, int(os.getenv("USER_MAX_SLOTS", "10")))

CLAIM_MAX_ATTEMPTS = max(1, int(os.getenv("CLAIM_MAX_ATTEMPTS", "6")))
CLAIM_WINDOW_SECONDS = float(os.getenv("CLAIM_WINDOW_SECONDS", "4.0"))
HEDGE_REQUESTS = max(1, int(os.getenv("HEDGE_REQUESTS", "3")))
REQUEST_CONCURRENCY = max(10, int(os.getenv("REQUEST_CONCURRENCY", "150")))
# Legacy knob: retained for env compatibility. X detection is admin-only now,
# so there is no multi-user fanout left to throttle.
USER_CONCURRENCY = max(1, int(os.getenv("USER_CONCURRENCY", "12")))
CLAIM_TIMEOUT = float(os.getenv("CLAIM_TIMEOUT", "4.0"))

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip() or _BASE_DIR


def _data_file(name):
    return os.path.join(DATA_DIR, name)


TOKENS_FILE = _data_file("tokens_multi.json")
CARTS_FILE = _data_file("carts_multi.json")
STATE_FILE = _data_file("bot_state.json")
LOCKOUT_FILE = _data_file("lockouts_multi.json")
ACTIVE_SLOT_FILE = _data_file("active_slots.json")
PENDING_PROMPT_FILE = _data_file("pending_prompts.json")
ACCOUNT_TOGGLES_FILE = _data_file("account_toggles.json")
BURNER_FILE = _data_file("admin_burner.json")
BURNER_CART_FILE = _data_file("admin_burner_cart.json")
PORTFOLIO_FILE = _data_file("portfolio.json")
LINKED_ACCOUNTS_FILE = _data_file("linked_accounts.json")
WHITELIST_FILE = _data_file("whitelist.json")
INVITE_CODES_FILE = _data_file("invite_codes.json")

# Codes / words that must never be treated as promo codes.
_IGNORE_TOKENS = {
    "CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL",
    "ABUJA", "LAGOS", "GHANA", "ISMAIL", "MAY", "JUNE", "JULY",
    "TRUE", "FALSE", "PROMO", "CODE", "VOUCHER", "ANSWER", "HELP",
    "START", "STOP", "YES", "NO",
    # Ordinary chatter that can show up in ALL CAPS. Only consulted for
    # digit-free tokens, so real codes such as CHOW5K are never affected.
    "PLANS", "SLIGHT", "DROPS", "WILD", "SECONDS", "TOMORROW", "TONIGHT",
    "MAYBE", "LATER", "SOON", "SAME", "TIME", "NEXT", "CHANGE", "FIRST",
    "LATEST", "NEW", "POST", "VOUCHERS", "CODES", "PROMOS",
}

# ---------------------------------------------------------------------------
# In-memory JSON store with debounced write-behind
# ---------------------------------------------------------------------------
_store_lock = threading.RLock()
_cache = {}
_dirty = set()
_dirty_event = threading.Event()
_writer_started = False


def _atomic_write(path, data):
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, path)
    except Exception as e:  # noqa: BLE001 - never let persistence kill the bot
        print(f"[store] failed to persist {path}: {e}", flush=True)
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass


def _writer_loop():
    while True:
        _dirty_event.wait()
        time.sleep(0.25)  # debounce: coalesce rapid successive writes
        with _store_lock:
            paths = list(_dirty)
            _dirty.clear()
            _dirty_event.clear()
            snapshot = {p: _cache.get(p) for p in paths}
        for path, data in snapshot.items():
            if data is not None:
                _atomic_write(path, data)


def start_store_writer():
    global _writer_started
    with _store_lock:
        if _writer_started:
            return
        _writer_started = True
    threading.Thread(target=_writer_loop, name="json-writer", daemon=True).start()


def load_json(file_path):
    """Return the cached document, hydrating from disk on first touch."""
    with _store_lock:
        if file_path in _cache:
            return _cache[file_path]
    data = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
        except Exception as e:  # noqa: BLE001
            print(f"[store] error reading {file_path}: {e}", flush=True)
            data = {}
    if not isinstance(data, dict):
        data = {}
    with _store_lock:
        _cache[file_path] = data
        return data


def save_json(file_path, data):
    with _store_lock:
        _cache[file_path] = data
        _dirty.add(file_path)
    _dirty_event.set()


def flush_store():
    """Force pending cache writes to disk (used on shutdown)."""
    with _store_lock:
        paths = list(_dirty)
        _dirty.clear()
        _dirty_event.clear()
        snapshot = {p: _cache.get(p) for p in paths}
    for path, data in snapshot.items():
        if data is not None:
            _atomic_write(path, data)


# ---------------------------------------------------------------------------
# Identity / access control
# ---------------------------------------------------------------------------
def is_admin_chat(chat_str):
    """Only an explicitly configured ADMIN_CHAT_ID user is an admin. The previous logic (``not ADMIN_CHAT_ID or chat_str == ADMIN_CHAT_ID``) granted admin rights to every user whenever ADMIN_CHAT_ID was unset. """
    chat_str = str(chat_str)
    return bool(ADMIN_CHAT_ID) and chat_str == ADMIN_CHAT_ID


def is_user_whitelisted(chat_str):
    chat_str = str(chat_str)
    if is_admin_chat(chat_str):
        return True
    whitelist = load_json(WHITELIST_FILE)
    return bool(whitelist.get(chat_str, False))


def add_to_whitelist(chat_str):
    chat_str = str(chat_str)
    whitelist = load_json(WHITELIST_FILE)
    whitelist[chat_str] = True
    save_json(WHITELIST_FILE, whitelist)


def remove_from_whitelist(chat_str):
    chat_str = str(chat_str)
    whitelist = load_json(WHITELIST_FILE)
    if chat_str in whitelist:
        del whitelist[chat_str]
        save_json(WHITELIST_FILE, whitelist)


def get_max_slots(chat_str):
    return ADMIN_MAX_SLOTS if is_admin_chat(chat_str) else USER_MAX_SLOTS


# ---------------------------------------------------------------------------
# Small state helpers
# ---------------------------------------------------------------------------
def get_bot_active():
    return bool(load_json(STATE_FILE).get("active", True))


def set_bot_active(val):
    state = load_json(STATE_FILE)
    state["active"] = bool(val)
    save_json(STATE_FILE, state)


def get_channel_announce():
    """Whether detected codes are announced to the public Telegram channel."""
    return bool(load_json(STATE_FILE).get("channel_announce", True))


def set_channel_announce(val):
    state = load_json(STATE_FILE)
    state["channel_announce"] = bool(val)
    save_json(STATE_FILE, state)


def get_active_slot(chat_str):
    slots = load_json(ACTIVE_SLOT_FILE)
    return str(slots.get(str(chat_str), "1"))


def set_active_slot(chat_str, slot):
    slots = load_json(ACTIVE_SLOT_FILE)
    slots[str(chat_str)] = str(slot)
    save_json(ACTIVE_SLOT_FILE, slots)


def is_slot_active_state(chat_str, slot_id):
    toggles = load_json(ACCOUNT_TOGGLES_FILE).get(str(chat_str), {})
    return bool(toggles.get(str(slot_id), True))


def toggle_slot_state(chat_str, slot_id):
    toggles = load_json(ACCOUNT_TOGGLES_FILE)
    user_toggles = toggles.setdefault(str(chat_str), {})
    current = bool(user_toggles.get(str(slot_id), True))
    user_toggles[str(slot_id)] = not current
    save_json(ACCOUNT_TOGGLES_FILE, toggles)
    return not current


def get_burner_credentials(chat_str):
    return load_json(BURNER_FILE).get(str(chat_str), {})


def save_burner_credentials(chat_str, token_data):
    burners = load_json(BURNER_FILE)
    burners[str(chat_str)] = token_data
    save_json(BURNER_FILE, burners)


def get_burner_cart(chat_str):
    return load_json(BURNER_CART_FILE).get(str(chat_str), {})


def save_burner_cart(chat_str, cart_data):
    carts = load_json(BURNER_CART_FILE)
    carts[str(chat_str)] = cart_data
    save_json(BURNER_CART_FILE, carts)


def get_linked_accounts(chat_str):
    return list(load_json(LINKED_ACCOUNTS_FILE).get(str(chat_str), []))


def add_linked_account(chat_str, secondary_chat_id):
    linked = load_json(LINKED_ACCOUNTS_FILE)
    bucket = linked.setdefault(str(chat_str), [])
    sec_str = str(secondary_chat_id).strip()
    if sec_str and sec_str not in bucket:
        bucket.append(sec_str)
        save_json(LINKED_ACCOUNTS_FILE, linked)


def remove_linked_account(chat_str, secondary_chat_id):
    linked = load_json(LINKED_ACCOUNTS_FILE)
    bucket = linked.get(str(chat_str))
    if bucket:
        sec_str = str(secondary_chat_id).strip()
        if sec_str in bucket:
            bucket.remove(sec_str)
            save_json(LINKED_ACCOUNTS_FILE, linked)


def get_admin_stats():
    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    total_users = len(tokens_db)
    total_slots = sum(len(slots) for slots in tokens_db.values())
    total_carts = sum(len(carts) for carts in carts_db.values())
    return total_users, total_slots, total_carts


def record_portfolio_win(chat_str, code, slot_id, amount=2000):
    chat_str = str(chat_str)
    portfolio = load_json(PORTFOLIO_FILE)
    bucket = portfolio.setdefault(chat_str, {"items": [], "total_value": 0})
    already = any(
        item.get("code") == code and str(item.get("slot")) == str(slot_id)
        for item in bucket.get("items", [])
    )
    if not already:
        bucket.setdefault("items", []).append({
            "code": code,
            "slot": str(slot_id),
            "amount": amount,
            "timestamp": time.time(),
        })
        bucket["total_value"] = sum(
            item.get("amount", 2000) for item in bucket["items"]
        )
        save_json(PORTFOLIO_FILE, portfolio)


# ---------------------------------------------------------------------------
# Pending security-question prompts (FIFO queue per chat)
# ---------------------------------------------------------------------------
def add_or_update_pending(chat_str, entry):
    store = load_json(PENDING_PROMPT_FILE)
    bucket = [e for e in store.get(str(chat_str), []) if e.get("code") != entry.get("code")]
    bucket.append(entry)
    store[str(chat_str)] = bucket[-10:]
    save_json(PENDING_PROMPT_FILE, store)


def list_pending(chat_str):
    return list(load_json(PENDING_PROMPT_FILE).get(str(chat_str), []))


def pop_pending(chat_str, code=None, reply_to=None):
    """Remove and return the prompt a user is answering. Resolution order: explicit code -> Telegram reply target -> oldest pending. """
    store = load_json(PENDING_PROMPT_FILE)
    bucket = list(store.get(str(chat_str), []))
    target = None
    if code:
        target = next((e for e in bucket if e.get("code") == code), None)
    elif reply_to is not None:
        target = next((e for e in bucket if e.get("msg_id") == reply_to), None)
    if target is None and bucket:
        target = bucket[0]
    if target is not None:
        bucket = [e for e in bucket if e is not target]
        store[str(chat_str)] = bucket
        save_json(PENDING_PROMPT_FILE, store)
    return target


def clear_pending(chat_str, code=None):
    store = load_json(PENDING_PROMPT_FILE)
    bucket = list(store.get(str(chat_str), []))
    if code:
        bucket = [e for e in bucket if e.get("code") != code]
    else:
        bucket = []
    store[str(chat_str)] = bucket
    save_json(PENDING_PROMPT_FILE, store)
    return bucket


# ---------------------------------------------------------------------------
# Code / credential parsing helpers
# ---------------------------------------------------------------------------
def robust_clean_code(raw):
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", str(raw))
    return "".join(c for c in normalized if c.isalnum()).upper()


def looks_like_code(raw):
    """Return a normalized promo code, or None when the text is not code-like. Used for code-priority parsing so a fresh promo code pasted while a security prompt is open is treated as a code, not as the answer. """
    cleaned = robust_clean_code(raw)
    if not (4 <= len(cleaned) <= 24):
        return None
    if cleaned in _IGNORE_TOKENS:
        return None
    if cleaned.startswith("INVITE"):
        return None
    if cleaned.isdigit():
        # Bare numbers are far more likely to be an answer than a code.
        return None
    return cleaned


def parse_jwt_payload(token):
    try:
        parts = str(token).split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            decoded = base64.urlsafe_b64decode(payload_b64)
            return json.loads(decoded.decode("utf-8"))
    except Exception:
        pass
    return {}


def extract_phone_from_data(data):
    if not isinstance(data, dict):
        return None
    user = data.get("user") if isinstance(data.get("user"), dict) else {}
    return (
        data.get("phone_number")
        or data.get("phone")
        or data.get("phoneNumber")
        or data.get("mobile")
        or data.get("telephone")
        or data.get("user_phone")
        or user.get("phone_number")
        or user.get("phone")
    )


def parse_and_store_credentials(raw_input, chat_str, slot):
    cleaned = str(raw_input).strip()
    extracted_token = cleaned
    extracted_cookies = {}
    extracted_phone = None

    if "user=" in cleaned or ";" in cleaned:
        try:
            for part in cleaned.split(";"):
                part = part.strip()
                if "=" in part:
                    k, v = part.split("=", 1)
                    extracted_cookies[k.strip()] = v.strip()
                    if k.strip() == "user":
                        decoded = urllib.parse.unquote(v.strip())
                        data = json.loads(decoded)
                        if "token" in data:
                            extracted_token = data["token"]
                        extracted_phone = extract_phone_from_data(data)
        except Exception as e:  # noqa: BLE001
            print(f"[creds] cookie parse error: {e}", flush=True)

    if cleaned.startswith("{") and "token" in cleaned:
        try:
            data = json.loads(cleaned)
            if "token" in data:
                extracted_token = data["token"]
            extracted_phone = extract_phone_from_data(data)
        except Exception:
            pass

    if extracted_token.lower().startswith("bearer "):
        extracted_token = extracted_token[7:].strip()

    if not extracted_phone and extracted_token.count(".") == 2:
        extracted_phone = extract_phone_from_data(parse_jwt_payload(extracted_token))

    tokens_db = load_json(TOKENS_FILE)
    user_slots = tokens_db.setdefault(str(chat_str), {})
    existing_phone = user_slots.get(str(slot), {}).get("phone")
    final_phone = extracted_phone or existing_phone

    user_slots[str(slot)] = {
        "token": extracted_token,
        "cookies": extracted_cookies,
        "phone": str(final_phone)[:15] if final_phone else None,
    }
    save_json(TOKENS_FILE, tokens_db)
    return extracted_token


async def fetch_account_phone(token, cookies):
    if token and token.count(".") == 2:
        phone = extract_phone_from_data(parse_jwt_payload(token))
        if phone:
            return phone

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": "https://chowdeck.com",
        "Referer": "https://chowdeck.com/",
        "User-Agent": _UA,
        "Accept": "application/json, text/plain, */*",
    }
    try:
        res = await _CD_CLIENT.get(
            "https://api.chowdeck.com/customer/profile",
            headers=headers,
            cookies=cookies or None,
        )
        if res.status_code == 200:
            res_json = res.json()
            data = res_json.get("data", res_json)
            if not isinstance(data, dict):
                data = res_json
            return extract_phone_from_data(data)
    except Exception as e:  # noqa: BLE001
        print(f"[creds] profile fetch error: {e}", flush=True)
    return None


# ---------------------------------------------------------------------------
# Shared HTTP clients (connection reuse = lower latency)
# ---------------------------------------------------------------------------
_UA = (
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
)

_CD_CLIENT = httpx.AsyncClient(
    timeout=CLAIM_TIMEOUT,
    proxy=PROXY_URL,
    follow_redirects=True,
    limits=httpx.Limits(
        max_keepalive_connections=REQUEST_CONCURRENCY,
        max_connections=REQUEST_CONCURRENCY * 2,
        keepalive_expiry=30.0,
    ),
)

_TG_CLIENT = httpx.AsyncClient(
    timeout=httpx.Timeout(6.0, connect=4.0),
    proxy=PROXY_URL,
    limits=httpx.Limits(max_keepalive_connections=64, max_connections=128),
)

_REQUEST_SEM = asyncio.Semaphore(REQUEST_CONCURRENCY)

_CHOW_HEADERS_BASE = {
    "Content-Type": "application/json",
    "Origin": "https://chowdeck.com",
    "Referer": "https://chowdeck.com/",
    "User-Agent": _UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}


def chow_headers(token):
    headers = dict(_CHOW_HEADERS_BASE)
    headers["Authorization"] = f"Bearer {token}"
    return headers


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------
async def send_telegram(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    """Send a message. Retries once without parse_mode on formatting errors."""
    if not chat_id or not TELEGRAM_BOT_TOKEN:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    for attempt in range(2):
        try:
            res = await _TG_CLIENT.post(url, json=payload)
            if res.status_code == 200:
                return res.json().get("result", {}).get("message_id")
            body = res.text.lower()
            if attempt == 0 and ("parse" in body or "entit" in body):
                payload.pop("parse_mode", None)
                continue
            return None
        except Exception as e:  # noqa: BLE001
            if attempt == 0:
                continue
            print(f"[tg] send error to {chat_id}: {e}", flush=True)
            return None
    return None


async def broadcast_to_channel(text):
    if not TELEGRAM_CHANNEL_ID:
        return
    await send_telegram(TELEGRAM_CHANNEL_ID, text)


async def announce_channel(text):
    """Post to the public channel only when channel announcements are enabled. The admin can flip this off from the admin panel (btn_toggle_channel) so detected codes are served only to the admin without a public broadcast. """
    if not get_channel_announce():
        return
    await broadcast_to_channel(text)


def get_admin_claim_targets():
    """Admin chat plus the admin's linked secondary accounts. These are the ONLY accounts an X-detected code is auto-claimed for. Regular whitelisted users grab the code from the channel broadcast and claim it themselves (paste the code, or /code <CODE>). """
    targets = []
    if ADMIN_CHAT_ID:
        targets.append(ADMIN_CHAT_ID)
        for linked in get_linked_accounts(ADMIN_CHAT_ID):
            linked = str(linked)
            if linked not in targets:
                targets.append(linked)
    return targets


async def answer_callback_query(callback_query_id, text=""):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        await _TG_CLIENT.post(
            url, json={"callback_query_id": callback_query_id, "text": text}
        )
    except Exception as e:  # noqa: BLE001
        print(f"[tg] callback answer error: {e}", flush=True)


async def edit_message_text(chat_id, message_id, text, reply_markup=None, parse_mode="Markdown"):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "reply_markup": reply_markup,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    for attempt in range(2):
        try:
            res = await _TG_CLIENT.post(url, json=payload)
            if res.status_code == 200:
                return
            if attempt == 0 and ("parse" in res.text.lower() or "entit" in res.text.lower()):
                payload.pop("parse_mode", None)
                continue
            return
        except Exception as e:  # noqa: BLE001
            if attempt == 0:
                continue
            print(f"[tg] edit error: {e}", flush=True)


async def perform_ocr_on_image(image_bytes):
    try:
        files = {"file": ("screenshot.jpg", image_bytes, "image/jpeg")}
        data = {"apikey": OCR_API_KEY, "language": "eng", "isOverlayRequired": False}
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(
                "https://api.ocr.space/parse/image", files=files, data=data
            )
        if res.status_code == 200:
            parsed = res.json().get("ParsedResults", [])
            if parsed:
                return parsed[0].get("ParsedText", "")
    except Exception as e:  # noqa: BLE001
        print(f"[ocr] error: {e}", flush=True)
    return ""


# ---------------------------------------------------------------------------
# Keyboards (prompt mode only - no mode toggle)
# ---------------------------------------------------------------------------
def build_control_keyboard(chat_str, is_admin):
    active_slot = get_active_slot(chat_str)
    max_slots = get_max_slots(chat_str)

    rows = [[], [], [], []]
    for i in range(1, max_slots + 1):
        s_str = str(i)
        icon = "✅" if is_slot_active_state(chat_str, s_str) else "❌"
        label = f"📌 S{i}" if s_str == active_slot else f"{icon} {i}"
        btn = {"text": label, "callback_data": f"slot_{i}"}
        if i <= 5:
            rows[0].append(btn)
        elif i <= 10:
            rows[1].append(btn)
        elif i <= 15:
            rows[2].append(btn)
        else:
            rows[3].append(btn)

    keyboard = [rows[0], rows[1]]
    for extra in (rows[2], rows[3]):
        if extra:
            keyboard.append(extra)

    keyboard.extend([
        [
            {"text": "👥 My Accounts", "callback_data": "btn_my_accounts"},
            {"text": "💰 Portfolio", "callback_data": "btn_portfolio"},
        ],
        [
            {"text": "🔄 Refresh", "callback_data": "btn_status"},
            {"text": "📖 Guide / Help", "callback_data": "btn_guide"},
        ],
    ])

    if is_admin:
        keyboard.insert(len(keyboard) - 1, [
            {"text": "🛡 Admin Stats", "callback_data": "btn_admin_stats"},
            {"text": "🧪 Burner Panel", "callback_data": "btn_burner_menu"},
        ])
        keyboard.insert(len(keyboard) - 1, [
            {"text": "🔗 Linked Accounts", "callback_data": "btn_linked_menu"},
            {"text": "🔑 Invites", "callback_data": "btn_invites_menu"},
        ])
        keyboard.append([
            {"text": "⏸️ Pause", "callback_data": "btn_pause"},
            {"text": "▶️ Resume", "callback_data": "btn_resume"},
        ])
        chan_on = get_channel_announce()
        keyboard.append([
            {"text": f"📣 Channel: {'ON' if chan_on else 'OFF'}",
             "callback_data": "btn_toggle_channel"},
        ])
    return {"inline_keyboard": keyboard}


def build_accounts_keyboard(chat_str):
    tokens_db = load_json(TOKENS_FILE).get(str(chat_str), {})
    max_slots = get_max_slots(chat_str)
    rows, row = [], []
    for i in range(1, max_slots + 1):
        s_str = str(i)
        icon = "✅" if is_slot_active_state(chat_str, s_str) else "❌"
        phone_label = tokens_db.get(s_str, {}).get("phone")
        label = f"{icon} S{i}: {phone_label}" if phone_label else f"{icon} S{i} (Empty)"
        row.append({"text": label, "callback_data": f"toggle_acc_{i}"})
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([
        {"text": "🔄 Refresh Phones", "callback_data": "btn_refresh_phones"},
        {"text": "🔙 Main Menu", "callback_data": "btn_main_menu"},
    ])
    return {"inline_keyboard": rows}


def build_admin_burner_keyboard():
    return {"inline_keyboard": [
        [{"text": "🧪 Test Code (send /test CODE)", "callback_data": "burner_info"}],
        [{"text": "🔙 Back to Main Menu", "callback_data": "btn_main_menu"}],
    ]}


def build_admin_linked_keyboard():
    return {"inline_keyboard": [
        [{"text": "🔙 Back to Main Menu", "callback_data": "btn_main_menu"}],
    ]}


def build_admin_invites_keyboard():
    return {"inline_keyboard": [
        [{"text": "➕ Generate Invite Code", "callback_data": "btn_gen_invite"}],
        [{"text": "📋 List Whitelist", "callback_data": "btn_list_whitelist"}],
        [{"text": "🔙 Back to Main Menu", "callback_data": "btn_main_menu"}],
    ]}


# ---------------------------------------------------------------------------
# Claim core - hedged single-slot claim (prompt mode only)
# ---------------------------------------------------------------------------
def _safe_json(res):
    try:
        return res.json()
    except Exception:
        return {}


async def _raw_post(client, url, payload, headers, cookies):
    async with _REQUEST_SEM:
        res = await client.post(url, json=payload, headers=headers, cookies=cookies or None)
        return res, _safe_json(res)


async def _hedged_post(client, url, payload, headers, cookies, n):
    """Fire N near-simultaneous identical requests; return the best response. Hedging is what lets a slot beat the "voucher is in high demand" throttle: the first successful claim wins and the duplicates are discarded. """
    n = max(1, int(n))
    if n == 1:
        return await _raw_post(client, url, payload, headers, cookies)

    async def one(delay):
        if delay > 0:
            await asyncio.sleep(delay)
        return await _raw_post(client, url, payload, headers, cookies)

    tasks = [
        asyncio.create_task(one(random.uniform(0.0, 0.08) if idx else 0.0))
        for idx in range(n)
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    best = None
    for item in results:
        if isinstance(item, BaseException) or item is None:
            continue
        res, body = item
        if res.status_code in (200, 201):
            return res, body
        if best is None:
            best = (res, body)
    if best is not None:
        return best
    return None, {"message": "network error"}


async def _apply_cart(client, headers, cookies, cart_info, code):
    cart_id = (cart_info or {}).get("cart_id")
    if not cart_id:
        return ""
    vendor_id = (cart_info or {}).get("vendor_id")
    payload = {"cart_id": cart_id, "promo_codes": [code]}
    if vendor_id:
        payload["vendor_id"] = vendor_id
    try:
        res = await client.post(
            "https://api.chowdeck.com/customer/discount/apply",
            json=payload,
            headers=headers,
            cookies=cookies or None,
        )
        return f"\nCart: `{res.status_code}` {res.text[:120]}"
    except Exception as e:  # noqa: BLE001
        return f"\nCart apply error: `{e}`"


async def claim_slot_worker(client, chat_id, slot_id, token_data, code, carts_db, lockouts_db, specific_answer=None, deadline=None):
    """Claim `code` for one slot using burst/hedged requests. With no `specific_answer` the first request probes with an empty answer so the API reveals the security question (prompt mode). With a `specific_answer` the answer is submitted immediately. """
    chat_str = str(chat_id)
    slot_key = str(slot_id)

    if not is_slot_active_state(chat_str, slot_key):
        return {"slot": slot_key, "status": 0, "response": {"message": "skipped (inactive)"}}

    token = (token_data or {}).get("token")
    if not token:
        return {"slot": slot_key, "status": 0, "response": {"message": "skipped (no token)"}}

    lockout_key = f"{chat_str}_{slot_key}"
    if not specific_answer and time.time() < lockouts_db.get(lockout_key, 0):
        return {"slot": slot_key, "status": 429, "response": {"message": "locked out"}}

    cookies = (token_data or {}).get("cookies", {})
    headers = chow_headers(token)
    url = f"https://api.chowdeck.com/customer/promo-code/{code}"

    if specific_answer:
        payload = {"code": code, "answer": specific_answer, "security_answer": specific_answer}
    else:
        payload = {"code": code, "answer": "", "security_answer": ""}

    if deadline is None:
        deadline = time.time() + CLAIM_WINDOW_SECONDS

    last_res, last_json = None, {}
    for attempt in range(CLAIM_MAX_ATTEMPTS):
        if time.time() >= deadline:
            break
        try:
            res, body = await _hedged_post(client, url, payload, headers, cookies, HEDGE_REQUESTS)
        except Exception as e:  # noqa: BLE001
            res, body = None, {"message": str(e)}

        if res is not None:
            last_res, last_json = res, body
            if res.status_code in (200, 201):
                break
            msg = str(body.get("message", "")).lower()
            data_obj = body.get("data", {}) if isinstance(body.get("data"), dict) else {}
            sub_code = str(data_obj.get("code", ""))
            congested = (
                res.status_code in (429, 500, 502, 503, 504)
                or sub_code == "claim_busy"
                or "high demand" in msg
                or "try again" in msg
                or "server error" in msg
            )
            if not congested:
                break
        await asyncio.sleep(random.uniform(0.04, 0.14) * (attempt + 1))

    if last_res is None:
        return {"slot": slot_key, "status": 500, "response": {"message": "request failed"}}

    status = last_res.status_code
    msg_text = str(last_json.get("message", "")).lower()
    data_obj = last_json.get("data", {}) if isinstance(last_json.get("data"), dict) else {}

    # Prompt mode: surface the security question when no answer was supplied.
    if not specific_answer:
        question = data_obj.get("security_question") or data_obj.get("question")
        hard_fail = any(
            token_ in msg_text
            for token_ in ("does not exist", "invalid", "expired", "used", "high demand")
        )
        needs_prompt = bool(question) or (status in (400, 422) and not hard_fail)
        if needs_prompt:
            if not question:
                question = last_json.get("message") or "Security question required for this code."
            return {
                "slot": slot_key,
                "status": status,
                "needs_prompt": True,
                "question": str(question)[:300],
                "response": last_json,
            }

    if not specific_answer and "too many failed" in msg_text:
        lockouts_db[lockout_key] = time.time() + 900
        save_json(LOCKOUT_FILE, lockouts_db)
        return {"slot": slot_key, "status": status, "response": last_json,
                "message": "locked out after repeated failures"}

    cart_suffix = ""
    if status in (200, 201):
        cart_suffix = await _apply_cart(
            client, headers, cookies, carts_db.get(chat_str, {}).get(slot_key, {}), code
        )
        record_portfolio_win(chat_str, code, slot_key, amount=2000)

    return {
        "slot": slot_key,
        "status": status,
        "response": last_json,
        "cart_str": cart_suffix,
        "needs_prompt": False,
    }


async def execute_burner_test(chat_id, code):
    chat_str = str(chat_id)
    token_data = get_burner_credentials(chat_str)
    token = token_data.get("token")
    if not token:
        return "❌ Burner account token not configured. Use `/setburnertoken` first."

    cookies = token_data.get("cookies", {})
    headers = chow_headers(token)
    url = f"https://api.chowdeck.com/customer/promo-code/{code}"
    payload = {"code": code, "answer": "", "security_answer": ""}
    try:
        res, body = await _hedged_post(_CD_CLIENT, url, payload, headers, cookies, 1)
        if res is None:
            return "❌ Burner test failed: no response from Chowdeck."
        cart_suffix = ""
        if res.status_code in (200, 201):
            cart_suffix = await _apply_cart(_CD_CLIENT, headers, cookies, get_burner_cart(chat_str), code)
        return (
            f"🧪 *Burner test for* `{code}`\n"
            f"Status: `{res.status_code}`\n"
            f"Response: `{json.dumps(body)[:400]}`{cart_suffix}"
        )
    except Exception as e:  # noqa: BLE001
        return f"❌ Burner test error: {e}"


# ---------------------------------------------------------------------------
# Code extraction / dedupe
# ---------------------------------------------------------------------------
_code_queue = asyncio.Queue(maxsize=5000)
loop_ref = None

_recent_lock = threading.Lock()
_recent_codes = {}


def reserve_code(code, ttl=180):
    """Return False when this code was already processed very recently."""
    now = time.time()
    with _recent_lock:
        for k in [k for k, v in _recent_codes.items() if now - v > ttl]:
            _recent_codes.pop(k, None)
        if code in _recent_codes:
            return False
        _recent_codes[code] = now
    return True


def extract_candidate_codes(text, min_len=5, require_caps=False):
    """Extract promo-code candidates from free text. require_caps=True is used for X (Twitter) input only: Chowdeck posts its real codes in ALL CAPS (ENDEAVOR26, FIVEFORFIVE) while ordinary chatter is sentence-case ("Slight change of plans."), so a token must be typed in caps with at least one letter to qualify. Manual Telegram pastes and OCR text stay case-insensitive (require_caps=False) so a user can still paste a lower-case code by hand. """
    out = []
    for raw in re.findall(r"[A-Za-z0-9]+", text or ""):
        if require_caps and (not any(c.isalpha() for c in raw) or raw != raw.upper()):
            continue
        cleaned = robust_clean_code(raw)
        if not (min_len <= len(cleaned) <= 24):
            continue
        if cleaned in _IGNORE_TOKENS or cleaned.startswith("INVITE"):
            continue
        if cleaned.isdigit():
            continue
        out.append(cleaned)
    return list(dict.fromkeys(out))


def strong_code_signal(text):
    """Stricter test used while a security prompt is open. Promo codes almost always carry a digit; a short purely alphabetic token (a typical security answer) is treated as the answer instead. """
    cleaned = robust_clean_code(text)
    if not (5 <= len(cleaned) <= 24):
        return False
    if cleaned in _IGNORE_TOKENS or cleaned.startswith("INVITE"):
        return False
    return any(ch.isdigit() for ch in cleaned) or len(cleaned) >= 10


def format_slot_range(slots):
    vals = sorted({int(s) for s in slots if str(s).isdigit()})
    if not vals:
        return "Slots"
    if len(vals) == 1:
        return f"Slot {vals[0]}"
    if vals == list(range(vals[0], vals[-1] + 1)):
        return f"Slots {vals[0]}-{vals[-1]}"
    return "Slots " + ", ".join(map(str, vals))


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Claim orchestration
# ---------------------------------------------------------------------------
async def _run_slots(chat_str, code, tokens_db, carts_db, lockouts_db, specific_answer=None):
    user_slots = tokens_db.get(chat_str, {}) or {}
    pending = [
        (str(sid), td) for sid, td in user_slots.items()
        if (td or {}).get("token") and is_slot_active_state(chat_str, str(sid))
    ]
    if not pending:
        return []

    chat_id = _to_int(chat_str)
    if chat_id is None:
        return []

    deadline = time.time() + CLAIM_WINDOW_SECONDS
    tasks = [
        claim_slot_worker(_CD_CLIENT, chat_id, sid, td, code, carts_db, lockouts_db,
                          specific_answer, deadline)
        for sid, td in pending
    ]
    raw = await asyncio.gather(*tasks, return_exceptions=True)

    results = []
    for r in raw:
        if isinstance(r, BaseException):
            results.append({"slot": "?", "status": 500, "response": {"message": str(r)}})
        else:
            results.append(r)
    return results


async def _report_user_results(chat_str, code, results, mirror=False, tag=""):
    """At most one prompt message and/or one result message per user per code."""
    chat_id = _to_int(chat_str)
    if chat_id is None or not results:
        return

    prompts = [r for r in results if r.get("needs_prompt")]
    others = [r for r in results if r.get("status") != 0 and not r.get("needs_prompt")]
    successes = [r for r in others if r.get("status") in (200, 201)]
    head = "🔗 [Mirrored] " if mirror else ""

    if prompts:
        question = str(prompts[0].get("question") or "Security question required.")
        slots = [str(r.get("slot")) for r in prompts]
        entry = {"code": code, "question": question, "slots": slots, "ts": time.time()}
        msg = (
            f"{head}🔒 *Security question for* `{code}` ({format_slot_range(slots)})\n\n"
            f"Q: *\"{question}\"*\n\n"
            f"⚡ Reply with the answer right here — it is claimed instantly.\n"
            f"If you also have a new code, send it and it will be treated as a code."
        )
        msg_id = await send_telegram(chat_id, msg)
        if msg_id:
            entry["msg_id"] = msg_id
        add_or_update_pending(chat_str, entry)

    if others and (successes or not prompts):
        lines = [f"{head}🎯 `{code}` {tag}".rstrip()]
        if successes:
            lines.append(f"✅ Claimed on {format_slot_range([r['slot'] for r in successes])}")
        failures = [r for r in others if r not in successes]
        grouped = {}
        for r in failures:
            resp = r.get("response", {}) if isinstance(r.get("response"), dict) else {}
            label = f"{r.get('status')} {str(resp.get('message', ''))[:60]}".strip()
            grouped.setdefault(label, []).append(str(r.get("slot")))
        for label, slots in list(grouped.items())[:4]:
            lines.append(f"• {label} → {format_slot_range(slots)}")
        if len(grouped) > 4:
            lines.append(f"• …and {len(grouped) - 4} more result type(s)")
        cart = next((r.get("cart_str") for r in successes if r.get("cart_str")), "")
        if cart:
            lines.append(cart.strip())
        await send_telegram(chat_id, "\n".join(lines))


async def claim_code_global(raw_code, announce=True):
    """Serve an X/bridge-detected code to the ADMIN only, instantly. Regular whitelisted users are NOT auto-served any more. They copy the code from the channel broadcast and claim it themselves (paste the code, or `/code <CODE>`), which runs through claim_code_for_chat on their own slots. Only the admin chat and the admin's linked secondary accounts are fired, all enabled slots in parallel with no per-user throttle so the claim lands before the crowd. Any attached security question is relayed immediately. """
    code = robust_clean_code(raw_code)
    if not code or len(code) < 4:
        return
    if not get_bot_active():
        return
    if not reserve_code(code):
        return

    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    lockouts_db = load_json(LOCKOUT_FILE)

    # Admin + linked accounts are the only auto-serve targets.
    targets = [c for c in get_admin_claim_targets() if tokens_db.get(c)]

    announce_text = (
        f"🚨 *New Chowdeck code detected!*\n\nCode: `{code}`\n"
        f"⚡ *Rush to your bot now — it will not wait.*"
    )
    # Announce concurrently so the public channel post never delays the claims.
    announce_task = (
        asyncio.create_task(announce_channel(announce_text)) if announce else None
    )

    try:
        if targets:
            async def run_admin(c_str):
                # No semaphore: admin slots all fire at once.
                try:
                    results = await _run_slots(c_str, code, tokens_db, carts_db, lockouts_db)
                    if results:
                        await _report_user_results(
                            c_str, code, results,
                            mirror=(str(c_str) != ADMIN_CHAT_ID), tag="(X)",
                        )
                except Exception as e:  # noqa: BLE001
                    print(f"[claim] admin target {c_str} error: {e}", flush=True)

            await asyncio.gather(*(run_admin(c) for c in targets))
        elif ADMIN_CHAT_ID:
            # Detected, but the admin has nothing to claim with - tell them.
            await send_telegram(
                _to_int(ADMIN_CHAT_ID),
                f"🚨 Code `{code}` detected on X, but you have no configured slots.\n"
                f"Add a token with `/settoken <token>` to auto-claim future codes.",
            )
    finally:
        if announce_task is not None:
            await announce_task


async def claim_code_for_chat(chat_str, raw_code):
    chat_str = str(chat_str)
    code = robust_clean_code(raw_code)
    if not code or len(code) < 4:
        await send_telegram(_to_int(chat_str), "⚠️ That does not look like a valid promo code.")
        return

    is_admin = is_admin_chat(chat_str)
    if is_admin:
        await announce_channel(
            f"🚨 *New Chowdeck code detected!*\n\nCode: `{code}`\n"
            f"⚡ *Rush to your bot now — it will not wait.*"
        )

    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    lockouts_db = load_json(LOCKOUT_FILE)

    targets = [chat_str]
    if is_admin:
        for linked in get_linked_accounts(chat_str):
            if linked not in targets:
                targets.append(linked)

    for c_str in targets:
        mirror = c_str != chat_str
        results = await _run_slots(c_str, code, tokens_db, carts_db, lockouts_db)
        if not results:
            if not mirror:
                await send_telegram(
                    _to_int(chat_str),
                    "❌ No enabled accounts to claim with. Add a token with `/settoken <token>` "
                    "and make sure the slot is ✅ enabled.",
                )
            continue
        await _report_user_results(c_str, code, results, mirror=mirror, tag="(manual)")


async def handle_answer(chat_str, answer_text, pending_entry):
    """Fire the user's pasted answer at their slots immediately, no storage."""
    chat_str = str(chat_str)
    answer_text = str(answer_text).strip()
    code = (pending_entry or {}).get("code")
    if not answer_text or not code:
        return

    is_admin = is_admin_chat(chat_str)
    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    lockouts_db = load_json(LOCKOUT_FILE)

    targets = [chat_str]
    if is_admin:
        for linked in get_linked_accounts(chat_str):
            if linked not in targets:
                targets.append(linked)

    for c_str in targets:
        mirror = c_str != chat_str
        results = await _run_slots(c_str, code, tokens_db, carts_db, lockouts_db,
                                   specific_answer=answer_text)
        if not results:
            if not mirror:
                await send_telegram(
                    _to_int(chat_str),
                    f"❌ No enabled accounts to submit the answer for `{code}`.",
                )
            continue
        await _report_user_results(c_str, code, results, mirror=mirror, tag="(answer submitted)")


async def worker():
    while True:
        try:
            item = await _code_queue.get()
        except asyncio.CancelledError:
            raise
        try:
            kind, payload = item
            if kind == "global":
                await claim_code_global(payload)
            elif kind == "isolated":
                c_str, code = payload
                await claim_code_for_chat(c_str, code)
        except Exception as e:  # noqa: BLE001
            print(f"[worker] error: {e}", flush=True)
        finally:
            _code_queue.task_done()


async def process_text_global(text, require_caps=True):
    """X (Twitter) intake path - caps-only, see extract_candidate_codes."""
    codes = extract_candidate_codes(text, min_len=5, require_caps=require_caps)
    for code in codes:
        try:
            _code_queue.put_nowait(("global", code))
        except asyncio.QueueFull:
            print("[queue] full, dropping code", flush=True)
    return codes


async def process_text_isolated(chat_str, text):
    codes = extract_candidate_codes(text, min_len=4)
    for code in codes:
        try:
            _code_queue.put_nowait(("isolated", (str(chat_str), code)))
        except asyncio.QueueFull:
            print("[queue] full, dropping code", flush=True)


# ---------------------------------------------------------------------------
# HTTP webhook server (bridge receiver)
# ---------------------------------------------------------------------------
class WebhookHandler(BaseHTTPRequestHandler):
    server_version = "ChowdeckBridge/2.0"
    protocol_version = "HTTP/1.1"

    def _authorized(self):
        if not WEBHOOK_SECRET:
            return bool(ALLOW_INSECURE_WEBHOOK)
        provided = (self.headers.get("X-Webhook-Secret") or "").strip()
        if not provided:
            auth = self.headers.get("Authorization", "")
            if auth.lower().startswith("bearer "):
                provided = auth[7:].strip()
        return bool(provided) and hmac.compare_digest(provided, WEBHOOK_SECRET)

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/health"):
            # Liveness only - no sensitive state, safe for platform healthchecks.
            self._send_json(200, {"status": "online"})
            return
        if path == "/status":
            if not self._authorized():
                self._send_json(401, {"status": "unauthorized"})
                return
            self._send_json(200, {"status": "online", "active": get_bot_active()})
            return
        self._send_json(404, {"status": "not_found"})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path not in ("/", "/tweet", "/webhook"):
            self._send_json(404, {"status": "not_found"})
            return
        if not self._authorized():
            self._send_json(401, {"status": "unauthorized"})
            return
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
            raw = self.rfile.read(length).decode("utf-8", "replace") if length else ""
            data = json.loads(raw) if raw else {}
            if not isinstance(data, dict):
                raise ValueError("body must be a JSON object")
        except Exception:
            self._send_json(400, {"status": "bad_request"})
            return

        text = str(data.get("text") or data.get("code") or "").strip()
        if not text:
            self._send_json(400, {"status": "no_text"})
            return
        if loop_ref is not None:
            asyncio.run_coroutine_threadsafe(
                process_text_global(text, require_caps=True), loop_ref
            )
        self._send_json(200, {"status": "ok"})

    def log_message(self, fmt, *args):
        return


def run_http_server():
    if not WEBHOOK_SECRET and not ALLOW_INSECURE_WEBHOOK:
        print(
            "[http] WEBHOOK_SECRET is NOT set - /status and /tweet (POST) will reject every "
            "request until it is configured. Set the same WEBHOOK_SECRET on the bot and the "
            "bridge service. (ALLOW_INSECURE_WEBHOOK=true disables this check for local tests.)",
            flush=True,
        )
    try:
        server = ThreadingHTTPServer(("0.0.0.0", PORT), WebhookHandler)
    except OSError as e:
        print(f"[http] could not bind port {PORT}: {e}", flush=True)
        return
    server.daemon_threads = True
    print(f"[http] listening on 0.0.0.0:{PORT}", flush=True)
    try:
        server.serve_forever()
    except Exception as e:  # noqa: BLE001
        print(f"[http] server stopped: {e}", flush=True)


# ---------------------------------------------------------------------------
# Telegram menu / command handling
# ---------------------------------------------------------------------------
def menu_text(chat_str):
    chat_str = str(chat_str)
    max_slots = get_max_slots(chat_str)
    tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
    enabled = sum(1 for i in range(1, max_slots + 1) if is_slot_active_state(chat_str, str(i)))
    return (
        f"🤖 *Chowdeck Multi-Account Sniper (1-{max_slots})*\n\n"
        f"• Active Slot: `Slot {get_active_slot(chat_str)}`\n"
        f"• Configured Slots: `{len(tokens_db)} / {max_slots}`\n"
        f"• Enabled Slots: `{enabled}`\n"
        f"• Mode: `PROMPT` — you supply the security answer on demand\n\n"
        "Pick an option below."
    )


GUIDE_TEXT = (
    "📖 *How this bot works*\n\n"
    "1️⃣ `/settoken <token or cookie string>` — link a Chowdeck account to the active slot.\n"
    "2️⃣ `👥 My Accounts` — toggle slots ✅/❌ and review linked phones.\n"
    "3️⃣ When a promo code drops, the bot tries it on every enabled slot.\n"
    "4️⃣ If a code needs a security answer, the question is relayed to you — "
    "*reply with the answer and it is submitted instantly*. No stored answers are used.\n\n"
    "*Useful commands*\n"
    "• `/code <CODE>` — force a code through now\n"
    "• `/ans <answer>` — force an answer for the newest pending question\n"
    "• `/ans <CODE> <answer>` — answer a specific code\n"
    "• `/pending` — show open questions\n"
    "• `/cancel` — drop open questions\n"
    "• `/announce on|off` — toggle channel code broadcasts (admin)\n"
    "• `/portfolio` — your claim history\n"
    "• `/setcart <cart_id> [vendor_id]` — auto-apply won codes to a cart"
)


async def refresh_all_phones(chat_str):
    chat_str = str(chat_str)
    tokens_db = load_json(TOKENS_FILE)
    user_slots = tokens_db.get(chat_str, {})
    updated = 0
    for slot_id, s_data in list(user_slots.items()):
        token = (s_data or {}).get("token")
        if not token:
            continue
        phone = await fetch_account_phone(token, (s_data or {}).get("cookies", {}))
        if phone:
            s_data["phone"] = str(phone)[:15]
            updated += 1
    if updated:
        save_json(TOKENS_FILE, tokens_db)
    return updated


async def handle_command(chat_id, chat_str, text, is_admin):
    parts = text.split(" ", 1)
    cmd = parts[0].split("@", 1)[0].lower()
    arg = parts[1].strip() if len(parts) > 1 else ""

    if cmd in ("/start", "/help"):
        # Menu only. The guide is on-demand: it is sent solely when the user
        # taps the "📖 Guide / Help" button (btn_guide) or types /guide.
        await send_telegram(chat_id, menu_text(chat_str),
                            reply_markup=build_control_keyboard(chat_str, is_admin))
    elif cmd == "/guide":
        await send_telegram(chat_id, GUIDE_TEXT)
    elif cmd == "/admin":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        u, s, c = get_admin_stats()
        await send_telegram(chat_id,
            f"🛡️ *Admin panel*\n\n"
            f"• Bot: {'🟢 active' if get_bot_active() else '⏸️ paused'}\n"
            f"• Channel broadcast: {'📣 ON' if get_channel_announce() else '🔇 OFF'}\n"
            f"• Users: `{u}`\n• Configured slots: `{s}`\n• Carts linked: `{c}`")
    elif cmd == "/geninvite":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        code_val = "INVITE-" + "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=6))
        inv = load_json(INVITE_CODES_FILE)
        inv[code_val] = True
        save_json(INVITE_CODES_FILE, inv)
        await send_telegram(chat_id, f"✅ New invite code:\n`{code_val}`\n\nShare it privately.")
    elif cmd == "/listwhitelist":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        wl = load_json(WHITELIST_FILE)
        snippet = "\n".join(f"• `{uid}`" for uid in wl) if wl else "Nobody yet."
        await send_telegram(chat_id, f"📋 *Whitelist:*\n{snippet}")
    elif cmd == "/revokeinvite":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/revokeinvite <code_or_chat_id>`")
            return
        inv = load_json(INVITE_CODES_FILE)
        if arg in inv:
            del inv[arg]
            save_json(INVITE_CODES_FILE, inv)
            await send_telegram(chat_id, f"✅ Invite `{arg}` revoked.")
        else:
            wl = load_json(WHITELIST_FILE)
            if arg in wl:
                remove_from_whitelist(arg)
                await send_telegram(chat_id, f"✅ `{arg}` removed from whitelist.")
            else:
                await send_telegram(chat_id, "⚠️ Not found in invites or whitelist.")
    elif cmd == "/linkaccount":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/linkaccount <chat_id>`")
            return
        add_linked_account(chat_str, arg)
        await send_telegram(chat_id, f"✅ Linked `{arg}`. Codes and answers mirror there instantly.")
    elif cmd == "/unlinkaccount":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/unlinkaccount <chat_id>`")
            return
        remove_linked_account(chat_str, arg)
        await send_telegram(chat_id, f"✅ Unlinked `{arg}`.")
    elif cmd == "/listlinked":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        linked = get_linked_accounts(chat_str)
        snippet = "\n".join(f"• `{a}`" for a in linked) if linked else "None linked."
        await send_telegram(chat_id, f"🔗 *Linked accounts:*\n{snippet}")
    elif cmd == "/settoken":
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/settoken <token or cookie string>`")
            return
        slot = get_active_slot(chat_str)
        parse_and_store_credentials(arg, chat_str, slot)
        slot_data = load_json(TOKENS_FILE).get(chat_str, {}).get(slot, {})
        await send_telegram(chat_id,
            f"✅ Credentials saved to *Slot {slot}*.\n"
            f"📞 Linked phone: `{slot_data.get('phone') or 'unknown'}`")
    elif cmd == "/setphone":
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/setphone <phone_or_label>`")
            return
        slot = get_active_slot(chat_str)
        tokens_db = load_json(TOKENS_FILE)
        tokens_db.setdefault(chat_str, {}).setdefault(slot, {})["phone"] = arg[:15]
        save_json(TOKENS_FILE, tokens_db)
        await send_telegram(chat_id, f"✅ Slot {slot} label set to `{arg[:15]}`.")
    elif cmd == "/refreshphones":
        await send_telegram(chat_id, "🔄 Reading profiles…")
        updated = await refresh_all_phones(chat_str)
        await send_telegram(chat_id, f"✅ Refreshed `{updated}` slot phone(s).")
    elif cmd == "/setcart":
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/setcart <cart_id> [vendor_id]`")
            return
        sub = arg.split()
        carts_db = load_json(CARTS_FILE)
        carts_db.setdefault(chat_str, {})[get_active_slot(chat_str)] = {
            "cart_id": sub[0], "vendor_id": sub[1] if len(sub) > 1 else None,
        }
        save_json(CARTS_FILE, carts_db)
        await send_telegram(chat_id, f"✅ Cart `{sub[0]}` linked to Slot {get_active_slot(chat_str)}.")
    elif cmd == "/portfolio" or cmd == "/vouchers":
        data = load_json(PORTFOLIO_FILE).get(chat_str, {"items": [], "total_value": 0})
        items = data.get("items", [])
        snippet = "\n".join(
            f"• `{i.get('code', '?')}` (S{i.get('slot', '?')}) — ₦{i.get('amount', 2000):,}"
            for i in items[-15:]
        ) if items else "No claims recorded yet."
        await send_telegram(chat_id,
            f"💰 *Your claim portfolio*\n\n"
            f"• Slot wins: `{len(items)}`\n• Value saved: `₦{data.get('total_value', 0):,}`\n\n"
            f"📜 *Recent:*\n{snippet}")
    elif cmd == "/code":
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/code <CODE>`")
            return
        forced = robust_clean_code(arg) or arg.strip().upper()
        await send_telegram(chat_id, f"🔍 Forcing code `{forced}` now…")
        await claim_code_for_chat(chat_str, forced)
    elif cmd == "/ans" or cmd == "/answer":
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/ans <answer>` or `/ans <CODE> <answer>`")
            return
        sub = arg.split(None, 1)
        target_code = None
        answer = arg
        if len(sub) == 2 and robust_clean_code(sub[0]) and sub[0].upper() == sub[0]:
            target_code = robust_clean_code(sub[0])
            answer = sub[1].strip()
        entry = pop_pending(chat_str, code=target_code)
        if not entry:
            await send_telegram(chat_id, "⚠️ No open security question to answer.")
            return
        await handle_answer(chat_str, answer, entry)
    elif cmd == "/pending":
        items = list_pending(chat_str)
        if not items:
            await send_telegram(chat_id, "📭 No open questions.")
            return
        snippet = "\n".join(f"• `{e.get('code')}` — \"{e.get('question', '')}\"" for e in items)
        await send_telegram(chat_id, f"🔒 *Open questions:*\n{snippet}\n\nAnswer with `/ans <answer>`.")
    elif cmd == "/cancel":
        clear_pending(chat_str, robust_clean_code(arg) if arg else None)
        await send_telegram(chat_id, "🗑️ Open question(s) cleared.")
    elif cmd == "/setburnertoken":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/setburnertoken <token or cookies>`")
            return
        token = arg
        cookies = {}
        if ";" in arg and "=" in arg:
            for p in arg.split(";"):
                if "=" in p:
                    k, v = p.split("=", 1)
                    cookies[k.strip()] = v.strip()
                    if k.strip() == "user":
                        try:
                            token = json.loads(urllib.parse.unquote(v.strip())).get("token", token)
                        except Exception:
                            pass
        if token.lower().startswith("bearer "):
            token = token[7:].strip()
        save_burner_credentials(chat_str, {"token": token, "cookies": cookies})
        await send_telegram(chat_id, "✅ Burner token linked.")
    elif cmd == "/setburnercart":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/setburnercart <cart_id> [vendor_id]`")
            return
        sub = arg.split()
        save_burner_cart(chat_str, {"cart_id": sub[0], "vendor_id": sub[1] if len(sub) > 1 else None})
        await send_telegram(chat_id, f"✅ Burner cart `{sub[0]}` linked.")
    elif cmd == "/test":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/test <CODE>`")
            return
        test_code = robust_clean_code(arg)
        await send_telegram(chat_id, f"🔬 Testing `{test_code}` on the burner account…")
        await send_telegram(chat_id, await execute_burner_test(chat_chat_id_guard(chat_id), test_code))
    elif cmd == "/pause":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        set_bot_active(False)
        await send_telegram(chat_id, "⏸️ Global claiming paused.")
    elif cmd == "/resume":
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        set_bot_active(True)
        await send_telegram(chat_id, "▶️ Global claiming resumed.")
    elif cmd in ("/announce", "/channel"):
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
            return
        val = arg.strip().lower()
        if val in ("on", "off"):
            set_channel_announce(val == "on")
        elif val:
            await send_telegram(chat_id, "⚠️ Usage: `/announce on` or `/announce off`")
            return
        state = "ON" if get_channel_announce() else "OFF"
        await send_telegram(
            chat_id,
            f"📣 *Code-detection channel notifications:* `{state}`\n"
            f"Usage: `/announce on` or `/announce off`",
        )
    else:
        await send_telegram(chat_id, "ℹ️ Unknown command. Send /help for the guide.")


def chat_chat_id_guard(chat_id):
    return chat_id


async def handle_callback(update, tg_client):
    cq = update["callback_query"]
    cq_id = cq["id"]
    chat_id = cq["message"]["chat"]["id"]
    msg_id = cq["message"]["message_id"]
    action = cq.get("data", "")
    chat_str = str(chat_id)
    is_admin = is_admin_chat(chat_str)
    max_slots = get_max_slots(chat_str)

    if not is_user_whitelisted(chat_str):
        await answer_callback_query(cq_id, "Access denied: invite code required.")
        return

    if action.startswith("slot_"):
        slot_num = action.split("_", 1)[1]
        set_active_slot(chat_str, slot_num)
        await answer_callback_query(cq_id, f"Slot {slot_num} active")
        await edit_message_text(chat_id, msg_id, menu_text(chat_str),
                                build_control_keyboard(chat_str, is_admin))
    elif action.startswith("toggle_acc_"):
        s_num = action.split("_")[2]
        toggle_slot_state(chat_str, s_num)
        await answer_callback_query(cq_id, f"Slot {s_num} toggled")
        await show_accounts(chat_id, msg_id, chat_str, max_slots)
    elif action == "btn_my_accounts":
        await answer_callback_query(cq_id, "Accounts")
        await show_accounts(chat_id, msg_id, chat_str, max_slots)
    elif action == "btn_refresh_phones":
        await answer_callback_query(cq_id, "Refreshing…")
        await refresh_all_phones(chat_str)
        await show_accounts(chat_id, msg_id, chat_str, max_slots, refreshed=True)
    elif action == "btn_portfolio":
        await answer_callback_query(cq_id, "Portfolio")
        data = load_json(PORTFOLIO_FILE).get(chat_str, {"items": [], "total_value": 0})
        items = data.get("items", [])
        snippet = "\n".join(
            f"• `{i.get('code', '?')}` (S{i.get('slot', '?')}) — ₦{i.get('amount', 2000):,}"
            for i in items[-10:]
        ) if items else "No claims recorded yet."
        await send_telegram(chat_id,
            f"💰 *Your claim portfolio*\n\n"
            f"• Slot wins: `{len(items)}`\n• Value saved: `₦{data.get('total_value', 0):,}`\n\n"
            f"📜 *Recent:*\n{snippet}")
    elif action == "btn_status":
        await answer_callback_query(cq_id, "Refreshed")
        await send_telegram(chat_id, menu_text(chat_str))
    elif action == "btn_guide":
        await answer_callback_query(cq_id, "Guide")
        await send_telegram(chat_id, GUIDE_TEXT)
    elif action == "btn_main_menu":
        await answer_callback_query(cq_id, "Main menu")
        await edit_message_text(chat_id, msg_id, menu_text(chat_str),
                                build_control_keyboard(chat_str, is_admin))
    elif action == "btn_admin_stats":
        await answer_callback_query(cq_id, "Stats")
        if not is_admin:
            await send_telegram(chat_id, "⛔ Admin only.")
        else:
            u, s, c = get_admin_stats()
            await send_telegram(chat_id,
                f"🛡️ *Admin panel*\n\n"
                f"• Bot: {'🟢 active' if get_bot_active() else '⏸️ paused'}\n"
                f"• Channel broadcast: {'📣 ON' if get_channel_announce() else '🔇 OFF'}\n"
                f"• Users: `{u}`\n• Configured slots: `{s}`\n• Carts linked: `{c}`")
    elif action == "btn_pause":
        await answer_callback_query(cq_id, "Paused")
        if is_admin:
            set_bot_active(False)
            await send_telegram(chat_id, "⏸️ Global claiming paused.")
    elif action == "btn_resume":
        await answer_callback_query(cq_id, "Resumed")
        if is_admin:
            set_bot_active(True)
            await send_telegram(chat_id, "▶️ Global claiming resumed.")
    elif action == "btn_toggle_channel":
        if not is_admin:
            await answer_callback_query(cq_id, "Admin only")
            return
        new_val = not get_channel_announce()
        set_channel_announce(new_val)
        await answer_callback_query(cq_id, f"Channel {'ON' if new_val else 'OFF'}")
        await send_telegram(
            chat_id,
            "📣 *Code-detection channel notifications are now "
            f"{'ON' if new_val else 'OFF'}.*\n\n"
            + ("Detected codes will be broadcast to the channel so users can copy and claim."
               if new_val else
               "Detected codes will be served to your admin slots only — no public broadcast."),
        )
        await edit_message_text(chat_id, msg_id, menu_text(chat_str),
                                build_control_keyboard(chat_str, is_admin))
    elif action == "btn_burner_menu":
        await answer_callback_query(cq_id, "Burner panel")
        if is_admin:
            b_status = "🟢 linked" if get_burner_credentials(chat_str).get("token") else "🔴 not configured"
            await edit_message_text(chat_id, msg_id,
                f"🧪 *Burner account panel*\n\n• Status: {b_status}\n\n"
                "• `/setburnertoken <token>`\n• `/setburnercart <cart_id> [vendor_id]`\n• `/test <CODE>`",
                build_admin_burner_keyboard())
    elif action == "btn_linked_menu":
        await answer_callback_query(cq_id, "Linked accounts")
        if is_admin:
            linked = get_linked_accounts(chat_str)
            snippet = "\n".join(f"• `{a}`" for a in linked) if linked else "None linked yet."
            await edit_message_text(chat_id, msg_id,
                f"🔗 *Linked secondary accounts*\n\n{snippet}\n\n"
                "• `/linkaccount <chat_id>`\n• `/unlinkaccount <chat_id>`\n• `/listlinked`",
                build_admin_linked_keyboard())
    elif action == "btn_invites_menu":
        await answer_callback_query(cq_id, "Invites")
        if is_admin:
            inv = load_json(INVITE_CODES_FILE)
            snippet = "\n".join(f"• `{c}`" for c in inv) if inv else "No active invite codes."
            await edit_message_text(chat_id, msg_id,
                f"🔑 *Invite codes*\n\n{snippet}\n\n"
                "• `/geninvite`\n• `/revokeinvite <code>`\n• `/listwhitelist`",
                build_admin_invites_keyboard())
    elif action == "btn_gen_invite":
        await answer_callback_query(cq_id, "Generated")
        if is_admin:
            code_val = "INVITE-" + "".join(random.choices("ABCDEFGHJKLMNPQRSTUVWXYZ23456789", k=6))
            inv = load_json(INVITE_CODES_FILE)
            inv[code_val] = True
            save_json(INVITE_CODES_FILE, inv)
            await send_telegram(chat_id, f"✅ New invite code:\n`{code_val}`")
    elif action == "btn_list_whitelist":
        await answer_callback_query(cq_id, "Whitelist")
        if is_admin:
            wl = load_json(WHITELIST_FILE)
            snippet = "\n".join(f"• `{uid}`" for uid in wl) if wl else "Nobody yet."
            await send_telegram(chat_id, f"📋 *Whitelist:*\n{snippet}")
    elif action == "burner_info":
        await answer_callback_query(cq_id, "Use /test <CODE>")
    else:
        await answer_callback_query(cq_id, "Unknown action")


async def show_accounts(chat_id, msg_id, chat_str, max_slots, refreshed=False):
    enabled = sum(1 for i in range(1, max_slots + 1) if is_slot_active_state(chat_str, str(i)))
    extra = "\nPhones refreshed." if refreshed else "\n✅ = enabled & claiming | ❌ = disabled"
    await edit_message_text(
        chat_id, msg_id,
        f"💼 *My Chowdeck accounts (1-{max_slots})*\n"
        f"Enabled: `{enabled}`{extra}",
        build_accounts_keyboard(chat_str),
    )


# ---------------------------------------------------------------------------
# Telegram long polling
# ---------------------------------------------------------------------------
async def handle_text_update(update):
    msg = update.get("message", {})
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id or not text:
        return
    chat_str = str(chat_id)
    is_admin = is_admin_chat(chat_str)

    if not is_user_whitelisted(chat_str):
        inv = load_json(INVITE_CODES_FILE)
        if text in inv:
            del inv[text]
            save_json(INVITE_CODES_FILE, inv)
            add_to_whitelist(chat_str)
            await send_telegram(chat_id, "🎉 *Invite accepted!*\n\nWelcome. Send /start to begin.")
        else:
            await send_telegram(
                chat_id,
                "🔒 *Access denied — private bot.*\n\n"
                "Send a valid *invite code* in this chat to unlock access.",
            )
        return

    if text.startswith("/"):
        await handle_command(chat_id, chat_str, text, is_admin)
        return

    pending = list_pending(chat_str)
    reply_to = (msg.get("reply_to_message") or {}).get("message_id")

    if pending:
        if reply_to is not None and any(e.get("msg_id") == reply_to for e in pending):
            entry = pop_pending(chat_str, reply_to=reply_to)
            await handle_answer(chat_str, text, entry)
            return
        if strong_code_signal(text):
            await process_text_isolated(chat_str, text)
            await send_telegram(
                chat_id,
                f"🔍 That looks like a code — claiming `{robust_clean_code(text)}` now.\n"
                f"If you meant it as the security answer, send `/ans {text}` "
                f"or reply to the question message.",
            )
            return
        entry = pop_pending(chat_str)
        await handle_answer(chat_str, text, entry)
        return

    if looks_like_code(text):
        await process_text_isolated(chat_str, text)
        return

    await send_telegram(
        chat_id,
        "ℹ️ That does not look like a promo code. Send the code as text "
        "(e.g. `CHOW5K`), or paste a screenshot to OCR it.",
    )


async def handle_photo_update(update, tg_client):
    msg = update["message"]
    chat_id = msg["chat"]["id"]
    chat_str = str(chat_id)
    if not is_user_whitelisted(chat_str):
        await send_telegram(chat_id, "🔒 Access denied — private bot.")
        return
    file_id = msg["photo"][-1]["file_id"]
    base = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
    try:
        file_res = await tg_client.get(f"{base}/getFile", params={"file_id": file_id})
        if file_res.status_code != 200:
            return
        file_path = file_res.json().get("result", {}).get("file_path")
        if not file_path:
            return
        img_res = await tg_client.get(f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}")
        if img_res.status_code != 200:
            await send_telegram(chat_id, "❌ Could not download that image.")
            return
        await send_telegram(chat_id, "🔍 Screenshot received — reading text…")
        extracted = await perform_ocr_on_image(img_res.content)
        if extracted:
            await process_text_isolated(chat_str, extracted)
        else:
            await send_telegram(chat_id, "❌ No text found in that screenshot. Please send it as text.")
    except Exception as e:  # noqa: BLE001
        print(f"[photo] error: {e}", flush=True)


async def telegram_poller():
    global last_update_id
    if not TELEGRAM_BOT_TOKEN:
        print("[tg] TELEGRAM_BOT_TOKEN missing - Telegram polling disabled.", flush=True)
        while True:
            await asyncio.sleep(3600)
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    async with httpx.AsyncClient(timeout=httpx.Timeout(35.0, connect=10.0), proxy=PROXY_URL) as tg_client:
        while True:
            try:
                res = await tg_client.get(
                    url, params={"offset": last_update_id + 1, "timeout": 25,
                                 "allowed_updates": json.dumps(["message", "callback_query"])}
                )
                if res.status_code == 200:
                    for update in res.json().get("result", []):
                        last_update_id = update.get("update_id", last_update_id)
                        try:
                            if "callback_query" in update:
                                await handle_callback(update, tg_client)
                            elif "message" in update and "photo" in update["message"]:
                                await handle_photo_update(update, tg_client)
                            elif "message" in update:
                                await handle_text_update(update)
                        except Exception as e:  # noqa: BLE001
                            print(f"[tg] update error: {e}", flush=True)
                elif res.status_code == 409:
                    print("[tg] 409 conflict - another poller/webhook is active.", flush=True)
                    await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                print(f"[tg] poller error: {e}", flush=True)
                await asyncio.sleep(2)
            await asyncio.sleep(0.02)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
last_update_id = 0
WORKER_COUNT = max(1, int(os.getenv("WORKER_COUNT", "3")))


async def main():
    global loop_ref
    loop_ref = asyncio.get_running_loop()
    start_store_writer()
    threading.Thread(target=run_http_server, daemon=True).start()

    if not TELEGRAM_BOT_TOKEN:
        print("[boot] WARNING: TELEGRAM_BOT_TOKEN is not set.", flush=True)
    if not ADMIN_CHAT_ID:
        print("[boot] WARNING: ADMIN_CHAT_ID is not set - no user will have admin rights.", flush=True)

    workers = [asyncio.create_task(worker()) for _ in range(WORKER_COUNT)]
    try:
        await telegram_poller()
    finally:
        for w in workers:
            w.cancel()
        flush_store()
        try:
            await _CD_CLIENT.aclose()
            await _TG_CLIENT.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped.", flush=True)
