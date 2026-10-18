#!/usr/bin/env python3
""" Chowdeck Multi-Account Promo Sniper - Telegram bot + HTTP bridge receiver. Rewrite notes ------------- * PROMPT MODE ONLY. The legacy "auto" mode and every stored / pre-configured security answer are gone. When a security-question-enabled code is detected the bot relays the question and captures the user's pasted answer instantly. * Code-priority parsing: a pasted promo code is never swallowed as a security answer. Explicit /ans and /code overrides exist for edge cases. * Fixed admin check: admins are only admins when ADMIN_CHAT_ID is set AND matches (the old logic granted admin to everyone when the var was missing). * Authenticated inbound webhook: POST /tweet and GET /status require WEBHOOK_SECRET (fail closed unless ALLOW_INSECURE_WEBHOOK=true). * Low latency: shared keep-alive HTTP clients, in-memory JSON cache with debounced write-behind to disk, per-slot result streaming as each slot finishes (no waiting on the slowest slot). * Burst / hedged claiming (CLAIM_MAX_ATTEMPTS, CLAIM_WINDOW_SECONDS, HEDGE_REQUESTS) to punch through "voucher is in high demand" responses. * Anti self-throttle pacing, per chat: every outbound claim request passes through the *claiming chat's own* token bucket (CLAIM_RATE / CLAIM_RATE_BURST) and each slot's first request is staggered (SLOT_STAGGER_SECONDS), so a 19-slot fanout no longer fires ~340 requests in ~4s and makes Chowdeck 429/500 the bot itself. A 429 drains only that chat's bucket via Retry-After, so one user's cooldown never stalls another. Telegram sends are paced per chat too (TG_RATE / TG_RATE_BURST). An optional CLAIM_GLOBAL_RATE adds a cross-user ceiling for the shared egress IP. * Claim UX: instant "🔍 Processing ..." ack, one "🎯 ... (Slot N)" message per slot (so an expired/inactive/congested slot never hides another), and the security question is surfaced immediately with a paste-in-chat answer. * Hardened error handling: bounded concurrency, per-request timeouts, no unbounded retry loops, Markdown-safe Telegram sends. * X detection is admin-only: a code detected on the bridge is auto-served to the admin chat plus the admin's linked accounts (all enabled slots fire in parallel, no per-user throttle) and any security question is relayed immediately. Regular users copy the code from the channel broadcast and claim it themselves; the channel broadcast can be toggled from the admin panel. """

import asyncio
import base64
import contextvars
import datetime
import email.utils
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

CLAIM_MAX_ATTEMPTS = max(1, int(os.getenv("CLAIM_MAX_ATTEMPTS", "3")))
CLAIM_WINDOW_SECONDS = float(os.getenv("CLAIM_WINDOW_SECONDS", "4.0"))
HEDGE_REQUESTS = max(1, int(os.getenv("HEDGE_REQUESTS", "1")))
REQUEST_CONCURRENCY = max(10, int(os.getenv("REQUEST_CONCURRENCY", "150")))

# --- Outbound pacing -------------------------------------------------------
# A 19-slot fanout used to fire ~340 requests inside ~4s (19 slots x 3 hedges x
# 6 attempts), which made Chowdeck throttle *us*: self-inflicted 429/500 storms.
# Pacing is *per chat*: each user gets their own token bucket, so one user's
# claim burst (or 429 cooldown) can never stall another user. The fleet keeps
# its concurrency but not its burst stampede.
CLAIM_RATE = max(1.0, float(os.getenv("CLAIM_RATE", "14")))
CLAIM_RATE_BURST = max(1, int(os.getenv("CLAIM_RATE_BURST", "24")))
# Slot N waits N * SLOT_STAGGER_SECONDS before its first request, so slots do
# not all land on the same millisecond. Kept tiny: every extra millisecond here
# is a millisecond the last slot is not yet racing.
SLOT_STAGGER_SECONDS = float(os.getenv("SLOT_STAGGER_SECONDS", "0.02"))
# Telegram send pacing, also per chat (avoids flood-control 429s from per-slot
# report bursts without one user's reports delaying another user's).
TG_RATE = max(1.0, float(os.getenv("TG_RATE", "18")))
TG_RATE_BURST = max(1, int(os.getenv("TG_RATE_BURST", "6")))
# Optional cross-user ceiling (requests/second) for the shared Railway egress
# IP. 0 = disabled: each chat is paced only by its own CLAIM_RATE bucket. Set
# e.g. CLAIM_GLOBAL_RATE=40 to cap the *sum* of all users when the upstream
# throttle is known to be IP-scoped.
CLAIM_GLOBAL_RATE = max(0.0, float(os.getenv("CLAIM_GLOBAL_RATE", "0")))
CLAIM_GLOBAL_RATE_BURST = max(1, int(os.getenv("CLAIM_GLOBAL_RATE_BURST", "24")))
# Debug aid: dump the real Chowdeck response body (never masked) to stdout.
LOG_RAW_CLAIM = os.getenv("LOG_RAW_CLAIM", "").strip().lower() in (
    "1", "true", "yes", "on",
)
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
LOGIN_FLOW_FILE = _data_file("login_flows.json")

# --- Feature 1: phone -> OTP login (a session token the BOT owns) ----------
# The bot signs in with its own POST /auth/login + /auth/login/validate-otp
# call, exactly like a second device signing in. The token it receives is a
# normal bearer JWT that is independent of whatever token the user's browser
# already holds, so linking/refreshing the bot's token never disturbs a web
# session, and a web login never invalidates the bot's token.
CHOW_API_BASE = os.getenv("CHOW_API_BASE", "https://api.chowdeck.com").strip().rstrip("/")
CHOW_AUTH_ROLE = os.getenv("CHOW_AUTH_ROLE", "customer").strip() or "customer"
CHOW_COUNTRY_CODE = os.getenv("CHOW_COUNTRY_CODE", "NG").strip().upper() or "NG"

# --- international phone numbers -------------------------------------------
# The API wants an ISO-3166 alpha-2 `country_code`, not a dial code. Verified
# live against POST /auth/phone/validate:
# {"phone":"9138972820","country_code":"NG"} -> is_valid true
# {"phone":"2125551234","country_code":"US"} -> is_valid true
# {"phone":"2125551234","country_code":"1"} -> is_valid false (dial code rejected)
# {"phone":"4387985720","country_code":"US"} -> is_valid true (NANP, 10-digit national)
# {"phone":"14387985720","country_code":"US"} -> is_valid true (11-digit with trunk 1)
# So a "+"/"00" prefix (or an explicit US/CA/NG hint) is resolved to its ISO
# code here, which is what makes US/CA accounts linkable like Nigerian ones.
_DIAL_TO_ISO = {
    "1": "US", "7": "RU", "20": "EG", "27": "ZA", "30": "GR", "31": "NL",
    "32": "BE", "33": "FR", "34": "ES", "39": "IT", "40": "RO", "41": "CH",
    "43": "AT", "44": "GB", "45": "DK", "46": "SE", "47": "NO", "48": "PL",
    "49": "DE", "51": "PE", "52": "MX", "54": "AR", "55": "BR", "56": "CL",
    "57": "CO", "60": "MY", "61": "AU", "62": "ID", "63": "PH", "64": "NZ",
    "65": "SG", "66": "TH", "81": "JP", "82": "KR", "84": "VN", "86": "CN",
    "90": "TR", "91": "IN", "92": "PK", "94": "LK", "98": "IR",
    "212": "MA", "213": "DZ", "216": "TN", "218": "LY", "220": "GM",
    "221": "SN", "225": "CI", "226": "BF", "227": "NE", "228": "TG",
    "229": "BJ", "230": "MU", "233": "GH", "234": "NG", "235": "TD",
    "237": "CM", "240": "GQ", "241": "GA", "242": "CG", "243": "CD",
    "244": "AO", "249": "SD", "250": "RW", "251": "ET", "254": "KE",
    "255": "TZ", "256": "UG", "257": "BI", "258": "MZ", "260": "ZM",
    "263": "ZW", "264": "NA", "265": "MW", "266": "LS", "267": "BW",
    "268": "SZ", "351": "PT", "353": "IE", "358": "FI", "359": "BG",
    "370": "LT", "371": "LV", "372": "EE", "380": "UA", "381": "RS",
    "385": "HR", "420": "CZ", "421": "SK", "880": "BD", "961": "LB",
    "962": "JO", "964": "IQ", "965": "KW", "966": "SA", "971": "AE",
    "972": "IL", "973": "BH", "974": "QA", "977": "NP",
}
# Dial code -> ISO, longest prefix first ("1" vs "12", "234" vs "23").
_DIAL_PREFIXES = sorted(_DIAL_TO_ISO, key=len, reverse=True)
# Countries whose national format drops a single leading trunk "0".
_TRUNK_ZERO_CC = {
    "NG", "GH", "KE", "GB", "IE", "ZA", "AU", "IN", "DE", "NL", "BE",
    "IT", "ES", "FR", "ID", "MY", "PK", "BD", "EG", "MA", "TR", "RU",
    "UA", "RO", "CH", "AT", "DK", "SE", "NO", "PL", "PT", "FI", "CZ",
    "SK", "HR", "RS", "BG", "LT", "LV", "EE", "VN", "TH", "GR", "IL",
}
# NANP: the same +1 country code is shared by the US and Canada, and the API
# validator accepts either (verified: 4387985720 is valid as both US and CA),
# so the alternate is tried when the first verdict is invalid.
_NANP_CC = ("US", "CA")
# ISO -> dial code, for stripping a redundant dial prefix off a national number.
_DIAL_BY_ISO = {}
for _dial, _iso in _DIAL_TO_ISO.items():
    _DIAL_BY_ISO.setdefault(_iso, _dial)
_DIAL_BY_ISO["CA"] = "1"

# Health probe: paths tried in order until one answers definitively. The first
# is the storefront's own endpoint; the rest are harmless fallbacks so a
# rename on Chowdeck's side degrades to a clear message instead of a mystery.
HEALTH_PROBE_PATHS = [
    p.strip() for p in os.getenv(
        "HEALTH_PROBE_PATH", "/customer/profile,/customer,/customer/me,/profile"
    ).split(",") if p.strip()
]
HEALTH_PROBE_CONCURRENCY = max(1, min(8, int(os.getenv("HEALTH_PROBE_CONCURRENCY", "3"))))
HEALTH_PROBE_STAGGER = max(0.0, float(os.getenv("HEALTH_PROBE_STAGGER", "0.2")))
LOGIN_ENABLED = os.getenv("LOGIN_ENABLED", "true").strip().lower() in (
    "1", "true", "yes", "on",
)
OTP_FLOW_TTL = max(30.0, float(os.getenv("OTP_FLOW_TTL", "600")))
OTP_RESEND_COOLDOWN = max(5.0, float(os.getenv("OTP_RESEND_COOLDOWN", "30")))
# off = never auto-refresh (identical to the old behaviour)
# safe = refresh only tokens this bot minted itself via /login (default)
# on = also refresh tokens the user pasted from their own browser session
CHOW_AUTO_REFRESH = os.getenv("CHOW_AUTO_REFRESH", "safe").strip().lower() or "safe"
TOKEN_REFRESH_SKEW = float(os.getenv("TOKEN_REFRESH_SKEW", "120"))
AUTH_TIMEOUT = float(os.getenv("AUTH_TIMEOUT", "6.0"))
# Debug aid: dump the raw auth API response body to stdout (never masked).
LOG_RAW_AUTH = os.getenv("LOG_RAW_AUTH", "").strip().lower() in (
    "1", "true", "yes", "on",
)

# --- Feature 2: AI security-answer assist (real-time web search) -----------
# auto = enabled when any provider key is present, disabled otherwise.
AI_ASSIST_ENABLED = os.getenv("AI_ASSIST_ENABLED", "auto").strip().lower()
AI_PROVIDER = os.getenv("AI_PROVIDER", "").strip().lower()
AI_MODEL = os.getenv("AI_MODEL", "").strip()
AI_TIMEOUT = float(os.getenv("AI_TIMEOUT", "8.0"))
AI_MAX_ANSWERS = max(1, min(8, int(os.getenv("AI_MAX_ANSWERS", "4"))))
# Race every configured provider at once and post the first usable answer set:
# this is what keeps the suggestion inside the 2-3s window instead of waiting
# for one slow provider to time out before trying the next.
AI_RACE = os.getenv("AI_RACE", "true").strip().lower() not in ("0", "false", "no", "off")
# Two passes, because one budget cannot serve both goals:
# pass 1 (AI_DEADLINE) - fast: keeps a suggestion inside the 2-3s claim
# window, which is what AI_RACE is for
# pass 2 (AI_LATE_DEADLINE) - second chance: a web-search grounded model
# routinely needs longer than a few seconds, and a
# late suggestion is still useful because the
# question stays open until it is answered
AI_DEADLINE = float(os.getenv("AI_DEADLINE", "3.0"))
AI_LATE_DEADLINE = float(os.getenv("AI_LATE_DEADLINE", "12.0"))
# When both passes come back empty, say so - with the provider's own reason -
# instead of staying silent. Silence is what makes a working feature look broken.
AI_NOTIFY_FAILURE = os.getenv("AI_NOTIFY_FAILURE", "true").strip().lower() not in (
    "0", "false", "no", "off",
)
AI_SHOW_LATENCY = os.getenv("AI_SHOW_LATENCY", "true").strip().lower() not in (
    "0", "false", "no", "off",
)
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "").strip()
PERPLEXITY_MODEL = os.getenv("PERPLEXITY_MODEL", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "").strip()

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
    # Everyday greetings / courtesy words: harmless chatter that must never be
    # mistaken for a code now that a lone mixed-case token is accepted.
    "HELLO", "HI", "HEY", "PLEASE", "THANKS", "THANK", "SORRY", "WELCOME",
    "OKAY", "NICE", "GREAT", "COOL", "MORNING", "EVENING", "NIGHT",
    "HAHA", "LOL", "CONGRATS", "CONGRATULATIONS",
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
# Pending security-question prompt (only ONE live question per chat)
# ---------------------------------------------------------------------------
def add_or_update_pending(chat_str, entry):
    """Register the security question that is currently shown to the user. Only one question is live per chat. Registering a question for a new code drops any older prompt for a *different* code, and re-registering the same code replaces its entry. This guarantees a pasted answer is always bound to the question the user is actually looking at; without it an answer could be applied to a previously-pending code instead of the one just claimed. """
    store = load_json(PENDING_PROMPT_FILE)
    # Exactly one question is live per chat: registering a question (for any
    # code) replaces whatever was pending, so a pasted answer is always bound
    # to the question the user is actually looking at.
    store[str(chat_str)] = [entry]
    save_json(PENDING_PROMPT_FILE, store)


def list_pending(chat_str):
    return list(load_json(PENDING_PROMPT_FILE).get(str(chat_str), []))


def pop_pending(chat_str, code=None, reply_to=None):
    """Remove and return the prompt a user is answering. Resolution order: explicit code -> Telegram reply target -> newest prompt. The fallback must be the NEWEST question: that is the one currently on screen, so a plain pasted answer is bound to the code the user just claimed. """
    store = load_json(PENDING_PROMPT_FILE)
    bucket = list(store.get(str(chat_str), []))
    target = None
    if code:
        target = next((e for e in bucket if e.get("code") == code), None)
    elif reply_to is not None:
        target = next((e for e in bucket if e.get("msg_id") == reply_to), None)
    if target is None and bucket:
        target = bucket[-1]
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


class _TokenBucket:
    """Async token bucket that bounds outbound rate without serialising I/O. Callers ``await acquire()`` immediately before a request. The bucket refills at ``rate`` tokens/second up to ``burst`` tokens, so concurrency is preserved (many in-flight requests) while the *average* request rate stays under the ceiling that makes the upstream throttle us. """

    def __init__(self, rate, burst):
        self.rate = max(0.1, float(rate))
        self.capacity = max(1.0, float(burst))
        self._tokens = self.capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self.capacity, self._tokens + (now - self._updated) * self.rate
                )
                self._updated = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(wait)

    async def backoff(self, seconds):
        """Drain the bucket so the whole fleet honours a Retry-After window."""
        seconds = max(0.0, min(float(seconds or 0.0), 30.0))
        if seconds <= 0:
            return
        async with self._lock:
            self._tokens = -seconds * self.rate
            self._updated = time.monotonic()


_PACERS = {}
_PACERS_LOCK = threading.Lock()


def _pacer_for(scope, rate, burst):
    """Return a token bucket unique to ``scope`` (created on first use). Keying the bucket by scope -- ``claim:<chat>`` for Chowdeck requests and ``tg:<chat>`` for Telegram sends -- is what keeps users isolated: a chat that fires 19 slot requests, or trips a 429 cooldown, drains only its own bucket and never delays anyone else's traffic. """
    key = (str(scope), float(rate), int(burst))
    with _PACERS_LOCK:
        bucket = _PACERS.get(key)
        if bucket is None:
            bucket = _TokenBucket(rate, burst)
            _PACERS[key] = bucket
        return bucket


# Per-task claim scope, set by the slot worker to its chat id so ``_raw_post``
# (and the hedged helpers it spawns) know which bucket to draw from.
_CLAIM_SCOPE = contextvars.ContextVar("claim_scope", default="")

# Optional IP-level guard; disabled unless CLAIM_GLOBAL_RATE > 0.
_GLOBAL_CLAIM_PACER = (
    _TokenBucket(CLAIM_GLOBAL_RATE, CLAIM_GLOBAL_RATE_BURST)
    if CLAIM_GLOBAL_RATE > 0
    else None
)


def _claim_pacer(scope):
    return _pacer_for("claim:" + str(scope or "_default"), CLAIM_RATE, CLAIM_RATE_BURST)


async def _acquire_claim(scope):
    """Take one claim token, applying the optional global (IP-level) guard."""
    if _GLOBAL_CLAIM_PACER is not None:
        await _GLOBAL_CLAIM_PACER.acquire()
    await _claim_pacer(scope).acquire()


async def _claim_backoff(scope, wait):
    """Drain this chat's bucket (and the global one) for a Retry-After window."""
    await _claim_pacer(scope).backoff(wait)
    if _GLOBAL_CLAIM_PACER is not None:
        await _GLOBAL_CLAIM_PACER.backoff(wait)

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
# Feature 1 - Chowdeck auth client: phone -> OTP -> bot-owned session token
# ---------------------------------------------------------------------------
# Endpoints/payloads mirror the storefront bundle:
# POST /auth/phone/validate {phone, country_code} -> data.is_valid
# POST /auth/login {country_code, phone, role} -> sends the OTP
# POST /auth/login/validate-otp {otp, phone, role} -> data.token (+ id, exp, ...)
# POST /auth/resend-otp {phone, role, country_code, type} -> sends the OTP again
# POST /auth/refresh-token {token} -> data.token
# The validate-otp token is used exactly like a pasted token. It is minted by a
# *separate* sign-in from the one the user's browser performed, so it is an
# independent session: refreshing it never touches the web session, and a web
# login never invalidates it.
_bg_tasks = set()


def spawn_bg(coro):
    """Run a coroutine as fire-and-forget, keeping a strong task reference. Claim latency is sacred: every optional extra (token refresh, AI lookup) runs here, so it can never sit in front of a claim request. """
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)

    def _done(t):
        _bg_tasks.discard(t)
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            print(f"[bg] background task failed: {exc}", flush=True)

    task.add_done_callback(_done)
    return task


def _auth_url(path):
    return f"{CHOW_API_BASE}{path}"


async def _auth_post(path, payload, token=None):
    """POST to the Chowdeck auth API. Returns (response|None, body dict)."""
    headers = dict(_CHOW_HEADERS_BASE)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        # Auth rides the shared connection pool but its per-request timeout is
        # the auth one: a sign-in is allowed to be slower than a claim.
        res = await _CD_CLIENT.post(
            _auth_url(path), json=payload, headers=headers, timeout=AUTH_TIMEOUT
        )
        body = _safe_json(res)
    except Exception as e:  # noqa: BLE001
        return None, {"message": str(e)}
    if LOG_RAW_AUTH:
        print(f"[auth-raw] {path} -> {res.status_code} {json.dumps(body)[:400]}", flush=True)
    return res, body


def _failure_reason(body):
    """Best-effort reason string from either {message} or {data:{...}} shapes."""
    for src in (body or {}, (body or {}).get("data") or {}):
        if not isinstance(src, dict):
            continue
        for key in ("failureReason", "failure_reason", "reason", "code", "error"):
            val = src.get(key)
            if val:
                return str(val)
    return ""


def _iso_from_dial(digits):
    """Split a dial code off `digits`. Returns (iso|None, national digits)."""
    for pref in _DIAL_PREFIXES:
        if digits.startswith(pref):
            rest = digits[len(pref):]
            # A dial code only counts when a plausible national part remains.
            if 6 <= len(rest) <= 12:
                return _DIAL_TO_ISO[pref], rest
    return None, digits


def _normalise_phone(raw, country_code=None):
    """Return (ISO country_code, national digits) for a user-typed phone number. Every shape a user may paste is accepted, so US/Canadian accounts link exactly like Nigerian ones: ``+12125551234`` / ``0012125551234`` / ``1 212 555 1234`` -> ("US", "2125551234") ``2125551234 US`` / ``US 2125551234`` -> ("US", "2125551234") ``12125551234`` (11-digit NANP, unambiguous) -> ("US", "2125551234") ``08031234567`` / ``2348031234567`` -> ("NG", "8031234567") The API wants an ISO alpha-2 code plus the *national* number, which is exactly what this returns. """
    text = str(raw or "").strip()
    digits = re.sub(r"\D", "", text)

    cc = (country_code or "").strip().upper() or None
    if cc and not re.fullmatch(r"[A-Z]{2}", cc):
        cc = None

    # An explicit "+" or "00" prefix always wins: it is the least ambiguous
    # thing the user can type.
    if text.startswith("+") or digits.startswith("00"):
        probe = digits[2:] if digits.startswith("00") else digits
        iso, rest = _iso_from_dial(probe)
        if iso:
            cc, digits = iso, rest
        elif digits.startswith("00"):
            digits = digits[2:]
    if cc is None:
        cc = CHOW_COUNTRY_CODE

    # A leading "1" on an 11-digit number can only be NANP: no national number
    # of a non-NANP country looks like that, so this guess is safe.
    if country_code is None and len(digits) == 11 and digits.startswith("1"):
        cc, digits = "US", digits[1:]

    if cc in _NANP_CC:
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
    else:
        dial = _DIAL_BY_ISO.get(cc)
        if len(digits) > 11 and dial and digits.startswith(dial):
            digits = digits[len(dial):]
        if cc in _TRUNK_ZERO_CC and len(digits) >= 8 and digits.startswith("0"):
            digits = digits[1:]
    return cc, digits


def _alt_national_form(cc, digits):
    """The dial-prefixed form (``1`` + national) used as a login fallback."""
    dial = _DIAL_BY_ISO.get(str(cc or "").upper())
    if not dial or str(digits).startswith(dial):
        return None
    return f"{dial}{digits}"


def _parse_login_arg(arg):
    """Parse `/login` arguments into (ISO cc, national digits, error). Accepts the country code before or after the number, with or without a "+", e.g. ``/login us 2125551234``, ``/login 2125551234 us``. """
    parts = [p for p in re.split(r"[\s,]+", str(arg or "").strip()) if p]
    cc_hint, nums = None, []
    for token in parts:
        if re.fullmatch(r"[A-Za-z]{2}", token):
            cc_hint = token.upper()
        elif re.fullmatch(r"\+?[\d()\- .]+", token) and re.search(r"\d", token):
            nums.append(token)
    if not nums:
        return cc_hint or CHOW_COUNTRY_CODE, "", "no number"
    cc, national = _normalise_phone(" ".join(nums), cc_hint)
    return cc, national, ""


_COUNTRY_LABELS = {
    "NG": "Nigeria", "US": "United States", "CA": "Canada", "GB": "United Kingdom",
    "GH": "Ghana", "KE": "Kenya", "ZA": "South Africa", "IN": "India",
}


def country_label(cc):
    """Human label for an ISO alpha-2 code (falls back to the code itself)."""
    cc = str(cc or "").upper()
    return _COUNTRY_LABELS.get(cc, cc or "?")


def phone_label(cc, digits):
    """`+1 212****234`-style label: shows the country, never the whole number."""
    dial = _DIAL_BY_ISO.get(str(cc or "").upper(), "")
    return f"+{dial} {mask_phone_mid(digits)}" if dial else mask_phone_mid(digits)


def mask_phone_mid(digits):
    d = re.sub(r"\D", "", str(digits or ""))
    if len(d) <= 4:
        return d
    return f"{d[:3]}****{d[-3:]}"


def mask_phone(digits):
    """Never echo a full phone number back into a chat."""
    d = str(digits or "")
    if len(d) <= 4:
        return d
    return f"{d[:3]}****{d[-3:]}"


# --- login-flow store (one pending phone login per chat) -------------------
def get_login_flow(chat_str):
    flow = load_json(LOGIN_FLOW_FILE).get(str(chat_str))
    if not isinstance(flow, dict):
        return None
    if time.time() - float(flow.get("ts") or 0) > OTP_FLOW_TTL:
        clear_login_flow(chat_str)
        return None
    return flow


def set_login_flow(chat_str, flow):
    store = load_json(LOGIN_FLOW_FILE)
    store[str(chat_str)] = flow
    save_json(LOGIN_FLOW_FILE, store)


def clear_login_flow(chat_str):
    store = load_json(LOGIN_FLOW_FILE)
    if str(chat_str) in store:
        del store[str(chat_str)]
        save_json(LOGIN_FLOW_FILE, store)


# --- token helpers ---------------------------------------------------------
def token_exp_ts(token):
    """`exp` claim of a JWT session token, as a unix timestamp (or None)."""
    try:
        payload = parse_jwt_payload(token) or {}
    except Exception:  # noqa: BLE001
        return None
    exp = payload.get("exp")
    try:
        return float(exp) if exp is not None else None
    except (TypeError, ValueError):
        return None


def token_seconds_left(token):
    exp = token_exp_ts(token)
    if exp is None:
        return None
    return exp - time.time()


def human_seconds(seconds):
    seconds = int(max(0, seconds))
    if seconds >= 86400:
        return f"{seconds // 86400}d {(seconds % 86400) // 3600}h"
    if seconds >= 3600:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    if seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def set_slot_active_state(chat_str, slot_id, value):
    """Force a slot's enabled/disabled flag (used to lock in a fresh login)."""
    toggles = load_json(ACCOUNT_TOGGLES_FILE)
    user_toggles = toggles.setdefault(str(chat_str), {})
    user_toggles[str(slot_id)] = bool(value)
    save_json(ACCOUNT_TOGGLES_FILE, toggles)
    return bool(value)


def set_slot_phone(chat_str, slot_id, phone, country_code=None):
    tokens_db = load_json(TOKENS_FILE)
    entry = (tokens_db.get(str(chat_str)) or {}).get(str(slot_id))
    if isinstance(entry, dict) and phone:
        entry["phone"] = str(phone)[:15]
        if country_code:
            entry["country_code"] = str(country_code).upper()[:2]
        save_json(TOKENS_FILE, tokens_db)


def store_slot_token(chat_str, slot, token, *, phone=None, cookies=None, origin="login", country_code=None):
    """Write a slot's session token (+ metadata) without touching other slots. `origin` records where the token came from: "login" for a token this bot minted with its own OTP sign-in, "paste" for a token the user copied out of their browser. `CHOW_AUTO_REFRESH=safe` (the default) only ever refreshes bot-owned tokens, so a live browser session can never be rotated away. """
    tokens_db = load_json(TOKENS_FILE)
    user_slots = tokens_db.setdefault(str(chat_str), {})
    entry = user_slots.get(str(slot))
    if not isinstance(entry, dict):
        entry = {}
    entry["token"] = token
    if cookies is not None:
        entry["cookies"] = cookies
    if phone:
        entry["phone"] = str(phone)[:15]
    if country_code:
        entry["country_code"] = str(country_code).upper()[:2]
    entry["origin"] = origin
    entry["token_exp"] = token_exp_ts(token)
    entry["linked_at"] = time.time()
    user_slots[str(slot)] = entry
    save_json(TOKENS_FILE, tokens_db)
    return entry


# --- auth API calls --------------------------------------------------------
async def chow_validate_phone(phone, country_code):
    """Optional pre-check. Returns True/False, or None when unknown."""
    res, body = await _auth_post(
        "/auth/phone/validate", {"phone": phone, "country_code": country_code}
    )
    if res is None or res.status_code != 200:
        return None
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    val = data.get("is_valid")
    if val is None and isinstance(data.get("data"), dict):
        val = data["data"].get("is_valid")
    return bool(val) if val is not None else None


async def chow_request_otp(phone, country_code):
    """Ask Chowdeck to send a sign-in OTP. Returns (ok, message, reason)."""
    res, body = await _auth_post(
        "/auth/login",
        {"country_code": country_code, "phone": phone, "role": CHOW_AUTH_ROLE},
    )
    if res is None:
        return False, "Could not reach Chowdeck. Please try again.", "network"
    msg = str((body or {}).get("message") or "")
    reason = _failure_reason(body)
    if res.status_code in (200, 201):
        return True, msg or "Verification code sent.", ""
    return False, msg or f"Request rejected (HTTP {res.status_code}).", reason


async def chow_validate_otp(phone, otp, country_code=None):
    """Submit the OTP. Returns (ok, message, data). `country_code` is optional and only sent when known: the endpoint is keyed on the phone, but passing the ISO-2 code keeps US/Canada +1 numbers unambiguous, exactly like the request that produced the OTP. """
    payload = {"otp": str(otp).strip(), "phone": phone, "role": CHOW_AUTH_ROLE}
    if country_code:
        payload["country_code"] = str(country_code).upper()
    res, body = await _auth_post("/auth/login/validate-otp", payload)
    if res is None:
        return False, "Could not reach Chowdeck. Please try again.", {}
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    if res.status_code in (200, 201) and data.get("token"):
        return True, str((body or {}).get("message") or "Signed in."), data
    msg = str((body or {}).get("message") or "")
    return False, msg or f"Code rejected (HTTP {res.status_code}).", data


async def chow_resend_otp(phone, country_code, otp_type="sign_in"):
    res, body = await _auth_post(
        "/auth/resend-otp",
        {
            "phone": phone,
            "role": CHOW_AUTH_ROLE,
            "country_code": country_code,
            "type": otp_type,
        },
    )
    if res is None:
        return False, "Could not reach Chowdeck. Please try again."
    if res.status_code in (200, 201):
        return True, str((body or {}).get("message") or "Code resent.")
    msg = str((body or {}).get("message") or "")
    return False, msg or f"Resend rejected (HTTP {res.status_code})."


async def chow_refresh_token(token):
    """Exchange a (possibly expired) session token for a fresh one."""
    if not token:
        return None
    res, body = await _auth_post("/auth/refresh-token", {"token": token})
    if res is None or res.status_code not in (200, 201):
        return None
    data = body.get("data") if isinstance(body.get("data"), dict) else {}
    new = data.get("token") or (body or {}).get("token")
    return str(new) if new else None


# --- refresh policy + deduplicated background refresh ----------------------
_refresh_tasks = {}
_refresh_lock = threading.Lock()


def _auto_refresh_mode():
    mode = (CHOW_AUTO_REFRESH or "safe").lower()
    if mode in ("0", "false", "no", "off", "never"):
        return "off"
    if mode in ("on", "true", "yes", "all", "always"):
        return "on"
    return "safe"


def _refresh_allowed(entry):
    """Whether this slot's token may be auto-refreshed under the current mode."""
    mode = _auto_refresh_mode()
    if mode == "off":
        return False
    if mode == "on":
        return bool((entry or {}).get("token"))
    return str((entry or {}).get("origin") or "") == "login"


def _token_needs_refresh(entry):
    if not _refresh_allowed(entry):
        return False
    left = token_seconds_left((entry or {}).get("token"))
    if left is None:
        return False  # opaque token: nothing to reason about locally
    return left <= TOKEN_REFRESH_SKEW


async def refresh_slot_token(chat_str, slot, entry=None, force=False):
    """Refresh one slot's token in place. Returns the new token, or None."""
    chat_str, slot = str(chat_str), str(slot)
    if entry is None:
        entry = ((load_json(TOKENS_FILE).get(chat_str) or {}).get(slot)) or {}
    token = (entry or {}).get("token")
    if not token or (not force and not _token_needs_refresh(entry)):
        return token
    new_token = await chow_refresh_token(token)
    if not new_token:
        return None
    store_slot_token(
        chat_str, slot, new_token,
        phone=(entry or {}).get("phone"),
        origin=(entry or {}).get("origin") or "login",
    )
    return new_token


async def _refresh_worker(chat_str, slot, entry):
    key = (str(chat_str), str(slot))
    try:
        return await refresh_slot_token(chat_str, slot, entry, force=True)
    except Exception as e:  # noqa: BLE001
        print(f"[auth] refresh {chat_str}/{slot} failed: {e}", flush=True)
        return None
    finally:
        with _refresh_lock:
            if _refresh_tasks.get(key) is asyncio.current_task():
                _refresh_tasks.pop(key, None)


def start_token_refresh(chat_str, slot, entry):
    """Kick off a de-duplicated background refresh for one slot's token."""
    key = (str(chat_str), str(slot))
    with _refresh_lock:
        task = _refresh_tasks.get(key)
        if task is None or task.done():
            task = asyncio.create_task(_refresh_worker(chat_str, slot, entry))
            _refresh_tasks[key] = task
    return task


# --- health check ----------------------------------------------------------
def _probe_definitive(status):
    """A status that settles the auth question, so no other path need be tried."""
    return status is not None and (200 <= status < 300 or status in (401, 403))


def _probe_note(status, body, path, err=""):
    """Short human diagnosis of a probe result — always shows the real status."""
    msg = ""
    if isinstance(body, dict):
        for key in ("message", "error", "detail"):
            if body.get(key):
                msg = str(body[key]).strip()
                break
        if not msg:
            msg = _failure_reason(body)
    if status is None:
        return f"`{path}` → no response ({err or 'timeout'})"
    tail = f" — {msg}" if msg else ""
    return f"`{path}` → HTTP {status}{tail}"


async def _probe_path(path, token, cookies=None):
    """GET one candidate profile path. Never raises."""
    try:
        res = await _CD_CLIENT.get(
            f"{CHOW_API_BASE}{path}",
            headers=chow_headers(token),
            cookies=cookies or None,
            timeout=AUTH_TIMEOUT,
        )
    except Exception as e:  # noqa: BLE001
        return {"path": path, "status": None, "body": {}, "err": str(e)}
    return {"path": path, "status": res.status_code, "body": _safe_json(res), "err": ""}


async def chow_profile(token, cookies=None):
    """Probe the account profile, trying ``HEALTH_PROBE_PATHS`` in order. Returns ``(status|None, phone, data, note)``. Requests are staggered and the first *decisive* answer (2xx / 401 / 403) wins, so a renamed or missing first path falls through quickly instead of yielding a mystery result. The returned ``note`` carries the path, the HTTP status and the API message, so the health view never reduces a failure to "unexpected response". """
    if not token:
        return None, None, {}, "no token stored"
    paths = list(HEALTH_PROBE_PATHS) or ["/customer/profile"]
    sem = asyncio.Semaphore(HEALTH_PROBE_CONCURRENCY)
    tasks, winner = [], None

    async def run(path):
        async with sem:
            return await _probe_path(path, token, cookies)

    for idx, path in enumerate(paths):
        tasks.append(asyncio.create_task(run(path)))
        if idx < len(paths) - 1 and HEALTH_PROBE_STAGGER:
            await asyncio.sleep(HEALTH_PROBE_STAGGER)
        for task in tasks:
            if task.done() and _probe_definitive(task.result()["status"]):
                winner = task.result()
                break
        if winner:
            break
    if winner is None:
        settled = await asyncio.gather(*tasks)
        for result in settled:
            if _probe_definitive(result["status"]):
                winner = result
                break
        if winner is None:
            winner = settled[-1] if settled else {
                "path": paths[0], "status": None, "body": {}, "err": ""
            }
    pending = [t for t in tasks if not t.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    body = winner["body"] if isinstance(winner.get("body"), dict) else {}
    data = body.get("data") if isinstance(body.get("data"), dict) else body
    if not isinstance(data, dict):
        data = {}
    phone = extract_phone_from_data(data) or extract_phone_from_data(body)
    note = _probe_note(winner.get("status"), body, winner.get("path") or paths[0],
                       winner.get("err") or "")
    return winner.get("status"), phone, data, note


async def probe_slot_token(chat_str, slot, allow_refresh=True):
    """Live health of one slot: refresh if needed, then probe the profile."""
    chat_str, slot = str(chat_str), str(slot)
    entry = ((load_json(TOKENS_FILE).get(chat_str) or {}).get(slot)) or {}
    token = (entry or {}).get("token")
    if not token:
        return {"slot": slot, "state": "empty", "status": None, "note": "not linked",
                "entry": entry}
    if allow_refresh and _token_needs_refresh(entry):
        new_token = await refresh_slot_token(chat_str, slot, entry, force=True)
        if new_token:
            entry = {**entry, "token": new_token}
            token = new_token
    status, phone, _data, note = await chow_profile(token, (entry or {}).get("cookies"))
    if status in (401, 403) and allow_refresh and _refresh_allowed(entry):
        new_token = await refresh_slot_token(chat_str, slot, entry, force=True)
        if new_token:
            entry = {**entry, "token": new_token}
            token = new_token
            status, phone, _data, note = await chow_profile(token, (entry or {}).get("cookies"))
    if phone and str(phone) != str((entry or {}).get("phone") or ""):
        set_slot_phone(chat_str, slot, phone)
        entry = {**entry, "phone": str(phone)[:15]}
    if status == 200:
        state = "active"
    elif status in (401, 403):
        state = "expired"
    elif status is None:
        state = "unreachable"
    else:
        state = "error"
    return {
        "slot": slot, "state": state, "status": status, "note": note,
        "phone": phone or (entry or {}).get("phone"), "entry": entry,
    }


async def check_all_slots(chat_str, concurrency=4):
    """Probe every configured slot (bounded concurrency) for the health view."""
    chat_str = str(chat_str)
    max_slots = get_max_slots(chat_str)
    configured = load_json(TOKENS_FILE).get(chat_str, {}) or {}
    slot_ids = [str(i) for i in range(1, max_slots + 1)]
    for sid in configured:
        if str(sid) not in slot_ids:
            slot_ids.append(str(sid))
    sem = asyncio.Semaphore(max(1, concurrency))

    async def one(sid):
        async with sem:
            try:
                return await probe_slot_token(chat_str, sid)
            except Exception as e:  # noqa: BLE001
                return {"slot": sid, "state": "error", "status": None,
                        "entry": {}, "error": str(e)}

    return list(await asyncio.gather(*(one(sid) for sid in slot_ids)))


_STATE_ICON = {
    "active": "🟢", "expired": "🔴", "expiring": "🟡",
    "empty": "⚫", "unreachable": "🟠", "error": "🟠",
}


def format_accounts_health(results, chat_str=None):
    """Render the multi-line accounts-health report (one line per slot)."""
    lines = ["🩺 *Account health check*", ""]
    active = 0
    for r in results:
        state = r.get("state")
        if state == "active":
            active += 1
        icon = _STATE_ICON.get(state, "⚪")
        phone = r.get("phone") or ((r.get("entry") or {}).get("phone"))
        label = f"{icon} S{r.get('slot')}"
        if phone:
            label += f": `{phone}`"
        detail = {
            "active": "active",
            "expired": "token expired — sign in again with /login",
            "empty": "empty — link with /login or /settoken",
            "unreachable": "no response",
            "error": "unexpected response",
        }.get(state, state or "unknown")
        note = r.get("note")
        if state == "active":
            left = token_seconds_left((r.get("entry") or {}).get("token"))
            if left is not None:
                detail += f" · {human_seconds(left)} left"
        elif note and state != "empty":
            # Show the path, HTTP status and API message: a failure must be
            # diagnosable from the health view alone.
            detail += f" · {note}"
        if chat_str is not None and state != "empty" and not is_slot_active_state(chat_str, r.get("slot")):
            detail += " · ⏸️ disabled"
        lines.append(f"{label} — {detail}")
    lines.append("")
    lines.append(f"🟢 active: `{active}` / `{len(results)}` slot(s)")
    return "\n".join(lines)


# --- phone -> OTP login flow ----------------------------------------------
def _phone_taken_by_other_slot(chat_str, phone, slot, country_code=None):
    """Return the other slot id already linked to `phone`, else None. One account can only claim a code once, so the same phone must not be linked to two slots of the same user. The match is country-aware: identical national digits in two different countries are two different accounts, so a US number never blocks an identical-looking number elsewhere. """
    tokens_db = load_json(TOKENS_FILE).get(str(chat_str), {}) or {}
    target = re.sub(r"\D", "", str(phone or ""))
    target_cc = str(country_code or "").upper() or None
    for sid, td in tokens_db.items():
        if str(sid) == str(slot):
            continue
        other = re.sub(r"\D", "", str((td or {}).get("phone") or ""))
        if not target or other != target:
            continue
        other_cc = str((td or {}).get("country_code") or "").upper() or None
        # Only a known, different country clears the duplicate.
        if target_cc and other_cc and other_cc != target_cc:
            continue
        return str(sid)
    return None


async def start_otp_login(chat_id, chat_str, arg):
    """`/login <phone>` - request a Chowdeck OTP and arm the code capture."""
    chat_str = str(chat_str)
    if not LOGIN_ENABLED:
        await send_telegram(chat_id, "🔐 Phone login is disabled on this deployment. Use `/settoken`.")
        return
    if not arg:
        await send_telegram(
            chat_id,
            "🔐 *Login with phone (OTP)*\n\n"
            "Usage: `/login <phone>`\n"
            "Example: `/login 08031234567`\n\n"
            "The bot signs in on its own (a second session, separate from your "
            "browser). You then send the code here and the account is linked to "
            "the active slot.",
        )
        return
    country_code, phone, parse_err = _parse_login_arg(arg)
    if parse_err or len(phone) < 7:
        await send_telegram(
            chat_id,
            "⚠️ That does not look like a phone number.\n\n"
            "Usage: `/login <phone>` — e.g. `/login 08031234567` (NG), "
            "`/login 2125551234 US` or `/login +12125551234`.\n"
            "A country code (ISO-2 like `US`, or `+1`) is optional; without one "
            f"the default is `{CHOW_COUNTRY_CODE}`.",
        )
        return
    slot = get_active_slot(chat_str)
    dup = _phone_taken_by_other_slot(chat_str, phone, slot, country_code)
    if dup:
        await send_telegram(
            chat_id,
            f"⚠️ That phone is already linked to *Slot {dup}*. An account can only "
            f"claim a code once, so remove it (`/settoken` on that slot) before "
            f"re-linking it here.",
        )
        return
    # The pre-check is only a courtesy: a false verdict must never block a
    # login. The US and Canada share +1, so the sibling code is tried before a
    # number is declared unknown.
    cc_try = [country_code]
    if country_code in _NANP_CC:
        cc_try += [c for c in _NANP_CC if c != country_code]
    plausible = await chow_validate_phone(phone, cc_try[0])
    if plausible is False and len(cc_try) > 1:
        if await chow_validate_phone(phone, cc_try[1]):
            plausible, cc_try = True, [cc_try[1]]
    if plausible is False:
        await send_telegram(
            chat_id,
            f"⚠️ Chowdeck does not recognise `{phone_label(country_code, phone)}` "
            f"({country_label(country_code)}).\n\n"
            "Check the number, or state the country explicitly — e.g. "
            "`/login 2125551234 US`, `/login +442079460000`.",
        )
        return
    await send_telegram(
        chat_id,
        f"📲 Requesting a Chowdeck verification code for "
        f"`{phone_label(cc_try[0], phone)}` ({country_label(cc_try[0])})…",
    )
    ok, message, reason = await chow_request_otp(phone, cc_try[0])
    if not ok and len(cc_try) > 1:
        low = f"{message} {reason}".lower()
        if any(w in low for w in ("invalid", "exist", "not found", "recognis", "recogniz")):
            ok, message, reason = await chow_request_otp(phone, cc_try[1])
            if ok:
                cc_try = [cc_try[1]]
    if not ok:
        hint = ""
        low = f"{message} {reason}".lower()
        if "exist" in low or "not found" in low or "invalid" in low:
            hint = "\n\nCheck the number and try again with `/login <phone>`."
        await send_telegram(chat_id, f"❌ {message}{hint}")
        return
    country_code = cc_try[0]
    set_login_flow(chat_str, {
        "phone": phone,
        "country_code": country_code,
        "slot": str(slot),
        "ts": time.time(),
        "otp_type": "sign_in",
        "attempts": 0,
        "resends": 0,
        "last_resend": time.time(),
    })
    await send_telegram(
        chat_id,
        f"✅ *Code sent to* `{mask_phone(phone)}`\n\n"
        f"👉 Send the *code* in this chat to finish signing in (Slot {slot}).\n"
        f"• `/otp <code>` if you prefer to be explicit\n"
        f"• `/resendotp` to send a new code\n"
        f"• `/cancelotp` to abort\n\n"
        f"_While this login is open, a bare 4- or 6-digit message is treated as the code._",
    )


async def complete_otp_login(chat_id, chat_str, code, flow):
    """Submit the OTP, link the resulting token, and enable (lock in) the slot."""
    chat_str = str(chat_str)
    flow = dict(flow or {})
    phone = flow.get("phone")
    slot = str(flow.get("slot") or get_active_slot(chat_str))
    ok, message, data = await chow_validate_otp(
        phone, code, flow.get("country_code")
    )
    if not ok:
        flow["attempts"] = int(flow.get("attempts") or 0) + 1
        if flow["attempts"] >= 5:
            clear_login_flow(chat_str)
            await send_telegram(
                chat_id,
                f"❌ {message}\n\nToo many attempts — login cancelled. "
                f"Start again with `/login <phone>`.",
            )
            return False
        flow["ts"] = time.time()
        set_login_flow(chat_str, flow)
        await send_telegram(
            chat_id,
            f"❌ {message}\n\nTry again (attempt {flow['attempts']}/5), "
            f"`/resendotp`, or `/cancelotp`.",
        )
        return False
    token = str(data.get("token"))
    linked_phone = data.get("phone") or phone
    store_slot_token(
        chat_str, slot, token, phone=linked_phone, origin="login",
        country_code=flow.get("country_code"),
    )
    set_slot_active_state(chat_str, slot, True)
    set_active_slot(chat_str, slot)
    clear_login_flow(chat_str)

    left = token_seconds_left(token)
    name_bits = [str(data.get(k) or "").strip() for k in ("first_name", "last_name")]
    name = " ".join(b for b in name_bits if b)
    linked_cc = flow.get("country_code") or CHOW_COUNTRY_CODE
    masked = phone_label(linked_cc, re.sub(r"\D", "", str(linked_phone or "")))
    expiry_txt = f"\n• Token valid for: `{human_seconds(left)}`" if left is not None else ""
    refresh_txt = (
        "\n• Auto-refresh: `ON` (bot-owned session)"
        if _refresh_allowed({"origin": "login", "token": token})
        else "\n• Auto-refresh: `OFF` (set `CHOW_AUTO_REFRESH=safe` or `on`)"
    )
    await send_telegram(
        chat_id,
        f"🔓 *Account locked in — Slot {slot}*\n\n"
        f"• Phone: `{masked}`"
        + (f"\n• Name: {name}" if name else "")
        + expiry_txt + refresh_txt +
        "\n\nThis is the bot's *own* session. Your browser session (if any) is "
        "untouched, and vice-versa. The slot is now ✅ enabled and will claim "
        "codes immediately.",
    )
    return True


async def resend_login_otp(chat_id, chat_str):
    flow = get_login_flow(chat_str)
    if not flow:
        await send_telegram(chat_id, "ℹ️ No login is waiting for a code. Start one with `/login <phone>`.")
        return
    now = time.time()
    wait = OTP_RESEND_COOLDOWN - (now - float(flow.get("last_resend") or 0))
    if wait > 0:
        await send_telegram(chat_id, f"⏳ Please wait `{int(wait) + 1}s` before requesting another code.")
        return
    ok, message = await chow_resend_otp(
        flow.get("phone"), flow.get("country_code"), flow.get("otp_type") or "sign_in"
    )
    if not ok:
        await send_telegram(chat_id, f"❌ {message}")
        return
    flow["resends"] = int(flow.get("resends") or 0) + 1
    flow["last_resend"] = now
    flow["ts"] = now
    set_login_flow(chat_str, flow)
    await send_telegram(
        chat_id,
        f"✅ New code sent to `{mask_phone(flow.get('phone'))}`. Send the code here.",
    )


def looks_like_otp(text):
    """A bare 4- or 6-digit message: the shapes a Chowdeck OTP can take."""
    return bool(re.fullmatch(r"\d{4}|\d{6}", str(text or "").strip()))


async def maybe_handle_login_otp(chat_id, chat_str, text, reply_to, pending):
    """Feature 1: a bare code completes an open OTP login. Only fires while THIS chat has a login awaiting a code, and never for a message that is a direct reply to a security-question prompt (that is an answer, not an OTP). Returns True when the message was consumed. """
    if not LOGIN_ENABLED:
        return False
    flow = get_login_flow(chat_str)
    if not flow:
        return False
    if not looks_like_otp(text):
        return False
    answering = (
        reply_to is not None
        and any((e or {}).get("msg_id") == reply_to for e in (pending or []))
    )
    if answering:
        return False
    await complete_otp_login(chat_id, chat_str, str(text).strip(), flow)
    return True


# ---------------------------------------------------------------------------
# Feature 2 - AI security-answer assist (real-time web search)
# ---------------------------------------------------------------------------
# The claim path is untouched. When a slot reports a security question the
# existing prompt is sent immediately and this lookup is spawned in the
# BACKGROUND: its answer is only ever a suggestion, so a slow or failing AI can
# never delay a claim, and the user still decides what gets submitted.
_AI_MODELS = {
    "perplexity": "sonar",
    "openai": "gpt-4o-mini",
    "gemini": "gemini-2.0-flash",
}

_AI_CLIENT = httpx.AsyncClient(
    timeout=httpx.Timeout(AI_TIMEOUT, connect=4.0),
    proxy=PROXY_URL,
    limits=httpx.Limits(max_keepalive_connections=8, max_connections=16),
)
# The late pass gets its own client on purpose: a search-grounded answer often
# needs longer than AI_TIMEOUT, and reusing the fast client would raise
# ReadTimeout long before AI_LATE_DEADLINE was ever reached.
_AI_CLIENT_LATE = httpx.AsyncClient(
    timeout=httpx.Timeout(max(AI_LATE_DEADLINE, AI_TIMEOUT) + 2.0, connect=4.0),
    proxy=PROXY_URL,
    limits=httpx.Limits(max_keepalive_connections=8, max_connections=16),
)

# Which client the current lookup should use. Set per lookup in
# ai_answer_candidates() and inherited by its provider tasks, so the provider
# functions keep the plain (question) signature the rest of the code expects.
_AI_CLIENT_CTX = contextvars.ContextVar("ai_client", default=None)


def _ai_client():
    return _AI_CLIENT_CTX.get() or _AI_CLIENT


_AI_MODEL_VARS = {
    "perplexity": lambda: PERPLEXITY_MODEL,
    "openai": lambda: OPENAI_MODEL,
    "gemini": lambda: GEMINI_MODEL,
}


def _ai_model_for(provider):
    """Model to call for `provider`. Per-provider vars win. `AI_MODEL` stays supported as a global override, but it is only applied to the provider it can plausibly belong to (the one pinned with AI_PROVIDER, or the only one configured): sending Gemini's model name to Perplexity is a guaranteed 400, which is exactly the kind of silent failure this feature must not have. """
    override = (_AI_MODEL_VARS.get(provider) or (lambda: ""))()
    if override:
        return override
    if AI_MODEL:
        order = _ai_provider_order()
        if AI_PROVIDER == provider or (
            not AI_PROVIDER and order and order[0] == provider
        ):
            return AI_MODEL
    return _AI_MODELS.get(provider, "")

_AI_PROMPT = (
    "You answer Chowdeck promo-code security questions. Chowdeck is a Nigerian "
    "food-delivery app; its security questions are short factual questions about "
    "Chowdeck itself, its vendors or partners, Nigerian and world events, sports "
    "clubs, or popular culture. Search the web, then put the single most likely "
    "answer first.\n"
    "Output rules: one answer per line and nothing else. No sentences, no "
    "explanation, no citations, no quotes, no numbering, no bullets. Each line is "
    "a bare value of 30 characters or fewer, for example `3500`, `Lagos` or "
    "`May 2024`. Give up to {n} lines, most likely first.\n\n"
    "Question: {question}"
)


def _ai_enabled():
    """`AI_ASSIST_ENABLED=auto` means: on as soon as any provider key exists."""
    if AI_ASSIST_ENABLED in ("0", "false", "no", "off", "never"):
        return False
    if AI_ASSIST_ENABLED in ("1", "true", "yes", "on", "always"):
        return True
    return bool(PERPLEXITY_API_KEY or OPENAI_API_KEY or GEMINI_API_KEY or TAVILY_API_KEY)


def _ai_provider_order():
    """Providers to try, best-first. `AI_PROVIDER` pins the preferred one."""
    available = []
    if PERPLEXITY_API_KEY:
        available.append("perplexity")
    if OPENAI_API_KEY:
        available.append("openai")
    if GEMINI_API_KEY:
        available.append("gemini")
    if TAVILY_API_KEY:
        available.append("tavily")
    if AI_PROVIDER and AI_PROVIDER in available:
        available.remove(AI_PROVIDER)
        available.insert(0, AI_PROVIDER)
    return available


def _parse_ai_answers(text, limit=None):
    """Turn a model reply into a clean, de-duplicated list of candidate answers. Two stages: the strict line-per-answer parse first, and - only when that yields nothing - the prose salvage (``_ai_prose_candidates``). A grounded model that answers in a full sentence is a good answer, not an empty one. """
    limit = int(limit or AI_MAX_ANSWERS)
    text = str(text or "")
    out, seen = [], set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        line = re.sub(r"^\s*(?:[-*\u2022]|\d+[.)])\s*", "", line).strip()
        line = line.strip("'\"`*_ ").replace("`", "'").strip()
        if not line or len(line) > 60:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(line[:40])
        if len(out) >= limit:
            break
    if not out:
        out = _ai_prose_candidates(text, limit)
    return out


# --- Last-outcome bookkeeping -------------------------------------------------
# A failed provider used to be swallowed completely: the lookup returned an
# empty list and the chat heard nothing, so a missing key, a renamed model or a
# grounding tool the model does not support looked exactly like a feature that
# was never wired up. Every failure is now recorded here (and printed), so `/ai`
# can name the reason and the chat can say why no suggestion arrived.
_AI_LAST = {"provider": "", "kind": "", "detail": "", "ok": False, "ts": 0.0}


def _ai_body_snippet(res, limit=160):
    """First `limit` characters of a response body, for the failure record."""
    try:
        body = re.sub(r"\s+", " ", str(res.text or "")).strip()
    except Exception:  # noqa: BLE001
        body = ""
    return body[:limit]


def _ai_note_error(provider, kind, detail=""):
    _AI_LAST.update({
        "provider": str(provider), "kind": str(kind),
        "detail": str(detail or ""), "ok": False, "ts": time.time(),
    })
    line = f"{provider}: {kind}"
    if detail:
        line += f" - {detail}"
    print(f"[ai] {line}", flush=True)


def _ai_note_ok(provider):
    _AI_LAST.update({
        "provider": str(provider), "kind": "", "detail": "",
        "ok": True, "ts": time.time(),
    })


def _ai_failure_note():
    """One short, Markdown-safe sentence describing the last lookup outcome."""
    ts = float(_AI_LAST.get("ts") or 0.0)
    if not ts:
        return "no lookup recorded yet"
    provider = str(_AI_LAST.get("provider") or "ai")
    label = _AI_PROVIDER_LABELS.get(provider, provider)
    kind = str(_AI_LAST.get("kind") or "").strip()
    detail = re.sub(r"[*_\[\]`]", " ", str(_AI_LAST.get("detail") or "")).strip()
    mins = int(max(0.0, time.time() - ts) // 60)
    age = f", {mins}m ago" if mins else ", just now"
    if _AI_LAST.get("ok"):
        return f"{label} answered{age}"
    if detail:
        return f"{label} returned {kind or 'an error'} ({detail}){age}"
    if kind:
        return f"{label}: {kind}{age}"
    return f"no usable answer{age}"


def _pending_question_open(chat_str, code):
    """True while the security question for `code` is still awaiting an answer. A second-pass suggestion is only worth posting while the question is still on screen: once the user has answered, the prompt has been consumed and a late result must be dropped instead of popping up a stale button. """
    try:
        return any(
            str((entry or {}).get("code") or "") == str(code)
            for entry in list_pending(str(chat_str))
        )
    except Exception:  # noqa: BLE001
        return True


# --- Prose salvage ------------------------------------------------------------
# The prompt asks for bare values, one per line, and most models comply. A
# web-grounded model sometimes answers in a sentence anyway ("Based on the
# search results, the answer is 3500 (source: chowdeck.com)."), and the strict
# line parser dropped anything longer than 60 characters - which turned a
# correct answer into no suggestion at all. These helpers recover the short
# value out of that sentence, and are only used when the strict parse is empty.
_AI_FILLER_RE = re.compile(
    r"^(?:the\s+answer\s+is\s*|the\s+answer\s*[:=-]\s*|answer\s*[:=-]\s*|"
    r"it\s+is\s+|it'?s\s+|i\s+(?:think|believe)\s+(?:the\s+answer\s+is\s+)?|"
    r"(?:the\s+)?(?:most\s+)?likely\s+answer\s*[:=-]?\s*|"
    r"based\s+on\s+[^,]{0,80},\s*(?:the\s+answer\s+is\s+)?|"
    r"according\s+to\s+[^,]{0,80},\s*)",
    re.IGNORECASE,
)
_AI_CITE_RE = re.compile(
    r"(?:\[\d+\]|\((?:source|sources|ref)\s*:[^)]*\)|\bsources?\s*:[^\n]*|https?://\S+)",
    re.IGNORECASE,
)


def _ai_clean_value(raw):
    """Strip filler, citations and trailing punctuation from one candidate value."""
    value = str(raw or "").strip()
    if not value:
        return ""
    value = _AI_CITE_RE.sub(" ", value)
    value = _AI_FILLER_RE.sub("", value).strip()
    value = re.split(r"(?<=[.!?;])\s+|\s+[|\u2013\u2014]\s+", value, maxsplit=1)[0]
    value = value.strip().strip("'\"`*_ ")
    value = value.strip(" .;:,-")
    value = re.sub(r"\s+", " ", value)
    if not value or len(value) > 30:
        return ""
    return value


def _ai_prose_candidates(text, limit=None):
    """Recover short answer values from a prose reply (fallback only)."""
    limit = int(limit or AI_MAX_ANSWERS)
    out, seen = [], set()
    for raw_line in str(text or "").splitlines():
        line = re.sub(r"^\s*(?:[-*\u2022]|\d+[.)])\s*", "", raw_line).strip()
        if not line:
            continue
        for piece in [line] + re.split(r"[;!?]\s+|\.\s+", line):
            value = _ai_clean_value(piece)
            if not value:
                continue
            key = value.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(value[:40])
            if len(out) >= limit:
                return out
    return out


async def _ai_perplexity(question):
    """Perplexity `sonar` is web-grounded by design: no extra tool needed."""
    client = _ai_client()
    model = _ai_model_for("perplexity")
    try:
        res = await client.post(
            "https://api.perplexity.ai/chat/completions",
            headers={
                "Authorization": f"Bearer {PERPLEXITY_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": model,
                "temperature": 0.2,
                "messages": [{
                    "role": "user",
                    "content": _AI_PROMPT.format(n=AI_MAX_ANSWERS, question=question),
                }],
            },
        )
    except Exception as e:  # noqa: BLE001
        _ai_note_error("perplexity", type(e).__name__, str(e))
        return []
    if res.status_code != 200:
        _ai_note_error("perplexity", f"HTTP {res.status_code}", _ai_body_snippet(res))
        return []
    choices = res.json().get("choices") or []
    if not choices:
        _ai_note_error("perplexity", "empty completion")
        return []
    return _parse_ai_answers((choices[0].get("message") or {}).get("content"))


async def _ai_openai(question):
    model = _ai_model_for("openai")
    client = _ai_client()
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    prompt = _AI_PROMPT.format(n=AI_MAX_ANSWERS, question=question)
    # Preferred: the Responses API with the built-in web-search tool (live web).
    try:
        res = await client.post(
            "https://api.openai.com/v1/responses",
            headers=headers,
            json={
                "model": model,
                "tools": [{"type": "web_search_preview"}],
                "input": prompt,
            },
        )
        if res.status_code == 200:
            body = res.json()
            text = body.get("output_text")
            if not text:
                chunks = []
                for item in body.get("output") or []:
                    for part in (item or {}).get("content") or []:
                        if isinstance(part, dict) and part.get("text"):
                            chunks.append(str(part["text"]))
                text = "\n".join(chunks)
            answers = _parse_ai_answers(text)
            if answers:
                return answers
        else:
            _ai_note_error("openai", f"HTTP {res.status_code}", _ai_body_snippet(res))
    except Exception as e:  # noqa: BLE001
        _ai_note_error("openai", type(e).__name__, str(e))
    # Fallback: classic chat completions (model knowledge, still useful).
    try:
        res = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json={
                "model": model,
                "temperature": 0.2,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        if res.status_code == 200:
            choices = res.json().get("choices") or []
            if choices:
                return _parse_ai_answers((choices[0].get("message") or {}).get("content"))
        else:
            _ai_note_error("openai", f"HTTP {res.status_code}", _ai_body_snippet(res))
    except Exception as e:  # noqa: BLE001
        _ai_note_error("openai", type(e).__name__, str(e))
    return []


async def _ai_gemini(question):
    """Gemini with the Google Search tool (live web), model knowledge as backup."""
    client = _ai_client()
    model = _ai_model_for("gemini")
    prompt = _AI_PROMPT.format(n=AI_MAX_ANSWERS, question=question)

    async def ask(with_search):
        body = {"contents": [{"parts": [{"text": prompt}]}]}
        if with_search:
            body["tools"] = [{"google_search": {}}]
        try:
            res = await client.post(
                "https://generativelanguage.googleapis.com/v1beta/models/"
                f"{model}:generateContent",
                headers={
                    "x-goog-api-key": GEMINI_API_KEY,
                    "Content-Type": "application/json",
                },
                json=body,
            )
        except Exception as e:  # noqa: BLE001
            _ai_note_error("gemini", type(e).__name__, str(e))
            return None, []
        if res.status_code != 200:
            detail = _ai_body_snippet(res)
            _ai_note_error("gemini", f"HTTP {res.status_code}", detail)
            return detail, []
        payload = res.json() or {}
        chunks = []
        finish = ""
        for cand in payload.get("candidates") or []:
            finish = str((cand or {}).get("finishReason") or finish)
            for part in ((cand or {}).get("content") or {}).get("parts") or []:
                if isinstance(part, dict) and part.get("text"):
                    chunks.append(str(part["text"]))
        answers = _parse_ai_answers("\n".join(chunks))
        if not answers and finish:
            _ai_note_error("gemini", f"no text (finishReason {finish})")
        return finish, answers

    note, answers = await ask(True)
    if answers:
        return answers
    # Search grounding is not available on every model / API version (the API
    # answers with a 400 saying so). Instead of reporting nothing at all, retry
    # once without the tool so the model's own knowledge still gets a chance.
    lowered = str(note or "").lower()
    if lowered and any(w in lowered for w in ("ground", "tool", "not supported")):
        fallback = (await ask(False))[1]
        if fallback:
            return fallback
    return []


async def _ai_tavily(question):
    """Tavily is a search API: its `answer` field is the grounded suggestion."""
    client = _ai_client()
    try:
        res = await client.post(
            "https://api.tavily.com/search",
            headers={"Content-Type": "application/json"},
            json={
                "api_key": TAVILY_API_KEY,
                "query": question,
                "search_depth": "basic",
                "include_answer": True,
                "max_results": 5,
            },
        )
    except Exception as e:  # noqa: BLE001
        _ai_note_error("tavily", type(e).__name__, str(e))
        return []
    if res.status_code != 200:
        _ai_note_error("tavily", f"HTTP {res.status_code}", _ai_body_snippet(res))
        return []
    body = res.json()
    lines = []
    if body.get("answer"):
        lines.append(str(body["answer"]).strip())
    for item in body.get("results") or []:
        title = str((item or {}).get("title") or "").strip()
        if title:
            lines.append(title)
    return _parse_ai_answers("\n".join(lines))


_AI_PROVIDER_FUNCS = {
    "perplexity": _ai_perplexity,
    "openai": _ai_openai,
    "gemini": _ai_gemini,
    "tavily": _ai_tavily,
}

_AI_PROVIDER_LABELS = {
    "perplexity": "Perplexity",
    "openai": "OpenAI",
    "gemini": "Gemini",
    "tavily": "Tavily",
}


async def _ai_with_client(func, client, question):
    """Run one provider function with `client` bound for the call. The provider functions keep their plain ``(question)`` signature; the client for the current pass is handed to them through a ContextVar so the fast and late passes can use different timeouts without touching every provider. """
    token = _AI_CLIENT_CTX.set(client)
    try:
        return await func(question)
    finally:
        _AI_CLIENT_CTX.reset(token)


async def _ai_lookup_once(question, deadline, client):
    """One full lookup: ``(provider, [answers], elapsed_seconds)``. Providers are raced in parallel (``AI_RACE``, the default) and the first usable answer set wins, so one slow provider can no longer hold the whole lookup for its full timeout while the next one waits its turn. The lookup is bounded by ``deadline``, which is what keeps the fast pass inside the 2-3s window. """
    funcs = [(n, _AI_PROVIDER_FUNCS[n]) for n in _ai_provider_order()
             if _AI_PROVIDER_FUNCS.get(n)]
    if not funcs:
        _ai_note_error("ai", "no provider configured")
        return None, [], 0.0
    started = time.monotonic()

    async def call(name, func):
        try:
            return name, await asyncio.wait_for(
                _ai_with_client(func, client, question), timeout=deadline
            )
        except Exception as e:  # noqa: BLE001
            print(f"[ai] {name} failed: {e}", flush=True)
            return name, []

    if not AI_RACE or len(funcs) == 1:
        for name, func in funcs:
            got_name, answers = await call(name, func)
            if answers:
                _ai_note_ok(got_name)
                return got_name, answers, time.monotonic() - started
        return None, [], time.monotonic() - started

    tasks = {asyncio.create_task(call(name, func)): name for name, func in funcs}
    stop_at = started + deadline
    winner = (None, [])
    try:
        while tasks:
            budget = stop_at - time.monotonic()
            if budget <= 0:
                break
            done, _ = await asyncio.wait(
                set(tasks), timeout=budget, return_when=asyncio.FIRST_COMPLETED
            )
            if not done:
                break
            for task in done:
                tasks.pop(task, None)
                if winner[0] is None:
                    name, answers = task.result()
                    if answers:
                        winner = (name, answers)
            if winner[0] is not None:
                break
    finally:
        pending = [t for t in tasks if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    if winner[0] is not None:
        # Record the win: without this the last-outcome note (shown by `/ai`)
        # would keep quoting a losing provider's error even though a suggestion
        # was posted - exactly the "is this even wired up?" confusion this
        # feature has to avoid.
        _ai_note_ok(winner[0])
    return winner[0], winner[1], time.monotonic() - started


async def ai_answer_candidates(question, late=False):
    """Look one question up: ``(provider, [answers], elapsed_seconds)``. ``late=False`` is the fast pass (``AI_DEADLINE``, short-timeout client): it is what keeps a suggestion inside the 2-3s claim window. ``late=True`` is the second chance (``AI_LATE_DEADLINE``, long-timeout client): a web-search grounded model routinely needs longer than the fast budget, and a late answer still helps because the question stays open until it is answered. """
    question = str(question or "").strip()
    if not question or not _ai_enabled():
        return None, [], 0.0
    if late:
        return await _ai_lookup_once(question, AI_LATE_DEADLINE, _AI_CLIENT_LATE)
    return await _ai_lookup_once(question, AI_DEADLINE, _AI_CLIENT)


def ai_status_text():
    """Read-only summary of the AI assist setup (for `/ai` with no argument)."""
    order = [n for n in _ai_provider_order() if _AI_PROVIDER_FUNCS.get(n)]
    labels = ", ".join(_AI_PROVIDER_LABELS.get(n, n) for n in order) or "*(none configured)*"
    models = ", ".join(
        f"{_AI_PROVIDER_LABELS.get(n, n)} `{_ai_model_for(n)}`"
        for n in order if _ai_model_for(n)
    ) or "*(provider default)*"
    late = (
        f"late pass `{AI_LATE_DEADLINE:g}s`"
        if AI_LATE_DEADLINE > AI_DEADLINE else "late pass `off`"
    )
    return (
        "🤖 *AI answer assist*\n\n"
        f"• Status: `{'ON' if _ai_enabled() else 'OFF'}`\n"
        f"• Providers: {labels}\n"
        f"• Models: {models}\n"
        f"• Lookup: `{'parallel race' if AI_RACE else 'sequential'}` · "
        f"fast pass `{AI_DEADLINE:g}s` · {late} · per-call cap `{AI_TIMEOUT:g}s`\n"
        f"• `AI_MODEL` override: `{AI_MODEL or 'not set'}`\n"
        f"• Failure notice: `{'ON' if AI_NOTIFY_FAILURE else 'OFF'}`\n"
        f"• Last lookup: {_ai_failure_note()}\n\n"
        "• It runs automatically the moment a claim hits a security question: "
        "the likely answers arrive with tap-to-submit buttons.\n"
        "• `/ai <question>` — look one up right now.\n\n"
        "_A provider key is required (`PERPLEXITY_API_KEY`, `OPENAI_API_KEY`, "
        "`GEMINI_API_KEY` or `TAVILY_API_KEY`). With none set the assist stays "
        "off and the bot behaves exactly as before._"
    )


def set_pending_ai(chat_str, code, provider, answers):
    """Attach AI suggestions to the live prompt for `code` (no-op if gone)."""
    store = load_json(PENDING_PROMPT_FILE)
    bucket = list(store.get(str(chat_str), []))
    changed = False
    for entry in bucket:
        if isinstance(entry, dict) and entry.get("code") == code:
            entry["ai"] = {
                "provider": provider,
                "answers": list(answers),
                "ts": time.time(),
            }
            changed = True
    if changed:
        store[str(chat_str)] = bucket
        save_json(PENDING_PROMPT_FILE, store)


def ai_answer_keyboard(code, answers, submitted=None):
    """One tap = submit that suggestion. `submitted` marks the chosen one."""
    rows = []
    for idx, answer in enumerate(answers):
        prefix = "✅" if submitted is not None and idx == submitted else "💡"
        rows.append([{
            "text": f"{prefix} {answer}"[:60],
            "callback_data": f"aians_{idx}_{code}",
        }])
    return {"inline_keyboard": rows} if rows else None


async def _ai_post_suggestions(chat_id, chat_str, code, question, provider, answers, elapsed):
    """Attach suggestions to the live prompt and post the tap-to-submit message."""
    set_pending_ai(chat_str, code, provider, answers)
    label = _AI_PROVIDER_LABELS.get(provider, provider)
    tail = f" · {elapsed:.1f}s" if AI_SHOW_LATENCY else ""
    lines = [
        f"🤖 *AI answer assist* (via {label}{tail})",
        "",
        f"Question: \"{question}\"",
        "",
        "Likely answers — *tap one* to submit it instantly:",
    ]
    lines += [f"{idx}. `{answer}`" for idx, answer in enumerate(answers, 1)]
    lines += [
        "",
        "_The AI is only a suggestion — type a different answer yourself at any "
        "time and it will be submitted instead._",
    ]
    await send_telegram(
        chat_id, "\n".join(lines), reply_markup=ai_answer_keyboard(code, answers)
    )


async def assist_security_question(chat_id, chat_str, code, question):
    """Background AI lookup for a live security question (never blocks a claim). Two passes, because one budget cannot serve both goals: * pass 1 (``AI_DEADLINE``) — fast enough to land beside the claim prompt; * pass 2 (``AI_LATE_DEADLINE``) — the second chance for a web-search grounded model, which routinely needs longer than a few seconds. The prompt stays open until it is answered, so a late suggestion is still useful; if the user has answered meanwhile the late result is dropped rather than posting a stale button. When both passes come back empty the chat is told why (unless ``AI_NOTIFY_FAILURE`` is off). Staying silent is what made a wired-up feature look like a broken one. """
    if not _ai_enabled():
        return
    question = str(question or "").strip()
    if not question:
        return

    provider, answers, elapsed = None, [], 0.0
    try:
        provider, answers, elapsed = await asyncio.wait_for(
            ai_answer_candidates(question), timeout=AI_DEADLINE + 2.0
        )
    except Exception as e:  # noqa: BLE001
        print(f"[ai] fast pass failed: {e}", flush=True)
    if answers:
        await _ai_post_suggestions(
            chat_id, chat_str, code, question, provider, answers, elapsed
        )
        return

    if AI_LATE_DEADLINE > AI_DEADLINE:
        try:
            provider, answers, elapsed = await asyncio.wait_for(
                ai_answer_candidates(question, late=True),
                timeout=AI_LATE_DEADLINE + 2.0,
            )
        except Exception as e:  # noqa: BLE001
            print(f"[ai] late pass failed: {e}", flush=True)
            provider, answers, elapsed = None, [], 0.0
        if answers:
            if _pending_question_open(chat_str, code):
                await _ai_post_suggestions(
                    chat_id, chat_str, code, question, provider, answers, elapsed
                )
            return

    if AI_NOTIFY_FAILURE and _pending_question_open(chat_str, code):
        await send_telegram(
            chat_id,
            "🤖 *AI answer assist* — no suggestion came back.\n\n"
            f"• Last lookup: {_ai_failure_note()}\n"
            f"• Budgets: fast `{AI_DEADLINE:g}s`, late `{AI_LATE_DEADLINE:g}s`.\n\n"
            "_The question is still open — answer it as usual._",
        )


# ---------------------------------------------------------------------------
# Telegram helpers
# ---------------------------------------------------------------------------
async def send_telegram(chat_id, text, reply_markup=None, parse_mode="Markdown"):
    """Send a message, paced through a shared token bucket. A multi-slot claim emits many reports in quick succession. Without pacing that burst trips Telegram flood control (HTTP 429) and messages are dropped silently. Here every send first takes a token, and a 429 is honoured using Telegram's ``retry_after`` before retrying, so no report is lost. Formatting errors still retry once with ``parse_mode`` removed. """
    if not chat_id or not TELEGRAM_BOT_TOKEN:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_markup:
        payload["reply_markup"] = reply_markup
    tg_pacer = _pacer_for("tg:" + str(chat_id), TG_RATE, TG_RATE_BURST)
    for attempt in range(3):
        await tg_pacer.acquire()
        try:
            res = await _TG_CLIENT.post(url, json=payload)
            if res.status_code == 200:
                return res.json().get("result", {}).get("message_id")
            if res.status_code == 429:
                wait = 1.0
                try:
                    wait = float(res.json().get("parameters", {}).get("retry_after", 1.0))
                except Exception:  # noqa: BLE001
                    pass
                if attempt < 2:
                    await asyncio.sleep(min(wait, 5.0) + 0.05)
                    continue
                print(f"[tg] flood control 429 persisting for {chat_id}", flush=True)
                return None
            body = res.text.lower()
            if attempt == 0 and ("parse" in body or "entit" in body):
                payload.pop("parse_mode", None)
                continue
            return None
        except Exception as e:  # noqa: BLE001
            if attempt < 2:
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
            {"text": "🩺 Accounts Health", "callback_data": "btn_health"},
            {"text": "🔐 Login (OTP)", "callback_data": "btn_login_help"},
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


def _retry_after_seconds(res, default=1.0):
    """Parse a Retry-After header (delta-seconds or HTTP-date); else `default`."""
    if res is None:
        return default
    raw = res.headers.get("Retry-After") or res.headers.get("retry-after")
    if not raw:
        return default
    raw = str(raw).strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(raw)
        if when is not None:
            if when.tzinfo is None:
                when = when.replace(tzinfo=datetime.timezone.utc)
            delta = (when - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            return max(0.0, delta)
    except Exception:  # noqa: BLE001
        pass
    return default


async def _raw_post(client, url, payload, headers, cookies):
    # Paced before it leaves the process, per chat: this request draws only on
    # its own chat's bucket so another user's fanout cannot slow it down.
    await _acquire_claim(_CLAIM_SCOPE.get())
    async with _REQUEST_SEM:
        res = await client.post(url, json=payload, headers=headers, cookies=cookies or None)
        if LOG_RAW_CLAIM:
            try:
                print(
                    f"[claim-raw] {res.status_code} {url} -> {res.text[:500]}",
                    flush=True,
                )
            except Exception:  # noqa: BLE001
                pass
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


async def claim_slot_worker(client, chat_id, slot_id, token_data, code, carts_db, lockouts_db, specific_answer=None, deadline=None, start_delay=0.0):
    """Claim `code` for one slot using burst/hedged requests. With no `specific_answer` the first request probes with an empty answer so the API reveals the security question (prompt mode). With a `specific_answer` the answer is submitted immediately. """
    chat_str = str(chat_id)
    slot_key = str(slot_id)

    # Bind this task's claim requests to this chat's pacer for the whole fanout
    # (slots and hedged sub-tasks inherit it when their tasks are created).
    _CLAIM_SCOPE.set(chat_str)

    if not is_slot_active_state(chat_str, slot_key):
        return {"slot": slot_key, "status": 0, "response": {"message": "skipped (inactive)"}}

    token = (token_data or {}).get("token")
    if not token:
        return {"slot": slot_key, "status": 0, "response": {"message": "skipped (no token)"}}

    # Feature 1: if this slot's token is at or near expiry, refresh it in the
    # BACKGROUND. The claim below still fires immediately with the current
    # token, so a refresh can never add latency to a claim - it only means the
    # next claim runs on a fresh token.
    if _token_needs_refresh(token_data):
        start_token_refresh(chat_str, slot_key, token_data)

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

    if start_delay and start_delay > 0:
        # Stagger the fleet so N slots do not all hit Chowdeck on the same tick.
        await asyncio.sleep(start_delay)

    last_res, last_json = None, {}
    rate_limited = False
    refreshed = False
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
            if res.status_code in (401, 403) and not refreshed and _refresh_allowed(token_data):
                # Reactive refresh: the token was rejected as unauthenticated.
                # Mint a replacement and retry this slot once, in place.
                refreshed = True
                new_token = await refresh_slot_token(chat_str, slot_key, token_data, force=True)
                if new_token:
                    token = new_token
                    token_data = {**token_data, "token": new_token}
                    headers = chow_headers(token)
                    continue
            msg = str(body.get("message", "")).lower()
            data_obj = body.get("data", {}) if isinstance(body.get("data"), dict) else {}
            sub_code = str(data_obj.get("code", ""))
            if res.status_code == 429:
                rate_limited = True
                wait = _retry_after_seconds(res)
                # Only this chat honours the cooldown; other users keep their
                # own pace (the optional global guard covers IP-wide limits).
                await _claim_backoff(_CLAIM_SCOPE.get(), wait)
                if wait >= 1.5:
                    # Upstream asked for a real cooldown; retrying inside this
                    # short claim window only deepens the throttle. Stop cleanly.
                    break
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
        "rate_limited": rate_limited,
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


# Money amounts (N2000, #5000, 20K) and clock times (5:50pm -> 50pm) are the
# two things that most often masquerade as a promo code in casual text.
_AMOUNT_RE = re.compile(r"^(?:[N\u20a6#]?\d+)(?:\.\d+)?[KM]?$", re.IGNORECASE)
_CLOCK_RE = re.compile(r"^\d{1,2}(?:AM|PM)$", re.IGNORECASE)


def _is_paste_code_token(raw):
    """True when a pasted token looks like something a human typed as a code. Voucher codes are always written in upper case. A pasted token qualifies when it is mixed (a digit AND a letter - CHOW5K, ENDEAVOR26, 5FOR5), when it was typed in ALL CAPS by the user (FIVEFORFIVE), or when it is a long alphabetic token that is clearly not ordinary chatter (a lower-case paste of a real code such as mrbeeistesting). Ordinary sentence words such as "grab" or "now" are rejected, so pasting "grab CHOW5K now" only claims CHOW5K instead of inventing GRABCHOW5KNOW. """
    if not raw or not any(c.isalpha() for c in raw):
        return False
    if _AMOUNT_RE.match(raw) or _CLOCK_RE.match(raw):
        return False
    has_digit = any(c.isdigit() for c in raw)
    if has_digit:
        return True
    if raw == raw.upper():
        return len(raw) >= 5
    # Lower-case, alphabetic only: accept only clearly code-sized tokens so a
    # hand-pasted lower-case code is still caught while "grab"/"now"/"please"
    # and similar chatter never claim.
    return len(raw) >= 10


def extract_candidate_codes(text, min_len=5, require_caps=False, paste_mode=False):
    """Extract promo-code candidates from free text. require_caps=True is used for X (Twitter) input only: Chowdeck posts its real codes in ALL CAPS (ENDEAVOR26, FIVEFORFIVE) while ordinary chatter is sentence-case ("Slight change of plans."), so a token must be typed in caps with at least one letter to qualify. paste_mode=True is used for Telegram pastes and OCR text: tokens are taken from the message individually (never the whole message as one code) and must be mixed or typed in ALL CAPS, so a lower-case code such as "chow5k" still works while random lower-case words never trigger a claim. One extra case is honoured for paste input: when the whole message is a single token, a *mixed-case* letters-only code (MrBee, Endeavor) is accepted even though it is short and has no digit, because people often retype a letters-only code in mixed case. Ordinary all-lowercase chatter ("hello") is still filtered, and a *sentence* such as "I am MRBEE of lagos" keeps the strict per-token rule so only the ALL-CAPS MRBEE is grabbed. """
    tokens = re.findall(r"[A-Za-z0-9]+", text or "")
    # A lone, mixed-case alphabetic token is an explicit code paste: accept it so
    # a letters-only code retyped as "MrBee" is never missed, while all-lowercase
    # words and multi-word sentences keep the stricter per-token gate below.
    solo_mixed = (
        paste_mode
        and not require_caps
        and len(tokens) == 1
        and tokens[0].isalpha()
        and tokens[0] != tokens[0].upper()
        and tokens[0] != tokens[0].lower()
    )
    out = []
    for raw in tokens:
        if require_caps and (not any(c.isalpha() for c in raw) or raw != raw.upper()):
            continue
        if paste_mode and not solo_mixed and not _is_paste_code_token(raw):
            continue
        if (require_caps or paste_mode) and (_AMOUNT_RE.match(raw) or _CLOCK_RE.match(raw)):
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
    """Stricter test used while a security prompt is open. Uses the same paste gate as the normal path, then additionally requires the winning token to carry a digit or to have been typed in ALL CAPS. That way a freshly pasted code still hijacks the claim (code priority), while a plain alphabetic word - a typical security answer - is passed to the answer handler instead of being mistaken for a code. """
    for raw in re.findall(r"[A-Za-z0-9]+", text or ""):
        if not _is_paste_code_token(raw):
            continue
        cleaned = robust_clean_code(raw)
        if not (5 <= len(cleaned) <= 24):
            continue
        if cleaned in _IGNORE_TOKENS or cleaned.startswith("INVITE") or cleaned.isdigit():
            continue
        if any(ch.isdigit() for ch in cleaned) or raw == raw.upper():
            return True
    return False


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
ACK_PROCESSING = "🔍 Processing personal code `{code}` across your active slots..."
ACK_PROCESSING_MIRROR = "🔗 *[Mirrored Admin Code]* Processing code `{code}` across secondary slots..."
ACK_ANSWER = (
    "⚡ *Emergency Answer Captured!* Blasting `{answer}` instantly across your "
    "active slots for `{code}`..."
)
ACK_ANSWER_MIRROR = (
    "🔗 *[Mirrored Admin Answer]* Blasting `{answer}` instantly across your "
    "secondary slots for `{code}`..."
)


def _safe_resp_text(resp_data):
    """JSON of a slot response, safe to embed inside a Markdown code span."""
    try:
        text = json.dumps(resp_data, ensure_ascii=False)
    except (TypeError, ValueError):
        text = str(resp_data)
    return text.replace("`", "'")


async def _claim_and_report(chat_str, code, *, label="Personal Code Target", mirror=False, ack_text=None, specific_answer=None, notify_empty=False, rearm_hint=None):
    """Fire `code` at every enabled slot and stream old-repo style messages. Each slot is reported on its own line so a slow, expired, inactive or congested slot can never hide another slot's result: * ``status == 0`` (inactive / expired / no token) is skipped silently and never blocks or hooks the rest of the claim. * a security question is surfaced immediately, grouped across every slot that asked for it, with a single paste-in-chat answer flow. * 500 / "high demand" congestion is collapsed into a single aggregated message after the burst retries complete, instead of one identical message per slot (which used to trip Telegram flood control). * The real response body is never masked: enable LOG_RAW_CLAIM to dump it. Returns ``{"ok": bool, "prompted": bool, "results": [...]}``. """
    chat_str = str(chat_str)
    chat_id = _to_int(chat_str)
    if chat_id is None:
        return {"ok": False, "prompted": False, "results": []}

    if ack_text:
        await send_telegram(chat_id, ack_text)

    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    lockouts_db = load_json(LOCKOUT_FILE)

    user_slots = tokens_db.get(chat_str, {}) or {}
    pending = [
        (str(sid), td) for sid, td in user_slots.items()
        if (td or {}).get("token") and is_slot_active_state(chat_str, str(sid))
    ]
    if not pending:
        if notify_empty and not mirror:
            await send_telegram(
                chat_id,
                "❌ No enabled accounts to claim with. Add a token with `/settoken <token>` "
                "and make sure the slot is ✅ enabled.",
            )
        return {"ok": False, "prompted": False, "results": []}

    head = "🔗 *[Mirrored]* " if mirror else ""
    deadline = time.time() + CLAIM_WINDOW_SECONDS
    tasks = [
        asyncio.create_task(
            claim_slot_worker(_CD_CLIENT, chat_id, sid, td, code, carts_db,
                              lockouts_db, specific_answer, deadline,
                              start_delay=idx * SLOT_STAGGER_SECONDS)
        )
        for idx, (sid, td) in enumerate(pending)
    ]

    collected = []
    prompt_slots = []
    congested_slots = []
    auth_failed_slots = []
    question = None
    state = {"sent": False}

    async def flush_prompt():
        """Send the grouped security question as soon as a slot reports one."""
        await asyncio.sleep(0.12)  # group near-simultaneous prompts from the same burst
        if state["sent"] or not prompt_slots:
            return
        state["sent"] = True
        slot_range_str = format_slot_range(prompt_slots)
        prompt_msg = (
            f"{head}🔒 *Security Question Detected for `{code}` ({slot_range_str})!*\n"
            f"Question: *\"{question}\"*\n\n"
            f"⚡ *Type the answer right here in chat* to respond!"
        )
        msg_id = await send_telegram(chat_id, prompt_msg)
        add_or_update_pending(chat_str, {
            "code": code,
            "question": question,
            "slots": list(prompt_slots),
            "ts": time.time(),
            "msg_id": msg_id,
        })
        # Feature 2: fire the AI lookup *after* the question is already on the
        # user's screen, in the background, so it can never delay the claim.
        if _ai_enabled() and question:
            spawn_bg(assist_security_question(chat_id, chat_str, code, str(question)))

    for fut in asyncio.as_completed(tasks):
        try:
            res = await fut
        except Exception as e:  # noqa: BLE001
            res = {"slot": "?", "status": 500, "response": {"message": str(e)}}
        if not isinstance(res, dict):
            continue
        status = res.get("status")
        if status == 0:
            continue  # inactive / expired / no token: never hook the claim
        slot = res.get("slot")
        collected.append({"slot": slot, "status": status,
                          "response": res.get("response", {})})

        if res.get("needs_prompt"):
            if question is None:
                question = str(res.get("question") or "Security question required.")
            if str(slot) not in prompt_slots:
                prompt_slots.append(str(slot))
            if not state["sent"]:
                asyncio.create_task(flush_prompt())
            continue

        resp_data = res.get("response", {})
        msg_lower = str(resp_data.get("message", "")).lower() if isinstance(resp_data, dict) else ""
        cart_suffix = res.get("cart_str", "")
        if status in (401, 403):
            # Token rejected as unauthenticated: report once, aggregated, and
            # point the user at the phone-login path instead of per-slot noise.
            auth_failed_slots.append(str(slot))
            continue
        if status in (429, 500, 502, 503, 504) or "high demand" in msg_lower:
            # Collapse every congested slot into a single report: N identical
            # "server congested" messages only add Telegram flood-control
            # pressure and bury the slots that returned a real verdict.
            congested_slots.append(str(slot))
            continue
        summary_msg = (
            f"{head}🎯 *{label}:* `{code}` (Slot {slot})\n"
            f"Status: `{status}`\n"
            f"Response: `{_safe_resp_text(resp_data)}`{cart_suffix}"
        )
        await send_telegram(chat_id, summary_msg)

    if congested_slots:
        slot_range_str = format_slot_range(congested_slots)
        await send_telegram(
            chat_id,
            f"{head}⚠️ *{label}:* `{code}` — congested: {slot_range_str}\n"
            f"Status: `429/5xx (Server congested)`\n"
            f"💡 Chowdeck throttled those slots; burst retries already completed. "
            f"Any slot that claimed successfully is reported above.",
        )

    if auth_failed_slots:
        slot_range_str = format_slot_range(auth_failed_slots)
        await send_telegram(
            chat_id,
            f"{head}🔑 *{label}:* `{code}` — not signed in: {slot_range_str}\n"
            f"💡 Those slots need a fresh session. Sign in with `/login <phone>` "
            f"(bot-owned token, auto-refreshed) or paste a new token with "
            f"`/settoken`.",
        )

    # A slow slot may have been the one holding the question: never leave it unshown.
    if prompt_slots and not state["sent"]:
        await flush_prompt()

    ok = any(r["status"] in (200, 201) for r in collected)
    if rearm_hint and not ok and not prompt_slots:
        # The answer was likely rejected: re-arm the question so a retry is one paste away.
        if any(
            r["status"] in (400, 401, 422)
            and any(w in str(r["response"].get("message", "")).lower()
                    for w in ("answer", "question", "incorrect", "wrong"))
            for r in collected
            if isinstance(r.get("response"), dict)
        ):
            add_or_update_pending(chat_str, {
                "code": code,
                "question": rearm_hint,
                "slots": [],
                "ts": time.time(),
            })

    return {"ok": ok, "prompted": bool(prompt_slots), "results": collected}


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
                mirror = str(c_str) != ADMIN_CHAT_ID
                try:
                    await _claim_and_report(
                        c_str, code,
                        label="Code Target",
                        mirror=mirror,
                        ack_text=(
                            ACK_PROCESSING_MIRROR.format(code=code) if mirror
                            else ACK_PROCESSING.format(code=code)
                        ),
                        notify_empty=not mirror,
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

    targets = [chat_str]
    if is_admin:
        for linked in get_linked_accounts(chat_str):
            if linked not in targets:
                targets.append(linked)

    for c_str in targets:
        mirror = c_str != chat_str
        await _claim_and_report(
            c_str, code,
            label="Personal Code Target",
            mirror=mirror,
            ack_text=(
                ACK_PROCESSING_MIRROR.format(code=code) if mirror
                else ACK_PROCESSING.format(code=code)
            ),
            notify_empty=not mirror,
        )


async def handle_answer(chat_str, answer_text, pending_entry):
    """Fire the user's pasted answer at their slots immediately, no storage."""
    chat_str = str(chat_str)
    answer_text = str(answer_text).strip()
    code = (pending_entry or {}).get("code")
    if not answer_text or not code:
        return

    is_admin = is_admin_chat(chat_str)

    targets = [chat_str]
    if is_admin:
        for linked in get_linked_accounts(chat_str):
            if linked not in targets:
                targets.append(linked)

    for c_str in targets:
        mirror = c_str != chat_str
        await _claim_and_report(
            c_str, code,
            label="Code Target",
            mirror=mirror,
            ack_text=(
                ACK_ANSWER_MIRROR.format(answer=answer_text, code=code) if mirror
                else ACK_ANSWER.format(answer=answer_text, code=code)
            ),
            specific_answer=answer_text,
            notify_empty=not mirror,
            rearm_hint=(pending_entry or {}).get("question") or "Security question required.",
        )


async def handle_ai_answer_callback(cq_id, chat_id, msg_id, chat_str, action):
    """Feature 2: the user tapped an AI suggestion - submit it as the answer. `action` is `aians_<idx>_<code>`. A suggestion is only ever a shortcut for typing the same text, so it is funnelled through the exact same handle_answer() path the typed flow uses. The question must still be live (same code) or the tap is ignored, so a stale button can never fire a claim against a question the user is not looking at. """
    chat_str = str(chat_str)
    parts = action.split("_", 2)
    if len(parts) < 3:
        await answer_callback_query(cq_id, "That suggestion expired.")
        return
    try:
        idx = int(parts[1])
    except (TypeError, ValueError):
        await answer_callback_query(cq_id, "That suggestion expired.")
        return
    code = parts[2]

    entry = next(
        (e for e in list_pending(chat_str) if (e or {}).get("code") == code),
        None,
    )
    answers = list(((entry or {}).get("ai") or {}).get("answers") or [])
    if not entry or idx < 0 or idx >= len(answers):
        await answer_callback_query(cq_id, "That question is no longer open.")
        return
    answer = str(answers[idx]).strip()
    if not answer:
        await answer_callback_query(cq_id, "Empty suggestion.")
        return

    # Consume the prompt exactly like a typed answer does, then fire it.
    entry = pop_pending(chat_str, code=code) or entry
    await answer_callback_query(cq_id, f"Submitting: {answer[:40]}")
    try:
        await edit_message_text(
            chat_id, msg_id,
            f"🤖 *AI answer assist*\n\n"
            f"Question: \"{entry.get('question', '')}\"\n\n"
            f"✅ Submitting: `{answer}`",
            reply_markup=ai_answer_keyboard(code, answers, submitted=idx),
        )
    except Exception:  # noqa: BLE001
        pass
    await handle_answer(chat_str, answer, entry)


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
    codes = extract_candidate_codes(text, min_len=4, paste_mode=True)
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
        f"• Mode: `PROMPT` — you supply the security answer on demand\n"
        f"• Phone login: `{'ON' if LOGIN_ENABLED else 'OFF'}` · "
        f"AI assist: `{'ON' if _ai_enabled() else 'OFF'}`\n\n"
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
    "• `/setcart <cart_id> [vendor_id]` — auto-apply won codes to a cart\n"
    "• `/accounts` — 🩺 live health of every slot (active / expired / empty)\n"
    "• `/refresh` — force a token refresh now\n\n"
    "*🔐 Sign in with a phone number (no more copy-pasting tokens)*\n"
    "• `/login <phone>` — the bot requests a Chowdeck code for that number\n"
    "• send the code back in the chat (or `/otp <code>`) — the bot signs in on "
    "its own and links the token to the active slot, which is then ✅ enabled\n"
    "• `/resendotp` — send a new code · `/cancelotp` — abort\n"
    "• The session is the bot's own, so it never disturbs a login on your "
    "browser, and it is refreshed automatically in the background.\n\n"
    "*🤖 AI answer assist (real-time web search)*\n"
    "• When a claim hits a security question, likely answers arrive with "
    "*tap-to-submit* buttons (Perplexity / OpenAI / Gemini / Tavily).\n"
    "• Two passes: a fast one (3s, lands with the prompt) and, when that is "
    "empty, a longer web-grounded pass (12s) — a late suggestion is still "
    "useful because the question stays open until you answer it.\n"
    "• If both passes come back empty the bot says so, quoting the provider's "
    "own reason, instead of staying silent.\n"
    "• `/ai` — show the AI status (on/off, providers, models, last lookup)\n"
    "• `/ai <question>` — look one up on demand (runs both passes)\n"
    "• It needs one provider key (`PERPLEXITY_API_KEY`, `OPENAI_API_KEY`, "
    "`GEMINI_API_KEY` or `TAVILY_API_KEY`); with no key it simply stays off.\n"
    "• It is only a suggestion: type your own answer any time and that is what "
    "gets submitted."
)

LOGIN_HELP_TEXT = (
    "🔐 *Sign in with a phone number*\n\n"
    "1️⃣ Send `/login <phone>` (e.g. `/login 08031234567`).\n"
    "2️⃣ Chowdeck texts you a code for the *active slot*.\n"
    "3️⃣ Send that code here (a bare 4- or 6-digit message is enough, or "
    "`/otp <code>`).\n"
    "4️⃣ The account is linked, the slot is ✅ enabled, and the token is "
    "refreshed automatically.\n\n"
    "*Why this is safe*: the bot performs its *own* sign-in, exactly like a "
    "second phone. You get an independent session token, so your browser login "
    "is never disturbed and a browser login never kicks the bot out.\n\n"
    "• `/resendotp` — new code · `/cancelotp` — abort\n"
    "• `/accounts` — check which linked accounts are still active"
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
    elif cmd in ("/login", "/loginotp"):
        await start_otp_login(chat_id, chat_str, arg)
    elif cmd == "/otp":
        if not arg:
            await send_telegram(chat_id, "⚠️ Usage: `/otp <code>`")
            return
        flow = get_login_flow(chat_str)
        if not flow:
            await send_telegram(chat_id, "ℹ️ No login is waiting for a code. Start one with `/login <phone>`.")
            return
        await complete_otp_login(chat_id, chat_str, arg.strip(), flow)
    elif cmd == "/resendotp":
        await resend_login_otp(chat_id, chat_str)
    elif cmd == "/cancelotp":
        clear_login_flow(chat_str)
        await send_telegram(chat_id, "🗑️ Login cancelled. Nothing was linked.")
    elif cmd in ("/accounts", "/health"):
        await send_telegram(chat_id, "🩺 Checking every slot against Chowdeck…")
        results = await check_all_slots(chat_str)
        await send_telegram(chat_id, format_accounts_health(results, chat_str))
    elif cmd == "/refresh":
        tokens_db = load_json(TOKENS_FILE)
        user_slots = tokens_db.get(chat_str, {}) or {}
        refreshed, failed, skipped = 0, 0, 0
        for sid, td in list(user_slots.items()):
            if not (td or {}).get("token"):
                continue
            if not _refresh_allowed(td):
                skipped += 1
                continue
            new_token = await refresh_slot_token(chat_str, sid, td, force=True)
            if new_token:
                refreshed += 1
            else:
                failed += 1
        note = ""
        if skipped and not refreshed:
            note = (
                "\nℹ️ `CHOW_AUTO_REFRESH=safe` only refreshes tokens minted by "
                "`/login`. Use `/login <phone>` for hands-free sessions."
            )
        tail = f" `{failed}` could not be refreshed." if failed else ""
        await send_telegram(chat_id, f"🔄 Refreshed `{refreshed}` slot token(s).{tail}{note}")
    elif cmd in ("/ai", "/ask"):
        if not _ai_enabled():
            await send_telegram(
                chat_id,
                "🤖 *AI assist is OFF* — no provider key is configured.\n\n"
                "Set one of `PERPLEXITY_API_KEY`, `OPENAI_API_KEY`, "
                "`GEMINI_API_KEY` or `TAVILY_API_KEY` on the deployment and it "
                "turns on by itself (`AI_ASSIST_ENABLED=auto`).\n\n"
                "Without a key the bot is otherwise unaffected: you still get the "
                "security question and answer it yourself.",
            )
            return
        if not arg:
            await send_telegram(chat_id, ai_status_text())
            return
        await send_telegram(chat_id, "🔎 Searching the web…")
        provider, answers, elapsed = await ai_answer_candidates(arg)
        if not answers and AI_LATE_DEADLINE > AI_DEADLINE:
            # Interactive lookup: the user is right here waiting, so give a
            # search-grounded model its longer budget before reporting nothing.
            await send_telegram(
                chat_id,
                "⏳ Still searching — a web-grounded model can take up to "
                f"`{AI_LATE_DEADLINE:g}s`…",
            )
            provider, answers, elapsed = await ai_answer_candidates(arg, late=True)
        if not answers:
            await send_telegram(
                chat_id,
                "🤖 *No suggestion came back.*\n\n"
                f"• Last lookup: {_ai_failure_note()}\n"
                f"• Budgets: fast `{AI_DEADLINE:g}s`, late `{AI_LATE_DEADLINE:g}s`.\n\n"
                "Answer manually as usual.",
            )
            return
        label = _AI_PROVIDER_LABELS.get(provider, provider)
        tail = f" · {elapsed:.1f}s" if AI_SHOW_LATENCY else ""
        lines = [f"🤖 *AI suggestions* (via {label}{tail})", ""]
        lines += [f"{i}. `{a}`" for i, a in enumerate(answers, 1)]
        await send_telegram(chat_id, "\n".join(lines))
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

    if action.startswith("aians_"):
        await handle_ai_answer_callback(cq_id, chat_id, msg_id, chat_str, action)
    elif action.startswith("slot_"):
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
    elif action == "btn_health":
        await answer_callback_query(cq_id, "Checking slots…")
        await send_telegram(chat_id, "🩺 Checking every slot against Chowdeck…")
        results = await check_all_slots(chat_str)
        await send_telegram(chat_id, format_accounts_health(results, chat_str))
    elif action == "btn_login_help":
        await answer_callback_query(cq_id, "Phone login")
        await send_telegram(chat_id, LOGIN_HELP_TEXT)
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

    # Feature 1: while an OTP login is open, a bare 4/6-digit message completes
    # the sign-in. A reply to a security-question prompt is an answer, not an OTP.
    if await maybe_handle_login_otp(chat_id, chat_str, text, reply_to, pending):
        return

    if pending:
        if reply_to is not None and any(e.get("msg_id") == reply_to for e in pending):
            entry = pop_pending(chat_str, reply_to=reply_to)
            await handle_answer(chat_str, text, entry)
            return
        if strong_code_signal(text):
            # A fresh code arrived while a question is open: claim it. The
            # "Processing personal code ..." acknowledgement comes from the
            # claim path, and the pending question stays open for one paste.
            await process_text_isolated(chat_str, text)
            return
        entry = pop_pending(chat_str)
        await handle_answer(chat_str, text, entry)
        return

    if extract_candidate_codes(text, min_len=4, paste_mode=True):
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
        # Background helpers (token refresh, AI lookups) are ours to stop.
        for task in list(_bg_tasks) + list(_refresh_tasks.values()):
            task.cancel()
        flush_store()
        try:
            await _CD_CLIENT.aclose()
            await _TG_CLIENT.aclose()
            await _AI_CLIENT.aclose()
            await _AI_CLIENT_LATE.aclose()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped.", flush=True)
