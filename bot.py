import re
import asyncio
import os
import json
import unicodedata
import urllib.parse
import threading
import base64
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
import httpx
import time

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()

GETXAPI_KEYS_RAW = os.getenv("GETXAPI_KEYS", "").strip()
GETXAPI_KEYS = [k.strip() for k in GETXAPI_KEYS_RAW.split(",") if k.strip()]
getxapi_index = 0
getxapi_lock = threading.Lock()

DATA_DIR = os.getenv("RAILWAY_VOLUME_MOUNT_PATH", os.path.dirname(os.path.abspath(__file__)))

TOKENS_FILE = os.path.join(DATA_DIR, "tokens_multi.json")
CARTS_FILE = os.path.join(DATA_DIR, "carts_multi.json")
ANSWER_FILE = os.path.join(DATA_DIR, "answers_multi.json")
MODE_FILE = os.path.join(DATA_DIR, "bot_modes.json")
CLAIMED_FILE = os.path.join(DATA_DIR, "claimed.json")
STATE_FILE = os.path.join(DATA_DIR, "bot_state.json")
LOCKOUT_FILE = os.path.join(DATA_DIR, "lockouts_multi.json")
ACTIVE_SLOT_FILE = os.path.join(DATA_DIR, "active_slots.json")
PENDING_PROMPT_FILE = os.path.join(DATA_DIR, "pending_prompts.json")
ACCOUNT_TOGGLES_FILE = os.path.join(DATA_DIR, "account_toggles.json")
BURNER_FILE = os.path.join(DATA_DIR, "admin_burner.json")
BURNER_CART_FILE = os.path.join(DATA_DIR, "admin_burner_cart.json")
PORTFOLIO_FILE = os.path.join(DATA_DIR, "portfolio.json")
LINKED_ACCOUNTS_FILE = os.path.join(DATA_DIR, "linked_accounts.json")

last_update_id = 0
code_queue = asyncio.Queue()
loop_ref = None

def load_json(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Error reading {file_path}: {e}", flush=True)
            return {}
    return {}

def save_json(file_path, data):
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"❌ Failed to save {file_path}: {e}", flush=True)

def get_bot_active():
    state = load_json(STATE_FILE)
    return state.get("active", True)

def set_bot_active(val):
    save_json(STATE_FILE, {"active": val})

def get_user_mode(chat_str):
    modes = load_json(MODE_FILE)
    return modes.get(chat_str, "auto")

def set_user_mode(chat_str, mode):
    modes = load_json(MODE_FILE)
    modes[chat_str] = mode
    save_json(MODE_FILE, modes)

def get_max_slots(chat_str):
    return 20 if (ADMIN_CHAT_ID and chat_str == ADMIN_CHAT_ID) else 10

def get_active_slot(chat_str):
    slots = load_json(ACTIVE_SLOT_FILE)
    return str(slots.get(chat_str, "1"))

def set_active_slot(chat_str, slot):
    slots = load_json(ACTIVE_SLOT_FILE)
    slots[chat_str] = str(slot)
    save_json(ACTIVE_SLOT_FILE, slots)

def is_slot_active_state(chat_str, slot_id):
    toggles = load_json(ACCOUNT_TOGGLES_FILE)
    user_toggles = toggles.get(chat_str, {})
    return user_toggles.get(str(slot_id), True)

def toggle_slot_state(chat_str, slot_id):
    toggles = load_json(ACCOUNT_TOGGLES_FILE)
    if chat_str not in toggles:
        toggles[chat_str] = {}
    current = toggles[chat_str].get(str(slot_id), True)
    toggles[chat_str][str(slot_id)] = not current
    save_json(ACCOUNT_TOGGLES_FILE, toggles)
    return not current

def get_burner_credentials(chat_str):
    burners = load_json(BURNER_FILE)
    return burners.get(chat_str, {})

def save_burner_credentials(chat_str, token_data):
    burners = load_json(BURNER_FILE)
    burners[chat_str] = token_data
    save_json(BURNER_FILE, burners)

def get_burner_cart(chat_str):
    carts = load_json(BURNER_CART_FILE)
    return carts.get(chat_str, {})

def save_burner_cart(chat_str, cart_data):
    carts = load_json(BURNER_CART_FILE)
    carts[chat_str] = cart_data
    save_json(BURNER_CART_FILE, carts)

def get_linked_accounts(chat_str):
    linked = load_json(LINKED_ACCOUNTS_FILE)
    return linked.get(chat_str, [])

def add_linked_account(chat_str, secondary_chat_id):
    linked = load_json(LINKED_ACCOUNTS_FILE)
    if chat_str not in linked:
        linked[chat_str] = []
    sec_str = str(secondary_chat_id).strip()
    if sec_str not in linked[chat_str]:
        linked[chat_str].append(sec_str)
        save_json(LINKED_ACCOUNTS_FILE, linked)

def remove_linked_account(chat_str, secondary_chat_id):
    linked = load_json(LINKED_ACCOUNTS_FILE)
    if chat_str in linked:
        sec_str = str(secondary_chat_id).strip()
        if sec_str in linked[chat_str]:
            linked[chat_str].remove(sec_str)
            save_json(LINKED_ACCOUNTS_FILE, linked)

def get_admin_stats():
    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    total_users = len(tokens_db)
    total_slots_configured = sum(len(slots) for slots in tokens_db.values())
    total_carts_configured = sum(len(carts) for carts in carts_db.values())
    return total_users, total_slots_configured, total_carts_configured

def record_portfolio_win(chat_str, code, slot_id, amount=2000):
    portfolio = load_json(PORTFOLIO_FILE)
    if chat_str not in portfolio:
        portfolio[chat_str] = {"items": [], "total_value": 0}
    
    already_logged = any(item["code"] == code and str(item.get("slot")) == str(slot_id) for item in portfolio[chat_str]["items"])
    if not already_logged:
        portfolio[chat_str]["items"].append({
            "code": code,
            "slot": str(slot_id),
            "amount": amount,
            "timestamp": time.time()
        })
        portfolio[chat_str]["total_value"] = sum(item.get("amount", 2000) for item in portfolio[chat_str]["items"])
        save_json(PORTFOLIO_FILE, portfolio)

def parse_jwt_payload(token):
    try:
        parts = token.split(".")
        if len(parts) >= 2:
            payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
            decoded_bytes = base64.urlsafe_b64decode(payload_b64)
            return json.loads(decoded_bytes.decode('utf-8'))
    except Exception:
        pass
    return {}

def extract_phone_from_data(data):
    if not isinstance(data, dict):
        return None
    return (
        data.get("phone_number") or 
        data.get("phone") or 
        data.get("phoneNumber") or 
        data.get("mobile") or 
        data.get("telephone") or 
        data.get("user_phone") or
        data.get("user", {}).get("phone_number") or
        data.get("user", {}).get("phone")
    )

def parse_and_store_credentials(raw_input, chat_str, slot):
    cleaned = raw_input.strip()
    extracted_token = cleaned
    extracted_cookies = {}
    extracted_phone = None

    if "user=" in cleaned or ";" in cleaned:
        try:
            parts = cleaned.split(";")
            for p in parts:
                p = p.strip()
                if "=" in p:
                    k, v = p.split("=", 1)
                    extracted_cookies[k.strip()] = v.strip()
                    if k.strip() == "user":
                        decoded = urllib.parse.unquote(v.strip())
                        data = json.loads(decoded)
                        if "token" in data:
                            extracted_token = data["token"]
                        extracted_phone = extract_phone_from_data(data)
        except Exception as e:
            print(f"Cookie parse error: {e}", flush=True)

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
        jwt_data = parse_jwt_payload(extracted_token)
        extracted_phone = extract_phone_from_data(jwt_data)

    tokens_db = load_json(TOKENS_FILE)
    if chat_str not in tokens_db:
        tokens_db[chat_str] = {}
    
    existing_phone = tokens_db[chat_str].get(slot, {}).get("phone")
    final_phone = extracted_phone if extracted_phone else existing_phone

    tokens_db[chat_str][slot] = {
        "token": extracted_token,
        "cookies": extracted_cookies,
        "phone": str(final_phone)[:15] if final_phone else None
    }
    save_json(TOKENS_FILE, tokens_db)
    return extracted_token

async def fetch_account_phone(token, cookies):
    if token and token.count(".") == 2:
        jwt_data = parse_jwt_payload(token)
        phone = extract_phone_from_data(jwt_data)
        if phone:
            return phone

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": "https://chowdeck.com",
        "Referer": "https://chowdeck.com/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    profile_url = "https://api.chowdeck.com/customer/profile"
    try:
        async with httpx.AsyncClient(timeout=7.0, proxy=PROXY_URL if PROXY_URL else None) as client:
            res = await client.get(profile_url, headers=headers, cookies=cookies)
            if res.status_code == 200:
                res_json = res.json()
                data = res_json.get("data", {})
                if not isinstance(data, dict):
                    data = res_json
                phone = extract_phone_from_data(data)
                return phone
    except Exception as e:
        print(f"Profile fetch error: {e}", flush=True)
    return None

def robust_clean_code(raw):
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", str(raw))
    cleaned = "".join(c for c in normalized if c.isalnum()).upper()
    return cleaned

async def send_telegram(chat_id, text, reply_markup=None):
    if not chat_id:
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with httpx.AsyncClient(proxy=PROXY_URL if PROXY_URL else None) as temp_client:
            res = await temp_client.post(url, json=payload, timeout=7.0)
            return res.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"Telegram send error to {chat_id}: {e}", flush=True)
        return None

async def broadcast_to_channel(text):
    if not TELEGRAM_CHANNEL_ID:
        return
    await send_telegram(TELEGRAM_CHANNEL_ID, text)

async def answer_callback_query(callback_query_id, text=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        async with httpx.AsyncClient(proxy=PROXY_URL if PROXY_URL else None) as temp_client:
            await temp_client.post(url, json={"callback_query_id": callback_query_id, "text": text}, timeout=5.0)
    except Exception as e:
        print(f"Callback answer error: {e}", flush=True)

def build_control_keyboard(chat_str, is_admin):
    active_slot = get_active_slot(chat_str)
    current_mode = get_user_mode(chat_str).upper()
    max_slots = get_max_slots(chat_str)
    
    slot_row_1 = []
    slot_row_2 = []
    slot_row_3 = []
    slot_row_4 = []
    
    for i in range(1, max_slots + 1):
        s_str = str(i)
        is_active = is_slot_active_state(chat_str, s_str)
        icon = "✅" if is_active else "❌"
        label = f"📌 S{i}" if s_str == active_slot else f"{icon} {i}"
        
        btn = {"text": label, "callback_data": f"slot_{i}"}
        if i <= 5:
            slot_row_1.append(btn)
        elif i <= 10:
            slot_row_2.append(btn)
        elif i <= 15:
            slot_row_3.append(btn)
        else:
            slot_row_4.append(btn)
        
    keyboard = [slot_row_1, slot_row_2]
    if slot_row_3:
        keyboard.append(slot_row_3)
    if slot_row_4:
        keyboard.append(slot_row_4)
        
    keyboard.extend([
        [
            {"text": "👥 My Accounts", "callback_data": "btn_my_accounts"},
            {"text": "💰 Portfolio", "callback_data": "btn_portfolio"}
        ],
        [
            {"text": f"⚙️ Mode: {current_mode}", "callback_data": "btn_toggle_mode"},
            {"text": "🔄 Refresh", "callback_data": "btn_status"}
        ],
        [
            {"text": "📖 Guide / Help", "callback_data": "btn_guide"}
        ]
    ])
    
    if is_admin:
        keyboard.insert(len(keyboard) - 1, [{"text": "🛡️ Admin Stats", "callback_data": "btn_admin_stats"}, {"text": "🧪 Burner Panel", "callback_data": "btn_burner_menu"}])
        keyboard.append([
            {"text": "⏸️ Pause", "callback_data": "btn_pause"},
            {"text": "▶️ Resume", "callback_data": "btn_resume"}
        ])
    return {"inline_keyboard": keyboard}

def build_accounts_keyboard(chat_str):
    tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
    max_slots = get_max_slots(chat_str)
    rows = []
    row = []
    for i in range(1, max_slots + 1):
        s_str = str(i)
        active = is_slot_active_state(chat_str, s_str)
        status_icon = "✅" if active else "❌"
        
        slot_info = tokens_db.get(s_str, {})
        phone_label = slot_info.get("phone")
        
        if phone_label:
            label = f"{status_icon} S{i}: {phone_label}"
        else:
            label = f"{status_icon} S{i} (Empty)"
            
        row.append({"text": label, "callback_data": f"toggle_acc_{i}"})
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([{"text": "🔄 Refresh Phones", "callback_data": "btn_refresh_phones"}, {"text": "🔙 Main Menu", "callback_data": "btn_main_menu"}])
    return {"inline_keyboard": rows}

def build_admin_burner_keyboard():
    return {"inline_keyboard": [
        [{"text": "🧪 Test Code (Send via /test)", "callback_data": "burner_info"}],
        [{"text": "🔙 Back to Main Menu", "callback_data": "btn_main_menu"}]
    ]}

async def execute_burner_test(chat_id, code):
    chat_str = str(chat_id)
    token_data = get_burner_credentials(chat_str)
    if not token_data or not token_data.get("token"):
        return "❌ Burner account token not configured! Use `/setburnertoken` first."

    token = token_data.get("token")
    user_cookies = token_data.get("cookies", {})
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": "https://chowdeck.com",
        "Referer": "https://chowdeck.com/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Accept": "application/json, text/plain, */*"
    }
    
    claim_url = f"https://api.chowdeck.com/customer/promo-code/{code}"
    payload_data = {"code": code, "answer": "Ismail", "security_answer": "Ismail"}
    
    try:
        async with httpx.AsyncClient(timeout=7.0, proxy=PROXY_URL if PROXY_URL else None) as client:
            claim_res = await client.post(claim_url, json=payload_data, headers=headers, cookies=user_cookies)
            res_json = {}
            try:
                res_json = claim_res.json()
            except Exception:
                pass

            cart_info = get_burner_cart(chat_str)
            cart_id = cart_info.get("cart_id")
            vendor_id = cart_info.get("vendor_id")
            
            cart_status_str = ""
            if cart_id and claim_res.status_code == 200:
                apply_payload = {"cart_id": cart_id, "promo_codes": [code]}
                if vendor_id:
                    apply_payload["vendor_id"] = vendor_id
                apply_res = await client.post("https://api.chowdeck.com/customer/discount/apply", json=apply_payload, headers=headers, cookies=user_cookies)
                cart_status_str = f"\nCart Apply Status: `{apply_res.status_code}` | Response: `{apply_res.text}`"

        return (
            f"🧪 *Burner Test Result for `{code}`*\n"
            f"Status Code: `{claim_res.status_code}`\n"
            f"API Response: `{json.dumps(res_json)}`{cart_status_str}"
        )
    except Exception as e:
        return f"❌ Burner test error: {e}"

async def claim_slot_worker(client, chat_id, slot_id, token_data, code, carts_db, answers_db, lockouts_db, specific_answer=None):
    chat_str = str(chat_id)
    slot_key = str(slot_id)
    
    if not is_slot_active_state(chat_str, slot_key):
        return {"slot": slot_key, "status": 0, "message": "skipped (inactive)"}

    lockout_key = f"{chat_str}_{slot_key}"
    if time.time() < lockouts_db.get(lockout_key, 0) and not specific_answer:
        return {"slot": slot_key, "status": 429, "message": "locked out"}

    token = token_data.get("token")
    user_cookies = token_data.get("cookies", {})
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Origin": "https://chowdeck.com",
        "Referer": "https://chowdeck.com/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty"
    }
    
    claim_url = f"https://api.chowdeck.com/customer/promo-code/{code}"
    user_mode = get_user_mode(chat_str)
    
    max_retries = 3
    claim_res = None
    res_json = {}

    for attempt in range(max_retries + 1):
        try:
            master_ans_list = answers_db.get(chat_str, {}).get("master", ["Ismail"])
            master_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"

            if user_mode == "prompt" and not specific_answer:
                probe_payload = {"code": code, "answer": "", "security_answer": ""}
                claim_res = await client.post(claim_url, json=probe_payload, headers=headers, cookies=user_cookies)
            else:
                ans_to_use = specific_answer if specific_answer else master_ans
                payload_data = {"code": code, "answer": ans_to_use, "security_answer": ans_to_use}
                claim_res = await client.post(claim_url, json=payload_data, headers=headers, cookies=user_cookies)

            try:
                res_json = claim_res.json()
            except Exception:
                res_json = {}

            msg_text = res_json.get("message", "").lower()
            data_obj = res_json.get("data", {})
            sub_code = data_obj.get("code", "") if isinstance(data_obj, dict) else ""

            is_busy = (
                claim_res.status_code in [429, 500, 503] or 
                sub_code == "claim_busy" or 
                "high demand" in msg_text or 
                "try again" in msg_text
            )

            if is_busy and attempt < max_retries:
                jitter = random.uniform(0.04, 0.12)
                await asyncio.sleep(jitter)
                continue
            break
        except (httpx.RequestError, httpx.TimeoutException):
            if attempt < max_retries:
                await asyncio.sleep(0.05)
                continue
            return {"slot": slot_key, "status": 500, "response": {"message": "Network/Timeout error"}}
        except Exception as e:
            return {"slot": slot_key, "status": 500, "response": {"message": str(e)}}

    if not claim_res:
        return {"slot": slot_key, "status": 500, "response": {"message": "Request failed"}}

    try:
        msg_text = res_json.get("message", "").lower()
        data_obj = res_json.get("data", {})
        
        q_text = None
        if isinstance(data_obj, dict):
            q_text = data_obj.get("security_question") or data_obj.get("question")
        if not q_text and ("security question" in msg_text or "incorrect" in msg_text or "not correct" in msg_text or "question" in msg_text):
            q_text = res_json.get("message")

        if user_mode == "prompt" and not specific_answer:
            if q_text or (claim_res.status_code in [400, 422] and not ("does not exist" in msg_text or "invalid" in msg_text or "expired" in msg_text or "used" in msg_text or "high demand" in msg_text)):
                if not q_text:
                    q_text = res_json.get("message") if res_json.get("message") else "Security question required for this code."
                return {
                    "slot": slot_key,
                    "status": claim_res.status_code,
                    "needs_prompt": True,
                    "question": q_text,
                    "response": res_json
                }

        if not specific_answer:
            if "too many failed" in msg_text:
                lockouts_db[lockout_key] = time.time() + 900
                save_json(LOCKOUT_FILE, lockouts_db)

        cart_info = carts_db.get(chat_str, {}).get(slot_key, {})
        cart_id = cart_info.get("cart_id")
        vendor_id = cart_info.get("vendor_id")
        
        cart_status_str = ""
        if cart_id and claim_res.status_code == 200:
            apply_payload = {
                "cart_id": cart_id,
                "promo_codes": [code]
            }
            if vendor_id:
                apply_payload["vendor_id"] = vendor_id
                
            apply_res = await client.post("https://api.chowdeck.com/customer/discount/apply", json=apply_payload, headers=headers, cookies=user_cookies)
            cart_status_str = f"\nCart Apply Status: `{apply_res.status_code}` | Response: `{apply_res.text}`"

        if claim_res.status_code in [200, 201]:
            record_portfolio_win(chat_str, code, slot_key, amount=2000)

        return {
            "slot": slot_key,
            "status": claim_res.status_code,
            "response": res_json,
            "cart_str": cart_status_str,
            "needs_prompt": False
        }
    except Exception as e:
        return {"slot": slot_key, "status": 500, "response": {"message": str(e)}}

def format_slot_range(slots):
    slots = sorted(list(set(int(s) for s in slots)))
    if not slots:
        return ""
    if len(slots) == 1:
        return f"Slot {slots[0]}"
    if slots == list(range(slots[0], slots[-1] + 1)):
        return f"Slots {slots[0]} to {slots[-1]}"
    return ", ".join(map(str, slots))

async def claim_code_global(raw_code):
    code = robust_clean_code(raw_code)
    if not code or len(code) < 4:
        return

    claimed_history = load_json(CLAIMED_FILE)
    if code in claimed_history:
        return

    await broadcast_to_channel(
        f"🚨 *New Chowdeck Code Detected!*\n\n"
        f"Code: `{code}`\n"
        f"⚡ *Rush to your bot now before it flies away!*"
    )

    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    answers_db = load_json(ANSWER_FILE)
    lockouts_db = load_json(LOCKOUT_FILE)
    
    if not tokens_db:
        return

    all_tasks = []
    task_meta = []

    limits = httpx.Limits(max_keepalive_connections=1000, max_connections=2000)
    async with httpx.AsyncClient(timeout=7.0, proxy=PROXY_URL if PROXY_URL else None, limits=limits) as client:
        for chat_str, user_slots in tokens_db.items():
            if not user_slots:
                continue
            for slot_id, t_data in user_slots.items():
                all_tasks.append(
                    claim_slot_worker(
                        client=client,
                        chat_id=int(chat_str), 
                        slot_id=slot_id, 
                        token_data=t_data, 
                        code=code, 
                        carts_db=carts_db, 
                        answers_db=answers_db, 
                        lockouts_db=lockouts_db
                    )
                )
                task_meta.append((chat_str, slot_id))

        if not all_tasks:
            return

        results = await asyncio.gather(*all_tasks)

    user_prompts = {}
    telegram_tasks = []
    
    for (chat_str, slot), res in zip(task_meta, results):
        if res.get("status") == 0:
            continue
            
        status = res["status"]
        resp_data = res.get("response", {})
        cart_suffix = res.get("cart_str", "")
        
        if res.get("needs_prompt"):
            q_text = res["question"]
            pending = load_json(PENDING_PROMPT_FILE)
            pending[chat_str] = {"code": code, "slot": slot}
            save_json(PENDING_PROMPT_FILE, pending)
            
            if chat_str not in user_prompts:
                user_prompts[chat_str] = {"slots": [], "question": q_text}
            user_prompts[chat_str]["slots"].append(slot)
        else:
            if status == 500:
                summary_msg = (
                    f"⚠️ *Code Target:* `{code}` (Slot {slot})\n"
                    f"Status: `500 (Internal Server Error)`\n"
                    f"💡 *Note:* Chowdeck returned a 500 error, check your app account!"
                )
            else:
                summary_msg = f"🎯 *Code Target:* `{code}` (Slot {slot})\nStatus: `{status}`\nResponse: `{json.dumps(resp_data)}`{cart_suffix}"
            
            telegram_tasks.append(send_telegram(chat_id=int(chat_str), text=summary_msg))

    for chat_str, p_data in user_prompts.items():
        slot_range_str = format_slot_range(p_data["slots"])
        q_text = p_data["question"]
        prompt_msg = (
            f"🔒 *Security Question Detected for `{code}` ({slot_range_str})!*\n"
            f"Question: *\"{q_text}\"*\n\n"
            f"⚡ *Type the answer right here in chat* to respond!"
        )
        telegram_tasks.append(send_telegram(chat_id=int(chat_str), text=prompt_msg))

    if telegram_tasks:
        await asyncio.gather(*telegram_tasks)

async def claim_code_isolated(chat_str, raw_code):
    code = robust_clean_code(raw_code)
    if not code or len(code) < 4:
        return

    is_admin = (not ADMIN_CHAT_ID or chat_str == ADMIN_CHAT_ID)

    if is_admin:
        await broadcast_to_channel(
            f"🚨 *New Chowdeck Code Detected!*\n\n"
            f"Code: `{code}`\n"
            f"⚡ *Rush to your bot now before it flies away!*"
        )

    # Determine all chat IDs to trigger (Admin + Linked secondary accounts if admin)
    target_chats = [chat_str]
    if is_admin:
        linked = get_linked_accounts(chat_str)
        for l_chat in linked:
            if l_chat not in target_chats:
                target_chats.append(l_chat)

    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    answers_db = load_json(ANSWER_FILE)
    lockouts_db = load_json(LOCKOUT_FILE)

    limits = httpx.Limits(max_keepalive_connections=500, max_connections=1000)
    async with httpx.AsyncClient(timeout=7.0, proxy=PROXY_URL if PROXY_URL else None, limits=limits) as client:
        for c_str in target_chats:
            user_slots = tokens_db.get(c_str, {})
            if not user_slots:
                if c_str == chat_str:
                    await send_telegram(int(chat_str), "❌ You have no accounts configured to claim this code.")
                continue

            if c_str == chat_str:
                await send_telegram(int(chat_str), f"🔍 Processing personal code `{code}` across your active slots...")
            else:
                await send_telegram(int(c_str), f"🔗 *[Mirrored Admin Code]* Processing code `{code}` across secondary slots...")

            tasks = [
                claim_slot_worker(client=client, chat_id=int(c_str), slot_id=slot_id, token_data=t_data, code=code, 
                                  carts_db=carts_db, answers_db=answers_db, lockouts_db=lockouts_db)
                for slot_id, t_data in user_slots.items()
            ]
            results = await asyncio.gather(*tasks)

            prompt_slots = []
            prompt_q = None
            telegram_tasks = []

            for res in results:
                slot = res["slot"]
                if res.get("status") == 0:
                    continue
                status = res["status"]
                resp_data = res.get("response", {})
                cart_suffix = res.get("cart_str", "")

                if res.get("needs_prompt"):
                    prompt_q = res["question"]
                    prompt_slots.append(slot)
                    pending = load_json(PENDING_PROMPT_FILE)
                    pending[c_str] = {"code": code, "slot": slot}
                    save_json(PENDING_PROMPT_FILE, pending)
                else:
                    if status == 500:
                        summary_msg = (
                            f"⚠️ *Personal Code Target:* `{code}` (Slot {slot})\n"
                            f"Status: `500 (Internal Server Error)`\n"
                            f"💡 *Note:* Chowdeck returned a 500 error, check your app account!"
                        )
                    else:
                        summary_msg = f"🎯 *Personal Code Target:* `{code}` (Slot {slot})\nStatus: `{status}`\nResponse: `{json.dumps(resp_data)}`{cart_suffix}"
                    telegram_tasks.append(send_telegram(chat_id=int(c_str), text=summary_msg))

            if prompt_q:
                slot_range_str = format_slot_range(prompt_slots)
                prompt_msg = (
                    f"🔒 *Security Question Detected for `{code}` ({slot_range_str})!*\n"
                    f"Question: *\"{prompt_q}\"*\n\n"
                    f"⚡ *Type the answer right here in chat* to respond!"
                )
                telegram_tasks.append(send_telegram(chat_id=int(c_str), text=prompt_msg))

            if telegram_tasks:
                await asyncio.gather(*telegram_tasks)

async def worker():
    while True:
        try:
            item = await code_queue.get()
            try:
                if isinstance(item, tuple):
                    source_type, data = item
                    if source_type == "global":
                        await claim_code_global(data)
                    elif source_type == "isolated":
                        chat_str, code = data
                        await claim_code_isolated(chat_str, code)
                else:
                    await claim_code_global(item)
            except Exception as e:
                print(f"Worker error: {e}", flush=True)
            finally:
                code_queue.task_done()
                await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await asyncio.sleep(1)

async def process_text_global(text):
    if not text:
        return
    pattern = r'\b(?=[A-Z0-9]*[A-Z])[A-Z0-9]{5,20}\b'
    codes = re.findall(pattern, text)
    ignore = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    
    valid = []
    for c in codes:
        cleaned = robust_clean_code(c)
        if cleaned and cleaned not in ignore and not cleaned.startswith("TEST"):
            valid.append(cleaned)
            
    for code in list(dict.fromkeys(valid)):
        await code_queue.put(("global", code))

async def process_text_isolated(chat_str, text):
    if not text:
        return
    pattern = r'\b(?=[A-Z0-9]*[A-Z])[A-Z0-9]{5,20}\b'
    codes = re.findall(pattern, text)
    ignore = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    
    valid = []
    for c in codes:
        cleaned = robust_clean_code(c)
        if cleaned and cleaned not in ignore:
            valid.append(cleaned)
            
    for code in list(dict.fromkeys(valid)):
        await code_queue.put(("isolated", (chat_str, code)))

class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/status" or self.path == "/":
            try:
                active = get_bot_active()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"status": "online", "active": active}).encode('utf-8'))
            except Exception:
                self.send_response(500)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        content_length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_length).decode('utf-8')
        try:
            data = json.loads(body)
            text = data.get("text", "")
            if text and loop_ref:
                asyncio.run_coroutine_threadsafe(process_text_global(text), loop_ref)
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        except Exception:
            self.send_response(400)
            self.end_headers()

    def log_message(self, format, *args):
        return

def run_http_server():
    server = HTTPServer(('0.0.0.0', 8080), WebhookHandler)
    print("🌐 Railway HTTP webhook server running on port 8080...", flush=True)
    server.serve_forever()

async def telegram_poller():
    global last_update_id
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    async with httpx.AsyncClient(proxy=PROXY_URL if PROXY_URL else None) as tg_client:
        while True:
            try:
                res = await tg_client.get(url, params={"offset": last_update_id + 1, "timeout": 25}, timeout=30.0)
                if res.status_code == 200:
                    for update in res.json().get("result", []):
                        last_update_id = update["update_id"]
                        
                        if "callback_query" in update:
                            cq = update["callback_query"]
                            cq_id = cq["id"]
                            chat_id = cq["message"]["chat"]["id"]
                            msg_id = cq["message"]["message_id"]
                            data_action = cq["data"]
                            chat_str = str(chat_id)
                            is_admin = (not ADMIN_CHAT_ID or chat_str == ADMIN_CHAT_ID)
                            max_slots = get_max_slots(chat_str)
                            
                            if data_action.startswith("slot_"):
                                await answer_callback_query(cq_id, f"Switched slots")
                                slot_num = data_action.split("_")[1]
                                set_active_slot(chat_str, slot_num)
                                
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                user_m = get_user_mode(chat_str)
                                master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                                m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                                updated_text = (
                                    f"🤖 *Chowdeck Multi-Account Sniper Active (1-{max_slots})*\n\n"
                                    f"• Active Slot: `Slot {slot_num}`\n"
                                    f"• Configured Slots: `{len(tokens_db)} / {max_slots}`\n"
                                    f"• Mode: `{user_m.upper()}`\n"
                                    f"• Master Answer: `{m_ans}`\n\n"
                                    "Select an option below:"
                                )
                                url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                await tg_client.post(url_edit, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": updated_text,
                                    "parse_mode": "Markdown",
                                    "reply_markup": build_control_keyboard(chat_str, is_admin)
                                })
                            elif data_action.startswith("toggle_acc_"):
                                await answer_callback_query(cq_id, f"Toggled account")
                                s_num = data_action.split("_")[2]
                                toggle_slot_state(chat_str, s_num)
                                
                                active_count = sum(1 for i in range(1, max_slots + 1) if is_slot_active_state(chat_str, str(i)))
                                acc_text = (
                                    f"💼 *My Chowdeck Accounts (1-{max_slots})*\n"
                                    f"Total Slots: `{max_slots}` | Active: `{active_count}`\n\n"
                                    f"Click a button below to toggle its active state:\n"
                                    f"✅ = Active & Sniping | ❌ = Disabled"
                                )
                                url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                await tg_client.post(url_edit, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": acc_text,
                                    "parse_mode": "Markdown",
                                    "reply_markup": build_accounts_keyboard(chat_str)
                                })
                            elif data_action == "btn_my_accounts":
                                await answer_callback_query(cq_id, "Accounts manager")
                                active_count = sum(1 for i in range(1, max_slots + 1) if is_slot_active_state(chat_str, str(i)))
                                acc_text = (
                                    f"💼 *My Chowdeck Accounts (1-{max_slots})*\n"
                                    f"Total Slots: `{max_slots}` | Active: `{active_count}`\n\n"
                                    f"Click a button below to toggle its active state:\n"
                                    f"✅ = Active & Sniping | ❌ = Disabled"
                                )
                                url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                await tg_client.post(url_edit, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": acc_text,
                                    "parse_mode": "Markdown",
                                    "reply_markup": build_accounts_keyboard(chat_str)
                                })
                            elif data_action == "btn_portfolio":
                                await answer_callback_query(cq_id, "Portfolio loaded!")
                                portfolio_data = load_json(PORTFOLIO_FILE).get(chat_str, {"items": [], "total_value": 0})
                                items = portfolio_data.get("items", [])
                                total_val = portfolio_data.get("total_value", 0)
                                
                                items_snippet = "\n".join([f"• `{i.get('code', 'UNKNOWN')}` (S{i.get('slot', '1')}) — ₦{i.get('amount', 2000):,}" for i in items[-10:]]) if items else "No claims recorded yet."
                                portfolio_text = (
                                    f"💰 *Your Claim Portfolio*\n\n"
                                    f"• Total Slot Wins: `{len(items)}`\n"
                                    f"• Total Value Saved: `₦{total_val:,}`\n\n"
                                    f"📜 *Recent Slot Claims:*\n{items_snippet}"
                                )
                                await send_telegram(chat_id, portfolio_text)
                            elif data_action == "btn_refresh_phones":
                                await answer_callback_query(cq_id, "Refreshing profiles...")
                                user_slots = load_json(TOKENS_FILE).get(chat_str, {})
                                updated_count = 0
                                for slot_id, s_data in user_slots.items():
                                    token = s_data.get("token")
                                    cookies = s_data.get("cookies", {})
                                    if token:
                                        phone = await fetch_account_phone(token, cookies)
                                        if phone:
                                            s_data["phone"] = str(phone)[:15]
                                            updated_count += 1
                                tokens_db = load_json(TOKENS_FILE)
                                tokens_db[chat_str] = user_slots
                                save_json(TOKENS_FILE, tokens_db)
                                
                                active_count = sum(1 for i in range(1, max_slots + 1) if is_slot_active_state(chat_str, str(i)))
                                acc_text = (
                                    f"💼 *My Chowdeck Accounts (1-{max_slots})*\n"
                                    f"Total Slots: `{max_slots}` | Active: `{active_count}`\n"
                                    f"Refreshed phones for `{updated_count}` slots!\n\n"
                                    f"Click a button below to toggle its active state:"
                                )
                                url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                await tg_client.post(url_edit, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": acc_text,
                                    "parse_mode": "Markdown",
                                    "reply_markup": build_accounts_keyboard(chat_str)
                                })
                            elif data_action == "btn_burner_menu":
                                await answer_callback_query(cq_id, "Burner panel opened")
                                if is_admin:
                                    burner_data = get_burner_credentials(chat_str)
                                    b_status = "🟢 Linked" if burner_data.get("token") else "🔴 Not Configured"
                                    burner_text = (
                                        f"🧪 *Admin Burner Account Panel*\n\n"
                                        f"• Burner Status: {b_status}\n\n"
                                        "Use commands to configure:\n"
                                        "• `/setburnertoken <token>`\n"
                                        "• `/setburnercart <cart_id> <vendor_id>`\n"
                                        "• `/test <CODE>`"
                                    )
                                    url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                    await tg_client.post(url_edit, json={
                                        "chat_id": chat_id,
                                        "message_id": msg_id,
                                        "text": burner_text,
                                        "parse_mode": "Markdown",
                                        "reply_markup": build_admin_burner_keyboard()
                                    })
                            elif data_action == "burner_info":
                                await answer_callback_query(cq_id, "Type /test <CODE>")
                            elif data_action == "btn_main_menu":
                                await answer_callback_query(cq_id, "Main menu")
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                active_s = get_active_slot(chat_str)
                                user_m = get_user_mode(chat_str)
                                master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                                m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                                menu_text = (
                                    f"🤖 *Chowdeck Multi-Account Sniper Active (1-{max_slots})*\n\n"
                                    f"• Active Slot: `Slot {active_s}`\n"
                                    f"• Configured Slots: `{len(tokens_db)} / {max_slots}`\n"
                                    f"• Mode: `{user_m.upper()}`\n"
                                    f"• Master Answer: `{m_ans}`\n\n"
                                    "Select an option below:"
                                )
                                url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                await tg_client.post(url_edit, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": menu_text,
                                    "parse_mode": "Markdown",
                                    "reply_markup": build_control_keyboard(chat_str, is_admin)
                                })
                            elif data_action == "btn_toggle_mode":
                                current_m = get_user_mode(chat_str)
                                new_m = "prompt" if current_m == "auto" else "auto"
                                set_user_mode(chat_str, new_m)
                                await answer_callback_query(cq_id, f"Mode: {new_m.upper()}")
                                
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                active_s = get_active_slot(chat_str)
                                master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                                m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                                updated_text = (
                                    f"🤖 *Chowdeck Multi-Account Sniper Active (1-{max_slots})*\n\n"
                                    f"• Active Slot: `Slot {active_s}`\n"
                                    f"• Configured Slots: `{len(tokens_db)} / {max_slots}`\n"
                                    f"• Mode: `{new_m.upper()}`\n"
                                    f"• Master Answer: `{m_ans}`\n\n"
                                    "Select an option below:"
                                )
                                url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                await tg_client.post(url_edit, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": updated_text,
                                    "parse_mode": "Markdown",
                                    "reply_markup": build_control_keyboard(chat_str, is_admin)
                                })
                            elif data_action == "btn_admin_stats":
                                await answer_callback_query(cq_id, "Stats loaded")
                                if is_admin:
                                    u_count, s_count, c_count = get_admin_stats()
                                    bot_status = "🟢 Active" if get_bot_active() else "⏸️ Paused"
                                    admin_text = (
                                        f"🛡️ *Admin Control Panel*\n\n"
                                        f"• Bot Status: {bot_status}\n"
                                        f"• Total Unique Users: `{u_count}`\n"
                                        f"• Total Configured Slots: `{s_count}`\n"
                                        f"• Total Carts Linked: `{c_count}`"
                                    )
                                    await send_telegram(chat_id, admin_text)
                            elif data_action == "btn_status":
                                await answer_callback_query(cq_id, "Status refreshed")
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                active_s = get_active_slot(chat_str)
                                user_m = get_user_mode(chat_str)
                                master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                                m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                                
                                status_msg = (
                                    f"📊 *Portfolio Status*\n\n"
                                    f"• Active Slot: `Slot {active_s}`\n"
                                    f"• Configured Accounts: `{len(tokens_db)} / {max_slots}`\n"
                                    f"• Sniper Mode: `{user_m.upper()}`\n"
                                    f"• Master Answer: `{m_ans}`"
                                )
                                await send_telegram(chat_id, status_msg)
                            elif data_action == "btn_guide":
                                await answer_callback_query(cq_id, "Guide opened")
                                guide_msg = (
                                    f"📖 *Multi-Account Sniper Guide (1-{max_slots})*\n\n"
                                    f"1️⃣ *Switch Slot:*\nTap `Slot 1` to `Slot {max_slots}` on the keyboard.\n\n"
                                    "2️⃣ *Manage Accounts:*\nTap `👥 My Accounts` to activate/deactivate specific slots with ✅/❌ toggles.\n\n"
                                    "3️⃣ *Link Token:*\n`/settoken <token>`\n\n"
                                    "4️⃣ *Set Phone/Label Manually:*\n`/setphone <phone_or_label>`\n\n"
                                    "5️⃣ *Refresh Phones:*\n`/refreshphones` or use the menu button.\n\n"
                                    "6️⃣ *Master Answer:*\n`/setanswer <answer>`\n\n"
                                    "7️⃣ *Toggle Mode:*\nTap `⚙️ Mode` button!"
                                )
                                await send_telegram(chat_id, guide_msg)
                            elif data_action == "btn_pause":
                                await answer_callback_query(cq_id, "Paused")
                                if is_admin:
                                    set_bot_active(False)
                                    await send_telegram(chat_id, "⏸️ *Bridge Paused* via inline button.")
                            elif data_action == "btn_resume":
                                await answer_callback_query(cq_id, "Resumed")
                                if is_admin:
                                    set_bot_active(True)
                                    await send_telegram(chat_id, "▶️ *Bridge Resumed* via inline button.")
                            continue

                        msg = update.get("message", {})
                        chat_id = msg.get("chat", {}).get("id")
                        text = msg.get("text", "").strip()
                        if not chat_id or not text:
                            continue
                        
                        chat_str = str(chat_id)
                        is_admin = (not ADMIN_CHAT_ID or chat_str == ADMIN_CHAT_ID)
                        active_slot = get_active_slot(chat_str)
                        max_slots = get_max_slots(chat_str)

                        cleaned_input = robust_clean_code(text)
                        is_new_code = (len(cleaned_input) >= 5 and re.match(r'^[A-Z0-9]{5,20}$', cleaned_input) and cleaned_input not in ["ABUJA", "LAGOS", "GHANA", "ISMAIL", "MAY", "JUNE", "JULY", "TRUE", "FALSE"])
                        
                        pending_prompts = load_json(PENDING_PROMPT_FILE)
                        if chat_str in pending_prompts and not text.startswith("/"):
                            if is_new_code:
                                pending_prompts.pop(chat_str, None)
                                save_json(PENDING_PROMPT_FILE, pending_prompts)
                            else:
                                prompt_data = pending_prompts.pop(chat_str, None)
                                save_json(PENDING_PROMPT_FILE, pending_prompts)
                                
                                if prompt_data:
                                    target_code = prompt_data["code"]
                                    user_ans = text
                                    
                                    # Collect all target chats to answer (Admin + Mirror Linked Accounts if admin)
                                    chats_to_answer = [chat_str]
                                    if is_admin:
                                        linked = get_linked_accounts(chat_str)
                                        for l_chat in linked:
                                            if l_chat not in chats_to_answer:
                                                chats_to_answer.append(l_chat)
                                    
                                    async def execute_answer_burst(c_str, code_val, ans_val, is_mirror=False):
                                        answers_db = load_json(ANSWER_FILE)
                                        if c_str not in answers_db:
                                            answers_db[c_str] = {}
                                        answers_db[c_str]["master"] = [ans_val]
                                        save_json(ANSWER_FILE, answers_db)

                                        broadcast_coro = broadcast_to_channel(
                                            f"💡 *Admin Answer Dropped!*\n\n"
                                            f"Code: `{code_val}`\n"
                                            f"Answer: `{ans_val}`\n\n"
                                            f"⚡ *Copy and paste fast!*"
                                        ) if (is_admin and not is_mirror) else asyncio.sleep(0)
                                        
                                        notif_text = f"🔗 *[Mirrored Admin Answer]* Blasting `{ans_val}` instantly across your secondary slots for `{code_val}`..." if is_mirror else f"⚡ *Emergency Answer Captured!* Blasting `{ans_val}` instantly across your active slots for `{code_val}`..."
                                        local_notif_coro = send_telegram(int(c_str), notif_text)
                                        
                                        tokens_db = load_json(TOKENS_FILE).get(c_str, {})
                                        carts_db = load_json(CARTS_FILE)
                                        lockouts_db = load_json(LOCKOUT_FILE)
                                        
                                        slot_result_tasks = []
                                        if tokens_db:
                                            limits = httpx.Limits(max_keepalive_connections=200, max_connections=400)
                                            async with httpx.AsyncClient(timeout=7.0, proxy=PROXY_URL if PROXY_URL else None, limits=limits) as client:
                                                tasks = [
                                                    claim_slot_worker(client=client, chat_id=int(c_str), slot_id=s_id, token_data=t_data, code=code_val,
                                                                      carts_db=carts_db, answers_db=answers_db, lockouts_db=lockouts_db, specific_answer=ans_val)
                                                    for s_id, t_data in tokens_db.items()
                                                ]
                                                results = await asyncio.gather(*tasks)
                                            
                                            for res in results:
                                                if res.get("status") == 0:
                                                    continue
                                                s_id = res["slot"]
                                                st = res["status"]
                                                rp = res.get("response", {})
                                                cs = res.get("cart_str", "")
                                                slot_result_tasks.append(send_telegram(int(c_str), f"🎯 *Code Target:* `{code_val}` (Slot {s_id})\nStatus: `{st}`\nResponse: `{json.dumps(rp)}`{cs}"))

                                        await asyncio.gather(
                                            broadcast_coro,
                                            local_notif_coro,
                                            *slot_result_tasks
                                        )

                                    # Run answer burst for admin and all linked secondary accounts simultaneously
                                    burst_tasks = [execute_answer_burst(c_s, target_code, user_ans, is_mirror=(c_s != chat_str)) for c_s in chats_to_answer]
                                    asyncio.create_task(asyncio.gather(*burst_tasks))
                                continue

                        if text.startswith("/start"):
                            tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                            user_m = get_user_mode(chat_str)
                            master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                            m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                            
                            help_text = (
                                f"🤖 *Chowdeck Multi-Account Sniper Active (1-{max_slots})*\n\n"
                                f"• Active Slot: `Slot {active_slot}`\n"
                                f"• Configured Slots: `{len(tokens_db)} / {max_slots}`\n"
                                f"• Mode: `{user_m.upper()}`\n"
                                f"• Master Answer: `{m_ans}`\n\n"
                                "Select an option below:"
                            )
                            await send_telegram(chat_id, help_text, reply_markup=build_control_keyboard(chat_str, is_admin))
                        elif text.startswith("/admin") and is_admin:
                            u_count, s_count, c_count = get_admin_stats()
                            bot_status = "🟢 Active" if get_bot_active() else "⏸️ Paused"
                            admin_text = (
                                f"🛡️ *Admin Control Panel*\n\n"
                                f"• Bot Status: {bot_status}\n"
                                f"• Total Unique Users: `{u_count}`\n"
                                f"• Total Configured Slots: `{s_count}`\n"
                                f"• Total Carts Linked: `{c_count}`"
                            )
                            await send_telegram(chat_id, admin_text, reply_markup=build_control_keyboard(chat_str, is_admin))
                        elif text.startswith("/portfolio") or text.startswith("/vouchers"):
                            portfolio_data = load_json(PORTFOLIO_FILE).get(chat_str, {"items": [], "total_value": 0})
                            items = portfolio_data.get("items", [])
                            total_val = portfolio_data.get("total_value", 0)
                            items_snippet = "\n".join([f"• `{i.get('code', 'UNKNOWN')}` (S{i.get('slot', '1')}) — ₦{i.get('amount', 2000):,}" for i in items[-15:]]) if items else "No claims recorded yet."
                            portfolio_text = (
                                f"💰 *Your Claim Portfolio*\n\n"
                                f"• Total Slot Wins: `{len(items)}`\n"
                                f"• Total Value Saved: `₦{total_val:,}`\n\n"
                                f"📜 *Recent Slot Claims:*\n{items_snippet}"
                            )
                            await send_telegram(chat_id, portfolio_text)
                        elif text.startswith("/acc"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1 and parts[1].strip() in [str(i) for i in range(1, max_slots + 1)]:
                                set_active_slot(chat_str, parts[1].strip())
                                await send_telegram(chat_id, f"✅ Switched to *Slot {parts[1].strip()}*.", reply_markup=build_control_keyboard(chat_str, is_admin))
                            else:
                                await send_telegram(chat_id, f"📌 Active Slot: *Slot {active_slot}*\nUse `/acc <1-{max_slots}>` or tap buttons above.")
                        elif text.startswith("/linkaccount") and is_admin:
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                sec_id = parts[1].strip()
                                add_linked_account(chat_str, sec_id)
                                await send_telegram(chat_id, f"✅ Successfully linked secondary Telegram Chat ID: `{sec_id}`.\nAll manual codes and security answers will now mirror to this account instantly!")
                            else:
                                await send_telegram(chat_id, "⚠️ Usage: `/linkaccount <secondary_chat_id>`")
                        elif text.startswith("/unlinkaccount") and is_admin:
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                sec_id = parts[1].strip()
                                remove_linked_account(chat_str, sec_id)
                                await send_telegram(chat_id, f"✅ Unlinked secondary Telegram Chat ID: `{sec_id}`.")
                            else:
                                await send_telegram(chat_id, "⚠️ Usage: `/unlinkaccount <secondary_chat_id>`")
                        elif text.startswith("/listlinked") and is_admin:
                            linked = get_linked_accounts(chat_str)
                            if linked:
                                linked_str = "\n".join([f"• `{acc}`" for acc in linked])
                                await send_telegram(chat_id, f"🔗 *Linked Secondary Telegram Accounts:*\n{linked_str}")
                            else:
                                await send_telegram(chat_id, "📌 No secondary accounts linked yet. Use `/linkaccount <chat_id>` to add one.")
                        elif text.startswith("/settoken"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                parse_and_store_credentials(parts[1], chat_str, active_slot)
                                
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                slot_data = tokens_db.get(active_slot, {})
                                phone_found = slot_data.get("phone", "Unknown")
                                
                                await send_telegram(chat_id, f"✅ Token & session cookies saved to *Slot {active_slot}*!\n📞 Linked Phone/User: `{phone_found}`")
                        elif text.startswith("/setphone"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                custom_phone = parts[1].strip()[:15]
                                tokens_db = load_json(TOKENS_FILE)
                                if chat_str not in tokens_db:
                                    tokens_db[chat_str] = {}
                                if active_slot not in tokens_db[chat_str]:
                                    tokens_db[chat_str][active_slot] = {}
                                tokens_db[chat_str][active_slot]["phone"] = custom_phone
                                save_json(TOKENS_FILE, tokens_db)
                                await send_telegram(chat_id, f"✅ Phone/Label for *Slot {active_slot}* set to: `{custom_phone}`")
                            else:
                                await send_telegram(chat_id, "⚠️ Usage: `/setphone <phone_number_or_label>`")
                        elif text.startswith("/refreshphones"):
                            tokens_db = load_json(TOKENS_FILE)
                            user_slots = tokens_db.get(chat_str, {})
                            
                            if not user_slots:
                                await send_telegram(chat_id, "❌ No accounts configured yet.")
                            else:
                                await send_telegram(chat_id, "🔄 Querying Chowdeck profiles & JWT claims...")
                                updated_count = 0
                                
                                for slot_id, s_data in user_slots.items():
                                    token = s_data.get("token")
                                    cookies = s_data.get("cookies", {})
                                    if token:
                                        phone = await fetch_account_phone(token, cookies)
                                        if phone:
                                            s_data["phone"] = str(phone)[:15]
                                            updated_count += 1
                                            
                                save_json(TOKENS_FILE, tokens_db)
                                await send_telegram(
                                    chat_id, 
                                    f"✅ *Phone Number Sync Complete!*\nSuccessfully updated phone numbers for `{updated_count}` configured slots. Open `👥 My Accounts` to view them."
                                )
                        elif text.startswith("/setcart"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                subparts = parts[1].strip().split()
                                c_id = subparts[0]
                                v_id = subparts[1] if len(subparts) > 1 else None
                                
                                carts_db = load_json(CARTS_FILE)
                                if chat_str not in carts_db:
                                    carts_db[chat_str] = {}
                                carts_db[chat_str][active_slot] = {"cart_id": c_id, "vendor_id": v_id}
                                save_json(CARTS_FILE, carts_db)
                                await send_telegram(chat_id, f"✅ Cart ID (`{c_id}`) & Vendor ID (`{v_id}`) saved to *Slot {active_slot}*.")
                        elif text.startswith("/setanswer"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                single_ans = parts[1].strip()
                                answers_db = load_json(ANSWER_FILE)
                                if chat_str not in answers_db:
                                    answers_db[chat_str] = {}
                                answers_db[chat_str]["master"] = [single_ans]
                                save_json(ANSWER_FILE, answers_db)
                                await send_telegram(chat_id, f"✅ Master Security Answer Saved: `{single_ans}`")
                        elif text.startswith("/mode"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1 and parts[1].strip().lower() in ["auto", "prompt"]:
                                new_mode = parts[1].strip().lower()
                                set_user_mode(chat_str, new_mode)
                                await send_telegram(chat_id, f"✅ Mode switched to: *{new_mode.upper()}*", reply_markup=build_control_keyboard(chat_str, is_admin))
                            else:
                                current_m = get_user_mode(chat_str)
                                await send_telegram(chat_id, f"📌 Current Mode: *{current_m.upper()}*")
                        elif text.startswith("/setburnertoken") and is_admin:
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                raw_input = parts[1].strip()
                                extracted_token = raw_input
                                extracted_cookies = {}
                                if "user=" in raw_input or ";" in raw_input:
                                    try:
                                        for p in raw_input.split(";"):
                                            p = p.strip()
                                            if "=" in p:
                                                k, v = p.split("=", 1)
                                                extracted_cookies[k.strip()] = v.strip()
                                                if k.strip() == "user":
                                                    decoded = urllib.parse.unquote(v.strip())
                                                    data = json.loads(decoded)
                                                    if "token" in data:
                                                        extracted_token = data["token"]
                                    except Exception:
                                        pass
                                if extracted_token.lower().startswith("bearer "):
                                    extracted_token = extracted_token[7:].strip()
                                
                                save_burner_credentials(chat_str, {"token": extracted_token, "cookies": extracted_cookies})
                                await send_telegram(chat_id, "✅ *Admin Burner Account Token* successfully linked!")
                        elif text.startswith("/setburnercart") and is_admin:
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                subparts = parts[1].strip().split()
                                c_id = subparts[0]
                                v_id = subparts[1] if len(subparts) > 1 else None
                                save_burner_cart(chat_str, {"cart_id": c_id, "vendor_id": v_id})
                                await send_telegram(chat_id, f"✅ *Burner Cart Linked* — Cart ID: `{c_id}`, Vendor ID: `{v_id}`")
                        elif text.startswith("/test") and is_admin:
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                test_code = robust_clean_code(parts[1])
                                await send_telegram(chat_id, f"🔬 Testing code `{test_code}` on burner account...")
                                result_msg = await execute_burner_test(chat_id, test_code)
                                await send_telegram(chat_id, result_msg)
                            else:
                                await send_telegram(chat_id, "⚠️ Usage: `/test <CODE>`")
                        elif text.startswith("/pause") and is_admin:
                            set_bot_active(False)
                            await send_telegram(chat_id, "⏸️ Bridge Paused.")
                        elif text.startswith("/resume") and is_admin:
                            set_bot_active(True)
                            await send_telegram(chat_id, "▶️ Bridge Resumed.")
                        else:
                            await process_text_isolated(chat_str, text)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"Poller error: {e}", flush=True)
            await asyncio.sleep(0.1)

async def main():
    global loop_ref
    loop_ref = asyncio.get_running_loop()
    threading.Thread(target=run_http_server, daemon=True).start()
    worker_task = asyncio.create_task(worker())
    try:
        await telegram_poller()
    finally:
        worker_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Stopped.", flush=True)
