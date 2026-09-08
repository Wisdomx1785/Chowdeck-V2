import re
import asyncio
import os
import json
import unicodedata
import urllib.parse
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import httpx
import time

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()

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

def get_admin_stats():
    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    total_users = len(tokens_db)
    total_slots_configured = sum(len(slots) for slots in tokens_db.values())
    total_carts_configured = sum(len(carts) for carts in carts_db.values())
    return total_users, total_slots_configured, total_carts_configured

def parse_and_store_credentials(raw_input, chat_str, slot):
    cleaned = raw_input.strip()
    extracted_token = cleaned
    extracted_cookies = {}

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
        except Exception as e:
            print(f"Cookie parse error: {e}", flush=True)

    if cleaned.startswith("{") and "token" in cleaned:
        try:
            data = json.loads(cleaned)
            if "token" in data:
                extracted_token = data["token"]
        except Exception:
            pass
            
    if extracted_token.lower().startswith("bearer "):
        extracted_token = extracted_token[7:].strip()

    tokens_db = load_json(TOKENS_FILE)
    if chat_str not in tokens_db:
        tokens_db[chat_str] = {}
    
    tokens_db[chat_str][slot] = {
        "token": extracted_token,
        "cookies": extracted_cookies
    }
    save_json(TOKENS_FILE, tokens_db)
    return extracted_token

def robust_clean_code(raw):
    if not raw:
        return ""
    normalized = unicodedata.normalize("NFKD", str(raw))
    cleaned = "".join(c for c in normalized if c.isalnum()).upper()
    return cleaned

async def send_telegram(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": int(chat_id), "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        async with httpx.AsyncClient(proxy=PROXY_URL if PROXY_URL else None) as temp_client:
            res = await temp_client.post(url, json=payload, timeout=15.0)
            return res.json().get("result", {}).get("message_id")
    except Exception as e:
        print(f"Telegram send error to {chat_id}: {e}", flush=True)
        return None

async def answer_callback_query(callback_query_id, text=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        async with httpx.AsyncClient(proxy=PROXY_URL if PROXY_URL else None) as temp_client:
            await temp_client.post(url, json={"callback_query_id": callback_query_id, "text": text}, timeout=10.0)
    except Exception as e:
        print(f"Callback answer error: {e}", flush=True)

def execute_chowdeck_request(method, url, headers, cookies, json_data):
    with httpx.Client(timeout=15.0, proxy=PROXY_URL if PROXY_URL else None) as client:
        if method == "POST":
            return client.post(url, json=json_data, headers=headers, cookies=cookies)
        return client.get(url, headers=headers, cookies=cookies)

def build_control_keyboard(chat_str, is_admin):
    active_slot = get_active_slot(chat_str)
    current_mode = get_user_mode(chat_str).upper()
    
    slot_row_1 = []
    slot_row_2 = []
    for i in range(1, 11):
        s_str = str(i)
        is_active = is_slot_active_state(chat_str, s_str)
        icon = "✅" if is_active else "❌"
        label = f"📌 S{i}" if s_str == active_slot else f"{icon} {i}"
        
        btn = {"text": label, "callback_data": f"slot_{i}"}
        if i <= 5:
            slot_row_1.append(btn)
        else:
            slot_row_2.append(btn)
        
    keyboard = [
        slot_row_1,
        slot_row_2,
        [
            {"text": "👥 My Accounts", "callback_data": "btn_my_accounts"},
            {"text": f"⚙️ Mode: {current_mode}", "callback_data": "btn_toggle_mode"}
        ],
        [
            {"text": "🔄 Refresh", "callback_data": "btn_status"},
            {"text": "📖 Guide / Help", "callback_data": "btn_guide"}
        ]
    ]
    if is_admin:
        keyboard.insert(2, [{"text": "🛡️ Admin Stats", "callback_data": "btn_admin_stats"}, {"text": "🧪 Burner Panel", "callback_data": "btn_burner_menu"}])
        keyboard.append([
            {"text": "⏸️ Pause", "callback_data": "btn_pause"},
            {"text": "▶️ Resume", "callback_data": "btn_resume"}
        ])
    return {"inline_keyboard": keyboard}

def build_accounts_keyboard(chat_str):
    rows = []
    row = []
    for i in range(1, 11):
        s_str = str(i)
        active = is_slot_active_state(chat_str, s_str)
        status_icon = "✅" if active else "❌"
        row.append({"text": f"{status_icon} Slot {i}", "callback_data": f"toggle_acc_{i}"})
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([{"text": "🔙 Back to Main Menu", "callback_data": "btn_main_menu"}])
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
        claim_res = await asyncio.to_thread(
            execute_chowdeck_request, "POST", claim_url, headers, user_cookies, payload_data
        )
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
            apply_res = await asyncio.to_thread(
                execute_chowdeck_request, "POST", "https://api.chowdeck.com/customer/discount/apply", headers, user_cookies, apply_payload
            )
            cart_status_str = f"\nCart Apply Status: `{apply_res.status_code}` | Response: `{apply_res.text}`"

        return (
            f"🧪 *Burner Test Result for `{code}`*\n"
            f"Status Code: `{claim_res.status_code}`\n"
            f"API Response: `{json.dumps(res_json)}`{cart_status_str}"
        )
    except Exception as e:
        return f"❌ Burner test error: {e}"

async def claim_slot_worker(chat_id, slot_id, token_data, code, carts_db, answers_db, lockouts_db, specific_answer=None):
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
    
    try:
        master_ans_list = answers_db.get(chat_str, {}).get("master", ["Ismail"])
        master_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"

        # PROMPT MODE: Probe first without guessing pre-configured answer
        if user_mode == "prompt" and not specific_answer:
            probe_payload = {"code": code, "answer": "", "security_answer": ""}
            probe_res = await asyncio.to_thread(
                execute_chowdeck_request, "POST", claim_url, headers, user_cookies, probe_payload
            )
            probe_json = {}
            try:
                probe_json = probe_res.json()
            except Exception:
                pass
            
            data_obj = probe_json.get("data", {})
            q_text = None
            if isinstance(data_obj, dict):
                q_text = data_obj.get("security_question") or data_obj.get("question")
            if not q_text:
                msg_txt = probe_json.get("message", "").lower()
                if "security question" in msg_txt or "question" in msg_txt:
                    q_text = probe_json.get("message")
            
            if q_text or probe_res.status_code == 400:
                final_q = q_text if q_text else probe_json.get("message", "Security question required.")
                return {
                    "slot": slot_key,
                    "status": probe_res.status_code,
                    "needs_prompt": True,
                    "question": final_q,
                    "response": probe_json
                }

        # AUTO MODE or user provided specific answer
        ans_to_use = specific_answer if specific_answer else master_ans
        payload_data = {"code": code, "answer": ans_to_use, "security_answer": ans_to_use}
        
        claim_res = await asyncio.to_thread(
            execute_chowdeck_request, "POST", claim_url, headers, user_cookies, payload_data
        )
        
        res_json = {}
        try:
            res_json = claim_res.json()
        except Exception:
            pass
            
        msg_text = res_json.get("message", "").lower()
        data_obj = res_json.get("data", {})
        
        q_text = None
        if isinstance(data_obj, dict):
            q_text = data_obj.get("security_question") or data_obj.get("question")
        if not q_text and ("security question" in msg_txt or "incorrect" in msg_txt or "not correct" in msg_txt):
            q_text = res_json.get("message")

        if not specific_answer:
            if "too many failed" in msg_text:
                lockouts_db[lockout_key] = time.time() + 900
                save_json(LOCKOUT_FILE, lockouts_db)
            elif q_text:
                # Auto mode pre-configured answer failed / triggered question -> ask user for emergency answer
                return {
                    "slot": slot_key, 
                    "status": claim_res.status_code, 
                    "needs_prompt": True, 
                    "question": q_text, 
                    "response": res_json
                }

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
                
            apply_res = await asyncio.to_thread(
                execute_chowdeck_request, "POST", "https://api.chowdeck.com/customer/discount/apply", headers, user_cookies, apply_payload
            )
            cart_status_str = f"\nCart Apply Status: `{apply_res.status_code}` | Response: `{apply_res.text}`"

        return {
            "slot": slot_key,
            "status": claim_res.status_code,
            "response": res_json,
            "cart_str": cart_status_str,
            "needs_prompt": False
        }
    except Exception as e:
        return {"slot": slot_key, "status": 500, "message": str(e)}

async def claim_code(raw_code):
    code = robust_clean_code(raw_code)
    if not code or len(code) < 4:
        return

    claimed_history = load_json(CLAIMED_FILE)
    if code in claimed_history:
        return

    tokens_db = load_json(TOKENS_FILE)
    carts_db = load_json(CARTS_FILE)
    answers_db = load_json(ANSWER_FILE)
    lockouts_db = load_json(LOCKOUT_FILE)
    
    if not tokens_db:
        return
    
    for chat_str, user_slots in tokens_db.items():
        if not user_slots:
            continue
            
        tasks = [
            claim_slot_worker(chat_id=int(chat_str), slot_id=slot_id, token_data=t_data, code=code, 
                              carts_db=carts_db, answers_db=answers_db, lockouts_db=lockouts_db)
            for slot_id, t_data in user_slots.items()
        ]
        
        results = await asyncio.gather(*tasks)
        
        for res in results:
            slot = res["slot"]
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
                
                prompt_msg = (
                    f"🔒 *Security Question Detected for `{code}` (Slot {slot})!*\n"
                    f"Question: *\"{q_text}\"*\n\n"
                    f"⚡ *Type the answer right here in chat* to instantly broadcast it to all active slots!"
                )
                await send_telegram(chat_id=int(chat_str), text=prompt_msg)
            else:
                summary_msg = f"🎯 *Code Target:* `{code}` (Slot {slot})\nStatus: `{status}`\nResponse: `{json.dumps(resp_data)}`{cart_suffix}"
                await send_telegram(chat_id=int(chat_str), text=summary_msg)

async def worker():
    while True:
        try:
            code = await code_queue.get()
            try:
                await claim_code(code)
            except Exception as e:
                print(f"Worker error: {e}", flush=True)
            finally:
                code_queue.task_done()
                await asyncio.sleep(2.5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            await asyncio.sleep(2)

async def process_text(text):
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
        await code_queue.put(code)

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
                asyncio.run_coroutine_threadsafe(process_text(text), loop_ref)
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
                res = await tg_client.get(url, params={"offset": last_update_id + 1, "timeout": 30}, timeout=35.0)
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
                            
                            if data_action.startswith("slot_"):
                                slot_num = data_action.split("_")[1]
                                set_active_slot(chat_str, slot_num)
                                await answer_callback_query(cq_id, f"Switched to Slot {slot_num}")
                                
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                user_m = get_user_mode(chat_str)
                                master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                                m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                                updated_text = (
                                    f"🤖 *Chowdeck Multi-Account Sniper Active (1-10)*\n\n"
                                    f"• Active Slot: `Slot {slot_num}`\n"
                                    f"• Configured Slots: `{len(tokens_db)} / 10`\n"
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
                                s_num = data_action.split("_")[2]
                                new_state = toggle_slot_state(chat_str, s_num)
                                state_text = "Activated" if new_state else "Deactivated"
                                await answer_callback_query(cq_id, f"Slot {s_num} {state_text}!")
                                
                                active_count = sum(1 for i in range(1, 11) if is_slot_active_state(chat_str, str(i)))
                                acc_text = (
                                    f"💼 *My Chowdeck Accounts (1-10)*\n"
                                    f"Total Configured: `10` | Active: `{active_count}`\n\n"
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
                                active_count = sum(1 for i in range(1, 11) if is_slot_active_state(chat_str, str(i)))
                                acc_text = (
                                    f"💼 *My Chowdeck Accounts (1-10)*\n"
                                    f"Total Slots: `10` | Active: `{active_count}`\n\n"
                                    f"Click a button below to toggle its active state:\n"
                                    f"✅ = Active & Sniping | ❌ = Disabled"
                                )
                                await answer_callback_query(cq_id, "Opening accounts manager...")
                                url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                await tg_client.post(url_edit, json={
                                    "chat_id": chat_id,
                                    "message_id": msg_id,
                                    "text": acc_text,
                                    "parse_mode": "Markdown",
                                    "reply_markup": build_accounts_keyboard(chat_str)
                                })
                            elif data_action == "btn_burner_menu":
                                if not is_admin:
                                    await answer_callback_query(cq_id, "⛔ Unauthorized!")
                                else:
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
                                    await answer_callback_query(cq_id, "Burner panel opened!")
                                    url_edit = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
                                    await tg_client.post(url_edit, json={
                                        "chat_id": chat_id,
                                        "message_id": msg_id,
                                        "text": burner_text,
                                        "parse_mode": "Markdown",
                                        "reply_markup": build_admin_burner_keyboard()
                                    })
                            elif data_action == "burner_info":
                                await answer_callback_query(cq_id, "Type /test <CODE> in chat to run safe check!")
                            elif data_action == "btn_main_menu":
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                active_s = get_active_slot(chat_str)
                                user_m = get_user_mode(chat_str)
                                master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                                m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                                menu_text = (
                                    f"🤖 *Chowdeck Multi-Account Sniper Active (1-10)*\n\n"
                                    f"• Active Slot: `Slot {active_s}`\n"
                                    f"• Configured Slots: `{len(tokens_db)} / 10`\n"
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
                                await answer_callback_query(cq_id, f"Mode switched to {new_m.upper()}")
                                
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                active_s = get_active_slot(chat_str)
                                master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                                m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                                updated_text = (
                                    f"🤖 *Chowdeck Multi-Account Sniper Active (1-10)*\n\n"
                                    f"• Active Slot: `Slot {active_s}`\n"
                                    f"• Configured Slots: `{len(tokens_db)} / 10`\n"
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
                                if not is_admin:
                                    await answer_callback_query(cq_id, "⛔ Unauthorized!")
                                else:
                                    u_count, s_count, c_count = get_admin_stats()
                                    bot_status = "🟢 Active" if get_bot_active() else "⏸️ Paused"
                                    admin_text = (
                                        f"🛡️ *Admin Control Panel*\n\n"
                                        f"• Bot Status: {bot_status}\n"
                                        f"• Total Unique Users: `{u_count}`\n"
                                        f"• Total Configured Slots: `{s_count}`\n"
                                        f"• Total Carts Linked: `{c_count}`"
                                    )
                                    await answer_callback_query(cq_id, "Admin stats loaded!")
                                    await send_telegram(chat_id, admin_text)
                            elif data_action == "btn_status":
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                active_s = get_active_slot(chat_str)
                                user_m = get_user_mode(chat_str)
                                master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                                m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                                
                                status_msg = (
                                    f"📊 *Portfolio Status*\n\n"
                                    f"• Active Slot: `Slot {active_s}`\n"
                                    f"• Configured Accounts: `{len(tokens_db)} / 10`\n"
                                    f"• Sniper Mode: `{user_m.upper()}`\n"
                                    f"• Master Answer: `{m_ans}`"
                                )
                                await answer_callback_query(cq_id, "Status refreshed!")
                                await send_telegram(chat_id, status_msg)
                            elif data_action == "btn_guide":
                                guide_msg = (
                                    "📖 *Multi-Account Sniper Guide (1-10)*\n\n"
                                    "1️⃣ *Switch Slot:*\nTap `Slot 1` to `Slot 10` on the keyboard.\n\n"
                                    "2️⃣ *Manage Accounts:*\nTap `👥 My Accounts` to activate/deactivate specific slots with ✅/❌ toggles.\n\n"
                                    "3️⃣ *Link Token:*\n`/settoken <token>`\n\n"
                                    "4️⃣ *Set Cart ID & Vendor ID:*\n`/setcart <cart_id> <vendor_id>`\n\n"
                                    "5️⃣ *Master Answer:*\n`/setanswer <answer>`\n\n"
                                    "6️⃣ *Toggle Mode:*\nTap `⚙️ Mode` button!"
                                )
                                await answer_callback_query(cq_id, "Opening guide...")
                                await send_telegram(chat_id, guide_msg)
                            elif data_action == "btn_pause":
                                if is_admin:
                                    set_bot_active(False)
                                    await answer_callback_query(cq_id, "Bridge Paused!")
                                    await send_telegram(chat_id, "⏸️ *Bridge Paused* via inline button.")
                            elif data_action == "btn_resume":
                                if is_admin:
                                    set_bot_active(True)
                                    await answer_callback_query(cq_id, "Bridge Resumed!")
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

                        pending_prompts = load_json(PENDING_PROMPT_FILE)
                        if chat_str in pending_prompts and not text.startswith("/"):
                            prompt_data = pending_prompts.pop(chat_str, None)
                            save_json(PENDING_PROMPT_FILE, pending_prompts)
                            
                            if prompt_data:
                                target_code = prompt_data["code"]
                                user_ans = text
                                
                                answers_db = load_json(ANSWER_FILE)
                                if chat_str not in answers_db:
                                    answers_db[chat_str] = {}
                                answers_db[chat_str]["master"] = [user_ans]
                                save_json(ANSWER_FILE, answers_db)
                                
                                await send_telegram(chat_id, f"⚡ *Emergency Answer Captured!* Blasting `{user_ans}` instantly across all active slots for `{target_code}`...")
                                
                                tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                                carts_db = load_json(CARTS_FILE)
                                lockouts_db = load_json(LOCKOUT_FILE)
                                
                                if tokens_db:
                                    tasks = [
                                        claim_slot_worker(chat_id=chat_id, slot_id=s_id, token_data=t_data, code=target_code,
                                                          carts_db=carts_db, answers_db=answers_db, lockouts_db=lockouts_db, specific_answer=user_ans)
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
                                        await send_telegram(chat_id, f"🎯 *Slot {s_id} Broadcast Result*\nStatus: `{st}`\nResponse: `{json.dumps(rp)}`{cs}")
                                continue

                        if text.startswith("/start"):
                            tokens_db = load_json(TOKENS_FILE).get(chat_str, {})
                            user_m = get_user_mode(chat_str)
                            master_ans_list = load_json(ANSWER_FILE).get(chat_str, {}).get("master", ["Ismail"])
                            m_ans = master_ans_list[0] if isinstance(master_ans_list, list) and master_ans_list else "Ismail"
                            
                            help_text = (
                                "🤖 *Chowdeck Multi-Account Sniper Active (1-10)*\n\n"
                                f"• Active Slot: `Slot {active_slot}`\n"
                                f"• Configured Slots: `{len(tokens_db)} / 10`\n"
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
                        elif text.startswith("/acc"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1 and parts[1].strip() in [str(i) for i in range(1, 11)]:
                                set_active_slot(chat_str, parts[1].strip())
                                await send_telegram(chat_id, f"✅ Switched to *Slot {parts[1].strip()}*.", reply_markup=build_control_keyboard(chat_str, is_admin))
                            else:
                                await send_telegram(chat_id, f"📌 Active Slot: *Slot {active_slot}*\nUse `/acc <1-10>` or tap buttons above.")
                        elif text.startswith("/settoken"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                parse_and_store_credentials(parts[1], chat_str, active_slot)
                                await send_telegram(chat_id, f"✅ Token & session cookies saved to *Slot {active_slot}*.")
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
                            await process_text(text)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"Poller error: {e}", flush=True)
            await asyncio.sleep(2)

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
