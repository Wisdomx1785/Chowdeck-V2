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

TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tokens.json")
CART_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cart.json")
COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies_db.json")
ANSWER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "answers.json")
CLAIMED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "claimed.json")
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_state.json")
LOCKOUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lockouts.json")

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
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"❌ Failed to save {file_path}: {e}", flush=True)

def get_bot_active():
    state = load_json(STATE_FILE)
    return state.get("active", True)

def set_bot_active(val):
    save_json(STATE_FILE, {"active": val})

def sanitize_token(raw_input):
    cleaned = raw_input.strip()
    if "user=" in cleaned:
        try:
            parts = cleaned.split(";")
            for p in parts:
                p = p.strip()
                if p.startswith("user="):
                    val = p[5:]
                    decoded = urllib.parse.unquote(val)
                    data = json.loads(decoded)
                    if "token" in data:
                        return data["token"]
        except Exception as e:
            print(f"Cookie string parse error: {e}", flush=True)

    if cleaned.startswith("{") and "token" in cleaned:
        try:
            data = json.loads(cleaned)
            if "token" in data:
                cleaned = data["token"]
        except Exception:
            pass
            
    if cleaned.lower().startswith("bearer "):
        cleaned = cleaned[7:].strip()
        
    return cleaned

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

async def claim_for_single_user(chat_id, token, code, user_cookies, carts, answers_db, lockouts, specific_answer=None):
    chat_str = str(chat_id)
    
    lockout_expiry = lockouts.get(chat_str, 0)
    if time.time() < lockout_expiry and not specific_answer:
        print(f"🔒 User {chat_id} is temporarily locked out. Skipping.", flush=True)
        return 429

    clean_token = sanitize_token(token)
    headers = {
        "Authorization": f"Bearer {clean_token}",
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
    print(f"📤 Firing request for code '{code}' (User {chat_id})...", flush=True)
    
    try:
        if specific_answer:
            print(f"⚡ Instant retry override with answer: '{specific_answer}'", flush=True)
            claim_res = await asyncio.to_thread(
                execute_chowdeck_request, "POST", claim_url, headers, user_cookies, {"code": code, "answer": specific_answer}
            )
        else:
            claim_res = await asyncio.to_thread(
                execute_chowdeck_request, "POST", claim_url, headers, user_cookies, {"code": code}
            )
        
        success_cleared = (claim_res.status_code == 200)
        tried_answers = []

        if not success_cleared and not specific_answer:
            try:
                res_data = claim_res.json()
                msg_text = res_data.get("message", "").lower()
                
                if "too many failed" in msg_text:
                    lockouts[chat_str] = time.time() + 900
                    save_json(LOCKOUT_FILE, lockouts)
                elif not any(dead in msg_text for dead in ["does not exist", "expired", "already claimed"]):
                    user_answers = answers_db.get(chat_str, ["Ismail"])
                    if isinstance(user_answers, str):
                        user_answers = [user_answers]
                    
                    for ans in user_answers[:3]:
                        tried_answers.append(ans)
                        claim_res = await asyncio.to_thread(
                            execute_chowdeck_request, "POST", claim_url, headers, user_cookies, {"code": code, "answer": ans}
                        )
                        try:
                            if "too many failed" in claim_res.json().get("message", "").lower():
                                lockouts[chat_str] = time.time() + 900
                                save_json(LOCKOUT_FILE, lockouts)
                                break
                        except Exception:
                            pass

                        if claim_res.status_code == 200:
                            success_cleared = True
                            break
                        await asyncio.sleep(0.05)
            except Exception as e:
                print(f"⚠️ Challenge eval error: {e}", flush=True)

        print(f"📥 User {chat_id} Status [{claim_res.status_code}]: {claim_res.text}", flush=True)
        msg = f"🎯 *Code Target:* `{code}`\nStatus: `{claim_res.status_code}`\nResponse: `{claim_res.text}`"
        
        user_cart = carts.get(chat_str)
        if user_cart and user_cart.get("cart_id") and claim_res.status_code == 200:
            payload = {
                "cart_id": user_cart.get("cart_id"),
                "promo_codes": [code]
            }
            apply_res = await asyncio.to_thread(
                execute_chowdeck_request, "POST", "https://api.chowdeck.com/customer/discount/apply", headers, user_cookies, payload
            )
            msg += f"\nCart Apply Status: `{apply_res.status_code}`\nResponse: `{apply_res.text}`"
        
        if not success_cleared and not specific_answer and tried_answers:
            alert_text = (
                f"🚨 *Security Failed for `{code}`!*\n"
                f"Tried: `{', '.join(tried_answers)}`\n\n"
                f"⚠️ *Reply directly to this message with the correct answer* to instantly save it and auto-retry!"
            )
            await send_telegram(chat_id, alert_text)
        else:
            await send_telegram(chat_id, msg)
            
        return claim_res.status_code
    except Exception as e:
        print(f"❌ Error for user {chat_id}: {e}", flush=True)
        return 500

async def claim_code(raw_code):
    code = robust_clean_code(raw_code)
    if not code or len(code) < 4:
        return

    claimed_history = load_json(CLAIMED_FILE)
    if code in claimed_history:
        print(f"⏩ Skipping '{code}': Already attempted/claimed previously.", flush=True)
        return

    tokens = load_json(TOKEN_FILE)
    carts = load_json(CART_FILE)
    cookie_db = load_json(COOKIE_FILE)
    answers_db = load_json(ANSWER_FILE)
    lockouts = load_json(LOCKOUT_FILE)
    
    if not tokens:
        return
    
    tasks = [
        claim_for_single_user(chat_id, token, code, cookie_db.get(str(chat_id), {}), carts, answers_db, lockouts)
        for chat_id, token in tokens.items()
    ]
    await asyncio.gather(*tasks)

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
            print(f"Worker loop error: {e}", flush=True)
            await asyncio.sleep(2)

async def process_text(text):
    if not text:
        return
    print(f"Scanning text: {text}", flush=True)
    pattern = r'\b(?=[A-Z0-9]*[A-Z])[A-Z0-9]{5,20}\b'
    codes = re.findall(pattern, text)
    ignore = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    
    valid = []
    for c in codes:
        cleaned = robust_clean_code(c)
        if cleaned and cleaned not in ignore and not cleaned.startswith("TEST"):
            valid.append(cleaned)
            
    for code in list(dict.fromkeys(valid)):
        print(f"🚨 Queued Code: {code}", flush=True)
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
            except Exception as e:
                print(f"Status check error: {e}", flush=True)
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
        except Exception as e:
            print(f"Webhook error: {e}", flush=True)
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
                        
                        # Handle Inline Keyboard Button Clicks (Callback Queries)
                        if "callback_query" in update:
                            cq = update["callback_query"]
                            cq_id = cq["id"]
                            chat_id = cq["message"]["chat"]["id"]
                            data_action = cq["data"]
                            chat_str = str(chat_id)
                            is_admin = (not ADMIN_CHAT_ID or chat_str == ADMIN_CHAT_ID)
                            
                            if data_action == "btn_status":
                                tokens = load_json(TOKEN_FILE)
                                carts = load_json(CART_FILE)
                                answers = load_json(ANSWER_FILE)
                                has_token = "✅ Configured" if chat_str in tokens else "❌ Not Set"
                                has_cart = "✅ Configured" if chat_str in carts else "❌ Not Set"
                                user_ans = answers.get(chat_str, ["Ismail"])
                                bot_status = "🟢 Active" if get_bot_active() else "⏸️ Paused"
                                
                                status_msg = (
                                    f"📊 *Live Status*\n\n"
                                    f"• Token: {has_token}\n• Cart: {has_cart}\n• Answers: `{', '.join(user_ans)}`\n• State: {bot_status}"
                                )
                                await answer_callback_query(cq_id, "Status refreshed!")
                                await send_telegram(chat_id, status_msg)
                            elif data_action == "btn_pause":
                                if not is_admin:
                                    await answer_callback_query(cq_id, "⛔ Unauthorized!")
                                else:
                                    set_bot_active(False)
                                    await answer_callback_query(cq_id, "Bridge Paused!")
                                    await send_telegram(chat_id, "⏸️ *Bridge Paused* via inline button.")
                            elif data_action == "btn_resume":
                                if not is_admin:
                                    await answer_callback_query(cq_id, "⛔ Unauthorized!")
                                else:
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

                        # Handle Emergency Reply-to-Message Override + Immediate Retry
                        reply_to = msg.get("reply_to_message")
                        if reply_to and "Security Failed" in reply_to.get("text", ""):
                            new_ans = text
                            if new_ans:
                                answers = load_json(ANSWER_FILE)
                                current_list = answers.get(chat_str, ["Ismail"])
                                if isinstance(current_list, str):
                                    current_list = [current_list]
                                if new_ans not in current_list:
                                    current_list.insert(0, new_ans)
                                answers[chat_str] = current_list[:3]
                                save_json(ANSWER_FILE, answers)
                                
                                match_code = re.search(r'`([A-Z0-9]+)`', reply_to.get("text", ""))
                                if match_code:
                                    target_code = match_code.group(1)
                                    await send_telegram(chat_id, f"⚡ *Override Saved!* Re-firing claim instantly for `{target_code}` with answer `{new_ans}`...")
                                    
                                    tokens = load_json(TOKEN_FILE)
                                    carts = load_json(CART_FILE)
                                    cookie_db = load_json(COOKIE_FILE)
                                    lockouts = load_json(LOCKOUT_FILE)
                                    
                                    if chat_str in tokens:
                                        await claim_for_single_user(
                                            chat_id, tokens[chat_str], target_code, 
                                            cookie_db.get(chat_str, {}), carts, answers, lockouts, specific_answer=new_ans
                                        )
                                continue

                        if text.startswith("/start"):
                            tokens = load_json(TOKEN_FILE)
                            carts = load_json(CART_FILE)
                            answers = load_json(ANSWER_FILE)
                            has_token = "✅ Configured" if chat_str in tokens else "❌ Not Set"
                            has_cart = "✅ Configured" if chat_str in carts else "❌ Not Set"
                            user_ans = answers.get(chat_str, ["Ismail"])
                            
                            help_text = (
                                "🤖 *Chowdeck Sniper Pro Active!*\n\n"
                                f"Status:\n• Token: {has_token}\n• Cart: {has_cart}\n• Answers: `{', '.join(user_ans)}`\n\n"
                                "Select an option below:"
                            )
                            
                            # Construct Inline Keyboard embedded inside the chat message bubble
                            keyboard_buttons = [
                                [{"text": "🔄 Refresh Status", "callback_data": "btn_status"}]
                            ]
                            if is_admin:
                                keyboard_buttons.append([
                                    {"text": "⏸️ Pause Bridge", "callback_data": "btn_pause"},
                                    {"text": "▶️ Resume Bridge", "callback_data": "btn_resume"}
                                ])
                                
                            reply_markup = {"inline_keyboard": keyboard_buttons}
                            await send_telegram(chat_id, help_text, reply_markup=reply_markup)
                        elif text.startswith("/settoken"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                tokens = load_json(TOKEN_FILE)
                                tokens[chat_str] = sanitize_token(parts[1])
                                save_json(TOKEN_FILE, tokens)
                                await send_telegram(chat_id, "✅ Token saved.")
                        elif text.startswith("/setcart"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                carts = load_json(CART_FILE)
                                carts[chat_str] = {"cart_id": parts[1].strip()}
                                save_json(CART_FILE, carts)
                                await send_telegram(chat_id, "✅ Cart ID saved.")
                        elif text.startswith("/setanswer"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                ans_list = [a.strip() for a in re.split(r'[,|/]', parts[1]) if a.strip()][:3]
                                answers = load_json(ANSWER_FILE)
                                answers[chat_str] = ans_list
                                save_json(ANSWER_FILE, answers)
                                await send_telegram(chat_id, f"✅ Security Answers Saved: `{', '.join(ans_list)}`")
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
