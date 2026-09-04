import re
import asyncio
import os
import json
import unicodedata
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import httpx

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()

TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tokens.json")
CART_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cart.json")
COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies_db.json")
CLAIMED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "claimed.json")
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_state.json")

last_update_id = 0
code_queue = asyncio.Queue()
loop_ref = None

def load_json(file_path):
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Error reading {file_path}: {e}")
            return {}
    return {}

def save_json(file_path, data):
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=4)
    except Exception as e:
        print(f"❌ Failed to save {file_path}: {e}")

def get_bot_active():
    state = load_json(STATE_FILE)
    return state.get("active", True)

def set_bot_active(val):
    save_json(STATE_FILE, {"active": val})

def sanitize_token(raw_input):
    cleaned = raw_input.strip()
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

async def send_telegram(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(proxy=PROXY_URL if PROXY_URL else None) as temp_client:
            await temp_client.post(url, json={"chat_id": int(chat_id), "text": text, "parse_mode": "Markdown"}, timeout=15.0)
    except Exception as e:
        print(f"Telegram send error to {chat_id}: {e}")

def execute_chowdeck_request(method, url, headers, cookies, json_data):
    with httpx.Client(timeout=15.0, proxy=PROXY_URL if PROXY_URL else None) as client:
        if method == "POST":
            return client.post(url, json=json_data, headers=headers, cookies=cookies)
        return client.get(url, headers=headers, cookies=cookies)

async def claim_for_single_user(chat_id, token, code, user_cookies, carts):
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
    print(f"📤 Firing parallel request for {code} (User {chat_id})...")
    
    try:
        claim_res = await asyncio.to_thread(
            execute_chowdeck_request, "POST", claim_url, headers, user_cookies, {"code": code}
        )
        
        if claim_res.status_code != 200:
            CANDIDATE_ANSWERS = ["Ismail", "Chowstore", "Chowpass", "Chowstar", "Relay", "Chowmart"]
            for ans in CANDIDATE_ANSWERS:
                claim_res = await asyncio.to_thread(
                    execute_chowdeck_request, "POST", claim_url, headers, user_cookies, {"code": code, "answer": ans}
                )
                if claim_res.status_code == 200:
                    print(f"✅ User {chat_id} cleared security with answer: '{ans}'!")
                    break
                await asyncio.sleep(0.05)

        print(f"📥 User {chat_id} Claim status [{claim_res.status_code}]: {claim_res.text}")
        
        msg = f"🎯 *Code Target:* `{code}`\nStatus: `{claim_res.status_code}`\nResponse: `{claim_res.text}`"
        
        user_cart = carts.get(str(chat_id))
        if user_cart and user_cart.get("cart_id") and claim_res.status_code == 200:
            payload = {
                "cart_id": user_cart.get("cart_id"),
                "promo_codes": [code]
            }
            apply_res = await asyncio.to_thread(
                execute_chowdeck_request, "POST", "https://api.chowdeck.com/customer/discount/apply", headers, user_cookies, payload
            )
            msg += f"\nCart Apply Status: `{apply_res.status_code}`\nResponse: `{apply_res.text}`"
        
        await send_telegram(chat_id, msg)
        return claim_res.status_code
    except Exception as e:
        print(f"❌ Error for user {chat_id}: {e}")
        return 500

async def claim_code(raw_code):
    code = robust_clean_code(raw_code)
    if not code or len(code) < 4:
        return

    claimed_history = load_json(CLAIMED_FILE)
    if code in claimed_history:
        print(f"⏩ Skipping '{code}': Already attempted/claimed previously.")
        return

    tokens = load_json(TOKEN_FILE)
    carts = load_json(CART_FILE)
    cookie_db = load_json(COOKIE_FILE)
    
    if not tokens:
        print("❌ Database check: No user tokens registered yet.")
        return
    
    print(f"🚀 Executing parallel broadcast for code '{code}' across {len(tokens)} user(s)...")
    
    tasks = [
        claim_for_single_user(chat_id, token, code, cookie_db.get(str(chat_id), {}), carts)
        for chat_id, token in tokens.items()
    ]
    
    results = await asyncio.gather(*tasks)
    
    if results:
        claimed_history[code] = max(results)
        save_json(CLAIMED_FILE, claimed_history)

async def worker():
    while True:
        try:
            code = await code_queue.get()
            try:
                await claim_code(code)
            except Exception as e:
                print(f"Worker execution error for {code}: {e}")
            finally:
                code_queue.task_done()
                await asyncio.sleep(2.5)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Worker loop error: {e}")
            await asyncio.sleep(2)

async def process_text(text):
    if not text:
        return
    print(f"Scanning text: {text}")
    pattern = r'\b(?=[A-Z0-9]*[A-Z])[A-Z0-9]{5,12}\b'
    codes = re.findall(pattern, text)
    ignore = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    
    valid = []
    for c in codes:
        cleaned = robust_clean_code(c)
        if cleaned and cleaned not in ignore and not cleaned.startswith("TEST"):
            valid.append(cleaned)
            
    for code in list(dict.fromkeys(valid)):
        print(f"🚨 Queued Code: {code}")
        await code_queue.put(code)

class WebhookHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/status":
            try:
                active = get_bot_active()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"active": active}).encode('utf-8'))
            except Exception as e:
                print(f"Status check error: {e}")
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
            print(f"Webhook error: {e}")
            self.send_response(400)
            self.end_headers()

    def log_message(self, format, *args):
        return

def run_http_server():
    server = HTTPServer(('0.0.0.0', 8080), WebhookHandler)
    print("🌐 Railway HTTP webhook server running on port 8080...")
    server.serve_forever()

async def telegram_poller():
    global last_update_id
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    async with httpx.AsyncClient(proxy=PROXY_URL if PROXY_URL else None) as tg_client:
        while True:
            try:
                res = await tg_client.get(url, params={"offset": last_update_id + 1, "timeout": 30}, timeout=35.0)
                if res.status_code == 200:
                    data = res.json()
                    for update in data.get("result", []):
                        last_update_id = update["update_id"]
                        msg = update.get("message", {})
                        chat_id = msg.get("chat", {}).get("id")
                        text = msg.get("text", "")
                        if not chat_id or not text:
                            continue
                        
                        if text.startswith("/start"):
                            tokens = load_json(TOKEN_FILE)
                            carts = load_json(CART_FILE)
                            has_token = "✅ Configured" if str(chat_id) in tokens else "❌ Not Set"
                            has_cart = "✅ Configured" if str(chat_id) in carts else "❌ Not Set"
                            bot_status = "🟢 Active" if get_bot_active() else "⏸️ Paused"
                            
                            help_text = (
                                "🤖 *Chowdeck Sniper Active!*\n\n"
                                f"Your Status:\n• Token: {has_token}\n• Cart: {has_cart}\n• Bridge State: {bot_status}\n\n"
                                "1️⃣ Link token:\n`/settoken <token_or_json>`\n\n"
                                "2️⃣ Set cart ID:\n`/setcart YOUR_CART_ID`"
                            )
                            if not ADMIN_CHAT_ID or str(chat_id) == ADMIN_CHAT_ID:
                                help_text += (
                                    "\n\n3️⃣ Control Termux Bridge:\n"
                                    "• `/pause` - Pause Termux polling\n"
                                    "• `/resume` - Resume Termux polling"
                                )
                            await send_telegram(chat_id, help_text)
                        elif text.startswith("/settoken"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                clean_val = sanitize_token(parts[1])
                                tokens = load_json(TOKEN_FILE)
                                tokens[str(chat_id)] = clean_val
                                save_json(TOKEN_FILE, tokens)
                                await send_telegram(chat_id, "✅ *Success!* Token saved and sanitized.")
                            else:
                                await send_telegram(chat_id, "⚠️ Usage: `/settoken <token>`")
                        elif text.startswith("/setcart"):
                            parts = text.split(" ", 1)
                            if len(parts) > 1:
                                cart_id = parts[1].strip()
                                carts = load_json(CART_FILE)
                                carts[str(chat_id)] = {"cart_id": cart_id}
                                save_json(CART_FILE, carts)
                                await send_telegram(chat_id, f"✅ *Cart ID Saved:* `{cart_id}`")
                            else:
                                await send_telegram(chat_id, "⚠️ Usage: `/setcart <cart_id>`")
                        elif text.startswith("/pause"):
                            if ADMIN_CHAT_ID and str(chat_id) != ADMIN_CHAT_ID:
                                await send_telegram(chat_id, "⛔ *Unauthorized:* You do not have permission to control this bot.")
                            else:
                                set_bot_active(False)
                                await send_telegram(chat_id, "⏸️ *Bridge Paused.* Termux polling is now sleeping to save credits.")
                        elif text.startswith("/resume"):
                            if ADMIN_CHAT_ID and str(chat_id) != ADMIN_CHAT_ID:
                                await send_telegram(chat_id, "⛔ *Unauthorized:* You do not have permission to control this bot.")
                            else:
                                set_bot_active(True)
                                await send_telegram(chat_id, "▶️ *Bridge Resumed.* Termux polling is active and watching.")
                        else:
                            await process_text(text)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"Telegram poller error: {e}")
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
        print("\n🛑 Bot stopped gracefully.")
