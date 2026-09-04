import re
import asyncio
import os
import json
import unicodedata
import httpx

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PROXY_URL = os.getenv("PROXY_URL", "").strip()
TARGET_SCREEN_NAME = "Oyinlola6464"

TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tokens.json")
CART_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cart.json")
COOKIE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cookies_db.json")
CLAIMED_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "claimed.json")

seen_tweet_ids = set()
last_update_id = 0
code_queue = asyncio.Queue()

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
    with httpx.Client(timeout=15.0, proxies=PROXY_URL if PROXY_URL else None) as client:
        if method == "POST":
            return client.post(url, json=json_data, headers=headers, cookies=cookies)
        return client.get(url, headers=headers, cookies=cookies)

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
    
    print(f"🚀 Processing code '{code}' for {len(tokens)} registered user(s)...")
    for chat_id, token in tokens.items():
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
        
        user_cookies = cookie_db.get(str(chat_id), {})
        
        try:
            claim_url = f"https://api.chowdeck.com/customer/promo-code/{code}"
            print(f"📤 Executing isolated request for {code} (User {chat_id})...")
            
            claim_res = await asyncio.to_thread(
                execute_chowdeck_request, "POST", claim_url, headers, user_cookies, {"code": code}
            )
            print(f"📥 Claim status [{claim_res.status_code}]: {claim_res.text}")
            
            if claim_res.status_code in [200, 400]:
                claimed_history[code] = claim_res.status_code
                save_json(CLAIMED_FILE, claimed_history)
            
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
                print(f"📥 Apply result [{apply_res.status_code}]: {apply_res.text}")
                msg += f"\nCart Apply Status: `{apply_res.status_code}`\nResponse: `{apply_res.text}`"
            
            await send_telegram(chat_id, msg)
            await asyncio.sleep(1.0)
        except Exception as e:
            print(f"❌ Error processing code {code} for user {chat_id}: {e}")
            try:
                await send_telegram(chat_id, f"🎯 *Code Target:* `{code}`\nError: `{e}`")
            except:
                pass

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
                            
                            help_text = (
                                "🤖 *Chowdeck Sniper Active!*\n\n"
                                f"Your Status:\n• Token: {has_token}\n• Cart: {has_cart}\n\n"
                                "1️⃣ Link token:\n`/settoken <token_or_json>`\n\n"
                                "2️⃣ Set cart ID:\n`/setcart YOUR_CART_ID`"
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
                        else:
                            await process_text(text)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"Telegram poller error: {e}")
            await asyncio.sleep(2)

async def robust_poller():
    syndication_url = f"https://cdn.syndication.twimg.com/timeline/profile?screen-name={TARGET_SCREEN_NAME}&suppress_response_codes=true"
    rss_bridges = [
        f"https://api.rss2json.com/v1/api.json?rss_url=https%3A%2F%2Fnitter.poast.org%2F{TARGET_SCREEN_NAME}%2Frss",
        f"https://api.rss2json.com/v1/api.json?rss_url=https%3A%2F%2Fnitter.privacydev.net%2F{TARGET_SCREEN_NAME}%2Frss"
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://platform.twitter.com",
        "Referer": "https://platform.twitter.com/"
    }
    
    print(f"🚀 Robust multi-source poller active for @{TARGET_SCREEN_NAME}...", flush=True)
    
    async with httpx.AsyncClient(proxy=PROXY_URL if PROXY_URL else None, follow_redirects=True) as client:
        # Initial baseline
        baseline_set = False
        try:
            res = await client.get(syndication_url, headers=headers, timeout=12.0)
            if res.status_code == 200 and "application/json" in res.headers.get("content-type", ""):
                data = res.json()
                for tweet in data.get("tweets", []):
                    tid = str(tweet.get("id_str") or tweet.get("id"))
                    if tid:
                        seen_tweet_ids.add(tid)
                baseline_set = True
                print(f"✅ Baseline locked via Syndication. Absorbed {len(seen_tweet_ids)} tweets.")
        except Exception:
            pass

        if not baseline_set:
            for bridge in rss_bridges:
                try:
                    res = await client.get(bridge, headers={"User-Agent": "Mozilla/5.0"}, timeout=10.0)
                    if res.status_code == 200:
                        data = res.json()
                        if data.get("status") == "ok":
                            for item in data.get("items", []):
                                tid = str(item.get("guid") or item.get("link"))
                                if tid:
                                    seen_tweet_ids.add(tid)
                            print(f"✅ Baseline locked via RSS Fallback. Absorbed {len(seen_tweet_ids)} tweets.")
                            break
                except Exception:
                    continue

        while True:
            success = False
            # Try Syndication JSON first
            try:
                res = await client.get(syndication_url, headers=headers, timeout=10.0)
                if res.status_code == 200 and "application/json" in res.headers.get("content-type", ""):
                    data = res.json()
                    for tweet in data.get("tweets", []):
                        tid = str(tweet.get("id_str") or tweet.get("id"))
                        text = tweet.get("text", "")
                        if tid and text and tid not in seen_tweet_ids:
                            seen_tweet_ids.add(tid)
                            print(f"🚨 Live Tweet Detected via Syndication!")
                            await process_text(text)
                    success = True
            except Exception:
                pass

            # Fallback to RSS bridges if syndication failed
            if not success:
                for bridge in rss_bridges:
                    try:
                        res = await client.get(bridge, headers={"User-Agent": "Mozilla/5.0"}, timeout=8.0)
                        if res.status_code == 200:
                            data = res.json()
                            if data.get("status") == "ok":
                                for item in data.get("items", []):
                                    tid = str(item.get("guid") or item.get("link"))
                                    text = item.get("title", "").strip()
                                    if tid and text and tid not in seen_tweet_ids:
                                        seen_tweet_ids.add(tid)
                                        print(f"🚨 Live Tweet Detected via RSS Fallback!")
                                        await process_text(text)
                                success = True
                                break
                    except Exception:
                        continue

            if not success:
                print("⚠️ All tweet fetch channels returned errors this cycle. Retrying...")

            await asyncio.sleep(15)

async def main():
    print("🤖 Waiting 30 seconds for old session to expire...", flush=True)
    await asyncio.sleep(30)
    
    worker_task = asyncio.create_task(worker())
    try:
        asyncio.gather(telegram_poller(), robust_poller())
        while True:
            await asyncio.sleep(3600)
    finally:
        worker_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped gracefully.")
