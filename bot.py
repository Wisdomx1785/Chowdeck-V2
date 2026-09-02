import re
import asyncio
import os
import httpx
from fastapi import FastAPI, Request
import uvicorn
import threading
import time

app = FastAPI()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
X_AUTH_TOKEN = os.getenv("X_AUTH_TOKEN")
API_BASE_URL = "https://api.chowdeck.com/customer/promo-code"
TARGET_SCREEN_NAME = "LordBinary_"

seen_ids = set()
user_tokens = {}  # Maps Telegram chat_id -> User's Chowdeck Bearer Token
active_chats = set()

async def broadcast_result_to_users(code: str, results: dict):
    if not TELEGRAM_BOT_TOKEN:
        return
    
    async with httpx.AsyncClient() as client:
        for chat_id, (status_code, response_text) in results.items():
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            msg = f"🎯 *Code Target:* `{code}`\nStatus: `{status_code}`\nResponse: `{response_text}`"
            payload = {"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"}
            try:
                await client.post(url, json=payload, timeout=3.0)
            except Exception as e:
                print(f"Failed to notify user {chat_id}: {e}")

async def claim_voucher_for_all_users(code: str):
    if not user_tokens:
        print("No user tokens registered yet!")
        return

    print(f"⚡ Firing multi-user claims for code: {code}...")
    headers_template = {
        "Content-Type": "application/json",
        "Origin": "https://chowdeck.com",
        "Referer": "https://chowdeck.com/"
    }

    claim_results = {}
    async with httpx.AsyncClient() as client:
        for chat_id, token in user_tokens.items():
            headers = headers_template.copy()
            headers["Authorization"] = f"Bearer {token}"
            url = f"{API_BASE_URL}/{code}"
            
            try:
                response = await client.post(url, headers=headers, timeout=5.0)
                claim_results[chat_id] = (response.status_code, response.text)
            except Exception as e:
                claim_results[chat_id] = (500, str(e))

    await broadcast_result_to_users(code, claim_results)

async def process_incoming_text(raw_text: str):
    print(f"Scanning tweet: {raw_text}")
    pattern = r'\b[A-Z0-9]{5,12}\b'
    found_codes = re.findall(pattern, raw_text)
    
    ignore_words = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    valid_codes = [code for code in found_codes if code not in ignore_words]
    
    if valid_codes:
        for code in valid_codes:
            print(f"🚨 Target Code Isolated: {code}")
            await claim_voucher_for_all_users(code)

@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    message = data.get("message") or data.get("channel_post")
    
    if message:
        chat_id = message["chat"]["id"]
        active_chats.add(chat_id)
        text = message.get("text") or message.get("caption", "")
        
        if text:
            if text.startswith("/start"):
                reply = (
                    "🤖 *Multi-User Chowdeck Sniper Active!*\n\n"
                    "To link your personal Chowdeck account, send your Bearer token like this:\n"
                    "`/settoken YOUR_BEARER_TOKEN_HERE`"
                )
                await send_telegram_message(chat_id, reply)
                
            elif text.startswith("/settoken"):
                parts = text.split(" ", 1)
                if len(parts) > 1:
                    token = parts[1].strip()
                    user_tokens[chat_id] = token
                    await send_telegram_message(chat_id, "✅ Your Chowdeck token has been securely mapped to your chat session!")
                else:
                    await send_telegram_message(chat_id, "⚠️ Please provide your token: `/settoken <token>`")
            else:
                await process_incoming_text(text)
                
    return {"status": "ok"}

async def send_telegram_message(chat_id: int, text: str):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json=payload)
        except Exception as e:
            print(f"Telegram reply failed: {e}")

@app.get("/")
def home():
    return {"status": "Authenticated X-Cookie Sniper Engine online."}

def run_authenticated_x_sniper():
    url = f"https://syndication.twitter.com/service/iframe/profile?screen_name={TARGET_SCREEN_NAME}"
    cookies = {"auth_token": X_AUTH_TOKEN} if X_AUTH_TOKEN else {}
    
    while True:
        try:
            with httpx.Client(headers={"User-Agent": "Mozilla/5.0"}, cookies=cookies, timeout=10.0) as client:
                response = client.get(url)
                if response.status_code == 200:
                    html_content = response.text
                    tweets = re.findall(r'class="Tweet-text"[^>]*>(.*?)<\/p>', html_content, re.DOTALL)
                    for tweet in tweets:
                        clean_tweet = re.sub(r'<[^>]+>', '', tweet)
                        tweet_id = clean_tweet[:30]
                        
                        if tweet_id not in seen_ids:
                            seen_ids.add(tweet_id)
                            asyncio.run(process_incoming_text(clean_tweet))
        except Exception as e:
            print(f"Polling error: {e}")
            
        time.sleep(5)

if __name__ == "__main__":
    sniper_thread = threading.Thread(target=run_authenticated_x_sniper, daemon=True)
    sniper_thread.start()
    
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
