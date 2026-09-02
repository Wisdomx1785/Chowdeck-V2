import re
import asyncio
import os
import httpx
from fastapi import FastAPI, Request
import uvicorn
import threading
import feedparser
import time

app = FastAPI()

CHOWDECK_TOKEN = os.getenv("CHOWDECK_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_BASE_URL = "https://api.chowdeck.com/customer/promo-code"
RSS_URL = "https://rsshub.app/twitter/user/Oyinlola6464"

seen_ids = set()

async def claim_voucher(code: str):
    if not CHOWDECK_TOKEN:
        print("Error: CHOWDECK_TOKEN is missing!")
        return

    url = f"{API_BASE_URL}/{code}"
    headers = {
        "Authorization": f"Bearer {CHOWDECK_TOKEN}",
        "Content-Type": "application/json",
        "Origin": "https://chowdeck.com",
        "Referer": "https://chowdeck.com/"
    }

    print(f"⚡ Firing auto-claim request for code: {code}...")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, headers=headers, timeout=5.0)
            print(f"Response Status: {response.status_code} | Body: {response.text}")
        except Exception as e:
            print(f"Request failed: {e}")

async def process_incoming_text(raw_text: str):
    print(f"Scanning text: {raw_text}")
    
    pattern = r'\b[A-Z0-9]{5,12}\b'
    found_codes = re.findall(pattern, raw_text)
    
    ignore_words = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    valid_codes = [code for code in found_codes if code not in ignore_words]
    
    if valid_codes:
        for code in valid_codes:
            print(f"🚨 Target Code Isolated: {code}")
            await claim_voucher(code)

# Endpoint for generic external webhooks
@app.post("/webhook")
async def receive_webhook(request: Request):
    data = await request.json()
    text = data.get("text", "")
    if text:
        await process_incoming_text(text)
    return {"status": "received"}

# Native Telegram Webhook endpoint (handles multi-user inputs instantly)
@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    data = await request.json()
    
    # Extract message or channel post from incoming Telegram payload
    message = data.get("message") or data.get("channel_post")
    if message:
        text = message.get("text") or message.get("caption")
        if text:
            # Handle start command for multi-user onboarding
            if text.startswith("/start"):
                chat_id = message["chat"]["id"]
                await send_telegram_reply(chat_id, "🤖 Multi-user Chowdeck Sniper active! Send or forward codes here to auto-claim them.")
            else:
                await process_incoming_text(text)
                
    return {"status": "ok"}

async def send_telegram_reply(chat_id: int, text: str):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    async with httpx.AsyncClient() as client:
        try:
            await client.post(url, json=payload)
        except Exception as e:
            print(f"Failed to reply to Telegram user: {e}")

@app.get("/")
def home():
    return {"status": "Multi-user Sniper & Telegram Webhook are live!"}

def initialize_rss_cache():
    try:
        feed = feedparser.parse(RSS_URL)
        for entry in feed.entries:
            tweet_id = entry.get("id") or entry.get("link")
            seen_ids.add(tweet_id)
        print("RSS sniper cache initialized successfully.")
    except Exception as e:
        print(f"Error initializing RSS cache: {e}")

def run_rss_sniper():
    initialize_rss_cache()
    while True:
        try:
            feed = feedparser.parse(RSS_URL)
            for entry in reversed(feed.entries):
                tweet_id = entry.get("id") or entry.get("link")
                
                if tweet_id not in seen_ids:
                    seen_ids.add(tweet_id)
                    tweet_text = entry.get("title", "") or entry.get("description", "")
                    
                    print(f"New tweet detected: {tweet_text}")
                    asyncio.run(process_incoming_text(tweet_text))
                    
        except Exception as e:
            print(f"Error checking RSS feed: {e}")
            
        time.sleep(60)

if __name__ == "__main__":
    # Start RSS sniper in a background thread
    rss_thread = threading.Thread(target=run_rss_sniper, daemon=True)
    rss_thread.start()
    
    # Run FastAPI web server on Railway port
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
