import re
import asyncio
import os
import httpx
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters
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

async def process_incoming_message(raw_text: str):
    print(f"Scanning text: {raw_text}")
    
    pattern = r'\b[A-Z0-9]{5,12}\b'
    found_codes = re.findall(pattern, raw_text)
    
    ignore_words = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    valid_codes = [code for code in found_codes if code not in ignore_words]
    
    if valid_codes:
        for code in valid_codes:
            print(f"🚨 Target Code Isolated: {code}")
            await claim_voucher(code)

@app.post("/webhook")
async def receive_webhook(request: Request):
    data = await request.json()
    text = data.get("text", "")
    if text:
        await process_incoming_message(text)
    return {"status": "received"}

async def handle_telegram_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.channel_post
    if msg:
        message_text = msg.text or msg.caption
        if message_text:
            await process_incoming_message(message_text)

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
                    asyncio.run(process_incoming_message(tweet_text))
                    
        except Exception as e:
            print(f"Error checking RSS feed: {e}")
            
        time.sleep(60)

def run_telegram_bot():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN is missing!")
        return

    tg_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", lambda u, c: u.message.reply_text("🤖 Webhook, Telegram & RSS Sniper Worker active!")))
    tg_app.add_handler(MessageHandler((filters.TEXT | filters.CAPTION) & (~filters.COMMAND), handle_telegram_message))
    
    print("🤖 Starting Telegram Polling Loop...")
    tg_app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    # Start RSS sniper in background thread
    rss_thread = threading.Thread(target=run_rss_sniper, daemon=True)
    rss_thread.start()

    # Start FastAPI in background thread so main thread can safely handle Telegram polling without signal collision
    def run_fastapi():
        port = int(os.environ.get("PORT", 8080))
        uvicorn.run(app, host="0.0.0.0", port=port)

    web_thread = threading.Thread(target=run_fastapi, daemon=True)
    web_thread.start()
    
    # Run Telegram bot in the main thread
    run_telegram_bot()
