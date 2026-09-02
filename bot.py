import re
import asyncio
import os
import httpx
from fastapi import FastAPI, Request
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters
import uvicorn
import threading

app = FastAPI()

CHOWDECK_TOKEN = os.getenv("CHOWDECK_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_BASE_URL = "https://api.chowdeck.com/customer/promo-code"

async def process_incoming_message(raw_text: str):
    print(f"Scanning text: {raw_text}")
    
    # Regex to catch uppercase codes (5 to 12 chars)
    pattern = r'\b[A-Z0-9]{5,12}\b'
    found_codes = re.findall(pattern, raw_text)
    
    ignore_words = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    valid_codes = [code for code in found_codes if code not in ignore_words]
    
    if valid_codes:
        for code in valid_codes:
            print(f"🚨 Target Code Isolated: {code}")
            asyncio.create_task(claim_voucher(code))

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

# Webhook endpoint to receive external text pushes instantly
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

def run_telegram_bot():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN is missing!")
        return

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    tg_app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", lambda u, c: u.message.reply_text("🤖 Webhook & Telegram Sniping Worker active!")))
    tg_app.add_handler(MessageHandler((filters.TEXT | filters.CAPTION) & (~filters.COMMAND), handle_telegram_message))
    
    print("🤖 Starting Telegram Polling Loop...")
    tg_app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    # Start Telegram bot in a background thread so FastAPI can run concurrently
    t = threading.Thread(target=run_telegram_bot, daemon=True)
    t.start()
    
    # Run FastAPI web server for webhooks on Railway port
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
