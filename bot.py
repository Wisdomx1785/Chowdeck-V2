import re
import asyncio
import os
import httpx
from telegram import Update
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters

CHOWDECK_TOKEN = os.getenv("CHOWDECK_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_BASE_URL = "https://api.chowdeck.com/customer/promo-code"

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Chow Sniper Bot is active and locked onto the channel!")

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
    
    async httpx.AsyncClient() as client:
        try:
            response = await client.post(url, headers=headers, timeout=5.0)
            print(f"Response Status: {response.status_code} | Body: {response.text}")
        except Exception as e:
            print(f"Request failed: {e}")

async def handle_telegram_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.channel_post
    if msg:
        message_text = msg.text or msg.caption
        if message_text:
            await process_incoming_message(message_text)

def main():
    if not TELEGRAM_BOT_TOKEN:
        print("Error: TELEGRAM_BOT_TOKEN is missing!")
        return

    print("🤖 Starting Telegram Listener Worker...")
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(MessageHandler((filters.TEXT | filters.CAPTION) & (~filters.COMMAND), handle_telegram_message))
    
    # Drop pending updates on startup to prevent conflict loops
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
