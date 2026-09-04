import os
import json
import time
import threading
import requests
import telebot

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
bot = telebot.TeleBot(TOKEN)

# Persistent file storage so restarts don't wipe tokens & cart IDs
USER_DATA_FILE = "users.json"

def load_users():
    if os.path.exists(USER_DATA_FILE):
        try:
            with open(USER_DATA_FILE, "r") as f:
                return {int(k): v for k, v in json.load(f).items()}
        except Exception:
            return {}
    return {}

def save_users():
    try:
        with open(USER_DATA_FILE, "w") as f:
            json.dump(user_data, f)
    except Exception as e:
        print(f"Error saving users: {e}", flush=True)

user_data = load_users()

@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    chat_id = message.chat.id
    has_token = chat_id in user_data and user_data[chat_id].get("token")
    has_cart = chat_id in user_data and user_data[chat_id].get("cart_id")
    
    status_token = "✅ Configured" if has_token else "❌ Not Set"
    status_cart = "✅ Configured" if has_cart else "❌ Not Set"
    
    text = (
        f"🤖 *Multi-User Chowdeck Sniper Active!*\n\n"
        f"Your Status:\n"
        f"• Token: {status_token}\n"
        f"• Cart: {status_cart}\n\n"
        f"1️⃣ Link token:\n`/settoken <token_or_json>`\n\n"
        f"2️⃣ Set cart ID:\n`/setcart YOUR_CART_ID`"
    )
    bot.send_message(chat_id, text, parse_mode="Markdown")

@bot.message_handler(commands=['settoken'])
def set_token(message):
    chat_id = message.chat.id
    raw_input = message.text.replace('/settoken', '').strip()
    
    if not raw_input:
        bot.send_message(chat_id, "❌ Please provide your token or JSON string.")
        return
        
    try:
        if raw_input.startswith('{'):
            parsed = json.loads(raw_input)
            bearer_token = parsed.get("token")
        else:
            bearer_token = raw_input
            
        if not bearer_token:
            bot.send_message(chat_id, "❌ Invalid token format.")
            return
            
        if chat_id not in user_data:
            user_data[chat_id] = {}
        user_data[chat_id]["token"] = bearer_token
        save_users()
        bot.send_message(chat_id, "✅ Success! Token saved and persisted.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Error parsing token: {e}")

@bot.message_handler(commands=['setcart'])
def set_cart(message):
    chat_id = message.chat.id
    cart_id = message.text.replace('/setcart', '').strip()
    
    if not cart_id.isdigit():
        bot.send_message(chat_id, "❌ Cart ID must be numbers only.")
        return
        
    if chat_id not in user_data:
        user_data[chat_id] = {}
    user_data[chat_id]["cart_id"] = cart_id
    save_users()
    bot.send_message(chat_id, f"✅ Cart ID Saved & Persisted: {cart_id}")

def poll_twitter_feed():
    target_user = "Oyinlola6464"
    bridges = [
        f"https://api.rss2json.com/v1/api.json?rss_url=https%3A%2F%2Fnitter.privacydev.net%2F{target_user}%2Frss",
        f"https://api.rss2json.com/v1/api.json?rss_url=https%3A%2F%2Fnitter.poast.org%2F{target_user}%2Frss",
        f"https://api.rss2json.com/v1/api.json?rss_url=https%3A%2F%2Fnitter.net%2F{target_user}%2Frss"
    ]
    last_guid = None
    
    print(f"🚀 X poller active for @{target_user}...", flush=True)
    
    while True:
        tweet_text = None
        tweet_id = None
        success = False
        
        for url in bridges:
            try:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                res = requests.get(url, headers=headers, timeout=6)
                if res.status_code == 200:
                    data = res.json()
                    if data.get("status") == "ok":
                        items = data.get("items", [])
                        if items:
                            latest = items[0]
                            tweet_id = str(latest.get("guid") or latest.get("link"))
                            tweet_text = latest.get("title", "").strip()
                            success = True
                            break
            except Exception:
                continue
        
        if success and tweet_text and tweet_id:
            if last_guid is None:
                last_guid = tweet_id
                print(f"📌 Initialized with latest tweet ID: {tweet_id}", flush=True)
            elif tweet_id != last_guid:
                last_guid = tweet_id
                print(f"🚨 New Tweet Detected: {tweet_text}", flush=True)
                
                promo_code = tweet_text.split()[0] if tweet_text else ""
                
                for chat_id, creds in user_data.items():
                    token = creds.get("token")
                    cart_id = creds.get("cart_id")
                    if token and cart_id and promo_code:
                        print(f"🛒 Applying code '{promo_code}' to cart {cart_id}...", flush=True)
                        
                        api_url = "https://api.chowdeck.com/customer/cart/promo-code"
                        api_headers = {
                            "Authorization": f"Bearer {token}",
                            "Content-Type": "application/json",
                            "Accept": "application/json",
                            "Origin": "https://chowdeck.com",
                            "Referer": "https://chowdeck.com/"
                        }
                        payload = {
                            "code": promo_code,
                            "cart_id": int(cart_id)
                        }
                        
                        try:
                            api_res = requests.post(api_url, headers=api_headers, json=payload, timeout=10)
                            print(f"📡 Chowdeck Response [{api_res.status_code}]: {api_res.text}", flush=True)
                            bot.send_message(
                                chat_id, 
                                f"🎯 Code: `{promo_code}`\nStatus: `{api_res.status_code}`\nResponse: `{api_res.text}`", 
                                parse_mode="Markdown"
                            )
                        except Exception as api_err:
                            print(f"❌ API Request Failed: {api_err}", flush=True)
        else:
            print("⚠️ All RSS bridges failed or timed out this cycle. Retrying...", flush=True)
            
        time.sleep(15)

if __name__ == "__main__":
    print("🤖 Container started. Waiting 35 seconds for Telegram old session to drop...", flush=True)
    time.sleep(35)
    
    poller_thread = threading.Thread(target=poll_twitter_feed, daemon=True)
    poller_thread.start()
    
    print("🤖 Telegram Bot polling started...", flush=True)
    bot.remove_webhook()
    
    while True:
        try:
            bot.infinity_polling(skip_pending=True, timeout=60, long_polling_timeout=60)
        except Exception as e:
            print(f"⚠️ Telegram Polling Error: {e}. Retrying in 10 seconds...", flush=True)
            time.sleep(10)
