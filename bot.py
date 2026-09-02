import re
import asyncio
import os
import httpx

CHOWDECK_TOKEN = os.getenv("CHOWDECK_TOKEN")
API_BASE_URL = "https://api.chowdeck.com/customer/promo-code"

async def process_incoming_message(raw_text: str):
    print(f"Scanning text: {raw_text}")
    
    # Regex pattern to match uppercase alphanumeric codes (5 to 12 characters)
    pattern = r'\b[A-Z0-9]{5,12}\b'
    found_codes = re.findall(pattern, raw_text)
    
    ignore_words = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    valid_codes = [code for code in found_codes if code not in ignore_words]
    
    if valid_codes:
        for code in valid_codes:
            print(f"🚨 Target Code Isolated: {code}")
            await claim_voucher(code)

async def claim_voucher(code: str):
    if not CHOWDECK_TOKEN:
        print("Error: CHOWDECK_TOKEN is missing in environment variables!")
        return

    url = f"{API_BASE_URL}/{code}"
    headers = {
        "Authorization": f"Bearer {CHOWDECK_TOKEN}",
        "Content-Type": "application/json",
        "Origin": "https://chowdeck.com",
        "Referer": "https://chowdeck.com/"
    }

    print(f"Firing auto-claim request for code: {code}...")
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, headers=headers, timeout=5.0)
            print(f"Response Status: {response.status_code}")
            print(f"Response Body: {response.text}")
        except Exception as e:
            print(f"Request failed: {e}")

async def background_worker():
    print("🤖 Chowdeck Auto-Claim Worker is active and listening...")
    
    # This loop keeps your Railway worker alive 24/7
    while True:
        # Here is where your live text listener or polling check will feed new strings into process_incoming_message()
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(background_worker())
