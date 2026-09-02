import re
import asyncio
import os
import httpx

# Retrieve the token from environment variables
CHOWDECK_TOKEN = os.getenv("CHOWDECK_TOKEN")
API_BASE_URL = "https://api.chowdeck.com/customer/promo-code"

async def process_incoming_message(raw_text: str):
    print(f"Scanning incoming text: {raw_text}")
    
    # Regex pattern to match uppercase alphanumeric codes typically 5 to 12 characters long
    pattern = r'\b[A-Z0-9]{5,12}\b'
    found_codes = re.findall(pattern, raw_text)
    
    # Filter out common false alarms
    ignore_words = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    valid_codes = [code for code in found_codes if code not in ignore_words]
    
    if valid_codes:
        for code in valid_codes:
            print(f"🚨 Target Code Isolated: {code}")
            await claim_voucher(code)
    else:
        print("No valid voucher pattern found.")

async def claim_voucher(code: str):
    if not CHOWDECK_TOKEN:
        print("Error: CHOWDECK_TOKEN environment variable is missing!")
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
            # High-speed POST request to claim the voucher
            response = await client.post(url, headers=headers, timeout=5.0)
            print(f"Response Status: {response.status_code}")
            print(f"Response Body: {response.text}")
        except Exception as e:
            print(f"Request failed: {e}")

async def main():
    # Simulation of a live drop to test execution
    sample_broadcast = "2KFORTHEMONTH All the best"
    await process_incoming_message(sample_broadcast)

if __name__ == "__main__":
    asyncio.run(main())
