import re
import asyncio
import os

# Placeholder function to simulate catching a raw message from X or Telegram
async def process_incoming_message(raw_text: str):
    print(f"Received raw text: {raw_text}")
    
    # Extract potential voucher codes using Regular Expressions
    # Looking for uppercase alphanumeric sequences between 5 and 12 characters
    pattern = r'\b[A-Z0-9]{5,12}\b'
    found_codes = re.findall(pattern, raw_text)
    
    # Filter out common false positives that are just regular uppercase words
    ignore_words = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH"}
    valid_codes = [code for code in found_codes if code not in ignore_words]
    
    if valid_codes:
        for code in valid_codes:
            print(f"🚨 Target Code Isolated: {code}")
            # Next step: Trigger the API claim function here
            await claim_voucher(code)
    else:
        print("No valid voucher pattern found.")

async def claim_voucher(code: str):
    # This is where we will hook up the HTTP POST request to the web checkout API
    print(f"Attempting to rush-claim code: {code}...")

# Test simulation loop
async def main():
    sample_broadcast = "FLASH VOUCHER ALERT! Use code CHOWFAST50 right now on your next meal!"
    await process_incoming_message(sample_broadcast)

if __name__ == "__main__":
    asyncio.run(main())
