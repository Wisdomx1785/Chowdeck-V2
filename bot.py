import re
import asyncio

async def process_incoming_message(raw_text: str):
    print(f"Scanning incoming text: {raw_text}")
    
    # Regex pattern to match uppercase alphanumeric codes typically 5 to 12 characters long
    pattern = r'\b[A-Z0-9]{5,12}\b'
    found_codes = re.findall(pattern, raw_text)
    
    # Filter out common common words to prevent false alarms
    ignore_words = {"CHOWDECK", "TODAY", "ORDER", "DELIVERY", "FREE", "LUNCH", "ALL"}
    valid_codes = [code for code in found_codes if code not in ignore_words]
    
    if valid_codes:
        for code in valid_codes:
            print(f"🚨 Target Code Isolated: {code}")
            await claim_voucher(code)
    else:
        print("No valid voucher pattern found.")

async def claim_voucher(code: str):
    print(f"Firing automated request for code: {code}...")

async def main():
    # Simulation of a live drop like the ones from @LordBinary_
    sample_broadcast = "2KFORTHEMONTH All the best"
    await process_incoming_message(sample_broadcast)

if __name__ == "__main__":
    asyncio.run(main())
