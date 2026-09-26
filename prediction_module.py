import httpx
import logging

logger = logging.getLogger(__name__)

async def probe_prediction_routes(token: str, proxy_url: str = None):
    """
    Diagnostic probe to discover Chowdeck's mobile app prediction and bill endpoints.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Linux; Android 14; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    }
    
    # Common endpoint paths used by mobile apps for games, rewards, and bills
    test_endpoints = [
        "https://api.chowdeck.com/customer/match-predictions",
        "https://api.chowdeck.com/customer/predictions",
        "https://api.chowdeck.com/customer/games/predict",
        "https://api.chowdeck.com/customer/bills/airtime",
        "https://api.chowdeck.co.uk/customer/predictions"
    ]
    
    results = {}
    async with httpx.AsyncClient(timeout=6.0, proxy=proxy_url) as client:
        for url in test_endpoints:
            try:
                # We test a GET or a light POST to see how the server responds
                res = await client.get(url, headers=headers)
                results[url] = {
                    "status": res.status_code,
                    "response": res.json() if res.status_code < 400 else res.text[:200]
                }
            except Exception as e:
                results[url] = {"status": "error", "message": str(e)}
                
    return results

async def submit_match_prediction(token: str, home_score: int, away_score: int, phone_number: str, proxy_url: str = None):
    """
    Executes the prediction submission combined with the ₦100 airtime checkout.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Linux; Android 14; Mobile) AppleWebKit/537.36"
    }
    
    # Payload structure mapped from your app inspection screenshots
    payload = {
        "bill_type": "airtime",
        "provider": "MTN",
        "phone_number": phone_number,
        "amount": 100,
        "prediction": {
            "home_score": home_score,
            "away_score": away_score
        },
        "payment_method": "wallet"
    }
    
    # Target route (will be updated dynamically once the probe confirms the exact path)
    endpoint = "https://api.chowdeck.com/customer/bills/airtime" 
    
    async with httpx.AsyncClient(timeout=8.0, proxy=proxy_url) as client:
        try:
            res = await client.post(endpoint, headers=headers, json=payload)
            return {
                "status_code": res.status_code,
                "data": res.json() if res.content else {"message": "No response body"}
            }
        except Exception as e:
            return {"status_code": 500, "error": str(e)}
