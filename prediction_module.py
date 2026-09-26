import httpx
import asyncio
import logging

logger = logging.getLogger(__name__)

async def test_single_endpoint(client, url, headers):
    try:
        res = await client.get(url, headers=headers)
        try:
            resp_data = res.json()
        except Exception:
            resp_data = res.text[:150]
        return url, {"status": res.status_code, "response": resp_data}
    except Exception as e:
        return url, {"status": "error", "message": str(e)}

async def probe_prediction_routes(token: str, proxy_url: str = None):
    """
    Concurrently probes Chowdeck prediction and bill endpoints with proxy support.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Linux; Android 14; Mobile) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
    }
    
    test_endpoints = [
        "https://api.chowdeck.com/customer/match-predictions",
        "https://api.chowdeck.com/customer/predictions",
        "https://api.chowdeck.com/customer/games/predict",
        "https://api.chowdeck.com/customer/bills/airtime",
        "https://api.chowdeck.co.uk/customer/predictions"
    ]
    
    async with httpx.AsyncClient(timeout=6.0, proxy=proxy_url if proxy_url else None) as client:
        tasks = [test_single_endpoint(client, url, headers) for url in test_endpoints]
        results_list = await asyncio.gather(*tasks)
        
    return {url: data for url, data in results_list}

async def submit_match_prediction(token: str, home_score: int, away_score: int, phone_number: str, proxy_url: str = None):
    """
    Executes the prediction submission combined with the ₦100 airtime checkout safely.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Linux; Android 14; Mobile) AppleWebKit/537.36"
    }
    
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
    
    endpoint = "https://api.chowdeck.com/customer/bills/airtime" 
    
    async with httpx.AsyncClient(timeout=8.0, proxy=proxy_url if proxy_url else None) as client:
        try:
            res = await client.post(endpoint, headers=headers, json=payload)
            try:
                data = res.json()
            except Exception:
                data = {"message": res.text[:150]}
            return {
                "status_code": res.status_code,
                "data": data
            }
        except Exception as e:
            return {"status_code": 500, "error": str(e)}
