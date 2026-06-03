import time
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, UTC

load_dotenv()

API_KEY = os.getenv('ALPHA_VANTAGE_KEY')

STOCKS = [
    'RELIANCE.BSE',
    'TCS.BSE',
    'HDFCBANK.BSE',
    'INFY.BSE',
    'WIPRO.BSE'
]

def extract_stock_quote(symbol: str) -> dict:
    """Extract current quote for a single stock."""
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': API_KEY
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    quote = data.get('Global Quote', {})

    if not quote:
        print(f"⚠️ No data returned for {symbol}")
        return None
    
    return {
        'symbol': quote.get('01. symbol'),
        'open': float(quote.get('02. open', 0)),
        'high': float(quote.get('03. high', 0)),
        'low': float(quote.get('04. low', 0)),
        'close': float(quote.get('05. price', 0)),
        'volume': int(quote.get('06. volume', 0)),
        'trading_day': quote.get('07. latest trading day'),
        'previous_close': float(quote.get('08. previous close', 0)),
        'change': float(quote.get('09. change', 0)),
        'change_percent': quote.get('10. change percent', '0%').replace('%', ''),
        'extracted_at': datetime.now(UTC).isoformat()
    }

def extract_all_stocks() -> list:
    """Extract quotes for all tracked stocks."""
    results = []
    for i, symbol in enumerate(STOCKS):
        print(f"🔄 Extracting {symbol}...")
        data = extract_stock_quote(symbol)
        if data:
            results.append(data)
            print(f"✅ {symbol}: ₹{data['close']} | Change: {data['change_percent']}%")
        
        # Rate limit: 5 calls/min on free tier
        if i < len(STOCKS) - 1:
            print("⏳ Waiting 15s (rate limit)...")
            time.sleep(15)
    
    return results

if __name__ == '__main__':
    print(f"📈 Starting extraction — {datetime.now(UTC).isoformat()}")
    stocks = extract_all_stocks()
    print(f"\n✅ Extracted {len(stocks)} stocks successfully")  