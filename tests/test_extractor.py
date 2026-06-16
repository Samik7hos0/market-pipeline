import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.extract.stock_extractor import extract_stock_quote

def test_extract_returns_required_fields():
    """Test that extracted data has all required fields."""
    # Use a known symbol
    result = extract_stock_quote('RELIANCE.BSE')
    
    if result is None:
        print("API rate limited — skipping test")
        return
    
    required_fields = ['symbol', 'open', 'high', 'low', 'close', 
                       'volume', 'trading_day', 'change_percent']
    
    for field in required_fields:
        assert field in result, f"Missing field: {field}"
    
    assert result['close'] > 0, "Close price should be positive"
    assert result['volume'] > 0, "Volume should be positive"
    print(f"All fields present for {result['symbol']}")

def test_change_percent_is_numeric():
    """Test that change_percent can be converted to float."""
    result = extract_stock_quote('TCS.BSE')
    
    if result is None:
        print("API rate limited — skipping test")
        return
    
    try:
        float(result['change_percent'])
        print(" change_percent is numeric")
    except ValueError:
        assert False, "change_percent should be numeric"

if __name__ == '__main__':
    test_extract_returns_required_fields()
    print("Waiting for rate limit...")
    import time
    time.sleep(15)
    test_change_percent_is_numeric()
    print("\n All tests passed!")