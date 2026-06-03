import sys
import os
from datetime import datetime, UTC

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.extract.stock_extractor import extract_all_stocks
from src.load.snowflake_loader import load_stocks

def run_pipeline():
    """
    Main ELT pipeline:
    Extract from Alpha Vantage API → Load into Snowflake RAW
    """
    print("=" * 50)
    print(f"🚀 Pipeline started: {datetime.now(UTC).isoformat()}")
    print("=" * 50)

    # EXTRACT
    print("\n📥 EXTRACT — pulling from Alpha Vantage...")
    records = extract_all_stocks()

    if not records:
        print("❌ No records extracted. Exiting.")
        return

    print(f"\n✅ Extracted {len(records)} records")

    # LOAD
    print("\n📤 LOAD — pushing to Snowflake RAW...")
    loaded = load_stocks(records)

    print("\n" + "=" * 50)
    print(f"✅ Pipeline complete: {loaded} new records loaded")
    print(f"⏰ Finished: {datetime.now(UTC).isoformat()}")
    print("=" * 50)

if __name__ == '__main__':
    run_pipeline()