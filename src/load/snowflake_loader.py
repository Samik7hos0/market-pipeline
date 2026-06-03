import snowflake.connector
import os
from dotenv import load_dotenv
from datetime import datetime, UTC

load_dotenv()

def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema='RAW',
        role=os.getenv('SNOWFLAKE_ROLE')
    )

def create_table_if_not_exists(cursor):
    """Create the raw stock prices table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw.stock_prices (
            id              INTEGER AUTOINCREMENT PRIMARY KEY,
            symbol          VARCHAR(20),
            open            FLOAT,
            high            FLOAT,
            low             FLOAT,
            close           FLOAT,
            volume          BIGINT,
            trading_day     DATE,
            previous_close  FLOAT,
            change          FLOAT,
            change_percent  FLOAT,
            extracted_at    TIMESTAMP,
            loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    print("✅ Table raw.stock_prices ready")

def is_duplicate(cursor, symbol: str, trading_day: str) -> bool:
    """Check if record already exists — incremental load logic."""
    cursor.execute("""
        SELECT COUNT(*) FROM raw.stock_prices
        WHERE symbol = %s AND trading_day = %s
    """, (symbol, trading_day))
    return cursor.fetchone()[0] > 0

def load_stocks(records: list) -> int:
    """Load stock records into Snowflake, skipping duplicates."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    create_table_if_not_exists(cursor)

    loaded = 0
    skipped = 0

    for record in records:
        if is_duplicate(cursor, record['symbol'], record['trading_day']):
            print(f"⏭️ Skipping {record['symbol']} {record['trading_day']} — already loaded")
            skipped += 1
            continue

        cursor.execute("""
            INSERT INTO raw.stock_prices
            (symbol, open, high, low, close, volume, trading_day,
             previous_close, change, change_percent, extracted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record['symbol'],
            record['open'],
            record['high'],
            record['low'],
            record['close'],
            record['volume'],
            record['trading_day'],
            record['previous_close'],
            record['change'],
            float(record['change_percent']),
            record['extracted_at']
        ))
        loaded += 1
        print(f"✅ Loaded {record['symbol']} — ₹{record['close']}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\n📊 Loaded: {loaded} | Skipped (duplicates): {skipped}")
    return loaded


if __name__ == '__main__':
    # Test with dummy data
    test_records = [{
        'symbol': 'TEST.BSE',
        'open': 100.0,
        'high': 110.0,
        'low': 95.0,
        'close': 105.0,
        'volume': 100000,
        'trading_day': '2026-06-03',
        'previous_close': 98.0,
        'change': 7.0,
        'change_percent': '7.0',
        'extracted_at': datetime.now(UTC).isoformat()
    }]
    load_stocks(test_records)