#!/usr/bin/env python3
"""
Quick script to check exact row counts for each symbol.
Shows both snapshot and orderbook row counts.
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "dbname": "dnk",
    "user": "user",
    "password": os.getenv("DB_PASSWORD", "pass"),
    "host": "localhost",
    "port": "5432",
}

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("=" * 80)
    print("SYMBOL ROW COUNTS")
    print("=" * 80)

    # Get counts for each symbol
    query = """
        SELECT
            s.symbol_code,
            s.symbol_id,
            COUNT(DISTINCT snap.snapshot_id) as snapshot_count,
            COUNT(ob.id) as orderbook_rows
        FROM upbit_symbols s
        LEFT JOIN upbit_orderbook_snapshots snap ON s.symbol_id = snap.symbol_id
        LEFT JOIN upbit_order_book_data ob ON s.symbol_id = ob.symbol_id
        GROUP BY s.symbol_code, s.symbol_id
        ORDER BY s.symbol_code
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    total_snapshots = 0
    total_orderbook = 0

    print(f"\n{'Symbol':<15} {'Snapshots':>15} {'Orderbook Rows':>20} {'Avg Levels':>12}")
    print("-" * 80)

    for symbol_code, symbol_id, snapshot_count, orderbook_count in rows:
        avg_levels = orderbook_count / snapshot_count if snapshot_count > 0 else 0
        print(f"{symbol_code:<15} {snapshot_count:>15,} {orderbook_count:>20,} {avg_levels:>12.1f}")
        total_snapshots += snapshot_count
        total_orderbook += orderbook_count

    print("-" * 80)
    print(f"{'TOTAL':<15} {total_snapshots:>15,} {total_orderbook:>20,}")
    print()

    # For USDT-BTC specifically
    print("\n" + "=" * 80)
    print("USDT-BTC DETAILED COUNT")
    print("=" * 80)

    cursor.execute("""
        SELECT
            COUNT(DISTINCT snapshot_id) as snapshots,
            COUNT(*) as orderbook_rows
        FROM upbit_order_book_data
        WHERE symbol_id = (SELECT symbol_id FROM upbit_symbols WHERE symbol_code = 'USDT-BTC')
    """)

    btc_snapshots, btc_orderbook = cursor.fetchone()
    print(f"Snapshots: {btc_snapshots:,}")
    print(f"Orderbook rows: {btc_orderbook:,}")
    print(f"Average levels per snapshot: {btc_orderbook / btc_snapshots:.1f}")
    print()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
