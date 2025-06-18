#!/usr/bin/env python3
"""
Clear (truncate) all Upbit order-book tables.

Usage:
    python order_book/pybit/clear_upbit_tables.py         # asks for confirmation
    python order_book/pybit/clear_upbit_tables.py --yes   # no prompt, just executes

This will TRUNCATE:
    • upbit_order_book_data
    • upbit_orderbook_snapshots
    • upbit_symbols

The operation is irreversible.
"""

import argparse
import sys

import psycopg2

DB_CONFIG = {
    "dbname": "dnk",
    "user": "user",
    "password": "pass",
    "host": "localhost",
    "port": "5432",
}

TABLES = [
    "upbit_order_book_data",
    "upbit_orderbook_snapshots",
    "upbit_symbols",
]


def parse_args():
    parser = argparse.ArgumentParser(description="Clear Upbit tables (truncate)")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    return parser.parse_args()


def confirm() -> bool:
    reply = input("This will delete ALL data in Upbit tables. Are you sure? [y/N]: ").strip().lower()
    return reply in {"y", "yes"}


def main():
    args = parse_args()

    if not args.yes and not confirm():
        print("Aborted.")
        sys.exit(0)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            # Use TRUNCATE ... CASCADE to respect foreign keys
            print("Truncating tables...")
            cur.execute(
                "TRUNCATE {} CASCADE;".format(
                    ", ".join(TABLES)
                )
            )
        conn.commit()
        print("✓ All Upbit tables cleared.")
    except Exception as exc:
        print(f"Error: {exc}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main() 