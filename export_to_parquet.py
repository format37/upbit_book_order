#!/usr/bin/env python3
"""
Export Upbit orderbook data from PostgreSQL to compressed Parquet files.

This script uses server-side cursors to stream large datasets efficiently
with minimal memory usage (~200MB max), suitable for resource-constrained machines.

Output:
- symbols.parquet: All symbol metadata
- snapshots_{symbol}.parquet: Snapshot metadata per symbol
- orderbook_{symbol}.parquet: Orderbook data per symbol

Usage:
    python3 export_to_parquet.py [--output-dir ./parquet_export] [--chunk-size 100000]
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Database configuration
DB_CONFIG = {
    "dbname": "dnk",
    "user": "user",
    "password": "pass",  # Update if needed
    "host": "localhost",
    "port": "5432",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("export_to_parquet.log"),
        logging.StreamHandler(sys.stdout),
    ],
)


def connect_db():
    """Create PostgreSQL connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logging.info("✓ Connected to database")
        return conn
    except Exception as exc:
        logging.error(f"Failed to connect to database: {exc}")
        sys.exit(1)


def export_symbols(conn, output_dir: Path):
    """Export the symbols table to parquet."""
    logging.info("Exporting symbols table...")

    query = """
        SELECT symbol_id, symbol_code, base_currency, quote_currency, created_at
        FROM upbit_symbols
        ORDER BY symbol_id
    """

    try:
        df = pd.read_sql_query(query, conn)
        output_file = output_dir / "symbols.parquet"
        df.to_parquet(
            output_file,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        logging.info(f"✓ Exported {len(df)} symbols to {output_file}")
        return df
    except Exception as exc:
        logging.error(f"Failed to export symbols: {exc}")
        raise


def export_snapshots_by_symbol(
    conn, symbol_code: str, symbol_id: int, output_dir: Path, chunk_size: int = 100000
):
    """
    Export orderbook snapshots for a specific symbol using server-side cursor.
    Memory-efficient streaming with chunked writes.
    """
    logging.info(f"Exporting snapshots for {symbol_code}...")

    output_file = output_dir / f"snapshots_{symbol_code.replace('-', '_')}.parquet"

    # Use server-side cursor with unique name
    cursor_name = f"snapshot_cursor_{symbol_id}"

    query = """
        SELECT
            snapshot_id,
            symbol_id,
            timestamp,
            total_ask_size,
            total_bid_size,
            stream_type,
            units_count,
            received_at
        FROM upbit_orderbook_snapshots
        WHERE symbol_id = %s
        ORDER BY snapshot_id
    """

    try:
        with conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as cursor:
            cursor.itersize = chunk_size  # Fetch this many rows at a time
            cursor.execute(query, (symbol_id,))

            total_rows = 0
            chunks = []

            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                # Convert to DataFrame
                chunk_df = pd.DataFrame(rows)
                chunks.append(chunk_df)
                total_rows += len(rows)

                # Write to parquet every 5 chunks to manage memory
                if len(chunks) >= 5:
                    combined_df = pd.concat(chunks, ignore_index=True)

                    # Append or create file
                    if output_file.exists():
                        # Read existing and append
                        existing_df = pd.read_parquet(output_file)
                        combined_df = pd.concat([existing_df, combined_df], ignore_index=True)

                    combined_df.to_parquet(
                        output_file,
                        engine="pyarrow",
                        compression="snappy",
                        index=False,
                    )
                    chunks = []
                    logging.info(f"  Progress: {total_rows:,} snapshots exported for {symbol_code}")

            # Write remaining chunks
            if chunks:
                combined_df = pd.concat(chunks, ignore_index=True)

                if output_file.exists():
                    existing_df = pd.read_parquet(output_file)
                    combined_df = pd.concat([existing_df, combined_df], ignore_index=True)

                combined_df.to_parquet(
                    output_file,
                    engine="pyarrow",
                    compression="snappy",
                    index=False,
                )

            logging.info(f"✓ Exported {total_rows:,} snapshots for {symbol_code} to {output_file}")
            return total_rows

    except Exception as exc:
        logging.error(f"Failed to export snapshots for {symbol_code}: {exc}")
        raise


def export_orderbook_by_symbol(
    conn, symbol_code: str, symbol_id: int, output_dir: Path, chunk_size: int = 100000
):
    """
    Export orderbook data for a specific symbol using server-side cursor.
    Memory-efficient streaming with chunked writes.
    """
    logging.info(f"Exporting orderbook data for {symbol_code}...")

    output_file = output_dir / f"orderbook_{symbol_code.replace('-', '_')}.parquet"

    # Use server-side cursor with unique name
    cursor_name = f"orderbook_cursor_{symbol_id}"

    query = """
        SELECT
            id,
            snapshot_id,
            symbol_id,
            timestamp,
            ask_price,
            bid_price,
            ask_size,
            bid_size,
            unit_level,
            received_at
        FROM upbit_order_book_data
        WHERE symbol_id = %s
        ORDER BY id
    """

    try:
        with conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as cursor:
            cursor.itersize = chunk_size  # Fetch this many rows at a time
            cursor.execute(query, (symbol_id,))

            total_rows = 0
            chunks = []

            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                # Convert to DataFrame
                chunk_df = pd.DataFrame(rows)
                chunks.append(chunk_df)
                total_rows += len(rows)

                # Write to parquet every 5 chunks to manage memory
                if len(chunks) >= 5:
                    combined_df = pd.concat(chunks, ignore_index=True)

                    # Append or create file
                    if output_file.exists():
                        # Read existing and append
                        existing_df = pd.read_parquet(output_file)
                        combined_df = pd.concat([existing_df, combined_df], ignore_index=True)

                    combined_df.to_parquet(
                        output_file,
                        engine="pyarrow",
                        compression="snappy",
                        index=False,
                    )
                    chunks = []
                    logging.info(f"  Progress: {total_rows:,} orderbook rows exported for {symbol_code}")

            # Write remaining chunks
            if chunks:
                combined_df = pd.concat(chunks, ignore_index=True)

                if output_file.exists():
                    existing_df = pd.read_parquet(output_file)
                    combined_df = pd.concat([existing_df, combined_df], ignore_index=True)

                combined_df.to_parquet(
                    output_file,
                    engine="pyarrow",
                    compression="snappy",
                    index=False,
                )

            logging.info(f"✓ Exported {total_rows:,} orderbook rows for {symbol_code} to {output_file}")
            return total_rows

    except Exception as exc:
        logging.error(f"Failed to export orderbook data for {symbol_code}: {exc}")
        raise


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export Upbit PostgreSQL data to Parquet files"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./parquet_export",
        help="Output directory for Parquet files (default: ./parquet_export)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100000,
        help="Number of rows to fetch per chunk (default: 100000)",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated list of symbol codes to export (default: all symbols)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"Output directory: {output_dir.absolute()}")

    # Connect to database
    conn = connect_db()
    conn.autocommit = True  # Required for server-side cursors

    try:
        # Export symbols table
        symbols_df = export_symbols(conn, output_dir)

        # Filter symbols if specified
        if args.symbols:
            symbol_codes = [s.strip().upper() for s in args.symbols.split(",")]
            symbols_df = symbols_df[symbols_df["symbol_code"].isin(symbol_codes)]
            logging.info(f"Filtered to symbols: {', '.join(symbol_codes)}")

        total_symbols = len(symbols_df)
        logging.info(f"Will export {total_symbols} symbols")

        # Export data for each symbol
        total_snapshots = 0
        total_orderbook_rows = 0

        for idx, row in symbols_df.iterrows():
            symbol_code = row["symbol_code"]
            symbol_id = row["symbol_id"]

            logging.info(f"\n{'='*60}")
            logging.info(f"Processing {symbol_code} ({idx + 1}/{total_symbols})")
            logging.info(f"{'='*60}")

            # Export snapshots
            snapshot_count = export_snapshots_by_symbol(
                conn, symbol_code, symbol_id, output_dir, args.chunk_size
            )
            total_snapshots += snapshot_count

            # Export orderbook data
            orderbook_count = export_orderbook_by_symbol(
                conn, symbol_code, symbol_id, output_dir, args.chunk_size
            )
            total_orderbook_rows += orderbook_count

        # Summary
        logging.info(f"\n{'='*60}")
        logging.info("EXPORT COMPLETE")
        logging.info(f"{'='*60}")
        logging.info(f"Total symbols exported: {total_symbols}")
        logging.info(f"Total snapshots: {total_snapshots:,}")
        logging.info(f"Total orderbook rows: {total_orderbook_rows:,}")
        logging.info(f"Output directory: {output_dir.absolute()}")

        # Show file sizes
        logging.info("\nFile sizes:")
        for file in sorted(output_dir.glob("*.parquet")):
            size_mb = file.stat().st_size / (1024 * 1024)
            logging.info(f"  {file.name}: {size_mb:.2f} MB")

    except Exception as exc:
        logging.error(f"Export failed: {exc}")
        sys.exit(1)
    finally:
        conn.close()
        logging.info("Database connection closed")


if __name__ == "__main__":
    main()
