#!/usr/bin/env python3
"""
Export Upbit orderbook data from PostgreSQL to compressed Parquet files.

This version uses efficient chunked export without re-reading files.
Supports resume capability.

Output:
- symbols.parquet: All symbol metadata
- snapshots_{symbol}.parquet: Snapshot metadata per symbol
- orderbook_{symbol}.parquet: Orderbook data per symbol

Usage:
    python3 export_to_parquet_v2.py [--output-dir ./parquet_export] [--chunk-size 100000]
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Optional, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database configuration
DB_CONFIG = {
    "dbname": "dnk",
    "user": "user",
    "password": os.getenv("DB_PASSWORD", "pass"),
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


def merge_parquet_chunks(chunk_files: List[Path], output_file: Path):
    """Efficiently merge parquet chunk files without loading all into memory."""
    if not chunk_files:
        return

    # Read all chunk files and merge using PyArrow (memory efficient)
    tables = []
    for chunk_file in chunk_files:
        tables.append(pq.read_table(chunk_file))

    # Concatenate tables
    combined_table = pa.concat_tables(tables)

    # Write to final file
    pq.write_table(combined_table, output_file, compression='snappy')

    # Clean up chunk files
    for chunk_file in chunk_files:
        chunk_file.unlink()


def export_snapshots_by_symbol(
    conn, symbol_code: str, symbol_id: int, output_dir: Path, chunk_size: int = 100000
):
    """
    Export orderbook snapshots for a specific symbol using server-side cursor.
    Writes chunks separately then merges at end (much faster).
    """
    logging.info(f"Exporting snapshots for {symbol_code}...")

    output_file = output_dir / f"snapshots_{symbol_code.replace('-', '_')}.parquet"

    # Check if already exported
    if output_file.exists():
        logging.info(f"  ✓ Already exported: {output_file.name} (skipping)")
        # Count rows for stats
        existing_df = pd.read_parquet(output_file)
        return len(existing_df)

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

    chunk_files = []
    try:
        with conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as cursor:
            cursor.itersize = chunk_size
            cursor.execute(query, (symbol_id,))

            total_rows = 0
            chunk_num = 0

            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                # Convert to DataFrame
                chunk_df = pd.DataFrame(rows)
                total_rows += len(rows)

                # Write chunk to separate file
                chunk_file = output_dir / f".snapshots_{symbol_code.replace('-', '_')}_chunk_{chunk_num}.parquet"
                chunk_df.to_parquet(
                    chunk_file,
                    engine="pyarrow",
                    compression="snappy",
                    index=False,
                )
                chunk_files.append(chunk_file)
                chunk_num += 1

                if chunk_num % 5 == 0:
                    logging.info(f"  Progress: {total_rows:,} snapshots exported for {symbol_code}")

            logging.info(f"  Merging {len(chunk_files)} chunks for {symbol_code}...")
            merge_parquet_chunks(chunk_files, output_file)

            logging.info(f"✓ Exported {total_rows:,} snapshots for {symbol_code} to {output_file}")
            return total_rows

    except Exception as exc:
        # Clean up chunk files on error
        for chunk_file in chunk_files:
            if chunk_file.exists():
                chunk_file.unlink()
        logging.error(f"Failed to export snapshots for {symbol_code}: {exc}")
        raise


def export_orderbook_by_symbol(
    conn, symbol_code: str, symbol_id: int, output_dir: Path, chunk_size: int = 100000
):
    """
    Export orderbook data for a specific symbol using server-side cursor.
    Writes chunks separately then merges at end (much faster).
    """
    logging.info(f"Exporting orderbook data for {symbol_code}...")

    output_file = output_dir / f"orderbook_{symbol_code.replace('-', '_')}.parquet"

    # Check if already exported
    if output_file.exists():
        logging.info(f"  ✓ Already exported: {output_file.name} (skipping)")
        # Count rows for stats
        existing_df = pd.read_parquet(output_file)
        return len(existing_df)

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

    chunk_files = []
    try:
        with conn.cursor(name=cursor_name, cursor_factory=RealDictCursor) as cursor:
            cursor.itersize = chunk_size
            cursor.execute(query, (symbol_id,))

            total_rows = 0
            chunk_num = 0

            while True:
                rows = cursor.fetchmany(chunk_size)
                if not rows:
                    break

                # Convert to DataFrame
                chunk_df = pd.DataFrame(rows)
                total_rows += len(rows)

                # Write chunk to separate file
                chunk_file = output_dir / f".orderbook_{symbol_code.replace('-', '_')}_chunk_{chunk_num}.parquet"
                chunk_df.to_parquet(
                    chunk_file,
                    engine="pyarrow",
                    compression="snappy",
                    index=False,
                )
                chunk_files.append(chunk_file)
                chunk_num += 1

                if chunk_num % 5 == 0:
                    logging.info(f"  Progress: {total_rows:,} orderbook rows exported for {symbol_code}")

            logging.info(f"  Merging {len(chunk_files)} chunks for {symbol_code}...")
            merge_parquet_chunks(chunk_files, output_file)

            logging.info(f"✓ Exported {total_rows:,} orderbook rows for {symbol_code} to {output_file}")
            return total_rows

    except Exception as exc:
        # Clean up chunk files on error
        for chunk_file in chunk_files:
            if chunk_file.exists():
                chunk_file.unlink()
        logging.error(f"Failed to export orderbook data for {symbol_code}: {exc}")
        raise


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export Upbit PostgreSQL data to Parquet files (V2 - Optimized)"
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
            if not file.name.startswith('.'):  # Skip hidden chunk files
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
