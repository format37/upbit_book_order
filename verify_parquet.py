#!/usr/bin/env python3
"""
Verify exported Parquet files and display summary statistics.

Usage:
    python3 verify_parquet.py [--parquet-dir ~/upbit_data_parquet]
"""

import argparse
from pathlib import Path

import pandas as pd


def format_timestamp(ts_ms):
    """Convert millisecond timestamp to readable datetime."""
    return pd.to_datetime(ts_ms, unit="ms")


def verify_parquet_export(parquet_dir: Path):
    """Verify all exported Parquet files and show summary."""
    print("=" * 80)
    print("PARQUET DATA VERIFICATION")
    print("=" * 80)

    if not parquet_dir.exists():
        print(f"❌ Error: Directory '{parquet_dir}' does not exist")
        return False

    # Verify symbols file
    symbols_file = parquet_dir / "symbols.parquet"
    if not symbols_file.exists():
        print(f"❌ Error: symbols.parquet not found in {parquet_dir}")
        return False

    symbols_df = pd.read_parquet(symbols_file)
    print(f"\n✓ Symbols table: {len(symbols_df)} symbols")
    print("\nSymbol Details:")
    for _, row in symbols_df.iterrows():
        print(f"  - {row['symbol_code']} (ID: {row['symbol_id']})")

    # Track totals
    total_snapshots = 0
    total_orderbook_rows = 0
    missing_files = []

    print("\n" + "=" * 80)
    print("SYMBOL-BY-SYMBOL VERIFICATION")
    print("=" * 80)

    # Verify each symbol's data
    for _, symbol_row in symbols_df.iterrows():
        symbol_code = symbol_row["symbol_code"]
        symbol_id = symbol_row["symbol_id"]
        symbol_file = symbol_code.replace("-", "_")

        print(f"\n{symbol_code}:")
        print("-" * 40)

        # Verify snapshot file
        snapshot_file = parquet_dir / f"snapshots_{symbol_file}.parquet"
        if not snapshot_file.exists():
            print(f"  ❌ Missing: snapshots_{symbol_file}.parquet")
            missing_files.append(snapshot_file.name)
        else:
            snapshots_df = pd.read_parquet(snapshot_file)
            snapshot_count = len(snapshots_df)
            total_snapshots += snapshot_count

            # Get date range
            min_ts = snapshots_df["timestamp"].min()
            max_ts = snapshots_df["timestamp"].max()
            min_date = format_timestamp(min_ts)
            max_date = format_timestamp(max_ts)

            # Calculate frequency
            duration_seconds = (max_ts - min_ts) / 1000
            freq_seconds = duration_seconds / (snapshot_count - 1) if snapshot_count > 1 else 0

            size_mb = snapshot_file.stat().st_size / (1024 * 1024)

            print(f"  ✓ Snapshots: {snapshot_count:,} rows ({size_mb:.2f} MB)")
            print(f"    Date range: {min_date} to {max_date}")
            print(f"    Frequency: {freq_seconds:.2f}s avg")

        # Verify orderbook file
        orderbook_file = parquet_dir / f"orderbook_{symbol_file}.parquet"
        if not orderbook_file.exists():
            print(f"  ❌ Missing: orderbook_{symbol_file}.parquet")
            missing_files.append(orderbook_file.name)
        else:
            orderbook_df = pd.read_parquet(orderbook_file)
            orderbook_count = len(orderbook_df)
            total_orderbook_rows += orderbook_count

            # Calculate levels per snapshot
            levels_per_snapshot = (
                orderbook_df.groupby("snapshot_id")["unit_level"].count().mean()
            )

            size_mb = orderbook_file.stat().st_size / (1024 * 1024)

            print(f"  ✓ Orderbook: {orderbook_count:,} rows ({size_mb:.2f} MB)")
            print(f"    Avg levels/snapshot: {levels_per_snapshot:.1f}")

            # Sample data quality check
            if orderbook_count > 0:
                # Check for nulls
                null_counts = orderbook_df.isnull().sum()
                if null_counts.sum() > 0:
                    print(f"    ⚠️  Warning: Found null values in {null_counts[null_counts > 0].to_dict()}")

                # Check price ranges
                min_ask = orderbook_df["ask_price"].min()
                max_ask = orderbook_df["ask_price"].max()
                min_bid = orderbook_df["bid_price"].min()
                max_bid = orderbook_df["bid_price"].max()

                print(f"    Price ranges:")
                print(f"      Ask: {min_ask:.8f} - {max_ask:.8f}")
                print(f"      Bid: {min_bid:.8f} - {max_bid:.8f}")

    # Final summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total symbols: {len(symbols_df)}")
    print(f"Total snapshots: {total_snapshots:,}")
    print(f"Total orderbook rows: {total_orderbook_rows:,}")

    # Calculate total size
    total_size_mb = sum(
        f.stat().st_size for f in parquet_dir.glob("*.parquet")
    ) / (1024 * 1024)
    print(f"Total size: {total_size_mb:.2f} MB ({total_size_mb / 1024:.2f} GB)")

    # Show all files
    print("\nAll files:")
    for file in sorted(parquet_dir.glob("*.parquet")):
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"  {file.name:<40} {size_mb:>10.2f} MB")

    if missing_files:
        print(f"\n❌ Missing {len(missing_files)} files:")
        for fname in missing_files:
            print(f"  - {fname}")
        return False
    else:
        print("\n✅ All files verified successfully!")
        return True


def parse_args():
    parser = argparse.ArgumentParser(description="Verify exported Parquet files")
    parser.add_argument(
        "--parquet-dir",
        type=str,
        default="~/upbit_data_parquet",
        help="Directory containing Parquet files (default: ~/upbit_data_parquet)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    parquet_dir = Path(args.parquet_dir).expanduser()

    success = verify_parquet_export(parquet_dir)

    if success:
        print("\n🎉 Data export verification complete!")
        return 0
    else:
        print("\n⚠️  Verification found issues - please check the output above")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())
