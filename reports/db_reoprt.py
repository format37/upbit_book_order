import argparse
import logging
import sys
from typing import List, Tuple, Optional

import psycopg2

# Reuse same DB config as in fetcher script (adjust if you changed credentials)
DB_CONFIG = {
    "dbname": "dnk",
    "user": "user",
    "password": "pass",
    "host": "localhost",
    "port": "5432",
}


def connect_db():
    """Create a new PostgreSQL connection (autocommit OFF)."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as exc:
        logging.error(f"Unable to connect to the database: {exc}")
        sys.exit(1)


# -------------------- Optimized Report logic --------------------

def fetch_symbol_stats(conn, symbol_filter: Optional[str] = None) -> List[Tuple[str, int, str, str, float, float]]:
    """
    Return list of (symbol_code, snapshot_count, start_date, end_date, freq_seconds, duration_hours).
    Much more efficient query that only uses the snapshots table.
    """
    where_clause = ""
    params = []
    
    if symbol_filter:
        where_clause = "WHERE s.symbol_code LIKE %s"
        params.append(f"%{symbol_filter}%")
    
    query = f"""
        WITH snapshot_stats AS (
            SELECT 
                s.symbol_code,
                COUNT(o.snapshot_id) AS snapshot_count,
                MIN(o.timestamp) AS min_ts,
                MAX(o.timestamp) AS max_ts
            FROM upbit_symbols s
            INNER JOIN upbit_orderbook_snapshots o ON s.symbol_id = o.symbol_id
            {where_clause}
            GROUP BY s.symbol_code, s.symbol_id
            HAVING COUNT(o.snapshot_id) > 0
        )
        SELECT 
            symbol_code,
            snapshot_count,
            TO_CHAR(TO_TIMESTAMP(min_ts / 1000), 'YYYY-MM-DD HH24:MI:SS') AS start_date,
            TO_CHAR(TO_TIMESTAMP(max_ts / 1000), 'YYYY-MM-DD HH24:MI:SS') AS end_date,
            CASE 
                WHEN snapshot_count > 1 THEN
                    ROUND((max_ts - min_ts) / 1000.0 / (snapshot_count - 1), 2)
                ELSE 
                    0
            END AS freq_seconds,
            ROUND((max_ts - min_ts) / 1000.0 / 3600.0, 2) AS duration_hours
        FROM snapshot_stats
        ORDER BY symbol_code;
    """
    
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def fetch_summary_stats(conn) -> Tuple[int, int, str, str]:
    """Get overall database statistics."""
    query = """
        SELECT 
            COUNT(DISTINCT s.symbol_code) as total_symbols,
            COUNT(o.snapshot_id) as total_snapshots,
            TO_CHAR(TO_TIMESTAMP(MIN(o.timestamp) / 1000), 'YYYY-MM-DD HH24:MI:SS') AS earliest_date,
            TO_CHAR(TO_TIMESTAMP(MAX(o.timestamp) / 1000), 'YYYY-MM-DD HH24:MI:SS') AS latest_date
        FROM upbit_symbols s
        INNER JOIN upbit_orderbook_snapshots o ON s.symbol_id = o.symbol_id;
    """
    
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()


def print_summary(stats: Tuple[int, int, str, str]):
    """Print database summary statistics."""
    total_symbols, total_snapshots, earliest, latest = stats
    print("=" * 80)
    print("DATABASE SUMMARY")
    print("=" * 80)
    print(f"Total Symbols: {total_symbols:,}")
    print(f"Total Snapshots: {total_snapshots:,}")
    print(f"Date Range: {earliest} to {latest}")
    print("=" * 80)
    print()


def print_efficient_table(rows: List[Tuple[str, int, str, str, float, float]], max_rows: int = None):
    """Print optimized table with better formatting and additional metrics."""
    if not rows:
        print("No data found.")
        return

    if max_rows is not None:
        rows = rows[:max_rows]
        print(f"Showing first {len(rows)} symbols (limited by --limit {max_rows})")
        print()

    # Calculate column widths
    sym_width = max(len("Symbol"), *(len(r[0]) for r in rows))
    snap_width = max(len("Snapshots"), *(len(str(r[1])) for r in rows))
    start_width = max(len("Start Date"), *(len(str(r[2])) for r in rows))
    end_width = max(len("End Date"), *(len(str(r[3])) for r in rows))
    freq_width = max(len("Freq(s)"), *(len(f"{r[4]:.1f}" if r[4] > 0 else "N/A") for r in rows))
    duration_width = max(len("Duration(h)"), *(len(f"{r[5]:.1f}") for r in rows))

    # Create separator and header
    sep = "+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+".format(
        "-" * sym_width, "-" * snap_width, "-" * start_width, 
        "-" * end_width, "-" * freq_width, "-" * duration_width
    )
    
    header = "| {sym:^{sw}} | {snap:^{snw}} | {start:^{stw}} | {end:^{ew}} | {freq:^{fw}} | {dur:^{dw}} |".format(
        sym="Symbol", snap="Snapshots", start="Start Date", end="End Date", 
        freq="Freq(s)", dur="Duration(h)",
        sw=sym_width, snw=snap_width, stw=start_width, ew=end_width, 
        fw=freq_width, dw=duration_width
    )
    
    print(sep)
    print(header)
    print(sep)
    
    for sym, snap_cnt, start_date, end_date, freq, duration in rows:
        freq_str = f"{freq:.1f}" if freq > 0 else "N/A"
        print("| {sym:<{sw}} | {snap:>{snw}} | {start:<{stw}} | {end:<{ew}} | {freq:>{fw}} | {dur:>{dw}} |".format(
            sym=sym, snap=snap_cnt, start=start_date, end=end_date, 
            freq=freq_str, dur=f"{duration:.1f}",
            sw=sym_width, snw=snap_width, stw=start_width, ew=end_width, 
            fw=freq_width, dw=duration_width
        ))
    
    print(sep)
    
    # Add statistics
    if rows:
        total_snapshots = sum(r[1] for r in rows)
        avg_freq = sum(r[4] for r in rows if r[4] > 0) / len([r for r in rows if r[4] > 0]) if any(r[4] > 0 for r in rows) else 0
        print(f"\nSTATISTICS:")
        print(f"Total Snapshots: {total_snapshots:,}")
        print(f"Average Frequency: {avg_freq:.1f} seconds" if avg_freq > 0 else "Average Frequency: N/A")
        print(f"Symbols Shown: {len(rows)}")


def parse_args():
    parser = argparse.ArgumentParser(description="Efficient Upbit DB snapshot report")
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit output to the first N symbols (alphabetical order).",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        help="Filter symbols containing this text (case-insensitive).",
    )
    parser.add_argument(
        "--no-summary",
        action="store_true",
        help="Skip database summary statistics.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    conn = connect_db()
    try:
        # Get summary stats unless disabled
        if not args.no_summary:
            summary_stats = fetch_summary_stats(conn)
            print_summary(summary_stats)
        
        # Get symbol statistics
        logging.info("Fetching symbol statistics...")
        rows = fetch_symbol_stats(conn, args.symbol)
        logging.info(f"Retrieved data for {len(rows)} symbols")
        
    finally:
        conn.close()

    print_efficient_table(rows, max_rows=args.limit)


if __name__ == "__main__":
    main()
