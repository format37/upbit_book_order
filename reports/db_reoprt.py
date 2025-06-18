import argparse
import logging
import sys
from typing import List, Tuple

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


# -------------------- Report logic --------------------

def fetch_counts(conn) -> List[Tuple[str, int, int, str, str, str]]:
    """Return list of (symbol_code, snapshot_count, data_rows_count, start_date, end_date, freq_seconds), ordered by symbol_code."""
    query = """
        SELECT 
            s.symbol_code,
            COUNT(DISTINCT o.snapshot_id) AS snapshots,
            COUNT(d.snapshot_id) AS data_rows,
            TO_CHAR(TO_TIMESTAMP(MIN(o.timestamp) / 1000), 'YYYY-MM-DD HH24:MI:SS') AS start_date,
            TO_CHAR(TO_TIMESTAMP(MAX(o.timestamp) / 1000), 'YYYY-MM-DD HH24:MI:SS') AS end_date,
            CASE 
                WHEN COUNT(DISTINCT o.snapshot_id) > 1 THEN
                    ROUND((MAX(o.timestamp) - MIN(o.timestamp)) / 1000.0 / (COUNT(DISTINCT o.snapshot_id) - 1), 2)
                ELSE 
                    0
            END AS mean_freq_seconds
        FROM upbit_symbols s
        LEFT JOIN upbit_orderbook_snapshots o ON s.symbol_id = o.symbol_id
        LEFT JOIN upbit_order_book_data d ON s.symbol_id = d.symbol_id
        GROUP BY s.symbol_code
        HAVING COUNT(DISTINCT o.snapshot_id) > 0 OR COUNT(d.snapshot_id) > 0
        ORDER BY s.symbol_code;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()


def print_table(rows: List[Tuple[str, int, int, str, str, str]], max_rows: int = None):
    """Pretty-print result as a simple table."""
    if not rows:
        print("No data found.")
        return

    if max_rows is not None:
        rows = rows[:max_rows]

    # Determine column widths
    sym_width = max(len("Symbol"), *(len(r[0]) for r in rows))
    snap_width = max(len("Snapshots"), *(len(str(r[1])) for r in rows))
    data_width = max(len("Data Rows"), *(len(str(r[2])) for r in rows))
    start_width = max(len("Start Date"), *(len(str(r[3])) for r in rows if r[3]))
    end_width = max(len("End Date"), *(len(str(r[4])) for r in rows if r[4]))
    freq_width = max(len("Freq(s)"), *(len(str(r[5])) for r in rows))

    sep = "+-{}-+-{}-+-{}-+-{}-+-{}-+-{}-+".format(
        "-" * sym_width, "-" * snap_width, "-" * data_width, 
        "-" * start_width, "-" * end_width, "-" * freq_width
    )
    header = "| {sym:^{sw}} | {snap:^{snw}} | {data:^{dw}} | {start:^{stw}} | {end:^{ew}} | {freq:^{fw}} |".format(
        sym="Symbol", snap="Snapshots", data="Data Rows", start="Start Date", end="End Date", freq="Freq(s)",
        sw=sym_width, snw=snap_width, dw=data_width, stw=start_width, ew=end_width, fw=freq_width
    )
    print(sep)
    print(header)
    print(sep)
    for sym, snap_cnt, data_cnt, start_date, end_date, freq in rows:
        print("| {sym:<{sw}} | {snap:>{snw}} | {data:>{dw}} | {start:<{stw}} | {end:<{ew}} | {freq:>{fw}} |".format(
            sym=sym, snap=snap_cnt, data=data_cnt, start=start_date or "N/A", end=end_date or "N/A", freq=freq,
            sw=sym_width, snw=snap_width, dw=data_width, stw=start_width, ew=end_width, fw=freq_width
        ))
    print(sep)


def parse_args():
    parser = argparse.ArgumentParser(description="Upbit DB snapshot row count per symbol")
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit output to the first N symbols (alphabetical order).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    conn = connect_db()
    try:
        rows = fetch_counts(conn)
    finally:
        conn.close()

    print_table(rows, max_rows=args.limit)


if __name__ == "__main__":
    main()
