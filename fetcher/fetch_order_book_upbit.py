#!/usr/bin/env python3
"""
Upbit Order Book Data Collector (WebSocket).

Features:
- Connects to Upbit WebSocket API and streams order book updates.
- Supports multiple symbols (default: all USDT symbols in upbit_symbols table).
- Stores every snapshot in optimized PostgreSQL tables created via create_tables_upbit.py.
- Calculates and optionally prints the spread (max ask − min bid) once per second.
- Robust reconnect logic and graceful shutdown.

Usage examples:
$ python fetch_order_book_upbit.py               # stream all default symbols
$ python fetch_order_book_upbit.py --symbols USDT-BTC,USDT-ETH  # specific symbols
$ python fetch_order_book_upbit.py --print-spread               # log spread info

Requirements:
- psycopg2-binary
- websockets>=12
"""

import asyncio
import json
import logging
import signal
import sys
import time
from typing import List, Dict, Any, Optional
from pathlib import Path

import psycopg2
import websockets
import argparse

import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("upbit_order_book_ws.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

db_password = os.getenv("DB_PASSWORD")

# Database configuration (adjust as needed)
DB_CONFIG = {
    "dbname": "dnk",
    "user": "user",
    "password": db_password,
    "host": "localhost",
    "port": "5432",
}

UPBIT_WS_ENDPOINT = "wss://api.upbit.com/websocket/v1"

# --------------- Database helpers ---------------

def connect_db():
    """Create a new PostgreSQL connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except Exception as exc:
        logging.error(f"Failed to connect to database: {exc}")
        raise

def check_and_create_tables(conn):
    """Check if required tables exist and create them if they don't"""
    try:
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('upbit_symbols', 'upbit_orderbook_snapshots', 'upbit_order_book_data')
        """)
        existing_tables = {row[0] for row in cursor.fetchall()}
        required_tables = {'upbit_symbols', 'upbit_orderbook_snapshots', 'upbit_order_book_data'}
        
        if required_tables.issubset(existing_tables):
            logging.info("All required tables exist")
            cursor.close()
            return True
        
        missing_tables = required_tables - existing_tables
        logging.info(f"Missing tables: {missing_tables}. Creating all required tables...")
        
        # Create Upbit symbols lookup table
        logging.info("Creating upbit_symbols table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS upbit_symbols (
                symbol_id SERIAL PRIMARY KEY,
                symbol_code VARCHAR(20) UNIQUE NOT NULL,
                base_currency VARCHAR(10) NOT NULL,
                quote_currency VARCHAR(10) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create orderbook snapshots metadata table
        logging.info("Creating upbit_orderbook_snapshots table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS upbit_orderbook_snapshots (
                snapshot_id SERIAL PRIMARY KEY,
                symbol_id INTEGER NOT NULL REFERENCES upbit_symbols(symbol_id),
                timestamp BIGINT NOT NULL,
                total_ask_size NUMERIC,
                total_bid_size NUMERIC,
                stream_type VARCHAR(20),
                units_count SMALLINT,
                received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create main order book data table for individual units
        logging.info("Creating upbit_order_book_data table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS upbit_order_book_data (
                id SERIAL PRIMARY KEY,
                snapshot_id INTEGER NOT NULL REFERENCES upbit_orderbook_snapshots(snapshot_id),
                symbol_id INTEGER NOT NULL REFERENCES upbit_symbols(symbol_id),
                timestamp BIGINT NOT NULL,
                ask_price NUMERIC NOT NULL,
                bid_price NUMERIC NOT NULL,
                ask_size NUMERIC NOT NULL,
                bid_size NUMERIC NOT NULL,
                unit_level SMALLINT NOT NULL,
                received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create indexes for performance
        logging.info("Creating indexes...")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_upbit_symbol_timestamp ON upbit_orderbook_snapshots(symbol_id, timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_upbit_snapshot_timestamp ON upbit_order_book_data(snapshot_id, timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_upbit_symbol_level ON upbit_order_book_data(symbol_id, unit_level, timestamp);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_upbit_prices ON upbit_order_book_data(ask_price, bid_price);")
        
        # Insert default symbols if upbit_symbols table was created
        if 'upbit_symbols' in missing_tables:
            logging.info("Inserting default symbols...")
            default_symbols = [
                ('USDT-BTC', 'BTC', 'USDT'),
                ('USDT-ETH', 'ETH', 'USDT'),
                ('USDT-XRP', 'XRP', 'USDT'),
                ('USDT-ADA', 'ADA', 'USDT'),
                ('USDT-DOGE', 'DOGE', 'USDT'),
                ('USDT-SOL', 'SOL', 'USDT'),
                ('USDT-MATIC', 'MATIC', 'USDT'),
                ('USDT-AVAX', 'AVAX', 'USDT'),
            ]
            cursor.executemany("""
                INSERT INTO upbit_symbols (symbol_code, base_currency, quote_currency)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol_code) DO NOTHING
            """, default_symbols)
        
        conn.commit()
        cursor.close()
        logging.info("✓ All required tables and indexes created successfully")
        return True
        
    except Exception as exc:
        conn.rollback()
        logging.error(f"Error creating tables: {exc}")
        raise

def get_or_create_symbol_id(conn, symbol_code: str) -> int:
    """Return symbol_id for given Upbit symbol code, inserting if needed."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol_id FROM upbit_symbols WHERE symbol_code = %s",
                (symbol_code,),
            )
            row = cur.fetchone()
            if row:
                return row[0]
            # parse base & quote if possible
            if "-" in symbol_code:
                quote, base = symbol_code.split("-", 1)
            else:
                base, quote = symbol_code, "KRW"
            cur.execute(
                """
                INSERT INTO upbit_symbols (symbol_code, base_currency, quote_currency)
                VALUES (%s, %s, %s) RETURNING symbol_id
                """,
                (symbol_code, base, quote),
            )
            symbol_id = cur.fetchone()[0]
            conn.commit()
            logging.info(f"Created new symbol {symbol_code} (ID: {symbol_id})")
            return symbol_id
    except Exception as exc:
        conn.rollback()
        logging.error(f"Error get/create symbol {symbol_code}: {exc}")
        raise

def store_orderbook_snapshot(conn, orderbook: Dict[str, Any]):
    """Persist a single orderbook snapshot message into DB."""
    symbol_code = orderbook["code"]
    timestamp = orderbook["timestamp"]
    total_ask_size = orderbook.get("total_ask_size")
    total_bid_size = orderbook.get("total_bid_size")
    stream_type = orderbook.get("stream_type")
    units = orderbook["orderbook_units"]
    units_count = len(units)

    try:
        symbol_id = get_or_create_symbol_id(conn, symbol_code)
        with conn.cursor() as cur:
            # Insert snapshot metadata
            cur.execute(
                """
                INSERT INTO upbit_orderbook_snapshots
                (symbol_id, timestamp, total_ask_size, total_bid_size, stream_type, units_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING snapshot_id
                """,
                (
                    symbol_id,
                    timestamp,
                    total_ask_size,
                    total_bid_size,
                    stream_type,
                    units_count,
                ),
            )
            snapshot_id = cur.fetchone()[0]
            # Prepare unit rows
            rows = []
            for level, unit in enumerate(units, 1):
                rows.append(
                    (
                        snapshot_id,
                        symbol_id,
                        timestamp,
                        unit["ask_price"],
                        unit["bid_price"],
                        unit["ask_size"],
                        unit["bid_size"],
                        level,
                    )
                )
            cur.executemany(
                """
                INSERT INTO upbit_order_book_data
                (snapshot_id, symbol_id, timestamp, ask_price, bid_price, ask_size, bid_size, unit_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
        logging.debug(
            f"Stored snapshot {snapshot_id} for {symbol_code} with {units_count} levels"
        )
    except Exception as exc:
        conn.rollback()
        logging.error(f"Failed to store snapshot for {symbol_code}: {exc}")
        raise

# --------------- WebSocket consumer ---------------

class UpbitOrderBookStreamer:
    def __init__(self, symbols: List[str], print_spread: bool = False, store_interval: float = 1.0):
        self.symbols = symbols
        self.print_spread = print_spread
        self.conn = connect_db()
        self._stop = asyncio.Event()
        self._last_print: Dict[str, float] = {}
        # Track last DB write per symbol to respect store_interval
        self._last_store: Dict[str, float] = {}
        self.store_interval = max(0.0, store_interval)

    async def start(self):
        while not self._stop.is_set():
            try:
                await self._consumer_loop()
            except Exception as exc:
                logging.error(f"WebSocket connection error: {exc}")
                await asyncio.sleep(3)  # retry delay

    async def _consumer_loop(self):
        subscribe_msg = [
            {"ticket": "dnk"},
            {"type": "orderbook", "codes": self.symbols},
            {"format": "DEFAULT"},
        ]
        logging.info(f"Connecting to Upbit WebSocket for {', '.join(self.symbols)}")
        async with websockets.connect(UPBIT_WS_ENDPOINT, ping_interval=60) as ws:
            await ws.send(json.dumps(subscribe_msg))
            logging.info("Subscription sent. Streaming order books...")
            async for message in ws:
                await self._handle_message(message)
                if self._stop.is_set():
                    break

    async def _handle_message(self, message: bytes):
        try:
            orderbook = json.loads(message)
            if orderbook.get("type") != "orderbook":
                return  # ignore other types
            # Store in DB at most once per symbol per store_interval
            symbol = orderbook["code"]
            now = time.time()
            if self.store_interval == 0 or now - self._last_store.get(symbol, 0) >= self.store_interval:
                await asyncio.to_thread(store_orderbook_snapshot, self.conn, orderbook)
                self._last_store[symbol] = now

            if self.print_spread:
                await self._maybe_print_spread(orderbook)
        except Exception as exc:
            logging.error(f"Error processing message: {exc}")

    async def _maybe_print_spread(self, orderbook: Dict[str, Any]):
        symbol = orderbook["code"]
        now = time.time()
        if now - self._last_print.get(symbol, 0) >= 1.0:
            units = orderbook["orderbook_units"]
            max_ask_unit = max(units, key=lambda u: u["ask_price"])
            min_bid_unit = min(units, key=lambda u: u["bid_price"])
            spread = max_ask_unit["ask_price"] - min_bid_unit["bid_price"]
            logging.info(
                f"[{symbol}] Ts: {orderbook['timestamp']} Max Ask: {max_ask_unit['ask_price']}"
                f" (Qty {max_ask_unit['ask_size']}) | Min Bid: {min_bid_unit['bid_price']}"
                f" (Qty {min_bid_unit['bid_size']}) | Spread: {spread}"
            )
            self._last_print[symbol] = now

    async def stop(self):
        self._stop.set()
        await asyncio.to_thread(self.conn.close)

# --------------- CLI entrypoint ---------------

def parse_args():
    parser = argparse.ArgumentParser(description="Upbit Order Book WebSocket Collector")
    parser.add_argument(
        "--symbols",
        help="Comma-separated list of Upbit symbol codes (e.g., 'USDT-BTC,USDT-ETH')."
             " If omitted, the script reads symbols from symbols.txt (or falls back to the database).",
    )
    parser.add_argument(
        "--print-spread",
        action="store_true",
        help="Print max-ask/min-bid spread once per second per symbol.",
    )
    parser.add_argument(
        "--store-interval",
        type=float,
        default=1.0,
        help="Minimum interval in seconds between DB writes per symbol (0 to store every update).",
    )
    return parser.parse_args()

def fetch_symbols_from_db(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT symbol_code FROM upbit_symbols ORDER BY symbol_code;")
        return [row[0] for row in cur.fetchall()]

def read_symbols_from_file(file_path: Path) -> List[str]:
    """Read symbols from a text file (one per line, case-insensitive). Lines beginning with '#' are ignored."""
    try:
        with file_path.open("r", encoding="utf-8") as f:
            return [
                line.strip().upper()
                for line in f
                if line.strip() and not line.lstrip().startswith("#")
            ]
    except FileNotFoundError:
        logging.warning(f"Symbols file {file_path} not found. Falling back to database.")
        return []

async def main_async():
    args = parse_args()
    
    # Check and create required tables before starting
    logging.info("Checking database tables...")
    tmp_conn = connect_db()
    try:
        check_and_create_tables(tmp_conn)
    finally:
        tmp_conn.close()
    
    # Determine symbols list according to precedence (CLI > file > DB)
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols_file = Path(__file__).with_name("symbols.txt")
        symbols = read_symbols_from_file(symbols_file)

        # Fallback to DB if file missing or empty
        if not symbols:
            tmp_conn = connect_db()
            symbols = fetch_symbols_from_db(tmp_conn)
            tmp_conn.close()

    if not symbols:
        logging.error("No symbols specified via --symbols, symbols.txt, or database.")
        sys.exit(1)

    streamer = UpbitOrderBookStreamer(
        symbols,
        print_spread=args.print_spread,
        store_interval=args.store_interval,
    )

    # Handle graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(streamer.stop()))

    await streamer.start()

def main():
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logging.info("Shutdown requested by user.")

if __name__ == "__main__":
    main() 