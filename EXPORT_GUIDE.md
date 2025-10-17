# Upbit Data Export Guide

This guide explains how to export your PostgreSQL data to Parquet files on your resource-constrained remote machine (2GB RAM, 1 VCPU) and transfer it to your local machine.

## Overview

The export process:
1. **Streams data** from PostgreSQL using server-side cursors (memory-efficient)
2. **Writes compressed Parquet files** directly (no intermediate formats)
3. **Processes symbol-by-symbol** to minimize memory usage (~200MB max)
4. **Transfers** compressed files to local machine via rsync/scp

**Estimated time:** 5-8 hours total (4-6 hours export + 10-90 minutes transfer)

---

## Step 1: Transfer Script to Remote Machine

From your **local machine**, run:

```bash
# Set your remote machine connection details
REMOTE_USER="ubuntu"
REMOTE_HOST="ip-172-31-20-33"  # or use IP address

# Transfer the export script and requirements
scp export_to_parquet.py requirements_export.txt ${REMOTE_USER}@${REMOTE_HOST}:~/projects/upbit_book_order/
```

---

## Step 2: Install Dependencies on Remote Machine

SSH into the remote machine:

```bash
ssh ${REMOTE_USER}@${REMOTE_HOST}
```

Then install Python dependencies:

```bash
cd ~/projects/upbit_book_order
pip3 install --user -r requirements_export.txt
```

**Verify installation:**

```bash
python3 -c "import pandas, pyarrow, psycopg2; print('✓ All dependencies installed')"
```

---

## Step 3: Update Database Password (if needed)

If your database password is different from "pass", update it in the script:

```bash
# Edit the DB_CONFIG section in export_to_parquet.py
nano export_to_parquet.py
# Or use your password from environment variable:
# sed -i 's/"password": "pass"/"password": "'$DB_PASSWORD'"/' export_to_parquet.py
```

---

## Step 4: Run the Export

### Option A: Export All Symbols (Recommended)

```bash
cd ~/projects/upbit_book_order
python3 export_to_parquet.py --output-dir ./parquet_export --chunk-size 100000
```

### Option B: Export Specific Symbols

```bash
# Export only BTC and ETH (faster for testing)
python3 export_to_parquet.py --output-dir ./parquet_export --symbols "USDT-BTC,USDT-ETH"
```

### Option C: Run in Background (Recommended for Long Exports)

```bash
# Run in background with nohup
nohup python3 export_to_parquet.py --output-dir ./parquet_export > export.log 2>&1 &

# Monitor progress
tail -f export.log

# Or use screen/tmux for resumable session
screen -S export
python3 export_to_parquet.py --output-dir ./parquet_export
# Detach with Ctrl+A, D
# Reattach with: screen -r export
```

---

## Step 5: Monitor Progress

The script provides detailed progress logging:

```bash
# Watch the log file in real-time
tail -f export_to_parquet.log

# Or check the status
tail -n 50 export_to_parquet.log
```

**Example output:**
```
2025-10-17 08:00:00 [INFO] ✓ Connected to database
2025-10-17 08:00:01 [INFO] ✓ Exported 6 symbols to parquet_export/symbols.parquet
2025-10-17 08:00:02 [INFO] Processing USDT-ADA (1/6)
2025-10-17 08:00:05 [INFO] Exporting snapshots for USDT-ADA...
2025-10-17 08:05:30 [INFO]   Progress: 500,000 snapshots exported for USDT-ADA
2025-10-17 08:10:45 [INFO]   Progress: 1,000,000 snapshots exported for USDT-ADA
...
```

---

## Step 6: Transfer Files to Local Machine

Once export is complete, transfer the Parquet files from remote to local machine.

### Option A: Using rsync (Recommended - Resumable)

From your **local machine**:

```bash
# Create local directory for parquet files
mkdir -p ~/upbit_data_parquet

# Transfer with rsync (supports resume if interrupted)
rsync -avz --progress ${REMOTE_USER}@${REMOTE_HOST}:~/projects/upbit_book_order/parquet_export/ ~/upbit_data_parquet/

# Verify transfer
ls -lh ~/upbit_data_parquet/
```

### Option B: Using scp

```bash
# Create local directory
mkdir -p ~/upbit_data_parquet

# Transfer all parquet files
scp -r ${REMOTE_USER}@${REMOTE_HOST}:~/projects/upbit_book_order/parquet_export/*.parquet ~/upbit_data_parquet/

# Verify transfer
ls -lh ~/upbit_data_parquet/
```

### Option C: Using tar + compression (Fastest for many small files)

On **remote machine**:

```bash
cd ~/projects/upbit_book_order
tar -czf parquet_export.tar.gz parquet_export/
```

On **local machine**:

```bash
# Transfer compressed archive
scp ${REMOTE_USER}@${REMOTE_HOST}:~/projects/upbit_book_order/parquet_export.tar.gz ~/

# Extract
cd ~
tar -xzf parquet_export.tar.gz
mv parquet_export ~/upbit_data_parquet

# Verify
ls -lh ~/upbit_data_parquet/
```

---

## Step 7: Verify Data Locally

Create a simple verification script on your local machine:

```python
# verify_parquet.py
import pandas as pd
from pathlib import Path

parquet_dir = Path("~/upbit_data_parquet").expanduser()

# Load symbols
symbols_df = pd.read_parquet(parquet_dir / "symbols.parquet")
print(f"\n✓ Symbols table: {len(symbols_df)} symbols")
print(symbols_df)

# Check each symbol's data
for _, symbol_row in symbols_df.iterrows():
    symbol_code = symbol_row["symbol_code"]
    symbol_file = symbol_code.replace("-", "_")

    # Load snapshot data
    snapshot_file = parquet_dir / f"snapshots_{symbol_file}.parquet"
    if snapshot_file.exists():
        snapshots_df = pd.read_parquet(snapshot_file)
        print(f"\n✓ {symbol_code} snapshots: {len(snapshots_df):,} rows")
        print(f"  Date range: {pd.to_datetime(snapshots_df['timestamp'], unit='ms').min()} to {pd.to_datetime(snapshots_df['timestamp'], unit='ms').max()}")

    # Load orderbook data
    orderbook_file = parquet_dir / f"orderbook_{symbol_file}.parquet"
    if orderbook_file.exists():
        orderbook_df = pd.read_parquet(orderbook_file)
        print(f"  Orderbook rows: {len(orderbook_df):,}")
        print(f"  Levels per snapshot: {orderbook_df.groupby('snapshot_id')['unit_level'].count().mean():.1f} avg")

print("\n" + "="*60)
print("VERIFICATION COMPLETE")
print("="*60)
```

Run verification:

```bash
python3 verify_parquet.py
```

---

## File Structure

After export, you'll have these files:

```
parquet_export/
├── symbols.parquet                    # Symbol metadata (small)
├── snapshots_USDT_ADA.parquet        # Snapshots for USDT-ADA
├── snapshots_USDT_BCH.parquet        # Snapshots for USDT-BCH
├── snapshots_USDT_BTC.parquet        # Snapshots for USDT-BTC
├── snapshots_USDT_ETH.parquet        # Snapshots for USDT-ETH
├── snapshots_USDT_TRX.parquet        # Snapshots for USDT-TRX
├── snapshots_USDT_XRP.parquet        # Snapshots for USDT-XRP
├── orderbook_USDT_ADA.parquet        # Orderbook data for USDT-ADA
├── orderbook_USDT_BCH.parquet        # Orderbook data for USDT-BCH
├── orderbook_USDT_BTC.parquet        # Orderbook data for USDT-BTC
├── orderbook_USDT_ETH.parquet        # Orderbook data for USDT-ETH
├── orderbook_USDT_TRX.parquet        # Orderbook data for USDT-TRX
└── orderbook_USDT_XRP.parquet        # Orderbook data for USDT-XRP
```

---

## Working with Parquet Files Locally

### Reading Data

```python
import pandas as pd

# Read symbols
symbols = pd.read_parquet("~/upbit_data_parquet/symbols.parquet")

# Read snapshots for a specific symbol
btc_snapshots = pd.read_parquet("~/upbit_data_parquet/snapshots_USDT_BTC.parquet")

# Read orderbook data
btc_orderbook = pd.read_parquet("~/upbit_data_parquet/orderbook_USDT_BTC.parquet")

# Query specific time range
import pyarrow.parquet as pq

# Read with filters (very efficient with Parquet)
table = pq.read_table(
    "~/upbit_data_parquet/orderbook_USDT_BTC.parquet",
    filters=[("timestamp", ">=", 1718712000000)]  # Filter by timestamp
)
df = table.to_pandas()
```

### Memory-Efficient Loading

```python
import pyarrow.parquet as pq

# Read only specific columns
columns = ["timestamp", "ask_price", "bid_price", "unit_level"]
df = pd.read_parquet("~/upbit_data_parquet/orderbook_USDT_BTC.parquet", columns=columns)

# Or use PyArrow for even better memory control
table = pq.read_table(
    "~/upbit_data_parquet/orderbook_USDT_BTC.parquet",
    columns=columns
)
```

---

## Troubleshooting

### Out of Memory Error

If you encounter memory issues on the remote machine:

```bash
# Reduce chunk size
python3 export_to_parquet.py --chunk-size 50000
```

### Connection Timeout During Transfer

Use rsync with compression and resume capability:

```bash
rsync -avz --timeout=300 --partial --progress \
  ${REMOTE_USER}@${REMOTE_HOST}:~/projects/upbit_book_order/parquet_export/ \
  ~/upbit_data_parquet/
```

### Disk Space Issues on Remote

Check available space:

```bash
df -h ~/projects/upbit_book_order
```

Export directly to a different disk if needed:

```bash
python3 export_to_parquet.py --output-dir /mnt/external/parquet_export
```

Or delete parquet files immediately after verification:

```bash
# After transferring each symbol, delete on remote
rm ~/projects/upbit_book_order/parquet_export/snapshots_USDT_BTC.parquet
```

---

## Performance Notes

- **Memory usage:** ~200MB max during export (safe for 2GB RAM)
- **Export speed:** ~500K-1M rows/minute (depends on data complexity)
- **Compression ratio:** Parquet with Snappy is typically 5-10x smaller than CSV
- **Network transfer:** Compressed Parquet = ~10-20GB (estimated)

---

## Clean Up (Optional)

After successful transfer and verification:

### On Remote Machine

```bash
# Remove exported parquet files
rm -rf ~/projects/upbit_book_order/parquet_export/

# Remove export script (keep if you want to re-export later)
rm ~/projects/upbit_book_order/export_to_parquet.py
rm ~/projects/upbit_book_order/requirements_export.txt
```

### On Local Machine

```bash
# Archive the original parquet files
tar -czf upbit_data_parquet_backup.tar.gz ~/upbit_data_parquet/

# Move to long-term storage
mv upbit_data_parquet_backup.tar.gz /path/to/backup/
```
