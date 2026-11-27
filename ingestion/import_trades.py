"""Replay saved Kracken trades into q in bounded batches,
Skips exisiting trades and saves a snapshot after a successfuly replay
"""
import json 
from pathlib import Path
from _collections_abc import Iterator

import pykx as kx

from .q_writer import INSERT_TRADE, Q_HOST, Q_PORT
from .trade_types import to_q_row

BATCH_SIZE = 1_000

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "btc_usd_trades.jsonl"
SNAPSHOT_PATH = PROJECT_ROOT / "data" / "trades"

def iter_batches(path: Path, batch_size: int,) -> Iterator[kx.List]:
    if batch_size < 1:
        raise ValueError("Batch size must be at least 1.")

    batch = []

    with path.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                row = to_q_row(record)
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(f"Invalid trade on line {line_number}: {error}") from error
            
            batch.append(row)
            
            if len(batch) >= batch_size:
                yield batch
                batch = []

    if batch:
        yield batch

def save_snapshot(connection: kx.SyncQConnection, path: Path) -> None:
    snapshot_path = path.resolve()
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = snapshot_path.with_name(f"{snapshot_path.name}.replay.tmp")

    file_symbol = kx.SymbolAtom(f":{temporary_path}")
    connection("{[path] path set trades}", file_symbol)

    temporary_path.replace(snapshot_path)

def main() -> None:
    processed = 0 
    inserted = 0

    with kx.SyncQConnection(host=Q_HOST, port=Q_PORT) as connection:
        before = connection("count trades").py()

        for batch_number, batch in enumerate(iter_batches(INPUT_PATH, BATCH_SIZE), start=1):
            for row in batch:
                inserted += connection(INSERT_TRADE, row).py()
                processed += 1

            print(f"Batch {batch_number}: "
                f"processed={processed:,}, "
                f"inserted={inserted:,}, "
                f"skipped={processed - inserted:,}")
            
        if processed == 0:
            print("No trades found. Snapshot unchanged.")
            return

        after = connection("count trades").py()

        if after - before != inserted:
            raise RuntimeError(f"Expected {inserted} new rows, " f"but the table grew by {after - before}.")
        
        save_snapshot(connection, SNAPSHOT_PATH)

        print("\nReplay complete.")
        print(f"Records processed: {processed:,}")
        print(f"Inserted: {inserted:,}")
        print(f"Skipped duplicates: {processed - inserted:,}")
        print(f"Table rows: {before:,} -> {after:,}")
        print(f"Saved snapshot: {SNAPSHOT_PATH}")

if __name__ == "__main__":
    main()