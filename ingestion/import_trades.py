"""Import a small batch of saved Kraken trades into a running q database"""
import json 
from pathlib import Path

import numpy as np
import pykx as kx

Q_HOST = "localhost"
Q_PORT = 5001
IMPORT_LIMIT = 10 # testing value

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "btc_usd_trades.jsonl"
SNAPSHOT_PATH = PROJECT_ROOT / "data" / "trades"

def to_q_timestamp(value: str) -> kx.TimestampAtom:
    if value.endswith("Z"):
        utc_value = value[:-1]
    elif value.endswith("+00:00"):
        utc_value = value[:-6]
    else:
        raise ValueError(f"Expected a UTC timestamp, got: {value}")
    
    timestamp = np.datetime64(utc_value, "ns")

    if np.isnat(timestamp):
        raise ValueError(f"Invalid timestamp: {value}")
    
    return kx.TimestampAtom(timestamp)

def to_q_row(record: dict) -> kx.List:
    if record["exchange"] != "kraken":
        raise ValueError("THis importer currently only support Kraken trades only.")
    
    return kx.List([
        to_q_timestamp(record["exchange_timestamp"]),
        to_q_timestamp(record["received_timestamp"]),
        kx.SymbolAtom(record["symbol"]),
        kx.FloatAtom(float(record["price"])),
        kx.FloatAtom(float(record["quantity"])),
        kx.LongAtom(int(record["trade_id"])),
    ])

def load_rows() -> list[kx.List]:
    rows = []

    with INPUT_PATH.open("r", encoding="utf-8") as source:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
                rows.append(to_q_row(record))
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError(f"Invalid trade on line {line_number}: {error}") from error
            
            if len(rows) >= IMPORT_LIMIT:
                break

    return rows 

INSERT_TRADE = (
    "{[r] "
    "if[any ((trades`sym)=r 2)&((trades`tradeId)=r 5); :0j]; "
    "`trades insert r; "
    "1j}"
)

def main() -> None:
    rows = load_rows()

    if not rows:
        print("No trades found in the input file.")
        return
    
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with kx.SyncQConnection(host=Q_HOST, port=Q_PORT) as connection:
        before = connection("count trades").py()
        inserted = 0

        for row in rows:
            inserted += connection(INSERT_TRADE, row).py()

        after = connection("count trades").py()

        if after - before != inserted:
            raise RuntimeError("The row-count change did not match the inserts.")
        
        snapshot = kx.SymbolAtom(f":{SNAPSHOT_PATH}")
        connection("{[path] path set trades}", snapshot)

        print(f"Records read: {len(rows)}")
        print(f"Inserted: {inserted}")
        print(f"Skipped duplicates: {len(rows) - inserted}")
        print(f"Table rows: {before} -> {after}")
        print(f"Saved snapshot: {SNAPSHOT_PATH}")

if __name__ == "__main__":
    main()