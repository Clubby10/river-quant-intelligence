"""Collect live CRYPTO/CURRENCY trades from Kraken's public WebSocket feed and write them ot q through a backlog worker."""

import asyncio
import json
import os
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

os.environ["PYKX_THREADING"] = "1"

import pykx as kx

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from .backlog_writer import run_writer
from .exchange_messages import extract_trades
from .rejections import RejectionLog

WEBSOCKET_URL = "wss://ws.kraken.com/v2"
SYMBOL = "BTC/USD"

CRYPTO, CURRENCY = SYMBOL.split("/")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / f"{CRYPTO.lower()}_{CURRENCY.lower()}_trades.jsonl"
SNAPSHOT_PATH = PROJECT_ROOT / "data" / "trades"
EXCHANGE_REJECTION_PATH = (PROJECT_ROOT / "data" / "rejected" / "exchange_messages.jsonl")

INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 30

async def collect_trades() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    trade_count = 0
    retry_delay = INITIAL_RETRY_DELAY
    rejections = RejectionLog(EXCHANGE_REJECTION_PATH)

    subscription = {
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": [SYMBOL],
            "snapshot": False,
        }
    }

    with OUTPUT_PATH.open("a", encoding="utf-8", buffering=1) as output:
        print(f"Writing trades to {OUTPUT_PATH}")
        print("CTRL + C to stop")

        try:
            while True:
                try:
                    async with connect(
                        WEBSOCKET_URL,
                        open_timeout=15,
                        close_timeout=5,
                    ) as websocket:
                        await websocket.send(json.dumps(subscription))

                        async for raw_message in websocket:
                            received_at = datetime.now(timezone.utc).isoformat()
                            records = extract_trades(raw_message, received_at, SYMBOL, rejections,)

                            try:
                                for record in records:
                                    output.write(json.dumps(record) + "\n")
                                    trade_count += 1
                            except OSError as error:
                                raise RuntimeError(f"Failed to save trade to {OUTPUT_PATH}: {error}") from error
                            
                            if records:
                                retry_delay = INITIAL_RETRY_DELAY

                            print(f"\rCollector: valid records={trade_count:,} | " f"rejection events={rejections.count:,}", end="", flush=True,)

                        raise ConnectionError("The exchange closed the connection.")
                    
                except (OSError, WebSocketException) as error:
                    print(f"\nConnection lost or unavailable: {error}. " f"Retrying in {retry_delay} secounds.", file=sys.stderr)

                    await asyncio.sleep(retry_delay)

                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
        finally:
            print(f"\n Collection ended. " f"Saved {trade_count:,} trades this run.")

async def main() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.touch(exist_ok=True)

    stop = threading.Event()

    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(collect_trades(), name="market-collector",)

            tasks.create_task(asyncio.to_thread(run_writer, OUTPUT_PATH, SNAPSHOT_PATH, stop,), name="database-writer",)

    finally:
        stop.set()

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
    except Exception as error:
        print(f"Ingestion stopped: {error}", file=sys.stderr)
        raise
    finally:
        kx.shutdown_thread()