"""Collect live CRYPTO/CURRENCY trades from Kraken's public WebSocket feed"""

import asyncio
import json
import sys
import threading
import pykx as kx
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from .backlog_writer import run_writer

WEBSOCKET_URL = "wss://ws.kraken.com/v2"
SYMBOL = "BTC/USD"

CRYPTO, CURRENCY = SYMBOL.split("/")

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / f"{CRYPTO.lower()}_{CURRENCY.lower()}_trades.jsonl"
SNAPSHOT_PATH = PROJECT_ROOT / "data" / "trades"

INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 30

def normalize_trades(trade: dict, received_at: str) -> dict:
    return {
        "exchange": "kraken",
        "symbol": trade["symbol"],
        "trade_id": trade["trade_id"],
        "exchange_timestamp": trade["timestamp"],
        "received_timestamp": received_at,
        "price": str(trade["price"]),
        "quantity": str(trade["qty"]),
        "side": trade["side"],
    }

async def collect_trades() -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    trade_count = 0
    retry_delay = INITIAL_RETRY_DELAY

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
                            message = json.loads(raw_message, parse_float=Decimal)

                            if message.get("method") == "subscribe":
                                if not message.get("success"):
                                    raise RuntimeError(f"Subscription failed: {message.get('error')}")
                                
                                print(f"Subscribed to {SYMBOL}.")
                                continue

                            if (message.get("channel") != "trade" or message.get("type") != "update"):
                                continue

                            records = [normalize_trades(trade, received_at) for trade in message["data"]]
                            try:
                                for record in records:
                                    output.write(json.dumps(record) + "\n")
                                    trade_count += 1
                            except OSError as error:
                                raise RuntimeError(f"Failed to save trade to {OUTPUT_PATH}: {error}") from error
                                
                            retry_delay = INITIAL_RETRY_DELAY

                            print(f"\rJSONL records: {trade_count:,}", end="", flush=True,)

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