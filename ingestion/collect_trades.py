"""Collect live CRYPTO/CURRENCY trades from Kraken's public WebSocket feed"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

from pykx.exceptions import QError

from .q_writer import QWriter, open_writer

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

async def collect_trades(writer: QWriter) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    trade_count = 0
    inserted_count = 0
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
                            
                            for record in records:
                                try:
                                    inserted = await writer.write_trade(record)
                                except (OSError, QError) as error:
                                    raise RuntimeError("Databse write failed. " "The received batch was recored in JSONL") from error
                                
                                inserted_count += int(inserted)
                                
                            retry_delay = INITIAL_RETRY_DELAY

                            print(f"\rJSONL records: {trade_count:,} | " f"New q rows: {inserted_count}", end="", flush=True,)

                        raise ConnectionError("The exchange closed the connection.")
                    
                except (OSError, WebSocketException) as error:
                    print(f"\nConnection lost or unavailable: {error}. " f"Retrying in {retry_delay} secounds.", file=sys.stderr)

                    await asyncio.sleep(retry_delay)

                    retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)
        finally:
            print(f"\n Collection ended. " f"Saved {trade_count:,} trades this run.")

async def main() -> None:
    async with open_writer() as writer:
        print(f"Connected to q. Exisiting rows: {await writer.count_trades()}")

        try:
            await collect_trades(writer)
        finally:
            print("\nSaving databse snapshot...")
            await writer.save_snapshot(SNAPSHOT_PATH)
            print(f"Saved snapshot: {SNAPSHOT_PATH}")

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped by user.")
    except (OSError, WebSocketException, RuntimeError) as error:
        print(f"Collector error: {error}", file=sys.stderr)
        sys.exit(1)