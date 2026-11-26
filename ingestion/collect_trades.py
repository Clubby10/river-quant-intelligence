"""Collect live BTC/CAD trades from Kraken's public WebSocket feed"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import WebSocketException

WEBSOCKET_URL = "wss://ws.kraken.com/v2"
SYMBOL = "BTC/CAD"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "data" / "raw" / "btc_cad_trades.jsonl"

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

    subscription = {
        "method": "subscribe",
        "params": {
            "channel": "trade",
            "symbol": [SYMBOL],
            "snapshot": False,
        }
    }

    with OUTPUT_PATH.open("a", encoding="utf-8", buffering=1) as output:
        try:
            async with connect(
                WEBSOCKET_URL,
                open_timeout=15,
                close_timeout=5,
            ) as websocket:
                await websocket.send(json.dumps(subscription))

                print(f"Writing trades to {OUTPUT_PATH}")
                print("Ctrl + C to stop.")

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

                    for trade in message["data"]:
                        record = normalize_trades(trade, received_at)
                        output.write(json.dumps(record) + "\n")
                        trade_count += 1

                    print(f"\rTrades saved this run: {trade_count:,}", end="", flush=True,)

                raise RuntimeError("The exchange closed the connecion. Restart the collector.")

        finally:
            print(f"\nCollection ended. Saved {trade_count:,} trades this run.")

if __name__ == "__main__":
    try: 
        asyncio.run(collect_trades())
    except KeyboardInterrupt:
        print("Stopped by user.")
    except (OSError, WebSocketException, RuntimeError) as error:
        print(f"Collector error: {error}", file=sys.stderr)
        sys.exit(1)