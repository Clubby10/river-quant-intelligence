"""Decode Kraken messages and keep valid trades from each update."""

import json
from decimal import Decimal

from .rejections import RejectionLog
from .trade_validation import validate_trade

SOURCE = "kraken:websocket-v2"

def reject_json_constant(value: str) -> None:
    raise ValueError(f"Invalid JSON numeric constant: {value}")

def normalize_trade(trade:object, received_at: str) -> dict:
    if not isinstance(trade, dict):
        raise ValueError("Each trade must be a JSON object.")
    
    record = {
        "exchange": "kraken",
        "symbol": trade["symbol"],
        "trade_id": trade["trade_id"],
        "exchange_timestamp": trade["timestamp"],
        "received_timestamp": received_at,
        "price": str(trade["price"]),
        "quantity": str(trade["qty"]),
        "side": trade["side"],
    }

    validate_trade(record)

    record["price"] = str(record["price"])
    record["quantity"] = str(record["quantity"])

    return record

def extract_trades(raw_meassge: str | bytes, received_at: str, expected_symbol: str, rejections: RejectionLog) -> list[dict]:
    raw = (raw_meassge.encode("utf-8") if isinstance(raw_meassge, str) else raw_meassge)

    try:
        message = json.loads(raw.decode("utf-8"), parse_float=Decimal, parse_constant=reject_json_constant,)

        if not isinstance(message, dict):
            raise ValueError("The exchange message must be a JSON object")
        
        if message.get("method") == "subscribe":
            success = message.get("success")

            if success is False:
                raise RuntimeError(f"Subscription failed: {message.get('error')}")
            
            if success is not True:
                raise ValueError("Subscritpion response lacks a boolean success.")
            
            result = message.get("result")

            if not isinstance(result, dict) or result.get("channel") != "trade" or result.get("symbol") != expected_symbol:
                raise ValueError("Unexpected subscription response.")
            
            print(f"\nSubscribed to {expected_symbol}.", flush=True)
            return []
        
        channel = message.get("channel")

        if channel in ("heartbeat", "status"):
            return []
        
        if channel != "trade":
            raise ValueError(f"Unexpected or missing channel: {channel!r}")
        
        message_type = message.get("type")

        if message_type not in ("snapshot", "update"):
            raise ValueError(f"Invalid trade message type: {message_type!r}")
        
        trades = message.get("data")

        if not isinstance(trades, list):
            raise ValueError("Trade message data must be a list.")
        
        if message_type == "snapshot":
            return []
        
    except (UnicodeError, ValueError) as error:
        rejections.write(raw, SOURCE, str(error))
        return []
    
    records = []

    for index, trade in enumerate(trades):
        try:
            record = normalize_trade(trade, received_at)

            if record["symbol"] != expected_symbol:
                raise ValueError(f"Expected {expected_symbol}, got {record['symbol']}.")
            
        except (KeyError, ValueError) as error:
            rejections.write(raw, SOURCE, f"Invalid trade: {error}", item_index=index,)
            continue

        records.append(record)

    return records 