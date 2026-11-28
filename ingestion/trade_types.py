"""Convert normalized Kraken trade records into types q rows"""

import numpy as np
import pykx as kx

from .trade_validation import validate_trade

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
    validate_trade(record)
    
    return kx.List([
        to_q_timestamp(record["exchange_timestamp"]),
        to_q_timestamp(record["received_timestamp"]),
        kx.SymbolAtom(record["symbol"]),
        kx.FloatAtom(float(record["price"])),
        kx.FloatAtom(float(record["quantity"])),
        kx.LongAtom(int(record["trade_id"])),
    ])