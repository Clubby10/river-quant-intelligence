"""Validate normalized trade records before conversion to q types."""

import math
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

SUPPORTED_SYMBOLS = frozenset({"BTC/USD"})

REQUIRED_FIELDS = frozenset({"exchange",
                            "symbol",
                            "trade_id",
                            "exchange_timestamp",
                            "received_timestamp",
                            "price",
                            "quantity",
                            "side",})

MAX_FINITE_LONG = (2**63) - 2

UTC_TIMESTAMP = re.compile(
    r"(?P<whole>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<fraction>\d{1,9}))?"
    r"(?:Z|\+00:00)"
)

def validate_positive_number(value: object, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError(f"{field} must be a number or numeric string.")
    
    try:
        number = Decimal(str(value))
    except InvalidOperation as error:
        raise ValueError(f"{field} is not a valud number: {value!r}") from error
    
    if not number.is_finite() or number <= 0:
        raise ValueError(f"{field} must be finite and greater than zero.")
    
    as_float = float(number)

    if not math.isfinite(as_float) or as_float <= 0:
        raise ValueError(f"{field} cannot be represented as a positive finite q float.")
    
def validate_timestamp(value: object, field: str) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a UTC timestamp string.")
    
    match = UTC_TIMESTAMP.fullmatch(value)

    if match is None:
        raise ValueError(f"{field} must be a complete UTC timestamp ending in Z or +00:00.")
    
    try:
        whole_secounds = datetime.strptime(match["whole"], "%Y-%m-%dT%H:%M:%S",)
    except ValueError as error:
        raise ValueError(f"{field} contains an invalid date or time.") from error
    
    fraction_ns = int((match["fraction"] or "").ljust(9, "0"))

    for epoch in (datetime(1970, 1, 1), datetime(2000, 1, 1)):
        elapsed = whole_secounds - epoch
        nanoseconds = ((elapsed.days * 86_400 + elapsed.seconds) * 1_000_000_000 + fraction_ns)

        if not -MAX_FINITE_LONG <= nanoseconds <= MAX_FINITE_LONG:
            raise ValueError(f"{field} is outside the supported timestamp range.")
        
def validate_trade(record: object) -> None:
    if not isinstance(record, dict):
        raise ValueError("A trade record must be a JSON object.")
    
    missing = REQUIRED_FIELDS.difference(record)

    if missing:
        raise ValueError(f"Missing required fields: {', '.join(sorted(missing))}")
    
    if record["exchange"] != "kraken":
        raise ValueError("Only Kraken trades are currently supported.")
    
    symbol = record["symbol"]

    if not isinstance(symbol, str) or symbol not in SUPPORTED_SYMBOLS:
        raise ValueError(f"Unsupported trading pair: {symbol!r}")
    
    trade_id = record["trade_id"]

    if type(trade_id) is not int or not 0 < trade_id <= MAX_FINITE_LONG:
        raise ValueError("trade_id must be a positive finite q long integer.")
    
    if record["side"] not in ("buy", "sell"):
        raise ValueError("side must be 'buy' or 'sell'.")
    
    validate_positive_number(record["price"], "price")
    validate_positive_number(record["quantity"], "quantity")

    validate_timestamp(record["exchange_timestamp"], "exchange_timestamp")
    validate_timestamp(record["received_timestamp"], "received_timestamp")