"""Parse trade records and log rejected input before continuing"""

import base64
import json
from datetime import datetime, timezone
from pathlib import Path

from .trade_validation import validate_trade

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REJECTION_PATH = PROJECT_ROOT / "data" / "rejected" / "trades.jsonl"

class RejectionLog:
    def __init__(self, path: Path = REJECTION_PATH) -> None:
        self.path = path
        self.count = 0

    def parse(self, raw: bytes, source: Path, offset: int,) -> dict | None:
        try:
            record = json.loads(raw.decode("utf-8"))
            validate_trade(record)
        except (UnicodeError, ValueError) as error:
            entry = {"rejected_at": datetime.now(timezone.utc).isoformat(),
                    "source": str(source.resolve()),
                    "byte_offset": offset,
                    "reason": str(error),
                    "raw_text": raw.decode("utf-8", errors="replace"),
                    "raw_base64": base64.b64encode(raw).decode("ascii"),}
            
            self.path.parent.mkdir(parents=True, exist_ok=True)

            with self.path.open("a", encoding="utf-8") as output:
                output.write(json.dumps(entry, allow_nan=False) + "\n")

            self.count += 1
            return None
        
        return record
    
