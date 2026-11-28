"""Track ingestion progres safely across the collector and writer thread."""

import threading
from dataclasses import dataclass, replace
from time import monotonic
from typing import Literal

@dataclass
class HealthSnapshot:
    collected: int = 0 
    inserted: int = 0 
    duplicates: int = 0
    exchange_rejected: int = 0
    writer_rejected: int = 0

    writer_offset: int = 0

    exchange_state: str = "startng"
    database_sate: str = "starting"

    exchange_reconnects: int = 0
    database_reconnects: int = 0

    last_trade_at: float | None = None

class IngestionHealth:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = HealthSnapshot()
        self._connected_once = {"exchange": False, "database": False,}

    def snapshot(self) -> HealthSnapshot:
        with self._lock:
            return replace(self._state)
        
    def record_collected(self, count: int = 1) -> None:
        if count < 0:
            raise ValueError("Collected count cannot be negative.")
        
        if count == 0:
            return
        
        with self._lock:
            self._state.collected += count
            self._state.last_trade_at = monotonic()

    def record_exchange_rejections(self, count: int) -> None:
        if count < 0:
            raise ValueError("Rejection count cannot be negative.")
        
        with self._lock:
            self._state.exchange_rejected += count

    def record_writer_progress(self, offset: int, outcome: Literal["inserted", "duplicate", "rejected", "blank"],) -> None:
        if offset < 0:
            raise ValueError("Writer offset cannot be negative.")
        
        if outcome not in ("inserted", "duplicate", "rejected", "blank"):
            raise ValueError(f"Unknown writer outcome: {outcome}")
        
        with self._lock:
            self._state.writer_offset = offset

            if outcome == "inserted":
                self._state.inserted += 1
            elif outcome == "duplicate":
                self._state.duplicates += 1
            elif outcome == "rejected":
                self._state.writer_rejected += 1

    def set_connection_state(self, service: Literal["exchange", "database"], state: Literal["connecting", "connected", "retrying", "stopped"],) -> None:
        if service not in ("exchange", "database"):
            raise ValueError(f"Unknown service: {service}")
        
        if state not in ("connecting", "connected", "retrying", "stopped"):
            raise ValueError(f"Unknown connection state: {state}")
        
        state_field = f"{service}_sate"
        reconnect_field = f"{service}_reconnects"

        with self._lock:
            previous = getattr(self._state, state_field)

            if state == "connected" and previous != "connected":
                if self._connected_once[service]:
                    reconnects = getattr(self._state, reconnect_field)
                    setattr(self._state, reconnect_field, reconnects + 1)

                self._connected_once[service] = True

            setattr(self._state, state_field, state)

            if service == "database" and state in ("connecting", "retrying"):
                self._state.writer_offset = 0
            