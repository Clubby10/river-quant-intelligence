"""Replay and follow the rade log and reconnecting once q bcomes unavailable."""

import json
import threading

import pykx as kx
from pykx.exceptions import QError

from .import_trades import save_snapshot
from .q_writer import INSERT_TRADE, Q_HOST, Q_PORT
from .trade_types import to_q_row

from pathlib import Path
from time import monotonic

INITIAL_RETRY_DELAY = 1
MAX_RETRY_DELAY = 30
POLL_INTERVAL = 0.25
QUERY_TIMEOUT = 5.0

def is_connection_failure(error: Exception) -> bool:
    if isinstance(error, (ConnectionError, TimeoutError)):
        return True
    
    if isinstance(error, (QError, RuntimeError)):
        message = str(error).lower()

        return (message.startswith(("hop.", "recv.", "send."))
                or "query timed out" in message
                or "closed ipc connection" in message
                or "connection closed" in message)
    
    return False

def follow_file(connection: kx.SyncQConnection, path: Path, stop: threading.Event,) -> None:
    processed = 0
    inserted = 0
    last_reported = -1

    with path.open("rb") as source:
        while not stop.is_set():
            position = source.tell()
            line = source.readline()

            if not line or not line.endswith(b"\n"):
                source.seek(position)

                if processed != last_reported:
                    print(f"\nWriter: processed={processed:,}, "
                        f"inserted={inserted:,}, "
                        f"skipped={processed - inserted:,}",
                        flush=True,)
                    last_reported = processed

                stop.wait(POLL_INTERVAL)
                continue

            if not line.strip():
                continue

            try:
                record = json.loads(line)
                row = to_q_row(record)
            except (KeyError, TypeError, ValueError, OverflowError) as error:
                raise ValueError(f"Invalid trade at byte offset {position}: {error}") from error
            
            inserted += connection(INSERT_TRADE, row).py()
            processed += 1

            if processed % 1_000 == 0:
                print(f"\nWriter: processed={processed:,}, "
                    f"inserted={inserted:,}",
                    flush=True,)
                
def _run_writer(input_path: Path, snapshot_path: Path, stop: threading.Event,) -> None:
    retry_delay = INITIAL_RETRY_DELAY

    while not stop.is_set():
        attempt_started = monotonic()

        try:
            with kx.SyncQConnection(host=Q_HOST, port=Q_PORT, timeout=QUERY_TIMEOUT, connection_timeout=2.0, reconnection_attempts=-1,) as connection:
                row_count = connection("count trades").py()
                print(f"\nWriter connected. Exisiting rows: {row_count:,}." "Replaying the log.", flush=True)

                follow_file(connection, input_path, stop)

                save_snapshot(connection, snapshot_path)
                print(f"\nSaved snapshot: {snapshot_path}", flush=True)
                return
            
        except (OSError, QError, RuntimeError) as error:
            if not is_connection_failure(error):
                raise

            if stop.is_set():
                print(f"\nWriter stopped without saving a new snapshot: {error}. " "Recorded trades reamin in JSONL.", flush=True)
                return
            
            if monotonic() - attempt_started >= 30:
                retry_delay = INITIAL_RETRY_DELAY

            print(f"\nDatabase unavailable: {error}. " f"Retrying in {retry_delay}s; collection continues.", flush=True)

            if stop.wait(retry_delay):
                return
            
            retry_delay = min(retry_delay * 2, MAX_RETRY_DELAY)

def run_writer(input_path: Path, snapshot_path: Path, stop: threading.Event,) -> None:
    try:
        _run_writer(input_path, snapshot_path, stop)
    finally:
        kx.core.m9()