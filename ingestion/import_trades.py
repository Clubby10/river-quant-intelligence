"""Import a small batch of saved Kraken trades into a running q database"""
import json 
from pathlib import Path

import numpy as np
import pykx as kx

Q_HOST = "localhost"
Q_PORT = 5001
IMPORT_LIMIT = 10 # testing value

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_PATH = PROJECT_ROOT / "data" / "raw" / "btc_usd_trades.jsonl"
SNAPSHOT_PATH = PROJECT_ROOT / "data" / "trades"

