"""Check connection to q database"""

import asyncio
from pathlib import Path

from .q_writer import open_writer

async def main() -> None:
    snapshot_path = Path(__file__).resolve().parents[1] / "data"/ "trades"
    
    async with open_writer() as writer:
        row_count = await writer.count_trades()
        print(f"Connected to q. Trades in memory: {row_count}")

        await writer.save_snapshot(snapshot_path)
        print(f"Saved snapshot: {snapshot_path}")

if __name__ == "__main__":
    asyncio.run(main())