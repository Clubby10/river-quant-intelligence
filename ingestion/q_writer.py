"""Write normalizied trades to q through an async IPC connection"""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncIterator

import pykx as kx

from .trade_types import to_q_row

Q_HOST = "localhost"
Q_PORT = 5001

INSERT_TRADE = (
    "{[r] "
    "if[any ((trades`sym)=r 2)&((trades`tradeId)=r 5); :0j]; "
    "`trades insert r; "
    "1j}"
)

class QWriter:
    def __init__(self, connection: kx.AsyncQConnection) -> None:
        self._connection = connection

    async def write_trade(self, record: dict) -> bool:
        row = to_q_row(record)
        result = await self._connection(INSERT_TRADE, row)
        return bool(result.py())
    
    async def count_trades(self) -> int:
        result = await self._connection("count trades")
        return result.py()
    
    async def save_snapshot(self, path: Path) -> None:
        snapshot_path = path.resolve()
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)

        file_symbol = kx.SymbolAtom(f":{snapshot_path}")
        await self._connection("{[path] path set trades}", file_symbol)

@asynccontextmanager
async def open_writer(host: str = Q_HOST, port: int = Q_PORT,) -> AsyncIterator[QWriter]:
    async with kx.AsyncQConnection(host=host, port=port) as connection:
        yield QWriter(connection)