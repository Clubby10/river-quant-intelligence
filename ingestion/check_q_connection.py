"""Check connection to q database"""

import pykx as kx

Q_HOST = "localhost"
Q_PORT = 5001

def main() -> None:
    with kx.SyncQConnection(host=Q_HOST, port=Q_PORT) as connection:
        row_count = connection("count trades").py()
        print(f"Connected to q. Trades in memory: {row_count}")

        print(connection("select from trades where i<5"))

if __name__ == "__main__":
    main()