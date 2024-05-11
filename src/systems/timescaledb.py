from datetime import datetime
from typing import List

import psycopg2
from config import TIMESCALE_DATABASE, TIMESCALE_USER, TIMESCALE_PASSWORD, TIMESCALE_ENDPOINT
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from systems.base_system import BaseSystem


TIMESCALE_CONNECTION=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASSWORD}@{TIMESCALE_ENDPOINT}"

class TimescaleDBSystem(BaseSystem):
    @classmethod
    async def create(cls):
        return cls()

    def __init__(self):
        self.con = psycopg2.connect(TIMESCALE_CONNECTION)
        self.con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = self.con.cursor()
        cur.execute(f"DROP DATABASE IF EXISTS {TIMESCALE_DATABASE}")
        cur.execute(f"CREATE DATABASE {TIMESCALE_DATABASE}")
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
        cur.execute(
            """CREATE TABLE IF NOT EXISTS data (
                time TIMESTAMPTZ NOT NULL,
                data BYTEA NOT NULL
                );
            """
        )
        self.con.commit()

    async def cleanup(self) -> None:
        with self.con.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS data;")
            cur.execute(f"DROP database IF EXISTS {TIMESCALE_DATABASE};")

    async def write_data(self, data: bytes, timestamp_ns: int) -> None:
        with self.con.cursor() as cur:
            cur.execute(
                "INSERT INTO data (time, data) VALUES (%s, %s);",
                (
                    datetime.fromtimestamp(timestamp_ns / 1e9),
                    psycopg2.Binary(data),
                ),
            )

    async def read_last(self) -> bytes:
        with self.con.cursor() as cur:
            cur.execute("SELECT data FROM data ORDER BY time DESC LIMIT 1;")
            obj = cur.fetchone()[0]
            return bytes(obj)

    async def read_batch(self, start_ns: int) -> List[bytes]:
        with self.con.cursor() as cur:
            cur.execute(
                "SELECT data FROM data WHERE time >= %s;",
                (datetime.fromtimestamp(start_ns / 1e9),),
            )
            return [bytes(row[0]) for row in cur.fetchall()]
