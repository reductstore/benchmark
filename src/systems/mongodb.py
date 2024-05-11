from datetime import datetime
from typing import List

from config import MONGODB_DATABASE, MONGODB_USER, MONGODB_PASSWORD, MONGODB_ENDPOINT
from gridfs import GridFS
from pymongo import MongoClient
from systems.base_system import BaseSystem

MONGODB_URI = f"mongodb://{MONGODB_USER}:{MONGODB_PASSWORD}@{MONGODB_ENDPOINT}"


class MongoDBSystem(BaseSystem):
    @classmethod
    async def create(cls):
        return cls()

    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DATABASE]
        self.setup_database()

    def setup_database(self):
        if "data" not in self.db.list_collection_names():
            self.db.create_collection(
                "data",
                timeseries={
                    "timeField": "time",
                    "granularity": "seconds",
                },
            )
        self.fs = GridFS(self.db)

    async def cleanup(self) -> None:
        self.db.drop_collection("data")

    async def write_data(self, data: bytes, timestamp_ns: int) -> None:
        timestamp = datetime.fromtimestamp(timestamp_ns / 1e9)
        blob_id = self.fs.put(data, filename=f"blob_{timestamp.isoformat()}")
        self.db["data"].insert_one({"time": timestamp, "blob_id": blob_id})

    async def read_last(self) -> bytes:
        last_record_cursor = self.db["data"].find().sort("time", -1).limit(1)
        last_record = last_record_cursor.next()
        if last_record:
            blob = self.fs.get(last_record["blob_id"]).read()
            return blob
        return b""

    async def read_batch(self, start_ns: int) -> List[bytes]:
        start_time = datetime.fromtimestamp(start_ns / 1e9)
        cursor = self.db["data"].find({"time": {"$gte": start_time}})
        return [self.fs.get(record["blob_id"]).read() for record in cursor]
