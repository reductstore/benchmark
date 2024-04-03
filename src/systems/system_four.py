from pymongo import MongoClient, ASCENDING
from gridfs import GridFS
from datetime import datetime

from config import (
    MONGODB_URI,
    MONGODB_DATABASE,
    MONGODB_COLLECTION,
    MONGODB_GRIDFS_BUCKET,
)


class SystemFour:
    def __init__(self):
        self.client = MongoClient(MONGODB_URI)
        self.db = self.client[MONGODB_DATABASE]
        self.collection = self.db[MONGODB_COLLECTION]
        self.fs = GridFS(self.db, MONGODB_GRIDFS_BUCKET)
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Ensure that necessary indexes are created."""
        self.collection.create_index([("timestamp", ASCENDING)])

    async def cleanup(self) -> None:
        """Cleanup the database by dropping the collection and GridFS bucket."""
        self.collection.drop()
        self.db[MONGODB_GRIDFS_BUCKET].chunks.drop()
        self.db[MONGODB_GRIDFS_BUCKET].files.drop()

    async def write_data(self, data: bytes, timestamp_ns: int) -> None:
        """Write data to MongoDB using GridFS."""
        timestamp = datetime.fromtimestamp(timestamp_ns / 1_000_000_000)
        blob_id = self.fs.put(data)
        self.collection.insert_one({"timestamp": timestamp, "blob_id": blob_id})

    async def read_last(self) -> bytes:
        """Read the last data blob inserted into MongoDB."""
        last_record = self.collection.find().sort("timestamp", -1).limit(1)
        if last_record.count() > 0:
            last_record = last_record[0]
            return self.fs.get(last_record["blob_id"]).read()
        return b""

    async def read_batch(self, start_ns: int) -> list[bytes]:
        """Read a batch of data starting from a specific timestamp."""
        start_time = datetime.fromtimestamp(start_ns / 1_000_000_000)
        cursor = self.collection.find({"timestamp": {"$gte": start_time}})
        result = []
        for record in cursor:
            data = self.fs.get(record["blob_id"]).read()
            result.append(data)
        return result
