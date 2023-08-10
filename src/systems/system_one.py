from systems.base_system import BaseSystem
from reduct import Client, Bucket, BucketSettings

from config import (
    REDUCTSTORE_ENDPOINT,
    REDUCTSTORE_ACCESS_KEY,
    REDUCTSTORE_BUCKET,
    REDUCTSTORE_ENTRY,
)


class SystemOne(BaseSystem):
    def __init__(self):
        """Initialize ReductStore client."""
        self.client = Client(
            url=REDUCTSTORE_ENDPOINT,
            api_token=REDUCTSTORE_ACCESS_KEY,
        )

    @classmethod
    async def create(cls: int):
        """Create ReductStore bucket."""
        self = cls()
        settings = BucketSettings()
        self.bucket: Bucket = await self.client.create_bucket(
            REDUCTSTORE_BUCKET, settings=settings, exist_ok=True
        )
        return self

    async def cleanup(self):
        """Delete ReductStore bucket."""
        await self.bucket.remove()

    async def write_data(self, data, timestamp):
        """Write data to ReductStore."""
        await self.bucket.write(REDUCTSTORE_ENTRY, data, timestamp)

    async def read_last(self):
        """Read last data from ReductStore."""
        async with self.bucket.read(REDUCTSTORE_ENTRY) as record:
            return await record.read_all()

    async def read_batch(self, start):
        """Read batch of data from ReductStore."""
        result = []
        async for record in self.bucket.query(REDUCTSTORE_ENTRY, start=start):
            data: bytes = await record.read_all()
            result.append(data)
        return result
