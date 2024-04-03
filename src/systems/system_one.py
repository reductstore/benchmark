from systems.base_system import BaseSystem
from reduct import Client, BucketSettings

from config import (
    REDUCTSTORE_ENDPOINT,
    REDUCTSTORE_ACCESS_KEY,
    REDUCTSTORE_BUCKET,
    REDUCTSTORE_ENTRY,
)


class SystemOne(BaseSystem):
    @classmethod
    async def create(cls) -> "SystemOne":
        """Create ReductStore bucket."""
        self = cls()
        settings = BucketSettings()
        async with Client(REDUCTSTORE_ENDPOINT, REDUCTSTORE_ACCESS_KEY) as client:
            await client.create_bucket(REDUCTSTORE_BUCKET, settings, True)
        return self

    async def cleanup(self) -> None:
        """Delete ReductStore bucket."""
        async with Client(REDUCTSTORE_ENDPOINT, REDUCTSTORE_ACCESS_KEY) as client:
            bucket = await client.get_bucket(REDUCTSTORE_BUCKET)
            await bucket.remove()

    async def write_data(self, data: bytes, timestamp_ns: int) -> None:
        """Write data to ReductStore."""
        async with Client(REDUCTSTORE_ENDPOINT, REDUCTSTORE_ACCESS_KEY) as client:
            bucket = await client.get_bucket(REDUCTSTORE_BUCKET)
            await bucket.write(REDUCTSTORE_ENTRY, data, timestamp_ns // 1_000)

    async def read_last(self) -> bytes:
        """Read last data from ReductStore."""
        async with Client(REDUCTSTORE_ENDPOINT, REDUCTSTORE_ACCESS_KEY) as client:
            bucket = await client.get_bucket(REDUCTSTORE_BUCKET)
            async with bucket.read(REDUCTSTORE_ENTRY) as record:
                return await record.read_all()

    async def read_batch(self, start_ns: int) -> list[bytes]:
        """Read batch of data from ReductStore."""
        result = []
        async with Client(REDUCTSTORE_ENDPOINT, REDUCTSTORE_ACCESS_KEY) as client:
            bucket = await client.get_bucket(REDUCTSTORE_BUCKET)
            async for record in bucket.query(REDUCTSTORE_ENTRY, start_ns // 1_000):
                data: bytes = await record.read_all()
                result.append(data)
            return result
