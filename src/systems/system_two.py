import io
import aiohttp

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from miniopy_async import Minio

from systems.base_system import BaseSystem
from utils import to_rfc3339

from config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_IS_SECURE,
    MINIO_BUCKET,
)

from config import (
    INFLUXDB_ENDPOINT,
    INFLUXDB_TOKEN,
    INFLUXDB_ORG,
    INFLUXDB_BUCKET,
    INFLUXDB_MEASUREMENT,
    INFLUXDB_FIELD,
)


class SystemTwo(BaseSystem):
    def __init__(self):
        """Initialize Minio and InfluxDB clients."""
        self.minio_client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_IS_SECURE,
        )
        self.influx_client = InfluxDBClient(
            url=INFLUXDB_ENDPOINT,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
        )

    @classmethod
    async def create(cls):
        """Create Minio and InfluxDB buckets."""
        self = cls()

        found = await self.minio_client.bucket_exists(MINIO_BUCKET)
        if not found:
            await self.minio_client.make_bucket(MINIO_BUCKET)

        found = self.influx_client.buckets_api().find_bucket_by_name(INFLUXDB_BUCKET)
        if not found:
            self.influx_client.buckets_api().create_bucket(
                org=INFLUXDB_ORG, bucket_name=INFLUXDB_BUCKET
            )

        return self

    async def cleanup(self):
        """Delete Minio and InfluxDB buckets."""
        # List all objects in Minio bucket
        objects = await self.minio_client.list_objects(MINIO_BUCKET)

        # Delete all objects in Minio bucket
        for obj in objects:
            await self.minio_client.remove_object(MINIO_BUCKET, obj.object_name)

        # Delete Minio bucket
        await self.minio_client.remove_bucket(MINIO_BUCKET)

        # Find InfluxDB bucket ID
        bucket = self.influx_client.buckets_api().find_bucket_by_name(INFLUXDB_BUCKET)

        # Delete InfluxDB bucket
        self.influx_client.buckets_api().delete_bucket(bucket=bucket)

    async def write_data(self, data, timestamp):
        """Write data to Minio and InfluxDB."""
        # Write data to Minio
        result = await self.minio_client.put_object(
            MINIO_BUCKET, str(timestamp), io.BytesIO(data), length=len(data)
        )

        # Write object name to InfluxDB
        point = Point(INFLUXDB_MEASUREMENT).field(INFLUXDB_FIELD, result.object_name)
        self.influx_client.write_api(write_options=SYNCHRONOUS).write(
            bucket=INFLUXDB_BUCKET,
            record=point,
            time=timestamp,
            write_precision=WritePrecision.NS,
        )

    async def read_last(self):
        """Retrieve the last object from MinIO based on filenames recorded in InfluxDB."""
        # Read object name from InfluxDB
        query = f'from(bucket: "{INFLUXDB_BUCKET}") \
            |> range(start: -10s) \
            |> filter(fn: (r) => r._measurement == "{INFLUXDB_MEASUREMENT}") \
            |> last()'

        result = self.influx_client.query_api().query(query)
        if not result or not result[0].records:
            raise ValueError("No records found in InfluxDB.")
        object_name = result[0].records[0]["_value"]

        # Get object from Minio
        async with aiohttp.ClientSession() as session:
            response = await self.minio_client.get_object(
                MINIO_BUCKET,
                object_name,
                session=session,
            )
            return await response.read()

    async def read_batch(self, start):
        """Retrieve objects from MinIO based on filenames recorded in InfluxDB."""
        # Read object names from InfluxDB
        query = f'from(bucket: "{INFLUXDB_BUCKET}") \
            |> range(start: {to_rfc3339(start)}) \
            |> filter(fn: (r) => r._measurement == "{INFLUXDB_MEASUREMENT}")'
        result = self.influx_client.query_api().query(query)

        object_names = [record["_value"] for record in result[0].records]

        # Get objects from Minio
        objects = []
        async with aiohttp.ClientSession() as session:
            for object_name in object_names:
                response = await self.minio_client.get_object(
                    MINIO_BUCKET,
                    object_name,
                    session=session,
                )
                objects.append(await response.read())

        return objects
