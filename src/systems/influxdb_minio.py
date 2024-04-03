import io

import aiohttp
from config import (
    INFLUXDB_BUCKET,
    INFLUXDB_ENDPOINT,
    INFLUXDB_FIELD,
    INFLUXDB_MEASUREMENT,
    INFLUXDB_ORG,
    INFLUXDB_TOKEN,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_IS_SECURE,
    MINIO_SECRET_KEY,
)
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from miniopy_async import Minio
from systems.base_system import BaseSystem
from utils import to_rfc3339


class InfluxDBMinioSystem(BaseSystem):
    @classmethod
    async def create(cls):
        """Create Minio and InfluxDB buckets."""
        self = cls()

        found = await self.minio_client.bucket_exists(MINIO_BUCKET)
        if not found:
            print("Creating Minio bucket...")
            await self.minio_client.make_bucket(MINIO_BUCKET)

        found = self.influx_client.buckets_api().find_bucket_by_name(INFLUXDB_BUCKET)
        if not found:
            self.influx_client.buckets_api().create_bucket(
                org=INFLUXDB_ORG, bucket_name=INFLUXDB_BUCKET
            )

        return self

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

    async def cleanup(self) -> None:
        """Delete Minio and InfluxDB buckets."""
        # Continuously list and delete objects in Minio bucket until no more objects are found
        while True:
            objects = await self.minio_client.list_objects(MINIO_BUCKET)
            object_list = list(objects)
            if not object_list:
                break
            for obj in object_list:
                try:
                    await self.minio_client.remove_object(MINIO_BUCKET, obj.object_name)
                except Exception as e:
                    print(f"Error removing object {obj.object_name}: {e}")
                    continue

        # Now that all objects are presumably deleted, delete the Minio bucket
        try:
            await self.minio_client.remove_bucket(MINIO_BUCKET)
        except Exception as e:
            print(f"Error removing bucket {MINIO_BUCKET}: {e}")

        # Find InfluxDB bucket ID
        bucket = self.influx_client.buckets_api().find_bucket_by_name(INFLUXDB_BUCKET)

        # Delete InfluxDB bucket
        try:
            self.influx_client.buckets_api().delete_bucket(bucket=bucket)
        except Exception as e:
            print(f"Error removing InfluxDB bucket {INFLUXDB_BUCKET}: {e}")

        # Verify InfluxDB bucket deletion
        if not self.influx_client.buckets_api().find_bucket_by_name(INFLUXDB_BUCKET):
            print(f"InfluxDB bucket {INFLUXDB_BUCKET} deleted successfully!")
        else:
            print(f"Failed to delete InfluxDB bucket {INFLUXDB_BUCKET}.")

    async def write_data(self, data: bytes, timestamp_ns: int) -> None:
        """Write data to Minio and InfluxDB."""
        # Write data to Minio
        result = await self.minio_client.put_object(
            MINIO_BUCKET,
            str(timestamp_ns),
            io.BytesIO(data),
            length=len(data),
        )

        # Write object name to InfluxDB
        point = Point(INFLUXDB_MEASUREMENT).field(INFLUXDB_FIELD, result.object_name)
        self.influx_client.write_api(write_options=SYNCHRONOUS).write(
            bucket=INFLUXDB_BUCKET,
            record=point,
            time=timestamp_ns,
            write_precision=WritePrecision.NS,
        )

    async def read_last(self) -> bytes:
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

    async def read_batch(self, start_ns: int) -> list[bytes]:
        """Retrieve objects from MinIO based on filenames recorded in InfluxDB."""
        # Read object names from InfluxDB
        query = f'from(bucket: "{INFLUXDB_BUCKET}") \
            |> range(start: {to_rfc3339(start_ns)}) \
            |> filter(fn: (r) => r._measurement == "{INFLUXDB_MEASUREMENT}")'
        result = self.influx_client.query_api().query(query)

        object_names = [record["_value"] for record in result[0].records]
        print(f"Found {len(object_names)} objects in InfluxDB.")
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
