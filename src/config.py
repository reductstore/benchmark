import os

from dotenv import load_dotenv

load_dotenv()


def str_to_bool(value: str) -> bool:
    """Convert the given string to a boolean."""
    return value.lower() in ["true", "1", "yes"]


# Minio
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_IS_SECURE = str_to_bool(os.getenv("MINIO_IS_SECURE"))
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# InfluxDB
INFLUXDB_ENDPOINT = os.getenv("INFLUXDB_ENDPOINT")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET")
INFLUXDB_MEASUREMENT = os.getenv("INFLUXDB_MEASUREMENT")
INFLUXDB_FIELD = os.getenv("INFLUXDB_FIELD")

# ReductStore
REDUCTSTORE_ENDPOINT = os.getenv("REDUCTSTORE_ENDPOINT")
REDUCTSTORE_ACCESS_KEY = os.getenv("REDUCTSTORE_ACCESS_KEY")
REDUCTSTORE_BUCKET = os.getenv("REDUCTSTORE_BUCKET")
REDUCTSTORE_ENTRY = os.getenv("REDUCTSTORE_ENTRY")

# TimescaleDB
TIMESCALE_CONNECTION = os.getenv("TIMESCALE_CONNECTION")
TIMESCALE_DATABASE = os.getenv("TIMESCALE_DATABASE")

# MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
