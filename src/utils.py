import os
import datetime
from itertools import zip_longest

from tqdm import tqdm


def generate_blob(size: int) -> bytes:
    """Generate a random blob of the given size."""
    return os.urandom(size)


def format_size_binary(size: int) -> str:
    """Format the given size in bytes to a human-readable string."""
    if size >= 2**30:
        return f"{size // 2**30} GiB"
    elif size >= 2**20:
        return f"{size // 2**20} MiB"
    elif size >= 2**10:
        return f"{size // 2**10} KiB"
    else:
        return f"{size} B"


def to_rfc3339(ns_timestamp: int) -> str:
    """Convert the given Unix timestamp (in nanoseconds) to the RFC3339 format."""
    dt = datetime.datetime.utcfromtimestamp(ns_timestamp / 1e9)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def save_results(
    filename: str,
    directory: str,
    blob_size: int,
    batch_size: int,
    write_times: list[float] = [],
    read_times: list[float] = [],
    verbose: bool = False,
):
    """Append the given results to the given CSV file."""
    if not os.path.exists(directory):
        os.makedirs(directory)

    path = os.path.join(directory, filename)
    write_header = not os.path.exists(path)

    with open(path, "a") as f:
        if write_header:
            f.write("write_time,read_time,blob_size,batch_size\n")
        for write_time, read_time in zip_longest(write_times, read_times):
            f.write(
                f"{write_time or 'N/A'},{read_time or 'N/A'},{blob_size},{batch_size}\n"
            )

    if verbose:
        tqdm.write(f"Results saved to {path}")
