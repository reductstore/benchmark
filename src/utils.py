import os
import sys
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


def print_benchmark_params(
    blob_sizes: list[int],
    batch_size: int,
    batch_reads: int,
    warmups: int,
):
    required_disk_space = max(blob_sizes) * (batch_size + warmups)
    statvfs = os.statvfs(".")
    disk_space_available = statvfs.f_frsize * statvfs.f_bavail

    blob_sizes_formatted = [format_size_binary(size) for size in blob_sizes]

    print("=" * 50)
    print("Benchmark Parameters".center(50))
    print("=" * 50)

    if len(blob_sizes_formatted) <= 4:
        print(f"Blob sizes: {', '.join(blob_sizes_formatted)}")
    else:
        prefix = ", ".join(blob_sizes_formatted[:3])
        suffix = blob_sizes_formatted[-1]
        print(f"Blob sizes: {prefix}, ..., {suffix}")

    print(f"Batch size: {batch_size}")
    print(f"Batch reads: {batch_reads}")
    print(f"Warmups: {warmups}")
    print(f"Disk space required: {format_size_binary(required_disk_space)}")
    print(f"Available disk space: {format_size_binary(disk_space_available)}")

    if required_disk_space > disk_space_available:
        print("\033[91m\033[43m Not enough local disk space. \033[0m")

    print("=" * 50)


def ask_for_confirmation():
    print("\nWould you like to continue? (y/n)")
    if input().lower() != "y":
        sys.exit(0)


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
    quiet: bool = False,
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

    if not quiet:
        tqdm.write(f"Results saved to {path}")
