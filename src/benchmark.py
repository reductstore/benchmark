#!/usr/bin/env python3
import os
import sys
import time
import asyncio
import argparse

from tqdm import tqdm

from systems.base_system import BaseSystem
from systems.system_one import SystemOne
from systems.system_two import SystemTwo
from utils import (
    generate_blob,
    format_size_binary,
    save_results,
    print_benchmark_params,
    ask_for_confirmation,
)


async def write_data(system: BaseSystem, blob: bytes, timestamp: int) -> float:
    """Write data to the system.

    Parameters:
    -----------
    system: BaseSystem
        The system to write to.
    blob: bytes
        The data to write.
    timestamp: int
        The timestamp of the data in nanoseconds.

    Returns:
    --------
    float
        The time taken to write the data in seconds.
    """
    start_time = time.time()
    await system.write_data(blob, timestamp)
    end_time = time.time()
    return end_time - start_time


async def read_last(system: BaseSystem) -> (bytes, float):
    """Read the last data from the system.

    Parameters:
    -----------
    system: BaseSystem
        The system to read from.

    Returns:
    --------
    (bytes, float)
        The data read and the time taken to read it in seconds.
    """
    start_time = time.time()
    result = await system.read_last()
    end_time = time.time()
    return result, end_time - start_time


async def read_batch(system: BaseSystem, start: int) -> (list[bytes], float):
    """Read a batch of data from the system.

    Parameters:
    -----------
    system: BaseSystem
        The system to read from.
    start: int
        The timestamp to start reading from.

    Returns:
    --------
    (list[bytes], float)
        The data read and the time taken to read it in seconds.
    """
    start_time = time.time()
    result = await system.read_batch(start)
    end_time = time.time()
    return result, end_time - start_time


async def benchmark_system(
    system: BaseSystem,
    blob_size: int,
    batch_size: int,
    batch_reads: int,
    warmups: int,
    directory: str = "results",
    quiet: bool = False,
):
    """
    Benchmark data writing and reading operations on a given system.

    This function measures the performance of a system by writing
    and reading blobs of specified size and number.
    It includes "warm-up" runs to stabilize initial measurements.

    Parameters:
    -----------
    - system : object
        The target system or storage platform being benchmarked.

    - blob_size : int
        Size (in bytes) of individual data blobs that will be written
        to and read from the system.

    - batch_size : int
        Total number of blobs written to the system. Each blob is written
        with a unique timestamp.

    - batch_reads : int
        Total number of times the batch is read from the system.

    - warmups : int
        Number of initial runs executed prior to the benchmark measurements.
        These runs serve as a "warm-up" phase and are not included in the
        final performance metrics.

    - directory : str, optional, default "results"
        The directory where the benchmark results will be saved.

    - quiet : bool, optional, default False
        If set to True, the benchmark will not print any progress information.

    Side Effects:
    -------------
    - Saves the benchmark results in two CSV files:
        1. A file named "{system_name}_read_write.csv" capturing individual write and read times.
        2. A file named "{system_name}_batch_read.csv" capturing batch read times.

    Where {system_name} is the name of the benchmarked storage platform.

    """
    if not quiet:
        tqdm.write(f"{'=' * 50}")
        tqdm.write(f"Benchmarking {type(system).__name__}...")
        tqdm.write(f"Blob size: {format_size_binary(blob_size)}")

    write_times = []
    read_times = []
    timestamps = []
    batch_read_times = []

    try:
        # Warmup
        if not quiet:
            tqdm.write(
                f"Warming up with {warmups} {'run' if warmups == 1 else 'runs'}..."
            )
        for _ in range(warmups):
            blob = generate_blob(blob_size)
            timestamp = time.time_ns()
            _ = await write_data(system, blob, timestamp)
            result, _ = await read_last(system)
            assert result == blob

        # Write and read one blob at a time
        with tqdm(
            total=batch_size,
            desc=f"Write and read operations",
            leave=False,
            position=2,
            disable=quiet,
        ) as third_bar:
            for _ in range(batch_size):
                blob = generate_blob(blob_size)

                # Benchmark write and read operations
                timestamp = time.time_ns()
                time_write = await write_data(system, blob, timestamp)
                result, time_read = await read_last(system)
                assert blob == result

                # Save the results
                write_times.append(time_write)
                read_times.append(time_read)
                timestamps.append(timestamp)

                third_bar.update()

        # Read all blobs in a batch
        with tqdm(
            total=batch_reads,
            desc=f"Batch reads",
            leave=False,
            position=3,
            disable=quiet,
        ) as fourth_bar:
            for _ in range(batch_reads):
                result, time_read = await read_batch(system, timestamps[0])
                assert len(result) == batch_size
                batch_read_times.append(time_read)

                fourth_bar.update()

        # Calculate average times
        if not quiet:
            avg_write_time = sum(write_times) / batch_size
            avg_read_time = sum(read_times) / batch_size
            avg_batch_read_time = sum(batch_read_times) / batch_reads

            tqdm.write(f"Average write time: {avg_write_time*1000:.2f} ms")
            tqdm.write(f"Average read time: {avg_read_time*1000:.2f} ms")
            tqdm.write(f"Average batch read time: {avg_batch_read_time*1000:.2f} ms")

        # Save the results
        save_results(
            filename=f"{type(system).__name__}_read_write.csv",
            directory=directory,
            blob_size=blob_size,
            batch_size=1,
            write_times=write_times,
            read_times=read_times,
            quiet=quiet,
        )
        save_results(
            filename=f"{type(system).__name__}_batch_read.csv",
            directory=directory,
            blob_size=blob_size,
            batch_size=batch_size,
            read_times=batch_read_times,
            quiet=quiet,
        )

    finally:
        # Clean up the system even if an exception occurs
        if not quiet:
            tqdm.write("Cleaning up...")
        await system.cleanup()
        if not quiet:
            tqdm.write("System cleaned up!")


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run the benchmarking script.")
    parser.add_argument(
        "--start_power", type=int, default=10, help="Start power of 2 for blob sizes."
    )
    parser.add_argument(
        "--end_power", type=int, default=20, help="End power of 2 for blob sizes."
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1_000,
        help="Number of separate blobs to write/read.",
    )
    parser.add_argument(
        "--batch_reads", type=int, default=50, help="Number of trials to run."
    )
    parser.add_argument("--warmups", type=int, default=1, help="Number of warmup runs.")
    parser.add_argument(
        "--quiet",
        action="store_false",
        default=False,
        help="Print detailed output during runs.",
    )
    parser.add_argument(
        "--directory",
        type=str,
        default="results",
        help="Directory where the results will be saved.",
    )
    return parser.parse_args()


async def main(
    start_power: int,
    end_power: int,
    batch_size: int,
    batch_reads: int,
    warmups: int,
    directory: str,
    quiet: bool,
):
    """
    Benchmark main execution function.

    Parameters:
    -----------
    - start_power : int, default=10
        Start power of 2 for blob sizes.

    - end_power : int, default=20
        End power of 2 for blob sizes.

    - batch_size : int, default=1_000
        Total number of blobs written to the system. Each blob is written
        with a unique timestamp.

    - batch_reads : int, default=50
        Total number of times the batch is read from the system.

    - warmups : int
        Number of initial runs executed prior to the benchmark measurements.
        These runs serve as a "warm-up" phase and are not included in the
        final performance metrics.

    - directory : str, optional
        Directory where the results will be saved. Default is "results".

    - quiet : bool, optional
        If set to True, the benchmark will not print detailed output during runs.

    Units of Measurement:
    ---------------------
        - Kibibyte (KiB): 2^10 bytes
        - Mebibyte (MiB): 2^20 bytes
        - Gibibyte (GiB): 2^30 bytes
        - Tebibyte (TiB): 2^40 bytes
    """
    # Calculate the total disk space required for the benchmark
    blob_sizes = [2**i for i in range(args.start_power, args.end_power + 1)]

    # Confirm the benchmark parameters
    print_benchmark_params(
        blob_sizes,
        batch_size,
        batch_reads,
        warmups,
    )
    ask_for_confirmation()

    # Run the benchmark
    with tqdm(
        total=len(blob_sizes),
        desc="Blob sizes",
        leave=False,
        position=0,
        disable=quiet,
    ) as first_pbar:
        for blob_size in blob_sizes:
            system_one = await SystemOne.create()
            system_two = await SystemTwo.create()
            systems = [system_one, system_two]
            with tqdm(
                total=len(systems),
                desc=f"Systems",
                leave=False,
                position=1,
                disable=quiet,
            ) as second_pbar:
                for system in systems:
                    await benchmark_system(
                        system,
                        blob_size,
                        batch_size,
                        batch_reads,
                        warmups,
                        directory,
                        quiet,
                    )
                    second_pbar.update()
            first_pbar.update()


if __name__ == "__main__":
    args = parse_arguments()
    asyncio.run(
        main(
            args.start_power,
            args.end_power,
            args.batch_size,
            args.batch_reads,
            args.warmups,
            args.directory,
            args.quiet,
        )
    )
