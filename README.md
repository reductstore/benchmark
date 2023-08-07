# benchmark
The Benchmark project provides a script to test the performance of different storage systems. 
It measures the time taken to write and read data blobs of varying sizes. 

The results are then saved in a directory for later analysis.

## Features
- Multiple Systems: This script can benchmark:
  - Systeme 1: ReductStore
  - System 2: MinIO + InfluxDB

- Detailed Benchmarking: Measures the performance of writing and reading individual blobs, as well as batch reading.

- Results Saving: Saves results in CSV format in a specified directory.

- Verbose Logging: Gives detailed operational output during each step of the benchmarking process if the verbose mode is enabled.

## Installation

Clone this repository:
```bash
git clone https://github.com/reductstore/benchmark.git
```

Navigate into the cloned repository:
```bash
cd benchmark
```

Install the required packages:
```bash
pip install -r requirements.txt
```

## Usage

To run the benchmarking script:

```bash
python src/benchmark.py --blob_sizes [BLOB_SIZES] --batch_size [BATCH_SIZE] --num_trials [NUM_TRIALS] --warmups [WARMUPS] --verbose --directory [DIRECTORY]
```

--blob_sizes (optional):
- Description: List of blob sizes for benchmarking.
- Default: from 1KB to 1MB in 10 steps.

--batch_size (optional):
- Description: Number of separate blobs to write/read.
- Default: 10.

--num_trials (optional):
- Description: Number of benchmarking trials to execute.
- Default: 10.

--warmups (optional):
- Description: Number of warmup runs before starting the benchmark.
- Default: 1.

--verbose (optional):
- Description: Provides detailed output during the benchmarking runs.
- Default: False.

--directory (optional):
- Description: Specifies the directory to save the results.
- Default: "results".

## Contributing
If you'd like to contribute to the benchmark project, please submit a pull request. Ensure that any new systems added adhere to the structure defined in base_system.py.

## License
This project is licensed under the MIT License.