# benchmark
The Benchmark project provides a script to test the performance of different storage systems. 
It measures the time taken to write and read data blobs of varying sizes. 

The results are then saved in a directory for later analysis.

## Features
- Multiple Systems: This script can benchmark:
  - System 1: ReductStore
  - System 2: MinIO + InfluxDB
  - System 3: TimescaleDB
  - System 4: MongoDB

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

## Initialization of the databases

To initialize the databases, create copy the .env.example file and rename it to .env.

```bash
cp .env.example .env
```

Then, fill in the required fields in the .env file with the appropriate values.

Then, run the following command:

```bash
docker-compose up -d
```

The databases will be initialized automatically.

## Usage

To run the benchmarking script:

```bash
python src/benchmark.py --start-power [START_POWER] --end-power [END_POWER]--batch-size [BATCH_SIZE] --batch-reads  [NUM_TRIALS] --warmups [WARMUPS]   --quiet  --directory [DIRECTORY]
```

Default values:
```
Blob sizes: 1 KiB, 2 KiB, 4 KiB, ..., 1 MiB
Batch size: 1000
Batch reads: 50
Warmups: 1
```

## Contributing
If you'd like to contribute to the benchmark project, please submit a pull request. Ensure that any new systems added adhere to the structure defined in base_system.py.

## License
This project is licensed under the MIT License.