# YDB pgbench-like Workload Tool

A tool for running pgbench-like workloads against YDB databases.

## Installation

```bash
# Activate virtual environment
source .venv/bin/activate

# Install dependencies (if not already installed)
pip install -r requirements.txt
```

## Configuration

The tool supports configuration via environment variables with CLI option overrides.

### Environment Variables

- `YDB_ENDPOINT` - YDB endpoint (e.g., `grpcs://ydb-host:2135`)
- `YDB_DATABASE` - Database path (e.g., `/Root/database`)
- `YDB_ROOT_CERT` - Path to root certificate file (optional)
- `YDB_USER` - Username for authentication (optional)
- `YDB_PASSWORD` - Password for authentication (optional)

### Example Environment Setup

```bash
export YDB_ENDPOINT="grpcs://ydb-static-node-2.ydb-cluster.com:2135"
export YDB_DATABASE="/Root/database"
export YDB_ROOT_CERT="./ca.crt"
export YDB_USER="root"
export YDB_PASSWORD="your_password"
```

## Usage

### Initialize Database

Create tables and populate with test data:

```bash
# Using environment variables
python executor.py init --scale 100

# Using CLI options (overrides environment variables)
python executor.py init \
  --endpoint "grpcs://ydb-host:2135" \
  --database "/Root/database" \
  --cert-file "./ca.crt" \
  --user "root" \
  --password "your_password" \
  --scale 100

# With multiple processes
python executor.py init --scale 100 --processes 4
```

**Options:**
- `--scale` - Number of branches to create (default: 100)
- `--processes` - Number of parallel processes (default: 1)

### Run Workload

Execute pgbench-like transactions:

```bash
# Using environment variables
python executor.py run --workers 100 --transactions 1000

# Using CLI options
python executor.py run \
  --endpoint "grpcs://ydb-host:2135" \
  --database "/Root/database" \
  --cert-file "./ca.crt" \
  --user "root" \
  --password "your_password" \
  --workers 100 \
  --transactions 1000

# With multiple processes
python executor.py run --workers 25 --transactions 1000 --processes 4
```

**Options:**
- `--workers` - Number of async workers per process (default: 7)
- `--transactions` - Number of transactions per worker (default: 100)
- `--processes` - Number of parallel processes (default: 1)

## Architecture

The application is organized into separate modules:

- **`executor.py`** - Main entry point
- **`cli.py`** - Click CLI implementation with commands and configuration management
- **`runner.py`** - YDB connection management and workload orchestration
- **`initializer.py`** - Database table creation and data population
- **`worker.py`** - Transaction execution logic

## Multiprocessing

When `--processes` is set to 1 (default), the tool runs in single-process mode using async/await for concurrency.

When `--processes` is greater than 1, the tool uses Python's multiprocessing to run multiple processes in parallel, each with its own set of async workers.

## Examples

```bash
# Initialize with 50 branches
python executor.py init --scale 50

# Run workload with 10 workers, 500 transactions each
python executor.py run --workers 10 --transactions 500

# Run workload with 4 processes, 25 workers per process
python executor.py run --workers 25 --transactions 1000 --processes 4