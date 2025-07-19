# Vehicle Parking System

A Python CLI tool for managing and analyzing vehicle parking data. It uses Parquet files and an SQLite database to track events and compare parking zone performance. Packaged for easy installation and usage using Poetry.

## Features
- Process and analyze parking event data
- CLI tools for running ETL and analysis pipelines
- Manage configuration and logs with a shared config directory
- Generate reports on parking zones and revenue
- Modular design with reusable components

## Installation

Use Poetry for dependency management and environment setup.

```
git clone https://github.com/yourusername/vehicle_parking_system.git
cd vehicle_parking_system
poetry install
```

Or, alternatively, install with pip from the distribution wheel:

```
pip install dist/vehicle_parking_system-0.1.0-py3-none-any.whl
```

## Dependencies & Sample Data

```` 
- Python 3.10+
- pandas
- duckdb
- sqlite3
- poetry (for package management)
````

Sample Data is avaialbe in `data` directory.


## Usage

### CLI1: Data Processing and Setup

Run the first CLI command to prepare your environment, process raw data, run queries and set-up database and tables. Choices of operations include `ingest`, `clean`, `validate` and finally `write` to database.

```
cli1 -h

# Example usage:

cli1 --operation ingest --events path/to/parking_events.csv --vehicles path/to/vehicles.csv --zones path/to/parking_zones.csv    [ ingestion of raw data -> conversion to parquet ]

# all operations after 'ingest' must be run on parquet files for faster query operations
cli1 --operation clean --events path/to/parking_events.parquet  --vehicles path/to/vehicles.csv --zones path/to/parking_zones.csv    [ ingestion of raw data -> conversion to parquet ]

```

### CLI2: Analysis and Reporting

After successfully running CLI1, use CLI2 to run analyses. This CLI can ONLY be RUN after writing to database through CLI1. Choose one or more analysis types from:

- `top-zone` — Top performing parking zones 
- `freq-parkers` — Frequent parkers analysis
- `comp-zone` — Comparative Zone Performance (total paid amount vs number of parking events)

```
#Example usage

cli2 comp-zone
```

## Logging & Counters & Tests

Logs are saved to `logs.log`. Operation Counter must be secured in production environments. CLI1 run status is managed by a shared config file.

Tests are implemented thorugh Pytests and are available in `tests` directory.

## Author

Sudarshan Zunja 

[github.com/sudarshan710](https://github.com/sudarshan710)  

## Contact

For questions or support, please open an issue or email me at:  
sudarshanhope710@gmail.com




