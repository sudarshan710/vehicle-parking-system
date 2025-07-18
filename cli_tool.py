import argparse
import sqlite3
import os
from pathlib import Path    
import pandas as pd
import logging

my_db_conn = sqlite3.connect('my_db.db')
cursor = my_db_conn.cursor()
COUNTER_FILE = 'operation_counter.txt'

def read_operation_counter():
    if os.path.exists(COUNTER_FILE):
        with open(COUNTER_FILE, 'r') as f:
            return int(f.read().strip())
    return 0

def write_operation_counter(value):
    with open(COUNTER_FILE, 'w') as f:
        f.write(str(value))

operation_counter = read_operation_counter()

def extract(csv_path):
    csv_path = Path(csv_path)
    df = pd.read_csv(csv_path)
    parquet_path = csv_path.with_suffix('.parquet')
    df.to_parquet(parquet_path, index=False)
    
    print('Data extracted from CSV file and conerted to parquet...')
    print(parquet_path)
    return parquet_path

def transform():
    print('transform')
    pass

# def load():
#     print('load')
#     pass

def clean():
    print('clean')
    pass

def validate(parquet_path):
    df = pd.read_parquet(parquet_path)
    for _, row in df.iterrows():
        if row['type'] == 'SUV':
            logging.info(f"Type SUV found: {row.to_dict()}")
    print('validate')
    pass

def write():
    print('write')
    pass

def main(args_op, args_path):
    global operation_counter
    print("\n")
    print("op:", operation_counter)

    if args_op == 'all':
        parquet_path = extract(args_path)
        parquet_path = transform(parquet_path)
        load()
        clean()
        validate(parquet_path)
        write()
        operation_counter = 4  
        write_operation_counter(operation_counter)

    elif args_op == 'ingest':
        extract(args_path)
        transform()
        load()
        operation_counter = 1
        write_operation_counter(operation_counter)

    elif args_op == 'clean' and operation_counter >= 1:
        clean()
        operation_counter = 2
        write_operation_counter(operation_counter)

    elif args_op == 'validate' and operation_counter >= 1:
        validate(args_path)                                         ####   PARQUET vs CSV file path CONFLICT                   
        operation_counter = 3
        write_operation_counter(operation_counter)

    elif args_op == 'write' and operation_counter > 1:
        write()
        operation_counter = 4
        write_operation_counter(operation_counter)

    else:
        print(f"Operation '{args_op}' not allowed at operation_counter={operation_counter}")

    print("op:", operation_counter)
    print("\n")

if __name__ == "__main__":
    
    logging.basicConfig(
        filename='logs.log',
        level=logging.INFO,
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation", choices=['ingest', 'clean', 'validate', 'write', 'all'], default='all', help='ETL operation to execute. Choices are ingest, clean, validate, write or all')
    parser.add_argument("path_to_data", help='Path to you ingestion data') 

    args = parser.parse_args()
    print(args)
    main(args.operation, args.path_to_data)