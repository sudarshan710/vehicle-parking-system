import argparse
import sqlite3
import os
from pathlib import Path    
import pandas as pd
import logging
from parking_events.sql_trigger import setup_database
from parking_events.events_table import create_events_table
from shared_config.config import FLAG_PATH
from shared_config import config


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

def transform(parquet_path):
    df = pd.read_parquet(parquet_path)
    df['entry_time'] = pd.to_datetime(df['entry_time'])
    df['exit_time'] = pd.to_datetime(df['exit_time'])   
    df['exit-entry'] = (df['exit_time']-df['entry_time']).dt.total_seconds() / 60
    for _, row in df.iterrows():
        if row['exit-entry'] < 0:
            logging.warning(f"INVALID EXIT!! {row.to_dict()}")
    print('transform')
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def clean(parquet_path):
    print('clean')
    return parquet_path

def validate(parquet_path):
    df = pd.read_parquet(parquet_path)
    df['flagged'] = 0
    my_db_conn = sqlite3.connect('my_db.db')
    cursor = my_db_conn.cursor()
    for index, row in df.iterrows():
        if row['paid_amount'] < 0:
            df.loc[index, 'flagged'] = 1
            logging.warning(f"PAID AMOUNT NEGATIVE. RECORD FLAGGED. {df.loc[index].to_dict()}")
            cursor.execute('''
                           INSERT INTO invalid_events (vehicle_id, event_id, reason)
                           VALUES (?, ?, ?)
                           ''', (row['vehicle_id'], row['event_id'], f"Negative paid ammount: {row['paid_amount']}")
                           )
        if row['exit-entry'] < 0:
            df.loc[index, 'flagged'] = 1
            logging.warning(f"INVALID EXIT-ENTRY TIME. RECORD FLAGGED. {df.loc[index].to_dict()}")
            cursor.execute('''
                           INSERT INTO invalid_events (vehicle_id, event_id, reason)
                           VALUES (?, ?, ?)
                           ''', (row['vehicle_id'], row['event_id'], f"Invalid EXIT-ENTRY Timestamps: {row['exit-entry']} after entry time")
                           )

    my_db_conn.commit()
    my_db_conn.close()
    # print('validate')
    return parquet_path

def write(parquet_path, vehicles_path, zones_path):

    def csv_to_sqlite_pandas(csv_file, sqlite_db, table_name):
        df = pd.read_csv(csv_file)
        conn = sqlite3.connect(sqlite_db)
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f"Imported {csv_file} into {sqlite_db} (table: {table_name})")

    csv_to_sqlite_pandas(vehicles_path, 'my_db.db', 'vehicles')
    csv_to_sqlite_pandas(zones_path, 'my_db.db', 'parking_zones')

    df = pd.read_parquet(parquet_path)

    if 'exit-entry' in df.columns:
            df.rename(columns={'exit-entry': 'exit_entry'}, inplace=True)

    df['vehicle_id'] = df['vehicle_id'].astype(str)
    df['zone_id'] = df['zone_id'].astype(str)
    df['entry_time'] = pd.to_datetime(df['entry_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['exit_time'] = pd.to_datetime(df['exit_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['paid_amount'] = df['paid_amount'].fillna(0).astype(int)
    df['exit_entry'] = df['exit_entry'].fillna(0).astype(int)


    create_events_table()

    conn = sqlite3.connect('my_db.db')
    cursor = conn.cursor()
    print(df.head())


    cursor.executemany('''
        INSERT INTO events_table(event_id, vehicle_id, zone_id, entry_time, exit_time, paid_amount, exit_entry)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', df[['event_id', 'vehicle_id', 'zone_id', 'entry_time', 'exit_time', 'paid_amount', 'exit_entry']].values.tolist())

    print('here')
    cursor.execute("PRAGMA table_info(vehicles)")
    print(cursor.fetchall())

    cursor.execute("ALTER TABLE events_table ADD COLUMN zone_name TEXT;")
    cursor.execute("ALTER TABLE events_table ADD COLUMN rate_per_hour REAL;")
    cursor.execute("ALTER TABLE events_table ADD COLUMN is_valet INTEGER;")  
    cursor.execute("ALTER TABLE events_table ADD COLUMN plate_number TEXT;")
    cursor.execute("ALTER TABLE events_table ADD COLUMN owner_name TEXT;")
    cursor.execute("ALTER TABLE events_table ADD COLUMN type TEXT;")
    cursor.execute("ALTER TABLE events_table ADD COLUMN reason TEXT DEFAULT 'valid';")

    update_sql = '''
    UPDATE events_table
    SET 
        zone_name = (SELECT pz.zone_name FROM parking_zones pz WHERE pz.zone_id = events_table.zone_id),
        rate_per_hour = (SELECT pz.rate_per_hour FROM parking_zones pz WHERE pz.zone_id = events_table.zone_id),
        is_valet = (SELECT pz.is_valet FROM parking_zones pz WHERE pz.zone_id = events_table.zone_id),
        plate_number = (SELECT v.plate_number FROM vehicles v WHERE v.vehicle_id = events_table.vehicle_id),
        owner_name = (SELECT v.owner_name FROM vehicles v WHERE v.vehicle_id = events_table.vehicle_id),
        type = (SELECT v.type FROM vehicles v WHERE v.vehicle_id = events_table.vehicle_id),
        reason = COALESCE((SELECT ev.reason FROM invalid_events ev WHERE ev.event_id = events_table.event_id),
        reason)
    ''' 

    cursor.execute(update_sql)
    conn.commit()
    df_final = pd.read_sql_query('SELECT * FROM events_table', conn)
    conn.close()
    os.makedirs('data', exist_ok=True)
    df_final.to_parquet('final_joined_events.parquet', index=False)

    with open(FLAG_PATH, 'w') as f:
        f.write("ready")


    print("events_table_ created in DB and written to 'data/final_joined_events.parquet'")

def _main(args_op, args_path, args_vehicles, args_zones):
    global operation_counter
    print("\n")
    print("op:", operation_counter)

    if args_op == 'all':
        parquet_path = extract(args_path)
        parquet_path = transform(parquet_path)
        parquet_path = clean(parquet_path)
        validate(parquet_path)
        write(args_path, args_vehicles, args_zones)
        operation_counter = 4  
        write_operation_counter(operation_counter)

    elif args_op == 'ingest':
        parquet_path = extract(args_path)
        # transform(parquet_path)
        operation_counter = 1
        write_operation_counter(operation_counter)

    elif args_op == 'clean' and operation_counter >= 1:
        transform(args_path)
        operation_counter = 2
        write_operation_counter(operation_counter)

    elif args_op == 'validate' and operation_counter >= 1:
        validate(args_path)                                                            
        operation_counter = 3
        write_operation_counter(operation_counter)

    elif args_op == 'write' and operation_counter > 1 and operation_counter < 4:
        write(args_path, args_vehicles, args_zones)
        operation_counter = 4
        write_operation_counter(operation_counter)

    else:
        print(f"Operation '{args_op}' not allowed at operation_counter={operation_counter}")

    print("op:", operation_counter)
    print("\n")




def main():    
    logging.basicConfig(
        # filename='logs.log',  [ only logs to file ]   ----> log to console for checking
        level=logging.INFO,
        # filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('logs.log', mode='a'),
            logging.StreamHandler()
        ]
    )

    config.reset_config_contents()

    with open(FLAG_PATH, "w") as f:
        f.write("not-ready")

    setup_database()

    parser = argparse.ArgumentParser()
    parser.add_argument("--operation", choices=['ingest', 'clean', 'validate', 'write', 'all'], default='all', help='ETL operation to execute. Choices are ingest, clean, validate, write or all')
    parser.add_argument("--events", type=str, required=True, help='Path to parking_events.csv file') 
    parser.add_argument("--vehicles", type=str, required=True, help="Path to vehicles.csv file")
    parser.add_argument("--zones", type=str, required=True, help="Path to parking_zones.csv file")

    args = parser.parse_args()
    print(args)
    _main(args.operation, args.events, args.vehicles, args.zones)

if __name__ == "__main__":
    main()