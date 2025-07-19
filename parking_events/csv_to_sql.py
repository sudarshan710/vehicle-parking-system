import pandas as pd
import sqlite3

def csv_to_sqlite_pandas(csv_file, sqlite_db, table_name):
    df = pd.read_csv(csv_file)
    conn = sqlite3.connect(sqlite_db)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()
    print(f"Imported {csv_file} into {sqlite_db} (table: {table_name})")

csv_to_sqlite_pandas('data/vehicles.csv', 'my_db.db', 'vehicles')
csv_to_sqlite_pandas('data/parking_zones.csv', 'my_db.db', 'parking_zones')