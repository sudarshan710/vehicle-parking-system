import sqlite3

def create_events_table(db_path='my_db.db'):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events_table (
        event_id TEXT,
        vehicle_id TEXT NOT NULL,
        zone_id TEXT NOT NULL,
        entry_time TIMESTAMP,
        exit_time TIMESTAMP,
        paid_amount INTEGER,
        exit_entry INTEGER
    )
    ''')

    conn.commit()
    conn.close()
