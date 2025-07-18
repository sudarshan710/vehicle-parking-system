import sqlite3

conn = sqlite3.connect('my_db.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS events_table (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id TEXT NOT NULL,
    zone_id TEXT NOT NULL,
    entry_time TIMESTAMP,
    exit_time TIMESTAMP,
    paid_amount INTEGER,
    plate_number TEXT,
    owner_name TEXT,
    type TEXT,
    rate_per_hour INTEGER,
    is_valet INTEGER
)
''')

conn.commit()
conn.close()