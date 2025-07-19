import sqlite3

conn = sqlite3.connect('my_db.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS events_table_2 (
    event_id TEXT PRIMARY KEY,
    vehicle_id TEXT,
    zone_id TEXT,
    entry_time TIMESTAMP,
    exit_time TIMESTAMP,
    paid_amount REAL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS invalid_events (
    vehicle_id TEXT,
    event_id TEXT,
    reason TEXT
)
''')

cursor.execute('''
CREATE TRIGGER IF NOT EXISTS trg_validate_paid_amount
AFTER INSERT ON events_table_2
WHEN NEW.paid_amount < 0
BEGIN
    INSERT INTO invalid_events (vehicle_id, event_id, reason)
    VALUES (NEW.vehicle_id, NEW.event_id, 'Negative paid amount');
END;
''')

cursor.execute('''
CREATE TRIGGER IF NOT EXISTS trg_validate_exit_entry
AFTER INSERT ON events_table_2
WHEN (strftime('%s', NEW.exit_time) - strftime('%s', NEW.entry_time)) < 0
BEGIN
    INSERT INTO invalid_events (vehicle_id, event_id, reason)
    VALUES (NEW.vehicle_id, NEW.event_id, 'Exit time before entry time');
END;
''')

conn.commit()
conn.close()