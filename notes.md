-- vehicles and parking_zones are STATIC/EXISTING DATA to be used.

-- parking_events are DYNAMIC, to be pipelined.

-- my new non-registered vehicles parking event occurs **TRIGGER** (maybe FLAG them as unregistered).

-- add CONSTRAINTS for negative, null values after dry-run.

-- CSV vs Parquet based on USAGE. For registered vehicles, parking zones just use CSV. While for larger events dataset use Parquet converted file.

-- logs triggered and appended on filtered conditions.

-- For now, empty entries/exists or negatives amounts are ONYL FLAGGED but RETAINED in the FINAL DATABASE.

-- **While writing to final database, make sure to set proper datatypes for all columns from parquet -> sql table.**
