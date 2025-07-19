import sqlite3
import pandas as pd
from pathlib import Path
import pytest
from unittest.mock import patch

from parking_events.cli_tool import (
    read_operation_counter,
    write_operation_counter,
    extract,
    transform,
    clean,
    validate,
)


def test_write_and_read_operation_counter(tmp_path):
    counter_file = tmp_path / "counter.txt"

    import parking_events.cli_tool as cli_tool
    original_file = cli_tool.COUNTER_FILE
    cli_tool.COUNTER_FILE = str(counter_file)

    write_operation_counter(3)
    assert read_operation_counter() == 3

    cli_tool.COUNTER_FILE = original_file


def test_extract(tmp_path):
    csv_path = tmp_path / "data.csv"
    df = pd.DataFrame({
        "vehicle_id": [1, 2],
        "entry_time": ["2024-01-01 08:00:00", "2024-01-01 09:00:00"],
        "exit_time": ["2024-01-01 10:00:00", "2024-01-01 11:00:00"]
    })
    df.to_csv(csv_path, index=False)

    parquet_path = extract(csv_path)
    assert parquet_path.exists()
    df2 = pd.read_parquet(parquet_path)
    assert df2.equals(df)


def test_transform(tmp_path, caplog):
    df = pd.DataFrame({
        "vehicle_id": [1],
        "entry_time": ["2024-01-01 10:00:00"],
        "exit_time": ["2024-01-01 09:00:00"], 
        "paid_amount": [10]
    })
    path = tmp_path / "data.parquet"
    df.to_parquet(path, index=False)

    with caplog.at_level("WARNING"):
        result_path = transform(path)

    df2 = pd.read_parquet(result_path)
    assert "exit-entry" in df2.columns
    assert df2["exit-entry"].iloc[0] < 0
    assert "INVALID EXIT!!" in caplog.text


def test_clean_returns_same_path(tmp_path):
    parquet_path = tmp_path / "dummy.parquet"
    parquet_path.touch()
    result = clean(parquet_path)
    assert result == parquet_path


def test_validate_with_flags(tmp_path):
    df = pd.DataFrame({
        "event_id": [1, 2],
        "vehicle_id": ["X", "Y"],
        "entry_time": ["2024-01-01 09:00:00", "2024-01-01 10:00:00"],
        "exit_time": ["2024-01-01 08:00:00", "2024-01-01 11:00:00"],  
        "exit-entry": [-60, 60],
        "paid_amount": [-5, 20] 
    })
    parquet_path = tmp_path / "data.parquet"
    df.to_parquet(parquet_path, index=False)

    db_path = tmp_path / "my_db.db"
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE invalid_events (
            vehicle_id TEXT,
            event_id INTEGER,
            reason TEXT
        )
    """)
    conn.commit()
    conn.close()

    import parking_events.cli_tool as cli_tool
    original_connect = sqlite3.connect
    sqlite3.connect = lambda _: original_connect(str(db_path))

    try:
        validate(parquet_path)

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM invalid_events")
        results = cursor.fetchall()
        conn.close()

        assert len(results) == 2
        reasons = [r[2] for r in results]
        assert any("Negative paid" in r for r in reasons)
        assert any("EXIT-ENTRY" in r for r in reasons)

    finally:
        sqlite3.connect = original_connect 