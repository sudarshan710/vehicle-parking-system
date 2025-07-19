import pytest
from unittest.mock import patch
import parking_events.cli_tool as cli_tool  


@pytest.fixture(autouse=True)
def reset_counter():
    cli_tool.operation_counter = 0
    yield
    cli_tool.operation_counter = 0


@patch('parking_events.cli_tool.extract')
@patch('parking_events.cli_tool.transform')
@patch('parking_events.cli_tool.clean')
@patch('parking_events.cli_tool.validate')
@patch('parking_events.cli_tool.write')
@patch('parking_events.cli_tool.write_operation_counter')
def test_all_operation(mock_write_counter, mock_write, mock_validate, mock_clean, mock_transform, mock_extract, capsys):
    mock_extract.return_value = 'extracted.parquet'
    mock_transform.return_value = 'transformed.parquet'
    mock_clean.return_value = 'cleaned.parquet'

    cli_tool._main('all', 'dummy_path')

    captured = capsys.readouterr()
    assert "op:" in captured.out
    mock_extract.assert_called_once()
    mock_transform.assert_called_once()
    mock_clean.assert_called_once()
    mock_validate.assert_called_once()
    mock_write.assert_called_once()
    mock_write_counter.assert_called_with(4)
    assert cli_tool.operation_counter == 4


@patch('cli_tool.extract')
@patch('cli_tool.write_operation_counter')
def test_ingest_operation(mock_write_counter, mock_extract, capsys):
    mock_extract.return_value = 'extracted.parquet'

    cli_tool._main('ingest', 'dummy_path')

    captured = capsys.readouterr()
    mock_extract.assert_called_once()
    mock_write_counter.assert_called_with(1)
    assert cli_tool.operation_counter == 1


@patch('cli_tool.transform')
@patch('cli_tool.write_operation_counter')
def test_clean_allowed(mock_write_counter, mock_transform, capsys):
    cli_tool.operation_counter = 1 

    cli_tool._main('clean', 'dummy_path')

    captured = capsys.readouterr()
    mock_transform.assert_called_once()
    mock_write_counter.assert_called_with(2)
    assert cli_tool.operation_counter == 2


def test_clean_not_allowed(capsys):
    cli_tool.operation_counter = 0 

    cli_tool._main('clean', 'dummy_path')

    captured = capsys.readouterr()
    assert "Operation 'clean' not allowed at operation_counter=0" in captured.out


@patch('cli_tool.validate')
@patch('cli_tool.write_operation_counter')
def test_validate_allowed(mock_write_counter, mock_validate, capsys):
    cli_tool.operation_counter = 1

    cli_tool._main('validate', 'dummy_path')

    captured = capsys.readouterr()
    mock_validate.assert_called_once()
    mock_write_counter.assert_called_with(3)
    assert cli_tool.operation_counter == 3


def test_validate_not_allowed(capsys):
    cli_tool.operation_counter = 0

    cli_tool._main('validate', 'dummy_path')

    captured = capsys.readouterr()
    assert "Operation 'validate' not allowed at operation_counter=0" in captured.out


def test_validate_not_allowed(capsys):
    cli_tool.operation_counter = 1

    cli_tool._main('validate', 'dummy_path')

    captured = capsys.readouterr()
    assert "Operation 'validate' not allowed at operation_counter=0" in captured.out

@patch('cli_tool.write')
@patch('cli_tool.write_operation_counter')
def test_write_allowed(mock_write_counter, mock_write, capsys):
    cli_tool.operation_counter = 2  

    cli_tool._main('write', 'dummy_path')

    captured = capsys.readouterr()
    mock_write.assert_called_once()
    mock_write_counter.assert_called_with(4)
    assert cli_tool.operation_counter == 4


def test_write_not_allowed(capsys):
    cli_tool.operation_counter = 1  

    cli_tool._main('write', 'dummy_path')

    captured = capsys.readouterr()
    assert "Operation 'write' not allowed at operation_counter=1" in captured.out

