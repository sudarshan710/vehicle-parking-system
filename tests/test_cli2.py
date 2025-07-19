import pytest
from unittest.mock import patch

from parking_toolkit import analysis_toolkit  


@pytest.fixture
def flag_setup_teardown(tmp_path):
    original_flag = analysis_toolkit.FLAG_PATH
    analysis_toolkit.FLAG_PATH = str(tmp_path / "flag.flag")
    with open(analysis_toolkit.FLAG_PATH, 'w') as f:
        f.write("ready")
    yield
    analysis_toolkit.FLAG_PATH = original_flag

@patch("parking_toolkit.analysis_toolkit.duckdb.query")
def test_single_valid_analysis_arg(mock_query, capsys, flag_setup_teardown):
    mock_query.return_value.df.return_value = "mock_single_df"
    analysis_toolkit.main(["top-zone"])
    captured = capsys.readouterr()
    
    assert "Top 5 Performing Zone" in captured.out
    assert "mock_single_df" in captured.out
    
@patch("parking_toolkit.analysis_toolkit.duckdb.query")
def test_single_valid_analysis_freq_parkers(mock_query, capsys, flag_setup_teardown):
    mock_query.return_value.df.return_value = "mock_single_freq_df"
    analysis_toolkit.main(["freq-parkers"])
    captured = capsys.readouterr()
    
    assert "Top 5 Frequent Parkers" in captured.out
    assert "mock_single_freq_df" in captured.out

@patch("parking_toolkit.analysis_toolkit.duckdb.query")
def test_single_valid_analysis_comp_zone(mock_query, capsys, flag_setup_teardown):
    mock_query.return_value.df.return_value = "mock_single_comp_df"
    analysis_toolkit.main(["comp-zone"])
    captured = capsys.readouterr()
    
    assert "Comparative Zone Performance" in captured.out
    assert "mock_single_comp_df" in captured.out


@patch("parking_toolkit.analysis_toolkit.duckdb.query")
def test_top_performing_zone(mock_query, capsys, flag_setup_teardown):
    mock_query.return_value.df.return_value = "mock_top_zone_df"
    analysis_toolkit.main(["top-zone"])
    captured = capsys.readouterr()
    assert "Top 5 Performing Zone" in captured.out
    assert "mock_top_zone_df" in captured.out


@patch("parking_toolkit.analysis_toolkit.duckdb.query")
def test_most_frequent_parkers(mock_query, capsys, flag_setup_teardown):
    mock_query.return_value.df.return_value = "mock_freq_df"
    analysis_toolkit.main(["freq-parkers"])
    captured = capsys.readouterr()
    assert "Top 5 Frequent Parkers" in captured.out
    assert "mock_freq_df" in captured.out


@patch("parking_toolkit.analysis_toolkit.duckdb.query")
def test_compare_zone_perfor(mock_query, capsys, flag_setup_teardown):
    mock_query.return_value.df.return_value = "mock_comp_df"
    analysis_toolkit.main(["comp-zone"])
    captured = capsys.readouterr()
    assert "Comparative Zone Performance" in captured.out
    assert "mock_comp_df" in captured.out


@patch("parking_toolkit.analysis_toolkit.duckdb.query")
def test_multiple_analyses(mock_query, capsys, flag_setup_teardown):
    mock_query.return_value.df.return_value = "mock_multi_df"
    analysis_toolkit.main(["top-zone", "freq-parkers", "comp-zone"])
    captured = capsys.readouterr()
    assert "Top 5 Performing Zone" in captured.out
    assert "Top 5 Frequent Parkers" in captured.out
    assert "Comparative Zone Performance" in captured.out


def test_flag_file_missing(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(analysis_toolkit, "FLAG_PATH", str(tmp_path / "missing.flag"))
    
    with pytest.raises(SystemExit) as e:
        analysis_toolkit.main(["top-zone"])
    assert e.value.code == 1

    captured = capsys.readouterr()
    assert "must run CLI1" in captured.out


def test_invalid_choice_skipped(capsys, flag_setup_teardown):
    analysis_toolkit.main(["invalid-choice"])
    captured = capsys.readouterr()
    assert "Invalid choice" in captured.out

