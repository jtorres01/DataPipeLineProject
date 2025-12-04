# Testing RefactoredDataUpload.py
import os
import builtins
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, mock_open
from io import StringIO

import RefactoredDataUpload as etl
#from RefactoredDataUpload import load_file

# Tests the file is properly loaded into the dataframe
def test_load_file(tmp_path):
    file = tmp_path / "data.csv"
    file.write_text("OrderID,OrderDate\n1,01/01/2024")

    df = etl.load_file(str(file))
    assert not df.empty
    assert list(df.columns) == ["OrderID", "OrderDate"]

# Tests that the data is transformed properly by checking for duplicates
def test_clean_data():
    df = pd.DataFrame({
        " OrderDate ": ["01/01/2024", "02/01/2024"],
        "OrderID": [1, 1]  # duplicate row
    })

    cleaned = etl.clean_data(df)
    assert "OrderDate" in cleaned.columns
    assert len(cleaned) == 1
    assert cleaned["OrderDate"].iloc[0].year == 2024


# Ensuring data is ready for insertion
# Checking that all required colunms are present and are not empty
def test_is_valid_row_valid():
    row = pd.Series({
        col: "x" for col in etl.REQUIRED_COLUMNS
    })
    assert etl.is_valid_row(row) is True

# Checking to make sure invalid rows are detected
def test_is_valid_row_missing():
    row = pd.Series({
        col: "" for col in etl.REQUIRED_COLUMNS
    })
    assert etl.is_valid_row(row) is False

# Testing a database connection is successfully loaded
@patch("RefactoredDataUpload.psycopg2.connect")
def test_get_db_connection(mock_connect):
    etl.get_db_connection()
    mock_connect.assert_called_once()

# Testing a row is inserted into the DB
def test_insert_row_inserted():
    cursor = MagicMock()
    cursor.rowcount = 1

    row = pd.Series({
        "OrderID": 10, "OrderDate": "2024-01-01",
        "UnitCost": 1, "Price": 2, "OrderQty": 1,
        "CostOfSales": 1, "Sales": 1, "Profit": 1,
        "ProductName": "A", "Manufacturer": "B", "Country": "USA"
    })

    log = StringIO()

    result = etl.insert_row(cursor, row, log)
    assert result == "inserted"

# Test case if a depulicate row was inserted 
def test_insert_row_duplicate():
    cursor = MagicMock()
    cursor.rowcount = 0

    row = pd.Series({
        "OrderID": 10, "OrderDate": "2024-01-01",
        "UnitCost": 1, "Price": 2, "OrderQty": 1,
        "CostOfSales": 1, "Sales": 1, "Profit": 1,
        "ProductName": "A", "Manufacturer": "B", "Country": "USA"
    })

    log = StringIO()

    result = etl.insert_row(cursor, row, log)
    assert result == "duplicate"

# Testing if there are missing values in the column that are trying to be inserted
# a error should occur
def test_insert_row_error():
    cursor = MagicMock()
    cursor.execute.side_effect = Exception("bad insert")

    row = pd.Series({"OrderID": 10})
    log = StringIO()

    result = etl.insert_row(cursor, row, log)
    assert result == "error"

# Testing deletion of oldest log files, maintaining 5 logs at a time
def test_cleanup_old_logs(tmp_path, monkeypatch):
    for i in range(10):
        f = tmp_path / f"insert_log_{i}.txt"
        f.write_text("log")

    monkeypatch.chdir(tmp_path)

    etl.cleanup_old_logs(max_logs=5)

    remaining = list(tmp_path.glob("insert_log_*.txt"))
    assert len(remaining) == 5

@patch("RefactoredDataUpload.plt.show")
def test_plot_profit(mock_show):
    df = pd.DataFrame({
        "Manufacturer": ["A", "B"],
        "Profit": [10, 20]
    })

    etl.plot_profit_By_UserInput(df, "Manufacturer")
    mock_show.assert_called_once()

