📦 ETL Pipeline for Order History Data

This project implements a full ETL (Extract–Transform–Load) pipeline for loading order history data into a PostgreSQL database, 
performing data validation, and generating visualizations based on user-selected categories.

The pipeline supports both CSV and JSON datasets, handles duplicate prevention, logs processing details, and cleans old logs automatically. 
After the ETL operation is complete, the user may generate a profit aggregation graph by category.

🚀 Features
1. Extract

Loads a dataset from a CSV or JSON file.

Automatically detects file type based on extension.

Uses pandas for efficient data processing.

2. Transform

The clean_data() function:

Strips unexpected spaces from column names.

Converts OrderDate into a proper datetime.date object.

Removes duplicate records based on OrderID.

Prepares data for database insertion.

The is_valid_row() function:

Ensures all required columns contain valid, non-empty data.

3. Load

Connects to PostgreSQL using environment variables stored in a .env file.

Creates (or recreates) the orderhistory table using setup_table().

Inserts rows one at a time using parameterized SQL to prevent injection attacks.

Skips duplicates automatically using ON CONFLICT (orderid) DO NOTHING.

Writes detailed logs for:

Inserted rows

Duplicate rows

Errors

Missing required values

4. Logging

Each run creates a log file named:
insert_log_YYYYMMDD_HHMMSS.txt

The cleanup_old_logs() function keeps only the 8 most recent logs.

5. Visualization

After ETL completes, the user can choose a category for profit aggregation from:

Channel, Manufacturer, Region, City, Country

The script then generates a bar chart visualizing total profit by the selected category using Matplotlib.


- in try/catch block use the messed up row to upload into a seperate 
table for missing value data "All data is good data"
