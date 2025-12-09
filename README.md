ETL Pipeline for Order Data

==================Overview==================

This Python-based ETL (Extract, Transform, Load) pipeline processes order data from CSV or JSON files and loads it into a PostgreSQL database. The pipeline is designed to handle missing values, duplicates, and data errors by logging them and storing rejected rows in a separate database table. It also provides basic visualization of profits by category.

==================Features==================

Extract: Reads CSV or JSON files into Pandas DataFrames.

Transform: Cleans data by standardizing column headers, converting dates, and handling missing or invalid values.

Load: Inserts validated rows into the orderhistory table in PostgreSQL.

Rejected Data Handling: Rows with missing values, duplicates, or insert errors are stored in a rejecteddata table for review.

Logging: Generates log files recording inserted rows, duplicates, errors, and rejected rows.

Visualization: Creates bar charts showing total profit by user-selected categories.

==================Technologies Used==================

Python 3

Pandas

Matplotlib

PostgreSQL (via psycopg2)

python-dotenv for environment variable management

==================Database Tables==================
orderhistory Table

Stores validated order data.
Columns:

OrderID (Primary Key)

OrderDate, UnitCost, Price, OrderQty, CostOfSales, Sales, Profit

Channel, PromotionName, ProductName, Manufacturer

ProductSubCategory, ProductCategory

Region, City, Country

rejecteddata Table

Stores rows that failed validation or caused errors during insertion.
Columns mirror orderhistory, but all columns allow NULL values.

==================ETL Pipeline Workflow==================

Extract

Load the data file using load_file() depending on file type.

Transform

Clean column headers and standardize the OrderDate format using clean_data().

Validate rows with is_valid_row().

Load

Insert valid rows into orderhistory using insert_row().

Insert rejected rows (duplicates or rows with missing values/errors) into rejecteddata using insert_rejected_rows().

==================Logging==================

Generate a timestamped log file recording inserted rows, duplicates, missing rows, and errors.

Automatically clean up old log files beyond a specified limit.

==================Visualization==================

Optionally create bar charts of total profit by user-selected category (Channel, Manufacturer, Region, City, Country).

Handling of Special Cases

Duplicates: Detected using PostgreSQL ON CONFLICT (orderid) DO NOTHING. Duplicates are logged and inserted into rejecteddata.

Missing Values / Invalid Data: Rows missing required columns or with empty/null values are rejected and inserted into rejecteddata.

Insertion Errors: Rows that cause exceptions during insertion (e.g., type mismatch) are also logged and stored in rejecteddata.

==================Usage==================

Environment Variables
Create a .env file in the project root with your PostgreSQL credentials:

DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydatabase
DB_USER=myuser
DB_PASSWORD=mypassword


==================Run the ETL Script==================

python etl_pipeline.py


The script will create or reset orderhistory and rejecteddata tables.

Logs are generated in insert_log_YYYYMMDD_HHMMSS.txt.

View Profit Charts

After the ETL run, the script prompts the user to select a category to visualize total profits.

==================Notes==================

Ensure your input CSV or JSON files have the required columns:

OrderID, OrderDate, UnitCost, Price, OrderQty, CostOfSales, Sales, Profit, ProductName, Manufacturer, Country


The pipeline is designed for incremental data insertion with proper handling of duplicates and invalid rows.

Rejected rows allow data analysts to review errors without interrupting the main data flow.