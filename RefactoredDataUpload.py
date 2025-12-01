import pandas as pd
import psycopg2
from datetime import datetime
import os
import glob


# -----------------------------------------------------------
# CONFIG
# -----------------------------------------------------------
REQUIRED_COLUMNS = [
    "OrderID", "OrderDate", "UnitCost", "Price", "OrderQty",
    "CostOfSales", "Sales", "Profit", "ProductName",
    "Manufacturer", "Country"
]

INSERT_QUERY = """
INSERT INTO orderhistory (
    orderid, orderdate, unitcost, price, orderqty,
    costofsales, sales, profit, channel, promotionname,
    productname, manufacturer, productsubcategory, productcategory,
    region, city, country
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (orderid) DO NOTHING;
"""


# -----------------------------------------------------------
# 1. Extract
# -----------------------------------------------------------
def load_file(file_path: str) -> pd.DataFrame:
    """Loads CSV or JSON into a DataFrame."""

    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path, encoding='utf-8-sig')
    else:
        df = pd.read_json(file_path)

    return df


# -----------------------------------------------------------
# 2. Transform
# -----------------------------------------------------------
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans headers, converts dates, and removes duplicates."""

    # Strip extra spaces from column names
    df.columns = df.columns.str.strip()

    # Standardize date formats
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], format='%m/%d/%Y',errors='coerce').dt.date

    # Remove duplicates
    df = df.drop_duplicates()

    return df


def is_valid_row(row: pd.Series) -> bool:
    """Checks if row has all required fields."""

    for col in REQUIRED_COLUMNS:
        if col not in row or pd.isna(row[col]) or str(row[col]).strip() == "":
            return False

    return True


# -----------------------------------------------------------
# 3. Database Connection
# -----------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        dbname="RetailDB",
        user="postgres",
        password="1234",
        host="localhost",
        port="5432"
    )

# Recreates table for testing/demo purposes.

def setup_table(cursor):
    cursor.execute("DROP TABLE IF EXISTS orderhistory;")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS OrderHistory (
            OrderID INT PRIMARY KEY NOT NULL,
            OrderDate DATE NOT NULL,
            UnitCost DECIMAL(18,8) NOT NULL,
            Price DECIMAL(12,2) NOT NULL,
            OrderQty INT NOT NULL,
            CostOfSales DECIMAL(18,8) NOT NULL,
            Sales DECIMAL(14,2) NOT NULL,
            Profit DECIMAL(18,8) NOT NULL,
            Channel VARCHAR(150),
            PromotionName VARCHAR(150),
            ProductName VARCHAR(150) NOT NULL,
            Manufacturer VARCHAR(150) NOT NULL,
            ProductSubCategory VARCHAR(150),
            ProductCategory VARCHAR(150),
            Region VARCHAR(150),
            City VARCHAR(150),
            Country VARCHAR(150) NOT NULL
        );
    """)


# -----------------------------------------------------------
# 4. Load (Insert)
# -----------------------------------------------------------
def insert_row(cursor, row, log_file):
    """Attempts to insert a row and logs the outcome."""

    try:
        cursor.execute(INSERT_QUERY, (
            int(row['OrderID']),
            row['OrderDate'],
            float(row['UnitCost']),
            float(row['Price']),
            int(row['OrderQty']),
            float(row['CostOfSales']),
            float(row['Sales']),
            float(row['Profit']),
            row.get('Channel'),
            row.get('PromotionName'),
            row.get('ProductName'),
            row.get('Manufacturer'),
            row.get('ProductSubCategory'),
            row.get('ProductCategory'),
            row.get('Region'),
            row.get('City'),
            row.get('Country')
        ))

        # Detect duplicate
        if cursor.rowcount == 0:
            log_file.write(f"[DUPLICATE] OrderID {row['OrderID']} skipped.\n")
            return "duplicate"

        return "inserted"

    except Exception as e:
        log_file.write(f"[ERROR] OrderID {row.get('OrderID')} failed: {e}\n")
        return "error"

def getUserInput(cursor,row,log_file):
    answer = input("Would you like to input a row?")

# -----------------------------------------------------------
# 5. Log Cleanup
# -----------------------------------------------------------
def cleanup_old_logs(max_logs=8):
    """Keeps only the most recent N log files."""
    logs = sorted(
        glob.glob("insert_log_*.txt"),
        key=os.path.getmtime,
        reverse=True
    )
    for old_log in logs[max_logs:]:
        os.remove(old_log)


# -----------------------------------------------------------
# 6. Main ETL Runner
# -----------------------------------------------------------
def main():
    # Choose file
    file_path = "messy_dataset.csv"

    # Load
    df = load_file(file_path)

    # Clean data
    df = clean_data(df)

    # Create log file
    log_filename = f"insert_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    log_file = open(log_filename, "w", encoding="utf-8")

    # Connect to DB
    conn = get_db_connection()
    cursor = conn.cursor()

    # Setup fresh table (testing/demo)
    setup_table(cursor)

    # Tracking Counters
    skipped_conflicts = 0
    skipped_errors = 0
    inserted_rows = 0

    # Insert row by row
    for index, row in df.iterrows():

        if not is_valid_row(row):
            log_file.write(f"[MISSING] Row {index} skipped due to missing values.\n")
            skipped_errors += 1
            continue

        result = insert_row(cursor, row, log_file)

        if result == "duplicate":
            skipped_conflicts += 1
        elif result == "inserted":
            inserted_rows += 1
        elif result == "error":
            skipped_errors += 1



    # Commit & close
    conn.commit()
    cursor.close()
    conn.close()
    log_file.close()

    # Log cleanup
    cleanup_old_logs()

    # Summary
    print("----- IMPORT SUMMARY -----")
    print(f"Inserted rows: {inserted_rows}")
    print(f"Skipped conflicts: {skipped_conflicts}")
    print(f"Skipped errors: {skipped_errors}")
    print("ETL complete.")


# -----------------------------------------------------------
# Entry Point
# -----------------------------------------------------------
if __name__ == "__main__":
    main()
