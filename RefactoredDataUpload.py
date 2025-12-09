import pandas as pd
import psycopg2
from datetime import datetime
import os
import glob
import matplotlib.pyplot as plt
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")



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
INSERT_QUERY_REJECTED = """ 
INSERT INTO rejecteddata (
    orderid, orderdate, unitcost, price, orderqty,
    costofsales, sales, profit, channel, promotionname,
    productname, manufacturer, productsubcategory, productcategory,
    region, city, country
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
    df['OrderDate'] = pd.to_datetime(df['OrderDate'],
                                      format='%m/%d/%Y',
                                      errors='coerce'
                                      ).dt.date

    # Remove duplicates
    # I could just have the below line run and get rid of all duplicates before attemping to insert into database,
    # but I rather have my insert_row method catch it and log all duplicates

    #df = df.drop_duplicates(subset=["OrderID"])

    return df


def is_valid_row(cursor, row: pd.Series, log_file) -> bool:
    # Checks if row has all required fields.

    for col in REQUIRED_COLUMNS:
        if col not in row:
            return False

        value = row[col]

        if pd.isna(row[col]):
            return False
        
        if isinstance(value,str) and value.strip() in ("", "nan", "none", "NaT"):
            return False
        
    return True


# -----------------------------------------------------------
# 3. Database Connection
# -----------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# Recreates table for testing/demo purposes.

def setup_table(cursor):

    cursor.execute("DROP TABLE IF EXISTS orderhistory;")

    cursor.execute("""
        CREATE TABLE orderhistory (
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

    cursor.execute("DROP TABLE IF EXISTS rejecteddata;")

    print("Creating rejecteddata table")
    cursor.execute("""
        CREATE TABLE rejecteddata (
            OrderID INT,
            OrderDate DATE,
            UnitCost DECIMAL(18,8),
            Price DECIMAL(12,2),
            OrderQty INT,
            CostOfSales DECIMAL(18,8),
            Sales DECIMAL(14,2),
            Profit DECIMAL(18,8) ,
            Channel VARCHAR(150),
            PromotionName VARCHAR(150),
            ProductName VARCHAR(150),
            Manufacturer VARCHAR(150),
            ProductSubCategory VARCHAR(150),
            ProductCategory VARCHAR(150),
            Region VARCHAR(150),
            City VARCHAR(150),
            Country VARCHAR(150)
        );
    """)



# -----------------------------------------------------------
# 4. Load (Insert)
# -----------------------------------------------------------
def insert_row(cursor, row, log_file):
    #Attempts to insert a row and logs the outcome.

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
            log_file.write(f"[DUPLICATE] OrderID {row['OrderID']} skipped at index {row.name}.\n")
            return "duplicate"

        return "inserted"

    except Exception as e:
        # Rolling back previous query that caused an error to allow next queries to run
        cursor.connection.rollback()
        log_file.write(f"[ERROR] OrderID {row.get('OrderID')} failed: {e}\n")
        return "error"

# Insert Functin used to insert into rejecteddata table
def insert_rejected_rows(cursor, row, log_file):

    try:
        # This test for edges cases where dates NaT(Not a Time) ex: 99/99/9999
        # Sets NaT to None
        order_date = None if pd.isna(row['OrderDate']) else row["OrderDate"]

        cursor.execute(INSERT_QUERY_REJECTED, (
                int(row['OrderID']) if pd.notna(row['OrderID']) else None,
                order_date,
                float(row['UnitCost']) if pd.notna(row['UnitCost']) else None,
                float(row['Price']) if pd.notna(row['Price']) else None,
                int(row['OrderQty']) if pd.notna(row['OrderQty']) else None,
                float(row['CostOfSales']) if pd.notna(row['CostOfSales']) else None,
                float(row['Sales']) if pd.notna(row['Sales']) else None,
                float(row['Profit']) if pd.notna(row['Profit']) else None,
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
    except Exception as e:
        log_file.write(f"[REJECTED ERROR] Failed to insert rejected row: {e}\n")

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
    file_path = "DatasetMessy.csv"

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

        if not is_valid_row(cursor,row, log_file):
            log_file.write(f"[MISSING] Row {index} skipped due to missing values.\n")
            insert_rejected_rows(cursor, row, log_file)
            skipped_errors += 1
            continue

        result = insert_row(cursor, row, log_file)

        if result == "duplicate":
            insert_rejected_rows(cursor, row, log_file)
            skipped_conflicts += 1
        elif result == "inserted":
            inserted_rows += 1
        elif result == "error":
            insert_rejected_rows(cursor, row, log_file)
            skipped_errors += 1

    

    # Commit & close
    conn.commit()
    cursor.close()
    conn.close()
    log_file.write("----- IMPORT SUMMARY ----- \n")
    log_file.write(f"Inserted rows: {inserted_rows}\n")
    log_file.write(f"Skipped conflicts: {skipped_conflicts}\n")
    log_file.write(f"Skipped errors: {skipped_errors}\n")
    log_file.write("ETL complete.")
    log_file.close()

    # Log cleanup
    cleanup_old_logs()

    # Summary
    print("----- IMPORT SUMMARY -----")
    print(f"Inserted rows: {inserted_rows}")
    print(f"Skipped conflicts: {skipped_conflicts}")
    print(f"Skipped errors: {skipped_errors}")
    print("ETL  complete.")

    # Getting user input and creating Graph
    categories = ["Channel", "Manufacturer","Region", "City", "Country"]
    
    while True:   
        print(categories)
        userInput = input(f"Which category would you like to sort by? (Enter q to exit)")
        userInput =userInput.title().strip()
        # Q input exits loop
        if userInput == "Q" or userInput == "Q":
            print("Exit successful.")
            break
        
        # Invalid input reruns loop
        elif userInput not in categories:
            print("Not a valid parameter. Please try again!")
            continue

        print("Outputing graph")
        plot_profit_By_UserInput(df,userInput)
        break # Exit the loop
      

def plot_profit_By_UserInput(df,col):
    
    profitBy = df.groupby(col)["Profit"].sum().sort_values(ascending=False)

    # Creating bar graph
    plt.figure(figsize=(10,6))
    plt.bar(profitBy.index, profitBy.values)
    plt.xlabel(col)
    plt.ylabel("Profit")
    plt.title("Total Profit by Manufacturer")
    plt.xticks(rotation =80)
    plt.tight_layout()
    plt.show()

# -----------------------------------------------------------
# Entry Point
# -----------------------------------------------------------
if __name__ == "__main__":
    main()
