import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime

# ----------------------------
# 1. Load CSV and clean headers
# ----------------------------

# Load .csv file into DataFrame
df = pd.read_csv("Dataset.csv", encoding='utf-8-sig')

# Load .json file into DataFrame
#df = pd.read_json("Dataset.json")

#Auto Detect CSV or JSON File   

#----------------------------
# Create log file
#----------------------------
log_filename= f"insert_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
log_file = open(log_filename, "w", encoding="utf-8")

#----------------------------
# 2. Data Cleaning/Transformation
#---------------------------

# Strip whitespace from headers
df.columns = df.columns.str.strip()

# Convert dates from MM/DD/YYYY to YYYY-MM-DD
df['OrderDate'] = pd.to_datetime(df['OrderDate'], format='%m/%d/%Y').dt.date

# Remove duplicate rows
df = df.drop_duplicates()


# ----------------------------
# 3. Connect to PostgreSQL
# ----------------------------
conn = psycopg2.connect(
    dbname="RetailDB",
    user="postgres",
    password="1234",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

#FOR TESTING PURPOSES
#dropping table in DB to add it again
cursor.execute("DROP TABLE IF EXISTS orderhistory;")
cursor.execute("""CREATE TABLE IF NOT EXISTS OrderHistory (
    OrderID INT PRIMARY KEY,
    OrderDate DATE,
    UnitCost DECIMAL(18,8),
    Price DECIMAL(12,2),
    OrderQty INT,
    CostOfSales DECIMAL(18,8),
    Sales DECIMAL(14,2),
    Profit DECIMAL(18,8),
    Channel VARCHAR(150),
    PromotionName VARCHAR(150),
    ProductName VARCHAR(150),
    Manufacturer VARCHAR(150),
    ProductSubCategory VARCHAR(150),
    ProductCategory VARCHAR(150),
    Region VARCHAR(150),
    City VARCHAR(150),
    Country VARCHAR(150)
);""")




# ----------------------------
# 4. INSERT statement (table columns are lowercase)
# ----------------------------
insert_query = """
INSERT INTO orderhistory (
    orderid, orderdate, unitcost, price, orderqty,
    costofsales, sales, profit, channel, promotionname,
    productname, manufacturer, productsubcategory, productcategory,
    region, city, country
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (orderid) DO NOTHING; 
"""
# Transforming
# ^ On conflict clause to skip duplicates based on primary key 'orderid'

# ----------------------------
# 5. Insert each row with logging
# ----------------------------

skippedConflicts = 0
skippedErrors = 0

for index, row in df.iterrows():
    try:
        missing_fields = [key for key, value  in row.items()
                          if value in (None, '', float('nan'))]
        if missing_fields:
            log_file.write(f"Skipped row at index {index} (OrderID={row.get('OrderID', 'N/A')}) due to missing fields: {', '.join(missing_fields)}\n")
            continue
        cursor.execute(insert_query, (
            int(row['OrderID']),
            row['OrderDate'],
            float(row['UnitCost']),
            float(row['Price']),
            int(row['OrderQty']),
            float(row['CostOfSales']),
            float(row['Sales']),
            float(row['Profit']),
            row['Channel'],
            row['PromotionName'],
            row['ProductName'],
            row['Manufacturer'],
            row['ProductSubCategory'],
            row['ProductCategory'],
            row['Region'],
            row['City'],
            row['Country']
        ))

        # Detect if there was a duplicate row confict skip & log it
        if cursor.rowcount == 0:
            skippedConflicts += 1
            print(f"[SKIPPED - CONFLICT] OrderID {row['OrderID']} already exists.")
            log_file.write(f"[DUPLICATE] OrderID {row["OrderID"]} skipped due to conflicit.\n")
    except Exception as e:
        skippedErrors += 1
        print(f"[SKIPPED - ERROR] OrderID {row['OrderID']} caused error. Reason: {e}")
        log_file.write(f"[EROOR] row {index} could not be inserted. Reason: {e}\n")

# ----------------------------
# 5. Commit & close connection
# ----------------------------
conn.commit()
cursor.close()
conn.close()

print("----- Import Summary -----")
print(f"Rows skipped due to conflicts: {skippedConflicts}")
print(f"Rows skipped due to errors: {skippedErrors}")
print("Data imported successfully!")
