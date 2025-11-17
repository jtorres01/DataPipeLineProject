import pandas as pd
import psycopg2
from psycopg2 import sql

# ----------------------------
# 1. Load CSV and clean headers
# ----------------------------

# Load .csv file into DataFrame
#df = pd.read_csv("Dataset.csv", encoding='utf-8-sig')

# Load .json file into DataFrame
df = pd.read_json("Dataset.json")

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

for _, row in df.iterrows():
    try:
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

        # Detect if there was a duplicate row confict skip
        if cursor.rowcount == 0:
            skippedConflicts += 1
            print(f"[SKIPPED - CONFLICT] OrderID {row['OrderID']} already exists.")
    except Exception as e:
        skippedErrors += 1
        print(f"[SKIPPED - ERROR] OrderID {row['OrderID']} caused error. Reason: {e}")
    

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
