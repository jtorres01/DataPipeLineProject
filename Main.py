import pandas as pd
import psycopg2
from psycopg2 import sql

# ----------------------------
# 1. Load CSV and clean headers
# ----------------------------
df = pd.read_csv("Dataset.csv", encoding='utf-8-sig')

# Strip whitespace from column names
df.columns = df.columns.str.strip()

# Convert dates from MM/DD/YYYY to YYYY-MM-DD
df['OrderDate'] = pd.to_datetime(df['OrderDate'], format='%m/%d/%Y').dt.date

# ----------------------------
# 2. Connect to PostgreSQL
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
# 3. INSERT statement (table columns are lowercase)
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

# ----------------------------
# 4. Insert each row
# ----------------------------
for _, row in df.iterrows():
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

# ----------------------------
# 5. Commit & close connection
# ----------------------------
conn.commit()
cursor.close()
conn.close()

print("Data imported successfully!")
