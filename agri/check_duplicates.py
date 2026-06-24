import mysql.connector

db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Nara@#2005",
    database="agridata"
)
cursor = db.cursor()

query = """
SELECT branch_id, price_date, item_name, COUNT(*) as duplicate_count 
FROM market_prices 
GROUP BY branch_id, price_date, item_name 
HAVING COUNT(*) > 1;
"""
cursor.execute(query)
duplicates = cursor.fetchall()

if duplicates:
    print(f"WARNING: Found {len(duplicates)} duplicate sets!")
else:
    print("SUCCESS: 0 duplicates found in the database. Every record is completely unique!")

cursor.close()
db.close()
