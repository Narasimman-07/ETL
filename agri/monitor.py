import mysql.connector
import time
from datetime import datetime

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Nara@#2005',
    'database': 'agridata'
}

print("Starting DB Monitor...")
update_count = 0
while True:
    try:
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM market_prices")
        count = cursor.fetchone()[0]
        
        cursor.execute("SELECT MIN(price_date), MAX(price_date) FROM market_prices")
        min_date, max_date = cursor.fetchone()
        
        cursor.close()
        db.close()
        
        with open("db_status.txt", "w") as f:
            f.write(f"--- LIVE DATABASE STATUS ---\n")
            f.write(f"Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Refreshes: {update_count:,}\n\n")
            f.write(f"Total records currently in database: {count:,}\n")
            f.write(f"Date range of downloaded records: {min_date} to {max_date}\n")
            
        update_count += 1
        time.sleep(420) # Update every 6 minute
        
    except Exception as e:
        with open("db_status.txt", "w") as f:
            f.write(f"Error querying database: {e}\n")
        time.sleep(420)
