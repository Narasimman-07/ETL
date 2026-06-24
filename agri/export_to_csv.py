import mysql.connector
import csv

DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Nara@#2005',
    'database': 'agridata'
}

def export_data_to_csv():
    try:
        # Connect to the database
        db = mysql.connector.connect(**DB_CONFIG)
        cursor = db.cursor()

        # Execute the query
        query = """
        select m.price_date, d.district_name, b.branch_name, m.item_name, m.min_price, m.max_price, m.qty 
        from market_prices m, branches b, districts d where
        b.branch_id = m.branch_id and d.district_code = m.district_code
        order by m.price_date, m.district_code, m.branch_id;
        """
        print("Fetching data from the database...")
        cursor.execute(query)

        # Get column names
        columns = [i[0] for i in cursor.description]

        # Fetch all rows
        rows = cursor.fetchall()

        # Write to CSV
        csv_filename = "market_prices_export.csv"
        print(f"Writing {len(rows)} records to {csv_filename}...")
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)
            writer.writerows(rows)

        print(f"Data successfully exported to {csv_filename}")

    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        if 'db' in locals() and db.is_connected():
            cursor.close()
            db.close()

if __name__ == "__main__":
    export_data_to_csv()
