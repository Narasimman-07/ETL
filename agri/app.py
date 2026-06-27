import requests
import json
import logging
from datetime import datetime, timedelta
import mysql.connector
from bs4 import BeautifulSoup
import urllib3
import time
urllib3.disable_warnings()
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
from dotenv import load_dotenv

load_dotenv()

# Setup Logging
logging.basicConfig(
    filename='agrimark_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': os.getenv('DB_PORT', 3306)
}

START_DATE = datetime(2021, 6, 1)
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}

class AgrimarkETL:
    def __init__(self):
        self.db = mysql.connector.connect(**DB_CONFIG)
        self.cursor = self.db.cursor()
        
        self.stats = {
            'districts_processed': 0,
            'branches_processed': 0,
            'dates_processed': 0,
            'records_inserted': 0
        }

    def close(self):
        self.cursor.close()
        self.db.close()

    def get_districts(self):
        logging.info("Fetching districts...")
        districts = []
        for attempt in range(5):
            try:
                res = requests.get("https://www.agrimark.tn.gov.in/", headers=HEADERS, verify=False, timeout=30)
                soup = BeautifulSoup(res.text, 'html.parser')
                dist_select = soup.find('select', id='distlist')
                if dist_select:
                    for opt in dist_select.find_all('option'):
                        val = opt.get('value')
                        if val and val != '0':
                            districts.append({'code': int(val), 'name': opt.text.strip()})
                    break
            except Exception as e:
                logging.warning(f"District fetch failed (attempt {attempt+1}): {e}")
                time.sleep(5)
                
        if not districts:
            logging.warning("Failed to fetch districts from website. Falling back to database...")
            self.cursor.execute("SELECT district_code, district_name FROM districts")
            for row in self.cursor.fetchall():
                districts.append({'code': row[0], 'name': row[1]})
            if not districts:
                raise Exception("Failed to fetch districts. Server is down and no database fallback available.")
            
        insert_query = """
            INSERT IGNORE INTO districts (district_code, district_name)
            VALUES (%s, %s)
        """
        self.cursor.executemany(insert_query, [(d['code'], d['name']) for d in districts])
        self.db.commit()
        self.stats['districts_processed'] = len(districts)
        logging.info(f"Loaded {len(districts)} districts.")
        return districts

    def get_branches(self, district_code):
        branches = []
        for attempt in range(3):
            try:
                res = requests.post("https://www.agrimark.tn.gov.in/home/getUSList", data={'district': district_code}, headers=HEADERS, verify=False, timeout=30)
                data = res.json()
                for row in data:
                    branches.append({
                        'id': int(row['master_us_id']),
                        'name': row['us_name'].strip(),
                        'district_code': int(row['dist_id'])
                    })
                break
            except Exception as e:
                time.sleep(2)
        if not branches:
            self.cursor.execute("SELECT branch_id, branch_name FROM branches WHERE district_code = %s", (district_code,))
            for row in self.cursor.fetchall():
                branches.append({'id': row[0], 'name': row[1], 'district_code': district_code})
        return branches

    def fetch_prices(self, date_str, dist_id, us_id):
        url = f"https://www.agrimark.tn.gov.in/home/getPrice_dir/{date_str}/{dist_id}/{us_id}"
        for attempt in range(3):
            try:
                res = requests.post(url, headers=HEADERS, timeout=30, verify=False)
                data = res.json()
                if 'price_list' in data and data['price_list']:
                    return data['price_list']
                return None
            except Exception as e:
                time.sleep(2)
        return None

    def process(self):
        districts = self.get_districts()
        
        all_branches = []
        logging.info("Fetching all branches...")
        for dist in districts:
            branches = self.get_branches(dist['code'])
            all_branches.extend(branches)
            
        if not all_branches:
            raise Exception("Failed to fetch any branches.")
            
        branch_query = """
            INSERT IGNORE INTO branches (branch_id, branch_name, district_code)
            VALUES (%s, %s, %s)
        """
        self.cursor.executemany(branch_query, [(b['id'], b['name'], b['district_code']) for b in all_branches])
        self.db.commit()
        self.stats['branches_processed'] = len(all_branches)
        logging.info(f"Loaded {len(all_branches)} branches.")
        
        end_date = datetime.now()
        delta = end_date - START_DATE
        all_dates = [(START_DATE + timedelta(days=i)).strftime('%d-%m-%Y') for i in range(delta.days + 1)]
        
        logging.info("Checking database for already completed dates...")
        self.cursor.execute("SELECT DISTINCT DATE_FORMAT(price_date, '%d-%m-%Y') FROM market_prices")
        completed_dates = set([row[0] for row in self.cursor.fetchall()])
        dates = [d for d in all_dates if d not in completed_dates]
        
        self.stats['dates_processed'] = len(dates)
        
        if not dates:
            logging.info("All dates are already fully imported. Database is up to date!")
            self.print_summary()
            return
            
        logging.info(f"Starting historical extraction for {len(dates)} missing dates (Skipped {len(completed_dates)})...")
        
        insert_query = """
            INSERT IGNORE INTO market_prices 
            (district_code, branch_id, price_date, item_name, min_price, max_price, qty)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        for date_str in reversed(dates):
            db_date = datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')
            logging.info(f"Processing date: {date_str}...")
            
            daily_records = []
            
            def worker(branch):
                prices = self.fetch_prices(date_str, branch['district_code'], branch['id'])
                if prices:
                    records = []
                    for item_name, item_data in prices.items():
                        records.append((
                            branch['district_code'],
                            branch['id'],
                            db_date,
                            item_name[:150],
                            float(item_data.get('min', 0)) if item_data.get('min') else None,
                            float(item_data.get('max', 0)) if item_data.get('max') else None,
                            float(item_data.get('qty', 0)) if item_data.get('qty') else None
                        ))
                    return records
                return []

            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(worker, b) for b in all_branches]
                for future in as_completed(futures):
                    res = future.result()
                    if res:
                        daily_records.extend(res)
            
            if daily_records:
                self.cursor.executemany(insert_query, daily_records)
                self.db.commit()
                new_records = self.cursor.rowcount
                self.stats['records_inserted'] += new_records
                logging.info(f"  -> Fetched {len(daily_records)}, Successfully inserted {new_records} NEW records for {date_str}")
            else:
                logging.info(f"  -> No records found for {date_str}")

        self.print_summary()

    def print_summary(self):
        logging.info("="*30)
        logging.info("ETL Extraction Complete!")
        logging.info("="*30)
        logging.info(f"Total Districts Processed: {self.stats['districts_processed']}")
        logging.info(f"Total Branches Processed: {self.stats['branches_processed']}")
        logging.info(f"Total Dates Processed: {self.stats['dates_processed']}")
        logging.info(f"Total Commodity Records Inserted: {self.stats['records_inserted']}")

if __name__ == "__main__":
    etl = AgrimarkETL()
    try:
        etl.process()
    except Exception as e:
        logging.error(f"Fatal error during execution: {e}")
    finally:
        etl.close()
