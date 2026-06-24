import requests
import json
import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import os
from bs4 import BeautifulSoup
import urllib3
import time
urllib3.disable_warnings()
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup Logging
logging.basicConfig(
    filename='agrimark_etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

DATABASE_URL = os.getenv(
    'DATABASE_URL',
    'postgresql://agri_63uq_user:hPZNCkCa80anWm9HXlm9M7yd371C1jvY@dpg-d8tn8mojs32c73bv2h40-a.singapore-postgres.render.com/agri_63uq'
)

START_DATE = datetime(2015, 1, 1)
END_DATE = datetime(2025, 12, 31)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

class AgrimarkETL:
    def __init__(self):
        self.db = psycopg2.connect(DATABASE_URL)
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
                time.sleep(2)
                
        if not districts:
            logging.warning("Falling back to local database for districts.")
            self.cursor.execute("SELECT district_code, district_name FROM districts")
            rows = self.cursor.fetchall()
            for r in rows:
                districts.append({'code': r[0], 'name': r[1]})
                
        if not districts:
            raise Exception("Failed to fetch districts and database is empty.")
            
        insert_query = """
            INSERT INTO districts (district_code, district_name)
            VALUES %s
            ON CONFLICT (district_name) DO NOTHING
        """
        execute_values(self.cursor, insert_query, [(d['code'], d['name']) for d in districts])
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
                time.sleep(1)
                
        if not branches:
            logging.warning(f"Falling back to local database for branches of district {district_code}.")
            self.cursor.execute("SELECT branch_id, branch_name, district_code FROM branches WHERE district_code = %s", (district_code,))
            rows = self.cursor.fetchall()
            for r in rows:
                branches.append({'id': r[0], 'name': r[1], 'district_code': r[2]})
                
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
                time.sleep(1)
        return None

    def process(self, start_date=None, end_date=None):
        districts = self.get_districts()
        
        all_branches = []
        logging.info("Fetching all branches...")
        for dist in districts:
            branches = self.get_branches(dist['code'])
            all_branches.extend(branches)
            
        if not all_branches:
            raise Exception("Failed to fetch any branches.")
            
        branch_query = """
            INSERT INTO branches (branch_id, branch_name, district_code)
            VALUES %s
            ON CONFLICT (branch_id) DO NOTHING
        """
        execute_values(self.cursor, branch_query, [(b['id'], b['name'], b['district_code']) for b in all_branches])
        self.db.commit()
        self.stats['branches_processed'] = len(all_branches)
        logging.info(f"Loaded {len(all_branches)} branches.")
        
        start = datetime.strptime(start_date, '%Y-%m-%d') if start_date else START_DATE
        end = datetime.strptime(end_date, '%Y-%m-%d') if end_date else END_DATE
        delta = end - start
        dates = [(start + timedelta(days=i)).strftime('%d-%m-%Y') for i in range(delta.days + 1)]
        self.stats['dates_processed'] = len(dates)
        
        logging.info(f"Starting historical extraction for {len(dates)} dates...")
        
        insert_query = """
            INSERT INTO market_prices 
            (district_code, branch_id, price_date, item_name, min_price, max_price, qty)
            VALUES %s
            ON CONFLICT (branch_id, price_date, item_name) DO NOTHING
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
                execute_values(self.cursor, insert_query, daily_records)
                self.db.commit()
                self.stats['records_inserted'] += len(daily_records)
                logging.info(f"  -> Inserted {len(daily_records)} records for {date_str}")
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
