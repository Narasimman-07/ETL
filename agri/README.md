# Agrimark Tamil Nadu ETL Pipeline

This project contains a Python-based ETL (Extract, Transform, Load) pipeline designed to automatically extract historical commodity prices from the official Tamil Nadu Agrimark portal (`agrimark.tn.gov.in`) and load them into a local MySQL database.

## Prerequisites

1. **Python 3.8+** installed on your system.
2. **MySQL Server** installed and running.
3. Database user credentials with privileges to create tables and insert data.

## Setup Instructions

### 1. Database Setup
The script requires a database named `agridata` to exist, and the credentials must match `root` and password `Nara@#2005` (as defined in the script). 

To initialize the database schema, run the following SQL script in your MySQL client or command line:
```bash
mysql -u root -p < schema.sql
```

### 2. Install Python Dependencies
Install the required Python packages using pip:
```bash
pip install -r requirements.txt
```

## Running the Extraction

To begin the historical extraction from `2021-06-01` through the current date, simply run:

```bash
python etl.py
```

### Features
* **Duplicate Prevention:** Uses `INSERT IGNORE` in MySQL based on unique composite keys (`branch_id`, `price_date`, `item_name`) to ensure identical records are skipped gracefully.
* **Multithreaded:** The script uses `ThreadPoolExecutor` to simultaneously fetch data from multiple markets concurrently, significantly speeding up the extraction.
* **Logging:** Logs all extraction metrics and output to `agrimark_etl.log` and the console.
* **Normalization:** District and market names are mapped to relational tables to reduce overall database size.

## Expected Performance
Extracting over 1,800 days across ~180 markets requires approximately 330,000 HTTP POST requests. 
Depending on your internet connection and the responsiveness of the Agrimark servers, a full historical load from 2021 to today may take **18 to 24 hours**. 

You can safely stop `Ctrl+C` and restart the script at any time. Because of the `INSERT IGNORE` SQL constraints, it will skip already downloaded records and continue without inserting duplicate rows.
