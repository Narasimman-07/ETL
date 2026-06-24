# Agrimark Tamil Nadu ETL - Technical Documentation

This document outlines the technical specifications, architecture, and rate-limiting strategy used by the ETL pipeline to safely extract historical commodity price data from the Tamil Nadu Agrimark portal.

## 1. Data Source and Endpoints
The pipeline pulls data exclusively from the official Agrimark portal (`https://www.agrimark.tn.gov.in/`). It relies on three primary, unauthenticated internal API endpoints:
1. **District Lookup**: Scrapes the homepage `distlist` `<select>` tag.
2. **Branch Lookup**: Sends a POST request to `/home/getUSList` with the `district` ID to get all Uzhavar Sandhai branches.
3. **Price Lookup**: Sends a POST request to `/home/getPrice_dir/{date}/{dist_id}/{us_id}` to fetch the commodity prices for a specific branch on a specific date.

## 2. Extraction Scope
* **Historical Range**: June 1, 2021 to the Current Date (Approx 1,849 days).
* **Scale**: Fetches 38 districts and 194 branches, meaning the script makes exactly **194 requests per day**, totaling around **358,000 HTTP requests** for the full 5-year history.
* **Volume**: Each day successfully processed inserts an average of **8,500 to 8,800 individual records** into the database.

## 3. Data Fetched (Database Schema)
For each commodity, the following data points are extracted and stored in the `market_prices` table:
* `district_code` (e.g., 2)
* `branch_id` (e.g., 14)
* `price_date` (e.g., 2026-06-23)
* `item_name` (e.g., "Tomato - Local")
* `min_price` (₹)
* `max_price` (₹)
* `qty` (Arrival quantity in quintals/kgs)

## 4. Rate Limiting & Sleep Timings (CRITICAL)
Due to strict Web Application Firewall (WAF) protections on the government server, rapid unthrottled requests will result in an immediate IP Ban (HTTP timeouts/connection blocks). To bypass this and safely extract data, the script uses heavy throttling:
* **Max Concurrent Workers**: Hardcoded to `5` threads (`ThreadPoolExecutor(max_workers=5)`). This is the maximum safe concurrency level.
* **HTTP Timeouts**: All requests have a strict `timeout=10` parameter to prevent the script from freezing if the server drops packets.
* **Retry Delays**: 
  * If a price fetch fails or times out, the script sleeps for `1 second` (`time.sleep(1)`) and retries up to 3 times.
  * If a district fetch fails, the script sleeps for `2 seconds` (`time.sleep(2)`) and retries up to 5 times.
* **Execution Speed**: With these safety limits in place, it takes exactly **6 to 7 minutes to process a single day**. Extracting the full 5-year history will take approximately **7.5 to 8 days of continuous execution**.

## 5. Fault Tolerance and Resumption
The ETL process is fully resumable:
* It relies on a unique composite key (`branch_id`, `price_date`, `item_name`) in the database.
* Data is inserted using MySQL's `INSERT IGNORE` command.
* If the script is stopped, cancelled, or your computer shuts down, simply running `python etl.py` again will cause it to instantly skip any data already safely downloaded, and resume exactly where it left off without duplicating records.
