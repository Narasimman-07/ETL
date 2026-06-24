from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import io
import csv
from typing import Optional
from datetime import datetime

# Import the ETL process
from etl import AgrimarkETL, DATABASE_URL

app = FastAPI(title="Agrimark ETL API")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

@app.get("/")
def read_root():
    return {"message": "Welcome to Agrimark ETL API"}

@app.get("/stats")
def get_stats():
    """
    Returns the total number of records, and the date range available in the database.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT 
                COUNT(*) as total_records,
                MIN(price_date) as min_date,
                MAX(price_date) as max_date
            FROM market_prices
        """)
        stats = cur.fetchone()
        cur.close()
        conn.close()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/run-etl")
def run_etl(start_date: str = Query(..., description="Start date in YYYY-MM-DD format"), 
            end_date: str = Query(..., description="End date in YYYY-MM-DD format")):
    """
    Triggers the ETL process for a specific date range.
    """
    try:
        # Validate dates
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
        
        etl = AgrimarkETL()
        try:
            etl.process(start_date=start_date, end_date=end_date)
            return {"message": "ETL process completed successfully", "stats": etl.stats}
        finally:
            etl.close()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data")
def get_data(start_date: Optional[str] = None, end_date: Optional[str] = None, limit: int = 100):
    """
    Fetch market prices, optionally filtered by a date range.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM market_prices"
        params = []
        conditions = []
        
        if start_date:
            conditions.append("price_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("price_date <= %s")
            params.append(end_date)
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += " ORDER BY price_date DESC LIMIT %s"
        params.append(limit)
        
        cur.execute(query, tuple(params))
        data = cur.fetchall()
        cur.close()
        conn.close()
        
        return {"count": len(data), "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download-csv")
def download_csv(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """
    Download market prices as a CSV file, optionally filtered by a date range.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        query = "SELECT * FROM market_prices"
        params = []
        conditions = []
        
        if start_date:
            conditions.append("price_date >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("price_date <= %s")
            params.append(end_date)
            
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += " ORDER BY price_date DESC"
        
        cur.execute(query, tuple(params))
        data = cur.fetchall()
        cur.close()
        conn.close()

        # Create CSV in memory
        output = io.StringIO()
        if data:
            # Use keys from the first row as headers
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        else:
            output.write("No data found for the given date range.")

        return Response(
            content=output.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=market_prices_{start_date or 'all'}_to_{end_date or 'all'}.csv"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    # Run using `python app.py` or `uvicorn app:app --reload`
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
