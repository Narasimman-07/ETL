import mysql.connector

def init_db():
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Nara@#2005"
    )
    cursor = db.cursor()
    
    # Drop existing tables if they have the wrong schema
    try:
        cursor.execute("USE agridata")
        cursor.execute("DROP TABLE IF EXISTS market_prices")
        cursor.execute("DROP TABLE IF EXISTS branches")
        cursor.execute("DROP TABLE IF EXISTS districts")
    except Exception as e:
        print(f"Error dropping tables: {e}")
    
    with open("schema.sql", "r") as f:
        sql = f.read()
        
    statements = sql.split(';')
    for statement in statements:
        if statement.strip():
            cursor.execute(statement)
            
    db.commit()
    cursor.close()
    db.close()
    print("Database and tables dropped and recreated successfully!")

if __name__ == "__main__":
    init_db()
