import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# -------------------------------
# Configuration
# -------------------------------
DB_URI = "postgresql+psycopg2://keerthana.s:MyStrongPassword123@localhost:5432/mydb"
OUTPUT_FOLDER = "/home/nineleaps/Downloads/gold_csv"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------------------
# Create SQLAlchemy engine
# -------------------------------
engine = create_engine(DB_URI)

# -------------------------------
# Fetch all tables in 'gold' schema
# -------------------------------
with engine.connect() as conn:
    tables_query = text("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'gold';
    """)
    result = conn.execute(tables_query)
    gold_tables = [row[0] for row in result.fetchall()]

# -------------------------------
# Export each table to CSV
# -------------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

for table in gold_tables:
    try:
        df = pd.read_sql(f"SELECT * FROM gold.{table};", engine)
        if not df.empty:
            filename = f"{table}_{timestamp}.csv"
            filepath = os.path.join(OUTPUT_FOLDER, filename)
            df.to_csv(filepath, index=False)
            print(f"✅ {table}.csv exported to {filepath}")
        else:
            print(f"⚠️ {table} exists but is empty.")
    except Exception as e:
        print(f"❌ Skipping {table}: {e}")

print("🎉 All Gold tables exported successfully!")
