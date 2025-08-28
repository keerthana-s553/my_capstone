from sqlalchemy import create_engine
import pandas as pd
import os

# Path to Downloads
output_folder = "/home/nineleaps/Downloads/gold_csv"
os.makedirs(output_folder, exist_ok=True)

# SQLAlchemy engine (instead of psycopg2 connection)
engine = create_engine("postgresql+psycopg2://keerthana.s:MyStrongPassword123@localhost:5432/mydb")

tables = [
    "payments_per_course",
    "enrollments_per_course",
    "student_activity_summary",
    "instructor_performance",
    "student_dashboard",
    "top5_courses_by_revenue",
    "age_distribution"
]

for table in tables:
    try:
        df = pd.read_sql(f"SELECT * FROM gold.{table};", engine)
        if not df.empty:
            output_file = os.path.join(output_folder, f"{table}.csv")
            df.to_csv(output_file, index=False)
            print(f"✅ {table}.csv exported to {output_file}")
        else:
            print(f"⚠️ {table} exists but is empty.")
    except Exception as e:
        print(f"❌ Skipping {table}: {e}")

print("🔒 PostgreSQL connection closed.")
