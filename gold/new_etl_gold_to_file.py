import pandas as pd
import psycopg2
import os

# Output folder (Downloads/gold_csv)
output_folder = "/home/nineleaps/Downloads/gold_csv"
os.makedirs(output_folder, exist_ok=True)

# DB connection
conn = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="keerthana.s",
    password="MyStrongPassword123"
)

# Gold tables list for education schema
tables = [
    "dashboard_table",
    "payments_per_course",
    "enrollments_per_course",
    "student_activity_summary",
    "instructor_performance",
    "students_per_country",
    "yearly_student_enrollments",
    "top5_courses_by_revenue",
    "top_course_per_year",
    "age_distribution"
]

# Export loop
for table in tables:
    try:
        df = pd.read_sql(f"SELECT * FROM gold.{table};", conn)
        if not df.empty:
            filepath = f"{output_folder}/{table}.csv"
            df.to_csv(filepath, index=False)
            print(f"{table}.csv exported to {filepath} successfully!")
        else:
            print(f"⚠️ {table} exists but is empty.")
    except Exception as e:
        print(f"⚠️ Skipping {table}: {e}")

# Close connection
conn.close()
