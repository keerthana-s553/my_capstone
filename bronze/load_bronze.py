import os
import pandas as pd
from sqlalchemy import create_engine

# Function to load CSV files relative to the script's location
def load_csv(filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))  # path of the script
    path = os.path.join(script_dir, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")
    return pd.read_csv(path)

# Database connection setup
def get_engine():
    # Replace with your actual database credentials
    user = "keerthana.s"
    password = "MyStrongPassword123"
    host = "localhost"
    port = "5432"
    database = "mydb"

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    return engine

# Load CSV files
df_students = load_csv("students_raw.csv")
df_courses = load_csv("courses_raw.csv")
df_instructors = load_csv("instructors_raw.csv")
df_enrollments = load_csv("enrollments_raw.csv")
df_payments = load_csv("payments_raw.csv")
df_activity = load_csv("activity_raw.csv")

# Load into database
engine = get_engine()

df_students.to_sql("students", engine, if_exists="replace", index=False)
df_courses.to_sql("courses", engine, if_exists="replace", index=False)
df_instructors.to_sql("instructors", engine, if_exists="replace", index=False)
df_enrollments.to_sql("enrollments", engine, if_exists="replace", index=False)
df_payments.to_sql("payments", engine, if_exists="replace", index=False)
df_activity.to_sql("activity", engine, if_exists="replace", index=False)

print("All CSV files loaded successfully into the database.")
