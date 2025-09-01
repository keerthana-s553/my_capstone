import os
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData, ForeignKey
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text

# ===============================
# Database connection
# ===============================
engine = create_engine(
    "postgresql+psycopg2://keerthana.s:MyStrongPassword123@localhost:5432/mydb"
)
metadata = MetaData(schema="silver")

# ===============================
# Paths
# ===============================
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))  # one level up from silver/
BRONZE_PATH = os.path.join(PROJECT_ROOT, "bronze")

def load_csv(filename):
    path = os.path.join(BRONZE_PATH, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")
    df = pd.read_csv(path)
    print(f"📂 Loaded {filename}: {len(df)} rows, {len(df.columns)} cols")
    return df

# ===============================
# Define Silver tables
# ===============================
students = Table(
    "students", metadata,
    Column("student_id", String, primary_key=True),
    Column("name", String),
    Column("age", Integer),
    Column("gender", String),
    Column("country", String),
    Column("signup_date", Date),
    Column("subscription_type", String),
)

instructors = Table(
    "instructors", metadata,
    Column("instructor_id", String, primary_key=True),
    Column("name", String),
    Column("expertise_area", String),
    Column("rating", Float),
    Column("join_date", Date),   # ✅ added
)

courses = Table(
    "courses", metadata,
    Column("course_id", String, primary_key=True),
    Column("course_title", String),
    Column("instructor_id", String, ForeignKey("silver.instructors.instructor_id")),
    Column("category", String),
    Column("difficulty_level", String),
    Column("duration_hours", Float),
    Column("price", Float),
    Column("published_date", Date),
)

enrollments = Table(
    "enrollments", metadata,
    Column("enrollment_id", String, primary_key=True),
    Column("student_id", String, ForeignKey("silver.students.student_id")),
    Column("course_id", String, ForeignKey("silver.courses.course_id")),
    Column("enrollment_date", Date),
    Column("status", String),
    Column("progress_percent", Float),
)

activity = Table(
    "activity", metadata,
    Column("activity_id", String, primary_key=True),
    Column("student_id", String, ForeignKey("silver.students.student_id")),
    Column("course_id", String, ForeignKey("silver.courses.course_id")),
    Column("video_watched_min", Float),
    Column("quiz_score", Float),
    Column("assignment_score", Float),
    Column("timestamp", Date),
)

payments = Table(
    "payments", metadata,
    Column("payment_id", String, primary_key=True),
    Column("student_id", String, ForeignKey("silver.students.student_id")),
    Column("course_id", String, ForeignKey("silver.courses.course_id")),
    Column("amount", Float),
    Column("currency", String),
    Column("payment_date", Date),
    Column("status", String),   # ✅ added
)

# ===============================
# Upsert helper
# ===============================
def upsert_safely(df, table, key_columns):
    if df.empty:
        print(f"⚠️ Skipping {table.name}, no rows")
        return
    with engine.begin() as conn:
        for _, row in df.iterrows():
            row_dict = {col.name: row[col.name] for col in table.columns if col.name in df.columns}
            stmt = insert(table).values(**row_dict)
            update_dict = {col: row_dict[col] for col in row_dict if col not in key_columns}
            stmt = stmt.on_conflict_do_update(index_elements=key_columns, set_=update_dict)
            conn.execute(stmt)
    print(f"✅ {table.name.capitalize()} upserted ({len(df)} rows)")

# ===============================
# Main ETL process
# ===============================
if __name__ == "__main__":
    # Create schema if not exists and recreate tables
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        metadata.drop_all(conn)
        metadata.create_all(conn)
        print("✅ Silver tables recreated")

    # Load CSVs from bronze
    df_students    = load_csv("students_raw.csv")
    df_instructors = load_csv("instructors_raw.csv")
    df_courses     = load_csv("courses_raw.csv")
    df_enrollments = load_csv("enrollments_raw.csv")
    df_activity    = load_csv("activity_raw.csv")
    df_payments    = load_csv("payments_raw.csv")

    # -----------------------------
    # Convert date columns to datetime
    # -----------------------------
    df_students['signup_date']     = pd.to_datetime(df_students['signup_date'], format='%m/%d/%Y', errors='coerce')
    df_instructors['join_date']    = pd.to_datetime(df_instructors['join_date'], format='%m/%d/%Y', errors='coerce')  # ✅ added
    df_courses['published_date']   = pd.to_datetime(df_courses['published_date'], format='%m/%d/%Y', errors='coerce')
    df_enrollments['enrollment_date'] = pd.to_datetime(df_enrollments['enrollment_date'], format='%m/%d/%Y', errors='coerce')
    df_activity['timestamp']       = pd.to_datetime(df_activity['timestamp'], format='%m/%d/%Y', errors='coerce')
    df_payments['payment_date']    = pd.to_datetime(df_payments['payment_date'], format='%m/%d/%Y', errors='coerce')

    # Upsert into Silver
    upsert_safely(df_students, students, ["student_id"])
    upsert_safely(df_instructors, instructors, ["instructor_id"])
    upsert_safely(df_courses, courses, ["course_id"])
    upsert_safely(df_enrollments, enrollments, ["enrollment_id"])
    upsert_safely(df_activity, activity, ["activity_id"])
    upsert_safely(df_payments, payments, ["payment_id"])

    print("🎉 All Silver tables loaded successfully!")
