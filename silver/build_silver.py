import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
import os

# ---------------- Database Connection ----------------
engine = create_engine("postgresql+psycopg2://keerthana.s:MyStrongPassword123@localhost:5432/mydb")
SILVER = "silver"

# ---------------- Create Schema if Not Exists ----------------
with engine.connect() as conn:
    conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {SILVER}'))
    conn.commit()

# ---------------- Path to Bronze CSVs ----------------
csv_folder = "../bronze"  # Update path if needed

# ---------------- Load Bronze CSVs ----------------
df_students = pd.read_csv(os.path.join(csv_folder, "students_raw.csv"))
df_instructors = pd.read_csv(os.path.join(csv_folder, "instructors_raw.csv"))
df_courses = pd.read_csv(os.path.join(csv_folder, "courses_raw.csv"))
df_enrollments = pd.read_csv(os.path.join(csv_folder, "enrollments_raw.csv"))
df_activity = pd.read_csv(os.path.join(csv_folder, "activity_raw.csv"))
df_payments = pd.read_csv(os.path.join(csv_folder, "payments_raw.csv"))

# ---------------- Data Cleaning ----------------

# Students
df_students_clean = df_students.drop_duplicates(subset=["student_id"])
df_students_clean = df_students_clean.fillna({
    "name": "Unknown",
    "age": 0,
    "gender": "Unknown",
    "country": "Unknown",
    "subscription_type": "free"
})

# Instructors
df_instructors_clean = df_instructors.drop_duplicates(subset=["instructor_id"])
df_instructors_clean = df_instructors_clean.fillna({
    "name": "Unknown",
    "expertise_area": "General",
    "rating": 0
})

# Courses
df_courses_clean = df_courses.drop_duplicates(subset=["course_id"])
df_courses_clean = df_courses_clean.fillna({
    "category": "General",
    "difficulty_level": "Beginner",
    "duration_hours": 0,
    "price": 0
})

# Enrollments
df_enrollments_clean = df_enrollments.drop_duplicates(subset=["enrollment_id"])
df_enrollments_clean = df_enrollments_clean.fillna({
    "status": "active",
    "progress_percent": 0
})

# Activity
df_activity_clean = df_activity.drop_duplicates(subset=["activity_id"])
df_activity_clean = df_activity_clean.fillna({
    "video_watched_min": 0,
    "quiz_score": 0,
    "assignment_score": 0
})

# Payments
df_payments_clean = df_payments.drop_duplicates(subset=["payment_id"])
df_payments_clean = df_payments_clean.fillna({
    "amount": 0,
    "currency": "USD"
})

# ---------------- Function to Upsert into Silver Schema ----------------
metadata = MetaData(schema=SILVER)

def append_safely(df, table_name, conflict_cols):
    table = Table(table_name, metadata, autoload_with=engine)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            stmt = pg_insert(table).values(**row.to_dict())
            stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
            conn.execute(stmt)

# ---------------- Append Data ----------------
append_safely(df_students_clean, "students", ["student_id"])
print("✅ Students appended")

append_safely(df_instructors_clean, "instructors", ["instructor_id"])
print("✅ Instructors appended")

append_safely(df_courses_clean, "courses", ["course_id"])
print("✅ Courses appended")

append_safely(df_enrollments_clean, "enrollments", ["enrollment_id"])
print("✅ Enrollments appended")

append_safely(df_activity_clean, "activity", ["activity_id"])
print("✅ Activity appended")

append_safely(df_payments_clean, "payments", ["payment_id"])
print("✅ Payments appended")

print("🎉 All Bronze data successfully loaded into Silver schema!")
