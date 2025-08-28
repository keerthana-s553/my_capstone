import sys
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text

# ---------------------------
# Database connection
# ---------------------------
DB_CONFIG = {
    "host": "localhost",
    "database": "mydb",
    "user": "keerthana.s",
    "password": "MyStrongPassword123"
}

def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )

def run_sql(sql):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

# ---------------------------
# Bronze Layer
# ---------------------------
def build_bronze():
    print("Building Bronze layer...")

    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:5432/{DB_CONFIG['database']}"
    )

    # Relative path to bronze CSVs
    csv_folder = os.path.join(os.path.dirname(__file__), "..", "bronze")

    # Create bronze schema if not exists
    run_sql("CREATE SCHEMA IF NOT EXISTS bronze;")

    tables = {
        "students_raw": "students_raw.csv",
        "instructors_raw": "instructors_raw.csv",
        "courses_raw": "courses_raw.csv",
        "enrollments_raw": "enrollments_raw.csv",
        "activity_raw": "activity_raw.csv",
        "payments_raw": "payments_raw.csv"
    }

    for table_name, file_name in tables.items():
        path = os.path.join(csv_folder, file_name)
        if not os.path.exists(path):
            print(f"CSV not found: {path}, skipping...")
            continue
        df = pd.read_csv(path)
        df.to_sql(table_name, engine, schema="bronze", if_exists="replace", index=False)
        print(f"✅ {table_name} loaded into Bronze")

# ---------------------------
# Silver Layer
# ---------------------------
def build_silver():
    print("Building Silver layer...")

    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:5432/{DB_CONFIG['database']}"
    )
    SILVER = "silver"
    metadata = MetaData(schema=SILVER)

    # Create silver schema if not exists
    run_sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER};")

    csv_folder = os.path.join(os.path.dirname(__file__), "..", "bronze_inputs")

    # Load Bronze tables
    df_students = pd.read_csv(os.path.join(csv_folder, "students_raw.csv")).drop_duplicates(subset=["student_id"]).fillna({
        "name": "Unknown", "age": 0, "gender": "Unknown", "country": "Unknown", "subscription_type": "free"
    })

    df_instructors = pd.read_csv(os.path.join(csv_folder, "instructors_raw.csv")).drop_duplicates(subset=["instructor_id"]).fillna({
        "name": "Unknown", "expertise_area": "General", "rating": 0
    })

    df_courses = pd.read_csv(os.path.join(csv_folder, "courses_raw.csv")).drop_duplicates(subset=["course_id"]).fillna({
        "category": "General", "difficulty_level": "Beginner", "duration_hours": 0, "price": 0
    })

    df_enrollments = pd.read_csv(os.path.join(csv_folder, "enrollments_raw.csv")).drop_duplicates(subset=["enrollment_id"]).fillna({
        "status": "active", "progress_percent": 0
    })

    df_activity = pd.read_csv(os.path.join(csv_folder, "activity_raw.csv")).drop_duplicates(subset=["activity_id"]).fillna({
        "video_watched_min": 0, "quiz_score": 0, "assignment_score": 0
    })

    df_payments = pd.read_csv(os.path.join(csv_folder, "payments_raw.csv")).drop_duplicates(subset=["payment_id"]).fillna({
        "amount": 0, "currency": "USD"
    })

    # Upsert function
    def append_safely(df, table_name, conflict_cols=None):
        table = Table(table_name, metadata, autoload_with=engine)
        with engine.begin() as conn:
            for _, row in df.iterrows():
                stmt = pg_insert(table).values(**row.to_dict())
                if conflict_cols:
                    stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)
                conn.execute(stmt)

    append_safely(df_students, "students", conflict_cols=["student_id"])
    append_safely(df_instructors, "instructors", conflict_cols=["instructor_id"])
    append_safely(df_courses, "courses", conflict_cols=["course_id"])
    append_safely(df_enrollments, "enrollments", conflict_cols=["enrollment_id"])
    append_safely(df_activity, "activity", conflict_cols=["activity_id"])
    append_safely(df_payments, "payments", conflict_cols=["payment_id"])

    print("✅ Silver layer built successfully.")

# ---------------------------
# Gold Layer
# ---------------------------
def build_gold():
    print("Building Gold layer...")

    run_sql("CREATE SCHEMA IF NOT EXISTS gold;")

    # Aggregate: total payments per student
    run_sql("""
        CREATE TABLE IF NOT EXISTS gold.student_payment_summary AS
        SELECT student_id, SUM(amount) AS total_paid, COUNT(course_id) AS courses_enrolled
        FROM silver.payments
        GROUP BY student_id;
    """)

    # Aggregate: student activity summary
    run_sql("""
        CREATE TABLE IF NOT EXISTS gold.student_activity_summary AS
        SELECT student_id, AVG(quiz_score) AS avg_quiz, AVG(assignment_score) AS avg_assignment, SUM(video_watched_min) AS total_video_minutes
        FROM silver.activity
        GROUP BY student_id;
    """)

    # Dashboard table
    run_sql("""
        CREATE TABLE IF NOT EXISTS gold.student_dashboard AS
        SELECT p.student_id, sp.total_paid, sp.courses_enrolled, sa.avg_quiz, sa.avg_assignment, sa.total_video_minutes
        FROM silver.students p
        LEFT JOIN gold.student_payment_summary sp ON p.student_id = sp.student_id
        LEFT JOIN gold.student_activity_summary sa ON p.student_id = sa.student_id;
    """)

    print("✅ Gold layer built successfully.")

# ---------------------------
# Main execution
# ---------------------------
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "all":
        build_bronze()
        build_silver()
        build_gold()
        print("🎉 ETL pipeline completed successfully!")
    else:
        print("Usage: python etl.py all")
