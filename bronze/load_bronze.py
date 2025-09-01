import os
import pandas as pd
from sqlalchemy import create_engine, text

def load_csv(filename):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(script_dir, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV file not found: {path}")
    print(f"\n📂 Loading CSV: {path}")
    df = pd.read_csv(path)
    print(f"   → {len(df)} rows, {len(df.columns)} cols")
    print(df.head(2))  # preview first rows
    return df

def get_engine():
    user = "keerthana.s"
    password = "MyStrongPassword123"
    host = "localhost"
    port = "5432"
    database = "mydb"

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
        echo=False
    )
    return engine

def load_to_db(df, table_name, engine, schema="bronze"):
    try:
        # make sure schema exists
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        # load data
        df.to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)

        # verify row count
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table_name}"))
            count = result.scalar()
        print(f"✅ Loaded table '{schema}.{table_name}' with {count} rows")

    except Exception as e:
        print(f"❌ Error updating {schema}.{table_name}: {e}")

# ---------------- MAIN ---------------- #
engine = get_engine()

# confirm DB + user
with engine.connect() as conn:
    info = conn.execute(text("SELECT current_database(), current_user;")).fetchall()
    print("\n🔎 Connected to DB:", info)

# load CSVs
df_students = load_csv("students_raw.csv")
df_courses = load_csv("courses_raw.csv")
df_instructors = load_csv("instructors_raw.csv")
df_enrollments = load_csv("enrollments_raw.csv")
df_payments = load_csv("payments_raw.csv")
df_activity = load_csv("activity_raw.csv")

# push into bronze schema
load_to_db(df_students, "students", engine, schema="bronze")
load_to_db(df_courses, "courses", engine, schema="bronze")
load_to_db(df_instructors, "instructors", engine, schema="bronze")
load_to_db(df_enrollments, "enrollments", engine, schema="bronze")
load_to_db(df_payments, "payments", engine, schema="bronze")
load_to_db(df_activity, "activity", engine, schema="bronze")

print("\n🎉 All CSV files loaded into the 'bronze' schema successfully.")
