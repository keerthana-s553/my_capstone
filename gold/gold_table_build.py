import psycopg2

# -------------------------------
# Connect to PostgreSQL
# -------------------------------
connection = psycopg2.connect(
    dbname="mydb",
    user="keerthana.s",
    password="MyStrongPassword123",
    host="localhost",
    port="5432"
)
connection.autocommit = True
cursor = connection.cursor()
print("✅ Connected to PostgreSQL successfully!")

# -------------------------------
# Helper function
# -------------------------------
def run_sql(query: str):
    try:
        cursor.execute(query)
        print("✅ Query executed successfully!")
    except Exception as e:
        print(f"⚠️ Error running query: {e}")

# -------------------------------
# Build Gold Tables
# -------------------------------
def build_gold_tables():
    # Ensure schema exists
    run_sql("CREATE SCHEMA IF NOT EXISTS gold;")

    # Drop existing Gold tables
    run_sql("""
    DO $$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN (SELECT table_name FROM information_schema.tables WHERE table_schema = 'gold') LOOP
            EXECUTE 'DROP TABLE IF EXISTS gold.' || quote_ident(r.table_name) || ' CASCADE';
        END LOOP;
    END $$;
    """)
    print("🗑 Dropped existing Gold tables.")

    # -------------------------------
    # 1️⃣ Gold metrics
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.gold_metrics AS
    SELECT
        (SELECT COUNT(DISTINCT student_id) FROM silver.students) AS total_students,
        (SELECT COUNT(DISTINCT student_id) FROM silver.enrollments) AS total_enrolled_students,
        (SELECT COUNT(DISTINCT instructor_id) FROM silver.instructors) AS total_instructors,
        (SELECT COUNT(DISTINCT instructor_id) FROM silver.courses) AS active_instructors,
        (SELECT COUNT(DISTINCT course_id) FROM silver.courses) AS total_courses,
        (SELECT SUM(amount) FROM silver.payments) AS total_revenue,
        (SELECT ROUND(SUM(amount)::NUMERIC / NULLIF(COUNT(DISTINCT student_id),0),2) FROM silver.payments) AS revenue_per_student;
    """)

    # -------------------------------
    # 2️⃣ Enrollments by year and month
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.enrollments_by_period AS
    SELECT
        EXTRACT(YEAR FROM enrollment_date)::INT AS year,
        EXTRACT(MONTH FROM enrollment_date)::INT AS month,
        COUNT(*) AS total_enrollments
    FROM silver.enrollments
    GROUP BY year, month
    ORDER BY year, month;
    """)

    # -------------------------------
    # 3️⃣ Students enrolled by country
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.students_enrolled_by_country AS
    SELECT 
        s.country,
        COUNT(DISTINCT e.student_id) AS total_enrolled_students
    FROM silver.students s
    JOIN silver.enrollments e ON s.student_id = e.student_id
    GROUP BY s.country
    ORDER BY total_enrolled_students DESC;
    """)

    # -------------------------------
    # 4️⃣ Enrolled students by age group
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.enrolled_students_by_age_group AS
    SELECT 
        CASE
            WHEN s.age BETWEEN 0 AND 18 THEN '0-18'
            WHEN s.age BETWEEN 19 AND 25 THEN '19-25'
            WHEN s.age BETWEEN 26 AND 40 THEN '26-40'
            ELSE '40+'
        END AS age_group,
        COUNT(DISTINCT e.student_id) AS total_enrolled_students
    FROM silver.students s
    JOIN silver.enrollments e ON s.student_id = e.student_id
    GROUP BY age_group
    ORDER BY age_group;
    """)

    # -------------------------------
    # 5️⃣ Courses by revenue (per category) - fixed
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.courses_by_revenue AS
    SELECT 
        c.category,
        c.course_id,
        c.course_title,
        COALESCE(SUM(p.amount), 0) AS total_revenue
    FROM silver.courses c
    LEFT JOIN silver.payments p 
        ON c.course_id = p.course_id
    GROUP BY c.category, c.course_id, c.course_title
    ORDER BY c.category, total_revenue DESC;
    """)

    # -------------------------------
    # 6️⃣ Courses by enrollment (per category)
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.courses_by_enrollment AS
    SELECT 
        c.category,
        c.course_id,
        c.course_title,
        COUNT(e.enrollment_id) AS total_enrollments
    FROM silver.courses c
    LEFT JOIN silver.enrollments e 
        ON c.course_id = e.course_id
    GROUP BY c.category, c.course_id, c.course_title
    ORDER BY c.category, total_enrollments DESC;
    """)

    # -------------------------------
    # 7️⃣ Top instructors by enrollment (per category)
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.top_instructors_by_enrollment AS
    SELECT
        c.category,
        i.instructor_id,
        i.name AS instructor_name,
        COUNT(e.enrollment_id) AS total_enrollments
    FROM silver.instructors i
    JOIN silver.courses c ON i.instructor_id = c.instructor_id
    LEFT JOIN silver.enrollments e ON c.course_id = e.course_id
    GROUP BY c.category, i.instructor_id, i.name
    ORDER BY c.category, total_enrollments DESC;
    """)

    print("🎉 Gold tables created successfully!")

# -------------------------------
# Run the script
# -------------------------------
if __name__ == "__main__":
    build_gold_tables()
    cursor.close()
    connection.close()
    print("🔒 PostgreSQL connection closed.")
