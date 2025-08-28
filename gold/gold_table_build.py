import psycopg2

# -------------------------------
# Connect to PostgreSQL
# -------------------------------
connection = psycopg2.connect(
    dbname="mydb",                # database name
    user="keerthana.s",           # your username
    password="MyStrongPassword123",  # your password
    host="localhost",
    port="5432"
)
connection.autocommit = True
cursor = connection.cursor()
print("✅ Connected to PostgreSQL successfully!")


# -------------------------------
# Helper function to run SQL
# -------------------------------
def run_sql(query: str):
    try:
        cursor.execute(query)
        print("✅ Query executed successfully!")
    except Exception as e:
        print(f"⚠️ Error running query: {e}")


# -------------------------------
# Build Gold Schema & Tables
# -------------------------------
def build_gold():
    # Create gold schema
    run_sql("CREATE SCHEMA IF NOT EXISTS gold;")

    # 1. Total enrollments per course
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.enrollments_per_course AS
    SELECT 
        c.course_title,
        COUNT(e.enrollment_id) AS total_enrollments
    FROM silver.courses c
    LEFT JOIN silver.enrollments e ON c.course_id = e.course_id
    GROUP BY c.course_title
    ORDER BY total_enrollments DESC;
    """)

    # 2. Revenue per course
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.revenue_per_course AS
    SELECT
        c.course_title,
        SUM(p.amount) AS total_revenue
    FROM silver.courses c
    JOIN silver.payments p ON c.course_id = p.course_id
    WHERE p.status = 'completed'
    GROUP BY c.course_title
    ORDER BY total_revenue DESC;
    """)

    # 3. Enrollments per instructor
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.enrollments_per_instructor AS
    SELECT
        i.name AS instructor_name,
        i.expertise_area,
        COUNT(e.enrollment_id) AS total_enrollments
    FROM silver.instructors i
    LEFT JOIN silver.courses c ON i.instructor_id = c.instructor_id
    LEFT JOIN silver.enrollments e ON c.course_id = e.course_id
    GROUP BY i.name, i.expertise_area
    ORDER BY total_enrollments DESC;
    """)

    # 4. Dashboard summary
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.dashboard_table AS
    SELECT
        COUNT(DISTINCT s.student_id) AS total_students,
        COUNT(DISTINCT i.instructor_id) AS total_instructors,
        COUNT(DISTINCT c.course_id) AS total_courses,
        COUNT(DISTINCT e.enrollment_id) AS total_enrollments,
        SUM(p.amount) AS total_revenue
    FROM silver.students s
    LEFT JOIN silver.enrollments e ON s.student_id = e.student_id
    LEFT JOIN silver.courses c ON e.course_id = c.course_id
    LEFT JOIN silver.instructors i ON c.instructor_id = i.instructor_id
    LEFT JOIN silver.payments p ON e.course_id = p.course_id;
    """)

    # 5. Student activity per course
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.student_activity_summary AS
    SELECT
        s.student_id,
        s.name AS student_name,
        c.course_title,
        SUM(a.video_watched_min) AS total_minutes_watched,
        AVG(a.quiz_score) AS avg_quiz_score,
        AVG(a.assignment_score) AS avg_assignment_score
    FROM silver.students s
    JOIN silver.activity a ON s.student_id = a.student_id
    JOIN silver.courses c ON a.course_id = c.course_id
    GROUP BY s.student_id, s.name, c.course_title;
    """)

    # 6. Top courses by enrollments
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.top_courses AS
    SELECT 
        c.course_id,
        c.course_title,
        COUNT(e.enrollment_id) AS total_enrollments
    FROM silver.courses c
    JOIN silver.enrollments e ON c.course_id = e.course_id
    GROUP BY c.course_id, c.course_title
    ORDER BY total_enrollments DESC
    LIMIT 5;
    """)

    # 7. Revenue by country
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.revenue_by_country AS
    SELECT
        s.country,
        SUM(p.amount) AS total_revenue
    FROM silver.students s
    JOIN silver.payments p ON s.student_id = p.student_id
    WHERE p.status = 'completed'
    GROUP BY s.country
    ORDER BY total_revenue DESC;
    """)

    # 8. Yearly enrollments
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.yearly_enrollments AS
    SELECT
        EXTRACT(YEAR FROM enrollment_date) AS year,
        COUNT(*) AS total_enrollments
    FROM silver.enrollments
    GROUP BY year
    ORDER BY year;
    """)

    # 9. Top instructors by revenue
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.top_instructors AS
    SELECT
        i.instructor_id,
        i.name AS instructor_name,
        SUM(p.amount) AS total_revenue
    FROM silver.instructors i
    JOIN silver.courses c ON i.instructor_id = c.instructor_id
    JOIN silver.payments p ON c.course_id = p.course_id
    WHERE p.status = 'completed'
    GROUP BY i.instructor_id, i.name
    ORDER BY total_revenue DESC
    LIMIT 5;
    """)

    # 10. Course difficulty distribution
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.course_difficulty_distribution AS
    SELECT
        difficulty_level,
        COUNT(*) AS total_courses
    FROM silver.courses
    GROUP BY difficulty_level
    ORDER BY total_courses DESC;
    """)

    print("🎉 Gold tables created successfully!")


# -------------------------------
# Run the build_gold function
# -------------------------------
if __name__ == "__main__":
    build_gold()
    cursor.close()
    connection.close()
    print("🔒 PostgreSQL connection closed.")
