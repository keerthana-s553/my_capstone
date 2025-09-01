import psycopg2

# -------------------------------
# Utility function to run SQL queries safely
# -------------------------------
def run_sql(query):
    try:
        conn = psycopg2.connect(
            dbname="mydb",
            user="keerthana.s",
            password="MyStrongPassword123",
            host="localhost",
            port="5432"
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(query)
        cur.close()
        conn.close()
        print("✅ Query executed successfully.")
    except Exception as e:
        print(f"❌ Error: {e}")

# -------------------------------
# Build Gold Layer (without dropping existing tables)
# -------------------------------
def build_gold():
    # Create schema if not exists
    run_sql("CREATE SCHEMA IF NOT EXISTS gold;")

    # -------------------------------
    # Payments per course
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.payments_per_course AS
    SELECT
        c.course_id,
        c.course_title,
        SUM(p.amount) AS total_revenue,
        COUNT(DISTINCT p.student_id) AS total_students
    FROM silver.payments p
    JOIN silver.courses c ON p.course_id = c.course_id
    GROUP BY c.course_id, c.course_title
    ORDER BY total_revenue DESC;
    """)

    # -------------------------------
    # Enrollments per course
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.enrollments_per_course AS
    SELECT
        c.course_id,
        c.course_title,
        COUNT(e.enrollment_id) AS total_enrollments,
        AVG(e.progress_percent) AS avg_progress
    FROM silver.enrollments e
    JOIN silver.courses c ON e.course_id = c.course_id
    GROUP BY c.course_id, c.course_title
    ORDER BY total_enrollments DESC;
    """)

    # -------------------------------
    # Student activity summary
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.student_activity_summary AS
    SELECT
        s.student_id,
        s.name AS student_name,
        SUM(a.video_watched_min) AS total_video_minutes,
        AVG(a.quiz_score) AS avg_quiz_score,
        AVG(a.assignment_score) AS avg_assignment_score
    FROM silver.activity a
    JOIN silver.students s ON a.student_id = s.student_id
    GROUP BY s.student_id, s.name
    ORDER BY total_video_minutes DESC;
    """)

    # -------------------------------
    # Instructor performance
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.instructor_performance AS
    SELECT
        i.instructor_id,
        i.name AS instructor_name,
        COUNT(DISTINCT e.student_id) AS total_students,
        SUM(p.amount) AS total_revenue
    FROM silver.courses c
    JOIN silver.instructors i ON c.instructor_id = i.instructor_id
    LEFT JOIN silver.enrollments e ON c.course_id = e.course_id
    LEFT JOIN silver.payments p ON c.course_id = p.course_id
    GROUP BY i.instructor_id, i.name
    ORDER BY total_revenue DESC;
    """)

    # -------------------------------
    # Student dashboard
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.student_dashboard AS
    SELECT
        s.student_id,
        s.name AS student_name,
        SUM(p.amount) AS total_paid,
        COUNT(DISTINCT e.course_id) AS courses_enrolled,
        AVG(a.quiz_score) AS avg_quiz_score,
        AVG(a.assignment_score) AS avg_assignment_score,
        SUM(a.video_watched_min) AS total_video_minutes
    FROM silver.students s
    LEFT JOIN silver.payments p ON s.student_id = p.student_id
    LEFT JOIN silver.enrollments e ON s.student_id = e.student_id
    LEFT JOIN silver.activity a ON s.student_id = a.student_id
    GROUP BY s.student_id, s.name;
    """)

    # -------------------------------
    # Top 5 courses by revenue
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.top5_courses_by_revenue AS
    SELECT
        c.course_id,
        c.course_title,
        SUM(p.amount) AS total_revenue
    FROM silver.courses c
    JOIN silver.payments p ON c.course_id = p.course_id
    GROUP BY c.course_id, c.course_title
    ORDER BY total_revenue DESC
    LIMIT 5;
    """)

    # -------------------------------
    # Student age distribution
    # -------------------------------
    run_sql("""
    CREATE TABLE IF NOT EXISTS gold.age_distribution AS
    SELECT 
        CASE
            WHEN age BETWEEN 0 AND 18 THEN '0-18'
            WHEN age BETWEEN 19 AND 25 THEN '19-25'
            WHEN age BETWEEN 26 AND 40 THEN '26-40'
            ELSE '40+'
        END AS age_group,
        COUNT(*) AS total_students
    FROM silver.students
    GROUP BY age_group
    ORDER BY age_group;
    """)

    print("✅ Gold layer for silver schema built successfully (existing tables preserved).")

# -------------------------------
# Run the build process
# -------------------------------
if __name__ == "__main__":
    build_gold()
