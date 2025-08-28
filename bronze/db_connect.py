import psycopg2

# connection details
connection = psycopg2.connect(
    dbname="mydb",          # your database name
    user="keerthana.s",      # your username
    password="MyStrongPassword123",  # replace with your actual password
    host="localhost",       # since DB is on your machine
    port="5432"             # default PostgreSQL port
)

print("✅ Connected to PostgreSQL successfully!")

connection.close()