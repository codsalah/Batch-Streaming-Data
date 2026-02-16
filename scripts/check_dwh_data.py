#!/usr/bin/env python3

import os
from dotenv import load_dotenv
import psycopg2

# =========================
# Load .env
# =========================
load_dotenv('../.env')

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "earthquake_dwh")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

# =========================
# Connect to PostgreSQL
# =========================
def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"Connection error: {e}")
        exit(1)

# =========================
# List all tables
# =========================
def list_tables(conn):
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema='public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        cur.close()
        return [t[0] for t in tables]
    except Exception as e:
        print(f"Error fetching tables: {e}")
        return []

# =========================
# Count rows for each table
# =========================
def count_rows(conn, table_name):
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table_name};")
        count = cur.fetchone()[0]
        cur.close()
        return count
    except Exception as e:
        print(f"Could not count rows for {table_name}: {e}")
        return None

# =========================
# Main
# =========================
def main():
    print("Checking DWH tables and row counts...\n")
    conn = connect_db()
    tables = list_tables(conn)

    if not tables:
        print("No tables found in the database.")
        conn.close()
        return

    print(f"Found {len(tables)} tables:\n")
    for table in tables:
        count = count_rows(conn, table)
        count_display = count if count is not None else "N/A"
        print(f" - {table}: {count_display} rows")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
