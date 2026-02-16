#!/bin/bash
set -e
 
APP_DIR="/app"
DDL_FILE="$APP_DIR/dwh/ddl/create_tables.sql"

DB_HOST="${DB_HOST:-postgres}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-mydb}"
DB_USER="${DB_USER:-user}"
DB_PASSWORD="${DB_PASSWORD:-password}"

echo "Creating DWH tables inside container..."

if ! command -v psql &> /dev/null
then
    echo "psql command not found. Make sure postgresql-client is installed in this container."
    exit 1
fi

export PGPASSWORD="$DB_PASSWORD"

psql -h "$DB_HOST" \
     -p "$DB_PORT" \
     -U "$DB_USER" \
     -d "$DB_NAME" \
     -f "$DDL_FILE"

echo "Tables created successfully!"
