# Resolve paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables from .env if it exists
if [ -f "$PROJECT_ROOT/.env" ]; then
    echo "Loading environment variables from $PROJECT_ROOT/.env"
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

DB_HOST="${DB_HOST:-postgres}"
DB_PORT="${POSTGRES_PORT:-${DB_PORT:-5432}}"
DB_NAME="${POSTGRES_DB:-${DB_NAME:-mydb}}"
DB_USER="${POSTGRES_USER:-${DB_USER:-user}}"
DB_PASSWORD="${POSTGRES_PASSWORD:-${DB_PASSWORD:-password}}"

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
