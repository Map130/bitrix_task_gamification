#!/bin/sh
# entrypoint.sh
# Waits for PostgreSQL to be ready before executing the main command.

set -e

# These variables are expected to be set in the docker-compose.yml environment.
: "${POSTGRES_HOST?Need to set POSTGRES_HOST}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_USER?Need to set POSTGRES_USER}"
: "${POSTGRES_PASSWORD?Need to set POSTGRES_PASSWORD}"
: "${POSTGRES_DB?Need to set POSTGRES_DB}"

TIMEOUT=60
ELAPSED=0

echo "Waiting for database at $POSTGRES_HOST:$POSTGRES_PORT..."

# Set PGPASSWORD so pg_isready doesn't prompt for a password.
export PGPASSWORD="$POSTGRES_PASSWORD"

# Loop until pg_isready returns a success code.
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -q; do
  ELAPSED=$((ELAPSED + 1))
  if [ "$ELAPSED" -gt "$TIMEOUT" ]; then
    echo "Timeout: Database at $POSTGRES_HOST:$POSTGRES_PORT was not ready within $TIMEOUT seconds." >&2
    exit 1
  fi
  echo "Database is unavailable - sleeping for 1 second..."
  sleep 1
done

# Unset the password variable for security.
unset PGPASSWORD

echo "Database is ready. Executing command: $@"
# Execute the command passed as arguments to this script (e.g., uvicorn...).
exec "$@"
