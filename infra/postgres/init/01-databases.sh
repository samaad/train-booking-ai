#!/usr/bin/env bash
# One Postgres container, one database + one owning role per service. No cross-service SQL:
# each service only ever receives its own credentials.
# Runs once, on first start with an empty volume (`docker compose down -v` to re-run).
set -euo pipefail

DATABASES=(inventory booking payment ticketing profile vectordb)

for db in "${DATABASES[@]}"; do
  echo "Creating database '$db' with owner role '$db'"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<-SQL
    CREATE ROLE "$db" LOGIN PASSWORD '$db';
    CREATE DATABASE "$db" OWNER "$db";
    REVOKE ALL ON DATABASE "$db" FROM PUBLIC;
SQL
done

# pgvector for ai-assistant RAG. CREATE EXTENSION needs superuser, so it happens here.
# Embeddings are mxbai-embed-large → vector(1024).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname vectordb <<-SQL
  CREATE EXTENSION IF NOT EXISTS vector;
SQL
