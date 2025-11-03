#!/bin/bash
set -e

echo "Starting PostgreSQL..."
docker-compose up -d db

echo "Waiting for database to be ready..."

until docker-compose exec -T db pg_isready -U transit -d transit >/dev/null 2>&1; do
  sleep 1
done

echo "Loading data..."
docker-compose run --rm app python load_data.py \
  --host db --dbname transit --user transit --password transit123 --datadir /app/data

echo ""
echo "Running sample queries..."
docker-compose run --rm app python queries.py --host db --query Q1 --dbname transit --user transit --password transit123
docker-compose run --rm app python queries.py --host db --query Q3 --dbname transit --user transit --password transit123
