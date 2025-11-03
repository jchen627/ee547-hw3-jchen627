#!/bin/bash
set -e
./build.sh
./run.sh

echo ""
echo "Testing all queries..."
for i in {1..10}; do
  docker-compose run --rm app python queries.py --host db --query Q$i --dbname transit --user transit --password transit123 --format json
done

docker-compose down
