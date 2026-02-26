#!/bin/sh
# Wait for Cassandra to accept CQL connections, then run seed data
echo "Waiting for Cassandra..."
until cqlsh -e "DESCRIBE KEYSPACES" 2>/dev/null; do
  sleep 5
done
echo "Cassandra ready, loading seed data..."
cqlsh -f /docker-entrypoint-initdb.d/init.cql
echo "Cassandra seed data loaded"
