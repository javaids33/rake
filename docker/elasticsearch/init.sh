#!/bin/sh
# RustLake Demo: Elasticsearch seed data
# Wait for ES to be ready, then load product catalog and logs

echo "Waiting for Elasticsearch..."
until curl -s http://elasticsearch:9200/_cluster/health | grep -q '"status"'; do
  sleep 2
done
echo "Elasticsearch ready, loading seed data..."

# Create products index with mappings
curl -s -X PUT "http://elasticsearch:9200/products" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "name": { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "description": { "type": "text" },
      "category": { "type": "keyword" },
      "price": { "type": "float" },
      "stock": { "type": "integer" },
      "rating": { "type": "float" },
      "created_at": { "type": "date" }
    }
  }
}'

# Bulk load products
curl -s -X POST "http://elasticsearch:9200/products/_bulk" -H 'Content-Type: application/x-ndjson' -d '
{"index":{"_id":"1"}}
{"name":"Wireless Headphones","description":"Noise-cancelling Bluetooth headphones with 30h battery","category":"Electronics","price":79.99,"stock":150,"rating":4.5,"created_at":"2024-01-15"}
{"index":{"_id":"2"}}
{"name":"Mechanical Keyboard","description":"Cherry MX Brown switches, RGB backlit, hot-swappable","category":"Electronics","price":129.99,"stock":75,"rating":4.7,"created_at":"2024-02-01"}
{"index":{"_id":"3"}}
{"name":"Standing Desk","description":"Electric height-adjustable desk, 60x30 bamboo top","category":"Furniture","price":399.99,"stock":30,"rating":4.3,"created_at":"2024-02-15"}
{"index":{"_id":"4"}}
{"name":"Monitor Light Bar","description":"LED screen light bar with auto-dimming sensor","category":"Electronics","price":49.99,"stock":200,"rating":4.6,"created_at":"2024-03-01"}
{"index":{"_id":"5"}}
{"name":"Ergonomic Chair","description":"Mesh back, adjustable lumbar, 4D armrests","category":"Furniture","price":549.99,"stock":25,"rating":4.8,"created_at":"2024-03-15"}
'

# Create application logs index
curl -s -X PUT "http://elasticsearch:9200/app-logs" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "timestamp": { "type": "date" },
      "level": { "type": "keyword" },
      "service": { "type": "keyword" },
      "message": { "type": "text" },
      "duration_ms": { "type": "integer" },
      "status_code": { "type": "integer" }
    }
  }
}'

curl -s -X POST "http://elasticsearch:9200/app-logs/_bulk" -H 'Content-Type: application/x-ndjson' -d '
{"index":{}}
{"timestamp":"2024-06-01T10:00:00Z","level":"INFO","service":"api-gateway","message":"GET /api/v1/products 200","duration_ms":12,"status_code":200}
{"index":{}}
{"timestamp":"2024-06-01T10:00:05Z","level":"INFO","service":"api-gateway","message":"POST /api/v1/orders 201","duration_ms":45,"status_code":201}
{"index":{}}
{"timestamp":"2024-06-01T10:00:10Z","level":"WARN","service":"payment","message":"Payment retry attempt 2 for order 1042","duration_ms":3200,"status_code":504}
{"index":{}}
{"timestamp":"2024-06-01T10:00:15Z","level":"ERROR","service":"inventory","message":"Stock check failed for product 3: connection timeout","duration_ms":5000,"status_code":500}
{"index":{}}
{"timestamp":"2024-06-01T10:00:20Z","level":"INFO","service":"search","message":"Full-text search: mechanical keyboard, 23 results","duration_ms":8,"status_code":200}
'

echo ""
echo "Elasticsearch seed data loaded: products (5 docs), app-logs (5 docs)"
