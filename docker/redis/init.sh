#!/bin/sh
# RustLake Demo: Redis seed data
# Populates product catalog, customer sessions, and a sample stream

redis-cli -h localhost <<'EOF'
# Product catalog (Hash)
HSET product:1 name "Wireless Headphones" price 79.99 category "Electronics" stock 150
HSET product:2 name "Mechanical Keyboard" price 129.99 category "Electronics" stock 75
HSET product:3 name "Standing Desk" price 399.99 category "Furniture" stock 30
HSET product:4 name "Monitor Light Bar" price 49.99 category "Electronics" stock 200
HSET product:5 name "Ergonomic Chair" price 549.99 category "Furniture" stock 25

# Customer sessions (Hash with TTL)
HSET session:user:1 user_id 1 name "James Chen" cart_items 3 last_active "2024-06-01T10:30:00Z"
HSET session:user:2 user_id 2 name "Maria Garcia" cart_items 1 last_active "2024-06-01T11:15:00Z"
HSET session:user:3 user_id 3 name "David Kim" cart_items 0 last_active "2024-06-01T09:45:00Z"

# Leaderboard (Sorted Set)
ZADD top_customers 1250.50 "James Chen" 890.25 "Maria Garcia" 567.00 "David Kim" 2100.75 "Emily Davis" 445.00 "Sarah Johnson"

# Event stream (Redis Stream)
XADD events * type page_view user_id 1 page /products timestamp "2024-06-01T10:00:00Z"
XADD events * type add_to_cart user_id 1 product_id 2 timestamp "2024-06-01T10:05:00Z"
XADD events * type purchase user_id 2 product_id 1 amount 79.99 timestamp "2024-06-01T10:10:00Z"
XADD events * type page_view user_id 3 page /checkout timestamp "2024-06-01T10:15:00Z"
XADD events * type search user_id 1 query "mechanical keyboard" timestamp "2024-06-01T10:20:00Z"

# Config cache (String)
SET config:feature_flags '{"dark_mode":true,"beta_search":false,"new_checkout":true}'
SET config:rate_limit "1000"

EOF
echo "Redis seed data loaded"
