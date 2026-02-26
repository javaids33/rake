-- RustLake Demo: ClickHouse seed data
-- Analytics-optimized tables for OLAP testing

CREATE DATABASE IF NOT EXISTS rustlake_demo;

-- Page events (append-only, time-series pattern)
CREATE TABLE rustlake_demo.page_events (
    event_id UUID DEFAULT generateUUIDv4(),
    user_id UInt32,
    event_type String,
    page String,
    referrer String DEFAULT '',
    duration_ms UInt32 DEFAULT 0,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id);

INSERT INTO rustlake_demo.page_events (user_id, event_type, page, referrer, duration_ms, timestamp) VALUES
(1, 'page_view', '/products', 'google.com', 4500, '2024-06-01 10:00:00'),
(1, 'page_view', '/products/keyboard', '/products', 12000, '2024-06-01 10:01:00'),
(2, 'page_view', '/home', 'direct', 3200, '2024-06-01 10:02:00'),
(1, 'add_to_cart', '/products/keyboard', '', 800, '2024-06-01 10:03:00'),
(3, 'page_view', '/pricing', 'twitter.com', 6700, '2024-06-01 10:04:00'),
(2, 'search', '/search?q=headphones', '/home', 2100, '2024-06-01 10:05:00'),
(1, 'purchase', '/checkout', '/cart', 15000, '2024-06-01 10:06:00'),
(4, 'page_view', '/home', 'linkedin.com', 5400, '2024-06-01 10:07:00'),
(2, 'add_to_cart', '/products/headphones', '/search', 600, '2024-06-01 10:08:00'),
(5, 'page_view', '/docs', 'google.com', 25000, '2024-06-01 10:09:00');

-- Aggregated daily metrics (materialized-view pattern)
CREATE TABLE rustlake_demo.daily_metrics (
    date Date,
    total_events UInt64,
    unique_users UInt32,
    total_revenue Decimal(12,2),
    avg_session_ms UInt32
) ENGINE = MergeTree()
ORDER BY date;

INSERT INTO rustlake_demo.daily_metrics VALUES
('2024-05-25', 12450, 342, 8945.50, 4200),
('2024-05-26', 11230, 298, 7120.25, 3800),
('2024-05-27', 13670, 401, 10250.00, 4500),
('2024-05-28', 14890, 425, 11340.75, 4100),
('2024-05-29', 15230, 448, 12100.50, 4300),
('2024-05-30', 13100, 380, 9870.00, 3900),
('2024-05-31', 11980, 315, 8450.25, 3700),
('2024-06-01', 16200, 502, 13450.00, 4600);
