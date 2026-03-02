-- RustLake Demo: MySQL seed data
-- Mirrors the CSV files in sample-data/ for cross-source query demos.

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(50) NOT NULL,
    country VARCHAR(10) NOT NULL DEFAULT 'US',
    signup_date DATE NOT NULL,
    tier VARCHAR(20) NOT NULL
);

INSERT INTO customers (customer_id, name, email, city, state, country, signup_date, tier) VALUES
(1, 'James Chen', 'james.chen@gmail.com', 'San Francisco', 'CA', 'US', '2023-01-15', 'enterprise'),
(2, 'Maria Garcia', 'maria.garcia@outlook.com', 'Austin', 'TX', 'US', '2023-02-08', 'pro'),
(3, 'David Kim', 'david.kim@yahoo.com', 'Seattle', 'WA', 'US', '2023-02-22', 'free'),
(4, 'Sarah Johnson', 'sarah.j@gmail.com', 'New York', 'NY', 'US', '2023-03-10', 'pro'),
(5, 'Michael Brown', 'm.brown@protonmail.com', 'Chicago', 'IL', 'US', '2023-03-28', 'free'),
(6, 'Emily Davis', 'emily.davis@gmail.com', 'Portland', 'OR', 'US', '2023-04-05', 'enterprise'),
(7, 'Robert Wilson', 'rwilson@outlook.com', 'Denver', 'CO', 'US', '2023-04-19', 'free'),
(8, 'Jennifer Lee', 'jlee@gmail.com', 'Los Angeles', 'CA', 'US', '2023-05-01', 'pro'),
(9, 'William Martinez', 'w.martinez@yahoo.com', 'Miami', 'FL', 'US', '2023-05-17', 'free'),
(10, 'Amanda Taylor', 'amanda.t@gmail.com', 'Boston', 'MA', 'US', '2023-06-02', 'pro');

CREATE TABLE products (
    product_id INT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2) NOT NULL,
    stock_qty INT NOT NULL
);

INSERT INTO products (product_id, name, category, price, cost, stock_qty) VALUES
(1, 'Wireless Noise-Canceling Headphones', 'Electronics', 149.99, 62.00, 340),
(2, 'USB-C Charging Hub 7-Port', 'Electronics', 39.99, 14.50, 520),
(3, '4K Webcam with Ring Light', 'Electronics', 89.99, 35.00, 185),
(4, 'Mechanical Keyboard RGB', 'Electronics', 129.99, 48.00, 275),
(5, 'Portable Bluetooth Speaker', 'Electronics', 59.99, 22.00, 410),
(6, 'Merino Wool Crew Neck Sweater', 'Clothing', 78.00, 28.00, 190),
(7, 'Waterproof Hiking Jacket', 'Clothing', 135.00, 52.00, 145),
(8, 'Organic Cotton T-Shirt Pack (3)', 'Clothing', 34.99, 11.00, 680),
(9, 'Slim Fit Chino Pants', 'Clothing', 54.99, 19.00, 310),
(10, 'Running Shoes Ultralight', 'Clothing', 119.99, 45.00, 225);

CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    order_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

INSERT INTO orders (order_id, customer_id, product_id, quantity, total_amount, order_date, status, payment_method) VALUES
(1001, 1, 1, 1, 149.99, '2024-01-05', 'completed', 'credit_card'),
(1002, 4, 8, 2, 69.98, '2024-01-08', 'completed', 'paypal'),
(1003, 2, 3, 1, 89.99, '2024-01-12', 'completed', 'debit_card'),
(1004, 6, 4, 2, 259.98, '2024-01-15', 'completed', 'credit_card'),
(1005, 3, 5, 1, 59.99, '2024-01-18', 'completed', 'apple_pay'),
(1006, 1, 4, 1, 129.99, '2024-01-22', 'completed', 'credit_card'),
(1007, 8, 6, 1, 78.00, '2024-01-25', 'completed', 'paypal'),
(1008, 10, 9, 1, 54.99, '2024-01-28', 'completed', 'credit_card'),
(1009, 5, 2, 3, 119.97, '2024-02-01', 'completed', 'debit_card'),
(1010, 2, 1, 1, 149.99, '2024-02-04', 'completed', 'credit_card');

CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_product ON orders(product_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_customers_tier ON customers(tier);
CREATE INDEX idx_products_category ON products(category);

-- ============================================================================
-- TPC-H tables (small scale for testing)
-- ============================================================================

CREATE TABLE tpch_region (
    r_regionkey INT PRIMARY KEY,
    r_name VARCHAR(25) NOT NULL,
    r_comment VARCHAR(152)
);

INSERT INTO tpch_region VALUES
(0, 'AFRICA', 'Vast continent with diverse economies'),
(1, 'AMERICA', 'North and South American markets'),
(2, 'ASIA', 'Fast-growing Asian economies'),
(3, 'EUROPE', 'Mature European markets'),
(4, 'MIDDLE EAST', 'Oil-rich Middle Eastern economies');

CREATE TABLE tpch_nation (
    n_nationkey INT PRIMARY KEY,
    n_name VARCHAR(25) NOT NULL,
    n_regionkey INT NOT NULL,
    n_comment VARCHAR(152),
    FOREIGN KEY (n_regionkey) REFERENCES tpch_region(r_regionkey)
);

INSERT INTO tpch_nation VALUES
(0, 'ALGERIA', 0, 'North African nation'),
(1, 'ARGENTINA', 1, 'South American nation'),
(2, 'BRAZIL', 1, 'Largest South American economy'),
(3, 'CANADA', 1, 'North American nation'),
(4, 'EGYPT', 4, 'North African and Middle Eastern'),
(5, 'ETHIOPIA', 0, 'East African nation'),
(6, 'FRANCE', 3, 'Western European nation'),
(7, 'GERMANY', 3, 'Central European economy'),
(8, 'INDIA', 2, 'South Asian economy'),
(9, 'INDONESIA', 2, 'Southeast Asian archipelago'),
(10, 'IRAN', 4, 'Middle Eastern nation'),
(11, 'IRAQ', 4, 'Middle Eastern nation'),
(12, 'JAPAN', 2, 'East Asian economy'),
(13, 'JORDAN', 4, 'Middle Eastern kingdom'),
(14, 'KENYA', 0, 'East African nation'),
(15, 'MOROCCO', 0, 'North African nation'),
(16, 'MOZAMBIQUE', 0, 'Southeast African nation'),
(17, 'PERU', 1, 'South American nation'),
(18, 'CHINA', 2, 'East Asian superpower'),
(19, 'ROMANIA', 3, 'Eastern European nation'),
(20, 'SAUDI ARABIA', 4, 'Middle Eastern kingdom'),
(21, 'VIETNAM', 2, 'Southeast Asian nation'),
(22, 'RUSSIA', 3, 'Eurasian nation'),
(23, 'UNITED KINGDOM', 3, 'Western European island'),
(24, 'UNITED STATES', 1, 'North American superpower');

CREATE TABLE tpch_customer (
    c_custkey INT PRIMARY KEY,
    c_name VARCHAR(25) NOT NULL,
    c_address VARCHAR(40) NOT NULL,
    c_nationkey INT NOT NULL,
    c_phone VARCHAR(25) NOT NULL,
    c_acctbal DECIMAL(12,2) NOT NULL,
    c_mktsegment VARCHAR(10) NOT NULL,
    c_comment VARCHAR(117),
    FOREIGN KEY (c_nationkey) REFERENCES tpch_nation(n_nationkey)
);

-- Use a numbers table approach to generate 150 customers (no stored procedures)
CREATE TABLE _nums (n INT PRIMARY KEY);
INSERT INTO _nums (n) VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),(41),(42),(43),(44),(45),(46),(47),(48),(49),(50),(51),(52),(53),(54),(55),(56),(57),(58),(59),(60),(61),(62),(63),(64),(65),(66),(67),(68),(69),(70),(71),(72),(73),(74),(75),(76),(77),(78),(79),(80),(81),(82),(83),(84),(85),(86),(87),(88),(89),(90),(91),(92),(93),(94),(95),(96),(97),(98),(99),(100),(101),(102),(103),(104),(105),(106),(107),(108),(109),(110),(111),(112),(113),(114),(115),(116),(117),(118),(119),(120),(121),(122),(123),(124),(125),(126),(127),(128),(129),(130),(131),(132),(133),(134),(135),(136),(137),(138),(139),(140),(141),(142),(143),(144),(145),(146),(147),(148),(149),(150);

INSERT INTO tpch_customer
SELECT
    n,
    CONCAT('Customer#', LPAD(n, 6, '0')),
    CONCAT((n * 7) % 999, ' Main St, City ', n % 50),
    n % 25,
    CONCAT(LPAD(10 + (n % 25), 2, '0'), '-', LPAD(100 + (n * 3 % 900), 3, '0'), '-', LPAD(1000 + (n * 7 % 9000), 4, '0'), '-', LPAD(1000 + (n * 11 % 9000), 4, '0')),
    ROUND(-999.99 + (n * 73.17 % 10998.98), 2),
    ELT(1 + (n % 5), 'AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY'),
    CONCAT('Comment for customer ', n)
FROM _nums WHERE n <= 150;

CREATE TABLE tpch_orders (
    o_orderkey INT PRIMARY KEY,
    o_custkey INT NOT NULL,
    o_orderstatus VARCHAR(1) NOT NULL,
    o_totalprice DECIMAL(12,2) NOT NULL,
    o_orderdate DATE NOT NULL,
    o_orderpriority VARCHAR(15) NOT NULL,
    o_clerk VARCHAR(15) NOT NULL,
    o_shippriority INT NOT NULL,
    o_comment VARCHAR(79),
    FOREIGN KEY (o_custkey) REFERENCES tpch_customer(c_custkey)
);

-- Expand numbers to 1500 via cross join
CREATE TABLE _nums10 (n INT PRIMARY KEY);
INSERT INTO _nums10 VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

INSERT INTO tpch_orders
SELECT
    a.n * 10 + b.n + 1 AS orderkey,
    1 + ((a.n * 10 + b.n + 1) % 150),
    ELT(1 + ((a.n * 10 + b.n + 1) % 3), 'O', 'F', 'P'),
    ROUND(1000.00 + ((a.n * 10 + b.n + 1) * 37.89 % 299000.00), 2),
    DATE_ADD('1992-01-01', INTERVAL ((a.n * 10 + b.n + 1) * 3 % 2556) DAY),
    ELT(1 + ((a.n * 10 + b.n + 1) % 5), '1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'),
    CONCAT('Clerk#', LPAD(1 + ((a.n * 10 + b.n + 1) % 100), 6, '0')),
    0,
    CONCAT('Order comment ', a.n * 10 + b.n + 1)
FROM _nums a CROSS JOIN _nums10 b
WHERE a.n * 10 + b.n + 1 <= 1500;

CREATE TABLE tpch_lineitem (
    l_orderkey INT NOT NULL,
    l_partkey INT NOT NULL,
    l_suppkey INT NOT NULL,
    l_linenumber INT NOT NULL,
    l_quantity DECIMAL(12,2) NOT NULL,
    l_extendedprice DECIMAL(12,2) NOT NULL,
    l_discount DECIMAL(12,2) NOT NULL,
    l_tax DECIMAL(12,2) NOT NULL,
    l_returnflag VARCHAR(1) NOT NULL,
    l_linestatus VARCHAR(1) NOT NULL,
    l_shipdate DATE NOT NULL,
    l_commitdate DATE NOT NULL,
    l_receiptdate DATE NOT NULL,
    l_shipinstruct VARCHAR(25) NOT NULL,
    l_shipmode VARCHAR(10) NOT NULL,
    l_comment VARCHAR(44),
    PRIMARY KEY (l_orderkey, l_linenumber)
);

-- Generate 3 lineitems per order = 4500 rows
CREATE TABLE _lines (j INT PRIMARY KEY);
INSERT INTO _lines VALUES (1),(2),(3);

INSERT INTO tpch_lineitem
SELECT
    o.o_orderkey,
    1 + ((o.o_orderkey * j.j) % 200),
    1 + ((o.o_orderkey * j.j) % 100),
    j.j,
    ROUND(1.00 + ((o.o_orderkey * j.j) % 50), 2),
    ROUND(900.00 + ((o.o_orderkey * j.j * 17) % 99000.00), 2),
    ROUND(((o.o_orderkey * j.j) % 11) * 0.01, 2),
    ROUND(((o.o_orderkey * j.j) % 9) * 0.01, 2),
    ELT(1 + ((o.o_orderkey + j.j) % 3), 'A', 'N', 'R'),
    IF(o.o_orderdate < '1995-06-17', 'F', 'O'),
    DATE_ADD(o.o_orderdate, INTERVAL (1 + (j.j * 7 % 120)) DAY),
    DATE_ADD(o.o_orderdate, INTERVAL (1 + (j.j * 5 % 90)) DAY),
    DATE_ADD(o.o_orderdate, INTERVAL (1 + (j.j * 9 % 150)) DAY),
    ELT(1 + ((o.o_orderkey + j.j) % 4), 'DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN'),
    ELT(1 + ((o.o_orderkey + j.j) % 7), 'TRUCK', 'MAIL', 'SHIP', 'AIR', 'RAIL', 'REG AIR', 'FOB'),
    CONCAT('Lineitem comment ', o.o_orderkey, '-', j.j)
FROM tpch_orders o CROSS JOIN _lines j;

-- Cleanup helper tables
DROP TABLE _nums;
DROP TABLE _nums10;
DROP TABLE _lines;

CREATE INDEX idx_tpch_orders_custkey ON tpch_orders(o_custkey);
CREATE INDEX idx_tpch_orders_date ON tpch_orders(o_orderdate);
CREATE INDEX idx_tpch_lineitem_orderkey ON tpch_lineitem(l_orderkey);
CREATE INDEX idx_tpch_lineitem_shipdate ON tpch_lineitem(l_shipdate);
CREATE INDEX idx_tpch_customer_nationkey ON tpch_customer(c_nationkey);

-- ============================================================================
-- CDC test table — events with timestamps for Change Data Capture testing
-- ============================================================================

CREATE TABLE cdc_events (
    event_id INT AUTO_INCREMENT PRIMARY KEY,
    event_type VARCHAR(30) NOT NULL,
    entity_type VARCHAR(30) NOT NULL,
    entity_id INT NOT NULL,
    payload JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO cdc_events (event_type, entity_type, entity_id, payload, created_at) VALUES
('INSERT', 'customer', 1, '{"name": "James Chen", "tier": "enterprise"}', '2024-01-01 10:00:00'),
('INSERT', 'customer', 2, '{"name": "Maria Garcia", "tier": "pro"}', '2024-01-01 10:01:00'),
('UPDATE', 'customer', 1, '{"tier": "enterprise", "city": "San Jose"}', '2024-01-02 14:30:00'),
('INSERT', 'order', 1001, '{"customer_id": 1, "amount": 149.99}', '2024-01-05 09:15:00'),
('INSERT', 'order', 1002, '{"customer_id": 4, "amount": 69.98}', '2024-01-08 11:22:00'),
('UPDATE', 'order', 1001, '{"status": "shipped"}', '2024-01-06 16:45:00'),
('INSERT', 'product', 11, '{"name": "New Widget", "price": 29.99}', '2024-01-10 08:00:00'),
('DELETE', 'product', 11, '{"reason": "discontinued"}', '2024-01-15 17:00:00'),
('UPDATE', 'customer', 3, '{"tier": "pro"}', '2024-01-20 09:00:00'),
('INSERT', 'order', 1011, '{"customer_id": 3, "amount": 199.99}', '2024-01-22 13:10:00');

CREATE INDEX idx_cdc_events_type ON cdc_events(event_type);
CREATE INDEX idx_cdc_events_entity ON cdc_events(entity_type, entity_id);
CREATE INDEX idx_cdc_events_created ON cdc_events(created_at);
