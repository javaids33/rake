-- RustLake Demo: Postgres seed data
-- Mirrors the CSV files in sample-data/ for cross-source query demos.

-- ── Customers ────────────────────────────────────────────────────────

CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    country TEXT NOT NULL DEFAULT 'US',
    signup_date DATE NOT NULL,
    tier TEXT NOT NULL
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
(10, 'Amanda Taylor', 'amanda.t@gmail.com', 'Boston', 'MA', 'US', '2023-06-02', 'pro'),
(11, 'Christopher Anderson', 'c.anderson@outlook.com', 'Phoenix', 'AZ', 'US', '2023-06-20', 'free'),
(12, 'Jessica Thomas', 'jess.thomas@gmail.com', 'Nashville', 'TN', 'US', '2023-07-08', 'enterprise'),
(13, 'Daniel Jackson', 'd.jackson@protonmail.com', 'Atlanta', 'GA', 'US', '2023-07-25', 'free'),
(14, 'Lisa White', 'lisa.white@gmail.com', 'Minneapolis', 'MN', 'US', '2023-08-11', 'pro'),
(15, 'Matthew Harris', 'm.harris@yahoo.com', 'Dallas', 'TX', 'US', '2023-08-30', 'free'),
(16, 'Ashley Clark', 'ashley.clark@gmail.com', 'San Diego', 'CA', 'US', '2023-09-14', 'pro'),
(17, 'Joshua Lewis', 'j.lewis@outlook.com', 'Raleigh', 'NC', 'US', '2023-09-29', 'free'),
(18, 'Megan Robinson', 'megan.r@gmail.com', 'Salt Lake City', 'UT', 'US', '2023-10-12', 'enterprise'),
(19, 'Andrew Walker', 'a.walker@yahoo.com', 'Philadelphia', 'PA', 'US', '2023-10-28', 'free'),
(20, 'Stephanie Hall', 'steph.hall@gmail.com', 'Columbus', 'OH', 'US', '2023-11-09', 'pro'),
(21, 'Ryan Allen', 'ryan.allen@protonmail.com', 'Charlotte', 'NC', 'US', '2023-11-25', 'free'),
(22, 'Nicole Young', 'n.young@gmail.com', 'Detroit', 'MI', 'US', '2023-12-07', 'pro'),
(23, 'Kevin King', 'kevin.king@outlook.com', 'San Jose', 'CA', 'US', '2024-01-03', 'enterprise'),
(24, 'Rachel Wright', 'r.wright@gmail.com', 'Pittsburgh', 'PA', 'US', '2024-01-18', 'free'),
(25, 'Brian Scott', 'brian.scott@yahoo.com', 'Tampa', 'FL', 'US', '2024-02-04', 'pro'),
(26, 'Lauren Green', 'lauren.g@gmail.com', 'Indianapolis', 'IN', 'US', '2024-02-20', 'free'),
(27, 'Tyler Adams', 't.adams@protonmail.com', 'Kansas City', 'MO', 'US', '2024-03-08', 'pro'),
(28, 'Amber Nelson', 'amber.n@gmail.com', 'Las Vegas', 'NV', 'US', '2024-03-24', 'free'),
(29, 'Justin Carter', 'j.carter@outlook.com', 'Sacramento', 'CA', 'US', '2024-04-10', 'enterprise'),
(30, 'Kayla Mitchell', 'kayla.m@gmail.com', 'Orlando', 'FL', 'US', '2024-04-28', 'free'),
(31, 'Brandon Perez', 'b.perez@yahoo.com', 'Tucson', 'AZ', 'US', '2024-05-15', 'pro'),
(32, 'Samantha Roberts', 'sam.roberts@gmail.com', 'Milwaukee', 'WI', 'US', '2024-06-01', 'free'),
(33, 'Nathan Turner', 'n.turner@protonmail.com', 'Richmond', 'VA', 'US', '2024-06-19', 'pro'),
(34, 'Christina Phillips', 'c.phillips@gmail.com', 'Honolulu', 'HI', 'US', '2024-07-05', 'free'),
(35, 'Patrick Campbell', 'p.campbell@outlook.com', 'St. Louis', 'MO', 'US', '2024-07-22', 'enterprise'),
(36, 'Heather Parker', 'h.parker@gmail.com', 'Louisville', 'KY', 'US', '2024-08-08', 'free'),
(37, 'Sean Evans', 'sean.evans@yahoo.com', 'Oklahoma City', 'OK', 'US', '2024-08-25', 'pro'),
(38, 'Courtney Edwards', 'c.edwards@gmail.com', 'Hartford', 'CT', 'US', '2024-09-11', 'free'),
(39, 'Derek Collins', 'derek.c@protonmail.com', 'Albuquerque', 'NM', 'US', '2024-09-28', 'pro'),
(40, 'Vanessa Stewart', 'v.stewart@gmail.com', 'Boise', 'ID', 'US', '2024-10-14', 'free'),
(41, 'Marcus Sanchez', 'm.sanchez@outlook.com', 'Omaha', 'NE', 'US', '2024-10-30', 'enterprise'),
(42, 'Tiffany Morris', 'tiffany.m@gmail.com', 'Anchorage', 'AK', 'US', '2024-11-16', 'free'),
(43, 'Eric Rogers', 'e.rogers@yahoo.com', 'Memphis', 'TN', 'US', '2024-12-02', 'pro'),
(44, 'Monica Reed', 'monica.r@gmail.com', 'Buffalo', 'NY', 'US', '2024-12-19', 'free'),
(45, 'Travis Cook', 't.cook@protonmail.com', 'Des Moines', 'IA', 'US', '2025-01-06', 'pro'),
(46, 'Diana Morgan', 'diana.m@gmail.com', 'Charleston', 'SC', 'US', '2025-01-23', 'free'),
(47, 'Cody Bell', 'cody.bell@outlook.com', 'Madison', 'WI', 'US', '2025-02-09', 'enterprise'),
(48, 'Alexis Murphy', 'a.murphy@gmail.com', 'Savannah', 'GA', 'US', '2025-02-26', 'free'),
(49, 'Kyle Bailey', 'kyle.b@yahoo.com', 'Portland', 'ME', 'US', '2025-03-15', 'pro'),
(50, 'Natalie Rivera', 'natalie.r@gmail.com', 'Spokane', 'WA', 'US', '2025-04-01', 'free');

-- ── Products ─────────────────────────────────────────────────────────

CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    cost NUMERIC(10,2) NOT NULL,
    stock_qty INTEGER NOT NULL
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
(10, 'Running Shoes Ultralight', 'Clothing', 119.99, 45.00, 225),
(11, 'Cast Iron Dutch Oven 6Qt', 'Home & Garden', 64.99, 25.00, 165),
(12, 'Indoor Herb Garden Kit', 'Home & Garden', 29.99, 10.50, 430),
(13, 'Smart LED Bulbs 4-Pack', 'Home & Garden', 44.99, 16.00, 550),
(14, 'Bamboo Cutting Board Set', 'Home & Garden', 24.99, 8.00, 375),
(15, 'Yoga Mat Premium 6mm', 'Sports', 38.99, 13.00, 290),
(16, 'Adjustable Dumbbell Set 25lb', 'Sports', 189.99, 78.00, 95),
(17, 'Insulated Water Bottle 32oz', 'Sports', 27.99, 9.50, 620),
(18, 'Resistance Band Set (5 bands)', 'Sports', 19.99, 6.00, 480),
(19, 'The Pragmatic Programmer (2nd Ed)', 'Books', 49.99, 22.00, 200),
(20, 'Designing Data-Intensive Applications', 'Books', 42.99, 18.00, 175);

-- ── Orders ───────────────────────────────────────────────────────────

CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    order_date DATE NOT NULL,
    status TEXT NOT NULL,
    payment_method TEXT NOT NULL
);

INSERT INTO orders (order_id, customer_id, product_id, quantity, total_amount, order_date, status, payment_method) VALUES
(1001,1,1,1,149.99,'2024-01-05','completed','credit_card'),
(1002,4,8,2,69.98,'2024-01-08','completed','paypal'),
(1003,2,3,1,89.99,'2024-01-12','completed','debit_card'),
(1004,6,13,2,89.98,'2024-01-15','completed','credit_card'),
(1005,3,17,1,27.99,'2024-01-18','completed','apple_pay'),
(1006,1,4,1,129.99,'2024-01-22','completed','credit_card'),
(1007,8,6,1,78.00,'2024-01-25','completed','paypal'),
(1008,10,19,1,49.99,'2024-01-28','completed','credit_card'),
(1009,5,12,3,89.97,'2024-02-01','completed','debit_card'),
(1010,12,1,1,149.99,'2024-02-04','completed','credit_card'),
(1011,2,14,2,49.98,'2024-02-07','completed','apple_pay'),
(1012,7,18,1,19.99,'2024-02-10','completed','paypal'),
(1013,4,5,1,59.99,'2024-02-14','completed','credit_card'),
(1014,1,2,2,79.98,'2024-02-17','completed','credit_card'),
(1015,9,8,3,104.97,'2024-02-20','completed','debit_card'),
(1016,15,11,1,64.99,'2024-02-23','completed','paypal'),
(1017,6,20,1,42.99,'2024-02-26','completed','credit_card'),
(1018,3,10,1,119.99,'2024-03-01','completed','apple_pay'),
(1019,11,17,2,55.98,'2024-03-04','completed','debit_card'),
(1020,2,1,1,149.99,'2024-03-07','completed','credit_card'),
(1021,18,7,1,135.00,'2024-03-10','completed','credit_card'),
(1022,1,13,3,134.97,'2024-03-14','completed','credit_card'),
(1023,14,9,2,109.98,'2024-03-17','completed','paypal'),
(1024,4,15,1,38.99,'2024-03-20','completed','apple_pay'),
(1025,8,2,1,39.99,'2024-03-23','completed','credit_card'),
(1026,20,3,1,89.99,'2024-03-26','completed','debit_card'),
(1027,6,16,1,189.99,'2024-03-30','completed','credit_card'),
(1028,1,17,4,111.96,'2024-04-02','completed','credit_card'),
(1029,13,8,2,69.98,'2024-04-05','completed','paypal'),
(1030,2,4,1,129.99,'2024-04-08','completed','credit_card'),
(1031,23,1,1,149.99,'2024-04-12','completed','apple_pay'),
(1032,5,12,1,29.99,'2024-04-15','completed','debit_card'),
(1033,16,6,2,156.00,'2024-04-18','completed','credit_card'),
(1034,4,11,1,64.99,'2024-04-21','completed','paypal'),
(1035,12,19,2,99.98,'2024-04-24','completed','credit_card'),
(1036,1,5,1,59.99,'2024-04-28','completed','credit_card'),
(1037,10,7,1,135.00,'2024-05-01','completed','debit_card'),
(1038,25,13,1,44.99,'2024-05-04','completed','credit_card'),
(1039,3,14,3,74.97,'2024-05-07','completed','apple_pay'),
(1040,6,1,2,299.98,'2024-05-10','completed','credit_card'),
(1041,22,18,2,39.98,'2024-05-14','completed','paypal'),
(1042,8,20,1,42.99,'2024-05-17','completed','credit_card'),
(1043,2,9,1,54.99,'2024-05-20','completed','debit_card'),
(1044,1,3,1,89.99,'2024-05-24','completed','credit_card'),
(1045,17,15,1,38.99,'2024-05-27','completed','apple_pay'),
(1046,4,2,3,119.97,'2024-05-30','completed','credit_card'),
(1047,29,10,1,119.99,'2024-06-02','completed','paypal'),
(1048,12,17,2,55.98,'2024-06-05','completed','credit_card'),
(1049,6,8,4,139.96,'2024-06-08','completed','credit_card'),
(1050,14,4,1,129.99,'2024-06-12','completed','debit_card');

-- ── Sales ────────────────────────────────────────────────────────────

CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    region TEXT NOT NULL,
    product TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    sale_date DATE NOT NULL
);

INSERT INTO sales (id, region, product, quantity, price, sale_date) VALUES
(1, 'North', 'Widget A', 100, 29.99, '2025-01-15'),
(2, 'South', 'Widget B', 250, 49.99, '2025-01-20'),
(3, 'East', 'Widget A', 75, 29.99, '2025-02-01'),
(4, 'West', 'Widget C', 300, 19.99, '2025-02-10'),
(5, 'North', 'Widget B', 180, 49.99, '2025-02-15'),
(6, 'South', 'Widget A', 90, 29.99, '2025-03-01'),
(7, 'East', 'Widget C', 400, 19.99, '2025-03-05'),
(8, 'West', 'Widget B', 220, 49.99, '2025-03-10'),
(9, 'North', 'Widget C', 150, 19.99, '2025-03-20'),
(10, 'South', 'Widget A', 130, 29.99, '2025-04-01'),
(11, 'East', 'Widget B', 310, 49.99, '2025-04-15'),
(12, 'West', 'Widget C', 280, 19.99, '2025-04-20'),
(13, 'North', 'Widget A', 200, 29.99, '2025-05-01'),
(14, 'South', 'Widget C', 170, 19.99, '2025-05-10'),
(15, 'East', 'Widget B', 140, 49.99, '2025-05-20');

-- ── Indexes ──────────────────────────────────────────────────────────

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_product_id ON orders(product_id);
CREATE INDEX idx_orders_order_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_customers_tier ON customers(tier);
CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_sales_region ON sales(region);
