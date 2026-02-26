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
