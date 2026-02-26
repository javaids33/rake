// RustLake Demo: MongoDB seed data
// Mirrors the CSV files in sample-data/ for cross-source query demos.

db = db.getSiblingDB('rustlake_demo');

// ── Customers ────────────────────────────────────────────────────────

db.customers.insertMany([
  { customer_id: 1, name: "James Chen", email: "james.chen@gmail.com", city: "San Francisco", state: "CA", country: "US", signup_date: new Date("2023-01-15"), tier: "enterprise" },
  { customer_id: 2, name: "Maria Garcia", email: "maria.garcia@outlook.com", city: "Austin", state: "TX", country: "US", signup_date: new Date("2023-02-08"), tier: "pro" },
  { customer_id: 3, name: "David Kim", email: "david.kim@yahoo.com", city: "Seattle", state: "WA", country: "US", signup_date: new Date("2023-02-22"), tier: "free" },
  { customer_id: 4, name: "Sarah Johnson", email: "sarah.j@gmail.com", city: "New York", state: "NY", country: "US", signup_date: new Date("2023-03-10"), tier: "pro" },
  { customer_id: 5, name: "Michael Brown", email: "m.brown@protonmail.com", city: "Chicago", state: "IL", country: "US", signup_date: new Date("2023-03-28"), tier: "free" },
  { customer_id: 6, name: "Emily Davis", email: "emily.davis@gmail.com", city: "Portland", state: "OR", country: "US", signup_date: new Date("2023-04-05"), tier: "enterprise" },
  { customer_id: 7, name: "Robert Wilson", email: "rwilson@outlook.com", city: "Denver", state: "CO", country: "US", signup_date: new Date("2023-04-19"), tier: "free" },
  { customer_id: 8, name: "Jennifer Lee", email: "jlee@gmail.com", city: "Los Angeles", state: "CA", country: "US", signup_date: new Date("2023-05-01"), tier: "pro" },
  { customer_id: 9, name: "William Martinez", email: "w.martinez@yahoo.com", city: "Miami", state: "FL", country: "US", signup_date: new Date("2023-05-17"), tier: "free" },
  { customer_id: 10, name: "Amanda Taylor", email: "amanda.t@gmail.com", city: "Boston", state: "MA", country: "US", signup_date: new Date("2023-06-02"), tier: "pro" },
]);

db.customers.createIndex({ customer_id: 1 }, { unique: true });
db.customers.createIndex({ tier: 1 });

// ── Products ─────────────────────────────────────────────────────────

db.products.insertMany([
  { product_id: 1, name: "Wireless Noise-Canceling Headphones", category: "Electronics", price: 149.99, cost: 62.00, stock_qty: 340 },
  { product_id: 2, name: "USB-C Charging Hub 7-Port", category: "Electronics", price: 39.99, cost: 14.50, stock_qty: 520 },
  { product_id: 3, name: "4K Webcam with Ring Light", category: "Electronics", price: 89.99, cost: 35.00, stock_qty: 185 },
  { product_id: 4, name: "Mechanical Keyboard RGB", category: "Electronics", price: 129.99, cost: 48.00, stock_qty: 275 },
  { product_id: 5, name: "Portable Bluetooth Speaker", category: "Electronics", price: 59.99, cost: 22.00, stock_qty: 410 },
  { product_id: 6, name: "Merino Wool Crew Neck Sweater", category: "Clothing", price: 78.00, cost: 28.00, stock_qty: 190 },
  { product_id: 7, name: "Waterproof Hiking Jacket", category: "Clothing", price: 135.00, cost: 52.00, stock_qty: 145 },
  { product_id: 8, name: "Organic Cotton T-Shirt Pack (3)", category: "Clothing", price: 34.99, cost: 11.00, stock_qty: 680 },
  { product_id: 9, name: "Slim Fit Chino Pants", category: "Clothing", price: 54.99, cost: 19.00, stock_qty: 310 },
  { product_id: 10, name: "Running Shoes Ultralight", category: "Clothing", price: 119.99, cost: 45.00, stock_qty: 225 },
]);

db.products.createIndex({ product_id: 1 }, { unique: true });
db.products.createIndex({ category: 1 });

// ── Orders ───────────────────────────────────────────────────────────

db.orders.insertMany([
  { order_id: 1001, customer_id: 1, product_id: 1, quantity: 1, total_amount: 149.99, order_date: new Date("2024-01-05"), status: "completed", payment_method: "credit_card" },
  { order_id: 1002, customer_id: 4, product_id: 8, quantity: 2, total_amount: 69.98, order_date: new Date("2024-01-08"), status: "completed", payment_method: "paypal" },
  { order_id: 1003, customer_id: 2, product_id: 3, quantity: 1, total_amount: 89.99, order_date: new Date("2024-01-12"), status: "completed", payment_method: "debit_card" },
  { order_id: 1004, customer_id: 6, product_id: 4, quantity: 2, total_amount: 259.98, order_date: new Date("2024-01-15"), status: "completed", payment_method: "credit_card" },
  { order_id: 1005, customer_id: 3, product_id: 5, quantity: 1, total_amount: 59.99, order_date: new Date("2024-01-18"), status: "completed", payment_method: "apple_pay" },
  { order_id: 1006, customer_id: 1, product_id: 4, quantity: 1, total_amount: 129.99, order_date: new Date("2024-01-22"), status: "completed", payment_method: "credit_card" },
  { order_id: 1007, customer_id: 8, product_id: 6, quantity: 1, total_amount: 78.00, order_date: new Date("2024-01-25"), status: "completed", payment_method: "paypal" },
  { order_id: 1008, customer_id: 10, product_id: 9, quantity: 1, total_amount: 54.99, order_date: new Date("2024-01-28"), status: "completed", payment_method: "credit_card" },
  { order_id: 1009, customer_id: 5, product_id: 2, quantity: 3, total_amount: 119.97, order_date: new Date("2024-02-01"), status: "completed", payment_method: "debit_card" },
  { order_id: 1010, customer_id: 2, product_id: 1, quantity: 1, total_amount: 149.99, order_date: new Date("2024-02-04"), status: "completed", payment_method: "credit_card" },
]);

db.orders.createIndex({ order_id: 1 }, { unique: true });
db.orders.createIndex({ customer_id: 1 });
db.orders.createIndex({ order_date: 1 });
