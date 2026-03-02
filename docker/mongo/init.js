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

// ── TPC-H: Region ──────────────────────────────────────────────────

db.tpch_region.insertMany([
  { r_regionkey: 0, r_name: "AFRICA", r_comment: "Vast continent with diverse economies" },
  { r_regionkey: 1, r_name: "AMERICA", r_comment: "North and South American markets" },
  { r_regionkey: 2, r_name: "ASIA", r_comment: "Fast-growing Asian economies" },
  { r_regionkey: 3, r_name: "EUROPE", r_comment: "Mature European markets" },
  { r_regionkey: 4, r_name: "MIDDLE EAST", r_comment: "Oil-rich Middle Eastern economies" },
]);
db.tpch_region.createIndex({ r_regionkey: 1 }, { unique: true });

// ── TPC-H: Nation ──────────────────────────────────────────────────

db.tpch_nation.insertMany([
  { n_nationkey: 0, n_name: "ALGERIA", n_regionkey: 0 },
  { n_nationkey: 1, n_name: "ARGENTINA", n_regionkey: 1 },
  { n_nationkey: 2, n_name: "BRAZIL", n_regionkey: 1 },
  { n_nationkey: 3, n_name: "CANADA", n_regionkey: 1 },
  { n_nationkey: 4, n_name: "EGYPT", n_regionkey: 4 },
  { n_nationkey: 5, n_name: "ETHIOPIA", n_regionkey: 0 },
  { n_nationkey: 6, n_name: "FRANCE", n_regionkey: 3 },
  { n_nationkey: 7, n_name: "GERMANY", n_regionkey: 3 },
  { n_nationkey: 8, n_name: "INDIA", n_regionkey: 2 },
  { n_nationkey: 9, n_name: "INDONESIA", n_regionkey: 2 },
  { n_nationkey: 10, n_name: "IRAN", n_regionkey: 4 },
  { n_nationkey: 11, n_name: "IRAQ", n_regionkey: 4 },
  { n_nationkey: 12, n_name: "JAPAN", n_regionkey: 2 },
  { n_nationkey: 13, n_name: "JORDAN", n_regionkey: 4 },
  { n_nationkey: 14, n_name: "KENYA", n_regionkey: 0 },
  { n_nationkey: 15, n_name: "MOROCCO", n_regionkey: 0 },
  { n_nationkey: 16, n_name: "MOZAMBIQUE", n_regionkey: 0 },
  { n_nationkey: 17, n_name: "PERU", n_regionkey: 1 },
  { n_nationkey: 18, n_name: "CHINA", n_regionkey: 2 },
  { n_nationkey: 19, n_name: "ROMANIA", n_regionkey: 3 },
  { n_nationkey: 20, n_name: "SAUDI ARABIA", n_regionkey: 4 },
  { n_nationkey: 21, n_name: "VIETNAM", n_regionkey: 2 },
  { n_nationkey: 22, n_name: "RUSSIA", n_regionkey: 3 },
  { n_nationkey: 23, n_name: "UNITED KINGDOM", n_regionkey: 3 },
  { n_nationkey: 24, n_name: "UNITED STATES", n_regionkey: 1 },
]);
db.tpch_nation.createIndex({ n_nationkey: 1 }, { unique: true });

// ── TPC-H: Customer (150 rows) ─────────────────────────────────────

const segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"];
const tpchCustomers = [];
for (let i = 1; i <= 150; i++) {
  tpchCustomers.push({
    c_custkey: i,
    c_name: "Customer#" + String(i).padStart(6, "0"),
    c_address: (i * 7 % 999) + " Main St, City " + (i % 50),
    c_nationkey: i % 25,
    c_phone: String(10 + (i % 25)).padStart(2, "0") + "-" + String(100 + (i * 3 % 900)).padStart(3, "0") + "-" + String(1000 + (i * 7 % 9000)).padStart(4, "0"),
    c_acctbal: Math.round((-999.99 + (i * 73.17 % 10998.98)) * 100) / 100,
    c_mktsegment: segments[i % 5],
    c_comment: "Comment for customer " + i,
  });
}
db.tpch_customer.insertMany(tpchCustomers);
db.tpch_customer.createIndex({ c_custkey: 1 }, { unique: true });
db.tpch_customer.createIndex({ c_nationkey: 1 });

// ── TPC-H: Orders (1500 rows) ──────────────────────────────────────

const orderStatuses = ["O", "F", "P"];
const priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];
const tpchOrders = [];
for (let i = 1; i <= 1500; i++) {
  const dayOffset = (i * 3) % 2556;
  const orderDate = new Date(1992, 0, 1);
  orderDate.setDate(orderDate.getDate() + dayOffset);
  tpchOrders.push({
    o_orderkey: i,
    o_custkey: 1 + (i % 150),
    o_orderstatus: orderStatuses[i % 3],
    o_totalprice: Math.round((1000.0 + (i * 37.89 % 299000.0)) * 100) / 100,
    o_orderdate: orderDate,
    o_orderpriority: priorities[i % 5],
    o_clerk: "Clerk#" + String(1 + (i % 100)).padStart(6, "0"),
    o_shippriority: 0,
    o_comment: "Order comment " + i,
  });
}
db.tpch_orders.insertMany(tpchOrders);
db.tpch_orders.createIndex({ o_orderkey: 1 }, { unique: true });
db.tpch_orders.createIndex({ o_custkey: 1 });
db.tpch_orders.createIndex({ o_orderdate: 1 });

// ── TPC-H: Lineitem (~4500 rows) ───────────────────────────────────

const shipinstructs = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"];
const shipmodes = ["TRUCK", "MAIL", "SHIP", "AIR", "RAIL", "REG AIR", "FOB"];
const returnflags = ["A", "N", "R"];
const tpchLineitems = [];
for (let i = 1; i <= 1500; i++) {
  const baseDateMs = new Date(1992, 0, 1).getTime();
  const dayOffset = (i * 3) % 2556;
  for (let j = 1; j <= 3; j++) {
    const odate = new Date(baseDateMs + dayOffset * 86400000);
    const shipdate = new Date(odate.getTime() + (1 + (j * 7 % 120)) * 86400000);
    const commitdate = new Date(odate.getTime() + (1 + (j * 5 % 90)) * 86400000);
    const receiptdate = new Date(odate.getTime() + (1 + (j * 9 % 150)) * 86400000);
    tpchLineitems.push({
      l_orderkey: i,
      l_partkey: 1 + ((i * j) % 200),
      l_suppkey: 1 + ((i * j) % 100),
      l_linenumber: j,
      l_quantity: Math.round((1.0 + ((i * j) % 50)) * 100) / 100,
      l_extendedprice: Math.round((900.0 + ((i * j * 17) % 99000.0)) * 100) / 100,
      l_discount: Math.round(((i * j % 11) * 0.01) * 100) / 100,
      l_tax: Math.round(((i * j % 9) * 0.01) * 100) / 100,
      l_returnflag: returnflags[(i + j) % 3],
      l_linestatus: odate < new Date(1995, 5, 17) ? "F" : "O",
      l_shipdate: shipdate,
      l_commitdate: commitdate,
      l_receiptdate: receiptdate,
      l_shipinstruct: shipinstructs[(i + j) % 4],
      l_shipmode: shipmodes[(i + j) % 7],
      l_comment: "Lineitem comment " + i + "-" + j,
    });
  }
}
// Insert in batches to avoid exceeding BSON size limit
for (let b = 0; b < tpchLineitems.length; b += 1000) {
  db.tpch_lineitem.insertMany(tpchLineitems.slice(b, b + 1000));
}
db.tpch_lineitem.createIndex({ l_orderkey: 1, l_linenumber: 1 }, { unique: true });
db.tpch_lineitem.createIndex({ l_shipdate: 1 });

// ── CDC Events (for Change Data Capture testing) ────────────────────

db.cdc_events.insertMany([
  { event_type: "INSERT", entity_type: "customer", entity_id: 1, payload: { name: "James Chen", tier: "enterprise" }, created_at: new Date("2024-01-01T10:00:00Z") },
  { event_type: "INSERT", entity_type: "customer", entity_id: 2, payload: { name: "Maria Garcia", tier: "pro" }, created_at: new Date("2024-01-01T10:01:00Z") },
  { event_type: "UPDATE", entity_type: "customer", entity_id: 1, payload: { tier: "enterprise", city: "San Jose" }, created_at: new Date("2024-01-02T14:30:00Z") },
  { event_type: "INSERT", entity_type: "order", entity_id: 1001, payload: { customer_id: 1, amount: 149.99 }, created_at: new Date("2024-01-05T09:15:00Z") },
  { event_type: "INSERT", entity_type: "order", entity_id: 1002, payload: { customer_id: 4, amount: 69.98 }, created_at: new Date("2024-01-08T11:22:00Z") },
  { event_type: "UPDATE", entity_type: "order", entity_id: 1001, payload: { status: "shipped" }, created_at: new Date("2024-01-06T16:45:00Z") },
  { event_type: "INSERT", entity_type: "product", entity_id: 11, payload: { name: "New Widget", price: 29.99 }, created_at: new Date("2024-01-10T08:00:00Z") },
  { event_type: "DELETE", entity_type: "product", entity_id: 11, payload: { reason: "discontinued" }, created_at: new Date("2024-01-15T17:00:00Z") },
  { event_type: "UPDATE", entity_type: "customer", entity_id: 3, payload: { tier: "pro" }, created_at: new Date("2024-01-20T09:00:00Z") },
  { event_type: "INSERT", entity_type: "order", entity_id: 1011, payload: { customer_id: 3, amount: 199.99 }, created_at: new Date("2024-01-22T13:10:00Z") },
]);
db.cdc_events.createIndex({ event_type: 1 });
db.cdc_events.createIndex({ entity_type: 1, entity_id: 1 });
db.cdc_events.createIndex({ created_at: 1 });
