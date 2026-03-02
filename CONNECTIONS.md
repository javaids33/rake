# RustLake Connection Details & Datasets

All services run via `docker compose up -d` from the project root.

---

## Postgres

| Field | Value |
|-------|-------|
| Host | `localhost` |
| Port | `5433` |
| Database | `rustlake_demo` |
| User | `rustlake` |
| Password | `rustlake` |
| Connection string | `postgresql://rustlake:rustlake@localhost:5433/rustlake_demo` |
| psql | `psql -h localhost -p 5433 -U rustlake -d rustlake_demo` |

### Demo Tables (public schema) -- 135 rows

| Table | Rows | Columns |
|-------|------|---------|
| `customers` | 50 | customer_id (int), name, email, city, state, country, signup_date (date), tier |
| `products` | 20 | product_id (int), name, category, price (numeric), cost (numeric), stock_qty (int) |
| `orders` | 50 | order_id (int), customer_id (int), product_id (int), quantity (int), total_amount (numeric), order_date (date), status, payment_method |
| `sales` | 15 | id (int), region, product, quantity (int), price (numeric), sale_date (date) |

### TPC-H Benchmark Tables (tpch schema) -- SF0.01, ~86,630 rows

Exposed as public views (`tpch_region`, `tpch_nation`, etc.) for auto-discovery.
Registered in RustLake as `pg_tpch_region`, `pg_tpch_nation`, etc.

| Table | Rows | Key Columns |
|-------|------|-------------|
| `tpch.region` | 5 | r_regionkey, r_name, r_comment |
| `tpch.nation` | 25 | n_nationkey, n_name, n_regionkey, n_comment |
| `tpch.supplier` | 100 | s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment |
| `tpch.part` | 2,000 | p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment |
| `tpch.partsupp` | 8,000 | ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment |
| `tpch.customer` | 1,500 | c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment |
| `tpch.orders` | 15,000 | o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment |
| `tpch.lineitem` | 60,000 | l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment |

### TPC-H Benchmark Queries Available (10)

| Query | Category | Description |
|-------|----------|-------------|
| Q1 | Aggregation | Pricing Summary Report |
| Q3 | Join + Filter | Shipping Priority |
| Q4 | Subquery | Order Priority Checking |
| Q5 | Multi-Join | Local Supplier Volume |
| Q6 | Scan + Filter | Forecasting Revenue Change |
| Q9 | Complex Join | Product Type Profit Measure |
| Q10 | Join + Aggregation | Returned Item Reporting |
| Q12 | Case + Aggregation | Shipping Modes and Order Priority |
| Q13 | Left Join + Aggregation | Customer Distribution |
| Q14 | Join + Conditional | Promotion Effect |

All runnable from the Benchmarks page in the UI or via `POST /api/v1/benchmarks/run`.

---

## MySQL

| Field | Value |
|-------|-------|
| Host | `localhost` |
| Port | `3307` |
| Database | `rustlake_demo` |
| User | `rustlake` |
| Password | `rustlake` |
| Connection string | `mysql://rustlake:rustlake@localhost:3307/rustlake_demo` |
| mysql cli | `mysql -h 127.0.0.1 -P 3307 -u rustlake -prustlake rustlake_demo` |

### Tables -- 30 rows

| Table | Rows | Columns |
|-------|------|---------|
| `customers` | 10 | customer_id (int PK), name (varchar 100), email (varchar 200), city (varchar 100), state (varchar 50), country (varchar 10), signup_date (date), tier (varchar 20) |
| `orders` | 10 | order_id (int PK), customer_id (int FK), product_id (int FK), quantity (int), total_amount (decimal 10,2), order_date (date), status (varchar 20), payment_method (varchar 30) |
| `products` | 10 | product_id (int PK), name (varchar 200), category (varchar 100), price (decimal 10,2), cost (decimal 10,2), stock_qty (int) |

---

## MongoDB

| Field | Value |
|-------|-------|
| Host | `localhost` |
| Port | `27018` |
| Database | `rustlake_demo` |
| User | `rustlake` |
| Password | `rustlake` |
| Auth database | `admin` |
| Connection string | `mongodb://rustlake:rustlake@localhost:27018/rustlake_demo?authSource=admin` |
| mongosh | `mongosh -u rustlake -p rustlake --authenticationDatabase admin --port 27018` |

### Collections -- 30 documents

| Collection | Docs | Sample Fields |
|------------|------|---------------|
| `customers` | 10 | customer_id, name, email, city, state, country, signup_date, tier |
| `orders` | 10 | order_id, customer_id, product_id, quantity, total_amount, order_date, status, payment_method |
| `products` | 10 | product_id, name, category, price, cost, stock_qty |

---

## MinIO (S3-compatible)

| Field | Value |
|-------|-------|
| Endpoint | `http://localhost:9000` |
| Console | `http://localhost:9001` |
| Access Key | `rustlake` |
| Secret Key | `rustlake123` |
| Region | `us-east-1` |
| Default Bucket | `rustlake-warehouse` |
| AWS CLI | `aws --endpoint-url http://localhost:9000 s3 ls` |

Set these environment variables to use with AWS SDK/CLI:
```
AWS_ACCESS_KEY_ID=rustlake
AWS_SECRET_ACCESS_KEY=rustlake123
AWS_ENDPOINT_URL=http://localhost:9000
AWS_REGION=us-east-1
```

---

## RustLake API

| Field | Value |
|-------|-------|
| HTTP API | `http://localhost:3000` |
| Health | `GET /health` |
| Arrow Flight gRPC | `localhost:50051` (when enabled) |
| Vite UI | `http://localhost:3001` |

### Key Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/sql` | POST | Execute SQL queries |
| `/api/v1/tables` | GET | List registered tables |
| `/api/v1/connections` | GET/POST | Manage database connections |
| `/api/v1/bootstrap/status` | GET | Check auto-bootstrap status |
| `/api/v1/bootstrap` | POST | Re-run bootstrap |
| `/api/v1/benchmarks/queries` | GET | List TPC-H benchmark queries |
| `/api/v1/benchmarks/run` | POST | Run a benchmark query |
| `/api/v1/benchmarks/results` | GET | Get benchmark results |
| `/api/v1/schedules` | GET/POST | Manage scheduled jobs |
| `/api/v1/streaming/pipelines` | GET/POST | Manage streaming pipelines |
| `/api/v1/transforms` | GET/POST | Manage SQL transforms |
| `/api/v1/upload` | POST | Upload CSV/Parquet/JSON files |

---

## Dataset Summary

| Source | Dataset | Rows | Use Case |
|--------|---------|------|----------|
| Postgres | Demo (customers, products, orders, sales) | 135 | Basic CRUD, joins, cross-source queries |
| Postgres | TPC-H SF0.01 (8 tables) | 86,630 | Benchmarks, complex analytics, ETL pipelines |
| MySQL | Demo (customers, orders, products) | 30 | Cross-database federation, CDC source |
| MongoDB | Demo (customers, orders, products) | 30 | NoSQL integration, change streams, CDC |
| MinIO | Object storage | -- | Iceberg warehouse, Parquet/CSV storage, data lake |

### What You Can Do With Each

**Postgres Demo Tables**
- Cross-table JOINs (customers + orders + products)
- Aggregations (sales by region, revenue by product)
- Scheduled SQL queries and materialized views
- ETL pipelines (source for transforms)

**TPC-H Benchmark Data**
- Run all 10 TPC-H queries from the Benchmarks page
- Build ETL pipelines (revenue by nation, customer segments)
- Schedule recurring analytics jobs
- Test query performance and optimizer behavior

**MySQL**
- Cross-database queries (MySQL + Postgres via DataFusion)
- CDC source for streaming pipelines
- Schema comparison across databases

**MongoDB**
- Change stream CDC for real-time ingestion
- Document-to-tabular transformation
- NoSQL integration testing

**MinIO**
- Iceberg table storage (write path)
- Parquet/CSV file uploads
- Data lake warehouse layer
