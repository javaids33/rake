-- ============================================================================
-- TPC-H Benchmark Schema + Data Generation (Scale Factor ~0.01)
-- ============================================================================
-- Generates the 8 standard TPC-H tables with realistic data distributions
-- using Postgres generate_series. Approximate row counts at SF0.01:
--   region:    5
--   nation:   25
--   part:     2,000
--   supplier: 100
--   partsupp: 8,000
--   customer: 1,500
--   orders:  15,000
--   lineitem: ~60,000
-- ============================================================================

-- Create a separate schema for TPC-H to avoid conflicts with demo tables
CREATE SCHEMA IF NOT EXISTS tpch;

-- ── Region (5 rows — fixed) ────────────────────────────────────────

CREATE TABLE tpch.region (
    r_regionkey INTEGER PRIMARY KEY,
    r_name      CHAR(25) NOT NULL,
    r_comment   VARCHAR(152)
);

INSERT INTO tpch.region VALUES
(0, 'AFRICA',    'Special requests from Africa are handled by the main branch in Nairobi'),
(1, 'AMERICA',   'Customers in the Americas benefit from coast-to-coast distribution'),
(2, 'ASIA',      'Asia-Pacific operations span multiple time zones and currencies'),
(3, 'EUROPE',    'European customers enjoy next-day delivery within the EU'),
(4, 'MIDDLE EAST','Middle East branch supports Arabic and English languages');

-- ── Nation (25 rows — fixed) ───────────────────────────────────────

CREATE TABLE tpch.nation (
    n_nationkey  INTEGER PRIMARY KEY,
    n_name       CHAR(25) NOT NULL,
    n_regionkey  INTEGER NOT NULL REFERENCES tpch.region(r_regionkey),
    n_comment    VARCHAR(152)
);

INSERT INTO tpch.nation VALUES
( 0, 'ALGERIA',          0, 'Algerian market expanding steadily'),
( 1, 'ARGENTINA',        1, 'Buenos Aires distribution center operational'),
( 2, 'BRAZIL',           1, 'Sao Paulo warehouse handles South American orders'),
( 3, 'CANADA',           1, 'Canadian operations centered in Toronto and Vancouver'),
( 4, 'EGYPT',            4, 'Cairo serves as the Middle East logistics hub'),
( 5, 'ETHIOPIA',         0, 'Addis Ababa regional office supports East African markets'),
( 6, 'FRANCE',           3, 'Paris headquarters for European operations'),
( 7, 'GERMANY',          3, 'Frankfurt logistics center serves Central Europe'),
( 8, 'INDIA',            2, 'Mumbai operations handle subcontinent orders'),
( 9, 'INDONESIA',        2, 'Jakarta warehouse for Southeast Asian distribution'),
(10, 'IRAN',             4, 'Tehran branch focusing on regional partnerships'),
(11, 'IRAQ',             4, 'Baghdad office reopened after reconstruction'),
(12, 'JAPAN',            2, 'Tokyo office manages Asia-Pacific premium accounts'),
(13, 'JORDAN',           4, 'Amman logistics center for Levant distribution'),
(14, 'KENYA',            0, 'Nairobi serves as East African regional hub'),
(15, 'MOROCCO',          0, 'Casablanca port crucial for North African imports'),
(16, 'MOZAMBIQUE',       0, 'Maputo operations expanding to serve Southern Africa'),
(17, 'PERU',             1, 'Lima office handles Andean region customers'),
(18, 'CHINA',            2, 'Shanghai warehouse is largest in Asia-Pacific region'),
(19, 'ROMANIA',          3, 'Bucharest technology center supports Eastern Europe'),
(20, 'SAUDI ARABIA',     4, 'Riyadh premium accounts division'),
(21, 'VIETNAM',          2, 'Ho Chi Minh City office for emerging market growth'),
(22, 'RUSSIA',           3, 'Moscow operations cover CIS countries'),
(23, 'UNITED KINGDOM',   3, 'London financial services and logistics coordination'),
(24, 'UNITED STATES',    1, 'New York headquarters with nationwide distribution');

-- ── Supplier (100 rows) ────────────────────────────────────────────

CREATE TABLE tpch.supplier (
    s_suppkey   INTEGER PRIMARY KEY,
    s_name      CHAR(25) NOT NULL,
    s_address   VARCHAR(40) NOT NULL,
    s_nationkey INTEGER NOT NULL REFERENCES tpch.nation(n_nationkey),
    s_phone     CHAR(15) NOT NULL,
    s_acctbal   NUMERIC(12,2) NOT NULL,
    s_comment   VARCHAR(101)
);

INSERT INTO tpch.supplier
SELECT
    s AS s_suppkey,
    'Supplier#' || LPAD(s::text, 9, '0') AS s_name,
    (s * 7 + 13)::text || ' Industrial Ave' AS s_address,
    s % 25 AS s_nationkey,
    LPAD(((s % 25) * 10 + 10)::text, 2, '0') || '-' ||
        LPAD(((s * 3 + 100) % 900 + 100)::text, 3, '0') || '-' ||
        LPAD(((s * 7 + 200) % 900 + 100)::text, 3, '0') || '-' ||
        LPAD(((s * 11 + 300) % 9000 + 1000)::text, 4, '0') AS s_phone,
    ROUND((RANDOM() * 9000 + 1000)::numeric, 2) AS s_acctbal,
    'Supplier ' || s || ' provides quality materials for manufacturing'
FROM generate_series(1, 100) AS s;

-- ── Part (2,000 rows) ──────────────────────────────────────────────

CREATE TABLE tpch.part (
    p_partkey     INTEGER PRIMARY KEY,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        CHAR(25) NOT NULL,
    p_brand       CHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INTEGER NOT NULL,
    p_container   CHAR(10) NOT NULL,
    p_retailprice NUMERIC(12,2) NOT NULL,
    p_comment     VARCHAR(23)
);

INSERT INTO tpch.part
SELECT
    p AS p_partkey,
    (ARRAY['almond','antique','aquamarine','azure','beige','bisque','black','blanched','blue','blush',
           'brown','burlywood','burnished','chartreuse','chiffon','chocolate','coral','cornflower',
           'cornsilk','cream','cyan','dark','deep','dim','dodger'])[1 + (p % 25)] || ' ' ||
    (ARRAY['steel','brass','copper','tin','nickel','iron','frosted','plated','polished','burnished'])[1 + (p % 10)] || ' ' ||
    (ARRAY['bolt','nut','screw','washer','spring','gear','valve','pipe','plate','wire'])[1 + ((p / 10) % 10)]
    AS p_name,
    'Manufacturer#' || (1 + p % 5) AS p_mfgr,
    'Brand#' || (1 + p % 5) || (1 + (p / 5) % 5) AS p_brand,
    (ARRAY['STANDARD','SMALL','MEDIUM','LARGE','ECONOMY','PROMO'])[1 + p % 6] || ' ' ||
    (ARRAY['ANODIZED','BURNISHED','PLATED','POLISHED','BRUSHED'])[1 + (p / 6) % 5] || ' ' ||
    (ARRAY['TIN','NICKEL','BRASS','STEEL','COPPER'])[1 + (p / 30) % 5]
    AS p_type,
    1 + p % 50 AS p_size,
    (ARRAY['SM CASE','SM BOX','SM PACK','SM PKG','SM BAG','MED BAG','MED BOX','MED PKG','LG CASE','LG BOX'])[1 + p % 10] AS p_container,
    ROUND((900 + p % 200 + (p / 10.0))::numeric, 2) AS p_retailprice,
    'part ' || p
FROM generate_series(1, 2000) AS p;

-- ── PartSupp (8,000 rows = 2000 parts x 4 suppliers each) ─────────

CREATE TABLE tpch.partsupp (
    ps_partkey    INTEGER NOT NULL REFERENCES tpch.part(p_partkey),
    ps_suppkey    INTEGER NOT NULL REFERENCES tpch.supplier(s_suppkey),
    ps_availqty   INTEGER NOT NULL,
    ps_supplycost NUMERIC(12,2) NOT NULL,
    ps_comment    VARCHAR(199),
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

INSERT INTO tpch.partsupp
SELECT
    p AS ps_partkey,
    1 + ((p + s_off - 1) % 100) AS ps_suppkey,
    FLOOR(RANDOM() * 9999 + 1)::int AS ps_availqty,
    ROUND((RANDOM() * 1000 + 1)::numeric, 2) AS ps_supplycost,
    'Supply arrangement for part ' || p || ' via supplier offset ' || s_off
FROM generate_series(1, 2000) AS p,
     generate_series(0, 3) AS s_off;

-- ── Customer (1,500 rows) ──────────────────────────────────────────

CREATE TABLE tpch.customer (
    c_custkey    INTEGER PRIMARY KEY,
    c_name       VARCHAR(25) NOT NULL,
    c_address    VARCHAR(40) NOT NULL,
    c_nationkey  INTEGER NOT NULL REFERENCES tpch.nation(n_nationkey),
    c_phone      CHAR(15) NOT NULL,
    c_acctbal    NUMERIC(12,2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment    VARCHAR(117)
);

INSERT INTO tpch.customer
SELECT
    c AS c_custkey,
    'Customer#' || LPAD(c::text, 9, '0') AS c_name,
    (c * 13 + 7)::text || ' Commerce Blvd' AS c_address,
    c % 25 AS c_nationkey,
    LPAD(((c % 25) * 10 + 10)::text, 2, '0') || '-' ||
        LPAD(((c * 3 + 100) % 900 + 100)::text, 3, '0') || '-' ||
        LPAD(((c * 7 + 200) % 900 + 100)::text, 3, '0') || '-' ||
        LPAD(((c * 11 + 300) % 9000 + 1000)::text, 4, '0') AS c_phone,
    ROUND((RANDOM() * 10000 - 999)::numeric, 2) AS c_acctbal,
    (ARRAY['AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY'])[1 + c % 5] AS c_mktsegment,
    'Customer ' || c || ' account details'
FROM generate_series(1, 1500) AS c;

-- ── Orders (15,000 rows) ───────────────────────────────────────────

CREATE TABLE tpch.orders (
    o_orderkey      INTEGER PRIMARY KEY,
    o_custkey       INTEGER NOT NULL REFERENCES tpch.customer(c_custkey),
    o_orderstatus   CHAR(1) NOT NULL,
    o_totalprice    NUMERIC(12,2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk         CHAR(15) NOT NULL,
    o_shippriority  INTEGER NOT NULL,
    o_comment       VARCHAR(79)
);

INSERT INTO tpch.orders
SELECT
    o AS o_orderkey,
    1 + (o % 1500) AS o_custkey,
    (ARRAY['O','F','P'])[1 + o % 3] AS o_orderstatus,
    ROUND((RANDOM() * 400000 + 1000)::numeric, 2) AS o_totalprice,
    DATE '1992-01-01' + (o % 2557) AS o_orderdate,
    (ARRAY['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'])[1 + o % 5] AS o_orderpriority,
    'Clerk#' || LPAD((1 + o % 1000)::text, 9, '0') AS o_clerk,
    0 AS o_shippriority,
    'order ' || o || ' comment'
FROM generate_series(1, 15000) AS o;

-- ── Lineitem (~60,000 rows = avg 4 items per order) ────────────────

CREATE TABLE tpch.lineitem (
    l_orderkey      INTEGER NOT NULL REFERENCES tpch.orders(o_orderkey),
    l_partkey       INTEGER NOT NULL REFERENCES tpch.part(p_partkey),
    l_suppkey       INTEGER NOT NULL REFERENCES tpch.supplier(s_suppkey),
    l_linenumber    INTEGER NOT NULL,
    l_quantity      NUMERIC(12,2) NOT NULL,
    l_extendedprice NUMERIC(12,2) NOT NULL,
    l_discount      NUMERIC(12,2) NOT NULL,
    l_tax           NUMERIC(12,2) NOT NULL,
    l_returnflag    CHAR(1) NOT NULL,
    l_linestatus    CHAR(1) NOT NULL,
    l_shipdate      DATE NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  CHAR(25) NOT NULL,
    l_shipmode      CHAR(10) NOT NULL,
    l_comment       VARCHAR(44),
    PRIMARY KEY (l_orderkey, l_linenumber)
);

INSERT INTO tpch.lineitem
SELECT
    o AS l_orderkey,
    1 + ((o + ln) % 2000) AS l_partkey,
    1 + ((o + ln * 3) % 100) AS l_suppkey,
    ln AS l_linenumber,
    FLOOR(RANDOM() * 50 + 1)::numeric AS l_quantity,
    ROUND((RANDOM() * 100000 + 100)::numeric, 2) AS l_extendedprice,
    ROUND((RANDOM() * 0.10)::numeric, 2) AS l_discount,
    ROUND((RANDOM() * 0.08)::numeric, 2) AS l_tax,
    (ARRAY['R','A','N'])[1 + (o + ln) % 3] AS l_returnflag,
    (ARRAY['O','F'])[1 + (o + ln) % 2] AS l_linestatus,
    DATE '1992-01-01' + ((o + ln * 7) % 2557) AS l_shipdate,
    DATE '1992-01-01' + ((o + ln * 7 + 30) % 2557) AS l_commitdate,
    DATE '1992-01-01' + ((o + ln * 7 + 37) % 2557) AS l_receiptdate,
    (ARRAY['DELIVER IN PERSON','COLLECT COD','TAKE BACK RETURN','NONE'])[1 + (o + ln) % 4] AS l_shipinstruct,
    (ARRAY['TRUCK','MAIL','REG AIR','AIR','FOB','RAIL','SHIP'])[1 + (o + ln) % 7] AS l_shipmode,
    'lineitem ' || o || '-' || ln
FROM generate_series(1, 15000) AS o,
     generate_series(1, 4) AS ln;

-- ── Create indexes for benchmark performance ───────────────────────

CREATE INDEX idx_tpch_lineitem_orderkey ON tpch.lineitem(l_orderkey);
CREATE INDEX idx_tpch_lineitem_partkey ON tpch.lineitem(l_partkey);
CREATE INDEX idx_tpch_lineitem_suppkey ON tpch.lineitem(l_suppkey);
CREATE INDEX idx_tpch_lineitem_shipdate ON tpch.lineitem(l_shipdate);
CREATE INDEX idx_tpch_orders_custkey ON tpch.orders(o_custkey);
CREATE INDEX idx_tpch_orders_orderdate ON tpch.orders(o_orderdate);
CREATE INDEX idx_tpch_customer_nationkey ON tpch.customer(c_nationkey);
CREATE INDEX idx_tpch_supplier_nationkey ON tpch.supplier(s_nationkey);
CREATE INDEX idx_tpch_nation_regionkey ON tpch.nation(n_regionkey);

-- ── Verify row counts ──────────────────────────────────────────────

DO $$
DECLARE
    cnt BIGINT;
BEGIN
    SELECT count(*) INTO cnt FROM tpch.lineitem;
    RAISE NOTICE 'TPC-H data loaded: lineitem=%, orders=15000, customer=1500, part=2000, supplier=100, partsupp=8000, nation=25, region=5', cnt;
END $$;
