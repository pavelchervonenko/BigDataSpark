CREATE KEYSPACE IF NOT EXISTS reports WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE reports;

CREATE TABLE IF NOT EXISTS report_products (
    product_id INT PRIMARY KEY,
    name TEXT,
    category TEXT,
    rating DECIMAL,
    reviews INT,
    total_quantity BIGINT,
    total_revenue DECIMAL,
    sales_count BIGINT
);

CREATE TABLE IF NOT EXISTS report_customers (
    customer_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    country TEXT,
    total_spent DECIMAL,
    avg_check DECIMAL,
    purchases_count BIGINT
);

CREATE TABLE IF NOT EXISTS report_time (
    year INT,
    month INT,
    total_revenue DECIMAL,
    sales_count BIGINT,
    avg_order_size DECIMAL,
    PRIMARY KEY (year, month)
);

CREATE TABLE IF NOT EXISTS report_stores (
    name TEXT PRIMARY KEY,
    city TEXT,
    country TEXT,
    total_revenue DECIMAL,
    avg_check DECIMAL,
    sales_count BIGINT
);

CREATE TABLE IF NOT EXISTS report_suppliers (
    name TEXT PRIMARY KEY,
    country TEXT,
    total_revenue DECIMAL,
    avg_product_price DECIMAL,
    sales_count BIGINT
);

CREATE TABLE IF NOT EXISTS report_quality (
    product_id INT PRIMARY KEY,
    name TEXT,
    category TEXT,
    rating DECIMAL,
    reviews INT,
    total_sold BIGINT,
    sales_count BIGINT
);
