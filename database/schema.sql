CREATE TABLE IF NOT EXISTS sales_aggregated (
    id           SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end   TIMESTAMP,
    region       VARCHAR(50),
    product      VARCHAR(100),
    total_sales  NUMERIC(12, 2),
    order_count  INTEGER,
    created_at   TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_region_time 
ON sales_aggregated (region, window_start);

CREATE INDEX IF NOT EXISTS idx_product_time 
ON sales_aggregated (product, window_start);

CREATE TABLE IF NOT EXISTS raw_sales_events (
    id         SERIAL PRIMARY KEY,
    order_id   VARCHAR(50),
    product    VARCHAR(100),
    region     VARCHAR(50),
    amount     NUMERIC(10, 2),
    event_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);