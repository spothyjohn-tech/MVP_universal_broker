CREATE TABLE IF NOT EXISTS stock_tables (
    product_id VARCHAR(255) NOT NULL,
    warehouse_id VARCHAR(255) NOT NULL,
    current_stock NUMERIC(15,2) NOT NULL DEFAULT 0.00,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY(product_id, warehouse_id)
)