CREATE TABLE IF NOT EXISTS stock_tables (
    product_id VARCHAR(255) NOT NULL,
    warehouse_id VARCHAR(255) NOT NULL,
    current_stock INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY(product_id, warehouse_id)
)