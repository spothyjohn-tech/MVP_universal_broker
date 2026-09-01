-- Таблица для фиксации продаж из РегистрНакопления.ВыручкаИСебестоимостьПродаж
CREATE TABLE IF NOT EXISTS sales (
    id String,
    product_id String,
    client_id Nullable(String),
    warehouse_id String,
    count UInt32,
    price float,
    period DateTime
) ENGINE = MergeTree()
ORDER BY (period, product_id);  
-- Таблица для фиксации остатков из РегистрНакопления.ТоварыНаСкладах
CREATE TABLE IF NOT EXISTS stocks (
    id String,
    product_id String,
    warehouse_id String,
    current_stock Int32,
    period DateTime
) ENGINE = MergeTree()
ORDER BY (period, product_id);
