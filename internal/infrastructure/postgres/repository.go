package postgres

import (
	"context"
	"fmt"
	"broker/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BalanceRepository struct {
	pool *pgxpool.Pool
}

func NewBalanceRepository(pool *pgxpool.Pool) *BalanceRepository {
	return &BalanceRepository{pool: pool}
}

func (r *BalanceRepository) UpsertBalancesBulk(ctx context.Context, stocks []domain.StocksPayload) error {
	if len(stocks) == 0 {
		return nil
	}
	// Берем нативное соединение из пула
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pgx begin tx failed: %w", err)
	}
	defer tx.Rollback(ctx)
// 1. Создаем быструю unlogged временную таблицу
	_, err = tx.Exec(ctx, `CREATE TEMP TABLE temp_stock_balances (
		product_id VARCHAR(255),
		warehouse_id VARCHAR(255),
		current_stock INT,
		period TIMESTAMP
	) ON COMMIT DROP;`)
	if err != nil {
		return fmt.Errorf("create temp table failed: %w", err)
	}
	// 2. Готовим данные для бинарного протокола COPY
	rows := make([][]interface{}, len(stocks))
	for i, s := range stocks{
		rows[i] = []interface{}{s.ProductID, s.WarehouseID, s.CurrentStock, s.Period}
	}
	// 3. Моментальный стриминг данных в Postgres через нативный CopyFrom
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"temp_stock_balances"},
		[]string{"product_id", "warehouse_id", "current_stock", "period"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("pgx copy from failed: %w", err)
	}
	// 4. Одномоментный перенос данных в целевую таблицу с разрешением конфликтов (UPSERT)
	_, err = tx.Exec(ctx, `
	INSERT INTO stock_tables (product_id, warehouse_id, current_stock, updated_at)
	SELECT 
		product_id, 
		warehouse_id, 
		SUM(current_stock) as current_stock, 
		MAX(period) as period 
	FROM temp_stock_balances
	GROUP BY product_id, warehouse_id
	ON CONFLICT (product_id, warehouse_id)
	DO UPDATE SET 
		current_stock = stock_tables.current_stock + EXCLUDED.current_stock,
		updated_at = CASE 
			WHEN EXCLUDED.updated_at > stock_tables.updated_at THEN EXCLUDED.updated_at 
			ELSE stock_tables.updated_at 
		END;
`)
	if err != nil {
		return fmt.Errorf("temp to target upsert failed: %w", err)
	}

	return tx.Commit(ctx)
}
