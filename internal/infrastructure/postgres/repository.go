package postgres

import (
	"broker/internal/domain"
	"context"
	"fmt"
	"time"

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

	productIDs := make([]string, len(stocks))
	warehouseIDs := make([]string, len(stocks))
	currentStocks := make([]float64, len(stocks)) 
	updatedAtTimestamps := make([]time.Time, len(stocks))

	for i := range stocks {
		productIDs[i] = stocks[i].ProductID
		warehouseIDs[i] = stocks[i].WarehouseID
		currentStocks[i] = stocks[i].CurrentStock
		updatedAtTimestamps[i] = stocks[i].Period
	}

	query := `
		INSERT INTO stock_tables (product_id, warehouse_id, current_stock, updated_at)
		SELECT p, w, SUM(s), MAX(u)
		FROM unnest($1::varchar[], $2::varchar[], $3::numeric[], $4::timestamp[]) 
		AS data(p, w, s, u)
		GROUP BY p, w
		ON CONFLICT (product_id, warehouse_id) 
		DO UPDATE SET 
			current_stock = stock_tables.current_stock + EXCLUDED.current_stock,
			updated_at = CASE 
				WHEN EXCLUDED.updated_at > stock_tables.updated_at THEN EXCLUDED.updated_at
				ELSE stock_tables.updated_at
			END;
	`
	_, err := r.pool.Exec(ctx, query, productIDs, warehouseIDs, currentStocks, updatedAtTimestamps)
	if err != nil {
		return fmt.Errorf("bulk upsert via unnest failed: %w", err)
	}

	return nil
}
