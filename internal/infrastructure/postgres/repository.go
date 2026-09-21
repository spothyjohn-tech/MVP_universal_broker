package postgres

import (
	"broker/internal/domain"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type poolBucket struct {
	productIDs          []string
	warehouseIDs        []string
	currentStocks       []float64
	updatedAtTimestamps []time.Time
}

type BalanceRepository struct {
	pool  *pgxpool.Pool
	bPool *sync.Pool
}

func NewBalanceRepository(pool *pgxpool.Pool) *BalanceRepository {
	return &BalanceRepository{
		pool: pool,
		bPool: &sync.Pool{
			New: func() interface{} {
				return &poolBucket{
					productIDs:          make([]string, 0, 1000),
					warehouseIDs:        make([]string, 0, 1000),
					currentStocks:       make([]float64, 0, 1000),
					updatedAtTimestamps: make([]time.Time, 0, 1000),
				}
			},
		},
	}
}

func (r *BalanceRepository) UpsertBalancesBulk(ctx context.Context, stocks []domain.StocksPayload) error {
	if len(stocks) == 0 {
		return nil
	}

	// productIDs := make([]string, len(stocks))
	// warehouseIDs := make([]string, len(stocks))
	// currentStocks := make([]float64, len(stocks))
	// updatedAtTimestamps := make([]time.Time, len(stocks))
	bucket := r.bPool.Get().(*poolBucket)
	
	if len(stocks) > len(bucket.productIDs) {
		bucket.productIDs = make([]string, len(stocks))
		bucket.warehouseIDs = make([]string, len(stocks))
		bucket.currentStocks = make([]float64, len(stocks))
		bucket.updatedAtTimestamps = make([]time.Time, len(stocks))
	}

	productIDs := bucket.productIDs[:len(stocks)]
	warehouseIDs := bucket.warehouseIDs[:len(stocks)]
	currentStocks := bucket.currentStocks[:len(stocks)]
	updatedAtTimestamps := bucket.updatedAtTimestamps[:len(stocks)]

	for i := range stocks {
		productIDs[i] = stocks[i].ProductID
		warehouseIDs[i] = stocks[i].WarehouseID
		currentStocks[i] = stocks[i].CurrentStock
		updatedAtTimestamps[i] = stocks[i].Period
	}

	// bucket.productIDs = productIDs
	// bucket.warehouseIDs = warehouseIDs
	// bucket.currentStocks = currentStocks
	// bucket.updatedAtTimestamps = updatedAtTimestamps

	defer r.bPool.Put(bucket)

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
