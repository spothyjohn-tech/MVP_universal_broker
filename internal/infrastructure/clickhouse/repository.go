package clickhouse

import (
	"context"
	"fmt"
	"broker/internal/domain"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type SalesRepository struct {
	conn clickhouse.Conn
}

func NewSalesRepository(conn clickhouse.Conn) *SalesRepository {
	return &SalesRepository{conn: conn}
}

func (r *SalesRepository) SaveSalesBulk(ctx context.Context, sales []domain.SalesPayload) error {
	if len(sales) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO sales (id, product_id, client_id, warehouse_id, count, price, period)")
	if err != nil {
		return fmt.Errorf("ch prepare sales batch failed: %w", err)
	}

	for i := range sales {
		var clientID interface{} = nil
		if sales[i].ClientId != "" {
			clientID = sales[i].ClientId
		}
		err = batch.Append(sales[i].ID, sales[i].ProductID, clientID, sales[i].WarehouseID, sales[i].Count, sales[i].Price, sales[i].Period)
		if err != nil {
			return fmt.Errorf("ch append sales row failed: %w", err)
		}
	}
	return batch.Send()
}

func (r *SalesRepository) SaveStocksBulk(ctx context.Context, stocks []domain.StocksPayload) error {
	if len(stocks) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx, "INSERT INTO stocks (id, product_id, warehouse_id, current_stock, period)")
	if err != nil {
		return fmt.Errorf("ch prepare stocks batch failed: %w", err)
	}
	for i := range stocks {
		err := batch.Append(stocks[i].ID, stocks[i].ProductID, stocks[i].WarehouseID, stocks[i].CurrentStock, stocks[i].Period)
		if err != nil {
			return fmt.Errorf("ch append stocks row failed: %w", err)
		}
	}
	return batch.Send()
}
