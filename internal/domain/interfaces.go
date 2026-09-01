package domain

import (
	"context"
	"time"
)

type MessageBroker interface {
	ConsumeBatch(ctx context.Context, batchSize int, timeout time.Duration) (*BatchData, []uint64, error)
	AcknowledgeBatch(ctx context.Context, deliveryTags []uint64) error
}

type SalesRepository interface {
	SaveSalesBulk(ctx context.Context, sales []SalesPayload) error
	SaveStocksBulk(ctx context.Context, stocks []StocksPayload) error
}

type BalanceRepository interface {
	UpsertBalancesBulk(ctx context.Context, stocks []StocksPayload) error
}
