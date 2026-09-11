// Package domain contains core business entities and technology-agnostic interfaces.
package domain

import (
	"context"
)

// Message represents the internal decoupled event contract inside the UseCase layer.
type Message struct {
	Body        []byte
	DeliveryTag uint64
}

// MessageBroker defines the behavior for the asynchronous message queue.
// It decouples the core business logic from specific transport implementations like RabbitMQ.
type MessageBroker interface {
	StartConsuming(ctx context.Context, batchSize int) (<-chan Message, error)
	AcknowledgeBatch(ctx context.Context, deliveryTags []uint64) error
	RejectToDLQ(ctx context.Context, deliveryTag uint64) error
}

// SalesRepository defines storage contracts for raw, immutable analytical records.
type SalesRepository interface {
	SaveSalesBulk(ctx context.Context, sales []SalesPayload) error
	SaveStocksBulk(ctx context.Context, stocks []StocksPayload) error
}

// BalanceRepository defines storage contracts for live operational aggregates.
type BalanceRepository interface {
	UpsertBalancesBulk(ctx context.Context, stocks []StocksPayload) error
}
