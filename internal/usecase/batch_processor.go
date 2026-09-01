package usecase

import (
	"broker/internal/domain"
	"context"
	"log/slog"
	"time"
)


type BatchProcessor struct {
	broker    domain.MessageBroker
	salesRepo domain.SalesRepository
	balRepo   domain.BalanceRepository
	batchSize int
	timeout time.Duration
}

func NewBatchProcessor(
	broker domain.MessageBroker,
	salesRepo domain.SalesRepository,
	balRepo domain.BalanceRepository,
	batchSize int,
	timeout time.Duration,
) *BatchProcessor {
	return &BatchProcessor{
		broker:    broker,
		salesRepo: salesRepo,
		balRepo:   balRepo,
		batchSize: batchSize,
		timeout: timeout,
	}
}

func (up *BatchProcessor) Execute(ctx context.Context) error {
	for{
		select{
		case <-ctx.Done():
			return ctx.Err()
		default:
			batchData, deliveryTags, err := up.broker.ConsumeBatch(ctx, up.batchSize, up.timeout)
			if err != nil {
				slog.Error("Failed to fetch processed batch from queue", "err", err)
				continue
			}
			if len(deliveryTags) == 0 {
				continue
			}

			// 2. Потоковая вставка лога транзакций в ClickHouse
			if err := up.salesRepo.SaveSalesBulk(ctx, batchData.Sales); err != nil {
				slog.Error("ClickHouse stream write sales batch failed", "err", err)
				continue
			}
			if err := up.salesRepo.SaveStocksBulk(ctx, batchData.Stocks); err != nil {
				slog.Error("ClickHouse stream write stocks batch failed", "err", err)
				continue
			}

			// 3. Бинарный высокоскоростной Bulk UPSERT текущих остатков в PostgreSQL
			if err := up.balRepo.UpsertBalancesBulk(ctx, batchData.Stocks); err != nil {
				slog.Error("Postgres unnest bulk upsert failed", "err", err)
				continue
			}

			// 4. Пакетный коммит транзакции в RabbitMQ (гарантирует At-Least-Once обработку)
			if err := up.broker.AcknowledgeBatch(ctx, deliveryTags); err != nil {
				slog.Error("Failed to acknowledge batch in RabbitMQ", "err", err)
			}
		}
	}
}
