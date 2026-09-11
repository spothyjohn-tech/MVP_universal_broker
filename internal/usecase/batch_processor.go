package usecase

import (
	"broker/internal/domain"
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

type rawMessage struct {
	Action  string `json:"action"`
	Payload string `json:"payload"`
}

// BatchProcessor manages stream ingestion pipelines by grouping incoming signals into atomic blocks.
type BatchProcessor struct {
	broker    domain.MessageBroker
	salesRepo domain.SalesRepository
	balRepo   domain.BalanceRepository
	batchSize int
	timeout   time.Duration
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
		timeout:   timeout,
	}
}

// Execute orchestrates the main event loop. It guarantees zero data loss during worker teardown.
func (up *BatchProcessor) Execute(ctx context.Context) error {
	deliveries, err := up.broker.StartConsuming(ctx, up.batchSize)
	if err != nil {
		slog.Error("Critical initialization error: failed to initialize broker consumer stream", "err", err)
		return err
	}

	for {
		select {
		// Cooperative context checking to enforce structured task cancellation.
		case <-ctx.Done():
			slog.Info("Termination signal received. Gracefully stopping usecase processor execution loop.")
			return ctx.Err()
		default:
		}
		batchData, deliveryTags := up.collectBatch(ctx, deliveries)
		if len(deliveryTags) == 0 {
			if ctx.Err() != nil {
				slog.Error("Воркер остановлен: сетевой канал RabbitMQ мертв. Проверьте имя очереди или логи брокера!", "err", err)
				return err
			}
			continue
		}

		// Flush accumulated memory frames downstream to analytical and transaction databases.
		if err := up.salesRepo.SaveSalesBulk(ctx, batchData.Sales); err != nil {
			slog.Error("Pipeline execution failure: ClickHouse sales bulk persistence failed", "err", err)
			continue
		}
		if err := up.salesRepo.SaveStocksBulk(ctx, batchData.Stocks); err != nil {
			slog.Error("Pipeline execution failure: ClickHouse stocks bulk persistence failed", "err", err)
			continue
		}
		if err := up.balRepo.UpsertBalancesBulk(ctx, batchData.Stocks); err != nil {
			slog.Error("Pipeline execution failure: PostgreSQL unnest state upsert failed", "err", err)
			continue
		}
		if err := up.broker.AcknowledgeBatch(ctx, deliveryTags); err != nil {
			slog.Error("Transport notification failure: message batch confirmation failed", "err", err)
		}
	}
}

// collectBatch aggregates incoming messages into a single transaction payload.
// It handles schema validation and routes malformed payloads directly to the DLQ.
func (up *BatchProcessor) collectBatch(ctx context.Context, deliveries <-chan domain.Message) (*domain.BatchData, []uint64) {
	batchData := &domain.BatchData{
		Sales:  make([]domain.SalesPayload, 0, up.batchSize),
		Stocks: make([]domain.StocksPayload, 0, up.batchSize),
	}
	deliveryTags := make([]uint64, 0, up.batchSize)

	timer := time.NewTimer(up.timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return batchData, deliveryTags

		case msg, ok := <-deliveries:
			if !ok {
				slog.Error("Transport stream disconnected: downstream network frame channel closed abruptly")
				return batchData, deliveryTags
			}

			var raw rawMessage
			// Safety check: prevent unparsed JSON from causing memory hangs by instantly sending it to DLQ.
			if err := json.Unmarshal(msg.Body, &raw); err != nil {
				slog.Error("Data validation failure: corrupted root JSON scheme structure. Isolation via routing to DLQ.", "err", err)
				_ = up.broker.RejectToDLQ(ctx, msg.DeliveryTag)
				if len(deliveryTags) >= up.batchSize {
					return batchData, deliveryTags
				}
				continue
			}
			
			deliveryTags = append(deliveryTags, msg.DeliveryTag)

			switch raw.Action {
			case "insert_sales":
				var wireData []domain.SalesPayload
				if err := json.Unmarshal([]byte(raw.Payload), &wireData); err != nil {
					slog.Error("Data validation failure: invalid inner SalesPayload array layout. Routing frame to DLQ.", "err", err)
					_ = up.broker.RejectToDLQ(ctx, msg.DeliveryTag)
					continue
				}
				for i := range wireData {
					wireData[i].Count = domain.RoundToTwoDecimal(wireData[i].Count)
					wireData[i].Price = domain.RoundToTwoDecimal(wireData[i].Price)
				}
				batchData.Sales = append(batchData.Sales, wireData...)
			case "insert_stocks":
				var stocks []domain.StocksPayload
				if err := json.Unmarshal([]byte(raw.Payload), &stocks); err != nil {
					slog.Error("Data validation failure: invalid inner StocksPayload array layout. Routing frame to DLQ.", "err", err)
					_ = up.broker.RejectToDLQ(ctx, msg.DeliveryTag)
					continue
				}
				// Fix numeric precision anomalies prior to making memory additions.
				for i := range stocks {
					stocks[i].CurrentStock = domain.RoundToTwoDecimal(stocks[i].CurrentStock)
				}
				batchData.Stocks = append(batchData.Stocks, stocks...)
			}
			if len(deliveryTags) >= up.batchSize {
				return batchData, deliveryTags
			}

			// Idiomatic, non-blocking drainage routine to safely reset Go runtime time.Timer entities.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(up.timeout)

		case <-timer.C:
			// Timeout limit reached. Yield whatever segment has been stored in memory.
			if len(deliveryTags) > 0 {
				return batchData, deliveryTags
			}
			timer.Reset(up.timeout)
		}
	}
}
