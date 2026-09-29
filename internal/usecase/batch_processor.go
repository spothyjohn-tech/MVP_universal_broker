package usecase

import (
	"broker/internal/domain"
	"context"
	"log/slog"
	"time"

	"github.com/bytedance/sonic"
	"golang.org/x/sync/errgroup"
)

type rawMessage struct {
	Action  string `json:"action"`
	Payload sonic.NoCopyRawMessage `json:"payload"`
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
			batchData, deliveryTags, dlqTags, err := up.collectBatch(ctx, deliveries)
			if err != nil {
				return err
			}
			
			if len(dlqTags) > 0 {
				if err := up.broker.RejectBatchToDLQ(ctx, dlqTags); err != nil {
					slog.Error("Failed to reject corrupt messages to DLQ", "err", err)
				}
			}

			if len(deliveryTags) == 0 {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}

			g, gCtx := errgroup.WithContext(ctx)

			// Flush accumulated memory frames downstream to analytical and transaction databases.
			if len(batchData.Sales) > 0 {
				g.Go(func() error{
					if err := up.salesRepo.SaveSalesBulk(gCtx, batchData.Sales); err != nil {
						slog.Error("Pipeline execution failure: ClickHouse sales save failed", "err", err)
						return err
					}
					return nil
				})
			}
			if len(batchData.Stocks) > 0 {
				g.Go(func() error{
					if err := up.salesRepo.SaveStocksBulk(gCtx, batchData.Stocks); err != nil {
						slog.Error("Pipeline execution failure: ClickHouse sales save failed", "err", err)
						return err
					}
					return nil
				})
				g.Go(func() error {
					if err := up.balRepo.UpsertBalancesBulk(gCtx, batchData.Stocks); err != nil {
						slog.Error("Pipeline execution failure: PostgreSQL unnest state upsert failed", "err", err)
						return err
					}
					return nil
				})
			}
			if err := g.Wait(); err != nil {
				_ = up.broker.NackBatchForRetry(ctx, deliveryTags)
				time.Sleep(1 * time.Second)
				continue
			}
			if err := up.broker.AcknowledgeBatch(ctx, deliveryTags); err != nil {
				slog.Error("Transport notification failure: message batch confirmation failed", "err", err)
			} else {
				slog.Info("Batch successfully processed and committed", 
				"sales_processed", len(batchData.Sales), 
				"stocks_processed", len(batchData.Stocks), 
				"messages_acked", len(deliveryTags))
			}
		}
	}
}

// collectBatch aggregates incoming messages into a single transaction payload.
// It handles schema validation and routes malformed payloads directly to the DLQ.
func (up *BatchProcessor) collectBatch(ctx context.Context, deliveries <-chan domain.Message) (*domain.BatchData, []uint64, []uint64, error) {
	batchData := &domain.BatchData{
		Sales:  make([]domain.SalesPayload, 0, up.batchSize),
		Stocks: make([]domain.StocksPayload, 0, up.batchSize),
	}

	deliveryTags := make([]uint64, 0, up.batchSize)
	dlqTags := make([]uint64, 0, up.batchSize)

	timer := time.NewTimer(up.timeout)
	defer timer.Stop()

	var corruptRootCount uint64
	var corruptInnerCount uint64

	for {
		select {
		case <-ctx.Done():
			up.logSkippedErrors(corruptRootCount, corruptInnerCount)
			return batchData, deliveryTags, dlqTags, nil
		case msg, ok := <-deliveries:
			if !ok {
				slog.Error("Transport stream disconnected: downstream network frame channel closed abruptly")
				return batchData, deliveryTags, dlqTags, context.Canceled
			}

			var raw rawMessage
			// Safety check: prevent unparsed JSON from causing memory hangs by instantly sending it to DLQ.
			if err := sonic.Unmarshal(msg.Body, &raw); err != nil {
				corruptRootCount++
				dlqTags = append(dlqTags, msg.DeliveryTag)
				continue
			}

			switch raw.Action {
			case "insert_sales":
				var wireData []domain.SalesPayload
				if err := sonic.Unmarshal(raw.Payload, &wireData); err != nil {
					corruptInnerCount++
					dlqTags = append(dlqTags, msg.DeliveryTag)
					continue
				}
				for i := range wireData {
					wireData[i].Count = domain.RoundToTwoDecimal(wireData[i].Count)
					wireData[i].Price = domain.RoundToTwoDecimal(wireData[i].Price)
				}
				batchData.Sales = append(batchData.Sales, wireData...)
				deliveryTags = append(deliveryTags, msg.DeliveryTag)

			case "insert_stocks":
				var stocks []domain.StocksPayload
				if err := sonic.Unmarshal(raw.Payload, &stocks); err != nil {
					corruptInnerCount++
					dlqTags = append(dlqTags, msg.DeliveryTag)
					continue
				}
				// Fix numeric precision anomalies prior to making memory additions.
				for i := range stocks {
					stocks[i].CurrentStock = domain.RoundToTwoDecimal(stocks[i].CurrentStock)
				}
				batchData.Stocks = append(batchData.Stocks, stocks...)
				deliveryTags = append(deliveryTags, msg.DeliveryTag)
			default:
				dlqTags = append(dlqTags, msg.DeliveryTag)
			}

			if len(deliveryTags) >= up.batchSize {
				return batchData, deliveryTags, dlqTags, nil
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
			if len(deliveryTags) > 0 || len(dlqTags) > 0 {
				return batchData, deliveryTags, dlqTags, nil
			}
			timer.Reset(up.timeout)
		}
	}
}

func (up *BatchProcessor) logSkippedErrors(root, inner uint64) {
	if root > 0 {
		slog.Error("Batch data validation failure: telemetry frames skipped due to corrupted root JSON structure", "skipped_count", root)
	}
	if inner > 0 {
		slog.Error("Batch data validation failure: entities skipped due to invalid inner payload layout", "skipped_count", inner)
	}
}