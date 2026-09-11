package usecase_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"broker/internal/domain"
	"broker/internal/usecase"
)


type mockSalesRepository struct{}
func (m mockSalesRepository) SaveSalesBulk(ctx context.Context, sales []domain.SalesPayload) error { return nil }
func (m mockSalesRepository) SaveStocksBulk(ctx context.Context, stocks []domain.StocksPayload) error { return nil }


type mockBalanceRepository struct{}
func (m mockBalanceRepository) UpsertBalancesBulk(ctx context.Context, stocks []domain.StocksPayload) error { return nil }


type benchBroker struct {
	msgData []byte
}

func (b *benchBroker) StartConsuming(ctx context.Context, batchSize int) (<-chan domain.Message, error) {
	ch := make(chan domain.Message, batchSize)
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(ch)
				return
			default:
				ch <- domain.Message{
					Body:        b.msgData,
					DeliveryTag: 1,
				}
			}
		}
	}()
	return ch, nil
}
func (b *benchBroker) AcknowledgeBatch(ctx context.Context, deliveryTags []uint64) error { return nil }
func (b *benchBroker) RejectToDLQ(ctx context.Context, deliveryTag uint64) error         { return nil }

func createBenchJSON() []byte {
	type rawMessage struct {
		Action  string `json:"action"`
		Payload string `json:"payload"`
	}
	stocks := make([]domain.StocksPayload, 50)
	for i := 0; i < 50; i++ {
		stocks[i] = domain.StocksPayload{
			ID:           fmt.Sprintf("st_%d", i),
			ProductID:    fmt.Sprintf("prod_%d", i%10),
			WarehouseID:  "wh_main",
			CurrentStock: 452.12345, 
			Period:       time.Now(),
		}
	}
	payloadBytes, _ := json.Marshal(stocks)
	
	raw := rawMessage{
		Action:  "insert_stocks",
		Payload: string(payloadBytes),
	}
	rawBytes, _ := json.Marshal(raw)
	return rawBytes
}

func Benchmark_Processor_MaxThroughput(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rawJSON := createBenchJSON()
	broker := &benchBroker{msgData: rawJSON}
	salesRepo := mockSalesRepository{}
	balRepo := mockBalanceRepository{}
	
	processor := usecase.NewBatchProcessor(broker, salesRepo, balRepo, 1000, 10*time.Millisecond)

	b.ResetTimer()

	for b.Loop() {
		subCtx, subCancel := context.WithTimeout(ctx, 50*time.Millisecond)
		_ = processor.Execute(subCtx)
		subCancel()
	}
}
