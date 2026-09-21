package usecase_test

import (
	"broker/internal/domain"
	"broker/internal/usecase"
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bytedance/sonic"
)

// mockSalesRepository для бенчмарка (имитирует ClickHouse)
type benchSalesRepo struct{}

func (r benchSalesRepo) SaveSalesBulk(ctx context.Context, sales []domain.SalesPayload) error {
	// Имитируем чтение данных (перебор), чтобы компилятор не оптимизировал код "в никуда"
	for i := range sales {
		_ = sales[i].ID
	}
	return nil
}

func (r benchSalesRepo) SaveStocksBulk(ctx context.Context, stocks []domain.StocksPayload) error {
	for i := range stocks {
		_ = stocks[i].ID
	}
	return nil
}

// mockBalanceRepository для бенчмарка (имитирует Postgres с sync.Pool)
type benchBalanceRepo struct{}

func (r benchBalanceRepo) UpsertBalancesBulk(ctx context.Context, stocks []domain.StocksPayload) error {
	// Имитируем проход по батчу, как это происходит при подготовке unnest
	for i := range stocks {
		_ = stocks[i].ProductID
		_ = stocks[i].WarehouseID
	}
	return nil
}

// Потоковый брокер, непрерывно отдающий валидные JSON-сообщения
type continuousBenchBroker struct {
	msgData []byte
}

func (b *continuousBenchBroker) StartConsuming(ctx context.Context, batchSize int) (<-chan domain.Message, error) {
	ch := make(chan domain.Message, batchSize)
	go func() {
		var msgTag uint64 = 1
		for {
			select {
			case <-ctx.Done():
				close(ch)
				return
			default:
				ch <- domain.Message{
					Body:        b.msgData,
					DeliveryTag: msgTag,
				}
				msgTag++
			}
		}
	}()
	return ch, nil
}

func (b *continuousBenchBroker) AcknowledgeBatch(ctx context.Context, deliveryTags []uint64) error {
	return nil
}

func (b *continuousBenchBroker) RejectToDLQ(ctx context.Context, deliveryTag uint64) error {
	return nil
}

// Вспомогательная функция генерации тестового JSON через sonic без лишних аллокаций
func generatePerfectBenchJSON(action string, count int) []byte {
	type rawMessage struct {
		Action  string          `json:"action"`
		Payload sonic.NoCopyRawMessage `json:"payload"`
	}

	stocks := make([]domain.StocksPayload, count)
	for i := 0; i < count; i++ {
		stocks[i] = domain.StocksPayload{
			ID:           fmt.Sprintf("id_stock_%d", i),
			ProductID:    fmt.Sprintf("prod_uuid_%d", i%10),
			WarehouseID:  "warehouse_central_zone_1",
			CurrentStock: 1250.4567, // Будет округляться в usecase
			Period:       time.Now(),
		}
	}

	payloadBytes, _ := sonic.Marshal(stocks)
	raw := rawMessage{
		Action:  action,
		Payload: payloadBytes,
	}
	rawBytes, _ := sonic.Marshal(raw)
	return rawBytes
}

// Главный бенчмарк производительности всей цепочки обработки
func Benchmark_NewSystem_PipelineThroughput(b *testing.B) {
	// 1. Отключаем логи глобально один раз
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jsonData := generatePerfectBenchJSON("insert_stocks", 50)
	broker := &continuousBenchBroker{msgData: jsonData}
	salesRepo := benchSalesRepo{}
	balRepo := benchBalanceRepo{}

	batchSize := 1000
	timeout := 5 * time.Millisecond
	
	// 2. Инициализируем процессор ДО цикла бенчмарка
	processor := usecase.NewBatchProcessor(broker, salesRepo, balRepo, batchSize, timeout)

	b.ReportAllocs()
	b.ResetTimer()

	// 3. Запускаем процессор в бэкграунде на весь период теста
	go func() {
		_ = processor.Execute(ctx)
	}()

	// В самом бенчмарке просто даем ему поработать N итераций
	for i := 0; i < b.N; i++ {
		time.Sleep(100 * time.Microsecond) // имитируем интервал
	}
}
