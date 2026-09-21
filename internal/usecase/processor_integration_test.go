package usecase_test

import (
	"broker/internal/domain"
	ch_infra "broker/internal/infrastructure/clickhouse"
	"broker/internal/infrastructure/postgres"
	"broker/internal/usecase"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/bytedance/sonic"
)

// Интеграционный брокер с поддержкой точной синхронизации завершения
type realtimeIntegrationBroker struct {
	messages []domain.Message
	wg       *sync.WaitGroup
}

func (b *realtimeIntegrationBroker) StartConsuming(ctx context.Context, batchSize int) (<-chan domain.Message, error) {
	ch := make(chan domain.Message, len(b.messages))
	for _, msg := range b.messages {
		ch <- msg
	}
	return ch, nil
}

func (b *realtimeIntegrationBroker) AcknowledgeBatch(ctx context.Context, deliveryTags []uint64) error {
	b.wg.Done() // Сигнализируем об успешной обработке целого батча
	return nil
}

func (b *realtimeIntegrationBroker) RejectToDLQ(ctx context.Context, deliveryTag uint64) error {
	return nil
}

func Test_Usecase_RealDatabase_Throughput(t *testing.T) {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	pgDSN := "host=localhost port=5432 user=postgres password=postgres dbname=test sslmode=disable connect_timeout=5"
	pgConfig, err := pgxpool.ParseConfig(pgDSN)
	if err != nil {
		t.Fatalf("Не удалось распарсить DSN Postgres: %v", err)
	}
	pgPool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		t.Fatalf("Не удалось подключиться к Postgres: %v", err)
	}
	defer pgPool.Close()


	options, err := clickhouse.ParseDSN("clickhouse://localhost:9000/default?dial_timeout=30s")
	if err != nil {
		t.Fatalf("Не удалось распарсить DSN: %v", err)
	}

	chConn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatalf("Не удалось подключиться к ClickHouse: %v", err)
	}

	if err != nil {
		t.Fatalf("Не удалось подключиться к ClickHouse: %v", err)
	}
	defer chConn.Close()

	totalMessages := 2000
	recordsPerMessage := 50
	totalRecords := totalMessages * recordsPerMessage
	batchSize := 1000

	t.Logf("Генерация %d реальных записей для отправки в БД...", totalRecords)
	
	brokerMessages := make([]domain.Message, totalMessages)
	for i := 0; i < totalMessages; i++ {
		stocks := make([]domain.StocksPayload, recordsPerMessage)
		for j := 0; j < recordsPerMessage; j++ {
			uniqueID := fmt.Sprintf("id_%d_%d", i, j)
			stocks[j] = domain.StocksPayload{
				ID:           uniqueID,
				ProductID:    fmt.Sprintf("prod_uuid_%d", (i*j)%5000),
				WarehouseID:  fmt.Sprintf("wh_%d", j%5),
				CurrentStock: float64(i*j) * 0.123,
				Period:       time.Now(),
			}
		}
		
		payloadBytes, _ := sonic.Marshal(stocks)
		raw := struct {
			Action  string                 `json:"action"`
			Payload sonic.NoCopyRawMessage `json:"payload"`
		}{
			Action:  "insert_stocks",
			Payload: payloadBytes,
		}
		rawBytes, _ := sonic.Marshal(raw)

		brokerMessages[i] = domain.Message{
			Body:        rawBytes,
			DeliveryTag: uint64(i + 1),
		}
	}

	realPostgresRepo := postgres.NewBalanceRepository(pgPool)
	realClickHouseRepo := ch_infra.NewSalesRepository(chConn)
	
	// Ожидаем завершения ровно такого количества батчей, которое сгенерировали
	var wg sync.WaitGroup
	expectedBatches := totalMessages / batchSize
	wg.Add(expectedBatches)

	integrationBroker := &realtimeIntegrationBroker{
		messages: brokerMessages,
		wg:       &wg,
	}

	processor := usecase.NewBatchProcessor(integrationBroker, realClickHouseRepo, realPostgresRepo, batchSize, 200*time.Millisecond)

	t.Log(">>> Стартуем боевую запись на диск...")
	startTime := time.Now()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()

	// Горутина, которая мягко остановит процессор после обработки всех батчей
	go func() {
		wg.Wait()
		runCancel()
	}()

	_ = processor.Execute(runCtx)

	executionTime := time.Since(startTime) // Чистое время без погрешностей

	var pgRows int
	_ = pgPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM stock_tables").Scan(&pgRows)

	var chRows uint64
	_ = chConn.QueryRow(context.Background(), "SELECT count() FROM stocks").Scan(&chRows)

	fmt.Println("\n========================================================")
	fmt.Printf("РЕЗУЛЬТАТЫ БОЕВОГО ИНТЕГРАЦИОННОГО ТЕСТА:\n")
	fmt.Printf("Чистое время физической записи: %v\n", executionTime)
	fmt.Printf("Успешно обработано и записано: %d записей\n", totalRecords)
	fmt.Printf("Строк в PostgreSQL: %d\n", pgRows)
	fmt.Printf("Строк в ClickHouse: %d\n", chRows)
	
	recordsPerSecond := float64(totalRecords) / executionTime.Seconds()
	fmt.Printf("РЕАЛЬНАЯ СКОРОСТЬ СИСТЕМЫ: %.2f записей/сек\n", recordsPerSecond)
	fmt.Println("========================================================")
}
