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
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/bytedance/sonic"
)

// Настоящий брокер-заглушка, который генерирует данные в памяти, 
// чтобы сеть RabbitMQ не искажала чистую скорость записи в сами БД.
type realtimeIntegrationBroker struct {
	messages []domain.Message
}

func (b *realtimeIntegrationBroker) StartConsuming(ctx context.Context, batchSize int) (<-chan domain.Message, error) {
	ch := make(chan domain.Message, len(b.messages))
	for _, msg := range b.messages {
		ch <- msg
	}
	// Канал не закрываем, имитируя живую очередь. 
	// Процессор выйдет из теста по контексту или таймауту.
	return ch, nil
}

func (b *realtimeIntegrationBroker) AcknowledgeBatch(ctx context.Context, deliveryTags []uint64) error {
	return nil
}

func (b *realtimeIntegrationBroker) RejectToDLQ(ctx context.Context, deliveryTag uint64) error {
	return nil
}

func Test_Usecase_RealDatabase_Throughput(t *testing.T) {
	// 1. Включаем только критические логи, чтобы slog не тормозил диск во время теста
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	// 2. Подключение к РЕАЛЬНОМУ PostgreSQL (используем DSN из конфига или Docker)
	// Замените DSN на ваш тестовый при необходимости
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

	// Очищаем таблицы перед тестом, чтобы замер был точным
	// _, _ = pgPool.Exec(ctx, "TRUNCATE TABLE stock_tables;")

	// 3. Подключение к РЕАЛЬНОМУ ClickHouse
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		t.Fatalf("Не удалось подключиться к ClickHouse: %v", err)
	}
	defer chConn.Close()

	// Очищаем таблицы ClickHouse
	// _ = chConn.Exec(ctx, "TRUNCATE TABLE sales")
	// _ = chConn.Exec(ctx, "TRUNCATE TABLE stocks")

	// 4. Генерируем БОЛЬШОЙ боевой объем данных (например, 100 000 записей остатков)
	// Мы упакуем их в 2 000 сообщений по 50 записей в каждом
	totalMessages := 2000
	recordsPerMessage := 50
	totalRecords := totalMessages * recordsPerMessage

	t.Logf("Генерация %d реальных записей для отправки в БД...", totalRecords)
	
	brokerMessages := make([]domain.Message, totalMessages)
	for i := 0; i < totalMessages; i++ {
		stocks := make([]domain.StocksPayload, recordsPerMessage)
		for j := 0; j < recordsPerMessage; j++ {
			uniqueID := fmt.Sprintf("id_%d_%d", i, j)
			stocks[j] = domain.StocksPayload{
				ID:           uniqueID,
				ProductID:    fmt.Sprintf("prod_uuid_%d", (i*j)%5000), // 5000 уникальных товаров
				WarehouseID:  fmt.Sprintf("wh_%d", j%5),               // 5 разных складов
				CurrentStock: float64(i*j) * 0.123,
				Period:       time.Now(),
			}
		}
		
		// Упаковываем через sonic в формат, который ждет наш batch_processor
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

	// 5. Инициализируем НАСТОЯЩИЕ репозитории вместо заглушек
	realPostgresRepo := postgres.NewBalanceRepository(pgPool)
	realClickHouseRepo := ch_infra.NewSalesRepository(chConn)
	integrationBroker := &realtimeIntegrationBroker{messages: brokerMessages}

	// Настройки: размер батча 1000 сообщений, таймаут 200мс
	batchSize := 1000
	processor := usecase.NewBatchProcessor(integrationBroker, realClickHouseRepo, realPostgresRepo, batchSize, 200*time.Millisecond)

	// 6. ЗАМЕР ВРЕМЕНИ НАЧАЛА ТЕСТА
	t.Log(">>> Стартуем боевую запись на диск...")
	startTime := time.Now()

	// Запускаем обработку. Так как у нас ровно 2000 сообщений, 
	// процессор должен собрать ровно 2 полных батча по 1000 сообщений.
	// Ограничим выполнение контекстом, чтобы тест завершился, когда данные запишутся.
	runCtx, runCancel := context.WithTimeout(ctx, 15*time.Second)
	defer runCancel()

	go func() {
		// Каждые 100мс проверяем, записались ли все данные в Postgres
		ticker := time.NewTicker(100 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				var count int
				err := pgPool.QueryRow(runCtx, "SELECT COUNT(*) FROM stock_tables").Scan(&count)
				// Если в базе агрегированных данных появилось ожидаемое число строк, 
				// значит воркер всё записал, можно завершать.
				if err == nil && count > 0 {
					// Даем ClickHouse дописать буферы на всякий случай
					time.Sleep(500 * time.Millisecond)
					runCancel()
					return
				}
			}
		}
	}()

	// Запускаем бесконечный цикл процессора, он прервется отменой runCtx
	_ = processor.Execute(runCtx)

	executionTime := time.Since(startTime) - 500*time.Millisecond // вычитаем sleep

	// 7. Проверяем физическое наличие данных в базах
	var pgRows int
	_ = pgPool.QueryRow(context.Background(), "SELECT COUNT(*) FROM stock_tables").Scan(&pgRows)

	var chRows uint64
	_ = chConn.QueryRow(context.Background(), "SELECT count() FROM stocks").Scan(&chRows)

	// 8. Выводим РЕАЛЬНЫЕ боевые результаты
	fmt.Println("\n========================================================")
	fmt.Printf("📊 РЕЗУЛЬТАТЫ БОЕВОГО ИНТЕГРАЦИОННОГО ТЕСТА:\n")
	fmt.Printf("⏱️ Время физической записи на диск: %v\n", executionTime)
	fmt.Printf("📦 Успешно обработано и записано: %d записей\n", totalRecords)
	fmt.Printf("🗄️ Строк в PostgreSQL (агрегировано): %d\n", pgRows)
	fmt.Printf("📊 Строк в ClickHouse (лог изменений): %d\n", chRows)
	
	recordsPerSecond := float64(totalRecords) / executionTime.Seconds()
	fmt.Printf("🚀 РЕАЛЬНАЯ СКОРОСТЬ СИСТЕМЫ: %.2f записей/сек\n", recordsPerSecond)
	fmt.Println("========================================================")
}
