package clickhouse_test

import (
	"context"
	"fmt"
	"testing"
	"time"
    "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"

	ch_infra "broker/internal/infrastructure/clickhouse"
	pg_infra "broker/internal/infrastructure/postgres"
	"broker/internal/domain"
)
// Генерируем "горячую пачку" данных в RAM, чтобы полностью исключить диск и I/O задержки
func prepareHotInMemoryBatch(size int) ([]domain.SalesPayload, []domain.StocksPayload) {
	sales := make([]domain.SalesPayload, size)
	stocks := make([]domain.StocksPayload, size)
	now := time.Now()

	for i := 0; i < size; i++ {
		pID := fmt.Sprintf("prod_%d", i%10000) // 10k уникальных товаров
		wID := fmt.Sprintf("wh_%d", i%50)     // 50 складов
		
		sales[i] = domain.SalesPayload{
			ID:          fmt.Sprintf("s_%d", i),
			ProductID:   pID,
			ClientId:    "client_enterprise_1С",
			WarehouseID: wID,
			Count:       2,
			Price:       340.5,
			Period:      now,
		}
		stocks[i] = domain.StocksPayload{
			ID:           fmt.Sprintf("st_%d", i),
			ProductID:    pID,
			WarehouseID:  wID,
			CurrentStock: 10,
			Period:       now,
		}
	}
	return sales, stocks
}

// БЕНЧМАРК 1: Измерение чистой скорости параллельной Highload-записи батчами (Тест на 50 млн строк)
func Benchmark_MaxThroughputIngestion_50M(b *testing.B) {
	ctx := context.Background()

	// Нативные пулы из инфраструктурного слоя, аналогично main.go
	pgPool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/test?sslmode=disable&pool_max_conns=40")
	if err != nil {
		b.Fatalf("Postgres connection pool failed: %v", err)
	}
	defer pgPool.Close()

	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{Database: "default"},
	})
	if err != nil {
		b.Fatalf("ClickHouse connection failed: %v", err)
	}
	defer chConn.Close()

	// Инициализируем репозитории
	pgRepo := pg_infra.NewBalanceRepository(pgPool)
	chRepo := ch_infra.NewSalesRepository(chConn)

	// Аллоцируем тестовый батч в RAM (размер пачки 50 000 строк)
	chunkSize := 50000
	salesBatch, stocksBatch := prepareHotInMemoryBatch(chunkSize)

	// Сбрасываем таймер: время подготовки данных в RAM не должно портить статистику чистой записи в СУБД
	b.ResetTimer() 

	// Запускаем параллельные потоки, симулируя конкурентную работу горутин UseCase
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Замеряем нативный бинарный COPY в Postgres временную таблицу + UPSERT
			if err := pgRepo.UpsertBalancesBulk(ctx, stocksBatch); err != nil {
				b.Fatalf("Postgres Bulk UPSERT via CopyFrom failed: %v", err)
			}
			// Замеряем бинарный PrepareBatch в ClickHouse
			if err := chRepo.SaveSalesBulk(ctx, salesBatch); err != nil {
				b.Fatalf("Clickhouse PrepareBatch Send failed: %v", err)
			}
		}
	})
}

// БЕНЧМАРК 2: Аналитическое агрегирование из ClickHouse -> Высокоскоростной экспорт в Postgres
func Benchmark_ClickHouseAnalyticsToPostgresUpsert(b *testing.B) {
	ctx := context.Background()

	chConn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"localhost:9000"}})
	if err != nil {
		b.Fatalf("ClickHouse connection failed: %v", err)
	}
	defer chConn.Close()

	pgPool, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/test?sslmode=disable")
	if err != nil {
		b.Fatalf("Postgres connection pool failed: %v", err)
	}
	defer pgPool.Close()

	pgRepo := pg_infra.NewBalanceRepository(pgPool)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// ClickHouse выполняет тяжелую агрегацию по миллионам строк мгновенно в RAM
		query := `
		SELECT product_id, warehouse_id, toInt32(SUM(count)), max(period) 
		FROM sales 
		GROUP BY product_id, warehouse_id`
		rows, err := chConn.Query(ctx, query)
		if err != nil {
			b.Fatalf("ClickHouse analytical query failed: %v", err)
		}

		var aggregatedStocks []domain.StocksPayload
		for rows.Next() {
			var pID, wID string
			var totalCount int32
			var maxPeriod time.Time
			
			if err := rows.Scan(&pID, &wID, &totalCount, &maxPeriod); err != nil {
				rows.Close()
				b.Fatalf("Rows scan failed: %v", err)
			}

			// Формируем чистый доменный слайс остатков для Postgres
			aggregatedStocks = append(aggregatedStocks, domain.StocksPayload{
				ProductID:    pID,
				WarehouseID:  wID,
				CurrentStock: totalCount,
				Period:       maxPeriod, // Сохраняем точный исторический таймштамп для логики UPSERT!
			})
		}
		rows.Close()

		// Пишем пачку сагрегированных результатов в Postgres через COPY протокол pgx
		if len(aggregatedStocks) > 0 {
			if err := pgRepo.UpsertBalancesBulk(ctx, aggregatedStocks); err != nil {
				b.Fatalf("Postgres unlogged temp table upsert failed: %v", err)
			}
		}
	}
}
