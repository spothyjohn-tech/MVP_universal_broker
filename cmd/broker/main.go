package main

import (
	"broker/internal/domain"
	ch_infra "broker/internal/infrastructure/clickhouse"
	"broker/internal/infrastructure/postgres"
	"broker/internal/infrastructure/rabbitmq"
	"broker/internal/usecase"
	"broker/lib/logger"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	logFile, err := logger.InitLogger("app.log")
	if err != nil {
		os.Exit(1)
	}
	defer logFile.Close()

	slog.Info("Starting Ingestion Worker Service Node...")
	
	// Initialize core application configuration layers using cleanenv.
	cfg, err := domain.LoadConfig()
	if err != nil {
		slog.Error("Process boot failure: environment configuration configuration parsing crashed", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pgPool := initPostgresWithRetry(ctx, cfg.Postgres)
	if pgPool == nil {
		return
	}
	defer pgPool.Close()

	chConn := initClickhouseWithRetry(ctx, cfg.ClickHouse)
	if chConn == nil {
		return
	}
	defer chConn.Close()

	postgresRepo := postgres.NewBalanceRepository(pgPool)
	clickhouseRepo := ch_infra.NewSalesRepository(chConn)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		runWorkerLoop(ctx, cfg, clickhouseRepo, postgresRepo)
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	slog.Info("Termination interceptor triggered. Shutting down streaming infrastructure cleanly...")
	
	cancel()  
	wg.Wait() 

	slog.Info("Ingestion server node completely stopped. Resources detached successfully.")
}

func runWorkerLoop(ctx context.Context, cfg *domain.AppConfig, clichouseRepo domain.SalesRepository, postgresRepo domain.BalanceRepository) {
	for {
		if ctx.Err() != nil {
			return
		}
		slog.Info("Attempting to connect to RabbitMQ...")
		rmqConn, err := amqp.Dial(cfg.RabbitMQ.URL())
		if err != nil {
			slog.Error("RabbitMQ dial failed, retrying in 5s...", "err", err)
			if !sleepContext(ctx, 5*time.Second) { return }
			continue
		}
		rmqCh, err := rmqConn.Channel()
		if err != nil {
			slog.Error("RabbitMQ channel creation failed, retrying...", "err", err)
			rmqConn.Close()
			if !sleepContext(ctx, 5*time.Second) { return }
			continue
		}
		rabbitmqBroker, err := rabbitmq.NewRabbitMQBroker(rmqCh, "1c_requests")
		if err != nil {
			slog.Error("RabbitMQ channel creation failed, retrying...", "err", err)
			rmqConn.Close()
			if !sleepContext(ctx, 5*time.Second) { return }
			continue
		}
		processor := usecase.NewBatchProcessor(
			rabbitmqBroker,
			clichouseRepo,
			postgresRepo,
			cfg.Batch.Size,
			cfg.Batch.Timeout,
		)
		slog.Info("Highload Clean Architecture Worker kernel successfully initialized and running.")

		// Блокирующий вызов. Вернет ошибку, если канал упадет
		err = processor.Execute(ctx)

		// Очищаем ресурсы перед новой попыткой
		rmqCh.Close()
		rmqConn.Close()

		if err != nil && err != context.Canceled {
			slog.Error("Pipeline processing failure, reconnecting in 3s...", "err", err)
			if !sleepContext(ctx, 3*time.Second) { return }
		}
	}
}

func initPostgresWithRetry(ctx context.Context, cfg domain.PostgresConfig) *pgxpool.Pool {
	pgConfig, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		slog.Error("Database subsystem failure: parsing PostgreSQL DSN variables failed", "err", err)
		return nil
	}
	pgConfig.MaxConns = 20
	pgConfig.MinConns = 5
	pgConfig.MaxConnLifetime = time.Hour
	
	var pool *pgxpool.Pool
	for i := 1; i <= 5; i++ {
		pool, err = pgxpool.NewWithConfig(ctx, pgConfig)
		if err == nil {
			err = pool.Ping(ctx)
			if err == nil {
				slog.Info("Successfully connected to PostgreSQL")
				return pool
			}
			slog.Warn(fmt.Sprintf("PostgreSQL not ready (attempt %d/5), retrying in 2s...", i), "err", err)
			if !sleepContext(ctx, 2*time.Second) { return nil }
		}
	}
	slog.Error("Failed to connect to PostgreSQL after multiple attempts")
	return nil
}

func initClickhouseWithRetry(ctx context.Context, cfg domain.ClickHouseConfig) clickhouse.Conn {
	var conn clickhouse.Conn
	var err error
	
	for i := 1; i <= 5; i++ {
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{cfg.Host + ":" + strconv.Itoa(cfg.Port)},
			Auth: clickhouse.Auth{
				Database: cfg.Database,
				Username: cfg.User,
				Password: cfg.Password,
			},
			DialTimeout: cfg.Timeout,
		})
		if err == nil {
			err = conn.Ping(ctx)
			if err == nil {
				slog.Info("Successfully connected to ClickHouse")
				return conn
			}
		}
		slog.Warn(fmt.Sprintf("ClickHouse not ready (attempt %d/5), retrying in 2s...", i), "err", err)
		if !sleepContext(ctx, 2*time.Second) { return nil }
	}
	slog.Error("Failed to connect to ClickHouse after multiple attempts")
	return nil
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	select {
	case <- ctx.Done():
		return false
	case <- time.After(delay):
		return false
	}
}