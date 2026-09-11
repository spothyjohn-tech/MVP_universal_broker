package main

import (
	"broker/internal/domain"
	ch_infra "broker/internal/infrastructure/clickhouse"
	"broker/internal/infrastructure/postgres"
	"broker/internal/infrastructure/rabbitmq"
	"broker/internal/usecase"
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Initialize core application configuration layers using cleanenv.
	cfg, err := domain.LoadConfig()
	if err != nil {
		slog.Error("Process boot failure: environment configuration configuration parsing crashed", "err", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Establish connection pool to operational PostgreSQL server instance.
	pgConfig, err := pgxpool.ParseConfig(cfg.Postgres.DSN())
	if err != nil {
		slog.Error("Database subsystem failure: parsing PostgreSQL DSN variables failed", "err", err)
		return
	}
	pgConfig.MaxConns = 20 
	pgConfig.MinConns = 5
	pgConfig.MaxConnLifetime = time.Hour

	pgPool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		slog.Error("Database subsystem failure: opening PostgreSQL transaction socket pool aborted", "err", err)
		return
	}
	defer pgPool.Close()

	// Establish native connection protocol to columnar analytical ClickHouse server cluster.	
	chConn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{cfg.ClickHouse.Host + ":" + strconv.Itoa(cfg.ClickHouse.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.ClickHouse.Database,
			Username: cfg.ClickHouse.User,
			Password: cfg.ClickHouse.Password,
		},
		DialTimeout: cfg.ClickHouse.Timeout,
	})
	if err != nil {
		slog.Error("Analytics subsystem failure: native ClickHouse client transport initialization aborted", "err", err)
		return
	}
	defer chConn.Close()

	// Connect to asynchronous message transport bus AMQP v0.9.1.
	rmqConn, err := amqp.Dial(cfg.RabbitMQ.URL())
	if err != nil {
		slog.Error("Transport network failure: broker handshake execution failed via AMQP protocol", "err", err)
		return
	}
	defer rmqConn.Close()

	rmqCh, err := rmqConn.Channel()
	if err != nil {
		slog.Error("Transport network failure: structural virtual channel extraction from AMQP link failed", "err", err)
		return
	}
	defer rmqCh.Close()

	// Dependency Injection: Bind adapter objects into доменные abstraction layers.
	postgresRepo := postgres.NewBalanceRepository(pgPool)
	clickhouseRepo := ch_infra.NewSalesRepository(chConn)
	rabbitmqBroker := rabbitmq.NewRabbitMQBroker(rmqCh, "1c_requests")

	processor := usecase.NewBatchProcessor(
		rabbitmqBroker,
		clickhouseRepo,
		postgresRepo,
		cfg.Batch.Size,
		cfg.Batch.Timeout,
	)

	// Intercept operating system termination interrupts to toggle the Graceful Shutdown flow sequence.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)


	go func() {
		slog.Info("Highload Clean Architecture Worker kernel successfully initialized and running.")
		if err := processor.Execute(ctx); err != nil && err != context.Canceled {
			slog.Error("Pipeline processing failure: engine encountered a critical lifecycle execution error", "err", err)
		}
	}()
	<-sigChan
	slog.Info("Termination interceptor triggered. Shutting down streaming infrastructure channels cleanly...")
	cancel() // Cancel context to signal downstream worker threads to conclude tasks.
	time.Sleep(500 * time.Millisecond) 
	slog.Info("Ingestion server node completely stopped. Resources detached successfully.")
}
