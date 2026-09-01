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
	/////////// INIT CONFIG ///////////
	cfg, err := domain.LoadConfig()
	if err != nil {
		slog.Error("Config load error:", "err", err)
		return
	}
	/////////// INIT CONTEXT ///////////
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	/////////// INIT CONNECTIONS ///////////
	// 1. PostgreSQL (pgx/v5)
	pgConfig, err := pgxpool.ParseConfig(cfg.Postgres.DSN())
	if err != nil {
		slog.Error("Failed to parse Postgres DSN", "err", err)
		return
	}
	pgConfig.MaxConns = 20 
	pgConfig.MinConns = 5
	pgConfig.MaxConnLifetime = time.Hour

	pgPool, err := pgxpool.NewWithConfig(ctx, pgConfig)
	if err != nil {
		slog.Error("Postgres connection pool initialization failed", "err", err)
		return
	}
	defer pgPool.Close()

	// Clickhouse
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
		slog.Error("ClickHouse native connection failed", "err", err)
		return
	}
	defer chConn.Close()

	// RabbitMQ
	rmqConn, err := amqp.Dial(cfg.RabbitMQ.URL())
	if err != nil {
		slog.Error("RabbitMQ amqp dial failed", "err", err)
		return
	}
	defer rmqConn.Close()

	rmqCh, err := rmqConn.Channel()
	if err != nil {
		slog.Error("Failed to open RabbitMQ channel", "err", err)
		return
	}
	defer rmqCh.Close()

	/////////// INIT INSTRUMENTS ///////////
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

	/////////// GRACEFUL SHUTDOWN ///////////
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		slog.Info("Shutting down worker gracefully...")
		cancel()
	}()
	slog.Info("Highload Clean Architecture Worker is running...")
	if err := processor.Execute(ctx); err != nil && err != context.Canceled {
		slog.Error("Worker execution error: ", "err", err)
	}
}
