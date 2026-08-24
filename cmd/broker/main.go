package main

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"marketplace_broker/internal/domain"
	"marketplace_broker/internal/infrastructure/clickhouse"
	"marketplace_broker/internal/infrastructure/ones"
	"marketplace_broker/internal/infrastructure/rabbitmq"
	"marketplace_broker/internal/infrastructure/router"
	"marketplace_broker/internal/usecase"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	cfg, _ := domain.LoadConfig()
	/////////// INIT CONTEXT ///////////
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	/////////// INIT CONNECTIONS ///////////
	// Clickhouse
	chConn, err := sql.Open("clickhouse", cfg.ClickHouseDSN)
	if err != nil {
		slog.Error("ClickHouse init error: ", "err", err)
	}
	defer chConn.Close()

	chConn.SetMaxOpenConns(20)
	chConn.SetMaxIdleConns(20)
	chConn.SetConnMaxLifetime(time.Hour)

	if err := chConn.PingContext(ctx); err != nil{
		log.Fatalf("Clickhouse ping failed: %v", err)
	}
	slog.Info("Successfully connected to Clickhouse")
	
	// RabbitMQ
	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		slog.Error("RabbitMQ connection error: ", "err", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		slog.Error("RabbitMQ channel error: ", "err", err)
	}
	defer ch.Close()
	/////////// INIT INSTRUMENTS ///////////
	batchCfg := domain.BatchConfig{
		Size:    cfg.BatchSize,
		Timeout: cfg.BatchTimeoutMS,
	}

	// 4. Внедрение зависимостей (Dependency Injection) 
	rtr := router.NewDataRouterAdapter()
	oneCDriver := ones.NewOneCDriver("http://1c-server/erp/hs/custom_broker/v1/batch", 6)
	rtr.RegisterDriver("1c_erp", oneCDriver)

	rtr.RegisterDriver("clickhouse", clickhouse.NewClickHouseDriver(chConn))

	broker, err := rabbitmq.NewRabbitMQBroker(ch, "1c_requests", "1c_responses", batchCfg.Size*2)
	if err != nil {
		slog.Error("Broker init error:", "err", err)
	}
	
	processor := usecase.NewBatchProcessorUsecase(rtr, broker, batchCfg)

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
