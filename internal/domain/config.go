package domain

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type AppConfig struct {
	ClickHouseDSN string
	RabbitMQURL string
	BatchSize int
	BatchTimeoutMS int
}

func LoadConfig() (*AppConfig, error){
	_ = godotenv.Load()

	 size, _ := strconv.Atoi(os.Getenv("BATCH_SIZE"))
	   timeout, _ := strconv.Atoi(os.Getenv("BATCH_TIMEOUT_MS"))

	   return &AppConfig{
		   ClickHouseDSN:  os.Getenv("CLICKHOUSE_DSN"),
		   RabbitMQURL:    os.Getenv("RABBITMQ_URL"),
		   BatchSize:      size,
		   BatchTimeoutMS: timeout,
	   }, nil
}