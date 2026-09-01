package domain

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

type BatchConfig struct {
	Size    int           `env:"BATCH_SIZE" env-default:"1000"`
	Timeout time.Duration `env:"BATCH_TIMEOUT" env-default:"500ms"`
}

type AppConfig struct {
	Env        string           `env:"APP_ENV" env-default:"development"`
	Batch      BatchConfig      `yaml:"batch"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
	RabbitMQ   RabbitMQConfig   `yaml:"rabbitmq"`
	Postgres   PostgresConfig   `yaml:"postgres"`
}

type ClickHouseConfig struct {
	Host     string        `env:"CLICKHOUSE_HOST" env-default:"localhost"`
	Port     int           `env:"CLICKHOUSE_PORT" env-default:"9000"`
	User     string        `env:"CLICKHOUSE_USER" env-default:"default"`
	Password string        `env:"CLICKHOUSE_PASSWORD" env-default:""`
	Database string        `env:"CLICKHOUSE_DB" env-default:"default"`
	Timeout  time.Duration `env:"CLICKHOUSE_TIMEOUT" env-default:"30s"`
}

func (c ClickHouseConfig) DSN() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s?dial_timeout=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.Timeout)
}

type RabbitMQConfig struct {
	Host     string `env:"RABBITMQ_HOST" env-default:"localhost"`
	Port     int    `env:"RABBITMQ_PORT" env-default:"5672"`
	User     string `env:"RABBITMQ_USER" env-default:"guest"`
	Password string `env:"RABBITMQ_PASSWORD" env-default:"guest"`
	VHost    string `env:"RABBITMQ_VHOST" env-default:"/"`
}

func (r RabbitMQConfig) URL() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d%s",
		r.User, r.Password, r.Host, r.Port, r.VHost)
}

type PostgresConfig struct {
	Host     string        `env:"POSTGRES_HOST" env-default:"localhost"`
	Port     int           `env:"POSTGRES_PORT" env-default:"5432"`
	User     string        `env:"POSTGRES_USER" env-default:"postgres"`
	Password string        `env:"POSTGRES_PASSWORD" env-default:"postgres"`
	Database string        `env:"POSTGRES_DB" env-default:"postgres"`
	SSLMode  string        `env:"POSTGRES_SSLMODE" env-default:"disable"`
	Timeout  time.Duration `env:"POSTGRES_TIMEOUT" env-default:"5s"`
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s connect_timeout=%d",
		p.Host, p.Port, p.User, p.Password, p.Database, p.SSLMode, int(p.Timeout.Seconds()))
}

func LoadConfig() (*AppConfig, error) {
	var cfg AppConfig
	err := cleanenv.ReadConfig(".env", &cfg)
	if err != nil {
		err = cleanenv.ReadEnv(&cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load configuration: %w", err)
		}
	}
	return &cfg, nil
}
