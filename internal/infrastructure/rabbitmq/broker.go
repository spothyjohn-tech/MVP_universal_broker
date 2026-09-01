package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"broker/internal/domain"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQBroker struct {
	ch           *amqp.Channel
	queue   string
}

func NewRabbitMQBroker(ch *amqp.Channel, queue string) *RabbitMQBroker {
	return &RabbitMQBroker{ch: ch, queue: queue}
}

// Вспомогательная структура для парсинга обертки сообщения из очереди
type rawMessage struct {
	Action  string `json:"action"`
	Payload string `json:"payload"`
}
type salesWire struct {
	ID          string  `json:"id"`
	ProductID   string  `json:"product_id"`
	ClientId    string  `json:"client_id"`
	WarehouseID string  `json:"warehouse_id"`
	Count       uint32  `json:"count"`
	Price       float64 `json:"price"`
	Period      string  `json:"period"` // Сетевой формат даты (string)
}

type stocksWire struct {
	ID           string `json:"id"`
	ProductID    string `json:"product_id"`
	WarehouseID  string `json:"warehouse_id"`
	CurrentStock uint32 `json:"current_stock"`
	Period       time.Time `json:"period"` // Или строка в зависимости от 1С
}

func (b *RabbitMQBroker) ConsumeBatch(ctx context.Context, batchSize int, timeout time.Duration) (*domain.BatchData, []uint64, error) {
	err := b.ch.Qos(batchSize*2,0,false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to set Qos: %w", err)
	}

	deliveries, err := b.ch.Consume(b.queue, "", false, false, false, false, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start consume: %w", err)
	}
	batchData := &domain.BatchData{
		Sales:  make([]domain.SalesPayload, 0, batchSize),
		Stocks: make([]domain.StocksPayload, 0, batchSize),
	}
	deliveryTags := make([]uint64, 0, batchSize)
		
	ticker := time.NewTicker(timeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil,nil,ctx.Err()
		case msg, ok := <-deliveries:
			if !ok {
				return  nil, nil, fmt.Errorf("rabbitmq channel closed")
			}
			deliveryTags = append(deliveryTags, msg.DeliveryTag)
			var raw rawMessage
			if err := json.Unmarshal(msg.Body, &raw); err != nil {
				_ = msg.Reject(false)
				continue
			}
			switch raw.Action{
			case "insert_sales":
				var wireData []salesWire
				if err := json.Unmarshal([]byte(raw.Payload),&wireData); err != nil{
					_ = msg.Reject(false)
					continue
				}
				for _, w := range wireData {
					t, _ := time.Parse("2006-01-02 15:04:05", w.Period)
					batchData.Sales = append(batchData.Sales, domain.SalesPayload{
						ID: w.ID, ProductID: w.ProductID, ClientId: w.ClientId, WarehouseID: w.WarehouseID, Count: w.Count, Price: w.Price, Period: t,
					})
				}
			case "insert_stocks":
				var stocks []domain.StocksPayload // Если 1С шлет сразу ISO время
				if err := json.Unmarshal([]byte(raw.Payload), &stocks); err != nil {
					_ = msg.Reject(false)
					continue
				}
				batchData.Stocks = append(batchData.Stocks, stocks...)
			}	
			if len(deliveryTags) >= batchSize {
				return batchData, deliveryTags, nil
			}
			ticker.Reset(timeout)
		case <- ticker.C:
			if len(deliveryTags) > 0 {
				return batchData, deliveryTags, nil
			}
		}

	}
	
}

func (b *RabbitMQBroker)  AcknowledgeBatch(ctx context.Context, deliveryTags []uint64) error {
	if len(deliveryTags) == 0 {
		return nil
	}
	return b.ch.Ack(deliveryTags[len(deliveryTags)-1], true)
}

