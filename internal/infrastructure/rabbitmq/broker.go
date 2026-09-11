package rabbitmq

import (
	"broker/internal/domain"
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQBroker struct {
	ch           *amqp.Channel
	queue   string
}

func NewRabbitMQBroker(ch *amqp.Channel, queue string) *RabbitMQBroker {
	return &RabbitMQBroker{ch: ch, queue: queue}
}


func (b *RabbitMQBroker) StartConsuming(ctx context.Context, batchSize int) (<-chan domain.Message, error) {
	err := b.ch.Qos(batchSize*2,0,false)
	if err != nil {
		return nil, fmt.Errorf("failed to set Qos: %w", err)
	}
	deliveries, err := b.ch.Consume(b.queue, "1c_worker_tag", false, false, false, false, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to start consume: %w", err)
	}
	out := make(chan domain.Message, batchSize)
	
	go func() {
		defer close(out)
		for msg := range deliveries {
			out <- domain.Message{
				Body: msg.Body,
				DeliveryTag: msg.DeliveryTag,
			}
		}
	}()
	return out, nil	
}

func (b *RabbitMQBroker)  AcknowledgeBatch(ctx context.Context, deliveryTags []uint64) error {
	if len(deliveryTags) == 0 {
		return nil
	}
	lastTag := deliveryTags[len(deliveryTags)-1]
	return b.ch.Ack(lastTag, true)
}

func (b *RabbitMQBroker) RejectToDLQ(ctx context.Context, deliveryTag uint64) error {
	return b.ch.Reject(deliveryTag, false)
}

