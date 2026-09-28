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

func NewRabbitMQBroker(ch *amqp.Channel, queue string) (*RabbitMQBroker, error) {

	dlqExchange := "dlq_exchange"
	dlqQueue := queue + "_dlq"
	routingKeyDLQ := queue + "_dead_letter"

	err := ch.ExchangeDeclare(
		dlqExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare DLX exchange: %w, err")
	}

	_, err = ch.QueueDeclare(
		dlqQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to DLQ queue: %w", err)
	}

	err = ch.QueueBind(
		dlqQueue,
		routingKeyDLQ,
		dlqExchange,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to bind DLQ to DLX: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange": dlqExchange,
		"x-dead-letter-routing-key": routingKeyDLQ,
	}

	_, err = ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare main queue with DLX args: %w", err)
	}

	return &RabbitMQBroker{ch: ch, queue: queue}, nil
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

func (b *RabbitMQBroker) RejectBatchToDLQ(ctx context.Context, deliveryTags []uint64) error {
	if len(deliveryTags) == 0 {
		return nil
	}
	for _, tag := range deliveryTags {
		if err := b.ch.Nack(tag, false, false); err != nil {
			return fmt.Errorf("failed to nack tag %d to DLQ: %w", tag, err)
		}
	}
	return nil
}

func (b *RabbitMQBroker) RejectToDLQ(ctx context.Context, deliveryTag uint64) error {
	return b.ch.Nack(deliveryTag, false, false)
	// for _, tag := range deliveryTags {
	// 	if err := b.ch.Nack(tag,false,false); err != nil {
	// 		return err
	// 	}
	// }
	// return nil
}

func (b *RabbitMQBroker) NackBatchForRetry(ctx context.Context, delveryTags []uint64) error {
	if len(delveryTags) == 0 {
		return nil
	}
	lastTag := delveryTags[len(delveryTags)-1]
	return b.ch.Nack(lastTag, true, true)
}


