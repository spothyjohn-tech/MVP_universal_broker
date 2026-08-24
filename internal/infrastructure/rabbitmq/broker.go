package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"marketplace_broker/internal/domain"

	amqp "github.com/rabbitmq/amqp091-go"
)

type rabbitMQBroker struct {
	ch           *amqp.Channel
	reqQueue     string
	respQueue    string
	prefetchSize int
}

func NewRabbitMQBroker(ch *amqp.Channel, reqQueue, respQueue string, perfetchSize int) (domain.QueueMessageBroker, error) {
	err := ch.Qos(perfetchSize, 0, false)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}
	return &rabbitMQBroker{
		ch: ch,
		reqQueue: reqQueue,
		respQueue: respQueue,
		prefetchSize: perfetchSize,
	}, nil
}

func (b *rabbitMQBroker) ConsumeRequests(ctx context.Context) (<-chan domain.MessageContainer, error){
	deliveries, err := b.ch.Consume(
		b.reqQueue,
		"",    // consumer tag
		false, // auto-ack = FALSE для надежности highload
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	out := make(chan domain.MessageContainer, b.prefetchSize)
	go func(){
		defer close(out)
		for {
			select {
			case <- ctx.Done():
				return 
			case d,ok := <-deliveries:
				if !ok{
					return 
				}
				var req domain.RequestMessage
				if err := json.Unmarshal(d.Body, &req); err != nil{
					// Невалидный JSON сразу отклоняем (Reject) без повторной отправки в очередь
					_ = d.Reject(false)
					continue
				}
				out <- domain.MessageContainer{
					DeliveryTag: d.DeliveryTag,
					Body:        req,
				}
			}
		}
	}()
	return out, nil
}

func (b *rabbitMQBroker) PublishResponses(ctx context.Context, responses []domain.ResponseMessage) error{
	
	for _, resp := range responses{
		body, err := json.Marshal(resp)
		if err != nil{
			return err
		}
		err = b.ch.PublishWithContext(ctx, 
			"",          // exchange
			"1c_responses", // routing key
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        body,
			},
		) 
		if err != nil {
			return err
		}
	}
	
	return nil
}
	
func (b *rabbitMQBroker)  AcknowledgeBatch(ctx context.Context, lastDeliveryTag uint64) error{
	return b.ch.Ack(lastDeliveryTag, true)
}

