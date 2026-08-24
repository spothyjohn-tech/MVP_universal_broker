package usecase

import (
	"context"
	"log/slog"
	"marketplace_broker/internal/domain"
	"time"
)

type BatchProcessorUsecase interface {
	Execute(ctx context.Context) error
}

type batchProcessorUsecase struct {
	router domain.DataRouter
	broker domain.QueueMessageBroker
	cfg domain.BatchConfig
}

func NewBatchProcessorUsecase(router domain.DataRouter, broker domain.QueueMessageBroker, cfg domain.BatchConfig) BatchProcessorUsecase {
	return &batchProcessorUsecase{
		router: router,
		broker: broker,
		cfg: cfg,
	}
}

func (u *batchProcessorUsecase) Execute(ctx context.Context) error{
	requestChan, err := u.broker.ConsumeRequests(ctx)
	if err != nil{
		return err
	}

	var buffer []domain.MessageContainer
	ticker := time.NewTicker(time.Duration(u.cfg.Timeout)*time.Millisecond)
	defer ticker.Stop()

	for{
		select{
		case <-ctx.Done():
			if len(buffer)> 0{
				u.process(ctx, buffer)
			}
			return ctx.Err()
		case msg, ok := <- requestChan:
			if !ok{
				return nil
			}
			buffer = append(buffer, msg)
			if len(buffer) >= u.cfg.Size{
				u.process(ctx, buffer)
				buffer = buffer[:0]
				ticker.Reset(time.Duration(u.cfg.Timeout)*time.Millisecond)
			}
		case <-ticker.C:
			if len(buffer) >0{
				u.process(ctx,buffer)
				buffer = buffer[:0]
			}
		}
	}
}

func (u *batchProcessorUsecase) process(ctx context.Context, batch []domain.MessageContainer) {
	if len(batch) == 0 {
		return
	}

	slog.Info("=== Начинается обработка нового батча ===", 
		"total_messages", len(batch), 
		"first_msg_id", batch[0].Body.ID,
		"first_msg_target", batch[0].Body.Target,
		"first_msg_action", batch[0].Body.Action,
	)

	groupedMessages := make(map[string][]domain.RequestMessage)
	for _, item := range batch{
		target := item.Body.Target
		groupedMessages[target] = append(groupedMessages[target], item.Body)
	}

	allResponces := make([]domain.ResponseMessage, 0, len(batch))

	for target, messages := range groupedMessages {
		responses, err := u.router.Route(ctx,target,messages)
		if err != nil {
			slog.Error("Routing failed for target", "target", target, "err", err)
			for _, msg := range messages {
				allResponces = append(allResponces, domain.ResponseMessage{
					RequestID: msg.RequestID,
					Status: "ERROR",
					Payload: err.Error(),
				})
			}
			continue
		}
		allResponces = append(allResponces, responses...)
	}
	// Публикация ответов в RabbitMQ
	if err := u.broker.PublishResponses(ctx, allResponces); err != nil {
		slog.Error("Failed to publish responses:", "err", err)
		return
	}

	// Множественный групповой коммит (Ack) в RabbitMQ
	lastTag := batch[len(batch)-1].DeliveryTag
	if err := u.broker.AcknowledgeBatch(ctx, lastTag); err != nil {
		slog.Error("Failed to Ack batch:", "err", err)
	} else {
		slog.Info("Successfully processed batch of %d messages", "lenbatch", len(batch))
	}
}