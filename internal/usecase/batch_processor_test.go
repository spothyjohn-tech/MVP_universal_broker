package usecase

import (
	"github.com/google/go-cmp/cmp"
	"context"
	"errors"
	"marketplace_broker/internal/domain"
	"testing"
)

type dataRouterStub struct {
	domain.DataRouter
	routeFunc func(ctx context.Context, target string, messages []domain.RequestMessage) ([]domain.ResponseMessage, error)
}

func (r *dataRouterStub) Route(ctx context.Context, target string, messages []domain.RequestMessage) ([]domain.ResponseMessage, error){
	return r.routeFunc(ctx, target, messages)
}

type queueMessageBrokerStub struct{
	domain.QueueMessageBroker
	consumeRequestsFunc func(ctx context.Context) (<-chan domain.MessageContainer, error)
	publishResponsesFunc func(ctx context.Context, responses []domain.ResponseMessage) error
	acknowledgeBatchFunc func(ctx context.Context, lastDeliveryTag uint64) error
}

func (b *queueMessageBrokerStub) ConsumeRequests(ctx context.Context) (<-chan domain.MessageContainer, error){
	return b.consumeRequestsFunc(ctx)
}

func (b *queueMessageBrokerStub) PublishResponses(ctx context.Context, responses []domain.ResponseMessage) error{
	return b.publishResponsesFunc(ctx, responses)
}

func (b *queueMessageBrokerStub) AcknowledgeBatch(ctx context.Context, lastDeliveryTag uint64) error{
	return b.acknowledgeBatchFunc(ctx, lastDeliveryTag)
}


func TestBatchProcessor_Execute(t *testing.T){
	msg1 := domain.RequestMessage{ID: "1", RequestID: "req_abc", Target: "1c_erp", Action: "create_order", Payload: "{}"}
	msg2 := domain.RequestMessage{ID: "2", RequestID: "req_xyz", Target: "clickhouse", Action: "write_metrics", Payload: "{}"}

	tests := []struct{
		name string
		routerRouteFunc func(ctx context.Context, target string, messages []domain.RequestMessage) ([]domain.ResponseMessage, error)
		expectedPublished []domain.ResponseMessage
	}{
		{
		name: "Успешная обработка всего батча",
			routerRouteFunc: func(ctx context.Context, target string, messages []domain.RequestMessage) ([]domain.ResponseMessage, error) {
				// Драйвер под каждую систему возвращает свой успешный ответ
				if target == "1c_erp" {
					return []domain.ResponseMessage{{RequestID: "req_abc", Status: "SUCCESS", Payload: "1C_OK"}}, nil
				}
				if target == "clickhouse" {
					return []domain.ResponseMessage{{RequestID: "req_xyz", Status: "SUCCESS", Payload: "CH_OK"}}, nil
				}
				return nil, nil
			},
			expectedPublished: []domain.ResponseMessage{
				{RequestID: "req_abc", Status: "SUCCESS", Payload: "1C_OK"},
				{RequestID: "req_xyz", Status: "SUCCESS", Payload: "CH_OK"},
			},
		},
		{
			name: "Критическая ошибка роутинга к 1С (Должен сработать continue и замапиться ERROR)",
			routerRouteFunc: func(ctx context.Context, target string, messages []domain.RequestMessage) ([]domain.ResponseMessage, error) {
				// Симулируем падение 1С, но ClickHouse при этом работает штатно
				if target == "1c_erp" {
					return nil, errors.New("1C connection timeout")
				}
				if target == "clickhouse" {
					return []domain.ResponseMessage{{RequestID: "req_xyz", Status: "SUCCESS", Payload: "CH_OK"}}, nil
				}
				return nil, nil
			},
			expectedPublished: []domain.ResponseMessage{
				// Сообщение для 1С превратилось в статус ERROR благодаря нашему внутреннему циклу
				{RequestID: "req_abc", Status: "ERROR", Payload: "1C connection timeout"},
				// ClickHouse обработался успешно, так как continue не прервал весь батч!
				{RequestID: "req_xyz", Status: "SUCCESS", Payload: "CH_OK"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func (t *testing.T)  {
			inputChan := make(chan domain.MessageContainer, 2)
			inputChan <- domain.MessageContainer{DeliveryTag: 101, Body: msg1}
			inputChan <- domain.MessageContainer{DeliveryTag: 102, Body: msg2}
			close(inputChan)

			var actualPublished []domain.ResponseMessage
			var ackCalled bool
			var actualLastTag uint64

			brokerStub := &queueMessageBrokerStub{
				consumeRequestsFunc: func(ctx context.Context) (<-chan domain.MessageContainer, error){
					return inputChan, nil
				},	
				publishResponsesFunc: func(ctx context.Context, responses []domain.ResponseMessage) error {
					actualPublished = responses
					return nil
				},
				acknowledgeBatchFunc: func(ctx context.Context, lastDeliveryTag uint64) error {
					ackCalled = true
					actualLastTag = lastDeliveryTag
					return nil
				},
			}

			routerStub := &dataRouterStub{
				routeFunc: tt.routerRouteFunc,
			}
			cfg := domain.BatchConfig{Size: 2, Timeout: 5000}

			// Инициализируем юзкейс через внедрение зависимостей (Dependency Injection)
			uc := NewBatchProcessorUsecase(routerStub, brokerStub, cfg)

			// Запускаем выполнение бизнес-логики
			err := uc.Execute(t.Context())
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("Execute returned unexpected error: %v", err)
			}

			// ПРОВЕРКА 1: Проверяем отправленные ответы с помощью библиотеки go-cmp 
			if diff := cmp.Diff(tt.expectedPublished, actualPublished); diff != "" {
				t.Errorf("PublishResponses mismatch (-want +got):\n%s", diff)
			}

			// ПРОВЕРКА 2: Убеждаемся, что батч подтвержден (вызван Ack)
			if !ackCalled {
				t.Error("Expected AcknowledgeBatch to be called, but it wasn't")
			}

			// ПРОВЕРКА 3: Проверяем, что подтверждение ушло по последнему DeliveryTag (102)
			if actualLastTag != 102 {
				t.Errorf("Expected Ack with last tag 102, got %d", actualLastTag)
			}
		})
	}
}