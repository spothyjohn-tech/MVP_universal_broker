package domain

import(
	"context"
)

type RequestMessage struct {
	ID string `json:"id"`
	RequestID string `json:"request_id"`
	Target string `json:"target"` // For example: "1c_erp", "postgres_main", "bitrix"
	Action string `json:"action"`	// Operation: "create_order", "get_metrics"
	Payload string `json:"payload"` // Raw data for transmission (JSON string / XML)
}

type ResponseMessage struct {
	RequestID string `json:"request_id"`
	Status string `json:"status"`  // "SUCCESS", "ERROR", "NOT_FOUND"
	Payload string `json:"payload"` // Ответ от целевой системы (код ошибки или данные)
}

type MessageContainer struct {
	DeliveryTag uint64
	Body RequestMessage
}

type BatchConfig struct {
	Size int
	Timeout int
}

type SystemDriver interface {
	Process(ctx context.Context, actions []RequestMessage) ([]ResponseMessage, error)
}

type DataRouter interface {
	Route(ctx context.Context, target string, messages []RequestMessage) ([]ResponseMessage, error)
	RegisterDriver(target string, driver SystemDriver)
}

type QueueMessageBroker interface {
	ConsumeRequests(ctx context.Context) (<-chan MessageContainer, error)
	PublishResponses(ctx context.Context, responses []ResponseMessage) error
	AcknowledgeBatch(ctx context.Context, lastDeliveryTag uint64) error
}

