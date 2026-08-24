package ones

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"marketplace_broker/internal/domain"
	"math"
	"net/http"
	"time"
)

type OneCDriver struct {
	httpClient *http.Client
	apiURL string
	maxRetries int
}

func NewOneCDriver(apiURL string,  maxRetries int) domain.SystemDriver {
	return &OneCDriver{
		apiURL: apiURL,
		maxRetries: maxRetries,
		httpClient: &http.Client{
			Timeout: 10 *time.Second,
		},
	}
}

func (d *OneCDriver) Process(ctx context.Context, messages []domain.RequestMessage) ([]domain.ResponseMessage, error){
	//responses := make([]domain.ResponseMessage, 0, len(messages))
	body, err := json.Marshal(messages)
	if err != nil {
		return nil, err
	}
	var resp *http.Response
	var lastErr error

	// ENTERPRISE PATTERN: Повторные попытки с экспоненциальной задержкой (Exponential Backoff)
	for attempt := 0; attempt <= d.maxRetries; attempt++ {
		if attempt > 0 {
			backoffDelay := time.Duration(math.Pow(2, float64(attempt)))
			slog.Warn("Retrying connection to 1C", "attempt", attempt, "delay", backoffDelay, "err", lastErr)
			select{
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoffDelay):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.apiURL,bytes.NewBuffer(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-type", "application/json")

		resp, err := d.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("1C HTTP internal error status: %d", resp.StatusCode)
			resp.Body.Close()
		}
		break
	}

	if resp == nil || resp.StatusCode >= 500 {
		slog.Error("1C service is completely unavailable. Transitioning batch to DLQ emulation.")
		return d.handleFailedBatch(messages, lastErr)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return d.handleFailedBatch(messages, fmt.Errorf("1C returned bad status code: %d", resp.StatusCode))
	}
	
	var responses []domain.ResponseMessage
	if err := json.NewDecoder(resp.Body).Decode(&responses); err != nil {
		return nil, fmt.Errorf("failed to decode 1C response batch: %w", err)
	}

	return responses, nil	
}

// handleFailedBatch — маркирует сообщения как системную ошибку. 
// По канонам Clean Architecture UseCase перехватит этот статус и отправит сообщения в RabbitMQ DLQ
func (d *OneCDriver) handleFailedBatch(messages []domain.RequestMessage, sysErr error) ([]domain.ResponseMessage, error){
	responses := make([]domain.ResponseMessage, 0, len(messages))
	errMsg := "1C_UNAVAILABLE: "
	if sysErr != nil {
		errMsg += sysErr.Error()
	}
		for _, msg := range messages {
			responses = append(responses, domain.ResponseMessage{
				RequestID: msg.RequestID,
				Status: "ERROR_DLQ",
				Payload: errMsg,
			})
		}
	return responses, nil
}