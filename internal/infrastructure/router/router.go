package router

import (
	"context"
	"fmt"
	"sync"
	"marketplace_broker/internal/domain"
)

type DataRouterAdapter struct {
	mu sync.RWMutex
	drivers map[string]domain.SystemDriver
}

func NewDataRouterAdapter() domain.DataRouter {
	return &DataRouterAdapter{
		drivers: make(map[string]domain.SystemDriver),
	}
}

func (r *DataRouterAdapter) RegisterDriver(target string, driver domain.SystemDriver){
	r.mu.Lock()
	defer r.mu.Unlock()
	r.drivers[target] = driver
}

func (r *DataRouterAdapter) Route(ctx context.Context, target string, messages []domain.RequestMessage) ([]domain.ResponseMessage, error){
	r.mu.RLock()
	driver, exists := r.drivers[target]
	r.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("system driver not found for target: %s", target)
	}
	return driver.Process(ctx, messages)
}
