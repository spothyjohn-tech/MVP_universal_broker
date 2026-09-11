package domain

import (
	"math"
	"time"
)

type SalesPayload struct {
	ID          string    `json:"id"`
	ProductID   string    `json:"product_id"`
	ClientId    string    `json:"client_id"`
	WarehouseID string    `json:"warehouse_id"`
	Count       float64   `json:"count"`
	Price       float64   `json:"price"`
	Period      time.Time `json:"period"`
}


type StocksPayload struct {
	ID           string    `json:"id"`
	ProductID    string    `json:"product_id"`
	WarehouseID  string    `json:"warehouse_id"`
	CurrentStock float64   `json:"current_stock"`
	Period       time.Time `json:"period"`
}

type BatchData struct {
	Sales  []SalesPayload
	Stocks []StocksPayload
}

func RoundToTwoDecimal(val float64) float64 {
	return math.Round(val*100) / 100
}