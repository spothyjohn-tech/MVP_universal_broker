package domain

import "time"


type SalesPayload struct {
	ID          string    `json:"id"`
	ProductID   string    `json:"product_id"`
	ClientId    string    `json:"client_id"`
	WarehouseID string    `json:"warehouse_id"`
	Count       uint32    `json:"count"`
	Price       float64   `json:"price"`
	Period      time.Time `json:"period"`
}


type StocksPayload struct {
	ID           string    `json:"id"`
	ProductID    string    `json:"product_id"`
	WarehouseID  string    `json:"warehouse_id"`
	CurrentStock int32     `json:"current_stock"`
	Period       time.Time `json:"period"`
}


type BatchData struct {
	Sales  []SalesPayload
	Stocks []StocksPayload
}
