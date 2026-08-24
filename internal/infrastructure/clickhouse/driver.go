package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"marketplace_broker/internal/domain"
)

type ClickHouseDriver struct {
	db *sql.DB
}

func NewClickHouseDriver(db *sql.DB) domain.SystemDriver {
	return &ClickHouseDriver{db: db}
}

type SalesPayload struct {
	ProductID   string  `json:"product_id"`
	WarehouseID string  `json:"warehouse_id"`
	Count       uint32  `json:"count"`
	Price       float64 `json:"price"`
	Period      string  `json:"period"`
}

type ReportRow struct {
	ProductID    string  `json:"product_id"`
	WarehouseID  string  `json:"warehouse_id"`
	MonthlySales uint32  `json:"monthly_sales"`
	CurrentStock int32   `json:"current_stock"`
	DaysLeft     float64 `json:"days_left"`
}

type StocksPayload struct {
	ProductID    string `json:"product_id"`
	WarehouseID  string `json:"warehouse_id"`
	CurrentStock uint32 `json:"current_stock"`
	Period       string `json:"period"`
}

func (d *ClickHouseDriver) Process(ctx context.Context, messages []domain.RequestMessage) ([]domain.ResponseMessage, error) {
	responces := make([]domain.ResponseMessage, 0, len(messages))

	// Так как в одной пачке могут быть разные операции (вставка продаж или остатков),
	// мы группируем операции, чтобы выполнить массовый Insert
	for _, msg := range messages {
		switch msg.Action {
		case "insert_sales":
			err := d.batchInsertSales(ctx, messages)
			if err != nil {
				return d.createErrorResponces(messages, err), nil
			}
			return d.createSuccessResponces(messages, "Sales inserted successfully"), nil
		case "insert_stocks":
			err := d.batchInsertStocks(ctx, messages)
			if err != nil {
				return d.createErrorResponces(messages, err), nil
			}
			return d.createSuccessResponces(messages, "Stocks inserted successfully"), nil
		case "select_turnover_report":
			return d.handleTurnoverReport(ctx, msg)
		default:
			responces = append(responces, domain.ResponseMessage{
				RequestID: msg.RequestID,
				Status:    "ERROR",
				Payload:   fmt.Sprintf("Unknown action: %s", msg.Action),
			})
		}
	}
	return responces, nil
}

func (d *ClickHouseDriver) batchInsertSales(ctx context.Context, messages []domain.RequestMessage) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("ClickHouse BeginTx failed", "err", err)
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO sales (id, product_id, warehouse_id, count, price, period) VALUES (?, ?, ?, ?, ?, ?)")
	if err != nil {
		slog.Error("ClickHouse PrepareContext failed", "err", err)
		return err
	}
	defer stmt.Close()

	for _, msg := range messages {
		if msg.Action != "insert_sales" {
			continue
		}
		var payloadSlice []SalesPayload
		if err := json.Unmarshal([]byte(msg.Payload), &payloadSlice); err != nil {
			slog.Error("Failed to unmarshal sales payload slice", "msg_id", msg.ID, "err", err)
			return err
		}
		for _, p := range payloadSlice {
			_, err = stmt.ExecContext(ctx, msg.ID, p.ProductID, p.WarehouseID, p.Count, p.Price, p.Period)
			if err != nil {
				slog.Error("ExecContext failed for row", "msg_id", msg.ID, "err", err)
				return err
			}
		}
	}

	return tx.Commit()
}

func (d *ClickHouseDriver) batchInsertStocks(ctx context.Context, messages []domain.RequestMessage) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("ClickHouse BeginTx failed for stocks", "err", err)
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, "INSERT INTO stocks (id, product_id, warehouse_id, current_stock, period) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		slog.Error("ClickHouse PrepareContext failed for stocks", "err", err)
		return err
	}
	defer stmt.Close()

	for _, msg := range messages {
		if msg.Action != "insert_stocks" {
			continue
		}
		var payloadSlice []StocksPayload
		if err := json.Unmarshal([]byte(msg.Payload), &payloadSlice); err != nil {
			slog.Error("Failed to unmarshal stocks payload slice", "msg_id", msg.ID, "err", err)
			return err
		}

		for _, p := range payloadSlice {
			_, err = stmt.ExecContext(ctx, msg.ID, p.ProductID, p.WarehouseID, p.CurrentStock, p.Period)
			if err != nil {
				slog.Error("ExecContext failed for stock row", "msg_id", msg.ID, "err", err)
				return err
			}
		}
	}

	return tx.Commit()
}


func (d *ClickHouseDriver) createErrorResponces(messages []domain.RequestMessage, err error) []domain.ResponseMessage {
	res := make([]domain.ResponseMessage, 0, len(messages))
	for _, m := range messages {
		res = append(res, domain.ResponseMessage{RequestID: m.RequestID, Status: "ERROR", Payload: err.Error()})
	}
	return res
}

func (d *ClickHouseDriver) createSuccessResponces(messages []domain.RequestMessage, msg string) []domain.ResponseMessage {
	res := make([]domain.ResponseMessage, 0, len(messages))
	for _, m := range messages {
		res = append(res, domain.ResponseMessage{RequestID: m.RequestID, Status: "SUCCESS", Payload: msg})
	}
	return res
}

func (d *ClickHouseDriver) handleTurnoverReport(ctx context.Context, msg domain.RequestMessage) ([]domain.ResponseMessage, error) {
	query := `
		SELECT s.product_id, s.warehouse_id, SUM(s.count) AS monthly_sales, ANY(st.current_stock) AS current_stock, 
		CASE 
		WHEN SUM(s.count) > 0 THEN ANY(st.current_stock) 
		ELSE 9999.0 
		END as days_left
		FROM sales s
		ANY LEFT JOIN (
			SELECT product_id, warehouse_id, current_stock
			FROM stocks
			WHERE period >=toDateTime(now() - INTERVAL 1 DAY) 
			ORDER BY period DESC
			LIMIT 1 BY product_id, warehouse_id
		) st ON s.product_id = st.product_id AND s.warehouse_id = st.warehouse_id
		WHERE s.period >= toDateTime(now() - INTERVAL 30 DAY)
		GROUP BY s.product_id, s.warehouse_id
	`
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var report []ReportRow
	for rows.Next() {
		var r ReportRow
		if err := rows.Scan(&r.ProductID, &r.WarehouseID, &r.MonthlySales, &r.CurrentStock, &r.DaysLeft); err != nil {
			return nil, err
		}
		report = append(report, r)
	}
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return nil, err
	}

	return []domain.ResponseMessage{{
		RequestID: msg.RequestID,
		Status:    "SUCCESS",
		Payload:   string(reportJSON),
	}}, nil

}
