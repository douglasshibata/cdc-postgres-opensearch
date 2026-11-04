package sync_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/sync"
)

func TestParseCDCMessage_DebeziumStandardCreate(t *testing.T) {
	product := domain.Product{
		ID:            "prod-123",
		Name:          "Notebook Ultra",
		PriceCents:    550000,
		StockQuantity: 10,
		CategoryID:    "computers",
		Brand:         "TechBrand",
		IsActive:      true,
	}

	prodBytes, err := json.Marshal(product)
	if err != nil {
		t.Fatalf("failed to marshal product: %v", err)
	}

	rawEvent := map[string]any{
		"payload": map[string]any{
			"before": nil,
			"after":  json.RawMessage(prodBytes),
			"op":     "c",
			"ts_ms":  time.Now().UnixMilli(),
		},
	}

	eventBytes, _ := json.Marshal(rawEvent)

	parsedProduct, deleteID, op, eventTime, err := sync.ParseCDCMessage(eventBytes)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	if op != "upsert" {
		t.Errorf("expected op 'upsert', got %s", op)
	}
	if parsedProduct == nil || parsedProduct.ID != "prod-123" {
		t.Errorf("expected product ID 'prod-123', got %+v", parsedProduct)
	}
	if deleteID != "" {
		t.Errorf("expected empty deleteID, got %s", deleteID)
	}
	if eventTime.IsZero() {
		t.Errorf("expected valid eventTime")
	}
}

func TestParseCDCMessage_DebeziumOutboxFormat(t *testing.T) {
	product := domain.Product{
		ID:         "outbox-prod-456",
		Name:       "Headphones Noise Cancelling",
		PriceCents: 89900,
		IsActive:   true,
	}

	prodBytes, _ := json.Marshal(product)

	outboxRecord := map[string]any{
		"aggregate_id":   "outbox-prod-456",
		"aggregate_type": "product",
		"event_type":     "ProductCreated",
		"payload":        string(prodBytes),
	}
	outboxBytes, _ := json.Marshal(outboxRecord)

	rawEvent := map[string]any{
		"payload": map[string]any{
			"before": nil,
			"after":  json.RawMessage(outboxBytes),
			"op":     "c",
			"ts_ms":  time.Now().UnixMilli(),
		},
	}
	eventBytes, _ := json.Marshal(rawEvent)

	parsedProduct, deleteID, op, _, err := sync.ParseCDCMessage(eventBytes)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if op != "upsert" {
		t.Errorf("expected op 'upsert', got %s", op)
	}
	if parsedProduct == nil || parsedProduct.ID != "outbox-prod-456" {
		t.Errorf("expected product ID 'outbox-prod-456', got %+v", parsedProduct)
	}
	if deleteID != "" {
		t.Errorf("expected empty deleteID, got %s", deleteID)
	}
}

func TestParseCDCMessage_DebeziumStandardDelete(t *testing.T) {
	beforeProduct := domain.Product{
		ID:   "del-789",
		Name: "Old Item",
	}
	beforeBytes, _ := json.Marshal(beforeProduct)

	rawEvent := map[string]any{
		"payload": map[string]any{
			"before": json.RawMessage(beforeBytes),
			"after":  nil,
			"op":     "d",
			"ts_ms":  time.Now().UnixMilli(),
		},
	}
	eventBytes, _ := json.Marshal(rawEvent)

	parsedProduct, deleteID, op, _, err := sync.ParseCDCMessage(eventBytes)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	if op != "delete" {
		t.Errorf("expected op 'delete', got %s", op)
	}
	if deleteID != "del-789" {
		t.Errorf("expected deleteID 'del-789', got %s", deleteID)
	}
	if parsedProduct != nil {
		t.Errorf("expected nil parsedProduct on delete, got %+v", parsedProduct)
	}
}

func TestParseCDCMessage_DirectFlattenedProduct(t *testing.T) {
	directProduct := domain.Product{
		ID:         "flat-999",
		Name:       "Mechanical Keyboard",
		PriceCents: 45000,
	}
	bytes, _ := json.Marshal(directProduct)

	parsedProduct, deleteID, op, _, err := sync.ParseCDCMessage(bytes)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if op != "upsert" || parsedProduct == nil || parsedProduct.ID != "flat-999" {
		t.Errorf("expected parsed product flat-999, got %+v", parsedProduct)
	}
	if deleteID != "" {
		t.Errorf("expected empty deleteID, got %s", deleteID)
	}
}
