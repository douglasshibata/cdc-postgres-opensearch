package domain

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrInvalidProductName  = errors.New("product name cannot be empty")
	ErrInvalidProductPrice = errors.New("product price must be greater than or equal to zero")
	ErrProductNotFound     = errors.New("product not found")
)

// Product represents the core e-commerce product entity.
type Product struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	PriceCents    int64             `json:"price_cents"`
	StockQuantity int               `json:"stock_quantity"`
	CategoryID    string            `json:"category_id"`
	Brand         string            `json:"brand"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	IsActive      bool              `json:"is_active"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

// CreateProductInput represents input required to register a new product.
type CreateProductInput struct {
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	PriceCents    int64             `json:"price_cents"`
	StockQuantity int               `json:"stock_quantity"`
	CategoryID    string            `json:"category_id"`
	Brand         string            `json:"brand"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	IsActive      *bool             `json:"is_active,omitempty"`
}

// Validate ensures input values fulfill basic business requirements.
func (in *CreateProductInput) Validate() error {
	if strings.TrimSpace(in.Name) == "" {
		return ErrInvalidProductName
	}
	if in.PriceCents < 0 {
		return ErrInvalidProductPrice
	}
	return nil
}

// OutboxEvent represents an atomic transactional event to be captured by CDC (Debezium).
type OutboxEvent struct {
	ID            string    `json:"id"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	EventType     string    `json:"event_type"`
	Payload       string    `json:"payload"`
	CreatedAt     time.Time `json:"created_at"`
}

// SearchFilter represents search query parameters for OpenSearch.
type SearchFilter struct {
	Query      string `json:"query"`
	CategoryID string `json:"category_id"`
	Brand      string `json:"brand"`
	MinPrice   *int64 `json:"min_price"`
	MaxPrice   *int64 `json:"max_price"`
	Page       int    `json:"page"`
	Size       int    `json:"size"`
}

// SearchHit represents a matched document in search results.
type SearchHit struct {
	ID     string  `json:"id"`
	Score  float64 `json:"score"`
	Source Product `json:"source"`
}

// SearchResult contains pagination metadata and matching items.
type SearchResult struct {
	TotalHits int64       `json:"total_hits"`
	TookMs    int64       `json:"took_ms"`
	Hits      []SearchHit `json:"hits"`
}
