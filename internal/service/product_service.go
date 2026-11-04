package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/core/ports"
	"github.com/jules/cdc-postgres-opensearch/internal/observability"
)

type productService struct {
	repo ports.ProductRepository
}

// NewProductService initializes a new domain product service.
func NewProductService(repo ports.ProductRepository) ports.ProductService {
	return &productService{repo: repo}
}

func (s *productService) CreateProduct(ctx context.Context, input domain.CreateProductInput) (*domain.Product, error) {
	ctx, span := observability.StartSpan(ctx, "ProductService.CreateProduct")
	defer span.End()

	if err := input.Validate(); err != nil {
		return nil, err
	}

	isActive := true
	if input.IsActive != nil {
		isActive = *input.IsActive
	}

	now := time.Now().UTC()
	product := &domain.Product{
		ID:            uuid.NewString(),
		Name:          input.Name,
		Description:   input.Description,
		PriceCents:    input.PriceCents,
		StockQuantity: input.StockQuantity,
		CategoryID:    input.CategoryID,
		Brand:         input.Brand,
		Attributes:    input.Attributes,
		IsActive:      isActive,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	payloadBytes, err := json.Marshal(product)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal outbox payload: %w", err)
	}

	outboxEvent := &domain.OutboxEvent{
		ID:            uuid.NewString(),
		AggregateType: "product",
		AggregateID:   product.ID,
		EventType:     "ProductCreated",
		Payload:       string(payloadBytes),
		CreatedAt:     now,
	}

	if err := s.repo.CreateProductWithOutbox(ctx, product, outboxEvent); err != nil {
		return nil, err
	}

	return product, nil
}

func (s *productService) GetProduct(ctx context.Context, id string) (*domain.Product, error) {
	ctx, span := observability.StartSpan(ctx, "ProductService.GetProduct")
	defer span.End()

	return s.repo.GetProductByID(ctx, id)
}

func (s *productService) DeleteProduct(ctx context.Context, id string) error {
	ctx, span := observability.StartSpan(ctx, "ProductService.DeleteProduct")
	defer span.End()

	now := time.Now().UTC()
	payloadBytes, _ := json.Marshal(map[string]string{"id": id})

	outboxEvent := &domain.OutboxEvent{
		ID:            uuid.NewString(),
		AggregateType: "product",
		AggregateID:   id,
		EventType:     "ProductDeleted",
		Payload:       string(payloadBytes),
		CreatedAt:     now,
	}

	return s.repo.DeleteProductWithOutbox(ctx, id, outboxEvent)
}
