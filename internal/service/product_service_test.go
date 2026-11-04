package service_test

import (
	"context"
	"testing"

	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/service"
)

type mockProductRepo struct {
	createdProduct *domain.Product
	createdEvent   *domain.OutboxEvent
	deletedID      string
	deletedEvent   *domain.OutboxEvent
	getReturn      *domain.Product
	getErr         error
}

func (m *mockProductRepo) CreateProductWithOutbox(ctx context.Context, product *domain.Product, event *domain.OutboxEvent) error {
	m.createdProduct = product
	m.createdEvent = event
	return nil
}

func (m *mockProductRepo) GetProductByID(ctx context.Context, id string) (*domain.Product, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.getReturn, nil
}

func (m *mockProductRepo) DeleteProductWithOutbox(ctx context.Context, id string, event *domain.OutboxEvent) error {
	m.deletedID = id
	m.deletedEvent = event
	return nil
}

func TestProductService_CreateProduct(t *testing.T) {
	repo := &mockProductRepo{}
	svc := service.NewProductService(repo)

	input := domain.CreateProductInput{
		Name:          "Smart Watch",
		Description:   "Waterproof smartwatch",
		PriceCents:    49999,
		StockQuantity: 30,
		CategoryID:    "wearables",
		Brand:         "Smarty",
	}

	prod, err := svc.CreateProduct(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if prod.ID == "" {
		t.Errorf("expected generated UUID, got empty")
	}
	if prod.Name != input.Name {
		t.Errorf("expected product name %s, got %s", input.Name, prod.Name)
	}
	if repo.createdProduct == nil || repo.createdProduct.ID != prod.ID {
		t.Errorf("repo should have received product")
	}
	if repo.createdEvent == nil || repo.createdEvent.EventType != "ProductCreated" {
		t.Errorf("repo should have received outbox event 'ProductCreated'")
	}
}

func TestProductService_DeleteProduct(t *testing.T) {
	repo := &mockProductRepo{}
	svc := service.NewProductService(repo)

	err := svc.DeleteProduct(context.Background(), "prod-to-delete")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if repo.deletedID != "prod-to-delete" {
		t.Errorf("expected deletedID 'prod-to-delete', got %s", repo.deletedID)
	}
	if repo.deletedEvent == nil || repo.deletedEvent.EventType != "ProductDeleted" {
		t.Errorf("expected outbox event 'ProductDeleted'")
	}
}
