package ports

import (
	"context"

	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
)

// ProductRepository defines atomic persistence operations on PostgreSQL.
type ProductRepository interface {
	CreateProductWithOutbox(ctx context.Context, product *domain.Product, event *domain.OutboxEvent) error
	GetProductByID(ctx context.Context, id string) (*domain.Product, error)
	DeleteProductWithOutbox(ctx context.Context, id string, event *domain.OutboxEvent) error
}

// SearchRepository defines search engine operations on OpenSearch.
type SearchRepository interface {
	Search(ctx context.Context, filter domain.SearchFilter) (*domain.SearchResult, error)
	BulkUpsert(ctx context.Context, products []domain.Product) error
	BulkDelete(ctx context.Context, ids []string) error
	HealthCheck(ctx context.Context) error
}

// ProductService defines domain business operations.
type ProductService interface {
	CreateProduct(ctx context.Context, input domain.CreateProductInput) (*domain.Product, error)
	GetProduct(ctx context.Context, id string) (*domain.Product, error)
	DeleteProduct(ctx context.Context, id string) error
}

// SearchService defines application search operations.
type SearchService interface {
	SearchProducts(ctx context.Context, filter domain.SearchFilter) (*domain.SearchResult, error)
}
