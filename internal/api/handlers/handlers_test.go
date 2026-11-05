package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jules/cdc-postgres-opensearch/internal/api"
	"github.com/jules/cdc-postgres-opensearch/internal/api/handlers"
	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/service"
)

type mockProductRepo struct {
	products map[string]*domain.Product
}

func (m *mockProductRepo) CreateProductWithOutbox(ctx context.Context, product *domain.Product, event *domain.OutboxEvent) error {
	m.products[product.ID] = product
	return nil
}

func (m *mockProductRepo) GetProductByID(ctx context.Context, id string) (*domain.Product, error) {
	if p, ok := m.products[id]; ok {
		return p, nil
	}
	return nil, domain.ErrProductNotFound
}

func (m *mockProductRepo) DeleteProductWithOutbox(ctx context.Context, id string, event *domain.OutboxEvent) error {
	if _, ok := m.products[id]; !ok {
		return domain.ErrProductNotFound
	}
	delete(m.products, id)
	return nil
}

type mockSearchRepo struct {
	healthy bool
}

func (m *mockSearchRepo) Search(ctx context.Context, filter domain.SearchFilter) (*domain.SearchResult, error) {
	return &domain.SearchResult{
		TotalHits: 1,
		TookMs:    2,
		Hits: []domain.SearchHit{
			{
				ID:    "p1",
				Score: 1.5,
				Source: domain.Product{
					ID:   "p1",
					Name: "Test Product",
				},
			},
		},
	}, nil
}

func (m *mockSearchRepo) BulkUpsert(ctx context.Context, products []domain.Product) error {
	return nil
}

func (m *mockSearchRepo) BulkDelete(ctx context.Context, ids []string) error {
	return nil
}

func (m *mockSearchRepo) HealthCheck(ctx context.Context) error {
	if !m.healthy {
		return domain.ErrProductNotFound
	}
	return nil
}

func TestHandlers_EndToEndRoutes(t *testing.T) {
	pRepo := &mockProductRepo{products: make(map[string]*domain.Product)}
	sRepo := &mockSearchRepo{healthy: true}

	pSvc := service.NewProductService(pRepo)
	sSvc := service.NewSearchService(sRepo)

	pHdl := handlers.NewProductHandler(pSvc)
	sHdl := handlers.NewSearchHandler(sSvc)
	aHdl := handlers.NewAuthHandler(nil)

	router := api.NewRouter(pHdl, sHdl, aHdl, sRepo, nil)

	// 1. Health check
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected /healthz status 200, got %d", rr.Code)
	}

	// 2. Create product
	body := []byte(`{"name":"Gaming Mouse","price_cents":15000,"category_id":"accessories"}`)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/products", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected create status 201, got %d: %s", rr.Code, rr.Body.String())
	}

	var created domain.Product
	if err := json.Unmarshal(rr.Body.Bytes(), &created); err != nil {
		t.Fatalf("failed to decode created product: %v", err)
	}
	if created.ID == "" || created.Name != "Gaming Mouse" {
		t.Errorf("unexpected created product data: %+v", created)
	}

	// 3. Get product
	req = httptest.NewRequest(http.MethodGet, "/api/v1/products?id="+created.ID, nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected get product status 200, got %d", rr.Code)
	}

	// 4. Search products
	req = httptest.NewRequest(http.MethodGet, "/api/v1/search?q=Gaming", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected search status 200, got %d", rr.Code)
	}

	// 5. Delete product
	req = httptest.NewRequest(http.MethodDelete, "/api/v1/products?id="+created.ID, nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Errorf("expected delete status 204, got %d", rr.Code)
	}
}
