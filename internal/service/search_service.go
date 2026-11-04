package service

import (
	"context"
	"time"

	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/core/ports"
	"github.com/jules/cdc-postgres-opensearch/internal/observability"
	"go.opentelemetry.io/otel/metric"
)

type searchService struct {
	repo ports.SearchRepository
}

// NewSearchService initializes application search service.
func NewSearchService(repo ports.SearchRepository) ports.SearchService {
	return &searchService{repo: repo}
}

func (s *searchService) SearchProducts(ctx context.Context, filter domain.SearchFilter) (*domain.SearchResult, error) {
	ctx, span := observability.StartSpan(ctx, "SearchService.SearchProducts")
	defer span.End()

	start := time.Now()
	result, err := s.repo.Search(ctx, filter)
	durationSec := time.Since(start).Seconds()

	if observability.SearchDurationHist != nil {
		observability.SearchDurationHist.Record(ctx, durationSec, metric.WithAttributes())
	}

	if err != nil {
		return nil, err
	}

	return result, nil
}
