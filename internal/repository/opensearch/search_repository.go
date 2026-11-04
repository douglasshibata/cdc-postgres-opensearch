package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/core/ports"
	osClient "github.com/opensearch-project/opensearch-go/v2"
	osAPI "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type searchRepository struct {
	client *osClient.Client
	index  string
}

// NewSearchRepository creates an OpenSearch-backed SearchRepository.
func NewSearchRepository(client *osClient.Client, index string) ports.SearchRepository {
	return &searchRepository{
		client: client,
		index:  index,
	}
}

// NewClient initializes an OpenSearch client.
func NewClient(addresses []string, username, password string) (*osClient.Client, error) {
	cfg := osClient.Config{
		Addresses: addresses,
		Username:  username,
		Password:  password,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return osClient.NewClient(cfg)
}

func (r *searchRepository) HealthCheck(ctx context.Context) error {
	req := osAPI.PingRequest{}
	res, err := req.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("opensearch ping failed: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("opensearch returned status %s", res.Status())
	}
	return nil
}

// Search executes query against OpenSearch index with fuzzy multi-match and facet filters.
func (r *searchRepository) Search(ctx context.Context, filter domain.SearchFilter) (*domain.SearchResult, error) {
	var mustClauses []map[string]any

	if strings.TrimSpace(filter.Query) != "" {
		mustClauses = append(mustClauses, map[string]any{
			"multi_match": map[string]any{
				"query":     filter.Query,
				"fields":    []string{"name^3", "name.suggest^2", "description", "brand^2"},
				"fuzziness": "AUTO",
			},
		})
	} else {
		mustClauses = append(mustClauses, map[string]any{
			"match_all": map[string]any{},
		})
	}

	var filterClauses []map[string]any
	filterClauses = append(filterClauses, map[string]any{
		"term": map[string]any{"is_active": true},
	})

	if filter.CategoryID != "" {
		filterClauses = append(filterClauses, map[string]any{
			"term": map[string]any{"category_id": filter.CategoryID},
		})
	}
	if filter.Brand != "" {
		filterClauses = append(filterClauses, map[string]any{
			"term": map[string]any{"brand": filter.Brand},
		})
	}
	if filter.MinPrice != nil || filter.MaxPrice != nil {
		rangeQuery := map[string]any{}
		if filter.MinPrice != nil {
			rangeQuery["gte"] = *filter.MinPrice
		}
		if filter.MaxPrice != nil {
			rangeQuery["lte"] = *filter.MaxPrice
		}
		filterClauses = append(filterClauses, map[string]any{
			"range": map[string]any{"price_cents": rangeQuery},
		})
	}

	page := filter.Page
	if page < 1 {
		page = 1
	}
	size := filter.Size
	if size <= 0 || size > 100 {
		size = 20
	}
	from := (page - 1) * size

	queryBody := map[string]any{
		"from": from,
		"size": size,
		"query": map[string]any{
			"bool": map[string]any{
				"must":   mustClauses,
				"filter": filterClauses,
			},
		},
	}

	reqJSON, err := json.Marshal(queryBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search query: %w", err)
	}

	searchReq := osAPI.SearchRequest{
		Index: []string{r.index},
		Body:  bytes.NewReader(reqJSON),
	}

	res, err := searchReq.Do(ctx, r.client)
	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("search error %s: %s", res.Status(), string(body))
	}

	var esRes struct {
		Took int64 `json:"took"`
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				ID     string          `json:"_id"`
				Score  float64         `json:"_score"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&esRes); err != nil {
		return nil, fmt.Errorf("failed to decode search response: %w", err)
	}

	hits := make([]domain.SearchHit, 0, len(esRes.Hits.Hits))
	for _, h := range esRes.Hits.Hits {
		var p domain.Product
		if err := json.Unmarshal(h.Source, &p); err != nil {
			continue
		}
		hits = append(hits, domain.SearchHit{
			ID:     h.ID,
			Score:  h.Score,
			Source: p,
		})
	}

	return &domain.SearchResult{
		TotalHits: esRes.Hits.Total.Value,
		TookMs:    esRes.Took,
		Hits:      hits,
	}, nil
}

// BulkUpsert executes bulk index/upsert into OpenSearch.
func (r *searchRepository) BulkUpsert(ctx context.Context, products []domain.Product) error {
	if len(products) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, p := range products {
		meta := fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}%s`, r.index, p.ID, "\n")
		data, err := json.Marshal(p)
		if err != nil {
			return fmt.Errorf("failed to marshal product for bulk: %w", err)
		}
		buf.WriteString(meta)
		buf.Write(data)
		buf.WriteString("\n")
	}

	bulkReq := osAPI.BulkRequest{
		Index: r.index,
		Body:  &buf,
	}

	res, err := bulkReq.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("failed to execute bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("bulk request failed %s: %s", res.Status(), string(body))
	}

	return nil
}

// BulkDelete deletes multiple documents in a single bulk operation.
func (r *searchRepository) BulkDelete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, id := range ids {
		meta := fmt.Sprintf(`{"delete":{"_index":"%s","_id":"%s"}}%s`, r.index, id, "\n")
		buf.WriteString(meta)
	}

	bulkReq := osAPI.BulkRequest{
		Index: r.index,
		Body:  &buf,
	}

	res, err := bulkReq.Do(ctx, r.client)
	if err != nil {
		return fmt.Errorf("failed to execute bulk delete: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		body, _ := io.ReadAll(res.Body)
		return fmt.Errorf("bulk delete failed %s: %s", res.Status(), string(body))
	}

	return nil
}
