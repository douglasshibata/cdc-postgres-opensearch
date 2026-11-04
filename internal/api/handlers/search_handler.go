package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/core/ports"
)

type SearchHandler struct {
	service ports.SearchService
}

func NewSearchHandler(service ports.SearchService) *SearchHandler {
	return &SearchHandler{service: service}
}

func (h *SearchHandler) Search(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query().Get("q")
	categoryID := r.URL.Query().Get("category_id")
	brand := r.URL.Query().Get("brand")

	page := 1
	if pageStr := r.URL.Query().Get("page"); pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}

	size := 20
	if sizeStr := r.URL.Query().Get("size"); sizeStr != "" {
		if s, err := strconv.Atoi(sizeStr); err == nil && s > 0 && s <= 100 {
			size = s
		}
	}

	var minPrice *int64
	if minStr := r.URL.Query().Get("min_price"); minStr != "" {
		if mp, err := strconv.ParseInt(minStr, 10, 64); err == nil {
			minPrice = &mp
		}
	}

	var maxPrice *int64
	if maxStr := r.URL.Query().Get("max_price"); maxStr != "" {
		if mp, err := strconv.ParseInt(maxStr, 10, 64); err == nil {
			maxPrice = &mp
		}
	}

	filter := domain.SearchFilter{
		Query:      q,
		CategoryID: categoryID,
		Brand:      brand,
		MinPrice:   minPrice,
		MaxPrice:   maxPrice,
		Page:       page,
		Size:       size,
	}

	result, err := h.service.SearchProducts(r.Context(), filter)
	if err != nil {
		http.Error(w, "Search Error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
