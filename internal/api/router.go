package api

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/jules/cdc-postgres-opensearch/internal/api/handlers"
	"github.com/jules/cdc-postgres-opensearch/internal/api/middleware"
	"github.com/jules/cdc-postgres-opensearch/internal/core/ports"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Router registers all application endpoints and returns standard http.Handler.
func NewRouter(
	productHandler *handlers.ProductHandler,
	searchHandler *handlers.SearchHandler,
	authHandler *handlers.AuthHandler,
	searchRepo ports.SearchRepository,
	logger *slog.Logger,
) http.Handler {
	mux := http.NewServeMux()

	// Business routes
	mux.HandleFunc("/api/v1/products", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			productHandler.CreateProduct(w, r)
		case http.MethodGet:
			productHandler.GetProduct(w, r)
		case http.MethodDelete:
			productHandler.DeleteProduct(w, r)
		default:
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/search", searchHandler.Search)

	// Auth and security audit simulation
	if authHandler != nil {
		mux.HandleFunc("/api/v1/auth/login", authHandler.Login)
	}

	// Health and metrics
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := searchRepo.HealthCheck(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"status": "unavailable",
				"error":  err.Error(),
			})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})

	mux.Handle("/metrics", promhttp.Handler())

	if logger == nil {
		logger = slog.Default()
	}

	return middleware.RequestLogger(logger)(mux)
}
