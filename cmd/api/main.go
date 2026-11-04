package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jules/cdc-postgres-opensearch/internal/api"
	"github.com/jules/cdc-postgres-opensearch/internal/api/handlers"
	"github.com/jules/cdc-postgres-opensearch/internal/observability"
	"github.com/jules/cdc-postgres-opensearch/internal/repository/opensearch"
	"github.com/jules/cdc-postgres-opensearch/internal/repository/postgres"
	"github.com/jules/cdc-postgres-opensearch/internal/service"
	"github.com/jules/cdc-postgres-opensearch/pkg/logger"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	logLevel := getEnv("LOG_LEVEL", "info")
	l := logger.Init(logLevel)

	l.Info("Starting E-Commerce Search & CDC API service")

	cleanupOtel, err := observability.InitObservability("cdc-api")
	if err != nil {
		l.Error("Failed to init OpenTelemetry", "error", err)
	} else {
		defer cleanupOtel(context.Background()) //nolint:errcheck
	}

	pgConnStr := getEnv("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/ecommerce?sslmode=disable")
	osURL := getEnv("OPENSEARCH_URL", "http://localhost:9200")
	osIndex := getEnv("OPENSEARCH_INDEX", "products")
	port := getEnv("PORT", "8080")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pgPool, err := postgres.NewPGXPool(ctx, pgConnStr)
	if err != nil {
		l.Warn("Postgres connection pending or failed", "error", err)
	} else {
		defer pgPool.Close()
	}

	osClient, err := opensearch.NewClient([]string{osURL}, "", "")
	if err != nil {
		l.Error("Failed to create OpenSearch client", "error", err)
		os.Exit(1)
	}

	productRepo := postgres.NewProductRepository(pgPool)
	searchRepo := opensearch.NewSearchRepository(osClient, osIndex)

	productSvc := service.NewProductService(productRepo)
	searchSvc := service.NewSearchService(searchRepo)

	productHdl := handlers.NewProductHandler(productSvc)
	searchHdl := handlers.NewSearchHandler(searchSvc)

	router := api.NewRouter(productHdl, searchHdl, searchRepo)

	addr := ":" + port
	if !strings.HasPrefix(port, ":") {
		addr = ":" + port
	}

	server := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		l.Info("API Server listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			l.Error("Server listen error", "error", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	l.Info("Shutting down API server gracefully...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		l.Error("API server forced to shutdown", "error", err)
	}

	l.Info("API server exiting successfully")
}
