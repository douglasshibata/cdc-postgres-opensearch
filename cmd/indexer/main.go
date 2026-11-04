package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jules/cdc-postgres-opensearch/internal/observability"
	"github.com/jules/cdc-postgres-opensearch/internal/repository/opensearch"
	"github.com/jules/cdc-postgres-opensearch/internal/sync"
	"github.com/jules/cdc-postgres-opensearch/pkg/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	l.Info("Starting CDC Indexer Worker service")

	cleanupOtel, err := observability.InitObservability("cdc-indexer")
	if err != nil {
		l.Error("Failed to init OpenTelemetry", "error", err)
	} else {
		defer cleanupOtel(context.Background()) //nolint:errcheck
	}

	metricsPort := getEnv("METRICS_PORT", "9091")
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		})
		l.Info("Worker metrics server listening", "port", metricsPort)
		_ = http.ListenAndServe(":"+metricsPort, mux)
	}()

	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaBrokers := strings.Split(kafkaBrokersStr, ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "cdc.ecommerce.outbox")
	kafkaGroupID := getEnv("KAFKA_GROUP_ID", "opensearch-sync-indexer")

	osURL := getEnv("OPENSEARCH_URL", "http://localhost:9200")
	osIndex := getEnv("OPENSEARCH_INDEX", "products")

	batchSize := 100
	if bsStr := getEnv("BATCH_SIZE", "100"); bsStr != "" {
		if bs, err := strconv.Atoi(bsStr); err == nil && bs > 0 {
			batchSize = bs
		}
	}

	flushInterval := 2 * time.Second
	if fiStr := getEnv("FLUSH_INTERVAL", "2s"); fiStr != "" {
		if fi, err := time.ParseDuration(fiStr); err == nil {
			flushInterval = fi
		}
	}

	osClient, err := opensearch.NewClient([]string{osURL}, "", "")
	if err != nil {
		l.Error("Failed to initialize OpenSearch client", "error", err)
		os.Exit(1)
	}

	searchRepo := opensearch.NewSearchRepository(osClient, osIndex)

	worker := sync.NewKafkaConsumerWorker(
		kafkaBrokers,
		kafkaTopic,
		kafkaGroupID,
		searchRepo,
		l,
		batchSize,
		flushInterval,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stop
		l.Info("Termination signal received. Stopping worker...")
		cancel()
	}()

	if err := worker.Start(ctx); err != nil {
		l.Error("Worker terminated with error", "error", err)
	}

	l.Info("CDC Indexer Worker shutdown complete")
}
