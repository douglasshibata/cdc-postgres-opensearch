package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/core/ports"
	"github.com/jules/cdc-postgres-opensearch/internal/observability"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/metric"
)

// DebeziumPayload represents the payload part of a standard Debezium CDC event or SMT Outbox message.
type DebeziumPayload struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Op     string          `json:"op"` // 'c' (create), 'u' (update), 'd' (delete), 'r' (read/snapshot)
	TsMs   int64           `json:"ts_ms"`
}

// DebeziumEvent represents the envelope structure sent by Kafka Connect / Debezium.
type DebeziumEvent struct {
	Schema  json.RawMessage `json:"schema,omitempty"`
	Payload DebeziumPayload `json:"payload"`
	// In case of Outbox Event Router SMT where payload itself is flattened
	EventType string          `json:"event_type,omitempty"`
	ID        string          `json:"id,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// KafkaConsumerWorker manages Kafka streaming ingestion and bulk indexing into OpenSearch.
type KafkaConsumerWorker struct {
	reader         *kafka.Reader
	searchRepo     ports.SearchRepository
	logger         *slog.Logger
	batchSize      int
	flushInterval  time.Duration
}

// NewKafkaConsumerWorker instantiates a new worker instance.
func NewKafkaConsumerWorker(
	brokers []string,
	topic string,
	groupID string,
	searchRepo ports.SearchRepository,
	logger *slog.Logger,
	batchSize int,
	flushInterval time.Duration,
) *KafkaConsumerWorker {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 1 * time.Second,
	})

	return &KafkaConsumerWorker{
		reader:        reader,
		searchRepo:    searchRepo,
		logger:        logger,
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}
}

// ParseCDCMessage decodes Debezium event JSON and determines the operation (index product or delete id).
func ParseCDCMessage(msgValue []byte) (*domain.Product, string, string, time.Time, error) {
	// 1. Try direct Debezium envelope
	var env DebeziumEvent
	if err := json.Unmarshal(msgValue, &env); err != nil {
		return nil, "", "", time.Time{}, fmt.Errorf("failed to decode event json: %w", err)
	}

	var eventTime time.Time
	if env.Payload.TsMs > 0 {
		eventTime = time.UnixMilli(env.Payload.TsMs)
	} else {
		eventTime = time.Now()
	}

	// Case A: Debezium standard CDC envelope with 'op'
	if env.Payload.Op != "" {
		op := env.Payload.Op
		switch op {
		case "c", "u", "r":
			// Target is inside Payload.After
			var p domain.Product
			// Check if payload has direct product fields or nested payload string (Outbox pattern)
			var outboxRecord struct {
				AggregateID string `json:"aggregate_id"`
				EventType   string `json:"event_type"`
				Payload     string `json:"payload"`
			}
			if err := json.Unmarshal(env.Payload.After, &outboxRecord); err == nil && outboxRecord.Payload != "" {
				if strings.Contains(strings.ToLower(outboxRecord.EventType), "delete") {
					return nil, outboxRecord.AggregateID, "delete", eventTime, nil
				}
				if err := json.Unmarshal([]byte(outboxRecord.Payload), &p); err == nil && p.ID != "" {
					return &p, "", "upsert", eventTime, nil
				}
			}

			if err := json.Unmarshal(env.Payload.After, &p); err == nil && p.ID != "" {
				return &p, "", "upsert", eventTime, nil
			}

		case "d":
			// Delete operation
			var p domain.Product
			_ = json.Unmarshal(env.Payload.Before, &p)
			if p.ID != "" {
				return nil, p.ID, "delete", eventTime, nil
			}
		}
	}

	// Case B: Flattened Outbox message format (e.g. from Debezium Outbox SMT router)
	var directProduct domain.Product
	if err := json.Unmarshal(msgValue, &directProduct); err == nil && directProduct.ID != "" {
		return &directProduct, "", "upsert", eventTime, nil
	}

	return nil, "", "", eventTime, fmt.Errorf("unrecognized CDC event format")
}

// Start begins processing the Kafka stream until context cancellation.
func (w *KafkaConsumerWorker) Start(ctx context.Context) error {
	w.logger.Info("Starting CDC Kafka consumer worker", "batchSize", w.batchSize, "flushInterval", w.flushInterval)

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	upsertBuffer := make([]domain.Product, 0, w.batchSize)
	deleteBuffer := make([]string, 0, w.batchSize)
	messagesToCommit := make([]kafka.Message, 0, w.batchSize)

	flush := func() {
		if len(upsertBuffer) == 0 && len(deleteBuffer) == 0 {
			return
		}

		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		flushCtx, span := observability.StartSpan(flushCtx, "CDC.FlushBatchToOpenSearch")
		defer span.End()

		if len(upsertBuffer) > 0 {
			if err := w.searchRepo.BulkUpsert(flushCtx, upsertBuffer); err != nil {
				w.logger.Error("Failed to bulk upsert to OpenSearch", "error", err, "count", len(upsertBuffer))
			} else {
				w.logger.Info("Successfully indexed batch into OpenSearch", "count", len(upsertBuffer))
				if observability.BulkItemsCounter != nil {
					observability.BulkItemsCounter.Add(flushCtx, int64(len(upsertBuffer)))
				}
			}
		}

		if len(deleteBuffer) > 0 {
			if err := w.searchRepo.BulkDelete(flushCtx, deleteBuffer); err != nil {
				w.logger.Error("Failed to bulk delete from OpenSearch", "error", err, "count", len(deleteBuffer))
			} else {
				w.logger.Info("Successfully deleted batch from OpenSearch", "count", len(deleteBuffer))
			}
		}

		if len(messagesToCommit) > 0 {
			if err := w.reader.CommitMessages(flushCtx, messagesToCommit...); err != nil {
				w.logger.Warn("Failed to commit Kafka offsets", "error", err)
			}
		}

		upsertBuffer = upsertBuffer[:0]
		deleteBuffer = deleteBuffer[:0]
		messagesToCommit = messagesToCommit[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return w.reader.Close()

		case <-ticker.C:
			flush()

		default:
			readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			msg, err := w.reader.FetchMessage(readCtx)
			cancel()

			if err != nil {
				if err == context.Canceled || err == context.DeadlineExceeded {
					continue
				}
				w.logger.Warn("Error fetching message from Kafka", "error", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}

			product, deleteID, op, eventTime, parseErr := ParseCDCMessage(msg.Value)
			if parseErr != nil {
				w.logger.Warn("Skipping unparseable CDC message", "error", parseErr, "offset", msg.Offset)
				_ = w.reader.CommitMessages(ctx, msg)
				continue
			}

			// Record sync lag metric
			lagSeconds := time.Since(eventTime).Seconds()
			if lagSeconds >= 0 && observability.SyncLagHistogram != nil {
				observability.SyncLagHistogram.Record(ctx, lagSeconds, metric.WithAttributes())
			}

			switch op {
			case "upsert":
				if product != nil {
					upsertBuffer = append(upsertBuffer, *product)
				}
			case "delete":
				if deleteID != "" {
					deleteBuffer = append(deleteBuffer, deleteID)
				}
			}

			messagesToCommit = append(messagesToCommit, msg)

			if len(upsertBuffer)+len(deleteBuffer) >= w.batchSize {
				flush()
			}
		}
	}
}
