# CDC PostgreSQL to OpenSearch Sync & E-Commerce Search Engine

High-performance, event-driven Change Data Capture (CDC) synchronization pipeline connecting **PostgreSQL 16** and **OpenSearch 2.x** using the **Transactional Outbox Pattern**, **Apache Kafka (KRaft)**, **Debezium**, **OpenTelemetry**, and **Go (Golang Standard Layout)**.

👉 **[Tutorial passo a passo em Português](TUTORIAL.md)**

---

## 🏗️ Architecture & Data Flow

```
                      ┌────────────────────────────────────────┐
                      │            Write Mutation              │
                      │   (ACID Transaction: Product + Outbox) │
                      ▼                                        │
┌─────────────────────────┐                                    │
│   Go REST API           │────────────────────────────────────┘
│   (cmd/api)             │
│   - POST /products      │
│   - GET  /search (fast) │◀──────────┐
└─────────────────────────┘           │
            │                         │ Search Query (< 15ms)
            ▼                         │ (Fuzzy, Edge-nGram Autocomplete)
┌─────────────────────────┐           │
│   PostgreSQL 16         │           │
│   - products table      │           │
│   - outbox_events table │           │
└─────────────────────────┘           │
            │                         │
            ▼ (Logical Replication /  │
               wal_level=logical)     │
┌─────────────────────────┐           │
│   Debezium Connector    │           │
└─────────────────────────┘           │
            │                         │
            ▼ (CDC JSON Events)       │
┌─────────────────────────┐           │
│   Apache Kafka          │           │
│   Topic: cdc.outbox     │           │
└─────────────────────────┘           │
            │                         │
            ▼ (Bulk Stream Ingestion) │
┌─────────────────────────┐           │
│   Go Indexer Worker     │           │
│   (cmd/indexer)         │           │
└─────────────────────────┘           │
            │                         │
            ▼ (Bulk Upsert/Delete)    │
┌─────────────────────────┐           │
│   OpenSearch 2.x        │───────────┘
│   Index: products       │
└─────────────────────────┘
            │
            ▼
┌─────────────────────────┐
│   Observability Stack   │
│   OTel Collector        │──▶ Prometheus ──▶ Grafana Dashboard
│   Sync Lag & Latencies  │
└─────────────────────────┘
```

### Key Architectural Tenets
1. **Zero Double-Writes:** The Go API writes solely to PostgreSQL. In the same ACID transaction, it records a domain event in the `outbox_events` table.
2. **Eventual Consistency via CDC:** Debezium reads PostgreSQL's Write-Ahead Log (`WAL`) via logical replication slots and streams events into Kafka partitioned by `product_id`.
3. **Decoupled Resilient Consumer:** The Go Indexer Worker buffers events in memory and flushes batches into OpenSearch via `BulkRequest`. If OpenSearch is temporarily unreachable, messages remain buffered in Kafka with zero data loss.
4. **End-to-End Observability:** OpenTelemetry traces request life-cycles and records vital KPIs:
   - `cdc_sync_lag_seconds`: Time elapsed from database commit to OpenSearch indexing.
   - `opensearch_query_duration_seconds`: Search execution p95 and p99 percentiles.
   - `cdc_bulk_items_indexed_total`: Throughput of indexed documents.

---

## 📁 Project Directory Structure (Golang Standard Layout)

```
cdc-postgres-opensearch/
├── cmd/
│   ├── api/                  # REST API entrypoint (HTTP mutations + search)
│   │   └── main.go
│   └── indexer/              # CDC Sync Worker entrypoint (Kafka -> OpenSearch bulk)
│       └── main.go
├── internal/
│   ├── core/
│   │   ├── domain/           # Business entities (Product, OutboxEvent, SearchFilter)
│   │   └── ports/            # Repository and Service interfaces
│   ├── api/
│   │   ├── handlers/         # HTTP handlers for Products and Search
│   │   └── router.go         # Route definitions and metrics endpoint
│   ├── service/              # Use cases (ProductService, SearchService)
│   ├── repository/
│   │   ├── postgres/         # Atomic PostgreSQL pgx transaction implementation
│   │   └── opensearch/       # OpenSearch queries & bulk upsert/delete operations
│   ├── sync/                 # Kafka streaming consumer & Debezium CDC parser
│   └── observability/        # OpenTelemetry tracers, meters, and Prometheus metrics
├── pkg/
│   └── logger/               # Structured logging using slog
├── deployments/              # Infrastructure as Code
│   ├── docker-compose.yml    # Postgres, Kafka, Debezium, OpenSearch, OTel, Prometheus, Grafana
│   ├── otel-collector/       # OpenTelemetry Collector pipeline configuration
│   └── grafana/dashboards/   # Pre-configured Grafana observability dashboard
├── scripts/
│   ├── migrations/           # PostgreSQL DDL migrations (products, outbox_events, publication)
│   └── elastic_mappings/     # OpenSearch index schema with edge_ngram analyzer
├── Dockerfile                # Multi-stage production container build
├── go.mod
└── go.sum
```

---

## 🚀 Quick Start

### 1. Start Infrastructure
```bash
docker compose up -d
```

### 2. Apply OpenSearch Mapping
```bash
curl -X PUT http://localhost:9200/products \
  -H "Content-Type: application/json" \
  -d @scripts/elastic_mappings/products.json
```

### 3. Register Debezium Connector
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-outbox-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "tasks.max": "1",
      "plugin.name": "pgoutput",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgrespassword",
      "database.dbname": "ecommerce",
      "database.server.name": "ecommerce",
      "table.include.list": "public.outbox_events,public.products",
      "topic.prefix": "cdc.ecommerce"
    }
  }'
```

### 4. Run Locally (Go)
```bash
# Terminal 1 - Sync Worker
go run cmd/indexer/main.go

# Terminal 2 - REST API
go run cmd/api/main.go
```

---

## 📡 API Reference

### Create Product (Writes to Postgres + Outbox)
```bash
curl -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Smartphone Ultra 5G 128GB",
    "description": "High-end smartphone with OLED display",
    "price_cents": 349900,
    "stock_quantity": 40,
    "category_id": "smartphones",
    "brand": "TechCorp",
    "attributes": {
      "color": "Midnight Black",
      "ram": "8GB"
    }
  }'
```

### Real-Time Search (Fast OpenSearch Query)
```bash
# Autocomplete search by partial name
curl "http://localhost:8080/api/v1/search?q=Smart&category_id=smartphones"
```

### Health & Metrics
- Liveness: `GET http://localhost:8080/healthz`
- Readiness: `GET http://localhost:8080/readyz`
- Prometheus Metrics: `GET http://localhost:8080/metrics`

---

## 🧪 Running Tests

Execute the unit and integration test suite:

```bash
go test -v -race ./...
```
