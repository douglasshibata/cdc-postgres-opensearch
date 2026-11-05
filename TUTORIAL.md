# 📘 Tutorial de Operação e Instalação: CDC PostgreSQL to OpenSearch Sync

Guia prático e passo a passo em português para configurar, executar e validar a sincronização CDC resiliente entre **PostgreSQL 16** e **OpenSearch 2.x** via **Debezium**, **Apache Kafka** e **Go**.

---

## 🎯 1. Visão Geral da Arquitetura

O padrão **Change Data Capture (CDC)** com **Transactional Outbox** resolve o problema clássico de escrita dupla (*dual-write problem*).
Em vez de gravar simultaneamente no banco de dados e no buscador (o que causaria inconsistências se um falhar), a API Go executa uma única transação ACID no PostgreSQL:
1. Grava os dados na tabela `products`.
2. Grava um registro de evento na tabela `outbox_events`.
3. O Debezium captura o log binário de replicação do PostgreSQL (`WAL - Write Ahead Log`) e publica as mensagens no Kafka.
4. O Worker Go (`cmd/indexer`) consome as mensagens em lote e executa operações de `BulkUpsert` ou `BulkDelete` no OpenSearch.
5. A busca na API Go (`GET /api/v1/search`) lê diretamente do OpenSearch com tempos de resposta na faixa de milissegundos.

---

## 📋 2. Pré-requisitos

- **Go 1.24+**: Para compilação e execução local.
- **Docker & Docker Compose**: Para orquestração de toda a stack (PostgreSQL, Kafka, Debezium, OpenSearch, OTel, Prometheus, Grafana).
- **cURL e jq**: Utilitários para testar requisições HTTP e inspecionar JSONs.

---

## 🚀 3. Passo a Passo de Execução

### Passo 1: Inicializar a Infraestrutura Containerizada
No diretório `cdc-postgres-opensearch`:
```bash
docker compose up -d
```
Verifique se todos os serviços estão saudáveis com `docker compose ps`.

### Passo 2: Criar o Índice e Mappings no OpenSearch
Execute a chamada para criar o índice `products` com analisador `edge_ngram` para autocompletar:
```bash
curl -X PUT http://localhost:9200/products \
  -H "Content-Type: application/json" \
  -d @scripts/elastic_mappings/products.json
```

### Passo 3: Registrar o Conector Debezium
Registre o conector CDC no Kafka Connect:
```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-cdc-connector",
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

### Passo 4: Executar a API e o Indexer Worker
Em dois terminais separados:
```bash
# Terminal 1 - Worker Indexer
go run cmd/indexer/main.go

# Terminal 2 - REST API
go run cmd/api/main.go
```

---

## 🔍 4. Validação da Sincronização

### Cadastrar um Produto via API
```bash
curl -X POST http://localhost:8080/api/v1/products \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Notebook Gamer RTX 4080",
    "description": "Equipamento para alto desempenho em jogos e inteligência artificial",
    "price_cents": 1250000,
    "stock_quantity": 8,
    "category_id": "computadores",
    "brand": "TechMaster",
    "attributes": {
      "processador": "Core i9",
      "memoria": "32GB"
    }
  }'
```

### Buscar o Produto Imediatamente no OpenSearch
```bash
curl "http://localhost:8080/api/v1/search?q=Note&category_id=computadores"
```

A busca retornará o item recém-indexado em poucos milissegundos com autocompletar ativado.

---

## 📊 5. Observabilidade no Grafana & Pipeline de Logs

O Grafana vem provisionado automaticamente com os datasources **Prometheus** (métricas de infra/lag) e **OpenSearch** (logs estruturados via Data Streams).
Acesse `http://localhost:3000` (Usuário: `admin` / Senha: `admin`) e abra o dashboard **E-Commerce Observability: Logs, Metrics & CDC Telemetry**:

### 1. Painel de Tráfego e Performance (Visão Engenharia)
- **Requisições por Segundo (RPS):** Throughput em tempo real de requisições HTTP da API.
- **Heatmap de Latência das APIs:** Distribuição e mapa de calor do tempo de resposta das buscas no OpenSearch.
- **Indicador Gauge de Erros 5xx:** Alerta visual instantâneo para taxas elevadas de erro interno do servidor.

### 2. Painel de Auditoria e Negócio (Visão Produto/Segurança)
- **Últimas Falhas de Login Crítico:** Tabela alimentada pelo log estruturado JSON do Go filtrando `event: user_login AND status: failed`.
- **Funcionalidades Mais Acessadas:** Gráfico de barras indicando endpoints com maior tráfego.
- **Origem de Acessos por IP / Cliente:** Mapeamento de IPs para detecção de anomalias ou acessos externos.

### 3. Painel de Saúde da Infraestrutura e Cluster
- **Consumo de Memória e Goroutines:** Monitoramento de uso de memória residente (`process_resident_memory_bytes`) e threads leves do runtime Go.
- **Vazão de Indexação e Lag do CDC:** Relação entre itens inseridos no OpenSearch por segundo e o percentil p95 de atraso de sincronização (`cdc_sync_lag_seconds`).

---

## ⚡ 6. Por que Logs Vão Direto para o OpenSearch (Data Streams)?

Passar logs de aplicação pelo PostgreSQL para depois replicar ao OpenSearch via Debezium CDC cria um gargalo severo de I/O de disco no banco relacional, pois logs são dados imutáveis (*append-only*) e de altíssimo volume.

**Solução Adotada:**
1. O PostgreSQL armazena exclusivamente dados de negócio (entidades ACID e a tabela `outbox_events`).
2. A aplicação Go emite logs estruturados em JSON via `log/slog` (com campos indexáveis: `event`, `user_id`, `status_code`, `latency_ms`).
3. O **OpenTelemetry Collector** atua como buffer amortecedor e injeta os logs no OpenSearch utilizando **Data Streams** (`logs-cdc-api-default`).
4. O OpenSearch aplica **Index Lifecycle Management (ILM)** automaticamente para particionar os logs por data/tamanho sem intervenção manual.

---

## 🧪 6. Testes Automatizados

Para rodar todos os testes com detecção de concorrência (*race detector*):
```bash
go test -v -race ./...
```
