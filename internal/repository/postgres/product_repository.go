package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
	"github.com/jules/cdc-postgres-opensearch/internal/core/ports"
)

type productRepository struct {
	pool *pgxpool.Pool
}

// NewProductRepository creates a new PostgreSQL ProductRepository with pgx connection pool.
func NewProductRepository(pool *pgxpool.Pool) ports.ProductRepository {
	return &productRepository{pool: pool}
}

// NewPGXPool creates and verifies a pgx connection pool.
func NewPGXPool(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}

	cfg.MaxConns = 25
	cfg.MinConns = 5
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return pool, nil
}

// CreateProductWithOutbox inserts a product and an outbox event in the same ACID transaction.
func (r *productRepository) CreateProductWithOutbox(ctx context.Context, product *domain.Product, event *domain.OutboxEvent) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	attrBytes, err := json.Marshal(product.Attributes)
	if err != nil {
		return fmt.Errorf("failed to marshal attributes: %w", err)
	}

	insertProductQuery := `
		INSERT INTO products (
			id, name, description, price_cents, stock_quantity,
			category_id, brand, attributes, is_active, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
	`
	_, err = tx.Exec(ctx, insertProductQuery,
		product.ID,
		product.Name,
		product.Description,
		product.PriceCents,
		product.StockQuantity,
		product.CategoryID,
		product.Brand,
		string(attrBytes),
		product.IsActive,
		product.CreatedAt,
		product.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert product: %w", err)
	}

	insertOutboxQuery := `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, payload, created_at
		) VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = tx.Exec(ctx, insertOutboxQuery,
		event.ID,
		event.AggregateType,
		event.AggregateID,
		event.EventType,
		event.Payload,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetProductByID retrieves a product by its ID.
func (r *productRepository) GetProductByID(ctx context.Context, id string) (*domain.Product, error) {
	query := `
		SELECT id, name, description, price_cents, stock_quantity,
		       category_id, brand, attributes, is_active, created_at, updated_at
		FROM products
		WHERE id = $1
	`
	row := r.pool.QueryRow(ctx, query, id)

	var p domain.Product
	var attrStr string
	err := row.Scan(
		&p.ID,
		&p.Name,
		&p.Description,
		&p.PriceCents,
		&p.StockQuantity,
		&p.CategoryID,
		&p.Brand,
		&attrStr,
		&p.IsActive,
		&p.CreatedAt,
		&p.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrProductNotFound
		}
		return nil, fmt.Errorf("failed to query product: %w", err)
	}

	if attrStr != "" {
		_ = json.Unmarshal([]byte(attrStr), &p.Attributes)
	}

	return &p, nil
}

// DeleteProductWithOutbox marks or removes a product and records an outbox deletion event.
func (r *productRepository) DeleteProductWithOutbox(ctx context.Context, id string, event *domain.OutboxEvent) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	deleteProductQuery := `DELETE FROM products WHERE id = $1`
	tag, err := tx.Exec(ctx, deleteProductQuery, id)
	if err != nil {
		return fmt.Errorf("failed to delete product: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrProductNotFound
	}

	insertOutboxQuery := `
		INSERT INTO outbox_events (
			id, aggregate_type, aggregate_id, event_type, payload, created_at
		) VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err = tx.Exec(ctx, insertOutboxQuery,
		event.ID,
		event.AggregateType,
		event.AggregateID,
		event.EventType,
		event.Payload,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to insert outbox delete event: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
