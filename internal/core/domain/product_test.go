package domain_test

import (
	"testing"

	"github.com/jules/cdc-postgres-opensearch/internal/core/domain"
)

func TestCreateProductInput_Validate(t *testing.T) {
	tests := []struct {
		name        string
		input       domain.CreateProductInput
		expectedErr error
	}{
		{
			name: "Valid input",
			input: domain.CreateProductInput{
				Name:          "Smartphone 5G",
				PriceCents:    299900,
				StockQuantity: 15,
				CategoryID:    "electronics",
			},
			expectedErr: nil,
		},
		{
			name: "Empty product name",
			input: domain.CreateProductInput{
				Name:       "   ",
				PriceCents: 1000,
			},
			expectedErr: domain.ErrInvalidProductName,
		},
		{
			name: "Negative price",
			input: domain.CreateProductInput{
				Name:       "Valid Name",
				PriceCents: -50,
			},
			expectedErr: domain.ErrInvalidProductPrice,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.input.Validate()
			if err != tt.expectedErr {
				t.Fatalf("expected error %v, got %v", tt.expectedErr, err)
			}
		})
	}
}
