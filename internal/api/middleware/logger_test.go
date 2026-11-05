package middleware_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jules/cdc-postgres-opensearch/internal/api/middleware"
)

func TestRequestLogger_StructuredOutput(t *testing.T) {
	buf := new(bytes.Buffer)
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"created"}`))
	})

	wrapped := middleware.RequestLogger(logger)(nextHandler)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/test?filter=true", nil)
	req.Header.Set("User-Agent", "TestClient/1.0")
	req.Header.Set("X-Forwarded-For", "192.168.1.100")
	rr := httptest.NewRecorder()

	wrapped.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rr.Code)
	}

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to parse JSON log output: %v, raw: %s", err, buf.String())
	}

	if logEntry["event"] != "http_request" {
		t.Errorf("expected event 'http_request', got %v", logEntry["event"])
	}
	if logEntry["http_method"] != "POST" {
		t.Errorf("expected http_method 'POST', got %v", logEntry["http_method"])
	}
	if logEntry["path"] != "/api/v1/test" {
		t.Errorf("expected path '/api/v1/test', got %v", logEntry["path"])
	}
	if logEntry["client_ip"] != "192.168.1.100" {
		t.Errorf("expected client_ip '192.168.1.100', got %v", logEntry["client_ip"])
	}
	if logEntry["status_code"] != float64(201) {
		t.Errorf("expected status_code 201, got %v", logEntry["status_code"])
	}
}

func TestRequestLogger_ErrorLevels(t *testing.T) {
	buf := new(bytes.Buffer)
	logger := slog.New(slog.NewJSONHandler(buf, nil))

	errorHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "server boom", http.StatusInternalServerError)
	})

	wrapped := middleware.RequestLogger(logger)(errorHandler)

	req := httptest.NewRequest(http.MethodGet, "/boom", nil)
	rr := httptest.NewRecorder()
	wrapped.ServeHTTP(rr, req)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "ERROR" {
		t.Errorf("expected log level ERROR for 500 status, got %v", logEntry["level"])
	}
}
