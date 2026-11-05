package handlers

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

type LoginRequest struct {
	UserID   string `json:"user_id"`
	Password string `json:"password"`
}

type AuthHandler struct {
	logger *slog.Logger
}

func NewAuthHandler(logger *slog.Logger) *AuthHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &AuthHandler{logger: logger}
}

func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	var req LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.WarnContext(r.Context(), "login_attempt",
			slog.String("event", "user_login"),
			slog.String("status", "failed"),
			slog.String("reason", "malformed_request"),
			slog.Int64("latency_ms", time.Since(start).Milliseconds()),
		)
		http.Error(w, "Malformed request", http.StatusBadRequest)
		return
	}

	// Simulation for security and business audit logs
	if req.Password != "correct_password" {
		reason := "invalid_password"
		if req.UserID == "" {
			reason = "missing_user_id"
		}
		h.logger.WarnContext(r.Context(), "login_attempt",
			slog.String("event", "user_login"),
			slog.String("user_id", req.UserID),
			slog.String("status", "failed"),
			slog.String("reason", reason),
			slog.String("client_ip", r.RemoteAddr),
			slog.Int64("latency_ms", time.Since(start).Milliseconds()),
		)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"status": "failed",
			"reason": reason,
		})
		return
	}

	h.logger.InfoContext(r.Context(), "login_attempt",
		slog.String("event", "user_login"),
		slog.String("user_id", req.UserID),
		slog.String("status", "success"),
		slog.String("reason", "authenticated"),
		slog.String("client_ip", r.RemoteAddr),
		slog.Int64("latency_ms", time.Since(start).Milliseconds()),
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"user_id": req.UserID,
		"token":   "mock-jwt-token-xyz",
	})
}
