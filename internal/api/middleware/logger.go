package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
	bytesWritten int64
}

func (rw *responseWriterInterceptor) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriterInterceptor) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytesWritten += int64(n)
	return n, err
}

// RequestLogger is an HTTP middleware that emits structured JSON access logs.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &responseWriterInterceptor{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next.ServeHTTP(wrapped, r)

			duration := time.Since(start)
			clientIP := r.RemoteAddr
			if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
				clientIP = forwarded
			}

			level := slog.LevelInfo
			statusStr := "success"
			if wrapped.statusCode >= 500 {
				level = slog.LevelError
				statusStr = "error"
			} else if wrapped.statusCode >= 400 {
				level = slog.LevelWarn
				statusStr = "client_error"
			}

			logger.Log(r.Context(), level, "http_request",
				slog.String("event", "http_request"),
				slog.String("status", statusStr),
				slog.String("http_method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("raw_query", r.URL.RawQuery),
				slog.Int("status_code", wrapped.statusCode),
				slog.Int64("latency_ms", duration.Milliseconds()),
				slog.Float64("duration_sec", duration.Seconds()),
				slog.Int64("bytes_written", wrapped.bytesWritten),
				slog.String("client_ip", clientIP),
				slog.String("user_agent", r.UserAgent()),
			)
		})
	}
}
