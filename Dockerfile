# Multi-stage Dockerfile for CDC API and Indexer Worker

FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/indexer ./cmd/indexer

# API Target
FROM alpine:3.20 AS api
RUN apk add --no-cache ca-certificates tzdata curl
WORKDIR /app
COPY --from=builder /bin/api /app/api
EXPOSE 8080
ENTRYPOINT ["/app/api"]

# Indexer Target
FROM alpine:3.20 AS indexer
RUN apk add --no-cache ca-certificates tzdata curl
WORKDIR /app
COPY --from=builder /bin/indexer /app/indexer
EXPOSE 9091
ENTRYPOINT ["/app/indexer"]
