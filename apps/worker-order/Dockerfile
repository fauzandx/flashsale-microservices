# Stage 1: Builder
FROM golang:alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -p 2 -ldflags="-w -s" -o worker-order main.go

# Stage 2: Minimal & Secure Runtime
FROM alpine:latest

# Upgrade paket OS bawaan untuk menambal vulnerability
RUN apk --no-cache add ca-certificates tzdata && apk --no-cache upgrade

WORKDIR /app

COPY --from=builder /app/worker-order .

CMD ["./worker-order"]