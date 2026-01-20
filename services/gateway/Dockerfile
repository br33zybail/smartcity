# Build stage
FROM golang:1.23-alpine AS builder

# Install git if needed for modules (usually not, but safe)
RUN apk add --no-cache git

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Build static binary: disable CGO, linux target, strip debug info
RUN CGO_ENABLED=0 GOOS=linux go build -a -ldflags '-w -s' -o ingester main.go

# Runtime stage
FROM alpine:latest

# Add ca-certificates for HTTPS (resty needs it for Nashville API)
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/ingester .
# No .env COPY needed

# Optional: tini for better signal handling (good habit)
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["./ingester"]