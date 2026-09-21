# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod ./
# RUN go mod download (if dependencies exist)

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/mock-bmkg .

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/mock-bmkg /app/mock-bmkg

EXPOSE 8081

ENV PORT=8081

USER nobody:nobody

ENTRYPOINT ["/app/mock-bmkg"]
