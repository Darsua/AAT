# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod ./

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/mock-pvmbg .

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/mock-pvmbg /app/mock-pvmbg

EXPOSE 8082

ENV PORT=8082

USER nobody:nobody

ENTRYPOINT ["/app/mock-pvmbg"]
