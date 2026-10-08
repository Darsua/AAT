FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/notifier-consumer .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/notifier-consumer /app/notifier-consumer

USER nobody:nobody

ENTRYPOINT ["/app/notifier-consumer"]
