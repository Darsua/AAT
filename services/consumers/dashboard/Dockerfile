FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/dashboard-consumer .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/dashboard-consumer /app/dashboard-consumer

EXPOSE 8085

ENV PORT=8085

USER nobody:nobody

ENTRYPOINT ["/app/dashboard-consumer"]
