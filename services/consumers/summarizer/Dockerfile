FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/summarizer-consumer .

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /bin/summarizer-consumer /app/summarizer-consumer

EXPOSE 8087

ENV PORT=8087

USER nobody:nobody

ENTRYPOINT ["/app/summarizer-consumer"]
