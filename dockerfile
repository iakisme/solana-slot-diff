FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git gcc musl-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o solana-stream-compare .

FROM alpine:latest

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/solana-stream-compare .

RUN mkdir -p /app/output

VOLUME /app/output

ENV METRICS_PORT=2112

EXPOSE 2112

USER 1000:1000

ENTRYPOINT ["/app/solana-stream-compare"]
