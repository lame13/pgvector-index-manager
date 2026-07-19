FROM golang:1.25.12-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /pgvector-index-manager .

FROM alpine:3.19
RUN apk add --no-cache ca-certificates
COPY --from=builder /pgvector-index-manager /usr/local/bin/pgvector-index-manager
ENTRYPOINT ["pgvector-index-manager"]
