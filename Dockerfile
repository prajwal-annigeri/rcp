# Build executable
FROM golang:1.23.6-alpine AS builder

WORKDIR /app
COPY . .
RUN GOOS=linux GOARCH=amd64 go build -o rcp main.go

# Lightweight image with the executable
FROM alpine
COPY --from=builder /app/rcp /app/rcp
WORKDIR /app/

ENTRYPOINT ["./rcp"]
