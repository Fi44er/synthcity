# Stage 1: Build
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE_PATH
RUN go build -o /app/binary ./${SERVICE_PATH}

FROM alpine:latest

RUN apk add --no-cache netcat-openbsd ca-certificates

WORKDIR /app
COPY --from=builder /app/binary .
COPY --from=builder /app/deployments/scripts/utils/wait-for-it.sh ./wait-for-it.sh
RUN chmod +x wait-for-it.sh

CMD ["./binary"]
