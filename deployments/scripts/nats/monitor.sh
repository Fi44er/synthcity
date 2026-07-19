#!/bin/bash

ENV_FILE="./deployments/docker-compose/.env"

BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== SynthCity NATS Monitor ===${NC}"

if [ -f "$ENV_FILE" ]; then
    # Достаем NATS_PORT из .env (удаляем возможные \r из windows-формата)
    NATS_PORT=$(grep NATS_PORT "$ENV_FILE" | cut -d '=' -f2 | tr -d '\r')
    echo "Using config from $ENV_FILE (Port: $NATS_PORT)"
else
    NATS_PORT=4222
    echo "Warning: .env not found, using default port 4222"
fi

if ! command -v nats &> /dev/null
then
    echo "Error: 'nats' CLI tool not found."
    echo "Install it: brew install nats (macOS) or go install github.com/nats-io/natscli/nats@latest"
    exit 1
fi

echo -e "${BLUE}Listening on city.v1.> ... (Press Ctrl+C to stop)${NC}"
echo "------------------------------------------------"

nats sub "city.updates" -s "localhost:$NATS_PORT"
