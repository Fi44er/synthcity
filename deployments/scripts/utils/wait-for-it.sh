#!/bin/sh
# wait-for-it.sh

# Разделяем первый аргумент (hi-service:50051) на хост и порт
HOST_PORT=$1
shift
CMD="$@"

HOST=$(echo $HOST_PORT | cut -d: -f1)
PORT=$(echo $HOST_PORT | cut -d: -f2)

if [ -z "$HOST" ] || [ -z "$PORT" ]; then
    echo "Usage: ./wait-for-it.sh host:port command"
    exit 1
fi

until nc -z "$HOST" "$PORT"; do
  >&2 echo "Service $HOST:$PORT is unavailable - sleeping..."
  sleep 1
done

>&2 echo "Service $HOST:$PORT is up - executing command: $CMD"
exec $CMD
