#!/usr/bin/env bash

HERE=$(dirname "$0")

echo "================================================"
echo "Before Test (mocktopus)"
echo "================================================"

rm -rf /tmp/kraft-combined-logs

echo "==> Generating cluster ID"
KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"
echo $KAFKA_CLUSTER_ID > cluster_id.txt

echo "==> Copying configuration file"
cp `spack location -i kafka`/config/server.properties .

echo "==> Formatting log directory"
kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c server.properties

echo "==> Starting kafka server"
kafka-server-start.sh server.properties 1> kafka.$KAFKA_CLUSTER_ID.out 2> kafka.$KAFKA_CLUSTER_ID.err &
KAFKA_PID=$!

sleep 5

echo "==> Starting mocktopus"
MOCKTOPUS_LOG=$(mktemp /tmp/mocktopus.XXXXXX.log)
echo "$MOCKTOPUS_LOG" > mocktopus_log_path.txt
MOCKTOPUS_PORT=${MOCKTOPUS_PORT:-18080}
$MOCKTOPUS_BIN --port $MOCKTOPUS_PORT --kafka localhost:9092 > "$MOCKTOPUS_LOG" 2>&1 &
MOCKTOPUS_PID=$!
echo $MOCKTOPUS_PID > mocktopus_pid.txt

# Wait for mocktopus to be ready
READY=""
for i in $(seq 1 20); do
    if grep -q 'listening on port' "$MOCKTOPUS_LOG" 2>/dev/null; then
        READY=1
        break
    fi
    sleep 0.5
done

if [ -z "$READY" ]; then
    echo "FATAL: mocktopus did not start"
    cat "$MOCKTOPUS_LOG"
    exit 1
fi

echo "==> mocktopus running on port $MOCKTOPUS_PORT (PID $MOCKTOPUS_PID)"
echo "==> Service is ready"
echo "================================================"
