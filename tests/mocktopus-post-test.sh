#!/usr/bin/env bash

echo "================================================"
echo "After test (mocktopus)"
echo "================================================"

RET=$1 # This is the return code of the test
echo "==> Test return $RET"

# Stop mocktopus
if [ -f mocktopus_pid.txt ]; then
    MOCKTOPUS_PID=$(<mocktopus_pid.txt)
    echo "==> Stopping mocktopus (PID $MOCKTOPUS_PID)"
    kill $MOCKTOPUS_PID 2>/dev/null
    wait $MOCKTOPUS_PID 2>/dev/null
    rm mocktopus_pid.txt
fi

if [ -f mocktopus_log_path.txt ]; then
    MOCKTOPUS_LOG=$(<mocktopus_log_path.txt)
    if [ "$RET" -ne "0" ] && [ -f "$MOCKTOPUS_LOG" ]; then
        echo "==> mocktopus log:"
        cat "$MOCKTOPUS_LOG"
    fi
    rm -f "$MOCKTOPUS_LOG"
    rm mocktopus_log_path.txt
fi

if [ -f mocktopus_port.txt ]; then
    rm mocktopus_port.txt
fi

# Stop Kafka
KAFKA_CLUSTER_ID=$(<cluster_id.txt)
rm cluster_id.txt

kafka-get-offsets.sh --bootstrap-server localhost:9092

echo "==> Stopping kafka server"
kafka-server-stop.sh server.properties
wait $KAFKA_PID

echo "==> Erasing data"
rm -rf /tmp/kraft-combined-logs
if [ "$RET" -eq "0" ]; then
    rm kafka.$KAFKA_CLUSTER_ID.out
    rm kafka.$KAFKA_CLUSTER_ID.err
fi

echo "==> Script completed"
exit $RET

echo "================================================"
