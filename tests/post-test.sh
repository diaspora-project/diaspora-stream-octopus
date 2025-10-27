#!/usr/bin/env bash

echo "================================================"
echo "After test"
echo "================================================"

RET=$1 # This is the return code of the test
echo "==> Test return $RET"

KAFKA_CLUSTER_ID=$(<cluster_id.txt)
rm cluster_id.txt

#kafka-get-offsets.sh --bootstrap-server localhost:9092

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
