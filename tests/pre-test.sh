#!/usr/bin/env bash

HERE=$(dirname $0)

echo "================================================"
echo "Before Test"
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

echo "==> Service is ready"
echo "================================================"
