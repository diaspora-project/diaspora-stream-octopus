#!/usr/bin/env bash

HERE=$(dirname $0)

$HERE/pre-test.sh

echo "==> Generating configuration for backend"
echo '{"bootstrap.servers":"localhost:9092"}' > benchmark_config.json

echo "==> Creating topic"
python $HERE/create-topic-for-benchmark.py
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/post-test.sh $r
    exit 1
fi

echo "==> Running producer benchmark"
diaspora-producer-benchmark -d octopus \
                            -c benchmark_config.json \
                            -t my_topic \
                            -n 100 \
                            -m 16 \
                            -s 128 \
                            -b 8 \
                            -f 10 \
                            -p 1
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/post-test.sh $r
    exit 1
fi

echo "==> Running consumer benchmark"
diaspora-consumer-benchmark -d octopus \
                            -c benchmark_config.json \
                            -t my_topic \
                            -n 100 \
                            -s 0.5 \
                            -i 0.8 \
                            -p 0
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/post-test.sh $r
    exit 1
fi

$HERE/post-test.sh 0
