#!/usr/bin/env bash

HERE=$(dirname "$0")
MOCKTOPUS="${HERE}/../build/tests/mocktopus"
if [ -n "$MOCKTOPUS_BIN" ]; then
    MOCKTOPUS="$MOCKTOPUS_BIN"
fi

PASS=0
FAIL=0
TESTS=0

pass() {
    PASS=$((PASS + 1))
    TESTS=$((TESTS + 1))
    echo "  PASS: $1"
}

fail() {
    FAIL=$((FAIL + 1))
    TESTS=$((TESTS + 1))
    echo "  FAIL: $1"
    echo "        $2"
}

assert_status() {
    local test_name="$1"
    local body="$2"
    local expected_status="$3"
    local actual_status
    actual_status=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
    if [ "$actual_status" = "$expected_status" ]; then
        pass "$test_name"
    else
        fail "$test_name" "expected status='$expected_status', got status='$actual_status', body=$body"
    fi
}

assert_json_field() {
    local test_name="$1"
    local body="$2"
    local field="$3"
    local expected="$4"
    local actual
    actual=$(echo "$body" | python3 -c "import sys,json; print(json.load(sys.stdin).get('$field',''))" 2>/dev/null)
    if [ "$actual" = "$expected" ]; then
        pass "$test_name"
    else
        fail "$test_name" "expected $field='$expected', got $field='$actual'"
    fi
}

assert_json_contains() {
    local test_name="$1"
    local body="$2"
    local field="$3"
    local value="$4"
    local found
    found=$(echo "$body" | python3 -c "
import sys, json
data = json.load(sys.stdin)
val = data.get('$field', [])
if isinstance(val, dict):
    print('$value' in val)
elif isinstance(val, list):
    print('$value' in val)
else:
    print(False)
" 2>/dev/null)
    if [ "$found" = "True" ]; then
        pass "$test_name"
    else
        fail "$test_name" "'$value' not found in '$field', body=$body"
    fi
}

assert_json_not_contains() {
    local test_name="$1"
    local body="$2"
    local field="$3"
    local value="$4"
    local found
    found=$(echo "$body" | python3 -c "
import sys, json
data = json.load(sys.stdin)
val = data.get('$field', [])
if isinstance(val, dict):
    print('$value' in val)
elif isinstance(val, list):
    print('$value' in val)
else:
    print(False)
" 2>/dev/null)
    if [ "$found" = "False" ]; then
        pass "$test_name"
    else
        fail "$test_name" "'$value' should not be in '$field', body=$body"
    fi
}

assert_http_code() {
    local test_name="$1"
    local actual_code="$2"
    local expected_code="$3"
    if [ "$actual_code" = "$expected_code" ]; then
        pass "$test_name"
    else
        fail "$test_name" "expected HTTP $expected_code, got HTTP $actual_code"
    fi
}

# ============================================================
# Start Kafka
# ============================================================

$HERE/pre-test.sh
if [ $? -ne 0 ]; then
    echo "FATAL: Failed to start Kafka"
    exit 1
fi

# ============================================================
# Start mocktopus
# ============================================================

MOCKTOPUS_LOG=$(mktemp /tmp/mocktopus.XXXXXX.log)

echo "==> Starting mocktopus"
$MOCKTOPUS --port 0 --kafka localhost:9092 > "$MOCKTOPUS_LOG" 2>&1 &
MOCKTOPUS_PID=$!

# Wait for mocktopus to print its port
MOCKTOPUS_PORT=""
for i in $(seq 1 20); do
    MOCKTOPUS_PORT=$(grep -oP 'listening on port \K[0-9]+' "$MOCKTOPUS_LOG" 2>/dev/null || true)
    if [ -n "$MOCKTOPUS_PORT" ]; then
        break
    fi
    sleep 0.5
done

if [ -z "$MOCKTOPUS_PORT" ]; then
    echo "FATAL: mocktopus did not start"
    cat "$MOCKTOPUS_LOG"
    rm -f "$MOCKTOPUS_LOG"
    kill $MOCKTOPUS_PID 2>/dev/null
    wait $MOCKTOPUS_PID 2>/dev/null
    $HERE/post-test.sh 1
    exit 1
fi

BASE_URL="http://localhost:${MOCKTOPUS_PORT}"
echo "==> mocktopus running on port $MOCKTOPUS_PORT (PID $MOCKTOPUS_PID)"

# ============================================================
# Run tests
# ============================================================

NS="test-ns"
TOPIC1="alpha"
TOPIC2="beta"

echo ""
echo "================================================"
echo "Test: Empty namespace listing"
echo "================================================"

BODY=$(curl -s "$BASE_URL/api/v3/namespace")
assert_status "GET namespace returns success" "$BODY" "success"
assert_json_not_contains "namespace '$NS' absent initially" "$BODY" "namespaces" "$NS"

echo ""
echo "================================================"
echo "Test: Create first topic"
echo "================================================"

BODY=$(curl -s -X POST "$BASE_URL/api/v3/$NS/$TOPIC1")
assert_status "POST create $TOPIC1 returns success" "$BODY" "success"
assert_json_field "POST create $TOPIC1 has message" "$BODY" "message" "Topic $TOPIC1 created in $NS"

# Allow Kafka metadata to propagate
sleep 2

echo ""
echo "================================================"
echo "Test: Create second topic"
echo "================================================"

BODY=$(curl -s -X POST "$BASE_URL/api/v3/$NS/$TOPIC2")
assert_status "POST create $TOPIC2 returns success" "$BODY" "success"
assert_json_contains "POST create $TOPIC2 lists $TOPIC2" "$BODY" "topics" "$TOPIC2"

sleep 2

echo ""
echo "================================================"
echo "Test: List topics after creation"
echo "================================================"

BODY=$(curl -s "$BASE_URL/api/v3/namespace")
assert_status "GET namespace returns success" "$BODY" "success"
assert_json_contains "namespace '$NS' present" "$BODY" "namespaces" "$NS"

# Verify both topics are listed under the namespace
TOPICS_CHECK=$(echo "$BODY" | python3 -c "
import sys, json
data = json.load(sys.stdin)
ns_topics = data.get('namespaces', {}).get('$NS', [])
print('$TOPIC1' in ns_topics and '$TOPIC2' in ns_topics)
" 2>/dev/null)
if [ "$TOPICS_CHECK" = "True" ]; then
    pass "Both $TOPIC1 and $TOPIC2 listed in namespace"
else
    fail "Both topics listed in namespace" "body=$BODY"
fi

echo ""
echo "================================================"
echo "Test: Create duplicate topic (should fail)"
echo "================================================"

HTTP_CODE=$(curl -s -o /tmp/mocktopus_dup.json -w "%{http_code}" -X POST "$BASE_URL/api/v3/$NS/$TOPIC1")
assert_http_code "POST duplicate topic returns 500" "$HTTP_CODE" "500"

echo ""
echo "================================================"
echo "Test: Delete first topic"
echo "================================================"

BODY=$(curl -s -X DELETE "$BASE_URL/api/v3/$NS/$TOPIC1")
assert_status "DELETE $TOPIC1 returns success" "$BODY" "success"
assert_json_field "DELETE $TOPIC1 has message" "$BODY" "message" "Topic $TOPIC1 deleted from $NS"

sleep 2

echo ""
echo "================================================"
echo "Test: List topics after deletion"
echo "================================================"

BODY=$(curl -s "$BASE_URL/api/v3/namespace")
assert_status "GET namespace returns success" "$BODY" "success"

NS_TOPICS=$(echo "$BODY" | python3 -c "
import sys, json
data = json.load(sys.stdin)
ns_topics = data.get('namespaces', {}).get('$NS', [])
has_alpha = '$TOPIC1' in ns_topics
has_beta = '$TOPIC2' in ns_topics
print(f'{has_alpha},{has_beta}')
" 2>/dev/null)
if [ "$NS_TOPICS" = "False,True" ]; then
    pass "$TOPIC1 removed, $TOPIC2 still present"
else
    fail "$TOPIC1 removed, $TOPIC2 still present" "got $NS_TOPICS, body=$BODY"
fi

echo ""
echo "================================================"
echo "Test: Delete non-existent topic (should succeed)"
echo "================================================"

BODY=$(curl -s -X DELETE "$BASE_URL/api/v3/$NS/no-such-topic")
assert_status "DELETE non-existent topic returns success" "$BODY" "success"

echo ""
echo "================================================"
echo "Test: Recreate topic"
echo "================================================"

BODY=$(curl -s -X PUT "$BASE_URL/api/v3/$NS/$TOPIC2/recreate")
assert_status "PUT recreate $TOPIC2 returns success" "$BODY" "success"
assert_json_field "PUT recreate has message" "$BODY" "message" "Topic recreated"

sleep 2

BODY=$(curl -s "$BASE_URL/api/v3/namespace")
assert_json_contains "Recreated $TOPIC2 still listed" "$BODY" "namespaces" "$NS"

echo ""
echo "================================================"
echo "Test: Clean up — delete remaining topic"
echo "================================================"

BODY=$(curl -s -X DELETE "$BASE_URL/api/v3/$NS/$TOPIC2")
assert_status "DELETE $TOPIC2 returns success" "$BODY" "success"

sleep 2

echo ""
echo "================================================"
echo "Test: Namespace empty after full cleanup"
echo "================================================"

BODY=$(curl -s "$BASE_URL/api/v3/namespace")
assert_status "GET namespace returns success" "$BODY" "success"
assert_json_not_contains "namespace '$NS' absent after cleanup" "$BODY" "namespaces" "$NS"

# ============================================================
# Cleanup
# ============================================================

echo ""
echo "================================================"
echo "Stopping mocktopus"
echo "================================================"

kill $MOCKTOPUS_PID 2>/dev/null
wait $MOCKTOPUS_PID 2>/dev/null
rm -f "$MOCKTOPUS_LOG"

echo ""
echo "================================================"
echo "Results: $PASS/$TESTS passed, $FAIL failed"
echo "================================================"

if [ "$FAIL" -ne 0 ]; then
    RET=1
else
    RET=0
fi

$HERE/post-test.sh $RET
exit $RET
