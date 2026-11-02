#!/bin/sh
# Tests create their own fable_e2e_* DB, route namespace and suffixed Topics.
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
: "${FABLE_LOGIC_TEST_MYSQL_DSN:?select an isolated MySQL test administrator}"
: "${FABLE_LOGIC_TEST_REDIS_ADDR:?select an isolated Redis test service}"
: "${FABLE_LOGIC_TEST_KAFKA_BROKER:?select an isolated Kafka test service}"
exec go test -p 1 -tags=e2e ./internal/e2e -count=1 -v "$@"
