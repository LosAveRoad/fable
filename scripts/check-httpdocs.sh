#!/bin/sh
# Run from any directory. Generation is read-only with respect to the checkout.
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT HUP INT TERM
go run ./cmd/httpdocs -output "$tmp_dir/http.json"
diff -u api/openapi/http.json "$tmp_dir/http.json"
go test ./internal/httpdocs ./internal/https_server
