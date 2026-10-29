#!/bin/sh
# Pinned source generation. --check compares without modifying the checkout.
set -eu
case "${1:-}" in ''|--check) ;; *) printf '%s\n' 'usage: sh scripts/generate-proto.sh [--check]' >&2; exit 2 ;; esac
proto_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
proto_tmp=$(mktemp -d "${TMPDIR:-/tmp}/fable-proto.XXXXXX")
trap 'rm -rf "$proto_tmp"' EXIT HUP INT TERM
if [ "$(protoc --version)" != 'libprotoc 33.4' ]; then
  printf '%s\n' 'generation requires protoc 33.4' >&2
  exit 1
fi
mkdir -p "$proto_tmp/tools" "$proto_tmp/out"
GOBIN="$proto_tmp/tools" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
GOBIN="$proto_tmp/tools" go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
cd "$proto_root"
protoc -I api/proto \
  --plugin="protoc-gen-go=$proto_tmp/tools/protoc-gen-go" \
  --plugin="protoc-gen-go-grpc=$proto_tmp/tools/protoc-gen-go-grpc" \
  --go_out="$proto_tmp/out" --go_opt=module=mychat \
  --go-grpc_out="$proto_tmp/out" --go-grpc_opt=module=mychat \
  api/proto/fable/v1/common.proto api/proto/fable/v1/logic.proto api/proto/fable/v1/comet.proto
if [ "${1:-}" = '--check' ]; then
  diff -ru api/gen "$proto_tmp/out/api/gen"
else
  mkdir -p api/gen
  cp -R "$proto_tmp/out/api/gen/." api/gen/
fi
