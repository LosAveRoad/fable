#!/bin/sh
# Compile specifications only. Does not generate Go stubs or contact services.
set -eu

contract_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
contract_tmp=$(mktemp -d "${TMPDIR:-/tmp}/fable-contracts.XXXXXX")
trap 'rm -rf "$contract_tmp"' EXIT HUP INT TERM

cd "$contract_root"
protoc -I api/proto \
  --include_imports \
  --descriptor_set_out="$contract_tmp/fable-v1.pb" \
  api/proto/fable/v1/common.proto \
  api/proto/fable/v1/logic.proto \
  api/proto/fable/v1/comet.proto
go test ./internal/dto/im
printf '%s\n' 'Protobuf specifications and Go DTO declarations compile.'
