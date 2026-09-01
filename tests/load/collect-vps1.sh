#!/usr/bin/env bash
set -u
out="${1:-/tmp/fable-components-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$out"
date -Is > "$out/time.txt"
{ uptime; free -m; vmstat 1 5; } > "$out/system.txt" 2>&1
mysqladmin extended-status 2>/dev/null > "$out/mysql-status.txt" || true
mysqladmin processlist 2>/dev/null > "$out/mysql-processlist.txt" || true
redis-cli INFO > "$out/redis-info.txt" 2>&1 || true
redis-cli INFO commandstats > "$out/redis-commandstats.txt" 2>&1 || true
tar -czf "$out.tar.gz" -C "$(dirname "$out")" "$(basename "$out")"
echo "$out.tar.gz"
