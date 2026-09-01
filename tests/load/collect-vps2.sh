#!/usr/bin/env bash
set -u
out="${1:-/tmp/fable-load-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$out"
date -Is > "$out/time.txt"
{ uptime; free -m; vmstat 1 5; } > "$out/system.txt" 2>&1
{ ss -s; ss -lntp; ulimit -n; cat /proc/sys/net/core/somaxconn; cat /proc/sys/net/ipv4/ip_local_port_range; } > "$out/network.txt" 2>&1
kubectl -n fable get pods -o wide > "$out/pods.txt" 2>&1
kubectl -n fable top pods > "$out/pod-top.txt" 2>&1 || true
kubectl -n fable get events --sort-by=.lastTimestamp > "$out/events.txt" 2>&1 || true
nginx -T > "$out/nginx-config.txt" 2>&1 || true
tail -n 500 /var/log/nginx/error.log > "$out/nginx-error.log" 2>&1 || true
docker stats --no-stream > "$out/docker-stats.txt" 2>&1 || true
tar -czf "$out.tar.gz" -C "$(dirname "$out")" "$(basename "$out")"
echo "$out.tar.gz"
