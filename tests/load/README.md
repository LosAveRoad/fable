# Fable load tests

Run from a separate machine, not VPS1/VPS2. Install k6 or use `grafana/k6`.

```powershell
$env:BASE_URL = "https://api.fableim.lol"
k6 run tests/load/healthz.js
k6 run tests/load/api.js
```

The default profile ramps 10 -> 50 -> 100 requests/sec. Override `BASE_URL`,
`TARGET_RPS`, and `DURATION`. Use dedicated test accounts for authenticated
scenarios. Set `WS_TOKEN` before running the WebSocket test.
