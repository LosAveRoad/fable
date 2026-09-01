param(
  [ValidateSet('healthz','business','websocket')][string]$Scenario = 'healthz',
  [int]$Rps = 100,
  [string]$Duration = '30s',
  [string]$TargetHost = 'api.fableim.lol',
  [string]$TargetIp = '38.147.170.246',
  [int]$WsVus = 100,
  [string]$WsToken = ''
)
$ErrorActionPreference = 'Stop'
$script = "tests/load/$Scenario.js"
$args = @('run', "--add-host", "$TargetHost`:$TargetIp", '-e', "BASE_URL=https://$TargetHost", '-e', "TARGET_RPS=$Rps", '-e', "DURATION=$Duration")
if ($Scenario -eq 'websocket') { $args += @('-e', "WS_BASE=wss://$TargetHost", '-e', "VUS=$WsVus", '-e', "WS_TOKEN=$WsToken") }
$args += @('-v', "${PWD}:/work", '-w', '/work', 'grafana/k6', $script)
docker run --rm @args
