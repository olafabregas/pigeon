<#
Runs a k6 script from a separate grafana/k6 container (outside the app containers).
Usage: .\loadtest\run.ps1 [smoke|spike]   (default: smoke)
Env:   BASE_URL (default http://host.docker.internal:8081), PEAK_RPS, SLOW_MS
#>
param([string]$Script = 'smoke')

$ErrorActionPreference = 'Stop'
$dir = $PSScriptRoot
if (-not (Test-Path (Join-Path $dir "$Script.js"))) {
    Write-Error "Unknown script '$Script'. Use smoke or spike."
}

$baseUrl = $env:BASE_URL
if (-not $baseUrl) {
    $port = docker compose port core 8081
    if (-not $port) {
        throw 'BASE_URL is required unless the Compose core service is running.'
    }
    $baseUrl = "http://host.docker.internal:$($port.Split(':')[-1])"
}
$envArgs = @('-e', "BASE_URL=$baseUrl")
if ($env:PEAK_RPS) { $envArgs += @('-e', "PEAK_RPS=$($env:PEAK_RPS)") }
if ($env:SLOW_MS)  { $envArgs += @('-e', "SLOW_MS=$($env:SLOW_MS)") }

docker run --rm -i `
    --add-host=host.docker.internal:host-gateway `
    -v "${dir}:/scripts:ro" `
    @envArgs `
    grafana/k6 run "/scripts/$Script.js"
exit $LASTEXITCODE
