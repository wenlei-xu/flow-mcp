$ErrorActionPreference = "Stop"
$gflowRoot = Join-Path $PSScriptRoot "..\gflow-cli"
$env:PYTHONPATH = Join-Path $gflowRoot "src"
$env:GFLOW_CLI_LOG_FORMAT = "json"
$env:GFLOW_STUDIO_MCP_UPSTREAM_URL = if ($env:GFLOW_STUDIO_MCP_UPSTREAM_URL) { $env:GFLOW_STUDIO_MCP_UPSTREAM_URL } else { "http://127.0.0.1:18080/mcp" }
$env:GFLOW_STUDIO_ROLE = "api"
Push-Location $PSScriptRoot
if (-not (Test-Path (Join-Path $PSScriptRoot "node_modules"))) {
    npm install
}
npm run build
$python = Join-Path $gflowRoot ".venv\Scripts\python.exe"
$mcp = Start-Process -FilePath $python -ArgumentList "-m gflow_cli.cli serve --host 127.0.0.1 --port 18080 --transport http" -WorkingDirectory (Join-Path $gflowRoot "src") -WindowStyle Hidden -PassThru
$worker = Start-Process -FilePath $python -ArgumentList "worker.py" -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru
try {
    & $python -m uvicorn app:app --host 127.0.0.1 --port 8090
}
finally {
    if ($mcp -and -not $mcp.HasExited) { Stop-Process -Id $mcp.Id -Force -ErrorAction SilentlyContinue }
    if ($worker -and -not $worker.HasExited) { Stop-Process -Id $worker.Id -Force -ErrorAction SilentlyContinue }
    Pop-Location
}
