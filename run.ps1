$ErrorActionPreference = "Stop"
$secretFile = Join-Path $PSScriptRoot ".studio-secrets.ps1"
if (Test-Path $secretFile) { . $secretFile }
$gflowRoot = Join-Path $PSScriptRoot "gflow-cli"
$env:PYTHONPATH = Join-Path $gflowRoot "src"
$env:GFLOW_CLI_SRC = $env:PYTHONPATH
$env:GFLOW_CLI_LOG_FORMAT = "json"
$env:GFLOW_CLI_FLOW_HOST = if ($env:GFLOW_CLI_FLOW_HOST) { $env:GFLOW_CLI_FLOW_HOST } else { "flow.google.com" }
$env:GFLOW_STUDIO_MCP_UPSTREAM_URL = if ($env:GFLOW_STUDIO_MCP_UPSTREAM_URL) { $env:GFLOW_STUDIO_MCP_UPSTREAM_URL } else { "http://127.0.0.1:8000/mcp" }
$env:GFLOW_STUDIO_ROLE = "api"
Push-Location $PSScriptRoot
if (-not (Test-Path (Join-Path $PSScriptRoot "node_modules"))) {
    npm install
}
npm run build
$python = Join-Path $gflowRoot ".venv\Scripts\python.exe"
$mcp = Start-Process -FilePath $python -ArgumentList "-m gflow_cli.cli serve --host 127.0.0.1 --port 8000 --transport http" -WorkingDirectory (Join-Path $gflowRoot "src") -WindowStyle Hidden -PassThru
$worker = Start-Process -FilePath $python -ArgumentList "worker.py" -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru
try {
    & $python -m uvicorn app:app --host 127.0.0.1 --port 8001
}
finally {
    if ($mcp -and -not $mcp.HasExited) { Stop-Process -Id $mcp.Id -Force -ErrorAction SilentlyContinue }
    if ($worker -and -not $worker.HasExited) { Stop-Process -Id $worker.Id -Force -ErrorAction SilentlyContinue }
    Pop-Location
}
