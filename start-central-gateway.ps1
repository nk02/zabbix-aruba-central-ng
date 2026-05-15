$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $PSScriptRoot
python .\central_gateway.py run
