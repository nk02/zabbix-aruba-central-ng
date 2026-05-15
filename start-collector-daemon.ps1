$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $PSScriptRoot
python .\central_collector.py run
