$ErrorActionPreference = "Stop"

Set-Location -LiteralPath $PSScriptRoot
python .\central_collector.py daemon --push-command push-all

