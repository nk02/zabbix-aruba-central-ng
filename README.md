# HPE Aruba Central NG Gateway for Zabbix

The Python process is a gateway/sync service. Zabbix collects metrics through HTTP agent items pointed at the gateway.

- `sync-zabbix`: authenticates to GreenLake, discovers tenants/devices, imports the Zabbix template, creates/updates Zabbix hosts, and writes `gateway_state.json`.
- `gateway`: exposes a local HTTP API used by Zabbix HTTP agent items. It manages Aruba tokens, cache, retry, and global API rate limiting.

The communication is bidirectional:

- gateway to Zabbix API: template import and managed host synchronization;
- Zabbix to gateway HTTP API: live metric collection through HTTP agent items.

Zabbix performs the actual monitoring through HTTP agent master items and dependent items.

## Why Gateway Mode

HPE documents a Central rate limit of 10 API calls per second across the Central account. This limit is shared by all tokens and client credentials in that account.

The gateway keeps all Aruba API calls behind one rate limiter. The default is `8` API calls per second to leave a small safety margin.

## Requirements

- Zabbix 7.0 or newer.
- Python 3.10 or newer.
- A server that can reach GreenLake, Aruba Central New Central APIs, and Zabbix API.
- Zabbix must be able to reach the gateway HTTP URL.

## Deployment And Exposure

Expose the gateway only when Zabbix needs to reach it from outside the local network, for example with Zabbix Cloud. In that case, publish it through a protected endpoint such as Cloudflare Tunnel plus access controls, and avoid exposing plain TCP `6767` directly to the Internet.

When Zabbix runs on a local VM or on the same private network, keep the gateway private. The recommended deployment is a VM or service host in the same L2/L3 network as Zabbix or the Zabbix proxy, with `gateway.base_url` pointing to an internal DNS name or IP address. Do not publish port `6767` publicly in this scenario.

## GreenLake Credentials

For each GreenLake workspace collect:

- `workspace_id`
- `client_id`
- `client_secret`
- `central_base_url`

For MSP workspaces, configure the MSP workspace. The sync command discovers MSP tenants and exchanges tenant tokens automatically.

For standalone workspaces, configure the customer workspace directly.

Example Central base URL:

```text
https://de2.api.central.arubanetworks.com
```

## Configuration

Copy the example:

```powershell
Copy-Item .\workspaces.example.json .\workspaces.json
```

Minimal structure:

```json
{
  "config_version": "2.0.0",
  "gateway": {
    "listen": "0.0.0.0",
    "port": 6767,
    "base_url": "http://ip-or-fqdn-gatewayserver:6767",
    "api_rate_limit_per_second": 8,
    "api_retry_attempts": 3,
    "device_cache_ttl_seconds": 240,
    "site_cache_ttl_seconds": 300,
    "version_check_enabled": true,
    "version_check_ref": "main"
  },
  "sync": {
    "interval_seconds": 1800
  },
  "zabbix": {
    "api_url": "https://zabbix.example.com/api_jsonrpc.php",
    "api_token": "zabbix-api-token",
    "unmapped_host_group": "HPE Aruba Central/Unmapped",
    "gateway_host": "HPE Aruba Central Gateway",
    "auto_import_template": true
  },
  "workspaces": []
}
```

Important settings:

- `gateway.base_url`: URL that Zabbix uses to reach the gateway.
- `gateway.api_rate_limit_per_second`: global Aruba API throttle. Keep it at or below `10`; default is `8`.
- `gateway.device_cache_ttl_seconds`: cache TTL for device raw data.
- `gateway.site_cache_ttl_seconds`: cache TTL for site health data.
- `sync.interval_seconds`: used by the combined `run` command for periodic Zabbix sync.
- `zabbix.unmapped_host_group`: landing host group for new managed hosts.
- `zabbix.gateway_host`: Zabbix host used for gateway health.

Host tags are configurable:

```json
"host_tags": {
  "ap": "WiFi",
  "switch": "Switch",
  "gateway": "Gateway"
}
```

Device host names are generated from the configured `host_prefix` plus the Central device name:

```text
CUSTOMER - AP01
CUSTOMER - SWITCH01
```

For MSP tenants, set `tenant_mappings`:

```json
"tenant_mappings": [
  {
    "tenant_name": "Central tenant name",
    "host_prefix": "CUSTOMER",
    "discover_devices": ["ap", "switch"]
  }
]
```

For standalone workspaces, set `mapping`:

```json
"mapping": {
  "host_prefix": "CUSTOMER",
  "discover_devices": "all"
}
```

`discover_devices` can be configured at workspace, MSP tenant mapping, or standalone mapping level. Supported values are:

- `"all"`
- `"ap"`
- `"switch"`
- `"gateway"`
- arrays such as `["ap", "switch"]`

`workspaces.json`, `.token_cache.json`, and `gateway_state.json` contain local operational data and must not be committed.

## Commands

Validate configuration:

```powershell
python .\central_gateway.py config-check
```

Preview template import:

```powershell
python .\central_gateway.py import-zabbix-template
```

Apply template import:

```powershell
python .\central_gateway.py import-zabbix-template --apply
```

Preview Zabbix synchronization:

```powershell
python .\central_gateway.py sync-zabbix
```

Apply Zabbix synchronization:

```powershell
python .\central_gateway.py sync-zabbix --apply
```

Start only the gateway:

```powershell
python .\central_gateway.py gateway
```

Start gateway and run periodic sync in the same process:

```powershell
python .\central_gateway.py run
```

## Gateway Endpoints

Health:

```text
GET /api/v2/health
```

Device raw data:

```text
GET /api/v2/device/{device_key}/raw
```

Site health:

```text
GET /api/v2/site/{site_id}/health
```

Client onboarding stage counts:

```text
GET /api/v2/site/{site_id}/client-onboarding-stage/count?field=topreasons&window-ms=3600000
```

Zabbix templates use host-level macros generated by `sync-zabbix`. These required macros are intentionally not defined as template-level defaults, because every managed host must receive its own values:

- `{$CENTRAL.GATEWAY.URL}` is set from `gateway.base_url`;
- `{$CENTRAL.DEVICE.KEY}` is set to the stable device key built from workspace, tenant, device type, and serial;
- `{$CENTRAL.DEVICE.SERIAL}` stores the device serial;
- `{$CENTRAL.SITE.ID}` stores the stable Central site id on Site hosts.

Only real defaults such as polling intervals, nodata windows, and sync age thresholds are kept inside the templates. Device and site renames in Central are handled by matching the stable host-level macros first, then updating the Zabbix host name in place.

Gateway response shape:

```json
{
  "gateway": {
    "status": "ok",
    "cache": "miss",
    "fetched_at": 1778830000
  },
  "device": {
    "kind": "ap",
    "serial": "CNXXXX"
  },
  "summary": {
    "status": "ONLINE",
    "firmware": "10.x"
  },
  "data": {}
}
```

Raw master items are intentionally configured with `history: 0`. They may look empty in Latest data, but they are still required because dependent items extract all visible metrics from their current JSON response.

## Streaming APIs

HPE Aruba Central Streaming APIs are intentionally not used by this gateway at the moment. They require Advanced licensing, while the current implementation relies on REST monitoring APIs so it can work in environments where Advanced licenses are not available.

## Zabbix Templates

The bundled template contains:

- `HPE Aruba Central NG - Gateway`
- `HPE Aruba Central NG - Site`
- `HPE Aruba Central NG - DeviceType AP`
- `HPE Aruba Central NG - DeviceType Switch`
- `HPE Aruba Central NG - DeviceType Gateway`

Device templates use:

- one HTTP agent raw master item
- dependent items for status, firmware, uptime, CPU, memory, and config status where available
- basic not-online triggers
- firmware detail enrichment, including recommended firmware and last upgraded timestamp where Central returns it
- AP radios/ports/WLAN payloads inside the raw gateway response
- switch interfaces plus best-effort stack, LAG, VSX, and hardware trend payloads where the tenant/API/model exposes them
- gateway device basic status, firmware, and ports; gateway-specific monitoring is intentionally minimal in this development branch

Site templates use:

- one Site Health HTTP master item;
- one Client Onboarding HTTP master item;
- dependent items for gateway status, Good/Fair/Poor health score, and client onboarding failed count.

The sync command creates one managed Zabbix Site host for each Central site discovered from monitored devices. Site hosts use `{$CENTRAL.SITE.ID}` and the same gateway URL macro as device hosts.

Triggers are intentionally conservative. Important triggers such as device down and AP radio down are enabled. Noisy or environment-specific triggers, such as interface down and CRC growth, are imported disabled by default.

## Operating Model

Recommended first run:

```powershell
python .\central_gateway.py config-check
python .\central_gateway.py sync-zabbix --apply
python .\central_gateway.py run
```

`run` is the preferred production mode. It starts both components in one long-running process:

- the HTTP gateway used by Zabbix HTTP agent items;
- the periodic Zabbix sync/import loop, controlled by `sync.interval_seconds`.

The first sync is executed immediately after startup, then repeated at the configured interval.

Alternative split mode is available when you want separate process ownership: run `python .\central_gateway.py gateway` continuously and schedule `python .\central_gateway.py sync-zabbix --apply` every 10 to 30 minutes. The single `run` mode is simpler and is the recommended default.

## Windows Service

Use a service wrapper such as NSSM. Example:

```powershell
nssm install HPEArubaCentralGateway "C:\Python313\python.exe"
nssm set HPEArubaCentralGateway AppParameters "C:\Program Files\hpe-central-zabbix\central_gateway.py" run
nssm set HPEArubaCentralGateway AppDirectory "C:\Program Files\hpe-central-zabbix"
nssm set HPEArubaCentralGateway Start SERVICE_AUTO_START
nssm start HPEArubaCentralGateway
```

If you prefer Windows Task Scheduler:

```powershell
$action = New-ScheduledTaskAction -Execute "C:\Python313\python.exe" -Argument ".\central_gateway.py run" -WorkingDirectory "C:\Program Files\hpe-central-zabbix"
$trigger = New-ScheduledTaskTrigger -AtStartup
Register-ScheduledTask -TaskName "HPE Aruba Central Gateway" -Action $action -Trigger $trigger -RunLevel Highest -Description "HPE Aruba Central NG gateway and Zabbix sync"
Start-ScheduledTask -TaskName "HPE Aruba Central Gateway"
```

Adjust paths to match the actual Python installation and project directory.

## Linux Service

Create a dedicated user and install the project under a stable directory, for example `/opt/hpe-central-zabbix`.

Example systemd unit:

```ini
[Unit]
Description=HPE Aruba Central NG Gateway for Zabbix
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=zabbix-gateway
WorkingDirectory=/opt/hpe-central-zabbix
ExecStart=/usr/bin/python3 /opt/hpe-central-zabbix/central_gateway.py run
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Enable it:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hpe-aruba-central-gateway.service
sudo systemctl status hpe-aruba-central-gateway.service
```

## Gateway Host Monitoring

The Zabbix gateway host is not only informational. Keep it enabled, because it monitors the health of the whole integration:

- HTTP gateway reachability and `/api/v2/health` status;
- gateway package/template update availability;
- managed Central device count;
- stale managed host count and stale managed host names;
- last successful Zabbix sync age;
- cache size and configured Central API rate limit.

The bundled gateway template includes triggers for gateway failure, missing gateway health data, stale sync state, orphaned/stale managed hosts, and available package/template updates. The missing health trigger uses `{$CENTRAL.GATEWAY.NODATA}`, default `10m`; the stale sync trigger uses `{$CENTRAL.SYNC.MAX_AGE_SECONDS}`, default `7200`.

## Safety

The sync process adds a managed tag to every host it creates:

```text
hpe-aruba-central-ng
```

If a host with the same name already exists but does not have that tag, sync refuses to update it.

New hosts are created only in `zabbix.unmapped_host_group`. Existing managed hosts are updated in place, but their host group membership is not used for customer/site placement automation in this release.
