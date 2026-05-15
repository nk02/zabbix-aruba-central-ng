# Changelog

All notable changes to this project are documented here.

## Unreleased

- Documentation now starts from Git clone installation on a production VM.
- Documented that `config_version` is the configuration schema version, not the application release version.

## 2.0.9

- Fixed preservation of existing Zabbix template group memberships during template import.
- Updated importer to read Zabbix template groups from the `templategroups` API field.

## 2.0.8

- Added `zabbix.template_group` to choose the Zabbix template group used by bundled templates.
- Preserved existing additional template groups during template import.
- Reworked README command examples around Linux production usage.
- Added production update guidance using `git pull`.

## 2.0.7

- Added a pre-check for required Zabbix template groups before importing templates.
- Added a clearer error when the configured template group does not exist.
- Documented that template groups are not created by the importer.

## 2.0.6

- Changed template import rules to avoid creating template groups during `configuration.import`.
- Documented the Zabbix API methods and permissions required for the integration user.

## 2.0.5

- Added Zabbix API TLS options: `zabbix.tls_verify` and `zabbix.tls_ca_file`.
- Improved Zabbix API network error reporting.
- Added Linux command examples using `python3` and `./` paths.

## 2.0.4

- Changed the default gateway port from `8080` to `6767`.
- Updated example gateway base URL to `http://ip-or-fqdn-gatewayserver:6767`.
- Aligned README and code fallbacks with the new default port.

## 2.0.3

- Removed required host-level macro defaults from templates.
- Kept only real template defaults such as polling intervals, nodata windows, sync age thresholds, and template version.
- Documented host-level macro behavior.

## 2.0.2

- Renamed device templates to clearly separate the local gateway service from Aruba gateway devices:
  - `HPE Aruba Central NG - DeviceType AP`
  - `HPE Aruba Central NG - DeviceType Switch`
  - `HPE Aruba Central NG - DeviceType Gateway`
- Added stale managed host health items and alerts.
- Improved device/site rename handling by matching stable host-level macros first.
- Added gateway health nodata monitoring.

## 2.0.1

- Removed the legacy PowerShell launcher.
- Documented gateway service operation for Windows and Linux.
- Added gateway sync freshness health monitoring.
- Switched local gateway HTTP endpoints to `/api/v2/...`.

## 2.0.0

- Introduced the v2 gateway architecture.
- Replaced push/trapper collection with a bidirectional gateway model:
  - gateway syncs templates and managed hosts through the Zabbix API;
  - Zabbix collects metrics through HTTP agent items against the gateway.
- Added AP, switch, gateway-device, site-health, and client-onboarding templates.
- Added workspace and MSP tenant discovery through `workspaces.json`.
- Added Central API caching and rate limiting.
