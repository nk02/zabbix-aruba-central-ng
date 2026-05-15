#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from urllib.request import Request, urlopen


APP_VERSION = "2.0.0-dev2"
CONFIG_SCHEMA_VERSION = "2.0.0"
TEMPLATE_VERSION = "2.0.0-dev2"
GREENLAKE_API = "https://global.api.greenlake.hpe.com"
TOKEN_PATH = "/authorization/v2/oauth2/{workspace_id}/token"
TENANTS_PATH = "/workspaces/v1/msp-tenants"
CONFIG_PATH = Path(__file__).with_name("workspaces.json")
TOKEN_CACHE_PATH = Path(__file__).with_name(".token_cache.json")
GATEWAY_STATE_PATH = Path(__file__).with_name("gateway_state.json")
TEMPLATE_PATH = Path(__file__).with_name("zabbix_template_hpe_aruba_central_new_ap_trapper.yaml")

RATE_LIMIT_LOCK = threading.Lock()
RATE_LIMIT_WINDOW = 0.0
RATE_LIMIT_COUNT = 0
HTTP_CACHE_LOCK = threading.Lock()
HTTP_CACHE: dict[str, dict[str, Any]] = {}


class ConfigError(Exception):
    pass


class CentralError(Exception):
    pass


class ZabbixError(Exception):
    pass


def utc_now() -> int:
    return int(time.time())


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json_config(path: Path = CONFIG_PATH) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Missing config file: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON config {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("Config root must be a JSON object")
    return data


def config_section(config: dict[str, Any], name: str) -> dict[str, Any]:
    value = config.get(name)
    return value if isinstance(value, dict) else {}


def config_list(config: dict[str, Any], name: str) -> list[dict[str, Any]]:
    value = config.get(name)
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def config_check(config: dict[str, Any]) -> dict[str, Any]:
    missing: list[str] = []
    if not config_section(config, "zabbix").get("api_url"):
        missing.append("zabbix.api_url")
    if not config_section(config, "zabbix").get("api_token"):
        missing.append("zabbix.api_token")
    if not config_section(config, "gateway").get("base_url"):
        missing.append("gateway.base_url")
    for index, workspace in enumerate(config_list(config, "workspaces")):
        for key in ("name", "mode", "workspace_id", "client_id", "client_secret", "central_base_url"):
            if not workspace.get(key):
                missing.append(f"workspaces[{index}].{key}")
    return {
        "status": "ok" if not missing else "warning",
        "app_version": APP_VERSION,
        "config_version": config.get("config_version"),
        "expected_config_version": CONFIG_SCHEMA_VERSION,
        "missing": missing,
        "workspace_count": len(config_list(config, "workspaces")),
    }


def token_cache_load() -> dict[str, Any]:
    if not TOKEN_CACHE_PATH.exists():
        return {}
    try:
        data = json.loads(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def token_cache_save(cache: dict[str, Any]) -> None:
    TOKEN_CACHE_PATH.write_text(json.dumps(cache, indent=2, sort_keys=True), encoding="utf-8")


def cached_token(cache_key: str) -> str | None:
    item = token_cache_load().get(cache_key)
    if not isinstance(item, dict):
        return None
    token = item.get("access_token")
    expires_at = int(item.get("expires_at") or 0)
    if isinstance(token, str) and token and expires_at - utc_now() > 90:
        return token
    return None


def store_cached_token(cache_key: str, token: str, expires_in: int) -> None:
    cache = token_cache_load()
    cache[cache_key] = {
        "access_token": token,
        "expires_at": utc_now() + max(60, int(expires_in)),
        "stored_at": utc_now(),
    }
    token_cache_save(cache)


def env_int(name: str, default: int, minimum: int | None = None, maximum: int | None = None) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def throttle_central(config: dict[str, Any]) -> None:
    global RATE_LIMIT_WINDOW, RATE_LIMIT_COUNT
    gateway = config_section(config, "gateway")
    limit = int(gateway.get("api_rate_limit_per_second") or 8)
    limit = max(1, min(10, limit))
    while True:
        with RATE_LIMIT_LOCK:
            now = time.monotonic()
            if now - RATE_LIMIT_WINDOW >= 1:
                RATE_LIMIT_WINDOW = now
                RATE_LIMIT_COUNT = 0
            if RATE_LIMIT_COUNT < limit:
                RATE_LIMIT_COUNT += 1
                return
            sleep_for = max(0.01, 1 - (now - RATE_LIMIT_WINDOW))
        time.sleep(sleep_for)


def request_json(
    method: str,
    url: str,
    config: dict[str, Any],
    token: str | None = None,
    form: dict[str, str] | None = None,
    query: dict[str, str | int] | None = None,
    throttle: bool = True,
) -> dict[str, Any]:
    if query:
        url = f"{url}?{urlencode(query)}"
    headers = {"Accept": "application/json"}
    data = None
    if form is not None:
        data = urlencode(form).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = f"Bearer {token}"

    attempts = int(config_section(config, "gateway").get("api_retry_attempts") or 3)
    for attempt in range(1, max(1, attempts) + 1):
        if throttle and "arubanetworks.com" in url:
            throttle_central(config)
        req = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(req, timeout=60) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            if exc.code == 429 and attempt < attempts:
                retry_after = exc.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else 0.0
                except (TypeError, ValueError):
                    delay = 0.0
                time.sleep(max(delay, min(2 ** attempt, 10)))
                continue
            raise CentralError(f"HTTP {exc.code} calling {url}: {body}") from exc
        except URLError as exc:
            raise CentralError(f"Network error calling {url}: {exc.reason}") from exc
    raise CentralError(f"Unable to call {url}")


def workspace_token(config: dict[str, Any], workspace: dict[str, Any], force_refresh: bool = False) -> str:
    workspace_id = str(workspace["workspace_id"]).replace("-", "")
    client_id = str(workspace["client_id"])
    cache_key = f"workspace:{workspace_id}:{client_id}"
    if not force_refresh:
        cached = cached_token(cache_key)
        if cached:
            return cached
    data = request_json(
        "POST",
        GREENLAKE_API + TOKEN_PATH.format(workspace_id=workspace_id),
        config,
        form={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": str(workspace["client_secret"]),
        },
        throttle=False,
    )
    token = data.get("access_token")
    if not isinstance(token, str) or not token:
        raise CentralError(f"Token response for workspace {workspace.get('name')} did not include access_token")
    store_cached_token(cache_key, token, int(data.get("expires_in") or 900))
    return token


def tenant_token(config: dict[str, Any], workspace: dict[str, Any], tenant: dict[str, Any], force_refresh: bool = False) -> str:
    if tenant.get("mode") == "standalone":
        return workspace_token(config, workspace, force_refresh=force_refresh)
    tenant_id = str(tenant["tenant_id"]).replace("-", "")
    workspace_id = str(workspace["workspace_id"]).replace("-", "")
    client_id = str(workspace["client_id"])
    cache_key = f"tenant:{workspace_id}:{client_id}:{tenant_id}"
    if not force_refresh:
        cached = cached_token(cache_key)
        if cached:
            return cached
    msp_token = workspace_token(config, workspace)
    data = request_json(
        "POST",
        GREENLAKE_API + TOKEN_PATH.format(workspace_id=tenant_id),
        config,
        form={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": msp_token,
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
        },
        throttle=False,
    )
    token = data.get("access_token")
    if not isinstance(token, str) or not token:
        raise CentralError(f"Token response for tenant {tenant.get('tenant_name')} did not include access_token")
    store_cached_token(cache_key, token, int(data.get("expires_in") or 900))
    return token


def central_get(
    config: dict[str, Any],
    workspace: dict[str, Any],
    tenant: dict[str, Any],
    path: str,
    query: dict[str, str | int] | None = None,
) -> dict[str, Any]:
    token = tenant_token(config, workspace, tenant)
    base_url = str(workspace["central_base_url"]).rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    try:
        return request_json("GET", base_url + path, config, token=token, query=query)
    except CentralError as exc:
        if "HTTP 401" not in str(exc):
            raise
        token = tenant_token(config, workspace, tenant, force_refresh=True)
        return request_json("GET", base_url + path, config, token=token, query=query)


def get_all_pages(
    config: dict[str, Any],
    workspace: dict[str, Any],
    tenant: dict[str, Any],
    path: str,
    query: dict[str, str | int] | None = None,
) -> list[dict[str, Any]]:
    offset = 0
    limit = 1000
    results: list[dict[str, Any]] = []
    while True:
        params = {"limit": limit, "offset": offset}
        if query:
            params.update(query)
        data = central_get(config, workspace, tenant, path, params)
        items = data.get("items")
        if not isinstance(items, list):
            items = data.get("data") if isinstance(data.get("data"), list) else []
        records = [item for item in items if isinstance(item, dict)]
        results.extend(records)
        total = int(data.get("total") or len(results))
        offset += len(records)
        if not records or offset >= total:
            break
    return results


def tenant_name(raw: dict[str, Any]) -> str:
    return str(raw.get("workspaceName") or raw.get("tenant_name") or raw.get("name") or raw.get("id") or "")


def workspace_tenants(config: dict[str, Any], workspace: dict[str, Any]) -> list[dict[str, Any]]:
    if workspace.get("mode") == "standalone":
        return [{
            "tenant_id": str(workspace["workspace_id"]),
            "tenant_name": str(workspace.get("name") or workspace["workspace_id"]),
            "workspace_id": str(workspace["workspace_id"]),
            "workspace_name": str(workspace.get("name") or workspace["workspace_id"]),
            "mode": "standalone",
        }]
    token = workspace_token(config, workspace)
    tenants: list[dict[str, Any]] = []
    offset = 0
    limit = 100
    while True:
        data = request_json(
            "GET",
            GREENLAKE_API + TENANTS_PATH,
            config,
            token=token,
            query={"offset": offset, "limit": limit},
            throttle=False,
        )
        items = data.get("items") if isinstance(data.get("items"), list) else []
        tenants.extend(item for item in items if isinstance(item, dict))
        total = int(data.get("total") or len(tenants))
        offset += len(items)
        if not items or offset >= total:
            break
    allow = {str(item).lower() for item in workspace.get("tenant_allowlist") or []}
    mapped: list[dict[str, Any]] = []
    for item in tenants:
        tid = str(item.get("id") or "")
        name = tenant_name(item)
        if allow and tid.lower() not in allow and name.lower() not in allow:
            continue
        mapped.append({
            "tenant_id": tid,
            "tenant_name": name,
            "workspace_id": str(workspace["workspace_id"]),
            "workspace_name": str(workspace.get("name") or workspace["workspace_id"]),
            "mode": "msp",
        })
    return mapped


def first_value(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return None


def normalize_status(value: Any) -> str:
    text = str(value or "").strip()
    lowered = text.lower()
    if lowered in ("online", "up", "connected", "ok"):
        return "ONLINE"
    if lowered in ("offline", "down", "disconnected"):
        return "OFFLINE"
    return text.upper() if text else ""


def millis_to_seconds(value: Any) -> int | None:
    try:
        return int(float(value) / 1000)
    except (TypeError, ValueError):
        return None


def normalize_device(raw: dict[str, Any], workspace: dict[str, Any], tenant: dict[str, Any]) -> dict[str, Any]:
    device_type = str(first_value(raw, "deviceType", "type", "device_type") or "").upper()
    if device_type in ("ACCESSPOINT", "AP"):
        device_type = "ACCESS_POINT"
    return {
        "workspace_id": tenant["workspace_id"],
        "workspace_name": tenant["workspace_name"],
        "tenant_id": tenant["tenant_id"],
        "tenant_name": tenant["tenant_name"],
        "serial": str(first_value(raw, "serialNumber", "serial", "id") or ""),
        "name": str(first_value(raw, "deviceName", "name", "hostname") or ""),
        "model": first_value(raw, "model", "partNumber"),
        "mac": first_value(raw, "macAddress", "mac"),
        "ipv4": raw.get("ipv4"),
        "site_id": first_value(raw, "siteId", "site_id"),
        "site_name": first_value(raw, "siteName", "site"),
        "status": normalize_status(first_value(raw, "status", "health")),
        "firmware": first_value(raw, "firmwareVersion", "softwareVersion"),
        "device_type": device_type,
        "raw": raw,
    }


def device_kind(device: dict[str, Any]) -> str | None:
    device_type = str(device.get("device_type") or "").upper()
    if device_type == "ACCESS_POINT":
        return "ap"
    if device_type == "SWITCH":
        return "switch"
    if device_type == "GATEWAY":
        return "gateway"
    return None


def safe_name(value: str) -> str:
    value = re.sub(r"\s+", " ", value.strip())
    return value or "Unnamed"


def mapping_for(workspace: dict[str, Any], tenant: dict[str, Any]) -> dict[str, Any]:
    if tenant.get("mode") == "standalone":
        mapping = workspace.get("mapping")
        return mapping if isinstance(mapping, dict) else {}
    mappings = workspace.get("tenant_mappings") if isinstance(workspace.get("tenant_mappings"), list) else []
    tenant_id = str(tenant.get("tenant_id") or "").lower()
    name = str(tenant.get("tenant_name") or "").lower()
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        if str(mapping.get("tenant_id") or "").lower() == tenant_id:
            return mapping
        if str(mapping.get("tenant_name") or "").lower() == name:
            return mapping
    return {}


def host_prefix(workspace: dict[str, Any], tenant: dict[str, Any]) -> str:
    mapping = mapping_for(workspace, tenant)
    return safe_name(str(mapping.get("host_prefix") or tenant.get("tenant_name") or workspace.get("name") or "Central"))


def device_host_name(prefix: str, device: dict[str, Any]) -> str:
    return safe_name(f"{prefix} - {device.get('name') or device.get('serial')}")


def device_key(workspace: dict[str, Any], tenant: dict[str, Any], kind: str, serial: str) -> str:
    parts = [str(workspace["workspace_id"]).replace("-", ""), str(tenant["tenant_id"]).replace("-", ""), kind, serial]
    return ".".join(re.sub(r"[^A-Za-z0-9_-]", "_", part) for part in parts)


def discover_devices(config: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    devices: list[dict[str, Any]] = []
    state: dict[str, Any] = {"version": APP_VERSION, "generated_at": iso_now(), "devices": {}}
    for workspace in config_list(config, "workspaces"):
        tenants = workspace_tenants(config, workspace)
        for tenant in tenants:
            discovered = get_all_pages(config, workspace, tenant, "/network-monitoring/v1/devices")
            seen: set[str] = set()
            for raw in discovered:
                device = normalize_device(raw, workspace, tenant)
                kind = device_kind(device)
                serial = str(device.get("serial") or "")
                if not kind or not serial or serial in seen:
                    continue
                seen.add(serial)
                prefix = host_prefix(workspace, tenant)
                host = device_host_name(prefix, device)
                key = device_key(workspace, tenant, kind, serial)
                device.update({"kind": kind, "host": host, "key": key})
                devices.append(device)
                state["devices"][key] = {
                    "key": key,
                    "kind": kind,
                    "serial": serial,
                    "site_id": device.get("site_id"),
                    "site_name": device.get("site_name"),
                    "host": host,
                    "workspace_id": str(workspace["workspace_id"]),
                    "workspace_name": str(workspace.get("name") or workspace["workspace_id"]),
                    "tenant_id": str(tenant["tenant_id"]),
                    "tenant_name": str(tenant["tenant_name"]),
                    "central_base_url": str(workspace["central_base_url"]),
                }
    return devices, state


def save_gateway_state(state: dict[str, Any]) -> None:
    GATEWAY_STATE_PATH.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def load_gateway_state() -> dict[str, Any]:
    if not GATEWAY_STATE_PATH.exists():
        return {"devices": {}}
    try:
        data = json.loads(GATEWAY_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"devices": {}}
    return data if isinstance(data, dict) else {"devices": {}}


def zabbix_config(config: dict[str, Any]) -> dict[str, Any]:
    zbx = config_section(config, "zabbix")
    if not zbx.get("api_url") or not zbx.get("api_token"):
        raise ConfigError("zabbix.api_url and zabbix.api_token are required")
    return zbx


def zabbix_api_call(config: dict[str, Any], method: str, params: dict[str, Any] | list[Any] | None = None) -> Any:
    zbx = zabbix_config(config)
    payload = {"jsonrpc": "2.0", "method": method, "params": params or {}, "id": 1}
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        str(zbx["api_url"]),
        data=data,
        headers={"Accept": "application/json", "Content-Type": "application/json-rpc", "Authorization": f"Bearer {zbx['api_token']}"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=60) as response:
            body = response.read().decode("utf-8")
            result = json.loads(body) if body else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise ZabbixError(f"Zabbix API HTTP {exc.code}: {body}") from exc
    if isinstance(result, dict) and result.get("error"):
        raise ZabbixError(f"Zabbix API {method} failed: {result['error']}")
    return result.get("result") if isinstance(result, dict) else result


def template_names() -> list[str]:
    source = TEMPLATE_PATH.read_text(encoding="utf-8")
    return re.findall(r"^\s+template:\s+(.+?)\s*$", source, flags=re.MULTILINE)


def import_zabbix_template(config: dict[str, Any], apply: bool = False) -> dict[str, Any]:
    result = {"apply": apply, "path": str(TEMPLATE_PATH), "templates": template_names()}
    if not apply:
        return result
    zabbix_api_call(config, "configuration.import", {
        "format": "yaml",
        "rules": {
            "template_groups": {"createMissing": True},
            "templates": {"createMissing": True, "updateExisting": True},
            "items": {"createMissing": True, "updateExisting": True},
            "triggers": {"createMissing": True, "updateExisting": True},
            "valueMaps": {"createMissing": True, "updateExisting": True},
        },
        "source": TEMPLATE_PATH.read_text(encoding="utf-8"),
    })
    result["imported"] = True
    return result


def hostgroup_id(config: dict[str, Any], name: str, apply: bool) -> str:
    groups = zabbix_api_call(config, "hostgroup.get", {"output": ["groupid", "name"], "filter": {"name": [name]}})
    if isinstance(groups, list) and groups:
        return str(groups[0]["groupid"])
    if not apply:
        return ""
    created = zabbix_api_call(config, "hostgroup.create", {"name": name})
    return str(created["groupids"][0])


def template_ids(config: dict[str, Any], names: list[str]) -> list[dict[str, str]]:
    templates = zabbix_api_call(config, "template.get", {"output": ["templateid", "host"], "filter": {"host": names}})
    by_name = {item["host"]: item["templateid"] for item in templates or [] if isinstance(item, dict)}
    missing = [name for name in names if name not in by_name]
    if missing:
        raise ZabbixError(f"Missing templates: {', '.join(missing)}")
    return [{"templateid": str(by_name[name])} for name in names]


def managed_tag(config: dict[str, Any]) -> dict[str, str]:
    tag = config_section(config, "zabbix").get("managed_tag")
    if isinstance(tag, dict) and tag.get("tag"):
        return {"tag": str(tag["tag"]), "value": str(tag.get("value") or "")}
    return {"tag": "hpe-aruba-central-ng", "value": ""}


def host_tags(config: dict[str, Any], kind: str) -> list[dict[str, str]]:
    tags = config_section(config, "zabbix").get("host_tags")
    if not isinstance(tags, dict):
        tags = {"ap": "WiFi", "switch": "Switch", "gateway": "Gateway"}
    tag = tags.get(kind)
    return [{"tag": str(tag), "value": ""}] if tag else []


def zabbix_templates(config: dict[str, Any]) -> dict[str, str]:
    templates = config_section(config, "zabbix").get("templates")
    if not isinstance(templates, dict):
        templates = {}
    return {
        "service": str(templates.get("service") or "HPE Aruba Central NG - Gateway"),
        "ap": str(templates.get("ap") or "HPE Aruba Central NG - AP"),
        "switch": str(templates.get("switch") or "HPE Aruba Central NG - Switch"),
        "gateway": str(templates.get("gateway") or "HPE Aruba Central NG - Gateway Device"),
    }


def host_has_tag(tags: list[dict[str, Any]], wanted: dict[str, str]) -> bool:
    return any(str(tag.get("tag") or "") == wanted["tag"] and str(tag.get("value") or "") == wanted["value"] for tag in tags or [])


def zabbix_host(config: dict[str, Any], host: str) -> dict[str, Any] | None:
    hosts = zabbix_api_call(config, "host.get", {
        "output": ["hostid", "host", "name"],
        "selectTags": "extend",
        "selectMacros": "extend",
        "selectParentTemplates": ["templateid", "host"],
        "filter": {"host": [host]},
    })
    return hosts[0] if isinstance(hosts, list) and hosts else None


def merge_macros(existing: list[dict[str, Any]], desired: dict[str, str]) -> list[dict[str, str]]:
    managed_names = set(desired)
    merged = [
        {"macro": str(item.get("macro")), "value": str(item.get("value") or "")}
        for item in existing or []
        if isinstance(item, dict) and item.get("macro") and str(item.get("macro")) not in managed_names
    ]
    merged.extend({"macro": macro, "value": value} for macro, value in desired.items())
    return merged


def ensure_host(config: dict[str, Any], plan: dict[str, Any], apply: bool) -> dict[str, Any]:
    zbx = config_section(config, "zabbix")
    group_name = str(zbx.get("unmapped_host_group") or "HPE Aruba Central/Unmapped")
    groupid = hostgroup_id(config, group_name, apply)
    existing = zabbix_host(config, str(plan["host"]))
    tag = managed_tag(config)
    tags = host_tags(config, str(plan["kind"])) + [tag]
    result = {"host": plan["host"], "kind": plan["kind"], "exists": bool(existing), "created": False, "updated": False}
    if not apply:
        result["pending"] = True
        return result
    templates = template_ids(config, [str(plan["template"])])
    macros = {
        "{$CENTRAL.GATEWAY.URL}": str(config_section(config, "gateway").get("base_url") or "http://127.0.0.1:8080"),
    }
    if plan.get("device_key"):
        macros.update({
            "{$CENTRAL.DEVICE.KEY}": str(plan["device_key"]),
            "{$CENTRAL.DEVICE.TYPE}": str(plan["kind"]),
            "{$CENTRAL.DEVICE.SERIAL}": str(plan.get("serial") or ""),
        })
    if existing:
        if not host_has_tag(existing.get("tags") or [], tag):
            raise ZabbixError(f"Existing host {plan['host']!r} is not managed by this integration")
        params: dict[str, Any] = {
            "hostid": existing["hostid"],
            "name": plan.get("visible_name") or plan["host"],
            "tags": tags,
            "macros": merge_macros(existing.get("macros") or [], macros),
        }
        current_template_ids = {str(item.get("templateid")) for item in existing.get("parentTemplates") or []}
        for template in templates:
            current_template_ids.add(template["templateid"])
        params["templates"] = [{"templateid": templateid} for templateid in sorted(current_template_ids)]
        zabbix_api_call(config, "host.update", params)
        result["updated"] = True
        return result
    params = {
        "host": plan["host"],
        "name": plan.get("visible_name") or plan["host"],
        "groups": [{"groupid": groupid}],
        "templates": templates,
        "tags": tags,
        "macros": merge_macros([], macros),
    }
    created = zabbix_api_call(config, "host.create", params)
    result["created"] = bool(created.get("hostids"))
    return result


def build_host_plans(config: dict[str, Any], devices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    zbx = config_section(config, "zabbix")
    templates = zabbix_templates(config)
    gateway_host = str(zbx.get("gateway_host") or "HPE Aruba Central Gateway")
    plans = [{
        "kind": "gateway_service",
        "host": gateway_host,
        "visible_name": gateway_host,
        "template": templates["service"],
    }]
    for device in devices:
        kind = str(device["kind"])
        plans.append({
            "kind": kind,
            "host": device["host"],
            "visible_name": device["host"],
            "template": templates[kind],
            "device_key": device["key"],
            "serial": device.get("serial"),
        })
    return sorted(plans, key=lambda item: str(item["host"]))


def sync_zabbix(config: dict[str, Any], apply: bool = False) -> dict[str, Any]:
    if config_section(config, "zabbix").get("auto_import_template", True):
        import_result = import_zabbix_template(config, apply=apply)
    else:
        import_result = {"status": "disabled"}
    devices, state = discover_devices(config)
    save_gateway_state(state)
    plans = build_host_plans(config, devices)
    results = [ensure_host(config, plan, apply) for plan in plans]
    return {
        "apply": apply,
        "version": APP_VERSION,
        "template": import_result,
        "devices": len(devices),
        "planned_hosts": len(plans),
        "created": sum(1 for item in results if item.get("created")),
        "updated": sum(1 for item in results if item.get("updated")),
        "hosts": results,
        "state_file": str(GATEWAY_STATE_PATH),
    }


def find_workspace(config: dict[str, Any], workspace_id: str) -> dict[str, Any]:
    wanted = workspace_id.replace("-", "").lower()
    for workspace in config_list(config, "workspaces"):
        if str(workspace.get("workspace_id") or "").replace("-", "").lower() == wanted:
            return workspace
    raise CentralError(f"Workspace {workspace_id} not found in config")


def gateway_tenant_record(device: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenant_id": str(device["tenant_id"]),
        "tenant_name": str(device.get("tenant_name") or device["tenant_id"]),
        "workspace_id": str(device["workspace_id"]),
        "workspace_name": str(device.get("workspace_name") or device["workspace_id"]),
        "mode": "standalone" if str(device["tenant_id"]).replace("-", "") == str(device["workspace_id"]).replace("-", "") else "msp",
    }


def device_path(kind: str, serial: str) -> str:
    if kind == "ap":
        return f"/network-monitoring/v1/aps/{quote(serial)}"
    if kind == "switch":
        return f"/network-monitoring/v1/switches/{quote(serial)}"
    if kind == "gateway":
        return f"/network-monitoring/v1/gateways/{quote(serial)}"
    raise CentralError(f"Unsupported device kind {kind}")


def normalize_summary(kind: str, raw: dict[str, Any], device: dict[str, Any]) -> dict[str, Any]:
    data = raw.get("ap") if kind == "ap" and isinstance(raw.get("ap"), dict) else raw
    stats = data.get("apStats")
    first_stats = stats[0] if isinstance(stats, list) and stats and isinstance(stats[0], dict) else {}
    return {
        "kind": kind,
        "serial": first_value(data, "serialNumber", "serial", "id") or device.get("serial"),
        "name": first_value(data, "deviceName", "name", "hostname") or device.get("host"),
        "model": first_value(data, "model", "partNumber"),
        "mac": first_value(data, "macAddress", "mac"),
        "ipv4": data.get("ipv4"),
        "site_id": first_value(data, "siteId", "site_id") or device.get("site_id"),
        "site_name": first_value(data, "siteName", "site") or device.get("site_name"),
        "status": normalize_status(first_value(data, "status", "health")),
        "firmware": first_value(data, "firmwareVersion", "softwareVersion"),
        "uptime_in_millis": data.get("uptimeInMillis"),
        "uptime_seconds": millis_to_seconds(data.get("uptimeInMillis")),
        "cpu_utilization": first_stats.get("cpuUtilization") if kind == "ap" else data.get("cpuUtilization"),
        "memory_utilization": first_stats.get("memoryUtilization") if kind == "ap" else data.get("memoryUtilization"),
        "config_status": data.get("configStatus"),
    }


def gateway_response_for_device(config: dict[str, Any], key: str) -> tuple[int, dict[str, Any]]:
    state = load_gateway_state()
    devices = state.get("devices") if isinstance(state.get("devices"), dict) else {}
    device = devices.get(key)
    if not isinstance(device, dict):
        return 404, {"gateway": {"status": "not_found"}, "error": f"Device key {key} not found. Run sync-zabbix first."}
    ttl = int(config_section(config, "gateway").get("device_cache_ttl_seconds") or 240)
    cache_key = f"device:{key}"
    with HTTP_CACHE_LOCK:
        cached = HTTP_CACHE.get(cache_key)
        if cached and utc_now() - int(cached.get("fetched_at") or 0) < ttl:
            body = dict(cached["body"])
            body["gateway"] = dict(body.get("gateway") or {}, cache="hit")
            return 200, body
    workspace = find_workspace(config, str(device["workspace_id"]))
    tenant = gateway_tenant_record(device)
    query = {"site-id": str(device["site_id"])} if device.get("site_id") else None
    try:
        raw = central_get(config, workspace, tenant, device_path(str(device["kind"]), str(device["serial"])), query)
        body = {
            "gateway": {"status": "ok", "cache": "miss", "fetched_at": utc_now(), "fetched_at_iso": iso_now()},
            "device": {k: v for k, v in device.items() if k != "central_base_url"},
            "summary": normalize_summary(str(device["kind"]), raw, device),
            "data": raw,
        }
        with HTTP_CACHE_LOCK:
            HTTP_CACHE[cache_key] = {"fetched_at": utc_now(), "body": body}
        return 200, body
    except Exception as exc:
        with HTTP_CACHE_LOCK:
            cached = HTTP_CACHE.get(cache_key)
        if cached:
            body = dict(cached["body"])
            body["gateway"] = dict(body.get("gateway") or {}, status="stale", cache="stale", error=str(exc))
            return 200, body
        return 502, {"gateway": {"status": "error"}, "error": str(exc)}


def gateway_health(config: dict[str, Any]) -> dict[str, Any]:
    state = load_gateway_state()
    devices = state.get("devices") if isinstance(state.get("devices"), dict) else {}
    return {
        "gateway": {"status": "ok", "version": APP_VERSION, "time": iso_now()},
        "state": {"generated_at": state.get("generated_at"), "device_count": len(devices)},
        "cache": {"entries": len(HTTP_CACHE)},
        "rate_limit": {"per_second": int(config_section(config, "gateway").get("api_rate_limit_per_second") or 8)},
    }


class GatewayHandler(BaseHTTPRequestHandler):
    config: dict[str, Any] = {}

    def write_json(self, status: int, body: dict[str, Any]) -> None:
        payload = json.dumps(body, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/v1/health":
            self.write_json(200, gateway_health(self.config))
            return
        match = re.match(r"^/api/v1/device/([^/]+)/raw$", parsed.path)
        if match:
            key = unquote(match.group(1))
            status, body = gateway_response_for_device(self.config, key)
            self.write_json(status, body)
            return
        if parsed.path == "/api/v1/device/raw":
            key = parse_qs(parsed.query).get("key", [""])[0]
            status, body = gateway_response_for_device(self.config, key)
            self.write_json(status, body)
            return
        self.write_json(404, {"gateway": {"status": "not_found"}, "error": "Unknown endpoint"})

    def log_message(self, fmt: str, *args: Any) -> None:
        if config_section(self.config, "gateway").get("access_log", False):
            super().log_message(fmt, *args)


def run_gateway(config: dict[str, Any]) -> None:
    gateway = config_section(config, "gateway")
    listen = str(gateway.get("listen") or "0.0.0.0")
    port = int(gateway.get("port") or 8080)
    GatewayHandler.config = config
    server = ThreadingHTTPServer((listen, port), GatewayHandler)
    print(json.dumps({"status": "listening", "listen": listen, "port": port, "version": APP_VERSION}))
    server.serve_forever()


def run_combined(config: dict[str, Any]) -> None:
    sync_interval = int(config_section(config, "sync").get("interval_seconds") or 1800)
    thread = threading.Thread(target=run_gateway, args=(config,), daemon=True)
    thread.start()
    while True:
        try:
            result = sync_zabbix(config, apply=True)
            print(json.dumps({"sync": result}, ensure_ascii=True))
        except Exception as exc:
            print(json.dumps({"sync": {"status": "error", "error": str(exc)}}, ensure_ascii=True), file=sys.stderr)
        time.sleep(sync_interval)


def main() -> int:
    parser = argparse.ArgumentParser(description="HPE Aruba Central NG gateway/sync for Zabbix")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("config-check")
    import_parser = sub.add_parser("import-zabbix-template")
    import_parser.add_argument("--apply", action="store_true")
    sync_parser = sub.add_parser("sync-zabbix")
    sync_parser.add_argument("--apply", action="store_true")
    sub.add_parser("gateway")
    sub.add_parser("run")
    args = parser.parse_args()

    try:
        config = load_json_config()
        if args.command == "config-check":
            print(json.dumps(config_check(config), ensure_ascii=True))
            return 0
        if args.command == "import-zabbix-template":
            print(json.dumps(import_zabbix_template(config, apply=args.apply), ensure_ascii=True))
            return 0
        if args.command == "sync-zabbix":
            print(json.dumps(sync_zabbix(config, apply=args.apply), ensure_ascii=True))
            return 0
        if args.command == "gateway":
            run_gateway(config)
            return 0
        if args.command == "run":
            run_combined(config)
            return 0
    except (ConfigError, CentralError, ZabbixError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=True))
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
