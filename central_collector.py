#!/usr/bin/env python3
import argparse
import base64
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


COLLECTOR_VERSION = "0.2.1"
TEMPLATE_VERSION = "0.2.0"
GITHUB_CONTENTS_BASE_URL = "https://api.github.com/repos/nk02/zabbix-aruba-central-ng/contents"
GITHUB_REF = "main"
VERSION_CHECK_TIMEOUT_SECONDS = 5
GREENLAKE_API = "https://global.api.greenlake.hpe.com"
TOKEN_PATH = "/authorization/v2/oauth2/{workspace_id}/token"
TENANTS_PATH = "/workspaces/v1/msp-tenants"
ACTIVE_CENTRAL_BASE_URL: str | None = None
CENTRAL_BASE_URLS = [
    "https://de1.api.central.arubanetworks.com",
    "https://de2.api.central.arubanetworks.com",
    "https://de3.api.central.arubanetworks.com",
    "https://gb1.api.central.arubanetworks.com",
    "https://us1.api.central.arubanetworks.com",
    "https://us2.api.central.arubanetworks.com",
    "https://us4.api.central.arubanetworks.com",
    "https://us5.api.central.arubanetworks.com",
    "https://us6.api.central.arubanetworks.com",
    "https://ca1.api.central.arubanetworks.com",
    "https://in1.api.central.arubanetworks.com",
    "https://jp1.api.central.arubanetworks.com",
    "https://au1.api.central.arubanetworks.com",
    "https://ae1.api.central.arubanetworks.com",
]


class CentralError(RuntimeError):
    pass


class ConfigError(RuntimeError):
    pass


def load_json_config() -> dict[str, Any] | None:
    path = Path(__file__).with_name("workspaces.json")
    if not path.exists():
        raise ConfigError("Missing workspaces.json. Copy workspaces.example.json and fill in your workspaces.")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Invalid JSON config {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("workspaces.json must contain a JSON object")
    return data


def use_workspace_env(workspace: dict[str, Any]) -> None:
    global ACTIVE_CENTRAL_BASE_URL
    ACTIVE_CENTRAL_BASE_URL = str(workspace["central_base_url"]).rstrip("/")
    os.environ["HPE_MSP_WORKSPACE_ID"] = str(workspace["workspace_id"])
    os.environ["HPE_CLIENT_ID"] = str(workspace["client_id"])
    os.environ["HPE_CLIENT_SECRET"] = str(workspace["client_secret"])
    if workspace.get("tenant_allowlist") is not None:
        allowlist = workspace.get("tenant_allowlist")
        if isinstance(allowlist, list):
            os.environ["HPE_TENANT_ALLOWLIST"] = ",".join(str(item) for item in allowlist)
        else:
            os.environ["HPE_TENANT_ALLOWLIST"] = str(allowlist)
    else:
        os.environ["HPE_TENANT_ALLOWLIST"] = ""


def apply_zabbix_config(config: dict[str, Any] | None) -> None:
    if not config:
        return
    zabbix = config.get("zabbix")
    if not isinstance(zabbix, dict):
        return
    mapping = {
        "server": "ZABBIX_SERVER",
        "port": "ZABBIX_PORT",
        "host": "ZABBIX_HOST",
        "sender_path": "ZABBIX_SENDER_PATH",
    }
    for source, target in mapping.items():
        if zabbix.get(source) is not None:
            os.environ[target] = str(zabbix[source])
    collector = config.get("collector")
    if isinstance(collector, dict):
        if collector.get("interval_seconds") is not None:
            os.environ["COLLECTOR_INTERVAL_SECONDS"] = str(collector["interval_seconds"])
        if collector.get("collect_client_counts") is not None:
            os.environ["HPE_COLLECT_CLIENT_COUNTS"] = "true" if collector["collect_client_counts"] else "false"
        if collector.get("version_check_enabled") is not None:
            os.environ["CENTRAL_VERSION_CHECK_ENABLED"] = "true" if collector["version_check_enabled"] else "false"
        if collector.get("version_check_base_url") is not None:
            os.environ["CENTRAL_VERSION_CHECK_BASE_URL"] = str(collector["version_check_base_url"])
        if collector.get("version_check_ref") is not None:
            os.environ["CENTRAL_VERSION_CHECK_REF"] = str(collector["version_check_ref"])
        device_type_tags = collector.get("device_type_tags")
        if isinstance(device_type_tags, dict):
            mapping = {
                "ap": "CENTRAL_TAG_AP",
                "switch": "CENTRAL_TAG_SWITCH",
                "gateway": "CENTRAL_TAG_GATEWAY",
            }
            for source, target in mapping.items():
                if device_type_tags.get(source) is not None:
                    os.environ[target] = str(device_type_tags[source])


def config_workspaces(config: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not config:
        raise ConfigError("Missing configuration")
    workspaces = config.get("workspaces")
    if not isinstance(workspaces, list) or not workspaces:
        raise ConfigError("workspaces.json must include a non-empty workspaces array")
    normalized = []
    for index, workspace in enumerate(workspaces, start=1):
        if not isinstance(workspace, dict):
            raise ConfigError(f"Workspace #{index} must be an object")
        for key in ("workspace_id", "client_id", "client_secret", "central_base_url"):
            if not workspace.get(key):
                raise ConfigError(f"Workspace #{index} missing required field: {key}")
        mode = str(workspace.get("mode") or "msp").lower()
        if mode not in ("msp", "standalone"):
            raise ConfigError(f"Workspace #{index} has invalid mode: {mode}")
        workspace = dict(workspace)
        workspace["mode"] = mode
        workspace.setdefault("name", workspace["workspace_id"])
        normalized.append(workspace)
    return normalized


def cache_path() -> Path:
    return Path(__file__).with_name(".token_cache.json")


def read_token_cache() -> dict[str, Any]:
    path = cache_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def write_token_cache(cache: dict[str, Any]) -> None:
    try:
        cache_path().write_text(json.dumps(cache, ensure_ascii=True), encoding="utf-8")
    except OSError:
        pass


def cached_token(cache_key: str) -> str | None:
    cache = read_token_cache()
    item = cache.get(cache_key)
    if not isinstance(item, dict):
        return None
    token = item.get("access_token")
    expires_at = item.get("expires_at", 0)
    if isinstance(token, str) and isinstance(expires_at, int) and expires_at > int(time.time()) + 60:
        return token
    return None


def clear_cached_token(cache_key: str) -> None:
    cache = read_token_cache()
    if cache_key in cache:
        del cache[cache_key]
        write_token_cache(cache)


def store_cached_token(cache_key: str, token: str, expires_in: int = 900) -> None:
    cache = read_token_cache()
    cache[cache_key] = {
        "access_token": token,
        "expires_at": int(time.time()) + max(60, expires_in),
    }
    write_token_cache(cache)


def env(name: str, required: bool = True, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise CentralError(f"Missing required environment variable: {name}")
    return value or ""


def env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def parse_version(value: str) -> tuple[int, ...]:
    parts = re.findall(r"\d+", value)
    return tuple(int(part) for part in parts) if parts else (0,)


def compare_versions(current: str, latest: str) -> str:
    current_parts = parse_version(current)
    latest_parts = parse_version(latest)
    size = max(len(current_parts), len(latest_parts))
    current_parts = current_parts + (0,) * (size - len(current_parts))
    latest_parts = latest_parts + (0,) * (size - len(latest_parts))
    if current_parts < latest_parts:
        return "outdated"
    if current_parts > latest_parts:
        return "newer"
    return "current"


def request_text(url: str, timeout: int = VERSION_CHECK_TIMEOUT_SECONDS) -> str:
    req = Request(url, headers={"Accept": "text/plain"}, method="GET")
    with urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def request_remote_repo_file(base_url: str, ref: str, path: str) -> str:
    if "api.github.com" in base_url and "/contents" in base_url:
        url = f"{base_url.rstrip('/')}/{path}?{urlencode({'ref': ref})}"
        req = Request(url, headers={"Accept": "application/vnd.github+json"}, method="GET")
        with urlopen(req, timeout=VERSION_CHECK_TIMEOUT_SECONDS) as response:
            data = json.loads(response.read().decode("utf-8", errors="replace"))
        content = str(data.get("content") or "")
        encoding = str(data.get("encoding") or "")
        if encoding != "base64" or not content:
            raise CentralError(f"Unexpected GitHub contents response for {path}")
        return base64.b64decode(content).decode("utf-8", errors="replace")
    return request_text(f"{base_url.rstrip('/')}/{path}")


def extract_python_constant(text: str, name: str) -> str | None:
    match = re.search(rf'^{re.escape(name)}\s*=\s*["\']([^"\']+)["\']', text, flags=re.MULTILINE)
    return match.group(1) if match else None


def extract_template_version(text: str) -> str | None:
    match = re.search(r"^\s*value:\s*['\"]?([^'\"\s]+)['\"]?\s*#\s*TEMPLATE_VERSION\s*$", text, flags=re.MULTILINE)
    return match.group(1) if match else None


def build_version_component(name: str, current: str, latest: str | None, error: str | None = None) -> dict[str, Any]:
    status = "unknown" if error or not latest else compare_versions(current, latest)
    return {
        "name": name,
        "current": current,
        "latest": latest or "",
        "status": status,
        "up_to_date": status in ("current", "newer"),
        "error": error or "",
    }


def collect_version_status() -> dict[str, Any]:
    enabled = env_bool("CENTRAL_VERSION_CHECK_ENABLED", True)
    result: dict[str, Any] = {
        "enabled": enabled,
        "collector": build_version_component("collector", COLLECTOR_VERSION, None, "version check disabled"),
        "template": build_version_component("template", TEMPLATE_VERSION, None, "version check disabled"),
        "status": "disabled",
        "checked_at": int(time.time()),
    }
    if not enabled:
        return result

    base_url = env("CENTRAL_VERSION_CHECK_BASE_URL", required=False, default=GITHUB_CONTENTS_BASE_URL).rstrip("/")
    ref = env("CENTRAL_VERSION_CHECK_REF", required=False, default=GITHUB_REF)
    try:
        collector_text = request_remote_repo_file(base_url, ref, "central_collector.py")
        latest_collector = extract_python_constant(collector_text, "COLLECTOR_VERSION")
        result["collector"] = build_version_component(
            "collector",
            COLLECTOR_VERSION,
            latest_collector,
            None if latest_collector else "COLLECTOR_VERSION not found in remote collector",
        )
    except Exception as exc:
        result["collector"] = build_version_component("collector", COLLECTOR_VERSION, None, str(exc))

    try:
        template_text = request_remote_repo_file(base_url, ref, "zabbix_template_hpe_aruba_central_new_ap_trapper.yaml")
        latest_template = extract_template_version(template_text)
        result["template"] = build_version_component(
            "template",
            TEMPLATE_VERSION,
            latest_template,
            None if latest_template else "CENTRAL.TEMPLATE.VERSION not found in remote template",
        )
    except Exception as exc:
        result["template"] = build_version_component("template", TEMPLATE_VERSION, None, str(exc))

    components = (result["collector"], result["template"])
    if any(component["status"] == "outdated" for component in components):
        result["status"] = "outdated"
    elif any(component["status"] == "unknown" for component in components):
        result["status"] = "unknown"
    else:
        result["status"] = "current"
    return result


def request_json(
    method: str,
    url: str,
    token: str | None = None,
    form: dict[str, str] | None = None,
    query: dict[str, str | int] | None = None,
) -> dict[str, Any]:
    if query:
        url = f"{url}?{urlencode(query)}"

    data = None
    headers = {"Accept": "application/json"}
    if form is not None:
        data = urlencode(form).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=60) as response:
            body = response.read().decode("utf-8")
            return json.loads(body) if body else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise CentralError(f"HTTP {exc.code} calling {url}: {body}") from exc
    except URLError as exc:
        raise CentralError(f"Network error calling {url}: {exc.reason}") from exc


def request_status(method: str, url: str, token: str, query: dict[str, str | int] | None = None) -> dict[str, Any]:
    if query:
        url = f"{url}?{urlencode(query)}"
    req = Request(url, headers={"Accept": "application/json", "Authorization": f"Bearer {token}"}, method=method)
    try:
        with urlopen(req, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {"url": url, "status": response.status, "ok": 200 <= response.status < 300, "body_preview": body[:300]}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {"url": url, "status": exc.code, "ok": False, "body_preview": body[:300]}
    except URLError as exc:
        return {"url": url, "status": None, "ok": False, "error": str(exc.reason)}


def get_msp_token() -> str:
    workspace_id = env("HPE_MSP_WORKSPACE_ID").replace("-", "")
    client_id = env("HPE_CLIENT_ID")
    cache_key = f"msp:{workspace_id}:{client_id}"
    cached = cached_token(cache_key)
    if cached:
        return cached
    client_secret = env("HPE_CLIENT_SECRET")
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    data = request_json("POST", GREENLAKE_API + TOKEN_PATH.format(workspace_id=workspace_id), form=payload)
    token = require_token(data, "workspace")
    store_cached_token(cache_key, token, int(data.get("expires_in") or 900))
    return token


def get_workspace_token(workspace_id: str, client_id: str, client_secret: str) -> str:
    normalized_workspace_id = workspace_id.replace("-", "")
    cache_key = f"workspace:{normalized_workspace_id}:{client_id}"
    cached = cached_token(cache_key)
    if cached:
        return cached
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    data = request_json("POST", GREENLAKE_API + TOKEN_PATH.format(workspace_id=normalized_workspace_id), form=payload)
    token = require_token(data, f"workspace {workspace_id}")
    store_cached_token(cache_key, token, int(data.get("expires_in") or 900))
    return token


def exchange_tenant_token(msp_token: str, tenant_workspace_id: str, force_refresh: bool = False) -> str:
    workspace_id = env("HPE_MSP_WORKSPACE_ID").replace("-", "")
    client_id = env("HPE_CLIENT_ID")
    cache_key = f"tenant:{workspace_id}:{client_id}:{tenant_workspace_id}"
    cached = None if force_refresh else cached_token(cache_key)
    if cached:
        return cached
    workspace_id = tenant_workspace_id.replace("-", "")
    payload = {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": msp_token,
        "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
    }
    data = request_json("POST", GREENLAKE_API + TOKEN_PATH.format(workspace_id=workspace_id), form=payload)
    token = require_token(data, f"tenant {tenant_workspace_id}")
    store_cached_token(cache_key, token, int(data.get("expires_in") or 900))
    return token


def require_token(data: dict[str, Any], label: str) -> str:
    token = data.get("access_token")
    if not isinstance(token, str) or not token:
        raise CentralError(f"Token response for {label} did not include access_token")
    return token


def get_tenants_from_greenlake(msp_token: str) -> list[dict[str, Any]]:
    tenants: list[dict[str, Any]] = []
    offset = 0
    limit = 100
    while True:
        data = request_json(
            "GET",
            GREENLAKE_API + TENANTS_PATH,
            token=msp_token,
            query={"offset": offset, "limit": limit},
        )
        items = as_list(data, "items")
        tenants.extend(items)
        total = int(data.get("total") or len(tenants))
        offset += len(items)
        if not items or offset >= total:
            break
    return tenants


def filter_tenants(tenants: list[dict[str, Any]]) -> list[dict[str, Any]]:
    allowlist = [item.strip() for item in env("HPE_TENANT_ALLOWLIST", required=False).split(",") if item.strip()]
    if not allowlist:
        return tenants
    wanted = {item.lower() for item in allowlist}
    return [
        tenant
        for tenant in tenants
        if str(tenant.get("id", "")).lower() in wanted
        or str(tenant.get("workspaceName", "")).lower() in wanted
    ]


def get_workspace_tenants(msp_token: str, workspace: dict[str, Any]) -> list[dict[str, Any]]:
    if workspace.get("mode") == "standalone":
        workspace_id = str(workspace.get("workspace_id"))
        return [
            {
                "id": workspace_id,
                "workspaceName": workspace.get("name") or workspace_id,
                "_direct_token": msp_token,
                "_workspace_name": workspace.get("name") or workspace_id,
                "_workspace_id": workspace_id,
                "_workspace_mode": "standalone",
            }
        ]
    tenants = filter_tenants(get_tenants_from_greenlake(msp_token))
    for tenant in tenants:
        tenant["_workspace_name"] = workspace.get("name")
        tenant["_workspace_id"] = workspace.get("workspace_id")
        tenant["_workspace_mode"] = "msp"
    return tenants


def tenant_token(msp_token: str, tenant: dict[str, Any], force_refresh: bool = False) -> str:
    direct_token = tenant.get("_direct_token")
    if isinstance(direct_token, str) and direct_token:
        return direct_token
    return exchange_tenant_token(msp_token, str(tenant.get("id")), force_refresh=force_refresh)


def central_get(token: str, path: str, query: dict[str, str | int] | None = None) -> dict[str, Any]:
    base_url = (ACTIVE_CENTRAL_BASE_URL or env("HPE_CENTRAL_BASE_URL")).rstrip("/")
    if not path.startswith("/"):
        path = "/" + path
    return request_json("GET", base_url + path, token=token, query=query)


def tenant_central_get(
    msp_token: str,
    tenant_id: str,
    path: str,
    query: dict[str, str | int] | None = None,
) -> dict[str, Any]:
    token = exchange_tenant_token(msp_token, tenant_id)
    try:
        return central_get(token, path, query)
    except CentralError as exc:
        if "HTTP 401" not in str(exc):
            raise
        clear_cached_token(f"tenant:{tenant_id}")
        token = exchange_tenant_token(msp_token, tenant_id, force_refresh=True)
        return central_get(token, path, query)


def get_all_pages(token: str, path: str, query: dict[str, str | int] | None = None) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {"limit": 1000}
    if query:
        params.update(query)

    all_items: list[dict[str, Any]] = []
    next_cursor = params.pop("next", None)
    while True:
        page_params = dict(params)
        if next_cursor:
            page_params["next"] = next_cursor
        data = central_get(token, path, page_params)
        items = extract_items(data)
        all_items.extend(items)
        next_cursor = data.get("next")
        if not next_cursor or not items:
            break
    return all_items


def extract_items(data: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("items", "switches", "devices", "data"):
        values = data.get(key)
        if isinstance(values, list):
            return [item for item in values if isinstance(item, dict)]
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]
    return []


def as_list(data: dict[str, Any], key: str) -> list[dict[str, Any]]:
    values = data.get(key)
    if not isinstance(values, list):
        return []
    return [item for item in values if isinstance(item, dict)]


def tenant_name(tenant: dict[str, Any]) -> str:
    return str(tenant.get("workspaceName") or tenant.get("name") or tenant.get("id") or "")


def get_switches_for_tenant(msp_token: str, tenant: dict[str, Any]) -> list[dict[str, Any]]:
    tenant_id = str(tenant.get("id") or "")
    if not tenant_id:
        return []
    token = tenant_token(msp_token, tenant)
    try:
        switches = get_all_pages(token, "/network-monitoring/v1/switches")
    except CentralError as exc:
        if "HTTP 401" not in str(exc):
            raise
        clear_cached_token(f"tenant:{tenant_id}")
        token = tenant_token(msp_token, tenant, force_refresh=True)
        switches = get_all_pages(token, "/network-monitoring/v1/switches")
    for switch in switches:
        switch["_tenant_id"] = tenant_id
        switch["_tenant_name"] = tenant_name(tenant)
        switch["_workspace_id"] = tenant.get("_workspace_id")
        switch["_workspace_name"] = tenant.get("_workspace_name")
    return switches


def get_devices_for_tenant(
    msp_token: str,
    tenant: dict[str, Any],
    device_type: str | None = None,
) -> list[dict[str, Any]]:
    tenant_id = str(tenant.get("id") or "")
    if not tenant_id:
        return []
    query: dict[str, str | int] = {}
    if device_type:
        query["filter"] = f"deviceType eq '{device_type}'"
    token = tenant_token(msp_token, tenant)
    try:
        devices = get_all_pages(token, "/network-monitoring/v1/devices", query)
    except CentralError as exc:
        if "HTTP 401" not in str(exc):
            raise
        clear_cached_token(f"tenant:{tenant_id}")
        token = tenant_token(msp_token, tenant, force_refresh=True)
        devices = get_all_pages(token, "/network-monitoring/v1/devices", query)
    for device in devices:
        device["_tenant_id"] = tenant_id
        device["_tenant_name"] = tenant_name(tenant)
        device["_workspace_id"] = tenant.get("_workspace_id")
        device["_workspace_name"] = tenant.get("_workspace_name")
    return devices


def get_ap_detail_for_tenant(msp_token: str, tenant: dict[str, Any], serial: str, site_id: str | None = None) -> dict[str, Any]:
    tenant_id = str(tenant.get("id") or "")
    query = {"site-id": site_id} if site_id else None
    token = tenant_token(msp_token, tenant)
    try:
        detail = central_get(token, f"/network-monitoring/v1/aps/{serial}", query)
    except CentralError as exc:
        if "HTTP 401" not in str(exc):
            raise
        token = tenant_token(msp_token, tenant, force_refresh=True)
        detail = central_get(token, f"/network-monitoring/v1/aps/{serial}", query)
    detail["_tenant_id"] = tenant_id
    if env_bool("HPE_COLLECT_CLIENT_COUNTS", default=True):
        detail["_clients_connected"] = get_connected_clients_count(token, serial, site_id)
    return detail


def get_switch_detail_for_tenant(msp_token: str, tenant: dict[str, Any], serial: str, site_id: str | None = None) -> dict[str, Any]:
    tenant_id = str(tenant.get("id") or "")
    query = {"site-id": site_id} if site_id else None
    token = tenant_token(msp_token, tenant)
    try:
        detail = central_get(token, f"/network-monitoring/v1/switches/{serial}", query)
    except CentralError as exc:
        if "HTTP 401" not in str(exc):
            raise
        token = tenant_token(msp_token, tenant, force_refresh=True)
        detail = central_get(token, f"/network-monitoring/v1/switches/{serial}", query)
    detail["_tenant_id"] = tenant_id
    return detail


def get_switch_interfaces_for_tenant(msp_token: str, tenant: dict[str, Any], serial: str, site_id: str | None = None) -> list[dict[str, Any]]:
    tenant_id = str(tenant.get("id") or "")
    token = tenant_token(msp_token, tenant)
    query: dict[str, str | int] = {"limit": 1000}
    if site_id:
        query["site-id"] = site_id
    try:
        interfaces = get_all_pages(token, f"/network-monitoring/v1/switches/{serial}/interfaces", query)
    except CentralError as exc:
        if "HTTP 401" not in str(exc):
            raise
        clear_cached_token(f"tenant:{tenant_id}")
        token = tenant_token(msp_token, tenant, force_refresh=True)
        interfaces = get_all_pages(token, f"/network-monitoring/v1/switches/{serial}/interfaces", query)
    for interface in interfaces:
        interface["_tenant_id"] = tenant_id
        interface["_switch_serial"] = serial
    return interfaces


def get_connected_clients_count(token: str, serial: str, site_id: str | None = None) -> int | None:
    query: dict[str, str | int] = {
        "serial-number": serial,
        "filter": "clientConnectionType eq 'Wireless' and status eq 'Connected'",
        "limit": 1,
    }
    if site_id:
        query["site-id"] = site_id
    data = central_get(token, "/network-monitoring/v1/clients", query)
    total = data.get("total")
    if isinstance(total, int):
        return total
    count = data.get("count")
    if isinstance(count, int):
        return count
    items = extract_items(data)
    return len(items)


def normalize_switch(switch: dict[str, Any]) -> dict[str, Any]:
    return {
        "workspace_id": switch.get("_workspace_id"),
        "workspace_name": switch.get("_workspace_name"),
        "tenant_id": switch.get("_tenant_id"),
        "tenant_name": switch.get("_tenant_name"),
        "serial": first_value(switch, "serialNumber", "serial", "id"),
        "name": first_value(switch, "deviceName", "name", "hostname"),
        "model": first_value(switch, "model", "partNumber"),
        "mac": first_value(switch, "macAddress", "mac"),
        "site_id": first_value(switch, "siteId", "site_id"),
        "site_name": first_value(switch, "siteName", "site"),
        "status": first_value(switch, "status", "health"),
        "firmware": first_value(switch, "firmwareVersion", "softwareVersion"),
        "deployment": switch.get("deployment"),
    }


def normalize_device(device: dict[str, Any]) -> dict[str, Any]:
    return {
        "workspace_id": device.get("_workspace_id"),
        "workspace_name": device.get("_workspace_name"),
        "tenant_id": device.get("_tenant_id"),
        "tenant_name": device.get("_tenant_name"),
        "serial": first_value(device, "serialNumber", "serial", "id"),
        "name": first_value(device, "deviceName", "name", "hostname"),
        "model": first_value(device, "model", "partNumber"),
        "mac": first_value(device, "macAddress", "mac"),
        "ipv4": device.get("ipv4"),
        "site_id": first_value(device, "siteId", "site_id"),
        "site_name": first_value(device, "siteName", "site"),
        "status": first_value(device, "status", "health"),
        "firmware": first_value(device, "firmwareVersion", "softwareVersion"),
        "device_type": device.get("deviceType"),
        "device_function": device.get("deviceFunction"),
        "deployment": device.get("deployment"),
    }


def normalize_ap_detail(detail: dict[str, Any]) -> dict[str, Any]:
    ap = detail.get("ap") if isinstance(detail.get("ap"), dict) else detail
    stats = ap.get("apStats")
    first_stats = stats[0] if isinstance(stats, list) and stats and isinstance(stats[0], dict) else {}
    return {
        "tenant_id": detail.get("_tenant_id"),
        "serial": first_value(ap, "serialNumber", "serial", "id"),
        "name": first_value(ap, "deviceName", "name", "hostname"),
        "model": first_value(ap, "model", "partNumber"),
        "mac": first_value(ap, "macAddress", "mac"),
        "ipv4": ap.get("ipv4"),
        "public_ipv4": ap.get("publicIpv4"),
        "default_gateway": ap.get("defaultGateway"),
        "site_id": first_value(ap, "siteId", "site_id"),
        "site_name": first_value(ap, "siteName", "site"),
        "status": first_value(ap, "status", "health"),
        "firmware": first_value(ap, "firmwareVersion", "softwareVersion"),
        "uptime_in_millis": ap.get("uptimeInMillis"),
        "cpu_utilization": first_stats.get("cpuUtilization"),
        "memory_utilization": first_stats.get("memoryUtilization"),
        "clients_connected": detail.get("_clients_connected"),
        "negotiated_power": ap.get("negotiatedPower"),
        "last_reboot_reason": ap.get("lastRebootReason"),
        "last_seen_at": ap.get("lastSeenAt"),
        "radios": normalize_radios(ap.get("radios")),
        "ports": ap.get("ports"),
        "wlans": ap.get("wlans"),
    }


def normalize_switch_detail(detail: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenant_id": detail.get("_tenant_id"),
        "serial": first_value(detail, "serialNumber", "serial", "id"),
        "name": first_value(detail, "deviceName", "name", "hostname"),
        "model": first_value(detail, "model", "partNumber"),
        "mac": first_value(detail, "macAddress", "mac"),
        "ipv4": detail.get("ipv4"),
        "public_ip": detail.get("publicIp"),
        "site_id": first_value(detail, "siteId", "site_id"),
        "site_name": first_value(detail, "siteName", "site"),
        "status": normalize_status(first_value(detail, "status", "health")),
        "health": detail.get("health"),
        "firmware": first_value(detail, "firmwareVersion", "softwareVersion"),
        "uptime_in_millis": detail.get("uptimeInMillis"),
        "config_status": detail.get("configStatus"),
        "last_restart_reason": detail.get("lastRestartReason"),
        "switch_role": detail.get("switchRole"),
        "switch_type": detail.get("switchType"),
        "deployment": detail.get("deployment"),
    }


def normalize_status(value: Any) -> Any:
    if isinstance(value, str):
        return value.upper()
    return value


def normalize_switch_interface(interface: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenant_id": interface.get("_tenant_id"),
        "switch_serial": interface.get("_switch_serial") or interface.get("serialNumber"),
        "port_index": interface.get("portIndex") or interface.get("index"),
        "name": first_value(interface, "name", "id"),
        "id": interface.get("id"),
        "status": interface.get("status"),
        "admin_status": interface.get("adminStatus"),
        "oper_status": interface.get("operStatus"),
        "speed_bps": interface.get("speed"),
        "duplex": interface.get("duplex"),
        "connector": interface.get("connector"),
        "vlan_mode": interface.get("vlanMode"),
        "native_vlan": interface.get("nativeVlan"),
        "poe_status": interface.get("poeStatus"),
        "transceiver_status": interface.get("transceiverStatus"),
        "transceiver_model": interface.get("transceiverModel"),
        "stp_port_state": interface.get("stpPortState"),
        "stp_port_role": interface.get("stpPortRole"),
        "uplink": interface.get("uplink"),
    }


def normalize_radios(radios: Any) -> list[dict[str, Any]]:
    if not isinstance(radios, list):
        return []
    normalized = []
    for radio in radios:
        if not isinstance(radio, dict):
            continue
        stats = radio.get("radioStats")
        first_stats = stats[0] if isinstance(stats, list) and stats and isinstance(stats[0], dict) else {}
        normalized.append(
            {
                "radio_number": radio.get("radioNumber"),
                "band": radio.get("band"),
                "status": radio.get("status"),
                "channel": radio.get("channel"),
                "channel_utilization": to_float(first_stats.get("channelUtilization")),
                "noise_floor": to_float(first_stats.get("noiseFloor")),
                "power": radio.get("power"),
                "power_dbm": parse_dbm(radio.get("power")),
                "mode": radio.get("mode"),
                "bandwidth": radio.get("bandwidth"),
                "antenna": radio.get("antenna"),
                "spatial_stream": radio.get("spatialStream"),
                "mac": radio.get("macAddress"),
            }
        )
    return normalized


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_dbm(value: Any) -> float | None:
    if not isinstance(value, str):
        return to_float(value)
    return to_float(value.replace("dBm", "").strip())


def first_value(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return None


def output_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=True, separators=(",", ":")))


def sender_escape(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def sender_line(host: str, key: str, value: Any) -> str:
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=True, separators=(",", ":"))
    return f"{sender_escape(host)} {key} {sender_escape(value)}"


def sender_unescape(value: str) -> str:
    result: list[str] = []
    escaped = False
    for char in value:
        if escaped:
            result.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        result.append(char)
    if escaped:
        result.append("\\")
    return "".join(result)


def parse_sender_line(line: str) -> tuple[str, str, str]:
    if not line.startswith('"'):
        raise ValueError(f"Invalid sender line: {line}")
    host_end = line.find('" ', 1)
    if host_end < 0:
        raise ValueError(f"Invalid sender line: {line}")
    key_start = host_end + 2
    key_end = line.find(" ", key_start)
    if key_end < 0 or key_end + 1 >= len(line) or line[key_end + 1] != '"':
        raise ValueError(f"Invalid sender line: {line}")
    return (
        sender_unescape(line[1:host_end]),
        line[key_start:key_end],
        sender_unescape(line[key_end + 2 : -1]),
    )


def build_ap_sender_lines(msp_token: str, tenants: list[dict[str, Any]], zabbix_host: str) -> list[str]:
    started = time.time()
    aps: list[dict[str, Any]] = []
    tenant_by_id = {str(tenant.get("id") or ""): tenant for tenant in tenants}
    radios: list[dict[str, Any]] = []
    lines: list[str] = []
    for tenant in tenants:
        aps.extend(normalize_device(device) for device in get_devices_for_tenant(msp_token, tenant, "ACCESS_POINT"))

    lines.append(sender_line(zabbix_host, "central.aps.discovery", devices_lld(aps)))

    for ap in aps:
        tenant_id = str(ap.get("tenant_id") or "")
        serial = str(ap.get("serial") or "")
        site_id = str(ap.get("site_id") or "")
        if not tenant_id or not serial:
            continue
        detail = normalize_ap_detail(get_ap_detail_for_tenant(msp_token, tenant_by_id[tenant_id], serial, site_id or None))
        detail["workspace_name"] = ap.get("workspace_name")
        detail["workspace_id"] = ap.get("workspace_id")
        lines.append(sender_line(zabbix_host, f"central.ap.raw[{tenant_id},{serial}]", detail))
        for radio in detail.get("radios") or []:
            if not isinstance(radio, dict):
                continue
            radio_number = radio.get("radio_number")
            if radio_number is None:
                continue
            radio_record = dict(radio)
            radio_record["tenant_id"] = tenant_id
            radio_record["tenant_name"] = ap.get("tenant_name")
            radio_record["workspace_name"] = ap.get("workspace_name")
            radio_record["workspace_id"] = ap.get("workspace_id")
            radio_record["ap_serial"] = serial
            radio_record["ap_name"] = ap.get("name")
            radio_record["ap_site_id"] = site_id
            radio_record["ap_site_name"] = ap.get("site_name")
            radios.append(radio_record)
            lines.append(sender_line(zabbix_host, f"central.ap.radio.raw[{tenant_id},{serial},{radio_number}]", radio_record))
    lines.insert(1, sender_line(zabbix_host, "central.ap.radios.discovery", radios_lld(radios)))
    health = {
        "status": "ok",
        "timestamp": int(time.time()),
        "aps_count": len(aps),
        "radios_count": len(radios),
        "sent_lines": len(lines) + 1,
        "elapsed_seconds": round(time.time() - started, 3),
    }
    lines.insert(0, sender_line(zabbix_host, "central.collector.health", health))
    return lines


def build_switch_sender_lines(msp_token: str, tenants: list[dict[str, Any]], zabbix_host: str) -> list[str]:
    switches: list[dict[str, Any]] = []
    tenant_by_id = {str(tenant.get("id") or ""): tenant for tenant in tenants}
    interfaces: list[dict[str, Any]] = []
    lines: list[str] = []

    for tenant in tenants:
        switches.extend(normalize_switch(switch) for switch in get_switches_for_tenant(msp_token, tenant))

    lines.append(sender_line(zabbix_host, "central.switches.discovery", switches_lld(switches)))

    for switch in switches:
        tenant_id = str(switch.get("tenant_id") or "")
        serial = str(switch.get("serial") or "")
        site_id = str(switch.get("site_id") or "")
        if not tenant_id or not serial:
            continue
        detail = normalize_switch_detail(get_switch_detail_for_tenant(msp_token, tenant_by_id[tenant_id], serial, site_id or None))
        detail["workspace_name"] = switch.get("workspace_name")
        detail["workspace_id"] = switch.get("workspace_id")
        lines.append(sender_line(zabbix_host, f"central.switch.raw[{tenant_id},{serial}]", detail))
        for interface in get_switch_interfaces_for_tenant(msp_token, tenant_by_id[tenant_id], serial, site_id or None):
            normalized = normalize_switch_interface(interface)
            normalized["tenant_name"] = switch.get("tenant_name")
            normalized["switch_name"] = switch.get("name")
            normalized["workspace_name"] = switch.get("workspace_name")
            normalized["workspace_id"] = switch.get("workspace_id")
            normalized["site_id"] = switch.get("site_id")
            normalized["site_name"] = switch.get("site_name")
            port_index = normalized.get("port_index")
            if port_index is None:
                continue
            interfaces.append(normalized)
            lines.append(sender_line(zabbix_host, f"central.switch.interface.raw[{tenant_id},{serial},{port_index}]", normalized))

    lines.insert(1, sender_line(zabbix_host, "central.switch.interfaces.discovery", switch_interfaces_lld(interfaces)))
    return lines


def build_all_sender_lines(msp_token: str, tenants: list[dict[str, Any]], zabbix_host: str) -> list[str]:
    started = time.time()
    ap_lines = build_ap_sender_lines(msp_token, tenants, zabbix_host)
    switch_lines = build_switch_sender_lines(msp_token, tenants, zabbix_host)
    inventory = collect_device_inventory_summary(msp_token, tenants)
    health = {
        "status": "ok",
        "timestamp": int(time.time()),
        "tenants_count": len(tenants),
        "device_counts_by_type": inventory["device_counts_by_type"],
        "device_counts_by_tenant": inventory["device_counts_by_tenant"],
        "devices_total": inventory["devices_total"],
        "sent_lines": len(ap_lines) + len(switch_lines),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    return [sender_line(zabbix_host, "central.collector.health", health)] + [
        line for line in ap_lines + switch_lines if " central.collector.health " not in line
    ]


def build_all_config_sender_lines(config: dict[str, Any] | None) -> list[str]:
    health, all_lines = collect_all_config_payload(config)
    zabbix_host = env("ZABBIX_HOST", required=False, default="HPE Aruba Central")
    return [sender_line(zabbix_host, "central.collector.health", health)] + all_lines


def collect_all_config_payload(config: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    apply_zabbix_config(config)
    zabbix_host = env("ZABBIX_HOST", required=False, default="HPE Aruba Central")
    started = time.time()
    all_lines: list[str] = []
    discovery_data: dict[str, list[dict[str, Any]]] = {
        "central.aps.discovery": [],
        "central.ap.radios.discovery": [],
        "central.switches.discovery": [],
        "central.switch.interfaces.discovery": [],
    }
    workspace_summaries: list[dict[str, Any]] = []
    workspace_count = 0
    tenant_count = 0
    devices_total = 0
    device_counts_by_type: dict[str, int] = {}

    for workspace in config_workspaces(config):
        workspace_count += 1
        workspace_started = time.time()
        use_workspace_env(workspace)
        summary: dict[str, Any] = {
            "workspace_name": workspace.get("name"),
            "workspace_id": workspace.get("workspace_id"),
            "mode": workspace.get("mode"),
            "status": "ok",
        }
        try:
            msp_token = get_msp_token()
            tenants = get_workspace_tenants(msp_token, workspace)
            tenant_count += len(tenants)
            workspace_lines = build_all_sender_lines(msp_token, tenants, zabbix_host)
            for line in workspace_lines:
                _host, key, value = parse_sender_line(line)
                if key == "central.collector.health":
                    continue
                if key in discovery_data:
                    payload = json.loads(value)
                    if isinstance(payload, dict) and isinstance(payload.get("data"), list):
                        discovery_data[key].extend(item for item in payload["data"] if isinstance(item, dict))
                    continue
                all_lines.append(line)
            inventory = collect_device_inventory_summary(msp_token, tenants)
            summary.update(
                {
                    "tenant_count": len(tenants),
                    "devices_total": inventory["devices_total"],
                    "device_counts_by_type": inventory["device_counts_by_type"],
                    "device_counts_by_tenant": inventory["device_counts_by_tenant"],
                    "elapsed_seconds": round(time.time() - workspace_started, 3),
                }
            )
            devices_total += inventory["devices_total"]
            for device_type, count in inventory["device_counts_by_type"].items():
                device_counts_by_type[device_type] = device_counts_by_type.get(device_type, 0) + count
        except Exception as exc:
            summary.update(
                {
                    "status": "error",
                    "error": str(exc),
                    "tenant_count": 0,
                    "devices_total": 0,
                    "device_counts_by_type": {},
                    "device_counts_by_tenant": [],
                    "elapsed_seconds": round(time.time() - workspace_started, 3),
                }
            )
        workspace_summaries.append(summary)

    discovery_lines = [
        sender_line(zabbix_host, key, {"data": data})
        for key, data in discovery_data.items()
    ]
    all_lines = discovery_lines + all_lines

    health = {
        "status": "ok" if all(item.get("status") == "ok" for item in workspace_summaries) else "degraded",
        "timestamp": int(time.time()),
        "collector_version": COLLECTOR_VERSION,
        "template_version": TEMPLATE_VERSION,
        "version_status": collect_version_status(),
        "workspace_count": workspace_count,
        "tenants_count": tenant_count,
        "devices_total": devices_total,
        "device_counts_by_type": device_counts_by_type,
        "device_counts_by_workspace": workspace_summaries,
        "sent_lines": len(all_lines) + 1,
        "elapsed_seconds": round(time.time() - started, 3),
    }
    return health, all_lines


def collect_device_inventory_summary(msp_token: str, tenants: list[dict[str, Any]]) -> dict[str, Any]:
    totals_by_type: dict[str, int] = {}
    by_tenant: list[dict[str, Any]] = []
    devices_total = 0
    for tenant in tenants:
        tenant_id = str(tenant.get("id") or "")
        if not tenant_id:
            continue
        try:
            devices = get_devices_for_tenant(msp_token, tenant)
        except CentralError as exc:
            by_tenant.append(
                {
                    "tenant_id": tenant_id,
                    "tenant_name": tenant_name(tenant),
                    "error": str(exc),
                    "devices_total": 0,
                    "device_counts_by_type": {},
                }
            )
            continue
        tenant_counts: dict[str, int] = {}
        for device in devices:
            device_type = str(device.get("deviceType") or "UNKNOWN")
            tenant_counts[device_type] = tenant_counts.get(device_type, 0) + 1
            totals_by_type[device_type] = totals_by_type.get(device_type, 0) + 1
            devices_total += 1
        by_tenant.append(
            {
                "tenant_id": tenant_id,
                "tenant_name": tenant_name(tenant),
                "devices_total": len(devices),
                "device_counts_by_type": tenant_counts,
            }
        )
    return {
        "devices_total": devices_total,
        "device_counts_by_type": totals_by_type,
        "device_counts_by_tenant": by_tenant,
    }


def run_zabbix_sender(lines: list[str]) -> dict[str, Any]:
    server = env("ZABBIX_SERVER")
    port = env("ZABBIX_PORT", required=False, default="10051")
    sender = env("ZABBIX_SENDER_PATH", required=False, default="zabbix_sender")

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".zbx") as fp:
        fp.write("\n".join(lines))
        fp.write("\n")
        input_path = fp.name

    command = [sender, "-z", server, "-p", port, "-i", input_path]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, timeout=120, check=False)
        return {
            "command": command,
            "input_file": input_path,
            "returncode": completed.returncode,
            "stdout": completed.stdout.strip(),
            "stderr": completed.stderr.strip(),
            "sent_lines": len(lines),
        }
    finally:
        try:
            Path(input_path).unlink()
        except OSError:
            pass


def log_line(message: str) -> None:
    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}", flush=True)


def run_daemon(command: str, interval_seconds: int) -> None:
    if interval_seconds < 60:
        raise CentralError("Daemon interval must be at least 60 seconds")
    log_line(f"starting daemon command={command} interval={interval_seconds}s")
    while True:
        started = time.time()
        try:
            config = load_json_config()
            if command == "push-all":
                lines = build_all_config_sender_lines(config)
            else:
                apply_zabbix_config(config)
                workspaces = config_workspaces(config)
                use_workspace_env(workspaces[0])
                zabbix_host = env("ZABBIX_HOST", required=False, default="HPE Aruba Central")
                msp_token = get_msp_token()
                tenants = get_workspace_tenants(msp_token, workspaces[0])
                if command == "push-aps":
                    lines = build_ap_sender_lines(msp_token, tenants, zabbix_host)
                elif command == "push-switches":
                    lines = build_switch_sender_lines(msp_token, tenants, zabbix_host)
                elif command == "push-all":
                    lines = build_all_sender_lines(msp_token, tenants, zabbix_host)
                else:
                    raise CentralError(f"Unsupported daemon command: {command}")
            result = run_zabbix_sender(lines)
            level = "ok" if result.get("returncode") == 0 else "error"
            log_line(
                f"{level} command={command} sent_lines={result.get('sent_lines')} "
                f"returncode={result.get('returncode')} elapsed={round(time.time() - started, 3)}s "
                f"stdout={result.get('stdout')!r} stderr={result.get('stderr')!r}"
            )
        except Exception as exc:
            log_line(f"error command={command} elapsed={round(time.time() - started, 3)}s error={exc}")
        sleep_for = max(1, interval_seconds - int(time.time() - started))
        time.sleep(sleep_for)


def decode_jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload = parts[1]
    payload += "=" * (-len(payload) % 4)
    try:
        return json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}


def safe_claims(token: str) -> dict[str, Any]:
    claims = decode_jwt_claims(token)
    keep = ("iss", "aud", "azp", "client_id", "scope", "scp", "exp", "iat", "nbf", "tid", "wid")
    safe = {key: claims.get(key) for key in keep if key in claims}
    now = int(time.time())
    if isinstance(safe.get("exp"), int):
        safe["expires_in_seconds"] = safe["exp"] - now
    return safe


def tenants_lld(tenants: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": tenant.get("id"),
                "{#TENANT_NAME}": tenant_name(tenant),
                "{#WORKSPACE_NAME}": tenant.get("_workspace_name") or tenant_name(tenant),
            }
            for tenant in tenants
        ]
    }


def device_type_tag(device_type: str) -> str:
    normalized = device_type.upper()
    if normalized == "ACCESS_POINT":
        return env("CENTRAL_TAG_AP", required=False, default="ap")
    if normalized == "SWITCH":
        return env("CENTRAL_TAG_SWITCH", required=False, default="switch")
    if normalized == "GATEWAY":
        return env("CENTRAL_TAG_GATEWAY", required=False, default="gateway")
    return device_type.lower()


def switches_lld(switches: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": switch.get("tenant_id"),
                "{#TENANT_NAME}": switch.get("tenant_name"),
                "{#WORKSPACE_NAME}": switch.get("workspace_name") or switch.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag("SWITCH"),
                "{#SWITCH_SERIAL}": switch.get("serial"),
                "{#SWITCH_NAME}": switch.get("name"),
                "{#SWITCH_MODEL}": switch.get("model"),
                "{#SITE_ID}": switch.get("site_id"),
                "{#SITE_NAME}": switch.get("site_name"),
            }
            for switch in switches
            if switch.get("serial")
        ]
    }


def switch_interfaces_lld(interfaces: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": interface.get("tenant_id"),
                "{#TENANT_NAME}": interface.get("tenant_name"),
                "{#WORKSPACE_NAME}": interface.get("workspace_name") or interface.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag("SWITCH"),
                "{#SWITCH_SERIAL}": interface.get("switch_serial"),
                "{#SWITCH_NAME}": interface.get("switch_name"),
                "{#PORT_INDEX}": interface.get("port_index"),
                "{#PORT_NAME}": interface.get("name"),
                "{#PORT_CONNECTOR}": interface.get("connector"),
                "{#SITE_ID}": interface.get("site_id"),
                "{#SITE_NAME}": interface.get("site_name"),
            }
            for interface in interfaces
            if interface.get("tenant_id")
            and interface.get("switch_serial")
            and interface.get("port_index") is not None
        ]
    }


def devices_lld(devices: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": device.get("tenant_id"),
                "{#TENANT_NAME}": device.get("tenant_name"),
                "{#WORKSPACE_NAME}": device.get("workspace_name") or device.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag(str(device.get("device_type") or "")),
                "{#DEVICE_SERIAL}": device.get("serial"),
                "{#DEVICE_NAME}": device.get("name"),
                "{#DEVICE_MODEL}": device.get("model"),
                "{#DEVICE_TYPE}": device.get("device_type"),
                "{#SITE_ID}": device.get("site_id"),
                "{#SITE_NAME}": device.get("site_name"),
            }
            for device in devices
            if device.get("serial")
        ]
    }


def radios_lld(radios: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": radio.get("tenant_id"),
                "{#TENANT_NAME}": radio.get("tenant_name"),
                "{#WORKSPACE_NAME}": radio.get("workspace_name") or radio.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag("ACCESS_POINT"),
                "{#DEVICE_SERIAL}": radio.get("ap_serial"),
                "{#DEVICE_NAME}": radio.get("ap_name"),
                "{#RADIO_NUMBER}": radio.get("radio_number"),
                "{#RADIO_BAND}": radio.get("band"),
                "{#SITE_ID}": radio.get("ap_site_id"),
                "{#SITE_NAME}": radio.get("ap_site_name"),
            }
            for radio in radios
            if radio.get("tenant_id") and radio.get("ap_serial") and radio.get("radio_number") is not None
        ]
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HPE Aruba Central Next Gen multi-workspace collector for Zabbix")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("config-check", help="Validate workspaces.json without sending data")
    sub.add_parser("auth-check", help="Validate token creation for every workspace without sending data to Zabbix")
    sub.add_parser("summary", help="Print workspace, tenant, and device summary")
    push_all_parser = sub.add_parser("push-all", help="Push AP, radio, switch, and interface data to Zabbix trapper items")
    push_all_parser.add_argument("--dry-run", action="store_true", help="Print zabbix_sender input instead of sending")
    daemon_parser = sub.add_parser("daemon", help="Run push command forever at a fixed interval")
    daemon_parser.add_argument("--push-command", choices=("push-all",), default="push-all")
    daemon_parser.add_argument("--interval", type=int, default=None, help="Interval in seconds; defaults to COLLECTOR_INTERVAL_SECONDS")
    return parser


def parse_query(values: list[str]) -> dict[str, str]:
    query = {}
    for value in values:
        if "=" not in value:
            raise CentralError(f"Invalid --query value {value!r}; expected key=value")
        key, val = value.split("=", 1)
        query[key] = val
    return query


def main() -> int:
    config = load_json_config()
    apply_zabbix_config(config)
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "daemon":
        interval = args.interval or int(env("COLLECTOR_INTERVAL_SECONDS", required=False, default="300"))
        run_daemon(args.push_command, interval)
        return 0

    if args.command == "config-check":
        workspaces = config_workspaces(config)
        output_json(
            {
                "status": "ok",
                "workspace_count": len(workspaces),
                "workspaces": [
                    {
                        "name": workspace.get("name"),
                        "mode": workspace.get("mode"),
                        "workspace_id": workspace.get("workspace_id"),
                        "central_base_url": workspace.get("central_base_url"),
                    }
                    for workspace in workspaces
                ],
            }
        )
        return 0

    if args.command == "auth-check":
        results = []
        for workspace in config_workspaces(config):
            use_workspace_env(workspace)
            try:
                token = get_workspace_token(
                    str(workspace["workspace_id"]),
                    str(workspace["client_id"]),
                    str(workspace["client_secret"]),
                )
                results.append(
                    {
                        "name": workspace.get("name"),
                        "mode": workspace.get("mode"),
                        "workspace_id": workspace.get("workspace_id"),
                        "status": "ok",
                        "claims": safe_claims(token),
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "name": workspace.get("name"),
                        "mode": workspace.get("mode"),
                        "workspace_id": workspace.get("workspace_id"),
                        "status": "error",
                        "error": str(exc),
                    }
                )
        output_json({"workspaces": results})
        return 0

    if args.command == "summary":
        health, _ = collect_all_config_payload(config)
        output_json(health)
        return 0

    if args.command == "push-all":
        lines = build_all_config_sender_lines(config)
        if args.dry_run:
            print("\n".join(lines))
        else:
            output_json(run_zabbix_sender(lines))
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CentralError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=True), file=sys.stderr)
        raise SystemExit(1)
