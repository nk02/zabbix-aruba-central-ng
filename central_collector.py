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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


COLLECTOR_VERSION = "1.0.1"
TEMPLATE_VERSION = "1.0.1"
CONFIG_SCHEMA_VERSION = "1.0.1"
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
RECOMMENDED_CONFIG_PATHS = (
    "config_version",
    "collector.interval_seconds",
    "collector.collect_client_counts",
    "collector.version_check_enabled",
    "collector.version_check_base_url",
    "collector.version_check_ref",
    "zabbix.host_tags.ap",
    "zabbix.host_tags.switch",
    "zabbix.host_tags.gateway",
)


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
        "collector_host": "ZABBIX_COLLECTOR_HOST",
        "api_url": "ZABBIX_API_URL",
        "api_token": "ZABBIX_API_TOKEN",
        "unmapped_host_group": "ZABBIX_UNMAPPED_HOST_GROUP",
        "host_name_prefix": "ZABBIX_HOST_NAME_PREFIX",
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
        if collector.get("lld_settle_seconds") is not None:
            os.environ["CENTRAL_LLD_SETTLE_SECONDS"] = str(collector["lld_settle_seconds"])
        if collector.get("version_check_enabled") is not None:
            os.environ["CENTRAL_VERSION_CHECK_ENABLED"] = "true" if collector["version_check_enabled"] else "false"
        if collector.get("version_check_base_url") is not None:
            os.environ["CENTRAL_VERSION_CHECK_BASE_URL"] = str(collector["version_check_base_url"])
        if collector.get("version_check_ref") is not None:
            os.environ["CENTRAL_VERSION_CHECK_REF"] = str(collector["version_check_ref"])


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


def config_path_exists(config: dict[str, Any], path: str) -> bool:
    value: Any = config
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            return False
        value = value[part]
    return True


def collect_config_status(config: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(config, dict):
        return {
            "status": "missing",
            "current": "",
            "expected": CONFIG_SCHEMA_VERSION,
            "missing_recommended_paths": list(RECOMMENDED_CONFIG_PATHS),
        }
    current = str(config.get("config_version") or "")
    if not current:
        version_status = "missing"
    else:
        version_status = compare_versions(current, CONFIG_SCHEMA_VERSION)
    missing_paths = [path for path in RECOMMENDED_CONFIG_PATHS if not config_path_exists(config, path)]
    status = "current" if version_status in ("current", "newer") and not missing_paths else "outdated"
    return {
        "status": status,
        "current": current,
        "expected": CONFIG_SCHEMA_VERSION,
        "version_status": version_status,
        "missing_recommended_paths": missing_paths,
    }


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
    if match:
        return match.group(1)
    match = re.search(r"^\s*value:\s*['\"]([^'\"\s]+)\s+#\s*TEMPLATE_VERSION['\"]\s*$", text, flags=re.MULTILINE)
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


def zabbix_api_call(method: str, params: dict[str, Any] | list[Any] | None = None) -> Any:
    api_url = env("ZABBIX_API_URL")
    api_token = env("ZABBIX_API_TOKEN")
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params or {},
        "id": 1,
    }
    data = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json-rpc",
        "Authorization": f"Bearer {api_token}",
    }
    req = Request(api_url, data=data, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=60) as response:
            body = response.read().decode("utf-8")
            result = json.loads(body) if body else {}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise CentralError(f"HTTP {exc.code} calling Zabbix API {method}: {body}") from exc
    except URLError as exc:
        raise CentralError(f"Network error calling Zabbix API {method}: {exc.reason}") from exc
    if isinstance(result, dict) and result.get("error"):
        raise CentralError(f"Zabbix API {method} failed: {result['error']}")
    return result.get("result") if isinstance(result, dict) else result


def zabbix_managed_tag_config(config: dict[str, Any]) -> dict[str, str]:
    zabbix = config.get("zabbix") if isinstance(config.get("zabbix"), dict) else {}
    tag = zabbix.get("managed_tag") if isinstance(zabbix.get("managed_tag"), dict) else {}
    return {
        "tag": str(tag.get("tag") or "hpe-aruba-central-ng"),
        "value": str(tag.get("value") or ""),
    }


def zabbix_has_tag(tags: Any, wanted: dict[str, str]) -> bool:
    if not isinstance(tags, list):
        return False
    return any(
        isinstance(item, dict)
        and str(item.get("tag") or "") == wanted["tag"]
        and str(item.get("value") or "") == wanted["value"]
        for item in tags
    )


def zabbix_legacy_managed_tags() -> list[dict[str, str]]:
    return [{"tag": "ManagedBy", "value": "hpe-aruba-central-ng"}]


def zabbix_has_managed_tag(tags: Any, wanted: dict[str, str]) -> bool:
    return zabbix_has_tag(tags, wanted) or any(zabbix_has_tag(tags, legacy) for legacy in zabbix_legacy_managed_tags())


def zabbix_merge_tags(*tag_lists: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[tuple[str, str], dict[str, str]] = {}
    for tags in tag_lists:
        for item in tags or []:
            tag = str(item.get("tag") or "")
            value = str(item.get("value") or "")
            if tag:
                merged[(tag, value)] = {"tag": tag, "value": value}
    return list(merged.values())


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


def get_greenlake_all_pages(token: str, path: str, query: dict[str, str | int] | None = None) -> list[dict[str, Any]]:
    params: dict[str, str | int] = {"limit": 100, "offset": 0}
    if query:
        params.update(query)

    all_items: list[dict[str, Any]] = []
    while True:
        data = request_json("GET", GREENLAKE_API + path, token=token, query=params)
        items = extract_items(data)
        all_items.extend(items)
        total = data.get("total")
        count = data.get("count")
        offset = int(params.get("offset") or 0)
        limit = int(params.get("limit") or len(items) or 100)
        if isinstance(total, int):
            if offset + len(items) >= total:
                break
        elif not items or len(items) < limit:
            break
        params["offset"] = offset + (count if isinstance(count, int) and count > 0 else limit)
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


def safe_host_part(value: Any, fallback: str = "Unknown") -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text or fallback


def mapping_matches(value: str, mapping: dict[str, Any], id_key: str, name_key: str) -> bool:
    wanted_id = str(mapping.get(id_key) or "").strip().lower()
    wanted_name = str(mapping.get(name_key) or "").strip().lower()
    return value.lower() in {wanted_id, wanted_name}


def workspace_mapping(workspace: dict[str, Any]) -> dict[str, Any]:
    mapping = workspace.get("mapping")
    return mapping if isinstance(mapping, dict) else {}


def tenant_mapping(workspace: dict[str, Any], tenant: dict[str, Any]) -> dict[str, Any]:
    mappings = workspace.get("tenant_mappings")
    if not isinstance(mappings, list):
        return workspace_mapping(workspace) if workspace.get("mode") == "standalone" else {}
    tenant_id = str(tenant.get("id") or "")
    name = tenant_name(tenant)
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        if mapping_matches(tenant_id, mapping, "tenant_id", "tenant_name") or mapping_matches(name, mapping, "tenant_id", "tenant_name"):
            return mapping
    return workspace_mapping(workspace) if workspace.get("mode") == "standalone" else {}


def mapping_has_host_prefix(mapping: dict[str, Any]) -> bool:
    return bool(str(mapping.get("host_prefix") or mapping.get("customer_prefix") or "").strip())


def unmapped_host_group() -> str:
    return env("ZABBIX_UNMAPPED_HOST_GROUP", required=False, default="HPE Aruba Central/Unmapped")


def collector_host_name() -> str:
    return env("ZABBIX_COLLECTOR_HOST", required=False, default="HPE Aruba Central Collector")


def host_prefix(mapping: dict[str, Any]) -> str:
    if mapping.get("host_prefix"):
        return safe_host_part(mapping.get("host_prefix"))
    if mapping.get("customer_prefix"):
        return safe_host_part(mapping.get("customer_prefix"))
    return "UNMAPPED"


def site_host_name(prefix: str, site_name: Any) -> str:
    return f"{prefix} - {safe_host_part(site_name, 'No Site')} - Central Site"


def device_host_name(prefix: str, device: dict[str, Any]) -> str:
    return f"{prefix} - {safe_host_part(device.get('name') or device.get('serial'), 'Central Device')}"


def apply_global_host_prefix(name: str) -> str:
    prefix = env("ZABBIX_HOST_NAME_PREFIX", required=False, default="").strip()
    if not prefix:
        return name
    separator = "" if prefix[-1].isspace() else " "
    full_prefix = f"{prefix}{separator}"
    return name if name.startswith(full_prefix) else f"{full_prefix}{name}"


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


def get_optional_switch_dict(
    token: str,
    path: str,
    query: dict[str, str | int] | None = None,
) -> dict[str, Any]:
    try:
        return central_get(token, path, query)
    except CentralError as exc:
        message = str(exc)
        if any(code in message for code in ("HTTP 400", "HTTP 404")):
            return {}
        raise


def get_switch_lag_summary(token: str, serial: str) -> list[dict[str, Any]]:
    data = get_optional_switch_dict(token, f"/network-monitoring/v1/switches/{serial}/lag")
    return extract_items(data)


def get_switch_stack_members(token: str, serial: str) -> list[dict[str, Any]]:
    data = get_optional_switch_dict(token, f"/network-monitoring/v1/stack/{serial}/members")
    return extract_items(data)


def get_switch_hardware_categories(token: str, serial: str) -> list[dict[str, Any]]:
    data = get_optional_switch_dict(token, f"/network-monitoring/v1/switches/{serial}/hardware-categories")
    return extract_items(data)


def get_switch_vsx(token: str, serial: str) -> dict[str, Any]:
    data = get_optional_switch_dict(token, f"/network-monitoring/v1/switches/{serial}/vsx")
    if not data:
        return {}
    vsx = data.get("vsx") if isinstance(data.get("vsx"), dict) else data
    if not isinstance(vsx, dict):
        return {}
    meaningful = [value for value in vsx.values() if value not in (None, "", [], {})]
    return vsx if meaningful else {}


def get_switch_hardware_trends(token: str, serial: str, site_id: str | None = None) -> dict[str, Any]:
    query = {"site-id": site_id} if site_id else None
    data = get_optional_switch_dict(token, f"/network-monitoring/v1/switches/{serial}/hardware-trends", query)
    return data if data else {}


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


def get_subscriptions_for_tenant(msp_token: str, tenant: dict[str, Any]) -> list[dict[str, Any]]:
    tenant_id = str(tenant.get("id") or "")
    if not tenant_id:
        return []
    token = tenant_token(msp_token, tenant)
    try:
        subscriptions = get_greenlake_all_pages(token, "/subscriptions/v1/subscriptions")
    except CentralError as exc:
        if "HTTP 401" not in str(exc):
            raise
        clear_cached_token(f"tenant:{tenant_id}")
        token = tenant_token(msp_token, tenant, force_refresh=True)
        subscriptions = get_greenlake_all_pages(token, "/subscriptions/v1/subscriptions")
    for subscription in subscriptions:
        subscription["_license_scope"] = "tenant"
        subscription["_owner_id"] = tenant_id
        subscription["_owner_name"] = tenant_name(tenant)
        subscription["_tenant_id"] = tenant_id
        subscription["_tenant_name"] = tenant_name(tenant)
        subscription["_workspace_id"] = tenant.get("_workspace_id")
        subscription["_workspace_name"] = tenant.get("_workspace_name")
    return subscriptions


def get_subscriptions_for_workspace(workspace_token: str, workspace: dict[str, Any]) -> list[dict[str, Any]]:
    subscriptions = get_greenlake_all_pages(workspace_token, "/subscriptions/v1/subscriptions")
    workspace_id = str(workspace.get("workspace_id") or "")
    workspace_name = str(workspace.get("name") or workspace_id)
    for subscription in subscriptions:
        subscription["_license_scope"] = "workspace"
        subscription["_owner_id"] = workspace_id
        subscription["_owner_name"] = workspace_name
        subscription["_tenant_id"] = workspace_id if workspace.get("mode") == "standalone" else ""
        subscription["_tenant_name"] = workspace_name if workspace.get("mode") == "standalone" else ""
        subscription["_workspace_id"] = workspace_id
        subscription["_workspace_name"] = workspace_name
    return subscriptions


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


def non_empty(value: Any) -> bool:
    return value not in (None, "", [], {})


def normalize_switch_lag(lag: dict[str, Any], switch: dict[str, Any]) -> dict[str, Any]:
    lag_id = first_value(lag, "id", "name", "lagName", "lagId", "interfaceName")
    return {
        "tenant_id": switch.get("tenant_id"),
        "tenant_name": switch.get("tenant_name"),
        "workspace_id": switch.get("workspace_id"),
        "workspace_name": switch.get("workspace_name"),
        "switch_serial": switch.get("serial"),
        "switch_name": switch.get("name"),
        "site_id": switch.get("site_id"),
        "site_name": switch.get("site_name"),
        "lag_id": lag_id,
        "name": first_value(lag, "name", "lagName", "interfaceName", "id"),
        "status": first_value(lag, "status", "operStatus", "state"),
        "admin_status": first_value(lag, "adminStatus", "admin_state"),
        "oper_status": first_value(lag, "operStatus", "oper_state"),
        "members": lag.get("members") or lag.get("interfaces") or lag.get("ports"),
        "raw": lag,
    }


def normalize_switch_stack_member(member: dict[str, Any], switch: dict[str, Any]) -> dict[str, Any]:
    member_id = first_value(member, "id", "stackMemberId", "memberId", "serialNumber", "serial")
    return {
        "tenant_id": switch.get("tenant_id"),
        "tenant_name": switch.get("tenant_name"),
        "workspace_id": switch.get("workspace_id"),
        "workspace_name": switch.get("workspace_name"),
        "switch_serial": switch.get("serial"),
        "switch_name": switch.get("name"),
        "site_id": switch.get("site_id"),
        "site_name": switch.get("site_name"),
        "member_id": member_id,
        "serial": first_value(member, "serialNumber", "serial", "id"),
        "name": first_value(member, "name", "hostname", "deviceName", "serialNumber"),
        "role": first_value(member, "role", "memberRole", "stackRole"),
        "status": first_value(member, "status", "state", "health"),
        "model": first_value(member, "model", "partNumber"),
        "raw": member,
    }


def normalize_switch_hardware(category: dict[str, Any], switch: dict[str, Any]) -> dict[str, Any]:
    category_id = first_value(category, "id", "serialNumber", "type", "name")
    return {
        "tenant_id": switch.get("tenant_id"),
        "tenant_name": switch.get("tenant_name"),
        "workspace_id": switch.get("workspace_id"),
        "workspace_name": switch.get("workspace_name"),
        "switch_serial": switch.get("serial"),
        "switch_name": switch.get("name"),
        "site_id": switch.get("site_id"),
        "site_name": switch.get("site_name"),
        "hardware_id": category_id,
        "name": first_value(category, "name", "type", "id", "serialNumber"),
        "type": first_value(category, "type", "category"),
        "role": category.get("role"),
        "status": first_value(category, "status", "state"),
        "cpu_health": nested_first(category, ("cpu", "health")),
        "memory_health": nested_first(category, ("memory", "health")),
        "temperature_health": nested_first(category, ("temperature", "health")),
        "fans_health": nested_first(category, ("fans", "health")),
        "fans_up_count": nested_first(category, ("fans", "upCount")),
        "fans_total_count": nested_first(category, ("fans", "totalCount")),
        "power_supplies_health": nested_first(category, ("powerSupplies", "health")),
        "power_supplies_up_count": nested_first(category, ("powerSupplies", "upCount")),
        "power_supplies_total_count": nested_first(category, ("powerSupplies", "totalCount")),
        "raw": category,
    }


def normalize_switch_vsx(vsx: dict[str, Any], switch: dict[str, Any]) -> dict[str, Any]:
    return {
        "tenant_id": switch.get("tenant_id"),
        "tenant_name": switch.get("tenant_name"),
        "workspace_id": switch.get("workspace_id"),
        "workspace_name": switch.get("workspace_name"),
        "switch_serial": switch.get("serial"),
        "switch_name": switch.get("name"),
        "site_id": switch.get("site_id"),
        "site_name": switch.get("site_name"),
        "role": first_value(vsx, "role", "vsxRole"),
        "status": first_value(vsx, "status", "state", "operStatus"),
        "peer_status": first_value(vsx, "peerStatus", "peer_status"),
        "isl_status": first_value(vsx, "islStatus", "isl_status"),
        "raw": vsx,
    }


def normalize_switch_hardware_trends(trends: dict[str, Any], switch: dict[str, Any]) -> dict[str, Any]:
    response = trends.get("response") if isinstance(trends.get("response"), dict) else trends
    keys = response.get("keys") if isinstance(response.get("keys"), list) else []
    samples: list[dict[str, Any]] = []
    metrics = response.get("switchMetrics")
    if isinstance(metrics, list):
        for metric in metrics:
            if isinstance(metric, dict) and isinstance(metric.get("samples"), list):
                samples.extend(sample for sample in metric["samples"] if isinstance(sample, dict))
    latest = max(samples, key=lambda item: item.get("timestamp") or 0) if samples else {}
    latest_data = latest.get("data") if isinstance(latest.get("data"), list) else []
    values = {str(key): latest_data[index] for index, key in enumerate(keys) if index < len(latest_data)}
    return {
        "tenant_id": switch.get("tenant_id"),
        "tenant_name": switch.get("tenant_name"),
        "workspace_id": switch.get("workspace_id"),
        "workspace_name": switch.get("workspace_name"),
        "switch_serial": switch.get("serial"),
        "switch_name": switch.get("name"),
        "site_id": switch.get("site_id"),
        "site_name": switch.get("site_name"),
        "timestamp": latest.get("timestamp"),
        "cpu_utilization": to_float(values.get("cpuUtilization")),
        "memory_utilization": to_float(values.get("memoryUtilization")),
        "system_temperature": to_float(values.get("systemTemperature")),
        "poe_available": to_float(values.get("poeAvailable")),
        "poe_consumption": to_float(values.get("poeConsumption")),
        "power_consumption": to_float(values.get("powerConsumption")),
        "total_power_consumption": to_float(values.get("totalPowerConsumption")),
        "raw": trends,
    }


def nested_first(data: dict[str, Any], path: tuple[str, ...]) -> Any:
    value: Any = data
    for part in path:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def days_until(value: Any) -> float | None:
    parsed = parse_iso_datetime(value)
    if not parsed:
        return None
    return round((parsed - datetime.now(timezone.utc)).total_seconds() / 86400, 3)


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def license_device_type_tag(subscription_type: Any) -> str:
    value = str(subscription_type or "").upper()
    if "AP" in value:
        return device_type_tag("ACCESS_POINT")
    if "SWITCH" in value or "CX" in value:
        return device_type_tag("SWITCH")
    if "GW" in value or "GATEWAY" in value:
        return device_type_tag("GATEWAY")
    return "license"


def normalize_subscription(subscription: dict[str, Any]) -> dict[str, Any]:
    key = str(subscription.get("key") or "")
    end_time = subscription.get("endTime")
    expiry_days = days_until(end_time)
    return {
        "license_scope": subscription.get("_license_scope"),
        "owner_id": subscription.get("_owner_id"),
        "owner_name": subscription.get("_owner_name"),
        "workspace_id": subscription.get("_workspace_id"),
        "workspace_name": subscription.get("_workspace_name"),
        "tenant_id": subscription.get("_tenant_id"),
        "tenant_name": subscription.get("_tenant_name"),
        "id": subscription.get("id"),
        "key_suffix": key[-4:] if key else "",
        "subscription_type": subscription.get("subscriptionType"),
        "subscription_status": subscription.get("subscriptionStatus"),
        "tier": subscription.get("tier"),
        "tier_description": subscription.get("tierDescription"),
        "sku": subscription.get("sku") or subscription.get("productSku"),
        "sku_description": subscription.get("skuDescription") or subscription.get("productDescription"),
        "product_type": subscription.get("productType"),
        "quantity": to_int(subscription.get("quantity")) or 0,
        "available_quantity": to_int(subscription.get("availableQuantity")) or 0,
        "start_time": subscription.get("startTime"),
        "end_time": end_time,
        "days_until_expiry": expiry_days if expiry_days is not None else 999999,
        "is_eval": subscription.get("isEval"),
        "tags": subscription.get("tags") if isinstance(subscription.get("tags"), dict) else {},
    }


def is_monitorable_subscription(subscription: dict[str, Any]) -> bool:
    status = str(subscription.get("subscription_status") or "").upper()
    return status in ("STARTED", "ACTIVE") and subscription.get("is_eval") is False


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


def sender_key_args(key: str) -> list[str]:
    match = re.search(r"\[(.*)\]$", key)
    if not match:
        return []
    return [item.strip() for item in match.group(1).split(",")]


def lld_payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"data": rows}


def retarget_sender_lines(lines: list[str], host_by_serial: dict[str, str], collector_host: str) -> list[str]:
    retargeted: list[str] = []
    ap_radios: dict[str, list[dict[str, Any]]] = {}
    switch_interfaces: dict[str, list[dict[str, Any]]] = {}
    switch_lags: dict[str, list[dict[str, Any]]] = {}
    switch_stack_members: dict[str, list[dict[str, Any]]] = {}
    switch_hardware: dict[str, list[dict[str, Any]]] = {}

    for line in lines:
        _host, key, value = parse_sender_line(line)
        target_host = collector_host
        args = sender_key_args(key)
        if key in {
            "central.aps.discovery",
            "central.ap.radios.discovery",
            "central.switches.discovery",
            "central.switch.interfaces.discovery",
            "central.switch.lags.discovery",
            "central.switch.stack_members.discovery",
            "central.switch.hardware.discovery",
            "central.switch.vsx.discovery",
            "central.switch.hardware_trends.discovery",
        }:
            continue
        if key.startswith("central.ap.raw[") and len(args) >= 2:
            target_host = host_by_serial.get(args[1], collector_host)
            key = "central.ap.raw"
        elif key.startswith("central.ap.radio.raw[") and len(args) >= 2:
            target_host = host_by_serial.get(args[1], collector_host)
            radio_number = args[2] if len(args) > 2 else ""
            key = f"central.ap.radio.raw[{radio_number}]"
            record = json.loads(value)
            ap_radios.setdefault(target_host, []).append(
                {
                    "{#RADIO_NUMBER}": record.get("radio_number"),
                    "{#RADIO_BAND}": record.get("band"),
                }
            )
        elif key.startswith("central.switch.") and ".raw[" in key and len(args) >= 2:
            target_host = host_by_serial.get(args[1], collector_host)
            record = json.loads(value)
            if key.startswith("central.switch.raw["):
                key = "central.switch.raw"
            elif key.startswith("central.switch.interface.raw["):
                port_index = args[2] if len(args) > 2 else ""
                key = f"central.switch.interface.raw[{port_index}]"
                switch_interfaces.setdefault(target_host, []).append(
                    {
                        "{#PORT_INDEX}": record.get("port_index"),
                        "{#PORT_NAME}": record.get("name"),
                        "{#PORT_CONNECTOR}": record.get("connector"),
                    }
                )
            elif key.startswith("central.switch.lag.raw["):
                lag_id = args[2] if len(args) > 2 else ""
                key = f"central.switch.lag.raw[{lag_id}]"
                switch_lags.setdefault(target_host, []).append(
                    {
                        "{#LAG_ID}": record.get("lag_id"),
                        "{#LAG_NAME}": record.get("name") or record.get("lag_id"),
                    }
                )
            elif key.startswith("central.switch.stack_member.raw["):
                member_id = args[2] if len(args) > 2 else ""
                key = f"central.switch.stack_member.raw[{member_id}]"
                switch_stack_members.setdefault(target_host, []).append(
                    {
                        "{#STACK_MEMBER_ID}": record.get("member_id"),
                        "{#STACK_MEMBER_NAME}": record.get("name") or record.get("serial") or record.get("member_id"),
                    }
                )
            elif key.startswith("central.switch.hardware.raw["):
                hardware_id = args[2] if len(args) > 2 else ""
                key = f"central.switch.hardware.raw[{hardware_id}]"
                switch_hardware.setdefault(target_host, []).append(
                    {
                        "{#HARDWARE_ID}": record.get("hardware_id"),
                        "{#HARDWARE_NAME}": record.get("name") or record.get("hardware_id"),
                        "{#HARDWARE_TYPE}": record.get("type"),
                    }
                )
            elif key.startswith("central.switch.vsx.raw["):
                key = "central.switch.vsx.raw"
            elif key.startswith("central.switch.hardware_trends.raw["):
                key = "central.switch.hardware_trends.raw"
        retargeted.append(sender_line(target_host, key, value))

    for host, rows in ap_radios.items():
        retargeted.append(sender_line(host, "central.ap.radios.discovery", lld_payload(rows)))
    for host, rows in switch_interfaces.items():
        retargeted.append(sender_line(host, "central.switch.interfaces.discovery", lld_payload(rows)))
    for host, rows in switch_lags.items():
        retargeted.append(sender_line(host, "central.switch.lags.discovery", lld_payload(rows)))
    for host, rows in switch_stack_members.items():
        retargeted.append(sender_line(host, "central.switch.stack_members.discovery", lld_payload(rows)))
    for host, rows in switch_hardware.items():
        retargeted.append(sender_line(host, "central.switch.hardware.discovery", lld_payload(rows)))
    return retargeted


def host_lookup_from_plans(plans: list[dict[str, Any]]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for plan in plans:
        serial = plan.get("serial")
        host = plan.get("host")
        if serial and host and plan.get("kind") in ("ap", "switch", "gateway"):
            lookup[str(serial)] = str(host)
    return lookup


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
            radio_record["ap_status"] = ap.get("status")
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
    lags: list[dict[str, Any]] = []
    stack_members: list[dict[str, Any]] = []
    hardware_categories: list[dict[str, Any]] = []
    hardware_trends: list[dict[str, Any]] = []
    vsx_details: list[dict[str, Any]] = []
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
        tenant = tenant_by_id[tenant_id]
        token = tenant_token(msp_token, tenant)
        detail = normalize_switch_detail(get_switch_detail_for_tenant(msp_token, tenant, serial, site_id or None))
        detail["workspace_name"] = switch.get("workspace_name")
        detail["workspace_id"] = switch.get("workspace_id")
        lines.append(sender_line(zabbix_host, f"central.switch.raw[{tenant_id},{serial}]", detail))
        for interface in get_switch_interfaces_for_tenant(msp_token, tenant, serial, site_id or None):
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

        for lag in get_switch_lag_summary(token, serial):
            normalized_lag = normalize_switch_lag(lag, switch)
            lag_id = normalized_lag.get("lag_id")
            if not non_empty(lag_id):
                continue
            lags.append(normalized_lag)
            lines.append(sender_line(zabbix_host, f"central.switch.lag.raw[{tenant_id},{serial},{lag_id}]", normalized_lag))

        for member in get_switch_stack_members(token, serial):
            normalized_member = normalize_switch_stack_member(member, switch)
            member_id = normalized_member.get("member_id")
            if not non_empty(member_id):
                continue
            stack_members.append(normalized_member)
            lines.append(sender_line(zabbix_host, f"central.switch.stack_member.raw[{tenant_id},{serial},{member_id}]", normalized_member))

        for category in get_switch_hardware_categories(token, serial):
            normalized_category = normalize_switch_hardware(category, switch)
            hardware_id = normalized_category.get("hardware_id")
            if not non_empty(hardware_id):
                continue
            hardware_categories.append(normalized_category)
            lines.append(sender_line(zabbix_host, f"central.switch.hardware.raw[{tenant_id},{serial},{hardware_id}]", normalized_category))

        normalized_vsx = normalize_switch_vsx(get_switch_vsx(token, serial), switch)
        if any(non_empty(normalized_vsx.get(key)) for key in ("role", "status", "peer_status", "isl_status")):
            vsx_details.append(normalized_vsx)
            lines.append(sender_line(zabbix_host, f"central.switch.vsx.raw[{tenant_id},{serial}]", normalized_vsx))

        normalized_trends = normalize_switch_hardware_trends(get_switch_hardware_trends(token, serial, site_id or None), switch)
        if any(non_empty(normalized_trends.get(key)) for key in ("cpu_utilization", "memory_utilization", "system_temperature", "poe_available", "poe_consumption", "power_consumption", "total_power_consumption")):
            hardware_trends.append(normalized_trends)
            lines.append(sender_line(zabbix_host, f"central.switch.hardware_trends.raw[{tenant_id},{serial}]", normalized_trends))

    lines.insert(1, sender_line(zabbix_host, "central.switch.interfaces.discovery", switch_interfaces_lld(interfaces)))
    lines.insert(2, sender_line(zabbix_host, "central.switch.lags.discovery", switch_lags_lld(lags)))
    lines.insert(3, sender_line(zabbix_host, "central.switch.stack_members.discovery", switch_stack_members_lld(stack_members)))
    lines.insert(4, sender_line(zabbix_host, "central.switch.hardware.discovery", switch_hardware_lld(hardware_categories)))
    lines.insert(5, sender_line(zabbix_host, "central.switch.vsx.discovery", switch_vsx_lld(vsx_details)))
    lines.insert(6, sender_line(zabbix_host, "central.switch.hardware_trends.discovery", switch_hardware_trends_lld(hardware_trends)))
    return lines


def build_license_sender_lines(
    msp_token: str,
    workspace: dict[str, Any],
    tenants: list[dict[str, Any]],
    zabbix_host: str,
) -> list[str]:
    subscriptions: list[dict[str, Any]] = []
    lines: list[str] = []
    subscriptions.extend(
        subscription
        for subscription in (normalize_subscription(item) for item in get_subscriptions_for_workspace(msp_token, workspace))
        if is_monitorable_subscription(subscription)
    )
    for tenant in tenants:
        if workspace.get("mode") == "standalone":
            continue
        subscriptions.extend(
            subscription
            for subscription in (normalize_subscription(item) for item in get_subscriptions_for_tenant(msp_token, tenant))
            if is_monitorable_subscription(subscription)
        )

    lines.append(sender_line(zabbix_host, "central.licenses.discovery", licenses_lld(subscriptions)))
    for subscription in subscriptions:
        license_scope = str(subscription.get("license_scope") or "")
        owner_id = str(subscription.get("owner_id") or "")
        subscription_id = str(subscription.get("id") or "")
        if not license_scope or not owner_id or not subscription_id:
            continue
        lines.append(sender_line(zabbix_host, f"central.license.raw[{license_scope},{owner_id},{subscription_id}]", subscription))
    return lines


def build_all_sender_lines(
    msp_token: str,
    tenants: list[dict[str, Any]],
    zabbix_host: str,
    workspace: dict[str, Any] | None = None,
) -> list[str]:
    started = time.time()
    ap_lines = build_ap_sender_lines(msp_token, tenants, zabbix_host)
    switch_lines = build_switch_sender_lines(msp_token, tenants, zabbix_host)
    license_lines = build_license_sender_lines(msp_token, workspace or {}, tenants, zabbix_host)
    inventory = collect_device_inventory_summary(msp_token, tenants)
    health = {
        "status": "ok",
        "timestamp": int(time.time()),
        "tenants_count": len(tenants),
        "device_counts_by_type": inventory["device_counts_by_type"],
        "device_counts_by_tenant": inventory["device_counts_by_tenant"],
        "devices_total": inventory["devices_total"],
        "sent_lines": len(ap_lines) + len(switch_lines) + len(license_lines),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    return [sender_line(zabbix_host, "central.collector.health", health)] + [
        line for line in ap_lines + switch_lines + license_lines if " central.collector.health " not in line
    ]


def build_all_config_sender_lines(config: dict[str, Any] | None) -> list[str]:
    health, all_lines = collect_all_config_payload(config)
    collector_host = apply_global_host_prefix(collector_host_name())
    try:
        plans = collect_host_plans(config or {})
        health["zabbix_managed_hosts_status"] = collect_zabbix_managed_hosts_status(config or {}, plans)
        lines = [sender_line(collector_host, "central.collector.health", health)] + all_lines
        return retarget_sender_lines(lines, host_lookup_from_plans(plans), collector_host)
    except Exception as exc:
        health["host_retargeting_status"] = {"status": "error", "error": str(exc)}
        return [sender_line(collector_host, "central.collector.health", health)] + all_lines


def collect_all_config_payload(config: dict[str, Any] | None) -> tuple[dict[str, Any], list[str]]:
    apply_zabbix_config(config)
    zabbix_host = apply_global_host_prefix(collector_host_name())
    started = time.time()
    all_lines: list[str] = []
    discovery_data: dict[str, list[dict[str, Any]]] = {
        "central.aps.discovery": [],
        "central.ap.radios.discovery": [],
        "central.switches.discovery": [],
        "central.switch.interfaces.discovery": [],
        "central.switch.lags.discovery": [],
        "central.switch.stack_members.discovery": [],
        "central.switch.hardware.discovery": [],
        "central.switch.vsx.discovery": [],
        "central.switch.hardware_trends.discovery": [],
        "central.licenses.discovery": [],
    }
    workspace_summaries: list[dict[str, Any]] = []
    workspace_count = 0
    tenant_count = 0
    devices_total = 0
    device_counts_by_type: dict[str, int] = {}
    unmapped_tenant_mappings: list[dict[str, Any]] = []

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
            if workspace.get("mode") == "msp":
                for tenant in tenants:
                    mapping = tenant_mapping(workspace, tenant)
                    if not mapping_has_host_prefix(mapping):
                        unmapped_tenant_mappings.append(
                            {
                                "workspace_name": workspace.get("name"),
                                "workspace_id": workspace.get("workspace_id"),
                                "tenant_id": tenant.get("id"),
                                "tenant_name": tenant_name(tenant),
                            }
                        )
            workspace_lines = build_all_sender_lines(msp_token, tenants, zabbix_host, workspace)
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
        "config_status": collect_config_status(config),
        "workspace_count": workspace_count,
        "tenants_count": tenant_count,
        "devices_total": devices_total,
        "device_counts_by_type": device_counts_by_type,
        "device_counts_by_workspace": workspace_summaries,
        "unmapped_tenant_mappings_count": len(unmapped_tenant_mappings),
        "unmapped_tenant_mappings": unmapped_tenant_mappings,
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


def is_discovery_sender_line(line: str) -> bool:
    try:
        _host, key, _value = parse_sender_line(line)
    except ValueError:
        return False
    return key.endswith(".discovery")


def run_zabbix_sender_discovery_first(lines: list[str]) -> dict[str, Any]:
    discovery_lines = [line for line in lines if is_discovery_sender_line(line)]
    value_lines = [line for line in lines if not is_discovery_sender_line(line)]
    if not discovery_lines or not value_lines:
        return run_zabbix_sender(lines)

    settle_seconds = max(0, int(env("CENTRAL_LLD_SETTLE_SECONDS", required=False, default="10")))
    discovery_result = run_zabbix_sender(discovery_lines)
    if settle_seconds:
        time.sleep(settle_seconds)
    values_result = run_zabbix_sender(value_lines)
    return {
        "mode": "discovery_first",
        "settle_seconds": settle_seconds,
        "returncode": values_result.get("returncode", 0) or discovery_result.get("returncode", 0),
        "sent_lines": len(lines),
        "discovery": discovery_result,
        "values": values_result,
    }


def zabbix_sender_stdout(result: dict[str, Any]) -> str:
    if result.get("mode") != "discovery_first":
        return str(result.get("stdout") or "")
    discovery = result.get("discovery") if isinstance(result.get("discovery"), dict) else {}
    values = result.get("values") if isinstance(result.get("values"), dict) else {}
    return f"discovery={discovery.get('stdout')!r} values={values.get('stdout')!r}"


def zabbix_sender_stderr(result: dict[str, Any]) -> str:
    if result.get("mode") != "discovery_first":
        return str(result.get("stderr") or "")
    discovery = result.get("discovery") if isinstance(result.get("discovery"), dict) else {}
    values = result.get("values") if isinstance(result.get("values"), dict) else {}
    return f"discovery={discovery.get('stderr')!r} values={values.get('stderr')!r}"


def summarize_host_sync(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": result.get("status"),
        "planned": result.get("planned"),
        "created": result.get("created"),
        "updated": result.get("updated"),
        "reason": result.get("reason"),
    }


def zabbix_get_or_create_hostgroup(name: str, apply: bool = False) -> dict[str, Any]:
    groups = zabbix_api_call("hostgroup.get", {"output": ["groupid", "name"], "filter": {"name": [name]}})
    if isinstance(groups, list) and groups:
        return {"name": name, "groupid": groups[0]["groupid"], "created": False}
    if not apply:
        return {"name": name, "groupid": "", "created": False, "pending": True}
    created = zabbix_api_call("hostgroup.create", {"name": name})
    groupids = created.get("groupids") if isinstance(created, dict) else None
    return {"name": name, "groupid": groupids[0] if groupids else "", "created": True}


def zabbix_get_template_ids(template_names: list[str]) -> list[dict[str, str]]:
    names = [name for name in template_names if name]
    if not names:
        return []
    templates = zabbix_api_call("template.get", {"output": ["templateid", "host"], "filter": {"host": names}})
    if not isinstance(templates, list):
        return []
    by_name = {template.get("host"): template.get("templateid") for template in templates if template.get("host")}
    missing = [name for name in names if name not in by_name]
    if missing:
        raise CentralError(f"Missing Zabbix templates: {', '.join(missing)}")
    return [{"templateid": str(by_name[name])} for name in names]


def zabbix_get_host(host: str) -> dict[str, Any] | None:
    hosts = zabbix_api_call(
        "host.get",
        {
            "output": ["hostid", "host", "name"],
            "selectTags": "extend",
            "selectParentTemplates": ["templateid", "host"],
            "filter": {"host": [host]},
        },
    )
    if isinstance(hosts, list) and hosts:
        return hosts[0]
    return None


def zabbix_get_managed_hosts(config: dict[str, Any]) -> list[dict[str, Any]]:
    managed_tag = zabbix_managed_tag_config(config)
    hosts = zabbix_api_call(
        "host.get",
        {
            "output": ["hostid", "host", "name", "status"],
            "selectTags": "extend",
        },
    )
    if not isinstance(hosts, list):
        return []
    return [host for host in hosts if isinstance(host, dict) and zabbix_has_managed_tag(host.get("tags"), managed_tag)]


def collect_zabbix_managed_hosts_status(config: dict[str, Any], plans: list[dict[str, Any]]) -> dict[str, Any]:
    apply_zabbix_config(config)
    if not env("ZABBIX_API_URL", required=False, default="") or not env("ZABBIX_API_TOKEN", required=False, default=""):
        return {"status": "skipped", "reason": "missing Zabbix API configuration"}
    planned_hosts = {str(plan.get("host") or "") for plan in plans if plan.get("host")}
    managed_hosts = zabbix_get_managed_hosts(config)
    stale_hosts = [
        {
            "hostid": host.get("hostid"),
            "host": host.get("host"),
            "name": host.get("name"),
            "status": host.get("status"),
        }
        for host in managed_hosts
        if str(host.get("host") or "") not in planned_hosts
    ]
    stale_hosts.sort(key=lambda item: str(item.get("host") or ""))
    return {
        "status": "ok",
        "managed_hosts_count": len(managed_hosts),
        "planned_hosts_count": len(planned_hosts),
        "stale_hosts_count": len(stale_hosts),
        "stale_host_names": ", ".join(str(host.get("host") or "") for host in stale_hosts if host.get("host")),
        "stale_hosts": stale_hosts,
    }


def zabbix_tags_equal(current: Any, desired: list[dict[str, str]]) -> bool:
    if not isinstance(current, list):
        current = []
    current_pairs = {(str(item.get("tag") or ""), str(item.get("value") or "")) for item in current if isinstance(item, dict)}
    desired_pairs = {(str(item.get("tag") or ""), str(item.get("value") or "")) for item in desired}
    return current_pairs == desired_pairs


def zabbix_template_names(current: Any) -> set[str]:
    if not isinstance(current, list):
        return set()
    return {str(item.get("host") or "") for item in current if isinstance(item, dict) and item.get("host")}


def zabbix_ensure_host(plan: dict[str, Any], apply: bool = False) -> dict[str, Any]:
    config = plan.get("_config") if isinstance(plan.get("_config"), dict) else {}
    managed_tag = zabbix_managed_tag_config(config)
    desired_tags = zabbix_merge_tags(plan.get("tags") or [], [managed_tag])
    group = zabbix_get_or_create_hostgroup(str(plan["host_group"]), apply=apply)
    existing = zabbix_get_host(str(plan["host"]))
    result = {
        "host": plan["host"],
        "visible_name": plan.get("visible_name") or plan["host"],
        "host_group": plan["host_group"],
        "templates": plan.get("templates") or [],
        "tags": desired_tags,
        "kind": plan.get("kind"),
        "exists": bool(existing),
        "created": False,
        "updated": False,
        "protected": False,
        "pending": not apply,
    }
    if not apply:
        return result
    templates = zabbix_get_template_ids([str(item) for item in plan.get("templates") or []])
    groups = [{"groupid": group["groupid"]}]
    if existing:
        if not zabbix_has_managed_tag(existing.get("tags"), managed_tag):
            result["protected"] = True
            result["pending"] = False
            result["error"] = (
                f"Existing host {plan['host']!r} is not tagged "
                f"{managed_tag['tag']}={managed_tag['value']!r}; refusing to update it"
            )
            raise CentralError(result["error"])
        params: dict[str, Any] = {
            "hostid": existing["hostid"],
            "name": plan.get("visible_name") or plan["host"],
        }
        current_template_names = zabbix_template_names(existing.get("parentTemplates"))
        desired_template_names = {str(item) for item in plan.get("templates") or []}
        needs_update = (
            str(existing.get("name") or "") != str(plan.get("visible_name") or plan["host"])
            or not zabbix_tags_equal(existing.get("tags"), desired_tags)
            or not desired_template_names.issubset(current_template_names)
        )
        if not needs_update:
            result["pending"] = False
            return result
        params["tags"] = desired_tags
        if templates:
            existing_templates = [
                {"templateid": str(item.get("templateid"))}
                for item in existing.get("parentTemplates") or []
                if isinstance(item, dict) and item.get("templateid")
            ]
            merged_templates = {item["templateid"]: item for item in existing_templates + templates}
            params["templates"] = list(merged_templates.values())
        zabbix_api_call("host.update", params)
        result["updated"] = True
        result["pending"] = False
        return result
    params = {
        "host": plan["host"],
        "name": plan.get("visible_name") or plan["host"],
        "groups": groups,
        "tags": desired_tags,
    }
    if templates:
        params["templates"] = templates
    created = zabbix_api_call("host.create", params)
    result["created"] = bool(isinstance(created, dict) and created.get("hostids"))
    result["pending"] = False
    return result


def zabbix_templates(config: dict[str, Any]) -> dict[str, str]:
    zabbix = config.get("zabbix") if isinstance(config.get("zabbix"), dict) else {}
    templates = zabbix.get("templates") if isinstance(zabbix.get("templates"), dict) else {}
    return {
        "collector": str(templates.get("collector") or "HPE Aruba Central NG - Collector"),
        "site": str(templates.get("site") or "HPE Aruba Central NG - Site"),
        "ap": str(templates.get("ap") or "HPE Aruba Central NG - AP"),
        "switch": str(templates.get("switch") or "HPE Aruba Central NG - Switch"),
        "gateway": str(templates.get("gateway") or "HPE Aruba Central NG - Gateway"),
    }


def zabbix_host_tags_config(config: dict[str, Any]) -> dict[str, str]:
    zabbix = config.get("zabbix") if isinstance(config.get("zabbix"), dict) else {}
    tags = zabbix.get("host_tags") if isinstance(zabbix.get("host_tags"), dict) else {}
    return {
        "ap": str(tags.get("ap") or "WiFi"),
        "switch": str(tags.get("switch") or "Switch"),
        "gateway": str(tags.get("gateway") or "Gateway"),
    }


def zabbix_host_tags(config: dict[str, Any], kind: str) -> list[dict[str, str]]:
    if kind not in ("ap", "switch", "gateway"):
        return []
    tags = zabbix_host_tags_config(config)
    tag = tags.get(kind)
    if not tag:
        return []
    return [{"tag": tag, "value": ""}]


def collect_host_plans(config: dict[str, Any]) -> list[dict[str, Any]]:
    apply_zabbix_config(config)
    templates = zabbix_templates(config)
    plans: dict[str, dict[str, Any]] = {}
    collector_host = apply_global_host_prefix(collector_host_name())
    plans[collector_host] = {
        "_config": config,
        "kind": "collector",
        "host": collector_host,
        "visible_name": collector_host,
        "host_group": unmapped_host_group(),
        "templates": [templates["collector"]],
        "tags": zabbix_host_tags(config, "collector"),
    }

    for workspace in config_workspaces(config):
        use_workspace_env(workspace)
        msp_token = get_msp_token()
        tenants = get_workspace_tenants(msp_token, workspace)
        for tenant in tenants:
            mapping = tenant_mapping(workspace, tenant)
            tenant_devices = get_devices_for_tenant(msp_token, tenant)
            tenant_switches = [normalize_switch(switch) for switch in get_switches_for_tenant(msp_token, tenant)]
            devices = [normalize_device(device) for device in tenant_devices] + tenant_switches
            for device in devices:
                prefix = host_prefix(mapping)
                host_group = unmapped_host_group()
                site_key = apply_global_host_prefix(site_host_name(prefix, device.get("site_name")))
                plans.setdefault(
                    site_key,
                    {
                        "_config": config,
                        "kind": "site",
                        "host": site_key,
                        "visible_name": site_key,
                        "host_group": host_group,
                        "templates": [templates["site"]],
                        "tags": zabbix_host_tags(config, "site"),
                        "workspace": workspace.get("name"),
                        "tenant": tenant_name(tenant),
                        "site_name": device.get("site_name"),
                    },
                )
                device_type = str(device.get("device_type") or "SWITCH").upper()
                template_key = "ap" if device_type == "ACCESS_POINT" else "switch" if device_type == "SWITCH" else "gateway" if device_type == "GATEWAY" else ""
                if not template_key:
                    continue
                host = apply_global_host_prefix(device_host_name(prefix, device))
                plans[host] = {
                    "_config": config,
                    "kind": template_key,
                    "host": host,
                    "visible_name": host,
                    "host_group": host_group,
                    "templates": [templates[template_key]],
                    "tags": zabbix_host_tags(config, template_key),
                    "workspace": workspace.get("name"),
                    "tenant": tenant_name(tenant),
                    "site_name": device.get("site_name"),
                    "serial": device.get("serial"),
                    "central_name": device.get("name"),
                }
    return sorted(plans.values(), key=lambda item: (str(item.get("host_group")), str(item.get("host"))))


def sync_zabbix_hosts(config: dict[str, Any], apply: bool = False) -> dict[str, Any]:
    plans = collect_host_plans(config)
    if not apply:
        return {
            "apply": False,
            "planned": len(plans),
            "created": 0,
            "updated": 0,
            "hosts": [public_host_plan(plan, config) for plan in plans],
        }
    results = [zabbix_ensure_host(plan, apply=apply) for plan in plans]
    return {
        "apply": apply,
        "planned": len(plans),
        "created": sum(1 for item in results if item.get("created")),
        "updated": sum(1 for item in results if item.get("updated")),
        "hosts": results,
    }


def sync_zabbix_hosts_if_configured(config: dict[str, Any]) -> dict[str, Any]:
    apply_zabbix_config(config)
    if not env("ZABBIX_API_URL", required=False, default="") or not env("ZABBIX_API_TOKEN", required=False, default=""):
        return {"status": "skipped", "reason": "missing Zabbix API configuration"}
    result = sync_zabbix_hosts(config, apply=True)
    result["status"] = "ok"
    return result


def import_zabbix_template(config: dict[str, Any], apply: bool = False) -> dict[str, Any]:
    apply_zabbix_config(config)
    path = Path(__file__).with_name("zabbix_template_hpe_aruba_central_new_ap_trapper.yaml")
    source = path.read_text(encoding="utf-8")
    template_names = extract_template_names(source)
    result = {
        "apply": apply,
        "path": str(path),
        "templates": template_names,
    }
    if not apply:
        return result
    zabbix_api_call(
        "configuration.import",
        {
            "format": "yaml",
            "rules": {
                "template_groups": {"createMissing": True},
                "templates": {"createMissing": True, "updateExisting": True},
                "items": {"createMissing": True, "updateExisting": True},
                "discoveryRules": {"createMissing": True, "updateExisting": True},
                "triggers": {"createMissing": True, "updateExisting": True},
                "valueMaps": {"createMissing": True, "updateExisting": True},
            },
            "source": source,
        },
    )
    result["imported"] = True
    return result


def extract_template_names(source: str) -> list[str]:
    try:
        data = yaml_safe_load(source)
    except Exception:
        return []
    export = data.get("zabbix_export") if isinstance(data, dict) else None
    templates = export.get("templates") if isinstance(export, dict) else None
    if not isinstance(templates, list):
        return []
    return [str(item.get("template") or item.get("name")) for item in templates if isinstance(item, dict)]


def yaml_safe_load(source: str) -> Any:
    try:
        import yaml
    except ImportError as exc:
        raise ConfigError("PyYAML is required for template import metadata. Install with: pip install pyyaml") from exc
    return yaml.safe_load(source)


def public_host_plan(plan: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    public = {key: value for key, value in plan.items() if not key.startswith("_")}
    public["tags"] = zabbix_merge_tags(public.get("tags") or [], [zabbix_managed_tag_config(config)])
    return public


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
                sync_result = sync_zabbix_hosts_if_configured(config)
                lines = build_all_config_sender_lines(config)
            else:
                apply_zabbix_config(config)
                workspaces = config_workspaces(config)
                use_workspace_env(workspaces[0])
                zabbix_host = apply_global_host_prefix(collector_host_name())
                msp_token = get_msp_token()
                tenants = get_workspace_tenants(msp_token, workspaces[0])
                if command == "push-aps":
                    lines = build_ap_sender_lines(msp_token, tenants, zabbix_host)
                elif command == "push-switches":
                    lines = build_switch_sender_lines(msp_token, tenants, zabbix_host)
                elif command == "push-all":
                    lines = build_all_sender_lines(msp_token, tenants, zabbix_host, workspaces[0])
                else:
                    raise CentralError(f"Unsupported daemon command: {command}")
            result = run_zabbix_sender_discovery_first(lines)
            level = "ok" if result.get("returncode") == 0 else "error"
            log_line(
                f"{level} command={command} sent_lines={result.get('sent_lines')} "
                f"returncode={result.get('returncode')} elapsed={round(time.time() - started, 3)}s "
                f"host_sync={summarize_host_sync(sync_result) if command == 'push-all' else 'not-applicable'} "
                f"stdout={zabbix_sender_stdout(result)!r} stderr={zabbix_sender_stderr(result)!r}"
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


def switch_lags_lld(lags: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": lag.get("tenant_id"),
                "{#TENANT_NAME}": lag.get("tenant_name"),
                "{#WORKSPACE_NAME}": lag.get("workspace_name") or lag.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag("SWITCH"),
                "{#SWITCH_SERIAL}": lag.get("switch_serial"),
                "{#SWITCH_NAME}": lag.get("switch_name"),
                "{#LAG_ID}": lag.get("lag_id"),
                "{#LAG_NAME}": lag.get("name") or lag.get("lag_id"),
                "{#SITE_ID}": lag.get("site_id"),
                "{#SITE_NAME}": lag.get("site_name"),
            }
            for lag in lags
            if lag.get("tenant_id") and lag.get("switch_serial") and non_empty(lag.get("lag_id"))
        ]
    }


def switch_stack_members_lld(members: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": member.get("tenant_id"),
                "{#TENANT_NAME}": member.get("tenant_name"),
                "{#WORKSPACE_NAME}": member.get("workspace_name") or member.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag("SWITCH"),
                "{#SWITCH_SERIAL}": member.get("switch_serial"),
                "{#SWITCH_NAME}": member.get("switch_name"),
                "{#STACK_MEMBER_ID}": member.get("member_id"),
                "{#STACK_MEMBER_NAME}": member.get("name") or member.get("serial") or member.get("member_id"),
                "{#STACK_MEMBER_SERIAL}": member.get("serial"),
                "{#SITE_ID}": member.get("site_id"),
                "{#SITE_NAME}": member.get("site_name"),
            }
            for member in members
            if member.get("tenant_id") and member.get("switch_serial") and non_empty(member.get("member_id"))
        ]
    }


def switch_hardware_lld(categories: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": category.get("tenant_id"),
                "{#TENANT_NAME}": category.get("tenant_name"),
                "{#WORKSPACE_NAME}": category.get("workspace_name") or category.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag("SWITCH"),
                "{#SWITCH_SERIAL}": category.get("switch_serial"),
                "{#SWITCH_NAME}": category.get("switch_name"),
                "{#HARDWARE_ID}": category.get("hardware_id"),
                "{#HARDWARE_NAME}": category.get("name") or category.get("hardware_id"),
                "{#HARDWARE_TYPE}": category.get("type"),
                "{#SITE_ID}": category.get("site_id"),
                "{#SITE_NAME}": category.get("site_name"),
            }
            for category in categories
            if category.get("tenant_id") and category.get("switch_serial") and non_empty(category.get("hardware_id"))
        ]
    }


def switch_vsx_lld(vsx_details: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": vsx.get("tenant_id"),
                "{#TENANT_NAME}": vsx.get("tenant_name"),
                "{#WORKSPACE_NAME}": vsx.get("workspace_name") or vsx.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag("SWITCH"),
                "{#SWITCH_SERIAL}": vsx.get("switch_serial"),
                "{#SWITCH_NAME}": vsx.get("switch_name"),
                "{#SITE_ID}": vsx.get("site_id"),
                "{#SITE_NAME}": vsx.get("site_name"),
            }
            for vsx in vsx_details
            if vsx.get("tenant_id") and vsx.get("switch_serial")
        ]
    }


def switch_hardware_trends_lld(trends: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": trend.get("tenant_id"),
                "{#TENANT_NAME}": trend.get("tenant_name"),
                "{#WORKSPACE_NAME}": trend.get("workspace_name") or trend.get("tenant_name"),
                "{#DEVICE_TYPE_TAG}": device_type_tag("SWITCH"),
                "{#SWITCH_SERIAL}": trend.get("switch_serial"),
                "{#SWITCH_NAME}": trend.get("switch_name"),
                "{#SITE_ID}": trend.get("site_id"),
                "{#SITE_NAME}": trend.get("site_name"),
            }
            for trend in trends
            if trend.get("tenant_id") and trend.get("switch_serial")
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


def licenses_lld(subscriptions: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "data": [
            {
                "{#TENANT_ID}": subscription.get("tenant_id"),
                "{#TENANT_NAME}": subscription.get("tenant_name"),
                "{#WORKSPACE_NAME}": subscription.get("workspace_name") or subscription.get("tenant_name"),
                "{#LICENSE_SCOPE}": subscription.get("license_scope"),
                "{#LICENSE_OWNER_ID}": subscription.get("owner_id"),
                "{#LICENSE_OWNER_NAME}": subscription.get("owner_name"),
                "{#DEVICE_TYPE_TAG}": license_device_type_tag(subscription.get("subscription_type")),
                "{#LICENSE_ID}": subscription.get("id"),
                "{#LICENSE_TYPE}": subscription.get("subscription_type"),
                "{#LICENSE_TIER}": subscription.get("tier"),
                "{#LICENSE_SKU}": subscription.get("sku"),
                "{#LICENSE_KEY_SUFFIX}": subscription.get("key_suffix"),
            }
            for subscription in subscriptions
            if subscription.get("license_scope") and subscription.get("owner_id") and subscription.get("id")
        ]
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HPE Aruba Central Next Gen multi-workspace collector for Zabbix")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("config-check", help="Validate workspaces.json without sending data")
    sub.add_parser("auth-check", help="Validate token creation for every workspace without sending data to Zabbix")
    sub.add_parser("summary", help="Print workspace, tenant, and device summary")
    sub.add_parser("plan-zabbix-hosts", help="Print planned Zabbix collector, site, and device hosts without calling the Zabbix API")
    import_template_parser = sub.add_parser("import-zabbix-template", help="Import or update the bundled Zabbix template through the Zabbix API")
    import_template_parser.add_argument("--apply", action="store_true", help="Actually import the template. Without this flag, only prints the import plan.")
    sync_parser = sub.add_parser("sync-zabbix-hosts", help="Create or update planned Zabbix host groups and hosts through the Zabbix API")
    sync_parser.add_argument("--apply", action="store_true", help="Actually create/update hosts. Without this flag, only prints the plan.")
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
                "config_status": collect_config_status(config),
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

    if args.command == "plan-zabbix-hosts":
        output_json(sync_zabbix_hosts(config, apply=False))
        return 0

    if args.command == "import-zabbix-template":
        output_json(import_zabbix_template(config, apply=bool(args.apply)))
        return 0

    if args.command == "sync-zabbix-hosts":
        output_json(sync_zabbix_hosts(config, apply=bool(args.apply)))
        return 0

    if args.command == "push-all":
        if not args.dry_run:
            sync_zabbix_hosts_if_configured(config)
        lines = build_all_config_sender_lines(config)
        if args.dry_run:
            print("\n".join(lines))
        else:
            output_json(run_zabbix_sender_discovery_first(lines))
        return 0

    parser.error("Unknown command")
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CentralError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=True), file=sys.stderr)
        raise SystemExit(1)
