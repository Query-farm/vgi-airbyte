"""Airbyte's public connector registry, manifest download, and the license policy.

The registry (``oss_registry.json``) lists every published source with its
Docker image, release stage, support level, language and **license**.
Manifest-only connectors (``language: manifest-only``) publish their
declarative ``manifest.yaml`` next to their metadata::

    https://connectors.airbyte.com/files/registries/v0/oss_registry.json
    https://connectors.airbyte.com/files/metadata/airbyte/<name>/<version>/manifest.yaml

The manifest URL is derived from the registry URL (everything before
``/registries/``), so a mirror with the same layout works unchanged.

**License policy.** Running a connector is using its code, so every connector
resolved from the registry is checked before it runs:

* permissive (MIT, Apache-2.0, BSD, ISC, MIT-0): runs.
* ELv2 (Elastic License 2.0, most Airbyte connectors): runs only when the
  query acknowledges it with ``accept_license := 'ELv2'``. ELv2 allows running
  the connector for your own data, but not offering it to third parties as a
  hosted or managed service; the acknowledgement makes that a deliberate choice.
* ``Airbyte Enterprise``: refused; it needs a commercial Airbyte license.
* anything else, or no license: runs only with ``accept_license :=`` naming
  that exact license string.

Local manifests, Docker images and executables named directly are the user's
own choice of code; the worker cannot know their license and does not check it.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_REGISTRY_URL = "https://connectors.airbyte.com/files/registries/v0/oss_registry.json"
REGISTRY_TTL_SECONDS = 3600
HTTP_TIMEOUT_SECONDS = 60
USER_AGENT = "vgi-airbyte-worker/0.1 (+https://github.com/Query-farm/vgi-airbyte)"

PERMISSIVE_LICENSES = {"mit", "mit-0", "apache-2.0", "apache 2.0", "bsd-2-clause", "bsd-3-clause", "isc", "0bsd"}
ELV2 = "elv2"
REFUSED_LICENSES = {"airbyte enterprise"}


class RegistryError(RuntimeError):
    """The registry or a manifest could not be fetched or used."""


class LicenseError(RuntimeError):
    """The license policy does not allow running a connector."""


def cache_root() -> Path:
    """Where the worker caches environments, the registry and manifests."""
    explicit = os.environ.get("VGI_AIRBYTE_CACHE_DIR")
    if explicit:
        return Path(explicit)
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "vgi-airbyte"
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "vgi-airbyte"


def http_get(url: str) -> bytes:
    """GET a URL (http(s) or file) and return the body; raises RegistryError with the status."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
            body: bytes = response.read()
            return body
    except urllib.error.HTTPError as exc:
        raise RegistryError(f"GET {url} failed with HTTP {exc.code}") from None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        reason = getattr(exc, "reason", exc)
        raise RegistryError(f"GET {url} failed: {reason}") from None


# ------------------------------------------------------------------ registry


@dataclass(frozen=True, slots=True)
class RegistryEntry:
    """One source of the registry, reduced to what the worker uses."""

    name: str
    display_name: str
    definition_id: str
    docker_repository: str
    docker_image_tag: str
    language: str
    release_stage: str
    support_level: str
    license: str
    documentation_url: str
    source_type: str
    tags: tuple[str, ...]
    spec: dict[str, Any]

    @property
    def manifest_only(self) -> bool:
        """Whether the connector is a declarative manifest that runs without its image."""
        return self.language == "manifest-only" or "language:manifest-only" in self.tags

    @property
    def license_class(self) -> str:
        """``permissive``, ``elv2``, ``refused`` or ``other``."""
        return license_class(self.license)


def license_class(license_name: str) -> str:
    """Classify a license string for the policy."""
    key = license_name.strip().lower()
    if key in PERMISSIVE_LICENSES:
        return "permissive"
    if key == ELV2:
        return "elv2"
    if key in REFUSED_LICENSES:
        return "refused"
    return "other"


def license_policy(license_name: str) -> str:
    """Human-readable policy for a license, as shown by ``connectors()``."""
    kind = license_class(license_name)
    if kind == "permissive":
        return "runs"
    if kind == "elv2":
        return "requires accept_license := 'ELv2'"
    if kind == "refused":
        return "refused (commercial license)"
    return f"requires accept_license := '{license_name}'" if license_name else "requires accept_license := 'unknown'"


def check_license(entry: RegistryEntry, accept_license: str) -> None:
    """Raise LicenseError unless the policy allows running ``entry``."""
    kind = entry.license_class
    if kind == "permissive":
        return
    name = entry.license or "unknown"
    if kind == "refused":
        raise LicenseError(
            f"{entry.name} is licensed under {name!r}, which needs a commercial agreement with Airbyte; "
            "the worker does not run it"
        )
    if accept_license.strip().lower() == name.strip().lower():
        log.info("airbyte: running %s under %s (acknowledged by accept_license)", entry.name, name)
        return
    if kind == "elv2":
        raise LicenseError(
            f"{entry.name} is licensed under the Elastic License 2.0 (ELv2). ELv2 lets you run it on your own "
            "data but forbids providing it to third parties as a hosted or managed service. Acknowledge the "
            "license with accept_license := 'ELv2' to run it."
        )
    raise LicenseError(
        f"{entry.name} has license {name!r}, which this worker does not recognise as permissive; "
        f"review it and pass accept_license := '{name}' to run the connector."
    )


def _entry(raw: dict[str, Any]) -> RegistryEntry | None:
    repo = str(raw.get("dockerRepository") or "")
    if not repo:
        return None
    spec = raw.get("spec") if isinstance(raw.get("spec"), dict) else {}
    assert isinstance(spec, dict)
    tags = raw.get("tags") if isinstance(raw.get("tags"), list) else []
    assert isinstance(tags, list)
    return RegistryEntry(
        name=repo.split("/")[-1],
        display_name=str(raw.get("name") or ""),
        definition_id=str(raw.get("sourceDefinitionId") or ""),
        docker_repository=repo,
        docker_image_tag=str(raw.get("dockerImageTag") or "latest"),
        language=str(raw.get("language") or ""),
        release_stage=str(raw.get("releaseStage") or ""),
        support_level=str(raw.get("supportLevel") or ""),
        license=str(raw.get("license") or ""),
        documentation_url=str(raw.get("documentationUrl") or ""),
        source_type=str(raw.get("sourceType") or ""),
        tags=tuple(str(t) for t in tags),
        spec=spec,
    )


_registry_cache: dict[str, tuple[float, list[RegistryEntry]]] = {}
_registry_lock = threading.Lock()


def _registry_file(url: str) -> Path:
    return cache_root() / "registry" / (hashlib.sha256(url.encode()).hexdigest()[:16] + ".json")


def load_registry(url: str = DEFAULT_REGISTRY_URL) -> list[RegistryEntry]:
    """Return the registry's sources, cached for an hour; a stale disk copy is used when offline."""
    url = url or DEFAULT_REGISTRY_URL
    with _registry_lock:
        hit = _registry_cache.get(url)
        if hit and time.monotonic() - hit[0] < REGISTRY_TTL_SECONDS:
            return hit[1]
    disk = _registry_file(url)
    try:
        body = http_get(url)
    except RegistryError:
        if not disk.is_file():
            raise
        log.warning("airbyte: registry %s unreachable, using the cached copy %s", url, disk)
        body = disk.read_bytes()
    else:
        disk.parent.mkdir(parents=True, exist_ok=True)
        tmp = disk.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_bytes(body)
        tmp.replace(disk)
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RegistryError(f"the registry at {url} is not JSON: {exc}") from None
    sources = data.get("sources") if isinstance(data, dict) else None
    if not isinstance(sources, list):
        raise RegistryError(f"the registry at {url} has no 'sources' list")
    entries = [e for e in (_entry(s) for s in sources if isinstance(s, dict)) if e is not None]
    entries.sort(key=lambda e: e.name)
    with _registry_lock:
        _registry_cache[url] = (time.monotonic(), entries)
    return entries


def find_connector(name: str, url: str = DEFAULT_REGISTRY_URL) -> RegistryEntry:
    """Return the registry entry named ``name`` (``source-`` prefix optional)."""
    wanted = name.strip().lower()
    candidates = {wanted, f"source-{wanted}"}
    entries = load_registry(url)
    for entry in entries:
        if entry.name.lower() in candidates:
            return entry
    close = [e.name for e in entries if wanted.removeprefix("source-") in e.name][:8]
    hint = f"; similar: {', '.join(close)}" if close else ""
    raise RegistryError(f"no source named {name!r} in the Airbyte registry{hint}")


def files_base(registry_url: str) -> str:
    """Return the ``.../files/`` base the manifests live under, derived from the registry URL."""
    url = registry_url or DEFAULT_REGISTRY_URL
    marker = "/registries/"
    if marker not in url:
        raise RegistryError(f"cannot derive the manifest location from registry URL {url!r}")
    return url.split(marker, 1)[0]


def manifest_url(entry: RegistryEntry, registry_url: str, version: str = "") -> str:
    """URL of a manifest-only connector's ``manifest.yaml`` at a version (default: the registry's)."""
    tag = version or entry.docker_image_tag
    return f"{files_base(registry_url)}/metadata/{entry.docker_repository}/{tag}/manifest.yaml"


def fetch_text(url: str) -> str:
    """Download a manifest (or components.py), caching versioned URLs on disk."""
    cacheable = "/latest/" not in url and url.startswith(("http://", "https://"))
    disk = cache_root() / "manifests" / (hashlib.sha256(url.encode()).hexdigest()[:24])
    if cacheable and disk.is_file():
        return disk.read_text()
    text = http_get(url).decode("utf-8")
    if cacheable:
        disk.parent.mkdir(parents=True, exist_ok=True)
        tmp = disk.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(text)
        tmp.replace(disk)
    return text
