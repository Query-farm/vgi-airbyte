"""Resolve, install and run Airbyte source connectors.

A connector reference (the first argument of every connector function) is one of:

==============================================  ==========================================
``source-pokeapi`` / ``pokeapi`` / ``...@0.3.79``  Airbyte registry name (optional version).
                                                Manifest-only connectors run in-process-
                                                style through the declarative CDK; others
                                                run their Docker image.
``/path/to/manifest.yaml`` / ``manifest:...``   A local declarative manifest.
``https://.../manifest.yaml``                   A declarative manifest at a URL.
``docker:airbyte/source-faker:6.2.0``           Any image speaking the Airbyte protocol.
``exec:/path/to/source``                        A local executable speaking the protocol.
==============================================  ==========================================

**Declarative manifests** run with the Airbyte CDK's ``source-declarative-manifest``
command, installed once per CDK version into its own cached virtual environment
(``airbyte-cdk`` pulls in pandas, nltk and google-cloud libraries, so it never
goes into the worker's environment). Manifests that reference custom Python
components (``components.py``) only run with ``allow_custom_code := true``.

**Docker images** run with ``docker run --rm -i``; the config, catalog and state
files are mounted read-only from a private temporary directory. Without a
working Docker daemon those connectors fail with a clear error.

Every connector speaks the same CLI: ``spec``, ``check --config``, ``discover
--config`` and ``read --config --catalog [--state]``, printing AirbyteMessages
on stdout. Config files are 0600, live in a 0700 directory, and are deleted as
soon as the connector exits.
"""

from __future__ import annotations

import atexit
import collections
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any, NoReturn

import jsonschema
import yaml

from . import registry as reg
from .protocol import ConnectorError, error_text, parse_message

log = logging.getLogger(__name__)

DEFAULT_CDK_VERSION = "7.48.0"
CDK_PYTHON = "3.13"
DEFAULT_TIMEOUT_SECONDS = 300
QUEUE_LINES = 2_000
STDERR_LINES = 40
DISCOVERY_CACHE_SECONDS = 300
IDLE_RUN_SECONDS = 600
CONTAINER_DIR = "/vgi-airbyte"
_EOF = object()
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


# ------------------------------------------------------------------ references


@dataclass(frozen=True, slots=True)
class ConnectorRef:
    """A parsed connector reference."""

    kind: str  # registry | manifest | docker | exec
    target: str
    version: str = ""

    @property
    def is_url(self) -> bool:
        """Whether a manifest reference is a URL."""
        return self.target.startswith(("http://", "https://"))


def parse_ref(text: str) -> ConnectorRef:
    """Parse a connector reference; raises ValueError for an unusable one."""
    raw = text.strip()
    if not raw:
        raise ValueError("connector must not be empty: use a registry name like 'source-pokeapi' or a manifest path")
    for prefix, kind in (("docker:", "docker"), ("exec:", "exec"), ("manifest:", "manifest")):
        if raw.startswith(prefix):
            target = raw[len(prefix) :].strip()
            if not target:
                raise ValueError(f"{prefix} needs a value, e.g. {prefix}{'airbyte/source-faker:6.2.0'}")
            return ConnectorRef(kind, target)
    if raw.startswith(("http://", "https://")) or raw.lower().endswith((".yaml", ".yml")):
        return ConnectorRef("manifest", raw)
    if "/" in raw or raw.startswith(("~", ".")):
        path = Path(raw).expanduser()
        if path.is_file() and os.access(path, os.X_OK):
            return ConnectorRef("exec", raw)
        raise ValueError(
            f"connector {raw!r} looks like a path but is not a .yaml manifest or an executable file; "
            "use manifest:/path, exec:/path or docker:image"
        )
    name, _, version = raw.partition("@")
    if not _NAME_RE.match(name) or (version and not _NAME_RE.match(version)):
        raise ValueError(
            f"connector {raw!r} is not a registry name such as 'source-pokeapi' or 'source-pokeapi@0.3.79'"
        )
    return ConnectorRef("registry", name, version)


# ------------------------------------------------------------------ resolution


@dataclass(frozen=True, slots=True)
class Options:
    """Per-call settings that change how a connector is resolved and run (no credentials)."""

    registry_url: str = reg.DEFAULT_REGISTRY_URL
    cdk_version: str = DEFAULT_CDK_VERSION
    accept_license: str = ""
    allow_custom_code: bool = False
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS


@dataclass(frozen=True, slots=True)
class Resolved:
    """A connector ready to run."""

    reference: str
    mode: str  # manifest | docker | exec
    name: str
    entry: reg.RegistryEntry | None = None
    manifest: dict[str, Any] | None = None
    manifest_path: str = ""
    manifest_source: str = ""
    components_path: str = ""
    image: str = ""
    executable: str = ""
    cdk_version: str = DEFAULT_CDK_VERSION

    @property
    def key(self) -> str:
        """Identity of the connector code (for caches)."""
        raw = json.dumps([self.mode, self.manifest_path, self.components_path, self.image, self.executable])
        return hashlib.sha256((raw + self.cdk_version).encode()).hexdigest()[:16]

    @property
    def license(self) -> str:
        """The registry license, or empty for connectors named directly."""
        return self.entry.license if self.entry else ""


def _uses_custom_code(node: Any) -> bool:
    if isinstance(node, dict):
        cls = node.get("class_name")
        if isinstance(cls, str) and "components." in cls:
            return True
        return any(_uses_custom_code(v) for v in node.values())
    if isinstance(node, list):
        return any(_uses_custom_code(v) for v in node)
    return False


def _parse_manifest(text: str, where: str) -> dict[str, Any]:
    try:
        manifest = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ValueError(f"the manifest at {where} is not valid YAML: {exc}") from None
    if not isinstance(manifest, dict) or not (manifest.get("streams") or manifest.get("dynamic_streams")):
        raise ValueError(f"{where} is not a declarative Airbyte manifest (no 'streams')")
    return manifest


def _materialize(text: str, suffix: str) -> str:
    """Write downloaded connector code to the cache, content-addressed."""
    digest = hashlib.sha256(text.encode()).hexdigest()[:24]
    path = reg.cache_root() / "code" / f"{digest}{suffix}"
    if not path.is_file():
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(text)
        tmp.replace(path)
    return str(path)


def _sibling(location: str, name: str) -> str:
    if location.startswith(("http://", "https://")):
        return location.rsplit("/", 1)[0] + "/" + name
    return str(Path(location).expanduser().resolve().parent / name)


def _load_components(location: str, opts: Options, connector: str) -> str:
    if not opts.allow_custom_code:
        raise ValueError(
            f"{connector} uses custom Python components (components.py), which run arbitrary code; pass "
            "allow_custom_code := true to run them, or use the connector's Docker image (docker:...)"
        )
    url = _sibling(location, "components.py")
    try:
        text = reg.fetch_text(url) if url.startswith(("http://", "https://")) else Path(url).read_text()
    except (reg.RegistryError, OSError) as exc:
        raise ValueError(f"{connector} references custom components but {url} could not be read: {exc}") from None
    return _materialize(text, ".components.py")


def _manifest_resolved(reference: str, name: str, location: str, opts: Options, **extra: Any) -> Resolved:
    if location.startswith(("http://", "https://")):
        text = reg.fetch_text(location)
        path = _materialize(text, ".manifest.yaml")
    else:
        local = Path(location).expanduser()
        if not local.is_file():
            raise ValueError(f"manifest file {location!r} does not exist")
        text = local.read_text()
        path = str(local.resolve())
    manifest = _parse_manifest(text, location)
    components = _load_components(location, opts, name) if _uses_custom_code(manifest) else ""
    return Resolved(
        reference=reference,
        mode="manifest",
        name=name,
        manifest=manifest,
        manifest_path=path,
        manifest_source=location,
        components_path=components,
        cdk_version=opts.cdk_version or DEFAULT_CDK_VERSION,
        **extra,
    )


_resolve_cache: dict[str, tuple[float, Resolved]] = {}
_resolve_lock = threading.Lock()


def resolve(reference: str, opts: Options) -> Resolved:
    """Resolve a reference (registry lookup, license check, manifest download)."""
    ref = parse_ref(reference)
    cache_key = json.dumps(
        [reference, opts.registry_url, opts.cdk_version, opts.accept_license, opts.allow_custom_code]
    )
    cacheable = ref.kind == "registry" or (ref.kind == "manifest" and ref.is_url)
    if cacheable:
        with _resolve_lock:
            hit = _resolve_cache.get(cache_key)
            if hit and time.monotonic() - hit[0] < DISCOVERY_CACHE_SECONDS:
                return hit[1]
    resolved = _resolve(reference, ref, opts)
    if cacheable:
        with _resolve_lock:
            _resolve_cache[cache_key] = (time.monotonic(), resolved)
    return resolved


def _resolve(reference: str, ref: ConnectorRef, opts: Options) -> Resolved:
    if ref.kind == "manifest":
        name = ref.target.rstrip("/").rsplit("/", 1)[-1]
        return _manifest_resolved(reference, name, ref.target, opts)
    if ref.kind == "docker":
        return Resolved(reference=reference, mode="docker", name=ref.target, image=ref.target)
    if ref.kind == "exec":
        path = Path(ref.target).expanduser()
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"executable {ref.target!r} is not an executable file")
        return Resolved(reference=reference, mode="exec", name=path.name, executable=str(path.resolve()))
    entry = reg.find_connector(ref.target, opts.registry_url)
    reg.check_license(entry, opts.accept_license)
    version = ref.version or entry.docker_image_tag
    if entry.manifest_only:
        url = reg.manifest_url(entry, opts.registry_url, version)
        return _manifest_resolved(reference, entry.name, url, opts, entry=entry)
    return Resolved(
        reference=reference,
        mode="docker",
        name=entry.name,
        entry=entry,
        image=f"{entry.docker_repository}:{version}",
    )


# ------------------------------------------------------------- environments


def _run_install(cmd: list[str], what: str) -> None:
    log.info("airbyte: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=1800, check=False)
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip().splitlines()[-15:]
        raise ConnectorError(f"could not {what} (exit {proc.returncode}):\n" + "\n".join(tail))


def cdk_command(version: str) -> str:
    """Return the ``source-declarative-manifest`` executable of an isolated, cached airbyte-cdk install."""
    if not _NAME_RE.match(version):
        raise ValueError(f"cdk_version {version!r} is not a version number")
    env_dir = reg.cache_root() / "cdk" / version
    exe = env_dir / "venv" / "bin" / "source-declarative-manifest"
    if exe.is_file():
        return str(exe)
    env_dir.mkdir(parents=True, exist_ok=True)
    with open(env_dir / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if exe.is_file():
            return str(exe)
        venv = env_dir / "venv"
        shutil.rmtree(venv, ignore_errors=True)
        requirement = f"airbyte-cdk=={version}"
        uv = shutil.which("uv")
        if uv:
            _run_install(
                [uv, "venv", "--quiet", "--python", CDK_PYTHON, str(venv)], "create the airbyte-cdk environment"
            )
            _run_install(
                [uv, "pip", "install", "--quiet", "--python", str(venv / "bin" / "python"), requirement],
                f"install {requirement}",
            )
        else:
            _run_install([sys.executable, "-m", "venv", str(venv)], "create the airbyte-cdk environment")
            _run_install([str(venv / "bin" / "pip"), "install", "--quiet", requirement], f"install {requirement}")
        if not exe.is_file():
            shutil.rmtree(venv, ignore_errors=True)
            raise ConnectorError(f"installing {requirement} did not provide source-declarative-manifest")
    return str(exe)


_docker_ok: bool | None = None


def docker_available() -> bool:
    """Whether a Docker daemon answers (checked once per process)."""
    global _docker_ok
    if _docker_ok is None:
        docker = shutil.which("docker")
        if docker is None:
            _docker_ok = False
        else:
            try:
                proc = subprocess.run(
                    [docker, "info", "--format", "{{.ServerVersion}}"],
                    capture_output=True,
                    timeout=20,
                    check=False,
                )
                _docker_ok = proc.returncode == 0
            except OSError, subprocess.TimeoutExpired:
                _docker_ok = False
    return _docker_ok


# ------------------------------------------------------------------ processes


def _private_dir() -> str:
    path = tempfile.mkdtemp(prefix="vgi-airbyte-")
    os.chmod(path, 0o700)
    return path


def _write_private(directory: str, name: str, content: str) -> str:
    path = os.path.join(directory, name)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


def _command_line(
    resolved: Resolved, command: str, files: dict[str, str], container: str
) -> tuple[list[str], dict[str, str] | None]:
    """Argv (and environment) for one protocol command; ``files`` maps flag -> host path."""
    env: dict[str, str] | None = None
    if resolved.mode == "docker":
        docker = shutil.which("docker")
        if docker is None or not docker_available():
            raise ConnectorError(
                f"{resolved.name} runs as the Docker image {resolved.image}, but no Docker daemon is available"
            )
        host_dir = os.path.dirname(next(iter(files.values()))) if files else ""
        argv = [docker, "run", "--rm", "-i", "--name", container]
        if host_dir:
            argv += ["-v", f"{host_dir}:{CONTAINER_DIR}:ro"]
        argv += [resolved.image, command]
        for flag, path in files.items():
            argv += [flag, f"{CONTAINER_DIR}/{os.path.basename(path)}"]
        return argv, env
    program = resolved.executable if resolved.mode == "exec" else cdk_command(resolved.cdk_version)
    argv = [program, command]
    for flag, path in files.items():
        argv += [flag, path]
    if resolved.mode == "manifest":
        argv += ["--manifest-path", resolved.manifest_path]
        if resolved.components_path:
            argv += ["--components-path", resolved.components_path]
            env = {**os.environ, "AIRBYTE_ENABLE_UNSAFE_CODE": "true"}
    return argv, env


@dataclass
class ConnectorRun:
    """One running connector process."""

    name: str
    proc: subprocess.Popen[str]
    directory: str
    container: str = ""
    lines: queue.Queue[object] = field(default_factory=lambda: queue.Queue(maxsize=QUEUE_LINES))
    stderr: collections.deque[str] = field(default_factory=lambda: collections.deque(maxlen=STDERR_LINES))
    error_logs: collections.deque[str] = field(default_factory=lambda: collections.deque(maxlen=5))
    errors: list[str] = field(default_factory=list)
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    last_used: float = field(default_factory=time.monotonic)
    last_output: float = field(default_factory=time.monotonic)
    finished: bool = False
    # Scan position: the cumulative state seen so far and the items read since the last STATE.
    last_state: str = ""
    since_state: int = 0
    tracker: Any = None

    @classmethod
    def start(
        cls,
        resolved: Resolved,
        command: str,
        *,
        config: str | None = None,
        catalog: dict[str, Any] | None = None,
        state: str | None = None,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> ConnectorRun:
        """Write the private files and start ``<connector> <command> ...``."""
        directory = _private_dir()
        files: dict[str, str] = {}
        try:
            if config is not None:
                files["--config"] = _write_private(directory, "config.json", config)
            if catalog is not None:
                files["--catalog"] = _write_private(directory, "catalog.json", json.dumps(catalog))
            if state:
                files["--state"] = _write_private(directory, "state.json", state)
            container = f"vgi-airbyte-{uuid.uuid4().hex[:12]}" if resolved.mode == "docker" else ""
            argv, env = _command_line(resolved, command, files, container)
            log.debug("airbyte: starting %s %s", resolved.name, command)
            proc = subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
                cwd=directory,
            )
        except OSError as exc:
            shutil.rmtree(directory, ignore_errors=True)
            raise ConnectorError(f"could not start {resolved.name}: {exc}") from exc
        except BaseException:
            shutil.rmtree(directory, ignore_errors=True)
            raise
        run = cls(
            name=resolved.name, proc=proc, directory=directory, container=container, timeout_seconds=timeout_seconds
        )
        assert proc.stdout is not None and proc.stderr is not None
        threading.Thread(target=run._pump_stdout, args=(proc.stdout,), daemon=True).start()
        threading.Thread(target=run._pump_stderr, args=(proc.stderr,), daemon=True).start()
        return run

    def _pump_stdout(self, stream: IO[str]) -> None:
        try:
            for line in stream:
                self.lines.put(line)
        finally:
            self.lines.put(_EOF)

    def _pump_stderr(self, stream: IO[str]) -> None:
        for line in stream:
            self.stderr.append(line.rstrip("\n"))

    def _raise_failure(self, code: int) -> NoReturn:
        if self.errors:
            detail = self.errors[0]
        elif self.error_logs:
            detail = self.error_logs[-1]
        else:
            detail = "stderr:\n" + ("\n".join(self.stderr) or "(no stderr output)")
        raise ConnectorError(f"{self.name} failed (exit code {code}): {detail}")

    def poll(self, wait: float) -> dict[str, Any] | bool | None:
        """Return the next message; None at the end; False if nothing arrived within ``wait`` seconds.

        LOG and TRACE messages are returned too; ERROR traces are remembered and
        raised when the connector exits.

        Args:
            wait: Seconds to wait for a line before returning False.

        Returns:
            The decoded message, None once the connector exited cleanly, or False.

        Raises:
            ConnectorError: The connector printed nothing for ``timeout_seconds``, failed,
                or reported an ERROR trace.

        """
        while True:
            if self.finished:
                return None
            try:
                item = self.lines.get(timeout=wait)
            except queue.Empty:
                if self.timeout_seconds and time.monotonic() - self.last_output > self.timeout_seconds:
                    self.close()
                    raise ConnectorError(
                        f"{self.name} printed nothing for {self.timeout_seconds} seconds and was stopped; "
                        "raise timeout_seconds for slow sources"
                    ) from None
                return False
            self.last_output = self.last_used = time.monotonic()
            if item is _EOF:
                self.finished = True
                code = self.proc.wait()
                time.sleep(0.05)  # let the stderr thread catch up
                self._cleanup()
                if code != 0 or self.errors:
                    self._raise_failure(code)
                return None
            assert isinstance(item, str)
            msg = parse_message(item)
            if msg is None:
                continue
            err = error_text(msg)
            if err is not None:
                self.errors.append(err)
            elif msg["type"] == "LOG":
                entry = msg.get("log") or {}
                if str(entry.get("level", "")).upper() in ("ERROR", "FATAL"):
                    self.error_logs.append(str(entry.get("message", ""))[:600])
            return msg

    def next_message(self) -> dict[str, Any] | None:
        """Return the next message, or None at the end (waits up to ``timeout_seconds``)."""
        while True:
            msg = self.poll(1.0)
            if msg is not False:
                assert msg is None or isinstance(msg, dict)
                return msg

    def _cleanup(self) -> None:
        if self.container:
            docker = shutil.which("docker")
            if docker:
                with contextlib.suppress(Exception):
                    subprocess.run([docker, "rm", "-f", self.container], capture_output=True, timeout=30, check=False)
            self.container = ""
        shutil.rmtree(self.directory, ignore_errors=True)

    def close(self) -> None:
        """Stop the connector (if still running) and delete its files."""
        self.finished = True
        if self.proc.poll() is None:
            self.proc.kill()
            with contextlib.suppress(Exception):
                self.proc.wait(timeout=5)
        self._cleanup()


def run_to_end(
    resolved: Resolved,
    command: str,
    *,
    config: str | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    """Run a short command (spec / check / discover) and return its messages."""
    run = ConnectorRun.start(resolved, command, config=config, timeout_seconds=timeout_seconds)
    out: list[dict[str, Any]] = []
    try:
        while (msg := run.next_message()) is not None:
            out.append(msg)
    finally:
        run.close()
    return out


def _reply(messages: list[dict[str, Any]], kind: str, resolved: Resolved, command: str) -> dict[str, Any]:
    for msg in messages:
        if msg.get("type") == kind:
            return msg
    raise ConnectorError(f"{resolved.name} {command} printed no {kind} message")


# ------------------------------------------------------------- spec / config

_spec_cache: dict[str, dict[str, Any]] = {}


def spec(resolved: Resolved, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Return the ConnectorSpecification (``connectionSpecification``, ``documentationUrl``, ...)."""
    if resolved.mode == "manifest":
        raw = (resolved.manifest or {}).get("spec") or {}
        return {
            "documentationUrl": raw.get("documentation_url") or raw.get("documentationUrl"),
            "connectionSpecification": raw.get("connection_specification")
            or raw.get("connectionSpecification")
            or {"type": "object", "properties": {}},
            "advanced_auth": raw.get("advanced_auth"),
        }
    if resolved.entry is not None and resolved.entry.spec.get("connectionSpecification"):
        return resolved.entry.spec
    hit = _spec_cache.get(resolved.key)
    if hit is not None:
        return hit
    msg = _reply(run_to_end(resolved, "spec", timeout_seconds=timeout_seconds), "SPEC", resolved, "spec")
    value = msg.get("spec")
    if not isinstance(value, dict):
        raise ConnectorError(f"{resolved.name} spec printed a SPEC message without a spec")
    _spec_cache[resolved.key] = value
    return value


def secret_fields(schema: Any, prefix: str = "") -> list[str]:
    """Dotted paths of the properties marked ``airbyte_secret`` in a connection specification."""
    out: list[str] = []
    if not isinstance(schema, dict):
        return out
    for name, sub in (schema.get("properties") or {}).items():
        if not isinstance(sub, dict):
            continue
        path = f"{prefix}{name}"
        if sub.get("airbyte_secret"):
            out.append(path)
        out.extend(secret_fields(sub, path + "."))
        for key in ("oneOf", "anyOf"):
            for alt in sub.get(key) or []:
                out.extend(p for p in secret_fields(alt, path + ".") if p not in out)
    return out


def validate_config(config: dict[str, Any], connection_spec: dict[str, Any], connector: str) -> None:
    """Raise ValueError listing every way ``config`` breaks the spec; never quotes a config value."""
    try:
        validator = jsonschema.Draft7Validator(connection_spec)
        found = sorted(validator.iter_errors(config), key=lambda e: list(e.absolute_path))
    except jsonschema.SchemaError:
        log.warning("airbyte: %s has an invalid connection specification; config not validated", connector)
        return
    problems: list[str] = []
    for err in found[:10]:
        where = ".".join(str(p) for p in err.absolute_path) or "(config)"
        if err.validator == "required":
            missing = re.findall(r"'([^']+)' is a required property", err.message)
            name = f"{where}.{missing[0]}" if missing and where != "(config)" else (missing[0] if missing else where)
            problems.append(f"missing required field {name!r}")
        elif err.validator == "type":
            problems.append(f"{where} must be of type {err.validator_value}")
        elif err.validator == "enum":
            problems.append(f"{where} must be one of {err.validator_value}")
        elif err.validator in ("oneOf", "anyOf"):
            problems.append(f"{where} matches none of the allowed shapes")
        else:
            problems.append(f"{where} fails the spec's {err.validator!r} rule ({err.validator_value!r})")
    if problems:
        raise ValueError(
            f"the airbyte secret's config does not match {connector}'s spec: "
            + "; ".join(problems)
            + ". See airbyte.main.spec('<connector>') for the fields."
        )


def check(resolved: Resolved, config: str, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Run ``check`` and return the ``connectionStatus`` object."""
    msgs = run_to_end(resolved, "check", config=config, timeout_seconds=timeout_seconds)
    msg = _reply(msgs, "CONNECTION_STATUS", resolved, "check")
    status = msg.get("connectionStatus")
    if not isinstance(status, dict):
        raise ConnectorError(f"{resolved.name} check printed a CONNECTION_STATUS without a status")
    return status


_discovery_cache: dict[str, tuple[float, dict[str, Any]]] = {}
_discovery_lock = threading.Lock()


def discover(resolved: Resolved, config: str, timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS) -> dict[str, Any]:
    """Run ``discover`` and return the AirbyteCatalog (cached briefly per connector and config)."""
    cache_key = hashlib.sha256(json.dumps([resolved.key, config]).encode()).hexdigest()
    with _discovery_lock:
        hit = _discovery_cache.get(cache_key)
        if hit and time.monotonic() - hit[0] < DISCOVERY_CACHE_SECONDS:
            cached: dict[str, Any] = json.loads(json.dumps(hit[1]))
            return cached
    msgs = run_to_end(resolved, "discover", config=config, timeout_seconds=timeout_seconds)
    catalog = _reply(msgs, "CATALOG", resolved, "discover").get("catalog")
    if not isinstance(catalog, dict) or not isinstance(catalog.get("streams"), list):
        raise ConnectorError(f"{resolved.name} discover printed a CATALOG without streams")
    with _discovery_lock:
        _discovery_cache[cache_key] = (time.monotonic(), catalog)
    fresh: dict[str, Any] = json.loads(json.dumps(catalog))
    return fresh


# ------------------------------------------------- live runs, keyed by scan token

_runs: dict[str, ConnectorRun] = {}
_runs_lock = threading.Lock()


def take_run(token: str) -> ConnectorRun | None:
    """Return the live run of a scan in this process, if any."""
    with _runs_lock:
        _reap_locked()
        return _runs.get(token)


def keep_run(token: str, run: ConnectorRun) -> None:
    """Remember a live run so the scan's next batch continues it."""
    with _runs_lock:
        _runs[token] = run


def drop_run(token: str) -> None:
    """Forget and stop a scan's run."""
    with _runs_lock:
        run = _runs.pop(token, None)
    if run is not None:
        run.close()


def _reap_locked() -> None:
    """Stop runs nobody has read from for a while (a LIMIT query stops reading early)."""
    now = time.monotonic()
    for token, run in list(_runs.items()):
        if now - run.last_used > IDLE_RUN_SECONDS:
            run.close()
            del _runs[token]


@atexit.register
def _close_all() -> None:
    with _runs_lock:
        for run in _runs.values():
            run.close()
        _runs.clear()
