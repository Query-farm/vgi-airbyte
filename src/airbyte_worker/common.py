"""Arguments, secrets and the resumable scan engine shared by the connector functions.

**Secrets.** A connector's config JSON comes from a DuckDB secret of type
``airbyte``::

    CREATE SECRET poke (TYPE airbyte, CONFIG '{"pokemon_name": "pikachu"}', SCOPE 'airbyte://source-pokeapi');

The worker asks DuckDB for the ``airbyte`` secret whose scope best matches
``airbyte://<connector>`` (the connector argument exactly as written, e.g.
``airbyte:///path/to/manifest.yaml`` or ``airbyte://docker:airbyte/source-faker:6.2.0``);
a secret with no scope applies to every connector. Name one explicitly with
``secret_name := 'poke'``. Without any secret the connector gets ``{}``. The
config is validated against the connector's spec before the connector runs.

**Resumable scans.** The scan cursor (``ScanCursor``) holds the cumulative
Airbyte state (the merged STATE messages so far) and how many records were read
after the last STATE, never the config. In one worker process the live
connector simply continues between batches. When a batch lands on another
process (HTTP) the connector is restarted with ``--state <cumulative state>``
and the records already returned after that checkpoint are skipped. This
relies on the connector replaying the same records from the same state, which
is the Airbyte checkpointing contract.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.catalog import SecretTypeSpec
from vgi_rpc import ArrowSerializableDataclass

from . import connector as cn
from . import registry as reg
from .protocol import ConnectorError, StateTracker, parse_state

SECRET_TYPE = "airbyte"
SCOPE_PREFIX = "airbyte://"
BATCH_ROWS = 1_000
FLUSH_SECONDS = 2.0
DOC_URL = "https://github.com/Query-farm/vgi-airbyte#secrets"

AIRBYTE_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Config JSON for an Airbyte source connector, validated against its spec and passed as --config.",
    schema=pa.schema(
        [
            pa.field(
                "config",
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_required": "true",
                    "vgi_doc": (
                        "The connector's config as a JSON object, exactly what it reads from --config "
                        "(see airbyte.main.spec). Bind it to one connector with SCOPE 'airbyte://<connector>'."
                    ),
                },
            )
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)


@dataclass(slots=True, frozen=True, kw_only=True)
class ConnectorArgs:
    """Arguments every connector-running function takes."""

    connector: Annotated[
        str,
        Arg(
            0,
            doc="Connector: a registry name ('source-pokeapi', optionally '@version'), a manifest path or URL "
            "(.yaml), 'docker:<image>' or 'exec:<path>'.",
        ),
    ]
    secret_name: Annotated[
        str,
        Arg(
            "secret_name",
            default="",
            doc="Name of the airbyte secret holding the config; empty picks the one scoped to 'airbyte://<connector>'.",
        ),
    ]
    accept_license: Annotated[
        str,
        Arg(
            "accept_license",
            default="",
            doc="Acknowledge a registry connector's non-permissive license to run it, e.g. 'ELv2'.",
        ),
    ]
    allow_custom_code: Annotated[
        bool,
        Arg(
            "allow_custom_code",
            default=False,
            doc="Allow a manifest's custom Python components (components.py) to run.",
        ),
    ]
    cdk_version: Annotated[
        str,
        Arg(
            "cdk_version",
            default=cn.DEFAULT_CDK_VERSION,
            doc="airbyte-cdk version that runs declarative manifests (installed once into a cached environment).",
        ),
    ]
    registry_url: Annotated[
        str,
        Arg(
            "registry_url",
            default=reg.DEFAULT_REGISTRY_URL,
            doc="Connector registry JSON; manifests are fetched from the same /files/ tree.",
            pattern=r"^(https?|file)://",
        ),
    ]
    timeout_seconds: Annotated[
        int,
        Arg(
            "timeout_seconds",
            default=cn.DEFAULT_TIMEOUT_SECONDS,
            doc="Stop the connector if it prints nothing for this many seconds; 0 waits forever.",
            ge=0,
        ),
    ]

    def options(self) -> cn.Options:
        """Return the resolution/run options."""
        return cn.Options(
            registry_url=self.registry_url,
            cdk_version=self.cdk_version,
            accept_license=self.accept_license,
            allow_custom_code=self.allow_custom_code,
            timeout_seconds=self.timeout_seconds,
        )

    def resolve(self) -> cn.Resolved:
        """Resolve the connector (registry lookup, license policy, manifest download)."""
        return cn.resolve(self.connector, self.options())


def _config_value(secret: dict[str, Any] | None) -> str:
    if secret is None:
        return "{}"
    value = secret.get("config")
    text = value.as_py() if value is not None and hasattr(value, "as_py") else value
    if not text:
        return "{}"
    try:
        parsed = json.loads(str(text))
    except json.JSONDecodeError as exc:
        # Never echo the value: it is a credential.
        raise ValueError(f"the airbyte secret's CONFIG is not valid JSON ({exc.msg} at position {exc.pos})") from None
    if not isinstance(parsed, dict):
        raise ValueError("the airbyte secret's CONFIG must be a JSON object")
    return str(text)


def bind_config(secrets: Any, args: ConnectorArgs) -> str | None:
    """Config JSON at bind time, or None while DuckDB still has to resolve the secret."""
    if args.secret_name:
        secret = secrets.get(SECRET_TYPE, name=args.secret_name)
        if secret is None and secrets.all_resolved:
            raise ValueError(f"no airbyte secret named {args.secret_name!r}")
    else:
        secret = secrets.get(SECRET_TYPE, scope=SCOPE_PREFIX + args.connector)
    if not secrets.all_resolved:
        return None
    return _config_value(secret)


def process_config(secrets: Any, args: ConnectorArgs) -> str:
    """Config JSON while scanning (the secrets resolved at bind)."""
    if args.secret_name:
        return _config_value(secrets.lookup(SECRET_TYPE, name=args.secret_name))
    return _config_value(secrets.lookup(SECRET_TYPE, scope=SCOPE_PREFIX + args.connector))


def validated(resolved: cn.Resolved, config: str, timeout_seconds: int) -> str:
    """Check the config against the connector's spec (raises ValueError) and return it."""
    connection_spec = cn.spec(resolved, timeout_seconds).get("connectionSpecification") or {}
    cn.validate_config(json.loads(config), connection_spec, resolved.name)
    return config


def state_text(state: str) -> str:
    """Normalise a ``state :=`` argument to the compact list form (empty for none)."""
    msgs = parse_state(state)
    return json.dumps(msgs, separators=(",", ":")) if msgs else ""


# ------------------------------------------------------------------ scan engine


@dataclass(kw_only=True)
class ScanCursor(ArrowSerializableDataclass):
    """Externalized scan position: no secrets, only Airbyte state and counters."""

    token: str = ""
    airbyte_state: str = ""
    since_state: int = 0
    done: bool = False


# Turns one message into the items it contributes (records, or the message itself).
ItemFn = Callable[[dict[str, Any]], list[dict[str, Any]]]


def read_items(
    cursor: ScanCursor,
    *,
    resolved: cn.Resolved,
    config: str,
    catalog: dict[str, Any],
    timeout_seconds: int,
    items_of: ItemFn,
    max_items: int = BATCH_ROWS,
) -> list[dict[str, Any]]:
    """Read the next items of a scan, resuming or restarting the connector as needed.

    Sets ``cursor.done`` once the connector has exited cleanly. Raises
    ConnectorError when it fails.
    """
    if not cursor.token:
        cursor.token = uuid.uuid4().hex
    run = cn.take_run(cursor.token)
    if run is not None and (run.last_state != cursor.airbyte_state or run.since_state != cursor.since_state):
        cn.drop_run(cursor.token)
        run = None
    skip = 0
    if run is None:
        run = cn.ConnectorRun.start(
            resolved,
            "read",
            config=config,
            catalog=catalog,
            state=cursor.airbyte_state or None,
            timeout_seconds=timeout_seconds,
        )
        run.tracker = StateTracker(parse_state(cursor.airbyte_state))
        run.last_state = cursor.airbyte_state
        skip = cursor.since_state
        run.since_state = 0
        cn.keep_run(cursor.token, run)
    items: list[dict[str, Any]] = []
    started = time.monotonic()
    try:
        while len(items) < max_items:
            wait = 1.0 if not items else max(0.05, FLUSH_SECONDS - (time.monotonic() - started))
            msg = run.poll(wait)
            if msg is None:
                cursor.done = True
                break
            if msg is False:
                if items and time.monotonic() - started >= FLUSH_SECONDS:
                    break
                continue
            assert isinstance(msg, dict)
            for item in items_of(msg):
                run.since_state += 1
                if skip:
                    skip -= 1
                else:
                    items.append(item)
            if msg["type"] == "STATE" and isinstance(msg.get("state"), dict):
                run.tracker.add(msg["state"])
                run.last_state = run.tracker.text()
                run.since_state = 0
    except BaseException:
        cn.drop_run(cursor.token)
        raise
    if cursor.done:
        cn.drop_run(cursor.token)
        if skip:
            raise ConnectorError(
                f"{resolved.name} produced fewer records when resumed from its state than before; "
                "the scan cannot continue consistently"
            )
    cursor.airbyte_state = run.last_state
    cursor.since_state = run.since_state
    return items
