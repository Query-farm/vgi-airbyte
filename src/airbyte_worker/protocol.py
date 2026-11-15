"""The Airbyte protocol: messages, catalogs, configured catalogs and state.

A source prints one JSON ``AirbyteMessage`` per line. The types used here:

* ``RECORD``  ``{"record": {"stream", "namespace", "data", "emitted_at"}}`` - a row.
* ``STATE``   ``{"state": {"type": "STREAM" | "GLOBAL" | "LEGACY", ...}}`` - a checkpoint.
* ``LOG``     ``{"log": {"level", "message"}}``.
* ``TRACE``   ``{"trace": {"type": "ERROR" | "STREAM_STATUS" | "ESTIMATE" | ...}}``;
  an ``ERROR`` trace fails the sync.
* ``SPEC``, ``CONNECTION_STATUS``, ``CATALOG`` - replies to spec / check / discover.
* ``CONTROL`` - config updates (logged, not applied).

State is cumulative: the input to the next ``read --state`` is the set of the
latest per-stream states (or the latest global / legacy state). ``StateTracker``
merges the STATE messages of a run into exactly that list.
"""

from __future__ import annotations

import copy
import json
import logging
from typing import Any

import pyarrow as pa

from .jsonschema import arrow_fields

log = logging.getLogger(__name__)

SYNC_MODES = ("auto", "full_refresh", "incremental")


class ConnectorError(RuntimeError):
    """A connector failed, or printed something the protocol does not allow."""


def parse_message(line: str) -> dict[str, Any] | None:
    """Decode one stdout line; None for blank or non-message lines (logged, then skipped)."""
    text = line.strip()
    if not text:
        return None
    try:
        msg = json.loads(text)
    except json.JSONDecodeError:
        log.debug("airbyte: skipping a non-JSON line on stdout: %.200s", text)
        return None
    if not isinstance(msg, dict) or not isinstance(msg.get("type"), str):
        log.debug("airbyte: skipping a line that is not an AirbyteMessage: %.200s", text)
        return None
    return msg


def error_text(msg: dict[str, Any]) -> str | None:
    """Return the message of an ERROR trace (plus its internal message), or None for anything else."""
    if msg.get("type") != "TRACE":
        return None
    trace = msg.get("trace") or {}
    if trace.get("type") != "ERROR":
        return None
    err = trace.get("error") or {}
    text = str(err.get("message") or "the connector reported an error")
    internal = err.get("internal_message")
    if internal and str(internal) not in text:
        text += f" ({str(internal)[:600]})"
    return text


# ------------------------------------------------------------------- catalogs


def streams(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the streams of an AirbyteCatalog (``{"streams": [AirbyteStream, ...]}``)."""
    return [s for s in catalog.get("streams") or [] if isinstance(s, dict) and s.get("name")]


def qualified(stream: dict[str, Any]) -> str:
    """``namespace.name`` or just ``name``."""
    ns = stream.get("namespace")
    return f"{ns}.{stream['name']}" if ns else str(stream["name"])


def find_stream(catalog: dict[str, Any], name: str) -> dict[str, Any]:
    """Return the stream called ``name`` or ``namespace.name`` (case-insensitive)."""
    wanted = name.lower()
    for key in (qualified, lambda s: str(s["name"])):
        hits = [s for s in streams(catalog) if key(s).lower() == wanted]
        if len(hits) == 1:
            return hits[0]
        if len(hits) > 1:
            raise ValueError(f"stream name {name!r} is ambiguous; use namespace.name: {[qualified(h) for h in hits]}")
    known = ", ".join(sorted(qualified(s) for s in streams(catalog))) or "none"
    raise ValueError(f"the connector has no stream named {name!r}; streams: {known}")


def table_names(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Table name -> stream: the stream name, or ``namespace_name`` when names collide."""
    by_name: dict[str, list[dict[str, Any]]] = {}
    for s in streams(catalog):
        by_name.setdefault(str(s["name"]).lower(), []).append(s)
    out: dict[str, dict[str, Any]] = {}
    for group in by_name.values():
        for s in group:
            ns = s.get("namespace")
            out[str(s["name"]) if len(group) == 1 or not ns else f"{ns}_{s['name']}"] = s
    return out


def stream_schema(stream: dict[str, Any]) -> pa.Schema:
    """Arrow schema of a stream's table: one nullable column per top-level property."""
    schema = stream.get("json_schema") if isinstance(stream.get("json_schema"), dict) else {}
    assert isinstance(schema, dict)
    return pa.schema(arrow_fields(schema))


def primary_key(stream: dict[str, Any]) -> list[list[str]]:
    """Return the source-defined primary key (a list of field paths)."""
    pk = stream.get("source_defined_primary_key") or []
    return [[str(p) for p in path] for path in pk if isinstance(path, list)]


def sync_modes(stream: dict[str, Any]) -> list[str]:
    """Return the supported sync modes (``full_refresh`` when none are declared)."""
    return [str(m) for m in stream.get("supported_sync_modes") or ["full_refresh"]]


def load_catalog(text: str) -> dict[str, Any]:
    """Parse AirbyteCatalog JSON (a ``CATALOG`` message's catalog, or a configured catalog)."""
    try:
        catalog = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"the catalog is not valid JSON: {exc}") from exc
    if isinstance(catalog, dict) and catalog.get("type") == "CATALOG":
        catalog = catalog.get("catalog")
    if not isinstance(catalog, dict) or not isinstance(catalog.get("streams"), list):
        raise ValueError("an Airbyte catalog is a JSON object with a 'streams' list")
    # A configured catalog nests each stream under "stream": unwrap it.
    out = [s["stream"] if isinstance(s, dict) and isinstance(s.get("stream"), dict) else s for s in catalog["streams"]]
    return {"streams": out}


def configure(
    stream: dict[str, Any],
    *,
    columns: list[str] | None,
    sync_mode: str = "auto",
    cursor_field: str = "",
) -> dict[str, Any]:
    """Build a one-stream ConfiguredAirbyteCatalog.

    ``auto`` reads incrementally when the stream supports it (so the run emits
    STATE to resume from), else as a full refresh. With ``columns`` the
    stream's ``json_schema`` keeps only those properties plus the primary key
    and cursor, which is how the Airbyte platform expresses column selection.
    """
    modes = sync_modes(stream)
    mode = sync_mode
    if mode == "auto":
        mode = "incremental" if "incremental" in modes else "full_refresh"
    if mode not in modes:
        raise ValueError(f"stream {qualified(stream)!r} does not support sync_mode {mode!r}; it supports {modes}")
    cursor: list[str] = []
    if mode == "incremental":
        if cursor_field:
            cursor = cursor_field.split(".")
        elif stream.get("default_cursor_field"):
            cursor = [str(c) for c in stream["default_cursor_field"]]
        elif not stream.get("source_defined_cursor"):
            raise ValueError(
                f"stream {qualified(stream)!r} has no default cursor; pass cursor_field := '<field>' "
                "for an incremental read"
            )
    entry = copy.deepcopy(stream)
    if columns is not None and isinstance(entry.get("json_schema"), dict):
        props = entry["json_schema"].get("properties")
        if isinstance(props, dict):
            keep = set(columns) | {p[0] for p in primary_key(stream) if p} | set(cursor[:1])
            entry["json_schema"] = {**entry["json_schema"], "properties": {k: v for k, v in props.items() if k in keep}}
    configured: dict[str, Any] = {
        "stream": entry,
        "sync_mode": mode,
        "destination_sync_mode": "append" if mode == "incremental" else "overwrite",
    }
    if cursor:
        configured["cursor_field"] = cursor
    pk = primary_key(stream)
    if pk:
        configured["primary_key"] = pk
    return {"streams": [configured]}


def configure_all(catalog: dict[str, Any]) -> dict[str, Any]:
    """Return a configured catalog selecting every stream (``auto`` sync mode, all columns)."""
    out: list[dict[str, Any]] = []
    for s in streams(catalog):
        try:
            out.extend(configure(s, columns=None)["streams"])
        except ValueError:
            out.extend(configure(s, columns=None, sync_mode="full_refresh")["streams"])
    return {"streams": out}


# ---------------------------------------------------------------------- state


def parse_state(text: str) -> list[dict[str, Any]]:
    """Normalise a ``state :=`` argument to a list of AirbyteStateMessages.

    Accepts the protocol's list form, a single state message, or a legacy
    state object (wrapped as ``{"type": "LEGACY", "data": ...}``).
    """
    if not text.strip():
        return []
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"state is not valid JSON: {exc}") from exc
    if isinstance(value, list):
        if not all(isinstance(m, dict) for m in value):
            raise ValueError("state must be a JSON array of Airbyte state messages")
        return value
    if isinstance(value, dict):
        if value.get("type") in ("STREAM", "GLOBAL", "LEGACY"):
            return [value]
        return [{"type": "LEGACY", "data": value}]
    raise ValueError("state must be a JSON array of Airbyte state messages (or a legacy state object)")


def _descriptor_key(desc: dict[str, Any]) -> str:
    return json.dumps([desc.get("namespace"), desc.get("name")])


class StateTracker:
    """Merge a run's STATE messages into the state to pass to the next run."""

    def __init__(self, initial: list[dict[str, Any]] | None = None) -> None:
        """Start from an input state (the list form)."""
        self.per_stream: dict[str, dict[str, Any]] = {}
        self.whole: dict[str, Any] | None = None
        self.messages = 0
        for msg in initial or []:
            self.add(msg, count=False)

    def add(self, state: dict[str, Any], *, count: bool = True) -> None:
        """Apply one AirbyteStateMessage (the ``state`` member of a STATE message)."""
        if count:
            self.messages += 1
        clean = {k: v for k, v in state.items() if k not in ("sourceStats", "destinationStats", "id")}
        kind = clean.get("type") or "LEGACY"
        if kind == "STREAM" and isinstance(clean.get("stream"), dict):
            desc = clean["stream"].get("stream_descriptor") or {}
            self.per_stream[_descriptor_key(desc)] = clean
            self.whole = None
        elif kind in ("GLOBAL", "LEGACY"):
            if kind == "GLOBAL":
                self.whole = clean
            else:
                self.whole = {"type": "LEGACY", "data": clean.get("data", clean)}
            self.per_stream.clear()

    def value(self) -> list[dict[str, Any]]:
        """Return the cumulative state as the list ``read --state`` accepts."""
        if self.whole is not None:
            return [self.whole]
        return [self.per_stream[k] for k in sorted(self.per_stream)]

    def text(self) -> str:
        """Return the cumulative state as compact JSON; empty when there is none."""
        value = self.value()
        return json.dumps(value, separators=(",", ":")) if value else ""
