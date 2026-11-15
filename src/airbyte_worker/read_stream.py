"""``read_stream``: one connector stream as a typed table, and ``final_state``.

Bind resolves the connector, validates the config against its spec and runs
``discover`` (or reads ``catalog_json``) to find the stream and turn its JSON
Schema into columns. The scan runs ``read`` with a configured catalog that
selects only that stream, in ``auto`` sync mode (incremental when the stream
supports it, so the connector checkpoints with STATE), and with the stream's
``json_schema`` cut down to the projected columns plus key and cursor - the way
the Airbyte platform expresses column selection. Rows stream back in batches
of up to 1,000; see ``common.py`` for how a scan resumes from Airbyte STATE.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

from . import connector as cn
from . import protocol as pr
from .common import ConnectorArgs, ScanCursor, bind_config, process_config, read_items, state_text, validated
from .jsonschema import JSON_TYPE, Coercer, records_to_batch
from .listing import OnceState
from .meta import CONNECTOR_ARGS_DOC, DYNAMIC_COLUMNS_MD, column, object_tags, result_columns

log = logging.getLogger(__name__)

_PLACEHOLDER = pa.schema([pa.field("pending", pa.string())])


@dataclass(slots=True, frozen=True, kw_only=True)
class ReadStreamArgs(ConnectorArgs):
    """Arguments for ``read_stream(connector, stream, ...)``."""

    stream: Annotated[str, Arg(1, doc="Stream to read: its name, or namespace.name.")]
    state: Annotated[
        str,
        Arg(
            "state",
            default="",
            doc="Airbyte state JSON to start from (e.g. final_state's result); empty reads everything.",
        ),
    ]
    sync_mode: Annotated[
        str,
        Arg(
            "sync_mode",
            default="auto",
            doc="auto (incremental when supported), full_refresh (ignores state) or incremental.",
            choices=list(pr.SYNC_MODES),
        ),
    ]
    cursor_field: Annotated[
        str,
        Arg("cursor_field", default="", doc="Cursor field for incremental reads when the source has no default."),
    ]
    catalog_json: Annotated[
        str,
        Arg(
            "catalog_json",
            default="",
            doc="An Airbyte catalog JSON to use instead of running discover (e.g. from airbyte.main.catalog).",
        ),
    ]


def records_of(stream: dict[str, Any]) -> Any:
    """Return a function mapping a message to the stream's record data (if it is one)."""
    name, namespace = stream["name"], stream.get("namespace")

    def items_of(msg: dict[str, Any]) -> list[dict[str, Any]]:
        if msg["type"] != "RECORD":
            return []
        rec = msg.get("record") or {}
        if rec.get("stream") != name or (namespace and rec.get("namespace") not in (None, namespace)):
            return []
        data = rec.get("data")
        return [data] if isinstance(data, dict) else []

    return items_of


def bound_catalog(args: Any, resolved: cn.Resolved, config: str) -> dict[str, Any]:
    """Return the catalog to find streams in: catalog_json, or a fresh discover."""
    if args.catalog_json:
        return pr.load_catalog(args.catalog_json)
    return cn.discover(resolved, config, args.timeout_seconds)


@init_single_worker
class ReadStream(TableFunctionGenerator[ReadStreamArgs, ScanCursor]):
    """Run an Airbyte connector and stream one of its streams as typed rows."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "read_stream"
        description = "Run an Airbyte source connector and return one stream as a typed table"
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Read Connector Stream",
                category="streams",
                keywords="airbyte, read, stream, sync, extract, elt, records, incremental, state",
                doc_llm=(
                    "Run an Airbyte source and return one stream as a table. `connector` is a registry name, "
                    "manifest path/URL, 'docker:<image>' or 'exec:<path>'; `stream` the stream name. Columns "
                    "come from the stream's JSON Schema: integer -> `BIGINT`, number -> `DOUBLE`, boolean -> "
                    "`BOOLEAN`, string -> `VARCHAR`, date-time -> `TIMESTAMPTZ` (or `TIMESTAMP` for "
                    "airbyte_type timestamp_without_timezone), date -> `DATE`, objects -> `STRUCT`, arrays -> "
                    "`LIST`, open shapes -> `JSON`. Named args: state (Airbyte state JSON, e.g. from "
                    "final_state, to read only what changed since), sync_mode (auto/full_refresh/incremental), "
                    "cursor_field, catalog_json (skip discover). Only the projected columns are kept in the "
                    "configured catalog. " + CONNECTOR_ARGS_DOC
                ),
                doc_md=(
                    "## read_stream(connector, stream, ...)\n\n"
                    "One stream of an Airbyte source as a typed table. Declarative manifests run through a "
                    "cached airbyte-cdk; the config comes from an `airbyte` secret."
                ),
            ),
            "vgi.result_dynamic_columns_md": DYNAMIC_COLUMNS_MD,
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT id, name, base_experience FROM airbyte.main.read_stream('source-pokeapi', 'pokemon', "
                    "accept_license := 'ELv2')"
                ),
                description="Read the pokemon stream of the declarative PokeAPI connector",
            ),
            FunctionExample(
                sql=(
                    "SELECT count(*) AS pokemon FROM airbyte.main.read_stream("
                    "'https://connectors.airbyte.com/files/metadata/airbyte/source-pokeapi/0.3.79/manifest.yaml', "
                    "'pokemon', state := getvariable('pokemon_state'))"
                ),
                description="Read a declarative manifest by URL, resuming from a saved Airbyte state",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ReadStreamArgs]) -> BindResponse:
        """Resolve the connector, validate the config, find the stream and publish its columns."""
        args = params.args
        config = bind_config(params.secrets, args)
        if config is None:
            return BindResponse(output_schema=_PLACEHOLDER)
        state_text(args.state)
        resolved = args.resolve()
        validated(resolved, config, args.timeout_seconds)
        stream = pr.find_stream(bound_catalog(args, resolved, config), args.stream)
        pr.configure(stream, columns=None, sync_mode=args.sync_mode, cursor_field=args.cursor_field)
        schema = pr.stream_schema(stream)
        if not len(schema):
            raise ValueError(f"stream {args.stream!r} declares no properties, so it has no columns")
        return BindResponse(output_schema=schema, opaque_data=json.dumps(stream).encode())

    @classmethod
    def initial_state(cls, params: ProcessParams[ReadStreamArgs]) -> ScanCursor:
        """Start from the given state (none for an explicit full refresh, which re-reads everything)."""
        if params.args.sync_mode == "full_refresh":
            return ScanCursor()
        return ScanCursor(airbyte_state=state_text(params.args.state))

    @classmethod
    def process(cls, params: ProcessParams[ReadStreamArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Read the next records of the stream and emit them as one batch."""
        if state.done:
            out.finish()
            return
        assert params.init_call is not None and params.init_call.bind_opaque_data
        args = params.args
        stream = json.loads(params.init_call.bind_opaque_data)
        coercer = Coercer()
        records = read_items(
            state,
            resolved=args.resolve(),
            config=process_config(params.secrets, args),
            catalog=pr.configure(
                stream,
                columns=list(params.output_schema.names),
                sync_mode=args.sync_mode,
                cursor_field=args.cursor_field,
            ),
            timeout_seconds=args.timeout_seconds,
            items_of=records_of(stream),
        )
        if records:
            out.emit(records_to_batch(records, params.output_schema, coercer))
        elif state.done:
            out.finish()
            return
        if coercer.rejected:
            log.warning("airbyte: %d values did not match their declared type and were set to NULL", coercer.rejected)


# ---------------------------------------------------------------- final_state


@dataclass(slots=True, frozen=True, kw_only=True)
class FinalStateArgs(ConnectorArgs):
    """Arguments for ``final_state(connector, stream := '', state := '')``."""

    stream: Annotated[str, Arg("stream", default="", doc="Only sync this stream; empty syncs every stream.")]
    state: Annotated[str, Arg("state", default="", doc="Airbyte state JSON to start from; empty = a full sync.")]
    catalog_json: Annotated[
        str, Arg("catalog_json", default="", doc="An Airbyte catalog JSON to use instead of running discover.")
    ]


FINAL_STATE_SCHEMA = pa.schema(
    [
        column(
            "final_state",
            JSON_TYPE,
            "The cumulative state after the sync (a JSON array of state messages; NULL if none was emitted).",
        ),
        column("records", pa.int64(), "RECORD messages read.", nullable=False),
        column("state_messages", pa.int64(), "STATE messages read.", nullable=False),
        column("streams", pa.list_(pa.string()), "Streams that produced records, sorted.", nullable=False),
        column("state_type", pa.string(), "STREAM, GLOBAL or LEGACY: the form of the final state."),
    ]
)


@init_single_worker
class FinalState(TableFunctionGenerator[FinalStateArgs, OnceState]):
    """Sync a connector from a state and return the state it ends with."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "final_state"
        description = "Run an Airbyte connector from a state and return the final state and record counts"
        tags = {
            **object_tags(
                title="Final Connector State",
                category="streams",
                keywords="airbyte, state, checkpoint, incremental, sync, resume, cursor, bookmark",
                doc_llm=(
                    "Run an Airbyte source to completion, starting from the `state` JSON (named arg, empty = "
                    "full sync), and return one row: final_state (JSON array of state messages, the merged "
                    "per-stream/global/legacy state - pass it as `state` next time for an incremental read), "
                    "records, state_messages, streams and state_type. `stream :=` limits the sync to one "
                    "stream. Records are counted, not returned; use read_stream for rows. " + CONNECTOR_ARGS_DOC
                ),
                doc_md=(
                    "## final_state(connector, stream := '', state := '')\n\n"
                    "Sync and keep only the final Airbyte state, for incremental pipelines: pass the "
                    "returned `final_state` as `state` on the next run."
                ),
            ),
            "vgi.result_columns_schema": result_columns(FINAL_STATE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT records, final_state FROM airbyte.main.final_state('source-pokeapi', "
                    "stream := 'pokemon', accept_license := 'ELv2')"
                ),
                description="Sync one stream and keep the state to resume from",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[FinalStateArgs]) -> BindResponse:
        """Resolve the secret and connector, and validate the JSON arguments."""
        args = params.args
        config = bind_config(params.secrets, args)
        if config is not None:
            state_text(args.state)
            if args.catalog_json:
                pr.load_catalog(args.catalog_json)
            validated(args.resolve(), config, args.timeout_seconds)
        return BindResponse(output_schema=FINAL_STATE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[FinalStateArgs]) -> OnceState:
        """Not started."""
        return OnceState()

    @classmethod
    def process(cls, params: ProcessParams[FinalStateArgs], state: OnceState, out: OutputCollector) -> None:
        """Run the connector to the end, counting records, and emit the one result row."""
        if state.done:
            out.finish()
            return
        args = params.args
        resolved = args.resolve()
        config = process_config(params.secrets, args)
        catalog = bound_catalog(args, resolved, config)
        configured = (
            pr.configure(pr.find_stream(catalog, args.stream), columns=None)
            if args.stream
            else pr.configure_all(catalog)
        )
        start = state_text(args.state)
        tracker = pr.StateTracker(pr.parse_state(start))
        run = cn.ConnectorRun.start(
            resolved,
            "read",
            config=config,
            catalog=configured,
            state=start or None,
            timeout_seconds=args.timeout_seconds,
        )
        records = 0
        seen: set[str] = set()
        try:
            while (msg := run.next_message()) is not None:
                if msg["type"] == "RECORD":
                    records += 1
                    rec = msg.get("record") or {}
                    ns = rec.get("namespace")
                    seen.add(f"{ns}.{rec.get('stream')}" if ns else str(rec.get("stream")))
                elif msg["type"] == "STATE" and isinstance(msg.get("state"), dict):
                    tracker.add(msg["state"])
        finally:
            run.close()
        value = tracker.value()
        row = {
            "final_state": json.dumps(value, separators=(",", ":")) if value else None,
            "records": records,
            "state_messages": tracker.messages,
            "streams": sorted(seen),
            "state_type": str(value[0].get("type") or "LEGACY") if value else None,
        }
        out.emit(pa.RecordBatch.from_pylist([row], schema=params.output_schema))
        state.done = True
