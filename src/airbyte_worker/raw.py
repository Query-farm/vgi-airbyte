"""``messages``: every AirbyteMessage a connector prints, one row per message."""

from __future__ import annotations

import datetime as dt
import json
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
from .jsonschema import JSON_TYPE, TIMESTAMP_TYPE
from .meta import CONNECTOR_ARGS_DOC, column, object_tags, result_columns
from .read_stream import bound_catalog

COMMANDS = ("read", "check", "discover", "spec")

MESSAGE_SCHEMA = pa.schema(
    [
        column("seq", pa.int64(), "Position of the message in the connector's output, from 1.", nullable=False),
        column(
            "type",
            pa.string(),
            "RECORD, STATE, LOG, TRACE, SPEC, CATALOG, CONNECTION_STATUS or CONTROL.",
            nullable=False,
        ),
        column("stream", pa.string(), "RECORD / STATE / STREAM_STATUS: the stream name."),
        column("namespace", pa.string(), "The stream namespace, if any."),
        column("emitted_at", TIMESTAMP_TYPE, "RECORD / TRACE: when the connector emitted it."),
        column("data", JSON_TYPE, "RECORD: the record data."),
        column("state", JSON_TYPE, "STATE: the state message (STREAM, GLOBAL or LEGACY)."),
        column("state_type", pa.string(), "STATE: STREAM, GLOBAL or LEGACY."),
        column("log_level", pa.string(), "LOG: FATAL, ERROR, WARN, INFO, DEBUG or TRACE."),
        column("log_message", pa.string(), "LOG: the message text."),
        column("trace_type", pa.string(), "TRACE: ERROR, ESTIMATE, STREAM_STATUS, ANALYTICS ..."),
        column("error_message", pa.string(), "TRACE ERROR: the user-facing message."),
        column("stream_status", pa.string(), "TRACE STREAM_STATUS: STARTED, RUNNING, COMPLETE, INCOMPLETE."),
        column("message", JSON_TYPE, "The whole message as JSON.", nullable=False),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class MessagesArgs(ConnectorArgs):
    """Arguments for ``messages(connector, command := 'read', stream := '', state := '')``."""

    command: Annotated[
        str,
        Arg(
            "command",
            default="read",
            doc="Protocol command to run: read, check, discover or spec.",
            choices=list(COMMANDS),
        ),
    ]
    stream: Annotated[str, Arg("stream", default="", doc="read: only this stream; empty reads every stream.")]
    state: Annotated[str, Arg("state", default="", doc="read: Airbyte state JSON to start from.")]
    catalog_json: Annotated[
        str, Arg("catalog_json", default="", doc="read: an Airbyte catalog JSON to use instead of discover.")
    ]


@dataclass(kw_only=True)
class MessagesCursor(ScanCursor):
    """The scan cursor plus the number of messages returned so far."""

    emitted: int = 0


def _ms(value: Any) -> dt.datetime | None:
    if isinstance(value, int | float) and not isinstance(value, bool):
        return dt.datetime.fromtimestamp(value / 1000, tz=dt.UTC)
    return None


def _js(value: Any) -> str | None:
    return None if value is None else json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def message_row(msg: dict[str, Any]) -> dict[str, Any]:
    """Return the decoded columns for one message."""
    kind = str(msg["type"])
    row: dict[str, Any] = {"type": kind, "message": _js(msg)}
    if kind == "RECORD":
        rec = msg.get("record") or {}
        row.update(
            stream=rec.get("stream"),
            namespace=rec.get("namespace"),
            emitted_at=_ms(rec.get("emitted_at")),
            data=_js(rec.get("data")),
        )
    elif kind == "STATE":
        st = msg.get("state") or {}
        desc = (st.get("stream") or {}).get("stream_descriptor") or {}
        row.update(
            state=_js(st),
            state_type=st.get("type") or "LEGACY",
            stream=desc.get("name"),
            namespace=desc.get("namespace"),
        )
    elif kind == "LOG":
        entry = msg.get("log") or {}
        row.update(log_level=entry.get("level"), log_message=entry.get("message"))
    elif kind == "TRACE":
        trace = msg.get("trace") or {}
        row.update(trace_type=trace.get("type"), emitted_at=_ms(trace.get("emitted_at")))
        if trace.get("type") == "ERROR":
            row["error_message"] = (trace.get("error") or {}).get("message")
        status = trace.get("stream_status")
        if isinstance(status, dict):
            desc = status.get("stream_descriptor") or {}
            row.update(stream_status=status.get("status"), stream=desc.get("name"), namespace=desc.get("namespace"))
    return row


@init_single_worker
class Messages(TableFunctionGenerator[MessagesArgs, MessagesCursor]):
    """Run a connector command and return every message it prints."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "messages"
        description = "Run an Airbyte connector command and return its raw protocol messages, one row per message"
        tags = {
            **object_tags(
                title="Raw Connector Messages",
                category="debugging",
                keywords="airbyte, messages, protocol, debug, log, trace, state, record, jsonl",
                doc_llm=(
                    "Run a connector command (command := 'read' default, or 'check', 'discover', 'spec') and "
                    "return every AirbyteMessage it prints: seq, type, stream, namespace, emitted_at, data "
                    "(RECORD JSON), state (STATE JSON), state_type, log_level, log_message, trace_type, "
                    "error_message, stream_status and message (the whole JSON). For debugging connectors and "
                    "inspecting LOG/TRACE/STATE; a failing read still raises its error at the end. read takes "
                    "stream (default all), state and catalog_json. " + CONNECTOR_ARGS_DOC
                ),
                doc_md=(
                    "## messages(connector, command := 'read', stream := '', state := '')\n\n"
                    "Everything a connector prints, one row per Airbyte protocol message."
                ),
            ),
            "vgi.result_columns_schema": result_columns(MESSAGE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT type, count(*) AS n FROM airbyte.main.messages('source-pokeapi', stream := 'pokemon', "
                    "accept_license := 'ELv2') GROUP BY type ORDER BY type"
                ),
                description="Message counts by type for one stream of the PokeAPI connector",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[MessagesArgs]) -> BindResponse:
        """Resolve the secret and connector, and validate the arguments."""
        args = params.args
        config = bind_config(params.secrets, args)
        if config is not None:
            state_text(args.state)
            if args.catalog_json:
                pr.load_catalog(args.catalog_json)
            resolved = args.resolve()
            if args.command != "spec":
                validated(resolved, config, args.timeout_seconds)
        return BindResponse(output_schema=MESSAGE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[MessagesArgs]) -> MessagesCursor:
        """Start from the given state (or none)."""
        return MessagesCursor(airbyte_state=state_text(params.args.state))

    @classmethod
    def process(cls, params: ProcessParams[MessagesArgs], state: MessagesCursor, out: OutputCollector) -> None:
        """Read the next messages and emit them."""
        if state.done:
            out.finish()
            return
        args = params.args
        resolved = args.resolve()
        config = process_config(params.secrets, args)
        if args.command == "read":
            catalog = bound_catalog(args, resolved, config)
            selected = (
                pr.configure(pr.find_stream(catalog, args.stream), columns=None)
                if args.stream
                else pr.configure_all(catalog)
            )
            msgs = read_items(
                state,
                resolved=resolved,
                config=config,
                catalog=selected,
                timeout_seconds=args.timeout_seconds,
                items_of=lambda m: [m],
            )
        elif args.command == "spec" and resolved.mode == "manifest":
            # The CDK's generic `spec` describes its own wrapper, not this manifest.
            msgs = [{"type": "SPEC", "spec": cn.spec(resolved, args.timeout_seconds)}]
            state.done = True
        else:
            msgs = cn.run_to_end(
                resolved,
                args.command,
                config=None if args.command == "spec" else config,
                timeout_seconds=args.timeout_seconds,
            )
            state.done = True
        if not msgs:
            out.finish()
            return
        rows = []
        for msg in msgs:
            state.emitted += 1
            rows.append({"seq": state.emitted, **message_row(msg)})
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
