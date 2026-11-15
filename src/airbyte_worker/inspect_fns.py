"""``spec``, ``check``, ``discover`` and ``catalog``: inspect one connector."""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
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
from .common import ConnectorArgs, bind_config, process_config, validated
from .jsonschema import JSON_TYPE, duckdb_type_name
from .listing import OnceState
from .meta import CONNECTOR_ARGS_DOC, column, object_tags, result_columns

PROPERTY_TYPE = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("type", pa.string()),
        pa.field("title", pa.string()),
        pa.field("description", pa.string()),
        pa.field("required", pa.bool_()),
        pa.field("secret", pa.bool_()),
        pa.field("default", pa.string()),
    ]
)

SPEC_SCHEMA = pa.schema(
    [
        column(
            "connector", pa.string(), "The connector as resolved (registry name, manifest file, image).", nullable=False
        ),
        column("mode", pa.string(), "How it runs: manifest (declarative CDK), docker or exec.", nullable=False),
        column("license", pa.string(), "Registry license (NULL for connectors named directly)."),
        column("documentation_url", pa.string(), "Documentation link from the spec."),
        column("required", pa.list_(pa.string()), "Top-level config fields that must be set.", nullable=False),
        column(
            "secret_fields",
            pa.list_(pa.string()),
            "Config fields marked airbyte_secret (dotted paths).",
            nullable=False,
        ),
        column(
            "properties",
            pa.list_(PROPERTY_TYPE),
            "Top-level config fields: name, JSON type, title, description, required, secret, default.",
            nullable=False,
        ),
        column("connection_specification", JSON_TYPE, "The JSON Schema the config must match.", nullable=False),
    ]
)

CHECK_SCHEMA = pa.schema(
    [
        column("connector", pa.string(), "The connector as resolved.", nullable=False),
        column("status", pa.string(), "SUCCEEDED or FAILED.", nullable=False),
        column("message", pa.string(), "Why the check failed (or the connector's message)."),
        column("config_valid", pa.bool_(), "Whether the config matched the connector's spec.", nullable=False),
    ]
)

COLUMN_TYPE = pa.struct([pa.field("name", pa.string()), pa.field("type", pa.string())])

DISCOVER_SCHEMA = pa.schema(
    [
        column("stream", pa.string(), "Stream name (pass it to read_stream).", nullable=False),
        column("namespace", pa.string(), "Stream namespace, if the source sets one."),
        column(
            "table_name", pa.string(), "Table name the stream gets in an attached connector catalog.", nullable=False
        ),
        column("supported_sync_modes", pa.list_(pa.string()), "full_refresh and/or incremental.", nullable=False),
        column("source_defined_cursor", pa.bool_(), "Whether the source chooses the incremental cursor."),
        column("default_cursor_field", pa.list_(pa.string()), "Cursor field path used for incremental reads."),
        column("primary_key", pa.list_(pa.string()), "Primary key fields (nested paths joined with '.')."),
        column("is_resumable", pa.bool_(), "Whether an interrupted full refresh can resume from state."),
        column("columns", pa.list_(COLUMN_TYPE), "Table columns with their DuckDB types.", nullable=False),
        column("json_schema", JSON_TYPE, "The stream's JSON Schema.", nullable=False),
    ]
)

CATALOG_SCHEMA = pa.schema(
    [
        column("catalog", JSON_TYPE, "The AirbyteCatalog printed by discover.", nullable=False),
        column("streams", pa.int64(), "Number of streams in the catalog.", nullable=False),
    ]
)


def _json_type(sub: Any) -> str | None:
    if not isinstance(sub, dict):
        return None
    kind = sub.get("type")
    if isinstance(kind, list):
        kind = next((k for k in kind if k != "null"), None)
    if kind is None and ("oneOf" in sub or "anyOf" in sub):
        return "oneOf"
    return str(kind) if kind is not None else None


def spec_row(resolved: cn.Resolved, spec: dict[str, Any]) -> dict[str, Any]:
    """Return the spec() row for a connector specification."""
    conn = spec.get("connectionSpecification") or {}
    required = [str(r) for r in conn.get("required") or []]
    secrets = cn.secret_fields(conn)
    props = []
    for name, sub in (conn.get("properties") or {}).items():
        sub = sub if isinstance(sub, dict) else {}
        props.append(
            {
                "name": str(name),
                "type": _json_type(sub),
                "title": sub.get("title"),
                "description": sub.get("description"),
                "required": name in required,
                "secret": bool(sub.get("airbyte_secret")),
                "default": json.dumps(sub["default"]) if "default" in sub else None,
            }
        )
    return {
        "connector": resolved.name,
        "mode": resolved.mode,
        "license": resolved.license or None,
        "documentation_url": spec.get("documentationUrl"),
        "required": required,
        "secret_fields": secrets,
        "properties": props,
        "connection_specification": json.dumps(conn),
    }


def discover_rows(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    """discover() rows for an AirbyteCatalog."""
    tables = {pr.qualified(s): name for name, s in pr.table_names(catalog).items()}
    rows = []
    for s in pr.streams(catalog):
        schema = pr.stream_schema(s)
        cursor = s.get("default_cursor_field")
        rows.append(
            {
                "stream": str(s["name"]),
                "namespace": s.get("namespace"),
                "table_name": tables.get(pr.qualified(s), str(s["name"])),
                "supported_sync_modes": pr.sync_modes(s),
                "source_defined_cursor": s.get("source_defined_cursor"),
                "default_cursor_field": [str(c) for c in cursor] if isinstance(cursor, list) else None,
                "primary_key": [".".join(p) for p in pr.primary_key(s)],
                "is_resumable": s.get("is_resumable"),
                "columns": [{"name": f.name, "type": duckdb_type_name(f.type)} for f in schema],
                "json_schema": json.dumps(s.get("json_schema") or {}),
            }
        )
    return rows


class _Once(TableFunctionGenerator[ConnectorArgs, OnceState]):
    """Shared shape: resolve the secret at bind, emit one batch."""

    SCHEMA: pa.Schema
    NEEDS_CONFIG = True

    @classmethod
    def on_bind(cls, params: BindParams[ConnectorArgs]) -> BindResponse:
        """Resolve the connector (and the secret) early so mistakes fail at bind."""
        if cls.NEEDS_CONFIG:
            if bind_config(params.secrets, params.args) is not None:
                params.args.resolve()
        else:
            params.args.resolve()
        return BindResponse(output_schema=cls.SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ConnectorArgs]) -> OnceState:
        """Not started."""
        return OnceState()

    @classmethod
    def rows(cls, params: ProcessParams[ConnectorArgs]) -> list[dict[str, Any]]:
        """Return the rows to emit."""
        raise NotImplementedError

    @classmethod
    def process(cls, params: ProcessParams[ConnectorArgs], state: OnceState, out: OutputCollector) -> None:
        """Emit the rows once."""
        if state.done:
            out.finish()
            return
        out.emit(pa.RecordBatch.from_pylist(cls.rows(params), schema=params.output_schema))
        state.done = True


@init_single_worker
class Spec(_Once):
    """The connector's config specification."""

    SCHEMA = SPEC_SCHEMA
    NEEDS_CONFIG = False

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "spec"
        description = "Show an Airbyte connector's config specification: fields, required ones and secrets"
        tags = {
            **object_tags(
                title="Connector Config Spec",
                category="connectors",
                keywords="airbyte, spec, config, connection specification, secret, fields, json schema",
                doc_llm=(
                    "Return one row describing what config JSON a connector needs: connector, mode "
                    "(manifest/docker/exec), license, documentation_url, required (field names), secret_fields "
                    "(airbyte_secret paths), properties (`LIST` of `STRUCT`(name, type, title, description, required, "
                    "secret, default)) and connection_specification (the JSON Schema). Use it to write the "
                    "CONFIG of the `airbyte` secret. Needs no secret. " + CONNECTOR_ARGS_DOC
                ),
                doc_md=(
                    "## spec(connector, ...)\n\n"
                    "The config fields a connector expects, which are required and which are secrets."
                ),
            ),
            "vgi.result_columns_schema": result_columns(SPEC_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT p.name, p.type, p.required, p.secret FROM ("
                    "SELECT unnest(properties) AS p FROM airbyte.main.spec('source-pokeapi', "
                    "accept_license := 'ELv2')) ORDER BY p.name"
                ),
                description="The config fields source-pokeapi expects",
            ),
        ]

    @classmethod
    def rows(cls, params: ProcessParams[ConnectorArgs]) -> list[dict[str, Any]]:
        """Resolve and read the spec."""
        resolved = params.args.resolve()
        return [spec_row(resolved, cn.spec(resolved, params.args.timeout_seconds))]


@init_single_worker
class Check(_Once):
    """Validate the config and run the connector's connection check."""

    SCHEMA = CHECK_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "check_connection"
        description = "Validate the config against an Airbyte connector's spec and run its connection check"
        tags = {
            **object_tags(
                title="Check Connector Connection",
                category="connectors",
                keywords="airbyte, check, connection, credentials, validate, config, test connection",
                doc_llm=(
                    "Validate the `airbyte` secret's config against the connector's spec, then run the "
                    "connector's `check` command. Returns one row: connector, status (SUCCEEDED or FAILED), "
                    "message (why it failed; config values are never quoted) and config_valid. A config that "
                    "breaks the spec gives FAILED without running the connector. " + CONNECTOR_ARGS_DOC
                ),
                doc_md=(
                    "## check_connection(connector, ...)\n\n"
                    "Test that the configured credentials work before reading any stream."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CHECK_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT status, message FROM airbyte.main.check_connection("
                    "'source-pokeapi', accept_license := 'ELv2')"
                ),
                description="Check that the source-pokeapi config in the airbyte secret works",
            ),
        ]

    @classmethod
    def rows(cls, params: ProcessParams[ConnectorArgs]) -> list[dict[str, Any]]:
        """Validate, then check."""
        args = params.args
        resolved = args.resolve()
        config = process_config(params.secrets, args)
        try:
            validated(resolved, config, args.timeout_seconds)
        except ValueError as exc:
            return [{"connector": resolved.name, "status": "FAILED", "message": str(exc), "config_valid": False}]
        status = cn.check(resolved, config, args.timeout_seconds)
        return [
            {
                "connector": resolved.name,
                "status": str(status.get("status") or "FAILED"),
                "message": status.get("message"),
                "config_valid": True,
            }
        ]


def discovered(params: ProcessParams[ConnectorArgs]) -> dict[str, Any]:
    """Run discover with the validated config."""
    args = params.args
    resolved = args.resolve()
    config = validated(resolved, process_config(params.secrets, args), args.timeout_seconds)
    return cn.discover(resolved, config, args.timeout_seconds)


@init_single_worker
class Discover(_Once):
    """Run discover and list the streams."""

    SCHEMA = DISCOVER_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "discover"
        description = "Run an Airbyte connector's discovery and list its streams with their columns"
        tags = {
            **object_tags(
                title="Discover Connector Streams",
                category="connectors",
                keywords="airbyte, discover, catalog, streams, schema, sync mode, cursor, primary key",
                doc_llm=(
                    "Run the connector's `discover` command and return one row per stream: stream, namespace, "
                    "table_name, supported_sync_modes, source_defined_cursor, default_cursor_field, "
                    "primary_key, is_resumable, columns (`LIST` of `STRUCT`(name, type) with DuckDB types) and "
                    "json_schema. Use it before read_stream. The config comes from the `airbyte` secret and is "
                    "validated against the spec first. " + CONNECTOR_ARGS_DOC
                ),
                doc_md=(
                    "## discover(connector, ...)\n\n"
                    "The streams a connector offers, with their sync modes, keys, cursors and columns."
                ),
            ),
            "vgi.result_columns_schema": result_columns(DISCOVER_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT stream, supported_sync_modes, primary_key, len(columns) AS column_count "
                    "FROM airbyte.main.discover('source-pokeapi', accept_license := 'ELv2') ORDER BY stream"
                ),
                description="Which streams source-pokeapi offers and how each can sync",
            ),
        ]

    @classmethod
    def rows(cls, params: ProcessParams[ConnectorArgs]) -> list[dict[str, Any]]:
        """Discover and describe each stream."""
        return discover_rows(discovered(params))


@init_single_worker
class CatalogJson(_Once):
    """Run discover and return the catalog JSON."""

    SCHEMA = CATALOG_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "catalog"
        description = "Run an Airbyte connector's discovery and return the catalog JSON"
        tags = {
            **object_tags(
                title="Connector Catalog JSON",
                category="connectors",
                keywords="airbyte, catalog, discover, json, attach, streams",
                doc_llm=(
                    "Run `discover` and return one row: catalog (the AirbyteCatalog JSON) and streams (count). "
                    "Pass the catalog to read_stream(catalog_json := ...) to skip discovery, or to ATTACH "
                    "(catalog getvariable('cat')) to expose every stream as a table. " + CONNECTOR_ARGS_DOC
                ),
                doc_md=(
                    "## catalog(connector, ...)\n\n"
                    "The raw Airbyte catalog, e.g. to attach a connector as a database with "
                    "`catalog getvariable('cat')` after storing it in a variable."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CATALOG_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT streams, catalog->>'$.streams[0].name' AS first_stream "
                    "FROM airbyte.main.catalog('source-pokeapi', accept_license := 'ELv2')"
                ),
                description="How many streams source-pokeapi discovers, and the first one's name",
            ),
        ]

    @classmethod
    def rows(cls, params: ProcessParams[ConnectorArgs]) -> list[dict[str, Any]]:
        """Discover and return the catalog."""
        catalog = discovered(params)
        return [{"catalog": json.dumps(catalog), "streams": len(pr.streams(catalog))}]
