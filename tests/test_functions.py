"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi.arguments import Arguments
from vgi.client import Client

from airbyte_worker import connector as cn
from airbyte_worker import protocol as pr
from airbyte_worker import registry as reg
from airbyte_worker.catalog import connector_attach
from airbyte_worker.common import ScanCursor, read_items
from airbyte_worker.jsonschema import (
    JSON_TYPE,
    NAIVE_TIMESTAMP_TYPE,
    TIMESTAMP_TYPE,
    Coercer,
    arrow_type,
    duckdb_type_name,
    records_to_batch,
)
from airbyte_worker.raw import message_row
from airbyte_worker.schema_type import schema_type_names

ROOT = Path(__file__).resolve().parent.parent
FAKE = str(ROOT / "test" / "fake_source" / "source-fake")
FIXTURES = ROOT / "test" / "fixtures"
REGISTRY_URL = (FIXTURES / "oss_registry.json").as_uri()
SCHEMA = ["main"]
GOOD = json.dumps({"password": "s3cret"})


# ------------------------------------------------------------ JSON Schema mapping


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        ({"type": "integer"}, pa.int64()),
        ({"type": ["null", "number"], "airbyte_type": "integer"}, pa.int64()),
        ({"type": ["integer", "number"]}, pa.float64()),
        ({"type": "string", "airbyte_type": "big_integer"}, pa.string()),
        ({"type": "string", "format": "date-time"}, TIMESTAMP_TYPE),
        ({"type": "string", "format": "date-time", "airbyte_type": "timestamp_with_timezone"}, TIMESTAMP_TYPE),
        ({"type": "string", "format": "date-time", "airbyte_type": "timestamp_without_timezone"}, NAIVE_TIMESTAMP_TYPE),
        ({"type": "string", "format": "date"}, pa.date32()),
        ({"type": ["string", "integer"]}, JSON_TYPE),
        ({"type": "object", "additionalProperties": True}, JSON_TYPE),
        ({"type": "object", "properties": {"a": {"type": "integer"}}}, pa.struct([pa.field("a", pa.int64())])),
        ({"type": "array", "items": {"type": "string"}}, pa.list_(pa.string())),
    ],
)
def test_arrow_type(schema: dict[str, Any], expected: pa.DataType) -> None:
    assert arrow_type(schema) == expected


def test_duckdb_type_names() -> None:
    assert duckdb_type_name(NAIVE_TIMESTAMP_TYPE) == "TIMESTAMP"
    assert duckdb_type_name(TIMESTAMP_TYPE) == "TIMESTAMP WITH TIME ZONE"
    assert schema_type_names(['{"type": "number", "airbyte_type": "integer"}', None, "{"]) == ["BIGINT", None, None]


def test_records_to_batch_coerces_and_counts_rejects() -> None:
    schema = pa.schema([pa.field("n", pa.int64()), pa.field("t", NAIVE_TIMESTAMP_TYPE), pa.field("z", TIMESTAMP_TYPE)])
    coercer = Coercer()
    batch = records_to_batch(
        [{"n": 7.0, "t": "2026-01-02T03:04:05", "z": "2026-01-02T03:04:05Z"}, {"n": "x", "t": None}],
        schema,
        coercer,
    )
    assert batch.column("n").to_pylist() == [7, None]
    assert batch.column("t").to_pylist()[0] == dt.datetime(2026, 1, 2, 3, 4, 5)
    assert batch.column("z").to_pylist()[0] == dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=dt.UTC)
    assert coercer.rejected == 1


# --------------------------------------------------------------- references


@pytest.mark.parametrize(
    ("text", "kind", "target", "version"),
    [
        ("source-pokeapi", "registry", "source-pokeapi", ""),
        ("pokeapi@0.3.79", "registry", "pokeapi", "0.3.79"),
        ("/tmp/x/manifest.yaml", "manifest", "/tmp/x/manifest.yaml", ""),
        ("https://example.com/m.yml", "manifest", "https://example.com/m.yml", ""),
        ("manifest:/tmp/m", "manifest", "/tmp/m", ""),
        ("docker:airbyte/source-faker:6.2.0", "docker", "airbyte/source-faker:6.2.0", ""),
        (FAKE, "exec", FAKE, ""),
        ("exec:/bin/x", "exec", "/bin/x", ""),
    ],
)
def test_parse_ref(text: str, kind: str, target: str, version: str) -> None:
    assert cn.parse_ref(text) == cn.ConnectorRef(kind, target, version)


@pytest.mark.parametrize("text", ["", "  ", "docker:", "bad name", "/etc/hosts", "src@bad version"])
def test_parse_ref_rejects(text: str) -> None:
    with pytest.raises(ValueError):
        cn.parse_ref(text)


# ------------------------------------------------------------------ registry


def test_registry_and_license_policy() -> None:
    entries = {e.name: e for e in reg.load_registry(REGISTRY_URL)}
    assert entries["source-fixture"].manifest_only
    assert not entries["source-dockeronly"].manifest_only
    assert reg.find_connector("fixture-elv2", REGISTRY_URL).license == "ELv2"
    reg.check_license(entries["source-fixture"], "")
    reg.check_license(entries["source-dockeronly"], "")
    with pytest.raises(reg.LicenseError, match="accept_license := 'ELv2'"):
        reg.check_license(entries["source-fixture-elv2"], "")
    reg.check_license(entries["source-fixture-elv2"], "elv2")
    with pytest.raises(reg.LicenseError, match="commercial"):
        reg.check_license(entries["source-fake-enterprise"], "Airbyte Enterprise")
    assert reg.license_policy("Some-License") == "requires accept_license := 'Some-License'"
    assert reg.license_policy("") == "requires accept_license := 'unknown'"
    with pytest.raises(reg.RegistryError, match="no source named"):
        reg.find_connector("source-nope", REGISTRY_URL)


def test_manifest_url_follows_registry_layout() -> None:
    entry = reg.find_connector("source-fixture", REGISTRY_URL)
    url = reg.manifest_url(entry, "https://connectors.airbyte.com/files/registries/v0/oss_registry.json")
    assert url == "https://connectors.airbyte.com/files/metadata/airbyte/source-fixture/0.1.0/manifest.yaml"
    assert reg.manifest_url(entry, "https://m/files/registries/v0/r.json", "9.9.9").endswith("/9.9.9/manifest.yaml")


def test_resolve_local_manifest_and_custom_code() -> None:
    resolved = cn.resolve(str(FIXTURES / "manifest.yaml"), cn.Options())
    assert resolved.mode == "manifest" and resolved.manifest is not None
    spec = cn.spec(resolved)
    assert spec["connectionSpecification"]["required"] == ["base_url", "api_token"]
    assert cn.secret_fields(spec["connectionSpecification"]) == ["api_token"]
    with pytest.raises(ValueError, match="allow_custom_code"):
        cn.resolve(str(FIXTURES / "manifest-custom.yaml"), cn.Options())
    with pytest.raises(ValueError, match=r"components\.py could not be read"):
        cn.resolve(str(FIXTURES / "manifest-custom.yaml"), cn.Options(allow_custom_code=True))


# ------------------------------------------------------------- config validation


def test_validate_config_never_echoes_values() -> None:
    spec = cn.spec(cn.resolve(str(FIXTURES / "manifest.yaml"), cn.Options()))["connectionSpecification"]
    with pytest.raises(ValueError) as info:
        cn.validate_config({"base_url": 5, "api_token": 99, "page_size": "TOPSECRET"}, spec, "fixture")
    text = str(info.value)
    assert "TOPSECRET" not in text and "99" not in text
    assert "base_url must be of type string" in text and "page_size must be of type integer" in text
    cn.validate_config({"base_url": "http://x", "api_token": "t"}, spec, "fixture")


def test_secret_fields_nested_oneof() -> None:
    spec = {
        "properties": {
            "credentials": {
                "type": "object",
                "oneOf": [
                    {"properties": {"api_key": {"type": "string", "airbyte_secret": True}}},
                    {"properties": {"client_secret": {"type": "string", "airbyte_secret": True}}},
                ],
            },
            "password": {"airbyte_secret": True},
        }
    }
    assert cn.secret_fields(spec) == ["credentials.api_key", "credentials.client_secret", "password"]


# --------------------------------------------------------------------- protocol


STREAM = {
    "name": "orders",
    "json_schema": {"type": "object", "properties": {"id": {}, "updated_at": {}, "amount": {}, "status": {}}},
    "supported_sync_modes": ["full_refresh", "incremental"],
    "source_defined_cursor": True,
    "default_cursor_field": ["updated_at"],
    "source_defined_primary_key": [["id"]],
}


def test_configure_projection_and_modes() -> None:
    cfg = pr.configure(STREAM, columns=["amount"])["streams"][0]
    assert cfg["sync_mode"] == "incremental" and cfg["cursor_field"] == ["updated_at"]
    assert list(cfg["stream"]["json_schema"]["properties"]) == ["id", "updated_at", "amount"]
    assert cfg["primary_key"] == [["id"]]
    full = pr.configure(STREAM, columns=None, sync_mode="full_refresh")["streams"][0]
    assert full["sync_mode"] == "full_refresh" and "cursor_field" not in full
    with pytest.raises(ValueError, match="does not support"):
        pr.configure({**STREAM, "supported_sync_modes": ["full_refresh"]}, columns=None, sync_mode="incremental")
    with pytest.raises(ValueError, match="cursor_field"):
        pr.configure({"name": "x", "supported_sync_modes": ["incremental"]}, columns=None)


def test_load_catalog_accepts_message_and_configured_forms() -> None:
    plain = {"streams": [STREAM]}
    assert pr.load_catalog(json.dumps(plain)) == plain
    assert pr.load_catalog(json.dumps({"type": "CATALOG", "catalog": plain})) == plain
    assert pr.load_catalog(json.dumps(pr.configure(STREAM, columns=None)))["streams"][0]["name"] == "orders"
    with pytest.raises(ValueError):
        pr.load_catalog("[]")


def test_find_stream_and_table_names() -> None:
    cat = {"streams": [{"name": "a", "namespace": "x"}, {"name": "a", "namespace": "y"}, {"name": "b"}]}
    assert set(pr.table_names(cat)) == {"x_a", "y_a", "b"}
    assert pr.find_stream(cat, "y.a")["namespace"] == "y"
    with pytest.raises(ValueError, match="ambiguous"):
        pr.find_stream(cat, "a")


def _stream_state(name: str, value: str) -> dict[str, Any]:
    return {"type": "STREAM", "stream": {"stream_descriptor": {"name": name}, "stream_state": {"c": value}}}


def test_state_tracker_merges_per_stream_global_legacy() -> None:
    t = pr.StateTracker([_stream_state("a", "1")])
    t.add({**_stream_state("b", "2"), "sourceStats": {"recordCount": 3.0}})
    t.add(_stream_state("a", "3"))
    value = t.value()
    assert [s["stream"]["stream_state"]["c"] for s in value] == ["3", "2"]
    assert all("sourceStats" not in s for s in value) and t.messages == 2
    t.add({"type": "GLOBAL", "global": {"shared_state": {}, "stream_states": []}})
    assert [s["type"] for s in t.value()] == ["GLOBAL"]
    t.add({"data": {"x": 1}})
    assert t.value() == [{"type": "LEGACY", "data": {"x": 1}}]
    assert pr.parse_state('{"a": 1}') == [{"type": "LEGACY", "data": {"a": 1}}]
    assert pr.parse_state("") == []
    with pytest.raises(ValueError):
        pr.parse_state("3")


def test_message_row_decodes_types() -> None:
    row = message_row({"type": "TRACE", "trace": {"type": "ERROR", "emitted_at": 0, "error": {"message": "boom"}}})
    assert row["error_message"] == "boom" and row["emitted_at"] == dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    assert (
        pr.error_text({"type": "TRACE", "trace": {"type": "ERROR", "error": {"message": "m", "internal_message": "i"}}})
        == "m (i)"
    )
    assert pr.parse_message("banner") is None and pr.parse_message('{"x": 1}') is None


# ------------------------------------------------------------ running connectors


def _fake() -> cn.Resolved:
    return cn.resolve(FAKE, cn.Options())


def test_run_fake_source_commands() -> None:
    resolved = _fake()
    assert cn.spec(resolved)["connectionSpecification"]["required"] == ["password"]
    assert cn.check(resolved, GOOD)["status"] == "SUCCEEDED"
    assert cn.check(resolved, json.dumps({"password": "x"}))["status"] == "FAILED"
    catalog = cn.discover(resolved, GOOD)
    assert [s["name"] for s in pr.streams(catalog)] == ["orders", "customers"]


def test_connector_error_carries_trace_message() -> None:
    resolved = _fake()
    stream = pr.find_stream(cn.discover(resolved, GOOD), "orders")
    run = cn.ConnectorRun.start(
        resolved,
        "read",
        config=json.dumps({"password": "s3cret", "fail_after": 1}),
        catalog=pr.configure(stream, columns=None),
    )
    with pytest.raises(pr.ConnectorError, match="returned HTTP 500"):
        while run.next_message() is not None:
            pass


def _read_all(cursor: ScanCursor, catalog: dict[str, Any], *, restart_each_batch: bool) -> list[int]:
    resolved = _fake()
    config = json.dumps({"password": "s3cret", "orders_rows": 23, "state_every": 5})
    ids: list[int] = []
    stream = pr.find_stream(catalog, "orders")
    while not cursor.done:
        items = read_items(
            cursor,
            resolved=resolved,
            config=config,
            catalog=pr.configure(stream, columns=None),
            timeout_seconds=30,
            items_of=lambda m: [m["record"]["data"]] if m["type"] == "RECORD" else [],
            max_items=7,
        )
        ids.extend(i["id"] for i in items)
        if restart_each_batch and not cursor.done:
            # Another process takes the next batch: the live run is gone, only the cursor travels.
            cursor = ScanCursor.deserialize_from_bytes(cursor.serialize_to_bytes())
            cn.drop_run(cursor.token)
    return ids


def test_scan_resumes_from_externalized_state() -> None:
    catalog = cn.discover(_fake(), GOOD)
    assert _read_all(ScanCursor(), catalog, restart_each_batch=False) == list(range(1, 24))
    assert _read_all(ScanCursor(), catalog, restart_each_batch=True) == list(range(1, 24))


def test_private_files_are_removed() -> None:
    run = cn.ConnectorRun.start(_fake(), "check", config=GOOD)
    directory = Path(run.directory)
    assert oct(directory.stat().st_mode & 0o777) == "0o700"
    assert oct((directory / "config.json").stat().st_mode & 0o777) == "0o600"
    while run.next_message() is not None:
        pass
    assert not directory.exists()


def test_docker_mode_without_daemon(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cn, "docker_available", lambda: False)
    resolved = cn.resolve("docker:airbyte/source-faker:6.2.0", cn.Options())
    with pytest.raises(pr.ConnectorError, match="no Docker daemon"):
        cn.run_to_end(resolved, "spec")


# ------------------------------------------------------------------- ATTACH


def test_connector_attach_options() -> None:
    assert connector_attach({}) is None
    attach = connector_attach(
        {"connector": FAKE, "catalog": str(FIXTURES / "fake-catalog.json"), "allow_custom_code": "true"}
    )
    assert attach is not None and attach.allow_custom_code and len(pr.streams(attach.catalog)) == 2
    with pytest.raises(ValueError, match="needs a catalog"):
        connector_attach({"connector": FAKE})


# -------------------------------------------------------- through the worker


def _table(client: Client, name: str, *positional: Any, **named: Any) -> pa.Table:
    arguments = Arguments(
        positional=tuple(pa.scalar(v) for v in positional),
        named={k: pa.scalar(v) for k, v in named.items()},
    )
    batches = list(client.table_function(function_name=name, schema_path=SCHEMA, arguments=arguments))
    return pa.Table.from_batches(batches)


def test_connectors_through_worker(client: Client) -> None:
    table = _table(client, "connectors", registry_url=REGISTRY_URL)
    rows = {r["name"]: r for r in table.to_pylist()}
    assert rows["source-fixture-elv2"]["license_policy"] == "requires accept_license := 'ELv2'"
    assert rows["source-fixture"]["manifest_only"] is True


def test_spec_through_worker(client: Client) -> None:
    row = _table(client, "spec", str(FIXTURES / "manifest.yaml")).to_pylist()[0]
    assert row["mode"] == "manifest" and row["secret_fields"] == ["api_token"]
