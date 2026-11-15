<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Run Airbyte Source Connectors and Query Their Streams in DuckDB

> **vgi-airbyte** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-airbyte/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-airbyte/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[Airbyte](https://airbyte.com) publishes about 600 source connectors. Most of them (over
500) are **declarative manifests**: a `manifest.yaml` that Airbyte's Python CDK turns into
a working connector. This worker runs them, or any other source that speaks the
[Airbyte protocol](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol), from
DuckDB, and returns their streams as typed tables.

- **Declarative manifests first.** Registry connectors that are manifest-only, local
  manifests and manifest URLs run through the CDK's `source-declarative-manifest`, pinned
  to `airbyte-cdk==7.48.0` and installed once into its own cached virtual environment.
  The CDK and its dependencies (pandas, nltk, Google Cloud libraries) never enter the
  worker's environment.
- **Any Airbyte-protocol source.** A Docker image (`docker:airbyte/source-faker:6.2.0`)
  or a local executable (`exec:/path/to/source`) that implements `spec`, `check`,
  `discover` and `read`.
- **Licenses are visible and enforced.** `connectors()` shows every registry connector's
  license. ELv2 connectors run only after you acknowledge the license; see
  [License policy](#license-policy).
- **Configs from secrets only**, checked against the connector's spec before it runs. The
  error names missing or mistyped fields and never quotes a value.
- **Real types.** Stream JSON Schemas (with Airbyte's `airbyte_type` hints) become
  `BIGINT`, `DOUBLE`, `BOOLEAN`, `VARCHAR`, `TIMESTAMPTZ`, `TIMESTAMP`, `DATE`, `STRUCT`,
  `LIST`, or `JSON` for open shapes.
- **Incremental and resumable.** Airbyte STATE (per-stream, global or legacy) is the scan
  cursor; pass a saved state to read only what changed.

## Quick start

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'airbyte' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-airbyte@v0.1.0 vgi-airbyte-worker');

-- What is in the registry, and under which licenses?
SELECT name, version, license, license_policy
FROM airbyte.main.connectors() WHERE manifest_only ORDER BY name;

-- PokeAPI needs no key; its whole config is the pokemon to fetch.
CREATE SECRET poke (TYPE airbyte, SCOPE 'airbyte://source-pokeapi', CONFIG '{"pokemon_name": "pikachu"}');

-- ELv2-licensed: acknowledge the license to run it.
SELECT id, name, base_experience, height, weight
FROM airbyte.main.read_stream('source-pokeapi', 'pokemon', accept_license := 'ELv2');
```

From a source checkout: `make build`, then `LOCATION '/abs/path/to/.venv/bin/vgi-airbyte-worker'`.

## Connectors

The first argument of every connector function says which connector to run:

| Reference | Runs |
|---|---|
| `source-pokeapi`, `pokeapi`, `source-pokeapi@0.3.79` | A registry connector at its current (or the given) version. Manifest-only connectors run through the CDK; the others run their Docker image. |
| `/path/to/manifest.yaml`, `manifest:/path/to/file` | A local declarative manifest. |
| `https://…/manifest.yaml` | A declarative manifest at a URL (versioned URLs are cached on disk). |
| `docker:airbyte/source-faker:6.2.0` | Any image speaking the Airbyte protocol (`docker run --rm -i`). Needs a Docker daemon; without one the query fails with a clear error. |
| `exec:/path/to/source` | A local executable speaking the Airbyte protocol. |

Registry lookups read `https://connectors.airbyte.com/files/registries/v0/oss_registry.json`
(cached for an hour, and on disk for offline use). Manifest-only connectors' manifests are
fetched from the same `/files/metadata/<image>/<version>/manifest.yaml` tree.
`registry_url :=` points at a mirror with the same layout.

Manifests that reference custom Python components (`components.py`) run arbitrary code,
so they only run with `allow_custom_code := true` (the worker then fetches the
`components.py` next to the manifest and enables the CDK's `AIRBYTE_ENABLE_UNSAFE_CODE`).
Otherwise run the connector's Docker image.

Environments and downloads live under `~/Library/Caches/vgi-airbyte` (macOS) or
`$XDG_CACHE_HOME/vgi-airbyte`; set `VGI_AIRBYTE_CACHE_DIR` to move them. The CDK
environment is created with `uv` (Python 3.13) when `uv` is on `PATH`, otherwise with
`python -m venv` and `pip`. `cdk_version :=` selects another airbyte-cdk release.

## Functions

All in `airbyte.main`. Connector functions take the connector first plus the named
arguments `secret_name`, `accept_license`, `allow_custom_code`, `cdk_version`,
`registry_url` and `timeout_seconds`.

| Function | Returns |
|---|---|
| `connectors(registry_url :=)` | One row per registry source: name, display name, image, version, language, `manifest_only`, release stage, support level, **license**, **license_policy**, source type, docs URL, manifest URL, tags. |
| `spec(connector)` | The config the connector expects: required fields, `airbyte_secret` fields, each property's type/title/description/default, and the JSON Schema. Needs no secret. |
| `check_connection(connector)` | Validates the config against the spec, then runs `check`: `SUCCEEDED` / `FAILED`, message, `config_valid`. |
| `discover(connector)` | One row per stream: sync modes, cursor, primary key, columns with DuckDB types, JSON Schema. |
| `catalog(connector)` | The whole AirbyteCatalog as `JSON` (one row), for `catalog_json :=` or ATTACH. |
| `read_stream(connector, stream, state :=, sync_mode :=, cursor_field :=, catalog_json :=)` | The stream as a typed table. |
| `final_state(connector, stream :=, state :=, catalog_json :=)` | Runs a sync and returns the merged final state, record and state-message counts, streams. |
| `messages(connector, command :=, stream :=, state :=)` | Every raw AirbyteMessage (RECORD, STATE, LOG, TRACE, …) of `read`, `check`, `discover` or `spec`, decoded into columns. |
| `json_schema_type(schema)` | Scalar: the DuckDB type a property's JSON Schema maps to. |

### Secrets

```sql
-- Bound to one connector by scope: 'airbyte://' + the connector argument as written.
CREATE SECRET gh (TYPE airbyte, SCOPE 'airbyte://source-github', CONFIG '{"credentials": {...}, "repositories": ["duckdb/duckdb"]}');
CREATE SECRET mine (TYPE airbyte, SCOPE 'airbyte:///srv/manifests/manifest.yaml', CONFIG '{"api_key": "..."}');
-- Or chosen by name.
SELECT * FROM airbyte.main.read_stream('source-github', 'issues', secret_name := 'gh');
```

The worker asks DuckDB for the `airbyte` secret whose scope best matches
`airbyte://<connector>`; a secret without a scope applies to every connector. Without any
secret the connector gets `{}`. Before the connector runs, the config is validated against
its spec (from the manifest, the registry, or the connector's `spec` command); the error
lists missing fields, wrong types, enum and range violations without quoting any value.
The config is written to a 0600 file in a 0700 temporary directory (mounted read-only
into Docker containers) and deleted when the connector exits. It never appears in scan
state or logs, and `duckdb_secrets()` shows it redacted.

### License policy

Running a connector means running its code, so every connector resolved from the
registry is checked against its registry `license` first:

| License | Policy |
|---|---|
| MIT, Apache-2.0, BSD, ISC, MIT-0, 0BSD | Runs. |
| ELv2 (Elastic License 2.0; most Airbyte connectors) | Runs only with `accept_license := 'ELv2'` (or the `accept_license` ATTACH option). |
| Airbyte Enterprise | Refused: it needs a commercial agreement with Airbyte. |
| anything else, or none | Runs only with `accept_license :=` naming that exact license. |

ELv2 lets you run a connector on your own data, but forbids providing it to third
parties as a hosted or managed service. Acknowledging the license is your statement that
your use fits it; if you deploy this worker as a shared HTTP service for others, ELv2
connectors are likely out of bounds. Manifests, images and executables you name directly
are your own choice of code and are not checked. The `license` and `license_policy`
columns of `connectors()` show where each connector stands.

### Attach a connector as a database

Catalog discovery needs the connector's config, which catalog listing cannot see, so pass
an Airbyte catalog at ATTACH (a file path, or the JSON itself):

```sql
SET VARIABLE cat = (SELECT catalog::VARCHAR FROM airbyte.main.catalog('source-pokeapi', accept_license := 'ELv2'));
ATTACH 'airbyte' AS poke (TYPE vgi, LOCATION '...', connector 'source-pokeapi', accept_license 'ELv2',
                          catalog getvariable('cat'));
SELECT name, base_experience FROM poke.pokemon;
```

Options: `connector`, `catalog`, `secret_name`, `accept_license`, `allow_custom_code`,
`cdk_version`, `registry_url`, `timeout_seconds`. Each stream becomes a table in `main`
(named `<namespace>_<stream>` when names collide); a scan runs `read_stream` with the
attach's settings, so the same secret lookup, license policy, pushdown and resume apply.

### Incremental reads

```sql
-- Sync once and keep the state.
SET VARIABLE st = (SELECT final_state::VARCHAR FROM airbyte.main.final_state('source-x', stream := 'issues'));
-- Next time, read only what changed since.
SELECT * FROM airbyte.main.read_stream('source-x', 'issues', state := getvariable('st'));
```

`state :=` takes the protocol's JSON array of state messages (`STREAM`, `GLOBAL` or
`LEGACY`), a single state message, or a bare legacy state object. `final_state` merges the
run's STATE messages the way the Airbyte platform does: the latest state per stream, or
the latest global / legacy state. `sync_mode := 'auto'` (the default) reads incrementally
when the stream supports it, so the connector checkpoints; `'full_refresh'` re-reads
everything and ignores `state`; `cursor_field :=` sets the cursor for sources without a
default. Note that the current airbyte-cdk ignores `LEGACY` input state, so declarative
manifests need per-stream state.

## What pushes down

- **Stream selection.** The configured catalog selects only the requested stream.
- **Projection.** The selected stream's `json_schema` keeps only the queried columns plus
  the primary key and cursor, which is how the Airbyte platform expresses column
  selection. Most connectors still fetch whole records; the worker drops the rest.
- **Incremental cursor.** `state :=` is passed as `--state`; declarative connectors turn
  it into the API's own filter (e.g. `updated_since=` request parameters).
- **LIMIT** stops reading early: once DuckDB stops pulling, the connector blocks on its
  pipe and is stopped after 10 idle minutes or when the worker exits.
- **Filters** do not push down: the Airbyte protocol has no filter language. Use
  `state :=` to start from a cursor.

## Type mapping

| JSON Schema | DuckDB |
|---|---|
| `integer`, or `number` + `airbyte_type: integer` | `BIGINT` |
| `number` | `DOUBLE` |
| `boolean` | `BOOLEAN` |
| `string` (also `airbyte_type: big_integer`) | `VARCHAR` |
| `string`, `format: date-time` | `TIMESTAMP WITH TIME ZONE` (offset-less values are UTC) |
| … + `airbyte_type: timestamp_without_timezone` | `TIMESTAMP` |
| `string`, `format: date` / `time` | `DATE` / `TIME` |
| `object` with `properties`, no extra keys | `STRUCT(...)`, up to 4 levels deep |
| `array` with `items` | `LIST` of the item type |
| open objects, several types, no type | `JSON` |

Every column is nullable. A value that does not fit its column becomes NULL and the count
is logged to stderr. Keys not declared in a STRUCT are dropped; use a `JSON` property to
keep everything.

## How scans run and resume

The connector runs as a subprocess (`source-declarative-manifest`, `docker run`, or the
executable). A reader thread moves its stdout into a bounded queue and rows go to DuckDB
in batches of up to 1,000, or sooner when the connector is slow. Non-JSON stdout lines are
skipped. An `ERROR` TRACE message fails the query with the connector's message (and its
internal message) once the connector exits; otherwise a non-zero exit quotes the last
ERROR/FATAL log or stderr. A connector that prints nothing for `timeout_seconds` (default
300; 0 disables) is stopped; Docker containers are removed with `docker rm -f`.

The scan cursor holds the cumulative Airbyte state and the number of rows read after the
last STATE, never the config. In one worker process the connector keeps running between
batches. If a batch lands on another process (HTTP deployments), the connector is
restarted with `--state <cumulative state>` and the rows already returned after that
checkpoint are skipped. This relies on the connector replaying the same records from the
same state (the Airbyte checkpointing contract); a connector that returns fewer records
on resume fails the scan rather than silently dropping rows.

## Testing

`make check` runs ruff, mypy, pydoclint, the unit tests, the SQL suite over stdio,
launch and HTTP transports, and the metadata lint. CI needs no Docker and no network
beyond PyPI (for the pinned airbyte-cdk).

- `test/fixture_api/server.py` is a hermetic REST API with bearer auth, cursor and offset
  pagination, an `updated_since` filter and a 403 endpoint, plus a mirror of the registry
  layout (`test/fixtures/oss_registry.json`: MIT, ELv2, Enterprise, custom-code and
  Docker-only fixture connectors).
- `test/fixtures/manifest.yaml` is a declarative manifest against it (pagination, auth,
  an incremental `DatetimeBasedCursor`), run through the real airbyte-cdk.
- `test/fake_source/source-fake` is a hermetic Airbyte-protocol executable: spec / check /
  discover / read, per-stream, global and legacy state, LOG and TRACE errors, Airbyte
  types, and a log of the configured catalog it received.
- Opt-in: `AIRBYTE_TEST_DOCKER=1 make test-sql` builds `test/fake_source/Dockerfile` and
  runs `test/sql/docker.test` through Docker mode; `AIRBYTE_LIVE=1 make test-sql` runs
  `test/sql/live.test` against the real registry and the PokeAPI connector. Both are
  expected skips otherwise.
- The metadata lint runs with execution disabled (`vgi-lint.toml`): every connector
  example needs network access and usually connector credentials.

## Support

Open an issue at https://github.com/Query-farm/vgi-airbyte/issues.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Licensed under the BSD Zero Clause License (0BSD); see [LICENSE](LICENSE). Airbyte
connectors are licensed separately by their authors (see `connectors()`).
