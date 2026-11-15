"""Discovery metadata: the one file to edit when you rename or extend the worker.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Keep every object documented and ``make lint`` stays at 100.

* Catalog-level tags (title, license, author, links, long description) live in
  ``CATALOG_TAGS`` below.
* Every function and table carries ``object_tags(...)``: a title, a category
  from ``SCHEMA_CATEGORIES``, search keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "airbyte"

SOURCE_URL = "https://github.com/Query-farm/vgi-airbyte"

# Categories group functions in docs and agent tools. Every object's
# ``vgi.category`` must name one of these.
SCHEMA_CATEGORIES = [
    {
        "name": "registry",
        "title": "Connector Registry",
        "description": "Browse Airbyte's public connector registry, with each connector's license.",
    },
    {
        "name": "connectors",
        "title": "Connectors",
        "description": "Inspect a connector: its config spec, a connection check, and its streams.",
    },
    {"name": "streams", "title": "Streams", "description": "Read a connector's streams as typed tables, with state."},
    {"name": "debugging", "title": "Debugging", "description": "Raw protocol messages and the type mapping."},
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` JSON for a fixed output schema."""
    from .jsonschema import duckdb_type_name

    return json.dumps(
        [
            {
                "name": f.name,
                "type": duckdb_type_name(f.type),
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in schema
        ]
    )


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


CATALOG_COMMENT = (
    "Run Airbyte source connectors from DuckDB - declarative manifests from the Airbyte registry, local "
    "manifests, or any Docker image speaking the Airbyte protocol - and query their streams as typed tables."
)

CATALOG_DOC_LLM = (
    "Runs Airbyte source connectors and returns their streams as DuckDB tables with real types. The "
    "connector argument is a registry name ('source-pokeapi', optional '@version'), a declarative manifest "
    "path or URL ending in .yaml, 'docker:<image>' (any image speaking the Airbyte protocol; needs Docker) "
    "or 'exec:<path>' (a local executable speaking it). Registry connectors that are manifest-only run "
    "through the airbyte-cdk's source-declarative-manifest, installed once per CDK version into an isolated "
    "cached environment; other registry connectors run their Docker image. License policy: MIT/Apache "
    "connectors run; ELv2 connectors (most of the registry) need accept_license := 'ELv2'; 'Airbyte "
    "Enterprise' connectors are refused. The connector config JSON comes only from a DuckDB secret of type "
    "`airbyte` scoped to 'airbyte://<connector>' and is validated against the connector's spec. Functions: "
    "connectors() lists the registry with licenses, spec/check_connection/discover/catalog inspect a connector, "
    "read_stream returns one stream as rows (columns from its JSON Schema; STATE is the resumable scan "
    "cursor; state := starts an incremental read), final_state returns the state to resume from, and "
    "messages returns raw protocol messages. ATTACH with `connector` and `catalog` options exposes every "
    "stream as a table."
)

CATALOG_DOC_MD = (
    "# Airbyte connectors in DuckDB\n\n"
    "Run [Airbyte](https://airbyte.com) source connectors and query their streams with SQL.\n\n"
    "- **Declarative manifests** (Airbyte's low-code connectors, ~500 in the registry) run through a "
    "pinned airbyte-cdk in an isolated, cached environment.\n"
    "- **Any Airbyte-protocol source** runs as a Docker image or a local executable.\n"
    "- Configs come from DuckDB secrets and are checked against the connector's spec.\n"
    "- Stream JSON Schemas become real DuckDB types; Airbyte STATE drives resumable, incremental reads.\n"
    "- Every registry connector shows its license; ELv2 connectors run only after you acknowledge it."
)

SCHEMA_COMMENT = (
    "Functions that list, inspect and read Airbyte source connectors; attached connector streams appear here as tables."
)

SCHEMA_DOC_LLM = (
    "`main` holds the worker's functions and, when the catalog is attached with `connector` and `catalog` "
    "options, one table per stream of that connector. Connector functions take the connector reference "
    "first and the named arguments secret_name, accept_license, allow_custom_code, cdk_version, "
    "registry_url and timeout_seconds; the reading functions also take state (Airbyte state JSON) and "
    "catalog_json (skip discovery). connectors() and json_schema_type() need no connector or secret."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Functions that list registry connectors, check and discover a connector, and read its streams.\n\n"
    "When a connector is attached with its catalog, each stream is also a table here, and scanning it runs "
    "the connector."
)

DYNAMIC_COLUMNS_MD = (
    "One column per top-level property of the stream's JSON Schema, in schema order, all nullable. "
    "The type depends on the property's schema:\n\n"
    "| Name | Type | Description |\n"
    "| --- | --- | --- |\n"
    "| integer property | BIGINT | `type: integer`, or `number` with `airbyte_type: integer` |\n"
    "| number property | DOUBLE | `type: number` |\n"
    "| boolean property | BOOLEAN | `type: boolean` |\n"
    "| string property | VARCHAR | `type: string` without a known format; also `airbyte_type: big_integer` |\n"
    "| date-time property | TIMESTAMP WITH TIME ZONE | `format: date-time`; offset-less values are UTC |\n"
    "| naive date-time property | TIMESTAMP | `airbyte_type: timestamp_without_timezone` |\n"
    "| date property | DATE | `format: date` |\n"
    "| object property | STRUCT(w BIGINT, h BIGINT) | declared `properties`, no extra keys allowed |\n"
    "| array property | VARCHAR[] | e.g. `items: {type: string}`; items are mapped by the same rules |\n"
    "| open property | JSON | open objects, mixed types, or no type |\n"
)

CONNECTOR_ARGS_DOC = (
    "Named args: secret_name (airbyte secret to use; default the one scoped to 'airbyte://<connector>'), "
    "accept_license (e.g. 'ELv2', required for non-permissive registry connectors), allow_custom_code "
    "(run a manifest's components.py), cdk_version, registry_url and timeout_seconds."
)

_POKE = (
    "Airbyte's declarative PokeAPI connector (registry name source-pokeapi, ELv2 licensed; you may accept "
    'the license) needs no API key: its whole config is {"pokemon_name": "pikachu"}. '
)

AGENT_TEST_TASKS = [
    {
        "name": "datetime_type",
        "prompt": (
            "Using the airbyte worker, which DuckDB type does an Airbyte stream property with JSON Schema "
            '{"type": ["null", "string"], "format": "date-time", "airbyte_type": "timestamp_without_timezone"} '
            "become?"
        ),
    },
    {
        "name": "big_integer_type",
        "prompt": (
            "Using the airbyte worker, what DuckDB type does the property schema "
            '{"type": "string", "airbyte_type": "big_integer"} map to?'
        ),
    },
    {
        "name": "pokeapi_license",
        "prompt": (
            "In the Airbyte connector registry, what license does source-pokeapi have, and what does the "
            "airbyte worker require before it runs it?"
        ),
    },
    {
        "name": "pokeapi_required_config",
        "prompt": "Which config fields does the Airbyte connector source-pokeapi require? Accept its ELv2 license.",
    },
    {"name": "pokeapi_check", "prompt": _POKE + "Does a connection check with that config succeed?"},
    {"name": "pokeapi_streams", "prompt": _POKE + "Which streams does it offer, and what is each one's primary key?"},
    {"name": "pokeapi_catalog_size", "prompt": _POKE + "How many streams are in its discovered Airbyte catalog?"},
    {"name": "pokeapi_pikachu", "prompt": _POKE + "What are pikachu's id, base_experience, height and weight?"},
    {
        "name": "pokeapi_sync_records",
        "prompt": _POKE + "Sync every stream once: how many records does the sync produce in total?",
    },
    {
        "name": "pokeapi_record_messages",
        "prompt": _POKE + "Reading the pokemon stream, how many RECORD protocol messages does the connector print?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Airbyte Connectors",
    "vgi.subtitle": "Run Airbyte source connectors and query their streams as typed DuckDB tables.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "airbyte, connectors, elt, extract, declarative manifest, low-code, cdk, source, state, incremental"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
