"""Assemble the functions into a catalog and run the worker.

To add a function: write it, then add the class to ``FUNCTIONS`` below.

Run it:

    vgi-airbyte-worker                      # stdio: what DuckDB launches by default
    vgi-airbyte-worker --http --port 8000   # HTTP: for Docker / hosted deploys
    vgi-airbyte-worker --help

Attach it from DuckDB:

    ATTACH 'airbyte' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-airbyte-worker');
    ATTACH 'airbyte' AS poke (TYPE vgi, LOCATION '...', connector 'source-pokeapi',
                              accept_license 'ELv2', catalog '/path/catalog.json');
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from .catalog import ConnectorCatalog
from .common import AIRBYTE_SECRET_TYPE
from .connector import DEFAULT_CDK_VERSION, DEFAULT_TIMEOUT_SECONDS
from .inspect_fns import CatalogJson, Check, Discover, Spec
from .listing import Connectors
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_COMMENT,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .raw import Messages
from .read_stream import FinalState, ReadStream
from .registry import DEFAULT_REGISTRY_URL
from .schema_type import JsonSchemaType

FUNCTIONS: list[type[Any]] = [
    Connectors,
    Spec,
    Check,
    Discover,
    CatalogJson,
    ReadStream,
    FinalState,
    Messages,
    JsonSchemaType,
]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries`` (with descriptions).

    Overloads share one SQL name, so their examples are merged per name.
    Returns every example, for the schema-level list.
    """
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entry for entries in by_name.values() for entry in entries]


_ALL_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class AirbyteCatalog(ConnectorCatalog):
    """The ``airbyte`` catalog: the functions, plus one table per stream of an attached connector."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_COMMENT,
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "Airbyte Connector Functions",
                    "domain": "data integration",
                    "vgi.keywords": keywords_array(
                        "airbyte, connectors, registry, spec, check, discover, read, state, manifest"
                    ),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_ALL_EXAMPLES),
                },
            )
        ],
    )


class AirbyteWorker(Worker):
    """The worker process: the ``airbyte`` catalog, its secret type and ATTACH options."""

    catalog_interface = AirbyteCatalog
    catalog_name = CATALOG_NAME
    secret_types = [AIRBYTE_SECRET_TYPE]

    class AttachOptions:
        """Options for ``ATTACH 'airbyte' AS name (TYPE vgi, LOCATION ..., connector '...', catalog '...')``."""

        connector: Annotated[
            str,
            AttachOption(
                desc="Connector reference (registry name, manifest path/URL, docker:image, exec:path). "
                "With catalog, every stream becomes a table."
            ),
        ] = ""
        catalog: Annotated[
            str,
            AttachOption(desc="Airbyte catalog for the connector: a file path or the catalog JSON itself."),
        ] = ""
        secret_name: Annotated[
            str,
            AttachOption(
                desc="Name of the airbyte secret with the config; empty uses the 'airbyte://<connector>' scope."
            ),
        ] = ""
        accept_license: Annotated[
            str, AttachOption(desc="Acknowledge a registry connector's non-permissive license, e.g. 'ELv2'.")
        ] = ""
        allow_custom_code: Annotated[
            bool, AttachOption(desc="Allow the manifest's custom Python components (components.py) to run.")
        ] = False
        cdk_version: Annotated[str, AttachOption(desc="airbyte-cdk version that runs declarative manifests.")] = (
            DEFAULT_CDK_VERSION
        )
        registry_url: Annotated[str, AttachOption(desc="Connector registry JSON used to resolve registry names.")] = (
            DEFAULT_REGISTRY_URL
        )
        timeout_seconds: Annotated[
            int, AttachOption(desc="Stop a connector that prints nothing for this many seconds; 0 waits forever.")
        ] = DEFAULT_TIMEOUT_SECONDS


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    AirbyteWorker.main()


if __name__ == "__main__":
    main()
