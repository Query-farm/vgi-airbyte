"""``connectors()``: the Airbyte connector registry, one row per source, with its license."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

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
from vgi_rpc import ArrowSerializableDataclass

from . import registry as reg
from .meta import column, object_tags, result_columns

CONNECTORS_SCHEMA = pa.schema(
    [
        column(
            "name",
            pa.string(),
            "Connector name to pass as the connector argument, e.g. source-pokeapi.",
            nullable=False,
        ),
        column("display_name", pa.string(), "Human-readable connector name."),
        column("definition_id", pa.string(), "Airbyte source definition id (UUID)."),
        column("docker_image", pa.string(), "Docker image and tag of the current version.", nullable=False),
        column("version", pa.string(), "Current version (the image tag).", nullable=False),
        column("language", pa.string(), "Implementation: manifest-only, python or java."),
        column(
            "manifest_only",
            pa.bool_(),
            "True when it is a declarative manifest the worker runs without Docker.",
            nullable=False,
        ),
        column("release_stage", pa.string(), "alpha, beta or generally_available."),
        column("support_level", pa.string(), "community or certified."),
        column("license", pa.string(), "License from the registry metadata, e.g. MIT or ELv2."),
        column(
            "license_policy",
            pa.string(),
            "What the worker requires before running it under that license.",
            nullable=False,
        ),
        column("source_type", pa.string(), "api, database, file, ..."),
        column("documentation_url", pa.string(), "Airbyte documentation page."),
        column("manifest_url", pa.string(), "URL of the declarative manifest.yaml (manifest-only connectors)."),
        column("tags", pa.list_(pa.string()), "Registry tags, e.g. language:manifest-only, cdk:low-code."),
    ]
)


@dataclass(slots=True, frozen=True, kw_only=True)
class ConnectorsArgs:
    """Arguments for ``connectors(registry_url := ...)``."""

    registry_url: Annotated[
        str,
        Arg(
            "registry_url",
            default=reg.DEFAULT_REGISTRY_URL,
            doc="Connector registry JSON to list (a mirror with the same layout works).",
            pattern=r"^(https?|file)://",
        ),
    ]


@dataclass(kw_only=True)
class OnceState(ArrowSerializableDataclass):
    """Cursor: whether the rows were produced."""

    done: bool = False


def _manifest_url(entry: reg.RegistryEntry, registry_url: str) -> str | None:
    if not entry.manifest_only:
        return None
    try:
        return reg.manifest_url(entry, registry_url)
    except reg.RegistryError:
        return None  # a registry file outside the /files/registries/ layout


@init_single_worker
class Connectors(TableFunctionGenerator[ConnectorsArgs, OnceState]):
    """List the sources of the Airbyte connector registry."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "connectors"
        description = "List Airbyte registry source connectors with version, release stage and license"
        tags = {
            **object_tags(
                title="Airbyte Connector Registry",
                category="registry",
                keywords="airbyte, registry, connectors, sources, license, elv2, mit, manifest-only, low-code",
                doc_llm=(
                    "Return one row per source in Airbyte's public connector registry: name (pass it as the "
                    "connector argument of the other functions), display_name, definition_id, docker_image, "
                    "version, language, manifest_only (runs without Docker through the declarative CDK), "
                    "release_stage, support_level, license, license_policy (what the worker requires: "
                    "'runs', \"requires accept_license := 'ELv2'\", or refused), source_type, "
                    "documentation_url, manifest_url and tags. registry_url := points at a mirror. The "
                    "registry is cached for an hour (and on disk for offline use)."
                ),
                doc_md=(
                    "## connectors(registry_url := ...)\n\n"
                    "Airbyte's connector registry as a table, including each connector's license and what "
                    "the worker requires before running it."
                ),
            ),
            "vgi.result_columns_schema": result_columns(CONNECTORS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT license, count(*) AS connectors, count(*) FILTER (WHERE manifest_only) AS manifest_only "
                    "FROM airbyte.main.connectors() GROUP BY license ORDER BY connectors DESC"
                ),
                description="How many registry connectors carry each license, and how many are manifest-only",
            ),
            FunctionExample(
                sql=(
                    "SELECT name, version, release_stage FROM airbyte.main.connectors() "
                    "WHERE manifest_only AND license = 'MIT' ORDER BY name"
                ),
                description="MIT-licensed declarative connectors that run without Docker or a license acknowledgement",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ConnectorsArgs]) -> BindResponse:
        """Publish the fixed columns."""
        return BindResponse(output_schema=CONNECTORS_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ConnectorsArgs]) -> OnceState:
        """Not started."""
        return OnceState()

    @classmethod
    def process(cls, params: ProcessParams[ConnectorsArgs], state: OnceState, out: OutputCollector) -> None:
        """Load the registry and emit every source."""
        if state.done:
            out.finish()
            return
        url = params.args.registry_url
        rows = [
            {
                "name": e.name,
                "display_name": e.display_name or None,
                "definition_id": e.definition_id or None,
                "docker_image": f"{e.docker_repository}:{e.docker_image_tag}",
                "version": e.docker_image_tag,
                "language": e.language or None,
                "manifest_only": e.manifest_only,
                "release_stage": e.release_stage or None,
                "support_level": e.support_level or None,
                "license": e.license or None,
                "license_policy": reg.license_policy(e.license),
                "source_type": e.source_type or None,
                "documentation_url": e.documentation_url or None,
                "manifest_url": _manifest_url(e, url),
                "tags": list(e.tags),
            }
            for e in reg.load_registry(url)
        ]
        out.emit(pa.RecordBatch.from_pylist(rows, schema=params.output_schema))
        state.done = True
