# Changelog

## 0.1.1

- Catalog and schema docs are overviews instead of object lists, and schemas have their own one-line comments (vgi-lint 2.8: VGI173, VGI102).

## 0.1.0

- First release: run Airbyte source connectors from DuckDB. Declarative manifests (registry
  manifest-only connectors, local paths, URLs) through a pinned, isolated airbyte-cdk 7.48.0;
  any Airbyte-protocol Docker image or executable.
- Functions: `connectors`, `spec`, `check_connection`, `discover`, `catalog`, `read_stream`,
  `final_state`, `messages`, `json_schema_type`; ATTACH with `connector` + `catalog`.
- `airbyte` secret type validated against the connector spec; registry license policy
  (ELv2 needs `accept_license`, Airbyte Enterprise refused).

Targets: vgi-python 0.51 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
