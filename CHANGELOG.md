# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.1,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3.
- `serve` extra is `vgi-python[http,oauth]`; the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The HEALTHCHECK uses `HEAD /health`, which stays
  anonymous in hosted mode.

## 0.1.3 (2026-11-16)

- Move to Python 3.14: `requires-python >=3.14`, CI, the Docker image and lint tooling target 3.14.

## 0.1.2 (2026-11-16)

- Bump vgi-python to 0.52.2.

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
