#!/usr/bin/env python3
"""A hermetic REST API and Airbyte registry mirror for the tests. Standard library only.

The in-repo declarative manifest (``test/fixtures/manifest.yaml``) reads this API,
and the worker's registry functions read the registry mirror, so the whole suite
runs offline.

API (every ``/api`` request needs ``Authorization: Bearer <token>``, default
``fixture-token``; anything else gets 401):

``GET /api/v1/items?updated_since=<iso>&page_token=<t>&limit=<n>``
    25 items ordered by ``updated_at`` (one hour apart from 2026-01-01T00:00:00Z),
    only those with ``updated_at >= updated_since``. Cursor pagination:
    ``{"data": [...], "next": "<token>" | null}``.
``GET /api/v1/users?offset=<n>&limit=<m>``
    7 users, offset pagination: ``{"users": [...]}``.
``GET /api/v1/forbidden``
    Always 403 (an upstream error the connector must surface).
``GET /_requests`` / ``POST /_reset``
    The request log (path + query of every ``/api`` call) for pushdown assertions.

Registry mirror (same layout as https://connectors.airbyte.com/files/):

``GET /files/registries/v0/oss_registry.json``     -> test/fixtures/oss_registry.json
``GET /files/metadata/airbyte/<name>/<tag>/manifest.yaml`` -> test/fixtures/manifest.yaml
    for the manifest-only fixture connectors; 404 otherwise.

Usage::

    python3 test/fixture_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
MANIFEST_CONNECTORS = {"source-fixture", "source-fixture-elv2", "source-fixture-custom"}

ITEMS = [
    {
        "id": i,
        "name": f"item-{i:02d}",
        "price": round(1.25 * i, 2),
        "in_stock": i % 3 != 0,
        "updated_at": (START + dt.timedelta(hours=i - 1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "tags": [f"t{i % 4}", "all"],
        "dims": {"w": i, "h": i * 2},
        "attributes": {"color": ["red", "green", "blue"][i % 3], "rank": i},
    }
    for i in range(1, 26)
]
USERS = [{"user_id": f"u{i}", "email": f"user{i}@example.com", "admin": i == 1} for i in range(1, 8)]

_log: list[str] = []
_log_lock = threading.Lock()


def _parse_ts(text: str) -> dt.datetime:
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))


class Handler(BaseHTTPRequestHandler):
    """Serve the fixture API and registry mirror."""

    token = "fixture-token"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Keep stdout clean (it carries PORT:<n>)."""
        sys.stderr.write("fixture: " + (format % args) + "\n")

    def _send(self, status: int, body: bytes, content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj: object) -> None:
        self._send(status, json.dumps(obj).encode())

    def do_POST(self) -> None:
        """Reset the request log."""
        if self.path == "/_reset":
            with _log_lock:
                _log.clear()
            self._json(200, {"ok": True})
        else:
            self._json(404, {"error": "not found"})

    def do_GET(self) -> None:
        """Route a GET request."""
        url = urlparse(self.path)
        q = {k: v[-1] for k, v in parse_qs(url.query).items()}
        if url.path == "/_requests":
            with _log_lock:
                self._json(200, list(_log))
            return
        if url.path == "/files/registries/v0/oss_registry.json":
            self._send(200, (FIXTURES / "oss_registry.json").read_bytes())
            return
        if url.path.startswith("/files/metadata/airbyte/"):
            parts = url.path.split("/")
            # /files/metadata/airbyte/<name>/<tag>/manifest.yaml
            if len(parts) == 7 and parts[4] in MANIFEST_CONNECTORS and parts[6] == "manifest.yaml":
                name = "manifest-custom.yaml" if parts[4] == "source-fixture-custom" else "manifest.yaml"
                self._send(200, (FIXTURES / name).read_bytes(), "application/yaml")
            else:
                self._json(404, {"error": "no such file"})
            return
        if not url.path.startswith("/api/"):
            self._json(404, {"error": "not found"})
            return
        with _log_lock:
            _log.append(self.path)
        if self.headers.get("Authorization") != f"Bearer {self.token}":
            self._json(401, {"error": "invalid token"})
            return
        if url.path == "/api/v1/items":
            self._items(q)
        elif url.path == "/api/v1/users":
            offset, limit = int(q.get("offset", 0)), int(q.get("limit", 3))
            self._json(200, {"users": USERS[offset : offset + limit]})
        elif url.path == "/api/v1/forbidden":
            self._json(403, {"error": "this token may not read forbidden things"})
        else:
            self._json(404, {"error": "not found"})

    def _items(self, q: dict[str, str]) -> None:
        rows = ITEMS
        if "updated_since" in q:
            since = _parse_ts(q["updated_since"])
            rows = [r for r in rows if _parse_ts(str(r["updated_at"])) >= since]
        start = int(q.get("page_token", 0))
        limit = int(q.get("limit", 10))
        page = rows[start : start + limit]
        nxt = str(start + limit) if start + limit < len(rows) else None
        self._json(200, {"data": page, "next": nxt})


def main() -> None:
    """Serve until killed, printing ``PORT:<n>`` once listening."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--token", default="fixture-token")
    args = parser.parse_args()
    Handler.token = args.token
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
