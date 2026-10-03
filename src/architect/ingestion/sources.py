"""Stage 1: a local file, a PDF or a git repository becomes one source.ingested event.

The only module in the ingestion package allowed to run a subprocess, and only `git` for
cloning. Raw bytes go to the object store; the event carries the content hash, the uri, the
media type, the license and the taint.
"""

from __future__ import annotations

import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from psycopg_pool import ConnectionPool

from architect.arbiter import Arbiter
from architect.ingestion.config import IngestConfig
from architect.ingestion.normalize import typed_id
from architect.ingestion.objectstore import ObjectStore, sha256_hex
from architect.ingestion.parse import MEDIA_REPO, media_type_for

ACTOR = {"kind": "system", "id": "ingestor", "role": "ingestor"}
LOCAL_ORIGINS = ("user", "internal", "external_trusted")


@dataclass(frozen=True)
class Source:
    source_id: str
    content_hash: str
    uri: str
    media_type: str
    taint_origin: str
    license: str | None
    created: bool  # False when the same content was already in the project


@dataclass(frozen=True)
class RepoFile:
    path: str
    sha256: str
    size: int


def source_id_for(content_hash: str) -> str:
    return typed_id("src", "source", content_hash)


def detect_license(text: str) -> str | None:
    head = text[:4000]
    for pattern, name in (
        (r"\bMIT License\b|Permission is hereby granted, free of charge", "MIT"),
        (r"Apache License,?\s+Version 2\.0", "Apache-2.0"),
        (r"GNU GENERAL PUBLIC LICENSE\s+Version 3", "GPL-3.0"),
        (r"GNU GENERAL PUBLIC LICENSE\s+Version 2", "GPL-2.0"),
        (r"Redistribution and use in source and binary forms", "BSD"),
        (r"Mozilla Public License,?\s+v(ersion)?\.? ?2\.0", "MPL-2.0"),
    ):
        if re.search(pattern, head, re.I):
            return name
    return None


def is_binary(data: bytes) -> bool:
    return b"\x00" in data[:8192]


class Ingestor:
    def __init__(self, pool: ConnectionPool, store: ObjectStore, config: IngestConfig) -> None:
        self._pool = pool
        self._store = store
        self._config = config

    # ---------------------------------------------------------------- entry points
    def ingest_file(self, project_id: str, path: Path, origin: str = "user") -> Source:
        if origin not in LOCAL_ORIGINS:
            raise ValueError(f"--origin must be one of {LOCAL_ORIGINS}")
        data = path.read_bytes()
        text = data if is_binary(data) else data.decode("utf-8", errors="replace")
        license_ = detect_license(text) if isinstance(text, str) else None
        return self._record(
            project_id,
            data,
            uri=path.resolve().as_uri(),
            media_type=media_type_for(path.name),
            taint_origin=origin,
            license_=license_,
        )

    def ingest_github(self, project_id: str, url: str, ref: str | None = None) -> Source:
        host = urlparse(url).hostname or ""
        taint = "external_trusted" if host in self._config.trusted_domains else "external_untrusted"
        with tempfile.TemporaryDirectory() as tmp:
            checkout = Path(tmp) / "repo"
            command = ["git", "clone", "--depth", "1", "--quiet"]
            if ref:
                command += ["--branch", ref]
            command += [url, str(checkout)]
            subprocess.run(command, check=True, capture_output=True, text=True)
            sha = subprocess.run(
                ["git", "-C", str(checkout), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
            manifest, license_ = self._manifest(checkout)
        data = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        return self._record(
            project_id,
            data,
            uri=f"{url}@{sha}",
            media_type=MEDIA_REPO,
            taint_origin=taint,
            license_=license_,
        )

    # ---------------------------------------------------------------- the repository manifest
    def _manifest(self, root: Path) -> tuple[dict[str, Any], str | None]:
        """Every included file, stored in the object store, listed with its hash and size."""
        files: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        license_ = None
        for path in sorted(p for p in root.rglob("*") if p.is_file()):
            relative = path.relative_to(root).as_posix()
            parts = relative.split("/")
            if any(part in self._config.skip_dirs for part in parts[:-1]):
                skipped.append({"path": relative, "reason": "vendored"})
                continue
            size = path.stat().st_size
            if size > self._config.max_file_bytes:
                skipped.append({"path": relative, "reason": "too_large", "size": size})
                continue
            data = path.read_bytes()
            if is_binary(data):
                skipped.append({"path": relative, "reason": "binary"})
                continue
            key = self._store.put(data)
            files.append({"path": relative, "sha256": key, "size": size})
            if parts[-1].upper().startswith("LICENSE") and license_ is None:
                license_ = detect_license(data.decode("utf-8", errors="replace"))
        return {"kind": "repo-manifest", "files": files, "skipped": skipped}, license_

    # ---------------------------------------------------------------- the event
    def _record(
        self,
        project_id: str,
        data: bytes,
        *,
        uri: str,
        media_type: str,
        taint_origin: str,
        license_: str | None,
    ) -> Source:
        content_hash = self._store.put(data)
        assert content_hash == sha256_hex(data)
        source_id = source_id_for(content_hash)
        payload: dict[str, Any] = {
            "source_id": source_id,
            "uri": uri,
            "content_hash": content_hash,
            "media_type": media_type,
            "taint_origin": taint_origin,
        }
        if license_:
            payload["license"] = license_
        commit = Arbiter(self._pool).submit(
            project_id,
            {
                "actor": ACTOR,
                "type": "source.ingested",
                "payload": payload,
                "idempotency_key": f"source:{content_hash}",
            },
        )
        recorded = commit.event["payload"]
        return Source(
            source_id=recorded["source_id"],
            content_hash=recorded["content_hash"],
            uri=recorded["uri"],
            media_type=recorded["media_type"],
            taint_origin=recorded["taint_origin"],
            license=recorded.get("license"),
            created=not commit.replayed,
        )


def repo_files(manifest: bytes) -> list[RepoFile]:
    data = json.loads(manifest)
    return [RepoFile(f["path"], f["sha256"], f["size"]) for f in data["files"]]
