"""Content-addressed storage for raw source bytes. Local disk now, S3 later behind the same
interface."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Protocol

DEFAULT_ROOT_ENV = "ARCHITECT_OBJECT_STORE"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ObjectStore(Protocol):
    def put(self, data: bytes) -> str:
        """Store bytes; return their sha256 hex, the key."""

    def get(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...


class LocalObjectStore:
    """data/objects/<sha256>, written atomically and never rewritten."""

    def __init__(self, root: Path | str | None = None) -> None:
        self.root = Path(root or os.environ.get(DEFAULT_ROOT_ENV) or "data/objects")

    def _path(self, key: str) -> Path:
        if len(key) != 64 or any(c not in "0123456789abcdef" for c in key):
            raise KeyError(f"not a content hash: {key!r}")
        return self.root / key[:2] / key

    def put(self, data: bytes) -> str:
        key = sha256_hex(data)
        path = self._path(key)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_bytes(data)
            os.replace(tmp, path)
        return key

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()
