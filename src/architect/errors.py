"""Typed rejections: the only way the Arbiter says no."""

from __future__ import annotations

CONFLICT_CODES = frozenset({"BASE_MOVED"})
NOT_FOUND_CODES = frozenset({"UNKNOWN_PROJECT", "UNKNOWN_EVENT"})


class Rejection(Exception):
    """A candidate was refused. Nothing was written."""

    def __init__(self, code: str, detail: str, json_path: str | None = None) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail
        self.json_path = json_path

    @property
    def http_status(self) -> int:
        if self.code in NOT_FOUND_CODES:
            return 404
        if self.code in CONFLICT_CODES or self.code.startswith("DUPLICATE_"):
            return 409
        return 422

    def body(self) -> dict[str, str]:
        body = {"code": self.code, "detail": self.detail}
        if self.json_path is not None:
            body["json_path"] = self.json_path
        return body
