"""Units the checks understand. Anything else is an UnknownUnit, which a check reports as an
error naming the parameter and its unit."""

from __future__ import annotations

# operations per second
THROUGHPUT = {
    "qps": 1.0,
    "rps": 1.0,
    "req/s": 1.0,
    "ops/s": 1.0,
    "writes/s": 1.0,
    "reads/s": 1.0,
    "msg/s": 1.0,
}
# dimensionless, 0..1
RATIO = {"ratio": 1.0, "%": 0.01}
# seconds
TIME = {"us": 1e-6, "ms": 1e-3, "s": 1.0}


class UnknownUnit(ValueError):
    def __init__(self, unit: str, dimension: str) -> None:
        super().__init__(f"unit {unit!r} is not a known {dimension} unit")
        self.unit = unit
        self.dimension = dimension


def _convert(value: float, unit: str, table: dict[str, float], dimension: str) -> float:
    if unit not in table:
        raise UnknownUnit(unit, dimension)
    return float(value) * table[unit]


def throughput(value: float, unit: str) -> float:
    """Operations per second."""
    return _convert(value, unit, THROUGHPUT, "throughput")


def ratio(value: float, unit: str) -> float:
    """A fraction between 0 and 1."""
    return _convert(value, unit, RATIO, "ratio")


def seconds(value: float, unit: str) -> float:
    return _convert(value, unit, TIME, "time")
