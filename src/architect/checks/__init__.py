"""The checks engine (M3): the deterministic core of the harness (spec §14, principle P4).

A check is a pure function over a System Model version and a CheckContext built from the
read models. The runner (`architect.checks.runner`) is the only module here that touches a
database or the Arbiter; everything else is pure, and tests/test_architecture.py keeps it so.
"""
