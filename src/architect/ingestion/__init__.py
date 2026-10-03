"""Ingestion and extraction (M5): sources in, evidence-backed claims out, through the Arbiter.

Models only propose content. The pipeline, not the model, decides status, evidence, taint,
provenance and ids (R1); every claim carries a verbatim quote found in its segment (R2);
source content reaches a model only inside an untrusted data block (R3); nothing here executes
tools (R4); every stage is idempotent and resumable (R5).
"""
