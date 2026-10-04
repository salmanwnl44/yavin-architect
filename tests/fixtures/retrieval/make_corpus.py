"""Write tests/fixtures/retrieval/corpus.json: ~60 labeled claims in six themes, 10 queries."""

import json
from pathlib import Path

THEMES = {
    "a": (
        "docs",
        "technique",
        [
            ("cache-layer", "REDUCES", "read-latency"),
            ("cache-layer", "REQUIRES", "eviction-policy"),
            ("lru-eviction", "BOUNDS", "memory-footprint"),
            ("cache-ttl", "LIMITS", "stale-reads"),
            ("write-through-cache", "INCREASES", "write-latency"),
            ("cache-stampede", "CAUSES", "origin-overload"),
            ("request-coalescing", "PREVENTS", "cache-stampede"),
            ("cache-hit-ratio", "DEPENDS_ON", "key-cardinality"),
            ("negative-caching", "REDUCES", "origin-load"),
            ("cache-warming", "IMPROVES", "cold-start-latency"),
        ],
    ),
    "b": (
        "paper",
        "protocol",
        [
            ("raft-consensus", "REQUIRES", "majority-quorum"),
            ("leader-election", "USES", "randomized-timeouts"),
            ("fencing-token", "PREVENTS", "split-brain"),
            ("lease-renewal", "DEPENDS_ON", "clock-drift-bound"),
            ("raft-log", "REPLICATES", "state-machine-commands"),
            ("quorum-loss", "CAUSES", "write-unavailability"),
            ("learner-replica", "IMPROVES", "read-scalability"),
            ("membership-change", "REQUIRES", "joint-consensus"),
            ("heartbeat-interval", "BOUNDS", "failure-detection-time"),
            ("leader-lease", "ENABLES", "local-reads"),
        ],
    ),
    "c": (
        "paper",
        "mechanism",
        [
            ("write-ahead-log", "GUARANTEES", "crash-durability"),
            ("group-commit", "IMPROVES", "fsync-throughput"),
            ("lsm-compaction", "CAUSES", "write-stalls"),
            ("bloom-filter", "REDUCES", "disk-lookups"),
            ("checkpointing", "BOUNDS", "recovery-time"),
            ("page-checksums", "DETECT", "silent-corruption"),
            ("direct-io", "AVOIDS", "double-buffering"),
            ("segment-format-xk9", "REQUIRES", "aligned-512-blocks"),
            ("tiered-storage", "REDUCES", "storage-cost"),
            ("snapshot-isolation", "PREVENTS", "dirty-reads"),
        ],
    ),
    "d": (
        "notes",
        "component",
        [
            ("message-broker", "DECOUPLES", "producers-and-consumers"),
            ("consumer-lag", "INDICATES", "insufficient-throughput"),
            ("backpressure", "PREVENTS", "queue-overflow"),
            ("dead-letter-queue", "ISOLATES", "poison-messages"),
            ("at-least-once-delivery", "REQUIRES", "idempotent-consumers"),
            ("partition-key", "PRESERVES", "per-key-ordering"),
            ("batch-size", "TRADES", "latency-for-throughput"),
            ("visibility-timeout", "PREVENTS", "duplicate-processing"),
            ("consumer-group", "ENABLES", "horizontal-scaling"),
            ("retention-period", "BOUNDS", "replay-window"),
        ],
    ),
    "e": (
        "docs",
        "control",
        [
            ("mutual-tls", "AUTHENTICATES", "service-identity"),
            ("token-rotation", "LIMITS", "credential-exposure"),
            ("short-lived-tokens", "REDUCE", "blast-radius"),
            ("secrets-manager", "CENTRALIZES", "credential-storage"),
            ("rate-limiting", "MITIGATES", "credential-stuffing"),
            ("audit-log", "RECORDS", "privileged-actions"),
            ("least-privilege", "RESTRICTS", "lateral-movement"),
            ("key-management-service", "PROTECTS", "encryption-keys"),
            ("envelope-encryption", "REDUCES", "key-exposure"),
            ("certificate-pinning", "PREVENTS", "impersonation"),
        ],
    ),
    "f": (
        "notes",
        "practice",
        [
            ("distributed-tracing", "REVEALS", "latency-bottlenecks"),
            ("trace-sampling", "REDUCES", "telemetry-cost"),
            ("slo-burn-rate", "TRIGGERS", "paging-alerts"),
            ("structured-logging", "ENABLES", "log-queries"),
            ("cardinality-explosion", "DEGRADES", "metrics-backend"),
            ("health-checks", "DETECT", "unhealthy-instances"),
            ("error-budget", "GOVERNS", "release-velocity"),
            ("exemplars", "LINK", "metrics-to-traces"),
            ("synthetic-probes", "MEASURE", "user-facing-availability"),
            ("alert-fatigue", "REDUCES", "incident-response-quality"),
        ],
    ),
}

claims = []
for theme, (source, entity_type, triples) in THEMES.items():
    for n, (subject, predicate, obj) in enumerate(triples, start=1):
        claims.append(
            {
                "name": f"{theme}{n:02d}",
                "source": source,
                "subject": {"entity_type": entity_type, "id": subject},
                "predicate": predicate,
                "object": {"entity_type": "property", "id": obj},
            }
        )
# one claim stated by two sources: both copies are design_grade (corroborated)
claims.append(
    {
        "name": "b03x",
        "source": "docs",
        "subject": {"entity_type": "protocol", "id": "fencing-token"},
        "predicate": "PREVENTS",
        "object": {"entity_type": "property", "id": "split-brain"},
    }
)

corpus = {
    "about": (
        "A labeled retrieval corpus: 61 claims in six themes (caching, consensus, storage, "
        "queues, security, observability), two quarantined proposals, and ten queries with "
        "the claims a correct search must return. `synonyms` is what the scripted embedder "
        "knows beyond the corpus's own words; `unknown_to_embedder` are words it has no "
        "direction for."
    ),
    "sources": {
        "paper": {"taint_origin": "external_untrusted", "uri": "https://example.org/survey.pdf"},
        "docs": {"taint_origin": "external_trusted", "uri": "https://docs.example.org/guide"},
        "notes": {"taint_origin": "user", "uri": "file:///notes/design.md"},
    },
    "claims": claims,
    "quarantined": [
        {
            "name": "q01",
            "source": "paper",
            "subject": {"entity_type": "protocol", "id": "fencing-token"},
            "predicate": "ELIMINATES",
            "object": {"entity_type": "property", "id": "split-brain"},
        },
        {
            "name": "q02",
            "source": "paper",
            "subject": {"entity_type": "technique", "id": "cache-layer"},
            "predicate": "GUARANTEES",
            "object": {"entity_type": "property", "id": "read-latency"},
        },
    ],
    "synonyms": {
        "memoization": "cache",
        "freshness": "stale",
        "expiry": "ttl",
        "netsplit": "split",
        "partitioned": "brain",
    },
    "unknown_to_embedder": ["xk9"],
    "queries": [
        {"q": "what prevents split brain", "relevant": ["b03", "b03x"]},
        {"q": "group commit and fsync throughput", "relevant": ["c02"]},
        {"q": "cache stampede", "relevant": ["a06", "a07"]},
        {"q": "dead letter queue for poison messages", "relevant": ["d04"]},
        {"q": "token rotation credential exposure", "relevant": ["e02"]},
        {"q": "trace sampling telemetry cost", "relevant": ["f02"]},
        {
            "q": "memoization freshness expiry",
            "relevant": ["a04"],
            "only": "vector",
            "note": "no word of the query is in the corpus; the embedder knows the synonyms",
        },
        {
            "q": "xk9",
            "relevant": ["c08"],
            "only": "text",
            "note": "an identifier the embedder has no direction for",
        },
        {"q": "raft majority quorum", "relevant": ["b01"]},
        {"q": "consumer lag throughput", "relevant": ["d02"]},
    ],
}
out = Path("tests/fixtures/retrieval/corpus.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(corpus, indent=2) + "\n", encoding="utf-8", newline="\n")
print(len(claims), "claims,", len(corpus["queries"]), "queries")
