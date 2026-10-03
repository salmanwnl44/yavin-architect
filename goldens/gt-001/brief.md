# Brief: multi-tenant event ingest pipeline

External clients send events over HTTPS to an API gateway inside our cluster. The gateway
enqueues each event on a queue, a pool of workers consumes the queue, and each worker writes
events to a datastore. The cluster is one trust boundary; clients are outside it.

## Requirements

- [peak-ingest] The pipeline must sustain a peak ingest of 1500 req/s.
- [ingest-latency] Ingest latency at the gateway must stay under p99 < 300 ms.
- [no-loss] No acknowledged event may be lost on a single-node failure: RPO 0 s.
- [tenant-isolation] Tenant data must be isolated: cross-tenant error rate 0 %.
- [robust] The system should be robust.
