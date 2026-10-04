# Postgres 16 with the two extensions of the knowledge plane (module M8):
#   Apache AGE  - the graph backend (owner decision §22-1), from the project's own PG16 image
#   pgvector    - the vector backend, from the PostgreSQL apt repository the base image uses
# CI builds and runs this image (a workflow `services:` entry cannot build one), and
# docker-compose.yml builds it for local Docker users. Without it, on a plain Postgres, the
# application falls back to its SQL graph store and its exact vector index.
FROM apache/age:release_PG16_1.6.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-pgvector \
    && rm -rf /var/lib/apt/lists/*

# created in the application's database on first start
COPY init-extensions.sql /docker-entrypoint-initdb.d/90-knowledge-extensions.sql

# AGE's hooks must be loaded by the server; the application also runs LOAD 'age' per session
CMD ["postgres", "-c", "shared_preload_libraries=age"]
