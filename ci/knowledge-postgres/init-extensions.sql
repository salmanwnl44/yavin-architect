-- The knowledge plane's extensions, in the application's database.
-- pgvector goes in `public`: the application names its type and operators by that schema,
-- because its own tables live in whatever schema the connection's search_path puts first.
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;
CREATE EXTENSION IF NOT EXISTS age;
