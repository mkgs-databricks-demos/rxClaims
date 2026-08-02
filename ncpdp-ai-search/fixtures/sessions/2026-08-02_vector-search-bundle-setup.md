# Session: Vector Search Bundle Setup

**Date:** 2026-08-02
**Branches:** `mg-genie-ncpdp-ai-search-scaffold`, `mg-genie-cleanup-legacy-pipelines`
**Bundle:** ncpdp-ai-search (dev target)

## Summary

Scaffolded and deployed the `ncpdp-ai-search` companion bundle for the rxClaims
NCPDP project. Declares a STANDARD Vector Search endpoint and Delta Sync index
over the `specification_search_chunks` table. Also cleaned up legacy pipeline
resources from the companion `ncpdp` bundle. Discovered that HYBRID search mode
is critical for NCPDP content (+0.34–0.44 improvement over pure ANN).

## Problems & Root Causes

1. **First deploy failed: `error_status` column doesn't exist** — PROJECT_MEMORY
   in the ncpdp bundle had stale column names from old imperative VS notebooks.
   Actual `ai_prep_search` output columns: `chunk_to_embed`, `chunk_to_retrieve`,
   `chunk_id`, `chunk_position`, `doc_source_id`, `path`.
   Fixed by updating `columns_to_sync` and `embedding_source_columns`.

2. **Index stuck at 1 indexed row after sync** — All 178 rows share the same
   `path` value (PDF source file path). Used `path` as PK → VS deduplicated to 1.
   Fixed by switching primary_key to `chunk_id` (unique per chunk).

3. **Repeated "pending deletion" errors during deploy** — VS index deletion is
   async (5–15 min). The `recreate` operation deletes + creates, but retries
   during the deletion window fail. Self-resolved after waiting.

4. **Dev schema name mismatch** — `databricks.yml` had `schema: rx_claims` but
   dev mode resolves to `dev_matthew_giglia_rx_claims`. Fixed in variables.

5. **Low ANN similarity scores (0.56–0.62)** — Root causes: large chunks
   (avg 5K chars), HTML markup noise, structured tabular content doesn't embed well.
   Mitigated at query time by switching to HYBRID search (0.97–1.00).

## Changes Made

### `ncpdp-ai-search/` (new bundle — all files created)

| File | Purpose |
| --- | --- |
| `databricks.yml` | Bundle config with variables, 3 targets, tags, conventions |
| `resources/ncpdp_ai_search.vector_search_endpoint.yml` | STANDARD endpoint |
| `resources/ncpdp_ai_search.vector_search_index.yml` | Delta Sync index (chunk_id PK, chunk_to_embed, TRIGGERED) |
| `README.md` | Architecture, deploy order, HYBRID search docs, sync instructions |
| `.gitignore` | Standard DABs ignores |
| `PROJECT_MEMORY.md` | Bundle-level project memory |
| `fixtures/sessions/2026-08-02_vector-search-bundle-setup.md` | This file |

### `ncpdp/` (companion bundle — cleanup)

| File | Action |
| --- | --- |
| `resources/ncpdp_specification_document_parsing.pipeline.yml` | Deleted |
| `resources/specification_further_processing.pipeline.yml` | Deleted |
| `src/ncpdp_specifications/` (4 files) | Deleted |
| `src/specification_further_processing/` (3 files) | Deleted |
| `src/vector_search/` (2 notebooks) | Deleted |
| `PROJECT_MEMORY.md` | Updated — AI Search status → "Scaffolded" |

## Decisions

1. **Separate bundle for VS** — Follows lakeLoom multi-bundle pattern. Keeps
   infrastructure (endpoint/index) decoupled from ETL pipelines. Deploy order:
   ncpdp first, ncpdp-ai-search second.

2. **STANDARD endpoint over Storage-Optimized** — Low latency (<50ms) preferred
   for RAG; only 178 chunks — no capacity concerns.

3. **TRIGGERED over CONTINUOUS** — Spec documents change infrequently. Manual
   sync after doc intelligence pipeline runs is sufficient.

4. **HYBRID search mode** — NCPDP content is heavily code-referenced (field IDs,
   segment codes). BM25 keyword matching is essential for these exact-match tokens.

5. **chunk_id as PK** — `path` is document-level (same for all chunks from one PDF).
   `chunk_id` is the actual unique identifier per chunk.

## Validated Deployment

**Endpoint:** `ncpdp-specifications-vs-endpoint` — ONLINE (STANDARD)
**Index:** `specification_search_chunks_index` — Ready, 178 rows indexed
**Sync:** TRIGGERED, initial sync completed successfully

### Test Query Results (HYBRID mode)

| Query | Score |
| --- | --- |
| Mandatory segments in B1 Claim Billing | 0.9841 |
| Claim Segment field 407-D7 NDC | 0.9766 |
| COB segment AM05 secondary payer | 0.9841 |
| Transaction Header 101-A1 BIN 102-A2 | 1.0000 |

## Git Commits

### Branch: `mg-genie-ncpdp-ai-search-scaffold`
1. `feat: scaffold ncpdp-ai-search bundle for Vector Search`

### Branch: `mg-genie-cleanup-legacy-pipelines`
1. `refactor: remove legacy pipeline resources`
2. `refactor: remove stale source directories`
3. `fix(ai-search): correct index columns and dev schema name`
4. `docs: fix stale column name in index YAML comment`
5. `fix(ai-search): use chunk_id as primary key instead of path`
6. `docs(ai-search): document HYBRID search mode and correct column names`
7. (pending) `docs: add PROJECT_MEMORY and session summary`
