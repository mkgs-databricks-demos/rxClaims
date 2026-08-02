# NCPDP AI Search — Project Memory

## Bundle Root

`/Users/matthew.giglia@databricks.com/rxClaims/ncpdp-ai-search/`

## Companion Bundle

This bundle depends on the `ncpdp` bundle at `rxClaims/ncpdp/` which creates
the source table, schema, and volumes. **Deploy `ncpdp` first.**

## Targets

| Target | Workspace | Catalog | Schema (resolved) |
| --- | --- | --- | --- |
| dev (default) | `fevm-hls-fde.cloud.databricks.com` | `ncpdp_dev` | `dev_matthew_giglia_rx_claims` |
| e2_demo_fe | `e2-demo-field-eng.cloud.databricks.com` | `mgiglia` | `ncpdp_rx` |
| free_edition | `dbc-e5684c0a-20fa.cloud.databricks.com` | `prod` | `ncpdp_rx` |

## Key Resource IDs (dev)

| Resource | ID / Name |
| --- | --- |
| Vector Search Endpoint | `ncpdp-specifications-vs-endpoint` (STANDARD, ONLINE) |
| Delta Sync Index | `ncpdp_dev.dev_matthew_giglia_rx_claims.specification_search_chunks_index` |
| Source Table | `ncpdp_dev.dev_matthew_giglia_rx_claims.specification_search_chunks` (178 rows) |
| Embedding Model | `databricks-gte-large-en` (1024 dims) |

## Index Configuration

| Property | Value |
| --- | --- |
| Primary Key | `chunk_id` |
| Embedding Source | `chunk_to_embed` |
| Pipeline Type | TRIGGERED |
| Columns Synced | `doc_source_id`, `path`, `chunk_position`, `chunk_to_retrieve` |
| Indexed Rows | 178 |

## Performance Findings

### Search Modes (tested 2026-08-02)

**Always use `query_type="HYBRID"`** for NCPDP content.

| Mode | Score Range | Why |
| --- | --- | --- |
| ANN (semantic only) | 0.56–0.64 | Field codes (101-A1, AM07) are opaque to embeddings |
| **HYBRID** (semantic + BM25) | **0.97–1.00** | BM25 keyword component matches exact field codes |

Improvement: +0.34 to +0.44 across all test queries.

### Chunking Limitations (known)

The source table uses `ai_prep_search` default chunking which produces:
- Large chunks (avg 4,947 chars, range 1,153–8,607)
- Raw HTML table markup in chunk content
- No section-aware boundaries (segments split mid-table)
- All chunks share one `path` (document-level, not chunk-level)

**Future improvement:** Custom chunking step in `ncpdp_document_intelligence` pipeline
that splits by NCPDP segment section, strips HTML, prepends context headers,
and targets 800–1200 chars per chunk. Expected score improvement: 0.60 → 0.75+ for ANN.

## Issues Encountered During Setup

1. **`error_status` / `text` columns don't exist** — PROJECT_MEMORY in the ncpdp bundle
   had stale column names from the old imperative notebooks. Actual columns from
   `ai_prep_search`: `chunk_to_embed`, `chunk_to_retrieve`, `chunk_id`, `chunk_position`,
   `doc_source_id`, `path`.

2. **`path` is not a valid PK** — All 178 rows share the same path (source PDF file path).
   Caused VS to deduplicate to 1 indexed row. Fixed by using `chunk_id` as PK.

3. **Index recreation takes 5-15 minutes** — VS index deletion is async and blocks
   re-creation with "pending deletion" errors during that window. Retries succeed
   once cleanup completes.

## Git Branches

| Branch | Status | Contents |
| --- | --- | --- |
| `mg-genie-ncpdp-ai-search-scaffold` | Pushed | Initial bundle scaffolding (databricks.yml, endpoint, index, README) |
| `mg-genie-cleanup-legacy-pipelines` | Pushed | Column fixes, PK fix, hybrid search docs, legacy pipeline removal |

## TODO / Future Work

1. **Merge branches to main** — Both feature branches ready for PR
2. **Custom chunking pipeline** — Replace `ai_prep_search` defaults with segment-aware chunking
3. **Add metadata filter columns** — `segment_name`, `transaction_type` for filtered search
4. **Wire sync task to ncpdp_parsing job** — Auto-sync after doc intelligence pipeline completes
5. **Build RAG agent** — Use hybrid search with this index for NCPDP Q&A
6. **Clean up nested `rxClaims/` directory** — Misplaced file creation left an empty nested dir
