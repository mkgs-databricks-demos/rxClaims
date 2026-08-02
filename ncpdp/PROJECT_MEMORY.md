# NCPDP Project Memory

## Bundle Root

`/Users/matthew.giglia@databricks.com/rxClaims/ncpdp/`

## Targets

| Target | Workspace | Catalog | Schema (resolved) |
| --- | --- | --- | --- |
| dev (default) | `fevm-hls-fde.cloud.databricks.com` | `ncpdp_dev` | `dev_matthew_giglia_rx_claims` |

## Key Resource IDs (dev)

| Resource | ID |
| --- | --- |
| ncpdp_parsing job | `343795472366843` |
| ncpdp_document_intelligence pipeline | `4f578841-dd5a-4790-88bd-37db06153eb5` |
| ncpdp_etl pipeline | (deployed, ID in state file) |
| ncpdp_specification_document_parsing pipeline | DELETED (legacy, 2026-08-02) |
| ncpdp_segments_etl pipeline | `ad447b3e-42bf-43bb-b233-50016ef00d2e` |
| SQL warehouse | `5f0c7384ea5e0d28` |

## Architecture

The `ncpdp_parsing` job orchestrates three independent branches:

1. **Volume Setup** (gated: `run_set_up=true`) — creates subdirectories under the `landing` volume, copies sample files in non-prod
2. **Specification Document Processing** (gated: `deployment_mode=development` AND `run_spec_process=true`) — downloads a real NCPDP Payer Sheet PDF, then runs `ncpdp_document_intelligence` pipeline (full or incremental)
3. **Primary ETL** — runs the `ncpdp_etl` Auto Loader pipeline for pharmacy claim transactions

### ncpdp_document_intelligence Pipeline

Replaces both the old `ncpdp_specification_document_parsing` pipeline AND the `specification_further_processing` pipeline + SQL workaround task. Single fully-streaming SDP pipeline using v2 AI functions:

- `stream_ingest()` — cloudFiles binaryFile → `specification_documents`
- `parse_documents()` — ai_parse_document v2 → `specification_documents_parsed`
- `classify_documents()` — ai_classify → `specification_documents_classified`
- `extract_fields()` — ai_extract → `specification_documents_extracted`
- `prep_search()` — two-stage explode/chunking → `specification_search_chunks`
- `chunk_by_segment()` — segment-aware re-chunking → `specification_chunks_by_segment`

Source: `src/ncpdp_document_intelligence/`

## Latest Validated Run (2026-08-01)

**Run ID:** `752978541164333` | **Job ID:** `343795472366843` | **Duration:** 10.5 min | **Result:** SUCCESS

**Parameters:** `run_set_up=true`, `pipeline_full_refresh=true`, `run_spec_process=true`, `deployment_mode=development`

All 11 active tasks passed (2 EXCLUDED = incremental refresh paths correctly skipped).

### Data Quality Summary

#### ETL Pipeline

| Table | Rows | Notes |
| --- | --- | --- |
| `claimbilling_bronze` | 3 | 3 XML files (3KB–13KB), all with `transaction_file_source_id` |
| `claimbilling_bronze_variant` | 3 | 1:1 with bronze, fully parsed to VARIANT |
| `claimbilling_bronze_requests` | 168 | 16 segment types across 3 files |
| `claimbilling_bronze_responses` | 101 | 9 segment types across 3 files |
| `claimbilling_bronze_supplemental` | 21 | Zero null keys |

Null `key` values in requests (68/168) and responses (44/101) are by design — full-segment rows store the complete segment object in VARIANT `value`.

#### Document Intelligence Pipeline

| Table | Rows | Notes |
| --- | --- | --- |
| `specification_documents` | 1 | 3.3MB PDF (Payer Sheet Template v18) |
| `specification_documents_parsed` | 1 | 787KB structured JSON, bounding boxes, confidence=1 |
| `specification_documents_classified` | 1 | Label: `payer_sheet`, no errors, API v2.0 |
| `specification_documents_extracted` | 1 | Title: "NCPDP Payer Sheet Template", Version: "18" |
| `specification_search_chunks` | 178 | Avg 4,947 chars, range 1,153–8,607, zero empty |
| `specification_chunks_by_segment` | 602 | Segment-aware, avg 749 chars, range 31–1,500, 25 segment codes |

Zero AI errors across all stages. `doc_source_id` consistent 1:1 across all 4 downstream tables.

## TODO / Future Work

### AI Search Bundle (Vector Search)

**Status:** DEPLOYED & OPERATIONAL (2026-08-02)

Companion bundle at `rxClaims/ncpdp-ai-search/` (branch: `mg-genie-cleanup-legacy-pipelines`):
- STANDARD Vector Search endpoint (`ncpdp-specifications-vs-endpoint`) — ONLINE
- Delta Sync index on `specification_search_chunks` — 178 rows indexed, Ready
  - Primary key: `chunk_id` (NOT `path` — all rows share one path)
  - Embedding: `chunk_to_embed` → `databricks-gte-large-en`
  - Synced: `doc_source_id`, `path`, `chunk_position`, `chunk_to_retrieve`
  - Pipeline type: TRIGGERED

Targets mirror this bundle (dev/e2_demo_fe/free_edition). Deploy `ncpdp` first, then `ncpdp-ai-search`.

**Critical finding: Always use `query_type="HYBRID"`** for NCPDP content.
BM25 keyword matching on field codes (101-A1, AM07, B1) raises scores from 0.56–0.62 (ANN) to 0.97–1.00 (HYBRID).

### Orchestrated Rule Extraction System (2026-08-03)

**Architecture:** Three Genie Code scheduled tasks execute workstreams in sequence,
coordinated via `fixtures/handoffs/` status file protocol (polling every 15 min).

**Key decision:** Workstreams B+C are implemented as a NEW SDP pipeline
(`ncpdp_rule_extraction`) rather than standalone notebooks. This enables CDF-driven
incremental processing when new spec documents are added later.

| Task | Workstream | Output | Status |
| --- | --- | --- | --- |
| NCPDP WS-A Re-Chunking | Segment-aware re-chunking in doc intelligence pipeline | `specification_chunks_by_segment` | **COMPLETE** (602 rows, 25 segments) |
| NCPDP WS-BC Rule Extraction | New `ncpdp_rule_extraction` pipeline (extraction + post-processing) | `specification_rules` | **COMPLETE** (1,153 rules) |
| NCPDP WS-D Silver Codegen | Rule-driven `ncpdp_segments_etl` implementation | `claimbilling_silver_{segment}` | **COMPLETE** (8 tables, 29 expectations) |

**Prompt files:** `fixtures/architecture/scheduled-tasks/`
**Handoff protocol:** `fixtures/handoffs/` (see README there)
**Branch strategy:** Each workstream creates independent feature branch off main (different directories, no conflicts)

### Rule Extraction Prototype (2026-08-02)

**Goal:** Extract structured validation rules from the specification chunks for use in bronze→silver expectations.

**Two rule granularities identified:**
1. **TRANSACTION-level** — segment presence/absence based on values in OTHER segments
   (e.g., Compound Segment AM10 required when `F_406_D6 = '2'` in Claim Segment)
2. **FIELD-level** — individual value validation (format, allowed values, conditionality)
   (e.g., BIN Number 101-A1: Mandatory, 6-digit numeric)

**Prototype results (3 chunks → 48 rules):**
- `ai_query('databricks-claude-sonnet-4', ...)` with structured JSON extraction prompt
- Rule types: MANDATORY (25), REQUIRED/REQUIRED_WHEN (15), FORMAT (5), SITUATIONAL (3)
- Correctly generates: silver column names, SQL data types, allowed values arrays
- Conditions reference bronze field codes (`F_406_D6 = '2'`) — directly usable in expectations
- Estimated full corpus: ~1,700 rules from ~106 rule-bearing chunks
- Cost: ~$0.32 total for full extraction

**Remaining issues for extraction quality:**
1. Chunk boundaries cut across segment sections → misses rules at boundaries
2. Cross-segment conditions need `referenced_segment` field
3. Repeating field groups (compound ingredients) need cardinality modeling
4. HTML table markup (~30% of content) dilutes embeddings and extraction
5. Dedup needed — same fields in Claim Billing AND Claim Rebill sections

**Architecture plan:** See `fixtures/architecture/rule-extraction-system.md`

**Next steps:**
- Build `specification_rules` table via extraction notebook
- Add second VS index (`specification_rules_index`) to AI Search bundle
- Custom segment-aware re-chunking (replaces `ai_prep_search` defaults)
- Wire rule extraction into `ncpdp_parsing` job or as standalone pipeline

### Known Issues

1. ~~segments.py syntax bug~~ — FIXED (2026-08-03, branch `mg-genie-fix-segments-syntax`)
2. ~~specification_further_processing/temporary_views.py~~ — DELETED (legacy pipeline removed 2026-08-02)
3. ~~Segments.review_segments() stub~~ — REPLACED (2026-08-02, WS-D: full metadata-driven implementation via SegmentBuilder)
4. **test_pipeline_wiring.py** — Tier 1 tests need the pipeline's source volume populated (run job with `run_spec_process=true` first)
5. **Column comments in SDP** — ALTER TABLE ALTER COLUMN COMMENT not supported in SDP spark.sql(). Comments logged to `claimbilling_silver_column_comments_log`; need post-pipeline SQL task for actual application.
6. **Segments not yet exploded in bronze** — 9 segments (02,05,06,08,09,10,13,15,16) only have VARIANT rows in bronze_requests. Silver pipeline will auto-expand when bronze ETL explodes these segments into key-value format.

### Test Status

| Suite | File | Status |
| --- | --- | --- |
| Tier 2 (pure functions) | `src/ncpdp_document_intelligence/tests/test_utils.py` | 82 tests PASSED |
| Tier 1 (pipeline wiring) | `src/ncpdp_document_intelligence/tests/test_pipeline_wiring.py` | In progress — needs volume setup |
