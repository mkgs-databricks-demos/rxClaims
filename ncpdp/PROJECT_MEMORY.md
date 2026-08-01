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
| ncpdp_specification_document_parsing pipeline | `99613f24-be05-430e-9aa8-745a877132d9` |
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

Zero AI errors across all stages. `doc_source_id` consistent 1:1 across all 4 downstream tables.

## TODO / Future Work

### AI Search Bundle (Vector Search)

**Status:** Scaffolded — branch `mg-genie-ncpdp-ai-search-scaffold` (2026-08-02)

Companion bundle at `rxClaims/ncpdp-ai-search/` declares:
- STANDARD Vector Search endpoint (`ncpdp-specifications-vs-endpoint`)
- Delta Sync index on `specification_search_chunks` (managed embeddings via `databricks-gte-large-en`, `TRIGGERED` pipeline type, synced column: `error_status`)

Targets mirror this bundle (dev/e2_demo_fe/free_edition) with matching catalog/schema values. Deploy this `ncpdp` bundle first, then `ncpdp-ai-search`.

**Next steps for AI Search:**
- Merge feature branch to main
- Deploy the companion bundle (`databricks bundle deploy --target dev`)
- Optionally add a sync task to `ncpdp_parsing` job after doc intelligence completes
- Remove old `src/vector_search/` notebooks (now fully replaced by declarative resources)
- Secret scope `ncpdp_vs_sp` may be retired once SP auth is no longer needed for imperative index creation

### Known Issues

1. **segments.py syntax bug** — Line 8: `segments_yaml_path = '../../../fixtures/config/segments/"'` — mismatched quotes
2. **specification_further_processing/temporary_views.py** — Hardcoded table ref `ncpdp_dev.dev_matthew_giglia_rx_claims.specification_documents_parsed` (legacy, pipeline replaced by document_intelligence)
3. **Segments.review_segments() stub** — Currently just streams all requests to output; YAML rules not applied
4. **test_pipeline_wiring.py** — Tier 1 tests need the pipeline's source volume populated (run job with `run_spec_process=true` first)

### Test Status

| Suite | File | Status |
| --- | --- | --- |
| Tier 2 (pure functions) | `src/ncpdp_document_intelligence/tests/test_utils.py` | 82 tests PASSED |
| Tier 1 (pipeline wiring) | `src/ncpdp_document_intelligence/tests/test_pipeline_wiring.py` | In progress — needs volume setup |
