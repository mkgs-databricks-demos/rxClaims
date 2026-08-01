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

**Status:** Not started — scaffolding needed

Create a **separate DABs bundle** to declare the AI Search endpoint and Delta Sync index as resources. Requires `engine: direct` in that bundle's `databricks.yml`.

**What to declare:**

```yaml
# Endpoint
vector_search_endpoints:
  ncpdp_specifications_endpoint:
    name: ncpdp-specifications-vs-endpoint
    endpoint_type: STANDARD

# Delta Sync Index
vector_search_indexes:
  ncpdp_specifications_index:
    name: ${var.catalog}.${var.schema}.specification_search_chunks_index_raw
    endpoint_name: ${resources.vector_search_endpoints.ncpdp_specifications_endpoint.name}
    primary_key: path
    index_type: DELTA_SYNC
    delta_sync_index_spec:
      source_table: ${var.catalog}.${var.schema}.specification_search_chunks
      pipeline_type: TRIGGERED  # or CONTINUOUS for auto-sync
      embedding_source_columns:
        - name: text
          model_endpoint_name: databricks-gte-large-en
      columns_to_sync:
        - error_status
```

**Reference:** Check other project examples (e.g., lakeLoom, dbxWearables) for bundle scaffolding patterns. VS endpoint requires CLI >= 0.298.0; index requires CLI >= 1.1.0.

**From the old notebooks (now removed from job):**
- Endpoint creation: `src/vector_search/00-enable-vector-search-endpoints` (used SP auth via secret scope `ncpdp_vs_sp`)
- Index creation: `src/vector_search/01-create-vector-index` (primary_key=`path`, embedding_source_column=`text`, columns_to_sync=`error_status`, embedding_model=`databricks-gte-large-en`)
- The index enables CDF on the source table before creation
- Secret scope `ncpdp_vs_sp` holds SP credentials (`client_id`, `secret`) for VS client auth

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
