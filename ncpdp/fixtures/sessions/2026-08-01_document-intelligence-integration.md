# Session: Document Intelligence Pipeline Integration

**Date:** 2026-08-01  
**Branch:** `mg-split-parsed-documents`  
**Bundle:** ncpdp (dev target)  

## Summary

Integrated the `ncpdp_document_intelligence` pipeline into the `ncpdp_parsing` job workflow, replacing the old `ncpdp_specification_document_parsing` pipeline + SQL workaround + `specification_further_processing` pipeline. Removed vector search tasks (moving to a separate DABs bundle). Achieved a completely clean job run with all tasks passing and validated data quality across all 10 output tables.

## Problems & Root Causes

1. **ETL pipeline failed on first run** — `landing` volume had no `ClaimBilling/` subdirectory because `run_set_up=true` was not passed. Fixed by running with setup enabled.
2. **`spec_documents` volume was empty** — The `raw/` and `parsed_image_output/` subdirectories didn't exist. The `Download_Spec_Doc` task creates `raw/` via `os.makedirs`, but `parsed_image_output/` was missing. Fixed by adding it to the download notebook.
3. **Vector search tasks failing** — `Ensure_Vector_Search_Endpoints` requires a service principal secret scope that isn't configured in this workspace. Decision: move VS to a separate declarative bundle rather than fixing imperative notebooks.

## Changes Made

### `resources/ncpdp_parsing.job.yml`
- Replaced `Full_Refresh_Spec_Doc_Ingest_Pipeline` / `Refresh_Spec_Doc_Ingest_Pipeline` (old pipeline) with `Full_Refresh_Document_Intelligence_Pipeline` / `Refresh_Document_Intelligence_Pipeline`
- Removed `temp_ai_parse_document_sql` SQL workaround task
- Removed `Ensure_Vector_Search_Endpoints` task
- Removed `Create_or_Sync_Vector_Search_Index_Initial` task
- Removed `vector_search` environment (databricks-vectorsearch dependency)
- Updated pipeline references to `${resources.pipelines.ncpdp_document_intelligence.id}`

### `src/ncpdp_spec_download/00-download-specs-to-volume.ipynb`
- Added `os.makedirs` for `parsed_image_output/` subdirectory alongside the existing `raw/` directory creation

### `PROJECT_MEMORY.md` (new)
- Created project memory with architecture, resource IDs, run results, data quality summary, stale resources inventory, and future work (AI Search bundle)

## Decisions

1. **AI Search as separate bundle** — Vector search endpoint + Delta Sync index will be declared as resources in their own bundle with `engine: direct`. Requires CLI >= 0.298.0 (endpoints) / >= 1.1.0 (indexes). Reference patterns from lakeLoom/dbxWearables.
2. **Keep old pipeline source temporarily** — `src/ncpdp_specifications/` and `src/specification_further_processing/` are stale but will be removed in a follow-up cleanup PR.
3. **Keep vector search notebooks as reference** — `src/vector_search/` notebooks document the index configuration (primary_key, embedding columns, etc.) needed for the declarative approach.

## Validated Run Results

**Run `752978541164333`** — 10.5 min, all SUCCESS:
- Setup: `Create_Sub_Directories` → `Copy_Sample_Files` (3 XML fixtures)
- Doc Intelligence: `Download_Spec_Doc` → `Full_Refresh_Document_Intelligence_Pipeline`
- ETL: `Full_Refresh_Pipeline`

**Data quality:** 10 tables, zero AI errors, full referential integrity, 178 search chunks from 1 PDF (avg 4,947 chars, zero empty).

## Files Modified

| File | Action |
| --- | --- |
| `resources/ncpdp_parsing.job.yml` | Modified (replaced old pipeline tasks, removed VS tasks) |
| `src/ncpdp_spec_download/00-download-specs-to-volume.ipynb` | Modified (added parsed_image_output mkdir) |
| `src/ncpdp_document_intelligence/tests/test_pipeline_wiring.py` | Modified (FQN-based test approach) |
| `PROJECT_MEMORY.md` | Created |
| `fixtures/sessions/2026-08-01_document-intelligence-integration.md` | Created |

## Git Commits (this session)

1. `feat(document-intelligence): integrate pipeline into job workflow`
2. `feat(document-intelligence): create parsed_image_output dir in download notebook`
3. `refactor(job): remove vector search tasks from ncpdp_parsing job`
4. `docs: add PROJECT_MEMORY.md for ncpdp bundle`
5. `docs: session summary and project memory updates` (pending)
