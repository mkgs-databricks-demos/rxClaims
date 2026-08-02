# Session: Legacy Cleanup & Rule Extraction Architecture

**Date:** 2026-08-02
**Branch:** `mg-genie-cleanup-legacy-pipelines`
**Bundles:** ncpdp (primary), ncpdp-ai-search (companion)

## Summary

Three major accomplishments:
1. Deleted legacy pipeline resources and source directories from the ncpdp bundle
2. Deployed and validated the ncpdp-ai-search companion bundle (VS endpoint + index)
3. Prototyped AI rule extraction and authored full architecture docs for the
   rule extraction system (4 parallel workstreams)

## Problems & Root Causes

1. **Legacy pipelines still declared in bundle** — `ncpdp_specification_document_parsing`
   and `specification_further_processing` pipelines + their source directories were
   superseded by `ncpdp_document_intelligence` but never removed.
   Fixed: deleted 2 resource YAMLs + 9 source files, validated, deployed.

2. **VS index stuck at 1 row** — All 178 chunks share the same `path` value
   (source PDF file path). Using `path` as primary key caused VS to deduplicate
   to 1 row. Fixed: switched PK to `chunk_id` (unique per chunk).

3. **Low ANN similarity scores (0.56-0.62)** — NCPDP content has structured field
   codes (101-A1, AM07) that are opaque to pure semantic embeddings.
   Mitigated: HYBRID search mode adds BM25 keyword matching → 0.97-1.00 scores.

4. **ai_query endpoint naming** — `databricks-claude-sonnet-4-20250514` doesn't
   exist; the correct endpoint is `databricks-claude-sonnet-4`.

5. **Extraction conditions too abstract** — v1 prompt produced `compound_code = '2'`
   (not usable against bronze data). v2 prompt instructs F_NNN_XX format →
   `F_406_D6 = '2'` (directly maps to bronze key column).

## Changes Made

### ncpdp bundle

| File | Action | Purpose |
| --- | --- | --- |
| resources/ncpdp_specification_document_parsing.pipeline.yml | Deleted | Legacy pipeline |
| resources/specification_further_processing.pipeline.yml | Deleted | Legacy pipeline |
| src/ncpdp_specifications/ (4 files) | Deleted | Old spec parsing code |
| src/specification_further_processing/ (3 files) | Deleted | Old processing code |
| src/vector_search/ (2 notebooks) | Deleted | Old imperative VS notebooks |
| PROJECT_MEMORY.md | Updated | AI Search status, prototype findings, architecture ref |
| fixtures/architecture/rule-extraction-system.md | Created | Master architecture doc |
| fixtures/architecture/workstream-a-rechunking.md | Created | Segment-aware re-chunking spec |
| fixtures/architecture/workstream-b-extraction.md | Created | AI extraction spec |
| fixtures/architecture/workstream-c-postprocessing.md | Created | Dedup/normalization spec |
| fixtures/architecture/workstream-d-silver-codegen.md | Created | Silver-layer codegen spec |
| fixtures/sessions/2026-08-02_*.md | Created | This session summary |

### ncpdp-ai-search bundle

| File | Action | Purpose |
| --- | --- | --- |
| databricks.yml | Fixed | Dev schema: rx_claims → dev_matthew_giglia_rx_claims |
| resources/ncpdp_ai_search.vector_search_index.yml | Fixed | PK: path → chunk_id; columns corrected; comment updated |
| README.md | Updated | HYBRID search docs, correct column names, mode comparison table |
| PROJECT_MEMORY.md | Created | Bundle-level project memory |
| fixtures/sessions/2026-08-02_vector-search-bundle-setup.md | Created | AI Search bundle session |

## Decisions

1. **HYBRID search is mandatory** for NCPDP content — field codes are keyword-matchable
2. **Two VS indexes** — keep existing `search_chunks` for Q&A, add `specification_rules`
   for precise filtered rule retrieval
3. **Four parallel workstreams** (A-D) designed for independent Genie Code sessions
4. **specification_rules table** is the integration contract between extraction and
   silver-layer code generation
5. **ai_query with claude-sonnet-4** is the extraction engine (proven, $0.32 total cost)
6. **Rules reference bronze keys directly** (F_NNN_XX format) for SQL-ready conditions

## Validated Deployments

| Resource | Status |
| --- | --- |
| ncpdp bundle (dev) | Validation OK, deployed (legacy pipelines destroyed) |
| ncpdp-ai-search endpoint | ONLINE (STANDARD) |
| ncpdp-ai-search index | Ready, 178 rows, sync triggered |

## Prototype Results: Rule Extraction

- 3 chunks → 48 structured rules extracted
- Rule types: MANDATORY (25), REQUIRED/REQUIRED_WHEN (15), FORMAT (5), SITUATIONAL (3)
- Silver column names and SQL types generated automatically
- Conditions use F_NNN_XX bronze key format (directly usable in expectations)
- ALLOWED_VALUES extracted as arrays (e.g., ["01", "03"])
- Estimated full corpus: ~1,700 rules from ~106 rule-bearing chunks

## Git Commits (branch: mg-genie-cleanup-legacy-pipelines)

1. `refactor: remove legacy pipeline resources`
2. `refactor: remove stale source directories`
3. `fix(ai-search): correct index columns and dev schema name`
4. `docs: fix stale column name in index YAML comment`
5. `fix(ai-search): use chunk_id as primary key instead of path`
6. `docs(ai-search): document HYBRID search mode and correct column names`
7. `docs(ai-search): add PROJECT_MEMORY and session summary`
8. `docs: add rule extraction system architecture and workstream plans`
9. (pending) `docs: add ncpdp session summary`

## Next Session Focus

- Workstream B: Build the rule extraction notebook (src/ncpdp_rule_extraction/00-extract-rules.ipynb)
- Merge this branch to main via PR
- Optionally begin Workstream A (re-chunking) in parallel
