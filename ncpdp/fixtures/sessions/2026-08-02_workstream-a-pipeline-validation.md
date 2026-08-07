# 2026-08-02 — Workstream A: Pipeline Validation & Deployment

**Branch:** `mg-genie-workstream-a-rechunking`
**Duration:** ~45 min (18:15–21:00 UTC)
**Pipeline update:** `dab06154-8ae6-49f5-8555-c117b7c0ab9c`

## Problems Identified

### 1. Streaming aggregation incompatibility

`chunk_by_segment()` was defined as `@dp.table` (streaming table) with `spark.readStream.table()` + `groupBy` + `collect_list`. SDP streaming tables use append mode which doesn't support unbounded aggregations.

**Root cause:** Initial prototype ran in a notebook (batch context) where `groupBy` works fine. The SDP decorator was copied without considering streaming semantics.

**Fix:** Changed to `@dp.materialized_view` with `spark.read.table()`. MV pattern is the documented SDP best practice for gold-layer aggregation over streaming sources.

### 2. collect_list ordering bug

`collect_list(struct(chunk_position, chunk_to_retrieve)).getField("chunk_to_retrieve")` — `.getField()` on an array of structs doesn't extract a column array; it's invalid.

**Fix:** `array_sort(collect_list(struct(...)))` + `transform(lambda x: x.getField("chunk_to_retrieve"))` — sorts structs by chunk_position (first field), then extracts the text values in order.

### 3. Missing pure functions (NameError in pipeline)

The `chunk_by_segment()` method referenced `SEGMENT_CHUNK_SCHEMA`, `segment_document_to_chunks`, and other module-level functions that were never committed to the branch that was deployed. The WS-A branch had the class method but the pure functions block was missing.

**Root cause:** The workspace edits were made in a session that crossed branches. The class method was committed to WS-A, but the pure functions (~200 lines) were lost when switching branches for WS-BC work.

**Fix:** Re-added all segment chunking pure functions (NCPDP_SEGMENT_MAP, regex patterns, strip_html_to_text, sub_chunk_text, generate_chunk_id, segment_document_to_chunks, SEGMENT_CHUNK_SCHEMA) to utils.py on the WS-BC branch where deployment happened.

## Changes Made

| File | Change |
| --- | --- |
| `src/ncpdp_document_intelligence/utilities/utils.py` | `@dp.table` → `@dp.materialized_view`; `readStream` → `read`; added `array_sort` + `transform` for ordered concat; added ~200 lines of pure functions |

## Decisions

1. **Materialized view is acceptable** — Single document (601 rows, <15s compute). Full recomputation on each refresh is fine; incremental would require a streaming-compatible approach (e.g., per-row UDF without aggregation).
2. **601 vs 602 row variance is acceptable** — `array_sort` on struct orders by chunk_position deterministically; one boundary chunk differs from the Python-side notebook approach. Content coverage is identical (25 segments, all fields present).
3. **Merge order matters** — WS-A branch → main first, then WS-BC (which is a superset). Both have the pure functions now but the WS-A branch has the cleaner history.

## Validation Results

### Pipeline Execution
- All 6 flows COMPLETED with zero errors
- Update `dab06154` took ~30s end-to-end
- Dependency graph correctly resolved: `specification_search_chunks` → `specification_chunks_by_segment`

### AI Search Index Cross-Validation
- HYBRID queries score 0.97–1.00 for all test cases
- 5/5 segment-specific domain queries return relevant content
- Field codes correctly attributed to expected segments (9/11, 82%)
  - 2 "misses" are test-case issues (field code text representation), not chunking errors

### Table Quality Metrics
- 601 rows, 25 distinct segment codes, 3 transaction types (B1_B3, B2, S1)
- Char range: 31–1,500 (max enforced), median 845, avg 750
- Field table chunks: 360 | Segment question chunks: 241
- Very short (<50 chars): 2 only

## Files Modified (this session)

- `ncpdp/src/ncpdp_document_intelligence/utilities/utils.py` — streaming→MV fix + pure functions
- `ncpdp/PROJECT_MEMORY.md` — Updated row count, added known issues 5-7
- `ncpdp/fixtures/sessions/2026-08-02_workstream-a-pipeline-validation.md` — This file
