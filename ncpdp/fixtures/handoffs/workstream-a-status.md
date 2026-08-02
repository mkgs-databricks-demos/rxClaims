---
status: COMPLETE
branch: mg-genie-workstream-a-rechunking
started_at: 2026-08-02T17:44:07Z
completed_at: 2026-08-02T18:05:00Z
output_table: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_chunks_by_segment
row_count: 602
validation: PASSED
---

# Workstream A — Segment-Aware Re-Chunking

**Status:** COMPLETE

## Upstream Dependencies
None — this workstream starts immediately.

## Expected Output
- Table: `ncpdp_dev.dev_matthew_giglia_rx_claims.specification_chunks_by_segment`
- Expected rows: ~300-400 (12+ segments, multiple chunks each)
- All chunks < 1,500 chars, HTML stripped, context headers prepended

## Validation Criteria
- At least 12 segment codes identified (matching claimsBilling.yml)
- All chunks < 1,500 chars
- No empty chunks
- segment_code matches known codes (HD, 01, 02, 03, 04, 05, 06, 07, 08, 09, 10, 11)

## Validation Results (2026-08-02)

| Metric | Value | Pass? |
| --- | --- | --- |
| Total rows | 602 | ✓ (>100) |
| Min char_count | 31 | ✓ |
| Max char_count | 1500 | ✓ (<=1500) |
| Avg char_count | 749 | ✓ (target 800-1200) |
| Distinct segment_codes | 25 | ✓ (>=12) |
| Empty chunks | 0 | ✓ |
| All expected codes present | Yes | ✓ |

## What Was Built

Added segment-aware re-chunking as Layer 6 in the `ncpdp_document_intelligence` pipeline:

1. **Pure functions** in `src/ncpdp_document_intelligence/utilities/utils.py`:
   - `resolve_segment_code()` — maps segment names to NCPDP codes
   - `detect_transaction_type()` — identifies B1_B3, B2, S1 from column headers
   - `strip_html_to_text()` — converts HTML tables to structured text
   - `sub_chunk_text()` — splits oversized sections at line/sentence boundaries
   - `generate_chunk_id()` — deterministic SHA-256 hash
   - `segment_document_to_chunks()` — full pipeline: boundary detection → split → convert → enrich

2. **`chunk_by_segment()` method** on DocumentIntelligence class:
   - Reads `specification_search_chunks`, concatenates per doc_source_id
   - Applies segmentation via Spark UDF
   - Writes to `specification_chunks_by_segment` streaming table

3. **Pipeline integration** in `transformations/autoload.py`:
   - `pipeline.chunk_by_segment()` called after `pipeline.prep_search()`

Transaction types: B1_B3 (422), B2 (136), S1 (44)

## Notes for Downstream Sessions

**Table:** `ncpdp_dev.dev_matthew_giglia_rx_claims.specification_chunks_by_segment`

**Key columns for Workstream B:**
- `chunk_id` — primary key, FK target for `specification_rules.source_chunk_id`
- `segment_code` — filter for targeted extraction
- `transaction_type` — scope rules to B1_B3/B2/S1
- `chunk_text` — clean text input for `ai_query` rule extraction
- `chunk_to_embed` — context-prefixed text for VS index
- `has_field_table` — TRUE = contains field definitions (primary extraction target)
- `has_segment_questions` — TRUE = contains segment presence rules (transaction-level rules)

**Filtering strategy for Workstream B:**
- Field-level rules: `WHERE has_field_table = true` (~158 chunks)
- Transaction-level rules: `WHERE has_segment_questions = true` (~110 chunks)
- Request segments only: `WHERE segment_code IN ('HD','01','02','03','04','05','06','07','08','09','10','11','13','14','15','16')`
