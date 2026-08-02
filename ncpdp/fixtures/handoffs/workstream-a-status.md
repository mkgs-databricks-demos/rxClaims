---
status: NOT_STARTED
branch: null
started_at: null
completed_at: null
output_table: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_chunks_by_segment
row_count: null
validation: PENDING
---

# Workstream A — Segment-Aware Re-Chunking

**Status:** NOT_STARTED

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
