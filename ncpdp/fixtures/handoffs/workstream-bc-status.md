---
status: NOT_STARTED
branch: null
started_at: null
completed_at: null
output_table: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_rules
row_count: null
validation: PENDING
---

# Workstream B+C — Rule Extraction Pipeline

**Status:** NOT_STARTED

## Upstream Dependencies
- Workstream A: `fixtures/handoffs/workstream-a-status.md` must show `status: COMPLETE`
- Table `specification_chunks_by_segment` must be populated

## Expected Output
- Pipeline: `ncpdp_rule_extraction` (new SDP pipeline in ncpdp bundle)
- Tables:
  - `specification_rules_raw` (streaming table — raw LLM extraction output)
  - `specification_rules` (materialized view — deduplicated, enriched production rules)
- Expected rows: 800-1,200 rules after deduplication

## Validation Criteria
- All 12 segment types represented
- ≥ 10 TRANSACTION-level rules
- ≥ 50 rules with non-null condition
- ≥ 30 rules with non-null allowed_values
- Zero rules with null segment_code or null rule_level
- All bronze_key values match pattern F_\d{3}_[A-Z]\d+
- No duplicate rule_id values
