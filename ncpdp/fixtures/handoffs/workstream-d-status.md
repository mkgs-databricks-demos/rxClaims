---
status: NOT_STARTED
branch: null
started_at: null
completed_at: null
output_table: ncpdp_dev.dev_matthew_giglia_rx_claims.claimbilling_silver_claim
row_count: null
validation: PENDING
---

# Workstream D — Silver-Layer Code Generation

**Status:** NOT_STARTED

## Upstream Dependencies
- Workstream B+C: `fixtures/handoffs/workstream-bc-status.md` must show `status: COMPLETE`
- Table `specification_rules` must be populated with production-quality rules
- Bronze tables must be populated (ncpdp_etl pipeline already running)

## Expected Output
- Pipeline: `ncpdp_segments_etl` (existing, currently a stub — fully implemented)
- Silver tables: one per segment (claimbilling_silver_header, _patient, _prescriber, _insurance, _claim, _compound, _pricing, _cob, _clinical)
- Transaction-level validation table
- Column comments DDL applied

## Validation Criteria
- At least 5 silver tables created with data
- @dp.expect rules applied from specification_rules
- Pivot columns match bronze_key → column_name mapping
- Data types correctly cast per specification_rules.data_type
- Transaction validation expectations pass on sample data
