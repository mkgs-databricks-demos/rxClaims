# NCPDP Rule Extraction Pipeline

Spark Declarative Pipeline that extracts structured validation rules from NCPDP
specification chunks using LLM-based extraction (`ai_query`).

## Architecture

### Stage 1: `specification_rules_raw` (streaming table)
- Reads `specification_chunks_by_segment` (from WS-A)
- Filters chunks with `has_field_table=true` OR `has_segment_questions=true`
- Calls `ai_query('databricks-claude-sonnet-4', ...)` with structured extraction prompt
- Parses JSON response and explodes array into individual rule rows

### Stage 2: `specification_rules` (materialized view)
- Deduplicates on natural key (segment_code, field_code, rule_type, condition)
- Generates deterministic `rule_id` via MD5 hash
- Derives `bronze_key` from field_code (e.g., `101-A1` → `F_101_A1`)
- Normalizes `column_name` to snake_case
- Resolves cross-segment condition references
- Generates `column_comment` for DDL
- Applies data quality expectations

## Key Files

| File | Purpose |
| --- | --- |
| `transformations/extract_rules.py` | Pipeline entry point (SDP decorators) |
| `utilities/utils.py` | Pure functions: prompt, parsing, ID generation |
| `tests/test_utils.py` | Unit tests for utility functions |

## Dependencies

- Upstream: `specification_chunks_by_segment` (Workstream A)
- Model: `databricks-claude-sonnet-4`
- Downstream: VS Index #2, Workstream D (silver codegen)

## Running

```bash
databricks bundle deploy --target dev
# Pipeline runs via source-linked mode or job trigger
```
