# Workstream D: Silver-Layer Code Generation

## Objective

Use the structured specification_rules table to generate the ncpdp_segments_etl
pipeline code: pivot SQL per segment, @dp.expect quality rules, column comments
DDL, and proper data type casting.

## Architecture Context

The ncpdp_segments_etl pipeline (already declared in the ncpdp bundle at
resources/ncpdp_segments_etl.pipeline.yml) is currently a stub. This workstream
completes its implementation using the rules as the driving metadata.

**Pipeline edition:** ADVANCED (needed for expectations)
**Channel:** PREVIEW
**Dependencies:** pyyaml (already declared)

## Silver Table Design

For each segment in the bronze data, produce one silver streaming table:

| Bronze Segment | Silver Table Name | Description |
| --- | --- | --- |
| S_HD | claimbilling_silver_header | Transaction header fields (BIN, version, txn code) |
| S_01 | claimbilling_silver_patient | Patient demographics |
| S_03 | claimbilling_silver_prescriber | Prescriber identification |
| S_04 | claimbilling_silver_insurance | Insurance/cardholder info |
| S_07 | claimbilling_silver_claim | Claim details (NDC, qty, days supply) |
| S_10 | claimbilling_silver_compound | Compound ingredient details |
| S_11 | claimbilling_silver_pricing | Pricing (ingredient cost, U&C) |
| S_05 | claimbilling_silver_cob | Coordination of Benefits |
| S_13 | claimbilling_silver_clinical | Clinical/DUR information |

## Transformation Pattern

Each silver table follows the same pattern:

1. **Filter** bronze_requests for the segment (WHERE request_segment = 'S_XX')
2. **Pivot** key-value rows into typed columns using rules metadata
3. **Cast** values to proper types (STRING, INT, DECIMAL, DATE)
4. **Validate** with @dp.expect rules derived from specification_rules

### Pivot Logic

The bronze data is in key-value format:
```
transaction_file_source_id | request_segment | request_pos | key      | value
file_001                   | S_07            | 0           | F_407_D7 | "11111111111"
file_001                   | S_07            | 0           | F_442_E7 | "540"
```

The silver table pivots to columnar:
```
transaction_file_source_id | request_pos | product_service_id | quantity_dispensed
file_001                   | 0           | 11111111111        | 540.000
```

Pivot SQL pattern per segment:
```sql
SELECT
    transaction_file_source_id,
    request_pos,
    MAX(CASE WHEN key = '{bronze_key}' THEN CAST(value::STRING AS {data_type}) END)
        AS {column_name}
    -- ... repeated for each field rule in this segment
FROM claimbilling_bronze_requests
WHERE request_segment = '{segment_filter}'
GROUP BY transaction_file_source_id, request_pos
```

## Metadata-Driven Code Generation

The pipeline code should NOT hardcode the pivot columns. Instead, it reads
specification_rules at pipeline definition time and dynamically builds the
transformation:

```python
from pyspark import pipelines as dp

def build_segment_table(segment_code: str, segment_filter: str, table_name: str):
    """
    Dynamically generate a silver streaming table for a segment
    by reading rules from specification_rules.
    """
    # Read rules for this segment
    rules = spark.read.table("specification_rules").filter(
        f"segment_code = '{segment_code}' AND rule_level = 'FIELD'"
    ).collect()
    
    # Build pivot expressions
    select_exprs = ["transaction_file_source_id", "request_pos"]
    for rule in rules:
        bronze_key = rule["bronze_key"]
        col_name = rule["column_name"]
        data_type = rule["data_type"] or "STRING"
        select_exprs.append(
            f"MAX(CASE WHEN key = '{bronze_key}' THEN "
            f"CAST(value::STRING AS {data_type}) END) AS {col_name}"
        )
    
    # Build expectations
    expectations = {}
    for rule in rules:
        if rule["rule_type"] == "MANDATORY":
            expectations[f"{rule['column_name']}_not_null"] = f"{rule['column_name']} IS NOT NULL"
        if rule["allowed_values"]:
            vals = ", ".join(f"'{v}'" for v in rule["allowed_values"])
            expectations[f"{rule['column_name']}_valid"] = f"{rule['column_name']} IN ({vals})"
        if rule["format_pattern"]:
            expectations[f"{rule['column_name']}_format"] = (
                f"{rule['column_name']} RLIKE '{rule['format_pattern']}'"
            )
    
    return select_exprs, expectations
```

## Transaction-Level Validation

Separate from field-level expectations, validate segment presence rules:

```python
@dp.table(name="claimbilling_silver_transaction_validation")
@dp.expect_or_fail("header_always_present", "header_present = true")
@dp.expect("compound_when_required",
    "NOT (compound_code = '2') OR compound_segment_present = true")
def validate_transactions():
    """
    Cross-segment validation: check that required segments are present
    based on field values in other segments.
    """
    # Build a transaction-level summary:
    # - Which segments are present per (transaction_file_source_id, request_pos)
    # - Key field values that trigger conditional segments
    ...
```

## Column Comments DDL

After silver tables are created, apply column comments from specification_rules:

```sql
ALTER TABLE claimbilling_silver_claim
    ALTER COLUMN product_service_id
    COMMENT 'NCPDP Field 407-D7: PRODUCT/SERVICE ID. Usage: Mandatory. 11-digit NDC.';

ALTER TABLE claimbilling_silver_claim
    ALTER COLUMN quantity_dispensed
    COMMENT 'NCPDP Field 442-E7: QUANTITY DISPENSED. Usage: Mandatory. Metric decimal quantity.';
```

This can be generated as a post-pipeline notebook that reads specification_rules
and emits ALTER statements.

## Implementation Location

Existing pipeline: src/ncpdp_segments_etl/

Files to create/modify:
- src/ncpdp_segments_etl/segments.py (fix syntax bug, implement rule-driven logic)
- src/ncpdp_segments_etl/silver_transforms.py (new: metadata-driven pivot builder)
- src/ncpdp_segments_etl/transaction_validation.py (new: cross-segment checks)
- src/ncpdp_segments_etl/apply_comments.py (new: DDL notebook for column comments)

## Integration with ncpdp_parsing Job

The segments pipeline should run AFTER the primary ETL pipeline completes
(it reads bronze tables as input). Add to ncpdp_parsing.job.yml:

```yaml
- task_key: Refresh_Segments_Pipeline
  depends_on:
    - task_key: Refresh_Pipeline
    - task_key: Full_Refresh_Pipeline
  run_if: AT_LEAST_ONE_SUCCESS
  pipeline_task:
    pipeline_id: resources.pipelines.ncpdp_segments_etl.id
    full_refresh: false
```

## Dependencies

- **Upstream:** Workstream C (production specification_rules table)
- **Upstream:** ncpdp_etl pipeline (bronze tables must be populated)
- **Existing:** ncpdp_segments_etl pipeline resource (already declared in bundle)

## Effort Estimate

- Pipeline code (pivot builder, expectations): 3-4 hours
- Transaction-level validation: 2 hours
- Column comments notebook: 1 hour
- Job integration: 30 min
- Testing: 2 hours
- Total: 8-10 hours

## Genie Code Session Context

When starting a new session for this workstream:

```
I'm working on the NCPDP rule extraction system, specifically Workstream D:
Silver-Layer Code Generation.

Read:
- fixtures/architecture/rule-extraction-system.md (overall architecture)
- fixtures/architecture/workstream-d-silver-codegen.md (this workstream's spec)
- src/ncpdp_segments_etl/ (existing pipeline stub)
- resources/ncpdp_segments_etl.pipeline.yml (pipeline resource config)
- resources/ncpdp_parsing.job.yml (job to wire into)
- PROJECT_MEMORY.md (known issue: segments.py syntax bug on line 8)

The goal is to implement the ncpdp_segments_etl pipeline using specification_rules
as driving metadata. The pipeline reads bronze key-value rows, pivots into typed
columns per segment, applies @dp.expect quality rules, and produces silver tables.

Key tables:
- Rules: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_rules
- Input: ncpdp_dev.dev_matthew_giglia_rx_claims.claimbilling_bronze_requests
- Input: ncpdp_dev.dev_matthew_giglia_rx_claims.claimbilling_bronze_responses
- Output: claimbilling_silver_{segment} (one per segment)

The pipeline is ADVANCED edition with PREVIEW channel and pyyaml dependency.
Use the modern SDP API (from pyspark import pipelines as dp).
```
