# Workstream C: Post-Processing & Deduplication

## Objective

Clean, enrich, and deduplicate the raw extracted rules from Workstream B into
a production-quality specification_rules table ready for VS indexing and
silver-layer code generation.

## Why This Is Needed

Raw LLM extraction produces approximately 1,700 rules with known issues:

1. **Duplicates** — Same field in "Claim Billing" and "Claim Rebill" sections
2. **Cross-segment references** — Condition fields live in different segments
3. **Inconsistent naming** — Same field gets different column_name across chunks
4. **Missing bronze_key** — Unusual field code formats fail derivation
5. **Incomplete conditions** — Abstract concepts need field code resolution

## Inputs

- specification_rules_raw (staging table from Workstream B)
- claimbilling_bronze_requests (for validating bronze_key existence)
- fixtures/config/segments/claimsBilling.yml (ground truth segment codes)

## Output

- specification_rules (production table, schema per rule-extraction-system.md)

## Processing Steps

### Step 1: Deduplication

Deduplicate on natural key (segment_code, field_code, rule_type, condition)
using ROW_NUMBER window function. Keep the rule with more populated fields
(allowed_values, format_pattern, column_comment) and longer rule_text.

Expected reduction: ~1,700 raw to ~800-1,000 deduplicated.

### Step 2: Cross-Segment Reference Resolution

For TRANSACTION-level rules with conditions referencing fields in OTHER segments,
populate condition_segment.

Known cross-segment mappings:
- F_406_D6 -> segment 07 (Compound Code in Claim)
- F_308_C8 -> segment 04 (Other Coverage Code in Insurance)
- F_202_B2 -> segment HD (Service Provider ID Qualifier in Header)
- F_461_EU -> segment 07 (Prior Auth Type Code in Claim)
- F_462_EV -> segment 07 (Prior Auth Number in Claim)

For unknown mappings, look up the field in bronze_requests:
`SELECT DISTINCT request_segment FROM bronze WHERE key = '{field}'`

### Step 3: Column Name Normalization

Canonical column name per field_code (deterministic):
1. Lowercase the field_name
2. Replace spaces and special chars with underscores
3. Strip leading/trailing underscores
4. Truncate to 63 chars (Spark limit)

MERGE canonical names so every rule for the same field_code gets the same value.

### Step 4: Validate bronze_key Coverage

LEFT JOIN rules against UNION of distinct keys from bronze_requests and
bronze_responses. Report rules with bronze_keys that don't appear in sample data.
(Expected: many rules reference fields not in 3-file sample. Still valid.)

### Step 5: Generate Column Comments

Format: "NCPDP Field {field_code}: {field_name}. Usage: {payer_usage}."
Append allowed_values if present. Truncate to 255 chars.

### Step 6: Final Write

Write production table with idempotent MERGE (upsert on rule_id).
Enable CDF and CLUSTER BY AUTO. Add table comment with provenance.

## Validation Criteria

| Metric | Target |
| --- | --- |
| Total rules after dedup | 800-1,200 |
| FIELD rules with bronze_key | >= 95% |
| FIELD rules with column_name | 100% |
| Cross-segment conditions resolved | 100% |
| Duplicate rule_id count | 0 |
| Rules matching bronze data keys | >= 40 |

## Implementation Location

src/ncpdp_rule_extraction/01-postprocess-rules.ipynb

## Dependencies

- Upstream: Workstream B (raw extraction output)
- Downstream: VS Index #2, Workstream D (silver codegen)

## Effort Estimate

- Development: 2-3 hours
- Cross-reference validation: 1 hour
- Total: 3-4 hours

## Genie Code Session Context

```
I'm working on the NCPDP rule extraction system, specifically Workstream C:
Post-Processing & Deduplication.

Read:
- fixtures/architecture/rule-extraction-system.md (overall architecture)
- fixtures/architecture/workstream-c-postprocessing.md (this workstream's spec)
- PROJECT_MEMORY.md (for bronze table schemas)

The goal is to build a notebook at src/ncpdp_rule_extraction/01-postprocess-rules.ipynb
that takes the raw extraction output, deduplicates, resolves cross-segment references,
normalizes column names, and writes the production specification_rules table.

Key tables:
- Input: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_rules_raw
- Output: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_rules
- Reference: ncpdp_dev.dev_matthew_giglia_rx_claims.claimbilling_bronze_requests
- Reference: fixtures/config/segments/claimsBilling.yml
```
