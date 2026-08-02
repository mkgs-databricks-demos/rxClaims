# Workstream B: AI Rule Extraction

## Objective

Extract structured validation rules from specification chunks using `ai_query`
and write them to the `specification_rules` Delta table. This is the core
intelligence step that transforms unstructured spec text into actionable rules.

## Inputs

**Primary (if Workstream A is complete):**
`specification_chunks_by_segment` — cleaner chunks with segment metadata

**Fallback (can start immediately):**
`specification_search_chunks` — existing 178 chunks (larger, HTML-laden, but available now)

The extraction prompt works with either source. Workstream A improves accuracy
but is not a blocker.

## Output

`specification_rules` table (see schema in `rule-extraction-system.md`)

## Proven Extraction Prompt (v2)

This prompt was validated in the 2026-08-02 prototype (48 rules from 3 chunks):

```python
EXTRACTION_PROMPT = """
You are an NCPDP D.0 specification parser. Extract ALL validation rules from
the following specification text.

CRITICAL: In the "condition" field, ALWAYS reference the original NCPDP field
code using the format F_NNN_XX (matching the bronze data keys), not abstract
column names.
Example: Instead of "compound_code = '2'", write "F_406_D6 = '2'" because
field 406-D6 is the Compound Code.

Return a JSON array where each object has:
- rule_level: "TRANSACTION" (segment presence/absence based on values in OTHER
  segments) or "FIELD" (value/format validation for this specific field)
- segment_code: The AM segment identifier from "111-AM = XX" (e.g., "10" for
  Compound, "01" for Patient, "04" for Insurance, "07" for Claim, "11" for Pricing)
- segment_name: Human-readable segment name
- field_code: NCPDP field number for FIELD rules (e.g., "450-EF"). null for
  TRANSACTION rules.
- field_name: Human-readable field name. null for TRANSACTION rules.
- transaction_types: Array of transaction codes (e.g., ["B1", "B3"])
- rule_type: "MANDATORY" (always required) | "SITUATIONAL" (segment presence
  depends on condition) | "REQUIRED_WHEN" (field required when condition met) |
  "FORMAT" (value format constraint) | "ALLOWED_VALUES" (enumerated valid values)
- payer_usage: Payer usage code (M, R, RW, Q) or null
- condition: SQL WHERE clause referencing NCPDP field codes as F_NNN_XX. null
  if always applies.
- condition_segment: The segment code where the condition field lives (for
  cross-segment references). null if condition is null or same-segment.
- rule_text: Concise description including any payer-specific situations.
- column_name: snake_case name for silver layer (FIELD only)
- data_type: SQL type (STRING, INT, DECIMAL(p,s), DATE) or null for TRANSACTION
- allowed_values: Array of valid values if known (e.g., ["01", "03"]) or null
- format_pattern: Regex pattern for format validation or null
- max_occurrences: For repeating field groups, max count (e.g., 25) or null

Be thorough. Extract EVERY rule including implicit ones like "This Segment is
always sent" or "Maximum count of 3".

Return ONLY valid JSON array, no markdown fences.
"""
```

## Implementation

### File Location

`src/ncpdp_rule_extraction/00-extract-rules.ipynb`

This is a standalone notebook (not part of an SDP pipeline) because:
- It's a one-time bulk extraction with retry logic
- It needs to handle JSON parsing failures gracefully
- It writes to a table that's consumed by VS Index #2 AND the segments pipeline

### Processing Logic

```python
import json
import re
import hashlib
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Configuration
MODEL_ENDPOINT = "databricks-claude-sonnet-4"
SOURCE_TABLE = "specification_search_chunks"  # or specification_chunks_by_segment
OUTPUT_TABLE = "specification_rules"
BATCH_SIZE = 5  # chunks per batch (for rate limiting)

# Process each chunk
for chunk_row in chunks_df.collect():
    chunk_id = chunk_row["chunk_id"]
    chunk_text = chunk_row["chunk_to_retrieve"]  # or chunk_text if using Workstream A output
    
    # Call ai_query
    result = spark.sql(
        "SELECT ai_query(:model, :prompt) as rules",
        args={
            "model": MODEL_ENDPOINT,
            "prompt": EXTRACTION_PROMPT + f"\n\nSpecification text:\n{chunk_text}"
        }
    ).first()["rules"]
    
    # Parse and validate JSON
    rules = parse_and_validate(result, chunk_id)
    
    # Generate deterministic rule_ids
    for rule in rules:
        rule["rule_id"] = generate_rule_id(rule)
        rule["source_chunk_id"] = chunk_id
        rule["extraction_model"] = MODEL_ENDPOINT
        rule["extracted_at"] = datetime.utcnow().isoformat()
    
    # Append to output
    batch_rules.extend(rules)
```

### Rule ID Generation

```python
def generate_rule_id(rule: dict) -> str:
    """Deterministic ID for deduplication."""
    components = [
        rule.get("segment_code", ""),
        rule.get("field_code", "") or "",
        rule.get("rule_type", ""),
        rule.get("condition", "") or "",
        "|".join(sorted(rule.get("transaction_types", [])))
    ]
    key = "|".join(components)
    return hashlib.md5(key.encode()).hexdigest()[:16]
```

### Error Handling

```python
def parse_and_validate(raw_response: str, chunk_id: str) -> list:
    """Parse LLM response, handling common issues."""
    # Strip markdown fences
    cleaned = re.sub(r'^```json?\s*', '', raw_response.strip())
    cleaned = re.sub(r'\s*```, '', cleaned)
    
    if not cleaned:
        log_error(chunk_id, "empty_response")
        return []
    
    try:
        rules = json.loads(cleaned)
    except json.JSONDecodeError as e:
        log_error(chunk_id, f"json_parse_error: {e}")
        return []
    
    # Validate required fields
    valid_rules = []
    for r in rules:
        if not r.get("rule_level") or not r.get("segment_code"):
            log_error(chunk_id, f"missing_required_field: {r}")
            continue
        
        # Normalize bronze_key from field_code
        if r.get("field_code"):
            r["bronze_key"] = "F_" + r["field_code"].replace("-", "_")
        
        valid_rules.append(r)
    
    return valid_rules
```

### bronze_key Derivation

The bronze data uses keys like `F_101_A1`, `F_407_D7`. The spec uses `101-A1`, `407-D7`.
Mapping: replace `-` with `_`, prepend `F_`.

```python
def field_code_to_bronze_key(field_code: str) -> str:
    """101-A1 -> F_101_A1"""
    return "F_" + field_code.replace("-", "_")
```

### Batch Processing Strategy

- Process 5 chunks per batch (rate limiting for ai_query)
- Write accumulated rules every 20 chunks (checkpoint)
- On failure: log the chunk_id and continue (don't fail the whole run)
- Final step: MERGE into target table (upsert on rule_id)

## Known Edge Cases

1. **Empty chunks** — Some chunks contain only boilerplate (cover page, table of contents).
   The LLM will return an empty array `[]`. This is correct behavior.

2. **Markdown fences in response** — Despite prompt instructions, ~10% of responses
   include ` ```json ` fences. The `parse_and_validate` function strips these.

3. **Cross-segment conditions** — "Compound Segment required when F_406_D6 = '2'"
   references field 406-D6 which lives in the Claim Segment (AM07), not AM10.
   The prompt asks for `condition_segment` to capture this.

4. **Repeating field groups** — Compound ingredients (488-RE through 449-EE) repeat
   per ingredient. `max_occurrences` captures the limit (e.g., 25).

5. **Payer-specific overrides** — Some rules say "Payer Requirement: Maximum of 10
   ingredients" which is stricter than the standard (25). Both should be captured
   as separate rules with different `rule_text`.

## Validation Criteria

**Minimum viable extraction:**
- ≥ 500 total rules extracted (conservative estimate from 178 chunks)
- All 12 segment types from `claimsBilling.yml` represented
- ≥ 10 TRANSACTION-level rules (segment presence)
- ≥ 50 rules with non-null `condition` (conditional rules)
- ≥ 30 rules with non-null `allowed_values`
- Zero rules with null `segment_code` or null `rule_level`

**Quality checks:**
- All `bronze_key` values match pattern `F_\d{3}_[A-Z]\d+`
- All `condition` values that reference fields use `F_NNN_XX` format
- No duplicate `rule_id` values (deterministic hashing works)
- `transaction_types` arrays contain only valid codes: B1, B2, B3, S1

## Testing Strategy

1. **Prototype test** (already done) — 3 chunks → 48 rules, validated structure
2. **Full extraction dry run** — Process all 178 chunks, write to staging table
3. **Cross-reference with `claimsBilling.yml`** — The fixture config has 12 segments
   with known rules; verify extraction finds them all
4. **Spot-check conditions** — Manually verify 10 conditional rules map correctly
   to bronze data keys

## Dependencies

- **Upstream:** `specification_search_chunks` table (already populated, 178 rows)
- **Optional upstream:** Workstream A (`specification_chunks_by_segment`)
- **Downstream:** Workstream C (post-processing), VS Index #2, Workstream D (silver codegen)

## Effort Estimate

- Notebook development: 2–3 hours
- Full extraction run: ~5 min (178 chunks × 200–600ms each)
- Validation & spot-checks: 1 hour
- Total: 4–5 hours

## Genie Code Session Context

When starting a new session for this workstream:

```
I'm working on the NCPDP rule extraction system, specifically Workstream B:
AI Rule Extraction.

Read:
- fixtures/architecture/rule-extraction-system.md (overall architecture)
- fixtures/architecture/workstream-b-extraction.md (this workstream's spec)
- PROJECT_MEMORY.md (for context on tables and bundle structure)

The goal is to build a notebook at src/ncpdp_rule_extraction/00-extract-rules.ipynb
that reads specification_search_chunks, calls ai_query per chunk to extract
structured rules, and writes to a new specification_rules Delta table.

Key tables:
- Source: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_search_chunks (178 rows)
- Target: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_rules (new)
- Reference: fixtures/config/segments/claimsBilling.yml (ground truth for validation)

The extraction prompt (v2) is proven — see the workstream doc for the exact prompt text.
Model: databricks-claude-sonnet-4 via ai_query (parameterized SQL).
```
