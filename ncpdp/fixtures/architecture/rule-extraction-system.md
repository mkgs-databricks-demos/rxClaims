# NCPDP Rule Extraction System — Architecture

## Overview

This document defines the architecture for extracting structured validation rules
from NCPDP specification documents and making them available for bronze→silver
transformations via Vector Search and Delta tables.

**Problem:** The NCPDP Payer Sheet Template PDF contains hundreds of validation rules
at multiple granularities (transaction-level segment presence, field-level format/value
constraints). These are currently stored as unstructured text chunks in a VS index.
To automate silver-layer transformations and quality expectations, we need these rules
in a structured, queryable format.

**Solution:** A multi-stage pipeline that:
1. Re-chunks the parsed specification by segment section (replacing `ai_prep_search` defaults)
2. Extracts structured rules via LLM (`ai_query`)
3. Stores rules in a typed Delta table (`specification_rules`)
4. Indexes rules in a second VS index for filtered HYBRID retrieval

---

## System Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     NCPDP Document Intelligence Pipeline                     │
│                     (existing — ncpdp_document_intelligence)                  │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  specification_search_chunks (178 rows, CDF enabled)                          │
│  Columns: doc_source_id, path, chunk_id, chunk_position,                     │
│           chunk_to_embed, chunk_to_retrieve                                  │
└──────────┬───────────────────────────────────────────────────┬───────────────┘
           │                                                   │
           ▼                                                   ▼
┌─────────────────────────┐               ┌──────────────────────────────────┐
│ VS Index #1 (existing)  │               │ WORKSTREAM A                      │
│ search_chunks_index     │               │ Segment-Aware Re-Chunking         │
│ • General Q&A           │               │ (optional, improves extraction)   │
│ • HYBRID search         │               └──────────────┬───────────────────┘
│ • 178 chunks            │                              │
└─────────────────────────┘                              ▼
                                          ┌──────────────────────────────────┐
                                          │ specification_chunks_by_segment   │
                                          │ (NEW — cleaner, smaller chunks)   │
                                          │ Columns: chunk_id, segment_code,  │
                                          │   transaction_type, chunk_text,   │
                                          │   chunk_to_embed (context header) │
                                          └──────────────┬───────────────────┘
                                                         │
                                                         ▼
                                          ┌──────────────────────────────────┐
                                          │ WORKSTREAM B                      │
                                          │ AI Rule Extraction                │
                                          │ ai_query per chunk → JSON rules   │
                                          └──────────────┬───────────────────┘
                                                         │
                                                         ▼
                                          ┌──────────────────────────────────┐
                                          │ WORKSTREAM C                      │
                                          │ Post-Processing & Dedup           │
                                          │ • Cross-segment ref resolution    │
                                          │ • Deduplication                   │
                                          │ • Rule ID generation              │
                                          └──────────────┬───────────────────┘
                                                         │
                                                         ▼
                                          ┌──────────────────────────────────┐
                                          │ specification_rules (NEW table)   │
                                          │ PK: rule_id                       │
                                          │ ~1,700 structured rules           │
                                          │ CDF enabled                       │
                                          └──────────┬───────────────────────┘
                                                     │
                                    ┌────────────────┼────────────────┐
                                    ▼                                 ▼
                     ┌─────────────────────────┐     ┌──────────────────────────┐
                     │ VS Index #2 (NEW)       │     │ WORKSTREAM D              │
                     │ specification_rules_idx │     │ Silver-Layer Codegen      │
                     │ • Filtered HYBRID       │     │ • Pivot SQL per segment   │
                     │ • Embed: rule_text      │     │ • @dp.expect rules        │
                     │ • Filter: rule_level,   │     │ • Column comments DDL     │
                     │   segment_code,         │     │ • NCPDP segments pipeline │
                     │   rule_type             │     └──────────────────────────┘
                     └─────────────────────────┘
```

---

## Rule Granularities

### Transaction-Level Rules (segment presence/absence)

Validate that the correct segments exist in a transaction based on field values
in OTHER segments. These operate at the message/claim level.

**Examples:**
- "Transaction Header Segment is always sent" → MANDATORY for all B1/B2/B3
- "Compound Segment required when Compound Code (F_406_D6) = '2'" → SITUATIONAL
- "COB Segment required when Other Coverage Code (F_308_C8) IN ('2','3','4','8')" → SITUATIONAL

**Expectation pattern:**
```python
@dp.expect_or_fail(
    "compound_segment_present_when_required",
    "NOT (F_406_D6 = '2') OR EXISTS(SELECT 1 FROM segments WHERE segment_code = 'AM10')"
)
```

### Field-Level Rules (value validation)

Validate individual field values within a segment. These operate at the
field/row level within a specific segment.

**Sub-types:**
- MANDATORY — field must always be present (non-null)
- REQUIRED_WHEN — field required when a condition is met
- FORMAT — value must match a pattern (6-digit, 11-digit NDC, date format)
- ALLOWED_VALUES — value must be from an enumerated list
- RANGE — numeric value must be within bounds

**Expectation pattern:**
```python
@dp.expect("bin_number_format", "LENGTH(bin_number) = 6 AND bin_number RLIKE '^[0-9]+")
@dp.expect("valid_transaction_code", "transaction_code IN ('B1', 'B2', 'B3')")
@dp.expect("positive_quantity", "quantity_dispensed > 0")
```

---

## Table Schema: `specification_rules`

```sql
CREATE TABLE specification_rules (
    -- Identity
    rule_id                 STRING NOT NULL,           -- Deterministic hash: segment_code + field_code + rule_type + condition
    source_chunk_id         STRING,                    -- FK to specification_search_chunks.chunk_id
    
    -- Classification
    rule_level              STRING NOT NULL,           -- TRANSACTION | FIELD
    rule_type               STRING NOT NULL,           -- MANDATORY | SITUATIONAL | REQUIRED_WHEN | FORMAT | ALLOWED_VALUES | RANGE
    
    -- Segment Context
    segment_code            STRING NOT NULL,           -- AM segment ID: "HD", "01", "04", "07", "10", "11"
    segment_name            STRING NOT NULL,           -- Human: "Transaction Header", "Patient", "Insurance", "Claim"
    
    -- Field Context (NULL for TRANSACTION-level rules)
    field_code              STRING,                    -- NCPDP field: "101-A1", "407-D7", "450-EF"
    field_name              STRING,                    -- Human: "BIN NUMBER", "PRODUCT/SERVICE ID"
    bronze_key              STRING,                    -- Maps to bronze key column: "F_101_A1", "F_407_D7"
    
    -- Transaction Scope
    transaction_types       ARRAY<STRING> NOT NULL,    -- ["B1", "B2", "B3"] or ["B1", "B3"]
    
    -- Rule Definition
    payer_usage             STRING,                    -- M, R, RW, Q (from spec)
    condition               STRING,                    -- SQL WHERE fragment: "F_406_D6 = '2'" or NULL
    condition_segment       STRING,                    -- Segment where condition field lives (for cross-segment refs)
    rule_text               STRING NOT NULL,           -- Natural language description
    allowed_values          ARRAY<STRING>,             -- ["01", "03"] for code fields, NULL otherwise
    format_pattern          STRING,                    -- Regex pattern: "^[0-9]{6}$" for BIN, NULL otherwise
    max_occurrences         INT,                       -- For repeating groups: max count (e.g., 25 ingredients)
    
    -- Silver-Layer Mapping
    column_name             STRING,                    -- snake_case target: "bin_number", "product_service_id"
    data_type               STRING,                    -- SQL type: "STRING", "INT", "DECIMAL(10,3)", "DATE"
    column_comment          STRING,                    -- For COMMENT ON COLUMN DDL
    
    -- Metadata
    extraction_model        STRING,                    -- "databricks-claude-sonnet-4"
    extracted_at            TIMESTAMP,                 -- When this rule was extracted
    confidence              DOUBLE                     -- Future: extraction confidence score
)
CLUSTER BY AUTO
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
```

---

## Workstream Breakdown

Each workstream is designed to be developed independently in a separate Genie Code
session. Dependencies flow top-down (A → B → C → D), but sessions can scaffold
and test with mock data before upstream is complete.

### Workstream A: Segment-Aware Re-Chunking

**See:** `fixtures/architecture/workstream-a-rechunking.md`

### Workstream B: AI Rule Extraction

**See:** `fixtures/architecture/workstream-b-extraction.md`

### Workstream C: Post-Processing & Deduplication

**See:** `fixtures/architecture/workstream-c-postprocessing.md`

### Workstream D: Silver-Layer Code Generation

**See:** `fixtures/architecture/workstream-d-silver-codegen.md`

---

## VS Index #2 Configuration

```yaml
# In ncpdp-ai-search/resources/ncpdp_rules_index.vector_search_index.yml
resources:
  vector_search_indexes:
    ncpdp_rules_index:
      name: ${var.catalog}.${var.schema}.specification_rules_${var.vs_index_suffix}
      endpoint_name: ${resources.vector_search_endpoints.ncpdp_specifications_endpoint.name}
      primary_key: rule_id
      index_type: DELTA_SYNC
      delta_sync_index_spec:
        source_table: ${var.catalog}.${var.schema}.specification_rules
        pipeline_type: TRIGGERED
        embedding_source_columns:
          - name: rule_text
            embedding_model_endpoint_name: ${var.embedding_model}
        columns_to_sync:
          - rule_level
          - rule_type
          - segment_code
          - segment_name
          - field_code
          - bronze_key
          - transaction_types
          - condition
          - condition_segment
          - allowed_values
          - format_pattern
          - column_name
          - data_type
          - payer_usage
          - max_occurrences
```

**Query patterns:**

```python
# Get all transaction-level rules for B1 Claim Billing
results = w.vector_search_indexes.query_index(
    index_name="...",
    columns=["rule_id", "segment_code", "condition", "rule_text"],
    query_text="segment required present mandatory",
    query_type="HYBRID",
    filters_json='{"rule_level": "TRANSACTION"}',
    num_results=20
)

# Get all field rules for the Claim Segment
results = w.vector_search_indexes.query_index(
    index_name="...",
    columns=["field_code", "bronze_key", "column_name", "rule_type", "allowed_values"],
    query_text="claim segment NDC quantity days supply",
    query_type="HYBRID",
    filters_json='{"segment_code": "07", "rule_level": "FIELD"}',
    num_results=50
)
```

---

## Integration Points

| System | How It Uses Rules |
| --- | --- |
| `ncpdp_segments_etl` pipeline | Reads `specification_rules` to generate expectations and pivot SQL |
| Genie Code sessions | Query VS Index #2 for context when building transformations |
| Data quality dashboards | Aggregate expectation pass/fail rates per segment |
| Column comments | `ALTER TABLE ... SET COLUMN COMMENT` DDL from `column_comment` field |
| Future: automated testing | Generate test cases from `allowed_values` and `format_pattern` |

---

## Deployment Order

1. `ncpdp` bundle (creates schema, volumes, runs document intelligence)
2. `ncpdp-ai-search` bundle (creates VS endpoint + both indexes)
3. Rule extraction notebook (populates `specification_rules` table)
4. Sync VS Index #2
5. `ncpdp_segments_etl` pipeline (reads rules, applies transformations)
