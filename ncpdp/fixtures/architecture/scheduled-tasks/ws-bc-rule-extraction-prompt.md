# Scheduled Task Prompt — Workstream B+C: Rule Extraction Pipeline

## scheduleAgentTool Parameters

- **title:** `NCPDP WS-BC Rule Extraction`
- **cronExpression:** `0 */15 * * * ?`

## Instructions (copy verbatim into scheduleAgentTool)

```
You are executing Workstream B+C of the NCPDP Rule Extraction System: AI Rule Extraction + Post-Processing, implemented as a new Spark Declarative Pipeline.

Project: /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/
Git repo: /Users/matthew.giglia@databricks.com/rxClaims

== GATE CHECK ==

1. Read /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/handoffs/workstream-bc-status.md
2. Parse YAML frontmatter. If status is "COMPLETE": respond "WS-BC already complete." and stop.
3. If status is "IN_PROGRESS": check if specification_rules table has >500 rows. If yes, proceed to validation (step 9). If no, respond "WS-BC in progress elsewhere. Exiting." and stop.

4. Read /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/handoffs/workstream-a-status.md
5. Parse YAML frontmatter. If status != "COMPLETE":
   - Update workstream-bc-status.md frontmatter to status: NOT_STARTED (leave other fields)
   - Respond "Upstream WS-A not complete. Waiting." and stop.

6. Verify upstream table exists: SELECT COUNT(*) FROM ncpdp_dev.dev_matthew_giglia_rx_claims.specification_chunks_by_segment
   - If table doesn't exist or has 0 rows: respond "Upstream table not populated. Waiting." and stop.

== CONTEXT ==

Read these files:
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/architecture/rule-extraction-system.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/architecture/workstream-b-extraction.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/architecture/workstream-c-postprocessing.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/PROJECT_MEMORY.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/handoffs/workstream-a-status.md (read the "Notes for Downstream Sessions" section)
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/config/segments/claimsBilling.yml

Also query the NCPDP AI Search index (ncpdp_dev.dev_matthew_giglia_rx_claims.specification_search_chunks_index, query_type="HYBRID") for any NCPDP specification questions.

== EXECUTE ==

7. Update workstream-bc-status.md to status: IN_PROGRESS, set started_at.

8. Create git branch: mg-genie-workstream-bc-rule-extraction (from main).

9. Build the ncpdp_rule_extraction pipeline:

   This is a NEW Spark Declarative Pipeline in the ncpdp bundle. Create:

   a. Resource YAML: resources/ncpdp_rule_extraction.pipeline.yml
      - Name: ncpdp_rule_extraction
      - Serverless: true
      - Channel: PREVIEW
      - Edition: ADVANCED (needed for expectations)
      - Catalog: ${var.catalog}
      - Schema: ${resources.schemas.ncpdp_schema.name}
      - Root path: ../src/ncpdp_rule_extraction
      - Libraries: glob include ../src/ncpdp_rule_extraction/transformations/**
      - Configuration: catalog_use, schema_use (same pattern as other pipelines)
      - Event log: event_log_ncpdp_rule_extraction in ${var.event_log_schema}
      - Tags: same pattern as other pipelines
      - Environment dependencies: none needed beyond default

   b. Source directory: src/ncpdp_rule_extraction/
      - transformations/extract_rules.py (entry point)
      - utilities/utils.py (extraction logic, prompt, parsing)
      - tests/ (unit tests for parsing functions)
      - README.md

   c. Pipeline stages (streaming tables and materialized views):

      STAGE 1 — specification_rules_raw (streaming table):
      - Reads from specification_chunks_by_segment via CDF (spark.readStream.table(...))
      - For each chunk, calls ai_query('databricks-claude-sonnet-4', extraction_prompt + chunk_text)
      - Parses the JSON response into a VARIANT column
      - Explodes the array of rules into individual rows
      - Adds: source_chunk_id, extraction_model, extracted_at
      - Generates deterministic rule_id via MD5 hash of (segment_code + field_code + rule_type + condition + transaction_types)

      The extraction prompt is specified in workstream-b-extraction.md — use it verbatim.

      IMPORTANT: ai_query in streaming context — use spark.sql() with parameterized SQL:
      SELECT ai_query(:model, CONCAT(:prompt, '\n\nSpecification text:\n', chunk_text)) as raw_rules
      FROM specification_chunks_by_segment

      STAGE 2 — specification_rules (materialized view):
      - Reads from specification_rules_raw
      - Deduplicates on natural key (segment_code, field_code, rule_type, condition) using ROW_NUMBER
      - Resolves cross-segment references (populates condition_segment)
      - Normalizes column_name (lowercase, underscores, truncate to 63 chars)
      - Generates bronze_key from field_code: replace '-' with '_', prepend 'F_'
      - Generates column_comment: "NCPDP Field {field_code}: {field_name}. Usage: {payer_usage}."
      - Applies @dp.expect rules:
        - "rule_id_not_null": rule_id IS NOT NULL
        - "segment_code_not_null": segment_code IS NOT NULL
        - "rule_level_valid": rule_level IN ('TRANSACTION', 'FIELD')
        - "bronze_key_format": bronze_key IS NULL OR bronze_key RLIKE '^F_[0-9]{3}_[A-Z][0-9]+

      Full schema for specification_rules is in rule-extraction-system.md.

   d. Table properties for both tables:
      - delta.enableChangeDataFeed: true
      - delta.enableDeletionVectors: true
      - delta.enableRowTracking: true
      - CLUSTER BY AUTO

10. After creating all files, deploy and run the pipeline:
    - The pipeline is source-linked in dev mode, so files in the workspace are live
    - If the pipeline resource doesn't exist yet in the deployed bundle, you may need to run: databricks bundle deploy --target dev (from the ncpdp bundle directory)
    - Trigger a full refresh of the pipeline
    - Wait for completion and verify output

== VALIDATE (step 9 for re-entry) ==

11. Run validation queries:
    - SELECT COUNT(*) as total_rules, COUNT(DISTINCT segment_code) as segments, COUNT(DISTINCT rule_level) as levels FROM ncpdp_dev.dev_matthew_giglia_rx_claims.specification_rules
    - Verify: total_rules >= 500, segments >= 12, levels = 2
    - SELECT rule_level, rule_type, COUNT(*) FROM specification_rules GROUP BY 1, 2
    - Verify: TRANSACTION rules >= 10, rules with non-null condition >= 50
    - SELECT COUNT(*) FROM specification_rules WHERE allowed_values IS NOT NULL
    - Verify: >= 30
    - SELECT COUNT(*) FROM specification_rules WHERE rule_id IS NULL OR segment_code IS NULL
    - Verify: 0
    - SELECT COUNT(*) - COUNT(DISTINCT rule_id) as duplicates FROM specification_rules
    - Verify: 0

12. COMPLETE:
    - Update fixtures/handoffs/workstream-bc-status.md:
      - status: COMPLETE, completed_at, row_count, validation: PASSED
      - "What Was Built" section: describe the pipeline, its stages, the resource YAML
      - "Notes for Downstream Sessions": table name, key columns (rule_id, segment_code, bronze_key, column_name, rule_type, allowed_values, format_pattern), how to query for segment-specific rules
    - Update PROJECT_MEMORY.md
    - Commit and push branch mg-genie-workstream-bc-rule-extraction

IMPORTANT: Never commit to main. Use modern SDP API only (from pyspark import pipelines as dp). Use ${resources.schemas.ncpdp_schema.name} for schema references in resource YAML (never raw ${var.schema}).
```
