# Scheduled Task Prompt — Workstream D: Silver-Layer Code Generation

## scheduleAgentTool Parameters

- **title:** `NCPDP WS-D Silver Codegen`
- **cronExpression:** `0 */15 * * * ?`

## Instructions (copy verbatim into scheduleAgentTool)

```
You are executing Workstream D of the NCPDP Rule Extraction System: Silver-Layer Code Generation. This implements the ncpdp_segments_etl pipeline using extracted specification_rules as driving metadata.

Project: /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/
Git repo: /Users/matthew.giglia@databricks.com/rxClaims

== GATE CHECK ==

1. Read /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/handoffs/workstream-d-status.md
2. Parse YAML frontmatter. If status is "COMPLETE": respond "WS-D already complete." and stop.
3. If status is "IN_PROGRESS": check if any claimbilling_silver_* tables exist with rows. If yes, proceed to validation (step 10). If no, respond "WS-D in progress elsewhere. Exiting." and stop.

4. Read /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/handoffs/workstream-bc-status.md
5. Parse YAML frontmatter. If status != "COMPLETE":
   - Respond "Upstream WS-BC not complete. Waiting." and stop.

6. Verify upstream: SELECT COUNT(*) FROM ncpdp_dev.dev_matthew_giglia_rx_claims.specification_rules
   - If table doesn't exist or has < 500 rows: respond "Upstream rules table insufficient. Waiting." and stop.

== CONTEXT ==

Read these files:
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/architecture/rule-extraction-system.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/architecture/workstream-d-silver-codegen.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/PROJECT_MEMORY.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/handoffs/workstream-bc-status.md (read "Notes for Downstream Sessions")
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/src/ncpdp_segments_etl/transformations/segments.py
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/src/ncpdp_segments_etl/utilities/utils.py
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/resources/ncpdp_segments_etl.pipeline.yml
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/resources/ncpdp_parsing.job.yml

Also explore the specification_rules table to understand available rules:
- SELECT segment_code, segment_name, rule_level, COUNT(*) FROM ncpdp_dev.dev_matthew_giglia_rx_claims.specification_rules GROUP BY 1, 2, 3 ORDER BY 1
- SELECT * FROM specification_rules WHERE segment_code = '07' AND rule_level = 'FIELD' LIMIT 20

Use the NCPDP AI Search index (ncpdp_dev.dev_matthew_giglia_rx_claims.specification_search_chunks_index, query_type="HYBRID") for any NCPDP specification questions.

== EXECUTE ==

7. Update workstream-d-status.md to status: IN_PROGRESS, set started_at.

8. Create git branch: mg-genie-workstream-d-silver-codegen (from main).

9. Implement the ncpdp_segments_etl pipeline:

   The existing pipeline at src/ncpdp_segments_etl/ is a stub. Replace its contents with a fully metadata-driven implementation:

   a. Rewrite src/ncpdp_segments_etl/utilities/utils.py:
      - A Segments class that reads specification_rules at pipeline definition time
      - For each segment: builds pivot SELECT expressions, data type casts, and expectation rules
      - Pure functions for generating SQL expressions from rule metadata

   b. Rewrite src/ncpdp_segments_etl/transformations/segments.py:
      - Replace the current broken stub with rule-driven table generation
      - For each segment_code in specification_rules with rule_level = 'FIELD':
        - Create a streaming table: claimbilling_silver_{segment_name_snake_case}
        - Filter bronze_requests for the segment (WHERE request_segment = 'S_{segment_code}')
        - Pivot key-value rows into typed columns using specification_rules metadata
        - Cast values: MAX(CASE WHEN key = '{bronze_key}' THEN CAST(value::STRING AS {data_type}) END) AS {column_name}
        - Apply @dp.expect rules for MANDATORY fields (NOT NULL), FORMAT rules (RLIKE), and ALLOWED_VALUES rules (IN)
        - Group by transaction_file_source_id, request_pos

   c. Add src/ncpdp_segments_etl/transformations/transaction_validation.py (new):
      - A cross-segment validation table: claimbilling_silver_transaction_validation
      - Read TRANSACTION-level rules from specification_rules
      - Build a summary of which segments are present per (transaction_file_source_id, request_pos)
      - Apply @dp.expect or @dp.expect_or_fail for segment presence rules
      - Example: "NOT (compound_code = '2') OR compound_segment_present = true"

   d. Add src/ncpdp_segments_etl/transformations/apply_comments.py (new, optional):
      - Read specification_rules where column_comment IS NOT NULL
      - Generate ALTER TABLE ... ALTER COLUMN ... COMMENT DDL statements
      - Execute them to apply column documentation

   e. Wire into the job: After implementation, note that ncpdp_parsing.job.yml needs a new task:
      - task_key: Refresh_Segments_Pipeline
      - depends_on: Refresh_Pipeline and Full_Refresh_Pipeline (AT_LEAST_ONE_SUCCESS)
      - pipeline_task referencing ncpdp_segments_etl
      (Document this in the handoff file — the actual YAML edit can happen at PR time)

   Pipeline conventions:
   - Use modern SDP API: from pyspark import pipelines as dp
   - @dp.table for streaming tables, @dp.materialized_view for materialized views
   - spark.readStream.table() for streaming reads
   - spark.read.table() for batch reads (specification_rules is read at definition time)
   - Edition: ADVANCED (already declared in resource YAML)
   - The pipeline resource YAML already exists — do NOT recreate it. Only modify source code.

== VALIDATE ==

10. Run the pipeline (trigger a refresh) and verify:
    - SELECT COUNT(*) FROM ncpdp_dev.dev_matthew_giglia_rx_claims.claimbilling_silver_claim
    - Verify at least 3 rows (matching 3 bronze source files)
    - SHOW TABLES IN ncpdp_dev.dev_matthew_giglia_rx_claims LIKE 'claimbilling_silver_*'
    - Verify at least 5 silver tables created
    - Check expectations passed in the pipeline event log

11. COMPLETE:
    - Update fixtures/handoffs/workstream-d-status.md:
      - status: COMPLETE, completed_at, row_count (from main silver table), validation: PASSED
      - "What Was Built" section: list all silver tables created, number of expectations applied
      - "Notes": which segments were processed, any segments skipped (insufficient rules), job wiring needed
    - Update PROJECT_MEMORY.md
    - Commit and push branch mg-genie-workstream-d-silver-codegen

IMPORTANT: Never commit to main. Use modern SDP API only. Use ${resources.schemas.ncpdp_schema.name} in resource YAML. The existing pipeline resource YAML is correct — only modify source Python files.
```
