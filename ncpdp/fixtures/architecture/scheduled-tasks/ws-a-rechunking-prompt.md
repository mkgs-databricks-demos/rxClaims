# Scheduled Task Prompt — Workstream A: Segment-Aware Re-Chunking

## scheduleAgentTool Parameters

- **title:** `NCPDP WS-A Re-Chunking`
- **cronExpression:** `0 */15 * * * ?`

## Instructions (copy verbatim into scheduleAgentTool)

```
You are executing Workstream A of the NCPDP Rule Extraction System: Segment-Aware Re-Chunking.

Project: /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/
Git repo: /Users/matthew.giglia@databricks.com/rxClaims

== GATE CHECK ==

1. Read the file at /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/handoffs/workstream-a-status.md
2. Parse the YAML frontmatter between the --- markers.
3. If status is "COMPLETE": respond "WS-A already complete. No action needed." and stop.
4. If status is "IN_PROGRESS": check if the output table exists and has rows. If yes, proceed to validation (step 8). If no, respond "WS-A is in progress in another session. Exiting." and stop.

== CONTEXT ==

Read these files to understand the full architecture:
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/architecture/rule-extraction-system.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/fixtures/architecture/workstream-a-rechunking.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/PROJECT_MEMORY.md
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/src/ncpdp_document_intelligence/utilities/utils.py
- /Users/matthew.giglia@databricks.com/rxClaims/ncpdp/src/ncpdp_document_intelligence/transformations/autoload.py

== EXECUTE ==

5. Update the status file frontmatter to status: IN_PROGRESS and set started_at to the current UTC timestamp.

6. Create a git feature branch: mg-genie-workstream-a-rechunking (branch from main in the rxClaims repo).

7. Implement the re-chunking stage:

   The goal: Add a new transformation to the ncpdp_document_intelligence pipeline that takes the parsed HTML content from specification_documents_parsed, splits it by NCPDP segment section boundaries, strips HTML table markup, and writes clean metadata-enriched chunks to a new table specification_chunks_by_segment.

   Source table: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_documents_parsed
   - Column: content (STRING) — full parsed HTML of the Payer Sheet PDF
   - 1 row per document

   Output table: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_chunks_by_segment
   Schema:
   - chunk_id STRING NOT NULL (deterministic hash of doc_source_id + segment_code + chunk_position)
   - doc_source_id STRING NOT NULL
   - segment_code STRING NOT NULL (e.g., "HD", "01", "07", "10")
   - segment_name STRING NOT NULL (e.g., "Transaction Header", "Claim")
   - segment_am_code STRING NOT NULL (e.g., "AM01", "AM07")
   - transaction_type STRING NOT NULL (e.g., "B1_B3", "B2")
   - chunk_position INT NOT NULL (0-indexed within segment)
   - chunk_text STRING NOT NULL (clean text, HTML stripped)
   - chunk_to_embed STRING NOT NULL (prefixed with context: "NCPDP D.0 | {segment_name} ({segment_code}) | {transaction_type}:\n{chunk_text}")
   - char_count INT
   - has_field_table BOOLEAN
   - has_segment_questions BOOLEAN

   Implementation approach:
   a. Add pure functions to src/ncpdp_document_intelligence/utilities/utils.py in the DocumentIntelligence class (or as module-level testable functions):
      - Segment boundary detection via regex on HTML patterns
      - HTML-to-text conversion for tables
      - Sub-chunking for segments exceeding 1,500 chars
      - Context header generation
   b. Add a new method chunk_by_segment() to the DocumentIntelligence class
   c. Call it from src/ncpdp_document_intelligence/transformations/autoload.py after prep_search()
   d. The new stage should be a streaming table using the SDP API (from pyspark import pipelines as dp)

   Target chunk size: 800-1,200 chars. Max: 1,500 chars.
   HTML table conversion format: "Field {field_code} | {field_name} | Payer Usage: {usage}"

   Use the NCPDP AI Search index (ncpdp_dev.dev_matthew_giglia_rx_claims.specification_search_chunks_index) with query_type="HYBRID" if you need to look up segment structure details.

8. VALIDATE:
   - Run: SELECT COUNT(*), MIN(char_count), MAX(char_count), COUNT(DISTINCT segment_code) FROM ncpdp_dev.dev_matthew_giglia_rx_claims.specification_chunks_by_segment
   - Verify: at least 12 distinct segment_codes, max char_count < 1500, total rows > 100
   - Run: SELECT segment_code, segment_name, COUNT(*) FROM ... GROUP BY 1, 2 ORDER BY 1
   - Verify all expected segments present

9. COMPLETE:
   - Update fixtures/handoffs/workstream-a-status.md:
     - Set status: COMPLETE
     - Set completed_at to current UTC timestamp
     - Set row_count to actual count
     - Set validation: PASSED
     - Add a "What Was Built" section describing the implementation
     - Add a "Notes for Downstream Sessions" section with the table name and key column details
   - Update PROJECT_MEMORY.md with what was accomplished
   - Commit all changes to the feature branch and push

IMPORTANT: Never commit to main. Always use the feature branch. Follow the project's SDP conventions (from pyspark import pipelines as dp, modern API only).
```
