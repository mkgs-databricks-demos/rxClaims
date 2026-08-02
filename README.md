# rxClaims
***

Ingestion pipelines for standard pharmacy formats from [NCPDP](https://www.ncpdp.org/) including Claims Billing requests, responses, and supplemental files.

## Architecture

This is a **two-bundle monorepo** with a shared git repository:

| Bundle | Purpose | Deploy Order |
| --- | --- | --- |
| `ncpdp/` | Core infrastructure: schema, volumes, ETL pipelines, document intelligence, rule extraction | 1st |
| `ncpdp-ai-search/` | Vector Search endpoint + Delta Sync indexes for specification retrieval | 2nd |

### Pipeline DAG

```
ncpdp_parsing (Job — orchestrator)
├─ Volume Setup (gated: run_set_up=true)
├─ Specification Document Processing (gated: development + run_spec_process=true)
│   ├─ Download Spec PDF
│   └─ ncpdp_document_intelligence (Pipeline)
│       ├─ stream_ingest → specification_documents
│       ├─ parse_documents → specification_documents_parsed
│       ├─ classify_documents → specification_documents_classified
│       ├─ extract_fields → specification_documents_extracted
│       ├─ prep_search → specification_search_chunks
│       └─ [NEW] chunk_by_segment → specification_chunks_by_segment
├─ ncpdp_rule_extraction (Pipeline — NEW)
│   ├─ extract_rules → specification_rules_raw
│   └─ postprocess_rules → specification_rules
├─ ncpdp_etl (Pipeline — primary ETL)
│   └─ Auto Loader → claimbilling_bronze_*
└─ ncpdp_segments_etl (Pipeline — silver layer)
    └─ Rule-driven pivot → claimbilling_silver_*
```

### Orchestration via Genie Code Scheduled Tasks

The Rule Extraction System is built by three coordinated Genie Code sessions:

1. **WS-A** (Re-Chunking) → adds segment-aware splitting to doc intelligence
2. **WS-BC** (Rule Extraction) → new pipeline extracting structured rules via LLM
3. **WS-D** (Silver Codegen) → metadata-driven silver table generation

Sessions coordinate via `fixtures/handoffs/` status files (15-min polling).
See `fixtures/architecture/scheduled-tasks/` for full prompt specifications.

## Prerequisites

This project requires:
- [ai_parse_document](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document) (not available on Free Edition)
- [ai_query](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query) for rule extraction
- Vector Search endpoint (STANDARD tier)
- Serverless compute

Please use a paid or Express Account to try this workflow.

## Quick Start

```bash
# Deploy core infrastructure first
cd ncpdp && databricks bundle deploy --target dev

# Then deploy AI Search indexes
cd ../ncpdp-ai-search && databricks bundle deploy --target dev
```
