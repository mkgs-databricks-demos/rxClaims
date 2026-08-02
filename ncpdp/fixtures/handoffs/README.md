# Workstream Handoffs — Inter-Session Communication Protocol

## Purpose

This folder enables asynchronous coordination between Genie Code scheduled task
sessions executing the NCPDP Rule Extraction System workstreams (A, B+C, D).

Each session reads its upstream status file as a gate check before starting work,
and writes its own status file upon completion. This pattern is inspired by the
lakeLoom `hi_genie/` / `hey_isaac/` async communication model.

## Protocol

### Status File Contract

Each file uses YAML frontmatter for machine-parseable status, followed by
human-readable notes for downstream sessions:

```markdown
---
status: NOT_STARTED | IN_PROGRESS | COMPLETE | BLOCKED
branch: mg-genie-workstream-{x}-{description}
started_at: ISO8601 timestamp (set when IN_PROGRESS)
completed_at: ISO8601 timestamp (set when COMPLETE)
output_table: fully qualified table name
row_count: integer
validation: PASSED | FAILED | PENDING
---

# Workstream {X} — {Status}

## What Was Built
...

## Notes for Downstream Sessions
...

## Validation Results
...
```

### Gate Check Logic (each session run)

```
1. Read OWN status file
   → If status == COMPLETE: exit immediately (already done)
   
2. Read UPSTREAM status file
   → If status != COMPLETE: write own status as "WAITING", exit
   
3. Upstream is COMPLETE → proceed with work
   → Write own status as IN_PROGRESS
   → Do the work
   → Validate output
   → Write own status as COMPLETE with metadata
```

### Dependency Graph

```
Workstream A (Re-Chunking)
    ↓ reads specification_chunks_by_segment table
Workstream B+C (Rule Extraction Pipeline)
    ↓ reads specification_rules table  
Workstream D (Silver Codegen)
```

- **WS-A** has no upstream dependency — starts immediately
- **WS-BC** gates on WS-A COMPLETE
- **WS-D** gates on WS-BC COMPLETE

### Branch Strategy

Each workstream creates its own feature branch off `main`:
- They operate on **different source directories** (no merge conflicts)
- The gate check verifies the **output table** exists and is populated
- Code merging happens after human review (PR per branch)

| Workstream | Source Directory | Output Table |
| --- | --- | --- |
| A | `src/ncpdp_document_intelligence/` | `specification_chunks_by_segment` |
| B+C | `src/ncpdp_rule_extraction/` + `resources/` | `specification_rules` |
| D | `src/ncpdp_segments_etl/` | `claimbilling_silver_{segment}` |

### Self-Termination

Scheduled tasks poll every 15 minutes. Once a session writes COMPLETE to its own
status file, subsequent runs exit immediately on step 1 (idempotent). The human
operator pauses or deletes the scheduled tasks once all three show COMPLETE.

## Files

| File | Written By | Read By |
| --- | --- | --- |
| `workstream-a-status.md` | WS-A session | WS-BC session |
| `workstream-bc-status.md` | WS-BC session | WS-D session |
| `workstream-d-status.md` | WS-D session | Human (final status) |
