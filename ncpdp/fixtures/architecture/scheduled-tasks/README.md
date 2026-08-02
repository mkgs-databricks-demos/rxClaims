# Scheduled Task Orchestration — NCPDP Rule Extraction System

## Overview

Three Genie Code scheduled tasks execute the Rule Extraction System workstreams
in sequence, coordinated via the `fixtures/handoffs/` status file protocol.

All tasks are created simultaneously and poll every 15 minutes. Each run costs
<10 seconds if the gate check fails (upstream not ready or own work already done).

## Task Summary

| Task | Title | Cron | Gates On | Editor Context |
| --- | --- | --- | --- | --- |
| WS-A | NCPDP Re-Chunking | `0 */15 * * * ?` | None (immediate) | Edits `src/ncpdp_document_intelligence/` |
| WS-BC | NCPDP Rule Extraction | `0 */15 * * * ?` | WS-A COMPLETE | Creates `src/ncpdp_rule_extraction/` + resource YAML |
| WS-D | NCPDP Silver Codegen | `0 */15 * * * ?` | WS-BC COMPLETE | Edits `src/ncpdp_segments_etl/` |

## Execution Flow

```
T+0 min:  All 3 tasks created, first cron fires
          WS-A: No gate → starts work (~2-3 hrs)
          WS-BC: Reads WS-A status = NOT_STARTED → writes WAITING, exits
          WS-D: Reads WS-BC status = NOT_STARTED → writes WAITING, exits

T+15 min: WS-BC and WS-D re-fire, still WAITING, exit immediately

T+~3 hrs: WS-A completes, writes COMPLETE to its status file

T+next 15min: WS-BC reads WS-A = COMPLETE → starts work (~4-5 hrs)
              WS-D: Reads WS-BC = NOT_STARTED → exits

T+~8 hrs: WS-BC completes, writes COMPLETE

T+next 15min: WS-D reads WS-BC = COMPLETE → starts work (~8-10 hrs)

T+~18 hrs: WS-D completes. All three status files show COMPLETE.
           Operator pauses/deletes all scheduled tasks.
```

## Prompt Files

- `ws-a-rechunking-prompt.md` — Full instructions for Workstream A
- `ws-bc-rule-extraction-prompt.md` — Full instructions for Workstream B+C
- `ws-d-silver-codegen-prompt.md` — Full instructions for Workstream D

## Key Design Decisions

1. **Polling not event-driven** — Scheduled tasks are cron-based. 15-min poll
   latency is negligible relative to multi-hour workstream execution times.

2. **Self-terminating** — Each task checks its own status first. Once COMPLETE,
   it exits in <10 seconds on every subsequent fire.

3. **Table-gated, not code-gated** — The gate check verifies the OUTPUT TABLE
   is populated (via SQL COUNT), not that code is merged. This decouples
   workstream execution from PR review/merge cadence.

4. **Independent branches** — Each workstream creates its own feature branch
   off `main`. They touch different source directories, so no merge conflicts.

5. **B+C as a pipeline** — Rule extraction and post-processing are streaming
   tables in a new `ncpdp_rule_extraction` SDP pipeline (not notebooks),
   enabling CDF-driven incremental processing when new spec documents are added.

## After Completion

1. Pause all three scheduled tasks
2. Review each feature branch and create PRs
3. Merge in order: A → BC → D
4. Deploy bundle to confirm pipeline DAG is correct
5. Add `ncpdp_rule_extraction` pipeline to `ncpdp_parsing` job
