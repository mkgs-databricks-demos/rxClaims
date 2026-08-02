# Session: Orchestration Planning for Rule Extraction System

**Date:** 2026-08-03
**Branch:** `mg-genie-orchestration-scaffold`

## Summary

Designed and scaffolded the orchestration system for executing the four-workstream
Rule Extraction System using Genie Code Scheduled Tasks with inter-session coordination.

## Problems Addressed

1. **How to execute 4 dependent workstreams autonomously** — A → B+C → D dependency chain
2. **Where B and C should live** — notebooks vs pipeline stages
3. **How sessions communicate completion** — need async handoff protocol
4. **How to avoid perpetual recurring tasks** — tasks should self-terminate

## Decisions Made

| Decision | Choice | Rationale |
| --- | --- | --- |
| B+C implementation | New SDP pipeline (`ncpdp_rule_extraction`) | CDF-driven incremental processing; ai_query in streaming table handles retries; post-processing as materialized view is declarative |
| Orchestration model | All tasks created upfront, 15-min polling | On-demand execution; no human intervention between stages; self-terminating |
| Communication protocol | `fixtures/handoffs/` status files with YAML frontmatter | Machine-parseable gate checks; human-readable notes; inspired by lakeLoom `hi_genie/hey_isaac` |
| Gate check method | Table existence + row count (SQL), not code merge | Decouples execution from PR review; workstreams operate on independent branches |
| Pipeline separation | Doc intelligence separate from rule extraction | Different concerns, cadences, failure modes; can re-extract without re-parsing PDFs |
| Collapse B+C into one task | Single scheduled task creates both stages | Same pipeline, sequential tables; simpler than 4 separate tasks |

## Key Architectural Insight

The lakeLoom model uses markdown files for human-agent async communication.
Here, all participants are Genie Code agents, so the protocol becomes a lightweight
state machine: YAML frontmatter is the machine-readable contract, prose below is
context for the downstream agent's planning.

## Files Created

| File | Purpose |
| --- | --- |
| `fixtures/handoffs/README.md` | Protocol documentation |
| `fixtures/handoffs/workstream-a-status.md` | WS-A gate file (NOT_STARTED) |
| `fixtures/handoffs/workstream-bc-status.md` | WS-BC gate file (NOT_STARTED) |
| `fixtures/handoffs/workstream-d-status.md` | WS-D gate file (NOT_STARTED) |
| `fixtures/architecture/scheduled-tasks/README.md` | Orchestration overview |
| `fixtures/architecture/scheduled-tasks/ws-a-rechunking-prompt.md` | Full prompt for WS-A task |
| `fixtures/architecture/scheduled-tasks/ws-bc-rule-extraction-prompt.md` | Full prompt for WS-BC task |
| `fixtures/architecture/scheduled-tasks/ws-d-silver-codegen-prompt.md` | Full prompt for WS-D task |

## Files Modified

| File | Change |
| --- | --- |
| `PROJECT_MEMORY.md` | Added orchestration section above the prototype notes |
| `../../README.md` (root) | Complete rewrite with architecture diagram, pipeline DAG, orchestration overview |

## Also This Session (prior commits)

- Fixed `segments.py` syntax bug (line 8 mismatched quotes) on branch `mg-genie-fix-segments-syntax`
- Updated PROJECT_MEMORY to mark it fixed with note about future architecture TBD

## Next Steps

1. Review and merge this scaffold branch
2. Create all three scheduled tasks via `scheduleAgentTool` (prompts are ready)
3. Monitor `fixtures/handoffs/` for completion signals
4. Review each workstream's feature branch and merge in order: A → BC → D
5. Deploy bundle, verify full pipeline DAG
6. Build semantic layer (dashboards, Genie spaces) on silver tables
