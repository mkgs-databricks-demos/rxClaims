# ncpdp-ai-search

Vector Search infrastructure for the **rxClaims** NCPDP project.

This bundle deploys a Databricks Vector Search endpoint and Delta Sync index
that enables semantic retrieval over parsed NCPDP specification documents
(payer sheets, implementation guides). Use the index to power RAG-based
Q&A about NCPDP Telecommunications Standard Version D.0 claim billing
rules, segment definitions, and field requirements.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  ncpdp bundle (deploy first)                             │
│                                                              │
│  ncpdp_document_intelligence pipeline                        │
│    └─ specification_search_chunks (source table)             │
│       • path (PK)                                            │
│       • text (chunked content, avg ~5K chars)                 │
│       • error_status                                         │
└──────────────────────────────────────────────────────────────┘
                          │
                          ▼ Delta Sync (CDF)
┌──────────────────────────────────────────────────────────────┐
│  ncpdp-ai-search bundle (this bundle)                    │
│                                                              │
│  Vector Search Endpoint (STANDARD)                           │
│    └─ Delta Sync Index                                       │
│       • Managed embeddings: databricks-gte-large-en (1024d)   │
│       • Pipeline type: TRIGGERED                             │
│       • Synced columns: error_status                         │
└──────────────────────────────────────────────────────────────┘
```

## Deploy Order

1. **`ncpdp` bundle** — creates the schema, pipelines, volumes, and the
   `specification_search_chunks` source table (with CDF + PK enabled).
2. **`ncpdp-ai-search` bundle** (this) — creates the VS endpoint and index
   that reads from that table.

## Targets

| Target | Workspace | Catalog | Schema |
| --- | --- | --- | --- |
| dev (default) | `fevm-hls-fde` | `ncpdp_dev` | `rx_claims` |
| e2_demo_fe | `e2-demo-field-eng` | `mgiglia` | `ncpdp_rx` |
| free_edition | `dbc-e5684c0a-20fa` | `prod` | `ncpdp_rx` |

## Querying the Index

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

results = w.vector_search_indexes.query_index(
    index_name="ncpdp_dev.rx_claims.specification_search_chunks_index",
    columns=["path", "text", "error_status"],
    query_text="What segments are mandatory in a B1 Claim Billing transaction?",
    num_results=5
)

for doc in results.result.data_array:
    print(f"Score: {doc[-1]:.3f} | {doc[1][:120]}...")
```

## Syncing the Index

After new specification documents are processed by the
`ncpdp_document_intelligence` pipeline:

```python
w.vector_search_indexes.sync_index(
    index_name="ncpdp_dev.rx_claims.specification_search_chunks_index"
)
```

Or via CLI:

```bash
databricks vector-search-indexes sync-index ncpdp_dev.rx_claims.specification_search_chunks_index
```

## Prerequisites

* Databricks CLI >= 1.1.0 (Vector Search index resource support)
* The `ncpdp` bundle must be deployed first for the source table to exist
* The deploying principal needs:
  * `USE CATALOG` + `USE SCHEMA` on the target catalog/schema
  * `CREATE TABLE` on the schema (for index creation)
  * `CAN_USE` on the Vector Search endpoint

## Getting Started

1. Click the **deployment rocket** in the left sidebar to open the **Deployments** panel, then click **Deploy**.
2. After deployment, trigger the initial sync via CLI or SDK (see above).

## Documentation

* [Databricks Vector Search](https://docs.databricks.com/en/generative-ai/vector-search.html)
* [Declarative Automation Bundles](https://docs.databricks.com/en/dev-tools/bundles/settings.html)
* [NCPDP Telecommunications Standard](https://www.ncpdp.org)
