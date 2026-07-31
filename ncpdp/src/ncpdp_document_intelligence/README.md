# NCPDP Document Intelligence Pipeline

Fully streaming Spark Declarative Pipeline that processes NCPDP specification documents through the complete AI document intelligence chain.

## Architecture

```
UC Volume (PDFs, DOCX, images)
    │
    ▼
┌────────────────────────────────────────────────────────────┐
│  Layer 1: specification_documents (Bronze)            │
│  Auto Loader → cloudFiles binaryFile streaming        │
└────────────────────────────────────────────────────────────┘
    │
    ▼
┌────────────────────────────────────────────────────────────┐
│  Layer 2: specification_documents_parsed (Silver)     │
│  ai_parse_document v2 + image output + descriptions   │
└────────────────────────────────────────────────────────────┘
    │
    ├────────────────────────────────────────────────┐
    │                                                │
    ▼                                                ▼
┌────────────────────────────┐  ┌────────────────────────────┐
│  Layer 3: ..._classified    │  │  Layer 5: ..._chunks       │
│  ai_classify v2             │  │  ai_prep_search            │
└────────────────────────────┘  └────────────────────────────┘
    │                                │
    ▼                                ▼
┌────────────────────────────┐  Vector Search Index
│  Layer 4: ..._extracted     │
│  ai_extract v2              │
└────────────────────────────┘
```

## AI Functions Used

| Function | Version | Purpose |
| --- | --- | --- |
| `ai_parse_document` | v2 | Extract structured content from PDFs/images/Office docs |
| `ai_classify` | v2 | Classify document type with descriptive labels |
| `ai_extract` | v2 | Extract NCPDP segments, fields, transaction types |
| `ai_prep_search` | latest | Semantic chunking for vector search / RAG |

## Output Tables

| Table | Quality | Description |
| --- | --- | --- |
| `specification_documents` | Bronze | Raw binary documents streamed from volume |
| `specification_documents_parsed` | Silver | Parsed VARIANT with pages, elements, figures |
| `specification_documents_classified` | Silver | Document type classification |
| `specification_documents_extracted` | Silver | Structured segment/field extraction |
| `specification_search_chunks` | Gold | Search-ready chunks for vector indexing |

## Configuration

Pipeline configuration variables (set in pipeline resource YAML):

| Variable | Description |
| --- | --- |
| `catalog_use` | Target Unity Catalog |
| `schema_use` | Target schema |
| `volume_use` | UC Volume containing spec documents |
| `volume_sub_path_use` | Optional sub-path within volume |
| `image_output_sub_path_use` | Path for ai_parse_document image output |

## Folder Structure

```
ncpdp_document_intelligence/
├── README.md
├── transformations/
│   └── autoload.py          # Entry point: instantiates and runs pipeline
└── utilities/
    └── utils.py             # DocumentIntelligence class implementation
```
