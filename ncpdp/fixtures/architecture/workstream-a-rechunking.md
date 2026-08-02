# Workstream A: Segment-Aware Re-Chunking

## Objective

Replace the default `ai_prep_search` chunking (178 chunks, avg 5K chars, HTML-laden)
with a custom segment-aware splitter that produces cleaner, smaller, metadata-enriched
chunks optimized for rule extraction.

## Why This Matters

The current chunks have three problems that degrade extraction accuracy:

1. **Cross-segment boundary cuts** — A chunk may start mid-way through the Insurance
   Segment and end mid-way through the Claim Segment. The LLM gets partial context
   for both and complete context for neither.

2. **HTML table noise** — ~30% of each chunk is `<table><tr><td>` markup that wastes
   embedding capacity and can confuse extraction (field boundaries are structural,
   not textual).

3. **No metadata** — Current chunks have no `segment_code` or `transaction_type`
   column, so VS queries can't filter before similarity search.

## Inputs

- Source: `ncpdp_dev.dev_matthew_giglia_rx_claims.specification_documents_parsed`
  - Column: `content` (STRING) — full parsed HTML of the Payer Sheet PDF
  - 1 row per document (currently 1 document)

- Alternative source: `specification_search_chunks` (existing 178 chunks)
  - Could re-process these instead of going back to raw parsed content
  - Pro: faster, already chunked
  - Con: boundaries already wrong, easier to re-split from raw

## Output Table: `specification_chunks_by_segment`

```sql
CREATE TABLE specification_chunks_by_segment (
    chunk_id            STRING NOT NULL,       -- Deterministic: hash(doc_source_id + segment_code + chunk_position)
    doc_source_id       STRING NOT NULL,       -- FK to specification_documents
    
    -- Segment Metadata
    segment_code        STRING NOT NULL,       -- "HD", "01", "03", "04", "07", "10", "11", etc.
    segment_name        STRING NOT NULL,       -- "Transaction Header", "Patient", "Claim", etc.
    segment_am_code     STRING NOT NULL,       -- "AM01", "AM04", "AM07", etc. (full 111-AM value)
    transaction_type    STRING NOT NULL,       -- "B1_B3" (Claim Billing/Rebill) or "B2" (Reversal) or "S1" (Service)
    
    -- Content
    chunk_position      INT NOT NULL,          -- 0-indexed within segment
    chunk_text          STRING NOT NULL,       -- Clean text (HTML stripped, tables converted)
    chunk_to_embed      STRING NOT NULL,       -- Prefixed: "NCPDP D.0 | {segment_name} ({segment_code}) | {transaction_type}:\n{chunk_text}"
    
    -- Quality
    char_count          INT,
    has_field_table     BOOLEAN,               -- True if this chunk contains a field definition table
    has_segment_questions BOOLEAN              -- True if this chunk contains segment-level questions (presence rules)
)
CLUSTER BY AUTO
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

## Algorithm

### Step 1: Identify Segment Boundaries

The Payer Sheet PDF has a consistent structure. Each segment section starts with:
```
<table><tr><th>{Segment Name} Segment Questions</th><th>Check</th>...
```

Or with a field table header:
```
<table><tr><th>Field #</th><th>{Segment Name} Segment Identification (111-AM) = "{code}"</th>...
```

**Regex patterns for boundary detection:**
```python
# Segment Questions block (contains presence rules)
SEGMENT_QUESTIONS_PATTERN = r'<table><tr><th>([^<]+)Segment Questions</th>'

# Field definition table header (contains field-level rules)
FIELD_TABLE_PATTERN = r'<table><tr><th>Field #</th><th>([^<]+)Segment Identification \(111-AM\) = "(\d+)"</th>'

# Alternative: segment name in header row
SEGMENT_HEADER_PATTERN = r'<th>([^<]+) Segment</th>'
```

### Step 2: Split at Boundaries

For each identified segment section:
1. Extract the full HTML block (from boundary to next boundary)
2. Tag with `segment_code` and `segment_name` from the header
3. Determine `transaction_type` from column headers ("Claim Billing", "Claim Reversal", "Service Billing")

### Step 3: Strip HTML and Convert Tables

For each segment section, transform HTML to clean text:

```python
def html_table_to_text(html: str) -> str:
    """
    Convert HTML tables to structured text format.
    
    Input:  <tr><td>101-A1</td><td>BIN NUMBER</td><td></td><td>M</td><td></td></tr>
    Output: 101-A1 | BIN NUMBER | Payer Usage: M
    """
    # Use BeautifulSoup or regex to extract td values
    # Format as pipe-delimited text preserving field relationships
    ...
```

Expected output format:
```
Field 101-A1 | BIN NUMBER | Payer Usage: M | Value: If more than one BIN/PCN...
Field 102-A2 | VERSION/RELEASE NUMBER | Payer Usage: M | Value: D0
Field 103-A3 | TRANSACTION CODE | Payer Usage: M | Value: B1, B3
```

### Step 4: Sub-Chunk if Needed

If a segment section exceeds 1,500 chars after HTML stripping:
- Split at field boundaries (each field definition is a natural break point)
- Keep segment metadata consistent across sub-chunks
- Maintain `chunk_position` ordering

Target: 800–1,200 chars per chunk (sweet spot for embedding precision).

### Step 5: Add Context Header for Embedding

Prepend each chunk's `chunk_to_embed` with hierarchical context:
```
NCPDP D.0 | Claim Billing | Claim Segment (AM07):
Field 407-D7 | PRODUCT/SERVICE ID | Payer Usage: M
Field 442-E7 | QUANTITY DISPENSED | Payer Usage: M
...
```

This gives the embedding model explicit signals about what segment and transaction
type the fields belong to, improving retrieval relevance.

## Implementation Location

**Pipeline:** `ncpdp_document_intelligence` (add new stage after `prep_search`)

**File:** `src/ncpdp_document_intelligence/06_chunk_by_segment.py` (new)

**Alternative:** Standalone notebook if we want to iterate independently of the
SDP pipeline. Can always be folded in later.

## Testing Strategy

1. **Unit tests** — `test_segment_splitter.py`:
   - Given a known HTML block for the Claim Segment, verify correct split
   - Given HTML with 3 tables, verify 3 chunks produced
   - Verify HTML is fully stripped from output
   - Verify context header format

2. **Integration test** — Run on actual `specification_documents_parsed` content:
   - Verify at least 12 segment sections identified (matching `claimsBilling.yml`)
   - Verify all chunks < 1,500 chars
   - Verify no empty chunks
   - Verify `segment_code` matches known codes (HD, 01, 03, 04, 07, 10, 11)

## Dependencies

- **Upstream:** `specification_documents_parsed` must be populated (run doc intelligence pipeline)
- **Downstream:** Workstream B reads from this table instead of `specification_search_chunks`

## Effort Estimate

- Development: 2–3 hours
- Testing: 1 hour
- Integration: 30 min (add to pipeline or standalone notebook)

## Genie Code Session Context

When starting a new session for this workstream:

```
I'm working on the NCPDP rule extraction system, specifically Workstream A:
Segment-Aware Re-Chunking.

Read:
- fixtures/architecture/rule-extraction-system.md (overall architecture)
- fixtures/architecture/workstream-a-rechunking.md (this workstream's spec)
- src/ncpdp_document_intelligence/ (existing pipeline code)

The goal is to create a new stage that splits the parsed PDF content by NCPDP
segment section, strips HTML, and produces metadata-enriched chunks in a new
`specification_chunks_by_segment` table.

Source data: ncpdp_dev.dev_matthew_giglia_rx_claims.specification_documents_parsed
(1 row, ~787KB HTML content).
```
