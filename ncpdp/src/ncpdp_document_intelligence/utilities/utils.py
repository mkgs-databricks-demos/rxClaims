"""Document Intelligence Pipeline Utilities

Metadata-driven class implementing a fully streaming document intelligence
pipeline using the modern Spark Declarative Pipelines API and v2 AI functions:
  - ai_parse_document v2: Structured VARIANT extraction from binary documents
  - ai_classify v2: Document type classification with label descriptions
  - ai_extract v2: Structured field extraction with typed JSON schemas
  - ai_prep_search: Semantic chunking for vector search / RAG indexing

Testable pure functions are defined at module level (no Spark dependency).
The DocumentIntelligence class orchestrates these into SDP streaming tables.
"""
import hashlib
import json
import re
from typing import Dict, List, Optional

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lower,
    regexp_extract,
    udf,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# ═══════════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════════

default_table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "delta.feature.variantType-preview": "supported",
    "delta.enableVariantShredding": "true",
}

# Supported binary file extensions for ai_parse_document
SUPPORTED_EXTENSIONS = [".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".doc", ".docx", ".ppt", ".pptx"]

# NCPDP specification document classification labels with descriptions
DOCUMENT_CLASSIFICATION_LABELS = """{
    "payer_sheet": "Payer-specific requirement templates that define segment usage, field requirements, and submission rules for pharmacy claims",
    "implementation_guide": "Technical implementation guides describing transaction formats, segment structures, data elements, and processing rules",
    "value_set": "Reference tables mapping codes to descriptions including rejection codes, qualifier codes, and field identifiers",
    "change_notice": "Release notes, errata, or change notifications describing updates to NCPDP standards",
    "general_reference": "General reference material, overviews, or introductory documents about NCPDP standards"
}"""

# NCPDP specification extraction schema (ai_extract user-schema format)
DOCUMENT_EXTRACTION_SCHEMA = """{
    "document_title": {"type": "string", "description": "Full title of the NCPDP specification document"},
    "version": {"type": "string", "description": "Version or release identifier of the specification"},
    "effective_date": {"type": "string", "description": "Effective or publication date in YYYY-MM-DD format"},
    "transaction_types": {
        "type": "array",
        "description": "List of NCPDP transaction types covered in this document",
        "items": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Transaction type name (e.g. Billing, Rebill, Reversal, Eligibility)"},
                "code": {"type": "string", "description": "Transaction code if specified (e.g. B1, B2, B3)"}
            }
        }
    },
    "segments": {
        "type": "array",
        "description": "NCPDP segments defined or referenced in this document",
        "items": {
            "type": "object",
            "properties": {
                "segment_id": {"type": "string", "description": "Segment identifier code (e.g. AM01, AM04)"},
                "segment_name": {"type": "string", "description": "Human-readable segment name"},
                "status": {"type": "enum", "labels": ["Mandatory", "Situational", "Not Used", "Optional"], "description": "Whether the segment is mandatory, situational, or not used"},
                "usage_notes": {"type": "string", "description": "Payer-specific rules or conditions for when this segment is required"}
            }
        }
    },
    "fields": {
        "type": "array",
        "description": "Data element / field definitions found in this document",
        "items": {
            "type": "object",
            "properties": {
                "field_id": {"type": "string", "description": "NCPDP field number and qualifier (e.g. 401-D1, 302-C2)"},
                "field_name": {"type": "string", "description": "Field name or description"},
                "usage": {"type": "enum", "labels": ["M", "R", "O", "N", "RW", "S"], "description": "Usage code: M=Mandatory, R=Required, O=Optional, N=Not Used, RW=Required When, S=Situational"},
                "format": {"type": "string", "description": "Data format, length, or type specification"},
                "values": {"type": "string", "description": "Allowed values or value set reference"}
            }
        }
    }
}"""

EXTRACTION_INSTRUCTIONS = (
    "These are NCPDP pharmacy claims standard specification documents. "
    "Extract all segments, fields, transaction types, and metadata. "
    "For payer sheets, focus on segment status (Mandatory/Situational) and field usage codes. "
    "For implementation guides, capture field definitions with format and allowed values. "
    "Return null for fields not present in the document."
)


# ═══════════════════════════════════════════════════════════════════════════════
# Testable Pure Functions (no Spark dependency)
# ═══════════════════════════════════════════════════════════════════════════════


def resolve_volume_path(catalog: str, schema: str, volume: str, sub_path: Optional[str] = None) -> str:
    """Resolve a fully qualified UC Volume path.

    Args:
        catalog: Unity Catalog name.
        schema: Schema name.
        volume: Volume name.
        sub_path: Optional sub-directory (no leading/trailing slash).

    Returns:
        Fully qualified volume path string.
    """
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    if sub_path:
        return f"{base}/{sub_path}"
    return base


def resolve_image_output_path(
    catalog: str, schema: str, volume: str, image_sub_path: Optional[str] = None
) -> str:
    """Resolve the image output path for ai_parse_document.

    Falls back to a 'parsed_images' subdirectory if no sub_path is provided.
    """
    base = f"/Volumes/{catalog}/{schema}/{volume}"
    if image_sub_path:
        return f"{base}/{image_sub_path}"
    return f"{base}/parsed_images"


def is_supported_extension(file_path: str) -> bool:
    """Check if a file path has a supported extension for ai_parse_document.

    Uses the same regex as the pipeline filter for consistent behavior.

    Args:
        file_path: Full path or filename to check.

    Returns:
        True if the file extension is in SUPPORTED_EXTENSIONS.
    """
    match = re.search(r"(\.[^.]+)$", file_path)
    if not match:
        return False
    return match.group(1).lower() in SUPPORTED_EXTENSIONS


def get_table_name(catalog: str, schema: str, table_suffix: str) -> str:
    """Build a fully qualified table name.

    Args:
        catalog: Unity Catalog name.
        schema: Schema name.
        table_suffix: Table name suffix (e.g. 'specification_documents_parsed').

    Returns:
        Fully qualified table name: catalog.schema.table_suffix
    """
    return f"{catalog}.{schema}.{table_suffix}"


def get_table_properties(base_properties: dict, quality: str) -> dict:
    """Create a copy of table properties with the quality tag set.

    Args:
        base_properties: Base Delta table properties dict.
        quality: Quality level ('bronze', 'silver', 'gold').

    Returns:
        New dict with quality set.
    """
    props = base_properties.copy()
    props["quality"] = quality
    return props


def validate_classification_labels(labels_json: str) -> dict:
    """Parse and validate the classification labels JSON string.

    Args:
        labels_json: JSON string of label-to-description mappings.

    Returns:
        Parsed dict of labels.

    Raises:
        ValueError: If JSON is invalid or has fewer than 2 labels.
    """
    try:
        labels = json.loads(labels_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Classification labels must be valid JSON: {e}") from e

    if not isinstance(labels, dict):
        raise ValueError("Classification labels must be a JSON object (dict).")
    if len(labels) < 2:
        raise ValueError(f"ai_classify requires at least 2 labels, got {len(labels)}.")
    if len(labels) > 500:
        raise ValueError(f"ai_classify supports at most 500 labels, got {len(labels)}.")

    for name, desc in labels.items():
        if not isinstance(name, str) or not (1 <= len(name) <= 100):
            raise ValueError(f"Label name must be 1-100 chars, got: '{name}'")
        if not isinstance(desc, str) or len(desc) > 1000:
            raise ValueError(f"Label description must be <=1000 chars for '{name}'.")

    return labels


def validate_extraction_schema(schema_json: str) -> dict:
    """Parse and validate the extraction schema JSON string.

    Validates structure against ai_extract user-schema format rules:
    - Allowed types: string, integer, number, boolean, enum, array, object
    - No SQL DDL types, no JSON-Schema keywords (required, anyOf, etc.)
    - Max 256 total properties, max depth 12

    Args:
        schema_json: JSON string in ai_extract user-schema format.

    Returns:
        Parsed schema dict.

    Raises:
        ValueError: If JSON is invalid or violates schema rules.
    """
    ALLOWED_TYPES = {"string", "integer", "number", "boolean", "enum", "array", "object"}
    ALLOWED_KEYWORDS = {"type", "description", "items", "properties", "labels"}
    FORBIDDEN_KEYWORDS = {
        "required", "anyOf", "oneOf", "allOf", "not", "$ref", "$schema",
        "$defs", "title", "additionalProperties", "patternProperties",
        "minLength", "maxLength", "pattern", "minimum", "maximum",
        "exclusiveMinimum", "exclusiveMaximum", "multipleOf",
        "minItems", "maxItems", "format", "const",
    }

    try:
        schema = json.loads(schema_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Extraction schema must be valid JSON: {e}") from e

    if not isinstance(schema, dict):
        raise ValueError("Extraction schema must be a JSON object.")

    property_count = 0

    def _validate_field(field: dict, path: str, depth: int):
        nonlocal property_count

        if depth > 12:
            raise ValueError(f"Schema exceeds max depth of 12 at '{path}'.")

        # Check for forbidden keywords
        for key in field:
            if key in FORBIDDEN_KEYWORDS:
                raise ValueError(f"Unsupported keyword '{key}' at '{path}'.")
            if key not in ALLOWED_KEYWORDS:
                raise ValueError(f"Unknown keyword '{key}' at '{path}'. Allowed: {ALLOWED_KEYWORDS}")

        field_type = field.get("type")
        if field_type and field_type not in ALLOWED_TYPES:
            raise ValueError(
                f"Unsupported type '{field_type}' at '{path}'. "
                f"Allowed: {ALLOWED_TYPES}"
            )

        if field_type == "enum":
            if "labels" not in field or not field["labels"]:
                raise ValueError(f"Enum type at '{path}' requires non-empty 'labels'.")

        if field_type == "array":
            if "items" not in field:
                raise ValueError(f"Array type at '{path}' requires 'items'.")
            _validate_field(field["items"], f"{path}.items", depth + 1)

        if field_type == "object":
            if "properties" not in field or not field["properties"]:
                raise ValueError(f"Object type at '{path}' requires non-empty 'properties'.")
            for prop_name, prop_def in field["properties"].items():
                property_count += 1
                _validate_field(prop_def, f"{path}.{prop_name}", depth + 1)

    for prop_name, prop_def in schema.items():
        property_count += 1
        _validate_field(prop_def, prop_name, 1)

    if property_count > 256:
        raise ValueError(f"Schema has {property_count} properties; max is 256.")

    return schema


def build_parse_document_expr(image_output_path: str) -> str:
    """Build the SQL expression for ai_parse_document v2.

    Args:
        image_output_path: UC Volume path for page image output.

    Returns:
        SQL expression string for use in selectExpr.
    """
    path_sql = image_output_path.replace("'", "\\'")
    return f"""ai_parse_document(
        content,
        MAP(
            'version', '2.0',
            'imageOutputPath', '{path_sql}',
            'descriptionElementTypes', '*'
        )
    ) as parsed"""


def build_classify_expr(labels: str, instructions: str) -> str:
    """Build the SQL expression for ai_classify v2.

    Args:
        labels: JSON string of label definitions.
        instructions: Global classification instructions.

    Returns:
        SQL expression string for use in selectExpr.
    """
    labels_sql = labels.replace("'", "\\'")
    instructions_sql = instructions.replace("'", "\\'")
    return f"""ai_classify(
        parsed,
        '{labels_sql}',
        MAP(
            'version', '2.0',
            'instructions', '{instructions_sql}'
        )
    ) as classification"""


def build_extract_expr(schema: str, instructions: str) -> str:
    """Build the SQL expression for ai_extract v2.

    Args:
        schema: JSON string of the extraction schema.
        instructions: Global extraction instructions.

    Returns:
        SQL expression string for use in selectExpr.
    """
    schema_sql = schema.replace("'", "\\'")
    instructions_sql = instructions.replace("'", "\\'")
    return f"""ai_extract(
        parsed,
        '{schema_sql}',
        MAP(
            'version', '2.0',
            'instructions', '{instructions_sql}'
        )
    ) as extracted"""


def build_prep_search_explode_expr() -> tuple[list, list]:
    """Build the SQL expressions for ai_prep_search with LATERAL explode.

    Returns:
        Tuple of (first selectExpr list, second selectExpr list) for the
        two-stage selectExpr pattern.
    """
    stage_1 = [
        "doc_source_id",
        "path",
        "ai_prep_search(parsed) as prep_result",
    ]
    stage_2 = [
        "doc_source_id",
        "path",
        "inline(from_json(to_json(prep_result:document:contents), "
        "'ARRAY<STRUCT<chunk_id: STRING, chunk_position: INT, "
        "chunk_to_retrieve: STRING, chunk_to_embed: STRING>>'))",
    ]
    return stage_1, stage_2


# ═══════════════════════════════════════════════════════════════════════════════
# Segment-Aware Re-Chunking Functions
# ═══════════════════════════════════════════════════════════════════════════════

# NCPDP D.0 segment code to name mapping
NCPDP_SEGMENT_MAP = {
    "HD": "Transaction Header",
    "01": "Patient",
    "02": "Pharmacy Provider",
    "03": "Prescriber",
    "04": "Insurance",
    "05": "Coordination of Benefits/Other Payments",
    "06": "Workers' Compensation",
    "07": "Claim",
    "08": "DUR/PPS",
    "09": "Coupon",
    "10": "Compound",
    "11": "Pricing",
    "13": "Clinical",
    "14": "Additional Documentation",
    "15": "Facility",
    "16": "Narrative",
    "20": "Response Message",
    "21": "Response Status",
    "22": "Response Claim",
    "23": "Response Pricing",
    "24": "Response DUR/PPS",
    "25": "Response Insurance",
    "26": "Response Prior Authorization",
    "28": "Response Coordination of Benefits/Other Payers",
    "29": "Response Patient",
}

_NCPDP_NAME_TO_CODE = {v.lower(): k for k, v in NCPDP_SEGMENT_MAP.items()}
_NCPDP_NAME_TO_CODE["response transaction header"] = "HD"

CHUNK_MAX_SIZE = 1500
CHUNK_TARGET_SIZE = 1000

_RE_FIELD_TABLE_WITH_AM = re.compile(
    r'<t[hd]>Field #</t[hd]><t[hd]>([^<]*?)Segment[^<]*?'
    r'Identification\s*\(111-AM\)\s*=\s*"(\d+)"</t[hd]>'
)
_RE_TX_HEADER_TABLE = re.compile(
    r'<t[hd]>Field #</t[hd]><t[hd]>Transaction Header Segment</t[hd]>'
)
_RE_SEGMENT_QUESTIONS = re.compile(
    r'<table><tr><th>([^<]+?)Segment Questions</th><th>Check</th><th>([^<]*)</th>'
)
_RE_TX_TYPE_IN_HEADER = re.compile(
    r'<th>([^<]*(?:Claim Billing|Claim Reversal|Service|Eligibility|Predetermination)[^<]*)</th>'
)


def resolve_segment_code(segment_name: str) -> str:
    """Resolve a segment display name to its NCPDP AM code."""
    name_lower = segment_name.lower().strip()
    if name_lower in _NCPDP_NAME_TO_CODE:
        return _NCPDP_NAME_TO_CODE[name_lower]
    for key, code in _NCPDP_NAME_TO_CODE.items():
        if key in name_lower or name_lower in key:
            return code
    return "XX"


def detect_transaction_type(header_text: str) -> str:
    """Determine NCPDP transaction type from column header text."""
    t = header_text.lower()
    if "claim reversal" in t:
        return "B2"
    elif "service billing" in t or "service rebill" in t:
        return "S1"
    elif "eligibility" in t:
        return "E1"
    elif "predetermination" in t:
        return "D1"
    return "B1_B3"


def strip_html_to_text(html: str) -> str:
    """Convert HTML content to clean structured text.

    Field rows: 'Field {code} | {name} | Payer Usage: {usage}'
    Other rows: pipe-separated values. All tags removed.
    """
    result_lines = []
    rows = re.findall(r'<tr>(.*?)</tr>', html, re.DOTALL)

    text_outside = re.sub(r'<tr>.*?</tr>', '\n', html, flags=re.DOTALL)
    text_outside = re.sub(r'<table>|</table>', '', text_outside)
    text_outside = re.sub(r'<[^>]+>', '', text_outside)
    for line in text_outside.split('\n'):
        line = line.strip()
        if line and len(line) > 3:
            result_lines.append(line)

    for row_html in rows:
        cells = re.findall(r'<t[hd][^>]*>(.*?)</t[hd]>', row_html, re.DOTALL)
        clean_cells = []
        for cell in cells:
            cell_text = re.sub(r'<br\s*/?>', ' ', cell)
            cell_text = re.sub(r'<i>([^<]*)</i>', r'\1', cell_text)
            cell_text = re.sub(r'<[^>]+>', '', cell_text)
            cell_text = re.sub(r'\s+', ' ', cell_text).strip()
            clean_cells.append(cell_text)

        if not any(c for c in clean_cells):
            continue
        first = clean_cells[0] if clean_cells else ""
        if first == "Field #" or first == "NCPDP Field Name":
            continue
        if first == "" and len(clean_cells) > 1 and clean_cells[1] == "NCPDP Field Name":
            continue

        if len(clean_cells) >= 2 and re.match(r'\d{3}-[A-Z]\w*', first):
            field_code = clean_cells[0]
            field_name = clean_cells[1]
            value = clean_cells[2] if len(clean_cells) > 2 else ""
            usage = clean_cells[3] if len(clean_cells) > 3 else ""
            situation = clean_cells[4] if len(clean_cells) > 4 else ""
            line = f"Field {field_code} | {field_name}"
            if usage:
                line += f" | Payer Usage: {usage}"
            if value:
                line += f" | Value: {value}"
            if situation and len(situation) > 2:
                line += f" | {situation}"
            result_lines.append(line)
        else:
            joined = " | ".join(c for c in clean_cells if c)
            if joined:
                result_lines.append(joined)

    return "\n".join(result_lines)


def sub_chunk_text(text: str, max_size: int = CHUNK_MAX_SIZE, target_size: int = CHUNK_TARGET_SIZE) -> list:
    """Split text into sub-chunks at line/sentence boundaries."""
    if len(text) <= max_size:
        return [text]

    lines = text.split('\n')
    expanded_lines = []
    for line in lines:
        if len(line) > max_size:
            sentences = re.split(r'(?<=[.!?])\s+', line)
            expanded_lines.extend(s for s in sentences if s.strip())
        else:
            expanded_lines.append(line)

    chunks = []
    current_chunk = []
    current_len = 0
    for line in expanded_lines:
        line_len = len(line) + 1
        if current_len + line_len > target_size and current_chunk:
            chunks.append('\n'.join(current_chunk))
            current_chunk = [line]
            current_len = line_len
        else:
            current_chunk.append(line)
            current_len += line_len
    if current_chunk:
        chunks.append('\n'.join(current_chunk))

    final_chunks = []
    for chunk in chunks:
        if len(chunk) <= max_size:
            final_chunks.append(chunk)
        else:
            for i in range(0, len(chunk), max_size):
                final_chunks.append(chunk[i:i + max_size])
    return final_chunks


def generate_chunk_id(doc_source_id: str, segment_code: str, chunk_position: int) -> str:
    """Generate a deterministic chunk ID (16-char hex)."""
    key = f"{doc_source_id}|{segment_code}|{chunk_position}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def segment_document_to_chunks(full_html: str, doc_source_id: str) -> list:
    """Split a full NCPDP Payer Sheet HTML into segment-aware chunks."""
    boundaries = []

    for m in _RE_FIELD_TABLE_WITH_AM.finditer(full_html):
        boundaries.append({
            'pos': m.start(),
            'segment_name': m.group(1).strip(),
            'segment_code': m.group(2),
            'type': 'field_table',
        })
    for m in _RE_TX_HEADER_TABLE.finditer(full_html):
        boundaries.append({
            'pos': m.start(),
            'segment_name': 'Transaction Header',
            'segment_code': 'HD',
            'type': 'field_table',
        })
    for m in _RE_SEGMENT_QUESTIONS.finditer(full_html):
        seg_name = m.group(1).strip()
        code = resolve_segment_code(seg_name)
        boundaries.append({
            'pos': m.start(),
            'segment_name': seg_name,
            'segment_code': code,
            'type': 'segment_questions',
            'tx_context': m.group(2).strip(),
        })

    boundaries.sort(key=lambda x: x['pos'])
    if not boundaries:
        return []

    chunks = []
    for i, boundary in enumerate(boundaries):
        end_pos = boundaries[i + 1]['pos'] if i + 1 < len(boundaries) else len(full_html)
        section_html = full_html[boundary['pos']:end_pos]

        if 'tx_context' in boundary:
            tx_type = detect_transaction_type(boundary['tx_context'])
        else:
            tx_matches = _RE_TX_TYPE_IN_HEADER.findall(section_html)
            if tx_matches:
                tx_type = detect_transaction_type(tx_matches[0])
            else:
                preceding = full_html[max(0, boundary['pos'] - 5000):boundary['pos']]
                tx_prev = _RE_TX_TYPE_IN_HEADER.findall(preceding)
                tx_type = detect_transaction_type(tx_prev[-1]) if tx_prev else "B1_B3"

        clean_text = strip_html_to_text(section_html)
        if len(clean_text.strip()) < 20:
            continue

        seg_code = boundary['segment_code']
        seg_name = boundary['segment_name']
        am_code = f"AM{seg_code.zfill(2)}" if seg_code not in ("HD", "XX") else seg_code
        has_field_table = boundary['type'] == 'field_table'
        has_seg_questions = boundary['type'] == 'segment_questions'

        for chunk_text in sub_chunk_text(clean_text):
            pos_idx = len(chunks)
            chunk_id = generate_chunk_id(doc_source_id, seg_code, pos_idx)
            context_header = f"NCPDP D.0 | {seg_name} ({seg_code}) | {tx_type}:"
            chunk_to_embed = f"{context_header}\n{chunk_text}"
            chunks.append({
                'chunk_id': chunk_id,
                'doc_source_id': doc_source_id,
                'segment_code': seg_code,
                'segment_name': seg_name,
                'segment_am_code': am_code,
                'transaction_type': tx_type,
                'chunk_position': pos_idx,
                'chunk_text': chunk_text,
                'chunk_to_embed': chunk_to_embed,
                'char_count': len(chunk_text),
                'has_field_table': has_field_table,
                'has_segment_questions': has_seg_questions,
            })
    return chunks


# Schema for the segment chunks UDF output
SEGMENT_CHUNK_SCHEMA = ArrayType(StructType([
    StructField("chunk_id", StringType(), False),
    StructField("doc_source_id", StringType(), False),
    StructField("segment_code", StringType(), False),
    StructField("segment_name", StringType(), False),
    StructField("segment_am_code", StringType(), False),
    StructField("transaction_type", StringType(), False),
    StructField("chunk_position", IntegerType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("chunk_to_embed", StringType(), False),
    StructField("char_count", IntegerType(), True),
    StructField("has_field_table", BooleanType(), True),
    StructField("has_segment_questions", BooleanType(), True),
]))


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline Orchestration Class
# ═══════════════════════════════════════════════════════════════════════════════


class DocumentIntelligence:
    """Fully streaming document intelligence pipeline for NCPDP specification documents.

    Implements the complete chain:
        stream_ingest → parse_documents → classify_documents → extract_fields → prep_search

    All testable logic is delegated to module-level pure functions.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog: str,
        schema: str,
        volume: str,
        volume_sub_path: str = None,
        image_output_sub_path: str = None,
        table_properties: dict = None,
    ):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self.volume = volume
        self.volume_sub_path = volume_sub_path
        self.image_output_sub_path = image_output_sub_path
        self.table_properties = get_table_properties(
            table_properties or default_table_properties, "bronze"
        )

        # Resolve volume paths using testable functions
        self.volume_path = resolve_volume_path(catalog, schema, volume, volume_sub_path)
        self.image_output_path = resolve_image_output_path(
            catalog, schema, volume, image_output_sub_path
        )

    def __repr__(self):
        return (
            f"DocumentIntelligence(catalog='{self.catalog}', schema='{self.schema}', "
            f"volume='{self.volume}', volume_sub_path='{self.volume_sub_path}')"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Layer 1: Bronze — Stream raw binary documents from UC Volume
    # ─────────────────────────────────────────────────────────────────────────

    def stream_ingest(self):
        """Auto Loader ingestion of binary documents into a streaming table."""

        schema_definition = """
            doc_source_id STRING NOT NULL PRIMARY KEY COMMENT 'SHA-256 hash of file metadata as unique identifier.',
            file_metadata STRUCT <
                file_path: STRING,
                file_name: STRING,
                file_size: BIGINT,
                file_block_start: BIGINT,
                file_block_length: BIGINT,
                file_modification_time: TIMESTAMP
            > NOT NULL COMMENT 'Metadata about the ingested file.',
            ingest_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() COMMENT 'Timestamp when the file was ingested.',
            path STRING COMMENT 'Full path to the source file.',
            modificationTime TIMESTAMP COMMENT 'File modification timestamp.',
            length LONG COMMENT 'File size in bytes.',
            content BINARY COMMENT 'Raw binary content of the document.'
        """

        table_name = get_table_name(self.catalog, self.schema, "specification_documents")

        @dp.table(
            name=table_name,
            comment="Streaming bronze ingestion of NCPDP specification documents as binary content.",
            table_properties=self.table_properties,
            cluster_by=["path"],
            cluster_by_auto=True,
            schema=schema_definition,
            temporary=False,
        )
        def ingest():
            return (
                self.spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "binaryFile")
                .option("cloudFiles.allowOverwrites", "true")
                .load(self.volume_path)
                .selectExpr(
                    "sha2(concat(_metadata.*), 256) as doc_source_id",
                    "_metadata as file_metadata",
                    "*",
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Layer 2: Parsed — ai_parse_document v2 with image output + descriptions
    # ─────────────────────────────────────────────────────────────────────────

    def parse_documents(self):
        """Parse binary documents into structured VARIANT using ai_parse_document v2."""

        silver_props = get_table_properties(self.table_properties, "silver")
        table_name = get_table_name(self.catalog, self.schema, "specification_documents_parsed")
        source_table = get_table_name(self.catalog, self.schema, "specification_documents")
        parse_expr = build_parse_document_expr(self.image_output_path)

        @dp.table(
            name=table_name,
            comment="Parsed NCPDP specification documents using ai_parse_document v2. Contains structured VARIANT with pages, elements, and figure descriptions.",
            table_properties=silver_props,
            cluster_by_auto=True,
            temporary=False,
        )
        def parse():
            return (
                self.spark.readStream
                .table(source_table)
                .filter(
                    lower(regexp_extract(col("path"), r"(\.[^.]+)$", 1)).isin(SUPPORTED_EXTENSIONS)
                )
                .selectExpr(
                    "doc_source_id",
                    "path",
                    "file_metadata",
                    parse_expr,
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Layer 3: Classified — ai_classify v2 with label descriptions
    # ─────────────────────────────────────────────────────────────────────────

    def classify_documents(self):
        """Classify documents by type using ai_classify v2 with descriptive labels."""

        silver_props = get_table_properties(self.table_properties, "silver")
        table_name = get_table_name(self.catalog, self.schema, "specification_documents_classified")
        source_table = get_table_name(self.catalog, self.schema, "specification_documents_parsed")
        classify_expr = build_classify_expr(
            DOCUMENT_CLASSIFICATION_LABELS,
            "Classify NCPDP pharmacy claims standard specification documents by their primary purpose and content type.",
        )

        @dp.table(
            name=table_name,
            comment="NCPDP specification documents classified by type using ai_classify v2.",
            table_properties=silver_props,
            cluster_by_auto=True,
            temporary=False,
        )
        def classify():
            return (
                self.spark.readStream
                .table(source_table)
                .filter("try_cast(parsed:error_status AS STRING) IS NULL")
                .selectExpr(
                    "doc_source_id",
                    "path",
                    "file_metadata",
                    "parsed",
                    classify_expr,
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Layer 4: Extracted — ai_extract v2 with typed NCPDP schema
    # ─────────────────────────────────────────────────────────────────────────

    def extract_fields(self):
        """Extract structured NCPDP fields using ai_extract v2 with typed schema."""

        silver_props = get_table_properties(self.table_properties, "silver")
        table_name = get_table_name(self.catalog, self.schema, "specification_documents_extracted")
        source_table = get_table_name(self.catalog, self.schema, "specification_documents_classified")
        extract_expr = build_extract_expr(DOCUMENT_EXTRACTION_SCHEMA, EXTRACTION_INSTRUCTIONS)

        @dp.table(
            name=table_name,
            comment="Structured extraction of NCPDP segments, fields, and metadata using ai_extract v2.",
            table_properties=silver_props,
            cluster_by_auto=True,
            temporary=False,
        )
        def extract():
            return (
                self.spark.readStream
                .table(source_table)
                .selectExpr(
                    "doc_source_id",
                    "path",
                    "file_metadata",
                    "parsed",
                    "classification",
                    extract_expr,
                )
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Layer 5: Search-Ready — ai_prep_search chunking for vector search / RAG
    # ─────────────────────────────────────────────────────────────────────────

    def prep_search(self):
        """Produce semantic chunks optimized for vector search indexing.

        Uses ai_prep_search to split parsed documents into context-enriched
        chunks with embedding-ready text and page image references.
        Output is suitable as a source for a Databricks Vector Search index.
        """

        gold_props = get_table_properties(self.table_properties, "gold")
        table_name = get_table_name(self.catalog, self.schema, "specification_search_chunks")
        source_table = get_table_name(self.catalog, self.schema, "specification_documents_parsed")
        stage_1, stage_2 = build_prep_search_explode_expr()

        @dp.table(
            name=table_name,
            comment="Semantic chunks from NCPDP specification documents, prepared for vector search indexing via ai_prep_search.",
            table_properties=gold_props,
            cluster_by_auto=True,
            temporary=False,
        )
        def chunks():
            return (
                self.spark.readStream
                .table(source_table)
                .filter("try_cast(parsed:error_status AS STRING) IS NULL")
                .selectExpr(*stage_1)
                .selectExpr(*stage_2)
            )

    # ─────────────────────────────────────────────────────────────────────────────
    # Layer 6: Segment Chunks — custom segment-aware re-chunking for rule extraction
    # ─────────────────────────────────────────────────────────────────────────────

    def chunk_by_segment(self):
        """Produce segment-aware chunks optimized for rule extraction.

        Reads the search chunks produced by prep_search(), reconstructs the
        full document per doc_source_id, splits by NCPDP segment boundaries,
        strips HTML, and writes metadata-enriched chunks suitable for
        LLM-based rule extraction.

        Uses a materialized view (batch read) because the reconstruction
        requires groupBy aggregation which is incompatible with streaming.

        Output table: specification_chunks_by_segment
        """

        gold_props = get_table_properties(self.table_properties, "gold")
        table_name = get_table_name(self.catalog, self.schema, "specification_chunks_by_segment")
        source_table = get_table_name(self.catalog, self.schema, "specification_search_chunks")

        # Define UDF for segment chunking
        @udf(returnType=SEGMENT_CHUNK_SCHEMA)
        def segment_chunk_udf(full_html: str, doc_source_id: str):
            """UDF wrapper around segment_document_to_chunks."""
            if not full_html or not doc_source_id:
                return []
            return segment_document_to_chunks(full_html, doc_source_id)

        @dp.materialized_view(
            name=table_name,
            comment=(
                "Segment-aware chunks from NCPDP specification documents. "
                "Each chunk corresponds to a single segment section with HTML stripped, "
                "metadata enriched, and context headers for embedding. "
                "Optimized for LLM rule extraction (Workstream B)."
            ),
            table_properties=gold_props,
            cluster_by_auto=True,
        )
        def segment_chunks():
            from pyspark.sql.functions import (
                array_sort,
                collect_list,
                concat_ws,
                explode,
                struct,
                transform,
            )

            # Reconstruct full document from search chunks (ordered by position)
            full_docs = (
                self.spark.read
                .table(source_table)
                .groupBy("doc_source_id")
                .agg(
                    concat_ws(
                        "\n",
                        transform(
                            array_sort(
                                collect_list(
                                    struct(col("chunk_position"), col("chunk_to_retrieve"))
                                )
                            ),
                            lambda x: x.getField("chunk_to_retrieve")
                        )
                    ).alias("full_html")
                )
            )

            # Apply segment chunking UDF and explode
            return (
                full_docs
                .withColumn("chunks", segment_chunk_udf(col("full_html"), col("doc_source_id")))
                .select(explode(col("chunks")).alias("chunk"))
                .select(
                    col("chunk.chunk_id"),
                    col("chunk.doc_source_id"),
                    col("chunk.segment_code"),
                    col("chunk.segment_name"),
                    col("chunk.segment_am_code"),
                    col("chunk.transaction_type"),
                    col("chunk.chunk_position"),
                    col("chunk.chunk_text"),
                    col("chunk.chunk_to_embed"),
                    col("chunk.char_count"),
                    col("chunk.has_field_table"),
                    col("chunk.has_segment_questions"),
                )
            )
