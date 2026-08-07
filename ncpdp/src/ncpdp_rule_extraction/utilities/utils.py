"""Utility functions for NCPDP rule extraction pipeline.

Contains:
- Extraction prompt definition
- Rule ID generation
- JSON parsing and validation
- Bronze key derivation
- Column name normalization
"""

import hashlib
import re


# === EXTRACTION PROMPT ===

EXTRACTION_PROMPT = """
You are an NCPDP D.0 specification parser. Extract ALL validation rules from
the following specification text.

CRITICAL: In the "condition" field, ALWAYS reference the original NCPDP field
code using the format F_NNN_XX (matching the bronze data keys), not abstract
column names.
Example: Instead of "compound_code = '2'", write "F_406_D6 = '2'" because
field 406-D6 is the Compound Code.

Return a JSON array where each object has:
- rule_level: "TRANSACTION" (segment presence/absence based on values in OTHER
  segments) or "FIELD" (value/format validation for this specific field)
- segment_code: The AM segment identifier from "111-AM = XX" (e.g., "10" for
  Compound, "01" for Patient, "04" for Insurance, "07" for Claim, "11" for Pricing)
- segment_name: Human-readable segment name
- field_code: NCPDP field number for FIELD rules (e.g., "450-EF"). null for
  TRANSACTION rules.
- field_name: Human-readable field name. null for TRANSACTION rules.
- transaction_types: Array of transaction codes (e.g., ["B1", "B3"])
- rule_type: "MANDATORY" (always required) | "SITUATIONAL" (segment presence
  depends on condition) | "REQUIRED_WHEN" (field required when condition met) |
  "FORMAT" (value format constraint) | "ALLOWED_VALUES" (enumerated valid values)
- payer_usage: Payer usage code (M, R, RW, Q) or null
- condition: SQL WHERE clause referencing NCPDP field codes as F_NNN_XX. null
  if always applies.
- condition_segment: The segment code where the condition field lives (for
  cross-segment references). null if condition is null or same-segment.
- rule_text: Concise description including any payer-specific situations.
- column_name: snake_case name for silver layer (FIELD only)
- data_type: SQL type (STRING, INT, DECIMAL(p,s), DATE) or null for TRANSACTION
- allowed_values: Array of valid values if known (e.g., ["01", "03"]) or null
- format_pattern: Regex pattern for format validation or null
- max_occurrences: For repeating field groups, max count (e.g., 25) or null

Be thorough. Extract EVERY rule including implicit ones like "This Segment is
always sent" or "Maximum count of 3".

Return ONLY valid JSON array, no markdown fences.
"""


# === RULE ID GENERATION ===

def generate_rule_id(segment_code: str, field_code: str, rule_type: str,
                     condition: str, transaction_types: list) -> str:
    """Generate deterministic rule_id via MD5 hash.
    
    Components: segment_code + field_code + rule_type + condition + sorted transaction_types
    Returns first 16 hex chars of MD5.
    """
    components = [
        segment_code or "",
        field_code or "",
        rule_type or "",
        condition or "",
        "|".join(sorted(transaction_types)) if transaction_types else ""
    ]
    key = "|".join(components)
    return hashlib.md5(key.encode()).hexdigest()[:16]


# === BRONZE KEY DERIVATION ===

def field_code_to_bronze_key(field_code: str) -> str:
    """Convert NCPDP field code to bronze key format.
    
    Example: '101-A1' -> 'F_101_A1'
             '407-D7' -> 'F_407_D7'
    """
    if not field_code:
        return None
    return "F_" + field_code.replace("-", "_")


# === COLUMN NAME NORMALIZATION ===

def normalize_column_name(field_name: str) -> str:
    """Normalize field name to snake_case column name.
    
    1. Lowercase
    2. Replace spaces and special chars with underscores
    3. Strip leading/trailing underscores
    4. Collapse multiple underscores
    5. Truncate to 63 chars
    """
    if not field_name:
        return None
    name = field_name.lower()
    name = re.sub(r'[^a-z0-9]+', '_', name)
    name = name.strip('_')
    name = re.sub(r'_+', '_', name)
    return name[:63]


# === COLUMN COMMENT GENERATION ===

def generate_column_comment(field_code: str, field_name: str, payer_usage: str) -> str:
    """Generate column comment for DDL.
    
    Format: 'NCPDP Field {field_code}: {field_name}. Usage: {payer_usage}.'
    Truncated to 255 chars.
    """
    if not field_code:
        return None
    parts = [f"NCPDP Field {field_code}: {field_name or 'Unknown'}"]
    if payer_usage:
        parts.append(f"Usage: {payer_usage}")
    comment = ". ".join(parts) + "."
    return comment[:255]


# === CROSS-SEGMENT REFERENCE RESOLUTION ===

# Known field-to-segment mappings for cross-segment conditions
FIELD_SEGMENT_MAP = {
    "F_406_D6": "07",  # Compound Code in Claim Segment
    "F_308_C8": "04",  # Other Coverage Code in Insurance Segment
    "F_202_B2": "HD",  # Service Provider ID Qualifier in Header
    "F_461_EU": "07",  # Prior Auth Type Code in Claim
    "F_462_EV": "07",  # Prior Auth Number in Claim
    "F_436_DN": "07",  # Submission Clarification Code in Claim
    "F_407_D7": "07",  # Place of Service in Claim
    "F_418_DI": "07",  # Level of Service in Claim
    "F_414_DE": "07",  # Date of Service in Claim
}


def resolve_condition_segment(condition: str, rule_segment_code: str) -> str:
    """Resolve the segment where a condition field lives.
    
    Looks for F_NNN_XX patterns in the condition string and maps to segment code.
    Returns None if condition is None, or if the field lives in the same segment.
    """
    if not condition:
        return None
    
    # Find all field references in the condition
    field_refs = re.findall(r'F_\d{3}_[A-Z][A-Z0-9]+', condition)
    
    for field_ref in field_refs:
        if field_ref in FIELD_SEGMENT_MAP:
            ref_segment = FIELD_SEGMENT_MAP[field_ref]
            if ref_segment != rule_segment_code:
                return ref_segment
    
    return None
