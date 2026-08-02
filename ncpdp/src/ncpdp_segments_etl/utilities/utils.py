"""
Segment Builder utilities for the ncpdp_segments_etl pipeline.

This module provides metadata-driven silver table generation by reading
specification_rules at pipeline definition time to construct:
- Pivot SQL expressions (key-value → columnar)
- Data type casts
- @dp.expect quality rules (MANDATORY, ALLOWED_VALUES, FORMAT)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


# ─── Segment Configuration ───────────────────────────────────────────────────
# Maps segment_code (from specification_rules) → (bronze_segment_filter, silver_table_suffix)
# The bronze segment filter is what appears in claimbilling_bronze_requests.request_segment

SEGMENT_CONFIG: Dict[str, Tuple[str, str]] = {
    "00": ("S_HD", "header"),
    "01": ("S_01", "patient"),
    "02": ("S_02", "pharmacy_provider"),
    "03": ("S_03", "prescriber"),
    "04": ("S_04", "insurance"),
    "05": ("S_05", "cob"),
    "06": ("S_06", "workers_compensation"),
    "07": ("S_07", "claim"),
    "08": ("S_08", "dur_pps"),
    "09": ("S_09", "coupon"),
    "10": ("S_10", "compound"),
    "11": ("S_11", "pricing"),
    "13": ("S_13", "clinical"),
    "15": ("S_15", "facility"),
    "16": ("S_16", "narrative"),
}

# Default table properties for silver tables
SILVER_TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableDeletionVectors": "true",
    "delta.enableRowTracking": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
    "quality": "silver",
}


# ─── Data Classes ────────────────────────────────────────────────────────────

@dataclass
class FieldDefinition:
    """A single field definition extracted from specification_rules."""
    bronze_key: str
    column_name: str
    data_type: str
    rule_type: str
    allowed_values: Optional[List[str]] = None
    format_pattern: Optional[str] = None
    column_comment: Optional[str] = None


@dataclass
class SegmentDefinition:
    """Complete definition for one silver segment table."""
    segment_code: str
    bronze_filter: str
    table_suffix: str
    fields: List[FieldDefinition] = field(default_factory=list)

    @property
    def table_name(self) -> str:
        return f"claimbilling_silver_{self.table_suffix}"


# ─── Segment Builder ─────────────────────────────────────────────────────────

class SegmentBuilder:
    """
    Reads specification_rules at pipeline definition time and builds
    transformation metadata for each segment.
    
    Usage:
        builder = SegmentBuilder(spark, catalog, schema)
        segments = builder.build_all_segments()
        for seg in segments:
            pivot_sql = builder.build_pivot_sql(seg)
            expectations = builder.build_expectations(seg)
    """

    def __init__(self, spark, catalog: str, schema: str):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema
        self._rules_df = None
        self._bronze_keys_by_segment: Dict[str, Set[str]] = {}

    @property
    def rules_table(self) -> str:
        return f"{self.catalog}.{self.schema}.specification_rules"

    @property
    def bronze_requests_table(self) -> str:
        return f"{self.catalog}.{self.schema}.claimbilling_bronze_requests"

    def _load_rules(self):
        """Load specification_rules once (batch read at definition time)."""
        if self._rules_df is None:
            self._rules_df = (
                self.spark.read.table(self.rules_table)
                .filter("segment_code IS NOT NULL AND rule_level = 'FIELD' AND bronze_key IS NOT NULL")
                .select(
                    "segment_code", "bronze_key", "column_name",
                    "data_type", "rule_type", "allowed_values",
                    "format_pattern", "column_comment"
                )
                .collect()
            )
        return self._rules_df

    def _load_bronze_keys(self):
        """Load actual bronze keys per segment to cross-validate rules."""
        if not self._bronze_keys_by_segment:
            rows = (
                self.spark.read.table(self.bronze_requests_table)
                .filter("key IS NOT NULL")
                .select("request_segment", "key")
                .distinct()
                .collect()
            )
            for row in rows:
                seg = row["request_segment"]
                key = row["key"]
                if seg not in self._bronze_keys_by_segment:
                    self._bronze_keys_by_segment[seg] = set()
                self._bronze_keys_by_segment[seg].add(key)
        return self._bronze_keys_by_segment

    def build_all_segments(self) -> List[SegmentDefinition]:
        """
        Build SegmentDefinitions for all configured segments that have both
        rules and bronze data.
        """
        rules = self._load_rules()
        bronze_keys = self._load_bronze_keys()
        segments = []

        for seg_code, (bronze_filter, table_suffix) in SEGMENT_CONFIG.items():
            # Get actual keys present in this bronze segment
            actual_keys = bronze_keys.get(bronze_filter, set())
            if not actual_keys:
                continue  # Skip segments with no bronze data

            # Collect rules for this segment, cross-validated against bronze keys
            seg_rules = [
                r for r in rules
                if r["segment_code"] == seg_code and r["bronze_key"] in actual_keys
            ]
            if not seg_rules:
                continue  # Skip segments with no matching rules

            # Deduplicate by bronze_key → take first occurrence per key
            seen_keys: Set[str] = set()
            fields: List[FieldDefinition] = []
            for r in seg_rules:
                bk = r["bronze_key"]
                if bk in seen_keys:
                    continue
                seen_keys.add(bk)
                fields.append(FieldDefinition(
                    bronze_key=bk,
                    column_name=r["column_name"],
                    data_type=r["data_type"] or "STRING",
                    rule_type=r["rule_type"] or "SITUATIONAL",
                    allowed_values=r["allowed_values"],
                    format_pattern=r["format_pattern"],
                    column_comment=r["column_comment"],
                ))

            if fields:
                segments.append(SegmentDefinition(
                    segment_code=seg_code,
                    bronze_filter=bronze_filter,
                    table_suffix=table_suffix,
                    fields=fields,
                ))

        return segments

    def build_pivot_sql(self, segment: SegmentDefinition) -> str:
        """
        Build the pivot SQL for a segment.
        
        Returns a complete SQL SELECT statement that:
        - Filters bronze_requests by segment
        - Pivots key-value rows into typed columns
        - Groups by (transaction_file_source_id, request_pos)
        """
        select_parts = [
            "transaction_file_source_id",
            "request_pos",
        ]

        for f in segment.fields:
            cast_type = _normalize_data_type(f.data_type)
            col_expr = (
                f"MAX(CASE WHEN key = '{f.bronze_key}' "
                f"THEN CAST(value::STRING AS {cast_type}) END) "
                f"AS `{f.column_name}`"
            )
            select_parts.append(col_expr)

        select_clause = ",\n    ".join(select_parts)
        sql = (
            f"SELECT\n    {select_clause}\n"
            f"FROM {self.bronze_requests_table}\n"
            f"WHERE request_segment = '{segment.bronze_filter}' AND key IS NOT NULL\n"
            f"GROUP BY transaction_file_source_id, request_pos"
        )
        return sql

    def build_expectations(self, segment: SegmentDefinition) -> Dict[str, str]:
        """
        Build @dp.expect rules from specification_rules metadata.
        
        Generates expectations for:
        - MANDATORY fields: column IS NOT NULL
        - ALLOWED_VALUES: column IN (...)
        - FORMAT patterns: column RLIKE '...'
        """
        expectations: Dict[str, str] = {}

        for f in segment.fields:
            col = f"`{f.column_name}`"

            # MANDATORY → NOT NULL check
            if f.rule_type == "MANDATORY":
                exp_name = f"{f.column_name}_not_null"
                expectations[exp_name] = f"{col} IS NOT NULL"

            # ALLOWED_VALUES → IN check (only when non-null)
            if f.allowed_values and len(f.allowed_values) > 0:
                vals = ", ".join(f"'{v}'" for v in f.allowed_values)
                exp_name = f"{f.column_name}_valid_values"
                expectations[exp_name] = f"{col} IS NULL OR {col} IN ({vals})"

            # FORMAT → RLIKE check (only when non-null)
            if f.format_pattern:
                # Escape single quotes in regex
                pattern = f.format_pattern.replace("'", "\\'")
                exp_name = f"{f.column_name}_format"
                expectations[exp_name] = f"{col} IS NULL OR {col} RLIKE '{pattern}'"

        return expectations

    def get_all_expectations_count(self, segments: List[SegmentDefinition]) -> int:
        """Count total expectations across all segments."""
        return sum(len(self.build_expectations(seg)) for seg in segments)


# ─── Helper Functions ────────────────────────────────────────────────────────

def _normalize_data_type(data_type: str) -> str:
    """
    Normalize data_type values from specification_rules to valid Spark SQL types.
    
    Handles variations like:
    - 'INT' → 'INT'
    - 'DECIMAL(10,3)' → 'DECIMAL(10,3)'
    - 'DATE' → 'DATE'
    - 'STRING' → 'STRING'
    - None or empty → 'STRING'
    """
    if not data_type:
        return "STRING"

    dt = data_type.strip().upper()

    # Direct pass-through for known types
    if dt in ("STRING", "INT", "INTEGER", "BIGINT", "LONG", "DATE", "TIMESTAMP",
              "BOOLEAN", "FLOAT", "DOUBLE"):
        return dt

    # DECIMAL with precision/scale
    if dt.startswith("DECIMAL"):
        return dt

    # Fallback
    return "STRING"
