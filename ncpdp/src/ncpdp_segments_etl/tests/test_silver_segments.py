"""
SDP-native tests for silver segment tables.

Validates:
- Pivot logic: key-value rows → typed columnar output
- Data type casting: STRING, INT, DECIMAL(10,3), DATE
- MANDATORY expectations: NOT NULL checks fire on missing fields
- ALLOWED_VALUES expectations: IN checks fire on invalid values
- FORMAT expectations: RLIKE checks fire on non-matching patterns
- Schema correctness: output columns match specification_rules
"""
import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark  # noqa: F401
from conftest import test_pipeline, fqn
from decimal import Decimal
from datetime import date


# ──────────────────────────────────────────────────────────────────────
# Claim Segment (S_07) Tests
# ──────────────────────────────────────────────────────────────────────

class TestClaimSegmentPivot:
    """Tests for claimbilling_silver_claim pivot logic and schema."""

    def test_pivot_produces_correct_row_count(self, test_spark, mock_all):
        """Each (transaction_file_source_id, request_pos) in S_07 becomes one row."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")
        pivot_sql = builder.build_pivot_sql(claim_seg)

        result = test_spark.sql(pivot_sql)
        # 4 distinct (file_id, pos) combinations have S_07 data
        assert result.count() == 4

    def test_pivot_column_values_correct(self, test_spark, mock_all):
        """Verify pivot correctly maps bronze keys to typed column values."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")
        pivot_sql = builder.build_pivot_sql(claim_seg)

        result = test_spark.sql(pivot_sql)
        row = result.filter("transaction_file_source_id = 'file_001'").collect()[0]

        # STRING field
        assert row["product_service_id"] == "12345678901"
        # DECIMAL(10,3) field
        assert row["quantity_dispensed"] == Decimal("30.000")
        # INT field
        assert row["days_supply"] == 30
        # DATE field
        assert row["date_prescription_written"] == date(2026, 1, 15)
        # ALLOWED_VALUES field
        assert row["compound_code"] == "1"

    def test_pivot_schema_has_expected_columns(self, test_spark, mock_all):
        """Output schema should include all FIELD rules for segment 07."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")
        pivot_sql = builder.build_pivot_sql(claim_seg)

        result = test_spark.sql(pivot_sql)
        columns = set(result.columns)

        # Must have grouping keys + all rule-defined columns
        assert "transaction_file_source_id" in columns
        assert "request_pos" in columns
        assert "product_service_id" in columns
        assert "quantity_dispensed" in columns
        assert "days_supply" in columns
        assert "compound_code" in columns
        assert "prescription_reference_number" in columns
        assert "date_prescription_written" in columns

    def test_missing_key_produces_null(self, test_spark, mock_all):
        """When a bronze_key is absent for a transaction, the column should be NULL."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")
        pivot_sql = builder.build_pivot_sql(claim_seg)

        result = test_spark.sql(pivot_sql)
        # file_003 is missing F_407_D7 (product_service_id)
        row = result.filter("transaction_file_source_id = 'file_003'").collect()[0]
        assert row["product_service_id"] is None


class TestClaimSegmentExpectations:
    """Tests for expectation generation logic."""

    def test_mandatory_expectation_generated(self, test_spark, mock_all):
        """MANDATORY rules should generate NOT NULL expectations."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")

        expectations = builder.build_expectations(claim_seg)

        # r001 is MANDATORY on product_service_id
        assert "product_service_id_not_null" in expectations
        assert "NOT NULL" in expectations["product_service_id_not_null"]

    def test_allowed_values_expectation_generated(self, test_spark, mock_all):
        """ALLOWED_VALUES rules should generate IN checks."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")

        expectations = builder.build_expectations(claim_seg)

        # r004 is ALLOWED_VALUES on compound_code (key uses _valid_values)
        assert "compound_code_valid_values" in expectations
        exp_sql = expectations["compound_code_valid_values"]
        assert "IS NULL OR" in exp_sql
        assert "IN (" in exp_sql
        assert "'1'" in exp_sql
        assert "'2'" in exp_sql

    def test_format_expectation_generated(self, test_spark, mock_all):
        """FORMAT rules should generate RLIKE checks."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")

        expectations = builder.build_expectations(claim_seg)

        # r005 is FORMAT on prescription_reference_number
        assert "prescription_reference_number_format" in expectations
        exp_sql = expectations["prescription_reference_number_format"]
        assert "RLIKE" in exp_sql or "rlike" in exp_sql

    def test_expectations_validate_data(self, test_spark, mock_all):
        """Expectations should correctly identify valid and invalid rows."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")
        pivot_sql = builder.build_pivot_sql(claim_seg)

        result = test_spark.sql(pivot_sql)

        # file_001: all valid
        valid_row = result.filter("transaction_file_source_id = 'file_001'").collect()[0]
        assert valid_row["product_service_id"] is not None
        assert valid_row["compound_code"] in ("1", "2")

        # file_003: missing mandatory field
        invalid_row = result.filter("transaction_file_source_id = 'file_003'").collect()[0]
        assert invalid_row["product_service_id"] is None

        # file_004: invalid allowed_values
        invalid_av = result.filter("transaction_file_source_id = 'file_004'").collect()[0]
        assert invalid_av["compound_code"] == "9"


# ──────────────────────────────────────────────────────────────────────
# Header Segment (S_HD) Tests
# ──────────────────────────────────────────────────────────────────────

class TestHeaderSegmentPivot:
    """Tests for claimbilling_silver_header."""

    def test_header_row_count(self, test_spark, mock_all):
        """Should produce one row per (file, pos) with S_HD data."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        header_seg = next(s for s in segments if s.segment_code == "00")
        pivot_sql = builder.build_pivot_sql(header_seg)

        result = test_spark.sql(pivot_sql)
        # 4 files have S_HD keys
        assert result.count() == 4

    def test_header_allowed_values(self, test_spark, mock_all):
        """
        transaction_code should be validated against ['B1','B2','B3','E1'].
        file_004 has 'XX' which is invalid.
        """
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        header_seg = next(s for s in segments if s.segment_code == "00")
        pivot_sql = builder.build_pivot_sql(header_seg)

        result = test_spark.sql(pivot_sql)

        # Valid transaction codes
        valid_count = result.filter(
            "transaction_code IN ('B1', 'B2', 'B3', 'E1')"
        ).count()
        assert valid_count == 3  # file_001, file_002, file_003

        # Invalid row still present (expect, not drop)
        invalid = result.filter(
            "transaction_file_source_id = 'file_004'"
        ).collect()[0]
        assert invalid["transaction_code"] == "XX"


# ──────────────────────────────────────────────────────────────────────
# SegmentBuilder Unit Tests
# ──────────────────────────────────────────────────────────────────────

class TestSegmentBuilder:
    """Unit tests for SegmentBuilder methods."""

    def test_build_all_segments_returns_correct_count(self, test_spark, mock_all):
        """Should return 2 segments: claim (07) and header (00)."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()

        assert len(segments) == 2
        seg_codes = {s.segment_code for s in segments}
        assert "07" in seg_codes
        assert "00" in seg_codes

    def test_segment_has_correct_fields(self, test_spark, mock_all):
        """Each segment should have fields matching its rules."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()

        claim_seg = next(s for s in segments if s.segment_code == "07")
        # Mock has 6 fields for segment 07
        assert len(claim_seg.fields) == 6

        field_names = {f.bronze_key for f in claim_seg.fields}
        assert "F_407_D7" in field_names  # product_service_id
        assert "F_442_E7" in field_names  # compound_code

    def test_pivot_sql_structure(self, test_spark, mock_all):
        """Generated SQL should have GROUP BY and CAST expressions."""
        builder = SegmentBuilder(test_spark, CATALOG, SCHEMA)
        segments = builder.build_all_segments()
        claim_seg = next(s for s in segments if s.segment_code == "07")

        pivot_sql = builder.build_pivot_sql(claim_seg)

        # SQL should contain key structural elements
        assert "GROUP BY" in pivot_sql.upper()
        assert "CAST" in pivot_sql.upper()
        assert claim_seg.bronze_filter in pivot_sql
        assert "transaction_file_source_id" in pivot_sql
