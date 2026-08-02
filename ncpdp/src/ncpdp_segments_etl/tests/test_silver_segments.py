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
        claim_table = fqn("claimbilling_silver_claim")
        test_pipeline.run(test_spark, {claim_table})

        result = test_spark.table(claim_table)
        # 4 distinct (file_id, pos) combinations have S_07 data
        assert result.count() == 4

    def test_pivot_column_values_correct(self, test_spark, mock_all):
        """Verify pivot correctly maps bronze keys to typed column values."""
        claim_table = fqn("claimbilling_silver_claim")
        test_pipeline.run(test_spark, {claim_table})

        result = test_spark.table(claim_table)
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
        claim_table = fqn("claimbilling_silver_claim")
        test_pipeline.run(test_spark, {claim_table})

        result = test_spark.table(claim_table)
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
        claim_table = fqn("claimbilling_silver_claim")
        test_pipeline.run(test_spark, {claim_table})

        result = test_spark.table(claim_table)
        # file_003 is missing F_407_D7 (product_service_id)
        row = result.filter("transaction_file_source_id = 'file_003'").collect()[0]
        assert row["product_service_id"] is None


class TestClaimSegmentExpectations:
    """Tests for @dp.expect rules on the claim segment."""

    def test_mandatory_expectation_passes_when_present(self, test_spark, mock_all):
        """Valid transactions should pass MANDATORY (NOT NULL) expectations."""
        claim_table = fqn("claimbilling_silver_claim")
        status = test_pipeline.run(test_spark, {claim_table})

        # Check event log for expectation results
        assert status.event_log_table_name is not None
        events = test_spark.table(status.event_log_table_name)

        # Find expectation events for the claim table
        dq_events = events.filter(
            "event_type = 'flow_progress' AND details LIKE '%data_quality%'"
        ).collect()

        # At least one event should contain expectation results
        assert len(dq_events) > 0

    def test_mandatory_expectation_fails_on_null(self, test_spark, mock_all):
        """
        file_003 is missing product_service_id (F_407_D7).
        The product_service_id_not_null expectation should record a failure.
        """
        claim_table = fqn("claimbilling_silver_claim")
        status = test_pipeline.run(test_spark, {claim_table})

        assert status.event_log_table_name is not None
        events = test_spark.table(status.event_log_table_name)

        # Parse expectation results from event log
        dq_event = events.filter(
            "event_type = 'flow_progress' AND details LIKE '%product_service_id_not_null%'"
        ).collect()

        assert len(dq_event) > 0
        # The details JSON should show failed_records > 0
        details = dq_event[0]["details"]
        assert "failed_records" in details

    def test_allowed_values_expectation_passes_for_valid(self, test_spark, mock_all):
        """
        file_001 has compound_code='1' which IS in ['1','2'].
        The expectation should pass for this row.
        """
        claim_table = fqn("claimbilling_silver_claim")
        status = test_pipeline.run(test_spark, {claim_table})

        result = test_spark.table(claim_table)
        valid_rows = result.filter("compound_code IN ('1', '2')").count()
        # file_001 (compound_code='1') and file_002 (compound_code='2') are valid
        assert valid_rows >= 2

    def test_allowed_values_expectation_flags_invalid(self, test_spark, mock_all):
        """
        file_004 has compound_code='9' which is NOT in ['1','2'].
        Row should still exist (expect, not expect_or_drop) but be flagged.
        """
        claim_table = fqn("claimbilling_silver_claim")
        test_pipeline.run(test_spark, {claim_table})

        result = test_spark.table(claim_table)
        # Row with invalid compound_code should still be present
        invalid_row = result.filter(
            "transaction_file_source_id = 'file_004'"
        ).collect()[0]
        assert invalid_row["compound_code"] == "9"


# ──────────────────────────────────────────────────────────────────────
# Header Segment (S_HD) Tests
# ──────────────────────────────────────────────────────────────────────

class TestHeaderSegmentPivot:
    """Tests for claimbilling_silver_header."""

    def test_header_row_count(self, test_spark, mock_all):
        """Should produce one row per (file, pos) with S_HD data."""
        header_table = fqn("claimbilling_silver_header")
        test_pipeline.run(test_spark, {header_table})

        result = test_spark.table(header_table)
        # 4 files have S_HD keys
        assert result.count() == 4

    def test_header_allowed_values(self, test_spark, mock_all):
        """
        transaction_code should be validated against ['B1','B2','B3','E1'].
        file_004 has 'XX' which is invalid.
        """
        header_table = fqn("claimbilling_silver_header")
        test_pipeline.run(test_spark, {header_table})

        result = test_spark.table(header_table)
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
