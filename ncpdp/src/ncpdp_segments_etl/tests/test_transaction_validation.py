"""
SDP-native tests for claimbilling_silver_transaction_validation.

Validates:
- Segment presence flags: correct boolean per (file, pos)
- Conditional expectations: compound_when_required logic
- Mandatory segment checks: header_present, patient_present, etc.
- Event log captures expectation pass/fail counts
"""
import pytest
from pyspark.pipelines.testing import TestPipeline, test_spark  # noqa: F401
from conftest import test_pipeline, fqn


class TestTransactionValidationPresenceFlags:
    """Tests for segment presence flag correctness."""

    def test_all_segments_detected(self, test_spark, mock_all):
        """file_001 has S_HD, S_07, S_01, S_04 → those flags should be true."""
        validation_table = fqn("claimbilling_silver_transaction_validation")
        test_pipeline.run(test_spark, {validation_table})

        result = test_spark.table(validation_table)
        row = result.filter(
            "transaction_file_source_id = 'file_001' AND request_pos = 0"
        ).collect()[0]

        assert row["header_present"] == True
        assert row["patient_present"] == True
        assert row["insurance_present"] == True
        assert row["claim_present"] == True
        # No compound segment for file_001
        assert row["compound_present"] == False

    def test_compound_segment_detected(self, test_spark, mock_all):
        """file_002 has S_10 (compound) → compound_present should be true."""
        validation_table = fqn("claimbilling_silver_transaction_validation")
        test_pipeline.run(test_spark, {validation_table})

        result = test_spark.table(validation_table)
        row = result.filter(
            "transaction_file_source_id = 'file_002' AND request_pos = 0"
        ).collect()[0]

        assert row["compound_present"] == True
        assert row["compound_code"] == "2"  # Triggers compound requirement

    def test_segment_count_correct(self, test_spark, mock_all):
        """segment_count should reflect distinct segments per transaction."""
        validation_table = fqn("claimbilling_silver_transaction_validation")
        test_pipeline.run(test_spark, {validation_table})

        result = test_spark.table(validation_table)
        # file_001: S_HD, S_07, S_01, S_04 = 4 segments
        row1 = result.filter(
            "transaction_file_source_id = 'file_001' AND request_pos = 0"
        ).collect()[0]
        assert row1["segment_count"] == 4

        # file_002: S_HD, S_07, S_01, S_04, S_10 = 5 segments
        row2 = result.filter(
            "transaction_file_source_id = 'file_002' AND request_pos = 0"
        ).collect()[0]
        assert row2["segment_count"] == 5


class TestTransactionValidationExpectations:
    """Tests for cross-segment expectation rules."""

    def test_compound_when_required_passes(self, test_spark, mock_all):
        """
        file_002: compound_code='2' AND compound_present=true.
        The compound_when_required expectation should PASS.
        """
        validation_table = fqn("claimbilling_silver_transaction_validation")
        status = test_pipeline.run(test_spark, {validation_table})

        result = test_spark.table(validation_table)
        row = result.filter(
            "transaction_file_source_id = 'file_002' AND request_pos = 0"
        ).collect()[0]

        # Verify the condition: NOT(compound_code='2') OR compound_present=true
        # compound_code='2' AND compound_present=True → True (passes)
        assert row["compound_code"] == "2"
        assert row["compound_present"] == True

    def test_compound_when_required_not_triggered(self, test_spark, mock_all):
        """
        file_001: compound_code='1' (not '2'), so compound segment not required.
        The expectation should PASS regardless of compound_present value.
        """
        validation_table = fqn("claimbilling_silver_transaction_validation")
        test_pipeline.run(test_spark, {validation_table})

        result = test_spark.table(validation_table)
        row = result.filter(
            "transaction_file_source_id = 'file_001' AND request_pos = 0"
        ).collect()[0]

        # NOT(compound_code='2') evaluates to True → expectation passes
        assert row["compound_code"] == "1"
        assert row["compound_present"] == False  # Doesn't matter for this case

    def test_mandatory_segments_all_present(self, test_spark, mock_all):
        """
        file_001 and file_002 have all mandatory segments (HD, 01, 04, 07).
        All mandatory-presence expectations should pass for these rows.
        """
        validation_table = fqn("claimbilling_silver_transaction_validation")
        test_pipeline.run(test_spark, {validation_table})

        result = test_spark.table(validation_table)

        # Both file_001 and file_002 should pass all mandatory checks
        for file_id in ["file_001", "file_002"]:
            row = result.filter(
                f"transaction_file_source_id = '{file_id}' AND request_pos = 0"
            ).collect()[0]
            assert row["header_present"] == True
            assert row["patient_present"] == True
            assert row["insurance_present"] == True
            assert row["claim_present"] == True

    def test_event_log_records_expectation_results(self, test_spark, mock_all):
        """
        The pipeline event log should contain data_quality entries
        with pass/fail counts for each expectation.
        """
        validation_table = fqn("claimbilling_silver_transaction_validation")
        status = test_pipeline.run(test_spark, {validation_table})

        # Event log should be available
        assert status.event_log_table_name is not None
        events = test_spark.table(status.event_log_table_name)

        # Should have flow_progress events with data_quality details
        dq_events = events.filter(
            "event_type = 'flow_progress' AND details LIKE '%data_quality%'"
        ).collect()
        assert len(dq_events) > 0

        # Should reference our expectations by name
        all_details = " ".join(e["details"] for e in dq_events)
        assert "header_present" in all_details
        assert "compound_when_required" in all_details


class TestTransactionValidationEdgeCases:
    """Edge case tests for transaction validation."""

    def test_transaction_with_only_header(self, test_spark):
        """
        A transaction with only S_HD should have all other flags as false.
        Mandatory expectations (patient, insurance, claim) should fail.
        """
        # Create minimal mock: just one header row
        test_spark.sql(f"""
            CREATE TABLE {fqn('claimbilling_bronze_requests')} AS
            SELECT * FROM VALUES
                ('file_solo', 'S_HD', 0, 'F_101_A1', CAST('111111' AS VARIANT))
            AS t(transaction_file_source_id, request_segment, request_pos, key, value)
        """)
        # Need rules table too (even if empty for this table's SQL)
        test_spark.sql(f"""
            CREATE TABLE {fqn('specification_rules')} (
                rule_id STRING, segment_code STRING, segment_name STRING,
                rule_level STRING, bronze_key STRING, column_name STRING,
                data_type STRING, rule_type STRING,
                allowed_values ARRAY<STRING>, format_pattern STRING,
                condition STRING, column_comment STRING
            )
        """)

        validation_table = fqn("claimbilling_silver_transaction_validation")
        test_pipeline.run(test_spark, {validation_table})

        result = test_spark.table(validation_table)
        row = result.filter(
            "transaction_file_source_id = 'file_solo'"
        ).collect()[0]

        assert row["header_present"] == True
        assert row["patient_present"] == False
        assert row["insurance_present"] == False
        assert row["claim_present"] == False
        assert row["compound_present"] == False
        assert row["compound_code"] is None
